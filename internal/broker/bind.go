// Package broker orchestrates just-in-time runner binds against warm Tart VMs.
// The Binder can clone, boot, and ready a VM (Warm), run a single ephemeral
// GitHub Actions job on it (RunJob), and tear it down (Teardown). All guest
// control goes over the tart-exec vsock channel, with no IP and no SSH.
// BindOnce composes these steps into one synchronous call for the bind CLI. The
// pool drives Warm and Teardown directly; the webhook server drives RunJob.
package broker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/ghapp"
	"goodkind.io/gha-mac-broker/internal/golden"
	"goodkind.io/gha-mac-broker/internal/tart"
)

// readinessTimeout bounds how long to wait for the guest vsock channel after boot.
const readinessTimeout = 90 * time.Second

// readinessInterval is the poll interval while waiting for readiness.
const readinessInterval = 2 * time.Second

// touchInterval is how often the broker refreshes each warm VM's liveness file
// over vsock. The guest watchdog's stale timeout must exceed this.
const touchInterval = 10 * time.Second

// heartbeatFile is the guest path the broker touches on a timer; the baked guest
// watchdog self-terminates the VM when this file goes stale.
const heartbeatFile = "/tmp/gha-broker.alive"

// runnerHome is where the golden image keeps the GitHub Actions runner.
const runnerHome = "~/actions-runner"

// activeJobProbeTimeout bounds /status guest process checks.
const activeJobProbeTimeout = 5 * time.Second

type activeJobProbeResult string

const (
	activeJobProbeResultYes activeJobProbeResult = "yes"
	activeJobProbeResultNo  activeJobProbeResult = "no"
)

// WarmVM is a booted, vsock-ready VM that has not yet been bound to a job. Name
// is safe to read from any goroutine once Warm returns.
type WarmVM struct {
	// Name is the tart VM name used for exec, stop, and delete.
	Name string
	// Image is the approved Cirrus tag this VM was cloned for.
	Image string
	boot  *exec.Cmd
	// stopTouch ends the per-VM liveness touch loop on teardown.
	stopTouch context.CancelFunc
}

// Binder performs JIT runner binds against a warm VM substrate over vsock.
type Binder struct {
	cfg *config.Config
	gh  *ghapp.Client
	vm  *tart.Tart
}

// New builds a Binder from its collaborators.
func New(cfg *config.Config, gh *ghapp.Client, vm *tart.Tart) *Binder {
	return &Binder{cfg: cfg, gh: gh, vm: vm}
}

// Warm clones the golden image to a prefixed name derived from id, boots the VM,
// waits until its vsock channel answers, and starts the liveness touch loop. On
// any failure, Warm tears down the partial VM before returning; the caller owns
// teardown only on success.
func (b *Binder) Warm(ctx context.Context, image, id string) (*WarmVM, error) {
	vmName := b.cfg.Tart.VMNamePrefix + "-" + id
	slog.InfoContext(ctx, "warming vm", "vm", vmName, "image", image)

	goldenName, err := golden.New(b.vm).EnsureGolden(ctx, golden.EnsureOptions{
		Image:         image,
		BuildVM:       golden.NameForImage(image) + "-build-" + id,
		RunnerVersion: "",
	})
	if err != nil {
		slog.ErrorContext(ctx, "ensure golden failed", "err", err, "image", image)
		return nil, fmt.Errorf("broker: ensure golden for %s: %w", image, err)
	}

	// Idempotent clone: best-effort delete any pre-existing VM of this exact
	// name before cloning, so the clone self-heals even if the startup sweep
	// missed a VM or a same-instant run-token clash leaves a stale name. A
	// "does not exist" error is the normal case and is logged at debug only.
	if err := b.vm.Delete(ctx, vmName); err != nil {
		slog.DebugContext(ctx, "pre-clone delete returned error (ignored)", "err", err, "vm", vmName)
	}

	if err := b.vm.Clone(ctx, goldenName, vmName, false); err != nil {
		slog.ErrorContext(ctx, "clone failed", "err", err, "vm", vmName)
		return nil, fmt.Errorf("broker: clone %s: %w", vmName, err)
	}

	bootCmd := b.bootCommand(ctx, vmName)
	if err := bootCmd.Start(); err != nil {
		slog.ErrorContext(ctx, "vm boot failed", "err", err, "vm", vmName)
		b.teardown(ctx, vmName)
		return nil, fmt.Errorf("broker: boot %s: %w", vmName, err)
	}

	if err := b.waitForReady(ctx, vmName); err != nil {
		_ = bootCmd.Process.Kill()
		b.teardown(ctx, vmName)
		return nil, err
	}

	// The touch loop outlives this call (the VM stays in the pool), so it runs
	// on a context detached from ctx and is stopped explicitly in Teardown.
	touchCtx, stopTouch := context.WithCancel(context.WithoutCancel(ctx))
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(touchCtx, "touch loop panic recovered", "err", fmt.Errorf("panic: %v", r), "vm", vmName)
			}
		}()
		b.touchLoop(touchCtx, vmName)
	}()

	return &WarmVM{Name: vmName, Image: image, boot: bootCmd, stopTouch: stopTouch}, nil
}

// RunJob checks the allowlist, mints a JIT config, and runs one ephemeral GitHub
// Actions job on the warm VM over vsock. runnerName is the runner registration
// name.
func (b *Binder) RunJob(ctx context.Context, vm *WarmVM, repo, runnerName string) error {
	owner, repoName, ok := strings.Cut(repo, "/")
	if !ok {
		return fmt.Errorf("broker: repo must be owner/repo, got %q", repo)
	}
	if !b.cfg.RepoAllowed(repo) {
		return fmt.Errorf("broker: repo %s is not in allowed_repos", repo)
	}

	jit, err := b.generateJIT(ctx, owner, repoName, runnerName)
	if err != nil {
		return err
	}

	remote := fmt.Sprintf("cd %s && ./run.sh --jitconfig %s", runnerHome, jit.EncodedJITConfig)
	slog.InfoContext(ctx, "running ephemeral job", "repo", repo, "vm", vm.Name, "runner", jit.Runner.Name)
	runLog, runLogPath, err := openRunLog(ctx, vm.Name)
	if err != nil {
		slog.WarnContext(ctx, "run log open failed; using buffered exec", "err", err, "vm", vm.Name, "path", runLogPath)
		if _, err := b.vm.Exec(ctx, vm.Name, "bash", "-lc", remote); err != nil {
			slog.ErrorContext(ctx, "job run failed", "err", err, "vm", vm.Name)
			return fmt.Errorf("broker: run job on %s: %w", vm.Name, err)
		}
		slog.InfoContext(ctx, "job complete", "repo", repo, "vm", vm.Name)
		return nil
	}
	defer func() {
		if err := runLog.Close(); err != nil {
			slog.WarnContext(ctx, "run log close failed", "err", err, "vm", vm.Name, "path", runLogPath)
		}
	}()
	if _, err := b.vm.ExecTee(ctx, vm.Name, runLog, "bash", "-lc", remote); err != nil {
		slog.ErrorContext(ctx, "job run failed", "err", err, "vm", vm.Name)
		return fmt.Errorf("broker: run job on %s: %w", vm.Name, err)
	}
	slog.InfoContext(ctx, "job complete", "repo", repo, "vm", vm.Name)
	return nil
}

func openRunLog(ctx context.Context, vmName string) (*os.File, string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		slog.WarnContext(ctx, "resolve home dir for run log failed", "err", err, "vm", vmName)
		return nil, "", fmt.Errorf("resolve home dir: %w", err)
	}
	logDir := filepath.Join(home, "Library", "Logs", "gha-mac-broker")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		slog.WarnContext(ctx, "create run log dir failed", "err", err, "vm", vmName, "path", logDir)
		return nil, logDir, fmt.Errorf("create run log dir: %w", err)
	}
	logPath := filepath.Join(logDir, "run-"+safeLogName(vmName)+".log")
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		slog.WarnContext(ctx, "open run log failed", "err", err, "vm", vmName, "path", logPath)
		return nil, logPath, fmt.Errorf("open run log: %w", err)
	}
	return file, logPath, nil
}

func safeLogName(name string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_")
	return replacer.Replace(name)
}

// Teardown stops the liveness touch loop, kills the boot process if running,
// then stops and deletes the VM. It is best effort; errors are logged at Warn.
func (b *Binder) Teardown(ctx context.Context, vm *WarmVM) {
	if vm.stopTouch != nil {
		vm.stopTouch()
	}
	if vm.boot != nil && vm.boot.Process != nil {
		_ = vm.boot.Process.Kill()
	}
	b.teardown(ctx, vm.Name)
}

// CheckAlive verifies that a cached warm VM still answers over vsock.
func (b *Binder) CheckAlive(ctx context.Context, vm *WarmVM) error {
	if _, err := b.vm.Exec(ctx, vm.Name, "touch", heartbeatFile); err != nil {
		slog.WarnContext(ctx, "warm vm liveness probe failed", "err", err, "vm", vm.Name)
		return fmt.Errorf("broker: check alive %s: %w", vm.Name, err)
	}
	return nil
}

// HasActiveJob reports whether the guest is running an actions job worker.
func (b *Binder) HasActiveJob(ctx context.Context, vm *WarmVM) (bool, error) {
	probeCtx, cancel := context.WithTimeout(ctx, activeJobProbeTimeout)
	defer cancel()
	out, err := b.vm.Exec(probeCtx, vm.Name, "bash", "-lc", "pgrep -f Runner.Worker >/dev/null 2>&1 && echo yes || echo no")
	if err != nil {
		slog.WarnContext(probeCtx, "active job probe failed", "err", err, "vm", vm.Name)
		return false, fmt.Errorf("broker: probe active job on %s: %w", vm.Name, err)
	}
	result := activeJobProbeResult(strings.TrimSpace(string(out)))
	switch result {
	case activeJobProbeResultYes:
		return true, nil
	case activeJobProbeResultNo:
		return false, nil
	default:
		slog.WarnContext(probeCtx, "active job probe returned unexpected output", "vm", vm.Name, "output", result)
		return false, fmt.Errorf("broker: active job probe on %s returned %q", vm.Name, string(result))
	}
}

// List returns the Tart VM names visible to the broker host.
func (b *Binder) List(ctx context.Context) ([]string, error) {
	names, err := b.vm.List(ctx)
	if err != nil {
		slog.WarnContext(ctx, "list tart vms failed", "err", err)
		return nil, fmt.Errorf("broker: list tart vms: %w", err)
	}
	return names, nil
}

// DeleteGolden removes the derived golden for image from disk.
func (b *Binder) DeleteGolden(ctx context.Context, image string) error {
	goldenName := golden.NameForImage(image)
	if err := b.vm.Delete(ctx, goldenName); err != nil {
		slog.WarnContext(ctx, "delete golden failed", "err", err, "golden", goldenName, "image", image)
		return fmt.Errorf("broker: delete golden %s: %w", goldenName, err)
	}
	return nil
}

// SweepOrphans stops and deletes any leftover pool VMs from a previous broker
// process. On a fresh start the pool owns no VMs, so every VM whose name carries
// the pool prefix is an orphan (for example after a hard restart that skipped
// graceful shutdown) and is torn down before the pool fills. The golden image is
// named separately and never matches the prefix. Best effort: a list failure
// aborts the sweep (its cause is logged by List), and teardown failures are
// logged, not returned.
func (b *Binder) SweepOrphans(ctx context.Context) {
	names, err := b.vm.List(ctx)
	if err != nil {
		return
	}
	prefix := b.cfg.Tart.VMNamePrefix + "-"
	swept := 0
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		slog.DebugContext(ctx, "orphan sweep: tearing down stale vm", "vm", name)
		b.teardown(ctx, name)
		swept++
	}
	if swept > 0 {
		slog.InfoContext(ctx, "orphan sweep complete", "count", swept)
	}
}

// BindOnce clones a warm VM, registers it as an ephemeral runner for repo, runs
// one job, and tears the VM down. id makes the VM and runner names unique. repo
// is owner/repo and must be in the allowlist.
func (b *Binder) BindOnce(ctx context.Context, repo, id string) error {
	if _, _, ok := strings.Cut(repo, "/"); !ok {
		return fmt.Errorf("broker: repo must be owner/repo, got %q", repo)
	}
	if !b.cfg.RepoAllowed(repo) {
		return fmt.Errorf("broker: repo %s is not in allowed_repos", repo)
	}
	vm, err := b.Warm(ctx, b.cfg.Tart.BaseImage, id)
	if err != nil {
		return err
	}
	defer b.Teardown(ctx, vm)
	return b.RunJob(ctx, vm, repo, vm.Name)
}

// bootCommand builds the headless boot command with the cache dir mounted.
func (b *Binder) bootCommand(ctx context.Context, vmName string) *exec.Cmd {
	var dirs []tart.DirMount
	if b.cfg.Tart.CacheDir != "" {
		// tart --dir requires the host path to exist, so create it before the
		// mount. MkdirAll is idempotent and cheap on the warm path.
		if err := os.MkdirAll(b.cfg.Tart.CacheDir, 0o700); err != nil {
			slog.WarnContext(ctx, "create cache dir failed; booting without cache mount", "err", err, "dir", b.cfg.Tart.CacheDir)
		} else {
			// Chmod after MkdirAll: MkdirAll applies 0700 only to dirs it
			// creates, so tighten an existing looser dir too. The build cache
			// can hold proprietary source and artifacts, so keep it private to
			// the owner on a multi-user host.
			if err := os.Chmod(b.cfg.Tart.CacheDir, 0o700); err != nil {
				slog.WarnContext(ctx, "chmod cache dir failed; continuing with existing perms", "err", err, "dir", b.cfg.Tart.CacheDir)
			}
			dirs = []tart.DirMount{{Name: "cache", Path: b.cfg.Tart.CacheDir}}
		}
	}
	return b.vm.BootCommand(ctx, vmName, tart.BootOptions{NoGraphics: true, Dirs: dirs})
}

// generateJIT mints the repo-scoped JIT config for one job.
func (b *Binder) generateJIT(ctx context.Context, owner, repoName, runnerName string) (*ghapp.JITConfig, error) {
	installationID, err := b.gh.InstallationID(ctx, owner, repoName)
	if err != nil {
		slog.ErrorContext(ctx, "installation lookup failed", "err", err, "repo", owner+"/"+repoName)
		return nil, fmt.Errorf("broker: installation lookup: %w", err)
	}
	token, err := b.gh.InstallationToken(ctx, installationID, repoName)
	if err != nil {
		slog.ErrorContext(ctx, "installation token failed", "err", err, "repo", owner+"/"+repoName)
		return nil, fmt.Errorf("broker: installation token: %w", err)
	}
	jit, err := b.gh.GenerateJITConfig(ctx, token, owner, repoName, runnerName, b.cfg.Labels)
	if err != nil {
		slog.ErrorContext(ctx, "generate jitconfig failed", "err", err, "repo", owner+"/"+repoName)
		return nil, fmt.Errorf("broker: generate jitconfig: %w", err)
	}
	return jit, nil
}

// waitForReady polls the guest vsock channel (`tart exec <vm> true`) until it
// answers or the timeout elapses.
func (b *Binder) waitForReady(ctx context.Context, vmName string) error {
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()
	ticker := time.NewTicker(readinessInterval)
	defer ticker.Stop()
	for {
		if _, err := b.vm.Exec(ctx, vmName, "true"); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			slog.ErrorContext(ctx, "timed out waiting for vsock readiness", "err", ctx.Err(), "vm", vmName)
			return fmt.Errorf("broker: waiting for vsock readiness of %s: %w", vmName, ctx.Err())
		case <-ticker.C:
		}
	}
}

// touchLoop refreshes the guest liveness file over vsock on a timer until the
// context is cancelled (at teardown). If the broker dies, touches stop, the file
// goes stale, and the guest watchdog self-terminates the VM.
func (b *Binder) touchLoop(ctx context.Context, vmName string) {
	ticker := time.NewTicker(touchInterval)
	defer ticker.Stop()
	for {
		if _, err := b.vm.Exec(ctx, vmName, "touch", heartbeatFile); err != nil {
			slog.DebugContext(ctx, "heartbeat touch failed", "err", err, "vm", vmName)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// teardown stops and deletes a VM, best effort.
func (b *Binder) teardown(ctx context.Context, vmName string) {
	if err := b.vm.Stop(ctx, vmName); err != nil {
		slog.WarnContext(ctx, "vm stop failed", "err", err, "vm", vmName)
	}
	if err := b.vm.Delete(ctx, vmName); err != nil {
		slog.WarnContext(ctx, "vm delete failed", "err", err, "vm", vmName)
	}
}
