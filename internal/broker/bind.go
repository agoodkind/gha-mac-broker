// Package broker orchestrates just-in-time runner binds against warm Tart VMs.
// The Binder can clone, boot, and SSH-probe a VM (Warm), run a single
// ephemeral GitHub Actions job on it (RunJob), and tear it down (Teardown).
// BindOnce composes these three steps into one synchronous call and is used by
// the bind CLI command. The pool drives Warm and Teardown directly; the webhook
// server drives RunJob.
package broker

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/ghapp"
	"goodkind.io/gha-mac-broker/internal/tart"
	"goodkind.io/gha-mac-broker/internal/vmssh"
)

// readinessTimeout bounds how long to wait for IP and SSH after boot.
const readinessTimeout = 90 * time.Second

// readinessInterval is the poll interval while waiting for readiness.
const readinessInterval = 2 * time.Second

// runnerHome is where the golden image keeps the GitHub Actions runner.
const runnerHome = "~/actions-runner"

// WarmVM is a booted, SSH-ready VM that has not yet been bound to a job.
// Name and Host are safe to read from any goroutine once Warm returns.
type WarmVM struct {
	// Name is the tart VM name used to stop and delete the VM.
	Name string
	// Host is the IP address the VM is reachable on over SSH.
	Host string
	boot *exec.Cmd
}

// Binder performs JIT runner binds against a warm VM substrate.
type Binder struct {
	cfg *config.Config
	gh  *ghapp.Client
	vm  *tart.Tart
	ssh *vmssh.Runner
}

// New builds a Binder from its collaborators.
func New(cfg *config.Config, gh *ghapp.Client, vm *tart.Tart, ssh *vmssh.Runner) *Binder {
	return &Binder{cfg: cfg, gh: gh, vm: vm, ssh: ssh}
}

// Warm clones the golden image to a prefixed name derived from id, boots the
// VM, and waits until it accepts SSH. On any failure, Warm tears down the
// partial VM before returning the error; the caller owns teardown only on
// success.
func (b *Binder) Warm(ctx context.Context, id string) (*WarmVM, error) {
	vmName := b.cfg.Tart.VMNamePrefix + "-" + id
	slog.InfoContext(ctx, "warming vm", "vm", vmName)

	// Idempotent clone: best-effort delete any pre-existing VM of this exact
	// name before cloning, so the clone self-heals even if the startup sweep
	// missed a VM or a same-instant run-token clash leaves a stale name. A
	// "does not exist" error is the normal case and is logged at debug only.
	if err := b.vm.Delete(ctx, vmName); err != nil {
		slog.DebugContext(ctx, "pre-clone delete returned error (ignored)", "err", err, "vm", vmName)
	}

	if err := b.vm.Clone(ctx, b.cfg.Tart.GoldenImage, vmName); err != nil {
		slog.ErrorContext(ctx, "clone failed", "err", err, "vm", vmName)
		return nil, fmt.Errorf("broker: clone %s: %w", vmName, err)
	}

	bootCmd := b.bootCommand(ctx, vmName)
	if err := bootCmd.Start(); err != nil {
		slog.ErrorContext(ctx, "vm boot failed", "err", err, "vm", vmName)
		b.teardown(ctx, vmName)
		return nil, fmt.Errorf("broker: boot %s: %w", vmName, err)
	}

	host, err := b.waitForIP(ctx, vmName)
	if err != nil {
		_ = bootCmd.Process.Kill()
		b.teardown(ctx, vmName)
		return nil, err
	}
	if err := b.waitForSSH(ctx, host); err != nil {
		_ = bootCmd.Process.Kill()
		b.teardown(ctx, vmName)
		return nil, err
	}

	return &WarmVM{Name: vmName, Host: host, boot: bootCmd}, nil
}

// RunJob checks the allowlist, mints a JIT config, and ssh-runs one ephemeral
// GitHub Actions job on the given warm VM. runnerName is used as the runner
// registration name.
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
	if _, err := b.ssh.Run(ctx, vm.Host, remote); err != nil {
		slog.ErrorContext(ctx, "job run failed", "err", err, "vm", vm.Name)
		return fmt.Errorf("broker: run job on %s: %w", vm.Name, err)
	}
	slog.InfoContext(ctx, "job complete", "repo", repo, "vm", vm.Name)
	return nil
}

// Teardown kills the boot process if it is still running, then stops and
// deletes the VM. It is best effort; errors are logged at Warn level.
func (b *Binder) Teardown(ctx context.Context, vm *WarmVM) {
	if vm.boot != nil && vm.boot.Process != nil {
		_ = vm.boot.Process.Kill()
	}
	b.teardown(ctx, vm.Name)
}

// SweepOrphans stops and deletes any leftover pool VMs from a previous broker
// process. On a fresh start the pool owns no VMs, so every VM whose name
// carries the pool prefix is an orphan (for example after a hard restart that
// skipped graceful shutdown) and is torn down before the pool fills. The golden
// image is named separately and never matches the prefix. Best effort: a list
// failure aborts the sweep (its cause is logged by List), and teardown failures
// are logged, not returned.
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

// BindOnce clones a warm VM, registers it as an ephemeral runner for repo,
// runs one job, and tears the VM down. id makes the VM and runner names
// unique. repo is owner/repo and must be in the allowlist.
func (b *Binder) BindOnce(ctx context.Context, repo, id string) error {
	if _, _, ok := strings.Cut(repo, "/"); !ok {
		return fmt.Errorf("broker: repo must be owner/repo, got %q", repo)
	}
	if !b.cfg.RepoAllowed(repo) {
		return fmt.Errorf("broker: repo %s is not in allowed_repos", repo)
	}
	vm, err := b.Warm(ctx, id)
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
		dirs = []tart.DirMount{{Name: "cache", Path: b.cfg.Tart.CacheDir}}
	}
	return b.vm.BootCommand(ctx, vmName, tart.BootOptions{
		NoGraphics:      true,
		Dirs:            dirs,
		BridgeInterface: b.cfg.Tart.BridgeInterface,
	})
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

// waitForIP polls until the VM reports an IP or the timeout elapses.
func (b *Binder) waitForIP(ctx context.Context, vmName string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()
	ticker := time.NewTicker(readinessInterval)
	defer ticker.Stop()
	for {
		ip, err := b.vm.IP(ctx, vmName)
		if err == nil && ip != "" {
			return ip, nil
		}
		select {
		case <-ctx.Done():
			slog.ErrorContext(ctx, "timed out waiting for IP", "err", ctx.Err(), "vm", vmName)
			return "", fmt.Errorf("broker: waiting for IP of %s: %w", vmName, ctx.Err())
		case <-ticker.C:
		}
	}
}

// waitForSSH polls until the VM accepts SSH or the timeout elapses.
func (b *Binder) waitForSSH(ctx context.Context, host string) error {
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()
	ticker := time.NewTicker(readinessInterval)
	defer ticker.Stop()
	for {
		if err := b.ssh.Probe(ctx, host); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			slog.ErrorContext(ctx, "timed out waiting for SSH", "err", ctx.Err(), "host", host)
			return fmt.Errorf("broker: waiting for SSH on %s: %w", host, ctx.Err())
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
