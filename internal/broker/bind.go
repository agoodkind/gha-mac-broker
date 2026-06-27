// Package broker orchestrates a single just-in-time runner bind: it clones a
// warm VM from the golden image, boots it, mints a repo-scoped JIT runner
// config, injects it over SSH so the VM runs exactly one ephemeral job, then
// tears the VM down. The warm pool and webhook server in later phases drive this
// same BindOnce primitive.
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

// Binder performs single JIT runner binds against a warm VM substrate.
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

// BindOnce clones a warm VM, registers it as an ephemeral runner for repo, runs
// one job, and tears the VM down. id makes the VM and runner names unique. repo
// is owner/repo and must be in the allowlist.
func (b *Binder) BindOnce(ctx context.Context, repo, id string) error {
	owner, repoName, ok := strings.Cut(repo, "/")
	if !ok {
		return fmt.Errorf("broker: repo must be owner/repo, got %q", repo)
	}
	if !b.cfg.RepoAllowed(repo) {
		return fmt.Errorf("broker: repo %s is not in allowed_repos", repo)
	}

	vmName := b.cfg.Tart.VMNamePrefix + "-" + id
	runnerName := vmName
	slog.InfoContext(ctx, "bind starting", "repo", repo, "vm", vmName)

	if err := b.vm.Clone(ctx, b.cfg.Tart.GoldenImage, vmName); err != nil {
		return fmt.Errorf("broker: clone %s: %w", vmName, err)
	}
	defer b.teardown(ctx, vmName)

	bootCmd := b.bootCommand(ctx, vmName)
	if err := bootCmd.Start(); err != nil {
		slog.ErrorContext(ctx, "vm boot failed", "err", err, "vm", vmName)
		return fmt.Errorf("broker: boot %s: %w", vmName, err)
	}
	defer func() {
		if bootCmd.Process != nil {
			_ = bootCmd.Process.Kill()
		}
	}()

	host, err := b.waitForIP(ctx, vmName)
	if err != nil {
		return err
	}
	if err := b.waitForSSH(ctx, host); err != nil {
		return err
	}

	jit, err := b.generateJIT(ctx, owner, repoName, runnerName)
	if err != nil {
		return err
	}

	remote := fmt.Sprintf("cd %s && ./run.sh --jitconfig %s", runnerHome, jit.EncodedJITConfig)
	slog.InfoContext(ctx, "running ephemeral job", "repo", repo, "vm", vmName, "runner", jit.Runner.Name)
	if _, err := b.ssh.Run(ctx, host, remote); err != nil {
		return fmt.Errorf("broker: run job on %s: %w", vmName, err)
	}
	slog.InfoContext(ctx, "bind complete", "repo", repo, "vm", vmName)
	return nil
}

// bootCommand builds the headless boot command with the cache dir mounted.
func (b *Binder) bootCommand(ctx context.Context, vmName string) *exec.Cmd {
	var dirs []tart.DirMount
	if b.cfg.Tart.CacheDir != "" {
		dirs = []tart.DirMount{{Name: "cache", Path: b.cfg.Tart.CacheDir}}
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
		return nil, fmt.Errorf("broker: installation token: %w", err)
	}
	jit, err := b.gh.GenerateJITConfig(ctx, token, owner, repoName, runnerName, b.cfg.Labels)
	if err != nil {
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
