package broker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"time"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/ghapp"
	"goodkind.io/gha-mac-broker/internal/tart"
)

func normalizedJobsPerVM(cfg *config.Config) int {
	if cfg == nil || cfg.JobsPerVM < 1 {
		return 1
	}

	return cfg.JobsPerVM
}

func normalizeSlotCount(slotCount int) int {
	if slotCount < 1 {
		return 1
	}

	return slotCount
}

// bootCommand builds the headless boot command with the cache dir mounted.
func (b *Binder) bootCommand(ctx context.Context, cfg *config.Config, vmName string) *exec.Cmd {
	var dirs []tart.DirMount
	if cfg.Tart.CacheDir != "" {
		// tart --dir requires the host path to exist, so create it before the
		// mount. MkdirAll is idempotent and cheap on the warm path.
		if err := os.MkdirAll(cfg.Tart.CacheDir, 0o700); err != nil {
			slog.WarnContext(
				ctx,
				"create cache dir failed; booting without cache mount",
				"err",
				err,
				"dir",
				cfg.Tart.CacheDir,
			)
		} else {
			// Chmod after MkdirAll: MkdirAll applies 0700 only to dirs it creates,
			// so tighten an existing looser dir too. The build cache can hold
			// proprietary source and artifacts, so keep it private to the owner on a
			// multi-user host.
			if err := os.Chmod(cfg.Tart.CacheDir, 0o700); err != nil {
				slog.WarnContext(
					ctx,
					"chmod cache dir failed; continuing with existing perms",
					"err",
					err,
					"dir",
					cfg.Tart.CacheDir,
				)
			}

			dirs = []tart.DirMount{{Name: "cache", Path: cfg.Tart.CacheDir}}
		}
	}

	return b.vm.BootCommand(
		ctx,
		vmName,
		tart.BootOptions{NoGraphics: true, Detach: true, Dirs: dirs},
	)
}

// generateJIT mints the repo-scoped JIT config for one job.
func (b *Binder) generateJIT(
	ctx context.Context,
	owner string,
	repoName string,
	runnerName string,
) (*ghapp.JITConfig, error) {
	cfg := b.configSnapshot()
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

	jit, err := b.gh.GenerateJITConfig(ctx, token, owner, repoName, runnerName, cfg.Labels)
	if err != nil {
		slog.ErrorContext(ctx, "generate jitconfig failed", "err", err, "repo", owner+"/"+repoName)

		return nil, fmt.Errorf("broker: generate jitconfig: %w", err)
	}

	return jit, nil
}

// waitForReady polls the guest vsock channel (`tart exec <vm> true`) until it
// answers or the timeout elapses. The token read and Hello then confirm the
// guest agent itself is up.
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
			slog.ErrorContext(
				ctx,
				"timed out waiting for vsock readiness",
				"err",
				ctx.Err(),
				"vm",
				vmName,
			)

			return fmt.Errorf("broker: waiting for vsock readiness of %s: %w", vmName, ctx.Err())
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
