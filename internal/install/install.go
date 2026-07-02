// Package install performs the full host setup for the broker daemon: it
// scaffolds the config directory, secrets, and webhook CIDR list, builds the
// golden Tart image when missing, and renders and bootstraps the OS service
// unit (launchd on macOS, systemd user unit on Linux) from embedded templates.
// Every step is idempotent and skips work that is already satisfied so the
// command is safe to re-run.
package install

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"

	"goodkind.io/gha-mac-broker/internal/config"
)

// Config is the set of resolved paths the installer renders into the service
// unit and scaffolds on disk. The caller in package main fills every field.
type Config struct {
	// BinPath is the absolute path to the installed broker binary.
	BinPath string
	// Home is the user home directory the unit and scaffold paths derive from.
	Home string
	// ConfigDir is the directory holding config.toml and the secret files.
	ConfigDir string
	// LogPath is the launchd stdout/stderr log file (macOS only).
	LogPath string
	// ConfigPath is the config.toml path the daemon is told to load.
	ConfigPath string
	// Maintenance configures the optional launchd maintenance timer.
	Maintenance config.MaintenanceConfig
}

// Install runs the full host setup in order: config dir, config.toml, secrets,
// webhook CIDRs, golden image, then the OS service unit. It finishes by
// printing the operator steps the binary cannot perform.
func Install(ctx context.Context, cfg Config) error {
	slog.InfoContext(ctx, "install starting", "bin", cfg.BinPath, "config", cfg.ConfigPath)

	if err := ensureConfigDir(ctx, cfg.ConfigDir); err != nil {
		return err
	}
	if err := scaffoldConfig(ctx, cfg); err != nil {
		return err
	}
	if err := ensureSecrets(ctx, cfg.ConfigDir); err != nil {
		return err
	}
	ensureWebhookCIDRs(ctx, cfg.ConfigDir)
	runtimeConfig, err := config.Load(cfg.ConfigPath)
	if err != nil {
		slog.ErrorContext(ctx, "load config for install failed", "err", err, "path", cfg.ConfigPath)
		return fmt.Errorf("install: load config: %w", err)
	}
	if err := buildGoldenIfAbsent(ctx, runtimeConfig); err != nil {
		return err
	}
	if err := installUnit(ctx, cfg); err != nil {
		return err
	}
	cfg.Maintenance = runtimeConfig.Maintenance
	if err := installMaintenanceTimer(ctx, cfg); err != nil {
		return err
	}

	printOperatorNotice(ctx, cfg)
	return nil
}

// Uninstall reverses the installed service units: it boots out the launchd jobs
// or disables the systemd unit, then removes the unit files. The config
// directory, secrets, and golden image are left in place.
func Uninstall(ctx context.Context, cfg Config) error {
	slog.InfoContext(ctx, "uninstall starting", "config", cfg.ConfigPath)
	if err := uninstallUnit(ctx, cfg); err != nil {
		return err
	}
	return uninstallMaintenanceTimer(ctx, cfg)
}

type combinedOutputRunner interface {
	CombinedOutput() ([]byte, error)
}

var commandRunner = func(ctx context.Context, name string, args ...string) combinedOutputRunner {
	return command(ctx, name, args...)
}

// command builds an [exec.Cmd] for an external tool. Centralizing construction
// keeps the single audited exec call site in one place.
func command(ctx context.Context, name string, args ...string) *exec.Cmd {
	slog.DebugContext(ctx, "install command built", "name", name, "args", strings.Join(args, " "))
	return exec.CommandContext(ctx, name, args...)
}

func requireHostBinary(ctx context.Context, binary string, installHint string) error {
	if _, err := lookPath(binary); err != nil {
		slog.ErrorContext(ctx, "required host binary not found on PATH", "err", err, "binary", binary)
		return fmt.Errorf("install: %s not found on PATH; install it with `%s`: %w", binary, installHint, err)
	}
	return nil
}

func lookPath(binary string) (string, error) {
	return exec.LookPath(binary)
}

// printOperatorNotice logs the steps the installer cannot perform, so the
// operator knows what remains before the daemon can serve.
func printOperatorNotice(ctx context.Context, cfg Config) {
	slog.InfoContext(ctx, "install complete; operator steps remain",
		"config", cfg.ConfigPath,
		"step_1", "edit config.toml and set app.app_id to the GitHub App ID",
		"step_2", "place the GitHub App private key at app.private_key_path",
		"step_3", "set up the Cloudflare tunnel to expose the webhook endpoint",
	)
}
