// Package install performs the full host setup for the broker daemon: it
// scaffolds the config directory, secrets, and webhook CIDR list, builds the
// golden Tart image when missing, and renders and bootstraps the OS service
// unit (launchd on macOS, systemd user unit on Linux) from embedded templates.
// Every step is idempotent and skips work that is already satisfied so the
// command is safe to re-run.
package install

import (
	"context"
	"log/slog"
	"os/exec"
	"strings"
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
	if err := buildGoldenIfAbsent(ctx, cfg.ConfigPath); err != nil {
		return err
	}
	if err := installUnit(ctx, cfg); err != nil {
		return err
	}

	printOperatorNotice(ctx, cfg)
	return nil
}

// Uninstall reverses only the service-unit step: it boots out the launchd job
// or disables the systemd unit, then removes the unit file. The config
// directory, secrets, and golden image are left in place.
func Uninstall(ctx context.Context, cfg Config) error {
	slog.InfoContext(ctx, "uninstall starting", "config", cfg.ConfigPath)
	return uninstallUnit(ctx, cfg)
}

// command builds an [exec.Cmd] for an external tool. Centralizing construction
// keeps the single audited exec call site in one place.
func command(ctx context.Context, name string, args ...string) *exec.Cmd {
	slog.DebugContext(ctx, "install command built", "name", name, "args", strings.Join(args, " "))
	return exec.CommandContext(ctx, name, args...)
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
