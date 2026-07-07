package install

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
)

type serviceCommandRunner func(ctx context.Context, name string, args ...string) ([]byte, error)

var (
	restartRuntimeOS                      = func() string { return runtime.GOOS }
	restartUserHome                       = os.UserHomeDir
	restartStat                           = os.Stat
	restartCommand   serviceCommandRunner = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		return command(ctx, name, args...).CombinedOutput()
	}
)

// Restart restarts the installed user service when its unit is present.
func Restart(ctx context.Context) (bool, error) {
	home, err := restartUserHome()
	if err != nil {
		slog.ErrorContext(ctx, "resolve home dir failed", "err", err)
		return false, fmt.Errorf("restart: resolve home dir: %w", err)
	}
	restartOS := restartRuntimeOS()
	switch restartOS {
	case osDarwin:
		return restartLaunchd(ctx, home)
	case osLinux:
		return restartSystemd(ctx, home)
	default:
		slog.ErrorContext(ctx, "unsupported OS", "err", errors.ErrUnsupported, "os", restartOS)
		return false, fmt.Errorf("restart: unsupported OS %q: %w", restartOS, errors.ErrUnsupported)
	}
}

func restartLaunchd(ctx context.Context, home string) (bool, error) {
	plistPath := filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist")
	if _, err := restartStat(plistPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("restart: stat plist %s: %w", plistPath, err)
	}
	target := fmt.Sprintf("gui/%d/%s", os.Getuid(), launchdLabel)
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	if err := bootoutThenBootstrap(ctx, restartCommand, "restart", target, domain, plistPath); err != nil {
		slog.ErrorContext(ctx, "restart launchd failed", "err", err)
		return true, err
	}
	slog.InfoContext(ctx, "launchd service restarted", "plist", plistPath)
	return true, nil
}

func restartSystemd(ctx context.Context, home string) (bool, error) {
	unitPath := filepath.Join(home, ".config", "systemd", "user", systemdUnit)
	if _, err := restartStat(unitPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("restart: stat unit %s: %w", unitPath, err)
	}
	if out, err := restartCommand(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		slog.ErrorContext(ctx, "systemctl daemon-reload failed", "err", err, "out", string(out))
		return true, fmt.Errorf("restart: systemctl daemon-reload: %w", err)
	}
	if out, err := restartCommand(ctx, "systemctl", "--user", "restart", systemdUnit); err != nil {
		slog.ErrorContext(ctx, "systemctl restart failed", "err", err, "out", string(out))
		return true, fmt.Errorf("restart: systemctl restart: %w", err)
	}
	slog.InfoContext(ctx, "systemd service restarted", "unit", unitPath)
	return true, nil
}
