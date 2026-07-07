package install

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"text/template"
	"time"
)

// launchdTemplate is the embedded macOS launchd plist template.
//
//go:embed templates/launchd.plist.tmpl
var launchdTemplate string

// systemdTemplate is the embedded Linux systemd user unit template.
//
//go:embed templates/systemd.service.tmpl
var systemdTemplate string

const (
	// launchdLabel is the launchd job label on macOS.
	launchdLabel = "io.goodkind.gha-mac-broker"
	// systemdUnit is the systemd user unit name on Linux.
	systemdUnit = "gha-mac-broker.service"
	// unitDirMode is the mode for service-unit parent directories.
	unitDirMode = 0o755
	// unitFileMode is the mode for the written service unit.
	unitFileMode = 0o644
	// logFileMode is the mode for the touched launchd log file.
	logFileMode               = 0o644
	launchdUnloadPollLimit    = 50
	launchdUnloadPollInterval = 200 * time.Millisecond
	// osDarwin and osLinux are the supported [runtime.GOOS] values.
	osDarwin = "darwin"
	osLinux  = "linux"
)

var launchdPollSleep = time.Sleep

type templateData interface {
	Config | maintenanceTemplateData
}

func renderTemplate[T templateData](ctx context.Context, name string, templateText string, data T) ([]byte, error) {
	tmpl, err := template.New(name).Parse(templateText)
	if err != nil {
		slog.ErrorContext(ctx, "parse template failed", "err", err, "template", name)
		return nil, fmt.Errorf("install: parse %s template: %w", name, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		slog.ErrorContext(ctx, "execute template failed", "err", err, "template", name)
		return nil, fmt.Errorf("install: execute %s template: %w", name, err)
	}
	return buf.Bytes(), nil
}

// renderUnit renders a service-unit template with cfg.
func renderUnit(ctx context.Context, templateText string, cfg Config) ([]byte, error) {
	return renderTemplate(ctx, "unit", templateText, cfg)
}

// installUnit renders and bootstraps the OS-appropriate service unit.
func installUnit(ctx context.Context, cfg Config) error {
	switch runtime.GOOS {
	case osDarwin:
		return installLaunchd(ctx, cfg)
	case osLinux:
		return installSystemd(ctx, cfg)
	default:
		slog.ErrorContext(ctx, "unsupported OS", "err", errors.ErrUnsupported, "os", runtime.GOOS)
		return fmt.Errorf("install: unsupported OS %q: %w", runtime.GOOS, errors.ErrUnsupported)
	}
}

// uninstallUnit boots out and removes the OS-appropriate service unit.
func uninstallUnit(ctx context.Context, cfg Config) error {
	switch runtime.GOOS {
	case osDarwin:
		return uninstallLaunchd(ctx, cfg)
	case osLinux:
		return uninstallSystemd(ctx, cfg)
	default:
		slog.ErrorContext(ctx, "unsupported OS", "err", errors.ErrUnsupported, "os", runtime.GOOS)
		return fmt.Errorf("install: unsupported OS %q: %w", runtime.GOOS, errors.ErrUnsupported)
	}
}

// installLaunchd writes the plist, creates the LaunchAgents and log dirs,
// touches the log file, then boots out (ignoring not-loaded) and bootstraps.
func installLaunchd(ctx context.Context, cfg Config) error {
	plistPath := filepath.Join(cfg.Home, "Library", "LaunchAgents", launchdLabel+".plist")
	if err := os.MkdirAll(filepath.Dir(plistPath), unitDirMode); err != nil {
		slog.ErrorContext(ctx, "create LaunchAgents dir failed", "err", err)
		return fmt.Errorf("install: create LaunchAgents dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.LogPath), unitDirMode); err != nil {
		slog.ErrorContext(ctx, "create log dir failed", "err", err)
		return fmt.Errorf("install: create log dir: %w", err)
	}
	if err := touchFile(ctx, cfg.LogPath); err != nil {
		return err
	}
	rendered, err := renderUnit(ctx, launchdTemplate, cfg)
	if err != nil {
		return err
	}
	if err := os.WriteFile(plistPath, rendered, unitFileMode); err != nil {
		slog.ErrorContext(ctx, "write plist failed", "err", err, "path", plistPath)
		return fmt.Errorf("install: write plist %s: %w", plistPath, err)
	}

	target := fmt.Sprintf("gui/%d/%s", os.Getuid(), launchdLabel)
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	installCommand := func(ctx context.Context, name string, args ...string) ([]byte, error) {
		return commandRunner(ctx, name, args...).CombinedOutput()
	}
	if err := bootoutThenBootstrap(ctx, installCommand, "install", target, domain, plistPath); err != nil {
		return err
	}
	slog.InfoContext(ctx, "launchd service installed", "plist", plistPath)
	return nil
}

func bootoutThenBootstrap(
	ctx context.Context,
	runner serviceCommandRunner,
	errorPrefix string,
	target string,
	domain string,
	plistPath string,
) error {
	if out, err := runner(ctx, "launchctl", "bootout", target); err != nil {
		slog.DebugContext(ctx, "launchctl bootout ignored (likely not loaded)", "err", err, "out", string(out))
	}
	waitForLaunchdUnload(ctx, runner, target)
	if out, err := runner(ctx, "launchctl", "bootstrap", domain, plistPath); err != nil {
		slog.ErrorContext(ctx, "launchctl bootstrap failed", "err", err, "out", string(out))
		return fmt.Errorf("%s: launchctl bootstrap: %w", errorPrefix, err)
	}
	return nil
}

func waitForLaunchdUnload(ctx context.Context, runner serviceCommandRunner, target string) {
	for pollIndex := range launchdUnloadPollLimit {
		out, err := runner(ctx, "launchctl", "print", target)
		if err != nil {
			slog.DebugContext(ctx, "launchd service unloaded", "target", target, "out", string(out))
			return
		}
		if pollIndex == launchdUnloadPollLimit-1 {
			slog.WarnContext(ctx, "launchd service still loaded after bootout wait", "target", target, "out", string(out))
			return
		}
		launchdPollSleep(launchdUnloadPollInterval)
	}
}

// installSystemd writes the user unit, reloads the daemon, and enables it.
func installSystemd(ctx context.Context, cfg Config) error {
	unitPath := filepath.Join(cfg.Home, ".config", "systemd", "user", systemdUnit)
	if err := os.MkdirAll(filepath.Dir(unitPath), unitDirMode); err != nil {
		slog.ErrorContext(ctx, "create systemd user dir failed", "err", err)
		return fmt.Errorf("install: create systemd user dir: %w", err)
	}
	rendered, err := renderUnit(ctx, systemdTemplate, cfg)
	if err != nil {
		return err
	}
	if err := os.WriteFile(unitPath, rendered, unitFileMode); err != nil {
		slog.ErrorContext(ctx, "write unit failed", "err", err, "path", unitPath)
		return fmt.Errorf("install: write unit %s: %w", unitPath, err)
	}

	if out, err := commandRunner(ctx, "systemctl", "--user", "daemon-reload").CombinedOutput(); err != nil {
		slog.ErrorContext(ctx, "systemctl daemon-reload failed", "err", err, "out", string(out))
		return fmt.Errorf("install: systemctl daemon-reload: %w", err)
	}
	if out, err := commandRunner(ctx, "systemctl", "--user", "enable", "--now", systemdUnit).CombinedOutput(); err != nil {
		slog.ErrorContext(ctx, "systemctl enable --now failed", "err", err, "out", string(out))
		return fmt.Errorf("install: systemctl enable --now: %w", err)
	}
	slog.InfoContext(ctx, "systemd service installed", "unit", unitPath)
	return nil
}

// uninstallLaunchd boots out the job (ignoring not-loaded) and removes the plist.
func uninstallLaunchd(ctx context.Context, cfg Config) error {
	target := fmt.Sprintf("gui/%d/%s", os.Getuid(), launchdLabel)
	if out, err := commandRunner(ctx, "launchctl", "bootout", target).CombinedOutput(); err != nil {
		slog.WarnContext(ctx, "launchctl bootout ignored (likely not loaded)", "err", err, "out", string(out))
	}
	plistPath := filepath.Join(cfg.Home, "Library", "LaunchAgents", launchdLabel+".plist")
	if err := os.Remove(plistPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.ErrorContext(ctx, "remove plist failed", "err", err, "path", plistPath)
		return fmt.Errorf("install: remove plist %s: %w", plistPath, err)
	}
	slog.InfoContext(ctx, "launchd service uninstalled", "plist", plistPath)
	return nil
}

// uninstallSystemd disables the unit (ignoring not-loaded) and removes the file.
func uninstallSystemd(ctx context.Context, cfg Config) error {
	if out, err := commandRunner(ctx, "systemctl", "--user", "disable", "--now", systemdUnit).CombinedOutput(); err != nil {
		slog.WarnContext(ctx, "systemctl disable --now ignored (likely not loaded)", "err", err, "out", string(out))
	}
	unitPath := filepath.Join(cfg.Home, ".config", "systemd", "user", systemdUnit)
	if err := os.Remove(unitPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.ErrorContext(ctx, "remove unit failed", "err", err, "path", unitPath)
		return fmt.Errorf("install: remove unit %s: %w", unitPath, err)
	}
	slog.InfoContext(ctx, "systemd service uninstalled", "unit", unitPath)
	return nil
}

// touchFile creates path if absent so launchd has a log target to open.
func touchFile(ctx context.Context, path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, logFileMode)
	if err != nil {
		slog.ErrorContext(ctx, "touch log file failed", "err", err, "path", path)
		return fmt.Errorf("install: touch %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		slog.ErrorContext(ctx, "close log file failed", "err", err, "path", path)
		return fmt.Errorf("install: close %s: %w", path, err)
	}
	return nil
}
