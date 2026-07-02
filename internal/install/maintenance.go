package install

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// maintenanceScriptTemplate is the embedded shell script template.
//
//go:embed templates/maintenance.sh.tmpl
var maintenanceScriptTemplate string

// maintenancePlistTemplate is the embedded launchd plist template.
//
//go:embed templates/maintenance.plist.tmpl
var maintenancePlistTemplate string

const (
	maintenanceLaunchdLabel = "io.goodkind.gha-mac-broker-maintenance"
	maintenanceScriptMode   = 0o755
)

type maintenanceTemplateData struct {
	Label           string
	Command         string
	ScriptPath      string
	LogPath         string
	IntervalSeconds int
}

func installMaintenanceTimer(ctx context.Context, cfg Config) error {
	if cfg.Maintenance.Command == "" {
		return nil
	}
	if runtime.GOOS != osDarwin {
		slog.DebugContext(ctx, "maintenance timer skipped on non-darwin OS", "os", runtime.GOOS)
		return nil
	}
	preflightMaintenanceCommand(ctx, cfg.Maintenance.Command)
	return installLaunchdMaintenance(ctx, cfg)
}

func installLaunchdMaintenance(ctx context.Context, cfg Config) error {
	scriptPath := maintenanceScriptPath(cfg)
	if err := os.MkdirAll(filepath.Dir(scriptPath), unitDirMode); err != nil {
		slog.ErrorContext(ctx, "create maintenance script dir failed", "err", err, "path", scriptPath)
		return fmt.Errorf("install: create maintenance script dir: %w", err)
	}
	data := maintenanceData(cfg)
	script, err := renderTemplate(ctx, "maintenance script", maintenanceScriptTemplate, data)
	if err != nil {
		return err
	}
	if err := os.WriteFile(scriptPath, script, maintenanceScriptMode); err != nil {
		slog.ErrorContext(ctx, "write maintenance script failed", "err", err, "path", scriptPath)
		return fmt.Errorf("install: write maintenance script %s: %w", scriptPath, err)
	}

	plistPath := maintenancePlistPath(cfg)
	if err := os.MkdirAll(filepath.Dir(plistPath), unitDirMode); err != nil {
		slog.ErrorContext(ctx, "create maintenance LaunchAgents dir failed", "err", err, "path", plistPath)
		return fmt.Errorf("install: create maintenance LaunchAgents dir: %w", err)
	}
	logPath := maintenanceLogPath(cfg)
	if err := os.MkdirAll(filepath.Dir(logPath), unitDirMode); err != nil {
		slog.ErrorContext(ctx, "create maintenance log dir failed", "err", err, "path", logPath)
		return fmt.Errorf("install: create maintenance log dir: %w", err)
	}
	if err := touchFile(ctx, logPath); err != nil {
		return err
	}
	plist, err := renderTemplate(ctx, "maintenance plist", maintenancePlistTemplate, data)
	if err != nil {
		return err
	}
	if err := os.WriteFile(plistPath, plist, unitFileMode); err != nil {
		slog.ErrorContext(ctx, "write maintenance plist failed", "err", err, "path", plistPath)
		return fmt.Errorf("install: write maintenance plist %s: %w", plistPath, err)
	}

	target := fmt.Sprintf("gui/%d/%s", os.Getuid(), maintenanceLaunchdLabel)
	if out, err := commandRunner(ctx, "launchctl", "bootout", target).CombinedOutput(); err != nil {
		slog.DebugContext(ctx, "launchctl maintenance bootout ignored (likely not loaded)", "err", err, "out", string(out))
	}
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	if out, err := commandRunner(ctx, "launchctl", "bootstrap", domain, plistPath).CombinedOutput(); err != nil {
		slog.ErrorContext(ctx, "launchctl maintenance bootstrap failed", "err", err, "out", string(out))
		return fmt.Errorf("install: launchctl maintenance bootstrap: %w", err)
	}
	slog.InfoContext(ctx, "launchd maintenance timer installed", "plist", plistPath)
	return nil
}

func uninstallMaintenanceTimer(ctx context.Context, cfg Config) error {
	if runtime.GOOS != osDarwin {
		slog.DebugContext(ctx, "maintenance timer uninstall skipped on non-darwin OS", "os", runtime.GOOS)
		return nil
	}
	target := fmt.Sprintf("gui/%d/%s", os.Getuid(), maintenanceLaunchdLabel)
	if out, err := commandRunner(ctx, "launchctl", "bootout", target).CombinedOutput(); err != nil {
		slog.WarnContext(ctx, "launchctl maintenance bootout ignored (likely not loaded)", "err", err, "out", string(out))
	}
	plistPath := maintenancePlistPath(cfg)
	if err := os.Remove(plistPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.ErrorContext(ctx, "remove maintenance plist failed", "err", err, "path", plistPath)
		return fmt.Errorf("install: remove maintenance plist %s: %w", plistPath, err)
	}
	scriptPath := maintenanceScriptPath(cfg)
	if err := os.Remove(scriptPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.ErrorContext(ctx, "remove maintenance script failed", "err", err, "path", scriptPath)
		return fmt.Errorf("install: remove maintenance script %s: %w", scriptPath, err)
	}
	slog.InfoContext(ctx, "launchd maintenance timer uninstalled", "plist", plistPath, "script", scriptPath)
	return nil
}

func preflightMaintenanceCommand(ctx context.Context, commandLine string) {
	fields := strings.Fields(commandLine)
	if len(fields) == 0 {
		return
	}
	binary := fields[0]
	if _, err := exec.LookPath(binary); err != nil {
		slog.WarnContext(ctx, "maintenance command binary not found on PATH; continuing", "err", err, "binary", binary)
	}
}

func maintenanceData(cfg Config) maintenanceTemplateData {
	return maintenanceTemplateData{
		Label:           maintenanceLaunchdLabel,
		Command:         cfg.Maintenance.Command,
		ScriptPath:      maintenanceScriptPath(cfg),
		LogPath:         maintenanceLogPath(cfg),
		IntervalSeconds: cfg.Maintenance.IntervalSeconds,
	}
}

func maintenanceScriptPath(cfg Config) string {
	return filepath.Join(cfg.Home, ".config", "gha-mac-broker", "maintenance.sh")
}

func maintenancePlistPath(cfg Config) string {
	return filepath.Join(cfg.Home, "Library", "LaunchAgents", maintenanceLaunchdLabel+".plist")
}

func maintenanceLogPath(cfg Config) string {
	return filepath.Join(cfg.Home, "Library", "Logs", "gha-mac-broker-maintenance.log")
}
