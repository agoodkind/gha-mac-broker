package install

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"os"
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
	Home            string
	Path            string
}

func installMaintenanceTimer(ctx context.Context, cfg Config) error {
	if runtime.GOOS != osDarwin {
		slog.DebugContext(ctx, "maintenance timer skipped on non-darwin OS", "os", runtime.GOOS)
		return nil
	}
	if cfg.Maintenance.Command == "" {
		// An empty command disables maintenance. Actively remove any previously
		// installed timer so re-running install after disabling does not leave a
		// stale job loaded. The darwin gate above already passed, so call the
		// worker directly.
		return uninstallLaunchdMaintenance(ctx, cfg)
	}
	preflightMaintenanceCommand(ctx, cfg.Maintenance.Command, maintenancePath(cfg))
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
	return uninstallLaunchdMaintenance(ctx, cfg)
}

// uninstallLaunchdMaintenance boots out and removes the launchd job. It carries
// no OS gate so tests exercise it on any platform through the mocked
// commandRunner; the production callers apply the darwin gate.
func uninstallLaunchdMaintenance(ctx context.Context, cfg Config) error {
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

func preflightMaintenanceCommand(ctx context.Context, commandLine, searchPath string) {
	binary := commandBinary(commandLine)
	if binary == "" {
		return
	}
	if !commandBinaryOnPath(binary, searchPath) {
		slog.WarnContext(ctx, "maintenance command binary not found on the launchd PATH; continuing",
			"binary", binary, "path", searchPath)
	}
}

// commandBinary returns the executable a shell command line runs, skipping a
// leading `env` and any NAME=VALUE assignments (e.g. `env FOO=1 swift-mk ...` or
// `FOO=1 swift-mk ...`), so the preflight checks the real binary.
func commandBinary(commandLine string) string {
	for field := range strings.FieldsSeq(commandLine) {
		if field == "env" || isEnvAssignment(field) {
			continue
		}
		return field
	}
	return ""
}

// isEnvAssignment reports whether field is a shell NAME=VALUE assignment (a
// name of word characters before '=', and no '/', which would mark a path).
func isEnvAssignment(field string) bool {
	eq := strings.IndexByte(field, '=')
	if eq <= 0 || strings.IndexByte(field[:eq], '/') >= 0 {
		return false
	}
	for _, r := range field[:eq] {
		if !isEnvNameChar(r) {
			return false
		}
	}
	return true
}

func isEnvNameChar(r rune) bool {
	return r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

// commandBinaryOnPath reports whether binary is an executable reachable via
// searchPath, the same colon-separated PATH the launchd job runs with. Checking
// against that PATH rather than the installer's own avoids a false warning for a
// binary in ~/.local/bin and catches one missing from the job's actual PATH.
func commandBinaryOnPath(binary, searchPath string) bool {
	if strings.Contains(binary, "/") {
		return isExecutableFile(binary)
	}
	for dir := range strings.SplitSeq(searchPath, ":") {
		if dir == "" {
			continue
		}
		if isExecutableFile(filepath.Join(dir, binary)) {
			return true
		}
	}
	return false
}

func isExecutableFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && info.Mode()&0o111 != 0
}

func maintenanceData(cfg Config) maintenanceTemplateData {
	return maintenanceTemplateData{
		Label:           maintenanceLaunchdLabel,
		Command:         cfg.Maintenance.Command,
		ScriptPath:      maintenanceScriptPath(cfg),
		LogPath:         maintenanceLogPath(cfg),
		IntervalSeconds: cfg.Maintenance.IntervalSeconds,
		Home:            cfg.Home,
		Path:            maintenancePath(cfg),
	}
}

// maintenancePath is the PATH the launchd job runs with. launchd starts with a
// minimal PATH, so a command installed into ~/.local/bin (where install.sh puts
// swift-mk) would otherwise fail with "command not found". The user bin dirs go
// first, then the common Homebrew and system locations.
func maintenancePath(cfg Config) string {
	return strings.Join([]string{
		filepath.Join(cfg.Home, ".local", "bin"),
		"/opt/homebrew/bin",
		"/usr/local/bin",
		"/usr/bin",
		"/bin",
		"/usr/sbin",
		"/sbin",
	}, ":")
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
