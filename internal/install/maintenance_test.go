package install

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"goodkind.io/gha-mac-broker/internal/config"
)

type recordingCommandRunner struct {
	calls []recordedCommand
}

type recordedCommand struct {
	name string
	args []string
}

type combinedOutputFunc func() ([]byte, error)

func (f combinedOutputFunc) CombinedOutput() ([]byte, error) {
	return f()
}

func replaceCommandRunner(runner func(context.Context, string, ...string) combinedOutputRunner) func() {
	original := commandRunner
	commandRunner = runner
	return func() {
		commandRunner = original
	}
}

func (r *recordingCommandRunner) build(_ context.Context, name string, args ...string) combinedOutputRunner {
	copiedArgs := append([]string(nil), args...)
	r.calls = append(r.calls, recordedCommand{name: name, args: copiedArgs})
	if name == "launchctl" && len(args) > 0 && args[0] == "bootout" {
		return combinedOutputFunc(func() ([]byte, error) {
			return []byte("not loaded"), errors.New("not loaded")
		})
	}
	return combinedOutputFunc(func() ([]byte, error) {
		return []byte("ok"), nil
	})
}

func TestInstallMaintenanceTimerRendersLaunchdFiles(t *testing.T) {
	if runtime.GOOS != osDarwin {
		t.Skipf("launchd maintenance installer is darwin-only, got %s", runtime.GOOS)
	}
	home := t.TempDir()
	cfg := sampleConfig()
	cfg.Home = home
	cfg.ConfigDir = filepath.Join(home, ".config", "gha-mac-broker")
	cfg.ConfigPath = filepath.Join(cfg.ConfigDir, "config.toml")
	cfg.LogPath = filepath.Join(home, "Library", "Logs", "gha-mac-broker.log")
	cfg.Maintenance = config.MaintenanceConfig{
		Command:         "printf maintenance-test",
		IntervalSeconds: 900,
	}
	recorder := &recordingCommandRunner{calls: nil}
	restore := replaceCommandRunner(recorder.build)
	t.Cleanup(restore)

	if err := installMaintenanceTimer(context.Background(), cfg); err != nil {
		t.Fatalf("installMaintenanceTimer: %v", err)
	}

	scriptPath := maintenanceScriptPath(cfg)
	script, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read maintenance script: %v", err)
	}
	scriptText := string(script)
	for _, want := range []string{
		"#!/usr/bin/env bash\n",
		"set -euo pipefail",
		"printf maintenance-test",
	} {
		if !strings.Contains(scriptText, want) {
			t.Errorf("maintenance script missing %q\n%s", want, scriptText)
		}
	}

	plistPath := maintenancePlistPath(cfg)
	plist, err := os.ReadFile(plistPath)
	if err != nil {
		t.Fatalf("read maintenance plist: %v", err)
	}
	plistText := string(plist)
	for _, want := range []string{
		"<string>io.goodkind.gha-mac-broker-maintenance</string>",
		"<string>/bin/bash</string>",
		"<string>" + scriptPath + "</string>",
		"<integer>900</integer>",
		"<string>" + filepath.Join(home, "Library", "Logs", "gha-mac-broker-maintenance.log") + "</string>",
	} {
		if !strings.Contains(plistText, want) {
			t.Errorf("maintenance plist missing %q\n%s", want, plistText)
		}
	}
	if len(recorder.calls) != 2 {
		t.Fatalf("launchctl calls = %d, want 2", len(recorder.calls))
	}
	if recorder.calls[0].name != "launchctl" || recorder.calls[0].args[0] != "bootout" {
		t.Fatalf("first command = %#v, want launchctl bootout", recorder.calls[0])
	}
	if recorder.calls[1].name != "launchctl" || recorder.calls[1].args[0] != "bootstrap" {
		t.Fatalf("second command = %#v, want launchctl bootstrap", recorder.calls[1])
	}
}

func TestInstallMaintenanceTimerSkipsEmptyCommand(t *testing.T) {
	cfg := sampleConfig()
	cfg.Home = t.TempDir()
	cfg.Maintenance = config.MaintenanceConfig{
		Command:         "",
		IntervalSeconds: 3600,
	}
	restore := replaceCommandRunner(func(_ context.Context, name string, args ...string) combinedOutputRunner {
		t.Fatalf("unexpected command %s %v", name, args)
		return combinedOutputFunc(func() ([]byte, error) {
			return nil, nil
		})
	})
	t.Cleanup(restore)

	if err := installMaintenanceTimer(context.Background(), cfg); err != nil {
		t.Fatalf("installMaintenanceTimer: %v", err)
	}
	if _, err := os.Stat(maintenanceScriptPath(cfg)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("maintenance script stat err = %v, want not exist", err)
	}
}

func TestUninstallRemovesMaintenanceFiles(t *testing.T) {
	if runtime.GOOS != osDarwin {
		t.Skipf("launchd maintenance uninstall is darwin-only, got %s", runtime.GOOS)
	}
	home := t.TempDir()
	cfg := sampleConfig()
	cfg.Home = home
	cfg.ConfigDir = filepath.Join(home, ".config", "gha-mac-broker")
	cfg.ConfigPath = filepath.Join(cfg.ConfigDir, "config.toml")
	cfg.LogPath = filepath.Join(home, "Library", "Logs", "gha-mac-broker.log")
	if err := os.MkdirAll(filepath.Dir(maintenanceScriptPath(cfg)), 0o755); err != nil {
		t.Fatalf("mkdir maintenance script dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(maintenancePlistPath(cfg)), 0o755); err != nil {
		t.Fatalf("mkdir maintenance plist dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(filepath.Join(cfg.Home, "Library", "LaunchAgents", launchdLabel+".plist")), 0o755); err != nil {
		t.Fatalf("mkdir service plist dir: %v", err)
	}
	if err := os.WriteFile(maintenanceScriptPath(cfg), []byte("script"), 0o755); err != nil {
		t.Fatalf("write maintenance script: %v", err)
	}
	if err := os.WriteFile(maintenancePlistPath(cfg), []byte("plist"), 0o644); err != nil {
		t.Fatalf("write maintenance plist: %v", err)
	}
	servicePlist := filepath.Join(cfg.Home, "Library", "LaunchAgents", launchdLabel+".plist")
	if err := os.WriteFile(servicePlist, []byte("plist"), 0o644); err != nil {
		t.Fatalf("write service plist: %v", err)
	}
	recorder := &recordingCommandRunner{calls: nil}
	restore := replaceCommandRunner(recorder.build)
	t.Cleanup(restore)

	if err := Uninstall(context.Background(), cfg); err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	for _, path := range []string{maintenanceScriptPath(cfg), maintenancePlistPath(cfg), servicePlist} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat %s err = %v, want not exist", path, err)
		}
	}
	if len(recorder.calls) != 2 {
		t.Fatalf("launchctl calls = %d, want 2", len(recorder.calls))
	}
}
