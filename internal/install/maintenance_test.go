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
	calls                []recordedCommand
	launchctlPrintErrors []error
	launchctlPrintCount  int
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
	if name == "launchctl" && len(args) > 0 && args[0] == "print" {
		printIndex := r.launchctlPrintCount
		r.launchctlPrintCount++
		if printIndex < len(r.launchctlPrintErrors) {
			err := r.launchctlPrintErrors[printIndex]
			if err != nil {
				return combinedOutputFunc(func() ([]byte, error) {
					return []byte("not loaded"), err
				})
			}
			return combinedOutputFunc(func() ([]byte, error) {
				return []byte("loaded"), nil
			})
		}
		return combinedOutputFunc(func() ([]byte, error) {
			return []byte("not loaded"), errors.New("not loaded")
		})
	}
	return combinedOutputFunc(func() ([]byte, error) {
		return []byte("ok"), nil
	})
}

// installLaunchdMaintenance is the un-gated worker, so this exercises render,
// file-write, and the launchctl calls (mocked) on any platform, including the
// Ubuntu CI runner. The runtime.GOOS gate lives only in the production dispatch.
func TestInstallLaunchdMaintenanceRendersLaunchdFiles(t *testing.T) {
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

	if err := installLaunchdMaintenance(context.Background(), cfg); err != nil {
		t.Fatalf("installLaunchdMaintenance: %v", err)
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
		// The launchd job runs in a minimal env, so the plist must set PATH (with
		// the user bin dir where swift-mk installs) and HOME.
		"<key>EnvironmentVariables</key>",
		"<key>PATH</key>",
		"<string>" + filepath.Join(home, ".local", "bin") + ":",
		"<key>HOME</key>",
		"<string>" + home + "</string>",
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

func TestCommandBinarySkipsEnvAssignments(t *testing.T) {
	cases := map[string]string{
		"swift-mk cache prune":         "swift-mk",
		"FOO=1 BAR=2 swift-mk prune":   "swift-mk",
		"env FOO=1 swift-mk prune":     "swift-mk",
		"/usr/local/bin/swift-mk prct": "/usr/local/bin/swift-mk",
		"":                             "",
		"   ":                          "",
	}
	for input, want := range cases {
		if got := commandBinary(input); got != want {
			t.Errorf("commandBinary(%q) = %q, want %q", input, got, want)
		}
	}
}

// uninstallLaunchdMaintenance is un-gated, so the boot-out and file removal run
// on any platform, including the Ubuntu CI runner, with launchctl mocked.
func TestUninstallLaunchdMaintenanceRemovesFiles(t *testing.T) {
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
	t.Cleanup(replaceCommandRunner(recorder.build))

	if err := installLaunchdMaintenance(context.Background(), cfg); err != nil {
		t.Fatalf("installLaunchdMaintenance: %v", err)
	}
	if err := uninstallLaunchdMaintenance(context.Background(), cfg); err != nil {
		t.Fatalf("uninstallLaunchdMaintenance: %v", err)
	}
	for _, path := range []string{maintenanceScriptPath(cfg), maintenancePlistPath(cfg)} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat %s err = %v, want not exist", path, err)
		}
	}
	var bootouts int
	for _, call := range recorder.calls {
		if call.name == "launchctl" && len(call.args) > 0 && call.args[0] == "bootout" {
			bootouts++
		}
	}
	if bootouts < 1 {
		t.Fatalf("expected at least one launchctl bootout, calls = %#v", recorder.calls)
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
