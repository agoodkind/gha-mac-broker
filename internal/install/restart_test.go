package install

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type capturedServiceCommand struct {
	name string
	args []string
}

func TestRestartLaunchdBootsOutAndBootstrapsExistingPlist(t *testing.T) {
	home := t.TempDir()
	plistPath := filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist")
	if err := os.MkdirAll(filepath.Dir(plistPath), 0o755); err != nil {
		t.Fatalf("create plist dir: %v", err)
	}
	if err := os.WriteFile(plistPath, []byte("plist"), 0o644); err != nil {
		t.Fatalf("write plist: %v", err)
	}
	var commands []capturedServiceCommand
	withRestartTestHooks(t, "darwin", home, func(_ context.Context, name string, args ...string) ([]byte, error) {
		commands = append(commands, capturedServiceCommand{name: name, args: append([]string(nil), args...)})
		if name == "launchctl" && len(args) > 0 && args[0] == "print" {
			return []byte("not loaded"), errors.New("not loaded")
		}
		return nil, nil
	})

	restarted, err := Restart(context.Background())
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if !restarted {
		t.Fatal("Restart returned restarted=false, want true")
	}

	domain := fmt.Sprintf("gui/%d", os.Getuid())
	target := domain + "/" + launchdLabel
	want := []capturedServiceCommand{
		{name: "launchctl", args: []string{"bootout", target}},
		{name: "launchctl", args: []string{"print", target}},
		{name: "launchctl", args: []string{"bootstrap", domain, plistPath}},
	}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands = %#v, want %#v", commands, want)
	}
}

func TestRestartSystemdReloadsAndRestartsExistingUnit(t *testing.T) {
	home := t.TempDir()
	unitPath := filepath.Join(home, ".config", "systemd", "user", systemdUnit)
	if err := os.MkdirAll(filepath.Dir(unitPath), 0o755); err != nil {
		t.Fatalf("create unit dir: %v", err)
	}
	if err := os.WriteFile(unitPath, []byte("unit"), 0o644); err != nil {
		t.Fatalf("write unit: %v", err)
	}
	var commands []capturedServiceCommand
	withRestartTestHooks(t, "linux", home, func(_ context.Context, name string, args ...string) ([]byte, error) {
		commands = append(commands, capturedServiceCommand{name: name, args: append([]string(nil), args...)})
		return nil, nil
	})

	restarted, err := Restart(context.Background())
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if !restarted {
		t.Fatal("Restart returned restarted=false, want true")
	}

	want := []capturedServiceCommand{
		{name: "systemctl", args: []string{"--user", "daemon-reload"}},
		{name: "systemctl", args: []string{"--user", "restart", systemdUnit}},
	}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands = %#v, want %#v", commands, want)
	}
}

func withRestartTestHooks(t *testing.T, goos string, home string, runner serviceCommandRunner) {
	t.Helper()
	oldRuntimeOS := restartRuntimeOS
	oldUserHome := restartUserHome
	oldCommand := restartCommand
	restartRuntimeOS = func() string { return goos }
	restartUserHome = func() (string, error) { return home, nil }
	restartCommand = runner
	t.Cleanup(func() {
		restartRuntimeOS = oldRuntimeOS
		restartUserHome = oldUserHome
		restartCommand = oldCommand
	})
}
