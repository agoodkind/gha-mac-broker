package install

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"goodkind.io/gha-mac-broker/internal/config"
)

func sampleConfig() Config {
	return Config{
		BinPath:    "/Users/test/.local/bin/gha-mac-broker",
		Home:       "/Users/test",
		ConfigDir:  "/Users/test/.config/gha-mac-broker",
		LogPath:    "/Users/test/Library/Logs/gha-mac-broker.log",
		ConfigPath: "/Users/test/.config/gha-mac-broker/config.toml",
		Maintenance: config.MaintenanceConfig{
			Command:         "",
			IntervalSeconds: 0,
		},
	}
}

func TestRenderLaunchd(t *testing.T) {
	cfg := sampleConfig()
	out, err := renderUnit(context.Background(), launchdTemplate, cfg)
	if err != nil {
		t.Fatalf("renderUnit launchd: %v", err)
	}
	rendered := string(out)
	for _, want := range []string{cfg.BinPath, cfg.ConfigPath, cfg.LogPath} {
		if !strings.Contains(rendered, want) {
			t.Errorf("launchd output missing %q\n%s", want, rendered)
		}
	}
	assertNoMarkers(t, rendered)
}

func TestRenderSystemd(t *testing.T) {
	cfg := sampleConfig()
	out, err := renderUnit(context.Background(), systemdTemplate, cfg)
	if err != nil {
		t.Fatalf("renderUnit systemd: %v", err)
	}
	rendered := string(out)
	for _, want := range []string{cfg.BinPath, cfg.ConfigPath} {
		if !strings.Contains(rendered, want) {
			t.Errorf("systemd output missing %q\n%s", want, rendered)
		}
	}
	assertNoMarkers(t, rendered)
}

func TestInstallLaunchdBootsOutBeforeBootstrap(t *testing.T) {
	t.Run("waits for running service to unload", func(t *testing.T) {
		home := t.TempDir()
		cfg := sampleConfig()
		cfg.Home = home
		cfg.ConfigDir = filepath.Join(home, ".config", "gha-mac-broker")
		cfg.ConfigPath = filepath.Join(cfg.ConfigDir, "config.toml")
		cfg.LogPath = filepath.Join(home, "Library", "Logs", "gha-mac-broker.log")
		notLoadedErr := errors.New("not loaded")
		recorder := &recordingCommandRunner{
			calls:                nil,
			launchctlPrintErrors: []error{nil, notLoadedErr},
		}
		t.Cleanup(replaceCommandRunner(recorder.build))
		var sleeps []time.Duration
		t.Cleanup(replaceLaunchdPollSleep(func(duration time.Duration) {
			sleeps = append(sleeps, duration)
		}))

		if err := installLaunchd(context.Background(), cfg); err != nil {
			t.Fatalf("installLaunchd: %v", err)
		}

		plistPath := filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist")
		domain := fmt.Sprintf("gui/%d", os.Getuid())
		target := domain + "/" + launchdLabel
		want := []recordedCommand{
			{name: "launchctl", args: []string{"bootout", target}},
			{name: "launchctl", args: []string{"print", target}},
			{name: "launchctl", args: []string{"print", target}},
			{name: "launchctl", args: []string{"bootstrap", domain, plistPath}},
		}
		if !reflect.DeepEqual(recorder.calls, want) {
			t.Fatalf("launchctl calls = %#v, want %#v", recorder.calls, want)
		}
		if !reflect.DeepEqual(sleeps, []time.Duration{launchdUnloadPollInterval}) {
			t.Fatalf("poll sleeps = %#v, want %#v", sleeps, []time.Duration{launchdUnloadPollInterval})
		}
	})

	t.Run("bootstraps when service is already unloaded", func(t *testing.T) {
		home := t.TempDir()
		cfg := sampleConfig()
		cfg.Home = home
		cfg.ConfigDir = filepath.Join(home, ".config", "gha-mac-broker")
		cfg.ConfigPath = filepath.Join(cfg.ConfigDir, "config.toml")
		cfg.LogPath = filepath.Join(home, "Library", "Logs", "gha-mac-broker.log")
		notLoadedErr := errors.New("not loaded")
		recorder := &recordingCommandRunner{
			calls:                nil,
			launchctlPrintErrors: []error{notLoadedErr},
		}
		t.Cleanup(replaceCommandRunner(recorder.build))
		var sleeps []time.Duration
		t.Cleanup(replaceLaunchdPollSleep(func(duration time.Duration) {
			sleeps = append(sleeps, duration)
		}))

		if err := installLaunchd(context.Background(), cfg); err != nil {
			t.Fatalf("installLaunchd: %v", err)
		}

		plistPath := filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist")
		domain := fmt.Sprintf("gui/%d", os.Getuid())
		target := domain + "/" + launchdLabel
		want := []recordedCommand{
			{name: "launchctl", args: []string{"bootout", target}},
			{name: "launchctl", args: []string{"print", target}},
			{name: "launchctl", args: []string{"bootstrap", domain, plistPath}},
		}
		if !reflect.DeepEqual(recorder.calls, want) {
			t.Fatalf("launchctl calls = %#v, want %#v", recorder.calls, want)
		}
		if len(sleeps) != 0 {
			t.Fatalf("poll sleeps = %#v, want none", sleeps)
		}
	})
}

func replaceLaunchdPollSleep(sleep func(time.Duration)) func() {
	original := launchdPollSleep
	launchdPollSleep = sleep
	return func() {
		launchdPollSleep = original
	}
}

// TestEmbeddedConfigMatchesRepoRoot guards against the embedded scaffold copy
// drifting from the repo-root config.example.toml, since go:embed cannot reach
// the parent directory and the two files are maintained by hand.
func TestEmbeddedConfigMatchesRepoRoot(t *testing.T) {
	embedded, err := os.ReadFile("config.example.toml")
	if err != nil {
		t.Fatalf("read embedded config.example.toml: %v", err)
	}
	root, err := os.ReadFile("../../config.example.toml")
	if err != nil {
		t.Fatalf("read repo-root config.example.toml: %v", err)
	}
	if !bytes.Equal(embedded, root) {
		t.Error("internal/install/config.example.toml drifted from the repo-root copy; keep them identical")
	}
}

func TestRequireHostBinaryReportsInstallHint(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir)
	err := requireHostBinary(context.Background(), "skopeo", "brew install skopeo")
	if err == nil {
		t.Fatal("expected missing binary error")
	}
	for _, want := range []string{"skopeo", "brew install skopeo"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q missing %q", err.Error(), want)
		}
	}

	tartPath := filepath.Join(dir, "tart")
	if err := os.WriteFile(tartPath, []byte("#!/usr/bin/env bash\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write fake tart: %v", err)
	}
	if err := requireHostBinary(context.Background(), "tart", "brew install cirruslabs/cli/tart"); err != nil {
		t.Fatalf("requireHostBinary with fake tart: %v", err)
	}
}

func assertNoMarkers(t *testing.T, rendered string) {
	t.Helper()
	for _, marker := range []string{"@@", "{{"} {
		if strings.Contains(rendered, marker) {
			t.Errorf("rendered output still contains marker %q\n%s", marker, rendered)
		}
	}
}
