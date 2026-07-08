package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"goodkind.io/gha-mac-broker/internal/config"
)

func TestConfigReloadWatcherWaitsForStableModTime(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	writeReloadConfig(t, configPath, 1)
	initialModTime := time.Date(2026, 7, 7, 9, 0, 0, 0, time.UTC)
	setReloadConfigModTime(t, configPath, initialModTime)

	var appliedJobsPerVM []int
	watcher := newConfigReloadWatcher(configReloadWatcherOptions{
		path:           configPath,
		initialModTime: initialModTime,
		load:           config.Load,
		apply: func(_ context.Context, cfg *config.Config) error {
			appliedJobsPerVM = append(appliedJobsPerVM, cfg.JobsPerVM)
			return nil
		},
	})

	firstChange := initialModTime.Add(time.Minute)
	writeReloadConfig(t, configPath, 2)
	setReloadConfigModTime(t, configPath, firstChange)
	watcher.poll(context.Background())
	if len(appliedJobsPerVM) != 0 {
		t.Fatalf("applied jobs_per_vm after first changed poll = %v, want none", appliedJobsPerVM)
	}

	secondChange := firstChange.Add(time.Minute)
	writeReloadConfig(t, configPath, 3)
	setReloadConfigModTime(t, configPath, secondChange)
	watcher.poll(context.Background())
	if len(appliedJobsPerVM) != 0 {
		t.Fatalf("applied jobs_per_vm while mtime changed again = %v, want none", appliedJobsPerVM)
	}

	watcher.poll(context.Background())
	if fmt.Sprint(appliedJobsPerVM) != "[3]" {
		t.Fatalf("applied jobs_per_vm after stable mtime = %v, want [3]", appliedJobsPerVM)
	}
}

func TestConfigReloadWatcherKeepsCurrentConfigAfterInvalidReload(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	writeReloadConfig(t, configPath, 1)
	initialModTime := time.Date(2026, 7, 7, 10, 0, 0, 0, time.UTC)
	setReloadConfigModTime(t, configPath, initialModTime)

	var appliedJobsPerVM []int
	watcher := newConfigReloadWatcher(configReloadWatcherOptions{
		path:           configPath,
		initialModTime: initialModTime,
		load:           config.Load,
		apply: func(_ context.Context, cfg *config.Config) error {
			appliedJobsPerVM = append(appliedJobsPerVM, cfg.JobsPerVM)
			return nil
		},
	})

	invalidChange := initialModTime.Add(time.Minute)
	if err := os.WriteFile(configPath, []byte("jobs_per_vm = 0\n"), 0o600); err != nil {
		t.Fatalf("write invalid config: %v", err)
	}
	setReloadConfigModTime(t, configPath, invalidChange)
	watcher.poll(context.Background())
	watcher.poll(context.Background())
	if len(appliedJobsPerVM) != 0 {
		t.Fatalf("applied jobs_per_vm after invalid reload = %v, want none", appliedJobsPerVM)
	}

	validChange := invalidChange.Add(time.Minute)
	writeReloadConfig(t, configPath, 4)
	setReloadConfigModTime(t, configPath, validChange)
	watcher.poll(context.Background())
	watcher.poll(context.Background())
	if fmt.Sprint(appliedJobsPerVM) != "[4]" {
		t.Fatalf("applied jobs_per_vm after valid reload = %v, want [4]", appliedJobsPerVM)
	}
}

func TestConfigReloadWatcherDropsPendingReloadWithoutApplyCallback(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	writeReloadConfig(t, configPath, 1)
	initialModTime := time.Date(2026, 7, 7, 11, 0, 0, 0, time.UTC)
	setReloadConfigModTime(t, configPath, initialModTime)

	loadCount := 0
	watcher := newConfigReloadWatcher(configReloadWatcherOptions{
		path:           configPath,
		initialModTime: initialModTime,
		load: func(path string) (*config.Config, error) {
			loadCount++
			return config.Load(path)
		},
		apply: nil,
	})

	changedModTime := initialModTime.Add(time.Minute)
	writeReloadConfig(t, configPath, 2)
	setReloadConfigModTime(t, configPath, changedModTime)
	watcher.poll(context.Background())
	watcher.poll(context.Background())
	watcher.poll(context.Background())

	if loadCount != 1 {
		t.Fatalf("load count = %d, want 1", loadCount)
	}
	if !watcher.appliedModTime.Equal(changedModTime) {
		t.Fatalf("applied mod time = %v, want %v", watcher.appliedModTime, changedModTime)
	}
	if watcher.hasPending {
		t.Fatal("has pending = true, want false")
	}
}

func TestConfigReloadWatcherPanicRecoveryUsesInjectedLogger(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	writeReloadConfig(t, configPath, 1)
	initialModTime := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	setReloadConfigModTime(t, configPath, initialModTime)

	changedModTime := initialModTime.Add(time.Minute)
	writeReloadConfig(t, configPath, 2)
	setReloadConfigModTime(t, configPath, changedModTime)

	records := make(chan slog.Record, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startConfigReloadWatcher(ctx, configReloadWatcherOptions{
		path:           configPath,
		initialModTime: initialModTime,
		interval:       time.Millisecond,
		load:           config.Load,
		apply: func(_ context.Context, _ *config.Config) error {
			panic("reload apply failed")
		},
		log: slog.New(reloadTestLogHandler{records: records}),
	})

	select {
	case record := <-records:
		if record.Message != "config reload watcher panic recovered" {
			t.Fatalf("log message = %q, want config reload watcher panic recovered", record.Message)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for injected logger record")
	}
}

func writeReloadConfig(t *testing.T, path string, jobsPerVM int) {
	t.Helper()
	body := fmt.Sprintf(`
listen_addr = "[::1]:8080"
runner_count = 1
jobs_per_vm = %d
labels = ["self-hosted", "macOS"]

[app]
app_id = "1"
private_key_path = %q

[tart]
base_image = %q
cache_dir = %q
fast_pull_dir = %q
`, jobsPerVM, filepath.Join(filepath.Dir(path), "key.pem"), config.DefaultBaseImage, filepath.Join(filepath.Dir(path), "cache"), filepath.Join(filepath.Dir(path), "fastpull"))
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

func setReloadConfigModTime(t *testing.T, path string, modTime time.Time) {
	t.Helper()
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("set config mtime: %v", err)
	}
}

type reloadTestLogHandler struct {
	records chan slog.Record
}

func (handler reloadTestLogHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return true
}

func (handler reloadTestLogHandler) Handle(_ context.Context, record slog.Record) error {
	handler.records <- record
	return nil
}

func (handler reloadTestLogHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return handler
}

func (handler reloadTestLogHandler) WithGroup(_ string) slog.Handler {
	return handler
}
