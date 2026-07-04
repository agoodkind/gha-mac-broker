package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadDefaultsAndAllowlist(t *testing.T) {
	path := writeConfig(t, `
allowed_repos = ["agoodkind/lmd", "agoodkind/swift-makefile"]

[app]
app_id = "12345"
private_key_path = "/tmp/key.pem"

[tart]
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ListenAddr != "[::1]:8080" {
		t.Errorf("default listen addr = %q", cfg.ListenAddr)
	}
	if cfg.RunnerCount != 3 {
		t.Errorf("default runner count = %d", cfg.RunnerCount)
	}
	// MaxIdle and MaxAge are honored verbatim, so an unset value stays zero and
	// disables that recycle trigger rather than defaulting.
	if time.Duration(cfg.MaxIdle) != 0 {
		t.Errorf("unset max idle = %s, want 0 (disabled)", time.Duration(cfg.MaxIdle))
	}
	if time.Duration(cfg.MaxAge) != 0 {
		t.Errorf("unset max age = %s, want 0 (disabled)", time.Duration(cfg.MaxAge))
	}
	if cfg.Tart.WarmBudget != 2 {
		t.Errorf("default warm budget = %d", cfg.Tart.WarmBudget)
	}
	if cfg.Tart.GoldenBudget != 3 {
		t.Errorf("default golden budget = %d", cfg.Tart.GoldenBudget)
	}
	if cfg.Tart.Binary != "tart" {
		t.Errorf("default tart binary = %q", cfg.Tart.Binary)
	}
	if cfg.Tart.FastPull == nil {
		t.Fatal("default fast pull should be set")
	}
	if !*cfg.Tart.FastPull {
		t.Error("default fast pull should be true")
	}
	wantFastPullDir := filepath.Join(os.TempDir(), "gha-mac-broker-fastpull-blobs")
	if cfg.Tart.FastPullDir != wantFastPullDir {
		t.Errorf("default fast pull dir = %q, want %q", cfg.Tart.FastPullDir, wantFastPullDir)
	}
	wantCacheDir := filepath.Join(os.TempDir(), "gha-mac-broker-pool-cache")
	if home, err := os.UserHomeDir(); err == nil {
		wantCacheDir = filepath.Join(home, "pool-cache")
	}
	if cfg.Tart.CacheDir != wantCacheDir {
		t.Errorf("default cache dir = %q, want %q", cfg.Tart.CacheDir, wantCacheDir)
	}
	image, ok := cfg.ResolveImage("", "")
	if !ok {
		t.Fatal("empty request should resolve to default base image")
	}
	if image != DefaultBaseImage {
		t.Errorf("default image = %q, want %q", image, DefaultBaseImage)
	}
	image, ok = cfg.ResolveImage("tahoe", "26.5")
	if !ok {
		t.Fatal("default mapping should resolve tahoe + 26.5")
	}
	if image != DefaultBaseImage {
		t.Errorf("mapped image = %q, want %q", image, DefaultBaseImage)
	}
	if !cfg.RepoAllowed("agoodkind/LMD") {
		t.Error("allowlist should match case-insensitively")
	}
	if cfg.RepoAllowed("agoodkind/secret") {
		t.Error("non-listed repo must not be allowed")
	}
}

func TestLoadRunnerPoolSettings(t *testing.T) {
	path := writeConfig(t, `
runner_count = 5
max_idle = "45m"
max_age = "6h"
allowed_repos = ["agoodkind/lmd"]

[app]
app_id = "12345"
private_key_path = "/tmp/key.pem"

[tart]
warm_budget = 7
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RunnerCount != 5 {
		t.Fatalf("runner count = %d, want 5", cfg.RunnerCount)
	}
	if time.Duration(cfg.MaxIdle) != 45*time.Minute {
		t.Fatalf("max idle = %s, want 45m0s", time.Duration(cfg.MaxIdle))
	}
	if time.Duration(cfg.MaxAge) != 6*time.Hour {
		t.Fatalf("max age = %s, want 6h0m0s", time.Duration(cfg.MaxAge))
	}
	if cfg.Tart.WarmBudget != 7 {
		t.Fatalf("warm budget = %d, want back-compat parse value 7", cfg.Tart.WarmBudget)
	}
}

func TestResolveImageUsesConfiguredAllowlist(t *testing.T) {
	path := writeConfig(t, `
allowed_repos = ["agoodkind/lmd"]

[app]
app_id = "12345"
private_key_path = "/tmp/key.pem"

[tart]
base_image = "ghcr.io/cirruslabs/macos-tahoe-xcode:26.5"
warm_budget = 4
golden_budget = 5

[[tart.images]]
macos = "ventura"
xcode = "15.4"
tag = "ghcr.io/cirruslabs/macos-ventura-xcode:15.4"
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	image, ok := cfg.ResolveImage("VENTURA", "15.4")
	if !ok {
		t.Fatal("configured mapping should resolve case-insensitively")
	}
	if image != "ghcr.io/cirruslabs/macos-ventura-xcode:15.4" {
		t.Fatalf("image = %q", image)
	}
	if _, ok := cfg.ResolveImage("ventura", "16.0"); ok {
		t.Fatal("unmapped declared pair should not resolve")
	}
}

func TestResolveImageRejectsUnsafeConfiguredTag(t *testing.T) {
	cfg := &Config{
		ListenAddr: "[::1]:8080",
		App: AppConfig{
			AppID:             "1",
			PrivateKeyPath:    "/tmp/key.pem",
			WebhookSecretPath: "",
			CapacityTokenPath: "",
			WebhookCIDRsPath:  "",
		},
		Tart: TartConfig{
			Binary:       "tart",
			GoldenImage:  "",
			BaseImage:    DefaultBaseImage,
			WarmBudget:   2,
			GoldenBudget: 3,
			Images:       []ImageMapping{{MacOS: "tahoe", Xcode: "raw", Tag: "docker.io/library/alpine:latest"}},
			VMNamePrefix: "gha-runner",
			CacheDir:     "",
			FastPull:     nil,
			FastPullDir:  "",
		},
		Labels:       []string{"self-hosted"},
		AllowedRepos: []string{"agoodkind/lmd"},
	}
	if _, ok := cfg.ResolveImage("tahoe", "raw"); ok {
		t.Fatal("unsafe mapped tag should not resolve")
	}
}

func TestLoadMissingRequired(t *testing.T) {
	path := writeConfig(t, `
[app]
app_id = "12345"
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing required fields")
	}
}

func TestLoadRejectsFastPullDirInsideCacheDir(t *testing.T) {
	path := writeConfig(t, `
allowed_repos = ["agoodkind/lmd"]

[app]
app_id = "12345"
private_key_path = "/tmp/key.pem"

[tart]
cache_dir = "/tmp/pool-cache"
fast_pull_dir = "/tmp/pool-cache/fastpull-blobs"
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for fast_pull_dir inside cache_dir")
	}
	if !strings.Contains(err.Error(), "must not be inside") {
		t.Errorf("error = %q, want it to mention the containment violation", err.Error())
	}
}
