package config

import (
	"os"
	"path/filepath"
	"testing"
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
