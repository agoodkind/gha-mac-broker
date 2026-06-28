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
golden_image = "gha-golden"
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ListenAddr != "[::1]:8080" {
		t.Errorf("default listen addr = %q", cfg.ListenAddr)
	}
	if cfg.PoolSize != 2 {
		t.Errorf("default pool size = %d", cfg.PoolSize)
	}
	if cfg.Tart.Binary != "tart" {
		t.Errorf("default tart binary = %q", cfg.Tart.Binary)
	}
	if !cfg.RepoAllowed("agoodkind/LMD") {
		t.Error("allowlist should match case-insensitively")
	}
	if cfg.RepoAllowed("agoodkind/secret") {
		t.Error("non-listed repo must not be allowed")
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
