package guestsupervisor

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMintTokenReturnsDistinctNonEmptyTokens(t *testing.T) {
	first, err := MintToken()
	if err != nil {
		t.Fatalf("MintToken: %v", err)
	}
	if first == "" {
		t.Fatal("minted token = empty, want non-empty")
	}
	second, err := MintToken()
	if err != nil {
		t.Fatalf("MintToken second: %v", err)
	}
	if first == second {
		t.Fatal("two minted tokens are equal, want distinct per-boot tokens")
	}
}

func TestWriteTokenFileWritesPrivateFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := WriteTokenFile(path, "boot-token"); err != nil {
		t.Fatalf("WriteTokenFile: %v", err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read token file: %v", err)
	}
	if string(content) != "boot-token" {
		t.Fatalf("token file content = %q, want boot-token", string(content))
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat token file: %v", err)
	}
	if info.Mode().Perm() != tokenFileMode {
		t.Fatalf("token file mode = %o, want %o", info.Mode().Perm(), tokenFileMode)
	}
}

func TestEnsureBootTokenReusesTokenAcrossProcessRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	first, err := EnsureBootToken("", path)
	if err != nil {
		t.Fatalf("first EnsureBootToken: %v", err)
	}
	if first == "" {
		t.Fatal("first token = empty, want minted")
	}
	// A supervisor process restart within the same boot reads the same file.
	second, err := EnsureBootToken("", path)
	if err != nil {
		t.Fatalf("second EnsureBootToken: %v", err)
	}
	if second != first {
		t.Fatalf("token = %q, want reused %q across process restart", second, first)
	}
}

func TestEnsureBootTokenPrefersEnvToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("stale-token"), tokenFileMode); err != nil {
		t.Fatalf("seed token file: %v", err)
	}
	token, err := EnsureBootToken("env-token", path)
	if err != nil {
		t.Fatalf("EnsureBootToken: %v", err)
	}
	if token != "env-token" {
		t.Fatalf("token = %q, want env-token precedence", token)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read token file: %v", err)
	}
	if string(content) != "env-token" {
		t.Fatalf("token file = %q, want env-token written", string(content))
	}
}

func TestEnsureBootTokenMintsWhenAbsent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	token, err := EnsureBootToken("", path)
	if err != nil {
		t.Fatalf("EnsureBootToken: %v", err)
	}
	if token == "" {
		t.Fatal("token = empty, want a minted token")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("token file not written: %v", err)
	}
}

func TestWriteTokenFileTightensLooserExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("stale"), 0o644); err != nil {
		t.Fatalf("seed loose token file: %v", err)
	}
	if err := WriteTokenFile(path, "fresh"); err != nil {
		t.Fatalf("WriteTokenFile: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat token file: %v", err)
	}
	if info.Mode().Perm() != tokenFileMode {
		t.Fatalf("token file mode = %o, want tightened %o", info.Mode().Perm(), tokenFileMode)
	}
}
