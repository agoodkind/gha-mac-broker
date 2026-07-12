// This file carries no build constraint so the host broker can import the
// shared guest-token path const on any platform while the rest of the
// supervisor package stays unix-only.

package guestsupervisor

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// TokenPath is the fixed 0600 file the guest-supervisor writes the per-boot
// bearer token to at startup. The host reads it once over the tart-exec control
// channel (`tart exec <vm> cat <TokenPath>`) and dials the guest agent with it.
const TokenPath = "/tmp/gha-guest-token" // #nosec G101 -- path only, not a secret.

// tokenFileMode keeps the boot token readable only by its owner.
const tokenFileMode = 0o600

// tokenEntropyBytes is the random length of a minted per-boot token.
const tokenEntropyBytes = 32

// EnsureBootToken resolves the boot-scoped bearer token and persists it at path,
// returning the token. An env-provided token wins. Otherwise it reuses an
// existing valid token at path, so a supervisor process restart within one boot
// (a KeepAlive respawn or a crash) keeps the same credential the broker cached,
// and only mints a fresh token on a genuinely fresh boot where path is absent.
// The write stays atomic at 0600.
func EnsureBootToken(envToken string, path string) (string, error) {
	token := envToken
	if token == "" {
		if existing := readBootToken(path); existing != "" {
			slog.Info("guest supervisor reusing boot token", "path", path)
			token = existing
		} else {
			minted, err := MintToken()
			if err != nil {
				return "", err
			}
			token = minted
		}
	}
	if err := WriteTokenFile(path, token); err != nil {
		return "", err
	}
	return token, nil
}

// readBootToken returns a trimmed non-empty token from path, or "" when the file
// is absent, unreadable, or empty.
func readBootToken(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// MintToken returns a fresh random hex bearer token for one VM boot.
func MintToken() (string, error) {
	entropy := make([]byte, tokenEntropyBytes)
	if _, err := rand.Read(entropy); err != nil {
		slog.Error("guest supervisor mint token failed", "err", err)
		return "", fmt.Errorf("guestsupervisor: mint token: %w", err)
	}
	return hex.EncodeToString(entropy), nil
}

// WriteTokenFile writes token to path atomically: it writes the bytes to a
// private 0600 temp file in the same directory, then renames it over path, so
// the token never lands in a possibly-looser pre-existing file and a reader
// never sees a partially written token.
func WriteTokenFile(path string, token string) error {
	temp, err := os.CreateTemp(filepath.Dir(path), ".gha-guest-token-*")
	if err != nil {
		slog.Error("guest supervisor create token temp file failed", "err", err, "path", path)
		return fmt.Errorf("guestsupervisor: create token temp file for %s: %w", path, err)
	}
	tempName := temp.Name()
	if _, err := temp.WriteString(token); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempName)
		slog.Error("guest supervisor write token temp file failed", "err", err, "path", tempName)
		return fmt.Errorf("guestsupervisor: write token temp file %s: %w", tempName, err)
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempName)
		slog.Error("guest supervisor close token temp file failed", "err", err, "path", tempName)
		return fmt.Errorf("guestsupervisor: close token temp file %s: %w", tempName, err)
	}
	// os.CreateTemp already creates the file 0600, so a reader of the temp path
	// never sees looser perms; set it explicitly to be defensive against a umask.
	if err := os.Chmod(tempName, tokenFileMode); err != nil {
		_ = os.Remove(tempName)
		slog.Error("guest supervisor chmod token temp file failed", "err", err, "path", tempName)
		return fmt.Errorf("guestsupervisor: chmod token temp file %s: %w", tempName, err)
	}
	if err := os.Rename(tempName, path); err != nil {
		_ = os.Remove(tempName)
		slog.Error("guest supervisor rename token file failed", "err", err, "path", path)
		return fmt.Errorf("guestsupervisor: rename token file to %s: %w", path, err)
	}
	slog.Info("guest supervisor token file written", "path", path)
	return nil
}
