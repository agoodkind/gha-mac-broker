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
)

// TokenPath is the fixed 0600 file the guest-supervisor writes the per-boot
// bearer token to at startup. The host reads it once over the tart-exec control
// channel (`tart exec <vm> cat <TokenPath>`) and dials the guest agent with it.
const TokenPath = "/tmp/gha-guest-token" // #nosec G101 -- path only, not a secret.

// tokenFileMode keeps the boot token readable only by its owner.
const tokenFileMode = 0o600

// tokenEntropyBytes is the random length of a minted per-boot token.
const tokenEntropyBytes = 32

// MintToken returns a fresh random hex bearer token for one VM boot.
func MintToken() (string, error) {
	entropy := make([]byte, tokenEntropyBytes)
	if _, err := rand.Read(entropy); err != nil {
		slog.Error("guest supervisor mint token failed", "err", err)
		return "", fmt.Errorf("guestsupervisor: mint token: %w", err)
	}
	return hex.EncodeToString(entropy), nil
}

// WriteTokenFile writes token to path with 0600 permissions, tightening an
// existing looser file so the host can read a private per-boot token.
func WriteTokenFile(path string, token string) error {
	if err := os.WriteFile(path, []byte(token), tokenFileMode); err != nil {
		slog.Error("guest supervisor write token file failed", "err", err, "path", path)
		return fmt.Errorf("guestsupervisor: write token file %s: %w", path, err)
	}
	// WriteFile keeps the mode of a pre-existing file, so tighten it explicitly.
	if err := os.Chmod(path, tokenFileMode); err != nil {
		slog.Error("guest supervisor chmod token file failed", "err", err, "path", path)
		return fmt.Errorf("guestsupervisor: chmod token file %s: %w", path, err)
	}
	slog.Info("guest supervisor token file written", "path", path)
	return nil
}
