package install

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// exampleConfig is the committed config template scaffolded on first install.
//
//go:embed config.example.toml
var exampleConfig string

const (
	// configDirMode is the private mode for the config directory.
	configDirMode = 0o700
	// configFileMode is the mode for the scaffolded config.toml.
	configFileMode = 0o600
	// secretFileMode is the mode for generated secret files.
	secretFileMode = 0o600
	// dataFileMode is the mode for the non-secret CIDR list.
	dataFileMode = 0o644
	// secretBytes is the number of random bytes per generated secret.
	secretBytes = 32
	// httpTimeout bounds the GitHub API calls the installer makes.
	httpTimeout = 30 * time.Second
	// placeholderHome is the literal home prefix in the example config.
	placeholderHome = "/Users/you"
	// githubMetaURL returns GitHub's published webhook source ranges.
	githubMetaURL = "https://api.github.com/meta"
)

// ensureConfigDir creates the config directory at mode 0700 when absent. An
// existing directory is left untouched so the step is idempotent.
func ensureConfigDir(ctx context.Context, dir string) error {
	if info, err := os.Stat(dir); err == nil && info.IsDir() {
		return nil
	}
	if err := os.MkdirAll(dir, configDirMode); err != nil {
		slog.ErrorContext(ctx, "create config dir failed", "err", err, "dir", dir)
		return fmt.Errorf("install: create config dir %s: %w", dir, err)
	}
	slog.InfoContext(ctx, "created config dir", "dir", dir)
	return nil
}

// scaffoldConfig writes config.toml from the embedded example when absent,
// replacing the placeholder home with the real home. The App ID placeholder is
// left for the operator to fill.
func scaffoldConfig(ctx context.Context, cfg Config) error {
	if _, err := os.Stat(cfg.ConfigPath); err == nil {
		return nil
	}
	rendered := strings.ReplaceAll(exampleConfig, placeholderHome, cfg.Home)
	if err := os.WriteFile(cfg.ConfigPath, []byte(rendered), configFileMode); err != nil {
		slog.ErrorContext(ctx, "write config failed", "err", err, "path", cfg.ConfigPath)
		return fmt.Errorf("install: write config %s: %w", cfg.ConfigPath, err)
	}
	slog.InfoContext(ctx, "scaffolded config", "path", cfg.ConfigPath)
	return nil
}

// ensureSecrets generates the webhook secret and capacity token when absent.
func ensureSecrets(ctx context.Context, dir string) error {
	if err := ensureSecret(ctx, filepath.Join(dir, "webhook-secret")); err != nil {
		return err
	}
	return ensureSecret(ctx, filepath.Join(dir, "capacity-token"))
}

// ensureSecret writes a hex-encoded 32-byte random secret at mode 0600 with no
// trailing newline when the file is absent.
func ensureSecret(ctx context.Context, path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	buf := make([]byte, secretBytes)
	if _, err := rand.Read(buf); err != nil {
		slog.ErrorContext(ctx, "generate secret failed", "err", err, "path", path)
		return fmt.Errorf("install: generate secret %s: %w", path, err)
	}
	encoded := hex.EncodeToString(buf)
	if err := os.WriteFile(path, []byte(encoded), secretFileMode); err != nil {
		slog.ErrorContext(ctx, "write secret failed", "err", err, "path", path)
		return fmt.Errorf("install: write secret %s: %w", path, err)
	}
	slog.InfoContext(ctx, "generated secret", "path", path)
	return nil
}

// metaResponse is the subset of GitHub's /meta response the installer reads.
type metaResponse struct {
	Hooks []string `json:"hooks"`
}

// ensureWebhookCIDRs fetches GitHub's webhook source ranges and writes them one
// per line. The file is optional, so any failure is logged at Warn and the
// install continues.
func ensureWebhookCIDRs(ctx context.Context, dir string) {
	path := filepath.Join(dir, "github-webhook-cidrs.txt")
	if _, err := os.Stat(path); err == nil {
		return
	}
	cidrs, err := fetchMetaHooks(ctx)
	if err != nil {
		slog.WarnContext(ctx, "fetch github webhook CIDRs failed; continuing", "err", err)
		return
	}
	content := strings.Join(cidrs, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), dataFileMode); err != nil {
		slog.WarnContext(ctx, "write github webhook CIDRs failed; continuing", "err", err, "path", path)
		return
	}
	slog.InfoContext(ctx, "wrote github webhook CIDRs", "path", path, "count", len(cidrs))
}

// fetchMetaHooks fetches the .hooks[] CIDR list from GitHub's /meta endpoint.
func fetchMetaHooks(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, githubMetaURL, nil)
	if err != nil {
		slog.ErrorContext(ctx, "build meta request failed", "err", err)
		return nil, fmt.Errorf("install: build meta request: %w", err)
	}
	client := &http.Client{Timeout: httpTimeout}
	resp, err := client.Do(req)
	if err != nil {
		slog.ErrorContext(ctx, "fetch meta failed", "err", err)
		return nil, fmt.Errorf("install: fetch meta: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		slog.ErrorContext(ctx, "meta status not ok", "err", fmt.Errorf("status %d", resp.StatusCode), "status", resp.StatusCode)
		return nil, fmt.Errorf("install: meta status %d", resp.StatusCode)
	}
	var meta metaResponse
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		slog.ErrorContext(ctx, "decode meta failed", "err", err)
		return nil, fmt.Errorf("install: decode meta: %w", err)
	}
	return meta.Hooks, nil
}
