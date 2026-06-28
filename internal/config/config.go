// Package config loads the broker configuration from a JSON file. Secret values
// are referenced by file path, never inlined, so the App private key and webhook
// secret stay out of the config file, logs, and process arguments.
package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Config is the broker's runtime configuration.
type Config struct {
	// ListenAddr is the address the webhook and capacity server binds to.
	ListenAddr string `json:"listen_addr"`

	// App identifies the GitHub App and where its private key lives.
	App AppConfig `json:"app"`

	// Tart configures the VM pool substrate.
	Tart TartConfig `json:"tart"`

	// Labels are the runner labels every JIT runner registers with. The
	// self-hosted job in CI targets one of these.
	Labels []string `json:"labels"`

	// AllowedRepos is the owner/repo allowlist the broker will serve. A queued
	// job for any other repository is ignored.
	AllowedRepos []string `json:"allowed_repos"`

	// PoolSize is the number of warm VMs kept booted and idle.
	PoolSize int `json:"pool_size"`
}

// AppConfig holds GitHub App identity and secret references.
type AppConfig struct {
	AppID string `json:"app_id"`
	// PrivateKeyPath points to the PEM private key on disk.
	PrivateKeyPath string `json:"private_key_path"`
	// WebhookSecretPath points to a file holding the webhook HMAC secret.
	WebhookSecretPath string `json:"webhook_secret_path"`
}

// TartConfig configures the VM substrate.
type TartConfig struct {
	// Binary is the tart executable (default "tart").
	Binary string `json:"binary"`
	// GoldenImage is the source VM the pool clones. It has the runner binary
	// installed but unconfigured.
	GoldenImage string `json:"golden_image"`
	// VMNamePrefix prefixes ephemeral clone names.
	VMNamePrefix string `json:"vm_name_prefix"`
	// CacheDir is a host directory shared into each VM so the build cache
	// survives VM deletion.
	CacheDir string `json:"cache_dir"`
	// SSHKeyPath is the private key the broker uses to control the VM over
	// SSH. Its public half is baked into the golden image.
	SSHKeyPath string `json:"ssh_key_path"`
	// SSHUser is the account the golden image exposes for runner control.
	SSHUser string `json:"ssh_user"`
}

// Load reads and validates a JSON config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		slog.Error("config read failed", "err", err, "path", path)
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		slog.Error("config parse failed", "err", err, "path", path)
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.ListenAddr == "" {
		c.ListenAddr = "[::1]:8080"
	}
	if c.Tart.Binary == "" {
		c.Tart.Binary = "tart"
	}
	if c.Tart.VMNamePrefix == "" {
		c.Tart.VMNamePrefix = "gha-runner"
	}
	if c.Tart.SSHUser == "" {
		c.Tart.SSHUser = "admin"
	}
	if c.PoolSize == 0 {
		c.PoolSize = 2
	}
	if len(c.Labels) == 0 {
		c.Labels = []string{"self-hosted", "macOS", "ARM64", "agk-local-macos-26"}
	}
}

func (c *Config) validate() error {
	var missing []string
	if c.App.AppID == "" {
		missing = append(missing, "app.app_id")
	}
	if c.App.PrivateKeyPath == "" {
		missing = append(missing, "app.private_key_path")
	}
	if c.Tart.GoldenImage == "" {
		missing = append(missing, "tart.golden_image")
	}
	if len(c.AllowedRepos) == 0 {
		missing = append(missing, "allowed_repos")
	}
	if len(missing) > 0 {
		return fmt.Errorf("config: missing required fields: %s", strings.Join(missing, ", "))
	}
	return nil
}

// RepoAllowed reports whether owner/repo is in the allowlist.
func (c *Config) RepoAllowed(fullName string) bool {
	for _, r := range c.AllowedRepos {
		if strings.EqualFold(r, fullName) {
			return true
		}
	}
	return false
}

// ReadPrivateKey reads the App private key bytes from disk.
func (c *Config) ReadPrivateKey() ([]byte, error) {
	key, err := os.ReadFile(c.App.PrivateKeyPath)
	if err != nil {
		slog.Error("read private key failed", "err", err, "path", c.App.PrivateKeyPath)
		return nil, fmt.Errorf("config: read private key %s: %w", c.App.PrivateKeyPath, err)
	}
	return key, nil
}
