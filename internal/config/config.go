// Package config loads the broker configuration from a TOML file. Secret
// values are referenced by file path, never inlined, so the App private key
// and webhook secret stay out of the config file, logs, and process arguments.
package config

import (
	"bytes"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// Config is the broker's runtime configuration.
type Config struct {
	// ListenAddr is the address the webhook and capacity server binds to.
	ListenAddr string `toml:"listen_addr"`

	// App identifies the GitHub App and where its private key lives.
	App AppConfig `toml:"app"`

	// Tart configures the VM pool substrate.
	Tart TartConfig `toml:"tart"`

	// Labels are the runner labels every JIT runner registers with. The
	// self-hosted job in CI targets one of these.
	Labels []string `toml:"labels"`

	// AllowedRepos is the owner/repo allowlist the broker will serve. A queued
	// job for any other repository is ignored.
	AllowedRepos []string `toml:"allowed_repos"`

	// PoolSize is the number of warm VMs kept booted and idle.
	PoolSize int `toml:"pool_size"`
}

// AppConfig holds GitHub App identity and secret references.
type AppConfig struct {
	AppID string `toml:"app_id"`
	// PrivateKeyPath points to the PEM private key on disk.
	PrivateKeyPath string `toml:"private_key_path"`
	// WebhookSecretPath points to a file holding the webhook HMAC secret.
	WebhookSecretPath string `toml:"webhook_secret_path"`
	// CapacityTokenPath points to a file holding the bearer token required
	// on GET /capacity. When empty, /capacity is closed (401 fail-safe).
	CapacityTokenPath string `toml:"capacity_token_path"`
	// WebhookCIDRsPath points to a file with one CIDR per line listing the
	// IP ranges allowed to deliver webhook payloads. When empty or the list
	// is empty after parsing, the IP guard is disabled (dev/local mode).
	WebhookCIDRsPath string `toml:"webhook_cidrs_path"`
}

// TartConfig configures the VM substrate.
type TartConfig struct {
	// Binary is the tart executable (default "tart").
	Binary string `toml:"binary"`
	// GoldenImage is the source VM the pool clones. It has the runner binary
	// installed but unconfigured.
	GoldenImage string `toml:"golden_image"`
	// VMNamePrefix prefixes ephemeral clone names.
	VMNamePrefix string `toml:"vm_name_prefix"`
	// CacheDir is a host directory shared into each VM so the build cache
	// survives VM deletion.
	CacheDir string `toml:"cache_dir"`
	// SSHKeyPath is the private key the broker uses to control the VM over
	// SSH. Its public half is baked into the golden image.
	SSHKeyPath string `toml:"ssh_key_path"`
	// SSHUser is the account the golden image exposes for runner control.
	SSHUser string `toml:"ssh_user"`
}

// DefaultConfigPath returns the XDG-aware default config file path:
// $XDG_CONFIG_HOME/gha-mac-broker/config.toml, falling back to
// $HOME/.config/gha-mac-broker/config.toml when XDG_CONFIG_HOME is unset.
func DefaultConfigPath() string {
	if dir := os.Getenv("XDG_CONFIG_HOME"); dir != "" {
		return filepath.Join(dir, "gha-mac-broker", "config.toml")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	return filepath.Join(home, ".config", "gha-mac-broker", "config.toml")
}

// Load reads and validates a TOML config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		slog.Error("config read failed", "err", err, "path", path)
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	var cfg Config
	if err := toml.Unmarshal(raw, &cfg); err != nil {
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

// ReadCapacityToken reads the bearer token required on GET /capacity. When
// CapacityTokenPath is empty the method returns (nil, nil): the server then
// treats /capacity as closed (401 fail-safe). Trailing line endings are
// stripped so a file written by echo or printf works without extra care.
func (c *Config) ReadCapacityToken() ([]byte, error) {
	if c.App.CapacityTokenPath == "" {
		return nil, nil
	}
	token, err := os.ReadFile(c.App.CapacityTokenPath)
	if err != nil {
		slog.Error("read capacity token failed", "err", err, "path", c.App.CapacityTokenPath)
		return nil, fmt.Errorf("config: read capacity token %s: %w", c.App.CapacityTokenPath, err)
	}
	return bytes.TrimRight(token, "\r\n"), nil
}

// ReadWebhookSecret reads the webhook HMAC secret from disk, stripping trailing
// line endings so a secret file written by echo or printf verifies correctly
// against GitHub's X-Hub-Signature-256 (a stray newline silently breaks HMAC).
// The path is required; an empty or unreadable path is an error so the server
// never starts with an unverifiable webhook.
func (c *Config) ReadWebhookSecret() ([]byte, error) {
	secret, err := os.ReadFile(c.App.WebhookSecretPath)
	if err != nil {
		slog.Error("read webhook secret failed", "err", err, "path", c.App.WebhookSecretPath)
		return nil, fmt.Errorf("config: read webhook secret %s: %w", c.App.WebhookSecretPath, err)
	}
	return bytes.TrimRight(secret, "\r\n"), nil
}

// ReadWebhookCIDRs reads the allowed IP ranges for /webhook from the file at
// WebhookCIDRsPath (one CIDR per line; blank lines are skipped). When the
// path is empty the method returns (nil, nil) and the server disables the IP
// guard so local/dev runs work without a CIDRs file.
func (c *Config) ReadWebhookCIDRs() ([]*net.IPNet, error) {
	if c.App.WebhookCIDRsPath == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(c.App.WebhookCIDRsPath)
	if err != nil {
		slog.Error("read webhook CIDRs failed", "err", err, "path", c.App.WebhookCIDRsPath)
		return nil, fmt.Errorf("config: read webhook CIDRs %s: %w", c.App.WebhookCIDRsPath, err)
	}
	var nets []*net.IPNet
	for line := range strings.SplitSeq(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		_, ipNet, parseErr := net.ParseCIDR(line)
		if parseErr != nil {
			slog.Error("parse webhook CIDR failed", "err", parseErr, "cidr", line)
			return nil, fmt.Errorf("config: parse CIDR %q: %w", line, parseErr)
		}
		nets = append(nets, ipNet)
	}
	return nets, nil
}
