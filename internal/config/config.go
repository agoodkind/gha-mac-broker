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

// DefaultBaseImage is the Cirrus base the golden is built from when config does
// not override it. It is the Xcode-bearing image (pinned) so the golden can
// build Swift; the CLI flag and config.toml both default to this.
const DefaultBaseImage = "ghcr.io/cirruslabs/macos-tahoe-xcode:26.5"

const (
	defaultWarmBudget   = 2
	defaultGoldenBudget = 3
	cirrusImagePrefix   = "ghcr.io/cirruslabs/"
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
	// GoldenImage is kept for compatibility with older configs and manual
	// build-golden flows; the runtime pool derives per-image golden names from
	// image tags.
	GoldenImage string `toml:"golden_image"`
	// BaseImage is the Cirrus image the golden is built from. Declared here
	// (not hardcoded in the binary) so the Xcode/base version is operator
	// policy; defaults to [DefaultBaseImage].
	BaseImage string `toml:"base_image"`
	// WarmBudget is the maximum number of idle warm VMs cached across images.
	WarmBudget int `toml:"warm_budget"`
	// GoldenBudget is the maximum number of per-image golden VMs cached on disk.
	GoldenBudget int `toml:"golden_budget"`
	// Images maps declared macOS and Xcode versions to pullable Cirrus tags.
	Images []ImageMapping `toml:"images"`
	// VMNamePrefix prefixes ephemeral clone names.
	VMNamePrefix string `toml:"vm_name_prefix"`
	// CacheDir is a host directory shared into each VM so the build cache
	// survives VM deletion.
	CacheDir string `toml:"cache_dir"`
	// FastPull enables the fast parallel base-image pull path when true. Nil
	// means enabled.
	FastPull *bool `toml:"fast_pull"`
	// FastPullSplit is the number of connections used per blob.
	FastPullSplit int `toml:"fast_pull_split"`
	// FastPullConnections is the maximum number of blobs downloaded at once.
	FastPullConnections int `toml:"fast_pull_connections"`
	// FastPullDir is the directory where downloaded blobs land.
	FastPullDir string `toml:"fast_pull_dir"`
	// FastPullKeepBlobs keeps downloaded blobs for fast rebuilds when true. Nil
	// means true.
	FastPullKeepBlobs *bool `toml:"fast_pull_keep_blobs"`
}

// ImageMapping maps a declared macOS and Xcode pair to an approved Cirrus tag.
type ImageMapping struct {
	MacOS string `toml:"macos"`
	Xcode string `toml:"xcode"`
	Tag   string `toml:"tag"`
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

// Default returns a Config with defaults applied and no file loaded. It suits a
// bare-host bootstrap (build-golden) where no config file exists yet.
func Default() *Config {
	var cfg Config
	cfg.applyDefaults()
	return &cfg
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
	if c.Tart.BaseImage == "" {
		c.Tart.BaseImage = DefaultBaseImage
	}
	if c.Tart.WarmBudget == 0 {
		c.Tart.WarmBudget = defaultWarmBudget
	}
	if c.Tart.GoldenBudget == 0 {
		c.Tart.GoldenBudget = defaultGoldenBudget
	}
	if len(c.Tart.Images) == 0 {
		c.Tart.Images = []ImageMapping{
			{MacOS: "tahoe", Xcode: "26.5", Tag: c.Tart.BaseImage},
		}
	}
	if c.Tart.FastPull == nil {
		c.Tart.FastPull = new(bool)
		*c.Tart.FastPull = true
	}
	if c.Tart.FastPullSplit == 0 {
		c.Tart.FastPullSplit = 16
	}
	if c.Tart.FastPullConnections == 0 {
		c.Tart.FastPullConnections = 8
	}
	if c.Tart.FastPullDir == "" {
		if c.Tart.CacheDir != "" {
			c.Tart.FastPullDir = filepath.Join(c.Tart.CacheDir, "fastpull-blobs")
		} else {
			c.Tart.FastPullDir = filepath.Join(os.TempDir(), "gha-mac-broker-fastpull-blobs")
		}
	}
	if c.Tart.FastPullKeepBlobs == nil {
		c.Tart.FastPullKeepBlobs = new(bool)
		*c.Tart.FastPullKeepBlobs = true
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
	if len(c.AllowedRepos) == 0 {
		missing = append(missing, "allowed_repos")
	}
	if len(missing) > 0 {
		return fmt.Errorf("config: missing required fields: %s", strings.Join(missing, ", "))
	}
	if !safeCirrusImageTag(c.Tart.BaseImage) {
		return fmt.Errorf("config: tart.base_image must be a ghcr.io/cirruslabs/macos-*-xcode:* tag")
	}
	for _, image := range c.Tart.Images {
		if image.MacOS == "" || image.Xcode == "" || image.Tag == "" {
			return fmt.Errorf("config: tart.images entries require macos, xcode, and tag")
		}
		if !safeCirrusImageTag(image.Tag) {
			return fmt.Errorf("config: tart.images tag %q must be a ghcr.io/cirruslabs/macos-*-xcode:* tag", image.Tag)
		}
	}
	return nil
}

// ResolveImage maps a declared macOS and Xcode request to an approved Cirrus
// image tag. An omitted pair resolves to tart.base_image. Partial, unmapped, or
// unsafe requests return ok=false.
func (c *Config) ResolveImage(macos, xcode string) (tag string, ok bool) {
	macos = normalizeImageKey(macos)
	xcode = normalizeImageKey(xcode)
	if macos == "" && xcode == "" {
		if safeCirrusImageTag(c.Tart.BaseImage) {
			return c.Tart.BaseImage, true
		}
		return "", false
	}
	if macos == "" || xcode == "" {
		return "", false
	}
	for _, image := range c.Tart.Images {
		if normalizeImageKey(image.MacOS) != macos {
			continue
		}
		if normalizeImageKey(image.Xcode) != xcode {
			continue
		}
		if !safeCirrusImageTag(image.Tag) {
			return "", false
		}
		return image.Tag, true
	}
	return "", false
}

func normalizeImageKey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func safeCirrusImageTag(tag string) bool {
	tag = strings.TrimSpace(tag)
	repository, version, ok := strings.Cut(tag, ":")
	if !ok {
		return false
	}
	if version == "" || strings.ContainsAny(version, "/ \t\r\n") {
		return false
	}
	if !strings.HasPrefix(repository, cirrusImagePrefix+"macos-") {
		return false
	}
	if !strings.HasSuffix(repository, "-xcode") {
		return false
	}
	macosPart := strings.TrimPrefix(repository, cirrusImagePrefix+"macos-")
	macosPart = strings.TrimSuffix(macosPart, "-xcode")
	if macosPart == "" {
		return false
	}
	return !strings.ContainsAny(macosPart, "/ \t\r\n")
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
	if c.App.WebhookSecretPath == "" {
		return nil, fmt.Errorf("config: webhook_secret_path is required")
	}
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
