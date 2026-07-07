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
	"time"

	"github.com/pelletier/go-toml/v2"
)

// DefaultBaseImage is the Cirrus base the golden is built from when config does
// not override it. It is the Xcode-bearing image (pinned) so the golden can
// build Swift; the CLI flag and config.toml both default to this.
const DefaultBaseImage = "ghcr.io/cirruslabs/macos-tahoe-xcode:26.5"

const (
	defaultWarmBudget                 = 2
	defaultGoldenBudget               = 3
	defaultRunnerCount                = 3
	defaultJobsPerVM                  = 1
	defaultMaintenanceIntervalSeconds = 3600
	cirrusImagePrefix                 = "ghcr.io/cirruslabs/"
	// defaultFastPullParallel is the skopeo layer-copy concurrency used when
	// fast_pull_parallel is unset. ghcr throttles each connection, so more
	// concurrent streams raise the cold-pull rate.
	defaultFastPullParallel = 16
)

// Config is the broker's runtime configuration.
type Config struct {
	// ListenAddr is the address the webhook and capacity server binds to.
	ListenAddr string `toml:"listen_addr"`

	// RunnerCount is the number of persistent worker VMs kept warm.
	RunnerCount int `toml:"runner_count"`

	// JobsPerVM is the number of concurrent runner slots inside each warm VM.
	JobsPerVM int `toml:"jobs_per_vm"`

	// MaxIdle is the idle age after which a worker VM is recycled. Zero or unset
	// disables idle recycling; the value is honored verbatim, not defaulted.
	MaxIdle Duration `toml:"max_idle"`

	// MaxAge is the total age after which a worker VM is recycled. Zero or unset
	// disables age recycling; the value is honored verbatim, not defaulted.
	MaxAge Duration `toml:"max_age"`

	// MaxBind is the maximum time a busy worker may stay bound before a dead
	// job probe can recycle it. Zero or unset uses the runner pool default.
	MaxBind Duration `toml:"max_bind"`

	// PickupTimeout is the time a busy worker may stay bound before a no-active
	// job probe can recycle it. Zero or unset uses the runner pool default.
	PickupTimeout Duration `toml:"pickup_timeout"`

	// StallTimeout is the low CPU duration after which a live job is logged as
	// stalled. Zero or unset uses the runner pool default.
	StallTimeout Duration `toml:"stall_timeout"`

	// StallReap allows a stalled busy worker to be recycled after logging.
	StallReap bool `toml:"stall_reap"`

	// App identifies the GitHub App and where its private key lives.
	App AppConfig `toml:"app"`

	// Tart configures the VM pool substrate.
	Tart TartConfig `toml:"tart"`

	// Maintenance configures the optional host maintenance launchd timer.
	Maintenance MaintenanceConfig `toml:"maintenance"`

	// Labels are the runner labels every JIT runner registers with. The
	// self-hosted job in CI targets one of these.
	Labels []string `toml:"labels"`
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
	// CacheDir is a host directory shared into each VM (virtiofs, guest path
	// /Volumes/My Shared Files/cache) so the build cache survives VM deletion.
	// Empty defaults to <home>/pool-cache, or <tmp>/gha-mac-broker-pool-cache
	// when the home dir cannot be resolved. It must not hold the base-image
	// blobs (see FastPullDir), which stay out of the mount.
	CacheDir string `toml:"cache_dir"`
	// FastPull enables the fast parallel base-image pull path when true. Nil
	// means enabled.
	FastPull *bool `toml:"fast_pull"`
	// FastPullDir is the OCI layout directory where skopeo stores pulled blobs.
	// skopeo is idempotent, so the layout is kept across runs for fast rebuilds.
	FastPullDir string `toml:"fast_pull_dir"`
	// FastPullParallel is how many image layers skopeo pulls simultaneously.
	// ghcr throttles each connection, so a higher count raises the cold-pull
	// rate. Zero or unset defaults to defaultFastPullParallel.
	FastPullParallel int `toml:"fast_pull_parallel"`
}

// MaintenanceConfig configures a host-side command run by an installer-managed
// launchd timer. An empty command disables the timer.
type MaintenanceConfig struct {
	// Command is the shell line the launchd timer runs. Empty disables it.
	Command string `toml:"command"`
	// IntervalSeconds is the launchd StartInterval value in seconds.
	IntervalSeconds int `toml:"interval_seconds"`
}

// ImageMapping maps a declared macOS and Xcode pair to an approved Cirrus tag.
type ImageMapping struct {
	MacOS string `toml:"macos"`
	Xcode string `toml:"xcode"`
	Tag   string `toml:"tag"`
}

// Duration parses TOML duration strings such as "2h" or "45m".
type Duration time.Duration

// UnmarshalText parses TOML strings.
func (d *Duration) UnmarshalText(text []byte) error {
	parsed, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("config: parse duration %q: %w", string(text), err)
	}
	*d = Duration(parsed)
	return nil
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
	if c.RunnerCount == 0 {
		c.RunnerCount = defaultRunnerCount
	}
	if c.JobsPerVM == 0 {
		c.JobsPerVM = defaultJobsPerVM
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
	if c.Tart.CacheDir == "" {
		if home, err := os.UserHomeDir(); err == nil {
			c.Tart.CacheDir = filepath.Join(home, "pool-cache")
		} else {
			c.Tart.CacheDir = filepath.Join(os.TempDir(), "gha-mac-broker-pool-cache")
		}
	}
	if c.Tart.FastPullDir == "" {
		// FastPullDir holds the multi-GB base-image blobs and must stay OUT of
		// CacheDir, because CacheDir is mounted into every guest and the image
		// blobs have no business inside a build VM.
		c.Tart.FastPullDir = filepath.Join(os.TempDir(), "gha-mac-broker-fastpull-blobs")
	}
	if c.Tart.FastPullParallel <= 0 {
		c.Tart.FastPullParallel = defaultFastPullParallel
	}
	if c.Maintenance.IntervalSeconds <= 0 {
		c.Maintenance.IntervalSeconds = defaultMaintenanceIntervalSeconds
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
	if len(missing) > 0 {
		return fmt.Errorf("config: missing required fields: %s", strings.Join(missing, ", "))
	}
	if c.RunnerCount < 1 {
		return fmt.Errorf("config: runner_count must be at least 1")
	}
	if c.JobsPerVM < 1 {
		return fmt.Errorf("config: jobs_per_vm must be at least 1")
	}
	if c.MaxIdle < 0 {
		return fmt.Errorf("config: max_idle must not be negative")
	}
	if c.MaxAge < 0 {
		return fmt.Errorf("config: max_age must not be negative")
	}
	if c.MaxBind < 0 {
		return fmt.Errorf("config: max_bind must not be negative")
	}
	if c.PickupTimeout < 0 {
		return fmt.Errorf("config: pickup_timeout must not be negative")
	}
	if c.StallTimeout < 0 {
		return fmt.Errorf("config: stall_timeout must not be negative")
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
	// CacheDir is mounted into every guest, so the multi-GB base-image blob
	// store must not live under it, whether an operator set fast_pull_dir
	// explicitly or a default placed it there.
	if c.Tart.CacheDir != "" && c.Tart.FastPullDir != "" && isWithin(c.Tart.FastPullDir, c.Tart.CacheDir) {
		return fmt.Errorf("config: tart.fast_pull_dir %q must not be inside tart.cache_dir %q (cache_dir is mounted into every VM)", c.Tart.FastPullDir, c.Tart.CacheDir)
	}
	return nil
}

// isWithin reports whether path is at or below root, comparing cleaned absolute
// paths so a relative or dot-laden entry cannot slip past the containment check.
func isWithin(path, root string) bool {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(absRoot, absPath)
	if err != nil {
		return false
	}
	return rel == "." || !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".."
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
