// Package golden builds the golden Tart VM image the broker pool clones. It
// clones a Cirrus base image, boots it, installs the GitHub Actions runner
// (unconfigured) and the guest liveness watchdog over the tart-exec vsock
// channel (no SSH, no IP), clean-shuts-down, snapshots the golden, then verifies
// a fresh clone so a broken image can never ship silently.
package golden

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os/exec"
	"slices"
	"strings"
	"time"

	"goodkind.io/gha-mac-broker/internal/tart"
)

//go:embed guest/gha-broker-watchdog.sh
var watchdogScript []byte

//go:embed guest/io.goodkind.gha-broker-watchdog.plist
var watchdogPlist []byte

const (
	watchdogScriptPath = "/usr/local/bin/gha-broker-watchdog.sh"
	watchdogPlistPath  = "/Library/LaunchDaemons/io.goodkind.gha-broker-watchdog.plist"
	readyTimeout       = 120 * time.Second
	readyInterval      = 3 * time.Second
	shutdownTimeout    = 90 * time.Second
	runnerLatestURL    = "https://api.github.com/repos/actions/runner/releases/latest"
)

// tarter is the VM substrate the builder drives; *tart.Tart satisfies it. It is
// an interface so tests can stub the tart CLI.
type tarter interface {
	List(ctx context.Context) ([]string, error)
	Clone(ctx context.Context, source, name string) error
	BootCommand(ctx context.Context, name string, opts tart.BootOptions) *exec.Cmd
	Exec(ctx context.Context, name string, argv ...string) ([]byte, error)
	Stop(ctx context.Context, name string) error
	Delete(ctx context.Context, name string) error
}

// Builder builds and self-verifies the golden image.
type Builder struct {
	vm tarter
}

// New returns a Builder driving the given VM substrate.
func New(vm tarter) *Builder {
	return &Builder{vm: vm}
}

// Options configures a golden build.
type Options struct {
	// BaseImage is the Cirrus base to clone (ships tart-guest-agent).
	BaseImage string
	// GoldenName is the snapshot the pool clones (e.g. "gha-golden").
	GoldenName string
	// BuildVM is the scratch VM name used during the build.
	BuildVM string
	// RunnerVersion is the actions/runner version to install (e.g. "2.335.1").
	RunnerVersion string
}

// EnsureOptions configures an idempotent golden ensure operation.
type EnsureOptions struct {
	// Image is the approved Cirrus tag the golden is derived from.
	Image string
	// BuildVM is the scratch VM name used if the golden must be built.
	BuildVM string
	// RunnerVersion is the actions/runner version to install. When empty, the
	// latest release is resolved only if a build is required.
	RunnerVersion string
}

// NameForImage returns the deterministic per-image golden name.
func NameForImage(image string) string {
	name := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(image)), "ghcr.io/cirruslabs/")
	var builder strings.Builder
	builder.WriteString("gha-golden-")
	for _, value := range name {
		if value >= 'a' && value <= 'z' {
			builder.WriteRune(value)
			continue
		}
		if value >= '0' && value <= '9' {
			builder.WriteRune(value)
			continue
		}
		if value == '.' || value == '-' {
			builder.WriteRune(value)
			continue
		}
		builder.WriteRune('-')
	}
	return strings.TrimRight(builder.String(), "-")
}

// EnsureGolden builds the derived golden for an image only when it is absent.
func (b *Builder) EnsureGolden(ctx context.Context, opts EnsureOptions) (string, error) {
	if opts.Image == "" {
		return "", fmt.Errorf("golden: image is required")
	}
	goldenName := NameForImage(opts.Image)
	names, err := b.vm.List(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "list VMs failed", "err", err)
		return "", fmt.Errorf("golden: list VMs: %w", err)
	}
	if slices.Contains(names, goldenName) {
		slog.InfoContext(ctx, "golden present; skipping build", "golden", goldenName, "image", opts.Image)
		return goldenName, nil
	}

	runnerVersion := opts.RunnerVersion
	if runnerVersion == "" {
		resolved, resolveErr := ResolveRunnerVersion(ctx)
		if resolveErr != nil {
			return "", resolveErr
		}
		runnerVersion = resolved
	}
	buildVM := opts.BuildVM
	if buildVM == "" {
		buildVM = goldenName + "-build"
	}
	if err := b.Build(ctx, Options{
		BaseImage:     opts.Image,
		GoldenName:    goldenName,
		BuildVM:       buildVM,
		RunnerVersion: runnerVersion,
	}); err != nil {
		slog.ErrorContext(ctx, "ensure golden build failed", "err", err, "golden", goldenName, "image", opts.Image)
		return "", fmt.Errorf("golden: ensure %s: %w", goldenName, err)
	}
	return goldenName, nil
}

// runnerRelease is the subset of the actions/runner latest-release API response
// the version resolver reads.
type runnerRelease struct {
	TagName string `json:"tag_name"`
}

// ResolveRunnerVersion fetches the latest actions/runner release tag and
// returns it without the leading "v".
func ResolveRunnerVersion(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, runnerLatestURL, nil)
	if err != nil {
		slog.ErrorContext(ctx, "build runner request failed", "err", err)
		return "", fmt.Errorf("golden: build runner request: %w", err)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		slog.ErrorContext(ctx, "resolve runner version request failed", "err", err)
		return "", fmt.Errorf("golden: fetch latest runner: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		slog.ErrorContext(ctx, "runner status not ok", "err", fmt.Errorf("status %d", resp.StatusCode), "status", resp.StatusCode)
		return "", fmt.Errorf("golden: latest runner status %d", resp.StatusCode)
	}
	var rel runnerRelease
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		slog.ErrorContext(ctx, "decode runner release failed", "err", err)
		return "", fmt.Errorf("golden: decode runner release: %w", err)
	}
	return strings.TrimPrefix(rel.TagName, "v"), nil
}

// Build runs the full golden build and self-verification. On any step failure it
// tears down scratch VMs and returns; on success the golden is ready and proven.
func (b *Builder) Build(ctx context.Context, opts Options) error {
	slog.InfoContext(ctx, "building golden", "base", opts.BaseImage, "golden", opts.GoldenName, "runner", opts.RunnerVersion)

	boot, err := b.cloneAndBoot(ctx, opts.BaseImage, opts.BuildVM)
	if err != nil {
		return err
	}

	if err := b.provision(ctx, opts.BuildVM, opts.RunnerVersion); err != nil {
		_ = boot.Process.Kill()
		b.teardown(ctx, opts.BuildVM)
		return err
	}

	if err := b.cleanShutdown(ctx, opts.BuildVM); err != nil {
		_ = boot.Process.Kill()
		b.teardown(ctx, opts.BuildVM)
		return err
	}
	_ = boot.Process.Kill()

	if err := b.snapshot(ctx, opts.BuildVM, opts.GoldenName); err != nil {
		return err
	}

	return b.verify(ctx, opts.GoldenName, opts.BuildVM+"-verify")
}

// provision installs the runner and bakes the watchdog into the booted VM.
func (b *Builder) provision(ctx context.Context, name, runnerVersion string) error {
	if err := b.installRunner(ctx, name, runnerVersion); err != nil {
		return err
	}
	return b.installWatchdog(ctx, name)
}

// snapshot replaces the golden with a clone of the prepared, stopped build VM,
// then removes the build VM.
func (b *Builder) snapshot(ctx context.Context, buildVM, golden string) error {
	if err := b.vm.Delete(ctx, golden); err != nil {
		slog.DebugContext(ctx, "delete old golden (ignored if absent)", "err", err, "golden", golden)
	}
	if err := b.vm.Clone(ctx, buildVM, golden); err != nil {
		slog.ErrorContext(ctx, "snapshot golden failed", "err", err, "golden", golden)
		return fmt.Errorf("golden: snapshot %s: %w", golden, err)
	}
	if err := b.vm.Delete(ctx, buildVM); err != nil {
		slog.WarnContext(ctx, "delete build vm failed", "err", err, "vm", buildVM)
	}
	return nil
}

// cloneAndBoot clones source to name, boots it headless, and waits for the vsock
// channel. The returned boot process must be killed by the caller.
func (b *Builder) cloneAndBoot(ctx context.Context, source, name string) (*exec.Cmd, error) {
	if err := b.vm.Delete(ctx, name); err != nil {
		slog.DebugContext(ctx, "pre-clone delete (ignored if absent)", "err", err, "vm", name)
	}
	if err := b.vm.Clone(ctx, source, name); err != nil {
		slog.ErrorContext(ctx, "clone failed", "err", err, "vm", name, "source", source)
		return nil, fmt.Errorf("golden: clone %s from %s: %w", name, source, err)
	}
	boot := b.vm.BootCommand(ctx, name, tart.BootOptions{NoGraphics: true, Dirs: nil})
	if err := boot.Start(); err != nil {
		slog.ErrorContext(ctx, "boot failed", "err", err, "vm", name)
		b.teardown(ctx, name)
		return nil, fmt.Errorf("golden: boot %s: %w", name, err)
	}
	if err := b.waitForReady(ctx, name); err != nil {
		_ = boot.Process.Kill()
		b.teardown(ctx, name)
		return nil, err
	}
	return boot, nil
}

// waitForReady polls the guest vsock channel until it answers or times out.
func (b *Builder) waitForReady(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	ticker := time.NewTicker(readyInterval)
	defer ticker.Stop()
	for {
		if _, err := b.vm.Exec(ctx, name, "true"); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			slog.ErrorContext(ctx, "timed out waiting for vsock readiness", "err", ctx.Err(), "vm", name)
			return fmt.Errorf("golden: waiting for vsock readiness of %s: %w", name, ctx.Err())
		case <-ticker.C:
		}
	}
}

// installRunner downloads and unpacks the GitHub Actions runner, unconfigured,
// into ~/actions-runner inside the VM.
func (b *Builder) installRunner(ctx context.Context, name, version string) error {
	url := fmt.Sprintf("https://github.com/actions/runner/releases/download/v%s/actions-runner-osx-arm64-%s.tar.gz", version, version)
	script := fmt.Sprintf("set -e; mkdir -p ~/actions-runner && cd ~/actions-runner && "+
		"curl -fsSL -o runner.tar.gz %q && tar xzf runner.tar.gz && rm runner.tar.gz && test -f run.sh", url)
	slog.InfoContext(ctx, "installing runner", "vm", name, "version", version)
	if _, err := b.vm.Exec(ctx, name, "bash", "-lc", script); err != nil {
		slog.ErrorContext(ctx, "install runner failed", "err", err, "vm", name)
		return fmt.Errorf("golden: install runner on %s: %w", name, err)
	}
	return nil
}

// installWatchdog bakes the guest liveness watchdog (script + LaunchDaemon
// plist) into the VM. The plist in /Library/LaunchDaemons auto-loads on every
// subsequent boot, so no launchctl call is needed here.
func (b *Builder) installWatchdog(ctx context.Context, name string) error {
	slog.InfoContext(ctx, "installing watchdog", "vm", name)
	if err := b.writeGuestFile(ctx, name, watchdogScript, watchdogScriptPath, "755"); err != nil {
		return err
	}
	return b.writeGuestFile(ctx, name, watchdogPlist, watchdogPlistPath, "644")
}

// writeGuestFile writes content to dest inside the VM with the given chmod mode,
// transferring it base64-encoded over tart exec (no special-char or stdin
// handling needed). The macOS guest decodes with `base64 -D`.
func (b *Builder) writeGuestFile(ctx context.Context, name string, content []byte, dest, mode string) error {
	enc := base64.StdEncoding.EncodeToString(content)
	script := fmt.Sprintf("echo %s | base64 -D | sudo tee %s >/dev/null && sudo chmod %s %s", enc, dest, mode, dest)
	if _, err := b.vm.Exec(ctx, name, "bash", "-lc", script); err != nil {
		slog.ErrorContext(ctx, "write guest file failed", "err", err, "vm", name, "dest", dest)
		return fmt.Errorf("golden: write %s on %s: %w", dest, name, err)
	}
	return nil
}

// cleanShutdown asks the guest OS to power off (flushing disk) and waits until
// the vsock channel stops answering, confirming the VM is down before snapshot.
func (b *Builder) cleanShutdown(ctx context.Context, name string) error {
	slog.InfoContext(ctx, "shutting down build vm", "vm", name)
	// The connection drops as the VM powers off, so an error here is expected.
	// Use the absolute path: tart exec's PATH does not include /sbin.
	if _, err := b.vm.Exec(ctx, name, "sudo", "/sbin/shutdown", "-h", "now"); err != nil {
		slog.DebugContext(ctx, "shutdown exec returned error (expected as vm powers off)", "err", err, "vm", name)
	}
	deadline, cancel := context.WithTimeout(ctx, shutdownTimeout)
	defer cancel()
	ticker := time.NewTicker(readyInterval)
	defer ticker.Stop()
	for {
		if _, err := b.vm.Exec(deadline, name, "true"); err != nil {
			return nil // no longer reachable: the VM is down
		}
		select {
		case <-deadline.Done():
			slog.ErrorContext(ctx, "timed out waiting for build vm to power off", "err", deadline.Err(), "vm", name)
			return fmt.Errorf("golden: build vm %s did not power off: %w", name, deadline.Err())
		case <-ticker.C:
		}
	}
}

// verify clones the freshly built golden, boots it, and confirms the runner and
// the watchdog are actually present and the watchdog auto-loaded, failing loudly
// otherwise. The verify VM is always torn down.
func (b *Builder) verify(ctx context.Context, golden, verifyVM string) error {
	slog.InfoContext(ctx, "verifying golden", "golden", golden)
	boot, err := b.cloneAndBoot(ctx, golden, verifyVM)
	if err != nil {
		return fmt.Errorf("golden: verify boot: %w", err)
	}
	defer func() {
		_ = boot.Process.Kill()
		b.teardown(ctx, verifyVM)
	}()

	if _, err := b.vm.Exec(ctx, verifyVM, "bash", "-lc", "test -f ~/actions-runner/run.sh"); err != nil {
		slog.ErrorContext(ctx, "verify failed: runner missing", "err", err, "golden", golden)
		return fmt.Errorf("golden: verify %s: runner run.sh missing: %w", golden, err)
	}
	check := "test -x " + watchdogScriptPath + " && sudo launchctl print system/io.goodkind.gha-broker-watchdog >/dev/null 2>&1"
	if _, err := b.vm.Exec(ctx, verifyVM, "bash", "-lc", check); err != nil {
		slog.ErrorContext(ctx, "verify failed: watchdog not active", "err", err, "golden", golden)
		return fmt.Errorf("golden: verify %s: watchdog not installed/loaded: %w", golden, err)
	}
	// A golden that cannot build Swift is useless, so fail loudly here rather
	// than ship an image whose first real build dies at Xcode selection.
	if _, err := b.vm.Exec(ctx, verifyVM, "bash", "-lc", "xcodebuild -version"); err != nil {
		slog.ErrorContext(ctx, "verify failed: xcodebuild -version did not succeed", "err", err, "golden", golden)
		return fmt.Errorf("golden: verify %s: xcodebuild -version failed (Xcode absent, not selected, or license/first-run pending): %w", golden, err)
	}
	slog.InfoContext(ctx, "golden verified", "golden", golden)
	return nil
}

// teardown stops and deletes a VM, best effort.
func (b *Builder) teardown(ctx context.Context, name string) {
	if err := b.vm.Stop(ctx, name); err != nil {
		slog.WarnContext(ctx, "vm stop failed", "err", err, "vm", name)
	}
	if err := b.vm.Delete(ctx, name); err != nil {
		slog.WarnContext(ctx, "vm delete failed", "err", err, "vm", name)
	}
}
