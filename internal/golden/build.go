// Package golden builds the golden Tart VM image the broker pool clones. It
// clones a Cirrus base image, boots it, and runs an all-Go golden-provision
// subcommand over the tart-exec vsock channel (no SSH, no IP): the provisioner
// installs the GitHub Actions runner (unconfigured), bakes the guest broker
// binary and the guest-agent LaunchDaemon, and persists a golden
// fingerprint. The builder then host-side stops the VM, snapshots the golden,
// and verifies a fresh clone so a broken image can never ship silently.
package golden

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"goodkind.io/gha-mac-broker/internal/tart"
)

const (
	readyTimeout    = 120 * time.Second
	readyInterval   = 3 * time.Second
	runnerLatestURL = "https://api.github.com/repos/actions/runner/releases/latest"
	// provisionMountName is the tart --dir share name that carries the signed
	// guest binary into the build VM.
	provisionMountName = "ghaprovision"
	// tartSharedMountRoot is where tart surfaces --dir shares inside a macOS guest.
	tartSharedMountRoot = "/Volumes/My Shared Files"
	// provisionBinaryName is the guest binary filename inside the scratch mount.
	provisionBinaryName = "gha-mac-broker"
	// localProvisionerPath is the in-VM path the host copies the mounted binary to
	// before signing and running it, so it never execs straight off the share.
	localProvisionerPath = "/tmp/gha-mac-broker-provision"
	// runnerTarballTimeout bounds the host-side runner tarball fetch used only to
	// compute the fingerprint digest.
	runnerTarballTimeout = 10 * time.Minute
	// goldenStagingSuffix names the staging image a build snapshots and verifies
	// before it replaces the live golden, so a failed rebuild never destroys the
	// existing golden.
	goldenStagingSuffix = "-staging"
)

// tarter is the VM substrate the builder drives; *tart.Tart satisfies it. It is
// an interface so tests can stub the tart CLI.
type tarter interface {
	List(ctx context.Context) ([]string, error)
	Clone(ctx context.Context, source, name string, insecure bool) error
	BootCommand(ctx context.Context, name string, opts tart.BootOptions) *exec.Cmd
	Exec(ctx context.Context, name string, argv ...string) ([]byte, error)
	IP(ctx context.Context, name string) (string, error)
	Stop(ctx context.Context, name string) error
	Delete(ctx context.Context, name string) error
}

// BaseStager stages a base image into a local registry and returns a clonable
// ref plus a stop func that shuts the registry down.
type BaseStager interface {
	Stage(ctx context.Context, image string) (ref string, stop func(), err error)
}

// runnerTarballDigester returns the content digest of the actions/runner release
// tarball for a version. It is a field so tests can supply a digest without
// downloading the tarball.
type runnerTarballDigester func(ctx context.Context, version string) (string, error)

// Option configures a Builder.
type Option func(*Builder)

// WithBaseStager sets the base-image stager used to fast-pull the base before
// cloning. When unset, Build clones the configured base ref directly.
func WithBaseStager(s BaseStager) Option {
	return func(b *Builder) { b.base = s }
}

// Builder builds and self-verifies the golden image.
type Builder struct {
	vm            tarter
	base          BaseStager
	runnerDigest  runnerTarballDigester
	resolveRunner func(ctx context.Context) (string, error)
}

// New returns a Builder driving the given VM substrate.
func New(vm tarter, opts ...Option) *Builder {
	builder := &Builder{
		vm:            vm,
		base:          nil,
		runnerDigest:  downloadRunnerTarballDigest,
		resolveRunner: ResolveRunnerVersion,
	}
	for _, opt := range opts {
		opt(builder)
	}
	return builder
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
	// BinaryPath is the guest broker binary baked into the image. When empty the
	// running executable is baked, so a bare host bootstrap needs no extra flag.
	BinaryPath string
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

// EnsureGolden builds the derived golden for an image unless the existing golden
// already carries the fingerprint the current binary and config would bake. It
// reads the baked fingerprint out of the present golden and rebuilds when it
// differs or cannot be read, so a golden baked by an older binary is replaced
// rather than silently kept. A matching golden is a no-op, so the command stays
// idempotent.
func (b *Builder) EnsureGolden(ctx context.Context, opts EnsureOptions) (string, error) {
	if opts.Image == "" {
		return "", fmt.Errorf("golden: image is required")
	}
	goldenName := NameForImage(opts.Image)
	buildVM := opts.BuildVM
	if buildVM == "" {
		buildVM = goldenName + "-build"
	}

	names, err := b.vm.List(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "list VMs failed", "err", err)
		return "", fmt.Errorf("golden: list VMs: %w", err)
	}

	// Compute the expected fingerprint only when a golden already exists, so the
	// idempotency check pays the runner-tarball fetch. The absent-golden path
	// goes straight to Build, which computes the same fingerprint from the same
	// inputs, so a fresh build never fetches the runner tarball twice.
	runnerVersion := opts.RunnerVersion
	if slices.Contains(names, goldenName) {
		expected, resolvedVersion, fpErr := b.expectedFingerprint(ctx, Options{
			BaseImage:     opts.Image,
			GoldenName:    goldenName,
			BuildVM:       buildVM,
			RunnerVersion: opts.RunnerVersion,
			BinaryPath:    "",
		})
		if fpErr != nil {
			return "", fpErr
		}
		runnerVersion = resolvedVersion
		baked, bakedErr := b.bakedFingerprint(ctx, goldenName)
		if bakedErr == nil && baked == expected {
			slog.InfoContext(ctx, "golden fingerprint current; skipping build", "golden", goldenName, "fingerprint", expected)
			return goldenName, nil
		}
		if bakedErr != nil {
			slog.InfoContext(ctx, "golden stale; rebuilding", "golden", goldenName, "read_err", bakedErr, "expected", expected)
		} else {
			slog.InfoContext(ctx, "golden stale; rebuilding", "golden", goldenName, "baked", baked, "expected", expected)
		}
		// The stale golden stays in place until Build snapshots, verifies, and
		// promotes its replacement, so a failed rebuild leaves the pool a working
		// golden to clone.
	}

	if err := b.Build(ctx, Options{
		BaseImage:     opts.Image,
		GoldenName:    goldenName,
		BuildVM:       buildVM,
		RunnerVersion: runnerVersion,
		BinaryPath:    "",
	}); err != nil {
		slog.ErrorContext(ctx, "ensure golden build failed", "err", err, "golden", goldenName, "image", opts.Image)
		return "", fmt.Errorf("golden: ensure %s: %w", goldenName, err)
	}
	return goldenName, nil
}

// expectedFingerprint computes the golden fingerprint the current binary and
// config would bake for opts, resolving the runner version when it is empty and
// returning the resolved version so the caller can pass it to Build unchanged. It
// shares the fingerprint assembly with stageProvisionInputs, so the idempotency
// check and the build path can never compute different fingerprints for the same
// inputs.
func (b *Builder) expectedFingerprint(ctx context.Context, opts Options) (fingerprint, runnerVersion string, err error) {
	_, fingerprint, runnerVersion, _, err = b.fingerprintFor(ctx, opts)
	return fingerprint, runnerVersion, err
}

// fingerprintFor resolves the baked-binary path, the runner version, the runner
// tarball digest, and the baked-binary digest for opts, then folds them plus the
// guest-agent plist payload into the golden fingerprint. It returns each
// resolved input so both the build path and the check path can reuse the exact
// same computation.
func (b *Builder) fingerprintFor(ctx context.Context, opts Options) (binaryPath, fingerprint, runnerVersion, runnerDigest string, err error) {
	binaryPath = opts.BinaryPath
	if binaryPath == "" {
		exe, exeErr := os.Executable()
		if exeErr != nil {
			slog.ErrorContext(ctx, "resolve executable for golden fingerprint failed", "err", exeErr)
			return "", "", "", "", fmt.Errorf("golden: resolve executable: %w", exeErr)
		}
		binaryPath = exe
	}
	runnerVersion = opts.RunnerVersion
	if runnerVersion == "" {
		resolved, resolveErr := b.resolveRunner(ctx)
		if resolveErr != nil {
			return "", "", "", "", resolveErr
		}
		runnerVersion = resolved
	}
	runnerDigest, err = b.runnerDigest(ctx, runnerVersion)
	if err != nil {
		return "", "", "", "", err
	}
	binaryDigest, err := sha256File(ctx, binaryPath)
	if err != nil {
		return "", "", "", "", err
	}
	fingerprint = Fingerprint(FingerprintInputs{
		BaseImageRef:        opts.BaseImage,
		RunnerVersion:       runnerVersion,
		RunnerTarballDigest: runnerDigest,
		BinaryDigest:        binaryDigest,
		Payloads: []PayloadDigest{
			{Name: GuestAgentPlistLabel, Digest: sha256Bytes(GuestAgentPlist())},
		},
	})
	return binaryPath, fingerprint, runnerVersion, runnerDigest, nil
}

// bakedFingerprint reads the fingerprint baked into an existing golden by cloning
// and booting a throwaway clone and catting the baked fingerprint file, then
// tearing the clone down. A clone, boot, read, or empty-file failure returns an
// error, which the caller treats as stale so the golden is rebuilt.
func (b *Builder) bakedFingerprint(ctx context.Context, goldenName string) (string, error) {
	cloneName := goldenName + "-fpcheck"
	boot, err := b.cloneAndBoot(ctx, goldenName, cloneName, false, nil)
	if err != nil {
		return "", fmt.Errorf("golden: read baked fingerprint of %s: %w", goldenName, err)
	}
	defer func() {
		_ = boot.Process.Kill()
		b.teardown(ctx, cloneName)
	}()
	out, err := b.vm.Exec(ctx, cloneName, "cat", FingerprintPath)
	if err != nil {
		slog.WarnContext(ctx, "read baked fingerprint failed", "err", err, "golden", goldenName)
		return "", fmt.Errorf("golden: read baked fingerprint from %s: %w", goldenName, err)
	}
	baked := strings.TrimSpace(string(out))
	if baked == "" {
		return "", fmt.Errorf("golden: baked fingerprint on %s is empty", goldenName)
	}
	return baked, nil
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

	slog.InfoContext(ctx, "golden phase: staging provision inputs (fingerprint, guest binary, runner digest)", "golden", opts.GoldenName)
	scratchDir, mountBinary, fingerprint, runnerVersion, runnerDigest, err := b.stageProvisionInputs(ctx, opts)
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(scratchDir) }()

	cloneSource, insecure := opts.BaseImage, false
	if b.base != nil {
		ref, stop, stageErr := b.base.Stage(ctx, opts.BaseImage)
		if stageErr != nil {
			slog.WarnContext(ctx, "fast-pull stage failed; cloning base directly", "err", stageErr, "image", opts.BaseImage)
		} else {
			defer stop()
			cloneSource, insecure = ref, true
		}
	}
	dirs := []tart.DirMount{{Name: provisionMountName, Path: scratchDir}}
	slog.InfoContext(ctx, "golden phase: cloning and booting build VM", "golden", opts.GoldenName, "vm", opts.BuildVM)
	boot, err := b.cloneAndBoot(ctx, cloneSource, opts.BuildVM, insecure, dirs)
	if err != nil {
		return err
	}

	slog.InfoContext(ctx, "golden phase: provisioning build VM (install runner, bake guest binary and agent)", "golden", opts.GoldenName, "vm", opts.BuildVM)
	if err := b.provision(ctx, opts.BuildVM, runnerVersion, runnerDigest, mountBinary, fingerprint); err != nil {
		_ = boot.Process.Kill()
		b.teardown(ctx, opts.BuildVM)
		return err
	}

	slog.InfoContext(ctx, "golden phase: stopping build VM", "golden", opts.GoldenName, "vm", opts.BuildVM)
	if err := b.stopBuildVM(ctx, opts.BuildVM); err != nil {
		_ = boot.Process.Kill()
		b.teardown(ctx, opts.BuildVM)
		return err
	}
	_ = boot.Process.Kill()

	// Snapshot to a staging image and verify it before it replaces the live
	// golden, so a clone, boot, or verify failure leaves the existing golden
	// intact instead of destroying it.
	staging := opts.GoldenName + goldenStagingSuffix
	slog.InfoContext(ctx, "golden phase: snapshotting staging image", "golden", opts.GoldenName, "staging", staging)
	if err := b.snapshot(ctx, opts.BuildVM, staging); err != nil {
		return err
	}
	slog.InfoContext(ctx, "golden phase: verifying staging image", "golden", opts.GoldenName, "staging", staging)
	if err := b.verify(ctx, staging, opts.BuildVM+"-verify", fingerprint); err != nil {
		b.teardown(ctx, staging)
		return err
	}
	slog.InfoContext(ctx, "golden phase: promoting staging to live golden", "golden", opts.GoldenName, "staging", staging)
	return b.promote(ctx, staging, opts.GoldenName)
}

// promote replaces the live golden with the verified staging image using a fast
// copy-on-write clone, then drops the staging image. The golden is absent only
// for the moment between the delete and the clone, which is far safer than
// deleting it before the replacement is built and proven.
func (b *Builder) promote(ctx context.Context, staging, golden string) error {
	if err := b.vm.Delete(ctx, golden); err != nil {
		slog.DebugContext(ctx, "delete old golden before promote (ignored if absent)", "err", err, "golden", golden)
	}
	if err := b.vm.Clone(ctx, staging, golden, false); err != nil {
		slog.ErrorContext(ctx, "promote staging to golden failed", "err", err, "golden", golden, "staging", staging)
		return fmt.Errorf("golden: promote %s to %s: %w", staging, golden, err)
	}
	if err := b.vm.Delete(ctx, staging); err != nil {
		slog.WarnContext(ctx, "delete staging after promote failed", "err", err, "staging", staging)
	}
	return nil
}

// stageProvisionInputs prepares a host scratch dir holding the guest binary to
// mount into the build VM, and computes the golden fingerprint host-side via
// fingerprintFor, the same helper the idempotency check uses, so the built
// image's fingerprint equals the one EnsureGolden expects. The fingerprint is a
// pure function of its inputs, so it is unit-testable without a VM.
func (b *Builder) stageProvisionInputs(ctx context.Context, opts Options) (scratchDir, mountBinary, fingerprint, runnerVersion, runnerDigest string, err error) {
	binaryPath, fingerprint, runnerVersion, runnerDigest, err := b.fingerprintFor(ctx, opts)
	if err != nil {
		return "", "", "", "", "", err
	}
	scratchDir, err = os.MkdirTemp("", "gha-golden-provision-")
	if err != nil {
		slog.ErrorContext(ctx, "create provision scratch dir failed", "err", err)
		return "", "", "", "", "", fmt.Errorf("golden: create provision scratch dir: %w", err)
	}
	scratchBinary := filepath.Join(scratchDir, provisionBinaryName)
	if err := copyFileMode(ctx, binaryPath, scratchBinary, 0o755); err != nil {
		_ = os.RemoveAll(scratchDir)
		return "", "", "", "", "", err
	}
	mountBinary = tartSharedMountRoot + "/" + provisionMountName + "/" + provisionBinaryName
	return scratchDir, mountBinary, fingerprint, runnerVersion, runnerDigest, nil
}

// provision lands the all-Go provisioner into the booted build VM and runs it.
// The mounted binary is copied to a local path, its quarantine attribute cleared,
// and it is ad-hoc signed before it runs, all via discrete-argv tart exec with no
// shell. The provisioner then installs the runner, bakes the binary and launchd
// unit, persists the fingerprint, and deletes the retired watchdog.
func (b *Builder) provision(ctx context.Context, name, runnerVersion, runnerDigest, mountBinary, fingerprint string) error {
	homeOut, err := b.vm.Exec(ctx, name, "printenv", "HOME")
	if err != nil {
		slog.ErrorContext(ctx, "resolve guest home failed", "err", err, "vm", name)
		return fmt.Errorf("golden: resolve guest home on %s: %w", name, err)
	}
	guestHome := strings.TrimSpace(string(homeOut))
	if guestHome == "" {
		return fmt.Errorf("golden: guest home is empty on %s", name)
	}
	runnerDir := guestHome + "/actions-runner"

	steps := [][]string{
		{"cp", mountBinary, localProvisionerPath},
		{"xattr", "-c", localProvisionerPath},
		{"codesign", "-s", "-", "-f", localProvisionerPath},
		{
			"sudo", localProvisionerPath, "golden-provision",
			"-runner-version", runnerVersion,
			"-runner-digest", runnerDigest,
			"-binary", mountBinary,
			"-runner-dir", runnerDir,
			"-fingerprint", fingerprint,
		},
	}
	slog.InfoContext(ctx, "provisioning golden", "vm", name, "runner", runnerVersion, "runner_dir", runnerDir)
	for _, argv := range steps {
		if _, err := b.vm.Exec(ctx, name, argv...); err != nil {
			slog.ErrorContext(ctx, "provision step failed", "err", err, "vm", name, "step", argv[0])
			return fmt.Errorf("golden: provision %s on %s: %w", argv[0], name, err)
		}
	}
	// The provisioner writes the runner, baked binary, supervisor plist, and
	// fingerprint through the guest page cache. The build stops and clones this VM
	// almost immediately, before macOS flushes those pages, so the snapshot can
	// miss them and fail verify with a missing fingerprint. A guest sync forces
	// the writes to the virtual disk before the snapshot captures it.
	if _, err := b.vm.Exec(ctx, name, "sync"); err != nil {
		slog.ErrorContext(ctx, "guest sync after provision failed", "err", err, "vm", name)
		return fmt.Errorf("golden: sync guest filesystem on %s: %w", name, err)
	}
	return nil
}

// snapshot replaces the golden with a clone of the prepared, stopped build VM,
// then removes the build VM.
func (b *Builder) snapshot(ctx context.Context, buildVM, golden string) error {
	if err := b.vm.Delete(ctx, golden); err != nil {
		slog.DebugContext(ctx, "delete old golden (ignored if absent)", "err", err, "golden", golden)
	}
	if err := b.vm.Clone(ctx, buildVM, golden, false); err != nil {
		slog.ErrorContext(ctx, "snapshot golden failed", "err", err, "golden", golden)
		return fmt.Errorf("golden: snapshot %s: %w", golden, err)
	}
	if err := b.vm.Delete(ctx, buildVM); err != nil {
		slog.WarnContext(ctx, "delete build vm failed", "err", err, "vm", buildVM)
	}
	return nil
}

// cloneAndBoot clones source to name, boots it headless with the given shared
// directories, and waits for the vsock channel. The returned boot process must be
// killed by the caller.
func (b *Builder) cloneAndBoot(ctx context.Context, source, name string, insecure bool, dirs []tart.DirMount) (*exec.Cmd, error) {
	if err := b.vm.Delete(ctx, name); err != nil {
		slog.DebugContext(ctx, "pre-clone delete (ignored if absent)", "err", err, "vm", name)
	}
	if err := b.vm.Clone(ctx, source, name, insecure); err != nil {
		slog.ErrorContext(ctx, "clone failed", "err", err, "vm", name, "source", source)
		return nil, fmt.Errorf("golden: clone %s from %s: %w", name, source, err)
	}
	boot := b.vm.BootCommand(ctx, name, tart.BootOptions{Dirs: dirs, Detach: false, NoGraphics: true})
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

// stopBuildVM powers the build VM off host-side with tart stop, so the disk is
// flushed before the snapshot without an in-guest shutdown command.
func (b *Builder) stopBuildVM(ctx context.Context, name string) error {
	slog.InfoContext(ctx, "stopping build vm", "vm", name)
	if err := b.vm.Stop(ctx, name); err != nil {
		slog.ErrorContext(ctx, "stop build vm failed", "err", err, "vm", name)
		return fmt.Errorf("golden: stop build vm %s: %w", name, err)
	}
	return nil
}

// verify clones the freshly built golden, boots it, and confirms the runner, the
// guest-agent launchd unit, and the baked fingerprint are present and the
// fingerprint matches the computed value, failing loudly otherwise. It also
// asserts the retired watchdog script was not baked. The verify VM is always torn
// down.
func (b *Builder) verify(ctx context.Context, golden, verifyVM, fingerprint string) error {
	slog.InfoContext(ctx, "verifying golden", "golden", golden)
	boot, err := b.cloneAndBoot(ctx, golden, verifyVM, false, nil)
	if err != nil {
		return fmt.Errorf("golden: verify boot: %w", err)
	}
	defer func() {
		_ = boot.Process.Kill()
		b.teardown(ctx, verifyVM)
	}()

	if _, err := b.vm.IP(ctx, verifyVM); err != nil {
		slog.ErrorContext(ctx, "verify failed: tart ip did not resolve", "err", err, "golden", golden)
		return fmt.Errorf("golden: verify %s: tart ip did not resolve: %w", golden, err)
	}

	homeOut, err := b.vm.Exec(ctx, verifyVM, "printenv", "HOME")
	if err != nil {
		return fmt.Errorf("golden: verify %s: resolve guest home: %w", golden, err)
	}
	runnerRun := strings.TrimSpace(string(homeOut)) + "/actions-runner/run.sh"
	if _, err := b.vm.Exec(ctx, verifyVM, "test", "-f", runnerRun); err != nil {
		slog.ErrorContext(ctx, "verify failed: runner missing", "err", err, "golden", golden)
		return fmt.Errorf("golden: verify %s: runner run.sh missing: %w", golden, err)
	}

	baked, err := b.vm.Exec(ctx, verifyVM, "cat", FingerprintPath)
	if err != nil {
		slog.ErrorContext(ctx, "verify failed: fingerprint file missing", "err", err, "golden", golden)
		return fmt.Errorf("golden: verify %s: read fingerprint: %w", golden, err)
	}
	bakedFingerprint := strings.TrimSpace(string(baked))
	if bakedFingerprint == "" {
		return fmt.Errorf("golden: verify %s: baked fingerprint is empty", golden)
	}
	if bakedFingerprint != fingerprint {
		return fmt.Errorf("golden: verify %s: baked fingerprint %q does not match computed %q", golden, bakedFingerprint, fingerprint)
	}

	if _, err := b.vm.Exec(ctx, verifyVM, "test", "-f", GuestAgentPlistPath); err != nil {
		slog.ErrorContext(ctx, "verify failed: guest agent plist missing", "err", err, "golden", golden)
		return fmt.Errorf("golden: verify %s: guest agent plist missing: %w", golden, err)
	}
	if _, err := b.vm.Exec(ctx, verifyVM, "sudo", "launchctl", "print", "system/"+GuestAgentPlistLabel); err != nil {
		slog.ErrorContext(ctx, "verify failed: guest agent unit not loaded", "err", err, "golden", golden)
		return fmt.Errorf("golden: verify %s: guest-agent unit not loaded: %w", golden, err)
	}
	if _, err := b.vm.Exec(ctx, verifyVM, "test", "!", "-e", LegacyWatchdogScriptPath); err != nil {
		slog.ErrorContext(ctx, "verify failed: watchdog shell script was baked", "err", err, "golden", golden)
		return fmt.Errorf("golden: verify %s: retired watchdog script %s is present: %w", golden, LegacyWatchdogScriptPath, err)
	}
	// A golden that cannot build Swift is useless, so fail loudly here rather
	// than ship an image whose first real build dies at Xcode selection.
	if _, err := b.vm.Exec(ctx, verifyVM, "xcodebuild", "-version"); err != nil {
		slog.ErrorContext(ctx, "verify failed: xcodebuild -version did not succeed", "err", err, "golden", golden)
		return fmt.Errorf("golden: verify %s: xcodebuild -version failed (Xcode absent, not selected, or license/first-run pending): %w", golden, err)
	}
	slog.InfoContext(ctx, "golden verified", "golden", golden, "fingerprint", fingerprint)
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

// downloadRunnerTarballDigest fetches the actions/runner release tarball and
// returns its sha256, so the fingerprint changes if the runner bytes change.
func downloadRunnerTarballDigest(ctx context.Context, version string) (string, error) {
	url := fmt.Sprintf("https://github.com/actions/runner/releases/download/v%s/actions-runner-osx-arm64-%s.tar.gz", version, version)
	// Not prefixed "golden phase:" because the digest also feeds the existing-golden
	// fingerprint check, so this fetch runs on a skipped build too and is not proof a
	// build started.
	slog.InfoContext(ctx, "golden: fetching runner tarball for digest", "version", version, "url", url)
	fetchCtx, cancel := context.WithTimeout(ctx, runnerTarballTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, url, nil)
	if err != nil {
		slog.ErrorContext(ctx, "build runner tarball request failed", "err", err, "url", url)
		return "", fmt.Errorf("golden: build runner tarball request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.ErrorContext(ctx, "fetch runner tarball failed", "err", err, "url", url)
		return "", fmt.Errorf("golden: fetch runner tarball %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		statusErr := fmt.Errorf("status %d", resp.StatusCode)
		slog.ErrorContext(ctx, "fetch runner tarball bad status", "err", statusErr, "url", url)
		return "", fmt.Errorf("golden: fetch runner tarball %s: %w", url, statusErr)
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, resp.Body); err != nil {
		slog.ErrorContext(ctx, "hash runner tarball failed", "err", err, "url", url)
		return "", fmt.Errorf("golden: hash runner tarball: %w", err)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// sha256File returns the hex sha256 of a file's contents.
func sha256File(ctx context.Context, path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		slog.ErrorContext(ctx, "open file for digest failed", "err", err, "path", path)
		return "", fmt.Errorf("golden: open %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		slog.ErrorContext(ctx, "digest file failed", "err", err, "path", path)
		return "", fmt.Errorf("golden: digest %s: %w", path, err)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// sha256Bytes returns the hex sha256 of a byte slice.
func sha256Bytes(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

// copyFileMode copies source to dest atomically via a temp file and rename,
// creating dest's parent directory and setting the given mode.
func copyFileMode(ctx context.Context, source, dest string, mode os.FileMode) error {
	destDir := filepath.Dir(dest)
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		slog.ErrorContext(ctx, "create binary dir failed", "err", err, "dir", destDir)
		return fmt.Errorf("golden: create dir %s: %w", destDir, err)
	}
	in, err := os.Open(source)
	if err != nil {
		slog.ErrorContext(ctx, "open source binary failed", "err", err, "source", source)
		return fmt.Errorf("golden: open source %s: %w", source, err)
	}
	defer func() { _ = in.Close() }()
	temp, err := os.CreateTemp(destDir, ".gha-golden-*")
	if err != nil {
		slog.ErrorContext(ctx, "create temp binary failed", "err", err, "dir", destDir)
		return fmt.Errorf("golden: create temp in %s: %w", destDir, err)
	}
	tempPath := temp.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := io.Copy(temp, in); err != nil {
		_ = temp.Close()
		slog.ErrorContext(ctx, "copy binary failed", "err", err, "temp", tempPath)
		return fmt.Errorf("golden: copy to %s: %w", tempPath, err)
	}
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		slog.ErrorContext(ctx, "chmod temp binary failed", "err", err, "temp", tempPath)
		return fmt.Errorf("golden: chmod %s: %w", tempPath, err)
	}
	if err := temp.Close(); err != nil {
		slog.ErrorContext(ctx, "close temp binary failed", "err", err, "temp", tempPath)
		return fmt.Errorf("golden: close %s: %w", tempPath, err)
	}
	if err := os.Rename(tempPath, dest); err != nil {
		slog.ErrorContext(ctx, "rename binary failed", "err", err, "temp", tempPath, "dest", dest)
		return fmt.Errorf("golden: rename to %s: %w", dest, err)
	}
	removeTemp = false
	return nil
}
