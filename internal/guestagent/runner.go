package guestagent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"goodkind.io/gha-mac-broker/internal/guestexec"
)

const (
	// brewBootRefreshMarker is the fixed VM-wide path a successful Homebrew boot
	// refresh writes. A later per-job brew step reads it from an isolated per-slot
	// HOME and skips a redundant refresh, so brew does not run on every job.
	brewBootRefreshMarker = "/tmp/swift-mk-brew-boot-refreshed"
	// runnerLaunchScript is the runner entry the JIT launch execs.
	runnerLaunchScript = "run.sh"
	// goldenRunnerDir is the golden image's runner tree that each slot clones from.
	goldenRunnerDir = "actions-runner"
	// loginKeychainRelPath is the per-slot login keychain under the slot HOME.
	loginKeychainRelPath = "Library/Keychains/login.keychain-db"
	// brewRefreshMaxAttempts bounds the Homebrew update lock-contention retry.
	brewRefreshMaxAttempts = 3
	// brewRefreshRetryDelay is the pause between lock-contention retries.
	brewRefreshRetryDelay = 5 * time.Second
	// slotDirPerm is the private mode for per-slot runner, tmp, and cache dirs.
	slotDirPerm = 0o700
	// markerFilePerm is the mode for the brew boot-refresh marker file.
	markerFilePerm = 0o644
)

// warmCachePaths are the by-presence caches seeded into each slot's isolated
// HOME via APFS clone, so co-tenant slots never share these dirs yet each starts
// warm. The swift-mk toolchain is intentionally absent: actions/cache restores it
// per-slot keyed by source hash, so seeding it risks a seed/cache merge.
var warmCachePaths = []string{
	".local",
	".swiftpm",
	".cache",
	"Library/Caches/org.swift.swiftpm",
	"Library/Caches/Homebrew",
	"Library/Developer/Xcode/DerivedData",
	".gitconfig",
	".netrc",
}

// brewLockContentionPattern matches the brew update output that signals another
// brew process holds the lock, so the refresh retries rather than gives up.
var brewLockContentionPattern = regexp.MustCompile(
	`(?i)already locked|another active homebrew|another .* process is already running`,
)

// JobRequest carries the RunJob fields the spec builder needs to assemble a
// runner ExecSpec and run its per-slot setup.
type JobRequest struct {
	ExecutionID string
	Slot        uint32
	Meta        guestexec.JobMeta
	JitConfig   string
	Env         map[string]string
}

// SpecBuilder turns a RunJob request into an ExecSpec, running any per-slot
// guest setup as a side effect before the supervisor forks the process. The
// supervisor owns the fork and wait, so a builder only assembles the spec and
// prepares the slot; it never launches a process.
type SpecBuilder interface {
	Build(ctx context.Context, request JobRequest) (guestexec.ExecSpec, error)
}

// commandFunc runs a system tool with args and env overrides layered over the
// process environment, returning its combined output. It is a seam so a host
// side test drives the port without invoking cp, security, or brew.
type commandFunc func(ctx context.Context, name string, args []string, envOverrides map[string]string) (string, error)

// runnerExecutor ports the two guest shell scripts to Go. It refreshes Homebrew
// once per boot, prepares a slot's isolated runner directory, TMPDIR, cache
// seeded HOME, and default login keychain, then builds the ExecSpec that
// launches run.sh --jitconfig for that slot with an absolute command.
type runnerExecutor struct {
	baseHome   string
	markerPath string
	runCommand commandFunc
	// lookBrew reports whether brew is on PATH.
	lookBrew func() bool
	// sleep waits before a Homebrew lock-contention retry, returning early when
	// the context is cancelled.
	sleep func(ctx context.Context)

	clearMarkerOnce sync.Once
}

var _ SpecBuilder = (*runnerExecutor)(nil)

// newRunnerExecutor returns the production spec builder. It resolves the guest
// user's real HOME as the base for per-slot directories and wires the system
// tool seam to real cp, security, and brew invocations.
func newRunnerExecutor() *runnerExecutor {
	baseHome := os.Getenv("HOME")
	if baseHome == "" {
		if resolved, err := os.UserHomeDir(); err == nil {
			baseHome = resolved
		}
	}
	return &runnerExecutor{
		baseHome:        baseHome,
		markerPath:      brewBootRefreshMarker,
		runCommand:      runSystemCommand,
		lookBrew:        brewOnPath,
		sleep:           sleepWithContext,
		clearMarkerOnce: sync.Once{},
	}
}

// Build refreshes Homebrew, prepares the slot, and returns the ExecSpec that
// launches the slot's runner. The command is the absolute run.sh path so the
// registry accepts it, and the environment carries the per-slot HOME, TMPDIR,
// and git isolation the runner needs.
func (e *runnerExecutor) Build(ctx context.Context, request JobRequest) (guestexec.ExecSpec, error) {
	if e.baseHome == "" {
		err := fmt.Errorf("guestagent: base home is empty")
		slog.ErrorContext(ctx, "guest runner base home is empty", "err", err)
		return guestexec.ExecSpec{}, err
	}
	e.ensureHomebrewRefreshed(ctx)
	if err := e.prepareSlot(ctx, request.Slot); err != nil {
		return guestexec.ExecSpec{}, err
	}
	runnerHome := e.runnerHome(request.Slot)
	spec := guestexec.ExecSpec{
		ExecutionID: request.ExecutionID,
		Slot:        request.Slot,
		Meta:        request.Meta,
		Command:     filepath.Join(runnerHome, runnerLaunchScript),
		Args:        []string{"--jitconfig", request.JitConfig},
		Env:         e.runnerEnv(request.Slot, request.Env),
		WorkingDir:  runnerHome,
	}
	return spec, nil
}

func (e *runnerExecutor) runnerHome(slot uint32) string {
	return filepath.Join(e.baseHome, fmt.Sprintf("actions-runner-%d", slot))
}

func (e *runnerExecutor) tmpDir(slot uint32) string {
	return filepath.Join(e.baseHome, fmt.Sprintf("tmp-%d", slot))
}

func (e *runnerExecutor) slotHome(slot uint32) string {
	return filepath.Join(e.baseHome, fmt.Sprintf("slot-home-%d", slot))
}

// runnerEnv builds the runner environment. It applies the caller's env first,
// then overrides the isolation keys so a caller can never redirect HOME, TMPDIR,
// or the git credential suppression that keeps co-tenant slots isolated. SwiftPM
// clone URLs already carry the token, so credential.helper is cleared to keep
// git-credential-manager (whose store path can deadlock in the headless VM) out
// of the process tree, and GIT_TERMINAL_PROMPT=0 makes a 401 fail fast.
func (e *runnerExecutor) runnerEnv(slot uint32, callerEnv map[string]string) map[string]string {
	env := make(map[string]string, len(callerEnv)+6)
	for key, value := range callerEnv {
		env[key] = value
	}
	env["GIT_CONFIG_COUNT"] = "1"
	env["GIT_CONFIG_KEY_0"] = "credential.helper"
	env["GIT_CONFIG_VALUE_0"] = ""
	env["GIT_TERMINAL_PROMPT"] = "0"
	env["TMPDIR"] = e.tmpDir(slot)
	env["HOME"] = e.slotHome(slot)
	return env
}

// prepareSlot rebuilds the slot's runner directory, tmp directory, cache seeded
// HOME, and default keychain. It removes the prior slot state first so a reused
// slot gets a pristine runner and an ephemeral JIT registration.
func (e *runnerExecutor) prepareSlot(ctx context.Context, slot uint32) error {
	runnerHome := e.runnerHome(slot)
	tmpDir := e.tmpDir(slot)
	slotHome := e.slotHome(slot)

	if err := e.removeDir(ctx, "runner home", runnerHome); err != nil {
		return err
	}
	if err := e.removeDir(ctx, "tmp dir", tmpDir); err != nil {
		return err
	}
	goldenRunner := filepath.Join(e.baseHome, goldenRunnerDir)
	if err := e.runTool(ctx, "clone runner home", "cp", []string{"-R", goldenRunner, runnerHome}, nil); err != nil {
		return err
	}
	if err := e.makeDir(ctx, "tmp dir", tmpDir); err != nil {
		return err
	}
	if err := e.seedSlotHome(ctx, slotHome); err != nil {
		return err
	}
	// Keychain setup is best effort: a failure warns and the job then fails on
	// signing as it does today, rather than aborting the whole job here.
	if err := e.setupSlotKeychain(ctx, slotHome); err != nil {
		slog.WarnContext(ctx, "guest slot keychain setup incomplete; signing steps may fail", "slot", slot, "err", err)
	}
	return nil
}

// seedSlotHome rebuilds the slot's isolated HOME and seeds each present warm
// cache via an APFS clone, falling back to a real copy when clonefile is
// unavailable. Seeding by presence keeps co-tenant slots isolated yet warm.
func (e *runnerExecutor) seedSlotHome(ctx context.Context, slotHome string) error {
	if err := e.removeDir(ctx, "slot home", slotHome); err != nil {
		return err
	}
	if err := e.makeDir(ctx, "slot home", slotHome); err != nil {
		return err
	}
	for _, cachePath := range warmCachePaths {
		source := filepath.Join(e.baseHome, cachePath)
		if _, err := os.Stat(source); err != nil {
			continue
		}
		dest := filepath.Join(slotHome, cachePath)
		if err := e.makeDir(ctx, "cache parent", filepath.Dir(dest)); err != nil {
			return err
		}
		// APFS clone (cp -cR) keeps the seed warm and cheap. A clonefile failure is
		// expected where the tool is unavailable, so fall back to a real copy and
		// only surface the fallback failure.
		if _, err := e.runCommand(ctx, "cp", []string{"-cR", source, dest}, nil); err != nil {
			slog.WarnContext(ctx, "guest slot cache clone failed; falling back to copy", "cache", cachePath, "err", err)
			_ = os.RemoveAll(dest)
			if fallbackErr := e.runTool(ctx, "seed cache "+cachePath, "cp", []string{"-R", source, dest}, nil); fallbackErr != nil {
				return fallbackErr
			}
		}
	}
	return nil
}

// setupSlotKeychain creates the slot's login keychain if absent and makes it the
// slot HOME's default, prepending it to the user search list while preserving
// the System keychain's trust anchors. Running security with HOME set to the
// slot home keeps this default per-slot, so co-tenant signing jobs stay isolated.
func (e *runnerExecutor) setupSlotKeychain(ctx context.Context, slotHome string) error {
	keychain := filepath.Join(slotHome, loginKeychainRelPath)
	if err := e.makeDir(ctx, "keychain dir", filepath.Dir(keychain)); err != nil {
		return err
	}
	slotEnv := map[string]string{"HOME": slotHome}
	if _, err := e.runCommand(ctx, "security", []string{"show-keychain-info", keychain}, slotEnv); err != nil {
		if createErr := e.runTool(ctx, "create keychain", "security", []string{"create-keychain", "-p", "", keychain}, slotEnv); createErr != nil {
			return createErr
		}
	}
	if err := e.runTool(ctx, "set default keychain", "security", []string{"default-keychain", "-s", keychain}, slotEnv); err != nil {
		return err
	}
	existing, err := e.userKeychains(ctx, keychain, slotEnv)
	if err != nil {
		return err
	}
	listArgs := append([]string{"list-keychains", "-d", "user", "-s", keychain}, existing...)
	if err := e.runTool(ctx, "set keychain search list", "security", listArgs, slotEnv); err != nil {
		return err
	}
	if err := e.runTool(ctx, "unlock keychain", "security", []string{"unlock-keychain", "-p", "", keychain}, slotEnv); err != nil {
		return err
	}
	return e.runTool(ctx, "set keychain settings", "security", []string{"set-keychain-settings", keychain}, slotEnv)
}

// userKeychains returns the current user keychain search list with the slot
// keychain removed, so re-adding the slot keychain at the front does not
// duplicate it while the System keychain's trust anchors stay in the list.
func (e *runnerExecutor) userKeychains(ctx context.Context, slotKeychain string, slotEnv map[string]string) ([]string, error) {
	output, err := e.runCommand(ctx, "security", []string{"list-keychains", "-d", "user"}, slotEnv)
	if err != nil {
		slog.WarnContext(ctx, "guest slot keychain list failed", "err", err)
		return nil, fmt.Errorf("guestagent: list keychains: %w", err)
	}
	keychains := make([]string, 0)
	for _, line := range strings.Split(output, "\n") {
		trimmed := strings.Trim(strings.TrimSpace(line), `"`)
		if trimmed == "" || trimmed == slotKeychain {
			continue
		}
		keychains = append(keychains, trimmed)
	}
	return keychains, nil
}

// runTool runs a system tool for a labeled slot-setup step, logging and wrapping
// any failure so a co-tenant setup problem surfaces loudly.
func (e *runnerExecutor) runTool(ctx context.Context, step string, name string, args []string, env map[string]string) error {
	if _, err := e.runCommand(ctx, name, args, env); err != nil {
		slog.WarnContext(ctx, "guest slot setup step failed", "step", step, "err", err)
		return fmt.Errorf("guestagent: %s: %w", step, err)
	}
	return nil
}

// removeDir removes a slot directory, logging and wrapping any failure.
func (e *runnerExecutor) removeDir(ctx context.Context, label string, path string) error {
	if err := os.RemoveAll(path); err != nil {
		slog.ErrorContext(ctx, "guest slot dir remove failed", "dir", label, "path", path, "err", err)
		return fmt.Errorf("guestagent: remove %s %q: %w", label, path, err)
	}
	return nil
}

// makeDir creates a private slot directory, logging and wrapping any failure.
func (e *runnerExecutor) makeDir(ctx context.Context, label string, path string) error {
	if err := os.MkdirAll(path, slotDirPerm); err != nil {
		slog.ErrorContext(ctx, "guest slot dir create failed", "dir", label, "path", path, "err", err)
		return fmt.Errorf("guestagent: create %s %q: %w", label, path, err)
	}
	return nil
}

// ensureHomebrewRefreshed refreshes the Homebrew index once per boot. It clears
// a stale marker once, then no-ops when a real marker is present so brew does
// not run on every job, and refreshes when the marker is absent.
func (e *runnerExecutor) ensureHomebrewRefreshed(ctx context.Context) {
	e.clearStaleMarker()
	if e.markerPresent() {
		return
	}
	if !e.lookBrew() {
		return
	}
	e.refreshHomebrewIndex(ctx)
}

// clearStaleMarker removes a stale marker or pre-planted symlink once per boot,
// mirroring the script's rm -f at VM prep, so a marker baked into the golden
// image never suppresses the first real refresh. It runs at most once because
// the executor lives for the guest agent's boot.
func (e *runnerExecutor) clearStaleMarker() {
	e.clearMarkerOnce.Do(func() {
		if err := os.Remove(e.markerPath); err != nil && !os.IsNotExist(err) {
			slog.Warn("guest brew boot refresh marker clear failed", "path", e.markerPath, "err", err)
		}
	})
}

// markerPresent reports whether a real refresh marker exists. It opens with
// O_NOFOLLOW|O_NONBLOCK so a symlink is not followed and a FIFO cannot block the
// open, then fstats the descriptor so only a regular file counts as a real
// marker. A symlink, FIFO, or any other non-regular occupant is not a valid
// marker, so this returns false and the refresh proceeds; the O_EXCL|O_NOFOLLOW
// write then refuses the occupant without truncating or following it, so an
// attacker cannot suppress the refresh by squatting the world-writable path.
func (e *runnerExecutor) markerPresent() bool {
	fd, err := unix.Open(e.markerPath, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return false
	}
	defer func() { _ = unix.Close(fd) }()
	var stat unix.Stat_t
	if fstatErr := unix.Fstat(fd, &stat); fstatErr != nil {
		return false
	}
	return stat.Mode&unix.S_IFMT == unix.S_IFREG
}

// refreshHomebrewIndex runs brew update, retrying only on lock contention and
// writing the marker only after a successful update. A non-lock failure gives up
// quietly and a persistent lock exhausts the attempts, both leaving no marker so
// a later job refreshes itself.
func (e *runnerExecutor) refreshHomebrewIndex(ctx context.Context) {
	for attempt := 1; attempt <= brewRefreshMaxAttempts; attempt++ {
		output, err := e.runCommand(ctx, "brew", []string{"update", "--quiet"}, nil)
		if err == nil {
			e.writeBootRefreshMarker(ctx)
			return
		}
		if !brewLockContentionPattern.MatchString(output) {
			return
		}
		if attempt == brewRefreshMaxAttempts {
			return
		}
		e.sleep(ctx)
	}
}

// writeBootRefreshMarker creates the marker only when the path is absent.
// O_NOFOLLOW refuses a symlink and O_EXCL refuses any pre-existing path, so a
// race that plants a symlink, FIFO, or regular file at the world-writable marker
// path between the presence check and this write cannot redirect the write or be
// truncated: the open fails and no marker is written, so a later job refreshes
// itself.
func (e *runnerExecutor) writeBootRefreshMarker(ctx context.Context) {
	fd, err := unix.Open(
		e.markerPath,
		unix.O_CREAT|unix.O_EXCL|unix.O_WRONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		markerFilePerm,
	)
	if err != nil {
		slog.WarnContext(ctx, "guest brew boot refresh marker write refused or failed", "path", e.markerPath, "err", err)
		return
	}
	_ = unix.Close(fd)
}

// brewOnPath reports whether brew is on PATH.
func brewOnPath() bool {
	_, err := exec.LookPath("brew")
	return err == nil
}

// sleepWithContext waits for the retry delay, returning early on cancellation so
// a cancelled RunJob does not block on a Homebrew lock retry.
func sleepWithContext(ctx context.Context) {
	timer := time.NewTimer(brewRefreshRetryDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

// runSystemCommand runs name with args, layering env overrides over the process
// environment, and returns the combined output so the caller can inspect it.
func runSystemCommand(ctx context.Context, name string, args []string, envOverrides map[string]string) (string, error) {
	slog.DebugContext(ctx, "guest system command built", "command", name, "args", strings.Join(args, " "))
	command := exec.CommandContext(ctx, name, args...)
	if len(envOverrides) > 0 {
		command.Env = envWithOverrides(os.Environ(), envOverrides)
	}
	output, err := command.CombinedOutput()
	return string(output), err
}

// envWithOverrides appends sorted key=value overrides to base so the later
// entries win in the child process environment.
func envWithOverrides(base []string, overrides map[string]string) []string {
	env := append([]string(nil), base...)
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		env = append(env, key+"="+overrides[key])
	}
	return env
}
