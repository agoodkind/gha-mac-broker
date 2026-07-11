package guestagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"goodkind.io/gha-mac-broker/internal/guestexec"
)

type recordedCommand struct {
	name string
	args []string
	env  map[string]string
}

// commandStub records every runCommand call and answers with a configurable
// responder, so a test drives the port without invoking cp, security, or brew.
type commandStub struct {
	mu        sync.Mutex
	commands  []recordedCommand
	responder func(name string, args []string) (string, error)
}

func (s *commandStub) run(_ context.Context, name string, args []string, env map[string]string) (string, error) {
	s.mu.Lock()
	s.commands = append(s.commands, recordedCommand{
		name: name,
		args: append([]string(nil), args...),
		env:  env,
	})
	s.mu.Unlock()
	if s.responder != nil {
		return s.responder(name, args)
	}
	return "", nil
}

func (s *commandStub) named(name string) []recordedCommand {
	s.mu.Lock()
	defer s.mu.Unlock()
	matches := make([]recordedCommand, 0)
	for _, command := range s.commands {
		if command.name == name {
			matches = append(matches, command)
		}
	}
	return matches
}

func (s *commandStub) count(name string) int {
	return len(s.named(name))
}

func newTestExecutor(t *testing.T, baseHome string) (*runnerExecutor, *commandStub) {
	t.Helper()
	stub := &commandStub{}
	executor := &runnerExecutor{
		baseHome:        baseHome,
		markerPath:      filepath.Join(t.TempDir(), "brew-marker"),
		runCommand:      stub.run,
		lookBrew:        func() bool { return false },
		sleep:           func(_ context.Context) {},
		clearMarkerOnce: sync.Once{},
	}
	return executor, stub
}

func writeGoldenRunner(t *testing.T, baseHome string) {
	t.Helper()
	runnerDir := filepath.Join(baseHome, goldenRunnerDir)
	if err := os.MkdirAll(runnerDir, 0o700); err != nil {
		t.Fatalf("create golden runner dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runnerDir, runnerLaunchScript), []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatalf("write stub run.sh: %v", err)
	}
}

func TestBuildProducesAbsoluteRunnerSpecForSlot(t *testing.T) {
	baseHome := t.TempDir()
	writeGoldenRunner(t, baseHome)
	executor, _ := newTestExecutor(t, baseHome)

	spec, err := executor.Build(context.Background(), JobRequest{
		ExecutionID: "exec-7",
		Slot:        1,
		Meta:        guestexec.JobMeta{Repo: "owner/repo", JobID: 42, RunID: 99, RunnerName: "runner-1"},
		JitConfig:   "encoded-jit",
		Env:         map[string]string{"FOO": "bar"},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	wantCommand := filepath.Join(baseHome, "actions-runner-1", "run.sh")
	if spec.Command != wantCommand {
		t.Fatalf("spec.Command = %q, want %q", spec.Command, wantCommand)
	}
	if !filepath.IsAbs(spec.Command) {
		t.Fatalf("spec.Command = %q, want absolute path", spec.Command)
	}
	wantArgs := []string{"--jitconfig", "encoded-jit"}
	if len(spec.Args) != len(wantArgs) || spec.Args[0] != wantArgs[0] || spec.Args[1] != wantArgs[1] {
		t.Fatalf("spec.Args = %v, want %v", spec.Args, wantArgs)
	}
	wantWorkingDir := filepath.Join(baseHome, "actions-runner-1")
	if spec.WorkingDir != wantWorkingDir {
		t.Fatalf("spec.WorkingDir = %q, want %q", spec.WorkingDir, wantWorkingDir)
	}
	if spec.ExecutionID != "exec-7" || spec.Slot != 1 || spec.Meta.RunID != 99 {
		t.Fatalf("spec identity = %+v, want exec-7/slot 1/run 99", spec)
	}
}

func TestBuildEnvIsolatesSlotHomeTmpAndGit(t *testing.T) {
	baseHome := t.TempDir()
	writeGoldenRunner(t, baseHome)
	executor, _ := newTestExecutor(t, baseHome)

	spec, err := executor.Build(context.Background(), JobRequest{
		ExecutionID: "exec-8",
		Slot:        2,
		JitConfig:   "encoded-jit",
		// A caller-supplied HOME, TMPDIR, and GIT_TERMINAL_PROMPT must not win over
		// the per-slot isolation values.
		Env: map[string]string{"HOME": "/attacker", "TMPDIR": "/attacker-tmp", "GIT_TERMINAL_PROMPT": "1", "FOO": "bar"},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	wantEnv := map[string]string{
		"HOME":                filepath.Join(baseHome, "slot-home-2"),
		"TMPDIR":              filepath.Join(baseHome, "tmp-2"),
		"GIT_CONFIG_COUNT":    "1",
		"GIT_CONFIG_KEY_0":    "credential.helper",
		"GIT_CONFIG_VALUE_0":  "",
		"GIT_TERMINAL_PROMPT": "0",
		"FOO":                 "bar",
	}
	for key, want := range wantEnv {
		got, ok := spec.Env[key]
		if !ok {
			t.Fatalf("spec.Env missing %q", key)
		}
		if got != want {
			t.Fatalf("spec.Env[%q] = %q, want %q", key, got, want)
		}
	}
}

func TestBuildPreparesSlotDirectoriesAndCloneAndKeychain(t *testing.T) {
	baseHome := t.TempDir()
	writeGoldenRunner(t, baseHome)
	// Two present warm caches and one absent, so seeding is by presence.
	if err := os.WriteFile(filepath.Join(baseHome, ".gitconfig"), []byte("[user]\n"), 0o600); err != nil {
		t.Fatalf("write .gitconfig: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(baseHome, ".swiftpm"), 0o700); err != nil {
		t.Fatalf("create .swiftpm: %v", err)
	}
	executor, stub := newTestExecutor(t, baseHome)

	if _, err := executor.Build(context.Background(), JobRequest{ExecutionID: "exec-9", Slot: 3, JitConfig: "jit"}); err != nil {
		t.Fatalf("Build: %v", err)
	}

	if info, err := os.Stat(filepath.Join(baseHome, "tmp-3")); err != nil || !info.IsDir() {
		t.Fatalf("tmp-3 stat err = %v, isDir = %v, want created dir", err, info != nil && info.IsDir())
	}
	if info, err := os.Stat(filepath.Join(baseHome, "slot-home-3")); err != nil || !info.IsDir() {
		t.Fatalf("slot-home-3 stat err = %v, want created dir", err)
	}

	cpCommands := stub.named("cp")
	wantRunnerClone := false
	seededCaches := map[string]bool{}
	for _, command := range cpCommands {
		if len(command.args) == 3 && command.args[0] == "-R" &&
			command.args[1] == filepath.Join(baseHome, goldenRunnerDir) &&
			command.args[2] == filepath.Join(baseHome, "actions-runner-3") {
			wantRunnerClone = true
		}
		if len(command.args) == 3 && command.args[0] == "-cR" {
			seededCaches[command.args[1]] = true
		}
	}
	if !wantRunnerClone {
		t.Fatalf("cp commands = %+v, want a -R clone of the golden runner into actions-runner-3", cpCommands)
	}
	if !seededCaches[filepath.Join(baseHome, ".gitconfig")] || !seededCaches[filepath.Join(baseHome, ".swiftpm")] {
		t.Fatalf("seeded caches = %v, want .gitconfig and .swiftpm cloned", seededCaches)
	}
	if seededCaches[filepath.Join(baseHome, ".netrc")] {
		t.Fatalf("seeded caches = %v, want absent .netrc skipped", seededCaches)
	}

	keychain := filepath.Join(baseHome, "slot-home-3", loginKeychainRelPath)
	securityCommands := stub.named("security")
	sawDefaultKeychain := false
	for _, command := range securityCommands {
		if len(command.args) >= 3 && command.args[0] == "default-keychain" && command.args[2] == keychain {
			sawDefaultKeychain = true
			if command.env["HOME"] != filepath.Join(baseHome, "slot-home-3") {
				t.Fatalf("default-keychain HOME env = %q, want slot home", command.env["HOME"])
			}
		}
	}
	if !sawDefaultKeychain {
		t.Fatalf("security commands = %+v, want default-keychain set to the slot keychain", securityCommands)
	}
}

func TestKeychainSearchListDropsPriorSlotKeychainCopy(t *testing.T) {
	baseHome := t.TempDir()
	slotHome := filepath.Join(baseHome, "slot-home-1")
	keychain := filepath.Join(slotHome, loginKeychainRelPath)
	executor, stub := newTestExecutor(t, baseHome)
	stub.responder = func(name string, args []string) (string, error) {
		if name == "security" && len(args) >= 3 && args[0] == "list-keychains" && args[1] == "-d" && args[2] == "user" && len(args) == 3 {
			// The existing user list already carries the slot keychain plus System.
			return fmt.Sprintf("    %q\n    \"/Library/Keychains/System.keychain\"\n", keychain), nil
		}
		return "", nil
	}

	if err := executor.setupSlotKeychain(context.Background(), slotHome); err != nil {
		t.Fatalf("setupSlotKeychain: %v", err)
	}

	var setListArgs []string
	for _, command := range stub.named("security") {
		if len(command.args) >= 4 && command.args[0] == "list-keychains" && command.args[3] == "-s" {
			setListArgs = command.args
		}
	}
	if setListArgs == nil {
		t.Fatalf("security commands = %+v, want a list-keychains -s call", stub.named("security"))
	}
	joined := strings.Join(setListArgs, " ")
	if !strings.Contains(joined, "/Library/Keychains/System.keychain") {
		t.Fatalf("set list args = %v, want System keychain preserved", setListArgs)
	}
	slotCount := 0
	for _, arg := range setListArgs {
		if arg == keychain {
			slotCount++
		}
	}
	if slotCount != 1 {
		t.Fatalf("set list args = %v, want the slot keychain exactly once", setListArgs)
	}
}

func TestBrewRefreshRunsOnceThenNoOpsWhenMarkerPresent(t *testing.T) {
	baseHome := t.TempDir()
	executor, stub := newTestExecutor(t, baseHome)
	executor.lookBrew = func() bool { return true }

	executor.ensureHomebrewRefreshed(context.Background())
	executor.ensureHomebrewRefreshed(context.Background())

	if got := stub.count("brew"); got != 1 {
		t.Fatalf("brew invocations = %d, want 1 (marker guards the second call)", got)
	}
	if _, err := os.Lstat(executor.markerPath); err != nil {
		t.Fatalf("marker stat err = %v, want marker written after a successful refresh", err)
	}
}

func TestBrewRefreshRetriesOnLockContention(t *testing.T) {
	baseHome := t.TempDir()
	executor, stub := newTestExecutor(t, baseHome)
	executor.lookBrew = func() bool { return true }
	sleeps := 0
	executor.sleep = func(_ context.Context) { sleeps++ }
	attempt := 0
	stub.responder = func(name string, args []string) (string, error) {
		if name == "brew" {
			attempt++
			if attempt == 1 {
				return "Another active Homebrew update process is already running.", fmt.Errorf("exit status 1")
			}
			return "", nil
		}
		return "", nil
	}

	executor.ensureHomebrewRefreshed(context.Background())

	if got := stub.count("brew"); got != 2 {
		t.Fatalf("brew invocations = %d, want 2 (one retry after lock contention)", got)
	}
	if sleeps != 1 {
		t.Fatalf("sleeps = %d, want 1 between the two attempts", sleeps)
	}
	if _, err := os.Lstat(executor.markerPath); err != nil {
		t.Fatalf("marker stat err = %v, want marker after the retry succeeded", err)
	}
}

func TestBrewRefreshGivesUpAndWritesNoMarkerOnNonLockFailure(t *testing.T) {
	baseHome := t.TempDir()
	executor, stub := newTestExecutor(t, baseHome)
	executor.lookBrew = func() bool { return true }
	stub.responder = func(name string, args []string) (string, error) {
		if name == "brew" {
			return "fatal: some other failure", fmt.Errorf("exit status 1")
		}
		return "", nil
	}

	executor.ensureHomebrewRefreshed(context.Background())

	if got := stub.count("brew"); got != 1 {
		t.Fatalf("brew invocations = %d, want 1 (non-lock failure gives up)", got)
	}
	if _, err := os.Lstat(executor.markerPath); !os.IsNotExist(err) {
		t.Fatalf("marker stat err = %v, want no marker so a later job retries", err)
	}
}

func TestBrewRefreshWriteRefusesSymlinkMarker(t *testing.T) {
	baseHome := t.TempDir()
	executor, stub := newTestExecutor(t, baseHome)
	executor.lookBrew = func() bool { return true }
	// A pre-planted symlink at the marker path must not receive the write.
	target := filepath.Join(t.TempDir(), "victim")
	if err := os.Symlink(target, executor.markerPath); err != nil {
		t.Fatalf("plant symlink: %v", err)
	}
	stub.responder = func(name string, args []string) (string, error) { return "", nil }

	// clearStaleMarker removes the planted symlink first, so the refresh proceeds
	// and writes a real file rather than following the link.
	executor.ensureHomebrewRefreshed(context.Background())

	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("victim stat err = %v, want the write never redirected through the symlink", err)
	}
	info, err := os.Lstat(executor.markerPath)
	if err != nil {
		t.Fatalf("marker stat err = %v, want a real marker file", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("marker mode = %v, want a regular file", info.Mode())
	}
}

func TestBrewMarkerRefusesSymlinkViaONoFollow(t *testing.T) {
	dir := t.TempDir()
	markerPath := filepath.Join(dir, "marker")
	victim := filepath.Join(dir, "victim")
	// Plant a symlink at the marker path pointing at a not-yet-existing victim, so
	// a followed open would create the victim through the link.
	if err := os.Symlink(victim, markerPath); err != nil {
		t.Fatalf("plant symlink: %v", err)
	}
	executor := &runnerExecutor{markerPath: markerPath}

	// The presence check must refuse the symlink (not report absent) and must not
	// follow it.
	if !executor.markerPresent() {
		t.Fatalf("markerPresent = false on a symlink, want refuse")
	}

	// The write must be refused atomically at open time: no victim is created
	// through the link, and the symlink is left in place rather than replaced.
	executor.writeBootRefreshMarker(context.Background())
	if _, err := os.Stat(victim); !os.IsNotExist(err) {
		t.Fatalf("victim stat err = %v, want the write refused (no create through the symlink)", err)
	}
	info, err := os.Lstat(markerPath)
	if err != nil {
		t.Fatalf("lstat marker: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("marker mode = %v, want the symlink refused and left in place", info.Mode())
	}
}

func TestBrewRefreshSkippedWhenBrewMissing(t *testing.T) {
	baseHome := t.TempDir()
	executor, stub := newTestExecutor(t, baseHome)
	executor.lookBrew = func() bool { return false }

	executor.ensureHomebrewRefreshed(context.Background())

	if got := stub.count("brew"); got != 0 {
		t.Fatalf("brew invocations = %d, want 0 when brew is absent", got)
	}
}
