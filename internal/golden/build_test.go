package golden

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"

	"goodkind.io/gha-mac-broker/internal/tart"
)

// stubTart records Exec calls and satisfies the tarter interface for white-box
// command-construction tests. It simulates a guest that persisted the fingerprint
// the provisioner was asked to bake, so verify can read it back.
type stubTart struct {
	execCalls              [][]string
	cloneFrom              []string
	cloneTo                []string
	cloneInsecure          []bool
	deleted                []string
	names                  []string
	provisionedFingerprint string
	// bakedFingerprint is the value the stub returns for a `cat` of the baked
	// fingerprint file before any provision has run, standing in for the
	// fingerprint an existing golden already carries.
	bakedFingerprint string
	// catErr, when set, makes a pre-provision `cat` of the baked fingerprint fail,
	// standing in for an unreadable golden the caller must treat as stale.
	catErr error
	// failExecArg, when set, makes any Exec whose argv contains it return an error,
	// so a test can fail a specific build phase (e.g. verify's xcodebuild check).
	failExecArg string
}

type stubStager struct {
	ref     string
	err     error
	stopped bool
}

func (s *stubStager) Stage(_ context.Context, _ string) (string, func(), error) {
	if s.err != nil {
		return "", nil, s.err
	}
	return s.ref, func() { s.stopped = true }, nil
}

func (s *stubTart) List(_ context.Context) ([]string, error) { return s.names, nil }

func (s *stubTart) Clone(_ context.Context, source, name string, insecure bool) error {
	s.cloneFrom = append(s.cloneFrom, source)
	s.cloneTo = append(s.cloneTo, name)
	s.cloneInsecure = append(s.cloneInsecure, insecure)
	return nil
}

func (s *stubTart) BootCommand(_ context.Context, _ string, _ tart.BootOptions) *exec.Cmd {
	return exec.Command("true")
}

func (s *stubTart) IP(_ context.Context, _ string) (string, error) {
	return "192.168.64.2", nil
}

func (s *stubTart) Exec(_ context.Context, _ string, argv ...string) ([]byte, error) {
	s.execCalls = append(s.execCalls, argv)
	if s.failExecArg != "" {
		for _, arg := range argv {
			if arg == s.failExecArg {
				return nil, errors.New("stub exec failure for " + arg)
			}
		}
	}
	if len(argv) >= 2 && argv[0] == "printenv" && argv[1] == "HOME" {
		return []byte("/Users/admin\n"), nil
	}
	if fingerprint, ok := fingerprintFromProvision(argv); ok {
		s.provisionedFingerprint = fingerprint
		return nil, nil
	}
	if len(argv) == 2 && argv[0] == "cat" && argv[1] == FingerprintPath {
		// After a provision has run, echo the freshly baked fingerprint so verify
		// reads what the build just wrote. Before any provision, serve the seeded
		// baked value (or error) that stands in for an existing golden's contents.
		if s.provisionedFingerprint != "" {
			return []byte(s.provisionedFingerprint + "\n"), nil
		}
		if s.catErr != nil {
			return nil, s.catErr
		}
		return []byte(s.bakedFingerprint + "\n"), nil
	}
	return nil, nil
}

// fingerprintFromProvision extracts the -fingerprint value from a golden-provision
// exec, so the stub can echo it back through the baked fingerprint file.
func fingerprintFromProvision(argv []string) (string, bool) {
	provision := false
	for _, arg := range argv {
		if arg == "golden-provision" {
			provision = true
			break
		}
	}
	if !provision {
		return "", false
	}
	for index := 0; index+1 < len(argv); index++ {
		if argv[index] == "-fingerprint" {
			return argv[index+1], true
		}
	}
	return "", true
}

func (s *stubTart) Stop(_ context.Context, _ string) error { return nil }

func (s *stubTart) Delete(_ context.Context, name string) error {
	s.deleted = append(s.deleted, name)
	return nil
}

// provisionInvoked reports whether the stub saw a golden-provision exec, which is
// the signal that EnsureGolden fell through to a real Build.
func provisionInvoked(s *stubTart) bool {
	for _, call := range s.execCalls {
		for _, arg := range call {
			if arg == "golden-provision" {
				return true
			}
		}
	}
	return false
}

// execInvoked reports whether the stub saw an exec whose argv equals want.
func execInvoked(s *stubTart, want ...string) bool {
	for _, call := range s.execCalls {
		if len(call) != len(want) {
			continue
		}
		match := true
		for index := range call {
			if call[index] != want[index] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// runnerVersionFromProvision returns the -runner-version value from the recorded
// golden-provision exec, so a test can assert the version reaching the guest.
func runnerVersionFromProvision(s *stubTart) (string, bool) {
	for _, call := range s.execCalls {
		if _, ok := fingerprintFromProvision(call); !ok {
			continue
		}
		for index := 0; index+1 < len(call); index++ {
			if call[index] == "-runner-version" {
				return call[index+1], true
			}
		}
	}
	return "", false
}

// fakeDigester injects a fixed runner-tarball digest so Build needs no network.
func fakeDigester(_ context.Context, _ string) (string, error) {
	return "fakerunnerdigest", nil
}

func TestNameForImageSanitizesCirrusTag(t *testing.T) {
	got := NameForImage("ghcr.io/cirruslabs/macos-tahoe-xcode:26.5")
	want := "gha-golden-macos-tahoe-xcode-26.5"
	if got != want {
		t.Fatalf("golden name = %q, want %q", got, want)
	}
}

// ensureFingerprint computes the fingerprint EnsureGolden expects for image, so a
// test can seed the stub's baked fingerprint to match (skip) or differ (rebuild).
func ensureFingerprint(t *testing.T, b *Builder, image string) string {
	t.Helper()
	goldenName := NameForImage(image)
	fingerprint, _, err := b.expectedFingerprint(context.Background(), Options{
		BaseImage:     image,
		GoldenName:    goldenName,
		BuildVM:       goldenName + "-build",
		RunnerVersion: "2.99.0",
		BinaryPath:    "",
	})
	if err != nil {
		t.Fatalf("expectedFingerprint: %v", err)
	}
	return fingerprint
}

func containsString(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

func TestEnsureGoldenSkipsWhenFingerprintCurrent(t *testing.T) {
	image := "ghcr.io/cirruslabs/macos-tahoe-xcode:26.5"
	goldenName := NameForImage(image)
	s := &stubTart{names: []string{goldenName}}
	b := New(s)
	b.runnerDigest = fakeDigester
	s.bakedFingerprint = ensureFingerprint(t, b, image)

	got, err := b.EnsureGolden(context.Background(), EnsureOptions{
		Image:         image,
		BuildVM:       goldenName + "-build",
		RunnerVersion: "2.99.0",
	})
	if err != nil {
		t.Fatalf("EnsureGolden: %v", err)
	}
	if got != goldenName {
		t.Fatalf("golden name = %q, want %q", got, goldenName)
	}
	if provisionInvoked(s) {
		t.Fatalf("current golden should not be rebuilt, exec calls = %v", s.execCalls)
	}
	if containsString(s.deleted, goldenName) {
		t.Fatalf("current golden should not be deleted, deletes = %v", s.deleted)
	}
}

func TestEnsureGoldenRebuildsWhenFingerprintDiffers(t *testing.T) {
	image := "ghcr.io/cirruslabs/macos-tahoe-xcode:26.5"
	goldenName := NameForImage(image)
	s := &stubTart{names: []string{goldenName}, bakedFingerprint: "stale-fingerprint-from-old-binary"}
	b := New(s)
	b.runnerDigest = fakeDigester

	got, err := b.EnsureGolden(context.Background(), EnsureOptions{
		Image:         image,
		BuildVM:       goldenName + "-build",
		RunnerVersion: "2.99.0",
	})
	if err != nil {
		t.Fatalf("EnsureGolden: %v", err)
	}
	if got != goldenName {
		t.Fatalf("golden name = %q, want %q", got, goldenName)
	}
	if !provisionInvoked(s) {
		t.Fatalf("stale golden should be rebuilt, exec calls = %v", s.execCalls)
	}
	if !containsString(s.deleted, goldenName) {
		t.Fatalf("stale golden should be deleted before rebuild, deletes = %v", s.deleted)
	}
}

func TestEnsureGoldenRebuildsWhenBakedFingerprintUnreadable(t *testing.T) {
	image := "ghcr.io/cirruslabs/macos-tahoe-xcode:26.5"
	goldenName := NameForImage(image)
	s := &stubTart{names: []string{goldenName}, catErr: errors.New("cat: no such file")}
	b := New(s)
	b.runnerDigest = fakeDigester

	got, err := b.EnsureGolden(context.Background(), EnsureOptions{
		Image:         image,
		BuildVM:       goldenName + "-build",
		RunnerVersion: "2.99.0",
	})
	if err != nil {
		t.Fatalf("EnsureGolden: %v", err)
	}
	if got != goldenName {
		t.Fatalf("golden name = %q, want %q", got, goldenName)
	}
	if !provisionInvoked(s) {
		t.Fatalf("unreadable golden should be rebuilt, exec calls = %v", s.execCalls)
	}
	if !containsString(s.deleted, goldenName) {
		t.Fatalf("unreadable golden should be deleted before rebuild, deletes = %v", s.deleted)
	}
}

func TestEnsureGoldenBuildsMissingGoldenFromImage(t *testing.T) {
	image := "ghcr.io/cirruslabs/macos-sonoma-xcode:15.4"
	goldenName := NameForImage(image)
	s := &stubTart{names: []string{}}
	b := New(s)
	b.runnerDigest = fakeDigester

	got, err := b.EnsureGolden(context.Background(), EnsureOptions{
		Image:         image,
		BuildVM:       goldenName + "-build",
		RunnerVersion: "2.99.0",
	})
	if err != nil {
		t.Fatalf("EnsureGolden: %v", err)
	}
	if got != goldenName {
		t.Fatalf("golden name = %q, want %q", got, goldenName)
	}
	if len(s.cloneFrom) == 0 || s.cloneFrom[0] != image {
		t.Fatalf("first clone source = %v, want %q", s.cloneFrom, image)
	}
	if !strings.Contains(strings.Join(s.cloneTo, "\n"), goldenName) {
		t.Fatalf("clone targets should include derived golden %q, got %v", goldenName, s.cloneTo)
	}
	if !provisionInvoked(s) {
		t.Fatalf("absent golden should be built, exec calls = %v", s.execCalls)
	}
	// The absent path must not read a baked fingerprint, so no -fpcheck clone
	// runs and the runner-tarball fetch is not paid before Build.
	if containsString(s.cloneTo, goldenName+"-fpcheck") {
		t.Fatalf("absent golden should not run a fingerprint-check clone, cloneTo = %v", s.cloneTo)
	}
}

func TestProvisionSyncsGuestBeforeSnapshot(t *testing.T) {
	image := "ghcr.io/cirruslabs/macos-tahoe-xcode:26.5"
	goldenName := NameForImage(image)
	s := &stubTart{names: []string{}}
	b := New(s)
	b.runnerDigest = fakeDigester

	if _, err := b.EnsureGolden(context.Background(), EnsureOptions{
		Image:         image,
		BuildVM:       goldenName + "-build",
		RunnerVersion: "2.99.0",
	}); err != nil {
		t.Fatalf("EnsureGolden: %v", err)
	}
	// The provisioner's writes must be flushed to the guest disk before the build
	// snapshots the VM, or the golden can miss the fingerprint and fail verify.
	if !execInvoked(s, "sync") {
		t.Fatalf("provision must sync the guest before snapshot, exec calls = %v", s.execCalls)
	}
}

func TestEnsureGoldenKeepsLiveGoldenWhenRebuildVerifyFails(t *testing.T) {
	image := "ghcr.io/cirruslabs/macos-tahoe-xcode:26.5"
	goldenName := NameForImage(image)
	// A stale golden is present and the rebuild's verify fails (xcodebuild check
	// errors), so the existing golden must survive and only the staging image is
	// cleaned up.
	s := &stubTart{names: []string{goldenName}, bakedFingerprint: "stale-fingerprint", failExecArg: "xcodebuild"}
	b := New(s)
	b.runnerDigest = fakeDigester

	if _, err := b.EnsureGolden(context.Background(), EnsureOptions{
		Image:         image,
		BuildVM:       goldenName + "-build",
		RunnerVersion: "2.99.0",
	}); err == nil {
		t.Fatal("expected the failed verify to surface as an error")
	}
	if containsString(s.deleted, goldenName) {
		t.Fatalf("live golden must survive a failed rebuild, deletes = %v", s.deleted)
	}
	if !containsString(s.deleted, goldenName+goldenStagingSuffix) {
		t.Fatalf("failed staging image must be cleaned up, deletes = %v", s.deleted)
	}
}

func TestEnsureGoldenThreadsResolvedRunnerVersionToProvision(t *testing.T) {
	image := "ghcr.io/cirruslabs/macos-tahoe-xcode:26.5"
	goldenName := NameForImage(image)
	s := &stubTart{names: []string{}}
	b := New(s)
	b.runnerDigest = fakeDigester
	b.resolveRunner = func(context.Context) (string, error) { return "9.9.9-resolved", nil }

	// With an empty RunnerVersion, fingerprintFor resolves the latest version, and
	// that resolved value (not the empty input) must reach golden-provision.
	if _, err := b.EnsureGolden(context.Background(), EnsureOptions{
		Image:   image,
		BuildVM: goldenName + "-build",
	}); err != nil {
		t.Fatalf("EnsureGolden: %v", err)
	}
	got, ok := runnerVersionFromProvision(s)
	if !ok {
		t.Fatalf("golden-provision was not invoked, exec calls = %v", s.execCalls)
	}
	if got != "9.9.9-resolved" {
		t.Fatalf("provision -runner-version = %q, want resolved %q", got, "9.9.9-resolved")
	}
}

func TestBuildStagesBaseBeforeFirstClone(t *testing.T) {
	image := "ghcr.io/cirruslabs/macos-sonoma-xcode:15.4"
	goldenName := NameForImage(image)
	stagedRef := "[::1]:51000/cirruslabs/macos-tahoe-xcode:26.5"
	tartStub := &stubTart{}
	stager := &stubStager{
		ref:     stagedRef,
		err:     nil,
		stopped: false,
	}
	builder := New(tartStub, WithBaseStager(stager))
	builder.runnerDigest = fakeDigester

	err := builder.Build(context.Background(), Options{
		BaseImage:     image,
		GoldenName:    goldenName,
		BuildVM:       goldenName + "-build",
		RunnerVersion: "2.99.0",
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(tartStub.cloneFrom) == 0 {
		t.Fatalf("Build made no clone calls")
	}
	if got := tartStub.cloneFrom[0]; got != stagedRef {
		t.Fatalf("first clone source = %q, want %q", got, stagedRef)
	}
	if got := tartStub.cloneInsecure[0]; !got {
		t.Fatalf("first clone insecure = %t, want true", got)
	}
	if !stager.stopped {
		t.Fatalf("stager stop was not called")
	}
	if tartStub.provisionedFingerprint == "" {
		t.Fatalf("provisioner was not asked to bake a fingerprint")
	}
}

func TestBuildFallsBackToBaseImageWhenStageFails(t *testing.T) {
	image := "ghcr.io/cirruslabs/macos-sonoma-xcode:15.4"
	goldenName := NameForImage(image)
	tartStub := &stubTart{}
	stager := &stubStager{
		ref:     "",
		err:     errors.New("stage failed"),
		stopped: false,
	}
	builder := New(tartStub, WithBaseStager(stager))
	builder.runnerDigest = fakeDigester

	err := builder.Build(context.Background(), Options{
		BaseImage:     image,
		GoldenName:    goldenName,
		BuildVM:       goldenName + "-build",
		RunnerVersion: "2.99.0",
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(tartStub.cloneFrom) == 0 {
		t.Fatalf("Build made no clone calls")
	}
	if got := tartStub.cloneFrom[0]; got != image {
		t.Fatalf("first clone source = %q, want %q", got, image)
	}
	if got := tartStub.cloneInsecure[0]; got {
		t.Fatalf("first clone insecure = %t, want false", got)
	}
}

func TestProvisionLandsProvisionerViaDiscreteArgv(t *testing.T) {
	s := &stubTart{}
	b := New(s)
	mountBinary := tartSharedMountRoot + "/" + provisionMountName + "/" + provisionBinaryName

	if err := b.provision(context.Background(), "vm", "2.99.0", "runnerdigest99", mountBinary, "fp123"); err != nil {
		t.Fatalf("provision: %v", err)
	}

	var provisionCall []string
	for _, call := range s.execCalls {
		for _, arg := range call {
			if arg == "bash" || arg == "-lc" {
				t.Fatalf("provision used a shell: %v", call)
			}
		}
		if len(call) >= 3 && call[0] == "sudo" && call[1] == localProvisionerPath && call[2] == "golden-provision" {
			provisionCall = call
		}
	}
	if provisionCall == nil {
		t.Fatalf("provision did not run the landed provisioner, calls = %v", s.execCalls)
	}
	joined := strings.Join(provisionCall, " ")
	for _, want := range []string{
		"-runner-version 2.99.0",
		"-runner-digest runnerdigest99",
		"-binary " + mountBinary,
		"-runner-dir /Users/admin/actions-runner",
		"-fingerprint fp123",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("provisioner argv %q missing %q", joined, want)
		}
	}

	all := ""
	for _, call := range s.execCalls {
		all += strings.Join(call, " ") + "\n"
	}
	for _, want := range []string{
		"cp " + mountBinary + " " + localProvisionerPath,
		"xattr -c " + localProvisionerPath,
		"codesign -s - -f " + localProvisionerPath,
	} {
		if !strings.Contains(all, want) {
			t.Errorf("provision missing fixup step %q in:\n%s", want, all)
		}
	}
}

func TestVerifyChecksFingerprintRunnerAndGuestAgent(t *testing.T) {
	s := &stubTart{provisionedFingerprint: "fpABC"}
	b := New(s)
	if err := b.verify(context.Background(), "gha-golden", "gha-golden-verify", "fpABC"); err != nil {
		t.Fatalf("verify: %v", err)
	}
	all := ""
	for _, call := range s.execCalls {
		for _, arg := range call {
			if arg == "bash" || arg == "-lc" {
				t.Fatalf("verify used a shell: %v", call)
			}
		}
		all += strings.Join(call, " ") + "\n"
	}
	for _, want := range []string{
		"test -f /Users/admin/actions-runner/run.sh",
		"cat " + FingerprintPath,
		"test -f " + GuestAgentPlistPath,
		"launchctl print system/" + GuestAgentPlistLabel,
		"test ! -e " + LegacyWatchdogScriptPath,
		"xcodebuild -version",
	} {
		if !strings.Contains(all, want) {
			t.Errorf("verify missing check %q in:\n%s", want, all)
		}
	}
}

func TestVerifyRejectsFingerprintMismatch(t *testing.T) {
	s := &stubTart{provisionedFingerprint: "different"}
	b := New(s)
	err := b.verify(context.Background(), "gha-golden", "gha-golden-verify", "expected")
	if err == nil {
		t.Fatal("verify accepted a fingerprint mismatch, want failure")
	}
	if !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("verify error = %q, want a fingerprint mismatch message", err.Error())
	}
}
