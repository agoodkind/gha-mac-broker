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
	names                  []string
	provisionedFingerprint string
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
	if len(argv) >= 2 && argv[0] == "printenv" && argv[1] == "HOME" {
		return []byte("/Users/admin\n"), nil
	}
	if fingerprint, ok := fingerprintFromProvision(argv); ok {
		s.provisionedFingerprint = fingerprint
		return nil, nil
	}
	if len(argv) == 2 && argv[0] == "cat" && argv[1] == FingerprintPath {
		return []byte(s.provisionedFingerprint + "\n"), nil
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

func (s *stubTart) Stop(_ context.Context, _ string) error   { return nil }
func (s *stubTart) Delete(_ context.Context, _ string) error { return nil }

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

func TestEnsureGoldenSkipsExistingGolden(t *testing.T) {
	image := "ghcr.io/cirruslabs/macos-tahoe-xcode:26.5"
	goldenName := NameForImage(image)
	s := &stubTart{names: []string{goldenName}}
	b := New(s)

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
	if len(s.cloneFrom) != 0 {
		t.Fatalf("existing golden should not be rebuilt, clone calls = %v", s.cloneFrom)
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

func TestVerifyChecksFingerprintRunnerAndSupervisor(t *testing.T) {
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
		"test -f " + GuestSupervisorPlistPath,
		"launchctl print system/" + GuestSupervisorPlistLabel,
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
