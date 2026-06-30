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
// command-construction tests.
type stubTart struct {
	execCalls     [][]string
	cloneFrom     []string
	cloneTo       []string
	cloneInsecure []bool
	names         []string
	shutdown      map[string]bool
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

func (s *stubTart) Exec(_ context.Context, name string, argv ...string) ([]byte, error) {
	s.execCalls = append(s.execCalls, argv)
	if len(argv) >= 3 && argv[0] == "sudo" && argv[1] == "/sbin/shutdown" {
		if s.shutdown == nil {
			s.shutdown = make(map[string]bool)
		}
		s.shutdown[name] = true
		return nil, nil
	}
	if len(argv) == 1 && argv[0] == "true" && s.shutdown[name] {
		return nil, errors.New("vm is down")
	}
	return nil, nil
}

func (s *stubTart) Stop(_ context.Context, _ string) error   { return nil }
func (s *stubTart) Delete(_ context.Context, _ string) error { return nil }

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

func TestInstallRunnerURL(t *testing.T) {
	s := &stubTart{}
	b := New(s)
	if err := b.installRunner(context.Background(), "vm", "2.99.0"); err != nil {
		t.Fatalf("installRunner: %v", err)
	}
	if len(s.execCalls) != 1 {
		t.Fatalf("want 1 exec, got %d", len(s.execCalls))
	}
	joined := strings.Join(s.execCalls[0], " ")
	if !strings.Contains(joined, "actions-runner-osx-arm64-2.99.0.tar.gz") {
		t.Errorf("runner install missing versioned url: %s", joined)
	}
	if !strings.Contains(joined, "test -f run.sh") {
		t.Errorf("runner install should verify run.sh: %s", joined)
	}
}

func TestVerifyChecksRunnerWatchdogAndXcode(t *testing.T) {
	s := &stubTart{}
	b := New(s)
	if err := b.verify(context.Background(), "gha-golden", "gha-golden-verify"); err != nil {
		t.Fatalf("verify: %v", err)
	}
	all := ""
	for _, call := range s.execCalls {
		all += strings.Join(call, " ") + "\n"
	}
	for _, want := range []string{"test -f ~/actions-runner/run.sh", "io.goodkind.gha-broker-watchdog", "xcodebuild -version"} {
		if !strings.Contains(all, want) {
			t.Errorf("verify missing check %q in:\n%s", want, all)
		}
	}
}

func TestInstallWatchdogWritesBothAssets(t *testing.T) {
	s := &stubTart{}
	b := New(s)
	if err := b.installWatchdog(context.Background(), "vm"); err != nil {
		t.Fatalf("installWatchdog: %v", err)
	}
	if len(s.execCalls) != 2 {
		t.Fatalf("want 2 writes (script + plist), got %d", len(s.execCalls))
	}
	all := strings.Join(s.execCalls[0], " ") + "\n" + strings.Join(s.execCalls[1], " ")
	for _, want := range []string{watchdogScriptPath, watchdogPlistPath, "base64 -D", "sudo tee"} {
		if !strings.Contains(all, want) {
			t.Errorf("watchdog install missing %q in:\n%s", want, all)
		}
	}
}
