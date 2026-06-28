package golden

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"goodkind.io/gha-mac-broker/internal/tart"
)

// stubTart records Exec calls and satisfies the tarter interface for white-box
// command-construction tests.
type stubTart struct {
	execCalls [][]string
}

func (s *stubTart) Clone(_ context.Context, _, _ string) error { return nil }

func (s *stubTart) BootCommand(_ context.Context, _ string, _ tart.BootOptions) *exec.Cmd {
	return exec.Command("true")
}

func (s *stubTart) Exec(_ context.Context, _ string, argv ...string) ([]byte, error) {
	s.execCalls = append(s.execCalls, argv)
	return nil, nil
}

func (s *stubTart) Stop(_ context.Context, _ string) error   { return nil }
func (s *stubTart) Delete(_ context.Context, _ string) error { return nil }

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
