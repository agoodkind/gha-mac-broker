package guestagent

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"goodkind.io/gha-mac-broker/internal/guestexec"
	"goodkind.io/gha-mac-broker/internal/guestproto"
)

// registerRunningSlot occupies a slot with a running execution whose process id
// is fake, so a later RunJob for the same id or slot must be refused. The pipe
// write ends close immediately, so capture reaches EOF while the execution stays
// running because no exit is ever reported.
func registerRunningSlot(t *testing.T, registry *guestexec.Registry, executionID string, slot uint32) {
	t.Helper()
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	spec := guestexec.ExecSpec{ExecutionID: executionID, Slot: slot, Command: "/bin/true"}
	outcome, err := registry.Register(spec, 999999, 999999, stdoutReader, stderrReader)
	if err != nil {
		t.Fatalf("register running slot: %v", err)
	}
	if outcome != guestexec.OutcomeAccepted {
		t.Fatalf("register outcome = %v, want accepted", outcome)
	}
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
}

func writeSentinel(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create sentinel dir: %v", err)
	}
	if err := os.WriteFile(path, []byte("sentinel\n"), 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
}

// TestRunJobSkipsDestructivePrepWhenSlotBusy proves the admission peek runs
// before any per-slot prep: a retry of a live execution and a job routed to a
// busy slot are both refused without wiping the running slot's directories.
func TestRunJobSkipsDestructivePrepWhenSlotBusy(t *testing.T) {
	ctx := context.Background()
	registry := guestexec.New(guestexec.Options{Retention: time.Minute, HeartbeatInterval: time.Hour})
	registerRunningSlot(t, registry, "job-live", 0)

	baseHome := t.TempDir()
	runnerSentinel := filepath.Join(baseHome, "actions-runner-0", "sentinel")
	homeSentinel := filepath.Join(baseHome, "slot-home-0", "sentinel")
	writeSentinel(t, runnerSentinel)
	writeSentinel(t, homeSentinel)

	stub := &commandStub{}
	executor := &runnerExecutor{
		baseHome:        baseHome,
		markerPath:      filepath.Join(t.TempDir(), "brew-marker"),
		runCommand:      stub.run,
		lookBrew:        func() bool { return false },
		sleep:           func(_ context.Context) {},
		clearMarkerOnce: sync.Once{},
	}
	handler := New(registry, Options{SlotCount: 1, SpecBuilder: executor})

	sameResponse, err := handler.RunJob(ctx, connect.NewRequest(&guestproto.RunJobRequest{
		ExecutionId: "job-live",
		Slot:        0,
		JitConfig:   "jit",
	}))
	if err != nil {
		t.Fatalf("RunJob same id: %v", err)
	}
	if got := sameResponse.Msg.GetOutcome(); got != guestproto.RunJobResponse_ALREADY_RUNNING {
		t.Fatalf("same-id outcome = %v, want ALREADY_RUNNING", got)
	}

	conflictResponse, err := handler.RunJob(ctx, connect.NewRequest(&guestproto.RunJobRequest{
		ExecutionId: "job-new",
		Slot:        0,
		JitConfig:   "jit",
	}))
	if err != nil {
		t.Fatalf("RunJob conflicting slot: %v", err)
	}
	if got := conflictResponse.Msg.GetOutcome(); got != guestproto.RunJobResponse_CONFLICT {
		t.Fatalf("conflict outcome = %v, want CONFLICT", got)
	}

	if _, err := os.Stat(runnerSentinel); err != nil {
		t.Fatalf("runner sentinel stat err = %v, want the busy slot's runner dir untouched", err)
	}
	if _, err := os.Stat(homeSentinel); err != nil {
		t.Fatalf("home sentinel stat err = %v, want the busy slot's HOME untouched", err)
	}
	if got := stub.count("cp") + stub.count("security") + stub.count("brew"); got != 0 {
		t.Fatalf("system commands run = %d, want 0 (no destructive prep on a refused admission)", got)
	}
}
