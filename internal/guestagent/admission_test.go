package guestagent

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"goodkind.io/gha-mac-broker/internal/guestexec"
	"goodkind.io/gha-mac-broker/internal/guestproto"
)

// blockingSpecBuilder reports each Build call on started and blocks it until
// release closes, so a test can hold one RunJob inside its critical section (and
// thus holding the slot lock) while it drives a second RunJob for the same slot.
type blockingSpecBuilder struct {
	builds  atomic.Int32
	started chan string
	release chan struct{}
}

func (b *blockingSpecBuilder) Build(_ context.Context, request JobRequest) (guestexec.ExecSpec, error) {
	b.builds.Add(1)
	b.started <- request.ExecutionID
	<-b.release
	return guestexec.ExecSpec{
		ExecutionID: request.ExecutionID,
		Slot:        request.Slot,
		Command:     "/bin/true",
	}, nil
}

// fakeSlotLauncher records an execution without forking a process, so a concurrent
// admission test occupies the slot deterministically and counts launches.
type fakeSlotLauncher struct {
	registry *guestexec.Registry
	launches atomic.Int32
}

func (l *fakeSlotLauncher) Run(spec guestexec.ExecSpec) (guestexec.Outcome, error) {
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		return guestexec.OutcomeUnspecified, err
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		return guestexec.OutcomeUnspecified, err
	}
	outcome, registerErr := l.registry.Register(spec, fakeSlotPID, fakeSlotPID, stdoutReader, stderrReader)
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	if registerErr == nil && outcome == guestexec.OutcomeAccepted {
		l.launches.Add(1)
	}
	return outcome, registerErr
}

const fakeSlotPID = 999999

// TestRunJobSerializesConcurrentSameSlot deterministically proves the per-slot
// lock: while a first RunJob is held inside Build (so it holds the slot lock), a
// second RunJob for the same slot with a different execution id cannot reach
// Build. Only after the first releases and records its execution does the second
// proceed, peek admission on the now-busy slot, and return CONFLICT without ever
// running Build. Without the slot lock the second would peek the still-idle slot
// and Build concurrently, which the started-channel assertion catches.
func TestRunJobSerializesConcurrentSameSlot(t *testing.T) {
	registry := guestexec.New(guestexec.Options{Retention: time.Minute, HeartbeatInterval: time.Hour})
	builder := &blockingSpecBuilder{
		builds:  atomic.Int32{},
		started: make(chan string, 2),
		release: make(chan struct{}),
	}
	launcher := &fakeSlotLauncher{registry: registry, launches: atomic.Int32{}}
	handler := New(registry, Options{SlotCount: 1, SpecBuilder: builder, ChildLauncher: launcher})

	runJob := func(executionID string) (guestproto.RunJobResponse_Outcome, error) {
		response, err := handler.RunJob(context.Background(), connect.NewRequest(&guestproto.RunJobRequest{
			ExecutionId: executionID,
			Slot:        0,
			JitConfig:   "jit",
		}))
		if err != nil {
			return guestproto.RunJobResponse_OUTCOME_UNSPECIFIED, err
		}
		return response.Msg.GetOutcome(), nil
	}

	firstResult := make(chan guestproto.RunJobResponse_Outcome, 1)
	firstErr := make(chan error, 1)
	go func() {
		outcome, err := runJob("job-a")
		if err != nil {
			firstErr <- err
			return
		}
		firstResult <- outcome
	}()

	// The first request is now inside Build, holding the slot lock.
	select {
	case id := <-builder.started:
		if id != "job-a" {
			t.Fatalf("first Build execution id = %q, want job-a", id)
		}
	case err := <-firstErr:
		t.Fatalf("first RunJob error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("first request did not reach Build")
	}

	secondResult := make(chan guestproto.RunJobResponse_Outcome, 1)
	secondErr := make(chan error, 1)
	go func() {
		outcome, err := runJob("job-b")
		if err != nil {
			secondErr <- err
			return
		}
		secondResult <- outcome
	}()

	// While the first holds the slot lock, the second must block on that lock and
	// never reach Build. Without the lock it would Build the same slot concurrently.
	select {
	case id := <-builder.started:
		t.Fatalf("second Build ran (id=%q) while the first held the slot lock; serialization missing", id)
	case outcome := <-secondResult:
		t.Fatalf("second returned %v before the first released; want it to block on the slot lock", outcome)
	case err := <-secondErr:
		t.Fatalf("second RunJob error before release: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	// Release the first: it records its execution and returns ACCEPTED.
	close(builder.release)
	select {
	case outcome := <-firstResult:
		if outcome != guestproto.RunJobResponse_ACCEPTED {
			t.Fatalf("first outcome = %v, want ACCEPTED", outcome)
		}
	case err := <-firstErr:
		t.Fatalf("first RunJob error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("first request did not complete after release")
	}

	// The second now proceeds, peeks the busy slot, and returns CONFLICT with no Build.
	select {
	case outcome := <-secondResult:
		if outcome != guestproto.RunJobResponse_CONFLICT {
			t.Fatalf("second outcome = %v, want CONFLICT", outcome)
		}
	case err := <-secondErr:
		t.Fatalf("second RunJob error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("second request did not complete after the first released")
	}

	if got := builder.builds.Load(); got != 1 {
		t.Fatalf("Build calls = %d, want exactly 1 (second refused at admission, no prep)", got)
	}
	if got := launcher.launches.Load(); got != 1 {
		t.Fatalf("launches = %d, want exactly 1", got)
	}
	// Free the slot's fake execution so its supervisor goroutine finishes.
	registry.ReportExit(fakeSlotPID, 0, "")
}

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
