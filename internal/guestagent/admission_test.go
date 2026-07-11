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

// countingSpecBuilder counts Build calls and returns a launchable spec, so a test
// can assert destructive prep ran at most once across concurrent RunJobs.
type countingSpecBuilder struct {
	builds atomic.Int32
}

func (b *countingSpecBuilder) Build(_ context.Context, request JobRequest) (guestexec.ExecSpec, error) {
	b.builds.Add(1)
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

// TestRunJobSerializesConcurrentSameSlot proves the per-slot lock closes the
// concurrent-same-slot window: two RunJobs racing on one idle slot never both run
// prepareSlot; exactly one is admitted and launches, and the other is refused
// without any destructive prep.
func TestRunJobSerializesConcurrentSameSlot(t *testing.T) {
	const iterations = 50
	ids := []string{"job-a", "job-b"}
	for iteration := 0; iteration < iterations; iteration++ {
		registry := guestexec.New(guestexec.Options{Retention: time.Minute, HeartbeatInterval: time.Hour})
		builder := &countingSpecBuilder{}
		launcher := &fakeSlotLauncher{registry: registry, launches: atomic.Int32{}}
		handler := New(registry, Options{SlotCount: 1, SpecBuilder: builder, ChildLauncher: launcher})

		outcomes := make([]guestproto.RunJobResponse_Outcome, len(ids))
		var waitGroup sync.WaitGroup
		waitGroup.Add(len(ids))
		for index := range ids {
			go func(index int) {
				defer waitGroup.Done()
				response, err := handler.RunJob(context.Background(), connect.NewRequest(&guestproto.RunJobRequest{
					ExecutionId: ids[index],
					Slot:        0,
					JitConfig:   "jit",
				}))
				if err != nil {
					t.Errorf("iteration %d RunJob %s: %v", iteration, ids[index], err)
					return
				}
				outcomes[index] = response.Msg.GetOutcome()
			}(index)
		}
		waitGroup.Wait()

		accepted := 0
		refused := 0
		for _, outcome := range outcomes {
			switch outcome {
			case guestproto.RunJobResponse_ACCEPTED:
				accepted++
			case guestproto.RunJobResponse_CONFLICT, guestproto.RunJobResponse_ALREADY_RUNNING:
				refused++
			default:
				t.Fatalf("iteration %d unexpected outcome %v", iteration, outcome)
			}
		}
		if accepted != 1 || refused != 1 {
			t.Fatalf("iteration %d outcomes = %v, want exactly one accepted and one refused", iteration, outcomes)
		}
		if got := builder.builds.Load(); got != 1 {
			t.Fatalf("iteration %d prepareSlot builds = %d, want exactly 1", iteration, got)
		}
		if got := launcher.launches.Load(); got != 1 {
			t.Fatalf("iteration %d launches = %d, want exactly 1", iteration, got)
		}
		// Free the slot's fake execution so its supervisor goroutine finishes.
		registry.ReportExit(fakeSlotPID, 0, "")
	}
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
