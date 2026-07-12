//go:build unix

package guestworker

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"goodkind.io/gha-mac-broker/internal/guestexec"
)

// goneTestPID is a process id high enough to be almost certainly absent, so
// processAlive reports it gone and degradeUnreachable treats it as unobservable.
const goneTestPID = 2147483646

const degradeTestTimeout = 5 * time.Second

// registerGoneRunner registers a running execution whose process id does not
// exist and closes the pipe write ends so a later reap drains the pipes to EOF.
func registerGoneRunner(t *testing.T, registry *guestexec.Registry, executionID string, pid int) {
	t.Helper()
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	spec := guestexec.ExecSpec{ExecutionID: executionID, Slot: 0, Command: "/bin/true"}
	outcome, err := registry.Register(spec, pid, pid, stdoutReader, stderrReader)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if outcome != guestexec.OutcomeAccepted {
		t.Fatalf("register outcome = %v, want accepted", outcome)
	}
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
}

func newDegradeTestWorker(registry *guestexec.Registry, tracker *pidTracker, pollFn func() ([]guestexec.ExitReport, error)) *worker {
	return &worker{
		registry:         registry,
		controlSocket:    "",
		generation:       1,
		log:              slog.Default(),
		tracker:          tracker,
		cancelRun:        nil,
		pollCancel:       nil,
		pollDone:         nil,
		pollFn:           pollFn,
		backoff:          time.Millisecond,
		degradeThreshold: degradeFailureThreshold,
		reloadMu:         sync.Mutex{},
		replaced:         false,
	}
}

// TestPollLoopDegradesAfterSustainedFailures proves that degradeFailureThreshold
// consecutive poll failures degrade a runner whose process is gone to exit -1.
func TestPollLoopDegradesAfterSustainedFailures(t *testing.T) {
	registry := guestexec.New(guestexec.Options{Retention: time.Minute, HeartbeatInterval: time.Hour})
	registerGoneRunner(t, registry, "job", goneTestPID)
	tracker := newPIDTracker()
	tracker.add(goneTestPID)

	var calls int32
	pollFn := func() ([]guestexec.ExitReport, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("supervisor unreachable")
	}
	worker := newDegradeTestWorker(registry, tracker, pollFn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.pollLoop(ctx)

	result := waitTerminalResult(t, registry, "job")
	if result.ExitCode != unobservedExitCode {
		t.Fatalf("degraded exit code = %d, want %d", result.ExitCode, unobservedExitCode)
	}
	if got := atomic.LoadInt32(&calls); got < int32(degradeFailureThreshold) {
		t.Fatalf("poll calls before degrade = %d, want at least %d", got, degradeFailureThreshold)
	}
}

// TestPollLoopDoesNotDegradeOnTransientFailures proves that fewer than the
// threshold of consecutive failures, cleared by a success, never degrades: the
// execution stays running even though its process id is gone.
func TestPollLoopDoesNotDegradeOnTransientFailures(t *testing.T) {
	registry := guestexec.New(guestexec.Options{Retention: time.Minute, HeartbeatInterval: time.Hour})
	registerGoneRunner(t, registry, "job", goneTestPID)
	tracker := newPIDTracker()
	tracker.add(goneTestPID)

	var calls int32
	pollFn := func() ([]guestexec.ExitReport, error) {
		attempt := atomic.AddInt32(&calls, 1)
		if int(attempt) < degradeFailureThreshold {
			return nil, errors.New("transient")
		}
		return nil, nil
	}
	worker := newDegradeTestWorker(registry, tracker, pollFn)
	ctx, cancel := context.WithCancel(context.Background())
	go worker.pollLoop(ctx)

	// Let the loop run well past the threshold so a degrade would have fired if
	// the counter were not reset by the success.
	deadline := time.Now().Add(degradeTestTimeout)
	for atomic.LoadInt32(&calls) < int32(3*degradeFailureThreshold) {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("poll loop did not iterate enough")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	states := registry.List()
	if len(states) != 1 || !states[0].Running {
		t.Fatalf("execution state = %+v, want one still-running execution", states)
	}
}

func waitTerminalResult(t *testing.T, registry *guestexec.Registry, executionID string) guestexec.TerminalResult {
	t.Helper()
	events, cancel, err := registry.Subscribe(executionID, 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()
	deadline := time.After(degradeTestTimeout)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("event stream closed before terminal result")
			}
			if result, ok := event.Payload.(guestexec.TerminalResult); ok {
				return result
			}
		case <-deadline:
			t.Fatal("no terminal result before deadline")
		}
	}
}
