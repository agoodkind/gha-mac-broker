package guestexec

import (
	"errors"
	"os"
	"testing"
	"time"
)

// TestRegisterAndReportExitEmitOrderedTerminalStreamOnce proves the
// register-driven path produces the same ordered stream today's Start does: a
// running phase, captured logs, a completed phase, and exactly one terminal
// result. A duplicate exit report for the same pid must not emit a second
// terminal.
func TestRegisterAndReportExitEmitOrderedTerminalStreamOnce(t *testing.T) {
	registry := newTestRegistry()
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}

	const pid = 4242
	spec := ExecSpec{ExecutionID: "registered", Slot: 1, Command: "/bin/true"}
	outcome, err := registry.Register(spec, pid, pid, stdoutReader, stderrReader)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if outcome != OutcomeAccepted {
		t.Fatalf("Register outcome = %v, want %v", outcome, OutcomeAccepted)
	}

	live, unsubscribe, err := registry.Subscribe(spec.ExecutionID, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := stdoutWriter.Write([]byte("hello\n")); err != nil {
		t.Fatalf("write stdout: %v", err)
	}
	waitForLog(t, live, "hello")
	unsubscribe()

	// Close the write ends so the capture goroutines reach EOF, then report the
	// exit twice to prove idempotency.
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	registry.ReportExit(pid, 0, "")
	registry.ReportExit(pid, 0, "")
	// An unknown pid must be a safe no-op.
	registry.ReportExit(pid+1, 7, "stray")

	replay, stopReplay, err := registry.Subscribe(spec.ExecutionID, 0)
	if err != nil {
		t.Fatalf("Subscribe replay: %v", err)
	}
	defer stopReplay()
	collected := collectThroughTerminal(t, replay)
	assertContiguousSequences(t, collected, 1)
	if !containsLog(collected, "hello") {
		t.Fatalf("logs = %q, want hello", joinedLogs(collected))
	}
	assertPhaseOrder(t, collected)
	if result := terminalResult(t, collected); result.ExitCode != 0 {
		t.Fatalf("terminal exit code = %d, want 0; message = %q", result.ExitCode, result.Message)
	}
	if terminals := terminalCountOf(t, registry, spec.ExecutionID); terminals != 1 {
		t.Fatalf("terminal events = %d, want exactly 1", terminals)
	}
}

// TestFreezeRestoreRoundTripsInFlightExecution proves Freeze then Restore
// continues an in-flight execution's sequence with no gap and no duplicate: a
// subscriber resuming at cursor N sees N+1 next, and the restored capture
// goroutine resumes reading the same pipe.
func TestFreezeRestoreRoundTripsInFlightExecution(t *testing.T) {
	registry := New(Options{Retention: time.Minute, HeartbeatInterval: time.Hour})
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}

	const pid = 7777
	spec := ExecSpec{ExecutionID: "inflight", Slot: 2, Command: "/bin/true"}
	if _, err := registry.Register(spec, pid, pid, stdoutReader, stderrReader); err != nil {
		t.Fatalf("Register: %v", err)
	}

	events, unsubscribe, err := registry.Subscribe(spec.ExecutionID, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := stdoutWriter.Write([]byte("chunk-1\n")); err != nil {
		t.Fatalf("write chunk-1: %v", err)
	}
	cursor := waitForLog(t, events, "chunk-1").Sequence
	unsubscribe()

	snapshot, err := registry.Freeze()
	if err != nil {
		t.Fatalf("Freeze: %v", err)
	}

	exitReports := make(chan ExitReport, 1)
	restored := New(Options{})
	if err := restored.Restore(
		snapshot,
		map[int]PipeReadEnds{pid: {Stdout: stdoutReader, Stderr: stderrReader}},
		exitReports,
	); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	resumed, stopResumed, err := restored.Subscribe(spec.ExecutionID, cursor)
	if err != nil {
		t.Fatalf("Subscribe resumed: %v", err)
	}
	defer stopResumed()

	if _, err := stdoutWriter.Write([]byte("chunk-2\n")); err != nil {
		t.Fatalf("write chunk-2: %v", err)
	}
	next := waitForLog(t, resumed, "chunk-2")
	if next.Sequence != cursor+1 {
		t.Fatalf("resumed next sequence = %d, want %d (no gap, no duplicate)", next.Sequence, cursor+1)
	}

	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	exitReports <- ExitReport{PID: pid, ExitCode: 0, Message: ""}
	close(exitReports)

	terminal := collectThroughTerminal(t, resumed)
	if result := terminalResult(t, terminal); result.ExitCode != 0 {
		t.Fatalf("restored terminal exit code = %d, want 0", result.ExitCode)
	}
}

// TestFreezeRestorePreservesEvictedLogChunkAsNil proves the snapshot round-trips
// an evicted LogChunk faithfully: a chunk whose Data the log-byte cap zeroed
// stays nil after Restore and is not re-materialized, while a later live chunk
// keeps its data.
func TestFreezeRestorePreservesEvictedLogChunkAsNil(t *testing.T) {
	registry := New(Options{Retention: time.Minute, HeartbeatInterval: time.Hour})
	// A completed execution is enough to exercise snapshot serialization of an
	// evicted chunk, and it needs no supervisor goroutine, so Freeze takes it
	// without the running-path completion barrier.
	execState := &execution{
		id:          "evict",
		slot:        3,
		phase:       "completed",
		running:     false,
		completedAt: time.Now(),
		notify:      make(chan struct{}),
		done:        make(chan struct{}),
		exitReport:  make(chan ExitReport, 1),
		stopped:     make(chan struct{}),
		supervised:  make(chan struct{}),
	}
	execState.appendEventLocked(PhaseChange{Phase: "running"})
	overCapChunks := (maxRetainedLogBytes / outputReadBufferSize) + 8
	for range overCapChunks {
		payload := make([]byte, outputReadBufferSize)
		for index := range payload {
			payload[index] = 'x'
		}
		execState.appendEventLocked(LogChunk{Stream: StreamStdout, Data: payload})
	}
	marker := []byte("LIVE_MARKER")
	execState.appendEventLocked(LogChunk{Stream: StreamStdout, Data: marker})
	markerIndex := len(execState.events) - 1
	execState.appendEventLocked(PhaseChange{Phase: "completed"})
	execState.appendEventLocked(TerminalResult{ExitCode: 0})

	registry.mu.Lock()
	registry.executions[execState.id] = execState
	registry.mu.Unlock()

	firstChunk, ok := execState.events[1].Payload.(LogChunk)
	if !ok || firstChunk.Data != nil {
		t.Fatalf("events[1] = %+v, want an evicted LogChunk with nil data", execState.events[1].Payload)
	}

	snapshot, err := registry.Freeze()
	if err != nil {
		t.Fatalf("Freeze: %v", err)
	}
	restored := New(Options{})
	if err := restored.Restore(snapshot, nil, nil); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	evicted := eventPayloadAt(t, restored, "evict", 1)
	evictedChunk, ok := evicted.(LogChunk)
	if !ok {
		t.Fatalf("restored events[1] = %T, want LogChunk", evicted)
	}
	if evictedChunk.Data != nil {
		t.Fatalf("restored evicted chunk data len = %d, want nil", len(evictedChunk.Data))
	}

	live := eventPayloadAt(t, restored, "evict", markerIndex)
	liveChunk, ok := live.(LogChunk)
	if !ok || string(liveChunk.Data) != "LIVE_MARKER" {
		t.Fatalf("restored live marker = %+v, want LogChunk carrying LIVE_MARKER", live)
	}
}

// TestRestoreReArmsRetentionFromCompletedAt proves a completed execution
// restored near the end of its retention window expires on schedule, not a full
// window later. Arming from now would keep it past the remaining window.
func TestRestoreReArmsRetentionFromCompletedAt(t *testing.T) {
	const retention = 400 * time.Millisecond
	const remaining = 80 * time.Millisecond
	snapshot := &Snapshot{
		Retention:         retention,
		HeartbeatInterval: time.Hour,
		Executions: []ExecutionSnapshot{{
			ExecutionID:  "near-expiry",
			Slot:         4,
			Phase:        "completed",
			Running:      false,
			Reaped:       true,
			ExitReported: true,
			CompletedAt:  time.Now().Add(-(retention - remaining)),
			Events: []Event{
				{Sequence: 1, Payload: PhaseChange{Phase: "running"}},
				{Sequence: 2, Payload: PhaseChange{Phase: "completed"}},
				{Sequence: 3, Payload: TerminalResult{ExitCode: 0}},
			},
		}},
	}

	restored := New(Options{})
	if err := restored.Restore(snapshot, nil, nil); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, _, err := restored.Subscribe("near-expiry", 0); err != nil {
		t.Fatalf("expected execution present right after restore: %v", err)
	}

	// The remaining window is ~80ms. A half-retention deadline (200ms) is well
	// under a full window (400ms), so expiry within it proves re-arming from
	// completedAt rather than from now.
	deadline := time.Now().Add(retention / 2)
	for {
		_, _, err := restored.Subscribe("near-expiry", 0)
		if errors.Is(err, ErrExecutionNotFound) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("execution did not expire within the remaining window; retention was re-armed from now, not completedAt")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestFreezeDuringInFlightReapSnapshotsCompleted proves the completion barrier.
// The exit report is delivered while the capture pipes are still open, so the
// supervisor consumes it and blocks in readers.Wait with exitReported set but no
// terminal emitted. Freeze must stop the captures, let that in-flight reap run to
// completion, and snapshot a completed execution. Without the barrier the
// snapshot could capture exitReported=true with running=true and no terminal,
// which Restore can never complete (a re-delivered report is dropped as stale),
// hanging the execution and blocking drain forever.
func TestFreezeDuringInFlightReapSnapshotsCompleted(t *testing.T) {
	registry := New(Options{Retention: time.Minute, HeartbeatInterval: time.Hour})
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	defer func() { _ = stdoutReader.Close() }()
	defer func() { _ = stderrReader.Close() }()
	defer func() { _ = stdoutWriter.Close() }()
	defer func() { _ = stderrWriter.Close() }()

	const pid = 5555
	spec := ExecSpec{ExecutionID: "reap-race", Slot: 1, Command: "/bin/true"}
	if _, err := registry.Register(spec, pid, pid, stdoutReader, stderrReader); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// The pipes stay open, so the captures block on Read and the reap blocks in
	// readers.Wait once it consumes this report.
	registry.ReportExit(pid, 0, "")
	waitForExitReported(t, registry, spec.ExecutionID)

	snapshot, err := registry.Freeze()
	if err != nil {
		t.Fatalf("Freeze: %v", err)
	}
	if len(snapshot.Executions) != 1 {
		t.Fatalf("snapshot executions = %d, want 1", len(snapshot.Executions))
	}
	frozen := snapshot.Executions[0]
	if frozen.Running {
		t.Fatal("snapshot execution is still running; the barrier did not wait for the in-flight reap")
	}
	if terminals := countTerminals(frozen.Events); terminals != 1 {
		t.Fatalf("snapshot terminal events = %d, want exactly 1", terminals)
	}

	restored := New(Options{})
	if err := restored.Restore(snapshot, nil, nil); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	events, unsubscribe, err := restored.Subscribe(spec.ExecutionID, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer unsubscribe()
	collected := collectThroughTerminal(t, events)
	if result := terminalResult(t, collected); result.ExitCode != 0 {
		t.Fatalf("restored terminal exit code = %d, want 0", result.ExitCode)
	}
	if terminals := terminalCountOf(t, restored, spec.ExecutionID); terminals != 1 {
		t.Fatalf("restored terminal events = %d, want exactly 1", terminals)
	}
	if state := restored.Drain(); !state.Idle || state.ActiveExecutions != 0 {
		t.Fatalf("restored drain = %+v, want idle with no active executions", state)
	}
}

func assertPhaseOrder(t *testing.T, events []Event) {
	t.Helper()
	runningAt := -1
	completedAt := -1
	for index, event := range events {
		change, ok := event.Payload.(PhaseChange)
		if !ok {
			continue
		}
		if change.Phase == "running" && runningAt < 0 {
			runningAt = index
		}
		if change.Phase == "completed" {
			completedAt = index
		}
	}
	if runningAt < 0 || completedAt < 0 || runningAt >= completedAt {
		t.Fatalf("phase order = running@%d completed@%d, want running before completed", runningAt, completedAt)
	}
}

func terminalCountOf(t *testing.T, registry *Registry, executionID string) int {
	t.Helper()
	registry.mu.Lock()
	execState, found := registry.executions[executionID]
	registry.mu.Unlock()
	if !found {
		t.Fatalf("execution %q not found", executionID)
	}
	execState.mu.Lock()
	defer execState.mu.Unlock()
	count := 0
	for _, event := range execState.events {
		if _, ok := event.Payload.(TerminalResult); ok {
			count++
		}
	}
	return count
}

func eventPayloadAt(t *testing.T, registry *Registry, executionID string, index int) EventPayload {
	t.Helper()
	registry.mu.Lock()
	execState, found := registry.executions[executionID]
	registry.mu.Unlock()
	if !found {
		t.Fatalf("execution %q not found", executionID)
	}
	execState.mu.Lock()
	defer execState.mu.Unlock()
	if index < 0 || index >= len(execState.events) {
		t.Fatalf("event index %d out of range (len %d)", index, len(execState.events))
	}
	return execState.events[index].Payload
}

func waitForExitReported(t *testing.T, registry *Registry, executionID string) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for {
		registry.mu.Lock()
		execState, found := registry.executions[executionID]
		registry.mu.Unlock()
		if found {
			execState.mu.Lock()
			reported := execState.exitReported
			execState.mu.Unlock()
			if reported {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("exit report was not observed before deadline")
		}
		time.Sleep(time.Millisecond)
	}
}

func countTerminals(events []Event) int {
	count := 0
	for _, event := range events {
		if _, ok := event.Payload.(TerminalResult); ok {
			count++
		}
	}
	return count
}
