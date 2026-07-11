package guestexec

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

func TestUnsubscribeDoesNotStopExecutionAndReconnectReplaysTerminalResult(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "finished")
	registry := newTestRegistry()
	executionID := "survives-disconnect"
	script := fmt.Sprintf("test x$GUESTEXEC_VALUE = xexpected; echo before; sleep 0.2; touch %q; echo after", markerPath)

	outcome, err := registry.Start(ExecSpec{
		ExecutionID: executionID,
		Slot:        1,
		Command:     "/bin/sh",
		Args:        []string{"-c", script},
		Env:         map[string]string{"GUESTEXEC_VALUE": "expected"},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if outcome != OutcomeAccepted {
		t.Fatalf("Start outcome = %v, want %v", outcome, OutcomeAccepted)
	}

	events, unsubscribe, err := registry.Subscribe(executionID, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitForLog(t, events, "before")
	unsubscribe()

	replayed, stopReplay, err := registry.Subscribe(executionID, 0)
	if err != nil {
		t.Fatalf("Subscribe replay: %v", err)
	}
	defer stopReplay()
	allEvents := collectThroughTerminal(t, replayed)

	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("child did not finish after unsubscribe: %v", err)
	}
	assertContiguousSequences(t, allEvents, 1)
	if !containsLog(allEvents, "before") || !containsLog(allEvents, "after") {
		t.Fatalf("replayed logs = %q, want before and after", joinedLogs(allEvents))
	}
	if !containsHeartbeat(allEvents) {
		t.Fatal("replayed events do not contain a heartbeat")
	}
	result := terminalResult(t, allEvents)
	if result.ExitCode != 0 {
		t.Fatalf("terminal exit code = %d, want 0; message = %q", result.ExitCode, result.Message)
	}
}

func TestReconnectFromSequenceReceivesOnlyLaterContiguousEvents(t *testing.T) {
	registry := newTestRegistry()
	executionID := "resume-cursor"
	_, err := registry.Start(ExecSpec{
		ExecutionID: executionID,
		Slot:        2,
		Command:     "/bin/sh",
		Args:        []string{"-c", "echo one; sleep 0.15; echo two; sleep 0.15; echo three"},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	first, unsubscribe, err := registry.Subscribe(executionID, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cursor := waitForLog(t, first, "one").Sequence
	unsubscribe()

	resumed, stopResumed, err := registry.Subscribe(executionID, cursor)
	if err != nil {
		t.Fatalf("Subscribe resumed: %v", err)
	}
	defer stopResumed()
	events := collectThroughTerminal(t, resumed)

	if len(events) == 0 {
		t.Fatal("resumed subscription returned no events")
	}
	assertContiguousSequences(t, events, cursor+1)
	if containsLog(events, "one") {
		t.Fatalf("resumed logs unexpectedly contain cursor event: %q", joinedLogs(events))
	}
	if !containsLog(events, "two") || !containsLog(events, "three") {
		t.Fatalf("resumed logs = %q, want two and three", joinedLogs(events))
	}
}

func TestStartIsIdempotentWhileExecutionIsRunning(t *testing.T) {
	counterPath := filepath.Join(t.TempDir(), "starts")
	registry := newTestRegistry()
	script := fmt.Sprintf("echo start >> %q; sleep 0.2", counterPath)
	spec := ExecSpec{
		ExecutionID: "idempotent",
		Slot:        3,
		Command:     "/bin/sh",
		Args:        []string{"-c", script},
	}

	first, err := registry.Start(spec)
	if err != nil {
		t.Fatalf("first Start: %v", err)
	}
	second, err := registry.Start(spec)
	if err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if first != OutcomeAccepted || second != OutcomeAlreadyRunning {
		t.Fatalf("outcomes = %v, %v; want %v, %v", first, second, OutcomeAccepted, OutcomeAlreadyRunning)
	}

	events, unsubscribe, err := registry.Subscribe(spec.ExecutionID, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer unsubscribe()
	collectThroughTerminal(t, events)

	contents, err := os.ReadFile(counterPath)
	if err != nil {
		t.Fatalf("ReadFile counter: %v", err)
	}
	if lines := strings.Count(strings.TrimSpace(string(contents)), "start"); lines != 1 {
		t.Fatalf("child start count = %d, want 1; contents = %q", lines, contents)
	}
	completed, err := registry.Start(spec)
	if err != nil {
		t.Fatalf("completed Start: %v", err)
	}
	if completed != OutcomeAlreadyCompleted {
		t.Fatalf("completed Start outcome = %v, want %v", completed, OutcomeAlreadyCompleted)
	}
}

func TestStartDrainsLargeOutputWithoutStallingReaper(t *testing.T) {
	// The child writes far more than one pipe buffer before it exits, so the
	// capture goroutines must drain the pipe while the reaper waits on the
	// process. If the reaper held execState.mu across command.Wait, the drain
	// would block on that mutex, the child would block writing to the full
	// pipe, and the execution would never reach completion before the deadline.
	const outputBytes = 512 * 1024
	registry := newTestRegistry()
	spec := ExecSpec{
		ExecutionID: "large-output",
		Slot:        3,
		Command:     "/bin/sh",
		Args:        []string{"-c", "dd if=/dev/zero bs=1024 count=512 2>/dev/null"},
	}

	outcome, err := registry.Start(spec)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if outcome != OutcomeAccepted {
		t.Fatalf("Start outcome = %v, want %v", outcome, OutcomeAccepted)
	}

	events, unsubscribe, err := registry.Subscribe(spec.ExecutionID, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer unsubscribe()
	collected := collectThroughTerminal(t, events)

	if got := capturedStdoutBytes(collected); got != outputBytes {
		t.Fatalf("captured stdout bytes = %d, want %d", got, outputBytes)
	}
	result := terminalResult(t, collected)
	if result.ExitCode != 0 {
		t.Fatalf("terminal exit code = %d, want 0; message = %q", result.ExitCode, result.Message)
	}
}

func TestCompletedExecutionRemainsSubscribableWithinRetention(t *testing.T) {
	registry := New(Options{Retention: time.Second, HeartbeatInterval: 10 * time.Millisecond})
	executionID := "retained"
	_, err := registry.Start(ExecSpec{
		ExecutionID: executionID,
		Slot:        5,
		Command:     "/bin/sh",
		Args:        []string{"-c", "echo retained"},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	initial, stopInitial, err := registry.Subscribe(executionID, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	collectThroughTerminal(t, initial)
	stopInitial()

	replayed, stopReplay, err := registry.Subscribe(executionID, 0)
	if err != nil {
		t.Fatalf("Subscribe retained: %v", err)
	}
	defer stopReplay()
	events := collectThroughTerminal(t, replayed)
	if terminalResult(t, events).ExitCode != 0 {
		t.Fatalf("retained terminal result = %+v", terminalResult(t, events))
	}
}

func TestCompletedExecutionExpiresAfterRetention(t *testing.T) {
	registry := New(Options{Retention: 30 * time.Millisecond, HeartbeatInterval: time.Second})
	executionID := "expires"
	_, err := registry.Start(ExecSpec{
		ExecutionID: executionID,
		Slot:        9,
		Command:     "/bin/sh",
		Args:        []string{"-c", "true"},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	events, unsubscribe, err := registry.Subscribe(executionID, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	collectThroughTerminal(t, events)
	unsubscribe()

	deadline := time.Now().Add(testTimeout)
	for {
		_, _, err := registry.Subscribe(executionID, 0)
		if errors.Is(err, ErrExecutionNotFound) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution remained after retention: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newTestRegistry() *Registry {
	return New(Options{Retention: time.Minute, HeartbeatInterval: 20 * time.Millisecond})
}

func waitForLog(t *testing.T, events <-chan Event, substring string) Event {
	t.Helper()
	deadline := time.After(testTimeout)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("event stream closed before log %q", substring)
			}
			logChunk, ok := event.Payload.(LogChunk)
			if ok && strings.Contains(string(logChunk.Data), substring) {
				return event
			}
		case <-deadline:
			t.Fatalf("timed out waiting for log %q", substring)
		}
	}
}

func waitForPhase(t *testing.T, events <-chan Event, phase string) Event {
	t.Helper()
	deadline := time.After(testTimeout)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("event stream closed before phase %q", phase)
			}
			change, ok := event.Payload.(PhaseChange)
			if ok && change.Phase == phase {
				return event
			}
		case <-deadline:
			t.Fatalf("timed out waiting for phase %q", phase)
		}
	}
}

func collectThroughTerminal(t *testing.T, events <-chan Event) []Event {
	t.Helper()
	var collected []Event
	deadline := time.After(testTimeout)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("event stream closed before terminal result")
			}
			collected = append(collected, event)
			if _, ok := event.Payload.(TerminalResult); ok {
				return collected
			}
		case <-deadline:
			t.Fatal("timed out waiting for terminal result")
		}
	}
}

func terminalResult(t *testing.T, events []Event) TerminalResult {
	t.Helper()
	for _, event := range events {
		if result, ok := event.Payload.(TerminalResult); ok {
			return result
		}
	}
	t.Fatal("events do not contain a terminal result")
	return TerminalResult{}
}

func capturedStdoutBytes(events []Event) int {
	total := 0
	for _, event := range events {
		chunk, ok := event.Payload.(LogChunk)
		if ok && chunk.Stream == StreamStdout {
			total += len(chunk.Data)
		}
	}
	return total
}

func assertContiguousSequences(t *testing.T, events []Event, first uint64) {
	t.Helper()
	for i, event := range events {
		want := first + uint64(i)
		if event.Sequence != want {
			t.Fatalf("event %d sequence = %d, want %d; events = %+v", i, event.Sequence, want, events)
		}
	}
}

func containsLog(events []Event, substring string) bool {
	return strings.Contains(joinedLogs(events), substring)
}

func containsHeartbeat(events []Event) bool {
	for _, event := range events {
		if _, ok := event.Payload.(Heartbeat); ok {
			return true
		}
	}
	return false
}

func joinedLogs(events []Event) string {
	var builder strings.Builder
	for _, event := range events {
		if chunk, ok := event.Payload.(LogChunk); ok {
			builder.Write(chunk.Data)
		}
	}
	return builder.String()
}
