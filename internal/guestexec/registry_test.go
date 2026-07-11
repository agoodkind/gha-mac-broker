package guestexec

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

func TestStartLaunchesProcessAndRetainsCompletionEvents(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "finished")
	registry := newTestRegistry()
	spec := ExecSpec{
		ExecutionID: "completes",
		Slot:        1,
		Command:     "/bin/sh",
		Args:        []string{"-c", fmt.Sprintf("echo hello; touch %q", markerPath)},
		Env:         map[string]string{"GUESTEXEC_VALUE": "expected"},
	}

	outcome, err := registry.Start(spec)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if outcome != OutcomeAccepted {
		t.Fatalf("Start outcome = %v, want %v", outcome, OutcomeAccepted)
	}
	waitForCompletion(t, registry, spec)

	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("child did not finish: %v", err)
	}
	events := executionEvents(t, registry, spec.ExecutionID)
	if !containsLog(events, "hello") {
		t.Fatalf("events do not contain stdout log: %+v", events)
	}
	result := terminalResult(t, events)
	if result.ExitCode != 0 {
		t.Fatalf("terminal exit code = %d, want 0; message = %q", result.ExitCode, result.Message)
	}
}

func TestStartIsIdempotentWhileExecutionIsRunning(t *testing.T) {
	counterPath := filepath.Join(t.TempDir(), "starts")
	registry := newTestRegistry()
	script := fmt.Sprintf("echo start >> %q; sleep 0.2", counterPath)
	spec := ExecSpec{
		ExecutionID: "idempotent",
		Slot:        2,
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

	waitForCompletion(t, registry, spec)
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
	waitForCompletion(t, registry, spec)

	events := executionEvents(t, registry, spec.ExecutionID)
	if got := capturedStdoutBytes(events); got != outputBytes {
		t.Fatalf("captured stdout bytes = %d, want %d", got, outputBytes)
	}
	result := terminalResult(t, events)
	if result.ExitCode != 0 {
		t.Fatalf("terminal exit code = %d, want 0; message = %q", result.ExitCode, result.Message)
	}
}

func newTestRegistry() *Registry {
	return New(Options{Retention: time.Minute, HeartbeatInterval: 20 * time.Millisecond})
}

func waitForCompletion(t *testing.T, registry *Registry, spec ExecSpec) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for {
		outcome, err := registry.Start(spec)
		if err != nil {
			t.Fatalf("Start while waiting for completion: %v", err)
		}
		if outcome == OutcomeAlreadyCompleted {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution did not complete before deadline, last outcome %v", outcome)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func executionEvents(t *testing.T, registry *Registry, executionID string) []Event {
	t.Helper()
	registry.mu.Lock()
	execState, found := registry.executions[executionID]
	registry.mu.Unlock()
	if !found {
		t.Fatalf("execution %q not found", executionID)
	}
	execState.mu.Lock()
	events := append([]Event(nil), execState.events...)
	execState.mu.Unlock()
	return events
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

func containsLog(events []Event, substring string) bool {
	var builder strings.Builder
	for _, event := range events {
		if chunk, ok := event.Payload.(LogChunk); ok {
			builder.Write(chunk.Data)
		}
	}
	return strings.Contains(builder.String(), substring)
}
