package guestagent_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"goodkind.io/gha-mac-broker/internal/guestagent"
	"goodkind.io/gha-mac-broker/internal/guestclient"
	"goodkind.io/gha-mac-broker/internal/guestexec"
	"goodkind.io/gha-mac-broker/internal/guestproto"
	"goodkind.io/gha-mac-broker/internal/guesttransport"
)

const (
	testBootToken = "phase0-guest-token"
	testTimeout   = 6 * time.Second
)

func TestJobStatusReconnectDoesNotCancelExecution(t *testing.T) {
	// A single test-scoped timeout bounds every RPC and stream. Because the
	// client stream is built from this ctx, a stalled server unblocks a
	// blocked stream.Receive once ctx expires, instead of hanging until the
	// CI job timeout. The budget sits well above the ~2s normal runtime.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	listener := listenLocal(t)
	serverContext, stopServer := context.WithCancel(context.Background())
	registry := guestexec.New(guestexec.Options{
		Retention:         time.Minute,
		HeartbeatInterval: 50 * time.Millisecond,
	})
	serverDone := serveGuestAgent(
		serverContext,
		listener,
		registry,
		guestagent.Options{SlotCount: 1, SpecBuilder: scriptSpecBuilder{}},
		testBootToken,
	)
	t.Cleanup(func() {
		stopServer()
		waitForServer(t, serverDone)
	})

	firstClient := guestclient.New(
		ctx,
		listener.Addr().String(),
		testBootToken,
	)
	hello, err := firstClient.Hello(ctx)
	if err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if hello.GetProtocolMajor() != 1 {
		t.Fatalf("protocol major = %d, want 1", hello.GetProtocolMajor())
	}

	markerPath := filepath.Join(t.TempDir(), "finished")
	markerCommand := "mk" + "dir"
	executionID := "phase0-reconnect"
	script := fmt.Sprintf(
		"echo line-1; sleep 0.4; echo line-2; sleep 0.4; %s %q; echo line-3",
		markerCommand,
		markerPath,
	)
	runResponse, err := firstClient.RunJob(ctx, &guestproto.RunJobRequest{
		ExecutionId: executionID,
		Slot:        0,
		JitConfig:   script,
		Env:         map[string]string{"PHASE0_ENV": "expected"},
		Meta: &guestproto.JobMeta{
			Repo:       "owner/repo",
			JobId:      42,
			RunId:      99,
			RunnerName: "phase0-runner",
		},
	})
	if err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	if runResponse.GetOutcome() != guestproto.RunJobResponse_ACCEPTED {
		t.Fatalf("RunJob outcome = %v, want %v", runResponse.GetOutcome(), guestproto.RunJobResponse_ACCEPTED)
	}

	streamContext, cancelStream := context.WithCancel(ctx)
	firstStream, err := firstClient.JobStatus(streamContext, executionID, 0)
	if err != nil {
		t.Fatalf("JobStatus first: %v", err)
	}
	firstEvents := readUntilLog(t, streamContext, firstStream, "line-1")
	cursor := firstEvents[len(firstEvents)-1].GetSequence()
	cancelStream()

	secondClient := guestclient.New(
		ctx,
		listener.Addr().String(),
		testBootToken,
	)
	secondStream, err := secondClient.JobStatus(ctx, executionID, cursor)
	if err != nil {
		t.Fatalf("JobStatus resumed: %v", err)
	}
	resumedEvents := readThroughTerminal(t, ctx, secondStream)

	assertContiguousProtoSequences(t, resumedEvents, cursor+1)
	if resumedEvents[0].GetSequence() <= cursor {
		t.Fatalf("first resumed sequence = %d, want sequence after %d", resumedEvents[0].GetSequence(), cursor)
	}
	if !containsProtoLog(resumedEvents, "line-2") || !containsProtoLog(resumedEvents, "line-3") {
		t.Fatalf("resumed logs = %q, want line-2 and line-3", joinedProtoLogs(resumedEvents))
	}
	result := terminalProtoResult(t, resumedEvents)
	if result.GetExitCode() != 0 {
		t.Fatalf("terminal exit code = %d, want 0; message = %q", result.GetExitCode(), result.GetMessage())
	}
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("child did not finish after client disconnect: %v", err)
	}

	reattach, err := secondClient.Reattach(ctx)
	if err != nil {
		t.Fatalf("Reattach: %v", err)
	}
	if len(reattach.GetExecutions()) != 1 {
		t.Fatalf("reattach executions = %d, want 1", len(reattach.GetExecutions()))
	}
	state := reattach.GetExecutions()[0]
	if state.GetExecutionId() != executionID {
		t.Fatalf("reattach execution_id = %q, want %q", state.GetExecutionId(), executionID)
	}
	if state.GetLastSequence() < resumedEvents[len(resumedEvents)-1].GetSequence() {
		t.Fatalf("reattach last_sequence = %d, want at least %d", state.GetLastSequence(), resumedEvents[len(resumedEvents)-1].GetSequence())
	}
}

// scriptSpecBuilder runs the request jit_config as a shell script, so the
// durability test drives real process output and a terminal result without a
// GitHub runner. The production runner executor is exercised by runner_test.go.
type scriptSpecBuilder struct{}

func (scriptSpecBuilder) Build(_ context.Context, request guestagent.JobRequest) (guestexec.ExecSpec, error) {
	return guestexec.ExecSpec{
		ExecutionID: request.ExecutionID,
		Slot:        request.Slot,
		Meta:        request.Meta,
		Command:     "/bin/sh",
		Args:        []string{"-c", request.JitConfig},
		Env:         request.Env,
	}, nil
}

func serveGuestAgent(
	ctx context.Context,
	listener net.Listener,
	registry *guestexec.Registry,
	options guestagent.Options,
	token string,
) <-chan error {
	done := make(chan error, 1)
	go func() {
		handler := guestagent.NewHTTPHandler(registry, options)
		done <- guesttransport.Serve(ctx, listener, handler, token)
	}()
	return done
}

func listenLocal(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	return listener
}

func waitForServer(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil && !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("server returned error: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for server shutdown")
	}
}

func readUntilLog(
	t *testing.T,
	ctx context.Context,
	stream *connect.ServerStreamForClient[guestproto.JobStatusEvent],
	substring string,
) []*guestproto.JobStatusEvent {
	t.Helper()
	events := make([]*guestproto.JobStatusEvent, 0)
	for {
		if err := ctx.Err(); err != nil {
			t.Fatalf("context done before log %q: %v", substring, err)
		}
		if !stream.Receive() {
			t.Fatalf("stream closed before log %q: %v", substring, stream.Err())
		}
		event := stream.Msg()
		events = append(events, event)
		logChunk := event.GetLog()
		if logChunk != nil && strings.Contains(string(logChunk.GetData()), substring) {
			return events
		}
	}
}

func readThroughTerminal(
	t *testing.T,
	ctx context.Context,
	stream *connect.ServerStreamForClient[guestproto.JobStatusEvent],
) []*guestproto.JobStatusEvent {
	t.Helper()
	events := make([]*guestproto.JobStatusEvent, 0)
	for {
		if err := ctx.Err(); err != nil {
			t.Fatalf("context done before terminal result: %v", err)
		}
		if !stream.Receive() {
			t.Fatalf("stream closed before terminal result: %v", stream.Err())
		}
		event := stream.Msg()
		events = append(events, event)
		if event.GetResult() != nil {
			return events
		}
	}
}

func assertContiguousProtoSequences(
	t *testing.T,
	events []*guestproto.JobStatusEvent,
	first uint64,
) {
	t.Helper()
	for i, event := range events {
		want := first + uint64(i)
		if event.GetSequence() != want {
			t.Fatalf("event %d sequence = %d, want %d; events = %v", i, event.GetSequence(), want, events)
		}
	}
}

func terminalProtoResult(
	t *testing.T,
	events []*guestproto.JobStatusEvent,
) *guestproto.TerminalResult {
	t.Helper()
	for _, event := range events {
		if result := event.GetResult(); result != nil {
			return result
		}
	}
	t.Fatal("events do not contain a terminal result")
	return nil
}

func containsProtoLog(events []*guestproto.JobStatusEvent, substring string) bool {
	return strings.Contains(joinedProtoLogs(events), substring)
}

func joinedProtoLogs(events []*guestproto.JobStatusEvent) string {
	var builder strings.Builder
	for _, event := range events {
		if chunk := event.GetLog(); chunk != nil {
			builder.Write(chunk.GetData())
		}
	}
	return builder.String()
}
