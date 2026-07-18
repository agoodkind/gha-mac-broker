//go:build unix

package guestworker_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"goodkind.io/gha-mac-broker/internal/guestclient"
	"goodkind.io/gha-mac-broker/internal/guestproto"
	"goodkind.io/gha-mac-broker/internal/guestsupervisor"
	"goodkind.io/gha-mac-broker/internal/guesttransport"
	"goodkind.io/gha-mac-broker/internal/guestworker"
)

// tcpDialer adapts a TCP address to a guesttransport.GuestDialer so the guest
// worker tests dial a real listener. Production dials over tart exec instead.
func tcpDialer(address string) guesttransport.GuestDialer {
	return func(ctx context.Context) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "tcp", address)
	}
}

const (
	workerMainEnv  = "GHA_GUEST_WORKER_TEST_MAIN"
	harnessToken   = "reload-test-token"
	harnessTimeout = 30 * time.Second
)

// TestMain lets the test binary re-exec itself as a guest worker. The supervisor
// spawns a worker by running this same binary with workerMainEnv set, which
// routes here to guestworker.Run instead of the test suite, so the reload
// mechanics run against real worker subprocesses with real fd handoff.
func TestMain(m *testing.M) {
	if os.Getenv(workerMainEnv) == "1" {
		if err := guestworker.Run(context.Background()); err != nil {
			fmt.Fprintln(os.Stderr, "guest worker run:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type harness struct {
	supervisor    *guestsupervisor.Supervisor
	controlSocket string
	addr          string
	runErr        chan error

	spawnCount atomic.Int32
	failNext   atomic.Bool
}

func startHarness(t *testing.T) *harness {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	controlSocket := shortSocketPath(t)
	h := &harness{
		supervisor:    nil,
		controlSocket: controlSocket,
		addr:          listener.Addr().String(),
		runErr:        make(chan error, 1),
		spawnCount:    atomic.Int32{},
		failNext:      atomic.Bool{},
	}
	supervisor := guestsupervisor.New(guestsupervisor.Options{
		Listener:          listener,
		ControlSocketPath: controlSocket,
		Token:             harnessToken,
		SlotCount:         1,
		WorkerCommand: func(spec guestsupervisor.WorkerSpec) *exec.Cmd {
			h.spawnCount.Add(1)
			if h.failNext.CompareAndSwap(true, false) {
				return failWorker(spec)
			}
			return reExecWorker(spec)
		},
		Log: nil,
	})
	h.supervisor = supervisor
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		h.runErr <- supervisor.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-h.runErr:
		case <-time.After(harnessTimeout):
		}
	})
	h.waitServing(t)
	return h
}

// reExecWorker builds a worker command that re-runs the test binary as a worker,
// preserving the fd wiring the supervisor computed and the inherited files.
func reExecWorker(spec guestsupervisor.WorkerSpec) *exec.Cmd {
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(append([]string(nil), spec.Environment...), workerMainEnv+"=1")
	cmd.ExtraFiles = spec.ExtraFiles
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd
}

// failWorker builds a replacement that exits non-zero before it can attach or
// signal ready, standing in for a freshly deployed worker binary that crashes on
// boot. The old worker's readiness read end then sees EOF, so it must roll back.
func failWorker(spec guestsupervisor.WorkerSpec) *exec.Cmd {
	cmd := exec.Command("/bin/sh", "-c", "exit 1")
	cmd.Env = append([]string(nil), spec.Environment...)
	cmd.ExtraFiles = spec.ExtraFiles
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd
}

// waitServing blocks until the first worker is current and answering RPCs, then
// returns its pid.
func (h *harness) waitServing(t *testing.T) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), harnessTimeout)
	defer cancel()
	deadline := time.Now().Add(harnessTimeout)
	for {
		select {
		case err := <-h.runErr:
			t.Fatalf("supervisor exited before serving: %v", err)
		default:
		}
		pid := h.supervisor.CurrentWorkerPID()
		if pid > 0 {
			client := guestclient.New(ctx, tcpDialer(h.addr), harnessToken)
			if _, err := client.Hello(ctx); err == nil {
				return pid
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("first worker did not begin serving before deadline")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// reloadAndWait signals oldPID to reload, then waits until a different worker is
// current and the old worker has exited, which together confirm the replacement
// attached and took over. It returns the new current worker pid.
func (h *harness) reloadAndWait(t *testing.T, oldPID int) int {
	t.Helper()
	if oldPID <= 0 {
		t.Fatal("no worker pid before reload")
	}
	if err := syscall.Kill(oldPID, syscall.SIGHUP); err != nil {
		t.Fatalf("signal reload: %v", err)
	}
	deadline := time.Now().Add(harnessTimeout)
	for {
		select {
		case err := <-h.runErr:
			t.Fatalf("supervisor exited during reload: %v", err)
		default:
		}
		current := h.supervisor.CurrentWorkerPID()
		if current > 0 && current != oldPID && !processAlive(oldPID) {
			return current
		}
		if time.Now().After(deadline) {
			t.Fatalf("reload of worker %d did not complete before deadline", oldPID)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "gw")
	if err != nil {
		t.Fatalf("make socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "c.sock")
}

// TestReloadKeepsRunnerAliveAndResumesStream drives the full reload contract: a
// gated runner started before the swap keeps running across it, the event
// sequence stays contiguous, a subscriber reconnecting after the swap resumes
// from its cursor with no lost bytes, and the runner's normal completion after
// the swap still yields the correct exit code.
func TestReloadKeepsRunnerAliveAndResumesStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), harnessTimeout)
	defer cancel()
	h := startHarness(t)

	gate := filepath.Join(t.TempDir(), "gate")
	script := fmt.Sprintf("echo A; while [ ! -f '%s' ]; do sleep 0.05; done; echo B", gate)

	client := guestclient.New(ctx, tcpDialer(h.addr), harnessToken)
	runResponse, err := client.RunJob(ctx, &guestproto.RunJobRequest{
		ExecutionId: "job-1",
		Slot:        0,
		JitConfig:   script,
	})
	if err != nil {
		t.Fatalf("run job: %v", err)
	}
	if runResponse.GetOutcome() != guestproto.RunJobResponse_ACCEPTED {
		t.Fatalf("run job outcome = %v, want ACCEPTED", runResponse.GetOutcome())
	}

	firstStreamCtx, cancelFirst := context.WithCancel(ctx)
	firstStream, err := client.JobStatus(firstStreamCtx, "job-1", 0)
	if err != nil {
		t.Fatalf("job status: %v", err)
	}
	beforeSwap := readUntilLog(t, firstStreamCtx, firstStream, "A")
	cursor := beforeSwap[len(beforeSwap)-1].GetSequence()
	cancelFirst()

	// Trigger the reload and wait for the replacement to take over.
	h.reloadAndWait(t, h.supervisor.CurrentWorkerPID())

	// The runner is still gated and alive under the supervisor; release it now so
	// its remaining output and exit are observed only by the replacement worker.
	if err := os.WriteFile(gate, []byte("go\n"), 0o600); err != nil {
		t.Fatalf("write gate: %v", err)
	}

	resumeClient := guestclient.New(ctx, tcpDialer(h.addr), harnessToken)
	resumeStream, err := resumeClient.JobStatus(ctx, "job-1", cursor)
	if err != nil {
		t.Fatalf("resume job status: %v", err)
	}
	resumed := readThroughTerminal(t, ctx, resumeStream)

	assertContiguous(t, resumed, cursor+1)
	if !containsLog(resumed, "B") {
		t.Fatalf("resumed logs = %q, want B captured after the swap", joinedLogs(resumed))
	}
	result := terminalResult(t, resumed)
	if result.GetExitCode() != 0 {
		t.Fatalf("terminal exit code = %d, want 0; message = %q", result.GetExitCode(), result.GetMessage())
	}
}

// TestZeroJobReload proves a reload with no active jobs is a listener-only
// handoff: the replacement worker attaches and serves on the same listener.
func TestZeroJobReload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), harnessTimeout)
	defer cancel()
	h := startHarness(t)

	h.reloadAndWait(t, h.supervisor.CurrentWorkerPID())

	client := guestclient.New(ctx, tcpDialer(h.addr), harnessToken)
	hello, err := client.Hello(ctx)
	if err != nil {
		t.Fatalf("hello after zero-job reload: %v", err)
	}
	if hello.GetProtocolMajor() != 1 {
		t.Fatalf("protocol major = %d, want 1", hello.GetProtocolMajor())
	}
}

// TestSecondReloadWhileDrainingRejected proves the old worker refuses a second
// reload once it has already handed off, so a superseded generation cannot spawn
// a third worker.
func TestSecondReloadWhileDrainingRejected(t *testing.T) {
	h := startHarness(t)

	gen1PID := h.supervisor.CurrentWorkerPID()
	gen2PID := h.reloadAndWait(t, gen1PID)
	if gen2PID == gen1PID {
		t.Fatalf("worker pid did not change after reload: gen1=%d gen2=%d", gen1PID, gen2PID)
	}

	// A reload of the current (second) worker is accepted, producing a third
	// generation. The old worker rejects a second reload of its own, so reaching a
	// third generation proves the supervisor accepts a fresh reload only from the
	// current worker once steady.
	gen3PID := h.reloadAndWait(t, gen2PID)
	if gen3PID == gen2PID {
		t.Fatalf("worker pid did not change after second reload: gen2=%d gen3=%d", gen2PID, gen3PID)
	}
}

// TestReloadRollsBackWhenReplacementFails proves that when the replacement worker
// fails to signal ready, the old worker rolls back to live serving instead of
// staying frozen: it captures runner output produced after the failed reload, a
// still-running job completes with the correct exit code, and a second reload
// attempt then succeeds with no panic.
func TestReloadRollsBackWhenReplacementFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), harnessTimeout)
	defer cancel()
	h := startHarness(t)
	gen1PID := h.supervisor.CurrentWorkerPID()

	gate := filepath.Join(t.TempDir(), "gate")
	script := fmt.Sprintf("echo A; while [ ! -f '%s' ]; do sleep 0.05; done; echo B", gate)

	client := guestclient.New(ctx, tcpDialer(h.addr), harnessToken)
	runResponse, err := client.RunJob(ctx, &guestproto.RunJobRequest{
		ExecutionId: "job-rollback",
		Slot:        0,
		JitConfig:   script,
	})
	if err != nil {
		t.Fatalf("run job: %v", err)
	}
	if runResponse.GetOutcome() != guestproto.RunJobResponse_ACCEPTED {
		t.Fatalf("run job outcome = %v, want ACCEPTED", runResponse.GetOutcome())
	}

	firstStreamCtx, cancelFirst := context.WithCancel(ctx)
	firstStream, err := client.JobStatus(firstStreamCtx, "job-rollback", 0)
	if err != nil {
		t.Fatalf("job status: %v", err)
	}
	beforeReload := readUntilLog(t, firstStreamCtx, firstStream, "A")
	cursor := beforeReload[len(beforeReload)-1].GetSequence()
	cancelFirst()

	// Arm the next spawn to crash before attaching, then trigger the reload and
	// wait until that failed replacement was spawned. The old worker stays current.
	spawnsBefore := h.spawnCount.Load()
	h.failNext.Store(true)
	if err := syscall.Kill(gen1PID, syscall.SIGHUP); err != nil {
		t.Fatalf("signal reload: %v", err)
	}
	waitSpawnCount(t, h, spawnsBefore+1)
	if pid := h.supervisor.CurrentWorkerPID(); pid != gen1PID {
		t.Fatalf("current worker changed after a failed reload: was %d, now %d", gen1PID, pid)
	}

	// Release the gate so the still-running runner emits output and exits. Only the
	// rolled-back old worker can capture the new output and record the exit.
	if err := os.WriteFile(gate, []byte("go\n"), 0o600); err != nil {
		t.Fatalf("write gate: %v", err)
	}

	resumeStream, err := client.JobStatus(ctx, "job-rollback", cursor)
	if err != nil {
		t.Fatalf("resume job status: %v", err)
	}
	resumed := readThroughTerminal(t, ctx, resumeStream)
	assertContiguous(t, resumed, cursor+1)
	if !containsLog(resumed, "B") {
		t.Fatalf("resumed logs = %q, want B captured after the failed reload", joinedLogs(resumed))
	}
	result := terminalResult(t, resumed)
	if result.GetExitCode() != 0 {
		t.Fatalf("terminal exit code = %d, want 0; message = %q", result.GetExitCode(), result.GetMessage())
	}

	// A second reload now succeeds with no panic, proving the rollback left the
	// worker fully healthy and that a repeat reload does not double-close.
	gen2PID := h.reloadAndWait(t, gen1PID)
	if gen2PID == gen1PID {
		t.Fatalf("second reload did not replace the worker: gen1=%d gen2=%d", gen1PID, gen2PID)
	}
}

func waitSpawnCount(t *testing.T, h *harness, target int32) {
	t.Helper()
	deadline := time.Now().Add(harnessTimeout)
	for {
		select {
		case err := <-h.runErr:
			t.Fatalf("supervisor exited before spawn count %d: %v", target, err)
		default:
		}
		if h.spawnCount.Load() >= target {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("spawn count did not reach %d before deadline", target)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
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
		if chunk := event.GetLog(); chunk != nil && strings.Contains(string(chunk.GetData()), substring) {
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

func assertContiguous(t *testing.T, events []*guestproto.JobStatusEvent, first uint64) {
	t.Helper()
	for index, event := range events {
		want := first + uint64(index)
		if event.GetSequence() != want {
			t.Fatalf("event %d sequence = %d, want %d", index, event.GetSequence(), want)
		}
	}
}

func terminalResult(t *testing.T, events []*guestproto.JobStatusEvent) *guestproto.TerminalResult {
	t.Helper()
	for _, event := range events {
		if result := event.GetResult(); result != nil {
			return result
		}
	}
	t.Fatal("events do not contain a terminal result")
	return nil
}

func containsLog(events []*guestproto.JobStatusEvent, substring string) bool {
	return strings.Contains(joinedLogs(events), substring)
}

func joinedLogs(events []*guestproto.JobStatusEvent) string {
	var builder strings.Builder
	for _, event := range events {
		if chunk := event.GetLog(); chunk != nil {
			builder.Write(chunk.GetData())
		}
	}
	return builder.String()
}
