//go:build unix

package guestsupervisor

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"goodkind.io/gha-mac-broker/internal/guestexec"
)

const controlTestTimeout = 5 * time.Second

// startControlOnly brings up just the control socket and its accept loop, so a
// test can drive the control protocol without spawning a real worker. It returns
// the socket path.
// shortSocketPath returns a unix socket path short enough for the macOS sun_path
// limit, which a test-name-based temp dir would exceed.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "gs")
	if err != nil {
		t.Fatalf("make socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "c.sock")
}

func startControlOnly(t *testing.T, supervisor *Supervisor) string {
	t.Helper()
	socketPath := shortSocketPath(t)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatalf("listen control socket: %v", err)
	}
	supervisor.controlSocket = listener
	supervisor.opts.ControlSocketPath = socketPath
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = supervisor.serveControl(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
	})
	return socketPath
}

// TestPollRedeliversUnackedExitAcrossGenerations proves the supervisor buffers a
// runner exit until a worker acks it and redelivers it to whichever generation
// is current. This is the mechanism that carries a runner exit that lands during
// a reload to the replacement worker after it restores.
func TestPollRedeliversUnackedExitAcrossGenerations(t *testing.T) {
	supervisor := New(Options{SlotCount: 1})
	socketPath := startControlOnly(t, supervisor)

	const pid = 90210
	supervisor.mu.Lock()
	supervisor.state = StateSteady
	supervisor.currentGeneration = 1
	supervisor.unacked[pid] = guestexec.ExitReport{PID: pid, ExitCode: 5, Message: "boom"}
	supervisor.mu.Unlock()

	firstPoll, err := PollExits(socketPath, 1, controlTestTimeout)
	if err != nil {
		t.Fatalf("first poll: %v", err)
	}
	if len(firstPoll) != 1 || firstPoll[0].PID != pid || firstPoll[0].ExitCode != 5 {
		t.Fatalf("first poll = %+v, want one exit for pid %d code 5", firstPoll, pid)
	}

	// A second poll from the same generation without an ack redelivers the exit.
	redeliver, err := PollExits(socketPath, 1, controlTestTimeout)
	if err != nil {
		t.Fatalf("redeliver poll: %v", err)
	}
	if len(redeliver) != 1 || redeliver[0].PID != pid {
		t.Fatalf("redeliver poll = %+v, want the unacked exit again", redeliver)
	}

	// A new generation attaching (currentGeneration bumps) still sees the exit.
	supervisor.mu.Lock()
	supervisor.currentGeneration = 2
	supervisor.mu.Unlock()
	acrossSwap, err := PollExits(socketPath, 2, controlTestTimeout)
	if err != nil {
		t.Fatalf("across-swap poll: %v", err)
	}
	if len(acrossSwap) != 1 || acrossSwap[0].PID != pid || acrossSwap[0].Message != "boom" {
		t.Fatalf("across-swap poll = %+v, want the redelivered exit for the new generation", acrossSwap)
	}

	if err := AckExit(socketPath, pid); err != nil {
		t.Fatalf("ack exit: %v", err)
	}
	afterAck, err := PollExits(socketPath, 2, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("post-ack poll: %v", err)
	}
	if len(afterAck) != 0 {
		t.Fatalf("post-ack poll = %+v, want empty after ack", afterAck)
	}
}

// TestPollFromStaleGenerationReturnsEmpty proves a superseded worker's poll
// returns nothing once a newer generation is current, so a draining old worker
// cannot keep consuming and acking exits meant for the replacement.
func TestPollFromStaleGenerationReturnsEmpty(t *testing.T) {
	supervisor := New(Options{SlotCount: 1})
	socketPath := startControlOnly(t, supervisor)

	supervisor.mu.Lock()
	supervisor.state = StateSteady
	supervisor.currentGeneration = 2
	supervisor.unacked[7] = guestexec.ExitReport{PID: 7, ExitCode: 0}
	supervisor.mu.Unlock()

	stale, err := PollExits(socketPath, 1, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("stale poll: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("stale generation poll = %+v, want empty", stale)
	}
}

// TestRejectReplaceWhenNotSteady proves a second reload arriving while a swap is
// already in flight is rejected, so only one worker replacement runs at a time.
func TestRejectReplaceWhenNotSteady(t *testing.T) {
	supervisor := New(Options{SlotCount: 1})
	socketPath := startControlOnly(t, supervisor)

	supervisor.mu.Lock()
	supervisor.state = StateReplacing
	supervisor.currentGeneration = 1
	supervisor.mu.Unlock()

	snapshotFile, readyWrite := replacePayload(t)
	defer func() { _ = snapshotFile.Close() }()
	defer func() { _ = readyWrite.Close() }()

	_, err := RequestReplacement(socketPath, "/bin/true", []string{"/bin/true"}, os.Environ(), snapshotFile, readyWrite)
	if err == nil {
		t.Fatal("replace_worker while replacing succeeded, want rejection")
	}
}

// TestStartChildForksWaitsAndBuffersExit proves the supervisor forks a runner,
// hands its pipe read ends to the caller, waits it, and buffers the observed exit
// code for a poll.
func TestStartChildForksWaitsAndBuffersExit(t *testing.T) {
	supervisor := New(Options{SlotCount: 1})
	socketPath := startControlOnly(t, supervisor)
	supervisor.mu.Lock()
	supervisor.state = StateSteady
	supervisor.currentGeneration = 1
	supervisor.mu.Unlock()

	spec := guestexec.ExecSpec{
		ExecutionID: "stub",
		Slot:        0,
		Command:     "/bin/sh",
		Args:        []string{"-c", "exit 3"},
	}
	launched, err := StartChild(socketPath, spec)
	if err != nil {
		t.Fatalf("start child: %v", err)
	}
	defer func() { _ = launched.Stdout.Close() }()
	defer func() { _ = launched.Stderr.Close() }()
	if launched.PID <= 0 {
		t.Fatalf("start child pid = %d, want positive", launched.PID)
	}

	deadline := time.Now().Add(controlTestTimeout)
	for {
		reports, pollErr := PollExits(socketPath, 1, 500*time.Millisecond)
		if pollErr != nil {
			t.Fatalf("poll exits: %v", pollErr)
		}
		if len(reports) == 1 && reports[0].PID == launched.PID {
			if reports[0].ExitCode != 3 {
				t.Fatalf("runner exit code = %d, want 3", reports[0].ExitCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("runner exit was not buffered before deadline; last poll = %+v", reports)
		}
	}
	if err := AckExit(socketPath, launched.PID); err != nil {
		t.Fatalf("ack exit: %v", err)
	}
}

// TestAssembleEnvCarriesGoldenFingerprint proves the supervisor passes the baked
// golden fingerprint to each worker through the environment, so the worker can
// report it via Hello.
func TestAssembleEnvCarriesGoldenFingerprint(t *testing.T) {
	supervisor := New(Options{GoldenFingerprint: "fp-abc123", SlotCount: 1})
	env, err := supervisor.assembleEnv(nil, 1, firstWorkerFDBase, nil, -1, -1)
	if err != nil {
		t.Fatalf("assembleEnv: %v", err)
	}
	want := EnvGoldenFingerprint + "=fp-abc123"
	found := false
	for _, entry := range env {
		if entry == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("worker env %v missing %q", env, want)
	}
}

func replacePayload(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	snapshotFile, err := os.CreateTemp(t.TempDir(), "snap-*")
	if err != nil {
		t.Fatalf("create snapshot temp: %v", err)
	}
	readRead, readWrite, err := os.Pipe()
	if err != nil {
		t.Fatalf("create readiness pipe: %v", err)
	}
	t.Cleanup(func() { _ = readRead.Close() })
	return snapshotFile, readWrite
}
