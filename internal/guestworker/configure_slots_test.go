//go:build unix

package guestworker_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"goodkind.io/gha-mac-broker/internal/guestclient"
	"goodkind.io/gha-mac-broker/internal/guestproto"
	"goodkind.io/gha-mac-broker/internal/guestsupervisor"
)

// startHarnessSlots brings up a supervisor and its first re-exec'd worker with a
// given boot slot count, so the ConfigureSlots tests can start from one or two
// slots. It mirrors startHarness, which is fixed at a single slot.
func startHarnessSlots(t *testing.T, slots uint32) *harness {
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
		SlotCount:         slots,
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

// waitHelloSlots polls Hello until the advertised inventory covers every wanted
// index, proving the reconfigured worker generation is current and serving.
func waitHelloSlots(t *testing.T, addr string, want uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), harnessTimeout)
	defer cancel()
	deadline := time.Now().Add(harnessTimeout)
	for {
		client := guestclient.New(ctx, addr, harnessToken)
		hello, err := client.Hello(ctx)
		if err == nil && slotsCover(hello.GetSlots(), want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("guest did not advertise %d slots before deadline; last err = %v", want, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// helloAdvertises reports whether a single Hello currently shows exactly count
// slots. It is non-fatal so a poll loop can retry.
func helloAdvertises(addr string, count uint32) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client := guestclient.New(ctx, addr, harnessToken)
	hello, err := client.Hello(ctx)
	if err != nil {
		return false
	}
	return slotsCover(hello.GetSlots(), count)
}

func slotsCover(slots []*guestproto.SlotInfo, count uint32) bool {
	if uint32(len(slots)) != count {
		return false
	}
	seen := make(map[uint32]struct{}, len(slots))
	for _, slot := range slots {
		seen[slot.GetIndex()] = struct{}{}
	}
	for index := uint32(0); index < count; index++ {
		if _, ok := seen[index]; !ok {
			return false
		}
	}
	return true
}

// TestConfigureSlotsGrowsAndHelloAdvertisesN proves a fresh single-slot guest
// grows to the requested count over the Connect RPC and then advertises every
// configured index through Hello, so the host slot-inventory gate passes.
func TestConfigureSlotsGrowsAndHelloAdvertisesN(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), harnessTimeout)
	defer cancel()
	h := startHarnessSlots(t, 1)

	client := guestclient.New(ctx, h.addr, harnessToken)
	response, err := client.ConfigureSlots(ctx, 2)
	if err != nil {
		t.Fatalf("ConfigureSlots(2): %v", err)
	}
	if response.GetSlotCount() != 2 {
		t.Fatalf("applied slot count = %d, want 2", response.GetSlotCount())
	}
	waitHelloSlots(t, h.addr, 2)
}

// TestConfigureSlotsNoOpWhenEqual proves a request for the current slot count is
// idempotent: no worker replacement runs, so the guest keeps serving without a
// swap.
func TestConfigureSlotsNoOpWhenEqual(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), harnessTimeout)
	defer cancel()
	h := startHarnessSlots(t, 1)
	spawnsBefore := h.spawnCount.Load()

	client := guestclient.New(ctx, h.addr, harnessToken)
	response, err := client.ConfigureSlots(ctx, 1)
	if err != nil {
		t.Fatalf("ConfigureSlots(1): %v", err)
	}
	if response.GetSlotCount() != 1 {
		t.Fatalf("applied slot count = %d, want 1", response.GetSlotCount())
	}
	// A no-op must not spawn a replacement worker; give any errant reload time to
	// land, then confirm the spawn count is unchanged.
	time.Sleep(300 * time.Millisecond)
	if got := h.spawnCount.Load(); got != spawnsBefore {
		t.Fatalf("spawn count = %d, want %d (a no-op reconfigure must not replace the worker)", got, spawnsBefore)
	}
}

// TestConfigureSlotsGrowsWhileJobRunning proves a grow while a job runs preserves
// the running execution across the worker replacement and then exposes the new
// slots: the gated runner keeps running through the ConfigureSlots-triggered
// reload, resumes its stream contiguously, and reaches its terminal.
func TestConfigureSlotsGrowsWhileJobRunning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), harnessTimeout)
	defer cancel()
	h := startHarnessSlots(t, 1)

	gate := filepath.Join(t.TempDir(), "gate")
	script := fmt.Sprintf("echo A; while [ ! -f '%s' ]; do sleep 0.05; done; echo B", gate)

	client := guestclient.New(ctx, h.addr, harnessToken)
	runResponse, err := client.RunJob(ctx, &guestproto.RunJobRequest{
		ExecutionId: "grow-job",
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
	firstStream, err := client.JobStatus(firstStreamCtx, "grow-job", 0)
	if err != nil {
		t.Fatalf("job status: %v", err)
	}
	beforeGrow := readUntilLog(t, firstStreamCtx, firstStream, "A")
	cursor := beforeGrow[len(beforeGrow)-1].GetSequence()
	cancelFirst()

	if _, err := client.ConfigureSlots(ctx, 2); err != nil {
		t.Fatalf("ConfigureSlots(2) while job running: %v", err)
	}
	// The reload is asynchronous; wait until the replacement is current and
	// advertising both slots before releasing the gated runner, so its remaining
	// output and exit are observed by the grown worker generation.
	waitHelloSlots(t, h.addr, 2)

	if err := os.WriteFile(gate, []byte("go\n"), 0o600); err != nil {
		t.Fatalf("write gate: %v", err)
	}

	resumeClient := guestclient.New(ctx, h.addr, harnessToken)
	resumeStream, err := resumeClient.JobStatus(ctx, "grow-job", cursor)
	if err != nil {
		t.Fatalf("resume job status: %v", err)
	}
	resumed := readThroughTerminal(t, ctx, resumeStream)
	assertContiguous(t, resumed, cursor+1)
	if !containsLog(resumed, "B") {
		t.Fatalf("resumed logs = %q, want B captured after the grow", joinedLogs(resumed))
	}
	result := terminalResult(t, resumed)
	if result.GetExitCode() != 0 {
		t.Fatalf("terminal exit code = %d, want 0; message = %q", result.GetExitCode(), result.GetMessage())
	}
}

// TestConfigureSlotsRedrivesAfterFailedReload proves the supervisor never
// durably reports a slot count the live worker is not serving. When the reload
// ConfigureSlots triggers fails and the worker rolls back, the served count stays
// at the old value, and a subsequent ConfigureSlots re-drives the reload rather
// than short-circuiting as a no-op, healing to the requested count.
func TestConfigureSlotsRedrivesAfterFailedReload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), harnessTimeout)
	defer cancel()
	h := startHarnessSlots(t, 1)
	gen1PID := h.supervisor.CurrentWorkerPID()

	// Arm the next worker spawn to crash before it can attach, so the reload the
	// first ConfigureSlots triggers fails and the worker rolls back to one slot.
	spawnsBefore := h.spawnCount.Load()
	h.failNext.Store(true)

	firstClient := guestclient.New(ctx, h.addr, harnessToken)
	if _, err := firstClient.ConfigureSlots(ctx, 2); err != nil {
		t.Fatalf("first ConfigureSlots(2): %v", err)
	}

	// The failed replacement is spawned, then the old worker rolls back and keeps
	// serving exactly one slot; the served count did not durably jump to two.
	waitSpawnCount(t, h, spawnsBefore+1)
	waitHelloSlots(t, h.addr, 1)
	if pid := h.supervisor.CurrentWorkerPID(); pid != gen1PID {
		t.Fatalf("current worker changed after a failed reload: was %d, now %d", gen1PID, pid)
	}

	// A retry must re-drive the reload rather than no-op, because the compare is
	// against the served count (still one), not the stored desired (two). Loop
	// because the rollback may still be settling to steady when the first retry
	// fires. Use a fresh client and tolerate a transient error each attempt, so an
	// RPC that lands on a draining generation redials the current worker.
	deadline := time.Now().Add(harnessTimeout)
	healed := false
	for time.Now().Before(deadline) {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, 3*time.Second)
		retryClient := guestclient.New(attemptCtx, h.addr, harnessToken)
		_, err := retryClient.ConfigureSlots(attemptCtx, 2)
		attemptCancel()
		if err == nil && helloAdvertises(h.addr, 2) {
			healed = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !healed {
		t.Fatal("retry did not re-drive the reload to two slots before deadline")
	}

	// The healed guest is a fresh generation serving both slots.
	if pid := h.supervisor.CurrentWorkerPID(); pid == gen1PID {
		t.Fatalf("worker did not replace on the re-drive: still %d", pid)
	}
	waitHelloSlots(t, h.addr, 2)
}

// TestConfigureSlotsShrinkBelowBusyRejected proves a shrink below a running slot
// is rejected: the guest keeps its current slot count and the running job is
// never orphaned.
func TestConfigureSlotsShrinkBelowBusyRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), harnessTimeout)
	defer cancel()
	h := startHarnessSlots(t, 2)

	gate := filepath.Join(t.TempDir(), "gate")
	script := fmt.Sprintf("echo A; while [ ! -f '%s' ]; do sleep 0.05; done; echo B", gate)

	client := guestclient.New(ctx, h.addr, harnessToken)
	runResponse, err := client.RunJob(ctx, &guestproto.RunJobRequest{
		ExecutionId: "busy-slot-1",
		Slot:        1,
		JitConfig:   script,
	})
	if err != nil {
		t.Fatalf("run job on slot 1: %v", err)
	}
	if runResponse.GetOutcome() != guestproto.RunJobResponse_ACCEPTED {
		t.Fatalf("run job outcome = %v, want ACCEPTED", runResponse.GetOutcome())
	}

	// Wait until the runner has actually started under the supervisor, so the
	// shrink check sees slot 1 as busy.
	firstStreamCtx, cancelFirst := context.WithCancel(ctx)
	firstStream, err := client.JobStatus(firstStreamCtx, "busy-slot-1", 0)
	if err != nil {
		t.Fatalf("job status: %v", err)
	}
	readUntilLog(t, firstStreamCtx, firstStream, "A")
	cancelFirst()

	if _, err := client.ConfigureSlots(ctx, 1); err == nil {
		t.Fatal("ConfigureSlots(1) with slot 1 busy = nil, want rejection")
	}

	// The guest kept both slots; a fresh Hello still advertises 0 and 1.
	hello, err := client.Hello(ctx)
	if err != nil {
		t.Fatalf("hello after rejected shrink: %v", err)
	}
	if !slotsCover(hello.GetSlots(), 2) {
		t.Fatalf("hello slots = %+v, want both 0 and 1 after a rejected shrink", hello.GetSlots())
	}

	// Release the gated runner so it exits cleanly before teardown.
	if err := os.WriteFile(gate, []byte("go\n"), 0o600); err != nil {
		t.Fatalf("write gate: %v", err)
	}
}
