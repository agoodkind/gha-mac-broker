package broker

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/ghapp"
	"goodkind.io/gha-mac-broker/internal/guestproto"
	"goodkind.io/gha-mac-broker/internal/tart"
)

// stubStream is a canned JobStatus stream. It replays events, then reports err.
type stubStream struct {
	events []*guestproto.JobStatusEvent
	pos    int
	err    error
}

func (s *stubStream) Receive() bool {
	if s.pos < len(s.events) {
		s.pos++
		return true
	}
	return false
}

func (s *stubStream) Msg() *guestproto.JobStatusEvent { return s.events[s.pos-1] }
func (s *stubStream) Err() error                      { return s.err }
func (s *stubStream) Close() error                    { return nil }

// stubGuest is a scripted guestConn for host-side unit tests.
type stubGuest struct {
	mu            sync.Mutex
	hello         *guestproto.HelloResponse
	helloErr      error
	runResp       *guestproto.RunJobResponse
	runErr        error
	runRequests   []*guestproto.RunJobRequest
	statusFactory []func(from uint64) (jobStatusStream, error)
	statusCalls   []uint64
	reattach      *guestproto.ReattachResponse
	reattachErr   error
	reattachBlock bool
	cancelErr     error
	cancelCalls   []string
	drainCalls    int
}

// defaultAdvertisedSlots is how many slots the stub's default Hello advertises,
// enough to cover the slot counts used across the host-side tests.
const defaultAdvertisedSlots = 4

func (g *stubGuest) Hello(_ context.Context) (*guestproto.HelloResponse, error) {
	if g.helloErr != nil {
		return nil, g.helloErr
	}
	if g.hello != nil {
		return g.hello, nil
	}
	slots := make([]*guestproto.SlotInfo, defaultAdvertisedSlots)
	for i := range slots {
		slots[i] = &guestproto.SlotInfo{Index: uint32(i)} // #nosec G115 -- small loop index.
	}
	return &guestproto.HelloResponse{ProtocolMajor: hostProtocolMajor, Slots: slots}, nil
}

func (g *stubGuest) RunJob(_ context.Context, request *guestproto.RunJobRequest) (*guestproto.RunJobResponse, error) {
	g.mu.Lock()
	g.runRequests = append(g.runRequests, request)
	g.mu.Unlock()
	if g.runErr != nil {
		return nil, g.runErr
	}
	return g.runResp, nil
}

func (g *stubGuest) JobStatus(_ context.Context, _ string, fromSequence uint64) (jobStatusStream, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.statusCalls = append(g.statusCalls, fromSequence)
	index := len(g.statusCalls) - 1
	if index >= len(g.statusFactory) {
		index = len(g.statusFactory) - 1
	}
	return g.statusFactory[index](fromSequence)
}

func (g *stubGuest) Reattach(ctx context.Context) (*guestproto.ReattachResponse, error) {
	if g.reattachBlock {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if g.reattachErr != nil {
		return nil, g.reattachErr
	}
	return g.reattach, nil
}

func (g *stubGuest) Drain(_ context.Context) (*guestproto.DrainResponse, error) {
	g.mu.Lock()
	g.drainCalls++
	g.mu.Unlock()
	return &guestproto.DrainResponse{Idle: true, ActiveExecutions: 0}, nil
}

func (g *stubGuest) CancelJob(_ context.Context, executionID string) error {
	g.mu.Lock()
	g.cancelCalls = append(g.cancelCalls, executionID)
	cancelErr := g.cancelErr
	g.mu.Unlock()
	return cancelErr
}

func (g *stubGuest) statusCallCursors() []uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]uint64(nil), g.statusCalls...)
}

func (g *stubGuest) cancelledExecutions() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.cancelCalls...)
}

func logEvent(sequence uint64, data string) *guestproto.JobStatusEvent {
	return &guestproto.JobStatusEvent{
		Sequence: sequence,
		Event: &guestproto.JobStatusEvent_Log{
			Log: &guestproto.LogChunk{Stream: guestproto.LogChunk_STDOUT, Data: []byte(data)},
		},
	}
}

func terminalEvent(sequence uint64, exitCode int32) *guestproto.JobStatusEvent {
	return &guestproto.JobStatusEvent{
		Sequence: sequence,
		Event: &guestproto.JobStatusEvent_Result{
			Result: &guestproto.TerminalResult{ExitCode: exitCode, Message: ""},
		},
	}
}

func stubBinder(t *testing.T, guest guestConn) *Binder {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	cfg := &config.Config{Labels: []string{"self-hosted"}}
	binder := New(cfg, newRunJobTestGitHubClient(t), tart.New("/nonexistent-tart"))
	binder.guestFor = func(_ context.Context, _ *WarmVM) (guestConn, error) {
		return guest, nil
	}
	return binder
}

func TestRunJobAcceptedZeroExitReturnsNil(t *testing.T) {
	guest := &stubGuest{
		runResp: &guestproto.RunJobResponse{Outcome: guestproto.RunJobResponse_ACCEPTED},
		statusFactory: []func(uint64) (jobStatusStream, error){
			func(uint64) (jobStatusStream, error) {
				return &stubStream{events: []*guestproto.JobStatusEvent{logEvent(1, "hi"), terminalEvent(2, 0)}}, nil
			},
		},
	}
	binder := stubBinder(t, guest)
	err := binder.RunJob(context.Background(), &WarmVM{Name: "warm-vm-1"}, "owner/repo", "runner-1", 0, 1, 1001, 42, time.Time{})
	if err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	if len(guest.runRequests) != 1 {
		t.Fatalf("run requests = %d, want 1", len(guest.runRequests))
	}
	request := guest.runRequests[0]
	if request.GetExecutionId() != "owner/repo#42#1001" {
		t.Fatalf("execution id = %q, want owner/repo#42#1001", request.GetExecutionId())
	}
	if request.GetSlot() != 0 {
		t.Fatalf("slot = %d, want 0", request.GetSlot())
	}
	if request.GetMeta().GetJobId() != 1001 || request.GetMeta().GetRunId() != 42 {
		t.Fatalf("meta = %+v, want job 1001 run 42", request.GetMeta())
	}
	if request.GetEnv()["GIT_TERMINAL_PROMPT"] != "0" {
		t.Fatalf("env = %v, want GIT_TERMINAL_PROMPT=0", request.GetEnv())
	}
}

func TestRunJobNonzeroExitReturnsError(t *testing.T) {
	guest := &stubGuest{
		runResp: &guestproto.RunJobResponse{Outcome: guestproto.RunJobResponse_ACCEPTED},
		statusFactory: []func(uint64) (jobStatusStream, error){
			func(uint64) (jobStatusStream, error) {
				return &stubStream{events: []*guestproto.JobStatusEvent{terminalEvent(1, 7)}}, nil
			},
		},
	}
	binder := stubBinder(t, guest)
	err := binder.RunJob(context.Background(), &WarmVM{Name: "warm-vm-1"}, "owner/repo", "runner-1", 0, 1, 1001, 42, time.Time{})
	if err == nil {
		t.Fatal("RunJob error = nil, want nonzero-exit error")
	}
	if !errors.Is(err, ErrJobTerminal) {
		t.Fatalf("RunJob error = %v, want wrapped ErrJobTerminal", err)
	}
}

func TestVerifySlotInventoryRejectsMissingIndex(t *testing.T) {
	binder := New(&config.Config{}, nil, tart.New("/nonexistent-tart"))
	cases := map[string][]*guestproto.SlotInfo{
		"sparse":    {{Index: 1}, {Index: 2}},
		"duplicate": {{Index: 0}, {Index: 0}},
	}
	for name, slots := range cases {
		t.Run(name, func(t *testing.T) {
			guest := &stubGuest{hello: &guestproto.HelloResponse{ProtocolMajor: hostProtocolMajor, Slots: slots}}
			if err := binder.verifySlotInventory(context.Background(), guest, "vm-x", 2); err == nil {
				t.Fatalf("verifySlotInventory(%+v) = nil, want rejection for a missing configured index", slots)
			}
		})
	}
}

func TestVerifySlotInventoryAcceptsAllIndicesPresent(t *testing.T) {
	binder := New(&config.Config{}, nil, tart.New("/nonexistent-tart"))
	guest := &stubGuest{hello: &guestproto.HelloResponse{
		ProtocolMajor: hostProtocolMajor,
		Slots:         []*guestproto.SlotInfo{{Index: 0}, {Index: 1}},
	}}
	if err := binder.verifySlotInventory(context.Background(), guest, "vm-x", 2); err != nil {
		t.Fatalf("verifySlotInventory = %v, want nil when 0..1 are present", err)
	}
}

func TestRunJobConflictReturnsErrorWithoutDraining(t *testing.T) {
	guest := &stubGuest{
		runResp: &guestproto.RunJobResponse{Outcome: guestproto.RunJobResponse_CONFLICT},
	}
	binder := stubBinder(t, guest)
	err := binder.RunJob(context.Background(), &WarmVM{Name: "warm-vm-1"}, "owner/repo", "runner-1", 0, 1, 1001, 42, time.Time{})
	if err == nil {
		t.Fatal("RunJob error = nil, want conflict error")
	}
	if len(guest.statusCallCursors()) != 0 {
		t.Fatalf("status calls = %v, want none on conflict", guest.statusCallCursors())
	}
}

func TestDrainJobReconnectsFromCursorAfterMidStreamDrop(t *testing.T) {
	guest := &stubGuest{
		statusFactory: []func(uint64) (jobStatusStream, error){
			func(uint64) (jobStatusStream, error) {
				return &stubStream{
					events: []*guestproto.JobStatusEvent{logEvent(1, "a"), logEvent(2, "b")},
					err:    errors.New("stream dropped"),
				}, nil
			},
			func(uint64) (jobStatusStream, error) {
				return &stubStream{events: []*guestproto.JobStatusEvent{terminalEvent(3, 0)}}, nil
			},
		},
	}
	binder := stubBinder(t, guest)
	err := binder.drainJob(context.Background(), guest, "owner/repo#42#1001", 0, &WarmVM{Name: "warm-vm-1"}, 0, 1)
	if err != nil {
		t.Fatalf("drainJob: %v", err)
	}
	cursors := guest.statusCallCursors()
	if len(cursors) != 2 {
		t.Fatalf("status calls = %v, want two (initial + reconnect)", cursors)
	}
	if cursors[0] != 0 || cursors[1] != 2 {
		t.Fatalf("reconnect cursors = %v, want [0 2]", cursors)
	}
}

func TestDrainJobExecutionNotFoundFreesSlot(t *testing.T) {
	guest := &stubGuest{
		statusFactory: []func(uint64) (jobStatusStream, error){
			func(uint64) (jobStatusStream, error) {
				return nil, connect.NewError(connect.CodeNotFound, errors.New("execution not found"))
			},
		},
	}
	binder := stubBinder(t, guest)
	err := binder.drainJob(context.Background(), guest, "owner/repo#42#1001", 0, &WarmVM{Name: "warm-vm-1"}, 0, 1)
	if err != nil {
		t.Fatalf("drainJob = %v, want nil for expired execution", err)
	}
}

func TestDrainJobRecycleCauseCancelsThenDrainsToTerminal(t *testing.T) {
	guest := &stubGuest{
		statusFactory: []func(uint64) (jobStatusStream, error){
			func(uint64) (jobStatusStream, error) {
				return &stubStream{events: []*guestproto.JobStatusEvent{terminalEvent(5, 0)}}, nil
			},
		},
	}
	binder := stubBinder(t, guest)
	recycleCause := errors.New("recycle slot")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(recycleCause)
	err := binder.drainJob(ctx, guest, "owner/repo#42#1001", 0, &WarmVM{Name: "warm-vm-1"}, 0, 1)
	if err != nil {
		t.Fatalf("drainJob = %v, want nil after cancel-drain to zero exit", err)
	}
	if cancels := guest.cancelledExecutions(); len(cancels) != 1 || cancels[0] != "owner/repo#42#1001" {
		t.Fatalf("cancel calls = %v, want [owner/repo#42#1001]", cancels)
	}
}

func TestDrainJobBoundsTeardownWhenVMUnreachable(t *testing.T) {
	restore := drainTimeout
	drainTimeout = 100 * time.Millisecond
	t.Cleanup(func() { drainTimeout = restore })

	// A dead/unreachable VM: CancelJob and every JobStatus reconnect fail with dial
	// errors (never a terminal, never NotFound). Without a bound the teardown drain
	// would spin forever and wedge the slot.
	guest := &stubGuest{
		cancelErr: errors.New("dial tcp: connection refused"),
		statusFactory: []func(uint64) (jobStatusStream, error){
			func(uint64) (jobStatusStream, error) {
				return nil, errors.New("dial tcp: connection refused")
			},
		},
	}
	binder := stubBinder(t, guest)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("recycle slot"))

	done := make(chan error, 1)
	go func() {
		done <- binder.drainJob(ctx, guest, "owner/repo#42#1001", 0, &WarmVM{Name: "warm-vm-1"}, 0, 1)
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("drainJob = nil, want a bounded teardown-timeout error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drainJob did not return; teardown drain is unbounded")
	}
	if cancels := guest.cancelledExecutions(); len(cancels) != 1 || cancels[0] != "owner/repo#42#1001" {
		t.Fatalf("cancel calls = %v, want one CancelJob attempt", cancels)
	}
}

func TestDrainJobPlainCancelDetachesWithoutCancelingJob(t *testing.T) {
	guest := &stubGuest{
		statusFactory: []func(uint64) (jobStatusStream, error){
			func(uint64) (jobStatusStream, error) {
				return &stubStream{events: []*guestproto.JobStatusEvent{terminalEvent(1, 0)}}, nil
			},
		},
	}
	binder := stubBinder(t, guest)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := binder.drainJob(ctx, guest, "owner/repo#42#1001", 0, &WarmVM{Name: "warm-vm-1"}, 0, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("drainJob = %v, want context.Canceled detach", err)
	}
	if cancels := guest.cancelledExecutions(); len(cancels) != 0 {
		t.Fatalf("cancel calls = %v, want none on plain-cancel detach", cancels)
	}
	if calls := guest.statusCallCursors(); len(calls) != 0 {
		t.Fatalf("status calls = %v, want none on plain-cancel detach", calls)
	}
}

func TestRunningSlotsReportsRunningExecutions(t *testing.T) {
	guest := &stubGuest{
		reattach: &guestproto.ReattachResponse{
			Executions: []*guestproto.ExecutionState{
				{ExecutionId: "owner/repo#42#1001", Slot: 0, Running: true, LastSequence: 9},
				{ExecutionId: "owner/repo#43#1002", Slot: 1, Running: false, LastSequence: 3},
			},
		},
	}
	binder := stubBinder(t, guest)
	running, err := binder.RunningSlots(context.Background(), &WarmVM{Name: "warm-vm-1"})
	if err != nil {
		t.Fatalf("RunningSlots: %v", err)
	}
	if !running[0] {
		t.Fatalf("running = %v, want slot 0 running", running)
	}
	if running[1] {
		t.Fatalf("running = %v, want slot 1 not running", running)
	}
}

func TestResumeJobDrainsFromCursor(t *testing.T) {
	guest := &stubGuest{
		statusFactory: []func(uint64) (jobStatusStream, error){
			func(uint64) (jobStatusStream, error) {
				return &stubStream{events: []*guestproto.JobStatusEvent{terminalEvent(8, 0)}}, nil
			},
		},
	}
	binder := stubBinder(t, guest)
	err := binder.ResumeJob(context.Background(), &WarmVM{Name: "warm-vm-1"}, "owner/repo#42#1001", 7, 0, 1)
	if err != nil {
		t.Fatalf("ResumeJob: %v", err)
	}
	cursors := guest.statusCallCursors()
	if len(cursors) != 1 || cursors[0] != 7 {
		t.Fatalf("resume cursors = %v, want [7]", cursors)
	}
}

func TestAdoptMarksRunningExecutionSlotBusyWithResumeCursor(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-tart")
	writeListOnlyFakeTart(t, bin, `[{"Name":"pool-a-busy","State":"running"}]`)

	guest := &stubGuest{
		reattach: &guestproto.ReattachResponse{
			Executions: []*guestproto.ExecutionState{
				{
					ExecutionId:  "owner/repo#42#1001",
					Slot:         0,
					Running:      true,
					LastSequence: 12,
					Meta:         &guestproto.JobMeta{Repo: "owner/repo", JobId: 1001, RunId: 42},
				},
			},
		},
	}
	cfg := &config.Config{Tart: config.TartConfig{VMNamePrefix: "pool"}}
	binder := New(cfg, nil, tart.New(bin))
	binder.guestFor = func(_ context.Context, _ *WarmVM) (guestConn, error) {
		return guest, nil
	}
	adopted, err := binder.Adopt(context.Background(), "image-a", 1, 1)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	if len(adopted) != 1 {
		t.Fatalf("adopted = %+v, want one busy VM", adopted)
	}
	if adopted[0].VM.Name != "pool-a-busy" {
		t.Fatalf("adopted vm = %q, want pool-a-busy", adopted[0].VM.Name)
	}
	if len(adopted[0].Slots) != 1 {
		t.Fatalf("adopted slots = %+v, want one busy slot", adopted[0].Slots)
	}
	slot := adopted[0].Slots[0]
	if slot.ExecutionID != "owner/repo#42#1001" || slot.ResumeCursor != 12 || slot.RunID != 42 || slot.JobID != 1001 {
		t.Fatalf("adopted slot = %+v, want execution owner/repo#42#1001 cursor 12 job 1001 run 42", slot)
	}
	if !slot.ObservedActive {
		t.Fatalf("adopted slot observed active = false, want true")
	}
}

func TestAdoptRejectsGuestAdvertisingTooFewSlots(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-tart")
	writeListOnlyFakeTart(t, bin, `[{"Name":"pool-a-thin","State":"running"}]`)

	// The guest advertises a single slot, but the pool wants two.
	guest := &stubGuest{
		hello: &guestproto.HelloResponse{
			ProtocolMajor: hostProtocolMajor,
			Slots:         []*guestproto.SlotInfo{{Index: 0}},
		},
		reattach: &guestproto.ReattachResponse{},
	}
	cfg := &config.Config{Tart: config.TartConfig{VMNamePrefix: "pool"}}
	binder := New(cfg, nil, tart.New(bin))
	binder.guestFor = func(_ context.Context, _ *WarmVM) (guestConn, error) {
		return guest, nil
	}
	adopted, err := binder.Adopt(context.Background(), "image-a", 2, 2)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	if len(adopted) != 0 {
		t.Fatalf("adopted = %+v, want none (guest advertises too few slots)", adopted)
	}
}

func TestAdoptSkipsHungReattachWithinBound(t *testing.T) {
	restore := checkAliveTimeout
	checkAliveTimeout = 100 * time.Millisecond
	t.Cleanup(func() { checkAliveTimeout = restore })

	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-tart")
	writeListOnlyFakeTart(t, bin, `[{"Name":"pool-a-hung","State":"running"}]`)

	guest := &stubGuest{reattachBlock: true}
	cfg := &config.Config{Tart: config.TartConfig{VMNamePrefix: "pool"}}
	binder := New(cfg, nil, tart.New(bin))
	binder.guestFor = func(_ context.Context, _ *WarmVM) (guestConn, error) {
		return guest, nil
	}

	done := make(chan struct{})
	var adopted []AdoptedVM
	go func() {
		adopted, _ = binder.Adopt(context.Background(), "image-a", 1, 1)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Adopt did not return; adoption Reattach is unbounded")
	}
	if len(adopted) != 0 {
		t.Fatalf("adopted = %+v, want none (hung VM skipped after bounded Reattach)", adopted)
	}
}

func writeListOnlyFakeTart(t *testing.T, bin string, listJSON string) {
	t.Helper()
	script := "#!/usr/bin/env bash\nset -euo pipefail\nif [[ \"$1\" == \"list\" ]]; then\n  printf '%s' '" + listJSON + "'\n  exit 0\nfi\nexit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake tart: %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func newRunJobTestGitHubClient(t *testing.T) *ghapp.Client {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/owner/repo/installation":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":999}`))
		case r.Method == http.MethodPost && r.URL.Path == "/app/installations/999/access_tokens":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"token":"ghs_installationtoken"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/repos/owner/repo/actions/runners/generate-jitconfig":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"encoded_jit_config":"encoded-jit","runner":{"id":7,"name":"runner-1"}}`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected", http.StatusNotFound)
		}
	})
	httpClient := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, r)
			return recorder.Result(), nil
		}),
	}
	client, err := ghapp.New("12345", testPrivateKeyPEM(t), ghapp.WithHTTPClient(httpClient))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return client
}

func testPrivateKeyPEM(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der := x509.MarshalPKCS1PrivateKey(key)
	block := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: der}
	return pem.EncodeToMemory(block)
}

func TestReapBootCommandWaitsForExitedProcess(t *testing.T) {
	binder := New(nil, nil, nil)
	ctx := context.Background()
	cmd := exec.CommandContext(ctx, "true")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start command: %v", err)
	}

	done := binder.reapBootCommand(context.Background(), "vm-reap", cmd)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("boot command reaper did not finish")
	}
}
