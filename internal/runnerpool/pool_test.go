package runnerpool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
	"goodkind.io/gha-mac-broker/internal/ghapp"
)

type warmCall struct {
	Image     string
	ID        string
	SlotCount int
}

type adoptCall struct {
	Image     string
	SlotCount int
	Limit     int
}

type fakeWarmer struct {
	mu       sync.Mutex
	calls    []warmCall
	torn     []string
	alive    map[string]bool
	checkErr map[string]error
	adopted  []broker.AdoptedVM
	adopts   []adoptCall
	adoptErr error
}

func newFakeWarmer() *fakeWarmer {
	return &fakeWarmer{
		mu:       sync.Mutex{},
		calls:    nil,
		torn:     nil,
		alive:    make(map[string]bool),
		checkErr: make(map[string]error),
		adopted:  nil,
		adopts:   nil,
		adoptErr: nil,
	}
}

func (w *fakeWarmer) Warm(_ context.Context, image string, id string, slotCount int) (*broker.WarmVM, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, warmCall{Image: image, ID: id, SlotCount: slotCount})
	name := "vm-" + id
	w.alive[name] = true
	return &broker.WarmVM{Name: name, Image: image}, nil
}

func (w *fakeWarmer) Teardown(_ context.Context, vm *broker.WarmVM) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.torn = append(w.torn, vm.Name)
	w.alive[vm.Name] = false
}

func (w *fakeWarmer) CheckAlive(_ context.Context, vm *broker.WarmVM) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkErr[vm.Name]; err != nil {
		return err
	}
	if w.alive[vm.Name] {
		return nil
	}
	return errors.New("vm is dead")
}

func (w *fakeWarmer) Adopt(_ context.Context, image string, slotCount int, limit int) ([]broker.AdoptedVM, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.adopts = append(w.adopts, adoptCall{Image: image, SlotCount: slotCount, Limit: limit})
	if w.adoptErr != nil {
		return nil, w.adoptErr
	}
	adopted := append([]broker.AdoptedVM(nil), w.adopted...)
	if limit >= 0 && len(adopted) > limit {
		adopted = adopted[:limit]
	}
	for _, adoptedVM := range adopted {
		if adoptedVM.VM != nil {
			w.alive[adoptedVM.VM.Name] = true
		}
	}
	return adopted, nil
}

func (w *fakeWarmer) WarmNames() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	names := make([]string, 0, len(w.calls))
	for _, call := range w.calls {
		names = append(names, "vm-"+call.ID)
	}
	return names
}

func (w *fakeWarmer) WarmSlotCounts() []int {
	w.mu.Lock()
	defer w.mu.Unlock()
	slotCounts := make([]int, 0, len(w.calls))
	for _, call := range w.calls {
		slotCounts = append(slotCounts, call.SlotCount)
	}
	return slotCounts
}

func (w *fakeWarmer) TornNames() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.torn...)
}

func (w *fakeWarmer) SetCheckError(vmName string, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.checkErr[vmName] = err
}

func (w *fakeWarmer) SetAdopted(adopted []broker.AdoptedVM) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.adopted = append([]broker.AdoptedVM(nil), adopted...)
}

func (w *fakeWarmer) AdoptCalls() []adoptCall {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]adoptCall(nil), w.adopts...)
}

type runCall struct {
	Repo        string
	RunnerName  string
	VMName      string
	SlotIndex   int
	SlotCount   int
	ExecutionID string
	Resume      bool
	Cursor      uint64
}

type fakeRunner struct {
	mu        sync.Mutex
	calls     []runCall
	started   chan runCall
	release   chan struct{}
	runErr    error
	panicErr  error
	active    int
	maxActive int
}

func newFakeRunner(buffer int) *fakeRunner {
	return &fakeRunner{
		mu:        sync.Mutex{},
		calls:     nil,
		started:   make(chan runCall, buffer),
		release:   nil,
		runErr:    nil,
		panicErr:  nil,
		active:    0,
		maxActive: 0,
	}
}

func newBlockingRunner(buffer int) *fakeRunner {
	runner := newFakeRunner(buffer)
	runner.release = make(chan struct{}, buffer)
	return runner
}

func (r *fakeRunner) run(ctx context.Context, call runCall) error {
	r.mu.Lock()
	r.calls = append(r.calls, call)
	r.active++
	if r.active > r.maxActive {
		r.maxActive = r.active
	}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.active--
		r.mu.Unlock()
	}()
	r.started <- call
	if r.panicErr != nil {
		panic(r.panicErr)
	}
	if r.release != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.release:
		}
	}
	return r.runErr
}

func (r *fakeRunner) RunJob(ctx context.Context, vm *broker.WarmVM, repo string, runnerName string, slotIndex int, slotCount int, _ int64, _ int64, _ time.Time) error {
	return r.run(ctx, runCall{
		Repo:       repo,
		RunnerName: runnerName,
		VMName:     vm.Name,
		SlotIndex:  slotIndex,
		SlotCount:  slotCount,
	})
}

func (r *fakeRunner) ResumeJob(ctx context.Context, vm *broker.WarmVM, executionID string, fromCursor uint64, slotIndex int, slotCount int) error {
	return r.run(ctx, runCall{
		VMName:      vm.Name,
		SlotIndex:   slotIndex,
		SlotCount:   slotCount,
		ExecutionID: executionID,
		Resume:      true,
		Cursor:      fromCursor,
	})
}

func (r *fakeRunner) Release() {
	r.release <- struct{}{}
}

func (r *fakeRunner) Calls() []runCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]runCall(nil), r.calls...)
}

func (r *fakeRunner) MaxActive() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.maxActive
}

func (r *fakeRunner) Active() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active
}

type fakeGitHub struct {
	mu             sync.Mutex
	installedRepos []string
	runners        map[string][]ghapp.Runner
	calls          []string
	installedCalls int
	err            error
}

func newFakeGitHub() *fakeGitHub {
	return &fakeGitHub{
		mu:             sync.Mutex{},
		installedRepos: []string{"owner/repo"},
		runners:        make(map[string][]ghapp.Runner),
		calls:          nil,
		installedCalls: 0,
		err:            nil,
	}
}

func (g *fakeGitHub) ListInstalledRepos(_ context.Context) ([]string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.installedCalls++
	if g.err != nil {
		return nil, g.err
	}
	return append([]string(nil), g.installedRepos...), nil
}

func (g *fakeGitHub) ListRunners(_ context.Context, repo string) ([]ghapp.Runner, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls = append(g.calls, repo)
	if g.err != nil {
		return nil, g.err
	}
	return append([]ghapp.Runner(nil), g.runners[repo]...), nil
}

func (g *fakeGitHub) SetRunners(repo string, runners []ghapp.Runner) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.runners[repo] = append([]ghapp.Runner(nil), runners...)
}

func (g *fakeGitHub) InstalledCalls() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.installedCalls
}

func (g *fakeGitHub) RunnerCalls() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.calls...)
}

// fakeSlotProber reports which slots are running per VM, from one call per VM.
type fakeSlotProber struct {
	mu       sync.Mutex
	running  map[string]map[int]bool
	probeErr map[string]error
	calls    []string
	onProbe  func()
}

func newFakeSlotProber() *fakeSlotProber {
	return &fakeSlotProber{
		mu:       sync.Mutex{},
		running:  make(map[string]map[int]bool),
		probeErr: make(map[string]error),
		calls:    nil,
		onProbe:  nil,
	}
}

func (p *fakeSlotProber) RunningSlots(_ context.Context, vm *broker.WarmVM) (map[int]bool, error) {
	p.mu.Lock()
	p.calls = append(p.calls, vm.Name)
	err := p.probeErr[vm.Name]
	slots := p.running[vm.Name]
	hook := p.onProbe
	p.mu.Unlock()
	if hook != nil {
		hook()
	}
	if err != nil {
		return nil, err
	}
	out := make(map[int]bool, len(slots))
	for slot, running := range slots {
		out[slot] = running
	}
	return out, nil
}

func (p *fakeSlotProber) SetRunning(vmName string, slotIndex int, running bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.running[vmName] == nil {
		p.running[vmName] = make(map[int]bool)
	}
	p.running[vmName][slotIndex] = running
}

func (p *fakeSlotProber) SetProbeErr(vmName string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.probeErr[vmName] = err
}

func (p *fakeSlotProber) Calls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

type mutableClock struct {
	mu  sync.Mutex
	now time.Time
}

func newMutableClock(now time.Time) *mutableClock {
	return &mutableClock{mu: sync.Mutex{}, now: now}
}

func (c *mutableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *mutableClock) Advance(delta time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(delta)
}

func testOptions(clock *mutableClock, runnerCount int) Options {
	return Options{
		RunnerCount:    runnerCount,
		JobsPerVM:      1,
		Image:          "image-a",
		MaxIdle:        2 * time.Hour,
		MaxAge:         24 * time.Hour,
		RunToken:       "test",
		WarmRetryDelay: time.Millisecond,
		Now:            clock.Now,
	}
}

func testOptionsWithSlots(clock *mutableClock, runnerCount int, jobsPerVM int) Options {
	options := testOptions(clock, runnerCount)
	options.JobsPerVM = jobsPerVM
	return options
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-timeout.C:
			t.Fatal("condition not met within timeout")
		case <-ticker.C:
		}
	}
}

func waitStarted(t *testing.T, runner *fakeRunner) runCall {
	t.Helper()
	select {
	case call := <-runner.started:
		return call
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not start within timeout")
	}
	return runCall{}
}

func startTestPool(t *testing.T, pool *Pool) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pool.Start(ctx)
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		pool.Shutdown(shutdownCtx)
	})
	return ctx
}

func captureTestLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buffer := &bytes.Buffer{}
	previous := slog.Default()
	logger := slog.New(slog.NewTextHandler(buffer, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})
	return buffer
}

func busyWorkerState(vmName string, bornAt time.Time, boundAt time.Time, jobID int64, runID int64, cancel context.CancelCauseFunc) workerState {
	return workerState{
		vm:        &broker.WarmVM{Name: vmName, Image: "image-a"},
		bornAt:    bornAt,
		idleSince: time.Time{},
		warming:   false,
		recycle:   false,
		slots: []slotState{
			{
				boundAt:   boundAt,
				busy:      true,
				jobID:     jobID,
				runID:     runID,
				jobCancel: cancel,
				lastErr:   nil,
			},
		},
		lastErr: nil,
	}
}

func idleWorkerState(vmName string, bornAt time.Time, idleSince time.Time) workerState {
	return workerState{
		vm:        &broker.WarmVM{Name: vmName, Image: "image-a"},
		bornAt:    bornAt,
		idleSince: idleSince,
		warming:   false,
		recycle:   false,
		slots:     []slotState{{}},
		lastErr:   nil,
	}
}

func TestStatusReportsWorkerViewsAndActiveJob(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	warmer := newFakeWarmer()
	runner := newBlockingRunner(1)
	github := newFakeGitHub()
	prober := newFakeSlotProber()
	prober.SetRunning("vm-busy", 0, false)
	pool := New(testOptions(clock, 2), warmer, runner, github, prober)

	pool.mu.Lock()
	pool.started = true
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-2*time.Minute), 0, 42, nil)
	pool.states[1] = idleWorkerState("vm-idle", now.Add(-time.Hour), now.Add(-time.Minute))
	pool.mu.Unlock()

	snapshot, workers := pool.Status(context.Background())

	if snapshot.Busy != 1 {
		t.Fatalf("snapshot busy = %d, want 1", snapshot.Busy)
	}
	if snapshot.Idle != 1 {
		t.Fatalf("snapshot idle = %d, want 1", snapshot.Idle)
	}
	busy := workers[0]
	if busy.VM != "vm-busy" || busy.Phase != "busy" || busy.RunID != 42 {
		t.Fatalf("busy worker = %+v, want vm-busy busy run 42", busy)
	}
	if busy.BindAgeSeconds != 120 {
		t.Fatalf("busy bind age seconds = %d, want 120", busy.BindAgeSeconds)
	}
	if busy.ActiveJob == nil || *busy.ActiveJob {
		t.Fatalf("busy active job = %v, want false", busy.ActiveJob)
	}
	idle := workers[1]
	if idle.Phase != "idle" || idle.ActiveJob != nil {
		t.Fatalf("idle worker = %+v, want idle nil active job", idle)
	}
	calls := prober.Calls()
	if len(calls) != 1 || calls[0] != "vm-busy" {
		t.Fatalf("prober calls = %v, want [vm-busy]", calls)
	}
}

func TestStatusReportsSlotViewsForMultiSlotWorker(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	prober := newFakeSlotProber()
	prober.SetRunning("vm-slots", 0, true)
	pool := New(testOptionsWithSlots(clock, 1, 2), newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)

	pool.mu.Lock()
	pool.started = true
	pool.states[0] = workerState{
		vm:        &broker.WarmVM{Name: "vm-slots", Image: "image-a"},
		bornAt:    now.Add(-time.Hour),
		idleSince: time.Time{},
		slots: []slotState{
			{boundAt: now.Add(-3 * time.Minute), busy: true, jobID: 1001, runID: 42},
			{},
		},
	}
	pool.mu.Unlock()

	snapshot, workers := pool.Status(context.Background())

	if snapshot.Busy != 1 || snapshot.Idle != 1 {
		t.Fatalf("snapshot busy/idle = %d/%d, want 1/1", snapshot.Busy, snapshot.Idle)
	}
	worker := workers[0]
	if len(worker.Slots) != 2 {
		t.Fatalf("worker slots = %+v, want 2 slots", worker.Slots)
	}
	busySlot := worker.Slots[0]
	if busySlot.Phase != "busy" || busySlot.RunID != 42 || busySlot.JobID != 1001 {
		t.Fatalf("busy slot = %+v, want busy run 42 job 1001", busySlot)
	}
	if busySlot.ActiveJob == nil || !*busySlot.ActiveJob {
		t.Fatalf("busy slot active job = %v, want true", busySlot.ActiveJob)
	}
	if worker.Slots[1].ActiveJob != nil {
		t.Fatalf("idle slot active job = %v, want nil", worker.Slots[1].ActiveJob)
	}
	calls := prober.Calls()
	if len(calls) != 1 || calls[0] != "vm-slots" {
		t.Fatalf("prober calls = %v, want [vm-slots]", calls)
	}
}

func TestWorkerReusesWarmVMAcrossJobs(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newFakeRunner(2)
	pool := New(testOptions(clock, 1), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)

	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo", JobID: 1, RunID: 10}); err != nil {
		t.Fatalf("Enqueue first job: %v", err)
	}
	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo", JobID: 2, RunID: 11}); err != nil {
		t.Fatalf("Enqueue second job: %v", err)
	}
	waitFor(t, func() bool { return len(runner.Calls()) == 2 })
	calls := runner.Calls()
	if calls[0].VMName != calls[1].VMName {
		t.Fatalf("jobs used VMs %q and %q, want one reused VM", calls[0].VMName, calls[1].VMName)
	}
	if got := len(warmer.WarmNames()); got != 1 {
		t.Fatalf("warm calls = %d, want 1", got)
	}
}

func TestVMWithTwoSlotsRunsTwoConcurrentJobs(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newBlockingRunner(2)
	pool := New(testOptionsWithSlots(clock, 1, 2), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)

	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo-a", JobID: 1, RunID: 10}); err != nil {
		t.Fatalf("Enqueue first job: %v", err)
	}
	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo-b", JobID: 2, RunID: 11}); err != nil {
		t.Fatalf("Enqueue second job: %v", err)
	}
	first := waitStarted(t, runner)
	second := waitStarted(t, runner)
	if first.VMName != second.VMName {
		t.Fatalf("jobs used VMs %q and %q, want one shared VM", first.VMName, second.VMName)
	}
	if first.SlotIndex == second.SlotIndex {
		t.Fatalf("jobs used slot %d twice, want distinct slots", first.SlotIndex)
	}
	waitFor(t, func() bool {
		snapshot := pool.Snapshot()
		return snapshot.Busy == 2 && snapshot.Idle == 0
	})
	runner.Release()
	runner.Release()
}

func TestReadyReportsOnlyImmediateIdleCapacity(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newBlockingRunner(2)
	pool := New(testOptionsWithSlots(clock, 1, 2), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)

	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo-a", JobID: 1, RunID: 10}); err != nil {
		t.Fatalf("Enqueue first job: %v", err)
	}
	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo-b", JobID: 2, RunID: 11}); err != nil {
		t.Fatalf("Enqueue second job: %v", err)
	}
	waitStarted(t, runner)
	waitStarted(t, runner)
	waitFor(t, func() bool {
		snapshot := pool.Snapshot()
		return snapshot.Busy == 2 && snapshot.Idle == 0 && snapshot.Queued == 0
	})
	if pool.Snapshot().Ready {
		t.Fatal("ready with all slots busy = true, want false")
	}
	runner.Release()
	waitFor(t, func() bool {
		snapshot := pool.Snapshot()
		return snapshot.Busy == 1 && snapshot.Idle == 1
	})
	if !pool.Snapshot().Ready {
		t.Fatal("ready with one idle slot = false, want true")
	}
	runner.Release()
}

func TestReconfigureAppliesScalarsAndKeepsRunnerCountAndImage(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	pool := New(testOptionsWithSlots(clock, 2, 1), newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), nil)
	var logs strings.Builder
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	nextOptions := Options{
		RunnerCount:    4,
		JobsPerVM:      3,
		Image:          "image-b",
		MaxIdle:        15 * time.Minute,
		MaxAge:         6 * time.Hour,
		MaxBind:        7 * time.Minute,
		PickupTimeout:  90 * time.Second,
		WarmRetryDelay: 25 * time.Millisecond,
	}
	pool.Reconfigure(nextOptions)

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.options.RunnerCount != 2 {
		t.Fatalf("runner count = %d, want 2", pool.options.RunnerCount)
	}
	if pool.options.JobsPerVM != 3 {
		t.Fatalf("jobs per vm = %d, want 3", pool.options.JobsPerVM)
	}
	if pool.options.Image != "image-a" {
		t.Fatalf("image = %q, want preserved image-a", pool.options.Image)
	}
	if pool.options.RunToken != "test" {
		t.Fatalf("run token = %q, want preserved test", pool.options.RunToken)
	}
	if pool.options.MaxBind != 7*time.Minute {
		t.Fatalf("max bind = %s, want 7m", pool.options.MaxBind)
	}
	if pool.options.PickupTimeout != 90*time.Second {
		t.Fatalf("pickup timeout = %s, want 90s", pool.options.PickupTimeout)
	}
	if !strings.Contains(logs.String(), "runner_count change requires restart") {
		t.Fatalf("logs = %q, want runner_count restart warning", logs.String())
	}
	if !strings.Contains(logs.String(), "image change requires restart") {
		t.Fatalf("logs = %q, want image restart warning", logs.String())
	}
}

// TestReconfigureJobsPerVMRecyclesIdleVMAndRewarmsAtNewCount proves a jobs_per_vm
// change recycles an idle VM whose slot count no longer matches and re-warms a
// fresh one at the new count. This is how the pool applies a slot-count change
// now that the guest slot count is fixed at warm rather than reconfigured live:
// the existing shouldRecycle (slotCount != JobsPerVM) trigger tears the VM down
// and the worker loop re-warms at the new JobsPerVM.
func TestReconfigureJobsPerVMRecyclesIdleVMAndRewarmsAtNewCount(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newFakeRunner(1)
	pool := New(testOptionsWithSlots(clock, 1, 1), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)

	// The first VM warms at the original single slot.
	waitFor(t, func() bool { return len(warmer.WarmSlotCounts()) == 1 })
	firstVM := warmer.WarmNames()[0]
	if got := warmer.WarmSlotCounts()[0]; got != 1 {
		t.Fatalf("first warm slot count = %d, want 1", got)
	}

	// Raise jobs_per_vm to 2, so the idle VM's slot count no longer matches.
	pool.Reconfigure(Options{JobsPerVM: 2})

	if err := pool.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// The mismatched idle VM is torn down and a fresh one is warmed at two slots.
	waitFor(t, func() bool { return len(warmer.WarmSlotCounts()) == 2 })
	torn := warmer.TornNames()
	if len(torn) != 1 || torn[0] != firstVM {
		t.Fatalf("torn VMs = %v, want [%s]", torn, firstVM)
	}
	counts := warmer.WarmSlotCounts()
	if counts[len(counts)-1] != 2 {
		t.Fatalf("re-warm slot count = %d, want 2 at the new jobs_per_vm", counts[len(counts)-1])
	}
}

func TestSlotRunJobPanicFreesSlot(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newFakeRunner(1)
	runner.panicErr = errors.New("runner crashed")
	pool := New(testOptionsWithSlots(clock, 1, 2), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)

	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo", JobID: 1, RunID: 42}); err != nil {
		t.Fatalf("Enqueue job: %v", err)
	}
	call := waitStarted(t, runner)
	waitFor(t, func() bool {
		snapshot := pool.Snapshot()
		return snapshot.Busy == 0 && snapshot.Idle == 2
	})
	_, workers := pool.Status(context.Background())
	slot := workers[0].Slots[call.SlotIndex]
	if slot.LastError != "panic: runner crashed" {
		t.Fatalf("slot last error = %q, want panic: runner crashed", slot.LastError)
	}
}

func TestCancelRunCancelsOneSlotWithoutStoppingCotenant(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newBlockingRunner(2)
	pool := New(testOptionsWithSlots(clock, 1, 2), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)

	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo-a", JobID: 1, RunID: 10}); err != nil {
		t.Fatalf("Enqueue first job: %v", err)
	}
	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo-b", JobID: 2, RunID: 11}); err != nil {
		t.Fatalf("Enqueue second job: %v", err)
	}
	waitStarted(t, runner)
	waitStarted(t, runner)

	pool.CancelRun(1)
	waitFor(t, func() bool { return runner.Active() == 1 })
	if torn := warmer.TornNames(); len(torn) != 0 {
		t.Fatalf("teardowns before cotenant finishes = %v, want none", torn)
	}
	runner.Release()
}

func TestCancelRunReapsMatchingBusyWorker(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	pool := New(testOptions(clock, 2), newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), nil)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-time.Minute), 1001, 42, func(error) {
		cancelCount++
	})
	pool.states[1] = busyWorkerState("vm-other", now.Add(-time.Hour), now.Add(-time.Minute), 1002, 43, nil)
	pool.mu.Unlock()

	pool.CancelRun(99)
	pool.mu.Lock()
	if pool.states[0].recycle || cancelCount != 0 {
		pool.mu.Unlock()
		t.Fatalf("non-matching CancelRun recycled or cancelled: recycle=%v count=%d", pool.states[0].recycle, cancelCount)
	}
	pool.mu.Unlock()

	pool.CancelRun(1001)
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if !pool.states[0].recycle {
		t.Fatal("matching worker recycle = false, want true")
	}
	if pool.states[0].slots[0].jobCancel != nil {
		t.Fatal("matching worker jobCancel is still set")
	}
	if pool.states[1].recycle {
		t.Fatal("non-matching worker recycle = true, want false")
	}
	if cancelCount != 1 {
		t.Fatalf("cancel count = %d, want 1", cancelCount)
	}
}

func TestCancelRunPassesNonContextCauseToJob(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	pool := New(testOptions(clock, 1), newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), nil)

	jobCtx, cancel := context.WithCancelCause(context.Background())
	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-time.Minute), 1001, 42, cancel)
	pool.mu.Unlock()

	pool.CancelRun(1001)
	if jobCtx.Err() == nil {
		t.Fatal("job context not cancelled after CancelRun")
	}
	cause := context.Cause(jobCtx)
	if errors.Is(cause, context.Canceled) {
		t.Fatalf("cancel cause = %v, want a non-context teardown cause", cause)
	}
}

func TestShutdownReleasesWorkerVMsWithoutTeardown(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newFakeRunner(1)
	pool := New(testOptions(clock, 3), warmer, runner, newFakeGitHub(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)
	waitFor(t, pool.Ready)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	pool.Shutdown(shutdownCtx)

	if torn := warmer.TornNames(); len(torn) != 0 {
		t.Fatalf("torn VMs = %v, want none on control-plane shutdown", torn)
	}
}

func TestQueueIsFIFOForSingleWorker(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newFakeRunner(3)
	pool := New(testOptions(clock, 1), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)

	for i := 1; i <= 3; i++ {
		repo := fmt.Sprintf("owner/repo-%d", i)
		if err := pool.Enqueue(ctx, Job{Repo: repo, JobID: int64(i), RunID: int64(100 + i)}); err != nil {
			t.Fatalf("Enqueue job %d: %v", i, err)
		}
	}
	waitFor(t, func() bool { return len(runner.Calls()) == 3 })
	calls := runner.Calls()
	for i, call := range calls {
		wantRepo := fmt.Sprintf("owner/repo-%d", i+1)
		if call.Repo != wantRepo {
			t.Fatalf("call %d repo = %q, want %q", i+1, call.Repo, wantRepo)
		}
	}
}

func TestConcurrentWorkersDoNotDoubleServeJobs(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newBlockingRunner(8)
	pool := New(testOptions(clock, 3), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)

	for i := 1; i <= 8; i++ {
		repo := fmt.Sprintf("owner/repo-%d", i)
		if err := pool.Enqueue(ctx, Job{Repo: repo, JobID: int64(i), RunID: int64(100 + i)}); err != nil {
			t.Fatalf("Enqueue job %d: %v", i, err)
		}
	}
	for i := 0; i < 3; i++ {
		waitStarted(t, runner)
	}
	for i := 0; i < 8; i++ {
		runner.Release()
	}
	waitFor(t, func() bool { return len(runner.Calls()) == 8 })
	if maxActive := runner.MaxActive(); maxActive > 3 {
		t.Fatalf("max active jobs = %d, want at most 3", maxActive)
	}
	seen := make(map[string]int)
	for _, call := range runner.Calls() {
		seen[call.Repo]++
	}
	var duplicates []string
	for repo, count := range seen {
		if count != 1 {
			duplicates = append(duplicates, repo)
		}
	}
	sort.Strings(duplicates)
	if len(duplicates) > 0 {
		t.Fatalf("jobs served more than once: %v", duplicates)
	}
}

func TestReconcileRecyclesIdleVMByMaxIdle(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newFakeRunner(1)
	options := testOptions(clock, 1)
	options.MaxIdle = time.Minute
	pool := New(options, warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)
	firstVM := warmer.WarmNames()[0]

	clock.Advance(time.Minute + time.Second)
	if err := pool.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	waitFor(t, func() bool { return len(warmer.WarmNames()) == 2 })
	torn := warmer.TornNames()
	if len(torn) != 1 || torn[0] != firstVM {
		t.Fatalf("torn VMs = %v, want [%s]", torn, firstVM)
	}
}

func TestReconcileReplacesIdleVMOnHealthFailure(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newFakeRunner(1)
	pool := New(testOptions(clock, 1), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)
	firstVM := warmer.WarmNames()[0]
	warmer.SetCheckError(firstVM, errors.New("hello failed"))

	if err := pool.Reconcile(ctx); err == nil {
		t.Fatal("Reconcile should report the health failure")
	}
	waitFor(t, func() bool { return len(warmer.WarmNames()) == 2 })
}

func TestReconcileReplacesIdleVMWithStaleGitHubRunner(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newFakeRunner(1)
	github := newFakeGitHub()
	pool := New(testOptions(clock, 1), warmer, runner, github, nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)
	firstVM := warmer.WarmNames()[0]
	github.SetRunners("owner/repo", []ghapp.Runner{{Name: firstVM, Status: "offline"}})

	if err := pool.Reconcile(ctx); err == nil {
		t.Fatal("Reconcile should report the stale GitHub runner")
	}
	waitFor(t, func() bool { return len(warmer.WarmNames()) == 2 })
}

func TestReconcileReapsBusyWorkerAfterPickupTimeoutWithoutRunningExecution(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	prober := newFakeSlotProber()
	prober.SetRunning("vm-busy", 0, false)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0
	logs := captureTestLogs(t)

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 0, 42, func(error) {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if !pool.states[0].recycle {
		t.Fatal("busy worker recycle = false, want true")
	}
	if cancelCount != 1 {
		t.Fatalf("cancel count = %d, want 1", cancelCount)
	}
	if !strings.Contains(logs.String(), "runnerpool reaping worker (no active job process)") {
		t.Fatalf("logs = %q, want no active job reap warning", logs.String())
	}
	if calls := prober.Calls(); len(calls) != 1 || calls[0] != "vm-busy" {
		t.Fatalf("prober calls = %v, want [vm-busy]", calls)
	}
}

func TestReconcileReapsRunningSlotPastMaxBind(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = 2 * time.Minute
	prober := newFakeSlotProber()
	// The guest still reports the execution running, but MaxBind is an absolute
	// ceiling, so a hung-but-alive runner past MaxBind is reaped anyway.
	prober.SetRunning("vm-busy", 0, true)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0
	logs := captureTestLogs(t)

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.MaxBind-time.Second), 0, 42, func(error) {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if !pool.states[0].recycle {
		t.Fatal("running slot past max bind recycle = false, want true (absolute ceiling)")
	}
	if cancelCount != 1 {
		t.Fatalf("cancel count = %d, want 1", cancelCount)
	}
	if !strings.Contains(logs.String(), "runnerpool reaping worker (past max_bind ceiling)") {
		t.Fatalf("logs = %q, want past max_bind ceiling reap warning", logs.String())
	}
	// Past MaxBind reaps before probing, so no Reattach call is made.
	if calls := prober.Calls(); len(calls) != 0 {
		t.Fatalf("prober calls = %v, want none (ceiling reaps before probing)", calls)
	}
}

func TestReconcileReapsPastMaxBindWithoutProber(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.MaxBind = 2 * time.Minute
	// No prober: the absolute MaxBind ceiling must still be enforced.
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), nil)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.MaxBind-time.Second), 0, 42, func(error) {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if !pool.states[0].recycle {
		t.Fatal("recycle = false, want true past max bind with a nil prober")
	}
	if cancelCount != 1 {
		t.Fatalf("cancel count = %d, want 1", cancelCount)
	}
}

func TestReconcileKeepsRunningSlotWithinMaxBind(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	prober := newFakeSlotProber()
	prober.SetRunning("vm-busy", 0, true)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 0, 42, func(error) {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.states[0].recycle {
		t.Fatal("running slot within max bind recycle = true, want false")
	}
	if cancelCount != 0 {
		t.Fatalf("cancel count = %d, want 0", cancelCount)
	}
}

func TestReconcileReapsOneExpiredSlotWithoutStoppingCotenant(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptionsWithSlots(clock, 1, 2)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	warmer := newFakeWarmer()
	runner := newBlockingRunner(2)
	prober := newFakeSlotProber()
	pool := New(options, warmer, runner, newFakeGitHub(), prober)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)

	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo-a", JobID: 1, RunID: 10}); err != nil {
		t.Fatalf("Enqueue first job: %v", err)
	}
	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo-b", JobID: 2, RunID: 11}); err != nil {
		t.Fatalf("Enqueue second job: %v", err)
	}
	first := waitStarted(t, runner)
	second := waitStarted(t, runner)
	if first.SlotIndex == second.SlotIndex {
		t.Fatalf("jobs used slot %d twice, want distinct slots", first.SlotIndex)
	}
	// Both slots are past the pickup timeout but within MaxBind, so the reap is
	// decided by the running probe: the expired slot is not running, the cotenant
	// is. Both share one VM, so RunningSlots is read once.
	prober.SetRunning(first.VMName, first.SlotIndex, false)
	prober.SetRunning(first.VMName, second.SlotIndex, true)

	pool.mu.Lock()
	pool.states[0].slots[first.SlotIndex].boundAt = now.Add(-options.PickupTimeout - time.Second)
	pool.states[0].slots[second.SlotIndex].boundAt = now.Add(-options.PickupTimeout - time.Second)
	pool.mu.Unlock()

	if err := pool.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	waitFor(t, func() bool { return runner.Active() == 1 })
	if torn := warmer.TornNames(); len(torn) != 0 {
		t.Fatalf("teardowns before cotenant finishes = %v, want none", torn)
	}
	calls := prober.Calls()
	if len(calls) != 1 || calls[0] != first.VMName {
		t.Fatalf("prober calls = %v, want one call [%s]", calls, first.VMName)
	}
	runner.Release()
}

func TestReconcileReapsPastMaxBindEvenWhenProbeWouldError(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = 2 * time.Minute
	prober := newFakeSlotProber()
	prober.SetProbeErr("vm-busy", errors.New("guest unreachable"))
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0
	logs := captureTestLogs(t)

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.MaxBind-time.Second), 0, 42, func(error) {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if !pool.states[0].recycle {
		t.Fatal("busy worker recycle = false, want true past max bind ceiling")
	}
	if cancelCount != 1 {
		t.Fatalf("cancel count = %d, want 1", cancelCount)
	}
	if !strings.Contains(logs.String(), "runnerpool reaping worker (past max_bind ceiling)") {
		t.Fatalf("logs = %q, want past max_bind ceiling reap warning", logs.String())
	}
	// The ceiling reaps before any probe, so the unreachable guest is never called.
	if calls := prober.Calls(); len(calls) != 0 {
		t.Fatalf("prober calls = %v, want none past the ceiling", calls)
	}
}

func TestReconcileKeepsBusyWorkerAfterPickupTimeoutWhenProbeErrors(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	prober := newFakeSlotProber()
	prober.SetProbeErr("vm-busy", errors.New("guest probe failed"))
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 0, 42, func(error) {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.states[0].recycle {
		t.Fatal("busy worker recycle = true after probe error before max bind, want false")
	}
	if cancelCount != 0 {
		t.Fatalf("cancel count = %d, want 0", cancelCount)
	}
}

func TestReconcileWarnsAndRequestsNoActiveJobRecycleOncePerBind(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	prober := newFakeSlotProber()
	prober.SetRunning("vm-busy", 0, false)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0
	repeatedCancelCount := 0
	logs := captureTestLogs(t)

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 0, 42, func(error) {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	pool.mu.Lock()
	pool.states[0].slots[0].jobCancel = func(error) { repeatedCancelCount++ }
	pool.mu.Unlock()
	clock.Advance(time.Second)
	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}

	warnMessage := "runnerpool reaping worker (no active job process)"
	if count := strings.Count(logs.String(), warnMessage); count != 1 {
		t.Fatalf("no active job warning count = %d, want 1", count)
	}
	if cancelCount != 1 || repeatedCancelCount != 0 {
		t.Fatalf("cancel counts = %d/%d, want 1/0", cancelCount, repeatedCancelCount)
	}
}

func TestReconcileKeepsBusyWorkerWhenBindingChangesBeforeRecycleApply(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	prober := newFakeSlotProber()
	prober.SetRunning("vm-busy", 0, false)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	oldCancelCount := 0
	newCancelCount := 0
	newBoundAt := now.Add(-30 * time.Second)

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 0, 42, func(error) {
		oldCancelCount++
	})
	pool.mu.Unlock()

	prober.onProbe = func() {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		pool.states[0].slots[0].boundAt = newBoundAt
		pool.states[0].slots[0].runID = 43
		pool.states[0].slots[0].jobCancel = func(error) { newCancelCount++ }
	}

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.states[0].recycle {
		t.Fatal("rebound worker recycle = true, want false")
	}
	if oldCancelCount != 0 || newCancelCount != 0 {
		t.Fatalf("cancel counts = %d/%d, want 0/0", oldCancelCount, newCancelCount)
	}
}

func TestStartAdoptsRunningVMsBeforeWarming(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	warmer.SetAdopted([]broker.AdoptedVM{
		{VM: &broker.WarmVM{Name: "vm-adopted", Image: "image-a"}, Slots: nil},
	})
	runner := newFakeRunner(1)
	pool := New(testOptions(clock, 2), warmer, runner, newFakeGitHub(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)
	waitFor(t, func() bool { return len(warmer.WarmNames()) == 1 })

	adoptCalls := warmer.AdoptCalls()
	if len(adoptCalls) != 1 || adoptCalls[0].Limit != 2 {
		t.Fatalf("adopt calls = %+v, want one call with limit 2", adoptCalls)
	}
	_, workers := pool.Status(context.Background())
	if workers[0].VM != "vm-adopted" {
		t.Fatalf("first worker vm = %q, want vm-adopted", workers[0].VM)
	}
}

func TestStartReAdoptsAndResumesBusySlotAcrossRestart(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	warmer := newFakeWarmer()
	warmer.SetAdopted([]broker.AdoptedVM{
		{
			VM: &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
			Slots: []broker.SlotBinding{
				{
					SlotIndex:    0,
					Repo:         "owner/repo",
					JobID:        1001,
					RunID:        42,
					ExecutionID:  "owner/repo#42#1001",
					ResumeCursor: 9,
					BoundAt:      now.Add(-time.Minute),
				},
			},
		},
	})
	runner := newBlockingRunner(1)
	pool := New(testOptions(clock, 1), warmer, runner, newFakeGitHub(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	resume := waitStarted(t, runner)
	if !resume.Resume || resume.ExecutionID != "owner/repo#42#1001" || resume.Cursor != 9 {
		t.Fatalf("resume call = %+v, want resume of owner/repo#42#1001 cursor 9", resume)
	}
	waitFor(t, func() bool {
		snapshot := pool.Snapshot()
		return snapshot.Busy == 1 && snapshot.Idle == 0
	})
	if got := len(warmer.WarmNames()); got != 0 {
		t.Fatalf("warm calls = %d, want 0 while adopted busy vm occupies the slot", got)
	}
	runner.Release()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	pool.Shutdown(shutdownCtx)
}

func TestResumeFailureRecyclesAdoptedVM(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	warmer := newFakeWarmer()
	warmer.SetAdopted([]broker.AdoptedVM{
		{
			VM: &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
			Slots: []broker.SlotBinding{
				{
					SlotIndex:    0,
					Repo:         "owner/repo",
					JobID:        1001,
					RunID:        42,
					ExecutionID:  "owner/repo#42#1001",
					ResumeCursor: 3,
					BoundAt:      now.Add(-time.Minute),
				},
			},
		},
	})
	runner := newFakeRunner(1)
	runner.runErr = errors.New("resume could not attach")
	pool := New(testOptions(clock, 1), warmer, runner, newFakeGitHub(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	// Resume fails before draining, so the inherited execution may still run; the
	// VM is recycled (torn down and re-warmed) rather than serving new work.
	waitFor(t, func() bool { return len(warmer.TornNames()) == 1 })
	if torn := warmer.TornNames(); torn[0] != "vm-busy" {
		t.Fatalf("torn VMs = %v, want [vm-busy]", torn)
	}
	waitFor(t, func() bool { return len(warmer.WarmNames()) == 1 })

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	pool.Shutdown(shutdownCtx)
}

func TestResumeNonzeroTerminalKeepsAdoptedVM(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	warmer := newFakeWarmer()
	warmer.SetAdopted([]broker.AdoptedVM{
		{
			VM: &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
			Slots: []broker.SlotBinding{
				{
					SlotIndex:    0,
					Repo:         "owner/repo",
					JobID:        1001,
					RunID:        42,
					ExecutionID:  "owner/repo#42#1001",
					ResumeCursor: 3,
					BoundAt:      now.Add(-time.Minute),
				},
			},
		},
	})
	runner := newFakeRunner(1)
	// The adopted job reached its terminal with a nonzero exit, wrapped with
	// ErrJobTerminal. That is a normal job failure, not a resume/attach failure.
	runner.runErr = fmt.Errorf("job exited 7: %w", broker.ErrJobTerminal)
	pool := New(testOptions(clock, 1), warmer, runner, newFakeGitHub(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	// The slot frees and the VM stays available rather than being recycled.
	waitFor(t, func() bool {
		snapshot := pool.Snapshot()
		return snapshot.Idle == 1 && snapshot.Busy == 0
	})
	if torn := warmer.TornNames(); len(torn) != 0 {
		t.Fatalf("torn VMs = %v, want none (terminal job failure keeps the VM)", torn)
	}
	if warmed := warmer.WarmNames(); len(warmed) != 0 {
		t.Fatalf("warm calls = %v, want none (adopted VM kept, not re-warmed)", warmed)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	pool.Shutdown(shutdownCtx)
}

func TestInstallAdoptedWorkersCarriesExecutionAndCursor(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	pool := New(testOptionsWithSlots(clock, 1, 2), newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), nil)
	boundAt := now.Add(-time.Minute)

	pool.mu.Lock()
	pool.installAdoptedWorkersLocked([]broker.AdoptedVM{
		{
			VM: &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
			Slots: []broker.SlotBinding{
				{
					SlotIndex:    0,
					Repo:         "owner/repo",
					JobID:        1001,
					RunID:        42,
					ExecutionID:  "owner/repo#42#1001",
					ResumeCursor: 5,
					BoundAt:      boundAt,
				},
			},
		},
	})
	pool.mu.Unlock()

	pool.mu.Lock()
	defer pool.mu.Unlock()
	slot := pool.states[0].slots[0]
	if !slot.busy || !slot.adopted || slot.executionID != "owner/repo#42#1001" || slot.resumeCursor != 5 {
		t.Fatalf("slot 0 = %+v, want adopted busy execution owner/repo#42#1001 cursor 5", slot)
	}
}

func TestReconcileRecyclesAdoptedBusySlotAfterExecutionStops(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	warmer := newFakeWarmer()
	runner := newBlockingRunner(1)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	prober := newFakeSlotProber()
	prober.SetRunning("vm-busy", 0, false)
	warmer.SetAdopted([]broker.AdoptedVM{
		{
			VM: &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
			Slots: []broker.SlotBinding{
				{
					SlotIndex:    0,
					Repo:         "owner/repo",
					JobID:        1001,
					RunID:        42,
					ExecutionID:  "owner/repo#42#1001",
					ResumeCursor: 3,
					BoundAt:      now.Add(-2 * time.Minute),
				},
			},
		},
	})
	pool := New(options, warmer, runner, newFakeGitHub(), prober)
	ctx := startTestPool(t, pool)
	waitStarted(t, runner)
	waitFor(t, func() bool {
		snapshot := pool.Snapshot()
		return snapshot.Busy == 1
	})

	if err := pool.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// The reap cancels the resumed drain with the recycle cause; releasing the
	// runner lets the drain return so the worker recycles and re-warms.
	runner.Release()
	waitFor(t, func() bool { return len(warmer.TornNames()) == 1 })
	torn := warmer.TornNames()
	if torn[0] != "vm-busy" {
		t.Fatalf("torn VMs = %v, want [vm-busy]", torn)
	}
}

func TestRunnerNameBelongsToVMMatchesExactNameWithMultipleSlots(t *testing.T) {
	if !runnerNameBelongsToVM("vm-slots", "vm-slots", 2) {
		t.Fatal("bare runner name did not match multi-slot VM name")
	}
}

func TestRunnerNameBelongsToVMMatchesStaleSlotNameWithSingleSlot(t *testing.T) {
	if !runnerNameBelongsToVM("vm-slots", "vm-slots-slot-1", 1) {
		t.Fatal("stale slot runner name did not match single-slot VM name")
	}
}

// TestNewJobDrainStallRecyclesVM proves a running job whose drain stalls (RunJob
// returns broker.ErrDrainStalled) recycles its VM. The stalled VM is torn down
// and a fresh one warmed, so a silent guest never strands the slot.
func TestNewJobDrainStallRecyclesVM(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newBlockingRunner(1)
	runner.runErr = broker.ErrDrainStalled
	pool := New(testOptions(clock, 1), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)
	firstVM := warmer.WarmNames()[0]

	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo", JobID: 1001, RunID: 42}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitStarted(t, runner)
	runner.Release()

	waitFor(t, func() bool {
		return len(warmer.WarmNames()) == 2
	})
	torn := warmer.TornNames()
	if len(torn) != 1 || torn[0] != firstVM {
		t.Fatalf("torn VMs after drain stall = %v, want [%s]", torn, firstVM)
	}
}
