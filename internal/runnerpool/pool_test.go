package runnerpool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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

type fakeWarmer struct {
	mu       sync.Mutex
	calls    []warmCall
	torn     []string
	alive    map[string]bool
	checkErr map[string]error
	swept    int
}

func newFakeWarmer() *fakeWarmer {
	return &fakeWarmer{
		mu:       sync.Mutex{},
		calls:    nil,
		torn:     nil,
		alive:    make(map[string]bool),
		checkErr: make(map[string]error),
		swept:    0,
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

func (w *fakeWarmer) SweepOrphans(_ context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.swept++
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

type runCall struct {
	Repo       string
	RunnerName string
	VMName     string
	SlotIndex  int
	SlotCount  int
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

func (r *fakeRunner) RunJob(ctx context.Context, vm *broker.WarmVM, repo string, runnerName string, slotIndex int, slotCount int) error {
	call := runCall{
		Repo:       repo,
		RunnerName: runnerName,
		VMName:     vm.Name,
		SlotIndex:  slotIndex,
		SlotCount:  slotCount,
	}
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

type fakeActiveJobProber struct {
	mu          sync.Mutex
	active      map[string]bool
	calls       []string
	probeErr    error
	onProbe     func()
	cpuActivity map[string]float64
	cpuCalls    []string
	cpuProbeErr error
	onCPUProbe  func()
}

func newFakeActiveJobProber(active map[string]bool) *fakeActiveJobProber {
	return &fakeActiveJobProber{
		mu:          sync.Mutex{},
		active:      active,
		calls:       nil,
		probeErr:    nil,
		onProbe:     nil,
		cpuActivity: make(map[string]float64),
		cpuCalls:    nil,
		cpuProbeErr: nil,
		onCPUProbe:  nil,
	}
}

func (p *fakeActiveJobProber) HasActiveJob(_ context.Context, vm *broker.WarmVM, slotIndex int, slotCount int) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := probeKey(vm.Name, slotIndex, slotCount)
	p.calls = append(p.calls, key)
	if p.probeErr != nil {
		return false, p.probeErr
	}
	if p.onProbe != nil {
		p.onProbe()
	}
	return p.active[key], nil
}

func (p *fakeActiveJobProber) SlotCPUActivity(_ context.Context, vm *broker.WarmVM, slotIndex int, slotCount int) (float64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := probeKey(vm.Name, slotIndex, slotCount)
	p.cpuCalls = append(p.cpuCalls, key)
	if p.cpuProbeErr != nil {
		return 0, p.cpuProbeErr
	}
	if p.onCPUProbe != nil {
		p.onCPUProbe()
	}
	return p.cpuActivity[key], nil
}

func (p *fakeActiveJobProber) Calls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

func (p *fakeActiveJobProber) CPUCalls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.cpuCalls...)
}

func (p *fakeActiveJobProber) SetActive(vmName string, slotIndex int, slotCount int, active bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := probeKey(vmName, slotIndex, slotCount)
	p.active[key] = active
}

func (p *fakeActiveJobProber) SetCPUActivity(key string, cpuActivity float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cpuActivity[key] = cpuActivity
}

func probeKey(vmName string, slotIndex int, slotCount int) string {
	if slotCount <= 1 {
		return vmName
	}
	return fmt.Sprintf("%s#%d", vmName, slotIndex)
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

func busyWorkerState(vmName string, bornAt time.Time, boundAt time.Time, jobID int64, runID int64, cancel context.CancelFunc) workerState {
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
	prober := newFakeActiveJobProber(map[string]bool{"vm-busy": false})
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
	if len(workers) != 2 {
		t.Fatalf("workers = %+v, want 2 workers", workers)
	}
	busy := workers[0]
	if busy.Index != 0 {
		t.Fatalf("busy index = %d, want 0", busy.Index)
	}
	if busy.VM != "vm-busy" {
		t.Fatalf("busy vm = %q, want vm-busy", busy.VM)
	}
	if busy.Phase != "busy" {
		t.Fatalf("busy phase = %q, want busy", busy.Phase)
	}
	if busy.RunID != 42 {
		t.Fatalf("busy run id = %d, want 42", busy.RunID)
	}
	if busy.BindAgeSeconds != 120 {
		t.Fatalf("busy bind age seconds = %d, want 120", busy.BindAgeSeconds)
	}
	if busy.ActiveJob == nil {
		t.Fatal("busy active job = nil, want false")
	}
	if *busy.ActiveJob {
		t.Fatal("busy active job = true, want false")
	}
	idle := workers[1]
	if idle.Phase != "idle" {
		t.Fatalf("idle phase = %q, want idle", idle.Phase)
	}
	if idle.ActiveJob != nil {
		t.Fatalf("idle active job = %v, want nil", *idle.ActiveJob)
	}
	if idle.BindAgeSeconds != 0 {
		t.Fatalf("idle bind age seconds = %d, want 0", idle.BindAgeSeconds)
	}
	calls := prober.Calls()
	if len(calls) != 1 || calls[0] != "vm-busy" {
		t.Fatalf("prober calls = %v, want [vm-busy]", calls)
	}
}

func TestStatusReportsSlotViewsForMultiSlotWorker(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	prober := newFakeActiveJobProber(map[string]bool{"vm-slots#0": true})
	pool := New(testOptionsWithSlots(clock, 1, 2), newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)

	pool.mu.Lock()
	pool.started = true
	pool.states[0] = workerState{
		vm:        &broker.WarmVM{Name: "vm-slots", Image: "image-a"},
		bornAt:    now.Add(-time.Hour),
		idleSince: time.Time{},
		warming:   false,
		recycle:   false,
		slots: []slotState{
			{
				boundAt: now.Add(-3 * time.Minute),
				busy:    true,
				jobID:   1001,
				runID:   42,
			},
			{},
		},
		lastErr: nil,
	}
	pool.mu.Unlock()

	snapshot, workers := pool.Status(context.Background())

	if snapshot.Busy != 1 || snapshot.Idle != 1 {
		t.Fatalf("snapshot busy/idle = %d/%d, want 1/1", snapshot.Busy, snapshot.Idle)
	}
	if len(workers) != 1 {
		t.Fatalf("workers = %+v, want 1 worker", workers)
	}
	worker := workers[0]
	if worker.Phase != "busy" {
		t.Fatalf("worker phase = %q, want busy", worker.Phase)
	}
	if len(worker.Slots) != 2 {
		t.Fatalf("worker slots = %+v, want 2 slots", worker.Slots)
	}
	busySlot := worker.Slots[0]
	if busySlot.Phase != "busy" || busySlot.RunID != 42 || busySlot.JobID != 1001 {
		t.Fatalf("busy slot = %+v, want busy run 42 job 1001", busySlot)
	}
	if busySlot.BindAgeSeconds != 180 {
		t.Fatalf("busy slot bind age = %d, want 180", busySlot.BindAgeSeconds)
	}
	if busySlot.ActiveJob == nil || !*busySlot.ActiveJob {
		t.Fatalf("busy slot active job = %v, want true", busySlot.ActiveJob)
	}
	idleSlot := worker.Slots[1]
	if idleSlot.Phase != "idle" {
		t.Fatalf("idle slot phase = %q, want idle", idleSlot.Phase)
	}
	if idleSlot.ActiveJob != nil {
		t.Fatalf("idle slot active job = %v, want nil", idleSlot.ActiveJob)
	}
	calls := prober.Calls()
	if len(calls) != 1 || calls[0] != "vm-slots#0" {
		t.Fatalf("prober calls = %v, want [vm-slots#0]", calls)
	}
}

func TestStatusSingleSlotKeepsLastErrorEmptyAfterRunJobError(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newFakeRunner(1)
	runner.runErr = errors.New("transient run error")
	pool := New(testOptionsWithSlots(clock, 1, 1), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)

	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo", JobID: 1, RunID: 42}); err != nil {
		t.Fatalf("Enqueue job: %v", err)
	}
	waitStarted(t, runner)
	waitFor(t, func() bool {
		snapshot := pool.Snapshot()
		return snapshot.Idle == 1 && snapshot.Busy == 0 && snapshot.Queued == 0
	})

	_, workers := pool.Status(context.Background())
	if len(workers) != 1 {
		t.Fatalf("workers = %+v, want 1 worker", workers)
	}
	worker := workers[0]
	if worker.LastError != "" {
		t.Fatalf("single-slot last error = %q, want empty", worker.LastError)
	}
	if worker.RunID != 0 {
		t.Fatalf("single-slot run id = %d, want 0", worker.RunID)
	}
	if worker.BindAgeSeconds != 0 {
		t.Fatalf("single-slot bind age seconds = %d, want 0", worker.BindAgeSeconds)
	}
	if worker.Slots != nil {
		t.Fatalf("single-slot slots = %+v, want nil", worker.Slots)
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

	waitFor(t, func() bool {
		return len(runner.Calls()) == 2
	})
	calls := runner.Calls()
	if calls[0].VMName != calls[1].VMName {
		t.Fatalf("jobs used VMs %q and %q, want one reused VM", calls[0].VMName, calls[1].VMName)
	}
	if got := len(warmer.WarmNames()); got != 1 {
		t.Fatalf("warm calls = %d, want 1", got)
	}
	if got := len(warmer.TornNames()); got != 0 {
		t.Fatalf("teardowns before shutdown = %d, want 0", got)
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
	if first.SlotCount != 2 || second.SlotCount != 2 {
		t.Fatalf("slot counts = %d and %d, want 2 and 2", first.SlotCount, second.SlotCount)
	}
	waitFor(t, func() bool {
		snapshot := pool.Snapshot()
		return snapshot.Busy == 2 && snapshot.Idle == 0
	})
	if maxActive := runner.MaxActive(); maxActive != 2 {
		t.Fatalf("max active jobs = %d, want 2", maxActive)
	}

	runner.Release()
	waitFor(t, func() bool {
		snapshot := pool.Snapshot()
		return snapshot.Busy == 1 && snapshot.Idle == 1
	})
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
	snapshot := pool.Snapshot()
	if snapshot.Ready {
		t.Fatalf("ready with all slots busy = true, want false: %+v", snapshot)
	}

	runner.Release()
	waitFor(t, func() bool {
		snapshot := pool.Snapshot()
		return snapshot.Busy == 1 && snapshot.Idle == 1 && snapshot.Queued == 0
	})
	snapshot = pool.Snapshot()
	if !snapshot.Ready {
		t.Fatalf("ready with one idle slot = false, want true: %+v", snapshot)
	}
	runner.Release()
}

func TestWarmWorkerPassesConfiguredSlotCount(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newFakeRunner(1)
	pool := New(testOptionsWithSlots(clock, 1, 2), warmer, runner, newFakeGitHub(), nil)
	startTestPool(t, pool)
	waitFor(t, pool.Ready)

	slotCounts := warmer.WarmSlotCounts()
	if len(slotCounts) != 1 || slotCounts[0] != 2 {
		t.Fatalf("initial warm slot counts = %v, want [2]", slotCounts)
	}

	nextOptions := testOptionsWithSlots(clock, 1, 3)
	pool.Reconfigure(nextOptions)
	firstVM := warmer.WarmNames()[0]
	pool.mu.Lock()
	pool.states[0].recycle = true
	pool.cond.Broadcast()
	pool.mu.Unlock()

	waitFor(t, func() bool {
		return len(warmer.WarmNames()) == 2
	})
	torn := warmer.TornNames()
	if len(torn) != 1 || torn[0] != firstVM {
		t.Fatalf("torn VMs = %v, want [%s]", torn, firstVM)
	}
	slotCounts = warmer.WarmSlotCounts()
	if len(slotCounts) != 2 || slotCounts[1] != 3 {
		t.Fatalf("warm slot counts after reconfigure = %v, want second count 3", slotCounts)
	}
}

func TestReconfigureAppliesScalarsAndKeepsRunnerCountAndImage(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	pool := New(testOptionsWithSlots(clock, 2, 1), newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), nil)
	var logs strings.Builder
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	nextOptions := Options{
		RunnerCount:    4,
		JobsPerVM:      3,
		Image:          "image-b",
		MaxIdle:        15 * time.Minute,
		MaxAge:         6 * time.Hour,
		MaxBind:        7 * time.Minute,
		PickupTimeout:  90 * time.Second,
		StallTimeout:   4 * time.Minute,
		StallReap:      true,
		RunToken:       "",
		WarmRetryDelay: 25 * time.Millisecond,
		Now:            nil,
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
	if pool.options.Now == nil {
		t.Fatal("now function = nil, want preserved clock")
	}
	if pool.options.MaxIdle != 15*time.Minute {
		t.Fatalf("max idle = %s, want 15m", pool.options.MaxIdle)
	}
	if pool.options.MaxAge != 6*time.Hour {
		t.Fatalf("max age = %s, want 6h", pool.options.MaxAge)
	}
	if pool.options.MaxBind != 7*time.Minute {
		t.Fatalf("max bind = %s, want 7m", pool.options.MaxBind)
	}
	if pool.options.PickupTimeout != 90*time.Second {
		t.Fatalf("pickup timeout = %s, want 90s", pool.options.PickupTimeout)
	}
	if pool.options.StallTimeout != 4*time.Minute {
		t.Fatalf("stall timeout = %s, want 4m", pool.options.StallTimeout)
	}
	if !pool.options.StallReap {
		t.Fatal("stall reap = false, want true")
	}
	if pool.options.WarmRetryDelay != 25*time.Millisecond {
		t.Fatalf("warm retry delay = %s, want 25ms", pool.options.WarmRetryDelay)
	}
	if !strings.Contains(logs.String(), "runner_count change requires restart") {
		t.Fatalf("logs = %q, want runner_count restart warning", logs.String())
	}
	if !strings.Contains(logs.String(), "image change requires restart") {
		t.Fatalf("logs = %q, want image restart warning", logs.String())
	}
	if !strings.Contains(logs.String(), "runnerpool reconfigured") {
		t.Fatalf("logs = %q, want reconfigured info", logs.String())
	}
}

func TestReconfigureBusyWorkerKeepsOldSlotsUntilIdleResize(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newBlockingRunner(2)
	pool := New(testOptionsWithSlots(clock, 1, 1), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)
	firstVM := warmer.WarmNames()[0]

	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo-a", JobID: 1, RunID: 10}); err != nil {
		t.Fatalf("Enqueue first job: %v", err)
	}
	first := waitStarted(t, runner)
	if first.SlotCount != 1 {
		t.Fatalf("first slot count = %d, want 1", first.SlotCount)
	}

	pool.Reconfigure(testOptionsWithSlots(clock, 1, 2))
	if err := pool.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile while busy: %v", err)
	}
	if torn := warmer.TornNames(); len(torn) != 0 {
		t.Fatalf("busy worker teardowns = %v, want none", torn)
	}
	pool.mu.Lock()
	busySlotCount := len(pool.states[0].slots)
	busyRecycle := pool.states[0].recycle
	pool.mu.Unlock()
	if busySlotCount != 1 || busyRecycle {
		t.Fatalf("busy worker slots recycle = %d %v, want 1 false", busySlotCount, busyRecycle)
	}

	runner.Release()
	waitFor(t, func() bool {
		snapshot := pool.Snapshot()
		return snapshot.Idle == 1 && snapshot.Busy == 0
	})
	if err := pool.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile while idle: %v", err)
	}
	waitFor(t, func() bool {
		return len(warmer.WarmNames()) == 2
	})
	torn := warmer.TornNames()
	if len(torn) != 1 || torn[0] != firstVM {
		t.Fatalf("idle resize teardowns = %v, want [%s]", torn, firstVM)
	}
	pool.mu.Lock()
	resizedSlotCount := len(pool.states[0].slots)
	pool.mu.Unlock()
	if resizedSlotCount != 2 {
		t.Fatalf("resized slot count = %d, want 2", resizedSlotCount)
	}
	slotCounts := warmer.WarmSlotCounts()
	if len(slotCounts) != 2 || slotCounts[1] != 2 {
		t.Fatalf("warm slot counts = %v, want second count 2", slotCounts)
	}

	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo-b", JobID: 2, RunID: 11}); err != nil {
		t.Fatalf("Enqueue second job: %v", err)
	}
	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo-c", JobID: 3, RunID: 12}); err != nil {
		t.Fatalf("Enqueue third job: %v", err)
	}
	second := waitStarted(t, runner)
	third := waitStarted(t, runner)
	if second.VMName != third.VMName {
		t.Fatalf("resized jobs used VMs %q and %q, want one VM", second.VMName, third.VMName)
	}
	if second.SlotCount != 2 || third.SlotCount != 2 {
		t.Fatalf("resized slot counts = %d and %d, want 2 and 2", second.SlotCount, third.SlotCount)
	}
	runner.Release()
	runner.Release()
}

func TestStatusReportsBusyWorkerActualSlotsUntilIdleResize(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newBlockingRunner(2)
	prober := newFakeActiveJobProber(map[string]bool{})
	pool := New(testOptionsWithSlots(clock, 1, 2), warmer, runner, newFakeGitHub(), prober)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)
	firstVM := warmer.WarmNames()[0]

	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo-a", JobID: 1, RunID: 10}); err != nil {
		t.Fatalf("Enqueue first job: %v", err)
	}
	first := waitStarted(t, runner)
	if first.SlotCount != 2 {
		t.Fatalf("first slot count = %d, want 2", first.SlotCount)
	}
	prober.SetActive(firstVM, first.SlotIndex, first.SlotCount, true)

	pool.Reconfigure(testOptionsWithSlots(clock, 1, 1))
	if err := pool.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile while busy: %v", err)
	}

	_, workers := pool.Status(context.Background())
	if len(workers) != 1 {
		t.Fatalf("workers = %+v, want 1 worker", workers)
	}
	if len(workers[0].Slots) != 2 {
		t.Fatalf("busy worker status slots = %+v, want 2 actual slots", workers[0].Slots)
	}
	activeSlot := workers[0].Slots[first.SlotIndex]
	if activeSlot.ActiveJob == nil || !*activeSlot.ActiveJob {
		t.Fatalf("busy worker active job = %v, want true", activeSlot.ActiveJob)
	}
	calls := prober.Calls()
	expectedCall := probeKey(firstVM, first.SlotIndex, first.SlotCount)
	if len(calls) != 1 || calls[0] != expectedCall {
		t.Fatalf("prober calls = %v, want [%s]", calls, expectedCall)
	}
	if torn := warmer.TornNames(); len(torn) != 0 {
		t.Fatalf("busy worker teardowns = %v, want none", torn)
	}

	runner.Release()
	waitFor(t, func() bool {
		snapshot := pool.Snapshot()
		return snapshot.Idle == 2 && snapshot.Busy == 0
	})
	if err := pool.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile while idle: %v", err)
	}
	waitFor(t, func() bool {
		return len(warmer.WarmNames()) == 2
	})
	torn := warmer.TornNames()
	if len(torn) != 1 || torn[0] != firstVM {
		t.Fatalf("idle resize teardowns = %v, want [%s]", torn, firstVM)
	}

	_, workers = pool.Status(context.Background())
	if len(workers) != 1 {
		t.Fatalf("workers after resize = %+v, want 1 worker", workers)
	}
	if workers[0].Slots != nil {
		t.Fatalf("resized worker status slots = %+v, want nil single-slot view", workers[0].Slots)
	}
}

func TestReconfigureShrinksIdleWorkerSlotCount(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newFakeRunner(1)
	pool := New(testOptionsWithSlots(clock, 1, 2), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)
	firstVM := warmer.WarmNames()[0]

	pool.Reconfigure(testOptionsWithSlots(clock, 1, 1))
	if err := pool.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	waitFor(t, func() bool {
		return len(warmer.WarmNames()) == 2
	})
	torn := warmer.TornNames()
	if len(torn) != 1 || torn[0] != firstVM {
		t.Fatalf("idle shrink teardowns = %v, want [%s]", torn, firstVM)
	}
	pool.mu.Lock()
	slotCount := len(pool.states[0].slots)
	pool.mu.Unlock()
	if slotCount != 1 {
		t.Fatalf("slot count after shrink = %d, want 1", slotCount)
	}
	slotCounts := warmer.WarmSlotCounts()
	if len(slotCounts) != 2 || slotCounts[1] != 1 {
		t.Fatalf("warm slot counts = %v, want second count 1", slotCounts)
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
	if len(workers) != 1 {
		t.Fatalf("workers = %+v, want 1 worker", workers)
	}
	worker := workers[0]
	if len(worker.Slots) != 2 {
		t.Fatalf("slots = %+v, want 2 slots", worker.Slots)
	}
	slot := worker.Slots[call.SlotIndex]
	if slot.LastError != "panic: runner crashed" {
		t.Fatalf("slot last error = %q, want panic: runner crashed", slot.LastError)
	}
}

func TestJobsPerVMOneKeepsSingleInflightCapacity(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newBlockingRunner(2)
	pool := New(testOptionsWithSlots(clock, 1, 1), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)

	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo-a", JobID: 1, RunID: 10}); err != nil {
		t.Fatalf("Enqueue first job: %v", err)
	}
	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo-b", JobID: 2, RunID: 11}); err != nil {
		t.Fatalf("Enqueue second job: %v", err)
	}

	first := waitStarted(t, runner)
	waitFor(t, func() bool {
		snapshot := pool.Snapshot()
		return snapshot.Busy == 1 && snapshot.Idle == 0 && snapshot.Queued == 1 && !snapshot.Ready
	})
	select {
	case second := <-runner.started:
		t.Fatalf("second job started early on slot %d: %+v", second.SlotIndex, second)
	case <-time.After(50 * time.Millisecond):
	}
	if first.RunnerName != first.VMName {
		t.Fatalf("single-slot runner name = %q, want VM name %q", first.RunnerName, first.VMName)
	}
	if first.SlotIndex != 0 || first.SlotCount != 1 {
		t.Fatalf("single-slot call used slot %d of %d, want 0 of 1", first.SlotIndex, first.SlotCount)
	}

	runner.Release()
	second := waitStarted(t, runner)
	if second.VMName != first.VMName {
		t.Fatalf("second job VM = %q, want reused VM %q", second.VMName, first.VMName)
	}
	runner.Release()
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

	waitFor(t, func() bool {
		return runner.Active() == 1
	})
	if torn := warmer.TornNames(); len(torn) != 0 {
		t.Fatalf("teardowns before cotenant finishes = %v, want none", torn)
	}
	snapshot := pool.Snapshot()
	if snapshot.Busy != 1 {
		t.Fatalf("busy slots after cancel = %d, want 1", snapshot.Busy)
	}

	runner.Release()
}

func TestReconcileReapsOneExpiredSlotWithoutStoppingCotenant(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptionsWithSlots(clock, 1, 2)
	options.PickupTimeout = time.Minute
	options.MaxBind = 2 * time.Minute
	warmer := newFakeWarmer()
	runner := newBlockingRunner(2)
	prober := newFakeActiveJobProber(map[string]bool{})
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

	pool.mu.Lock()
	expiredSlot := first.SlotIndex
	pool.states[0].slots[expiredSlot].boundAt = now.Add(-options.MaxBind - time.Second)
	pool.states[0].slots[second.SlotIndex].boundAt = now.Add(-30 * time.Second)
	pool.mu.Unlock()

	if err := pool.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	waitFor(t, func() bool {
		return runner.Active() == 1
	})
	if torn := warmer.TornNames(); len(torn) != 0 {
		t.Fatalf("teardowns before cotenant finishes = %v, want none", torn)
	}
	calls := prober.Calls()
	wantCall := probeKey(first.VMName, expiredSlot, 2)
	if len(calls) != 1 || calls[0] != wantCall {
		t.Fatalf("prober calls = %v, want [%s]", calls, wantCall)
	}

	runner.Release()
}

func TestCancelRunReapsMatchingBusyWorker(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	pool := New(testOptions(clock, 2), newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), nil)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-time.Minute), 1001, 42, func() {
		cancelCount++
	})
	pool.states[1] = busyWorkerState("vm-other", now.Add(-time.Hour), now.Add(-time.Minute), 1002, 43, nil)
	pool.mu.Unlock()

	pool.CancelRun(99)
	pool.mu.Lock()
	if pool.states[0].recycle {
		t.Fatal("worker recycle = true after non-matching CancelRun, want false")
	}
	if cancelCount != 0 {
		t.Fatalf("cancel count = %d after non-matching CancelRun, want 0", cancelCount)
	}
	pool.mu.Unlock()

	pool.CancelRun(1001)
	pool.mu.Lock()
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
	pool.mu.Unlock()

	pool.CancelRun(1001)
	if cancelCount != 1 {
		t.Fatalf("cancel count after duplicate CancelRun = %d, want 1", cancelCount)
	}
}

func TestCancelRunIgnoresSiblingJobWithSameRunID(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newBlockingRunner(1)
	pool := New(testOptions(clock, 1), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)
	firstVM := warmer.WarmNames()[0]

	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo", JobID: 1001, RunID: 42}); err != nil {
		t.Fatalf("Enqueue job: %v", err)
	}
	waitStarted(t, runner)

	pool.CancelRun(1002)

	if torn := warmer.TornNames(); len(torn) != 0 {
		t.Fatalf("teardowns after sibling cancel = %v, want none", torn)
	}

	pool.CancelRun(1001)

	waitFor(t, func() bool {
		return len(warmer.WarmNames()) == 2
	})
	torn := warmer.TornNames()
	if len(torn) != 1 || torn[0] != firstVM {
		t.Fatalf("teardowns after matching cancel = %v, want [%s]", torn, firstVM)
	}
}

func TestReconcileReapsBusyWorkerAfterPickupTimeoutWithoutActiveJob(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	prober := newFakeActiveJobProber(map[string]bool{"vm-busy": false})
	prober.SetCPUActivity(probeKey("vm-busy", 0, 1), 90)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0
	logs := captureTestLogs(t)

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 0, 42, func() {
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
	if pool.states[0].slots[0].jobCancel != nil {
		t.Fatal("busy worker jobCancel is still set")
	}
	if cancelCount != 1 {
		t.Fatalf("cancel count = %d, want 1", cancelCount)
	}
	calls := prober.Calls()
	if len(calls) != 1 || calls[0] != "vm-busy" {
		t.Fatalf("prober calls = %v, want [vm-busy]", calls)
	}
	if cpuCalls := prober.CPUCalls(); len(cpuCalls) != 0 {
		t.Fatalf("cpu calls = %v, want none", cpuCalls)
	}
	if !strings.Contains(logs.String(), "runnerpool reaping worker (no active job process)") {
		t.Fatalf("logs = %q, want no active job reap warning", logs.String())
	}
}

func TestReconcileWarnsAndRequestsNoActiveJobRecycleOncePerBind(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	prober := newFakeActiveJobProber(map[string]bool{"vm-busy": false})
	prober.SetCPUActivity(probeKey("vm-busy", 0, 1), 90)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0
	repeatedCancelCount := 0
	logs := captureTestLogs(t)

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 0, 42, func() {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}

	pool.mu.Lock()
	pool.states[0].slots[0].jobCancel = func() {
		repeatedCancelCount++
	}
	pool.mu.Unlock()

	clock.Advance(time.Second)
	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}

	warnMessage := "runnerpool reaping worker (no active job process)"
	if warnCount := strings.Count(logs.String(), warnMessage); warnCount != 1 {
		t.Fatalf("no active job warning count = %d, want 1; logs = %q", warnCount, logs.String())
	}
	if cancelCount != 1 {
		t.Fatalf("cancel count = %d, want 1", cancelCount)
	}
	if repeatedCancelCount != 0 {
		t.Fatalf("repeated cancel count = %d, want 0", repeatedCancelCount)
	}

	pool.mu.Lock()
	pool.states[0].recycle = false
	pool.states[0].slots[0].boundAt = clock.Now().Add(-options.PickupTimeout - time.Second)
	pool.states[0].slots[0].jobID = 1002
	pool.states[0].slots[0].runID = 43
	pool.states[0].slots[0].jobCancel = func() {
		cancelCount++
	}
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("fresh bind Reconcile: %v", err)
	}

	if warnCount := strings.Count(logs.String(), warnMessage); warnCount != 2 {
		t.Fatalf("no active job warning count after fresh bind = %d, want 2; logs = %q", warnCount, logs.String())
	}
	if cancelCount != 2 {
		t.Fatalf("cancel count after fresh bind = %d, want 2", cancelCount)
	}
}

func TestReconcileKeepsBusyWorkerWithActiveJobAfterPickupTimeout(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	prober := newFakeActiveJobProber(map[string]bool{"vm-busy": true})
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 0, 42, func() {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.states[0].recycle {
		t.Fatal("busy worker recycle = true, want false")
	}
	if pool.states[0].slots[0].jobCancel == nil {
		t.Fatal("busy worker jobCancel = nil, want still set")
	}
	if cancelCount != 0 {
		t.Fatalf("cancel count = %d, want 0", cancelCount)
	}
	calls := prober.Calls()
	if len(calls) != 1 || calls[0] != "vm-busy" {
		t.Fatalf("prober calls = %v, want [vm-busy]", calls)
	}
}

func TestReconcileReapsStalledBusyWorkerWhenStallReapEnabled(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	options.StallTimeout = 2 * time.Minute
	options.StallReap = true
	prober := newFakeActiveJobProber(map[string]bool{"vm-busy": true})
	prober.SetCPUActivity(probeKey("vm-busy", 0, 1), 0)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0
	logs := captureTestLogs(t)

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 1001, 42, func() {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	clock.Advance(options.StallTimeout)
	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if !pool.states[0].recycle {
		t.Fatal("stalled worker recycle = false, want true")
	}
	if pool.states[0].slots[0].jobCancel != nil {
		t.Fatal("stalled worker jobCancel is still set")
	}
	if cancelCount != 1 {
		t.Fatalf("cancel count = %d, want 1", cancelCount)
	}
	if !strings.Contains(logs.String(), "runnerpool worker stalled (no cpu progress)") {
		t.Fatalf("logs = %q, want stalled worker warning", logs.String())
	}
}

func TestReconcileUsesSlotProbeKeyForMultiSlotCPUActivity(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptionsWithSlots(clock, 1, 2)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	options.StallTimeout = 2 * time.Minute
	options.StallReap = true
	slotIndex := 1
	slotCount := 2
	key := probeKey("vm-busy", slotIndex, slotCount)
	prober := newFakeActiveJobProber(map[string]bool{key: true})
	prober.SetCPUActivity(key, 90)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = workerState{
		vm:        &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
		bornAt:    now.Add(-time.Hour),
		idleSince: time.Time{},
		warming:   false,
		recycle:   false,
		slots:     make([]slotState, slotCount),
		lastErr:   nil,
	}
	pool.states[0].slots[slotIndex] = slotState{
		boundAt:         now.Add(-options.PickupTimeout - options.StallTimeout - time.Second),
		busy:            true,
		jobID:           1001,
		runID:           42,
		jobCancel:       func() { cancelCount++ },
		cpuStalledSince: now.Add(-options.StallTimeout),
		lastErr:         nil,
	}
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.states[0].recycle {
		t.Fatal("busy worker recycle = true, want false")
	}
	if !pool.states[0].slots[slotIndex].cpuStalledSince.IsZero() {
		t.Fatalf("cpu stalled since = %v, want zero", pool.states[0].slots[slotIndex].cpuStalledSince)
	}
	if cancelCount != 0 {
		t.Fatalf("cancel count = %d, want 0", cancelCount)
	}
	if calls := prober.Calls(); len(calls) != 1 || calls[0] != key {
		t.Fatalf("prober calls = %v, want [%s]", calls, key)
	}
	if cpuCalls := prober.CPUCalls(); len(cpuCalls) != 1 || cpuCalls[0] != key {
		t.Fatalf("cpu calls = %v, want [%s]", cpuCalls, key)
	}
}

func TestReconcileUsesSlotProbeKeyForMultiSlotActiveJob(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptionsWithSlots(clock, 1, 2)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	options.StallTimeout = 2 * time.Minute
	options.StallReap = true
	slotIndex := 1
	slotCount := 2
	key := probeKey("vm-busy", slotIndex, slotCount)
	prober := newFakeActiveJobProber(map[string]bool{})
	prober.SetActive("vm-busy", slotIndex, slotCount, true)
	prober.SetCPUActivity(key, 90)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = workerState{
		vm:        &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
		bornAt:    now.Add(-time.Hour),
		idleSince: time.Time{},
		warming:   false,
		recycle:   false,
		slots:     make([]slotState, slotCount),
		lastErr:   nil,
	}
	pool.states[0].slots[slotIndex] = slotState{
		boundAt:   now.Add(-options.PickupTimeout - time.Second),
		busy:      true,
		jobID:     1001,
		runID:     42,
		jobCancel: func() { cancelCount++ },
		lastErr:   nil,
	}
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.states[0].recycle {
		t.Fatal("busy worker recycle = true, want false")
	}
	if pool.states[0].slots[slotIndex].jobCancel == nil {
		t.Fatal("busy worker jobCancel = nil, want still set")
	}
	if cancelCount != 0 {
		t.Fatalf("cancel count = %d, want 0", cancelCount)
	}
	if calls := prober.Calls(); len(calls) != 1 || calls[0] != key {
		t.Fatalf("prober calls = %v, want [%s]", calls, key)
	}
	if cpuCalls := prober.CPUCalls(); len(cpuCalls) != 1 || cpuCalls[0] != key {
		t.Fatalf("cpu calls = %v, want [%s]", cpuCalls, key)
	}
}

func TestReconcileWarnsAndRequestsStallRecycleOncePerBind(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	options.StallTimeout = 2 * time.Minute
	options.StallReap = true
	prober := newFakeActiveJobProber(map[string]bool{"vm-busy": true})
	prober.SetCPUActivity(probeKey("vm-busy", 0, 1), 0)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0
	repeatedCancelCount := 0
	logs := captureTestLogs(t)

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 1001, 42, func() {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	clock.Advance(options.StallTimeout)
	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}

	pool.mu.Lock()
	pool.states[0].slots[0].jobCancel = func() {
		repeatedCancelCount++
	}
	pool.mu.Unlock()

	clock.Advance(time.Second)
	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("third Reconcile: %v", err)
	}

	warnMessage := "runnerpool worker stalled (no cpu progress)"
	if warnCount := strings.Count(logs.String(), warnMessage); warnCount != 1 {
		t.Fatalf("stall warning count = %d, want 1; logs = %q", warnCount, logs.String())
	}
	if cancelCount != 1 {
		t.Fatalf("cancel count = %d, want 1", cancelCount)
	}
	if repeatedCancelCount != 0 {
		t.Fatalf("repeated cancel count = %d, want 0", repeatedCancelCount)
	}

	pool.mu.Lock()
	newBoundAt := clock.Now().Add(-options.PickupTimeout - options.StallTimeout - time.Second)
	pool.states[0].recycle = false
	pool.states[0].slots[0].boundAt = newBoundAt
	pool.states[0].slots[0].jobID = 1002
	pool.states[0].slots[0].runID = 43
	pool.states[0].slots[0].jobCancel = func() {
		cancelCount++
	}
	pool.states[0].slots[0].cpuStalledSince = clock.Now().Add(-options.StallTimeout)
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("fresh bind Reconcile: %v", err)
	}

	if warnCount := strings.Count(logs.String(), warnMessage); warnCount != 2 {
		t.Fatalf("stall warning count after fresh bind = %d, want 2; logs = %q", warnCount, logs.String())
	}
	if cancelCount != 2 {
		t.Fatalf("cancel count after fresh bind = %d, want 2", cancelCount)
	}
}

func TestReconcileLogsStalledBusyWorkerWhenStallReapDisabled(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	options.StallTimeout = 2 * time.Minute
	options.StallReap = false
	prober := newFakeActiveJobProber(map[string]bool{"vm-busy": true})
	prober.SetCPUActivity(probeKey("vm-busy", 0, 1), 0)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0
	logs := captureTestLogs(t)

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 1001, 42, func() {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	clock.Advance(options.StallTimeout)
	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.states[0].recycle {
		t.Fatal("stalled worker recycle = true with stall reap disabled, want false")
	}
	if pool.states[0].slots[0].jobCancel == nil {
		t.Fatal("stalled worker jobCancel = nil, want still set")
	}
	if cancelCount != 0 {
		t.Fatalf("cancel count = %d, want 0", cancelCount)
	}
	if !strings.Contains(logs.String(), "runnerpool worker stalled (no cpu progress)") {
		t.Fatalf("logs = %q, want stalled worker warning", logs.String())
	}
}

func TestMaybeWarnStalledBusyWorkerSnapshotsOptionsDuringReconfigure(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Nanosecond
	options.StallTimeout = time.Nanosecond
	options.StallReap = true
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), nil)

	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	boundAt := now.Add(-time.Minute)
	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), boundAt, 1001, 42, nil)
	pool.states[0].slots[0].cpuStalledSince = now.Add(-time.Minute)
	pool.mu.Unlock()

	const iterationCount = 500
	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	go func() {
		defer waitGroup.Done()
		<-start
		for i := range iterationCount {
			nextOptions := options
			if i%2 == 0 {
				nextOptions.StallTimeout = time.Nanosecond
				nextOptions.StallReap = true
			} else {
				nextOptions.StallTimeout = time.Hour
				nextOptions.StallReap = false
			}
			pool.Reconfigure(nextOptions)
		}
	}()

	go func() {
		defer waitGroup.Done()
		<-start
		for i := range iterationCount {
			currentBoundAt := boundAt.Add(time.Duration(i) * time.Nanosecond)
			currentJobID := int64(1001 + i)
			currentRunID := int64(42 + i)

			pool.mu.Lock()
			pool.states[0].recycle = false
			pool.states[0].slots[0] = slotState{
				boundAt:         currentBoundAt,
				busy:            true,
				jobID:           currentJobID,
				runID:           currentRunID,
				jobCancel:       nil,
				cpuStalledSince: now.Add(-time.Minute),
				stallWarnedAt:   time.Time{},
				reapWarnedAt:    time.Time{},
				lastErr:         nil,
			}
			pool.mu.Unlock()

			candidate := busyCandidate{
				index:     0,
				slotIndex: 0,
				slotCount: 1,
				vm:        &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
				boundAt:   currentBoundAt,
				jobID:     currentJobID,
				runID:     currentRunID,
				now:       now,
			}
			pool.maybeWarnStalledBusyWorker(context.Background(), candidate, time.Minute, now.Add(-time.Minute), 0)
		}
	}()

	close(start)
	waitGroup.Wait()
}

func TestReconcileKeepsProgressingBusyWorkerPastStallTimeout(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	options.StallTimeout = 2 * time.Minute
	options.StallReap = true
	prober := newFakeActiveJobProber(map[string]bool{"vm-busy": true})
	prober.SetCPUActivity(probeKey("vm-busy", 0, 1), 90)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 1001, 42, func() {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	clock.Advance(options.StallTimeout)
	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.states[0].recycle {
		t.Fatal("progressing worker recycle = true, want false")
	}
	if !pool.states[0].slots[0].cpuStalledSince.IsZero() {
		t.Fatalf("cpu stalled since = %v, want zero", pool.states[0].slots[0].cpuStalledSince)
	}
	if cancelCount != 0 {
		t.Fatalf("cancel count = %d, want 0", cancelCount)
	}
}

func TestReconcileResetsStallClockWhenCPUProgressReturns(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	options.StallTimeout = 2 * time.Minute
	options.StallReap = true
	prober := newFakeActiveJobProber(map[string]bool{"vm-busy": true})
	prober.SetCPUActivity(probeKey("vm-busy", 0, 1), 0)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 1001, 42, func() {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	clock.Advance(time.Minute)
	prober.SetCPUActivity(probeKey("vm-busy", 0, 1), 90)
	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	pool.mu.Lock()
	if !pool.states[0].slots[0].cpuStalledSince.IsZero() {
		t.Fatalf("cpu stalled since after progress = %v, want zero", pool.states[0].slots[0].cpuStalledSince)
	}
	pool.mu.Unlock()

	clock.Advance(options.StallTimeout)
	prober.SetCPUActivity(probeKey("vm-busy", 0, 1), 0)
	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("third Reconcile: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.states[0].recycle {
		t.Fatal("worker recycle = true after reset stall clock, want false")
	}
	if !pool.states[0].slots[0].cpuStalledSince.Equal(clock.Now()) {
		t.Fatalf("cpu stalled since = %v, want %v", pool.states[0].slots[0].cpuStalledSince, clock.Now())
	}
	if cancelCount != 0 {
		t.Fatalf("cancel count = %d, want 0", cancelCount)
	}
}

func TestReconcileKeepsBusyWorkerWhenBindingChangesBeforeRecycleApply(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	prober := newFakeActiveJobProber(map[string]bool{"vm-busy": false})
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	oldCancelCount := 0
	newCancelCount := 0
	newBoundAt := now.Add(-30 * time.Second)

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 0, 42, func() {
		oldCancelCount++
	})
	pool.mu.Unlock()

	prober.onProbe = func() {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		pool.states[0].slots[0].boundAt = newBoundAt
		pool.states[0].slots[0].runID = 43
		pool.states[0].slots[0].jobCancel = func() {
			newCancelCount++
		}
	}

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.states[0].recycle {
		t.Fatal("rebound worker recycle = true, want false")
	}
	if pool.states[0].slots[0].runID != 43 {
		t.Fatalf("rebound worker run id = %d, want 43", pool.states[0].slots[0].runID)
	}
	if !pool.states[0].slots[0].boundAt.Equal(newBoundAt) {
		t.Fatalf("rebound worker boundAt = %v, want %v", pool.states[0].slots[0].boundAt, newBoundAt)
	}
	if pool.states[0].slots[0].jobCancel == nil {
		t.Fatal("rebound worker jobCancel = nil, want still set")
	}
	if oldCancelCount != 0 {
		t.Fatalf("old cancel count = %d, want 0", oldCancelCount)
	}
	if newCancelCount != 0 {
		t.Fatalf("new cancel count = %d, want 0", newCancelCount)
	}
	calls := prober.Calls()
	if len(calls) != 1 || calls[0] != "vm-busy" {
		t.Fatalf("prober calls = %v, want [vm-busy]", calls)
	}
}

func TestReconcileKeepsBusyWorkerAfterPickupTimeoutWhenProbeErrors(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = time.Hour
	prober := newFakeActiveJobProber(map[string]bool{"vm-busy": false})
	prober.probeErr = errors.New("guest probe failed")
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.PickupTimeout-time.Second), 0, 42, func() {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.states[0].recycle {
		t.Fatal("busy worker recycle = true after probe error, want false")
	}
	if pool.states[0].slots[0].jobCancel == nil {
		t.Fatal("busy worker jobCancel = nil, want still set")
	}
	if cancelCount != 0 {
		t.Fatalf("cancel count = %d, want 0", cancelCount)
	}
	calls := prober.Calls()
	if len(calls) != 1 || calls[0] != "vm-busy" {
		t.Fatalf("prober calls = %v, want [vm-busy]", calls)
	}
}

func TestReconcileKeepsBusyWorkerPastMaxBindWithActiveJob(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = 2 * time.Minute
	prober := newFakeActiveJobProber(map[string]bool{"vm-busy": true})
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.MaxBind-time.Second), 0, 42, func() {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.states[0].recycle {
		t.Fatal("busy worker recycle = true, want false")
	}
	if pool.states[0].slots[0].jobCancel == nil {
		t.Fatal("busy worker jobCancel = nil, want still set")
	}
	if cancelCount != 0 {
		t.Fatalf("cancel count = %d, want 0", cancelCount)
	}
	calls := prober.Calls()
	if len(calls) != 1 || calls[0] != "vm-busy" {
		t.Fatalf("prober calls = %v, want [vm-busy]", calls)
	}
}

func TestReconcileReapsBusyWorkerPastMaxBindWhenProbeErrors(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = 2 * time.Minute
	prober := newFakeActiveJobProber(map[string]bool{"vm-busy": false})
	prober.probeErr = errors.New("guest probe failed")
	prober.SetCPUActivity(probeKey("vm-busy", 0, 1), 90)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0
	logs := captureTestLogs(t)

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.MaxBind-time.Second), 0, 42, func() {
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
	if pool.states[0].slots[0].jobCancel != nil {
		t.Fatal("busy worker jobCancel is still set")
	}
	if cancelCount != 1 {
		t.Fatalf("cancel count = %d, want 1", cancelCount)
	}
	calls := prober.Calls()
	if len(calls) != 1 || calls[0] != "vm-busy" {
		t.Fatalf("prober calls = %v, want [vm-busy]", calls)
	}
	if cpuCalls := prober.CPUCalls(); len(cpuCalls) != 0 {
		t.Fatalf("cpu calls = %v, want none", cpuCalls)
	}
	if !strings.Contains(logs.String(), "runnerpool reaping worker (probe error past max_bind)") {
		t.Fatalf("logs = %q, want probe error reap warning", logs.String())
	}
}

func TestReconcileWarnsAndRequestsProbeErrorMaxBindRecycleOncePerBind(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	options := testOptions(clock, 1)
	options.PickupTimeout = time.Minute
	options.MaxBind = 2 * time.Minute
	prober := newFakeActiveJobProber(map[string]bool{"vm-busy": false})
	prober.probeErr = errors.New("guest probe failed")
	prober.SetCPUActivity(probeKey("vm-busy", 0, 1), 90)
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0
	repeatedCancelCount := 0
	logs := captureTestLogs(t)

	pool.mu.Lock()
	pool.states[0] = busyWorkerState("vm-busy", now.Add(-time.Hour), now.Add(-options.MaxBind-time.Second), 0, 42, func() {
		cancelCount++
	})
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}

	pool.mu.Lock()
	pool.states[0].slots[0].jobCancel = func() {
		repeatedCancelCount++
	}
	pool.mu.Unlock()

	clock.Advance(time.Second)
	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}

	warnMessage := "runnerpool reaping worker (probe error past max_bind)"
	if warnCount := strings.Count(logs.String(), warnMessage); warnCount != 1 {
		t.Fatalf("probe error warning count = %d, want 1; logs = %q", warnCount, logs.String())
	}
	if cancelCount != 1 {
		t.Fatalf("cancel count = %d, want 1", cancelCount)
	}
	if repeatedCancelCount != 0 {
		t.Fatalf("repeated cancel count = %d, want 0", repeatedCancelCount)
	}

	pool.mu.Lock()
	pool.states[0].recycle = false
	pool.states[0].slots[0].boundAt = clock.Now().Add(-options.MaxBind - time.Second)
	pool.states[0].slots[0].jobID = 1002
	pool.states[0].slots[0].runID = 43
	pool.states[0].slots[0].jobCancel = func() {
		cancelCount++
	}
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("fresh bind Reconcile: %v", err)
	}

	if warnCount := strings.Count(logs.String(), warnMessage); warnCount != 2 {
		t.Fatalf("probe error warning count after fresh bind = %d, want 2; logs = %q", warnCount, logs.String())
	}
	if cancelCount != 2 {
		t.Fatalf("cancel count after fresh bind = %d, want 2", cancelCount)
	}
}

func TestCancelRunTearsDownAndRewarmsWorker(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newBlockingRunner(1)
	pool := New(testOptions(clock, 1), warmer, runner, newFakeGitHub(), nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)
	firstVM := warmer.WarmNames()[0]

	if err := pool.Enqueue(ctx, Job{Repo: "owner/repo", JobID: 1, RunID: 42}); err != nil {
		t.Fatalf("Enqueue job: %v", err)
	}
	waitStarted(t, runner)

	pool.CancelRun(1)

	waitFor(t, func() bool {
		return len(warmer.WarmNames()) == 2
	})
	torn := warmer.TornNames()
	if len(torn) != 1 || torn[0] != firstVM {
		t.Fatalf("torn VMs = %v, want [%s]", torn, firstVM)
	}
	waitFor(t, func() bool {
		return pool.Snapshot().Idle == 1
	})
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

	waitFor(t, func() bool {
		return len(runner.Calls()) == 3
	})
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
	if pool.Ready() {
		t.Fatal("Ready should be false when all workers are busy and the queue is backed up")
	}
	for i := 0; i < 8; i++ {
		runner.Release()
	}
	waitFor(t, func() bool {
		return len(runner.Calls()) == 8
	})
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
	waitFor(t, func() bool {
		return len(warmer.WarmNames()) == 2
	})
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
	warmer.SetCheckError(firstVM, errors.New("vsock failed"))

	if err := pool.Reconcile(ctx); err == nil {
		t.Fatal("Reconcile should report the health failure")
	}
	waitFor(t, func() bool {
		return len(warmer.WarmNames()) == 2
	})
	torn := warmer.TornNames()
	if len(torn) != 1 || torn[0] != firstVM {
		t.Fatalf("torn VMs = %v, want [%s]", torn, firstVM)
	}
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
	waitFor(t, func() bool {
		return len(warmer.WarmNames()) == 2
	})
	torn := warmer.TornNames()
	if len(torn) != 1 || torn[0] != firstVM {
		t.Fatalf("torn VMs = %v, want [%s]", torn, firstVM)
	}
}

func TestReconcileReplacesIdleVMWithStaleGitHubSlotRunner(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newFakeRunner(1)
	github := newFakeGitHub()
	pool := New(testOptionsWithSlots(clock, 1, 2), warmer, runner, github, nil)
	ctx := startTestPool(t, pool)
	waitFor(t, pool.Ready)
	firstVM := warmer.WarmNames()[0]
	github.SetRunners("owner/repo", []ghapp.Runner{{Name: firstVM + "-slot-1", Status: "offline"}})

	if err := pool.Reconcile(ctx); err == nil {
		t.Fatal("Reconcile should report the stale GitHub slot runner")
	}
	waitFor(t, func() bool {
		return len(warmer.WarmNames()) == 2
	})
	torn := warmer.TornNames()
	if len(torn) != 1 || torn[0] != firstVM {
		t.Fatalf("torn VMs = %v, want [%s]", torn, firstVM)
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

func TestReconcileListsInstalledReposOnceForMultipleIdleCandidates(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	warmer := newFakeWarmer()
	runner := newFakeRunner(1)
	github := newFakeGitHub()
	github.installedRepos = []string{"owner/repo-a", "owner/repo-b"}
	pool := New(testOptions(clock, 3), warmer, runner, github, nil)

	pool.mu.Lock()
	pool.started = true
	for i := range pool.states {
		vmName := fmt.Sprintf("vm-idle-%d", i)
		pool.states[i] = idleWorkerState(vmName, now.Add(-time.Hour), now.Add(-time.Minute))
		warmer.alive[vmName] = true
	}
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if calls := github.InstalledCalls(); calls != 1 {
		t.Fatalf("installed repo calls = %d, want 1", calls)
	}
	runnerCalls := github.RunnerCalls()
	if len(runnerCalls) != 6 {
		t.Fatalf("runner calls = %v, want 6 calls", runnerCalls)
	}
}

func TestReconcileSkipsRegistrationLeakCheckWhenInstalledRepoListingFails(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	warmer := newFakeWarmer()
	runner := newFakeRunner(1)
	github := newFakeGitHub()
	github.err = errors.New("installations unavailable")
	pool := New(testOptions(clock, 2), warmer, runner, github, nil)

	pool.mu.Lock()
	pool.started = true
	for i := range pool.states {
		vmName := fmt.Sprintf("vm-idle-%d", i)
		pool.states[i] = idleWorkerState(vmName, now.Add(-time.Hour), now.Add(-time.Minute))
		warmer.alive[vmName] = true
	}
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if calls := github.InstalledCalls(); calls != 1 {
		t.Fatalf("installed repo calls = %d, want 1", calls)
	}
	if runnerCalls := github.RunnerCalls(); len(runnerCalls) != 0 {
		t.Fatalf("runner calls = %v, want none", runnerCalls)
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	for i, state := range pool.states {
		if state.recycle {
			t.Fatalf("state %d recycle = true, want false", i)
		}
	}
}

func TestShutdownTearsDownAllWorkerVMs(t *testing.T) {
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

	torn := warmer.TornNames()
	sort.Strings(torn)
	want := warmer.WarmNames()
	sort.Strings(want)
	if fmt.Sprint(torn) != fmt.Sprint(want) {
		t.Fatalf("torn VMs = %v, want %v", torn, want)
	}
}
