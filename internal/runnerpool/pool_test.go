package runnerpool

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
	"goodkind.io/gha-mac-broker/internal/ghapp"
)

type warmCall struct {
	Image string
	ID    string
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

func (w *fakeWarmer) Warm(_ context.Context, image string, id string) (*broker.WarmVM, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, warmCall{Image: image, ID: id})
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
}

type fakeRunner struct {
	mu        sync.Mutex
	calls     []runCall
	started   chan runCall
	release   chan struct{}
	active    int
	maxActive int
}

func newFakeRunner(buffer int) *fakeRunner {
	return &fakeRunner{
		mu:        sync.Mutex{},
		calls:     nil,
		started:   make(chan runCall, buffer),
		release:   nil,
		active:    0,
		maxActive: 0,
	}
}

func newBlockingRunner(buffer int) *fakeRunner {
	runner := newFakeRunner(buffer)
	runner.release = make(chan struct{}, buffer)
	return runner
}

func (r *fakeRunner) RunJob(ctx context.Context, vm *broker.WarmVM, repo string, runnerName string) error {
	call := runCall{Repo: repo, RunnerName: runnerName, VMName: vm.Name}
	r.mu.Lock()
	r.calls = append(r.calls, call)
	r.active++
	if r.active > r.maxActive {
		r.maxActive = r.active
	}
	r.mu.Unlock()
	r.started <- call
	if r.release != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.release:
		}
	}
	r.mu.Lock()
	r.active--
	r.mu.Unlock()
	return nil
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

type fakeGitHub struct {
	mu             sync.Mutex
	installedRepos []string
	runners        map[string][]ghapp.Runner
	calls          []string
	err            error
}

func newFakeGitHub() *fakeGitHub {
	return &fakeGitHub{
		mu:             sync.Mutex{},
		installedRepos: []string{"owner/repo"},
		runners:        make(map[string][]ghapp.Runner),
		calls:          nil,
		err:            nil,
	}
}

func (g *fakeGitHub) ListInstalledRepos(_ context.Context) ([]string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
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

type fakeActiveJobProber struct {
	mu       sync.Mutex
	active   map[string]bool
	calls    []string
	probeErr error
	onProbe  func()
}

func newFakeActiveJobProber(active map[string]bool) *fakeActiveJobProber {
	return &fakeActiveJobProber{
		mu:       sync.Mutex{},
		active:   active,
		calls:    nil,
		probeErr: nil,
		onProbe:  nil,
	}
}

func (p *fakeActiveJobProber) HasActiveJob(_ context.Context, vm *broker.WarmVM) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, vm.Name)
	if p.probeErr != nil {
		return false, p.probeErr
	}
	if p.onProbe != nil {
		p.onProbe()
	}
	return p.active[vm.Name], nil
}

func (p *fakeActiveJobProber) Calls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

func (p *fakeActiveJobProber) SetActive(vmName string, active bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active[vmName] = active
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
		Image:          "image-a",
		MaxIdle:        2 * time.Hour,
		MaxAge:         24 * time.Hour,
		RunToken:       "test",
		WarmRetryDelay: time.Millisecond,
		Now:            clock.Now,
	}
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
	pool.states[0] = workerState{
		vm:        &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
		bornAt:    now.Add(-time.Hour),
		idleSince: time.Time{},
		warming:   false,
		busy:      true,
		recycle:   false,
		boundAt:   now.Add(-2 * time.Minute),
		runID:     42,
		lastErr:   nil,
	}
	pool.states[1] = workerState{
		vm:        &broker.WarmVM{Name: "vm-idle", Image: "image-a"},
		bornAt:    now.Add(-time.Hour),
		idleSince: now.Add(-time.Minute),
		warming:   false,
		busy:      false,
		recycle:   false,
		boundAt:   time.Time{},
		runID:     0,
		lastErr:   nil,
	}
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

func TestCancelRunReapsMatchingBusyWorker(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	clock := newMutableClock(now)
	pool := New(testOptions(clock, 2), newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), nil)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = workerState{
		vm:        &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
		bornAt:    now.Add(-time.Hour),
		idleSince: time.Time{},
		warming:   false,
		busy:      true,
		recycle:   false,
		boundAt:   now.Add(-time.Minute),
		jobID:     1001,
		runID:     42,
		jobCancel: func() {
			cancelCount++
		},
		lastErr: nil,
	}
	pool.states[1] = workerState{
		vm:        &broker.WarmVM{Name: "vm-other", Image: "image-a"},
		bornAt:    now.Add(-time.Hour),
		idleSince: time.Time{},
		warming:   false,
		busy:      true,
		recycle:   false,
		boundAt:   now.Add(-time.Minute),
		jobID:     1002,
		runID:     43,
		lastErr:   nil,
	}
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
	if pool.states[0].jobCancel != nil {
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
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = workerState{
		vm:        &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
		bornAt:    now.Add(-time.Hour),
		idleSince: time.Time{},
		warming:   false,
		busy:      true,
		recycle:   false,
		boundAt:   now.Add(-options.PickupTimeout - time.Second),
		runID:     42,
		jobCancel: func() {
			cancelCount++
		},
		lastErr: nil,
	}
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if !pool.states[0].recycle {
		t.Fatal("busy worker recycle = false, want true")
	}
	if pool.states[0].jobCancel != nil {
		t.Fatal("busy worker jobCancel is still set")
	}
	if cancelCount != 1 {
		t.Fatalf("cancel count = %d, want 1", cancelCount)
	}
	calls := prober.Calls()
	if len(calls) != 1 || calls[0] != "vm-busy" {
		t.Fatalf("prober calls = %v, want [vm-busy]", calls)
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
	pool.states[0] = workerState{
		vm:        &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
		bornAt:    now.Add(-time.Hour),
		idleSince: time.Time{},
		warming:   false,
		busy:      true,
		recycle:   false,
		boundAt:   now.Add(-options.PickupTimeout - time.Second),
		runID:     42,
		jobCancel: func() {
			cancelCount++
		},
		lastErr: nil,
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
	if pool.states[0].jobCancel == nil {
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
	pool.states[0] = workerState{
		vm:        &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
		bornAt:    now.Add(-time.Hour),
		idleSince: time.Time{},
		warming:   false,
		busy:      true,
		recycle:   false,
		boundAt:   now.Add(-options.PickupTimeout - time.Second),
		runID:     42,
		jobCancel: func() {
			oldCancelCount++
		},
		lastErr: nil,
	}
	pool.mu.Unlock()

	prober.onProbe = func() {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		pool.states[0].boundAt = newBoundAt
		pool.states[0].runID = 43
		pool.states[0].jobCancel = func() {
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
	if pool.states[0].runID != 43 {
		t.Fatalf("rebound worker run id = %d, want 43", pool.states[0].runID)
	}
	if !pool.states[0].boundAt.Equal(newBoundAt) {
		t.Fatalf("rebound worker boundAt = %v, want %v", pool.states[0].boundAt, newBoundAt)
	}
	if pool.states[0].jobCancel == nil {
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
	pool.states[0] = workerState{
		vm:        &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
		bornAt:    now.Add(-time.Hour),
		idleSince: time.Time{},
		warming:   false,
		busy:      true,
		recycle:   false,
		boundAt:   now.Add(-options.PickupTimeout - time.Second),
		runID:     42,
		jobCancel: func() {
			cancelCount++
		},
		lastErr: nil,
	}
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.states[0].recycle {
		t.Fatal("busy worker recycle = true after probe error, want false")
	}
	if pool.states[0].jobCancel == nil {
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
	pool.states[0] = workerState{
		vm:        &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
		bornAt:    now.Add(-time.Hour),
		idleSince: time.Time{},
		warming:   false,
		busy:      true,
		recycle:   false,
		boundAt:   now.Add(-options.MaxBind - time.Second),
		runID:     42,
		jobCancel: func() {
			cancelCount++
		},
		lastErr: nil,
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
	if pool.states[0].jobCancel == nil {
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
	pool := New(options, newFakeWarmer(), newFakeRunner(1), newFakeGitHub(), prober)
	cancelCount := 0

	pool.mu.Lock()
	pool.states[0] = workerState{
		vm:        &broker.WarmVM{Name: "vm-busy", Image: "image-a"},
		bornAt:    now.Add(-time.Hour),
		idleSince: time.Time{},
		warming:   false,
		busy:      true,
		recycle:   false,
		boundAt:   now.Add(-options.MaxBind - time.Second),
		runID:     42,
		jobCancel: func() {
			cancelCount++
		},
		lastErr: nil,
	}
	pool.mu.Unlock()

	if err := pool.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if !pool.states[0].recycle {
		t.Fatal("busy worker recycle = false, want true")
	}
	if pool.states[0].jobCancel != nil {
		t.Fatal("busy worker jobCancel is still set")
	}
	if cancelCount != 1 {
		t.Fatalf("cancel count = %d, want 1", cancelCount)
	}
	calls := prober.Calls()
	if len(calls) != 1 || calls[0] != "vm-busy" {
		t.Fatalf("prober calls = %v, want [vm-busy]", calls)
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
