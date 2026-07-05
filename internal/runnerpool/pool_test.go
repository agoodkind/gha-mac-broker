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
	mu      sync.Mutex
	runners map[string][]ghapp.Runner
	calls   []string
	err     error
}

func newFakeGitHub() *fakeGitHub {
	return &fakeGitHub{
		mu:      sync.Mutex{},
		runners: make(map[string][]ghapp.Runner),
		calls:   nil,
		err:     nil,
	}
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
		AllowedRepos:   []string{"owner/repo"},
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

func TestWorkerReusesWarmVMAcrossJobs(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newFakeRunner(2)
	pool := New(testOptions(clock, 1), warmer, runner, newFakeGitHub())
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

func TestQueueIsFIFOForSingleWorker(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC))
	warmer := newFakeWarmer()
	runner := newFakeRunner(3)
	pool := New(testOptions(clock, 1), warmer, runner, newFakeGitHub())
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
	pool := New(testOptions(clock, 3), warmer, runner, newFakeGitHub())
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
	pool := New(options, warmer, runner, newFakeGitHub())
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
	pool := New(testOptions(clock, 1), warmer, runner, newFakeGitHub())
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
	pool := New(testOptions(clock, 1), warmer, runner, github)
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
	pool := New(testOptions(clock, 3), warmer, runner, newFakeGitHub())
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
