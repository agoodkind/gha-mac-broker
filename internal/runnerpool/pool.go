// Package runnerpool runs a fixed set of persistent warm VMs against a FIFO
// queue of repo-scoped GitHub Actions jobs.
package runnerpool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
	"goodkind.io/gha-mac-broker/internal/ghapp"
)

const (
	defaultRunnerCount       = 3
	defaultWarmRetryDelay    = 5 * time.Second
	defaultReconcileInterval = time.Minute
)

// Job is one queued workflow job accepted from the webhook server.
type Job struct {
	Repo  string
	JobID int64
	RunID int64
}

// Options configures a persistent worker pool.
type Options struct {
	RunnerCount    int
	Image          string
	MaxIdle        time.Duration
	MaxAge         time.Duration
	RunToken       string
	AllowedRepos   []string
	WarmRetryDelay time.Duration
	Now            func() time.Time
}

// Snapshot is a concurrency-safe view of pool readiness and backlog.
type Snapshot struct {
	RunnerCount int
	Idle        int
	Busy        int
	Queued      int
	Healthy     bool
	Ready       bool
}

// Warmer creates, probes, and tears down warm VMs. *broker.Binder satisfies
// this interface.
type Warmer interface {
	Warm(ctx context.Context, image string, id string) (*broker.WarmVM, error)
	Teardown(ctx context.Context, vm *broker.WarmVM)
	CheckAlive(ctx context.Context, vm *broker.WarmVM) error
	SweepOrphans(ctx context.Context)
}

// Runner executes one JIT job on a warm VM. *broker.Binder satisfies this
// interface.
type Runner interface {
	RunJob(ctx context.Context, vm *broker.WarmVM, repo string, runnerName string) error
}

// RunnerLister lists GitHub runners for idle VM health checks.
type RunnerLister interface {
	ListRunners(ctx context.Context, repo string) ([]ghapp.Runner, error)
}

type workerState struct {
	vm        *broker.WarmVM
	bornAt    time.Time
	idleSince time.Time
	warming   bool
	busy      bool
	recycle   bool
	lastErr   error
}

// Pool owns N persistent warm VMs and drains a FIFO job queue through them.
type Pool struct {
	options Options
	warmer  Warmer
	runner  Runner
	github  RunnerLister

	mu           sync.Mutex
	cond         *sync.Cond
	queue        []Job
	states       []workerState
	started      bool
	shuttingDown bool
	done         chan struct{}

	cancel       context.CancelFunc
	startOnce    sync.Once
	shutdownOnce sync.Once
	wg           sync.WaitGroup
	counter      atomic.Int64
}

// New builds a persistent worker pool.
func New(options Options, warmer Warmer, runner Runner, github RunnerLister) *Pool {
	options = normalizeOptions(options)
	pool := &Pool{
		options:      options,
		warmer:       warmer,
		runner:       runner,
		github:       github,
		mu:           sync.Mutex{},
		cond:         nil,
		queue:        nil,
		states:       make([]workerState, options.RunnerCount),
		started:      false,
		shuttingDown: false,
		done:         make(chan struct{}),
		cancel:       nil,
		startOnce:    sync.Once{},
		shutdownOnce: sync.Once{},
		wg:           sync.WaitGroup{},
		counter:      atomic.Int64{},
	}
	pool.cond = sync.NewCond(&pool.mu)
	return pool
}

func normalizeOptions(options Options) Options {
	if options.RunnerCount <= 0 {
		options.RunnerCount = defaultRunnerCount
	}
	if options.WarmRetryDelay <= 0 {
		options.WarmRetryDelay = defaultWarmRetryDelay
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.RunToken == "" {
		options.RunToken = "pool"
	}
	return options
}

// Start launches the fixed worker set. Calling Start more than once is a no-op.
func (p *Pool) Start(ctx context.Context) {
	p.startOnce.Do(func() {
		poolCtx, cancel := context.WithCancel(ctx)
		p.mu.Lock()
		p.started = true
		p.cancel = cancel
		p.mu.Unlock()

		p.wg.Go(func() {
			defer recoverGoroutine(poolCtx, "runnerpool context watcher")
			<-poolCtx.Done()
			p.mu.Lock()
			p.shuttingDown = true
			p.cond.Broadcast()
			p.mu.Unlock()
		})

		for i := range p.options.RunnerCount {
			workerIndex := i
			p.wg.Go(func() {
				defer recoverGoroutine(poolCtx, "runnerpool worker")
				p.workerLoop(poolCtx, workerIndex)
			})
		}
	})
}

// StartReconcile launches periodic idle worker reconciliation.
func (p *Pool) StartReconcile(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = defaultReconcileInterval
	}
	p.wg.Go(func() {
		defer recoverGoroutine(ctx, "runnerpool reconciler")
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-p.done:
				return
			case <-ticker.C:
				if err := p.Reconcile(ctx); err != nil {
					slog.WarnContext(ctx, "runnerpool reconcile failed", "err", err)
				}
			}
		}
	})
}

// Enqueue appends job to the FIFO queue.
func (p *Pool) Enqueue(ctx context.Context, job Job) error {
	if err := ctx.Err(); err != nil {
		slog.WarnContext(ctx, "runnerpool enqueue context failed", "err", err)
		return fmt.Errorf("runnerpool: enqueue: %w", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.shuttingDown {
		slog.WarnContext(ctx, "runnerpool enqueue after shutdown")
		return errors.New("runnerpool: enqueue after shutdown")
	}
	p.queue = append(p.queue, job)
	p.cond.Signal()
	return nil
}

// Ready reports whether the pool is healthy and has free or near-free capacity.
func (p *Pool) Ready() bool {
	return p.Snapshot().Ready
}

// Snapshot returns a concurrency-safe view of the worker pool.
func (p *Pool) Snapshot() Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotLocked()
}

func (p *Pool) snapshotLocked() Snapshot {
	snapshot := Snapshot{
		RunnerCount: p.options.RunnerCount,
		Idle:        0,
		Busy:        0,
		Queued:      len(p.queue),
		Healthy:     p.started && !p.shuttingDown,
		Ready:       false,
	}
	if !p.started || p.shuttingDown {
		return snapshot
	}
	active := 0
	for _, state := range p.states {
		alive := state.vm != nil && !state.recycle && state.lastErr == nil
		if alive {
			active++
		}
		if alive && !state.busy {
			snapshot.Idle++
		}
		if state.busy {
			snapshot.Busy++
		}
	}
	// The pool is healthy when at least one worker VM can serve a job. A single
	// worker warming, recycling, or errored does not take the whole pool down,
	// so routine recycling never sheds every consumer to hosted while other VMs
	// remain live.
	snapshot.Healthy = active > 0
	snapshot.Ready = active > 0 && (snapshot.Idle > 0 || snapshot.Queued < active)
	return snapshot
}

// Reconcile recycles idle VMs that exceed hygiene limits or fail health checks.
func (p *Pool) Reconcile(ctx context.Context) error {
	candidates := p.idleCandidates()
	var recycleErrs []error
	for _, candidate := range candidates {
		recycle, err := p.shouldRecycle(ctx, candidate)
		if err != nil {
			recycleErrs = append(recycleErrs, err)
		}
		if recycle {
			p.requestRecycle(candidate.index, candidate.vm, err)
		}
	}
	err := errors.Join(recycleErrs...)
	if err != nil {
		slog.WarnContext(ctx, "runnerpool reconcile found unhealthy idle vm", "err", err)
	}
	return err
}

type idleCandidate struct {
	index     int
	vm        *broker.WarmVM
	bornAt    time.Time
	idleSince time.Time
	now       time.Time
}

func (p *Pool) idleCandidates() []idleCandidate {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.options.Now()
	candidates := make([]idleCandidate, 0, len(p.states))
	for index, state := range p.states {
		if state.vm == nil || state.warming || state.busy || state.recycle {
			continue
		}
		candidates = append(candidates, idleCandidate{
			index:     index,
			vm:        state.vm,
			bornAt:    state.bornAt,
			idleSince: state.idleSince,
			now:       now,
		})
	}
	return candidates
}

func (p *Pool) shouldRecycle(ctx context.Context, candidate idleCandidate) (bool, error) {
	if p.options.MaxIdle > 0 && !candidate.idleSince.IsZero() && candidate.now.Sub(candidate.idleSince) >= p.options.MaxIdle {
		return true, nil
	}
	if p.options.MaxAge > 0 && !candidate.bornAt.IsZero() && candidate.now.Sub(candidate.bornAt) >= p.options.MaxAge {
		return true, nil
	}
	if err := p.checkHealth(ctx, candidate.vm); err != nil {
		return true, err
	}
	return false, nil
}

func (p *Pool) checkHealth(ctx context.Context, vm *broker.WarmVM) error {
	if err := p.warmer.CheckAlive(ctx, vm); err != nil {
		slog.WarnContext(ctx, "runnerpool alive check failed", "err", err, "vm", vm.Name)
		return fmt.Errorf("runnerpool: check alive %s: %w", vm.Name, err)
	}
	if p.github == nil {
		return nil
	}
	for _, repo := range p.options.AllowedRepos {
		runners, err := p.github.ListRunners(ctx, repo)
		if err != nil {
			slog.WarnContext(ctx, "runnerpool list runners failed", "err", err, "repo", repo, "vm", vm.Name)
			return fmt.Errorf("runnerpool: list runners %s: %w", repo, err)
		}
		for _, runner := range runners {
			if runner.Name == vm.Name {
				slog.WarnContext(ctx, "runnerpool idle vm still registered", "repo", repo, "vm", vm.Name)
				return fmt.Errorf("runnerpool: idle vm %s still registered in %s", vm.Name, repo)
			}
		}
	}
	return nil
}

func (p *Pool) requestRecycle(index int, vm *broker.WarmVM, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := &p.states[index]
	if state.vm == nil || state.vm.Name != vm.Name || state.busy {
		return
	}
	state.recycle = true
	state.lastErr = err
	p.cond.Broadcast()
}

// Shutdown stops workers and tears down every VM they own.
func (p *Pool) Shutdown(ctx context.Context) {
	p.shutdownOnce.Do(func() {
		p.mu.Lock()
		p.shuttingDown = true
		if p.cancel != nil {
			p.cancel()
		}
		close(p.done)
		p.cond.Broadcast()
		p.mu.Unlock()
	})

	done := make(chan struct{})
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(ctx, "runnerpool shutdown waiter panic recovered", "err", fmt.Errorf("panic: %v", recovered))
			}
		}()
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
	case <-done:
	}
}

func (p *Pool) workerLoop(ctx context.Context, index int) {
	var vm *broker.WarmVM
	var bornAt time.Time
	for {
		if vm == nil {
			warmed, ok := p.warmWorker(ctx, index)
			if !ok {
				p.clearWorker(index)
				return
			}
			vm = warmed
			bornAt = p.options.Now()
		}

		job, recycle, ok := p.waitForJobOrRecycle(ctx, index, vm, bornAt)
		if !ok {
			p.teardownVM(context.WithoutCancel(ctx), vm)
			p.clearWorker(index)
			return
		}
		if recycle {
			p.teardownVM(context.WithoutCancel(ctx), vm)
			vm = nil
			bornAt = time.Time{}
			continue
		}

		if err := p.runner.RunJob(ctx, vm, job.Repo, vm.Name); err != nil {
			slog.WarnContext(ctx, "runnerpool job failed", "err", err, "repo", job.Repo, "job_id", job.JobID, "run_id", job.RunID, "vm", vm.Name)
		}
		p.finishJob(index)
	}
}

func (p *Pool) warmWorker(ctx context.Context, index int) (*broker.WarmVM, bool) {
	for {
		p.markWarming(index)
		id := p.nextID()
		vm, err := p.warmer.Warm(ctx, p.options.Image, id)
		if err == nil {
			if vm.Image == "" {
				vm.Image = p.options.Image
			}
			return vm, true
		}
		p.markWarmError(index, err)
		slog.WarnContext(ctx, "runnerpool warm failed", "err", err, "worker", index, "id", id)
		timer := time.NewTimer(p.options.WarmRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, false
		case <-timer.C:
		}
	}
}

func (p *Pool) markWarming(index int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := &p.states[index]
	state.vm = nil
	state.warming = true
	state.busy = false
	state.recycle = false
	p.cond.Broadcast()
}

func (p *Pool) markWarmError(index int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := &p.states[index]
	state.lastErr = err
	state.warming = false
	p.cond.Broadcast()
}

func (p *Pool) waitForJobOrRecycle(ctx context.Context, index int, vm *broker.WarmVM, bornAt time.Time) (Job, bool, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := &p.states[index]
	state.vm = vm
	state.bornAt = bornAt
	state.idleSince = p.options.Now()
	state.warming = false
	state.busy = false
	state.lastErr = nil
	p.cond.Broadcast()
	for {
		if p.shuttingDown || ctx.Err() != nil {
			return Job{Repo: "", JobID: 0, RunID: 0}, false, false
		}
		if state.recycle {
			state.vm = nil
			state.recycle = false
			state.warming = true
			p.cond.Broadcast()
			return Job{Repo: "", JobID: 0, RunID: 0}, true, true
		}
		if len(p.queue) > 0 {
			job := p.queue[0]
			// Advance the head and zero the removed slot so the dequeue is O(1)
			// and the popped Job's references are not retained by the backing
			// array.
			p.queue[0] = Job{Repo: "", JobID: 0, RunID: 0}
			p.queue = p.queue[1:]
			state.busy = true
			state.idleSince = time.Time{}
			p.cond.Broadcast()
			return job, false, true
		}
		p.cond.Wait()
	}
}

func (p *Pool) finishJob(index int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := &p.states[index]
	state.busy = false
	p.cond.Broadcast()
}

func (p *Pool) clearWorker(index int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.states[index] = workerState{
		vm:        nil,
		bornAt:    time.Time{},
		idleSince: time.Time{},
		warming:   false,
		busy:      false,
		recycle:   false,
		lastErr:   nil,
	}
	p.cond.Broadcast()
}

func (p *Pool) teardownVM(ctx context.Context, vm *broker.WarmVM) {
	if vm == nil {
		return
	}
	p.warmer.Teardown(ctx, vm)
}

func (p *Pool) nextID() string {
	next := p.counter.Add(1)
	return fmt.Sprintf("%s-%d", p.options.RunToken, next)
}

func recoverGoroutine(ctx context.Context, label string) {
	if recovered := recover(); recovered != nil {
		slog.ErrorContext(ctx, label+" panic recovered", "err", fmt.Errorf("panic: %v", recovered))
	}
}
