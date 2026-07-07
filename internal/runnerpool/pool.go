// Package runnerpool runs a fixed set of persistent warm VMs against a FIFO
// queue of repo-scoped GitHub Actions jobs.
package runnerpool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
	"goodkind.io/gha-mac-broker/internal/ghapp"
)

const (
	defaultRunnerCount       = 3
	defaultJobsPerVM         = 1
	defaultWarmRetryDelay    = 5 * time.Second
	defaultReconcileInterval = time.Minute
	defaultMaxBind           = 65 * time.Minute
	defaultPickupTimeout     = 5 * time.Minute
	defaultStallTimeout      = 10 * time.Minute
	stallCPUThresholdPercent = 2.0
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
	JobsPerVM      int
	Image          string
	MaxIdle        time.Duration
	MaxAge         time.Duration
	MaxBind        time.Duration
	PickupTimeout  time.Duration
	StallTimeout   time.Duration
	StallReap      bool
	RunToken       string
	WarmRetryDelay time.Duration
	Now            func() time.Time
}

// Snapshot is a concurrency-safe view of pool readiness and backlog.
type Snapshot struct {
	RunnerCount int  `json:"runner_count"`
	Idle        int  `json:"idle"`
	Busy        int  `json:"busy"`
	Queued      int  `json:"queued"`
	Healthy     bool `json:"healthy"`
	Ready       bool `json:"ready"`
}

// WorkerView is a concurrency-safe per-worker status row.
type WorkerView struct {
	Index          int        `json:"index"`
	VM             string     `json:"vm"`
	Phase          string     `json:"phase"`
	RunID          int64      `json:"run_id"`
	BindAgeSeconds int64      `json:"bind_age_seconds"`
	ActiveJob      *bool      `json:"active_job"`
	LastError      string     `json:"last_error"`
	Slots          []SlotView `json:"slots,omitempty"`
}

// SlotView is a concurrency-safe status row for one runner slot in a VM.
type SlotView struct {
	Index          int    `json:"index"`
	Phase          string `json:"phase"`
	RunID          int64  `json:"run_id"`
	JobID          int64  `json:"job_id"`
	BindAgeSeconds int64  `json:"bind_age_seconds"`
	ActiveJob      *bool  `json:"active_job"`
	LastError      string `json:"last_error"`
}

// Warmer creates, probes, and tears down warm VMs. *broker.Binder satisfies
// this interface.
type Warmer interface {
	Warm(ctx context.Context, image string, id string, slotCount int) (*broker.WarmVM, error)
	Teardown(ctx context.Context, vm *broker.WarmVM)
	CheckAlive(ctx context.Context, vm *broker.WarmVM) error
	SweepOrphans(ctx context.Context)
}

// Runner executes one JIT job on a warm VM. *broker.Binder satisfies this
// interface.
type Runner interface {
	RunJob(ctx context.Context, vm *broker.WarmVM, repo string, runnerName string, slotIndex int, slotCount int) error
}

// ActiveJobProber checks whether a busy worker slot is running a job process.
type ActiveJobProber interface {
	HasActiveJob(ctx context.Context, vm *broker.WarmVM, slotIndex int, slotCount int) (bool, error)
	SlotCPUActivity(ctx context.Context, vm *broker.WarmVM, slotIndex int, slotCount int) (float64, error)
}

// RunnerLister lists GitHub runners for idle VM health checks.
type RunnerLister interface {
	ListInstalledRepos(ctx context.Context) ([]string, error)
	ListRunners(ctx context.Context, repo string) ([]ghapp.Runner, error)
}

type workerState struct {
	vm        *broker.WarmVM
	bornAt    time.Time
	idleSince time.Time
	warming   bool
	recycle   bool
	slots     []slotState
	lastErr   error
}

type slotState struct {
	boundAt         time.Time
	busy            bool
	jobID           int64
	runID           int64
	jobCancel       context.CancelFunc
	cpuStalledSince time.Time
	stallWarnedAt   time.Time
	reapWarnedAt    time.Time
	lastErr         error
}

// Pool owns N persistent warm VMs and drains a FIFO job queue through them.
type Pool struct {
	options Options
	warmer  Warmer
	runner  Runner
	github  RunnerLister
	prober  ActiveJobProber

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
func New(options Options, warmer Warmer, runner Runner, github RunnerLister, prober ActiveJobProber) *Pool {
	options = normalizeOptions(options)
	pool := &Pool{
		options:      options,
		warmer:       warmer,
		runner:       runner,
		github:       github,
		prober:       prober,
		mu:           sync.Mutex{},
		cond:         nil,
		queue:        nil,
		states:       newWorkerStates(options.RunnerCount, options.JobsPerVM),
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

// Reconfigure applies live safe pool options without interrupting running jobs.
func (p *Pool) Reconfigure(newOptions Options) {
	preserveNow := newOptions.Now == nil
	preserveRunToken := newOptions.RunToken == ""
	preserveImage := newOptions.Image == ""
	newOptions = normalizeOptions(newOptions)
	requestedRunnerCount := newOptions.RunnerCount

	p.mu.Lock()
	oldOptions := p.options
	if preserveNow {
		newOptions.Now = oldOptions.Now
	}
	if preserveRunToken {
		newOptions.RunToken = oldOptions.RunToken
	}
	if preserveImage {
		newOptions.Image = oldOptions.Image
	}
	runnerCountChanged := newOptions.RunnerCount != oldOptions.RunnerCount
	if runnerCountChanged {
		newOptions.RunnerCount = oldOptions.RunnerCount
	}
	p.options = Options{
		RunnerCount:    oldOptions.RunnerCount,
		JobsPerVM:      newOptions.JobsPerVM,
		Image:          newOptions.Image,
		MaxIdle:        newOptions.MaxIdle,
		MaxAge:         newOptions.MaxAge,
		MaxBind:        newOptions.MaxBind,
		PickupTimeout:  newOptions.PickupTimeout,
		StallTimeout:   newOptions.StallTimeout,
		StallReap:      newOptions.StallReap,
		RunToken:       newOptions.RunToken,
		WarmRetryDelay: newOptions.WarmRetryDelay,
		Now:            newOptions.Now,
	}
	p.cond.Broadcast()
	p.mu.Unlock()

	if runnerCountChanged {
		slog.Warn(
			"runnerpool reconfigure: runner_count change requires restart; keeping current",
			"old_runner_count", oldOptions.RunnerCount,
			"new_runner_count", requestedRunnerCount,
		)
	}
	slog.Info(
		"runnerpool reconfigured",
		"old_jobs_per_vm", oldOptions.JobsPerVM,
		"new_jobs_per_vm", newOptions.JobsPerVM,
		"old_max_idle", oldOptions.MaxIdle,
		"new_max_idle", newOptions.MaxIdle,
		"old_max_age", oldOptions.MaxAge,
		"new_max_age", newOptions.MaxAge,
		"old_max_bind", oldOptions.MaxBind,
		"new_max_bind", newOptions.MaxBind,
		"old_pickup_timeout", oldOptions.PickupTimeout,
		"new_pickup_timeout", newOptions.PickupTimeout,
	)
}

func normalizeOptions(options Options) Options {
	if options.RunnerCount <= 0 {
		options.RunnerCount = defaultRunnerCount
	}
	if options.JobsPerVM <= 0 {
		options.JobsPerVM = defaultJobsPerVM
	}
	if options.WarmRetryDelay <= 0 {
		options.WarmRetryDelay = defaultWarmRetryDelay
	}
	if options.MaxBind <= 0 {
		options.MaxBind = defaultMaxBind
	}
	if options.PickupTimeout <= 0 {
		options.PickupTimeout = defaultPickupTimeout
	}
	if options.StallTimeout <= 0 {
		options.StallTimeout = defaultStallTimeout
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.RunToken == "" {
		options.RunToken = "pool"
	}
	return options
}

func newWorkerStates(runnerCount int, jobsPerVM int) []workerState {
	states := make([]workerState, runnerCount)
	for index := range states {
		states[index].slots = make([]slotState, jobsPerVM)
	}
	return states
}

func (p *Pool) optionsSnapshot() Options {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.options
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

		options := p.optionsSnapshot()
		for i := range options.RunnerCount {
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
	p.cond.Broadcast()
	return nil
}

// Ready reports whether the pool has immediate capacity for the next job.
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
	activeSlots := 0
	for _, state := range p.states {
		alive := state.vm != nil && !state.recycle && state.lastErr == nil
		if alive {
			activeSlots += len(state.slots)
		}
		for _, slot := range state.slots {
			if alive && !slot.busy {
				snapshot.Idle++
			}
			if slot.busy {
				snapshot.Busy++
			}
		}
	}
	// The pool is healthy when at least one worker VM can serve a job. A single
	// worker warming, recycling, or errored does not take the whole pool down,
	// so routine recycling never sheds every consumer to hosted while other VMs
	// remain live.
	snapshot.Healthy = activeSlots > 0
	// Ready means a genuinely idle slot exists for the next job, net of jobs
	// already queued ahead. Idle only counts alive, non-busy slots, so
	// activeSlots == 0 yields Idle == 0 and Ready == false. Never report the
	// optimistic bet that a busy slot will free soon, since a long build can
	// strand the job and the broker contract is truthful capacity.
	snapshot.Ready = snapshot.Idle > snapshot.Queued
	return snapshot
}

// Reconcile recycles idle VMs that exceed hygiene limits or fail health checks.
func (p *Pool) Reconcile(ctx context.Context) error {
	p.reapBusyWorkers(ctx)
	candidates := p.idleCandidates()
	repos, skipRegistrationCheck := p.installedReposForHealth(ctx, candidates)
	var recycleErrs []error
	for _, candidate := range candidates {
		recycle, err := p.shouldRecycle(ctx, candidate, repos, skipRegistrationCheck)
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

func (p *Pool) installedReposForHealth(ctx context.Context, candidates []idleCandidate) ([]string, bool) {
	if p.github == nil || len(candidates) == 0 {
		return nil, false
	}
	repos, err := p.github.ListInstalledRepos(ctx)
	if err != nil {
		// Cannot enumerate the App's repos this cycle (transient outage, rate
		// limit, permission hiccup). Skip the registration-leak check rather than
		// recycle the VMs, so one listing failure does not churn the whole warm
		// pool. A real leak is still caught on a later reconcile once the API
		// recovers.
		slog.WarnContext(ctx, "runnerpool list installed repos failed; skipping registration check", "err", err, "candidate_count", len(candidates))
		return nil, true
	}
	return repos, false
}

type idleCandidate struct {
	index     int
	vm        *broker.WarmVM
	bornAt    time.Time
	idleSince time.Time
	slotCount int
	now       time.Time
}

func (p *Pool) idleCandidates() []idleCandidate {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.options.Now()
	candidates := make([]idleCandidate, 0, len(p.states))
	for index, state := range p.states {
		if state.vm == nil || state.warming || workerBusy(state) || state.recycle {
			continue
		}
		candidates = append(candidates, idleCandidate{
			index:     index,
			vm:        state.vm,
			bornAt:    state.bornAt,
			idleSince: state.idleSince,
			slotCount: len(state.slots),
			now:       now,
		})
	}
	return candidates
}

func (p *Pool) shouldRecycle(ctx context.Context, candidate idleCandidate, repos []string, skipRegistrationCheck bool) (bool, error) {
	options := p.optionsSnapshot()
	if candidate.slotCount != options.JobsPerVM {
		slog.InfoContext(
			ctx,
			"runnerpool idle vm slot count mismatch; recycling",
			"vm", candidate.vm.Name,
			"old_jobs_per_vm", candidate.slotCount,
			"new_jobs_per_vm", options.JobsPerVM,
		)
		return true, nil
	}
	if options.MaxIdle > 0 && !candidate.idleSince.IsZero() && candidate.now.Sub(candidate.idleSince) >= options.MaxIdle {
		return true, nil
	}
	if options.MaxAge > 0 && !candidate.bornAt.IsZero() && candidate.now.Sub(candidate.bornAt) >= options.MaxAge {
		return true, nil
	}
	if err := p.checkHealth(ctx, candidate.vm, candidate.slotCount, repos, skipRegistrationCheck); err != nil {
		return true, err
	}
	return false, nil
}

func (p *Pool) checkHealth(ctx context.Context, vm *broker.WarmVM, slotCount int, repos []string, skipRegistrationCheck bool) error {
	if err := p.warmer.CheckAlive(ctx, vm); err != nil {
		slog.WarnContext(ctx, "runnerpool alive check failed", "err", err, "vm", vm.Name)
		return fmt.Errorf("runnerpool: check alive %s: %w", vm.Name, err)
	}
	if p.github == nil || skipRegistrationCheck {
		return nil
	}
	for _, repo := range repos {
		runners, err := p.github.ListRunners(ctx, repo)
		if err != nil {
			slog.WarnContext(ctx, "runnerpool list runners failed", "err", err, "repo", repo, "vm", vm.Name)
			return fmt.Errorf("runnerpool: list runners %s: %w", repo, err)
		}
		for _, runner := range runners {
			if runnerNameBelongsToVM(vm.Name, runner.Name, slotCount) {
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
	if state.vm == nil || state.vm.Name != vm.Name || workerBusy(*state) {
		return
	}
	state.recycle = true
	state.lastErr = err
	p.cond.Broadcast()
}

func (p *Pool) requestBusyRecycle(candidate busyCandidate) {
	var cancel context.CancelFunc
	p.mu.Lock()
	state := &p.states[candidate.index]
	if state.vm == nil ||
		state.vm.Name != candidate.vm.Name ||
		candidate.slotIndex < 0 ||
		candidate.slotIndex >= len(state.slots) {
		p.mu.Unlock()
		return
	}
	slot := &state.slots[candidate.slotIndex]
	if !slot.busy ||
		slot.jobID != candidate.jobID ||
		slot.runID != candidate.runID ||
		!slot.boundAt.Equal(candidate.boundAt) {
		p.mu.Unlock()
		return
	}
	state.recycle = true
	cancel = slot.jobCancel
	slot.jobCancel = nil
	p.cond.Broadcast()
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// CancelRun reaps the busy slot bound to a workflow job id, if one is still
// running. The run id stays as observable status on the slot.
func (p *Pool) CancelRun(jobID int64) {
	var cancel context.CancelFunc
	p.mu.Lock()
	for index := range p.states {
		state := &p.states[index]
		for slotIndex := range state.slots {
			slot := &state.slots[slotIndex]
			if !slot.busy || slot.jobID != jobID {
				continue
			}
			state.recycle = true
			cancel = slot.jobCancel
			slot.jobCancel = nil
			p.cond.Broadcast()
			break
		}
		if cancel != nil {
			break
		}
	}
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
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
	for {
		vm, slotCount, ok := p.warmWorker(ctx, index)
		if !ok {
			p.clearWorker(index)
			return
		}
		p.activateWorker(index, vm, slotCount)
		recycle, ok := p.runWorkerSlots(ctx, index, vm)
		p.teardownVM(context.WithoutCancel(ctx), vm)
		p.clearWorker(index)
		if !ok || !recycle {
			return
		}
	}
}

func (p *Pool) runWorkerSlots(ctx context.Context, index int, vm *broker.WarmVM) (bool, bool) {
	slotCtx, cancelSlots := context.WithCancel(ctx)
	var slotWG sync.WaitGroup
	slotCount := p.workerSlotCount(index, vm)
	for slotIndex := range slotCount {
		slot := slotIndex
		slotWG.Go(func() {
			defer recoverGoroutine(slotCtx, "runnerpool slot")
			p.slotLoop(slotCtx, index, slot, vm)
		})
	}
	recycle, ok := p.waitForWorkerRecycleOrShutdown(ctx, index, vm)
	cancelSlots()
	p.mu.Lock()
	p.cond.Broadcast()
	p.mu.Unlock()
	slotWG.Wait()
	return recycle, ok
}

func (p *Pool) workerSlotCount(index int, vm *broker.WarmVM) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if index < 0 || index >= len(p.states) {
		return 0
	}
	state := p.states[index]
	if state.vm == nil || state.vm.Name != vm.Name {
		return 0
	}
	return len(state.slots)
}

func (p *Pool) waitForWorkerRecycleOrShutdown(ctx context.Context, index int, vm *broker.WarmVM) (bool, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		state := &p.states[index]
		if p.shuttingDown || ctx.Err() != nil {
			return false, false
		}
		if state.vm == nil || state.vm.Name != vm.Name {
			return true, true
		}
		if state.recycle && !workerBusy(*state) {
			state.vm = nil
			state.recycle = false
			state.warming = true
			state.idleSince = time.Time{}
			p.resetWorkerSlotsLocked(state)
			p.cond.Broadcast()
			return true, true
		}
		p.cond.Wait()
	}
}

func (p *Pool) slotLoop(ctx context.Context, index int, slotIndex int, vm *broker.WarmVM) {
	for {
		job, jobCtx, cancel, slotCount, ok := p.waitForSlotJob(ctx, index, slotIndex, vm)
		if !ok {
			return
		}
		err := func() (err error) {
			defer func() {
				if recovered := recover(); recovered != nil {
					err = fmt.Errorf("panic: %v", recovered)
				}
				cancel()
				p.finishSlotJob(index, slotIndex, vm, err)
			}()
			return p.runner.RunJob(jobCtx, vm, job.Repo, runnerNameForSlot(vm.Name, slotIndex, slotCount), slotIndex, slotCount)
		}()
		if err != nil {
			slog.WarnContext(ctx, "runnerpool job failed", "err", err, "repo", job.Repo, "job_id", job.JobID, "run_id", job.RunID, "vm", vm.Name, "slot", slotIndex)
		}
	}
}

func (p *Pool) warmWorker(ctx context.Context, index int) (*broker.WarmVM, int, bool) {
	for {
		p.markWarming(index)
		id := p.nextID()
		image, slotCount, warmRetryDelay := p.warmRequestOptions()
		vm, err := p.warmer.Warm(ctx, image, id, slotCount)
		if err == nil {
			if vm.Image == "" {
				vm.Image = image
			}
			return vm, slotCount, true
		}
		p.markWarmError(index, err)
		slog.WarnContext(ctx, "runnerpool warm failed", "err", err, "worker", index, "id", id)
		timer := time.NewTimer(warmRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, 0, false
		case <-timer.C:
		}
	}
}

func (p *Pool) warmRequestOptions() (string, int, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.options.Image, p.options.JobsPerVM, p.options.WarmRetryDelay
}

func (p *Pool) markWarming(index int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := &p.states[index]
	state.vm = nil
	state.bornAt = time.Time{}
	state.idleSince = time.Time{}
	state.warming = true
	state.recycle = false
	state.lastErr = nil
	p.resetWorkerSlotsLocked(state)
	p.cond.Broadcast()
}

func (p *Pool) markWarmError(index int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := &p.states[index]
	state.lastErr = err
	state.warming = false
	state.recycle = false
	p.resetWorkerSlotsLocked(state)
	p.cond.Broadcast()
}

func (p *Pool) activateWorker(index int, vm *broker.WarmVM, slotCount int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.options.Now()
	state := &p.states[index]
	state.vm = vm
	state.bornAt = now
	state.idleSince = now
	state.warming = false
	state.recycle = false
	state.lastErr = nil
	p.resetWorkerSlotsToLocked(state, slotCount)
	p.cond.Broadcast()
}

func (p *Pool) waitForSlotJob(ctx context.Context, index int, slotIndex int, vm *broker.WarmVM) (Job, context.Context, context.CancelFunc, int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if p.shuttingDown || ctx.Err() != nil {
			return emptyJob(), nil, nil, 0, false
		}
		state := &p.states[index]
		if state.vm == nil || state.vm.Name != vm.Name || state.recycle {
			return emptyJob(), nil, nil, 0, false
		}
		if slotIndex < 0 || slotIndex >= len(state.slots) {
			return emptyJob(), nil, nil, 0, false
		}
		slot := &state.slots[slotIndex]
		if slot.busy {
			p.cond.Wait()
			continue
		}
		if len(p.queue) > 0 {
			job := p.dequeueJobLocked()
			jobCtx, cancel := context.WithCancel(ctx)
			slot.busy = true
			state.idleSince = time.Time{}
			slot.boundAt = p.options.Now()
			slot.jobID = job.JobID
			slot.runID = job.RunID
			slot.jobCancel = cancel
			slot.cpuStalledSince = time.Time{}
			slot.stallWarnedAt = time.Time{}
			slot.reapWarnedAt = time.Time{}
			slot.lastErr = nil
			slotCount := len(state.slots)
			p.cond.Broadcast()
			return job, jobCtx, cancel, slotCount, true
		}
		p.cond.Wait()
	}
}

func (p *Pool) dequeueJobLocked() Job {
	job := p.queue[0]
	// Advance the head and zero the removed slot so the dequeue is O(1)
	// and the popped Job's references are not retained by the backing array.
	p.queue[0] = Job{Repo: "", JobID: 0, RunID: 0}
	p.queue = p.queue[1:]
	return job
}

func emptyJob() Job {
	return Job{
		Repo:  "",
		JobID: 0,
		RunID: 0,
	}
}

func (p *Pool) finishSlotJob(index int, slotIndex int, vm *broker.WarmVM, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := &p.states[index]
	if state.vm == nil || state.vm.Name != vm.Name || slotIndex < 0 || slotIndex >= len(state.slots) {
		return
	}
	slot := &state.slots[slotIndex]
	slot.busy = false
	slot.boundAt = time.Time{}
	slot.jobID = 0
	slot.runID = 0
	slot.jobCancel = nil
	slot.cpuStalledSince = time.Time{}
	slot.stallWarnedAt = time.Time{}
	slot.reapWarnedAt = time.Time{}
	slot.lastErr = err
	if !workerBusy(*state) {
		if state.recycle {
			state.idleSince = time.Time{}
		} else {
			state.idleSince = p.options.Now()
		}
	}
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
		recycle:   false,
		slots:     make([]slotState, p.options.JobsPerVM),
		lastErr:   nil,
	}
	p.cond.Broadcast()
}

func (p *Pool) resetWorkerSlotsLocked(state *workerState) {
	p.resetWorkerSlotsToLocked(state, p.options.JobsPerVM)
}

func (p *Pool) resetWorkerSlotsToLocked(state *workerState, slotCount int) {
	if slotCount <= 0 {
		slotCount = defaultJobsPerVM
	}
	if len(state.slots) != slotCount {
		state.slots = make([]slotState, slotCount)
		return
	}
	resetSlots(state.slots)
}

func runnerNameForSlot(vmName string, slotIndex int, slotCount int) string {
	if slotCount <= 1 {
		return vmName
	}
	return fmt.Sprintf("%s-slot-%d", vmName, slotIndex)
}

func runnerNameBelongsToVM(vmName string, runnerName string, slotCount int) bool {
	if runnerName == vmName {
		return true
	}
	if slotCount > 1 {
		for slotIndex := range slotCount {
			if runnerName == runnerNameForSlot(vmName, slotIndex, slotCount) {
				return true
			}
		}
	}
	slotSuffix, found := strings.CutPrefix(runnerName, vmName+"-slot-")
	if !found {
		return false
	}
	slotIndex, err := strconv.Atoi(slotSuffix)
	return err == nil && slotIndex >= 0
}

func (p *Pool) teardownVM(ctx context.Context, vm *broker.WarmVM) {
	if vm == nil {
		return
	}
	p.warmer.Teardown(ctx, vm)
}

func (p *Pool) nextID() string {
	next := p.counter.Add(1)
	p.mu.Lock()
	runToken := p.options.RunToken
	p.mu.Unlock()
	return fmt.Sprintf("%s-%d", runToken, next)
}

func recoverGoroutine(ctx context.Context, label string) {
	if recovered := recover(); recovered != nil {
		slog.ErrorContext(ctx, label+" panic recovered", "err", fmt.Errorf("panic: %v", recovered))
	}
}
