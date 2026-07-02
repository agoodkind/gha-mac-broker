// Package pool maintains a demand-driven, image-keyed warm cache of Tart VMs so
// jobs skip boot cost when their declared macOS/Xcode image is hot.
package pool

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
)

// warmFailDelay is the back-off pause between a failed background Warm call and
// the next retry. It prevents a tight goroutine loop when Warm fails
// persistently, for example when tart or a base image is unavailable.
const warmFailDelay = 5 * time.Second

// warmFailureThreshold opens the capacity circuit after consecutive warm or
// liveness failures.
const warmFailureThreshold = 3

// reconcileInterval is the default cadence for checking cached warm VMs
// against Tart's current VM list.
const reconcileInterval = 60 * time.Second

// Warmer creates and tears down warm VMs, evicts per-image goldens, and sweeps
// orphaned VMs left by a prior process. *broker.Binder satisfies this
// interface; tests use a stub.
type Warmer interface {
	Warm(ctx context.Context, image, id string) (*broker.WarmVM, error)
	Teardown(ctx context.Context, vm *broker.WarmVM)
	CheckAlive(ctx context.Context, vm *broker.WarmVM) error
	List(ctx context.Context) ([]string, error)
	DeleteGolden(ctx context.Context, image string) error
	SweepOrphans(ctx context.Context)
}

type imageState struct {
	warm       *broker.WarmVM
	warming    bool
	warmCancel context.CancelFunc
	golden     bool
}

// Pool keeps a bounded image-keyed cache of idle warm VMs and per-image golden
// disks. Start must be called once before Lease.
type Pool struct {
	warmBudget   int
	goldenBudget int
	warmer       Warmer
	done         chan struct{}
	shutdownOnce sync.Once
	mu           sync.Mutex
	shuttingDown bool
	states       map[string]*imageState
	lru          []string
	leased       int
	warmFailures int
	lastWarmOK   bool
	counter      atomic.Int64
	runToken     string // per-process token embedded in VM names; prevents cross-restart and cross-process name collisions
	wg           sync.WaitGroup
	failDelay    time.Duration // overrides warmFailDelay in tests; zero uses warmFailDelay
}

// New returns a Pool backed by the given Warmer. warmBudget bounds idle warm
// VMs across all images, goldenBudget bounds derived golden disks, and runToken
// is embedded in each VM name beside an incrementing counter so names do not
// repeat across restarts or overlapping processes.
func New(warmBudget, goldenBudget int, w Warmer, runToken string) *Pool {
	return &Pool{
		warmBudget:   warmBudget,
		goldenBudget: goldenBudget,
		warmer:       w,
		done:         make(chan struct{}),
		shutdownOnce: sync.Once{},
		mu:           sync.Mutex{},
		shuttingDown: false,
		states:       make(map[string]*imageState),
		lru:          nil,
		leased:       0,
		warmFailures: 0,
		lastWarmOK:   true,
		counter:      atomic.Int64{},
		runToken:     runToken,
		wg:           sync.WaitGroup{},
		failDelay:    0,
	}
}

// Start launches the orphan-sweep goroutine. It returns immediately; the
// goroutine is tracked by wg so Shutdown waits for it before returning.
func (p *Pool) Start(ctx context.Context) {
	p.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "pool start goroutine panic recovered", "err", fmt.Errorf("panic: %v", r))
			}
		}()
		p.warmer.SweepOrphans(ctx)
		select {
		case <-ctx.Done():
		case <-p.done:
		}
	})
}

// StartReconcile launches the periodic warm-cache reconciliation goroutine. A
// non-positive interval uses the default cadence.
func (p *Pool) StartReconcile(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = reconcileInterval
	}
	p.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "pool reconcile goroutine panic recovered", "err", fmt.Errorf("panic: %v", r))
			}
		}()
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
					slog.WarnContext(ctx, "pool reconcile failed", "err", err)
				}
			}
		}
	})
}

// Lease returns a VM for image. It uses a matching warm VM when one is cached,
// otherwise it clones one on demand. Successful leases mark image most recently
// used and start a background replacement warm when the warm budget permits it.
func (p *Pool) Lease(ctx context.Context, image string) (*broker.WarmVM, error) {
	image = strings.TrimSpace(image)
	if image == "" {
		return nil, fmt.Errorf("pool: lease image is required")
	}
	vm, hit, err := p.beginLease(image)
	if err != nil {
		return nil, err
	}
	if hit {
		err := p.warmer.CheckAlive(ctx, vm)
		if err == nil {
			p.startWarmIfNeeded(ctx, image)
			return vm, nil
		}
		slog.WarnContext(ctx, "cached warm vm failed liveness check", "err", err, "vm", vm.Name, "image", image)
		p.recordWarmOutcome(false)
		p.warmer.Teardown(ctx, vm)
	}

	id := p.nextID()
	vm, err = p.warmer.Warm(ctx, image, id)
	if err != nil {
		p.finishLease()
		p.recordWarmOutcome(false)
		slog.WarnContext(ctx, "on-demand warm failed", "err", err, "image", image, "id", id)
		return nil, fmt.Errorf("pool: warm %s: %w", image, err)
	}
	if vm.Image == "" {
		vm.Image = image
	}
	p.recordWarmOutcome(true)
	p.recordGolden(ctx, image)
	p.startWarmIfNeeded(ctx, image)
	return vm, nil
}

// Recycle tears down a leased VM, releases its lease slot, and refreshes the
// image's warm cache entry when the image is still recent.
func (p *Pool) Recycle(ctx context.Context, vm *broker.WarmVM) {
	image := vm.Image
	p.warmer.Teardown(ctx, vm)
	p.finishLease()
	if image != "" {
		p.startWarmIfNeeded(ctx, image)
	}
}

// Shutdown stops background warming, waits for in-flight goroutines, and tears
// down all cached idle VMs.
func (p *Pool) Shutdown(ctx context.Context) {
	p.shutdownOnce.Do(func() {
		p.mu.Lock()
		p.shuttingDown = true
		warmCancels := p.takeWarmCancelsLocked()
		warmVMs := p.takeAllWarmLocked()
		p.mu.Unlock()

		for _, cancel := range warmCancels {
			cancel()
		}
		close(p.done)
		p.wg.Wait()
		for _, vm := range warmVMs {
			p.warmer.Teardown(ctx, vm)
		}
	})
}

// FreeSlots returns the remaining active lease capacity. The reservation store
// uses this as its capacity ceiling so outstanding reservations and running jobs
// do not overbook the warm budget.
func (p *Pool) FreeSlots() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.circuitOpenLocked() {
		return 0
	}
	freeSlots := p.warmBudget - p.leased
	if freeSlots < 0 {
		return 0
	}
	return freeSlots
}

// Reconcile removes cached warm entries that no longer exist in Tart or no
// longer answer a liveness probe, then starts replacement warming as needed.
func (p *Pool) Reconcile(ctx context.Context) error {
	names, err := p.warmer.List(ctx)
	if err != nil {
		slog.WarnContext(ctx, "list warm vms failed", "err", err)
		return fmt.Errorf("pool: list warm vms: %w", err)
	}
	present := make(map[string]struct{}, len(names))
	for _, name := range names {
		present[name] = struct{}{}
	}

	entries := p.warmEntries()
	for _, entry := range entries {
		if _, ok := present[entry.vm.Name]; !ok {
			p.evictWarm(ctx, entry.image, entry.vm, "missing")
			continue
		}
		if err := p.warmer.CheckAlive(ctx, entry.vm); err != nil {
			slog.WarnContext(ctx, "warm vm liveness check failed", "err", err, "vm", entry.vm.Name, "image", entry.image)
			p.evictWarm(ctx, entry.image, entry.vm, "liveness")
		}
	}
	return nil
}

func (p *Pool) beginLease(image string) (*broker.WarmVM, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.shuttingDown {
		return nil, false, fmt.Errorf("pool: shutting down")
	}
	if p.warmBudget <= 0 {
		return nil, false, fmt.Errorf("pool: warm budget is zero")
	}
	if p.leased >= p.warmBudget {
		return nil, false, fmt.Errorf("pool: no free lease slots")
	}
	p.leased++
	state := p.markMRULocked(image)
	if state.warm == nil {
		return nil, false, nil
	}
	vm := state.warm
	state.warm = nil
	p.cleanupStateLocked(image)
	return vm, true, nil
}

type warmEntry struct {
	image string
	vm    *broker.WarmVM
}

func (p *Pool) warmEntries() []warmEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	entries := make([]warmEntry, 0, len(p.states))
	for image, state := range p.states {
		if state.warm == nil {
			continue
		}
		entries = append(entries, warmEntry{image: image, vm: state.warm})
	}
	return entries
}

func (p *Pool) finishLease() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.leased > 0 {
		p.leased--
	}
}

func (p *Pool) recordWarmOutcome(success bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recordWarmOutcomeLocked(success)
}

func (p *Pool) recordWarmOutcomeLocked(success bool) {
	if success {
		p.warmFailures = 0
		p.lastWarmOK = true
		return
	}
	p.warmFailures++
	p.lastWarmOK = false
}

func (p *Pool) circuitOpenLocked() bool {
	return !p.lastWarmOK && p.warmFailures >= warmFailureThreshold
}

func (p *Pool) evictWarm(ctx context.Context, image string, vm *broker.WarmVM, reason string) {
	evicted := p.removeWarm(image, vm.Name)
	if evicted == nil {
		return
	}
	p.recordWarmOutcome(false)
	slog.WarnContext(ctx, "evicting warm vm", "vm", evicted.Name, "image", image, "reason", reason)
	p.warmer.Teardown(ctx, evicted)
	p.startWarmIfNeeded(ctx, image)
}

func (p *Pool) removeWarm(image, vmName string) *broker.WarmVM {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, ok := p.states[image]
	if !ok || state.warm == nil || state.warm.Name != vmName {
		return nil
	}
	vm := state.warm
	state.warm = nil
	p.cleanupStateLocked(image)
	return vm
}

func (p *Pool) recordGolden(ctx context.Context, image string) {
	p.mu.Lock()
	state := p.markMRULocked(image)
	state.golden = true
	deletedGoldens := p.evictGoldensLocked()
	p.mu.Unlock()
	p.deleteGoldens(ctx, deletedGoldens)
}

func (p *Pool) startWarmIfNeeded(ctx context.Context, image string) {
	p.mu.Lock()
	if p.shuttingDown || p.warmBudget <= 0 {
		p.mu.Unlock()
		return
	}
	state := p.ensureStateLocked(image)
	if state.warm != nil || state.warming {
		p.mu.Unlock()
		return
	}
	evictedWarm := p.evictWarmForSlotLocked()
	if p.countWarmSlotsLocked() >= p.warmBudget {
		p.mu.Unlock()
		p.teardownWarm(context.WithoutCancel(ctx), evictedWarm)
		return
	}
	warmCtx, warmCancel := context.WithCancel(context.WithoutCancel(ctx))
	state.warming = true
	state.warmCancel = warmCancel
	id := p.nextID()
	p.wg.Add(1)
	p.mu.Unlock()

	p.teardownWarm(context.WithoutCancel(ctx), evictedWarm)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(warmCtx, "warm cache goroutine panic recovered", "err", fmt.Errorf("panic: %v", r), "image", image, "id", id)
			}
		}()
		p.warmCache(warmCtx, warmCancel, image, id)
	}()
}

func (p *Pool) warmCache(ctx context.Context, cancel context.CancelFunc, image, id string) {
	defer p.wg.Done()
	defer cancel()

	vm, err := p.warmer.Warm(ctx, image, id)
	if err != nil {
		p.recordWarmOutcome(false)
		slog.WarnContext(ctx, "background warm failed", "err", err, "image", image, "id", id)
		p.clearWarming(image)
		p.waitBeforeRetry(ctx, image)
		return
	}
	if vm.Image == "" {
		vm.Image = image
	}
	p.recordWarmOutcome(true)

	p.mu.Lock()
	state := p.ensureStateLocked(image)
	state.warming = false
	state.warmCancel = nil
	if p.shuttingDown {
		p.cleanupStateLocked(image)
		p.mu.Unlock()
		p.warmer.Teardown(ctx, vm)
		return
	}
	duplicateWarm := state.warm
	state.warm = vm
	state.golden = true
	evictedWarm := p.evictWarmOverBudgetLocked()
	deletedGoldens := p.evictGoldensLocked()
	p.mu.Unlock()

	if duplicateWarm != nil {
		p.warmer.Teardown(ctx, duplicateWarm)
	}
	p.teardownWarm(ctx, evictedWarm)
	p.deleteGoldens(ctx, deletedGoldens)
}

func (p *Pool) clearWarming(image string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.ensureStateLocked(image)
	state.warming = false
	state.warmCancel = nil
	p.cleanupStateLocked(image)
}

func (p *Pool) waitBeforeRetry(ctx context.Context, image string) {
	delay := p.failDelay
	if delay == 0 {
		delay = warmFailDelay
	}
	retryTimer := time.NewTimer(delay)
	defer retryTimer.Stop()
	select {
	case <-retryTimer.C:
	case <-p.done:
		return
	case <-ctx.Done():
		return
	}
	p.startWarmIfNeeded(ctx, image)
}

func (p *Pool) nextID() string {
	return p.runToken + "-" + strconv.FormatInt(p.counter.Add(1), 10)
}

func (p *Pool) markMRULocked(image string) *imageState {
	state := p.ensureStateLocked(image)
	p.removeFromLRULocked(image)
	p.lru = append(p.lru, image)
	return state
}

func (p *Pool) ensureStateLocked(image string) *imageState {
	state, ok := p.states[image]
	if ok {
		return state
	}
	state = &imageState{
		warm:       nil,
		warming:    false,
		warmCancel: nil,
		golden:     false,
	}
	p.states[image] = state
	p.lru = append(p.lru, image)
	return state
}

func (p *Pool) removeFromLRULocked(image string) {
	for index, value := range p.lru {
		if value != image {
			continue
		}
		p.lru = append(p.lru[:index], p.lru[index+1:]...)
		return
	}
}

func (p *Pool) cleanupStateLocked(image string) {
	state, ok := p.states[image]
	if !ok {
		return
	}
	if state.warm != nil || state.warming || state.golden {
		return
	}
	delete(p.states, image)
	p.removeFromLRULocked(image)
}

func (p *Pool) countWarmSlotsLocked() int {
	count := 0
	for _, state := range p.states {
		if state.warm != nil {
			count++
		}
		if state.warming {
			count++
		}
	}
	return count
}

func (p *Pool) countGoldensLocked() int {
	count := 0
	for _, state := range p.states {
		if state.golden {
			count++
		}
	}
	return count
}

func (p *Pool) evictWarmForSlotLocked() []*broker.WarmVM {
	var evicted []*broker.WarmVM
	for p.countWarmSlotsLocked() >= p.warmBudget {
		vm := p.popLeastRecentWarmLocked()
		if vm == nil {
			return evicted
		}
		evicted = append(evicted, vm)
	}
	return evicted
}

func (p *Pool) evictWarmOverBudgetLocked() []*broker.WarmVM {
	var evicted []*broker.WarmVM
	for p.countWarmSlotsLocked() > p.warmBudget {
		vm := p.popLeastRecentWarmLocked()
		if vm == nil {
			return evicted
		}
		evicted = append(evicted, vm)
	}
	return evicted
}

func (p *Pool) popLeastRecentWarmLocked() *broker.WarmVM {
	for _, image := range p.lru {
		state := p.states[image]
		if state == nil || state.warm == nil {
			continue
		}
		vm := state.warm
		state.warm = nil
		p.cleanupStateLocked(image)
		return vm
	}
	return nil
}

func (p *Pool) evictGoldensLocked() []string {
	if p.goldenBudget <= 0 {
		return nil
	}
	var deleted []string
	for p.countGoldensLocked() > p.goldenBudget {
		image := p.popLeastRecentGoldenLocked()
		if image == "" {
			return deleted
		}
		deleted = append(deleted, image)
	}
	return deleted
}

func (p *Pool) popLeastRecentGoldenLocked() string {
	for _, image := range p.lru {
		state := p.states[image]
		if state == nil || !state.golden {
			continue
		}
		state.golden = false
		p.cleanupStateLocked(image)
		return image
	}
	return ""
}

func (p *Pool) takeAllWarmLocked() []*broker.WarmVM {
	var warmVMs []*broker.WarmVM
	for image, state := range p.states {
		if state.warm == nil {
			continue
		}
		warmVMs = append(warmVMs, state.warm)
		state.warm = nil
		p.cleanupStateLocked(image)
	}
	return warmVMs
}

func (p *Pool) takeWarmCancelsLocked() []context.CancelFunc {
	var cancels []context.CancelFunc
	for _, state := range p.states {
		if state.warmCancel == nil {
			continue
		}
		cancels = append(cancels, state.warmCancel)
		state.warmCancel = nil
	}
	return cancels
}

func (p *Pool) teardownWarm(ctx context.Context, warmVMs []*broker.WarmVM) {
	for _, vm := range warmVMs {
		p.warmer.Teardown(ctx, vm)
	}
}

func (p *Pool) deleteGoldens(ctx context.Context, images []string) {
	for _, image := range images {
		if err := p.warmer.DeleteGolden(ctx, image); err != nil {
			slog.WarnContext(ctx, "delete golden failed", "err", err, "image", image)
		}
	}
}
