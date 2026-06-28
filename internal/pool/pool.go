// Package pool maintains a warm pool of pre-booted Tart VMs so jobs skip
// boot cost. The pool keeps up to size idle VMs and refills automatically as
// VMs are leased out or recycled after a job completes.
package pool

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
)

// warmFailDelay is the back-off pause between a failed Warm call and the next
// refill signal. It prevents a tight goroutine retry loop when Warm fails
// persistently (e.g. tart binary missing or golden image unavailable).
const warmFailDelay = 5 * time.Second

// Warmer creates and tears down warm VMs, and sweeps orphaned VMs left by a
// prior process. *broker.Binder satisfies this interface; tests use a stub.
type Warmer interface {
	Warm(ctx context.Context, id string) (*broker.WarmVM, error)
	Teardown(ctx context.Context, vm *broker.WarmVM)
	SweepOrphans(ctx context.Context)
}

// Pool keeps up to size idle warm VMs and refills as VMs are leased or
// recycled. Start must be called once before Lease.
type Pool struct {
	size      int
	warmer    Warmer
	idle      chan *broker.WarmVM
	refill    chan struct{}
	done      chan struct{}
	mu        sync.Mutex
	warming   int
	counter   atomic.Int64
	runToken  string // per-process token embedded in VM names; prevents cross-restart and cross-process name collisions
	wg        sync.WaitGroup
	failDelay time.Duration // overrides warmFailDelay in tests; zero uses warmFailDelay
}

// New returns a Pool of the given size backed by the given Warmer. runToken is a
// per-process token embedded in every VM name (alongside an incrementing
// counter), so names never repeat across restarts and never collide between two
// overlapping processes. Call Start to begin the fill loop.
func New(size int, w Warmer, runToken string) *Pool {
	return &Pool{
		size:      size,
		warmer:    w,
		idle:      make(chan *broker.WarmVM, size),
		refill:    make(chan struct{}, 1),
		done:      make(chan struct{}),
		mu:        sync.Mutex{},
		warming:   0,
		counter:   atomic.Int64{},
		runToken:  runToken,
		wg:        sync.WaitGroup{},
		failDelay: 0,
	}
}

// Start launches the background fill goroutine. It returns immediately; the
// fill loop runs until ctx is cancelled or Shutdown is called. The goroutine
// is tracked by wg so Shutdown waits for it to exit before draining idle VMs.
func (p *Pool) Start(ctx context.Context) {
	p.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "fillLoop panic recovered", "err", fmt.Errorf("panic: %v", r))
			}
		}()
		p.fillLoop(ctx)
	})
}

// Lease returns an idle warm VM, blocking until one is available or ctx is
// done. A successful Lease decrements the idle count and triggers a refill.
func (p *Pool) Lease(ctx context.Context) (*broker.WarmVM, error) {
	select {
	case vm := <-p.idle:
		p.sendRefill()
		return vm, nil
	case <-ctx.Done():
		slog.ErrorContext(ctx, "lease context done", "err", ctx.Err())
		return nil, fmt.Errorf("pool: lease: %w", ctx.Err())
	}
}

// Recycle tears down the VM and triggers a refill so the pool stays at size.
func (p *Pool) Recycle(ctx context.Context, vm *broker.WarmVM) {
	p.warmer.Teardown(ctx, vm)
	p.sendRefill()
}

// Shutdown stops the fill loop, waits for in-flight warm goroutines to finish,
// and tears down all idle VMs.
func (p *Pool) Shutdown(ctx context.Context) {
	close(p.done)
	p.wg.Wait()
	for {
		select {
		case vm := <-p.idle:
			p.warmer.Teardown(ctx, vm)
		default:
			return
		}
	}
}

// FreeSlots returns the count of idle VMs plus in-progress warm goroutines.
// It is used by the capacity endpoint to indicate available capacity.
func (p *Pool) FreeSlots() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle) + p.warming
}

// fillLoop runs until ctx is done or done is closed, triggering tryFill on
// each refill signal.
func (p *Pool) fillLoop(ctx context.Context) {
	// Clear any VMs orphaned by a previous process before filling so a hard
	// restart cannot leave stale clones behind or collide on a reused name.
	p.warmer.SweepOrphans(ctx)
	p.tryFill(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		case <-p.refill:
			p.tryFill(ctx)
		}
	}
}

// tryFill starts warm goroutines until the pool is at capacity or done is
// closed.
func (p *Pool) tryFill(ctx context.Context) {
	for {
		select {
		case <-p.done:
			return
		default:
		}

		p.mu.Lock()
		need := p.size - len(p.idle) - p.warming
		if need <= 0 {
			p.mu.Unlock()
			return
		}
		p.warming++
		// Add to wg while the mutex is still held so Shutdown's wg.Wait()
		// cannot return between the unlock and the goroutine launch.
		p.wg.Add(1)
		p.mu.Unlock()

		id := p.runToken + "-" + strconv.FormatInt(p.counter.Add(1), 10)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					slog.ErrorContext(ctx, "warmOne panic recovered", "err", fmt.Errorf("panic: %v", r), "id", id)
				}
			}()
			p.warmOne(ctx, id)
		}()
	}
}

// warmOne calls Warm, decrements the warming counter, and sends the result to
// idle (or tears it down if the pool is shutting down).
//
// warming is decremented explicitly on every path, always before any
// sendRefill call, so tryFill observes the correct in-flight count.
func (p *Pool) warmOne(ctx context.Context, id string) {
	defer p.wg.Done()

	vm, err := p.warmer.Warm(ctx, id)
	if err != nil {
		slog.WarnContext(ctx, "warm failed", "err", err, "id", id)
		// Back off before signalling a refill so a persistent Warm failure
		// (e.g. tart missing or image unavailable) does not spin goroutines.
		delay := p.failDelay
		if delay == 0 {
			delay = warmFailDelay
		}
		retryTimer := time.NewTimer(delay)
		defer retryTimer.Stop()
		select {
		case <-retryTimer.C:
		case <-p.done:
			p.mu.Lock()
			p.warming--
			p.mu.Unlock()
			return
		case <-ctx.Done():
			p.mu.Lock()
			p.warming--
			p.mu.Unlock()
			return
		}
		// Decrement before sending the refill signal so tryFill sees the
		// correct warming count and spawns a replacement goroutine.
		p.mu.Lock()
		p.warming--
		p.mu.Unlock()
		p.sendRefill()
		return
	}

	// Decrement before placing the VM into idle so that a concurrent Lease
	// (which calls sendRefill) triggers tryFill with the correct count.
	p.mu.Lock()
	p.warming--
	p.mu.Unlock()

	select {
	case p.idle <- vm:
	case <-p.done:
		p.warmer.Teardown(ctx, vm)
	case <-ctx.Done():
		p.warmer.Teardown(ctx, vm)
	}
}

// sendRefill sends a non-blocking signal to the fill loop.
func (p *Pool) sendRefill() {
	select {
	case p.refill <- struct{}{}:
	default:
	}
}
