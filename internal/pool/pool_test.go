package pool

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
)

// stubWarmer is a deterministic Warmer stub for pool tests.
type stubWarmer struct {
	mu      sync.Mutex
	warmed  int
	torn    int
	warmErr error
	// failN makes the first failN calls return a transient error before succeeding.
	failN int
	swept int
	ids   []string
}

func (s *stubWarmer) Warm(_ context.Context, id string) (*broker.WarmVM, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.warmErr != nil {
		return nil, s.warmErr
	}
	if s.failN > 0 {
		s.failN--
		return nil, errors.New("transient warm error")
	}
	s.warmed++
	s.ids = append(s.ids, id)
	return &broker.WarmVM{Name: "vm-" + id, Host: "127.0.0.1"}, nil
}

func (s *stubWarmer) Teardown(_ context.Context, _ *broker.WarmVM) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.torn++
}

func (s *stubWarmer) SweepOrphans(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.swept++
}

// waitFor polls cond every 10 ms until it returns true or 5 s elapses.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPoolFillsToSize(t *testing.T) {
	w := &stubWarmer{}
	p := New(2, w, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	waitFor(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.warmed >= 2
	})
}

func TestPoolLeaseDecrementsAndRefills(t *testing.T) {
	w := &stubWarmer{}
	p := New(2, w, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	waitFor(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.warmed >= 2
	})

	vm, err := p.Lease(ctx)
	if err != nil {
		t.Fatalf("lease failed: %v", err)
	}
	if vm == nil {
		t.Fatal("lease returned nil vm")
	}

	// Leasing one slot triggers a refill; total warmed climbs above 2.
	waitFor(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.warmed >= 3
	})
}

func TestPoolRecycleTeardownAndRefill(t *testing.T) {
	w := &stubWarmer{}
	p := New(1, w, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	waitFor(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.warmed >= 1
	})

	vm, err := p.Lease(ctx)
	if err != nil {
		t.Fatalf("lease failed: %v", err)
	}

	p.Recycle(ctx, vm)

	waitFor(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.torn >= 1 && w.warmed >= 2
	})
}

func TestPoolShutdownTeardownsIdle(t *testing.T) {
	w := &stubWarmer{}
	p := New(2, w, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	waitFor(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.warmed >= 2
	})

	p.Shutdown(context.Background())

	w.mu.Lock()
	torn := w.torn
	w.mu.Unlock()
	if torn != 2 {
		t.Fatalf("expected 2 torn down on shutdown, got %d", torn)
	}
}

func TestPoolLeaseContextCancelled(t *testing.T) {
	w := &stubWarmer{warmErr: context.Canceled}
	p := New(1, w, "test")
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	cancel()

	leaseCtx, leaseCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer leaseCancel()
	_, err := p.Lease(leaseCtx)
	if err == nil {
		t.Fatal("expected error when leasing with cancelled context")
	}
}

// TestPoolRefillsAfterWarmFailure verifies that a size-1 pool recovers to full
// capacity after a single warm failure. Without the fix, the failing goroutine
// calls sendRefill while warming is still 1, tryFill computes need=0 and skips
// spawning, and after the deferred decrement no further refill is sent, leaving
// the pool permanently empty.
func TestPoolRefillsAfterWarmFailure(t *testing.T) {
	w := &stubWarmer{failN: 1}
	p := New(1, w, "test")
	p.failDelay = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	// Pool must reach one warm VM despite the initial failure.
	waitFor(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.warmed >= 1
	})
}

// TestPoolSweepsAndNamesCarryRunToken verifies the pool sweeps orphans once on
// start and that every VM id embeds the injected run token, so names stay
// readable yet never collide across restarts.
func TestPoolSweepsAndNamesCarryRunToken(t *testing.T) {
	w := &stubWarmer{}
	p := New(1, w, "tok123")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	waitFor(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.swept >= 1 && len(w.ids) >= 1
	})

	w.mu.Lock()
	defer w.mu.Unlock()
	if !strings.HasPrefix(w.ids[0], "tok123-") {
		t.Fatalf("id %q should start with run token %q", w.ids[0], "tok123-")
	}
}

func TestPoolFreeSlotsIncludesWarming(t *testing.T) {
	// Use a blocking warmer so warming count stays > 0 during the check.
	block := make(chan struct{})
	bw := &blockingWarmer{block: block}
	p := New(2, bw, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	// Give the fill loop a moment to start warm goroutines.
	time.Sleep(50 * time.Millisecond)

	slots := p.FreeSlots()
	if slots < 1 {
		t.Fatalf("expected FreeSlots >= 1 while warming, got %d", slots)
	}

	close(block)
}

// blockingWarmer blocks until the block channel is closed.
type blockingWarmer struct {
	block <-chan struct{}
	mu    sync.Mutex
	torn  int
}

func (b *blockingWarmer) Warm(ctx context.Context, id string) (*broker.WarmVM, error) {
	select {
	case <-b.block:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &broker.WarmVM{Name: "vm-" + id, Host: "127.0.0.1"}, nil
}

func (b *blockingWarmer) Teardown(_ context.Context, _ *broker.WarmVM) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.torn++
}

func (b *blockingWarmer) SweepOrphans(_ context.Context) {}
