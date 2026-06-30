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

type warmCall struct {
	Image string
	ID    string
}

// stubWarmer is a deterministic Warmer stub for pool tests.
type stubWarmer struct {
	mu             sync.Mutex
	calls          []warmCall
	torn           []string
	deletedGoldens []string
	warmErr        error
	failN          int
	swept          int
	block          <-chan struct{}
}

func (s *stubWarmer) Warm(ctx context.Context, image, id string) (*broker.WarmVM, error) {
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.warmErr != nil {
		return nil, s.warmErr
	}
	if s.failN > 0 {
		s.failN--
		return nil, errors.New("transient warm error")
	}
	s.calls = append(s.calls, warmCall{Image: image, ID: id})
	return &broker.WarmVM{Name: "vm-" + id, Image: image}, nil
}

func (s *stubWarmer) Teardown(_ context.Context, vm *broker.WarmVM) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.torn = append(s.torn, vm.Image)
}

func (s *stubWarmer) DeleteGolden(_ context.Context, image string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletedGoldens = append(s.deletedGoldens, image)
	return nil
}

func (s *stubWarmer) SweepOrphans(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.swept++
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-timeout.C:
			t.Fatal("condition not met within timeout")
		case <-ticker.C:
		}
	}
}

func imageHasWarmVM(p *Pool, image string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, ok := p.states[image]
	return ok && state.warm != nil
}

func imageWarmCount(p *Pool) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, state := range p.states {
		if state.warm != nil {
			count++
		}
	}
	return count
}

func TestPoolSweepsAndNamesCarryRunToken(t *testing.T) {
	w := &stubWarmer{}
	p := New(2, 3, w, "tok123")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	waitFor(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.swept >= 1
	})

	vm, err := p.Lease(ctx, "image-a")
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	defer p.Recycle(ctx, vm)

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.calls) == 0 {
		t.Fatal("expected a warm call")
	}
	if !strings.HasPrefix(w.calls[0].ID, "tok123-") {
		t.Fatalf("id %q should start with run token %q", w.calls[0].ID, "tok123-")
	}
}

func TestPoolLeaseWarmsRequestedImageAndTracksFreeSlots(t *testing.T) {
	w := &stubWarmer{}
	p := New(2, 3, w, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	if slots := p.FreeSlots(); slots != 2 {
		t.Fatalf("initial FreeSlots = %d, want 2", slots)
	}
	vm, err := p.Lease(ctx, "image-a")
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if vm.Image != "image-a" {
		t.Fatalf("leased image = %q, want image-a", vm.Image)
	}
	if slots := p.FreeSlots(); slots != 1 {
		t.Fatalf("FreeSlots during lease = %d, want 1", slots)
	}
	p.Recycle(ctx, vm)
	if slots := p.FreeSlots(); slots != 2 {
		t.Fatalf("FreeSlots after recycle = %d, want 2", slots)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.calls) == 0 || w.calls[0].Image != "image-a" {
		t.Fatalf("first warm call = %+v, want image-a", w.calls)
	}
}

func TestPoolKeepsRequestedImageWarmAfterLease(t *testing.T) {
	w := &stubWarmer{}
	p := New(1, 3, w, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	vm, err := p.Lease(ctx, "image-a")
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	p.Recycle(ctx, vm)

	waitFor(t, func() bool {
		return imageHasWarmVM(p, "image-a")
	})
}

func TestPoolLRUEvictionOrder(t *testing.T) {
	w := &stubWarmer{}
	p := New(2, 3, w, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	vmA, err := p.Lease(ctx, "image-a")
	if err != nil {
		t.Fatalf("Lease image-a: %v", err)
	}
	p.Recycle(ctx, vmA)
	waitFor(t, func() bool { return imageHasWarmVM(p, "image-a") })

	vmB, err := p.Lease(ctx, "image-b")
	if err != nil {
		t.Fatalf("Lease image-b: %v", err)
	}
	p.Recycle(ctx, vmB)
	waitFor(t, func() bool {
		return imageHasWarmVM(p, "image-a") && imageHasWarmVM(p, "image-b")
	})

	vmA2, err := p.Lease(ctx, "image-a")
	if err != nil {
		t.Fatalf("Lease image-a again: %v", err)
	}
	p.Recycle(ctx, vmA2)
	waitFor(t, func() bool {
		return imageHasWarmVM(p, "image-a") && imageHasWarmVM(p, "image-b")
	})

	vmC, err := p.Lease(ctx, "image-c")
	if err != nil {
		t.Fatalf("Lease image-c: %v", err)
	}
	p.Recycle(ctx, vmC)

	waitFor(t, func() bool {
		return imageHasWarmVM(p, "image-a") && imageHasWarmVM(p, "image-c") && !imageHasWarmVM(p, "image-b")
	})
	if count := imageWarmCount(p); count != 2 {
		t.Fatalf("warm cache count = %d, want 2", count)
	}
}

func TestPoolEnforcesGoldenBudget(t *testing.T) {
	w := &stubWarmer{}
	p := New(2, 1, w, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	vmA, err := p.Lease(ctx, "image-a")
	if err != nil {
		t.Fatalf("Lease image-a: %v", err)
	}
	p.Recycle(ctx, vmA)

	vmB, err := p.Lease(ctx, "image-b")
	if err != nil {
		t.Fatalf("Lease image-b: %v", err)
	}
	p.Recycle(ctx, vmB)

	waitFor(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return strings.Contains(strings.Join(w.deletedGoldens, "\n"), "image-a")
	})
}

func TestPoolLeaseContextCancelledRestoresSlot(t *testing.T) {
	block := make(chan struct{})
	w := &stubWarmer{block: block}
	p := New(1, 1, w, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	leaseCtx, leaseCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer leaseCancel()
	_, err := p.Lease(leaseCtx, "image-a")
	if err == nil {
		t.Fatal("expected lease error when context is cancelled")
	}
	if slots := p.FreeSlots(); slots != 1 {
		t.Fatalf("FreeSlots after failed lease = %d, want 1", slots)
	}
	close(block)
}

func TestPoolShutdownTeardownsWarmVMs(t *testing.T) {
	w := &stubWarmer{}
	p := New(1, 3, w, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	vm, err := p.Lease(ctx, "image-a")
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	p.Recycle(ctx, vm)
	waitFor(t, func() bool { return imageHasWarmVM(p, "image-a") })

	p.Shutdown(context.Background())

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.torn) < 2 {
		t.Fatalf("expected active and idle VMs torn down, got %v", w.torn)
	}
}
