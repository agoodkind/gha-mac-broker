//go:build unix

package guestsupervisor

import (
	"testing"
)

// TestSetSlotsGrowUpdatesDesiredCount proves a grow request updates the desired
// count the next worker generation is spawned with and returns the requested
// value. Without a worker to attach, the serving count stays at the old value.
func TestSetSlotsGrowUpdatesDesiredCount(t *testing.T) {
	supervisor := New(Options{SlotCount: 1})
	socketPath := startControlOnly(t, supervisor)

	applied, err := ConfigureSlots(socketPath, 2)
	if err != nil {
		t.Fatalf("ConfigureSlots grow: %v", err)
	}
	if applied != 2 {
		t.Fatalf("applied = %d, want 2", applied)
	}
	if got := supervisor.desiredSlotCount.Load(); got != 2 {
		t.Fatalf("desired slot count = %d, want 2", got)
	}
	if got := supervisor.servingSlotCount.Load(); got != 1 {
		t.Fatalf("serving slot count = %d, want 1 until a worker attaches", got)
	}
}

// TestSetSlotsNoOpWhenEqualServing proves a request for the count already being
// served is idempotent: it returns that count and leaves both counts unchanged.
func TestSetSlotsNoOpWhenEqualServing(t *testing.T) {
	supervisor := New(Options{SlotCount: 2})
	socketPath := startControlOnly(t, supervisor)

	applied, err := ConfigureSlots(socketPath, 2)
	if err != nil {
		t.Fatalf("ConfigureSlots no-op: %v", err)
	}
	if applied != 2 {
		t.Fatalf("applied = %d, want 2", applied)
	}
	if got := supervisor.servingSlotCount.Load(); got != 2 {
		t.Fatalf("serving slot count = %d, want 2", got)
	}
	if got := supervisor.desiredSlotCount.Load(); got != 2 {
		t.Fatalf("desired slot count = %d, want 2", got)
	}
}

// TestSetSlotsRejectsZero proves a zero slot count is rejected and the count is
// left unchanged.
func TestSetSlotsRejectsZero(t *testing.T) {
	supervisor := New(Options{SlotCount: 1})
	socketPath := startControlOnly(t, supervisor)

	if _, err := ConfigureSlots(socketPath, 0); err == nil {
		t.Fatal("ConfigureSlots(0) = nil, want rejection")
	}
	if got := supervisor.desiredSlotCount.Load(); got != 1 {
		t.Fatalf("desired slot count = %d, want 1 unchanged", got)
	}
}

// TestSetSlotsShrinkBelowBusyRejected proves a shrink that would strand a running
// slot is rejected and the slot count is left unchanged, so a running job is
// never orphaned.
func TestSetSlotsShrinkBelowBusyRejected(t *testing.T) {
	supervisor := New(Options{SlotCount: 2})
	socketPath := startControlOnly(t, supervisor)

	// A running runner occupies slot 1.
	supervisor.mu.Lock()
	supervisor.children[4242] = &child{slot: 1, pid: 4242, exited: false}
	supervisor.mu.Unlock()

	if _, err := ConfigureSlots(socketPath, 1); err == nil {
		t.Fatal("ConfigureSlots shrink below busy slot = nil, want rejection")
	}
	if got := supervisor.desiredSlotCount.Load(); got != 2 {
		t.Fatalf("desired slot count = %d, want 2 unchanged after a rejected shrink", got)
	}
}

// TestSetSlotsShrinkAboveBusyAllowed proves a shrink that keeps every running
// slot in range is applied, so freeing high slots after a job on a low slot works.
func TestSetSlotsShrinkAboveBusyAllowed(t *testing.T) {
	supervisor := New(Options{SlotCount: 3})
	socketPath := startControlOnly(t, supervisor)

	// A running runner occupies slot 0, so shrinking to 2 keeps it in range.
	supervisor.mu.Lock()
	supervisor.children[7] = &child{slot: 0, pid: 7, exited: false}
	supervisor.mu.Unlock()

	applied, err := ConfigureSlots(socketPath, 2)
	if err != nil {
		t.Fatalf("ConfigureSlots shrink above busy: %v", err)
	}
	if applied != 2 {
		t.Fatalf("applied = %d, want 2", applied)
	}
	if got := supervisor.desiredSlotCount.Load(); got != 2 {
		t.Fatalf("desired slot count = %d, want 2", got)
	}
}
