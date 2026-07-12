//go:build unix

package guestsupervisor

import (
	"testing"
)

// TestSetSlotsGrowUpdatesCount proves a grow request updates the supervisor slot
// count and returns the applied value, so the next worker generation inherits it.
func TestSetSlotsGrowUpdatesCount(t *testing.T) {
	supervisor := New(Options{SlotCount: 1})
	socketPath := startControlOnly(t, supervisor)

	applied, err := ConfigureSlots(socketPath, 2)
	if err != nil {
		t.Fatalf("ConfigureSlots grow: %v", err)
	}
	if applied != 2 {
		t.Fatalf("applied = %d, want 2", applied)
	}
	if got := supervisor.slotCount.Load(); got != 2 {
		t.Fatalf("supervisor slot count = %d, want 2", got)
	}
}

// TestSetSlotsNoOpWhenEqual proves a request for the current count is idempotent:
// it returns the current count and leaves it unchanged.
func TestSetSlotsNoOpWhenEqual(t *testing.T) {
	supervisor := New(Options{SlotCount: 2})
	socketPath := startControlOnly(t, supervisor)

	applied, err := ConfigureSlots(socketPath, 2)
	if err != nil {
		t.Fatalf("ConfigureSlots no-op: %v", err)
	}
	if applied != 2 {
		t.Fatalf("applied = %d, want 2", applied)
	}
	if got := supervisor.slotCount.Load(); got != 2 {
		t.Fatalf("supervisor slot count = %d, want 2", got)
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
	if got := supervisor.slotCount.Load(); got != 1 {
		t.Fatalf("supervisor slot count = %d, want 1 unchanged", got)
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
	if got := supervisor.slotCount.Load(); got != 2 {
		t.Fatalf("supervisor slot count = %d, want 2 unchanged after a rejected shrink", got)
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
	if got := supervisor.slotCount.Load(); got != 2 {
		t.Fatalf("supervisor slot count = %d, want 2", got)
	}
}
