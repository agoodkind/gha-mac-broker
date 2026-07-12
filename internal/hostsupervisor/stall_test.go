//go:build unix

package hostsupervisor

import (
	"testing"
	"time"
)

// TestStallCheckRequestsReloadWhenProgressStops drives the watchdog decision with
// a stubbed progress stamp: a stale last-progress time while steady requests a
// reload, and a fresh stamp does not. This is the seam the reconcile stall
// watchdog runs on each tick.
func TestStallCheckRequestsReloadWhenProgressStops(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	supervisor := New(Options{
		StallTimeout: time.Minute,
		Now:          func() time.Time { return now },
	})

	// A fresh stamp while steady is not a stall.
	supervisor.mu.Lock()
	supervisor.state = StateSteady
	supervisor.lastProgress = now.Add(-30 * time.Second)
	supervisor.mu.Unlock()
	if supervisor.stallCheck(now) {
		t.Fatal("stallCheck reported a stall for fresh progress")
	}
	if reloadRequested(supervisor) {
		t.Fatal("fresh progress requested a reload")
	}

	// A stamp older than the stall timeout is a stall and requests exactly one reload.
	supervisor.mu.Lock()
	supervisor.lastProgress = now.Add(-2 * time.Minute)
	supervisor.mu.Unlock()
	if !supervisor.stallCheck(now) {
		t.Fatal("stallCheck did not report a stall for stale progress")
	}
	if !reloadRequested(supervisor) {
		t.Fatal("stall did not request a reload")
	}

	// The clock reset means the next immediate tick does not fire again.
	if supervisor.stallCheck(now) {
		t.Fatal("stallCheck fired twice for a single stall")
	}
}

// TestStallCheckIgnoresNonSteadyState proves the watchdog does not restart a
// worker mid-replacement, so a swap already in flight is not compounded.
func TestStallCheckIgnoresNonSteadyState(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	supervisor := New(Options{
		StallTimeout: time.Minute,
		Now:          func() time.Time { return now },
	})
	supervisor.mu.Lock()
	supervisor.state = StateReplacing
	supervisor.lastProgress = now.Add(-2 * time.Minute)
	supervisor.mu.Unlock()
	if supervisor.stallCheck(now) {
		t.Fatal("stallCheck fired while replacing")
	}
}

func reloadRequested(supervisor *Supervisor) bool {
	select {
	case <-supervisor.reloadCh:
		return true
	default:
		return false
	}
}
