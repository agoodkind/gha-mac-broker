package hostedload

import (
	"sync"
	"testing"
)

func TestTrackerMarkInProgressAddsAndDedupes(t *testing.T) {
	tracker := NewTracker()
	tracker.MarkInProgress(1)
	if got := tracker.Count(); got != 1 {
		t.Fatalf("after one MarkInProgress Count = %d, want 1", got)
	}
	tracker.MarkInProgress(1)
	if got := tracker.Count(); got != 1 {
		t.Fatalf("after duplicate MarkInProgress Count = %d, want 1", got)
	}
	tracker.MarkInProgress(2)
	if got := tracker.Count(); got != 2 {
		t.Fatalf("after second distinct MarkInProgress Count = %d, want 2", got)
	}
}

func TestTrackerMarkCompletedRemovesAndIsIdempotent(t *testing.T) {
	tracker := NewTracker()
	tracker.MarkInProgress(1)
	tracker.MarkInProgress(2)
	tracker.MarkCompleted(1)
	if got := tracker.Count(); got != 1 {
		t.Fatalf("after MarkCompleted Count = %d, want 1", got)
	}
	tracker.MarkCompleted(1)
	if got := tracker.Count(); got != 1 {
		t.Fatalf("after MarkCompleted of absent id Count = %d, want 1", got)
	}
	tracker.MarkCompleted(99)
	if got := tracker.Count(); got != 1 {
		t.Fatalf("after MarkCompleted of never-present id Count = %d, want 1", got)
	}
}

func TestTrackerReconcileReplacesSet(t *testing.T) {
	tracker := NewTracker()
	tracker.MarkInProgress(1)
	tracker.MarkInProgress(2)
	tracker.MarkInProgress(3)

	tracker.Reconcile(map[int64]struct{}{4: {}, 5: {}})
	if got := tracker.Count(); got != 2 {
		t.Fatalf("after Reconcile to smaller set Count = %d, want 2", got)
	}

	tracker.Reconcile(map[int64]struct{}{})
	if got := tracker.Count(); got != 0 {
		t.Fatalf("after Reconcile to empty set Count = %d, want 0", got)
	}

	tracker.Reconcile(nil)
	if got := tracker.Count(); got != 0 {
		t.Fatalf("after Reconcile(nil) Count = %d, want 0", got)
	}

	tracker.Reconcile(map[int64]struct{}{7: {}})
	if got := tracker.Count(); got != 1 {
		t.Fatalf("after Reconcile to non-empty set Count = %d, want 1", got)
	}
}

func TestTrackerReconcileCopiesInput(t *testing.T) {
	tracker := NewTracker()
	input := map[int64]struct{}{1: {}, 2: {}}
	tracker.Reconcile(input)

	// Mutating the caller's map after Reconcile must not affect the Tracker.
	input[3] = struct{}{}
	delete(input, 1)
	if got := tracker.Count(); got != 2 {
		t.Fatalf("Reconcile did not copy input, Count = %d, want 2", got)
	}
}

func TestTrackerConcurrentAccess(t *testing.T) {
	tracker := NewTracker()
	const workers = 50

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int64) {
			defer wg.Done()
			tracker.MarkInProgress(id)
			tracker.Count()
			tracker.MarkCompleted(id)
		}(int64(i))
	}
	wg.Wait()

	if got := tracker.Count(); got != 0 {
		t.Fatalf("after concurrent Mark/Complete Count = %d, want 0", got)
	}
}

func TestIsHostedMacOSJob(t *testing.T) {
	poolLabels := []string{"self-hosted", "macOS", "ARM64", "agk-local-macos-26"}

	tests := []struct {
		name      string
		jobLabels []string
		want      bool
	}{
		{
			name:      "macos-latest hosted",
			jobLabels: []string{"macos-latest"},
			want:      true,
		},
		{
			name:      "macos-14 hosted",
			jobLabels: []string{"macos-14"},
			want:      true,
		},
		{
			name:      "macos-15-large hosted",
			jobLabels: []string{"macos-15-large"},
			want:      true,
		},
		{
			name:      "self-hosted pool job",
			jobLabels: []string{"self-hosted", "macOS", "ARM64", "agk-local-macos-26"},
			want:      false,
		},
		{
			name:      "ubuntu-latest not macos",
			jobLabels: []string{"ubuntu-latest"},
			want:      false,
		},
		{
			name:      "empty job labels",
			jobLabels: []string{},
			want:      false,
		},
		{
			name:      "mixed hosted macos label and pool label",
			jobLabels: []string{"macos-14", "agk-local-macos-26"},
			want:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsHostedMacOSJob(tc.jobLabels, poolLabels); got != tc.want {
				t.Fatalf("IsHostedMacOSJob(%v, %v) = %v, want %v", tc.jobLabels, poolLabels, got, tc.want)
			}
		})
	}
}
