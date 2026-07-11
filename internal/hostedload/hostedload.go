// Package hostedload tracks GitHub-hosted macOS job load for capacity decisions.
package hostedload

import (
	"strings"
	"sync"
)

// Tracker tracks the set of in-progress GitHub-hosted macOS job IDs.
//
// It is a set of job IDs rather than a bare counter for idempotency: GitHub can
// deliver the same webhook more than once, so counting each delivery would
// over-count in-progress jobs. Keying on the job ID means a duplicate
// in-progress webhook is a no-op and a completed webhook removes the exact job
// that finished, so the count reflects distinct live jobs.
type Tracker struct {
	mu   sync.Mutex
	jobs map[int64]struct{}
}

// NewTracker returns a Tracker with an initialized backing set.
func NewTracker() *Tracker {
	return &Tracker{mu: sync.Mutex{}, jobs: make(map[int64]struct{})}
}

// MarkInProgress records jobID as in-progress. A duplicate call for the same
// jobID leaves the set unchanged.
func (t *Tracker) MarkInProgress(jobID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.jobs[jobID] = struct{}{}
}

// MarkCompleted removes jobID from the in-progress set. It is safe to call for a
// jobID that is not present.
func (t *Tracker) MarkCompleted(jobID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.jobs, jobID)
}

// Reconcile atomically replaces the whole in-progress set with a copy of
// jobIDs. A nil jobIDs is treated as an empty set. Copying decouples the
// Tracker from the caller's map so later mutations do not leak in.
func (t *Tracker) Reconcile(jobIDs map[int64]struct{}) {
	replacement := make(map[int64]struct{}, len(jobIDs))
	for jobID := range jobIDs {
		replacement[jobID] = struct{}{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.jobs = replacement
}

// Count returns the number of distinct in-progress jobs.
func (t *Tracker) Count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.jobs)
}

const hostedMacOSLabelPrefix = "macos-"

// IsHostedMacOSJob reports whether a job requested a GitHub-hosted macOS runner
// and is not a self-hosted pool job.
//
// It returns true only when both conditions hold:
//
//	(a) at least one jobLabels entry case-insensitively has the "macos-" prefix
//	    (hosted image labels such as macos-latest, macos-14, macos-15-large), and
//	(b) no jobLabels entry case-insensitively equals any poolLabels entry, since
//	    a self-hosted pool job carries labels like self-hosted / macOS / ARM64 /
//	    agk-local-macos-26 that route it to the local pool rather than a hosted
//	    runner.
func IsHostedMacOSJob(jobLabels, poolLabels []string) bool {
	hasHostedLabel := false
	for _, label := range jobLabels {
		if strings.HasPrefix(strings.ToLower(label), hostedMacOSLabelPrefix) {
			hasHostedLabel = true
			break
		}
	}
	if !hasHostedLabel {
		return false
	}
	for _, label := range jobLabels {
		for _, poolLabel := range poolLabels {
			if strings.EqualFold(label, poolLabel) {
				return false
			}
		}
	}
	return true
}
