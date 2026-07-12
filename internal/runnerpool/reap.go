package runnerpool

import (
	"context"
	"log/slog"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
)

type busyCandidate struct {
	index     int
	slotIndex int
	slotCount int
	vm        *broker.WarmVM
	boundAt   time.Time
	jobID     int64
	runID     int64
	now       time.Time
}

// reap messages name the reason a busy slot was recycled.
const (
	reapPastMaxBindMessage = "runnerpool reaping worker (past max_bind ceiling)"
	reapNoActiveJobMessage = "runnerpool reaping worker (no active job process)"
)

// runningProbe caches one VM's running-slot map for a single reconcile pass, so
// the reap loop makes at most one Reattach call per VM.
type runningProbe struct {
	running map[int]bool
	err     error
}

// reapBusyWorkers recycles busy slots that have blown past a deadline. MaxBind is
// an absolute ceiling: a slot bound past it is recycled regardless of running
// state, so a hung-but-alive runner the guest still reports as running cannot
// hold its slot forever. Within MaxBind, a slot past the pickup timeout is
// recycled only when its guest execution is not running, read at most once per
// VM per pass; a probe error within MaxBind is treated as a possibly-transient
// blip and the slot is kept until the ceiling.
func (p *Pool) reapBusyWorkers(ctx context.Context) {
	candidates := p.busyCandidates()
	options := p.optionsSnapshot()
	probes := make(map[string]runningProbe)
	for _, candidate := range candidates {
		bindAge := candidate.now.Sub(candidate.boundAt)
		pastMaxBind := options.MaxBind > 0 && bindAge >= options.MaxBind
		if pastMaxBind {
			// The absolute MaxBind ceiling is enforced even without a prober.
			p.warnAndRequestBusyRecycle(ctx, candidate, bindAge, reapPastMaxBindMessage)
			continue
		}
		if p.prober == nil {
			continue
		}
		pastPickupTimeout := options.PickupTimeout > 0 && bindAge >= options.PickupTimeout
		if !pastPickupTimeout {
			continue
		}
		probe, ok := probes[candidate.vm.Name]
		if !ok {
			running, err := p.prober.RunningSlots(ctx, candidate.vm)
			probe = runningProbe{running: running, err: err}
			probes[candidate.vm.Name] = probe
		}
		if probe.err != nil {
			slog.WarnContext(ctx, "runnerpool running slots probe failed", "err", probe.err, "vm", candidate.vm.Name, "slot", candidate.slotIndex)
			continue
		}
		if !probe.running[candidate.slotIndex] {
			p.warnAndRequestBusyRecycle(ctx, candidate, bindAge, reapNoActiveJobMessage)
		}
	}
}

func (p *Pool) warnAndRequestBusyRecycle(ctx context.Context, candidate busyCandidate, bindAge time.Duration, message string) {
	if !p.claimSlotReapWarning(candidate) {
		return
	}
	slog.WarnContext(ctx, message, "vm", candidate.vm.Name, "run_id", candidate.runID, "job_id", candidate.jobID, "slot", candidate.slotIndex, "bind_age_seconds", int64(bindAge.Seconds()))
	p.requestBusyRecycle(candidate)
}

func (p *Pool) claimSlotReapWarning(candidate busyCandidate) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if candidate.index < 0 || candidate.index >= len(p.states) {
		return false
	}
	state := &p.states[candidate.index]
	if state.vm == nil ||
		state.vm.Name != candidate.vm.Name ||
		state.recycle ||
		candidate.slotIndex < 0 ||
		candidate.slotIndex >= len(state.slots) {
		return false
	}
	slot := &state.slots[candidate.slotIndex]
	if !slot.busy ||
		slot.jobID != candidate.jobID ||
		slot.runID != candidate.runID ||
		!slot.boundAt.Equal(candidate.boundAt) ||
		slot.reapWarnedAt.Equal(candidate.boundAt) {
		return false
	}
	slot.reapWarnedAt = candidate.boundAt
	return true
}

func (p *Pool) busyCandidates() []busyCandidate {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.options.Now()
	candidates := make([]busyCandidate, 0, len(p.states)*p.options.JobsPerVM)
	for index, state := range p.states {
		if state.vm == nil {
			continue
		}
		for slotIndex, slot := range state.slots {
			if !slot.busy || slot.boundAt.IsZero() {
				continue
			}
			candidates = append(candidates, busyCandidate{
				index:     index,
				slotIndex: slotIndex,
				slotCount: len(state.slots),
				vm:        state.vm,
				boundAt:   slot.boundAt,
				jobID:     slot.jobID,
				runID:     slot.runID,
				now:       now,
			})
		}
	}
	return candidates
}
