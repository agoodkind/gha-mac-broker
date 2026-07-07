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

func (p *Pool) reapBusyWorkers(ctx context.Context) {
	candidates := p.busyCandidates()
	options := p.optionsSnapshot()
	for _, candidate := range candidates {
		bindAge := candidate.now.Sub(candidate.boundAt)
		pastMaxBind := options.MaxBind > 0 && bindAge >= options.MaxBind
		pastPickupTimeout := options.PickupTimeout > 0 && bindAge >= options.PickupTimeout
		if !pastMaxBind && !pastPickupTimeout {
			continue
		}
		if p.prober == nil {
			continue
		}
		active, err := p.prober.HasActiveJob(ctx, candidate.vm, candidate.slotIndex, candidate.slotCount)
		if err != nil {
			p.handleActiveJobProbeError(ctx, candidate, bindAge, pastMaxBind, err)
			continue
		}
		if !active {
			p.warnAndRequestBusyRecycle(ctx, candidate, bindAge, "runnerpool reaping worker (no active job process)")
			continue
		}
		if !pastPickupTimeout {
			continue
		}
		cpuActivity, err := p.prober.SlotCPUActivity(ctx, candidate.vm, candidate.slotIndex, candidate.slotCount)
		if err != nil {
			slog.WarnContext(ctx, "runnerpool slot cpu activity probe failed", "err", err, "vm", candidate.vm.Name, "slot", candidate.slotIndex)
			continue
		}
		if cpuActivity < 0 {
			continue
		}
		cpuStalledSince, ok := p.observeSlotCPU(candidate, cpuActivity, candidate.now)
		if !ok || cpuStalledSince.IsZero() {
			continue
		}
		p.maybeWarnStalledBusyWorker(ctx, candidate, bindAge, cpuStalledSince, cpuActivity)
	}
}

func (p *Pool) handleActiveJobProbeError(ctx context.Context, candidate busyCandidate, bindAge time.Duration, pastMaxBind bool, err error) {
	slog.WarnContext(ctx, "runnerpool active job probe failed", "err", err, "vm", candidate.vm.Name, "slot", candidate.slotIndex)
	if !pastMaxBind {
		return
	}
	p.warnAndRequestBusyRecycle(ctx, candidate, bindAge, "runnerpool reaping worker (probe error past max_bind)")
}

func (p *Pool) warnAndRequestBusyRecycle(ctx context.Context, candidate busyCandidate, bindAge time.Duration, message string) {
	if !p.claimSlotReapWarning(candidate) {
		return
	}
	slog.WarnContext(ctx, message, "vm", candidate.vm.Name, "run_id", candidate.runID, "job_id", candidate.jobID, "slot", candidate.slotIndex, "bind_age_seconds", int64(bindAge.Seconds()))
	p.requestBusyRecycle(candidate)
}

func (p *Pool) maybeWarnStalledBusyWorker(ctx context.Context, candidate busyCandidate, bindAge time.Duration, cpuStalledSince time.Time, cpuActivity float64) {
	stalledFor := candidate.now.Sub(cpuStalledSince)
	if stalledFor < p.options.StallTimeout {
		return
	}
	if !p.claimSlotStallWarning(candidate) {
		return
	}
	slog.WarnContext(ctx, "runnerpool worker stalled (no cpu progress)", "vm", candidate.vm.Name, "run_id", candidate.runID, "job_id", candidate.jobID, "slot", candidate.slotIndex, "bind_age_seconds", int64(bindAge.Seconds()), "stalled_for_seconds", int64(stalledFor.Seconds()), "cpu_percent", cpuActivity)
	if !p.options.StallReap {
		return
	}
	p.requestBusyRecycle(candidate)
}

func (p *Pool) observeSlotCPU(candidate busyCandidate, cpuActivity float64, now time.Time) (time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if candidate.index < 0 || candidate.index >= len(p.states) {
		return time.Time{}, false
	}
	state := &p.states[candidate.index]
	if state.vm == nil ||
		state.vm.Name != candidate.vm.Name ||
		candidate.slotIndex < 0 ||
		candidate.slotIndex >= len(state.slots) {
		return time.Time{}, false
	}
	slot := &state.slots[candidate.slotIndex]
	if !slot.busy ||
		slot.jobID != candidate.jobID ||
		slot.runID != candidate.runID ||
		!slot.boundAt.Equal(candidate.boundAt) {
		return time.Time{}, false
	}
	if cpuActivity >= stallCPUThresholdPercent {
		slot.cpuStalledSince = time.Time{}
		return slot.cpuStalledSince, true
	}
	if slot.cpuStalledSince.IsZero() {
		slot.cpuStalledSince = now
	}
	return slot.cpuStalledSince, true
}

func (p *Pool) claimSlotStallWarning(candidate busyCandidate) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if candidate.index < 0 || candidate.index >= len(p.states) {
		return false
	}
	state := &p.states[candidate.index]
	if state.vm == nil ||
		state.vm.Name != candidate.vm.Name ||
		candidate.slotIndex < 0 ||
		candidate.slotIndex >= len(state.slots) {
		return false
	}
	slot := &state.slots[candidate.slotIndex]
	if !slot.busy ||
		slot.jobID != candidate.jobID ||
		slot.runID != candidate.runID ||
		!slot.boundAt.Equal(candidate.boundAt) ||
		slot.stallWarnedAt.Equal(candidate.boundAt) {
		return false
	}
	slot.stallWarnedAt = candidate.boundAt
	return true
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
