package runnerpool

import (
	"context"
	"log/slog"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
)

type statusProbeTarget struct {
	viewIndex int
	slotIndex int
	slotCount int
	vm        *broker.WarmVM
}

// Status returns a pool snapshot plus per-worker views. Active-job probes run
// after the pool lock is released so a slow guest probe cannot block workers.
func (p *Pool) Status(ctx context.Context) (Snapshot, []WorkerView) {
	p.mu.Lock()
	snapshot := p.snapshotLocked()
	now := p.options.Now()
	views := make([]WorkerView, 0, len(p.states))
	probeTargets := make([]statusProbeTarget, 0, len(p.states)*p.options.JobsPerVM)
	for index, state := range p.states {
		view := workerView(index, state, now)
		views = append(views, view)
		if p.prober == nil || state.vm == nil {
			continue
		}
		for slotIndex, slot := range state.slots {
			if !slot.busy {
				continue
			}
			probeTargets = append(probeTargets, statusProbeTarget{
				viewIndex: index,
				slotIndex: slotIndex,
				slotCount: len(state.slots),
				vm:        state.vm,
			})
		}
	}
	prober := p.prober
	p.mu.Unlock()

	if prober == nil {
		return snapshot, views
	}
	for _, target := range probeTargets {
		active, err := prober.HasActiveJob(ctx, target.vm, target.slotIndex, target.slotCount)
		if err != nil {
			slog.WarnContext(ctx, "runnerpool active job probe failed", "err", err, "vm", target.vm.Name, "slot", target.slotIndex)
			continue
		}
		activeJob := active
		if target.slotCount <= 1 {
			views[target.viewIndex].ActiveJob = &activeJob
			continue
		}
		if target.slotIndex >= 0 && target.slotIndex < len(views[target.viewIndex].Slots) {
			views[target.viewIndex].Slots[target.slotIndex].ActiveJob = &activeJob
		}
	}
	return snapshot, views
}

func workerView(index int, state workerState, now time.Time) WorkerView {
	view := WorkerView{
		Index:          index,
		VM:             "",
		Phase:          workerPhase(state),
		RunID:          0,
		BindAgeSeconds: 0,
		ActiveJob:      nil,
		LastError:      "",
		Slots:          nil,
	}
	if state.vm != nil {
		view.VM = state.vm.Name
	}
	if state.lastErr != nil {
		view.LastError = state.lastErr.Error()
	}
	if len(state.slots) == 0 {
		return view
	}
	if len(state.slots) <= 1 {
		slot := state.slots[0]
		view.RunID = slot.runID
		view.BindAgeSeconds = bindAgeSeconds(slot.boundAt, now)
		return view
	}
	view.Slots = make([]SlotView, 0, len(state.slots))
	for slotIndex, slot := range state.slots {
		slotView := slotView(slotIndex, state, slot, now)
		view.Slots = append(view.Slots, slotView)
		if view.RunID == 0 && slot.busy {
			view.RunID = slot.runID
			view.BindAgeSeconds = slotView.BindAgeSeconds
		}
		if view.LastError == "" && slot.lastErr != nil {
			view.LastError = slot.lastErr.Error()
		}
	}
	return view
}

func workerPhase(state workerState) string {
	if state.recycle {
		return "recycle"
	}
	if state.warming {
		return "warming"
	}
	if state.vm == nil {
		return "empty"
	}
	if workerBusy(state) {
		return "busy"
	}
	return "idle"
}

func slotView(index int, state workerState, slot slotState, now time.Time) SlotView {
	view := SlotView{
		Index:          index,
		Phase:          slotPhase(state, slot),
		RunID:          slot.runID,
		JobID:          slot.jobID,
		BindAgeSeconds: bindAgeSeconds(slot.boundAt, now),
		ActiveJob:      nil,
		LastError:      "",
	}
	if slot.lastErr != nil {
		view.LastError = slot.lastErr.Error()
	}
	return view
}

func slotPhase(state workerState, slot slotState) string {
	if slot.busy {
		return "busy"
	}
	if state.recycle {
		return "recycle"
	}
	if state.warming {
		return "warming"
	}
	if state.vm == nil {
		return "empty"
	}
	return "idle"
}

func bindAgeSeconds(boundAt time.Time, now time.Time) int64 {
	if boundAt.IsZero() {
		return 0
	}
	bindAge := now.Sub(boundAt)
	if bindAge <= 0 {
		return 0
	}
	return int64(bindAge.Seconds())
}

func workerBusy(state workerState) bool {
	for _, slot := range state.slots {
		if slot.busy {
			return true
		}
	}
	return false
}

func resetSlots(slots []slotState) {
	for index := range slots {
		cancel := slots[index].jobCancel
		if cancel != nil {
			cancel()
		}
		slots[index] = emptySlotState()
	}
}

func emptySlotState() slotState {
	return slotState{
		boundAt:         time.Time{},
		busy:            false,
		jobID:           0,
		runID:           0,
		jobCancel:       nil,
		cpuStalledSince: time.Time{},
		stallWarnedAt:   time.Time{},
		reapWarnedAt:    time.Time{},
		lastErr:         nil,
	}
}
