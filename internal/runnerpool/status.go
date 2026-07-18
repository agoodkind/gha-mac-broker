package runnerpool

import (
	"context"
	"log/slog"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
)

type statusProbeTarget struct {
	viewIndex int
	slotCount int
	busySlots []int
	vm        *broker.WarmVM
}

// statusProbeTimeout bounds the best-effort running-slots enrichment on /status so
// a frozen guest cannot make the endpoint unresponsive. It is a var so tests can
// shorten it. It sits well below the broker's per-call checkAliveTimeout, so the
// /status path returns within a couple of seconds even when a guest RPC over
// `tart exec` would otherwise block for the full liveness window. One shared
// deadline caps total latency across every busy VM, since the per-VM probes run
// sequentially.
var statusProbeTimeout = 2 * time.Second

// Status returns a pool snapshot plus per-worker views. The running-slots probe
// runs after the pool lock is released, one Reattach call per VM, so a slow
// guest cannot block workers. The probe is bounded by statusProbeTimeout, so a
// frozen guest leaves ActiveJob nil (unknown) rather than holding the endpoint.
func (p *Pool) Status(ctx context.Context) (Snapshot, []WorkerView) {
	p.mu.Lock()
	snapshot := p.snapshotLocked()
	now := p.options.Now()
	views := make([]WorkerView, 0, len(p.states))
	probeTargets := make([]statusProbeTarget, 0, len(p.states))
	for index, state := range p.states {
		view := workerView(index, state, now)
		views = append(views, view)
		if p.prober == nil || state.vm == nil {
			continue
		}
		busySlots := make([]int, 0, len(state.slots))
		for slotIndex, slot := range state.slots {
			if slot.busy {
				busySlots = append(busySlots, slotIndex)
			}
		}
		if len(busySlots) == 0 {
			continue
		}
		probeTargets = append(probeTargets, statusProbeTarget{
			viewIndex: index,
			slotCount: len(state.slots),
			busySlots: busySlots,
			vm:        state.vm,
		})
	}
	prober := p.prober
	p.mu.Unlock()

	if prober == nil {
		return snapshot, views
	}
	// Bound the whole probe phase with one shared deadline. The snapshot and
	// worker views are already computed under the lock and returned regardless;
	// only the ActiveJob field depends on the guest probe, so a probe that exceeds
	// this deadline leaves ActiveJob nil (unknown) instead of blocking /status.
	probeCtx, cancelProbe := context.WithTimeout(ctx, statusProbeTimeout)
	defer cancelProbe()
	for _, target := range probeTargets {
		running, err := prober.RunningSlots(probeCtx, target.vm)
		if err != nil {
			slog.WarnContext(ctx, "runnerpool running slots probe failed", "err", err, "vm", target.vm.Name)
			continue
		}
		for _, slotIndex := range target.busySlots {
			active := running[slotIndex]
			if target.slotCount <= 1 {
				views[target.viewIndex].ActiveJob = &active
				continue
			}
			if slotIndex >= 0 && slotIndex < len(views[target.viewIndex].Slots) {
				views[target.viewIndex].Slots[slotIndex].ActiveJob = &active
			}
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
			// A worker reset is a plain cancellation, so the drain detaches without
			// killing the guest job; a later Reattach re-adopts it.
			cancel(nil)
		}
		slots[index] = emptySlotState()
	}
}

func emptySlotState() slotState {
	return slotState{
		boundAt:      time.Time{},
		busy:         false,
		jobID:        0,
		runID:        0,
		executionID:  "",
		resumeCursor: 0,
		jobCancel:    nil,
		reapWarnedAt: time.Time{},
		adopted:      false,
		lastErr:      nil,
	}
}
