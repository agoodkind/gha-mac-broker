package runnerpool

import (
	"context"
	"log/slog"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
)

func (p *Pool) adoptWorkers(ctx context.Context) []broker.AdoptedVM {
	// Binder.Adopt applies this limit after liveness filtering, so a failed
	// probe cannot consume a pool slot before later live VMs are considered.
	adopted, err := p.warmer.Adopt(ctx, p.options.Image, p.options.JobsPerVM, p.options.RunnerCount)
	if err != nil {
		slog.WarnContext(ctx, "runnerpool adopt running vms failed", "err", err)
		return nil
	}
	return adopted
}

func (p *Pool) installAdoptedWorkersLocked(adopted []broker.AdoptedVM) {
	now := p.options.Now()
	stateIndex := 0
	for _, adoptedVM := range adopted {
		if stateIndex >= len(p.states) {
			break
		}
		if adoptedVM.VM == nil {
			continue
		}
		state := &p.states[stateIndex]
		stateIndex++
		state.vm = adoptedVM.VM
		// Tart does not expose VM creation time here, so adopted workers age
		// from adoption time and still participate in MaxAge recycling.
		state.bornAt = now
		state.idleSince = now
		state.warming = false
		state.recycle = false
		state.lastErr = nil
		state.slots = make([]slotState, p.options.JobsPerVM)
		for _, binding := range adoptedVM.Slots {
			if binding.SlotIndex < 0 || binding.SlotIndex >= len(state.slots) {
				continue
			}
			if !adoptedSlotBindingBusy(binding) {
				continue
			}
			boundAt := binding.BoundAt
			if boundAt.IsZero() {
				boundAt = now
			}
			state.slots[binding.SlotIndex] = slotState{
				boundAt:      boundAt,
				busy:         true,
				jobID:        binding.JobID,
				runID:        binding.RunID,
				executionID:  binding.ExecutionID,
				resumeCursor: binding.ResumeCursor,
				jobCancel:    nil,
				reapWarnedAt: time.Time{},
				adopted:      true,
				lastErr:      nil,
			}
			state.idleSince = time.Time{}
		}
	}
}

func adoptedSlotBindingBusy(binding broker.SlotBinding) bool {
	if binding.ObservedActive {
		return true
	}
	return binding.HasJobMetadata()
}
