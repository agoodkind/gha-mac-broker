package runnerpool

import (
	"context"
	"log/slog"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
)

func (p *Pool) adoptWorkers(ctx context.Context) []broker.AdoptedVM {
	adopted, err := p.warmer.Adopt(ctx, p.options.Image, p.options.JobsPerVM, p.options.RunnerCount)
	if err != nil {
		slog.WarnContext(ctx, "runnerpool adopt running vms failed", "err", err)
		return nil
	}
	return adopted
}

func (p *Pool) installAdoptedWorkersLocked(adopted []broker.AdoptedVM) {
	now := p.options.Now()
	for index, adoptedVM := range adopted {
		if index >= len(p.states) || adoptedVM.VM == nil {
			return
		}
		state := &p.states[index]
		state.vm = adoptedVM.VM
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
			boundAt := binding.BoundAt
			if boundAt.IsZero() {
				boundAt = now
			}
			state.slots[binding.SlotIndex] = slotState{
				boundAt:         boundAt,
				busy:            true,
				jobID:           binding.JobID,
				runID:           binding.RunID,
				jobCancel:       nil,
				cpuStalledSince: time.Time{},
				stallWarnedAt:   time.Time{},
				reapWarnedAt:    time.Time{},
				adopted:         true,
				lastErr:         nil,
			}
			state.idleSince = time.Time{}
		}
	}
}
