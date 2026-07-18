package runnerpool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"goodkind.io/gha-mac-broker/internal/broker"
)

func (p *Pool) claimWorker(index int) (*broker.WarmVM, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := &p.states[index]
	if state.vm == nil || state.warming {
		return nil, false
	}
	return state.vm, true
}

func (p *Pool) teardownVM(ctx context.Context, vm *broker.WarmVM) {
	if vm == nil {
		return
	}
	p.warmer.Teardown(ctx, vm)
}

func (p *Pool) nextID() string {
	next := p.counter.Add(1)
	p.mu.Lock()
	runToken := p.options.RunToken
	p.mu.Unlock()
	return fmt.Sprintf("%s-%d", runToken, next)
}

func recoverGoroutine(ctx context.Context, label string) {
	if recovered := recover(); recovered != nil {
		slog.ErrorContext(ctx, label+" panic recovered", "err", fmt.Errorf("panic: %v", recovered))
	}
}

func runnerNameForSlot(vmName string, slotIndex int, slotCount int) string {
	if slotCount <= 1 {
		return vmName
	}
	return fmt.Sprintf("%s-slot-%d", vmName, slotIndex)
}

func runnerNameBelongsToVM(vmName string, runnerName string, slotCount int) bool {
	if runnerName == vmName {
		return true
	}
	if slotCount > 1 {
		for slotIndex := range slotCount {
			if runnerName == runnerNameForSlot(vmName, slotIndex, slotCount) {
				return true
			}
		}
	}
	slotSuffix, found := strings.CutPrefix(runnerName, vmName+"-slot-")
	if !found {
		return false
	}
	slotIndex, err := strconv.Atoi(slotSuffix)
	return err == nil && slotIndex >= 0
}

func (p *Pool) slotLoop(ctx context.Context, index int, slotIndex int, vm *broker.WarmVM) {
	// An adopted slot resumes its inherited execution from its cursor before it
	// serves any new job, so a busy slot carried across a broker restart drains to
	// its terminal on the same VM.
	if execID, cursor, jobCtx, cancel, slotCount, ok := p.claimResumeSlot(ctx, index, slotIndex, vm); ok {
		err := func() (err error) {
			defer func() {
				if recovered := recover(); recovered != nil {
					err = fmt.Errorf("panic: %v", recovered)
				}
				cancel(nil)
			}()
			return p.runner.ResumeJob(jobCtx, vm, execID, cursor, slotIndex, slotCount)
		}()
		switch {
		case err == nil:
			// Resume drained the inherited execution to a zero-exit terminal (or an
			// expired execution), so the slot frees and the VM keeps serving.
			p.finishSlotJob(index, slotIndex, vm, nil)
		case errors.Is(err, broker.ErrJobTerminal):
			// The adopted job ran to its terminal with a nonzero exit. That is a
			// normal job failure, not a resume failure, so free the slot and keep the
			// VM available for the next job, exactly as a non-adopted completion does.
			p.finishSlotJob(index, slotIndex, vm, err)
		case errors.Is(err, context.Canceled):
			// A worker shutdown detached the resume drain; the inherited execution
			// keeps running and is re-adopted later, so do not recycle the VM.
			slog.DebugContext(ctx, "runnerpool resume detached on shutdown", "vm", vm.Name, "slot", slotIndex, "execution", execID)
			p.finishSlotJob(index, slotIndex, vm, err)
		default:
			// Resume could not attach to or drain the inherited execution to a
			// terminal, which may still be running on the guest. Freeing the slot for
			// a new job could dispatch onto a busy guest slot, so recycle the VM: it
			// is torn down and re-warmed clean rather than serving new work here.
			slog.WarnContext(ctx, "runnerpool resume job failed; recycling vm", "err", err, "vm", vm.Name, "slot", slotIndex, "execution", execID)
			p.recycleWorkerFreeingSlot(index, slotIndex, vm, err)
			return
		}
	}
	for {
		job, boundAt, jobCtx, cancel, slotCount, ok := p.waitForSlotJob(ctx, index, slotIndex, vm)
		if !ok {
			return
		}
		err := func() (err error) {
			defer func() {
				if recovered := recover(); recovered != nil {
					err = fmt.Errorf("panic: %v", recovered)
				}
				cancel(nil)
				p.finishSlotJob(index, slotIndex, vm, err)
			}()
			return p.runner.RunJob(jobCtx, vm, job.Repo, runnerNameForSlot(vm.Name, slotIndex, slotCount), slotIndex, slotCount, job.JobID, job.RunID, boundAt)
		}()
		if err != nil {
			slog.WarnContext(ctx, "runnerpool job failed", "err", err, "repo", job.Repo, "job_id", job.JobID, "run_id", job.RunID, "vm", vm.Name, "slot", slotIndex)
			if errors.Is(err, broker.ErrDrainStalled) {
				// The guest status stream went silent with no terminal. Recycle the VM
				// so the dead or wedged guest is torn down and re-warmed clean, rather
				// than serving new work on a slot the host can no longer track.
				p.recycleWorkerFreeingSlot(index, slotIndex, vm, err)
				return
			}
		}
	}
}

// claimResumeSlot installs a cancelable job context on an adopted busy slot that
// has not yet been resumed, so the reap and cancel paths can cancel the resumed
// drain. It returns the execution id and cursor to resume from.
func (p *Pool) claimResumeSlot(ctx context.Context, index int, slotIndex int, vm *broker.WarmVM) (string, uint64, context.Context, context.CancelCauseFunc, int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ctx.Err() != nil || p.shuttingDown {
		return "", 0, nil, nil, 0, false
	}
	if index < 0 || index >= len(p.states) {
		return "", 0, nil, nil, 0, false
	}
	state := &p.states[index]
	if state.vm == nil || state.vm.Name != vm.Name {
		return "", 0, nil, nil, 0, false
	}
	if slotIndex < 0 || slotIndex >= len(state.slots) {
		return "", 0, nil, nil, 0, false
	}
	slot := &state.slots[slotIndex]
	if !slot.busy || !slot.adopted || slot.executionID == "" || slot.jobCancel != nil {
		return "", 0, nil, nil, 0, false
	}
	jobCtx, cancel := context.WithCancelCause(ctx)
	slot.jobCancel = cancel
	return slot.executionID, slot.resumeCursor, jobCtx, cancel, len(state.slots), true
}

// recycleWorkerFreeingSlot frees the slot and marks the worker for recycle, so a
// guest slot the host can no longer track is not reused under a new job. This
// covers an inherited execution that could not be resumed and a drain that
// stalled with no terminal. Marking recycle stops waitForSlotJob from dispatching
// new work, and the worker loop tears the VM down and re-warms clean once its
// slots are idle.
func (p *Pool) recycleWorkerFreeingSlot(index int, slotIndex int, vm *broker.WarmVM, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if index < 0 || index >= len(p.states) {
		return
	}
	state := &p.states[index]
	if state.vm == nil || state.vm.Name != vm.Name || slotIndex < 0 || slotIndex >= len(state.slots) {
		return
	}
	slot := emptySlotState()
	slot.lastErr = err
	state.slots[slotIndex] = slot
	state.recycle = true
	p.cond.Broadcast()
}
