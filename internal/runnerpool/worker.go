package runnerpool

import (
	"context"
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
