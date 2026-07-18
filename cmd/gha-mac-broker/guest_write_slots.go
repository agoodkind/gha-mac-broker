package main

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"goodkind.io/gha-mac-broker/internal/guestsupervisor"
)

// runGuestWriteSlots writes the pool's configured slot count to the guest's
// well-known slot-count file. The host runs it in the guest via a one-shot
// `tart exec <vm> gha-mac-broker guest-write-slots <n>` at VM warm, so the guest
// supervisor reads the count at startup and serves it for the VM's whole life.
func runGuestWriteSlots(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("guest-write-slots requires exactly one slot count argument")
	}
	parsed, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		slog.ErrorContext(ctx, "guest-write-slots parse slot count failed", "err", err, "arg", args[0])
		return fmt.Errorf("guest-write-slots parse slot count %q: %w", args[0], err)
	}
	if parsed == 0 {
		return fmt.Errorf("guest-write-slots requires a positive slot count")
	}
	if err := guestsupervisor.WriteSlotCount(guestsupervisor.SlotCountPath, uint32(parsed)); err != nil {
		return fmt.Errorf("guest-write-slots: %w", err)
	}
	slog.InfoContext(ctx, "guest-write-slots wrote slot count", "path", guestsupervisor.SlotCountPath, "slot_count", parsed)
	return nil
}
