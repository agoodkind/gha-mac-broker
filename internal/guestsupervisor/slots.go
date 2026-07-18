// Package guestsupervisor holds the boot-scoped bearer token and the VM-warm
// slot-count helpers the host seeds and the single guest-agent daemon reads at
// startup. It carries no build constraint so the host broker can import the
// shared token and slot-count path constants on any platform.
package guestsupervisor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// SlotCountPath is the well-known file the host seeds at VM warm with the pool's
// configured jobs_per_vm. The guest supervisor reads it once at startup and
// spawns its single worker generation at that count, so the guest serves the
// count for its whole life. A jobs_per_vm change recycles the VM to a fresh one
// the host seeds with the new count, rather than reconfiguring a live guest.
const SlotCountPath = "/tmp/gha-guest-slots"

// slotCountFileMode keeps the seeded count world-readable so the supervisor can
// read it regardless of which user the LaunchDaemon runs the process as. The
// count is not a secret.
const slotCountFileMode = 0o644

// slotCountAwaitInterval paces the poll while the supervisor waits for the host
// to seed the slot-count file at warm.
const slotCountAwaitInterval = 200 * time.Millisecond

// WriteSlotCount writes slotCount to path atomically: it writes the decimal
// bytes to a private temp file in the same directory, then renames it over path,
// so a concurrent reader never sees a partially written count. The host runs
// this in the guest via a one-shot `tart exec ... guest-write-slots` at warm.
func WriteSlotCount(path string, slotCount uint32) error {
	if slotCount == 0 {
		return fmt.Errorf("guestsupervisor: slot count must be positive")
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".gha-guest-slots-*")
	if err != nil {
		slog.Error("guest supervisor create slot-count temp file failed", "err", err, "path", path)
		return fmt.Errorf("guestsupervisor: create slot-count temp file for %s: %w", path, err)
	}
	tempName := temp.Name()
	if _, err := temp.WriteString(strconv.FormatUint(uint64(slotCount), 10)); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempName)
		slog.Error("guest supervisor write slot-count temp file failed", "err", err, "path", tempName)
		return fmt.Errorf("guestsupervisor: write slot-count temp file %s: %w", tempName, err)
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempName)
		slog.Error("guest supervisor close slot-count temp file failed", "err", err, "path", tempName)
		return fmt.Errorf("guestsupervisor: close slot-count temp file %s: %w", tempName, err)
	}
	if err := os.Chmod(tempName, slotCountFileMode); err != nil {
		_ = os.Remove(tempName)
		slog.Error("guest supervisor chmod slot-count temp file failed", "err", err, "path", tempName)
		return fmt.Errorf("guestsupervisor: chmod slot-count temp file %s: %w", tempName, err)
	}
	if err := os.Rename(tempName, path); err != nil {
		_ = os.Remove(tempName)
		slog.Error("guest supervisor rename slot-count file failed", "err", err, "path", path)
		return fmt.Errorf("guestsupervisor: rename slot-count file to %s: %w", path, err)
	}
	slog.Info("guest supervisor slot-count file written", "path", path, "slot_count", slotCount)
	return nil
}

// readSeededSlotCount reads and validates the seeded slot count at path. It
// reports ok=false when the file is absent, unreadable, or does not hold a
// positive count that fits a uint32, so the await loop simply keeps polling
// rather than treating a not-yet-seeded file as a hard error.
func readSeededSlotCount(path string) (uint32, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	trimmed := strings.TrimSpace(string(data))
	parsed, err := strconv.ParseUint(trimmed, 10, 32)
	if err != nil || parsed == 0 {
		return 0, false
	}
	return uint32(parsed), true
}

// AwaitSlotCount polls path until it holds a valid seeded slot count, then
// returns it. The host seeds the file at warm shortly after the guest's exec
// channel comes up, so the supervisor waits here before it spawns its first
// worker generation, giving the guest its final slot count with no live
// reconfigure. On timeout it returns fallback (the boot-flag default) and logs a
// warning, so a dev run or a never-seeded VM still serves rather than hanging.
func AwaitSlotCount(ctx context.Context, path string, timeout time.Duration, fallback uint32) uint32 {
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(slotCountAwaitInterval)
	defer ticker.Stop()
	for {
		if slotCount, ok := readSeededSlotCount(path); ok {
			return slotCount
		}
		select {
		case <-deadlineCtx.Done():
			slog.WarnContext(deadlineCtx, "guest supervisor slot-count file not seeded before deadline; using fallback", "path", path, "fallback", fallback)
			return fallback
		case <-ticker.C:
		}
	}
}
