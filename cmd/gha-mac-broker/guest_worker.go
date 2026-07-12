package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"

	"goodkind.io/gha-mac-broker/internal/guestworker"
)

// runGuestWorker runs one swappable guest-worker generation. It reads its
// listener, registry snapshot, and runner pipes from inherited file descriptors,
// so it is only meaningful when the supervisor spawned it. SIGINT and SIGTERM
// cancel serving; SIGHUP (handled inside guestworker.Run) triggers a graceful
// self-replacement.
func runGuestWorker(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("guest-worker", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "guest-worker flag parse failed", "err", err)
		return fmt.Errorf("guest-worker flags: %w", err)
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := guestworker.Run(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("guest-worker: %w", err)
	}
	return nil
}
