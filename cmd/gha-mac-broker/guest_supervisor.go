package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"goodkind.io/gha-mac-broker/internal/golden"
	"goodkind.io/gha-mac-broker/internal/guestsupervisor"
)

const guestSupervisorSocketName = "gha-mac-broker-guest.sock"

// slotCountAwaitTimeout bounds how long the supervisor waits at startup for the
// host to seed the slot-count file at VM warm. The host seeds it shortly after
// the guest exec channel answers, so this only has to cover the guest boot-to-
// seed gap; on timeout the supervisor falls back to the -slots flag default so a
// dev run or a never-seeded VM still serves.
const slotCountAwaitTimeout = 180 * time.Second

// runGuestSupervisor runs the durable guest-supervisor: it binds the Connect TCP
// listener, then forks and supervises the swappable guest-worker, replacing the
// worker on request while runner jobs keep running.
func runGuestSupervisor(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("guest-supervisor", flag.ExitOnError)
	listenAddr := fs.String("listen", "", "listen address (default: first private non-loopback address on port 53931)")
	slotCount := fs.Uint("slots", 1, "number of guest execution slots")
	controlSocket := fs.String("control-socket", defaultGuestControlSocket(), "supervisor control socket path")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "guest-supervisor flag parse failed", "err", err)
		return fmt.Errorf("guest-supervisor flags: %w", err)
	}
	// Resolve the boot-scoped token: an env token wins, otherwise reuse an existing
	// token file this boot or mint a fresh one. Reusing across process restarts
	// keeps the credential stable so a KeepAlive respawn does not invalidate the
	// token the broker cached. The golden's baked launchd unit (KeepAlive, no env
	// token) boots cleanly instead of crashlooping. The host reads the file over
	// the tart-exec control channel and dials the guest agent with it.
	token, err := guestsupervisor.EnsureBootToken(os.Getenv(guestAgentCredentialEnv), guestsupervisor.TokenPath)
	if err != nil {
		return fmt.Errorf("guest-supervisor resolve token: %w", err)
	}
	if *slotCount == 0 {
		return fmt.Errorf("guest-supervisor requires at least one slot")
	}
	if *slotCount > uint(^uint32(0)) {
		return fmt.Errorf("guest-supervisor slot count %d exceeds uint32", *slotCount)
	}
	// The host seeds the pool's jobs_per_vm into the slot-count file at VM warm,
	// so wait for it and serve that count for this guest's whole life. The -slots
	// flag is only the fallback default for a dev run or a never-seeded VM.
	slotCountValue := guestsupervisor.AwaitSlotCount(ctx, guestsupervisor.SlotCountPath, slotCountAwaitTimeout, uint32(*slotCount))
	resolvedListenAddr := *listenAddr
	if resolvedListenAddr == "" {
		resolvedListenAddr = defaultGuestAgentListenAddr()
	}

	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(ctx, "tcp", resolvedListenAddr)
	if err != nil {
		return fmt.Errorf("guest-supervisor listen %q: %w", resolvedListenAddr, err)
	}
	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		_ = listener.Close()
		return fmt.Errorf("guest-supervisor listener is %T, want *net.TCPListener", listener)
	}

	supervisor := guestsupervisor.New(guestsupervisor.Options{
		Listener:          tcpListener,
		ControlSocketPath: *controlSocket,
		Token:             token,
		GoldenFingerprint: readGoldenFingerprint(ctx),
		SlotCount:         slotCountValue,
		WorkerCommand:     nil,
		Log:               slog.Default(),
	})

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.InfoContext(ctx, "guest supervisor starting", "addr", tcpListener.Addr().String(), "control_socket", *controlSocket, "slot_count", slotCountValue)
	if err := supervisor.Run(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("guest-supervisor: %w", err)
	}
	return nil
}

func defaultGuestControlSocket() string {
	return filepath.Join(os.TempDir(), guestSupervisorSocketName)
}

// readGoldenFingerprint reads the baked golden fingerprint the provisioner wrote.
// It is best effort: a golden without the file (or a dev run) simply reports an
// empty fingerprint via Hello rather than failing supervisor startup.
func readGoldenFingerprint(ctx context.Context) string {
	content, err := os.ReadFile(golden.FingerprintPath)
	if err != nil {
		slog.DebugContext(ctx, "golden fingerprint file absent; reporting empty", "path", golden.FingerprintPath, "err", err)
		return ""
	}
	return strings.TrimSpace(string(content))
}
