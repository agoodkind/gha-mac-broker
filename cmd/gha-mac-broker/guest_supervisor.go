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

	"goodkind.io/gha-mac-broker/internal/golden"
	"goodkind.io/gha-mac-broker/internal/guestsupervisor"
)

const guestSupervisorSocketName = "gha-mac-broker-guest.sock"

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
	token := os.Getenv(guestAgentCredentialEnv)
	if token == "" {
		return fmt.Errorf("guest-supervisor requires %s", guestAgentCredentialEnv)
	}
	if *slotCount == 0 {
		return fmt.Errorf("guest-supervisor requires at least one slot")
	}
	if *slotCount > uint(^uint32(0)) {
		return fmt.Errorf("guest-supervisor slot count %d exceeds uint32", *slotCount)
	}
	resolvedListenAddr := *listenAddr
	if resolvedListenAddr == "" {
		resolved, err := defaultGuestAgentListenAddr()
		if err != nil {
			return err
		}
		resolvedListenAddr = resolved
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
		SlotCount:         uint32(*slotCount),
		WorkerCommand:     nil,
		Log:               slog.Default(),
	})

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.InfoContext(ctx, "guest supervisor starting", "addr", tcpListener.Addr().String(), "control_socket", *controlSocket)
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
