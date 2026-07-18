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
	"strings"
	"syscall"
	"time"

	"goodkind.io/gha-mac-broker/internal/golden"
	"goodkind.io/gha-mac-broker/internal/guestagent"
	"goodkind.io/gha-mac-broker/internal/guestsupervisor"
	"goodkind.io/gha-mac-broker/internal/guesttransport"
)

const (
	guestAgentCredentialEnv = "GHA_GUEST_TOKEN" // #nosec G101 -- environment variable name only.
	guestAgentDefaultPort   = "53931"
	// guestAgentLoopbackHost is the only address the guest agent binds. The broker
	// reaches it over `tart exec` running the guest-dial relay, so the agent never
	// needs a routable interface. Binding loopback also removes the first-boot race
	// where the guest has no private non-loopback address yet and the agent would
	// otherwise exit and rely on a KeepAlive restart.
	guestAgentLoopbackHost = "127.0.0.1"
)

// slotCountAwaitTimeout bounds how long the guest agent waits at startup for the
// host to seed the slot-count file at VM warm. The host seeds it shortly after
// the guest exec channel answers, so this only has to cover the guest boot-to-
// seed gap; on timeout the agent falls back to the -slots flag default so a dev
// run or a never-seeded VM still serves.
const slotCountAwaitTimeout = 180 * time.Second

// runGuestAgent runs the single sticky guest-agent daemon. It resolves the
// boot-scoped token, reads its slot count seeded at VM warm, binds the loopback
// Connect listener, and serves the ConnectRPC handlers directly. RunJob forks
// each runner in-process through the registry (no worker/supervisor split, no
// snapshot handoff, no live reload). launchd (KeepAlive) restarts the daemon
// wholesale on death, and the affected CI job retries; no in-flight state is
// preserved across a restart.
func runGuestAgent(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("guest-agent", flag.ExitOnError)
	listenAddr := fs.String("listen", "", "listen address (default: loopback on port 53931)")
	slotCount := fs.Uint("slots", 1, "number of guest execution slots")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "guest-agent flag parse failed", "err", err)
		return fmt.Errorf("guest-agent flags: %w", err)
	}
	// Resolve the boot-scoped token: an env token wins, otherwise reuse an existing
	// token file this boot or mint a fresh one. Reusing across process restarts
	// keeps the credential stable so a KeepAlive respawn does not invalidate the
	// token the broker cached. The golden's baked launchd unit (KeepAlive, no env
	// token) boots cleanly instead of crashlooping. The host reads the file over
	// the tart-exec control channel and dials the guest agent with it.
	token, err := guestsupervisor.EnsureBootToken(os.Getenv(guestAgentCredentialEnv), guestsupervisor.TokenPath)
	if err != nil {
		return fmt.Errorf("guest-agent resolve token: %w", err)
	}
	if *slotCount == 0 {
		return fmt.Errorf("guest-agent requires at least one slot")
	}
	if *slotCount > uint(^uint32(0)) {
		return fmt.Errorf("guest-agent slot count %d exceeds uint32", *slotCount)
	}
	// The host seeds the pool's jobs_per_vm into the slot-count file at VM warm, so
	// wait for it and serve that count for this guest's whole life. The -slots flag
	// is only the fallback default for a dev run or a never-seeded VM.
	slotCountValue := guestsupervisor.AwaitSlotCount(ctx, guestsupervisor.SlotCountPath, slotCountAwaitTimeout, uint32(*slotCount))

	resolvedListenAddr := *listenAddr
	if resolvedListenAddr == "" {
		resolvedListenAddr = defaultGuestAgentListenAddr()
	}
	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(ctx, "tcp", resolvedListenAddr)
	if err != nil {
		return fmt.Errorf("guest-agent listen %q: %w", resolvedListenAddr, err)
	}

	handler := guestagent.NewHTTPHandler(nil, guestagent.Options{
		SlotCount:         slotCountValue,
		BootID:            "",
		AgentBuild:        "",
		GoldenFingerprint: readGoldenFingerprint(ctx),
		ChildLauncher:     nil,
		SpecBuilder:       nil,
	})

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.InfoContext(ctx, "guest agent starting", "addr", listener.Addr().String(), "slot_count", slotCountValue)
	if err := guesttransport.Serve(ctx, listener, handler, token); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("guest-agent: %w", err)
	}
	return nil
}

// defaultGuestAgentListenAddr is the guest agent's listen address when no
// -listen override is given. It binds loopback because the host reaches the
// agent over the tart guest-agent channel, not over the guest's NAT IP.
func defaultGuestAgentListenAddr() string {
	return net.JoinHostPort(guestAgentLoopbackHost, guestAgentDefaultPort)
}

// readGoldenFingerprint reads the baked golden fingerprint the provisioner wrote.
// It is best effort: a golden without the file (or a dev run) simply reports an
// empty fingerprint via Hello rather than failing agent startup.
func readGoldenFingerprint(ctx context.Context) string {
	content, err := os.ReadFile(golden.FingerprintPath)
	if err != nil {
		slog.DebugContext(ctx, "golden fingerprint file absent; reporting empty", "path", golden.FingerprintPath, "err", err)
		return ""
	}
	return strings.TrimSpace(string(content))
}
