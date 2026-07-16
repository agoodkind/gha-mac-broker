package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"goodkind.io/gha-mac-broker/internal/golden"
	"goodkind.io/gha-mac-broker/internal/guestclient"
	"goodkind.io/gha-mac-broker/internal/guestsupervisor"
	"goodkind.io/gha-mac-broker/internal/guesttransport"
)

// guestDialSubcommand is the broker binary's subcommand, run inside the guest
// via `tart exec`, that relays the exec stdio channel to the guest-agent
// loopback listener.
const guestDialSubcommand = "guest-dial"

// guestClientAdapter adapts the concrete guestclient.Client to the guestConn
// interface the binder depends on. It embeds the client so its already-wrapped
// Hello, RunJob, Reattach, Drain, and CancelJob methods satisfy guestConn
// directly, and overrides JobStatus to narrow the concrete stream to the
// jobStatusStream interface.
type guestClientAdapter struct {
	*guestclient.Client
}

func newGuestClientAdapter(ctx context.Context, dial guesttransport.GuestDialer, token string) guestConn {
	return guestClientAdapter{Client: guestclient.New(ctx, dial, token)}
}

func (a guestClientAdapter) JobStatus(ctx context.Context, executionID string, fromSequence uint64) (jobStatusStream, error) {
	stream, err := a.Client.JobStatus(ctx, executionID, fromSequence)
	if err != nil {
		slog.WarnContext(ctx, "guest job status open failed", "err", err, "execution", executionID)
		return nil, fmt.Errorf("broker: guest job status: %w", err)
	}
	return stream, nil
}

// resolveGuest resolves and caches a VM's guest-agent client. It reads the
// per-boot token over the tart guest-agent channel, dials the agent over that
// same channel (no guest NAT IP), and confirms Hello answers with a compatible
// protocol before caching.
func (b *Binder) resolveGuest(ctx context.Context, vm *WarmVM) (guestConn, error) {
	// Hold the per-VM lock across the whole resolution, so concurrent callers for
	// the same VM dial exactly once: the first resolves and caches, the rest see
	// the cache. Resolution runs single threaded in Warm and Adopt before the VM
	// is served, so the hot path always hits the cache and never blocks here.
	vm.guestMu.Lock()
	defer vm.guestMu.Unlock()
	if vm.guestConn != nil {
		return vm.guestConn, nil
	}

	token, err := b.readGuestToken(ctx, vm.Name)
	if err != nil {
		return nil, err
	}
	// Reach the guest agent over the tart guest-agent channel by running the
	// guest-dial relay, which bridges the exec stdio to the agent's loopback
	// listener. This avoids dialing the guest NAT IP, whose bridge route stays
	// unreachable from the host until well after the guest boots.
	dial := func(dialCtx context.Context) (net.Conn, error) {
		return b.vm.ExecConn(dialCtx, vm.Name, golden.BakedBinaryPath, guestDialSubcommand)
	}
	client := b.dialGuest(ctx, dial, token)
	if err := b.waitForGuestHello(ctx, client, vm.Name); err != nil {
		return nil, err
	}

	vm.guestConn = client
	return vm.guestConn, nil
}

// readGuestToken reads the per-boot guest token file over the tart-exec channel,
// retrying within the readiness window because the supervisor writes it shortly
// after the vsock channel comes up. The guest-supervisor runs as root and writes
// the token file mode 0600, so tart-exec (which runs as the unprivileged admin
// user) must read it through sudo; a plain cat gets permission denied and the
// read would otherwise loop until the readiness deadline.
func (b *Binder) readGuestToken(ctx context.Context, vmName string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()
	ticker := time.NewTicker(readinessInterval)
	defer ticker.Stop()
	for {
		out, err := b.vm.Exec(ctx, vmName, "sudo", "cat", guestsupervisor.TokenPath)
		if err == nil {
			token := strings.TrimSpace(string(out))
			if token != "" {
				return token, nil
			}
		}
		select {
		case <-ctx.Done():
			slog.ErrorContext(ctx, "timed out reading guest token", "err", ctx.Err(), "vm", vmName)
			return "", fmt.Errorf("broker: read guest token for %s: %w", vmName, ctx.Err())
		case <-ticker.C:
		}
	}
}

// waitForGuestHello polls the guest agent until Hello answers or the readiness
// window elapses. A protocol mismatch is terminal and returns immediately.
func (b *Binder) waitForGuestHello(ctx context.Context, client guestConn, vmName string) error {
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()
	ticker := time.NewTicker(readinessInterval)
	defer ticker.Stop()
	for {
		response, err := client.Hello(ctx)
		if err == nil {
			if response.GetProtocolMajor() != hostProtocolMajor {
				return fmt.Errorf("broker: guest %s speaks protocol %d, want %d", vmName, response.GetProtocolMajor(), hostProtocolMajor)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			slog.ErrorContext(ctx, "timed out waiting for guest hello", "err", ctx.Err(), "vm", vmName)
			return fmt.Errorf("broker: waiting for guest hello on %s: %w", vmName, ctx.Err())
		case <-ticker.C:
		}
	}
}
