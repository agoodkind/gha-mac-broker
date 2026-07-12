package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"goodkind.io/gha-mac-broker/internal/guestclient"
	"goodkind.io/gha-mac-broker/internal/guestsupervisor"
)

// guestClientAdapter adapts the concrete guestclient.Client to the guestConn
// interface the binder depends on. It embeds the client so its already-wrapped
// Hello, RunJob, Reattach, Drain, and CancelJob methods satisfy guestConn
// directly, and overrides JobStatus to narrow the concrete stream to the
// jobStatusStream interface.
type guestClientAdapter struct {
	*guestclient.Client
}

func newGuestClientAdapter(ctx context.Context, address string, token string) guestConn {
	return guestClientAdapter{Client: guestclient.New(ctx, address, token)}
}

func (a guestClientAdapter) JobStatus(ctx context.Context, executionID string, fromSequence uint64) (jobStatusStream, error) {
	stream, err := a.Client.JobStatus(ctx, executionID, fromSequence)
	if err != nil {
		slog.WarnContext(ctx, "guest job status open failed", "err", err, "execution", executionID)
		return nil, fmt.Errorf("broker: guest job status: %w", err)
	}
	return stream, nil
}

// resolveGuest resolves and caches a VM's guest-agent client. It resolves the
// NAT IP, reads the per-boot token over the tart-exec control channel, dials the
// agent, and confirms Hello answers with a compatible protocol before caching.
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

	ip, err := b.vm.IP(ctx, vm.Name)
	if err != nil {
		slog.WarnContext(ctx, "resolve guest ip failed", "err", err, "vm", vm.Name)
		return nil, fmt.Errorf("broker: resolve guest ip for %s: %w", vm.Name, err)
	}
	address := net.JoinHostPort(ip, guestAgentPort)
	token, err := b.readGuestToken(ctx, vm.Name)
	if err != nil {
		return nil, err
	}
	client := b.dialGuest(ctx, address, token)
	if err := b.waitForGuestHello(ctx, client, vm.Name); err != nil {
		return nil, err
	}

	vm.guestAddr = address
	vm.guestConn = client
	return vm.guestConn, nil
}

// readGuestToken reads the per-boot guest token file over the tart-exec channel,
// retrying within the readiness window because the supervisor writes it shortly
// after the vsock channel comes up.
func (b *Binder) readGuestToken(ctx context.Context, vmName string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()
	ticker := time.NewTicker(readinessInterval)
	defer ticker.Stop()
	for {
		out, err := b.vm.Exec(ctx, vmName, "cat", guestsupervisor.TokenPath)
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
