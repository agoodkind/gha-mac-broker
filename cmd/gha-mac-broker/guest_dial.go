package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"time"
)

// guestDialConnectTimeout bounds the loopback dial to the guest-agent listener.
// The listener is a local socket in the same VM, so a healthy connect is
// immediate; this only bounds the window while the guest agent is still binding
// after boot.
const guestDialConnectTimeout = 10 * time.Second

// runGuestDial bridges the tart-exec stdio channel to the guest-agent's loopback
// listener. The broker runs `tart exec -i <vm> gha-mac-broker guest-dial`, so
// this process reads host-to-guest bytes on stdin and writes the guest-to-host
// reply on stdout. It dials 127.0.0.1:<guest-agent-port> inside the VM and
// copies both directions, which lets the host reach the guest agent over the
// tart guest-agent channel with no dependence on the guest's NAT IP or the host
// bridge route.
func runGuestDial(ctx context.Context, _ []string) error {
	address := net.JoinHostPort("127.0.0.1", guestAgentDefaultPort)
	slog.DebugContext(ctx, "guest-dial relay starting", "address", address)
	dialer := net.Dialer{Timeout: guestDialConnectTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		slog.ErrorContext(ctx, "guest-dial connect failed", "err", err, "address", address)
		return fmt.Errorf("guest-dial: connect %s: %w", address, err)
	}
	defer func() { _ = conn.Close() }()

	// Copy each direction independently. A buffered channel holds both results so
	// the goroutine whose copy is still blocked when the first one returns never
	// leaks its send.
	relayDone := make(chan error, 2)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicErr := fmt.Errorf("panic: %v", r)
				slog.ErrorContext(ctx, "guest-dial write relay panic", "err", panicErr)
				relayDone <- panicErr
			}
		}()
		_, copyErr := io.Copy(conn, os.Stdin)
		// Half-close the write side so the agent sees end-of-request and can flush
		// its reply, while the read side below still drains that reply.
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.CloseWrite()
		}
		relayDone <- copyErr
	}()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicErr := fmt.Errorf("panic: %v", r)
				slog.ErrorContext(ctx, "guest-dial read relay panic", "err", panicErr)
				relayDone <- panicErr
			}
		}()
		_, copyErr := io.Copy(os.Stdout, conn)
		relayDone <- copyErr
	}()

	// The transport connection is long-lived, so the relay lives until either the
	// host drops stdin or the agent closes its side. Whichever finishes first ends
	// the relay; the deferred conn.Close unblocks the other copy.
	firstErr := <-relayDone
	if firstErr != nil &&
		!errors.Is(firstErr, io.EOF) &&
		!errors.Is(firstErr, net.ErrClosed) &&
		!errors.Is(firstErr, os.ErrClosed) {
		slog.ErrorContext(ctx, "guest-dial relay failed", "err", firstErr, "address", address)
		return fmt.Errorf("guest-dial: relay: %w", firstErr)
	}
	return nil
}
