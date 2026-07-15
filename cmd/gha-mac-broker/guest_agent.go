package main

import (
	"context"
	"net"
)

const (
	guestAgentCredentialEnv = "GHA_GUEST_TOKEN" // #nosec G101 -- environment variable name only.
	guestAgentDefaultPort   = "53931"
	// guestAgentLoopbackHost is the only address the guest agent binds. The broker
	// reaches it over `tart exec` running the guest-dial relay, so the agent never
	// needs a routable interface. Binding loopback also removes the first-boot race
	// where the guest has no private non-loopback address yet and the supervisor
	// would otherwise exit and rely on a KeepAlive restart.
	guestAgentLoopbackHost = "127.0.0.1"
)

// runGuestAgent is a thin alias that runs the guest-supervisor, so anything that
// still launches `guest-agent` keeps working after the supervisor and worker
// split. The supervisor owns the listener and forks the swappable worker.
func runGuestAgent(ctx context.Context, args []string) error {
	return runGuestSupervisor(ctx, args)
}

// defaultGuestAgentListenAddr is the guest agent's listen address when no
// -listen override is given. It binds loopback because the host reaches the
// agent over the tart guest-agent channel, not over the guest's NAT IP.
func defaultGuestAgentListenAddr() string {
	return net.JoinHostPort(guestAgentLoopbackHost, guestAgentDefaultPort)
}
