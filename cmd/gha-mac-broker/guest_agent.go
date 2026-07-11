package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
)

const (
	guestAgentCredentialEnv = "GHA_GUEST_TOKEN" // #nosec G101 -- environment variable name only.
	guestAgentDefaultPort   = "53931"
)

// runGuestAgent is a thin alias that runs the guest-supervisor, so anything that
// still launches `guest-agent` keeps working after the supervisor and worker
// split. The supervisor owns the listener and forks the swappable worker.
func runGuestAgent(ctx context.Context, args []string) error {
	return runGuestSupervisor(ctx, args)
}

func defaultGuestAgentListenAddr() (string, error) {
	ip, err := defaultGuestAgentListenIP()
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(ip.String(), guestAgentDefaultPort), nil
}

func defaultGuestAgentListenIP() (net.IP, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		slog.Error("guest-agent interface list failed", "err", err)
		return nil, fmt.Errorf("guest-agent list interfaces: %w", err)
	}
	for _, networkInterface := range interfaces {
		if networkInterface.Flags&net.FlagUp == 0 {
			continue
		}
		if networkInterface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, addressErr := networkInterface.Addrs()
		if addressErr != nil {
			slog.Debug("guest-agent interface address list failed",
				"interface", networkInterface.Name,
				"err", addressErr,
			)
			continue
		}
		for _, address := range addresses {
			ip := addrIP(address)
			if isGuestAgentListenCandidate(ip) {
				return ip, nil
			}
		}
	}
	err = fmt.Errorf("guest-agent: no private non-loopback interface address found; pass -listen")
	slog.Error("guest-agent listen address discovery failed", "err", err)
	return nil, err
}

func addrIP(address net.Addr) net.IP {
	switch typedAddress := address.(type) {
	case *net.IPAddr:
		return typedAddress.IP
	case *net.IPNet:
		return typedAddress.IP
	default:
		return nil
	}
}

func isGuestAgentListenCandidate(ip net.IP) bool {
	ipv4 := ip.To4()
	if ipv4 == nil {
		return false
	}
	if ipv4.IsUnspecified() || ipv4.IsLoopback() || ipv4.IsLinkLocalUnicast() {
		return false
	}
	return ipv4.IsPrivate()
}
