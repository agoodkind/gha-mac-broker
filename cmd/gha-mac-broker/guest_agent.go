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
	"syscall"

	"goodkind.io/gha-mac-broker/internal/guestagent"
	"goodkind.io/gha-mac-broker/internal/guestexec"
	"goodkind.io/gha-mac-broker/internal/guesttransport"
)

const (
	guestAgentCredentialEnv = "GHA_GUEST_TOKEN" // #nosec G101 -- environment variable name only.
	guestAgentDefaultPort   = "53931"
)

func runGuestAgent(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("guest-agent", flag.ExitOnError)
	listenAddr := fs.String("listen", "", "agent listen address (default: first private non-loopback address on port 53931)")
	slotCount := fs.Uint("slots", 1, "number of guest execution slots")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "guest-agent flag parse failed", "err", err)
		return fmt.Errorf("guest-agent flags: %w", err)
	}
	token := os.Getenv(guestAgentCredentialEnv)
	if token == "" {
		return fmt.Errorf("guest-agent requires %s", guestAgentCredentialEnv)
	}
	if *slotCount == 0 {
		return fmt.Errorf("guest-agent requires at least one slot")
	}
	if *slotCount > uint(^uint32(0)) {
		return fmt.Errorf("guest-agent slot count %d exceeds uint32", *slotCount)
	}
	resolvedListenAddr := *listenAddr
	if resolvedListenAddr == "" {
		var err error
		resolvedListenAddr, err = defaultGuestAgentListenAddr()
		if err != nil {
			return err
		}
	}
	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(ctx, "tcp", resolvedListenAddr)
	if err != nil {
		return fmt.Errorf("guest-agent listen %q: %w", resolvedListenAddr, err)
	}

	registry := guestexec.New(guestexec.Options{
		Retention:         0,
		HeartbeatInterval: 0,
	})
	handler := guestagent.NewHTTPHandler(registry, guestagent.Options{
		SlotCount:         uint32(*slotCount),
		BootID:            "",
		AgentBuild:        "",
		GoldenFingerprint: "",
	})
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.InfoContext(ctx, "guest agent listening", "addr", listener.Addr().String())
	err = guesttransport.Serve(ctx, listener, handler, token)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("guest-agent serve: %w", err)
	}
	return nil
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
