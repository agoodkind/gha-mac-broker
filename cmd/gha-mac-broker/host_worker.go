package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/hostsupervisor"
)

// runWorker runs one swappable host worker generation. When the host supervisor
// spawns it, the worker rebuilds the webhook listener from an inherited descriptor,
// signals readiness once serving, and stamps reconcile progress so the supervisor
// stall watchdog can see the reconcile loop advance. Absent the inherited
// descriptor it binds its own listener, so the worker role degrades to a standalone
// daemon and never depends on a supervisor being present.
func runWorker(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	configPath := fs.String("config", "", "path to broker config TOML (default: XDG path)")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "worker flag parse failed", "err", err)
		return fmt.Errorf("worker flags: %w", err)
	}
	if *configPath == "" {
		*configPath = config.DefaultConfigPath()
	}
	return serveDaemon(ctx, *configPath, serveComponents{
		acquireListener: workerListener,
		onReady:         workerReadySignal(),
		stampProgress:   workerProgressStamp(),
	})
}

// workerListener rebuilds the listener from the supervisor-owned descriptor when
// present, otherwise binds addr directly. Rebuilding from the inherited descriptor
// is what keeps the listener up across a worker swap: the supervisor owns the
// socket and each worker holds a dup of it.
func workerListener(ctx context.Context, addr string) (net.Listener, error) {
	raw := os.Getenv(hostsupervisor.EnvListenerFD)
	if raw == "" {
		return listenTCP(ctx, addr)
	}
	fd, err := strconv.Atoi(raw)
	if err != nil {
		slog.ErrorContext(ctx, "worker listener fd parse failed", "err", err, "raw", raw)
		return nil, fmt.Errorf("worker: parse %s: %w", hostsupervisor.EnvListenerFD, err)
	}
	file := os.NewFile(uintptr(fd), "host-listener")
	listener, err := net.FileListener(file)
	// FileListener dups the descriptor, so the inherited file is closed here.
	_ = file.Close()
	if err != nil {
		slog.ErrorContext(ctx, "worker rebuild listener failed", "err", err, "fd", fd)
		return nil, fmt.Errorf("worker: rebuild listener from fd %d: %w", fd, err)
	}
	return listener, nil
}

// workerReadySignal returns a callback that writes the ready message to the
// supervisor readiness pipe once the worker is serving, or nil when no readiness
// descriptor was inherited (the standalone worker path).
func workerReadySignal() func() {
	fd, ok := inheritedFD(hostsupervisor.EnvReadyFD)
	if !ok {
		return nil
	}
	return func() {
		file := os.NewFile(uintptr(fd), "host-ready")
		if file == nil {
			return
		}
		if _, err := file.WriteString(hostsupervisor.ReadyMessage); err != nil {
			slog.Warn("worker readiness signal failed", "err", err)
		}
		_ = file.Close()
	}
}

// workerProgressStamp returns a callback that writes one heartbeat line to the
// supervisor progress pipe after each reconcile pass, or nil when no progress
// descriptor was inherited. The write end stays open for the worker's life so the
// supervisor sees a live stream until the worker exits.
func workerProgressStamp() func() {
	fd, ok := inheritedFD(hostsupervisor.EnvProgressFD)
	if !ok {
		return nil
	}
	file := os.NewFile(uintptr(fd), "host-progress")
	if file == nil {
		return nil
	}
	var mu sync.Mutex
	return func() {
		mu.Lock()
		defer mu.Unlock()
		if _, err := file.WriteString("progress\n"); err != nil {
			slog.Warn("worker reconcile progress heartbeat failed", "err", err)
		}
	}
}

// inheritedFD parses one inherited descriptor number from the environment,
// reporting absence so a standalone worker skips the supervisor-only wiring.
func inheritedFD(name string) (int, bool) {
	raw := os.Getenv(name)
	if raw == "" {
		return 0, false
	}
	fd, err := strconv.Atoi(raw)
	if err != nil {
		slog.Warn("worker inherited fd parse failed; skipping", "env", name, "raw", raw, "err", err)
		return 0, false
	}
	return fd, true
}
