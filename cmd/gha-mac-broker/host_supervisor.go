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
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/hostsupervisor"
)

// hostReconcileStallTimeout is how long the reconcile loop may go without stamping
// progress before the supervisor restarts the worker. It exceeds a healthy
// reconcile interval by a wide margin, so only a genuinely wedged loop trips it.
// This is the in-broker half of the wedge fix: PR1 bounded the tart calls, and
// this catches a reconcile stall from any other cause.
const hostReconcileStallTimeout = 6 * time.Minute

// supervisorPIDFileMode keeps the pidfile owner-only; it is not sensitive but a
// tight mode avoids a broad-permission warning and is all the deploy path needs.
const supervisorPIDFileMode = 0o600

// supervisorPIDDirMode is the mode for the pidfile's parent directory.
const supervisorPIDDirMode = 0o755

// osDarwinName is the [runtime.GOOS] value for macOS, used to place the pidfile in
// the platform's per-user application state directory.
const osDarwinName = "darwin"

// runSupervisor runs the durable host supervisor: it binds the webhook, capacity,
// and status listener, then spawns and supervises the swappable worker, replacing
// it on SIGHUP and restarting it when the reconcile loop stalls, all while the
// listener stays up so a worker swap never drops a webhook or a /status stream.
func runSupervisor(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("supervisor", flag.ExitOnError)
	configPath := fs.String("config", "", "path to broker config TOML (default: XDG path)")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "supervisor flag parse failed", "err", err)
		return fmt.Errorf("supervisor flags: %w", err)
	}
	if *configPath == "" {
		*configPath = config.DefaultConfigPath()
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("supervisor: load config: %w", err)
	}

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		slog.ErrorContext(ctx, "supervisor listen failed", "err", err, "addr", cfg.ListenAddr)
		return fmt.Errorf("supervisor: listen %q: %w", cfg.ListenAddr, err)
	}
	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		_ = listener.Close()
		err := fmt.Errorf("supervisor: listener is %T, want *net.TCPListener", listener)
		slog.ErrorContext(ctx, "supervisor listener is not TCP", "err", err)
		return err
	}

	supervisor := hostsupervisor.New(hostsupervisor.Options{
		Listener:                tcpListener,
		WorkerCommand:           nil,
		StallTimeout:            hostReconcileStallTimeout,
		StallCheckInterval:      0,
		FirstReadyTimeout:       0,
		ReplacementReadyTimeout: 0,
		WorkerStopTimeout:       0,
		Now:                     nil,
		Log:                     nil,
	})

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Arm SIGHUP handling before publishing the pidfile. The pidfile is what makes
	// the supervisor discoverable to reloadOrRestart and deploy, so a reload request
	// can arrive the instant it is written; if the handler were installed after the
	// write, a SIGHUP landing in that gap would hit the default disposition and
	// terminate the supervisor instead of triggering an in-place worker reload.
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	defer signal.Stop(sighup)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(ctx, "supervisor reload signal goroutine panic recovered", "err", fmt.Errorf("panic: %v", recovered))
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-sighup:
				supervisor.RequestReload()
			}
		}
	}()

	if err := writeSupervisorPIDFile(os.Getpid()); err != nil {
		slog.WarnContext(ctx, "supervisor pidfile write failed; in-place worker reload signaling disabled", "err", err)
	}
	defer removeSupervisorPIDFile()

	slog.InfoContext(ctx, "host supervisor starting", "addr", tcpListener.Addr().String())
	if err := supervisor.Run(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("supervisor: %w", err)
	}
	return nil
}

// supervisorPIDPath is the stable, well-known file the supervisor writes its pid
// to, so a separate deploy or update process can find and signal it. It derives
// from HOME rather than TMPDIR because a launchd user agent and a shell both
// resolve the same HOME, while their TMPDIR differs, so a TMPDIR path would let a
// shell-run deploy miss the live supervisor and silently downgrade a worker reload
// to a full service restart.
func supervisorPIDPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		// HOME should always resolve for a user agent; fall back to a fixed name only
		// so the pidfile still has a path when it does not.
		return filepath.Join(os.TempDir(), "gha-mac-broker-supervisor.pid")
	}
	if runtime.GOOS == osDarwinName {
		return filepath.Join(home, "Library", "Application Support", "gha-mac-broker", "supervisor.pid")
	}
	return filepath.Join(home, ".local", "state", "gha-mac-broker", "supervisor.pid")
}

func writeSupervisorPIDFile(pid int) error {
	path := supervisorPIDPath()
	if err := os.MkdirAll(filepath.Dir(path), supervisorPIDDirMode); err != nil {
		slog.Error("supervisor create pidfile dir failed", "err", err, "path", path)
		return fmt.Errorf("supervisor: create pidfile dir: %w", err)
	}
	content := []byte(strconv.Itoa(pid) + "\n")
	if err := os.WriteFile(path, content, supervisorPIDFileMode); err != nil {
		slog.Error("supervisor write pidfile failed", "err", err, "path", path)
		return fmt.Errorf("supervisor: write pidfile: %w", err)
	}
	slog.Debug("supervisor pidfile written", "path", path, "pid", pid)
	return nil
}

func removeSupervisorPIDFile() {
	path := supervisorPIDPath()
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Warn("supervisor remove pidfile failed", "err", err, "path", path)
		return
	}
	slog.Debug("supervisor pidfile removed", "path", path)
}

// liveSupervisorPID returns the running host supervisor's pid when its pidfile
// names a live process, so the deploy path can prefer an in-place worker reload
// over a full service restart.
func liveSupervisorPID() (int, bool) {
	content, err := os.ReadFile(supervisorPIDPath())
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(content)))
	if err != nil || pid <= 0 {
		return 0, false
	}
	if !hostProcessAlive(pid) {
		return 0, false
	}
	return pid, true
}

// hostProcessAlive reports whether pid names a live process. EPERM means the
// process exists but is owned by another user; ESRCH means it is gone.
func hostProcessAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
