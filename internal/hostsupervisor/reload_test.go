//go:build unix

package hostsupervisor_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"goodkind.io/gha-mac-broker/internal/hostsupervisor"
)

const (
	workerMainEnv  = "GHA_HOST_WORKER_TEST_MAIN"
	workerStallEnv = "GHA_HOST_WORKER_TEST_STALL"
	harnessTimeout = 30 * time.Second
)

// TestMain lets the test binary re-exec itself as a host worker. The supervisor
// spawns a worker by running this same binary with workerMainEnv set, which routes
// to a stub worker that rebuilds the listener from the inherited descriptor and
// serves its generation, so the reload mechanics run against real worker
// subprocesses with real fd handoff.
func TestMain(m *testing.M) {
	if os.Getenv(workerMainEnv) == "1" {
		runStubWorker()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// runStubWorker is the re-exec worker body: it rebuilds the listener from the
// inherited descriptor, serves its generation on every request, signals readiness,
// stamps reconcile progress unless told to stall, and drains gracefully on SIGTERM.
func runStubWorker() {
	listener := stubListener()
	generation := os.Getenv(hostsupervisor.EnvGeneration)
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, generation)
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = server.Serve(listener) }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if os.Getenv(workerStallEnv) != "1" {
		go stubProgress(ctx)
	}
	stubSignalReady()

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutCtx)
}

func stubListener() net.Listener {
	fd, err := strconv.Atoi(os.Getenv(hostsupervisor.EnvListenerFD))
	if err != nil {
		fmt.Fprintln(os.Stderr, "stub worker: parse listener fd:", err)
		os.Exit(2)
	}
	file := os.NewFile(uintptr(fd), "stub-listener")
	listener, err := net.FileListener(file)
	_ = file.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, "stub worker: rebuild listener:", err)
		os.Exit(2)
	}
	return listener
}

func stubSignalReady() {
	fd, err := strconv.Atoi(os.Getenv(hostsupervisor.EnvReadyFD))
	if err != nil {
		return
	}
	file := os.NewFile(uintptr(fd), "stub-ready")
	_, _ = io.WriteString(file, "ready\n")
	_ = file.Close()
}

func stubProgress(ctx context.Context) {
	fd, err := strconv.Atoi(os.Getenv(hostsupervisor.EnvProgressFD))
	if err != nil {
		return
	}
	file := os.NewFile(uintptr(fd), "stub-progress")
	defer func() { _ = file.Close() }()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := io.WriteString(file, "progress\n"); err != nil {
				return
			}
		}
	}
}

type harness struct {
	supervisor *hostsupervisor.Supervisor
	addr       string
	runErr     chan error
	spawnCount atomic.Int32
}

func startHarness(t *testing.T, opts hostsupervisor.Options) *harness {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	h := &harness{
		supervisor: nil,
		addr:       listener.Addr().String(),
		runErr:     make(chan error, 1),
		spawnCount: atomic.Int32{},
	}
	opts.Listener = listener
	opts.WorkerCommand = func(spec hostsupervisor.WorkerSpec) *exec.Cmd {
		h.spawnCount.Add(1)
		return reExecWorker(spec)
	}
	supervisor := hostsupervisor.New(opts)
	h.supervisor = supervisor
	ctx, cancel := context.WithCancel(context.Background())
	go func() { h.runErr <- supervisor.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-h.runErr:
		case <-time.After(harnessTimeout):
		}
	})
	h.waitServing(t)
	return h
}

// reExecWorker re-runs the test binary as a stub worker, preserving the fd wiring
// the supervisor computed and the inherited files.
func reExecWorker(spec hostsupervisor.WorkerSpec) *exec.Cmd {
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(append([]string(nil), spec.Environment...), workerMainEnv+"=1")
	cmd.ExtraFiles = spec.ExtraFiles
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd
}

func (h *harness) waitServing(t *testing.T) int {
	t.Helper()
	deadline := time.Now().Add(harnessTimeout)
	for {
		select {
		case err := <-h.runErr:
			t.Fatalf("supervisor exited before serving: %v", err)
		default:
		}
		pid := h.supervisor.CurrentWorkerPID()
		if pid > 0 {
			if _, err := httpGet(h.addr); err == nil {
				return pid
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("first worker did not begin serving before deadline")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func httpGet(addr string) (string, error) {
	client := &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		Timeout:   2 * time.Second,
	}
	resp, err := client.Get("http://" + addr + "/")
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// TestReloadKeepsListenerUpAcrossWorkerSwap proves a worker reload is a
// listener-only handoff: the replacement worker serves on the same listener, the
// served generation advances from one to two, and a continuous poller never once
// fails to reach the listener across the swap.
func TestReloadKeepsListenerUpAcrossWorkerSwap(t *testing.T) {
	h := startHarness(t, hostsupervisor.Options{Log: nil})

	firstGen, err := httpGet(h.addr)
	if err != nil {
		t.Fatalf("first serve: %v", err)
	}
	if firstGen != "1" {
		t.Fatalf("first generation body = %q, want 1", firstGen)
	}
	firstPID := h.supervisor.CurrentWorkerPID()

	// Dial the listener continuously across the reload. A successful dial proves the
	// listening socket stays in LISTEN, which is the "no listener gap" property: the
	// supervisor owns the socket and each worker holds a dup, so a draining worker
	// closing its dup never closes the socket. A gap would surface as a refused dial.
	pollStop := make(chan struct{})
	pollFailures := make(chan error, 1)
	go func() {
		for {
			select {
			case <-pollStop:
				close(pollFailures)
				return
			default:
			}
			conn, err := net.DialTimeout("tcp", h.addr, time.Second)
			if err != nil {
				select {
				case pollFailures <- err:
				default:
				}
			} else {
				_ = conn.Close()
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	h.supervisor.RequestReload()

	deadline := time.Now().Add(harnessTimeout)
	for {
		body, err := httpGet(h.addr)
		if err == nil && body == "2" && h.supervisor.CurrentWorkerPID() != firstPID {
			break
		}
		if time.Now().After(deadline) {
			close(pollStop)
			t.Fatalf("reload did not promote a new worker before deadline; last body = %q err = %v", body, err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	close(pollStop)
	if err, ok := <-pollFailures; ok {
		t.Fatalf("listener dropped during reload: %v", err)
	}
	if h.supervisor.CurrentWorkerPID() == firstPID {
		t.Fatal("worker pid did not change after reload")
	}
}

// TestStallWatchdogRestartsStalledWorker proves the watchdog restarts a worker
// whose reconcile loop stops stamping progress: the stub worker never stamps, so
// the watchdog fires and promotes a fresh generation with a different pid.
func TestStallWatchdogRestartsStalledWorker(t *testing.T) {
	t.Setenv(workerStallEnv, "1")
	h := startHarness(t, hostsupervisor.Options{
		StallTimeout:            100 * time.Millisecond,
		StallCheckInterval:      20 * time.Millisecond,
		ReplacementReadyTimeout: 5 * time.Second,
		Log:                     nil,
	})
	firstPID := h.supervisor.CurrentWorkerPID()

	deadline := time.Now().Add(harnessTimeout)
	for {
		if pid := h.supervisor.CurrentWorkerPID(); pid > 0 && pid != firstPID {
			return
		}
		select {
		case err := <-h.runErr:
			t.Fatalf("supervisor exited before restart: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("stall watchdog did not restart the worker before deadline")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// TestReloadDrainsOldWorker proves the old worker is stopped after the replacement
// takes over, so a reload does not leak the superseded worker.
func TestReloadDrainsOldWorker(t *testing.T) {
	h := startHarness(t, hostsupervisor.Options{Log: nil})
	firstPID := h.supervisor.CurrentWorkerPID()

	h.supervisor.RequestReload()

	deadline := time.Now().Add(harnessTimeout)
	for {
		current := h.supervisor.CurrentWorkerPID()
		if current > 0 && current != firstPID && !processAlive(firstPID) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("old worker %d was not drained after reload", firstPID)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
