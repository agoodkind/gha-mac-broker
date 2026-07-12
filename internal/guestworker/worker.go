//go:build unix

// Package guestworker is the swappable in-VM guest worker. It holds only the
// volatile guestexec registry (event ring buffers, sequences, subscribers) and
// serves the ConnectRPC handlers. The durable guest-supervisor owns the runner
// processes, the pipe read ends, and the waitpid loop, so a worker swap never
// touches a running job: the worker freezes its registry, hands a snapshot to
// the supervisor, and the replacement restores it and resumes capture from the
// same offset with no lost bytes.
package guestworker

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"goodkind.io/gha-mac-broker/internal/guestagent"
	"goodkind.io/gha-mac-broker/internal/guestexec"
	"goodkind.io/gha-mac-broker/internal/guestsupervisor"
	"goodkind.io/gha-mac-broker/internal/guesttransport"
)

const (
	readyMessage         = "ready\n"
	pollTimeout          = 2 * time.Second
	pollBackoff          = 200 * time.Millisecond
	replacementReadyWait = 30 * time.Second
	unobservedExitCode   = -1
	unobservedMessage    = "exit status unobserved"
	defaultSlotCount     = uint32(1)
	// degradeFailureThreshold is the number of consecutive poll failures the
	// worker tolerates before it degrades a runner whose process is gone to an
	// unobserved exit. A buffered exit in the supervisor proves it did observe the
	// child, so degrading stays conservative and a single transient dial failure
	// right after a runner exits never overwrites the real exit code with -1.
	degradeFailureThreshold = 5
)

func init() {
	gob.Register(guestexec.PhaseChange{Phase: ""})
	gob.Register(guestexec.LogChunk{Stream: guestexec.StreamUnspecified, Data: nil})
	gob.Register(guestexec.Heartbeat{UnixNanos: 0})
	gob.Register(guestexec.TerminalResult{ExitCode: 0, Message: ""})
}

// specBuilderOverride lets tests substitute a shell passthrough spec builder for
// the production runner executor, so the reload tests drive the durability
// mechanics without a real GitHub runner. It is nil in production, where the
// guest agent defaults to the real runner executor.
var specBuilderOverride guestagent.SpecBuilder

type pipeFD struct {
	PID    int `json:"pid"`
	Stdout int `json:"stdout"`
	Stderr int `json:"stderr"`
}

type config struct {
	controlSocket     string
	listenerFD        int
	readyFD           int
	snapshotFD        int
	generation        uint64
	slotCount         uint32
	token             string
	goldenFingerprint string
	pipes             []pipeFD
}

type worker struct {
	registry      *guestexec.Registry
	controlSocket string
	generation    uint64
	log           *slog.Logger
	tracker       *pidTracker

	cancelRun context.CancelFunc

	pollCancel context.CancelFunc
	pollDone   chan struct{}

	// pollFn returns the supervisor's currently buffered runner exits, injected
	// so a test can drive the degrade threshold without a real supervisor.
	pollFn           func() ([]guestexec.ExitReport, error)
	backoff          time.Duration
	degradeThreshold int

	reloadMu sync.Mutex
	replaced bool
	// pendingExecutable holds the binary an UpdateAgent RPC asked the worker to
	// reload onto. It is read and cleared under reloadMu by the next handoff, so a
	// plain reload with it empty stays on the running binary via os.Executable.
	pendingExecutable string
}

// Run is the guest-worker entry point. It rebuilds its listener and registry
// from inherited file descriptors, attaches to the supervisor, serves RPC,
// signals readiness, and swaps itself out on SIGHUP without disturbing any
// running runner.
func Run(ctx context.Context) error {
	log := slog.Default()
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	listener, err := listenerFromFD(cfg.listenerFD)
	if err != nil {
		return err
	}

	registry, tracker, err := buildRegistry(cfg)
	if err != nil {
		return err
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	w := &worker{
		registry:          registry,
		controlSocket:     cfg.controlSocket,
		generation:        cfg.generation,
		log:               log,
		tracker:           tracker,
		cancelRun:         cancelRun,
		pollCancel:        nil,
		pollDone:          nil,
		pollFn:            nil,
		backoff:           pollBackoff,
		degradeThreshold:  degradeFailureThreshold,
		reloadMu:          sync.Mutex{},
		replaced:          false,
		pendingExecutable: "",
	}
	w.pollFn = func() ([]guestexec.ExitReport, error) {
		return guestsupervisor.PollExits(w.controlSocket, w.generation, pollTimeout)
	}

	handler := buildAgentHandler(cfg, w)

	// Install the reload signal handler before attaching, so once the supervisor
	// marks this worker current a reload signal is always handled here rather than
	// terminating the process through the default hangup disposition.
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	defer signal.Stop(sighup)

	if _, err := guestsupervisor.Attach(cfg.controlSocket, cfg.generation); err != nil {
		return fmt.Errorf("guestworker: attach to supervisor: %w", err)
	}

	serveDone := make(chan error, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				serveErr := fmt.Errorf("guestworker: serve panic: %v", recovered)
				log.ErrorContext(runCtx, "guest worker serve goroutine panic recovered", "err", serveErr)
				serveDone <- serveErr
			}
		}()
		serveDone <- guesttransport.Serve(runCtx, listener, handler, cfg.token)
	}()

	w.startPollLoop(runCtx)
	if err := signalReady(cfg.readyFD); err != nil {
		log.WarnContext(runCtx, "guestworker readiness signal failed", "err", err)
	}
	log.InfoContext(runCtx, "guest worker serving", "generation", cfg.generation, "addr", listener.Addr().String())

	for {
		select {
		case <-runCtx.Done():
			<-serveDone
			return nil
		case serveErr := <-serveDone:
			cancelRun()
			if serveErr != nil && !errors.Is(serveErr, context.Canceled) {
				return fmt.Errorf("guestworker: serve: %w", serveErr)
			}
			return nil
		case <-sighup:
			go func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						w.log.ErrorContext(runCtx, "guest worker reload goroutine panic recovered", "err", fmt.Errorf("panic: %v", recovered))
					}
				}()
				if err := w.reload(runCtx); err != nil {
					w.log.WarnContext(runCtx, "guest worker reload failed; continuing to serve", "err", err)
				}
			}()
		}
	}
}

// reload freezes the registry and hands a snapshot to the supervisor. On a
// successful handoff it cancels its own serve context so in-flight JobStatus
// streams return nil without cancelling any execution, then exits. On any error
// after the freeze it rolls back with resumeServing, so a failed handoff leaves
// the old worker fully serving with live capture and poll rather than bricked on
// a frozen registry. Because resumeServing installs fresh stopped channels, a
// later reload never double-closes and never panics.
func (w *worker) reload(ctx context.Context) error {
	w.reloadMu.Lock()
	defer w.reloadMu.Unlock()
	if w.replaced {
		return fmt.Errorf("guestworker: worker already replaced; reload not permitted")
	}

	// Stop polling before freezing so no exit is acked away from the supervisor
	// while the registry cannot record it; an exit that lands during the swap
	// stays unacked and is redelivered to the replacement.
	w.stopPollLoop()

	snapshot, err := w.registry.Freeze()
	if err != nil {
		w.resumeServing(ctx)
		w.log.WarnContext(ctx, "guest worker freeze registry failed; resumed serving", "err", err)
		return fmt.Errorf("guestworker: freeze registry: %w", err)
	}

	newPID, err := w.handOff(ctx, snapshot)
	if err != nil {
		// The replacement never became current, so resume live serving instead of
		// staying frozen and bricked.
		w.resumeServing(ctx)
		w.log.WarnContext(ctx, "guest worker reload rolled back; resumed serving", "err", err)
		return err
	}

	w.replaced = true
	w.log.InfoContext(ctx, "guest worker replaced; draining", "new_pid", newPID, "generation", w.generation)
	// Cancel the serve context so in-flight JobStatus streams return nil. No
	// execution is cancelled, so every runner keeps running under the supervisor.
	w.cancelRun()
	return nil
}

// handOff writes the snapshot, asks the supervisor to spawn the replacement, and
// waits for it to signal ready. It returns the replacement pid only once the
// replacement has attached (become current) and signaled ready; any earlier
// failure returns an error so the caller rolls back.
func (w *worker) handOff(ctx context.Context, snapshot *guestexec.Snapshot) (int, error) {
	snapshotFile, err := writeSnapshotFile(snapshot)
	if err != nil {
		return 0, err
	}
	defer func() { _ = snapshotFile.Close() }()

	readRead, readWrite, err := os.Pipe()
	if err != nil {
		w.log.WarnContext(ctx, "guest worker create readiness pipe failed", "err", err)
		return 0, fmt.Errorf("guestworker: create readiness pipe: %w", err)
	}
	defer func() { _ = readRead.Close() }()

	executablePath, err := os.Executable()
	if err != nil {
		_ = readWrite.Close()
		w.log.WarnContext(ctx, "guest worker resolve executable failed", "err", err)
		return 0, fmt.Errorf("guestworker: resolve executable: %w", err)
	}
	arguments := append([]string(nil), os.Args...)
	// A prior UpdateAgent handoff records a pending binary under reloadMu, which
	// reload holds across this handoff, so swap onto it and clear it here. A plain
	// reload leaves it empty and stays on the running binary.
	if w.pendingExecutable != "" {
		executablePath = w.pendingExecutable
		arguments = []string{executablePath, "guest-worker"}
		w.pendingExecutable = ""
	}
	newPID, err := guestsupervisor.RequestReplacement(
		w.controlSocket,
		executablePath,
		arguments,
		os.Environ(),
		snapshotFile,
		readWrite,
	)
	// The replacement inherited its own copy of the readiness write end, so this
	// worker closes its copy; otherwise the read end never sees the write side
	// close and readiness could hang.
	_ = readWrite.Close()
	if err != nil {
		return 0, fmt.Errorf("guestworker: request replacement: %w", err)
	}

	if err := waitReady(ctx, readRead); err != nil {
		return 0, fmt.Errorf("guestworker: wait for replacement readiness: %w", err)
	}
	return newPID, nil
}

// triggerAgentUpdate records the verified new binary and raises SIGHUP so the
// worker's own reload path swaps onto it through the durable supervisor handoff.
// It reuses the exact reload flow, so an agent update never disturbs a running
// job and never reimplements the swap.
func (w *worker) triggerAgentUpdate(newBinaryPath string) error {
	// Hold reloadMu across the pending-path record and the SIGHUP so two
	// concurrent UpdateAgent RPCs cannot overwrite each other's pendingExecutable
	// and have a coalesced SIGHUP hand off only one requested version.
	w.reloadMu.Lock()
	defer w.reloadMu.Unlock()
	if w.replaced {
		w.log.Warn("guest worker update reload refused; already replaced", "path", newBinaryPath)
		return fmt.Errorf("guestworker: worker already replaced; update reload not permitted")
	}
	if w.pendingExecutable != "" && w.pendingExecutable != newBinaryPath {
		w.log.Warn("guest worker update reload refused; another update already pending",
			"pending", w.pendingExecutable, "path", newBinaryPath)
		return fmt.Errorf("guestworker: update reload already pending for %q", w.pendingExecutable)
	}
	w.pendingExecutable = newBinaryPath
	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		w.pendingExecutable = ""
		w.log.Warn("guest worker update reload signal failed", "path", newBinaryPath, "err", err)
		return fmt.Errorf("guestworker: signal update reload: %w", err)
	}
	w.log.Info("guest worker update reload signaled", "path", newBinaryPath)
	return nil
}

// resumeServing rolls a failed reload back to live serving: Thaw resumes capture
// on the still-open pipe read ends, and the poll loop restarts under the worker's
// run context so runner exits are recorded again. The worker stays current and
// healthy.
func (w *worker) resumeServing(runCtx context.Context) {
	w.registry.Thaw()
	w.startPollLoop(runCtx)
}

func (w *worker) startPollLoop(ctx context.Context) {
	pollCtx, pollCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	w.pollCancel = pollCancel
	w.pollDone = done
	go func() {
		defer close(done)
		defer func() {
			if recovered := recover(); recovered != nil {
				w.log.ErrorContext(ctx, "guest worker poll loop panic recovered", "err", fmt.Errorf("panic: %v", recovered))
			}
		}()
		w.pollLoop(pollCtx)
	}()
}

func (w *worker) stopPollLoop() {
	if w.pollCancel == nil {
		return
	}
	w.pollCancel()
	<-w.pollDone
}

// pollLoop long-polls the supervisor for runner exits, records each into the
// registry, and acks it. It degrades a runner whose process is gone to an
// unobserved exit only after degradeThreshold consecutive poll failures, so a
// single transient control-socket failure right after a runner exits cannot
// overwrite the real exit code with -1. Any successful poll resets the counter.
func (w *worker) pollLoop(ctx context.Context) {
	consecutiveFailures := 0
	for {
		if ctx.Err() != nil {
			return
		}
		reports, err := w.pollFn()
		if err != nil {
			consecutiveFailures++
			if consecutiveFailures >= w.degradeThreshold {
				w.degradeUnreachable()
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.backoff):
			}
			continue
		}
		consecutiveFailures = 0
		for _, report := range reports {
			w.registry.ReportExit(report.PID, report.ExitCode, report.Message)
			w.tracker.remove(report.PID)
			if ackErr := guestsupervisor.AckExit(w.controlSocket, report.PID); ackErr != nil {
				w.log.WarnContext(ctx, "guest worker ack exit failed", "pid", report.PID, "err", ackErr)
			}
		}
	}
}

// degradeUnreachable reports an unobserved exit for any tracked runner whose
// process is gone while the supervisor is unreachable. The macOS limit is that
// an orphaned runner reparents to launchd and becomes unreapable, so its code
// cannot be observed; degrading to -1 completes the job rather than hanging it.
func (w *worker) degradeUnreachable() {
	for _, pid := range w.tracker.list() {
		if processAlive(pid) {
			continue
		}
		w.registry.ReportExit(pid, unobservedExitCode, unobservedMessage)
		w.tracker.remove(pid)
		w.log.Warn("guest worker degraded runner exit to unobserved", "pid", pid)
	}
}

// buildAgentHandler wires the guest-agent HTTP handler over the registry with a
// supervisor-backed launcher, so the durable supervisor forks and waits each
// runner, plus an update reloader that drives the same reload path onto a new
// binary. A nil specBuilderOverride selects the production runner executor; a
// test sets it to a shell passthrough builder.
func buildAgentHandler(cfg config, w *worker) http.Handler {
	launcher := &supervisorLauncher{
		registry:      w.registry,
		controlSocket: cfg.controlSocket,
		tracker:       w.tracker,
	}
	return guestagent.NewHTTPHandler(w.registry, guestagent.Options{
		SlotCount:         cfg.slotCount,
		BootID:            "",
		AgentBuild:        "",
		GoldenFingerprint: cfg.goldenFingerprint,
		ChildLauncher:     launcher,
		SpecBuilder:       specBuilderOverride,
		Reloader:          &agentReloader{worker: w},
		InstallDir:        "",
		UpdatePublicKey:   nil,
		SlotConfigurer:    &supervisorSlotConfigurer{controlSocket: cfg.controlSocket},
	})
}

// supervisorSlotConfigurer applies a host-requested slot count by asking the
// durable supervisor over the control socket. The supervisor updates its slot
// count and replaces the worker, so the new generation serves the new count.
type supervisorSlotConfigurer struct {
	controlSocket string
}

// ConfigureSlots forwards the requested count to the supervisor and returns the
// applied count.
func (c *supervisorSlotConfigurer) ConfigureSlots(ctx context.Context, slotCount uint32) (uint32, error) {
	applied, err := guestsupervisor.ConfigureSlots(c.controlSocket, slotCount)
	if err != nil {
		slog.WarnContext(ctx, "guest worker configure slots failed", "slot_count", slotCount, "err", err)
		return 0, fmt.Errorf("guestworker: configure slots: %w", err)
	}
	return applied, nil
}

// agentReloader adapts the worker's update trigger to the guest-agent
// AgentReloader interface, so UpdateAgent reuses the PR3 reload path rather than
// reimplementing the durable snapshot handoff.
type agentReloader struct {
	worker *worker
}

// Reload records the new binary and asks the worker to swap onto it.
func (reloader *agentReloader) Reload(_ context.Context, newBinaryPath string) error {
	return reloader.worker.triggerAgentUpdate(newBinaryPath)
}

// supervisorLauncher records an execution by asking the supervisor to fork the
// runner, then registering the returned pipe read ends. The supervisor stays the
// runner's parent and delivers its exit through the poll loop.
type supervisorLauncher struct {
	registry      *guestexec.Registry
	controlSocket string
	tracker       *pidTracker
}

// Run launches spec through the supervisor and registers it, discarding the
// forked runner if admission is refused.
func (l *supervisorLauncher) Run(spec guestexec.ExecSpec) (guestexec.Outcome, error) {
	launched, err := guestsupervisor.StartChild(l.controlSocket, spec)
	if err != nil {
		slog.Warn("guest worker start child failed", "execution_id", spec.ExecutionID, "err", err)
		return guestexec.OutcomeUnspecified, fmt.Errorf("guestworker: start child: %w", err)
	}
	outcome, registerErr := l.registry.Register(spec, launched.PID, launched.PGID, launched.Stdout, launched.Stderr)
	if registerErr != nil || outcome != guestexec.OutcomeAccepted {
		// Admission was refused after the fork, so discard the runner. The
		// supervisor's waitpid observes the kill and buffers the exit, which the
		// poll loop then acks away.
		_ = launched.Stdout.Close()
		_ = launched.Stderr.Close()
		_ = killGroup(launched.PGID)
		if registerErr != nil {
			slog.Warn("guest worker register runner failed", "execution_id", spec.ExecutionID, "err", registerErr)
			return outcome, fmt.Errorf("guestworker: register runner: %w", registerErr)
		}
		return outcome, nil
	}
	l.tracker.add(launched.PID)
	return outcome, nil
}

type pidTracker struct {
	mu   sync.Mutex
	pids map[int]struct{}
}

func newPIDTracker() *pidTracker {
	return &pidTracker{mu: sync.Mutex{}, pids: make(map[int]struct{})}
}

func (t *pidTracker) add(pid int) {
	t.mu.Lock()
	t.pids[pid] = struct{}{}
	t.mu.Unlock()
}

func (t *pidTracker) remove(pid int) {
	t.mu.Lock()
	delete(t.pids, pid)
	t.mu.Unlock()
}

func (t *pidTracker) list() []int {
	t.mu.Lock()
	pids := make([]int, 0, len(t.pids))
	for pid := range t.pids {
		pids = append(pids, pid)
	}
	t.mu.Unlock()
	return pids
}

func buildRegistry(cfg config) (*guestexec.Registry, *pidTracker, error) {
	tracker := newPIDTracker()
	if cfg.snapshotFD < 0 {
		registry := guestexec.New(guestexec.Options{Retention: 0, HeartbeatInterval: 0})
		return registry, tracker, nil
	}

	snapshot, err := readSnapshotFD(cfg.snapshotFD)
	if err != nil {
		return nil, nil, err
	}
	pipeEnds := make(map[int]guestexec.PipeReadEnds, len(cfg.pipes))
	for _, pipe := range cfg.pipes {
		pipeEnds[pipe.PID] = guestexec.PipeReadEnds{
			Stdout: readEndFromFD(pipe.Stdout, "guest-stdout"),
			Stderr: readEndFromFD(pipe.Stderr, "guest-stderr"),
		}
		tracker.add(pipe.PID)
	}
	registry := guestexec.New(guestexec.Options{Retention: 0, HeartbeatInterval: 0})
	if err := registry.Restore(snapshot, pipeEnds, nil); err != nil {
		slog.Warn("guest worker restore registry failed", "err", err)
		return nil, nil, fmt.Errorf("guestworker: restore registry: %w", err)
	}
	return registry, tracker, nil
}

func loadConfig() (config, error) {
	var zero config
	controlSocket := os.Getenv(guestsupervisor.EnvControlSocket)
	if controlSocket == "" {
		slog.Warn("guest worker missing control socket env", "env", guestsupervisor.EnvControlSocket)
		return zero, fmt.Errorf("guestworker requires %s", guestsupervisor.EnvControlSocket)
	}
	token := os.Getenv(guestsupervisor.EnvToken)
	if token == "" {
		return zero, fmt.Errorf("guestworker requires %s", guestsupervisor.EnvToken)
	}
	listenerFD, err := requiredFD(guestsupervisor.EnvListenerFD)
	if err != nil {
		return zero, err
	}
	readyFD, err := requiredFD(guestsupervisor.EnvReadyFD)
	if err != nil {
		return zero, err
	}
	generation, err := strconv.ParseUint(os.Getenv(guestsupervisor.EnvGeneration), 10, 64)
	if err != nil {
		return zero, fmt.Errorf("guestworker parse %s: %w", guestsupervisor.EnvGeneration, err)
	}
	slotCount := defaultSlotCount
	if raw := os.Getenv(guestsupervisor.EnvSlots); raw != "" {
		parsed, parseErr := strconv.ParseUint(raw, 10, 32)
		if parseErr != nil {
			return zero, fmt.Errorf("guestworker parse %s: %w", guestsupervisor.EnvSlots, parseErr)
		}
		if parsed > 0 {
			slotCount = uint32(parsed)
		}
	}
	snapshotFD := -1
	if raw := os.Getenv(guestsupervisor.EnvSnapshotFD); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil {
			return zero, fmt.Errorf("guestworker parse %s: %w", guestsupervisor.EnvSnapshotFD, parseErr)
		}
		snapshotFD = parsed
	}
	pipes, err := parsePipeFDs(os.Getenv(guestsupervisor.EnvPipeFDs))
	if err != nil {
		return zero, err
	}
	return config{
		controlSocket:     controlSocket,
		listenerFD:        listenerFD,
		readyFD:           readyFD,
		snapshotFD:        snapshotFD,
		generation:        generation,
		slotCount:         slotCount,
		token:             token,
		goldenFingerprint: os.Getenv(guestsupervisor.EnvGoldenFingerprint),
		pipes:             pipes,
	}, nil
}

func parsePipeFDs(raw string) ([]pipeFD, error) {
	if raw == "" {
		return nil, nil
	}
	var pipes []pipeFD
	if err := json.Unmarshal([]byte(raw), &pipes); err != nil {
		slog.Warn("guest worker parse pipe fd table failed", "err", err)
		return nil, fmt.Errorf("guestworker parse %s: %w", guestsupervisor.EnvPipeFDs, err)
	}
	return pipes, nil
}

func requiredFD(name string) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		slog.Warn("guest worker missing required fd env", "env", name)
		return 0, fmt.Errorf("guestworker requires %s", name)
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		slog.Warn("guest worker parse required fd env failed", "env", name, "err", err)
		return 0, fmt.Errorf("guestworker parse %s: %w", name, err)
	}
	return value, nil
}

func listenerFromFD(fd int) (net.Listener, error) {
	file := os.NewFile(uintptr(fd), "guest-listener")
	if file == nil {
		slog.Warn("guest worker listener fd invalid", "fd", fd)
		return nil, fmt.Errorf("guestworker: listener fd %d is invalid", fd)
	}
	listener, err := net.FileListener(file)
	// FileListener dups the descriptor, so the inherited file is closed here.
	_ = file.Close()
	if err != nil {
		slog.Warn("guest worker rebuild listener failed", "fd", fd, "err", err)
		return nil, fmt.Errorf("guestworker: rebuild listener from fd %d: %w", fd, err)
	}
	return listener, nil
}

func signalReady(fd int) error {
	file := os.NewFile(uintptr(fd), "guest-ready")
	if file == nil {
		slog.Warn("guest worker readiness fd invalid", "fd", fd)
		return fmt.Errorf("guestworker: readiness fd %d is invalid", fd)
	}
	_, writeErr := file.WriteString(readyMessage)
	closeErr := file.Close()
	if writeErr != nil {
		slog.Warn("guest worker signal readiness failed", "err", writeErr)
		return fmt.Errorf("guestworker: signal readiness: %w", writeErr)
	}
	if closeErr != nil {
		slog.Warn("guest worker close readiness pipe failed", "err", closeErr)
		return fmt.Errorf("guestworker: close readiness pipe: %w", closeErr)
	}
	return nil
}

func waitReady(ctx context.Context, readRead *os.File) error {
	result := make(chan error, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				result <- fmt.Errorf("guestworker: readiness read panic: %v", recovered)
			}
		}()
		data, err := io.ReadAll(readRead)
		if err != nil {
			result <- err
			return
		}
		if string(data) != readyMessage {
			result <- fmt.Errorf("readiness payload %q, want %q", string(data), readyMessage)
			return
		}
		result <- nil
	}()
	select {
	case err := <-result:
		if err != nil {
			slog.WarnContext(ctx, "guest worker replacement readiness failed", "err", err)
			return fmt.Errorf("guestworker: readiness: %w", err)
		}
		return nil
	case <-time.After(replacementReadyWait):
		slog.WarnContext(ctx, "guest worker replacement readiness timed out", "wait", replacementReadyWait)
		return fmt.Errorf("guestworker: replacement did not become ready within %s", replacementReadyWait)
	case <-ctx.Done():
		return fmt.Errorf("guestworker: readiness wait cancelled: %w", ctx.Err())
	}
}

func writeSnapshotFile(snapshot *guestexec.Snapshot) (*os.File, error) {
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(snapshot); err != nil {
		slog.Warn("guest worker encode snapshot failed", "err", err)
		return nil, fmt.Errorf("guestworker: encode snapshot: %w", err)
	}
	file, err := os.CreateTemp("", "guest-snapshot-*")
	if err != nil {
		slog.Warn("guest worker create snapshot temp file failed", "err", err)
		return nil, fmt.Errorf("guestworker: create snapshot temp file: %w", err)
	}
	// Unlink immediately so the snapshot is an anonymous file backed only by the
	// descriptor handed to the replacement.
	_ = os.Remove(file.Name())
	if _, err := file.Write(buffer.Bytes()); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("guestworker: write snapshot: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("guestworker: rewind snapshot: %w", err)
	}
	return file, nil
}

func readSnapshotFD(fd int) (*guestexec.Snapshot, error) {
	file := os.NewFile(uintptr(fd), "guest-snapshot")
	if file == nil {
		slog.Warn("guest worker snapshot fd invalid", "fd", fd)
		return nil, fmt.Errorf("guestworker: snapshot fd %d is invalid", fd)
	}
	defer func() { _ = file.Close() }()
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		slog.Warn("guest worker rewind snapshot failed", "fd", fd, "err", err)
		return nil, fmt.Errorf("guestworker: rewind snapshot fd %d: %w", fd, err)
	}
	var snapshot guestexec.Snapshot
	if err := gob.NewDecoder(file).Decode(&snapshot); err != nil {
		slog.Warn("guest worker decode snapshot failed", "err", err)
		return nil, fmt.Errorf("guestworker: decode snapshot: %w", err)
	}
	return &snapshot, nil
}

// readEndFromFD wraps an inherited pipe read-end descriptor, setting it
// non-blocking first so [os.NewFile] registers it with the runtime poller and
// the resumed capture goroutine can honor a freeze read deadline.
func readEndFromFD(fd int, name string) *os.File {
	_ = syscall.SetNonblock(fd, true)
	return os.NewFile(uintptr(fd), name)
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	// EPERM means the process exists but is owned by another user; ESRCH means it
	// is gone.
	return errors.Is(err, syscall.EPERM)
}

func killGroup(pgid int) error {
	err := syscall.Kill(-pgid, syscall.SIGKILL)
	if err == nil || errors.Is(err, syscall.ESRCH) {
		return nil
	}
	slog.Warn("guest worker kill process group failed", "pgid", pgid, "err", err)
	return fmt.Errorf("guestworker: kill process group %d: %w", pgid, err)
}
