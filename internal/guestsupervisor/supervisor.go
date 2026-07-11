//go:build unix

package guestsupervisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"goodkind.io/gha-mac-broker/internal/guestexec"
)

const (
	// firstWorkerReadyTimeout bounds the initial worker's boot before Run fails.
	firstWorkerReadyTimeout = 30 * time.Second
	// workerStopTimeout bounds a graceful worker stop before a forced kill.
	workerStopTimeout = 10 * time.Second
	// unobservedExitCode marks a runner exit the supervisor could not read, so
	// the terminal degrades rather than misreporting a code.
	unobservedExitCode = -1
	// firstWorkerFDBase is the first inheritable descriptor number a child sees;
	// exec.Cmd.ExtraFiles maps entry i to descriptor firstWorkerFDBase+i.
	firstWorkerFDBase = 3
)

// WorkerSpec describes one worker invocation the supervisor is about to spawn.
// The default WorkerCommand turns it into an [exec.Cmd]; tests override
// WorkerCommand to re-exec the test binary as a worker while preserving the
// environment and inherited files the supervisor computed.
type WorkerSpec struct {
	ExecutablePath string
	Arguments      []string
	Environment    []string
	ExtraFiles     []*os.File
}

// Options configures a Supervisor.
type Options struct {
	// Listener is the Connect TCP listener the supervisor owns and hands to
	// every worker generation. The supervisor never serves on it directly.
	Listener *net.TCPListener
	// ControlSocketPath is the unix domain control socket workers dial.
	ControlSocketPath string
	// Token is the boot-scoped bearer token passed to each worker.
	Token string
	// SlotCount is the configured guest execution slot count.
	SlotCount uint32
	// WorkerCommand builds the exec.Cmd for a worker from a WorkerSpec. When nil
	// the supervisor uses a production builder that execs ExecutablePath.
	WorkerCommand func(WorkerSpec) *exec.Cmd
	// Log receives supervisor lifecycle logs; defaults to slog.Default().
	Log *slog.Logger
}

type child struct {
	executionID string
	pid         int
	pgid        int
	cmd         *exec.Cmd
	stdoutR     *os.File
	stderrR     *os.File
	exited      bool
}

type workerHandle struct {
	generation uint64
	cmd        *exec.Cmd
}

type workerExit struct {
	generation uint64
	err        error
}

// Supervisor is the durable parent of the runner children and the spawner of the
// swappable worker. It owns the TCP listener, forks runners in their own process
// groups, holds their pipe read ends open for life, waits them, and replaces the
// worker on request while the runners keep running.
type Supervisor struct {
	opts          Options
	log           *slog.Logger
	controlSocket *net.UnixListener

	mu                sync.Mutex
	state             State
	generationCounter uint64
	currentGeneration uint64
	pendingGeneration uint64
	superseded        map[uint64]struct{}
	workers           map[uint64]*workerHandle
	children          map[int]*child
	unacked           map[int]guestexec.ExitReport
	exitNotify        chan struct{}

	workerExitCh chan workerExit
	firstReady   chan struct{}
	firstOnce    sync.Once
}

// New returns a Supervisor for the supplied options.
func New(opts Options) *Supervisor {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	return &Supervisor{
		opts:              opts,
		log:               log,
		controlSocket:     nil,
		mu:                sync.Mutex{},
		state:             StateBooting,
		generationCounter: 0,
		currentGeneration: 0,
		pendingGeneration: 0,
		superseded:        make(map[uint64]struct{}),
		workers:           make(map[uint64]*workerHandle),
		children:          make(map[int]*child),
		unacked:           make(map[int]guestexec.ExitReport),
		exitNotify:        make(chan struct{}),
		workerExitCh:      make(chan workerExit, 8),
		firstReady:        make(chan struct{}),
		firstOnce:         sync.Once{},
	}
}

// Run spawns the first worker, serves the control socket, and supervises worker
// replacement until ctx is canceled or the current worker exits unexpectedly.
func (s *Supervisor) Run(ctx context.Context) error {
	if s.opts.Listener == nil {
		return fmt.Errorf("guestsupervisor: listener is required")
	}
	if s.opts.ControlSocketPath == "" {
		return fmt.Errorf("guestsupervisor: control socket path is required")
	}
	_ = os.Remove(s.opts.ControlSocketPath)
	controlSocket, err := net.ListenUnix("unix", &net.UnixAddr{Name: s.opts.ControlSocketPath, Net: "unix"})
	if err != nil {
		s.log.ErrorContext(ctx, "guest supervisor control socket listen failed", "socket", s.opts.ControlSocketPath, "err", err)
		return fmt.Errorf("guestsupervisor: listen control socket %s: %w", s.opts.ControlSocketPath, err)
	}
	s.controlSocket = controlSocket
	defer func() { _ = controlSocket.Close() }()
	defer func() { _ = os.Remove(s.opts.ControlSocketPath) }()
	defer s.shutdownChildren()

	controlErrCh := make(chan error, 1)
	goSafe(s.log, "control accept", func() {
		controlErrCh <- s.serveControl(ctx)
	})

	readyPipeRead, err := s.spawnFirstWorker()
	if err != nil {
		return err
	}
	defer func() {
		if readyPipeRead != nil {
			_ = readyPipeRead.Close()
		}
	}()

	select {
	case <-s.firstReady:
		s.log.InfoContext(ctx, "guest supervisor first worker attached", "worker_pid", s.CurrentWorkerPID())
	case exit := <-s.workerExitCh:
		s.handleWorkerExitBookkeeping(exit)
		return fmt.Errorf("guestsupervisor: first worker exited before attach: %w", workerExitErr(exit.err))
	case <-time.After(firstWorkerReadyTimeout):
		return fmt.Errorf("guestsupervisor: first worker did not attach within %s", firstWorkerReadyTimeout)
	case <-ctx.Done():
		return nil
	}

	return s.supervise(ctx, controlErrCh)
}

// CurrentWorkerPID returns the process id of the worker currently serving, or 0
// if none is current yet. It reads under the supervisor lock, so it is safe to
// call while the supervisor spawns and replaces workers.
func (s *Supervisor) CurrentWorkerPID() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	handle, ok := s.workers[s.currentGeneration]
	if !ok || handle.cmd.Process == nil {
		return 0
	}
	return handle.cmd.Process.Pid
}

// supervise runs the steady-state loop after the first worker attaches.
func (s *Supervisor) supervise(ctx context.Context, controlErrCh <-chan error) error {
	for {
		select {
		case <-ctx.Done():
			s.stopCurrentWorker()
			return nil
		case err := <-controlErrCh:
			if err != nil && !errors.Is(err, net.ErrClosed) {
				return err
			}
			return nil
		case exit := <-s.workerExitCh:
			if fatal, fatalErr := s.handleWorkerExit(exit); fatal {
				return fatalErr
			}
		}
	}
}

// handleWorkerExit records a worker generation's exit. A superseded generation
// draining after a reload is expected; a pending generation crashing before it
// attaches reverts the phase to Steady; the current generation exiting is fatal
// so launchd can restart the supervisor.
func (s *Supervisor) handleWorkerExit(exit workerExit) (bool, error) {
	s.mu.Lock()
	delete(s.workers, exit.generation)
	_, wasSuperseded := s.superseded[exit.generation]
	delete(s.superseded, exit.generation)
	isCurrent := exit.generation == s.currentGeneration
	isPending := exit.generation == s.pendingGeneration
	replacing := s.state == StateReplacing
	if isPending {
		s.pendingGeneration = 0
		s.state = StateSteady
	}
	s.mu.Unlock()

	if wasSuperseded {
		s.log.Debug("guest supervisor superseded worker exited", "generation", exit.generation)
		return false, nil
	}
	if isPending {
		s.log.Warn("guest supervisor replacement worker exited before attach; staying on current worker",
			"generation", exit.generation, "err", exit.err)
		return false, nil
	}
	if isCurrent && replacing {
		// The old current worker drained mid-swap before its replacement
		// attached; the replacement takes over on attach, so this is expected.
		s.log.Debug("guest supervisor current worker drained during replacement", "generation", exit.generation)
		return false, nil
	}
	if isCurrent {
		return true, fmt.Errorf("guestsupervisor: current worker exited: %w", workerExitErr(exit.err))
	}
	s.log.Debug("guest supervisor stale worker exited", "generation", exit.generation, "err", exit.err)
	return false, nil
}

func (s *Supervisor) handleWorkerExitBookkeeping(exit workerExit) {
	s.mu.Lock()
	delete(s.workers, exit.generation)
	delete(s.superseded, exit.generation)
	s.mu.Unlock()
}

// spawnFirstWorker starts generation one with a supervisor-owned readiness pipe
// and no inherited runner pipes or snapshot. It returns the readiness read end so
// Run can close it; readiness is confirmed by the worker's attach, not the pipe.
func (s *Supervisor) spawnFirstWorker() (*os.File, error) {
	readRead, readWrite, err := os.Pipe()
	if err != nil {
		s.log.Warn("guest supervisor first worker readiness pipe failed", "err", err)
		return nil, fmt.Errorf("guestsupervisor: create first worker readiness pipe: %w", err)
	}
	executablePath, err := os.Executable()
	if err != nil {
		_ = readRead.Close()
		_ = readWrite.Close()
		return nil, fmt.Errorf("guestsupervisor: resolve executable: %w", err)
	}
	generation := s.reserveGeneration()
	extraFiles, env, cleanup, err := s.buildInherit(os.Environ(), generation, nil, readWrite)
	if err != nil {
		_ = readRead.Close()
		_ = readWrite.Close()
		return nil, err
	}
	spec := WorkerSpec{
		ExecutablePath: executablePath,
		Arguments:      []string{executablePath, "guest-worker"},
		Environment:    env,
		ExtraFiles:     extraFiles,
	}
	if err := s.spawnWorker(generation, spec); err != nil {
		cleanup()
		_ = readRead.Close()
		_ = readWrite.Close()
		return nil, err
	}
	cleanup()
	_ = readWrite.Close()
	return readRead, nil
}

// spawnWorker builds and starts a worker command, tracking its handle and a
// goroutine that reports its exit.
func (s *Supervisor) spawnWorker(generation uint64, spec WorkerSpec) error {
	build := s.opts.WorkerCommand
	if build == nil {
		build = defaultWorkerCommand
	}
	cmd := build(spec)
	if err := cmd.Start(); err != nil {
		s.log.Warn("guest supervisor worker start failed", "generation", generation, "err", err)
		return fmt.Errorf("guestsupervisor: start worker generation %d: %w", generation, err)
	}
	handle := &workerHandle{generation: generation, cmd: cmd}
	s.mu.Lock()
	s.workers[generation] = handle
	s.mu.Unlock()
	goSafe(s.log, "worker wait", func() {
		s.workerExitCh <- workerExit{generation: generation, err: cmd.Wait()}
	})
	s.log.Info("guest supervisor worker spawned", "generation", generation, "pid", cmd.Process.Pid)
	return nil
}

func defaultWorkerCommand(spec WorkerSpec) *exec.Cmd {
	cmd := &exec.Cmd{
		Path:       spec.ExecutablePath,
		Args:       spec.Arguments,
		Env:        spec.Environment,
		ExtraFiles: spec.ExtraFiles,
		Stdout:     os.Stdout,
		Stderr:     os.Stderr,
	}
	return cmd
}

func (s *Supervisor) reserveGeneration() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.generationCounter++
	generation := s.generationCounter
	s.pendingGeneration = generation
	return generation
}

// buildInherit assembles the ExtraFiles and fd environment a worker inherits: the
// listener dup, each live child's durable pipe read ends, and optionally the
// snapshot and readiness write end. The returned cleanup closes the transient
// dups (listener, snapshot, readiness) after the child has forked, leaving the
// durable runner read ends open.
func (s *Supervisor) buildInherit(
	baseEnv []string,
	generation uint64,
	snapshot *os.File,
	readyWrite *os.File,
) ([]*os.File, []string, func(), error) {
	listenerFile, err := s.opts.Listener.File()
	if err != nil {
		s.log.Warn("guest supervisor listener dup failed", "err", err)
		return nil, nil, nil, fmt.Errorf("guestsupervisor: dup listener: %w", err)
	}
	extraFiles := []*os.File{listenerFile}
	listenerFD := firstWorkerFDBase

	s.mu.Lock()
	pids := make([]int, 0, len(s.children))
	for pid := range s.children {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	pipeWires := make([]pipeFDWire, 0, len(pids))
	for _, pid := range pids {
		runnerChild := s.children[pid]
		extraFiles = append(extraFiles, runnerChild.stdoutR)
		stdoutFD := firstWorkerFDBase + len(extraFiles) - 1
		extraFiles = append(extraFiles, runnerChild.stderrR)
		stderrFD := firstWorkerFDBase + len(extraFiles) - 1
		pipeWires = append(pipeWires, pipeFDWire{PID: pid, Stdout: stdoutFD, Stderr: stderrFD})
	}
	s.mu.Unlock()

	snapshotFD := -1
	if snapshot != nil {
		extraFiles = append(extraFiles, snapshot)
		snapshotFD = firstWorkerFDBase + len(extraFiles) - 1
	}
	readyFD := -1
	if readyWrite != nil {
		extraFiles = append(extraFiles, readyWrite)
		readyFD = firstWorkerFDBase + len(extraFiles) - 1
	}

	env, err := s.assembleEnv(baseEnv, generation, listenerFD, pipeWires, snapshotFD, readyFD)
	if err != nil {
		_ = listenerFile.Close()
		return nil, nil, nil, err
	}
	cleanup := func() {
		_ = listenerFile.Close()
		if snapshot != nil {
			_ = snapshot.Close()
		}
		if readyWrite != nil {
			_ = readyWrite.Close()
		}
	}
	return extraFiles, env, cleanup, nil
}

func (s *Supervisor) assembleEnv(
	baseEnv []string,
	generation uint64,
	listenerFD int,
	pipeWires []pipeFDWire,
	snapshotFD int,
	readyFD int,
) ([]string, error) {
	pipeJSON := ""
	if len(pipeWires) > 0 {
		encoded, err := json.Marshal(pipeWires)
		if err != nil {
			s.log.Warn("guest supervisor encode pipe fd table failed", "err", err)
			return nil, fmt.Errorf("guestsupervisor: encode pipe fd table: %w", err)
		}
		pipeJSON = string(encoded)
	}
	overrides := map[string]string{
		EnvControlSocket: s.opts.ControlSocketPath,
		EnvListenerFD:    strconv.Itoa(listenerFD),
		EnvReadyFD:       strconv.Itoa(readyFD),
		EnvGeneration:    strconv.FormatUint(generation, 10),
		EnvSlots:         strconv.FormatUint(uint64(s.opts.SlotCount), 10),
		EnvToken:         s.opts.Token,
		EnvPipeFDs:       pipeJSON,
		EnvSnapshotFD:    "",
	}
	if snapshotFD >= 0 {
		overrides[EnvSnapshotFD] = strconv.Itoa(snapshotFD)
	}
	return envWithOverrides(baseEnv, overrides), nil
}

// serveControl accepts control connections until the socket closes.
func (s *Supervisor) serveControl(ctx context.Context) error {
	for {
		conn, err := s.controlSocket.AcceptUnix()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			s.log.WarnContext(ctx, "guest supervisor control accept failed", "err", err)
			return fmt.Errorf("guestsupervisor: accept control: %w", err)
		}
		goSafe(s.log, "control request", func() {
			s.handleControl(ctx, conn)
		})
	}
}

func (s *Supervisor) handleControl(ctx context.Context, conn *net.UnixConn) {
	defer func() { _ = conn.Close() }()
	body, files, err := readFrame(conn)
	if err != nil {
		// A draining worker closing its control connection surfaces as EOF; that is
		// expected during a swap, so it stays at debug rather than warning.
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
			s.log.DebugContext(ctx, "guest supervisor control connection closed", "err", err)
			return
		}
		s.log.WarnContext(ctx, "guest supervisor control read failed", "err", err)
		return
	}
	var request controlRequest
	if err := json.Unmarshal(body, &request); err != nil {
		closeFiles(files)
		s.replyError(conn, fmt.Errorf("decode control request: %w", err))
		return
	}
	switch request.Op {
	case opStatus:
		closeFiles(files)
		s.handleStatus(conn)
	case opAttach:
		closeFiles(files)
		s.handleAttach(conn, request)
	case opStartChild:
		closeFiles(files)
		s.handleStartChild(conn, request)
	case opPollExits:
		closeFiles(files)
		s.handlePollExits(ctx, conn, request)
	case opAckExit:
		closeFiles(files)
		s.handleAckExit(conn, request)
	case opReplaceWorker:
		s.handleReplaceWorker(conn, request, files)
	default:
		closeFiles(files)
		s.replyError(conn, fmt.Errorf("unsupported control op %q", request.Op))
	}
}

func (s *Supervisor) handleStatus(conn *net.UnixConn) {
	response := newControlResponse()
	s.mu.Lock()
	response.SupervisorPID = os.Getpid()
	response.State = s.state.String()
	response.Generation = s.currentGeneration
	s.mu.Unlock()
	s.reply(conn, response, nil)
}

func (s *Supervisor) handleAttach(conn *net.UnixConn, request controlRequest) {
	s.mu.Lock()
	switch {
	case request.Generation == s.currentGeneration && s.currentGeneration != 0:
		// Idempotent re-attach from the already-current worker.
	case request.Generation == s.pendingGeneration && request.Generation != 0:
		previous := s.currentGeneration
		if previous != 0 {
			s.superseded[previous] = struct{}{}
		}
		s.currentGeneration = request.Generation
		s.pendingGeneration = 0
		s.state = StateSteady
	default:
		s.mu.Unlock()
		s.replyError(conn, fmt.Errorf("attach from stale generation %d (current %d, pending %d)",
			request.Generation, s.currentGeneration, s.pendingGeneration))
		return
	}
	response := newControlResponse()
	response.SupervisorPID = os.Getpid()
	response.State = s.state.String()
	response.Generation = s.currentGeneration
	s.mu.Unlock()
	s.firstOnce.Do(func() { close(s.firstReady) })
	s.notifyExit()
	s.reply(conn, response, nil)
}

// handleStartChild forks a runner in its own process group, keeps its durable
// read ends, starts its waitpid goroutine, and returns the pid, pgid, and the
// worker's private dup of the read ends over SCM_RIGHTS.
func (s *Supervisor) handleStartChild(conn *net.UnixConn, request controlRequest) {
	spec := wireToExecSpec(request.Spec)
	launched, err := guestexec.Launch(spec)
	if err != nil {
		s.replyError(conn, fmt.Errorf("launch runner: %w", err))
		return
	}
	stdoutR, stdoutOK := launched.Stdout.(*os.File)
	stderrR, stderrOK := launched.Stderr.(*os.File)
	if !stdoutOK || !stderrOK {
		_ = killGroup(launched.PGID)
		_ = launched.Stdout.Close()
		_ = launched.Stderr.Close()
		s.replyError(conn, fmt.Errorf("runner pipe read ends are not files"))
		return
	}
	runnerChild := &child{
		executionID: spec.ExecutionID,
		pid:         launched.PID,
		pgid:        launched.PGID,
		cmd:         launched.Command,
		stdoutR:     stdoutR,
		stderrR:     stderrR,
		exited:      false,
	}
	s.mu.Lock()
	s.children[launched.PID] = runnerChild
	s.mu.Unlock()

	goSafe(s.log, "runner wait", func() {
		s.waitRunner(runnerChild)
	})

	response := newControlResponse()
	response.PID = launched.PID
	response.PGID = launched.PGID
	s.reply(conn, response, []*os.File{stdoutR, stderrR})
}

// waitRunner reaps one runner and buffers its exit until a worker acks it.
func (s *Supervisor) waitRunner(runnerChild *child) {
	waitErr := runnerChild.cmd.Wait()
	exitCode, message := exitOutcome(runnerChild.cmd, waitErr)
	s.mu.Lock()
	runnerChild.exited = true
	s.unacked[runnerChild.pid] = guestexec.ExitReport{PID: runnerChild.pid, ExitCode: exitCode, Message: message}
	s.mu.Unlock()
	s.notifyExit()
	s.log.Info("guest supervisor runner exited", "pid", runnerChild.pid, "exit_code", exitCode)
}

func (s *Supervisor) handlePollExits(ctx context.Context, conn *net.UnixConn, request controlRequest) {
	timeout := time.Duration(request.PollTimeoutMillis) * time.Millisecond
	deadline := time.After(timeout)
	for {
		s.mu.Lock()
		if request.Generation != s.currentGeneration {
			s.mu.Unlock()
			s.replyExits(conn, nil)
			return
		}
		if len(s.unacked) > 0 {
			exits := s.snapshotUnackedLocked()
			s.mu.Unlock()
			s.replyExits(conn, exits)
			return
		}
		notify := s.exitNotify
		s.mu.Unlock()
		if timeout <= 0 {
			s.replyExits(conn, nil)
			return
		}
		select {
		case <-notify:
		case <-deadline:
			s.replyExits(conn, nil)
			return
		case <-ctx.Done():
			s.replyExits(conn, nil)
			return
		}
	}
}

func (s *Supervisor) replyExits(conn *net.UnixConn, exits []exitWire) {
	response := newControlResponse()
	response.Exits = exits
	s.reply(conn, response, nil)
}

func (s *Supervisor) snapshotUnackedLocked() []exitWire {
	pids := make([]int, 0, len(s.unacked))
	for pid := range s.unacked {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	exits := make([]exitWire, 0, len(pids))
	for _, pid := range pids {
		report := s.unacked[pid]
		exits = append(exits, exitWire{PID: report.PID, ExitCode: report.ExitCode, Message: report.Message})
	}
	return exits
}

func (s *Supervisor) handleAckExit(conn *net.UnixConn, request controlRequest) {
	s.mu.Lock()
	delete(s.unacked, request.AckPID)
	runnerChild, found := s.children[request.AckPID]
	if found && runnerChild.exited {
		delete(s.children, request.AckPID)
	} else {
		runnerChild = nil
	}
	s.mu.Unlock()
	if runnerChild != nil {
		_ = runnerChild.stdoutR.Close()
		_ = runnerChild.stderrR.Close()
	}
	s.reply(conn, newControlResponse(), nil)
}

// handleReplaceWorker spawns a replacement worker, handing it the durable
// listener and runner pipes plus the caller's snapshot and readiness write end.
// It rejects the request unless the supervisor is Steady, so only one swap runs
// at a time.
func (s *Supervisor) handleReplaceWorker(conn *net.UnixConn, request controlRequest, files []*os.File) {
	if len(files) != 2 {
		closeFiles(files)
		s.replyError(conn, fmt.Errorf("replace_worker expects snapshot and readiness files, got %d", len(files)))
		return
	}
	snapshot := files[0]
	readyWrite := files[1]

	s.mu.Lock()
	if s.state != StateSteady {
		s.mu.Unlock()
		_ = snapshot.Close()
		_ = readyWrite.Close()
		s.replyError(conn, fmt.Errorf("replace_worker rejected: supervisor state is %s, not steady", s.state))
		return
	}
	s.state = StateReplacing
	s.mu.Unlock()

	generation := s.reserveGeneration()
	baseEnv := request.Environment
	if baseEnv == nil {
		baseEnv = os.Environ()
	}
	extraFiles, env, cleanup, err := s.buildInherit(baseEnv, generation, snapshot, readyWrite)
	if err != nil {
		s.revertReplacing(generation)
		_ = snapshot.Close()
		_ = readyWrite.Close()
		s.replyError(conn, err)
		return
	}
	arguments := request.Arguments
	if len(arguments) == 0 {
		arguments = []string{request.ExecutablePath}
	}
	spec := WorkerSpec{
		ExecutablePath: request.ExecutablePath,
		Arguments:      arguments,
		Environment:    env,
		ExtraFiles:     extraFiles,
	}
	if err := s.spawnWorker(generation, spec); err != nil {
		cleanup()
		s.revertReplacing(generation)
		s.replyError(conn, err)
		return
	}
	cleanup()
	s.mu.Lock()
	newPID := 0
	if handle, ok := s.workers[generation]; ok && handle.cmd.Process != nil {
		newPID = handle.cmd.Process.Pid
	}
	s.mu.Unlock()
	response := newControlResponse()
	response.NewPID = newPID
	s.reply(conn, response, nil)
}

func (s *Supervisor) revertReplacing(generation uint64) {
	s.mu.Lock()
	if s.pendingGeneration == generation {
		s.pendingGeneration = 0
		s.state = StateSteady
	}
	s.mu.Unlock()
}

func (s *Supervisor) notifyExit() {
	s.mu.Lock()
	close(s.exitNotify)
	s.exitNotify = make(chan struct{})
	s.mu.Unlock()
}

func (s *Supervisor) reply(conn *net.UnixConn, response controlResponse, files []*os.File) {
	payload, err := json.Marshal(response)
	if err != nil {
		s.log.Warn("guest supervisor encode control response failed", "err", err)
		return
	}
	if err := writeFrame(conn, payload, files); err != nil {
		s.log.Warn("guest supervisor write control response failed", "err", err)
	}
}

func (s *Supervisor) replyError(conn *net.UnixConn, err error) {
	response := newControlResponse()
	response.Error = err.Error()
	payload, marshalErr := json.Marshal(response)
	if marshalErr != nil {
		return
	}
	_ = writeFrame(conn, payload, nil)
}

// stopCurrentWorker signals the current worker to stop and waits briefly before
// forcing it.
func (s *Supervisor) stopCurrentWorker() {
	s.mu.Lock()
	handle := s.workers[s.currentGeneration]
	s.mu.Unlock()
	if handle == nil || handle.cmd.Process == nil {
		return
	}
	_ = handle.cmd.Process.Signal(os.Interrupt)
	select {
	case exit := <-s.workerExitCh:
		s.handleWorkerExitBookkeeping(exit)
	case <-time.After(workerStopTimeout):
		_ = handle.cmd.Process.Kill()
	}
}

// shutdownChildren kills every runner process group and closes the durable read
// ends, so a supervisor shutdown leaves no stray runners or leaked pipes.
func (s *Supervisor) shutdownChildren() {
	s.mu.Lock()
	children := make([]*child, 0, len(s.children))
	for _, runnerChild := range s.children {
		children = append(children, runnerChild)
	}
	s.children = make(map[int]*child)
	s.mu.Unlock()
	for _, runnerChild := range children {
		_ = killGroup(runnerChild.pgid)
		_ = runnerChild.stdoutR.Close()
		_ = runnerChild.stderrR.Close()
	}
}

func exitOutcome(cmd *exec.Cmd, waitErr error) (int, string) {
	state := cmd.ProcessState
	if state == nil {
		return unobservedExitCode, waitErrMessage(waitErr)
	}
	code := state.ExitCode()
	if code < 0 {
		return unobservedExitCode, ""
	}
	message := ""
	var exitErr *exec.ExitError
	if waitErr != nil && !errors.As(waitErr, &exitErr) {
		message = waitErr.Error()
	}
	return code, message
}

func waitErrMessage(waitErr error) string {
	if waitErr == nil {
		return "exit status unobserved"
	}
	return waitErr.Error()
}

func workerExitErr(err error) error {
	if err == nil {
		return errors.New("worker exited")
	}
	return err
}

func envWithOverrides(base []string, overrides map[string]string) []string {
	out := make([]string, 0, len(base)+len(overrides))
	for _, entry := range base {
		key, ok := envKey(entry)
		if !ok {
			out = append(out, entry)
			continue
		}
		if _, replaced := overrides[key]; replaced {
			continue
		}
		out = append(out, entry)
	}
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out = append(out, key+"="+overrides[key])
	}
	return out
}

func envKey(entry string) (string, bool) {
	index := strings.IndexByte(entry, '=')
	if index <= 0 {
		return "", false
	}
	return entry[:index], true
}

func goSafe(log *slog.Logger, label string, fn func()) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Error("guest supervisor goroutine panic recovered", "label", label, "err", fmt.Errorf("panic: %v", recovered))
			}
		}()
		fn()
	}()
}
