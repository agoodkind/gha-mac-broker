//go:build unix

package hostsupervisor

import (
	"bufio"
	"context"
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
	"syscall"
	"time"
)

const (
	// firstWorkerFDBase is the first inheritable descriptor a child sees;
	// exec.Cmd.ExtraFiles entry i maps to descriptor firstWorkerFDBase+i.
	firstWorkerFDBase = 3
	// defaultFirstReadyTimeout bounds the initial worker's boot before Run fails.
	defaultFirstReadyTimeout = 30 * time.Second
	// defaultReplacementReadyTimeout bounds a replacement worker's boot before the
	// reload rolls back and keeps the old worker current.
	defaultReplacementReadyTimeout = 30 * time.Second
	// defaultWorkerStopTimeout bounds a graceful worker stop before a forced kill.
	defaultWorkerStopTimeout = 15 * time.Second
	// defaultStallCheckInterval is how often the watchdog samples reconcile progress.
	defaultStallCheckInterval = 30 * time.Second
	// workerExitBuffer sizes the worker-exit channel so a burst of superseded
	// workers draining during a swap never blocks a wait goroutine.
	workerExitBuffer = 16
	// reloadRespawnAttempts bounds how many times a reload retries spawning a
	// replacement after the old worker has already stopped, before the supervisor
	// gives up and exits so launchd restarts the whole service.
	reloadRespawnAttempts = 3
	// reloadRespawnBackoff is the pause between replacement spawn attempts.
	reloadRespawnBackoff = time.Second
)

// WorkerSpec describes one worker invocation the supervisor is about to spawn. The
// default WorkerCommand turns it into an [exec.Cmd]; tests override WorkerCommand
// to re-exec the test binary as a worker while preserving the computed environment
// and inherited files.
type WorkerSpec struct {
	ExecutablePath string
	Arguments      []string
	Environment    []string
	ExtraFiles     []*os.File
}

// Options configures a Supervisor.
type Options struct {
	// Listener is the webhook, capacity, and status TCP listener the supervisor
	// owns and hands to every worker generation. The supervisor never serves on it.
	Listener *net.TCPListener
	// WorkerCommand builds the exec.Cmd for a worker from a WorkerSpec. When nil the
	// supervisor uses a production builder that execs ExecutablePath.
	WorkerCommand func(WorkerSpec) *exec.Cmd
	// StallTimeout is how long the reconcile loop may go without stamping progress
	// before the watchdog restarts the worker. A non-positive value disables the
	// watchdog.
	StallTimeout time.Duration
	// StallCheckInterval is how often the watchdog samples progress; it defaults to
	// defaultStallCheckInterval.
	StallCheckInterval time.Duration
	// FirstReadyTimeout bounds the first worker's boot; it defaults to
	// defaultFirstReadyTimeout.
	FirstReadyTimeout time.Duration
	// ReplacementReadyTimeout bounds a replacement worker's boot; it defaults to
	// defaultReplacementReadyTimeout.
	ReplacementReadyTimeout time.Duration
	// WorkerStopTimeout bounds a graceful worker stop; it defaults to
	// defaultWorkerStopTimeout.
	WorkerStopTimeout time.Duration
	// Now returns the current time; it defaults to time.Now. Tests inject a clock.
	Now func() time.Time
	// Log receives supervisor lifecycle logs; it defaults to slog.Default().
	Log *slog.Logger
}

type workerHandle struct {
	generation uint64
	cmd        *exec.Cmd
	done       chan struct{}
}

type workerExit struct {
	generation uint64
	err        error
}

// Supervisor is the durable parent of the swappable host worker. It owns the TCP
// listener, spawns each worker generation with an inherited listener descriptor, a
// readiness pipe, and a progress pipe, replaces the worker on request while the
// listener stays up, and restarts a worker whose reconcile loop has stalled.
type Supervisor struct {
	opts Options
	log  *slog.Logger
	now  func() time.Time

	mu                sync.Mutex
	state             State
	generationCounter uint64
	currentGeneration uint64
	pendingGeneration uint64
	superseded        map[uint64]struct{}
	workers           map[uint64]*workerHandle
	lastProgress      time.Time

	workerExitCh chan workerExit
	reloadCh     chan struct{}
}

// New returns a Supervisor for the supplied options.
func New(opts Options) *Supervisor {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	if opts.StallCheckInterval <= 0 {
		opts.StallCheckInterval = defaultStallCheckInterval
	}
	if opts.FirstReadyTimeout <= 0 {
		opts.FirstReadyTimeout = defaultFirstReadyTimeout
	}
	if opts.ReplacementReadyTimeout <= 0 {
		opts.ReplacementReadyTimeout = defaultReplacementReadyTimeout
	}
	if opts.WorkerStopTimeout <= 0 {
		opts.WorkerStopTimeout = defaultWorkerStopTimeout
	}
	return &Supervisor{
		opts:              opts,
		log:               log,
		now:               now,
		mu:                sync.Mutex{},
		state:             StateBooting,
		generationCounter: 0,
		currentGeneration: 0,
		pendingGeneration: 0,
		superseded:        make(map[uint64]struct{}),
		workers:           make(map[uint64]*workerHandle),
		lastProgress:      time.Time{},
		workerExitCh:      make(chan workerExit, workerExitBuffer),
		reloadCh:          make(chan struct{}, 1),
	}
}

// Run spawns the first worker, waits for it to serve, starts the stall watchdog,
// and supervises worker replacement until ctx is canceled or the current worker
// exits unexpectedly.
func (s *Supervisor) Run(ctx context.Context) error {
	if s.opts.Listener == nil {
		return fmt.Errorf("hostsupervisor: listener is required")
	}
	defer s.shutdownWorkers()

	generation, readyRead, err := s.spawn(os.Environ())
	if err != nil {
		return err
	}
	defer func() { _ = readyRead.Close() }()

	readyCh := readyChan(readyRead)
	select {
	case err := <-readyCh:
		if err != nil {
			s.log.ErrorContext(ctx, "host supervisor first worker did not signal ready", "err", err, "generation", generation)
			return fmt.Errorf("hostsupervisor: first worker readiness: %w", err)
		}
		s.markCurrent(generation)
		s.log.InfoContext(ctx, "host supervisor first worker serving", "generation", generation, "pid", s.CurrentWorkerPID())
	case exit := <-s.workerExitCh:
		s.forgetWorker(exit.generation)
		s.log.ErrorContext(ctx, "host supervisor first worker exited before serving", "err", exit.err, "generation", exit.generation)
		return fmt.Errorf("hostsupervisor: first worker exited before serving: %w", workerExitErr(exit.err))
	case <-time.After(s.opts.FirstReadyTimeout):
		err := fmt.Errorf("hostsupervisor: first worker did not serve within %s", s.opts.FirstReadyTimeout)
		s.log.ErrorContext(ctx, "host supervisor first worker readiness timed out", "err", err, "generation", generation)
		return err
	case <-ctx.Done():
		return nil
	}

	if s.opts.StallTimeout > 0 {
		goSafe(s.log, "stall watchdog", func() { s.runWatchdog(ctx) })
	}
	return s.supervise(ctx)
}

// supervise runs the steady-state loop after the first worker serves.
func (s *Supervisor) supervise(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			// Give the worker the full graceful stop window before returning. Run's
			// deferred shutdownWorkers then only escalates to SIGKILL for anything
			// still alive, which is the intended detach-not-kill behavior on a normal
			// launchd stop rather than an immediate kill.
			s.stopAndWaitGeneration(s.currentWorkerGeneration())
			return nil
		case exit := <-s.workerExitCh:
			if fatal, fatalErr := s.handleWorkerExit(exit); fatal {
				return fatalErr
			}
		case <-s.reloadCh:
			if err := s.replaceWorker(ctx); err != nil {
				return err
			}
		}
	}
}

// RequestReload asks the supervisor to replace the worker. The stall watchdog and
// the SIGHUP handler both call it; a request coalesces with a pending one, so a
// burst of triggers still runs at most one swap at a time.
func (s *Supervisor) RequestReload() {
	select {
	case s.reloadCh <- struct{}{}:
	default:
	}
}

// CurrentWorkerPID returns the process id of the worker currently serving, or 0 if
// none is current yet. It reads under the lock, so it is safe to call while the
// supervisor spawns and replaces workers.
func (s *Supervisor) CurrentWorkerPID() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	handle, ok := s.workers[s.currentGeneration]
	if !ok || handle.cmd.Process == nil {
		return 0
	}
	return handle.cmd.Process.Pid
}

func (s *Supervisor) currentWorkerGeneration() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentGeneration
}

// markCurrent promotes a freshly-serving generation to current and starts its
// progress grace period, so a newly booted worker is not immediately flagged as
// stalled before its reconcile loop has run once.
func (s *Supervisor) markCurrent(generation uint64) {
	s.mu.Lock()
	s.currentGeneration = generation
	s.pendingGeneration = 0
	s.state = StateSteady
	s.lastProgress = s.now()
	s.mu.Unlock()
}

// replaceWorker reloads the worker stop-old-first, so at most one worker ever
// mutates the pool. Unlike the guest, whose old worker is read-only while it
// drains, the host worker owns the pool mutations (adopt, dispatch, warm, reap),
// so two live workers would double-bind jobs onto one slot and recycle VMs out from
// under each other. It stops the old worker and waits for it to fully exit, which
// quiesces its accept loop and its reconcile, dispatch, warm, and reap loops and
// detaches (does not kill) its running-job drains, then brings up the replacement,
// which re-adopts every VM and resumes every drain from its cursor. The supervisor
// keeps the listener open throughout, so connections that arrive during the brief
// swap queue in the kernel backlog and are served by the replacement rather than
// refused. It returns a fatal error only when no replacement can be brought up
// after the old worker is gone, so the supervisor exits and launchd restarts it.
func (s *Supervisor) replaceWorker(ctx context.Context) error {
	s.mu.Lock()
	if s.state != StateSteady {
		state := s.state
		s.mu.Unlock()
		s.log.WarnContext(ctx, "host supervisor reload skipped: not steady", "state", state.String())
		return nil
	}
	s.state = StateReplacing
	oldGeneration := s.currentGeneration
	s.superseded[oldGeneration] = struct{}{}
	s.mu.Unlock()

	// Stop the old worker and block until it has fully exited, so no second worker
	// begins mutating the pool while the old one still owns it.
	s.stopAndWaitGeneration(oldGeneration)

	return s.bringUpReplacement(ctx, oldGeneration)
}

// bringUpReplacement spawns a replacement worker and promotes it once it serves. It
// retries a bounded number of times because the old worker is already gone, so the
// service has no worker until one comes up; if none does, it returns a fatal error
// so the supervisor exits and launchd restarts the whole service.
func (s *Supervisor) bringUpReplacement(ctx context.Context, oldGeneration uint64) error {
	var served uint64
	for attempt := 1; attempt <= reloadRespawnAttempts; attempt++ {
		generation, ok := s.trySpawnReplacement(ctx, attempt)
		if ok {
			served = generation
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("hostsupervisor: reload cancelled before a worker came up: %w", ctx.Err())
		case <-time.After(reloadRespawnBackoff):
		}
	}
	if served == 0 {
		err := fmt.Errorf("hostsupervisor: no replacement worker came up after %d attempts", reloadRespawnAttempts)
		s.log.ErrorContext(ctx, "host supervisor reload failed; exiting for restart", "err", err)
		return err
	}
	s.markCurrent(served)
	s.log.InfoContext(ctx, "host supervisor worker reloaded", "new_generation", served, "old_generation", oldGeneration, "new_pid", s.CurrentWorkerPID())
	return nil
}

// trySpawnReplacement makes one attempt to spawn a replacement worker and wait for
// it to serve, returning the generation and whether it served. It stops a spawned
// worker that never serves, so a failed attempt leaves no stray process. The caller
// logs the eventual success or the final give-up, so the success log stays out of
// the retry loop.
func (s *Supervisor) trySpawnReplacement(ctx context.Context, attempt int) (uint64, bool) {
	generation, readyRead, err := s.spawn(os.Environ())
	if err != nil {
		s.log.ErrorContext(ctx, "host supervisor replacement spawn failed", "err", err, "attempt", attempt)
		return 0, false
	}
	readyErr := s.waitReplacementReady(ctx, readyRead)
	_ = readyRead.Close()
	if readyErr != nil {
		s.stopAndWaitGeneration(generation)
		s.clearPending(generation)
		s.log.ErrorContext(ctx, "host supervisor replacement did not serve", "err", readyErr, "attempt", attempt, "failed_generation", generation)
		return 0, false
	}
	return generation, true
}

// clearPending drops a pending generation without changing the replacement state,
// used between reload attempts so the next spawn reserves a fresh generation while
// the supervisor stays in the replacing phase until a worker serves.
func (s *Supervisor) clearPending(generation uint64) {
	s.mu.Lock()
	if s.pendingGeneration == generation {
		s.pendingGeneration = 0
	}
	s.mu.Unlock()
}

func (s *Supervisor) waitReplacementReady(ctx context.Context, readyRead *os.File) error {
	readyCh := readyChan(readyRead)
	select {
	case err := <-readyCh:
		if err != nil {
			s.log.WarnContext(ctx, "host supervisor replacement worker readiness failed", "err", err)
		}
		return err
	case <-time.After(s.opts.ReplacementReadyTimeout):
		s.log.WarnContext(ctx, "host supervisor replacement worker readiness timed out", "timeout", s.opts.ReplacementReadyTimeout)
		return fmt.Errorf("replacement did not serve within %s", s.opts.ReplacementReadyTimeout)
	case <-ctx.Done():
		s.log.WarnContext(ctx, "host supervisor reload cancelled", "err", ctx.Err())
		return fmt.Errorf("reload cancelled: %w", ctx.Err())
	}
}

// handleWorkerExit records a worker generation's exit. A superseded generation
// draining after a reload is expected; a pending generation crashing before it
// serves reverts to Steady; the current generation exiting while steady is fatal,
// so launchd restarts the supervisor and adoption reattaches jobs.
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
		s.log.Debug("host supervisor superseded worker exited", "generation", exit.generation)
		return false, nil
	}
	if isPending {
		s.log.Warn("host supervisor replacement worker exited before serving; staying on current worker", "generation", exit.generation, "err", exit.err)
		return false, nil
	}
	if isCurrent && replacing {
		s.log.Debug("host supervisor current worker drained during replacement", "generation", exit.generation)
		return false, nil
	}
	if isCurrent {
		return true, fmt.Errorf("hostsupervisor: current worker exited: %w", workerExitErr(exit.err))
	}
	s.log.Debug("host supervisor stale worker exited", "generation", exit.generation, "err", exit.err)
	return false, nil
}

// spawn starts a new worker generation with an inherited listener, a fresh
// readiness pipe, and a fresh progress pipe. It returns the generation and the
// readiness read end so the caller can wait for the worker to serve.
func (s *Supervisor) spawn(baseEnv []string) (uint64, *os.File, error) {
	readyRead, readyWrite, err := os.Pipe()
	if err != nil {
		s.log.Error("host supervisor create readiness pipe failed", "err", err)
		return 0, nil, fmt.Errorf("hostsupervisor: create readiness pipe: %w", err)
	}
	progressRead, progressWrite, err := os.Pipe()
	if err != nil {
		_ = readyRead.Close()
		_ = readyWrite.Close()
		s.log.Error("host supervisor create progress pipe failed", "err", err)
		return 0, nil, fmt.Errorf("hostsupervisor: create progress pipe: %w", err)
	}
	executablePath, err := os.Executable()
	if err != nil {
		closeFiles(readyRead, readyWrite, progressRead, progressWrite)
		s.log.Error("host supervisor resolve executable failed", "err", err)
		return 0, nil, fmt.Errorf("hostsupervisor: resolve executable: %w", err)
	}
	generation := s.reserveGeneration()
	extraFiles, env, cleanup, err := s.buildInherit(baseEnv, generation, readyWrite, progressWrite)
	if err != nil {
		closeFiles(readyRead, readyWrite, progressRead, progressWrite)
		s.clearPending(generation)
		return 0, nil, err
	}
	spec := WorkerSpec{
		ExecutablePath: executablePath,
		Arguments:      []string{executablePath, "worker"},
		Environment:    env,
		ExtraFiles:     extraFiles,
	}
	if err := s.startWorker(generation, spec); err != nil {
		cleanup()
		closeFiles(readyRead, progressRead)
		s.clearPending(generation)
		return 0, nil, err
	}
	cleanup()
	goSafe(s.log, "progress reader", func() { s.readProgress(generation, progressRead) })
	return generation, readyRead, nil
}

// buildInherit assembles the ExtraFiles and fd environment a worker inherits: the
// listener dup, the readiness write end, and the progress write end. The returned
// cleanup closes the supervisor's copies of all three after the child forks, so
// the child owns its inherited copies and the supervisor keeps only the read ends.
func (s *Supervisor) buildInherit(baseEnv []string, generation uint64, readyWrite, progressWrite *os.File) ([]*os.File, []string, func(), error) {
	listenerFile, err := s.opts.Listener.File()
	if err != nil {
		s.log.Error("host supervisor dup listener failed", "err", err)
		return nil, nil, nil, fmt.Errorf("hostsupervisor: dup listener: %w", err)
	}
	extraFiles := []*os.File{listenerFile, readyWrite, progressWrite}
	listenerFD := firstWorkerFDBase
	readyFD := firstWorkerFDBase + 1
	progressFD := firstWorkerFDBase + 2
	overrides := map[string]string{
		EnvListenerFD: strconv.Itoa(listenerFD),
		EnvReadyFD:    strconv.Itoa(readyFD),
		EnvProgressFD: strconv.Itoa(progressFD),
		EnvGeneration: strconv.FormatUint(generation, 10),
	}
	env := envWithOverrides(baseEnv, overrides)
	cleanup := func() {
		_ = listenerFile.Close()
		_ = readyWrite.Close()
		_ = progressWrite.Close()
	}
	return extraFiles, env, cleanup, nil
}

// startWorker builds and starts a worker command, tracking its handle and a
// goroutine that reports its exit and closes the handle's done channel.
func (s *Supervisor) startWorker(generation uint64, spec WorkerSpec) error {
	build := s.opts.WorkerCommand
	if build == nil {
		build = defaultWorkerCommand
	}
	cmd := build(spec)
	if err := cmd.Start(); err != nil {
		s.log.Error("host supervisor worker start failed", "generation", generation, "err", err)
		return fmt.Errorf("hostsupervisor: start worker generation %d: %w", generation, err)
	}
	handle := &workerHandle{generation: generation, cmd: cmd, done: make(chan struct{})}
	s.mu.Lock()
	s.workers[generation] = handle
	s.mu.Unlock()
	goSafe(s.log, "worker wait", func() {
		waitErr := cmd.Wait()
		s.workerExitCh <- workerExit{generation: generation, err: waitErr}
		close(handle.done)
	})
	s.log.Info("host supervisor worker spawned", "generation", generation, "pid", cmd.Process.Pid)
	return nil
}

func defaultWorkerCommand(spec WorkerSpec) *exec.Cmd {
	return &exec.Cmd{
		Path:       spec.ExecutablePath,
		Args:       spec.Arguments,
		Env:        spec.Environment,
		ExtraFiles: spec.ExtraFiles,
		Stdout:     os.Stdout,
		Stderr:     os.Stderr,
	}
}

func (s *Supervisor) reserveGeneration() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.generationCounter++
	generation := s.generationCounter
	s.pendingGeneration = generation
	return generation
}

// readProgress consumes the worker's reconcile-progress heartbeat, stamping the
// last-progress time on every line. It closes the read end when the worker exits
// and its write end closes.
func (s *Supervisor) readProgress(_ uint64, progressRead *os.File) {
	defer func() { _ = progressRead.Close() }()
	scanner := bufio.NewScanner(progressRead)
	for scanner.Scan() {
		s.markProgress()
	}
}

func (s *Supervisor) markProgress() {
	s.mu.Lock()
	s.lastProgress = s.now()
	s.mu.Unlock()
}

// stallCheck samples reconcile progress once. When the current worker is steady
// and its reconcile loop has not stamped progress within StallTimeout, it resets
// the progress clock and requests a reload, returning true. It is the unit the
// watchdog runs on each tick and the seam a test drives with a stubbed stamp.
func (s *Supervisor) stallCheck(now time.Time) bool {
	s.mu.Lock()
	stalled := s.state == StateSteady && !s.lastProgress.IsZero() && now.Sub(s.lastProgress) >= s.opts.StallTimeout
	if stalled {
		// Reset the clock so a single stall triggers one reload rather than firing
		// again on the next tick before the replacement has stamped progress.
		s.lastProgress = now
	}
	s.mu.Unlock()
	if !stalled {
		return false
	}
	s.log.Warn("host supervisor reconcile stalled; restarting worker", "stall_timeout", s.opts.StallTimeout)
	s.RequestReload()
	return true
}

func (s *Supervisor) runWatchdog(ctx context.Context) {
	ticker := time.NewTicker(s.opts.StallCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.stallCheck(s.now())
		}
	}
}

// stopAndWaitGeneration signals a worker generation to stop and blocks until it has
// fully exited, force-killing it after the stop timeout. Blocking until the process
// is gone is what guarantees single-writer pool ownership across a reload: the next
// worker is not spawned until the old one has quiesced. The wait goroutine still
// delivers the exit to the exit channel, so the supervise loop accounts for it.
func (s *Supervisor) stopAndWaitGeneration(generation uint64) {
	if generation == 0 {
		return
	}
	s.mu.Lock()
	handle := s.workers[generation]
	s.mu.Unlock()
	if handle == nil || handle.cmd.Process == nil {
		return
	}
	_ = handle.cmd.Process.Signal(syscall.SIGTERM)
	timer := time.NewTimer(s.opts.WorkerStopTimeout)
	defer timer.Stop()
	select {
	case <-handle.done:
		return
	case <-timer.C:
		_ = handle.cmd.Process.Kill()
	}
	// A SIGKILL always reaps, so the wait goroutine closes done promptly.
	<-handle.done
}

// forgetWorker drops a generation from the worker and supersession tables, used
// when the first worker exits before it ever serves.
func (s *Supervisor) forgetWorker(generation uint64) {
	s.mu.Lock()
	delete(s.workers, generation)
	delete(s.superseded, generation)
	s.mu.Unlock()
}

// shutdownWorkers kills every remaining worker on supervisor exit, so a shutdown
// leaves no stray worker holding the listener.
func (s *Supervisor) shutdownWorkers() {
	s.mu.Lock()
	handles := make([]*workerHandle, 0, len(s.workers))
	for _, handle := range s.workers {
		handles = append(handles, handle)
	}
	s.mu.Unlock()
	for _, handle := range handles {
		if handle.cmd.Process != nil {
			_ = handle.cmd.Process.Kill()
		}
	}
}

// readyChan reads the readiness pipe in a goroutine and reports the outcome: nil
// once the worker writes the ready message and closes the pipe, or an error if the
// pipe closes first (a crashed worker) or the payload is wrong.
func readyChan(readyRead *os.File) <-chan error {
	result := make(chan error, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				result <- fmt.Errorf("hostsupervisor: readiness read panic: %v", recovered)
			}
		}()
		data, err := io.ReadAll(readyRead)
		if err != nil {
			result <- fmt.Errorf("hostsupervisor: read readiness: %w", err)
			return
		}
		if string(data) != ReadyMessage {
			result <- fmt.Errorf("hostsupervisor: readiness payload %q, want %q", string(data), ReadyMessage)
			return
		}
		result <- nil
	}()
	return result
}

func workerExitErr(err error) error {
	if err == nil {
		return errors.New("worker exited")
	}
	return err
}

func closeFiles(files ...*os.File) {
	for _, file := range files {
		if file != nil {
			_ = file.Close()
		}
	}
}

// envWithOverrides returns base with each override key replaced or appended, so a
// worker inherits the supervisor environment plus the fd wiring for its generation.
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

// goSafe runs fn in a goroutine with a deferred recover, so a panic in a
// supervisor background task is logged rather than crashing the process.
func goSafe(log *slog.Logger, label string, fn func()) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Error("host supervisor goroutine panic recovered", "label", label, "err", fmt.Errorf("panic: %v", recovered))
			}
		}()
		fn()
	}()
}
