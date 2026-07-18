package guestexec

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"goodkind.io/gha-mac-broker/internal/clock"
)

const (
	defaultRetention         = 5 * time.Minute
	defaultHeartbeatInterval = 10 * time.Second
	outputReadBufferSize     = 32 * 1024
	maximumProcessExitCode   = 1<<31 - 1
	// maxRetainedLogBytes caps the total LogChunk data an execution retains, so a
	// high-output job cannot hold unbounded memory across the retention window.
	maxRetainedLogBytes = 4 * 1024 * 1024
)

var (
	// ErrDraining means the registry no longer accepts new executions.
	ErrDraining = errors.New("guestexec: registry is draining")
	// ErrExecutionNotFound means no active or retained execution has the supplied ID.
	ErrExecutionNotFound = errors.New("guestexec: execution not found")
)

// deadlineReader is satisfied by pipe read ends (*os.File) whose read deadline
// can be set. captureOutput clears any deadline on entry so a reader reads from
// its current offset rather than tripping a stale deadline.
type deadlineReader interface {
	SetReadDeadline(time.Time) error
}

// Registry owns retained event logs, slots, and drain state. It records
// executions the caller has already spawned, captures their piped output, and
// completes them when the caller reports the process exit. It never forks or
// waits a process itself.
type Registry struct {
	mu                sync.Mutex
	executions        map[string]*execution
	slots             map[uint32]string
	retention         time.Duration
	heartbeatInterval time.Duration
	draining          bool
	activeCount       uint32
	drained           chan struct{}
	drainedClosed     bool
	clock             clock.Clock
}

type execution struct {
	mu               sync.Mutex
	id               string
	slot             uint32
	meta             JobMeta
	processID        int
	pgid             int
	phase            string
	running          bool
	reaped           bool
	cancelled        bool
	events           []Event
	retainedLogBytes int
	logCapCursor     int
	completedAt      time.Time
	exitReported     bool
	exitCode         int
	exitMsg          string
	notify           chan struct{}
	done             chan struct{}
	// exitReport carries the caller's waitpid result to the supervisor
	// goroutine. It is buffered by one and delivered at most once, so a
	// duplicate report never blocks and never drives a second completion.
	exitReport chan ExitReport
	reapOnce   sync.Once
	// stopped, when closed, releases the capture, heartbeat, and supervisor
	// goroutines at a read boundary without completing the execution. No live path
	// closes it now that the worker snapshot handoff is gone, so it stays open for
	// the execution's life.
	stopped      chan struct{}
	readers      *sync.WaitGroup
	stdoutReader io.ReadCloser
	stderrReader io.ReadCloser
	// supervised is closed when the supervisor goroutine returns after reaping the
	// execution to completion.
	supervised chan struct{}
}

// New returns an empty process registry.
func New(options Options) *Registry {
	retention := options.Retention
	if retention <= 0 {
		retention = defaultRetention
	}
	heartbeatInterval := options.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = defaultHeartbeatInterval
	}
	registry := &Registry{
		mu:                sync.Mutex{},
		executions:        make(map[string]*execution),
		slots:             make(map[uint32]string),
		retention:         retention,
		heartbeatInterval: heartbeatInterval,
		draining:          false,
		activeCount:       uint32(0),
		drained:           make(chan struct{}),
		drainedClosed:     false,
		clock:             clock.System(),
	}
	slog.Debug("guest execution registry created",
		"retention", retention,
		"heartbeat_interval", heartbeatInterval,
	)
	return registry
}

// Start launches a fresh execution or reports the idempotent outcome. It is the
// guest-agent RunJob path: it forks the process with Launch, hands the read ends
// to Register, and runs a supervisor goroutine that waits the child and reports
// its exit.
func (r *Registry) Start(spec ExecSpec) (Outcome, error) {
	if spec.ExecutionID == "" {
		return OutcomeUnspecified, fmt.Errorf("guestexec: execution ID is required")
	}
	if spec.Command == "" {
		return OutcomeUnspecified, fmt.Errorf("guestexec: command is required")
	}
	if !filepath.IsAbs(spec.Command) {
		return OutcomeUnspecified, fmt.Errorf("guestexec: command path must be absolute")
	}

	// Check admission before forking, so a draining registry or an idempotent
	// duplicate never spawns a child that would run its command before Register
	// rejected it. Register re-checks under the lock, so a concurrent race that
	// slips past this peek is still caught and the loser is discarded.
	r.mu.Lock()
	preOutcome, proceed, preErr := r.checkAdmissionLocked(spec)
	r.mu.Unlock()
	if !proceed {
		return preOutcome, preErr
	}

	launched, err := Launch(spec)
	if err != nil {
		return OutcomeUnspecified, err
	}
	outcome, registerErr := r.Register(spec, launched.PID, launched.PGID, launched.Stdout, launched.Stderr)
	if registerErr != nil || outcome != OutcomeAccepted {
		r.discardLaunched(launched)
		return outcome, registerErr
	}

	goSafe("start supervisor", func() {
		observeErr := waitUntilExited(launched.PID)
		waitErr := launched.Command.Wait()
		exitCode := int(processExitCode(launched.Command.ProcessState))
		message := ""
		if joined := errors.Join(observeErr, waitErr); joined != nil {
			message = joined.Error()
		}
		r.ReportExit(launched.PID, exitCode, message)
	})
	return OutcomeAccepted, nil
}

// CheckAdmission reports the outcome the registry would return for spec's
// execution ID and slot without launching, recording, or mutating anything. It
// lets a caller decide whether to run expensive or destructive per-slot
// preparation before Start or Register, so an idempotent duplicate of a live
// execution or a conflicting slot never triggers that work. The authoritative
// admission still happens under the lock in Register, so a concurrent race that
// slips past this peek is still caught there.
func (r *Registry) CheckAdmission(spec ExecSpec) (Outcome, error) {
	if spec.ExecutionID == "" {
		return OutcomeUnspecified, fmt.Errorf("guestexec: execution ID is required")
	}
	r.mu.Lock()
	outcome, _, err := r.checkAdmissionLocked(spec)
	r.mu.Unlock()
	return outcome, err
}

// checkAdmissionLocked reports the outcome of admitting spec and whether the
// caller should proceed to record it. It must run under r.mu. proceed is true
// only for OutcomeAccepted; every other outcome carries the reason a new
// execution is refused, matching the pre-refactor Start semantics.
func (r *Registry) checkAdmissionLocked(spec ExecSpec) (Outcome, bool, error) {
	if r.draining {
		return OutcomeUnspecified, false, ErrDraining
	}
	if existing, found := r.executions[spec.ExecutionID]; found {
		existing.mu.Lock()
		sameSlot := existing.slot == spec.Slot
		running := existing.running
		existing.mu.Unlock()
		if !sameSlot {
			return OutcomeConflict, false, nil
		}
		if running {
			return OutcomeAlreadyRunning, false, nil
		}
		return OutcomeAlreadyCompleted, false, nil
	}
	if _, occupied := r.slots[spec.Slot]; occupied {
		return OutcomeConflict, false, nil
	}
	return OutcomeAccepted, true, nil
}

// Register records a caller-spawned execution and starts capturing its output.
// It keeps the idempotency and one-execution-per-slot semantics of Start: a
// duplicate execution ID reports the existing outcome, and a taken slot
// conflicts. The caller has already forked the process in its own group and
// passes the pid, pgid, and pipe read ends; Register never opens the process.
func (r *Registry) Register(
	spec ExecSpec,
	pid int,
	pgid int,
	stdoutR io.ReadCloser,
	stderrR io.ReadCloser,
) (Outcome, error) {
	if spec.ExecutionID == "" {
		return OutcomeUnspecified, fmt.Errorf("guestexec: execution ID is required")
	}
	if spec.Command == "" {
		return OutcomeUnspecified, fmt.Errorf("guestexec: command is required")
	}
	if !filepath.IsAbs(spec.Command) {
		return OutcomeUnspecified, fmt.Errorf("guestexec: command path must be absolute")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if outcome, proceed, admitErr := r.checkAdmissionLocked(spec); !proceed {
		return outcome, admitErr
	}

	readers := &sync.WaitGroup{}
	readers.Add(2)
	execState := &execution{
		mu:               sync.Mutex{},
		id:               spec.ExecutionID,
		slot:             spec.Slot,
		meta:             spec.Meta,
		processID:        pid,
		pgid:             pgid,
		phase:            "running",
		running:          true,
		reaped:           false,
		cancelled:        false,
		events:           nil,
		retainedLogBytes: 0,
		logCapCursor:     0,
		completedAt:      time.Time{},
		exitReported:     false,
		exitCode:         0,
		exitMsg:          "",
		notify:           make(chan struct{}),
		done:             make(chan struct{}),
		exitReport:       make(chan ExitReport, 1),
		reapOnce:         sync.Once{},
		stopped:          make(chan struct{}),
		readers:          readers,
		stdoutReader:     stdoutR,
		stderrReader:     stderrR,
		supervised:       make(chan struct{}),
	}
	execState.appendEvent(PhaseChange{Phase: "running"})
	r.executions[spec.ExecutionID] = execState
	r.slots[spec.Slot] = spec.ExecutionID
	r.activeCount++

	goSafe("stdout capture", func() {
		captureOutput(execState, StreamStdout, stdoutR, readers, execState.stopped)
	})
	goSafe("stderr capture", func() {
		captureOutput(execState, StreamStderr, stderrR, readers, execState.stopped)
	})
	goSafe("execution heartbeat", func() {
		r.emitHeartbeats(execState)
	})
	goSafe("execution supervisor", func() {
		r.superviseExecution(execState)
	})
	return OutcomeAccepted, nil
}

// ReportExit delivers a waitpid result to the running execution that owns pid,
// which drains its output to EOF and emits a single terminal result. It is safe
// for an unknown, already-reaped, or expired pid: the call is a no-op and never
// panics, so a duplicate report cannot double-emit a terminal.
func (r *Registry) ReportExit(pid int, exitCode int, message string) {
	r.mu.Lock()
	execState := r.executionByPIDLocked(pid)
	r.mu.Unlock()
	if execState == nil {
		return
	}
	execState.mu.Lock()
	stale := execState.exitReported || !execState.running
	execState.mu.Unlock()
	if stale {
		return
	}
	execState.reapOnce.Do(func() {
		execState.exitReport <- ExitReport{PID: pid, ExitCode: exitCode, Message: message}
	})
}

// Subscribe replays events with Sequence greater than fromSequence, then follows live events.
func (r *Registry) Subscribe(
	executionID string,
	fromSequence uint64,
) (<-chan Event, func(), error) {
	r.mu.Lock()
	execState, found := r.executions[executionID]
	r.mu.Unlock()
	if !found {
		slog.Error("guest execution subscription not found", "err", ErrExecutionNotFound, "execution_id", executionID)
		return nil, nil, fmt.Errorf("%w: %s", ErrExecutionNotFound, executionID)
	}

	events := make(chan Event)
	done := make(chan struct{})
	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() { close(done) })
	}
	goSafe("execution subscriber", func() {
		execState.stream(fromSequence, events, done)
	})
	return events, cancel, nil
}

// Cancel kills the execution process group. Repeated cancellation is a no-op.
func (r *Registry) Cancel(executionID string) error {
	r.mu.Lock()
	execState, found := r.executions[executionID]
	r.mu.Unlock()
	if !found {
		slog.Error("guest execution cancellation not found", "err", ErrExecutionNotFound, "execution_id", executionID)
		return fmt.Errorf("%w: %s", ErrExecutionNotFound, executionID)
	}

	execState.mu.Lock()
	if !execState.running || execState.reaped || execState.cancelled {
		execState.mu.Unlock()
		return nil
	}
	pgid := execState.pgid
	err := killProcessGroup(pgid)
	if err == nil {
		execState.cancelled = true
	}
	execState.mu.Unlock()
	if err != nil {
		slog.Error("guest execution cancellation failed", "err", err, "execution_id", executionID)
		return fmt.Errorf("guestexec: cancel %s: %w", executionID, err)
	}
	return nil
}

// List returns active and retained executions sorted by execution ID.
func (r *Registry) List() []ExecutionState {
	r.mu.Lock()
	executions := make([]*execution, 0, len(r.executions))
	for _, execState := range r.executions {
		executions = append(executions, execState)
	}
	r.mu.Unlock()

	states := make([]ExecutionState, 0, len(executions))
	for _, execState := range executions {
		execState.mu.Lock()
		lastSequence := uint64(0)
		if len(execState.events) > 0 {
			lastSequence = execState.events[len(execState.events)-1].Sequence
		}
		states = append(states, ExecutionState{
			ExecutionID:  execState.id,
			Slot:         execState.slot,
			Meta:         execState.meta,
			Phase:        execState.phase,
			Running:      execState.running,
			LastSequence: lastSequence,
		})
		execState.mu.Unlock()
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].ExecutionID < states[j].ExecutionID
	})
	return states
}

// Drain refuses new executions and returns an atomic state plus an idle notification.
func (r *Registry) Drain() DrainState {
	r.mu.Lock()
	r.draining = true
	if r.activeCount == 0 {
		r.closeDrainedLocked()
	}
	state := DrainState{
		Idle:             r.activeCount == 0,
		ActiveExecutions: r.activeCount,
		Done:             r.drained,
	}
	r.mu.Unlock()
	return state
}

// discardLaunched kills and reaps a process that Start forked but Register did
// not accept, so an idempotent duplicate or a lost admission race leaves no
// running child and no leaked pipe.
func (r *Registry) discardLaunched(launched *LaunchedProcess) {
	_ = killProcessGroup(launched.PGID)
	_ = launched.Stdout.Close()
	_ = launched.Stderr.Close()
	goSafe("discarded launch reaper", func() {
		_ = waitUntilExited(launched.PID)
		_ = launched.Command.Wait()
	})
}

// executionByPIDLocked returns the execution that owns pid, preferring a live
// (running, not yet reaped) one. processID is set once at Register before the
// execution is published, so reading it under r.mu is race-free. The preference
// routes a report to the live execution when a recycled pid also matches a
// retained completed execution still inside its retention window; the reap-order
// of that live execution's state is read under its own mutex.
func (r *Registry) executionByPIDLocked(pid int) *execution {
	var fallback *execution
	for _, execState := range r.executions {
		if execState.processID != pid {
			continue
		}
		execState.mu.Lock()
		live := execState.running && !execState.reaped
		execState.mu.Unlock()
		if live {
			return execState
		}
		if fallback == nil {
			fallback = execState
		}
	}
	return fallback
}

// superviseExecution awaits the caller's exit report, then completes the
// execution. Closing stopped would release it early without completing the
// execution, though no live path does that now. Closing supervised on return
// signals that the goroutine has settled.
func (r *Registry) superviseExecution(execState *execution) {
	defer close(execState.supervised)
	select {
	case report := <-execState.exitReport:
		r.reapAndComplete(execState, report)
	case <-execState.stopped:
		return
	}
}

// reapAndComplete records the reported exit, drains the output pipes to EOF,
// completes the execution, and arms its retention timer. Draining before the
// terminal preserves the ordering that every captured LogChunk precedes the
// completion.
func (r *Registry) reapAndComplete(execState *execution, report ExitReport) {
	execState.mu.Lock()
	execState.reaped = true
	execState.exitReported = true
	execState.exitCode = report.ExitCode
	execState.exitMsg = report.Message
	readers := execState.readers
	execState.mu.Unlock()

	if readers != nil {
		readers.Wait()
	}
	r.completeExecution(execState)
	time.AfterFunc(r.retention, func() {
		r.expire(execState)
	})
}

func (r *Registry) emitHeartbeats(execState *execution) {
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			execState.appendHeartbeat(now)
		case <-execState.done:
			return
		case <-execState.stopped:
			return
		}
	}
}

func (r *Registry) expire(execState *execution) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, found := r.executions[execState.id]
	if found && current == execState {
		delete(r.executions, execState.id)
	}
}

func (r *Registry) completeExecution(execState *execution) {
	r.mu.Lock()
	execState.mu.Lock()
	message := execState.exitMsg
	if execState.cancelled {
		message = "execution cancelled"
	}
	execState.phase = "completed"
	execState.running = false
	execState.completedAt = r.clock.Now()
	execState.appendEventLocked(PhaseChange{Phase: "completed"})
	execState.appendEventLocked(TerminalResult{ExitCode: clampExitCode(execState.exitCode), Message: message})
	close(execState.done)
	delete(r.slots, execState.slot)
	r.activeCount--
	if r.draining && r.activeCount == 0 {
		r.closeDrainedLocked()
	}
	execState.mu.Unlock()
	r.mu.Unlock()
}

func (r *Registry) closeDrainedLocked() {
	if r.drainedClosed {
		return
	}
	close(r.drained)
	r.drainedClosed = true
}

func captureOutput(
	execState *execution,
	stream Stream,
	reader io.ReadCloser,
	readers *sync.WaitGroup,
	stopped <-chan struct{},
) {
	defer readers.Done()
	// Clear any past read deadline so the reader reads from its current offset
	// instead of tripping a stale deadline again.
	if dr, ok := reader.(deadlineReader); ok {
		_ = dr.SetReadDeadline(time.Time{})
	}
	buffer := make([]byte, outputReadBufferSize)
	for {
		select {
		case <-stopped:
			// Released without completing: leave the reader open.
			return
		default:
		}
		bytesRead, err := reader.Read(buffer)
		if bytesRead > 0 {
			data := append([]byte(nil), buffer[:bytesRead]...)
			execState.appendEvent(LogChunk{Stream: stream, Data: data})
		}
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				// A read deadline interrupted a blocked read without consuming bytes.
				// Leave the reader open so a later read resumes from this offset.
				return
			}
			_ = reader.Close()
			if !errors.Is(err, io.EOF) {
				execState.appendEvent(LogChunk{
					Stream: StreamStderr,
					Data:   fmt.Appendf(nil, "guestexec: capture %v: %v\n", stream, err),
				})
			}
			return
		}
	}
}

func (e *execution) appendEvent(payload EventPayload) {
	e.mu.Lock()
	e.appendEventLocked(payload)
	e.mu.Unlock()
}

func (e *execution) appendHeartbeat(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.appendEventLocked(Heartbeat{UnixNanos: now.UnixNano()})
}

func (e *execution) appendEventLocked(payload EventPayload) {
	sequence := uint64(len(e.events) + 1)
	e.events = append(e.events, Event{Sequence: sequence, Payload: payload})
	if chunk, ok := payload.(LogChunk); ok {
		e.retainedLogBytes += len(chunk.Data)
		e.enforceLogByteCapLocked()
	}
	close(e.notify)
	e.notify = make(chan struct{})
}

// enforceLogByteCapLocked truncates the data of the oldest retained LogChunk
// events until the total retained log bytes fall back under the cap. It keeps
// every Event in place, so Sequence numbers stay contiguous and replay from a
// cursor still resolves each sequence to its slot, and it never touches
// PhaseChange, Heartbeat, or TerminalResult events. Each captured chunk is at
// most outputReadBufferSize, well under the cap, so evicting whole oldest chunks
// always reaches the target without splitting the newest chunk.
//
// The scan resumes from logCapCursor rather than rescanning e.events from zero
// on every append, so it stays O(1) amortized and does not hold e.mu across an
// O(n) walk that would stall the capture goroutines draining the pipes. A
// forward-only cursor is correct because events are only appended (never
// removed) and a LogChunk payload is only ever zeroed (never un-zeroed), so no
// entry before the cursor can require eviction again: every entry the cursor
// passed was either a non-LogChunk event or a LogChunk this method already
// zeroed, and the early return leaves the cursor on the oldest live chunk.
func (e *execution) enforceLogByteCapLocked() {
	for ; e.logCapCursor < len(e.events); e.logCapCursor++ {
		if e.retainedLogBytes <= maxRetainedLogBytes {
			return
		}
		chunk, ok := e.events[e.logCapCursor].Payload.(LogChunk)
		if !ok || len(chunk.Data) == 0 {
			continue
		}
		e.retainedLogBytes -= len(chunk.Data)
		e.events[e.logCapCursor].Payload = LogChunk{Stream: chunk.Stream, Data: nil}
	}
}

func mergedEnvironment(overrides map[string]string) []string {
	environment := append([]string(nil), os.Environ()...)
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		environment = append(environment, key+"="+overrides[key])
	}
	return environment
}

func processExitCode(processState *os.ProcessState) int32 {
	exitCode := processState.ExitCode()
	if exitCode < 0 || exitCode > maximumProcessExitCode {
		return -1
	}
	return int32(exitCode)
}

// clampExitCode narrows a reported exit code to the TerminalResult range, so an
// out-of-range or signal-derived value becomes -1 rather than overflowing.
func clampExitCode(exitCode int) int32 {
	if exitCode < 0 || exitCode > maximumProcessExitCode {
		return -1
	}
	return int32(exitCode)
}

func goSafe(label string, function func()) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error(label+" panic recovered", "err", fmt.Errorf("panic: %v", recovered))
			}
		}()
		function()
	}()
}

func (e *execution) stream(fromSequence uint64, output chan<- Event, done <-chan struct{}) {
	defer close(output)
	nextSequence := fromSequence + 1
	for {
		e.mu.Lock()
		if nextSequence > 0 && nextSequence <= uint64(len(e.events)) {
			event := cloneEvent(e.events[nextSequence-1])
			e.mu.Unlock()
			select {
			case output <- event:
				nextSequence++
			case <-done:
				return
			}
			continue
		}
		terminal := !e.running
		notify := e.notify
		e.mu.Unlock()
		if terminal {
			return
		}
		select {
		case <-notify:
		case <-done:
			return
		}
	}
}

func cloneEvent(event Event) Event {
	chunk, ok := event.Payload.(LogChunk)
	if ok {
		chunk.Data = append([]byte(nil), chunk.Data...)
		event.Payload = chunk
	}
	return event
}
