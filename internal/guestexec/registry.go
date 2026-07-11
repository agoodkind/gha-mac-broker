package guestexec

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"time"
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

// Registry owns process lifetimes, retained event logs, slots, and drain state.
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
}

type execution struct {
	mu               sync.Mutex
	id               string
	slot             uint32
	meta             JobMeta
	command          *exec.Cmd
	processID        int
	phase            string
	running          bool
	reaped           bool
	cancelled        bool
	events           []Event
	retainedLogBytes int
	logCapCursor     int
	notify           chan struct{}
	done             chan struct{}
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
	}
	slog.Debug("guest execution registry created",
		"retention", retention,
		"heartbeat_interval", heartbeatInterval,
	)
	return registry
}

// Start launches a fresh execution or reports the idempotent outcome.
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

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.draining {
		return OutcomeUnspecified, ErrDraining
	}
	if existing, found := r.executions[spec.ExecutionID]; found {
		existing.mu.Lock()
		sameSlot := existing.slot == spec.Slot
		running := existing.running
		existing.mu.Unlock()
		if !sameSlot {
			return OutcomeConflict, nil
		}
		if running {
			return OutcomeAlreadyRunning, nil
		}
		return OutcomeAlreadyCompleted, nil
	}
	if _, occupied := r.slots[spec.Slot]; occupied {
		return OutcomeConflict, nil
	}

	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		slog.Error("guest execution stdout pipe creation failed", "err", err)
		return OutcomeUnspecified, fmt.Errorf("guestexec: create stdout pipe: %w", err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		slog.Error("guest execution stderr pipe creation failed", "err", err)
		return OutcomeUnspecified, fmt.Errorf("guestexec: create stderr pipe: %w", err)
	}
	closePipes := func() {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		_ = stderrReader.Close()
		_ = stderrWriter.Close()
	}

	command := new(exec.Cmd)
	command.Path = spec.Command
	command.Args = append([]string{spec.Command}, spec.Args...)
	command.Dir = spec.WorkingDir
	command.Env = mergedEnvironment(spec.Env)
	command.Stdout = stdoutWriter
	command.Stderr = stderrWriter
	configureProcessGroup(command)
	if err := command.Start(); err != nil {
		closePipes()
		slog.Error("guest execution process start failed", "err", err, "execution_id", spec.ExecutionID)
		return OutcomeUnspecified, fmt.Errorf("guestexec: start %q: %w", spec.Command, err)
	}
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()

	execState := &execution{
		mu:               sync.Mutex{},
		id:               spec.ExecutionID,
		slot:             spec.Slot,
		meta:             spec.Meta,
		command:          command,
		processID:        command.Process.Pid,
		phase:            "running",
		running:          true,
		reaped:           false,
		cancelled:        false,
		events:           nil,
		retainedLogBytes: 0,
		logCapCursor:     0,
		notify:           make(chan struct{}),
		done:             make(chan struct{}),
	}
	execState.appendEvent(PhaseChange{Phase: "running"})
	r.executions[spec.ExecutionID] = execState
	r.slots[spec.Slot] = spec.ExecutionID
	r.activeCount++

	goSafe("execution reaper", func() {
		r.runExecution(execState, stdoutReader, stderrReader)
	})
	goSafe("execution heartbeat", func() {
		r.emitHeartbeats(execState)
	})
	return OutcomeAccepted, nil
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

func (r *Registry) runExecution(
	execState *execution,
	stdoutReader *os.File,
	stderrReader *os.File,
) {
	var readers sync.WaitGroup
	readers.Add(2)
	goSafe("stdout capture", func() {
		captureOutput(execState, StreamStdout, stdoutReader, &readers)
	})
	goSafe("stderr capture", func() {
		captureOutput(execState, StreamStderr, stderrReader, &readers)
	})

	observeErr := waitUntilExited(execState.processID)
	// command.Wait must not run under execState.mu. On the platform where
	// waitUntilExited is a non-reaping stub, Wait blocks until the child exits,
	// and the child does not exit until its stdout/stderr pipes drain. The
	// capture goroutines need execState.mu to append log events, so holding the
	// mutex across Wait would stall the drain and deadlock on large output.
	waitErr := execState.command.Wait()
	execState.mu.Lock()
	execState.reaped = true
	execState.mu.Unlock()
	waitErr = errors.Join(observeErr, waitErr)
	readers.Wait()
	exitCode := processExitCode(execState.command.ProcessState)
	r.completeExecution(execState, exitCode, waitErr)

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

func (r *Registry) completeExecution(execState *execution, exitCode int32, waitErr error) {
	r.mu.Lock()
	execState.mu.Lock()
	message := ""
	if execState.cancelled {
		message = "execution cancelled"
	} else if waitErr != nil {
		message = waitErr.Error()
	}
	execState.phase = "completed"
	execState.running = false
	execState.appendEventLocked(PhaseChange{Phase: "completed"})
	execState.appendEventLocked(TerminalResult{ExitCode: exitCode, Message: message})
	close(execState.done)
	delete(r.slots, execState.slot)
	r.activeCount--
	execState.mu.Unlock()
	r.mu.Unlock()
}

func captureOutput(
	execState *execution,
	stream Stream,
	reader *os.File,
	readers *sync.WaitGroup,
) {
	defer readers.Done()
	defer func() { _ = reader.Close() }()
	buffer := make([]byte, outputReadBufferSize)
	for {
		bytesRead, err := reader.Read(buffer)
		if bytesRead > 0 {
			data := append([]byte(nil), buffer[:bytesRead]...)
			execState.appendEvent(LogChunk{Stream: stream, Data: data})
		}
		if err != nil {
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
