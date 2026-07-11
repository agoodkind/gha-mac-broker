package guestexec

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// PipeReadEnds carries the read halves of a launched process's captured pipes.
// The supervisor forks the process, keeps the write ends attached to the child,
// and hands the read ends to Restore so a new worker generation can resume
// draining them. PR3 passes the same pipe read ends across the reload; the
// registry only needs the read side.
type PipeReadEnds struct {
	Stdout io.ReadCloser
	Stderr io.ReadCloser
}

// ExitReport is one waitpid result delivered from the caller to the registry.
// PID identifies the reaped child; ExitCode and Message describe how it ended.
// Restore consumes a stream of these so a running execution completes after a
// reload even though a different generation reaped the child.
type ExitReport struct {
	PID      int
	ExitCode int
	Message  string
}

// Snapshot is a registry frozen for handoff to a new worker generation. It
// carries the registry-level retention settings and drain state plus one
// ExecutionSnapshot per active or retained execution.
type Snapshot struct {
	Draining          bool
	Retention         time.Duration
	HeartbeatInterval time.Duration
	Executions        []ExecutionSnapshot
}

// ExecutionSnapshot is one execution's serialized state. Events is the full
// retained event slice with the nil-ness of evicted LogChunk.Data preserved: an
// evicted chunk stays nil and is not re-materialized.
type ExecutionSnapshot struct {
	ExecutionID      string
	Slot             uint32
	Meta             JobMeta
	Phase            string
	Running          bool
	Cancelled        bool
	Reaped           bool
	PID              int
	PGID             int
	CompletedAt      time.Time
	ExitReported     bool
	ExitCode         int
	ExitMsg          string
	RetainedLogBytes int
	LogCapCursor     int
	Events           []Event
}

// RestoreOption adjusts a registry rebuilt by Restore. PR3 uses it to inject the
// pieces a live supervisor supplies; PR2 defines the shape so the signature is
// stable.
type RestoreOption func(*Registry)

// Freeze applies a strict append barrier, then serializes the registry. For each
// running execution it stops the capture goroutines only after they finish their
// in-flight read-and-append, so no bytes are lost or duplicated and the sequence
// stays contiguous. It then copies each execution's fields and full event slice,
// preserving evicted (nil) LogChunk data as nil.
func (r *Registry) Freeze() (*Snapshot, error) {
	r.mu.Lock()
	snapshot := &Snapshot{
		Draining:          r.draining,
		Retention:         r.retention,
		HeartbeatInterval: r.heartbeatInterval,
		Executions:        nil,
	}
	executions := make([]*execution, 0, len(r.executions))
	for _, execState := range r.executions {
		executions = append(executions, execState)
	}
	r.mu.Unlock()

	sort.Slice(executions, func(i, j int) bool {
		return executions[i].id < executions[j].id
	})

	snapshot.Executions = make([]ExecutionSnapshot, 0, len(executions))
	for _, execState := range executions {
		execState.mu.Lock()
		running := execState.running
		execState.mu.Unlock()
		if running {
			// Completion barrier: stop the capture goroutines, then wait for the
			// supervisor to settle. If a reap was already in flight, its
			// readers.Wait now returns because the captures stopped, so it runs
			// through completeExecution before supervised closes. The snapshot is
			// then of an execution that is either cleanly running (no exit
			// observed) or fully completed (its terminal already in events), never
			// mid-reap. An idle supervisor settles at once through stopped, so this
			// never waits on an exit that has not arrived.
			freezeCapture(execState)
			<-execState.supervised
		}
		execState.mu.Lock()
		snapshot.Executions = append(snapshot.Executions, snapshotExecutionLocked(execState))
		execState.mu.Unlock()
	}
	return snapshot, nil
}

// freezeCapture is the append barrier for one running execution. Closing stopped
// releases the capture, heartbeat, and supervisor goroutines at a boundary. A
// past read deadline unblocks any in-flight Read without consuming bytes, so a
// blocked capture goroutine finishes its current read-and-append and stops.
// readers.Wait then guarantees every appended event is visible before the caller
// serializes.
func freezeCapture(execState *execution) {
	close(execState.stopped)
	execState.mu.Lock()
	readers := execState.readers
	stdoutReader := execState.stdoutReader
	stderrReader := execState.stderrReader
	execState.mu.Unlock()

	setPastReadDeadline(stdoutReader)
	setPastReadDeadline(stderrReader)
	if readers != nil {
		readers.Wait()
	}
}

// freezeReadDeadline is a fixed past instant. Setting it as a read deadline
// makes any blocked capture Read return immediately without consuming bytes, so
// the goroutine stops at a boundary and the next generation resumes from the
// same offset. Restore clears it with the zero time before reading again.
var freezeReadDeadline = time.Unix(1, 0)

func setPastReadDeadline(reader io.ReadCloser) {
	if reader == nil {
		return
	}
	if dr, ok := reader.(deadlineReader); ok {
		_ = dr.SetReadDeadline(freezeReadDeadline)
	}
}

// snapshotExecutionLocked copies one execution under its mutex. cloneEvent deep
// copies each event and preserves a nil LogChunk.Data as nil, so an evicted
// chunk round-trips as evicted.
func snapshotExecutionLocked(e *execution) ExecutionSnapshot {
	events := make([]Event, len(e.events))
	for index := range e.events {
		events[index] = cloneEvent(e.events[index])
	}
	return ExecutionSnapshot{
		ExecutionID:      e.id,
		Slot:             e.slot,
		Meta:             e.meta,
		Phase:            e.phase,
		Running:          e.running,
		Cancelled:        e.cancelled,
		Reaped:           e.reaped,
		PID:              e.processID,
		PGID:             e.pgid,
		CompletedAt:      e.completedAt,
		ExitReported:     e.exitReported,
		ExitCode:         e.exitCode,
		ExitMsg:          e.exitMsg,
		RetainedLogBytes: e.retainedLogBytes,
		LogCapCursor:     e.logCapCursor,
		Events:           events,
	}
}

// Restore rebuilds registry r in place from a snapshot. Callers pass an empty
// registry from New, which supplies the clock. Restore recomputes the active
// count and slots from the running executions, continues each execution's
// sequence from len(events) so the next emitted event has no gap and no
// duplicate, re-attaches the provided read ends and resumes capture for
// still-running executions, and re-arms each completed execution's retention
// timer from its recorded completedAt rather than from now. A shared exitReports
// channel delivers waitpid results to running executions after the reload. It is
// a method, not a package function, so it stays reachable alongside Freeze.
func (r *Registry) Restore(
	snapshot *Snapshot,
	pipeFDs map[int]PipeReadEnds,
	exitReports <-chan ExitReport,
	opts ...RestoreOption,
) error {
	if snapshot == nil {
		return fmt.Errorf("guestexec: snapshot is required")
	}
	retention := snapshot.Retention
	if retention <= 0 {
		retention = defaultRetention
	}
	heartbeatInterval := snapshot.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = defaultHeartbeatInterval
	}
	r.mu.Lock()
	r.retention = retention
	r.heartbeatInterval = heartbeatInterval
	r.draining = snapshot.Draining
	r.mu.Unlock()
	for _, opt := range opts {
		opt(r)
	}

	now := r.clock.Now()
	for index := range snapshot.Executions {
		source := &snapshot.Executions[index]
		r.restoreExecution(source, pipeFDs, now)
	}

	if exitReports != nil {
		goSafe("restore exit dispatcher", func() {
			for report := range exitReports {
				r.ReportExit(report.PID, report.ExitCode, report.Message)
			}
		})
	}
	return nil
}

// restoreExecution rebuilds one execution into the registry. A running execution
// re-attaches its read ends and resumes capture, heartbeat, and the supervisor
// awaiting a fresh exit report. A completed execution closes done as the live
// completion path does and re-arms retention from the remaining window measured
// from completedAt. The registry maps are mutated under r.mu before any goroutine
// starts, so a resumed supervisor completing early cannot race the insert.
func (r *Registry) restoreExecution(
	source *ExecutionSnapshot,
	pipeFDs map[int]PipeReadEnds,
	now time.Time,
) {
	execState := &execution{
		mu:               sync.Mutex{},
		id:               source.ExecutionID,
		slot:             source.Slot,
		meta:             source.Meta,
		processID:        source.PID,
		pgid:             source.PGID,
		phase:            source.Phase,
		running:          source.Running,
		reaped:           source.Reaped,
		cancelled:        source.Cancelled,
		events:           append([]Event(nil), source.Events...),
		retainedLogBytes: source.RetainedLogBytes,
		logCapCursor:     source.LogCapCursor,
		completedAt:      source.CompletedAt,
		exitReported:     source.ExitReported,
		exitCode:         source.ExitCode,
		exitMsg:          source.ExitMsg,
		notify:           make(chan struct{}),
		done:             make(chan struct{}),
		exitReport:       make(chan ExitReport, 1),
		reapOnce:         sync.Once{},
		stopped:          make(chan struct{}),
		readers:          nil,
		stdoutReader:     nil,
		stderrReader:     nil,
		supervised:       make(chan struct{}),
	}

	if !execState.running {
		close(execState.done)
		r.mu.Lock()
		r.executions[execState.id] = execState
		r.mu.Unlock()
		remaining := max(r.retention-now.Sub(execState.completedAt), 0)
		time.AfterFunc(remaining, func() {
			r.expire(execState)
		})
		return
	}

	ends := pipeFDs[execState.processID]
	readers := &sync.WaitGroup{}
	execState.readers = readers
	execState.stdoutReader = ends.Stdout
	execState.stderrReader = ends.Stderr

	r.mu.Lock()
	r.executions[execState.id] = execState
	r.slots[execState.slot] = execState.id
	r.activeCount++
	r.mu.Unlock()

	if ends.Stdout != nil {
		readers.Add(1)
		goSafe("stdout capture", func() {
			captureOutput(execState, StreamStdout, ends.Stdout, readers, execState.stopped)
		})
	}
	if ends.Stderr != nil {
		readers.Add(1)
		goSafe("stderr capture", func() {
			captureOutput(execState, StreamStderr, ends.Stderr, readers, execState.stopped)
		})
	}
	goSafe("execution heartbeat", func() {
		r.emitHeartbeats(execState)
	})
	goSafe("execution supervisor", func() {
		r.superviseExecution(execState)
	})
}
