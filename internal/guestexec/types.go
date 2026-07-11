// Package guestexec owns guest job processes independently of client connections.
package guestexec

import "time"

// Outcome describes how Registry.Start handled an execution request.
type Outcome uint8

const (
	// OutcomeUnspecified means no execution outcome was produced.
	OutcomeUnspecified Outcome = iota
	// OutcomeAccepted means a new process was launched.
	OutcomeAccepted
	// OutcomeAlreadyRunning means the execution ID already owns a running process.
	OutcomeAlreadyRunning
	// OutcomeAlreadyCompleted means the execution ID has a retained terminal result.
	OutcomeAlreadyCompleted
	// OutcomeConflict means the execution ID or slot conflicts with another execution.
	OutcomeConflict
)

// String returns the contract name for an Outcome.
func (o Outcome) String() string {
	switch o {
	case OutcomeUnspecified:
		return "OUTCOME_UNSPECIFIED"
	case OutcomeAccepted:
		return "ACCEPTED"
	case OutcomeAlreadyRunning:
		return "ALREADY_RUNNING"
	case OutcomeAlreadyCompleted:
		return "ALREADY_COMPLETED"
	case OutcomeConflict:
		return "CONFLICT"
	default:
		return "OUTCOME_UNSPECIFIED"
	}
}

// Stream identifies a captured process output stream.
type Stream uint8

const (
	// StreamUnspecified means no output stream was selected.
	StreamUnspecified Stream = iota
	// StreamStdout identifies standard output.
	StreamStdout
	// StreamStderr identifies standard error.
	StreamStderr
)

// ExecSpec identifies an execution and describes how to launch its process.
type ExecSpec struct {
	ExecutionID string
	Slot        uint32
	Meta        JobMeta
	Command     string
	Args        []string
	Env         map[string]string
	WorkingDir  string
}

// JobMeta identifies the host job associated with an execution.
type JobMeta struct {
	Repo       string
	JobID      int64
	RunID      int64
	RunnerName string
}

// Options configures a Registry.
type Options struct {
	Retention         time.Duration
	HeartbeatInterval time.Duration
}

// Event is one sequenced execution status update.
type Event struct {
	Sequence uint64
	Payload  EventPayload
}

// EventPayload is implemented by each supported event variant.
type EventPayload interface {
	isEventPayload()
}

// PhaseChange reports a lifecycle phase.
type PhaseChange struct {
	Phase string
}

func (PhaseChange) isEventPayload() {}

// LogChunk contains bytes captured from one process stream.
type LogChunk struct {
	Stream Stream
	Data   []byte
}

func (LogChunk) isEventPayload() {}

// Heartbeat reports that an execution is still running.
type Heartbeat struct {
	UnixNanos int64
}

func (Heartbeat) isEventPayload() {}

// TerminalResult reports the final child process result.
type TerminalResult struct {
	ExitCode int32
	Message  string
}

func (TerminalResult) isEventPayload() {}

// ExecutionState is the reconnect snapshot for an active or retained execution.
type ExecutionState struct {
	ExecutionID  string
	Slot         uint32
	Meta         JobMeta
	Phase        string
	Running      bool
	LastSequence uint64
}

// DrainState is the atomic registry snapshot returned when draining begins.
type DrainState struct {
	Idle             bool
	ActiveExecutions uint32
	Done             <-chan struct{}
}

var _, _ = configureProcessGroup, waitUntilExited
