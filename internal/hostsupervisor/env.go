//go:build unix

// Package hostsupervisor is the durable, launchd-owned parent of the host broker
// worker. It owns the webhook, capacity, and status TCP listener, spawns and
// replaces the swappable worker over an inherited listener descriptor so a worker
// swap never drops the listener, reads a reconcile-progress heartbeat the worker
// stamps, and restarts the worker when the reconcile loop stalls. The worker owns
// the queue, routing, VM lifecycle, and the guest-agent clients; a worker restart
// is non-destructive because running jobs are guest-owned and re-adopted after the
// worker comes back.
package hostsupervisor

// Environment variable names carry the inherited descriptors and generation from
// the supervisor to a freshly spawned worker. A worker reads these at startup to
// rebuild its listener, signal readiness, and stamp reconcile progress.
const (
	// EnvListenerFD is the inherited TCP listener file descriptor number.
	EnvListenerFD = "GHA_HOST_LISTENER_FD"
	// EnvReadyFD is the readiness pipe write end a worker signals once serving.
	EnvReadyFD = "GHA_HOST_READY_FD"
	// EnvProgressFD is the reconcile-progress pipe write end a worker stamps each
	// time its reconcile loop advances, so the supervisor can detect a stall.
	EnvProgressFD = "GHA_HOST_PROGRESS_FD"
	// EnvGeneration is the monotone worker generation the supervisor assigned.
	EnvGeneration = "GHA_HOST_GENERATION"
)

// ReadyMessage is the exact payload a worker writes to the readiness pipe once it
// is serving, so the supervisor distinguishes a ready signal from a crashed worker
// whose pipe simply closed. The worker and supervisor share this constant so the
// signal and its check cannot drift.
const ReadyMessage = "ready\n"

// State is the supervisor's worker-replacement phase. It rejects a reload unless
// Steady, so only one worker swap runs at a time.
type State int

const (
	// StateBooting is the initial phase before the first worker signals ready.
	StateBooting State = iota
	// StateSteady means one worker is current and serving; a reload is allowed.
	StateSteady
	// StateReplacing means a replacement worker was spawned and has not signaled ready.
	StateReplacing
)

// String names a State for logs.
func (s State) String() string {
	switch s {
	case StateBooting:
		return "booting"
	case StateSteady:
		return "steady"
	case StateReplacing:
		return "replacing"
	default:
		return "unknown"
	}
}
