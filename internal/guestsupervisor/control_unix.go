//go:build unix

// Package guestsupervisor is the durable, launchd-owned parent of the in-VM
// runner children. It forks each runner in its own process group, holds the
// runner pipe read ends open for the child's whole life, runs the waitpid loop,
// owns the Connect TCP listener, and spawns and replaces the swappable
// guest-worker. macOS has no child subreaper, so whichever process must wait a
// runner to learn its exit code has to stay that runner's direct parent; the
// supervisor is that parent, and the worker holds only the volatile registry.
package guestsupervisor

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"syscall"
	"time"

	"goodkind.io/gha-mac-broker/internal/clock"
	"goodkind.io/gha-mac-broker/internal/guestexec"
)

// Environment variable names carry the inherited file descriptors and control
// wiring from the supervisor to a freshly spawned worker. A worker parses these
// at startup to rebuild its listener, registry, and pipe attachments.
const (
	// EnvControlSocket is the supervisor control socket path a worker dials.
	EnvControlSocket = "GHA_GUEST_CONTROL_SOCKET"
	// EnvListenerFD is the inherited TCP listener file descriptor number.
	EnvListenerFD = "GHA_GUEST_LISTENER_FD"
	// EnvPipeFDs is the JSON pid-to-pipe-fd table for restored running children.
	EnvPipeFDs = "GHA_GUEST_PIPE_FDS"
	// EnvSnapshotFD is the frozen-registry snapshot file descriptor, empty on first boot.
	EnvSnapshotFD = "GHA_GUEST_SNAPSHOT_FD"
	// EnvReadyFD is the readiness pipe write end a worker signals once serving.
	EnvReadyFD = "GHA_GUEST_READY_FD"
	// EnvGeneration is the monotone worker generation the supervisor assigned.
	EnvGeneration = "GHA_GUEST_GENERATION"
	// EnvSlots is the configured guest execution slot count.
	EnvSlots = "GHA_GUEST_SLOTS"
	// EnvToken is the boot-scoped bearer token shared with the RPC transport.
	EnvToken = "GHA_GUEST_TOKEN" // #nosec G101 -- environment variable name only.
	// EnvGoldenFingerprint carries the baked golden fingerprint the supervisor
	// read at startup, so the worker reports it via Hello.
	EnvGoldenFingerprint = "GHA_GUEST_GOLDEN_FINGERPRINT"
)

const (
	opAttach        = "attach"
	opStartChild    = "start_child"
	opReplaceWorker = "replace_worker"
	opPollExits     = "poll_exits"
	opAckExit       = "ack_exit"
	opStatus        = "status"
)

const (
	// controlLengthPrefixBytes is the fixed big-endian length header before a
	// control frame body, so the receiver knows the exact body size.
	controlLengthPrefixBytes = 4
	// maxControlFrameBytes bounds one control frame body, so a corrupt or
	// oversized length prefix cannot force a huge allocation on the receiver.
	maxControlFrameBytes uint32 = 1 << 20
	// maxControlFDs bounds the ancillary descriptor buffer. A start_child reply
	// carries two pipe read ends and a replace_worker request carries the
	// snapshot plus the readiness write end, so a small cap covers every op.
	maxControlFDs = 64
)

// State is the supervisor's worker-replacement phase. It rejects a
// replace_worker unless Steady, so only one swap runs at a time.
type State int

const (
	// StateBooting is the initial phase before the first worker attaches.
	StateBooting State = iota
	// StateSteady means one worker is current and serving; replacement is allowed.
	StateSteady
	// StateReplacing means a replacement worker was spawned and has not attached.
	StateReplacing
)

// String names a State for logs and status responses.
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

// LaunchedChild is a runner the supervisor forked, described for the worker that
// registers it. Stdout and Stderr are the worker's private dup of the read ends;
// the supervisor keeps its own durable read ends open for the child's whole life.
type LaunchedChild struct {
	PID    int
	PGID   int
	Stdout *os.File
	Stderr *os.File
}

// SupervisorStatus reports the supervisor identity and phase over the control socket.
type SupervisorStatus struct {
	PID        int
	State      State
	Generation uint64
}

type childSpecWire struct {
	ExecutionID string            `json:"execution_id"`
	Slot        uint32            `json:"slot"`
	Command     string            `json:"command"`
	Args        []string          `json:"args,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
	WorkingDir  string            `json:"working_dir,omitempty"`
	Meta        jobMetaWire       `json:"meta"`
}

type jobMetaWire struct {
	Repo       string `json:"repo,omitempty"`
	JobID      int64  `json:"job_id,omitempty"`
	RunID      int64  `json:"run_id,omitempty"`
	RunnerName string `json:"runner_name,omitempty"`
}

type pipeFDWire struct {
	PID    int `json:"pid"`
	Stdout int `json:"stdout"`
	Stderr int `json:"stderr"`
}

type exitWire struct {
	PID      int    `json:"pid"`
	ExitCode int    `json:"exit_code"`
	Message  string `json:"message,omitempty"`
}

type controlRequest struct {
	Op                string         `json:"op"`
	Generation        uint64         `json:"generation,omitempty"`
	Spec              *childSpecWire `json:"spec,omitempty"`
	AckPID            int            `json:"ack_pid,omitempty"`
	ExecutablePath    string         `json:"executable_path,omitempty"`
	Arguments         []string       `json:"arguments,omitempty"`
	Environment       []string       `json:"environment,omitempty"`
	PollTimeoutMillis int64          `json:"poll_timeout_ms,omitempty"`
}

type controlResponse struct {
	Error         string     `json:"error,omitempty"`
	PID           int        `json:"pid,omitempty"`
	PGID          int        `json:"pgid,omitempty"`
	NewPID        int        `json:"new_pid,omitempty"`
	Exits         []exitWire `json:"exits,omitempty"`
	SupervisorPID int        `json:"supervisor_pid,omitempty"`
	State         string     `json:"state,omitempty"`
	Generation    uint64     `json:"generation,omitempty"`
}

// writeFrame sends one length-prefixed control frame, attaching files as
// SCM_RIGHTS ancillary data to the length prefix. A stream unix socket
// short-writes once a single message exceeds the send buffer, so the descriptors
// ride the fixed-size prefix and the body follows on a plain Write that flushes
// every byte.
func writeFrame(conn *net.UnixConn, payload []byte, files []*os.File) error {
	if len(payload) > int(maxControlFrameBytes) {
		slog.Warn("guest supervisor control frame too large", "bytes", len(payload), "limit", maxControlFrameBytes)
		return fmt.Errorf("guestsupervisor: control frame %d bytes exceeds limit %d", len(payload), maxControlFrameBytes)
	}
	var header [controlLengthPrefixBytes]byte
	// len(payload) is bounded above by maxControlFrameBytes, so the conversion
	// cannot overflow a uint32.
	binary.BigEndian.PutUint32(header[:], uint32(len(payload))) // #nosec G115 -- bounded by maxControlFrameBytes
	rights := unixRights(files)
	written, _, err := conn.WriteMsgUnix(header[:], rights, nil)
	if err != nil {
		return fmt.Errorf("guestsupervisor: write control header: %w", err)
	}
	if written != len(header) {
		return fmt.Errorf("guestsupervisor: write control header: short write %d of %d", written, len(header))
	}
	if len(payload) == 0 {
		return nil
	}
	bodyWritten, err := conn.Write(payload)
	if err != nil {
		return fmt.Errorf("guestsupervisor: write control body: %w", err)
	}
	if bodyWritten != len(payload) {
		return fmt.Errorf("guestsupervisor: write control body: short write %d of %d", bodyWritten, len(payload))
	}
	return nil
}

// readFrame reads one length-prefixed control frame plus any SCM_RIGHTS files
// attached to the prefix. A truncated control message means the ancillary buffer
// was too small for the sender's descriptors, so the received set is incomplete
// and the frame is rejected rather than acted on with missing file descriptors.
func readFrame(conn *net.UnixConn) ([]byte, []*os.File, error) {
	var header [controlLengthPrefixBytes]byte
	oob := make([]byte, syscall.CmsgSpace(maxControlFDs*4))
	headerRead, oobRead, recvFlags, _, err := conn.ReadMsgUnix(header[:], oob)
	if err != nil {
		return nil, nil, fmt.Errorf("guestsupervisor: read control header: %w", err)
	}
	if headerRead == 0 {
		return nil, nil, fmt.Errorf("guestsupervisor: read control header: %w", io.ErrUnexpectedEOF)
	}
	files, err := filesFromRights(oob[:oobRead])
	if err != nil {
		return nil, nil, err
	}
	if recvFlags&syscall.MSG_CTRUNC != 0 {
		closeFiles(files)
		err := fmt.Errorf("guestsupervisor: read control header: ancillary message truncated")
		slog.Warn("guest supervisor control ancillary truncated", "err", err)
		return nil, nil, err
	}
	if headerRead < len(header) {
		if _, err := io.ReadFull(conn, header[headerRead:]); err != nil {
			closeFiles(files)
			return nil, nil, fmt.Errorf("guestsupervisor: read control header remainder: %w", err)
		}
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 {
		return nil, files, nil
	}
	if length > maxControlFrameBytes {
		closeFiles(files)
		return nil, nil, fmt.Errorf("guestsupervisor: control frame %d bytes exceeds limit %d", length, maxControlFrameBytes)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(conn, body); err != nil {
		closeFiles(files)
		return nil, nil, fmt.Errorf("guestsupervisor: read control body: %w", err)
	}
	return body, files, nil
}

func unixRights(files []*os.File) []byte {
	descriptors := make([]int, 0, len(files))
	for _, file := range files {
		if file == nil {
			continue
		}
		descriptors = append(descriptors, int(file.Fd()))
	}
	if len(descriptors) == 0 {
		return nil
	}
	return syscall.UnixRights(descriptors...)
}

func filesFromRights(oob []byte) ([]*os.File, error) {
	if len(oob) == 0 {
		return nil, nil
	}
	messages, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		slog.Warn("guest supervisor parse control messages failed", "err", err)
		return nil, fmt.Errorf("guestsupervisor: parse control messages: %w", err)
	}
	files := make([]*os.File, 0)
	for index := range messages {
		descriptors, err := syscall.ParseUnixRights(&messages[index])
		if err != nil {
			if errors.Is(err, syscall.EINVAL) {
				continue
			}
			closeFiles(files)
			return nil, fmt.Errorf("guestsupervisor: parse control descriptors: %w", err)
		}
		for _, descriptor := range descriptors {
			if descriptor < 0 {
				continue
			}
			// A descriptor received over SCM_RIGHTS arrives in blocking mode, and
			// os.NewFile only registers a blocking descriptor with the runtime
			// poller when it is first set non-blocking. Without the poller a pipe
			// read end cannot honor a read deadline, which the freeze barrier needs
			// to stop a blocked capture. Setting it non-blocking is harmless for the
			// regular-file snapshot and the pipe write end that also travel this path.
			_ = syscall.SetNonblock(descriptor, true)
			files = append(files, os.NewFile(uintptr(descriptor), "guest-control-fd"))
		}
	}
	return files, nil
}

func closeFiles(files []*os.File) {
	for _, file := range files {
		if file != nil {
			_ = file.Close()
		}
	}
}

// roundTrip dials the control socket, sends one request with any attached files,
// and returns the decoded response plus any files the supervisor sent back.
func roundTrip(
	socketPath string,
	request controlRequest,
	sendFiles []*os.File,
	timeout time.Duration,
) (controlResponse, []*os.File, error) {
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		slog.Warn("guest supervisor dial control socket failed", "socket", socketPath, "err", err)
		return controlResponse{}, nil, fmt.Errorf("guestsupervisor: dial control socket %s: %w", socketPath, err)
	}
	defer func() { _ = conn.Close() }()
	if timeout > 0 {
		_ = conn.SetDeadline(clock.System().Now().Add(timeout))
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return controlResponse{}, nil, fmt.Errorf("guestsupervisor: encode control request: %w", err)
	}
	if err := writeFrame(conn, payload, sendFiles); err != nil {
		return controlResponse{}, nil, err
	}
	body, files, err := readFrame(conn)
	if err != nil {
		return controlResponse{}, nil, err
	}
	var response controlResponse
	if err := json.Unmarshal(body, &response); err != nil {
		closeFiles(files)
		return controlResponse{}, nil, fmt.Errorf("guestsupervisor: decode control response: %w", err)
	}
	if response.Error != "" {
		closeFiles(files)
		return controlResponse{}, nil, fmt.Errorf("guestsupervisor: control request rejected: %s", response.Error)
	}
	return response, files, nil
}

// controlRequestTimeout bounds a single non-blocking control round trip.
const controlRequestTimeout = 10 * time.Second

// newControlRequest returns a zero-valued request for op, so every field is
// specified in one place and callers set only the fields their op uses.
func newControlRequest(op string) controlRequest {
	return controlRequest{
		Op:                op,
		Generation:        0,
		Spec:              nil,
		AckPID:            0,
		ExecutablePath:    "",
		Arguments:         nil,
		Environment:       nil,
		PollTimeoutMillis: 0,
	}
}

// newControlResponse returns a zero-valued response, so callers set only the
// fields their reply carries while every field stays specified in one place.
func newControlResponse() controlResponse {
	return controlResponse{
		Error:         "",
		PID:           0,
		PGID:          0,
		NewPID:        0,
		Exits:         nil,
		SupervisorPID: 0,
		State:         "",
		Generation:    0,
	}
}

// Attach announces a freshly started or restored worker generation as current,
// which flips the supervisor to Steady and routes buffered exits to it.
func Attach(socketPath string, generation uint64) (SupervisorStatus, error) {
	request := newControlRequest(opAttach)
	request.Generation = generation
	response, files, err := roundTrip(socketPath, request, nil, controlRequestTimeout)
	closeFiles(files)
	if err != nil {
		return SupervisorStatus{}, err
	}
	return SupervisorStatus{PID: response.SupervisorPID, State: parseState(response.State), Generation: response.Generation}, nil
}

// StartChild asks the supervisor to fork a runner and returns its identifiers
// plus the worker's private dup of the pipe read ends. The supervisor keeps its
// own durable read ends and runs the waitpid loop.
func StartChild(socketPath string, spec guestexec.ExecSpec) (LaunchedChild, error) {
	request := newControlRequest(opStartChild)
	request.Spec = execSpecToWire(spec)
	response, files, err := roundTrip(socketPath, request, nil, controlRequestTimeout)
	if err != nil {
		return LaunchedChild{}, err
	}
	if len(files) != 2 {
		closeFiles(files)
		slog.Warn("guest supervisor start_child reply carried wrong file count", "count", len(files))
		return LaunchedChild{}, fmt.Errorf("guestsupervisor: start_child reply carried %d files, want 2", len(files))
	}
	return LaunchedChild{PID: response.PID, PGID: response.PGID, Stdout: files[0], Stderr: files[1]}, nil
}

// PollExits long-polls the supervisor for the calling generation's unacked
// runner exits. It returns an empty slice on timeout. An unacked exit is
// redelivered on every poll until AckExit clears it, which is how a runner that
// exits mid-reload reaches whichever worker becomes current.
func PollExits(socketPath string, generation uint64, timeout time.Duration) ([]guestexec.ExitReport, error) {
	request := newControlRequest(opPollExits)
	request.Generation = generation
	request.PollTimeoutMillis = timeout.Milliseconds()
	response, files, err := roundTrip(socketPath, request, nil, timeout+controlRequestTimeout)
	closeFiles(files)
	if err != nil {
		return nil, err
	}
	reports := make([]guestexec.ExitReport, 0, len(response.Exits))
	for _, exit := range response.Exits {
		reports = append(reports, guestexec.ExitReport{PID: exit.PID, ExitCode: exit.ExitCode, Message: exit.Message})
	}
	return reports, nil
}

// AckExit clears one runner exit from the supervisor's unacked buffer once a
// worker has recorded it into its registry.
func AckExit(socketPath string, pid int) error {
	request := newControlRequest(opAckExit)
	request.AckPID = pid
	_, files, err := roundTrip(socketPath, request, nil, controlRequestTimeout)
	closeFiles(files)
	return err
}

// RequestReplacement hands the supervisor a frozen snapshot and a readiness pipe
// write end, and asks it to spawn a replacement worker. The supervisor supplies
// the durable listener and pipe read ends itself, so the caller passes only the
// two file descriptors it owns.
func RequestReplacement(
	socketPath string,
	executablePath string,
	arguments []string,
	environment []string,
	snapshot *os.File,
	readyWrite *os.File,
) (int, error) {
	request := newControlRequest(opReplaceWorker)
	request.ExecutablePath = executablePath
	request.Arguments = arguments
	request.Environment = environment
	response, files, err := roundTrip(socketPath, request, []*os.File{snapshot, readyWrite}, controlRequestTimeout)
	closeFiles(files)
	if err != nil {
		return 0, err
	}
	if response.NewPID <= 0 {
		slog.Warn("guest supervisor replace_worker returned invalid pid", "pid", response.NewPID)
		return 0, fmt.Errorf("guestsupervisor: replace_worker returned invalid pid %d", response.NewPID)
	}
	return response.NewPID, nil
}

func parseState(name string) State {
	switch name {
	case StateBooting.String():
		return StateBooting
	case StateSteady.String():
		return StateSteady
	case StateReplacing.String():
		return StateReplacing
	default:
		return StateBooting
	}
}

func execSpecToWire(spec guestexec.ExecSpec) *childSpecWire {
	return &childSpecWire{
		ExecutionID: spec.ExecutionID,
		Slot:        spec.Slot,
		Command:     spec.Command,
		Args:        spec.Args,
		Env:         spec.Env,
		WorkingDir:  spec.WorkingDir,
		Meta: jobMetaWire{
			Repo:       spec.Meta.Repo,
			JobID:      spec.Meta.JobID,
			RunID:      spec.Meta.RunID,
			RunnerName: spec.Meta.RunnerName,
		},
	}
}

func wireToExecSpec(wire *childSpecWire) guestexec.ExecSpec {
	if wire == nil {
		var empty guestexec.ExecSpec
		return empty
	}
	return guestexec.ExecSpec{
		ExecutionID: wire.ExecutionID,
		Slot:        wire.Slot,
		Command:     wire.Command,
		Args:        wire.Args,
		Env:         wire.Env,
		WorkingDir:  wire.WorkingDir,
		Meta: guestexec.JobMeta{
			Repo:       wire.Meta.Repo,
			JobID:      wire.Meta.JobID,
			RunID:      wire.Meta.RunID,
			RunnerName: wire.Meta.RunnerName,
		},
	}
}

// killGroup sends SIGKILL to a whole process group, treating a vanished group as
// success so a repeated cleanup is a no-op.
func killGroup(pgid int) error {
	err := syscall.Kill(-pgid, syscall.SIGKILL)
	if err == nil || errors.Is(err, syscall.ESRCH) {
		return nil
	}
	slog.Warn("guest supervisor kill process group failed", "pgid", pgid, "err", err)
	return fmt.Errorf("guestsupervisor: kill process group %d: %w", pgid, err)
}
