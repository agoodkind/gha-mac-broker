// Package guestagent adapts the guest execution registry to the generated
// ConnectRPC guest-agent service.
package guestagent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"goodkind.io/gha-mac-broker/internal/guestexec"
	"goodkind.io/gha-mac-broker/internal/guestproto"
)

const phase0Placeholder = "echo guest-agent phase0 placeholder"

func phase0Script(jitConfig string) string {
	script := strings.TrimSpace(jitConfig)
	if script == "" {
		return phase0Placeholder
	}
	return script
}

func copyEnvironment(environment map[string]string) map[string]string {
	if len(environment) == 0 {
		return nil
	}
	copied := make(map[string]string, len(environment))
	for key, value := range environment {
		copied[key] = value
	}
	return copied
}

func protoMetaToExec(meta *guestproto.JobMeta) guestexec.JobMeta {
	if meta == nil {
		return guestexec.JobMeta{}
	}
	return guestexec.JobMeta{
		Repo:       meta.GetRepo(),
		JobID:      meta.GetJobId(),
		RunID:      meta.GetRunId(),
		RunnerName: meta.GetRunnerName(),
	}
}

func execMetaToProto(meta guestexec.JobMeta) *guestproto.JobMeta {
	return &guestproto.JobMeta{
		Repo:       meta.Repo,
		JobId:      meta.JobID,
		RunId:      meta.RunID,
		RunnerName: meta.RunnerName,
	}
}

func outcomeToProto(outcome guestexec.Outcome) guestproto.RunJobResponse_Outcome {
	switch outcome {
	case guestexec.OutcomeAccepted:
		return guestproto.RunJobResponse_ACCEPTED
	case guestexec.OutcomeAlreadyRunning:
		return guestproto.RunJobResponse_ALREADY_RUNNING
	case guestexec.OutcomeAlreadyCompleted:
		return guestproto.RunJobResponse_ALREADY_COMPLETED
	case guestexec.OutcomeConflict:
		return guestproto.RunJobResponse_CONFLICT
	default:
		return guestproto.RunJobResponse_OUTCOME_UNSPECIFIED
	}
}

func streamToProto(stream guestexec.Stream) guestproto.LogChunk_Stream {
	switch stream {
	case guestexec.StreamStdout:
		return guestproto.LogChunk_STDOUT
	case guestexec.StreamStderr:
		return guestproto.LogChunk_STDERR
	default:
		return guestproto.LogChunk_STREAM_UNSPECIFIED
	}
}

func eventToProto(event guestexec.Event) (*guestproto.JobStatusEvent, bool) {
	message := &guestproto.JobStatusEvent{Sequence: event.Sequence}
	switch payload := event.Payload.(type) {
	case guestexec.PhaseChange:
		message.Event = &guestproto.JobStatusEvent_Phase{
			Phase: &guestproto.PhaseChange{Phase: payload.Phase},
		}
	case guestexec.LogChunk:
		message.Event = &guestproto.JobStatusEvent_Log{
			Log: &guestproto.LogChunk{
				Stream: streamToProto(payload.Stream),
				Data:   append([]byte(nil), payload.Data...),
			},
		}
	case guestexec.Heartbeat:
		message.Event = &guestproto.JobStatusEvent_Heartbeat{
			Heartbeat: &guestproto.Heartbeat{UnixNanos: payload.UnixNanos},
		}
	case guestexec.TerminalResult:
		message.Event = &guestproto.JobStatusEvent_Result{
			Result: &guestproto.TerminalResult{
				ExitCode: payload.ExitCode,
				Message:  payload.Message,
			},
		}
		return message, true
	default:
		message.Event = &guestproto.JobStatusEvent_Phase{
			Phase: &guestproto.PhaseChange{Phase: "unknown"},
		}
	}
	return message, false
}

func executionStateToProto(state guestexec.ExecutionState) *guestproto.ExecutionState {
	return &guestproto.ExecutionState{
		ExecutionId:  state.ExecutionID,
		Slot:         state.Slot,
		Meta:         execMetaToProto(state.Meta),
		Phase:        state.Phase,
		Running:      state.Running,
		LastSequence: state.LastSequence,
	}
}

func mapRegistryError(err error) error {
	switch {
	case errors.Is(err, guestexec.ErrExecutionNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, guestexec.ErrDraining):
		return connect.NewError(connect.CodeUnavailable, err)
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

func generateBootID() string {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err == nil {
		return hex.EncodeToString(entropy[:])
	}
	return "boot-" + strconv.FormatUint(uint64(entropy[0]), 36)
}
