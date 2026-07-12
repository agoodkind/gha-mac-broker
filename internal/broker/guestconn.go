package broker

import (
	"context"
	"fmt"
	"log/slog"

	"goodkind.io/gha-mac-broker/internal/guestclient"
)

// guestClientAdapter adapts the concrete guestclient.Client to the guestConn
// interface the binder depends on. It embeds the client so its already-wrapped
// Hello, RunJob, Reattach, Drain, and CancelJob methods satisfy guestConn
// directly, and overrides JobStatus to narrow the concrete stream to the
// jobStatusStream interface.
type guestClientAdapter struct {
	*guestclient.Client
}

func newGuestClientAdapter(ctx context.Context, address string, token string) guestConn {
	return guestClientAdapter{Client: guestclient.New(ctx, address, token)}
}

func (a guestClientAdapter) JobStatus(ctx context.Context, executionID string, fromSequence uint64) (jobStatusStream, error) {
	stream, err := a.Client.JobStatus(ctx, executionID, fromSequence)
	if err != nil {
		slog.WarnContext(ctx, "guest job status open failed", "err", err, "execution", executionID)
		return nil, fmt.Errorf("broker: guest job status: %w", err)
	}
	return stream, nil
}
