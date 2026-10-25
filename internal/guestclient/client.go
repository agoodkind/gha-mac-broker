// Package guestclient wraps the generated guest-agent ConnectRPC client for
// host-side callers.
package guestclient

import (
	"context"
	"fmt"
	"log/slog"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"goodkind.io/gha-mac-broker/internal/guestproto"
	"goodkind.io/gha-mac-broker/internal/guestproto/guestprotoconnect"
	"goodkind.io/gha-mac-broker/internal/guesttransport"
)

// guestBaseURL is a fixed pseudo-authority for the ConnectRPC client. The
// transport dials over `tart exec` through the dialer's DialContext, so the host
// portion of this URL never routes a connection; it only satisfies ConnectRPC's
// URL construction.
const guestBaseURL = "http://guest"

// Client is a token-authenticated host-side guest-agent client.
type Client struct {
	service guestprotoconnect.GuestAgentServiceClient
}

// New creates a guest-agent client whose transport connections come from dial,
// which reaches the guest agent over the tart guest-agent channel.
func New(ctx context.Context, dial guesttransport.GuestDialer, token string) *Client {
	transport := guesttransport.DialContext(ctx, dial, token)
	httpTransport := connecthttp.NewTransport(
		transport.HTTPClient(),
		guestBaseURL,
		connecthttp.WithReadMaxBytes(0),
	)
	rpcClient := connect.NewClient(httpTransport, transport.Interceptor())
	service := guestprotoconnect.NewGuestAgentServiceClient(rpcClient)
	return &Client{service: service}
}

// Hello returns the agent metadata snapshot.
func (client *Client) Hello(ctx context.Context) (*guestproto.HelloResponse, error) {
	response, err := client.service.Hello(
		ctx,
		&guestproto.HelloRequest{},
	)
	if err != nil {
		slog.WarnContext(ctx, "guestclient hello failed", "err", err)
		return nil, fmt.Errorf("guestclient hello: %w", err)
	}
	return response, nil
}

// RunJob starts or reuses one guest execution.
func (client *Client) RunJob(
	ctx context.Context,
	request *guestproto.RunJobRequest,
) (*guestproto.RunJobResponse, error) {
	response, err := client.service.RunJob(ctx, request)
	if err != nil {
		slog.WarnContext(ctx, "guestclient run job failed", "err", err)
		return nil, fmt.Errorf("guestclient run job: %w", err)
	}
	return response, nil
}

// JobStatus opens a status stream from the supplied resume cursor.
func (client *Client) JobStatus(
	ctx context.Context,
	executionID string,
	fromSequence uint64,
) (guestprotoconnect.GuestAgentServiceJobStatusClientStream, error) {
	stream, err := client.service.JobStatus(
		ctx,
		&guestproto.JobStatusRequest{
			ExecutionId:  executionID,
			FromSequence: fromSequence,
		},
	)
	if err != nil {
		slog.WarnContext(ctx, "guestclient job status failed", "err", err)
		return guestprotoconnect.GuestAgentServiceJobStatusClientStream{}, fmt.Errorf("guestclient job status: %w", err)
	}
	return stream, nil
}

// Reattach returns the agent's current active and retained executions.
func (client *Client) Reattach(ctx context.Context) (*guestproto.ReattachResponse, error) {
	response, err := client.service.Reattach(
		ctx,
		&guestproto.ReattachRequest{},
	)
	if err != nil {
		slog.WarnContext(ctx, "guestclient reattach failed", "err", err)
		return nil, fmt.Errorf("guestclient reattach: %w", err)
	}
	return response, nil
}

// Drain refuses future executions and reports whether the agent is idle.
func (client *Client) Drain(ctx context.Context) (*guestproto.DrainResponse, error) {
	response, err := client.service.Drain(
		ctx,
		&guestproto.DrainRequest{},
	)
	if err != nil {
		slog.WarnContext(ctx, "guestclient drain failed", "err", err)
		return nil, fmt.Errorf("guestclient drain: %w", err)
	}
	return response, nil
}

// CancelJob requests cancellation for one guest execution.
func (client *Client) CancelJob(ctx context.Context, executionID string) error {
	_, err := client.service.CancelJob(
		ctx,
		&guestproto.CancelJobRequest{ExecutionId: executionID},
	)
	if err != nil {
		slog.WarnContext(ctx, "guestclient cancel job failed", "err", err)
		return fmt.Errorf("guestclient cancel job: %w", err)
	}
	return nil
}
