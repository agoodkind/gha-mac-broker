// Package guestclient wraps the generated guest-agent ConnectRPC client for
// host-side callers.
package guestclient

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"connectrpc.com/connect"
	"goodkind.io/gha-mac-broker/internal/guestproto"
	"goodkind.io/gha-mac-broker/internal/guestproto/guestprotoconnect"
	"goodkind.io/gha-mac-broker/internal/guesttransport"
)

// Client is a token-authenticated host-side guest-agent client.
type Client struct {
	service guestprotoconnect.GuestAgentServiceClient
}

// New creates a guest-agent client for an h2c TCP address.
func New(ctx context.Context, address string, token string) *Client {
	transport := guesttransport.DialContext(ctx, address, token)
	service := guestprotoconnect.NewGuestAgentServiceClient(
		transport.HTTPClient(),
		baseURL(address),
		connect.WithInterceptors(transport.Interceptor()),
	)
	return &Client{service: service}
}

// Hello returns the agent metadata snapshot.
func (client *Client) Hello(ctx context.Context) (*guestproto.HelloResponse, error) {
	response, err := client.service.Hello(
		ctx,
		connect.NewRequest(&guestproto.HelloRequest{}),
	)
	if err != nil {
		slog.WarnContext(ctx, "guestclient hello failed", "err", err)
		return nil, fmt.Errorf("guestclient hello: %w", err)
	}
	return response.Msg, nil
}

// RunJob starts or reuses one guest execution.
func (client *Client) RunJob(
	ctx context.Context,
	request *guestproto.RunJobRequest,
) (*guestproto.RunJobResponse, error) {
	response, err := client.service.RunJob(ctx, connect.NewRequest(request))
	if err != nil {
		slog.WarnContext(ctx, "guestclient run job failed", "err", err)
		return nil, fmt.Errorf("guestclient run job: %w", err)
	}
	return response.Msg, nil
}

// JobStatus opens a status stream from the supplied resume cursor.
func (client *Client) JobStatus(
	ctx context.Context,
	executionID string,
	fromSequence uint64,
) (*connect.ServerStreamForClient[guestproto.JobStatusEvent], error) {
	stream, err := client.service.JobStatus(
		ctx,
		connect.NewRequest(&guestproto.JobStatusRequest{
			ExecutionId:  executionID,
			FromSequence: fromSequence,
		}),
	)
	if err != nil {
		slog.WarnContext(ctx, "guestclient job status failed", "err", err)
		return nil, fmt.Errorf("guestclient job status: %w", err)
	}
	return stream, nil
}

// Reattach returns the agent's current active and retained executions.
func (client *Client) Reattach(ctx context.Context) (*guestproto.ReattachResponse, error) {
	response, err := client.service.Reattach(
		ctx,
		connect.NewRequest(&guestproto.ReattachRequest{}),
	)
	if err != nil {
		slog.WarnContext(ctx, "guestclient reattach failed", "err", err)
		return nil, fmt.Errorf("guestclient reattach: %w", err)
	}
	return response.Msg, nil
}

// Drain refuses future executions and reports whether the agent is idle.
func (client *Client) Drain(ctx context.Context) (*guestproto.DrainResponse, error) {
	response, err := client.service.Drain(
		ctx,
		connect.NewRequest(&guestproto.DrainRequest{}),
	)
	if err != nil {
		slog.WarnContext(ctx, "guestclient drain failed", "err", err)
		return nil, fmt.Errorf("guestclient drain: %w", err)
	}
	return response.Msg, nil
}

// CancelJob requests cancellation for one guest execution.
func (client *Client) CancelJob(ctx context.Context, executionID string) error {
	_, err := client.service.CancelJob(
		ctx,
		connect.NewRequest(&guestproto.CancelJobRequest{ExecutionId: executionID}),
	)
	if err != nil {
		slog.WarnContext(ctx, "guestclient cancel job failed", "err", err)
		return fmt.Errorf("guestclient cancel job: %w", err)
	}
	return nil
}

func baseURL(address string) string {
	if strings.HasPrefix(address, "http://") || strings.HasPrefix(address, "https://") {
		return strings.TrimRight(address, "/")
	}
	return "http://" + address
}
