package broker

import (
	"context"
	"os/exec"
	"sync"
	"time"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/ghapp"
	"goodkind.io/gha-mac-broker/internal/guestproto"
	"goodkind.io/gha-mac-broker/internal/guesttransport"
	"goodkind.io/gha-mac-broker/internal/tart"
)

// jobStatusStream receives guest execution events for the broker.
type jobStatusStream interface {
	Receive() (*guestproto.JobStatusEvent, error)
	Close() error
}

// guestConn defines the guest-agent operations that Binder uses.
type guestConn interface {
	Hello(ctx context.Context) (*guestproto.HelloResponse, error)
	RunJob(
		ctx context.Context,
		request *guestproto.RunJobRequest,
	) (*guestproto.RunJobResponse, error)
	JobStatus(ctx context.Context, executionID string, fromSequence uint64) (jobStatusStream, error)
	Reattach(ctx context.Context) (*guestproto.ReattachResponse, error)
	Drain(ctx context.Context) (*guestproto.DrainResponse, error)
	CancelJob(ctx context.Context, executionID string) error
}

// WarmVM stores a booted VM after its guest agent answers Hello.
// Warm initializes Name before concurrent reads. Warm and Adopt resolve the
// guest endpoint once before concurrent job execution.
type WarmVM struct {
	// Name is the tart VM name used for exec, stop, and delete.
	Name string
	// Image is the approved Cirrus tag this VM was cloned for.
	Image string
	boot  *exec.Cmd
	// guestMu guards the cached guest endpoint.
	guestMu   sync.Mutex
	guestConn guestConn
}

// SlotBinding stores one busy runner slot discovered during VM adoption.
type SlotBinding struct {
	SlotIndex      int
	Repo           string
	JobID          int64
	RunID          int64
	ExecutionID    string
	ResumeCursor   uint64
	BoundAt        time.Time
	ObservedActive bool
}

// AdoptedVM is a running pool VM discovered during broker startup.
type AdoptedVM struct {
	VM    *WarmVM
	Slots []SlotBinding
}

// Binder uses the guest agent for job execution and adoption.
// It uses Tart for VM lifecycle operations.
type Binder struct {
	cfgMu sync.RWMutex
	cfg   *config.Config
	gh    *ghapp.Client
	vm    *tart.Tart
	// dialGuest builds a guest-agent client from a connection dialer and token.
	// It is a field so tests can stub the transport.
	dialGuest func(ctx context.Context, dial guesttransport.GuestDialer, token string) guestConn
	// guestFor resolves and caches the guest client for a VM. It is the single
	// seam tests override to return a stub without a real VM.
	guestFor func(ctx context.Context, vm *WarmVM) (guestConn, error)
}
