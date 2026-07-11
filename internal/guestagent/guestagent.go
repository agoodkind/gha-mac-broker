// Package guestagent adapts the guest execution registry to the generated
// ConnectRPC guest-agent service.
package guestagent

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"goodkind.io/gha-mac-broker/internal/guestexec"
	"goodkind.io/gha-mac-broker/internal/guestproto"
	"goodkind.io/gha-mac-broker/internal/guestproto/guestprotoconnect"
	"goodkind.io/gha-mac-broker/internal/version"
)

const (
	protocolMajor       = uint32(1)
	defaultAgentBuild   = "dev"
	defaultSlotCount    = uint32(1)
	capabilityRunJob    = "run-job"
	capabilityReattach  = "reattach"
	capabilityDrain     = "drain"
	capabilityCancelJob = "cancel-job"
)

var processBootID = generateBootID()

// ChildLauncher launches a runner outside the worker process and records it in
// the worker's registry, returning the admission outcome. The guest-worker
// supplies a supervisor-backed launcher so the durable supervisor stays the
// runner's parent; when it is nil the handler forks in-process through the
// registry, preserving the single-process guest-agent path.
type ChildLauncher interface {
	Run(spec guestexec.ExecSpec) (guestexec.Outcome, error)
}

// Options configures the guest-agent service.
type Options struct {
	SlotCount         uint32
	BootID            string
	AgentBuild        string
	GoldenFingerprint string
	ChildLauncher     ChildLauncher
	// SpecBuilder assembles the runner ExecSpec and runs per-slot setup for each
	// RunJob. When nil the handler uses the production runner executor, which
	// clones the runner, seeds the slot HOME, and sets the slot keychain.
	SpecBuilder SpecBuilder
}

// Handler implements GuestAgentService over a guest execution registry.
type Handler struct {
	registry          *guestexec.Registry
	slotCount         uint32
	bootID            string
	agentBuild        string
	goldenFingerprint string
	childLauncher     ChildLauncher
	specBuilder       SpecBuilder
}

var _ guestprotoconnect.GuestAgentServiceHandler = (*Handler)(nil)

// New returns a registry-backed guest-agent service handler.
func New(registry *guestexec.Registry, options Options) *Handler {
	if registry == nil {
		registry = guestexec.New(guestexec.Options{
			Retention:         0,
			HeartbeatInterval: 0,
		})
	}
	slotCount := options.SlotCount
	if slotCount == 0 {
		slotCount = defaultSlotCount
	}
	bootID := options.BootID
	if bootID == "" {
		bootID = processBootID
	}
	agentBuild := options.AgentBuild
	if agentBuild == "" {
		agentBuild = version.Version
	}
	if agentBuild == "" {
		agentBuild = defaultAgentBuild
	}
	specBuilder := options.SpecBuilder
	if specBuilder == nil {
		specBuilder = newRunnerExecutor()
	}
	return &Handler{
		registry:          registry,
		slotCount:         slotCount,
		bootID:            bootID,
		agentBuild:        agentBuild,
		goldenFingerprint: options.GoldenFingerprint,
		childLauncher:     options.ChildLauncher,
		specBuilder:       specBuilder,
	}
}

// NewHTTPHandler mounts a guest-agent service on the generated ConnectRPC path.
func NewHTTPHandler(registry *guestexec.Registry, options Options) http.Handler {
	mux := http.NewServeMux()
	path, handler := guestprotoconnect.NewGuestAgentServiceHandler(New(registry, options))
	mux.Handle(path, handler)
	return mux
}

// Hello returns agent metadata and the current configured slot snapshot.
func (handler *Handler) Hello(
	_ context.Context,
	_ *connect.Request[guestproto.HelloRequest],
) (*connect.Response[guestproto.HelloResponse], error) {
	response := &guestproto.HelloResponse{
		BootId:        handler.bootID,
		AgentBuild:    handler.agentBuild,
		ProtocolMajor: protocolMajor,
		Capabilities: []string{
			capabilityRunJob,
			capabilityReattach,
			capabilityDrain,
			capabilityCancelJob,
		},
		Slots:             handler.slots(),
		GoldenFingerprint: handler.goldenFingerprint,
	}
	return connect.NewResponse(response), nil
}

// RunJob builds the runner ExecSpec through the spec builder, which prepares the
// slot and returns the absolute run.sh launch, then starts or reuses a registry
// execution using the request ID as the idempotency key.
func (handler *Handler) RunJob(
	ctx context.Context,
	request *connect.Request[guestproto.RunJobRequest],
) (*connect.Response[guestproto.RunJobResponse], error) {
	jobRequest := JobRequest{
		ExecutionID: request.Msg.GetExecutionId(),
		Slot:        request.Msg.GetSlot(),
		Meta:        protoMetaToExec(request.Msg.GetMeta()),
		JitConfig:   request.Msg.GetJitConfig(),
		Env:         copyEnvironment(request.Msg.GetEnv()),
	}
	spec, err := handler.specBuilder.Build(ctx, jobRequest)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	outcome, err := handler.startExecution(spec)
	if err != nil {
		return nil, mapRegistryError(err)
	}
	response := &guestproto.RunJobResponse{Outcome: outcomeToProto(outcome)}
	return connect.NewResponse(response), nil
}

// startExecution routes an execution to the supervisor-backed launcher when one
// is configured, so the durable supervisor forks and waits the runner. Without a
// launcher it forks in-process through the registry.
func (handler *Handler) startExecution(spec guestexec.ExecSpec) (guestexec.Outcome, error) {
	if handler.childLauncher != nil {
		return handler.childLauncher.Run(spec)
	}
	return handler.registry.Start(spec)
}

// JobStatus streams replayed and live execution events until a terminal result,
// stream cancellation, or registry close.
func (handler *Handler) JobStatus(
	ctx context.Context,
	request *connect.Request[guestproto.JobStatusRequest],
	stream *connect.ServerStream[guestproto.JobStatusEvent],
) error {
	events, unsubscribe, err := handler.registry.Subscribe(
		request.Msg.GetExecutionId(),
		request.Msg.GetFromSequence(),
	)
	if err != nil {
		return mapRegistryError(err)
	}
	defer unsubscribe()

	for {
		select {
		case event, ok := <-events:
			if !ok {
				return nil
			}
			protoEvent, terminal := eventToProto(event)
			if sendErr := stream.Send(protoEvent); sendErr != nil {
				return sendErr
			}
			if terminal {
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// Reattach returns active and retained executions with their last sequence.
func (handler *Handler) Reattach(
	_ context.Context,
	_ *connect.Request[guestproto.ReattachRequest],
) (*connect.Response[guestproto.ReattachResponse], error) {
	states := handler.registry.List()
	executions := make([]*guestproto.ExecutionState, 0, len(states))
	for _, state := range states {
		executions = append(executions, executionStateToProto(state))
	}
	return connect.NewResponse(&guestproto.ReattachResponse{Executions: executions}), nil
}

// Drain refuses future executions and reports whether the registry is idle.
func (handler *Handler) Drain(
	_ context.Context,
	_ *connect.Request[guestproto.DrainRequest],
) (*connect.Response[guestproto.DrainResponse], error) {
	state := handler.registry.Drain()
	response := &guestproto.DrainResponse{
		Idle:             state.Idle,
		ActiveExecutions: state.ActiveExecutions,
	}
	return connect.NewResponse(response), nil
}

// CancelJob requests process-group cancellation for one execution.
func (handler *Handler) CancelJob(
	_ context.Context,
	request *connect.Request[guestproto.CancelJobRequest],
) (*connect.Response[guestproto.CancelJobResponse], error) {
	if err := handler.registry.Cancel(request.Msg.GetExecutionId()); err != nil {
		return nil, mapRegistryError(err)
	}
	return connect.NewResponse(&guestproto.CancelJobResponse{}), nil
}

func (handler *Handler) slots() []*guestproto.SlotInfo {
	activeBySlot := make(map[uint32]string)
	for _, state := range handler.registry.List() {
		if state.Running {
			activeBySlot[state.Slot] = state.ExecutionID
		}
	}
	slots := make([]*guestproto.SlotInfo, 0, handler.slotCount)
	for slot := uint32(0); slot < handler.slotCount; slot++ {
		executionID, busy := activeBySlot[slot]
		slots = append(slots, &guestproto.SlotInfo{
			Index:       slot,
			Busy:        busy,
			ExecutionId: executionID,
		})
	}
	return slots
}
