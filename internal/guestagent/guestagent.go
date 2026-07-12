// Package guestagent adapts the guest execution registry to the generated
// ConnectRPC guest-agent service.
package guestagent

import (
	"context"
	"crypto/ed25519"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"connectrpc.com/connect"
	"goodkind.io/gha-mac-broker/internal/guestexec"
	"goodkind.io/gha-mac-broker/internal/guestproto"
	"goodkind.io/gha-mac-broker/internal/guestproto/guestprotoconnect"
	"goodkind.io/gha-mac-broker/internal/version"
)

const (
	protocolMajor            = uint32(1)
	defaultAgentBuild        = "dev"
	defaultSlotCount         = uint32(1)
	capabilityRunJob         = "run-job"
	capabilityReattach       = "reattach"
	capabilityDrain          = "drain"
	capabilityCancelJob      = "cancel-job"
	capabilityUpdateAgent    = "update-agent"
	capabilityConfigureSlots = "configure-slots"
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

// SlotConfigurer applies a host-requested slot count. A worker cannot change its
// own slot count, which is fixed at worker start, so the implementation asks the
// durable supervisor to reconfigure and replace the worker. It returns the
// applied count, or an error when the supervisor rejects the change (for example
// a shrink below a running slot).
type SlotConfigurer interface {
	ConfigureSlots(ctx context.Context, slotCount uint32) (uint32, error)
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
	// Reloader triggers the in-VM worker reload onto a freshly placed binary
	// after UpdateAgent verifies and installs it. When nil, UpdateAgent reports
	// CodeUnimplemented, so a build without update wiring simply refuses updates.
	Reloader AgentReloader
	// InstallDir is the directory UpdateAgent streams the temp binary into and
	// renames the versioned binary within. It must sit on the same filesystem as
	// the running binary so the rename stays atomic. When empty it defaults to
	// the directory holding the running executable.
	InstallDir string
	// UpdatePublicKey overrides the baked ed25519 public key UpdateAgent trusts
	// for the detached signature. When nil the handler uses the compile-time
	// baked key. Tests inject a known key here.
	UpdatePublicKey ed25519.PublicKey
	// SlotConfigurer applies a host-requested slot count by asking the supervisor
	// to reconfigure and replace the worker. When nil, ConfigureSlots reports
	// CodeUnimplemented, so a single-process guest-agent without a supervisor
	// refuses slot reconfiguration.
	SlotConfigurer SlotConfigurer
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
	reloader          AgentReloader
	installDir        string
	currentBinary     string
	updatePublicKey   ed25519.PublicKey
	slotConfigurer    SlotConfigurer
	// slotLocksMu guards slotLocks; each slot lock serializes admission, per-slot
	// prep, and start for one slot, so two concurrent RunJobs for the same slot
	// cannot both run destructive prepareSlot before either records the execution.
	slotLocksMu sync.Mutex
	slotLocks   map[uint32]*sync.Mutex
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
	currentBinary := ""
	if executable, err := os.Executable(); err == nil {
		currentBinary = executable
	}
	installDir := options.InstallDir
	if installDir == "" && currentBinary != "" {
		installDir = filepath.Dir(currentBinary)
	}
	updatePublicKey := options.UpdatePublicKey
	if updatePublicKey == nil {
		updatePublicKey = updateSigningPublicKey
	}
	return &Handler{
		registry:          registry,
		slotCount:         slotCount,
		bootID:            bootID,
		agentBuild:        agentBuild,
		goldenFingerprint: options.GoldenFingerprint,
		childLauncher:     options.ChildLauncher,
		specBuilder:       specBuilder,
		reloader:          options.Reloader,
		installDir:        installDir,
		currentBinary:     currentBinary,
		updatePublicKey:   updatePublicKey,
		slotConfigurer:    options.SlotConfigurer,
		slotLocksMu:       sync.Mutex{},
		slotLocks:         make(map[uint32]*sync.Mutex),
	}
}

// slotLock returns the lock that serializes admission, prep, and start for slot,
// creating it on first use. The returned lock is stable for the slot's lifetime,
// so every RunJob for that slot contends on the same lock.
func (handler *Handler) slotLock(slot uint32) *sync.Mutex {
	handler.slotLocksMu.Lock()
	defer handler.slotLocksMu.Unlock()
	lock, found := handler.slotLocks[slot]
	if !found {
		lock = &sync.Mutex{}
		handler.slotLocks[slot] = lock
	}
	return lock
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
			capabilityUpdateAgent,
			capabilityConfigureSlots,
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
	// Serialize admission, prep, and start per slot for the whole critical section,
	// so two concurrent RunJobs for the same slot cannot both pass admission and
	// both run destructive prepareSlot before either records the execution. The
	// lock is released on every return path, and never held across the job run.
	slotLock := handler.slotLock(jobRequest.Slot)
	slotLock.Lock()
	defer slotLock.Unlock()
	// Peek admission before any destructive per-slot prep. Build runs prepareSlot,
	// which rm -rf's and re-clones the slot's runner dir, HOME, and keychain, so an
	// idempotent retry of a live execution or a job routed to a busy slot must be
	// rejected here rather than wiping a co-tenant runner out from under it. The
	// authoritative admission still happens under the registry lock at Register.
	admission, admitErr := handler.registry.CheckAdmission(guestexec.ExecSpec{
		ExecutionID: jobRequest.ExecutionID,
		Slot:        jobRequest.Slot,
	})
	if admitErr != nil {
		return nil, mapRegistryError(admitErr)
	}
	if admission != guestexec.OutcomeAccepted {
		response := &guestproto.RunJobResponse{Outcome: outcomeToProto(admission)}
		return connect.NewResponse(response), nil
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

// ConfigureSlots applies a host-requested slot count. The worker's own slot
// count is fixed at start, so the handler asks the supervisor to reconfigure and
// replace the worker; a subsequent Hello (after the replacement is current)
// advertises the applied slots. A shrink below a running slot is rejected as a
// failed precondition, so the host leaves that VM as is rather than orphaning a
// job.
//
// The returned slot_count and slots are the INTENDED (requested) inventory,
// computed before the replacement worker is serving, not the currently-serving
// set. The reload is asynchronous, so a consumer that needs the live inventory
// must re-Hello; the host does. Do not trust the returned slots as live.
func (handler *Handler) ConfigureSlots(
	ctx context.Context,
	request *connect.Request[guestproto.ConfigureSlotsRequest],
) (*connect.Response[guestproto.ConfigureSlotsResponse], error) {
	requested := request.Msg.GetSlotCount()
	if requested == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("guestagent: slot_count must be positive"))
	}
	if handler.slotConfigurer == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("guestagent: slot reconfiguration unavailable"))
	}
	applied, err := handler.slotConfigurer.ConfigureSlots(ctx, requested)
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	slots := make([]*guestproto.SlotInfo, 0, applied)
	for slot := uint32(0); slot < applied; slot++ {
		slots = append(slots, &guestproto.SlotInfo{Index: slot})
	}
	response := &guestproto.ConfigureSlotsResponse{SlotCount: applied, Slots: slots}
	return connect.NewResponse(response), nil
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
