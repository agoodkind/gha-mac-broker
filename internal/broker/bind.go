// Package broker orchestrates just-in-time runner binds against warm Tart VMs.
// The Binder can clone, boot, and ready a VM (Warm), run a single ephemeral
// GitHub Actions job on it through the in-VM guest agent (RunJob), and tear it
// down (Teardown). Job execution goes over the guest agent's authenticated
// HTTP/2 channel, so a broker restart no longer kills a running job: the guest
// keeps the runner alive and the host re-adopts it. The one-shot tart-exec
// control channel still handles VM readiness, `tart ip`, and reading the
// per-boot guest token. BindOnce composes the steps into one synchronous call
// for the bind CLI. The pool drives Warm and Teardown directly; the webhook
// server drives RunJob.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/ghapp"
	"goodkind.io/gha-mac-broker/internal/golden"
	"goodkind.io/gha-mac-broker/internal/guestproto"
	"goodkind.io/gha-mac-broker/internal/tart"
)

// readinessTimeout bounds how long to wait for the guest vsock channel and the
// guest agent's Hello after boot.
const readinessTimeout = 90 * time.Second

// readinessInterval is the poll interval while waiting for readiness.
const readinessInterval = 2 * time.Second

// checkAliveTimeout bounds a warm VM liveness probe, one Reattach reconcile
// call, and each adoption Reattach, so one wedged guest cannot park the whole
// pass or block startup adoption. It is a var so tests can shorten it.
var checkAliveTimeout = 15 * time.Second

// drainTimeout bounds the best-effort guest Drain before a VM teardown and the
// recycle/cancel teardown drain, so a dead or unreachable VM cannot spin either
// forever. It is a var so tests can shorten it.
var drainTimeout = 30 * time.Second

// drainReconnectBackoff paces reconnect attempts when a status stream drops
// before its terminal event.
const drainReconnectBackoff = time.Second

// slotReconfigureTimeout bounds the wait for a guest to serve the requested slot
// count after ConfigureSlots. The guest applies the count by replacing its
// worker, which is asynchronous, so the host re-Hellos until the inventory
// covers every configured slot or this deadline elapses. It is a var so tests
// can shorten it.
var slotReconfigureTimeout = 30 * time.Second

// slotReconfigureInterval paces the re-Hello poll while the guest worker
// replacement completes.
var slotReconfigureInterval = 500 * time.Millisecond

// guestAgentPort is the fixed TCP port the guest agent listens on inside the VM.
const guestAgentPort = "53931"

// hostProtocolMajor is the guest-agent protocol version this host speaks. A
// mismatch means the VM runs an incompatible agent and is skipped.
const hostProtocolMajor = uint32(1)

// jobStatusStream is the subset of a guest JobStatus server stream drainJob
// consumes. *connect.ServerStreamForClient satisfies it.
type jobStatusStream interface {
	Receive() bool
	Msg() *guestproto.JobStatusEvent
	Err() error
	Close() error
}

// guestConn is the host-side guest-agent surface the binder depends on. The
// production implementation wraps *guestclient.Client; tests stub it.
type guestConn interface {
	Hello(ctx context.Context) (*guestproto.HelloResponse, error)
	RunJob(ctx context.Context, request *guestproto.RunJobRequest) (*guestproto.RunJobResponse, error)
	JobStatus(ctx context.Context, executionID string, fromSequence uint64) (jobStatusStream, error)
	Reattach(ctx context.Context) (*guestproto.ReattachResponse, error)
	Drain(ctx context.Context) (*guestproto.DrainResponse, error)
	CancelJob(ctx context.Context, executionID string) error
	ConfigureSlots(ctx context.Context, slotCount uint32) (*guestproto.ConfigureSlotsResponse, error)
}

// WarmVM is a booted VM whose guest agent answers Hello. Name is safe to read
// from any goroutine once Warm returns. The cached guest endpoint is resolved
// once (during Warm or Adopt, single threaded) and read on the hot path.
type WarmVM struct {
	// Name is the tart VM name used for exec, stop, and delete.
	Name string
	// Image is the approved Cirrus tag this VM was cloned for.
	Image string
	boot  *exec.Cmd
	// guestMu guards the cached guest endpoint fields.
	guestMu   sync.Mutex
	guestAddr string
	guestConn guestConn
}

// SlotBinding is one busy runner slot discovered on a running VM during adoption.
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

// HasJobMetadata reports whether an adopted binding names a GitHub job and run.
func (binding SlotBinding) HasJobMetadata() bool {
	return binding.JobID > 0 && binding.RunID > 0
}

// AdoptedVM is a running pool VM discovered during broker startup.
type AdoptedVM struct {
	VM    *WarmVM
	Slots []SlotBinding
}

// Binder performs JIT runner binds against a warm VM substrate. Job execution
// and adoption go over the guest agent; VM lifecycle goes over tart.
type Binder struct {
	cfgMu sync.RWMutex
	cfg   *config.Config
	gh    *ghapp.Client
	vm    *tart.Tart
	// dialGuest builds a guest-agent client for an address and token. It is a
	// field so tests can stub the transport.
	dialGuest func(ctx context.Context, address string, token string) guestConn
	// guestFor resolves and caches the guest client for a VM. It is the single
	// seam tests override to return a stub without a real VM.
	guestFor func(ctx context.Context, vm *WarmVM) (guestConn, error)
}

// New builds a Binder from its collaborators.
func New(cfg *config.Config, gh *ghapp.Client, vm *tart.Tart) *Binder {
	binder := &Binder{
		cfgMu:     sync.RWMutex{},
		cfg:       cfg,
		gh:        gh,
		vm:        vm,
		dialGuest: nil,
		guestFor:  nil,
	}
	binder.dialGuest = newGuestClientAdapter
	binder.guestFor = binder.resolveGuest
	return binder
}

// Reconfigure swaps the config used by future broker operations.
func (b *Binder) Reconfigure(cfg *config.Config) {
	b.cfgMu.Lock()
	defer b.cfgMu.Unlock()
	b.cfg = cfg
}

func (b *Binder) configSnapshot() *config.Config {
	b.cfgMu.RLock()
	defer b.cfgMu.RUnlock()
	return b.cfg
}

// Warm clones the golden image, boots the VM, waits for the tart-exec channel,
// confirms the guest agent answers Hello (caching its endpoint), and verifies
// its slot inventory. On any failure it tears the partial VM down; the caller
// owns teardown only on success.
func (b *Binder) Warm(ctx context.Context, image string, id string, slotCount int) (*WarmVM, error) {
	slotCount = normalizeSlotCount(slotCount)
	cfg := b.configSnapshot()
	vmName := cfg.Tart.VMNamePrefix + "-" + id
	slog.InfoContext(ctx, "warming vm", "vm", vmName, "image", image, "slot_count", slotCount)

	goldenName, err := golden.New(b.vm).EnsureGolden(ctx, golden.EnsureOptions{
		Image:         image,
		BuildVM:       golden.NameForImage(image) + "-build-" + id,
		RunnerVersion: "",
	})
	if err != nil {
		slog.ErrorContext(ctx, "ensure golden failed", "err", err, "image", image)
		return nil, fmt.Errorf("broker: ensure golden for %s: %w", image, err)
	}

	// Idempotent clone: best-effort delete any pre-existing VM of this exact name
	// before cloning, so the clone self-heals even if the startup sweep missed a
	// VM or a same-instant run-token clash leaves a stale name.
	if err := b.vm.Delete(ctx, vmName); err != nil {
		slog.DebugContext(ctx, "pre-clone delete returned error (ignored)", "err", err, "vm", vmName)
	}

	if err := b.vm.Clone(ctx, goldenName, vmName, false); err != nil {
		slog.ErrorContext(ctx, "clone failed", "err", err, "vm", vmName)
		return nil, fmt.Errorf("broker: clone %s: %w", vmName, err)
	}

	bootCmd := b.bootCommand(ctx, cfg, vmName)
	if err := bootCmd.Start(); err != nil {
		slog.ErrorContext(ctx, "vm boot failed", "err", err, "vm", vmName)
		b.teardown(ctx, vmName)
		return nil, fmt.Errorf("broker: boot %s: %w", vmName, err)
	}
	b.reapBootCommand(context.WithoutCancel(ctx), vmName, bootCmd)

	if err := b.waitForReady(ctx, vmName); err != nil {
		_ = bootCmd.Process.Kill()
		b.teardown(ctx, vmName)
		return nil, err
	}

	vm := &WarmVM{Name: vmName, Image: image, boot: bootCmd, guestMu: sync.Mutex{}, guestAddr: "", guestConn: nil}
	// Confirm the guest agent is up and cache its endpoint before serving. This
	// replaces the old post-boot runner-slot clone as the readiness gate.
	client, err := b.guestFor(ctx, vm)
	if err != nil {
		_ = bootCmd.Process.Kill()
		b.teardown(ctx, vmName)
		return nil, err
	}
	// Ask the guest to serve the configured slot count, then reject it if the
	// advertised inventory still does not cover every slot, so a later RunJob never
	// targets a slot the guest does not have. A fresh guest boots with one slot and
	// grows to the configured count here.
	if err := b.ensureSlots(ctx, client, vmName, slotCount); err != nil {
		_ = bootCmd.Process.Kill()
		b.teardown(ctx, vmName)
		return nil, err
	}

	return vm, nil
}

// ensureSlots asks the guest to serve slotCount slots, then confirms the
// advertised inventory covers them. A fresh guest booted with the default single
// slot grows to slotCount; a guest a prior host already configured is a no-op.
// The guest applies the count by replacing its worker, which is asynchronous, so
// verifySlotInventory is re-Hello-polled until the new worker is current or the
// deadline elapses. A guest that rejects the request (a shrink below a running
// slot) surfaces the error here so the caller leaves the VM as is.
func (b *Binder) ensureSlots(ctx context.Context, client guestConn, vmName string, slotCount int) error {
	if _, err := client.ConfigureSlots(ctx, uint32(slotCount)); err != nil { // #nosec G115 -- slotCount is normalized to at least 1.
		slog.WarnContext(ctx, "configure slots failed", "err", err, "vm", vmName, "slot_count", slotCount)
		return fmt.Errorf("broker: configure %d slots on %s: %w", slotCount, vmName, err)
	}
	ctx, cancel := context.WithTimeout(ctx, slotReconfigureTimeout)
	defer cancel()
	ticker := time.NewTicker(slotReconfigureInterval)
	defer ticker.Stop()
	for {
		err := b.verifySlotInventory(ctx, client, vmName, slotCount)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-ticker.C:
		}
	}
}

// verifySlotInventory rejects a guest whose advertised slot inventory does not
// cover the configured slot count, so slots 0..slotCount-1 all exist before the
// VM serves or is adopted.
func (b *Binder) verifySlotInventory(ctx context.Context, client guestConn, vmName string, slotCount int) error {
	response, err := client.Hello(ctx)
	if err != nil {
		slog.WarnContext(ctx, "slot inventory hello failed", "err", err, "vm", vmName)
		return fmt.Errorf("broker: slot inventory hello on %s: %w", vmName, err)
	}
	advertised := make(map[uint32]struct{}, len(response.GetSlots()))
	for _, slot := range response.GetSlots() {
		advertised[slot.GetIndex()] = struct{}{}
	}
	// Require every configured index 0..slotCount-1 to be present, not just a
	// matching count, so a sparse or duplicate inventory (say [1, 2] or [0, 0])
	// cannot pass while host dispatch still targets a missing slot.
	for slotIndex := range slotCount {
		if _, ok := advertised[uint32(slotIndex)]; !ok { // #nosec G115 -- slot index is bounded and non-negative.
			slog.WarnContext(ctx, "guest missing configured slot", "vm", vmName, "missing_slot", slotIndex, "advertised", len(advertised), "want", slotCount)
			return fmt.Errorf("broker: guest %s missing slot %d of %d configured slots", vmName, slotIndex, slotCount)
		}
	}
	return nil
}

func (b *Binder) reapBootCommand(ctx context.Context, vmName string, bootCmd *exec.Cmd) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "vm boot reaper panic recovered", "err", fmt.Errorf("panic: %v", r), "vm", vmName)
			}
		}()
		if err := bootCmd.Wait(); err != nil {
			slog.DebugContext(ctx, "vm boot process exited", "err", err, "vm", vmName)
		}
	}()
	return done
}

// RunJob mints a JIT config and runs one ephemeral GitHub Actions job on the
// warm VM through the guest agent. It dispatches the execution, then drains its
// status stream to the terminal result. runnerName is the runner registration
// name. It blocks until the job terminates or the context is canceled.
func (b *Binder) RunJob(ctx context.Context, vm *WarmVM, repo string, runnerName string, slotIndex int, slotCount int, jobID int64, runID int64, _ time.Time) error {
	owner, repoName, ok := strings.Cut(repo, "/")
	if !ok {
		return fmt.Errorf("broker: repo must be owner/repo, got %q", repo)
	}
	execID := executionID(repo, runID, jobID)

	client, err := b.guestFor(ctx, vm)
	if err != nil {
		return err
	}

	jit, err := b.generateJIT(ctx, owner, repoName, runnerName)
	if err != nil {
		return err
	}

	request := &guestproto.RunJobRequest{
		ExecutionId: execID,
		// slotIndex is a small non-negative slot number, so the conversion is safe.
		Slot:      uint32(slotIndex), // #nosec G115 -- slot index is bounded and non-negative.
		JitConfig: jit.EncodedJITConfig,
		Env:       gitCredentialEnv(),
		Meta: &guestproto.JobMeta{
			Repo:       repo,
			JobId:      jobID,
			RunId:      runID,
			RunnerName: jit.Runner.Name,
		},
	}
	slog.InfoContext(ctx, "dispatching ephemeral job", "repo", repo, "vm", vm.Name, "runner", jit.Runner.Name, "slot", slotIndex, "execution", execID)
	response, err := client.RunJob(ctx, request)
	if err != nil {
		slog.ErrorContext(ctx, "job dispatch failed", "err", err, "vm", vm.Name, "slot", slotIndex, "execution", execID)
		return fmt.Errorf("broker: dispatch job on %s: %w", vm.Name, err)
	}
	switch response.GetOutcome() {
	case guestproto.RunJobResponse_ACCEPTED,
		guestproto.RunJobResponse_ALREADY_RUNNING,
		guestproto.RunJobResponse_ALREADY_COMPLETED:
		// Dispatch has no prior cursor, so replay the stream from the start; an
		// already-running or already-completed execution replays to its terminal.
	case guestproto.RunJobResponse_CONFLICT:
		return fmt.Errorf("broker: guest reported execution conflict for %s on %s", execID, vm.Name)
	case guestproto.RunJobResponse_OUTCOME_UNSPECIFIED:
		return fmt.Errorf("broker: guest returned unspecified outcome for %s on %s", execID, vm.Name)
	default:
		return fmt.Errorf("broker: guest returned unknown outcome %d for %s on %s", response.GetOutcome(), execID, vm.Name)
	}
	return b.drainJob(ctx, client, execID, 0, vm, slotIndex, slotCount)
}

// ResumeJob re-attaches to an adopted execution and drains it from its cursor,
// so a busy slot inherited across a broker restart resolves to its terminal.
func (b *Binder) ResumeJob(ctx context.Context, vm *WarmVM, executionID string, fromCursor uint64, slotIndex int, slotCount int) error {
	client, err := b.guestFor(ctx, vm)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "resuming adopted job", "vm", vm.Name, "slot", slotIndex, "execution", executionID, "cursor", fromCursor)
	return b.drainJob(ctx, client, executionID, fromCursor, vm, slotIndex, slotCount)
}

type cancelAction int

const (
	cancelNone cancelAction = iota
	cancelDetach
	cancelTeardown
)

// classifyCancel reads the drain context's cancellation cause. A plain
// [context.Canceled] or [context.DeadlineExceeded] is a worker shutdown, so the
// job detaches and is re-adopted later; any custom cause is an explicit recycle
// or cancel-run that must cancel the guest job and drain it to its terminal.
func classifyCancel(ctx context.Context) cancelAction {
	if ctx.Err() == nil {
		return cancelNone
	}
	cause := context.Cause(ctx)
	if cause != nil && !errors.Is(cause, context.Canceled) && !errors.Is(cause, context.DeadlineExceeded) {
		return cancelTeardown
	}
	return cancelDetach
}

// drainJob streams execution status from fromSeq, mirroring log chunks to the
// run log and tracking the cursor so a dropped stream reconnects from where it
// left off. A zero terminal exit maps to nil and a nonzero exit to an error,
// preserving the old run.sh semantics. An expired execution (NotFound) is a
// terminal-unknown outcome that frees the slot, not an error loop. A worker
// shutdown cancel detaches; a recycle or cancel-run cause cancels the guest job
// and keeps draining to the terminal.
func (b *Binder) drainJob(ctx context.Context, client guestConn, execID string, fromSeq uint64, vm *WarmVM, slotIndex int, slotCount int) error {
	runLog, closeLog := b.openDrainLog(ctx, vm, slotIndex, slotCount)
	defer closeLog()

	cursor := fromSeq
	canceled, err := b.drainToTerminal(ctx, client, execID, vm, runLog, &cursor)
	if !canceled {
		if err != nil {
			// The job reached a terminal with a nonzero exit. Mark it so the pool can
			// tell a terminal job failure (free the slot, keep the VM) from a resume
			// attach or drain failure (recycle the VM).
			return errors.Join(err, ErrJobTerminal)
		}
		return nil
	}
	if classifyCancel(ctx) == cancelDetach {
		slog.DebugContext(ctx, "detaching drain on worker shutdown", "vm", vm.Name, "execution", execID)
		return context.Canceled
	}
	// A recycle or cancel-run cause: cancel the guest job, then drain it to its
	// terminal on a detached, time-bounded context. The timeout guarantees a dead
	// or unreachable VM cannot spin the teardown drain forever, which would leave
	// the slot goroutine hung and block recycle and VM teardown.
	teardownCtx, cancelTeardown := context.WithTimeout(context.WithoutCancel(ctx), drainTimeout)
	defer cancelTeardown()
	if err := client.CancelJob(teardownCtx, execID); err != nil {
		slog.WarnContext(ctx, "guest cancel job failed", "err", err, "vm", vm.Name, "execution", execID)
	}
	timedOut, drainErr := b.drainToTerminal(teardownCtx, client, execID, vm, runLog, &cursor)
	if timedOut {
		slog.WarnContext(ctx, "teardown drain timed out; proceeding with recycle", "vm", vm.Name, "execution", execID)
		return fmt.Errorf("broker: teardown drain timed out for %s on %s", execID, vm.Name)
	}
	return drainErr
}

// openDrainLog opens the per-slot run log, or a no-op sink when it cannot be
// created, and returns a close function for the caller to defer.
func (b *Binder) openDrainLog(ctx context.Context, vm *WarmVM, slotIndex int, slotCount int) (*os.File, func()) {
	runLog, logPath, logErr := openRunLog(ctx, vm.Name, slotIndex, slotCount)
	if logErr != nil {
		slog.WarnContext(ctx, "run log open failed; draining without a log sink", "err", logErr, "vm", vm.Name, "path", logPath)
		return nil, func() {}
	}
	return runLog, func() {
		if err := runLog.Close(); err != nil {
			slog.WarnContext(ctx, "run log close failed", "err", err, "vm", vm.Name, "path", logPath)
		}
	}
}

// drainToTerminal reconnects from the cursor until the execution reaches a
// terminal result or expires, or ctx is canceled. It reports canceled=true when
// ctx is done so the caller can decide detach versus teardown, and otherwise
// returns the terminal outcome (nil for a zero exit or an expired execution).
func (b *Binder) drainToTerminal(ctx context.Context, client guestConn, execID string, vm *WarmVM, runLog *os.File, cursor *uint64) (bool, error) {
	for {
		if contextDone(ctx) {
			return true, nil
		}
		stream, err := client.JobStatus(ctx, execID, *cursor)
		if isExecutionNotFound(err) {
			slog.WarnContext(ctx, "guest execution not found; freeing slot", "vm", vm.Name, "execution", execID)
			return false, nil
		}
		if err != nil {
			slog.WarnContext(ctx, "guest job status open failed; retrying", "err", err, "vm", vm.Name, "execution", execID)
			sleepWithContext(ctx, drainReconnectBackoff)
			continue
		}
		terminal, streamErr := drainStream(ctx, stream, runLog, cursor)
		_ = stream.Close()
		if terminal != nil {
			return false, terminalToError(terminal, execID, vm)
		}
		if isExecutionNotFound(streamErr) {
			slog.WarnContext(ctx, "guest execution not found mid-stream; freeing slot", "vm", vm.Name, "execution", execID)
			return false, nil
		}
		if contextDone(ctx) {
			return true, nil
		}
		slog.WarnContext(ctx, "guest status stream dropped before terminal; reconnecting", "err", streamErr, "vm", vm.Name, "execution", execID, "cursor", *cursor)
		sleepWithContext(ctx, drainReconnectBackoff)
	}
}

// contextDone reports whether ctx has been canceled, as a boolean so cancel
// handling stays out of error-return branches.
func contextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// drainStream consumes one status stream, mirroring log chunks to runLog and
// advancing cursor on every event. It returns the terminal result if the stream
// reaches one, or the stream error when it ends early.
func drainStream(ctx context.Context, stream jobStatusStream, runLog *os.File, cursor *uint64) (*guestproto.TerminalResult, error) {
	for stream.Receive() {
		event := stream.Msg()
		*cursor = event.GetSequence()
		if chunk := event.GetLog(); chunk != nil && runLog != nil {
			// An evicted (nil-data) chunk is tolerated, not an error.
			if data := chunk.GetData(); len(data) > 0 {
				if _, err := runLog.Write(data); err != nil {
					slog.WarnContext(ctx, "run log write failed", "err", err)
				}
			}
		}
		if result := event.GetResult(); result != nil {
			return result, nil
		}
		// Heartbeats keep the stream alive; there is no host read deadline because
		// jobs run for hours. Stall detection lives in the reap loop, not here.
	}
	if err := stream.Err(); err != nil {
		return nil, fmt.Errorf("broker: guest status stream: %w", err)
	}
	return nil, nil
}

// sleepWithContext sleeps for delay unless ctx is done first.
func sleepWithContext(ctx context.Context, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// ErrJobTerminal marks an error that reports a guest job which reached its
// terminal result with a nonzero exit. The pool uses it to tell a normal adopted
// job failure (the job ran to a terminal) from a resume attach or drain failure
// (the inherited execution could never be reached), so it frees the slot instead
// of recycling a healthy VM.
var ErrJobTerminal = errors.New("broker: guest job reached terminal")

func terminalToError(terminal *guestproto.TerminalResult, execID string, vm *WarmVM) error {
	if terminal.GetExitCode() == 0 {
		return nil
	}
	return fmt.Errorf("broker: guest job %s on %s exited %d: %s", execID, vm.Name, terminal.GetExitCode(), terminal.GetMessage())
}

func isExecutionNotFound(err error) bool {
	return err != nil && connect.CodeOf(err) == connect.CodeNotFound
}

// executionID is the deterministic id for one workflow job, so a host restart
// recomputes the same id and re-attaches to the running guest execution.
func executionID(repo string, runID int64, jobID int64) string {
	return fmt.Sprintf("%s#%d#%d", repo, runID, jobID)
}

// gitCredentialEnv clears the git credential helper for the runner process tree
// so the Git Credential Manager is never invoked, since its store path can
// deadlock in the headless VM, and keeps a 401 failing fast.
func gitCredentialEnv() map[string]string {
	return map[string]string{
		"GIT_CONFIG_COUNT":    "1",
		"GIT_CONFIG_KEY_0":    "credential.helper",
		"GIT_CONFIG_VALUE_0":  "",
		"GIT_TERMINAL_PROMPT": "0",
	}
}

func openRunLog(ctx context.Context, vmName string, slotIndex int, slotCount int) (*os.File, string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		slog.WarnContext(ctx, "resolve home dir for run log failed", "err", err, "vm", vmName)
		return nil, "", fmt.Errorf("resolve home dir: %w", err)
	}
	logDir := filepath.Join(home, "Library", "Logs", "gha-mac-broker")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		slog.WarnContext(ctx, "create run log dir failed", "err", err, "vm", vmName, "path", logDir)
		return nil, logDir, fmt.Errorf("create run log dir: %w", err)
	}
	logName := vmName
	if slotCount > 1 {
		logName = fmt.Sprintf("%s-slot-%d", vmName, slotIndex)
	}
	logPath := filepath.Join(logDir, "run-"+safeLogName(logName)+".log")
	// Truncate rather than append: the pool reuses VM names across jobs, so
	// appending would grow run-<vm>.log without bound and interleave unrelated
	// jobs. Each job replaces the prior, leaving the latest job's output for a
	// post-mortem of a wedge on that VM.
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		slog.WarnContext(ctx, "open run log failed", "err", err, "vm", vmName, "path", logPath)
		return nil, logPath, fmt.Errorf("open run log: %w", err)
	}
	return file, logPath, nil
}

func safeLogName(name string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_")
	return replacer.Replace(name)
}

// Teardown best-effort drains the guest agent so in-flight executions get a
// chance to stop, kills the boot process if running, then stops and deletes the
// VM. It is best effort; errors are logged at Warn.
func (b *Binder) Teardown(ctx context.Context, vm *WarmVM) {
	if client, err := b.guestFor(context.WithoutCancel(ctx), vm); err == nil {
		drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), drainTimeout)
		if _, err := client.Drain(drainCtx); err != nil {
			slog.DebugContext(ctx, "guest drain before teardown failed", "err", err, "vm", vm.Name)
		}
		cancel()
	}
	if vm.boot != nil && vm.boot.Process != nil {
		_ = vm.boot.Process.Kill()
	}
	b.teardown(ctx, vm.Name)
}

// CheckAlive verifies that a cached warm VM's guest agent still answers Hello. It
// bounds the probe so one wedged guest cannot park the whole reconcile pass.
func (b *Binder) CheckAlive(ctx context.Context, vm *WarmVM) error {
	ctx, cancel := context.WithTimeout(ctx, checkAliveTimeout)
	defer cancel()
	client, err := b.guestFor(ctx, vm)
	if err != nil {
		slog.WarnContext(ctx, "warm vm guest resolve failed", "err", err, "vm", vm.Name)
		return fmt.Errorf("broker: check alive %s: %w", vm.Name, err)
	}
	if _, err := client.Hello(ctx); err != nil {
		slog.WarnContext(ctx, "warm vm hello probe failed", "err", err, "vm", vm.Name)
		return fmt.Errorf("broker: check alive %s: %w", vm.Name, err)
	}
	return nil
}

// RunningSlots reports which slots have a running guest execution, from a single
// Reattach call per VM. The reap and status paths use it in place of the old
// per-slot pgrep and CPU probes.
func (b *Binder) RunningSlots(ctx context.Context, vm *WarmVM) (map[int]bool, error) {
	ctx, cancel := context.WithTimeout(ctx, checkAliveTimeout)
	defer cancel()
	client, err := b.guestFor(ctx, vm)
	if err != nil {
		slog.WarnContext(ctx, "running slots guest resolve failed", "err", err, "vm", vm.Name)
		return nil, fmt.Errorf("broker: running slots resolve %s: %w", vm.Name, err)
	}
	response, err := client.Reattach(ctx)
	if err != nil {
		slog.WarnContext(ctx, "running slots reattach failed", "err", err, "vm", vm.Name)
		return nil, fmt.Errorf("broker: running slots reattach %s: %w", vm.Name, err)
	}
	running := make(map[int]bool)
	for _, state := range response.GetExecutions() {
		if state.GetRunning() {
			running[int(state.GetSlot())] = true
		}
	}
	return running, nil
}

// List returns the Tart VM names visible to the broker host.
func (b *Binder) List(ctx context.Context) ([]string, error) {
	names, err := b.vm.List(ctx)
	if err != nil {
		slog.WarnContext(ctx, "list tart vms failed", "err", err)
		return nil, fmt.Errorf("broker: list tart vms: %w", err)
	}
	return names, nil
}

// Adopt discovers already-running pool VMs and returns the subset this broker
// should manage. It resolves each VM's guest agent (skipping a VM whose agent is
// dead or incompatible), reads its running executions via Reattach, and marks a
// slot busy when a running execution names it. It does not clone, delete, or
// rewrite runner-slot directories.
func (b *Binder) Adopt(ctx context.Context, image string, slotCount int, limit int) ([]AdoptedVM, error) {
	entries, err := b.vm.ListVMs(ctx)
	if err != nil {
		slog.WarnContext(ctx, "list tart vms failed", "err", err)
		return nil, fmt.Errorf("broker: list tart vms for adoption: %w", err)
	}
	cfg := b.configSnapshot()
	prefix := cfg.Tart.VMNamePrefix + "-"
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name, prefix) {
			continue
		}
		if !strings.EqualFold(entry.State, "running") {
			continue
		}
		names = append(names, entry.Name)
	}
	sort.Strings(names)
	type adoptionCandidate struct {
		vm    *WarmVM
		slots []SlotBinding
	}
	busyCandidates := make([]adoptionCandidate, 0, len(names))
	idleCandidates := make([]adoptionCandidate, 0, len(names))
	normalizedSlotCount := normalizeSlotCount(slotCount)
	for _, name := range names {
		vm := &WarmVM{Name: name, Image: image, boot: nil, guestMu: sync.Mutex{}, guestAddr: "", guestConn: nil}
		client, err := b.guestFor(ctx, vm)
		if err != nil {
			slog.WarnContext(ctx, "skip running vm adoption after guest resolve failure", "err", err, "vm", name)
			continue
		}
		if err := b.ensureSlots(ctx, client, name, normalizedSlotCount); err != nil {
			slog.WarnContext(ctx, "skip running vm adoption after slot inventory check", "err", err, "vm", name)
			continue
		}
		// Bound each adoption Reattach so one unresponsive VM cannot block startup
		// adoption and every following candidate indefinitely.
		reattachCtx, cancelReattach := context.WithTimeout(ctx, checkAliveTimeout)
		response, err := client.Reattach(reattachCtx)
		cancelReattach()
		if err != nil {
			slog.WarnContext(ctx, "skip running vm adoption after reattach failure", "err", err, "vm", name)
			continue
		}
		candidate := adoptionCandidate{
			vm:    vm,
			slots: reattachSlots(response, normalizedSlotCount),
		}
		if len(candidate.slots) > 0 {
			busyCandidates = append(busyCandidates, candidate)
			continue
		}
		idleCandidates = append(idleCandidates, candidate)
	}
	selected := make([]adoptionCandidate, 0, len(busyCandidates)+len(idleCandidates))
	addCandidate := func(candidate adoptionCandidate) bool {
		if limit >= 0 && len(selected) >= limit {
			return false
		}
		selected = append(selected, candidate)
		return true
	}
	for _, candidate := range busyCandidates {
		addCandidate(candidate)
	}
	for _, candidate := range idleCandidates {
		if addCandidate(candidate) {
			continue
		}
		b.teardown(context.WithoutCancel(ctx), candidate.vm.Name)
	}
	adopted := make([]AdoptedVM, 0, len(selected))
	for _, candidate := range selected {
		adopted = append(adopted, AdoptedVM{
			VM:    candidate.vm,
			Slots: candidate.slots,
		})
	}
	return adopted, nil
}

// reattachSlots turns a Reattach response into the busy slot bindings for a VM.
// Only running executions occupy a slot; retained terminals do not.
func reattachSlots(response *guestproto.ReattachResponse, slotCount int) []SlotBinding {
	bindings := make([]SlotBinding, 0, slotCount)
	for _, state := range response.GetExecutions() {
		if !state.GetRunning() {
			continue
		}
		slotIndex := int(state.GetSlot())
		if slotIndex < 0 || slotIndex >= slotCount {
			continue
		}
		meta := state.GetMeta()
		bindings = append(bindings, SlotBinding{
			SlotIndex:      slotIndex,
			Repo:           meta.GetRepo(),
			JobID:          meta.GetJobId(),
			RunID:          meta.GetRunId(),
			ExecutionID:    state.GetExecutionId(),
			ResumeCursor:   state.GetLastSequence(),
			BoundAt:        time.Time{},
			ObservedActive: true,
		})
	}
	return bindings
}

// DeleteGolden removes the derived golden for image from disk.
func (b *Binder) DeleteGolden(ctx context.Context, image string) error {
	goldenName := golden.NameForImage(image)
	if err := b.vm.Delete(ctx, goldenName); err != nil {
		slog.WarnContext(ctx, "delete golden failed", "err", err, "golden", goldenName, "image", image)
		return fmt.Errorf("broker: delete golden %s: %w", goldenName, err)
	}
	return nil
}

// SweepOrphans stops and deletes leftover pool VMs. Startup no longer calls it:
// the pool adopts live VMs instead so broker restarts do not kill jobs. This is
// retained as a manual cleanup primitive for callers that explicitly want VM
// teardown.
func (b *Binder) SweepOrphans(ctx context.Context) {
	names, err := b.vm.List(ctx)
	if err != nil {
		return
	}
	cfg := b.configSnapshot()
	prefix := cfg.Tart.VMNamePrefix + "-"
	swept := 0
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		slog.DebugContext(ctx, "orphan sweep: tearing down stale vm", "vm", name)
		b.teardown(ctx, name)
		swept++
	}
	if swept > 0 {
		slog.InfoContext(ctx, "orphan sweep complete", "count", swept)
	}
}

// BindOnce clones a warm VM, registers it as an ephemeral runner for repo, runs
// one job, and tears the VM down. id makes the VM and runner names unique.
func (b *Binder) BindOnce(ctx context.Context, repo, id string) error {
	if _, _, ok := strings.Cut(repo, "/"); !ok {
		return fmt.Errorf("broker: repo must be owner/repo, got %q", repo)
	}
	cfg := b.configSnapshot()
	jobsPerVM := normalizedJobsPerVM(cfg)
	vm, err := b.Warm(ctx, cfg.Tart.BaseImage, id, jobsPerVM)
	if err != nil {
		return err
	}
	defer b.Teardown(ctx, vm)
	return b.RunJob(ctx, vm, repo, vm.Name, 0, jobsPerVM, 0, 0, time.Time{})
}

func normalizedJobsPerVM(cfg *config.Config) int {
	if cfg == nil || cfg.JobsPerVM < 1 {
		return 1
	}
	return cfg.JobsPerVM
}

func normalizeSlotCount(slotCount int) int {
	if slotCount < 1 {
		return 1
	}
	return slotCount
}

// bootCommand builds the headless boot command with the cache dir mounted.
func (b *Binder) bootCommand(ctx context.Context, cfg *config.Config, vmName string) *exec.Cmd {
	var dirs []tart.DirMount
	if cfg.Tart.CacheDir != "" {
		// tart --dir requires the host path to exist, so create it before the
		// mount. MkdirAll is idempotent and cheap on the warm path.
		if err := os.MkdirAll(cfg.Tart.CacheDir, 0o700); err != nil {
			slog.WarnContext(ctx, "create cache dir failed; booting without cache mount", "err", err, "dir", cfg.Tart.CacheDir)
		} else {
			// Chmod after MkdirAll: MkdirAll applies 0700 only to dirs it creates,
			// so tighten an existing looser dir too. The build cache can hold
			// proprietary source and artifacts, so keep it private to the owner on a
			// multi-user host.
			if err := os.Chmod(cfg.Tart.CacheDir, 0o700); err != nil {
				slog.WarnContext(ctx, "chmod cache dir failed; continuing with existing perms", "err", err, "dir", cfg.Tart.CacheDir)
			}
			dirs = []tart.DirMount{{Name: "cache", Path: cfg.Tart.CacheDir}}
		}
	}
	return b.vm.BootCommand(ctx, vmName, tart.BootOptions{NoGraphics: true, Detach: true, Dirs: dirs})
}

// generateJIT mints the repo-scoped JIT config for one job.
func (b *Binder) generateJIT(ctx context.Context, owner, repoName, runnerName string) (*ghapp.JITConfig, error) {
	cfg := b.configSnapshot()
	installationID, err := b.gh.InstallationID(ctx, owner, repoName)
	if err != nil {
		slog.ErrorContext(ctx, "installation lookup failed", "err", err, "repo", owner+"/"+repoName)
		return nil, fmt.Errorf("broker: installation lookup: %w", err)
	}
	token, err := b.gh.InstallationToken(ctx, installationID, repoName)
	if err != nil {
		slog.ErrorContext(ctx, "installation token failed", "err", err, "repo", owner+"/"+repoName)
		return nil, fmt.Errorf("broker: installation token: %w", err)
	}
	jit, err := b.gh.GenerateJITConfig(ctx, token, owner, repoName, runnerName, cfg.Labels)
	if err != nil {
		slog.ErrorContext(ctx, "generate jitconfig failed", "err", err, "repo", owner+"/"+repoName)
		return nil, fmt.Errorf("broker: generate jitconfig: %w", err)
	}
	return jit, nil
}

// waitForReady polls the guest vsock channel (`tart exec <vm> true`) until it
// answers or the timeout elapses. The token read and Hello then confirm the
// guest agent itself is up.
func (b *Binder) waitForReady(ctx context.Context, vmName string) error {
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()
	ticker := time.NewTicker(readinessInterval)
	defer ticker.Stop()
	for {
		if _, err := b.vm.Exec(ctx, vmName, "true"); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			slog.ErrorContext(ctx, "timed out waiting for vsock readiness", "err", ctx.Err(), "vm", vmName)
			return fmt.Errorf("broker: waiting for vsock readiness of %s: %w", vmName, ctx.Err())
		case <-ticker.C:
		}
	}
}

// teardown stops and deletes a VM, best effort.
func (b *Binder) teardown(ctx context.Context, vmName string) {
	if err := b.vm.Stop(ctx, vmName); err != nil {
		slog.WarnContext(ctx, "vm stop failed", "err", err, "vm", vmName)
	}
	if err := b.vm.Delete(ctx, vmName); err != nil {
		slog.WarnContext(ctx, "vm delete failed", "err", err, "vm", vmName)
	}
}
