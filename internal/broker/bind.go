// Package broker orchestrates just-in-time runner binds against warm Tart VMs.
// The Binder can clone, boot, and ready a VM (Warm), run a single ephemeral
// GitHub Actions job on it (RunJob), and tear it down (Teardown). All guest
// control goes over the tart-exec vsock channel, with no IP and no SSH.
// BindOnce composes these steps into one synchronous call for the bind CLI. The
// pool drives Warm and Teardown directly; the webhook server drives RunJob.
package broker

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/ghapp"
	"goodkind.io/gha-mac-broker/internal/golden"
	"goodkind.io/gha-mac-broker/internal/tart"
)

// readinessTimeout bounds how long to wait for the guest vsock channel after boot.
const readinessTimeout = 90 * time.Second

// readinessInterval is the poll interval while waiting for readiness.
const readinessInterval = 2 * time.Second

// touchInterval is how often the broker refreshes each warm VM's liveness file
// over vsock. The guest watchdog's stale timeout must exceed this.
const touchInterval = 10 * time.Second

// heartbeatFile is the guest path the broker touches on a timer; the baked guest
// watchdog logs when this file goes stale.
const heartbeatFile = "/tmp/gha-broker.alive"

// slotBindingFilePrefix is the guest-side prefix for per-slot job metadata.
const slotBindingFilePrefix = "/tmp/gha-broker-slot-"

// runnerHome is where the golden image keeps the GitHub Actions runner.
const runnerHome = "~/actions-runner"

//go:embed guest/clone-runner-slots.sh
var cloneRunnerSlotsScript string

//go:embed guest/run-slot-job.sh
var runSlotJobScript string

// activeJobProbeTimeout bounds /status guest process checks.
const activeJobProbeTimeout = 5 * time.Second

// activeJobProbeScript prints "yes" when a Runner.Worker is running, "no" only
// when pgrep exits 1 (no match), and otherwise propagates pgrep's nonzero exit
// so a real probe failure surfaces as an Exec error rather than a false "no".
// A masked error would let the pickup-timeout reap path drop a healthy worker.
const activeJobProbeScript = "pgrep -f '[R]unner\\.Worker' >/dev/null 2>&1; rc=$?; if [ \"$rc\" -eq 0 ]; then echo yes; elif [ \"$rc\" -eq 1 ]; then echo no; else exit \"$rc\"; fi"

type activeJobProbeResult string

const (
	activeJobProbeResultYes activeJobProbeResult = "yes"
	activeJobProbeResultNo  activeJobProbeResult = "no"
)

// WarmVM is a booted, vsock-ready VM that has not yet been bound to a job. Name
// is safe to read from any goroutine once Warm returns.
type WarmVM struct {
	// Name is the tart VM name used for exec, stop, and delete.
	Name string
	// Image is the approved Cirrus tag this VM was cloned for.
	Image string
	boot  *exec.Cmd
	// stopTouch ends the per-VM liveness touch loop on teardown.
	stopTouch context.CancelFunc
}

// SlotBinding is the guest-persisted job binding for one VM slot.
type SlotBinding struct {
	SlotIndex      int       `json:"slot_index"`
	Repo           string    `json:"repo"`
	JobID          int64     `json:"job_id"`
	RunID          int64     `json:"run_id"`
	BoundAt        time.Time `json:"bound_at"`
	ObservedActive bool      `json:"-"`
}

// HasJobMetadata reports whether a persisted binding names a GitHub job and run.
func (binding SlotBinding) HasJobMetadata() bool {
	return binding.JobID > 0 && binding.RunID > 0
}

// AdoptedVM is a running pool VM discovered during broker startup.
type AdoptedVM struct {
	VM    *WarmVM
	Slots []SlotBinding
}

// Binder performs JIT runner binds against a warm VM substrate over vsock.
type Binder struct {
	cfgMu sync.RWMutex
	cfg   *config.Config
	gh    *ghapp.Client
	vm    *tart.Tart
}

// New builds a Binder from its collaborators.
func New(cfg *config.Config, gh *ghapp.Client, vm *tart.Tart) *Binder {
	return &Binder{cfgMu: sync.RWMutex{}, cfg: cfg, gh: gh, vm: vm}
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

// Warm clones the golden image to a prefixed name derived from id, boots the VM,
// waits until its vsock channel answers, and starts the liveness touch loop. On
// any failure, Warm tears down the partial VM before returning; the caller owns
// teardown only on success.
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

	// Idempotent clone: best-effort delete any pre-existing VM of this exact
	// name before cloning, so the clone self-heals even if the startup sweep
	// missed a VM or a same-instant run-token clash leaves a stale name. A
	// "does not exist" error is the normal case and is logged at debug only.
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

	if err := b.cloneRunnerSlots(ctx, vmName, slotCount); err != nil {
		_ = bootCmd.Process.Kill()
		b.teardown(ctx, vmName)
		return nil, err
	}

	stopTouch := b.startTouchLoop(ctx, vmName)
	return &WarmVM{Name: vmName, Image: image, boot: bootCmd, stopTouch: stopTouch}, nil
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

func (b *Binder) startTouchLoop(ctx context.Context, vmName string) context.CancelFunc {
	touchCtx, stopTouch := context.WithCancel(context.WithoutCancel(ctx))
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(touchCtx, "touch loop panic recovered", "err", fmt.Errorf("panic: %v", r), "vm", vmName)
			}
		}()
		b.touchLoop(touchCtx, vmName)
	}()
	return stopTouch
}

func (b *Binder) cloneRunnerSlots(ctx context.Context, vmName string, slotCount int) error {
	remote := cloneRunnerSlotsCommand(normalizeSlotCount(slotCount))
	if remote == "" {
		return nil
	}
	if _, err := b.vm.Exec(ctx, vmName, "bash", "-lc", remote); err != nil {
		slog.ErrorContext(ctx, "runner slot clone failed", "err", err, "vm", vmName)
		return fmt.Errorf("broker: clone runner slots on %s: %w", vmName, err)
	}
	return nil
}

// RunJob mints a JIT config and runs one ephemeral GitHub Actions job on the
// warm VM over vsock. runnerName is the runner registration name.
func (b *Binder) RunJob(ctx context.Context, vm *WarmVM, repo string, runnerName string, slotIndex int, slotCount int, jobID int64, runID int64, boundAt time.Time) (err error) {
	owner, repoName, ok := strings.Cut(repo, "/")
	if !ok {
		return fmt.Errorf("broker: repo must be owner/repo, got %q", repo)
	}

	binding := SlotBinding{
		SlotIndex:      slotIndex,
		Repo:           repo,
		JobID:          jobID,
		RunID:          runID,
		BoundAt:        boundAt.UTC(),
		ObservedActive: false,
	}
	if err := b.writeSlotBinding(ctx, vm.Name, binding); err != nil {
		return err
	}
	jobCompleted := false
	defer func() {
		if runJobInterruptedByCancellation(ctx, err, jobCompleted) {
			return
		}
		b.clearSlotBinding(context.WithoutCancel(ctx), vm.Name, slotIndex)
	}()

	jit, err := b.generateJIT(ctx, owner, repoName, runnerName)
	if err != nil {
		return err
	}

	remote := runJobRemoteCommand(jit.EncodedJITConfig, slotIndex, slotCount)
	slog.InfoContext(ctx, "running ephemeral job", "repo", repo, "vm", vm.Name, "runner", jit.Runner.Name, "slot", slotIndex)
	runLog, runLogPath, err := openRunLog(ctx, vm.Name, slotIndex, slotCount)
	if err != nil {
		slog.WarnContext(ctx, "run log open failed; using buffered exec", "err", err, "vm", vm.Name, "path", runLogPath)
		if _, err := b.vm.Exec(ctx, vm.Name, "bash", "-lc", remote); err != nil {
			slog.ErrorContext(ctx, "job run failed", "err", err, "vm", vm.Name, "slot", slotIndex)
			return fmt.Errorf("broker: run job on %s: %w", vm.Name, err)
		}
		jobCompleted = true
		slog.InfoContext(ctx, "job complete", "repo", repo, "vm", vm.Name, "slot", slotIndex)
		return nil
	}
	defer func() {
		if err := runLog.Close(); err != nil {
			slog.WarnContext(ctx, "run log close failed", "err", err, "vm", vm.Name, "path", runLogPath)
		}
	}()
	if _, err := b.vm.ExecTee(ctx, vm.Name, runLog, "bash", "-lc", remote); err != nil {
		slog.ErrorContext(ctx, "job run failed", "err", err, "vm", vm.Name, "slot", slotIndex)
		return fmt.Errorf("broker: run job on %s: %w", vm.Name, err)
	}
	jobCompleted = true
	slog.InfoContext(ctx, "job complete", "repo", repo, "vm", vm.Name, "slot", slotIndex)
	return nil
}

func runJobInterruptedByCancellation(ctx context.Context, err error, jobCompleted bool) bool {
	if jobCompleted {
		return false
	}
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return true
	}
	return ctx.Err() != nil
}

func (b *Binder) writeSlotBinding(ctx context.Context, vmName string, binding SlotBinding) error {
	data, err := json.Marshal(binding)
	if err != nil {
		return fmt.Errorf("broker: marshal slot binding: %w", err)
	}
	path := slotBindingPath(binding.SlotIndex)
	remote := fmt.Sprintf("cat > %s <<'EOF'\n%s\nEOF\n", shellQuote(path), string(data))
	if _, err := b.vm.Exec(ctx, vmName, "bash", "-lc", remote); err != nil {
		slog.WarnContext(ctx, "slot binding write failed", "err", err, "vm", vmName, "slot", binding.SlotIndex)
		return fmt.Errorf("broker: write slot binding on %s slot %d: %w", vmName, binding.SlotIndex, err)
	}
	return nil
}

func (b *Binder) clearSlotBinding(ctx context.Context, vmName string, slotIndex int) {
	if _, err := b.vm.Exec(ctx, vmName, "rm", "-f", slotBindingPath(slotIndex)); err != nil {
		slog.DebugContext(ctx, "slot binding clear failed", "err", err, "vm", vmName, "slot", slotIndex)
	}
}

func (b *Binder) readSlotBinding(ctx context.Context, vmName string, slotIndex int) (SlotBinding, bool, error) {
	var zero SlotBinding
	path := shellQuote(slotBindingPath(slotIndex))
	remote := fmt.Sprintf("if [[ -f %s ]]; then cat %s; fi", path, path)
	out, err := b.vm.Exec(ctx, vmName, "bash", "-lc", remote)
	if err != nil {
		slog.WarnContext(ctx, "slot binding read failed", "err", err, "vm", vmName, "slot", slotIndex)
		return zero, false, fmt.Errorf("broker: read slot binding on %s slot %d: %w", vmName, slotIndex, err)
	}
	trimmed := strings.TrimSpace(string(out))
	if trimmed == "" {
		return zero, false, nil
	}
	var binding SlotBinding
	if err := json.Unmarshal([]byte(trimmed), &binding); err != nil {
		slog.WarnContext(ctx, "slot binding parse failed", "err", err, "vm", vmName, "slot", slotIndex)
		return zero, false, fmt.Errorf("broker: parse slot binding on %s slot %d: %w", vmName, slotIndex, err)
	}
	if binding.SlotIndex != slotIndex {
		err := fmt.Errorf("broker: slot binding index on %s = %d, want %d", vmName, binding.SlotIndex, slotIndex)
		slog.WarnContext(ctx, "slot binding index mismatch", "err", err, "vm", vmName, "slot", slotIndex)
		return zero, false, err
	}
	if !binding.HasJobMetadata() {
		slog.WarnContext(ctx, "slot binding metadata incomplete; probing active job", "vm", vmName, "slot", slotIndex, "job_id", binding.JobID, "run_id", binding.RunID)
		return zero, false, nil
	}
	return binding, true, nil
}

func slotBindingPath(slotIndex int) string {
	return fmt.Sprintf("%s%d.json", slotBindingFilePrefix, slotIndex)
}

func runJobRemoteCommand(encodedJITConfig string, slotIndex int, slotCount int) string {
	if slotCount <= 1 {
		// Git clone URLs already carry the token, so no credential helper is
		// needed. Clear credential.helper for this process tree so GCM is never
		// invoked, since its credential store path can deadlock in the headless VM.
		// GIT_TERMINAL_PROMPT=0 keeps a 401 failing fast. The multi slot path sets
		// the same environment in run-slot-job.sh.
		return fmt.Sprintf(
			"cd %s && export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=credential.helper GIT_CONFIG_VALUE_0= GIT_TERMINAL_PROMPT=0 && ./run.sh --jitconfig %s",
			runnerHome, shellQuote(encodedJITConfig))
	}
	replacer := strings.NewReplacer(
		"{{SLOT_INDEX}}", strconv.Itoa(slotIndex),
		"{{JIT_CONFIG}}", shellQuote(encodedJITConfig),
	)
	return replacer.Replace(runSlotJobScript)
}

func cloneRunnerSlotsCommand(slotCount int) string {
	if slotCount <= 1 {
		return ""
	}
	replacer := strings.NewReplacer("{{SLOT_COUNT}}", strconv.Itoa(slotCount))
	return replacer.Replace(cloneRunnerSlotsScript)
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
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

// Teardown stops the liveness touch loop, kills the boot process if running,
// then stops and deletes the VM. It is best effort; errors are logged at Warn.
func (b *Binder) Teardown(ctx context.Context, vm *WarmVM) {
	if vm.stopTouch != nil {
		vm.stopTouch()
	}
	if vm.boot != nil && vm.boot.Process != nil {
		_ = vm.boot.Process.Kill()
	}
	b.teardown(ctx, vm.Name)
}

// CheckAlive verifies that a cached warm VM still answers over vsock.
func (b *Binder) CheckAlive(ctx context.Context, vm *WarmVM) error {
	if _, err := b.vm.Exec(ctx, vm.Name, "touch", heartbeatFile); err != nil {
		slog.WarnContext(ctx, "warm vm liveness probe failed", "err", err, "vm", vm.Name)
		return fmt.Errorf("broker: check alive %s: %w", vm.Name, err)
	}
	return nil
}

// HasActiveJob reports whether the guest is running an actions job worker for a slot.
func (b *Binder) HasActiveJob(ctx context.Context, vm *WarmVM, slotIndex int, slotCount int) (bool, error) {
	probeCtx, cancel := context.WithTimeout(ctx, activeJobProbeTimeout)
	defer cancel()
	command := activeJobProbeCommand(slotIndex, slotCount)
	out, err := b.vm.Exec(probeCtx, vm.Name, "bash", "-lc", command)
	if err != nil {
		slog.WarnContext(probeCtx, "active job probe failed", "err", err, "vm", vm.Name, "slot", slotIndex)
		return false, fmt.Errorf("broker: probe active job on %s slot %d: %w", vm.Name, slotIndex, err)
	}
	result := activeJobProbeResult(strings.TrimSpace(string(out)))
	switch result {
	case activeJobProbeResultYes:
		return true, nil
	case activeJobProbeResultNo:
		return false, nil
	default:
		slog.WarnContext(probeCtx, "active job probe returned unexpected output", "vm", vm.Name, "slot", slotIndex, "output", result)
		return false, fmt.Errorf("broker: active job probe on %s slot %d returned %q", vm.Name, slotIndex, string(result))
	}
}

// SlotCPUActivity returns the aggregate CPU percent for one slot job process tree.
func (b *Binder) SlotCPUActivity(ctx context.Context, vm *WarmVM, slotIndex int, slotCount int) (float64, error) {
	probeCtx, cancel := context.WithTimeout(ctx, activeJobProbeTimeout)
	defer cancel()
	command := slotCPUActivityCommand(slotIndex, slotCount)
	out, err := b.vm.Exec(probeCtx, vm.Name, "bash", "-lc", command)
	if err != nil {
		slog.WarnContext(probeCtx, "slot cpu activity probe failed", "err", err, "vm", vm.Name, "slot", slotIndex)
		return 0, fmt.Errorf("broker: probe slot cpu activity on %s slot %d: %w", vm.Name, slotIndex, err)
	}
	output := strings.TrimSpace(string(out))
	cpuActivity, err := strconv.ParseFloat(output, 64)
	if err != nil {
		slog.WarnContext(probeCtx, "slot cpu activity probe returned unexpected output", "vm", vm.Name, "slot", slotIndex, "output", output)
		return 0, fmt.Errorf("broker: slot cpu activity probe on %s slot %d returned %q", vm.Name, slotIndex, output)
	}
	return cpuActivity, nil
}

func activeJobProbeCommand(slotIndex int, slotCount int) string {
	if slotCount <= 1 {
		return activeJobProbeScript
	}
	pattern := runnerWorkerPattern(slotIndex, slotCount)
	return fmt.Sprintf(`pgrep -f %s >/dev/null 2>&1; rc=$?; if [ "$rc" -eq 0 ]; then echo yes; elif [ "$rc" -eq 1 ]; then echo no; else exit "$rc"; fi`, shellQuote(pattern))
}

func slotCPUActivityCommand(slotIndex int, slotCount int) string {
	pattern := runnerWorkerPattern(slotIndex, slotCount)
	return fmt.Sprintf(`roots=$(pgrep -f %s 2>/dev/null); rc=$?; if [ "$rc" -eq 1 ]; then echo -1; exit 0; elif [ "$rc" -ne 0 ]; then exit "$rc"; fi; ps -Ao pid=,ppid=,pcpu= | awk -v roots="$roots" 'BEGIN { split(roots, root_parts, /[[:space:]]+/); for (i in root_parts) { if (root_parts[i] != "") { active[root_parts[i]] = 1 } } } { pid[NR] = $1; ppid[NR] = $2; cpu[NR] = $3 + 0 } END { changed = 1; while (changed) { changed = 0; for (i = 1; i <= NR; i++) { if (active[ppid[i]] && !active[pid[i]]) { active[pid[i]] = 1; changed = 1 } } } total = 0; for (i = 1; i <= NR; i++) { if (active[pid[i]]) { total += cpu[i] } } printf "%%.1f\n", total }'`, shellQuote(pattern))
}

func runnerWorkerPattern(slotIndex int, slotCount int) string {
	if slotCount <= 1 {
		return "[R]unner\\.Worker"
	}
	return fmt.Sprintf("actions-runner-%d/bin/[R]unner\\.Worker", slotIndex)
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
// should manage. It starts heartbeat refreshes for adopted VMs but does not
// clone, delete, or rewrite runner-slot directories.
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
	for _, name := range names {
		vm := &WarmVM{Name: name, Image: image, boot: nil, stopTouch: nil}
		if err := b.CheckAlive(ctx, vm); err != nil {
			slog.WarnContext(ctx, "skip running vm adoption after liveness failure", "err", err, "vm", name)
			continue
		}
		candidate := adoptionCandidate{
			vm:    vm,
			slots: b.adoptSlotBindings(ctx, vm, normalizeSlotCount(slotCount)),
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
		candidate.vm.stopTouch = b.startTouchLoop(ctx, candidate.vm.Name)
		adopted = append(adopted, AdoptedVM{
			VM:    candidate.vm,
			Slots: candidate.slots,
		})
	}
	return adopted, nil
}

func (b *Binder) adoptSlotBindings(ctx context.Context, vm *WarmVM, slotCount int) []SlotBinding {
	bindings := make([]SlotBinding, 0, slotCount)
	for slotIndex := range slotCount {
		binding, ok, err := b.readSlotBinding(ctx, vm.Name, slotIndex)
		if err != nil {
			slog.DebugContext(ctx, "slot binding unavailable during adoption; treating slot as busy", "err", err, "vm", vm.Name, "slot", slotIndex)
			bindings = append(bindings, busyFallbackSlotBinding(slotIndex))
			continue
		}
		if ok {
			bindings = append(bindings, binding)
			continue
		}
		active, err := b.HasActiveJob(ctx, vm, slotIndex, slotCount)
		if err != nil {
			slog.DebugContext(ctx, "active job probe failed during adoption; treating slot as busy", "err", err, "vm", vm.Name, "slot", slotIndex)
			bindings = append(bindings, busyFallbackSlotBinding(slotIndex))
			continue
		}
		if active {
			bindings = append(bindings, busyFallbackSlotBinding(slotIndex))
		}
	}
	return bindings
}

func busyFallbackSlotBinding(slotIndex int) SlotBinding {
	return SlotBinding{
		SlotIndex:      slotIndex,
		Repo:           "",
		JobID:          0,
		RunID:          0,
		BoundAt:        time.Time{},
		ObservedActive: true,
	}
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
			// Chmod after MkdirAll: MkdirAll applies 0700 only to dirs it
			// creates, so tighten an existing looser dir too. The build cache
			// can hold proprietary source and artifacts, so keep it private to
			// the owner on a multi-user host.
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
// answers or the timeout elapses.
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

// touchLoop refreshes the guest liveness file over vsock on a timer until the
// context is cancelled. If the broker dies, touches stop and the guest watchdog
// logs the stale heartbeat without powering off the VM.
func (b *Binder) touchLoop(ctx context.Context, vmName string) {
	ticker := time.NewTicker(touchInterval)
	defer ticker.Stop()
	for {
		if _, err := b.vm.Exec(ctx, vmName, "touch", heartbeatFile); err != nil {
			slog.DebugContext(ctx, "heartbeat touch failed", "err", err, "vm", vmName)
		}
		select {
		case <-ctx.Done():
			return
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
