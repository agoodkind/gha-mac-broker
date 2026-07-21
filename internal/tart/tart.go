// Package tart wraps the `tart` CLI to manage the lifecycle of ephemeral macOS
// VMs: clone a golden image, boot it, run commands inside it over the guest
// agent's vsock channel (no IP, no SSH), and tear it down. Commands that
// complete and return output go through an injectable runner so they can be unit
// tested; booting a VM is a long-lived process handled separately.
package tart

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	maxTeeTailBytes  = 64 * 1024
	redactedLogValue = "[redacted]"
)

// Per-operation deadlines bound the short-lived tart calls so a wedged
// subprocess cannot block a caller forever. They are generous relative to real
// operation time: clone is minute-scale, teardown and list are second-scale. The
// detached boot is deliberately excluded; it is the VM's lifetime process.
const (
	// cloneDeadline bounds a copy-on-write `tart clone` of a golden image.
	cloneDeadline = 10 * time.Minute
	// stopDeadline bounds a graceful `tart stop`.
	stopDeadline = 2 * time.Minute
	// deleteDeadline bounds a `tart delete`.
	deleteDeadline = 2 * time.Minute
	// listDeadline bounds a `tart list`.
	listDeadline = 30 * time.Second
	// ipWaitSeconds is the `--wait` window passed to `tart ip`, the seconds tart
	// blocks for the guest to acquire a NAT lease.
	ipWaitSeconds = 30
	// ipDeadline bounds `tart ip`. It exceeds ipWaitSeconds so the process, not
	// the context, owns the normal wait and the deadline is only a backstop.
	ipDeadline = 45 * time.Second
	// killGrace is how long a bounded tart process group has to exit after
	// SIGTERM before it is force-killed with SIGKILL.
	killGrace = 5 * time.Second
)

// CommandRunner runs `tart <args...>` and returns combined stdout. It is a field
// so tests can stub the CLI.
type CommandRunner func(ctx context.Context, bin string, args ...string) ([]byte, error)

// TeeCommandRunner runs `tart <args...>` and mirrors combined output to sink.
// It is a field so tests can stub streaming CLI paths.
type TeeCommandRunner func(ctx context.Context, bin string, sink io.Writer, args ...string) ([]byte, error)

// Tart drives the tart binary. Runner fields are swappable in white-box tests.
type Tart struct {
	bin    string
	run    CommandRunner
	runTee TeeCommandRunner
}

// New returns a Tart that invokes the given binary (default "tart").
func New(bin string) *Tart {
	if bin == "" {
		bin = "tart"
	}
	return &Tart{bin: bin, run: execRunner, runTee: execRunnerTee}
}

// command builds a context-bound [exec.Cmd] for the foreground boot path, where
// the caller's context cancellation tears the boot down. detachedCommand is the
// deliberately detached path for warm VMs that outlive broker cancellation.
func command(ctx context.Context, bin string, args ...string) *exec.Cmd {
	loggedArgs, _ := tartCommandLogValues(args, nil)
	slog.DebugContext(ctx, "tart command built", "bin", bin, "args", loggedArgs)
	return exec.CommandContext(ctx, bin, args...)
}

func detachedCommand(ctx context.Context, bin string, args ...string) *exec.Cmd {
	loggedArgs, _ := tartCommandLogValues(args, nil)
	slog.DebugContext(ctx, "tart detached command built", "bin", bin, "args", loggedArgs)
	// The detached tart run is the VM's long-lived hypervisor process, so it must
	// survive broker context cancellation and carry no wall-clock ceiling. It dies
	// only at teardown (Process.Kill plus tart stop and delete); a boot that never
	// readies is caught by the caller's bounded readiness wait.
	cmd := exec.CommandContext(context.WithoutCancel(ctx), bin, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return cmd
}

// runProcessGroup starts cmd in its own process group, then waits for it. If ctx
// is cancelled or its deadline fires first, the whole group is signalled
// (SIGTERM, then SIGKILL after killGrace) so a wedged tart and every child it
// spawned die together rather than orphaning under the reconcile loop.
func runProcessGroup(ctx context.Context, cmd *exec.Cmd) error {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	// The command carries ctx (built with CommandContext), but cancellation is
	// owned here. A no-op Cancel with the default zero WaitDelay stops os/exec
	// from single-child killing on ctx.Done, so the SIGTERM grace and SIGKILL
	// below reach the whole process group instead of racing a leader-only kill.
	cmd.Cancel = func() error { return nil }
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("tart: start: %w", err)
	}
	// With Setpgid and an unset Pgid, the child leads a new group whose id equals
	// its pid, so negating the pid addresses the whole group.
	pgid := cmd.Process.Pid
	waitErr := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic: %v", r)
				slog.ErrorContext(ctx, "tart wait goroutine panic recovered", "err", err)
				waitErr <- err
			}
		}()
		waitErr <- cmd.Wait()
	}()
	select {
	case err := <-waitErr:
		return err
	case <-ctx.Done():
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
		leaderExited := false
		select {
		case <-waitErr:
			leaderExited = true
		case <-time.After(killGrace):
		}
		// Sweep any group member that ignored SIGTERM, then reap the leader.
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		if !leaderExited {
			<-waitErr
		}
		return fmt.Errorf("tart: bounded run: %w", ctx.Err())
	}
}

func execRunner(ctx context.Context, bin string, args ...string) ([]byte, error) {
	loggedArgs, _ := tartCommandLogValues(args, nil)
	slog.DebugContext(ctx, "tart command built", "bin", bin, "args", loggedArgs)
	cmd := exec.CommandContext(ctx, bin, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := runProcessGroup(ctx, cmd); err != nil {
		loggedArgs, loggedOutput := tartCommandLogValues(args, out.Bytes())
		slog.ErrorContext(ctx, "tart command failed", "err", err, "args", loggedArgs, "output", loggedOutput)
		return out.Bytes(), fmt.Errorf("tart %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(out.String()))
	}
	loggedArgs, loggedOutput := tartCommandLogValues(args, out.Bytes())
	slog.DebugContext(ctx, "tart command output", "args", loggedArgs, "output", loggedOutput)
	return out.Bytes(), nil
}

type tailBuffer struct {
	limit int
	buf   []byte
}

func newTailBuffer(limit int) *tailBuffer {
	return &tailBuffer{
		limit: limit,
		buf:   make([]byte, 0, limit),
	}
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	if b.limit <= 0 {
		return len(p), nil
	}
	if len(p) >= b.limit {
		b.buf = append(b.buf[:0], p[len(p)-b.limit:]...)
		return len(p), nil
	}
	excess := len(b.buf) + len(p) - b.limit
	if excess > 0 {
		copy(b.buf, b.buf[excess:])
		b.buf = b.buf[:len(b.buf)-excess]
	}
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *tailBuffer) Bytes() []byte {
	return b.buf
}

func execRunnerTee(ctx context.Context, bin string, sink io.Writer, args ...string) ([]byte, error) {
	loggedArgs, _ := tartCommandLogValues(args, nil)
	slog.DebugContext(ctx, "tart command built", "bin", bin, "args", loggedArgs)
	cmd := exec.CommandContext(ctx, bin, args...)
	out := newTailBuffer(maxTeeTailBytes)
	writer := io.Writer(out)
	if sink != nil {
		writer = io.MultiWriter(out, sink)
	}
	cmd.Stdout = writer
	cmd.Stderr = writer
	if err := runProcessGroup(ctx, cmd); err != nil {
		loggedArgs, loggedOutput := tartCommandLogValues(args, out.Bytes())
		slog.ErrorContext(ctx, "tart command failed", "err", err, "args", loggedArgs, "output", loggedOutput)
		tail := string(out.Bytes())
		return out.Bytes(), fmt.Errorf("tart %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(tail))
	}
	loggedArgs, loggedOutput := tartCommandLogValues(args, out.Bytes())
	slog.DebugContext(ctx, "tart command output", "args", loggedArgs, "output", loggedOutput)
	return out.Bytes(), nil
}

func tartCommandLogValues(args []string, output []byte) (string, string) {
	loggedArgs := strings.Join(args, " ")
	loggedOutput := strings.TrimSpace(string(output))
	redactArgs, redactOutput := tartCommandRedaction(args)
	if redactArgs {
		loggedArgs = redactedLogValue
	}
	if redactOutput {
		loggedOutput = redactedLogValue
	}
	return loggedArgs, loggedOutput
}

func tartCommandRedaction(args []string) (bool, bool) {
	if tartArgsContainSensitiveValue(args) {
		return true, true
	}
	securityArgs := nestedSecurityArgs(args)
	if !securityArgsContainSecret(securityArgs) {
		return false, false
	}
	return true, !securityOutputIsDiagnostic(securityArgs)
}

func tartArgsContainSensitiveValue(args []string) bool {
	for _, arg := range args {
		lower := strings.ToLower(arg)
		if strings.Contains(lower, "jitconfig") || strings.Contains(lower, "base64") ||
			strings.Contains(lower, "p12") || strings.Contains(lower, "token") {
			return true
		}
	}
	return false
}

func nestedSecurityArgs(args []string) []string {
	for index, arg := range args {
		if filepath.Base(arg) == "security" {
			return args[index+1:]
		}
	}
	return nil
}

func securityArgsContainSecret(args []string) bool {
	for _, arg := range args {
		if arg == "import" || arg == "-P" || arg == "-p" {
			return true
		}
	}
	return false
}

func securityOutputIsDiagnostic(args []string) bool {
	if len(args) == 0 {
		return false
	}
	command := args[0]
	return command == "create-keychain" || command == "default-keychain" ||
		command == "list-keychains" || command == "unlock-keychain" ||
		command == "set-keychain-settings" || command == "show-keychain-info" ||
		command == "find-identity"
}

// listEntry is the subset of one `tart list --format json` record the broker
// reads. Other columns such as source and disk are ignored.
type listEntry struct {
	Name  string `json:"Name"`
	State string `json:"State"`
}

// VM is one entry from `tart list --format json`.
type VM struct {
	Name  string
	State string
}

// List returns the names of every VM tart knows about.
func (t *Tart) List(ctx context.Context) ([]string, error) {
	entries, err := t.ListVMs(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name)
	}
	return names, nil
}

// ListVMs returns the VM names and states visible to tart.
func (t *Tart) ListVMs(ctx context.Context) ([]VM, error) {
	ctx, cancel := context.WithTimeout(ctx, listDeadline)
	defer cancel()
	out, err := t.run(ctx, t.bin, "list", "--format", "json")
	if err != nil {
		return nil, err
	}
	var entries []listEntry
	if err := json.Unmarshal(out, &entries); err != nil {
		slog.ErrorContext(ctx, "tart list parse failed", "err", err)
		return nil, fmt.Errorf("tart: parse list output: %w", err)
	}
	vms := make([]VM, 0, len(entries))
	for _, e := range entries {
		// Go struct conversions ignore tags; listEntry and VM intentionally keep
		// matching field names and types so JSON tags stay isolated to parsing.
		vms = append(vms, VM(e))
	}
	return vms, nil
}

// Clone makes a copy-on-write clone of source under name.
func (t *Tart) Clone(ctx context.Context, source, name string, insecure bool) error {
	ctx, cancel := context.WithTimeout(ctx, cloneDeadline)
	defer cancel()
	args := []string{"clone"}
	if insecure {
		args = append(args, "--insecure")
	}
	args = append(args, source, name)
	_, err := t.run(ctx, t.bin, args...)
	return err
}

// IP resolves the VM's NAT address via `tart ip <name> --wait`. It returns an
// error if tart yields output that is not a valid IP. The host uses this to dial
// the guest agent.
func (t *Tart) IP(ctx context.Context, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, ipDeadline)
	defer cancel()
	out, err := t.run(ctx, t.bin, "ip", name, "--wait", strconv.Itoa(ipWaitSeconds))
	if err != nil {
		return "", err
	}
	addr := strings.TrimSpace(string(out))
	if net.ParseIP(addr) == nil {
		return "", fmt.Errorf("tart: ip %s returned %q, not a valid IP", name, addr)
	}
	return addr, nil
}

// Exec runs a command inside a booted VM over the guest agent's vsock channel
// (`tart exec <name> <argv...>`), with no IP and no SSH. It returns the combined
// output; a non-zero guest exit code surfaces as an error.
func (t *Tart) Exec(ctx context.Context, name string, argv ...string) ([]byte, error) {
	args := append([]string{"exec", name}, argv...)
	return t.run(ctx, t.bin, args...)
}

// ExecTee runs a guest command and mirrors combined output to sink while it is
// produced. It still returns the buffered combined output when the command exits.
func (t *Tart) ExecTee(ctx context.Context, name string, sink io.Writer, argv ...string) ([]byte, error) {
	args := append([]string{"exec", name}, argv...)
	return t.runTee(ctx, t.bin, sink, args...)
}

// Stop gracefully stops a running VM.
func (t *Tart) Stop(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, stopDeadline)
	defer cancel()
	_, err := t.run(ctx, t.bin, "stop", name)
	return err
}

// Delete removes a VM. It is safe to call on an already-stopped VM.
func (t *Tart) Delete(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, deleteDeadline)
	defer cancel()
	_, err := t.run(ctx, t.bin, "delete", name)
	return err
}

// BootOptions configures a VM boot.
type BootOptions struct {
	// Dirs are host directories shared into the VM as `--dir name:path`.
	// The cache directory is mounted this way so it survives VM deletion.
	Dirs []DirMount
	// Detach starts tart in a new process group and ignores caller context
	// cancellation. The broker uses this for warm VMs so launchd bootout of the
	// broker job does not signal the VM's `tart run` process.
	Detach bool
	// NoGraphics runs the VM headless.
	NoGraphics bool
}

// DirMount is one host directory shared into a VM.
type DirMount struct {
	Name string
	Path string
}

// BootCommand builds the `tart run` invocation that boots a VM. Booting is a
// long-lived foreground process, so the caller owns the returned [exec.Cmd]
// (start it, then Stop/Delete when the job finishes). The command is returned
// rather than started so the broker can manage the process and its logs.
func (t *Tart) BootCommand(ctx context.Context, name string, opts BootOptions) *exec.Cmd {
	args := []string{"run", name}
	if opts.NoGraphics {
		args = append(args, "--no-graphics")
	}
	for _, d := range opts.Dirs {
		args = append(args, "--dir", d.Name+":"+d.Path)
	}
	if opts.Detach {
		return detachedCommand(ctx, t.bin, args...)
	}
	return command(ctx, t.bin, args...)
}
