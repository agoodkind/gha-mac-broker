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
	"os/exec"
	"strings"
	"syscall"
)

const maxTeeTailBytes = 64 * 1024

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

// command builds an [exec.Cmd] for the tart binary. Centralizing construction
// keeps the single audited exec call site in one place.
func command(ctx context.Context, bin string, args ...string) *exec.Cmd {
	slog.DebugContext(ctx, "tart command built", "bin", bin, "args", strings.Join(args, " "))
	return exec.CommandContext(ctx, bin, args...)
}

func detachedCommand(ctx context.Context, bin string, args ...string) *exec.Cmd {
	slog.DebugContext(ctx, "tart detached command built", "bin", bin, "args", strings.Join(args, " "))
	cmd := exec.CommandContext(context.WithoutCancel(ctx), bin, args...)
	cmd.Cancel = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return cmd
}

func execRunner(ctx context.Context, bin string, args ...string) ([]byte, error) {
	cmd := command(ctx, bin, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		slog.ErrorContext(ctx, "tart command failed", "err", err, "args", strings.Join(args, " "))
		return out.Bytes(), fmt.Errorf("tart %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(out.String()))
	}
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
	cmd := command(ctx, bin, args...)
	out := newTailBuffer(maxTeeTailBytes)
	writer := io.Writer(out)
	if sink != nil {
		writer = io.MultiWriter(out, sink)
	}
	cmd.Stdout = writer
	cmd.Stderr = writer
	if err := cmd.Run(); err != nil {
		slog.ErrorContext(ctx, "tart command failed", "err", err, "args", strings.Join(args, " "))
		tail := string(out.Bytes())
		return out.Bytes(), fmt.Errorf("tart %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(tail))
	}
	return out.Bytes(), nil
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
	args := []string{"clone"}
	if insecure {
		args = append(args, "--insecure")
	}
	args = append(args, source, name)
	_, err := t.run(ctx, t.bin, args...)
	return err
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
	_, err := t.run(ctx, t.bin, "stop", name)
	return err
}

// Delete removes a VM. It is safe to call on an already-stopped VM.
func (t *Tart) Delete(ctx context.Context, name string) error {
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
