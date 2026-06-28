// Package tart wraps the `tart` CLI to manage the lifecycle of ephemeral macOS
// VMs: clone a golden image, boot it, read its IP, and tear it down. Commands
// that complete and return output go through an injectable runner so they can be
// unit tested; booting a VM is a long-lived process handled separately.
package tart

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
)

// CommandRunner runs `tart <args...>` and returns combined stdout. It is a field
// so tests can stub the CLI.
type CommandRunner func(ctx context.Context, bin string, args ...string) ([]byte, error)

// Tart drives the tart binary. The run field is swappable in white-box tests.
type Tart struct {
	bin string
	run CommandRunner
}

// New returns a Tart that invokes the given binary (default "tart").
func New(bin string) *Tart {
	if bin == "" {
		bin = "tart"
	}
	return &Tart{bin: bin, run: execRunner}
}

// command builds an [exec.Cmd] for the tart binary. Centralizing construction
// keeps the single audited exec call site in one place.
func command(ctx context.Context, bin string, args ...string) *exec.Cmd {
	slog.DebugContext(ctx, "tart command built", "bin", bin, "args", strings.Join(args, " "))
	return exec.CommandContext(ctx, bin, args...)
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

// listEntry is the subset of one `tart list --format json` record the broker
// reads. Other columns (source, disk, state) are ignored.
type listEntry struct {
	Name string `json:"Name"`
}

// List returns the names of every VM tart knows about. The orphan sweep uses it
// to find stale pool clones left by a previous broker process.
func (t *Tart) List(ctx context.Context) ([]string, error) {
	out, err := t.run(ctx, t.bin, "list", "--format", "json")
	if err != nil {
		return nil, err
	}
	var entries []listEntry
	if err := json.Unmarshal(out, &entries); err != nil {
		slog.ErrorContext(ctx, "tart list parse failed", "err", err)
		return nil, fmt.Errorf("tart: parse list output: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name)
	}
	return names, nil
}

// Clone makes a copy-on-write clone of source under name.
func (t *Tart) Clone(ctx context.Context, source, name string) error {
	_, err := t.run(ctx, t.bin, "clone", source, name)
	return err
}

// IP returns the VM's IP address.
func (t *Tart) IP(ctx context.Context, name string) (string, error) {
	out, err := t.run(ctx, t.bin, "ip", name)
	if err != nil {
		return "", err
	}
	ip := strings.TrimSpace(string(out))
	if ip == "" {
		return "", fmt.Errorf("tart: empty ip for %s", name)
	}
	return ip, nil
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
	return command(ctx, t.bin, args...)
}
