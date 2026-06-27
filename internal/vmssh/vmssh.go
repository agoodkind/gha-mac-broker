// Package vmssh runs commands inside a booted Tart VM over SSH. The golden image
// carries the broker's public key, so authentication is key-based and no
// password handling is needed. Commands shell out to the system ssh client.
package vmssh

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
)

// CommandRunner runs an ssh invocation and returns combined output. It is a
// field so tests can stub the client.
type CommandRunner func(ctx context.Context, name string, args ...string) ([]byte, error)

// Runner executes remote commands as a fixed user with a fixed key. The run
// field is swappable in white-box tests.
type Runner struct {
	user    string
	keyPath string
	run     CommandRunner
}

// New returns a Runner that connects as user with the given private key.
func New(user, keyPath string) *Runner {
	return &Runner{user: user, keyPath: keyPath, run: execRunner}
}

func execRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		slog.ErrorContext(ctx, "ssh command failed", "err", err, "out", strings.TrimSpace(out.String()))
		return out.Bytes(), fmt.Errorf("ssh: %w: %s", err, strings.TrimSpace(out.String()))
	}
	return out.Bytes(), nil
}

// sshArgs builds the common ssh argument prefix for a host.
func (r *Runner) sshArgs(host string, extra ...string) []string {
	args := []string{
		"-i", r.keyPath,
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "ConnectTimeout=5",
		r.user + "@" + host,
	}
	return append(args, extra...)
}

// Probe reports whether the VM accepts SSH yet. It is used to wait for boot.
func (r *Runner) Probe(ctx context.Context, host string) error {
	_, err := r.run(ctx, "ssh", r.sshArgs(host, "true")...)
	return err
}

// Run executes a remote command and returns its combined output.
func (r *Runner) Run(ctx context.Context, host, remoteCmd string) ([]byte, error) {
	return r.run(ctx, "ssh", r.sshArgs(host, remoteCmd)...)
}
