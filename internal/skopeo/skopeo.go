// Package skopeo wraps the `skopeo` CLI to copy OCI images into a local OCI
// layout. The broker uses that layout as the blob store for its loopback
// registry before Tart clones the image over [::1].
package skopeo

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
)

// CommandRunner runs `skopeo <args...>` and returns combined output. It is a
// field so tests can stub the CLI.
type CommandRunner func(ctx context.Context, bin string, args ...string) ([]byte, error)

// Client drives the skopeo binary. The run field is swappable in white-box
// tests.
type Client struct {
	bin string
	run CommandRunner
}

// New returns a Client that invokes the given binary (default "skopeo").
func New(bin string) *Client {
	if bin == "" {
		bin = "skopeo"
	}
	return &Client{bin: bin, run: execRunner}
}

// command builds an [exec.Cmd] for the skopeo binary. Centralizing construction
// keeps the single audited exec call site in one place.
func command(ctx context.Context, bin string, args ...string) *exec.Cmd {
	slog.DebugContext(ctx, "skopeo command built", "bin", bin, "args", strings.Join(args, " "))
	return exec.CommandContext(ctx, bin, args...)
}

func execRunner(ctx context.Context, bin string, args ...string) ([]byte, error) {
	cmd := command(ctx, bin, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		slog.ErrorContext(ctx, "skopeo command failed", "err", err, "args", strings.Join(args, " "))
		return out.Bytes(), fmt.Errorf("skopeo %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(out.String()))
	}
	return out.Bytes(), nil
}

// CopyToOCILayout copies srcImageRef into layoutDir as an OCI layout tagged with
// tag and constrained to the requested OS and architecture.
func (c *Client) CopyToOCILayout(ctx context.Context, srcImageRef, layoutDir, tag, osName, arch string) error {
	args := []string{
		"copy",
		"--override-os",
		osName,
		"--override-arch",
		arch,
		"docker://" + srcImageRef,
		"oci:" + layoutDir + ":" + tag,
	}
	if out, err := c.run(ctx, c.bin, args...); err != nil {
		slog.ErrorContext(ctx, "skopeo copy failed", "err", err, "output", strings.TrimSpace(string(out)))
		return fmt.Errorf("skopeo: copy %s to %s: %w", srcImageRef, layoutDir, err)
	}
	return nil
}
