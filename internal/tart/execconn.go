package tart

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
)

// ExecConn starts `tart exec -i <name> <argv...>` and returns a [net.Conn] whose
// reads and writes ride an AF_UNIX socket wired to the guest command's stdio. A
// host-side HTTP/2 client can then speak to a guest server over the tart
// guest-agent channel with no guest IP, which is the reliable path: the guest
// agent channel is up before the NAT bridge route to the guest settles. Closing
// the conn closes the socket and reaps the exec process group.
//
// The exec runs under [context.WithoutCancel] because an HTTP/2 client keeps one
// transport connection open across many requests, so the conn lifetime is owned
// by Close, not by the ctx of whichever request first dialed it.
func (t *Tart) ExecConn(ctx context.Context, name string, argv ...string) (net.Conn, error) {
	args := append([]string{"exec", "-i", name}, argv...)
	slog.DebugContext(ctx, "tart exec conn built", "vm", name, "args", strings.Join(args, " "))

	// Create a connected AF_UNIX stream pair under ForkLock, marked close-on-exec
	// so a concurrent fork cannot inherit the descriptors, mirroring how the
	// standard library builds socket pairs safely.
	syscall.ForkLock.RLock()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err == nil {
		syscall.CloseOnExec(fds[0])
		syscall.CloseOnExec(fds[1])
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		slog.ErrorContext(ctx, "tart exec conn socketpair failed", "err", err, "vm", name)
		return nil, fmt.Errorf("tart: exec conn socketpair for %s: %w", name, err)
	}
	// The parent file becomes a net.Conn; the child file wires to the guest
	// command's stdin and stdout so it reads requests and writes replies over one
	// socket.
	parentFile := os.NewFile(uintptr(fds[0]), "tart-exec-parent")
	childFile := os.NewFile(uintptr(fds[1]), "tart-exec-child")

	// #nosec G204 -- args is broker-controlled: the VM name plus the baked guest binary path and a fixed subcommand, never user input.
	cmd := exec.CommandContext(context.WithoutCancel(ctx), t.bin, args...)
	// The child reads requests on stdin and writes replies on stdout, both wired
	// to its single socket end.
	cmd.Stdin = childFile
	cmd.Stdout = childFile
	// Guest-relay stderr goes to the null device: RPC failures surface on the
	// stream itself, and a relay has no other diagnostic output worth keeping.
	cmd.Stderr = nil
	// Setpgid so Close can reap the whole tart process group, matching how the
	// bounded run path avoids orphaning tart children.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		_ = parentFile.Close()
		_ = childFile.Close()
		slog.ErrorContext(ctx, "tart exec conn start failed", "err", err, "vm", name)
		return nil, fmt.Errorf("tart: exec conn start %s: %w", name, err)
	}
	// The child now holds its socket end through stdin and stdout, so the parent
	// no longer needs that end.
	_ = childFile.Close()

	conn, err := net.FileConn(parentFile)
	// FileConn dups the descriptor, so the original file is no longer needed.
	_ = parentFile.Close()
	if err != nil {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		_ = cmd.Wait()
		slog.ErrorContext(ctx, "tart exec conn wrap socket failed", "err", err, "vm", name)
		return nil, fmt.Errorf("tart: exec conn wrap socket for %s: %w", name, err)
	}
	return &execConn{Conn: conn, cmd: cmd, closeOnce: sync.Once{}, closeErr: nil}, nil
}

// execConn adapts a running `tart exec -i` process to [net.Conn]. Reads, writes,
// and deadlines ride the embedded AF_UNIX conn wired to the guest command's
// stdio; Close closes that conn and reaps the exec process group so no tart
// child is orphaned.
type execConn struct {
	net.Conn
	cmd       *exec.Cmd
	closeOnce sync.Once
	closeErr  error
}

// Close closes the socket, which lets the guest relay see end-of-stream, then
// reaps the exec process group so no tart child is orphaned.
func (c *execConn) Close() error {
	c.closeOnce.Do(func() {
		connErr := c.Conn.Close()
		if c.cmd.Process != nil {
			// Negating the pid signals the whole group started by Setpgid.
			_ = syscall.Kill(-c.cmd.Process.Pid, syscall.SIGKILL)
		}
		_ = c.cmd.Wait()
		c.closeErr = connErr
	})
	return c.closeErr
}
