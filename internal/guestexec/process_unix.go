//go:build unix

package guestexec

import (
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"syscall"
)

func configureProcessGroup(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killProcessGroup(pgid int) error {
	// A process-group kill negates pgid, so a pgid of 0 or 1 would signal the
	// caller's own group (kill(0)) or every reachable process (kill(-1)). Reject
	// these unsafe ids before signaling rather than take down the guest agent.
	if pgid <= 1 {
		return fmt.Errorf("refusing to kill unsafe process group %d", pgid)
	}
	err := syscall.Kill(-pgid, syscall.SIGKILL)
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	wrappedError := fmt.Errorf("kill process group %d: %w", pgid, err)
	slog.Error("guest execution process group kill failed", "err", wrappedError)
	return wrappedError
}
