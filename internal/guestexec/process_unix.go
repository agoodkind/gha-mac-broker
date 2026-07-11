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
