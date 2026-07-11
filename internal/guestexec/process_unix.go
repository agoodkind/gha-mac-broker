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

func killProcessGroup(processID int) error {
	err := syscall.Kill(-processID, syscall.SIGKILL)
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	wrappedError := fmt.Errorf("kill process group %d: %w", processID, err)
	slog.Error("guest execution process group kill failed", "err", wrappedError)
	return wrappedError
}

// Reference the Unix-only process helpers so the dead-code gate sees them used
// on the branch that introduces them, without pulling Unix symbols into the
// portable types.go file.
var _, _ = configureProcessGroup, waitUntilExited
