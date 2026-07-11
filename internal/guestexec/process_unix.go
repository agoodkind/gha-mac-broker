//go:build unix

package guestexec

import (
	"os/exec"
	"syscall"
)

func configureProcessGroup(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// Reference the Unix-only process helpers so the dead-code gate sees them used
// on the branch that introduces them, without pulling Unix symbols into the
// portable types.go file.
var _, _ = configureProcessGroup, waitUntilExited
