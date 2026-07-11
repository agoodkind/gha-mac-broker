//go:build darwin || linux

package guestexec

import (
	"fmt"
	"log/slog"
	"runtime"
	"syscall"
	"unsafe"
)

const (
	waitIDProcess = 1
	waitExited    = 0x4
)

func waitUntilExited(processID int) error {
	waitNoWait := uintptr(0x1000000)
	if runtime.GOOS == "darwin" {
		waitNoWait = 0x20
	}
	var processInfo [128]byte
	for {
		_, _, errno := syscall.Syscall6(
			syscall.SYS_WAITID,
			waitIDProcess,
			uintptr(processID),
			uintptr(unsafe.Pointer(&processInfo[0])),
			waitExited|waitNoWait,
			0,
			0,
		)
		if errno == 0 {
			return nil
		}
		if errno == syscall.EINTR {
			continue
		}
		waitError := fmt.Errorf("wait for process %d exit without reaping: %w", processID, errno)
		slog.Error("guest execution process exit observation failed", "err", waitError)
		return waitError
	}
}
