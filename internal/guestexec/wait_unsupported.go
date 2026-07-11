//go:build unix && !darwin && !linux

package guestexec

import "fmt"

func waitUntilExited(processID int) error {
	return fmt.Errorf("guestexec: non-reaping process wait is unsupported for process %d", processID)
}
