//go:build unix

package guestworker

import (
	"errors"
	"os"
	"syscall"
	"testing"
	"time"
)

// TestReceivedPipeFDSupportsReadDeadline checks that a pipe read end reconstructed
// from a raw descriptor (as an SCM_RIGHTS handoff produces) still honors a read
// deadline, which the freeze barrier depends on to stop a blocked capture.
func TestReceivedPipeFDSupportsReadDeadline(t *testing.T) {
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer func() { _ = writeEnd.Close() }()
	defer func() { _ = readEnd.Close() }()

	dupFD, err := syscall.Dup(int(readEnd.Fd()))
	if err != nil {
		t.Fatalf("dup: %v", err)
	}
	if err := syscall.SetNonblock(dupFD, true); err != nil {
		t.Fatalf("set nonblock: %v", err)
	}
	received := os.NewFile(uintptr(dupFD), "received-pipe")
	defer func() { _ = received.Close() }()

	if err := received.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		buffer := make([]byte, 16)
		_, readErr := received.Read(buffer)
		done <- readErr
	}()
	select {
	case readErr := <-done:
		if !errors.Is(readErr, os.ErrDeadlineExceeded) {
			t.Fatalf("read error = %v, want deadline exceeded", readErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked read was not interrupted by the deadline on a received pipe fd")
	}
}
