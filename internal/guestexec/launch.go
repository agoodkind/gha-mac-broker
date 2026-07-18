package guestexec

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
)

// LaunchedProcess is a guest process the caller forked and must waitpid for
// life. macOS has no child-subreaper, so whichever process reaps the child must
// stay its direct parent. The caller registers this handle with the registry
// (handing over the pipe read ends), waits on Command to learn the exit code,
// and reports it with Registry.ReportExit. The registry never forks or waits a
// process itself.
type LaunchedProcess struct {
	Command *exec.Cmd
	PID     int
	PGID    int
	Stdout  io.ReadCloser
	Stderr  io.ReadCloser
}

// Launch forks the execution's process in its own process group and returns the
// handle plus the read ends of its captured stdout and stderr. The child is a
// group leader, so its process group ID equals its PID. The write ends stay
// attached to the child, so the read ends reach EOF only after the child exits
// and the registry drains them. Registry.Start calls it so the guest-agent
// RunJob path spawns a real process.
func Launch(spec ExecSpec) (*LaunchedProcess, error) {
	if spec.Command == "" {
		return nil, fmt.Errorf("guestexec: command is required")
	}
	if !filepath.IsAbs(spec.Command) {
		return nil, fmt.Errorf("guestexec: command path must be absolute")
	}

	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		slog.Error("guest execution stdout pipe creation failed", "err", err)
		return nil, fmt.Errorf("guestexec: create stdout pipe: %w", err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		slog.Error("guest execution stderr pipe creation failed", "err", err)
		return nil, fmt.Errorf("guestexec: create stderr pipe: %w", err)
	}
	closePipes := func() {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		_ = stderrReader.Close()
		_ = stderrWriter.Close()
	}

	command := new(exec.Cmd)
	command.Path = spec.Command
	command.Args = append([]string{spec.Command}, spec.Args...)
	command.Dir = spec.WorkingDir
	command.Env = mergedEnvironment(spec.Env)
	command.Stdout = stdoutWriter
	command.Stderr = stderrWriter
	configureProcessGroup(command)
	if err := command.Start(); err != nil {
		closePipes()
		slog.Error("guest execution process start failed", "err", err, "execution_id", spec.ExecutionID)
		return nil, fmt.Errorf("guestexec: start %q: %w", spec.Command, err)
	}
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()

	return &LaunchedProcess{
		Command: command,
		PID:     command.Process.Pid,
		PGID:    command.Process.Pid,
		Stdout:  stdoutReader,
		Stderr:  stderrReader,
	}, nil
}
