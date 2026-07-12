//go:build unix

package guestworker

import (
	"context"

	"goodkind.io/gha-mac-broker/internal/guestagent"
	"goodkind.io/gha-mac-broker/internal/guestexec"
)

// init substitutes a shell passthrough spec builder so the worker reload tests
// run the request jit_config as a shell script instead of a real GitHub runner.
// This runs in every worker subprocess the reload harness re-execs, because the
// test binary initializes its packages before TestMain routes to Run.
func init() {
	specBuilderOverride = shellSpecBuilder{}
}

// shellSpecBuilder runs the request jit_config through /bin/sh, giving the reload
// tests a process that streams output and returns a terminal result.
type shellSpecBuilder struct{}

func (shellSpecBuilder) Build(_ context.Context, request guestagent.JobRequest) (guestexec.ExecSpec, error) {
	return guestexec.ExecSpec{
		ExecutionID: request.ExecutionID,
		Slot:        request.Slot,
		Meta:        request.Meta,
		Command:     "/bin/sh",
		Args:        []string{"-c", request.JitConfig},
		Env:         request.Env,
	}, nil
}
