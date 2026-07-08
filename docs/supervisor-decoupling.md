# Supervisor Decoupling

The broker control plane must be restartable without killing warm VMs or jobs
that are already running inside them.

## Direction

The long-term shape is a supervisor/control-plane split.

The supervisor owns VM lifecycle: clone, boot, stop, delete, heartbeat, and
eventual cleanup. The control plane owns webhook intake, capacity reporting,
routing, and per-job bind requests. A control-plane restart must only drop HTTP
state and in-memory queues; it must not send a signal, context cancellation, or
startup sweep that powers off an existing VM.

## macOS Process Boundary

The safe core uses a detached `tart run` process group for warm VMs. The broker
starts each warm VM with `SysProcAttr.Setpgid = true` and builds that command
without a cancellation hook tied to the broker request context. That gives the
VM runner its own process group, so `launchctl bootout` of
`io.goodkind.gha-mac-broker` does not signal the VM through the broker job's
process group.

The broker still keeps the `exec.Cmd` while it is alive so explicit recycle and
teardown paths can stop a VM. It also starts a goroutine that waits on the
`tart run` command, which reaps the child process when it exits and prevents
`<defunct>` children from accumulating.

## Startup Reconcile

Startup no longer sweeps every VM with the configured pool prefix. The pool asks
the binder to enumerate Tart VMs, filters to running VMs with the pool prefix,
checks vsock liveness, and adopts up to the configured runner count.

Adoption recreates pool worker state from the live VM instead of cloning a
replacement. Each adopted VM restarts the broker heartbeat touch loop. Each slot
is marked busy when the guest contains a persisted slot binding, and older VMs
without a binding are treated as busy when the active-job probe finds a running
`Runner.Worker`.

The broker writes a small JSON binding file in the guest before starting each
job. The file records slot index, repo, job id, run id, and bound time. The file
is removed when the broker observes job completion. If the broker restarts mid
job, the new broker can recover the busy slot and keep routing away from it.

## Shutdown Semantics

Control-plane shutdown releases worker goroutines but does not tear down their
VMs. Reconcile, cancel, stale idle, failed health, and explicit teardown paths
still recycle VMs when the broker is intentionally replacing a worker.

The guest watchdog no longer powers off a VM when the broker heartbeat is stale.
It logs the stale heartbeat as a diagnostic. VM cleanup belongs to the host-side
supervisor/reconcile path so a slow redeploy cannot kill a running job from
inside the guest.

## Implemented Core

- Detached warm VM `tart run` process groups for broker-spawned VMs.
- Reaper goroutines for warm VM boot commands.
- Startup adoption of running pool VMs through Tart list plus vsock liveness.
- Per-slot guest binding files for job id, run id, repo, and bound time.
- Adopted busy-slot preservation across broker restart.
- Control-plane shutdown without VM teardown.
- Guest watchdog stale-heartbeat logging instead of guest shutdown.

## Follow-On Supervisor Work

- Move VM lifecycle calls behind a long-lived launchd supervisor service.
- Expose a local supervisor API for boot, adopt, recycle, and status.
- Store durable slot state in the supervisor instead of only in guest binding
  files.
- Add an operator cleanup command that can intentionally stop abandoned idle VMs
  after proving no active job is running.
