# Runner Pool

The runner pool keeps a fixed set of persistent warm VMs and drains a FIFO job queue through [Pool](../../internal/runnerpool/pool.go). Each worker owns one or more slots. [runWorkerSlots](../../internal/runnerpool/pool.go) starts one [slotLoop](../../internal/runnerpool/pool.go) for each configured slot, and each slot launches a fresh ephemeral GitHub JIT runner for the bound job through [RunJob](../../internal/broker/bind.go). [finishSlotJob](../../internal/runnerpool/pool.go) records the completed slot as no longer busy.

## Reap Contract

A JIT runner serves one GitHub Actions job and then exits under the broker path in [RunJob](../../internal/broker/bind.go). A busy worker whose bound job is cancelled, skipped, or stale is reaped by marking that worker for recycle through [CancelRun](../../internal/runnerpool/pool.go), [reapBusyWorkers](../../internal/runnerpool/pool.go), [busyCandidates](../../internal/runnerpool/pool.go), and [requestBusyRecycle](../../internal/runnerpool/pool.go).

Webhook-driven reap starts when the server handles a completed workflow job whose conclusion is cancelled or skipped and calls [CancelRun](../../internal/server/server.go) with that workflow job id. Timeout-driven reap is governed by [Options.MaxBind](../../internal/runnerpool/pool.go) and [Options.PickupTimeout](../../internal/runnerpool/pool.go), and their defaults are defined by [normalizeOptions](../../internal/runnerpool/pool.go). A timeout reap first checks [ActiveJobProber](../../internal/runnerpool/pool.go), and a confirmed active job is not recycled. A recycle request is applied by [workerLoop](../../internal/runnerpool/pool.go) after all busy slots finish. [slotLoop](../../internal/runnerpool/pool.go) calls [finishSlotJob](../../internal/runnerpool/pool.go) as each job exits, then [teardownVM](../../internal/runnerpool/pool.go) stops the VM and the worker warms a replacement.

Idle registration checks compare GitHub runner names with the VM name through [runnerNameBelongsToVM](../../internal/runnerpool/pool.go), so stale base runner names and stale slot runner names both belong to the same VM during health reconciliation.

The reap contract is enforced by [TestCancelRunReapsMatchingBusyWorker](../../internal/runnerpool/pool_test.go), [TestCancelRunIgnoresSiblingJobWithSameRunID](../../internal/runnerpool/pool_test.go), [TestReconcileReapsBusyWorkerAfterPickupTimeoutWithoutActiveJob](../../internal/runnerpool/pool_test.go), [TestReconcileKeepsBusyWorkerWithActiveJobAfterPickupTimeout](../../internal/runnerpool/pool_test.go), [TestReconcileKeepsBusyWorkerWhenBindingChangesBeforeRecycleApply](../../internal/runnerpool/pool_test.go), [TestReconcileKeepsBusyWorkerAfterPickupTimeoutWhenProbeErrors](../../internal/runnerpool/pool_test.go), [TestReconcileKeepsBusyWorkerPastMaxBindWithActiveJob](../../internal/runnerpool/pool_test.go), [TestReconcileReapsBusyWorkerPastMaxBindWhenProbeErrors](../../internal/runnerpool/pool_test.go), and [TestCancelRunTearsDownAndRewarmsWorker](../../internal/runnerpool/pool_test.go).

## Wedge Signature

A pool wedge appears when [WorkerView](../../internal/runnerpool/pool.go) reports a worker as busy with a bound run id, a bind age beyond the pickup window, and no active job. That state suggests the runner registered for the job but never received work, and the pickup window is defined by [Options.PickupTimeout](../../internal/runnerpool/pool.go).

Live status comes from [GET /status](../../internal/server/server.go) or the [runStatus](../../cmd/gha-mac-broker/main.go) CLI path. The status payload is built by [Pool.Status](../../internal/runnerpool/pool.go), which returns each worker's phase, bound run id, bind age, and active-job flag through [WorkerView](../../internal/runnerpool/pool.go).

The active-job flag comes from [Binder.HasActiveJob](../../internal/broker/bind.go), which runs [activeJobProbeScript](../../internal/broker/bind.go). The probe avoids matching itself under [TestActiveJobProbeScriptAvoidsSelfMatch](../../internal/broker/bind_test.go).

Run console output is streamed into the host run log by [RunJob](../../internal/broker/bind.go) through [Tart.ExecTee](../../internal/tart/tart.go). The host log location and per-VM file name are defined by [openRunLog](../../internal/broker/bind.go).

## Worked Example

A captured wedge specimen with both workers busy, no active jobs, and a bind age beyond the pickup window matches the status shape reported by [WorkerView](../../internal/runnerpool/pool.go). That specimen points to a runner that registered but did not pick up a job under the broker contract in [RunJob](../../internal/broker/bind.go), so the reap path applies through [reapBusyWorkers](../../internal/runnerpool/pool.go) or [CancelRun](../../internal/runnerpool/pool.go).
