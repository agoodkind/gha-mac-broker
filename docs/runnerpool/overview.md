# Runner Pool

The broker keeps a small set of macOS virtual machines warm and hands each incoming CI job to one of them. A machine can serve more than one job at the same time. Each job gets a fresh, single-use GitHub runner registration, and the machine is reused across jobs. A slot's home directory persists across the jobs that run in it and resets only when the machine recycles (see [Runner Slots](../slots.md)).

## Warm Pool

The pool keeps `runner_count` Tart virtual machines booted from the golden image. Each machine exposes `jobs_per_vm` runner slots, so the total warm capacity is the number of machines multiplied by the slot count. A queued workflow job waits in the broker queue until an idle slot is available.

Binding a job mints a repository-scoped just-in-time runner config and runs the runner over the Tart vsock channel. The broker uses `tart exec` to run `run.sh` inside the warm guest, so the VM needs no IP address and no SSH server. Each job gets a fresh GitHub runner registration even when the VM itself stays warm for the next job.

Each warm VM is a `tart run` child process of the `gha-mac-broker serve` process. A broker restart therefore tears down running VMs. A deploy that must avoid killing jobs waits until no `tart exec ... run.sh` job is active before it restarts the service.

## Capacity and Control

`GET /capacity` reports whether the pool can serve the next job immediately. The endpoint returns `pool.Ready()`, and `Ready` is true only when idle slots exceed queued jobs, so queued work already accepted by the broker consumes capacity before a new caller routes to the pool.

`GET /status` exposes the full pool snapshot and worker rows, so it requires the configured bearer token. `POST /webhook` accepts GitHub `workflow_job` deliveries only after the HMAC signature verifies, then enqueues matching queued jobs and cancels matching cancelled or skipped jobs.

## Live Config Reload

The daemon watches the config file and applies a valid reload after the file modification time stays stable across two polls. Timing knobs and `jobs_per_vm` apply without a process restart. A `jobs_per_vm` change marks an idle worker for replacement when its slot count differs, while a busy worker drains its current jobs before it recycles with the new slot count.

Changing `runner_count` or the base image requires a service restart. The reload path logs that requirement and keeps the running pool size and image until the process restarts.

## Slot Isolation

When one machine serves more than one job at once, the jobs share the same computer and the same user account, so without care they would fight over the same caches and working directories under the home folder. To prevent that, each slot has its own fixed home directory, and every job in a slot runs entirely inside it. Two slots on one machine never read or write the same files, so concurrent jobs cannot corrupt or delete what another is using.

A slot's home starts warm rather than empty. The broker provisions a slot on its first job, cloning the caches that make a build fast, its downloaded dependencies, installed tools, and build intermediates, from a shared warm snapshot, then reuses that home for later jobs in the slot. The build toolchain is the one thing left out of that copy, because CI restores it into each job on its own; sharing it through the snapshot as well would let two slots collide on the same directory.

A machine configured to serve a single job uses one slot home, reused across its jobs, with no co-tenant slot to isolate from.

## Recycling Stuck or Finished Work

A runner registration serves exactly one job and then exits, so each job gets fresh runner credentials. The machine and its slot homes are reused across jobs, and a slot's files persist until the machine recycles. When a job is cancelled, skipped, or never picked up, the machine bound to it is recycled: it is torn down and replaced with a fresh warm one. Recycling waits for any job still running on that machine to finish first, and a machine is never recycled while it is actually working, even past a timeout, so live jobs are protected and only genuinely idle or stuck machines are reclaimed.

## Detecting a Stuck Machine

A machine can get stuck when it registers a runner for a job but never receives the work: it then sits marked busy with nothing actually running. The broker publishes each machine's live state, so this shows up as a machine that has been bound to a job for longer than the pickup window while reporting no active work. That signature is what the recycling path watches for, and it is the same state a captured stuck specimen shows.

## Detecting a Stalled Job

A live job can also stop making CPU progress while the runner process still exists. After the pickup timeout, the broker probes the slot's runner process tree and records when CPU activity stays below the stall threshold. Once that low-CPU window reaches `stall_timeout`, the broker logs a warning. The default timeout is ten minutes, and `stall_reap` is false by default, so a stalled active job is observed but not recycled unless the operator opts in.
