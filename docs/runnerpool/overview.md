# Runner Pool

The broker keeps a small set of macOS virtual machines warm and hands each incoming CI job to one of them. A machine can serve more than one job at the same time, and every job runs on a fresh, throwaway GitHub runner that is discarded when the job finishes, so no job ever inherits another job's state.

## Slot Isolation

When one machine serves more than one job at once, the jobs share the same computer and the same user account, so without care they would fight over the same caches and working directories under the home folder. To prevent that, each concurrent job gets its own home directory and runs entirely inside it. Two jobs on one machine never read or write the same files, so one job cannot corrupt or delete what another job is still using.

A per-job home starts warm rather than empty. The caches that make a build fast, its downloaded dependencies, installed tools, and build intermediates, are copied from a shared warm snapshot when the machine boots, so isolating the jobs adds no cold-start penalty. The build toolchain is the one thing left out of that copy, because CI restores it into each job on its own; sharing it through the snapshot as well would let two jobs collide on the same directory.

A machine configured to serve a single job keeps the shared home and behaves exactly as it did before per-job isolation existed.

## Recycling Stuck or Finished Work

A runner serves exactly one job and then exits, so a machine is reused across jobs but never carries one job's files or credentials into the next. When a job is cancelled, skipped, or never picked up, the machine bound to it is recycled: it is torn down and replaced with a fresh warm one. Recycling waits for any job still running on that machine to finish first, and a machine is never recycled while it is actually working, even past a timeout, so live jobs are protected and only genuinely idle or stuck machines are reclaimed.

## Detecting a Stuck Machine

A machine can get stuck when it registers a runner for a job but never receives the work: it then sits marked busy with nothing actually running. The broker publishes each machine's live state, so this shows up as a machine that has been bound to a job for longer than the pickup window while reporting no active work. That signature is what the recycling path watches for, and it is the same state a captured stuck specimen shows.
