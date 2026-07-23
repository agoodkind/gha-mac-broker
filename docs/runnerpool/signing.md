# Signing on the pool

Code signing works on a pool machine because the runner runs in the machine's graphical login session as the daemon user, with that user's real home folder. In that session `codesign` finds the signing identity a CI job imports; in the machine's background service session it cannot.

## Why the session decides

The guest agent is a background service, so a runner it launches directly would inherit the background service session. In that session the keychain search list is fixed to the system keychain, a job's own signing keychain never enters it, and `codesign` fails fast with "no identity found" even when the identity is present and the keychain is unlocked.

The one graphical login session on each machine is the only session where a job can add its signing keychain to the search list and sign. The runner launch therefore moves the runner into that session before `run.sh` starts, while the guest agent stays the single background service that binds, reaps, and cancels every job. [`wrapRunnerForAquaSession`](../../internal/guestagent/runner.go) builds that launch.

## Why the home folder decides

`codesign` resolves the signing identity through a per-user keychain search list, and that list resolves through the login user's real home folder, not the launched process's `HOME`. When the runner's `HOME` points at a per-slot home folder, the job's `security list-keychains` writes against that folder, the login session's effective search list never gains the signing keychain, and signing fails.

A machine that serves one job runs the runner with the real home folder, so the signing keychain enters the search list and the job signs. A machine that serves more than one job keeps a per-slot home folder for cache isolation, so its signing keychain does not reach the login session's search list. [`runnerEnv`](../../internal/guestagent/runner.go) selects the home folder by slot count.

## Multi-slot signing is deferred

The keychain search list is per-user and shared across every process of that user, so two concurrent jobs on one machine share one list. This is why the single-job case uses the real home safely and the multi-job case does not yet sign.

Two concurrent jobs that sign with the same identity both succeed, because whichever keychain wins the shared list still holds that identity. Two sequential jobs on a reused machine sign with different identities, because each job rewrites the search list and the earlier job has already finished. Two concurrent jobs that sign with different identities cannot both succeed on one user account, because each job's `security list-keychains` evicts the other's keychain. True multi-slot signing isolation needs a separate user account per slot, which is not yet implemented; today the pool runs one job per machine.

## The broker holds no signing identity

The broker imports no certificate and holds no identity. It only places the runner in a session and home folder where a job's own signing keychain resolves, so the signing workflow needs no pool-specific step. The job's signing action provides the keychain and identity.
