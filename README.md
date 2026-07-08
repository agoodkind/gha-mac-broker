# gha-mac-broker

A generic GitHub Actions macOS warm-pool runner broker. One Mac keeps a fixed set
of pre-booted [Tart](https://tart.run) VMs and runs each queued job on a free VM
using repo-scoped just-in-time runner config. A VM is reused across many jobs, so
one VM serves gates from any installed repository over its life. One shared pool
serves many personal-account repositories without an organization.

## Why

GitHub self-hosted runners are scoped per repository, organization, or
enterprise; there is no native "all repos under one personal account" pool.
Repo-scoped JIT runner config (`POST /repos/{owner}/{repo}/actions/runners/generate-jitconfig`)
sidesteps that: a generic, already-booted VM becomes an ephemeral runner for one
repository and one job, with no reboot. A single GitHub App installed on the
chosen personal repos mints those configs centrally. The broker owns the warm
pool and does the per-repo binding.

The broker is the primary pool; CI workflows fail over to GitHub-hosted
`macos-26` when the pool is unavailable. That failover lives in the consumer's
reusable workflow, not in the broker.

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/agoodkind/gha-mac-broker/main/install.sh | bash
```

Published releases ship as signed release tarballs. The thin installer routes to
go-makefile's hosted installer, which verifies the release binary, installs it,
then runs `gha-mac-broker install` to scaffold the config, build the golden image,
install the user service, and restart it.

Host prerequisites are Tart and skopeo; on macOS install them with
`brew install cirruslabs/cli/tart skopeo`. swift-mk is only needed if you keep
the default maintenance command, and the maintenance timer is macOS-only, so it
is not required on Linux. To install swift-mk by hand:

```sh
curl -fsSL https://raw.githubusercontent.com/agoodkind/swift-makefile/main/install.sh | bash
```

`gha-mac-broker install` renders the service unit with the `-bin` path. The flag
defaults to the path of the executable running the install command. When the
installer launches a temporary copy, pass `-bin ~/.local/bin/gha-mac-broker` so
launchd points at the durable installed binary rather than the temporary file.

The service owns every warm VM as a `tart run` child process. Restarting it kills
running VMs, so a zero-kill deploy restarts only when no `tart exec ... run.sh`
job is active.

## Build

```sh
make build      # local build-check + compile + sign -> dist/gha-mac-broker
make check      # lint only
make test       # go test ./...
```

`make build` fetches the go-makefile pipeline at parse time. To build against a
local checkout without network, set `GO_MK_DEV_DIR` in `config.mk`.

## Configure

Copy `config.example.toml` to the default path and fill in the App credentials
and pool settings. The default path is `$XDG_CONFIG_HOME/gha-mac-broker/config.toml`
when `XDG_CONFIG_HOME` is set, otherwise `~/.config/gha-mac-broker/config.toml`;
override it with `-config`. Secrets are referenced by absolute file path (no
tilde expansion), never inlined.

While `serve` is running, the broker watches that config file and reloads valid
changes after the file mtime is stable across two polls. Pool timing knobs apply
without a restart. A `jobs_per_vm` change updates the target slot count, and
each warm VM keeps its current slots while busy, then recycles and returns with
the new slot count once it is idle. Running jobs are not cancelled. A
`runner_count` change still requires a process restart.

| Field | Meaning |
| --- | --- |
| `app.app_id` | GitHub App ID |
| `app.private_key_path` | PEM private key on disk |
| `app.webhook_secret_path` | file holding the webhook HMAC secret |
| `app.capacity_token_path` | file holding the bearer token required on `GET /status` and `gha-mac-broker status` |
| `runner_count` | number of persistent warm VMs the pool keeps booted (default 3, restart required for changes) |
| `jobs_per_vm` | runner slots per warm VM, resized as VMs go idle after a reload |
| `max_idle` | recycle an idle VM after this long (hygiene; the cache is a host mount, so this is free) |
| `max_age` | recycle a VM once it has run this long |
| `max_bind` | probe a busy worker after this long and recycle it only when active work is not confirmed |
| `pickup_timeout` | probe a newly bound busy worker after this long and recycle it when no active job exists |
| `stall_timeout` | warn when a live busy worker has no CPU progress for this long after pickup timeout, default 10m |
| `stall_reap` | recycle a stalled busy worker after the warning when true, default false |
| `tart.base_image` | Cirrus image the golden is built from (runner baked in, unconfigured) |
| `tart.cache_dir` | host dir mounted into each VM, survives VM deletion |

### Maintenance timer

The installer provisions a macOS launchd timer from the `[maintenance]` config
section. `command` is the shell line to run, and `interval_seconds` is the
launchd interval in seconds. Set `command = ""` to disable the timer.

## Subcommands

```sh
gha-mac-broker version
gha-mac-broker jitconfig -repo agoodkind/lmd
gha-mac-broker bind      -repo agoodkind/lmd
gha-mac-broker serve
gha-mac-broker status
# override the default XDG path with -config:
gha-mac-broker serve -config /path/to/config.toml
```

- `jitconfig` mints a repo-scoped JIT runner config and prints it. It exercises
  the full GitHub side (App JWT, installation token, generate-jitconfig) without
  a VM, so App credentials and permissions can be verified against live GitHub
  before the pool exists.
- `bind` clones a warm VM, registers it as an ephemeral runner, runs one job,
  and tears the VM down. It needs the golden image to be present.
- `status` prints the bearer-guarded `/status` snapshot from a running broker.

## Architecture

Runner pool details live in [docs/runnerpool/overview.md](docs/runnerpool/overview.md).

`serve` runs a fixed persistent worker pool. Each worker owns one warm VM cloned
from the golden and reuses it across many jobs. The webhook is the demand signal:
a `workflow_job.queued` delivery carrying a pool label is
enqueued (`internal/server/server.go`), and an idle worker pulls the next job,
mints a repo-scoped JIT config for that job's repo, and runs it on its VM over the
tart-exec vsock channel (`internal/runnerpool/pool.go`, `internal/broker/bind.go`).
The VM is not torn down between jobs, so `runner_count` VMs run `runner_count` jobs
at once and a fan-out drains across them in waves. Each job is a fresh JIT
registration to its own repository, so one generic VM serves any installed repo.

`GET /capacity` reports `runnerpool.Ready()`: true only when a worker slot is
genuinely idle for the next job (`idle > queued`), so a consumer's `plan-runners`
step routes to the pool only when it can serve immediately and falls back to
GitHub-hosted `macos-26` when the pool is full or down. The pool never reports the
optimistic bet that a busy slot will free soon, so a routed job never strands. That failover, plus a stranded-run backstop, lives in the
consumer's reusable workflow, not in the broker. Idle VMs recycle on `max_idle`,
`max_age`, a vsock liveness failure, or a stale GitHub runner registration; the
build cache is a host mount, so recycling never cold-starts the cache. Busy VMs
recycle on `pickup_timeout` or `max_bind` only after the active-job probe reports
no active job, except that a `max_bind` probe error is treated as stale work.
