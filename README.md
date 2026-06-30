# gha-mac-broker

A generic GitHub Actions macOS warm-pool runner broker. One Mac keeps a pool of
pre-booted [Tart](https://tart.run) VMs and binds a free VM to whichever
repository just queued a job, using repo-scoped just-in-time runner config. One
shared pool serves many personal-account repositories without an organization.

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

The script downloads the signed binary from the latest release into
`${XDG_BIN_HOME:-$HOME/.local/bin}` and runs `gha-mac-broker install`, which
scaffolds the config and secrets, builds the golden Tart image when missing,
and installs the launchd (macOS) or systemd (Linux) user service. Every step is
idempotent. Pass `--version <tag>` to pin a release, `--bin-dir <dir>` to change
the install location, or `--no-service` to install only the binary. After
install, set `app.app_id` in the config, place the App private key at
`app.private_key_path`, and set up the Cloudflare tunnel.

Host prerequisites are Tart and skopeo. On macOS, install them with
`brew install cirruslabs/cli/tart skopeo`.

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

| Field | Meaning |
| --- | --- |
| `app.app_id` | GitHub App ID |
| `app.private_key_path` | PEM private key on disk |
| `app.webhook_secret_path` | file holding the webhook HMAC secret |
| `tart.golden_image` | source VM the pool clones (runner installed, unconfigured) |
| `tart.cache_dir` | host dir shared into each VM, survives VM deletion |
| `tart.ssh_key_path` | private key the broker uses to control the VM |
| `allowed_repos` | `owner/repo` allowlist the broker will serve |
| `pool_size` | number of warm VMs kept booted |

## Subcommands

```sh
gha-mac-broker version
gha-mac-broker jitconfig -repo agoodkind/lmd
gha-mac-broker bind      -repo agoodkind/lmd
gha-mac-broker serve
# override the default XDG path with -config:
gha-mac-broker serve -config /path/to/config.toml
```

- `jitconfig` mints a repo-scoped JIT runner config and prints it. It exercises
  the full GitHub side (App JWT, installation token, generate-jitconfig) without
  a VM, so App credentials and permissions can be verified against live GitHub
  before the pool exists.
- `bind` clones a warm VM, registers it as an ephemeral runner, runs one job,
  and tears the VM down. It needs the golden image to be present.

## Status

The webhook server, warm pool, and `/capacity` reservation endpoint are not yet
implemented; `bind` is the single-shot primitive they will drive.
