#!/usr/bin/env bash
set -euo pipefail

# SwiftPM clone URLs already include the GitHub token, so no credential helper
# is needed. Clear credential.helper for this process tree so
# git-credential-manager is never invoked, since its credential store path can
# deadlock in the headless VM. Keep terminal prompts off so a 401 fails fast.
export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0=credential.helper
export GIT_CONFIG_VALUE_0=
export GIT_TERMINAL_PROMPT=0

base_home="$HOME"
runner_home="$base_home/actions-runner-{{SLOT_INDEX}}"
export TMPDIR="$base_home/tmp-{{SLOT_INDEX}}"
mkdir -p "$TMPDIR"

# Use a per-slot $HOME for co-tenant cache isolation.
export HOME="$base_home/slot-home-{{SLOT_INDEX}}"
mkdir -p "$HOME"

cd "$runner_home"
./run.sh --jitconfig {{JIT_CONFIG}}
