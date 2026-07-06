#!/usr/bin/env bash
set -euo pipefail

# Fail fast on any git credential request instead of hanging the slot. A headless
# CI VM cannot answer git-credential-manager's interactive prompt, so a git 401
# (for example a GitHub rate-limit on an unauthenticated clone) would otherwise
# wedge the slot at 0% CPU until the bind timeout.
export GCM_INTERACTIVE=never
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
