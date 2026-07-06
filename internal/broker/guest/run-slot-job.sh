#!/usr/bin/env bash
set -euo pipefail

base_home="$HOME"
runner_home="$base_home/actions-runner-{{SLOT_INDEX}}"
export TMPDIR="$base_home/tmp-{{SLOT_INDEX}}"
mkdir -p "$TMPDIR"

# Use a per-slot $HOME for co-tenant cache isolation.
export HOME="$base_home/slot-home-{{SLOT_INDEX}}"
mkdir -p "$HOME"

cd "$runner_home"
./run.sh --jitconfig {{JIT_CONFIG}}
