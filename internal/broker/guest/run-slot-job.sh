#!/usr/bin/env bash
set -euo pipefail

runner_home="$HOME/actions-runner-{{SLOT_INDEX}}"
export TMPDIR="$HOME/tmp-{{SLOT_INDEX}}"
mkdir -p "$TMPDIR"
cd "$runner_home"
./run.sh --jitconfig {{JIT_CONFIG}}
