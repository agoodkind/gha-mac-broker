#!/usr/bin/env bash
set -euo pipefail

slot_count={{SLOT_COUNT}}
slot_index=0

while [[ "$slot_index" -lt "$slot_count" ]]; do
    runner_home="$HOME/actions-runner-$slot_index"
    tmp_dir="$HOME/tmp-$slot_index"
    rm -rf "$runner_home" "$tmp_dir"
    cp -R "$HOME/actions-runner" "$runner_home"
    mkdir -p "$tmp_dir"
    slot_index=$((slot_index + 1))
done
