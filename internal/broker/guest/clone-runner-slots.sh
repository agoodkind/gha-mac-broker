#!/usr/bin/env bash
set -euo pipefail

slot_count={{SLOT_COUNT}}
slot_index=0

# Warm by-presence caches seeded into each slot's isolated $HOME via APFS clone,
# so co-tenant slots never share these dirs yet each starts warm. The swift-mk
# toolchain is intentionally absent: it is keyed by source hash and restored
# per-slot by actions/cache, so seeding it would only risk a seed/cache merge.
warm_cache_paths=(
    ".local"
    ".swiftpm"
    ".cache"
    "Library/Caches/org.swift.swiftpm"
    "Library/Caches/Homebrew"
    "Library/Developer/Xcode/DerivedData"
    ".gitconfig"
    ".netrc"
)

while [[ "$slot_index" -lt "$slot_count" ]]; do
    runner_home="$HOME/actions-runner-$slot_index"
    tmp_dir="$HOME/tmp-$slot_index"
    rm -rf "$runner_home" "$tmp_dir"
    cp -R "$HOME/actions-runner" "$runner_home"
    mkdir -p "$tmp_dir"

    # Seed per-slot $HOME for co-tenant cache isolation; APFS clone keeps it warm and cheap.
    slot_home="$HOME/slot-home-$slot_index"
    rm -rf "$slot_home"
    mkdir -p "$slot_home"
    for warm_cache_path in "${warm_cache_paths[@]}"; do
        source_path="$HOME/$warm_cache_path"
        if [[ -e "$source_path" ]]; then
            dest_path="$slot_home/$warm_cache_path"
            mkdir -p "$(dirname "$dest_path")"
            if ! cp -cR "$source_path" "$dest_path"; then
                rm -rf "$dest_path"
                cp -R "$source_path" "$dest_path"
            fi
        fi
    done

    slot_index=$((slot_index + 1))
done
