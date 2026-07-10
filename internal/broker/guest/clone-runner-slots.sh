#!/usr/bin/env bash
set -euo pipefail

# shellcheck disable=SC1083
slot_count={{SLOT_COUNT}}

brew_boot_refresh_marker="/tmp/swift-mk-brew-boot-refreshed"
# Clear any stale marker or pre-planted symlink at the predictable path. This
# runs at VM prep before any job registers, so no untrusted process is racing.
rm -f "$brew_boot_refresh_marker"

write_boot_refresh_marker() {
    # The marker must live at a fixed VM-wide path because the later per-job brew
    # step reads it from an isolated per-slot $HOME. Refuse to follow a symlink
    # at that path so a race cannot redirect the write to an arbitrary file, and
    # propagate a write failure so a failed marker leaves jobs to refresh
    # themselves.
    if [[ -L "$brew_boot_refresh_marker" ]]; then
        return 1
    fi
    : > "$brew_boot_refresh_marker"
}

refresh_homebrew_index() {
    local attempt_count=1
    local brew_output=""
    local max_attempts=3

    while [[ "$attempt_count" -le "$max_attempts" ]]; do
        if brew_output="$(brew update --quiet 2>&1)"; then
            write_boot_refresh_marker || return 1
            return 0
        fi

        if ! printf '%s\n' "$brew_output" | grep -Eiq "already locked|another active homebrew|another .* process is already running"; then
            return 0
        fi

        if [[ "$attempt_count" -eq "$max_attempts" ]]; then
            return 0
        fi

        sleep 5
        attempt_count=$((attempt_count + 1))
    done
}

if command -v brew >/dev/null 2>&1; then
    refresh_homebrew_index || true
fi

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
