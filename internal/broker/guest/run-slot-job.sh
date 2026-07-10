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

# The isolated slot $HOME has no login keychain, so signing steps fail with
# "A default keychain could not be found" (security cms -D on provisioning
# profiles, codesign). Hosted runners ship a default login keychain; create one
# per slot $HOME. Per-slot $HOME keeps this default per-slot, so co-tenant slots
# never race on a shared default keychain. Best effort: a failure warns but does
# not abort the job, which then fails on signing as it does today.
setup_slot_keychain() {
    local slot_keychain="$HOME/Library/Keychains/login.keychain-db"
    mkdir -p "$HOME/Library/Keychains"
    if ! security show-keychain-info "$slot_keychain" >/dev/null 2>&1; then
        security create-keychain -p "" "$slot_keychain" || return 1
    fi
    security default-keychain -s "$slot_keychain" || return 1
    security list-keychains -d user -s "$slot_keychain" || return 1
    security unlock-keychain -p "" "$slot_keychain" || return 1
    security set-keychain-settings "$slot_keychain" || return 1
}
if ! setup_slot_keychain; then
    printf 'warning: slot keychain setup failed; signing steps may fail\n' >&2
fi

cd "$runner_home"
./run.sh --jitconfig {{JIT_CONFIG}}
