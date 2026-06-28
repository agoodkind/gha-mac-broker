#!/usr/bin/env bash
# gha-broker guest watchdog. launchd runs this on a short interval inside each
# pool VM. While the broker owns the VM it touches HEARTBEAT_FILE over the
# tart-exec vsock channel; if the broker dies the file goes stale and this VM
# self-terminates, so a dead broker never leaves a live orphan.

set -euo pipefail

HEARTBEAT_FILE="/tmp/gha-broker.alive"
STALE_AFTER_SECONDS=60

# Do nothing until the broker has provisioned the VM (the file first appears).
# This avoids self-terminating during boot, before the broker's first touch.
if [[ ! -f "$HEARTBEAT_FILE" ]]; then
    exit 0
fi

now="$(date +%s)"
mtime="$(stat -f %m "$HEARTBEAT_FILE")"
age=$(( now - mtime ))

if (( age > STALE_AFTER_SECONDS )); then
    /sbin/shutdown -h now
fi
