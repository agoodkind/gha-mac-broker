#!/usr/bin/env bash
# Thin installer. Routes to go-makefile's hosted installer, which fetches and
# verifies go-mk-install, installs the gha-mac-broker release binary, then runs
# gha-mac-broker install to set up config and the user service.
set -euo pipefail
curl -fsSL https://raw.githubusercontent.com/agoodkind/go-makefile/main/install.sh \
    | bash -s -- --repo agoodkind/gha-mac-broker --binary gha-mac-broker "$@" -- install
