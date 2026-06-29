#!/usr/bin/env bash
#
# install.sh downloads the signed gha-mac-broker binary from the latest GitHub
# release and runs its `install` subcommand, which owns the full host setup
# (config, secrets, golden image, and the launchd/systemd service).
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/agoodkind/gha-mac-broker/main/install.sh | bash
#
# Local checkout:
#   ./install.sh [flags]
#
# Flags:
#   --version TAG     pin to a specific release tag (default: latest)
#   --bin-dir DIR     override binary install dir (default: $XDG_BIN_HOME or
#                     $HOME/.local/bin)
#   --no-service      install the binary only; skip `gha-mac-broker install`
#   -h, --help        show this help
#
# Exit codes:
#   0 success
#   1 usage / unsupported platform
#   2 download / extract / install failure

set -euo pipefail

REPO="agoodkind/gha-mac-broker"
BIN_DIR="${XDG_BIN_HOME:-$HOME/.local/bin}"
VERSION=""
DO_SERVICE=1

usage() {
    sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
}

die() {
    printf 'install.sh: %s\n' "$*" >&2
    exit 1
}

need() {
    command -v "$1" >/dev/null 2>&1 || die "missing dependency: $1"
}

detect_platform() {
    local os arch
    case "$(uname -s)" in
        Darwin) os=darwin ;;
        Linux)  os=linux ;;
        *) die "unsupported OS: $(uname -s)" ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64)  arch=amd64 ;;
        arm64|aarch64) arch=arm64 ;;
        *) die "unsupported arch: $(uname -m)" ;;
    esac
    printf '%s_%s' "$os" "$arch"
}

resolve_version() {
    if [[ -n "$VERSION" ]]; then
        printf '%s' "$VERSION"
        return
    fi
    # The releases list is newest-first and includes pre-releases, which the
    # /releases/latest endpoint excludes; the broker publishes pre-releases.
    # Parse the first tag_name with grep/sed so a fresh host needs no jq.
    curl -fsSL "https://api.github.com/repos/$REPO/releases?per_page=1" \
        | grep -m1 '"tag_name"' \
        | sed -E 's/.*"tag_name"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/' \
        || die "failed to query latest release from $REPO"
}

install_bin() {
    local platform tag url tmpdir tarball extracted
    platform="$(detect_platform)"
    tag="$(resolve_version)"
    if [[ -z "$tag" ]]; then
        die "could not resolve release tag (use --version)"
    fi

    url="https://github.com/$REPO/releases/download/$tag/gha-mac-broker_${platform}.tar.gz"
    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' RETURN

    tarball="$tmpdir/gha-mac-broker.tar.gz"
    printf 'install.sh: downloading %s\n' "$url"
    curl -fsSL "$url" -o "$tarball" || die "download failed: $url"
    tar -xzf "$tarball" -C "$tmpdir" || die "extract failed: $tarball"

    extracted="$tmpdir/gha-mac-broker"
    if [[ ! -x "$extracted" ]]; then
        die "binary not found in tarball at $extracted"
    fi

    mkdir -p "$BIN_DIR"
    install -m 0755 "$extracted" "$BIN_DIR/gha-mac-broker"
    printf 'install.sh: installed %s (%s)\n' "$BIN_DIR/gha-mac-broker" "$tag"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)    shift; VERSION="${1:?--version requires a value}" ;;
        --bin-dir)    shift; BIN_DIR="${1:?--bin-dir requires a value}" ;;
        --no-service) DO_SERVICE=0 ;;
        -h|--help)    usage; exit 0 ;;
        *) die "unknown flag: $1 (try --help)" ;;
    esac
    shift
done

need curl
need tar

install_bin

if [[ "$DO_SERVICE" -eq 1 ]]; then
    "$BIN_DIR/gha-mac-broker" install || die "gha-mac-broker install failed"
fi

printf 'install.sh: done\n'
