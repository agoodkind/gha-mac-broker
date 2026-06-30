#!/usr/bin/env bash
# Build a minimal static libaria2 from the vendored release tarball into a
# per-target prefix. HTTP/HTTPS only (AppleTLS on darwin), no BitTorrent,
# Metalink, or optional libraries, so the static archive is small and the link
# closure is just libc++ plus the AppleTLS frameworks. The CGO wrapper in
# internal/aria2 links the resulting libaria2.a. Idempotent: a present archive
# is left untouched.
set -euo pipefail

src_dir="$1"
ver="$2"
prefix="$3"

lib="${prefix}/lib/libaria2.a"
if [[ -f "${lib}" ]]; then
    echo "libaria2 present: ${lib}"
    exit 0
fi

tarball="${src_dir}/aria2-${ver}.tar.xz"
if [[ ! -f "${tarball}" ]]; then
    echo "build-libaria2: missing ${tarball}" >&2
    exit 1
fi

goos="${GOOS:-$(go env GOOS)}"
goarch="${GOARCH:-$(go env GOARCH)}"

work="${src_dir}/.build/src-${goos}-${goarch}"
rm -rf "${work}"
mkdir -p "${work}"
tar xf "${tarball}" -C "${work}"
cd "${work}/aria2-${ver}"

conf=(./configure
    "--prefix=${prefix}"
    --enable-libaria2
    --enable-static
    --disable-shared
    --without-gnutls
    --without-openssl
    --without-libgcrypt
    --without-libnettle
    --without-libgmp
    --without-libssh2
    --without-libcares
    --without-sqlite3
    --without-libxml2
    --without-libexpat
    --without-libz
    --disable-bittorrent
    --disable-metalink
    --disable-websocket)

if [[ "${goos}" == "darwin" ]]; then
    conf+=(--with-appletls)
fi

# Cross-compile when a cross compiler is provided (CI osxcross sets CC/CXX).
if [[ -n "${CC:-}" ]]; then
    case "${goarch}" in
        arm64) host="aarch64-apple-darwin" ;;
        amd64) host="x86_64-apple-darwin" ;;
        *) host="" ;;
    esac
    if [[ -n "${host}" ]]; then
        conf+=("--host=${host}")
    fi
    conf+=("CC=${CC}" "CXX=${CXX:-${CC}}")
fi

conf+=("CXXFLAGS=-std=c++14 -O2")

"${conf[@]}"
make -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
make install
echo "build-libaria2: built ${lib}"
