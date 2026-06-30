# Lint, build, test, deadcode, and release all live in the central go-makefile
# pipeline fetched at parse time. Do NOT add project-local lint, deadcode,
# audit, fmt, vet, or staticcheck targets here. Run `make help` for the
# canonical entry points.
#
# gha-mac-broker Makefile. Generic GitHub Actions macOS warm-pool JIT runner
# broker. Build/lint/release pipeline lives in go-makefile and is fetched at
# runtime. The binary owns host service install/uninstall via its own `install`
# subcommand, so no host service module is wired here.

# Optional local overrides (signing identity, GO_MK_DEV_DIR), never committed.
-include config.mk

# Identity
BINARY := gha-mac-broker
CMD    := ./cmd/$(BINARY)
VPKG   := goodkind.io/gha-mac-broker/internal/version

BUNDLE_ID ?= io.goodkind.gha-mac-broker

# Pipeline modules. Service install/uninstall is owned by the binary's own
# `install` subcommand, not a host make module.
GO_MK_MODULES := go-build.mk go-release.mk

# CGO: a vendored static libaria2 is linked into the binary for the fast
# parallel base-image pull. GO_MK_GENERATE makes go.mk build libaria2 as an
# order-only prerequisite before any compile, lint, or test. The link flags live
# in internal/aria2 #cgo directives (package-scoped), NOT exported here, so
# building unrelated tools such as golangci-lint does not inherit the libaria2
# link. Only CGO_ENABLED is exported. The build prefix is stable (not per-arch):
# each build context targets a single arch, and the #cgo directive references
# this path SRCDIR-relative.
export CGO_ENABLED := 1
ARIA2_VER    := 1.37.0
ARIA2_DIR    := third_party/aria2
ARIA2_PREFIX := $(CURDIR)/$(ARIA2_DIR)/.build
ARIA2_LIB    := $(ARIA2_PREFIX)/lib/libaria2.a
GO_MK_GENERATE := libaria2

# bootstrap.mk fetches go.mk + golangci.yml + every module in GO_MK_MODULES at
# parse time and -includes them. Set GO_MK_DEV_DIR in config.mk to build against
# a local go-makefile checkout without network access.
include bootstrap.mk

.DEFAULT_GOAL := check

# Build the vendored static libaria2 into the per-target prefix. Idempotent: an
# existing archive is left untouched. go.mk runs this before any compile.
.PHONY: libaria2
libaria2: $(ARIA2_LIB)
$(ARIA2_LIB):
	@bash scripts/build-libaria2.sh "$(ARIA2_DIR)" "$(ARIA2_VER)" "$(ARIA2_PREFIX)"
