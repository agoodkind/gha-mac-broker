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

# bootstrap.mk fetches go.mk + golangci.yml + every module in GO_MK_MODULES at
# parse time and -includes them. Set GO_MK_DEV_DIR in config.mk to build against
# a local go-makefile checkout without network access.
include bootstrap.mk

.DEFAULT_GOAL := check

.PHONY: buf-generate buf-lint buf-breaking

# Generated protobuf and ConnectRPC code is committed.
buf-generate:
	buf generate

buf-lint:
	buf lint

buf-breaking:
	git fetch origin
	@if git cat-file -e origin/main:api/guest/v1/guest.proto 2>/dev/null; then \
	    buf breaking --against '.git#ref=origin/main'; \
	else \
	    echo 'No guest protobuf baseline exists on origin/main; skipping.'; \
	fi
