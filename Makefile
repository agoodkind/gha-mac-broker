# Lint, build, test, deadcode, release, and service-install all live in the
# central go-makefile pipeline fetched at parse time. Do NOT add project-local
# lint, deadcode, audit, fmt, vet, or staticcheck targets here. Run `make help`
# for the canonical entry points.
#
# gha-mac-broker Makefile. Generic GitHub Actions macOS warm-pool JIT runner
# broker. Build/lint/release pipeline lives in go-makefile and is fetched at
# runtime; the daemon ships through the shared build/sign/install path.

# Optional local overrides (signing identity, GO_MK_DEV_DIR), never committed.
-include config.mk

# Identity
BINARY := gha-mac-broker
CMD    := ./cmd/$(BINARY)
VPKG   := goodkind.io/gha-mac-broker/internal/version

# Daemon identity. go-service.mk reads these at parse time, so they must be set
# BEFORE -include $(GO_MK).
LAUNCHD_LABEL := io.goodkind.gha-mac-broker
SYSTEMD_UNIT  := gha-mac-broker.service
LOG_PATH      := $(HOME)/Library/Logs/gha-mac-broker.log
BUNDLE_ID     ?= io.goodkind.gha-mac-broker

# Pipeline modules. go-service.mk supplies service-install/uninstall/restart.
GO_MK_MODULES := go-build.mk go-release.mk go-service.mk

# bootstrap.mk fetches go.mk + golangci.yml + every module in GO_MK_MODULES at
# parse time and -includes them. Set GO_MK_DEV_DIR in config.mk to build against
# a local go-makefile checkout without network access.
include bootstrap.mk

.DEFAULT_GOAL := check
