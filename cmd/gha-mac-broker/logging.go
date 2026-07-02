package main

import (
	"log/slog"
	"os"

	"goodkind.io/gha-mac-broker/internal/version"
	"goodkind.io/gklog"
)

// logLevelEnv overrides the default log level when set. It accepts DEBUG, INFO,
// WARN, or ERROR (case-insensitive); an empty or unrecognised value keeps INFO,
// so raising verbosity to diagnose a live issue is a service-env change with no
// rebuild.
const logLevelEnv = "GHA_MAC_BROKER_LOG"

// setupLogging installs the process-wide structured logger. Records are JSON on
// stderr (captured by the launchd/systemd service log) through gklog, gated by
// logLevelEnv (default INFO) and stamped with the build version so logs from
// different deploys stay distinguishable. The gklog closer is intentionally
// dropped: the stderr handler holds no file resource to release, and main exits
// via [os.Exit] on several paths where a deferred close would not run anyway.
func setupLogging() {
	level := slog.LevelInfo
	if raw := os.Getenv(logLevelEnv); raw != "" {
		level = gklog.ParseLevel(raw)
	}
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	logger, _ := gklog.New(gklog.Config{
		BuildVersion: version.Version,
		Handlers:     []slog.Handler{handler},
	})
	slog.SetDefault(logger)
}
