package main

import (
	"log/slog"
	"os"
	"strings"

	"goodkind.io/gha-mac-broker/internal/version"
	"goodkind.io/gklog"
)

// logLevelEnv overrides the default log level when set. It accepts DEBUG, INFO,
// WARN, or ERROR (case-insensitive); an empty or unrecognised value keeps INFO,
// so raising verbosity to diagnose a live issue is a service-env change with no
// rebuild.
const logLevelEnv = "GHA_MAC_BROKER_LOG"

// logLevelNames maps the recognised GHA_MAC_BROKER_LOG values (upper-cased) to
// their slog level.
var logLevelNames = map[string]slog.Level{
	"DEBUG":   slog.LevelDebug,
	"INFO":    slog.LevelInfo,
	"WARN":    slog.LevelWarn,
	"WARNING": slog.LevelWarn,
	"ERROR":   slog.LevelError,
}

// parseLogLevel maps a GHA_MAC_BROKER_LOG value to a slog level. Only the
// recognised names (case-insensitive, surrounding whitespace ignored) change the
// level; every other value, including a typo, keeps INFO so an operator mistake
// never silently suppresses logs. This is deliberately stricter than
// gklog.ParseLevel, which defaults unrecognised input to WARN.
func parseLogLevel(raw string) slog.Level {
	if level, ok := logLevelNames[strings.ToUpper(strings.TrimSpace(raw))]; ok {
		return level
	}
	return slog.LevelInfo
}

// setupLogging installs the process-wide structured logger. Records are JSON on
// stderr (captured by the launchd/systemd service log) through gklog, gated by
// logLevelEnv (default INFO) and stamped with the build version so logs from
// different deploys stay distinguishable. The gklog closer is intentionally
// dropped: the stderr handler holds no file resource to release, and main exits
// via [os.Exit] on several paths where a deferred close would not run anyway.
func setupLogging() {
	level := parseLogLevel(os.Getenv(logLevelEnv))
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	logger, _ := gklog.New(gklog.Config{
		BuildVersion: version.Version,
		Handlers:     []slog.Handler{handler},
	})
	slog.SetDefault(logger)
}
