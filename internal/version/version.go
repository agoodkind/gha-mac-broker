// Package version exposes build identifiers populated via -ldflags at link
// time by go-makefile's go-build.mk.
package version

import "log/slog"

// Set via ldflags at build time (go-build.mk stamps Commit, Version, Dirty,
// BuildTime onto VPKG).
var (
	Commit    = "unknown"
	Version   = "dev"
	Dirty     = "false"
	BuildTime = "unknown"
)

// Attrs returns slog attributes for build metadata.
func Attrs() []slog.Attr {
	return []slog.Attr{
		slog.String("commit", Commit),
		slog.String("version", Version),
		slog.String("dirty", Dirty),
		slog.String("buildTime", BuildTime),
	}
}
