// Package updateopts adapts gha-mac-broker build metadata to selfupdate options.
package updateopts

import (
	"log/slog"
	"net/http"

	"goodkind.io/gha-mac-broker/internal/version"
	"goodkind.io/go-makefile/selfupdate"
)

const (
	updateRepo       = "agoodkind/gha-mac-broker"
	updateBinary     = "gha-mac-broker"
	currentBuildHash = "unknown"
)

// Overrides carries operation-specific update settings.
type Overrides struct {
	Client      *http.Client
	InstallPath string
	DryRun      bool
	Log         *slog.Logger
}

// Options builds selfupdate options using the library default state and cache
// paths because gha-mac-broker has no repo-specific update state convention.
func Options(overrides Overrides) selfupdate.Options {
	return selfupdate.Options{
		Config: selfupdate.Config{
			Repo:             updateRepo,
			Binary:           updateBinary,
			CurrentVersion:   version.Version,
			CurrentCommit:    version.Commit,
			CurrentBuildHash: currentBuildHash,
			AllowPrerelease:  nil,
			ValidateArgs:     []string{"version"},
			ValidateMatch:    "gha-mac-broker ",
		},
		Client:      overrides.Client,
		InstallPath: overrides.InstallPath,
		CacheDir:    selfupdate.DefaultCacheDir(updateBinary),
		StatePath:   selfupdate.DefaultStatePath(updateBinary),
		DryRun:      overrides.DryRun,
		Log:         overrides.Log,
	}
}
