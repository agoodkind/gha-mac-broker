package updateopts

import (
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"testing"

	"goodkind.io/gha-mac-broker/internal/version"
	"goodkind.io/go-makefile/selfupdate"
)

func TestOptionsUseBrokerReleaseIdentityAndValidator(t *testing.T) {
	oldVersion := version.Version
	oldCommit := version.Commit
	t.Cleanup(func() {
		version.Version = oldVersion
		version.Commit = oldCommit
	})
	version.Version = "202607030215-16-122a5cc"
	version.Commit = "122a5cc719a89ac61c17183fd66ddfe3844ae4cb"

	client := &http.Client{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	options := Options(Overrides{
		Client:      client,
		InstallPath: "/tmp/gha-mac-broker",
		DryRun:      true,
		Log:         log,
	})

	if options.Config.Repo != "agoodkind/gha-mac-broker" {
		t.Fatalf("repo = %q, want agoodkind/gha-mac-broker", options.Config.Repo)
	}
	if options.Config.Binary != "gha-mac-broker" {
		t.Fatalf("binary = %q, want gha-mac-broker", options.Config.Binary)
	}
	if options.Config.CurrentVersion != version.Version {
		t.Fatalf("current version = %q, want %q", options.Config.CurrentVersion, version.Version)
	}
	if options.Config.CurrentCommit != version.Commit {
		t.Fatalf("current commit = %q, want %q", options.Config.CurrentCommit, version.Commit)
	}
	if options.Config.CurrentBuildHash == "" {
		t.Fatal("current build hash is empty")
	}
	if options.Config.AllowPrerelease != nil {
		t.Fatalf("allow prerelease = %v, want nil for rolling default", *options.Config.AllowPrerelease)
	}
	if !reflect.DeepEqual(options.Config.ValidateArgs, []string{"version"}) {
		t.Fatalf("validate args = %v, want [version]", options.Config.ValidateArgs)
	}
	if options.Config.ValidateMatch != "gha-mac-broker " {
		t.Fatalf("validate match = %q, want gha-mac-broker ", options.Config.ValidateMatch)
	}
	if options.StatePath != selfupdate.DefaultStatePath("gha-mac-broker") {
		t.Fatalf("state path = %q, want library default", options.StatePath)
	}
	if options.CacheDir != selfupdate.DefaultCacheDir("gha-mac-broker") {
		t.Fatalf("cache dir = %q, want library default", options.CacheDir)
	}
	if options.Client != client {
		t.Fatal("client override was not preserved")
	}
	if options.InstallPath != "/tmp/gha-mac-broker" {
		t.Fatalf("install path = %q, want /tmp/gha-mac-broker", options.InstallPath)
	}
	if !options.DryRun {
		t.Fatal("dry run override was not preserved")
	}
	if options.Log != log {
		t.Fatal("log override was not preserved")
	}
}
