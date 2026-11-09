package golden

import (
	"context"
	"os/exec"
	"time"

	"goodkind.io/gha-mac-broker/internal/tart"
)

const (
	readyTimeout    = 120 * time.Second
	readyInterval   = 3 * time.Second
	runnerLatestURL = "https://api.github.com/repos/actions/runner/releases/latest"
	// provisionMountName selects the share for the signed guest binary.
	provisionMountName = "ghaprovision"
	// tartSharedMountRoot stores the guest root for Tart directory shares.
	tartSharedMountRoot = "/Volumes/My Shared Files"
	// provisionBinaryName is the guest binary filename inside the scratch mount.
	provisionBinaryName = "gha-mac-broker"
	// localProvisionerPath selects the guest path for the provisioner binary.
	localProvisionerPath = "/tmp/gha-mac-broker-provision"
	// runnerTarballTimeout bounds the host-side runner tarball fetch used only to
	// compute the fingerprint digest.
	runnerTarballTimeout = 10 * time.Minute
	// goldenStagingSuffix identifies the snapshot verified before promotion.
	goldenStagingSuffix = "-staging"
	probeCleanupTimeout = 30 * time.Second
)

// tarter is the VM substrate the builder drives; *tart.Tart satisfies it. It is
// an interface so tests can stub the tart CLI.
type tarter interface {
	List(ctx context.Context) ([]string, error)
	Clone(ctx context.Context, source, name string, insecure bool) error
	BootCommand(ctx context.Context, name string, opts tart.BootOptions) *exec.Cmd
	Exec(ctx context.Context, name string, argv ...string) ([]byte, error)
	IP(ctx context.Context, name string) (string, error)
	Stop(ctx context.Context, name string) error
	Delete(ctx context.Context, name string) error
}

// BaseStager stages a base image into a local registry and returns a clonable
// ref plus a stop func that shuts the registry down.
type BaseStager interface {
	Stage(ctx context.Context, image string) (ref string, stop func(), err error)
}

// runnerTarballDigester computes the content digest for one actions/runner
// release archive.
type runnerTarballDigester func(ctx context.Context, version string) (string, error)

// Option configures a Builder.
type Option func(*Builder)

// Builder builds and self-verifies the golden image.
type Builder struct {
	vm            tarter
	base          BaseStager
	runnerDigest  runnerTarballDigester
	resolveRunner func(ctx context.Context) (string, error)
}

// Options configures a golden build.
type Options struct {
	// BaseImage is the Cirrus base to clone (ships tart-guest-agent).
	BaseImage string
	// GoldenName is the snapshot the pool clones (e.g. "gha-golden").
	GoldenName string
	// BuildVM is the scratch VM name used during the build.
	BuildVM string
	// RunnerVersion is the actions/runner version to install (e.g. "2.335.1").
	RunnerVersion string
	// BinaryPath is the guest broker binary baked into the image. When empty the
	// running executable is baked, so a bare host bootstrap needs no extra flag.
	BinaryPath string
}

// EnsureOptions configures an idempotent golden ensure operation.
type EnsureOptions struct {
	// Image is the approved Cirrus tag the golden is derived from.
	Image string
	// BuildVM is the scratch VM name used if the golden must be built.
	BuildVM string
	// RunnerVersion is the actions/runner version to install. When empty, the
	// latest release is resolved only if a build is required.
	RunnerVersion string
}

// runnerRelease is the subset of the actions/runner latest-release API response
// the version resolver reads.
type runnerRelease struct {
	TagName string `json:"tag_name"`
}
