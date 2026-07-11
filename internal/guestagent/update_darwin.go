//go:build darwin

package guestagent

import (
	"errors"
	"log/slog"

	"golang.org/x/sys/unix"
)

// provenanceXattrs are the macOS gatekeeper attributes that would otherwise mark
// a freshly downloaded binary as quarantined or externally provisioned. Clearing
// them lets the reload spawn the new binary without a gatekeeper prompt or kill.
var provenanceXattrs = []string{"com.apple.quarantine", "com.apple.provenance"}

// removeProvenanceXattrs strips the gatekeeper attributes from the placed binary
// best effort. A missing attribute (ENOATTR) is the normal case and ignored; any
// other error is logged but never fails the update, since the bytes are already
// verified and the attribute is advisory.
func removeProvenanceXattrs(path string) {
	for _, name := range provenanceXattrs {
		err := unix.Removexattr(path, name)
		if err == nil || errors.Is(err, unix.ENOATTR) {
			continue
		}
		slog.Warn("guestagent clear update xattr failed", "path", path, "xattr", name, "err", err)
	}
}
