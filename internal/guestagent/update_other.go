//go:build !darwin

package guestagent

// removeProvenanceXattrs is a no-op off darwin. The gatekeeper attributes it
// clears on the guest only exist on macOS, and the guest agent runs on macOS;
// this stub keeps the package building for host-side tooling on other systems.
func removeProvenanceXattrs(_ string) {}
