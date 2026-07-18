package golden

import _ "embed"

// Baked paths inside the golden image. The provisioner writes to these fixed
// locations and the guest agent reads the fingerprint from FingerprintPath, so
// both the host builder and the in-VM provisioner agree on one contract.
const (
	// BakedBinaryPath is where the provisioner installs the guest broker binary.
	// The guest-agent LaunchDaemon runs it.
	BakedBinaryPath = "/usr/local/bin/gha-mac-broker"
	// FingerprintPath is the baked golden fingerprint file the guest-agent reads
	// at startup and reports via Hello.
	FingerprintPath = "/usr/local/share/gha-guest/golden.fingerprint"
	// GuestAgentPlistLabel is the LaunchDaemon label for the guest agent.
	GuestAgentPlistLabel = "io.goodkind.gha-mac-broker-guest"
	// GuestAgentPlistPath is where the provisioner writes the guest-agent
	// LaunchDaemon. A plist under /Library/LaunchDaemons auto-loads on every boot.
	GuestAgentPlistPath = "/Library/LaunchDaemons/io.goodkind.gha-mac-broker-guest.plist"
	// LegacyWatchdogScriptPath is the retired watchdog shell script the provisioner
	// deletes so no .sh script is left baked in the image.
	LegacyWatchdogScriptPath = "/usr/local/bin/gha-broker-watchdog.sh"
	// LegacyWatchdogPlistPath is the retired watchdog LaunchDaemon the provisioner
	// deletes; the guest-agent KeepAlive unit replaces it.
	LegacyWatchdogPlistPath = "/Library/LaunchDaemons/io.goodkind.gha-broker-watchdog.plist"
)

//go:embed guest/io.goodkind.gha-mac-broker-guest.plist
var guestAgentPlist []byte

// GuestAgentPlist returns a copy of the baked guest-agent LaunchDaemon plist.
// The provisioner writes it into the image and the builder hashes it as a baked
// payload for the fingerprint, so both read the same embedded bytes.
func GuestAgentPlist() []byte {
	out := make([]byte, len(guestAgentPlist))
	copy(out, guestAgentPlist)
	return out
}
