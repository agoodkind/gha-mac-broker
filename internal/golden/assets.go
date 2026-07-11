package golden

import _ "embed"

// Baked paths inside the golden image. The provisioner writes to these fixed
// locations and the guest supervisor reads the fingerprint from FingerprintPath,
// so both the host builder and the in-VM provisioner agree on one contract.
const (
	// BakedBinaryPath is where the provisioner installs the guest broker binary.
	// The guest-supervisor LaunchDaemon runs it.
	BakedBinaryPath = "/usr/local/bin/gha-mac-broker"
	// FingerprintPath is the baked golden fingerprint file the guest-supervisor
	// reads at startup and reports via Hello.
	FingerprintPath = "/usr/local/share/gha-guest/golden.fingerprint"
	// GuestSupervisorPlistLabel is the LaunchDaemon label for the guest supervisor.
	GuestSupervisorPlistLabel = "io.goodkind.gha-mac-broker-guest"
	// GuestSupervisorPlistPath is where the provisioner writes the guest-supervisor
	// LaunchDaemon. A plist under /Library/LaunchDaemons auto-loads on every boot.
	GuestSupervisorPlistPath = "/Library/LaunchDaemons/io.goodkind.gha-mac-broker-guest.plist"
	// LegacyWatchdogScriptPath is the retired watchdog shell script the provisioner
	// deletes so no .sh script is left baked in the image.
	LegacyWatchdogScriptPath = "/usr/local/bin/gha-broker-watchdog.sh"
	// LegacyWatchdogPlistPath is the retired watchdog LaunchDaemon the provisioner
	// deletes; the guest-supervisor KeepAlive unit replaces it.
	LegacyWatchdogPlistPath = "/Library/LaunchDaemons/io.goodkind.gha-broker-watchdog.plist"
)

//go:embed guest/io.goodkind.gha-mac-broker-guest.plist
var guestSupervisorPlist []byte

// GuestSupervisorPlist returns a copy of the baked guest-supervisor LaunchDaemon
// plist. The provisioner writes it into the image and the builder hashes it as a
// baked payload for the fingerprint, so both read the same embedded bytes.
func GuestSupervisorPlist() []byte {
	out := make([]byte, len(guestSupervisorPlist))
	copy(out, guestSupervisorPlist)
	return out
}
