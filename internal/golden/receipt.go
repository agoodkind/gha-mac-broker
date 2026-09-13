package golden

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// RunnerReceiptPath records the verified runner inputs inside the golden image.
const RunnerReceiptPath = "/usr/local/share/gha-guest/runner.json"

// RunnerReceipt preserves the runner inputs needed to validate a golden offline.
type RunnerReceipt struct {
	Version       string `json:"version"`
	TarballDigest string `json:"tarball_digest"`
}

// Validate rejects incomplete or malformed runner identities.
func (r RunnerReceipt) Validate() error {
	if strings.TrimSpace(r.Version) == "" || strings.TrimSpace(r.Version) != r.Version {
		return fmt.Errorf("golden: runner receipt version is empty or padded")
	}
	digest, err := hex.DecodeString(r.TarballDigest)
	if err != nil || len(digest) != sha256.Size {
		return fmt.Errorf("golden: runner receipt digest must be a hex SHA-256")
	}
	return nil
}
