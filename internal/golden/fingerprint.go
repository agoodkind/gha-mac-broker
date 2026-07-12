package golden

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"sort"
)

// PayloadDigest is one baked payload identified by name with its content digest.
type PayloadDigest struct {
	// Name labels the payload (e.g. the plist filename) and is folded into the
	// fingerprint, so renaming a payload changes the fingerprint.
	Name string
	// Digest is the hex content digest of the payload bytes.
	Digest string
}

// FingerprintInputs are the identity-bearing inputs of a golden image. Every
// field feeds the fingerprint, so a change to any one produces a new fingerprint.
type FingerprintInputs struct {
	// BaseImageRef is the Cirrus base image the golden derives from.
	BaseImageRef string
	// RunnerVersion is the resolved actions/runner version baked in.
	RunnerVersion string
	// RunnerTarballDigest is the content digest of the runner release tarball.
	RunnerTarballDigest string
	// BinaryDigest is the content digest of the baked guest broker binary.
	BinaryDigest string
	// Payloads are the other baked payloads (such as the supervisor plist).
	Payloads []PayloadDigest
}

// Fingerprint computes a deterministic sha256 over the golden image identity
// inputs. It is a pure function of its inputs: the same inputs always yield the
// same fingerprint, and changing any single input changes the result. Each field
// is written with a label and a null terminator so distinct inputs cannot be
// confused by concatenation, and payloads are sorted by name so input ordering
// does not affect the result.
func Fingerprint(inputs FingerprintInputs) string {
	digest := sha256.New()
	writeFingerprintField(digest, "base-image", inputs.BaseImageRef)
	writeFingerprintField(digest, "runner-version", inputs.RunnerVersion)
	writeFingerprintField(digest, "runner-tarball", inputs.RunnerTarballDigest)
	writeFingerprintField(digest, "binary", inputs.BinaryDigest)
	payloads := make([]PayloadDigest, len(inputs.Payloads))
	copy(payloads, inputs.Payloads)
	sort.Slice(payloads, func(i, j int) bool {
		return payloads[i].Name < payloads[j].Name
	})
	for _, payload := range payloads {
		writeFingerprintField(digest, "payload-name", payload.Name)
		writeFingerprintField(digest, "payload-digest", payload.Digest)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// writeFingerprintField folds one labelled field into the running hash with a
// null terminator, so no combination of field values can collide with another.
func writeFingerprintField(digest hash.Hash, label, value string) {
	_, _ = io.WriteString(digest, label)
	_, _ = io.WriteString(digest, "=")
	_, _ = io.WriteString(digest, value)
	_, _ = digest.Write([]byte{0})
}
