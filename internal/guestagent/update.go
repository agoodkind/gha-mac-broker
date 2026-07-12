package guestagent

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"debug/macho"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"connectrpc.com/connect"
	"goodkind.io/gha-mac-broker/internal/guestproto"
)

const (
	// bakedUpdatePublicKeyHex is the ed25519 public key the guest trusts for
	// agent updates. It is fail-closed: the matching private key was discarded
	// when this placeholder was minted, so no real update verifies until the
	// host-side deploy path bakes in the production keypair. The host holds the
	// private half and signs (version || big-endian size || sha256).
	bakedUpdatePublicKeyHex = "0991436defdf2f3d8c00c76b4f00959b7f9000017e95b877455446290439de35"

	// updateTempPrefix names the streamed temp file inside the install dir. The
	// rejection tests assert no file with this fragment survives a failed update.
	updateTempPrefix = "guest-agent-update-"

	// maxUpdateSize bounds both the declared and the received binary size so a
	// token holder cannot exhaust the guest disk with an unbounded stream.
	maxUpdateSize = 512 << 20

	// updateBinaryMode makes the placed binary executable so the reload can
	// execve it; os.CreateTemp yields 0600, which would fail execve with EACCES.
	updateBinaryMode = 0o755

	// maxEmptyUpdateFrames bounds how many zero-length data frames a stream may
	// send. Empty frames carry no bytes, so the size counter never bounds them; a
	// token holder could otherwise spin the receive loop until the deadline.
	maxEmptyUpdateFrames = 64

	// machoCodeSignatureCommand is LC_CODE_SIGNATURE, the ad-hoc seal the Go
	// linker emits on a darwin/arm64 image. debug/macho does not type this load
	// command, so the structural check scans the raw load-command list for it.
	machoCodeSignatureCommand = 0x1d

	sizeFieldBytes = 8
)

// versionAllowedRunes limits an update version to filename-safe characters, so
// a hostile version string cannot traverse out of the install dir when it is
// folded into the placed filename.
const versionAllowedRunes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"

// updateSigningPublicKey is a package var, not a const, only so a test can
// substitute a known keypair; production never reassigns it.
var updateSigningPublicKey = decodeBakedPublicKey(bakedUpdatePublicKeyHex)

// decodeBakedPublicKey decodes the compile-time key. It returns nil on a
// malformed constant rather than panicking; the handler then rejects every
// update at the key-size guard, so a bad key stays fail-closed.
func decodeBakedPublicKey(encoded string) ed25519.PublicKey {
	raw, err := hex.DecodeString(encoded)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return nil
	}
	return ed25519.PublicKey(raw)
}

// AgentReloader triggers an in-VM worker reload onto a freshly placed binary.
// The guest worker supplies a supervisor-backed implementation that reuses the
// durable snapshot handoff, so a reload never disturbs a running job.
type AgentReloader interface {
	Reload(ctx context.Context, newBinaryPath string) error
}

// UpdateAgent streams a replacement guest binary, verifies it end to end,
// places it atomically at a new versioned path, and triggers the worker reload
// onto it. The verification runs inside the boot-token auth boundary and adds
// binary-level checks so a mere token holder cannot push arbitrary code: the
// declared size is enforced while streaming, the sha256 must match, a detached
// ed25519 signature over (version || big-endian size || sha256) must verify
// against the baked public key, and the image must be an arm64 Mach-O carrying
// an LC_CODE_SIGNATURE. Any failure removes the temp file and never renames.
func (handler *Handler) UpdateAgent(
	ctx context.Context,
	stream *connect.ClientStream[guestproto.UpdateAgentRequest],
) (*connect.Response[guestproto.UpdateAgentResponse], error) {
	if handler.reloader == nil {
		return nil, connect.NewError(
			connect.CodeUnimplemented,
			errors.New("guestagent: agent update is not configured"),
		)
	}
	if handler.installDir == "" || handler.currentBinary == "" {
		return nil, connect.NewError(
			connect.CodeInternal,
			errors.New("guestagent: agent install path is unknown"),
		)
	}
	if len(handler.updatePublicKey) != ed25519.PublicKeySize {
		return nil, connect.NewError(
			connect.CodeInternal,
			errors.New("guestagent: update public key is not configured"),
		)
	}

	header, err := receiveUpdateHeader(stream)
	if err != nil {
		return nil, err
	}
	if err := validateUpdateHeader(header); err != nil {
		return nil, err
	}

	tempPath, sum, err := handler.streamToTemp(ctx, stream, header.GetSize())
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tempPath)
		}
	}()

	if subtle.ConstantTimeCompare(sum, header.GetSha256()) != 1 {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("guestagent: update sha256 does not match the received bytes"),
		)
	}
	if !ed25519.Verify(handler.updatePublicKey, signingMessage(header, sum), header.GetEd25519Signature()) {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("guestagent: update ed25519 signature is not valid"),
		)
	}
	if err := verifyArm64CodeSigned(tempPath); err != nil {
		return nil, err
	}

	removeProvenanceXattrs(tempPath)

	target := filepath.Join(handler.installDir, filepath.Base(handler.currentBinary)+"-"+header.GetVersion())
	if target == handler.currentBinary {
		return nil, connect.NewError(
			connect.CodeAlreadyExists,
			errors.New("guestagent: update version matches the running binary path"),
		)
	}
	// A symlink at the target is rejected so an update can never be redirected out
	// of the install dir onto an attacker-chosen path. A regular file is a prior
	// placement of this same version, whose reload may have failed; os.Rename
	// atomically replaces it so a re-verified retry succeeds without manual cleanup.
	if info, statErr := os.Lstat(target); statErr == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, connect.NewError(
			connect.CodeAlreadyExists,
			fmt.Errorf("guestagent: update target %q is a symlink", target),
		)
	}
	// Rename to the versioned filename on the same filesystem, never onto the
	// running inode. macOS AMFI kills a process whose live image is mutated, and
	// writing the running path risks ETXTBSY; a new directory entry pointing at a
	// new inode leaves the live process untouched, so the swap is atomic and safe.
	if err := os.Rename(tempPath, target); err != nil {
		return nil, connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("guestagent: place update binary: %w", err),
		)
	}
	committed = true

	slog.InfoContext(ctx, "guestagent accepted agent update", "version", header.GetVersion(), "path", target)
	if err := handler.reloader.Reload(ctx, target); err != nil {
		return nil, connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("guestagent: trigger worker reload: %w", err),
		)
	}
	return connect.NewResponse(&guestproto.UpdateAgentResponse{
		AcceptedVersion: header.GetVersion(),
	}), nil
}

// receiveUpdateHeader reads the first stream message, which must be the header.
func receiveUpdateHeader(
	stream *connect.ClientStream[guestproto.UpdateAgentRequest],
) (*guestproto.UpdateAgentHeader, error) {
	if !stream.Receive() {
		if err := stream.Err(); err != nil {
			return nil, err
		}
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("guestagent: update stream ended before the header"),
		)
	}
	header := stream.Msg().GetHeader()
	if header == nil {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("guestagent: update stream must send the header first"),
		)
	}
	return header, nil
}

func validateUpdateHeader(header *guestproto.UpdateAgentHeader) error {
	if err := validateUpdateVersion(header.GetVersion()); err != nil {
		return err
	}
	if header.GetSize() == 0 {
		return connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("guestagent: update size must be positive"),
		)
	}
	if header.GetSize() > maxUpdateSize {
		return connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf("guestagent: update size %d exceeds the %d byte limit", header.GetSize(), maxUpdateSize),
		)
	}
	if len(header.GetSha256()) != sha256.Size {
		return connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf("guestagent: update sha256 must be %d bytes", sha256.Size),
		)
	}
	if len(header.GetEd25519Signature()) != ed25519.SignatureSize {
		return connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf("guestagent: update ed25519 signature must be %d bytes", ed25519.SignatureSize),
		)
	}
	return nil
}

func validateUpdateVersion(version string) error {
	if version == "" {
		return connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("guestagent: update version must not be empty"),
		)
	}
	for _, character := range version {
		if !strings.ContainsRune(versionAllowedRunes, character) {
			return connect.NewError(
				connect.CodeInvalidArgument,
				fmt.Errorf("guestagent: update version %q has a disallowed character", version),
			)
		}
	}
	return nil
}

// streamToTemp writes the data chunks into a temp file in the install dir while
// hashing them and enforcing the declared size. It returns the temp path and
// the sha256 of the received bytes. On any error it removes the temp file, so
// the caller only owns cleanup once this returns successfully.
func (handler *Handler) streamToTemp(
	ctx context.Context,
	stream *connect.ClientStream[guestproto.UpdateAgentRequest],
	declaredSize uint64,
) (string, []byte, error) {
	tempFile, err := os.CreateTemp(handler.installDir, updateTempPrefix+"*")
	if err != nil {
		return "", nil, connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("guestagent: create update temp file: %w", err),
		)
	}
	tempPath := tempFile.Name()

	hasher := sha256.New()
	received := uint64(0)
	emptyFrames := 0
	streamErr := error(nil)
	for stream.Receive() {
		message := stream.Msg()
		if message.GetHeader() != nil {
			streamErr = connect.NewError(
				connect.CodeInvalidArgument,
				errors.New("guestagent: update stream sent a second header"),
			)
			break
		}
		data := message.GetData()
		if len(data) == 0 {
			emptyFrames++
			if emptyFrames > maxEmptyUpdateFrames {
				streamErr = connect.NewError(
					connect.CodeInvalidArgument,
					fmt.Errorf("guestagent: update stream sent more than %d empty frames", maxEmptyUpdateFrames),
				)
				break
			}
			continue
		}
		received += uint64(len(data))
		if received > declaredSize {
			streamErr = connect.NewError(
				connect.CodeInvalidArgument,
				fmt.Errorf("guestagent: update stream exceeds the declared size %d", declaredSize),
			)
			break
		}
		if _, err := tempFile.Write(data); err != nil {
			streamErr = connect.NewError(
				connect.CodeInternal,
				fmt.Errorf("guestagent: write update temp file: %w", err),
			)
			break
		}
		hasher.Write(data)
	}
	if streamErr == nil {
		streamErr = stream.Err()
	}
	if streamErr == nil && received != declaredSize {
		streamErr = connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf("guestagent: update received %d bytes, declared size %d", received, declaredSize),
		)
	}
	if chmodErr := tempFile.Chmod(updateBinaryMode); chmodErr != nil && streamErr == nil {
		streamErr = connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("guestagent: mark update binary executable: %w", chmodErr),
		)
	}
	if syncErr := tempFile.Sync(); syncErr != nil && streamErr == nil {
		streamErr = connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("guestagent: flush update temp file: %w", syncErr),
		)
	}
	if closeErr := tempFile.Close(); closeErr != nil && streamErr == nil {
		streamErr = connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("guestagent: close update temp file: %w", closeErr),
		)
	}
	if streamErr != nil {
		_ = os.Remove(tempPath)
		slog.WarnContext(ctx, "guestagent update stream to temp failed", "err", streamErr)
		return "", nil, streamErr
	}
	return tempPath, hasher.Sum(nil), nil
}

// signingMessage rebuilds the exact bytes the host signed: the version string,
// then the big-endian uint64 size, then the sha256 of the binary.
func signingMessage(header *guestproto.UpdateAgentHeader, sum []byte) []byte {
	version := header.GetVersion()
	message := make([]byte, 0, len(version)+sizeFieldBytes+len(sum))
	message = append(message, version...)
	var sizeField [sizeFieldBytes]byte
	binary.BigEndian.PutUint64(sizeField[:], header.GetSize())
	message = append(message, sizeField[:]...)
	message = append(message, sum...)
	return message
}

// verifyArm64CodeSigned parses the image with stdlib debug/macho and requires an
// arm64 CPU plus an LC_CODE_SIGNATURE load command. A fat binary fails to parse
// as a thin image and is rejected here too.
func verifyArm64CodeSigned(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("guestagent: open update image: %w", err),
		)
	}
	defer func() { _ = file.Close() }()

	image, err := macho.NewFile(file)
	if err != nil {
		return connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf("guestagent: update is not a thin Mach-O image: %w", err),
		)
	}
	if image.Cpu != macho.CpuArm64 {
		return connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf("guestagent: update Mach-O CPU is %s, want arm64", image.Cpu),
		)
	}
	for _, load := range image.Loads {
		raw := load.Raw()
		if len(raw) < 4 {
			continue
		}
		if image.ByteOrder.Uint32(raw[:4]) == machoCodeSignatureCommand {
			return nil
		}
	}
	return connect.NewError(
		connect.CodeInvalidArgument,
		errors.New("guestagent: update Mach-O is not code signed (no LC_CODE_SIGNATURE)"),
	)
}
