package guestagent_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"goodkind.io/gha-mac-broker/internal/guestagent"
	"goodkind.io/gha-mac-broker/internal/guestproto"
	"goodkind.io/gha-mac-broker/internal/guestproto/guestprotoconnect"
	"goodkind.io/gha-mac-broker/internal/guesttransport"
)

const updateTestToken = "phase0-update-token"

// machoMagic64 and the surrounding constants describe the minimal Mach-O images
// the update fixtures build. The structural check only inspects the header CPU
// and the load-command list, so a hand-built image exercises the same path a
// real Go-linked arm64 binary hits without shelling out to a compiler.
const (
	machoMagic64            = 0xfeedfacf
	machoCPUArm64           = 0x0100000c
	machoCPUx8664           = 0x01000007
	machoExecute            = 0x2
	machoLoadCodeSignature  = 0x1d
	machoCodeSignatureBytes = 16
)

// stubReloader records each reload it is asked to perform so a test can assert
// the handler triggered the PR3 reload path exactly once onto the placed binary.
type stubReloader struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (reloader *stubReloader) Reload(_ context.Context, newBinaryPath string) error {
	reloader.mu.Lock()
	defer reloader.mu.Unlock()
	reloader.calls = append(reloader.calls, newBinaryPath)
	return reloader.err
}

func (reloader *stubReloader) recorded() []string {
	reloader.mu.Lock()
	defer reloader.mu.Unlock()
	return append([]string(nil), reloader.calls...)
}

func TestUpdateAgentAcceptsSignedArm64Binary(t *testing.T) {
	publicKey, privateKey := updateKeypair(t)
	binaryBytes := machoImage(machoCPUArm64, true)
	reloader := &stubReloader{calls: nil, err: nil}
	installDir, address := startUpdateServer(t, guestagent.Options{
		Reloader:        reloader,
		InstallDir:      "",
		UpdatePublicKey: publicKey,
	})
	_ = installDir

	response, err := sendUpdate(t, address, signedHeader(privateKey, "1.2.3", binaryBytes), binaryBytes)
	if err != nil {
		t.Fatalf("UpdateAgent rejected a valid signed arm64 binary: %v", err)
	}
	if response.GetAcceptedVersion() != "1.2.3" {
		t.Fatalf("accepted version = %q, want 1.2.3", response.GetAcceptedVersion())
	}

	calls := reloader.recorded()
	if len(calls) != 1 {
		t.Fatalf("reloader called %d times, want 1", len(calls))
	}
	placed := calls[0]
	if filepath.Dir(placed) != installDir {
		t.Fatalf("placed path %q not in install dir %q", placed, installDir)
	}
	if !strings.Contains(filepath.Base(placed), "1.2.3") {
		t.Fatalf("placed filename %q does not carry the version", filepath.Base(placed))
	}
	written, err := os.ReadFile(placed)
	if err != nil {
		t.Fatalf("read placed binary: %v", err)
	}
	if !bytes.Equal(written, binaryBytes) {
		t.Fatal("placed binary content differs from the uploaded bytes")
	}
	info, err := os.Stat(placed)
	if err != nil {
		t.Fatalf("stat placed binary: %v", err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("placed binary mode = %o, want 0755 so the reload can execve it", info.Mode().Perm())
	}
	if leftover := tempLeftovers(t, installDir); len(leftover) != 0 {
		t.Fatalf("temp files left behind after success: %v", leftover)
	}
}

// TestUpdateAgentReplacesSameVersionRegularFile proves a re-verified retry of a
// version already placed as a regular file succeeds by atomically replacing it,
// so a failed reload does not permanently wedge that version.
func TestUpdateAgentReplacesSameVersionRegularFile(t *testing.T) {
	publicKey, privateKey := updateKeypair(t)
	binaryBytes := machoImage(machoCPUArm64, true)
	reloader := &stubReloader{calls: nil, err: nil}
	_, address := startUpdateServer(t, guestagent.Options{
		Reloader:        reloader,
		InstallDir:      "",
		UpdatePublicKey: publicKey,
	})

	header := signedHeader(privateKey, "4.5.6", binaryBytes)
	if _, err := sendUpdate(t, address, header, binaryBytes); err != nil {
		t.Fatalf("first placement rejected: %v", err)
	}
	if _, err := sendUpdate(t, address, signedHeader(privateKey, "4.5.6", binaryBytes), binaryBytes); err != nil {
		t.Fatalf("same-version retry rejected, want atomic replace: %v", err)
	}
	if calls := reloader.recorded(); len(calls) != 2 {
		t.Fatalf("reloader called %d times, want 2 (one per placement)", len(calls))
	}
}

// TestUpdateAgentRejectsSymlinkTarget proves a symlink sitting at the versioned
// path is refused, so an update can never be redirected out of the install dir.
func TestUpdateAgentRejectsSymlinkTarget(t *testing.T) {
	publicKey, privateKey := updateKeypair(t)
	binaryBytes := machoImage(machoCPUArm64, true)
	reloader := &stubReloader{calls: nil, err: nil}
	installDir, address := startUpdateServer(t, guestagent.Options{
		Reloader:        reloader,
		InstallDir:      "",
		UpdatePublicKey: publicKey,
	})

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve executable: %v", err)
	}
	target := filepath.Join(installDir, filepath.Base(executable)+"-7.7.7")
	outside := filepath.Join(t.TempDir(), "elsewhere")
	if err := os.Symlink(outside, target); err != nil {
		t.Fatalf("create symlink target: %v", err)
	}

	_, err = sendUpdate(t, address, signedHeader(privateKey, "7.7.7", binaryBytes), binaryBytes)
	if err == nil {
		t.Fatal("update onto a symlink target was accepted, want rejection")
	}
	if got := connect.CodeOf(err); got != connect.CodeAlreadyExists {
		t.Fatalf("error code = %v, want AlreadyExists (err: %v)", got, err)
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("error %q does not mention symlink", err.Error())
	}
	if calls := reloader.recorded(); len(calls) != 0 {
		t.Fatalf("reloader triggered on a symlink rejection: %v", calls)
	}
	info, err := os.Lstat(target)
	if err != nil {
		t.Fatalf("lstat target after rejection: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("symlink target was replaced, want it left intact")
	}
}

// TestUpdateAgentRejectsEmptyFrameFlood proves an authenticated stream of only
// empty data frames is refused promptly rather than spinning until the deadline.
func TestUpdateAgentRejectsEmptyFrameFlood(t *testing.T) {
	publicKey, privateKey := updateKeypair(t)
	binaryBytes := machoImage(machoCPUArm64, true)
	reloader := &stubReloader{calls: nil, err: nil}
	installDir, address := startUpdateServer(t, guestagent.Options{
		Reloader:        reloader,
		InstallDir:      "",
		UpdatePublicKey: publicKey,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transport := guesttransport.DialContext(ctx, tcpDialer(address), updateTestToken)
	service := guestprotoconnect.NewGuestAgentServiceClient(
		transport.HTTPClient(),
		"http://"+address,
		connect.WithInterceptors(transport.Interceptor()),
	)
	stream := service.UpdateAgent(ctx)
	if err := stream.Send(&guestproto.UpdateAgentRequest{
		Chunk: &guestproto.UpdateAgentRequest_Header{Header: signedHeader(privateKey, "8.8.8", binaryBytes)},
	}); err != nil {
		t.Fatalf("send header: %v", err)
	}
	const emptyFrameFlood = 4096
	sendErr := error(nil)
	for frame := 0; frame < emptyFrameFlood; frame++ {
		if sendErr = stream.Send(&guestproto.UpdateAgentRequest{
			Chunk: &guestproto.UpdateAgentRequest_Data{Data: nil},
		}); sendErr != nil {
			break
		}
	}
	_, closeErr := stream.CloseAndReceive()
	if closeErr == nil {
		t.Fatal("empty-frame flood was accepted, want rejection")
	}
	if got := connect.CodeOf(closeErr); got != connect.CodeInvalidArgument {
		t.Fatalf("error code = %v, want InvalidArgument (err: %v)", got, closeErr)
	}
	if calls := reloader.recorded(); len(calls) != 0 {
		t.Fatalf("reloader triggered on an empty-frame flood: %v", calls)
	}
	if entries, err := os.ReadDir(installDir); err != nil {
		t.Fatalf("read install dir: %v", err)
	} else if len(entries) != 0 {
		t.Fatalf("install dir not clean after empty-frame rejection: %d entries", len(entries))
	}
}

func TestUpdateAgentRejectsBadInputs(t *testing.T) {
	arm64Signed := machoImage(machoCPUArm64, true)
	arm64Unsigned := machoImage(machoCPUArm64, false)
	x8664Signed := machoImage(machoCPUx8664, true)

	cases := []struct {
		name      string
		mutate    func(privateKey ed25519.PrivateKey) (*guestproto.UpdateAgentHeader, []byte)
		wantCode  connect.Code
		wantInErr string
	}{
		{
			name: "wrong signature",
			mutate: func(privateKey ed25519.PrivateKey) (*guestproto.UpdateAgentHeader, []byte) {
				header := signedHeader(privateKey, "9.9.9", arm64Signed)
				header.Ed25519Signature[0] ^= 0xff
				return header, arm64Signed
			},
			wantCode:  connect.CodeInvalidArgument,
			wantInErr: "signature",
		},
		{
			name: "sha256 mismatch",
			mutate: func(privateKey ed25519.PrivateKey) (*guestproto.UpdateAgentHeader, []byte) {
				header := signedHeader(privateKey, "9.9.9", arm64Signed)
				header.Sha256[0] ^= 0xff
				return header, arm64Signed
			},
			wantCode:  connect.CodeInvalidArgument,
			wantInErr: "sha256",
		},
		{
			name: "size overflow",
			mutate: func(privateKey ed25519.PrivateKey) (*guestproto.UpdateAgentHeader, []byte) {
				header := signedHeader(privateKey, "9.9.9", arm64Signed)
				header.Size = uint64(len(arm64Signed)) - 1
				return header, arm64Signed
			},
			wantCode:  connect.CodeInvalidArgument,
			wantInErr: "size",
		},
		{
			name: "non arm64",
			mutate: func(privateKey ed25519.PrivateKey) (*guestproto.UpdateAgentHeader, []byte) {
				return signedHeader(privateKey, "9.9.9", x8664Signed), x8664Signed
			},
			wantCode:  connect.CodeInvalidArgument,
			wantInErr: "arm64",
		},
		{
			name: "unsigned macho",
			mutate: func(privateKey ed25519.PrivateKey) (*guestproto.UpdateAgentHeader, []byte) {
				return signedHeader(privateKey, "9.9.9", arm64Unsigned), arm64Unsigned
			},
			wantCode:  connect.CodeInvalidArgument,
			wantInErr: "LC_CODE_SIGNATURE",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			publicKey, privateKey := updateKeypair(t)
			reloader := &stubReloader{calls: nil, err: nil}
			installDir, address := startUpdateServer(t, guestagent.Options{
				Reloader:        reloader,
				InstallDir:      "",
				UpdatePublicKey: publicKey,
			})

			header, data := testCase.mutate(privateKey)
			_, err := sendUpdate(t, address, header, data)
			if err == nil {
				t.Fatalf("%s was accepted, want rejection", testCase.name)
			}
			if got := connect.CodeOf(err); got != testCase.wantCode {
				t.Fatalf("error code = %v, want %v (err: %v)", got, testCase.wantCode, err)
			}
			if !strings.Contains(err.Error(), testCase.wantInErr) {
				t.Fatalf("error %q does not mention %q", err.Error(), testCase.wantInErr)
			}
			if calls := reloader.recorded(); len(calls) != 0 {
				t.Fatalf("reloader was triggered on a rejected update: %v", calls)
			}
			entries, err := os.ReadDir(installDir)
			if err != nil {
				t.Fatalf("read install dir: %v", err)
			}
			if len(entries) != 0 {
				names := make([]string, 0, len(entries))
				for _, entry := range entries {
					names = append(names, entry.Name())
				}
				t.Fatalf("install dir not clean after rejection: %v", names)
			}
		})
	}
}

// updateKeypair returns a fresh ed25519 keypair; the public half is injected
// into the handler so the test signs with a key the guest trusts.
func updateKeypair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	return publicKey, privateKey
}

func signedHeader(privateKey ed25519.PrivateKey, version string, binaryBytes []byte) *guestproto.UpdateAgentHeader {
	sum := sha256.Sum256(binaryBytes)
	message := make([]byte, 0, len(version)+8+len(sum))
	message = append(message, version...)
	var sizeField [8]byte
	binary.BigEndian.PutUint64(sizeField[:], uint64(len(binaryBytes)))
	message = append(message, sizeField[:]...)
	message = append(message, sum[:]...)
	signature := ed25519.Sign(privateKey, message)
	return &guestproto.UpdateAgentHeader{
		Version:          version,
		Size:             uint64(len(binaryBytes)),
		Sha256:           sum[:],
		Ed25519Signature: signature,
	}
}

// machoImage builds a minimal thin Mach-O image with the given CPU type,
// optionally carrying a single LC_CODE_SIGNATURE load command.
func machoImage(cpuType uint32, codeSigned bool) []byte {
	var loads bytes.Buffer
	commandCount := uint32(0)
	if codeSigned {
		writeUint32(&loads, machoLoadCodeSignature)
		writeUint32(&loads, machoCodeSignatureBytes)
		writeUint32(&loads, 0)
		writeUint32(&loads, 0)
		commandCount++
	}
	var image bytes.Buffer
	writeUint32(&image, machoMagic64)
	writeUint32(&image, cpuType)
	writeUint32(&image, 0)
	writeUint32(&image, machoExecute)
	writeUint32(&image, commandCount)
	writeUint32(&image, uint32(loads.Len()))
	writeUint32(&image, 0)
	writeUint32(&image, 0)
	image.Write(loads.Bytes())
	return image.Bytes()
}

func writeUint32(buffer *bytes.Buffer, value uint32) {
	var field [4]byte
	binary.LittleEndian.PutUint32(field[:], value)
	buffer.Write(field[:])
}

func tempLeftovers(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	leftovers := make([]string, 0)
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "update-") {
			leftovers = append(leftovers, entry.Name())
		}
	}
	return leftovers
}

// startUpdateServer serves a guest-agent handler with the given options over an
// authenticated h2c listener and returns the resolved install directory and the
// listen address. When options.InstallDir is empty it is set to a fresh temp
// directory so placement stays inside the test sandbox.
func startUpdateServer(t *testing.T, options guestagent.Options) (string, string) {
	t.Helper()
	if options.InstallDir == "" {
		options.InstallDir = t.TempDir()
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serverContext, stopServer := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		handler := guestagent.NewHTTPHandler(nil, options)
		done <- guesttransport.Serve(serverContext, listener, handler, updateTestToken)
	}()
	t.Cleanup(func() {
		stopServer()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("update server did not stop")
		}
	})
	return options.InstallDir, listener.Addr().String()
}

func sendUpdate(
	t *testing.T,
	address string,
	header *guestproto.UpdateAgentHeader,
	data []byte,
) (*guestproto.UpdateAgentResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transport := guesttransport.DialContext(ctx, tcpDialer(address), updateTestToken)
	service := guestprotoconnect.NewGuestAgentServiceClient(
		transport.HTTPClient(),
		"http://"+address,
		connect.WithInterceptors(transport.Interceptor()),
	)
	stream := service.UpdateAgent(ctx)
	if err := stream.Send(&guestproto.UpdateAgentRequest{
		Chunk: &guestproto.UpdateAgentRequest_Header{Header: header},
	}); err != nil {
		return nil, err
	}
	const chunkSize = 7
	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&guestproto.UpdateAgentRequest{
			Chunk: &guestproto.UpdateAgentRequest_Data{Data: data[offset:end]},
		}); err != nil {
			return nil, err
		}
	}
	response, err := stream.CloseAndReceive()
	if err != nil {
		return nil, err
	}
	return response.Msg, nil
}
