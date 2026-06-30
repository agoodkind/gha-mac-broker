package fastpull

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type copyCall struct {
	srcImageRef string
	layoutDir   string
	tag         string
	os          string
	arch        string
}

type layoutCopier struct {
	configBytes    []byte
	layerOneBytes  []byte
	layerTwoBytes  []byte
	manifestBytes  []byte
	manifestDigest string
	calls          []copyCall
}

func (c *layoutCopier) CopyToOCILayout(ctx context.Context, srcImageRef, layoutDir, tag, osName, arch string) error {
	c.calls = append(c.calls, copyCall{
		srcImageRef: srcImageRef,
		layoutDir:   layoutDir,
		tag:         tag,
		os:          osName,
		arch:        arch,
	})
	return writeTinyOCILayout(ctx, layoutDir, tag, c)
}

func digestOf(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func blobPath(layoutDir string, digest string) string {
	algorithm, hexValue, found := strings.Cut(digest, ":")
	if !found {
		return filepath.Join(layoutDir, "blobs", "unknown", digest)
	}
	return filepath.Join(layoutDir, "blobs", algorithm, hexValue)
}

func writeBlob(layoutDir string, digest string, body []byte) error {
	path := blobPath(layoutDir, digest)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return err
	}
	return nil
}

func writeTinyOCILayout(_ context.Context, layoutDir string, _ string, copier *layoutCopier) error {
	configDigest := digestOf(copier.configBytes)
	layerOneDigest := digestOf(copier.layerOneBytes)
	layerTwoDigest := digestOf(copier.layerTwoBytes)
	copier.manifestBytes = []byte(fmt.Sprintf(
		`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",`+
			`"config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"%s","size":%d},`+
			`"layers":[`+
			`{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"%s","size":%d},`+
			`{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"%s","size":%d}`+
			`]}`,
		configDigest,
		len(copier.configBytes),
		layerOneDigest,
		len(copier.layerOneBytes),
		layerTwoDigest,
		len(copier.layerTwoBytes),
	))
	copier.manifestDigest = digestOf(copier.manifestBytes)
	indexBytes := []byte(fmt.Sprintf(
		`{"schemaVersion":2,"manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"%s","size":%d}]}`,
		copier.manifestDigest,
		len(copier.manifestBytes),
	))
	if err := os.MkdirAll(layoutDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(layoutDir, "oci-layout"), []byte(`{"imageLayoutVersion":"1.0.0"}`), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(layoutDir, "index.json"), indexBytes, 0o644); err != nil {
		return err
	}
	if err := writeBlob(layoutDir, configDigest, copier.configBytes); err != nil {
		return err
	}
	if err := writeBlob(layoutDir, layerOneDigest, copier.layerOneBytes); err != nil {
		return err
	}
	if err := writeBlob(layoutDir, layerTwoDigest, copier.layerTwoBytes); err != nil {
		return err
	}
	if err := writeBlob(layoutDir, copier.manifestDigest, copier.manifestBytes); err != nil {
		return err
	}
	return nil
}

func getBody(ctx context.Context, t *testing.T, url string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build request %s: %v", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return resp.StatusCode, body
}

func requireLoopbackListen(t *testing.T) {
	t.Helper()
	listener, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("loopback listen unavailable in this environment: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close loopback probe listener: %v", err)
	}
}

func TestStageServesOCILayoutManifestAndBlobsOnLoopback(t *testing.T) {
	requireLoopbackListen(t)

	copier := &layoutCopier{
		configBytes:    []byte(`{"architecture":"arm64","os":"darwin","rootfs":{"type":"layers","diff_ids":[]}}`),
		layerOneBytes:  []byte("fake-layer-one-payload"),
		layerTwoBytes:  []byte("fake-layer-two-payload"),
		manifestBytes:  nil,
		manifestDigest: "",
		calls:          nil,
	}
	layoutDir := t.TempDir()
	stager := New(Options{Copier: copier, Dir: layoutDir})
	ctx := context.Background()

	ref, stop, err := stager.Stage(ctx, "ghcr.io/cirruslabs/macos-tahoe-xcode:26.5")
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	defer stop()

	if !strings.HasPrefix(ref, "[::1]:") || !strings.HasSuffix(ref, "/cirruslabs/macos-tahoe-xcode:26.5") {
		t.Fatalf("unexpected ref %q", ref)
	}
	if len(copier.calls) != 1 {
		t.Fatalf("copy calls = %d, want 1", len(copier.calls))
	}
	call := copier.calls[0]
	if call.srcImageRef != "ghcr.io/cirruslabs/macos-tahoe-xcode:26.5" {
		t.Fatalf("src image = %q", call.srcImageRef)
	}
	if call.layoutDir != layoutDir {
		t.Fatalf("layout dir = %q, want %q", call.layoutDir, layoutDir)
	}
	if call.tag != "26.5" || call.os != "darwin" || call.arch != "arm64" {
		t.Fatalf("copy call = %+v", call)
	}

	hostPort := strings.SplitN(ref, "/", 2)[0]
	base := "http://" + hostPort + "/v2/cirruslabs/macos-tahoe-xcode"

	status, gotManifest := getBody(ctx, t, base+"/manifests/26.5")
	if status != http.StatusOK {
		t.Fatalf("manifest status %d", status)
	}
	if !bytes.Equal(gotManifest, copier.manifestBytes) {
		t.Fatalf("served manifest mismatch:\n got %s\nwant %s", gotManifest, copier.manifestBytes)
	}

	layerDigest := digestOf(copier.layerOneBytes)
	status, gotLayer := getBody(ctx, t, base+"/blobs/"+layerDigest)
	if status != http.StatusOK {
		t.Fatalf("layer status %d", status)
	}
	if !bytes.Equal(gotLayer, copier.layerOneBytes) {
		t.Fatalf("served layer blob mismatch")
	}

	status, gotDigestManifest := getBody(ctx, t, base+"/manifests/"+copier.manifestDigest)
	if status != http.StatusOK {
		t.Fatalf("digest manifest status %d", status)
	}
	if !bytes.Equal(gotDigestManifest, copier.manifestBytes) {
		t.Fatalf("served digest manifest mismatch")
	}
}

func TestStageRefusesNonCirrusImage(t *testing.T) {
	stager := New(Options{Copier: nil, Dir: t.TempDir()})
	_, _, err := stager.Stage(context.Background(), "ghcr.io/example/not-cirrus:1.0")
	if err == nil {
		t.Fatal("expected refusal of non-cirrus image")
	}
}

func TestIsCirrusXcodeRepo(t *testing.T) {
	cases := map[string]bool{
		"cirruslabs/macos-tahoe-xcode":  true,
		"cirruslabs/macos-sonoma-xcode": true,
		"cirruslabs/macos-tahoe-base":   false,
		"example/macos-tahoe-xcode":     false,
		"cirruslabs/ubuntu":             false,
	}
	for repo, want := range cases {
		if got := isCirrusXcodeRepo(repo); got != want {
			t.Errorf("isCirrusXcodeRepo(%q)=%v want %v", repo, got, want)
		}
	}
}
