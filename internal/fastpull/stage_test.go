package fastpull

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"goodkind.io/gha-mac-broker/internal/aria2"
)

func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
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

func TestStageServesIndexAndBlobsOnLoopback(t *testing.T) {
	configBytes := []byte(`{"architecture":"arm64","os":"darwin","rootfs":{"type":"layers","diff_ids":[]}}`)
	layerBytes := []byte("fake-layer-payload-0123456789")
	configDigest := digestOf(configBytes)
	layerDigest := digestOf(layerBytes)

	manifestBytes := []byte(fmt.Sprintf(
		`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",`+
			`"config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"%s","size":%d},`+
			`"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"%s","size":%d}]}`,
		configDigest, len(configBytes), layerDigest, len(layerBytes)))
	manifestDigest := digestOf(manifestBytes)

	indexBytes := []byte(fmt.Sprintf(
		`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json",`+
			`"manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"%s","size":%d,`+
			`"platform":{"architecture":"arm64","os":"darwin"}}]}`,
		manifestDigest, len(manifestBytes)))
	indexDigest := digestOf(indexBytes)

	blobs := map[string][]byte{configDigest: configBytes, layerDigest: layerBytes}
	manifests := map[string][]byte{manifestDigest: manifestBytes, indexDigest: indexBytes, "26.5": indexBytes}
	mediaTypes := map[string]string{
		manifestDigest: "application/vnd.oci.image.manifest.v1+json",
		indexDigest:    "application/vnd.oci.image.index.v1+json",
		"26.5":         "application/vnd.oci.image.index.v1+json",
	}

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/token":
			_, _ = w.Write([]byte(`{"token":"testtoken"}`))
		case strings.Contains(r.URL.Path, "/manifests/"):
			ref := path.Base(r.URL.Path)
			body, ok := manifests[ref]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", mediaTypes[ref])
			w.Header().Set("Docker-Content-Digest", digestOf(body))
			_, _ = w.Write(body)
		case strings.Contains(r.URL.Path, "/blobs/"):
			ref := path.Base(r.URL.Path)
			body, ok := blobs[ref]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write(body)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer source.Close()
	host := strings.TrimPrefix(source.URL, "http://")

	var downloadCalls int
	download := func(_ context.Context, items []aria2.Item, authHeader string, _ aria2.Options) error {
		downloadCalls++
		if authHeader != "Authorization: Bearer testtoken" {
			return fmt.Errorf("unexpected auth header %q", authHeader)
		}
		for _, item := range items {
			digest := "sha256:" + filepath.Base(item.OutPath)
			body, ok := blobs[digest]
			if !ok {
				return fmt.Errorf("stub has no blob for %s", digest)
			}
			if err := os.MkdirAll(filepath.Dir(item.OutPath), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(item.OutPath, body, 0o644); err != nil {
				return err
			}
		}
		return nil
	}

	dir := t.TempDir()
	stager := New(Options{Download: download, Dir: dir, Split: 4, MaxConnPerServer: 4, MaxConcurrent: 2})
	ctx := context.Background()
	ref, stop, err := stager.Stage(ctx, host+"/cirruslabs/macos-tahoe-xcode:26.5")
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	defer stop()

	if !strings.HasPrefix(ref, "[::1]:") || !strings.HasSuffix(ref, "/cirruslabs/macos-tahoe-xcode:26.5") {
		t.Fatalf("unexpected ref %q", ref)
	}
	if downloadCalls != 1 {
		t.Fatalf("download called %d times, want 1", downloadCalls)
	}

	hostPort := strings.SplitN(ref, "/", 2)[0]
	base := "http://" + hostPort + "/v2/cirruslabs/macos-tahoe-xcode"

	status, gotIndex := getBody(ctx, t, base+"/manifests/26.5")
	if status != http.StatusOK {
		t.Fatalf("manifest status %d", status)
	}
	if !bytes.Equal(gotIndex, indexBytes) {
		t.Fatalf("served index mismatch:\n got %s\nwant %s", gotIndex, indexBytes)
	}

	status, gotConfig := getBody(ctx, t, base+"/blobs/"+configDigest)
	if status != http.StatusOK {
		t.Fatalf("config blob status %d", status)
	}
	if !bytes.Equal(gotConfig, configBytes) {
		t.Fatalf("served config blob mismatch")
	}

	status, gotLayer := getBody(ctx, t, base+"/blobs/"+layerDigest)
	if status != http.StatusOK || !bytes.Equal(gotLayer, layerBytes) {
		t.Fatalf("served layer blob mismatch status=%d", status)
	}
}

func TestStageRefusesNonCirrusImage(t *testing.T) {
	stager := New(Options{Download: nil, Dir: t.TempDir(), Split: 1, MaxConnPerServer: 1, MaxConcurrent: 1})
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
