// Package fastpull stages a Cirrus base image into a loopback OCI registry on
// [::1] so tart can pull it fast. Blobs download in parallel through the injected
// downloader (libaria2); the registry is served in-process by
// go-containerregistry, so tart pulls content-addressed blobs over loopback and
// does its own decompression. This sidesteps tart's single-stream pull, which
// ghcr throttles per connection.
package fastpull

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"goodkind.io/gha-mac-broker/internal/aria2"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

const (
	loopbackHost      = "[::1]"
	readHeaderTimeout = 30 * time.Second
	clientTimeout     = 30 * time.Second
)

// DownloadFunc downloads items in parallel; [aria2.Download] satisfies it.
type DownloadFunc func(ctx context.Context, items []aria2.Item, authHeader string, opts aria2.Options) error

// Options configures a [Stager].
type Options struct {
	// Download is the parallel downloader; [aria2.Download] in production.
	Download DownloadFunc
	// Dir is the blob directory; the ggcr disk handler reads <Dir>/sha256/<hex>.
	Dir string
	// Split is the number of connections per blob.
	Split int
	// MaxConnPerServer caps connections per server per blob.
	MaxConnPerServer int
	// MaxConcurrent caps blobs downloaded at once.
	MaxConcurrent int
}

// Stager stages a Cirrus base image into a loopback registry. See the package
// doc for the mechanism.
type Stager struct {
	download         DownloadFunc
	dir              string
	split            int
	maxConnPerServer int
	maxConcurrent    int
}

// New returns a Stager configured by opts.
func New(opts Options) *Stager {
	return &Stager{
		download:         opts.Download,
		dir:              opts.Dir,
		split:            opts.Split,
		maxConnPerServer: opts.MaxConnPerServer,
		maxConcurrent:    opts.MaxConcurrent,
	}
}

// Stage downloads image's blobs in parallel, serves them from a loopback OCI
// registry on [::1], and returns a clonable insecure ref plus a stop func that
// shuts the registry down. The caller clones the returned ref with tart
// --insecure, then calls stop.
func (s *Stager) Stage(ctx context.Context, image string) (string, func(), error) {
	ref, err := name.ParseReference(image, name.Insecure)
	if err != nil {
		slog.ErrorContext(ctx, "fastpull parse ref failed", "err", err, "image", image)
		return "", nil, fmt.Errorf("fastpull: parse %s: %w", image, err)
	}
	repo := ref.Context().RepositoryStr()
	if !isCirrusXcodeRepo(repo) {
		err := fmt.Errorf("fastpull: refusing non-cirrus image %q", image)
		slog.ErrorContext(ctx, "fastpull refusing non-cirrus image", "err", err, "image", image)
		return "", nil, err
	}
	tag := ref.Identifier()
	host := ref.Context().RegistryStr()
	scheme := ref.Context().Scheme()

	desc, err := remote.Get(ref, remote.WithContext(ctx))
	if err != nil {
		slog.ErrorContext(ctx, "fastpull manifest fetch failed", "err", err, "image", image)
		return "", nil, fmt.Errorf("fastpull: get %s: %w", image, err)
	}

	img, idxRaw, idxMediaType, archDigest, err := resolveImage(ctx, desc)
	if err != nil {
		return "", nil, err
	}

	manifest, err := img.Manifest()
	if err != nil {
		slog.ErrorContext(ctx, "fastpull read manifest failed", "err", err)
		return "", nil, fmt.Errorf("fastpull: manifest %s: %w", image, err)
	}
	blobs := make([]v1.Descriptor, 0, len(manifest.Layers)+1)
	blobs = append(blobs, manifest.Config)
	blobs = append(blobs, manifest.Layers...)

	client := &http.Client{Timeout: clientTimeout}
	if err := s.downloadBlobs(ctx, client, scheme, host, repo, blobs); err != nil {
		return "", nil, err
	}

	handler := registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(s.dir)))
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", loopbackHost+":0")
	if err != nil {
		slog.ErrorContext(ctx, "fastpull listen failed", "err", err)
		return "", nil, fmt.Errorf("fastpull: listen %s: %w", loopbackHost, err)
	}
	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		_ = listener.Close()
		err := fmt.Errorf("fastpull: unexpected listener address type %T", listener.Addr())
		slog.ErrorContext(ctx, "fastpull listener addr type", "err", err)
		return "", nil, err
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: readHeaderTimeout}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "fastpull registry goroutine panic", "err", fmt.Errorf("panic: %v", r))
			}
		}()
		_ = srv.Serve(listener)
	}()
	stop := func() { _ = srv.Close() }
	port := tcpAddr.Port

	if err := s.registerManifests(ctx, client, port, repo, tag, img, archDigest, idxRaw, idxMediaType); err != nil {
		stop()
		return "", nil, err
	}

	localRef := loopbackHost + ":" + strconv.Itoa(port) + "/" + repo + ":" + tag
	slog.InfoContext(ctx, "fastpull staged base image", "ref", localRef, "blobs", len(blobs))
	return localRef, stop, nil
}

// resolveImage returns the arm64 image to serve. When desc is an index it
// returns the index raw bytes and media type as well, so the index can be served
// at the tag; otherwise those are empty and the arm64 image is served at the tag.
func resolveImage(ctx context.Context, desc *remote.Descriptor) (v1.Image, []byte, string, v1.Hash, error) {
	if !desc.MediaType.IsIndex() {
		img, err := desc.Image()
		if err != nil {
			slog.ErrorContext(ctx, "fastpull resolve image failed", "err", err)
			return nil, nil, "", v1.Hash{}, fmt.Errorf("fastpull: image: %w", err)
		}
		digest, err := img.Digest()
		if err != nil {
			slog.ErrorContext(ctx, "fastpull image digest failed", "err", err)
			return nil, nil, "", v1.Hash{}, fmt.Errorf("fastpull: image digest: %w", err)
		}
		return img, nil, "", digest, nil
	}
	idx, err := desc.ImageIndex()
	if err != nil {
		slog.ErrorContext(ctx, "fastpull image index failed", "err", err)
		return nil, nil, "", v1.Hash{}, fmt.Errorf("fastpull: index: %w", err)
	}
	indexManifest, err := idx.IndexManifest()
	if err != nil {
		slog.ErrorContext(ctx, "fastpull index manifest failed", "err", err)
		return nil, nil, "", v1.Hash{}, fmt.Errorf("fastpull: index manifest: %w", err)
	}
	archDigest, err := selectArm64(indexManifest.Manifests)
	if err != nil {
		slog.ErrorContext(ctx, "fastpull arch select failed", "err", err)
		return nil, nil, "", v1.Hash{}, err
	}
	img, err := idx.Image(archDigest)
	if err != nil {
		slog.ErrorContext(ctx, "fastpull index image failed", "err", err)
		return nil, nil, "", v1.Hash{}, fmt.Errorf("fastpull: index image: %w", err)
	}
	idxRaw, err := idx.RawManifest()
	if err != nil {
		slog.ErrorContext(ctx, "fastpull index raw failed", "err", err)
		return nil, nil, "", v1.Hash{}, fmt.Errorf("fastpull: index raw: %w", err)
	}
	mediaType, err := idx.MediaType()
	if err != nil {
		slog.ErrorContext(ctx, "fastpull index media type failed", "err", err)
		return nil, nil, "", v1.Hash{}, fmt.Errorf("fastpull: index media type: %w", err)
	}
	return img, idxRaw, string(mediaType), archDigest, nil
}

// downloadBlobs fetches every blob not already present, writing each to
// <dir>/sha256/<hex>, which is exactly where the ggcr disk handler reads it.
func (s *Stager) downloadBlobs(ctx context.Context, client *http.Client, scheme, host, repo string, blobs []v1.Descriptor) error {
	items := make([]aria2.Item, 0, len(blobs))
	for _, blob := range blobs {
		outPath := filepath.Join(s.dir, blob.Digest.Algorithm, blob.Digest.Hex)
		if info, statErr := os.Stat(outPath); statErr == nil && info.Size() == blob.Size {
			continue
		}
		items = append(items, aria2.Item{
			URL:     scheme + "://" + host + "/v2/" + repo + "/blobs/" + blob.Digest.String(),
			OutPath: outPath,
		})
	}
	if len(items) == 0 {
		return nil
	}
	token := fetchPullToken(ctx, client, scheme, host, repo)
	authHeader := ""
	if token != "" {
		authHeader = "Authorization: Bearer " + token
	}
	if err := s.download(ctx, items, authHeader, aria2.Options{
		Split:            s.split,
		MaxConnPerServer: s.maxConnPerServer,
		MaxConcurrent:    s.maxConcurrent,
	}); err != nil {
		slog.ErrorContext(ctx, "fastpull blob download failed", "err", err, "blobs", len(items))
		return fmt.Errorf("fastpull: download blobs: %w", err)
	}
	return nil
}

// registerManifests PUTs the arm64 image manifest by digest, then serves the tag:
// the index when there is one, otherwise the arm64 image manifest.
func (s *Stager) registerManifests(ctx context.Context, client *http.Client, port int, repo, tag string, img v1.Image, archDigest v1.Hash, idxRaw []byte, idxMediaType string) error {
	base := "http://" + loopbackHost + ":" + strconv.Itoa(port) + "/v2/" + repo + "/manifests/"
	armRaw, err := img.RawManifest()
	if err != nil {
		slog.ErrorContext(ctx, "fastpull arm raw manifest failed", "err", err)
		return fmt.Errorf("fastpull: arm raw manifest: %w", err)
	}
	armMediaType, err := img.MediaType()
	if err != nil {
		slog.ErrorContext(ctx, "fastpull arm media type failed", "err", err)
		return fmt.Errorf("fastpull: arm media type: %w", err)
	}
	if err := putManifest(ctx, client, base+archDigest.String(), string(armMediaType), armRaw); err != nil {
		return err
	}
	if idxRaw != nil {
		return putManifest(ctx, client, base+tag, idxMediaType, idxRaw)
	}
	return putManifest(ctx, client, base+tag, string(armMediaType), armRaw)
}

// putManifest PUTs raw manifest bytes to the loopback registry.
func putManifest(ctx context.Context, client *http.Client, url, mediaType string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		slog.ErrorContext(ctx, "fastpull build put failed", "err", err, "url", url)
		return fmt.Errorf("fastpull: build put %s: %w", url, err)
	}
	req.Header.Set("Content-Type", mediaType)
	resp, err := client.Do(req)
	if err != nil {
		slog.ErrorContext(ctx, "fastpull put request failed", "err", err, "url", url)
		return fmt.Errorf("fastpull: put %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		err := fmt.Errorf("fastpull: put %s status %d", url, resp.StatusCode)
		slog.ErrorContext(ctx, "fastpull put non-2xx", "err", err, "status", resp.StatusCode)
		return err
	}
	return nil
}
