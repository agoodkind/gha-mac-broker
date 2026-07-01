// Package fastpull stages a Cirrus base image into a loopback OCI registry on
// the localhost loopback so tart can clone it quickly. Skopeo copies the source image into an OCI
// layout on disk, and go-containerregistry serves that layout's blobs over the
// loopback registry while tart pulls the tag with --insecure.
package fastpull

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
)

const (
	loopbackHost      = "localhost"
	readHeaderTimeout = 30 * time.Second
	clientTimeout     = 30 * time.Second
	targetOS          = "darwin"
	targetArch        = "arm64"
	// cirrusRegistry is the only registry host the fast pull serves from, so a
	// cirrus-shaped repo path on a different host (docker.io/cirruslabs/...)
	// cannot slip past the allowlist.
	cirrusRegistry = "ghcr.io"
)

// Options configures a [Stager].
type Options struct {
	// Copier copies the source image into an OCI layout. Production passes the
	// skopeo client; tests provide a tiny fake layout writer.
	Copier ociCopier
	// Dir is the OCI layout directory. Blobs live under <Dir>/blobs.
	Dir string
}

type ociCopier interface {
	CopyToOCILayout(ctx context.Context, srcImageRef, layoutDir, tag, osName, arch string) error
}

// Stager stages a Cirrus base image into a loopback registry. See the package
// doc for the mechanism.
type Stager struct {
	copier ociCopier
	dir    string
}

type ociIndex struct {
	SchemaVersion int             `json:"schemaVersion"`
	Manifests     []ociDescriptor `json:"manifests"`
}

type ociDescriptor struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
}

// New returns a Stager configured by opts.
func New(opts Options) *Stager {
	return &Stager{copier: opts.Copier, dir: opts.Dir}
}

// Stage copies image into an OCI layout, serves the layout blobs from a loopback
// OCI registry on the localhost loopback, and returns a clonable insecure ref plus a stop func
// that shuts the registry down. The caller clones the returned ref with tart
// --insecure, then calls stop.
func (s *Stager) Stage(ctx context.Context, image string) (string, func(), error) {
	ref, err := name.ParseReference(image, name.Insecure)
	if err != nil {
		slog.ErrorContext(ctx, "fastpull parse ref failed", "err", err, "image", image)
		return "", nil, fmt.Errorf("fastpull: parse %s: %w", image, err)
	}
	host := ref.Context().RegistryStr()
	repo := ref.Context().RepositoryStr()
	if host != cirrusRegistry || !isCirrusXcodeRepo(repo) {
		err := fmt.Errorf("fastpull: refusing non-cirrus image %q", image)
		slog.ErrorContext(ctx, "fastpull refusing non-cirrus image", "err", err, "image", image, "host", host)
		return "", nil, err
	}
	if s.copier == nil {
		err := fmt.Errorf("fastpull: missing OCI layout copier")
		slog.ErrorContext(ctx, "fastpull copier missing", "err", err)
		return "", nil, err
	}

	tag := ref.Identifier()
	if err := s.copier.CopyToOCILayout(ctx, image, s.dir, tag, targetOS, targetArch); err != nil {
		slog.ErrorContext(ctx, "fastpull OCI layout copy failed", "err", err, "image", image)
		return "", nil, fmt.Errorf("fastpull: copy %s to OCI layout: %w", image, err)
	}

	client := &http.Client{Timeout: clientTimeout}
	handler := registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(filepath.Join(s.dir, "blobs"))))
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

	if err := s.registerManifest(ctx, client, port, repo, tag); err != nil {
		stop()
		return "", nil, err
	}

	localRef := loopbackHost + ":" + strconv.Itoa(port) + "/" + repo + ":" + tag
	slog.InfoContext(ctx, "fastpull staged base image", "ref", localRef)
	return localRef, stop, nil
}

func (s *Stager) registerManifest(ctx context.Context, client *http.Client, port int, repo, tag string) error {
	desc, manifestBytes, err := s.readLayoutManifest(ctx)
	if err != nil {
		return err
	}
	base := "http://" + loopbackHost + ":" + strconv.Itoa(port) + "/v2/" + repo + "/manifests/"
	if err := putManifest(ctx, client, base+desc.Digest, desc.MediaType, manifestBytes); err != nil {
		return err
	}
	if err := putManifest(ctx, client, base+tag, desc.MediaType, manifestBytes); err != nil {
		return err
	}
	return nil
}

func (s *Stager) readLayoutManifest(ctx context.Context) (ociDescriptor, []byte, error) {
	indexPath := filepath.Join(s.dir, "index.json")
	indexBytes, err := os.ReadFile(indexPath)
	if err != nil {
		slog.ErrorContext(ctx, "fastpull read OCI index failed", "err", err, "path", indexPath)
		return ociDescriptor{}, nil, fmt.Errorf("fastpull: read OCI index %s: %w", indexPath, err)
	}
	var index ociIndex
	if err := json.Unmarshal(indexBytes, &index); err != nil {
		slog.ErrorContext(ctx, "fastpull parse OCI index failed", "err", err, "path", indexPath)
		return ociDescriptor{}, nil, fmt.Errorf("fastpull: parse OCI index %s: %w", indexPath, err)
	}
	if len(index.Manifests) != 1 {
		err := fmt.Errorf("fastpull: OCI index has %d manifests, want 1", len(index.Manifests))
		slog.ErrorContext(ctx, "fastpull OCI index manifest count invalid", "err", err, "path", indexPath)
		return ociDescriptor{}, nil, err
	}
	desc := index.Manifests[0]
	if desc.MediaType == "" || desc.Digest == "" {
		err := fmt.Errorf("fastpull: OCI index manifest descriptor missing mediaType or digest")
		slog.ErrorContext(ctx, "fastpull OCI descriptor invalid", "err", err, "path", indexPath)
		return ociDescriptor{}, nil, err
	}
	manifestPath, err := blobPathForDigest(s.dir, desc.Digest)
	if err != nil {
		slog.ErrorContext(ctx, "fastpull OCI manifest digest invalid", "err", err, "digest", desc.Digest)
		return ociDescriptor{}, nil, err
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		slog.ErrorContext(ctx, "fastpull read OCI manifest blob failed", "err", err, "path", manifestPath)
		return ociDescriptor{}, nil, fmt.Errorf("fastpull: read OCI manifest %s: %w", manifestPath, err)
	}
	return desc, manifestBytes, nil
}

func blobPathForDigest(layoutDir string, digest string) (string, error) {
	algorithm, hexValue, found := strings.Cut(digest, ":")
	if !found || algorithm == "" || hexValue == "" {
		return "", fmt.Errorf("fastpull: invalid digest %q", digest)
	}
	return filepath.Join(layoutDir, "blobs", algorithm, hexValue), nil
}

// putManifest PUTs raw manifest bytes to the loopback registry.
func putManifest(ctx context.Context, client *http.Client, url, mediaType string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, mediaTypeReader(body))
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

func mediaTypeReader(body []byte) *bytes.Reader {
	return bytes.NewReader(body)
}
