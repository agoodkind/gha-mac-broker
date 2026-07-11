package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"goodkind.io/gha-mac-broker/internal/golden"
)

// fixtureRunnerTarball builds a gzip tar stream that resembles the actions/runner
// release layout: an executable run.sh, a nested regular file, and a symlink.
func fixtureRunnerTarball(t *testing.T) io.ReadCloser {
	t.Helper()
	var buffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&buffer)
	tarWriter := tar.NewWriter(gzipWriter)

	writeFile := func(name string, mode int64, body string) {
		header := &tar.Header{Name: name, Mode: mode, Size: int64(len(body)), Typeflag: tar.TypeReg}
		if err := tarWriter.WriteHeader(header); err != nil {
			t.Fatalf("write header %s: %v", name, err)
		}
		if _, err := tarWriter.Write([]byte(body)); err != nil {
			t.Fatalf("write body %s: %v", name, err)
		}
	}
	dirHeader := &tar.Header{Name: "bin/", Mode: 0o755, Typeflag: tar.TypeDir}
	if err := tarWriter.WriteHeader(dirHeader); err != nil {
		t.Fatalf("write dir header: %v", err)
	}
	writeFile("run.sh", 0o755, "#!/bin/sh\necho runner\n")
	writeFile("bin/Runner.Listener", 0o755, "listener")
	symlinkHeader := &tar.Header{Name: "run", Linkname: "run.sh", Typeflag: tar.TypeSymlink}
	if err := tarWriter.WriteHeader(symlinkHeader); err != nil {
		t.Fatalf("write symlink header: %v", err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	return io.NopCloser(bytes.NewReader(buffer.Bytes()))
}

func TestExtractRunnerTarballLandsExecutableEntrypoint(t *testing.T) {
	destDir := filepath.Join(t.TempDir(), "actions-runner")
	stream := fixtureRunnerTarball(t)
	defer func() { _ = stream.Close() }()
	if err := extractRunnerTarball(context.Background(), stream, destDir); err != nil {
		t.Fatalf("extractRunnerTarball: %v", err)
	}

	runScript := filepath.Join(destDir, "run.sh")
	info, err := os.Stat(runScript)
	if err != nil {
		t.Fatalf("stat run.sh: %v", err)
	}
	if info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("run.sh mode = %v, want owner-executable", info.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(destDir, "bin", "Runner.Listener")); err != nil {
		t.Fatalf("nested runner file missing: %v", err)
	}
	linkTarget, err := os.Readlink(filepath.Join(destDir, "run"))
	if err != nil {
		t.Fatalf("readlink run: %v", err)
	}
	if linkTarget != "run.sh" {
		t.Fatalf("symlink target = %q, want run.sh", linkTarget)
	}
}

func TestExtractRunnerTarballRejectsPathEscape(t *testing.T) {
	destDir := filepath.Join(t.TempDir(), "actions-runner")
	var buffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&buffer)
	tarWriter := tar.NewWriter(gzipWriter)
	header := &tar.Header{Name: "../escape.sh", Mode: 0o755, Size: 3, Typeflag: tar.TypeReg}
	if err := tarWriter.WriteHeader(header); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if _, err := tarWriter.Write([]byte("bad")); err != nil {
		t.Fatalf("write body: %v", err)
	}
	_ = tarWriter.Close()
	_ = gzipWriter.Close()

	err := extractRunnerTarball(context.Background(), bytes.NewReader(buffer.Bytes()), destDir)
	if err == nil {
		t.Fatal("extractRunnerTarball accepted a path-escaping entry, want rejection")
	}
}

func TestProvisionGoldenWritesBakedFilesAndRemovesWatchdog(t *testing.T) {
	root := t.TempDir()
	runnerDir := filepath.Join(root, "home", "actions-runner")
	watchdogScript := filepath.Join(root, "usr", "local", "bin", "gha-broker-watchdog.sh")
	watchdogPlist := filepath.Join(root, "Library", "LaunchDaemons", "io.goodkind.gha-broker-watchdog.plist")
	if err := os.MkdirAll(filepath.Dir(watchdogScript), 0o755); err != nil {
		t.Fatalf("mkdir watchdog script dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(watchdogPlist), 0o755); err != nil {
		t.Fatalf("mkdir watchdog plist dir: %v", err)
	}
	if err := os.WriteFile(watchdogScript, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("seed watchdog script: %v", err)
	}
	if err := os.WriteFile(watchdogPlist, []byte("<plist/>"), 0o644); err != nil {
		t.Fatalf("seed watchdog plist: %v", err)
	}

	binarySource := filepath.Join(root, "src-binary")
	if err := os.WriteFile(binarySource, []byte("guest broker binary"), 0o755); err != nil {
		t.Fatalf("write source binary: %v", err)
	}

	paths := provisionPaths{
		binaryDest:      filepath.Join(root, "usr", "local", "bin", "gha-mac-broker"),
		plistDest:       filepath.Join(root, "Library", "LaunchDaemons", "io.goodkind.gha-mac-broker-guest.plist"),
		fingerprintDest: filepath.Join(root, "usr", "local", "share", "gha-guest", "golden.fingerprint"),
		watchdogScript:  watchdogScript,
		watchdogPlist:   watchdogPlist,
		runnerDir:       runnerDir,
	}

	signed := 0
	req := provisionRequest{
		runnerVersion: "2.335.1",
		binarySource:  binarySource,
		fingerprint:   "deadbeef",
		paths:         paths,
		download: func(_ context.Context, _ string) (io.ReadCloser, error) {
			return fixtureRunnerTarball(t), nil
		},
		sign: func(_ context.Context, path string) error {
			signed++
			if path != paths.binaryDest {
				t.Fatalf("sign called on %q, want %q", path, paths.binaryDest)
			}
			return nil
		},
	}

	if err := provisionGolden(context.Background(), req); err != nil {
		t.Fatalf("provisionGolden: %v", err)
	}

	if _, err := os.Stat(filepath.Join(runnerDir, "run.sh")); err != nil {
		t.Fatalf("runner run.sh missing: %v", err)
	}
	bakedBinary, err := os.ReadFile(paths.binaryDest)
	if err != nil {
		t.Fatalf("read baked binary: %v", err)
	}
	if string(bakedBinary) != "guest broker binary" {
		t.Fatalf("baked binary = %q, want source content", string(bakedBinary))
	}
	binaryInfo, err := os.Stat(paths.binaryDest)
	if err != nil {
		t.Fatalf("stat baked binary: %v", err)
	}
	if binaryInfo.Mode().Perm() != bakedBinaryMode {
		t.Fatalf("baked binary mode = %v, want %v", binaryInfo.Mode().Perm(), os.FileMode(bakedBinaryMode))
	}
	if signed != 1 {
		t.Fatalf("sign call count = %d, want 1", signed)
	}

	plist, err := os.ReadFile(paths.plistDest)
	if err != nil {
		t.Fatalf("read baked plist: %v", err)
	}
	if !bytes.Equal(plist, golden.GuestSupervisorPlist()) {
		t.Fatal("baked plist does not match embedded supervisor plist")
	}

	fingerprint, err := os.ReadFile(paths.fingerprintDest)
	if err != nil {
		t.Fatalf("read fingerprint: %v", err)
	}
	if string(fingerprint) != "deadbeef\n" {
		t.Fatalf("fingerprint file = %q, want %q", string(fingerprint), "deadbeef\n")
	}

	if _, err := os.Stat(watchdogScript); !os.IsNotExist(err) {
		t.Fatalf("watchdog script still present, stat err = %v", err)
	}
	if _, err := os.Stat(watchdogPlist); !os.IsNotExist(err) {
		t.Fatalf("watchdog plist still present, stat err = %v", err)
	}
}

func TestRemoveLegacyWatchdogIgnoresMissingFiles(t *testing.T) {
	root := t.TempDir()
	paths := provisionPaths{
		watchdogScript: filepath.Join(root, "absent.sh"),
		watchdogPlist:  filepath.Join(root, "absent.plist"),
	}
	if err := removeLegacyWatchdog(context.Background(), paths); err != nil {
		t.Fatalf("removeLegacyWatchdog on absent files: %v", err)
	}
}
