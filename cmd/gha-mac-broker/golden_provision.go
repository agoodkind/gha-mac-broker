package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"goodkind.io/gha-mac-broker/internal/golden"
)

const (
	// provisionRunnerAsset is the actions/runner tarball name pattern for macOS
	// arm64, matching the release the host resolves the version for.
	provisionRunnerAsset = "actions-runner-osx-arm64-%s.tar.gz"
	// provisionRunnerURL is the actions/runner release download URL template.
	provisionRunnerURL = "https://github.com/actions/runner/releases/download/v%s/" + provisionRunnerAsset
	// runnerDownloadTimeout bounds the in-VM runner tarball download.
	runnerDownloadTimeout = 10 * time.Minute
	// bakedBinaryMode is the mode of the installed guest broker binary.
	bakedBinaryMode = 0o755
	// bakedFileMode is the mode of the baked plist and fingerprint files.
	bakedFileMode = 0o644
	// bakedDirMode is the mode of directories the provisioner creates.
	bakedDirMode = 0o755
	// runnerRunScript is the runner entrypoint the extract step must land.
	runnerRunScript = "run.sh"
)

// binarySigner clears the quarantine xattr and re-signs a placed arm64 binary so
// it is not Killed:9 on first exec. It is a field so tests can substitute a no-op.
type binarySigner func(ctx context.Context, path string) error

// runnerDownloader opens the actions/runner tarball stream for a version. It is a
// field so tests can drive the provisioner without network access.
type runnerDownloader func(ctx context.Context, version string) (io.ReadCloser, error)

// provisionPaths are the destination paths the provisioner writes. Tests point
// them at a temp dir; production uses the fixed baked locations.
type provisionPaths struct {
	binaryDest      string
	plistDest       string
	fingerprintDest string
	watchdogScript  string
	watchdogPlist   string
	runnerDir       string
}

// defaultProvisionPaths returns the fixed baked destinations, with the runner
// installed into runnerDir (the admin user's home, resolved by the host).
func defaultProvisionPaths(runnerDir string) provisionPaths {
	return provisionPaths{
		binaryDest:      golden.BakedBinaryPath,
		plistDest:       golden.GuestSupervisorPlistPath,
		fingerprintDest: golden.FingerprintPath,
		watchdogScript:  golden.LegacyWatchdogScriptPath,
		watchdogPlist:   golden.LegacyWatchdogPlistPath,
		runnerDir:       runnerDir,
	}
}

// provisionRequest is the fully-resolved provisioning job the orchestrator runs.
type provisionRequest struct {
	runnerVersion string
	binarySource  string
	fingerprint   string
	paths         provisionPaths
	download      runnerDownloader
	sign          binarySigner
}

// runGoldenProvision is the in-VM golden-provision subcommand. It runs as root
// under the host's tart exec, installing the runner, baking the guest binary and
// launchd unit, persisting the fingerprint, and deleting the retired watchdog.
func runGoldenProvision(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("golden-provision", flag.ExitOnError)
	runnerVersion := fs.String("runner-version", "", "actions/runner version to install")
	binarySource := fs.String("binary", "", "path to the guest broker binary to bake into the image")
	fingerprint := fs.String("fingerprint", "", "golden fingerprint to persist into the image")
	runnerDir := fs.String("runner-dir", "", "directory to install the unconfigured runner into")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "golden-provision flag parse failed", "err", err)
		return fmt.Errorf("golden-provision flags: %w", err)
	}
	if *runnerVersion == "" {
		return fmt.Errorf("golden-provision requires -runner-version")
	}
	if *binarySource == "" {
		return fmt.Errorf("golden-provision requires -binary")
	}
	if *fingerprint == "" {
		return fmt.Errorf("golden-provision requires -fingerprint")
	}
	if *runnerDir == "" {
		return fmt.Errorf("golden-provision requires -runner-dir")
	}
	return provisionGolden(ctx, provisionRequest{
		runnerVersion: *runnerVersion,
		binarySource:  *binarySource,
		fingerprint:   *fingerprint,
		paths:         defaultProvisionPaths(*runnerDir),
		download:      downloadRunnerTarball,
		sign:          codesignAdhoc,
	})
}

// provisionGolden runs the provisioning steps in order: install the runner, bake
// the guest binary and supervisor launchd unit, persist the fingerprint, and
// delete the retired watchdog. Every file write is pure Go via os primitives; the
// only external tools are xattr and codesign for the binary signature fixup.
func provisionGolden(ctx context.Context, req provisionRequest) error {
	if err := installRunner(ctx, req.download, req.runnerVersion, req.paths.runnerDir); err != nil {
		return err
	}
	if err := installBakedBinary(ctx, req.binarySource, req.paths.binaryDest, req.sign); err != nil {
		return err
	}
	if err := writeBakedFile(req.paths.plistDest, golden.GuestSupervisorPlist(), bakedFileMode); err != nil {
		slog.ErrorContext(ctx, "write supervisor plist failed", "err", err, "dest", req.paths.plistDest)
		return fmt.Errorf("golden-provision: write supervisor plist: %w", err)
	}
	if err := writeBakedFile(req.paths.fingerprintDest, []byte(req.fingerprint+"\n"), bakedFileMode); err != nil {
		slog.ErrorContext(ctx, "write fingerprint failed", "err", err, "dest", req.paths.fingerprintDest)
		return fmt.Errorf("golden-provision: write fingerprint: %w", err)
	}
	if err := removeLegacyWatchdog(ctx, req.paths); err != nil {
		return err
	}
	slog.InfoContext(ctx, "golden provisioned", "fingerprint", req.fingerprint, "runner_dir", req.paths.runnerDir)
	return nil
}

// installRunner downloads the actions/runner tarball and extracts it, unconfigured,
// into runnerDir, then confirms the runner entrypoint landed.
func installRunner(ctx context.Context, download runnerDownloader, version, runnerDir string) error {
	body, err := download(ctx, version)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()
	if err := extractRunnerTarball(ctx, body, runnerDir); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(runnerDir, runnerRunScript)); err != nil {
		slog.ErrorContext(ctx, "runner entrypoint missing after extract", "err", err, "runner_dir", runnerDir)
		return fmt.Errorf("golden-provision: runner %s missing after extract: %w", runnerRunScript, err)
	}
	return nil
}

// downloadRunnerTarball opens the actions/runner release tarball over net/http.
// The client carries a total timeout covering the streamed body read, so the
// returned body needs no separate cancel wiring.
func downloadRunnerTarball(ctx context.Context, version string) (io.ReadCloser, error) {
	url := fmt.Sprintf(provisionRunnerURL, version, version)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		slog.ErrorContext(ctx, "build runner request failed", "err", err, "url", url)
		return nil, fmt.Errorf("golden-provision: build runner request: %w", err)
	}
	client := &http.Client{Timeout: runnerDownloadTimeout}
	resp, err := client.Do(req)
	if err != nil {
		slog.ErrorContext(ctx, "download runner failed", "err", err, "url", url)
		return nil, fmt.Errorf("golden-provision: download runner %s: %w", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		statusErr := fmt.Errorf("status %d", resp.StatusCode)
		slog.ErrorContext(ctx, "download runner bad status", "err", statusErr, "url", url)
		return nil, fmt.Errorf("golden-provision: download runner %s: %w", url, statusErr)
	}
	return resp.Body, nil
}

// extractRunnerTarball extracts a gzip-compressed tar stream into destDir. It
// rejects any entry whose path escapes destDir and preserves file modes so the
// runner entrypoint stays executable.
func extractRunnerTarball(ctx context.Context, gzStream io.Reader, destDir string) error {
	if err := os.MkdirAll(destDir, bakedDirMode); err != nil {
		slog.ErrorContext(ctx, "create runner dir failed", "err", err, "dir", destDir)
		return fmt.Errorf("golden-provision: create runner dir %s: %w", destDir, err)
	}
	gzipReader, err := gzip.NewReader(gzStream)
	if err != nil {
		slog.ErrorContext(ctx, "open runner gzip failed", "err", err)
		return fmt.Errorf("golden-provision: open runner gzip: %w", err)
	}
	defer func() { _ = gzipReader.Close() }()
	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			slog.ErrorContext(ctx, "read runner tar failed", "err", err)
			return fmt.Errorf("golden-provision: read runner tar: %w", err)
		}
		target, err := safeJoin(ctx, destDir, header.Name)
		if err != nil {
			return err
		}
		if err := extractTarEntry(ctx, tarReader, header, target); err != nil {
			return err
		}
	}
}

// safeJoin joins name onto destDir and rejects any result that escapes destDir,
// so a crafted tar entry cannot write outside the runner directory.
func safeJoin(ctx context.Context, destDir, name string) (string, error) {
	target := filepath.Join(destDir, name)
	rel, err := filepath.Rel(destDir, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		escapeErr := fmt.Errorf("tar entry %q escapes runner dir", name)
		slog.ErrorContext(ctx, "tar entry escapes runner dir", "err", escapeErr, "dir", destDir)
		return "", fmt.Errorf("golden-provision: %w", escapeErr)
	}
	return target, nil
}

// extractTarEntry writes one tar entry (directory, regular file, or symlink) to
// target, creating parent directories and preserving the recorded file mode.
func extractTarEntry(ctx context.Context, tarReader io.Reader, header *tar.Header, target string) error {
	entryMode := header.FileInfo().Mode().Perm()
	switch header.Typeflag {
	case tar.TypeDir:
		if err := os.MkdirAll(target, entryMode); err != nil {
			slog.ErrorContext(ctx, "create tar dir failed", "err", err, "target", target)
			return fmt.Errorf("golden-provision: create dir %s: %w", target, err)
		}
		return nil
	case tar.TypeReg:
		if err := os.MkdirAll(filepath.Dir(target), bakedDirMode); err != nil {
			slog.ErrorContext(ctx, "create tar parent failed", "err", err, "target", target)
			return fmt.Errorf("golden-provision: create parent of %s: %w", target, err)
		}
		file, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, entryMode)
		if err != nil {
			slog.ErrorContext(ctx, "create tar file failed", "err", err, "target", target)
			return fmt.Errorf("golden-provision: create %s: %w", target, err)
		}
		// The tar body is bounded by the archive, and the runner release is a
		// trusted GitHub asset, so a full copy of the entry is intended here.
		if _, err := io.Copy(file, tarReader); err != nil { // #nosec G110 -- trusted release asset
			_ = file.Close()
			slog.ErrorContext(ctx, "write tar file failed", "err", err, "target", target)
			return fmt.Errorf("golden-provision: write %s: %w", target, err)
		}
		if err := file.Close(); err != nil {
			slog.ErrorContext(ctx, "close tar file failed", "err", err, "target", target)
			return fmt.Errorf("golden-provision: close %s: %w", target, err)
		}
		return nil
	case tar.TypeSymlink:
		if err := os.MkdirAll(filepath.Dir(target), bakedDirMode); err != nil {
			slog.ErrorContext(ctx, "create symlink parent failed", "err", err, "target", target)
			return fmt.Errorf("golden-provision: create parent of %s: %w", target, err)
		}
		_ = os.Remove(target)
		if err := os.Symlink(header.Linkname, target); err != nil {
			slog.ErrorContext(ctx, "create symlink failed", "err", err, "target", target)
			return fmt.Errorf("golden-provision: symlink %s: %w", target, err)
		}
		return nil
	default:
		return nil
	}
}

// installBakedBinary copies the source binary to dest mode 0755, then clears its
// quarantine attribute and ad-hoc re-signs it so the copied arm64 binary runs.
func installBakedBinary(ctx context.Context, source, dest string, sign binarySigner) error {
	if err := copyFileMode(ctx, source, dest, bakedBinaryMode); err != nil {
		return err
	}
	if err := sign(ctx, dest); err != nil {
		return err
	}
	slog.InfoContext(ctx, "baked guest binary", "source", source, "dest", dest)
	return nil
}

// copyFileMode copies source to dest atomically via a temp file and rename,
// creating dest's parent directory and setting the given mode.
func copyFileMode(ctx context.Context, source, dest string, mode os.FileMode) error {
	destDir := filepath.Dir(dest)
	if err := os.MkdirAll(destDir, bakedDirMode); err != nil {
		slog.ErrorContext(ctx, "create binary dir failed", "err", err, "dir", destDir)
		return fmt.Errorf("golden-provision: create dir %s: %w", destDir, err)
	}
	in, err := os.Open(source)
	if err != nil {
		slog.ErrorContext(ctx, "open source binary failed", "err", err, "source", source)
		return fmt.Errorf("golden-provision: open source %s: %w", source, err)
	}
	defer func() { _ = in.Close() }()
	temp, err := os.CreateTemp(destDir, ".gha-provision-*")
	if err != nil {
		slog.ErrorContext(ctx, "create temp binary failed", "err", err, "dir", destDir)
		return fmt.Errorf("golden-provision: create temp in %s: %w", destDir, err)
	}
	tempPath := temp.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := io.Copy(temp, in); err != nil {
		_ = temp.Close()
		slog.ErrorContext(ctx, "copy binary failed", "err", err, "temp", tempPath)
		return fmt.Errorf("golden-provision: copy to %s: %w", tempPath, err)
	}
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		slog.ErrorContext(ctx, "chmod temp binary failed", "err", err, "temp", tempPath)
		return fmt.Errorf("golden-provision: chmod %s: %w", tempPath, err)
	}
	if err := temp.Close(); err != nil {
		slog.ErrorContext(ctx, "close temp binary failed", "err", err, "temp", tempPath)
		return fmt.Errorf("golden-provision: close %s: %w", tempPath, err)
	}
	if err := os.Rename(tempPath, dest); err != nil {
		slog.ErrorContext(ctx, "rename binary failed", "err", err, "temp", tempPath, "dest", dest)
		return fmt.Errorf("golden-provision: rename to %s: %w", dest, err)
	}
	removeTemp = false
	return nil
}

// writeBakedFile writes content to dest with the given mode, creating dest's
// parent directory.
func writeBakedFile(dest string, content []byte, mode os.FileMode) error {
	destDir := filepath.Dir(dest)
	if err := os.MkdirAll(destDir, bakedDirMode); err != nil {
		slog.Error("create baked file dir failed", "err", err, "dir", destDir)
		return fmt.Errorf("golden-provision: create dir %s: %w", destDir, err)
	}
	if err := os.WriteFile(dest, content, mode); err != nil {
		slog.Error("write baked file failed", "err", err, "dest", dest)
		return fmt.Errorf("golden-provision: write %s: %w", dest, err)
	}
	return nil
}

// removeLegacyWatchdog deletes the retired watchdog script and its plist, so the
// image never ships the old shell watchdog once the guest-supervisor unit owns
// liveness. A missing file is not an error.
func removeLegacyWatchdog(ctx context.Context, paths provisionPaths) error {
	for _, path := range []string{paths.watchdogScript, paths.watchdogPlist} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.ErrorContext(ctx, "remove legacy watchdog failed", "err", err, "path", path)
			return fmt.Errorf("golden-provision: remove %s: %w", path, err)
		}
	}
	return nil
}

// codesignAdhoc clears the quarantine xattr then ad-hoc signs the binary at path,
// using the Apple system tools directly. A copied arm64 binary without a valid
// signature is Killed:9 on first exec, so this fixup is required before the baked
// binary can run under launchd.
func codesignAdhoc(ctx context.Context, path string) error {
	// xattr -c is best effort: a binary with no extended attributes still succeeds,
	// but a nonzero exit is tolerated so a missing quarantine attribute is not fatal.
	xattrCmd := exec.CommandContext(ctx, "xattr", "-c", path)
	if out, err := xattrCmd.CombinedOutput(); err != nil {
		slog.WarnContext(ctx, "xattr clear returned nonzero; continuing", "err", err, "path", path, "output", strings.TrimSpace(string(out)))
	}
	signCmd := exec.CommandContext(ctx, "codesign", "-s", "-", "-f", path)
	if out, err := signCmd.CombinedOutput(); err != nil {
		slog.ErrorContext(ctx, "codesign failed", "err", err, "path", path, "output", strings.TrimSpace(string(out)))
		return fmt.Errorf("golden-provision: codesign %s: %w: %s", path, err, strings.TrimSpace(string(out)))
	}
	return nil
}
