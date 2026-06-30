package install

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os/exec"
	"slices"
	"strings"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/golden"
	"goodkind.io/gha-mac-broker/internal/tart"
)

const (
	// defaultBaseImage is the Cirrus base the golden build clones.
	defaultBaseImage = "ghcr.io/cirruslabs/macos-tahoe-base:latest"
	// buildVMSuffix names the scratch VM used during the golden build.
	buildVMSuffix = "-build"
	// runnerLatestURL returns the latest actions/runner release.
	runnerLatestURL = "https://api.github.com/repos/actions/runner/releases/latest"
)

// buildGoldenIfAbsent builds the configured golden image when it is not already
// present. It requires tart on PATH and fails with an install hint otherwise.
func buildGoldenIfAbsent(ctx context.Context, configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		slog.ErrorContext(ctx, "load config for golden failed", "err", err, "path", configPath)
		return fmt.Errorf("install: load config: %w", err)
	}
	if _, err := exec.LookPath(cfg.Tart.Binary); err != nil {
		slog.ErrorContext(ctx, "tart not found on PATH", "err", err, "binary", cfg.Tart.Binary)
		return fmt.Errorf("install: tart %q not found on PATH; install it with `brew install cirruslabs/cli/tart`: %w", cfg.Tart.Binary, err)
	}

	vm := tart.New(cfg.Tart.Binary)
	present, err := goldenPresent(ctx, vm, cfg.Tart.GoldenImage)
	if err != nil {
		return err
	}
	if present {
		slog.InfoContext(ctx, "golden image present; skipping build", "golden", cfg.Tart.GoldenImage)
		return nil
	}

	runnerVersion, err := resolveRunnerVersion(ctx)
	if err != nil {
		return err
	}
	builder := golden.New(vm)
	if err := builder.Build(ctx, golden.Options{
		BaseImage:     defaultBaseImage,
		GoldenName:    cfg.Tart.GoldenImage,
		BuildVM:       cfg.Tart.GoldenImage + buildVMSuffix,
		RunnerVersion: runnerVersion,
	}); err != nil {
		slog.ErrorContext(ctx, "golden build failed", "err", err, "golden", cfg.Tart.GoldenImage)
		return fmt.Errorf("install: build golden: %w", err)
	}
	slog.InfoContext(ctx, "golden build complete", "golden", cfg.Tart.GoldenImage)
	return nil
}

// goldenPresent reports whether the named image already exists in tart.
func goldenPresent(ctx context.Context, vm *tart.Tart, goldenName string) (bool, error) {
	names, err := vm.List(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "list VMs failed", "err", err)
		return false, fmt.Errorf("install: list VMs: %w", err)
	}
	return slices.Contains(names, goldenName), nil
}

// runnerRelease is the subset of the actions/runner release API the installer
// reads.
type runnerRelease struct {
	TagName string `json:"tag_name"`
}

// resolveRunnerVersion fetches the latest actions/runner release tag without
// the leading "v".
func resolveRunnerVersion(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, runnerLatestURL, nil)
	if err != nil {
		slog.ErrorContext(ctx, "build runner request failed", "err", err)
		return "", fmt.Errorf("install: build runner request: %w", err)
	}
	client := &http.Client{Timeout: httpTimeout}
	resp, err := client.Do(req)
	if err != nil {
		slog.ErrorContext(ctx, "fetch latest runner failed", "err", err)
		return "", fmt.Errorf("install: fetch latest runner: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		slog.ErrorContext(ctx, "runner status not ok", "err", fmt.Errorf("status %d", resp.StatusCode), "status", resp.StatusCode)
		return "", fmt.Errorf("install: latest runner status %d", resp.StatusCode)
	}
	var rel runnerRelease
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		slog.ErrorContext(ctx, "decode runner release failed", "err", err)
		return "", fmt.Errorf("install: decode runner release: %w", err)
	}
	return strings.TrimPrefix(rel.TagName, "v"), nil
}
