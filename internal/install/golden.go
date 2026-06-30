package install

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/golden"
	"goodkind.io/gha-mac-broker/internal/tart"
)

const (
	// buildVMSuffix names the scratch VM used during the golden build.
	buildVMSuffix = "-build"
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
	builder := golden.New(vm)
	goldenName := golden.NameForImage(cfg.Tart.BaseImage)
	if _, err := builder.EnsureGolden(ctx, golden.EnsureOptions{
		Image:         cfg.Tart.BaseImage,
		BuildVM:       goldenName + buildVMSuffix,
		RunnerVersion: "",
	}); err != nil {
		slog.ErrorContext(ctx, "golden ensure failed", "err", err, "golden", goldenName)
		return fmt.Errorf("install: ensure golden: %w", err)
	}
	slog.InfoContext(ctx, "golden ready", "golden", goldenName)
	return nil
}
