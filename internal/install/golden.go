package install

import (
	"context"
	"fmt"
	"log/slog"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/fastpull"
	"goodkind.io/gha-mac-broker/internal/golden"
	"goodkind.io/gha-mac-broker/internal/skopeo"
	"goodkind.io/gha-mac-broker/internal/tart"
)

const (
	// buildVMSuffix names the scratch VM used during the golden build.
	buildVMSuffix = "-build"
)

// buildGoldenIfAbsent builds the configured golden image when it is not already
// present. It requires tart and skopeo on PATH and fails with install hints
// otherwise.
func buildGoldenIfAbsent(ctx context.Context, configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		slog.ErrorContext(ctx, "load config for golden failed", "err", err, "path", configPath)
		return fmt.Errorf("install: load config: %w", err)
	}
	if err := requireHostBinary(ctx, cfg.Tart.Binary, "brew install cirruslabs/cli/tart"); err != nil {
		return err
	}
	if err := requireHostBinary(ctx, "skopeo", "brew install skopeo"); err != nil {
		return err
	}

	vm := tart.New(cfg.Tart.Binary)
	builder := golden.New(vm, golden.WithBaseStager(fastPullStager(cfg)))
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

// fastPullStager returns the base-image stager configured by cfg, or nil when
// fast pull is disabled. The nil return is an untyped nil interface, so the
// golden builder treats it as absent and clones the base ref directly.
func fastPullStager(cfg *config.Config) golden.BaseStager {
	if cfg.Tart.FastPull != nil && !*cfg.Tart.FastPull {
		return nil
	}
	return fastpull.New(fastpull.Options{
		Copier: skopeo.New("skopeo"),
		Dir:    cfg.Tart.FastPullDir,
	})
}
