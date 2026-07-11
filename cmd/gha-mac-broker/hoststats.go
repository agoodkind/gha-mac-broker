package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/hoststats"
	"goodkind.io/gha-mac-broker/internal/runnerpool"
	"goodkind.io/gha-mac-broker/internal/server"
)

// metricsOptions derives hoststats.Options from the config's Metrics block.
func metricsOptions(cfg *config.Config) hoststats.Options {
	return hoststats.Options{
		Enabled:  cfg.Metrics.Enabled != nil && *cfg.Metrics.Enabled,
		Interval: time.Duration(cfg.Metrics.Interval),
	}
}

// newHostStatsSampler builds the host-stats sampler that reads real host
// metrics through gopsutil and correlates them with p's pool inventory. The
// caller starts its loop with Sampler.Start and applies config reloads with
// Sampler.Reconfigure.
func newHostStatsSampler(cfg *config.Config, p *runnerpool.Pool) *hoststats.Sampler {
	reader := hoststats.NewGopsutilReader(cfg.Metrics.DiskPath)
	inventory := func(ctx context.Context) hoststats.Inventory {
		snap, _ := p.Status(ctx)
		return hoststats.Inventory{
			RunnerCount: snap.RunnerCount,
			Idle:        snap.Idle,
			Busy:        snap.Busy,
			Queued:      snap.Queued,
		}
	}
	return hoststats.New(reader, inventory, time.Now, metricsOptions(cfg))
}

// startBrokerConfigReloadWatcher starts the config-file poll-watcher and wires
// its apply callback to applyReloadedConfig with the daemon's live components.
func startBrokerConfigReloadWatcher(ctx context.Context, configPath string, initialModTime time.Time, binder *broker.Binder, p *runnerpool.Pool, srv *server.Server, sampler *hoststats.Sampler) {
	startConfigReloadWatcher(ctx, configReloadWatcherOptions{
		path:           configPath,
		initialModTime: initialModTime,
		apply: func(reloadCtx context.Context, reloadedConfig *config.Config) error {
			return applyReloadedConfig(reloadCtx, reloadedConfig, binder, p, srv, sampler)
		},
	})
}

func applyReloadedConfig(ctx context.Context, cfg *config.Config, binder *broker.Binder, p *runnerpool.Pool, srv *server.Server, sampler *hoststats.Sampler) error {
	secret, err := cfg.ReadWebhookSecret()
	if err != nil {
		slog.ErrorContext(ctx, "config reload read webhook secret failed", "err", err)
		return fmt.Errorf("read webhook secret: %w", err)
	}
	capacityToken, err := cfg.ReadCapacityToken()
	if err != nil {
		slog.ErrorContext(ctx, "config reload read capacity token failed", "err", err)
		return fmt.Errorf("read capacity token: %w", err)
	}
	webhookCIDRs, err := cfg.ReadWebhookCIDRs()
	if err != nil {
		slog.ErrorContext(ctx, "config reload read webhook CIDRs failed", "err", err)
		return fmt.Errorf("read webhook CIDRs: %w", err)
	}
	binder.Reconfigure(cfg)
	p.Reconfigure(runnerPoolOptionsFromConfig(cfg, "", nil))
	srv.Reconfigure(secret, cfg, capacityToken, webhookCIDRs)
	sampler.Reconfigure(metricsOptions(cfg))
	appliedRunnerCount := p.Snapshot().RunnerCount
	if cfg.RunnerCount != appliedRunnerCount {
		slog.InfoContext(
			ctx,
			"config reload applied",
			"runner_count",
			appliedRunnerCount,
			"requested_runner_count",
			cfg.RunnerCount,
			"runner_count_note",
			"restart required",
			"jobs_per_vm",
			cfg.JobsPerVM,
		)
		return nil
	}
	slog.InfoContext(ctx, "config reload applied", "runner_count", appliedRunnerCount, "jobs_per_vm", cfg.JobsPerVM)
	return nil
}
