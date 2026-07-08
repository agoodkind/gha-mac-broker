package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"goodkind.io/gha-mac-broker/internal/config"
)

const configReloadPollInterval = 5 * time.Second

type configReloadWatcherOptions struct {
	path           string
	initialModTime time.Time
	interval       time.Duration
	stat           func(string) (os.FileInfo, error)
	load           func(string) (*config.Config, error)
	apply          func(context.Context, *config.Config) error
	log            *slog.Logger
}

type configReloadWatcher struct {
	path           string
	stat           func(string) (os.FileInfo, error)
	load           func(string) (*config.Config, error)
	apply          func(context.Context, *config.Config) error
	log            *slog.Logger
	appliedModTime time.Time
	pendingModTime time.Time
	hasPending     bool
	initialized    bool
}

func startConfigReloadWatcher(ctx context.Context, options configReloadWatcherOptions) {
	watcher := newConfigReloadWatcher(options)
	interval := options.interval
	if interval <= 0 {
		interval = configReloadPollInterval
	}
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				watcher.log.ErrorContext(ctx, "config reload watcher panic recovered", "err", fmt.Errorf("panic: %v", recovered))
			}
		}()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				watcher.poll(ctx)
			}
		}
	}()
}

func newConfigReloadWatcher(options configReloadWatcherOptions) *configReloadWatcher {
	stat := options.stat
	if stat == nil {
		stat = os.Stat
	}
	load := options.load
	if load == nil {
		load = config.Load
	}
	log := options.log
	if log == nil {
		log = slog.Default()
	}
	return &configReloadWatcher{
		path:           options.path,
		stat:           stat,
		load:           load,
		apply:          options.apply,
		log:            log,
		appliedModTime: options.initialModTime,
		pendingModTime: time.Time{},
		hasPending:     false,
		initialized:    !options.initialModTime.IsZero(),
	}
}

func (w *configReloadWatcher) poll(ctx context.Context) {
	info, err := w.stat(w.path)
	if err != nil {
		w.log.WarnContext(ctx, "config watch stat failed; keeping current config", "err", err, "path", w.path)
		return
	}
	modTime := info.ModTime()
	if !w.initialized {
		w.appliedModTime = modTime
		w.initialized = true
		w.hasPending = false
		return
	}
	if modTime.Equal(w.appliedModTime) {
		w.hasPending = false
		return
	}
	if !w.hasPending || !modTime.Equal(w.pendingModTime) {
		w.pendingModTime = modTime
		w.hasPending = true
		return
	}
	cfg, err := w.load(w.path)
	if err != nil {
		w.log.ErrorContext(ctx, "config reload failed; keeping current config", "err", err, "path", w.path)
		w.appliedModTime = modTime
		w.hasPending = false
		return
	}
	if w.apply == nil {
		w.log.ErrorContext(ctx, "config reload failed; keeping current config", "err", "missing apply callback", "path", w.path)
		w.appliedModTime = modTime
		w.pendingModTime = time.Time{}
		w.hasPending = false
		return
	}
	if err := w.apply(ctx, cfg); err != nil {
		w.log.ErrorContext(ctx, "config reload apply failed; keeping current config", "err", err, "path", w.path)
		return
	}
	w.log.InfoContext(ctx, "config reloaded", "path", w.path, "mod_time", modTime)
	w.appliedModTime = modTime
	w.hasPending = false
}
