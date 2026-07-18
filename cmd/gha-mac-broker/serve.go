package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"syscall"

	"goodkind.io/gha-mac-broker/internal/broker"
	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/hostedload"
	"goodkind.io/gha-mac-broker/internal/server"
	"goodkind.io/gha-mac-broker/internal/tart"
)

// runServe runs the single-process daemon: it binds the listener directly and
// serves in one process under launchd KeepAlive, which relaunches it wholesale on
// death. It is the only serving role; there is no host supervisor or swappable
// worker.
func runServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	configPath := fs.String("config", "", "path to broker config TOML (default: XDG path)")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "serve flag parse failed", "err", err)
		return fmt.Errorf("serve flags: %w", err)
	}
	if *configPath == "" {
		*configPath = config.DefaultConfigPath()
	}
	return serveDaemon(ctx, *configPath)
}

// serveDaemon loads config, builds the pool and HTTP server, starts the fill and
// reconcile loops, and serves on its own bound listener until SIGINT or SIGTERM
// triggers a graceful shutdown.
func serveDaemon(ctx context.Context, configPath string) error {
	initialConfigModTime := configInitialModTime(ctx, configPath)

	cfg, gh, err := loadDeps(ctx, configPath)
	if err != nil {
		return err
	}

	secret, err := cfg.ReadWebhookSecret()
	if err != nil {
		slog.ErrorContext(ctx, "read webhook secret failed", "err", err)
		return fmt.Errorf("serve: read webhook secret: %w", err)
	}

	capacityToken, err := cfg.ReadCapacityToken()
	if err != nil {
		slog.ErrorContext(ctx, "read capacity token failed", "err", err)
		return fmt.Errorf("serve: read capacity token: %w", err)
	}

	webhookCIDRs, err := cfg.ReadWebhookCIDRs()
	if err != nil {
		slog.ErrorContext(ctx, "read webhook CIDRs failed", "err", err)
		return fmt.Errorf("serve: read webhook CIDRs: %w", err)
	}

	v := tart.New(cfg.Tart.Binary)
	binder := broker.New(cfg, gh, v)

	p, err := newRunnerPool(ctx, cfg, binder, gh)
	if err != nil {
		return err
	}
	hostedTracker := hostedload.NewTracker()
	sampler := newHostStatsSampler(cfg, p)
	srv := server.New(secret, cfg, capacityToken, webhookCIDRs, p, hostedTracker, sampler)

	listener, err := listenTCP(ctx, cfg.ListenAddr)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	startServeLoops(ctx, stop, cfg, gh, p)
	startBrokerConfigReloadWatcher(ctx, configPath, initialConfigModTime, binder, p, srv, sampler)
	startHostedLoadReconcile(ctx, gh, hostedTracker, cfg.Labels)
	sampler.Start(ctx)

	serveErr := httpServe(ctx, listener, srv, nil)

	shutCtx, shutCancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer shutCancel()
	p.Shutdown(shutCtx)
	return serveErr
}

// httpServe serves handler on an already-bound listener and shuts down gracefully
// on ctx cancel. It takes the listener as a seam rather than binding one itself, so
// a test can hand it an injected listener, and it sets no WriteTimeout because the
// /status stream is long-lived. onReady, when set, is called once the server is
// serving, so a caller can synchronize on readiness.
func httpServe(ctx context.Context, listener net.Listener, handler http.Handler, onReady func()) error {
	httpSrv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: httpTimeout,
		ReadTimeout:       httpTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "server goroutine panic recovered", "err", fmt.Errorf("panic: %v", r))
				errCh <- fmt.Errorf("serve: panic: %v", r)
			}
		}()
		slog.InfoContext(ctx, "server listening", "addr", listener.Addr().String())
		if err := httpSrv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.ErrorContext(ctx, "server error", "err", err)
			errCh <- fmt.Errorf("serve: listen: %w", err)
			return
		}
		errCh <- nil
	}()

	if onReady != nil {
		onReady()
	}

	select {
	case <-ctx.Done():
		slog.InfoContext(ctx, "shutting down", "reason", ctx.Err())
	case err := <-errCh:
		return err
	}

	shutCtx, shutCancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer shutCancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		slog.WarnContext(shutCtx, "http shutdown error", "err", err)
	}
	return nil
}

// listenTCP binds a TCP listener on addr for the serve daemon.
func listenTCP(ctx context.Context, addr string) (net.Listener, error) {
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", addr)
	if err != nil {
		slog.ErrorContext(ctx, "serve listen failed", "err", err, "addr", addr)
		return nil, fmt.Errorf("serve: listen %q: %w", addr, err)
	}
	return listener, nil
}
