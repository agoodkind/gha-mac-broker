// Command gha-mac-broker is a generic GitHub Actions macOS warm-pool JIT runner
// broker. It owns a pool of pre-warmed Tart VMs and binds a free VM to whichever
// repository just queued a job, using repo-scoped just-in-time runner config so
// one shared pool serves many personal-account repositories.
//
// Subcommands:
//
//	version            print version and exit
//	jitconfig          mint a repo-scoped JIT runner config (proves App auth)
//	bind               clone a warm VM, run one ephemeral job, tear it down
//	serve              run the HTTP daemon with warm pool and webhook handler
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/ghapp"
	"goodkind.io/gha-mac-broker/internal/pool"
	"goodkind.io/gha-mac-broker/internal/reservation"
	"goodkind.io/gha-mac-broker/internal/server"
	"goodkind.io/gha-mac-broker/internal/tart"
	"goodkind.io/gha-mac-broker/internal/version"
)

// commandName is the broker's top-level subcommand.
type commandName string

const (
	commandVersion   commandName = "version"
	commandJITConfig commandName = "jitconfig"
	commandBind      commandName = "bind"
	commandServe     commandName = "serve"
)

// httpTimeout bounds GitHub API calls.
const httpTimeout = 30 * time.Second

// shutdownTimeout bounds the graceful HTTP shutdown.
const shutdownTimeout = 30 * time.Second

// webhookWriteTimeout caps the total per-connection handler time. A Lease
// call can block until a warming VM is ready (up to 90 s per broker.Warm);
// 120 s covers one full boot cycle plus processing headroom so a stuck lease
// cannot pin a connection open indefinitely.
const webhookWriteTimeout = 120 * time.Second

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx := context.Background()
	slog.LogAttrs(ctx, slog.LevelInfo, "gha-mac-broker invocation", version.Attrs()...)

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	args := os.Args[2:]

	var err error
	switch commandName(os.Args[1]) {
	case commandVersion:
		fmt.Printf("gha-mac-broker %s (%s)\n", version.Version, version.Commit)
	case commandJITConfig:
		err = runJITConfig(ctx, args)
	case commandBind:
		err = runBind(ctx, args)
	case commandServe:
		err = runServe(ctx, args)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		slog.ErrorContext(ctx, "command failed", "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: gha-mac-broker <version|jitconfig|bind|serve> [flags]")
}

// loadDeps loads config and builds the GitHub App client shared by subcommands.
func loadDeps(ctx context.Context, configPath string) (*config.Config, *ghapp.Client, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		slog.ErrorContext(ctx, "load config failed", "err", err)
		return nil, nil, fmt.Errorf("load config: %w", err)
	}
	pemKey, err := cfg.ReadPrivateKey()
	if err != nil {
		slog.ErrorContext(ctx, "read private key failed", "err", err)
		return nil, nil, fmt.Errorf("read private key: %w", err)
	}
	httpClient := &http.Client{Timeout: httpTimeout}
	gh, err := ghapp.New(cfg.App.AppID, pemKey, ghapp.WithHTTPClient(httpClient))
	if err != nil {
		slog.ErrorContext(ctx, "init github app failed", "err", err)
		return nil, nil, fmt.Errorf("init github app: %w", err)
	}
	return cfg, gh, nil
}

// runJITConfig mints a just-in-time runner config for one repository. It
// exercises the full GitHub side end to end (App JWT, installation token,
// generate-jitconfig) without needing a VM, so the App credentials and
// permissions can be verified against live GitHub before the pool exists.
func runJITConfig(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("jitconfig", flag.ExitOnError)
	configPath := fs.String("config", "", "path to broker config TOML (default: XDG path)")
	repo := fs.String("repo", "", "target repository as owner/repo")
	name := fs.String("name", "gha-mac-broker-probe", "runner name to register")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "jitconfig flag parse failed", "err", err)
		return fmt.Errorf("jitconfig flags: %w", err)
	}
	if *configPath == "" {
		*configPath = config.DefaultConfigPath()
	}
	if *repo == "" {
		return fmt.Errorf("jitconfig requires -repo")
	}
	owner, repoName, ok := strings.Cut(*repo, "/")
	if !ok {
		return fmt.Errorf("repo must be owner/repo, got %q", *repo)
	}

	cfg, gh, err := loadDeps(ctx, *configPath)
	if err != nil {
		return err
	}
	if !cfg.RepoAllowed(*repo) {
		return fmt.Errorf("repo %s is not in allowed_repos", *repo)
	}

	installationID, err := gh.InstallationID(ctx, owner, repoName)
	if err != nil {
		return fmt.Errorf("installation lookup: %w", err)
	}
	token, err := gh.InstallationToken(ctx, installationID, repoName)
	if err != nil {
		return fmt.Errorf("installation token: %w", err)
	}
	jit, err := gh.GenerateJITConfig(ctx, token, owner, repoName, *name, cfg.Labels)
	if err != nil {
		return fmt.Errorf("generate jitconfig: %w", err)
	}

	fmt.Printf("installation: %d\n", installationID)
	fmt.Printf("runner:       %s (id %d)\n", jit.Runner.Name, jit.Runner.ID)
	fmt.Printf("labels:       %s\n", strings.Join(cfg.Labels, ", "))
	fmt.Printf("encoded_jit_config:\n%s\n", jit.EncodedJITConfig)
	return nil
}

// runBind performs one full VM bind: clone, boot, register, run one job, tear
// down. It is the manual trigger that proves the end-to-end path once the
// golden image exists.
func runBind(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("bind", flag.ExitOnError)
	configPath := fs.String("config", "", "path to broker config TOML (default: XDG path)")
	repo := fs.String("repo", "", "target repository as owner/repo")
	id := fs.String("id", "", "unique id for the VM and runner name (default: timestamp)")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "bind flag parse failed", "err", err)
		return fmt.Errorf("bind flags: %w", err)
	}
	if *configPath == "" {
		*configPath = config.DefaultConfigPath()
	}
	if *repo == "" {
		return fmt.Errorf("bind requires -repo")
	}

	cfg, gh, err := loadDeps(ctx, *configPath)
	if err != nil {
		return err
	}
	vm := tart.New(cfg.Tart.Binary)
	binder := broker.New(cfg, gh, vm)

	bindID := *id
	if bindID == "" {
		bindID = strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	if err := binder.BindOnce(ctx, *repo, bindID); err != nil {
		return fmt.Errorf("bind: %w", err)
	}
	return nil
}

// runServe loads config, builds the pool, reservation store, and HTTP server,
// starts the fill loop, and listens until SIGINT or SIGTERM triggers a
// graceful shutdown.
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

	cfg, gh, err := loadDeps(ctx, *configPath)
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

	// runToken is embedded in every VM name so names stay readable yet never
	// repeat across restarts or collide between overlapping processes. It pairs
	// a compact timestamp (readable, sortable) with random entropy so two
	// processes that start within the same second still get distinct names.
	// Generated here at the main boundary where time.Now is permitted.
	var entropy [3]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		slog.ErrorContext(ctx, "generate run token entropy failed", "err", err)
		return fmt.Errorf("serve: generate run token entropy: %w", err)
	}
	runToken := time.Now().Format("060102T150405") + "-" + hex.EncodeToString(entropy[:])
	p := pool.New(cfg.PoolSize, binder, runToken)
	store := reservation.New()
	srv := server.New(secret, cfg, capacityToken, webhookCIDRs, p, store, binder)

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	p.Start(ctx)

	httpSrv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv,
		ReadHeaderTimeout: httpTimeout,
		ReadTimeout:       httpTimeout,
		WriteTimeout:      webhookWriteTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "server goroutine panic recovered", "err", fmt.Errorf("panic: %v", r))
				errCh <- fmt.Errorf("serve: panic: %v", r)
			}
		}()
		slog.InfoContext(ctx, "server listening", "addr", cfg.ListenAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.ErrorContext(ctx, "server error", "err", err)
			errCh <- fmt.Errorf("serve: listen: %w", err)
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		slog.InfoContext(ctx, "shutting down", "reason", ctx.Err())
	case err := <-errCh:
		if err != nil {
			return err
		}
		return nil
	}

	shutCtx, shutCancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer shutCancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		slog.WarnContext(shutCtx, "http shutdown error", "err", err)
	}
	p.Shutdown(shutCtx)
	return nil
}
