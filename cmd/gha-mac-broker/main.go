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
//	install            scaffold config and secrets, build golden, install the service
//	uninstall          remove the installed service unit
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/fastpull"
	"goodkind.io/gha-mac-broker/internal/ghapp"
	"goodkind.io/gha-mac-broker/internal/golden"
	"goodkind.io/gha-mac-broker/internal/install"
	"goodkind.io/gha-mac-broker/internal/pool"
	"goodkind.io/gha-mac-broker/internal/reservation"
	"goodkind.io/gha-mac-broker/internal/server"
	"goodkind.io/gha-mac-broker/internal/skopeo"
	"goodkind.io/gha-mac-broker/internal/tart"
	"goodkind.io/gha-mac-broker/internal/version"
)

// commandName is the broker's top-level subcommand.
type commandName string

const (
	commandVersion     commandName = "version"
	commandJITConfig   commandName = "jitconfig"
	commandBind        commandName = "bind"
	commandServe       commandName = "serve"
	commandBuildGolden commandName = "build-golden"
	commandInstall     commandName = "install"
	commandUninstall   commandName = "uninstall"
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
	case commandBuildGolden:
		err = runBuildGolden(ctx, args)
	case commandInstall:
		err = runInstall(ctx, args)
	case commandUninstall:
		err = runUninstall(ctx, args)
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
	fmt.Fprintln(os.Stderr, "usage: gha-mac-broker <version|jitconfig|bind|serve|build-golden|install|uninstall> [flags]")
}

// runInstall performs the full host setup: it scaffolds the config and secrets,
// builds the golden image when missing, and installs the OS service unit.
func runInstall(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("install", flag.ExitOnError)
	configPath := fs.String("config", config.DefaultConfigPath(), "path to broker config TOML")
	exe, err := os.Executable()
	if err != nil {
		slog.ErrorContext(ctx, "resolve executable failed", "err", err)
		return fmt.Errorf("install: resolve executable: %w", err)
	}
	binPath := fs.String("bin", exe, "path to the installed broker binary")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "install flag parse failed", "err", err)
		return fmt.Errorf("install flags: %w", err)
	}
	cfg, err := buildInstallConfig(ctx, *configPath, *binPath)
	if err != nil {
		return err
	}
	if err := install.Install(ctx, cfg); err != nil {
		return fmt.Errorf("run install: %w", err)
	}
	return nil
}

// runUninstall removes the installed service unit, leaving config and secrets.
func runUninstall(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("uninstall", flag.ExitOnError)
	configPath := fs.String("config", config.DefaultConfigPath(), "path to broker config TOML")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "uninstall flag parse failed", "err", err)
		return fmt.Errorf("uninstall flags: %w", err)
	}
	exe, err := os.Executable()
	if err != nil {
		slog.ErrorContext(ctx, "resolve executable failed", "err", err)
		return fmt.Errorf("uninstall: resolve executable: %w", err)
	}
	cfg, err := buildInstallConfig(ctx, *configPath, exe)
	if err != nil {
		return err
	}
	if err := install.Uninstall(ctx, cfg); err != nil {
		return fmt.Errorf("run uninstall: %w", err)
	}
	return nil
}

// buildInstallConfig resolves the home directory and derives the install paths
// the installer renders into the service unit and scaffolds on disk.
func buildInstallConfig(ctx context.Context, configPath, binPath string) (install.Config, error) {
	var zero install.Config
	home, err := os.UserHomeDir()
	if err != nil {
		slog.ErrorContext(ctx, "resolve home dir failed", "err", err)
		return zero, fmt.Errorf("resolve home dir: %w", err)
	}
	return install.Config{
		BinPath:    binPath,
		Home:       home,
		ConfigDir:  filepath.Dir(configPath),
		LogPath:    defaultLogPath(home),
		ConfigPath: configPath,
	}, nil
}

// defaultLogPath returns the daemon log path: the macOS Logs directory on
// darwin, otherwise the XDG state directory.
func defaultLogPath(home string) string {
	if runtime.GOOS == "darwin" {
		return filepath.Join(home, "Library", "Logs", "gha-mac-broker.log")
	}
	return filepath.Join(home, ".local", "state", "gha-mac-broker", "gha-mac-broker.log")
}

// runBuildGolden builds and self-verifies the golden VM image from a Cirrus base
// over vsock. It needs no config (suitable for a bare host bootstrap): the
// runner version defaults to the latest actions/runner release.
func runBuildGolden(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("build-golden", flag.ExitOnError)
	tartBin := fs.String("tart", "tart", "tart binary")
	baseImage := fs.String("base-image", config.DefaultBaseImage, "Cirrus base image to clone (manual override; install reads tart.base_image from config)")
	goldenName := fs.String("golden", "gha-golden", "golden image name to (re)build")
	buildVM := fs.String("build-vm", "gha-golden-build", "scratch VM name used during the build")
	runnerVersion := fs.String("runner-version", "", "actions/runner version to install (default: latest)")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "build-golden flag parse failed", "err", err)
		return fmt.Errorf("build-golden flags: %w", err)
	}

	version := *runnerVersion
	if version == "" {
		resolved, err := resolveRunnerVersion(ctx)
		if err != nil {
			return err
		}
		version = resolved
	}

	cfg, err := config.Load(config.DefaultConfigPath())
	if err != nil {
		cfg = config.Default()
	}
	builder := golden.New(tart.New(*tartBin), golden.WithBaseStager(fastPullStager(cfg)))
	if err := builder.Build(ctx, golden.Options{
		BaseImage:     *baseImage,
		GoldenName:    *goldenName,
		BuildVM:       *buildVM,
		RunnerVersion: version,
	}); err != nil {
		return fmt.Errorf("build-golden: %w", err)
	}
	slog.InfoContext(ctx, "golden build complete", "golden", *goldenName)
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

// runnerRelease is the subset of the actions/runner latest-release API response
// the version resolver reads.
type runnerRelease struct {
	TagName string `json:"tag_name"`
}

// resolveRunnerVersion fetches the latest actions/runner release tag and returns
// it without the leading "v".
func resolveRunnerVersion(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/repos/actions/runner/releases/latest", nil)
	if err != nil {
		slog.ErrorContext(ctx, "build request failed", "err", err)
		return "", fmt.Errorf("build-golden: build request: %w", err)
	}
	client := &http.Client{Timeout: httpTimeout}
	resp, err := client.Do(req)
	if err != nil {
		slog.ErrorContext(ctx, "resolve runner version request failed", "err", err)
		return "", fmt.Errorf("build-golden: fetch latest runner: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("build-golden: latest runner status %d", resp.StatusCode)
	}
	var rel runnerRelease
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		slog.ErrorContext(ctx, "decode runner release failed", "err", err)
		return "", fmt.Errorf("build-golden: decode runner release: %w", err)
	}
	return strings.TrimPrefix(rel.TagName, "v"), nil
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
	p := pool.New(cfg.Tart.WarmBudget, cfg.Tart.GoldenBudget, binder, runToken)
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
