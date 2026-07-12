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
//	serve              run the single-process HTTP daemon (no supervisor)
//	supervisor         run the durable host supervisor owning the listener and worker lifecycle
//	worker             run one swappable host worker generation (supervisor spawns it)
//	status             print the daemon pool status JSON
//	install            scaffold config and secrets, build golden, install the service
//	uninstall          remove the installed service unit
//	update             check, apply, or show release update state
//	deploy             pick and apply the least-destructive reconcile action
//	guest-agent        alias that runs the guest supervisor
//	guest-supervisor   run the durable guest-side supervisor of runner processes
//	guest-worker       run one swappable guest-worker generation (supervisor spawns it)
//	golden-provision   provision a golden build VM from inside it (host-invoked)
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/fastpull"
	"goodkind.io/gha-mac-broker/internal/ghapp"
	"goodkind.io/gha-mac-broker/internal/golden"
	"goodkind.io/gha-mac-broker/internal/hostedload"
	"goodkind.io/gha-mac-broker/internal/install"
	"goodkind.io/gha-mac-broker/internal/runnerpool"
	"goodkind.io/gha-mac-broker/internal/server"
	"goodkind.io/gha-mac-broker/internal/skopeo"
	"goodkind.io/gha-mac-broker/internal/tart"
	"goodkind.io/gha-mac-broker/internal/updateopts"
	"goodkind.io/gha-mac-broker/internal/version"
	"goodkind.io/go-makefile/selfupdate"
)

// commandName is the broker's top-level subcommand.
type commandName string

const (
	commandVersion     commandName = "version"
	commandJITConfig   commandName = "jitconfig"
	commandBind        commandName = "bind"
	commandServe       commandName = "serve"
	commandSupervisor  commandName = "supervisor"
	commandWorker      commandName = "worker"
	commandStatus      commandName = "status"
	commandBuildGolden commandName = "build-golden"
	commandInstall     commandName = "install"
	commandUninstall   commandName = "uninstall"
	commandUpdate      commandName = "update"
	commandDeploy      commandName = "deploy"
	commandGuestAgent  commandName = "guest-agent"
	commandGuestSuper  commandName = "guest-supervisor"
	commandGuestWorker commandName = "guest-worker"
	commandGoldenProv  commandName = "golden-provision"

	brokerBinaryName = "gha-mac-broker"
)

type updateCommandName string

const (
	updateCommandApply  updateCommandName = "apply"
	updateCommandCheck  updateCommandName = "check"
	updateCommandStatus updateCommandName = "status"
)

// httpTimeout bounds GitHub API calls.
const httpTimeout = 30 * time.Second

// shutdownTimeout bounds the graceful HTTP shutdown.
const shutdownTimeout = 30 * time.Second

var (
	checkUpdate            = selfupdate.Check
	applyUpdate            = selfupdate.Apply
	loadUpdateState        = selfupdate.LoadState
	currentExecutable      = os.Executable
	installBroker          = install.Install
	restartManagedService  = install.Restart
	runSelfUpdateScheduler = selfupdate.RunScheduler
	statusHTTPClient       = &http.Client{Timeout: httpTimeout}
)

func main() {
	setupLogging()
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
	case commandSupervisor:
		err = runSupervisor(ctx, args)
	case commandWorker:
		err = runWorker(ctx, args)
	case commandStatus:
		err = runStatus(ctx, args)
	case commandBuildGolden:
		err = runBuildGolden(ctx, args)
	case commandInstall:
		err = runInstall(ctx, args)
	case commandUninstall:
		err = runUninstall(ctx, args)
	case commandUpdate:
		err = runUpdate(ctx, args)
	case commandDeploy:
		err = runDeploy(ctx, args)
	case commandGuestAgent:
		err = runGuestAgent(ctx, args)
	case commandGuestSuper:
		err = runGuestSupervisor(ctx, args)
	case commandGuestWorker:
		err = runGuestWorker(ctx, args)
	case commandGoldenProv:
		err = runGoldenProvision(ctx, args)
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
	fmt.Fprintln(os.Stderr, "usage: gha-mac-broker <version|jitconfig|bind|serve|supervisor|status|build-golden|install|uninstall|update|deploy|guest-agent|guest-supervisor|guest-worker|golden-provision> [flags]")
}

func writeUserLine(writer io.Writer, line string) {
	_, _ = io.WriteString(writer, line+"\n")
}

func runStatus(ctx context.Context, args []string) error {
	return runStatusWithWriters(ctx, args, os.Stdout, os.Stderr)
}

func runStatusWithWriters(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "path to broker config TOML (default: XDG path)")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "status flag parse failed", "err", err)
		return fmt.Errorf("status flags: %w", err)
	}
	if *configPath == "" {
		*configPath = config.DefaultConfigPath()
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("status: load config: %w", err)
	}
	capacityToken, err := cfg.ReadCapacityToken()
	if err != nil {
		return fmt.Errorf("status: read capacity token: %w", err)
	}
	if len(capacityToken) == 0 {
		return fmt.Errorf("status: capacity token is not configured")
	}
	statusURL, err := statusEndpoint(ctx, cfg.ListenAddr)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
	if err != nil {
		return fmt.Errorf("status: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+string(capacityToken))
	resp, err := statusHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("status: request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		message := strings.TrimSpace(string(body))
		if message == "" {
			return fmt.Errorf("status: server returned %d", resp.StatusCode)
		}
		return fmt.Errorf("status: server returned %d: %s", resp.StatusCode, message)
	}
	if _, err := io.Copy(stdout, resp.Body); err != nil {
		return fmt.Errorf("status: copy response: %w", err)
	}
	return nil
}

func statusEndpoint(ctx context.Context, listenAddr string) (string, error) {
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		slog.ErrorContext(ctx, "status listen address parse failed", "err", err, "listen_addr", listenAddr)
		return "", fmt.Errorf("status: parse listen_addr %q: %w", listenAddr, err)
	}
	// Dial localhost so the CLI reaches the daemon whether it bound IPv6 or IPv4
	// loopback. On macOS localhost resolves to IPv6 first, matching the daemon's
	// default ::1 bind.
	return "http://" + net.JoinHostPort("localhost", port) + "/status", nil
}

func runUpdate(ctx context.Context, args []string) error {
	return runUpdateWithWriters(ctx, args, os.Stdout, os.Stderr)
}

func runUpdateWithWriters(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: gha-mac-broker update check|apply|status")
		return fmt.Errorf("update requires check, apply, or status")
	}
	switch updateCommandName(args[0]) {
	case updateCommandCheck:
		return runUpdateCheck(ctx, args[1:], stdout, stderr)
	case updateCommandApply:
		return runUpdateApply(ctx, args[1:], stdout, stderr)
	case updateCommandStatus:
		return runUpdateStatus(ctx, args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "gha-mac-broker update: unknown subcommand %q\n", args[0])
		return fmt.Errorf("unknown update subcommand %q", args[0])
	}
}

func runUpdateCheck(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("update check", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "update check flag parse failed", "err", err)
		return fmt.Errorf("update check flags: %w", err)
	}
	result, err := checkUpdate(ctx, updateopts.Options(updateopts.Overrides{
		Client:      nil,
		InstallPath: "",
		DryRun:      false,
		Log:         nil,
	}))
	if err != nil {
		slog.ErrorContext(ctx, "update check failed", "err", err)
		return fmt.Errorf("update check: %w", err)
	}
	printUpdateCheckResult(stdout, result)
	return nil
}

func runUpdateApply(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("update apply", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dryRun := fs.Bool("dry-run", false, "download and verify without installing")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "update apply flag parse failed", "err", err)
		return fmt.Errorf("update apply flags: %w", err)
	}
	result, err := applyUpdate(ctx, updateopts.Options(updateopts.Overrides{
		Client:      nil,
		InstallPath: "",
		DryRun:      *dryRun,
		Log:         nil,
	}))
	if err != nil {
		slog.ErrorContext(ctx, "update apply failed", "err", err)
		return fmt.Errorf("update apply: %w", err)
	}
	if !result.UpdateAvailable {
		writeUserLine(stdout, "gha-mac-broker: already current")
		return nil
	}
	if result.DryRun {
		writeUserLine(stdout, "gha-mac-broker: update apply dry run ok")
		return nil
	}
	if !result.Applied {
		writeUserLine(stdout, "gha-mac-broker: update available but not applied")
		return nil
	}
	restarted, err := reloadOrRestart(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "update apply service reconcile failed", "err", err)
		return fmt.Errorf("update apply restart: %w", err)
	}
	if restarted {
		writeUserLine(stdout, "gha-mac-broker: update applied and service restarted")
		return nil
	}
	writeUserLine(stdout, "gha-mac-broker: update applied; service not installed")
	return nil
}

func runUpdateStatus(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("update status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "update status flag parse failed", "err", err)
		return fmt.Errorf("update status flags: %w", err)
	}
	options := updateopts.Options(updateopts.Overrides{
		Client:      nil,
		InstallPath: "",
		DryRun:      false,
		Log:         nil,
	})
	state, err := loadUpdateState(options.StatePath)
	if err != nil {
		slog.ErrorContext(ctx, "update status load failed", "err", err, "path", options.StatePath)
		return fmt.Errorf("update status: %w", err)
	}
	writeUserLine(stdout, "current version:   "+options.Config.CurrentVersion)
	writeUserLine(stdout, "current commit:    "+options.Config.CurrentCommit)
	writeUserLine(stdout, "current buildHash: "+options.Config.CurrentBuildHash)
	if !state.LastCheckAt.IsZero() {
		writeUserLine(stdout, "last check:        "+state.LastCheckAt.Format(time.RFC3339))
	}
	if !state.NextCheckAt.IsZero() {
		writeUserLine(stdout, "next check:        "+state.NextCheckAt.Format(time.RFC3339))
	}
	if state.LatestTag != "" {
		writeUserLine(stdout, "latest tag:        "+state.LatestTag)
	}
	if state.AppliedTag != "" {
		writeUserLine(stdout, "applied tag:       "+state.AppliedTag)
	}
	if state.LastResult != "" {
		writeUserLine(stdout, "last result:       "+state.LastResult)
	}
	if state.LastError != "" {
		writeUserLine(stdout, "last error:        "+state.LastError)
	}
	return nil
}

func printUpdateCheckResult(stdout io.Writer, result selfupdate.CheckResult) {
	writeUserLine(stdout, "current version: "+result.CurrentVersion)
	writeUserLine(stdout, "latest tag:      "+result.LatestTag)
	writeUserLine(stdout, "asset:           "+result.AssetName)
	if result.UpdateAvailable {
		writeUserLine(stdout, "update available: yes")
		return
	}
	writeUserLine(stdout, "update available: no")
}

// runInstall performs the full host setup: it scaffolds the config and secrets,
// builds the golden image when missing, and installs the OS service unit.
func runInstall(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("install", flag.ExitOnError)
	configPath := fs.String("config", config.DefaultConfigPath(), "path to broker config TOML")
	binPath := fs.String("bin", "", "path to the installed broker binary (default $HOME/.local/bin/gha-mac-broker)")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "install flag parse failed", "err", err)
		return fmt.Errorf("install flags: %w", err)
	}
	if *binPath == "" {
		defaultBinPath, err := defaultInstallBinPath(ctx)
		if err != nil {
			return err
		}
		*binPath = defaultBinPath
	}
	cfg, err := buildInstallConfig(ctx, *configPath, *binPath)
	if err != nil {
		return err
	}
	exe, err := currentExecutable()
	if err != nil {
		slog.ErrorContext(ctx, "resolve executable failed", "err", err)
		return fmt.Errorf("install: resolve executable: %w", err)
	}
	if err := installRunningBinary(ctx, exe, cfg.BinPath); err != nil {
		return err
	}
	if err := installBroker(ctx, cfg); err != nil {
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
	exe, err := currentExecutable()
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
		Maintenance: config.MaintenanceConfig{
			Command:         "",
			IntervalSeconds: 0,
		},
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
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("build-golden: load config %s: %w", config.DefaultConfigPath(), err)
		}
		cfg = config.Default()
	}
	builder := golden.New(tart.New(*tartBin), golden.WithBaseStager(fastPullStager(cfg)))
	if err := builder.Build(ctx, golden.Options{
		BaseImage:     *baseImage,
		GoldenName:    *goldenName,
		BuildVM:       *buildVM,
		RunnerVersion: version,
		BinaryPath:    "",
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
		Copier: skopeo.New("skopeo", cfg.Tart.FastPullParallel),
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

type staleRunnerCleaner interface {
	ListInstalledRepos(ctx context.Context) ([]string, error)
	ListRunners(ctx context.Context, repo string) ([]ghapp.Runner, error)
	DeleteRunner(ctx context.Context, repo string, runnerID int64) error
}

func deleteStaleRunners(ctx context.Context, cfg *config.Config, cleaner staleRunnerCleaner) {
	totalDeleted := 0
	runnerNamePrefix := cfg.Tart.VMNamePrefix
	repos, err := cleaner.ListInstalledRepos(ctx)
	if err != nil {
		slog.WarnContext(ctx, "installed repository list failed; continuing startup", "err", err)
		return
	}
	for _, repo := range repos {
		runners, err := cleaner.ListRunners(ctx, repo)
		if err != nil {
			slog.WarnContext(ctx, "stale runner list failed; continuing startup", "err", err, "repo", repo)
			continue
		}
		for _, runner := range runners {
			if !strings.EqualFold(runner.Status, "offline") {
				continue
			}
			if !strings.HasPrefix(runner.Name, runnerNamePrefix) {
				continue
			}
			if err := cleaner.DeleteRunner(ctx, repo, runner.ID); err != nil {
				slog.WarnContext(ctx, "stale runner delete failed; continuing startup", "err", err, "repo", repo, "runner", runner.Name, "runner_id", runner.ID)
				continue
			}
			totalDeleted++
		}
	}
	if totalDeleted > 0 {
		slog.InfoContext(ctx, "stale offline runners deleted", "count", totalDeleted)
	}
}

type runnerPoolBinder interface {
	runnerpool.Warmer
	runnerpool.Runner
	runnerpool.SlotProber
}

func newRunnerPool(ctx context.Context, cfg *config.Config, binder runnerPoolBinder, github runnerpool.RunnerLister) (*runnerpool.Pool, error) {
	runToken, err := newRunToken(ctx)
	if err != nil {
		return nil, err
	}
	return runnerpool.New(runnerPoolOptionsFromConfig(cfg, runToken, time.Now), binder, binder, github, binder), nil
}

func runnerPoolOptionsFromConfig(cfg *config.Config, runToken string, now func() time.Time) runnerpool.Options {
	return runnerpool.Options{
		RunnerCount:    cfg.RunnerCount,
		JobsPerVM:      cfg.JobsPerVM,
		Image:          cfg.Tart.BaseImage,
		MaxIdle:        time.Duration(cfg.MaxIdle),
		MaxAge:         time.Duration(cfg.MaxAge),
		MaxBind:        time.Duration(cfg.MaxBind),
		PickupTimeout:  time.Duration(cfg.PickupTimeout),
		RunToken:       runToken,
		WarmRetryDelay: 0,
		Now:            now,
	}
}

func configInitialModTime(ctx context.Context, path string) time.Time {
	info, err := os.Stat(path)
	if err != nil {
		slog.WarnContext(ctx, "config watch initial stat failed; watcher will initialize on first poll", "err", err, "path", path)
		return time.Time{}
	}
	return info.ModTime()
}

func newRunToken(ctx context.Context) (string, error) {
	var entropy [3]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		slog.ErrorContext(ctx, "generate run token entropy failed", "err", err)
		return "", fmt.Errorf("serve: generate run token entropy: %w", err)
	}
	return time.Now().Format("060102T150405") + "-" + hex.EncodeToString(entropy[:]), nil
}

func startServeLoops(ctx context.Context, stop func(), cfg *config.Config, cleaner staleRunnerCleaner, p *runnerpool.Pool, onReconcile func()) {
	startUpdateSchedulerInBackground(ctx, stop, slog.Default())
	deleteStaleRunners(ctx, cfg, cleaner)
	p.Start(ctx)
	p.StartReconcile(ctx, 0, onReconcile)
}

// hostedLoadReconcileInterval is how often the in-progress hosted-macOS job
// set is rebuilt from the GitHub API to correct webhook-counter drift after a
// broker restart or a missed webhook delivery.
const hostedLoadReconcileInterval = 5 * time.Minute

type hostedJobLister interface {
	ListInProgressHostedMacOSJobs(ctx context.Context, poolLabels []string) (map[int64]struct{}, error)
}

func startHostedLoadReconcile(ctx context.Context, lister hostedJobLister, tracker *hostedload.Tracker, poolLabels []string) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(ctx, "hosted load reconcile goroutine panic recovered", "err", recovered)
			}
		}()
		reconcileHostedLoadOnce(ctx, lister, tracker, poolLabels)
		ticker := time.NewTicker(hostedLoadReconcileInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reconcileHostedLoadOnce(ctx, lister, tracker, poolLabels)
			}
		}
	}()
}

func reconcileHostedLoadOnce(ctx context.Context, lister hostedJobLister, tracker *hostedload.Tracker, poolLabels []string) {
	jobs, err := lister.ListInProgressHostedMacOSJobs(ctx, poolLabels)
	if err != nil {
		slog.WarnContext(ctx, "hosted load reconcile failed; keeping live counter", "err", err)
		return
	}
	tracker.Reconcile(jobs)
	slog.DebugContext(ctx, "hosted load reconciled", "in_progress_hosted_macos", len(jobs))
}

func startUpdateSchedulerInBackground(ctx context.Context, stop func(), log *slog.Logger) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.ErrorContext(ctx, "update scheduler goroutine panic recovered", "err", recovered)
			}
		}()
		startUpdateScheduler(ctx, stop, log)
	}()
}

func startUpdateScheduler(ctx context.Context, stop func(), log *slog.Logger) {
	if stop == nil {
		return
	}
	runSelfUpdateScheduler(ctx, selfupdate.SchedulerHooks{
		Enabled: func() bool {
			return true
		},
		Mode: func() string {
			return selfupdate.ModeApply
		},
		Options: func() selfupdate.Options {
			return updateopts.Options(updateopts.Overrides{
				Client:      nil,
				InstallPath: "",
				DryRun:      false,
				Log:         log,
			})
		},
		StopForRelaunch: stop,
		Log:             log,
	})
}
