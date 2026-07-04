package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/ghapp"
	"goodkind.io/go-makefile/selfupdate"
)

type staleRunnerClient struct {
	runners []ghapp.Runner
	listErr error
	deleted []int64
}

func (c *staleRunnerClient) ListRunners(_ context.Context, _ string) ([]ghapp.Runner, error) {
	if c.listErr != nil {
		return nil, c.listErr
	}
	return append([]ghapp.Runner(nil), c.runners...), nil
}

func (c *staleRunnerClient) DeleteRunner(_ context.Context, _ string, runnerID int64) error {
	c.deleted = append(c.deleted, runnerID)
	return nil
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func testServeConfig() *config.Config {
	return &config.Config{
		ListenAddr: ":8080",
		App: config.AppConfig{
			AppID:             "1",
			PrivateKeyPath:    "/tmp/key",
			WebhookSecretPath: "/tmp/secret",
			CapacityTokenPath: "/tmp/capacity",
			WebhookCIDRsPath:  "",
		},
		Tart: config.TartConfig{
			Binary:           "tart",
			GoldenImage:      "",
			BaseImage:        config.DefaultBaseImage,
			WarmBudget:       2,
			GoldenBudget:     3,
			Images:           []config.ImageMapping{{MacOS: "tahoe", Xcode: "26.5", Tag: config.DefaultBaseImage}},
			VMNamePrefix:     "gha",
			CacheDir:         "",
			FastPull:         nil,
			FastPullDir:      "",
			FastPullParallel: 16,
		},
		Labels:       []string{"self-hosted", "macOS"},
		AllowedRepos: []string{"owner/repo"},
	}
}

func TestRunStatusUsesCapacityTokenAndListenPort(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "capacity-token")
	if err := os.WriteFile(tokenPath, []byte("status-token\n"), 0o600); err != nil {
		t.Fatalf("write capacity token: %v", err)
	}
	configPath := filepath.Join(dir, "config.toml")
	configBody := fmt.Sprintf(`
listen_addr = "[::1]:23456"
runner_count = 1
labels = ["self-hosted"]
allowed_repos = ["owner/repo"]

[app]
app_id = "1"
private_key_path = "/tmp/private-key.pem"
webhook_secret_path = "/tmp/webhook-secret"
capacity_token_path = %q

[tart]
base_image = %q
`, tokenPath, config.DefaultBaseImage)
	if err := os.WriteFile(configPath, []byte(configBody), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	var capturedRequest *http.Request
	oldClient := statusHTTPClient
	statusHTTPClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			capturedRequest = req
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"snapshot":{"ready":true},"workers":[]}`)),
			}, nil
		}),
	}
	t.Cleanup(func() {
		statusHTTPClient = oldClient
	})

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runStatusWithWriters(context.Background(), []string{"-config", configPath}, &stdout, &stderr); err != nil {
		t.Fatalf("runStatusWithWriters: %v\nstderr=%s", err, stderr.String())
	}
	if capturedRequest == nil {
		t.Fatal("status request was not sent")
	}
	if capturedRequest.URL.String() != "http://localhost:23456/status" {
		t.Fatalf("status url = %q, want http://localhost:23456/status", capturedRequest.URL.String())
	}
	if capturedRequest.Header.Get("Authorization") != "Bearer status-token" {
		t.Fatalf("authorization = %q, want bearer token", capturedRequest.Header.Get("Authorization"))
	}
	if !strings.Contains(stdout.String(), `"workers":[]`) {
		t.Fatalf("stdout = %q, want status JSON", stdout.String())
	}
}

func TestStatusEndpointUsesLocalhost(t *testing.T) {
	for _, listenAddr := range []string{"[::1]:23456", "127.0.0.1:23456", "0.0.0.0:23456"} {
		statusURL, err := statusEndpoint(context.Background(), listenAddr)
		if err != nil {
			t.Fatalf("statusEndpoint(%q): %v", listenAddr, err)
		}
		if statusURL != "http://localhost:23456/status" {
			t.Fatalf("status url for %q = %q, want http://localhost:23456/status", listenAddr, statusURL)
		}
	}
}

func TestDeleteStaleRunnersDeletesOfflineRunners(t *testing.T) {
	client := &staleRunnerClient{
		runners: []ghapp.Runner{
			{ID: 11, Name: "gha-old", Status: "offline", Busy: false},
			{ID: 12, Name: "gha-new", Status: "online", Busy: true},
		},
		listErr: nil,
		deleted: nil,
	}

	deleteStaleRunners(context.Background(), testServeConfig(), client)

	if len(client.deleted) != 1 {
		t.Fatalf("deleted runners = %v, want [11]", client.deleted)
	}
	if client.deleted[0] != 11 {
		t.Fatalf("deleted runner = %d, want 11", client.deleted[0])
	}
}

func TestDeleteStaleRunnersOnlyDeletesOfflineRunnersWithConfiguredPrefix(t *testing.T) {
	cfg := testServeConfig()
	cfg.Tart.VMNamePrefix = "broker-managed"
	client := &staleRunnerClient{
		runners: []ghapp.Runner{
			{ID: 11, Name: "broker-managed-old", Status: "offline", Busy: false},
			{ID: 12, Name: "external-runner", Status: "offline", Busy: false},
			{ID: 13, Name: "broker-managed-new", Status: "online", Busy: true},
		},
		listErr: nil,
		deleted: nil,
	}

	deleteStaleRunners(context.Background(), cfg, client)

	if len(client.deleted) != 1 {
		t.Fatalf("deleted runners = %v, want [11]", client.deleted)
	}
	if client.deleted[0] != 11 {
		t.Fatalf("deleted runner = %d, want 11", client.deleted[0])
	}
}

func TestDeleteStaleRunnersListErrorDoesNotBlockStartup(t *testing.T) {
	client := &staleRunnerClient{
		runners: nil,
		listErr: errors.New("github unavailable"),
		deleted: nil,
	}

	deleteStaleRunners(context.Background(), testServeConfig(), client)

	if len(client.deleted) != 0 {
		t.Fatalf("deleted runners after list error = %v, want none", client.deleted)
	}
}

func TestRunUpdateApplyRestartsServiceAfterAppliedUpdate(t *testing.T) {
	var capturedOptions selfupdate.Options
	restarted := false
	withUpdateTestHooks(t, updateTestHooks{
		apply: func(_ context.Context, options selfupdate.Options) (selfupdate.ApplyResult, error) {
			capturedOptions = options
			return selfupdate.ApplyResult{
				CheckResult: selfupdate.CheckResult{
					CurrentVersion:  "202607030215-16-122a5cc",
					LatestTag:       "202607030301-b-1d33d4f",
					AssetName:       "gha-mac-broker_darwin_arm64.tar.gz",
					UpdateAvailable: true,
				},
				Applied: true,
				DryRun:  false,
			}, nil
		},
		restart: func(_ context.Context) (bool, error) {
			restarted = true
			return true, nil
		},
	})
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := runUpdateWithWriters(context.Background(), []string{"apply"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runUpdateWithWriters: %v\nstderr=%s", err, stderr.String())
	}
	if !restarted {
		t.Fatal("service restart was not triggered")
	}
	if capturedOptions.Config.ValidateMatch != "gha-mac-broker " {
		t.Fatalf("validate match = %q, want gha-mac-broker ", capturedOptions.Config.ValidateMatch)
	}
	if !strings.Contains(stdout.String(), "gha-mac-broker: update applied and service restarted") {
		t.Fatalf("stdout = %q, want applied restart message", stdout.String())
	}
}

func TestRunUpdateApplyDryRunDoesNotRestartService(t *testing.T) {
	restarted := false
	withUpdateTestHooks(t, updateTestHooks{
		apply: func(_ context.Context, options selfupdate.Options) (selfupdate.ApplyResult, error) {
			if !options.DryRun {
				t.Fatal("dry-run option was not passed to selfupdate.Apply")
			}
			return selfupdate.ApplyResult{
				CheckResult: selfupdate.CheckResult{
					CurrentVersion:  "202607030215-16-122a5cc",
					LatestTag:       "202607030301-b-1d33d4f",
					AssetName:       "gha-mac-broker_darwin_arm64.tar.gz",
					UpdateAvailable: true,
				},
				Applied: false,
				DryRun:  true,
			}, nil
		},
		restart: func(_ context.Context) (bool, error) {
			restarted = true
			return true, nil
		},
	})
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := runUpdateWithWriters(context.Background(), []string{"apply", "-dry-run"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runUpdateWithWriters: %v\nstderr=%s", err, stderr.String())
	}
	if restarted {
		t.Fatal("service restart was triggered for a dry run")
	}
	if !strings.Contains(stdout.String(), "gha-mac-broker: update apply dry run ok") {
		t.Fatalf("stdout = %q, want dry-run message", stdout.String())
	}
}

func TestStartUpdateSchedulerStopsServeContextForRelaunch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopCalled := false
	var capturedHooks selfupdate.SchedulerHooks
	withUpdateTestHooks(t, updateTestHooks{
		runScheduler: func(_ context.Context, hooks selfupdate.SchedulerHooks) {
			capturedHooks = hooks
		},
	})

	startUpdateScheduler(ctx, func() {
		stopCalled = true
		cancel()
	}, slog.Default())

	if capturedHooks.Enabled == nil || !capturedHooks.Enabled() {
		t.Fatal("scheduler enabled hook is missing or false")
	}
	if capturedHooks.Mode == nil || capturedHooks.Mode() != selfupdate.ModeApply {
		t.Fatalf("scheduler mode = %q, want apply", capturedHooks.Mode())
	}
	if capturedHooks.Options == nil {
		t.Fatal("scheduler options hook is missing")
	}
	if capturedHooks.Options().Config.ValidateMatch != "gha-mac-broker " {
		t.Fatalf("scheduler validate match = %q, want gha-mac-broker ", capturedHooks.Options().Config.ValidateMatch)
	}
	if capturedHooks.StopForRelaunch == nil {
		t.Fatal("scheduler stop hook is missing")
	}
	capturedHooks.StopForRelaunch()
	if !stopCalled {
		t.Fatal("scheduler stop hook did not call serve stop function")
	}
	if ctx.Err() == nil {
		t.Fatal("serve context was not canceled")
	}
}

type updateTestHooks struct {
	check        func(context.Context, selfupdate.Options) (selfupdate.CheckResult, error)
	apply        func(context.Context, selfupdate.Options) (selfupdate.ApplyResult, error)
	loadState    func(string) (selfupdate.State, error)
	restart      func(context.Context) (bool, error)
	runScheduler func(context.Context, selfupdate.SchedulerHooks)
}

func withUpdateTestHooks(t *testing.T, hooks updateTestHooks) {
	t.Helper()
	oldCheck := checkUpdate
	oldApply := applyUpdate
	oldLoadState := loadUpdateState
	oldRestart := restartManagedService
	oldRunScheduler := runSelfUpdateScheduler
	if hooks.check != nil {
		checkUpdate = hooks.check
	}
	if hooks.apply != nil {
		applyUpdate = hooks.apply
	}
	if hooks.loadState != nil {
		loadUpdateState = hooks.loadState
	}
	if hooks.restart != nil {
		restartManagedService = hooks.restart
	}
	if hooks.runScheduler != nil {
		runSelfUpdateScheduler = hooks.runScheduler
	}
	t.Cleanup(func() {
		checkUpdate = oldCheck
		applyUpdate = oldApply
		loadUpdateState = oldLoadState
		restartManagedService = oldRestart
		runSelfUpdateScheduler = oldRunScheduler
	})
}
