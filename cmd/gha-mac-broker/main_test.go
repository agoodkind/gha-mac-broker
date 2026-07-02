package main

import (
	"context"
	"errors"
	"testing"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/ghapp"
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
