package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"goodkind.io/gha-mac-broker/internal/hostedload"
	"goodkind.io/gha-mac-broker/internal/runnerpool"
	"goodkind.io/gha-mac-broker/internal/server"
)

// stubPool satisfies the server's pooler interface without a real VM pool, so the
// injectable listener path can be exercised end to end over HTTP.
type stubPool struct {
	ready     bool
	enqueued  []runnerpool.Job
	cancelled []int64
}

func (p *stubPool) Enqueue(_ context.Context, job runnerpool.Job) error {
	p.enqueued = append(p.enqueued, job)
	return nil
}

func (p *stubPool) Ready() bool { return p.ready }

func (p *stubPool) Status(_ context.Context) (runnerpool.Snapshot, []runnerpool.WorkerView) {
	return runnerpool.Snapshot{Ready: p.ready}, nil
}

func (p *stubPool) CancelRun(jobID int64) { p.cancelled = append(p.cancelled, jobID) }

// TestHTTPServeServesWebhookAndStatusOnInjectedListener proves the injectable
// listener path serves the webhook and /status: httpServe binds nothing itself and
// serves on a listener handed to it, which is the seam the supervisor uses to own
// the descriptor and hand it to the worker.
func TestHTTPServeServesWebhookAndStatusOnInjectedListener(t *testing.T) {
	webhookSecret := []byte("webhook-secret")
	capacityToken := []byte("capacity-token")
	pool := &stubPool{ready: true, enqueued: nil, cancelled: nil}
	srv := server.New(webhookSecret, testServeConfig(), capacityToken, nil, pool, hostedload.NewTracker(), nil)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- httpServe(ctx, listener, srv, func() { close(ready) })
	}()
	<-ready
	t.Cleanup(func() {
		cancel()
		select {
		case <-serveDone:
		case <-time.After(10 * time.Second):
			t.Error("httpServe did not return after cancel")
		}
	})

	// /status requires the bearer token and returns the pool snapshot.
	statusReq, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/status", nil)
	if err != nil {
		t.Fatalf("build status request: %v", err)
	}
	statusReq.Header.Set("Authorization", "Bearer "+string(capacityToken))
	statusResp, err := http.DefaultClient.Do(statusReq)
	if err != nil {
		t.Fatalf("status request: %v", err)
	}
	defer func() { _ = statusResp.Body.Close() }()
	if statusResp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d, want 200", statusResp.StatusCode)
	}
	var statusBody struct {
		Snapshot runnerpool.Snapshot `json:"snapshot"`
	}
	if err := json.NewDecoder(statusResp.Body).Decode(&statusBody); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if !statusBody.Snapshot.Ready {
		t.Fatalf("status snapshot ready = false, want true")
	}

	// A signed completed+cancelled webhook cancels the run and returns 204, proving
	// the webhook endpoint serves and verifies signatures on the injected listener.
	payload := []byte(`{"action":"completed","repository":{"full_name":"owner/repo"},"workflow_job":{"id":77,"run_id":9,"conclusion":"cancelled"}}`)
	webhookResp := postWebhook(t, ctx, addr, webhookSecret, payload)
	defer func() { _ = webhookResp.Body.Close() }()
	if webhookResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(webhookResp.Body)
		t.Fatalf("webhook code = %d, want 204; body = %s", webhookResp.StatusCode, string(body))
	}
	if len(pool.cancelled) != 1 || pool.cancelled[0] != 77 {
		t.Fatalf("cancelled runs = %v, want [77]", pool.cancelled)
	}
}

func postWebhook(t *testing.T, ctx context.Context, addr string, secret []byte, payload []byte) *http.Response {
	t.Helper()
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/webhook", strings.NewReader(string(payload)))
	if err != nil {
		t.Fatalf("build webhook request: %v", err)
	}
	req.Header.Set("X-Hub-Signature-256", signature)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("webhook request: %v", err)
	}
	return resp
}

func TestReloadOrRestartRestartsManagedService(t *testing.T) {
	restoreReloadSeams(t)
	restartCalled := false
	restartManagedService = func(_ context.Context) (bool, error) {
		restartCalled = true
		return true, nil
	}

	changed, err := reloadOrRestart(context.Background())
	if err != nil {
		t.Fatalf("reloadOrRestart: %v", err)
	}
	if !changed || !restartCalled {
		t.Fatalf("changed=%v restartCalled=%v, want both true", changed, restartCalled)
	}
}

func TestRunDeployPlansServiceRestartForHostChange(t *testing.T) {
	restoreReloadSeams(t)

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	writeReloadConfig(t, configPath, 1)

	var stdout strings.Builder
	var stderr strings.Builder
	args := []string{
		"-config", configPath,
		"-compiled-host-fingerprint", "host-new",
		"-running-host-fingerprint", "host-old",
	}
	if err := runDeployWithWriters(context.Background(), args, &stdout, &stderr); err != nil {
		t.Fatalf("runDeployWithWriters: %v; stderr=%s", err, stderr.String())
	}
	// With one serve daemon and no host supervisor, a host-only change always plans
	// a full service restart rather than an in-place worker reload.
	if !strings.Contains(stdout.String(), "deploy plan: service-restart") {
		t.Fatalf("stdout = %q, want service-restart plan", stdout.String())
	}
}

func restoreReloadSeams(t *testing.T) {
	t.Helper()
	oldRestart := restartManagedService
	t.Cleanup(func() {
		restartManagedService = oldRestart
	})
}
