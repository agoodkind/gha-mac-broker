package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/hostedload"
	"goodkind.io/gha-mac-broker/internal/hoststats"
	"goodkind.io/gha-mac-broker/internal/runnerpool"
)

type testPool struct {
	mu         sync.Mutex
	ready      bool
	enqueueErr error
	enqueued   []runnerpool.Job
	cancelled  []int64
	snapshot   runnerpool.Snapshot
	workers    []runnerpool.WorkerView
}

type blockingWebhookBody struct {
	body        []byte
	readStarted chan struct{}
	release     chan struct{}
	once        sync.Once
	offset      int
}

func (b *blockingWebhookBody) Read(p []byte) (int, error) {
	b.once.Do(func() {
		close(b.readStarted)
		<-b.release
	})
	if b.offset >= len(b.body) {
		return 0, io.EOF
	}
	n := copy(p, b.body[b.offset:])
	b.offset += n
	return n, nil
}

func (b *blockingWebhookBody) Close() error {
	return nil
}

func (p *testPool) Enqueue(_ context.Context, job runnerpool.Job) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.enqueueErr != nil {
		return p.enqueueErr
	}
	p.enqueued = append(p.enqueued, job)
	return nil
}

func (p *testPool) Ready() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ready
}

func (p *testPool) Status(_ context.Context) (runnerpool.Snapshot, []runnerpool.WorkerView) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshot, append([]runnerpool.WorkerView(nil), p.workers...)
}

func (p *testPool) CancelRun(jobID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancelled = append(p.cancelled, jobID)
}

func (p *testPool) Jobs() []runnerpool.Job {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]runnerpool.Job(nil), p.enqueued...)
}

func (p *testPool) CancelledJobs() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int64(nil), p.cancelled...)
}

type fakeStatsProvider struct {
	sample hoststats.Sample
}

func (f *fakeStatsProvider) Latest() hoststats.Sample {
	return f.sample
}

var testSecret = []byte("test-secret")

var testCapacityToken = []byte("test-capacity-token")

func newTestConfig() *config.Config {
	return &config.Config{
		ListenAddr:  ":8080",
		RunnerCount: 3,
		MaxIdle:     config.Duration(2),
		MaxAge:      config.Duration(24),
		App: config.AppConfig{
			AppID:             "1",
			PrivateKeyPath:    "/tmp/key",
			WebhookSecretPath: "/tmp/secret",
			CapacityTokenPath: "",
			WebhookCIDRsPath:  "",
		},
		Tart: config.TartConfig{
			Binary:       "tart",
			GoldenImage:  "",
			BaseImage:    config.DefaultBaseImage,
			WarmBudget:   2,
			GoldenBudget: 3,
			Images: []config.ImageMapping{
				{MacOS: "tahoe", Xcode: "26.5", Tag: config.DefaultBaseImage},
			},
			VMNamePrefix:     "gha",
			CacheDir:         "",
			FastPull:         nil,
			FastPullDir:      "",
			FastPullParallel: 0,
		},
		Labels: []string{"self-hosted", "macOS"},
	}
}

func signBody(body []byte) string {
	mac := hmac.New(sha256.New, testSecret)
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func webhookBody(action string, repo string, labels []string, runID int64) []byte {
	return webhookBodyWithJobID(action, repo, labels, runID, 7000+runID)
}

func webhookBodyWithJobID(action string, repo string, labels []string, runID int64, jobID int64) []byte {
	return webhookBodyWithConclusion(action, repo, labels, runID, jobID, "")
}

func webhookBodyWithConclusion(action string, repo string, labels []string, runID int64, jobID int64, conclusion string) []byte {
	payload := webhookPayload{
		Action:     webhookAction(action),
		Repository: webhookRepo{FullName: repo},
		WorkflowJob: webhookJobField{
			ID:         jobID,
			Labels:     labels,
			RunID:      runID,
			Status:     action,
			Conclusion: conclusion,
			RunnerName: "",
			RunnerID:   0,
		},
	}
	body, _ := json.Marshal(payload)
	return body
}

func postWebhook(t *testing.T, srv *Server, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	return w
}

func capacityRequest(t *testing.T, srv *Server, target string) capacityResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("Authorization", "Bearer test-capacity-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("capacity status = %d, want 200", w.Code)
	}
	var resp capacityResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	return resp
}

func TestVerifySignatureValid(t *testing.T) {
	body := []byte(`{"action":"queued"}`)
	sig := signBody(body)
	if !verifySignature(testSecret, body, sig) {
		t.Fatal("expected valid signature to pass")
	}
}

func TestVerifySignatureBadHex(t *testing.T) {
	body := []byte(`{"action":"queued"}`)
	if verifySignature(testSecret, body, "sha256=notvalidhex!") {
		t.Fatal("expected bad hex to fail")
	}
}

func TestVerifySignatureWrongPrefix(t *testing.T) {
	body := []byte(`{"action":"queued"}`)
	mac := hmac.New(sha256.New, testSecret)
	_, _ = mac.Write(body)
	if verifySignature(testSecret, body, "md5="+hex.EncodeToString(mac.Sum(nil))) {
		t.Fatal("expected non-sha256 prefix to fail")
	}
}

func TestVerifySignatureWrongSecret(t *testing.T) {
	body := []byte(`{"action":"queued"}`)
	mac := hmac.New(sha256.New, []byte("other-secret"))
	_, _ = mac.Write(body)
	sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if verifySignature(testSecret, body, sig) {
		t.Fatal("expected wrong-secret signature to fail")
	}
}

func TestWebhookBadSignatureReturns401(t *testing.T) {
	srv := New(testSecret, newTestConfig(), nil, nil, &testPool{}, hostedload.NewTracker(), nil)
	body := webhookBody("queued", "owner/repo", []string{"self-hosted"}, 42)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", "sha256=badbadbadbad")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestWebhookNonQueuedReturns204(t *testing.T) {
	pool := &testPool{}
	srv := New(testSecret, newTestConfig(), nil, nil, pool, hostedload.NewTracker(), nil)
	body := webhookBody("in_progress", "owner/repo", []string{"self-hosted"}, 42)
	w := postWebhook(t, srv, body)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for non-queued, got %d", w.Code)
	}
	if got := len(pool.Jobs()); got != 0 {
		t.Fatalf("enqueued jobs = %d, want 0", got)
	}
}

func TestWebhookCompletedCancelsCancelledAndSkippedJobs(t *testing.T) {
	pool := &testPool{}
	srv := New(testSecret, newTestConfig(), nil, nil, pool, hostedload.NewTracker(), nil)
	testCases := []struct {
		name       string
		runID      int64
		jobID      int64
		conclusion string
		wantJobs   []int64
	}{
		{name: "cancelled", runID: 42, jobID: 1001, conclusion: "cancelled", wantJobs: []int64{1001}},
		{name: "skipped", runID: 42, jobID: 1002, conclusion: "skipped", wantJobs: []int64{1001, 1002}},
		{name: "success", runID: 42, jobID: 1003, conclusion: "success", wantJobs: []int64{1001, 1002}},
		{name: "empty", runID: 42, jobID: 1004, conclusion: "", wantJobs: []int64{1001, 1002}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			body := webhookBodyWithConclusion("completed", "owner/repo", []string{"self-hosted"}, testCase.runID, testCase.jobID, testCase.conclusion)
			w := postWebhook(t, srv, body)
			if w.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 204", w.Code)
			}
			gotJobs := pool.CancelledJobs()
			if fmt.Sprint(gotJobs) != fmt.Sprint(testCase.wantJobs) {
				t.Fatalf("cancelled jobs = %v, want %v", gotJobs, testCase.wantJobs)
			}
		})
	}
}

func TestWebhookQueuedEnqueuesJobForAnyRepoWithMatchingLabel(t *testing.T) {
	pool := &testPool{}
	srv := New(testSecret, newTestConfig(), nil, nil, pool, hostedload.NewTracker(), nil)
	body := webhookBody("queued", "other/repo", []string{"self-hosted"}, 99)
	w := postWebhook(t, srv, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for matching pool label, got %d", w.Code)
	}
	jobs := pool.Jobs()
	if len(jobs) != 1 {
		t.Fatalf("enqueued jobs = %+v, want one", jobs)
	}
	want := runnerpool.Job{Repo: "other/repo", JobID: 7099, RunID: 99}
	if jobs[0] != want {
		t.Fatalf("enqueued job = %+v, want %+v", jobs[0], want)
	}
}

func TestWebhookNoMatchingLabelReturns204(t *testing.T) {
	pool := &testPool{}
	srv := New(testSecret, newTestConfig(), nil, nil, pool, hostedload.NewTracker(), nil)
	body := webhookBody("queued", "owner/repo", []string{"ubuntu-latest"}, 99)
	w := postWebhook(t, srv, body)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 when no label matches, got %d", w.Code)
	}
	if got := len(pool.Jobs()); got != 0 {
		t.Fatalf("enqueued jobs = %d, want 0", got)
	}
}

func TestWebhookQueuedEnqueuesJobAndReturns202(t *testing.T) {
	pool := &testPool{}
	srv := New(testSecret, newTestConfig(), nil, nil, pool, hostedload.NewTracker(), nil)
	body := webhookBodyWithJobID("queued", "owner/repo", []string{"self-hosted"}, 7, 1007)
	w := postWebhook(t, srv, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}
	jobs := pool.Jobs()
	if len(jobs) != 1 {
		t.Fatalf("enqueued jobs = %+v, want one", jobs)
	}
	want := runnerpool.Job{Repo: "owner/repo", JobID: 1007, RunID: 7}
	if jobs[0] != want {
		t.Fatalf("enqueued job = %+v, want %+v", jobs[0], want)
	}
}

func TestWebhookQueuedReturns503WhenEnqueueFails(t *testing.T) {
	pool := &testPool{enqueueErr: errors.New("pool shutting down")}
	srv := New(testSecret, newTestConfig(), nil, nil, pool, hostedload.NewTracker(), nil)
	body := webhookBody("queued", "owner/repo", []string{"self-hosted"}, 7)
	w := postWebhook(t, srv, body)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestWebhookQueuedWithUnresolvedDefaultImageReturns204(t *testing.T) {
	cfg := newTestConfig()
	cfg.Tart.Images = nil
	cfg.Tart.BaseImage = "docker.io/library/alpine:latest"
	pool := &testPool{}
	srv := New(testSecret, cfg, nil, nil, pool, hostedload.NewTracker(), nil)
	body := webhookBody("queued", "owner/repo", []string{"self-hosted"}, 7)
	w := postWebhook(t, srv, body)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if got := len(pool.Jobs()); got != 0 {
		t.Fatalf("enqueued jobs = %d, want 0", got)
	}
}

func TestWebhookUsesSingleConfigSnapshotDuringRequest(t *testing.T) {
	pool := &testPool{}
	allowed := []*net.IPNet{mustParseCIDR(t, "192.30.252.0/22")}
	srv := New(testSecret, newTestConfig(), nil, allowed, pool, hostedload.NewTracker(), nil)
	body := webhookBodyWithJobID("queued", "owner/repo", []string{"self-hosted"}, 7, 1007)
	blockingBody := &blockingWebhookBody{
		body:        body,
		readStarted: make(chan struct{}),
		release:     make(chan struct{}),
	}
	req := httptest.NewRequest(http.MethodPost, "/webhook", nil)
	req.Body = blockingBody
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	req.Header.Set("CF-Connecting-IP", "192.30.252.1")
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeHTTP(w, req)
	}()

	select {
	case <-blockingBody.readStarted:
	case <-time.After(time.Second):
		t.Fatal("webhook body read did not start")
	}

	reloadedConfig := newTestConfig()
	reloadedConfig.Labels = []string{"other-label"}
	srv.Reconfigure([]byte("rotated-secret"), reloadedConfig, nil, []*net.IPNet{mustParseCIDR(t, "10.0.0.0/8")})
	close(blockingBody.release)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("webhook request did not finish")
	}

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", w.Code)
	}
	jobs := pool.Jobs()
	if len(jobs) != 1 {
		t.Fatalf("enqueued jobs = %+v, want one", jobs)
	}
	want := runnerpool.Job{Repo: "owner/repo", JobID: 1007, RunID: 7}
	if jobs[0] != want {
		t.Fatalf("enqueued job = %+v, want %+v", jobs[0], want)
	}
}

func TestWebhookNonPostMethodReturns405(t *testing.T) {
	srv := New(testSecret, newTestConfig(), nil, nil, &testPool{}, hostedload.NewTracker(), nil)
	body := webhookBody("queued", "owner/repo", []string{"self-hosted"}, 42)
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(method, "/webhook", strings.NewReader(string(body)))
		req.Header.Set("X-Hub-Signature-256", signBody(body))
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405 for %s, got %d", method, w.Code)
		}
	}
}

func TestCapacityAvailableForAnyRepo(t *testing.T) {
	srv := New(testSecret, newTestConfig(), testCapacityToken, nil, &testPool{ready: true}, hostedload.NewTracker(), nil)
	resp := capacityRequest(t, srv, "/capacity?repo=other/repo&run_id=1")
	if !resp.Available {
		t.Fatal("expected available=true for any repo when runner pool is ready")
	}
}

func TestCapacityAvailableUsesPoolReadiness(t *testing.T) {
	srv := New(testSecret, newTestConfig(), testCapacityToken, nil, &testPool{ready: true}, hostedload.NewTracker(), nil)
	resp := capacityRequest(t, srv, "/capacity?repo=owner/repo&os=tahoe&xcode=26.5")
	if !resp.Available {
		t.Fatal("expected available=true when runner pool is ready")
	}
}

func TestCapacityUnmappedImageReturnsUnavailable(t *testing.T) {
	srv := New(testSecret, newTestConfig(), testCapacityToken, nil, &testPool{ready: true}, hostedload.NewTracker(), nil)
	resp := capacityRequest(t, srv, "/capacity?repo=owner/repo&os=tahoe&xcode=raw")
	if resp.Available {
		t.Fatal("expected available=false for unmapped image")
	}
}

func TestCapacityNotAvailableWhenPoolNotReady(t *testing.T) {
	srv := New(testSecret, newTestConfig(), testCapacityToken, nil, &testPool{ready: false}, hostedload.NewTracker(), nil)
	resp := capacityRequest(t, srv, "/capacity?repo=owner/repo")
	if resp.Available {
		t.Fatal("expected available=false when runner pool is not ready")
	}
}

func TestCapacityPublicNoHeaderReturns200(t *testing.T) {
	srv := New(testSecret, newTestConfig(), testCapacityToken, nil, &testPool{ready: true}, hostedload.NewTracker(), nil)
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=10", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with no auth header (capacity is public), got %d", w.Code)
	}
}

func TestCapacityPublicIgnoresWrongToken(t *testing.T) {
	srv := New(testSecret, newTestConfig(), testCapacityToken, nil, &testPool{ready: true}, hostedload.NewTracker(), nil)
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=11", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with a wrong token (capacity is public), got %d", w.Code)
	}
}

func TestCapacityReturns200AndAvailable(t *testing.T) {
	srv := New(testSecret, newTestConfig(), testCapacityToken, nil, &testPool{ready: true}, hostedload.NewTracker(), nil)
	resp := capacityRequest(t, srv, "/capacity")
	if !resp.Available {
		t.Fatal("expected available=true when runner pool is ready")
	}
}

func TestCapacityHostedFree(t *testing.T) {
	t.Run("free when in-progress count is below the limit", func(t *testing.T) {
		cfg := newTestConfig()
		cfg.HostedMacOSConcurrencyLimit = 2
		tracker := hostedload.NewTracker()
		srv := New(testSecret, cfg, testCapacityToken, nil, &testPool{ready: true}, tracker, nil)
		tracker.MarkInProgress(1)
		resp := capacityRequest(t, srv, "/capacity")
		if !resp.Available {
			t.Fatal("expected available=true when runner pool is ready")
		}
		if !resp.HostedFree {
			t.Fatal("expected hosted_free=true when count is below the limit")
		}
	})

	t.Run("not free when in-progress count reaches the limit", func(t *testing.T) {
		cfg := newTestConfig()
		cfg.HostedMacOSConcurrencyLimit = 2
		tracker := hostedload.NewTracker()
		srv := New(testSecret, cfg, testCapacityToken, nil, &testPool{ready: true}, tracker, nil)
		tracker.MarkInProgress(1)
		tracker.MarkInProgress(2)
		resp := capacityRequest(t, srv, "/capacity")
		if resp.HostedFree {
			t.Fatal("expected hosted_free=false when count is at the limit")
		}
	})

	t.Run("free regardless of count when the limit is non-positive", func(t *testing.T) {
		cfg := newTestConfig()
		cfg.HostedMacOSConcurrencyLimit = 0
		tracker := hostedload.NewTracker()
		srv := New(testSecret, cfg, testCapacityToken, nil, &testPool{ready: true}, tracker, nil)
		tracker.MarkInProgress(1)
		tracker.MarkInProgress(2)
		tracker.MarkInProgress(3)
		resp := capacityRequest(t, srv, "/capacity")
		if !resp.HostedFree {
			t.Fatal("expected hosted_free=true when the limit is non-positive")
		}
	})

	t.Run("spill case reports hosted_free even when unavailable", func(t *testing.T) {
		cfg := newTestConfig()
		cfg.HostedMacOSConcurrencyLimit = 2
		tracker := hostedload.NewTracker()
		srv := New(testSecret, cfg, testCapacityToken, nil, &testPool{ready: true}, tracker, nil)
		resp := capacityRequest(t, srv, "/capacity?os=tahoe&xcode=raw")
		if resp.Available {
			t.Fatal("expected available=false for an unmapped image")
		}
		if !resp.HostedFree {
			t.Fatal("expected hosted_free=true in the unavailable spill response")
		}
	})
}

func TestStatusRequiresBearerTokenAndReturnsPoolViews(t *testing.T) {
	activeJob := false
	pool := &testPool{
		ready: true,
		snapshot: runnerpool.Snapshot{
			RunnerCount: 2,
			Idle:        1,
			Busy:        1,
			Queued:      0,
			Healthy:     true,
			Ready:       true,
		},
		workers: []runnerpool.WorkerView{
			{
				Index:          0,
				VM:             "vm-busy",
				Phase:          "busy",
				RunID:          42,
				BindAgeSeconds: 120,
				ActiveJob:      &activeJob,
				LastError:      "",
			},
			{
				Index:          1,
				VM:             "vm-idle",
				Phase:          "idle",
				RunID:          0,
				BindAgeSeconds: 0,
				ActiveJob:      nil,
				LastError:      "",
			},
		},
	}
	srv := New(testSecret, newTestConfig(), testCapacityToken, nil, pool, hostedload.NewTracker(), nil)

	unauthorizedReq := httptest.NewRequest(http.MethodGet, "/status", nil)
	unauthorizedRecorder := httptest.NewRecorder()
	srv.ServeHTTP(unauthorizedRecorder, unauthorizedReq)
	if unauthorizedRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("status without token = %d, want 401", unauthorizedRecorder.Code)
	}

	authorizedReq := httptest.NewRequest(http.MethodGet, "/status", nil)
	authorizedReq.Header.Set("Authorization", "Bearer test-capacity-token")
	authorizedRecorder := httptest.NewRecorder()
	srv.ServeHTTP(authorizedRecorder, authorizedReq)
	if authorizedRecorder.Code != http.StatusOK {
		t.Fatalf("status with token = %d, want 200", authorizedRecorder.Code)
	}

	var resp statusResponse
	if err := json.Unmarshal(authorizedRecorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal status response: %v", err)
	}
	if resp.Snapshot.Busy != 1 {
		t.Fatalf("status busy = %d, want 1", resp.Snapshot.Busy)
	}
	if len(resp.Workers) != 2 {
		t.Fatalf("status workers = %+v, want 2 workers", resp.Workers)
	}
	if resp.Workers[0].Phase != "busy" {
		t.Fatalf("status worker phase = %q, want busy", resp.Workers[0].Phase)
	}
	if resp.Workers[0].ActiveJob == nil {
		t.Fatal("status worker active job = nil, want false")
	}
	if *resp.Workers[0].ActiveJob {
		t.Fatal("status worker active job = true, want false")
	}
}

func TestStatusIncludesHostStatsFromProvider(t *testing.T) {
	sampledAt := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	fakeStats := &fakeStatsProvider{
		sample: hoststats.Sample{
			Host: hoststats.Host{
				IdlePct: 42.5,
			},
			Inventory: hoststats.Inventory{
				Busy: 3,
			},
			SampledAt: sampledAt,
		},
	}
	pool := &testPool{
		ready: true,
		snapshot: runnerpool.Snapshot{
			RunnerCount: 2,
			Idle:        1,
			Busy:        1,
		},
	}
	srv := New(testSecret, newTestConfig(), testCapacityToken, nil, pool, hostedload.NewTracker(), fakeStats)

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.Header.Set("Authorization", "Bearer test-capacity-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status with token = %d, want 200", w.Code)
	}

	var resp statusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal status response: %v", err)
	}
	if resp.Snapshot.Busy != 1 {
		t.Fatalf("status snapshot busy = %d, want 1", resp.Snapshot.Busy)
	}
	if resp.HostStats.Host.IdlePct != 42.5 {
		t.Fatalf("status host_stats.host.idle_pct = %v, want 42.5", resp.HostStats.Host.IdlePct)
	}
	if resp.HostStats.Inventory.Busy != 3 {
		t.Fatalf("status host_stats.inventory.busy = %d, want 3", resp.HostStats.Inventory.Busy)
	}
	if !resp.HostStats.SampledAt.Equal(sampledAt) {
		t.Fatalf("status host_stats.sampled_at = %v, want %v", resp.HostStats.SampledAt, sampledAt)
	}
}

func TestStatusNilStatsProviderOmitsHostStats(t *testing.T) {
	pool := &testPool{ready: true}
	srv := New(testSecret, newTestConfig(), testCapacityToken, nil, pool, hostedload.NewTracker(), nil)

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.Header.Set("Authorization", "Bearer test-capacity-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status with nil stats provider = %d, want 200", w.Code)
	}

	var resp statusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal status response: %v", err)
	}
	if resp.HostStats.Host.IdlePct != 0 {
		t.Fatalf("status host_stats.host.idle_pct = %v, want zero value", resp.HostStats.Host.IdlePct)
	}
}

func mustParseCIDR(t *testing.T, cidr string) *net.IPNet {
	t.Helper()
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		t.Fatalf("ParseCIDR(%q): %v", cidr, err)
	}
	return ipNet
}

func TestWebhookDisallowedIPReturns403(t *testing.T) {
	allowed := []*net.IPNet{mustParseCIDR(t, "192.30.252.0/22")}
	srv := New(testSecret, newTestConfig(), nil, allowed, &testPool{}, hostedload.NewTracker(), nil)
	body := webhookBody("queued", "owner/repo", []string{"self-hosted"}, 99)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	req.Header.Set("CF-Connecting-IP", "1.2.3.4")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for disallowed IP via CF-Connecting-IP, got %d", w.Code)
	}
}

func TestWebhookAllowedIPProceedsToHMAC(t *testing.T) {
	allowed := []*net.IPNet{mustParseCIDR(t, "192.30.252.0/22")}
	srv := New(testSecret, newTestConfig(), nil, allowed, &testPool{}, hostedload.NewTracker(), nil)
	body := webhookBody("queued", "owner/repo", []string{"self-hosted"}, 99)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", "sha256=badbadbadbad")
	req.Header.Set("CF-Connecting-IP", "192.30.252.1")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for bad HMAC after IP allowed, got %d", w.Code)
	}
}

func TestWebhookEmptyCIDRsSkipsIPCheck(t *testing.T) {
	srv := New(testSecret, newTestConfig(), nil, nil, &testPool{}, hostedload.NewTracker(), nil)
	body := webhookBody("in_progress", "owner/repo", []string{"self-hosted"}, 42)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
}

func TestHealthz(t *testing.T) {
	srv := New(testSecret, newTestConfig(), nil, nil, &testPool{}, hostedload.NewTracker(), nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

type cancelCall struct {
	repo    string
	headSHA string
}

type fakeRunCanceller struct {
	mu        sync.Mutex
	calls     []cancelCall
	cancelled int
	err       error
}

func (f *fakeRunCanceller) CancelActiveRunsForHeadSHA(_ context.Context, repo, headSHA string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, cancelCall{repo: repo, headSHA: headSHA})
	if f.err != nil {
		return 0, f.err
	}
	return f.cancelled, nil
}

func (f *fakeRunCanceller) Calls() []cancelCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]cancelCall(nil), f.calls...)
}

func pullRequestBody(action string, repo string, headSHA string, number int) []byte {
	payload := webhookPayload{
		Action:     webhookAction(action),
		Repository: webhookRepo{FullName: repo},
	}
	payload.PullRequest.Number = number
	payload.PullRequest.Head.SHA = headSHA
	body, _ := json.Marshal(payload)
	return body
}

func postPullRequestWebhook(t *testing.T, srv *Server, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	req.Header.Set("X-Github-Event", "pull_request")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	return w
}

func TestPullRequestClosedCancelsRunsForHeadSHA(t *testing.T) {
	rc := &fakeRunCanceller{cancelled: 2}
	srv := New(testSecret, newTestConfig(), nil, nil, &testPool{}, hostedload.NewTracker(), nil, WithRunCanceller(rc))
	w := postPullRequestWebhook(t, srv, pullRequestBody("closed", "agoodkind/lmd", "abc123", 42))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNoContent)
	}
	calls := rc.Calls()
	if len(calls) != 1 {
		t.Fatalf("cancel calls = %d, want 1", len(calls))
	}
	if calls[0].repo != "agoodkind/lmd" || calls[0].headSHA != "abc123" {
		t.Fatalf("cancel call = %+v, want repo agoodkind/lmd head abc123", calls[0])
	}
}

func TestPullRequestNonClosedActionDoesNotCancel(t *testing.T) {
	rc := &fakeRunCanceller{}
	srv := New(testSecret, newTestConfig(), nil, nil, &testPool{}, hostedload.NewTracker(), nil, WithRunCanceller(rc))
	w := postPullRequestWebhook(t, srv, pullRequestBody("opened", "agoodkind/lmd", "abc123", 42))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNoContent)
	}
	if got := len(rc.Calls()); got != 0 {
		t.Fatalf("cancel calls = %d, want 0 for a non-closed action", got)
	}
}

func TestPullRequestClosedWithoutHeadSHADoesNotCancel(t *testing.T) {
	rc := &fakeRunCanceller{}
	srv := New(testSecret, newTestConfig(), nil, nil, &testPool{}, hostedload.NewTracker(), nil, WithRunCanceller(rc))
	w := postPullRequestWebhook(t, srv, pullRequestBody("closed", "agoodkind/lmd", "", 42))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNoContent)
	}
	if got := len(rc.Calls()); got != 0 {
		t.Fatalf("cancel calls = %d, want 0 when head sha is missing", got)
	}
}

func TestPullRequestClosedWithoutCancellerReturns204(t *testing.T) {
	srv := New(testSecret, newTestConfig(), nil, nil, &testPool{}, hostedload.NewTracker(), nil)
	w := postPullRequestWebhook(t, srv, pullRequestBody("closed", "agoodkind/lmd", "abc123", 42))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNoContent)
	}
}

func TestPullRequestClosedCancellerErrorStillReturns204(t *testing.T) {
	rc := &fakeRunCanceller{err: errors.New("github unavailable")}
	srv := New(testSecret, newTestConfig(), nil, nil, &testPool{}, hostedload.NewTracker(), nil, WithRunCanceller(rc))
	w := postPullRequestWebhook(t, srv, pullRequestBody("closed", "agoodkind/lmd", "abc123", 42))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d (cancellation is best-effort)", w.Code, http.StatusNoContent)
	}
	if got := len(rc.Calls()); got != 1 {
		t.Fatalf("cancel calls = %d, want 1", got)
	}
}

func TestWorkflowJobWebhookIgnoresPullRequestPath(t *testing.T) {
	rc := &fakeRunCanceller{}
	pool := &testPool{}
	srv := New(testSecret, newTestConfig(), nil, nil, pool, hostedload.NewTracker(), nil, WithRunCanceller(rc))
	// A workflow_job delivery carries no X-Github-Event: pull_request header, so it
	// keeps the existing action switch and never calls the run canceller.
	w := postWebhook(t, srv, webhookBody("queued", "owner/repo", []string{"self-hosted", "macOS"}, 42))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusAccepted)
	}
	if got := len(rc.Calls()); got != 0 {
		t.Fatalf("cancel calls = %d, want 0 for a workflow_job delivery", got)
	}
}
