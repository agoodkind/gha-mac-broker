package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"goodkind.io/gha-mac-broker/internal/config"
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

var testSecret = []byte("test-secret")

var testCapacityToken = []byte("test-capacity-token")

func newTestConfig(allowedRepo string) *config.Config {
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
		Labels:       []string{"self-hosted", "macOS"},
		AllowedRepos: []string{allowedRepo},
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{})
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, pool)
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, pool)
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

func TestWebhookDisallowedRepoReturns204(t *testing.T) {
	pool := &testPool{}
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, pool)
	body := webhookBody("queued", "other/repo", []string{"self-hosted"}, 99)
	w := postWebhook(t, srv, body)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for disallowed repo, got %d", w.Code)
	}
	if got := len(pool.Jobs()); got != 0 {
		t.Fatalf("enqueued jobs = %d, want 0", got)
	}
}

func TestWebhookNoMatchingLabelReturns204(t *testing.T) {
	pool := &testPool{}
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, pool)
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, pool)
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, pool)
	body := webhookBody("queued", "owner/repo", []string{"self-hosted"}, 7)
	w := postWebhook(t, srv, body)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestWebhookQueuedWithUnresolvedDefaultImageReturns204(t *testing.T) {
	cfg := newTestConfig("owner/repo")
	cfg.Tart.Images = nil
	cfg.Tart.BaseImage = "docker.io/library/alpine:latest"
	pool := &testPool{}
	srv := New(testSecret, cfg, nil, nil, pool)
	body := webhookBody("queued", "owner/repo", []string{"self-hosted"}, 7)
	w := postWebhook(t, srv, body)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if got := len(pool.Jobs()); got != 0 {
		t.Fatalf("enqueued jobs = %d, want 0", got)
	}
}

func TestWebhookNonPostMethodReturns405(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{})
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

func TestCapacityDisallowedRepo(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{ready: true})
	resp := capacityRequest(t, srv, "/capacity?repo=other/repo&run_id=1")
	if resp.Available {
		t.Fatal("expected available=false for disallowed repo")
	}
}

func TestCapacityAvailableUsesPoolReadiness(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{ready: true})
	resp := capacityRequest(t, srv, "/capacity?repo=owner/repo&os=tahoe&xcode=26.5")
	if !resp.Available {
		t.Fatal("expected available=true when runner pool is ready")
	}
}

func TestCapacityUnmappedImageReturnsUnavailable(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{ready: true})
	resp := capacityRequest(t, srv, "/capacity?repo=owner/repo&os=tahoe&xcode=raw")
	if resp.Available {
		t.Fatal("expected available=false for unmapped image")
	}
}

func TestCapacityNotAvailableWhenPoolNotReady(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{ready: false})
	resp := capacityRequest(t, srv, "/capacity?repo=owner/repo")
	if resp.Available {
		t.Fatal("expected available=false when runner pool is not ready")
	}
}

func TestCapacityNoHeaderReturns401(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{ready: true})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=10", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with no auth header, got %d", w.Code)
	}
}

func TestCapacityWrongTokenReturns401(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{ready: true})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=11", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong token, got %d", w.Code)
	}
}

func TestCapacityCorrectTokenReturns200AndAvailable(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{ready: true})
	resp := capacityRequest(t, srv, "/capacity?repo=owner/repo")
	if !resp.Available {
		t.Fatal("expected available=true when runner pool is ready")
	}
}

func TestCapacityEmptyTokenAlwaysReturns401(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{ready: true})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=13", nil)
	req.Header.Set("Authorization", "Bearer any-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when server has no token configured, got %d", w.Code)
	}
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
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, pool)

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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, allowed, &testPool{})
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, allowed, &testPool{})
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{})
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}
