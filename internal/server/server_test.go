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
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
	"goodkind.io/gha-mac-broker/internal/config"
)

// --- test stubs ---

type testPool struct {
	capacity     int
	freeSlots    int
	leaseVM      *broker.WarmVM
	leaseErr     error
	mu           sync.Mutex
	inUse        int
	nextID       int
	leasedImages []string
	recycled     []*broker.WarmVM
}

func (p *testPool) Lease(_ context.Context, image string) (*broker.WarmVM, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.leasedImages = append(p.leasedImages, image)
	if p.leaseErr != nil {
		return nil, p.leaseErr
	}
	if capacity, ok := p.configuredCapacityLocked(); ok {
		if p.inUse >= capacity {
			return nil, errors.New("pool: no free lease slots")
		}
		p.inUse++
	}
	if p.leaseVM != nil {
		return p.leaseVM, nil
	}
	p.nextID++
	return &broker.WarmVM{Name: fmt.Sprintf("gha-vm-%d", p.nextID), Image: image}, nil
}

func (p *testPool) configuredCapacityLocked() (int, bool) {
	if p.capacity > 0 {
		return p.capacity, true
	}
	if p.freeSlots > 0 {
		return p.freeSlots, true
	}
	return 0, false
}

func (p *testPool) FreeSlots() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	capacity := p.freeSlots
	if p.capacity > 0 {
		capacity = p.capacity
	}
	freeSlots := capacity - p.inUse
	if freeSlots < 0 {
		return 0
	}
	return freeSlots
}

func (p *testPool) Recycle(_ context.Context, vm *broker.WarmVM) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recycled = append(p.recycled, vm)
	if _, ok := p.configuredCapacityLocked(); ok && p.inUse > 0 {
		p.inUse--
	}
}

type testRunner struct {
	ran chan struct{}
	err error
}

func (r *testRunner) RunJob(_ context.Context, _ *broker.WarmVM, _, _ string) error {
	r.ran <- struct{}{}
	return r.err
}

type runStart struct {
	Active     int
	MaxActive  int
	Repo       string
	RunnerName string
}

type blockingRunner struct {
	started chan runStart
	release chan struct{}
	mu      sync.Mutex
	active  int
	max     int
}

func newBlockingRunner(expectedRuns int) *blockingRunner {
	return &blockingRunner{
		started: make(chan runStart, expectedRuns),
		release: make(chan struct{}, expectedRuns),
		mu:      sync.Mutex{},
	}
}

func (r *blockingRunner) RunJob(_ context.Context, _ *broker.WarmVM, repo string, runnerName string) error {
	r.mu.Lock()
	r.active++
	if r.active > r.max {
		r.max = r.active
	}
	start := runStart{
		Active:     r.active,
		MaxActive:  r.max,
		Repo:       repo,
		RunnerName: runnerName,
	}
	r.mu.Unlock()

	r.started <- start
	<-r.release

	r.mu.Lock()
	r.active--
	r.mu.Unlock()
	return nil
}

func (r *blockingRunner) Release() {
	r.release <- struct{}{}
}

func (r *blockingRunner) MaxActive() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.max
}

func waitRunStart(t *testing.T, runner *blockingRunner) runStart {
	t.Helper()
	select {
	case start := <-runner.started:
		return start
	case <-time.After(2 * time.Second):
		t.Fatal("RunJob was not called within timeout")
	}
	return runStart{}
}

func assertNoRunStart(t *testing.T, runner *blockingRunner) {
	t.Helper()
	select {
	case start := <-runner.started:
		t.Fatalf("unexpected RunJob start while pool slots were full: %+v", start)
	case <-time.After(200 * time.Millisecond):
	}
}

type cancelCall struct {
	Repo  string
	RunID int64
}

type testCanceller struct {
	mu        sync.Mutex
	calls     []cancelCall
	cancelErr error
}

func (c *testCanceller) CancelRun(_ context.Context, repo string, runID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, cancelCall{Repo: repo, RunID: runID})
	return c.cancelErr
}

type mutableClock struct {
	mu  sync.Mutex
	now time.Time
}

func newMutableClock(now time.Time) *mutableClock {
	return &mutableClock{mu: sync.Mutex{}, now: now}
}

func (c *mutableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *mutableClock) Advance(delta time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(delta)
}

// testSecret is the webhook HMAC secret used across all handler tests.
var testSecret = []byte("test-secret")

// testCapacityToken is the bearer token used across capacity handler tests.
var testCapacityToken = []byte("test-capacity-token")

// newTestConfig returns a minimal config with one allowed repo and labels.
func newTestConfig(allowedRepo string) *config.Config {
	return &config.Config{
		ListenAddr: ":8080",
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
			VMNamePrefix: "gha",
			CacheDir:     "",
			FastPull:     nil,
			FastPullDir:  "",
		},
		Labels:       []string{"self-hosted", "macOS"},
		AllowedRepos: []string{allowedRepo},
	}
}

// signBody computes the X-Hub-Signature-256 value for body under testSecret.
func signBody(body []byte) string {
	mac := hmac.New(sha256.New, testSecret)
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// --- verifySignature unit tests ---

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
	mac.Write(body)
	if verifySignature(testSecret, body, "md5="+hex.EncodeToString(mac.Sum(nil))) {
		t.Fatal("expected non-sha256 prefix to fail")
	}
}

func TestVerifySignatureWrongSecret(t *testing.T) {
	body := []byte(`{"action":"queued"}`)
	mac := hmac.New(sha256.New, []byte("other-secret"))
	mac.Write(body)
	sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if verifySignature(testSecret, body, sig) {
		t.Fatal("expected wrong-secret signature to fail")
	}
}

// --- webhook handler tests ---

func webhookBody(action, repo string, labels []string, runID int64) []byte {
	return webhookBodyWithJobID(action, repo, labels, runID, 7000+runID)
}

func webhookBodyWithJobID(action, repo string, labels []string, runID int64, jobID int64) []byte {
	return webhookBodyWithJobIDAndRunner(action, repo, labels, runID, jobID, "", 0)
}

func webhookBodyWithRunner(action, repo string, labels []string, runID int64, runnerName string, runnerID int64) []byte {
	return webhookBodyWithJobIDAndRunner(action, repo, labels, runID, 7000+runID, runnerName, runnerID)
}

func webhookBodyWithJobIDAndRunner(action, repo string, labels []string, runID int64, jobID int64, runnerName string, runnerID int64) []byte {
	payload := webhookPayload{
		Action:     webhookAction(action),
		Repository: webhookRepo{FullName: repo},
		WorkflowJob: webhookJobField{
			ID:         jobID,
			Labels:     labels,
			RunID:      runID,
			Status:     action,
			Conclusion: "",
			RunnerName: runnerName,
			RunnerID:   runnerID,
		},
	}
	b, _ := json.Marshal(payload)
	return b
}

func pendingDeliveryCount(s *Server) int {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	return len(s.pending)
}

func serveQueuedWebhook(t *testing.T, srv *Server, repo string, runID int64, jobID int64) int {
	t.Helper()
	body := webhookBodyWithJobID("queued", repo, []string{"self-hosted"}, runID, jobID)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	return w.Code
}

func TestWebhookBadSignatureReturns401(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{}, &testRunner{ran: make(chan struct{}, 1)})
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{}, &testRunner{ran: make(chan struct{}, 1)})
	body := webhookBody("in_progress", "owner/repo", []string{"self-hosted"}, 42)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for non-queued, got %d", w.Code)
	}
}

func TestWebhookDisallowedRepoReturns204(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{freeSlots: 2}, &testRunner{ran: make(chan struct{}, 1)})
	body := webhookBody("queued", "other/repo", []string{"self-hosted"}, 99)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for disallowed repo, got %d", w.Code)
	}
}

func TestWebhookNoMatchingLabelReturns204(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{freeSlots: 2}, &testRunner{ran: make(chan struct{}, 1)})
	body := webhookBody("queued", "owner/repo", []string{"ubuntu-latest"}, 99)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 when no label matches, got %d", w.Code)
	}
}

func TestWebhookQueuedDispatchesJobAndReturns202(t *testing.T) {
	vm := &broker.WarmVM{Name: "vm-1", Image: config.DefaultBaseImage}
	pool := &testPool{freeSlots: 2, leaseVM: vm}
	runner := newBlockingRunner(1)
	defer runner.Release()
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, pool, runner)

	body := webhookBody("queued", "owner/repo", []string{"self-hosted"}, 7)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}

	start := waitRunStart(t, runner)
	srv.markDelivered("owner/repo", 7007, start.RunnerName, 4242)
	runner.Release()

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.leasedImages) != 1 || pool.leasedImages[0] != config.DefaultBaseImage {
		t.Fatalf("leased images = %v, want %q", pool.leasedImages, config.DefaultBaseImage)
	}
}

func TestWebhookQueuedRecordsPendingDelivery(t *testing.T) {
	vm := &broker.WarmVM{Name: "gha-vm-1", Image: config.DefaultBaseImage}
	pool := &testPool{freeSlots: 2, leaseVM: vm}
	runner := newBlockingRunner(1)
	defer runner.Release()
	clock := newMutableClock(time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC))
	srv := New(
		testSecret,
		newTestConfig("owner/repo"),
		nil,
		nil,
		pool,
		runner,
		WithClock(clock.Now),
	)

	body := webhookBody("queued", "owner/repo", []string{"self-hosted"}, 7)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}
	if got := pendingDeliveryCount(srv); got != 1 {
		t.Fatalf("pending delivery count = %d, want 1", got)
	}
	start := waitRunStart(t, runner)
	srv.markDelivered("owner/repo", 7007, start.RunnerName, 4242)
	runner.Release()
}

func TestWebhookQueuedJobsDrainAfterRecycle(t *testing.T) {
	pool := &testPool{capacity: 1}
	runner := newBlockingRunner(2)
	defer runner.Release()
	defer runner.Release()
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, pool, runner)

	if got := serveQueuedWebhook(t, srv, "owner/repo", 42, 1001); got != http.StatusAccepted {
		t.Fatalf("first queued webhook status = %d, want 202", got)
	}
	firstStart := waitRunStart(t, runner)
	srv.markDelivered("owner/repo", 1001, firstStart.RunnerName, 4241)

	if got := serveQueuedWebhook(t, srv, "owner/repo", 42, 1002); got != http.StatusAccepted {
		t.Fatalf("second queued webhook status = %d, want 202", got)
	}
	assertNoRunStart(t, runner)

	runner.Release()
	secondStart := waitRunStart(t, runner)
	if secondStart.Repo != "owner/repo" {
		t.Fatalf("second RunJob repo = %q, want owner/repo", secondStart.Repo)
	}
	srv.markDelivered("owner/repo", 1002, secondStart.RunnerName, 4242)

	if maxActive := runner.MaxActive(); maxActive > 1 {
		t.Fatalf("max active RunJob goroutines = %d, want <= 1", maxActive)
	}
	if got := pendingDeliveryCount(srv); got != 0 {
		t.Fatalf("pending delivery count after drained deliveries = %d, want 0", got)
	}
}

func TestWebhookQueuedJobsDoNotOverbookDuringDrain(t *testing.T) {
	const jobCount = 5
	pool := &testPool{capacity: 2}
	runner := newBlockingRunner(jobCount)
	for i := 0; i < jobCount; i++ {
		defer runner.Release()
	}
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, pool, runner)

	for i := 0; i < jobCount; i++ {
		jobID := int64(1001 + i)
		if got := serveQueuedWebhook(t, srv, "owner/repo", 42, jobID); got != http.StatusAccepted {
			t.Fatalf("queued webhook %d status = %d, want 202", i+1, got)
		}
	}

	firstStart := waitRunStart(t, runner)
	secondStart := waitRunStart(t, runner)
	srv.markDelivered("owner/repo", 1001, firstStart.RunnerName, 4241)
	srv.markDelivered("owner/repo", 1002, secondStart.RunnerName, 4242)
	assertNoRunStart(t, runner)

	for i := 2; i < jobCount; i++ {
		runner.Release()
		start := waitRunStart(t, runner)
		jobID := int64(1001 + i)
		runnerID := int64(4241 + i)
		srv.markDelivered("owner/repo", jobID, start.RunnerName, runnerID)
		if start.Active > 2 {
			t.Fatalf("RunJob start %d active count = %d, want <= 2", i+1, start.Active)
		}
	}

	if maxActive := runner.MaxActive(); maxActive > 2 {
		t.Fatalf("max active RunJob goroutines = %d, want <= 2", maxActive)
	}
	if got := pendingDeliveryCount(srv); got != 0 {
		t.Fatalf("pending delivery count after drained deliveries = %d, want 0", got)
	}
}

func TestWebhookInProgressClearsOnlyMatchingPendingWorkflowJob(t *testing.T) {
	vm := &broker.WarmVM{Name: "gha-vm-1", Image: config.DefaultBaseImage}
	pool := &testPool{freeSlots: 2, leaseVM: vm}
	runner := &testRunner{ran: make(chan struct{}, 2)}
	clock := newMutableClock(time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC))
	canceller := &testCanceller{}
	srv := New(
		testSecret,
		newTestConfig("owner/repo"),
		nil,
		nil,
		pool,
		runner,
		WithClock(clock.Now),
		WithRunCanceller(canceller),
	)

	firstJob := webhookBodyWithJobID("queued", "owner/repo", []string{"self-hosted"}, 42, 1001)
	firstReq := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(firstJob)))
	firstReq.Header.Set("X-Hub-Signature-256", signBody(firstJob))
	firstResp := httptest.NewRecorder()
	srv.ServeHTTP(firstResp, firstReq)
	if firstResp.Code != http.StatusAccepted {
		t.Fatalf("first queued webhook status = %d, want 202", firstResp.Code)
	}

	secondJob := webhookBodyWithJobID("queued", "owner/repo", []string{"self-hosted"}, 42, 1002)
	secondReq := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(secondJob)))
	secondReq.Header.Set("X-Hub-Signature-256", signBody(secondJob))
	secondResp := httptest.NewRecorder()
	srv.ServeHTTP(secondResp, secondReq)
	if secondResp.Code != http.StatusAccepted {
		t.Fatalf("second queued webhook status = %d, want 202", secondResp.Code)
	}
	if got := pendingDeliveryCount(srv); got != 2 {
		t.Fatalf("pending delivery count after two jobs in one run = %d, want 2", got)
	}

	deliveredJob := webhookBodyWithJobIDAndRunner("in_progress", "owner/repo", []string{"self-hosted"}, 42, 1001, "gha-vm-1", 4242)
	deliveredReq := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(deliveredJob)))
	deliveredReq.Header.Set("X-Hub-Signature-256", signBody(deliveredJob))
	deliveredResp := httptest.NewRecorder()
	srv.ServeHTTP(deliveredResp, deliveredReq)
	if deliveredResp.Code != http.StatusNoContent {
		t.Fatalf("in_progress webhook status = %d, want 204", deliveredResp.Code)
	}
	if got := pendingDeliveryCount(srv); got != 1 {
		t.Fatalf("pending delivery count after one job is delivered = %d, want 1", got)
	}

	clock.Advance(servingDeadline + time.Second)
	srv.sweepPendingDeliveries(context.Background())

	canceller.mu.Lock()
	defer canceller.mu.Unlock()
	if len(canceller.calls) != 1 {
		t.Fatalf("cancel calls = %+v, want one", canceller.calls)
	}
	if canceller.calls[0].Repo != "owner/repo" || canceller.calls[0].RunID != 42 {
		t.Fatalf("cancel call = %+v, want owner/repo run 42", canceller.calls[0])
	}
}

func TestWebhookQueuedRetryKeepsOriginalPendingDeadline(t *testing.T) {
	vm := &broker.WarmVM{Name: "gha-vm-1", Image: config.DefaultBaseImage}
	pool := &testPool{freeSlots: 2, leaseVM: vm}
	runner := &testRunner{ran: make(chan struct{}, 2)}
	clock := newMutableClock(time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC))
	canceller := &testCanceller{}
	srv := New(
		testSecret,
		newTestConfig("owner/repo"),
		nil,
		nil,
		pool,
		runner,
		WithClock(clock.Now),
		WithRunCanceller(canceller),
	)

	body := webhookBodyWithJobID("queued", "owner/repo", []string{"self-hosted"}, 42, 1001)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("first queued webhook status = %d, want 202", w.Code)
	}

	clock.Advance(servingDeadline - time.Second)
	retryReq := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	retryReq.Header.Set("X-Hub-Signature-256", signBody(body))
	retryResp := httptest.NewRecorder()
	srv.ServeHTTP(retryResp, retryReq)
	if retryResp.Code != http.StatusAccepted {
		t.Fatalf("retry queued webhook status = %d, want 202", retryResp.Code)
	}

	clock.Advance(2 * time.Second)
	srv.sweepPendingDeliveries(context.Background())

	canceller.mu.Lock()
	defer canceller.mu.Unlock()
	if len(canceller.calls) != 1 {
		t.Fatalf("cancel calls after original deadline = %+v, want one", canceller.calls)
	}
	if canceller.calls[0].Repo != "owner/repo" || canceller.calls[0].RunID != 42 {
		t.Fatalf("cancel call = %+v, want owner/repo run 42", canceller.calls[0])
	}
}

func TestServingDeadlineIsStuckPoolBackstop(t *testing.T) {
	if servingDeadline != 45*time.Minute {
		t.Fatalf("servingDeadline = %s, want 45m0s", servingDeadline)
	}
}

func TestDeliverySweeperCancelsExpiredPendingRun(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC))
	canceller := &testCanceller{}
	srv := New(
		testSecret,
		newTestConfig("owner/repo"),
		nil,
		nil,
		&testPool{},
		&testRunner{ran: make(chan struct{}, 1)},
		WithClock(clock.Now),
		WithRunCanceller(canceller),
	)
	srv.recordPendingDelivery("owner/repo", 4200, 42)
	clock.Advance(servingDeadline + time.Second)

	srv.sweepPendingDeliveries(context.Background())

	canceller.mu.Lock()
	defer canceller.mu.Unlock()
	if len(canceller.calls) != 1 {
		t.Fatalf("cancel calls = %+v, want one", canceller.calls)
	}
	if canceller.calls[0].Repo != "owner/repo" || canceller.calls[0].RunID != 42 {
		t.Fatalf("cancel call = %+v, want owner/repo run 42", canceller.calls[0])
	}
	if got := pendingDeliveryCount(srv); got != 0 {
		t.Fatalf("pending delivery count after cancel = %d, want 0", got)
	}
}

func TestDeliverySweeperDoesNotCancelDeliveredRun(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC))
	canceller := &testCanceller{}
	srv := New(
		testSecret,
		newTestConfig("owner/repo"),
		nil,
		nil,
		&testPool{},
		&testRunner{ran: make(chan struct{}, 1)},
		WithClock(clock.Now),
		WithRunCanceller(canceller),
	)
	srv.recordPendingDelivery("owner/repo", 4200, 42)
	srv.markDelivered("owner/repo", 4200, "gha-vm-42", 4242)
	clock.Advance(servingDeadline + time.Second)

	srv.sweepPendingDeliveries(context.Background())

	canceller.mu.Lock()
	defer canceller.mu.Unlock()
	if len(canceller.calls) != 0 {
		t.Fatalf("cancel calls = %+v, want none", canceller.calls)
	}
	if got := pendingDeliveryCount(srv); got != 0 {
		t.Fatalf("pending delivery count after delivery = %d, want 0", got)
	}
}

func TestWebhookInProgressClearsPendingDelivery(t *testing.T) {
	clock := newMutableClock(time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC))
	canceller := &testCanceller{}
	srv := New(
		testSecret,
		newTestConfig("owner/repo"),
		nil,
		nil,
		&testPool{},
		&testRunner{ran: make(chan struct{}, 1)},
		WithClock(clock.Now),
		WithRunCanceller(canceller),
	)
	srv.recordPendingDelivery("owner/repo", 7042, 42)

	body := webhookBodyWithRunner("in_progress", "owner/repo", []string{"self-hosted"}, 42, "gha-vm-42", 4242)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if got := pendingDeliveryCount(srv); got != 0 {
		t.Fatalf("pending delivery count after in_progress = %d, want 0", got)
	}
}

func TestWebhookQueuedWithoutReservationServesDefaultImage(t *testing.T) {
	// A pool-labeled job is served on the default image because dispatch resolves
	// the image at serve time.
	vm := &broker.WarmVM{Name: "vm-1", Image: config.DefaultBaseImage}
	pool := &testPool{freeSlots: 2, leaseVM: vm}
	runner := newBlockingRunner(1)
	defer runner.Release()
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, pool, runner)

	body := webhookBody("queued", "owner/repo", []string{"self-hosted"}, 7)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 serving the default image without a reservation, got %d", w.Code)
	}
	start := waitRunStart(t, runner)
	srv.markDelivered("owner/repo", 7007, start.RunnerName, 4242)
	runner.Release()
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.leasedImages) != 1 || pool.leasedImages[0] != config.DefaultBaseImage {
		t.Fatalf("leased images = %v, want %q", pool.leasedImages, config.DefaultBaseImage)
	}
}

func TestWebhookNonPostMethodReturns405(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{}, &testRunner{ran: make(chan struct{}, 1)})
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

// --- capacity handler tests ---

func TestCapacityDisallowedRepo(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 2}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=other/repo&run_id=1", nil)
	req.Header.Set("Authorization", "Bearer test-capacity-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp capacityResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if resp.Available {
		t.Fatal("expected available=false for disallowed repo")
	}
}

func TestCapacityAvailable(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 2}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&os=tahoe&xcode=26.5", nil)
	req.Header.Set("Authorization", "Bearer test-capacity-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp capacityResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if !resp.Available {
		t.Fatal("expected available=true when pool has free slots")
	}
}

func TestCapacityUnmappedImageReturnsUnavailable(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 2}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&os=tahoe&xcode=raw", nil)
	req.Header.Set("Authorization", "Bearer test-capacity-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp capacityResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if resp.Available {
		t.Fatal("expected available=false for unmapped image")
	}
}

func TestCapacityNotAvailable(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 0}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo", nil)
	req.Header.Set("Authorization", "Bearer test-capacity-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp capacityResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if resp.Available {
		t.Fatal("expected available=false when pool has no free slots")
	}
}

// --- capacity bearer token tests ---

func TestCapacityNoHeaderReturns401(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 2}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=10", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with no auth header, got %d", w.Code)
	}
}

func TestCapacityWrongTokenReturns401(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 2}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=11", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong token, got %d", w.Code)
	}
}

func TestCapacityCorrectTokenReturns200AndAvailable(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 2}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo", nil)
	req.Header.Set("Authorization", "Bearer test-capacity-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with correct token, got %d", w.Code)
	}
	var resp capacityResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if !resp.Available {
		t.Fatal("expected available=true when pool has free slots")
	}
}

func TestCapacityEmptyTokenAlwaysReturns401(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{freeSlots: 2}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=13", nil)
	req.Header.Set("Authorization", "Bearer any-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when server has no token configured, got %d", w.Code)
	}
}

// --- webhook IP allowlist tests ---

// mustParseCIDR is a test helper that parses a CIDR or fails the test.
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, allowed, &testPool{freeSlots: 2}, &testRunner{ran: make(chan struct{}, 1)})
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, allowed, &testPool{freeSlots: 2}, &testRunner{ran: make(chan struct{}, 1)})
	body := webhookBody("queued", "owner/repo", []string{"self-hosted"}, 99)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", "sha256=badbadbadbad")
	req.Header.Set("CF-Connecting-IP", "192.30.252.1")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	// IP is allowed, so we reach the HMAC check and get 401 for the bad signature.
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 (bad HMAC after IP allowed), got %d", w.Code)
	}
}

func TestWebhookEmptyCIDRsSkipsIPCheck(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{}, &testRunner{ran: make(chan struct{}, 1)})
	body := webhookBody("in_progress", "owner/repo", []string{"self-hosted"}, 42)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	// No CF-Connecting-IP: falls back to RemoteAddr; IP guard is skipped anyway.
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	// IP check skipped, HMAC passes, action=in_progress -> 204.
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 (IP check skipped, non-queued event), got %d", w.Code)
	}
}

// --- healthz ---

func TestHealthz(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestClaimNextPendingIsAtomicAcrossConcurrentDrains(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{freeSlots: 2}, &testRunner{ran: make(chan struct{}, 1)})
	srv.recordPendingDelivery("owner/repo", 1, 100)
	srv.recordPendingDelivery("owner/repo", 2, 100)

	k1, _, ok1 := srv.claimNextPending()
	k2, _, ok2 := srv.claimNextPending()
	if !ok1 || !ok2 {
		t.Fatalf("expected two claims, got ok1=%v ok2=%v", ok1, ok2)
	}
	if k1.jobID == k2.jobID {
		t.Fatalf("two claims picked the same pending entry %d; selection is not atomic", k1.jobID)
	}
	if _, _, ok3 := srv.claimNextPending(); ok3 {
		t.Fatal("expected no undispatched entry left after both were claimed")
	}

	srv.releasePending(k1)
	kr, _, okr := srv.claimNextPending()
	if !okr || kr.jobID != k1.jobID {
		t.Fatalf("expected released entry %d to be claimable again, got ok=%v key=%d", k1.jobID, okr, kr.jobID)
	}
}
