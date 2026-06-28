package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	freeSlots int
	leaseVM   *broker.WarmVM
	leaseErr  error
	mu        sync.Mutex
	recycled  []*broker.WarmVM
}

func (p *testPool) Lease(_ context.Context) (*broker.WarmVM, error) {
	return p.leaseVM, p.leaseErr
}

func (p *testPool) FreeSlots() int { return p.freeSlots }

func (p *testPool) Recycle(_ context.Context, vm *broker.WarmVM) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recycled = append(p.recycled, vm)
}

type testStore struct {
	mu           sync.Mutex
	reserved     map[string]struct{}
	consumed     []string
	reserveAllow bool
}

func (s *testStore) Reserve(_ string, _ int) bool {
	return s.reserveAllow
}

func (s *testStore) Consume(runID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reserved == nil {
		return false
	}
	if _, ok := s.reserved[runID]; ok {
		delete(s.reserved, runID)
		s.consumed = append(s.consumed, runID)
		return true
	}
	return false
}

type testRunner struct {
	ran chan struct{}
	err error
}

func (r *testRunner) RunJob(_ context.Context, _ *broker.WarmVM, _, _ string) error {
	r.ran <- struct{}{}
	return r.err
}

// testSecret is the webhook HMAC secret used across all handler tests.
var testSecret = []byte("test-secret")

// newTestConfig returns a minimal config with one allowed repo and labels.
func newTestConfig(allowedRepo string) *config.Config {
	return &config.Config{
		ListenAddr:   ":8080",
		App:          config.AppConfig{AppID: "1", PrivateKeyPath: "/tmp/key", WebhookSecretPath: "/tmp/secret"},
		Tart:         config.TartConfig{Binary: "tart", GoldenImage: "golden", VMNamePrefix: "gha", SSHKeyPath: "/tmp/id_rsa", SSHUser: "admin", CacheDir: ""},
		Labels:       []string{"self-hosted", "macOS"},
		AllowedRepos: []string{allowedRepo},
		PoolSize:     2,
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
	payload := webhookPayload{
		Action:      action,
		Repository:  webhookRepo{FullName: repo},
		WorkflowJob: webhookJobField{Labels: labels, RunID: runID},
	}
	b, _ := json.Marshal(payload)
	return b
}

func TestWebhookBadSignatureReturns401(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), &testPool{}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
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
	srv := New(testSecret, newTestConfig("owner/repo"), &testPool{}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
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
	srv := New(testSecret, newTestConfig("owner/repo"), &testPool{freeSlots: 2}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
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
	srv := New(testSecret, newTestConfig("owner/repo"), &testPool{freeSlots: 2}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
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
	vm := &broker.WarmVM{Name: "vm-1", Host: "127.0.0.1"}
	pool := &testPool{freeSlots: 2, leaseVM: vm}
	runner := &testRunner{ran: make(chan struct{}, 1)}
	srv := New(testSecret, newTestConfig("owner/repo"), pool, &testStore{}, runner)

	body := webhookBody("queued", "owner/repo", []string{"self-hosted"}, 7)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}

	// Wait for the job goroutine to call RunJob.
	select {
	case <-runner.ran:
	case <-time.After(2 * time.Second):
		t.Fatal("RunJob was not called within timeout")
	}
}

func TestWebhookNonPostMethodReturns405(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), &testPool{}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
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
	srv := New(testSecret, newTestConfig("owner/repo"), &testPool{freeSlots: 2}, &testStore{reserveAllow: true}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=other/repo&run_id=1", nil)
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
	srv := New(testSecret, newTestConfig("owner/repo"), &testPool{freeSlots: 2}, &testStore{reserveAllow: true}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=2", nil)
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

func TestCapacityNotAvailable(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), &testPool{freeSlots: 0}, &testStore{reserveAllow: false}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=3", nil)
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

func TestCapacityEmptyRunIDReturns400(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), &testPool{freeSlots: 2}, &testStore{reserveAllow: true}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty run_id, got %d", w.Code)
	}
}

func TestCapacityNonNumericRunIDReturns400(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), &testPool{freeSlots: 2}, &testStore{reserveAllow: true}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=notanumber", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-numeric run_id, got %d", w.Code)
	}
}

// --- healthz ---

func TestHealthz(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), &testPool{}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}
