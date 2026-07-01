package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	freeSlots    int
	leaseVM      *broker.WarmVM
	leaseErr     error
	mu           sync.Mutex
	leasedImages []string
	recycled     []*broker.WarmVM
}

func (p *testPool) Lease(_ context.Context, image string) (*broker.WarmVM, error) {
	p.mu.Lock()
	p.leasedImages = append(p.leasedImages, image)
	p.mu.Unlock()
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
	reserved     map[string]string
	reserveCalls []reserveCall
	consumed     []string
	reserveAllow bool
}

type reserveCall struct {
	RunID    string
	Image    string
	Capacity int
}

func (s *testStore) Reserve(runID, image string, capacity int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reserveCalls = append(s.reserveCalls, reserveCall{RunID: runID, Image: image, Capacity: capacity})
	if s.reserveAllow {
		if s.reserved == nil {
			s.reserved = make(map[string]string)
		}
		s.reserved[runID] = image
	}
	return s.reserveAllow
}

func (s *testStore) Consume(runID string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reserved == nil {
		return "", false
	}
	image, ok := s.reserved[runID]
	if ok {
		delete(s.reserved, runID)
		s.consumed = append(s.consumed, runID)
		return image, true
	}
	return "", false
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
	payload := webhookPayload{
		Action:      action,
		Repository:  webhookRepo{FullName: repo},
		WorkflowJob: webhookJobField{Labels: labels, RunID: runID},
	}
	b, _ := json.Marshal(payload)
	return b
}

func TestWebhookBadSignatureReturns401(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{freeSlots: 2}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{freeSlots: 2}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
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
	runner := &testRunner{ran: make(chan struct{}, 1)}
	store := &testStore{reserved: map[string]string{"7": config.DefaultBaseImage}}
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, pool, store, runner)

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

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.leasedImages) != 1 || pool.leasedImages[0] != config.DefaultBaseImage {
		t.Fatalf("leased images = %v, want %q", pool.leasedImages, config.DefaultBaseImage)
	}
}

func TestWebhookQueuedWithoutReservationServesDefaultImage(t *testing.T) {
	// A pool-labeled job whose reservation expired before its webhook arrived is
	// still served on the default image so slow delivery never strands it.
	vm := &broker.WarmVM{Name: "vm-1", Image: config.DefaultBaseImage}
	pool := &testPool{freeSlots: 2, leaseVM: vm}
	runner := &testRunner{ran: make(chan struct{}, 1)}
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, pool, &testStore{}, runner)

	body := webhookBody("queued", "owner/repo", []string{"self-hosted"}, 7)
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signBody(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 serving the default image without a reservation, got %d", w.Code)
	}
	select {
	case <-runner.ran:
	case <-time.After(2 * time.Second):
		t.Fatal("RunJob was not called within timeout")
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.leasedImages) != 1 || pool.leasedImages[0] != config.DefaultBaseImage {
		t.Fatalf("leased images = %v, want %q", pool.leasedImages, config.DefaultBaseImage)
	}
}

func TestWebhookNonPostMethodReturns405(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
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
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 2}, &testStore{reserveAllow: true}, &testRunner{ran: make(chan struct{}, 1)})
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
	store := &testStore{reserveAllow: true}
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 2}, store, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=2&os=tahoe&xcode=26.5", nil)
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
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.reserveCalls) != 1 || store.reserveCalls[0].Image != config.DefaultBaseImage {
		t.Fatalf("reserve calls = %+v, want image %q", store.reserveCalls, config.DefaultBaseImage)
	}
}

func TestCapacityUnmappedImageReturnsUnavailable(t *testing.T) {
	store := &testStore{reserveAllow: true}
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 2}, store, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=22&os=tahoe&xcode=raw", nil)
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
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.reserveCalls) != 0 {
		t.Fatalf("unmapped image should not reserve, got %+v", store.reserveCalls)
	}
}

func TestCapacityNotAvailable(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 0}, &testStore{reserveAllow: false}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=3", nil)
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

func TestCapacityEmptyRunIDReturns400(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 2}, &testStore{reserveAllow: true}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo", nil)
	req.Header.Set("Authorization", "Bearer test-capacity-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty run_id, got %d", w.Code)
	}
}

func TestCapacityNonNumericRunIDReturns400(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 2}, &testStore{reserveAllow: true}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=notanumber", nil)
	req.Header.Set("Authorization", "Bearer test-capacity-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-numeric run_id, got %d", w.Code)
	}
}

// --- capacity bearer token tests ---

func TestCapacityNoHeaderReturns401(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 2}, &testStore{reserveAllow: true}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=10", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with no auth header, got %d", w.Code)
	}
}

func TestCapacityWrongTokenReturns401(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 2}, &testStore{reserveAllow: true}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=11", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong token, got %d", w.Code)
	}
}

func TestCapacityCorrectTokenReturns200AndReserves(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), testCapacityToken, nil, &testPool{freeSlots: 2}, &testStore{reserveAllow: true}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/capacity?repo=owner/repo&run_id=12", nil)
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
		t.Fatal("expected available=true when pool has free slots and reservation allowed")
	}
}

func TestCapacityEmptyTokenAlwaysReturns401(t *testing.T) {
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{freeSlots: 2}, &testStore{reserveAllow: true}, &testRunner{ran: make(chan struct{}, 1)})
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, allowed, &testPool{freeSlots: 2}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, allowed, &testPool{freeSlots: 2}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
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
	srv := New(testSecret, newTestConfig("owner/repo"), nil, nil, &testPool{}, &testStore{}, &testRunner{ran: make(chan struct{}, 1)})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}
