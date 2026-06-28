// Package server wires the pool, reservation store, and broker binder into
// HTTP handlers for the webhook, capacity, and health endpoints.
package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"

	"goodkind.io/gha-mac-broker/internal/broker"
	"goodkind.io/gha-mac-broker/internal/config"
)

// maxBodyBytes caps how much of a webhook body is read into memory.
const maxBodyBytes = 1 << 20

// pooler is the subset of pool.Pool used by the server.
type pooler interface {
	Lease(ctx context.Context) (*broker.WarmVM, error)
	FreeSlots() int
	Recycle(ctx context.Context, vm *broker.WarmVM)
}

// reserver is the subset of reservation.Store used by the server.
type reserver interface {
	Reserve(runID string, capacity int) bool
	Consume(runID string) bool
}

// jobRunner is the subset of broker.Binder used by the server.
type jobRunner interface {
	RunJob(ctx context.Context, vm *broker.WarmVM, repo, runnerName string) error
}

// webhookPayload is the relevant subset of a GitHub workflow_job webhook body.
type webhookPayload struct {
	Action      string          `json:"action"`
	Repository  webhookRepo     `json:"repository"`
	WorkflowJob webhookJobField `json:"workflow_job"`
}

// webhookRepo holds the repository name from the webhook payload.
type webhookRepo struct {
	FullName string `json:"full_name"`
}

// webhookJobField holds the labels and run id from the workflow_job field.
type webhookJobField struct {
	Labels []string `json:"labels"`
	RunID  int64    `json:"run_id"`
}

// capacityResponse is the JSON body returned by GET /capacity.
type capacityResponse struct {
	Available bool `json:"available"`
}

// Server handles the /webhook, /capacity, and /healthz endpoints.
type Server struct {
	mux           *http.ServeMux
	secret        []byte
	capacityToken []byte
	webhookCIDRs  []*net.IPNet
	cfg           *config.Config
	pool          pooler
	store         reserver
	binder        jobRunner
}

// New builds a Server and registers its routes on an internal mux.
// capacityToken is the bearer token required on GET /capacity; a nil or empty
// token closes the endpoint (401 fail-safe). webhookCIDRs is the IP allowlist
// for POST /webhook; a nil or empty slice disables the IP guard.
func New(secret []byte, cfg *config.Config, capacityToken []byte, webhookCIDRs []*net.IPNet, p pooler, store reserver, binder jobRunner) *Server {
	s := &Server{
		mux:           http.NewServeMux(),
		secret:        secret,
		capacityToken: capacityToken,
		webhookCIDRs:  webhookCIDRs,
		cfg:           cfg,
		pool:          p,
		store:         store,
		binder:        binder,
	}
	s.mux.HandleFunc("/webhook", s.handleWebhook)
	s.mux.HandleFunc("/capacity", s.handleCapacity)
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	return s
}

// ServeHTTP implements [http.Handler] by delegating to the internal mux.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	slog.DebugContext(r.Context(), "http request", "method", r.Method, "path", r.URL.Path)
	s.mux.ServeHTTP(w, r)
}

// handleWebhook processes GitHub workflow_job webhook deliveries. It verifies
// the HMAC-SHA256 signature, ignores non-queued events, checks the repo
// allowlist and label set, and dispatches a job goroutine for queued events
// the broker can handle.
func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.checkWebhookIP(w, r) {
		return
	}
	slog.DebugContext(r.Context(), "webhook received")
	body, ok := s.readVerifiedBody(w, r)
	if !ok {
		return
	}
	var payload webhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		slog.WarnContext(r.Context(), "webhook unmarshal failed", "err", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if payload.Action != "queued" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.dispatchJob(w, r, payload)
}

// readVerifiedBody reads the request body up to maxBodyBytes and verifies the
// X-Hub-Signature-256 HMAC. It writes an error response and returns false on
// any failure.
func (s *Server) readVerifiedBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		slog.WarnContext(r.Context(), "webhook body read error", "err", err)
		http.Error(w, "read error", http.StatusBadRequest)
		return nil, false
	}
	sig := r.Header.Get("X-Hub-Signature-256")
	if !verifySignature(s.secret, body, sig) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return nil, false
	}
	return body, true
}

// dispatchJob checks the repo allowlist and label set for a queued job and,
// if the broker can handle it, leases a warm VM and launches a RunJob
// goroutine. It responds 202 on dispatch and 204 when the job is ignored.
func (s *Server) dispatchJob(w http.ResponseWriter, r *http.Request, payload webhookPayload) {
	ctx := r.Context()
	repo := payload.Repository.FullName
	if !s.cfg.RepoAllowed(repo) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !s.hasLabel(payload.WorkflowJob.Labels) {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	runID := strconv.FormatInt(payload.WorkflowJob.RunID, 10)
	if !s.store.Consume(runID) && s.pool.FreeSlots() <= 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	vm, err := s.pool.Lease(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "lease failed", "err", err, "repo", repo)
		http.Error(w, "no vm available", http.StatusServiceUnavailable)
		return
	}

	jobCtx := context.WithoutCancel(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(jobCtx, "job goroutine panic recovered", "err", fmt.Errorf("panic: %v", r), "vm", vm.Name)
			}
		}()
		defer s.pool.Recycle(jobCtx, vm)
		if err := s.binder.RunJob(jobCtx, vm, repo, vm.Name); err != nil {
			slog.WarnContext(jobCtx, "job failed", "err", err, "vm", vm.Name, "repo", repo)
		}
	}()

	w.WriteHeader(http.StatusAccepted)
}

// handleCapacity checks whether the pool has a free slot for a given repo and
// run_id. If so it records a reservation and returns {"available":true}.
// A missing or non-numeric run_id is rejected with 400 so a bad request
// cannot create a phantom reservation that wastes a pool slot until TTL.
func (s *Server) handleCapacity(w http.ResponseWriter, r *http.Request) {
	if !s.checkBearerToken(w, r) {
		return
	}
	repo := r.URL.Query().Get("repo")
	runID := r.URL.Query().Get("run_id")
	slog.DebugContext(r.Context(), "capacity request", "repo", repo, "run_id", runID)
	if !s.cfg.RepoAllowed(repo) {
		writeJSON(w, capacityResponse{Available: false})
		return
	}
	if runID == "" {
		http.Error(w, "run_id required", http.StatusBadRequest)
		return
	}
	if _, err := strconv.ParseInt(runID, 10, 64); err != nil {
		http.Error(w, "run_id must be numeric", http.StatusBadRequest)
		return
	}
	available := s.store.Reserve(runID, s.pool.FreeSlots())
	writeJSON(w, capacityResponse{Available: available})
}

// handleHealthz returns 200 "ok" so load balancers can probe liveness.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	slog.DebugContext(r.Context(), "healthz")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// hasLabel reports whether any of the job labels matches a broker label.
func (s *Server) hasLabel(jobLabels []string) bool {
	for _, jl := range jobLabels {
		for _, sl := range s.cfg.Labels {
			if strings.EqualFold(jl, sl) {
				return true
			}
		}
	}
	return false
}

// checkBearerToken verifies the Authorization: Bearer <token> header against
// the server's configured capacity token using a constant-time compare. An
// empty configured token always returns false (401 fail-safe): a server with
// no token configured never opens /capacity by accident.
func (s *Server) checkBearerToken(w http.ResponseWriter, r *http.Request) bool {
	if len(s.capacityToken) == 0 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	const bearerPrefix = "Bearer "
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, bearerPrefix) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	provided := []byte(strings.TrimPrefix(auth, bearerPrefix))
	if subtle.ConstantTimeCompare(provided, s.capacityToken) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

// checkWebhookIP extracts the real client IP from the request and confirms it
// falls within one of the configured webhook CIDRs. When the CIDR list is
// empty the check is skipped (dev/local mode) and a Warn is logged. Returns
// false and writes a 403 response when the IP is not allowed.
func (s *Server) checkWebhookIP(w http.ResponseWriter, r *http.Request) bool {
	if len(s.webhookCIDRs) == 0 {
		slog.WarnContext(r.Context(), "webhook IP guard disabled: no CIDRs configured")
		return true
	}
	ipStr := webhookClientIP(r)
	ip := net.ParseIP(ipStr)
	if ip == nil {
		slog.WarnContext(r.Context(), "webhook: unparseable client IP", "raw", ipStr)
		http.Error(w, "forbidden", http.StatusForbidden)
		return false
	}
	for _, cidr := range s.webhookCIDRs {
		if cidr.Contains(ip) {
			return true
		}
	}
	slog.WarnContext(r.Context(), "webhook: client IP not in allowlist", "ip", ipStr)
	http.Error(w, "forbidden", http.StatusForbidden)
	return false
}

// webhookClientIP returns the best-effort real client IP for a webhook
// request. It prefers the CF-Connecting-IP header set by cloudflared, then
// True-Client-IP, and falls back to the TCP RemoteAddr host.
func webhookClientIP(r *http.Request) string {
	if ip := r.Header.Get("Cf-Connecting-Ip"); ip != "" {
		return ip
	}
	if ip := r.Header.Get("True-Client-Ip"); ip != "" {
		return ip
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// writeJSON marshals payload and writes it as an application/json 200 response.
func writeJSON(w http.ResponseWriter, payload capacityResponse) {
	body, err := json.Marshal(payload)
	if err != nil {
		slog.Error("marshal failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// verifySignature reports whether the X-Hub-Signature-256 header value is a
// valid HMAC-SHA256 of body under secret.
func verifySignature(secret, body []byte, sigHeader string) bool {
	const prefix = "sha256="
	if !strings.HasPrefix(sigHeader, prefix) {
		return false
	}
	sig, err := hex.DecodeString(sigHeader[len(prefix):])
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	expected := mac.Sum(nil)
	return hmac.Equal(expected, sig)
}
