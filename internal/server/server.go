// Package server wires the pool and broker binder into HTTP handlers for the
// webhook, capacity, and health endpoints.
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
	"strings"
	"sync"
	"time"

	"goodkind.io/gha-mac-broker/internal/broker"
	"goodkind.io/gha-mac-broker/internal/config"
)

// maxBodyBytes caps how much of a webhook body is read into memory.
const maxBodyBytes = 1 << 20

// servingDeadline bounds how long a pool-routed workflow run may remain
// undelivered before the broker cancels the run for hosted retry.
const servingDeadline = 4 * time.Minute

// deliverySweepInterval is the periodic cancellation sweep cadence.
const deliverySweepInterval = 30 * time.Second

// pooler is the subset of pool.Pool used by the server.
type pooler interface {
	Lease(ctx context.Context, image string) (*broker.WarmVM, error)
	FreeSlots() int
	Recycle(ctx context.Context, vm *broker.WarmVM)
}

// jobRunner is the subset of broker.Binder used by the server.
type jobRunner interface {
	RunJob(ctx context.Context, vm *broker.WarmVM, repo, runnerName string) error
}

// runCanceller is the subset of ghapp.Client used by the delivery sweeper.
type runCanceller interface {
	CancelRun(ctx context.Context, repo string, runID int64) error
}

type webhookAction string

const (
	webhookActionQueued     webhookAction = "queued"
	webhookActionInProgress webhookAction = "in_progress"
	webhookActionCompleted  webhookAction = "completed"
)

// Option configures optional server collaborators.
type Option func(*Server)

// WithClock overrides the server clock used for delivery deadlines.
func WithClock(now func() time.Time) Option {
	return func(s *Server) {
		s.now = now
	}
}

// WithRunCanceller enables delivery-deadline workflow-run cancellation.
func WithRunCanceller(canceller runCanceller) Option {
	return func(s *Server) {
		s.canceller = canceller
	}
}

// webhookPayload is the relevant subset of a GitHub workflow_job webhook body.
type webhookPayload struct {
	Action      webhookAction   `json:"action"`
	Repository  webhookRepo     `json:"repository"`
	WorkflowJob webhookJobField `json:"workflow_job"`
}

// webhookRepo holds the repository name from the webhook payload.
type webhookRepo struct {
	FullName string `json:"full_name"`
}

// webhookJobField holds the labels and run id from the workflow_job field.
type webhookJobField struct {
	ID         int64    `json:"id"`
	Labels     []string `json:"labels"`
	RunID      int64    `json:"run_id"`
	Status     string   `json:"status"`
	Conclusion string   `json:"conclusion"`
	RunnerName string   `json:"runner_name"`
	RunnerID   int64    `json:"runner_id"`
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
	binder        jobRunner
	canceller     runCanceller
	now           func() time.Time
	pendingMu     sync.Mutex
	pending       map[pendingKey]pendingDelivery
}

// New builds a Server and registers its routes on an internal mux.
// capacityToken is the bearer token required on GET /capacity; a nil or empty
// token closes the endpoint (401 fail-safe). webhookCIDRs is the IP allowlist
// for POST /webhook; a nil or empty slice disables the IP guard.
func New(secret []byte, cfg *config.Config, capacityToken []byte, webhookCIDRs []*net.IPNet, p pooler, binder jobRunner, opts ...Option) *Server {
	s := &Server{
		mux:           http.NewServeMux(),
		secret:        secret,
		capacityToken: capacityToken,
		webhookCIDRs:  webhookCIDRs,
		cfg:           cfg,
		pool:          p,
		binder:        binder,
		canceller:     nil,
		now:           time.Now,
		pendingMu:     sync.Mutex{},
		pending:       make(map[pendingKey]pendingDelivery),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.mux.HandleFunc("/webhook", s.handleWebhook)
	s.mux.HandleFunc("/capacity", s.handleCapacity)
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	if len(webhookCIDRs) == 0 {
		slog.Warn("webhook IP guard disabled: no CIDRs configured")
	}
	return s
}

// ServeHTTP implements [http.Handler] by delegating to the internal mux.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	slog.DebugContext(r.Context(), "http request", "method", r.Method, "path", r.URL.Path)
	s.mux.ServeHTTP(w, r)
}

// StartDeliverySweeper launches the cancellation loop for overdue pool-routed
// workflow runs. It is a no-op when no run canceller is configured.
func (s *Server) StartDeliverySweeper(ctx context.Context) {
	if s.canceller == nil {
		slog.WarnContext(ctx, "delivery sweeper disabled: no run canceller configured")
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "delivery sweeper panic recovered", "err", fmt.Errorf("panic: %v", r))
			}
		}()
		ticker := time.NewTicker(deliverySweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.sweepPendingDeliveries(ctx)
			}
		}
	}()
}

// handleWebhook processes GitHub workflow_job webhook deliveries. It verifies
// the HMAC-SHA256 signature, branches on the workflow_job action, dispatches
// queued pool jobs, and records in_progress delivery confirmations.
func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.checkWebhookIP(w, r) {
		return
	}
	slog.InfoContext(r.Context(), "webhook received")
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
	switch payload.Action {
	case webhookActionQueued:
		s.dispatchJob(w, r, payload)
	case webhookActionInProgress:
		s.handleJobInProgress(r.Context(), payload)
		w.WriteHeader(http.StatusNoContent)
	case webhookActionCompleted:
		slog.DebugContext(r.Context(), "workflow job completed", "repo", payload.Repository.FullName, "run_id", payload.WorkflowJob.RunID, "status", payload.WorkflowJob.Status, "conclusion", payload.WorkflowJob.Conclusion)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
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
		slog.InfoContext(r.Context(), "webhook rejected", "reason", "bad signature")
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
		slog.InfoContext(ctx, "webhook ignored", "reason", "repo not allowed", "repo", repo)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !s.hasLabel(payload.WorkflowJob.Labels) {
		slog.InfoContext(ctx, "webhook ignored", "reason", "no matching label", "repo", repo, "labels", payload.WorkflowJob.Labels)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	s.recordPendingDelivery(repo, payload.WorkflowJob.ID, payload.WorkflowJob.RunID)
	// The capacity check is a read-only probe that holds no reservation, so the
	// broker resolves the image from config here at serve time. With a single
	// golden this is always the default image. Lease enforces the warm budget,
	// so a job that finds the slot taken releases to hosted, never overbooking.
	image, resolved := s.cfg.ResolveImage("", "")
	if !resolved {
		slog.WarnContext(ctx, "no default image configured; ignoring pool job", "repo", repo, "run_id", payload.WorkflowJob.RunID)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Lease on a detached context: an on-demand warm boots `tart run` as a
	// CommandContext child, so tying it to the request context would kill the VM
	// the instant this handler returns 202, before RunJob can exec into it.
	jobCtx := context.WithoutCancel(ctx)
	vm, err := s.pool.Lease(jobCtx, image)
	if err != nil {
		slog.ErrorContext(ctx, "lease failed", "err", err, "repo", repo)
		http.Error(w, "no vm available", http.StatusServiceUnavailable)
		return
	}

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

func (s *Server) handleJobInProgress(ctx context.Context, payload webhookPayload) {
	repo := payload.Repository.FullName
	if !s.cfg.RepoAllowed(repo) {
		slog.DebugContext(ctx, "in_progress webhook ignored", "reason", "repo not allowed", "repo", repo)
		return
	}
	s.markDelivered(repo, payload.WorkflowJob.ID, payload.WorkflowJob.RunnerName, payload.WorkflowJob.RunnerID)
}

type pendingKey struct {
	jobID int64
}

type pendingDelivery struct {
	repo     string
	runID    int64
	deadline time.Time
}

type expiredDelivery struct {
	key      pendingKey
	delivery pendingDelivery
}

func (s *Server) recordPendingDelivery(repo string, jobID int64, runID int64) {
	if jobID == 0 || runID == 0 {
		return
	}
	key := pendingKey{jobID: jobID}
	delivery := pendingDelivery{
		repo:     repo,
		runID:    runID,
		deadline: s.now().Add(servingDeadline),
	}
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if _, ok := s.pending[key]; ok {
		return
	}
	s.pending[key] = delivery
}

func (s *Server) markDelivered(repo string, jobID int64, runnerName string, runnerID int64) {
	if jobID == 0 || !s.isPoolRunner(runnerName) {
		return
	}
	key := pendingKey{jobID: jobID}
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	delivery, ok := s.pending[key]
	if !ok || delivery.repo != repo {
		return
	}
	delete(s.pending, key)
	slog.Info("workflow job delivered to pool runner", "repo", repo, "run_id", delivery.runID, "job_id", jobID, "runner", runnerName, "runner_id", runnerID)
}

func (s *Server) isPoolRunner(runnerName string) bool {
	prefix := s.cfg.Tart.VMNamePrefix + "-"
	return strings.HasPrefix(runnerName, prefix)
}

func (s *Server) sweepPendingDeliveries(ctx context.Context) {
	if s.canceller == nil {
		return
	}
	expired := s.expiredDeliveries()
	for _, item := range expired {
		if err := s.canceller.CancelRun(ctx, item.delivery.repo, item.delivery.runID); err != nil {
			slog.ErrorContext(ctx, "cancel overdue workflow run failed", "err", err, "repo", item.delivery.repo, "run_id", item.delivery.runID, "reason", "serving deadline exceeded")
			continue
		}
		slog.WarnContext(ctx, "cancelled overdue workflow run", "repo", item.delivery.repo, "run_id", item.delivery.runID, "reason", "serving deadline exceeded")
		s.clearPendingDelivery(item.key)
	}
}

func (s *Server) expiredDeliveries() []expiredDelivery {
	now := s.now()
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	var expired []expiredDelivery
	for key, delivery := range s.pending {
		if now.Before(delivery.deadline) {
			continue
		}
		expired = append(expired, expiredDelivery{key: key, delivery: delivery})
	}
	return expired
}

func (s *Server) clearPendingDelivery(key pendingKey) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	delete(s.pending, key)
}

// handleCapacity reports whether the pool has a free warm slot for a given repo
// and requested image. It is a pure read: it holds nothing, so any number of
// probes leaves pool state unchanged and can never strand a slot. The webhook
// re-derives the image from config at serve time and Lease enforces the warm
// budget, so a job that finds the slot taken between this check and its webhook
// releases to hosted rather than overbooking.
func (s *Server) handleCapacity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.checkBearerToken(w, r) {
		return
	}
	repo := r.URL.Query().Get("repo")
	macos := r.URL.Query().Get("os")
	xcode := r.URL.Query().Get("xcode")
	slog.DebugContext(r.Context(), "capacity request", "repo", repo, "os", macos, "xcode", xcode)
	if !s.cfg.RepoAllowed(repo) {
		writeJSON(w, capacityResponse{Available: false})
		return
	}
	if _, ok := s.cfg.ResolveImage(macos, xcode); !ok {
		writeJSON(w, capacityResponse{Available: false})
		return
	}
	writeJSON(w, capacityResponse{Available: s.pool.FreeSlots() > 0})
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
	if len(auth) < len(bearerPrefix) || !strings.EqualFold(auth[:len(bearerPrefix)], bearerPrefix) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	provided := []byte(auth[len(bearerPrefix):])
	if subtle.ConstantTimeCompare(provided, s.capacityToken) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

// checkWebhookIP extracts the real client IP from the request and confirms it
// falls within one of the configured webhook CIDRs. When the CIDR list is
// empty the check is skipped (dev/local mode); New logs that once at startup.
// Returns false and writes a 403 response when the IP is not allowed.
func (s *Server) checkWebhookIP(w http.ResponseWriter, r *http.Request) bool {
	if len(s.webhookCIDRs) == 0 {
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
