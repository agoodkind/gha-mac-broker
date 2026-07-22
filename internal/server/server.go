// Package server wires the runner pool into HTTP handlers for the webhook,
// capacity, and health endpoints.
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

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/hostedload"
	"goodkind.io/gha-mac-broker/internal/hoststats"
	"goodkind.io/gha-mac-broker/internal/runnerpool"
)

const maxBodyBytes = 1 << 20

// pullRequestCancelTimeout bounds the background cancellation of a closed pull
// request's runs. The webhook handler acks immediately and does this work on a
// detached context, so GitHub closing the delivery connection at its ~10s
// deadline cannot abort the token lookup, listing, and per-run cancels midway.
const pullRequestCancelTimeout = 60 * time.Second

// pooler is the subset of runnerpool.Pool used by the server.
type pooler interface {
	Enqueue(ctx context.Context, job runnerpool.Job) error
	Ready() bool
	Status(ctx context.Context) (runnerpool.Snapshot, []runnerpool.WorkerView)
	CancelRun(jobID int64)
}

// statsProvider is the subset of hoststats.Sampler used by the server.
type statsProvider interface {
	Latest() hoststats.Sample
}

// runCanceller cancels the GitHub workflow runs a closed pull request leaves in
// flight. *ghapp.Client satisfies it. It is nil when no GitHub client is wired,
// in which case a pull_request close is acknowledged and no cancellation runs.
type runCanceller interface {
	CancelActiveRunsForHeadSHA(ctx context.Context, repo, headSHA string) (int, error)
}

type webhookAction string

const (
	webhookActionQueued     webhookAction = "queued"
	webhookActionInProgress webhookAction = "in_progress"
	webhookActionCompleted  webhookAction = "completed"
	// webhookActionClosed is the pull_request action for both a merge and a plain
	// close. GitHub never cancels a pull request's in-flight runs on either, so the
	// broker cancels them itself (see handlePullRequestClose).
	webhookActionClosed webhookAction = "closed"
)

// githubEventHeader names the webhook event, so the handler can tell a
// pull_request delivery from a workflow_job delivery even though both carry an
// action field. A workflow_job delivery leaves this unmatched and keeps the
// existing action switch.
// The canonical MIME form is "X-Github-Event"; net/http canonicalizes header keys
// on parse and lookup, so this matches GitHub's "X-GitHub-Event" delivery header.
const githubEventHeader = "X-Github-Event"

const eventPullRequest = "pull_request"

type webhookPayload struct {
	Action      webhookAction   `json:"action"`
	Repository  webhookRepo     `json:"repository"`
	WorkflowJob webhookJobField `json:"workflow_job"`
	PullRequest webhookPRField  `json:"pull_request"`
}

type webhookRepo struct {
	FullName string `json:"full_name"`
}

type webhookJobField struct {
	ID         int64    `json:"id"`
	Labels     []string `json:"labels"`
	RunID      int64    `json:"run_id"`
	Status     string   `json:"status"`
	Conclusion string   `json:"conclusion"`
	RunnerName string   `json:"runner_name"`
	RunnerID   int64    `json:"runner_id"`
}

// webhookPRField is the subset of a pull_request payload the broker reads to find
// the run a closed pull request left in flight.
type webhookPRField struct {
	Number int `json:"number"`
	Head   struct {
		SHA string `json:"sha"`
	} `json:"head"`
}

type capacityResponse struct {
	Available  bool `json:"available"`
	HostedFree bool `json:"hosted_free"`
}

type statusResponse struct {
	Snapshot  runnerpool.Snapshot     `json:"snapshot"`
	Workers   []runnerpool.WorkerView `json:"workers"`
	HostStats hoststats.Sample        `json:"host_stats"`
}

// Server handles the /webhook, /capacity, /status, and /healthz endpoints.
type Server struct {
	mux           *http.ServeMux
	mu            sync.RWMutex
	webhookKey    []byte
	capacityToken []byte
	webhookCIDRs  []*net.IPNet
	cfg           *config.Config
	pool          pooler
	hostedTracker *hostedload.Tracker
	stats         statsProvider
	runs          runCanceller
}

// Option configures a Server built by New.
type Option func(*Server)

// WithRunCanceller wires the GitHub run canceller used to cancel a pull request's
// in-flight runs when it merges or closes. Without it, a pull_request close is
// acknowledged and no run is cancelled.
func WithRunCanceller(rc runCanceller) Option {
	return func(s *Server) { s.runs = rc }
}

type serverConfig struct {
	webhookKey    []byte
	capacityToken []byte
	webhookCIDRs  []*net.IPNet
	cfg           *config.Config
}

// New builds a Server and registers its routes on an internal mux.
func New(webhookKey []byte, cfg *config.Config, capacityToken []byte, webhookCIDRs []*net.IPNet, p pooler, hostedTracker *hostedload.Tracker, stats statsProvider, opts ...Option) *Server {
	s := &Server{
		mux:           http.NewServeMux(),
		mu:            sync.RWMutex{},
		webhookKey:    webhookKey,
		capacityToken: capacityToken,
		webhookCIDRs:  webhookCIDRs,
		cfg:           cfg,
		pool:          p,
		hostedTracker: hostedTracker,
		stats:         stats,
		runs:          nil,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.mux.HandleFunc("/webhook", s.handleWebhook)
	s.mux.HandleFunc("/capacity", s.handleCapacity)
	s.mux.HandleFunc("/status", s.handleStatus)
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	if len(webhookCIDRs) == 0 {
		slog.Warn("webhook IP guard disabled: no CIDRs configured")
	}
	return s
}

// Reconfigure swaps the config dependent request state used by live handlers.
func (s *Server) Reconfigure(webhookKey []byte, cfg *config.Config, capacityToken []byte, webhookCIDRs []*net.IPNet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.webhookKey = append([]byte(nil), webhookKey...)
	s.capacityToken = append([]byte(nil), capacityToken...)
	s.webhookCIDRs = append([]*net.IPNet(nil), webhookCIDRs...)
	s.cfg = cfg
	if len(webhookCIDRs) == 0 {
		slog.Warn("webhook IP guard disabled: no CIDRs configured")
	}
}

func (s *Server) configSnapshot() serverConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return serverConfig{
		webhookKey:    s.webhookKey,
		capacityToken: s.capacityToken,
		webhookCIDRs:  s.webhookCIDRs,
		cfg:           s.cfg,
	}
}

// ServeHTTP implements [http.Handler] by delegating to the internal mux.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	slog.DebugContext(r.Context(), "http request", "method", r.Method, "path", r.URL.Path)
	s.mux.ServeHTTP(w, r)
}

// handleWebhook processes GitHub workflow_job and pull_request webhook
// deliveries.
func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	liveConfig := s.configSnapshot()
	if !s.checkWebhookIP(w, r, liveConfig) {
		return
	}
	slog.InfoContext(r.Context(), "webhook received")
	body, ok := s.readVerifiedBody(w, r, liveConfig)
	if !ok {
		return
	}
	var payload webhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		slog.WarnContext(r.Context(), "webhook unmarshal failed", "err", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if r.Header.Get(githubEventHeader) == eventPullRequest {
		s.handlePullRequestClose(w, r, payload)
		return
	}
	switch payload.Action {
	case webhookActionQueued:
		s.dispatchJob(w, r, payload, liveConfig)
	case webhookActionInProgress:
		slog.DebugContext(r.Context(), "workflow job in progress", "repo", payload.Repository.FullName, "run_id", payload.WorkflowJob.RunID, "runner", payload.WorkflowJob.RunnerName, "runner_id", payload.WorkflowJob.RunnerID)
		if hostedload.IsHostedMacOSJob(payload.WorkflowJob.Labels, liveConfig.cfg.Labels) {
			s.hostedTracker.MarkInProgress(payload.WorkflowJob.ID)
		}
		w.WriteHeader(http.StatusNoContent)
	case webhookActionCompleted:
		slog.DebugContext(r.Context(), "workflow job completed", "repo", payload.Repository.FullName, "run_id", payload.WorkflowJob.RunID, "job_id", payload.WorkflowJob.ID, "status", payload.WorkflowJob.Status, "conclusion", payload.WorkflowJob.Conclusion)
		s.hostedTracker.MarkCompleted(payload.WorkflowJob.ID)
		if payload.WorkflowJob.Conclusion == "cancelled" || payload.WorkflowJob.Conclusion == "skipped" {
			s.pool.CancelRun(payload.WorkflowJob.ID)
		}
		w.WriteHeader(http.StatusNoContent)
	case webhookActionClosed:
		// A pull_request close is handled above via the event header and returns
		// before this switch. A workflow_job delivery never carries this action, so
		// reaching here is a no-op.
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// handlePullRequestClose cancels the CI runs a pull request leaves in flight when
// it merges or closes. GitHub cancels an in-progress run only when a newer run
// enters the same concurrency group, which a merge or close never does, so an
// admin-merged pull request otherwise keeps its gates running and its pool slot
// held. Selecting runs by the pull request head sha never touches the merge
// commit's run on the default branch, which carries a different sha. The work is
// best-effort: any failure is logged and the webhook still returns 204 so GitHub
// does not retry a partial cancel.
func (s *Server) handlePullRequestClose(w http.ResponseWriter, r *http.Request, payload webhookPayload) {
	ctx := r.Context()
	if payload.Action != webhookActionClosed {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	repo := payload.Repository.FullName
	headSHA := payload.PullRequest.Head.SHA
	if repo == "" || headSHA == "" {
		slog.WarnContext(ctx, "pull_request closed webhook missing repo or head sha", "repo", repo, "pr", payload.PullRequest.Number)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if s.runs == nil {
		slog.WarnContext(ctx, "pull_request closed but no run canceller wired; skipping", "repo", repo, "pr", payload.PullRequest.Number)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// Ack immediately and cancel on a detached context, so a slow GitHub API
	// sequence is not aborted when GitHub closes the webhook delivery at its
	// deadline. Cancellation is idempotent, so a redelivered close is harmless.
	// WithoutCancel keeps the request's log values while dropping its cancellation.
	bg := context.WithoutCancel(ctx)
	pr := payload.PullRequest.Number
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				slog.ErrorContext(bg, "panic cancelling closed pull request runs", "err", fmt.Errorf("panic: %v", rec), "repo", repo, "pr", pr, "head_sha", headSHA)
			}
		}()
		s.cancelClosedPullRequestRuns(bg, repo, headSHA, pr)
	}()
	w.WriteHeader(http.StatusNoContent)
}

// cancelClosedPullRequestRuns cancels a closed pull request's in-flight runs on a
// context bounded by pullRequestCancelTimeout. It logs the outcome; a failure to
// cancel one or more runs is logged at warn, since the webhook has already been
// acked and there is no delivery to fail.
func (s *Server) cancelClosedPullRequestRuns(ctx context.Context, repo, headSHA string, pr int) {
	ctx, cancel := context.WithTimeout(ctx, pullRequestCancelTimeout)
	defer cancel()
	cancelled, err := s.runs.CancelActiveRunsForHeadSHA(ctx, repo, headSHA)
	if err != nil {
		slog.WarnContext(ctx, "cancel in-flight runs for closed pull request failed", "err", err, "repo", repo, "pr", pr, "head_sha", headSHA, "cancelled", cancelled)
		return
	}
	slog.InfoContext(ctx, "cancelled in-flight runs for closed pull request", "repo", repo, "pr", pr, "head_sha", headSHA, "cancelled", cancelled)
}

func (s *Server) readVerifiedBody(w http.ResponseWriter, r *http.Request, liveConfig serverConfig) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		slog.WarnContext(r.Context(), "webhook body read error", "err", err)
		http.Error(w, "read error", http.StatusBadRequest)
		return nil, false
	}
	sig := r.Header.Get("X-Hub-Signature-256")
	if !verifySignature(liveConfig.webhookKey, body, sig) {
		slog.InfoContext(r.Context(), "webhook rejected", "reason", "bad signature")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return nil, false
	}
	return body, true
}

func (s *Server) dispatchJob(w http.ResponseWriter, r *http.Request, payload webhookPayload, liveConfig serverConfig) {
	ctx := r.Context()
	repo := payload.Repository.FullName
	if !hasLabel(liveConfig.cfg, payload.WorkflowJob.Labels) {
		slog.InfoContext(ctx, "webhook ignored", "reason", "no matching label", "repo", repo, "labels", payload.WorkflowJob.Labels)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if _, resolved := liveConfig.cfg.ResolveImage("", ""); !resolved {
		slog.WarnContext(ctx, "no default image configured; ignoring pool job", "repo", repo, "run_id", payload.WorkflowJob.RunID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	job := runnerpool.Job{
		Repo:  repo,
		JobID: payload.WorkflowJob.ID,
		RunID: payload.WorkflowJob.RunID,
	}
	if err := s.pool.Enqueue(ctx, job); err != nil {
		slog.WarnContext(ctx, "runner pool enqueue failed", "err", err, "repo", repo, "run_id", payload.WorkflowJob.RunID, "job_id", payload.WorkflowJob.ID)
		http.Error(w, "pool unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleCapacity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// /capacity is intentionally public. It returns only a coarse availability
	// boolean and takes no action, so any consumer can probe it with no per-repo
	// secret, which lets swift-makefile route every consumer to the pool from one
	// place. The webhook that actually provisions runners stays HMAC-guarded, and
	// /status stays bearer-guarded because it exposes worker internals.
	repo := r.URL.Query().Get("repo")
	macos := r.URL.Query().Get("os")
	xcode := r.URL.Query().Get("xcode")
	slog.DebugContext(r.Context(), "capacity request", "repo", repo, "os", macos, "xcode", xcode)
	liveConfig := s.configSnapshot()
	// hosted_free reports whether the account is below its GitHub-hosted macOS
	// concurrency limit, so a consumer can spill to hosted runners even when the
	// pool cannot serve the image. A non-positive limit means unbounded, so it is
	// always free. Compute it once and include it in both responses below.
	limit := liveConfig.cfg.HostedMacOSConcurrencyLimit
	hostedFree := limit <= 0 || s.hostedTracker.Count() < limit
	// Available only when the requested image resolves to the exact image the
	// pool is running. The pool warms every VM on cfg.Tart.BaseImage, so a
	// mappable-but-different tag cannot be served and must route to hosted.
	tag, ok := liveConfig.cfg.ResolveImage(macos, xcode)
	if !ok || tag != liveConfig.cfg.Tart.BaseImage {
		writeJSON(w, capacityResponse{Available: false, HostedFree: hostedFree})
		return
	}
	writeJSON(w, capacityResponse{Available: s.pool.Ready(), HostedFree: hostedFree})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.checkBearerToken(w, r) {
		return
	}
	var resp statusResponse
	resp.Snapshot, resp.Workers = s.pool.Status(r.Context())
	if s.stats != nil {
		resp.HostStats = s.stats.Latest()
	}
	writeJSON(w, resp)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	slog.DebugContext(r.Context(), "healthz")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func hasLabel(cfg *config.Config, jobLabels []string) bool {
	for _, jobLabel := range jobLabels {
		for _, serverLabel := range cfg.Labels {
			if strings.EqualFold(jobLabel, serverLabel) {
				return true
			}
		}
	}
	return false
}

func (s *Server) checkBearerToken(w http.ResponseWriter, r *http.Request) bool {
	liveConfig := s.configSnapshot()
	if len(liveConfig.capacityToken) == 0 {
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
	if subtle.ConstantTimeCompare(provided, liveConfig.capacityToken) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func (s *Server) checkWebhookIP(w http.ResponseWriter, r *http.Request, liveConfig serverConfig) bool {
	if len(liveConfig.webhookCIDRs) == 0 {
		return true
	}
	ipStr := webhookClientIP(r)
	ip := net.ParseIP(ipStr)
	if ip == nil {
		slog.WarnContext(r.Context(), "webhook: unparseable client IP", "raw", ipStr)
		http.Error(w, "forbidden", http.StatusForbidden)
		return false
	}
	for _, cidr := range liveConfig.webhookCIDRs {
		if cidr.Contains(ip) {
			return true
		}
	}
	slog.WarnContext(r.Context(), "webhook: client IP not in allowlist", "ip", ipStr)
	http.Error(w, "forbidden", http.StatusForbidden)
	return false
}

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

type jsonResponse interface {
	capacityResponse | statusResponse
}

func writeJSON[T jsonResponse](w http.ResponseWriter, payload T) {
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

func verifySignature(secret []byte, body []byte, sigHeader string) bool {
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
