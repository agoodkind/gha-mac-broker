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
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/runnerpool"
)

const maxBodyBytes = 1 << 20

// pooler is the subset of runnerpool.Pool used by the server.
type pooler interface {
	Enqueue(ctx context.Context, job runnerpool.Job) error
	Ready() bool
	Status(ctx context.Context) (runnerpool.Snapshot, []runnerpool.WorkerView)
	CancelRun(jobID int64)
}

type webhookAction string

const (
	webhookActionQueued     webhookAction = "queued"
	webhookActionInProgress webhookAction = "in_progress"
	webhookActionCompleted  webhookAction = "completed"
)

type webhookPayload struct {
	Action      webhookAction   `json:"action"`
	Repository  webhookRepo     `json:"repository"`
	WorkflowJob webhookJobField `json:"workflow_job"`
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

type capacityResponse struct {
	Available bool `json:"available"`
}

type statusResponse struct {
	Snapshot runnerpool.Snapshot     `json:"snapshot"`
	Workers  []runnerpool.WorkerView `json:"workers"`
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
}

type serverConfig struct {
	webhookKey    []byte
	capacityToken []byte
	webhookCIDRs  []*net.IPNet
	cfg           *config.Config
}

// New builds a Server and registers its routes on an internal mux.
func New(webhookKey []byte, cfg *config.Config, capacityToken []byte, webhookCIDRs []*net.IPNet, p pooler) *Server {
	s := &Server{
		mux:           http.NewServeMux(),
		mu:            sync.RWMutex{},
		webhookKey:    webhookKey,
		capacityToken: capacityToken,
		webhookCIDRs:  webhookCIDRs,
		cfg:           cfg,
		pool:          p,
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

// handleWebhook processes GitHub workflow_job webhook deliveries.
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
	switch payload.Action {
	case webhookActionQueued:
		s.dispatchJob(w, r, payload, liveConfig)
	case webhookActionInProgress:
		slog.DebugContext(r.Context(), "workflow job in progress", "repo", payload.Repository.FullName, "run_id", payload.WorkflowJob.RunID, "runner", payload.WorkflowJob.RunnerName, "runner_id", payload.WorkflowJob.RunnerID)
		w.WriteHeader(http.StatusNoContent)
	case webhookActionCompleted:
		slog.DebugContext(r.Context(), "workflow job completed", "repo", payload.Repository.FullName, "run_id", payload.WorkflowJob.RunID, "job_id", payload.WorkflowJob.ID, "status", payload.WorkflowJob.Status, "conclusion", payload.WorkflowJob.Conclusion)
		if payload.WorkflowJob.Conclusion == "cancelled" || payload.WorkflowJob.Conclusion == "skipped" {
			s.pool.CancelRun(payload.WorkflowJob.ID)
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
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
	// Available only when the requested image resolves to the exact image the
	// pool is running. The pool warms every VM on cfg.Tart.BaseImage, so a
	// mappable-but-different tag cannot be served and must route to hosted.
	tag, ok := liveConfig.cfg.ResolveImage(macos, xcode)
	if !ok || tag != liveConfig.cfg.Tart.BaseImage {
		writeJSON(w, capacityResponse{Available: false})
		return
	}
	writeJSON(w, capacityResponse{Available: s.pool.Ready()})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.checkBearerToken(w, r) {
		return
	}
	snapshot, workers := s.pool.Status(r.Context())
	writeJSON(w, statusResponse{
		Snapshot: snapshot,
		Workers:  workers,
	})
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
