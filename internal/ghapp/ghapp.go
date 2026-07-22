// Package ghapp authenticates as a GitHub App and mints the repo-scoped
// just-in-time runner config that lets a pre-booted VM register as an ephemeral
// self-hosted runner for a single repository and a single job.
//
// The flow is: sign a short-lived RS256 JWT with the App private key, exchange
// it for a repo-scoped installation access token, then call
// generate-jitconfig for the target repository. No third-party dependencies are
// used; RS256 signing is done with the standard library.
package ghapp

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"goodkind.io/gha-mac-broker/internal/hostedload"
)

// defaultAPIBase is the GitHub REST API root.
const defaultAPIBase = "https://api.github.com"

// jwtTTL is how long the App JWT stays valid. GitHub rejects App JWTs whose
// exp is more than 10 minutes ahead, so this stays comfortably under that.
const jwtTTL = 9 * time.Minute

// maxResponseBytes caps how much of a response body is read.
const maxResponseBytes = 1 << 20

// runnerListPageSize is the largest page size accepted by GitHub's runners API.
const runnerListPageSize = 100

// installationListPageSize is the largest page size used for App installations
// and installation repository lists.
const installationListPageSize = 100

// actionsListPageSize is the largest page size accepted by GitHub's Actions runs
// and jobs list endpoints.
const actionsListPageSize = 100

// Client authenticates as one GitHub App.
type Client struct {
	appID      string
	privateKey *rsa.PrivateKey
	http       *http.Client
	apiBase    string
	now        func() time.Time
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient overrides the HTTP client.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) { c.http = h }
}

// New builds a Client from an App ID and a PEM-encoded RSA private key.
func New(appID string, pemKey []byte, opts ...Option) (*Client, error) {
	key, err := unmarshalRSAPrivateKey(pemKey)
	if err != nil {
		return nil, err
	}
	client := &Client{
		appID:      appID,
		privateKey: key,
		http:       http.DefaultClient,
		apiBase:    defaultAPIBase,
		now:        time.Now,
	}
	for _, opt := range opts {
		opt(client)
	}
	return client, nil
}

// unmarshalRSAPrivateKey accepts a PKCS#1 or PKCS#8 PEM block.
func unmarshalRSAPrivateKey(pemKey []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemKey)
	if block == nil {
		return nil, errors.New("ghapp: no PEM block found in private key")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		slog.Error("parse private key failed", "err", err)
		return nil, fmt.Errorf("ghapp: parse private key: %w", err)
	}
	rsaKey, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("ghapp: private key is not RSA")
	}
	return rsaKey, nil
}

// jwtHeader is the fixed JOSE header for an RS256 App JWT.
type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// jwtClaims are the registered claims GitHub requires on an App JWT.
type jwtClaims struct {
	Iat int64  `json:"iat"`
	Exp int64  `json:"exp"`
	Iss string `json:"iss"`
}

// appJWT signs a GitHub App JWT valid for jwtTTL.
func (c *Client) appJWT(ctx context.Context) (string, error) {
	issued := c.now().Add(-30 * time.Second)
	claims := jwtClaims{
		Iat: issued.Unix(),
		Exp: issued.Add(jwtTTL).Unix(),
		Iss: c.appID,
	}
	header := jwtHeader{Alg: "RS256", Typ: "JWT"}

	headerJSON, err := json.Marshal(header)
	if err != nil {
		slog.ErrorContext(ctx, "ghapp jwt header marshal failed", "err", err)
		return "", fmt.Errorf("ghapp: marshal jwt header: %w", err)
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		slog.ErrorContext(ctx, "ghapp jwt claims marshal failed", "err", err)
		return "", fmt.Errorf("ghapp: marshal jwt claims: %w", err)
	}
	signingInput := b64(headerJSON) + "." + b64(claimsJSON)

	digest := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, c.privateKey, crypto.SHA256, digest[:])
	if err != nil {
		slog.ErrorContext(ctx, "ghapp jwt sign failed", "err", err)
		return "", fmt.Errorf("ghapp: sign jwt: %w", err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func b64(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

// installationResponse is the relevant field of GET /repos/{o}/{r}/installation.
type installationResponse struct {
	ID int64 `json:"id"`
}

// InstallationID resolves the installation that grants this App access to the
// given repository.
func (c *Client) InstallationID(ctx context.Context, owner, repo string) (int64, error) {
	jwt, err := c.appJWT(ctx)
	if err != nil {
		return 0, err
	}
	path := fmt.Sprintf("/repos/%s/%s/installation", owner, repo)
	body, err := c.do(ctx, http.MethodGet, path, "Bearer "+jwt, nil)
	if err != nil {
		return 0, err
	}
	var out installationResponse
	if err := json.Unmarshal(body, &out); err != nil {
		slog.ErrorContext(ctx, "ghapp decode installation failed", "err", err, "repo", owner+"/"+repo)
		return 0, fmt.Errorf("ghapp: decode installation: %w", err)
	}
	if out.ID == 0 {
		return 0, fmt.Errorf("ghapp: no installation for %s/%s", owner, repo)
	}
	return out.ID, nil
}

// tokenPermissions is the least-privilege permission set for the broker.
type tokenPermissions struct {
	Administration string `json:"administration"`
	Actions        string `json:"actions"`
}

// accessTokenRequest scopes an installation token to one repository.
type accessTokenRequest struct {
	Repositories []string         `json:"repositories,omitempty"`
	Permissions  tokenPermissions `json:"permissions"`
}

// accessTokenResponse is the relevant field of the access-tokens response.
type accessTokenResponse struct {
	Token string `json:"token"`
}

// InstallationToken mints a repo-scoped installation access token.
func (c *Client) InstallationToken(ctx context.Context, installationID int64, repo string) (string, error) {
	return c.installationToken(ctx, installationID, []string{repo})
}

func (c *Client) installationToken(ctx context.Context, installationID int64, repositories []string) (string, error) {
	jwt, err := c.appJWT(ctx)
	if err != nil {
		return "", err
	}
	path := fmt.Sprintf("/app/installations/%d/access_tokens", installationID)
	reqBody := accessTokenRequest{
		Repositories: repositories,
		Permissions:  tokenPermissions{Administration: "write", Actions: "write"},
	}
	slog.DebugContext(ctx, "minting installation token",
		"installation_id", installationID,
		"repositories", reqBody.Repositories,
		"administration", reqBody.Permissions.Administration,
		"actions", reqBody.Permissions.Actions)
	encoded, err := json.Marshal(reqBody)
	if err != nil {
		slog.ErrorContext(ctx, "ghapp marshal token request failed", "err", err)
		return "", fmt.Errorf("ghapp: marshal token request: %w", err)
	}
	body, err := c.do(ctx, http.MethodPost, path, "Bearer "+jwt, encoded)
	if err != nil {
		return "", err
	}
	var out accessTokenResponse
	if err := json.Unmarshal(body, &out); err != nil {
		slog.ErrorContext(ctx, "ghapp decode token failed", "err", err)
		return "", fmt.Errorf("ghapp: decode token: %w", err)
	}
	if out.Token == "" {
		return "", errors.New("ghapp: empty installation token")
	}
	return out.Token, nil
}

type appInstallation struct {
	ID int64 `json:"id"`
}

type installedRepository struct {
	FullName string `json:"full_name"`
}

type installedRepositoriesResponse struct {
	TotalCount   int                   `json:"total_count"`
	Repositories []installedRepository `json:"repositories"`
}

// ListInstalledRepos returns every repository visible to the GitHub App
// installations.
func (c *Client) ListInstalledRepos(ctx context.Context) ([]string, error) {
	jwt, err := c.appJWT(ctx)
	if err != nil {
		return nil, err
	}
	var repos []string
	for page := 1; ; page++ {
		path := fmt.Sprintf("/app/installations?per_page=%d&page=%d", installationListPageSize, page)
		body, err := c.do(ctx, http.MethodGet, path, "Bearer "+jwt, nil)
		if err != nil {
			slog.ErrorContext(ctx, "ghapp installation list failed", "err", err, "page", page)
			return nil, fmt.Errorf("ghapp: list installations page %d: %w", page, err)
		}
		var installations []appInstallation
		if err := json.Unmarshal(body, &installations); err != nil {
			slog.ErrorContext(ctx, "ghapp decode installations failed", "err", err, "page", page)
			return nil, fmt.Errorf("ghapp: decode installations page %d: %w", page, err)
		}
		if len(installations) == 0 {
			return repos, nil
		}
		for _, installation := range installations {
			if installation.ID == 0 {
				return nil, fmt.Errorf("ghapp: installation page %d contained an installation with no id", page)
			}
			token, err := c.installationToken(ctx, installation.ID, nil)
			if err != nil {
				return nil, fmt.Errorf("ghapp: installation token for installed repos %d: %w", installation.ID, err)
			}
			installationRepos, err := c.listInstallationRepos(ctx, installation.ID, token)
			if err != nil {
				return nil, err
			}
			repos = append(repos, installationRepos...)
		}
	}
}

func (c *Client) listInstallationRepos(ctx context.Context, installationID int64, token string) ([]string, error) {
	var repos []string
	for page := 1; ; page++ {
		path := fmt.Sprintf("/installation/repositories?per_page=%d&page=%d", installationListPageSize, page)
		body, err := c.do(ctx, http.MethodGet, path, "token "+token, nil)
		if err != nil {
			slog.ErrorContext(ctx, "ghapp installed repository list failed", "err", err, "installation_id", installationID, "page", page)
			return nil, fmt.Errorf("ghapp: list installed repositories for installation %d page %d: %w", installationID, page, err)
		}
		var out installedRepositoriesResponse
		if err := json.Unmarshal(body, &out); err != nil {
			slog.ErrorContext(ctx, "ghapp decode installed repositories failed", "err", err, "installation_id", installationID, "page", page)
			return nil, fmt.Errorf("ghapp: decode installed repositories for installation %d page %d: %w", installationID, page, err)
		}
		for _, repository := range out.Repositories {
			if repository.FullName == "" {
				return nil, fmt.Errorf("ghapp: installed repository for installation %d page %d has no full_name", installationID, page)
			}
			repos = append(repos, repository.FullName)
		}
		if len(out.Repositories) == 0 || len(repos) >= out.TotalCount {
			return repos, nil
		}
	}
}

// jitConfigRequest is the body of generate-jitconfig.
type jitConfigRequest struct {
	Name          string   `json:"name"`
	RunnerGroupID int64    `json:"runner_group_id"`
	Labels        []string `json:"labels"`
	WorkFolder    string   `json:"work_folder"`
}

// runnerInfo identifies the registered ephemeral runner.
type runnerInfo struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// Runner describes one repository runner returned by the GitHub REST API.
type Runner struct {
	// ID is the GitHub runner id used for deletion.
	ID int64 `json:"id"`
	// Name is the registered runner name.
	Name string `json:"name"`
	// Status is the GitHub runner status, usually "online" or "offline".
	Status string `json:"status"`
	// Busy reports whether GitHub currently has a job assigned to the runner.
	Busy bool `json:"busy"`
}

type runnerListResponse struct {
	TotalCount int      `json:"total_count"`
	Runners    []Runner `json:"runners"`
}

// inProgressRunsResponse is the relevant subset of
// GET /repos/{owner}/{repo}/actions/runs?status=in_progress.
type inProgressRunsResponse struct {
	TotalCount   int `json:"total_count"`
	WorkflowRuns []struct {
		ID int64 `json:"id"`
	} `json:"workflow_runs"`
}

// runsByHeadResponse is the relevant subset of
// GET /repos/{owner}/{repo}/actions/runs?head_sha={sha}.
type runsByHeadResponse struct {
	TotalCount   int `json:"total_count"`
	WorkflowRuns []struct {
		ID     int64  `json:"id"`
		Status string `json:"status"`
	} `json:"workflow_runs"`
}

// runJobsResponse is the relevant subset of
// GET /repos/{owner}/{repo}/actions/runs/{runID}/jobs.
type runJobsResponse struct {
	TotalCount int `json:"total_count"`
	Jobs       []struct {
		ID     int64    `json:"id"`
		Status string   `json:"status"`
		Labels []string `json:"labels"`
	} `json:"jobs"`
}

// JITConfig is the result of generate-jitconfig.
type JITConfig struct {
	// EncodedJITConfig is passed verbatim to the runner via --jitconfig.
	EncodedJITConfig string     `json:"encoded_jit_config"`
	Runner           runnerInfo `json:"runner"`
}

// GenerateJITConfig creates an ephemeral, repo-scoped just-in-time runner
// config. The returned EncodedJITConfig is injected into a warm VM and consumed
// once; the runner deregisters itself after a single job.
func (c *Client) GenerateJITConfig(ctx context.Context, token, owner, repo, name string, labels []string) (*JITConfig, error) {
	path := fmt.Sprintf("/repos/%s/%s/actions/runners/generate-jitconfig", owner, repo)
	reqBody := jitConfigRequest{
		Name:          name,
		RunnerGroupID: 1,
		Labels:        labels,
		WorkFolder:    "_work",
	}
	encoded, err := json.Marshal(reqBody)
	if err != nil {
		slog.ErrorContext(ctx, "ghapp marshal jitconfig request failed", "err", err)
		return nil, fmt.Errorf("ghapp: marshal jitconfig request: %w", err)
	}
	body, err := c.do(ctx, http.MethodPost, path, "token "+token, encoded)
	if err != nil {
		return nil, err
	}
	var out JITConfig
	if err := json.Unmarshal(body, &out); err != nil {
		slog.ErrorContext(ctx, "ghapp decode jitconfig failed", "err", err, "repo", owner+"/"+repo)
		return nil, fmt.Errorf("ghapp: decode jitconfig: %w", err)
	}
	if out.EncodedJITConfig == "" {
		return nil, errors.New("ghapp: empty encoded_jit_config")
	}
	return &out, nil
}

// CancelRun cancels one workflow run in repo.
func (c *Client) CancelRun(ctx context.Context, repo string, runID int64) error {
	owner, repoName, token, err := c.repoToken(ctx, repo)
	if err != nil {
		slog.ErrorContext(ctx, "ghapp prepare cancel run failed", "err", err, "repo", repo, "run_id", runID)
		return fmt.Errorf("ghapp: prepare cancel run %s#%d: %w", repo, runID, err)
	}
	path := fmt.Sprintf("/repos/%s/%s/actions/runs/%d/cancel", owner, repoName, runID)
	if _, err := c.do(ctx, http.MethodPost, path, "token "+token, nil); err != nil {
		slog.ErrorContext(ctx, "ghapp cancel run failed", "err", err, "repo", repo, "run_id", runID)
		return fmt.Errorf("ghapp: cancel run %s#%d: %w", repo, runID, err)
	}
	return nil
}

// CancelActiveRunsForHeadSHA cancels every workflow run in repo whose head commit
// is headSHA and that is still queued or in progress, and returns how many it
// cancelled. GitHub does not cancel a pull request's in-flight runs when it merges
// or closes, so the broker calls this on a pull_request close to free any pool
// slot and stop orphaned gates. Selecting by head sha never touches the merge
// commit's run on the default branch, which carries a different sha. A failure to
// cancel one run is logged and skipped so the remaining runs are still cancelled.
func (c *Client) CancelActiveRunsForHeadSHA(ctx context.Context, repo, headSHA string) (int, error) {
	owner, repoName, token, err := c.repoToken(ctx, repo)
	if err != nil {
		slog.ErrorContext(ctx, "ghapp prepare cancel runs for head sha failed", "err", err, "repo", repo, "head_sha", headSHA)
		return 0, fmt.Errorf("ghapp: prepare cancel runs for %s@%s: %w", repo, headSHA, err)
	}
	runIDs, err := c.listActiveRunIDsForHeadSHA(ctx, owner, repoName, token, headSHA)
	if err != nil {
		return 0, err
	}
	cancelled := 0
	for _, runID := range runIDs {
		path := fmt.Sprintf("/repos/%s/%s/actions/runs/%d/cancel", owner, repoName, runID)
		if _, err := c.do(ctx, http.MethodPost, path, "token "+token, nil); err != nil {
			slog.WarnContext(ctx, "ghapp cancel run for head sha failed", "err", err, "repo", repo, "run_id", runID, "head_sha", headSHA)
			continue
		}
		cancelled++
	}
	return cancelled, nil
}

// listActiveRunIDsForHeadSHA returns the ids of workflow runs on headSHA that are
// still queued or in progress, following pagination until the reported total is
// reached. The head sha is a hex commit id, so it needs no query escaping.
func (c *Client) listActiveRunIDsForHeadSHA(ctx context.Context, owner, repoName, token, headSHA string) ([]int64, error) {
	var runIDs []int64
	fetched := 0
	for page := 1; ; page++ {
		path := fmt.Sprintf("/repos/%s/%s/actions/runs?head_sha=%s&per_page=%d&page=%d", owner, repoName, headSHA, actionsListPageSize, page)
		body, err := c.do(ctx, http.MethodGet, path, "token "+token, nil)
		if err != nil {
			slog.ErrorContext(ctx, "ghapp runs-by-head list failed", "err", err, "repo", owner+"/"+repoName, "head_sha", headSHA, "page", page)
			return nil, fmt.Errorf("ghapp: list runs for %s/%s head %s page %d: %w", owner, repoName, headSHA, page, err)
		}
		var out runsByHeadResponse
		if err := json.Unmarshal(body, &out); err != nil {
			slog.ErrorContext(ctx, "ghapp decode runs-by-head failed", "err", err, "repo", owner+"/"+repoName, "head_sha", headSHA, "page", page)
			return nil, fmt.Errorf("ghapp: decode runs for %s/%s head %s page %d: %w", owner, repoName, headSHA, page, err)
		}
		for _, run := range out.WorkflowRuns {
			if run.Status == "queued" || run.Status == "in_progress" {
				runIDs = append(runIDs, run.ID)
			}
		}
		fetched += len(out.WorkflowRuns)
		if len(out.WorkflowRuns) == 0 || fetched >= out.TotalCount {
			return runIDs, nil
		}
	}
}

// ListRunners lists the repository runners registered for repo.
func (c *Client) ListRunners(ctx context.Context, repo string) ([]Runner, error) {
	owner, repoName, token, err := c.repoToken(ctx, repo)
	if err != nil {
		slog.ErrorContext(ctx, "ghapp prepare runner list failed", "err", err, "repo", repo)
		return nil, fmt.Errorf("ghapp: prepare runner list %s: %w", repo, err)
	}
	var runners []Runner
	for page := 1; ; page++ {
		path := fmt.Sprintf("/repos/%s/%s/actions/runners?per_page=%d&page=%d", owner, repoName, runnerListPageSize, page)
		body, err := c.do(ctx, http.MethodGet, path, "token "+token, nil)
		if err != nil {
			slog.ErrorContext(ctx, "ghapp runner list failed", "err", err, "repo", repo, "page", page)
			return nil, fmt.Errorf("ghapp: list runners %s page %d: %w", repo, page, err)
		}
		var out runnerListResponse
		if err := json.Unmarshal(body, &out); err != nil {
			slog.ErrorContext(ctx, "ghapp decode runners failed", "err", err, "repo", repo, "page", page)
			return nil, fmt.Errorf("ghapp: decode runners %s page %d: %w", repo, page, err)
		}
		runners = append(runners, out.Runners...)
		if len(out.Runners) == 0 || len(runners) >= out.TotalCount {
			return runners, nil
		}
	}
}

// DeleteRunner deregisters one repository runner by id.
func (c *Client) DeleteRunner(ctx context.Context, repo string, runnerID int64) error {
	owner, repoName, token, err := c.repoToken(ctx, repo)
	if err != nil {
		slog.ErrorContext(ctx, "ghapp prepare runner delete failed", "err", err, "repo", repo, "runner_id", runnerID)
		return fmt.Errorf("ghapp: prepare runner delete %s#%d: %w", repo, runnerID, err)
	}
	path := fmt.Sprintf("/repos/%s/%s/actions/runners/%d", owner, repoName, runnerID)
	if _, err := c.do(ctx, http.MethodDelete, path, "token "+token, nil); err != nil {
		slog.ErrorContext(ctx, "ghapp runner delete failed", "err", err, "repo", repo, "runner_id", runnerID)
		return fmt.Errorf("ghapp: delete runner %s#%d: %w", repo, runnerID, err)
	}
	return nil
}

// ListInProgressHostedMacOSJobs returns the set of in-progress GitHub-hosted
// macOS job IDs across every installed repository. It is the periodic reconcile
// that corrects the live webhook counter for restart or missed-delivery drift.
// On any API error it returns the error so the caller keeps its existing live
// set rather than clobbering it to empty.
func (c *Client) ListInProgressHostedMacOSJobs(ctx context.Context, poolLabels []string) (map[int64]struct{}, error) {
	repos, err := c.ListInstalledRepos(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "ghapp hosted job sweep list repos failed", "err", err)
		return nil, fmt.Errorf("ghapp: list installed repos for hosted job sweep: %w", err)
	}
	result := make(map[int64]struct{})
	for _, repo := range repos {
		owner, repoName, ok := strings.Cut(repo, "/")
		if !ok || owner == "" || repoName == "" {
			slog.WarnContext(ctx, "ghapp hosted job sweep skipping malformed repo", "repo", repo)
			continue
		}
		_, _, token, err := c.repoToken(ctx, repo)
		if err != nil {
			slog.ErrorContext(ctx, "ghapp hosted job sweep repo token failed", "err", err, "repo", repo)
			return nil, fmt.Errorf("ghapp: repo token for hosted job sweep %s: %w", repo, err)
		}
		runIDs, err := c.listInProgressRunIDs(ctx, owner, repoName, token)
		if err != nil {
			return nil, err
		}
		for _, runID := range runIDs {
			if err := c.collectHostedMacOSJobs(ctx, owner, repoName, token, runID, poolLabels, result); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// listInProgressRunIDs returns the ids of every in-progress workflow run in one
// repository, following pagination until the reported total is reached.
func (c *Client) listInProgressRunIDs(ctx context.Context, owner, repoName, token string) ([]int64, error) {
	var runIDs []int64
	for page := 1; ; page++ {
		path := fmt.Sprintf("/repos/%s/%s/actions/runs?status=in_progress&per_page=%d&page=%d", owner, repoName, actionsListPageSize, page)
		body, err := c.do(ctx, http.MethodGet, path, "token "+token, nil)
		if err != nil {
			slog.ErrorContext(ctx, "ghapp in-progress runs list failed", "err", err, "repo", owner+"/"+repoName, "page", page)
			return nil, fmt.Errorf("ghapp: list in-progress runs %s/%s page %d: %w", owner, repoName, page, err)
		}
		var out inProgressRunsResponse
		if err := json.Unmarshal(body, &out); err != nil {
			slog.ErrorContext(ctx, "ghapp decode in-progress runs failed", "err", err, "repo", owner+"/"+repoName, "page", page)
			return nil, fmt.Errorf("ghapp: decode in-progress runs %s/%s page %d: %w", owner, repoName, page, err)
		}
		for _, run := range out.WorkflowRuns {
			runIDs = append(runIDs, run.ID)
		}
		if len(out.WorkflowRuns) == 0 || len(runIDs) >= out.TotalCount {
			return runIDs, nil
		}
	}
}

// collectHostedMacOSJobs adds every in-progress GitHub-hosted macOS job id in one
// run to result, following pagination until the reported total is reached.
func (c *Client) collectHostedMacOSJobs(ctx context.Context, owner, repoName, token string, runID int64, poolLabels []string, result map[int64]struct{}) error {
	collected := 0
	for page := 1; ; page++ {
		path := fmt.Sprintf("/repos/%s/%s/actions/runs/%d/jobs?per_page=%d&page=%d", owner, repoName, runID, actionsListPageSize, page)
		body, err := c.do(ctx, http.MethodGet, path, "token "+token, nil)
		if err != nil {
			slog.ErrorContext(ctx, "ghapp run jobs list failed", "err", err, "repo", owner+"/"+repoName, "run_id", runID, "page", page)
			return fmt.Errorf("ghapp: list jobs for run %s/%s#%d page %d: %w", owner, repoName, runID, page, err)
		}
		var out runJobsResponse
		if err := json.Unmarshal(body, &out); err != nil {
			slog.ErrorContext(ctx, "ghapp decode run jobs failed", "err", err, "repo", owner+"/"+repoName, "run_id", runID, "page", page)
			return fmt.Errorf("ghapp: decode jobs for run %s/%s#%d page %d: %w", owner, repoName, runID, page, err)
		}
		for _, job := range out.Jobs {
			if job.Status == "in_progress" && hostedload.IsHostedMacOSJob(job.Labels, poolLabels) {
				result[job.ID] = struct{}{}
			}
		}
		collected += len(out.Jobs)
		if len(out.Jobs) == 0 || collected >= out.TotalCount {
			return nil
		}
	}
}

func (c *Client) repoToken(ctx context.Context, fullRepo string) (string, string, string, error) {
	owner, repoName, ok := strings.Cut(fullRepo, "/")
	if !ok || owner == "" || repoName == "" {
		err := fmt.Errorf("repo must be owner/repo, got %q", fullRepo)
		slog.ErrorContext(ctx, "ghapp repo parse failed", "err", err, "repo", fullRepo)
		return "", "", "", err
	}
	installationID, err := c.InstallationID(ctx, owner, repoName)
	if err != nil {
		slog.ErrorContext(ctx, "ghapp repo installation lookup failed", "err", err, "repo", fullRepo)
		return "", "", "", fmt.Errorf("installation lookup: %w", err)
	}
	token, err := c.InstallationToken(ctx, installationID, repoName)
	if err != nil {
		slog.ErrorContext(ctx, "ghapp repo installation token failed", "err", err, "repo", fullRepo)
		return "", "", "", fmt.Errorf("installation token: %w", err)
	}
	return owner, repoName, token, nil
}

// do performs one authenticated REST call and returns the raw response body.
func (c *Client) do(ctx context.Context, method, path, authorization string, body []byte) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.apiBase+path, reader)
	if err != nil {
		slog.ErrorContext(ctx, "ghapp build request failed", "err", err, "path", path)
		return nil, fmt.Errorf("ghapp: build request %s %s: %w", method, path, err)
	}
	req.Header.Set("Authorization", authorization)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-Github-Api-Version", "2022-11-28")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		slog.ErrorContext(ctx, "ghapp request failed", "err", err, "path", path)
		return nil, fmt.Errorf("ghapp: %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		slog.ErrorContext(ctx, "ghapp read response failed", "err", err, "path", path)
		return nil, fmt.Errorf("ghapp: read response %s %s: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("ghapp: %s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(payload)))
	}
	return payload, nil
}
