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
)

// defaultAPIBase is the GitHub REST API root.
const defaultAPIBase = "https://api.github.com"

// jwtTTL is how long the App JWT stays valid. GitHub rejects App JWTs whose
// exp is more than 10 minutes ahead, so this stays comfortably under that.
const jwtTTL = 9 * time.Minute

// maxResponseBytes caps how much of a response body is read.
const maxResponseBytes = 1 << 20

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
	Repositories []string         `json:"repositories"`
	Permissions  tokenPermissions `json:"permissions"`
}

// accessTokenResponse is the relevant field of the access-tokens response.
type accessTokenResponse struct {
	Token string `json:"token"`
}

// InstallationToken mints a repo-scoped installation access token.
func (c *Client) InstallationToken(ctx context.Context, installationID int64, repo string) (string, error) {
	jwt, err := c.appJWT(ctx)
	if err != nil {
		return "", err
	}
	path := fmt.Sprintf("/app/installations/%d/access_tokens", installationID)
	reqBody := accessTokenRequest{
		Repositories: []string{repo},
		Permissions:  tokenPermissions{Administration: "write", Actions: "read"},
	}
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
