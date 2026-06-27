package ghapp

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testKeyPEM(t *testing.T) ([]byte, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der := x509.MarshalPKCS1PrivateKey(key)
	block := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: der}
	return pem.EncodeToMemory(block), key
}

func TestAppJWTVerifies(t *testing.T) {
	pemKey, key := testKeyPEM(t)
	fixed := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	client, err := New("12345", pemKey)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client.now = func() time.Time { return fixed }

	token, err := client.appJWT(context.Background())
	if err != nil {
		t.Fatalf("appJWT: %v", err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 jwt parts, got %d", len(parts))
	}

	signingInput := parts[0] + "." + parts[1]
	digest := sha256.Sum256([]byte(signingInput))
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decode sig: %v", err)
	}
	if err := rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], sig); err != nil {
		t.Fatalf("jwt signature did not verify: %v", err)
	}

	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode claims: %v", err)
	}
	var claims struct {
		Iss string `json:"iss"`
		Iat int64  `json:"iat"`
		Exp int64  `json:"exp"`
	}
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		t.Fatalf("unmarshal claims: %v", err)
	}
	if claims.Iss != "12345" {
		t.Errorf("iss = %q, want 12345", claims.Iss)
	}
	if claims.Exp <= claims.Iat {
		t.Errorf("exp %d must be after iat %d", claims.Exp, claims.Iat)
	}
	if got := claims.Exp - claims.Iat; got > int64((10 * time.Minute).Seconds()) {
		t.Errorf("jwt window %ds exceeds GitHub 10 minute cap", got)
	}
}

func TestGenerateJITConfigFlow(t *testing.T) {
	pemKey, _ := testKeyPEM(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/agoodkind/lmd/installation":
			if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
				t.Errorf("installation lookup must use App JWT bearer, got %q", r.Header.Get("Authorization"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 999})
		case r.Method == http.MethodPost && r.URL.Path == "/app/installations/999/access_tokens":
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "ghs_installationtoken"})
		case r.Method == http.MethodPost && r.URL.Path == "/repos/agoodkind/lmd/actions/runners/generate-jitconfig":
			if got := r.Header.Get("Authorization"); got != "token ghs_installationtoken" {
				t.Errorf("jitconfig must use installation token, got %q", got)
			}
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["name"] != "warm-vm-1" {
				t.Errorf("runner name = %v, want warm-vm-1", body["name"])
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"encoded_jit_config": "ZW5jb2RlZA==",
				"runner":             map[string]any{"id": 7, "name": "warm-vm-1"},
			})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected", http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := New("12345", pemKey, WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client.apiBase = server.URL
	ctx := context.Background()

	installationID, err := client.InstallationID(ctx, "agoodkind", "lmd")
	if err != nil {
		t.Fatalf("InstallationID: %v", err)
	}
	if installationID != 999 {
		t.Fatalf("installationID = %d, want 999", installationID)
	}

	token, err := client.InstallationToken(ctx, installationID, "lmd")
	if err != nil {
		t.Fatalf("InstallationToken: %v", err)
	}

	jit, err := client.GenerateJITConfig(ctx, token, "agoodkind", "lmd", "warm-vm-1", []string{"self-hosted", "macOS", "agk-local-macos-26"})
	if err != nil {
		t.Fatalf("GenerateJITConfig: %v", err)
	}
	if jit.EncodedJITConfig != "ZW5jb2RlZA==" {
		t.Errorf("encoded config = %q", jit.EncodedJITConfig)
	}
	if jit.Runner.Name != "warm-vm-1" {
		t.Errorf("runner name = %q", jit.Runner.Name)
	}
}
