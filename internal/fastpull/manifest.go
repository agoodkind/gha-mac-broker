package fastpull

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

const (
	cirrusRepoPrefix = "cirruslabs/macos-"
	cirrusRepoSuffix = "-xcode"
)

// isCirrusXcodeRepo reports whether repo is a Cirrus macOS-Xcode image path. It
// keeps the fast pull restricted to the images the pool is allowed to serve.
func isCirrusXcodeRepo(repo string) bool {
	return strings.HasPrefix(repo, cirrusRepoPrefix) && strings.HasSuffix(repo, cirrusRepoSuffix)
}

// tokenResponse is the ghcr anonymous pull token response.
type tokenResponse struct {
	Token string `json:"token"`
}

// fetchPullToken fetches an anonymous registry pull token for repo. A failure
// returns an empty token, since a registry that needs none still serves the
// blobs; the failure is logged, not fatal.
func fetchPullToken(ctx context.Context, client *http.Client, scheme, host, repo string) string {
	url := scheme + "://" + host + "/token?scope=repository:" + repo + ":pull"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		slog.WarnContext(ctx, "fastpull token request build failed", "err", err)
		return ""
	}
	resp, err := client.Do(req)
	if err != nil {
		slog.WarnContext(ctx, "fastpull token request failed", "err", err)
		return ""
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		slog.WarnContext(ctx, "fastpull token request non-200", "status", resp.StatusCode)
		return ""
	}
	var parsed tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		slog.WarnContext(ctx, "fastpull token decode failed", "err", err)
		return ""
	}
	return parsed.Token
}

// selectArm64 returns the digest of the arm64 child manifest in an image index,
// preferring a darwin/arm64 entry. It returns an error when no arm64 manifest is
// present.
func selectArm64(manifests []v1.Descriptor) (v1.Hash, error) {
	var darwinArm, anyArm *v1.Descriptor
	for i := range manifests {
		desc := &manifests[i]
		if desc.Platform == nil || desc.Platform.Architecture != "arm64" {
			continue
		}
		if anyArm == nil {
			anyArm = desc
		}
		if desc.Platform.OS == "darwin" && darwinArm == nil {
			darwinArm = desc
		}
	}
	if darwinArm != nil {
		return darwinArm.Digest, nil
	}
	if anyArm != nil {
		return anyArm.Digest, nil
	}
	return v1.Hash{}, fmt.Errorf("fastpull: no arm64 manifest in index")
}
