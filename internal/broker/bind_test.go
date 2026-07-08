package broker

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/ghapp"
	"goodkind.io/gha-mac-broker/internal/tart"
)

func TestActiveJobProbeScriptAvoidsSelfMatch(t *testing.T) {
	if !strings.Contains(activeJobProbeScript, "[R]unner\\.Worker") {
		t.Fatalf("active job probe script = %q, want bracketed Runner.Worker pattern", activeJobProbeScript)
	}
	if strings.Contains(activeJobProbeScript, "Runner.Worker") {
		t.Fatalf("active job probe script = %q, want no bare Runner.Worker substring", activeJobProbeScript)
	}
}

func TestRunJobRemoteCommandKeepsLegacySingleSlotPath(t *testing.T) {
	command := runJobRemoteCommand("encoded-jit", 0, 1)
	want := "cd ~/actions-runner && export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=credential.helper GIT_CONFIG_VALUE_0= GIT_TERMINAL_PROMPT=0 && ./run.sh --jitconfig 'encoded-jit'"
	if command != want {
		t.Fatalf("single-slot command = %q, want %q", command, want)
	}
	if strings.Contains(command, "GCM_INTERACTIVE") {
		t.Fatalf("single slot command = %q, want no GCM_INTERACTIVE", command)
	}
}

func TestRunJobRemoteCommandUsesSlotHomeAndTMPDIR(t *testing.T) {
	command := runJobRemoteCommand("encoded-jit", 1, 2)
	for _, fragment := range []string{
		`base_home="$HOME"`,
		`runner_home="$base_home/actions-runner-1"`,
		`export TMPDIR="$base_home/tmp-1"`,
		`export HOME="$base_home/slot-home-1"`,
		`mkdir -p "$HOME"`,
		"export GIT_CONFIG_COUNT=1",
		"export GIT_CONFIG_KEY_0=credential.helper",
		"export GIT_CONFIG_VALUE_0=",
		"export GIT_TERMINAL_PROMPT=0",
		"./run.sh --jitconfig 'encoded-jit'",
	} {
		if !strings.Contains(command, fragment) {
			t.Fatalf("slot command = %q, want fragment %q", command, fragment)
		}
	}
	homeIndex := strings.Index(command, `export HOME="$base_home/slot-home-1"`)
	cdIndex := strings.Index(command, `cd "$runner_home"`)
	if homeIndex < 0 || cdIndex < 0 || homeIndex > cdIndex {
		t.Fatalf("slot command = %q, want HOME exported before cd into runner home", command)
	}
	if strings.Contains(command, "cd ~/actions-runner &&") {
		t.Fatalf("slot command = %q, want no legacy runner home", command)
	}
	if strings.Contains(command, `runner_home="$HOME/actions-runner-1"`) {
		t.Fatalf("slot command = %q, want runner home from base home", command)
	}
	if strings.Contains(command, "GCM_INTERACTIVE") {
		t.Fatalf("slot command = %q, want no GCM_INTERACTIVE", command)
	}
}

func TestCloneRunnerSlotsCommandSkipsSingleSlot(t *testing.T) {
	command := cloneRunnerSlotsCommand(1)
	if command != "" {
		t.Fatalf("single-slot clone command = %q, want empty", command)
	}
}

func TestCloneRunnerSlotsCommandCopiesGoldenRunnerToSlotDirs(t *testing.T) {
	command := cloneRunnerSlotsCommand(3)
	for _, fragment := range []string{
		"slot_count=3",
		`cp -R "$HOME/actions-runner" "$runner_home"`,
		`mkdir -p "$tmp_dir"`,
		`warm_cache_paths=(`,
		`".local"`,
		`".swiftpm"`,
		`".cache"`,
		`"Library/Caches/org.swift.swiftpm"`,
		`"Library/Caches/Homebrew"`,
		`"Library/Developer/Xcode/DerivedData"`,
		`".gitconfig"`,
		`".netrc"`,
		`slot_home="$HOME/slot-home-$slot_index"`,
		`rm -rf "$slot_home"`,
		`mkdir -p "$slot_home"`,
		`source_path="$HOME/$warm_cache_path"`,
		`dest_path="$slot_home/$warm_cache_path"`,
		`mkdir -p "$(dirname "$dest_path")"`,
		`cp -cR "$source_path" "$dest_path"`,
		`cp -R "$source_path" "$dest_path"`,
	} {
		if !strings.Contains(command, fragment) {
			t.Fatalf("clone command = %q, want fragment %q", command, fragment)
		}
	}
	// The toolchain is keyed by source hash and restored per slot by actions/cache,
	// so it must not be seeded into the slot home (a seed/cache merge risk).
	if strings.Contains(command, ".swift-mk-ci-toolchain") {
		t.Fatalf("clone command = %q, want no toolchain in the warm cache seed", command)
	}
}

func TestActiveJobProbeCommandTargetsSlotRunner(t *testing.T) {
	command := activeJobProbeCommand(2, 4)
	if !strings.Contains(command, "actions-runner-2/bin/[R]unner\\.Worker") {
		t.Fatalf("active job probe command = %q, want slot runner path", command)
	}
	if strings.Contains(command, "bin/Runner.Worker") {
		t.Fatalf("active job probe command = %q, want bracketed Runner.Worker pattern", command)
	}
}

func TestSlotCPUActivityCommandTargetsSlotRunnerAndSumsSubtree(t *testing.T) {
	command := slotCPUActivityCommand(2, 4)
	for _, fragment := range []string{
		"actions-runner-2/bin/[R]unner\\.Worker",
		"ps -Ao pid=,ppid=,pcpu=",
		`awk -v roots="$roots"`,
		"while (changed)",
		`printf "%.1f\n", total`,
	} {
		if !strings.Contains(command, fragment) {
			t.Fatalf("slot cpu activity command = %q, want fragment %q", command, fragment)
		}
	}
	if strings.Contains(command, "bin/Runner.Worker") {
		t.Fatalf("slot cpu activity command = %q, want bracketed Runner.Worker pattern", command)
	}
	for _, disallowed := range []string{"head", "tail", "sort"} {
		if strings.Contains(command, disallowed) {
			t.Fatalf("slot cpu activity command = %q, want no %s", command, disallowed)
		}
	}
}

func TestRunJobClearsSlotBindingAfterJobCompletion(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-tart")
	bindingFile := filepath.Join(dir, "binding.json")
	runStartedFile := filepath.Join(dir, "run-started")
	writeRunJobFakeTart(t, bin, bindingFile, runStartedFile, "complete")

	cfg := &config.Config{Labels: []string{"self-hosted"}}
	binder := New(cfg, newRunJobTestGitHubClient(t), tart.New(bin))
	err := binder.RunJob(
		context.Background(),
		&WarmVM{Name: "warm-vm-1"},
		"owner/repo",
		"runner-1",
		0,
		1,
		1001,
		42,
		time.Date(2026, 7, 3, 11, 59, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	if _, err := os.Stat(bindingFile); !os.IsNotExist(err) {
		t.Fatalf("binding file stat err = %v, want removed binding file", err)
	}
}

func TestRunJobKeepsSlotBindingWhenContextCanceled(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-tart")
	bindingFile := filepath.Join(dir, "binding.json")
	runStartedFile := filepath.Join(dir, "run-started")
	writeRunJobFakeTart(t, bin, bindingFile, runStartedFile, "block")

	cfg := &config.Config{Labels: []string{"self-hosted"}}
	binder := New(cfg, newRunJobTestGitHubClient(t), tart.New(bin))
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- binder.RunJob(
			ctx,
			&WarmVM{Name: "warm-vm-1"},
			"owner/repo",
			"runner-1",
			0,
			1,
			1001,
			42,
			time.Date(2026, 7, 3, 11, 59, 0, 0, time.UTC),
		)
	}()

	waitForFile(t, runStartedFile)
	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("RunJob error = nil, want canceled exec error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunJob did not return after context cancellation")
	}
	if ctx.Err() != context.Canceled {
		t.Fatalf("context err = %v, want context.Canceled", ctx.Err())
	}
	if _, err := os.Stat(bindingFile); err != nil {
		t.Fatalf("binding file stat err = %v, want binding file left for adoption", err)
	}
}

func TestAdoptPrefersLiveBusyVMWithinLimit(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-tart")
	binding := `{"slot_index":0,"repo":"owner/repo","job_id":1001,"run_id":42,"bound_at":"2026-07-03T11:59:00Z"}`
	script := strings.ReplaceAll(`#!/usr/bin/env bash
set -euo pipefail

if [[ "$1" == "list" ]]; then
    printf '[{"Name":"pool-a-dead","State":"running"},{"Name":"pool-b-idle","State":"running"},{"Name":"pool-c-busy","State":"running"}]'
    exit 0
fi

if [[ "$1" == "exec" ]]; then
    vm="$2"
    shift 2
    joined="$*"
    if [[ "$vm" == "pool-a-dead" ]]; then
        exit 1
    fi
    if [[ "$joined" == "touch /tmp/gha-broker.alive" ]]; then
        exit 0
    fi
    if [[ "$vm" == "pool-c-busy" && "$joined" == *"gha-broker-slot-0.json"* ]]; then
        cat <<'JSON'
__BINDING__
JSON
        exit 0
    fi
    if [[ "$joined" == *"pgrep"* ]]; then
        printf 'no\n'
        exit 0
    fi
    exit 0
fi

exit 1
`, "__BINDING__", binding)
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake tart: %v", err)
	}

	cfg := &config.Config{Tart: config.TartConfig{VMNamePrefix: "pool"}}
	binder := New(cfg, nil, tart.New(bin))
	adopted, err := binder.Adopt(context.Background(), "image-a", 1, 1)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	t.Cleanup(func() {
		for _, adoptedVM := range adopted {
			if adoptedVM.VM != nil && adoptedVM.VM.stopTouch != nil {
				adoptedVM.VM.stopTouch()
			}
		}
	})

	if len(adopted) != 1 {
		t.Fatalf("adopted = %+v, want one live busy VM", adopted)
	}
	if adopted[0].VM.Name != "pool-c-busy" {
		t.Fatalf("adopted vm = %q, want pool-c-busy", adopted[0].VM.Name)
	}
	if len(adopted[0].Slots) != 1 || adopted[0].Slots[0].RunID != 42 {
		t.Fatalf("adopted slots = %+v, want run 42 binding", adopted[0].Slots)
	}
}

func TestAdoptDoesNotLetIncompleteBindingConsumeLimit(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-tart")
	incompleteBinding := `{"slot_index":0,"repo":"owner/repo","job_id":0,"run_id":42,"bound_at":"2026-07-03T11:58:00Z"}`
	busyBinding := `{"slot_index":0,"repo":"owner/repo","job_id":1001,"run_id":43,"bound_at":"2026-07-03T11:59:00Z"}`
	script := strings.ReplaceAll(strings.ReplaceAll(`#!/usr/bin/env bash
set -euo pipefail

if [[ "$1" == "list" ]]; then
    printf '[{"Name":"pool-a-incomplete","State":"running"},{"Name":"pool-b-busy","State":"running"}]'
    exit 0
fi

if [[ "$1" == "exec" ]]; then
    vm="$2"
    shift 2
    joined="$*"
    if [[ "$joined" == "touch /tmp/gha-broker.alive" ]]; then
        exit 0
    fi
    if [[ "$vm" == "pool-a-incomplete" && "$joined" == *"gha-broker-slot-0.json"* ]]; then
        cat <<'JSON'
__INCOMPLETE_BINDING__
JSON
        exit 0
    fi
    if [[ "$vm" == "pool-b-busy" && "$joined" == *"gha-broker-slot-0.json"* ]]; then
        cat <<'JSON'
__BUSY_BINDING__
JSON
        exit 0
    fi
    if [[ "$joined" == *"pgrep"* ]]; then
        printf 'no\n'
        exit 0
    fi
    exit 0
fi

exit 1
`, "__INCOMPLETE_BINDING__", incompleteBinding), "__BUSY_BINDING__", busyBinding)
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake tart: %v", err)
	}

	cfg := &config.Config{Tart: config.TartConfig{VMNamePrefix: "pool"}}
	binder := New(cfg, nil, tart.New(bin))
	adopted, err := binder.Adopt(context.Background(), "image-a", 1, 1)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	t.Cleanup(func() {
		for _, adoptedVM := range adopted {
			if adoptedVM.VM != nil && adoptedVM.VM.stopTouch != nil {
				adoptedVM.VM.stopTouch()
			}
		}
	})

	if len(adopted) != 1 {
		t.Fatalf("adopted = %+v, want one valid busy VM", adopted)
	}
	if adopted[0].VM.Name != "pool-b-busy" {
		t.Fatalf("adopted vm = %q, want pool-b-busy", adopted[0].VM.Name)
	}
}

func TestAdoptFallsBackToActiveProbeForIncompleteBinding(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-tart")
	binding := `{"slot_index":0,"repo":"owner/repo","job_id":0,"run_id":43,"bound_at":"2026-07-03T11:59:00Z"}`
	script := strings.ReplaceAll(`#!/usr/bin/env bash
set -euo pipefail

if [[ "$1" == "list" ]]; then
    printf '[{"Name":"pool-a-busy","State":"running"}]'
    exit 0
fi

if [[ "$1" == "exec" ]]; then
    shift 2
    joined="$*"
    if [[ "$joined" == "touch /tmp/gha-broker.alive" ]]; then
        exit 0
    fi
    if [[ "$joined" == *"gha-broker-slot-0.json"* ]]; then
        cat <<'JSON'
__BINDING__
JSON
        exit 0
    fi
    if [[ "$joined" == *"pgrep"* ]]; then
        printf 'yes\n'
        exit 0
    fi
    exit 0
fi

exit 1
`, "__BINDING__", binding)
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake tart: %v", err)
	}

	cfg := &config.Config{Tart: config.TartConfig{VMNamePrefix: "pool"}}
	binder := New(cfg, nil, tart.New(bin))
	adopted, err := binder.Adopt(context.Background(), "image-a", 1, 1)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	t.Cleanup(func() {
		for _, adoptedVM := range adopted {
			if adoptedVM.VM != nil && adoptedVM.VM.stopTouch != nil {
				adoptedVM.VM.stopTouch()
			}
		}
	})

	if len(adopted) != 1 {
		t.Fatalf("adopted = %+v, want one live VM", adopted)
	}
	if len(adopted[0].Slots) != 1 {
		t.Fatalf("adopted slots = %+v, want one active fallback slot", adopted[0].Slots)
	}
	slot := adopted[0].Slots[0]
	if !slot.ObservedActive || slot.JobID != 0 || slot.RunID != 0 {
		t.Fatalf("adopted slot = %+v, want observed-active fallback without stale IDs", slot)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func writeRunJobFakeTart(t *testing.T, bin string, bindingFile string, runStartedFile string, runMode string) {
	t.Helper()
	t.Setenv("FAKE_TART_BINDING_FILE", bindingFile)
	t.Setenv("FAKE_TART_RUN_STARTED_FILE", runStartedFile)
	t.Setenv("FAKE_TART_RUN_MODE", runMode)
	t.Setenv("FAKE_TART_BLOCK_FIFO", filepath.Join(filepath.Dir(bindingFile), "block-fifo"))
	script := `#!/usr/bin/env bash
set -euo pipefail

binding_file="${FAKE_TART_BINDING_FILE:?}"
run_started_file="${FAKE_TART_RUN_STARTED_FILE:?}"
run_mode="${FAKE_TART_RUN_MODE:?}"
block_fifo="${FAKE_TART_BLOCK_FIFO:?}"

if [[ "$1" == "exec" ]]; then
	shift 2
	joined="$*"
	if [[ "$joined" == *"cat >"* && "$joined" == *"gha-broker-slot-0.json"* ]]; then
		printf '%s\n' "$joined" > "$binding_file"
		exit 0
	fi
    if [[ "$joined" == "rm -f /tmp/gha-broker-slot-0.json" ]]; then
        rm -f "$binding_file"
        exit 0
    fi
    if [[ "$joined" == *"./run.sh --jitconfig"* ]]; then
        if [[ ! -f "$binding_file" ]]; then
            printf 'missing binding\n' >&2
            exit 3
		fi
		printf 'started\n' > "$run_started_file"
		if [[ "$run_mode" == "block" ]]; then
			mkfifo "$block_fifo"
			read -r _ < "$block_fifo"
		fi
		exit 0
	fi
    exit 0
fi

exit 1
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake tart: %v", err)
	}
}

func newRunJobTestGitHubClient(t *testing.T) *ghapp.Client {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/owner/repo/installation":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":999}`))
		case r.Method == http.MethodPost && r.URL.Path == "/app/installations/999/access_tokens":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"token":"ghs_installationtoken"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/repos/owner/repo/actions/runners/generate-jitconfig":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"encoded_jit_config":"encoded-jit","runner":{"id":7,"name":"runner-1"}}`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected", http.StatusNotFound)
		}
	})
	httpClient := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, r)
			return recorder.Result(), nil
		}),
	}
	client, err := ghapp.New("12345", testPrivateKeyPEM(t), ghapp.WithHTTPClient(httpClient))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return client
}

func testPrivateKeyPEM(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der := x509.MarshalPKCS1PrivateKey(key)
	block := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: der}
	return pem.EncodeToMemory(block)
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := os.Stat(path)
		if err == nil {
			return
		}
		if !os.IsNotExist(err) {
			t.Fatalf("stat %s: %v", path, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
