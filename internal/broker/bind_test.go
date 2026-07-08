package broker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"goodkind.io/gha-mac-broker/internal/config"
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
