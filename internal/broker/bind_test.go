package broker

import (
	"strings"
	"testing"
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
