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
	want := "cd ~/actions-runner && ./run.sh --jitconfig 'encoded-jit'"
	if command != want {
		t.Fatalf("single-slot command = %q, want %q", command, want)
	}
}

func TestRunJobRemoteCommandUsesSlotHomeAndTMPDIR(t *testing.T) {
	command := runJobRemoteCommand("encoded-jit", 1, 2)
	for _, fragment := range []string{
		"actions-runner-1",
		`TMPDIR="$HOME/tmp-1"`,
		"./run.sh --jitconfig 'encoded-jit'",
	} {
		if !strings.Contains(command, fragment) {
			t.Fatalf("slot command = %q, want fragment %q", command, fragment)
		}
	}
	if strings.Contains(command, "cd ~/actions-runner &&") {
		t.Fatalf("slot command = %q, want no legacy runner home", command)
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
	} {
		if !strings.Contains(command, fragment) {
			t.Fatalf("clone command = %q, want fragment %q", command, fragment)
		}
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
