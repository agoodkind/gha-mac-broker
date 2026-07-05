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
