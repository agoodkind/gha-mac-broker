package vmssh

import (
	"context"
	"strings"
	"testing"
)

func TestRunBuildsSSHArgs(t *testing.T) {
	var got []string
	stub := func(_ context.Context, name string, args ...string) ([]byte, error) {
		got = append([]string{name}, args...)
		return []byte("ok"), nil
	}
	r := New("admin", "/home/me/.ssh/id_broker")
	r.run = stub

	if _, err := r.Run(context.Background(), "192.168.64.7", "echo hi"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	joined := strings.Join(got, " ")
	for _, want := range []string{
		"ssh", "-i /home/me/.ssh/id_broker",
		"StrictHostKeyChecking=no", "admin@192.168.64.7", "echo hi",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("ssh args %q missing %q", joined, want)
		}
	}
}
