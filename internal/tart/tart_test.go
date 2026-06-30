package tart

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestLifecycleCommands(t *testing.T) {
	var calls [][]string
	stub := func(_ context.Context, _ string, args ...string) ([]byte, error) {
		calls = append(calls, args)
		return nil, nil
	}
	tt := New("tart")
	tt.run = stub
	ctx := context.Background()

	if err := tt.Clone(ctx, "golden", "warm-1", false); err != nil {
		t.Fatalf("Clone: %v", err)
	}
	if err := tt.Clone(ctx, "golden", "warm-2", true); err != nil {
		t.Fatalf("Clone insecure: %v", err)
	}
	if err := tt.Stop(ctx, "warm-1"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := tt.Delete(ctx, "warm-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if !slices.Equal(calls[0], []string{"clone", "golden", "warm-1"}) {
		t.Fatalf("clone args = %v, want %v", calls[0], []string{"clone", "golden", "warm-1"})
	}
	if !slices.Equal(calls[1], []string{"clone", "--insecure", "golden", "warm-2"}) {
		t.Fatalf("insecure clone args = %v, want %v", calls[1], []string{"clone", "--insecure", "golden", "warm-2"})
	}
}

func TestExecArgsAndExitCode(t *testing.T) {
	var gotArgs []string
	stub := func(_ context.Context, _ string, args ...string) ([]byte, error) {
		gotArgs = args
		if args[len(args)-1] == "false" {
			return []byte("boom"), errors.New("exit status 1")
		}
		return []byte("HELLO\n"), nil
	}
	tt := New("tart")
	tt.run = stub

	out, err := tt.Exec(context.Background(), "warm-1", "bash", "-lc", "echo HELLO")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if strings.TrimSpace(string(out)) != "HELLO" {
		t.Errorf("out = %q, want HELLO", out)
	}
	joined := strings.Join(gotArgs, " ")
	if !strings.HasPrefix(joined, "exec warm-1 bash -lc") {
		t.Errorf("exec args %q missing expected prefix", joined)
	}

	if _, err := tt.Exec(context.Background(), "warm-1", "false"); err == nil {
		t.Error("Exec should surface a non-zero guest exit code as an error")
	}
}

func TestListParsesNames(t *testing.T) {
	stub := func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[0] != "list" {
			t.Fatalf("expected list call, got %v", args)
		}
		return []byte(`[{"Name":"gha-golden","State":"stopped"},{"Name":"gha-runner-260628T170530-1","State":"running"}]`), nil
	}
	tt := New("tart")
	tt.run = stub
	names, err := tt.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := []string{"gha-golden", "gha-runner-260628T170530-1"}
	if len(names) != len(want) {
		t.Fatalf("names = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Errorf("names[%d] = %q, want %q", i, names[i], want[i])
		}
	}
}

func TestBootCommandArgs(t *testing.T) {
	tt := New("tart")
	cmd := tt.BootCommand(context.Background(), "warm-1", BootOptions{
		NoGraphics: true,
		Dirs:       []DirMount{{Name: "cache", Path: "/Users/x/.tart/cache"}},
	})
	got := strings.Join(cmd.Args, " ")
	for _, want := range []string{"run warm-1", "--no-graphics", "--dir cache:/Users/x/.tart/cache"} {
		if !strings.Contains(got, want) {
			t.Errorf("boot args %q missing %q", got, want)
		}
	}
}
