package tart

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
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

func TestExecTeeWritesOutputToSink(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-tart")
	script := "#!/usr/bin/env bash\nprintf 'stdout-line\\n'\nprintf 'stderr-line\\n' >&2\n"
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake tart: %v", err)
	}
	tt := New(bin)
	var sink bytes.Buffer

	out, err := tt.ExecTee(context.Background(), "warm-1", &sink, "bash", "-lc", "echo ignored")
	if err != nil {
		t.Fatalf("ExecTee: %v", err)
	}
	for _, want := range []string{"stdout-line", "stderr-line"} {
		if !strings.Contains(sink.String(), want) {
			t.Fatalf("sink = %q, want %q", sink.String(), want)
		}
		if !strings.Contains(string(out), want) {
			t.Fatalf("out = %q, want %q", out, want)
		}
	}
}

func TestExecTeeReturnsBoundedTailAndStreamsFullOutput(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-tart")
	script := "#!/usr/bin/env bash\nfor i in {1..70000}; do printf 'x'; done\nprintf 'tail-marker\\n'\n"
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake tart: %v", err)
	}
	tt := New(bin)
	var sink bytes.Buffer

	out, err := tt.ExecTee(context.Background(), "warm-1", &sink, "bash", "-lc", "echo ignored")
	if err != nil {
		t.Fatalf("ExecTee: %v", err)
	}
	if sink.Len() != 70012 {
		t.Fatalf("sink len = %d, want 70012", sink.Len())
	}
	const expectedTailLimit = 64 * 1024
	if len(out) > expectedTailLimit {
		t.Fatalf("out len = %d, want at most %d", len(out), expectedTailLimit)
	}
	if !strings.Contains(string(out), "tail-marker") {
		t.Fatalf("out = %q, want trailing marker", out)
	}
}

func TestExecTeeUsesInjectedRunner(t *testing.T) {
	var gotBin string
	var gotArgs []string
	stub := func(_ context.Context, bin string, sink io.Writer, args ...string) ([]byte, error) {
		gotBin = bin
		gotArgs = args
		if _, err := sink.Write([]byte("tee-line\n")); err != nil {
			return nil, err
		}
		return []byte("buffered-line\n"), nil
	}
	tt := New("fake-tart")
	tt.runTee = stub
	var sink bytes.Buffer

	out, err := tt.ExecTee(context.Background(), "warm-1", &sink, "bash", "-lc", "echo ignored")
	if err != nil {
		t.Fatalf("ExecTee: %v", err)
	}
	if gotBin != "fake-tart" {
		t.Fatalf("tee bin = %q, want fake-tart", gotBin)
	}
	wantArgs := []string{"exec", "warm-1", "bash", "-lc", "echo ignored"}
	if !slices.Equal(gotArgs, wantArgs) {
		t.Fatalf("tee args = %v, want %v", gotArgs, wantArgs)
	}
	if sink.String() != "tee-line\n" {
		t.Fatalf("sink = %q, want tee-line", sink.String())
	}
	if string(out) != "buffered-line\n" {
		t.Fatalf("out = %q, want buffered-line", out)
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

func TestBootCommandDetachCreatesOwnSession(t *testing.T) {
	tt := New("tart")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := tt.BootCommand(ctx, "warm-1", BootOptions{
		NoGraphics: true,
		Detach:     true,
	})

	if cmd.SysProcAttr == nil {
		t.Fatal("boot command SysProcAttr = nil, want detached process attributes")
	}
	if !cmd.SysProcAttr.Setsid {
		t.Fatal("boot command Setsid = false, want true")
	}
	if cmd.SysProcAttr.Setpgid {
		t.Fatal("boot command Setpgid = true, want false because Setsid already creates a process group")
	}
	if cmd.Cancel != nil {
		t.Fatal("boot command has context cancel hook, want detached command independent of caller cancellation")
	}
}
