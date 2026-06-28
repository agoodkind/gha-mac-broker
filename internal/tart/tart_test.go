package tart

import (
	"context"
	"strings"
	"testing"
)

func TestLifecycleCommands(t *testing.T) {
	var calls [][]string
	stub := func(_ context.Context, _ string, args ...string) ([]byte, error) {
		calls = append(calls, args)
		if args[0] == "ip" {
			return []byte("192.168.64.7\n"), nil
		}
		return nil, nil
	}
	tt := New("tart", "")
	tt.run = stub
	ctx := context.Background()

	if err := tt.Clone(ctx, "golden", "warm-1"); err != nil {
		t.Fatalf("Clone: %v", err)
	}
	ip, err := tt.IP(ctx, "warm-1")
	if err != nil {
		t.Fatalf("IP: %v", err)
	}
	if ip != "192.168.64.7" {
		t.Errorf("ip = %q, want trimmed 192.168.64.7", ip)
	}
	if err := tt.Stop(ctx, "warm-1"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := tt.Delete(ctx, "warm-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if len(calls) == 0 || calls[0][0] != "clone" {
		t.Fatalf("first call should be clone, got %v", calls)
	}
}

func TestListParsesNames(t *testing.T) {
	stub := func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[0] != "list" {
			t.Fatalf("expected list call, got %v", args)
		}
		return []byte(`[{"Name":"gha-golden","State":"stopped"},{"Name":"gha-runner-260628T170530-1","State":"running"}]`), nil
	}
	tt := New("tart", "")
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
	tt := New("tart", "")
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

func TestIPUsesResolver(t *testing.T) {
	var gotArgs []string
	stub := func(_ context.Context, _ string, args ...string) ([]byte, error) {
		gotArgs = args
		return []byte("fd00::1\n"), nil
	}
	tt := New("tart", "agent")
	tt.run = stub
	ip, err := tt.IP(context.Background(), "warm-1")
	if err != nil {
		t.Fatalf("IP: %v", err)
	}
	if ip != "fd00::1" {
		t.Errorf("ip = %q, want fd00::1", ip)
	}
	joined := strings.Join(gotArgs, " ")
	if !strings.Contains(joined, "ip warm-1") || !strings.Contains(joined, "--resolver agent") {
		t.Errorf("ip args %q missing resolver", joined)
	}
}

func TestBootCommandBridged(t *testing.T) {
	tt := New("tart", "")
	cmd := tt.BootCommand(context.Background(), "warm-1", BootOptions{NoGraphics: true, BridgeInterface: "en0"})
	got := strings.Join(cmd.Args, " ")
	if !strings.Contains(got, "--net-bridged=en0") {
		t.Errorf("boot args %q missing --net-bridged=en0", got)
	}
}
