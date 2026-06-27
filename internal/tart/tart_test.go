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
	tt := New("tart")
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
