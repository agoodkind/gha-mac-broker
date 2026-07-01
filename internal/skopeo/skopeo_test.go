package skopeo

import (
	"context"
	"slices"
	"testing"
)

func TestCopyToOCILayoutArgs(t *testing.T) {
	var gotBin string
	var gotArgs []string
	stub := func(_ context.Context, bin string, args ...string) ([]byte, error) {
		gotBin = bin
		gotArgs = slices.Clone(args)
		return nil, nil
	}
	client := New("skopeo", 16)
	client.run = stub

	err := client.CopyToOCILayout(
		context.Background(),
		"ghcr.io/cirruslabs/macos-tahoe-xcode:26.5",
		"/tmp/layout",
		"26.5",
		"darwin",
		"arm64",
	)
	if err != nil {
		t.Fatalf("CopyToOCILayout: %v", err)
	}

	if gotBin != "skopeo" {
		t.Fatalf("bin = %q, want skopeo", gotBin)
	}
	wantArgs := []string{
		"copy",
		"--image-parallel-copies",
		"16",
		"--override-os",
		"darwin",
		"--override-arch",
		"arm64",
		"docker://ghcr.io/cirruslabs/macos-tahoe-xcode:26.5",
		"oci:/tmp/layout:26.5",
	}
	if !slices.Equal(gotArgs, wantArgs) {
		t.Fatalf("args = %v, want %v", gotArgs, wantArgs)
	}
}
