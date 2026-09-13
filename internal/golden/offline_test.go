package golden

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"goodkind.io/gha-mac-broker/internal/tart"
)

// diskTart preserves each image's files through clone, provision, and deletion.
// Only the VM commands are substituted; Builder runs its full lifecycle.
type diskTart struct {
	root           string
	provisionCount int
	corruptReceipt []byte
	cancelProbe    context.CancelFunc
	cancelOn       string
}

func newDiskTart(t *testing.T) *diskTart {
	t.Helper()
	return &diskTart{root: t.TempDir()}
}

func (d *diskTart) file(name, path string) string {
	return filepath.Join(d.root, name, filepath.Base(path))
}

func (d *diskTart) List(context.Context) ([]string, error) {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names, nil
}

func (d *diskTart) Clone(_ context.Context, source, name string, _ bool) error {
	dest := filepath.Join(d.root, name)
	if err := os.Mkdir(dest, 0o755); err != nil {
		return err
	}
	if strings.HasPrefix(source, "ghcr.io/") {
		return nil
	}
	entries, err := os.ReadDir(filepath.Join(d.root, source))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		body, err := os.ReadFile(d.file(source, entry.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(d.file(name, entry.Name()), body, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func (*diskTart) BootCommand(context.Context, string, tart.BootOptions) *exec.Cmd {
	return exec.Command("true")
}

func (*diskTart) Stop(context.Context, string) error { return nil }

func (*diskTart) IP(context.Context, string) (string, error) { return "192.168.64.2", nil }

func (d *diskTart) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.RemoveAll(filepath.Join(d.root, name))
}

func (d *diskTart) Exec(ctx context.Context, name string, argv ...string) ([]byte, error) {
	if d.cancelProbe != nil && strings.Contains(name, "-fpcheck-") && argv[0] == d.cancelOn {
		d.cancelProbe()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fingerprint, ok := fingerprintFromProvision(argv); ok {
		d.provisionCount++
		var receipt RunnerReceipt
		for index := 0; index+1 < len(argv); index++ {
			switch argv[index] {
			case "-runner-version":
				receipt.Version = argv[index+1]
			case "-runner-digest":
				receipt.TarballDigest = argv[index+1]
			}
		}
		body, err := json.Marshal(receipt)
		if err != nil {
			return nil, err
		}
		if d.corruptReceipt != nil {
			body = d.corruptReceipt
		}
		for path, content := range map[string][]byte{
			FingerprintPath: []byte(fingerprint), RunnerReceiptPath: body,
			"run.sh": []byte("runner"), GuestAgentPlistPath: GuestAgentPlist(),
		} {
			if err := os.WriteFile(d.file(name, path), content, 0o644); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	if len(argv) == 2 && argv[0] == "cat" {
		return os.ReadFile(d.file(name, argv[1]))
	}
	if len(argv) == 2 && argv[0] == "printenv" {
		return []byte("/Users/admin\n"), nil
	}
	if len(argv) == 3 && argv[0] == "test" && argv[1] == "-f" {
		_, err := os.Stat(d.file(name, argv[2]))
		return nil, err
	}
	return nil, nil
}

func TestEnsureGoldenCleansProbeAfterCancellation(t *testing.T) {
	for _, cancelOn := range []string{"true", "cat"} {
		t.Run(cancelOn, func(t *testing.T) {
			vm, image := buildDiskGolden(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			vm.cancelProbe, vm.cancelOn = cancel, cancelOn
			builder := New(vm)
			builder.resolveRunner = func(ctx context.Context) (string, error) { return "", ctx.Err() }
			if _, err := builder.EnsureGolden(ctx, EnsureOptions{Image: image}); err == nil {
				t.Fatal("canceled ensure succeeded")
			}
			names, err := vm.List(t.Context())
			if err != nil || len(names) != 1 || names[0] != NameForImage(image) {
				t.Fatalf("probe leaked after cancellation: %v, %v", names, err)
			}
		})
	}
}

func buildDiskGolden(t *testing.T) (*diskTart, string) {
	t.Helper()
	image := "ghcr.io/cirruslabs/macos-tahoe-xcode:26.5"
	name := NameForImage(image)
	vm := newDiskTart(t)
	builder := New(vm)
	builder.runnerDigest = fakeDigester
	if err := builder.Build(t.Context(), Options{
		BaseImage: image, GoldenName: name, BuildVM: name + "-build", RunnerVersion: "2.99.0",
	}); err != nil {
		t.Fatal(err)
	}
	return vm, image
}

func TestEnsureGoldenRejectsStaleCacheOffline(t *testing.T) {
	for _, scenario := range []string{"explicit version", "missing receipt", "malformed receipt", "empty version", "invalid digest", "changed digest", "old binary", "different image", "missing runner", "missing agent"} {
		t.Run(scenario, func(t *testing.T) {
			vm, image := buildDiskGolden(t)
			name := NameForImage(image)
			requestedVersion := ""
			var path string
			var body []byte
			switch scenario {
			case "explicit version":
				requestedVersion = "2.100.0"
			case "missing receipt":
				path = RunnerReceiptPath
			case "missing runner":
				path = "run.sh"
			case "missing agent":
				path = GuestAgentPlistPath
			case "malformed receipt":
				path, body = RunnerReceiptPath, []byte("{")
			case "empty version":
				path, body = RunnerReceiptPath, []byte(fmt.Sprintf(`{"version":"","tarball_digest":%q}`, sha256Bytes([]byte("fake runner tarball"))))
			case "invalid digest":
				path, body = RunnerReceiptPath, []byte(`{"version":"2.99.0","tarball_digest":"bad"}`)
			case "changed digest":
				path, body = RunnerReceiptPath, []byte(fmt.Sprintf(`{"version":"2.99.0","tarball_digest":%q}`, sha256Bytes([]byte("changed tarball"))))
			case "old binary":
				path, body = FingerprintPath, []byte(fingerprintWithRunner(image, sha256Bytes([]byte("old binary")), RunnerReceipt{Version: "2.99.0", TarballDigest: sha256Bytes([]byte("fake runner tarball"))}))
			case "different image":
				path, body = FingerprintPath, []byte(fingerprintWithRunner("another-image", "binary", RunnerReceipt{Version: "2.99.0", TarballDigest: sha256Bytes([]byte("fake runner tarball"))}))
			}
			if path != "" {
				if body == nil {
					if err := os.Remove(vm.file(name, path)); err != nil {
						t.Fatal(err)
					}
				} else if err := os.WriteFile(vm.file(name, path), body, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			offline := New(vm)
			offline.resolveRunner = func(context.Context) (string, error) { return "", errors.New("offline resolver") }
			offline.runnerDigest = func(context.Context, string) (string, error) { return "", errors.New("offline digest") }
			if _, err := offline.EnsureGolden(t.Context(), EnsureOptions{Image: image, RunnerVersion: requestedVersion}); err == nil {
				t.Fatal("accepted stale cached image offline")
			}
			if _, err := os.Stat(filepath.Join(vm.root, name)); err != nil {
				t.Fatalf("failed rebuild removed live golden: %v", err)
			}
		})
	}
}

func TestBuildRejectsUnusableRunnerReceiptBeforePromotion(t *testing.T) {
	for _, receipt := range []string{"", "{", `{"version":"2.99.0","tarball_digest":"bad"}`, fmt.Sprintf(`{"version":"2.100.0","tarball_digest":%q}`, sha256Bytes([]byte("fake runner tarball")))} {
		t.Run(receipt, func(t *testing.T) {
			vm, image := buildDiskGolden(t)
			name := NameForImage(image)
			before, err := os.ReadFile(vm.file(name, RunnerReceiptPath))
			if err != nil {
				t.Fatal(err)
			}
			vm.corruptReceipt = []byte(receipt)
			builder := New(vm)
			builder.runnerDigest = fakeDigester
			if err := builder.Build(t.Context(), Options{BaseImage: image, GoldenName: name, BuildVM: name + "-build", RunnerVersion: "2.99.0"}); err == nil {
				t.Fatal("promoted golden with invalid runner receipt")
			}
			after, err := os.ReadFile(vm.file(name, RunnerReceiptPath))
			if err != nil || string(after) != string(before) {
				t.Fatalf("failed replacement changed live golden: %q, %v", after, err)
			}
		})
	}
}
