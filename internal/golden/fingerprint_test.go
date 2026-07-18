package golden

import "testing"

func baseFingerprintInputs() FingerprintInputs {
	return FingerprintInputs{
		BaseImageRef:        "ghcr.io/cirruslabs/macos-tahoe-xcode:26.5",
		RunnerVersion:       "2.335.1",
		RunnerTarballDigest: "aaaa",
		BinaryDigest:        "bbbb",
		Payloads: []PayloadDigest{
			{Name: "guest-agent.plist", Digest: "cccc"},
		},
	}
}

func TestFingerprintIsDeterministic(t *testing.T) {
	inputs := baseFingerprintInputs()
	first := Fingerprint(inputs)
	second := Fingerprint(inputs)
	if first != second {
		t.Fatalf("fingerprint not deterministic: %q != %q", first, second)
	}
	if first == "" {
		t.Fatal("fingerprint is empty")
	}
}

func TestFingerprintIndependentOfPayloadOrder(t *testing.T) {
	ordered := baseFingerprintInputs()
	ordered.Payloads = []PayloadDigest{
		{Name: "a.plist", Digest: "1"},
		{Name: "b.plist", Digest: "2"},
	}
	reversed := baseFingerprintInputs()
	reversed.Payloads = []PayloadDigest{
		{Name: "b.plist", Digest: "2"},
		{Name: "a.plist", Digest: "1"},
	}
	if Fingerprint(ordered) != Fingerprint(reversed) {
		t.Fatal("fingerprint changed with payload ordering; want order-independent")
	}
}

func TestFingerprintChangesWhenAnyInputChanges(t *testing.T) {
	baseline := Fingerprint(baseFingerprintInputs())

	mutations := map[string]func(*FingerprintInputs){
		"base image": func(in *FingerprintInputs) {
			in.BaseImageRef = "ghcr.io/cirruslabs/macos-sonoma-xcode:15.4"
		},
		"runner version": func(in *FingerprintInputs) {
			in.RunnerVersion = "2.999.0"
		},
		"runner tarball digest": func(in *FingerprintInputs) {
			in.RunnerTarballDigest = "zzzz"
		},
		"binary digest": func(in *FingerprintInputs) {
			in.BinaryDigest = "zzzz"
		},
		"payload digest": func(in *FingerprintInputs) {
			in.Payloads = []PayloadDigest{{Name: "guest-agent.plist", Digest: "dddd"}}
		},
		"payload name": func(in *FingerprintInputs) {
			in.Payloads = []PayloadDigest{{Name: "renamed.plist", Digest: "cccc"}}
		},
		"added payload": func(in *FingerprintInputs) {
			in.Payloads = append(in.Payloads, PayloadDigest{Name: "extra", Digest: "eeee"})
		},
	}

	for name, mutate := range mutations {
		inputs := baseFingerprintInputs()
		mutate(&inputs)
		if got := Fingerprint(inputs); got == baseline {
			t.Errorf("fingerprint unchanged after mutating %s; want a different fingerprint", name)
		}
	}
}
