package deploy

import "testing"

// baseInputs returns inputs where the compiled artifact matches what is running,
// so each case flips exactly one field and asserts the resulting action.
func baseInputs() Inputs {
	return Inputs{
		CompiledBaseRef:          "base:1",
		RunningBaseRef:           "base:1",
		CompiledGuestFingerprint: "guest-fp",
		RunningGuestFingerprint:  "guest-fp",
		CompiledProtocolMajor:    1,
		GuestProtocolMajor:       1,
		CompiledHostFingerprint:  "host-fp",
		RunningHostFingerprint:   "host-fp",
		SupervisorPresent:        true,
	}
}

func TestDecideNoopWhenNothingChanged(t *testing.T) {
	if action := Decide(baseInputs()); action != ActionNoop {
		t.Fatalf("action = %s, want noop", action)
	}
}

func TestDecideBaseChangeRebuildsAndRecycles(t *testing.T) {
	in := baseInputs()
	in.CompiledBaseRef = "base:2"
	// A base change dominates even when the guest and host also differ, so it is the
	// only path that recycles VMs.
	in.CompiledGuestFingerprint = "guest-fp-new"
	in.CompiledHostFingerprint = "host-fp-new"
	if action := Decide(in); action != ActionGoldenRebuildRecycle {
		t.Fatalf("action = %s, want golden-rebuild-recycle", action)
	}
}

func TestDecideGuestFingerprintChangeReloadsGuestWhenProtocolCompatible(t *testing.T) {
	in := baseInputs()
	in.CompiledGuestFingerprint = "guest-fp-new"
	if action := Decide(in); action != ActionGuestReload {
		t.Fatalf("action = %s, want guest-reload", action)
	}
}

func TestDecideGuestFingerprintChangeRebuildsWhenProtocolIncompatible(t *testing.T) {
	in := baseInputs()
	in.CompiledGuestFingerprint = "guest-fp-new"
	in.CompiledProtocolMajor = 2
	in.GuestProtocolMajor = 1
	// A protocol break cannot re-adopt running jobs in place, so a guest change
	// across it falls back to the rebuild-and-recycle path.
	if action := Decide(in); action != ActionGoldenRebuildRecycle {
		t.Fatalf("action = %s, want golden-rebuild-recycle", action)
	}
}

func TestDecideHostOnlyChangeReloadsWorkerWhenSupervisorPresent(t *testing.T) {
	in := baseInputs()
	in.CompiledHostFingerprint = "host-fp-new"
	if action := Decide(in); action != ActionWorkerReload {
		t.Fatalf("action = %s, want worker-reload", action)
	}
}

func TestDecideHostOnlyChangeRestartsServiceWithoutSupervisor(t *testing.T) {
	in := baseInputs()
	in.CompiledHostFingerprint = "host-fp-new"
	in.SupervisorPresent = false
	if action := Decide(in); action != ActionServiceRestart {
		t.Fatalf("action = %s, want service-restart", action)
	}
}
