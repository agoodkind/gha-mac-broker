// Package deploy chooses the least-destructive action that reconciles a running
// broker with a freshly compiled artifact. It compares the compiled base image,
// guest fingerprint, guest protocol, and host binary against what is running and
// picks a single action, so a routine deploy reloads a worker or a guest with no
// job death and only a base-image change ever recycles a VM.
package deploy

// Action is the reconciliation step a deploy applies. The values are ordered
// least-destructive first, but each Decide result is chosen by comparison, not by
// rank, so the numeric order is only for readable status output.
type Action int

const (
	// ActionNoop means the running broker already matches the compiled artifact.
	ActionNoop Action = iota
	// ActionWorkerReload replaces the host worker in place through the supervisor,
	// so the listener stays up and adoption reattaches running jobs.
	ActionWorkerReload
	// ActionServiceRestart boots out and bootstraps the whole host service. It is
	// the fallback when no supervisor is present to reload the worker in place;
	// adoption still reattaches jobs after the restart.
	ActionServiceRestart
	// ActionGuestReload marks a guest fingerprint change that keeps a compatible
	// protocol. The deploy command reports it and holds it for the operator rather
	// than touching the pool.
	ActionGuestReload
	// ActionGoldenRebuildRecycle rebuilds the golden image and rolls the pool one
	// VM at a time. It is the only action that destroys VMs.
	ActionGoldenRebuildRecycle
)

// String names an Action for logs and the deploy plan output.
func (a Action) String() string {
	switch a {
	case ActionNoop:
		return "noop"
	case ActionWorkerReload:
		return "worker-reload"
	case ActionServiceRestart:
		return "service-restart"
	case ActionGuestReload:
		return "guest-reload"
	case ActionGoldenRebuildRecycle:
		return "golden-rebuild-recycle"
	default:
		return "unknown"
	}
}

// Inputs are the compiled-versus-running identities Decide compares. The compiled
// values come from the artifact being deployed; the running values come from the
// live host binary and a guest Hello probe.
type Inputs struct {
	// CompiledBaseRef is the base image the compiled artifact targets.
	CompiledBaseRef string
	// RunningBaseRef is the base image the running pool VMs were cloned from.
	RunningBaseRef string
	// CompiledGuestFingerprint is the golden fingerprint the compiled artifact bakes.
	CompiledGuestFingerprint string
	// RunningGuestFingerprint is the fingerprint the running guest reports via Hello.
	RunningGuestFingerprint string
	// CompiledProtocolMajor is the guest protocol major the compiled artifact speaks.
	CompiledProtocolMajor uint32
	// GuestProtocolMajor is the protocol major the running guest reports via Hello.
	GuestProtocolMajor uint32
	// CompiledHostFingerprint identifies the compiled host binary.
	CompiledHostFingerprint string
	// RunningHostFingerprint identifies the running host binary (the supervisor).
	RunningHostFingerprint string
	// SupervisorPresent reports whether a host supervisor is running and can reload
	// the worker in place. When false the host change falls back to a service restart.
	SupervisorPresent bool
}

// Decide returns the single least-destructive action that reconciles the running
// broker with the compiled artifact. It checks the most destructive-to-fix
// difference first so a base change wins over a guest change and a guest change
// wins over a host-only change, and it never recycles a VM unless the base image
// itself changed.
func Decide(in Inputs) Action {
	// A base image change is the only difference that requires new VMs, so it
	// dominates every other difference and takes the sole VM-destroying path.
	if in.CompiledBaseRef != in.RunningBaseRef {
		return ActionGoldenRebuildRecycle
	}
	// A guest fingerprint change with a compatible protocol reloads the guest in
	// place, so running jobs re-adopt and no VM recycles. An incompatible protocol
	// cannot re-adopt across the break, so it falls back to a golden rebuild.
	if in.CompiledGuestFingerprint != in.RunningGuestFingerprint {
		if in.GuestProtocolMajor == in.CompiledProtocolMajor {
			return ActionGuestReload
		}
		return ActionGoldenRebuildRecycle
	}
	// A host-only change replaces the worker in place when a supervisor is present,
	// otherwise it restarts the whole service; adoption reattaches jobs either way.
	if in.CompiledHostFingerprint != in.RunningHostFingerprint {
		if in.SupervisorPresent {
			return ActionWorkerReload
		}
		return ActionServiceRestart
	}
	return ActionNoop
}
