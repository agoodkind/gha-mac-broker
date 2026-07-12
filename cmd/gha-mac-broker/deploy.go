package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"syscall"

	"goodkind.io/gha-mac-broker/internal/config"
	"goodkind.io/gha-mac-broker/internal/deploy"
)

// hostSupervisorPID and signalProcess are the seams the reload path depends on, as
// variables so tests can drive both the worker-reload and service-restart branches
// without a running supervisor.
var (
	hostSupervisorPID = liveSupervisorPID
	signalProcess     = syscall.Kill
)

// reloadOrRestart reconciles the running service with a freshly applied binary,
// preferring the least-destructive action. It signals a live host supervisor to
// swap its worker in place, which keeps the listener up and lets adoption reattach
// running jobs; with no supervisor it falls back to the full service restart. Both
// paths survive running jobs. It reports whether any managed service was acted on.
func reloadOrRestart(ctx context.Context) (bool, error) {
	if pid, ok := hostSupervisorPID(); ok {
		err := signalProcess(pid, syscall.SIGHUP)
		if err == nil {
			slog.InfoContext(ctx, "worker reload signaled to host supervisor", "pid", pid)
			return true, nil
		}
		slog.WarnContext(ctx, "worker reload signal failed; falling back to service restart", "err", err, "pid", pid)
	}
	return restartManagedService(ctx)
}

// runDeploy prints the least-destructive action that reconciles the running broker
// with the compiled artifact, and with -execute applies only the safe host-side
// action. The guest-reload and golden-rebuild actions touch the production pool, so
// they are reported and held for the operator rather than run here.
func runDeploy(ctx context.Context, args []string) error {
	return runDeployWithWriters(ctx, args, os.Stdout, os.Stderr)
}

func runDeployWithWriters(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("deploy", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "path to broker config TOML (default: XDG path)")
	runningBase := fs.String("running-base", "", "base image the running pool uses (default: config base)")
	compiledGuestFP := fs.String("compiled-guest-fingerprint", "", "golden fingerprint the compiled artifact bakes")
	runningGuestFP := fs.String("running-guest-fingerprint", "", "golden fingerprint the running guest reports via Hello")
	compiledProtocol := fs.Uint("compiled-guest-protocol", 0, "guest protocol major the compiled artifact speaks")
	guestProtocol := fs.Uint("running-guest-protocol", 0, "guest protocol major the running guest reports via Hello")
	compiledHostFP := fs.String("compiled-host-fingerprint", "", "host binary fingerprint of the compiled artifact")
	runningHostFP := fs.String("running-host-fingerprint", "", "host binary fingerprint currently running")
	execute := fs.Bool("execute", false, "apply the chosen host-side action; guest and base actions stay held")
	if err := fs.Parse(args); err != nil {
		slog.ErrorContext(ctx, "deploy flag parse failed", "err", err)
		return fmt.Errorf("deploy flags: %w", err)
	}
	if *configPath == "" {
		*configPath = config.DefaultConfigPath()
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.ErrorContext(ctx, "deploy load config failed", "err", err, "path", *configPath)
		return fmt.Errorf("deploy: load config: %w", err)
	}
	runningBaseRef := *runningBase
	if runningBaseRef == "" {
		runningBaseRef = cfg.Tart.BaseImage
	}
	_, supervisorPresent := hostSupervisorPID()

	inputs := deploy.Inputs{
		CompiledBaseRef:          cfg.Tart.BaseImage,
		RunningBaseRef:           runningBaseRef,
		CompiledGuestFingerprint: *compiledGuestFP,
		RunningGuestFingerprint:  *runningGuestFP,
		CompiledProtocolMajor:    clampUint32(*compiledProtocol),
		GuestProtocolMajor:       clampUint32(*guestProtocol),
		CompiledHostFingerprint:  *compiledHostFP,
		RunningHostFingerprint:   *runningHostFP,
		SupervisorPresent:        supervisorPresent,
	}
	action := deploy.Decide(inputs)
	writeUserLine(stdout, "deploy plan: "+action.String())
	if !*execute {
		return nil
	}
	return executeDeploy(ctx, action, stdout)
}

// executeDeploy applies only the host-side actions, which are safe to run from the
// deploy command. The guest and base actions touch the production pool and are held
// for the operator, so they are reported without being applied.
func executeDeploy(ctx context.Context, action deploy.Action, stdout io.Writer) error {
	switch action {
	case deploy.ActionNoop:
		writeUserLine(stdout, "deploy: nothing to reconcile")
		return nil
	case deploy.ActionWorkerReload, deploy.ActionServiceRestart:
		changed, err := reloadOrRestart(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "deploy reconcile host failed", "err", err, "action", action.String())
			return fmt.Errorf("deploy: reconcile host: %w", err)
		}
		if changed {
			writeUserLine(stdout, "deploy: host reconciled ("+action.String()+")")
			return nil
		}
		writeUserLine(stdout, "deploy: no managed service to reconcile")
		return nil
	case deploy.ActionGuestReload, deploy.ActionGoldenRebuildRecycle:
		writeUserLine(stdout, "deploy: "+action.String()+" touches the pool and is held for the operator; not applied")
		return nil
	default:
		err := fmt.Errorf("deploy: unknown action %s", action)
		slog.ErrorContext(ctx, "deploy unknown action", "err", err, "action", action.String())
		return err
	}
}

// clampUint32 narrows a uint flag value to uint32, saturating at the maximum so an
// out-of-range protocol major cannot silently wrap. Protocol majors are small
// version numbers in practice, so the clamp never changes a real input.
func clampUint32(value uint) uint32 {
	const maxUint32 = uint(^uint32(0))
	if value > maxUint32 {
		return ^uint32(0)
	}
	return uint32(value) // #nosec G115 -- bounded by the maxUint32 check above
}
