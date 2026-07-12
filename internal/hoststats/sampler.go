package hoststats

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Sampler periodically observes host stats and pool inventory and holds the
// latest Sample under a mutex for concurrent readers.
type Sampler struct {
	reader    Reader
	inventory func(context.Context) Inventory
	now       func() time.Time // clock seam; main injects time.Now, tests inject a fake

	mu       sync.Mutex
	latest   Sample
	enabled  bool
	interval time.Duration
}

// New returns a Sampler that reads host stats through reader and pool counts
// through inventory, configured with opts. now supplies the clock used to
// stamp each Sample; callers pass [time.Now] in production and a fake clock
// in tests.
func New(reader Reader, inventory func(context.Context) Inventory, now func() time.Time, opts Options) *Sampler {
	s := new(Sampler)
	s.reader = reader
	s.inventory = inventory
	s.now = now
	s.enabled = opts.Enabled
	s.interval = opts.Interval
	return s
}

// Latest returns a copy of the most recently recorded Sample. Before the
// first successful observation it returns the zero Sample.
func (s *Sampler) Latest() Sample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest
}

// Reconfigure updates the enabled flag and sampling interval under the mutex
// so a running Start loop picks up the change on its next iteration.
func (s *Sampler) Reconfigure(opts Options) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enabled = opts.Enabled
	s.interval = opts.Interval
}

// currentInterval returns the guarded interval. It exists for tests; Start
// reads the interval itself on each loop iteration.
func (s *Sampler) currentInterval() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.interval
}

// sampleOnce performs a single observation. When the sampler is disabled it
// returns immediately without touching latest. On a Reader error it leaves the
// last good Sample in place rather than overwriting it with a partial or zero
// one, so Latest() never regresses to stale-but-wrong data because of a
// transient read failure. The Reader logs which read failed, so sampleOnce
// does not log the error again.
func (s *Sampler) sampleOnce(ctx context.Context) {
	s.mu.Lock()
	enabled := s.enabled
	s.mu.Unlock()
	if !enabled {
		return
	}

	h, err := s.reader.Read(ctx)
	if err != nil {
		return
	}
	inv := s.inventory(ctx)
	sample := Sample{Host: h, Inventory: inv, SampledAt: s.now()}

	s.mu.Lock()
	s.latest = sample
	s.mu.Unlock()

	slog.InfoContext(ctx, "host stats sampled",
		"idle_pct", h.IdlePct,
		"load1", h.Load1,
		"mem_used_pct", h.MemUsedPct,
		"swap_out", h.SwapOut,
		"disk_used_pct", h.DiskUsedPct,
		"uptime_seconds", h.UptimeSeconds,
		"running_vms", inv.RunnerCount,
		"busy", inv.Busy,
		"queued", inv.Queued,
	)
}

// sampleOnceSafe runs sampleOnce and recovers from a panic so one bad
// observation, such as a gopsutil panic, logs and is skipped rather than
// killing the sampler goroutine for the life of the daemon.
func (s *Sampler) sampleOnceSafe(ctx context.Context) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.ErrorContext(ctx, "host stats sample panic recovered; skipping this observation", "err", recovered)
		}
	}()
	s.sampleOnce(ctx)
}

// Start launches the sampler's ticker goroutine: an immediate first pass,
// then one sample per interval until ctx is done. Each observation runs
// through sampleOnceSafe, so a panic in one sample is logged and skipped and
// the loop keeps sampling. The goroutine also carries a top-level recover as a
// backstop for a panic outside the sample path. The interval is read fresh on
// each iteration so a concurrent Reconfigure takes effect without restarting
// the loop.
func (s *Sampler) Start(ctx context.Context) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(ctx, "host stats sampler goroutine panic recovered", "err", recovered)
			}
		}()
		s.sampleOnceSafe(ctx)
		timer := time.NewTimer(s.currentInterval())
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				s.sampleOnceSafe(ctx)
				timer.Reset(s.currentInterval())
			}
		}
	}()
}
