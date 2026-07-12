package hoststats

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeReader is a Reader whose Read result and error are controlled by the
// test, so sampler tests never touch the real host.
type fakeReader struct {
	host Host
	err  error
}

func (r *fakeReader) Read(_ context.Context) (Host, error) {
	return r.host, r.err
}

func fakeInventory(inv Inventory) func(context.Context) Inventory {
	return func(_ context.Context) Inventory {
		return inv
	}
}

// fakeNow returns a clock seam that always reports fixedTime, so tests never
// depend on wall-clock timing.
func fakeNow(fixedTime time.Time) func() time.Time {
	return func() time.Time {
		return fixedTime
	}
}

func TestSamplerSampleOnceRecordsReaderAndInventoryOutput(t *testing.T) {
	wantHost := Host{UserPct: 12.5, IdlePct: 80, MemUsedPct: 40, DiskUsedPct: 55}
	wantInv := Inventory{RunnerCount: 3, Idle: 1, Busy: 2, Queued: 4}
	wantTime := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	reader := &fakeReader{host: wantHost}
	s := New(reader, fakeInventory(wantInv), fakeNow(wantTime), Options{Enabled: true, Interval: time.Second})

	s.sampleOnce(context.Background())

	got := s.Latest()
	if got.Host != wantHost {
		t.Fatalf("Latest().Host = %+v, want %+v", got.Host, wantHost)
	}
	if got.Inventory != wantInv {
		t.Fatalf("Latest().Inventory = %+v, want %+v", got.Inventory, wantInv)
	}
	if !got.SampledAt.Equal(wantTime) {
		t.Fatalf("Latest().SampledAt = %v, want %v", got.SampledAt, wantTime)
	}
}

func TestSamplerSampleOnceDisabledDoesNotOverwriteLatest(t *testing.T) {
	firstHost := Host{UserPct: 10}
	firstInv := Inventory{RunnerCount: 1}
	firstTime := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	reader := &fakeReader{host: firstHost}
	s := New(reader, fakeInventory(firstInv), fakeNow(firstTime), Options{Enabled: true, Interval: time.Second})
	s.sampleOnce(context.Background())
	want := s.Latest()

	s.Reconfigure(Options{Enabled: false, Interval: time.Second})
	reader.host = Host{UserPct: 99}
	s.sampleOnce(context.Background())

	got := s.Latest()
	if got != want {
		t.Fatalf("Latest() after disabled sampleOnce = %+v, want unchanged %+v", got, want)
	}
}

func TestSamplerReconfigureUpdatesInterval(t *testing.T) {
	reader := &fakeReader{host: Host{}}
	s := New(reader, fakeInventory(Inventory{}), fakeNow(time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)), Options{Enabled: true, Interval: time.Second})

	s.Reconfigure(Options{Enabled: true, Interval: 5 * time.Minute})

	if got := s.currentInterval(); got != 5*time.Minute {
		t.Fatalf("currentInterval() = %v, want %v", got, 5*time.Minute)
	}
}

func TestSamplerSampleOnceReadErrorPreservesLatest(t *testing.T) {
	goodHost := Host{UserPct: 42}
	goodInv := Inventory{RunnerCount: 7}
	goodTime := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	reader := &fakeReader{host: goodHost}
	s := New(reader, fakeInventory(goodInv), fakeNow(goodTime), Options{Enabled: true, Interval: time.Second})
	s.sampleOnce(context.Background())
	want := s.Latest()

	reader.err = errors.New("boom")
	reader.host = Host{UserPct: 0}
	s.sampleOnce(context.Background())

	got := s.Latest()
	if got != want {
		t.Fatalf("Latest() after read error = %+v, want preserved %+v", got, want)
	}
}

func TestSamplerSampleOnceReadErrorBeforeAnySampleLeavesHasLatestFalse(t *testing.T) {
	reader := &fakeReader{err: errors.New("boom")}
	s := New(reader, fakeInventory(Inventory{}), fakeNow(time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)), Options{Enabled: true, Interval: time.Second})

	s.sampleOnce(context.Background())

	if s.hasLatest {
		t.Fatalf("hasLatest = true after a failed first sample, want false")
	}
}
