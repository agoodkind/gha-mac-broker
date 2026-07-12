// Package hoststats samples host system stats (CPU, memory, swap, disk, load,
// uptime) on a timer and correlates them with the broker's own pool inventory.
//
// The package depends on neither internal/config nor internal/runnerpool: the
// live-reconfigurable knobs arrive as Options and the pool counts arrive
// through a callback, so hoststats stays testable with a fake Reader that
// never touches the real host.
package hoststats

import (
	"context"
	"time"
)

// Inventory is the broker's own pool state, correlated into each sample.
type Inventory struct {
	RunnerCount int `json:"runner_count"`
	Idle        int `json:"idle"`
	Busy        int `json:"busy"`
	Queued      int `json:"queued"`
}

// Host holds the host-derived stats a Reader returns.
type Host struct {
	UserPct       float64 `json:"user_pct"`
	SysPct        float64 `json:"sys_pct"`
	IdlePct       float64 `json:"idle_pct"`
	Load1         float64 `json:"load1"`
	Load5         float64 `json:"load5"`
	Load15        float64 `json:"load15"`
	MemTotal      uint64  `json:"mem_total"`
	MemUsed       uint64  `json:"mem_used"`
	MemAvailable  uint64  `json:"mem_available"`
	MemUsedPct    float64 `json:"mem_used_pct"`
	SwapTotal     uint64  `json:"swap_total"`
	SwapUsed      uint64  `json:"swap_used"`
	SwapIn        uint64  `json:"swap_in"`
	SwapOut       uint64  `json:"swap_out"`
	DiskPath      string  `json:"disk_path"`
	DiskTotal     uint64  `json:"disk_total"`
	DiskUsed      uint64  `json:"disk_used"`
	DiskFree      uint64  `json:"disk_free"`
	DiskUsedPct   float64 `json:"disk_used_pct"`
	UptimeSeconds uint64  `json:"uptime_seconds"`
	BootTime      uint64  `json:"boot_time"`
}

// Sample is one full observation: host stats + pool inventory + when.
type Sample struct {
	Host      Host      `json:"host"`
	Inventory Inventory `json:"inventory"`
	SampledAt time.Time `json:"sampled_at"`
}

// Reader reads host-derived stats. Fakeable in tests.
type Reader interface {
	Read(ctx context.Context) (Host, error)
}

// Options are the live-reconfigurable knobs.
type Options struct {
	Enabled  bool
	Interval time.Duration
}
