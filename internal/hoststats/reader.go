package hoststats

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
)

// cpuSampleWindow is the interval between the two cpu.TimesWithContext
// snapshots gopsutilReader uses to split user/sys/idle percentages.
const cpuSampleWindow = 250 * time.Millisecond

// gopsutilReader is the production Reader, backed by gopsutil. It is
// exercised only by compilation and a live run, never by a unit test that
// touches the real host.
type gopsutilReader struct {
	diskPath string
}

// NewGopsutilReader returns a Reader that samples the real host, reporting
// disk usage for diskPath.
func NewGopsutilReader(diskPath string) Reader {
	return gopsutilReader{diskPath: diskPath}
}

// Read samples CPU, load, memory, swap, disk, and uptime from the host.
func (r gopsutilReader) Read(ctx context.Context) (Host, error) {
	userPct, sysPct, idlePct, err := r.readCPUPercents(ctx)
	if err != nil {
		return Host{}, err
	}

	loadStat, err := load.AvgWithContext(ctx)
	if err != nil {
		slog.WarnContext(ctx, "hoststats: read load average failed", "err", err)
		return Host{}, fmt.Errorf("hoststats: read load average: %w", err)
	}

	virtualMem, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		slog.WarnContext(ctx, "hoststats: read virtual memory failed", "err", err)
		return Host{}, fmt.Errorf("hoststats: read virtual memory: %w", err)
	}

	swapMem, err := mem.SwapMemoryWithContext(ctx)
	if err != nil {
		slog.WarnContext(ctx, "hoststats: read swap memory failed", "err", err)
		return Host{}, fmt.Errorf("hoststats: read swap memory: %w", err)
	}

	diskUsage, err := disk.UsageWithContext(ctx, r.diskPath)
	if err != nil {
		slog.WarnContext(ctx, "hoststats: read disk usage failed", "err", err)
		return Host{}, fmt.Errorf("hoststats: read disk usage: %w", err)
	}

	hostInfo, err := host.InfoWithContext(ctx)
	if err != nil {
		slog.WarnContext(ctx, "hoststats: read host info failed", "err", err)
		return Host{}, fmt.Errorf("hoststats: read host info: %w", err)
	}

	return Host{
		UserPct:       userPct,
		SysPct:        sysPct,
		IdlePct:       idlePct,
		Load1:         loadStat.Load1,
		Load5:         loadStat.Load5,
		Load15:        loadStat.Load15,
		MemTotal:      virtualMem.Total,
		MemUsed:       virtualMem.Used,
		MemAvailable:  virtualMem.Available,
		MemUsedPct:    virtualMem.UsedPercent,
		SwapTotal:     swapMem.Total,
		SwapUsed:      swapMem.Used,
		SwapIn:        swapMem.Sin,
		SwapOut:       swapMem.Sout,
		DiskPath:      r.diskPath,
		DiskTotal:     diskUsage.Total,
		DiskUsed:      diskUsage.Used,
		DiskFree:      diskUsage.Free,
		DiskUsedPct:   diskUsage.UsedPercent,
		UptimeSeconds: hostInfo.Uptime,
		BootTime:      hostInfo.BootTime,
	}, nil
}

// readCPUPercents takes two cpu.TimesWithContext snapshots cpuSampleWindow
// apart and derives user/sys/idle percentages from the deltas over the total
// delta. gopsutil's cpu.PercentWithContext only reports total busy time, not
// the user/sys split, so this reader computes the split itself from
// TimesStat. A zero total delta (e.g. a context that is canceled between
// snapshots) reports zeros rather than dividing by zero.
func (r gopsutilReader) readCPUPercents(ctx context.Context) (userPct, sysPct, idlePct float64, err error) {
	before, err := cpu.TimesWithContext(ctx, false)
	if err != nil {
		slog.WarnContext(ctx, "hoststats: read cpu times failed", "err", err)
		return 0, 0, 0, fmt.Errorf("hoststats: read cpu times: %w", err)
	}
	if len(before) == 0 {
		slog.WarnContext(ctx, "hoststats: read cpu times failed", "err", "no cpu stats returned")
		return 0, 0, 0, fmt.Errorf("hoststats: read cpu times: no cpu stats returned")
	}

	timer := time.NewTimer(cpuSampleWindow)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		slog.WarnContext(ctx, "hoststats: read cpu times failed", "err", ctx.Err())
		return 0, 0, 0, fmt.Errorf("hoststats: read cpu times: %w", ctx.Err())
	case <-timer.C:
	}

	after, err := cpu.TimesWithContext(ctx, false)
	if err != nil {
		slog.WarnContext(ctx, "hoststats: read cpu times failed", "err", err)
		return 0, 0, 0, fmt.Errorf("hoststats: read cpu times: %w", err)
	}
	if len(after) == 0 {
		slog.WarnContext(ctx, "hoststats: read cpu times failed", "err", "no cpu stats returned")
		return 0, 0, 0, fmt.Errorf("hoststats: read cpu times: no cpu stats returned")
	}

	userDelta := after[0].User - before[0].User
	sysDelta := after[0].System - before[0].System
	idleDelta := after[0].Idle - before[0].Idle
	totalDelta := totalCPUTime(after[0]) - totalCPUTime(before[0])
	if totalDelta <= 0 {
		return 0, 0, 0, nil
	}

	return (userDelta / totalDelta) * 100,
		(sysDelta / totalDelta) * 100,
		(idleDelta / totalDelta) * 100,
		nil
}

// totalCPUTime sums every field gopsutil reports for a cpu.TimesStat
// snapshot, so the percentage denominator matches whatever the host
// contributes beyond user/system/idle (nice, iowait, irq, and so on).
func totalCPUTime(t cpu.TimesStat) float64 {
	return t.User + t.System + t.Idle + t.Nice + t.Iowait + t.Irq +
		t.Softirq + t.Steal + t.Guest + t.GuestNice
}
