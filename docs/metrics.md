# Host Metrics

The broker samples host system stats on an interval, logs each sample, and serves the latest one on `/status`. An operator gets a broker-native view of the machine the warm pool runs on, without opening an SSH session.

## What it samples

Each sample carries the host CPU split as user, system, and idle percentages, plus the 1, 5, and 15 minute load averages. It carries memory as total, used, available, and used percentage, and swap as total, used, and cumulative swap-in and swap-out. It carries disk usage for one path as total, used, free, and used percentage, and the host uptime and boot time. Each sample also carries the broker's own pool inventory of runner count, idle, busy, and queued, so host load and scheduling state read together.

## Where it surfaces

The broker logs one line per sample at info level. The line carries a snake_case summary of the load-bearing fields: idle CPU, the one-minute load average, memory and disk used percentage, swap-out, uptime, and the pool's running, busy, and queued counts. `GET /status` returns the full latest sample as a `host_stats` object next to the pool snapshot, behind the same bearer token as the rest of `/status`.

## Configuration

The `[metrics]` block controls the sampler. `enabled` turns sampling on or off and defaults to on. `interval` sets the sampling period and defaults to 60 seconds. `disk_path` chooses the volume to measure and defaults to `/`. The `enabled` flag and the `interval` reload live through the config watcher, so a change to either applies without a restart. `disk_path` is read once at startup.
