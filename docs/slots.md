# Runner Slots

Each virtual machine runs `jobs_per_vm` runner slots. Each slot is a persistent GitHub self-hosted runner with a fixed `HOME` at `slot-home-<n>`. The broker provisions each slot on first use and reuses it across jobs. This follows GitHub's [persistent self-hosted runner model](https://docs.github.com/en/actions/reference/runners/self-hosted-runners) with multiple runners on one host.

Each slot uses distinct, fixed `HOME` and `TMPDIR` paths that are set on first use and never shared with another slot. The fixed paths isolate concurrent jobs while preserving each slot's state across jobs.

The whole virtual machine is the reset boundary. Recycling after `MaxBind`, `MaxAge`, or a `jobs_per_vm` change re-clones the virtual machine from the golden image and wipes all slot state. The broker does not reset slots between jobs.

Jobs in the same slot share accumulated caches and leftover state, as they do on a persistent self-hosted runner. This is acceptable because the runners serve private, trusted repositories. A job that needs a clean work tree relies on `actions/checkout` rather than a broker-level scrub.

The broker does not scrub a slot's `HOME` between jobs because the scrub failed on read-only Go module caches and does not match GitHub's self-hosted runner model.
