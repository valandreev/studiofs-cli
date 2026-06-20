# `studiofs perf`

`studiofs perf <mountpoint>` watches the live `.studiofs-runtime-stats` surface
exposed by a mounted StudioFS volume and prints interval deltas. It is an
observer only: it does not run benchmark workloads or mutate the mounted
volume.

Examples:

```powershell
studiofs perf Z:
studiofs perf Z: --interval 500 --count 10
studiofs perf Z: --section bridge --section vfs-read
studiofs perf Z: --json --count 1
studiofs perf Z: --json-lines --count 5
studiofs perf Z: --schema
```

On Linux or macOS, pass the mount directory:

```sh
studiofs perf /mnt/studio --interval 1000 --count 30
```

## Output

The human table shows one row per counter, gauge, or verbose latency metric:

```text
time_ms       section       metric                               rate/s          value unit
1720000005000 vfs_read      requested_bytes                     1048576.00      1048576 bytes
1720000005000 upload_queue  pending_depth                                -            3 count
```

Counters are reported as totals plus per-second rates computed from two
runtime stats snapshots. Gauges are point-in-time values. Verbose mode includes
latency averages computed from histogram `count` and `sum` deltas.

`--json` prints each sample as pretty JSON. `--json-lines` prints compact
newline-delimited JSON, one sample per line.

## Sections

The stable sections are inherited from the runtime stats schema:

- `system`
- `bridge`
- `vfs_read`
- `vfs_write`
- `cache`
- `upload_queue`
- `object_io`
- `metadata`

Section names accept `-` aliases on the CLI, for example `--section vfs-read`
maps to `vfs_read`.

If a selected section is unavailable or empty, `studiofs perf` emits a warning
instead of silently presenting missing stats as zero.

See [runtime-stats-schema.md](./runtime-stats-schema.md) for the producer
schema and text format.
