# `studiofs perf`

`studiofs perf <mountpoint>` watches the live `.studiofs-runtime-stats` surface
exposed by a mounted StudioFS volume and prints interval deltas. It is an
observer only: it does not run benchmark workloads or mutate the mounted
volume.

Examples:

```powershell
studiofs perf Z:
studiofs perf Z: --interval 500 --count 10
studiofs perf Z: --section bridge --section vfs
studiofs perf Z: --section runtime --section upload --section object
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
time_ms       section       kind      metric                               rate/s        current unit
1720000005000 VFS read      counter   requested bytes                  1048576.00        1048576 bytes
1720000005000 upload        gauge     pending depth                             -              3 count
```

Counters are reported as totals plus per-second rates computed from two
runtime stats snapshots. Gauges are point-in-time values. Verbose mode includes
latency averages computed from histogram `count` and `sum` deltas.

`--json --count N` prints one pretty JSON array containing `N` interval samples.
Use `--json-lines` for continuous streaming; it prints compact
newline-delimited JSON, one sample per line. Both machine-readable modes keep
the stable schema section keys such as `vfs_read` and `upload_queue`.

`--schema` prints the supported schema version, stats file name, section list,
line kinds, and units without reading the mounted stats file.

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

Operator aliases are also accepted:

| Alias | Expands to |
| --- | --- |
| `runtime` | `system` |
| `vfs` | `vfs_read`, `vfs_write` |
| `upload` | `upload_queue` |
| `object` | `object_io` |

If a selected section is unavailable or empty, `studiofs perf` emits a warning
instead of silently presenting missing stats as zero.

## Errors and non-goals

If `<mountpoint>/.studiofs-runtime-stats` cannot be read, `studiofs perf`
reports that the StudioFS runtime stats surface is unavailable and asks the
operator to verify that the path is a mounted StudioFS volume. If the file is
present but malformed, the command reports an incompatible or malformed stats
surface.

`studiofs perf` does not run benchmark workloads, create files, or require a
mount to be started with debug-only flags.

See [runtime-stats-schema.md](./runtime-stats-schema.md) for the producer
schema and text format.
