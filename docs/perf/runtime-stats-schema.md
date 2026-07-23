# StudioFS Runtime Stats Schema

StudioFS exposes one cross-platform runtime stats schema for live performance
readers and benchmark reports. Windows, macOS, and Linux producers should fill
the same `RuntimeStatsSnapshot` shape from `studiofs-vfs`; platform-specific
bridges may leave unavailable counters at zero, but must not rename counters by
platform.

The current schema version is `1`.

## Mounted Access

Mounted StudioFS volumes expose the live text representation through the
root-local hidden file `.studiofs-runtime-stats`. Windows WinFSP, macFUSE, and
Linux FUSE bridges all use this same file name and the same parser-compatible
text format. The file is synthetic: it is not persisted in volume metadata, and
readers should treat each open/read as a fresh point-in-time snapshot.

## Snapshot Shape

`RuntimeStatsSnapshot` contains these stable sections:

| Section | Purpose |
| --- | --- |
| `system` | process/runtime counters such as uptime, memory, worker state |
| `bridge` | FUSE, macFUSE, WinFSP, or future bridge callback counters |
| `vfs_read` | VFS read-path calls, bytes, errors, and foreground read work |
| `vfs_write` | VFS write-path calls, bytes, barriers, and backpressure |
| `cache` | local cache hit/miss, resident bytes, dirty bytes, staging work |
| `upload_queue` | pending/active/completed upload queue state |
| `object_io` | object-store GET/PUT/DELETE calls, bytes, errors, retries |
| `metadata` | metadata read/write/commit/listener calls and latency |

Each section contains:

| Field | Meaning |
| --- | --- |
| `counters` | monotonically increasing values used for rates and totals |
| `gauges` | point-in-time values such as queue depth or resident bytes |
| `latencies` | histograms with `count`, `sum`, unit, and bucket counts |

Latency histograms must expose `count` and `sum` so readers can compute interval
average latency by subtracting two snapshots. Bucket upper bounds are always
stored as inclusive nanoseconds in `le_ns`; the histogram `unit` identifies the
unit used by `sum`.

## Stable Units

Use these unit strings:

| Unit | Meaning |
| --- | --- |
| `count` | operation count, object count, or event count |
| `bytes` | byte count |
| `ns` | nanoseconds |
| `ms` | milliseconds |
| `percent` | integer percentage where needed |

Counters that represent bytes should use `bytes`, not MiB/GiB. Rate readers can
compute per-second values from two monotonic snapshots.

## Recommended Counter Names

Producers should prefer these names when the signal exists:

| Section | Counters |
| --- | --- |
| `bridge` | `read_callbacks`, `write_callbacks`, `flush_callbacks`, `cleanup_callbacks`, `close_callbacks`, `callback_errors` |
| `vfs_read` | `calls`, `requested_bytes`, `served_bytes`, `cache_hit_reads`, `cache_miss_reads`, `errors` |
| `vfs_write` | `calls`, `accepted_bytes`, `synced_bytes`, `flush_barrier_calls`, `backpressure_events`, `errors` |
| `cache` | `hits`, `misses`, `evictions`, `staged_payloads`, `staged_bytes`, `materialized_payloads`, `materialized_bytes` |
| `upload_queue` | `submitted`, `completed`, `completed_bytes`, `requeued`, `canceled`, `snapshot_pending_calls` |
| `object_io` | `get_calls`, `get_bytes`, `put_calls`, `put_bytes`, `delete_calls`, `errors`, `retries` |
| `metadata` | `read_calls`, `write_calls`, `commit_calls`, `listener_events`, `errors`, `overlay_enqueue`, `overlay_coalesced`, `overlay_batches`, `overlay_requested_operations`, `overlay_actual_operations`, `overlay_forced_barriers` |

Recommended gauges:

| Section | Gauges |
| --- | --- |
| `system` | `uptime_ms`, `rss_bytes`, `worker_threads` |
| `bridge` | `fuse_max_write_bytes`, `fuse_max_readahead_bytes`, `fuse_max_background`, `fuse_congestion_threshold`, `fuse_read_buffer_budget_bytes`, `fuse_write_buffer_budget_bytes`, `fuse_prefetch_concurrency`, `fuse_upload_concurrency`, `fuse_download_concurrency`, `fuse_kernel_writeback_cache` |
| `cache` | `resident_bytes`, `dirty_bytes`, `evictable_bytes`, `prefetch_queue_depth` |
| `upload_queue` | `pending_depth`, `pending_bytes`, `active_uploads`, `oldest_pending_age_ms` |
| `metadata` | `pending_ops`, `inflight_ops`, `overlay_queue_depth`, `overlay_queue_bytes`, `overlay_dirty_age_ms` |

Recommended latency names:

| Section | Latencies |
| --- | --- |
| `bridge` | `read_callback`, `write_callback`, `flush_callback` |
| `vfs_read` | `read`, `fetch`, `local_cache_io` |
| `vfs_write` | `write`, `sync_data`, `payload_persist` |
| `cache` | `materialize_batch`, `cache_get`, `cache_put` |
| `upload_queue` | `queue_wait`, `upload` |
| `object_io` | `get`, `put`, `delete` |
| `metadata` | `commit`, `query`, `listener_poll` |

## Text Representation

The local text representation is line-oriented and machine-readable:

```text
# studiofs_runtime_stats schema_version=1
gauge system.schema_version count 1
gauge system.generated_unix_ms ms 1720000000000
counter vfs_read.requested_bytes bytes 1048576
gauge upload_queue.pending_depth count 3
histogram_count object_io.get ns 12
histogram_sum object_io.get ns 36000000
histogram_bucket object_io.get 1000000 4
```

The supported line kinds are `counter`, `gauge`, `histogram_count`,
`histogram_sum`, and `histogram_bucket`. Readers must ignore comments and blank
lines. Schema readers should accept missing newer sections or fields and treat
them as defaults.
