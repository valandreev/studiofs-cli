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

The VFS write-path observability JSON also carries additive Linux bridge
objects. They are optional/defaultable so older snapshots and readers remain
valid:

| Object | Required diagnostics |
| --- | --- |
| `linux_fuse_open_cache` | selected policy; direct/kernel opens; keep/drop and generation replace/reuse counts; memory bypass; per-`OsCacheReason` counts |
| `fuse_async_write` | pending requests/bytes/handles/inodes; peak and per-inode depth; admission and scoped barrier waits; failures, drops, cancellations, and shutdown timeouts |
| `fuse_invalidation` | current/peak queue depth; coalescing; unobserved skips; notification failures and latency; shutdown timeout |
| `fuse_connection` | requested and negotiated `max_read`, `max_readahead`, `max_write`, background/congestion limits, and enabled capability flags |

`macfuse_open_cache` keeps its existing keys for compatibility. It adds the
defaultable counters `read_only_memory_unavailable_direct_io` and
`read_only_exceeds_memory_direct_io`. They refine read-only opens that used
`direct_io` because of host-memory telemetry. They are not part of `total`.
A steadily rising unavailable count means the memory sampler is failing.
New producers must add fields rather than rename or repurpose existing macOS
or Linux keys.

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
| `bridge` | `read_callbacks`, `write_callbacks`, `flush_callbacks`, `cleanup_callbacks`, `close_callbacks`, `callback_errors`, `fuse_read_buffer_pool_hits`, `fuse_read_buffer_pool_misses` |
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
| `bridge` | `fuse_max_read_bytes`, `fuse_max_write_bytes`, `fuse_max_readahead_bytes`, `fuse_max_background`, `fuse_congestion_threshold`, `fuse_read_buffer_budget_bytes`, `fuse_read_buffer_pool_retained_bytes`, `fuse_read_buffer_pool_budget_bytes`, `fuse_write_buffer_budget_bytes`, `fuse_prefetch_concurrency`, `fuse_upload_concurrency`, `fuse_download_concurrency`, `fuse_kernel_writeback_cache`, `linux_fuse_cache_policy` |
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

## Local Metadata redb

The `metadata` section exposes local metadata database measurements with the
`metadata_redb_` prefix. The prefix and section identify the serving metadata
database; these metrics do not describe `cache.redb`.

| Metric | Kind / unit | Meaning |
| --- | --- | --- |
| `metadata_redb_physical_file_bytes` | gauge / bytes | Logical file length from `std::fs::metadata().len()`; not filesystem-allocated disk blocks. |
| `metadata_redb_allocated_pages` | gauge / count | redb allocated page count. |
| `metadata_redb_leaf_pages` | gauge / count | redb user-data leaf page count. |
| `metadata_redb_branch_pages` | gauge / count | redb user-data branch page count. |
| `metadata_redb_btree_tree_height` | gauge / count | Maximum B-tree traversal distance reported by redb across tables. |
| `metadata_redb_stored_bytes` | gauge / bytes | redb key and value bytes stored in leaf pages, excluding index overhead. |
| `metadata_redb_metadata_bytes` | gauge / bytes | redb index and metadata bytes. |
| `metadata_redb_fragmented_bytes` | gauge / bytes | redb fragmented data/metadata and free-page bytes. |
| `metadata_redb_page_size_bytes` | gauge / bytes | redb page size. |
| `metadata_redb_first_local_event_seq` | gauge / count | Lowest sequence still retained in the local metadata event log. Omitted when the event log is empty or unavailable. |
| `metadata_redb_next_local_event_seq` | gauge / count | Reported exclusive next sequence; if the counter is missing or below the retained tail, diagnostics use one past the highest retained event. |
| `metadata_redb_retained_local_event_span` | gauge / count | `next_local_event_seq - first_local_event_seq`, or zero when the event log is empty. This is a sequence span, not a row count. |
| `metadata_redb_local_dentry_mutation_baseline_rows` | gauge / count | Number of local dentry authority summary rows. Omitted when the table cannot be read. It can grow with distinct local authority changes; no safe incremental reclamation cutoff is currently established. |
| `metadata_redb_local_event_log_pruned_through_seq` | gauge / count | Inclusive acknowledged/pruned-through frontier persisted in local redb. No event-log row at or below it remains; the range can include sequence gaps that had no row. Reported only when the counter can be read and is greater than zero; otherwise omitted. |
| `metadata_redb_local_event_log_pruned_rows_total` | counter / count | Local event-log rows removed since this metadata engine opened. |
| `metadata_redb_local_event_log_pruned_payload_bytes_total` | counter / bytes | Encoded event payload bytes removed since this metadata engine opened. |
| `metadata_redb_local_event_log_prune_skipped_busy_total` | counter / count | Background prune attempts skipped because a redb writer was busy since this metadata engine opened. |
| `metadata_redb_local_event_log_prune_last_rows` | gauge / count | Rows removed by the latest completed prune attempt. Omitted before an attempt, after an error, or after a busy-writer skip; a successful no-op reports zero. |
| `metadata_redb_local_event_log_prune_last_first_seq` | gauge / count | First sequence removed by the latest non-empty successful prune batch. |
| `metadata_redb_local_event_log_prune_last_last_seq` | gauge / count | Last sequence removed by the latest non-empty successful prune batch. |
| `metadata_redb_local_event_log_prune_last_payload_bytes` | gauge / bytes | Encoded payload bytes removed by the latest completed prune attempt. Omitted before an attempt, after an error, or after a busy-writer skip; a successful no-op reports zero. |
| `metadata_redb_local_event_log_prune_last_duration_ns` | gauge / ns | Elapsed time for the latest prune attempt, including attempts that error. |
| `metadata_redb_compaction_attempts_total` | counter / count | Compaction calls since this metadata engine opened. |
| `metadata_redb_compaction_compacted_total` | counter / count | Calls where redb compaction made progress. |
| `metadata_redb_compaction_no_work_total` | counter / count | Calls where redb reported no work. |
| `metadata_redb_compaction_skipped_busy_total` | counter / count | Calls skipped because a database lock or redb transaction was active. |
| `metadata_redb_compaction_errors_total` | counter / count | Calls that returned a storage error. |
| `metadata_redb_compaction_last_duration_ns` | gauge / ns | Elapsed time for the latest compaction attempt. |
| `metadata_redb_compaction_last_success_bytes_before` | gauge / bytes | Physical file bytes before the latest successful compaction. |
| `metadata_redb_compaction_last_success_bytes_after` | gauge / bytes | Physical file bytes after the latest successful compaction. |

The boolean result gauges are `metadata_redb_compaction_last_result_compacted`,
`metadata_redb_compaction_last_result_no_work`,
`metadata_redb_compaction_last_result_skipped_busy`, and
`metadata_redb_compaction_last_result_error`. Each is `0` or `1`, and exactly
one is `1` after an attempt. A busy attempt sets either
`metadata_redb_compaction_last_skip_reason_database_lock_contended` or
`metadata_redb_compaction_last_skip_reason_active_transaction` to `1`. These
Compaction counters, local prune row/byte totals, prune busy-skip counts, and
last-attempt values reset when the local metadata engine opens. The
acknowledged/pruned-through event sequence frontier is persisted in the local
redb counter table.

Physical file bytes and redb page statistics describe different things: the
file may contain free or reusable pages, and redb stored bytes are not a count
of logical StudioFS rows. The redb page gauges are best-effort and cached; a
successful sample is reused for five seconds. redb (through 4.4) exposes them
through `WriteTransaction::stats()`, so StudioFS refreshes them only when the shared
metadata writer mutex and nonblocking database read lock are available. After
the sample expires, diagnostics reads attempt a refresh; if either gate or the
refresh lock is busy, the previous sample is returned, even if stale. Page
gauges are omitted until the first successful sample. Concurrent diagnostics
reads use the previous sample while a refresh is in progress. Read-only snapshot
mounts report file-size and event-log metrics but omit page gauges because redb
4.3 exposes these statistics only through a write transaction. The stats-only
transaction is aborted without changing metadata rows. A refresh can serialize
metadata writes and durability barriers for the duration of the page/tree scan;
the cache limits repeat scans, but a single refresh can still delay writers. An
active stats reader does not take the exclusive database lock used by
compaction.
Event-sequence and physical-size metrics use separate best-effort reads.

## Foreground Read Protection

Upload workers back off while foreground reads are slow. The upload worker loop
compares the p90 latency of foreground VFS reads, over the latency window, with
`upload_queue.fg_read_latency_target_ns`; above the target it raises the
upload admission penalty and the number of upload cache-I/O slots one upload
reread takes. These metrics show whether that throttle engaged.

| Metric | Kind / unit | Meaning |
| --- | --- | --- |
| `vfs_read.foreground_p90_latency_ns` | gauge / ns | p90 latency of foreground VFS reads in the latency window (`STUDIOFS_UPLOAD_FG_READ_LATENCY_WINDOW_MS`). Omitted when the window has no samples and no recent sticky value. |
| `vfs_read.foreground_window_samples` | gauge / count | Foreground read samples in the window. |
| `vfs_read.foreground_window_bytes` | gauge / bytes | Bytes those samples returned. |
| `vfs_read.foreground_window_coverage_ms` | gauge / ms | Time between the oldest and newest sample in the window. |
| `vfs_read.active_sequential_media_reads`, `vfs_read.active_random_interactive_reads` | gauge / count | Open read streams by access class. |
| `vfs_read.foreground_local_payload_reads`, `vfs_read.foreground_local_payload_read_bytes` | counter / count, bytes | Foreground reads served by the local payload store. |
| `vfs_read.foreground_local_payload_read_io_ns` | counter / ns | Backend I/O time of those reads. |
| `vfs_read.foreground_local_payload_read_io_max_ns` | gauge / ns | Longest such backend I/O since mount. |
| `vfs_write.active_sequential_writers`, `vfs_write.active_random_writers` | gauge / count | Open write streams by access class. |
| `upload_queue.fg_read_latency_target_ns` | gauge / ns | Configured p90 target (`STUDIOFS_UPLOAD_FG_READ_LATENCY_TARGET_MS`). |
| `upload_queue.scheduler_penalty_ms` | gauge / ms | Current upload admission penalty; zero means the throttle is not engaged. |
| `upload_queue.cache_io_foreground_cost` | gauge / count | Upload cache-I/O slots one reread takes while foreground I/O is active. |
| `upload_queue.cache_io_slots_available`, `upload_queue.cache_io_slots_max` | gauge / count | Free and total upload cache-I/O slots. |
| `upload_queue.dominant_upload_pressure_boost` | gauge / count | Penalty multiplier of the dominant foreground access class. |
| `upload_queue.local_payload_reads`, `upload_queue.local_payload_read_bytes` | counter / count, bytes | Upload payload reads, including hot-buffer and staged reads. |
| `upload_queue.hot_payload_reads`, `upload_queue.hot_payload_read_bytes` | counter / count, bytes | Upload reads served from the in-memory hot payload buffer. |
| `upload_queue.staged_payload_reads`, `upload_queue.staged_payload_read_bytes` | counter / count, bytes | Upload reads served from staged payload links. |
| `upload_queue.fallback_rereads`, `upload_queue.fallback_reread_bytes`, `upload_queue.fallback_reread_cache_misses` | counter / count, bytes | Upload rereads from the local payload store. |
| `upload_queue.fallback_wait_ns`, `upload_queue.fallback_read_ns` | counter / ns | Time those rereads waited for upload cache-I/O slots, and spent reading. The foreground-idle yield and pacing delay before the slot wait are not counted. |
| `upload_queue.fallback_wait_max_ns`, `upload_queue.fallback_read_max_ns` | gauge / ns | Longest such wait and read since mount. |

## Sequential RAM Read-Ahead

Handles that a bridge opted into sequential RAM read-ahead (macFUSE read-only
`direct_io` handles) load 4 MiB chunks ahead of a sequential stream. The
`vfs_read` section counts that work.

| Metric | Kind / unit | Meaning |
| --- | --- | --- |
| `vfs_read.ram_readahead_scheduled_chunks` | counter / count | Chunk loads started. |
| `vfs_read.ram_readahead_loaded_bytes` | counter / bytes | Bytes of chunks that were loaded and installed. |
| `vfs_read.ram_readahead_hit_reads`, `vfs_read.ram_readahead_hit_bytes` | counter / count, bytes | Read callbacks served entirely from chunks, and the bytes they returned. |
| `vfs_read.ram_readahead_wait_reads` | counter / count | Hits that waited for an in-flight chunk. |
| `vfs_read.ram_readahead_discarded_chunks` | counter / count | Chunk load tasks that finished without installing their result, for example because the inode changed or the read failed. Evictions of installed chunks and cancellations that never reach load completion are excluded. |
| `vfs_read.ram_readahead_budget_skips` | counter / count | Scheduling passes that stopped because the next chunk could not acquire a slot from the 256 MiB mount-wide budget; at most one skip is counted per pass. |

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
