# PostgreSQL Metadata Migration Notes

This document tracks the PostgreSQL metadata architecture currently used in StudioFS.

## Current Status

Implemented:
- PostgreSQL is the shared metadata replication layer.
- foreground metadata reads/writes stay local in `redb`.
- runtime cross-client sync uses delta batches in `event_log`.
- `metadata_state` stores bootstrap/checkpoint snapshots, not the primary runtime stream.
- remote-visible regular-file size is driven by `committed_size`.
- stale upload completions are filtered by `content_epoch`.

StudioFS always uses database `studiofs`. `init` creates that database when the
server allows it.

## Operational Model

1. Client mutates local metadata in `redb`.
2. Local event log records pending metadata events.
3. Background flush projects only remote-visible events into PostgreSQL `event_log`.
4. Upload completion promotes blocks into `committed_file_blocks` and advances `committed_size`.
5. Clients periodically pull only newer foreign batches.
6. `metadata_state` is refreshed as a checkpoint for bootstrap and `discover`.

## PostgreSQL Objects Used

- `volumes`
- `event_log`
- `metadata_state`

## Key Invariants

- `event_log` is append-only.
- batch ordering uses `batch_version`.
- clients do not replay their own PG events back into local state.
- staged file blocks are never published as remote-visible deltas.
- checkpoints must not leak local-only staged blocks.

## Rationale

PostgreSQL gives ordered multi-client metadata convergence without putting PG on
the hot foreground read/write path.

## Testing

Primary PG regression suite:
```bash
cargo test -p studiofs-meta pg_engine::tests::test_pg_ -- --ignored --nocapture --test-threads=1
```

Useful focused checks:
```bash
cargo test -p studiofs-meta --lib -- --nocapture
cargo test -p studiofs-vfs --test vfs_test test_truncate_shrink_then_extend_zero_fills_old_tail -- --ignored --nocapture
```
