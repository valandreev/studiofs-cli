# PostgreSQL Metadata Migration Notes

This document tracks the PostgreSQL metadata architecture now used in StudioFS.

## Current Status

Implemented:
- `PgMetaEngine` is the distributed metadata engine.
- Tonbo and NATS runtime paths are removed.
- Foreground metadata path is local `redb` only.
- Background worker performs batched flush/pull with PostgreSQL.
- Multi-client convergence is eventual consistency through PostgreSQL transaction ordering.
- CLI supports:
  - `init ... --postgres <dsn>`
  - `discover <name> --postgres <dsn>`
  - `mount <name> <mount-point>`

## Operational Model

1. Client mutates local metadata in `redb`.
2. Local event log accumulates pending metadata events.
3. Background flush merges pending events onto current PostgreSQL snapshot and commits.
4. Clients periodically pull newer PostgreSQL snapshots.

## PostgreSQL Objects Used

- `volumes`
- `counters`
- `event_log`
- `metadata_state`

## Rationale

PostgreSQL provides central ACID serialization for metadata writes and removes CAS retry storms typical for manifest-based multi-writer designs.

## Testing

Primary regression suite:
```bash
cargo test -p studiofs-meta pg_engine::tests::test_pg_ -- --ignored --nocapture --test-threads=1
```

Full infra suite:
```bash
cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```
