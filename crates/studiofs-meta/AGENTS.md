# `studiofs-meta` agent contract

This crate owns the local metadata engine and the PostgreSQL bootstrap,
replication, snapshot, and lifecycle paths. Read the root `AGENTS.md`,
`docs/metadata.md`, `docs/architecture.md`, and the relevant ADRs first.

- Local `redb` is the foreground serving view. PostgreSQL supplies bootstrap,
  ordered event replication, checkpoints, and shared coordination; do not move
  PostgreSQL into the mounted foreground read path.
- Trace `PgMetaEngine` startup, worker registration, `prepare_for_close`, and
  `Drop` before changing lifecycle code. Join or cancel workers that retain redb
  handles before reopen; a `Database already open` error is evidence of ownership
  that still exists, not a reason to add sleeps or retries.
- Foreign apply must preserve newer local authority state. Keep event versions,
  allocator authority, invalidation boundaries, and snapshot reachability
  semantics explicit and typed.
- Bound every remote/object visibility probe by its operation deadline. Preserve
  typed error categories and redact credentials/full DSNs from diagnostics.
- Use focused `studiofs-meta` tests first. Ignored PostgreSQL tests require the
  configured service and `.env`; report skipped infrastructure separately from
  compile or unit evidence.
- Metadata lock order is `local_position_lock` → `pending_state` →
  `active_open_inodes` when nested. See [Local Replay Lock Ordering](../../docs/pg-sync-contract.md#local-replay-lock-ordering).

Use `docs/agents-coding-guide.md`, `docs/metadata.md`, and `docs/tests.md` for
the detailed contract and command matrix.
