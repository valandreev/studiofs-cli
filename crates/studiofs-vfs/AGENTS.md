# `studiofs-vfs` agent contract

This crate is the platform-neutral filesystem core. Read the root `AGENTS.md`,
`docs/juicefs-local-first-rust-contract.md`, `docs/architecture.md`, and
`docs/local-first-validation-matrix.md` before changing behavior.

- Foreground metadata reads and writes use local `redb`; PostgreSQL and object
  storage remain asynchronous dependencies for bootstrap, replication, and
  payload work.
- Preserve newer local writer state when foreign metadata is applied. Publish
  invalidation after the local commit, retain old names/aliases before destructive
  namespace changes, and compare observation tokens before admitting cache data.
- Keep `write`, `flush`, `fsync`, release, shutdown, and remote publication as
  separate boundaries. A callback must not wait for peer visibility or an
  unbounded remote probe.
- Follow the lock order in `src/lock_order.rs`; use bounded condition waits for
  concurrency tests. Scheduling sleeps and blind retries do not establish a race
  boundary.
- For lifecycle changes, inspect `Arc`/task ownership and ensure background work
  is joined or owned by shutdown before a redb database can be reopened.
- Validate with focused VFS tests and crate check/Clippy. Do not claim mounted
  acceptance from unit tests; native FUSE/WinFSP evidence is a separate gate.

Use `docs/agents-coding-guide.md` for detailed Rust style and local durability
rules, and `docs/tests.md` for the current focused command matrix.
