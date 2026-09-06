# `studiofs-winfsp` agent contract

This crate is the Windows WinFSP adapter. Read the root `AGENTS.md`,
`docs/platform-bridges.md`, `docs/metadata-cache-coherence.md`, and
`docs/contributing.md` before changing adapter behavior.

- Keep semantic invalidation platform-neutral until the adapter boundary. Preserve
  inode/name identity, old rename paths, generation ordering, queue acceptance,
  and explicit unsupported/failure states.
- Write-capable handles must preserve the current cache-bypass and cleanup
  contract. Read-only handles may use the normal WinFSP cache-manager path. Do
  not disable caching globally to hide a writeback or coherence regression.
- Native invalidation is asynchronous and stronger than notification names imply;
  protect dirty writers at admission, preparation, and submission. Queueing work
  is not proof that the kernel consumed it.
- Unit tests and Windows compilation do not prove a live mount. Native smoke/E2E
  tests require WinFSP, an explicitly selected unused drive, bounded waits, and
  retained diagnostics. State that boundary in reports.
- Run focused WinFSP tests and Clippy first; run ignored mount tests only when the
  host prerequisites are present and the user task requires them.

Use `docs/tests.md` for the exact WinFSP commands and environment variables.
