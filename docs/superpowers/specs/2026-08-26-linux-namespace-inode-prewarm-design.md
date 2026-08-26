# Linux namespace inode prewarm

## Context

End-to-end scenario 23, `mounted-symlink-attribute-convergence`, intermittently
times out on Linux while creating the parent directory for its first nested
file. The failed run records `before_create_parent` but not
`after_create_parent`, and the VFS observability snapshot contains no completed
`mkdir` sample. The same scenario passes when run alone and in repeated full
runs, so the failure is a startup race rather than a deterministic symlink
failure.

The first namespace mutation on a fresh writable VFS calls
`StudioVfs::reserve_namespace_ino()` in
`crates/studiofs-vfs/src/vfs/namespace_ops.rs`. An empty reservation queue
causes it to allocate a batch of 4096 inode IDs through the local Redb
metadata engine before the namespace mutation can return. The failed run had
six Redb write transactions begun and only five committed at the diagnostic
snapshot. This means the FUSE callback can spend its entire scenario timeout
waiting in the first allocator refill while startup/replay work is still
using the same local metadata store.

The user-visible goal is that a writable Linux mount can service the first
nested create without making that FUSE callback perform the initial allocator
refill. macOS and Windows must retain their current startup and namespace
paths.

## Chosen design

During the one successful transition from uninitialized to running in
`StudioVfs::start()`, and only when compiling for Linux and mounting writable,
reserve one namespace inode before any background startup worker is spawned.
Immediately return that inode to the front of the existing reservation queue.
The operation therefore warms the existing 4096-ID batch without consuming an
ID or introducing a second allocator implementation.

The prewarm belongs in the VFS startup sequence rather than in the Linux FUSE
callback. `start()` claims the one-shot startup channels before spawning
workers, so putting the prewarm immediately after that claim makes it run once,
before `spawn_startup_workers()` can create local Redb contention. The
`#[cfg(target_os = "linux")]` block leaves macOS and Windows source behavior
unchanged. Read-only mounts already return from `start()` before write workers
are started and will not prewarm.

Prewarm is an optimization of the first mutation, not a new availability
requirement. If the allocator cannot be warmed, startup logs a warning and
continues exactly as before; the normal namespace mutation path remains
responsible for reporting allocator errors. This preserves the local-first
contract that a mount may remain readable when mutation admission is degraded.

The internal helper in `namespace_ops.rs` will call the existing reservation
and return methods. No allocator lock, batch size, metadata schema, or platform
bridge callback will change.

## Alternatives considered

Restoring Linux `mkdir` to a synchronous bridge path would align one platform
table entry with its older implementation, but it would still leave the first
namespace callback responsible for the empty allocator refill. It does not
address the observed Redb contention and is not part of this fix.

Changing global Redb transaction ordering or replay locking could remove more
contention, but it would affect every platform and every metadata operation.
The observed failure is specifically the avoidable first allocator refill, so
that broader change has a larger regression surface than necessary.

Prewarming in the Linux bridge immediately before mounting would also work,
but would duplicate platform-specific startup knowledge there and would run
after VFS startup workers have already been started by the mount session. The
VFS start boundary is the smaller and earlier seam.

## Error handling and invariants

The reservation helper propagates `VfsError` to its caller. The Linux-only
startup call logs that error at warning level and does not consume or discard a
partial reservation. A successful prewarm leaves the reservation queue with
the same inode it contained before the call, plus the rest of the newly
allocated batch when the queue was initially empty.

The following invariants must remain true:

* a prewarm never allocates or publishes a namespace entry;
* a prewarm does not consume an inode ID;
* a second `StudioVfs::start()` call does not prewarm again because startup
  channel ownership is one-shot;
* read-only Linux mounts do not touch the allocator;
* macOS and Windows do not compile or execute this prewarm block;
* if prewarm fails, the existing first-mutation error behavior remains intact.

## Verification design

Add a Linux-gated VFS regression test beside the existing namespace allocator
tests in `crates/studiofs-vfs/src/vfs/tests.rs`. Construct an unstarted writable
test VFS, call `start()` under its Tokio runtime, assert that exactly one
allocator batch was recorded, reserve one inode, and assert that the allocator
count is unchanged. The test fails before the change because startup currently
performs zero namespace allocations and the first explicit reservation performs
the first allocation; it passes after the prewarm.

Keep the existing allocator uniqueness, restart-gap, and shared create/mkdir
tests unchanged. Run the focused VFS test, the full `studiofs-vfs` library test
target, and the Linux E2E scenario 23. A successful E2E run must report
`setup`, `run`, `validate`, and `cleanup` as passed for
`mounted-symlink-attribute-convergence`. Run `cargo check` for the macOS and
Windows bridge crates when their target toolchains are available; the source
change is gated in the platform-agnostic VFS crate and has no Mac/Windows
behavioral branch.
