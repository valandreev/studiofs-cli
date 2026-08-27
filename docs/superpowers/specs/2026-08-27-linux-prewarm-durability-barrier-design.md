# Linux namespace prewarm durability barrier

## Purpose

The Linux FUSE mount must complete the first namespace-backed write in
`mounted-invalidation-dropout-fallback-convergence` within its normal timeout.
The existing Linux namespace prewarm correctly removes the first inode-batch
allocation from the FUSE callback, but it leaves that allocation in the
one-second relaxed Redb durability window. Under the load of the full E2E
sweep, the first background durability barrier then competes with the deferred
namespace metadata batch and makes its Redb `begin_write` wait for about 1.55
seconds. macOS and Windows must retain their current startup paths.

## Current evidence

The failed run records a successful parent-directory admission and then stalls
inside the first file write. The Linux async-write counters show one completed
write, no pending writes, and zero FUSE barrier waits. The same snapshot shows
one `metadata_engine.begin_write` sample taking 1,552 ms; mutation and commit
work are sub-millisecond. This identifies local Redb writer contention rather
than a FUSE writeback watermark or object-storage operation.

The regression was introduced by the Linux-only startup prewarm in
`crates/studiofs-vfs/src/vfs.rs`. It allocates a namespace inode batch through
the existing relaxed Redb transaction path and returns the first inode to the
reservation queue. The transaction is accepted immediately, but its local
durability is intentionally deferred. A later immediate barrier can therefore
occupy Redb while the first deferred namespace batch is trying to begin its
write transaction.

## Selected design

Keep the existing Linux-only prewarm and its position after the one-shot
startup-channel claim. After the reservation is returned to the shared queue,
call the existing `VfsMeta::sync_local_metadata()` operation once. This forces
the prewarm's relaxed transaction through the same immediate local metadata
barrier used by `fsync` and clean shutdown, clears the pending durability state,
and starts the next relaxed-durability interval after startup has settled.

The startup sequence remains:

1. claim startup channels;
2. Linux writable mounts reserve and return one namespace batch;
3. Linux writable mounts force that allocator transaction locally durable;
4. start the existing compaction and background workers.

The helper continues to reuse `reserve_namespace_ino()` and
`return_reserved_namespace_ino()`. It does not change the batch size, allocator
schema, inode uniqueness rules, FUSE callbacks, or platform bridge behavior.

## Error handling

If either the allocation or the immediate local barrier fails, the existing
startup policy remains best effort: `StudioVfs::start()` logs a warning and
continues so a readable mount is not made unavailable solely because an
optimization could not complete. The normal first namespace mutation remains
responsible for reporting allocator or metadata errors. No new retry loop or
timeout is added.

## Platform boundary

Both the helper and its startup call remain guarded by
`#[cfg(target_os = "linux")]`. On macOS and Windows the new durability call is
absent at compile time, and no FUSE bridge source is changed.

## Verification

The existing Linux startup-prewarm test will continue to prove that startup
fills the reservation queue exactly once and that the first explicit
reservation reuses it. Add a regression assertion at the metadata/VFS test
seam that proves the startup prewarm also executes the immediate local barrier;
the assertion must fail with the current implementation and pass only after
the new call is present. Run the focused test, the full `studiofs-vfs` library
tests, formatting and diff checks, the target Linux E2E scenario, and a full
Linux sweep when practical. Run host checks for the macOS and Windows bridge
crates and verify that no platform-specific source files changed.

## Alternatives rejected

Moving the prewarm after startup compaction would change ordering but would
still leave a pending relaxed transaction that can collide with the first
foreground operation. Disabling the relaxed durability worker or increasing
the E2E timeout would hide the contention and weaken the local-first contract.
Changing Redb writer scheduling globally would broaden the regression surface
for every platform. Forcing only the Linux startup prewarm durable is the
smallest change that addresses the observed contention at its source.
