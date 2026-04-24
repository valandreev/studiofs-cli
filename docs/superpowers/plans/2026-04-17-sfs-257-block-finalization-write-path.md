# SFS-257 Block Finalization Write Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move normal write-path behavior to block-finalization semantics where completed full blocks are locally accepted eagerly, while partial tail state remains buffered until an explicit durability barrier.

**Architecture:** Keep the new `SFS-256` local payload store and cache index as the acceptance boundary. Normal writes should use opportunistic local sync by default, eagerly finalizing completed full blocks into the local payload store + overlay metadata + pending-upload queue, while partial current-payload-object state stays in memory until `flush`, `fsync`, size-changing operations, or last-handle release force the strict barrier path.

**Tech Stack:** Rust 2024, `studiofs-vfs`, `studiofs-cache`, redb metadata, local payload backend, focused `cargo test` verification.

---

### Task 1: Lock the New Contract in Focused Tests

**Files:**
- Modify: `crates/studiofs-vfs/tests/vfs_test.rs`
- Modify: `crates/studiofs-vfs/src/vfs/tests.rs`

- [ ] Rewrite the partial-write expectations so ordinary writes no longer require the partial tail block to be durably committed before `write()` returns.
- [ ] Keep the eager full-block expectations intact: a completed full block should still become locally accepted and queued for upload without waiting for `flush`.
- [ ] Add or update a focused regression around the mixed case: one full block plus a partial tail should leave only the full block materialized before `flush`, then commit the tail after the barrier.
- [ ] Run the targeted Rust tests to verify the new assertions fail against the old behavior.

### Task 2: Split Ordinary Inode Writes into Eager Full-Block Acceptance vs Buffered Tail State

**Files:**
- Modify: `crates/studiofs-vfs/src/local_dirty.rs`
- Modify: `crates/studiofs-vfs/src/vfs/io_ops.rs`

- [ ] Factor the direct inode-write helper so it can persist prebuilt full-block snapshots without forcing the partial tail down the same path.
- [ ] For opportunistic inode writes, persist only completed full blocks into the local payload store + overlay metadata + pending-upload queue.
- [ ] Stage partial/current-block snapshots in local in-memory dirty state so read-your-writes stays intact before `flush`.
- [ ] Leave strict barriers (`flush`, `fsync`, size-changing operations) responsible for draining the remaining staged tail bytes to locally durable state.

### Task 3: Align Mounted Normal-Write Defaults with the New Barrier Model

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/sync_ops.rs`
- Modify: `crates/studiofs-fuse/src/fs.rs` (behavior verified through `write_fh` default path)
- Modify: `crates/studiofs-winfsp/src/context.rs` (behavior checks only if needed)

- [ ] Change the default mounted buffered-handle write path to use opportunistic local sync instead of strict-per-write persistence.
- [ ] Preserve the existing strict behavior for explicit write-through / unbuffered WinFSP handles and for explicit durability barriers.
- [ ] Make sure last-handle release and explicit flush still own the strict drain path once dirty state remains.

### Task 4: Focused Verification

**Files:**
- Modify: none

- [ ] Run the updated focused regression tests for partial tail buffering, explicit flush, and eager full-block acceptance.
- [ ] Run a focused `cargo test` slice for `studiofs-vfs` and any touched WinFSP/FUSE tests only.
- [ ] Run `cargo fmt` on touched files.
- [ ] Summarize the exact commands used and leave the full-suite command for the user to run manually.
