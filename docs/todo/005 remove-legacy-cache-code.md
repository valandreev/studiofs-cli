# Task: Remove Legacy Cache Code

The legacy cache modules in `studiofs-cache` (`disk.rs`, `memory.rs`, `slab.rs`, `writeback.rs`) are no longer used by the v3 architecture and should be permanently removed to reduce technical debt and compilation time.

## Steps

1. Delete `crates/studiofs-cache/src/disk.rs`.
2. Delete `crates/studiofs-cache/src/memory.rs`.
3. Delete `crates/studiofs-cache/src/slab.rs`.
4. Delete `crates/studiofs-cache/src/writeback.rs`.
5. Remove their module declarations (`pub mod disk;` etc.) from `crates/studiofs-cache/src/lib.rs`.
6. Ensure no lingering imports remain in `studiofs-vfs` or related crates that might fail compilation.
