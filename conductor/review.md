# Code Review: SFS-171 - Remove Adaptive Memory Envelope & Clean up Observability

## Summary of Intent
The goal of these changes is to finalize the transition to the Cache v2 data path by removing the `MemoryEnvelope` (SFS-130), which adaptively managed memory budgets for L1, transit, and dirty buffers. This includes deleting the adaptive controller, narrowing the `BlockCache` shim's responsibilities to primarily focus on the upload queue, and simplifying VFS observability.

---

## High-Level Observations
1. **Architectural Cleanup**: The removal of `MemoryEnvelope` is comprehensive across the VFS and CLI. Deleting `memory_envelope.rs` and stripping its integration from `StudioVfs` correctly reflects the new data plane's design where memory management is more deterministic.
2. **Narrowed BlockCache**: While the PR successfully removes `L1` staging from the production write path (`write_buffer.rs`) and read path (`vfs.rs`), the `BlockCache` struct still retains significant "dead" logic and fields related to `L1` management (eviction, hit counting, resizing).
3. **Documentation Lag**: Several key documentation files (`docs/vfs.md`, `docs/configuration.md`) still reference the `MemoryEnvelope` and its adaptive parameters, which will confuse users as those parameters are no longer valid.

---

## Detailed Review Comments

### crates/studiofs-cache/src/block_cache.rs

| Severity | Issue | Recommendation |
| :--- | :--- | :--- |
| **MEDIUM** | **Dead Code**: `set_l1_max_bytes` is no longer called. | Since `MemoryEnvelope` (the only caller) is removed, this method is dead code. While `test-cache` uses the initial `l1_size`, it never resizes it. This method should be removed or marked as for benchmark-only. |
| **MEDIUM** | **Dead Code**: `resize_l2` is a no-op that is never called. | Remove this method as it no longer serves any purpose in the final architecture. |
| **LOW** | **Unused Metrics**: `l1_hit_count` and `l1_eviction_count`. | These counters are incremented but no longer read by `StudioVfs` or the CLI. To fulfill the "remove transition-era observability" goal, these should be removed. |
| **LOW** | **Redundant Metrics**: `shim_read` and `shim_write`. | In `get_with_trace_and_priority`, these now only measure `l1_latency`. Since `StudioVfs` no longer uses these for production reads/writes, consider isolating them to benchmark-only paths. |

### crates/studiofs-vfs/src/vfs.rs

| Severity | Issue | Recommendation |
| :--- | :--- | :--- |
| **LOW** | **Zombie Parameter**: `memory_limit` initialization. | `StudioVfs` still calculates `max_dirty_bytes` and passes a `memory_limit` to `BlockCache`, but then never uses the `L1` features of `BlockCache`. While harmless, it perpetuates the idea that `BlockCache` is still an active staging layer. |

### Documentation (Multiple Files)

| Severity | Issue | Recommendation |
| :--- | :--- | :--- |
| **MEDIUM** | **Outdated Docs**: `docs/vfs.md` & `docs/configuration.md`. | These files still describe `memory_adaptive`, `total_ram`, and the `MemoryEnvelope` logic. They must be updated to reflect that memory management is now static or managed differently. |

---

## Logic & Correctness Check
- **Write Path**: Removing `stage_l1` from `write_buffer.rs` is correct; the `BlockStore` (v2) is now the source of truth for local blocks.
- **Read Path**: Removing the fallback to `block_cache.get_sync` in `vfs.rs` correctly ensures that the system doesn't rely on the old staging layer for data consistency.
- **Tests**: Updating `vfs_test.rs` to remove `memory_adaptive: Some(false)` is correct as the flag no longer exists.
