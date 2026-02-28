# WinFSP I/O Path Refactor Notes

This note captures constraints and risks for a deeper WinFSP read/write path refactor.
It is intentionally implementation-oriented so the next cycle can execute quickly.

## Current Hotspots

1. Per-request heap allocations in callbacks:
- `read`: allocates owned buffer, then copies back to WinFSP buffer.
- `write`: clones request buffer with `to_vec()` before passing to async VFS.

2. Sync wait bridge:
- `spawn_wait()` uses `sync_channel` and blocks callback thread until async work completes.
- This preserves correctness but can limit throughput under heavy small I/O.

3. Small I/O amplification:
- WinFSP often issues many small reads/writes for sequential workloads.
- Extra copies and callback-thread blocking become visible before S3 saturates.

## Invariants To Preserve

1. No Tokio reactor misuse from WinFSP threads.
- Keep async work on Tokio worker threads.
- Avoid `Runtime::block_on()` directly from callback threads.

2. Callback safety and bounded latency.
- No unbounded queue growth in bridge layer.
- No lock held across async boundaries.

3. Correct file semantics.
- Preserve `write_to_eof` and `constrained_io` behavior.
- Preserve flush behavior in `cleanup`/`flush` callbacks.

4. Backpressure behavior.
- Do not bypass VFS dirty-bytes flow control.
- Avoid accepting writes faster than VFS can safely stage.

## Refactor Direction

1. Read path:
- Investigate direct fill into WinFSP buffer without intermediate owned `Vec<u8>`.
- If direct fill is not possible, reuse per-handle buffers to reduce allocation churn.

2. Write path:
- Reduce or eliminate unconditional `to_vec()` copies.
- Prefer borrow-to-owned conversion only when lifetime requires it.

3. Bridge model:
- Evaluate a bounded request queue + worker task model vs current per-call spawn/wait.
- Ensure queueing policy is explicit (drop/timeout/backpressure) and measurable.

4. Observability:
- Add counters and latency histograms around callback entry/exit and bridge handoff:
  - callback wall time
  - queue wait time
  - async execution time
  - bytes per op

## Suggested Acceptance Criteria (for refactor cycle)

1. Functional parity with existing WinFSP tests.
2. No regression in shutdown/flush correctness.
3. Reduced CPU per MB transferred in sequential read/write.
4. Improved throughput for mixed concurrent read+write workloads.
