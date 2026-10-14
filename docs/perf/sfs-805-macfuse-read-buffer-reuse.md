# SFS-805 macFUSE Read-Buffer Reuse Evidence

Queued macFUSE reads lend storage from `ReadBufferPool`
(`crates/studiofs-macfuse/src/adapter/read_buffer.rs`). Before SFS-805, every
acquire truncated the recycled `Vec` and called `resize(len, 0)`, so each read
zero-filled the whole request before `StudioVfs::read_into_fh` overwrote it.

The pool now keeps storage initialized across replies:

- A hit picks the newest pooled buffer whose initialized length covers the
  request. It does no fill.
- A miss evicts one undersized buffer and allocates `vec![0; len]`
  (`alloc_zeroed`). It does not grow stale storage with a realloc copy plus a
  memset.
- Only the prefix the VFS reports as written is reply-visible.
- Debug and test builds poison lent storage (`0xdb`). A VFS path that
  under-fills its reported prefix then fails tests instead of leaking stale
  bytes.

## Microbenchmark

This is an isolated `rustc -O` program with the system allocator on an Apple
M5. It is not the product build: `studiofs` uses `TrackingMiMalloc`. The values
are the mean per operation:

| Request | `main`: truncate + `resize(len, 0)` | SFS-805 hit | Miss: `vec![0; len]` | Miss: `reserve_exact` + `resize` |
|---:|---:|---:|---:|---:|
| 128 KiB | 2.0 µs | ~0 | 0.6 µs | 7.7 µs |
| 1 MiB | 15.8 µs | ~0 | 0.7 µs | 28.1 µs |
| 8 MiB | 81.4 µs | ~0 | 91.9 µs | 396.0 µs |

How to read the table:

- **Steady-state hits** remove the full per-read memset.
- **Miss columns** include allocation and free. The `vec![0; len]` column
  defers page faults to the first VFS write, so compare it only with the
  `reserve_exact` + `resize` column.

The pool unit tests and the adapter tests that check for no new zeroed bytes
(`freshly_zeroed_read_buffer_bytes_for_test`) are the functional proof.
