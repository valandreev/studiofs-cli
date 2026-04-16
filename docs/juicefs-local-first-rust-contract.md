# JuiceFS-Inspired Local-First Rust Contract

## Status: Canonical rewrite contract
## Date: 2026-04-16
## Issues: SFS-242, SFS-243
## JuiceFS reference snapshot: `valandreev/juicefs` `d3bce0efdb25ca19ca43599450bd75d4fb46467e`

This document is the single canonical contract for the SFS-242 rewrite.
If any older StudioFS redesign note, ADR, or issue disagrees with this file,
this file wins for the rewrite target.

JuiceFS is the design reference for data-model and IO ideas. It is not a
literal semantic clone target. StudioFS deliberately preserves Local-First
visibility and eventual remote publication even where JuiceFS defaults differ.

## 1. What We Are Borrowing Vs Preserving

| Category | Contract |
|---|---|
| JuiceFS ideas we are borrowing | chunk/slice/block modeling, per-file writer state, slice-oriented flush behavior, `dataReader`-style read resolution, adaptive readahead, block prefetch, cached-store cache/object-store structure, and a coherent operator-facing cache/mount surface |
| StudioFS invariants we are preserving | Local-First read-your-writes, durable local unpublished state before remote publication, eventual consistency across clients, remote visibility only after upload plus publish, and file-version publication as an atomic control-plane event |
| Rust-native implementation rules | explicit ownership, enums and state transitions, domain-specific `Result` error types, bounded async concurrency, and module boundaries shaped by responsibilities instead of Go package mirroring |

## 2. Target Vocabulary And Invariants

| Term | Origin | Meaning in this contract |
|---|---|---|
| `chunk` | Borrowed from JuiceFS | Fixed logical file range used for lookup, split, overlap, and compaction boundaries. |
| `slice` | Borrowed from JuiceFS | Chunk-relative metadata record that maps a byte range to one immutable block or tombstone. |
| `block` | Borrowed from JuiceFS | Immutable payload object. Any content change produces a new block and new slice reference. |
| `writer state` | Borrowed shape, StudioFS-adapted | Per-handle/per-inode transient state that can satisfy same-mount reads before local publication work is complete. |
| `local unpublished state` | Retained StudioFS semantic | Locally durable slices/attrs that the current mount may read before remote publication. |
| `published state` | Retained StudioFS semantic | Shared remote-visible file state discoverable by other clients through metadata plus object storage. |
| `hole` | Shared filesystem concept | Uncovered byte range that reads as zeroes. |

Invariants:

- A slice never crosses a chunk boundary.
- Overlap resolution is per byte, not per slice.
- Resolution order is deterministic: writer state, then local unpublished state, then published state, then holes.
- Blocks are immutable and are never edited in place.
- Publication is file-version atomic; other clients do not observe partially published manifests.

## 3. Local-First Visibility Contract

Newly written bytes become visible in three stages:

1. `write` success makes bytes visible to the writing mount through active writer state.
2. `flush`, `fsync`, and the last-handle release boundary make those bytes durable in local unpublished state.
3. Background upload plus metadata publish makes those bytes visible to other mounts as published state.

Rules:

- Same-mount reads must observe accepted writes immediately through the merged local view.
- Remote mounts must never depend on another client's RAM-only writer state.
- Success on `flush`, `fsync`, or the fallback last-handle release barrier means local durability, not remote publication.
- Failed remote publication must not revoke already established local unpublished visibility; it is retried until resolved or surfaced as an explicit sticky error state.

## 4. Eventual Consistency And Remote Publication

StudioFS preserves eventual consistency, not close-to-open parity.

- Upload and publish are asynchronous background phases.
- Other clients observe new data only after both payload blocks and the corresponding published metadata state are available.
- Publication switches visibility at the file-version level, not block-by-block.
- Local clients may temporarily observe a newer state than remote clients.
- Convergence is achieved by background upload, metadata replication, and cache invalidation/expiration rather than synchronous cross-client coordination on every write.

## 5. Read Resolution Contract

Foreground reads resolve data in this order:

1. Active writer state for the current mount and handle.
2. Locally durable unpublished slices.
3. Published remote-visible slices.
4. Zero-fill for uncovered holes.

Implications:

- Sparse writes and overlapping writes are legal and resolved by per-byte precedence.
- A published slice never overrides newer local unpublished data on the writing mount.
- Remote readers use published state only; they do not consult another mount's local unpublished state.
- Readahead and prefetch operate on the effective read view after this merge rule is applied.

## 6. Borrowed JuiceFS Ideas By Subsystem

| StudioFS area | Current StudioFS paths | JuiceFS reference files | What is borrowed |
|---|---|---|---|
| Regular-file vocabulary and schema | `crates/studiofs-meta/src/redb_engine/{blocks.rs,chunk_state.rs,replication.rs}` | `docs/en/introduction/architecture.md`, `pkg/meta/slice.go`, `pkg/meta/*.go` | Chunk/slice/block language, immutable block references, slice overlap model |
| Writer path | `crates/studiofs-vfs/src/write_buffer.rs`, `crates/studiofs-vfs/src/vfs/upload_lifecycle.rs` | `pkg/vfs/writer.go` | Per-file writer ownership, chunk/slice decomposition, flush-driven slice materialization |
| Reader path, readahead, prefetch | `crates/studiofs-vfs/src/vfs/read_path.rs`, `crates/studiofs-vfs/src/readahead.rs`, `crates/studiofs-vfs/src/vfs/prefetch.rs` | `pkg/vfs/reader.go`, `pkg/chunk/prefetch.go`, `docs/en/guide/cache.md` | `dataReader`-style resolution, adaptive readahead, whole-block prefetch |
| Cache, staging, object-store handoff | `crates/studiofs-cache/src/upload_coordinator.rs`, `crates/studiofs-cache/src/v3/*.rs`, `crates/studiofs-data/src/store.rs` | `pkg/chunk/cached_store.go`, `pkg/chunk/cache_eviction.go`, `docs/en/guide/cache.md` | Cached-store lifecycle, cache/download/writeback structure, eviction and background upload concepts |
| Metadata-cache and operator surface | `crates/studiofs/src/cli/mount/*.rs`, `docs/configuration.md` | `cmd/flags.go`, `cmd/mount.go`, `cmd/mount_unix.go`, `cmd/mount_windows.go`, `docs/en/guide/cache.md` | Coherent mount/cache flags, metadata-cache knobs, operator-facing terminology |
| Bridge and mount behavior | `crates/studiofs-fuse/src/lib.rs`, `crates/studiofs-winfsp/src/*.rs` | `pkg/vfs/vfs.go`, `pkg/vfs/vfs_unix.go`, `pkg/vfs/vfs_windows.go`, `cmd/mount*.go` | Performance-sensitive bridge/mount patterns and callback expectations |

## 7. Current StudioFS Concepts: Delete, Retain, Rename, Simplify

| Current concept | Decision in this rewrite |
|---|---|
| `WriteBuffer` as the canonical architecture | Delete as the target design. Transitional code may remain only until slice-writer replacement lands. |
| `ReadPipeline` as the canonical foreground read engine | Delete as the target design. Replace with a direct merged reader over writer/local unpublished/published state. |
| `UploadCoordinator` as the canonical upload architecture | Simplify/delete as a top-level design concept. Replace with one cached-store-style staging/upload pipeline. |
| Cache 3.0 as product architecture vocabulary | Rename/simplify. It may remain as an implementation detail for the local backend, but not as the rewrite's conceptual model. |
| `overlay` / `base` wording | Retain the semantics, but prefer `local unpublished state` and `published state` in new architecture docs and issues. |
| File-version publication boundary (`incarnation`, manifest-like switch) | Retain the semantic requirement: remote visibility changes atomically at a file-version boundary. |
| `STUDIOFS_*` tuning sprawl | Simplify into a JuiceFS-inspired CLI/config surface with only transitional env vars where necessary. |

## 8. Rust-Native Implementation Rules

Rust-native implementation means:

- Do not perform a line-by-line Go port.
- Model lifecycle with owned types, enums, and explicit state transitions.
- Use domain-specific error enums and `Result<T, E>` in library layers; reserve panic paths for invariant violations and unrecoverable bootstrap failures.
- Use Tokio tasks, channels, and mutexes only where they express real ownership handoff, bounded backpressure, or async waiting requirements.
- Prefer localized state owners such as per-file writers, per-reader sessions, and clearly bounded background workers over one large mutable runtime mesh.
- Keep blocking IO and async orchestration separated explicitly.
- Prefer module boundaries based on responsibilities such as metadata schema, writer state, reader resolution, cached store, and bridge adapters instead of transliterating Go package layout.

Review guardrails:

- Reject code that copies goroutine/channel topology when a direct Rust state machine or owned task boundary is clearer.
- Reject APIs that leak transitional names as permanent architecture.
- Reject generic `anyhow`-style library errors where callers need domain decisions.
- Reject designs that require remote readers to reason about writer-local unpublished RAM state.

## 9. Historical Docs And Issues That Are No Longer Authoritative

| Source | Status after this contract |
|---|---|
| `docs/chunk-slice-block-vocabulary.md` | Removed from active docs. Its vocabulary freeze is now absorbed by this contract. |
| `docs/data-plane-redesign-guardrails.md` | Removed from active docs. Its sequencing and deletion guidance is now absorbed by this contract. |
| `SFS-154` | Historical vocabulary freeze that is superseded by this contract. |
| `SFS-155` | Historical guardrail issue that is superseded by this contract. |
| `SFS-148` | Historical redesign umbrella referenced by older guardrails; SFS-242 is the active umbrella. |

Supporting implementation snapshots such as `docs/architecture.md`, `docs/vfs.md`,
and `docs/configuration.md` still describe in-flight code, but they are not
authoritative for the rewrite target when they conflict with this contract.

## 10. Glossary For Downstream Issues

| Term to use in new issues | Type | Meaning |
|---|---|---|
| `chunk`, `slice`, `block` | Borrowed JuiceFS concept | The rewrite's core data vocabulary. |
| `writer state` | Borrowed-and-adapted concept | Local transient state that serves same-mount reads before durable local publication work finishes. |
| `local unpublished state` | Retained StudioFS semantic | Locally durable, newer-than-published data. |
| `published state` | Retained StudioFS semantic | Shared remote-visible state other clients can discover. |
| `Local-First` | Retained StudioFS semantic | The writing mount observes accepted changes immediately through the merged local view. |
| `eventual consistency` | Retained StudioFS semantic | Other mounts converge later through upload plus publish, not synchronously on each write. |
| `cached-store pipeline` | Borrowed JuiceFS concept | Unified cache, staging, download, upload, and eviction pipeline. |
| `Rust-native implementation` | StudioFS rewrite rule | A design that embraces ownership, type-driven state, explicit async boundaries, and testable modules instead of a Go-shaped transliteration. |

Downstream issues should cite this file directly and then name the relevant row
from section 6 instead of re-explaining the rewrite target.
