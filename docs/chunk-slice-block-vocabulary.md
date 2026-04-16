# Chunk, Slice, Block Vocabulary ADR

For the SFS-242 rewrite, this ADR is now historical background. The canonical
rewrite contract lives in
[juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md),
which supersedes this ADR when the two differ.

## Status: ACCEPTED
## Date: 2026-04-02
## Issue: SFS-154

## Context

StudioFS is moving to a JuiceFS-style data plane with local-first acceptance and
eventual remote visibility. Earlier notes used the words `chunk`, `slice`,
`block`, `overlay`, and `base` in overlapping ways. This ADR freezes the
meaning of those terms for all follow-up implementation work.

The vocabulary in this document is normative. If a later design wants a
different meaning, that design needs a new ADR instead of reusing these terms
loosely.

## Decision

### Terms

| Term | Meaning |
|---|---|
| `chunk` | The top-level logical file range used for addressing and split decisions. A chunk is a fixed boundary partition for a given volume format. |
| `slice` | A chunk-relative byte range that maps a portion of file data to one immutable block. Slices are metadata records, not payload objects. |
| `block` | An immutable payload object. A block stores bytes once and is never edited in place. |
| `overlay` | The local-visible slice set owned by the current client. Overlay is where accepted local writes live before they become remote-visible. |
| `base` | The remote-visible slice set replicated from shared metadata. Base is the lower-precedence view for the current client. |
| `local-visible` | Data that the current mount may return immediately after local acceptance, even if the data is not yet uploaded or published. |
| `remote-visible` | Data that other clients can see after publish using only shared metadata and the data plane, without consulting the writer's local overlay. |

### Mapping From Legacy Terms

The current codebase and older docs use a different set of terms. This ADR
intentionally rebinds them:

| Legacy / current term | New term | Meaning in this ADR |
|---|---|---|
| logical block | `chunk` | The 16 MiB file-range unit used for split, lookup, and overlap decisions today. |
| fragment | `block` | The 4 MiB immutable payload unit stored in the data plane. |
| extent / block slice | `slice` | A chunk-relative range that points at a block. |
| local serving view | `overlay` | The current client's local-visible state. |
| remote-visible committed view | `base` | The shared replicated state other clients may observe. |

When this ADR says `chunk`, it means the legacy/current logical block unit.
When this ADR says `block`, it means the legacy/current cloud fragment unit.
That naming is deliberate and should not be “corrected” in follow-up tickets.

### Chunk Boundary Rules

1. A slice never crosses a chunk boundary.
2. Any read or write span that crosses a boundary is split into per-chunk work.
3. Chunk boundaries are fixed for a given volume format.
4. Changing chunk size is a format decision, not a local optimization.
5. A chunk may contain zero or more slices.

### Slice Rules

1. A slice is always chunk-relative and uses a half-open byte range: `[start, end)`.
2. Slices may overlap.
3. Overlap resolution is per byte, not per slice as a whole.
4. The effective byte at any offset is chosen by precedence, not by where the slice was created.
5. If no slice covers a byte, that byte is a hole and reads as zeroes.

### Slice Precedence Rules

Precedence is strict and deterministic:

1. Local-visible overlay beats remote-visible base.
2. Within the same visibility tier, the later accepted slice wins.
3. A newer slice only shadows the bytes it actually covers.
4. Bytes outside the winning slice continue to fall back to lower-precedence slices.
5. Tombstones and invalidations remove coverage before any lower-precedence slice can be used.

This means the effective file view is always `overlay over base`.

### Tombstone Representation

A tombstone is a metadata-only slice record:

- it carries a chunk-relative byte range
- it has no payload block reference
- it behaves as a deletion marker for that range
- it is used to remove coverage before lower-precedence slices can apply

For implementation purposes, a tombstone may be represented as a slice record
with a `None`/absent block reference or an explicit tombstone kind, as long as
the resolver can distinguish it from a normal data-bearing slice.

### Block Immutability Rules

1. A block is written once and then never changed.
2. Block contents, length, checksum, and identity are fixed at creation.
3. Any content change requires a new block and a new slice that points to it.
4. Blocks are never reused for different bytes, even after garbage collection.
5. Multiple slices may reference the same block only if the implementation
   chooses to deduplicate, but the vocabulary does not depend on deduplication.

### Local-Visible vs Remote-Visible Semantics

Local-visible is the writer's immediate truth:

- the current mount may read it right away
- it may still live only in local state
- it may not yet be uploaded or published

Remote-visible is the shared truth:

- other clients can discover it from shared metadata
- the backing block must be available through the data plane
- the state must be durable enough to survive restart without writer-local overlay

The local writer is allowed to observe more than the remote world, but the remote
world must never observe bytes that violate the immutability rules above.

## Canonical Read Path

1. Resolve the inode's effective view as `overlay over base`.
2. For each requested chunk, select the winning slices by precedence.
3. Fetch any missing immutable blocks from local storage or the remote data plane.
4. Assemble the bytes in order and return them to the caller.

## Canonical Write Path

1. Split the incoming write by chunk boundary.
2. Materialize new immutable blocks for the written bytes.
3. Create local-visible overlay slices that point at those blocks.
4. Return to the caller once the local mount has accepted the data.
5. Publish the same logical bytes into remote-visible base later through the
   background materialization and upload pipeline.

## Consequences

- Follow-up tickets can reference `chunk`, `slice`, `block`, `overlay`, `base`,
  `local-visible`, and `remote-visible` without redefining them.
- Any overlap behavior that does not fit these precedence rules is a model bug.
- Any change to chunk size or slice precedence needs a new ADR and a migration
  plan.
- Future read/write implementation work should describe behavior using this
  vocabulary instead of introducing parallel terms.

## Non-Goals

This ADR does not decide:

- the numeric chunk size
- the numeric block size
- the on-disk encoding of slices or blocks
- garbage-collection policy for unreferenced blocks
- migration mechanics for existing volumes

Those topics can be specified later, but they must use the vocabulary frozen
here.
