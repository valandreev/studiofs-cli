# Collision-free local allocation proof matrix

| Layer | Required proof | Focused command |
| --- | --- | --- |
| Pure allocator state | Versioned paired-state decode; namespace/counter bounds; lowest/highest encodings; exclusive endpoint; separate allocator namespaces; checked reservation and crash-tail skip. | `cargo test -p studiofs-meta sfs623_allocator_state_ -- --nocapture` |
| PostgreSQL namespace authority | Fresh roots mint four distinct namespaces; namespace registry never reuses or wraps; legacy `next_inode` blocks minting; client-slot identity survives. | `cargo test -p studiofs-meta sfs623_pg_allocator_ -- --ignored --nocapture --test-threads=1` |
| Recovery and offline authority | Trusted pair allocates offline; malformed/missing pair rotates both records online; offline recovery is readable/read-only; reopen is required to restore authority; counter exhaustion is terminal. | `cargo test -p studiofs-meta sfs623_recovery_ -- --nocapture` |
| Metadata and VFS | Inode and payload allocation are independent; namespace batches consume inode IDs; direct redb remains unchanged; read-only authority blocks mutation while retaining known reads. | `cargo test -p studiofs-vfs sfs623_read_only_allocator_ -- --nocapture` |
| ID representation and storage | Positive 63-bit IDs preserve 16-digit object keys; parser round trips; remote low-counter sharding spreads successive composite payload IDs; local hashed sharding remains stable. | Focused `studiofs-data` and `studiofs-cache` tests added with the encoding implementation. |
| Lints and contract | Metadata/VFS compile with warnings denied; local-first contract and validation matrix describe namespace minting and degraded authority. | `RUSTFLAGS='-D warnings' cargo check -p studiofs-meta -p studiofs-vfs` |
| Bridge neutrality | Existing shared-VFS semantics remain valid in platform crates; run all available library checks, recording missing host prerequisites rather than substituting another platform. | macOS: `cargo test -p studiofs-macfuse --lib -- --nocapture`<br>Linux: `cargo test -p studiofs-linux-fuse --lib -- --nocapture`<br>Windows: `cargo test -p studiofs-winfsp --lib -- --nocapture` |

The PostgreSQL row runs only with the repository fixture. The macFUSE command
is the applicable check for this metadata-only change because the crate has no
mount-smoke target. A Linux or Windows mounted smoke may run only on a host
with its required driver and disposable mount point; otherwise record the
missing prerequisite. No timing extension, warning suppression, or
bridge-specific workaround substitutes for a failed proof.
