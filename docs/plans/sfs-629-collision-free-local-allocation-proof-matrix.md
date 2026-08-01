# Collision-free local allocation proof matrix

| Layer | Required proof | Focused command |
| --- | --- | --- |
| Pure allocator state | Versioned paired-state decode; namespace/counter bounds; lowest/highest encodings; exclusive endpoint; separate allocator namespaces; checked reservation and crash-tail skip. | `cargo test -p studiofs-meta sfs623_allocator_state_ -- --nocapture` |
| PostgreSQL namespace authority | Fresh roots mint four distinct namespaces; namespace registry never reuses or wraps; bootstrap deletes a retired `next_inode` before minting; client-slot identity survives. | `cargo test -p studiofs-meta sfs623_pg_allocator_ -- --ignored --nocapture --test-threads=1` |
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

## SFS-634 proof transcript (2026-08-01, Windows 11)

| Layer | Command | Outcome |
| --- | --- | --- |
| Lints | `RUSTFLAGS='-D warnings' cargo check -p studiofs-meta -p studiofs-vfs` | Passed. |
| Pure allocator state | `cargo test -p studiofs-meta sfs623_allocator_state_ -- --nocapture` | Passed: 15 tests. |
| Recovery and offline authority | `cargo test -p studiofs-meta sfs623_recovery_ -- --nocapture` | Passed: 6 tests. |
| Metadata and VFS authority | `cargo test -p studiofs-vfs sfs623_read_only_allocator_ -- --nocapture` | Passed: 1 test. |
| ID representation | `cargo test -p studiofs-data object_key -- --nocapture` | Passed: 3 tests, including composite-ID shard preservation and key parsing. |
| Local payload layout | `cargo test -p studiofs-cache payload_shard -- --nocapture`<br>`cargo test -p studiofs-cache parse_payload_object_file_name -- --nocapture` | Passed: 3 sharding and 2 canonical-parser tests. |
| Windows bridge library | `cargo test -p studiofs-winfsp --lib -- --nocapture` | Passed: 104 tests. |
| PostgreSQL namespace authority | `cargo test -p studiofs-meta sfs623_pg_allocator_ -- --ignored --nocapture --test-threads=1` | Passed: 4 tests. |
| Windows mounted smoke | `cargo test -p studiofs-winfsp --test mount_sanity winfsp_mount_write_read_unmount_smoke -- --ignored --nocapture --test-threads=1` | Passed: 1 test. |
| Linux/macOS bridge library and mounted checks | Platform commands from the matrix | Not available on this Windows host; not substituted. |

The transcript confirms the direct-redb boundary and shared-VFS bridge contract
through their local authority, PostgreSQL fixture, Windows bridge-library, and
real mounted smoke checks. Linux/macOS evidence remains platform-gated and is
not inferred from these Windows results.
