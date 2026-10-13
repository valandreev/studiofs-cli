# Merge concurrently created directories and resolve concurrent file creates by last writer

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. Maintain it according to `.agent/PLANS.md`.

## Purpose / Big Picture

StudioFS is a local-first filesystem: every computer mounts the volume, works against its own local metadata database (`redb`), and exchanges metadata with other computers through an ordered event log stored in PostgreSQL. Computers connect and disconnect, live on different networks, and never see the same state at the same instant. A delay between one computer making a change and another computer seeing it (the "sync window") is normal and must never cost anyone their work.

Today it does. If two computers create a directory with the same name during a sync window (the everyday `mkdir -p project/renders` case) the directory created by one of them, together with every file written into it, silently disappears from every computer. If the second computer has not even committed its directory locally when the first computer's directory arrives, its own pending directory and all files under it are rejected with "entry already exists", which an application only sees at `fsync` or `close`.

After this change, both directories merge into one directory that contains the files from both computers, on every computer, including the one whose directory "lost". Two files created with the same name during a sync window resolve by last writer: the file published later in the global order wins, which is the existing rule for overwrites. A user can observe the fix by running the mounted end-to-end scenario described under Validation, where two clients create the same directory without waiting for each other and both files remain readable on all four clients.

## Progress

- [x] (2026-10-13) PR #794 follow-up: reproduce and fix displaced-file parent rewriting, pending local name authority matching base, and malformed checkpoint suffix acceptance; correct the alias-chain comment. All four new regression tests failed before their corrections and passed afterwards.
- [x] (2026-10-13) PR #794 follow-up validation: replay suite 112 passed / 44 ignored; redb suite 328 passed / 1 ignored; snapshot suite 24 passed; metadata check and Clippy; formatting and diff checks. Independent read-only review found no actionable issues.
- [x] (2026-10-13) Root-cause investigation of the five test-only commits on branch `bug/linux-bughunt-01`; see Surprises & Discoveries.
- [x] (2026-10-13) User decisions recorded: directories merge; same-name file creates resolve by last writer.
- [x] (2026-10-13) Red test `replay_merges_concurrently_created_directories` in `crates/studiofs-meta/src/pg_engine/tests/remote_apply.rs` fails: `from-a.txt must survive the directory merge`.
- [x] (2026-10-13) Milestone 1: `RedbMetaEngine::canonicalize_replay_batch_in_txn` in `crates/studiofs-meta/src/redb_engine/namespace_merge.rs`, table `DIRECTORY_ALIASES_TABLE`, wired into `apply_decoded_batches_in_txn`. Tests `replay_merges_concurrently_created_directories`, `replay_redirects_later_events_under_a_merged_directory`, `replay_rmdir_of_alias_keeps_merged_directory` pass; removing the canonicalization call makes the first fail.
- [x] (2026-10-13) Milestone 2: `self_replay_of_merged_directory_acknowledges_and_keeps_local_subtree` passes (raw journal still self-acknowledges; both subtrees visible). Replay restart relies on the replay write transaction: aliases are written in the same redb transaction and roll back with an aborted page; no separate test.
- [x] (2026-10-13) Milestone 3: `ServingCheckpoint` format version 2 with `directory_aliases`, version-1 fallback in `ServingCheckpoint::decode`, all four decode sites switched, snapshot rows accept versions 1 and 2. Tests `serving_checkpoint_decodes_both_format_versions` and `checkpoint_carries_directory_aliases_to_cold_clients` pass; dropping alias import makes the latter fail. Full `cargo test -p studiofs-meta --lib`: 841 passed.
- [x] (2026-10-13) Milestone 4: replay already binds a same-name file to the later `AddEntry`; characterized by `replay_resolves_concurrent_file_creates_by_last_writer` and `replay_does_not_merge_a_directory_into_a_file`.
- [x] (2026-10-13) Milestone 5: instead of rewriting VFS queues, a deferred create (`RedbMetaEngine::apply_namespace_mutations`, used only by the VFS deferred drain) may shadow a same-kind entry that serving holds only because replay put it in base (`NamespaceCreateSpec::shadowing_replayed_entry`). The client then behaves as if its create had committed first, and Milestones 1 to 3 merge or resolve it on replay. Tests: `deferred_create_shadows_replayed_directory_and_merges_on_self_replay`, `deferred_create_keeps_rejecting_local_and_cross_kind_conflicts`, `deferred_file_create_over_replayed_file_wins_by_last_writer` (meta) and `deferred_namespace_dir_shadows_peer_directory_replayed_after_admission` (VFS). Without the shadowing the VFS test fails with exactly the abandoned-commit error from commit `463c6433`: "entry already exists: \"shared\" in parent 1". Full suites: meta 845 passed, VFS 1153 passed.
- [x] (2026-10-13) Milestone 6: reverted the `client_a_peer_visible` wait from `463c6433` in the working tree; `mounted-lww-reconnect-chaos-convergence` passed for seeds 360, 361, 362 (runs `20261013T075007Z-ed67fc`, `20261013T075044Z-8a3d94`, `20261013T075123Z-42eb02`). Added registered scenario `mounted-concurrent-mkdir-merge-convergence` (`crates/studiofs-e2e/src/scenario/workloads/concurrent_mkdir.rs`); with the fixed binary run `20261013T075556Z-f911b9` passed and all three clients read both files.
- [x] (2026-10-13) Milestone 7: `docs/adr/0012-concurrent-same-name-creates-merge-directories.md`, section "Concurrent Same-Name Creates" in `docs/pg-sync-contract.md`, scenario entry in `docs/tests.md`.
- [x] (2026-10-13) Red run of `mounted-concurrent-mkdir-merge-convergence` with a product binary built from commit `68087d85` (before this plan): run `20261013T080244Z-ccbbe4` failed validation; `from-client-a.txt` was unreadable on all three clients, including client-a, while the trees still "converged".

## Surprises & Discoveries

- PR #794 review: `DENTRIES_TABLE` contains replay mirrors as well as local changes, so requiring an absent serving row would reject legitimate deferred creates. A local rename away and back reproduces the actual authority gap: the serving binding equals base while unacknowledged namespace events still protect the name. Check those events only on a same-name replay conflict.
- PR #794 review: postcard `from_bytes` ignores unused bytes. The v2-error-to-v1 fallback therefore accepted an incomplete alias suffix. Decode the shared six-table prefix once and require any present alias suffix to decode completely, preserving existing v1/v2 bytes.
- Observation: Commit `463c6433` hid a data-loss bug, not a test race. A lagging client created its own directory with the same name, and the deferred namespace drain abandoned that directory and all dependent operations.
  Evidence: `crates/studiofs-vfs/src/vfs/namespace_ops.rs` `drain_deferred_namespace_metadata` logs "deferred namespace metadata operations lost a concurrent namespace conflict" and calls `abandon_failed_deferred_namespace_batch`; the existing test `deferred_namespace_dir_conflict_abandons_child_and_retries_independent_ops_across_batches` in `crates/studiofs-vfs/src/vfs/tests.rs` codifies that the child file is abandoned.

- Observation: The loss also happens on the fully published path, without any deferred-window race. Applying `MetaEvent::AddEntry` to the base table simply overwrites `(parent, name) -> inode`, so the earlier directory and its children become unreachable on every client.
  Evidence: `replay_merges_concurrently_created_directories` fails with "from-a.txt must survive the directory merge".

- Observation: The other test-only commits were not hiding product races. `987e1051` and the second hunk of `909426a5` raised hang guards around waits that are notification-driven; the measured cause of slowness is local `fsync` latency on the redb file (25 ms to 1106 ms on the development host, even for an isolated test), and short guards were asserting disk latency. `d85793d6` isolates PostgreSQL unit fixtures from the periodic redb durability thread; event-log pruning intentionally skips a busy writer and retries later. `d530f902` raised a controller lease TTL in diagnostics tests; the lease is wall-clock based by design and the controller owns renewal; isolated workflow tests take at most 0.43 s.
  Evidence: temporary probes (removed) printed `durability_barrier_commit_ms=1106`, `redb_commit_ms=74 force_immediate=Some(false)` during takeover outliers, and `release_wait_ms` of 0 to 6 ms under 16-way parallel load.

- Observation: The first hunk of `909426a5` waited for a product timer, not an event. The open-handle deferred materializer lingers 6 x 200 ms before committing, so the test always took about 1.23 s against a 2 s guard. Fixed in this branch by interrupting the linger (`wait_for_deferred_materialization_inflight`) and waiting for the deferred setattr (`wait_for_deferred_namespace_metadata`); the test now takes about 0.3 s, and removing the materializer wait makes it fail with `left: 0, right: 24`.

- Observation: A separate convergence bug exists. Retained run `target/e2e-runs/20261012T225217Z-aeac44` failed `mounted-hotspot-churn-convergence`: client-b kept `hotspot-*/slot-3-round-9-renamed.txt` at 223 bytes while client-a had 227 bytes for 90 s. Out of scope for this plan; tracked separately.

## Decision Log

- PR #794 follow-up: rewrite `displaced_set_attr` with the existing non-alias attribute policy, which resolves a displaced child's parent and preserves canonical owner authority. Keep `displaced_remove_attr_ino` unchanged: `rename_atomic` emits that field only for non-directory victims; only directory inodes become aliases. Resolving an alias removal to the canonical directory would contradict the owner-preservation contract.
- PR #794 follow-up: skip the encoded in-memory key suggestion (the tuple has identical key semantics), the symlink call-site suggestion (`NamespaceMutation` has only file, directory, and setattr variants), and the hypothetical `has_aliases` reset (the flag is initialized from the table and only transitions to true). These findings do not identify incorrect current behavior.
- Decision: Concurrently created directories with the same parent and name merge into one directory. The directory whose `AddEntry` appears first in the global PostgreSQL event order is canonical; the later one becomes an alias of it.
  Rationale: User decision (2026-10-13). `mkdir -p` is idempotent in intent, and all clients replay the same order, so "first published wins the identity" is deterministic everywhere without coordination.
  Date/Author: 2026-10-13, user and Claude.

- Decision: Same-name regular-file creates resolve by last writer: the later `AddEntry` in global order binds the name. This is already what base replay does for overwrites; the work is to make the deferred-window path agree and to prove convergence with tests.
  Rationale: User decision (2026-10-13).
  Date/Author: 2026-10-13, user and Claude.

- Decision: Canonicalize each replayed batch once, inside `RemoteApplyCoordinator::apply_decoded_batches_in_txn` in `crates/studiofs-meta/src/pg_engine/remote_apply.rs`, before both the self-batch and foreign-batch branches. Every downstream applier (base, auxiliary, serving, reconcile) then sees identical canonical events.
  Rationale: More than a dozen functions match on `MetaEvent::AddEntry`; duplicating merge rules in each would let clients diverge. Self-batch prefix matching (`acknowledge_local_prefix_from_self_batch_in_txn`) must keep comparing the raw events, because the local journal holds raw events.
  Date/Author: 2026-10-13, Claude.

- Decision: Persist aliases in a new local redb table and carry them in the serving checkpoint (format version 2, still decoding version 1 as "no aliases").
  Rationale: The client that created the alias directory can publish more events that reference it after the merge (operations admitted before it replayed its own batch). A client that bootstraps from a later checkpoint must still redirect those events. `ServingCheckpoint` is encoded with postcard, which is not self-describing, so a new field requires a format version bump.
  Date/Author: 2026-10-13, Claude.

- Decision: Out of scope for this plan, recorded as known gaps in the ADR: a rename of a directory onto a name that a peer concurrently created as a directory; a type conflict (file versus directory) for the same name; `RemoveEntry` of a merged directory by a peer that only saw its own empty alias; exact `nlink` of a merged directory before the next attribute event from its owner.
  Rationale: Each needs re-parenting of already-applied children or new event fields; the create/create case is the common one and the one the failing scenario exercises.
  Date/Author: 2026-10-13, Claude.

## Outcomes & Retrospective

PR #794 follow-up preserves pending local name authority even when serving matches base, rewrites displaced-file parents through directory aliases, and rejects malformed checkpoint suffixes without changing valid v1/v2 bytes. Validation ran `cargo test -p studiofs-meta --lib pg_engine::tests::remote_apply:: -- --quiet`, `cargo test -p studiofs-meta --lib redb_engine::tests:: -- --quiet`, and `cargo test -p studiofs-meta --lib snapshot::tests:: -- --quiet` (464 passed total), plus `cargo check -p studiofs-meta`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, `cargo fmt --all --check`, and `git diff --check`. Ignored PostgreSQL tests, the ignored local storage stress test, and mounted E2E scenarios were not run for this follow-up.

Directories created concurrently under one name now merge on every client, including the client whose directory became an alias and clients bootstrapped from a later checkpoint; same-name files resolve by last writer; the VFS deferred window no longer abandons a lagging client's work. The design moved the deferred-window fix into the metadata create path (shadowing a replayed entry) instead of a VFS-wide alias map, which reused the replay merge and kept the VFS unchanged. Remaining gaps are recorded in ADR 0012.

## Context and Orientation

The metadata crate is `crates/studiofs-meta`. Its local database is `RedbMetaEngine` (`crates/studiofs-meta/src/redb_engine.rs` and `crates/studiofs-meta/src/redb_engine/`). It keeps two layers of namespace rows. The "base" layer (`BASE_DENTRIES_TABLE` in `crates/studiofs-meta/src/redb_engine/layout.rs`) is what this client has learned from the shared PostgreSQL event log. The "overlay" layer holds this client's own changes that are not yet published. The "serving" state is what the mounted filesystem shows: overlay on top of base.

`PgMetaEngine` (`crates/studiofs-meta/src/pg_engine.rs` and `crates/studiofs-meta/src/pg_engine/`) publishes local events to PostgreSQL in transactional batches, each with a monotonically increasing `batch_version`, and replays batches from other clients. Replay pages arrive as `DecodedReplayBatch { batch_version, events, is_self_batch }` and are applied by `RemoteApplyCoordinator::apply_decoded_batches_in_txn` in `crates/studiofs-meta/src/pg_engine/remote_apply.rs`. A self batch is one this client published; replaying it acknowledges ("self-ack") the matching prefix of the local event journal and retires the corresponding overlay rows. A foreign batch is applied to base and, where no local overlay owns the same keys, to serving.

A namespace event is a `MetaEvent` (`crates/studiofs-meta/src/events.rs`). The relevant variants are `SetAttr { attr }` (an inode's full attributes, including its `parent`), `AddEntry { parent, name, entry: DirEntryValue { ino, kind } }`, `RemoveEntry { parent, name }`, and `RenameEntry { parent, name, new_parent, new_name, entry, .. }`.

The VFS crate is `crates/studiofs-vfs`. To keep `mkdir`/`create` callbacks fast, it admits namespace operations into an in-memory "deferred namespace queue" and commits them to local redb a few milliseconds later (`crates/studiofs-vfs/src/vfs/namespace_ops.rs`, `drain_deferred_namespace_metadata`). If replay has meanwhile inserted the same name into local redb, the commit returns `MetaError::NamespaceBatchOperation { source: EntryExists, .. }` and today the conflicting operation and everything that depends on it are abandoned.

## Plan of Work

Milestone 1 adds a namespace merge resolver to the metadata crate. Create `crates/studiofs-meta/src/pg_engine/namespace_merge.rs` with a function that takes the replay write transaction and one batch's events and returns canonical events. It keeps a small in-memory map of dentries written earlier in the same batch, and otherwise reads base dentries. For `AddEntry` of a directory whose `(parent, name)` already resolves to a different directory, it records `alias -> canonical` and drops the event. For every event it rewrites parent inode references through the alias map: `AddEntry.parent`, `RemoveEntry.parent`, `RenameEntry.parent`, `RenameEntry.new_parent`, `RenameEntry.entry.ino` when it names an alias, and `SetAttr.attr.parent`. It drops `SetAttr` whose `attr.ino` is an alias, because the canonical directory's owner remains authoritative for its attributes. Aliases are stored in a new table `DIRECTORY_ALIASES_TABLE: u64 -> u64` declared in `crates/studiofs-meta/src/redb_engine/layout.rs` and created at open next to the other tables. Wire the resolver into `apply_decoded_batches_in_txn`: compute canonical events for each batch, collect reconcile keys from both raw and canonical events, pass canonical events to `apply_visible_self_batch_in_txn` and `apply_foreign_batch_in_txn`, and keep raw events for `acknowledge_local_prefix_from_self_batch_in_txn`. Acceptance: `replay_merges_concurrently_created_directories` passes, and a second test proves that a later foreign batch creating `from-b-later.txt` under the alias lands in the canonical directory.

Milestone 2 proves the self view. Write a test in the same file where the local engine creates `shared/` and `from-b.txt` through `PgMetaEngine` local operations, a foreign batch creates `shared/` and `from-a.txt` at version 1, and the local client's own batch arrives at version 2 as a self batch. After replay, `lookup(ROOT, "shared")` resolves to the canonical directory and both files are visible; the overlay rows for the alias are retired. Also prove that applying the same pages again after an aborted transaction yields the same result (replay restart idempotence).

Milestone 3 extends `ServingCheckpoint` in `crates/studiofs-meta/src/snapshot.rs` with `directory_aliases: Vec<(u64, u64)>`, bumps `FORMAT_VERSION` to 2, keeps a version-1 decode path that yields no aliases, exports and imports the table in `crates/studiofs-meta/src/redb_engine/snapshots.rs`, and tests a round trip plus decoding of a version-1 byte string.

Milestone 4 adds tests that two foreign `AddEntry` events for the same file name converge to the later inode on an observer and on both creators, and that the displaced inode is not left reachable.

Milestone 5 changes the VFS deferred drain. When a deferred `CreateDir` conflicts with an existing directory, the drain resolves the local directory to the existing one: queued operations whose parent is the local directory are rewritten to the existing directory, the local `CreateDir` settles successfully, and a VFS-local alias redirects later kernel requests that still name the local inode. When a deferred `CreateFile` conflicts with an existing regular file, the local create wins by last writer and replaces the binding. Update `deferred_namespace_dir_conflict_abandons_child_and_retries_independent_ops_across_batches` to the new expected behavior and add a file-conflict test.

Milestone 6 removes the `client_a_peer_visible` wait added by commit `463c6433` in `crates/studiofs-e2e/src/scenario/workloads/advanced_mounted/open_handle.rs` and adds a registered mounted scenario in which two clients `mkdir -p` the same directory and write different files without waiting, then all clients converge to both files.

Milestone 7 adds `docs/adr/0012-concurrent-namespace-creates-merge-directories.md` and updates `docs/pg-sync-contract.md` with the merge and last-writer rules and the known gaps.

## Concrete Steps

From the repository root `/home/val/dev/studiofs-cli`:

    cargo test -p studiofs-meta --lib replay_merges_concurrently_created_directories
    cargo test -p studiofs-meta --lib remote_apply
    cargo clippy -p studiofs-meta --all-targets -- -D warnings

Before Milestone 1 the first command fails with "from-a.txt must survive the directory merge"; after it, it reports `1 passed`.

## Validation and Acceptance

Metadata acceptance is the passing unit tests named in each milestone, run with `cargo test -p studiofs-meta --lib` filters, with nonzero test counts. VFS acceptance is `cargo test -p studiofs-vfs --lib deferred_namespace`. Mounted acceptance requires the `studiofs-e2e` runner with Docker, PostgreSQL and RustFS; rebuild `studiofs` and `studiofs-e2e` first, run the new scenario and `mounted-lww-reconnect-chaos-convergence`, and retain artifacts under `target/e2e-runs`.

## Idempotence and Recovery

The resolver is deterministic for a given base state and batch, and runs inside the replay write transaction, so an aborted page leaves no alias rows. Re-running tests is safe; they use temporary directories under `target/`.

## Artifacts and Notes

    pg_engine::tests::remote_apply::replay_merges_concurrently_created_directories panicked:
    from-a.txt must survive the directory merge

## Interfaces and Dependencies

In `crates/studiofs-meta/src/redb_engine/namespace_merge.rs`:

    impl RedbMetaEngine {
        pub(crate) fn canonicalize_replay_batch_in_txn<'event>(
            txn: &RedbWriteTransaction<'_>,
            events: &'event [MetaEvent],
        ) -> MetaResult<Cow<'event, [MetaEvent]>>;
    }

The resolver lives in `crates/studiofs-meta/src/redb_engine/namespace_merge.rs` (not under `pg_engine/`) because the dentry lookup helpers it needs are private to the `redb_engine` module.

No new crate dependencies.
