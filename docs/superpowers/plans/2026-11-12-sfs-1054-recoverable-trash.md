# SFS-1054 recoverable trash

## Purpose and context

Users must be able to use native OS Move to Trash, list, restore and Clear on a
shared StudioFS volume without retaining direct deletes. Work starts on
`feat/sfs-1054-recoverable-trash` at
`0358242f3c1e90959faf66e0be0cff95f424b47c`, with a clean worktree, in
the repository root. The canonical product contract is
`docs/superpowers/specs/2026-11-12-recoverable-trash-design.md`; delivery is
tracked in `docs/superpowers/plans/2026-11-12-qol-023.md`.

The VFS currently destroys a tree on a trash rename in
`crates/studiofs-vfs/src/vfs/namespace_ops.rs`, method
`delete_source_tree_and_invalidate_trash_target`, called by `rename_inner`.
WinFSP tests can mount a fresh local redb VFS without using a user volume via
`with_winfsp_mount` in `crates/studiofs-winfsp/tests/mount_sanity.rs`.

## Progress

- [x] (2026-11-16) Complete the Linux native-trash slice in WSL from this
  checkout, including GIO draft lifecycle, kernel rename identity, recursive
  Clear permissions, long names and local deletion time. Final Windows/Linux
  focused results are recorded in the closing checkpoint below.
- [x] (2026-11-16) Obtain fresh independent review of the completed Windows/Linux
  slice: Opus 5.5 APPROVED ("Independent Linux/Windows review follow-up").
- [x] (2026-11-16) Pre-macOS item 1: mounted original inode and outside hard link.
- [x] (2026-11-16) Item 2: mounted S3-outage crash replay and cold bootstrap GREEN;
  PG-down + S3-down repair refusal stays an intentional, documented limit.
  (2026-11-18) Lifted by the publication fence, ADR 0013: the scenario passes.
- [x] (2026-11-16) Item 3: mounted janitor GC GREEN (runs 7, 8), reviewed.
- [x] (2026-11-16) Item 4: desktop GVFS `trash:///` GREEN (runs 5, 6), reviewed.
- [x] (2026-11-16) Item 5: bilingual guides, platform contract, validation
  matrix and final focused gates; see "SFS-1054 final state" at the end.
- [ ] DEFERRED (user decision): compile and accept the preserved macOS
  implementation on a macOS host; Finder and legacy macFUSE/FSKit acceptance.
- [x] Read root/scoped contracts, canonical specification and delivery plan;
  confirmed branch, base and clean status.
- [x] Graph discovery and exact source verification of destructive rename;
  coverage reports metadata_match for six inspected source/test files.
- [x] Prove native Shell recycling is reachable on a disposable WinFSP mount
  (prototype A green; production red at the root `$RECYCLE.BIN` gate).
- [x] Production seam: visible root `$RECYCLE.BIN` plus retention of renames
  into it when TTL is positive; restore and Clear proven through Shell API
  probes (`winfsp_native_trash_*`). Superseded scope: interactive Explorer UI
  stays unproven.
- [x] Execute actual old 0.2.2 decoder against proposed format 6 (2 tests).
- [x] Enforce format-6 publication/checkpoint and old deletion-claim boundaries
  with isolated runtime tests; actual old mounted-binary acceptance remains open.
- [x] Implement deterministic whole-day retention policy (6 tests).
- [x] Enforce positive-policy decode invariant and implement local durable settings
  CAS, including concurrent writers, stale revisions and reopen (4 new tests).
- [x] Complete settings journal/event, base/serving projection, online PG ordering
  and checkpoint transport; real PG two-client/catch-up/bootstrap test green.
- [x] Implement stopped-cache format-6 upgrade CLI, server-success/local-failure
  retry and real command restart preservation of accepted inode/journal.
- [x] Implement local atomic retained receipt/restore/purge-request transitions,
  existing replay/checkpoint transport, readonly forwarding and shared terminal
  publication CAS with real two-client conflict ordering.
- [x] Complete payload-backed offline conflict/restart recovery, shared physical
  settlement, creation TTL flag (`init --trash-days`) and explicit online
  `trash-days` CLI (2026-11-12/13 sections). `upgrade` has no TTL flag.
- [x] (2026-11-12) Extend the existing PG terminal-conflict test through foreign
  apply and restart of both clients, preserving accepted receipts and journals.
- [x] (2026-11-12) Persist exact retained-publication transaction retry receipts;
  response-loss retry across reopen skips namespace replay and version changes.
- [x] (2026-11-12) Byte-backed conflicting-restore recovery: only the losing
  terminal receipt record is rejected from publication; VFS restore captures a
  regular file's bytes under new object IDs before the restore commits.
- [x] (2026-11-12) Authority projection seeding finished: dump/load PG test
  deadlock diagnosed and fixed (test-only), `studiofs load` seeds
  `metadata_state.volume_settings`, checkpoint-bootstrapped client regression.
- [x] (2026-11-12) Physical purge/expiry settlement (handoff item 3): shared-ack
  gated unlink of `receipt/data` through the existing deletion lifecycle, expiry
  in the PG poll loop, read-handle protection. See "Physical purge and expiry
  settlement". A losing directory restore after settlement is still open.
- [x] Implement permission-filtered native views and restore/purge fencing.
  VFS seam done (2026-11-13, see "Native-view VFS seam"). *(Superseded: the
  2026-11-13 note "Native $I/$R views, WinFSP root visibility and ACL-filtered
  listing are NOT done" was closed by the 2026-11-13..15 native checkpoints.)*
- [x] Integrate GC, restart/replay, focused acceptance and bilingual guides
  (items 2–5, 2026-11-16).

## Milestones and concrete steps

First establish native feasibility on Windows using a fresh local test VFS and
an unused drive. The probe must test `$Recycle.Bin` and the current Windows SID
directory and invoke native Shell recycling; successful ordinary rename alone
does not prove native recycle UI. Keep probe data under target and never select
an existing mount. Record failure precisely; a missing driver or services is a
validation gap, not evidence native integration is impossible.

Next enforce a new persisted format before retaining data. Existing 0.2.2
`decode_format_info` first decodes a u16 format version and rejects any value
other than its CURRENT_FORMAT_VERSION (5). Both local and PostgreSQL format
checks and bootstrap checkpoints must agree. Test actual old code, not an
unknown optional flag. No migration of a user volume is authorized here.

Then add a separately versioned mutable volume setting replicated as metadata,
with nonnegative whole-day TTL defaulting to zero. Preserve the last positive
period when disabling. Retained records identify original-parent inode,
initial deletion time and transition version. Current original-parent ACLs
filter all views; missing or itself-trashed parents hide records. Restore also
requires destination write access. No administrator bypass is introduced.

Use atomic local metadata transactions and existing publication/GC ownership.
Before enabling retained entries, tests must establish an explicit conflict
outcome for offline restore versus published purge/expiry, preserving accepted
local data. Local purge cannot itself authorize remote deletion. Direct unlink,
overwrite and replacement keep existing semantics. Adapt native views only
after proving platform behavior, and document mount-principal trust limits.

Run focused nonzero tests for TTL/disable, ACL revocation and parent identity,
collision/retry, offline restart, hardlinks/open handles, snapshot protection,
peer convergence and restore/purge/expiry conflicts. Use controlled time and
bounded condition polling. Build product before mounted acceptance. Never run
full suites. Finish affected package check/Clippy, cargo fmt --all -- --check
and git diff --check. Exact focused commands/results are recorded below as
tests become available.

## Surprises & Discoveries

2026-11-16: GIO atomically writes `.trashinfo` through a temporary filename and
closes it before rename. A Linux kernel rename also keeps the source dentry's
inode although the retained view uses a different synthesized inode. Finally,
kernel `default_permissions` rejects recursive Clear of root-owned `0555`
retained directories. These required native execution, not just VFS tests; see
the final Linux checkpoint for the fixes and focused RED/GREEN evidence.

WinFSP is installed, including its x64 runtime and driver. `ctx7` is unavailable
in PowerShell; use official upstream documentation for external API details.
The old v0.2.2 source is locally available through Git. No native feasibility
or persisted-boundary proof is claimed yet.

## Decision Log

2026-11-16: Complete Linux in WSL using the current `/mnt/e/dev/studiofs-cli`
sources, a separate build directory and owned ext4 test paths. Reuse the
existing bounded invalidation worker for target-entry notifications after
rename replies; do not expose private retained inodes or introduce detached
notification workers. Keep Linux synthetic directory ownership/permissions
local to the mount profile and preserve Windows behavior. macOS acceptance is
explicitly deferred. This follows the user's final WSL authorization.

2026-11-12: Keep retention disabled until both native and format gates pass.
Do not change destructive production behavior merely to make a discovery probe
pass; any temporary native prototype must remain clearly scoped.

2026-11-12: Native Windows integration uses Shell's shared `\$RECYCLE.BIN`
layout on the existing non-persistent-ACL volume. Changing the FS name or
ACL flags is not needed and is no longer under investigation; see the
correction under Outcomes.

2026-11-12: Conflict outcome for an accepted offline restore that loses the
shared receipt authority to a peer purge: the shared receipt row keeps the
winner (PurgeRequested); only the loser's terminal receipt record is dropped
from publication. The rest of the accepted local journal (the namespace move
out of the receipt and new-object-ID slices) still publishes, so accepted local
data is neither discarded nor blocked. The symmetric case (losing purge) just
leaves the restored data in place. See "Byte-backed conflicting restore".

2026-11-12: A losing terminal receipt record is final, so it is retired through
`complete_local_event_ranges` instead of `reject_local_event_ranges` (a rejected
range is a durable-cursor barrier, so the persisted cursor never passed it and
a restart republished the accepted journal). Retirement is process-local, so
after a restart self-ack may omit a local terminal receipt record only with
proof that it lost: base already holds a different terminal record for that
receipt (superseded shape-only omission; see "Second-round review fixes").

2026-11-12: Finding-4 investigation (superseded by the 2026-11-13 eager-copy
decision below; the `nlink > 1` refusal no longer applies). User direction: extend an existing path
before copying). No existing path re-uploads locally present slices under new
object IDs once old IDs are deletion-fenced. Fresh IDs are minted only for dirty
buffered blocks (`studiofs-vfs/src/local_dirty/core.rs`
`try_prepare_persisted_blocks`, `local_dirty.rs` `drain_snapshot`); the fence in
`pg_engine/snapshot_reachability.rs` only fails publication with "requires a
new object id after deletion has started", and no VFS/cache/CLI code handles
that error by re-keying. Decision: keep the eager copy, but only when the
receipt is still `Retained` and the destination is free, refuse it with
`ResourceBusy` when the payload has another link or an open handle, and require
a background caller when wired (it may fetch remote-only bodies and pins a
second local version). A lazy "re-key on fence" path is the upgrade if restore
cost matters; it does not exist today.

2026-11-12: The reserved receipt xattr is never user-mutable (meta rejects
set/remove with `AccessDenied`), and shared publication rejects removing a
receipt xattr or a receipt inode that has a `trash_entry_authority` row. A
rejected removal stays pending (not retired like a classified loser), so the
private store must be unreachable for user namespace mutation before wiring.
See "Third-round review fixes".

2026-11-12: Authority projection seeding (handoff item 2). Remote snapshots
and serving checkpoints export the BASE receipt record whenever base holds a
terminal one, because a retired local loser keeps its overlay record. `studiofs
load` rebuilds `trash_entry_authority` in its own transaction: checkpoint
records are inserted as cut state, and later batches go through
`apply_retained_authority`, which is the only transition validator. Load is
refused with `TargetHasTrashAuthorityState` when the target already has trash
authority or publication-receipt rows, because replacing them would drop the
terminal tombstones that delayed clients rely on. See "Authority projection
seeding".

2026-11-12: `studiofs load` seeds `metadata_state.volume_settings` (the
trash-days CAS row) in the same transaction, from the checkpoint's
`volume_settings` superseded by post-cut tag-19 events. One ordering rule,
`VolumeSettings::is_superseded_by`, serves both this seed and redb replica
apply. Seeding is chosen over refusing, because the checkpoint and the log
already carry the authoritative settings. See "Seeding completion".

2026-11-12: Physical settlement reuses the deletion lifecycle. A purged entry
is settled by unlinking its `receipt/data` tree with `NamespaceDeleteTxn`
(`with_expected_ino`), only when base holds the replayed shared
`PurgeRequested` record and `data` still binds `entry.inode`. Publication then
closes reachability intervals and queues `snapshot_payload_gc_candidates`; the
janitor deletes after its grace window and snapshot pins. No new cleaner, no S3
call, no receipt or authority-row removal, no guard bypass (`receipt/data` is
never itself a receipt). See "Physical purge and expiry settlement".

2026-11-12: Runner choice. Expiry and settlement run in the existing
`pg-meta-poll` thread (`pg_engine/lifecycle.rs`), at most once per 60 s, only
while `Live`. It already owns periodic replication metadata work, it can read
the shared authority rows, and read-only snapshot engines
(`ReadOnlyMetaEngine` over plain redb) never run it. Rejected: the CLI janitor
(no redb namespace to unlink in), and the VFS `background_gc` loop (it has no
retained store without widening `VfsMeta`, which is item-4 wiring). Because
meta knew only write handles (`active_open_inodes`), the VFS now reports every
open/release through `MetaReplication::note_open_handle`.

2026-11-13: Design confirmed for losing restore after settlement (open item 1):
reproduced in RED tests for both directories (`EntryNotFound` on child after
settlement replay) and regular files (purged inode identity resurrected on
peer). VFS `restore_retained_entry` now eagerly copies files, symlinks, and
directory trees (preserving in-tree hard links via an `nlink > 1` map) into new
inodes under a unique `receipt/restore-<id>` stage, keeping open-handle
`ResourceBusy` refusal across the tree and dropping the `nlink > 1` refusal.
Meta `restore_retained_entry(receipt, parent, name, stage)` then commits in one
transaction and journal order: `Restored` record, rename `stage` ->
`(parent, name)`, and post-order `NamespaceDeleteTxn` unlink of `receipt/data`
(shared helper factored out of `settle_retained_purge`). Settlement still
touches only `receipt/data`, never stages. Known limits marked with `ponytail:`:
a crash-abandoned stage leaks, xattrs are not copied, and the inode number
changes.

2026-11-13: Item 4 private-store wiring must block kernel-facing lookup,
readdir, create, mkdir, rmdir, rename and link traversal into or out of the
store, rather than deny access via ACL. Internal restore copying continues as
the mount principal; restore requires original-parent and destination write
permissions. This path-blocking is a dependency of item 4.

2026-11-13: Restore stages are named `restore-<owner>-<id>`, where `<owner>` is
a random 64-bit tag persisted per local volume in the `retainedRestoreOwner`
counter. Ownership proof options rejected: an inode-range check, because PG
inode batches live only in memory and are lost on restart; and the replication
`invalidation_sender_id`, because it is PG-only and `None` on redb, so it cannot
cover local-only volumes or the redb unit tests. A stage is swept only when its
name carries this volume's owner tag, so a peer's stage is never removed.

## Validation and Acceptance

Completion requires native Move to Trash/list/restore/Clear, direct delete
bypass, TTL CLI and peer convergence, current ACL enforcement, crash/conflict
recovery and GC protection. Unit results are separate from native Windows,
Linux and macOS evidence. *(Superseded 2026-11-16: implementation is complete
for Windows and Linux; macOS stays deferred. See "SFS-1054 final state".)*

## Idempotence and Recovery

Use only new target-local temporary volumes. Existing mounts/data must not be
modified. Retain diagnostics; do not reset shared branch changes. No commits,
pushes, PRs, issue writes or nested agents are authorized for this implementer.

## Outcomes & Retrospective

**Final (2026-11-16, item 5):** Windows and Linux recoverable trash are
implemented and pass the focused and mounted evidence listed in "SFS-1054
final state" at the end of this plan. Acceptance for 0.2.3 still awaits the
user decision on the unproven surfaces listed there. macOS is deferred. Every outcome below is
historical and kept for traceability; where it conflicts with the final state,
the final state wins.

2026-11-16 current outcome (historical): the Windows shared-VFS controls and Linux native
trash slice pass focused validation, including a rebuilt mounted GIO test.
Fresh independent review and macOS native acceptance remain separate gates.
The initial discovery outcome immediately below is historical; the latest
checkpoint at the end of this plan is the current handoff.

Incomplete. Production behavior has not changed. Added one ignored native
discovery test, `winfsp_recycle_namespace_feasibility_probe`, to the existing
WinFSP mount harness. It selects only an explicitly supplied unused drive,
creates an empty file on a fresh target-local volume, and joins shutdown. The
test is intentionally red against current production behavior.

Executed from PowerShell at the repository root:

    $env:STUDIOFS_WINFSP_TEST_MOUNT_POINT='W:'
    cargo test -p studiofs-winfsp --test mount_sanity winfsp_recycle_namespace_feasibility_probe -- --ignored --exact --nocapture

Baseline: running 1 test; 0 passed, 1 failed, 27 filtered out, 0.31s.
Creating the per-SID recycle directory failed with Windows error 3 (path not
found). No existing mount or user data was used.

Temporary prototype A made `is_windows_recycle_bin_component` return false.
This bypassed both root hiding and destructive trash classification. The same
native mount then allowed per-SID creation/open and ordinary rename, with
source=false and destination=true. Shell SendToRecycleBin returned exit 0 and
source=false, but created no recycle artifacts or Shell namespace entry. Only
the deliberately renamed `$Rprobe.txt` existed. An early probe without the
metadata assertion passed; that result proves namespace access only, not trash
acceptance. The final probe explicitly requires native `$I` metadata.

Prototype B additionally enabled persistent_acls: running 1 test; 0 passed,
1 failed, 27 filtered out, 1.36s, at the missing native recycle metadata
assertion. Prototype C additionally reported filesystem_name NTFS: same failure,
1.34s. All three production prototypes were reverted using explicit patches.
Do not treat these bounded failures as proof native integration is impossible.
Explorer UI and its recycling policy were not exercised. Further work must
trace Shell decision-making and callback traffic before choosing platform flags
or security descriptor changes. Do not retain permanent fallback deletion as
an acceptable implementation.

`cargo run -p studiofs-e2e -- list` exited 0 and listed registered scenarios
1 through 32; none specifically implements recoverable trash. No full suite
was run. `cargo fmt --all -- --check` and `git diff --check` exited 0 after
prototype reversion. The old v0.2.2 decoder and constant 5 were inspected with
`git show`; no actual old-client rejection execution or new format was added.
All shared setting, ACL view, replication/GC, conflict/recovery and user-guide
implementation remains outstanding. This change must not be delivered as a
completed SFS-1054 feature.

The earlier error 3 failure was caused by the probe's own `create_dir_all`, not
by Shell. That probe has been replaced; its transcript was overwritten by the
corrected baseline below.

### Correction: native Shell recycling works; the earlier oracle was wrong

The prototype A "no `$I` artifacts" result above is a false negative and is
superseded. That probe looked only in `$Recycle.Bin\<SID>`. StudioFS reports
file-system flags `0x00000087`, without `FILE_PERSISTENT_ACLS` (`0x8`). On such
volumes Shell uses one shared `\$RECYCLE.BIN` folder, the FAT/exFAT layout,
and never creates a per-SID folder. The probe also pre-created the folder and
renamed a file into it itself, which is not what Shell does.

The probe now runs one PowerShell oracle,
`Microsoft.VisualBasic.FileIO.FileSystem.DeleteFile(..., SendToRecycleBin)`
(`SHFileOperation` with `FOF_ALLOWUNDO`), twice. It first runs on a known-good
self-created empty file under `target/studiofs-tests` on the host NTFS volume,
then on the mount. The oracle prints drive type, FS name and flags,
volume-GUID presence and `SHQueryRecycleBin` HRESULT. It then accepts only a v2
`$I` record whose decoded original path is that exact file, in either layout,
with its `$R` payload present and listed by Shell namespace 10 (Recycle Bin).
The control removes only its own `$I`/`$R` pair. No registry or Shell setting
was read for writing or changed.

Command (PowerShell, repository root, unused `W:`):

    $env:STUDIOFS_WINFSP_TEST_MOUNT_POINT='W:'
    cargo test -p studiofs-winfsp --test mount_sanity winfsp_recycle_namespace_feasibility_probe -- --ignored --exact --nocapture

Both volumes report drive type 3 (fixed), a volume GUID (mount-manager
`\\.\W:` mount) and `SHQueryRecycleBin` S_OK. Drive type and volume
identity therefore do not block Shell.

- Control on E: (NTFS): `recycled_metadata=1 shell_listed=1 layout=per-sid`.
- Production (rebuilt, no prototype): running 1 test; 0 passed, 1 failed,
  27 filtered out, 2.29s. `DeleteFile` throws `ERROR_ALREADY_EXISTS`
  ("Cannot create a file when that file already exists"). Transcript:
  `target/sfs-1054-native-probe-baseline.log`.
- Temporary prototype A (`is_windows_recycle_bin_component` returns false,
  reverted afterwards): running 1 test; 1 passed, 0 failed, 27 filtered out,
  2.64s. Result: `recycled_metadata=1 shell_listed=1 layout=shared`.
  Transcript: `target/sfs-1054-protoA-green.log`.

Temporary tagged `eprintln!` callback tracing in `context.rs`
(`get_security_by_name`, `create`, `set_delete`, `rename`, `cleanup`,
`get_volume_info`) shows where the two runs diverge. Transcripts:
`target/sfs-1054-trace-run1.log` (production) and
`target/sfs-1054-trace-protoA.log`.

- Both runs: Shell probes `\shell-probe-empty.txt`, then issues
  `create \$RECYCLE.BIN` with directory and FILE_CREATE disposition
  (`opts=0x2200021`).
- Production: the create persists a real directory through `mkdir`, because
  `should_ignore_directory_path` does not route `$RECYCLE.BIN`. But
  `lookup_component_impl` (`namespace_ops.rs`, root
  `is_probable_trash_component` branch) returns NOT_FOUND for it on every later
  `get_security_by_name`. Shell retries the create, gets a name collision,
  runs a `set_delete true/false` deletability check and aborts. The source
  file is not deleted. The directory can be created but never opened again.
- Prototype A: Shell creates `\$RECYCLE.BIN\desktop.ini` and
  `\$RECYCLE.BIN\$I<id>.txt`, then calls
  `rename \shell-probe-empty.txt -> \$RECYCLE.BIN\$R<id>.txt`. That rename is
  the native "Move to Trash" intent signal. In production it is exactly the
  rename that `rename_inner` routes to
  `delete_source_tree_and_invalidate_trash_target`.

Conclusion: Windows native recycling on a WinFSP StudioFS drive needs no Shell
policy, drive type, label or ACL-flag change. Two VFS gates block it today:
root `$RECYCLE.BIN` hiding and destructive classification of renames into it.
Explorer UI, restore and Clear were not exercised (they need an interactive
desktop session). Restore through namespace 10 `InvokeVerb("undelete")` is
scriptable in the same oracle as the next probe step.

Recommended next production seam, behind the TTL gate (default 0 keeps
today's behavior):

1. In `lookup_component_impl` and `readdir_entries_and_attrs`, stop hiding
   root `$RECYCLE.BIN` only when the volume trash TTL is positive. Keep it
   hidden and non-creatable (route `mkdir` through the ignored-entry path)
   when TTL is 0. The current create-but-hide asymmetry is a latent defect
   either way: Shell gets `ERROR_ALREADY_EXISTS` instead of a clean
   unsupported status.
2. In `rename_inner`, when TTL is positive and the target parent chain is the
   root `$RECYCLE.BIN`, perform an ordinary retained rename. Record original
   parent inode and deletion time from the companion `$I` (or from the
   rename). Do not call `delete_source_tree_and_invalidate_trash_target`.
   TTL 0 keeps the current destructive path.
3. Leave `persistent_acls(false)`. The shared, non-per-SID layout matches the
   spec's single per-volume trash with current-ACL filtering, and avoids
   per-SID folder security that the shared Everyone descriptor cannot honour.

Other `is_probable_trash_component` callers also need the same TTL-gated
decision: the inode-under-trash check in `misc_ops.rs` and the ignored-entry
ancestor check in `ignored_entries.rs`. All temporary instrumentation and
prototype A were reverted. The tracing lines were removed by reverse-applying
their own diff, saved at `target/sfs-1054-temp-instrumentation.patch`.
`git status` then showed no change under `crates/*/src`. Then
`cargo clippy -p studiofs-winfsp --test mount_sanity -- -D warnings`,
`cargo fmt --all -- --check` and `git diff --check` all exited 0.

## Interfaces and Dependencies

Reuse MetaNamespace transactions, local redb, ordered metadata replication,
payload reference settlement and platform bridges. No dependencies added.

### Implementation continuation after corrected native proof

Carry forward prototype A success without repeating platform flag experiments.
The first production increment introduces `studiofs_meta::trash::TrashPolicy`,
a platform-neutral serializable policy with private u32 whole-day values.
`set_days` retains the last positive period; `is_expired` takes explicit Unix
seconds, so clock-controlled tests need no scheduler sleeps. The policy is a
value inside the future versioned settings record, not a mount option or a
FormatInfo field. It does not authorize deletion by itself.

Next add a versioned settings event and serving/base settings rows, wired into
the existing local journal, remote apply, publication and checkpoint export.
Trace all exhaustive MetaEvent consumers before adding its wire tag (append
after 18; do not reuse retired 16/17). Compare-and-set settings changes must
reject stale updates; positive TTL must remain inaccessible until upgrade
fencing and retained transitions are implemented. Do not expose a partial CLI.

Compatibility must cover already-mounted clients too: header rejection on
new open alone is insufficient. The old decoder fixture can prove rejection
of version 6, but the upgrade still needs an exclusive stopped-client boundary
and cannot rely on expiration of a peer lease to discard offline accepted data.
Do not bump the format or migrate data until this protocol is implemented.

2026-11-12 continuation evidence: `cargo test -p studiofs-meta --lib
trash::tests:: -- --nocapture` passed 6 tests, 0 failed, 1164 filtered out.
`cargo test -p studiofs-meta --test trash_compatibility -- --nocapture`
passed 2 tests, 0 failed. The fixture function is copied from actual v0.2.2
commit `2680630adc8f61ce39ca1fd59bdd023e8977cb62`, with the old constant
pinned to 5 and imports supplied by the harness. It executes the old version
check before decoding any new-format fields. This is source-level executable
proof, not a mounted old-binary or migration test. No format bump was made.

`cargo clippy -p studiofs-meta --lib --test trash_compatibility -- -D warnings`,
`cargo fmt --all -- --check` and `git diff --check` exited 0.

Further compatibility discovery found an existing per-publication fence in
both v0.2.2 and current `pg_engine/sql.rs`: BUMP_METADATA_VERSION updates only
WHERE metadata_format_version = the binary's expected version. In
`metadata_io.rs`, event insertion follows this update in the same transaction;
no returned version causes UnsupportedFormatVersion. Thus a future atomic
shared format upgrade can reject an already-mounted old client's subsequent
remote metadata publication. This needs an isolated PostgreSQL regression
before being counted as runtime proof. It does not prevent old offline local
writes, and an upgrade must reconcile those accepted writes explicitly rather
than resetting or discarding their caches. Existing lifecycle tests
`test_pg_event_publication_refuses_legacy_format` and
`test_pg_checkpoint_refuses_to_promote_legacy_format` provide focused patterns;
neither was executed in this continuation.

Next concrete implementation: introduce the mutable settings port with atomic
expected-revision comparison, new append-only MetaEvent tag, base/serving rows,
journal replay, PG projection/apply and checkpoint export. Keep admission
disabled until upgrade and retained-record transactions are connected. Then
connect TTL CLI and native root visibility; do not repeat the successful native
prototype or SID/ACL/FS-name experiments. The current new policy module is not
yet connected to storage, VFS or CLI and does not enable recoverable trash.

### Local persisted settings increment

`trash::VolumeSettings` now contains a monotonic `revision` and `TrashPolicy`.
The separate `MetaVolumeSettings` capability supplies `volume_settings` and
`compare_and_set_trash_days(expected_revision, days)`. Its redb implementation
in `redb_engine/volume_settings.rs` uses the existing settings table with a
distinct `volume_settings` key. One write transaction reads the expected
revision, checks overflow, writes the next revision and commits. Stale CAS
returns None with no mutation. This port is not added to the aggregate engine
and is not used by VFS/CLI yet: this is only local persistence, not completed
shared replication. No new event tag or format migration has been enabled.

Deserialize now goes through `TrashPolicyFields::try_from`: enabled days must
equal last_positive_days. Disabled policy may retain any prior positive period.
This invariant applies to serde JSON and the future postcard event payload;
event-wire validation still needs its own regression when the event is added.

Commands and evidence from this increment:

    cargo test -p studiofs-meta --lib volume_settings::tests:: -- --nocapture
    running 3 tests; 3 passed; 0 failed; 1171 filtered out; 0.03s
    cargo test -p studiofs-meta --lib trash::tests::decode_rejects_inconsistent_positive_period -- --exact --nocapture
    running 1 test; 1 passed; 0 failed; 1173 filtered out; 0.00s
    cargo clippy -p studiofs-meta --lib -- -D warnings
    exit 0

The three real-storage tests cover stale CAS plus restart, two barrier-released
concurrent writers with exactly one winner, and invalid persisted policy
rejected on read/CAS without replacing the corrupt bytes. Prior 8 focused
results remain evidence; no native probe or platform flag experiments rerun.

Next work remains the same dependent milestone, now starting from the local
port: append MetaEvent tag 19 for the settings update, append it in the same
CAS transaction, add base/serving replay with explicit stale-update outcome,
wire PG projection/apply and checkpoint import/export, and test actual replay
and checkpoint roundtrip. Inspect all exhaustive event matches before editing;
`apply_replica_aux_state_in_txn`, `apply_events_to_base_in_txn` and local replay
in `redb_engine/replication.rs` are separate paths. Do not count the current
local CAS as a replicated setting, and do not turn retention on yet.

### Integrated settings transport and online authority

The preceding next-work paragraph is now completed. MetaEvent tag 19 is
`SetVolumeSettings`; existing tags are unchanged. Local redb CAS appends it in
the same transaction as the serving row. A forced journal-sequence overflow
proves that the settings row rolls back. The three replay paths now apply
separate `volume_settings` and `base_volume_settings` rows. Older revisions are
ignored; different contents at the same revision fail explicitly. Equal replay
is idempotent. Remote exports exclude unaccepted local settings.

Global TTL edits use `MetaVolumeSettings for PgMetaEngine`, implemented in
`pg_engine/volume_settings.rs`. This control-plane operation requires online
PostgreSQL. It locks the volume's metadata_state row, checks authoritative
revision, increments it, bumps metadata batch version under the existing format
fence, stores the settings column and inserts tag 19 in one transaction, then
notifies peers. A stale expected revision returns None. Two clients observing N
cannot both publish N+1: whichever reaches the row lock first wins; the other
must reread/retry explicitly. The ordinary production projected-event publisher
rejects local settings events, so a local redb CAS cannot bypass global ordering.
The dedicated PG update does not enqueue an unapproved local proposal. No PG
operation was added to a foreground file callback.

Real bootstrap testing caught a missing link beyond the generic redb snapshot:
the thin ServingCheckpoint previously excluded settings. Checkpoint format 3
now appends the validated policy record. Decoder still reads formats 1 and 2
as disabled defaults and rejects truncated suffixes. Import replaces local
settings with checkpoint base state. A third PG client now bootstraps with the
accepted revision after checkpoint publication. This is checkpoint format 3,
not completion of the required metadata-format-6 upgrade; CURRENT_FORMAT_VERSION
is still 5 and retention remains unexposed.

Exact focused results in this continuation:

    cargo test -p studiofs-meta --lib volume_settings -- --nocapture
    7 passed, 0 failed, 1171 filtered out, 0.04s
    cargo test -p studiofs-meta --lib trash::tests:: -- --nocapture
    7 passed, 0 failed, 1172 filtered out, 0.00s
    cargo test -p studiofs-meta --lib serving_checkpoint_ -- --nocapture
    9 passed, 0 failed, 2 ignored, 1168 filtered out, 0.11s
    cargo test -p studiofs-meta --lib test_pg_volume_settings_authoritative_cas_converges_two_clients -- --ignored --nocapture
    final: 1 passed, 0 failed, 1178 filtered out, 8.03s
    cargo test -p studiofs-meta --lib test_pg_event_publication_refuses_legacy_format -- --ignored --nocapture
    1 passed, 0 failed, 1178 filtered out, 3.41s
    cargo test -p studiofs-meta --lib test_pg_checkpoint_refuses_to_promote_legacy_format -- --ignored --nocapture
    1 passed, 0 failed, 1178 filtered out, 3.28s

The PG test first passed with manually applied decoded rows, then failed when
extended to true checkpoint bootstrap (third client revision 0 instead of 2).
After the checkpoint fix it passes using `sync_metadata` and
`write_checkpoint_with_lease`, not manual peer apply. It proves opposite arrival
ordering, stale rejection, positive-to-zero last-period preservation, actual
event ordering, peer catch-up and third-client bootstrap. Existing test helpers
use configured PostgreSQL and isolated uniquely named test volumes with cleanup.
Docker pipe access was denied, but direct PostgreSQL worked; it was not a blocker.

`cargo check -p studiofs`, final `cargo clippy -p studiofs-meta --all-targets --
-D warnings`, `cargo fmt --all -- --check` and `git diff --check` passed.
No native experiment or full suite was repeated. No user-volume migration ran.

Next dependent milestone: design/implement explicit stopped-volume v5-to-v6
upgrade preserving local pending data and fencing active old publishers and
old janitors. Existing janitor inspection shows format validation during
protection snapshot loading, but this alone is not proof against an already
claimed deletion; inspect global janitor lock, claim lease and publication fence
before enabling retention. Then wire the setting capability into CLI and VFS,
add retained-entry transactions, per-platform views/ACL filtering, expiry and
restore/purge conflict/reference protection. These feature paths and bilingual
guides remain outstanding; settings transport is not the complete feature.

### Compatibility ordering and upgrade backend

The metadata format is now 6. New volume initialization therefore persists a
header which actual 0.2.2 code rejects before seeing tag 19 or checkpoint 3.
Settings CAS and replay validate a persisted header before updating rows;
checkpoint export and import reject header 5 as well. Unformatted contract
fixtures have no legacy-accepted header and retain their lightweight behavior.
The old behavior of writing tag 19/checkpoint 3 under metadata format 5 is
removed. No user volume was upgraded while testing.

`pg_engine::upgrade_shared_format5(client, volume, expected_uuid)` is the
explicit server-side transition primitive. A transaction takes the existing
global janitor advisory lock, takes metadata_state FOR UPDATE (the same row
locked by publication and claims), checks UUID/header, refuses any outstanding
reconcile_owner even after lease expiry, and atomically changes the metadata
format and serialized header. It preserves checkpoint bytes, history and all
reference rows. Lock acquisition fails promptly, SQL lock waits are limited to
2 seconds, and the operation has a 30-second deadline. Successful retry is
idempotent. New settings publication is separately fenced by format 6.

A new DB trigger rejects deletion claim acquisition on format-6 volumes unless
the transaction declares the new deletion protocol. Both existing CLI batch and
mounted single-object claims run the shared publication-fence query, which now
sets that transaction-local capability while acquiring the existing shared row
lock. Old clients use the old query and cannot claim after the transition.
Old in-flight claims cannot slip through: a claim already committed leaves an
owner and blocks upgrade; a claim transaction already holding the shared row
lock completes before upgrade obtains its exclusive lock. A later claim waits
for upgrade and then hits the trigger. The trigger does not authorize reuse of
claimed IDs or remove existing protection checks. Payload-GC schema marker is
now 2 and its probe requires the new seventh guard. Old janitors fail their
schema-version probe; already-running old janitors are additionally fenced by
the trigger. No lease expiry is treated as proof an S3 request finished.

`RedbMetaEngine::upgrade_local_format5(shared)` completes a stopped cache's
header only after the caller obtained upgraded shared authority. It compares
the full format identity after normalizing only the version, then atomically
updates that one row. It does not clear local journal, inodes, counters or
pending file state. The future CLI must own the mount lock/local database,
deploy/verify schema, run server transition, fetch the resulting shared format,
and finish the local header. If the response/local commit is lost, retry this
sequence without resetting data. Other old caches need the same explicit local
upgrade before opening; offline old file writes cannot publish into format 6
and must not be discarded by that workflow.

Focused evidence:

    legacy_header_blocks_settings_event_and_checkpoint3_before_upgrade
    1 passed, 0 failed; checks no event append before upgrade, checkpoint export
    rejection, and inode/journal equality after idempotent local header upgrade.
    cargo test -p studiofs-meta --test trash_compatibility -- --nocapture
    3 passed, 0 failed; actual old checkpoint decoder also rejects the v3 suffix.
    test_format6_upgrade_fences_old_publication_and_deletion_claims --ignored
    final: 1 passed, 0 failed, 1180 filtered out, 3.06s
    test_pg_volume_settings_authoritative_cas_converges_two_clients --ignored
    1 passed, 0 failed, 1180 filtered out, 7.72s (now metadata format 6)
    janitor_session_fences_object_deletion_claims --ignored
    1 passed, 0 failed, 1180 filtered out, 6.70s
    mounted_deletion_claim_fences_untracked_objects_against_late_replay --ignored
    1 passed, 0 failed, 1180 filtered out, 6.80s
    cargo test -p studiofs-meta --lib volume_settings -- --nocapture
    8 passed, 0 failed, 1 ignored, 1172 filtered out, 0.06s

The PG upgrade test uses a separate live connection holding the janitor lock,
then an expired old claim row, then settlement, successful upgrade/retry, failed
old version-5 publication and old raw claim SQL, and successful new-protocol
claim SQL. Synchronization is through actual SQL locks/rows, with no sleeps.
An initial test failure came from moving the PgTestVolume cleanup guard into an
async runtime; copying only ID/UUID corrected the test harness lifetime.
The local test proves namespace/journal preservation, not payload-backed mounted
restart acceptance; that still needs the complete CLI and retained lifecycle.

`cargo check -p studiofs`, `cargo clippy -p studiofs-meta --all-targets --
-D warnings`, formatting and diff checks passed. No full suite or native
prototype was repeated. The task remains unfinished: next wire the stopped
upgrade CLI using these primitives and add the authoritative retained-entry
transactions/views/ACL/GC/expiry/recovery paths. The migration backend alone is
not delivery of SFS-1054; production CLI migration is not available yet.

### Stopped-cache CLI integration (in progress)

Implement `studiofs upgrade NAME` through the existing client-root discovery
layout. Reject an active mount record, then retain the actual exclusive redb
database owner throughout schema deployment, shared upgrade, shared header fetch
and local header commit. A racing mount cannot acquire that database. Missing
databases are rejected before opening; no reinitialization or cache clearing.
The migration-only header reader accepts exactly 5/6 and validates the contract.
The command uses the existing PostgreSQL connection-task owner, schema advisory
lock, 2-second lock deadline and an overall 45-second network deadline. Errors
from remote operations are replaced with redacted retry instructions. Test the
actual child process against isolated PG rows, including exclusive-owner refusal,
server-success/local-identity-failure and repeated local completion after restart.

Deployment scope: payload_gc schema marker 2 is shared across the database.
Older janitors fail closed on still-format-5 volumes too once this schema is
deployed. Operators must deploy a compatible updated janitor for that database;
upgrading only a single volume does not isolate the janitor schema change.

Retained storage choice: use one unique metadata directory receipt per retained
namespace entry under the private shared store, with its payload linked as
`data`. A versioned neutral receipt in a reserved system xattr uses existing
journal/replay/checkpoint paths; ordinary namespace references protect payloads
and hard links. This avoids a second refcount or destructive cleaner. Capture
the original parent/name and deletion timestamp in the local rename transaction,
never from a native sidecar. Extract the existing rename transaction body so
receipt creation, payload move and record append commit together. Restore uses
the same transaction and refuses replacement. Native views and current-parent
ACL checks must mediate this private store before exposure. Shared terminal
authority/offline conflict tests remain a gate before enabling any native path;
local CAS alone does not settle a distributed purge/restore race.

### Handoff requested by user — 2026-11-12, current implementation boundary

The user requested a safe handoff to another implementer because limits are
running out. Stop feature expansion after finishing focused checks. No commits,
pushes, PRs, nested agents or external tracker writes were made. All changes
remain uncommitted on `feat/sfs-1054-recoverable-trash`; preserve the complete
working tree, including the earlier corrected native probe and compatibility
fixtures. SFS-1054 remains unfinished; this is not a feature delivery claim.

Completed in this continuation:

- `studiofs upgrade NAME` is parsed and dispatched by the real CLI. It rejects
  active mount records, verifies an existing local DB/name, owns the same redb
  exclusive database lock as mount startup throughout remote/local commits,
  deploys schema under the existing init advisory lock, runs the fenced shared
  upgrade with UUID validation, fetches strict format 6 and commits only the local
  header. Remote errors are redacted and give retry/stop/schema instructions.
  EN/UK maintenance guides document the command, cache preservation and the
  database-wide payload_gc marker-2 effect on old janitors.
- `RedbMetaEngine::open_for_format_upgrade` is an explicit existing-cache path
  accepting validated format 5/6. Normal `open` still rejects 5. Migration opening
  skips undo/index/lifecycle repair; it does not clear or replay accepted data.
  `format_info_for_upgrade` reads the header while the exclusive owner is held.
  Unsupported-format diagnostics now point to upgrade instead of reinitializing.
- Extracted the unchanged namespace rename transaction body to `rename_in_txn`.
  The original `rename_atomic` owns the transaction and delegates; 11 existing
  focused rename regressions pass after this extraction.
- `trash::RetainedEntry` v1 uses a unique receipt-directory inode, payload inode,
  actual original-parent inode/name, trusted deletion timestamp, transition
  revision 0/1 and Retained/Restored/PurgeRequested state. It stores no mount
  path or OS sidecar. Receipt directories own a `data` link; two hard links can
  have separate receipts without attaching one deletion record to the payload.
  The record is a reserved `system.studiofs.retained.v1` xattr, reusing existing
  event, replay, full snapshot and thin checkpoint transport rather than adding
  another event tag. Tag 19 remains settings; historical tag numbers are unchanged.
- `retain_namespace_entry` atomically creates the receipt, captures trusted
  identity/time, appends its record, then moves the source with no replacement.
  `restore_retained_entry` atomically records a terminal outcome and moves `data`
  without overwrite; matching local retry is idempotent. Record append precedes
  rename events in the journal, so publication can fence the receipt first even
  if journal windows split. A failed collision rolls everything back.
- `request_retained_purge` ONLY records an intent. It keeps the physical `data`
  link and cannot authorize GC. Expiry reads the shared policy in the same redb
  transaction; deterministic tests cover last-positive TTL after disabling.
  This is deliberately not physical Clear/expiry settlement yet.
- `MetaRetainedStore` is an optional capability on the existing MetaEngine,
  implemented by redb and PG (using the existing `mutate` pending-state lock and
  after-commit tracking). ReadOnlyMetaEngine forwards reads and denies every
  mutation/settings CAS. Engines without this capability retain the default None.
- PG `trash_entry_authority` is a small per-volume receipt projection. The actual
  publication transaction, after the format-fenced metadata row lock, validates
  each reserved record and admits revision 0 or one terminal revision 1. Equal
  identical records are idempotent; changed immutable identity or conflicting
  terminal state rejects the WHOLE transaction. It never discards a local journal.
  This projection survives checkpoint pruning as a shared SQL row; import/load
  seeding and crash/retry boundaries still need completion below.

Exact latest focused evidence (no full suites):

    cargo test -p studiofs --test snapshot_cli_test upgrade_cli_restarts_after_shared_success_and_preserves_local_journal -- --ignored --nocapture
    final-tree rerun: 1 passed, 0 failed, 1 filtered out, 3.92s
    cargo test -p studiofs --bin studiofs upgrade_requires_exactly_one_volume -- --nocapture
    1 passed, 0 failed, 543 filtered out, 0.00s
    cargo test -p studiofs-meta --lib test_rename -- --nocapture
    11 passed, 0 failed, 1178 filtered out, 0.05s
    cargo test -p studiofs-meta --lib retained_entry_ -- --nocapture
    7 passed, 0 failed, 1182 filtered out, 0.07s
    cargo test -p studiofs-meta --lib test_pg_retained_restore_purge_conflict_preserves_local_loser -- --ignored --nocapture
    1 passed, 0 failed, 1188 filtered out, 13.34s

The CLI process test owns isolated PG rows and target tempdirs. It refuses while
another redb owner exists, verifies no server upgrade then, forces server success
with local identity mismatch, verifies failed local completion leaves header 5
and journal intact, corrects only the test identity, invokes the real command
twice across process restarts and checks format 6 plus accepted inode/journal.
It does not prove payload-backed mounted dirty-file restart or a mounted old binary.
An initial failed CLI run exposed normal open rejecting 5 before migration; the
dedicated validated upgrade opener fixed it. An initial replay fixture expected
an unpublished empty regular-file child in a remote snapshot; that contradicted
existing projection. The transport test now uses a nested directory tree;
local unpublished file restart is separately exercised by the restore test.

The PG conflict test uses TWO real clients with background polling disabled,
the production local retained methods and production event publication. Both
accept opposite local terminal transitions from the same receipt, then publish
purge-first and restore-first in separate isolated cases. It verifies one shared
winner, failed loser's version/event transaction rollback, unchanged accepted
local restored directory/child/journal and the purge request's still-owned data
link. `sync_metadata` only pulls; explicit existing `flush_due_pending` is used
to publish initial state. No timing sleep is used as synchronization proof.
This is NOT a proof of later peer-apply/restart recovery or physical remote GC
with an accepted offline regular-file restore; do not infer those guarantees.

Concrete next work (continue implementation, do not repeat passed prototypes):

1. Extend the SAME PG conflict regression through losing-client foreign apply
   and restart, then cover accepted regular-file payload bytes and remote GC.
   Current conflict handling leaves a pending local journal and an actionable
   error; automatic recovery/republishing of a conflicting accepted restore is
   not implemented. A remote-only body cannot be assumed safe just because its
   local namespace was restored: determine durable local materialization/pinning
   and existing publication/recovery handling BEFORE enabling restore callbacks.
2. Complete authority projection initialization for dump/load/checkpoint-seeded
   volumes and response-loss/self-replay tests. The SQL row currently fences
   normal publication; initial record replay after a terminal shared record is
   conservatively rejected, not claimed exactly-once. Evaluate actual ack/pull
   retry ordering before relaxing this. Keep terminal receipts/authority tombstones
   until delayed clients cannot resurrect them. Equal revisions with different
   contents must never silently overwrite either base or accepted local state.
3. Implement physical purge/expiry settlement only after shared authority ack,
   through existing namespace deletion/reference/open-handle/snapshot/unpublished
   protocols. No cleaner has been added. PurgeRequested still owns and charges
   its payload; nothing currently reclaims it. Preserve live hard links.
4. Connect VFS private store and native views over receipts. Block reserved xattr
   mutation and raw private-store traversal, including all lookup/readdir/mkdir/
   ignored/classifier paths. Sidecar drafts remain local until the trusted source
   rename binds them. Synthesize Windows $I with this client's mount path from
   original-parent identity; do not publish raw W: paths. Windows uses the proven
   shared root layout with existing volume flags. Add current original-parent ACL
   list/restore/purge checks, target RW, missing/trashed-parent ancestry filtering
   and identity-following on parent moves. No admin role or identity framework.
5. Add requested init --trash-days and online post-creation trash-days command
   using shared CAS; validate negative/fractional/overflow inputs. No TTL product
   CLI or enabled native production path is present yet. Default remains zero.
6. Finish EN/UK trash guides with actual guarantees/trust boundary. Rebuild and
   extend the corrected native oracle on owned disposable mount data to enabled
   Move/list/restore/Clear, disabled/default and direct-delete bypass. Do not
   repeat SID/ACL/NTFS flag experiments or touch global host trash. Explorer UI,
   Linux/macOS and physical-GC/offline payload acceptance remain unproved.

No infrastructure blocker was found: isolated configured PostgreSQL tests work.
The user explicitly requested this handoff; do not label ordinary remaining
implementation as an environmental blocker or mark SFS-1054 complete.

Final handoff checks: `cargo clippy -p studiofs-meta -p studiofs --all-targets
-- -D warnings` passed (29.90s); affected-package `cargo check -p studiofs`
and `cargo check -p studiofs-meta` passed during this continuation. Final Rust
formatting and `git diff --check` passed. No product native run was counted in
this continuation, and no earlier green prototype or unchanged policy suite
was repeated. The final CLI rerun rebuilt the current product dependency graph.

### Continuation evidence — foreign apply and both-client restart

The same `test_pg_retained_restore_purge_conflict_preserves_local_loser` now
pulls the winner into both clients, verifies both accepted local terminal
receipts, closes both clients and reopens each cache with polling disabled.
It checks the restored tree/child, both retained local journals, and the payload
namespace link on the purge client. When restore wins, that link is correctly
at `restored-offline`, not receipt `data`. An intermediate test incorrectly
required `data` in both orders; correcting that oracle produced the final green.

Final focused command: `cargo test -p studiofs-meta --lib
test_pg_retained_restore_purge_conflict_preserves_local_loser -- --ignored
--nocapture`: 1 passed, 0 failed, 1188 filtered, 18.80s. Final affected
`cargo clippy -p studiofs-meta --all-targets -- -D warnings`, `cargo fmt --all`
and `git diff --check` passed. No product implementation changed in this
continuation; this strengthens the existing directory-only safety evidence.

The first remaining task is still regular-file byte protection and automatic
recovery/publication after losing shared authority. Directory restart does not
prove durable local materialization of remote-only bodies or remote GC safety.
All six remaining implementation areas in the preceding handoff remain open,
except that basic directory foreign-apply/both-client restart is now covered.
Do not enable native production retention or physical purge on this evidence.
No commits, external writes, subagents, native mount runs or full suites.

Revision 2026-11-12: Added the both-client restart regression and exact evidence;
kept payload and product delivery gates open because no byte-backed recovery or
native production behavior was implemented here.

### Trash-days CLI (handoff item 5) — separate continuation

Another implementer was concurrently extending item 1 in
`pg_engine/tests/lifecycle.rs` and this plan; this continuation deliberately
did not touch `pg_engine/retained.rs` or the conflict tests.

- The authoritative online CAS body moved unchanged into
  `pg_engine::set_shared_trash_days(client, volume, expected_revision, days,
  client_id, now_ms)`. `expected_revision: None` sets under the existing
  `metadata_state FOR UPDATE` lock; `Some` keeps stale-CAS semantics.
  `MetaVolumeSettings for PgMetaEngine` delegates to it, so mounted engines and
  the CLI share one format-fenced, ordered tag-19 path. No local redb is opened.
- `studiofs trash-days NAME [DAYS]` (new `cli/trash.rs`) shows or sets the
  shared value. It works while the volume is mounted: it reuses
  `snapshot::mounted_format_info` (now `pub(super)`) and otherwise reads the
  header read-only, checks name and shared UUID, and has a 30 s deadline. DAYS
  is clap `u32`: negative, fractional, suffixed and >u32::MAX values fail before
  any write. Output reports the last positive period when disabled.
- `studiofs init --trash-days N` (default 0) calls the same function with
  `Some(0)` after metadata_state is seeded. If that step fails the volume stays
  disabled and the error says to rerun `studiofs trash-days`.
- Release gate: this CLI only stores the shared setting. Until the VFS native
  path is wired, a positive value does NOT make Move to Trash recoverable (the
  destructive rename path is unchanged). Do not ship or document the command
  to users before item 4 lands; EN/UK guides were intentionally not changed.

Evidence (no full suites):

    cargo test -p studiofs --bin studiofs trash_days_accepts_only_whole_nonnegative_days
    1 passed, 0 failed, 544 filtered out (also covers init --trash-days parsing)
    cargo test -p studiofs --test snapshot_cli_test trash_days_cli_updates_shared_setting_and_client_converges -- --ignored --nocapture
    1 passed, 0 failed, 2 filtered out, 4.81s
    cargo test -p studiofs-meta --lib test_pg_volume_settings_authoritative_cas_converges_two_clients -- --ignored --nocapture
    1 passed, 0 failed, 1188 filtered out, 7.85s (refactor regression)
    cargo clippy -p studiofs-meta -p studiofs --all-targets -- -D warnings: exit 0

The process test runs the real binary against isolated PG rows: show default,
three invalid inputs with no write, enable 7, disable 0 (revision 2, last
positive 7, no DSN in output), then opens a real client on the local cache and
`sync_metadata` replays both events to the same settings. Its first run failed
(`tokio::time::timeout` built outside the runtime) and was fixed before green.
`init --trash-days` end-to-end was not run: init requires a reachable S3 bucket.

### User-requested stop and handoff — retained publication retry

SFS-1054 remains UNFINISHED. The user explicitly requested stopping at this
logical checkpoint for another agent. Preserve all existing uncommitted work,
including the separately completed TTL CLI and `cli/snapshot.rs` visibility
change. This continuation made no commits, pushes, external writes, reviews,
subagents or background tasks. Work remains on
`feat/sfs-1054-recoverable-trash`; verify HEAD/status before resuming because
this is a shared checkout. The original feature starting commit recorded above
is `0358242f3c1e90959faf66e0be0cff95f424b47c`.

Production changes made in this continuation:

- `crates/studiofs-meta/src/pg_engine/retained.rs` adds
  `publication_digest(events)`: only projected transactions containing the
  reserved retained receipt xattr get a domain-separated SHA-256 digest of
  length-framed encoded events. Other publications retain their previous path.
- `crates/studiofs-meta/src/pg_engine/schema.rs` adds
  `trash_publication_receipts(vol,digest,version)` with volume cascade and a
  32-byte digest constraint. Keep these retry receipts durable; no pruning was
  added. Existing schema deployment must run before using this new path.
- `crates/studiofs-meta/src/pg_engine/metadata_io.rs` computes that digest before
  splitting the transaction. Inside the PG transaction it locks metadata_state,
  checks format compatibility and consults the durable publication receipt.
  An exact retry returns the original publication version plus current
  checkpoint freshness, without bumping version or replaying namespace events.
  First publication inserts its receipt in the SAME transaction as authority,
  event log and object lifetime changes. No callback or local journal behavior
  changed; shared terminal conflicts remain rejected.
- `crates/studiofs-meta/src/pg_engine/tests/lifecycle.rs` adds
  `test_pg_retained_publication_response_loss_retry`. It publishes a single
  admission+restore packet, retries immediately and after reopen, then publishes
  deletion and verifies the stale exact retry does not advance remote head.
  A changed immutable identity is rejected with no remote version change.

The initial response-loss regression was red: replaying admission after its
terminal authority row returned `conflicting shared transition`. Do NOT solve
this by ignoring admission conflicts or replaying namespace changes: doing so
could resurrect data after later deletion. The durable transaction receipt
skips the entire already-committed publication instead. An initial exploratory
retry combined packets that had originally committed separately; that was not
an exact response-loss retry and was removed before the final regression.

Exact final behavior verification:

    cargo test -p studiofs-meta --lib test_pg_retained_ -- --ignored --nocapture
    3 passed, 0 failed, 1187 filtered out, 19.79s

This filter ran the new response-loss test, the existing both-order retained
restore/purge foreign-apply/restart test, and the existing unpublished-draft
rename test. The new test alone also passed before extending its negative
assertions: 1 passed, 1189 filtered, 4.77s. Final
`cargo clippy -p studiofs-meta --all-targets -- -D warnings` passed (3.90s),
`cargo fmt --all` and `git -c core.safecrlf=false diff --check` passed. An
intermediate Clippy failure in the new test (`collapsible_if`) was fixed with
a let-chain; it changed no behavior after the final test run. No full suites,
native mount, S3 or CLI reruns were performed in this continuation.

Important boundary: this is EXACT projected-packet retry idempotence, not
automatic recovery of a conflicting accepted restore. A changed pending window
or changed packet is not deduplicated. Projection seeding from dump/load and
checkpoints remains open. Receipt-table lifecycle/schema integrity and concurrent
response-loss fault injection have not received separate acceptance tests.

Next actionable task remains the user-requested byte-backed conflict recovery.
There is no external infrastructure blocker. Source tracing established:
cache `runtime.rs` pins are process-local (the existing test explicitly proves
they do not persist across reopen); `snapshot_reachability.rs` refuses publication
of object IDs once deletion has started, requiring new IDs. Therefore local
namespace/journal preservation alone does not protect a remote-only file body.
Trace VFS read/write/fsync and upload recovery before accepting restore: capture
durable local bytes before committing an accepted restore, retain them across
restart/cache eviction, and republish new object IDs if shared purge won. Do not
weaken receipt authority to retry old references. Add the byte-backed regression
RED first, covering peer purge/GC, losing restore publication, foreign apply,
reopen and successful recovered publication. No such byte regression or durable
restore-body implementation was added here; do not infer this guarantee from
the green directory or response-loss tests.

After that, finish authority projection seeding, acknowledged physical
purge/expiry, then VFS private store/native views/current-parent ACL integration.
Preserve the completed TTL CLI; its positive setting still does not enable safe
production retention. Keep EN/UK TTL guides gated as recorded in its handoff.

Revision 2026-11-12: Recorded exact production retry semantics, focused red/green
evidence and the unresolved byte-safety boundary at the user's requested stop.

### Byte-backed conflicting restore — 2026-11-12 continuation

Start state: branch `feat/sfs-1054-recoverable-trash`, the full uncommitted
tree from earlier implementers preserved (no reset/stash/checkout). The
cancelled previous implementer left no edits newer than this plan; the 405-line
`lifecycle.rs` diff was the earlier recorded retained tests.

Trace findings (source-verified):

- A rejected PG publication (`flush_pending_claimed_batch_with_restore`) restores
  the whole claimed batch. With a losing terminal receipt record in the window,
  every later local event was blocked forever (RED: second flush still failed).
- An existing per-event seam handles this class: `reject_local_event_ranges`
  (used for oversized events). Rejected ranges are skipped by selection but stay
  durable-cursor barriers in `flushed_event_seq_for_progress`; the rejection is
  process-local, so after reopen the first flush re-derives it from shared state.
- Regular-file bytes are slices in meta with `PayloadRefState`; `LocalOnly` is
  `PinnedUnpublished` (metadata-derived, survives restart), unlike the
  process-local cache runtime pins. New writes get new object IDs, so rewriting a
  file's bytes through the ordinary VFS write path + `fsync` captures durable
  local bytes under IDs that `snapshot_reachability` will accept.

Production changes:

- `pg_engine/retained.rs`: `PgMetaEngine::losing_retained_terminal_records`
  reads `trash_entry_authority` for local terminal records in the failed window
  and returns those whose shared row is a different terminal outcome.
- `pg_engine/replication_flush.rs`: on a generic publication error, reject only
  those records' local sequences (warn log, no DSN), then return the error as
  before. Classification failure rejects nothing (fail closed, retried later).
  Shared authority is never overwritten; admission conflicts are untouched.
- `studiofs-vfs/src/vfs/io_ops.rs`: `StudioVfs::restore_retained_entry(store,
  receipt, parent, name)` reads a regular file's whole content through the read
  path, rewrites it through the write path (new `LocalOnly` slices, new object
  IDs), `fsync`s, then commits the meta restore. It takes the retained store as
  a parameter rather than widening `VfsMeta`. No callback is wired to it yet;
  no remote work happens in an FS callback.

Tests:

- RED then GREEN: `test_pg_retained_restore_purge_conflict_preserves_local_loser`
  now drives the loser's real `flush_due_pending` (first attempt errors, second
  publishes), checks the peer sees `restored-offline` in BOTH orders and the
  shared receipt row is unchanged, and after reopen both journals drain. The old
  oracle (purge-winner keeps `receipt/data`) was replaced: the accepted restore
  move now publishes, so the receipt holds no data link in either order.
- RED then GREEN: `retained_restore_losing_to_peer_purge_keeps_bytes_and_republishes_new_objects`
  (`studiofs-vfs/tests/vfs_test.rs`, two PG-backed VFS clients over live S3,
  background polls disabled). Retain a published 3 MiB file, B's purge wins,
  A restores, old objects are deleted from S3 (emulated GC), A's journal drains,
  A reopens and reads exact bytes, B observes the restored name with object IDs
  disjoint from the old ones and reads the exact bytes. RED (meta-only restore):
  B kept the old deleted object IDs ("peer must observe the restore republished
  under new object IDs" failed after 15 s).

Evidence (final tree, no full suites):

    cargo test -p studiofs-meta --lib test_pg_retained_ -- --ignored --nocapture
    3 passed, 0 failed, 1187 filtered out, 25.23s
    cargo test -p studiofs-vfs --test vfs_test retained_restore_losing_to_peer_purge -- --ignored --nocapture
    1 passed, 0 failed, 190 filtered out, 18.20s
    cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs --all-targets -- -D warnings: exit 0
    cargo fmt --all -- --check and git -c core.safecrlf=false diff --check: clean

Open boundaries (do not infer these from the green tests):

- Physical purge settlement (item 3) does not exist. If it ever removes a
  receipt's `data` link/graph shared BEFORE a losing offline restore publishes
  its move, that move would hit a missing source. Settlement must account for
  this (e.g. republish as a new inode from the captured local bytes); the
  emulated GC here only deleted S3 objects, not shared namespace or upload rows.
- Restoring a directory does not localize its descendants' bytes yet (marked
  `ponytail:` in `restore_retained_entry`); the read is chunked but O(file).
- Superseded by "Independent review fixes" below: a losing terminal record is
  now retired (completed), not rejected, so it is no longer a cursor barrier.
- Restore rewrite copies into new inodes and preserves the source atime/mtime of the restored file (ctime is not preserved).
- Authority projection seeding (handoff item 2) was not started in this run.

Revision 2026-11-12: recorded byte-backed conflict recovery, its RED/GREEN
evidence and remaining settlement/directory boundaries.

### Independent review fixes — 2026-11-12

Start: branch `feat/sfs-1054-recoverable-trash`, shared uncommitted tree
preserved (no reset/stash/checkout/revert).

1. HIGH, fixed. Confirmed `reject_local_event_ranges` made the loser a cursor
   barrier (`local_state.rs` `flushed_event_seq_for_progress`). RED: the
   extended `test_pg_retained_restore_purge_conflict_preserves_local_loser`
   failed after reopen with "conflicting shared transition". Fix:
   `replication_flush.rs` retires losers with `complete_local_event_ranges`;
   `flush_pending_claimed_batch` reads from `max(claim start, durable cursor)`,
   because `restore_pending_batch` merged the old claim start back below a
   cursor the retirement had advanced; `remote_apply.rs`
   `matching_local_prefix` skips an omitted local terminal receipt record
   (`retained::is_terminal_retained_record`). The PG test now asserts that
   post-reopen flushes leave `metadata_state.version` and `event_log` row count
   unchanged, the cursor reaches `NEXT_EVENT_SEQ`, and the journal prunes to
   empty. Its restart window (published remainder, no self-ack yet) is not
   reachable through that test (flush replays its own batch in-process), so
   `self_ack_after_restart_skips_only_a_retired_terminal_receipt_record` covers
   it as a unit test, RED with the tolerance disabled. A losing purge that is the
   whole batch now retires on the first attempt (`Ok`); the test asserts per order.
2. HIGH, fixed (refusal). No inode lock may span the awaited copy
   (`lock_order.rs`: synchronous guards must not cross an await). The copy is
   refused with `ResourceBusy` when `nlink > 1` or `open_count > 0`. `ponytail:`
   residual: a handle opened through the private receipt path during the copy
   is unordered; add a per-inode copy gate before wiring native trash.
3. MEDIUM, fixed. `restore_retained_entry` now waits for deferred entries on
   `receipt/data` and the destination, then calls
   `namespace_mutations().record_rename`, `bump_acl_namespace_epoch`,
   `invalidate_kernel_entries` and `request_eager_flush`, which is the
   `rename_inner` post-commit path (`wait_for_deferred_namespace_entry` is now
   `pub(super)`). The new VFS test caught a stale negative dentry without it.
4. MEDIUM, fixed per the decision above. The copy is skipped for a non-`Retained`
   receipt or an occupied destination (the store reports the error). The restore
   still preserves the source atime/mtime on the copied file (ctime is not preserved).
5. MEDIUM, recorded as a spec obligation: purge settlement must re-verify
   `receipt/data` still links to the inode and never delete by inode alone.
6. LOW, fixed with item 1 (post-reopen flush results are asserted).
7. LOW, fixed. Classification failure logs `warn!` with the volume id and the
   rendered `MetaError` (no DSN), then keeps the journal pending.

Evidence (no full or workspace suites; DSN/S3 loaded from `.env`, not printed):

    cargo test -p studiofs-meta --lib test_pg_retained_ -- --ignored --nocapture
    3 passed, 0 failed, 1187 filtered out
    cargo test -p studiofs-meta --lib pg_engine::
    468 passed, 0 failed, 167 ignored
    cargo test -p studiofs-meta --lib self_ack_after_restart
    1 passed (RED with the tolerance disabled)
    cargo test -p studiofs-vfs --test vfs_test retained_restore_
    1 passed (retained_restore_copy_refuses_concurrent_writers_and_skips_occupied_destination), 1 ignored
    cargo test -p studiofs-vfs --test vfs_test retained_restore_ -- --ignored --nocapture
    1 passed (live PG + S3, 17.98s; DSN present, so no early return)

### Second-round review fixes — 2026-11-12

Start: branch `feat/sfs-1054-recoverable-trash`, shared uncommitted tree
preserved (no reset/stash/checkout/revert).

1. MEDIUM, fixed. Shape-only omission let a rejected (oversized) or unclassified
   terminal record be skipped after a restart. Chosen proof: base replay, not a
   new redb table. `apply_retained_authority` is the only writer of
   `trash_entry_authority` and commits the winner with its own publication, and
   base replay applies foreign `SetXattr` to `BASE_XATTRS_TABLE`
   (`redb_engine/replication.rs`), so the winner is in base before any later
   self batch. `retained::lost_terminal_records_in_txn` marks a local terminal
   record lost only when base holds a different terminal record;
   `prepare_self_batch_in_txn` passes those indices to
   `matching_local_prefix_omitting`. A proven loser can never be in a successful
   self batch (the authority rejects it), so it is excluded from `visible` and
   never compared. `matching_local_prefix` itself omits nothing again. A persisted
   loser-seq table was rejected: it adds a redb table that snapshot/state
   replacement and prune would all have to own. RED:
   `self_ack_after_restart_keeps_an_unproven_terminal_receipt_record` returned
   `Some(2)` instead of `None`.
2. MEDIUM, fixed. `write` enters the per-inode `FileSessionOperation::Write` gate
   (`io_ops.rs`), so `enter_op` cannot span the copy. `StudioVfs` now has
   `retained_restore_gate` (a `tokio::sync::Mutex<()>`, like
   `landing_zone_recovery_gate`), held across check, copy and commit; no sync
   guard crosses an await. `open_count` is re-checked before the store commit.
   `ponytail:` one gate for all receipts. RED (gate disabled, 3/3 runs):
   `retained_restore_concurrent_same_receipt_never_recopies_over_newer_writes`
   read back `orig` instead of the newer `next`.
3. LOW, fixed. Restore now runs `enforce_access(parent, ReadWrite)`,
   `validate_name`, `reject_runtime_stats_entry_mutation`, the ignored/trash
   target rejection (`reject_nontransactional_no_replace`, now `pub(super)`
   with the stats check) and `enforce_no_known_foreign_hard_lock`. Only
   `EntryNotFound` counts as a free destination; other lookup errors return.
4. LOW, fixed by item 1: the comparison is back to the original single
   `visible.len() == batch.len()` position with a zipped compare, O(M) per
   candidate as before the relaxation.
5. LOW, recorded as a spec obligation: purge settlement must keep the
   `trash_entry_authority` row and the base winner while delayed clients may
   hold local records.

Evidence (no full or workspace suites; DSN/S3 loaded from `.env`, not printed,
DSN presence checked before each ignored run):

    cargo test -p studiofs-meta --lib self_ack_after_restart
    2 passed (new negative test RED first: Some(2) != None)
    cargo test -p studiofs-meta --lib pg_engine::
    469 passed, 0 failed, 167 ignored
    cargo test -p studiofs-meta --lib test_pg_retained_ -- --ignored --nocapture
    3 passed, 0 failed
    cargo test -p studiofs-vfs --test vfs_test retained_restore_
    2 passed, 1 ignored
    cargo test -p studiofs-vfs --test vfs_test retained_restore_ -- --ignored --nocapture
    1 passed (live PG + S3)
    cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs --all-targets -- -D warnings
    clean
    rustfmt --edition 2024 <touched files>; cargo fmt --all -- --check; git diff --check
    clean

Residual: the base proof fails closed. If a later shared event removes the
winner's base receipt xattr before a delayed client restarts, its loser stays
pending (no data loss, but a stuck journal); the spec obligation forbids that.

### Third-round review fixes — 2026-11-12

Start: branch `feat/sfs-1054-recoverable-trash`, shared uncommitted tree
preserved (no reset/stash/checkout/revert).

1. MEDIUM, fixed. Confirmed: no layer protected `system.studiofs.retained.v1`,
   and `apply_retained_authority` checked only `SetXattr`. RED: a local rmdir of
   the emptied receipt after restore published `RemoveAttr { receipt }` (base
   replay of it deletes the receipt's base xattrs, i.e. the loser proof).
   (a) `RedbMetaEngine::set_xattr`/`remove_xattr` (`redb_engine/xattr.rs`), the
   single sink for VFS, FUSE, macFUSE, WinFSP and `PgMetaEngine::mutate`, reject
   the reserved name with `MetaError::AccessDenied` (EACCES /
   STATUS_ACCESS_DENIED through the existing mapping). Retained-store
   transactions use `set_xattr_in_txn` and are unaffected. This also blocks a
   forged receipt record on an ordinary inode. (b) `apply_retained_authority`
   now ends with `reject_receipt_removal`: a `RemoveXattr` of the reserved name,
   or a `RemoveAttr`/`RenameEntry.displaced_remove_attr_ino` naming an inode with
   a `trash_entry_authority` row (rows admitted earlier in the same batch
   count), fails the publication with `MetaError::Storage` and rolls back the
   transaction, like a conflicting terminal transition. Unlike a classified
   loser it is not retired, so such a batch would stall the client's journal.
   (Superseded by the fourth round:) the local redb guard
   `reject_receipt_removal_in_txn` now refuses, with `MetaError::AccessDenied`
   and no journal event, every rmdir/unlink of an inode carrying the receipt
   record and every rename displacing one. A receipt removal therefore never
   enters a local journal, and (b) remains only as defense in depth against a
   forged or legacy batch.
   Traversal: no production code creates or exposes a receipt store yet
   (`retain_namespace_entry` takes a caller-chosen store inode and has no VFS
   caller). Still required before wiring (handoff item 4): hide the private
   store from lookup/readdir/mkdir/rename-into; removal of a receipt itself is
   already refused locally.
2. MEDIUM, fixed. `StudioVfs::restore_retained_entry` now enforces ReadWrite on
   the receipt (rename source parent) and on the receipt's `original_parent`
   before any copy; a missing original parent fails as `InodeNotFound`. RED:
   `retained_restore_denies_without_source_or_original_parent_write` restored
   through a read-only original parent.
3. LOW, fixed. The race test moved in-crate
   (`vfs::tests::retained_restore_concurrent_same_receipt_never_recopies_over_newer_writes`)
   to use a `#[cfg(test)]` `retained_restore_hook` with stages `Entered`
   (before the gate) and `CopyRead` (between a copy chunk's read and write).
   The first copy parks until the second restore has also read its copy (no
   gate) or has entered while the gate is held; the second copy parks until the
   writer has written `next` after the first commit. Waits are a `watch`
   condition bounded by 10 s; no sleeps. RED with the gate line removed: 3/3
   runs failed at the `next` assertion (stale `orig` written back); green with
   the gate restored.
4. LOW, fixed. `lock_order.rs` module doc records `retained_restore_gate` as the
   outermost async lock of a restore, outside `LockTier`/`LockOrderGuard`.

Evidence (no full or workspace suites; DSN/S3 loaded from `.env`, not printed,
DSN presence checked before each ignored run):

    cargo test -p studiofs-meta --lib retained_receipt_xattr_rejects
    RED first (Ok(()) for a forged set), then 1 passed
    cargo test -p studiofs-meta --lib test_pg_retained_authority_rejects -- --ignored --nocapture
    RED first (receipt RemoveAttr published), then passed
    cargo test -p studiofs-meta --lib self_ack_after_restart
    2 passed
    cargo test -p studiofs-meta --lib retained
    13 passed, 6 ignored
    cargo test -p studiofs-meta --lib test_pg_retained_ -- --ignored --nocapture
    4 passed, 0 failed
    cargo test -p studiofs-vfs --lib retained_restore_
    1 passed (RED 3/3 with the gate removed)
    cargo test -p studiofs-vfs --test vfs_test retained_
    3 passed, 1 ignored (ACL test RED first; the VFS xattr test was written
    after the meta guard, whose RED is the meta test above)
    cargo test -p studiofs-vfs --test vfs_test retained_restore_ -- --ignored --nocapture
    1 passed (live PG + S3, 17.13 s)
    cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs --all-targets -- -D warnings
    clean
    rustfmt --edition 2024 <touched files>; cargo fmt --all -- --check; git diff --check
    clean

### Authority projection seeding (handoff item 2) — paused at user request

Start: branch `feat/sfs-1054-recoverable-trash`. The shared uncommitted tree
was preserved (no reset, stash, checkout or revert). The user asked to pause at
the nearest boundary. Everything below compiles and the focused tests are green.

Path findings (read in source):

- Full remote snapshots and thin serving checkpoints did NOT carry base-only
  xattrs. `export_snapshot_from_read_txn_with_check` (`redb_engine/snapshots.rs`)
  merges base + overlay - tombstones for xattrs, and
  `export_remote_snapshot_from_read_txn_with_check` (`replication.rs`) feeds
  both snapshot kinds. A checkpoint cut requires head == local version and a
  fully flushed journal (`checkpoint_coordinator.rs`
  `checkpoint_source_cut_is_eligible`, `checkpoint_source_event_seq`). A retired
  loser counts as flushed, but PG foreign apply keeps its overlay. So a loser's
  checkpoint published its losing record (RED below). Base, by contrast, holds
  the winner at that cut. Import writes `BASE_XATTRS_TABLE` directly, so a
  bootstrapped client's base keeps whatever the checkpoint carries.
- `studiofs load` (`cli/dump_load.rs` `apply_dump`) replaced metadata_state,
  counters and event_log but never touched `trash_entry_authority` or
  `trash_publication_receipts`. A fresh target had an empty projection. That
  rejected legitimate restores ("no shared admission") and would admit a
  delayed revision-0 replay over a terminal record. An existing target kept
  stale rows, and a stale digest could turn a new publication into a skipped
  "retry". The dump carries no authority rows; the checkpoint snapshot is
  inline in metadata_state, so it can be decoded without S3.
- Format upgrade 5->6 (`pg_engine/format_upgrade.rs`) only rewrites the header.
  Format 5 cannot publish real receipts. An old client could still have set the
  reserved name as an ordinary xattr on an existing inode, because Linux does
  not permission-check `system.*` (unverified for WinFSP/macFUSE). Such a
  forged record has no authority row: no transition can publish, and a
  `RemoveAttr` of that inode is not fenced. This is not fixed. Obligation for
  item 4: native views must enumerate receipts from the private store, never by
  scanning for the xattr name.
- Snapshot v2 has create/list/inspect/delete/policy but no restore or clone
  into shared metadata. `init` and the janitor test seed only empty
  metadata_state. Neither can carry receipts.
- Sibling gap, NOT fixed: load also does not set `metadata_state.volume_settings`
  (the trash-days CAS row read by `set_shared_trash_days`). After a load, that
  CAS starts from the default while checkpoint/events carry the real settings.

Changes:

- `redb_engine/retained.rs`: `project_shared_receipt_records(txn, xattrs)`
  replaces a receipt xattr's exported value with base's when base holds a
  terminal record. Called in `export_remote_snapshot_from_read_txn_with_check`
  (`replication.rs`), so full snapshots and serving checkpoints share it. An
  undecodable base record is treated as non-terminal, like
  `lost_terminal_records_in_txn`.
- `pg_engine/retained.rs`: `pub async fn seed_retained_authority(tx, volume,
  checkpoint, checkpoint_version, events)` inserts checkpoint receipt records
  (validated by `decode_retained_entry`). It then groups post-cut log rows by
  batch_version and calls `apply_retained_authority` per batch. It is
  re-exported from `pg_engine.rs`.
- `cli/dump_load.rs`: `TRASH_AUTHORITY_TABLES` guard plus a new
  `DumpLoadError::TargetHasTrashAuthorityState`. `apply_dump` seeds before
  commit, so any error rolls back the whole load. The `test_dump` fixture now
  uses a real empty serving checkpoint (`vec![0; 6]`), a real encoded
  `set_xattr` event, and format 6. The format-5 fixture and the `expected: 5`
  assertion were already stale at HEAD: 15 `dump_load` unit tests failed with
  `UnsupportedFormatVersion { found: 5, expected: 6 }` before this fix.

Tests (DSN loaded from `.env`, not printed; presence checked before each run):

    cargo test -p studiofs-meta --lib retained_checkpoint_exports
    RED first (thin checkpoint had Restored, expected PurgeRequested), then pass
    cargo test -p studiofs-meta --lib retained
    14 passed, 0 failed, 7 ignored
    cargo test -p studiofs-meta --lib test_pg_retained_seeded -- --ignored --nocapture
    1 passed (seed from checkpoint at head == seed from pure log replay ==
    published projection; terminal-without-admission seed rolls back;
    conflicting purge and delayed admission replay rejected via the real
    publication path with head unchanged; restore of a seeded Retained receipt
    publishes). Not run RED: the function did not exist before.
    cargo test -p studiofs-meta --lib test_pg_retained_ -- --ignored --nocapture
    5 passed, 0 failed
    cargo test -p studiofs-meta --lib self_ack_after_restart
    2 passed
    cargo test -p studiofs --bin studiofs load_seeds_trash -- --ignored --nocapture
    RED with the seed call disabled ("load must seed the receipt authority"),
    then 1 passed (also asserts a reload is refused with
    TargetHasTrashAuthorityState naming trash_entry_authority)
    cargo test -p studiofs --bin studiofs dump_load
    22 passed, 0 failed, 2 ignored
    cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs --all-targets -- -D warnings
    clean
    rustfmt --edition 2024 <touched files>; cargo fmt --all -- --check;
    git -c core.safecrlf=false diff --check
    clean

Open / failing, not investigated:
`cargo test -p studiofs --bin studiofs dump_load -- --ignored`: 1 passed,
1 failed. `dump_snapshot_preserves_tail_pruned_after_metadata_read` panics with
"dump did not reach its counters query: Elapsed". Its `apply_dump` (with the
new seeding, including a post-cut event) succeeded; the timeout is in the
untouched `load_remote_dump` lock-wait phase. Whether it failed before this
work is not established.

Round-3 compatibility: load seeding writes SQL rows only. Checkpoint import and
base replay write xattr tables directly (`import_snapshot_with_mode`,
`set_xattr_value_in_table_in_txn`), so the reserved-name guard in
`RedbMetaEngine::set_xattr`/`remove_xattr` does not block them, by design.
Seeded batches pass through `reject_receipt_removal`, like publication.

Next concrete steps:
1. Diagnose the `dump_snapshot_preserves_tail_pruned_after_metadata_read`
   timeout (first check whether it fails with HEAD's `dump_load.rs`).
2. Decide on the `volume_settings` column for load: seed it from the
   checkpoint plus tag-19 tail, or refuse.
3. Add a checkpoint-bootstrapped (`initialize_discovered_postgres_checkpoint`)
   PG regression, so a delayed loser restarting after bootstrap keeps the
   winner proof in base. Only the redb export/import level is covered now.
4. Response-loss of the load transaction itself is safe to retry only if the
   target was empty: a committed-but-unacknowledged load is then refused by
   the new guard. Document this in the maintenance guide, which is gated.


### Seeding completion (handoff item 2, steps 1–3) — 2026-11-12

Start: branch `feat/sfs-1054-recoverable-trash`. The shared uncommitted tree
was preserved (no reset, stash, checkout or revert). One temporary RED edit
(step 3) was made and restored in place. Diagnostics are under
`target/sfs1054-diag/`.

1. `dump_snapshot_preserves_tail_pruned_after_metadata_read` timeout. Caused by
   this branch, test-only; the product path is unchanged.
   - It passes alone (`cargo test -p studiofs --bin studiofs
     dump_snapshot_preserves_tail -- --ignored`: 1 passed). It fails only
     together with `load_seeds_trash_authority_and_rejects_existing_trash_rows`.
     That test is new on this branch: HEAD's `dump_load.rs` has exactly one
     `#[ignore]` PG test, so HEAD cannot hit this.
   - Root cause: a cross-resource deadlock that only the test's 10 s timeout
     breaks. `runtime_bridge::block_on` outside a Tokio context serializes on
     the process-wide `CLI_RUNTIME: Mutex<Runtime>`. The tail test holds
     `LOCK TABLE counters IN ACCESS EXCLUSIVE MODE` and waits for its loader
     thread. The loader waits on the runtime mutex. That mutex is held by the
     other test's `apply_dump`, which waits in PG on `DELETE FROM counters`.
     `pg_locks` sampled mid-hang showed exactly this: the `DELETE FROM
     counters` backend blocked by the `AccessExclusiveLock` holder, and no
     loader lock wait.
   - Fix: a module-local `PG_TEST_SERIAL` mutex taken first by both PG tests in
     `cli/dump_load.rs` tests. No sleeps or retries.
   - Residual, not fixed (pre-existing test design): the tail test's wait polls
     `pg_locks` DB-wide. It can therefore be satisfied by another process's
     loader. Two overlapping `cargo test` processes against the same PG made it
     fail once with `captured.metadata_state_checkpoint_version` 101 != 100;
     the sequential rerun is green. Run PG suites one process at a time.
2. Load seeds `metadata_state.volume_settings`. `seed_retained_authority`
   (`pg_engine/retained.rs`) folds the checkpoint's `volume_settings` with every
   post-cut `SetVolumeSettings` it already decodes, using the new
   `VolumeSettings::is_superseded_by` (`trash.rs`; redb
   `apply_volume_settings_in_txn` now uses it too). It then writes the column
   and requires exactly one `metadata_state` row. Any error rolls back the
   whole load. RED first: `load_seeds_trash_authority_and_rejects_existing_trash_rows`
   (left: None, right: revision 1 / 7 days). That test now also loads a second
   volume whose tail disables trash (revision 2, days 0, last positive 7), and
   then proves `set_shared_trash_days(Some(2), 3)` is accepted at revision 3.
   `test_pg_retained_seeded_authority_matches_publication` clears the column
   before each reseed and asserts both the checkpoint and the pure-log seeds
   reproduce the published settings.
3. Checkpoint-bootstrapped client regression, in
   `test_pg_retained_restore_purge_conflict_preserves_local_loser` (both
   orders, after the reopen phase). The LOSER exports its serving checkpoint at
   the shared head. A fresh DB goes through the production
   `initialize_discovered_postgres_checkpoint` (as `cli/discovery.rs` does) and
   is opened as `PgMetaEngine`. Asserts: it holds the shared winner record and
   the published restore namespace. After `try_complete_catchup_sync` it is
   Live. Its own `create_dir` publishes exactly one batch (`head + 1`), with no
   receipt `SetXattr`, and self-acks with the cursor at next_seq. The shared row
   is unchanged and the loser replays without republishing. RED: with
   `project_shared_receipt_records` disabled in `replication.rs`, the cold
   client held the loser's `Restored` record (expected `PurgeRequested`). The
   line was restored and the test is green.
   Note: `background_poll: false` means the test calls `try_complete_catchup_sync`
   itself; without it, `create_dir` fails with "allocator namespace is
   unavailable", because discovery leaves the fresh-client gate set.

Evidence (DSN presence checked in `.env`, never printed; runs sequential):

    cargo test -p studiofs --bin studiofs dump_load -- --ignored
    before fix: 1 passed, 1 failed (reproduced twice); after: 2 passed
    cargo test -p studiofs --bin studiofs load_seeds_trash -- --ignored
    RED (volume_settings None), then green inside the run below
    cargo test -p studiofs --bin studiofs dump_load
    22 passed, 0 failed, 2 ignored
    cargo test -p studiofs --bin studiofs dump_load -- --ignored
    2 passed, 0 failed
    cargo test -p studiofs-meta --lib retained
    14 passed, 0 failed, 7 ignored
    cargo test -p studiofs-meta --lib volume_settings
    8 passed, 0 failed, 1 ignored
    cargo test -p studiofs-meta --lib self_ack_after_restart
    2 passed
    cargo test -p studiofs-meta --lib test_pg_retained_restore_purge_conflict -- --ignored --nocapture
    RED with projection disabled, then 1 passed
    cargo test -p studiofs-meta --lib test_pg_retained_ -- --ignored --nocapture
    5 passed, 0 failed
    cargo test -p studiofs-meta --lib test_pg_volume_settings -- --ignored --nocapture
    1 passed, 0 failed
    cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs --all-targets -- -D warnings
    clean
    rustfmt --edition 2024 <touched files>; cargo fmt --all -- --check;
    git -c core.safecrlf=false diff --check
    clean

Still open from the paused list: step 4 (document load response-loss retry
semantics in the maintenance guide) is gated with the guides. Physical
purge/expiry settlement, native views/ACL wiring and guides were not started.

### Fourth-round review fixes — 2026-11-12

Start: branch `feat/sfs-1054-recoverable-trash`, shared uncommitted tree
preserved (no reset/stash/checkout/revert). Temporary RED edits were restored
in place.

1. MEDIUM, fixed. Confirmed: neither `NamespaceDeleteTxn::execute` nor
   `rename_in_txn` (`redb_engine/fs_ops.rs`) checked for a receipt, so a local
   rmdir of an emptied receipt, or a rename displacing one, was accepted and
   its `RemoveAttr` would then be rejected forever by PG
   `reject_receipt_removal`. New `RedbMetaEngine::reject_receipt_removal_in_txn`
   (`redb_engine/retained.rs`) refuses with `MetaError::AccessDenied` (same
   shape as the reserved-xattr guard) when the target inode holds a merged
   receipt record. It is called in `NamespaceDeleteTxn::execute` (unlink,
   unlink_open_handle, rmdir) and in `rename_in_txn` for a displaced target:
   the redb sinks under VFS, FUSE, macFUSE, WinFSP and `PgMetaEngine::mutate`.
   `retain_namespace_entry`/`restore_retained_entry` rename with
   `replace_if_exists=false` and `request_retained_purge` renames nothing, so
   they are unaffected. RED: `retained_receipt_rejects_rmdir_and_displacing_rename`
   (`rmdir` returned `Ok(())`). PG `reject_receipt_removal` is kept.
2. LOW, fixed. `test_pg_retained_authority_rejects_receipt_removal` now
   asserts the local rmdir is `AccessDenied`, then publishes forged batches and
   requires the `receipt cannot be removed from shared metadata` message for:
   `RemoveXattr`, `RemoveAttr`, a directory `RenameEntry` over the empty
   receipt (`displaced_remove_attr_ino`), and one batch of retain → restore →
   forged `RemoveAttr` (row admitted earlier in the same batch; asserted rolled
   back). RED: with the `RenameEntry` arm made unmatchable, the test failed.
3. LOW, fixed. `project_shared_receipt_records` now also scans
   `XATTR_TOMBSTONES_TABLE`: a tombstoned receipt key absent from the merged
   list whose base holds a terminal record is emitted with that record, and the
   list is re-sorted. RED: `retained_snapshot_exports_shared_winner_over_overlay_receipt_tombstone`
   (imported record `None` with the new branch disabled).
4. LOW, fixed (broader than reported). Confirmed `InodeNotFound(original_parent)`
   on a retry after the original parent was removed; additionally, even without
   that check the retry would fail at `lookup(receipt, "data")`, because a
   completed restore moved the payload out. `StudioVfs::restore_retained_entry`
   now checks `original_parent` only while `Retained`, and sends a terminal
   receipt straight to the store (idempotent for an identical retry, refused
   otherwise) without namespace side effects. Receipt and destination ACL
   checks are kept. RED: `retained_restore_retry_is_idempotent_after_original_parent_removed`.

Evidence (DSN presence checked in `.env`, never printed; PG runs sequential):

    cargo test -p studiofs-meta --lib retained_receipt_rejects_rmdir
    RED (Ok(())), then green
    cargo test -p studiofs-meta --lib retained_snapshot_exports_shared_winner
    RED (None) with the tombstone branch disabled, then 1 passed
    cargo test -p studiofs-meta --lib retained
    16 passed, 0 failed, 7 ignored
    cargo test -p studiofs-meta --lib self_ack_after_restart
    2 passed
    cargo test -p studiofs-meta --lib rename
    76 passed, 0 failed, 6 ignored
    cargo test -p studiofs-meta --lib rmdir
    13 passed, 0 failed
    cargo test -p studiofs-meta --lib test_pg_retained_ -- --ignored --nocapture --test-threads=1
    5 passed, 0 failed (rename-arm RED run: 1 failed, then restored)
    cargo test -p studiofs-vfs --lib retained_restore_
    1 passed
    cargo test -p studiofs-vfs --test vfs_test retained_
    4 passed, 1 ignored (new retry test RED first: InodeNotFound(3))
    cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs --all-targets -- -D warnings
    clean
    rustfmt --edition 2024 <touched files>; cargo fmt --all -- --check;
    git -c core.safecrlf=false diff --check
    clean

Not started (unchanged scope): physical purge/expiry settlement, native
views/ACL wiring, guides. Purge settlement will need its own in-transaction
path past `reject_receipt_removal_in_txn` and PG `reject_receipt_removal`.

### Physical purge and expiry settlement (handoff item 3) — 2026-11-12

Start: branch `feat/sfs-1054-recoverable-trash`; the shared uncommitted tree
was preserved (no reset, stash, checkout or revert). Temporary RED edits were
made from backups under `target/sfs1054-settle/` and restored byte-identical
(`cmp`).

Traced deletion path (source-verified):

1. `NamespaceDeleteTxn::execute` (`redb_engine/fs_ops.rs`) runs
   `reject_receipt_removal_in_txn` on the target, then `execute_unlink` /
   `execute_rmdir`. An unlink to nlink 0 removes the inode overlay and file
   graph (or, for unpublished content, records `deleted_before_publish` and
   queues uploaded overlay objects locally); nlink > 0 only rewrites the attr.
   Events: `RemoveEntry` (+ `RemoveAttr` / `RemoveRegularFileGraph`).
2. `PgMetaEngine::mutate` marks the journal pending; `flush_due_pending`
   publishes it. In the publication transaction (`metadata_io.rs`)
   `apply_retained_authority` (+ `reject_receipt_removal`) runs, then
   `apply_object_lifetime_changes` (`snapshot_reachability.rs`): affected inodes
   lose their `snapshot_current_references`, an object whose global count drops
   to zero gets `end_version` on `snapshot_object_reachability` and, if its
   upload row is `published`, a `snapshot_payload_gc_candidates` row.
3. The janitor (`pg_engine/janitor.rs`) claims candidates (`delete_pending`,
   fenced), excludes current references and `SNAPSHOT_PIN_QUERY` pins (a
   `creating`/`ready` snapshot whose cut lies inside the reachability interval),
   waits `--skip-newer-minutes`, deletes, and settles the claim.

Changes:

- `redb_engine/retained.rs`: `settle_retained_purge(receipt, is_open)` returns
  `RetainedSettlement::{Settled, NotAcknowledged, NothingToSettle, Busy}`. One
  redb transaction: the base record must be `PurgeRequested`; `receipt/data`
  must still bind `entry.inode` (else nothing to settle); the tree is collected
  post-order with the (now `pub(super)`) unfiltered
  `merged_children_in_write_txn`; any open inode defers; each entry is deleted
  by `NamespaceDeleteTxn` with `with_expected_ino` (a changed binding aborts as
  `Busy`). The receipt directory, its record and the PG row stay.
  `request_retained_purge_at` is now `pub(crate)` (controllable clock).
- `pg_engine/retained.rs`: `pub fn run_retained_lifecycle_once(now)` returns
  `RetainedLifecycleSweep { expired, settled, deferred }` (re-exported). It
  skips everything when `existing_entry_days() == 0` (no receipt can exist).
  Candidates come from `trash_entry_authority` (never from scanning xattrs, so a
  forged format-5 record is never a candidate). A shared `Retained` row whose
  local record is `Retained` and `is_expired` becomes a local
  `PurgeRequested(Expired)` through `mutate`, published and conflict-checked
  exactly like a manual purge (identical concurrent expiries are idempotent in
  `apply_retained_authority`). A shared `PurgeRequested` row calls
  `settle_retained_purge` with the open-handle registry.
- `pg_engine/lifecycle.rs`: the poll loop calls it at most every
  `RETAINED_LIFECYCLE_INTERVAL` (60 s), only in `Live`, and warns on error.
- Open handles: `MetaReplication::note_open_handle(ino, open)` (default no-op;
  forwarded in `studiofs-vfs/src/runtime.rs`) and the PG field
  `open_handle_inodes`. The VFS calls it next to both
  `file_session.note_open_handle` sites in `vfs.rs` `open_file` and in
  `sync_ops.rs` release. `active_open_inodes` is unchanged (write overlays only).

Crash and idempotence: every step re-derives its state from the shared row,
base and the local namespace. Ack, then a crash before the unlink: the next
sweep settles. Unlink, then a crash before publication: the pending journal
publishes once, and the next sweep finds no `data` link. Tested below: each
`data` `RemoveEntry` is in the shared log exactly once, and extra flushes do
not move the head.

Fourth-round self-check:

- redb `reject_receipt_removal_in_txn`: settlement's targets are the payload
  tree, never a receipt, so the guard passes without a bypass (the unit test
  settles through it). If a private store were ever placed inside a trashed
  tree, the guard would refuse and the sweep would keep failing closed. Item 4
  must keep the store outside user trees.
- PG `reject_receipt_removal`: the settlement batch carries `RemoveEntry`
  (receipt, data) and `RemoveAttr` of payload inodes. No receipt has those
  inodes; publication succeeded and all 3 authority rows remained (PG test).
- `project_shared_receipt_records`: settlement writes no receipt xattr or
  tombstone, so checkpoint projection is unaffected (not separately tested).
- VFS `restore_retained_entry` terminal retry: on the settling client the
  merged record is `PurgeRequested`, so the store refuses restore with no
  namespace side effect. On a restoring loser the `data` link moved, so
  settlement there is `NothingToSettle` (asserted in the expiry test). A
  restore whose eager copy started before the winner replayed can have its
  inode unlinked mid-copy; the restore then fails, consistent with the winner.

Evidence (DSN and S3 settings loaded from `.env`, never printed; PG runs
sequential with `--test-threads=1`):

    cargo test -p studiofs-meta --lib retained_settlement
    3 passed; RED per guard: base-ack check -> 2 failed; data->inode check ->
    1 failed; open check -> 1 failed (all restored)
    cargo test -p studiofs-meta --lib retained
    19 passed, 0 failed, 9 ignored
    cargo test -p studiofs-meta --lib self_ack_after_restart
    2 passed
    cargo test -p studiofs-meta --lib test_pg_retained_ -- --ignored --nocapture --test-threads=1
    7 passed, 0 failed, 92.80 s (includes the two new tests below)
    test_pg_retained_purge_settlement_reclaims_through_gc_queue: RED with the
    open registry ignored ((3, 0) != (2, 1))
    test_pg_retained_expiry_conflicts_with_offline_restore_like_purge: RED with
    the TTL check skipped (redb refused: "retained entry has not expired")
    cargo test -p studiofs-vfs --test vfs_test retained_
    4 passed, 2 ignored
    cargo test -p studiofs-vfs --test vfs_test retained_ -- --ignored --nocapture --test-threads=1
    2 passed (live PG + S3), including
    retained_purge_settlement_defers_while_a_read_only_handle_is_open: RED with
    the open-path registration removed ((1, 0) != (0, 1))
    cargo test -p studiofs-vfs --lib retained_restore_
    1 passed
    cargo test -p studiofs-vfs --lib active_open
    3 passed
    cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs --all-targets -- -D warnings
    clean
    rustfmt --edition 2024 <touched files>; cargo fmt --all -- --check;
    git -c core.safecrlf=false diff --check
    clean

What the PG settlement test proves: nothing settles before publication. After
the ack, two of three settle and an open handle defers the third; the open
inode keeps its link and graph; a restart settles it. Each `data` link is
removed once. 3 authority rows and 3 receipt directories remain.
`snapshot_payload_gc_candidates` holds exactly the two unreferenced objects:
the live hard link keeps its object out, and its deletion claim is
`Protected`. A `ready` snapshot cut pins the purged object (`Protected`) until
the snapshot moves to `deleting` (`Claimed`). The expiry test covers the TTL
boundary (`deleted_at + 7 d - 1` vs `+ 7 d`) with an explicit clock, no
settlement before the ack, both publication orders, settlement only when
expiry wins, and the restoring client never settling.

Open (do NOT infer these from the green tests):

1. HIGH, open. A losing offline DIRECTORY restore that publishes after the
   winner settled loses its descendants on peers. Observed in the expiry-first
   case: the loser republishes `Ok`, the peer gets `restored-offline`, but
   `accepted-child` is `EntryNotFound`; the restoring client keeps everything
   locally. Settlement cannot bound how late an offline loser publishes, so the
   fix belongs to the loser: when its terminal record loses, republish the
   restored tree as new inodes (directories and children), the directory
   analogue of the regular-file byte capture. A regular-file restore through
   the VFS path republishes `SetAttr` and new slices before its rename, which
   base replay applies even with a missing source dentry, so it is expected to
   converge; that order (peer settles before the loser publishes) is not tested.
2. The background loop wiring (about 15 lines) is not exercised by a test;
   tests call `run_retained_lifecycle_once` directly with `background_poll: false`.
3. Peer-client open handles are not consulted: to peers, settlement is an
   ordinary unlink (existing foreign-unlink semantics).
4. Two clients settling the same receipt concurrently both publish a
   `RemoveEntry`; the janitor lock is not taken. Not tested.
5. `ponytail:` one redb transaction per receipt tree; every sweep reads all
   authority rows (tombstones are kept forever); the open-handle snapshot is
   taken before the transaction, so a handle opened through the private receipt
   path in between is unordered (item 4 blocks that path).
6. Settling a file that another client is still uploading follows the existing
   peer-unlink-during-upload protocol; not separately tested here.

Next: open item 1 before enabling native retention, then handoff item 4 (VFS
private store, native views, ACL), whose wiring must also keep the private
store outside any trashable tree.

Revision 2026-11-12: Implemented acknowledged physical purge and expiry
settlement through the existing deletion lifecycle, with RED/GREEN evidence,
and recorded the losing-directory-restore boundary it exposes.

### Handoff — losing directory restore after settlement (open item 1) — stopped at user request, 2026-11-12

Start: branch `feat/sfs-1054-recoverable-trash`; `git status --short` showed
57 entries (45 modified, 12 untracked), all preserved. This run made NO source
or test edits. It only added this section. No tests were run, so there is no
new RED or GREEN evidence.

Event-level root cause. It is derived from source, NOT yet reproduced by an
asserted test:

1. Loser A: `RedbMetaEngine::restore_retained_entry`
   (`redb_engine/retained.rs`) journals `SetXattr` (the `Restored` record),
   then `RenameEntry { parent: receipt, name: "data", new_parent, new_name,
   entry.ino = payload.ino, moved_dir_attr: Some(..) }`. It keeps the inode
   identity.
2. A's first flush fails in `apply_retained_authority`. The flush error path
   (`replication_flush.rs`, about line 1260) retires only the losing
   `SetXattr` through `complete_local_event_ranges`. The `RenameEntry`
   stays pending.
3. Winner B: `settle_retained_purge` publishes post-order `RemoveEntry` and
   `RemoveAttr` for the whole `receipt/data` tree, including
   `RemoveEntry(payload, accepted-child)` + `RemoveAttr(child)` and
   `RemoveEntry(receipt, data)` + `RemoveAttr(payload)`.
4. A republishes the `RenameEntry`, and PG accepts it: the only PG check is on
   receipt rows, and `payload.ino` is not a receipt. Base apply
   (`apply_events_to_base_in_txn`, `RenameEntry` arm in
   `redb_engine/replication.rs`, about line 1513) does this: the source
   dentry no longer matches, and `base_dentry_keys_for_rename_entry` finds no
   prior key. It still runs `set_base_dentry(new_parent, new_name, entry)` and
   re-inserts `moved_dir_attr`. Result: peers RESURRECT the purged inode
   `payload.ino` as an empty directory. Its children were removed by B's
   settlement, and nothing republishes them. A's own view may also lose them
   once its overlay retires after self-ack. Not checked; assert it in the RED
   test.
5. Regular-file analogue (also from source): the VFS copy in
   `StudioVfs::restore_retained_entry` (`studiofs-vfs/src/vfs/io_ops.rs`)
   rewrites the SAME inode. The republished `SetAttr` + new slices +
   `RenameEntry` therefore re-insert `payload.ino` on peers. The bytes
   probably converge (new object IDs), but the purged identity is
   resurrected. That violates "never resurrect the purged inode identity", so
   the file path needs the same fix.

Proposed design. Not implemented; the next agent confirms it and records it
in the Decision Log: an EAGER copy into new identities, with no rename of the
receipt payload ever.

- VFS `restore_retained_entry` (background-only, as today) builds a full copy
  under a unique stage name inside the receipt, for example
  `receipt/restore-<stage-root-ino>`. Unique per attempt, so no other client
  ever touches it. The copy uses the existing ops: mkdir, create + write in
  `FOREGROUND_READ_WINDOW_BYTES` windows (new LocalOnly object IDs), symlink,
  and `link` for in-tree hard links (an old->new map only for `nlink > 1`).
  Walk with an explicit stack, one directory listing at a time. ponytail:
  memory is O(tree size); revisit if large trees require streaming state.
  Keep the open-handle `ResourceBusy` refusal for any inode in
  the tree. The `nlink > 1` refusal is no longer needed, because the old inode
  keeps its other links.
- The meta `restore_retained_entry(receipt, parent, name, stage)` runs as one
  txn. It writes the `Restored` record, renames stage -> destination (a NEW
  inode), then unlinks the `receipt/data` tree with the post-order
  `NamespaceDeleteTxn` loop factored out of `settle_retained_purge`. Journal
  order matters: record, rename, unlink. If the record wins, the unlink is
  authorized by it. If it loses, the unlink matches the purge winner, and
  B's settlement becomes `NothingToSettle` when A publishes first. Either
  way, peers converge to A's tree and no settled inode is renamed.
- Why eager rather than lazy: GC can delete remote-only bodies after
  settlement and its grace window, and meta cannot copy bytes. A lazy
  re-identification at proven loss would still need an eager local byte
  capture and a rewrite of the pending journal. Cost: every restore copies the
  tree's bytes. Today that applies to single files only.
- Known limits to record as `ponytail:`. A stage abandoned by a crash
  mid-copy leaks inside the private receipt. Settlement must NOT remove
  stages: a stage removed by a peer and then renamed by A would recreate this
  same bug. Xattrs are not copied unless the next agent adds them. Restore
  changes the inode number.
- Blast radius: every caller of `restore_retained_entry` changes (trait
  `trash.rs:139`, `read_only.rs:71`, `pg_engine/volume_settings.rs:18`, the
  redb unit tests in `retained.rs`, PG tests in `pg_engine/tests/lifecycle.rs`
  at lines 36/129/211/453/594/730/814, `studiofs/src/cli/dump_load.rs:1411`,
  `vfs/tests.rs:28891/28949`, and `vfs_test.rs` around lines 4561–4975).
  Meta-only tests must stage a copy with `create_dir`/`mknod` under the
  receipt. Assertions that expect `restored == payload.ino` must change to
  "a new inode with the same tree". Restore-vs-restore then keeps both copies
  instead of the second rename stealing the first name.

Next concrete steps, in order:

1. RED: in `test_pg_retained_expiry_conflicts_with_offline_restore_like_purge`
   (`pg_engine/tests/lifecycle.rs`, `expiry_first` branch), replace the
   `eprintln!` observation with a deterministic order:
   `publish_and_pull(&second)` (settlement published), `first.sync_metadata()`,
   `first` flush, `second.sync_metadata()`. Then assert that both clients see
   `restored-offline/accepted-child`, and that `second.get_attr(payload.ino)`
   is `InodeNotFound` (no resurrection). Add a VFS-level regular-file twin to
   `vfs_test.rs` (`retained_` prefix, live PG + S3, ignored). Run each test
   and record the actual failure.
2. Implement the design above and turn the tests GREEN.
3. Duplicate `RemoveEntry`/`RemoveAttr`, from two settlers or from restore
   unlink + settlement. Test convergence: no error, no journal stall, and
   object counts in `apply_object_lifetime_changes` not decremented twice.
   If either fails, make the second one a no-op.
4. Poll-loop gating test for `run_retained_lifecycle_once`
   (`pg_engine/lifecycle.rs`): `Live` state + the 60 s
   `RETAINED_LIFECYCLE_INTERVAL` with a controlled clock (extract the gate as
   a pure function if there is no clock seam). No sleeps.
5. Run the validation set from the task, then clippy, rustfmt and diff-check.
   Update this section with the exact counts.

Commands this run: `cargo fmt --all -- --check` and
`git -c core.safecrlf=false diff --check` (results below). Nothing else was
run. Both exited 0 (clean).

### Losing directory & file restore after settlement (steps 1–2) — completed, 2026-11-13

Root cause confirmed by RED tests before implementation edits:

- `cargo test -p studiofs-meta --lib pg_engine::tests::lifecycle::test_pg_retained_expiry_conflicts_with_offline_restore_like_purge -- --ignored --exact`:
  **FAILED (RED)** in the `expiry_first` branch after `publish_and_pull(&second)`
  -> `first.sync_metadata()` -> `first` flush -> `second.sync_metadata()`.
  Panicked at `first.lookup(first_restored.ino, b"accepted-child")` with
  `EntryNotFound { parent: 549755813888, name: "accepted-child" }` — once
  settlement replayed and `first`'s overlay retired on self-ack, even `first`
  lost its children and `payload.ino` became an empty resurrected directory.
- `cargo test -p studiofs-vfs --test vfs_test retained_restore_losing_to_settled_peer_purge_never_resurrects_inode -- --ignored --exact`:
  **FAILED (RED)** with `assertion left != right failed: restored file must use
  a new inode instead of resurrecting the settled inode (left: 549755813889,
  right: 549755813889)`.

Implementation (Decision Log 2026-11-13):

- `crates/studiofs-meta/src/trash.rs`, `read_only.rs`,
  `pg_engine/volume_settings.rs`: added `stage: &[u8]` to
  `MetaRetainedStore::restore_retained_entry`.
- `crates/studiofs-meta/src/redb_engine/retained.rs`: factored
  `collect_subtree_entries_in_txn` and `delete_subtree_postorder_in_txn` out of
  `settle_retained_purge` (keeping settlement untouched for stages) and updated
  `RedbMetaEngine::restore_retained_entry` to check terminal idempotency,
  `receipt/data` binding, destination collision, and valid non-`data` new-inode
  `stage`, then atomically write `Restored`, rename `stage` -> `(parent, name)`,
  and unlink `receipt/data` post-order in one txn. Updated unit tests and added
  stage preservation assertion in `settle_retained_purge`.
- `crates/studiofs-meta/src/pg_engine/tests/lifecycle.rs` and
  `crates/studiofs/src/cli/dump_load.rs`: updated callers to stage copies and
  assert new-inode convergence (`second.get_attr(payload.ino)` returns
  `InodeNotFound`).
- `crates/studiofs-vfs/src/vfs/io_ops.rs`, `namespace.rs`, `namespace_ops.rs`:
  updated `StudioVfs::restore_retained_entry` to eagerly copy files, symlinks,
  and directory trees (explicit stack, single directory listing per pop,
  in-tree hard-link map for `nlink > 1`, `FOREGROUND_READ_WINDOW_BYTES` windows,
  no `nlink > 1` refusal, open-handle `ResourceBusy` preserved across the tree)
  into `receipt/restore-<id>`, clean up failed stages, atomically commit via
  `store.restore_retained_entry`, record `record_retained_restore` + kernel
  invalidations, and retire deleted unlinked inodes (`InodeNotFound`) from runtime
  upload/cache state. Recorded the three `ponytail:` limits (abandoned stage
  leaks, xattrs not copied, inode number changes).
- `crates/studiofs-vfs/src/vfs/tests.rs` and `tests/vfs_test.rs`: updated
  existing tests and added local test
  `retained_restore_copies_directory_tree_symlinks_hardlinks_and_refuses_open_child`.

GREEN test results:

- `cargo test -p studiofs-meta --lib retained`: 19 passed, 0 failed (9 ignored)
- `cargo test -p studiofs-meta --lib pg_engine::tests::lifecycle::test_pg_retained_ -- --ignored --test-threads=1`: 6 passed, 0 failed
- `cargo test -p studiofs-vfs --lib retained_restore_`: 1 passed, 0 failed
- `cargo test -p studiofs-vfs --test vfs_test retained_`: 5 passed, 0 failed (3 ignored)
- `cargo test -p studiofs-vfs --test vfs_test retained_ -- --ignored --test-threads=1`: 3 passed, 0 failed
- `cargo test -p studiofs --bin studiofs cli::dump_load::tests::load_seeds_trash_authority_and_rejects_existing_trash_rows -- --ignored --exact`: 1 passed, 0 failed

Open next steps from the prior handoff remain open (not in this task's scope):
step 3 (duplicate `RemoveEntry`/`RemoveAttr` convergence) and step 4
(poll-loop gating test).


### Handoff — Opus review of eager-copy restore (stopped at user request, 2026-11-13)

Gemini 4 Hydrogen (medium) did handoff steps 1 and 2. RED reproduced the bug
in both the PG lifecycle test and the VFS file test; both are now GREEN.
An independent Opus 5.5 (medium) review found no HIGH issues. No fixes have
been applied yet. Resume by fixing these, in this order:

1. MEDIUM — `io_ops.rs:601-640`. The open-handle re-check runs before the
   meta commit and is racy. A write through an already-copied old inode
   during the copy is lost. Old-tree cleanup bypasses
   `mark_orphan_if_open`, the Release gate and the deferred
   materialization wait (compare rename-overwrite at
   `namespace_ops.rs:4053-4075`). Fix:
   - pass an `is_open` check into the meta txn, as `settle_retained_purge`
     does;
   - fence each source's `(ctime, size)` and return `TargetChanged` if it
     changed;
   - route removed inodes through `mark_orphan_if_open`.
2. MEDIUM — an abandoned `receipt/restore-N` stage leaks forever, including
   its published S3 bytes and quota. Before each copy, sweep only stages
   this client created (client id in the stage name, or a local redb row).
   Never remove a peer's stage.
3. MEDIUM — the copy needs RW on the private receipt as the mount
   principal, but the contract lists only original-parent and destination
   permissions. Decide now, and record it in the Decision Log, how item 4
   hides the store: path-blocking, or internal ops that bypass the ACL.
4. LOW-MEDIUM — `io_ops.rs:740`: the `unlinked_files.iter().any` scan is
   O(n²) and redundant. Memory is O(tree), not bounded: fix the plan's
   claim and add a `ponytail:` note.
5. LOW — `redb_engine/retained.rs:422`: meta checks only that the stage
   root is a new inode. Also refuse any stage subtree inode that is in
   `doomed`.
6. LOW — old dirs and symlinks get no `retire_os_cache_inode`,
   directory-generation bump or dentry-cache invalidation.
7. LOW — timestamps are not preserved (no `setattr` for times). Fix it, or
   list it in the `ponytail:` comment and the spec.
8. LOW — doc drift:
   - spec lines 72-76 still say a losing restore moves `receipt.inode`;
   - the old `nlink > 1` Decision Log entry is superseded but not marked.
9. LOW — test gaps:
   - no test for the post-copy open re-check;
   - no test for stage cleanup after a failed commit (destination appears
     during the copy; use the `CopyRead` hook);
   - the PG test should also assert, on the loser:
     `first.get_attr(payload.ino)` is `InodeNotFound`, `pending_ops == 0`,
     and the old object IDs are in `snapshot_payload_gc_candidates`.

Rejected by the review, with evidence in the review thread:
- the PG `reject_receipt_removal` guard does not block a winning restore;
- duplicate RemoveEntry/RemoveAttr cannot double-decrement GC counts,
  because the ledger diffs current references;
- settlement never touches stages;
- stage names are unique (per-client inode allocator).

Known pre-existing class, not fixed here: an offline unlink of an inode that
still has an outside hard link publishes an absolute `SetAttr`, which can
overwrite a peer's newer attributes.

Still open from the earlier handoff:
- step 4: the poll-loop gating test;
- the full review of the settlement step with the fourth-round fixes;
- items 4 and 6 (native views and private store; guides and the native
  oracle).

### Handoff — review fixes paused, 2026-11-13

Recorded the private-store path-blocking decision, marked the old hard-link
refusal decision superseded, corrected the spec's losing-restore description,
and corrected the O(tree size) memory claim. No source or test changes were
made in this run. No tests were run; prior test results above are historical.
`cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs --all-targets -- -D warnings`,
`cargo fmt --all -- --check`, and `git -c core.safecrlf=false diff --check`
each exited 0. Review items 1, 2, 5, 6, 7 and 9 remain open; item 4
still needs its code change. Next: add the deterministic CopyRead race test,
then implement transactional source fencing and the normal unlink lifecycle.

### Handoff — eager-copy review items 1, 4 and 5 — completed, 2026-11-13

Implemented transactional retained-restore source fencing and normal runtime
retirement. `MetaRetainedStore::restore_retained_entry` now accepts each
copied inode's `(ctime, size)` fence and an `is_open` callback. The Redb write
transaction rejects a changed or open source with `MetaError::TargetChanged`
before writing the terminal receipt, verifies no stage-subtree inode overlaps
the doomed `receipt/data` tree, then commits restore atomically. The VFS refreshes
source attrs from local metadata before recording fences, passes its open-handle
check into that transaction, and cleans its stage on refusal. After commit it
retires removed files, symlinks, and directories through the rename-overwrite
runtime cleanup path, including the orphan marker, release gate, deferred
materialization wait, OS-cache retirement, and directory/dentry invalidation.
The directory walk reuses its `seen` set rather than scanning accumulated file
records. Copy metadata remains O(tree) memory; this known limit is marked by a
`ponytail:` comment.

RED then GREEN:

- `cargo test -p studiofs-vfs --lib retained_restore_rejects_source_changed_during_copy -- --nocapture`:
  RED 1 failed because restore committed after a controlled source write during
  `CopyRead`; after the fix, 1 passed, with `TargetChanged`, receipt retained,
  destination absent, and stage cleaned.
- `cargo test -p studiofs-meta --lib retained_restore_rejects_stage_hardlink_to_doomed_payload -- --nocapture`:
  1 passed; the hard-linked stage is rejected without changing receipt/data.

Focused validation after the changes:

- `cargo test -p studiofs-meta --lib retained`: 20 passed, 0 failed, 9 ignored.
- `cargo test -p studiofs-vfs --lib retained_restore_`: 2 passed, 0 failed.
- `cargo test -p studiofs-vfs --test vfs_test retained_`: 5 passed, 0 failed,
  3 ignored.
- `cargo test -p studiofs-meta --lib pg_engine::tests::lifecycle::test_pg_retained_expiry_conflicts_with_offline_restore_like_purge -- --ignored --exact --nocapture`:
  1 passed, 0 failed.
- `cargo test -p studiofs-vfs --test vfs_test retained_restore_losing_to_settled_peer_purge_never_resurrects_inode -- --ignored --exact --nocapture`:
  stopped with Ctrl-C after 5 minutes with no test output beyond `running 1 test`;
  not counted as a result.
- The ignored CLI load test was selected once and reported 1 ignored because it
  requires PostgreSQL. No suite-wide tests were run.
- `cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs --all-targets -- -D warnings`,
  `cargo fmt --all -- --check`, and `git -c core.safecrlf=false diff --check`:
  exited 0.

Remaining review items from the preceding handoff: item 2 (abandoned-stage
sweep), item 6 (complete cache/dentry review), item 7 (timestamp policy), and
item 9 (post-copy open-race test and expanded live PG loser assertions). The
live VFS ignored case still needs investigation because it exceeded the bounded
run; the workspace test `.env` supplied its service settings without printing
them.

### Haiku: pending-state fence (review finding 1 and same-size overwrite)

What changed:

- `io_ops.rs` `restore_retained_entry`: waits for in-flight deferred
  materialization on the root source before its fence capture, and on each child
  before its fence capture (same pattern as rename-overwrite in
  `namespace_ops.rs`). The commit `is_open` closure now refuses while a doomed
  inode has pending local state, not only open handles. The per-child busy check
  refuses on pending local state too.
- `sync_ops.rs`: `inode_has_pending_local_state` is `pub(super)` so `io_ops`
  can call it. No new mechanism was added.
- `tests.rs`: `retained_restore_rejects_same_size_overwrite_during_copy` (review
  finding 2) and `retained_restore_does_not_lose_bytes_pending_last_handle_materialization`
  (finding 1). The latter writes and releases inside the `CopyRead` window with
  materialization held, so the release is pending while redb size, ctime, and
  the handle count look unchanged.

Why the test window is during the copy: a write that is released before the
restore starts is already caught by `read`, which waits on the deferred release
barrier. The restore then refuses with `TargetChanged`. The data-loss window is
a pending release that starts after the copy has read its bytes.

RED then GREEN:

- Before the fix, `cargo test -p studiofs-vfs --lib retained_restore_`:
  3 passed, 1 failed. `retained_restore_does_not_lose_bytes_pending_last_handle_materialization`
  returned `Ok` with the stale bytes `old` instead of `NEW-longer-value`, and the
  source was deleted. This is the data loss.
- After the fix, the same command: 4 passed, 0 failed.
- Guard check for finding 2: the ctime half of the fence at `retained.rs:437` was
  removed temporarily. `retained_restore_rejects_same_size_overwrite_during_copy`
  then FAILED (0 passed, 1 failed; the restore succeeded instead of returning
  `TargetChanged`). The line was restored, and its SHA-256 was checked against a
  backup before the final run.

Focused validation after the final change:

- `cargo test -p studiofs-vfs --lib retained_restore_`: 4 passed, 0 failed.
- `cargo test -p studiofs-meta --lib retained`: 20 passed, 0 failed, 9 ignored.
- `cargo test -p studiofs-vfs --test vfs_test retained_`: 5 passed, 0 failed,
  3 ignored.
- `cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings`,
  `cargo fmt --all -- --check`, and `git -c core.safecrlf=false diff --check`:
  exited 0.

Ignored tests were not run. No workspace-wide suites were run.

Still open from the same review: finding 3 (stale link-change attr at
`io_ops.rs:654`), finding 4 (post-commit `?` at `io_ops.rs:671/682` skips
invalidations), and finding 5 (captured object IDs are unused).

### Haiku: post-commit restore fixes (findings 3–5)

Changed in `crates/studiofs-vfs/src/vfs/io_ops.rs` (`restore_retained_entry`):

- Finding 3: the committed link count is now read after the commit with
  `meta.get_attr`. A surviving inode passes its committed `nlink` and `ctime`.
  A removed inode passes `nlink` 0 and its pre-removal ctime, as rename-overwrite
  does.
- Finding 4: the ACL epoch bump and kernel invalidation now run before the
  retirement loop. The loop no longer returns early: each source's failure is
  recorded (first error wins), retirement continues, and `request_eager_flush`
  runs before the first error is returned. A retry therefore cannot take the
  terminal branch without the invalidations having run.
- Finding 5: `removed_sources` carries only `InodeAttr`. The captured object IDs
  and the `regular_file_graph_usage` capture in the copy path are removed. The
  directory cleanup receives `Vec::new()`, as rename-overwrite does.

Test added: `retained_restore_keeps_outside_hardlink_at_committed_nlink`
(`crates/studiofs-vfs/src/vfs/tests.rs`). A file with an outside hard link is
restored. The survivor's `getattr` reports nlink 1, and the restored copy reads
the original bytes. This test passes on both the old and the new code. The stale
attr only matters when a dirty or deferred attr entry exists for the source, and
the commit's fence refuses inodes with pending local state, so this change is a
correctness fix without a deterministic red reproduction. No failure hook was
added for finding 4.

Validation after the final change:

- `cargo test -p studiofs-vfs --lib retained_restore_`: 5 passed, 0 failed.
- `cargo test -p studiofs-meta --lib retained`: 20 passed, 0 failed, 9 ignored.
- `cargo test -p studiofs-vfs --test vfs_test retained_`: 5 passed, 0 failed,
  3 ignored.
- `cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings`,
  `cargo fmt --all -- --check`, and `git -c core.safecrlf=false diff --check`:
  exited 0.

Ignored tests were not run. No workspace-wide suites were run.

Review items still open: 2 (abandoned stage sweep), 6 (stale caches for old
directories and symlinks), 7 (timestamps), and 9 (PG test assertions).

### Haiku: own-stage sweep (item 2)

What changed:

- `restore_retained_entry` (`crates/studiofs-vfs/src/vfs/io_ops.rs`) names each
  stage `restore-<owner:016x>-<id>`. Before copying, under
  `retained_restore_gate`, it lists the receipt and removes every entry whose
  name carries this volume's exact prefix and an all-digit id, using the existing
  guarded `cleanup_retained_restore_stage`. A listing failure logs `warn!` and the
  restore continues. `data` and foreign or legacy stages are never touched.
- The owner is `retained_restore_owner()`: a random 64-bit tag persisted in the
  local `retainedRestoreOwner` counter, created once per volume. Option chosen and
  rejected options are in the Decision Log (2026-11-13).
- `settle_retained_purge` is unchanged and still never touches stages.
- The `ponytail:` comments now state the sweep limit: it runs only on the next
  restore of the same receipt, and stages of receipts never restored again stay
  until purge.
- Known limit: a legacy `restore-<id>` stage from before this change is never
  swept, because no client can prove it is its own.

Tests (`crates/studiofs-vfs/src/vfs/tests.rs`):

- `retained_restore_sweeps_own_abandoned_stage`: a leftover stage with this
  volume's tag is removed, with its inodes, and the restore succeeds.
- `retained_restore_never_sweeps_foreign_or_legacy_stages`: a stage with another
  tag and a legacy `restore-<id>` stage both survive, and their bytes stay
  readable. The restore succeeds.

Validation after the final change:

- `cargo test -p studiofs-vfs --lib retained_restore_`: 7 passed, 0 failed.
- `cargo test -p studiofs-meta --lib retained`: 20 passed, 0 failed, 9 ignored.
- `cargo test -p studiofs-vfs --test vfs_test retained_`: 5 passed, 0 failed,
  3 ignored.
- `cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings`,
  `cargo fmt --all -- --check`, and `git -c core.safecrlf=false diff --check`:
  exited 0.

Ignored tests were not run. No workspace-wide suites were run.

Review items still open: 6 (stale caches for old directories and symlinks),
7 (timestamps), and 9 (PG test assertions).

### Haiku: restore caches and timestamps (items 6, 7)

What changed (`crates/studiofs-vfs/src/vfs/io_ops.rs`):

- Item 6: the removed-source loop in `restore_retained_entry` now treats
  `nlink == 0` as removed, not only a missing inode. A source the metadata
  store keeps at nlink 0 for deferred deletion now gets the same retirement as
  rename-overwrite: OS cache retirement, deleted-for-uploads marking, orphan
  handling, and, for directories, the generation bump and dentry-parent
  invalidation. Missing-inode removals already received all of this; the gap
  was the nlink-0 branch. `record_retained_restore` is unchanged: it already
  invalidates attribute caches for every visited inode, and the per-directory
  invalidation stays in the removal loop. The existing first-error-wins loop is
  kept.
- Item 7: each copied file, directory, and symlink gets its source atime and
  mtime through `setattr` (`restore_copied_times`). Directory times are applied
  after the walk, deepest first, so child creation cannot move them. Symlinks go
  through the same path and are not skipped. Limits, noted in the `ponytail:`
  comment: ctime is not preserved, and the directory time list is O(directories)
  in memory.

Tests (`crates/studiofs-vfs/src/vfs/tests.rs`):

- `retained_restore_drops_cached_children_of_removed_directories`: warms
  `receipt/data/sub/child`, restores, and asserts the old directory and child no
  longer resolve. It passes both before and after the fix. It guards the
  existing removal path; the nlink-0 branch has no unit test because the restore
  path cannot produce that state on demand.
- `retained_restore_preserves_copied_node_times`: failed before the fix (the
  restored directory took the current time) and passes after. It checks atime
  and mtime on a directory, a file, and a symlink.

Validation after the final change:

- `cargo test -p studiofs-vfs --lib retained_restore_`: 9 passed, 0 failed
  (7 existing and 2 new).
- `cargo test -p studiofs-meta --lib retained`: 20 passed, 0 failed, 9 ignored.
- `cargo test -p studiofs-vfs --test vfs_test retained_`: 5 passed, 0 failed,
  3 ignored.
- `cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings`,
  `cargo fmt --all -- --check`, and `git -c core.safecrlf=false diff --check`:
  exited 0.

Ignored tests were not run. No workspace-wide suites were run. Item 9 (PG test
assertions) remains open.

### Haiku: PG loser assertions (item 9)

Test: `test_pg_retained_expiry_conflicts_with_offline_restore_like_purge`
(`crates/studiofs-meta/src/pg_engine/tests/lifecycle.rs`), `expiry_first`
branch. On the losing client (`first`), it now asserts:

- `first.get_attr(payload.ino)` returns `InodeNotFound`: the purged inode is
  not resurrected locally.
- `first.replication_backlog().pending_ops == 0` after its flush and sync.

Skipped: the janitor-queue check (`snapshot_payload_gc_candidates`). This test
creates only directories, so no payload objects exist and there are no object
IDs to assert on. The file-backed queue checks live in
`test_pg_retained_purge_settlement_reclaims_through_gc_queue`.

`restore_retained_entry` in this test passes `&[]` sources and `|_| false`
is_open. That is correct: there are no copied file inodes to fence, and no open
handles.

Test counts:

- `cargo test -p studiofs-meta --lib retained`: 20 passed, 0 failed, 9 ignored.
- Live, `test_pg_retained_expiry_conflicts_with_offline_restore_like_purge`
  (`--ignored --exact`): 1 passed, 0 failed, 1203 filtered out, 20.82s.
- Live, `test_pg_retained_` (`--ignored --test-threads=1`): 6 passed, 0 failed,
  1198 filtered out, 89.00s.
- `cargo clippy -p studiofs-meta --all-targets -- -D warnings`: exit 0.
- `cargo fmt --all -- --check`: exit 0.
- `git -c core.safecrlf=false diff --check`: exit 0.

All review items 1–9 are now addressed, pending the final Opus review.

### Haiku: final-review LOW fixes (F1–F3)

**F1 — restore owner tag survives a local-state replace.** `retainedRestoreOwner`
is now `pub const LOCAL_PG_RETAINED_RESTORE_OWNER_COUNTER` in
`crates/studiofs-meta/src/pg_engine.rs`, re-exported from `lib.rs`. It is listed
in `is_local_pg_snapshot_state_counter`, so the preserving replace keeps it, the
same as `pgClientSlotPlusOne`. `retained_restore_owner` in
`crates/studiofs-vfs/src/vfs/io_ops.rs` reads and writes the constant. The
test `import_remote_snapshot_preserving_local_state_keeps_local_overlay_and_event_log`
(`crates/studiofs-meta/src/redb_engine/tests/snapshots.rs`) sets the tag to 77
locally and 99 on the source, then asserts the local value is still 77.

Export check: `export_remote_snapshot_from_read_txn_with_check`
(`redb_engine/replication.rs`) uses the list as a reject-list
(`!is_local_pg_snapshot_state_counter`). Adding the tag therefore removes it
from exports. It cannot export it. That same helper feeds the serving-checkpoint
export (`export_serving_checkpoint_from_read_txn_with_check`), so the tag is
also kept out of checkpoints. No exposure.

Known limit: a full `ReplaceLocalState` still wipes the tag, as it does the
client slot. Stages created before such a replace stay as a leak.

**F2 — a committed restore no longer returns `Err`.** After the durable commit,
per-source failures (the attr read and the retirement step) are logged with
`tracing::warn!` (inode and error only). Retirement continues for the remaining
sources. `request_eager_flush` is kept, and the call returns `Ok(restored)`. No
new test, per the review scope.

**F3 — the root pre-copy busy check matches the child check.**
`restore_retained_entry` now refuses on `open_count > 0 ||
inode_has_pending_local_state`. New test
`retained_restore_refuses_root_with_pending_local_state_before_staging`
(`crates/studiofs-vfs/src/vfs/tests.rs`): an unflushed write with no open
handle leaves pending state. The test asserts that state, asserts the restore
returns `ResourceBusy`, and asserts the `CopyRead` hook never fires, so no stage
copy started. Red check: with the guard removed, this test failed (0 passed,
1 failed). The guard was restored before the counts below.

Test counts (all on the final code):

- `cargo test -p studiofs-vfs --lib retained_restore_`: 10 passed, 0 failed (9 before + the F3 test).
- `cargo test -p studiofs-meta --lib retained`: 20 passed, 0 failed, 9 ignored.
- `cargo test -p studiofs-meta --lib snapshots`: 0 passed, 0 failed, 1 ignored. The filter matches no test name: the edited test is registered as `redb_engine::tests::…`. The exact-name run below covers it.
- `cargo test -p studiofs-meta --lib import_remote_snapshot_preserving_local_state_keeps_local_overlay`: 1 passed, 0 failed.
- `cargo test -p studiofs-meta --lib import_remote_snapshot`: 5 passed, 0 failed.
- `cargo test -p studiofs-vfs --test vfs_test retained_`: 5 passed, 0 failed, 3 ignored.
- `cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs --all-targets -- -D warnings`: clean.
- `cargo fmt --all -- --check`: exit 0.
- `git -c core.safecrlf=false diff --check`: exit 0.

### Retained lifecycle gating test (2026-11-13)

The Haiku run extracted the gate and wrote the tests, then failed on an
external API authentication error (Cloudflare 403) before it could validate
them. The parent session finished the validation.

The poll loop now calls `retained_lifecycle_due(state_is_live, last_run,
now, interval)` in `pg_engine/lifecycle.rs`. Behaviour is unchanged:
- The sweep runs only when the engine is in `Live`; `TargetReached` does
  not count.
- It runs at most once per `RETAINED_LIFECYCLE_INTERVAL`. At the boundary,
  `>=` applies: exactly 60 s counts as due.
- `last_retained_sweep` is updated before the sweep starts, so an attempt
  that ends in an error still waits a full interval and is not retried in a
  loop. This is deliberate; the doc comment says so.

Four pure unit tests cover the gate: not `Live`, never run, inside the
interval, and at or after the boundary.

Validation:
- `cargo test -p studiofs-meta --lib retained_lifecycle_due`: 4 passed.
- `cargo test -p studiofs-meta --lib retained`: 24 passed, 0 failed,
  9 ignored.
- `cargo clippy -p studiofs-meta --all-targets -- -D warnings`: clean.
- `cargo fmt --all -- --check` and `git diff --check`: clean.

Still open for SFS-1054:
1. An Opus review of the settlement step together with the fourth-round
   fixes.
2. Item 4: native trash views; private-store path blocking.
3. Item 6: EN/UK guides; the native oracle on a disposable WinFSP mount.

### Peer expiry identity and stale expiry commit (F1, F2) — 2026-11-13

Start: branch `feat/sfs-1054-recoverable-trash`; the shared uncommitted tree
was preserved (no reset, stash, checkout or revert). Graph project
`E-dev-studiofs-cli` was indexed and `check_index_coverage` reported no recorded
issue for `pg_engine/{retained,metadata_io,replication_flush,remote_apply}.rs`,
`trash.rs` and `pg_engine/volume_settings.rs`; claims were confirmed by reading
source directly.

**F1 — identical peer expiry (verified RED, correction recorded).**
`publication_digest` covered only event bytes. Two clients proposing
byte-identical expiries therefore collided: B's publication returned A's version
and wrote no `event_log` row under B's client id. The reported symptom
"pending_ops stays above 0 without a new batch" was NOT reproduced: in both
pull orders the journal drained to 0. The reproducible defect is the missing own
publication, which the regression asserts.

Fix: `publication_digest(client_id, events)` with domain `studiofs-retained-publication-v2`.
A same-client response-loss retry still hits its receipt. A peer's identical batch
is now its own publication: it passes the existing `current == incoming` idempotence
in `apply_retained_authority` and lands in `event_log` under its client id.
The digest has one caller (`metadata_io.rs`); the receipt table schema is unchanged.

**F2 — expiry judged under a superseded shared period (verified RED).**
B queued `Expired` under 7 days; A raised the shared period to 30; B's stale
proposal was accepted, so the peer saw `PurgeRequested{Expired}`.

Design (smallest correct integration with the existing loser mechanism):

- SUPERSEDED by the revision stamp in the round below: this period-equality
  check was ABA-unsafe. The original text carried `period_days`, the last positive
  period the proposer judged expiry under. Commit compared it with the current
  shared period, read under the `metadata_state` row lock already taken by
  `BUMP_METADATA_VERSION`. No PG wall clock was used: the controllable-clock tests
  pass a `deleted_at + 7d` that lies in the real future.
- Only a fresh `Retained → PurgeRequested` transition is checked. Historical
  seeding (`seed_retained_authority`) passes `validate_expiry = false`. An
  identical retry short-circuits before the check. Manual `User` purge is unchanged.
- Local request (`request_retained_purge_at`) refuses an `Expired` whose period
  differs from the current local one, even when due.
- Client loss path: `losing_retained_terminal_records` also reports a stale
  expiry when the shared row is still `Retained` and the period differs. The flush
  reverts the overlay with `discard_stale_retained_expiry`, then retires the range
  (revert, then retire). The overlay revert removes only the exact stale overlay,
  when base is `Retained`, and writes no journal event. Its original claim that a
  crash here leaves the proposal "pending but safe" was incomplete; see the
  round below.
- Other records in the same batch stay accepted: the loser path retires only the
  stale range, and the next flush publishes the rest.

Evidence, from live PG unless marked unit:

- RED F1, before the digest change:
  `cargo test -p studiofs-meta --lib pg_engine::tests::lifecycle::test_pg_retained_identical_peer_expiry_publishes_own_batch -- --ignored --exact`
  failed on the peer's own `event_log` count (0) for `pull_before_flush=true`,
  and the same for `false`.
- GREEN F1 (both orders, with pending drain and local `PurgeRequested`): 1 passed.
- RED F2 with the commit guard disabled (`validate_expiry=false`):
  `test_pg_retained_stale_expiry_after_period_raise_is_retired` failed
  `left: PurgeRequested { reason: Expired { period_days: 7 } }`, `right: Retained`.
- GREEN F2 (both orders: raise before flush and raise after sync; restart;
  peer visibility; no expiry at 7 under 30; expiry at 30 succeeds): 1 passed, 22.93s.
  The flush returns `Ok` for a fully retired batch, through the existing cursor
  recovery path, because the retirement advances the cursor. The test checks state
  (pending 0, local and peer `Retained`), not the return value.
- `cargo test -p studiofs-meta --lib pg_engine::tests::lifecycle::test_pg_retained_ -- --ignored --test-threads=1`:
  8 passed, 0 failed, 130.35s. This includes same-client response-loss retry,
  both-order expiry/restore conflict, seeded authority, settlement, and
  restore/purge loser.
- `... test_pg_volume_settings_authoritative_cas_converges_two_clients -- --ignored`: 1 passed.
- `... test_pg_retained_rename_over_unpublished_draft_leaves_no_peer_inode -- --ignored`: 1 passed.
- Unit: `-p studiofs-meta --lib retained`: 24 passed, 11 ignored (the new `Expired`
  period assertion sits in `retained_entry_disabled_policy_expires...`);
  `trash`: 7 passed; `volume_settings`: 8 passed, 1 ignored;
  `-p studiofs-vfs --lib retained_restore_`: 10 passed;
  `-p studiofs-vfs --test vfs_test retained_`: 5 passed, 3 ignored.
- `cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings`: clean.
  `cargo clippy -p studiofs --all-targets -- -D warnings`: clean.
  `cargo fmt --all -- --check`: clean. `git -c core.safecrlf=false diff --check`: clean.

Boundaries and remaining risk:

- Client clock trust boundary (explicit): "due" is judged by the proposing client
  from its own clock, the same trust already given to `deleted_at`. The commit
  checks only the shared settings revision, not a PG-authoritative time. A
  publisher with a wrong clock can still propose an early expiry under the
  current revision. The revision check is ABA-safe; it does not defend against
  clock lies. Its guarantee is "the proposer judged under current settings",
  not "the entry is due by server time". No new security claim is made.
- A proposer that has not yet synced the newest revision is rejected at commit
  and retired, then re-proposes after its sync. Each flush can repeat that until
  sync arrives. Not measured.
- Record format: `Expired` is now `{"Expired":{"settings_revision":N}}`. The
  earlier `period_days` shape was never released (0.2.3 unreleased), so there is
  no migration. Receipt digests use domain v2, so v1 rows can never match.
- The "pending stays stuck without a new batch" symptom was not reproduced in
  either pull order. Other interleavings were not explored and are not claimed fixed.
- Not run: full suites, native mounts, S3, CLI. Restore-loser behaviour and the
  `expiry_first` branch are unchanged and green.
- Still open for SFS-1054: Opus review of settlement with these fixes; item 4
  native views and private-store blocking; item 6 guides and native oracle.

### Crash window and ABA correction (Opus follow-up, F2 revision stamp) — 2026-11-13

Finding (accepted by Opus; verified here): the loser path reverted the stale
overlay and then retired its journal range in two durable steps. A crash between
them, followed by a user restore and a shared 7→30→7 round trip, could publish
the abandoned `Expired` ahead of or inside the restore's batch. The earlier
"pending but safe" explanation was incomplete. Two facts caused it:

1. Serving replay is authoritative for overlays of pending events:
   `restore_pending_state_from_local_log` replays `[flushed_seq, NEXT_SEQ)`. A
   silent overlay revert is therefore undone on restart while the stale
   `SetXattr` remains pending. Observed: after `discard` and `reopen`, the record
   is `PurgeRequested{Expired}` again.
2. Period equality is ABA-unsafe. 7→30→7 restores the period, so a stale proposal
   judged at period 7 passes the commit check again.

A restore is refused locally while the merged record is `PurgeRequested`
(`retained entry has a pending purge request`, `redb_engine/retained.rs`). So
with the replayed proposal, the restore is blocked until a flush heals it. An
accepted stale expiry blocks restore and can cause the restore to lose.

Fix (minimal, no journal guard, no new framework):

- `RetainedPurgeReason::Expired` now carries `settings_revision: u64`, the shared
  volume-settings revision the proposer judged under. Revisions are monotonic and
  never repeat.
- Commit (`check_expiry_revision`, under the `metadata_state` row lock) accepts
  an expiry only at the current revision. Historical seeding still passes
  `validate_expiry = false`. Identical retries short-circuit first.
- A rejection is permanent. A proposal rejected at revision r stays stale
  forever, because the current revision only grows. So reverting its overlay
  after rejection cannot be undone by a later publication. Nothing needs to
  guard the replayed proposal beyond the existing classifier.
- Loser path: stale means the shared row is still `Retained` and the revision
  differs. The flush reverts the overlay (only when it still equals the stale
  proposal, so a later restore is never reverted), then retires the range.
- Crash window: a crash between revert and retirement restarts with the proposal
  replayed. Restore is refused until a flush, whose commit rejects the proposal,
  then reverts and retires it. Restore never publishes on top of it.
- Wording: the earlier "retired before retirement" phrase (code comment and this
  plan) now reads "revert the overlay, then retire the range".

Evidence:

- RED, restart leg (before the revision fix; same test shape): after `discard`
  and `reopen`, `retained_entry` was `PurgeRequested{Expired{7}}`, not
  `Retained`. This is the replay effect in point 1. The period-equality ABA
  acceptance itself was NOT observed directly: the first ABA run stopped at the
  restart assertion. Its acceptance follows analytically from the period round
  trip and is not claimed as a separate red.
- RED, commit guard disabled (revision fix in place, `validate_expiry=false`):
  `test_pg_retained_abandoned_expiry_never_publishes_after_restore_and_period_aba`
  failed `Storage("retained entry has a pending purge request")` at the restore
  step. The stale proposal was published, and that blocks the accepted restore.
- GREEN: the same test, 3 shapes: in-process window with a combined batch;
  in-process window with split publication (flush before restore); restart
  with heal before restore. `1 passed`, 30.65s.
- `cargo test -p studiofs-meta --lib pg_engine::tests::lifecycle::test_pg_retained_ -- --ignored --test-threads=1`:
  9 passed, 0 failed, 159.79s. Includes same-client response-loss retry, F1
  identical peer expiry, F2 stale expiry after raise (both orders), the expiry
  and restore conflict, seeded authority, settlement, and loser preservation.
- `test_pg_volume_settings_authoritative_cas_converges_two_clients` (ignored): 1 passed.
- `test_pg_retained_rename_over_unpublished_draft_leaves_no_peer_inode` (ignored): 1 passed.
- Unit: `-p studiofs-meta --lib retained`: 24 passed, 12 ignored (new revision
  assertion in `retained_entry_disabled_policy_expires...`);
  `trash`: 7 passed; `-p studiofs-vfs --lib retained_restore_`: 10 passed.
- `cargo fmt --all -- --check`, `cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings`,
  `cargo clippy -p studiofs --all-targets -- -D warnings`, and
  `git -c core.safecrlf=false diff --check`: all clean.

Boundaries and remaining risk:

- Clock trust is explicit: "due" comes from the proposer's clock, like `deleted_at`.
  The revision check does not defend against clock lies (see the earlier
  "Client clock trust boundary" bullet). The restart heal waits for the next flush.
- Generic, pre-existing, not changed here: retirement of non-contiguous ranges
  may not be durable across restart. This is INFERRED from the ledger and cursor
  code (`complete_local_event_ranges_locked`), not proven by a test. If true, a
  restart replays retired events. The existing classifiers are idempotent, and the
  revision rule keeps this safe for expiries.
- Not run: full suites, mounted runs, S3, CLI, native views, and the Opus
  re-review, which is authorized to decide whether this closes the item.

### Native-view VFS seam (handoff item 4, partial) — 2026-11-13

Scope actually delivered: the VFS-level seam that native views will call. Not
delivered: native Explorer/WinFSP visibility, `$I`/`$R` synthesis, ACL-filtered
listing, mounted acceptance, Linux/macOS, and the EN/UK guides.

Code (new module `crates/studiofs-vfs/src/vfs/retained_views.rs`, hooks in
`namespace_ops.rs`, `misc_ops.rs`, `io_ops.rs`, `runtime.rs`):

- Private store: hidden root directory `.studiofs-retained`. Lookup/readdir of
  the name and of anything below it return not-found; rename, unlink and
  readdir reject paths inside it. Created through `reserve_namespace_ino` and
  `create_dir_with_ino`, the same path as `mkdir`.
- Retention: with a positive period, renaming into a trash-like directory is a
  retained move (`retain_namespace_entry`), not the destructive delete. Period
  zero keeps the existing destructive path and its `OperationNotSupported`
  no-replace refusal. Trash root `$RECYCLE.BIN` is created on demand.
- View names: `$R` plus six-or-more uppercase hex digits of the receipt inode.
  Only the canonical spelling parses (`trash_view_receipt`). A view is visible
  only while its receipt is `Retained`.
- Restore: rename of a visible view to `(parent, name)` runs the existing eager
  `restore_retained_entry`; it never overwrites.
- Clear: unlink of a visible view requests purge (`RetainedPurgeReason::User`),
  gated on read-write access to the original parent. Settlement stays with the
  shared lifecycle.
- Transport: `VfsRetainedStoreProvider` (runtime.rs) exposes
  `MetaEngine::retained_store` through `VfsMeta`, following the
  `VfsSnapshotProvider` pattern. `RuntimeMetaHandle` forwards it.

Decisions and why:

- The OS-chosen `$R` name is not kept. The view name is derived from the
  receipt, so a Shell-chosen name is not preserved. A local alias map is the
  next step, not a shared record.
- Store and trash are created through the reservation path (`reserve_namespace_ino`
  plus `create_dir_with_ino`). The earlier suspicion that a meta-only store
  directory breaks restore is NOT supported: every failing run used the guarded
  store name, so the store guard explains them. Creation path not separately tested.
- Store guard vs internal restore (root cause of every apparent failure seen in
  this work): the restore copy and cleanup used public `lookup`/`readdir_with_attrs`,
  which the store guard blocks. Those internal calls now use `sync_meta()`
  directly, and `readdir_with_attrs_unguarded` serves the copy. The guard stays
  on public paths. Found during this work, not a new redb defect. The earlier
  "meta occupied-refusal then restore fails" repro was the same guard, not redb.
- Receipt inode: `meta.next_inode()` as in existing tests. Switching to
  `reserve_namespace_ino` was tried only while the guard blocked the restore, so it
  was not a separate finding. Either allocator is untested after the fix.
- `record_removal` on the moved source invalidates attribute and dentry caches
  only. It does not tombstone the inode.

Focused evidence (no full suites):

    cargo test -p studiofs-vfs --lib retained -- --nocapture
    27 passed, 0 failed, 1342 filtered out
    cargo test -p studiofs-vfs --lib -- trash sfs815 recycle
    19 passed, 0 failed, 1350 filtered out
    cargo clippy -p studiofs-vfs --all-targets -- -D warnings  (clean)
    cargo fmt -p studiofs-vfs -- --check  (clean)
    git diff --check on crates/studiofs-vfs  (clean)
    cargo check -p studiofs-winfsp  (finished)

New tests in `retained_views::tests`: name round trip and rejection of
non-canonical spellings; store hidden from lookup, readdir, rename and
descendants; retained rename vs destructive at period zero; restore that
moves back and hides the view; purge via view unlink hides the name and sets
`PurgeRequested{User}`; collision refusal then retry (restore after an
`EntryExists` refusal). The collision test passes as written: an earlier
suspected defect in redb was the store-guard issue above.

Not yet proved (do not infer from the green tests):

- Purge denial when original-parent write access is revoked. No ACL-denial
  test exists yet; the code path is `request_trash_view_purge`.
- Read-only listing of views and ACL-filtered visibility: views are not listed
  by `readdir` yet, so this is absent, not tested.
- Guards on create, mkdir, rmdir and link into the store are not added.
  Kernel-facing paths reach store contents only through lookup or readdir,
  which the guards cover, so those two are the enforced boundary today.
- Hard links and open-handle behavior of retained entries are covered only by
  the existing retained-restore tests, not by new view tests.
- No WinFSP/Explorer visibility: `$RECYCLE.BIN` stays hidden at the root in
  `studiofs-winfsp/src/context.rs` and in `lookup_component_impl`, so Shell
  cannot discover views. Native recycle needs `$I` synthesis, which
  `ignored_entries` cannot provide: that registry is attribute-only and
  discards writes.
- The item 4 TTL gate for root visibility is not wired. Current behavior is
  unchanged at zero.
- Unverified concern from the earlier review, `replication_flush.rs` stale
  expiry retirement when `discard_stale_retained_expiry` is false: not
  investigated in this continuation. Open.

Next, in order: `$I`/`$R` synthesized listing with trusted original-parent
path, root visibility gated by positive TTL, a view-name map for Shell names,
ACL-denial tests for purge, the create/mkdir/rmdir/link guards, then native
Windows evidence on disposable mount data, which requires a product rebuild.

### Protection completion and first Windows Shell evidence (2026-11-13, continuation)

check_index_coverage (project E-dev-studiofs-cli, best-effort signal, run 2026-11-13 12:08 UTC) for the
files changed in this continuation: `retained_views.rs`, `retained_views/guard_tests.rs`,
`retained_views/native_tests.rs`, `acl.rs`, `locking.rs`, `runtime.rs`, `studiofs-winfsp/src/lib.rs`,
`studiofs-winfsp/tests/mount_sanity.rs`: all `metadata_match`, no recorded issue. The
earlier run (11:46 UTC) covered `namespace_ops.rs`, `io_ops.rs`, `misc_ops.rs`,
`ignored_entries.rs`, `studiofs-winfsp/src/context.rs`, `studiofs-winfsp/tests/mount_sanity.rs`, all
`metadata_match`. That signal does not prove completeness.

Private-store protection (VFS):

- One guard, `reject_private_store_ino`, in `enforce_access` and
  `enforce_access_async` (ACL layer every public op uses), plus explicit calls in
  `read`, `read_into`, `write_with_local_sync_policy`, `flush_with_outcome`,
  `readlink`, `fsync`, `open_file`, `flock`. `getattr` is covered by its ACL call.
- A store inode is private when every parent binding is private, down to the
  store. A payload with a live outside hard link keeps its outside parent, so it
  remains reachable by that name.
- Trusted seam: `trusted_retained_scope` (a `tokio::task_local`) set only inside
  `restore_retained_entry`. Its body runs inside the scope, so the copy and cleanup
  pass. It is not an exported bypass: only this seam sets it, and a task outside it
  never sees `true`. Verified by the restore tests.
- Visibility: a view is hidden when its original parent is missing, is under the
  trash root, or is under the store. A recreated path at the same name grants
  nothing (new inode).

Negative tests (all green): `private_store_inodes_refuse_every_public_entrypoint`
(known store, receipt and payload inodes against create, mkdir, symlink, link,
unlink, rmdir, rename both ways, readdir, lookup, readlink, open, get and set
xattr, flock, read, write, fsync; control operations succeed),
`get_xattr_of_store_inodes_is_absent_for_ordinary_clients`,
`revoked_original_parent_write_refuses_purge_and_restore_but_keeps_listing`,
`trashed_original_parent_hides_its_retained_entries`,
`missing_original_parent_hides_entry_and_recreated_path_grants_nothing`.

Native Windows view (VFS projection, `retained_views.rs`, `native_tests.rs`):

- Root `$RECYCLE.BIN` is visible when it exists; with a positive period it is
  created on demand (lookup and root readdir). Shell's case is matched to the
  stored canonical name.
- Pairs `$R<hex>` and `$I<hex>` with a shared six-digit uppercase suffix, per visible receipt.
- `$I` is a version-2 record generated from the receipt. Path = mount prefix (set
  per client with `set_native_trash_path_prefix`, called from `mount_and_serve`),
  then the current binding chain of the original parent. Size = payload size.
  Deletion time = trusted receipt time (FILETIME). Nothing is published.
- Views and sidecars are ignored-entries (local, unpublished). `IgnoredEntry` gained
  `view_bytes`; reads of a view return those bytes, and zeros otherwise.
- Draft `$I` names created under the trash root are local ignored entries (writes
  never change authority).

Focused VFS evidence: `cargo test -p studiofs-vfs --lib retained` 35 passed;
`cargo test -p studiofs-vfs --lib -- link rename` 72 passed (after the enforce
guard); `cargo test -p studiofs-vfs --lib -- trash sfs815 recycle` 19 passed (previous run).
`cargo clippy -p studiofs-vfs --all-targets -- -D warnings` clean;
`cargo clippy -p studiofs-winfsp --all-targets -- -D warnings` clean;
`cargo fmt --check` clean for both.

Actual Windows Shell evidence (WinFSP mount on owned test volume, drive `W:`, unused per
`GetLogicalDrives`; product rebuilt before the run; not the full suite):

- `winfsp_native_trash_retention_shell_probe` (TTL 7): PASS.
  `source_exists=False recycled_metadata=1 shell_listed=1 layout=shared`;
  `top_listing=$I001004,$R001004`. Real Move to Trash from Shell, listed by Shell
  namespace 10, the `$I` decodes to the exact `W:\shell-native-trash.txt`.
- `winfsp_default_disabled_trash_is_destructive_shell_probe` (TTL 0): PASS as a
  refusal. Shell reports `The request is not supported`; `source_exists=True`;
  no pair is published. This is the existing refusal path (`sfs815`), not a
  destructive delete. The earlier "destructive" wording in the contract does not
  match this behavior. Decision needed on whether TTL 0 should refuse or delete.
- `winfsp_native_trash_restore_and_clear_shell_probe`: FAIL. Shell's
  `InvokeVerb('undelete')` did not return within the 20 s bound (step log in
  `%TEMP%\studiofs-restore-steps.txt`: `invoke_undelete item_found=True`, no
  `invoked`). Teardown then reports `VFS shutdown should succeed: Meta(InodeNotFound(2))`.
  Not isolated: whether the undelete rename, the restore copy (blocking on
  `block_in_place` under the mount runtime), or the WinFSP callback is responsible.
  Unit tests of the same restore by name pass.

Open items (exact, not claimed done):

1. Restore via Shell hangs (above). Next step: capture WinFSP rename/create
   callback order during `undelete`; check the restore's `block_in_place` path on
   the mount runtime; check the `InodeNotFound(2)` teardown on the payload inode.
2. Clear through Shell is not separately observed (the restore probe fails first).
   Purge-by-unlink with ACL is unit-tested only.
3. Readdir/lookup of `$R` returns ignored-entry attributes copied from the payload;
   bytes are zeros. Shell does not read `$R` for listing, but an open of `$R` would
   read zeros. Limit recorded.
4. Shell-chosen `$R` names are not preserved; the view name is derived from the
   receipt. Shell's `rename` target name is discarded. Shell listed the derived
   name, so the pair is consistent. No alias map yet.
5. TTL 0 refusal vs destructive: user decision (see above).
6. Linux and macOS views are not implemented, and no cross-platform claim is made.
7. EN/UK guides are not updated (guarantees are not confirmed for restore and Clear).
8. The `replication_flush` stale-expiry concern is not investigated in this continuation.

### Native Shell restore root cause and fix (Opus one-off, 2026-11-13)

Root cause (from the WinFSP callback trace `target/probe-trace/restore-run3.log`):
Shell's Move to Trash follows a writer open (`fh=2`, `disable_os_cache=true`).
That writer gets `cleanup` at once, but its kernel `close` never arrives within
the 20 s probe. StudioFS drains a writer's dirty bytes only on the last-handle
`close`, so the retained payload keeps pending local state with a redb size of 0.
Two symptoms followed from this one cause:

1. Restore. Shell's `undelete` sends exactly one rename `$R… -> original`.
   Restore refused with `ResourceBusy` (first on the open handle, then, after
   Haiku's cleanup fence, on `inode_has_pending_local_state`). Shell maps that
   to a sharing violation and opens a modal "File In Use" dialog. In a headless
   probe this looks like a 20 s hang. Shell does not retry.
2. `$R` read. The view's size came from `meta.get_attr` (redb, still 0). So
   Shell's `ReadAllBytes` issued no read at all (`view_bytes_ok=False`).

Patch (minimal, local-only; no PG/S3 foreground step, no sleeps or retries):

- `io_ops.rs` `restore_retained_entry_trusted`: a live handle is one whose
  `cleanup` has not run (`handle_table.uncleaned_open_count`). It still refuses
  with `ResourceBusy` before staging. Without a live handle, pending local state
  is drained with the same local barrier that `flush` runs
  (`flush_inode_barrier_with_outcome`). Then the source fence is read, and the
  pending check runs again: anything still pending refuses. The directory-child
  walk uses the same rule. The post-stage check and the commit-time
  `is_open` fence use the uncleaned count. The commit fence keeps
  `inode_has_pending_local_state`. A cleaned-up but unclosed source is orphaned
  by the existing `mark_orphan_if_open`, and its late `close` completes normally.
  On FUSE no cleanup is recorded, so the uncleaned count equals the open count:
  behavior there is unchanged.
- `retained_views.rs` `trash_child_attr`: view and `$I` size come from
  `getattr_unchecked`, which includes the dirty attr.
- Probe `RECYCLE_RESTORE_PS1`: the Clear phase deleted the restored file and
  then recycled a missing path. It now recycles the restored file again, then
  removes only its own `$I`/`$R` pair.

Safety guard kept: `retained_restore_refuses_root_with_pending_local_state_before_staging`
now creates the pending state through a live (uncleaned) RDWR handle. It still
asserts `ResourceBusy` before any stage exists. The vfs_test
`retained_restore_copy_refuses_concurrent_writers…` (open handle) is unchanged
and passes.

RED, then GREEN:

- `retained_restore_drains_cleaned_up_writer_and_copies_exact_bytes` (new; open
  RDWR, `write_fh` without flush, `note_handle_cleanup`, retain, restore). Before
  the fix: `ResourceBusy "retained payload has an open handle…"`, the exact
  native error. After the fix: passes, with exact bytes and a later `release_fh` Ok.
- `trash_view_of_unflushed_payload_reports_and_reads_pending_bytes` (new).
  Before the fix: `left: 0, right: 10`. After the fix: passes.
- Haiku's unfinished `trash_view_restore_drains_unflushed_writes_first`: passes.

Commands and results:

    cargo test -p studiofs-vfs --lib retained                      40 passed, 0 failed
    cargo test -p studiofs-vfs --test vfs_test retained_           5 passed, 0 failed, 3 ignored
    cargo clippy -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings   clean
    cargo fmt -p studiofs-vfs -p studiofs-winfsp -- --check       clean (applied a pure reformat
                                                                   to Haiku's TTL0 probe lines)
    git diff --check -- crates/studiofs-vfs crates/studiofs-winfsp clean

Native (WinFSP, unused `W:`, owned disposable volume, test binary rebuilt after the change):

    STUDIOFS_WINFSP_TEST_MOUNT_POINT=W: STUDIOFS_WINFSP_TRACE_LOG=target/probe-trace/restore-run5-final.log \
      cargo test -p studiofs-winfsp --test mount_sanity winfsp_native_trash_restore_and_clear_shell_probe -- --ignored --exact --nocapture
    PASS: view_bytes_ok=True; restored_exists=True content_ok=True pairs_after_restore=0; cleared_pairs=0
    steps: invoke_undelete 12.199 -> invoked 12.365 (166 ms); clean VFS shutdown, no InodeNotFound
    STUDIOFS_WINFSP_TEST_MOUNT_POINT=W: cargo test ... winfsp_native_trash_retention_shell_probe   PASS (regression check)

Traces kept: `target/probe-trace/restore-run{1..3}.log` (Haiku, before the fix),
`restore-run4-drain.log` (restore fixed, `$R` size still 0),
`restore-run5-final.log` (all green). Step log: `%TEMP%\studiofs-restore-steps.txt`.

Remaining, not claimed:

- `cargo test -p studiofs-vfs --lib -- trash sfs815 recycle`: 22 passed and 2
  FAILED. Both failures predate this patch and are outside its paths.
  `sfs815_no_replace_preserves_source_for_trash_destination` still expects the
  TTL 0 `OperationNotSupported` refusal. `root_readdir_hides_trash_like_directories`
  expects `$RECYCLE.BIN` to be hidden. Haiku's uncommitted TTL 0 direct-delete
  change and the root-visibility change changed both behaviors. Reconcile each
  test with the chosen contract.
- Clear: Shell-side removal of the own pair is proven. Purge settlement
  (`PurgeRequested{User}` to physical purge) is unit-tested only, and Explorer's
  "Empty Recycle Bin" UI was not exercised. The host recycle bin was not touched.
- Directory restore through Shell, and Shell-chosen `$R` alias consistency, are
  not exercised natively.
- Linux/macOS views, the EN/UK guides, and the `replication_flush` stale-expiry
  item stay open (see above).
- Instrumentation still in the tree (Haiku's, kept because it is env-gated and
  useful): `install_probe_trace_log` plus the `tracing-subscriber` dev-dependency
  in `studiofs-winfsp`, the `ino`/`fh` fields on the WinFSP `cleanup` trace, and
  the step log in `RECYCLE_RESTORE_PS1`. This patch added no instrumentation.

### Cleanup write fence and native re-check (Haiku follow-up, 2026-11-13)

Superseded by the "Mapped-writer contract and restore fence" section below. Its
claim that `DisableCache` precludes post-cleanup writes is false, and its
cleaned-handle refusal and cleaned-handle restore fence were removed.

Question resolved: can a handle whose WinFSP `cleanup` already ran still
deliver bytes after restore commits? Sources:

- WinFSP header `inc/winfsp/winfsp.h` (installed SDK, primary source; `ctx7`
  is not on PATH, so no Context7 lookup was possible). Lines 120-146: the
  fine-grained model says "File I/O is not guarded and concurrent reads/writes/etc.
  are possible"; Cleanup(Delete) is EXCL while non-delete Cleanup and Write are
  NONE, so a Write can run concurrently with a non-delete Cleanup. Lines 349-360:
  Cleanup "is <b>not</b> the final close", and the file system "must be ready to
  receive additional operations until close time". The header does not document
  cache-manager lazy-write ordering after Cleanup.
- StudioFS policy (`crates/studiofs-winfsp/src/context.rs`, `should_disable_winfsp_file_cache`):
  every write-capable handle disables the OS cache, so no cache-manager writeback
  can arrive after close. Opus's cached-writer concern therefore does not apply to
  any handle that can write.
- The remaining hazard is the concurrent race: a Write admitted before Cleanup
  can finish after restore's fence. The write records bytes under the per-inode
  `file_session` Write gate, and restore's fence did not take that gate, so the
  bytes could land on a source already moved out of the namespace.

Fix (minimal; no PG/S3 step, no sleeps):

- `handle_table.rs` `is_cleaned_up(fh)`. `vfs.rs` `enforce_write_access_for_handle`
  refuses a cleaned-up handle with the same `AccessDenied` a released handle
  gets. Only the two write paths call this helper, both already inside the Write
  gate. The namespace caller uses the lower guard and is unchanged.
- `file_session.rs` new `FileSessionOperation::Restore`: exclusive gate,
  blocks materialization (same as Delete).
- `io_ops.rs` `enter_restore_source_gates` takes a Restore permit on every
  `source_fences` inode after the copy and before the visited-handle check. The
  permits are released after the store commit, on the busy path too. Restore
  waits for an in-flight write to finish recording, and a write that starts
  after the gate is refused by the cleaned-handle check, so no accepted byte can
  reach a moved source. A busy result can now occur if a write records bytes
  during the copy.

RED, then GREEN (`cargo test -p studiofs-vfs --lib cleaned_up_writer`):

- `cleaned_up_writer_refuses_later_writes_without_accepting_bytes`: before the
  fix, the late write returned `Ok(4)`. After: `Err`, size 4, bytes `kept`.
- `cleaned_up_writer_late_write_after_restore_commit_is_refused`: before the
  fix, the late write returned `Ok(5)`. After: refused; restored bytes `exact`.
- The restore gate's in-flight interleaving is not tested deterministically.
  Holding a permit also blocks the restore's own reads of the source, so a
  timing test would not isolate the gate.

Pre-existing failures, decided from source, not from the old expectations:

- `sfs815` at period 0: an existing trash name is refused with
  `MetaError::EntryExists` and the source is kept. A fresh trash name is a direct
  delete. The old expectation (`OperationNotSupported` for both) predates the
  direct-delete change. Split into `sfs815_no_replace_preserves_source_for_trash_collision`,
  `sfs815_period_zero_fresh_trash_name_is_direct_delete`, and
  `sfs815_positive_period_retains_trash_root_move` (positive period: source
  gone, one `$R` plus one `$I` pair listed). All pass.
- `root_readdir_hides_trash_like_directories`: STILL FAILING, deliberately. The
  code always lists an existing `$RECYCLE.BIN` root, since the trash-root comments
  say Shell needs it. The test expects it hidden. Realigning requires the Windows
  display contract, which unit tests cannot establish (the code sets no hidden or
  system attribute on the root). Open native check: whether Explorer and `dir`
  show `W:\$RECYCLE.BIN`. The unix `.Trash*` names are hidden and pass.

Commands and results:

    cargo test -p studiofs-vfs --lib cleaned_up_writer     3 passed (after fix)
    cargo test -p studiofs-vfs --lib retained             40 passed
    cargo test -p studiofs-vfs --lib sfs815                5 passed
    cargo test -p studiofs-vfs --test vfs_test retained_   5 passed, 3 ignored
    cargo test -p studiofs-winfsp --lib                  191 passed, 1 ignored
    cargo test -p studiofs-vfs --lib -- trash sfs815 recycle  25 passed, 1 failed (root_readdir, above)
    cargo clippy -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings   clean
    cargo fmt -p studiofs-vfs -p studiofs-winfsp -- --check   clean (cargo fmt rewrote whitespace in the touched files)
    git diff --check -- crates/studiofs-vfs crates/studiofs-winfsp   clean

Native (WinFSP, `W:` free before and after, test binary rebuilt after the change):

    STUDIOFS_WINFSP_TEST_MOUNT_POINT=W: STUDIOFS_WINFSP_TRACE_LOG="E:/dev/studiofs-cli/target/probe-trace/restore-run6-final.log" \
      cargo test -p studiofs-winfsp --test mount_sanity winfsp_native_trash_restore_and_clear_shell_probe -- --ignored --exact --nocapture
    PASS: view_bytes_ok=True restored_exists=True content_ok=True pairs_after_restore=0 cleared_pairs=0
    STUDIOFS_WINFSP_TEST_MOUNT_POINT=W: ... winfsp_native_trash_retention_shell_probe
    PASS: top_listing=$I001004,$R001004 source_exists=False recycled_metadata=1 shell_listed=1 layout=shared

Instrumentation: the `ino`/`fh` fields on the WinFSP `cleanup` trace were
uncommitted and are removed (HEAD had only the bare trace). `install_probe_trace_log`
and the `tracing-subscriber` dev-dependency remain as env-gated test diagnostics.

Still open, not claimed:

- Directory restore through Shell and Shell-chosen `$R` alias consistency: not
  exercised natively in this pass.
- Clear: `PurgeRequested{User}` settlement to physical purge is still unit-tested
  only; the probe asserts pair removal, not the purge receipt.
- `root_readdir_hides_trash_like_directories` (above).
- Restore gate interleaving: covered by source reasoning and the sequential
  regressions, not by a deterministic test.
- Linux/macOS views, the EN/UK guides, and the `replication_flush` stale-expiry
  item stay open.

### Mapped-writer contract and restore fence (Haiku, 2026-11-13; supersedes the cleanup-fence section above)

Correction. The previous section said that `DisableCache` precludes every
post-cleanup write. That was false. `DisableCache` turns off only the
cache-manager (Cc) writeback. A mapped view (Mm) still page-writes through the
file object, which stays alive until `Close`. The blanket cleaned-handle
refusal added in that section broke this path natively and is removed.

Contract (primary source: installed WinFSP SDK `inc/winfsp/winfsp.h`; `ctx7`
is not on PATH, so no Context7 lookup was possible):

- Lines 349-360: Cleanup is "not the final close operation"; the file system
  "must be ready to receive additional operations until close time".
- Lines 120-146: fine-grained model. File I/O is not guarded, and non-delete
  Cleanup is in the NONE class, so Write and Cleanup can overlap.
- Not documented by the SDK: cache-manager and Mm paging-write ordering after
  Cleanup. The probe below is the evidence for that ordering.

Native probe `winfsp_native_mapped_writer_survives_handle_close_probe`
(`CreateFileMappingW` read-write, `MapViewOfFile`, the original handle dropped
before the view is modified, `FlushViewOfFile`, unmap, reopen). It asserts that
the view shows the written bytes before the handle closes, so a missing mapping
cannot pass. It also asserts a constrained write after the cleanup of the same
file. Trace evidence (`target/probe-trace/mapped-writer-red.log`,
`mapped-writer-final.log`):

    write ino=2 fh=1 offset=0 len=65536 constrained_io=false   (user write)
    cleanup ino=2 fh=1                                         (original handle closed)
    write ino=2 fh=1 offset=0 len=4096 constrained_io=true     (Mm paging write)

- RED (blanket refusal in place): paging write refused as `access denied:
  handle:1 required=ReadWrite actual=ReadOnly`; `FlushViewOfFile` returned
  `0x80070005`.
- GREEN (refusal removed): paging write accepted; the reopened file equals the
  expected bytes exactly.

Instrumentation kept for the probe: the `ino`/`fh` fields on the WinFSP
`cleanup` trace, and a `write` trace on the mounted path (`write_async`) with
`ino`, `fh`, `offset`, `len`, and `constrained_io`. The sync `write` trait
method is not on the mounted path and has no trace.

Fix:

- Removed the blanket refusal (`vfs.rs` `enforce_write_access_for_handle`), the
  handle table's cleaned-up set, `is_cleaned_up`, `uncleaned_open_count`,
  `note_handle_cleanup` (`sync_ops.rs`), and its call in `handle_drain.rs`.
  Cleanup no longer changes VFS state.
- Restore fence: any open handle on a source or child inode (`open_count > 0`)
  returns `ResourceBusy`. This applies before staging, after the copy, and in the
  commit predicate. The pending-state drain for closed handles is unchanged.
  Source session gates are held from the fence through the commit.
- Invariant: restore commits only when no handle is open on any source inode.
  Then the file object and any mapping are gone, and no admitted write holds a
  source gate. Any later write fails at handle lookup.
- Rejected narrower options (exempt constrained I/O while live, or restore-away,
  orphan, or nlink-0 exemptions): not proven. A mapped writer that outlives the
  restore commit can still target the old inode, so permitting writes while the
  handle is live does not establish restore safety.
- Cost, stated plainly: a Shell writer whose close lags now makes restore
  return busy until that close. Before this change, restore drained such a writer
  and succeeded (see the section above).

Tests:

- Removed: the cleaned-handle write refusal tests, and the cleaned-handle
  restore test that expected success while the handle was open.
- `retained_restore_waits_for_writer_close_then_copies_exact_bytes`: an open
  writer blocks restore with `ResourceBusy`, the source bytes are intact, and
  after close restore copies the exact bytes. A write on the released handle is
  refused, and the copy is unchanged.
- `retained_restore_gate_orders_admitted_write_before_fence`: deterministic.
  A new `RetainedRestoreStage::BeforeSourceGates` hook parks restore after the
  copy, with bounded 10 s coordination through `Notify`. The test holds a
  Write permit on the source, opens a writer handle, and starts a write. Its
  bounded wait checks `debug_snapshot(ino).queued_ops >= 2`, so the writer and
  restore are both queued behind the permit. Then it releases the permit.
  Result: the write returns 4; restore returns `ResourceBusy`; the restored
  name is absent; the source reads `LIVE`. After close, restore copies `LIVE`
  exactly.
- Mutation check: with the source gates disabled, the test fails at the
  queue-depth wait. Restored, it passes. Note: the per-inode depth assertion
  was needed; the global `file_session_stats().queued_ops` passed even without
  the gate.
- The existing concurrency hook now returns early for `BeforeSourceGates`.
- `lock_order.rs`: one sentence records that restore takes one `Restore` permit
  per unique source inode, in traversal order, and is the only multi-permit holder
  under `retained_restore_gate`.

Commands and results:

    cargo test -p studiofs-vfs --lib -- retained cleaned sfs815   46 passed
    cargo test -p studiofs-vfs --test vfs_test retained_          5 passed, 3 ignored
    cargo test -p studiofs-winfsp --lib                           191 passed, 1 ignored
    cargo clippy -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings   clean
    cargo fmt -p studiofs-vfs -p studiofs-winfsp -- --check       clean (fmt rewrote whitespace in touched files)
    git diff --check                                              clean

Native (WinFSP, `W:` free before and after, final binary):

    winfsp_native_mapped_writer_survives_handle_close_probe   PASS (final trace above)
    winfsp_native_trash_retention_shell_probe                 PASS (source_exists=False, shell_listed=1)
    winfsp_native_trash_restore_and_clear_shell_probe         INTERMITTENT, not claimed

The restore-and-clear probe hit the 20 s "did not finish" timeout in 2 of 4
untraced runs. It passed in the others, and in 10 traced runs. No traced run
contains the busy error text, and no hang was captured with a trace, so the
cause is unattributed and not proven to be the new fence. Passing traces show
`I/O error: entity not found` mapping lines, not investigated.

Still open, not claimed:

- `root_readdir_hides_trash_like_directories` stays red. Its realignment needs
  the Windows display contract, which was not checked.
- Clear: `PurgeRequested{User}` settlement to physical purge. The probe asserts
  pair removal only.
- Directory restore through Shell and Shell-chosen `$R` alias consistency.
- The intermittent Shell hang above: classified in the next subsection (restore
  busy refusal, not a generic flake). Its fix is an open architecture choice.
- Linux/macOS views, the EN/UK guides, and the `replication_flush` stale-expiry
  item stay open.

### Restore-and-clear timeout classification (Haiku, 2026-11-13)

Test oracle and diagnostics only. No product code changed in this pass.

Diagnostics in `crates/studiofs-winfsp/tests/mount_sanity.rs`:

- The restore script writes its step log to `STUDIOFS_PROBE_STEPS`, a file beside
  the script in that test's own temp directory, so it cannot be stale. New markers:
  `shell_namespace`, `checked`, `recycled_again`, `cleared`.
- On the 20 s timeout, `shell_recycle_probe` prints that step log in the panic.
- `STUDIOFS_WINFSP_TRACE_FILTER` overrides the probe trace filter. The default is
  unchanged. The runs below used `=error` levels and `file_session`/`context` debug.

Evidence:

- Untraced stall: the step log ends at `invoke_undelete item_found=True`, with no
  `invoked` line. The 15 s restore wait never started, so the 20 s limit expired
  inside `InvokeVerb('undelete')`.
- Error trace (`studiofs_winfsp=error,studiofs_vfs=error`): both stalled runs logged
  `vfs error: I/O error: retained payload has an open handle; close it before restore`,
  71 ms and 74 ms after the `invoke_undelete` step. The passing error-trace run
  logged nothing. The line is the restore fence (busy returns in
  `vfs/io_ops.rs`, the pre-stage, post-drain, and commit checks). `classify_io_error`
  maps `ResourceBusy` to `GenericFailure`, so WinFSP returns `STATUS_UNSUCCESSFUL`.
- Mechanism, inferred and not observed: Shell presents the failed undelete as a
  modal error dialog, which holds `InvokeVerb`. Dialog polling in three later runs
  saw no stall, so no dialog was captured.
- Which handle was open: not identified. In the passing traced run (`fence-10`) the
  writer's cleanup (13:26:13.887Z) precedes its close (13.922Z) by 35 ms, and the
  restore came about 280 ms after the close. A file the application has closed can
  still be open to WinFSP for a short gap, and the fence refuses during that gap.

Runs this session (10 total: 3 timeouts, 7 passes; no run is claimed green from one
pass):

| runs | trace | result |
|---|---|---|
| 1 | none | timeout at `invoke_undelete`; busy not traced |
| 3 | error-level | 2 timeouts, both with busy logged; 1 pass, no busy |
| 3 | none, dialog poll | 3 passes, no dialog seen |
| 3 | file_session/context debug | 3 passes, no busy |

Commands and artifacts:

    cargo test -p studiofs-winfsp --test mount_sanity --no-run      built
    STUDIOFS_WINFSP_TEST_MOUNT_POINT=W: target\debug\deps\mount_sanity-0ccc233bed46233d.exe --ignored --exact winfsp_native_trash_restore_and_clear_shell_probe --nocapture
    cargo clippy -p studiofs-winfsp --all-targets -- -D warnings   clean
    rustfmt --check --edition 2024 crates/studiofs-winfsp/tests/mount_sanity.rs   clean
    git diff --check                                               clean (line-ending advisory only)

Artifacts: `target/probe-trace/untraced-steps-1.out`, `errors-run-{1,2,3}.{out,log}`,
`dialog-run*.{out,log}`, `handles-run-{1,2,3}.{out,log}`. No test process survived
the last run. `W:` was free before the session; it was not re-checked after.

Decision needed (architecture; not patched here):

- A (recommended): restore waits, bounded, for in-flight closes of its source inodes
  before it returns busy. It blocks on a handle-close notification with a fixed upper
  bound, before staging and before the source gates are taken. A mapped writer whose
  close never arrives still gets busy after the bound, so the safety fence stays.
  Needs a close notification in the handle table, the bound (for example 1-2 s), and
  an accepted cost: a WinFSP rename callback may block for that bound.
- B: keep immediate busy and change only the probe so it does not block on
  `InvokeVerb`. Not recommended as the main fix. A just-closed file would still fail
  to restore intermittently, and the test would hide that.
- C: map busy to `STATUS_SHARING_VIOLATION` for a clearer message. It neither stops
  the modal nor removes the cause.

Not started: `root_readdir_hides_trash_like_directories`. Its gate, a proven native
basic restore, is not met. Clear's purge receipt, directory restore, and Linux/macOS
remain open.

### Bounded close wait: root decision A (Haiku, 2026-11-13, later pass)

Root decision: A. A restore that meets an open source handle waits, bounded and
on a close notification, before it returns busy. The wait is local. It has no PG
or S3 dependency. A true open or mapped writer still returns busy at the bound.

WinFSP guard and threading, verified from source, not assumed:

- `winfsp` 0.12.4 `FileSystemHost::new_with_timer_async` defaults to
  `OperationGuardStrategy::Fine` (`src/host/fshost.rs`). The adapter never calls
  a guard setter.
- `winfsp.h` (Fine model): EXCL = Create, Cleanup(Delete), SetInformation(Rename);
  SHRD = Open, ReadDirectory, GetVolumeInfo, Disposition; NONE = all others.
  Close and non-delete Cleanup are NONE, so Close runs while a rename holds the
  namespace lock.
- The rename callback calls `vfs.rename`, which uses `run_sync_future` with no
  outer lock, on one dispatcher thread. The default dispatcher count is 2x CPUs,
  clamped (`winfsp/src/lib.rs:293`), so at least two threads exist and one blocked
  rename leaves a thread for Close.
- Close path locks (handle-table write lock, `writer_stack.mark_closed`,
  `sustained_write_streams`) are short and release before any wait. The deferred
  release task's `enter_op(Release)` runs on a spawned task, not in the callback.
- Cost, not measured natively: while a restore waits, the rename holds the Fine
  EXCL namespace lock, so Opens (SHRD) on the mount wait up to the bound.

Implementation (`studiofs-vfs`):

- `vfs/handle_table.rs`: `HandleTable.closed: tokio::sync::Notify`, signalled in
  `close_with` after the counts are updated. This is the only new state.
- `vfs/io_ops.rs`: `RETAINED_RESTORE_CLOSE_WAIT` is 1.5 s, one aggregate deadline per
  restore. `restore_retained_entry_trusted` is now a retry loop around
  `restore_retained_entry_attempt`, which holds the previous body. An open-handle
  busy records its inode. A local-state busy records none and returns at once.
  Every attempt releases `retained_restore_gate`, the source gates, and staging before
  it returns, so the wait holds no lock.
- `wait_for_source_handle_close`: create `Notified`, `enable()`, read `open_count`,
  then await with `timeout_at(deadline)`. After a timeout it re-reads the count. A
  debug log records the inode and count.
- Test seams (`vfs.rs`, cfg(test)): `RetainedRestoreStage::BeforeCloseWait` (before
  arming) and `CloseWaitArmed` (after arming and the count check). The exhaustive
  stage match in the existing progress-hook test lists them.

Tests (new, `vfs/tests.rs`):

- `retained_restore_wakes_when_source_handle_closes_during_bounded_wait`: the writer
  closes at `CloseWaitArmed`. Restore is Ok, bytes exact, elapsed under the bound.
- `retained_restore_close_before_wait_arms_is_not_a_lost_wake`: the writer closes at
  `BeforeCloseWait`. Restore is Ok, elapsed under the bound.
- `retained_restore_open_handle_times_out_busy_after_bounded_wait`: never closed.
  Busy, elapsed at least the bound, source bytes intact, no destination.
- `retained_restore_unrelated_closes_do_not_extend_the_wait`: three unrelated closes
  wake the wait. Busy, elapsed under twice the bound (one aggregate deadline).

Unchanged and still green: `retained_restore_waits_for_writer_close_then_copies_exact_bytes`,
`retained_restore_gate_orders_admitted_write_before_fence` (now about 1.5 s), and the
`vfs_test` directory-child test `retained_restore_copies_directory_tree_symlinks_hardlinks_and_refuses_open_child`.

Mutation evidence:

- No wait (the wait returns false at entry, which is the old behavior): the four new
  tests fail and the twelve existing restore tests pass.
- Check before create (count read, then `notified()`): the wake test fails at the
  elapsed assertion, so the restore waited the full bound. The lost wake is caught.
- `enable()` removed: not detected. Tokio documents that a `Notified` created before
  `notify_waiters` is woken even if never polled. `enable()` stays as explicit arming,
  but the protection that matters is the ordering above.

Commands and results:

    cargo test -p studiofs-vfs --lib retained_restore_     16 passed (mutation: 4 failed, 12 passed)
    cargo test -p studiofs-vfs --lib handle_table          9 passed
    cargo test -p studiofs-vfs --test vfs_test retained_   5 passed, 3 ignored (PG/S3-gated)
    cargo clippy -p studiofs-vfs --all-targets -- -D warnings   clean
    rustfmt --check --edition 2024 (io_ops.rs, handle_table.rs, vfs.rs, tests.rs)   clean
    git diff --check                                       clean (CRLF advisory only)

Native (WinFSP, probe rebuilt with this change; each PASS checked `restored_exists=True
content_ok=True pairs_after_restore=0` and `cleared_pairs=0`):

    run                     trace                              result        busy/wait
    fix-native-run-1        error + io_ops debug               FAIL (20 s)   busy, 1.5 s timeout
    cleanup-run-1..3        cleanup/write TRACE + io_ops debug PASS x3       no busy
    fail-cw-run-1..2        same                               PASS x2       no busy
    fail-cw-run-3           same                               FAIL (20 s)   busy, 1.5 s timeout

Six native runs, two failed. Each failure is a run where the fence refused. No
passing run reached busy, so every busy in the sample was a failure.

Held handle, identified cheaply from callbacks (no heavy trace in the failing run):

- `ino=2` (the `shell-native-restore.txt` payload), `fh=2`: the PowerShell
  `WriteAllText` writer (WRONLY, create and truncate, `disable_os_cache=true`).
  Trace (`fail-cw-run-3`): write at 50.4227, cleanup at 50.4272, no close. The
  VFS `open_handles=1` matches the callback sequence, so the VFS count is not leaking.
- In passing runs the writer's close came 35-120 ms after its cleanup. In failing runs
  no close arrived within the 1.5 s bound. Whether one ever arrives was not established.
  The harness kill and mount teardown followed within milliseconds, so no later close
  can be attributed to them.
- Interpretation: a FILE_OBJECT reference held outside the VFS after cleanup. The
  mapped-writer contract forbids treating cleanup as close. WinFSP callbacks cannot
  distinguish a mapped section from another post-cleanup reference (cache manager or
  a filter driver). Not checked: `fltmc filters` returned access denied without
  elevation, so the filter list could not be read.

Blocker, concrete and unsolved:

- The bounded wait is correct for the close-lag case it was built for, and the VFS
  tests verify it. It does not make the native probe reliable: two of six runs still
  fail, and in both the writer's file object stayed referenced beyond the 1.5 s bound.
- Raising the bound to 2 s is inside the authorized range but was not tested. It
  would not help if the reference outlives the bound, as these failures suggest.
- To close: identify the post-cleanup holder (elevated `fltmc`, or ETW or Process
  Monitor), or decide whether a post-cleanup non-mapped file object may stop blocking
  restore. The second needs a way to tell mapped sections apart from other references,
  which WinFSP callbacks do not expose.
- Still inferred: the Shell modal. No dialog was captured. The dialog-watch runs
  passed without a stall.

Not done in this pass: `root_readdir_hides_trash_like_directories` (its gate, a
proven native basic restore, is not met); Clear's purge receipt; directory restore
through Shell; Linux and macOS. The native restore-and-clear probe remains
INTERMITTENT and is not claimed green.
### Sol production slice: identity-preserving local payload protection — 2026-11-13

Implemented `MetaRetainedStore::protect_retained_file_payload(receipt, source,
stage)`, with Redb and Pg implementations and explicit read-only rejection.
The PG adapter uses its existing local `mutate` pending-state boundary; this
operation makes no PG/S3 request. The Redb transaction requires a retained
receipt, source membership in receipt/data, matching source ctime/size, and a
distinct regular-file stage of identical length. It requires a complete overlay
covering the entire file, fresh object IDs disjoint from both source graphs,
and local/uploading slice states with no published stage base.

It installs those slices through the EXISTING
`apply_regular_file_overlay_mutation_in_txn`, advances the source content epoch,
and commits overlay, normalized pending attr, lifecycle/recovery bookkeeping and
ordinary SetRegularFileOverlaySlices/SetAttr journal events atomically. The
source inode, crtime, link count, names and external hardlinks remain intact.
Only the helper visibility changed to pub(super); no second journal or storage
framework was introduced. A failed fence or incomplete fork rolls back without
adding source recovery events or changing the source graph.

This is a production PORT and local metadata primitive, not yet connected to
the restore callback. Its explicit caller preconditions are important: hold the
source session permit, own the stage against mutation/publication, and durably
sync all stage bodies first. Metadata cannot establish body durability itself.
Runtime upload ownership must be rebound before removing the stage. This slice
retains the stage; it does not claim that stage cleanup or runtime upload
handoff is implemented. It also does not claim full namespace reconstruction.

Focused real-byte proof is
`vfs::tests::retained_payload_protection_reopens_same_inode_with_fresh_local_bodies`.
It materializes 131089 patterned bytes separately on source and stage, protects
the source, shuts down/drops the first VFS and database, reopens the same local
DB/cache, deletes the OLD source objects from that test's local payload backend,
and reads the exact bytes through the original inode. It verifies fresh IDs,
the external hardlink, and the advanced epoch. Negative checks reject a short
stage, a same-length graph with an uncaptured tail, and a stale source fence,
without journal changes. This is graceful reopen and local body independence,
not abrupt-crash, shared purge/apply or mounted acceptance.

Commands:

    cargo check -p studiofs-meta
    cargo test -p studiofs-vfs --lib retained_payload_protection_reopens_same_inode_with_fresh_local_bodies -- --exact vfs::tests::retained_payload_protection_reopens_same_inode_with_fresh_local_bodies --nocapture
    cargo test -p studiofs-meta --lib retained_entry_read_only_capability_lists_but_denies_all_mutations -- --nocapture
    cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings

Check and final Clippy passed; read-only mutation test passed (1 nonzero test).
The final strengthened real-byte test, including old-object deletion and all
negative cases, passed (1 nonzero test, 1391 filtered out). Rustfmt and diff
whitespace checks passed. The two previously RED identity restore tests were
not rerun: their restore callback has not changed in this slice.

Next coherent integration is still required: reuse the stage-copy pipeline
under ordered source gates without recursively acquiring read/write/flush
permits; freeze/rebind the existing cached-store pending uploads to the source
inode/new epoch; refresh runtime attr/incarnation authority; commit the receipt
transition and an identity-preserving destination move with COMPLETE tree
reconstruction events (including xattrs and external aliases) in one metadata
transaction. Only after that may the stage be retired. Losing terminal-receipt
retirement must leave those reconstruction events pending. Then turn the two
existing native-identity RED tests green and prove peer purge/expiry followed
by apply/restart and old-object GC. Those are unresolved implementation steps,
not an external blocker or feature-completion claim. No native rerun is counted.

### Sol continuation: executable native-identity regressions — 2026-11-13

The earlier assessment must not be interpreted as proof that handle retargeting
is required. Keeping the inode and durably forking its payload is the simpler
candidate for native semantics: every existing handle, WinFSP cached inode,
lock key, external hardlink and directory handle then retains its identity.
This is an implementation choice still to complete, not an external blocker.

Two executable RED regressions now live in `vfs/tests.rs`:

    cargo test -p studiofs-vfs --lib retained_restore_preserves_ -- --nocapture

Result: 3 selected, 1 passed (existing copied-node-times test), 2 failed.
`retained_restore_preserves_outstanding_writer_identity` retains a live VFS
writer, requests restore, then uses the original fh/ino for late write, flush,
fsync and release and requires exact bytes at the restored name. It currently
fails with ResourceBusy. The handle is released and VFS shutdown completes
before the RED assertion. This is the VFS callback identity model, NOT an actual
native mapping/cleanup proof.
`retained_restore_preserves_external_hardlink_identity` restores a CLOSED file
with an external hardlink, writes AFTER! through that alias, and requires both
names to retain one inode and exact bytes. It fails with restored inode 4,
external alias inode 3. This second failure is independent of delayed Close:
retargeting open handles alone would leave the hardlink defect intact.

Further source checks narrow the next implementation:

- `redb_engine/snapshots.rs:484-487` clones metadata graphs and their original
  object references. This is not a payload fork into new immutable objects.
- `remote_apply.rs::serving_event_conflicts_with_overlay` preserves a local
  regular-file overlay against RemoveAttr. That is a serving-state protection,
  not capture of remote-only bodies and not indefinite shared object protection.
- `replication_flush.rs:1260` retires only a losing receipt terminal event;
  the remaining journal publishes. In-place recovery therefore needs a complete
  durable reconstruction journal for the accepted tree, not just RenameEntry.
- `fs_ops.rs::rename_in_txn` journals RenameEntry plus moved/parent attrs;
  it does not journal every descendant's metadata/payload. Plain native rename
  alone does not reconstruct a directory after the peer settled its purge.
- `read` and `write_with_local_sync_policy` both take session permits. So do
  fsync and flush. An in-place fork holding source Restore permits must reuse
  the ordered bodies/local barrier under those permits; calling the public
  operations recursively would deadlock. The local barrier explicitly expects
  its caller to hold a permit and to join deferred materialization first.

Required invariants for that path: join deferred materialization before taking
source gates; take gates in inode order; capture every remote-only byte into
fresh durable local objects before accepting restore; atomically retain the
complete tree reconstruction and destination move; do not replace inode/fh,
writer/lock keys or hardlinks. A write admitted before the gate must be included;
a late write after commit must address the same restored file. Flush/fsync,
getattr/setattr/truncate, release and stale WinFSP callbacks continue using the
same identity. No wait for Close may retain a permit needed by Close.

The old acceptance assertion `never_resurrects_inode` conflicts with this
identity-preserving choice AFTER a peer has already physically removed that
inode. It must be explicitly replaced by a stronger data/namespace outcome:
accepted local reconstruction survives apply/restart, republishes fresh object
IDs, peer settlement cannot remove a restored live name, and both peers read
exact bytes. Merely deleting that assertion without those proofs is forbidden.
No such contract change or production implementation has yet been made.

Checks in this continuation: rustfmt --check for the changed test file passed;
git diff --check passed (existing LF/CRLF advisories). Graph exact-path coverage
was checked for each source used above, followed by direct source reads. No
native run, full suite, mount, helper, commit or external mutation occurred.
Current worktree intentionally contains the TWO NEW FAILING TESTS; this is
unfinished RED-stage work and must not be reported as feature completion.

### Sol bounded architecture assessment — 2026-11-13

This pass preserves all product code and the open-handle fence. It does not
claim a native restore fix. Repository: `E:/dev/studiofs-cli`, branch
`feat/sfs-1054-recoverable-trash`; the extensive dirty worktree is intentional.

Source-backed incompatibility in the CURRENT implementation:

- `vfs/io_ops.rs::restore_retained_entry_attempt` stages fresh inodes, takes
  source session gates, checks open counts, and commits before retiring source
  runtime state. The fence protects late writes, not merely incomplete copying.
- `redb_engine/retained.rs::restore_retained_entry` explicitly rejects a stage
  with the original inode or any overlap with the removed source tree. Its one
  transaction records Restored, renames the stage, and deletes receipt/data.
- `winfsp/src/context.rs::StudioFileHandle` retains both `ino` and `fh`.
  `write` uses those original values even for constrained paging I/O. Merely
  changing the destination dentry or copying fresh payload objects cannot make
  that callback address the newly restored inode.
- `pg_engine/retained.rs::apply_retained_authority` serializes receipt terminal
  transitions in PG. Local accepted data cannot establish a new shared winner
  after a peer purge. `janitor.rs` checks shared object references, snapshot
  pins and upload leases: a process-local handle/pin is not itself one of those
  shared protections. An offline mount cannot rely on publishing one in time.
- The current spec explicitly requires fresh inodes for a losing offline
  restore. Retaining the old inode with fresh object IDs would change that
  contract and still requires proving peer deletion/replay cannot remove the
  restored inode. Byte independence alone does not prove namespace survival.

Consequently, the existing combination cannot guarantee all three: restore
before final Close, replacement with a fresh inode, and continued mapped writes
through the old identity reaching the restored file. Increasing the 1.5 s wait
does not resolve this incompatibility. Cleanup cannot serve as final Close;
the earlier native mapped-write evidence already disproves that premise.

Actionable choices, rather than another timeout experiment:

1. Keep fresh-inode restore and explicitly refuse while a source remains open.
   This preserves current data safety but leaves ordinary Shell undelete
   unreliable on this host. It is the current behavior, not completed acceptance.
2. To support those handles, separate local open-file identity from the new
   replicated inode. Before removing the source, atomically hand its live I/O
   authority to the restored inode under the source gates. Every operation on
   existing handles (read/write, EOF/setattr, flush, locks, cleanup/release), plus
   writer state, upload ownership and kernel file identity must agree on that
   handoff. Restart must retain accepted bytes and publish only the new identity.
   A write-only redirect is insufficient. This is a design candidate, NOT an
   established safe patch; no redirect was added in this pass.

The narrower native alternative would be a documented operation proving that
all writable sections are gone before replacement. This pass has no such SDK
evidence; no cache purge/section API is recommended or claimed. `ctx7` was not
available on PATH. The exact external owner retaining the FILE_OBJECT after
Cleanup remains unidentified; this assessment does not attribute it to Mm,
antivirus, Shell, or WinFSP caching.

Verification this pass: graph index status, outbound restore trace, metadata
snippet and exact-path coverage for the evidence files, with direct source
inspection. Focused regression:

    cargo test -p studiofs-vfs --lib retained_restore_gate_orders_admitted_write_before_fence -- --exact vfs::tests::retained_restore_gate_orders_admitted_write_before_fence --nocapture

Result: 1 passed, 0 failed, 1388 filtered out. This proves the existing admitted
write/fence ordering only. No native test, new mount, helper, product-code edit,
commit, or external write was performed. Native failures and all previously
listed acceptance gaps remain open. A coherent handle-identity handoff needs a
red regression where a mapping remains live across successful restore and its
later write is visible through the restored name, followed by offline
purge/restart proof. Neither proof exists for that candidate yet.


### Connected identity-preserving restore implementation — 2026-11-13

This entry supersedes the fresh-inode/Close-fence architecture assessment above.
The user selected original inode identity plus durable independent payloads.
Fresh object IDs are necessary for offline peer-GC safety; fresh inode IDs are
not. Retargeting handles was rejected because it would require changing every
handle operation, kernel cached identity, hardlink and directory handle.

Production path now implemented:

- VFS joins existing deferred materializers, acquires sorted Restore file-session
  permits, drains source local state through the existing strict sync barrier,
  and builds a closed stage payload set. CachedStoreWriter uses
  `submit_upload=false`: stages have durable DIRTY/upload_stage bodies but no
  background stage uploader can publish or consume them during adoption.
- Fork reads require local bodies and take independent backend pins with Drop
  cleanup, including cancellation. No foreground S3 or PG was added. Remote-only
  content absent locally returns RemoteContentPending before accepting restore.
- Local cache sync precedes one redb transaction: verify source fences and exact
  tree/stage mapping, adopt fresh local graphs once per original hardlinked
  inode, detach adopted stage graphs, record Restored, rename original data,
  append existing SetAttr/AddEntry/symlink/xattr/ACL reconstruction events for
  the complete tree, all outside hardlinks and required ancestors, then delete
  stage namespace. Collisions/changed sources roll back the whole transaction.
- Original inode/new epoch owns every submitted upload after commit. Startup
  replay derives that ownership from the committed graph and durable DIRTY
  bodies. No fallible metadata access after commit can convert acceptance into
  stage cleanup. Source permits remain held until postcommit runtime invalidation
  and upload submission finish; admitted writes drain before adoption, queued
  writes afterward still target the restored original inode.
- Receipt loser retirement in existing replication_flush still retires only
  its terminal SetXattr event. Reconstruction/content events remain in the same
  existing pending journal; no parallel journal or remote lease was introduced.
- Original fh/ino, locks and WinFSP cached inode remain unchanged. read/write,
  flush/fsync, setattr/truncate and release keep their existing inode routing.
  The obsolete 1.5-second Close wait is being removed after native mapped proof.

Focused evidence in this pass:

    cargo test -p studiofs-vfs --lib retained_restore_preserves_ -- --nocapture

3 passed, including both original RED writer/hardlink tests.

    cargo test -p studiofs-vfs --lib retained_restore_tree_reopens_with_original_upload_owners -- --exact vfs::tests::retained_restore_tree_reopens_with_original_upload_owners --nocapture

1 passed: nested directory, internal/outside hardlinks, symlink, xattr, later
write, shutdown/reopen, exact bytes and original-inode upload replay ownership.

    cargo test -p studiofs-vfs --lib retained_restore_gate_orders_admitted_write_before_fence -- --exact vfs::tests::retained_restore_gate_orders_admitted_write_before_fence --nocapture

1 passed: accepted write before restore permit remains exact, restore succeeds
with the live handle and original inode.

Native binary rebuilt with `cargo test -p studiofs-winfsp --test mount_sanity
--no-run`; directly ran the resulting mount_sanity binary on owned unused W:
`winfsp_native_mapped_writer_survives_restore_probe --ignored --exact --nocapture`.
1 passed (0.59s), trace target/probe-trace/identity-mapped-restore-final.log:
original file handle closed, same-inode retained restore while writable mapping
remains alive, later constrained paging write, flush/unmap/close, exact reopened
bytes. Windows refused Shell recycle while that mapped section existed, so this
probe deliberately invokes the retained-store transition directly; it does not
claim Shell recycling an active mapping works.

Separate `winfsp_native_trash_restore_and_clear_shell_probe` failed before
restore: Shell issued cleanup-delete without creating an owned $I/$R pair.
Trace target/probe-trace/identity-shell-restore.log. No host recycle clear was
used. Ordinary Shell undelete acceptance remains open; do not count it passed.

Service-backed settled-peer-purge test now expects original inode and deletes
old remote objects before loser publication. First run stopped at original
upload publication (311s). A bounded prerequisite PUT added to the same test
failed/timed out before restore (19.92s total). Thus TCP reachability did not
establish a usable configured S3 service; remote-byte/GC acceptance is blocked
and must not be inferred from local restart proof. No service was started.

Remaining in this pass: finish focused restore-test migration to same-inode
semantics, metadata PG foreign-apply/restart proof without S3, affected clippy,
format/diff checks, and exact final results below. Remaining feature acceptance:
S3 peer-purge/GC exact-byte convergence, native Shell pair/undelete and Clear
receipt settlement, root trash visibility, Linux/macOS and guides. No feature
completion, commit, external application write or full-suite claim.

#### Connected integration verification and remaining boundary

The stronger PostgreSQL regression initially failed after actual peer settlement:
foreign RemoveAttr removed the locally restored directory and destination alias,
even though its reconstruction events were pending. Existing foreign-delete
semantics deliberately override ordinary directory attr overlays. Fixed this in
`RedbMetaEngine::pending_restored_inodes_in_txn` and
`RemoteApplyCoordinator::apply_foreign_batch_in_txn`: use the locally accepted
Restored receipt plus its still-pending destination dentry key to protect the
current restored tree from serving/aux RemoveAttr and RemoveRegularFileGraph.
The shared base still accepts the peer deletion. Protection derives from the
existing local receipt and pending overlay, survives receipt-event retirement
and restart, and ends when the namespace journal is acknowledged. Ordinary
rename/unlink conflicts retain their existing semantics. The scan runs only
for foreign batches removing inodes, with pending namespace state.

Final focused results:

    cargo test -p studiofs-vfs --lib retained_restore_ -- --nocapture

14 passed, 0 failed, 1374 filtered. This includes original RED identity tests,
whole-tree restart/upload ownership, source fence rollback/cleanup, accepted
write ordering, pending last-handle materialization, outside hardlinks,
concurrent restore, source/destination cache identity and stage ownership.
The old timeout/Close-only tests were removed with the obsolete wait; live
writer identity and actual native mapping now test the required behavior.
Metadata-bypass fence tests were updated to mutate source metadata directly:
normal VFS writes are now ordered behind Restore and cannot cross its commit.

    cargo test -p studiofs-vfs --test vfs_test retained_restore_ -- --nocapture

4 passed, 2 explicitly service-dependent ignored, 192 filtered. Existing ACL,
collision, retry-after-parent-removal and directory/hardlink/live-handle tests
now assert original identities and later accepted bytes.

    cargo test -p studiofs-meta --lib redb_engine::retained::tests:: -- --nocapture

15 passed, 1196 filtered. Includes collision rollback, same-inode hardlinks,
concurrent purge, receipt immutability and shared settlement boundaries.

    cargo test -p studiofs-meta --lib test_pg_retained_restore_purge_conflict_preserves_local_loser -- --ignored --exact pg_engine::tests::lifecycle::test_pg_retained_restore_purge_conflict_preserves_local_loser --nocapture

1 passed, 39.62s. Both authority orders; purge winner actually removes original
directory and child, restoring client applies those foreign events and reopens
BEFORE accepted namespace republish, keeps original IDs, retires only losing
receipt, republishes full tree, self-acks/prunes across later restart. Subsequent
peer deletion after acknowledgement succeeds, proving no permanent immunity.
This is real PostgreSQL metadata recovery, not remote payload-byte proof.

    cargo test -p studiofs-meta --lib test_pg_retained_expiry_conflicts_with_offline_restore_like_purge -- --ignored --exact pg_engine::tests::lifecycle::test_pg_retained_expiry_conflicts_with_offline_restore_like_purge --nocapture

1 passed, 23.63s, same identity through expiry settlement/republication.

    cargo test -p studiofs-meta --lib test_pg_retained_publication_response_loss_retry -- --ignored --exact pg_engine::tests::lifecycle::test_pg_retained_publication_response_loss_retry --nocapture

1 passed, 6.75s, existing publication idempotency remains intact with reconstruction.
Other existing PG restore fixtures now supply complete source fences and assert
original identity; their full ignored suite was not run.

    cargo test -p studiofs-meta --lib foreign_unlink_after_local_rename_removes_destination_alias -- --exact pg_engine::tests::remote_apply::foreign_unlink_after_local_rename_removes_destination_alias --nocapture

1 passed: ordinary foreign deletion after local rename remains unchanged.

    cargo clippy -p studiofs-vfs -p studiofs-meta -p studiofs-winfsp --all-targets -- -D warnings

Passed after final production changes. Formatting applied to affected files;
final rustfmt --check and git diff --check results are recorded with handoff.
Native mount_sanity rebuilt again after removing Close wait; mapped restore
passed (0.61s), trace target/probe-trace/identity-mapped-no-close-wait.log. W:
was unmounted and verified unused afterward. No repeated unchanged-build flake
loop or host recycle cleanup was used.

Graph Tier2 exact-path coverage was checked for evidence source files; the MCP
transport later closed during the final remote-apply lookup/coverage refresh.
Those final ranges and the ordinary-delete regression were inspected directly
as the required source fallback, with no exhaustive graph-absence claim.

Historical checkpoint (superseded by the review follow-up below): the connected
production restore path was not feature-complete. The configured S3 PUT failure
was subsequently diagnosed as a refused proxy tunnel, and the settled-purge
remote-byte test now passes with a process-local proxy bypass. Separately
diagnose Shell choosing cleanup-delete before producing any owned
$I/$R pair on its fresh mount; its last failure never reached restore. Root
visibility, true Clear PurgeRequested(User) receipt settlement, platform aliases,
Linux/macOS and guides remain open. Preserve all intentional worktree changes;
no commit, PR, full suite, global recycle clear or external app write was done.

Final handoff checks: affected-file `rustfmt --edition 2024 --check` passed;
`git diff --check` passed; `Test-Path W:\` returned False.


### Review follow-up and user-requested overnight checkpoint — 2026-11-13

This section is the latest handoff and supersedes earlier unresolved S3 and
review-finding status. The user explicitly requested that work stop at the
current coherent checkpoint until they resume it tomorrow. Finish only current
verification, preserve the dirty checkout, and launch no further implementation,
review, delegation, service acceptance or native probes after that checkpoint.
No commit, push, PR, full suite or external application write is authorized.

Repository: the repository root; branch
`feat/sfs-1054-recoverable-trash`; HEAD
`0358242f3c1e90959faf66e0be0cff95f424b47c`; merge-base with the locally
available `origin/main` is `291a4a392a5e36d88e4d498523df639066fb514f`.
No fetch was performed. Extensive tracked/untracked feature changes predate
this turn and remain intentional; the HEAD diff is not this turn's isolated diff.

Progress and decisions:

- [x] F1 recursive ancestor authority: real PG RED showed an offline restore
  restoring ancestor ACL `ReadWrite` after the peer changed it to `ReadOnly`.
  `journal_restored_tree_in_txn` now iterates only restored source inode IDs.
  It retains their actual merged dentries (including outside hardlinks), source
  xattrs/ACLs and symlink targets, and does not enqueue ancestors recursively.
  The PG test also covers ancestor chmod, moving the ancestor without recreating
  its old name, and preserving the restored subtree's own ACL. Latest GREEN:
  1 test, 9.53s. Direct destination-parent authority under the pre-existing
  ordinary RenameEntry parent-attr conflict rules was not separately tested;
  do not generalize this proof into all concurrent ACL/rename cases.
- [x] F2 competing restore names: `reconcile_losing_retained_restore` handles only
  a proven shared Restored winner at another destination. It atomically replaces
  the rejected, unpublished restore group's terminal marker, root RenameEntry
  and root destination AddEntry with existing local-only RemoveLocalEntry events,
  normalizes the restored directory's parent, removes only the exact wrong local
  binding, and preserves content, internal/outside hardlinks and later journal
  operations. The group is bounded by its terminal marker and the stage root's
  final postorder RemoveEntry. Event sequence numbers are preserved. A different
  terminal shared winner proves that this marker could never have been accepted
  into any successful PG publication, so only this rejected suffix is rewritten;
  successful packet history is not rewritten. The local receipt adopts the
  shared winner in the same transaction. Purge losers keep full reconstruction.
  The classifier invokes this before retiring the terminal record.
- [x] F2 source ownership: `RetainedSourceFence` includes `content_epoch` as well
  as ctime/size. The injected epoch-only race was RED, then GREEN. Validate the
  complete source set before the first adoption: validating a second hardlink
  after adopting its first alias incorrectly rejected the transaction's own
  epoch increment (found by the final focused restore group; corrected here).
- [x] Port/API cleanup: `MetaRetainedStore::restore_retained_entry` no longer
  takes unused `is_open`; it returns `RetainedRestoreResult { entry,
  upload_epochs }`. Redb captures adopted original-inode epochs in the atomic
  restore result; VFS uses these actual epochs for upload ownership rather than
  predicting `old_epoch + 1`. The VFS public restore still returns RetainedEntry.
  Pg, read-only adapters, test helpers and the CLI dump/load fixture were updated.
- [x] F3 progress: missing local bodies still return RemoteContentPending
  (bridge Retryable). Only after source permits and metadata transactions have
  been released does restore enqueue missing object IDs into existing bounded
  prefetch ingress. `PrefetchWorkItem::RestorePayload` uses the existing owned
  JoinSet/semaphore and FetchScheduler prefetch/singleflight/cache path, without
  read-ahead relevance filtering or foreground S3 waits. The unit test was RED
  with zero queued requests and is GREEN after enqueue plus simulated cache
  completion/retry. The real S3 settled-purge test separately evicts the old body,
  observes the initial Retryable, waits on cache presence for background fetch,
  and retries with exact bytes. Native Shell automatic retry/UX is NOT proven.
- [x] Real S3 diagnosis: the redacted actual error chain was `client error
  (Connect): tunnel error ... tcp connect error ... actively refused ...
  os error 10061`. A process-local `NO_PROXY=*` / `no_proxy=*` bypass makes PUT
  work. No host environment/proxy configuration was changed. The earlier claim
  that configured S3 itself was unavailable is superseded.
- [x] Connected purge-byte fixes: actual service RED exposed a restored peer file
  as Updating with a Hole graph. A fresh durable payload fork now clears its old
  committed size/epoch; its old published generation may already be purged.
  Projection follows the latest committed marker, and holds a restore window
  containing surviving hidden namespace until fresh payloads are ready. This
  prevents trailing stage cleanup from acknowledging hidden recovery names.
  Required local replay alone is not a readiness predicate: historic LocalOnly
  overlay events may still exist after the fresh generation becomes Ready.
- [x] Foreign RemoveAttr previously grandfathered pending restored files as
  Ready despite mismatched content/committed epochs. This caused the existing
  finalizer to skip the fresh generation permanently. Keep the actual upload
  state for pending restored inodes; ordinary non-restore grandfathering remains.
- [x] The expanded F2 PG proof found an additional empty-file case: an empty
  complete publication was classified as partial, suppressing a later hardlink
  nlink update even though its dentry published. Treat ready committed size zero
  as a complete empty extent. The cross-parent/restart/hardlink test is GREEN:
  1 test, 11.80s. It checks a single directory destination, original directory
  parent, original child identities, outside and subsequently accepted hardlinks,
  nlink counts and empty loser-parent nlink after publication and restart.
- [x] Exact invalid UTF-8 repaired: the plan had one lone byte 0x97 at original
  byte offset 209605 / line 3367. Replaced only that sequence with UTF-8 em dash
  E2 80 94; the entire plan now decodes strictly as UTF-8.

Changed production interfaces/files this turn: meta `trash.rs`,
`redb_engine/retained.rs`, `pg_engine/retained.rs`, `pg_engine/volume_settings.rs`,
`pg_engine/remote_apply.rs`, `read_only.rs`; VFS `vfs/io_ops.rs` and
`vfs/prefetch.rs`. Test/fixture updates: meta `pg_engine/tests/lifecycle.rs`,
`pg_engine/tests/remote_apply.rs`, VFS `vfs/tests.rs`, `tests/vfs_test.rs`, CLI
`cli/dump_load.rs`. This plan is updated; other feature files remain preserved.

Focused verification (run from repository root; no whole suite):

    cargo test -p studiofs-meta --lib retained_restore_does_not_republish_ancestor_authority -- --ignored --nocapture
    cargo test -p studiofs-meta --lib retained_competing_restores_keep_only_winner_destination_after_restart -- --ignored --nocapture
    cargo test -p studiofs-vfs --lib retained_restore_rejects_epoch_change_during_copy -- --exact vfs::tests::retained_restore_rejects_epoch_change_during_copy --nocapture
    cargo test -p studiofs-vfs --lib retained_restore_missing_body_queues_background_fetch_and_retries -- --nocapture
    cargo test -p studiofs-meta --lib restore_payload_fork_hides_old_committed_identity_until_fresh_publication -- --nocapture
    cargo test -p studiofs-meta --lib restore_keeps_namespace_journal_pending_until_all_fresh_payloads_are_ready -- --nocapture
    cargo test -p studiofs-meta --lib discarded_restore_stage_overlay_never_projects_shared_payload_reference -- --nocapture
    cargo test -p studiofs-vfs --lib retained_restore_empty_file_queues_committed_epoch_for_publication -- --nocapture

Each listed filter ran exactly one nonzero test and passed after its correction
(the empty-file finalization test was GREEN without an additional production
queue change; existing VFS scheduling already provides that finalization).
The stage projection test establishes that LocalOnly stage overlay plus its
RemoveAttr never projects a shared payload reference; it is not a live SQL
assertion about GC candidates.

    $env:NO_PROXY='*'
    $env:no_proxy='*'
    cargo test -p studiofs-vfs --test vfs_test retained_restore_losing_to_settled_peer_purge_preserves_inode_and_bytes -- --ignored --exact --nocapture

GREEN, 1 test, 16.14s before final diagnostic cleanup; current final rerun is
recorded below. Real configured PG/S3, isolated test volume/prefix only: source
publication, cache eviction, first restore Retryable and actual background fetch,
settled peer purge, accepted same-inode restore, removal of old test-owned cloud
objects, actual foreign apply, fresh epoch publication and exact peer bytes.
Old keys were removed directly to emulate the remote reclamation effect; this
is not a real janitor physical-deletion execution. No database/VFS restart occurs
inside this particular byte-backed test yet. Earlier metadata-only restart proof
and local-cache upload-owner restart proof must not be reported as that missing
combined PG/S3/purge/GC/restart proof.

An early command after a failed Python edit ran zero tests; it is invalid evidence
and was replaced by the successful nonzero filter runs above. Several intentional
RED stages occurred while diagnosing the real service behavior; do not treat
those intermediate runs as the current acceptance result.

The first final `retained_restore_` group ran 17 tests: 16 passed, one RED
`retained_restore_tree_reopens_with_original_upload_owners` found self-induced
hardlink epoch rejection described above. Its final corrected result follows.
Current final validation results/process state are appended below after completion.

Remaining work for the next authorized session, not tonight:

1. Add combined accepted-before-publication VFS+metadata restart to the real
   settled-purge/S3 test, prove fresh DIRTY upload ownership resumes, and assert
   actual SQL `snapshot_current_references` has no stage inode reference and
   `snapshot_payload_gc_candidates` has no candidate for adopted fresh objects.
   Keep exact test-owned keys/volume only; no user bucket/global recycle cleanup.
2. Explicit adversarial transport batch splitting, crash between losing-restore
   reconciliation and retirement, retry/idempotence, same-receipt competing
   restores with real nonempty payload bytes and concurrent destination/ancestor
   ACL changes. The current cross-parent PG test is not all of these proofs.
   Assess specialized unsent-journal rewriting against those exact boundaries.
3. Direct destination-parent security/rename conflict test (ordinary RenameEntry
   embeds parent attrs); current F1 proof covers the recursive ancestor bug only.
4. Rebuild before any counted native acceptance. Current native mapping proof
   and failed Shell trace are historical from the preceding slice. Diagnose the
   actual cleanup-delete/$I/$R admission prerequisite before claiming Shell
   undelete acceptance; mapped restore is not Shell restore. Remote-only native
   retry UX also remains unproven. Use only unused owned W: and exact owned pair.
5. Known `root_readdir_hides_trash_like_directories` RED remains unresolved and
   was not rerun tonight. True Clear must prove PurgeRequested(User), not merely
   disappearance of one $I/$R pair. Directory aliases/platform mappings,
   Linux/macOS mounted acceptance and guides remain unfinished. Feature is not
   complete, and review/PR/CI delivery remains outside this stopped checkpoint.

Graph Tier2 index/coverage requests failed with MCP `Transport closed`.
The CLI fallback also failed `secure CLI coordination could not be created
(endpoint)`; direct source inspection was used for all relied-on ranges.
No fresh exhaustive graph-coverage claim is made. ctx7 CLI was unavailable;
this slice changes internal business logic and adds no third-party API contract.

Final checkpoint verification:

    cargo test -p studiofs-vfs --lib retained_restore_ -- --nocapture

17 passed, 0 failed, 0 ignored, 1374 filtered, 0.35s, after moving complete
source-fence validation before adoption. This includes original-inode upload-owner
restart, internal/external hardlinks, live later writer bytes, epoch-only injected
race, missing-body enqueue/retry, gate ordering and rollback/stage ownership.

    cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings
    cargo check -p studiofs --tests
    cargo fmt --all -- --check
    git -c core.safecrlf=false diff --check

All passed (CLI test-build: 1m06s; final Clippy after fence-order correction:
12.96s). Formatting/diff checks passed. `Test-Path W:\` returned False; no
cargo/rustc, StudioFS, mount_sanity or legacy powershell helper remained when
checked. No mount or native helper was started in this turn, and no host recycle
cleanup was done. All test-owned helper work completed through existing test
lifecycle cleanup. The final service rerun below finishes before handoff; do not
start any next-area work after it. Preserve all existing tracked/untracked files.

Final rebuilt service rerun:

    $env:NO_PROXY='*'; $env:no_proxy='*'
    cargo test -p studiofs-vfs --test vfs_test retained_restore_losing_to_settled_peer_purge_preserves_inode_and_bytes -- --ignored --exact --nocapture

Passed: 1 passed, 0 failed, 0 ignored, 197 filtered, 15.13s (build 31.92s).
The proxy bypass was process-local; no host configuration was changed. This is
actual configured PG/S3 foreground-local restore, background-fetch and peer-purge
byte protection evidence, with the combined restart/SQL/native limitations above.

Stopped at this reviewable checkpoint at the user's explicit request. No further
finding, feature area, native run, commit, push or PR is started. Resume only when
the user requests it; preserve all current changes for tomorrow.


### Continuation checkpoint (Slices 1, 2, dest-parent ACL, and native Windows Shell root visibility) — 2026-11-14

Resumed from the stopped checkpoint above on branch `feat/sfs-1054-recoverable-trash`
(`HEAD 0358242f3c1e90959faf66e0be0cff95f424b47c`). Preserved all existing tracked and
untracked work; performed no commits, pushes, PRs, Linear writes, or host Recycle Bin clears.

1. **Slice 1 — Combined restart + real peer janitor + SQL verification (`crates/studiofs-vfs/tests/vfs_test.rs`)**:
   - Extended `retained_restore_losing_to_settled_peer_purge_preserves_inode_and_bytes`
     (with `upload_delay_ms = 60_000` across pre-publication VFS+metadata restart) to prove
     fresh `Dirty` upload ownership resumes on reopen, stage-inode journal check excludes
     `SetRegularFileOverlaySlices`/`RemoveAttr`, a real mounted peer janitor reclaims old
     test-owned S3 objects (replacing manual S3 object deletion emulation), foreign apply
     and republish preserve exact bytes on both peers, and SQL assertions confirm
     `snapshot_current_references` has 0 stage-inode references and
     `snapshot_payload_gc_candidates` has 0 candidates for adopted fresh objects.

2. **Slice 2 — Adversarial competing-restore/crash boundaries & destination-parent ACL authority**:
   - Fixed single-file stage removal (`MetaEvent::RemoveEntry | MetaEvent::RemoveLocalEntry`)
     and competing-restore loser payload fork neutralization (`SetAttr { RegularFile }` plus
     `(PublishRegularFileBaseSlices, SetAttr)` at `restore_epoch`, clearing unpublished
     `REGULAR_FILE_OVERLAY_TABLE` entries and resetting `BASE_INODES_TABLE` size/epoch/upload
     state) in `crates/studiofs-meta/src/redb_engine/retained.rs`.
   - Refactored `PgMetaEngine::prepare_self_batch_in_txn` via `SelfBatchPrepareInput<'a>` in
     `crates/studiofs-meta/src/pg_engine/remote_apply.rs` to satisfy `clippy::too_many_arguments`.
   - Verified redb tests (`redb_engine::retained::tests::`, 17 passed) and real PG tests:
     - `pg_engine::tests::lifecycle::retained_competing_restore_crash_between_reconcile_and_range_completion_retries_and_converges` (1 passed, 13.68s)
     - `pg_engine::tests::lifecycle::retained_competing_restores_nonempty_payload_with_concurrent_dest_and_ancestor_acl_attr_mutations` (1 passed, 13.36s)
     - `pg_engine::tests::lifecycle::retained_competing_restores_keep_only_winner_destination_after_restart` (1 passed, 11.36s)
     - `pg_engine::tests::lifecycle::rename_and_restore_do_not_republish_stale_parent_attrs_or_acl` (1 passed, 9.15s)
     - `pg_engine::tests::replication_backlog::split_publication_self_ack_survives_reopen_between_pages` (1 passed, after `SelfBatchPrepareInput` refactor)

3. **Native Windows Shell `$RECYCLE.BIN` root visibility & receipt-verified Clear**:
   - Diagnosed `readdir_entries_and_attrs` in `crates/studiofs-vfs/src/vfs/namespace_ops.rs`
     calling `meta.readdir(ino)?` **before** `self.ensure_trash_root_when_enabled()?` when
     `ino == ROOT_INO`, omitting `$RECYCLE.BIN` on the first root listing (`read_directory ino=1 count=1`
     in `target/probe-trace/diag-shell-restore-new.log`) and caching the incomplete snapshot in
     WinFSP `DirectorySnapshotCache`.
   - Diagnosed `ensure_root_directory` in `crates/studiofs-vfs/src/vfs/retained_views.rs` calling
     `meta.create_dir_with_ino` without invalidating root `attr_read_cache`, `dentry_cache`, or
     parent directory generations.
   - Moved `ensure_trash_root_when_enabled()` before `namespace_cache_token_for_parent(ino)` and
     `meta.readdir(ino)` in `crates/studiofs-vfs/src/vfs/namespace_ops.rs`, added root cache
     invalidation in `ensure_root_directory`, and added unit test
     `first_root_readdir_includes_trash_root_and_bumps_cache_token` in
     `crates/studiofs-vfs/src/vfs/retained_views.rs`.
   - Strengthened `winfsp_native_trash_restore_and_clear_shell_probe` in
     `crates/studiofs-winfsp/tests/mount_sanity.rs` to assert actual metadata receipt transitions
     (`RetainedEntryState::Restored` and `RetainedEntryState::PurgeRequested { reason: RetainedPurgeReason::User }`)
     rather than only PowerShell `cleared_pairs=0`.
   - Note: historical `target/probe-trace/identity-shell-restore.log` (`2026-11-13 14:46:38Z`)
     predated the removal of the bounded Close-wait; after rebuilding `mount_sanity` on the current
     codebase, `winfsp_native_trash_restore_and_clear_shell_probe` (0.99s, trace
     `target/probe-trace/diag-shell-restore-fixed.log` confirming `read_directory ino=1 count=2`
     on the first call), `winfsp_native_trash_retention_shell_probe` (1.09s),
     `winfsp_default_disabled_trash_is_destructive_shell_probe` (0.98s), and
     `winfsp_native_mapped_writer_survives_restore_probe` (0.59s, trace
     `target/probe-trace/diag-mapped-restore-fixed.log`) all passed on unused owned `W:`, with
     `Test-Path W:\` returning `False` after every run.

4. **Remaining open items & honest boundaries**:
   - `root_readdir_hides_trash_like_directories` (`crates/studiofs-vfs/src/vfs/tests.rs:23183`)
     remains known RED because `show_trash_root` follows `$RECYCLE.BIN` existence at root even
     when period is `0` to keep previously retained views reachable.
   - Clear in `RECYCLE_RESTORE_PS1` uses PowerShell `Remove-Item` on the exact owned `$I`/`$R`
     pair (`$R` unlink -> `PurgeRequested(User)`); global Windows Explorer Empty Recycle Bin /
     `SHEmptyRecycleBinW` is intentionally not executed against the host.
   - Remote-only native retry UX, directory aliases / cross-platform mappings, Linux/macOS mounted
     acceptance, and EN/UK user guides remain open.
   - Codebase Memory MCP was unavailable (`Transport closed` / direct source inspection used instead).

5. **Final post-edit validation**: recorded at completion below (VFS unit tests, Clippy, CLI `--tests`
   check, `cargo fmt --check`, `git diff --check`, and `Test-Path W:\ == False`).

### Continuation checkpoint — losing-restore reconciliation review (2026-11-14)

Scope was narrowed to F1/F2 only. Preserve the shared worktree; no commits, pushes, PRs,
Linear writes, full suites, or host recycle cleanup were performed.

- **F2 marker ordering:** added `retained_restore_receipt_marker_precedes_adopted_payload_journal`
  in `crates/studiofs-meta/src/redb_engine/retained.rs`. Before the production ordering change,
  it observed the receipt marker at journal index 7 and adopted overlay at index 5 (RED). Moved
  the `Restored` receipt write before regular-file payload adoption inside the same redb write
  transaction. The focused unit then passed 1/1. This puts adoption events inside the restore
  reconciliation range without exposing a partially committed restore.
- **F1 exploratory PG harness:** added ignored PG cases for a post-restore B payload write in
  both uncommitted and committed variants, including the outside hardlink identity and terminal
  Ready/size/epoch checks. The last exact command was
  `cargo test -p studiofs-meta retained_losing_restore_unpublished_post_restore_write_survives_winner_apply -- --ignored --nocapture`
  with process-local `NO_PROXY=*` and `no_proxy=*`. It failed because the pending epoch did not
  advance after `flush_due_pending`; `target/sfs1054-f1-unpublished-debug.log` records the
  receipt/adoption/stage-removal journal range followed by B overlay events. The test did not
  establish which internal conflict-classification branch ran. Therefore this is **not** a valid
  RED of the F1 reconcile path and does not establish a production F1 regression. Temporary
  event-print instrumentation was removed. Keep the two tests as exploratory until the
  caller/classification path is exercised.
- **Unresolved F1 trace/design:** inspect the real selected-batch/foreign-apply path that invokes
  `losing_retained_terminal_records`, then make the focused test reach it without treating
  `flush_due_pending`'s return value as a conflict oracle. The current reconciliation edits are
  provisional and have no F1 GREEN proof. Any solution must protect accepted same-epoch dirty
  VFS state that has no durable payload journal yet, preserve concurrent winner-side writes,
  retain exact payload bytes and alias identity across restart, and avoid silently retiring or
  stranding pending/upload ownership. A journal-suffix guard alone cannot distinguish an
  unjournaled accepted writer.
- **F1 acceptance still required:** actual VFS write/fsync, foreign Ready apply/refresh before
  local-dirty flush, payload retry evidence, restart, exact bytes on both peers, and separate
  committed/uncommitted plus outside-hardlink coverage. Do not count historical VFS runs or the
  exploratory PG failures as proof.
- **Current boundary:** F2 is the only green slice from this continuation. F1 remains open for
  escalation. F3–F5 were not started in this continuation.
- **Checkpoint validation:**
  - `cargo test -p studiofs-meta retained_restore_receipt_marker_precedes_adopted_payload_journal -- --nocapture` — 1 passed, 0 failed; test target compiled with the current retained code.
  - `cargo fmt --all -- --check` — passed.
  - `git -c core.safecrlf=false diff --check` — passed.

### Continuation checkpoint — F1 accepted writes on a losing restore (2026-11-14)

This supersedes the exploratory F1 status above for metadata reconciliation only.
Work stayed on `feat/sfs-1054-recoverable-trash` at `0358242f`, preserving the
intentional tracked/untracked worktree. No commits, pushes, PRs, Linear mutations,
full suites, mounted runs, or host cleanup were performed in this slice.

#### Progress

- [x] Exercise the actual `PgMetaEngine::losing_retained_terminal_records` classifier
  against a committed shared winner, asserting exactly one losing terminal record.
- [x] Preserve a later same-epoch accepted payload graph before and after local
  publication, including untouched slices and another chunk, outside hardlink,
  winner-only destination, local reachability and pending lifecycle, restart before
  winner apply, republish, peer convergence, and both-peer restart.
- [x] Prove a pristine losing restore, including its upload-completion events,
  does not supersede a winner's later partial write at a newer epoch.
- [x] Prove metadata-only size extension survives local classifier/restart/winner
  replay; this case deliberately stops before remote sparse publication.
- [ ] Exact-byte VFS accepted-but-unjournaled write acceptance is being handled
  independently; metadata object-ID tests alone do not prove it.

#### Surprises and discoveries

Direct classifier invocation made the previously exploratory published-write case
fail after winner apply: serving object `92001` replaced accepted object `94001`
(`target/sfs1054-f1-classifier-published.log`). Reconciliation had rebased the Ready
attribute but left accepted bytes only in the private base. Foreign apply replaces
base; local `PublishRegularFileBaseSlices` replay does not reinstall its payload
rows. Merely increasing the Ready epoch therefore did not preserve the bytes.

The pristine-B control then failed because upload completion also journals
`SetRegularFileOverlaySlices`. Treating every later overlay event as a write
rebased an untouched restore over the winner's later content. The final classifier
compares canonical resolved payload ranges with adoption records, ignoring
slice identity, equivalent splitting/overlap and upload state, and also recognizes changed file size. Completion-only overlay
records are neutralized with the rejected pristine fork.

A synthetic bare metadata size extension has no VFS sparse-hole journal. It
survived local reconciliation but did not establish remote sparse publication
convergence, so its test is explicitly named
`retained_losing_restore_size_only_extension_survives_local_winner_replay` and
asserts that local boundary only. Truncate-to-zero and native/mounted behavior were
not demonstrated by this metadata-only control.

#### Decision log

Keep complete accepted content in the existing overlay machinery rather than
invent a second publication path. `redb_engine/retained.rs` snapshots the complete
current overlay (unpublished) or private base (published), neutralizes obsolete
same-epoch content records, and calls the existing
`apply_regular_file_overlay_mutation_in_txn` at the next pending epoch in the same
transaction. This preserves payload upload states, updates lifecycle/recovery
roots, and journals complete slices before the pending attribute. Ordinary upload
completion and overlay publication then converge that graph. The shared receipt
still owns the winner destination; content preservation never recreates the loser
name. Existing same-epoch Ready rebase/retry in VFS `local_dirty.rs` remains the
route for accepted bytes that have not reached metadata yet; its proof is separate.
F2's receipt-marker-before-adoption ordering remains intact.

#### Metadata validation and outcomes

All PG commands below used process-local `NO_PROXY=*` and `no_proxy=*`; configured
test helpers load `.env` without printing credentials. Graph discovery found the
real classifier caller in `flush_pending_claimed_batch`; exact-file coverage checks
returned `no_recorded_issue`/`metadata_match`, with direct source inspection for the
material event and replay behavior.

    cargo test -p studiofs-meta retained_losing_restore_ --lib -- --ignored --nocapture

Passed 4 tests, 0 failed, 1226 filtered, 14.88s; build 14.59s. Log:
`target/sfs1054-f1-final-pg.log`. Three cases prove complete two-chunk payload
convergence/restart; the fourth proves only local size-extension replay as named.
The unpublished case retains LocalOnly objects through reconciliation/restart,
asserts publication is still blocked, marks the new-epoch objects uploaded, then
publishes. Uploaded objects keep their state and can republish without fabricated
local bodies. Reachability and pending lifecycle are asserted before publication.

    cargo test -p studiofs-meta redb_engine::retained::tests:: --lib -- --nocapture

Passed 18 tests, 0 failed, 1212 filtered, 0.09s. Includes F2 ordering and existing
namespace reconciliation/idempotence coverage. Log: `target/sfs1054-f1-redb.log`.

The feature as a whole remains incomplete. F3-F5, service-backed physical GC,
mounted/native acceptance, and remote sparse size-only publication are outside
this metadata slice. Focused metadata check/Clippy and existing competing-restore
regressions are recorded below when completed.
Compaction follow-up within F1: background GC calls metadata chunk compaction
without an upload-epoch gate (`vfs/workers.rs`); compaction reassigns slice IDs and
orders and may merge equivalent pieces (`redb_engine/chunk_state.rs`), while
leaving payload object references unchanged. A real pristine-B compaction control
failed with the first graph comparison: B's old `93001/93002` replaced the winner's
`92001/94001/92002` (`target/sfs1054-f1-compaction-red.log`, one failed test).
The final comparison uses the existing chunk compactor to canonicalize resolved
range/payload mappings after normalizing lifecycle states. Slice identities,
equivalent splits/overlaps, upload completion and compaction therefore do not
count as accepted content changes. The unchanged pristine-B control now performs
real compaction and converges the winner's later bytes. The four-case PG run then
passed 4/4 in 15.25s (build 19.28s); final checks after the empty-chunk equivalence
cleanup are recorded next.

Pristine restore cannot create a payload-object rewrite through deferred writer
materialization: the aggressive worker enumerates writer-stack dirty inodes
(`vfs/workers.rs:696`), and `run_deferred_materialization_pass` returns Idle when
`dirty_before == 0` (`vfs/sync_ops.rs:473`). Actual buffered writes do supply dirty
bytes and are handled by the separately validated VFS path. The metadata
compactor itself rewrites only graph rows and preserves payload references.
Final post-compaction metadata validation (current source):

    cargo test -p studiofs-meta retained_losing_restore_ --lib -- --ignored --nocapture

Passed 4/4, 0 failed, 1226 filtered, 15.12s (build 13.35s), including actual
compaction of pristine B and LocalOnly B restart/upload retry. Log remains
`target/sfs1054-f1-final-pg.log`.

    cargo test -p studiofs-meta redb_engine::retained::tests:: --lib -- --nocapture
    cargo test -p studiofs-meta retained_competing_restore --lib -- --ignored --nocapture

Passed 18/18 in 0.09s and 4/4 in 24.07s respectively. The latter covers winner
namespace/restart, reconciliation crash/idempotence, destination/ancestor ACL
mutations, and split losing remainders. Logs: `target/sfs1054-f1-redb.log` and
`target/sfs1054-f1-existing-pg.log`.

    cargo check -p studiofs-meta --tests
    cargo clippy -p studiofs-meta --all-targets -- -D warnings
    rustfmt --edition 2024 --check crates/studiofs-meta/src/redb_engine/retained.rs crates/studiofs-meta/src/redb_engine/payload_objects.rs crates/studiofs-meta/src/pg_engine/tests/lifecycle.rs
    git -c core.safecrlf=false diff --check

Passed (check 5.13s; Clippy 4.41s). Logs: `target/sfs1054-f1-check.log` and
`target/sfs1054-f1-clippy.log`. A concurrent global `cargo fmt --all -- --check`
reported only the independently edited VFS fixture's line wrapping; its owner/root
must perform the final global format check after their edits settle. Metadata
files pass their direct rustfmt check. No claim of complete F1/VFS acceptance,
whole-feature completion, mounted behavior or full-suite success is made here.
### R2 follow-up — avoid an already-published winner epoch (2026-11-14)

The scoped review found that local `current_epoch + 1` can equal a winner's
already-published later epoch. Added a PG case where A restores at E, writes and
publishes at E+1, then B accepts/publishes a partial write at its independently
allocated E. Actual losing-terminal classification and restart observed B pending
at E+1, failing the explicit greater-than-shared-winner assertion (1 failed,
11.44s; `target/sfs1054-r2-winner-epoch-red.log`). This is a numeric content-identity
collision even if a later same-epoch conflict rule happens to retain the bytes.

The shared schema has no scalar inode-epoch projection. Reused the validated
bootstrap checkpoint reader (`metadata_io.rs`, sibling visibility only), its
frozen remote head/checkpoint cut, existing bounded foreign-event pagination, and
existing batch-continuity decoder. The background classifier now derives maximum
published epochs for candidate regular-file inodes from checkpoint attributes
plus SetAttr events through that frozen head, then passes those bounds to redb.
The existing pending-overlay reconstruction chooses
`max(current_epoch, observed_shared_epoch) + 1`. A pruning gap or descriptor error
fails classification closed and leaves the journal pending for its ordinary
retry. No new SQL, shared schema, foreground PG dependency, or publication path
was added. Concurrent publications after the observed cut retain the existing
multiwriter conflict rules; this change avoids the already-observed winner epoch.

The two new R2 variants assert actual distinct sources: the tail variant limits
pages to one batch and proves checkpoint_version < remote_head_version; the
checkpoint variant flushes A's checkpoint, asserts it reaches remote head, and
decodes A's E+1 attribute from that checkpoint. Both then prove B's epoch is
strictly greater, full two-chunk partial-write/hardlink content convergence,
winning destination, pending lifecycle, and both-peer restart. VFS read-cache
acceptance remains separate R1 work.

    cargo test -p studiofs-meta retained_losing_restore_ --lib -- --ignored --nocapture
    cargo test -p studiofs-meta redb_engine::retained::tests:: --lib -- --nocapture
    cargo check -p studiofs-meta --tests
    cargo clippy -p studiofs-meta --all-targets -- -D warnings

Current results: 6/6 PG (16.79s, build 14.13s), 18/18 redb (0.09s), check passed
8.65s, Clippy passed 10.78s. Logs: `target/sfs1054-r2-final-pg.log`,
`target/sfs1054-r2-redb.log`, `target/sfs1054-r2-check.log`, and
`target/sfs1054-r2-clippy.log`. Direct rustfmt check of the four changed metadata
files and `git diff --check` passed. F2 and canonical compaction controls remain
green. No VFS source/test edits were made during R2, and no commits/external
mutations/full suites were performed.
### 2026-11-14 R1 accepted unjournaled payload versus newer Ready EOF

- Repaired the existing ignored VFS fixture `retained_restore_pg_winner_base_apply_preserves_unjournaled_partial_write_exactly` instead of adding another fixture. Distinct `winner.bin`/`loser.bin` receipts enter the actual failed-publication losing classifier; a fresh third peer proves the winner's nonempty committed graph/epoch is shared. B promotes its restored graph before accepting a writer-stack-only tail (NEXT_EVENT_SEQ unchanged). A advances twice and publishes a shorter Ready EOF31; B accepts bytes beyond its old EOF before applying A.
- Valid RED: `cargo test -p studiofs-vfs --test vfs_test retained_restore_pg_winner_base_apply_preserves_unjournaled_partial_write_exactly -- --ignored --exact --nocapture`, with process-local NO_PROXY/no_proxy=`*`, `target/sfs1054-r1-fixture-red.log`: 1 failed, 13.28s. Exact read before dirty flush succeeded; after flush length31 versus expected65889. `LocalDirtyFlushSession::commit_pending_slice_snapshot` treated newer Ready/shorter EOF as stale cleanup despite accepted pending bytes.
- Minimal correction: reuse existing complete-Ready accepted-payload rebase for current epoch >= pending epoch (previously equality only). The overlay receives current+1 and retains dirty extent/size; the Uploading/truncate cleanup branch is unchanged. No foreground remote IO or new mechanism.
- Final same command GREEN: `target/sfs1054-r1-fixture-green.log`, 1 passed, 16.71s. Exact bytes checked before/after dirty flush and B local restart; hardlink identity retained, B epoch strictly exceeds A. A warms its old winner read before B publication; B drains actual publication, A pulls committed B epoch and reads exact65889 bytes through its existing VFS/cache. Local object bodies are copied between fixture caches to simulate completed uploads; this proves PG/local-body publication and cache invalidation, not S3 transport or mounted/native acceptance.
- Controls: `cargo test -p studiofs-vfs --lib pending_sparse_slice_commit_ -- --nocapture`, `target/sfs1054-r1-pending-controls.log`: 4 passed, 0.12s (same-epoch Ready, larger same-epoch EOF, growth, newer in-flight metadata).
- Checks: `cargo check -p studiofs-vfs --tests` (`target/sfs1054-r1-check.log`) green; `cargo clippy -p studiofs-vfs --all-targets -- -D warnings` final (`target/sfs1054-r1-clippy-final.log`) green. Direct rustfmt for local_dirty.rs/vfs_test.rs and git diff --check green. Full suites not run. R2 metadata checkpoint/tail proofs remain as documented above; F3-F5 remain separate.

### R1/R2 review approval and LOW follow-up (2026-11-14)

Fresh Opus medium review approved R1/R2 without blockers and accepted F1 in its
stated PG/local-body scope. F2 remains approved. The review was static, using
root-verified logs; it did not run tests or approve the whole feature.

Addressed the two LOW follow-ups in the smallest existing seams:

- `shared_retained_content_epochs` now gives each event page its own command
  timeout, rather than timing the entire frozen-head scan as one command. An
  empty decoded page before the frozen head returns `ReplayGap` immediately;
  pruning can no longer cause a query loop without cursor progress. The cut,
  decoding and continuity validation remain unchanged. No fault-injected empty
  page test was added; that guard was verified statically.
- Added `pending_sparse_slice_commit_rebases_inside_old_eof_against_newer_ready_eofs`:
  a write inside B's old EOF survives both a newer Ready EOF covering the write
  and a newer Ready EOF shorter than the write. It checks the new epoch, EOF and
  exact bytes including zero-filled holes. One focused test, two cases: passed
  in 0.13s. This is a local synthetic-Ready control, not mounted/remote evidence.
- Ran the existing `stale_pending_slice_commit_with_dirty_attr_preserves_newer_metadata`
  control separately: 1 passed in 0.04s, confirming the previously omitted test.

Current paging acceptance:

    cargo test -p studiofs-meta retained_losing_restore_published_write_advances_past_ --lib -- --ignored --nocapture

Two tests passed in 16.41s (tail with one-batch pages and checkpoint source),
log `target/sfs1054-r2-paging-followup.log`. PostgreSQL credentials were imported
without printing; NO_PROXY/no_proxy were process-local. VFS commands:

    cargo test -p studiofs-vfs --lib pending_sparse_slice_commit_rebases_inside_old_eof_against_newer_ready_eofs -- --nocapture
    cargo test -p studiofs-vfs --lib stale_pending_slice_commit_with_dirty_attr_preserves_newer_metadata -- --nocapture

F3-F5 and whole-feature acceptance remain open. F4 is assigned independently to
Luna xhigh (read-only/snapshot root listing and lookup); it does not modify these
paging or local-dirty controls. No commits, pushes, PR or global cleanup occurred.

### F4 read-only root creation correction (2026-11-14)

Luna xhigh reproduced lazy creation of `$RECYCLE.BIN` during root readdir through
`new_read_only_with_snapshot_mount_fence` with positive TTL. The shared
`ensure_trash_root_when_enabled` now returns before TTL lookup/reservation/create
when `read_only_mount` is set. Production snapshot mount construction uses this
constructor; bootstrap sets that flag. Existing metadata remains readable.

    cargo test -p studiofs-vfs --lib read_only_root_views_do_not_create_missing_trash_root -- --nocapture
    cargo test -p studiofs-vfs --lib first_root_readdir_includes_trash_root_and_bumps_cache_token -- --nocapture

RED ran one test and failed on unexpected trash-root creation/listing. The final
first command passed one test: normal root entries listed, absent trash lookup
not found, no root/allocator/journal changes, then an existing seeded root is
listed and looked up. Mutable positive-TTL control passed one test. The earlier
zero-match exact filter was discarded and is not evidence. VFS check --tests,
Clippy --all-targets -D warnings and direct rustfmt passed. No mounted test or
separate ReadOnlyMetaEngine adapter fixture is claimed. F5 is a separate follow-up.

F3 acceptance is being strengthened before its current counted run: a test-owned
post-publication orphan is reclaimed by the real peer janitor, acquiring the
same global janitor lock afterwards proves the peer released its completed
cycle, then fresh adopted-object GC candidates and S3 existence are checked
again. This is test code, not a second production cleanup mechanism. No F3 GREEN
result is claimed until the rebuilt test runs against configured PostgreSQL/S3.

### F3 current PG/S3 acceptance and F5 native correction (2026-11-14)

F3 rebuilt command:

    cargo test -p studiofs-vfs --test vfs_test retained_restore_losing_to_settled_peer_purge_preserves_inode_and_bytes -- --ignored --exact --nocapture

Passed 1/1 in 32.28s after a 42.78s build, current log
`target/sfs1054-f3-current-pg-s3.log`. This is real PostgreSQL + S3 + background
VFS janitor, not a kernel-mounted WinFSP/FUSE test. The test verifies restart
before republish, recovered upload ownership, real reclamation of old owned
S3 objects by peer B, foreign apply/republish, same inode and exact peer bytes,
no stage-inode shared references, and no fresh-object GC candidates. It then
creates only its own sentinel under its unique data prefix, waits for real
janitor deletion, acquires A's global janitor lock after the peer finishes its
cycle, repeats the fresh-object candidate query (zero), and verifies the adopted
objects still exist in S3. There is no manual deletion replacing janitor proof.
The test's preflight deletes only its own preflight key. Credentials were not
printed; NO_PROXY/no_proxy were set only for that process.

F5 initially used one predicate for both lookup and listing: positive TTL or a
visible retained receipt made the root visible. Empty TTL0 root and sole
NoAccess receipt REDs reproduced a root listing leak. The receipt selector now
checks current original-parent effective ACL; NoAccess is hidden and ReadOnly
remains listable. ACL lookup errors propagate. Existing missing-parent handling
is unchanged. The first-root cache test now observes an actual Negative dentry
before readdir and a direct Positive cache result afterwards.

Luna's focused `cargo test -p studiofs-vfs --lib retained_views:: -- --nocapture`
passed 23/23, and check/Clippy/rustfmt passed. Root additionally ran the original
`root_readdir_hides_trash_like_directories`: 1 passed in 0.05s. However, native
TTL0 acceptance found a real regression in hiding the root from exact lookup:
Shell creates it, cannot reopen it, retries create and fails AlreadyExists,
leaving the source intact. Log `target/sfs1054-current-native-disabled.log`:
1 failed, 0.77s, source_exists=True and no metadata pair.

That native evidence supersedes the inferred lookup-hiding test expectation.
The current shared predicate is named `trash_root_is_listed` and controls root
readdir only. Exact case-insensitive lookup of an existing system root remains
available to Shell. It does not expose hidden receipt names: child lookup/listing
still filters the original parent's current ACL. Tests now explicitly preserve
that distinction, including the NoAccess sole-receipt control. No special caller
identity, new API, auth boundary or private-store bypass was added.

Rebuilt native TTL0 command:

    cargo test -p studiofs-winfsp --test mount_sanity winfsp_default_disabled_trash_is_destructive_shell_probe -- --ignored --exact --nocapture

Passed 1/1 in 0.92s, log `target/sfs1054-current-native-disabled-fixed.log`:
source_exists=False, recycled_metadata=0, shell_listed=0. Before that correction,
positive-TTL native restore/owned-pair clear and Move/list both passed (2 tests,
2.79s), log `target/sfs1054-current-native-trash.log`; those positive-TTL paths
are unchanged. The restore probe checks exact bytes and receipt states Restored
and PurgeRequested(User), not just missing sidecars. No host-wide Empty Recycle
Bin command was executed. Native probes use their own temporary local metadata
and previously unused W: only. Mapped-writer probe and final checks are recorded
below when completed. Whole feature and cross-platform acceptance remain open.

Final current focused follow-up:

- `cargo test -p studiofs-vfs --lib retained_views:: -- --nocapture`: 23/23 passed
  in 0.25s after the TTL0 exact-path correction (includes F4 and ACL controls).
- `cargo test -p studiofs-winfsp --test mount_sanity winfsp_native_mapped_writer_survives_restore_probe -- --ignored --exact --nocapture`:
  1/1 passed in 0.56s; log `target/sfs1054-current-native-mapped-result.log`,
  callback trace `target/sfs1054-current-native-mapped.log`. This is real WinFSP
  mapped I/O, not simulated handle lifetime. W: is absent after completion.
- Global `cargo fmt --all -- --check` and `git -c core.safecrlf=false diff --check`
  passed after scoped formatting. Final affected-crate Clippy is reported below.

F3-F5 are ready for fresh independent review, not a claim of whole-feature
completion. Native directory aliases, remote-only retry UX, Linux/macOS native
views/acceptance and English/Ukrainian guides remain unfinished. No commits,
pushes, new PR, host-wide recycle-bin clearing or full suites were performed.
Final cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings passed in 13.33s.

### Pause checkpoint: cached native trash visibility (2026-11-14)

User requested a break after the current bounded work. Do not start further
implementation or a new review until the user resumes.

The latest independent review accepted F3 service/GC acceptance, F4 readonly
root handling, and R1/R2 follow-ups. It requested one MEDIUM correction: a
previously generated cached `$I` sidecar could expose original-path metadata
after original-parent access was revoked; cached exact `$R`/`$I` lookup also
fell through to ignored entries after visibility was denied.

The current correction revalidates generated retained views for public metadata
and reads, removes hidden generated names before ignored-entry lookup fallback,
and validates the exact cloned entry snapshot whose bytes are copied. Registry
locks are released before receipt/ACL queries. Shell drafts are distinguished
by their existing backing provenance, not merely by canonical hexadecimal names.
No new storage format or remote foreground dependency was added.

Evidence at this checkpoint:

- A focused RED reproduced warmed sidecar lookup/read leakage after NoAccess on
  a non-root original parent, including a previously opened read handle.
- After the correction, all 10 guard tests passed. Coverage includes cached
  metadata/read denial before and after detach, an already-open handle, and a
  saved sidecar snapshot after its registry inode has been evicted. Canonical
  hex Shell sidecar drafts remain usable.
- Before the final snapshot-validator refinement, real WinFSP Move/List and
  Restore/owned-pair Clear passed 2/2. Log:
  `target/sfs1054-cached-acl-native.log`. This is not a post-refinement rerun.
- After the final refinement, rebuilt real WinFSP TTL0 destructive Shell probe
  passed 1/1 in 1.01s (build 42.20s including build-lock wait). Log:
  `target/sfs1054-cached-acl-native-disabled.log`. Source absent, no retained
  metadata. W: is absent after teardown.
- Codebase Memory coverage for ignored_entries.rs, read_path.rs, io_ops.rs,
  namespace_ops.rs and retained_views/guard_tests.rs reported metadata_match /
  no_recorded_issue; material behavior was also checked directly in source.

Resume with a fresh independent Opus 5.5 medium review of this cache visibility
correction, not a re-review of already accepted F1/F2/R1/R2/F3/F4 work. No new
review was launched after the pause request. VFS tests do not prove revocation
of bytes already cached by Windows or application memory; native kernel-cache
revocation was not investigated or changed in this slice.

Whole-feature work remains: remote-only retry UX, directory restore/aliases,
Linux/macOS native views and acceptance, English/Ukrainian guides, final feature
review and delivery. No full suites, commits, push, new PR, Linear updates or
host-wide recycle-bin clearing were performed in this checkpoint.

Worker final handoff: completed and idle at the pause. Focused guard tests 10/10,
`cargo check -p studiofs-vfs`, affected VFS Clippy with `-D warnings`, rustfmt and
`git diff --check` all passed. Root also reran the repository diff check after
writing this checkpoint; it passed. No active worker checks remain.

### Narrow F5 cached-trash visibility & snapshot-read correction (2026-11-15)

Follow-up slice resolving two MEDIUM review findings on cached `$I`/`$R` trash views without altering PG/GC/F1/R1/R2 retained payload or restore design:

1. **ACL & cached-attr mutation/open/create bypasses on warmed generated `$I`/`$R` after non-root `original_parent` `NoAccess`:**
   - `crates/studiofs-vfs/src/vfs/acl.rs` & `ignored_entries.rs`: Inodes `>= FIRST_IGNORED_INO` never read from or write to `effective_acl_cache`. Generated entries (`retained_view_receipt` `Some(_)`, strictly `TrashViewBacking::Payload` / `Bytes`) resolve `AccessLevel` via `trash_view_receipt_access` (checking `Retained` state, non-missing non-trashed `original_parent`, and `original_parent` effective ACL) rather than following `$RECYCLE.BIN` -> `ROOT_INO`. Plain ignored entries and canonical hex Shell `$IABCDEF` drafts (`view: None`) walk ignored parents to their real namespace parent `< FIRST_IGNORED_INO` (caching only the real parent) in both sync and async ACL paths.
   - `crates/studiofs-vfs/src/vfs.rs`, `sync_ops.rs`, `io_ops.rs`, `namespace_ops.rs`, `misc_ops.rs`, `read_path.rs`: Guarded `enforce_write_access_for_handle` orphan fallback with `ino < FIRST_IGNORED_INO`; routed `open_file_resolved` (even when `check_authority == false`), `get_open_file_attr`, `setattr_attr_only_deferred_local`, `setattr_with_handle_and_mtime`, `write*`/`write_fh*`, `fsync*`/`flush*`, `getattr*`, `readlink`, `set_xattr`, and `remove_xattr` through `visible_ignored_entry_snapshot` + ACL checks so hidden or evicted generated entries never expose cached attrs or mutate; added `lookup_public_ignored_entry` / `create_public_ignored_entry` / `unlink_ignored_entry_if_matches` so public `create*`/`link`/`mkdir*`/`symlink`/`unlink`/`rmdir`/`rename` and `reject_nontransactional_no_replace` prune stale hidden generated entries instead of reusing their `ino`/`size` or acting as an `EntryExists` oracle. Registry mutex is dropped before ACL/meta/async calls.

2. **Single-snapshot read & no absent zero-fill (`read_ordered`, `read_into_impl`, `read_ignored_entry_snapshot_into`, `read_trash_view_snapshot`):**
   - `io_ops.rs`, `ignored_entries.rs`, `retained_views.rs`: Replaced `read_ignored_entry_into` and `read_trash_view` with `read_ignored_entry_snapshot_into` and `read_trash_view_snapshot` using the exact `IgnoredEntry` snapshot fetched once in `read_ordered` / `read_into_impl` for both size clamping and byte reading, removing the absent-entry `buffer.fill(0)` fallback completely.

Verification evidence (repository root, branch `feat/sfs-1054-recoverable-trash`):

- Focused RED (before fix): `cargo test -p studiofs-vfs --lib retained_views:: -- --nocapture` -> `26 passed; 2 failed; 0 ignored; 1374 filtered out; finished in 0.34s` (compile `54.04s`): `warmed_generated_trash_after_nonroot_revoke_refuses_cached_attr_and_mutation_bypasses` panicked on `enforce_access` and `read_uses_single_snapshot_and_absent_ignored_entry_never_zero_fills` panicked on absent `read_ignored_entry_into` zero-fill.
- Focused GREEN (after fix):
  - `cargo test -p studiofs-vfs --lib retained_views:: -- --nocapture`: `28 passed; 0 failed; 0 ignored; 1374 filtered out; finished in 0.34s` (compile `32.89s`), covering non-root revoke across `enforce_access`, `setattr*`, `open_file*`, `get_open_file_attr`, `write*`/`write_fh*`, `read_into_fh`, `set_xattr`/`remove_xattr`/`get_xattr*`/`list_xattrs`, `fsync*`, `rename`/`unlink`/`create*`, `ReadOnly` ACL restoration controls, canonical hex `$IABCDEF` draft create/write/read (including cold async ACL cache) + plain ignored file controls, and snapshot eviction / no zero-fill.
  - `cargo test -p studiofs-vfs --lib ignored_entries:: -- --nocapture`: `7 passed; 0 failed; 0 ignored; 1395 filtered out; finished in 0.12s`.
  - `cargo test -p studiofs-vfs --lib acl::tests:: -- --nocapture`: `2 passed; 0 failed; 0 ignored; 1400 filtered out; finished in 2.19s`.
  - `cargo test -p studiofs-vfs --lib ignored_entry_writes_preserve_synthetic_attr_size -- --nocapture`: `1 passed; 0 failed; 0 ignored; 1401 filtered out; finished in 0.04s`.
  - `cargo test -p studiofs-vfs --lib deferred_release_ignored_entry_skips_release_barrier -- --nocapture`: `1 passed; 0 failed; 0 ignored; 1401 filtered out; finished in 0.03s`.
  - `cargo test -p studiofs-vfs --lib root_readdir_hides_trash_like_directories -- --nocapture`: `1 passed; 0 failed; 0 ignored; 1401 filtered out; finished in 0.05s`.
  - `cargo check -p studiofs-vfs --tests`: passed in `14.04s`.
  - `cargo clippy -p studiofs-vfs --all-targets -- -D warnings`: passed in `12.69s`.
  - `cargo fmt --all -- --check`: passed (`exit 0`).
  - `git -c core.safecrlf=false diff --check`: passed (`exit 0`).
- Scope limits / not run: full workspace/crate test suites, PostgreSQL/S3 service tests, and WinFSP native mount probes (`studiofs-winfsp`) were not run; all runtime checks are `studiofs-vfs` unit tests with local `RedbMetaEngine`. Native kernel-cache revocation of bytes previously read by Windows remains out of scope. No commits, pushes, PRs, Linear updates, daemon restarts, or host-wide Recycle Bin clears were performed.

### F5 stale-handle name ownership correction (2026-11-15)

Fresh Gemini review accepted the shared ACL and single-snapshot read design,
but identified old orphan release/detach removing a replacement's by_name key.
Root added a post-close lookup assertion to the warmed/revoked parent test:
RED 0 passed/1 failed with EntryNotFound for the replacement sidecar. Log:
`target/sfs1054-name-release-red.log`.
Both release_ignored_entry and detach_ignored_entry_name now remove the name
only if its current mapping still equals the old inode. The existing parent
child cleanup remains inode-specific. GREEN retained_views: 28 passed, 0 failed
in 0.33s (`target/sfs1054-name-release-green.log`). A separate focused control
covers repeat detach followed by old-inode release while the replacement stays
lookupable. Follow-up checks/native evidence will be recorded by the next task.
No commits/push/full suites or external changes. The incidental pre-existing
negative path shortcut behavior after ACL restoration remains outside this fix.

### F5 name-ownership review & rebuilt WinFSP native acceptance (2026-11-15)

- **Small name-ownership fix verification (`crates/studiofs-vfs/src/vfs/ignored_entries.rs`)**:
  - Attempted Codebase Memory MCP CLI (`codebase-memory-mcp cli list_projects '{}'`), which was unavailable due to a daemon version conflict (`active version 0.12.1` vs `requested version 0.11.0`); fell back to direct `rg` + source inspection of `ignored_entries.rs`, `sync_ops.rs`, and `namespace_ops.rs`.
  - Confirmed `release_ignored_entry` (`ignored_entries.rs:422-434`) and `detach_ignored_entry_name` (`ignored_entries.rs:436-451`) both guard `registry.by_name.remove(&key)` on `registry.by_name.get(&key).copied() == Some(ino)`, while `remove_parent_child_link` and `children_by_parent.remove(&ino)` remain inode-keyed. Verified all callers (`cleanup_orphan_on_last_handle_close` in `sync_ops.rs:841`, `take_ignored_entry_release_action` in `sync_ops.rs:906-908`) and tests (`warmed_generated_trash_after_nonroot_revoke_refuses_cached_attr_and_mutation_bypasses` in `retained_views/guard_tests.rs:780-787`, `stale_detach_and_release_preserve_replacement_name` in `ignored_entries.rs:725-737`).
  - Existing root logs verified: RED (`target/sfs1054-name-release-red.log`: 0 passed, 1 failed on `EntryNotFound` for `$I001003`), GREEN (`target/sfs1054-name-release-green.log`: 28/28 passed in 0.33s), controls (`target/sfs1054-name-release-controls.log`: 8/8 passed in 0.13s). **Verdict: Approve** the small name-ownership fix.
- **Rebuilt WinFSP focused native acceptance (repository root, `STUDIOFS_WINFSP_TEST_MOUNT_POINT=W:`, sequential, `Test-Path W:\ == False` before and after every run, no global Recycle Bin clears, no PG/S3)**:
  - Rebuilt test binary: `cargo test -p studiofs-winfsp --test mount_sanity --no-run` (`target/sfs1054-f5-final-build.log`, 36.90s, produced `target/debug/deps/mount_sanity-0ccc233bed46233d.exe`; same binary reused across all three runs).
  - `cargo test -p studiofs-winfsp --test mount_sanity winfsp_native_trash_ -- --ignored --test-threads=1 --nocapture` (`target/sfs1054-f5-final-native-trash.log`): **2 passed, 0 failed, 31 filtered out in 2.75s** (`winfsp_native_trash_restore_and_clear_shell_probe`: `view_bytes_ok=True restored_exists=True content_ok=True pairs_after_restore=0 cleared_pairs=0` + `Restored`/`PurgeRequested(User)` receipt checks; `winfsp_native_trash_retention_shell_probe`: `source_exists=False recycled_metadata=1 shell_listed=1 layout=shared`).
  - `cargo test -p studiofs-winfsp --test mount_sanity winfsp_default_disabled_trash_is_destructive_shell_probe -- --ignored --exact --nocapture` (`target/sfs1054-f5-final-native-disabled.log`): **1 passed, 0 failed, 32 filtered out in 0.92s** (`source_exists=False recycled_metadata=0 shell_listed=0`).
  - `cargo test -p studiofs-winfsp --test mount_sanity winfsp_native_mapped_writer_survives_restore_probe -- --ignored --exact --nocapture` with `STUDIOFS_WINFSP_TRACE_LOG=target\sfs1054-f5-final-mapped-trace.log` (`target/sfs1054-f5-final-native-mapped.log`): **1 passed, 0 failed, 32 filtered out in 0.57s** (trace confirms cleanup precedes `constrained_io=true` paging write on restored inode).
- **Limits & out-of-scope**: No product edits were made in this verification pass; full suites, PG/S3 runs, commits/pushes/PRs/Linear updates, and global Recycle Bin clears were not run. Native probes use local `RedbMetaEngine` on disposable `W:` mounts and do not prove Windows/application kernel-cache revocation, remote-only native retry UX, directory aliases, Linux/macOS native mounts, or user guides; the whole SFS-1054 feature remains incomplete.

### Windows directory restore, `$R` alias vs canonical receipt names, and name-conflict acceptance (2026-11-15)

- **Shell `$R` aliases vs canonical volume-global `$R<receipt>` views**:
  - Verified in native trace (`target/sfs1054-directory-red-trace.log` and `target/sfs1054-directory-green-trace.log`) and VFS `retained_views::`: when Windows Shell (`FileSystem::DeleteDirectory` / `DeleteFile`) recycles an item, it locally creates a draft `$I<shell_id>` sidecar and renames the target to `$RECYCLE.BIN\$R<shell_id>`. Under positive TTL, `rename_inner` retains the source directory/file in `.studiofs-retained/<receipt>/data` under a fresh `receipt` inode and discards the client-local Shell alias (`$R<shell_id>`) rather than publishing client-chosen random `$R` names.
  - Because views are **volume-global generated views**, `$RECYCLE.BIN` synthesizes one canonical `$R<receipt:06X>` / `$I<receipt:06X>` pair per visible `Retained` receipt from `.studiofs-retained` (and formats `$I` v2 paths with the client's local mount prefix such as `W:\...`). Shell then enumerates `$RECYCLE.BIN` (`NameSpace(10).Items()`) and binds directly to `$R<receipt:06X>` / `$I<receipt:06X>`. Non-canonical `$R` aliases are never exposed or accepted as retained view targets.
- **Non-empty directory view traversal, ACL enforcement, and bottom-up Clear (`crates/studiofs-vfs` + `crates/studiofs-winfsp`)**:
  - `crates/studiofs-vfs/src/vfs/ignored_entries.rs`: added `TrashViewBacking::Subtree { receipt, source_ino, parent_source_ino }`, `deleted_view_children` tombstones per `(receipt, parent_source_ino, name)`, `upsert_ignored_view_entry`, `is_synthesized_ignored_ino`, and snapshot ACL/tombstone revalidation; `ignored_dir_attr` excludes retained directory views (`view.is_none()`).
  - `crates/studiofs-vfs/src/vfs/retained_views.rs`: added `trash_directory_bytes_total` (dirty-aware recursive regular-file byte sum for `$I` v2 size), `TrashListingEntries`, `retained_view_subtree_dir`, `trash_subtree_child_attr`, `trash_subtree_listing`, `trash_subtree_unlink`, `trash_subtree_rmdir`, `cleanup_trash_view_cache_on_terminal`, and subtree read support in `read_trash_view_snapshot` (refusing directory reads with `MetaError::IsADirectory`).
  - `crates/studiofs-vfs/src/vfs/namespace_ops.rs`, `io_ops.rs`, `sync_ops.rs`, `misc_ops.rs`: wired lookup, readdir/readdir_with_attrs, readlink, unlink (bottom-up subtree file tombstones, rejecting top-level `$R` dir with `IsADirectory`), rmdir (enforcing `original_parent` `ReadWrite` ACL before `DirNotEmpty`, bottom-up subtree dir tombstones, top-level `$R` dir calling `request_trash_view_purge`), and read-only view guards rejecting create/mkdir/link/symlink/write/xattr/size/mode/owner/timestamp mutations while permitting Windows `Remove-Item -Force` `SetBasicInfo` `win_attrs`-only pre-delete clears on writable retained views (`OperationNotSupported` for all non-attribute mutations, `AccessDenied` when `ReadOnly`).
  - `crates/studiofs-winfsp/src/context.rs`: `heal_directory_orphans_for_delete` skips `meta.readdir` for synthesized ignored inodes (`is_synthesized_ignored_ino`) so WinFSP `set_delete` / `cleanup_delete` on `$R<receipt>` and nested subdirectories succeeds cleanly.
- **Name-conflict contract & interactive Shell modal limit**:
  - Restore (`rename_inner` -> `restore_trash_view_entry` -> `restore_retained_entry`) ignores `replace_if_exists` and rejects any occupied destination (`EntryExists` / `STATUS_OBJECT_NAME_COLLISION`), never silently overwriting existing files/directories and keeping `Retained` state, exact payload bytes, same original inode, and outside hard links intact.
  - Under real Windows Shell (`NameSpace(10)` `InvokeVerb("undelete")`), restoring a directory over an existing path opens Windows' interactive `OperationStatusWindow` replacement prompt rather than silently overwriting (`prompt_seen=True prompt_class=OperationStatusWindow`); after bounded modal cancellation (`WM_CLOSE`), the colliding file (`keep.txt`), `$R`/`$I` pair, and `Retained` state remain intact, direct `IO.Directory::Move` and `MoveFileEx(..., MOVEFILE_REPLACE_EXISTING=1)` over an occupied alternate target both fail without overwriting (`blocked_rename_refused=True replace_rename_refused=True`), and explicit alternate-destination restore (`shell-dir-conflict-alt`) succeeds preserving exact nested bytes, `orig_dir_ino`, and `RetainedEntryState::Restored`. **Exact limit**: headless test automation cannot choose Explorer's interactive merge/replace UI buttons directly (only prove the prompt opens without silent overwrite, cancel it safely, verify Win32/WinFSP overwrite refusal, and prove explicit alternate-destination / post-blocker restore).
- **Verification evidence (repository root, branch `feat/sfs-1054-recoverable-trash`, sequential `W:` probes, `Test-Path W:\ == False` before and after every run, no global Recycle Bin clears)**:
  - RED controls (`target/sfs1054-directory-red.log`, `target/sfs1054-directory-vfs-red.log`): WinFSP probe reproduced `i_size=4096 r_isdir=False r_a_ok=False r_b_ok=False shell_isfolder=False`; 3 new VFS directory/collision/ACL tests failed pre-fix (28 passed, 3 failed).
  - Post-final-edit VFS unit checks:
    - `cargo test -p studiofs-vfs --lib retained_views:: -- --nocapture` (`target/sfs1054-directory-vfs-green.log`): **31 passed, 0 failed** (0.44s; covers directory kind/subtree/recursive `$I` size `18`, collision `EntryExists` preserving data & outside hardlinks + bottom-up Clear `PurgeRequested(User)`, and `original_parent` ReadOnly/NoAccess subtree ACL).
    - `cargo test -p studiofs-vfs --lib ignored_entries:: -- --nocapture` (`target/sfs1054-directory-vfs-ignored.log`): **8 passed, 0 failed**.
    - `cargo test -p studiofs-vfs --lib root_readdir_hides_trash_like_directories` (`target/sfs1054-directory-vfs-root-readdir.log`): **1 passed, 0 failed**.
    - `cargo test -p studiofs-vfs --lib ignored_entry_writes_preserve_synthetic_attr_size` (`target/sfs1054-directory-vfs-writes.log`): **1 passed, 0 failed**.
    - `cargo test -p studiofs-vfs --lib deferred_release_ignored_entry_skips_release_barrier` (`target/sfs1054-directory-vfs-release.log`): **1 passed, 0 failed**.
  - Post-final-edit WinFSP native probes (`target/sfs1054-directory-build.log`, rebuilt `mount_sanity-0ccc233bed46233d.exe`):
    - `winfsp_native_trash_directory_restore_and_clear_shell_probe` (`target/sfs1054-directory-green.log`, trace `target/sfs1054-directory-green-trace.log`): **1 passed, 0 failed** (`1.86s`; `i_size=18 r_isdir=True r_children=\nested;\a.txt;\nested\b.txt r_a_ok=True r_b_ok=True shell_found=True shell_isfolder=True restored_exists=True a_ok=True b_ok=True pairs_after_restore=0 cleared_pairs=0` + `Restored` and `PurgeRequested(User)` receipt states).
    - `winfsp_native_trash_directory_collision_shell_probe` (`target/sfs1054-directory-collision.log`, trace `target/sfs1054-directory-collision-trace.log`): **1 passed, 0 failed** (`5.36s`; `prompt_seen=True prompt_class=OperationStatusWindow keep_ok=True no_silent_overwrite=True pair_preserved=True blocked_rename_refused=True replace_rename_refused=True blocker_intact=True alt_restored_ok=True pairs_after_alt_restore=0` + same `orig_dir_ino` and `Restored { parent: 1, name: b"shell-dir-conflict-alt" }` receipt).
    - Sequential regression reruns (`target/sfs1054-directory-reg-*.log`): `winfsp_native_trash_restore_and_clear_shell_probe` (**1 passed**, `1.01s`), `winfsp_native_trash_retention_shell_probe` (**1 passed**, `0.91s`), `winfsp_default_disabled_trash_is_destructive_shell_probe` (**1 passed**, `0.91s`), `winfsp_native_mapped_writer_survives_restore_probe` (**1 passed**, `0.58s`).
  - Lint/format/diff checks: `cargo clippy -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings` (`target/sfs1054-directory-clippy.log`: exit 0), `cargo fmt --all -- --check` (`target/sfs1054-directory-fmt.log`: exit 0), `git -c core.safecrlf=false diff --check` (`target/sfs1054-directory-diffcheck.log`: exit 0).
- **Remaining unproven / outside this slice**: Remote-only recovery UX, Linux/macOS native mounts/acceptance, EN/UK user guides, full workspace test suites, and PG/S3 reruns remain open; SFS-1054 as a whole is not complete.

### Directory trash review fixes & bounded tombstone lifecycle (2026-11-15)

Follow-up resolution of all three RequestChanges findings (1 HIGH, 2 MEDIUM) plus the LOW synthesized-inode predicate issue, without altering hardlink logical `$I` folder byte totals (`trash_directory_bytes_total`) or approved PG/GC/R1/R2/F5 shared ACL work:

1. **HIGH — Reject writes to retained views on `write_fh_payload_with_deferred_materialize` (`sync_ops.rs`, `io_ops.rs`, `ignored_entries.rs`):**
   - Extracted shared `StudioVfs::write_ignored_entry(ino, offset, data_len)` (`ignored_entries.rs`) and routed `io_ops::write`, `sync_ops::write_fh_with_local_sync_policy`, and `sync_ops::write_fh_payload_with_deferred_materialize` through it so `ignored.view.is_some()` (`$I` sidecars, `$R` payloads/directories, and nested `Subtree` entries) returns `VfsError::OperationNotSupported` instead of mutating synthetic `attr.size` and discarding bytes under writable `original_parent`.
2. **MEDIUM — Bounded deterministic lifecycle for `deleted_view_children` Clear tombstones (`ignored_entries.rs`, `retained_views.rs`, `acl.rs`, `workers.rs`, `io_ops.rs`, `namespace_ops.rs`, `studiofs-winfsp/src/context.rs`):**
   - Replaced the unbounded `HashSet<(Ino, Ino, Vec<u8>)>` with bounded `TrashViewTombstoneMap` (`MAX_TRASH_VIEW_TOMBSTONE_RECEIPTS`: `256` prod / `4` test; `MAX_TRASH_VIEW_TOMBSTONE_ENTRIES`: `65_536` prod / `64` test), keyed per `receipt` with `acl_epoch` and `last_touched_seq` for atomic per-receipt LRU eviction when exceeding caps.
   - **Documented lifecycle & cleanup triggers**:
     - Native bottom-up `Remove-Item -Recurse` / Explorer Delete records tombstones per receipt while `$RECYCLE.BIN` and `$R` open/close handles remain active (verified via `target/sfs1054-directory-green-trace.log`), so tombstones are never tied to `$R` handle closes or `$I`/`$RECYCLE.BIN` reads.
     - Cleared immediately on local restore (`restore_retained_entry_trusted` / `restore_trash_view_entry`) and local purge (`request_trash_view_purge` -> `cleanup_trash_view_cache_on_terminal`).
     - Rolled back atomically per receipt (and view directory generations bumped) on `set_acl_rule` / `remove_acl_rule` (`acl_epoch` mismatch, intentionally NOT `acl_namespace_epoch`), failed Clear operations in `trash_subtree_unlink` / `trash_subtree_rmdir` / `rmdir_inner` (preserving tombstones on expected path-probe errors `DirNotEmpty`, `EntryNotFound`, `IsADirectory`, `NotADirectory`), `trash_view_receipt_access_by_receipt` observing `access != ReadWrite`, WinFSP `set_delete(..., false)` cancellation, and WinFSP `set_delete(..., true)` access denial via `enforce_retained_view_delete_access`.
     - Reconciled across peers/expiry/TTL purge in `workers.rs` (`apply_semantic_invalidation_batch`, `reconcile_metadata_full_walk`, `reconcile_metadata_scope` -> `reconcile_trash_view_tombstones`).
   - **Residual policy trade-off (OS process kill mid-Clear)**: If an external Windows client process is forcibly killed (`TerminateProcess`) *without* `set_delete(false)` and with no ACL change, restore, or remote expiry, remaining in-memory tombstones for that receipt persist only up to LRU receipt capacity (`256` receipts / `65_536` entries) until another Clear evicts them, ACL changes, or the receipt is purged/restored/mounted anew. Policy alternatives reported to the user:
     1. *Keep current deterministic bounded lifecycle (no timer)* — zero slow-delete race, bounded RAM (`<=256` receipts / `<=65_536` entries), stale state cleaned on next ACL change, restore, purge, remote expiry, remount, or LRU eviction; residual partial state can persist across an ungraceful client `SIGKILL`/`TerminateProcess` until one of those events occurs.
     2. *Add a secondary sliding inactivity expiry per tombstone receipt* — automatically recovers after an ungraceful process kill without extra Clear traffic, at the cost of a policy deadline trade-off where a slow recursive Clear paused beyond the idle window rolls back mid-operation.
     3. *Remove bottom-up child tombstones and allow Clear only via top-level `$R` unlink/rmdir* — eliminates partial Clear state completely, but breaks Windows native `Remove-Item -Recurse` / Explorer Delete (which recursively deletes children before removing `$R`).
3. **MEDIUM — Receipt branch ACL ordering in `rmdir_inner` & `rename_inner` and `.studiofs-retained` restore stage ACLs (`namespace_ops.rs`, `io_ops.rs`, `acl.rs`, `retained_views.rs`):**
   - Moved `trash_view_receipt_for` ahead of generic `enforce_access(parent, AccessLevel::ReadWrite)` in `rmdir_inner` and `rename_inner` (matching `unlink_inner`), so Clear and restore under `ROOT_INO=ReadOnly` + explicit `/work=ReadWrite` no longer fail with `AccessDenied` on `$RECYCLE.BIN` (`ino 4`), while `original_parent` `ReadWrite` and destination `ReadWrite` remain strictly enforced.
   - Scoped `in_trusted_retained_scope()` ACL ancestor-walk bypass in `acl.rs` strictly to `ino < FIRST_IGNORED_INO && self.inode_is_in_private_store(ino)?` (checking `.studiofs-retained` specifically), added `enforce_public_access` for destination `parent` and `original_parent`, and explicitly rejected `.studiofs-retained` destinations in `restore_retained_entry_trusted`. This preserves existing custom-store `b"private"` test behavior (`vfs_test::retained_restore_denies_without_source_or_original_parent_write`) when `ROOT_INO` is `ReadOnly`.
4. **LOW — Exclude `RUNTIME_STATS_FILE_INO` from `is_synthesized_ignored_ino` (`ignored_entries.rs`):**
   - Updated `is_synthesized_ignored_ino` to check `ino >= FIRST_IGNORED_INO && !self.is_runtime_stats_file_ino(ino)`.

Verification evidence (repository root, branch `feat/sfs-1054-recoverable-trash`):

- **RED regressions before fixes**:
  - `target/sfs1054-directory-review-red-ignored.log`: `is_synthesized_ignored_ino_excludes_runtime_stats_file_ino` failed (`8 passed; 1 failed`).
  - `target/sfs1054-directory-review-red.log`: `retained_views::` failed 4 new tests (`31 passed; 4 failed`): `write_fh_with_deferred_materialize_refuses_writable_retained_views` ($I deferred `write_fh` succeeded under RW parent), `root_read_only_with_writable_work_dir_allows_restore_and_clear_while_ro_original_denies` (`AccessDenied { ino: 4 [$RECYCLE.BIN], required: ReadWrite, actual: ReadOnly }`), `interrupted_directory_clear_acl_downgrade_and_retry_clears_tombstones` (nested stayed hidden after downgrade), `interrupted_directory_clear_restore_and_remote_terminal_clean_up_tombstones` (`2 != 0` tombstones left after peer `request_retained_purge` + invalidation batch).
  - `target/sfs1054-directory-review-red-winfsp.log`: `context::tests::set_delete_cancellation_and_access_denial_for_retained_views` failed (`0 passed; 1 failed` — `set_delete(..., false)` left `nested` hidden).
- **Post-fix GREEN unit & static checks**:
  - `cargo test -p studiofs-vfs --lib retained_views:: -- --nocapture` (`target/sfs1054-directory-review-vfs-retained.log`): **36 passed, 0 failed** (covers all 31 existing + 4 RED->GREEN + `trash_view_tombstones_enforce_bounded_receipt_and_entry_limits`).
  - `cargo test -p studiofs-vfs --lib ignored_entries:: -- --nocapture` (`target/sfs1054-directory-review-vfs-ignored.log`): **9 passed, 0 failed**.
  - `cargo test -p studiofs-vfs --test vfs_test retained_restore_denies_without_source_or_original_parent_write -- --nocapture` (`target/sfs1054-directory-review-vfs-acl-regression.log`): **1 passed, 0 failed**.
  - `cargo test -p studiofs-vfs --lib acl::tests::`, `ignored_entry_writes_preserve_synthetic_attr_size`, `deferred_release_ignored_entry_skips_release_barrier`, `root_readdir_hides_trash_like_directories`: **5 passed, 0 failed** (2 + 1 + 1 + 1).
  - `cargo test -p studiofs-winfsp --lib set_delete -- --nocapture` (`target/sfs1054-directory-review-winfsp-unit.log`): **3 passed, 0 failed**.
  - `cargo check -p studiofs-vfs -p studiofs-winfsp --tests` (`target/sfs1054-directory-review-check.log`): exit 0.
  - `cargo clippy -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings` (`target/sfs1054-directory-review-clippy.log`): exit 0.
  - `cargo fmt --all -- --check` and `git -c core.safecrlf=false diff --check`: exit 0.
- **Rebuilt WinFSP sequential native `W:` probes (`target/sfs1054-directory-review-build.log`, `target/sfs1054-directory-review-native.log`, `mount_sanity-0ccc233bed46233d.exe`, `Test-Path W:\ == False` before and after every run, no host Recycle Bin empty)**:
  - `winfsp_native_trash_directory_restore_and_clear_shell_probe`: **1 passed, 0 failed** (`2.37s`).
  - `winfsp_native_trash_directory_collision_shell_probe`: **1 passed, 0 failed** (`3.62s`).
  - `winfsp_native_trash_restore_and_clear_shell_probe`: **1 passed, 0 failed** (`1.18s`).
  - `winfsp_native_trash_retention_shell_probe`: **1 passed, 0 failed** (`1.26s`).
  - `winfsp_default_disabled_trash_is_destructive_shell_probe`: **1 passed, 0 failed** (`1.05s`).
  - `winfsp_native_mapped_writer_survives_restore_probe` (with `STUDIOFS_WINFSP_TRACE_LOG=target/sfs1054-directory-review-mapped-trace.log`; first run without env var panicked on missing env var guard, immediate rerun with env var passed): **1 passed, 0 failed** (`0.61s`).
- **Remaining limits**: Root-RO / `/work`-RW ACL restore+Clear was verified at VFS unit level (`root_read_only_with_writable_work_dir_allows_restore_and_clear_while_ro_original_denies`) rather than as a dedicated `W:` native mount test; OS-level client `TerminateProcess` recovery without `set_delete(false)` relies on the bounded LRU/reconciliation policy alternatives above. Full workspace suites, PG/S3 service tests, remote-only recovery UX, Linux/macOS native mounts, and EN/UK guides remain outside this narrow review-fix slice.



### Durable per-child directory Clear (supersedes the tombstone lifecycle above) — 2026-11-15

Approved user decision: directory Clear behaves like a local disk. Each
successful child delete is permanent; Cancel, Ctrl+C, or a killed process leaves
the remaining children visible in trash. This replaces the mount-local
`TrashViewTombstoneMap` (receipt/entry caps, ACL-epoch eviction, rollback on
`set_delete(false)`, worker reconciliation), which a review found broken:
self-eviction above 65,536 children failed Clear, any global ACL change aborted
unrelated Clears, and WinFSP never delivers `set_delete(false)` mid-Clear (each
child is an independent open/delete/close; the `$R` root is marked last).

Recovery: a cancelled Gemini run had added metadata support and partially
removed the tombstone map (the VFS crate did not compile). Its metadata work was
reused after review; the VFS, WinFSP, and tests were finished here.

#### Contract and implementation

- Metadata (`crates/studiofs-meta/src/redb_engine/retained.rs`, `trash.rs`,
  `read_only.rs`, `pg_engine/volume_settings.rs`): `purge_retained_child`
  verifies the receipt is `Retained`, the parent is beneath `receipt/data`, and
  `(parent, name)` still binds the expected inode, then journals a `Retained`
  receipt-record fence immediately before an ordinary `NamespaceDeleteTxn`
  (rmdir, unlink, or open-handle unlink) in one commit. It never deletes by
  inode alone; a changed binding returns `TargetChanged`.
- PG authority (`pg_engine/retained.rs`): the fence is an identity write while
  the shared row is `Retained`; after a peer's terminal transition it conflicts,
  so the whole publication transaction rolls back. Loser classification
  (`child_purge_event_span`) retires exactly that span and
  `reconcile_losing_retained_child_purges` drops its local overlay rows
  (dentry/inode tombstones, lifecycle, deferred deletes) so the winner's base
  shows through. Foreign apply (`remote_apply.rs`) runs the same reconcile.
  A whole pending local range publishes in one PG transaction, so the fence and
  its delete commit together.
- VFS (`retained_views.rs`, `ignored_entries.rs`, `namespace_ops.rs`,
  `workers.rs`): `trash_subtree_unlink`/`trash_subtree_rmdir` now call one
  `trash_subtree_delete` that enforces original-parent write access, takes the
  child's file-session Delete gate, calls `purge_retained_child`, and applies the
  ordinary unlink follow-up (link-count cache, orphan marking for open handles,
  `cleanup_deleted_inode_storage` otherwise; outside hard links stay live).
  Subtree view liveness is now `subtree_view_child_is_live` (the retained binding
  still resolves to the same inode) instead of a tombstone lookup. All tombstone
  state, rollback paths, and the worker reconcile were removed.
- WinFSP (`context.rs`): `set_delete(false)` is a no-op again; generated `$I`
  records (`is_generated_trash_record_ino`) disable the OS cache because their
  bytes change without a write (native RED showed a stale `$I` size of 18).
- Docs: `docs/platform-bridges.md` (Trash section) and the spec record the
  contract.

#### Evidence

- VFS RED by mutation (purge call skipped, in-memory-only delete):
  `target/sfs1054-partial-clear-red.log` — 5 failed (reopen persistence,
  unrelated/reverted ACL, >64 children, hard link/open handle, root-RO/work-RW).
- VFS GREEN: `cargo test -p studiofs-vfs --lib retained_views::` — 38 passed
  (`target/sfs1054-partial-clear-vfs-retained.log`); with `ignored_entries::`
  and `acl::tests::` 49 passed (`target/sfs1054-partial-clear-final-unit.log`);
  focused set incl. `vfs_test retained_` (5 passed) and meta `retained` (28
  passed): `target/sfs1054-partial-clear-focused.log`.
- Metadata: `retained_child_purge_spans_are_recognized_for_every_delete_shape`
  (empty dir, draft file, outside hard link, open handle; `TargetChanged` on a
  replaced name) — 1 passed (`target/sfs1054-partial-clear-meta-span.log`).
- Real PostgreSQL (unique per-run volume, `.env` loaded without printing,
  process-local `NO_PROXY=*`):
  `test_pg_retained_child_purge_versus_peer_restore_never_deletes_live_child`,
  both orders — 1 passed in 18.59s (`target/sfs1054-partial-clear-pg-race.log`).
  Restore first: the losing child purge retires, the restored child stays live
  on both clients, and the journal drains. Purge first: the peer sees the child
  gone and restores only the remaining tree. RED by mutation (fence removed):
  the restored live child was deleted (`EntryNotFound`),
  `target/sfs1054-partial-clear-pg-race-red.log`.
- WinFSP unit: `cargo test -p studiofs-winfsp --lib set_delete` — 3 passed;
  `set_delete(false)` keeps completed child deletes and the `$R` root.
- Native WinFSP, rebuilt `mount_sanity-0ccc233bed46233d.exe`, sequential on `W:`
  (`Test-Path W:\` False before and after each run, host Recycle Bin never
  emptied), `target/sfs1054-partial-clear-native.log`:
  new `winfsp_native_trash_directory_partial_clear_shell_probe` (RED
  `i_size=18` before the `$I` cache fix, GREEN `after_child_delete i_size=9
  nested_gone=True a_ok=True` / `restored_a_ok=True restored_nested_absent=True
  pairs_after_restore=0`, one `Restored` receipt), plus directory
  restore/Clear, directory collision, file restore/Clear, retention,
  TTL0-destructive, and mapped-writer probes — 7/7 passed.
- `cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs-winfsp
  --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and
  `git diff --check`: exit 0.

#### Remaining limits

- Stop/restart persistence is proven at the VFS level (reopen of the same redb),
  not by remounting a native `W:` volume mid-Clear.
- Payload GC after a published child purge reuses the ordinary unlink path
  (local cache eviction; shared deletion through the existing janitor after
  acknowledgement). No new GC-specific test was added for child purges; the
  losing path is covered by the PG race test (deferred deletes dropped).
- Handle reads after Move to Trash remain subject to per-call ACL (private
  payloads read as `InodeNotFound`); this predates the change.
- The journal-shape recognizer must stay in sync with `NamespaceDeleteTxn`
  event shapes; published-file delete shapes are covered only through the PG
  path, not the redb span unit test.
- Not run: workspace/full suites, Linux/macOS, remote retry UX, user guides,
  commits, PRs, Linear.

### Partial-Clear review corrections: loser span ownership, cost, coverage, GC (2026-11-15)

Scope: the REQUEST-CHANGES review of the durable per-child Clear. The fence,
ordinary delete, PG transaction split, and restart authority were reviewed
correct and were not reopened.

#### Findings and decisions

1. **HIGH, confirmed (journal rewrite).** The flush classifier found losing
   child-purge spans only in the selected (unpublished) journal, but
   `reconcile_losing_retained_child_purges` rescanned everything from
   `flushed_event_seq` and rewrote every span for the receipt to the no-op
   placeholder, including a span already committed to PG and awaiting its
   self-ack. Fix: the reconcile now takes `retire: &dyn Fn(u64) -> bool` (span
   start sequences the caller owns). The flush path
   (`losing_retained_terminal_records(events, ranges)`) maps each losing span to
   its journal sequences through `selected_local_event_seq` and retires only
   those (a span whose sequences are not contiguous is not retired). Foreign
   apply excludes spans inside the in-memory published ledger
   (`RemoteApplyCoordinator::with_published_ranges`, from
   `LocalEventProgress::published`), which covers the ack-barrier case.
2. **MEDIUM, fixed (cost).** The redb reconcile builds the surviving-journal
   dentry and inode `HashSet`s in one pass (it was O(spans × events) in the write
   transaction). `apply_retained_authority` keeps a per-PG-transaction map of the
   receipt rows it locked and settled; an identical later record (each Clear
   fence) skips the `FOR UPDATE` round trip. A different record still reads,
   validates, and transitions as before. The map is created inside the
   publication retry closure, so a retried transaction starts empty; seeding has
   its own.
3. **Coverage.** Published-base standard delete, published base plus local
   overlay (base rollback shape), and symlink spans are recognized; a mixed
   reconcile with two receipts, two spans on a common parent, and an unrelated
   unlink changes exactly the four selected events.
4. **GC, confirmed recognizer bug; marker kept by evidence.** Unlinking a child
   with uploaded but unpublished overlay bytes journals `AddPendingDeletion`
   markers between the fence and the delete, so `child_purge_event_span` did not
   recognize the span: such a losing Clear could never retire and would keep
   the journal pinned. The recognizer now absorbs those leading markers into the
   span. The local `PENDING_DELETIONS_TABLE` row is deliberately kept: the purge
   already removed the only local reference, the marker events are local-only
   (stripped by projection). *Corrected in the orphan-close slice below:* there
   are two physical consumers, not one. `retire_abandoned_uploads`
   (`RETIRE_ABANDONED_UPLOADS_SQL` / `ABANDONED_UPLOADS_PREDICATE`) retires only
   `uploaded` objects with no `snapshot_current_references` row and no pin, after
   the whole local journal is acknowledged. The VFS background GC worker
   (`crates/studiofs-vfs/src/vfs/workers.rs`, `get_pending_deletions_limited`)
   also deletes S3 fragments from local markers, including published base
   objects, skipping only objects in `get_referenced_object_ids` (local refs plus
   PG current references), snapshot pins, the grace period, or a `Protected`
   deletion claim. A PG-referenced object is therefore protected only while its
   shared current reference exists, which is why a losing purge must never
   publish an orphan finalization (fixed below), and cancelling the marker would
   leak a truly abandoned upload. The offline
   accepted-bytes-survive-peer-purge guarantee is untouched (different path).
5. **LOW, no change.** View handles are keyed by the view inode, so
   `open_count(child.ino)` sees only source-inode handles (opened before Move to
   Trash). WinFSP is mounted without POSIX unlink semantics, so
   `FspCleanupDelete` arrives only on the last cleanup of the name-keyed file
   node: no other view handle can be open when the child delete runs. Source
   handles keep the orphan-until-close path. No regression test was added
   because no behavior changed.
6. **LOW, not investigated.** The recursive `$I` size walk was not needed for
   any proof here; no cache was added.

#### Evidence (logs under `target/`)

- HIGH RED, real PostgreSQL (unique per-run volume, `.env` loaded by the test
  helper, process-local `NO_PROXY=*`):
  `test_pg_retained_losing_child_purge_keeps_published_unacked_span` publishes
  child purge 1 without self-ack (`claim_pending_batch` +
  `flush_pending_claimed_batch_with_restore`), lets the peer restore, then
  pushes child purge 2 through `flush_pending_claimed_batch` (made
  `pub(super)` as a test seam) before any replay and crashes before the recovery
  pull. RED: `left: 0, right: 1` live spans — the published span was rewritten
  (`sfs1054-partial-clear-review-high-red.log`). The visible end state
  converged even before the fix (the cursor advances through the published
  prefix ledger and the batch then applies as foreign), so the defect was the
  rewritten PG-committed journal, not an observed stuck cursor.
- HIGH GREEN plus the PG retained regression set: 12 passed
  (`sfs1054-partial-clear-review-high-green.log`); the test also checks journal
  drain, peer convergence, and restart.
- Broader PG set `-- --ignored --test-threads=2 test_pg_retained retained_losing
  retained_competing lost_terminal`: 21 passed, 1 failed
  (`sfs1054-partial-clear-review-pg.log`). The failure,
  `test_pg_retained_purge_settlement_reclaims_through_gc_queue`, also fails alone
  (`sfs1054-partial-clear-review-pg-gcqueue.log`) at `get_global_snapshot_pins`
  with `snapshot reachability bootstrap incomplete`: the janitor bootstrap guard
  dates from an August commit, the fixture never seeds
  `snapshot_reachability_bootstrap`, and this slice changes neither path. It is
  recorded as outside this slice and was not fixed. The first parallel run hit
  `too many clients` (infrastructure).
- GC RED: `retained_child_purge_uploaded_overlay_span_is_recognized_and_retires`
  failed with an unrecognized `[SetXattr, AddPendingDeletion, RemoveLocalEntry,
  RemoveRegularFileGraph, RemoveAttr]` span (`sfs1054-partial-clear-review-gc-red.log`).
- redb GREEN: `cargo test -p studiofs-meta --lib redb_engine::retained::tests::`
  — 23 passed (`sfs1054-partial-clear-review-redb-retained.log`), including the
  coverage tests and `retained_child_purge_reconcile_handles_ten_thousand_spans`
  (10,000 synthetic rmdir spans, 40,000 events retired in about 0.53 s, debug
  build; `sfs1054-partial-clear-review-redb-spans.log`). The timing is printed,
  not asserted; the work bound is structural (one pass plus O(1) lookups).
- `cargo test -p studiofs-meta --lib retained`: 32 passed, 26 ignored
  (`sfs1054-partial-clear-review-meta-retained.log`).
- `cargo clippy -p studiofs-meta --all-targets -- -D warnings`: exit 0
  (`sfs1054-partial-clear-review-clippy.log`); `cargo fmt -p studiofs-meta`
  applied; `git diff --check` clean.

#### Remaining limits

- No VFS, WinFSP, or native change in this slice, so no native rerun.
- The PG dedupe has no round-trip counter test; correctness is covered by the
  PG conflict tests (a conflicting fence still reads, then fails).
- The 10k test bounds work structurally, not by an instrumented counter.
- The uploaded-overlay GC decision is proven from code (the abandoned-upload
  predicate), not by a PG/S3 run.
- A rejected-range ack barrier after a restart has no in-memory published
  ledger; foreign apply then relies on PG order (a published span precedes the
  winner and is acknowledged first). Not separately tested.
- Not run: workspace/full suites, Linux/macOS, commits, PRs, Linear.

### Orphan finalization, loser overlay restore, and deferred-orphan retry (2026-11-15)

Scope: the fresh REQUEST-CHANGES review (HIGH orphan close, MEDIUM pre-span
touches, MEDIUM GC-queue fixture, LOW GC-consumer docs) plus the three gaps the
first pass left open (closed-file purge graph loss, pre-span dentry changes,
deferred winners waiting for restart). Earlier exact-span retire, PG dedupe and
recognizer fixes were not reopened.

#### Contract and implementation

- **Shared orphan guard.** `MetaRecovery::finalize_orphan_inode` (redb
  `finalize_orphan_inode_guarded`, `payload_objects.rs`) checks and deletes in
  one write transaction: `Live` when the merged inode has `nlink > 0` (drops
  only its deferred-delete record), `Deferred` when the deferred record's parent
  is beneath a receipt and an unacknowledged `purge_retained_child` span for that
  inode is still in the journal (`unacknowledged_retained_child_purge_in_txn`,
  scan bounded to receipt descendants), else `Removed` (graph, pending
  deletions, attr, deferred record, used-space counter in one commit; an
  already-finalized inode with no record is an idempotent empty `Removed`). The
  PG engine passes `defer=true`; redb open-time replay defers when the persisted
  format backend is `Postgres`. Restart replay (`recover_undo_logs_guarded`),
  VFS last close, background abandoned cleanup and rename-overwrite all use it;
  `set_undo_log_phase_for` became dead and was removed.
- **VFS.** `collect_orphan_deleted_inode_cleanup` returns `None` for
  `Live`/`Deferred`, clears the upload tombstone so accepted bytes still upload,
  and the last-close path then runs an ordinary local sync instead of runtime
  cleanup (an inode the sweep already finalized is tolerated).
- **Closed-file purge keeps its graph.** `purge_retained_child` lost its
  `is_open` parameter: every file purge is an `UnlinkOpenHandle`-style deferred
  delete, so neither base nor accepted overlay rows are cleared before shared
  authority settles the purge. The VFS no-handle branch finalizes through the
  guard (immediately on redb-only volumes, deferred on PG until acknowledged).
- **Loser reconcile.** One pass records each key's latest surviving touch; only
  a touch after `span.end` keeps the span's state. Otherwise the child's
  tombstone, lifecycle and deferred record go, its overlay row is restored from
  the latest earlier `SetAttr` (or dropped to base), the name is rebound from the
  latest earlier `AddEntry`/`RenameEntry` binding, and the retired span's own
  `AddPendingDeletion` markers are withdrawn (the restored child still
  references those bytes).
- **Bounded retry.** `run_retained_lifecycle_once` (PG poll loop, every
  `RETAINED_LIFECYCLE_INTERVAL` = 60 s while `Live`) now also retries up to
  `ORPHAN_FINALIZE_BATCH` = 1024 deferred-delete records per sweep whose inode
  has no open handle in `open_handle_inodes`
  (`RetainedLifecycleSweep::orphans_finalized`). A winning purge finalizes
  within one interval after its acknowledgement and last close.
- **GC fixture.** `test_pg_retained_purge_settlement_reclaims_through_gc_queue`
  calls `backfill_current_reference_ledger_if_needed` (`sync_all_pages` skips
  `sync_metadata` at the head) and asserts the bootstrap before the existing pin
  and `Protected` checks; the janitor guard is unchanged.
- **Docs.** The GC item in the previous section is corrected (the VFS background
  GC worker is a second physical consumer of pending markers); the spec records
  the orphan-finalization rule.

#### Evidence (logs under `target/`, real PostgreSQL with a unique volume per run, `.env` loaded by the test helper, process-local `NO_PROXY=*`, `--test-threads=1`)

- HIGH RED by mutation (guard checks disabled): close before classification
  returned `Removed { removed_ids: [62001] }` (`sfs1054-child-orphan-red.log`).
  GREEN `test_pg_retained_open_child_purge_orphan_finalize_respects_live_binding`
  (close before / after classification, restart before classification, winner
  finalized by restart replay, winner finalized by the running sweep): live
  binding, exact restored object, no queued deletion, no PG
  `RemoveAttr`/`RemoveRegularFileGraph`, `Protected` claim, reopen
  (`sfs1054-child-orphan-gaps-green.log`). Sweep RED (`.take(0)`): `left: 0,
  right: 1` (`sfs1054-child-orphan-sweep-red.log`).
- MEDIUM SetAttr: RED (old ownership rule) `InodeNotFound` after loser
  reconcile; GREEN chmod kept on both clients
  (`sfs1054-child-orphan-setattr-{red,green}.log`).
- Gap 1 `test_pg_retained_losing_file_purge_keeps_local_graph_and_overlay`
  (published base, uploaded unpublished overwrite, closed-file purge, peer
  restore first): RED (old `new_unlink` spec) finalize `Removed` instead of
  `Deferred`; GREEN child visible with the overwrite object locally before any
  pull, no marker, uploader publish succeeds, both clients converge on the
  overwrite (`sfs1054-child-orphan-gaps-red-*.log`,
  `sfs1054-child-orphan-gap1-green.log`).
- Gap 2 `test_pg_retained_losing_child_purge_restores_pre_purge_rename_and_add`:
  RED (bindings not reapplied) `EntryNotFound "b"`; GREEN renamed and added
  children restored locally and converged on both clients.
- Restart foreign-apply variant
  `test_pg_retained_losing_child_purge_restart_foreign_apply_keeps_published_span`:
  passed without a code change (`sfs1054-child-orphan-restart-foreign.log`).
- GC queue: 1 passed with bootstrap asserted (`sfs1054-child-orphan-gcqueue.log`).
- Focused PG set `test_pg_retained retained_losing retained_competing
  lost_terminal`: 25 passed before the gap fixes
  (`sfs1054-child-orphan-pg-set.log`), 27 passed after
  (`sfs1054-child-orphan-pg-set2.log`).
- Units: meta `retained` 32 passed (`sfs1054-child-orphan-meta-retained.log`);
  VFS `retained_views::` 38, `orphan` 17, `vfs_test retained_` 5 passed.
  `retained_child_purge_uploaded_overlay_span_is_recognized_and_retires` now
  asserts the loser keeps the overlay referenced and withdraws its marker; the
  orphan-cleanup unit test asserts one metadata transaction.
- `cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs-winfsp
  --all-targets -- -D warnings`, `cargo fmt --check` and `git diff --check`:
  exit 0.
- Native WinFSP, rebuilt `mount_sanity-0ccc233bed46233d.exe`, `W:` free before
  and after each run, sequential, no Recycle Bin clear:
  `winfsp_native_trash_directory_partial_clear_shell_probe` passed
  (`after_child_delete i_size=9 nested_gone=True a_ok=True`,
  `restored_a_ok=True restored_nested_absent=True pairs_after_restore=0`) and
  `winfsp_native_mapped_writer_survives_handle_close_probe` passed
  (`sfs1054-child-orphan-native-{partial-clear,mapped}.log`). Native volumes are
  redb-only, so they exercise the guard's immediate `Removed` path, not deferral.

#### Remaining limits

- A sweep-finalized orphan skips VFS runtime cleanup; its local cache payloads
  are left for ordinary cache eviction/reconcile.
- Dentry restore uses the latest earlier binding event; it does not rebuild
  directory contents removed by an earlier local rmdir inside the same tree.
- The PG loser tests drive the meta API the VFS calls (purge plus
  `finalize_orphan_inode`), not a mounted VFS with PG.
- Not run: workspace/full suites, Linux/macOS, commits, PR, Linear.

### Orphan upload ordering, origin-independent guard, finalize cost (2026-11-15)

Scope: the fresh scoped review of the orphan-finalization slice (F1–F5). The
atomic finalize, pre-span touch reconcile and exact-span retire were reviewed
correct and were not reopened; the earlier pre-span rmdir review found no data
loss and was not reopened.

#### Findings and decisions

1. **F1 HIGH, confirmed (VFS).** `collect_orphan_deleted_inode_cleanup` marked
   the inode deleted for uploads before the metadata guard ran. That cancelled
   the inode's upload token (it stays cancelled in `upload_cancel_tokens`) and
   let the uploader drop queued work while `run_meta` awaited, even when the
   guard then returned `Live`/`Deferred`. The ad hoc tombstone removal also broke
   the insert-only `deleted_inodes` invariant the uploader sweep asserts, and the
   open-handle retained purge marked at purge time, before any finalization.
   Fix: the tombstone is set only on `Removed` (after the metadata commit); the
   retained open-handle purge no longer marks; the ad hoc removal is gone. Late
   uploads after `Removed` stay protected by the existing uploader gates: the
   tombstone short-circuit, `get_attr` → `None` cancel, and the stale-epoch /
   overlay-object CAS under the inode upload gate.
2. **F2 HIGH, confirmed (meta).** `unacknowledged_retained_child_purge_in_txn`
   derived "beneath a receipt" from the current parent chain. After a local
   restore moved the purge parent out, an unacknowledged purge finalized as
   `Removed`, and its `RemoveRegularFileGraph`/`RemoveAttr` publish outside the
   fence. Fix: the guard looks only for an unacknowledged `purge_retained_child`
   span whose `child_ino` matches. The new counter
   `counters::LOCAL_RETAINED_CHILD_PURGE_SEQ` (fence sequence of the latest child
   purge, written in the purge commit) replaces the cost bound the path walk
   gave: below `LOCAL_PG_FLUSHED_EVENT_SEQ` no span can be unacknowledged and the
   guard does no scan. The check and delete stay in one write transaction.
3. **F3 MEDIUM, fixed.** `UnackedChildPurges` carries the scanned set across the
   write transactions of one pass. Each transaction decodes only the journal
   delta since the previous one (commits append whole spans), so a sweep batch
   or restart replay is O(journal + batch). A cached entry can only be stale
   toward "unacknowledged" (acks and loser retires shrink the true set), which
   defers but never deletes; a purge committed mid-pass is found by the next
   delta. The VFS no-handle Clear branch skips the immediate finalize when
   `local_outbound_ack_snapshot()` reports an unacknowledged journal (the
   just-journaled purge always is on PG); the sweep finalizes it later.
4. **Last close vs. concurrent sweep (asked to trace).** Confirmed hazard:
   `apply_regular_file_overlay_mutation_in_txn` accepts `InodeNotFound`, so a
   `Deferred` last-close barrier racing a sweep `Removed` could recreate the
   inode's overlay (orphan rows and a published `SetAttr`). Fix: the last close
   registers the inode in `open_handle_inodes` before its finalize and keeps it
   through the barrier; the sweep rechecks the registry inside its
   `mutate_with_hint` closure (under `pending_state`, which every finalize
   takes). A sweep finalize ordered after the close's `Deferred` sees the
   registration; one ordered before makes the close's finalize return an empty
   `Removed`, so no barrier runs. This is proven by the ordering argument, not by
   an interleaving test. A dropped release future leaves the count raised, which
   only keeps the sweep off that inode.
5. **F4 LOW, documented.** The loser reconcile withdraws markers by object id.
   `PENDING_DELETIONS_TABLE` has no provenance, so a packed object another delete
   also queued loses that marker. This is a bounded leak, never a deletion, and
   `studiofs janitor` remote orphan discovery reclaims it. Recorded as a
   `ponytail:` comment; no provenance framework was added.
6. **F5 LOW, fixed narrowly.** One failing deferred finalize no longer aborts the
   sweep. It is counted in `RetainedLifecycleSweep::orphan_finalize_errors` and
   logged with the inode number (local redb error, no credentials); its
   transaction aborts, so nothing is deleted and the record stays for the next
   sweep. `nlink > 0` → `Live` drops only the deferred record, which is the
   hard-link invariant. An `nlink > 0` inode with no dentry would be existing
   corruption; it is left alone (no deletion) and not widened here.

#### Evidence (logs under `target/`; real PostgreSQL with a unique volume per run, `.env` loaded by the test helper, process-local `NO_PROXY=*`, `--test-threads=1`)

- F2 RED `test_pg_retained_child_purge_then_local_restore_defers_orphan_finalize`
  (B restores and publishes; A purges the open child, restores R locally, then
  last close): `left: Removed { removed_ids: [64001], freed_bytes: 64 }, right:
  Deferred` (`sfs1054-orphan-upload-f2-red.log`). GREEN for both last close and
  the no-handle sweep: deferred, no PG event touches the child, B keeps the
  child bound with its restored bytes, `Protected` claim
  (`sfs1054-orphan-upload-f2-green.log`). The sweep variant was not run RED on
  its own (the loop stopped at the first variant).
- F1 RED (old VFS hunks restored temporarily):
  `orphan_finalize_of_live_inode_keeps_accepted_upload` failed with "a kept
  inode must not cancel its in-flight upload", and
  `partial_directory_clear_with_outside_hardlink_and_open_handle_preserves_live_data`
  failed on the purge-time tombstone (`sfs1054-orphan-upload-f1-red.log`).
  Note on historical artifact: `sfs1054-orphan-upload-f1-green.log` failed on the
  superseded `the accepted payload must finish uploading` S3-completion assertion
  (`tests.rs:17804`) and is preserved untouched on disk. Actual GREEN proof for
  both tests (`orphan_finalize_of_live_inode_keeps_accepted_upload` and
  `partial_directory_clear_with_outside_hardlink_and_open_handle_preserves_live_data`)
  is in `sfs1054-orphan-upload-vfs-focused.log` (136 passed) and the fresh
  `sfs1054-repair-sweep-vfs-focused.log` (137 passed): the kept inode's token
  is not cancelled, a new upload attempt does not start cancelled, and the
  uploader still claims the payload (uploader gates only; the unit VFS has no
  reachable object store so no real S3 PUT is exercised). Deferred takes the same
  VFS arm as Live; its metadata outcome is proven by the PG tests.
- F3 `orphan_guard_scans_unacked_purges_once_per_pass_and_skips_when_acked`
  (300 purged children plus one mid-pass purge): GREEN decodes the journal at
  most once and the acknowledged case decodes nothing; RED by mutation (delta
  start ignored): `637520 > 2120` (`sfs1054-orphan-upload-f3-red.log`).
- F5 `test_pg_retained_lifecycle_sweep_continues_past_failing_orphan`: GREEN
  `(1, 1)` with the failing record kept (`sfs1054-orphan-upload-f5-green.log`);
  RED by mutation (return on error): the sweep failed with the poison
  `Serialization` error (`sfs1054-orphan-upload-f5-red.log`).
- Focused PG set `test_pg_retained retained_losing retained_competing
  lost_terminal`: 29 passed (the earlier 27 plus the two new tests) in 467 s
  (`sfs1054-orphan-upload-pg-set.log`).
- Meta units `retained orphan undo deferred`: 56 passed
  (`sfs1054-orphan-upload-meta-unit.log`). VFS `orphan retained_views::
  deleted_inode upload_lifecycle::worker ignored_entries::`: 136 passed
  (`sfs1054-orphan-upload-vfs-focused.log`); `vfs_test retained_`: 5 passed
  (`sfs1054-orphan-upload-vfs-integration.log`).
- `cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs-winfsp
  --all-targets -- -D warnings` exit 0 (`sfs1054-orphan-upload-clippy.log`),
  `cargo fmt --all -- --check` exit 0, `git diff --check` clean.
- Native WinFSP, rebuilt `mount_sanity-0ccc233bed46233d.exe`, sequential, `W:`
  free before and after each run, host Recycle Bin never cleared:
  `winfsp_native_trash_directory_partial_clear_shell_probe` passed
  (`after_child_delete i_size=9 nested_gone=True a_ok=True`,
  `restored_a_ok=True restored_nested_absent=True pairs_after_restore=0`) and
  `winfsp_native_mapped_writer_survives_handle_close_probe` passed
  (`sfs1054-orphan-upload-native-{partial-clear,mapped}.log`). Native volumes
  are redb-only, so they take the immediate `Removed` path.

#### Remaining limits

- The last-close/sweep race is closed by lock ordering, not by an interleaving
  test.
- F1 upload completion against real object storage is not exercised in the VFS
  unit; only the uploader gates are.
- F4 leaves a bounded, janitor-recoverable marker leak for shared packed
  objects.
- Not run: workspace/full suites, Linux/macOS, commits, PR, Linear.

### Repair Counter & Fair Sweep Pass (post-review fixes)

1. **`LOCAL_RETAINED_CHILD_PURGE_SEQ` preserved across repair checkpoint import.**
   Added `crate::schema::counters::LOCAL_RETAINED_CHILD_PURGE_SEQ` to
   `is_local_pg_snapshot_state_counter` (`crates/studiofs-meta/src/pg_engine.rs`)
   so `ReplaceLocalStatePreservingEventLog` /
   `import_serving_checkpoint_preserving_local_event_log` retains the sequence
   counter while `export_remote_snapshot_from_read_txn_with_check` excludes it
   from remote checkpoints.
2. **Fair bounded traversal in `run_retained_lifecycle_once`.**
   Added a process-local `retained_orphan_cursor: AtomicU64` on `PgMetaEngine`
   and refactored the deferred-orphan finalization loop in
   `crates/studiofs-meta/src/pg_engine/retained.rs` (`finalize_deferred_orphans_batch`
   + lock-free `take_fair_orphan_batch` using `AtomicU64::try_update`) to resume
   after the last visited inode with wrap-around, capped to
   `ORPHAN_FINALIZE_BATCH` (1,024) per pass, keeping no lock across awaits,
   keeping the atomic `open_handle_inodes` recheck under `mutate_with_hint`, and
   retaining the incremental `UnackedChildPurges` guard.
3. **Cancellation-safe open-handle registration in VFS orphan cleanup.**
   Replaced manual `meta.note_open_handle(ino, true/false)` in
   `StudioVfs::cleanup_orphan_on_last_handle_close`
   (`crates/studiofs-vfs/src/vfs/sync_ops.rs`) with a local `Drop` guard
   (`OpenHandleRegistrationGuard`) that keeps registration active across
   `collect_orphan_deleted_inode_cleanup` and `local_sync_barrier_with_outcome`,
   and unregisters on completion, error, or mid-await cancellation.

#### Evidence (`target/sfs1054-repair-sweep-*.log`; `.env` loaded silently for PG tests, process-local `NO_PROXY=*` and `no_proxy=*`, `--test-threads=1`)

- Item 1 RED (`cargo test -p studiofs-meta --lib pg_engine::tests::lifecycle::retained_child_purge_seq_survives_repair_checkpoint_import_and_defers_orphan_finalize -- --exact --nocapture`): failed with `left: Removed { removed_ids: [66001], freed_bytes: 64 }, right: Deferred` (`sfs1054-repair-sweep-item1-red.log`). GREEN after fix (`sfs1054-repair-sweep-item1-green.log`): 1 passed in 0.03s.
- Item 2 RED (`cargo test -p studiofs-meta --lib pg_engine::tests::lifecycle::test_pg_retained_lifecycle_sweep_advances_past_deferred_and_failing_low_inodes -- --ignored --exact --test-threads=1 --nocapture`): 1,020 synthetic poison low inodes + 10 unacknowledged `Deferred` low inodes (>1,024 batch) starved the acknowledged healthy high inode on pass 2 (`left: 0, right: 1`; `sfs1054-repair-sweep-item2-red.log`). GREEN after fix (`sfs1054-repair-sweep-item2-green.log`): 1 passed in 6.15s (pass 2 visits `1,024` bounded items, finalizes the healthy high inode, leaves unacked payloads and poison records intact). Unit test `deferred_orphan_sweep_batch_wraps_and_respects_concurrent_open_handle_recheck` also passed (batch=3 wrap-around + concurrent `std::thread::scope` sweeps + open-handle skip).
- Item 3 RED (`cargo test -p studiofs-vfs --lib vfs::tests::cancelled_orphan_cleanup_unregisters_open_handle_without_dropping_active_barrier -- --exact --nocapture`): offline `PgMetaEngine` + `hold_deferred_namespace_drain` parked `cleanup_orphan_on_last_handle_close` at `wait_for_deferred_namespace_metadata`; dropping the pinned future leaked `open_handle_registration_count == 1` (`sfs1054-repair-sweep-item3-red.log`). GREEN after `OpenHandleRegistrationGuard` (`sfs1054-repair-sweep-item3-green.log`): 1 passed in 0.07s (registration stays `1` while parked across finalize + barrier, drops to `0` on mid-await drop and on normal completion).
- Meta focused units (`cargo test -p studiofs-meta --lib -- retained orphan undo deferred`): 58 passed, 0 failed, 34 ignored in 1.69s (`sfs1054-repair-sweep-meta-unit.log`).
- VFS focused units (`cargo test -p studiofs-vfs --lib -- orphan retained_views:: deleted_inode upload_lifecycle::worker ignored_entries::`): 137 passed, 0 failed in 7.78s (`sfs1054-repair-sweep-vfs-focused.log`, includes `orphan_finalize_of_live_inode_keeps_accepted_upload`).
- PG focused controls (`cargo test -p studiofs-meta --lib -- --ignored --test-threads=1 test_pg_retained_lifecycle_sweep test_pg_retained_child_purge_then_local_restore_defers_orphan_finalize test_pg_retained_open_child_purge_orphan_finalize_respects_live_binding redb_repair_preserves_sparse_published_graph_and_pending_overlay`): 5 passed, 0 failed in 97.28s (`sfs1054-repair-sweep-pg-focused.log`).
- Hygiene: `cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings` exit 0 (`sfs1054-repair-sweep-clippy.log`); `cargo fmt --all -- --check` exit 0; `git diff --check` exit 0.

#### Remaining limits (updated)

- `orphan_finalize_of_live_inode_keeps_accepted_upload` checks uploader gates only; real S3 PUT completion is not exercised in the unit VFS test.
- Native WinFSP probe was not re-run for this pass because the VFS change (`OpenHandleRegistrationGuard`) only cleans up open-handle registrations on future cancellation/completion and redb-backed WinFSP volumes use no-op `note_open_handle`.
- Not run: full workspace/all-ignored suites, Linux/macOS, commits, push, PR, Linear.

**Independent review status:** The Repair Counter / Fair Sweep / RAII `OpenHandleRegistrationGuard` checkpoint above was independently APPROVED and left untouched.

### Remote-Only Recovery & Windows Retry UX Slice (2026-11-15)

Scope: existing foreground `restore_retained_entry` / background `PrefetchWorkItem::RestorePayload` seams, `VfsConfig` default transit budget resolution, focused unit tests, and native WinFSP (`W:`) + real S3 + Windows Explorer Shell retry UX probe. No synchronous remote callbacks in foreground VFS/WinFSP, no `Close` waiting, and no unrequested Shell UX policy changes were introduced.

#### Findings and fixes

1. **`RestorePayload` background fetch skipped under foreground reserve (`crates/studiofs-vfs/src/vfs/prefetch.rs`, `crates/studiofs-vfs/src/vfs/io_ops.rs`).**
   - `PrefetchWorkItem::RestorePayload` previously called `fetch_block_prefetch`, which was subject to `prefetch_foreground_reserve` (default reserve 16; the test window caps the effective reserve at 2 slots). On an evicted 10-file directory, objects after the first 2 hit `FetchError::PrefetchSkipped`, silently preventing background hydration and stalling retry.
   - Fix: route `RestorePayload` through `fetch_block` (foreground-equivalent singleflight + AIMD + QoS + transit budget, still concurrency-bounded by `prefetch_sem`) and log unexpected non-shutdown `fetch_block` failures via `tracing::warn!`. Fixed `enqueue` loop in `StudioVfs::restore_retained_entry` (`io_ops.rs`) to exit `'enqueue` when the bounded ingress channel is full (`break 'enqueue;`).
2. **Zero `transit_memory_budget_bytes` override from `VfsConfig::default().try_into()` (`crates/studiofs-vfs/src/vfs/config.rs`, `crates/studiofs-vfs/src/vfs.rs`).**
   - `VfsConfig::default()` sets `transit_memory_budget_bytes = DEFAULT_TRANSIT_MEMORY_BUDGET_BYTES` (`0`, sentinel for auto-derivation), yet `TryFrom<VfsConfig> for VfsConfigValidated` marked `transit_memory_budget_bytes: true` in `CacheRuntimeTuningOverrides`, and `resolve_cache_runtime_tuning_overrides` unconditionally took `0`. `TransitMemoryBudget::new(0)` clamped to `1` byte, causing `FetchScheduler` to abort every S3 GET immediately with `FetchError::TransitBudgetTooSmall` (discovered on the first native `W:` trace).
   - Fix: `TryFrom<VfsConfig>` only marks `transit_memory_budget_bytes` overridden when `!= DEFAULT_TRANSIT_MEMORY_BUDGET_BYTES` (`0`), and `resolve_cache_runtime_tuning_overrides` falls back to `derived.transit_memory_budget_bytes` when the sentinel `0` is supplied. Added `vfs_config_default_try_from_does_not_force_zero_transit_budget`.
3. **Windows retryable mapping and observed Shell UX (`crates/studiofs-winfsp/src/context.rs`, `docs/platform-bridges.md`).**
   - `VfsError::RemoteContentPending` → `BridgeVfsErrorKind::Retryable` → `STATUS_UNSUCCESSFUL` (Win32 `ERROR_GEN_FAILURE` / `31`).
   - Observed natively under Windows Explorer (`NameSpace(10)` `InvokeVerb("undelete")`): first attempt on an evicted file fails fast (`666 ms`), surfaces `OperationStatusWindow` (`"1 Interrupted Action"`), preserves `$I`/`$R` and `Retained` receipt, does **not** hang or fall back to copy+delete, triggers background S3 fetch, and retry succeeds silently (`427 ms`, `dialogs=0`) restoring exact bytes and original `ino`.
   - Unresolved product UX decision (intentionally left unchanged): whether `RemoteContentPending` should keep mapping to `STATUS_UNSUCCESSFUL` / `"1 Interrupted Action"` (manual retry after hydration) or use another NTSTATUS / Shell-friendly status code without causing blocking callbacks or unwanted Explorer copy+delete fallback.

#### Evidence (`target/sfs1054-remote-recovery-*.log`; `.env` loaded silently, process-local `NO_PROXY=*` / `no_proxy=*`, sequential `W:` free before/after)

- **RED** (before `RestorePayload` fix, `sfs1054-remote-recovery-vfs-red.log`): `trash_view_remote_only_directory_triggers_background_fetch_for_all_missing_files_and_retries` timed out with `remaining` uncached objects and `prefetch_skips=6`.
- **RED (native trace, before config fix)** (`sfs1054-remote-recovery-trace.log` / `sfs1054-remote-recovery-native-trace-run.log`): `transit_memory_budget_bytes=0`, zero S3 GETs after `VFS rename failed ... error=remote content is still uploading (EAGAIN)`.
- **GREEN VFS units (final tree):**
  - `cargo test -p studiofs-vfs --lib trash_view_remote_only_ -- --nocapture` (`sfs1054-remote-recovery-vfs-green.log`): **3 passed, 0 failed** (2.37s) — covers evicted single file, multi-block partial cache (block 0 cached, block 1 remote-only, stage cleanup + targeted fetch + retry), and 10-file mixed directory.
  - `cargo test -p studiofs-vfs --lib vfs::config::tests -- --nocapture` (`sfs1054-remote-recovery-config-tests.log`): **11 passed, 0 failed** (0.00s).
  - `cargo test -p studiofs-vfs --lib "prefetch::tests" -- --nocapture` (`sfs1054-remote-recovery-prefetch-tests.log`): **10 passed, 0 failed** (0.09s).
  - `cargo test -p studiofs-vfs --lib "retained_views::tests" -- --nocapture` (`sfs1054-remote-recovery-retained-views.log`): **8 passed, 0 failed** (0.13s).
- **GREEN Native WinFSP (`W:`) + real S3 + Windows Explorer (`mount_sanity` rebuilt immediately prior, `target/debug/deps/mount_sanity-0ccc233bed46233d.exe`):**
  - `cargo test -p studiofs-winfsp --test mount_sanity winfsp_native_trash_remote_only_retry_probe -- --ignored --exact --nocapture` (`sfs1054-remote-recovery-native.log`): **1 passed, 0 failed** (6.90s), `W_FREE_AFTER=True`, owned S3 prefix cleaned up:
    - Phase 1 evicted dir (`MoveFileEx`): first attempt `move_ok=False win32_err=31 elapsed_ms=20`, receipt stays `Retained`, `$I`/`$R` intact, background S3 fetch populates objects, retry `move_ok=True win32_err=0 elapsed_ms=44`, exact bytes + original inodes (`dir`/`sub`/`a.bin`/`b.bin`) + `Restored` receipt + `$I`/`$R` cleaned.
    - Phase 2 evicted file (`InvokeVerb('undelete')`): first attempt `shell_exited=True exit_code=0 elapsed_ms=666 dialogs=1 dialog_details=...|OperationStatusWindow|1 Interrupted Action|[]`, background S3 fetch populates object, retry `shell_exited=True exit_code=0 elapsed_ms=427 dialogs=0`, exact `65536` bytes + `orig_file_ino` preserved + `Restored` receipt + `$I`/`$R` cleaned.
- **Hygiene:** `cargo clippy -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings` exit 0 (`sfs1054-remote-recovery-clippy.log`); `cargo check -p studiofs-vfs -p studiofs-winfsp --tests` exit 0 (`sfs1054-remote-recovery-check.log`); `cargo fmt --all -- --check` and `git -c core.safecrlf=false diff --check` were independently re-run successfully during review; the originally named standalone log files were not retained.

#### Remaining limits / prerequisites

- Native WinFSP probe exercises real S3 `DataStore` + local `RedbMetaEngine` (no PG DSN present in `.env`); combined mounted WinFSP + live PostgreSQL + S3 remote-only recovery remains unprobed end-to-end on `W:`.
- `STATUS_UNSUCCESSFUL` (`ERROR_GEN_FAILURE` / `31`) and the resulting Windows Explorer `"1 Interrupted Action"` dialog policy were observed and documented, not redesigned.
- Not run: full workspace suites, Linux/macOS native mounts, commits, push, PR, Linear.



**Independent review:** APPROVED for the remote-only hydration, transit-budget sentinel and native retry probe slice. Reviewer re-ran the 3 remote-only, 11 config, 10 prefetch and 8 retained-view tests successfully; native S3 evidence was inspected rather than re-run. The reserve and RED skip count above were corrected against source and retained artifacts.

### Linux/macOS Native Trash Mapping Slice (2026-11-15 — Paused Checkpoint)

Scope: `NativeTrashProfile` cross-OS volume trash views (`Windows` `$RECYCLE.BIN`, `Linux` `.Trash-<uid>/{files,info}`, `MacOs` `.Trashes/<uid>`), zero-TTL/cache-bypass hooks in Linux FUSE and macFUSE/FSKit adapters, and focused VFS/Linux unit + mounted `gio trash` test coverage. Paused mid-verification per user request before completing GREEN verification runs.

#### Implemented (saved in working tree)

1. **Shared VFS POSIX trash projection & operations (`crates/studiofs-vfs/src/lib.rs`, `src/vfs.rs`, `src/vfs/ignored_entries.rs`, `src/vfs/retained_views.rs`, `src/vfs/read_path.rs`, `src/vfs/namespace_ops.rs`):**
   - Added `NativeTrashProfile` (`Windows` | `Linux { uid }` | `MacOs { uid }`), `set_native_trash_profile` / `native_trash_profile`, `should_bypass_bridge_entry_cache`, and `is_generated_trash_record_ino` (`StudioVfs`).
   - Added `TrashContainerRole` (`WindowsRoot`, `LinuxRoot`, `LinuxFiles`, `LinuxInfo`, `MacOsRoot`, `MacOsUser`) and extended `TrashViewBacking` (`VirtualTrashRoot`, `LinuxFilesDir`, `LinuxInfoDir`, `MacOsUserDir`, `Bytes { receipt, data }`, `Payload { receipt }`, `Subtree`).
   - Implemented deterministic canonical view naming across OS profiles, RFC 2396 percent-encoded `.trashinfo` (`[Trash Info]\nPath=...\nDeletionDate=YYYY-MM-DDTHH:MM:SS\n`) via `chrono`, client-local draft `.trashinfo` isolation (`Ignored` entries cleaned on trash rename under both `TTL > 0` and `TTL == 0`), `.DS_Store`/AppleDouble `.Trashes/<uid>` isolation, hiding of non-active root trash directories (`$RECYCLE.BIN` / `.Trash-*` / `.Trashes`), virtual trash root projection when retention exists without touching metadata on read-only mounts, size-preserving container `setattr` no-op normalization, restore collision no-overwrite (`EntryExists`), bottom-up `rmdir`/`unlink` purge (including Linux `.trashinfo` Clear), and current `original_parent` ACL enforcement.
2. **Linux FUSE adapter (`crates/studiofs-linux-fuse/src/low_level.rs`):**
   - `SessionState::new` configures `NativeTrashProfile::Linux { uid: mount_uid }`.
   - Added `fuse_ttl_for_ino(ino)` returning `0.0` when `should_bypass_bridge_entry_cache(ino)` and wired `entry_param` (`entry_timeout` + `attr_timeout`), `getattr`, `setattr`, `link`, `readdirplus`, and `reply_reserved_entry_with` (`lookup`/`mkdir`/`mknod`/`symlink`/`create`).
   - Forced `OsCacheParticipation::Disabled` + `direct_io: true` + `keep_cache: false` in `prepare_file_open` for `is_generated_trash_record_ino(ino)`, and bypassed `directory_snapshots` caching in `directory_snapshot` when `should_bypass_bridge_entry_cache(ino)` is true.
   - Added unit test `linux_fuse_session_configures_linux_profile_zero_ttl_direct_io_and_trash_containers` and ignored mounted WSL2 test `mounted_linux_gio_trash_retains_lists_restores_and_clears`.
3. **macFUSE / FSKit adapter (`crates/studiofs-macfuse/src/adapter.rs`, `crates/studiofs-macfuse/src/fskit.rs`):**
   - `StudioVfsAdapter` configures `NativeTrashProfile::MacOs { uid: mount_uid }`, adds `entry_ttl(ino)`, wires zero TTL into all `Entry::new` / `Reply::attr` and FSKit `entry_from_stat` / `attr_out_from_stat` replies, disables OS open page caching (`direct_io: true`, `keep_cache: false`) for `is_generated_trash_record_ino(ino)`, and adds unit test `macfuse_adapter_configures_macos_profile_and_zero_ttl_for_trash_containers`.

#### Focused verification status at pause

- **RED log captured:** `target/sfs1054-posix-trash-red-linux.log` (initial WSL2 RED compilation/test run before fixes).
- **Pre-final-fix VFS run (`task-922`, Windows):** `cargo test -p studiofs-vfs --lib -- retained_views:: ignored_entries:: trash` ran 67 tests: **66 passed, 1 failed** (`cross_os_recycle_on_windows_is_listed_restored_and_cleared_on_linux_and_macos` panicked because the second `ro_cache` read-only `StudioVfs` attempted HTTP S3 fetch to unreachable `127.0.0.1:9000` instead of validating read-only metadata/trashinfo without MinIO and payload reads on the shared VFS cache).
- **Post-fix status (at pause):** Fixed `cross_os_recycle_on_windows_is_listed_restored_and_cleared_on_linux_and_macos` in [native_tests.rs](file:///E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs/retained_views/native_tests.rs), fixed `compare_and_set_trash_days(1, 0)` in `low_level.rs` `mounted_linux_gio_trash_retains_lists_restores_and_clears`, and completed all `adapter.rs` / `fskit.rs` edits. `cargo check -p studiofs-vfs -p studiofs-winfsp --tests` exited `0` (21.50s); `git -c core.safecrlf=false diff --check` exited `0`; `mount | grep studiofs` in WSL2 Ubuntu confirmed zero leftover mounts; background task list is empty. Final GREEN test suites, Clippy, `cargo fmt --all -- --check`, WSL2 FUSE compilation/tests/mounted acceptance, and `docs/platform-bridges.md` updates remain open on resume.

#### Remaining blockers / gates and precise next steps on resume

1. **Windows focused verification & logs:** Run `cargo test -p studiofs-vfs --lib -- retained_views:: ignored_entries:: trash` (`target/sfs1054-posix-trash-win-vfs.log`), `cargo test -p studiofs-winfsp --lib` (`target/sfs1054-posix-trash-winfsp.log`), and `cargo clippy -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings` (`target/sfs1054-posix-trash-win-clippy.log`).
2. **WSL2 Linux unit, clippy, and mounted `/usr/bin/gio trash` acceptance (without touching `/home/valen/dev/studiofs` or host `~/.local/share/Trash`):**
   - `cargo test -p studiofs-vfs --lib -- retained_views:: ignored_entries:: trash` -> `target/sfs1054-posix-trash-linux-vfs.log`
   - `cargo test -p studiofs-linux-fuse --lib` -> `target/sfs1054-posix-trash-linux-fuse-unit.log`
   - `cargo test -p studiofs-linux-fuse --lib mounted_linux_gio_trash_retains_lists_restores_and_clears -- --ignored --exact --nocapture` -> `target/sfs1054-posix-trash-linux-fuse-mounted.log`
   - `cargo clippy -p studiofs-vfs -p studiofs-linux-fuse --all-targets -- -D warnings` -> `target/sfs1054-posix-trash-linux-clippy.log`
3. **Formatting & docs:** Run `cargo fmt --all -- --check` and `git -c core.safecrlf=false diff --check`; update `docs/platform-bridges.md` and append final GREEN evidence to this ExecPlan.
4. **External platform gate (remains unverified here):** `studiofs-macfuse` (`#![cfg(target_os = "macos")]`) cannot compile or run tests on Windows or WSL2 Linux; macOS compilation, unit tests, and native Finder/FSKit Trash acceptance remain gated until tested on a macOS host.


### Linux native trash completed in WSL (2026-11-16; independent review pending)

This checkpoint supersedes the paused Linux/macOS verification checkpoint.
Repository root, branch `feat/sfs-1054-recoverable-trash`, HEAD
`0358242f3c1e90959faf66e0be0cff95f424b47c`; all pre-existing dirty changes were
preserved. The user authorized finishing Linux in WSL. Existing macOS edits
were preserved and formatted, but macOS development/native acceptance is deferred.

#### Completed behavior and decisions

GIO's actual sequence is temporary sidecar create/write/fsync/close, atomic
sidecar rename, then payload rename. Linux `info` drafts now remain client-local
through last close and can rename within that directory; they cannot replace an
authoritative generated record. Receipt creation removes the matching draft.
The extended regression checks close/rename isolation, metadata-version stability,
and refusal to overwrite a generated record.

A native Linux rename leaves the moved source inode in the kernel dentry. The
first retained payload read originally reached `getattr(5)` and received
`Meta(InodeNotFound(5))`, because the VFS correctly hid its private-store inode.
`crates/studiofs-linux-fuse/src/invalidation.rs` now keys its existing bounded
queue by inode or `(parent, name)`. The rename callback queues target-entry
notification after replying; the sole existing worker performs the notification
under its existing session-shutdown guard. No detached worker or private-inode
access bypass was added. The C shim in `csrc/low_level_ops.c` calls libfuse's
entry invalidation API. Its Rust caller uses a `CString`, because libfuse reads
`namelen + 1` bytes including the final NUL. A focused queue regression checks
that two names under one parent and that parent's inode notification remain
separate, duplicate names coalesce, and shutdown drains all three notifications.

Linux retained directory views now use mount-uid ownership and `0700`, allowing
kernel `default_permissions` to admit recursive Clear. VFS still rejects new
children/payload writes and checks the original parent's current ACL before
purging a child. Windows projection modes and ownership are unchanged.

POSIX names are shortened at UTF-8 boundaries when necessary, reserving ten
bytes for Linux `.trashinfo` and space for collision suffixes. This keeps
255-byte names retained on another platform accessible on Linux. Original
paths remain in generated metadata. `.trashinfo` deletion dates use client
local time, as specified by Freedesktop, instead of unlabelled UTC. Both fixes
have focused regressions. The Linux toolchain's `drop_non_drop` failure was
resolved by removing only the redundant `drop(restore_inode)` closure call in
`crates/studiofs-meta/src/redb_engine/retained.rs`; all table drops and settled
restore logic remain intact.

`docs/platform-bridges.md` and both maintenance guides now describe positive
retention, Linux layout, collision/clear behavior, shared ACL/replication
boundaries, and the separate macOS/desktop UI gates.

#### RED evidence

The retained logs under `target/` record the narrow regressions before each
correction: `sfs1054-posix-trash-gio-draft-red.log` failed creating GIO's temporary
name with `OperationNotSupported`; `sfs1054-posix-trash-gio-close-red.log` failed
`closed GIO draft must survive until rename`; `sfs1054-posix-trash-dir-mode-red.log`
failed Linux owner UID `0 != 1000`; `sfs1054-posix-trash-long-name-red.log` failed
the 245-byte payload-view limit; `sfs1054-posix-trash-local-time-red.log` failed
the local deletion-date assertion on Windows. Final focused VFS runs below
include all of these regressions. The intermediate mounted ENOENT and EACCES
outputs were observed during development; their working log was overwritten by
the final GREEN native run, so no separate persisted native RED log is claimed.

#### Final focused evidence

From PowerShell in the repository root:

    cargo test -p studiofs-vfs --lib -- retained_views:: ignored_entries:: trash
    cargo test -p studiofs-winfsp --lib -- retained_views hidden_platform_root hidden_non_macos_root
    cargo check -p studiofs-vfs -p studiofs-winfsp --tests
    cargo clippy -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings

VFS: **70 passed**, 0 failed, 1.20s (`sfs1054-posix-trash-win-vfs.log`).
WinFSP focused controls: **4 passed**, 0 failed, 0.10s
(`sfs1054-posix-trash-winfsp.log`); this was not the full WinFSP library suite.
Check and Clippy exited 0 (`sfs1054-posix-trash-win-{check,clippy}.log`).

From WSL Ubuntu in `/mnt/e/dev/studiofs-cli`, using only current-tree sources
and separate `CARGO_TARGET_DIR=/mnt/e/dev/studiofs-cli/target/posix-linux`:

    cargo test --offline -p studiofs-vfs --lib -- retained_views:: ignored_entries:: trash
    cargo test --offline -p studiofs-linux-fuse --lib -- linux_fuse_session_configures_linux_profile_zero_ttl_direct_io_and_trash_containers invalidation::tests::
    cargo clippy --offline -p studiofs-vfs -p studiofs-linux-fuse --all-targets -- -D warnings

Linux VFS: **70 passed**, 0 failed, 2.87s (`sfs1054-posix-trash-linux-vfs.log`).
Linux FUSE focused bridge + invalidation tests: **39 passed**, 0 failed, 0.33s
(`sfs1054-posix-trash-linux-fuse-unit.log`). Linux Clippy exited 0 in 23.20s
(`sfs1054-posix-trash-linux-clippy.log`).

`fusermount3` rejects mount points on this checkout's DrvFS filesystem. The
repository test helper honors `CARGO_TARGET_DIR` at runtime, so the final freshly
rebuilt test executable was run with its owned temporary mount/cache/XDG paths
on WSL ext4, without copying sources or using another checkout's executable:

    CARGO_TARGET_DIR=/tmp/studiofs-sfs1054-posix-trash timeout 120s target/posix-linux/debug/deps/studiofs_linux_fuse-896cd85ad7297144 low_level::tests::mounted_linux_gio_trash_retains_lists_restores_and_clears --ignored --exact --nocapture

Native result: **1 passed**, 0 failed, 0.72s
(`sfs1054-posix-trash-linux-fuse-mounted-ext4.log`). The test proves two GIO
same-name moves; listing and exact payload/sidecar reads; direct unlink bypass;
restore collision refusal and successful restore; directory subtree read and
recursive Clear; `.trashinfo` Clear; and TTL-zero GIO deletion without retention.
It uses isolated HOME/XDG paths and never empties the host trash. The final
binary SHA-256 is `3c692d6bb1a1c6b62a37016660813189cf3327d868f8baf3e19e5db5f1f127a4`
(`sfs1054-posix-trash-linux-binary.sha256`). Mount checks after execution found
no StudioFS mounts, and all owned command/worker sessions completed.

Formatting and whitespace checks are logged in
`sfs1054-posix-trash-{fmt,diff-check}.log`; both exit 0.

#### Remaining boundaries and handoff

Fresh independent review is next. No full workspace, full library, or full
ignored suite was run. Native WinFSP Shell probes were not repeated in this
Linux pass; earlier Windows native evidence remains in the preceding approved
checkpoints, while current shared-code/WinFSP focused controls are GREEN.
Linux acceptance uses a real WSL2 FUSE mount and local `RedbMetaEngine`, not a
mounted PostgreSQL/S3 multi-client fixture. Shared receipt/remote recovery tests
in the earlier checkpoints remain separate evidence. Desktop `trash:///` UI
listing/restore/Clear was not exercised; native GIO delete plus filesystem
listing/restore/Clear is the exact current Linux acceptance surface.

macOS compilation, Finder behavior, and legacy macFUSE/FSKit acceptance remain
unverified on this Windows/WSL host and deferred by the user. Neither Linux nor
shared-profile unit tests prove those macOS gates. No commit, push, PR, or
Linear write was performed. The separate `/home/valen/dev/studiofs` and
`/home/valen/dev/studiofs-cli` checkouts were not modified.

Plan update reason: replace the paused verification state with actual Linux
implementation fixes and final mounted/focused evidence, while keeping platform,
service-backed, and independent-review boundaries explicit.

### Independent Linux/Windows review follow-up (2026-11-16)

Opus 5.5 APPROVED the Linux/WSL and Windows slice, with macOS deferred.
The reviewer independently re-ran 70 focused VFS tests successfully and checked
the mounted binary hash and source/build timestamps against retained artifacts.
The GIO strace artifact is a FAILED DrvFS mount attempt, not a trace of the
successful GIO operation sequence. The successful ext4 mounted test is the
acceptance evidence. Desktop trash:/// and mounted PG/S3 multi-client remain
unproven. Queued rename invalidation has a short immediate-lookup ENOENT window;
synchronous kernel invalidation inside rename would deadlock.

Small review follow-up: root-attribute normalization now filters regular files
and non-root directories before trash-root lookup. Container classification
skips known ordinary inodes once the active trash root is discovered; an
unknown root still uses authoritative lookup. Full receipt-list scans inside
the trash containers remain documented as a scaling limit, without a new cache.

#### Final verification after review follow-up

The current tree was rebuilt after the root-attribute/container fast-path
changes. From WSL Ubuntu in `/mnt/e/dev/studiofs-cli`, with build target
`CARGO_TARGET_DIR=/mnt/e/dev/studiofs-cli/target/posix-linux`:

    cargo test --offline -p studiofs-vfs --lib -- retained_views:: ignored_entries:: trash
    cargo test --offline -p studiofs-linux-fuse --lib -- linux_fuse_session_configures_linux_profile_zero_ttl_direct_io_and_trash_containers invalidation::tests::
    cargo clippy --offline -p studiofs-vfs -p studiofs-linux-fuse --all-targets -- -D warnings

Results: VFS **70 passed**, 0 failed, 2.47s; Linux FUSE controls **39 passed**,
0 failed, 0.36s; Clippy exited 0 in 20.86s. Logs are
`target/sfs1054-posix-trash-review-linux-vfs.log`,
`target/sfs1054-posix-trash-review-linux-fuse-unit.log`, and
`target/sfs1054-posix-trash-review-linux-clippy.log`.

The freshly rebuilt executable then passed the same exact native acceptance
with its runtime mount/cache paths on owned WSL ext4:

    CARGO_TARGET_DIR=/tmp/studiofs-sfs1054-posix-trash timeout 120s target/posix-linux/debug/deps/studiofs_linux_fuse-896cd85ad7297144 low_level::tests::mounted_linux_gio_trash_retains_lists_restores_and_clears --ignored --exact --nocapture

Result: **1 passed**, 0 failed, 0.60s, in
`target/sfs1054-posix-trash-review-linux-fuse-mounted-ext4.log`. Rebuilt binary
SHA-256: `88e5c4cf20a47283e05d5398f9115afcfe0843329ff5243206a000846e7e0219`,
recorded in `target/sfs1054-posix-trash-review-linux-binary.sha256`.
Afterward, mount inventory contained no owned test mount, no WSL `cargo` or
`rustc` process remained, and the verified exact
`/tmp/studiofs-sfs1054-posix-trash` temporary root was removed.

The root agent separately re-ran Windows
`cargo test -p studiofs-vfs --lib -- retained_views:: ignored_entries:: trash`:
**70 passed**, 0 failed, 1.57s (build 40.20s), and
`cargo clippy -p studiofs-vfs --all-targets -- -D warnings`: exit 0 in 25.87s.
`cargo fmt --all -- --check` and `git diff --check` also exited 0. Those Windows
results are retained in root tool output, without separate log files. An initial
compile attempt lacked the `ROOT_INO` import; the final successful run includes
the qualified `studiofs_meta::ROOT_INO` correction. All previously stated
platform/service/UI boundaries remain unchanged.

### User-requested mounted PostgreSQL/S3 two-client acceptance (2026-11-16)

The user explicitly requested the remaining mounted PG/S3 multi-client gate.
This checkpoint supersedes that specific unproven boundary above; macOS and
desktop `trash:///` remain separate gates. No product implementation was changed
in this pass. Codebase Memory MCP returned `Transport closed`; the installed
CLI fallback also could not create its secure coordination endpoint. Discovery
therefore used the scoped E2E contract and direct source inspection.

The registered runner had no trash scenario. Added
`mounted-recoverable-trash-two-client` using its existing
`MultiClientController`, bootstrap/discover, mount/restart/unmount, bounded
mounted IO and payload readback helpers. It is one narrow registered acceptance,
not a new runner or full suite. The scenario validates returned bytes explicitly
because the existing readback helper may return its last mismatching sample at
the deadline. It writes `clients.json` and `trash-summary.json` with hashes and
accepted phases.

Build and discovery in WSL Ubuntu, from `/mnt/e/dev/studiofs-cli`:

    export CARGO_TARGET_DIR=/mnt/e/dev/studiofs-cli/target/posix-linux
    cargo run --offline -p studiofs-e2e -- list
    cargo build --offline -p studiofs
    cargo build --offline -p studiofs-e2e

Current-tree product build passed in 2m 27s. The final harness rebuild passed
in 32.39s after adding an explicit peer receipt-publication barrier. Logs:
`target/sfs1054-mounted-multiclient-{list,build,harness-build}.log`.

First live attempt `sfs1054-pgs3-20261116-1` reached two real mounts and GIO
retention, but stopped the writer before both retained receipts had converged.
The observer saw the file receipt while the directory remained in its original
namespace; the later directory payload read failed with ENOENT. This attempt
did not establish the requested gate. Its report and cleanup evidence are
preserved in `target/sfs1054-mounted-multiclient-run-1/`, with console log
`target/sfs1054-mounted-multiclient-run-1.log`. The harness now waits for both
retained entries and generated sidecars on the peer, without reading payloads,
before writer shutdown. GIO's foreground completion is a local commit, not a
remote publication barrier; no product correction is claimed from that attempt.

Final exact acceptance command (process-local loopback proxy bypass only):

    NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost target/posix-linux/debug/studiofs-e2e run mounted-recoverable-trash-two-client --studiofs-bin /mnt/e/dev/studiofs-cli/target/posix-linux/debug/studiofs --root /tmp/studiofs-sfs1054-multiclient --run-id sfs1054-pgs3-20261116-2 --seed 1054 --platform linux-fuse --retain always --infra compose --port-range 30400-30500

Result: **1/1 scenario passed**, exit 0. `run`, `validate`, scenario cleanup and
runner cleanup all passed; report duration 17,575ms. Evidence:
`target/sfs1054-mounted-multiclient-run-2.log` and complete archived sandbox
`target/sfs1054-mounted-multiclient-run-2/`, including `report.json`,
`artifacts/scenarios/mounted-recoverable-trash-two-client/trash-summary.json`,
client lifecycle logs, PostgreSQL diagnostics and RustFS object listing.

The run used real PostgreSQL 18 and RustFS S3 in the unique disposable Compose
project `studiofs-e2e-sfs1054-pgs3-20261116-2`, one unique shared StudioFS volume,
and two independent client metadata/cache roots on WSL ext4. It proved:

- GIO retained a file and directory through client-a's native FUSE mount;
- client-b observed both shared receipts and generated sidecars before reading
  either payload;
- client-a unmounted and client-b restarted before the peer's first payload
  read; exact file and nested directory-child bytes were then read through the
  independent mounted peer, backed by the real S3 service;
- peer listing and original-path `.trashinfo` contents matched;
- client-b restored the file and recursively cleared the retained directory;
- remounted client-a read exact restored bytes and both mounts converged on
  removal of both retained entries and both generated sidecars.

Payload SHA-256 values: file
`d1a7baa3152e9735952e5b1b6d8ed2125f11996b121648c30b0386cd528103cf`;
directory child
`6f1c76af03f094bfbcbcecab08eb282aa58e43fb1835df60c150a8ccd63f0c03`.
Current-checkout executable hashes in
`target/sfs1054-mounted-multiclient-binaries.sha256`: product
`663eb496e0b09130034e090f34f189b6c55b0c5c392c0cbd4bce023899f4f300`;
runner `ed8be71a9a2a7d06294aef1a211c2fd355df68d2a45e905f82b12d8443e1a3d4`.

Focused harness check:
`cargo clippy --offline -p studiofs-e2e --all-targets -- -D warnings` exited 0
in 49.17s (`target/sfs1054-mounted-multiclient-clippy.log`). Formatting and
whitespace checks are recorded in the matching `-fmt.log` and `-diff-check.log`.
Only this mounted scenario was run; no full test/ignored suite was executed.

Both attempts' runner cleanup ran project-scoped `docker compose down --volumes
--remove-orphans`. Post-run checks found no containers or volumes bearing
either exact project label, no owned mounts, and no remaining StudioFS, E2E,
Cargo or rustc processes. After copying both complete sandboxes into `target/`,
the verified exact `/tmp/studiofs-sfs1054-multiclient` runtime root was removed.
Settlement is recorded in `target/sfs1054-mounted-multiclient-settlement.log`.
No host trash, other checkout, global Docker resource, commit, PR or Linear
state was changed.

Limits: this is native mounted two-client PostgreSQL/S3 evidence, distinct from
the earlier Redb-only GIO test and metadata-only service tests. The restart
retains the observer's metadata/cache directories; it is not a cold bootstrap
with deleted local state, an adversarial crash, remote-only restore retry UX,
actual janitor GC, desktop GVFS UI, or macOS proof. The independent Linux/Windows
product approval predates this newly added acceptance harness; its diff remains
available for root review.

### Independent mounted PG/S3 multi-client acceptance review (2026-11-16)

Opus 5.5 APPROVED the new registered two-client Linux FUSE scenario and its
retained run-2 artifacts: separate local metadata/cache, one shared PostgreSQL
volume and real S3, exact remote bytes after writer unmount and peer restart,
peer restore/recursive Clear, convergence after writer remount, and owned cleanup.
The reviewer independently checked binary hashes and artifact/source timestamps;
services and mounted tests were not re-run during review.

One non-blocking oracle gap remains explicit: this mounted scenario checks bytes
and namespace convergence, not original payload inode identity across restore.
A future assertion must capture the original real inode before recycling; the
synthetic trash-view inode is a distinct projection and is not an identity oracle.
Earlier identity tests remain separate evidence, not coverage supplied by this run.

This run does not establish offline unpublished receipt replay, cold bootstrap,
crash replay, remote-only restore retry, janitor GC, desktop GVFS, or macOS.
Those boundaries do not invalidate the focused mounted PG/S3 multi-client result.

### Pre-macOS item 1: mounted original inode and outside hardlink (2026-11-16)

The user authorized closing the remaining pre-macOS items sequentially. This
pass addresses only item 1, the mounted identity-oracle gap identified above.
No product source changed. The existing registered
`mounted-recoverable-trash-two-client` scenario now creates
`outside-hardlink.bin` outside the retained tree and captures
`fs::symlink_metadata(note.bin).ino()` and `nlink()` on the real original file
before GIO. The initial file and alias must share that inode with `nlink=2`.
The synthetic trash-view inode is never used as the identity oracle.

Before GIO, the peer observes only namespace/alias metadata, without reading
either file body. The existing writer-unmount/peer-restart-before-first-payload-
read sequence remains intact. After peer restore, bounded metadata checks
require both `restored.bin` and the outside alias to retain the captured
original inode and `nlink=2`; both paths must return exact expected bytes.
The same checks run after writer remount. Existing shared listing, sidecar,
retained-directory read and recursive Clear/convergence assertions remain.
Summary records now include the real original inode/link count, both clients'
identity pairs, alias name and exact-byte result.

Codebase Memory symbol/coverage calls again returned `Transport closed`;
direct inspection of the scoped scenario was used, without claiming graph
coverage. This change is confined to that scenario, its documentation and this
living checkpoint.

Exact WSL commands from `/mnt/e/dev/studiofs-cli`:

    CARGO_TARGET_DIR=/mnt/e/dev/studiofs-cli/target/posix-linux cargo build --offline -p studiofs -p studiofs-e2e
    NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost target/posix-linux/debug/studiofs-e2e run mounted-recoverable-trash-two-client --studiofs-bin /mnt/e/dev/studiofs-cli/target/posix-linux/debug/studiofs --root /tmp/studiofs-sfs1054-identity --run-id sfs1054-identity-20261116-1 --seed 1054 --platform linux-fuse --retain always --infra compose --port-range 30400-30500
    CARGO_TARGET_DIR=/mnt/e/dev/studiofs-cli/target/posix-linux cargo clippy --offline -p studiofs-e2e --all-targets -- -D warnings

Build passed in 1m 58s. The first mounted attempt was GREEN: **1/1 scenario**,
exit 0, report duration 15,793ms. Both native mounts used independent local
metadata/cache directories on owned WSL ext4, one unique shared StudioFS volume,
real PostgreSQL 18 and RustFS S3. All run/validate/scenario-cleanup/runner-cleanup
phases passed. Captured real original inode was **549755813889**, `nlink=2`;
both restored/alias pairs on peer and remounted writer matched
`(549755813889, 2)`, with exact expected bytes through all four names.
The directory Clear and receipt-removal checks also passed. No product defect
or failing native attempt was observed in this item.

Artifacts: `target/sfs1054-mounted-identity-{build,run-1,clippy}.log`, complete
archived sandbox `target/sfs1054-mounted-identity-run-1/`, including `report.json`
and `artifacts/scenarios/mounted-recoverable-trash-two-client/trash-summary.json`.
Binary SHA-256 values in `target/sfs1054-mounted-identity-binaries.sha256`:
product `663eb496e0b09130034e090f34f189b6c55b0c5c392c0cbd4bce023899f4f300`;
runner `8af8cd679d89de1ad511bdfaaf14c6b8983561fbc7460970fda699bb476932cc`.
The product hash is unchanged from the approved preceding acceptance; the
current-checkout build was still rechecked before this mounted run.

Focused E2E Clippy exited 0; fmt and diff-check results are recorded in
`target/sfs1054-mounted-identity-{fmt,diff-check}.log`. No full suites ran.
Project-scoped Compose teardown removed its containers/volumes, and exact-label
post-checks found none remaining. No owned mounts or StudioFS/E2E/Cargo/rustc
processes remained. After preserving the complete sandbox in `target/`, the
verified exact `/tmp/studiofs-sfs1054-identity` runtime root was removed;
`target/sfs1054-mounted-identity-settlement.log` records settlement. No host trash,
other checkout, global resource, commit, PR or Linear state changed.

Item 1 is now mounted PG/S3 evidence for original real inode plus outside
hardlink preservation, rather than a trash-projection comparison or metadata-
only proof. Next authorized piece is item 2: crash/offline unpublished receipt
replay and cold bootstrap, with its own focused mounted acceptance. It was not
started in this pass. GC, desktop GVFS and final documentation review follow
separately; none is implied by the present GREEN identity result.

### 2026-11-16 item 2: mounted crash replay and fresh cold bootstrap

Scope: the existing E2E controller/runner now registers three focused Linux
probes in `recoverable_trash_durability.rs`, sharing bounded mounted I/O helpers
with the approved two-client scenario. Item 1's real original-inode/outside-
hardlink assertions and its cold peer-read ordering are preserved. No lasting
product code change was made in this pass. Codebase Memory coverage/search
attempts returned `Transport closed`; direct scoped source inspection was used.

Every mounted run used this current checkout's binaries, independent client
metadata/cache roots on owned WSL ext4, a unique StudioFS volume and project-
scoped Compose PostgreSQL 18 plus real RustFS S3. Services were stopped only
through the existing Compose controller for that run. No host networking,
host trash, other checkout or global Docker resource was modified.

Exact runner shape, from `/mnt/e/dev/studiofs-cli`:

```bash
CARGO_TARGET_DIR=target/posix-linux cargo build -p studiofs -p studiofs-e2e
NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
  target/posix-linux/debug/studiofs-e2e run SCENARIO \
  --studiofs-bin /mnt/e/dev/studiofs-cli/target/posix-linux/debug/studiofs \
  --root /tmp/studiofs-sfs1054-durability --run-id RUN_ID --seed 1054 \
  --platform linux-fuse --retain always --infra compose --port-range 30400-30500
```

**Path B GREEN:** `SCENARIO=mounted-recoverable-trash-cold-bootstrap`,
`RUN_ID=sfs1054-cold-20261116-1`: 1/1 passed, exit 0, report duration 15,987ms.
File and directory restore plus one retained file were published; both warm
clients were unmounted before a third client was created under
`fresh-client-runtime`. Before discover its metadata DB did not exist and its
cache was empty. The third client fetched exact three payloads, matched all
five expected attribute sets (size/mode/uid/gid/nlink/type), retained receipt
original path and absent pre-restore names. Its real S3 GET counter rose 0→3;
shared authority head rose 1→6. This is actual remote bootstrap without cache
copy/injection, not preserved-metadata restart, and it makes no crash claim.
All run/validate/scenario-cleanup/runner-cleanup phases passed.
Artifacts: `target/sfs1054-mounted-durability-cold-run-1.log` and complete
`target/sfs1054-mounted-durability-cold-run-1/`, including
`durability-summary.json` and before/after runtime snapshots. Build 35.24s;
`target/sfs1054-mounted-durability-cold-binaries.sha256` records product
`663eb496e0b09130034e090f34f189b6c55b0c5c392c0cbd4bce023899f4f300`
and runner `a565fc4a8c10264a1db72445d6711425046043ee65af79009af0c431c2544165`.

**Path A RED, full PG/S3 outage:**
`SCENARIO=mounted-recoverable-trash-durability`,
`RUN_ID=sfs1054-durability-20261116-1`: 1/1 failed, exit 1. Both owned services
were stopped before payload creation. Three file payloads were locally fsynced,
three GIO receipts committed, then an auxiliary file's strict fsync drained
namespace work and synchronized redb. Pending journal generation was 52;
peer originals/receipts were absent and shared authority was unavailable from
before the creates (last observed head 1). The writer was actually hard killed.
Same-metadata/cache reopen failed before FUSE readiness with
`remote unavailable: postgres connect failed: error connecting to server`.
Artifacts: `target/sfs1054-mounted-durability-run-1.log` and complete
`target/sfs1054-mounted-durability-run-1/`; original build 36.47s.

This is an existing graph-repair safety decision: `studiofs-meta/src/lib.rs`
marks repaired redb state for PG graph recovery, and `PgMetaEngine::open_with_options`
requires a shared snapshot/head and catch-up before local serving. CLI already
requests deferred remote startup, but repair intentionally overrides it.
`repair_startup_keeps_recovery_marker_when_remote_sync_fails` explicitly expects
refusal and a retained repair marker rather than cleanup from stale local
state. This pass does not weaken that guarantee. Fully offline crash reopen
therefore remains unaccepted and needs a deliberate repair/write-authority
solution, not a fixture retry or removal of the existing safety gate.

**Additional Path A RED, S3 outage with PG available for repair:**
`SCENARIO=mounted-recoverable-trash-s3-crash-replay`, run IDs
`sfs1054-s3-crash-20261116-{1,2,3}`: each 1/1 failed, exit 1, all cleanup phases
passed. S3 was stopped before payload creation. A read-only scoped SQL count
proved zero shared `set_xattr` events before the kill; in this fresh fixture
there are no other application xattrs, so no receipt reached shared authority.
Peer originals/receipts were absent, local fsync barrier completed, pending
journal generation was 52. Hard kill and same-cache remount succeeded with PG
available while S3 stayed stopped. All three exact retained payloads and all
three generated receipt sidecars were recovered. The very first mounted
restore `offline.bin`→`restored.bin` then returned ENOENT. Run 1 preserved the
original generic error; run 2 added operation context and reproduced it (runner
build 36.65s). Run 3 temporarily logged the typed adapter error:
`Meta(EntryNotFound { parent: 549755817984, name: "data" })`.
Thus the receipt/source payload survives but its private `receipt/data` binding
is missing after repair/replay. View reads use the receipt's inode identity and
alone do not prove a complete retained namespace or a usable restore. Peer
publication and subsequent cold bootstrap in this crash mode were not reached.
This is a real product regression, not an auxiliary barrier-file fixture error.

All failing sandboxes are preserved in
`target/sfs1054-mounted-durability-s3-run-{1,2,3}/` with console `.log` siblings,
`unpublished-proof.json`, SQL absence evidence, and pre-kill/pre-restore runtime
snapshots. Run 2 used unchanged product hash `663eb496...` and runner
`a2d5f165113d88950f2a00488e22dbb95b26abf7e138a17235bcdbed460537b0`.
Run 3 is diagnostic evidence only: its product contained a temporary typed-error
print; that print was removed immediately afterward. It is not counted as an
uninstrumented acceptance. The final ordinary binaries are rebuilt below.
An initial S3-variant runner build failed E0603 by referring to a private event
decoder; the fixture was corrected to scoped read-only SQL counting without
expanding any product API. No failed artifact was discarded.

Current boundary: item 2 is incomplete. Fresh remote cold bootstrap is GREEN;
fully offline post-kill mount and S3-offline post-repair restore remain RED.
The registered failing probes preserve those obligations. Do not infer Windows,
macOS, power-loss, janitor GC or desktop GVFS acceptance from this result. Items
3–5 were not started. Root was informed of both concrete repair defects before
any wider product change.

Final item-2 checks and settlement: temporary adapter instrumentation was
removed and the current uninstrumented product/runner rebuilt successfully in
57.27s. `CARGO_TARGET_DIR=target/posix-linux cargo clippy -p studiofs-e2e
--all-targets -- -D warnings` exited 0 in 9.81s; see
`target/sfs1054-mounted-durability-final-build-clippy.log`. `cargo fmt --all --
--check` and `git diff --check` exited 0 (logs
`target/sfs1054-mounted-durability-{fmt,diff-check}.log`; Git line-ending notices
are warnings, not validation failures). No full suites ran. Final hashes in
`target/sfs1054-mounted-durability-settlement.log`: product
`663eb496e0b09130034e090f34f189b6c55b0c5c392c0cbd4bce023899f4f300`;
runner `fee54352c7dd9c30672183f4e687c7bd37b507a2fd8dad4c51abd5b37cd065f7`.
The cold GREEN predates only diagnostic/error-context and mode factoring; no
claim is made that this final build passed either retained RED crash path.

All five complete run sandboxes were verified archived before cleanup. Exact
Compose project-label checks found no containers or volumes for these runs;
no owned mount, StudioFS/E2E/Cargo/rustc process remained. The resolved exact
`/tmp/studiofs-sfs1054-durability` root was then removed. The settlement log
records every archive check and `OWNED_EXECUTION_SETTLED`. An initial cleanup
precheck stopped before any removal because inline shell variable quoting lost
the archive-loop values; a retained literal script under `target/` corrected
that command, and the full verified settlement then exited 0. No requested
acceptance was hidden by cleanup or a fixture exception.

### 2026-11-16 item 2 binding investigation — explicit pause checkpoint

The user requested a pause; no fix or further acceptance was started after that
request. Item 2 remains incomplete. The last accepted uninstrumented product
is still hash `663eb496...` from the preceding item-2 checkpoint; cold bootstrap
1/1 GREEN remains the only accepted new durability path. Existing PG-offline
repair refusal is a separate intentional safety boundary, not proved a bug by
the failed offline mount.

Correction/refinement of the earlier ENOENT diagnosis: the raw `receipt/data`
dentry **exists in base** at the failing instant. The namespace visibility
filter hides it. Diagnostic native run 2 recorded exactly:

```text
SFS1054 raw data parent=549755817984 entry=Some(DirEntryValue { ino: 549755813889, kind: RegularFile }) overlay=false
SFS1054 lookup data parent=549755817984 overlay=false
SFS1054 initial receipt lookup EntryNotFound { parent: 549755817984, name: "data" }
```

The failure is the initial `sync_meta().lookup(receipt, b"data")` in restore,
before source flush, stage creation or metadata adoption. All three exact
retained payloads and all three receipt sidecars still passed before it. It is
therefore not proved to be permanent physical binding loss. Source views can
resolve retained bytes by inode while ordinary metadata name lookup is hidden.
The reason visibility rejects the base row (missing current/base attr versus
missing local lifecycle authority) is not yet observed in process and must not
be asserted as established root cause.

Three additional focused diagnostic runs of
`mounted-recoverable-trash-s3-crash-replay` used run IDs
`sfs1054-binding-diag-20261116-{1,2,3}` with the same exact runner command shape
above, except `--root /tmp/studiofs-sfs1054-binding`. All ran one actual WSL
FUSE/PG/S3 scenario and failed at the same first restore. Their builds contained
temporary typed lookup prints; these are diagnostic RED evidence, not counted
acceptance. Console logs and complete sandboxes are respectively:
`target/sfs1054-mounted-durability-binding-diag-run[{-2,-3}].log` and
`target/sfs1054-mounted-durability-binding-diag-run[{-2,-3}]/`.
The first raw lookup diagnostic build failed E0382 in its temporary print
expression; after correction the builds passed (1m33s, then 1m27s); the last
runner-only diagnostic build passed in 39.13s. These build failures did not
change product behavior or erase preceding native failures.

Run 2 additionally preserves a stable post-SIGKILL/pre-reopen metadata file
`writer-killed-before-reopen.db`. Reading a copy via a temporary focused test
confirmed before repair: data dentry was local overlay, source attr was 32-byte
Uploading epoch 1/committed epoch 0, and lifecycle was LocalOnly with
PinnedUnpublished retention. The journal included all three retained rename
operations. The diagnostic test never modified the retained artifact.
Run 3 preserves both that file and
`writer-failed-before-service-resume.db`. Important limit: a writable reopen of
this latter copied file repairs/reverts not-yet-durable post-repair transactions,
so it showed the pre-kill graph again; it is **not** a faithful proof of the
failed mount's in-memory attr/lifecycle state. It remains retained evidence,
not a substitute for the required in-process diagnostic. The ordinary archived
DB after cleanup/S3 resume also exposes data normally; timing matters.

A standalone focused redb repair→full journal replay control test passed 1/1
and therefore did not reproduce the native defect. Its first draft failed an
unrelated standalone allocator assertion because checkpoint import resets
NEXT_INODE (PgMetaEngine uses a separate namespaced allocator). The corrected
control manually represented that separate allocation authority and passed.
No claim of a matching deterministic RED was made. The temporary control and
artifact diagnostic tests were removed at pause; logs remain under
`target/sfs1054-mounted-durability-binding-{red,replay-control,diagnostic,before-repair-diagnostic[-2],failed-state-diagnostic}.log`.

All named temporary product prints were removed from redb namespace lookup and
VFS restore, and both temporary tests were removed. `namespace.rs` has no net
diff from before this diagnostic work. No production fix was made. The only
permanent code addition since the preceding checkpoint is the scenario's
stable metadata copy immediately after its deliberate writer kill and before
reopen, retained as an artifact for future diagnosis. A temporary failure-path
force-kill/copy block was removed; it is not part of the scenario cleanup.
Prior feature changes, item-1 identity assertions and error contexts are intact.

The current WSL product/runner binaries are still diagnostic builds and are
**unaccepted and out of sync with the now-uninstrumented source**. No rebuild or
new acceptance was launched after the pause request. Their exact hashes are
recorded in `target/sfs1054-mounted-durability-binding-settlement.log`; rebuild
both from current source before any subsequent counted acceptance. The Windows
focused test binary likewise contains removed temporary test code; rebuild
focused tests on resume instead of relying on that artifact.

Cleanup is complete. Run 3's diagnostic extra force-kill left one owned stale
FUSE mount; it was identified by exact path, unmounted with fusermount3, and the
complete sandbox was archived again. The initial archive attempt returned
Transport endpoint is not connected on that mount, so it was not counted as
complete until the post-unmount copy succeeded. Project-label checks found no
containers or volumes for all three owned runs; no StudioFS/E2E/Cargo/rustc
process or owned mount remained. Both killed/failed metadata copies and all
three reports were verified archived before deleting only the resolved exact
`/tmp/studiofs-sfs1054-binding` root. Settlement log ends
`OWNED_EXECUTION_SETTLED`. No host trash or global resource changed.

Next on resume: add a narrow **in-process** source attr/lifecycle diagnostic at
the rejected raw data lookup, reproduce once with scoped PG available/S3 paused,
then extract a deterministic regression for the actual authority/visibility
transition and make the smallest cause-level repair. Retain remote namespace
visibility/private-store/restore fences; do not unhide arbitrary base entries,
read by inode as a restore fallback, retry the first restore or skip repair.
Rebuild uninstrumented binaries and rerun the existing mounted scenario to
GREEN, including peer exact bytes after S3 returns. Then focused meta/VFS
controls, affected-package checks/Clippy, fmt/diff and review are required.

Root's independent offline-publication design work is separate and remains
unimplemented: durable local journal is the publication intent, with an
Immediate barrier before PG push; startup and ambiguous-COMMIT recovery must
pull before push. Proposed protocol proof is limited to fresh format 6 and a
clean 5→6 migration, preserving import semantics; unknown existing format-6
state stays conservative. Existing repair safety refusal remains until a proof
is established. Startup mode, acknowledgment barriers, object-ID durability
before S3 PUT and the focused RED matrix still need verification. None of that
was added to this binding investigation or accepted by these runs. Items 3–5
remain unstarted.

### 2026-11-16 item 2 root causes resolved & mounted S3-crash-replay + cold-bootstrap GREEN

Two cooperating root causes blocked `mounted-recoverable-trash-s3-crash-replay`
and are now fixed at their causes without weakening namespace visibility filters,
private-store/restore fences, or PG repair safety refusal:

1. **Cause 1 — Pending local replay omitted `INODE_LIFECYCLE_TABLE` resync after PG repair snapshot import:**
   - **Verified mechanism (vs prior `missing dentry` hypothesis):** Before SIGKILL,
     `receipt/data` pointed to an `Uploading` (32B, `content_epoch=1`, `committed_content_epoch=0`)
     file whose `INODE_LIFECYCLE_TABLE` row was `LocalOnly`/`PinnedUnpublished`.
     PG repair imported the remote snapshot (clearing `INODE_LIFECYCLE_TABLE`) and
     `apply_replica_serving_state_for_pending_replay` replayed the pending local
     window into serving `INODES_TABLE` / `BASE_DENTRIES_TABLE` / `REGULAR_FILE_OVERLAY_TABLE`,
     leaving `INODE_LIFECYCLE_TABLE` unpopulated. When background flush later
     committed `RenameEntry` (`receipt/data`, `overlay=false`) to base while hiding
     the `Uploading` inode's `SetAttr` (`base_attr=None`), `namespace_entry_visible`
     rejected the base dentry with `EntryNotFound` despite intact payload bytes
     and sidecars.
   - **Fix:** Added [`sync_replayed_inode_lifecycles_in_txn`](file:///E:/dev/studiofs-cli/crates/studiofs-meta/src/redb_engine/inodes.rs#L1019-L1122),
     invoked inside [`apply_replica_serving_state_for_pending_replay`](file:///E:/dev/studiofs-cli/crates/studiofs-meta/src/redb_engine/replication.rs#L1776),
     recomputing `INODE_LIFECYCLE_TABLE` from current serving attrs and overlay
     chunks while preserving `DeletedBeforePublish` rows and `deferred_cleanup_object_ids`.
   - **Verification:** Prior clean diagnostic `run-5` (`target/sfs1054-mounted-durability-binding-diag-run-5/`)
     proved `client-a` replayed cleanly after SIGKILL, read all 3 retained payloads
     + `.trashinfo` sidecars, and restored `offline.bin -> restored.bin` and
     `album -> restored-album` locally, but post-restore PG publication stalled
     on stale epoch-1 uploads.
   - **Focused meta tests (2/2 ok):**
     - `repair_snapshot_and_pending_replay_keeps_uploading_renamed_entry_lifecycle_and_visibility`
     - `pending_replay_preserves_deleted_before_publish_lifecycle_and_deferred_cleanup`
       ([`remote_apply.rs`](file:///E:/dev/studiofs-cli/crates/studiofs-meta/src/pg_engine/tests/remote_apply.rs#L5652-L5831)).

2. **Cause 2 — Retained restore left superseded epoch-1 uploads & `live_extent_records`, which `protect_stale_upload_if_covered` requeued indefinitely:**
   - **Verified mechanism (from `run-5` DB/tracing inspection):** On post-crash
     remount, `startup_replay` queued the pre-restore epoch-1 objects into
     `UploadQueue` and `live_extent_records`. `restore_retained_entry_trusted`
     replaced those inodes' overlays with fresh stage payloads at epoch `2`
     (`committed_size=0, committed_content_epoch=0`) and queued the epoch-2 uploads,
     without cancelling pre-restore uploads or replacing `live_extent_records`.
     `protect_stale_upload_if_covered` then saw `live_extent_epoch_for_object >= op.content_epoch`
     (`1 >= 1`) without checking `attr.content_epoch` (`2`), causing superseded
     epoch-1 uploads to loop in `UploadQueue` forever and blocking `CommitCoordinator`
     epoch finalization and PG publication.
   - **Fixes:**
     - [`protect_stale_upload_if_covered`](file:///E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs/upload_lifecycle/worker.rs#L1010-L1046):
       require `attr.as_ref().is_some_and(|attr| live_epoch >= attr.content_epoch)`
       before protecting a stale upload by `live_extent_epoch_for_object`.
     - [`restore_retained_entry_trusted`](file:///E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs/io_ops.rs#L709-L738):
       clear pre-restore pending uploads, `live_extent_records`, pending epoch
       finalizations, and sticky upload errors for every restored regular-file inode,
       record new `live_extent_records`, submit the new-epoch uploads, and note
       `FileSessionOperation::Upload` (`"queued"`).
   - **Focused VFS RED→GREEN tests:**
     - `process_upload_cancels_stale_epoch_when_live_extent_trails_current_epoch`
       ([`worker.rs`](file:///E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs/upload_lifecycle/worker.rs#L4875-L4896)):
       RED (`Requeued` vs `CancelledStale`) → GREEN.
     - `retained_restore_clears_stale_pending_uploads_and_live_extents`
       ([`tests.rs`](file:///E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs/tests.rs#L29485-L29543)):
       RED (`pending=[(0, 4098, 1), (0, 4101, 2)]`) → GREEN.
   - **Windows focused controls (all passed after final formatting):**
     - `cargo test -p studiofs-vfs --lib process_upload_` → 32 passed, 0 failed.
     - `cargo test -p studiofs-vfs --lib retained_` → 77 passed, 0 failed.
     - `cargo test -p studiofs-meta --lib trash` → 7 passed, 0 failed.
     - `cargo test -p studiofs-meta --lib repair_snapshot_and_pending_replay_keeps_uploading_renamed_entry_lifecycle_and_visibility` & `pending_replay_preserves_deleted_before_publish_lifecycle_and_deferred_cleanup` → 2 passed, 0 failed.

3. **Uninstrumented WSL binaries & mounted E2E acceptance (`target/posix-linux/debug/`):**
   - Exact binary SHA-256 hashes (recorded in `target/sfs1054-mounted-durability-binding-binaries.sha256`, uninstrumented build `1m 06s`):
     - `studiofs`: `934a5b56de87f277ff72b8f54adec22a13e07d9854740cd387146cc53ba07ba9`
     - `studiofs-e2e`: `bfb15ec8017c4dbd5f889850b345b6e67868bc5cbdb7c8415bf17789e3d56721`
   - **`mounted-recoverable-trash-s3-crash-replay` (`run_id=sfs1054-binding-green-20261116-2`) — 1/1 PASSED (exit 0):**
     - Report duration `21180`ms (scenario `12836`ms). S3 stopped before creates;
       PG available (`postgres_offline_during_reopen=false`); `shared_set_xattr_events=0`
       before kill; local pending journal generation `52`; writer SIGKILL (`writer-killed-before-reopen.db` archived);
       same-cache reopen retained exact bytes & restored `offline.bin -> restored.bin`
       and `album -> restored-album` while S3 stayed stopped; after S3 resume,
       shared authority head advanced `1 -> 4`, warm peer `client-b` read exact
       bytes for `restored.bin`, `restored-album/child.bin`, `.Trash-1000/files/keep.bin`,
       and fresh cold `client-c` (absent metadata DB, empty cache) fetched exact
       bytes + matching 5 attribute tuples + `.trashinfo` sidecar (`cold_s3_gets: 0 -> 3`).
     - Log: `target/sfs1054-mounted-durability-binding-green-run.log`; complete sandbox: `target/sfs1054-mounted-durability-binding-green-run/`.
   - **`mounted-recoverable-trash-cold-bootstrap` (`run_id=sfs1054-binding-cold-20261116-1`) — 1/1 PASSED (exit 0):**
     - Report duration `13923`ms (scenario `6062`ms); shared head `1 -> 7`;
       `cold_s3_gets: 0 -> 3`; matching 5 attribute tuples.
     - Log: `target/sfs1054-mounted-durability-binding-cold-run.log`; complete sandbox: `target/sfs1054-mounted-durability-binding-cold-run/`.
   - **Static & hygiene checks:** `cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs-cache -p studiofs-e2e --all-targets -- -D warnings` (Windows + WSL Linux), `cargo fmt --all -- --check`, and `git diff --check` all exited `0`.
   - **Owned cleanup & settlement:** Both GREEN archives + preserved diagnostic
     archives (`target/sfs1054-mounted-durability-binding-diag-run-{2,3,4,5}/`)
     verified intact before deleting only `/tmp/studiofs-sfs1054-binding`
     (`dir_removed=true`, `mounts=0`, `procs=0`, `containers=0`, `volumes=0`);
     `target/sfs1054-mounted-durability-binding-settlement.log` ends `OWNED_EXECUTION_SETTLED`.
     No host trash, `studiofs-e2e-20261104t*`, or global resources touched.

4. **Remaining limits / separate work (unchanged):**
   - `mounted-recoverable-trash-durability` (PG down + S3 down repair refusal)
     remains separate per Opus's unimplemented offline-publication protocol
     design and is untouched in this slice.
   - Items 3 (janitor GC), 4 (desktop GVFS), and 5 (final gates/review) wait
     on this item 2 completion; no Windows/macOS/full-workspace acceptance is claimed.

### 2026-11-16 item 2 review remediation (F1, F2, F4)

1. **F1 — Reverted `protect_stale_upload_if_covered` narrowing (`crates/studiofs-vfs/src/vfs/upload_lifecycle/worker.rs`):**
   - **Change:** Reverted `protect_stale_upload_if_covered` to the original `if let Some(attr) = ...` check (removing `attr.as_ref().is_some_and(|attr| live_epoch >= attr.content_epoch)`) so pre-commit metadata-gap and open-handle unlink windows stay protected; kept `restore_retained_entry_trusted` cleanup in `crates/studiofs-vfs/src/vfs/io_ops.rs`.
   - **Test:** Replaced `process_upload_cancels_stale_epoch_when_live_extent_trails_current_epoch` with `process_upload_keeps_live_extent_when_attr_epoch_advances`, asserting `UploadOutcome::Requeued` when `live_extent_epoch_for_object` trails a bumped `attr.content_epoch`.

2. **F2 — Preserved `DeletedBeforePublish` across repair snapshot import (`crates/studiofs-meta/src/redb_engine/snapshots.rs`):**
   - **Change:** Guarded `Self::clear_table_in_txn(&txn, INODE_LIFECYCLE_TABLE)?;` with `if !preserve_event_log` inside `import_snapshot_with_mode_and_preserved_inodes`, reusing `Self::rebuild_inode_lifecycle_table_in_txn(&txn)?` so `DeletedBeforePublish` rows and their `deferred_cleanup_object_ids` survive repair snapshot import + pending replay without duplicating lifecycle rebuild logic.
   - **RED → GREEN:** Rewrote `pending_replay_preserves_deleted_before_publish_lifecycle_and_deferred_cleanup` (`crates/studiofs-meta/src/pg_engine/tests/remote_apply.rs`) to call `import_remote_snapshot_preserving_local_event_log` + `restore_pending_state_from_local_log`. Pre-fix run failed (`0 passed, 1 failed`, missing `DeletedBeforePublish` row); post-fix run passed (`1 passed, 0 failed`).

3. **F4 — Strengthened restore cleanup & worker-race coverage (`crates/studiofs-vfs/src/vfs/tests.rs`, `worker.rs`):**
   - **Change:** Extended `retained_restore_clears_stale_pending_uploads_and_live_extents` to seed pre-restore `queue_epoch_finalization`, inject a sticky upload error via `retained_restore_hook` (`RetainedRestoreStage::CopyRead`, after `flush_retained_source_ordered`), and assert old local payloads are retired in `retired_uploads` (still present in `has_local_payload` for refcheck sweep rather than deleted directly), sticky upload errors are cleared, and pre-restore `pending_epoch_finalizations` entries are cleared.
   - **Dequeued-worker race test:** Added deterministic `process_upload_retained_restore_cancels_already_dequeued_upload` using `make_unstarted_test_vfs`, `claim_pending_uploads`, and `GatedVfsUploadLocalPayloadPort` to pause the worker mid-`read_for_upload`, run `restore_retained_entry`, and verify `UploadOutcome::Shutdown`, payload retirement without immediate unlink, sticky-error / old-epoch-finalization clearing, and intact new-epoch upload + live extent.

4. **Verification (after final formatting):**
   - **Focused Windows tests:**
     - `cargo test -p studiofs-meta --lib repair_snapshot_and_pending_replay` → `1 passed; 0 failed` (1251 filtered out)
     - `cargo test -p studiofs-meta --lib pending_replay_preserves_deleted_before_publish` → `1 passed; 0 failed` (1251 filtered out)
     - `cargo test -p studiofs-meta --lib retained` → `34 passed; 0 failed; 34 ignored` (1184 filtered out)
     - `cargo test -p studiofs-vfs --lib process_upload_` → `33 passed; 0 failed` (1399 filtered out)
     - `cargo test -p studiofs-vfs --lib retained_` → `78 passed; 0 failed` (1354 filtered out)
   - **Static & hygiene checks:** `cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs-cache --all-targets -- -D warnings` (Windows), `cargo fmt --all -- --check`, and `git -c core.safecrlf=false diff --check` all exited `0`.
   - **WSL uninstrumented binaries (`target/posix-linux/debug/`, `target/sfs1054-f1f2f4-binaries.sha256`):**
     - `studiofs`: `e4da9cb959345cf96d3fb470527ad87b8f27208f25cd5d1018f40140630f2551`
     - `studiofs-e2e`: `a71054480b2c3e59e2c188e2584e66f2b0a842833e4fc457fffd4f48dedcd1b5`
   - **Mounted Linux E2E reruns (`--platform linux-fuse --infra compose --port-range 30400-30500 --seed 1054`):**
     - `mounted-recoverable-trash-s3-crash-replay` (`run_id=sfs1054-f1f2f4-crash-20261116-1`) → **1/1 PASSED** (exit `0`, report duration `21183`ms, `shared_head: 1 -> 3`, `cold_s3_gets: 0 -> 3`; log `target/sfs1054-f1f2f4-crash-run.log`, sandbox `target/sfs1054-f1f2f4-crash-run/`).
     - `mounted-recoverable-trash-cold-bootstrap` (`run_id=sfs1054-f1f2f4-cold-20261116-1`) → **1/1 PASSED** (exit `0`, report duration `13447`ms, `shared_head: 1 -> 5`, `cold_s3_gets: 0 -> 3`; log `target/sfs1054-f1f2f4-cold-run.log`, sandbox `target/sfs1054-f1f2f4-cold-run/`).
     - Settlement recorded in `target/sfs1054-f1f2f4-settlement.log` (`OWNED_EXECUTION_SETTLED`; `/tmp/studiofs-sfs1054-f1f2f4` removed, 0 owned mounts/processes/containers/volumes).

### 2026-11-16 item 2 re-review remediation (F2-a, F2-b)

1. **F2-a — Skip preserved `DeletedBeforePublish` inodes during `rebuild_inode_lifecycle_table_in_txn` (`crates/studiofs-meta/src/redb_engine/inodes.rs`):**
   - **Change:** Collected the preserved `DeletedBeforePublish` inode numbers into a `HashSet` while re-inserting them in [`rebuild_inode_lifecycle_table_in_txn`](file:///E:/dev/studiofs-cli/crates/studiofs-meta/src/redb_engine/inodes.rs#L351-L419), and skipped those inodes in the `regular_inos` recompute loop (`if preserved_inos.contains(&ino) { continue; }`) because inode numbers are never reused so `DeletedBeforePublish` is terminal even when a remote snapshot still contains the inode.
   - **RED → GREEN:** Added `pending_replay_preserves_deleted_before_publish_for_published_file_edited_before_unlink` in [`remote_apply.rs`](file:///E:/dev/studiofs-cli/crates/studiofs-meta/src/pg_engine/tests/remote_apply.rs#L5854-L5919) (publish file → dirty locally → unlink before publish → `import_remote_snapshot_preserving_local_event_log` → `restore_pending_state_from_local_log`). Pre-fix run failed (`0 passed; 1 failed`, `left: Published, right: DeletedBeforePublish`); post-fix run passed (`1 passed; 0 failed`), keeping `has_published_base = true` and `deferred_cleanup_object_ids = [77_407]`.

2. **F2-b — `RemotePreserveLocalState` checkpoint catch-up scope (`crates/studiofs-meta/src/redb_engine/snapshots.rs`, `pg_engine/metadata_io.rs`):**
   - **Scope note:** `preserve_event_log` is also `true` for `SnapshotImportMode::RemotePreserveLocalState` (runtime checkpoint catch-up via [`import_serving_checkpoint_locked`](file:///E:/dev/studiofs-cli/crates/studiofs-meta/src/pg_engine/metadata_io.rs#L465-L509) → [`import_serving_checkpoint_preserving_local_state`](file:///E:/dev/studiofs-cli/crates/studiofs-meta/src/redb_engine/snapshots.rs#L427-L438)). Retaining `DeletedBeforePublish` rows across catch-up is safe and consistent because `INODE_TOMBSTONES_TABLE` and `DENTRY_TOMBSTONES_TABLE` are preserved in that mode.
   - **Test:** Added `catch_up_checkpoint_preserve_local_state_keeps_deleted_before_publish_and_hides_inode` in [`remote_apply.rs`](file:///E:/dev/studiofs-cli/crates/studiofs-meta/src/pg_engine/tests/remote_apply.rs#L5921-L5984), confirming `DeletedBeforePublish` (`has_published_base = true`, `deferred_cleanup_object_ids = [77_409]`) survives `import_serving_checkpoint_locked(StartupSnapshotImportMode::PreserveLocalState, ...)` while `get_attr` returns `InodeNotFound` and `lookup` returns `EntryNotFound` (`1 passed; 0 failed`).

3. **Verification:**
   - **Focused Windows tests:**
     - `pending_replay_preserves_deleted_before_publish_lifecycle_and_deferred_cleanup` (`--exact`) → `1 passed; 0 failed` (1253 filtered out)
     - `pending_replay_preserves_deleted_before_publish_for_published_file_edited_before_unlink` (`--exact`) → RED `0 passed; 1 failed`, GREEN `1 passed; 0 failed` (1253 filtered out)
     - `catch_up_checkpoint_preserve_local_state_keeps_deleted_before_publish_and_hides_inode` (`--exact`) → `1 passed; 0 failed` (1253 filtered out)
     - `cargo test -p studiofs-meta --lib import_remote_snapshot` → `5 passed; 0 failed` (1249 filtered out)
     - `cargo test -p studiofs-meta --lib lifecycle` → `142 passed; 0 failed; 71 ignored` (1041 filtered out)
     - `cargo test -p studiofs-meta --lib retained` → `34 passed; 0 failed; 34 ignored` (1186 filtered out)
     - `cargo test -p studiofs-vfs --lib retained_` → `78 passed; 0 failed` (1354 filtered out)
   - **Static & hygiene checks:** `cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git -c core.safecrlf=false diff --check` all exited `0`.
   - **WSL uninstrumented binaries (`target/posix-linux/debug/`, `target/sfs1054-f2ab-binaries.sha256`):**
     - `studiofs`: `31854631c1d126d1d338106fb8feabe1b24addc878b1a358dee594cf2ba444c5`
     - `studiofs-e2e`: `4a74dae63b05677551aee9728daa7697775a01c2ca388de722a2032fe4cb22dd`
   - **Mounted Linux E2E reruns (`--platform linux-fuse --infra compose --port-range 30400-30500 --seed 1054`):**
     - `mounted-recoverable-trash-s3-crash-replay` (`run_id=sfs1054-f2ab-crash-20261116-1`) → **1/1 PASSED** (exit `0`, report duration `21497`ms, `shared_head: 1 -> 4`, `cold_s3_gets: 0 -> 3`; log `target/sfs1054-f2ab-crash-run.log`, sandbox `target/sfs1054-f2ab-crash-run/`).
     - `mounted-recoverable-trash-cold-bootstrap` (`run_id=sfs1054-f2ab-cold-20261116-1`) → **1/1 PASSED** (exit `0`, report duration `13412`ms, `shared_head: 1 -> 6`, `cold_s3_gets: 0 -> 3`; log `target/sfs1054-f2ab-cold-run.log`, sandbox `target/sfs1054-f2ab-cold-run/`).
     - Settlement recorded in `target/sfs1054-f2ab-settlement.log` (`OWNED_EXECUTION_SETTLED`; `/tmp/studiofs-sfs1054-f2ab` removed, 0 owned mounts/processes/containers/volumes).

### 2026-11-16 item 3 — paused checkpoint

1. **What is done:**
   - Added mounted Linux FUSE + Compose (PG + RustFS) janitor GC scenario `mounted-recoverable-trash-janitor-gc` in [`recoverable_trash_janitor.rs`](file:///E:/dev/studiofs-cli/crates/studiofs-e2e/src/scenario/workloads/recoverable_trash_janitor.rs) and registered it in [`workloads.rs`](file:///E:/dev/studiofs-cli/crates/studiofs-e2e/src/scenario/workloads.rs) and [`registry.rs`](file:///E:/dev/studiofs-cli/crates/studiofs-e2e/src/scenario/registry.rs).
   - Scenario covers:
     - **Reclaim:** purged trashed `purge-me.bin` via `studiofs janitor <vol> --full-scan --skip-newer-minutes 0 --max-deletes 500 --output jsonl` (dry run + live run 1), plus post-snapshot-delete reclaim in run 2.
     - **Protection (a) snapshot before purge:** `snap-keep.bin` trashed before `studiofs snapshot <vol> create` (`ready` at `cut_version=4`), `purge-me.bin` created and trashed after cut, both purged; verifies `snap-keep` survives run 1 and is reclaimed in run 2 only after `studiofs snapshot <vol> delete`.
     - **Protection (b) live hard link outside trash:** `hardlink-trashed.bin` + `outside-live.bin` (`nlink=2`), trashed and purged `hardlink-trashed.bin`; verifies `outside-live.bin` survives both janitor runs and stays readable (`nlink=1`) on both clients.
     - **Protection (c) peer restore before settlement:** `restore-race.bin` trashed, PG stopped, local purge + sync barrier on `client-a` + `force_kill`, PG restarted, `client-b` restores `restore-race.bin` -> `restored-before-settle.bin` (new published payload `object_id`), `client-a` remounts to retire the losing purge; verifies restored payload stays protected across both runs while superseded pre-restore `restore_orig_obj` is reclaimed in run 2 after snapshot deletion.
     - **No second destructive path:** verifies all tracked S3 payload keys remain present after shared settlement prior to any janitor run, asserts exact S3 key set diffs per janitor run, and scans daemon `stdout`/`stderr`/tracing logs for forbidden deletion markers.
   - Fixed two test-side harness bugs during bring-up: canonical payload-object key filtering (`metadata/snapshots/...` excluded from payload diffs) and waiting on `wait_published_object_for_inode_excluding` + `wait_client_drained` on `client-b` after `restore_retained_entry` so the new restored `object_id` (`2199023255552`) is observed cleanly.

2. **What is NOT done (current blocker & remaining work):**
   - Scenario reached Step 7 (`janitor-dry-run-1-attempt-1`) cleanly (Steps 1–6 — setup, snapshot creation, offline purge + crash, peer restore, `client-a` remount retirement, post-purge PG settlement `hl_settled` and `restore_settled`, pre-janitor S3 intact check — all passed in `-3` and `-4`), then failed at Step 7 with exit code `4`:
     `unsafe_to_delete: local PostgreSQL metadata has pending sync or recovery state` (from [`validate_protection`](file:///E:/dev/studiofs-cli/crates/studiofs-meta/src/janitor.rs#L225-L281)).
   - Exact failing predicate among the 4 checks at `janitor.rs:252-272` (`LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER != 0`, `LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER != 0`, `NEXT_EVENT_SEQ != LOCAL_PG_FLUSHED_EVENT_SEQ`, or non-empty `pending_local_upload_ledger`) on the retained `"janitor"` client DB (`target/sfs1054-item3-janitor-run-4/artifacts/clients/janitor/client-root/meta/.../*.db`) has not yet been printed, and the root cause (product vs fixture lifecycle) is not yet resolved. In `-4`, `"janitor"`'s final `runtime_snapshot` reported `pg_sync.lifecycle="live"` and `startup_target_version=null` (`sync_safety.state="safe"`, `coherence.state="catching_up"`), and `open_with_options` in [`pg_engine.rs:1597-1603`](file:///E:/dev/studiofs-cli/crates/studiofs-meta/src/pg_engine.rs#L1597-L1603) only computes `restored_flushed_event_seq` in memory without persisting `LOCAL_PG_FLUSHED_EVENT_SEQ` if `NEXT_EVENT_SEQ != 0`.
   - Final GREEN mounted run of `mounted-recoverable-trash-janitor-gc` and any required `studiofs-meta` / `studiofs` product fix + WSL clippy remain pending.

3. **RED / GREEN evidence so far:**
   - **Focused Windows tests (post-edit):**
     - `cargo test -p studiofs-e2e janitor -- --nocapture` → **`5 passed; 0 failed; 0 ignored`** across 6 test binaries (5/5 in `lib.rs`; 0 in `main.rs`, `client_controller`, `compose_infra`, `mounted_tree_validator`, `runner_contract`).
     - `cargo clippy -p studiofs-e2e --all-targets -- -D warnings` (Windows) → **passed** (exit `0`).
     - `cargo fmt --all -- --check` and `git -c core.safecrlf=false diff --check` → **passed** (exit `0`).
   - **WSL uninstrumented binaries (`CARGO_TARGET_DIR=target/posix-linux`, `target/sfs1054-item3-janitor-binaries.sha256`):**
     - `studiofs`: `31854631c1d126d1d338106fb8feabe1b24addc878b1a358dee594cf2ba444c5`
     - `studiofs-e2e`: `f3ee64be3a339b6d24b2aa9e6fa0085894f8327a9d7211f368219eb19b605875`
   - **Mounted Linux E2E runs (`mounted-recoverable-trash-janitor-gc`, `--platform linux-fuse --infra compose --port-range 30400-30500 --seed 1054`):**
     - `run_id=sfs1054-janitor-20261116-1` → **Failed** (`0/1`; metadata key diff included `metadata/snapshots/...`; log `target/sfs1054-item3-janitor-run-1.log`).
     - `run_id=sfs1054-janitor-20261116-2` → **Failed** (`0/1`; `restore_live_obj` poll returned pre-restore `object_id`; log `target/sfs1054-item3-janitor-run-2.log`).
     - `run_id=sfs1054-janitor-20261116-3` → **Failed** (`0/1`; Steps 1–6 passed, failed Step 7 `janitor-dry-run-1-attempt-1` exit `4` `unsafe_to_delete`; log `target/sfs1054-item3-janitor-run-3.log`).
     - `run_id=sfs1054-janitor-20261116-4` → **Failed** (`0/1`; Steps 1–6 passed, `"janitor"` mount/unmount reached `pg_sync.lifecycle="live"` before file creation, still failed Step 7 `janitor-dry-run-1-attempt-1` exit `4` `unsafe_to_delete: local PostgreSQL metadata has pending sync or recovery state`; log `target/sfs1054-item3-janitor-run-4.log`, archived sandbox `target/sfs1054-item3-janitor-run-4/`).
   - **Settlement:** Verified `0` owned mounts, `0` owned processes, `0` Compose containers/volumes, and `/tmp/studiofs-sfs1054-janitor` removed (`target/sfs1054-item3-janitor-settlement.log`, `OWNED_EXECUTION_SETTLED`).

4. **Precise next step for resuming:**
   1. Open the archived read-only DB `target/sfs1054-item3-janitor-run-4/artifacts/clients/janitor/client-root/meta/sfs-e2e-sfs1054-janitor-20261116-4-1054-mounte-093a241e577b305e/sfs-e2e-sfs1054-janitor-20261116-4-1054-mounte-093a241e577b305e.db` via a small unit test (without leaving temp edits) to print the 4 `janitor.rs:252-272` checks (`repair`, `fresh`, `next_event_seq`, `flushed_event_seq`, `pending_local_upload_ledger`).
   2. If a product defect is confirmed (e.g., `PgMetaEngine` / `discover` leaves `LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER != 0` or `LOCAL_PG_FLUSHED_EVENT_SEQ != NEXT_EVENT_SEQ` on a clean synced client), write the RED unit/PG test first, apply the minimal cause-level fix, verify GREEN on Windows + WSL, rebuild `target/posix-linux` binaries, and re-run `mounted-recoverable-trash-janitor-gc` (`--run-id sfs1054-janitor-20261116-5`).




### 2026-11-16 item 3 result — janitor GC mounted PG/S3 GREEN

1. **Root cause (product defect, not a scenario bug):**
   - A read-only probe of a *copy* of the archived run-4 `janitor` client DB
     (scratch `redb` 4.4.0 `ReadOnlyDatabase`; the archive itself was not opened)
     showed `nextEventSeq` **absent** (the client never wrote a local event),
     `pgLastFlushedLocalEventSeq = 1`, `pgFreshClientPending = 0`, no repair
     counter. `client-a`/`client-b` had `nextEventSeq == pgLastFlushedLocalEventSeq`
     (`83/83`, `41/41`).
   - `JanitorMetadataSession::validate_protection` compared the **raw** counters
     (`0 != 1`) and failed closed. Everywhere else the engine treats an absent
     `NEXT_EVENT_SEQ` as the empty log with exclusive end `1`
     (`RedbMetaEngine::local_outbound_ack_positions`, `retained.rs`,
     `reduce_local_dentry_mutations_in_txn`). So any synced client with an
     empty local log, such as a dedicated janitor client, could never run the
     janitor. The scenario ran the janitor the documented way: on the client's
     own metadata dir, after a safe sync and unmount.
   - **Fix (`crates/studiofs-meta/src/janitor.rs`):** `validate_protection` now
     compares the normalized pair from `local_outbound_ack_positions()`
     (end `.max(1)`, cursor with the pruned floor and `.max(1)`). It is still an
     exact `!=` check, so it still fails closed on any unacknowledged event or a
     cursor past the end. No safety predicate was removed.
   - **Test fixes (`crates/studiofs-meta/src/pg_engine/tests/janitor.rs`):**
     - The existing unsynced-local fixture now sets
       `NEXT_EVENT_SEQ = next.max(1) + 1`, so it is a real unacknowledged event
       under exclusive-end semantics (it previously relied on the raw `0 → 1`).
     - A stale branch assertion in
       `janitor_refuses_missing_interrupted_drifted_or_unsupported_gc_schema`
       expected `payload_gc` version `1`. The branch's schema is version `2`
       (`PAYLOAD_GC_SCHEMA_VERSION`). This test failed before this fix too.

2. **RED → GREEN (Windows, PG tests use `STUDIOFS_TEST_PG_DSN` from `.env`):**
   - New test `janitor_accepts_synced_client_without_local_events`:
     - RED: `cargo test -p studiofs-meta --lib janitor_ -- --ignored` →
       `9 passed; 2 failed` (new test panicked at `validate_protection`; the
       other failure was the pre-existing stale schema-version assertion).
     - GREEN: `cargo test -p studiofs-meta --lib janitor -- --ignored` →
       `12 passed; 0 failed` (1243 filtered out).
   - `cargo test -p studiofs-meta --lib janitor` (non-ignored) →
     `3 passed; 0 failed; 12 ignored`.
   - `cargo clippy -p studiofs-meta --all-targets -- -D warnings` (Windows),
     `CARGO_TARGET_DIR=target/posix-linux cargo clippy -p studiofs-meta -p studiofs-e2e --all-targets -- -D warnings` (WSL),
     `cargo fmt --all -- --check` and `git -c core.safecrlf=false diff --check`
     all exited `0`.

3. **WSL uninstrumented binaries (`target/sfs1054-item3-janitor-fix-binaries.sha256`):**
   - `studiofs`: `0f92a672edabcfa1221ed6cf61d473814835349d98b00132fe727a54110d5261`
   - `studiofs-e2e`: `2bd72223f7edd178026d8b71d17b557f9186ef1cc081e4e64907d66c79adb369`

4. **Mounted run (same command shape, `--root /tmp/studiofs-sfs1054-janitor --seed 1054 --platform linux-fuse --retain always --infra compose --port-range 30400-30500`):**
   - `mounted-recoverable-trash-janitor-gc`, `run_id=sfs1054-janitor-20261116-5` →
     **1/1 PASSED** (exit `0`, report duration `91401`ms). No scenario change was
     needed. Log `target/sfs1054-item3-janitor-run-5.log`; complete sandbox
     `target/sfs1054-item3-janitor-run-5/` (archive verified with `diff -rq`).
   - `janitor-gc-summary.json`: S3 payload keys `5 → 4 → 2`.
     - dry-run-1: `would_delete=1`, `skipped_young=0`, `skipped_unknown=0`.
     - run-1: `delete_confirmed=1`, `delete_errors=0`.
     - dry-run-2: `would_delete=2`.
     - run-2: `delete_confirmed=2`, `delete_errors=0`.
     - `daemon_deletion_log_hits=0` across 6 log files;
       `outside_hardlink_final_nlink=1`.

5. **Protection cases proven (all flags `true` in the summary):**
   - **Reclaim:** purged `purge-me.bin` payload deleted by run-1.
   - **Snapshot before purge:** `snap-keep.bin` payload survives run-1, and is
     reclaimed by run-2 only after `snapshot delete`.
   - **Live hard link outside trash:** `outside-live.bin` payload survives both
     runs and stays readable on both clients.
   - **Peer restore before settlement:** the restored payload
     (`object_id=2199023255552`) survives both runs. The superseded pre-restore
     object is reclaimed only in run-2. *(Superseded by "item 3 review
     remediation" below: in this run `restore-race.bin` predated the snapshot
     cut, so the run-2-only reclaim proved snapshot pinning, not the
     restore/purge outcome.)*
   - **No second destructive path:** all keys were present before any janitor
     run; key diffs are exact per run; there are no daemon deletion markers.
   - **Not proven here:** Windows/macOS mounted janitor runs; the
     PG-down + S3-down durability path, which stays separate; a repeat
     (flakiness) loop; and the meaning of run-1 `delete_attempted=2` vs
     `delete_confirmed=1`, which is reported by the janitor and was not
     investigated.

6. **Settlement:** `target/sfs1054-item3-janitor-fix-settlement.log` ends
   `OWNED_EXECUTION_SETTLED`.
   - `mounts_owned=0`, `procs_owned=0`, `0` run-owned Compose
     containers/volumes, and `/tmp/studiofs-sfs1054-janitor` was removed after
     the archive was verified.
   - The first two passes reported `procs_owned=3`. These were `pgrep -f`
     matching the settlement shell's own argv; `ps` showed no studiofs
     process. The final pass ran from `target/sfs1054-item3-settle.sh`.

### 2026-11-16 item 3 review remediation

The independent review found no product defect. It found four scenario and test
weaknesses (F1–F4). All four are fixed. No product source changed: the `studiofs`
binary hash is identical to run 5.

1. **What changed:**
   - **F1, protection case (c)** (`recoverable_trash_janitor.rs`):
     - Traced `RedbMetaEngine::restore_retained_entry` (`redb_engine/retained.rs`).
       Restore *renames* the retained inode (`receipt/data`) back into the
       namespace. `protect_retained_file_payload_in_txn` then adopts the staged
       copy's slices onto that same inode as a fresh object and bumps the
       content epoch. The stage inode is deleted.
     - So the review's proposed `restore_live_ino != restore_ino` is incorrect
       for this design. The scenario now asserts `restore_live_ino == restore_ino`
       (identity preserved) and `restore_live_obj != restore_orig_obj` (fresh
       payload). The underlying concern still holds: the losing purge's old
       generation cannot reach the restored payload object.
     - `restore-race.bin` is now written **after** the snapshot cut, together
       with `purge-me.bin`. Before the janitor runs, the scenario waits for
       `restore_orig_obj` to reach the ordinary unacknowledged GC-candidate state
       (`ref=0`, reachability closed, `published`, candidate not acknowledged).
     - Exact S3 key-set expectations:
       - run 1 removes exactly `purge_keys ∪ restore_orig_keys`, and both PG rows
         are settled;
       - run 2 removes exactly `snap_keys`.
     - Dry-run thresholds are now `2` and `1`. The conditional
       `restore_live_obj != restore_orig_obj` branch was removed.
       `validate()` now checks both count deltas and that `restore_orig_keys` is
       non-empty.
     - The scenario description was reworded.
   - **F2:** the peer restore no longer retries `fs::rename` on every error.
     - Run 6 showed why the old loop passed: restoring a payload that is not in
       client-b's local cache returns `RemoteContentPending` → `EAGAIN`
       (`io_ops.rs` `copy_retained_entry_to_stage`) and queues a fetch.
     - The scenario now waits on the real condition: a bounded
       `expect_payload(b_restore_src, RESTORE_RACE_BYTES)` readback. It then
       calls `rename` once and propagates any error.
   - **F3:** log reads now use `String::from_utf8_lossy(&fs::read(..)?)` and
     propagate I/O errors. The `janitor` client's daemon logs are now in the
     scan.
     - The directory filter was `name.contains(client.name)`. It never matched
       rolled files (`studiofs-tracing.log.<date>`), and the janitor client has
       only the rolled file. It now matches the tracing-log file-name prefix in
       the per-client artifact dir. Scanned files went from 6 to 11.
     - The janitor CLI's own logs (`client-root/logs/studiofs-cli-tool*`) are
       still excluded by design.
   - **F4** (`pg_engine/tests/janitor.rs`): `query_i64` takes
     `&'static [i32]` params. The `payload_gc` assertion binds
     `PAYLOAD_GC_SCHEMA_VERSION` as `$1`.

2. **Protection cases now proven** (runs 7 and 8, all summary flags `true`):
   - **Reclaim:** `purge-me.bin` payload deleted by run 1.
   - **Snapshot before purge:** `snap-keep.bin` survives run 1. Run 2 reclaims
     it after `snapshot delete`, and it is the only key set run 2 removes.
   - **Live hard link outside trash:** `outside-live.bin` survives both runs and
     is readable on both clients with `nlink=1`.
   - **Peer restore before settlement:**
     - the losing offline purge on client-a is retired (after remount, the
       trash entry is absent and the restored file is readable on client-a);
     - the restored copy is durable: same inode, fresh `object_id=2199023255552`,
       survives both runs, readable on both clients;
     - the old payload (`object_id=1099511627779`, not snapshot-pinned) is
       reclaimed only via the janitor, in run 1, and was present after shared
       settlement.
   - **No second destructive path:**
     - all keys were present after settlement and before any janitor run;
     - the per-run key diffs are exact;
     - there are 0 deletion markers across 11 daemon log files for
       `client-a`, `client-b`, and `janitor`.

3. **Counts:**
   - Windows:
     - `cargo test -p studiofs-e2e janitor` → `5 passed; 0 failed` (lib; the
       other 5 binaries filter to 0).
     - `cargo test -p studiofs-meta --lib janitor` → `3 passed; 0 failed;
       12 ignored`.
     - `cargo test -p studiofs-meta --lib janitor -- --ignored
       --test-threads=1` (live PG) → `12 passed; 0 failed`.
     - `cargo clippy -p studiofs-meta -p studiofs-e2e --all-targets -- -D
       warnings`, `cargo fmt --all -- --check`, and
       `git -c core.safecrlf=false diff --check` all exited `0`.
   - WSL: `CARGO_TARGET_DIR=target/posix-linux cargo clippy -p studiofs-e2e
     --all-targets -- -D warnings` exited `0`.
   - Mounted runs 7 and 8 had identical results:
     - S3 payload keys `5 → 3 → 2`;
     - dry-run-1 `would_delete=2`, run-1 `delete_confirmed=2`;
     - dry-run-2 `would_delete=1`, run-2 `delete_confirmed=1`;
     - `delete_errors=0`, `skipped_young=0`, `skipped_unknown=0`.
     - `delete_attempted` is `2×` confirmed in both runs. This was not
       investigated, as in run 5.

4. **Run ids** (same command shape as run 5,
   `--root /tmp/studiofs-sfs1054-janitor --seed 1054 --platform linux-fuse
   --retain always --infra compose --port-range 30400-30500`):
   - `sfs1054-janitor-20261116-6` → **Failed** (`0/1`, `EAGAIN` from the
     single restore rename). This is the F2 finding above. The scenario was
     fixed and the binary rebuilt.
   - `sfs1054-janitor-20261116-7` → **1/1 PASSED** (`90300` ms).
   - `sfs1054-janitor-20261116-8` → **1/1 PASSED** (`91596` ms). Stability
     rerun of the same binary; `sha256sum -c` passed before the run.
   - Logs are `target/sfs1054-item3-janitor-run-{6,7,8}.log`. Sandboxes are
     archived at `target/sfs1054-item3-janitor-run-{6,7,8}/`, each verified
     with `diff -rq`.

5. **Hashes** (`target/sfs1054-item3-janitor-remediation-binaries.sha256`,
   runs 7–8):
   - `studiofs`: `0f92a672edabcfa1221ed6cf61d473814835349d98b00132fe727a54110d5261`
     (unchanged from run 5)
   - `studiofs-e2e`: `37fa6ce4cfadb264fc81e66b770d204050d9838d7b631100b90874ee4c45e8c3`
   - Run 6 used `studiofs-e2e`
     `35b2cb29cddd174c1470ad8ae12d70439f05ed2cc05565bc5c2902338071a7a8`.

6. **Settlement:** `target/sfs1054-item3-janitor-remediation-settlement.log`
   (script `target/sfs1054-item3-remediation-settle.sh`) ends
   `OWNED_EXECUTION_SETTLED`.
   - `mounts_owned=0`, `procs_owned=0`, and 0 Compose containers or volumes
     for runs 6–8.
   - All three archives were verified. `/tmp/studiofs-sfs1054-janitor` was
     removed.
   - **Harness observation (not fixed, out of scope):** after run 6 failed
     between `force_kill("client-a")` and `remount`, runner cleanup reported
     `passed` but left client-a's FUSE mount disconnected
     ("Transport endpoint is not connected"). I lazily unmounted it with
     `fusermount3 -u -z` before archiving.

### 2026-11-16 item 4 — desktop GVFS: blocked checkpoint (not feasible on this host)

Repository root, branch `feat/sfs-1054-recoverable-trash`, the
same uncommitted working tree (101 changed/untracked paths preserved). No
product, test, or scenario source changed in this pass. No package was
installed and no global configuration was changed.

1. **Already proven (citations).**
   - Client-side `gio trash` (GLib's local-file move into `.Trash-<uid>/{files,info}`)
     on a real WSL2 FUSE mount, with filesystem listing, exact reads, restore by
     rename, collision refusal, directory recursive Clear and TTL-zero deletion:
     `crates/studiofs-linux-fuse/src/low_level.rs`
     `low_level::tests::mounted_linux_gio_trash_retains_lists_restores_and_clears`
     (Redb only; see "Linux native trash completed in WSL" above).
   - Mounted PG/S3 two-client GIO retention, peer listing via `.Trash-<uid>/info`,
     peer restore and Clear: registered scenario
     `mounted-recoverable-trash-two-client`
     (`crates/studiofs-e2e/src/scenario/workloads/recoverable_trash.rs`, `gio trash`
     at the `GIO trash` command), run `sfs1054-pgs3-20261116-2`.
   - `gio trash` under crash replay / cold bootstrap
     (`recoverable_trash_durability.rs`, `offline GIO trash`) and janitor GC
     (`recoverable_trash_janitor.rs`, `gio_trash_path`).
   - GIO's sidecar draft/rename sequence at VFS level:
     `retained_views::native_tests::linux_gio_atomic_trashinfo_draft_stays_local_and_cannot_replace_generated_record`.
2. **Not proven (item 4 remainder).** Every operation that goes through the
   GVFS `trash:` backend: `gio list trash:///`, `gio trash --list`,
   `gio trash --restore trash:///…`, `gio trash --empty`, and any file manager
   (Nautilus) view of the volume's `.Trash-<uid>`. GLib alone implements only
   Move to Trash; listing/restore/empty of `trash:///` require `gvfsd-trash`,
   which also decides whether a FUSE mount's `.Trash-<uid>` is aggregated
   (its mount-type filtering is untested against `fuse.studiofs`).
3. **Feasibility evidence** (`target/sfs1054-item4-feasibility.log`, SHA-256
   `fa142091a3fabeed0a51a960131681324b59630d1b28ef6cee1a008db6f41f1c`),
   WSL `Ubuntu 24.04.5 LTS`, kernel `6.6.114.1-microsoft-standard-WSL2`:
   - present: `/usr/bin/gio` (GLib `2.80.0`, `libglib2.0-bin 2.80.0-6ubuntu3.10`),
     `dbus` and `dbus-user-session 1.14.10-4ubuntu4.1`, a live session bus
     `unix:path=/run/user/1000/bus`, `systemctl --user` = `running`, WSLg
     (`DISPLAY=:0`, `WAYLAND_DISPLAY=wayland-0`, `/mnt/wslg`), `/dev/fuse`,
     `fusermount3`;
   - missing: `dpkg -l gvfs` → `un gvfs <none>`; `gvfs-daemons`,
     `gvfs-backends`, `gvfs-common`, `gvfs-libs`, `nautilus` not installed;
     `/usr/libexec/gvfsd` and `/usr/libexec/gvfsd-trash` absent; GIO modules
     dir holds only `libdconfsettings.so` (no `libgvfsdbus.so`); session-bus
     `ListNames` has 0 `gvfs` names;
   - effect: `gio list trash:///` → `gio: trash:///: Operation not supported`;
     `gio trash --list` → `gio: trash:: Operation not supported` (GLib 2.80
     still printed exit 0 for both, so the stderr text is the evidence);
   - install path exists but is blocked: `apt-cache policy` shows candidates
     `gvfs 1.54.4-0ubuntu1~24.04.5`, `gvfs-daemons`/`gvfs-backends` same,
     `nautilus 1:46.4-0ubuntu0.2`; `sudo -n true` → `sudo: a password is required`.
4. **Decision.** Stopped per spec ("If native integration is impossible, report
   concrete evidence … Do not silently replace it with CLI-only UX"). The
   filesystem-level `.Trash-<uid>/info` listing already covered by items above
   is NOT counted as `trash:///` evidence. No RED/GREEN cycle ran; no binaries
   were built; no mounted run IDs, hashes or Compose resources exist for this
   item, so there is nothing to settle.
5. **What the user must provide to resume.**
   - Authorization plus sudo in WSL Ubuntu for:
     `sudo apt-get install --no-install-recommends gvfs gvfs-daemons gvfs-backends`
     (adds `gvfsd`, `gvfsd-trash` and the GIO D-Bus VFS module; this is a
     system-wide package change). `nautilus` is optional, only for a visual
     file-manager check over WSLg.
   - Verification before resuming: `ls /usr/libexec/gvfsd-trash` exists and
     `gio list trash:///` returns without `Operation not supported`.
   - Resume plan: extend `mounted-recoverable-trash-two-client` (or add one
     sibling scenario) with an isolated `XDG_DATA_HOME`/`HOME` under the
     sandbox so the host trash is never touched, and a `dbus-run-session`
     wrapper so its `gvfsd` is scenario-owned; then `gio trash` → assert the
     entry in `gio list trash:///` (with `trash::orig-path`) → `gio trash
     --restore` → `gio trash --empty`, asserting StudioFS receipts and the peer
     view after each step. Build uninstrumented `CARGO_TARGET_DIR=target/posix-linux`
     binaries and record hashes as in items 1–3.

### 2026-11-16 item 4 result — desktop GVFS `trash:///` mounted PG/S3 GREEN

Resumed after the user-approved install of `gvfs`, `gvfs-daemons`, and
`gvfs-backends` `1.54.4-0ubuntu1~24.04.5` in WSL Ubuntu 24.04. Nothing else was
installed and sudo was not used. Nothing was committed.

1. **Proven through `trash:///`.** Registered scenario
   `mounted-recoverable-trash-two-client` (extended, not a new scenario)
   now runs, after its existing GIO/peer assertions and with both clients
   mounted:
   - `gio trash <writer mount>/gvfs-restore.bin`.
   - `gio list -u -a trash::orig-path trash:///` lists the entry once per
     mount: GVFS aggregates the writer's and the peer's `.Trash-1000`, so the
     peer's view of the shared receipt is itself visible through `trash:///`.
   - `gio trash --restore trash:///…` restores the file to its original path.
   - A second entry is moved with `gio trash`, then `gio trash --empty` purges
     it.
   After each step the scenario asserts writer and peer state through the
   mounts: original visible or hidden, `files/<name>` exact bytes,
   `info/<name>.trashinfo` `Path=`, and receipt removal after restore or empty.
   The restored bytes are also checked on both clients after the purge.
   Helper: `crates/studiofs-e2e/src/scenario/workloads/recoverable_trash_gvfs.rs`.
   Phase: `gvfs_trash_phase` in `recoverable_trash.rs`.
2. **Isolation.** A private `dbus-run-session` holds the bus open with `cat`
   on a piped stdin, and closing that stdin ends the bus.
   - `HOME`, `XDG_DATA_HOME`, `XDG_CONFIG_HOME`, `XDG_CACHE_HOME`, and a `0700`
     `XDG_RUNTIME_DIR` live under `<run sandbox>/gvfs` in the run-owned `/tmp`
     root. `DBUS_SESSION_BUS_ADDRESS`, `DISPLAY`, and `WAYLAND_DISPLAY` are
     removed. `gvfsd` and `gvfsd-trash` are D-Bus-activated only on that bus.
   - `stop` proves settlement: bounded polling of `/proc/*/environ` for the
     private `XDG_RUNTIME_DIR` must reach zero processes. A failure is reported
     as `CleanupAfterFailure` and never hides the primary error.
   - `gio trash --empty` purges every aggregated trash, including other host
     mounts. The scenario therefore refuses to run it unless the `trash:///`
     listing contains exactly this run's entry, once per mount. Residual: this
     is a check-then-act window.
   - GLib 2.80 can exit 0 while printing `gio: …` errors, so stderr is also
     treated as failure.
3. **GVFS mount-inclusion verdict: included; no FUSE adapter change.**
   - Source: in gvfs 1.54.4 `daemon/trashlib/trashwatcher.c`,
     `ignore_trash_mount` drops a mount only for
     `g_unix_mount_is_system_internal` or the `x-gvfs-notrash` option.
     `decide_watch_type` special-cases only `nfs`, `nfs4`, and `cifs`.
   - In GLib 2.80 `gunixmounts.c`, `fuse.studiofs` is not in the system
     fs-type list, and the device or path is not in the system device or path
     lists (exact matches plus `/dev/`, `/proc/`, `/sys/` prefixes).
   - Observed: run 1 listed the writer entry in `trash:///`, and run 1's
     refusal message recorded both clients' `.Trash-1000` entries.
4. **Product defect found and fixed: GVFS delete/Empty Trash was a silent
   no-op.**
   - gvfs `trashitem.c` `trash_item_delete` first runs
     `g_file_make_directory_with_parents(<trash>/expunged)`. It then needs
     `rename(files/<x> -> expunged/<n>)` (up to 1000 names; errors discarded),
     followed by a best-effort sidecar unlink and `trash_expunge`.
   - StudioFS returned `OperationNotSupported` for any `.Trash-<uid>` child
     other than `files` and `info` (`reject_trash_container_mkdir`,
     `namespace_ops.rs`), so every delete failed. GLib's `gio trash --empty`
     ignores per-item delete errors.
   - RED, mounted: run `sfs1054-gvfs-20261116-2` passed listing and restore.
     After `gio trash --empty` exited 0, both entries were still listed for
     80 s (archived report).
   - Fix (`studiofs-vfs`): Linux projects a synthetic, always-empty
     `.Trash-<uid>/expunged` with the same `0700`/uid shape as `files`/`info`
     (`TrashViewBacking::LinuxExpungedDir`, `TrashContainerRole::LinuxExpunged`):
     - `mkdir` reports `EntryExists`, `rmdir` is refused, and listing is empty.
     - It is a non-target container, so ordinary renames into it and
       `create`/`mkdir` inside it are refused.
     - A rename of a *visible restorable receipt view* into it calls the
       existing `request_trash_view_purge`, which applies the same
       original-parent write check as `unlink`. A directory receipt is cleared
       as one receipt. This matches expiry, not the per-child `rm -r` path,
       which still applies when a client deletes children itself.
   - `docs/platform-bridges.md` documents the sink.
   - RED unit test
     `retained_views::native_tests::linux_gvfs_expunge_rename_clears_file_and_directory_receipts`
     failed at the `expunged` `mkdir` assertion: Windows,
     `0 passed; 1 failed`, log `target/sfs1054-item4-vfs-red.log`.
   - GREEN: `1 passed` (`target/sfs1054-item4-vfs-green.log`). The existing
     Linux container test now expects `expunged`, `files`, `info`.
5. **Validation.**
   - Windows:
     - `cargo test -p studiofs-vfs --lib retained_views` -> `48 passed; 0 failed`.
     - `cargo test -p studiofs-vfs --lib trash` -> `40 passed; 0 failed`.
     - `cargo test -p studiofs-e2e --lib recoverable_trash` -> `6 passed; 0 failed`.
     - `cargo clippy -p studiofs-vfs -p studiofs-e2e --all-targets -- -D warnings`
       -> exit 0.
   - WSL, `CARGO_TARGET_DIR=target/posix-linux`
     (`target/sfs1054-item4-wsl-validate.log`):
     - `retained_views` -> `48 passed`.
     - `studiofs-e2e` `recoverable_trash` -> `6 passed`.
     - `cargo clippy -p studiofs-vfs -p studiofs-linux-fuse -p studiofs-e2e --all-targets -- -D warnings`
       -> exit 0.
   - Real FUSE test `mounted_linux_gio_trash_retains_lists_restores_and_clears`:
     the run with the mount under `/mnt/e` failed at `Session::mount` with
     ENOENT, which is a drvfs mount point and not a trash assertion. Rerun as
     before with `CARGO_TARGET_DIR=/tmp/…`, inside a private `dbus-run-session`
     -> `1 passed` (`target/sfs1054-item4-linux-fuse-mounted.log`).
   - `cargo fmt --all -- --check` and `git -c core.safecrlf=false diff --check`
     -> exit 0.
6. **Binaries** (uninstrumented WSL debug,
   `target/sfs1054-item4-gvfs-binaries.sha256`):
   - `studiofs`: `c353281ae4ff34cc56486f1c97f77dbb1421f498375e4205b0f8c01e8a261e42`
   - `studiofs-e2e`: `b03fa78609a580de2aa371a3f53b74e27493edf041e76a75263c4577f81a0b9f`
   Runs 1–2 used `studiofs` `0f92a672…` (unchanged item-3 product). Their
   harness builds were `studiofs-e2e` `46c7317c…` and `53474352…`.
7. **Mounted runs.** Same command shape: `--root /tmp/studiofs-sfs1054-gvfs
   --seed 1054 --platform linux-fuse --retain always --infra compose
   --port-range 30400-30500`. Logs are at `target/sfs1054-item4-gvfs-run-N.log`,
   and the full sandboxes are archived at `target/sfs1054-item4-gvfs-run-N/`.
   - `sfs1054-gvfs-20261116-1` — FAILED (harness):
     - The `--empty` guard counted the per-mount duplicate listing as foreign.
     - Separately, the `XDG_RUNTIME_DIR` under `artifacts/scenarios/…` exceeded
       the 108-byte `sun_path` limit, so gvfsd sockets collided
       (`Address already in use`).
     - Fixed with an origin-set guard and `<sandbox>/gvfs`.
   - `sfs1054-gvfs-20261116-2` — FAILED: the product RED for GVFS delete
     (item 4 above).
   - `sfs1054-gvfs-20261116-3` — **PASSED** 1/1, report duration 22,068 ms.
   - `sfs1054-gvfs-20261116-4` — **PASSED** 1/1 (stability rerun, same
     binaries), report duration 23,546 ms.
     - Summary: `empty_listing_before_purge=2` (one per mount),
       `listing_after_empty=0`,
       `restored_exact_bytes_on_writer_and_peer=true`,
       `empty_purged_receipts_on_writer_and_peer=true`,
       `gvfs_private_session_settled=true`.
8. **Not proven.**
   - Nautilus or any graphical file manager. Nautilus is not installed; it uses
     the same `gvfsd-trash` delete and restore path, but no UI run happened.
   - The `trash:///` view from a host where only one client is mounted. Here
     both mounts sit on one host, which is why entries appear twice.
   - A directory receipt through `trash:///`: only file entries went through
     GVFS in the mounted run. The directory case is unit-tested.
   - `gio trash --restore` onto an occupied path through GVFS.
   - GVFS behaviour while PG or S3 is down.
   - macOS and Windows equivalents.
   - The `--empty` guard has a check-then-act window.
9. **Settlement** (`target/sfs1054-item4-gvfs-settlement.log`, ends
   `OWNED_EXECUTION_SETTLED`):
   - `mounts_owned=0` and `procs_owned=0`. This includes private
     gvfsd/dbus processes; the `/proc` scan skips the settlement shell itself.
   - 0 `sfs1054-gvfs` containers and volumes.
   - `/tmp/studiofs-sfs1054-gvfs` and `/tmp/studiofs-sfs1054-item4-fuse` were
     removed after the archives were copied (`diff -rq` clean, sockets
     excepted). The format probe and fusetest `/tmp` dirs were removed by
     their own scripts.

### 2026-11-16 item 4 review remediation

Same branch and uncommitted working tree; nothing committed. Fixes the five
findings from the independent item 4 review. No product correctness defect was
reported, and none of the changes below alter the GVFS contract.

1. **F1 (Medium): no host-wide `gio trash --empty`.** `gvfsd-trash` aggregates
   every non-system mount (drvfs `/mnt/c`, other FUSE mounts, USB drives), so
   `--empty` could purge the host user's trash on other volumes, and the
   listing guard had a check-then-act window.
   - `gvfs_trash_phase` (`recoverable_trash.rs`) now runs
     `gio remove <exact trash:/// URI>`. The URI is the writer entry taken from
     the `trash:///` listing that the phase has just asserted. The guard is
     removed. The scenario never runs `--empty`.
   - Coverage of the Empty Trash verb: `gio trash --empty` (and Nautilus Empty
     Trash) enumerates `trash:///` and deletes each child through the backend
     `trash_item_delete`, which renames `files/<x>` into `expunged/<n>`.
     `gio remove trash:///…` reaches the same per-item backend delete. The
     verb is therefore covered through that delete, scoped to one item, not by
     running `--empty`.
   - The peer's listing entry for the same receipt converges through
     replication. The phase waits until neither origin is listed.
2. **F2 (Low): expunge target recorded absent.** The expunge branch in
   `rename_inner` (`namespace_ops.rs`) now records the source and
   `expunged/<n>` as negative dentries in one `record_ignored_rename` call.
   - A second `record_absent_target` after `record_removal` is not enough. Both
     records would carry the same start epoch, and `record_dentry_changes`
     admits only the first. The second would only bump the parent generation
     (`namespace.rs` `record_dentry_changes`).
   - The positive test asserts `dentry_cache_lookup(expunged, <n>)` is
     `Negative`.
3. **F3 (Low): a retried expunge is ENOENT.** When `new_parent` has the
   `LinuxExpunged` role and the source is not a restorable receipt view, the
   source is looked up first. A missing source returns the existing
   `EntryNotFound`. Every other refusal is unchanged: a present sidecar,
   subtree child, or live entry still gets `ENOTSUP`.
4. **F4 (Low): no orphaned daemons on a hung holder.** `GvfsSession::stop`
   (`recoverable_trash_gvfs.rs`) kills and waits for a hung or failed
   `dbus-run-session` holder. It then always runs the bounded `/proc` scan
   (`reap_survivors`), which matches the exact private `XDG_RUNTIME_DIR` and
   kills the owned survivors. It returns the holder error first, then the scan
   error.
5. **F5: negative tests** in `retained_views/native_tests.rs`, reusing
   `make_unstarted_test_vfs`:
   - New test
     `linux_gvfs_expunge_refusals_keep_receipts_and_retry_is_not_found`:
     - A `ReadOnly` original parent gets `AccessDenied`, and the receipt stays
       `Retained` in the store.
     - `info/note.trashinfo` into `expunged` gets `ENOTSUP`.
     - A subtree child of a directory view gets `ENOTSUP`, and the child is
       kept.
     - A second expunge of the same name gets `EntryNotFound`.
     - `rmdir expunged`, renaming `expunged` itself, and a rename out of it
       are all refused.
     - The Windows and macOS profiles project no `expunged`.
   - The existing positive test also asserts `PurgeRequested { reason: User }`
     and, for F2, the negative dentry.
6. **Spec.** `2026-11-12-recoverable-trash-design.md` now says, next to the
   per-child directory Clear decision, that a native whole-item "Delete
   permanently" or Empty Trash (GVFS expunge) purges a whole retained entry in
   one step, as expiry does. Per-child deletes still follow the per-child rule.
7. **RED/GREEN** (Windows, `cargo test -p studiofs-vfs --lib linux_gvfs_expunge`):
   - RED, `target/sfs1054-item4-remediation-vfs-red.log`:
     `0 passed; 2 failed`. Two assertions failed: the F2 negative dentry
     (`native_tests.rs:1135`), and the F3 retry, which returned `ENOTSUP`
     instead of `EntryNotFound` (`:1225`). Checks 1–3 and the store-state
     assertion already passed before the fix.
   - GREEN, `target/sfs1054-item4-remediation-vfs-green.log`:
     `2 passed; 0 failed`.
8. **Validation.**
   - Windows (`target/sfs1054-item4-remediation-win-validate.log`,
     `target/sfs1054-item4-remediation-e2e-win.log`):
     - `retained_views` -> `49 passed; 0 failed`.
     - `trash` -> `40 passed; 0 failed`.
     - `cargo test -p studiofs-e2e recoverable_trash` -> `6 passed; 0 failed`
       (lib; the other targets ran 0, filtered).
     - `cargo clippy -p studiofs-vfs -p studiofs-e2e --all-targets -- -D warnings`
       -> exit 0.
     - `cargo fmt --all -- --check` -> exit 0.
     - `git -c core.safecrlf=false diff --check` -> exit 0.
   - WSL, `CARGO_TARGET_DIR=target/posix-linux`
     (`target/sfs1054-item4-remediation-wsl-validate.log`):
     - `retained_views` -> `49 passed`.
     - `trash` -> `40 passed`.
     - `studiofs-e2e recoverable_trash` -> `6 passed`.
     - clippy -> exit 0.
9. **Binaries** (uninstrumented WSL debug,
   `target/sfs1054-item4-remediation-binaries.sha256`):
   - `studiofs`: `5be2f2226b674ec9eded2523bdbc9f193500dbfa19aa4b53ee59d8e562bbdc8d`
   - `studiofs-e2e`: `aa4b5bb4222ce586d1b94e718619c0cd6a775ed5894d7dd4926bea659e8e385b`
10. **Mounted runs.** Same command shape as runs 3–4. Logs are at
    `target/sfs1054-item4-gvfs-run-N.log`, and the sandboxes are archived at
    `target/sfs1054-item4-gvfs-run-N/`.
    - `sfs1054-gvfs-20261116-5` — **PASSED** 1/1, exit 0, report duration
      27,751 ms.
    - `sfs1054-gvfs-20261116-6` — **PASSED** 1/1, exit 0, report duration
      28,532 ms.
    - Run 6 summary: `deleted_uri` is the writer's exact
      `…/.Trash-1000/files/gvfs-empty.bin` entry,
      `empty_listing_before_purge=2`, `listing_after_empty=0`,
      `empty_purged_receipts_on_writer_and_peer=true`,
      `gvfs_private_session_settled=true`.
11. **Settlement** (`target/sfs1054-item4-remediation-settlement.log`, ends
    `OWNED_EXECUTION_SETTLED`):
    - `mounts_owned=0` and `procs_owned=0`, including the private gvfsd and
      dbus processes.
    - 0 `sfs1054-gvfs` containers and volumes.
    - `/tmp/studiofs-sfs1054-gvfs` was removed after both archives were
      copied.
12. **Not proven.** The F4 hung-holder branch is not exercised by a test. No
    run made the holder hang; runs 5–6 took the normal exit path.

## SFS-1054 final state (2026-11-16, item 5)

Same branch and uncommitted working tree; nothing committed. Item 5 changed
only documentation. No product or test source changed, and no gate regression
needed a fix.

### Delivered

- Shared per-volume trash TTL in whole days. It defaults to 0 (disabled). It is
  set by `studiofs init --trash-days N` or `studiofs trash-days NAME [DAYS]`,
  stored as a PostgreSQL CAS row and replicated to peers. `studiofs upgrade` has
  no TTL flag and does not enable retention.
- Persisted format 6 with old-client rejection: the v0.2.2 decoder rejects the
  header and checkpoint suffix, and PostgreSQL fences old publication and
  deletion claims.
- Only native Move to Trash is retained. `rm`, Shift+Delete, overwrite and
  rename replacement bypass retention.
- Native views: WinFSP `$RECYCLE.BIN` (`$I`/`$R`) and Linux `.Trash-<uid>` with
  `files`, `info` and the GVFS `expunged` sink. macOS `.Trashes/<uid>` exists in
  source only.
- Visibility follows the current original-parent ACL. Purge needs
  original-parent write; restore needs original-parent and destination write.
  There is no admin bypass.
- Offline restore versus purge/expiry: the first shared terminal transition
  wins the receipt, and restored data and identity always survive.
- Physical reclaim goes only through the shared GC queue and `studiofs janitor`.
  Expiry runs in `pg-meta-poll`.
- Documentation: EN/UK guide chapter 23 (rewritten), chapter 27 (`trash-days`
  section), and one-line corrections in chapters 15 and 28. Also
  `docs/platform-bridges.md` (format gate, per-platform proven/unproven table,
  E2E scenarios) and `docs/local-first-validation-matrix.md` (five
  recoverable-trash rows).

### Evidence

Final focused gates were run on the current tree in this pass. Logs are under
`target/sfs1054-gates/`.

| Gate | Result |
| --- | --- |
| Windows `cargo check` and `clippy --all-targets -D warnings`, `-p` meta/vfs/cache/studiofs/e2e/winfsp | exit 0 / exit 0 |
| WSL check and clippy, `-p` meta/vfs/cache/studiofs/e2e/linux-fuse (`target/posix-linux`) | exit 0 / exit 0 |
| `studiofs-meta --lib retained` | 34 passed, 0 failed, 34 ignored |
| `studiofs-meta --lib janitor` | 3 passed, 0 failed, 12 ignored |
| `studiofs-meta --lib trash`; `--test trash_compatibility` | 7 passed; 3 passed |
| `studiofs-vfs --lib retained_` | Win 80 passed; WSL 80 passed |
| `studiofs-vfs --lib trash` | Win 40 passed; WSL 40 passed |
| `studiofs-vfs --lib process_upload_` | 33 passed |
| `studiofs --bin studiofs trash` | 1 passed, 1 ignored |
| `studiofs --bin studiofs dump_load` | 22 passed, 2 ignored |
| `studiofs-e2e recoverable_trash` | Win 6 passed; WSL 6 passed |
| `studiofs-e2e janitor` | 5 passed |
| `studiofs-linux-fuse` focused bridge and invalidation tests (WSL) | 39 passed |
| Live PG: `pg_engine::tests::lifecycle::test_pg_retained_ -- --ignored --test-threads=1` | 19 passed, 0 failed, 321 s (see the note below) |
| Live PG+S3: `studiofs-vfs --test vfs_test retained_ -- --ignored --test-threads=1` | 4 passed, 61 s |
| `cargo fmt --all -- --check`; `git -c core.safecrlf=false diff --check` | exit 0; exit 0 |

Note on the live PG gate: the run with a 300 s budget passed 18 of 19 tests.
`timeout` killed it while it was running
`test_pg_retained_stale_expiry_after_period_raise_is_retired`. That test then
passed alone in 22 s, and the full filter passed 19/19 in 321 s with a 600 s
budget. This is a run-time budget overrun, not a hang or a regression.

Mounted acceptance evidence from earlier checkpoints was not re-run in item 5:

| Surface | Test or scenario | Run ids |
| --- | --- | --- |
| Windows Shell (local redb WinFSP) | `winfsp_native_trash_restore_and_clear_shell_probe`, `_directory_restore_and_clear_`, `_directory_partial_clear_`, `_directory_collision_`, `_remote_only_retry_probe`, `winfsp_default_disabled_trash_is_destructive_shell_probe` | 2026-11-13..15 checkpoints |
| Linux FUSE (redb) | `mounted_linux_gio_trash_retains_lists_restores_and_clears` | review rerun 2026-11-16 |
| PG/S3 two clients, identity and GVFS | `mounted-recoverable-trash-two-client` | `sfs1054-pgs3-20261116-2`, `sfs1054-identity-20261116-1`, `sfs1054-gvfs-20261116-5`, `-6` |
| S3-outage crash replay | `mounted-recoverable-trash-s3-crash-replay` | `sfs1054-f2ab-crash-20261116-1` |
| Cold bootstrap | `mounted-recoverable-trash-cold-bootstrap` | `sfs1054-f2ab-cold-20261116-1` |
| Janitor GC | `mounted-recoverable-trash-janitor-gc` | `sfs1054-janitor-20261116-7`, `-8` |

### Explicitly unproven or deferred

- macOS: compilation, Finder, legacy macFUSE and FSKit (deferred by the user).
- Interactive Windows Explorer UI. A mounted WinFSP + PostgreSQL multi-client
  run. The Windows Shell probes were not re-run on the final tree.
- Nautilus or any graphical file manager. `gio trash --empty` itself (its
  per-item backend delete is covered). GVFS while PG or S3 is down. A directory
  receipt through `trash:///`.
- An old mounted 0.2.2 binary against an upgraded volume. Only the decoder
  boundary is executed.
- Logical quota charging: SFS-1058 is not implemented. Retained bytes simply
  stay in storage until purge or expiry and a janitor run.
- Known limits, documented in the guides:
  - `studiofs load` refuses a target that already has trash authority rows.
  - Restore stages leak after a full `ReplaceLocalState`.
  - Restore keeps atime and mtime, but not ctime or xattrs.
  - The PG-down + S3-down repair refusal: `mounted-recoverable-trash-durability`
    is an expected RED. (2026-11-18: superseded by ADR 0013; it now passes.)
- Not run: workspace or full ignored suites.

### Item-5 docs review fixes (2026-11-16)

An independent review of the item-5 docs diff was applied. Docs only; no
product or test source changed.

- Guide chapter 23 (EN/UK): the Linux "What is proven" line now separates
  mounted-folder operations from GVFS `trash:///` file operations. It also says
  both clients ran on one test machine, and it lists a folder through
  `trash:///` as unproven. The old-0.2.2 paragraph now says only the decoder
  boundary is checked, and that every computer must be updated before the
  trash is turned on.
- UK wording: chapter 15 "главі" became "розділі", and "розраховуйте" became
  "покладайтеся". Chapter 23 "подання кошика", "Обходу для адміністратора" and
  "принципала" were reworded.
- `docs/platform-bridges.md`: remote-only restore is now described as a
  Shell-automated `InvokeVerb("undelete")`, not interactive Explorer.
- `docs/local-first-validation-matrix.md`: added `load_seeds_trash` to the TTL
  row, `retained_restore_denies_without_source_or_original_parent_write` to the
  permissions row, and `winfsp_default_disabled_trash_` to the mounted row.
- This plan: the outcome no longer claims acceptance, a stale paragraph is
  marked historical, and machine-specific `E:\…` paths became repository-relative.
  Older `file:///E:/…` links and forward-slash paths in historical checkpoints
  were left as recorded.

Tests that the matrix cites but the final gates had skipped:

| Command | Result |
| --- | --- |
| `cargo test -p studiofs-vfs --test vfs_test retained_restore_denies_without_source_or_original_parent_write` | 1 passed, 0 failed, 198 filtered out (`target/sfs1054-gates/vfs-retained-restore-denies.log`) |
| `cargo test -p studiofs --bin studiofs load_seeds_trash -- --ignored --test-threads=1` (live PG) | **0 passed, 1 failed**, 545 filtered out (`target/sfs1054-gates/load-seeds-trash.log`) |
| `git -c core.safecrlf=false diff --check` | exit 0 |

`load_seeds_trash_authority_and_rejects_existing_trash_rows` fails before it
touches PostgreSQL. Its local redb fixture calls
`restore_retained_entry(receipts[1], ROOT_INO, b"restored", b"stage", &[])`
(`crates/studiofs/src/cli/dump_load.rs:1414`). The current API requires one
`RetainedSourceFence` per source inode: `source_ids.len() != fences.len()`
returns `TargetChanged` (`crates/studiofs-meta/src/redb_engine/retained.rs`).
The fixture is stale; the restore fencing is correct. It was not fixed in this
docs-only pass, so the matrix TTL row currently cites a red test.

### What still blocks a PR

0. Fix the stale `load_seeds_trash_authority_and_rejects_existing_trash_rows`
   fixture. Pass a fence for the restored receipt's source inode, then rerun it
   against live PostgreSQL. See "Item-5 docs review fixes" above.

1. ~~Independent review of the item-5 documentation diff.~~ Done: reviewed,
   then fixed (see "Item-5 docs review fixes" below). Items 1–4 are reviewed.
2. A user decision on accepting the deferred and unproven surfaces above for
   0.2.3. The spec requires native-UI evidence per platform. Specifically:
   macOS, Explorer UI, Nautilus, and whether to re-run the Windows Shell
   probes on the final tree.
3. Commits, staged per task, and the PR itself are not authorized in this pass.
   The ExecPlan and the new source files are still untracked.

### Fix for the load_seeds_trash test (parent session, 2026-11-16)

`load_seeds_trash_authority_and_rejects_existing_trash_rows`
(`crates/studiofs/src/cli/dump_load.rs`) set up its fixture by calling
`restore_retained_entry(..., &[])`, the call shape from before fences
existed. The function now requires one `RetainedSourceFence` for each
source item, so the test fixture failed. The fixture now builds the
fence from the attrs of `receipt/data`; product code is unchanged.

Results:
- `cargo test -p studiofs --bin studiofs load_seeds_trash -- --ignored --test-threads=1`
  (live PostgreSQL): 1 passed, 0 failed.
- `cargo clippy -p studiofs --all-targets -- -D warnings`: clean.
- `cargo fmt --check` and `diff --check`: clean.

PR blocker item 0 is resolved.
