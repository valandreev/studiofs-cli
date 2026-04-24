# SFS-268 First-Publish Visibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hide a brand-new regular file from peer mounts until its first complete published version exists, while preserving existing update, overlay, and delete-before-publish behavior.

**Architecture:** Keep the publication contract centered in `studiofs-meta`. Peer-applied merged namespace reads and remote snapshot export stop surfacing unpublished new regular files, while first publish continues to reuse the existing event stream and becomes visible when the committed attr/base state flips to `Ready`.

**Tech Stack:** Rust 2024, `studiofs-meta`, `studiofs-vfs`, redb metadata engine, PG replication layer, focused `cargo test` commands.

---

### Task 1: Lock the new contract in meta-layer tests

**Files:**
- Modify: `crates/studiofs-meta/src/pg_engine.rs`
- Modify: `crates/studiofs-meta/src/redb_engine/tests/payload_objects.rs`
- Test: `crates/studiofs-meta/src/pg_engine.rs`
- Test: `crates/studiofs-meta/src/redb_engine/tests/payload_objects.rs`

- [ ] **Step 1: Write the failing test for PG peer invisibility before first publish**

Add or update the existing PG test so it asserts that a peer cannot `lookup()` a
new unpublished regular file:

```rust
assert!(wait_until(Duration::from_secs(10), || {
    let _ = b.sync_metadata();
    matches!(
        b.lookup(ROOT_INO, b"uploading.bin"),
        Err(MetaError::EntryNotFound { .. })
    )
}));
```

- [ ] **Step 2: Run the PG-focused test to verify it fails for the right reason**

Run: `cargo test -p studiofs-meta "pg_engine::tests::test_pg_remote_uploading_marker" -- --exact`

Expected: FAIL because the peer still sees `uploading.bin`.

- [ ] **Step 3: Write the failing temp-file-plus-rename visibility test**

Flip the pre-publish assertion in the nested rename test so the peer must not
see `final.bin` until publish:

```rust
assert!(wait_until(Duration::from_secs(12), || {
    let _ = b.sync_metadata();
    match b.lookup(ROOT_INO, b"drafts") {
        Ok(remote_dir) => matches!(
            b.lookup(remote_dir.ino, b"final.bin"),
            Err(MetaError::EntryNotFound { .. })
        ),
        Err(_) => false,
    }
}));
```

- [ ] **Step 4: Run the nested rename test to verify it fails**

Run: `cargo test -p studiofs-meta "pg_engine::tests::test_pg_nested_temp_rename_then_publish_visible_on_remote_peer" -- --exact`

Expected: FAIL because the peer still sees `final.bin` as a placeholder.

- [ ] **Step 5: Add a failing snapshot export regression test**

Add a redb snapshot test that creates a new unpublished regular file, exports a
remote snapshot, imports it into a peer engine, and asserts `lookup()` fails on
the peer until publish.

- [ ] **Step 6: Run the snapshot export regression test to verify it fails**

Run: `cargo test -p studiofs-meta "redb_engine::tests::new_unpublished_file_is_hidden_from_remote_snapshot" -- --exact`

Expected: FAIL because snapshot export still includes the unpublished file.

- [ ] **Step 7: Commit the red tests**

```bash
git add crates/studiofs-meta/src/pg_engine.rs crates/studiofs-meta/src/redb_engine/tests/payload_objects.rs
git commit -m "test: lock first-publish visibility before implementation (SFS-268)"
```

### Task 2: Hide unpublished new regular files in merged namespace reads

**Files:**
- Modify: `crates/studiofs-meta/src/redb_engine/namespace.rs`
- Modify: `crates/studiofs-meta/src/redb_engine/txn.rs`
- Test: `crates/studiofs-meta/src/pg_engine.rs`

- [ ] **Step 1: Add a helper that decides whether a merged regular-file dentry is peer-visible**

Introduce a focused helper in the redb merged namespace path that:

```rust
fn regular_file_dentry_visible(
    attr: &InodeAttr,
    has_local_overlay: bool,
) -> bool {
    !(
        attr.kind == FileType::RegularFile
            && attr.upload_state == UploadState::Uploading
            && attr.size == 0
            && attr.committed_size == 0
            && !has_local_overlay
    )
}
```

- [ ] **Step 2: Thread the visibility check into merged `lookup()`**

Update the merged namespace lookup path so a candidate dentry for a regular file
is rejected when `regular_file_dentry_visible()` returns `false`.

- [ ] **Step 3: Thread the visibility check into merged `readdir()`**

Apply the same filter while collecting merged directory entries so peers do not
list a name that later fails `lookup()`.

- [ ] **Step 4: Run the two PG tests from Task 1**

Run:

```bash
cargo test -p studiofs-meta "pg_engine::tests::test_pg_remote_uploading_marker" -- --exact
cargo test -p studiofs-meta "pg_engine::tests::test_pg_nested_temp_rename_then_publish_visible_on_remote_peer" -- --exact
```

Expected: PASS.

- [ ] **Step 5: Run an existing overlay-preservation regression**

Run: `cargo test -p studiofs-meta "pg_engine::tests::test_pg_overlay_allows_unrelated_remote_batches_to_apply" -- --exact`

Expected: PASS, proving the visibility gate did not clobber local overlay rules.

- [ ] **Step 6: Commit the merged-namespace change**

```bash
git add crates/studiofs-meta/src/redb_engine/namespace.rs crates/studiofs-meta/src/redb_engine/txn.rs crates/studiofs-meta/src/pg_engine.rs
git commit -m "fix: hide unpublished new regular files from peer namespace (SFS-268)"
```

### Task 3: Align remote snapshot export with the same visibility rule

**Files:**
- Modify: `crates/studiofs-meta/src/redb_engine/replication.rs`
- Modify: `crates/studiofs-meta/src/redb_engine/tests/payload_objects.rs`
- Test: `crates/studiofs-meta/src/redb_engine/tests/payload_objects.rs`

- [ ] **Step 1: Filter unpublished new regular files out of `export_remote_snapshot()`**

When exporting a remote snapshot:

- drop dentries for regular files whose attrs match the unpublished-first-version
  visibility rule;
- drop inode rows for those same files;
- drop any matching base/overlay chunk rows for those inodes from the exported
  snapshot payload.

- [ ] **Step 2: Keep directories and published files untouched**

Do not broaden the filter to directories, symlinks, xattrs, ACLs, or already
published regular files.

- [ ] **Step 3: Run the snapshot regression from Task 1**

Run: `cargo test -p studiofs-meta "redb_engine::tests::new_unpublished_file_is_hidden_from_remote_snapshot" -- --exact`

Expected: PASS.

- [ ] **Step 4: Re-run the published-update regression**

Run: `cargo test -p studiofs-meta "redb_engine::tests::redb_partial_update_does_not_expose_new_blocks_before_publish" -- --exact`

Expected: PASS.

- [ ] **Step 5: Commit the snapshot export fix**

```bash
git add crates/studiofs-meta/src/redb_engine/replication.rs crates/studiofs-meta/src/redb_engine/tests/payload_objects.rs
git commit -m "fix: keep remote snapshots aligned with first-publish visibility (SFS-268)"
```

### Task 4: Remove stale VFS placeholder semantics and update integration coverage

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs.rs`
- Modify: `crates/studiofs-vfs/src/vfs/namespace.rs`
- Modify: `crates/studiofs-vfs/tests/vfs_test.rs`
- Test: `crates/studiofs-vfs/tests/vfs_test.rs`

- [ ] **Step 1: Flip the VFS integration tests away from placeholder visibility**

Update integration tests so they assert:

- the peer cannot `lookup()` the new file before first publish;
- after first publish, the peer sees the final path and committed data;
- existing published-file updates still keep the previous committed version.

- [ ] **Step 2: Remove or narrow `remote_upload_placeholder_visible()`**

Delete the stale placeholder path or reduce it to a dead-simple helper that no
longer blocks `open/read` for peers once the namespace entry itself is hidden.

- [ ] **Step 3: Run the focused VFS visibility test**

Run: `cargo test -p studiofs-vfs remote_snapshot_reads_nested_file_after_temp_rename_publish -- --exact`

Expected: PASS.

- [ ] **Step 4: Run the existing committed-version regression**

Run: `cargo test -p studiofs-vfs remote_clients_keep_previous_committed_version_during_update -- --exact`

Expected: PASS.

- [ ] **Step 5: Commit the VFS cleanup**

```bash
git add crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/vfs/namespace.rs crates/studiofs-vfs/tests/vfs_test.rs
git commit -m "fix: remove peer placeholder reads before first publish (SFS-268)"
```

### Task 5: Final focused verification and handoff

**Files:**
- Modify: `docs/superpowers/specs/2026-04-19-sfs-268-first-publish-visibility-design.md`
- Modify: `docs/superpowers/plans/2026-04-19-sfs-268-first-publish-visibility.md`

- [ ] **Step 1: Run the focused meta verification batch**

Run:

```bash
cargo test -p studiofs-meta "pg_engine::tests::unit_remote_projection_preserves_overlay_publish_path" -- --exact
cargo test -p studiofs-meta "pg_engine::tests::unit_remote_projection_updates_nested_file_after_temp_rename_then_publish" -- --exact
cargo test -p studiofs-meta "redb_engine::tests::redb_partial_update_does_not_expose_new_blocks_before_publish" -- --exact
cargo test -p studiofs-meta "redb_engine::tests::zeroio_delete_before_publish_persists_tombstone_lifecycle" -- --exact
```

Expected: PASS.

- [ ] **Step 2: Run the focused VFS verification batch**

Run:

```bash
cargo test -p studiofs-vfs remote_snapshot_reads_nested_file_after_temp_rename_publish -- --exact
cargo test -p studiofs-vfs remote_clients_keep_previous_committed_version_during_update -- --exact
```

Expected: PASS.

- [ ] **Step 3: Inspect the final diff**

Run: `git status --short`

Expected: only the intended `studiofs-meta`, `studiofs-vfs`, and `docs/superpowers` files are modified.

- [ ] **Step 4: Ask the user to run the larger suites**

Share:

```powershell
cargo test -p studiofs-meta
cargo test -p studiofs-vfs --test vfs_test
```

Expected: user runs the broader suites outside this session because we do not run full test suites here.

- [ ] **Step 5: Commit the completed implementation**

```bash
git add docs/superpowers/specs/2026-04-19-sfs-268-first-publish-visibility-design.md docs/superpowers/plans/2026-04-19-sfs-268-first-publish-visibility.md crates/studiofs-meta crates/studiofs-vfs
git commit -m "fix: align first-publish peer visibility with canonical contract (SFS-268)"
```
