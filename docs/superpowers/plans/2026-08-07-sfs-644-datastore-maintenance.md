# SFS-644 DataStore Maintenance Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the raw `DataStore::operator` escape hatch, give the VFS janitor a streaming provider-neutral listing API, validate delete keys at the data boundary, and make WinFSP's direct `studiofs-data` edge test-only.

**Architecture:** `studiofs-data` owns the OpenDAL listing and maps each file to `DataStoreObjectEntry { key, last_modified }`. `studiofs-vfs` keeps the metadata/age/orphan decision and uses only `list_object_entries` plus the existing validated `delete_by_key`; no generic repository abstraction is added. Test-only ciphertext inspection constructs a separate OpenDAL operator in the VFS integration test.

**Tech Stack:** Rust 2024, Tokio, futures streams, Apache OpenDAL 0.55, Cargo metadata, focused Cargo tests and Clippy.

---

This living plan follows `.agent/PLANS.md`. The repository is `E:\dev\studiofs-cli` on branch `feature/sfs-644-s33-narrow-datastore-maintenance-operations-and-remove-the`. Full workspace test suites are intentionally excluded; run focused package checks only.

## Purpose / Big Picture

The janitor currently reaches through `DataStore::operator()` to list and delete provider objects. That lets VFS bypass data-layer validation and couples the worker to OpenDAL. After implementation, the janitor still streams the same file entries and preserves its orphan/age/delete counters, but the only provider calls are inside `studiofs-data`. A delete key outside the configured volume or containing path traversal segments fails locally as `InvalidObjectKey`.

## Progress

- [x] (2026-08-07 10:50 +03:00) Read SFS-644, Linear comments, repository instructions, and current branch state.
- [x] (2026-08-07 10:50 +03:00) Traced all `DataStore::operator` and `delete_by_key` callers with codebase-memory.
- [x] (2026-08-07 10:50 +03:00) Confirmed current WinFSP direct dependency is normal in Cargo metadata and `studiofs-vfs` remains the runtime transitive owner.
- [x] (2026-08-07 10:50 +03:00) Design accepted by the user; spec saved at `docs/superpowers/specs/2026-08-07-sfs-644-datastore-maintenance-design.md`.
- [x] (2026-08-07 10:50 +03:00) Implementation plan saved; implementation commits remain.
- [x] (2026-08-07) Add failing DataStore tests for listing and invalid delete keys; the expected compile failures were observed before implementation.
- [x] (2026-08-07) Implement the provider-neutral listing stream, error variant, key validation, and remove `operator`.
- [x] (2026-08-07) Migrate VFS workers and tests; move the WinFSP dependency.
- [x] (2026-08-07) Run focused tests/checks, formatting, diff checks, and metadata verification.
- [x] (2026-08-07) Update this plan with evidence and commit the implementation.

## Surprises & Discoveries

- Observation: `operator()` has one production caller in `background_janitor` and two VFS integration-test call sites; `background_gc` already uses `delete_by_key`.
  Evidence: codebase-memory inbound trace and `rg -n "\.operator\(\)" crates`.
- Observation: `studiofs-winfsp` still reaches `studiofs-data` transitively through `studiofs-vfs`.
  Evidence: `cargo tree -p studiofs-winfsp --edges normal` lists both the direct edge and `studiofs-vfs -> studiofs-data`.
- Observation: OpenDAL's asynchronous lister is streaming and stops after yielding a provider error.
  Evidence: OpenDAL 0.55 source and current worker behavior; the new wrapper will preserve streaming and map the error to `DataError::OpenDal`.
- Observation: The provider-neutral stream is not `Unpin` because it owns an async `unfold` state machine.
  Evidence: Focused compilation required `Box::pin` at the DataStore test and janitor call sites; no provider type leaked through the API.

## Decision Log

- Decision: Return a provider-neutral stream rather than `Vec`.
  Rationale: The existing janitor deliberately streams potentially large prefixes; collecting all entries would introduce an unnecessary memory ceiling.
  Date/Author: 2026-08-07 / Codex.
- Decision: Do not add a new delete service or repository abstraction.
  Rationale: `delete_by_key` is already the one delete method used by background GC and integration fixtures; validating it once is the smallest shared fix.
  Date/Author: 2026-08-07 / Codex.
- Decision: Keep ciphertext inspection below the DataStore API in the VFS test.
  Rationale: The encrypted test must read and tamper with raw ciphertext; that is test instrumentation, not a runtime storage capability.
  Date/Author: 2026-08-07 / Codex.

## Outcomes & Retrospective

The janitor's raw listing and fragment deletes now use `list_object_entries` and validated `delete_by_key`. VFS encryption tests use a test-local raw OpenDAL operator only for ciphertext inspection, and the orphan fixture uses `put_by_key`. `studiofs-winfsp` now has a dev-only direct `studiofs-data` dependency; `studiofs-data` remains a runtime transitive dependency through `studiofs-vfs` as intended.

Evidence:

- `cargo test -p studiofs-data --lib`: 49 passed.
- `cargo check -p studiofs-vfs --test vfs_test`: passed.
- `cargo check -p studiofs-winfsp --tests`: passed.
- `cargo clippy -p studiofs-data -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings`: passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.
- Cargo metadata reports `studiofs-data` for `studiofs-winfsp` with `kind dev`.
- `rg -n "\.operator\(\)|pub fn operator" crates Cargo.toml`: no matches.

Ignored live S3 tests and the full workspace suite were not run; the repository requires live `STUDIOFS_S3_*` infrastructure for the former, and full suites are outside this focused validation.

## Context and Orientation

`crates/studiofs-data/src/store.rs` owns the `DataStore` and its private OpenDAL `Operator`. It already exposes encrypted `put_by_key`, `get_by_key`, and `delete_by_key` operations plus `DataError`. `crates/studiofs-vfs/src/vfs/workers.rs` owns `background_janitor` and `background_gc`: metadata determines whether an object is referenced and old enough, while the data store performs provider I/O. `crates/studiofs-vfs/tests/vfs_test.rs` contains ignored live-S3 janitor and encryption tests. `crates/studiofs-winfsp/Cargo.toml` currently lists `studiofs-data` under normal dependencies even though its references are in tests and test-only context code.

The listing entry is intentionally not called “stale object”: staleness depends on metadata references and timestamps assembled by VFS, not on the storage provider alone. The data-layer type only represents a file returned by the provider.

## Plan of Work

### Task 1: Add red tests for the DataStore boundary

Files:

- Modify `crates/studiofs-data/src/store.rs` test module near the existing memory-backed tests.

Add one async test that writes two keys below `unit-volume/objects/`, calls `list_object_entries("unit-volume/objects/")`, consumes the stream with `futures::StreamExt::next`, and asserts the two returned keys. Add one async test that calls `delete_by_key` with `other-volume/object` and `unit-volume/../other/object`, then asserts each error is `DataError::InvalidObjectKey`. The tests must be written before production changes and use the existing `Operator::new(Memory::default())` fixture.

Run from `E:\dev\studiofs-cli`:

    cargo test -p studiofs-data list_object_entries --lib

Expected result before implementation: compilation fails because `list_object_entries` and `DataError::InvalidObjectKey` do not exist. If the test instead passes or fails for a setup typo, correct the test before adding production code.

### Task 2: Implement the narrow DataStore API

Files:

- Modify `crates/studiofs-data/Cargo.toml` to add `futures.workspace = true` under `[dependencies]`.
- Modify `crates/studiofs-data/src/store.rs` imports, `DataError`, and the `DataStore` methods around lines 314 and 639-679.

Add this public data-layer value and accessors:

    #[derive(Debug, Clone)]
    pub struct DataStoreObjectEntry {
        pub key: String,
        pub last_modified: Option<SystemTime>,
    }

Add `DataError::InvalidObjectKey(String)` with a non-retryable error message. Add a private validator that requires a non-empty key under `format!("{}/", self.volume_name)`, rejects backslashes, and rejects `.` or `..` path segments. Use it in `list_object_entries(prefix)` before calling OpenDAL and in `delete_by_key(key)` before deletion.

Implement the listing signature:

    pub async fn list_object_entries(
        &self,
        prefix: &str,
    ) -> DataResult<impl futures::Stream<Item = DataResult<DataStoreObjectEntry>>>

Create the OpenDAL lister with `self.op.lister_with(prefix).recursive(true).await?`. Wrap it with `futures::stream::unfold`; use `StreamExt::next` to skip non-file entries, return each file's path and `metadata().last_modified().map(Into::into)`, and map each provider error with `DataError::from`. Do not expose `opendal::Lister` or `opendal::Entry` in the signature. Remove the public `operator` method. Route `delete_object` through the validated `delete_by_key` method so object deletes receive the same scope check.

Run:

    cargo test -p studiofs-data list_object_entries --lib
    cargo test -p studiofs-data delete_by_key_rejects --lib

Expected result: both new tests pass, and the existing `studiofs-data` library tests remain green.

### Task 3: Migrate the VFS worker

Files:

- Modify `crates/studiofs-vfs/src/vfs/workers.rs` in `StudioVfs::background_janitor` around the listing loop.

Replace the raw chain:

    self.data_write.operator().lister_with(&self.data_prefix).recursive(true).await

with `self.data_write.list_object_entries(&self.data_prefix).await`. Keep the existing lock-release/error logging paths. In the loop, use `entry.key` and `entry.last_modified` in place of OpenDAL entry methods. Keep the VFS `parse_object_id_from_key` filter, reference checks, age logic, delete cap, counters, and warning text unchanged. Replace the raw `operator().delete(&key)` call with `self.data_write.delete_by_key(&key).await`; keep `NotFound` handling by matching `DataError::OpenDal(error)` and `ErrorKind::NotFound`.

Run:

    cargo check -p studiofs-vfs --test vfs_test

Expected result: the worker compiles without any `DataStore::operator` call. `background_gc` continues to use the now-validated `delete_by_key`.

### Task 4: Remove test-only raw DataStore operator calls

Files:

- Modify `crates/studiofs-vfs/tests/vfs_test.rs` imports and the ignored tests around `encrypted_remote_object_storage_keeps_remote_ciphertext_and_local_plaintext_cache` and `test_background_janitor_removes_orphan_object`.

Replace the orphan setup's `ds.operator().write` with `ds.put_by_key`, preserving the existing skip behavior for an unavailable bucket. For the encryption test, add a small test-only helper that builds an OpenDAL S3 operator from `DataStoreConfig::from(format_info)`; use that independently constructed operator only to read and overwrite ciphertext. Replace every `data_store.operator()` call with the helper's raw operator. This keeps ciphertext tampering coverage while leaving no caller of `DataStore::operator`.

Run:

    cargo check -p studiofs-vfs --test vfs_test

Expected result: the integration test target compiles and `rg -n "\.operator\(\)" crates/studiofs-vfs` returns no matches.

### Task 5: Move the WinFSP direct dependency

Files:

- Modify `crates/studiofs-winfsp/Cargo.toml` by removing `studiofs-data.workspace = true` from `[dependencies]` and adding it under `[dev-dependencies]`.

Run:

    cargo metadata --no-deps --format-version 1 | ConvertFrom-Json | ForEach-Object { $_.packages | Where-Object name -eq 'studiofs-winfsp' } | ForEach-Object { $_.dependencies | Where-Object name -eq 'studiofs-data' | Select-Object name,kind,target }
    cargo check -p studiofs-winfsp

Expected metadata output has `name studiofs-data` and `kind dev`. The package may still appear transitively through `studiofs-vfs`; that is expected because VFS owns the runtime data path.

### Task 6: Focused verification and handoff

Run from `E:\dev\studiofs-cli`:

    cargo fmt --all -- --check
    cargo test -p studiofs-data --lib
    cargo check -p studiofs-winfsp --tests
    cargo clippy -p studiofs-data -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings
    cargo metadata --no-deps --format-version 1 | ConvertFrom-Json | ...
    rg -n "\.operator\(\)|pub fn operator" crates Cargo.toml
    git diff --check

The operator search must return no DataStore operator surface or VFS call. Full workspace test suites are not run. Live ignored S3 janitor tests may be run only when the required `STUDIOFS_S3_*` environment is present; otherwise report them as unavailable rather than as a pass.

Update `Progress`, `Surprises & Discoveries`, `Outcomes & Retrospective`, and the bottom revision note with actual command evidence. Commit all implementation changes with an English message such as `refactor: narrow datastore maintenance operations`.

## Validation and Acceptance

The data crate unit tests pass and prove both listing and invalid-key rejection. `studiofs-vfs` compiles with the janitor using only `DataStore` APIs, and no `DataStore::operator` definition or call remains. The WinFSP package's direct Cargo dependency is dev-only. Provider errors remain `DataError::OpenDal`, invalid keys are `DataError::InvalidObjectKey`, and no bridge-specific errno mapping is introduced.

## Idempotence and Recovery

All edits are additive until the old operator method and calls are removed. If a focused test fails, keep the failing test and correct production code; do not relax the key validator or change janitor timing. If Cargo metadata or checks are blocked by a locked Windows target directory, rerun with an isolated target directory under `target/` and report the exact command. Do not reset or discard unrelated work.

## Artifacts and Notes

The design decision is recorded in `docs/superpowers/specs/2026-08-07-sfs-644-datastore-maintenance-design.md`. The focused evidence will include the DataStore test result, Cargo metadata direct dependency row, operator search, and `git diff --check` output.

## Interfaces and Dependencies

At completion, `crates/studiofs-data/src/store.rs` must provide:

    pub struct DataStoreObjectEntry {
        pub key: String,
        pub last_modified: Option<std::time::SystemTime>,
    }

    impl DataStore {
        pub async fn list_object_entries(
            &self,
            prefix: &str,
        ) -> DataResult<impl futures::Stream<Item = DataResult<DataStoreObjectEntry>>>;

        pub async fn delete_by_key(&self, key: &str) -> DataResult<()>;
    }

`DataError::InvalidObjectKey(String)` must be non-retryable. `opendal::Operator` remains a private DataStore implementation detail; OpenDAL's lister and entry types do not appear in the public maintenance API.

## Revision Note

Initial plan created after user approval of the SFS-644 design on 2026-08-07. Implementation completed on 2026-08-07 in the current feature branch. The plan deliberately keeps `delete_by_key` as the one existing delete API and adds only the provider-neutral listing operation.
