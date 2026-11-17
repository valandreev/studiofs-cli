# SFS-1056: Update shared S3 volume settings

## Purpose / Big Picture

`studiofs umeta --meta <dsn> <disk-name>` or `studiofs umeta --disk <disk-name>` lets an operator rotate or correct the shared S3 endpoint, bucket, access key, secret key, region, and trash retention without recreating the volume. Exactly one connection source is required; `--disk` reads the DSN from local metadata. The command updates PostgreSQL as one transaction while preserving volume identity, encryption, block size, metadata configuration, and all fields not named on the command line. A successful background metadata poll refreshes the local S3 fields. Upgraded live and snapshot mounts apply a changed key pair after that poll; endpoint, bucket, and region still require remounting.

## Context and Orientation

The `studiofs` CLI is in `crates/studiofs/src/cli`; `FormatInfo` is stored locally in `redb` and shared as `metadata_state.format_info`. `studiofs-meta` owns transactional PostgreSQL settings updates in `crates/studiofs-meta/src/pg_engine/volume_settings.rs`. Checkpoint publication also writes `format_info`, so it must preserve newer shared S3 settings instead of restoring a stale local copy. The new command must use the existing `trash-days` revision/event semantics and never persist PostgreSQL credentials in the volume format.

## Plan of Work

Add a small `umeta` CLI module with exactly one required `--meta`/`--disk` connection source, conditional positional name, and optional S3 fields plus `--trash-days`; reject invocations with no updates. Implement one PostgreSQL transaction that row-locks and decodes current shared `FormatInfo`, applies only supplied values, updates optional trash retention through the existing ordered event/CAS mechanism, and writes the resulting format bytes. The row lock prevents concurrent CLI updates from losing fields. Keep credentials and DSNs out of `Debug`, logs, errors, and confirmation output.

Trace how an existing local database opens against PostgreSQL. Refresh its mutable S3 configuration from the authoritative shared format before the next mount builds `DataStoreConfig`. Ensure checkpoint publication cannot overwrite a newer shared configuration with the old local copy, using the transaction's authoritative row as the merge source. Add focused tests for parser/redaction, requested-field preservation and immutable-field preservation, and existing-cache reconnect refresh. Add brief English/Ukrainian user-guide instructions in chapter 27 and a pointer in chapter 26.

## Progress

- [x] Confirmed clean worktree, branch `feature/sfs-1056-023-rotate-s3-credentials-without`, applicable contracts, graph project, and source coverage.
- [x] Traced `init`, shared `trash-days` CAS/event transaction, shared `metadata_state.format_info`, and checkpoint publication source.
- [x] Implemented the atomic metadata helper, checkpoint-format preservation, reconnect refresh, and CLI surface including `--disk`.
- [x] Added `--meta` name lookup without requiring a local cache, plus a shared local-format helper used by `trash-days` and `umeta`.
- [x] Finalized the partial-update/background-refresh PostgreSQL regression; its ignored live-PostgreSQL execution is pending rerun after review fixes.
- [x] Documented the command and upgrade-first/deferred-pickup rule in both languages.
- [x] Ran focused CLI/meta tests, PostgreSQL acceptance, package check/clippy, formatting, and diff hygiene after review fixes.

## Surprises & Discoveries

- The existing `set_shared_trash_days` starts and commits its own transaction. Combining it directly with `format_info` would allow partial success, so its transaction body must be factored to accept a caller-owned PostgreSQL transaction.
- Checkpoint publication serializes local `FormatInfo` and writes it to PostgreSQL. An operator update therefore needs a merge at the final locked publication point, not just a refresh on reconnect.

## Decision Log

- Decision: Preserve the existing metadata event/version semantics for trash changes and update format bytes in that same transaction. Rationale: peers already converge through the ordered settings event, while S3 volume configuration remains an authoritative bootstrap field. Date/Author: 2026-11-17, Codex.
- Decision: Keep immutable identity, encryption, format, and block size fields untouched by applying a partial S3 update to the locked current shared format. Rationale: the CLI offers no identity or encryption rotation operation and must not imply that changing S3 credentials re-encrypts or moves objects. Date/Author: 2026-11-17, Codex.
- Decision: Refresh the five shared S3 fields from the existing successful `pg-meta-poll` cycle, never synchronously during deferred mount open. Rationale: local-first mount admission remains PostgreSQL-independent. Endpoint, bucket, and region remain fixed in the running S3 client and require remounting; access and secret keys are swapped in place after the next successful poll. Date/Author: 2026-11-17, Codex.

## Concrete Steps

From `E:\dev\studiofs-cli`, add the CLI command and transactional helper in the existing modules, then add focused tests and localized guidance. Run only the requested package-scoped `umeta` and `meta` tests, package check/clippy for touched Rust crates, formatting, and diff validation; never run a full workspace suite.

## Validation and Acceptance

CLI parsing accepts exactly one of `--meta` with a positional volume name or `--disk` with an optional matching positional name; it rejects no connection, both connections, no update, and conflicting names. It accepts one or more of the six mutable fields and prints field names without values. The database test verifies that requested S3 fields and optional trash days change atomically, an access-key-only update preserves every other serialized format field, and immutable `FormatInfo` values are unchanged. The deferred-open regression proves that mount admission does not synchronously read shared S3 settings; the focused PostgreSQL regression verifies the shared-format loader, pooled background refresh, and stale-checkpoint protection.

## Idempotence and Recovery

The update is safe to retry: the caller reads/locks current shared state and applies the supplied fields to that state. The transaction commits either all format/trash changes or none. A CAS conflict is reported without printing credentials. If a transaction result is ambiguous, rerunning the same requested values converges on those values while preserving unrelated updates.

## Artifacts and Notes

Temporary command logs belong under `target/sfs1056-gates/`. Do not place DSNs, credentials, or `.env` contents in the plan, logs, or final report. Do not change the metadata format version or add local redb tables/bindings.

## Interfaces and Dependencies

Use existing `clap`, `tokio-postgres`, `serde_json`, and `FormatInfo`/`VolumeSettings` APIs for the shared settings command. The rotating loader uses the workspace's already-resolved OpenDAL 0.55, reqsign 0.16.5, reqwest, anyhow, and async-trait crates; reqsign and async-trait are declared directly because the custom loader implements their public API. The CLI delegates shared metadata to `studiofs-meta`; mounted foreground metadata operations still use local `redb`.

## Outcomes & Retrospective

The shared update helper locks the current metadata row, applies only explicitly supplied S3 fields, and invokes the existing trash-days event writer inside the same transaction when retention is present. Checkpoint upserts preserve non-empty shared `format_info`. Successful background metadata polls refresh only changed S3 fields in local `redb`; the standalone janitor and snapshot CLI load the same UUID-checked authoritative shared format before setting up S3. Deferred mount open remains free of shared-format reads. Every client binary must be upgraded before `umeta`, because an older mounted client can publish stale format settings at a checkpoint. No workspace-wide suite, commit, or PR was created.

## SFS-1056 Hot Credential Rotation Extension

The mount now creates one shared `S3CredentialHolder` for its access/secret pair and attaches it to the mount `DataStoreConfig`. Read and write profile clones preserve that same holder. A background PG poll refreshes shared S3 fields only after the shared volume UUID/name have been checked and the local `redb` format update succeeds; an observer then swaps the access/secret pair together. The observer is registered with the current local pair while holding its synchronization lock, which closes the race where a poll could finish between registration's initial read and callback installation. Endpoint, bucket, and region remain fixed in the mounted S3 client and therefore require remounting. Live and snapshot mounts use rotation; standalone snapshot and janitor DataStores keep static credentials.

The refresh uses the engine's cancellable PostgreSQL command pool instead of opening a connection on every poll. It warns on the first refresh failure in a streak and logs subsequent failures at debug level until a poll succeeds. Snapshot CLI refresh preserves a mounted session's endpoint override while taking bucket, credentials, and region from shared metadata.

OpenDAL 0.55 invokes the custom reqsign credential loader at signing time and has no credential cache of its own. The loader returns the current atomic access/secret pair with no expiry; every signing request therefore observes the latest pair without rebuilding the DataStore. The custom loader is the only credential source for this operator, so environment, shared AWS profile, and IMDS fallback are disabled.

The focused mock HTTP regression sends two S3 HEAD requests through one DataStore, swaps the holder between them, and checks that the Authorization credential identifier changes while the store remains alive. The ignored PostgreSQL regression also registers an observer, checks its initial local pair, refreshes from shared PG state, then checks that the pair delivered after local `redb` refresh matches the changed credentials. Secret values are asserted absent from the DataStore configuration Debug output and are not logged.

### Progress

- [x] Added the shared atomic credential holder and custom OpenDAL S3 loader.
- [x] Wired successful background shared-format refresh to update the mount holder, with registration race closure.
- [x] Added live mock-HTTP same-DataStore authorization rotation coverage and expanded the ignored PG refresh test.
- [x] Updated English and Ukrainian chapters 26/27 to describe upgrade-first ordering, pickup after the next successful poll (2 seconds by default, configurable with `STUDIOFS_PG_SYNC_POLL_SECS`), old-key overlap, and remount-only endpoint/bucket/region changes.
- [x] Replaced the review-identified nonfunctional loader expiry/cache with an unexpired current-pair result on each signing request, and removed `chrono` from `studiofs-data`.
- [x] Used the pooled cancellable metadata path with one warning per refresh failure streak; preserved mounted snapshot endpoint overrides and added a focused regression.

### Extension Validation Results

Focused Windows validation after review 2 is retained under `target/sfs1056-gates/r2-*.log`: `cargo test -p studiofs umeta` (5 passed); `cargo test -p studiofs-data --lib credential` (3 passed); exact signer and current-pair loader tests (1 each); snapshot endpoint regression (1); deferred mount startup (1); metadata volume settings (8 passed, 1 ignored); exact ignored PostgreSQL refresh regression (1 passed); `prepare_for_close_cancels_inflight_remote_step` (1); package `cargo check` and Clippy passed; formatting and `git diff --check` passed. The WSL `cargo check -p studiofs -p studiofs-linux-fuse` also passed against this feature branch; its log is `target/sfs1056-gates/r2-wsl-check.log`. The ignored PostgreSQL integration exercises current-pair observer registration and delivery after pooled shared-settings refresh. The pooled query path is established by source inspection and the live PostgreSQL regression, but no direct connection-count instrumentation was added. This does not establish a live mount acceptance test; no FUSE/WinFSP bridge code changed.

### Decision Log

- Superseded decision: The initial loader used a 150-second expiry and inferred an effective 30-second refresh window from reqsign's 120-second validity buffer. Review confirmed the custom-loader path does not call `is_valid()`, so the cache was ineffective. Current decision: return the holder's latest pair with no expiry on each signing request; key changes apply on the next request after a successful metadata poll. Date/Author: 2026-11-17, Codex.
- Decision: Keep an atomic pair lock and install a process-local observer on `MetaEngine`. Rationale: requests cannot mix an old access key with a new secret, and the observer bridge lets metadata publish updates without depending on the data crate. Registration replays local settings while holding the same observer lock used after each PG-driven redb write. Date/Author: 2026-11-17, Codex.
