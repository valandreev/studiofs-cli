# Version the persisted MetaEvent wire format

This is a living ExecPlan for SFS-639. It follows `.agent/PLANS.md` and must be updated as implementation evidence changes the plan. The purpose is to make metadata event replay safe when the Rust `MetaEvent` enum evolves: existing local redb rows and PostgreSQL rows must continue to replay, while newly written rows must not depend on the public enum's ordinal position.

## Purpose / Big Picture

After this change, a StudioFS client can add, remove, or reorder `MetaEvent` variants in source without silently changing the meaning of already-persisted event rows. New local and PostgreSQL event rows will carry a version marker and an explicit stable event tag. Readers will accept the historical ordinal postcard payload as a compatibility format, but unknown versions, tags, event types, and trailing bytes will fail closed with an actionable serialization error.

The result is visible through the metadata crate tests: a raw redb row written by the normal append path starts with the new envelope, a historical `[1, 42]` fixture still decodes as `RemoveAttr { ino: 42 }`, PostgreSQL replay validates `event_type` against the payload, and malformed or unknown rows are rejected before application.

## Progress

- [x] (2026-08-05) Read SFS-639 from Linear and confirmed the issue is already `In Progress` on branch `feature/sfs-639-s14-version-the-persisted-metaevent-wire-format`.
- [x] (2026-08-05) Verified the checkout is clean at `a0cb93d9` and the current schema already has `event_log.event_type`; no schema migration is needed for this task.
- [x] (2026-08-05) Traced the current paths: redb append/read in `redb_engine/txn.rs` and `redb_engine/replication.rs`, PostgreSQL write/read in `pg_engine/metadata_io.rs`, `pg_engine/sql.rs`, and `pg_engine/remote_apply.rs`.
- [x] (2026-08-05) Added and ran `new_local_event_log_rows_use_versioned_wire_envelope`; it fails on the legacy raw row as expected, proving the regression test is real.
- [x] (2026-08-05) Implemented the private versioned codec with explicit tags, legacy dual-read, and strict payload validation.
- [x] (2026-08-05) Routed redb and PostgreSQL event-log writes and reads through the codec; PostgreSQL replay now validates `event_type`.
- [x] (2026-08-05) Added stable-tag/golden fixtures for all current event kinds, legacy/unknown/trailing/type rejection tests, local restart coverage, and a mixed new/legacy PostgreSQL replay fixture.
- [x] (2026-08-05) Documented the SFS-639 operational rollout gate and minimum reader version in `docs/architecture.md`.
- [x] (2026-08-05) Ran final metadata verification after review fixes: 389 passed, 0 failed, 68 ignored; `-D warnings` all-targets check, formatting, and `git diff --check` passed.
- [x] (2026-08-05) Loaded `STUDIOFS_TEST_PG_DSN` from the workspace `.env` through the existing test harness and ran `test_pg_conflicting_batch_does_not_block_later_foreign_batch`: 1 passed.
- [x] (2026-08-05) Applied review simplification: removed the duplicate PostgreSQL event-kind allowlist; direct `MetaEvent::kind()` comparison still rejects unknown and mismatched event types.
- [x] (2026-08-05) Addressed review findings: legacy rows now use a frozen ordinal-to-tag mapping instead of deserializing into the public enum, and all 17 event variants have independent legacy and V1 byte fixtures. A temporary tag mutation made the V1 golden test fail before the original tag was restored.
- [x] (2026-08-06) Applied the performance review fix: postcard now serializes the fixed header tuple and variant payload into one output buffer; the independent V1 fixtures remain byte-identical.
- [x] (2026-08-06) Applied the remaining valid review cleanups: PostgreSQL replay now borrows row text and payload slices instead of allocating per row, and the metadata encoder map uses the function directly. The local metadata suite and the live PostgreSQL replay test pass; one initial rerun was temporarily blocked by a network outage, then passed once connectivity returned.
- [x] (2026-08-06) Removed the eager `format!` context allocation from event payload decoding; error messages retain the tag-specific context while successful replay allocates no temporary context string. The partial-magic prefix check remains intentional because valid legacy ordinals cannot begin with `S`.
- [x] (2026-08-06) Removed `Serialize`/`Deserialize` from the public `MetaEvent` derive and migrated the remaining in-workspace direct postcard comparisons and fixtures to `encode_event`. PostgreSQL replay now validates the stable tag before decoding variant fields, for both V1 and frozen legacy rows; the metadata suite and live replay remain green.

## Surprises & Discoveries

- The issue description says PostgreSQL currently lacks `event_type`, but the checked-out schema and insert query already contain that column. The remaining defect is that foreign-row SELECT omits it and replay ignores it; the implementation will preserve the existing column and add validation rather than introduce a duplicate migration.
- `postcard::from_bytes` does not report unused input. The codec must use `postcard::take_from_bytes` and reject a non-empty remainder so a corrupted payload cannot be accepted as a valid prefix.
- The generic redb `encode`/`decode` helpers are used for many unrelated persisted types. They will remain unchanged; only event-log call sites will use the new event-specific codec.
- Deserializing legacy bytes directly into `MetaEvent` preserves the original ordinal dependency. The compatibility path must parse the historical ordinal separately and map it explicitly to the shared tagged payload decoder.

## Decision Log

- Decision: Keep the codec in `crates/studiofs-meta/src/events.rs` as private crate-visible helpers instead of adding a general serialization framework. Rationale: the format is specific to `MetaEvent`, and the fewest files preserve the existing domain boundary. Date/Author: 2026-08-05, Codex.
- Decision: Frame new rows as a small binary envelope: fixed magic bytes, one version byte, one explicit stable tag byte, then a postcard payload for the variant fields. Rationale: legacy postcard enum rows start with a small ordinal, so the magic distinguishes formats without heuristic fallback; the tag is independent of public enum order and the payload remains compact. Date/Author: 2026-08-05, Codex.
- Decision: Use the existing lowercase `MetaEvent::kind()` strings as PostgreSQL tags and require them to match the decoded event. Rationale: the strings are already persisted and populated by the current insert path, so this adds validation without changing the table or requiring a migration. Date/Author: 2026-08-05, Codex.
- Decision: Encode each event's fields explicitly rather than serializing `MetaEvent` inside the new envelope. Rationale: serializing the public enum again would retain the ordinal-evolution problem inside the payload. Date/Author: 2026-08-05, Codex.
- Decision: Treat the SFS-639 migration gate as an operational minimum-reader rule instead of adding a new persisted feature flag or schema migration. Rationale: the existing schema has no capability negotiation field, and introducing one would expand the issue; all new readers dual-read legacy rows, while the architecture docs require every shared-log reader to be upgraded before a new writer is enabled. Date/Author: 2026-08-05, Codex.
- Decision: Decode the historical postcard ordinal as `u32`, map only ordinals `0..=16` to the existing stable tags, and reject every other value. Rationale: this freezes legacy meaning without duplicating the full public enum or its payload decoder. Date/Author: 2026-08-05, Codex.
- Decision: Serialize the V1 header as a postcard tuple containing the fixed seven-byte magic, version, tag, and payload. Rationale: postcard emits the fixed byte array without a length prefix, preserving the established wire format while avoiding a separate payload allocation and copy. Date/Author: 2026-08-06, Codex.

## Outcomes & Retrospective

Implementation is complete for the local metadata and shared codec paths. The final diff is limited to the event codec, redb/PostgreSQL event-log call sites, focused tests, architecture documentation, and this plan. The metadata lib suite passes with 389 tests and 68 environment-gated PostgreSQL tests ignored, and the live mixed-row PostgreSQL replay test passes with the workspace `.env` configuration. The V1 envelope encoder no longer allocates and copies a separate payload buffer, while all independent byte fixtures remain unchanged.

## Context and Orientation

`crates/studiofs-meta/src/events.rs` defines the public `MetaEvent` enum and its stable human-readable `kind()` string. Historically, `crates/studiofs-meta/src/redb_engine/layout.rs::encode` and `decode` serialize that enum directly with postcard, so the first enum byte is an ordinal. `crates/studiofs-meta/src/redb_engine/txn.rs::RedbMetaEngine::append_event_in_txn` writes those bytes into the local `EVENT_LOG_TABLE`; `crates/studiofs-meta/src/redb_engine/replication.rs::RedbMetaEngine::read_events_range` reads them.

The PostgreSQL path is in `crates/studiofs-meta/src/pg_engine/metadata_io.rs`: `push_projected_event_log_batch_locked` writes one `event_type` string and one `event_payload` bytea per event. `crates/studiofs-meta/src/pg_engine/sql.rs::SELECT_FOREIGN_EVENT_ROWS` currently selects only the payload and client fields, and `crates/studiofs-meta/src/pg_engine/remote_apply.rs::decode_foreign_batch` decodes the payload directly into the current enum. The test schema in `crates/studiofs-meta/src/pg_engine/tests/mod.rs` and the runtime initialization schema already define `event_type TEXT NOT NULL`.

The compatibility reader must accept the historical postcard representation, including fixtures such as `[1, 42]` for `RemoveAttr { ino: 42 }`. New payloads must use explicit field payloads, not a serialized public `MetaEvent`, so public enum reordering cannot alter their meaning. All three supported OS targets consume this same metadata crate format; no platform-specific encoding is allowed.

## Plan of Work

First add a red test in `crates/studiofs-meta/src/redb_engine/tests.rs` that calls the existing append path for `MetaEvent::RemoveAttr { ino: 42 }`, reads the raw event-log row, and asserts the new magic prefix. Run only the metadata test filter and confirm the assertion fails because the current row is the legacy ordinal payload.

Then add crate-visible `encode_event` and `decode_event` helpers in `events.rs`. The encoder will map every current variant to a fixed tag and serialize only its fields into postcard. The decoder will recognize the magic envelope, reject short headers, unsupported versions, unknown tags, and trailing payload bytes, then reconstruct the public enum. Bytes without the magic will decode through a frozen historical ordinal-to-tag mapping and the same tagged payload decoder, never through the evolving public enum. Add a helper that validates a PostgreSQL `event_type` string and compares it with the decoded event's stable `kind()`.

Route only event-log persistence through these helpers: replace `encode(event)` in `append_event_in_txn`, replace the direct PostgreSQL postcard encoder in `push_projected_event_log_batch_locked`, and include `event_type` in `SELECT_FOREIGN_EVENT_ROWS`. Update `decode_foreign_batch` to read the shifted columns, reject unknown or mismatched event types, and decode both legacy and new payloads through the shared helper. Keep dump/load byte preservation unchanged; it copies event rows and does not need to reinterpret them.

Finally add behavior-focused tests. Cover every current event kind with a hand-constructed fixture and stable tag assertion, retain the existing legacy redb fixture, add new-envelope and legacy PostgreSQL replay fixtures, and assert unknown version/tag/type, mismatched type, and trailing bytes fail closed. Add a local restart/read-range test that writes a new row, drops the engine, reopens it, and decodes the row. Use the existing real PostgreSQL test harness for replay coverage when its environment is available; otherwise report the unavailable matrix cell rather than replacing it with a mock-only claim.

## Concrete Steps

Run all commands from `/Volumes/Storage/dev/studiofs`.

1. Confirm the branch and clean baseline:

        git status --short --branch
        git log -1 --oneline

2. After each red/green step, run the smallest relevant command. The initial red proof is:

        cargo test -p studiofs-meta new_local_event_log_rows_use_versioned_wire_envelope -- --nocapture

   Before the implementation this must fail at the new assertion; after the encoder is wired it must pass.

3. Run focused metadata tests one filter at a time to avoid workspace file-lock contention, followed by:

        cargo test -p studiofs-meta --lib
        RUSTFLAGS='-D warnings' cargo check -p studiofs-meta --all-targets
        cargo fmt --all -- --check
        git diff --check

4. If PostgreSQL integration tests require an unavailable service, run the non-PostgreSQL metadata tests and record the exact blocked command and environment reason in the final handoff and this plan.

## Validation and Acceptance

Acceptance requires all of the following observable behavior:

- The normal redb append path writes the new magic/version/tag envelope, and a fresh engine restart reads it back as the same event.
- Independent byte fixtures for all historical ordinals replay as their original events, including `[1, 42]` as `RemoveAttr { ino: 42 }`; unknown ordinals fail closed.
- Every current event variant round-trips through the new codec with a stable explicit tag whose value is not derived from the public enum ordinal.
- PostgreSQL writes the new payload and its existing `event_type`; foreign replay selects and validates both. A legacy payload with the correct type still replays.
- Unknown envelope versions, unknown tags, unknown PostgreSQL event types, mismatched type/payload pairs, truncated headers, and trailing bytes return `MetaError::Serialization` with an actionable message and do not apply an event.
- The focused metadata tests, metadata crate check with warnings denied, formatting check, and whitespace check pass. Cross-platform behavior uses the shared codec; if Windows/macOS-specific execution is unavailable in this environment, that limitation is stated explicitly.

## Idempotence and Recovery

The change is additive for stored data: readers retain the old ordinal decoder and writers switch only future rows to the new envelope. No destructive migration or data rewrite is required. If a test or compile step fails, leave the working tree intact, fix the smallest failing behavior, and rerun that single filter. Do not reset or overwrite unrelated user changes.

## Artifacts and Notes

The primary implementation artifacts are the event codec in `crates/studiofs-meta/src/events.rs`, event-log call-site changes in the redb and PostgreSQL modules named above, focused regression tests, and this living plan. The final handoff should link the changed files and include the exact test results rather than claiming an unavailable mounted or cross-platform scenario passed.

## Interfaces and Dependencies

The final crate-local interface is:

    pub(crate) fn encode_event(event: &MetaEvent) -> MetaResult<Vec<u8>>
    pub(crate) fn decode_event(bytes: &[u8]) -> MetaResult<MetaEvent>
    pub(crate) fn decode_event_with_type(event_type: &str, bytes: &[u8]) -> MetaResult<MetaEvent>

These helpers use the already-installed `postcard`, `serde`, and `thiserror` dependencies. They do not change the public `MetaEvent` shape, the generic redb codec, the PostgreSQL table schema, or any platform bridge.

Plan revision note (2026-08-05/06): created after inspecting the live issue and current HEAD. The plan explicitly records that `event_type` already exists and narrows the work to shared versioned payload encoding, dual-read compatibility, and validation. Updated after review to freeze the legacy ordinal mapping, replace self-roundtrip coverage with independent V0/V1 byte fixtures, and serialize the V1 envelope in one allocation.
