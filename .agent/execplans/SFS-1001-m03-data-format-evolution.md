# Milestone 03: Data Format Evolution

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan is maintained in accordance with `.agent/PLANS.md` at the repository root. It depends on milestones 01 and 02 (`.agent/execplans/SFS-1001-m01-contracts-baseline.md`, `.agent/execplans/SFS-1001-m02-pg-compat-foundation.md`).

## Purpose / Big Picture

After this milestone, every data format a successor might change has its own version contract: metadata events, serving checkpoints, the local redb metadata database and the local payload index. A 0.2.5 binary that meets a newer event, checkpoint or local format refuses with a typed reason and keeps all local unpublished work. Existing 0.2.x volumes (format 5 from 0.2.0–0.2.2, format 6 from 0.2.3–0.2.4) can be adopted into the 0.2.5 baseline with `studiofs upgrade`, which chains them to format 7 and fences older writers. Local migrations survive a crash at any point.

## Progress

- [x] (2026-11-25) SFS-1009: register baseline and test-successor MetaEvent codecs, refuse unsupported events before replay advances, and guard settings rewrites.
- [x] (2026-11-26) SFS-1010: publication negotiates each event encoding against the stamped volume policy and refuses, keeping the journal, when a reader may not decode it; replay keeps decoding the whole page before the redb apply. A refusal is a sticky `PublicationBlocked` sync failure, warned once with the event kind and capability, that survives successful polls, does not invalidate remote identity validation, and clears on the next successful publication.
- [x] (2026-11-26) SFS-1011: publish compatible checkpoint encodings at the same cut and retain a baseline prune anchor. [Implementation and focused validation](SFS-1011-checkpoint-formats.md).
- [ ] SFS-1012: crash-safe local redb and payload-index format migration and recovery.
- [ ] SFS-1013: version-aware discovery, snapshot mounts and dump/load restore.
- [x] (2026-11-26) SFS-1132: implement format 5→6→7 and 6→7 in `studiofs upgrade`. Apply registered migrations, retain the intermediate format-6 commit, and atomically install the policy and format 7. Upgrade the existing payload index before activating the staged local header.
- [x] (2026-11-26) Verify SFS-1132 with pinned fixtures, stage interruptions, PostgreSQL and RustFS. Test the CLI retry after shared adoption and verify that load refuses an existing format-6 target.

## Surprises & Discoveries

- Observation: the payload index reports an unknown entry encoding as `Corruption`, the same class used for real damage.
  Evidence: `crates/studiofs-cache/src/payload_index.rs:1790`.
- Observation: the stored settings JSON previously accepted unknown fields, including fields nested in the trash and compression policies. Shared and local compare-and-set operations could drop those fields.
  Evidence: the settings compare-and-set regression in `crates/studiofs-meta/src/redb_engine/volume_settings.rs` verifies byte and journal preservation.
- Observation: `load` can replace a PostgreSQL metadata row for a matching volume UUID. Validate the existing row's format under the schema lock before replacing it, so a format-7 dump cannot implicitly adopt a format-5 or format-6 volume.

## Decision Log

- Decision: the codec registry records, per tag, the read capability every active reader must declare; publication emits the oldest faithful tag only when that capability is in the policy's `read` set, otherwise it returns `VolumeProtocolRejected(EventNotRepresentable)`.
  Rationale: the `read` set is what every admitted reader is guaranteed to hold; a `compatible` feature is optional, so a reader may lack it. The policy row decides, so publication never scans event history. All tags the encoders emit share `event.codec.v1`, so the oldest faithful tag (19, 20 or 21 for settings) is the only candidate; a successor tag with its own capability must extend `negotiate_wire_tag` to try faithful tags oldest-first.
  Date/Author: 2026-11-26, SFS-1010.
- Decision: the session lease carries the policy it validated, and the publication stamp is stale unless the stamped epoch equals that policy's epoch. Negotiation runs inside the publication transaction right after the stamp.
  Rationale: a concurrent heartbeat can stamp a newer epoch than the lease's policy; the existing one-retry force refresh then renegotiates. Local events stay `MetaEvent` values until publication, so events authored before a policy change are re-encoded under the current policy, which is a translation only where the encoding is identical. A refusal takes the generic flush error path and restores the pending batch. Atomic trigger fencing stays with SFS-1014 and mount lifecycle states with SFS-1015.
  Date/Author: 2026-11-26, SFS-1010.

- Decision: continue the tag-per-version pattern for settings events (tags 19, 20, 21) instead of versioning payloads inside a tag.
  Rationale: the encoder already picks the oldest tag that can carry the value, so older readers keep working until a field they cannot represent is set.
  Date/Author: 2026-11-25, SFS-1001 audit.
- Decision: `studiofs upgrade` is the only adoption path for pre-0.2.5 volumes; there is no read compatibility with 0.2.0–0.2.4 binaries afterwards.
  Rationale: preflight decision of 2026-11-25.
  Date/Author: 2026-11-25, project preflight.
- Decision: register a separate deletion-fence schema migration to preserve existing schema checksums. Its function admits claimants only when their declared format matches the volume. When repairing the baseline, restore that function in the same transaction.
  Date/Author: 2026-11-26, SFS-1132.

- Decision: the test-successor tag 22 encodes an unconditional assignment to a user xattr and normalizes to the existing replay mutation. It declares a read capability only.
  Rationale: the fixture exercises a new shared kind while the regular journal and publication encoders keep producing their baseline tags. A persisted tag 22 requires the successor to replay it, so the manifest declares it irreversible.
  Date/Author: 2026-11-25, SFS-1009.

## Outcomes & Retrospective

SFS-1009 preserves every baseline event byte fixture and keeps tag 22 confined to the test-successor reader. An isolated PostgreSQL 18 test proved that the unknown event refused a whole replay page before any settings or cursor changes. The valid prefix applied on its own, and a retry kept the durable cursor at the last accepted batch. The redb settings regression proved that a failed rewrite preserves the original JSON and appends no event.

Review regressions reproduced three failures before the correction. Snapshot reconstruction now reports a permanent upgrade requirement, a damaged envelope prefix reports a serialization error, and the CLI reports the upgrade requirement for unknown settings. A PostgreSQL 18 test verified that the CLI and authoritative settings update both refuse the unknown field and preserve the stored bytes.

SFS-1132 verified both release fixtures against their recorded checkpoint checksums. Interrupted local migration preserved the outbound journal, upload ledger, client slot and pending inode. The PostgreSQL test proved policy insertion rolls back with a failed format bump. After adoption, the old writer predicate matched zero rows and the old deletion claim was rejected. The CLI test retried after a shared commit and verified an existing RustFS object's SHA-256.

## Context and Orientation

Events are encoded in `crates/studiofs-meta/src/events.rs` with the magic `SFS-EVT`, a wire version byte and a kind tag; tags are append-only and 16–17 are reserved. Serving checkpoints are `ServingCheckpoint` in `crates/studiofs-meta/src/snapshot.rs` (format 4, decoded by suffix). The local metadata database is redb, opened by `RedbMetaEngine`; its header is `FormatInfo` in the `format` key of the settings table, handled in `crates/studiofs-meta/src/redb_engine/format.rs`. The local payload index (`crates/studiofs-cache/src/payload_index.rs`) stores dirty and upload state of payloads that are not yet published, with index format 2 and entry encoding 3. The existing upgrade command is `crates/studiofs/src/cli/upgrade.rs`, which calls `upgrade_shared_format5` in `crates/studiofs-meta/src/pg_engine/format_upgrade.rs` and then `upgrade_local_format5`.

"Unpublished local work" means writes that the local redb journal and payload index have accepted but that are not yet visible to other clients. "Replay" means applying events published by other clients to the local database. Read the boundary sections 2, 5, 6, 7, 9, 10 and 11 of `docs/persisted-format-compatibility.md` and its "Rules" section.

## Plan of Work

SFS-1009 registers the baseline codecs, returns typed unsupported-event errors before applying the selected page, and refuses to rewrite unknown settings fields. The baseline STFS v2/v3 and SFEC v1 writers and capability declarations are already present. Any future payload format requires an activated capability and an encoder selected under that policy. SFS-1010 owns the choice of event encoding from the active volume protocol. SFS-1011 adds a format identity to the serving checkpoint and keeps, during pruning, the newest checkpoint each supported reader can decode. SFS-1012 adds explicit local identities and a crash-safe migration with a recorded step marker; a newer local format returns `LocalUpgradeRequired` and never deletes dirty entries. SFS-1013 applies the same checks to discover, snapshot mounts and dump/load, before any write or DDL. SFS-1132 extends the upgrade command so 5→6→7 runs as one retryable chain under the existing Janitor advisory lock and deletion-owner checks. The format 7 bump must also update `studiofs_fence_legacy_deletion_claim` (`crates/studiofs-meta/src/pg_engine/schema.rs:569`): it compares `studiofs.deletion_format` with the constant `'6'` on any volume at format 6 or later, so a 0.2.3 or 0.2.4 Janitor passes it on a format 7 volume. The trigger must compare the GUC with the volume's current format (or, with SFS-1016, its maintenance capability).

## Concrete Steps

From the repository root:

    cargo test -p studiofs-meta --lib events::tests
    cargo test -p studiofs-meta --lib --features test-successor events::tests
    cargo test -p studiofs-meta --lib pg_engine::snapshot_control::tests
    cargo test -p studiofs-meta --lib baseline_fixtures::baseline_events_decode_one_per_tag
    cargo test -p studiofs-meta --lib --features test-successor test_successor::tests
    cargo test -p studiofs-meta --lib future_volume_settings_are_not_rewritten
    cargo test -p studiofs-meta --lib unknown_required_capabilities_never_grant_write_authority
    cargo test -p studiofs-meta --lib pg_unsupported_event_stops_replay_before_durable_cursor_advance -- --ignored
    cargo test -p studiofs-meta --lib pending_events_stay_unpublished_while_the_policy_cannot_represent_them -- --ignored
    cargo test -p studiofs --test snapshot_cli_test compression_cli_updates_shared_setting_handles_cas_conflict_and_converges -- --ignored
    cargo test -p studiofs-meta --lib snapshot
    cargo test -p studiofs-cache --lib payload_index
    cargo test -p studiofs --bin studiofs upgrade
    cargo clippy -p studiofs-meta -p studiofs-cache --all-targets -- -D warnings

## Validation and Acceptance

With a dedicated PostgreSQL test instance configured in `STUDIOFS_TEST_PG_DSN`, fetch a page with a baseline event followed by an unsupported tag. The engine returns `UpgradeRequired`, and neither the settings nor the durable cursor changes. The known prefix applies when fetched alone, and retrying the unsupported batch leaves the new cursor intact. Bootstrap with only a newer checkpoint available falls back to a supported checkpoint plus replay, or refuses with a typed reason. Killing the process at each step of a local migration and reopening yields either the old or the new format with all dirty payloads intact. Upgrading a format 5 and a format 6 fixture volume reaches format 7, a retried upgrade is a no-op, and a 0.2.4-style publication with format 6 is rejected by the publication predicate.

## Idempotence and Recovery

All migrations and upgrades must be retryable after a lost response. Local migrations keep the original data until the new format is committed. The upgrade command already refuses to run while a mount holds the cache; keep that behavior.

## Artifacts and Notes

Issues: https://linear.app/valandreev/issue/SFS-1009, https://linear.app/valandreev/issue/SFS-1010, https://linear.app/valandreev/issue/SFS-1011, https://linear.app/valandreev/issue/SFS-1012, https://linear.app/valandreev/issue/SFS-1013, https://linear.app/valandreev/issue/SFS-1132.

## Interfaces and Dependencies

Event and checkpoint codecs stay backend-neutral. The local migration must follow the lock order in `crates/studiofs-vfs/src/lock_order.rs` when it runs inside a mount.

Revision note (2026-11-25). SFS-1009 records the codec and replay boundary, the successor fixture, and the scope of capability-gated format selection.

Revision note (2026-11-26). SFS-1011 adds a linked plan for checkpoint formats, safe replay, and PostgreSQL validation.
