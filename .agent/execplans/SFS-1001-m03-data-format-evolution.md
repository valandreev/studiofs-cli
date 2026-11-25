# Milestone 03: Data Format Evolution

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan is maintained in accordance with `.agent/PLANS.md` at the repository root. It depends on milestones 01 and 02 (`.agent/execplans/SFS-1001-m01-contracts-baseline.md`, `.agent/execplans/SFS-1001-m02-pg-compat-foundation.md`).

## Purpose / Big Picture

After this milestone, every data format a successor might change has its own version contract: metadata events, serving checkpoints, the local redb metadata database and the local payload index. A 0.2.5 binary that meets a newer event, checkpoint or local format refuses with a typed reason and keeps all local unpublished work. Existing 0.2.x volumes (format 5 from 0.2.0–0.2.2, format 6 from 0.2.3–0.2.4) can be adopted into the 0.2.5 baseline with `studiofs upgrade`, which chains them to format 7 and fences older writers. Local migrations survive a crash at any point.

## Progress

- [ ] SFS-1009: version MetaEvent codecs, keep tags 1–21 decoding, return a typed error for unknown tags and wire versions, and gate new payload object formats on activated capabilities.
- [ ] SFS-1010: negotiate event publication and replay against the active volume protocol.
- [ ] SFS-1011: support multiple checkpoint formats with safe bootstrap and pruning.
- [ ] SFS-1012: crash-safe local redb and payload-index format migration and recovery.
- [ ] SFS-1013: version-aware discovery, snapshot mounts and dump/load restore.
- [ ] SFS-1132: adopt format 5 and 6 volumes into the baseline via `studiofs upgrade`, chaining to format 7.

## Surprises & Discoveries

- Observation: the payload index reports an unknown entry encoding as `Corruption`, the same class used for real damage.
  Evidence: `crates/studiofs-cache/src/payload_index.rs:1790`.
- Observation: `VolumeSettings` has no `deny_unknown_fields`; quota is protected only by the `quota_v1_revision` rename trick.
  Evidence: `crates/studiofs-meta/src/trash.rs:209`, `crates/studiofs-meta/src/trash.rs:224`.

## Decision Log

- Decision: continue the tag-per-version pattern for settings events (tags 19, 20, 21) instead of versioning payloads inside a tag.
  Rationale: the encoder already picks the oldest tag that can carry the value, so older readers keep working until a field they cannot represent is set.
  Date/Author: 2026-11-25, SFS-1001 audit.
- Decision: `studiofs upgrade` is the only adoption path for pre-0.2.5 volumes; there is no read compatibility with 0.2.0–0.2.4 binaries afterwards.
  Rationale: preflight decision of 2026-11-25.
  Date/Author: 2026-11-25, project preflight.

## Outcomes & Retrospective

Not started.

## Context and Orientation

Events are encoded in `crates/studiofs-meta/src/events.rs` with the magic `SFS-EVT`, a wire version byte and a kind tag; tags are append-only and 16–17 are reserved. Serving checkpoints are `ServingCheckpoint` in `crates/studiofs-meta/src/snapshot.rs` (format 4, decoded by suffix). The local metadata database is redb, opened by `RedbMetaEngine`; its header is `FormatInfo` in the `format` key of the settings table, handled in `crates/studiofs-meta/src/redb_engine/format.rs`. The local payload index (`crates/studiofs-cache/src/payload_index.rs`) stores dirty and upload state of payloads that are not yet published, with index format 2 and entry encoding 3. The existing upgrade command is `crates/studiofs/src/cli/upgrade.rs`, which calls `upgrade_shared_format5` in `crates/studiofs-meta/src/pg_engine/format_upgrade.rs` and then `upgrade_local_format5`.

"Unpublished local work" means writes that the local redb journal and payload index have accepted but that are not yet visible to other clients. "Replay" means applying events published by other clients to the local database. Read the boundary sections 2, 5, 6, 7, 9, 10 and 11 of `docs/persisted-format-compatibility.md` and its "Rules" section.

## Plan of Work

SFS-1009 introduces a typed unsupported-event error and a capability check before a writer emits a new tag or a new payload object format. SFS-1010 makes publication choose the encoding the volume protocol allows and makes replay stop, without advancing the cursor, at the first event it cannot decode. SFS-1011 adds a format identity to the serving checkpoint and keeps, during pruning, the newest checkpoint each supported reader can decode. SFS-1012 adds explicit local identities and a crash-safe migration with a recorded step marker; a newer local format returns `LocalUpgradeRequired` and never deletes dirty entries. SFS-1013 applies the same checks to discover, snapshot mounts and dump/load, before any write or DDL. SFS-1132 extends the upgrade command so 5→6→7 runs as one retryable chain under the existing Janitor advisory lock and deletion-owner checks. The format 7 bump must also update `studiofs_fence_legacy_deletion_claim` (`crates/studiofs-meta/src/pg_engine/schema.rs:569`): it compares `studiofs.deletion_format` with the constant `'6'` on any volume at format 6 or later, so a 0.2.3 or 0.2.4 Janitor passes it on a format 7 volume. The trigger must compare the GUC with the volume's current format (or, with SFS-1016, its maintenance capability).

## Concrete Steps

From the repository root:

    cargo test -p studiofs-meta --lib events
    cargo test -p studiofs-meta --lib snapshot
    cargo test -p studiofs-cache --lib payload_index
    cargo test -p studiofs --bin studiofs upgrade
    cargo clippy -p studiofs-meta -p studiofs-cache --all-targets -- -D warnings

## Validation and Acceptance

Feeding an event with an unknown tag to replay returns the typed error and leaves the local version unchanged. Bootstrap with only a newer checkpoint available falls back to a supported checkpoint plus replay, or refuses with a typed reason. Killing the process at each step of a local migration and reopening yields either the old or the new format with all dirty payloads intact. Upgrading a format 5 and a format 6 fixture volume reaches format 7, a retried upgrade is a no-op, and a 0.2.4-style publication with format 6 is rejected by the publication predicate.

## Idempotence and Recovery

All migrations and upgrades must be retryable after a lost response. Local migrations keep the original data until the new format is committed. The upgrade command already refuses to run while a mount holds the cache; keep that behavior.

## Artifacts and Notes

Issues: https://linear.app/valandreev/issue/SFS-1009, https://linear.app/valandreev/issue/SFS-1010, https://linear.app/valandreev/issue/SFS-1011, https://linear.app/valandreev/issue/SFS-1012, https://linear.app/valandreev/issue/SFS-1013, https://linear.app/valandreev/issue/SFS-1132.

## Interfaces and Dependencies

Event and checkpoint codecs stay backend-neutral. The local migration must follow the lock order in `crates/studiofs-vfs/src/lock_order.rs` when it runs inside a mount.
