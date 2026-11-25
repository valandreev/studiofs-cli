# Persisted-Format Audit And 0.2.5 Forward-Compatibility Policy

Status: accepted policy for the 0.2.5 baseline
([SFS-1001](https://linear.app/valandreev/issue/SFS-1001)). The version and
capability model is recorded in
[ADR 0018](adr/0018-version-identities-and-capability-model.md). Implementation
is tracked in the Linear project "[0.2.5] Metadata Compatibility Layer".

This page lists every boundary where StudioFS persists bytes that another
binary, another release, or a later process of the same binary reads back. For
each one it records the current version identity, how strict the reader is
today, who writes it, what a successor may change, and how the 0.2.5 binary
must fail when it meets something it does not understand. Every `path:line`
reference was verified against `main` at `f9e258cf`.

## Scope and binding decisions

- The baseline is 0.2.5. This policy guarantees behavior between 0.2.5 and
  releases that explicitly declare support for it. Compatibility with pre-0.2.5
  clients or state is out of scope: there is no guarantee, and such volumes are
  adopted only through `studiofs upgrade`.
- 0.2.x volumes join the baseline only through `studiofs upgrade`. Format 5
  (0.2.0–0.2.2) and format 6 (0.2.3–0.2.4) chain to format 7
  ([SFS-1132](https://linear.app/valandreev/issue/SFS-1132)). That bump fences
  every older 0.2.x writer, because their publication predicate compares
  against the old format number. Interop with them is out of scope.
- Writer fencing reuses the existing publication predicate and adds a
  PostgreSQL trigger keyed on a session GUC
  ([SFS-1014](https://linear.app/valandreev/issue/SFS-1014)). A GUC is a
  per-session PostgreSQL setting set with `set_config`. Separate PostgreSQL
  roles are out of scope.
- 0.3.0 upgrades only from 0.2.5 or later. 0.2.0–0.2.4 volumes reach 0.3.0
  only through the 0.2.5 `studiofs upgrade`. The 0.3.0 upgrade policy itself
  stays owned by
  [SFS-982](https://linear.app/valandreev/issue/SFS-982),
  [SFS-983](https://linear.app/valandreev/issue/SFS-983),
  [SFS-984](https://linear.app/valandreev/issue/SFS-984) and
  [SFS-985](https://linear.app/valandreev/issue/SFS-985); this page only fixes
  the 0.2.5 entry point they build on.
- 0.2.7 (Redis-compatible metadata backend) is the first real successor of
  0.2.5. Version identities, evaluator inputs, volume policy, sessions and
  fencing are defined backend-neutrally in ADR 0018. PostgreSQL is the only
  adapter in 0.2.5.
- Compression shipped in 0.2.4 and is part of the 0.2.5 baseline. It is not a
  separate capability
  ([ADR 0015](adr/0015-adaptive-zstd-payload-compression.md)).

## Key finding: one number, many meanings

`CURRENT_FORMAT_VERSION` (`crates/studiofs-meta/src/schema.rs:46`, value 6) is
documented as "on-disk metadata and PostgreSQL sync format version". The same
number gates the local redb header, the shared `FormatInfo`, the
`metadata_state.metadata_format_version` column, the bootstrap checkpoint
descriptor, and the publication predicate. Every check is an exact equality:

- `decode_format_info` (`crates/studiofs-meta/src/lib.rs:93`) rejects any
  `format_version` other than the current one (`crates/studiofs-meta/src/lib.rs:96`).
- `ensure_supported_format_version` (`crates/studiofs-meta/src/lib.rs:190`) and
  `ensure_supported_pg_sync_format_version`
  (`crates/studiofs-meta/src/lib.rs:206`) do the same for in-memory and
  PostgreSQL values.
- The bootstrap checkpoint descriptor repeats the check
  (`crates/studiofs-meta/src/engine.rs:141`).
- Publication bumps `metadata_state.version` only when
  `metadata_format_version` still equals the writer's value
  (`crates/studiofs-meta/src/pg_engine/sql.rs:56`).

So a harmless additive change, such as a new optional table, must either reuse
format 6 (and be invisible to older readers) or bump the number (and lock out
every older reader and writer at once). The rest of this page splits that
number into separate identities.

## Boundary audit

Each subsection covers one persisted boundary. "Successor" means any release
that declares support for 0.2.5 (the compiled test successor of
[SFS-1131](https://linear.app/valandreev/issue/SFS-1131), and 0.2.7).

### 1. FormatInfo (shared volume header)

- Location: JSON in redb `SETTINGS_TABLE["format"]` and in
  `metadata_state.format_info` (`crates/studiofs-meta/src/pg_engine/schema.rs:89`).
  Type: `FormatInfo` (`crates/studiofs-meta/src/schema.rs:1574`).
- Version field: `format_version`, value 6.
- Reader strictness: `FormatInfo`, `MetadataConfig` and `PostgresConfig` use
  `deny_unknown_fields` (`crates/studiofs-meta/src/schema.rs:1573`,
  `crates/studiofs-meta/src/schema.rs:1821`,
  `crates/studiofs-meta/src/schema.rs:1836`). `decode_format_info` checks the
  version first, then the full shape. A wrong version is the typed
  `UnsupportedFormatVersion`; an unknown field or an unknown
  `MetadataBackend` variant (`crates/studiofs-meta/src/schema.rs:1779`) becomes
  the generic `MetaError::Serialization` (`crates/studiofs-meta/src/lib.rs:103`).
  The bootstrap descriptor turns that into `InvalidCheckpointDescriptor`
  (`crates/studiofs-meta/src/engine.rs:162`), which the client lifecycle maps to
  `InvalidCheckpoint` rather than an upgrade message
  (`crates/studiofs-meta/src/engine.rs:964`).
- Writers: `studiofs init` (`crates/studiofs/src/cli/init.rs:585`),
  `studiofs umeta` (S3 connection fields), `studiofs upgrade`
  (`crates/studiofs-meta/src/pg_engine/format_upgrade.rs:58`), dump/load.
- Successor may change additively: nothing in the JSON shape without a new
  required capability, because `deny_unknown_fields` is deliberate. New
  per-volume policy belongs in the volume policy record (ADR 0018), not here.
- Incompatible: any new field, new backend variant, or identity change.
- Fail-closed outcome required: typed `UpgradeRequired` naming the field or
  variant, never `Serialization`. Owner:
  [SFS-1003](https://linear.app/valandreev/issue/SFS-1003).

### 2. Local redb metadata

- Location: per-volume `<name>.db`; header in `SETTINGS_TABLE["format"]`.
- Version field: none of its own. The redb layout is versioned by
  `FormatInfo.format_version`, read through
  `read_format_info_from_read_txn`
  (`crates/studiofs-meta/src/redb_engine/format.rs:70`), which calls
  `decode_format_info` (`crates/studiofs-meta/src/redb_engine/format.rs:82`).
- Reader strictness: exact equality. `format_info_for_upgrade` is the only path
  that accepts format 5 or 6 (`crates/studiofs-meta/src/redb_engine/format.rs:20`),
  and `upgrade_local_format5` rewrites only the header after the shared
  authority is upgraded (`crates/studiofs-meta/src/redb_engine/format.rs:33`).
  A cache whose header is still at format 5 keeps serving local writes but
  blocks settings events and checkpoint export (test evidence:
  `crates/studiofs-meta/src/redb_engine/volume_settings.rs:139`).
- Writers: the mounted engine, `discover`, `upgrade`, snapshot materialization.
- Successor may change additively: new tables that an older binary never
  opens, if the local format identity records them as optional.
- Incompatible: changing an existing table's key or value encoding.
- Fail-closed outcome: typed `LocalUpgradeRequired` before any write
  transaction; the cache is never reset. Owner:
  [SFS-1003](https://linear.app/valandreev/issue/SFS-1003) (identity split) and
  [SFS-1012](https://linear.app/valandreev/issue/SFS-1012) (crash-safe local
  migration).

### 3. PostgreSQL schema and DDL

- Location: `studiofs_pg_schema_sql`
  (`crates/studiofs-meta/src/pg_engine/schema.rs:13`), applied as one batch by
  `ensure_studiofs_pg_schema` (`crates/studiofs-meta/src/pg_engine/schema.rs:935`).
- Version field: only the `payload_gc` component in
  `studiofs_schema_components`
  (`crates/studiofs-meta/src/pg_engine/schema.rs:903`), value
  `PAYLOAD_GC_SCHEMA_VERSION = 2`
  (`crates/studiofs-meta/src/pg_engine/schema.rs:922`). The marker insert is the
  last statement and never moves backwards
  (`crates/studiofs-meta/src/pg_engine/schema.rs:912`).
- Reader strictness: mount startup runs `validate_postgres_schema`
  (`crates/studiofs-meta/src/pg_engine.rs:1280`) against one global list of
  required tables (`crates/studiofs-meta/src/pg_engine.rs:1314`) and columns.
  A missing table tells the operator to destroy and re-init
  (`crates/studiofs-meta/src/pg_engine.rs:1365`). Extra tables are ignored.
  The Janitor requires `payload_gc` to equal exactly 2 and its guard triggers
  to be enabled (`crates/studiofs-meta/src/pg_engine/janitor.rs:430`).
- Writers: DDL runs only from CLI commands: `init`
  (`crates/studiofs/src/cli/init.rs:322`, `crates/studiofs/src/cli/init.rs:420`),
  `load`
  (`crates/studiofs/src/cli/dump_load.rs:413`) and `upgrade`
  (`crates/studiofs/src/cli/upgrade.rs:43`). Mounts never run DDL.
- Database-wide versus per-volume: DDL, functions and triggers are database
  wide; every volume in the database shares them. Format, policy and
  activation are per volume (`metadata_state` row). The DDL uses
  `CREATE OR REPLACE FUNCTION` for trigger bodies, for example
  `studiofs_fence_legacy_deletion_claim`
  (`crates/studiofs-meta/src/pg_engine/schema.rs:569`). An older binary's
  `init` or `load` into a database that a successor already expanded would
  replace the successor's function bodies. 0.2.5 must refuse DDL when any
  `studiofs_schema_components` row is newer than it knows.
- Successor may change additively: new tables, columns with defaults, indexes,
  and new component rows, when the declared capability marks them optional for
  0.2.5.
- Incompatible: dropping or retyping a column 0.2.5 reads, changing trigger
  semantics 0.2.5 relies on, or raising a component version 0.2.5 checks.
- Fail-closed outcome: typed `UpgradeRequired` (unknown required component) or
  `MaintenanceForbidden` (Janitor); no DDL from an older binary. Owner:
  [SFS-1005](https://linear.app/valandreev/issue/SFS-1005) (migration registry)
  and [SFS-1006](https://linear.app/valandreev/issue/SFS-1006)
  (capability-scoped validation).

### 4. Shared metadata state and publication

- Location: `metadata_state` (`crates/studiofs-meta/src/pg_engine/schema.rs:70`)
  holds `metadata_format_version`, the serving checkpoint, and the publication
  counter.
- Version field: `metadata_format_version SMALLINT`, value 6.
- Writer check: `BUMP_METADATA_VERSION`
  (`crates/studiofs-meta/src/pg_engine/sql.rs:51`) bumps only
  `WHERE vol = $1 AND metadata_format_version = $2`. Event publication passes
  `CURRENT_FORMAT_VERSION` (`crates/studiofs-meta/src/pg_engine/metadata_io.rs:601`,
  `crates/studiofs-meta/src/pg_engine/metadata_io.rs:644`) and, when no row
  matches, re-reads the format to report a typed error
  (`crates/studiofs-meta/src/pg_engine/metadata_io.rs:660`). Volume-settings
  writes use the same predicate, but through `query_one` and
  `classify_pg_error("order volume settings")`
  (`crates/studiofs-meta/src/pg_engine/volume_settings.rs:339`), so a format
  mismatch there surfaces as a generic PostgreSQL error, not the typed
  `UnsupportedFormatVersion` that event publication reports
  (`crates/studiofs-meta/src/pg_engine/metadata_io.rs:671`).
- `metadata_format_version` is also the read gate: the checkpoint descriptor
  (`crates/studiofs-meta/src/engine.rs:141`),
  `ensure_supported_pg_sync_format_version`
  (`crates/studiofs-meta/src/lib.rs:206`) and the Janitor
  (`crates/studiofs-meta/src/pg_engine/janitor.rs:571`) require exact
  equality. Raising it would lock out 0.2.5 readers, so a successor must not
  raise it at Activate. From format 7 on it stays fixed (ADR 0018).
- Successor may change: `BUMP_METADATA_VERSION` is the template. SFS-1007 adds
  a separate per-volume write-requirement field that publication checks in the
  same UPDATE. Activate raises that field, which fences 0.2.5 writers and
  leaves 0.2.5 readers working when the capability is read-compatible. The
  check runs inside the publication transaction, so a writer that reconnects
  is re-checked on its next publication.
- Fail-closed outcome: publication returns `UpgradeRequired`; the local
  journal is kept as unpublished local work. Owner:
  [SFS-1007](https://linear.app/valandreev/issue/SFS-1007) (per-volume
  requirements) and [SFS-1014](https://linear.app/valandreev/issue/SFS-1014)
  (trigger).

### 5. Events and event tags

- Location: `event_log.event_type` and `event_log.event_payload`
  (`crates/studiofs-meta/src/pg_engine/schema.rs:51`); local journal in redb.
- Version fields: wire envelope `SFS-EVT` plus wire version 1
  (`crates/studiofs-meta/src/events.rs:6`), and a one-byte kind tag
  (`crates/studiofs-meta/src/events.rs:8`). Tags are append-only; 16 and 17 are
  reserved (`crates/studiofs-meta/src/events.rs:23`,
  `crates/studiofs-meta/src/events.rs:29`).
- Existing evolution mechanism: tag-per-version for `VolumeSettings`: tag 19
  (revision, trash), 20 (adds quota), 21 (full settings with compression)
  (`crates/studiofs-meta/src/events.rs:25`). The encoder picks the oldest tag
  that can carry the value (`crates/studiofs-meta/src/events.rs:297`), and the
  PostgreSQL `event_type` `set_volume_settings` accepts all three
  (`crates/studiofs-meta/src/events.rs:387`).
- Reader strictness: an unknown wire version, kind tag, or `event_type`
  fails closed, but as `MetaError::Serialization`
  (`crates/studiofs-meta/src/events.rs:433`,
  `crates/studiofs-meta/src/events.rs:617`,
  `crates/studiofs-meta/src/events.rs:383`), which the lifecycle maps to a
  generic storage error (`crates/studiofs-meta/src/engine.rs:974`).
- Successor may change additively: new tags, published only after the volume
  activates the capability that requires them.
- Incompatible: reusing a tag or changing an existing tag's payload.
- Fail-closed outcome: a typed unsupported-event error naming the tag, mapped
  to `UpgradeRequired` (ADR 0018), that stops replay at the last applied
  version without advancing the local cursor. Owner:
  [SFS-1009](https://linear.app/valandreev/issue/SFS-1009) (codecs) and
  [SFS-1010](https://linear.app/valandreev/issue/SFS-1010) (negotiation).

### 6. Serving checkpoints

- Location: `metadata_state.snapshot` with `checkpoint_sha256`,
  publication state and generation; snapshot source checkpoints in
  `snapshot_source_checkpoints` with explicit `checkpoint_format_version`
  (`crates/studiofs-meta/src/pg_engine/schema.rs:327`).
- Version fields: `ServingCheckpoint::FORMAT_VERSION = 4` and
  `SCHEMA_VERSION = 1` (`crates/studiofs-meta/src/snapshot.rs:310`). The
  serving checkpoint in `metadata_state` has no format column of its own; it is
  gated by `metadata_format_version` through the descriptor
  (`crates/studiofs-meta/src/engine.rs:141`).
- Reader strictness: `decode` is suffix-based and rejects trailing bytes after
  the format 4 suffix (`crates/studiofs-meta/src/snapshot.rs:341`). Snapshot
  source checkpoints are rejected unless
  `supports_format_version` (1..=4, `crates/studiofs-meta/src/snapshot.rs:315`)
  accepts them (`crates/studiofs-meta/src/pg_engine/snapshot_control.rs:362`,
  `crates/studiofs-meta/src/pg_engine/snapshot_control.rs:681`), but the
  refusal is untyped: an unknown format, a checksum mismatch and a schema
  mismatch all return the same permanent `snapshot source checkpoint is
  invalid` (`crates/studiofs-meta/src/pg_engine/snapshot_control.rs:360`).
- Writers: checkpoint coordinator under `bootstrap_checkpoint_leases`
  (`crates/studiofs-meta/src/pg_engine/schema.rs:158`). The lease uses the
  server clock (`clock_timestamp()`) and increments a fencing token on
  takeover (`crates/studiofs-meta/src/pg_engine/sql.rs:158`).
- Successor may change additively: a new checkpoint format published beside
  the format 4 one while any 0.2.5 reader is supported.
- Fail-closed outcome: a typed unsupported-checkpoint error, mapped to
  `UpgradeRequired` (ADR 0018); bootstrap falls
  back to a supported checkpoint plus bounded replay, or refuses. Pruning must
  keep the newest checkpoint each supported reader can decode. Owner:
  [SFS-1011](https://linear.app/valandreev/issue/SFS-1011).

### 7. VolumeSettings

- Location: redb settings, checkpoint suffix, and settings events (tags 19–21).
  Type: `VolumeSettings` (`crates/studiofs-meta/src/trash.rs:209`).
- Fields: `revision`, `trash`, `logical_quota_bytes`, and `compression`
  (`crates/studiofs-meta/src/trash.rs:220`), a `CompressionPolicy` of mode
  `off` or `zstd` with level 1–19 (`crates/studiofs-meta/src/trash.rs:105`).
- Reader strictness: no `deny_unknown_fields`. The JSON form protects quota by
  renaming `revision` to `quota_v1_revision` when a quota is set, so a
  quota-unaware reader fails instead of dropping the quota
  (`crates/studiofs-meta/src/trash.rs:224`). An unknown `CompressionMode`
  fails to decode.
- Writers: `studiofs volume` settings commands through `compare_and_set_*`,
  which use the publication predicate
  (`crates/studiofs-meta/src/pg_engine/volume_settings.rs:336`).
- Successor may change additively: new settings only through a new event tag
  and a new checkpoint suffix, gated by a required capability.
- Fail-closed outcome: a 0.2.5 binary must refuse to rewrite settings it
  cannot represent (`UpgradeRequired`), never drop a field. Owner:
  [SFS-1009](https://linear.app/valandreev/issue/SFS-1009).

### 8. Remote payload objects (STFS inside SFEC)

- Location: S3 objects under the volume data prefix.
- STFS: magic `STFS` (`crates/studiofs-data/src/object.rs:23`), version 2 for
  raw payloads and version 3 for zstd chunks with a seek table
  (`crates/studiofs-data/src/object.rs:26`,
  `crates/studiofs-data/src/object.rs:29`, SFS-1087). The reader rejects
  `version > 3` with the typed `UnsupportedVersion`
  (`crates/studiofs-data/src/object.rs:325`) and an unknown codec byte with
  `UnsupportedCodec` (`crates/studiofs-data/src/object.rs:160`).
- SFEC: magic `SFEC`, version 1 (`crates/studiofs-data/src/envelope.rs:15`),
  no flags supported (`crates/studiofs-data/src/envelope.rs:28`). The frame
  size is written per object: 64 KiB for STFS v3 and 1 MiB for v2
  (`crates/studiofs-data/src/envelope.rs:18`,
  `crates/studiofs-data/src/envelope.rs:21`, SFS-1086). The reader accepts only
  those two sizes (`crates/studiofs-data/src/envelope.rs:580`).
- Key material: `volume_crypto_keys`
  (`crates/studiofs-meta/src/pg_engine/schema.rs:33`) and
  `EncryptionConfig.format_version` (`crates/studiofs-meta/src/schema.rs:48`),
  checked for equality (`crates/studiofs-meta/src/schema.rs:1739`).
- Baseline fixture: `STFS_V3_BASELINE_FIXTURE`
  (`crates/studiofs-data/src/object.rs:2358`, bytes in
  `crates/studiofs-data/tests/fixtures/stfs-v3-mixed.bin`), reused in
  `crates/studiofs-data/src/compression.rs:972`,
  `crates/studiofs-data/src/store.rs:1991` and
  `crates/studiofs-vfs/src/fetch.rs:2476`.
- Successor may change additively: a new codec id or a new STFS/SFEC version,
  written only after the volume activates the capability that requires it.
  Objects are immutable and never rewritten.
- Fail-closed outcome: typed read error for that object only; the payload is
  never cached and the read returns `EIO`. Writers never emit a format the
  volume has not activated. Owner:
  [SFS-1009](https://linear.app/valandreev/issue/SFS-1009) (capability gating
  of writes) and [SFS-1002](https://linear.app/valandreev/issue/SFS-1002)
  (fixtures).

### 9. Local payload index

- Location: cache redb behind `crates/studiofs-cache/src/payload_index.rs`;
  holds dirty and upload state of unpublished payloads.
- Version fields: index format 2 and entry encoding 3
  (`crates/studiofs-cache/src/payload_index.rs:24`,
  `crates/studiofs-cache/src/payload_index.rs:27`), stored in the
  `index_format_version` counter (`crates/studiofs-cache/src/payload_index.rs:36`).
- Reader strictness: format 1 is upgraded in place to 2 on open; any other
  value is the typed `UnsupportedFormat`
  (`crates/studiofs-cache/src/payload_index.rs:1295`). A missing counter in a
  non-empty index and an unknown entry encoding are reported as `Corruption`
  (`crates/studiofs-cache/src/payload_index.rs:1306`,
  `crates/studiofs-cache/src/payload_index.rs:1790`). `Corruption` is the wrong
  class for a newer encoding: it must never trigger repair or eviction of
  unpublished data.
- Successor may change additively: a new entry encoding only after a crash-safe
  local migration that 0.2.5 can refuse cleanly.
- Fail-closed outcome: typed `LocalUpgradeRequired` before the mount serves;
  dirty entries are left untouched. Owner:
  [SFS-1012](https://linear.app/valandreev/issue/SFS-1012).

### 10. Snapshot artifacts

- Location: immutable metadata artifacts in S3, cataloged in
  `snapshot_artifacts`.
- Version fields: `SNAPSHOT_ARTIFACT_FORMAT_VERSION = 1` and
  `SNAPSHOT_ARTIFACT_SCHEMA_VERSION = 1`
  (`crates/studiofs-vfs/src/snapshot_worker.rs:37`).
- Reader strictness: snapshot mount rejects any other value with the typed
  permanent error `unsupported_artifact_version`
  (`crates/studiofs-vfs/src/snapshot_mount.rs:386`). The artifact also embeds
  `FormatInfo`, checked by `open_read_only_snapshot_meta`
  (`crates/studiofs-meta/src/lib.rs:140`).
- Successor may change: a new artifact version for new snapshots. Old artifacts
  stay readable by the release that wrote them.
- Fail-closed outcome: the 0.2.5 binary refuses to mount, delete, or
  re-verify an artifact it cannot decode; cleanup of such artifacts is a
  maintenance capability. Owner:
  [SFS-1013](https://linear.app/valandreev/issue/SFS-1013) and
  [SFS-1016](https://linear.app/valandreev/issue/SFS-1016).

### 11. Dump and load

- Location: JSON file written by `studiofs dump`, type `MetadataDump` with
  `deny_unknown_fields` (`crates/studiofs/src/cli/dump_load.rs:38`).
- Version field: `dump_version = 4` (`crates/studiofs/src/cli/dump_load.rs:320`).
- Reader strictness: anything other than 4 is rejected
  (`crates/studiofs/src/cli/dump_load.rs:693`) with an untyped message.
- Successor may change: a new dump version that records the source release and
  required capabilities.
- Fail-closed outcome: a typed unsupported-dump error, mapped to
  `UpgradeRequired` (ADR 0018); `load` performs no DDL and no writes before the
  version check. Owner:
  [SFS-1013](https://linear.app/valandreev/issue/SFS-1013).

### 12. Janitor and GC state

- Location: `snapshot_payload_upload_objects`, `snapshot_payload_gc_candidates`,
  `snapshot_payload_sweep_checkpoints`, `snapshot_payload_upload_watermarks`
  (`crates/studiofs-meta/src/pg_engine/schema.rs:543` onward), versioned as the
  `payload_gc` component (SFS-980).
- Existing fences: the claim statement first runs
  `CLAIM_PUBLICATION_FENCE_SQL`, which sets the transaction GUC
  `studiofs.deletion_format = '6'` under a share lock on `metadata_state`
  (`crates/studiofs-meta/src/pg_engine/janitor.rs:1089`). The trigger
  `studiofs_fence_legacy_deletion_claim` rejects a deletion claim on a volume
  with `metadata_format_version >= 6` when that GUC is not `'6'`
  (`crates/studiofs-meta/src/pg_engine/schema.rs:574`,
  `crates/studiofs-meta/src/pg_engine/schema.rs:575`). The GUC is compared
  with the constant `'6'`, so a 0.2.3 or 0.2.4 Janitor passes the trigger on a
  format 7 volume; only the app-side format check
  (`crates/studiofs-meta/src/pg_engine/janitor.rs:571`) fences it. SFS-1132 and
  SFS-1016 must make the trigger compare the GUC with the volume's current
  format or maintenance capability, not a constant. The schema probe
  requires the exact component version and enabled guards
  (`crates/studiofs-meta/src/pg_engine/janitor.rs:60`).
  `upgrade_shared_format5` takes the Janitor advisory lock and requires settled
  deletion owners (`crates/studiofs-meta/src/pg_engine/format_upgrade.rs:20`).
- Successor may change: raise `payload_gc`, which fences 0.2.5 Janitors through
  the exact-version probe. That is fail-closed but blocks GC on every volume in
  the database until all Janitors are upgraded.
- Fail-closed outcome: `MaintenanceForbidden` before any S3 DELETE. Owner:
  [SFS-1016](https://linear.app/valandreev/issue/SFS-1016).

## Existing mechanisms to extend, not duplicate

Later issues must build on these instead of adding parallel mechanisms:

| Mechanism | Source | Extend in |
|---|---|---|
| `studiofs_schema_components` with monotonic marker (SFS-980) | `crates/studiofs-meta/src/pg_engine/schema.rs:903` | SFS-1005, SFS-1006 |
| `studiofs upgrade` (format 5→6) | `crates/studiofs/src/cli/upgrade.rs:15`, `crates/studiofs-meta/src/pg_engine/format_upgrade.rs:10` | SFS-1132 |
| `BUMP_METADATA_VERSION` format predicate on publication | `crates/studiofs-meta/src/pg_engine/sql.rs:51` | SFS-1007, SFS-1014 |
| `studiofs_fence_legacy_deletion_claim` trigger with `studiofs.deletion_format` GUC | `crates/studiofs-meta/src/pg_engine/schema.rs:569`, `crates/studiofs-meta/src/pg_engine/janitor.rs:1089` | SFS-1014, SFS-1016 |
| `bootstrap_checkpoint_leases` (server-clock expiry, fencing token) | `crates/studiofs-meta/src/pg_engine/schema.rs:158`, `crates/studiofs-meta/src/pg_engine/sql.rs:158` | SFS-1008, SFS-1011 |
| VolumeSettings tag-per-version events (19, 20, 21) | `crates/studiofs-meta/src/events.rs:25` | SFS-1009 |
| `STFS_V3_BASELINE_FIXTURE` | `crates/studiofs-data/src/object.rs:2358` | SFS-1002 |

## Multi-volume databases

PostgreSQL DDL is database-wide; format, policy and activation are per volume.
The following operations need every volume in the database to agree, because
they change shared objects:

- Expand that installs or replaces a trigger or function, because the body
  applies to every volume's rows. The trigger body must branch on per-volume
  state (as `studiofs_fence_legacy_deletion_claim` does with
  `metadata_format_version >= 6`). It must also compare the actor's GUC with
  that per-volume state, not with a constant: the current trigger accepts
  `studiofs.deletion_format = '6'` on any format 6 or later volume, so it does
  not fence a 0.2.3 or 0.2.4 Janitor on a format 7 volume (SFS-1132,
  SFS-1016).
- Contract (dropping a table, column or trigger body branch), which requires
  that no volume in the database still needs it.
- Raising a database-wide component version that older Janitors check exactly
  (`payload_gc`).

Activate, policy epoch changes and per-volume format bumps are per volume and
need agreement only among that volume's clients.

## Baseline obligations

The 0.2.5 binary cannot be patched later. It must do the following itself so a
successor can fence it safely. "Current state" is what `main` does today.

| Obligation | Current state | Owner |
|---|---|---|
| Publication checks a per-volume field that a successor can raise at Activate, in the same UPDATE as the version bump. | Partial: `BUMP_METADATA_VERSION` checks `metadata_format_version` (`crates/studiofs-meta/src/pg_engine/sql.rs:56`), but that field is also the read gate, so raising it locks out readers. A separate write-requirement field is missing. | [SFS-1007](https://linear.app/valandreev/issue/SFS-1007) |
| A refused volume-settings publication reports a typed reason, like event publication. | Missing: `query_one` plus `classify_pg_error("order volume settings")` turns a format mismatch into a generic PostgreSQL error (`crates/studiofs-meta/src/pg_engine/volume_settings.rs:339`); event publication re-reads the format and returns `UnsupportedFormatVersion` (`crates/studiofs-meta/src/pg_engine/metadata_io.rs:671`). | [SFS-1007](https://linear.app/valandreev/issue/SFS-1007) |
| Every publication stamps its session and policy epoch (GUC or column) so a later trigger can reject it. | Missing: publication sets no GUC; `event_log.client_id` exists (`crates/studiofs-meta/src/pg_engine/schema.rs:57`) but there is no session or epoch. | [SFS-1014](https://linear.app/valandreev/issue/SFS-1014), [SFS-1008](https://linear.app/valandreev/issue/SFS-1008) |
| Destructive maintenance sets a maintenance capability GUC before deleting. | Partial: Janitor claims set `studiofs.deletion_format` (`crates/studiofs-meta/src/pg_engine/janitor.rs:1089`); snapshot cleanup, event-log pruning and checkpoint pruning set none. | [SFS-1016](https://linear.app/valandreev/issue/SFS-1016) |
| Unknown required capabilities fail closed with a typed reason. | Missing: no capability set exists. | [SFS-1004](https://linear.app/valandreev/issue/SFS-1004) |
| Unknown event tags fail closed with a typed reason. | Partial: fails closed as `Serialization` (`crates/studiofs-meta/src/events.rs:617`). | [SFS-1009](https://linear.app/valandreev/issue/SFS-1009) |
| Unknown checkpoint formats fail closed with a typed reason. | Missing typed reason: snapshot sources fail closed, but one untyped `snapshot source checkpoint is invalid` covers unknown format, checksum mismatch and schema mismatch (`crates/studiofs-meta/src/pg_engine/snapshot_control.rs:360`). The serving checkpoint has no format field of its own and relies on `metadata_format_version`. | [SFS-1011](https://linear.app/valandreev/issue/SFS-1011) |
| Unknown payload object formats fail closed with a typed reason. | Present: `UnsupportedVersion` and `UnsupportedCodec` (`crates/studiofs-data/src/object.rs:325`, `crates/studiofs-data/src/object.rs:160`); SFEC version and flags are strict. | [SFS-1009](https://linear.app/valandreev/issue/SFS-1009) (write gating) |
| Unknown local formats fail closed with a typed reason. | Partial: payload index format is typed (`crates/studiofs-cache/src/payload_index.rs:1299`), entry encoding is `Corruption` (`crates/studiofs-cache/src/payload_index.rs:1790`); redb has no own identity. | [SFS-1012](https://linear.app/valandreev/issue/SFS-1012), [SFS-1003](https://linear.app/valandreev/issue/SFS-1003) |
| Additive PG objects are tolerated when declared capabilities allow it. | Partial: extra tables are ignored by `validate_postgres_schema`, but the required list is global and not capability-scoped (`crates/studiofs-meta/src/pg_engine.rs:1314`); `init`/`load`/`upgrade` re-run `CREATE OR REPLACE` DDL without checking for newer components. | [SFS-1006](https://linear.app/valandreev/issue/SFS-1006), [SFS-1005](https://linear.app/valandreev/issue/SFS-1005) |
| Policy epoch is revalidated after reconnect. | Partial: the format predicate is re-evaluated in every publication transaction; no policy epoch exists. | [SFS-1015](https://linear.app/valandreev/issue/SFS-1015), [SFS-1007](https://linear.app/valandreev/issue/SFS-1007) |
| Unknown `MetadataBackend` variant or unknown `FormatInfo` field fails closed with typed `UpgradeRequired`. | Missing: both surface as `Serialization` (`crates/studiofs-meta/src/lib.rs:103`). `deny_unknown_fields` is present on `FormatInfo`, `MetadataConfig`, `PostgresConfig`, shared through `metadata_state.format_info`. | [SFS-1003](https://linear.app/valandreev/issue/SFS-1003) |
| 0.2.x volumes adopt the baseline by `studiofs upgrade` to format 7. | Missing: upgrade handles 5→6 only (`crates/studiofs-meta/src/pg_engine/format_upgrade.rs:35`). | [SFS-1132](https://linear.app/valandreev/issue/SFS-1132) |

## Version and operation support matrix

Rows are release pairs: the first release owns the persisted state or has
activated the volume; the second is the binary acting on it. "Successor" is
the compiled test successor
([SFS-1131](https://linear.app/valandreev/issue/SFS-1131)) or 0.2.7.

| Pair | Read / mount | Write / publish | Maintenance, GC, Janitor | Snapshot | Dump / load | Offline client with pending local writes | Rollback |
|---|---|---|---|---|---|---|---|
| 0.2.5 ↔ 0.2.5 | Supported | Supported | Supported | Supported | Supported | Publishes on reconnect | Not applicable |
| Successor state, 0.2.5 binary, successor capability not activated | Supported | Supported | Supported if `payload_gc` is unchanged; otherwise `MaintenanceForbidden` | Supported for artifacts it can decode; others `unsupported_artifact_version` | Load of a successor dump: `UpgradeRequired` (unsupported dump) | Publishes on reconnect | Supported: nothing activated |
| Successor state, 0.2.5 binary, capability activated as read-compatible (Activate raised the per-volume write-requirement field from SFS-1007; `metadata_format_version` unchanged) | Supported | `UpgradeRequired` at publication, from the write-requirement check; journal kept | `MaintenanceForbidden` | Mount of new artifacts refused; create refused | `UpgradeRequired` (unsupported dump) | Keeps serving locally; publication refused with `UpgradeRequired`; pending work kept until the binary is upgraded | Only before Contract, by the successor's documented deactivate step |
| Successor state, 0.2.5 binary, capability activated as read-incompatible | `UpgradeRequired` at mount and discover; an open mount stops replay at the last applied version | `UpgradeRequired` | `MaintenanceForbidden` | Refused | `UpgradeRequired` (unsupported dump) | Local journal kept; never discarded or reset | None after Activate |
| 0.2.5 state, successor binary | Supported | Supported until it activates | Supported | Supported | Loads 0.2.5 dumps | Publishes on reconnect | Not applicable |
| Unknown future ↔ 0.2.5 binary | `UpgradeRequired` for any unknown required capability, tag, checkpoint or payload format; `LocalUpgradeRequired` for an unknown local format | `UpgradeRequired` | `MaintenanceForbidden` | Refused with typed reason | `UpgradeRequired` (unsupported dump) | Local journal kept; refused at publication | No guarantee |
| Pre-0.2.5 ↔ 0.2.5 binary | No guarantee; adopt via `studiofs upgrade` | No guarantee; adopt via `studiofs upgrade` | No guarantee; adopt via `studiofs upgrade` | No guarantee; adopt via `studiofs upgrade` | No guarantee; adopt via `studiofs upgrade` | No guarantee; adopt via `studiofs upgrade` | No guarantee |

## Rules

### Pending local writes

Local writer state can be newer than shared state. A compatibility refusal
never deletes, resets or rewrites the local journal, the redb metadata, or
dirty payload index entries. Refused publication leaves the work unpublished
and reports `UpgradeRequired`; the operator upgrades the binary and the
journal publishes. `studiofs upgrade` refuses to run while a mount holds the
cache (`crates/studiofs/src/cli/upgrade.rs:16`) and owns the redb lock across
both commits. A successor's Activate must not invalidate pending 0.2.5 work
that it can still apply; if it cannot, the operator documentation must say so
before Activate (SFS-1018).

### Snapshot and GC

Destructive work runs only when the actor's maintenance capability matches the
volume policy. GC never deletes an object referenced by a checkpoint, artifact
or event that some supported reader still needs. A 0.2.5 Janitor that meets a
newer `payload_gc` stops before claiming. Snapshot cleanup must not delete an
artifact whose version it cannot decode.

### Offline clients

An offline client has no way to learn about Activate. On reconnect it
revalidates the policy epoch before its first publication and its first replay
batch. A client offline across Activate keeps its local work and fails closed
at publication or replay with a typed reason.

### Rollback limits

Rollback is supported only before Activate, or after Activate only until
Contract when the successor documents a deactivate step. After Contract there
is no rollback. Format 7 adoption is one-way: once a 0.2.x volume is upgraded,
0.2.0–0.2.4 binaries are fenced and are not supported again. Restoring a
PostgreSQL backup taken before Activate is a disaster-recovery action, not a
supported rollback, and loses events published after the backup.
