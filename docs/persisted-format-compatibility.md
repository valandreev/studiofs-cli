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
- Client support window: a metadata schema change must not break older clients
  immediately; operators get one minor line to upgrade. When minor line 0.N
  changes the schema or volume state, clients of the previous minor line
  0.(N-1).x keep working for all of 0.N. From 0.(N+1) on, support for 0.(N-1).x
  may be dropped. Example: a change in 0.4 keeps 0.3.x clients working; 0.5 or
  0.6 may refuse them. Patch releases within one minor line stay mutually
  compatible. For the 0.2 line the window starts at 0.2.5: 0.3 supports 0.2.5+
  clients, never 0.2.0–0.2.4.
- A supported older client may lose access to new features, but it must still
  mount, read (lookup, readdir, stat, read), write (create, write, truncate,
  rename) and delete (unlink, rmdir) as a full read-write client. A change that
  would make a supported client read-only or incompatible for these operations
  must not ship, or must stay behind an operator-activated feature that cannot
  be activated while such clients are within the window. Clients outside the
  window, and everything before 0.2.5, get no guarantee.
- Test only what this window needs. Do not build or test compatibility paths
  for releases outside it, or for hypothetical multi-format states that no
  supported release produces.
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
number into separate identities. SFS-1003 implemented the split in code
(`crates/studiofs-meta/src/version.rs`) without changing any value; see
"Version-bump rules" below. The line references in this audit describe `main`
before that change.

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
- Reader strictness (SFS-1006): mount startup runs `validate_postgres_schema`
  (`crates/studiofs-meta/src/pg_engine/startup_schema.rs`) per component, not
  against one global list of tables. `MOUNT_COMPONENTS` names the components
  a mount uses and the objects it touches:
  - `core`: the bootstrap, replay, publication, checkpoint and file-lock
    tables, `trash_entry_authority` and `trash_publication_receipts` for
    retained-entry publication and recovery, `snapshot_catalog` and
    `snapshot_source_checkpoints` (event log prune floor), and the
    `metadata_state` checkpoint, prune and `write_requirement` columns.
  - `payload_gc`: the payload upload and reachability ledger that publication
    writes.
  - `event_publication_fence`: the mirrored `metadata_state.publication_epoch`
    and the three triggers that initialize the epoch, synchronize policy changes,
    and fence inserts into `event_log`. A missing trigger or one disabled for
    ordinary writes requires schema repair before the mount can start.

  Each component's `studiofs_schema_components` row must exist and be at least
  its `min_supported`. A newer version or an unknown component is additive for
  a mount and is accepted. A missing object refuses the mount as
  `schema_component_outdated` naming the object, even under a newer version.
  If an object is missing and its recorded component is newer than the
  running binary, use a binary that supports that version or newer when
  running `studiofs upgrade --repair-schema`.
  A missing registry or row refuses the mount as `schema_component_missing`. These
  refusals reuse the `compat::ReasonCode` codes and actions and are permanent
  `Storage` errors. Only an unreachable PostgreSQL or a bounded timeout falls
  back to local-first startup; a rejected probe statement fails startup.
  Optional-feature tables are not checked at mount: snapshot creation jobs and
  leases, cleanup and mount leases, policies, artifacts and attempts, and
  Janitor sweep checkpoints. A missing one fails only that feature's
  statements. Snapshot ports still report this as a redacted retryable
  backend error. The Janitor requires `payload_gc` to equal exactly 2 and its
  guard triggers to be enabled
  (`crates/studiofs-meta/src/pg_engine/janitor.rs:430`).
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
- Migration registry (SFS-1005,
  `crates/studiofs-meta/src/pg_engine/migrations.rs`): `MIGRATIONS` lists
  numbered steps per component, each `additive`, `backfill` or `contract`.
  The `core` component's version 1 is the 0.2.5 baseline
  (`studiofs_pg_schema_sql`, checksum pinned by a unit test). It freezes at the
  0.2.5 release; until then 0.2.5 issues extend it (SFS-1007 added the volume
  protocol policy), and a development database recorded with an older baseline
  checksum is reconciled by `studiofs upgrade`. Every schema change after the
  release is a new step, and `POSTGRES_CORE_SCHEMA` tracks the latest
  `core` version. Each `studiofs_schema_components` row also stores a SHA-256
  chained over the component's steps, and a recorded history that differs
  from the registry is refused as `RepairRequired`. A recorded version newer
  than the binary knows, or an unknown component, is refused as
  `UpgradeRequired` (`schema_component_newer`) before any DDL. Each step
  commits with its marker under the existing advisory lock (`902202603`).
  `init` and `load` apply only pending additive steps and run no DDL against
  a verified schema; a pending backfill or contract step makes them refuse
  with `SchemaMigrationPending`, and a baseline whose checksum differs makes
  them refuse with `RepairRequired`. `studiofs upgrade` (`--dry-run` reports
  only) applies every step in its own transaction and resumes after a
  failure. While nothing is recorded above the baseline, it reconciles a
  differing baseline, and `--repair-schema` re-applies a verified one to
  restore dropped objects. It runs no DDL against a verified schema
  otherwise. Neither path checks the shape of tables that already existed;
  that is SFS-1006. The test successor adds `core` 2, one additive table.
- Fail-closed outcome: typed `UpgradeRequired` (unknown required component) or
  `MaintenanceForbidden` (Janitor); no DDL from an older binary. Owner:
  [SFS-1005](https://linear.app/valandreev/issue/SFS-1005) (migration registry)
  and [SFS-1006](https://linear.app/valandreev/issue/SFS-1006)
  (capability-scoped validation). A missing PostgreSQL column reaches the
  capability evaluator only as `schema_component_missing` or
  `schema_component_outdated` for its component, produced by SFS-1006 schema
  validation; the evaluator never inspects columns.

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
- Policy (SFS-1007): `volume_protocol` stores the required capabilities,
  feature states, checkpoint format and state schema, and policy epoch.
  Provisioning writes the baseline policy with checkpoint format 4 and
  schema 1. Schema reconciliation creates the shared tables. A legacy
  format 5 or 6 volume receives its policy during explicit adoption in
  SFS-1132. When `load` replaces an existing metadata state, it preserves the
  target policy and write requirement.
- Write requirement (SFS-1007): `metadata_state.write_requirement SMALLINT`,
  value 1 (`SHARED_WRITE_REQUIREMENT`). `BUMP_METADATA_VERSION` also requires
  `write_requirement <= $4`, where `$4` is the publisher's
  `SHARED_WRITE_REQUIREMENT.current`. Event and volume-settings publication
  both reread the two gates when no row matches and report
  `UnsupportedFormatVersion` or `UpgradeRequired`
  (`write_requirement_newer`) instead of a generic PostgreSQL error
  (`crates/studiofs-meta/src/pg_engine/volume_protocol.rs`).
- Successor may change: Activate raises the write requirement, which fences 0.2.5 writers and
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
- Envelope: seven magic bytes `SFS-EVT`, one wire version byte, one kind tag
  byte, then the exact postcard payload for that tag. The reader and writer
  both support wire version 1. The reader also accepts the baseline's
  untagged postcard ordinals 0 through 14.
- The append-only registry in `crates/studiofs-meta/src/events.rs` binds
  each tag to its payload and PostgreSQL kind. Tag 18 is journal-only.
  Retired snapshot tags 16 and 17 are permanently reserved.
- A new kind, field layout, or apply meaning requires a new tag, exact byte
  fixtures, a required read capability, and a defined rollback policy.
  Settings show the tag-per-version pattern. Tag 19 carries revision and
  trash, tag 20 adds quota, and tag 21 adds compression. The writer selects
  the oldest tag that preserves the requested policy.
- Unknown tagged wire versions, tags, and PostgreSQL kinds return
  `UpgradeRequired` with `UnsupportedEventReason`. An unknown ordinal in the
  frozen untagged format, a known PostgreSQL kind that disagrees with the
  tag, malformed or truncated bytes, and trailing payload bytes return
  `Serialization`.
  Replay decodes the complete page before opening the local apply
  transaction, so the durable cursor cannot cross a refused event. Snapshot
  reconstruction reports unsupported events as a permanent upgrade requirement.
- The compiled test successor reads tag 22 (`set_user_xattr`), an
  unconditional xattr assignment with a name relative to `user.`. The
  ordinary encoder in both builds emits tag 10 for the equivalent mutation.
  Publishing a successor tag requires an activated capability and the
  encoding selection from [SFS-1010](https://linear.app/valandreev/issue/SFS-1010).
- Publication negotiation (SFS-1010): the registry names each tag's read
  capability. The publisher validates its session against the policy epoch
  and checks every event's oldest faithful tag against the policy `read`
  set. A missing capability returns
  `VolumeProtocolRejected(EventNotRepresentable)` before any write and the
  local journal stays pending. Online settings changes apply the same check.
- Publication fencing (SFS-1014): the backend must check the authorized
  writer's epoch atomically with the metadata head advance and the full
  event batch append. A newer epoch or stale session leaves the head and
  event log unchanged. A future Redis backend must enforce the same contract.
- PostgreSQL mirrors the policy epoch on the `metadata_state` row so the
  version-bump predicate checks the epoch alongside the write requirement
  and format, including when the update waits for a concurrent activation.
  The event insert trigger checks the live volume session using transaction-local
  `studiofs.session_id`, `studiofs.policy_epoch`, and
  `studiofs.session_fencing_token`. It applies to format 7 and later or when
  a volume has a policy. Format 5 and 6 volumes without a policy remain
  writable by their existing clients in the same database. The trigger
  rejects outdated clients that omit the stamp. An operator issuing SQL
  with a forged valid stamp can bypass it, and a hand-written
  `UPDATE metadata_state` that advances the head without appending events
  is not fenced. Separate PostgreSQL roles are outside the scope of this
  fence.

### 6. Serving checkpoints

- The coordinator always writes the 0.2.5 format 4, schema 1 checkpoint to
  `metadata_state.snapshot`, with its SHA-256 and publication generation.
  The additive `checkpoint_encodings` migration adds
  `serving_checkpoint_encodings`, keyed by volume, byte format, state schema
  and checkpoint cut. `snapshot_source_checkpoints` retains separate copies
  for historical snapshot reconstruction.
- The `test-successor` build also publishes format 5, schema 1 in the same
  fenced lease transaction from the same redb read view. Its `SFS-CHK`
  envelope contains the encoding identities, volume ID and cut. Its SHA-256
  covers the entire envelope. Publication reclaims older copies of that
  format and schema after the replacement becomes available.
- Bootstrap observes the remote head, durable prune floor and the selected
  published checkpoint in one PostgreSQL statement. It chooses the newest
  compatible cut and prefers the newer encoding on a tie. Every supported
  client can decode the baseline slot, which remains the prune anchor.
  Retained snapshot sources can further limit pruning. Replay rechecks the
  floor under its shared advisory lock before fetching a page.
- Formats 1–4 use strict untagged postcard decoding with a bounded suffix.
  Tagged formats dispatch by explicit format and schema. The reader checks
  the declared range, descriptor identity and checksum before import. An
  unsupported encoding returns `UnsupportedCheckpointEncoding`. Bootstrap
  returns `NeedCompatibleCheckpoint` when it has no compatible cut covering
  the retained event range.

### 7. VolumeSettings

- Location: redb settings, checkpoint suffix, and settings events (tags 19–21).
  Type: `VolumeSettings` (`crates/studiofs-meta/src/trash.rs:209`).
- Fields: `revision`, `trash`, `logical_quota_bytes`, and `compression`
  (`crates/studiofs-meta/src/trash.rs:220`), a `CompressionPolicy` of mode
  `off` or `zstd` with level 1–19 (`crates/studiofs-meta/src/trash.rs:105`).
- Stored settings JSON rejects unknown fields in the settings object, trash
  policy, and compression policy. An unknown field or enum variant returns
  `UpgradeRequired` on CLI reads and before a compare-and-set can rewrite
  the record.
  Historical missing quota and compression fields still use their defaults.
  Quota-enabled records retain the `quota_v1_revision` spelling.
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
  The baseline write capability set includes STFS v2/v3 and SFEC v1.
  The successor fixture adds only an event read capability. Future payload
  encoders require their own write capability and activation before use.
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

Dump and load cover only volumes and dumps created by StudioFS 0.2.5 and later.

Back up first. Take a dump before you upgrade PostgreSQL metadata or restore a
volume. Stop all clients and janitors before you replace an existing volume.
Keep a copy of the original PostgreSQL database and the referenced object-store
data until you have verified the restored volume. Keep the dump private: it
contains volume connection and storage settings.

`studiofs dump` writes version 5 JSON from one PostgreSQL read snapshot. The
dump carries exactly one checkpoint: the published baseline slot in
`metadata_state`, plus the event tail after it. Alternate encodings in
`serving_checkpoint_encodings` are per-format caches that the checkpoint
coordinator republishes, so the dump leaves them out. The manifest records the
PostgreSQL migration-ledger revisions, the volume protocol policy, the format
and schema of that checkpoint, the event encodings in the tail, and the source
redb layout revision. If the baseline slot is not published yet, `dump` refuses
and asks you to mount the volume read-write until the coordinator publishes a
checkpoint.

`studiofs load` validates the whole manifest and its payload before it opens a
PostgreSQL transaction:

- A dump older than version 5 comes from StudioFS 0.2.4 or earlier and is
  refused. Load it with the 0.2.4 binary, run `studiofs upgrade`, then dump
  again with StudioFS 0.2.5 or later.
- If any recorded version is newer than this binary supports, load refuses,
  names the component, and asks you to upgrade studiofs.
- An older supported dump is loaded. The SFS-1005 migration registry prepares
  the target schema. The volume, policy, checkpoint, and replay tail are then
  published in one transaction, so a failed load publishes nothing and can be
  retried. A committed schema migration can remain after a failed load. Object
  data stays in place.

Snapshot mounts check their artifact format and schema independently of the
serving checkpoint used for live mounts.

### 12. Janitor and GC state

- Location: `snapshot_payload_upload_objects`, `snapshot_payload_gc_candidates`,
  `snapshot_payload_sweep_checkpoints`, `snapshot_payload_upload_watermarks`
  (`crates/studiofs-meta/src/pg_engine/schema.rs:543` onward), versioned as the
  `payload_gc` component (SFS-980).
- Existing fences: `fence_maintenance` locks `metadata_state`, checks the
  volume policy and sets the transaction-local maintenance epoch and capability
  GUCs before destructive SQL. PostgreSQL checks them on payload deletion
  claims, snapshot cleanup lease acquisition and renewal, replay and serving
  checkpoint deletes, and replay floor advances. The newly guarded operations
  accept pre-fence workers under the baseline maintenance policy. An expanded
  maintenance requirement fences those workers for that volume. The guards
  leave foreground mount, read, write and delete behavior for in-window
  clients governed by their existing policies. CLI and mounted Janitors require
  the recorded `payload_gc` component version and enabled deletion guards.
- A change to required maintenance capabilities returns a retryable policy
  refusal while a Janitor holds the per-volume lock or a snapshot cleanup or
  payload deletion lease remains live. Expired leases and owners without a
  live lease permit activation. The epoch fence applies when work is claimed
  or renewed. A cancelled or timed-out S3 DELETE sent before activation can
  still reach the object store afterward because the request can outlive its
  lease. Its target was proven unreachable under the old policy. Compatible
  checkpoint publication can retain superseded encodings until an authorized
  worker can prune them.
- Successor may change: raise `payload_gc`, which fences 0.2.5 Janitors through
  the exact-version probe. That is fail-closed but blocks GC on every volume in
  the database until all Janitors are upgraded.
- Fail-closed outcome: `MaintenanceForbidden` on a new claim or renewal
  under incompatible policy. Owner:
  [SFS-1016](https://linear.app/valandreev/issue/SFS-1016).

## Existing mechanisms to extend, not duplicate

Later issues must build on these instead of adding parallel mechanisms:

| Mechanism | Source | Extend in |
|---|---|---|
| `studiofs_schema_components` with monotonic marker (SFS-980) | `crates/studiofs-meta/src/pg_engine/schema.rs:903` | SFS-1005, SFS-1006 |
| `studiofs upgrade` (format 5→6→7 or 6→7) | `crates/studiofs/src/cli/upgrade.rs`, `crates/studiofs-meta/src/pg_engine/format_upgrade.rs` | SFS-1132 |
| `BUMP_METADATA_VERSION` format and write-requirement predicates on publication | `crates/studiofs-meta/src/pg_engine/sql.rs:51` | SFS-1014 |
| Per-volume policy record `volume_protocol` with epoch, audit history and `check_update` transition rules | `crates/studiofs-meta/src/volume_protocol.rs`, `crates/studiofs-meta/src/pg_engine/volume_protocol.rs` | SFS-1008, SFS-1015, SFS-1017, SFS-1132 |
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
  `metadata_format_version >= 6`). The registered `format_fences` migration
  (SFS-1132) adds triggers under names the 0.2.x DDL never touches, so an
  older `init` cannot replace them. `studiofs_deletion_claim_volume_format_fence`
  compares the actor's `studiofs.deletion_volume_format` with the format of a
  volume at format 7 or later, so a 0.2.4 Janitor, which sets only
  `studiofs.deletion_format = '6'`, loses deletion admission there.
  `studiofs_adopted_format_downgrade_fence` refuses writing a format below 7
  for a volume that has a `volume_protocol` row, so an older `load` cannot
  restore a format-6 row over an adopted volume.
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
| Publication checks a per-volume field that a successor can raise at Activate, in the same UPDATE as the version bump. | Present (SFS-1007): `BUMP_METADATA_VERSION` checks `write_requirement <= SHARED_WRITE_REQUIREMENT.current` beside the format predicate (`crates/studiofs-meta/src/pg_engine/sql.rs`); the read gate `metadata_format_version` is separate. | [SFS-1007](https://linear.app/valandreev/issue/SFS-1007) |
| A refused volume-settings publication reports a typed reason, like event publication. | Present (SFS-1007): both paths call `refused_publication` (`crates/studiofs-meta/src/pg_engine/volume_protocol.rs`), which returns `UnsupportedFormatVersion` or `UpgradeRequired` (`write_requirement_newer`). | [SFS-1007](https://linear.app/valandreev/issue/SFS-1007) |
| Every publication stamps its session and policy epoch so the backend can reject stale writers. | Implemented. The PostgreSQL version-bump predicate checks the stamped epoch and the event insert trigger verifies the current session and fencing token. | [SFS-1014](https://linear.app/valandreev/issue/SFS-1014), [SFS-1008](https://linear.app/valandreev/issue/SFS-1008) |
| Destructive maintenance sets a maintenance capability GUC before deleting. | Payload claims, snapshot cleanup and shared event/checkpoint pruning use the transaction-bound epoch and capability check. | [SFS-1016](https://linear.app/valandreev/issue/SFS-1016) |
| Unknown required capabilities fail closed with a typed reason. | Present in the pure evaluator (SFS-1004): `compat::evaluate` refuses unknown required capabilities, unknown safety-critical optional ones, unknown events and payload formats with stable reason codes. Not wired into mount, replay or publication until the volume policy and enforcement land. | [SFS-1004](https://linear.app/valandreev/issue/SFS-1004) |
| Unknown event tags fail closed with a typed reason. | Present: replay preserves `UpgradeRequired` with `UnsupportedEventReason` and leaves the durable cursor at its last applied position. | [SFS-1009](https://linear.app/valandreev/issue/SFS-1009) |
| Unknown checkpoint formats fail closed with a typed reason. | Tagged serving checkpoints and retained sources report unsupported format or schema separately from invalid bytes or a missing compatible cut. | [SFS-1011](https://linear.app/valandreev/issue/SFS-1011) |
| Unknown payload object formats fail closed with a typed reason. | Present: `UnsupportedVersion` and `UnsupportedCodec` (`crates/studiofs-data/src/object.rs:325`, `crates/studiofs-data/src/object.rs:160`); SFEC version and flags are strict. | [SFS-1009](https://linear.app/valandreev/issue/SFS-1009) (write gating) |
| Unknown local formats fail closed with a typed reason. | Partial: payload index format is typed (`crates/studiofs-cache/src/payload_index.rs:1299`), entry encoding is `Corruption` (`crates/studiofs-cache/src/payload_index.rs:1790`); redb has no own identity. | [SFS-1012](https://linear.app/valandreev/issue/SFS-1012), [SFS-1003](https://linear.app/valandreev/issue/SFS-1003) |
| Additive PG objects are tolerated when declared capabilities allow it. | Present for mount startup: `validate_postgres_schema` checks only the components and objects used by the mount (`crates/studiofs-meta/src/pg_engine/startup_schema.rs`). DDL from `init`/`load`/`upgrade` goes through the SFS-1005 migration registry, which refuses newer or unknown components before any DDL. | [SFS-1006](https://linear.app/valandreev/issue/SFS-1006) |
| Policy epoch is revalidated after reconnect. | Partial: the format and write-requirement predicates are re-evaluated in every publication transaction; the policy epoch exists (`volume_protocol.policy_epoch`, SFS-1007) but no client caches or compares it yet. | [SFS-1015](https://linear.app/valandreev/issue/SFS-1015), [SFS-1007](https://linear.app/valandreev/issue/SFS-1007) |
| Unknown `MetadataBackend` variant or unknown `FormatInfo` field fails closed with typed `UpgradeRequired`. | Present (SFS-1003): `decode_format_info` returns `MetaError::UpgradeRequired` naming the field or variant, including nested configs, and the bootstrap descriptor keeps it instead of reporting an invalid checkpoint. | [SFS-1003](https://linear.app/valandreev/issue/SFS-1003) |
| 0.2.x volumes adopt the baseline by `studiofs upgrade` to format 7. | Pending migrations run before policy initialization and the format bump. The stopped client then upgrades its local stores in place. | [SFS-1132](https://linear.app/valandreev/issue/SFS-1132) |

## Version and operation support matrix

Rows are release pairs: the first release owns the persisted state or has
activated the volume; the second is the binary acting on it. "Successor" is
the compiled test successor
([SFS-1131](https://linear.app/valandreev/issue/SFS-1131)) or 0.2.7.

Outcome names map onto the capability evaluator
(`crates/studiofs-meta/src/compat.rs`): `MaintenanceForbidden` is a refused
maintenance operation (reason `maintenance_capability_missing`,
`schema_component_newer` or `schema_component_unknown`). `LocalUpgradeRequired` is `UpgradeRequired` with
`local_format_newer` or `local_format_unknown`, or `NeedsMigration` with
`local_format_outdated` for a local store older than the binary opens.

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

### Version-bump rules

The identities live in `crates/studiofs-meta/src/version.rs` (SFS-1003). Each
is a `VersionIdentity` with the version this binary writes (`current`) and the
oldest it reads (`min_supported`); `check` refuses anything outside that range
with the typed `UnsupportedFormatVersion`. The version to change depends on the
persisted boundary:

| Change | Identity to bump | Not bumped |
|---|---|---|
| Shared volume layout or the `metadata_state` protocol all writers must speak (only the SFS-1132 6→7 adoption; from format 7 on, use capabilities) | `SHARED_VOLUME_FORMAT` (`FormatInfo.format_version`, `metadata_format_version`). Keep `LOCAL_METADATA_LAYOUT` in sync while both use that field. | codecs |
| Key or value encoding of an existing local redb table | With the dedicated marker and migration from SFS-1012, bump `LOCAL_METADATA_LAYOUT`. Before that, defer an independent local layout change. | shared format once the local marker exists |
| New `MetaEvent` wire envelope | `EVENT_CODEC_WIRE`; a new event kind is a new append-only tag instead | shared format |
| New serving checkpoint suffix | `SERVING_CHECKPOINT_FORMAT` (keep older formats in range) | shared format |
| Serving-state schema change | `SERVING_CHECKPOINT_SCHEMA` | checkpoint format |
| Additive PostgreSQL table, column or index | a new step in `pg_engine::migrations::MIGRATIONS`, which raises its component (`POSTGRES_CORE_SCHEMA` for `core`); `POSTGRES_PAYLOAD_GC_SCHEMA` only when the Janitor's GC schema changes | every identity above |
| Payload index, snapshot artifact, dump, STFS, SFEC | the owner constant listed in `version.rs` | every identity above |

Today the local redb header is still the `FormatInfo` copy in
`SETTINGS_TABLE["format"]`, so both checks read `FormatInfo.format_version`.
Local header reads use `LOCAL_METADATA_LAYOUT`. Shared `FormatInfo`, the
descriptor, `ensure_supported_pg_sync_format_version`, and publication use
`SHARED_VOLUME_FORMAT`. A compile-time assertion keeps both current versions
and minimum supported versions equal until SFS-1012 adds a dedicated marker.

`CLIENT_RELEASE` (the binary's `CARGO_PKG_VERSION`) is informative only. It
appears in diagnostics and, later, in session records; it is never a storage
compatibility gate.

`FormatInfo`, `MetadataConfig`, `PostgresConfig`, `RemoteStoreConfig`, and
`EncryptionConfig` keep `deny_unknown_fields`. `decode_format_info` reports
an unknown field or enum variant (for example a new `MetadataBackend`) as
`MetaError::UpgradeRequired` with the schema identifier when it meets the
diagnostic redaction rules. Invalid types and malformed values are decoding
errors. A new optional field must use `#[serde(default, skip_serializing_if = ...)]` so a
volume that does not use it keeps byte-identical `FormatInfo` and stays
readable by 0.2.5. A field or variant that a volume needs (such as the 0.2.7
Redis backend) is a required capability: writing it fences 0.2.5 readers with
`UpgradeRequired`, which is the intended outcome.

### Test successor

The compiled test successor (SFS-1131) is `studiofs` built with the
test-only `test-successor` Cargo feature. It plays the newer release in the
forward-compatibility matrix (SFS-1019, SFS-1020). Each format issue adds its
change in the owning code behind `#[cfg(feature = "test-successor")]` and
registers one `SuccessorChange` in `CHANGES` in
`crates/studiofs-meta/src/test_successor.rs`. The entry records the change
kind (`postgres_migration`, `event_kind`, `checkpoint_format`, or
`local_format`), the rollback class (`reversible` or `irreversible`), and the
identity bump, if any. A crate other than `studiofs-meta` forwards the feature
from its own manifest and the `studiofs` feature list. The successor only adds
behavior. It never changes a baseline encoder.

`studiofs internal version-identities` prints the identities from
`version::IDENTITIES` and the declared changes as JSON. An identity that a
successor change bumps must appear in that list. `baseline` 0 marks an
identity that the baseline does not report. Each identity has at most one
bump; changes that share an identity declare one combined bump.
`python3 tools/test_successor/build.py` builds the baseline and the successor
(`target/test-successor/`). It diffs both reports and fails unless every
`current` difference is a declared bump and every `min_supported` value is
unchanged. It writes `target/test-successor/manifest.json` with the source
commit, both binary SHA-256 values, both identity lists, and the declared
changes. `--require-complete` also requires one change of each kind, plus
one reversible change and one irreversible change. SFS-1019 runs that check
before the matrix.

Release builds cannot contain the successor. The feature raises a
`compile_error!` without `debug_assertions`, and the Linux quality gate checks
that refusal. `tools/release/package.py` also rejects a binary that embeds the
successor marker `STUDIOFS-TEST-SUCCESSOR-BUILD`.

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

Because an expired session lease in `client_sessions` does not prove that an
offline client has no unpublished local work, `studiofs upgrade NAME --feature CAP --phase activate`
treats expired unsupported sessions as blockers by default unless the operator
explicitly passes `--accept-offline-clients`.

### Explicit feature cutover (`studiofs upgrade --feature`)

Operators drive per-volume Expand–Migrate–Activate–Contract transitions through
`studiofs upgrade <name> --feature <cap> --phase <expand|migrate|activate|deactivate|contract>`:

- `--dry-run` inspects the volume policy, transition epochs, rollback boundary,
  eligible and blocking active sessions, expired unsupported sessions, whether
  a published serving checkpoint exists for the policy's (unchanged) checkpoint
  descriptor, and snapshot/GC state, without mutating anything. It may run
  while the volume is mounted; every other cutover requires it unmounted.
- `expand` (`available`) and `migrate` (`compatible`) keep 0.2.5+ clients writable.
- `activate` (`required`) atomically checks readiness under the `volumes` and
  `metadata_state` row locks used by publication, raises `write_requirement`
  when gating a write capability, and fences stale writers.
- `deactivate` rolls back `required -> compatible` or `compatible -> available`
  before `contract`. It removes every role and refuses `--role`.
- `contract` (`retired`, terminal) is per volume, runs no DDL and is allowed
  only from `available`, so a feature in use must be deactivated first;
  `deactivate` is refused after `retired`.
- The database-wide check gates the `MigrationPhase::Contract` DDL step: it is
  refused while any volume in the database lacks a policy or still has the
  feature `compatible`, `required` or in a required set.
- Each applied transition is recorded in `volume_protocol_history` with the
  command (feature, phase, roles, write requirement and
  `--accept-offline-clients`) as the actor.

### Rollback limits

Rollback is supported only before Activate, or after Activate only until
Contract when the successor documents a deactivate step. After Contract there
is no rollback. Format 7 adoption is one-way: once a 0.2.x volume is upgraded,
0.2.0–0.2.4 binaries are fenced and are not supported again. Restoring a
PostgreSQL backup taken before Activate is a disaster-recovery action, not a
supported rollback, and loses events published after the backup.

## Operator-facing migration rules

Operators check a volume with `studiofs compatibility <volume>` and
`studiofs upgrade <volume> --dry-run` before they upgrade
([user guide chapter 32](user-guide/en/32-upgrades-and-version-compatibility.md)).
Every change to a persisted boundary keeps those two reports accurate:

- Every new `ReasonCode` needs an `action()` text an operator can follow. The
  report prints that text as is.
- Every migration (a `pg_engine::migrations` step, a local layout or format
  bump) must be visible in `studiofs upgrade --dry-run`: as a pending step, or
  as a `Volume format` or `Layout` line. It must also change the evaluator
  result that `studiofs compatibility` prints. A migration visible in neither
  is a bug.
- The compatibility lines in `studiofs status` stay local only: they add no
  PostgreSQL or object-store call and no full scan. Shared checks belong to
  `compatibility` and `upgrade --dry-run`.
- Reports never print connection data or secrets: no DSN, password, token, key,
  or S3 credential. `FormatInfo` must not reach a `Debug` line, an error, or a
  report. Map backend errors to a fixed operation message, as
  `read_shared_facts` does.
- Reports are read-only and open local redb read-only. A mount holds the redb
  lock, so `compatibility` refuses with an explicit "unmount first" message on
  the mounting computer; other computers see the mount as a session.
- The report decides operations with `VolumeProtocol::compatibility_decision`
  on the baseline requirements and the serving checkpoint encodings, as mounts
  do. A refusal of the whole volume (newer schema or policy, missing policy,
  outdated local format) is reported before any operation is evaluated.
- Known duplication to remove: the write-requirement-newer refusal is checked
  in `VolumeProtocol::check_operations` and again as a report override in
  `cli/compatibility.rs`. Move it into `compatibility_decision` with its own
  `ReasonCode` when that reason is added to `compat.rs`.
