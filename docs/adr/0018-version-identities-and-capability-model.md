# Persisted Formats Use Separate Version Identities And A Per-Volume Capability Policy

Status: Accepted (SFS-1001). Implementation is tracked in the Linear project
"[0.2.5] Metadata Compatibility Layer". The boundary audit and the support
matrix are in
[persisted-format-compatibility.md](../persisted-format-compatibility.md).

## Context

StudioFS uses one number, `CURRENT_FORMAT_VERSION`
(`crates/studiofs-meta/src/schema.rs:46`, value 6), for the local redb header,
the shared `FormatInfo`, the PostgreSQL `metadata_format_version` column, the
bootstrap checkpoint descriptor, and the publication predicate. Every check is
exact equality. A harmless additive change and an incompatible change look the
same, so the only safe move today is to bump the number and lock out every
older reader and writer at once.

Other boundaries already carry their own versions: event wire version and tags,
serving checkpoint format 4, STFS v2/v3, SFEC v1, payload index format 2 and
entry encoding 3, snapshot artifact format 1, dump version 4, and the
`payload_gc` schema component. They are checked independently and nothing ties
them to a volume's policy.

0.2.5 is the baseline. 0.2.7 adds a Redis-compatible metadata backend and is
the first real successor. The 0.2.5 binary cannot be patched later, so it must
carry every check a successor needs to fence it.

## Decision

### Version identities

Each persisted boundary gets its own identity. A successor changes only the
identity it touches.

| Identity | Today | Owner of the split |
|---|---|---|
| Client release | binary version, not persisted | SFS-1003 |
| Shared metadata schema (backend schema components) | `studiofs_schema_components`, only `payload_gc` | SFS-1005 |
| Volume protocol (what writers must speak) | shared `metadata_format_version` | SFS-1003, SFS-1007 |
| Local metadata (redb layout) | shared format number | SFS-1003, SFS-1012 |
| Event codec | wire version 1 plus append-only tags | SFS-1009 |
| Checkpoint | serving checkpoint format 4, schema 1 | SFS-1011 |
| Remote payload object | STFS v2/v3, SFEC v1, codec byte | SFS-1009 |
| Local payload index | index format 2, entry encoding 3 | SFS-1012 |
| Dump | `dump_version` 4 | SFS-1013 |
| Snapshot artifact | format 1, schema 1 | SFS-1013 |

Format 7 is the shared format number that `studiofs upgrade` writes when it
adopts a 0.2.x volume (SFS-1132). From format 7 on, the format number names
the baseline layout only; later changes move the identities above, not the
format number.

### Capabilities

A capability is a named, versioned feature, for example
`event.volume_settings.v4` or `checkpoint.v5`. Each binary declares the
capabilities it can read, the capabilities it can write, and the
capabilities it can maintain (delete or prune data for). Each volume declares:

- required-for-read: a binary missing any of these must not mount or replay;
- required-for-write: a binary missing any of these must not publish;
- required-for-maintenance: a binary missing any of these must not delete,
  prune, or run GC.

Compression (STFS v3, 64 KiB SFEC frames, `VolumeSettings.compression`) is
part of the 0.2.5 baseline set and is not a separate capability (ADR 0015).

A capability is optional for a reader when the data it adds can be ignored
without changing what the reader serves, for example a new table that only
the successor reads. Optional capabilities never block an older binary.

### Evaluator

One evaluator (SFS-1004) decides every operation. Its inputs are the binary's
declared capability sets, the volume policy record, the schema components
present in the shared backend, and the local identities read from redb and the
payload index. Its output is a decision per operation (read, write, maintain,
snapshot, dump/load) with a typed reason: `Allowed`, `UpgradeRequired`
(this binary is too old), `MaintenanceForbidden`, or `LocalUpgradeRequired`
(a local store needs migration). The evaluator is a pure function so it can be
tested without a backend. Unknown names in any required set are treated as
missing; the decision never falls back to `Allowed`.

Boundary decoders report their own typed errors (an unknown event tag, an
unknown checkpoint format, an unsupported dump version, an unknown
`FormatInfo` field); these map into the evaluator's `UpgradeRequired`. A refusal
of the local redb or payload index format maps into `LocalUpgradeRequired`.

### Per-volume policy and policy epoch

Each volume has one policy record in the shared backend holding the three
required sets and a policy epoch, a counter that increases on every change
(SFS-1007). Changing the record is the Activate step of an
Expand–Migrate–Activate–Contract cutover (SFS-1017):

1. Expand adds optional schema objects and capabilities. Older binaries keep
   working.
2. Migrate rewrites or backfills data while both formats are readable.
3. Activate adds the new capability to a required set and bumps the epoch in
   the same transaction that raises the per-volume write-requirement field
   (SFS-1007). Publication checks that field in the same UPDATE as
   `BUMP_METADATA_VERSION`. Activate never raises `metadata_format_version`:
   readers check it by exact equality, so raising it would lock out 0.2.5
   readers too. From format 7 on that field stays fixed.
4. Contract removes what no supported binary needs. Contract on a database-wide
   object needs agreement from every volume in the database.

### Sessions

Each mounted client and each maintenance actor registers a session with its
client release, declared capabilities, the policy epoch it validated, and a
lease that expires on the server clock (SFS-1008). This extends the existing
lease pattern of `bootstrap_checkpoint_leases`. Activate reports sessions that
lack the new capability and refuses unless the operator accepts that they will
be fenced.

### Fencing

Fencing is enforced by the backend, not by trusting the client.

- Every publication stamps its session and policy epoch, and checks the
  write-requirement field in the same atomic step that advances the volume
  version. In PostgreSQL this is the `BUMP_METADATA_VERSION` predicate, extended
  with the write-requirement field, plus a
  transaction GUC read by a trigger (SFS-1014), following the existing
  `studiofs.deletion_format` pattern.
- Every destructive maintenance step first declares its maintenance capability
  in the same transaction; a backend guard rejects deletion without it
  (SFS-1016).
- After any reconnect the client revalidates the policy epoch before its first
  publication or replay batch (SFS-1015).

The 0.2.7 adapter must provide the same guarantees with its own primitives:
an atomic conditional version advance and a per-operation capability stamp
that a server-side check can reject.

## Consequences

- An additive change no longer locks out older binaries. Only an activated
  required capability does, and the refusal is typed.
- The 0.2.5 binary must ship the evaluator, the policy read, the session
  registration, the publication stamp, the maintenance stamp, typed
  `UpgradeRequired` for unknown `FormatInfo` fields, backend variants, event
  tags, checkpoint formats, and payload formats, and typed
  `LocalUpgradeRequired` for unknown local redb and payload index formats. Gaps
  found in the audit are listed there with owners.
- Pre-0.2.5 volumes are adopted only by `studiofs upgrade` to format 7, which
  fences 0.2.0–0.2.4 writers. Those releases are not part of any support
  matrix.
- Database-wide DDL becomes a coordinated operation: an older binary must not
  run DDL against a database whose schema components are newer than it knows.
- Rollback is possible only before Activate, or before Contract when the
  successor documents a deactivate step.
- 0.3.0 upgrades start from 0.2.5 or later; its upgrade policy stays with
  SFS-982 through SFS-985.
