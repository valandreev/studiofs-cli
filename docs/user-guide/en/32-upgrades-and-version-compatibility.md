# 32. Upgrades and Version Compatibility

A volume is shared by every computer that mounts it, so one computer's StudioFS version can affect the others. This chapter explains how to check before you upgrade, how to read the report, and what to do when a computer cannot use a volume.

Two rules apply to every upgrade:

- Check first. Run the read-only report on each computer before you change anything.
- Never reinitialize a volume or clear its local cache to "fix" an upgrade. Local cache holds changes that may not be on the shared volume yet.

Supported versions: StudioFS 0.2.5 and later. Clients older than 0.2.5 are not supported and cannot safely share a volume with 0.2.5. StudioFS 0.3.0 upgrades only from 0.2.5 or later.

## Check before you upgrade

Run these commands on each computer that uses the volume. Neither one changes anything, and a volume that is mounted can stay mounted:

```bash
studiofs compatibility myvolume
studiofs upgrade myvolume --dry-run
```

`studiofs compatibility` reads the local metadata and the shared PostgreSQL metadata. It tells you what this version of StudioFS can do with the volume. `studiofs upgrade --dry-run` shows the same report after the list of schema migrations that an upgrade would apply.

Both commands may take up to 45 seconds. If they time out, nothing was changed. Run them again.

## Read the report

The report has six parts. This example shows a volume that requires a writer this computer does not have, a client left on an older rule epoch, and two unpublished local changes:

```text
Client
  Build:              0.2.5
  Volume format:      7..=7
  Write requirement:  up to 1
Volume
  Local format:       7
  Shared format:      7
  Policy:             epoch 3, write requirement 1 (publishes up to 1)
Operations
  bootstrap    allowed
  replay       allowed
  publication  blocked: write_capability_missing (table.future_writer.v1) — upgrade StudioFS to a release that supports this volume
  checkpoint   blocked: write_capability_missing (table.future_writer.v1) — upgrade StudioFS to a release that supports this volume
  maintenance  allowed
Sessions
  active 1, expired 0
  3f2a9c1e build 0.2.4 epoch 1: stale epoch 1 (current 3); missing capabilities: write:table.future_writer.v1 — upgrade or unmount that client
Local
  Layout:             6
  Unpublished events: 2
  Local version:      8 (shared head 10)
  Unsafe pending:     yes; these changes are not on the shared volume yet
Overall: compatible_ro
  reason: write_capability_missing (table.future_writer.v1)
Actions
  - upgrade StudioFS to a release that supports this volume
  - upgrade or unmount that client
  - do not reinitialize or clear this cache; mount it with a compatible release so it publishes, or keep it until upgraded
```

What each part tells you:

- **Client** is this computer's StudioFS version and the range of volume formats it supports.
- **Volume** shows the format on this computer's local cache and on the shared volume. *Policy* is the rule set the volume follows. *Epoch* goes up by one each time the rules change.
- **Operations** lists what this computer may do: `bootstrap` (start serving the volume), `replay` (apply other computers' changes), `publication` (send your changes to the shared volume), `checkpoint`, and `maintenance` (cleanup and deletion). A blocked line gives the reason and the action.
- **Sessions** lists the other computers that are currently connected. A session is flagged when it is on an older rule epoch or lacks a capability the volume now needs.
- **Local** shows changes this computer has made that are not yet on the shared volume.
- **Overall** is the summary, and **Actions** is what to do next.

### Overall values

| Overall | Meaning |
|---|---|
| `compatible_rw` | This computer can mount, read, and publish changes. |
| `compatible_ro` | This computer can mount and read, but its changes cannot be published yet. Its local work waits. |
| `needs_migration` | The volume or this cache must be upgraded with `studiofs upgrade` first. |
| `upgrade_required` | This StudioFS version is too old for something on the volume. Install a newer release. |
| `incompatible` | The volume predates every supported release. Restore it with the release that wrote it. |
| `unknown` | The shared volume record could not be found. Check the PostgreSQL connection and the volume name. |

### The four explicit actions

The report always gives one of these actions. Follow the one that matches:

| If the report says | Do this |
|---|---|
| A missing feature, such as `write_capability_missing` or `read_capability_missing` | Install a StudioFS release that supports the volume. Do not mount with an older release. |
| `needs_migration`, `volume_format_needs_migration`, `schema_component_missing`, or `local_format_outdated` | Stop every mount of the volume, then run `studiofs upgrade myvolume` on this computer. See the upgrade steps below. |
| A stale session, or a session that lacks a capability | Upgrade that computer, or unmount it. Until then, its changes may not be accepted. |
| Unsafe local pending data (unpublished events above zero) | Do not reinitialize or clear this cache. Mount it with a compatible release so it publishes. If you cannot do that yet, keep the cache until this computer is upgraded. |

## Upgrading a volume from 0.2.x to 0.2.5

Volumes written by StudioFS 0.2.0 to 0.2.4 use format 5 or 6. They must be adopted into the 0.2.5 format before 0.2.5 can mount them. Plan this as a short, planned change:

1. Back up the PostgreSQL database, for example with `pg_dump`. Back up the stopped metadata file and cache folder of every computer that uses the volume. The `studiofs status myvolume` output shows the paths: the `Metadata` line gives the metadata file, and the `Root` line under `Local Cache` gives the cache folder. Copy both while nothing is running.
2. Stop every mount and every janitor of the volume, on every computer. `studiofs upgrade` refuses to run while the volume is mounted.
3. On each computer, run the check, then the upgrade:

```bash
studiofs upgrade myvolume --dry-run
studiofs upgrade myvolume
```

The upgrade applies any pending schema migrations and writes the new format. It prints one line for each migration and then a line such as:

```text
Volume 'myvolume' now uses format 7. Repeat upgrade on every other stopped cache before remounting.
```

Do the same on every other computer's stopped cache before you mount again. An older computer that is not upgraded has its publication refused by the shared volume. Its unpublished changes stay in its local cache until it is upgraded.

Schema upgrades affect the whole PostgreSQL database, not just this volume. The `payload_gc` cleanup schema moves to version 2, and older janitors then stop working for every volume in that database. Upgrade the janitor for every volume in the same database before you rely on cleanup. The upgrade does not turn retention on.

### If the upgrade is interrupted

Run the same `studiofs upgrade myvolume` command again. A migration that already committed stays applied, and a failed one is rolled back. Your accepted local data is kept. Do not reinitialize the volume or clear its cache. If the command says it cannot own the local metadata, a process still has the file open: stop it and retry.

## Staged rollout of new clients

Clients from 0.2.5 onward can share a volume during a rollout. Upgrade one computer at a time and check the report after each step:

1. Install the new release on one computer. Run `studiofs compatibility myvolume`. Confirm `Overall` is still `compatible_rw`.
2. Mount and use the volume there. Watch that the other computers still show the sessions as `ok`.
3. Upgrade the remaining computers in the same way.

A feature that needs every computer to upgrade first is not activated until you run the cutover steps below. Until then, computers on 0.2.5 and later keep working together.

## Planning a feature activation

Some newer features need an explicit cutover, in four phases: expand, migrate, activate, and contract. Each phase has its own dry run, and you run the dry run first:

```bash
studiofs upgrade myvolume --feature <feature-name> --phase activate --dry-run
```

The dry run lists the blocking sessions, the unpublished local work, and the rollback boundary. Do not run the real cutover until the dry run shows no blockers: upgrade or unmount each blocking computer first, and let each one publish its unpublished changes. The `--accept-offline-clients` option is an explicit decision for an offline computer that lacks the feature. That computer is fenced when it reconnects, and any changes it still holds unpublished cannot be sent to the volume.

## Interrupted or stuck upgrades

If an upgrade does not finish, do not reinitialize the volume and do not delete the cache. Check the report, stop any process that still has the metadata open, and rerun the same command. For a cutover that stopped after its dry run, rerun the dry run first, because the state may have changed.

## What is not supported

- Clients older than 0.2.5 cannot mount or publish to a volume that 0.2.5 uses. Upgrade them before you rely on the volume.
- Going back to an older release after an upgrade is not supported. Restore a PostgreSQL backup only as a disaster-recovery step, and expect to lose changes made after that backup.
- StudioFS 0.3.0 upgrades only from 0.2.5 or later. Upgrade to 0.2.5 first.

## Status is local only

`studiofs status myvolume` ends with a short compatibility section. It reads only this computer's local metadata and never calls PostgreSQL or the object store, so it stays fast while the volume is busy:

```text
Compatibility (local only)
──────────────────────────
  Volume format:       7 (this client supports 7..=7)
  Unpublished events:  2
  Shared check:        run `studiofs compatibility myvolume`
```

For the shared view, run `studiofs compatibility myvolume`.
