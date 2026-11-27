# 32. Upgrades and Version Compatibility

A volume is shared by every computer that mounts it, so one computer's StudioFS version can affect the others. This chapter explains how to check before you upgrade, how to read the report, and what to do when a computer cannot use a volume.

Two rules apply to every upgrade:

- Check first. Run the read-only report on each computer before you change anything.
- Never reinitialize a volume or clear its local cache to "fix" an upgrade. Local cache holds changes that may not be on the shared volume yet.

The support window starts at StudioFS 0.2.5. When a minor line changes the schema, clients on the previous minor line retain read-write access throughout the new line. Only 0.2.5 and later patches count in the 0.2 line. StudioFS 0.3.0 upgrades only from 0.2.5 or later. See the [release support matrix](../../persisted-format-compatibility.md#version-and-operation-support-matrix).

## Operator checklist

1. **Inventory.** Record the binary, metadata file and cache path on each computer, including offline ones. Identify every volume and Janitor that uses the same PostgreSQL database.
2. **Preflight.** Run `studiofs compatibility <volume>` and `studiofs upgrade <volume> --dry-run`. Check the session list, pending migrations and unpublished local work. Keep caches with unpublished work and let compatible clients publish before cutover.
3. **Back up.** Before a schema change or activation, stop affected mounts and Janitors. Take a consistent PostgreSQL backup and copy each stopped local redb metadata file and its payload cache. Retain access to the corresponding S3 objects and the binary needed to restore that backup.
4. **Roll out in stages.** Start with one client and verify `compatible_rw` and normal file operations before advancing. For a legacy format 5 or 6 volume, follow the [adoption procedure](#upgrading-a-volume-from-02x-to-025) before any client remounts.
5. **Plan activation.** Run the feature dry run, resolve active and expired session blockers and check the rollback boundary. Keep breaking features inactive while the support window protects affected clients. See [Planning a feature activation](#planning-a-feature-activation).
6. **Set the rollback boundary.** Follow the successor's documented deactivation procedure when it can preserve compatible persisted state. Format-7 adoption, irreversible activation and Contract have no supported downgrade. Recover from a consistent backup only as a disaster-recovery action.

## Check before you upgrade

Run these commands on each computer that uses the volume. Neither one changes anything. Both need the volume unmounted on the computer where you run them: a mount holds the local metadata lock, and the commands then tell you to unmount instead of reading. Other computers can keep the volume mounted; they show up under *Sessions*.

```bash
studiofs compatibility myvolume
studiofs upgrade myvolume --dry-run
```

`studiofs compatibility` reads the local metadata and the shared PostgreSQL metadata. It tells you what this version of StudioFS can do with the volume. `studiofs upgrade --dry-run` shows the same report after the list of schema migrations that an upgrade would apply.

Both commands may take up to 45 seconds. If they time out, nothing was changed. Run them again.

## Read the report

The report has seven parts. This example shows a volume that requires a writer that neither this computer nor another connected computer has, an expired session, and two unpublished local changes:

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
  active 1, expired 1
  3f2a9c1e build 0.2.5 epoch 1: missing capabilities: write:table.future_writer.v1 — upgrade or unmount that client
  5b7d0a42 build 0.2.5 epoch 2: expired
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
- **Sessions** lists every computer that has connected to the volume, with its StudioFS build. Use it to see which releases share the volume. A session that lacks a capability the volume needs gets an action. A capable session on an older rule epoch shows `stale epoch (current N); refreshes on its next heartbeat` and needs nothing from you. An `expired` session is not connected now.
- **Local** shows changes this computer has made that are not yet on the shared volume. `Layout` is `unknown` on a cache that still needs `studiofs upgrade`.
- **Overall** is the summary, and **Actions** is what to do next.

### Overall values

| Overall | Meaning |
|---|---|
| `compatible_rw` | This computer can mount, read, and publish changes. |
| `compatible_ro` | This computer can mount and read, but its changes cannot be published yet. Its local work waits. |
| `needs_migration` | The volume or this cache must be upgraded with `studiofs upgrade` first. |
| `upgrade_required` | This StudioFS version is too old for something on the volume. Install a newer release. |
| `incompatible` | The volume predates every supported release, or its rule record is gone (`volume_policy_missing`). Do not reinitialize anything. Restore the PostgreSQL backup, or restore the volume with the release that wrote it. |
| `unknown` | The shared volume record could not be found. Check that this cache points at the right PostgreSQL database and volume; `studiofs discover` lists the volumes there. |

### The four explicit actions

When something needs attention, the *Actions* list names it; for a healthy volume it shows `none`. Follow the row that matches:

| If the report says | Do this |
|---|---|
| A missing feature, such as `write_capability_missing` or `read_capability_missing` | Install a StudioFS release that supports the volume. Do not mount with an older release. |
| `needs_migration`, `volume_format_needs_migration`, `schema_component_missing`, or `local_format_outdated` | Stop every mount of the volume, then run `studiofs upgrade myvolume` on this computer. See the upgrade steps below. |
| A session with `missing capabilities` | Upgrade that computer, or unmount it. A session that is only on a stale epoch refreshes by itself on its next heartbeat. |
| Unsafe local pending data (unpublished events above zero) | Do not reinitialize or clear this cache. Mount it with a compatible release so it publishes. If you cannot do that yet, keep the cache until this computer is upgraded. |

## Upgrading a volume from 0.2.x to 0.2.5

Volumes written by StudioFS 0.2.0 to 0.2.4 use format 5 or 6. They must be adopted into the 0.2.5 format before 0.2.5 can mount them. Plan this as a short, planned change:

1. Use `studiofs status myvolume` with the old release to record the `Metadata` file and the `Root` under `Local Cache`. Run `studiofs dump` with the release that last opened the volume if you also want a logical backup.
2. Stop every mount and Janitor sharing the PostgreSQL database. Take the PostgreSQL and local backups in the checklist before upgrading the shared metadata. `studiofs upgrade` refuses to run while the volume is mounted on that computer.
3. On each computer, run the check, then the upgrade:

```bash
studiofs upgrade myvolume --dry-run
studiofs upgrade myvolume
```

The upgrade applies any pending schema migrations and writes the new format. It prints one line for each migration and then a line such as:

```text
Volume 'myvolume' now uses format 7. Repeat upgrade on every other stopped cache before remounting.
```

Do the same on every other computer's stopped cache before you mount again. On a computer that has not run the upgrade yet, `studiofs compatibility` reports `needs_migration` with the reason `local_format_outdated`. An older computer that is not upgraded has its publication refused by the shared volume. Its unpublished changes stay in its local cache until it is upgraded.

Schema upgrades affect the whole PostgreSQL database, not just this volume. The `payload_gc` cleanup schema moves to version 2, and older janitors then stop working for every volume in that database. Upgrade the janitor for every volume in the same database before you rely on cleanup. The upgrade does not turn retention on.

### If the upgrade is interrupted

Run the same `studiofs upgrade myvolume` command again. A migration that already committed stays applied, and a failed one is rolled back. Your accepted local data is kept. Do not reinitialize the volume or clear its cache. If the command says it cannot own the local metadata, a process still has the file open: stop it and retry.

## Staged rollout of new clients

Clients in the support window can share a volume during a rollout. Before you start, run `studiofs compatibility myvolume` on a stopped computer and read *Sessions*. Check that every client you plan to keep is in the new release's support window. Then upgrade one computer at a time and check the report after each step:

1. Install the new release on one computer. With the volume unmounted there, run `studiofs compatibility myvolume`. Confirm `Overall` is still `compatible_rw`.
2. Mount and use the volume there. On another stopped computer, check that the sessions still show `ok`.
3. Upgrade the remaining computers in the same way.

A feature whose activation would take away read-write access from a supported client must stay inactive while that client release is in the support window.

## Planning a feature activation

Some newer features need an explicit cutover in phases. Expand and migrate prepare the volume. Activate makes the capability required, and Contract finalizes retirement. A feature's release notes specify whether `deactivate` can reverse its activation before Contract. Run the dry run for each phase first. For Activate, `--role` (`read`, `write`, or `maintain`, repeatable) selects what the feature becomes required for. The feature's release notes name the roles.

```bash
studiofs upgrade myvolume --feature <feature-name> --phase activate --role write --dry-run
```

The dry run lists the blocking sessions, the unpublished local work, and the rollback boundary. Do not run the real cutover until the dry run shows no blockers: upgrade or unmount each blocking computer first, and let each one publish its unpublished changes. The `--accept-offline-clients` option is an explicit decision for an offline computer that lacks the feature. That computer is fenced when it reconnects, and any changes it still holds unpublished cannot be sent to the volume.

## Interrupted or stuck upgrades

If an upgrade does not finish, do not reinitialize the volume and do not delete the cache. Check the report, stop any process that still has the metadata open, and rerun the same command. For a cutover that stopped after its dry run, rerun the dry run first, because the state may have changed.

## Rollback limits

- Clients older than 0.2.5 cannot mount or publish to a volume that 0.2.5 uses. Upgrade them before you rely on the volume.
- Restore a consistent pre-cutover PostgreSQL, local redb and payload backup with the matching release only as a disaster-recovery step. Expect to lose changes made after that backup. See the [exact rollback boundaries](../../persisted-format-compatibility.md#rollback-limits).
- StudioFS 0.3.0 upgrades only from 0.2.5 or later. Upgrade to 0.2.5 first.

## Status is local only

When the volume is not mounted, `studiofs status myvolume` includes a short compatibility section before *Local Cache*. These lines come only from this computer's local metadata and add no PostgreSQL or object-store call:

```text
Compatibility (local only)
──────────────────────────
  Volume format:       7 (this client supports 7..=7)
  Unpublished events:  2
  Shared check:        run `studiofs compatibility myvolume`
```

For the shared view, run `studiofs compatibility myvolume`.
