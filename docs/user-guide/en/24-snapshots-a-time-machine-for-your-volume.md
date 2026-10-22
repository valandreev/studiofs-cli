# 24. Snapshots: A Time Machine for Your Volume
> **Work in progress**

Yesterday your edit opened fine. Today someone has overwritten a file you need. You would like to go back a step and retrieve the old version. That is what StudioFS **snapshots** are for: captured states of the entire volume that you can open separately and browse without write access.

This time machine has a very practical purpose: find the file you need and copy it back. But you have to buy your ticket to the past in advance—a snapshot created after a mistake will not show the state before it.

## What exactly goes into a snapshot?

A snapshot captures the volume's state from the shared metadata in PostgreSQL. It covers **the entire volume**, rather than a selected folder. Its contents are tied to data in object storage.

The distinction we already know matters here: locally saved changes may not yet have been published for other clients. A snapshot does not automatically gather everything in your computer's local cache. Before an important checkpoint, save your work in the application and wait for data uploads and metadata synchronization to finish. If a project consists of several related files, let the application finish writing them; if necessary, close the project and agree on a pause with your colleagues.

**A snapshot does not turn saving a project with multiple files into one indivisible operation.** If the application is in the middle of rewriting its parts, the captured state may not be usable in that application.

The commands below require a configured PostgreSQL-backed volume and access to that database. Creating and reading a snapshot also require access to object storage.

## Creating a checkpoint

Before a major cleanup or changes to a project, run:

```bash
studiofs snapshot create --ttl 7d
```

The command registers a snapshot and waits until it becomes **`Ready`**—ready to use. Keep the identifier from the response. StudioFS assigns it; you do not need to supply your own name to this command.

`--ttl 7d` means seven days of retention **from the moment it becomes ready**, rather than from the start of creation. Durations accept positive whole numbers with the suffixes `s` (seconds), `m` (minutes), `h` (hours), `d` (days), and `w` (weeks): for example, `12h`, `7d`, or `2w`.

You can create a snapshot without an automatic expiry:

```bash
studiofs snapshot create
```

This kind of manual snapshot is kept until you explicitly delete it. “No expiry” does not mean “no cost”: historical data also takes up storage space.

Pay attention to the syntax: `studiofs snapshot` commands **do not accept a volume name**. They find the current volume using the client's local service data and validate it in PostgreSQL. There must be one unambiguously identifiable PostgreSQL-backed volume in the client data root. If the command reports multiple volumes or an unavailable context, adding a name at the end will not help: resolve the client configuration first. Do not delete service databases at random.

## Seeing what is already there

```bash
studiofs snapshot list
```

For a closer look, copy an identifier from the list:

```bash
studiofs snapshot inspect projectfs.2026-08-12T14-07-09.482Z
```

Here and below, `projectfs` and the identifier shown are examples. Replace them with your own values.

These fields in the response are useful to distinguish:

| Field | What it tells you |
| --- | --- |
| `origin` | Whether the snapshot was created manually (`manual`) or on a schedule (`automatic`). |
| `captured_at`, `cut_version` | The capture time and the version of shared metadata that was captured. |
| `state` | The snapshot's overall state. Mounting requires `Ready`. |
| `creation` | The separate internal state of the snapshot creation job. |
| `ready_at`, `expires_at` | When the snapshot became ready and when its retention period expires. |
| `checksum` | The checksum of the prepared snapshot. |
| `last_error` | The last recorded error, if any. |

A `-` means there is no value for that field. For example, a manual snapshot without a TTL has no `expires_at`.

`Creating` does not mean ready yet; `Failed` indicates unsuccessful creation, while `Deleting` and `Deleted` refer to deletion. Do not try to mount a snapshot just because its identifier has appeared in the list.

If you interrupt `create` with Ctrl+C, its registration remains in PostgreSQL. Check that identifier with `inspect` before creating another snapshot. Interrupting a terminal command and canceling a job are different things.

## Opening the past as a separate drive

Use `smount` for a ready snapshot. This is where the volume name **is required**, followed by the snapshot identifier and a separate mount point.

On Windows, if drive letter `Z:` is available:

```powershell
studiofs smount projectfs projectfs.2026-08-12T14-07-09.482Z Z:
```

On Linux or macOS, use a separate directory you have prepared, for example:

```bash
studiofs smount projectfs projectfs.2026-08-12T14-07-09.482Z /mnt/snapshot
```

Choose a path your user can access; do not mount the snapshot over the working volume. If needed, you can set a separate cache with `--cache-dir <path>`.

`smount` always opens the snapshot **read-only**. You can browse files on this drive and copy them elsewhere, but you cannot modify, rename, or delete them.

To recover a file, copy it from the snapshot to a separate folder on an ordinary drive or on the working volume. Check that it is the version you need before replacing the current file. For a project with multiple files, copy the required set together and check it in the appropriate application. There is no separate command here to roll back the entire volume.

## Daily snapshots without calendar reminders

You can set one daily policy for the volume. For example, create snapshots every day at 02:30 Kyiv time and retain ready snapshots for two weeks:

```bash
studiofs snapshot policy set --daily-at 02:30 --timezone Europe/Kyiv --retention 14d
studiofs snapshot policy show
```

Specify the time in `HH:MM` format and the timezone as an IANA name. If you omit `--timezone`, **UTC** is used, even if your computer runs on Kyiv time. `--retention` is required; automatic snapshots need a finite, positive retention period. That period also starts at `Ready`.

Running `policy set` again replaces the current policy. To disable future automatic creation:

```bash
studiofs snapshot policy cancel
```

This does not delete snapshots that have already been created.

The schedule is handled by a background mechanism in a running mount of the working volume. Saving a policy does not start a separate server that runs around the clock: if all clients are off, a snapshot will not appear at exactly 02:30. After downtime, the mechanism can process the most recent due run, but it does not create a separate snapshot for every missed day or reconstruct past states retroactively.

Clock changes have a wrinkle too: if the scheduled local time does not exist on that day in the chosen timezone, that run is skipped; if the time occurs twice, the earlier instant is used. For a predictable schedule, check the timezone with `policy show`.

## Deleting what you no longer need

When you no longer need a snapshot, finish working with its mounted view first, then run:

```bash
studiofs snapshot delete projectfs.2026-08-12T14-07-09.482Z
```

This is a deletion request that you can repeat. It does not mean that every related object instantly disappears from storage. Existing mounts may continue working for a while: they hold renewable permissions to use the snapshot, called leases. Cleanup must wait for those leases to end. TTL expiry should not be taken as a promise to free space at that exact second either.

Double-check the identifier before deleting. A copy of a file you still need to retrieve from the snapshot may become unavailable after deletion.

## Why is this still not a backup?

A snapshot helps you retrieve an older version while the necessary metadata, objects, and volume key material remain available. It depends on the same infrastructure and does not create an independent copy of all the data somewhere else.

Losing PostgreSQL, the required S3 objects, or key material can cost you both your working files and the ability to use snapshots. Those situations call for backups and a tested recovery procedure. That is the subject of the next chapter.

---

[Contents](README.md) · [Українська](../uk/24-snapshots-a-time-machine-for-your-volume.md) · [← Previous](23-deleting-files-there-is-no-magic-trash-can.md) · [Next →](25-migrating-studiofs-metadata-to-new-postgresql-server.md)
