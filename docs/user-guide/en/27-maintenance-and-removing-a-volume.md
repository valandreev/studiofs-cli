# 27. Maintenance and Removing a Volume
> **Work in progress**

Maintaining StudioFS involves several different tasks: checking metadata, finding unused objects in S3, removing a client's local data, or permanently deleting a volume. Each has its own command. We will start with checks and move on to deletion once we understand exactly what it removes.

## Checking metadata: `fsck`

`fsck` looks for inconsistencies in the file system structure: directory entries without a corresponding metadata object, objects disconnected from directories, incorrect link counts, and unfinished internal operations. For a PostgreSQL-backed volume, the command first synchronizes local metadata with the server, then builds its report.

**A regular `fsck` run is not a strictly read-only check.** The initial synchronization already updates the local database. If problems are found, the command offers to repair them. For an initial inspection, decline repair: when you see `Repair all detected issues now? [y/N]:`, press Enter or type `n`.

```bash
studiofs fsck myvolume
```

Pay attention to these lines in the report:

| Field | Meaning |
| --- | --- |
| `dangling dentries` | Directory entries pointing to missing metadata objects |
| `orphan inodes` | Metadata objects disconnected from the directory tree |
| `nlink mismatches` | Discrepancies in link counts |
| `pending creates`, `deferred deletes`, `undo` fields | Internal state of unfinished operations |

`No issues found.` means this check found none of the metadata problems it looks for. It does not verify the contents of every file in S3 or prove that a backup is usable.

### When repair is needed

**Repair changes metadata and may delete entries and objects the command considers invalid. Stop client activity and prepare backups before agreeing to it.** Do not approve repair simply because some numbers in the report are nonzero.

You can agree to repair after reviewing the report from a regular `fsck` run. A separate flag starts repair without an additional prompt:

```bash
studiofs fsck myvolume --repair
```

The current implementation processes internal logs of unfinished operations, removes invalid directory entries, fixes link counts, and cleans up supported cases of objects disconnected from the tree. Data belonging to removed objects may enter the garbage collection queue. This does not recover lost files into a special folder.

After repair, the command scans the metadata again. If problems remain, save the report and logs for investigation. Do not repeatedly run repair at random: not every inconsistency can be fixed automatically.

## Unused objects in S3: `janitor`

After files are deleted, storage can keep objects that no metadata references any more. `janitor` finds them and deletes them. Before deleting, it checks each one in PostgreSQL, including snapshot protection and uploads still in progress. It does not clean your computer's SSD and does not replace `fsck`.

### Before you start

- **Run it on a computer that already has the volume.** The command reads this computer's local metadata (created by `studiofs init` or `studiofs discover`). Otherwise it stops with `Volume '…' not found`.
- **The volume must use PostgreSQL metadata.** Without it the command refuses to delete anything.
- **Unmount the volume on this computer first.** While a mount is running, it holds the local metadata, and the janitor cannot open it. Other computers can keep their mounts and keep working: each deletion is checked again in PostgreSQL immediately before it happens.
- **This computer's changes must be published.** If local metadata still has unpublished changes or recovery work, the command stops (exit code 4). Mount the volume, wait until synchronization finishes, unmount it, and try again.
- **Use the same StudioFS version as the one that set up PostgreSQL.** See [After an upgrade](#after-an-upgrade).

### How a run works

One run is one *cycle*. A cycle has two stages:

1. **Known work.** PostgreSQL keeps a queue of objects that StudioFS already knows are no longer needed, for example after a deletion. The janitor processes this queue first. It does not need to list the bucket for this.
2. **Orphan sweep.** Then the janitor lists the volume's prefix in S3 to find objects that nothing references and the queue does not know about. The list is read gradually, not all at once. The position reached is saved in PostgreSQL for this volume, so the next run **continues from that point** instead of starting again.

Before deleting an object, the janitor checks it again in PostgreSQL, *claims* it, deletes every key of that object in S3, and confirms the deletion in PostgreSQL. If anything is uncertain (the object is referenced, belongs to a snapshot, is still uploading, is too new or has an unknown age), the object is kept.

Only one janitor works on a volume at a time. That includes the background janitor of mounted clients. Snapshot creation waits while a janitor cycle holds the volume.

### Step 1: preview with `--dry-run`

```powershell
studiofs janitor myvolume --dry-run
```

`--dry-run` deletes nothing in S3 and saves nothing in PostgreSQL: no queue changes and no saved scan position. It still connects to PostgreSQL and S3 and lists objects. It is not an offline check.

A dry run starts from the saved scan position. To preview the whole prefix, add `--full-scan`:

```powershell
studiofs janitor myvolume --dry-run --full-scan
```

The run first prints its settings:

```text
Janitor started for 'myvolume'
  Mode:       dry-run
  Storage:    https://s3.example.com / my-bucket
  Prefix:     myvolume/objects/
  Scope:      remote objects only
  Grace:      15 minutes
  Max delete: 500
  S3 rate:    <= 4 HTTP requests/second
```

Every 5 seconds it prints a `Janitor progress:` line with the current phase and counters, so a long run is never silent. At the end of the cycle it prints a summary. Its most important lines are:

| Line | Meaning |
| --- | --- |
| `scanned` | Objects examined: queued objects and listed objects together |
| `referenced` | Objects kept because metadata or a snapshot still uses them |
| `abandoned uploads` | This computer's uploads that were cancelled before publication; they are retired and then deleted like any other unused object |
| `orphan candidates` | Objects that nothing references |
| `skipped young`, `skipped unknown age` | Candidates kept because they are newer than the grace period or their age could not be read |
| `outcome` | `complete`, or why the cycle stopped early (see the table below) |
| `scan coverage` | `complete` if the sweep reached the end of the prefix, otherwise `partial`, with the next position |
| `would delete` / `deleted` | Objects that would be deleted (dry run) or were deleted and confirmed |
| `deferred candidates` | Queued objects postponed to a later cycle |
| `delete errors`, `head errors` | Objects whose deletion or age check failed; a later run tries them again |
| `fences pruned` | Old internal deletion records removed from PostgreSQL |

**`scan coverage: partial` means part of the bucket has not been checked in this sweep. It is not a full cleanup.** Run the command again to continue.

### Step 2: limited cleanup

**Without `--dry-run`, the command really deletes objects in S3, without a separate confirmation.** Before the first real run, check the dry-run report, your backups, and that all clients have finished synchronizing.

Start with a small, cautious run:

```powershell
studiofs janitor myvolume --max-deletes 100 --skip-newer-minutes 60
```

- `--max-deletes` (default 500) is the most objects deleted in one cycle. It counts only confirmed deletions. When it is reached, the cycle stops listing.
- `--skip-newer-minutes` (default 15) is the grace period. Objects newer than this are never deleted.

These limits make a run smaller. They do not replace the safety checks, and they do not make an unsafe situation safe.

### Step 3: continue and finish the sweep

A large bucket usually needs several runs. You can limit each run by the number of listed entries or by time:

```powershell
studiofs janitor myvolume --max-scan-entries 100000
studiofs janitor myvolume --max-runtime-minutes 30
```

Each run continues from the saved position. When a run reaches the end of the prefix, its summary shows `scan coverage: complete`, and the next sweep starts from the beginning again. To start a new sweep from the beginning on purpose, add `--full-scan`; it applies to the first cycle only.

### Repeat automatically: `--loop`

```powershell
studiofs janitor myvolume --loop --interval-minutes 30
```

`--loop` repeats cycles until you stop it. If another janitor holds the volume, it waits for the next interval. After a failed cycle it waits longer each time (up to 8 times the interval). After 5 failed cycles in a row it stops with an error.

### Stop safely

Press **Ctrl+C** once. The janitor stops taking new work, finishes the deletions already in progress, confirms them in PostgreSQL, and exits with code 2. The saved position keeps the progress.

Press Ctrl+C a second time only if the command does not finish. It then exits immediately (code 130). This is also safe: an unfinished claim expires after about one minute, and the object is processed again by a later run.

This graceful stop is tested on Linux and macOS. On Windows, stopping and restarting the process was tested, but the Ctrl+C drain in a console was not yet. Check the exit code after you stop a run.

### Load on S3 and PostgreSQL

The janitor sends S3 requests one at a time, by default at most 4 per second (`--s3-requests-per-second`, from 1 to 32). If the provider answers "too many requests" (HTTP 429 or 503), the janitor slows down by itself and recovers gradually. PostgreSQL commands are also sent one at a time, and objects are processed in batches. The janitor needs no extra settings for a small PostgreSQL server.

Raise the S3 rate only if your provider allows it and PostgreSQL is on a fast network. In tests, a slow network to PostgreSQL limited speed more than the S3 rate did.

### Exit codes and output for scripts

The exit code tells a script what happened:

| Code | Meaning | What to do |
| --- | --- | --- |
| 0 | The cycle finished completely | Nothing |
| 2 | Partial: a delete, scan or time limit was reached, or you pressed Ctrl+C, and no object failed | Run the command again to continue |
| 3 | Another janitor is working on this volume (single run only) | Try again later, or use `--loop` |
| 4 | Unsafe metadata state: nothing was deleted | See [Troubleshooting](#when-the-janitor-stops) |
| 5 | PostgreSQL or S3 is unavailable, at least one object could not be checked or deleted, the schema does not match, or another failure | Read the error message; work already confirmed is kept, and unfinished work stays for the next run |

In PowerShell, read the code with `$LASTEXITCODE`; in bash, with `$?`:

```powershell
studiofs janitor myvolume --max-deletes 100
$LASTEXITCODE
```

For automation, `--output jsonl` prints one JSON object per line: `progress` events, a `cycle` event per cycle, and exactly one final `summary` event with `outcome`, `coverage` and `exit_code`. Every event has `schema_version: 1`. Messages about phases and errors go to the error stream, so they do not mix with JSON.

```powershell
studiofs janitor myvolume --dry-run --output jsonl > janitor.jsonl
```

Use PowerShell 7 (`pwsh`) for this: Windows PowerShell 5.1 writes the file as UTF-16, which most JSON tools cannot read.

### When the janitor stops

| Message | Cause | What to do |
| --- | --- | --- |
| `Volume '…' not found` | The volume is not set up on this computer | Run the command on a computer where the volume exists, or connect it with `studiofs discover` |
| Exit code 5 with an error opening the local database | The volume is mounted on this computer | Unmount it and try again. The last line still suggests checking PostgreSQL/S3 connectivity; the error line above it names the real cause |
| `unsafe metadata state` … `pending sync or recovery state` (code 4) | This computer has unpublished changes or unfinished recovery | Mount, wait for synchronization, unmount, retry |
| `unsafe metadata state` for the volume identity or storage binding (code 4) | Local and PostgreSQL information about the volume do not match | Do not try to bypass it; check that this is the right volume and storage, and ask for help with the logs |
| `global lock is busy` (code 3) | Another janitor, possibly a mounted client's, is working | Wait and retry, or use `--loop` |
| `PostgreSQL schema or privileges do not fit this version` (code 5) | The schema is missing, damaged, or from another StudioFS version | See [After an upgrade](#after-an-upgrade) |
| `backend unavailable` (code 5) | PostgreSQL or S3 cannot be reached | Check the network and credentials; work already confirmed is kept |
| `S3 overload circuit open` or `completed with transient errors` (code 5) | At least one object could not be checked or deleted in S3 | Run again later, or lower `--s3-requests-per-second`; failed objects are tried again |

None of these cases has a "force" option. If the janitor is not sure an object is unused, it keeps the object.

### After an upgrade

The janitor stores its queue and saved position in PostgreSQL tables with a version number. After installing a new StudioFS version, update these tables once, from any computer with the new version. Run `studiofs init` with the same storage and PostgreSQL options you used to create the volume ([chapter 13](13-create-your-first-volume-studiofs-init.md)):

```powershell
studiofs init myvolume --endpoint https://s3.example.com --bucket my-bucket `
  --access-key "<key>" --secret-key "<secret>" --meta "<postgresql-dsn>"
```

Use exactly the same endpoint and bucket: `init` creates a bucket that does not exist, so a typo creates a new, empty bucket.

For an existing volume, the command applies the update first and then stops with `Volume 'myvolume' already exists in PostgreSQL metadata`. **That message is expected here. Do not follow its suggestion to run `destroy`: that would delete the volume.** The update does not change your files, and repeating it is safe.

If the janitor reports that the schema does not fit, either the update was not applied, or PostgreSQL was updated by a **newer** StudioFS. An older version cannot downgrade the tables. Install the matching version instead of editing the database by hand.

The background janitor inside mounted clients works as before. Only the `studiofs janitor` command changed.

## Remove a volume from this computer or delete it for everyone?

These are fundamentally different actions:

| Command | This client's local state | Shared data |
| --- | --- | --- |
| `destroy myvolume --local` | Deletes the local metadata directory, including the cache inside it | Keeps S3 data and shared metadata |
| `destroy myvolume` | Deletes the same local state | Deletes the volume's S3 data and shared PostgreSQL metadata |

### Local removal: `destroy --local`

Use this option when you deliberately want to remove a volume's local state from a particular computer, including a stale local configuration. In the current implementation, it does not require access to PostgreSQL or S3.

**Local removal permanently deletes local metadata and the cache within its directory. If unpublished changes or the only copies of data remain there, they may be lost.** First make sure the files you need are available from another client or from an independent, verified copy. If remote services are unavailable and you cannot verify publication, do not assume the local data is expendable.

```bash
studiofs destroy myvolume --local
```

Read the displayed path and cleanup scope. The command first asks you to answer `y`, then to repeat the volume name exactly. An empty answer to the first prompt or an incorrect name cancels deletion.

After confirmation, the current command acquires exclusive access to the local database. If a mounted volume or another process holds that same database, deletion should fail. Unmount the volume and close the process using the database; do not manually delete database files to bypass the lock.

### Full deletion: `destroy`

**This permanently deletes the volume for all users: its S3 data and shared metadata. The command does not create a backup and has no undo command.** Before running it, stop work and unmount the volume on every computer, then check the volume name, storage, and backups. Run it only when the volume is no longer needed.

```bash
studiofs destroy myvolume
```

The command checks that the local and remote volumes match, displays the deletion scope, and asks for two confirmations, just like the local option. If it cannot verify the remote identity or the UUIDs do not match, it does not proceed with full deletion and may suggest local cleanup. That is a separate decision with the risks described above.

After confirmation, full deletion also acquires exclusive access to the local database and holds it during remote cleanup. If a local process is using the database, remote deletion does not begin. **This lock does not stop clients on other computers.** You must unmount those in advance.

Remote cleanup and deletion of the local directory are sequential steps, rather than one indivisible operation. If the command fails partway through, some data may already have been deleted. Save the error message and check the actual state; do not assume everything was automatically rolled back.

## What remains on the local disk?

Both `destroy` modes remove the volume's entire metadata directory, including the default cache inside it. **A custom `cache_dir` is not automatically detected or removed.** The command reminds you of this and, if it could read the local metadata, displays the volume UUID.

Do not delete an arbitrary cache folder because its name looks similar. First check your settings, the path, and the UUID, and make sure the directory belongs to this volume and contains no local changes you need. Leave it in place until then. If you only want to free space on your SSD, return to the [chapter on caching](20-cache-why-is-the-cloud-using-my-ssd.md) rather than deleting the volume.

---

[Contents](README.md) · [Українська](../uk/27-maintenance-and-removing-a-volume.md) · [← Previous](26-credentials-encryption-and-access.md) · [Next →](28-troubleshooting-when-the-cat-stops-purring.md)
