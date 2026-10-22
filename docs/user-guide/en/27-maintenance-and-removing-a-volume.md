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

After deletions, storage may retain objects that are no longer referenced by metadata. `janitor` looks for these candidates and respects snapshot protection for objects. It does not clean up your computer's SSD or replace `fsck`.

Start with a preview:

```bash
studiofs janitor myvolume --dry-run
```

`--dry-run` does not delete objects in S3. The command still opens metadata and contacts remote services; this is not an offline inspection of the local cache.

In the summary, `scanned` counts the file objects examined, `orphan candidates` counts candidates without references, and `would delete` shows how many objects would be deleted under the current limits. The candidate count may differ from the planned deletion count: the command skips objects that are too new, objects of unknown age, and objects beyond the per-cycle limit.

The default age threshold is 15 minutes, and the limit is 500 objects per cycle. For a more cautious preview, you can increase the threshold and reduce the limit:

```bash
studiofs janitor myvolume --dry-run --skip-newer-minutes 60 --max-deletes 100
```

**Without `--dry-run`, the command actually deletes objects in S3, without a separate interactive confirmation.** Proceed only after checking the report, backups, and synchronization state of all clients. The age threshold and limit reduce the scope of a single pass, but do not guarantee protection against data loss.

```bash
studiofs janitor myvolume --skip-newer-minutes 60 --max-deletes 100
```

The `--loop` flag repeats cycles; `--interval-minutes` sets the interval between them. For your first manual maintenance session, a single pass is enough. Check `deleted`, `delete errors`, and the reasons for skipped objects in its summary.

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

[Linear source](https://linear.app/valandreev/document/27-maintenance-and-removing-a-volume-56eef42bf8af)
