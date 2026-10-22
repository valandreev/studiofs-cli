# 22. Settings, Status and Activity
> **Work in progress**

StudioFS usually works alongside your editing software: you open files while it reads data, maintains a cache, and sends changes in the background. But sometimes you want to know: what is happening now, is there enough space, and why did the same clip open faster yesterday?

Three commands help with that: `settings` shows saved settings, `status` shows the state of a particular volume, and `perf` shows a mounted drive's activity over time. An icon in the system tray or menu bar lets you watch synchronization without keeping a terminal open. Let us see which questions each one can answer.

## Settings: what is saved for the next mount

Start with:

```bash
studiofs settings
```

This shows the default mount point, cache directory and size, and read settings. `<unset>` means that no value has been saved for that setting. Unless configured otherwise, the disk cache budget is `100GB`; StudioFS chooses how to manage that budget automatically.

`settings` shows **saved values for future `mount` commands**, rather than the complete configuration of an already mounted drive. For example, you may have explicitly chosen a different cache size when mounting it.

Suppose you decide to give the cache 200 GB on a local SSD. On Windows, that might look like this:

```powershell
studiofs settings --cache-dir "D:\StudioFS-cache" --cache-size 200GB
```

On macOS or Linux, choose a local directory of your own, for example:

```bash
studiofs settings --cache-dir "$HOME/StudioFS-cache" --cache-size 200GB
```

The command saves the settings in `config.toml` and prints its path. The usual locations are:

| System | Settings file |
|---|---|
| Windows | `%APPDATA%\studiofs\config.toml` |
| macOS | `~/Library/Application Support/studiofs/config.toml` |
| Linux | `~/.config/studiofs/config.toml` |

Changing these values does not reconfigure an existing mount or move the old cache to the new directory. To apply them, finish working with the files and remount the volume.

For the cache directory and size, explicit `mount` options take priority over saved values, which take priority over defaults. If no directory is specified, StudioFS uses `<meta-dir>/cache`, usually at `~/.studiofs/meta/<volume-name>/cache`. Within the base directory, each volume's cache lives separately under its UUID.

On macOS and Linux, you can also save a default mount point:

```bash
studiofs settings --mount-point /mnt/studio
```

## Disk cache and memory are different resources

A larger cache helps reopen previously read media without downloading it from S3 again. It also participates in handling written data locally. However, `--cache-size` does not increase the volume's capacity or reserve the entire specified amount of disk space in advance. Leave free space for the operating system, applications, and their temporary files. A completely full SSD rarely gets faster just because we remain optimistic.

## Volume status: read the snapshot and its warnings together

In these examples, `projectfs` is your volume's name:

```bash
studiofs status projectfs
studiofs status projectfs --verbose
```

Depending on the availability of the local database and diagnostics from the mounted process, the command shows information about the volume, metadata, cache, and synchronization. `--verbose` adds detailed write-path metrics when available. The mounted process may keep the local database open; in that case, the command may report a lock warning and show the available diagnostics from that process instead of a full report. That warning alone does not prove database corruption.

If you use a custom cache directory, pass **the same base directory** you used when mounting:

```powershell
studiofs status projectfs --cache-dir "D:\StudioFS-cache" --verbose
```

Without `--cache-dir`, the current `status` command looks for the cache in `<meta-dir>/cache`, even if a different path is saved in `settings`. Do not append the UUID to the option: the command finds the volume's directory itself.

In the `Local Cache` section, distinguish these fields:

* `Usage bytes` — the accounted size of local cache data.
* `Dirty bytes` — the amount of data still marked as unfinished local changes.
* `Evictable bytes` — the amount of data the cache can evict.
* `Pending uploads` — the number of local objects awaiting upload, rather than the number of files in your project.

`Runtime readiness: ready` in this section refers to the local cache layout. It **does not confirm** that all changes have reached S3, that another computer has received them, or that you can power off. Likewise, `Reconcile needed: no` is not a general certificate of synchronization.

Read warnings alongside the numbers. If the cache index is unavailable or locked, counting may fail; in particular, after warning that it could not calculate the queue, the command may print `Pending uploads: 0`. This does not prove the queue is empty. `unknown`, `unavailable`, and a stale snapshot mean there is not enough reliable information.

When available, the `PostgreSQL Sync` section helps you see metadata application progress, how long ago synchronization last succeeded, and errors. Completing one stage of metadata catch-up does not by itself confirm that all file contents have been uploaded.

## Activity: what is changing right now

`perf` takes a **mount point**, rather than a volume name. On Windows:

```powershell
studiofs perf Z: --interval 1000 --count 10
```

On macOS or Linux, pass the actual path of the mounted volume:

```bash
studiofs perf /mnt/studio/projectfs --interval 1000 --count 10
```

The interval is in milliseconds; this produces ten interval samples roughly one second apart. Without `--count`, the view continues until you interrupt it with `Ctrl+C`; that stops observation, rather than unmounting the drive.

The table includes metrics for resource usage, filesystem operations, metadata, cache, and object storage. Watch it first while idle, then open a large clip and open it again. The first read may access S3, while the next may use data already fetched. The operating system's cache also affects the result, so one fast attempt does not prove you have found the perfect settings.

For more detail, use `--verbosity verbose`; notice that this is different from the `--verbose` flag used by `status`. To record a short report for investigating a problem:

```powershell
studiofs perf Z: --interval 1000 --count 10 --json-lines > studiofs-perf.jsonl
```

`--json-lines` writes a separate JSON line for each sample. The `--json` alternative produces one array and requires `--count`. If statistics are unavailable, check that you specified the actual mounted StudioFS drive; a message about unavailable metrics does not mean there is no activity.

## The synchronization icon: handy, but read the tooltip

StudioFS automatically tries to show an indicator in the Windows or Linux system tray or the macOS menu bar. Whether it is visible depends on the graphical environment; no separate option is needed to enable it. If the indicator is missing, use the available command-line metrics.

A red pulse means that changes accepted by this mount still need synchronization. This may involve writer data, staged or retrying uploads, file-version publication, or metadata acknowledgement. Finishing the upload of one object does not finish all of that work.

An animated blue-and-yellow flag appears when a fresh check confirms remote completion of the data and metadata accepted by this mount. Ordinary reads and prefetching do not themselves mean that you have outgoing changes. During startup, or when diagnostics are lost or become stale, the indicator cannot confirm completion; hover over it and read the tooltip.

Even `safe to shut down` describes the moment of the check. An open application may immediately write something else. Before powering off, close applications that use the volume and perform a normal [unmount](17-unplug-safely-studiofs-umount.md). The flag does not replace that procedure or prove that another computer has already applied the changes to its local state.

## When to intervene

While copying large media files, a growing queue and a red pulse are expected. After writing finishes, and with the network available, work should gradually progress. Look at several observations: a single number does not show which way things are moving.

If the queue does not shrink for a long time after writing stops, errors keep recurring, or space runs short, check free space on the cache disk, network and VPN access, and the availability of S3 and PostgreSQL. Record when the problem occurred and the action that triggered it, and save an available `status --verbose` report or a short `perf` recording. Before sharing reports, check them for private addresses and paths.

Do not try to cure an unexplained state by deleting the cache or recreating the volume. First determine what has stalled: reading, uploading data, or synchronizing metadata. That is much more useful than immediately increasing every number in the settings.

---

[Contents](README.md) · [Українська](../uk/22-settings-status-and-activity.md) · [← Previous](21-two-people-one-file-collaboration-and-conflicts.md) · [Next →](23-deleting-files-there-is-no-magic-trash-can.md)
