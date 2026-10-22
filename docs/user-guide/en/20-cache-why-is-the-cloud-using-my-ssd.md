# 20. Cache: Why Is the Cloud Using My SSD?

You opened a few clips from a cloud volume, and now your laptop has less free space. Has StudioFS decided to download the entire archive? No: it keeps the data you are working with nearby, so it does not have to go back to S3 for every next piece of video. The cloud is far away, and editing applications do not like waiting.

But the word “cache” is a little misleading here. Your local disk holds more than copies of data that has already been published. It may also contain **new changes that have not reached the cloud yet**. That is why you cannot treat this folder like ordinary temporary files.

## What stays on your computer

When reading, StudioFS fetches the data it needs from storage and caches it locally. That is not necessarily the exact number of bytes the application requested: fetching fragments and reading ahead can bring in more. Reopening material you have already read usually takes fewer network requests.

When writing, changes become available on your computer first. Background processes then upload the contents to S3 and send shared metadata through PostgreSQL. An application's “saved” message alone does not mean another team member can already access that version.

It helps to distinguish two states of local data:

- **Dirty** means locally modified data that still needs to be published. This may be the only saved copy of your new work. StudioFS protects it from normal cache eviction.
- **Evictable** means data the cache can now remove when it needs space. The next time it is needed, it can be fetched from storage again.

Not every byte that is no longer `dirty` immediately becomes `evictable`: a current operation may still be using the data. And cached reads are no promise of offline access—the cache contents can change. Do not rely on the cache as a guaranteed offline copy of your project.

## Where it all lives

For each volume, StudioFS creates a separate directory named after its UUID inside the cache base directory. If `--cache-dir` is not specified and there is no saved setting, it uses `cache` inside the volume's local metadata directory.

Inside the volume's cache directory, `payload/objects` holds local contents, `upload/objects` stages uploads, `index/cache.redb` holds the index, and `tmp` is used for temporary operations. StudioFS also keeps local metadata separately. These parts form a connected working state, rather than a collection of independent files you can clean up by hand.

Choose a fast local SSD with space to spare for the cache. Do not place it inside the mounted StudioFS volume itself. Remember what else needs that disk: the operating system, your editing application's files, proxies, and exports.

Here is an example of options for **the next mount** on Linux:

```bash
studiofs mount studio /mnt/studio --cache-dir /srv/studiofs-cache --cache-size 50GB
```

Here, `studio` is the example volume name, and you need to replace both paths with your own. On Windows, the cache path might look like `D:\StudioFS-cache`; on macOS, `/Users/yourname/StudioFS-cache`.

You can save the options as global defaults:

```bash
studiofs settings --cache-dir /srv/studiofs-cache --cache-size 50GB
```

Explicit `mount` options take precedence over saved settings. `settings` does not reconfigure a mount that is already running. And changing the path **does not move the existing data**: if you have unpublished changes, do not switch to an empty directory hoping StudioFS will find them itself. First close your applications, wait for publication to finish, and unmount the volume normally; coordinate moving the working state with your administrator.

## Cache size is not volume size

`--cache-size` sets the disk cache budget. If neither an explicit nor a saved value is provided, the default is `100GB`. This does not mean StudioFS immediately occupies all that space, or that the cloud volume cannot be larger.

It is also **not a guaranteed size limit for the entire local directory**. The index, metadata, temporary operations, and upload staging need space too. Protected data cannot simply be thrown away to make the settings look tidy. If enough space cannot be freed, writes may slow down or fail because capacity is exhausted.

For sizes, use the suffixes `B`, `KB`, `MB`, `GB`, `TB`, `PB`, and `EB`. In StudioFS, these use binary units: `1KB = 1024B`, `1GB = 1024 × 1024 × 1024B`. For example, `50GB` and `1.5GB` are valid values; the parser discards any fractional byte. Write `GB`, rather than `GiB` or just `G`: the CLI does not support those alternative suffixes.

Do not give the cache all the SSD's free space. Leave headroom for the largest local write you expect and for the rest of the system. If you write faster than the connection can upload to S3, the queue and local data will grow even with a healthy connection.

## Three read options that are easy to mix up

| Option | What it controls | What to watch for |
|---|---|---|
| `--read-buffer` | The read buffer budget in RAM | Takes a size, such as `256MB`; rounded up to whole MiB. This is not an SSD budget. |
| `--readahead` | The sequential read-ahead width in blocks | Takes a whole number of at least `1`, rather than a number of megabytes. |
| `--prefetch` | The prefetch concurrency budget | Takes a whole number of at least `1`; more concurrent requests may increase network and disk load. |

These options are available in `mount` and `settings`. Start with the defaults. Then change one option at a time and test the same working scenario: playing a clip sequentially and jumping around the timeline put different demands on the system. Turning every number up to the maximum will not turn a laptop into a server—sometimes it just makes the cooling louder.

## Checking where the space went

For our example with a separate cache directory:

```bash
studiofs status studio --cache-dir /srv/studiofs-cache
studiofs status studio --cache-dir /srv/studiofs-cache --verbose
```

Pass the **base directory**, without the UUID: the command adds it itself. For `status`, specify a custom `--cache-dir` explicitly; do not expect it to pick up the mount path saved through `settings`.

In the `Local Cache` section, check:

- `Root`—whether you are looking at the correct cache for this volume;
- `Usage bytes`—the accounted size of local contents;
- `Dirty bytes`—the amount of locally modified data;
- `Evictable bytes`—the amount of data available for eviction;
- `Pending uploads`—the number of uploads still awaiting completion.

These counters do not replace checking the disk's free space through your operating system. `Usage bytes` is not the size of all files on the SSD, while `Used space` in the general part of the report refers to the volume's data. If you see `unknown` instead of a counter, the value is unknown, rather than zero.

`--verbose` adds diagnostics from the mounted process when available. Pay attention to the snapshot's freshness. A `Pending uploads` count of zero alone also does not prove that all shared metadata has been published: check the synchronization diagnostics described in the previous chapter.

## If the SSD is almost full

Pause large new writes and exports to this disk. Check the free space, the cache path, `Dirty bytes`, and `Pending uploads`. If the queue keeps growing or does not shrink, check access to S3; investigate shared metadata problems separately through PostgreSQL connectivity and synchronization diagnostics.

Let background work finish. For immediate headroom, you can free space **outside StudioFS's working directories**, for example by removing your own exports that you no longer need. Do not manually delete the cache itself, its index, staged uploads, or local metadata. Reducing `--cache-size` does not make unpublished data safe to delete either.

Review the cache budget and disk location for the next start. If unpublished data remains after a crash, preserve its directory and return to the same working state: locally saved data and metadata are needed for recovery. Bytes that were still only in process memory before the crash do not have that guarantee.

The cache is there to make work faster. The safest way to think of it is as your desk: it holds both copies of reference material and a draft you have not handed to anyone yet.

---

[Contents](README.md) · [Українська](../uk/20-cache-why-is-the-cloud-using-my-ssd.md) · [← Previous](19-sync-bad-wi-fi-and-other-surprises.md) · [Next →](21-two-people-one-file-collaboration-and-conflicts.md)

[Linear source](https://linear.app/valandreev/document/20-cache-why-is-the-cloud-using-my-ssd-6612dc14bcb2)
