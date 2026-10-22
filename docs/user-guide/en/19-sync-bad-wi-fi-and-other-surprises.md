# 19. Sync, Bad Wi-Fi, and Other Surprises

You saved a scene, but a colleague still sees yesterday’s version. Or folders open normally, yet one of the clips suddenly will not read. That does not necessarily mean the file has disappeared. In StudioFS, local work, uploading data to S3, and changes appearing on other computers happen separately. Bad Wi-Fi simply makes the difference especially noticeable.

Let us look at what can keep working, what will have to wait, and where to check the status before clicking everything in sight.

## “Saved”—at which stage?

Suppose you changed a file and clicked “Save.”

1. **StudioFS accepted the changes locally.** Your application can continue working while StudioFS prepares the data on your computer. A successful write alone does not mean everything will survive an unexpected shutdown.
2. **The changes crossed a local durability boundary.** In `Durable` cache mode, a successful `fsync` ensures that accepted data can be recovered locally after a crash. In `Fast` mode, StudioFS does not force the system to physically write cached contents to disk, so some data may be lost after a crash even after `fsync`.
3. **The contents have been uploaded to S3.** This still does not mean the new version has been published for other computers.
4. **The version has been published, and another computer has received the update.** Your colleague can now see the changes and download the parts of the file they need from S3. Until then, their local catalog may still show the previous version.

The application working with the file calls `fsync`; an ordinary “Save” button does not guarantee that it will do so. Ordinary `flush` operations and closing a file also do not wait for publication to colleagues, and may report success before local metadata has been durably written. They have a short deferred-write window—up to one second while the background worker is operating normally.

So **“the application saved it,” “it will survive a local crash,” and “colleagues can already see it” are different claims**. If you need to hand work over to another computer, look for completed synchronization, rather than just the asterisk disappearing beside the project name.

## What happens when the connection goes down?

StudioFS serves the mounted drive through a local metadata database. It does not ask PostgreSQL for every folder name. But file contents, shared updates, and some operations still need the network.

| What is unavailable | What to expect |
| -- | -- |
| **S3** | The required parts of files may remain readable if they are already cached. Parts that are not local cannot be downloaded: reads may be delayed by retries or fail with an error. Uploads of new data will wait for access to return. |
| **PostgreSQL** | An already mounted drive with usable local state may continue ordinary local work. Metadata exchange with colleagues is delayed. Shared file locks depend on PostgreSQL and may fail. |
| **VPN** | You lose access to services reached through that private network. For example, S3 may remain available over the internet while PostgreSQL does not. Check both connections separately. |
| **Internet** | If both services are remote, access to both is lost. Only the capabilities supported by existing local state remain. If the services are on your local network, an external internet outage alone will not necessarily cut them off. |

This is **not a promise of full offline operation**. Even if you have opened a video before, only the sections you viewed may have reached the SSD. Jumping to another point in the timeline may require data from S3 for the first time. StudioFS may also evict previously cached clean data later to free up space.

There is also a difference between a drive that is already working and a first mount. A new computer needs to obtain the shared catalog and write authority. It remains read-only until its initial catch-up is complete. If the local state needed to allocate identifiers safely is missing or damaged, writes may also be unavailable without PostgreSQL. Restarting during a network outage will not conjure that state out of thin air.

## How much can you write while S3 is unavailable?

As much as local resources and StudioFS limits allow. There is no infinite “send it later” queue—even if your deadline makes a very compelling argument.

While uploads are stalled, unfinished changes occupy memory and local storage. StudioFS limits the amount of this data and may slow new writes or make them wait. A full disk or a local error can also cause a save to fail. Increasing the cache limit does not, by itself, add free space to your SSD.

Clean data can be downloaded from storage again. Changes that have not been uploaded may exist only here. **Do not delete the cache, local metadata database, or volume directory to “kick synchronization into life.” Do not reinitialize the volume as a way to fix the network.** You could lose precisely the data that is still waiting to be sent.

## Where can you check for progress?

For a volume named `myvolume`:

```bash
studiofs status myvolume
studiofs status myvolume --verbose
```

The first command shows volume information, its PostgreSQL binding, and local counters. Additional metrics from the mounted drive, including PostgreSQL synchronization and write-path diagnostics, depend on its diagnostics being available. The details let you check catch-up status and the time of the last successful synchronization, when those data are available. A stale or missing metric does not confirm success.

For live metrics, you need the **mount point**, rather than the volume name. For example, on Linux:

```bash
studiofs perf /mnt/studio --count 5
```

Substitute your own path; on Windows, it might be `Z:`. The command observes the mounted drive. It does not run repairs or force synchronization. Watch changes in the upload queue, the number of active uploads, retries, and unfinished metadata operations. Five snapshots help reveal a trend; an empty upload queue alone does not prove publication is complete.

When a graphical environment is available, StudioFS automatically tries to show an indicator in the system tray or menu bar. If it appears:

* **A red pulse** means changes accepted by this mount still need synchronization. This may involve writer data, uploads, retries, version publication, or metadata acknowledgement.
* **A yellow-and-blue flag** appears when a fresh check confirms remote completion of accepted data and metadata. This describes the work of this mount; it does not confirm that every colleague’s computer has already received the updates.
* **Starting, unavailable, or unverified status** provides no basis for considering synchronization complete. The absence of a red pulse does not replace confirmation.

Even a tooltip saying it is safe to shut down applies only to the moment of the check: an application may write something new immediately afterward. Close applications and unmount the drive normally before shutting down. The icon does not perform that procedure for you.

## The connection is back. What now?

Leave StudioFS running: background workers retry uploads and publication and receive missed metadata. Catch-up time depends on the amount of accumulated changes, network speed, and the availability of both services. Wi-Fi returning does not mean the VPN is already up or your S3 credentials are still valid.

If there is no progress:

1. Check the internet or local network, the VPN, and access to S3 and PostgreSQL separately.
2. Check free space on the disk holding the cache, and read the application error or StudioFS log. An access error and a lack of disk space call for different actions.
3. Compare several fresh observations from `status` or `perf`. Keep the error text and the time it appeared for diagnosis, removing passwords, tokens, and full connection strings.
4. Before handing work over, wait for confirmation that outbound synchronization is complete, then check the required version on the recipient’s computer.

While computers are not exchanging changes, it is best to avoid editing the same file in parallel. Restoring the network transfers changes, but it does not turn two different edits into one agreed version.

In the next chapter, we will take a closer look at the local cache: what uses SSD space, what can be evicted, and why a folder labeled “cache” sometimes holds the most important copy of your work.

---

[Contents](README.md) · [Українська](../uk/19-sync-bad-wi-fi-and-other-surprises.md) · [← Previous](18-working-with-real-creative-projects.md) · [Next →](20-cache-why-is-the-cloud-using-my-ssd.md)

[Linear source](https://linear.app/valandreev/document/19-sync-bad-wi-fi-and-other-surprises-48d23b2a42ab)
