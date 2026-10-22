# 03. Capabilities, Limits, and System Requirements

StudioFS can already do the essentials: mount shared storage as a regular drive, read large media files without downloading them in full, and transfer changes between computers. But it is still a young project, so it is better to know the limitations before moving all your projects onto it.

## What already works?

* **A shared file space.** Several computers work with the same files and folders. There is no need to maintain a separate project copy on every workstation.
* **Partial reads.** You can open a large file without downloading it in full. StudioFS fetches the parts it needs from S3 and keeps what it has read in the local cache.
* **Local writes and background synchronization.** Your application saves changes on your computer, and StudioFS uploads them to storage and publishes them for colleagues in the background.
* **Compatibility with ordinary applications.** To any application, StudioFS looks like a regular local drive. There is no need to change anything or install extra plugins. If an application can work with files on a local drive, it can work with StudioFS—whether that is DaVinci Resolve, Flame, Nuke, or any other application.
* **Self-hosted.** You choose your own S3 storage and PostgreSQL server. There is no lock-in to a single cloud provider.

## What does it run on?

| System | Requirement |
| -- | -- |
| **Windows** | WinFSP |
| **macOS** | macFUSE |
| **Linux** | libfuse3 |

On macOS, the main option at present is macFUSE.

StudioFS is a command-line application. Installation requires knowing how to run commands in a terminal, but you do not need to know Rust or administer servers professionally.

## What do you need for shared storage?

**S3-compatible storage** for the files themselves and **PostgreSQL** for metadata. You can deploy them on your own servers or use ready-made cloud services.

All computers must have access to these services. It is better to connect to PostgreSQL through a private network such as **Tailscale or NetBird**. Exposing PostgreSQL ports directly to the internet is not the best idea. But if you enjoy extreme sports, who are we to stop you? If you try it, please share your experience: how long did your database survive?

If you want to deploy S3 storage yourself, we test StudioFS with RustFS and NGINX. Other S3-compatible solutions should work too, but we have not had time to verify them yet. Among commercial S3-compatible storage providers, StudioFS has been tested with **Backblaze B2 and Wasabi**. We will expand the list of verified services over time.

Each workstation also needs space for a local cache, preferably on an **NVMe SSD**. The default cache limit is **100 GB**, but you can change it as needed. That does not mean StudioFS will immediately occupy all 100 GB. The local cache grows gradually from zero to the configured limit. When space gets tight, StudioFS starts deleting older cached data (not data in the primary storage!) to make room for new data. StudioFS also tries to leave at least **10% free space on the physical disk** holding the cache and clears cached data when necessary. So your SSD does not turn into a pumpkin (and slow down horribly).

## What limitations should you keep in mind?

* **Synchronization is not instant.** After you save a file, colleagues may still see the previous version for a while. Uploading the changed parts to S3 and publishing the update takes time.
* **Editing the same file simultaneously is not magic.** StudioFS does not automatically merge different edits. Divide work among artists as you would in a regular media pipeline.
* **There is no full offline mode.** You can do some work with cached data without a network connection, but new files will not download from S3, and changes will not reach the team until you pay your internet bill or connect to your neighbor's Wi-Fi.
* **Updates may break compatibility.** StudioFS is under active development, and old clients or test volumes will not always be compatible with a new version.
* **It works even on modest hardware.** Our testing team, spread across several countries, made a music video together—editing, color grading, and VFX. All of them worked with media physically stored on one old SSD connected by USB to a small server in the colorist's home. The server ran RustFS in Docker, and the whole team accessed the files through StudioFS. The only problem this experiment revealed was that RustFS stops working when the colorist's cat, Filia, chews through the cable connecting the drive it uses. The cat was not hungry—just bored. But it is better not to do this, because happiness does not last forever.

One more point about security: StudioFS automatically encrypts file contents in S3, but **StudioFS itself does not encrypt the local cache or PostgreSQL metadata**. Protecting workstations and the server remains your responsibility. Or finally hire a full-time sysadmin.

**StudioFS does not replace backups.** Before working with important material, test your pipeline and make an independent backup. Better yet, two or three.

## What is next?

In the next chapter, we will compare StudioFS with LucidLink and other solutions. Sometimes it is better to use a ready-made tool, and sometimes to build your own. With blackjack, as agreed.

---

[Contents](README.md) · [Українська](../uk/03-capabilities-limits-and-requirements.md) · [← Previous](02-how-does-this-thing-work.md) · [Next →](04-studiofs-vs-alternatives-which-tool-fits-your-workflow.md)

[Linear source](https://linear.app/valandreev/document/03-capabilities-limits-and-requirements-5008587538c8) · Source updated: 2026-10-17T22:08:06.219Z · English translation; review pending.
