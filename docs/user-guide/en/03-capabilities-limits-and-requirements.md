# 03. Capabilities, limitations, and system requirements
> **Work in progress**

StudioFS can already do the essentials: connect shared storage as a regular drive, read large media files without downloading them in full, and transfer changes between computers. But it is still a young project, so it is best to understand the limitations before moving all your projects onto it.

## What already works?

* **A shared file space.** Multiple computers work with the same files and folders. There is no need to maintain a separate project copy on every workstation.
* **Partial reads.** You can open a large file without downloading it completely. StudioFS fetches the required parts from S3 and keeps the data it has read in the local cache.
* **Local writes and background synchronization.** The application saves changes on your computer, and StudioFS uploads them to storage and publishes them for colleagues in the background.
* **Works with regular applications.** StudioFS looks like a regular local drive to any application. You do not need to change anything or install extra plugins. If an application can work with files on a local drive, it can work with StudioFS—whether it is DaVinci Resolve, Flame, Nuke, or something else.
* **Self-hosted.** You choose the S3 storage and PostgreSQL server. You are not tied to a single cloud provider.

## What can it run on?

| System | Requirement |
| -- | -- |
| **Windows** | WinFSP |
| **macOS** | macFUSE |
| **Linux** | libfuse3 |

For macOS, macFUSE is currently the primary option.

StudioFS is a command-line application. You will need to know how to run commands in a terminal to install it, but you do not need to know Rust or be a professional server administrator.

## What do you need for shared storage?

You need **S3-compatible storage** for the files themselves and **PostgreSQL** for metadata. You can run them on your own servers or use hosted cloud services.

All computers need access to these services. It is best to connect to PostgreSQL through a private network such as **Tailscale or NetBird**. Exposing PostgreSQL ports directly to the internet is not a great idea. But if you enjoy extreme sports, who are we to stop you? If you try it, please share how long your database lasted.

If you want to run S3 storage yourself, we test StudioFS with RustFS and NGINX. Other S3-compatible solutions should work too, but we have not had a chance to test them yet. Among commercial S3-compatible storage providers, StudioFS has been tested with **Backblaze B2 and Wasabi**. We will expand the list of verified services over time.

Each workstation also needs space for a local cache, preferably on an **NVMe SSD**. The default cache limit is **100 GB**, but you can change it if needed. This does not mean StudioFS will immediately use all 100 GB. The local cache grows gradually from zero up to the configured limit. When space runs low, StudioFS starts removing older cached data (not data from the primary storage!) to make room for new data. StudioFS also tries to keep at least **10% of free space on the physical disk** that holds the cache, and clears the cache when needed. That keeps your SSD from turning into a pumpkin (and becoming painfully slow).

## What limitations should you keep in mind?

* **Synchronization is not instant.** After a file is saved, colleagues may continue seeing the previous version for a while. It takes time to upload the changed parts of the file to S3 and publish the update.
* **Simultaneous editing of one file is not magic.** StudioFS does not automatically merge different edits. Divide the work among artists as you would in a regular media pipeline.
* **There is no full offline mode.** You can work with cached data to some extent while offline, but new files will not download from S3, and changes will not reach the team until you pay for internet or connect to your neighbor’s Wi-Fi.
* **Updates may break compatibility.** StudioFS is under active development, and older clients or test volumes may not always be compatible with a new version.
* **It even works on modest hardware.** Our team of testers from different countries made a music video together—editing, color grading, and VFX. They all worked with media physically stored on one old SSD, connected over USB to the colorist’s small home server. RustFS ran on that server in Docker, and the whole team accessed the files through StudioFS. The only problem this experiment uncovered was that RustFS stops working if the colorist’s cat, Filya, chews through the cable connecting the drive he uses. The cat was not hungry; he was just bored. Still, we do not recommend doing this, because happiness never lasts forever.

And a note about security: StudioFS automatically encrypts file contents in S3, but **StudioFS itself does not encrypt the local cache or PostgreSQL metadata**. Protecting your workstations and server is your responsibility. Or finally hire a sysadmin.

**StudioFS is not a substitute for backups.** Before working with important media, check your pipeline and make an independent backup. Better yet, make two or three.

## What’s next?

In the next chapter, we will compare StudioFS with LucidLink and other solutions. Sometimes a ready-made tool is the better choice; sometimes it makes sense to build your own. With blackjack, as promised.

---

[Contents](README.md) · [Українська](../uk/03-capabilities-limits-and-requirements.md) · [← Previous](02-how-does-this-thing-work.md) · [Next →](04-studiofs-vs-alternatives-which-tool-fits-your-workflow.md)

[Linear source](https://linear.app/valandreev/document/03-capabilities-limits-and-requirements-5008587538c8) · Source updated: 2026-10-17T22:08:06.219Z · Ukrainian original approved for translation.
