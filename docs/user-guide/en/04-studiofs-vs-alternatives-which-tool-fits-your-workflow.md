# 04. StudioFS and the Alternatives: Which Should You Choose?

Imagine an editor in Germany, a colorist in Ukraine, and a VFX artist in Poland working on the same project. There are 20 TB of media, and nobody wants to download it all to their computer first.

There are plenty of solutions on the market. But before comparing names, it helps to understand **what they actually do with files**.

## Synchronization or a shared drive?

There are two main approaches.

**Synchronization** means an application transfers files or their changed parts between computers. Each participant works with their own local copy. This is convenient when there are relatively few files or they need to be available without internet access.

**A shared file system** means everyone sees one file space, with the required parts of files downloaded as work happens. You do not necessarily need a local copy of the entire project. This is how StudioFS works.

There are also options between these two approaches: cloud drives with “online-only” files, for example, or utilities that mount S3 as a folder.

## The closest to StudioFS

**LucidLink** is probably the closest product to StudioFS in concept. It lets a remote team work with shared files as if they were on a regular drive, without downloading the entire project to every computer.

In fact, **StudioFS has the same goal as LucidLink: to give a post-production team a single workspace**, regardless of the countries its members are in.

The difference is the approach. LucidLink is a ready-made commercial product: you pay for the service and get client applications and vendor support. StudioFS is an open project that you can deploy on your own infrastructure and use with your own S3 storage. More freedom and control, but also more responsibility for setup and maintenance.

**JuiceFS** is StudioFS's closest architectural relative and inspired much of our development. Both systems use S3, a separate metadata database, and a local cache.

But there is a fundamental difference: **local-first**. In JuiceFS, a round-trip time (RTT) to the metadata server above 10 ms can make the system practically unusable for post-production tasks, especially writes. Metadata caching helps reads but does not solve the write problem.

In StudioFS, file operations run locally, and changes are synchronized through PostgreSQL in the background. So even with an RTT of 100 ms, ordinary file work does not depend on every network request.

## Why not just Dropbox or Google Drive?

Dropbox and Google Drive are great for sharing files and synchronizing working folders. Problems begin when there is **A LOT of media**.

Consider our 20 TB project. If five artists work with fully synchronized copies, each needs 20 TB of local storage. That adds up to 100 TB of disk space for a single project. And all that data still has to be downloaded over the internet.

Yes, both services let you leave files in the cloud and download them as needed. But this is not the same as a file system specifically designed for streaming work with large media files. Opening a large RAW file, for example, may require downloading considerably more data than the application needs for that particular operation.

There is a second issue: **egress**, or outbound traffic from the cloud. Cloud services have transfer quotas and limits that depend on the plan and access method. For a team reading hundreds of gigabytes or terabytes of media every day, this can become a serious constraint.

StudioFS takes a different approach: everyone sees a shared file space, but each person downloads only the parts of files needed for their work. If an editor needs a few seconds from a two-hour RAW file, there is no need to transfer the entire file to their computer first.

**Resilio and Syncthing** are a different case. They are good when you need to deliver media to several computers and keep local copies. But a synchronized folder and a single shared drive are not the same thing.

## Why not mount S3 directly?

You can. **rclone mount**, for example, lets you mount S3 as a regular drive. It supports local caching, partial reads of large files, and even many familiar file operations.

But active editing of large files makes things more complicated. Depending on the storage and configuration, changing a small part of a file may require uploading the entire object again. And when several computers work with the same files, additional questions arise about caching and consistency.

This is not rclone's fault. Making dozens of different cloud services behave like a fully fledged local file system is simply not an easy task.

There are other ways to mount S3 as a drive, too. Some are quite suitable for reading files or transferring media. But **S3 is object storage, not a regular file system**. It does not support every familiar file operation in the same form, particularly random writes within a file or renames.

You can read more about this problem in [How To Make Your Object Storage Look Like a File System](https://blog.ronin.cloud/how-to-make-your-object-storage-look-like-a-file-system/).

That is why, in **StudioFS's architecture, we separate data from metadata**. S3 stores contents, while StudioFS manages the file structure, versions, and changes itself. This makes familiar file operations possible without asking S3 to do something it was not designed for.

## When is StudioFS not the best choice?

* **You need a solution for important production work today.** StudioFS is still under active development. If you are ready to be a pioneer—welcome to the bright side! If not, my cat will bite your leg when he meets you.
* **You do not want to administer S3, PostgreSQL, and access to them.** Your own infrastructure gives you control, but it does not maintain itself.
* **You need to work offline with the entire project.** Full local copies and ordinary synchronization may be more convenient.
* **The Ukrainian flag makes you squirm.** There is probably little we can do to help you here.

## How much does it cost?

StudioFS has no mandatory license fee, but **“open source” does not mean “free storage.”** You need to pay for S3, PostgreSQL, servers, internet traffic, and spend time on setup and maintenance.

And do not confuse file-system snapshots with an independent backup—regardless of which product you choose.

---

[Contents](README.md) · [Українська](../uk/04-studiofs-vs-alternatives-which-tool-fits-your-workflow.md) · [← Previous](03-capabilities-limits-and-requirements.md) · [Next →](05-choose-your-setup.md)

[Linear source](https://linear.app/valandreev/document/04-studiofs-vs-alternatives-which-tool-fits-your-workflow-aea0d7f7308d) · Source updated: 2026-10-19T20:17:49.261Z · English translation; review pending.
