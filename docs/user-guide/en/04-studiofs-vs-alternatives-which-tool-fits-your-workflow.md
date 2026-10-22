# 04. StudioFS and the alternatives: which should you choose?
> **Work in progress**

Imagine an editor in Germany, a colorist in Ukraine, and a VFX artist in Poland working on one project. There are 20 TB of media, and nobody wants to download all of it to their computer first.

There are many solutions on the market. But before comparing product names, it helps to understand **what they do with files**.

## Synchronization or a shared drive?

There are two main approaches.

**Synchronization** moves files or their changed parts between computers. Each person works with a local copy. This is convenient when there are not many files, or when they need to be available without internet access.

A **shared file system** gives everyone one file space, while the parts of files they need are downloaded as they work. You do not need to keep a local copy of the entire project. StudioFS uses this approach.

There are also solutions between these two approaches, such as cloud drives with “online-only” files or utilities that mount S3 as a folder.

## The closest options to StudioFS

**LucidLink** is probably the product closest to StudioFS in concept. It lets a remote team work with shared files as if they were on a regular drive, without downloading the entire project to every computer.

In fact, **StudioFS has the same goal as LucidLink: to give a post-production team one shared workspace**, wherever its members are in the world.

The difference is in the approach. LucidLink is a ready-made commercial product: you pay for the service and get client applications and vendor support. StudioFS is an open project that you can deploy on your own infrastructure and use with your own S3 storage. That gives you more freedom and control, but also more responsibility for setup and maintenance.

**JuiceFS** is StudioFS’s closest architectural relative and inspired much of our development. Both systems use S3, a separate metadata database, and a local cache.

But there is a fundamental difference: **local-first design**. In JuiceFS, a round-trip time (RTT) to the metadata server above 10 ms can make the system practically unsuitable for post-production work, especially when writing. Metadata caching helps with reads, but does not solve the write problem.

In StudioFS, file operations happen locally, and changes synchronize through PostgreSQL in the background. So even with a 100 ms RTT, regular file work does not depend on every network request.

## Why not just use Dropbox or Google Drive?

Dropbox and Google Drive are great for sharing files and synchronizing work folders. Problems start when you have **a LOT** of media.

Imagine our 20 TB project. If five artists work from fully synchronized copies, each needs 20 TB of local storage. That is 100 TB of disk space for one project, and all that data still has to be downloaded over the internet.

Yes, both services let you keep files in the cloud and download them on demand. But that is not the same as a file system designed specifically for streaming large media files. For example, opening a large RAW file may require downloading significantly more data than the application needs for a particular operation.

There is a second problem: **egress**, or outbound traffic from the cloud. Cloud services have data-transfer quotas and limits that depend on the plan and access method. For a team that reads hundreds of gigabytes or terabytes of media every day, this can become a serious limitation.

StudioFS takes a different approach: everyone sees a shared file space, but each person downloads only the parts of files needed for their work. If an editor needs a few seconds from a two-hour RAW file, there is no need to move the entire file to their computer first.

**Resilio and Syncthing** are a different case. They are good when you need to deliver media to several computers and keep local copies. But a synchronized folder and one shared drive are not the same thing.

## Why not mount S3 directly?

You can. For example, **rclone mount** lets you mount S3 as a regular drive. It supports local caching, partial reads of large files, and even many familiar file operations.

But active editing of large files makes things more complicated. Depending on the storage and configuration, changing a small part of a file may require uploading the entire object again. And when several computers work with the same files, additional questions arise around caching and consistency of changes.

This is not rclone’s fault. Making dozens of different cloud services behave like a full local file system is simply a difficult problem.

There are other ways to mount S3 as a drive, too. Some of them work perfectly well for reading files or transferring media. But **S3 is object storage, not a regular file system**. It does not support all familiar file operations in the same way, including arbitrary writes within a file (*random writes*) or renaming (*rename*).

For more detail, see [How To Make Your Object Storage Look Like a File System](<https://blog.ronin.cloud/how-to-make-your-object-storage-look-like-a-file-system/>).

That is why **StudioFS separates data from metadata** in its architecture. S3 stores file contents, while StudioFS manages the file structure, versions, and changes. This makes familiar file operations possible without asking S3 to do something it was not designed for.

## When is StudioFS not the best choice?

* **You need a solution for important production work today.** StudioFS is still under active development. If you are ready to be a pioneer, welcome to the bright side! If not, my cat will bite your leg when it sees you.
* **You do not want to administer S3, PostgreSQL, and access to them.** Your own infrastructure gives you control, but it does not maintain itself.
* **You need to work offline with the entire project.** Full local copies and regular synchronization may be more convenient in that case.
* **The Ukrainian flag makes you cringe.** We probably cannot help with that.

## How much does it cost?

StudioFS has no mandatory license fee, but **“open source” does not mean “free storage.”** You still need to pay for S3, PostgreSQL, servers, and internet traffic, and spend time on setup and maintenance.

And do not confuse file-system snapshots with an independent backup, whatever product you choose.

---

[Contents](README.md) · [Українська](../uk/04-studiofs-vs-alternatives-which-tool-fits-your-workflow.md) · [← Previous](03-capabilities-limits-and-requirements.md) · [Next →](05-choose-your-setup.md)

[Linear source](https://linear.app/valandreev/document/04-studiofs-vs-alternatives-which-tool-fits-your-workflow-aea0d7f7308d) · Source updated: 2026-10-19T20:17:49.261Z · Ukrainian original approved for translation.
