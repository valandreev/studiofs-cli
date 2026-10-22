# 02. How does it all work?
> **Work in progress**

StudioFS stores file contents and metadata separately. **Data** lives in S3-compatible storage. **Metadata**—file names, folder structure, sizes, and change information—is synchronized between computers through PostgreSQL.

Each computer has a local copy of the metadata and caches the parts of files it needs. This lets the whole team work in one shared file space without downloading the entire project to every workstation.

## S3, PostgreSQL, and the local cache

* **S3 is the warehouse.** It stores file contents: video, EXRs, 3D scenes, and everything else. StudioFS divides data into chunks, so if you look directly in S3, you will not see a familiar folder structure.
* **PostgreSQL is the shared catalog.** It stores information about files, folders, and changes. This lets computers know what is happening in one shared project.
* **The local cache keeps things close at hand.** StudioFS stores data already downloaded to your SSD there, along with new changes that still need to be sent to the cloud.

There is also a fourth, less visible piece: a **local metadata database** on each computer. It means StudioFS does not have to contact PostgreSQL over the internet every time you open a folder or file.

## What happens when you open a file?

For example, an editor opens a large video file and jumps to the middle of the timeline.

StudioFS checks whether the required data is in the local cache. If it is, it reads it from disk. If not, StudioFS fetches the relevant parts from S3 and saves them locally for next time.

**You do not have to download the entire file first.** But the speed of the first read still depends on your internet connection and the S3 storage itself. Parts of the file that are already cached are read immediately from the local drive on later access.

## What happens when you save something?

StudioFS first accepts changes on your computer. It then uploads the required data to S3 and publishes the updated version in the background. Other computers learn about it through PostgreSQL and, when needed, read the new data from S3.

That means **saving locally and changes appearing for your colleagues happen at different times**. Until synchronization finishes, someone else may still see the old version of the file.

When a new computer joins the project, the `studiofs discover` command helps it get the shared file structure. There is no need to copy terabytes of media in advance.

## What if the internet goes down?

Under certain conditions, you can keep reading data that is already in the local cache. Accepted local changes can wait until the connection comes back. But data that is not on your computer cannot be downloaded without access to S3, and colleagues will not see new changes until they are published.

And most importantly: **do not delete the working cache manually**. It may contain data that has not reached the cloud yet.

StudioFS also shows synchronization status with an icon in the system tray. During active synchronization, it pulses red—so it is better not to turn off the computer at that moment. When there is no active synchronization, the icon is blue and yellow. Because Ukraine will prevail.

## What’s next?

Now you know why StudioFS needs S3, PostgreSQL, and a local SSD. In the next chapter, we will look at what already works, what the limitations are, and what you need to get started.

---

[Contents](README.md) · [Українська](../uk/02-how-does-this-thing-work.md) · [← Previous](01-welcome-to-studiofs.md) · [Next →](03-capabilities-limits-and-requirements.md)
