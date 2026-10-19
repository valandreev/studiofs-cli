# 02. How Does This Thing Work?

StudioFS stores file contents and metadata separately. **Data** lives in S3-compatible storage. **Metadata**—file names, folder structure, sizes, and information about changes—is synchronized between computers through PostgreSQL.

Each computer keeps a local copy of the metadata and caches the parts of files it needs. This lets the whole team work in one file space without downloading the entire project to every workstation.

## S3, PostgreSQL, and the local cache

* **S3 is the warehouse.** It stores file contents: video, EXRs, 3D scenes, and everything else. StudioFS splits the data into fragments itself, so if you open S3 directly, you will not see the familiar file structure.
* **PostgreSQL is the shared catalog.** It stores information about files, folders, and changes. This is how computers know what is happening in a shared project.
* **The local cache is what you keep within reach.** StudioFS stores downloaded data and new changes that still need to be sent to the cloud on your SSD.

There is also a fourth, less visible component: **a local metadata database** on each computer. Thanks to it, StudioFS does not have to run across the internet to PostgreSQL every time you open a folder or file.

## What happens when you open a file?

Suppose an editor opens a large video file and jumps to the middle of the timeline.

StudioFS checks whether the data it needs is in the local cache. If it is, StudioFS reads it from disk. If it is not, StudioFS fetches the relevant parts from S3 and stores them locally for next time.

**You do not have to download the entire file first.** But the speed of the first read still depends on your internet connection and the S3 storage itself. On subsequent access, parts that are already cached are read immediately from the local disk.

## What about saving something?

StudioFS accepts changes on your computer first. Then, in the background, it uploads the necessary data to S3 and publishes the updated version. Other computers learn about it through PostgreSQL and read the new data from S3 as needed.

That is why **saving locally and having colleagues see your changes happen at different times**. Until synchronization finishes, someone may still see the old version of a file.

When a new computer joins the project, the `studiofs discover` command helps it obtain the shared file structure. There is no need to copy terabytes of material in advance.

## What if the internet goes down?

Under certain conditions, you can continue reading data already in the local cache. Changes accepted locally can wait until the connection returns. But data that is not on your computer cannot be downloaded without access to S3, and colleagues will not see new changes until they are published.

Most importantly, **do not delete the working cache manually**. It may contain data that has not yet reached the cloud.

StudioFS also shows synchronization status with an icon in the system tray. During active synchronization, it pulses red—this is a good time to avoid shutting down your computer. When there is no active synchronization, the icon is yellow and blue. Because everything will be Ukraine.

## What is next?

Now you know why StudioFS needs S3, PostgreSQL, and a local SSD. In the next chapter, we will look at what already works, what the limitations are, and what you need to get started.

---

[Contents](README.md) · [Українська](../uk/02-how-does-this-thing-work.md) · [← Previous](01-welcome-to-studiofs.md) · [Next →](03-capabilities-limits-and-requirements.md)

[Linear source](https://linear.app/valandreev/document/02-how-does-this-thing-work-04c31d4991eb) · Source updated: 2026-10-17T20:47:34.115Z · English translation; review pending.
