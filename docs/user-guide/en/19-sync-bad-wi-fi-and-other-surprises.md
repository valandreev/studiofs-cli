# 19. Sync, Bad Wi-Fi, and Other Surprises
> **Work in progress**

Synchronization in StudioFS runs automatically in the background. For ordinary work, you do not need to run a separate command, select files to transfer, or configure synchronization manually. StudioFS sends file contents to S3 and publishes metadata changes through PostgreSQL when those services are available.

## When will a colleague see the changes?

Usually, changes appear for your colleague almost immediately: StudioFS synchronizes them automatically in the background. A large amount of data or an unstable connection may add a delay, but you still do not need to start synchronization manually.

## If the connection goes down

Files already downloaded to the cache may remain available, but parts that are not local cannot be fetched without access to storage. Changes that have not been sent will wait for the connection to return, after which StudioFS will resume synchronization automatically. This is not a guarantee of full offline operation; if too many changes accumulate or there is not enough space, new writes may slow down.

Do not delete the cache or local metadata database to “fix” synchronization, and do not initialize the volume again. They may contain changes that have not yet reached shared storage.

## If changes take a long time to appear

Check access to the network, S3, and PostgreSQL. For diagnostics, you can view the volume status with `studiofs status myvolume`. It reports status, but does not start or speed up synchronization. Before handing work over, check the required version on the recipient's computer.

In the next chapter, we will take a closer look at the StudioFS local cache.

---

[Contents](README.md) · [Українська](../uk/19-sync-bad-wi-fi-and-other-surprises.md) · [← Previous](18-studiofs-workloads.md) · [Next →](20-cache-why-is-the-cloud-using-my-ssd.md)
