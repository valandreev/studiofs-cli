# 05. Choose Your Setup

Before installing StudioFS, you need to decide **where your files will live**. There are two main options: your own S3-compatible storage or a ready-to-use commercial service.

StudioFS works the same way in both cases: S3 stores file contents, PostgreSQL stores shared metadata, and the client on each computer mounts the volume as a regular drive and caches the data it needs.

## Option 1. Your own S3 storage

If you already have servers and disk storage, you can deploy an S3-compatible service on your own infrastructure—for example, RustFS.

**Advantage:** you control the hardware, data location, and access rules. You do not have to pay a third-party provider for every gigabyte of outbound traffic.

**Disadvantage:** availability, updates, security, and backups are your responsibility. If the team works remotely, the storage needs enough network bandwidth, especially for sending data out.

This option is best suited to studios that already have infrastructure and people to maintain it.

## Option 2. A commercial S3 service

Instead of maintaining your own storage, you can rent it from a provider—for example, AWS S3 or Backblaze B2.

**Advantage:** you do not have to buy disks, configure a storage server, or ensure its physical availability yourself. Storage capacity is easier to increase as your projects grow.

**Disadvantage:** there are recurring charges. Compare more than storage prices: check **outbound traffic (egress), S3 requests, and the plan's terms**. For video work, reading large amounts of data can have a significant impact on the bill.

This is a convenient choice if you do not have your own infrastructure or do not want to administer it.

## Where should PostgreSQL live?

**PostgreSQL is required in both cases.** You can run it on your own server or VPS, or use a managed database. S3 and PostgreSQL do not have to be with the same provider.

For remote access to PostgreSQL, we recommend a private network using **Tailscale or NetBird**. Do not expose port 5432 to the public internet.

## Which should you choose?

**Do you have your own servers, storage, and the ability to maintain them?** Consider self-hosted S3.

**Want to get started faster and leave storage maintenance to a provider?** Choose a commercial S3 service after checking its pricing.

Either way, remember independent backups: **S3 is a place to store data, not an automatic guarantee against data loss.**

Made your choice? In the next chapter, we will create an S3 bucket and prepare access settings for StudioFS.

---

[Contents](README.md) · [Українська](../uk/05-choose-your-setup.md) · [← Previous](04-studiofs-vs-alternatives-which-tool-fits-your-workflow.md) · [Next →](06-s3-a-home-for-your-files.md)

[Linear source](https://linear.app/valandreev/document/05-choose-your-setup-161edffb5f51) · Source updated: 2026-10-19T16:50:42.072Z · English translation; review pending.
