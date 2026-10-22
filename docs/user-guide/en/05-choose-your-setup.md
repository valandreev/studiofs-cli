# 05. Choosing your setup
> **Work in progress**

Before installing StudioFS, you need to decide **where your files will live**. There are two main options: your own S3-compatible storage or a ready-made commercial service.

StudioFS works the same way in either case: S3 stores file contents, PostgreSQL stores shared metadata, and the client on each computer mounts the volume as a regular drive and caches the data it needs.

## Option 1: Your own S3 storage

If you already have servers and disk storage, you can deploy an S3-compatible service on your own infrastructure—for example, RustFS.

**Advantage:** You control the hardware, where the data lives, and the access rules. You do not have to pay a third-party provider for every gigabyte of outbound traffic.

**Disadvantage:** Availability, updates, security, and backups are your responsibility. If your team works remotely, the storage needs enough network bandwidth, especially for outgoing data.

This option works best for studios that already have infrastructure and people to maintain it.

## Option 2: Commercial S3 service

Instead of maintaining your own storage, you can rent it from a provider—for example, AWS S3 or Backblaze B2.

**Advantage:** You do not need to buy disks, configure a storage server, or ensure its physical availability yourself. It is easier to increase storage as your projects grow.

**Disadvantage:** There are recurring charges. Compare more than the storage price: look at **outbound traffic (egress), S3 requests, and the plan terms**. With video workflows, reading large amounts of data can have a significant effect on the bill.

This is a convenient choice if you do not have your own infrastructure or do not want to administer it.

## Where should you host PostgreSQL?

**PostgreSQL is required in both setups.** You can run it on your own server or VPS, or use a managed database. S3 and PostgreSQL do not have to be with the same provider.

For remote PostgreSQL access, we recommend a private network such as **Tailscale or NetBird**. Do not expose port 5432 to the public internet.

## Which should you choose?

**Do you have your own servers and storage, and can you maintain them?** Consider self-hosted S3.

**Do you want to get started sooner and let a provider maintain the storage?** Choose a commercial S3 service after checking its pricing.

Either way, remember to keep independent backups: **S3 is a place to store data, not an automatic guarantee against data loss.**

Made your choice? In the next chapter, we will create an S3 bucket and prepare access settings for StudioFS.

---

[Contents](README.md) · [Українська](../uk/05-choose-your-setup.md) · [← Previous](04-studiofs-vs-alternatives-which-tool-fits-your-workflow.md) · [Next →](06-s3-a-home-for-your-files.md)
