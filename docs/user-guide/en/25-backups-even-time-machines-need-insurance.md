# 25. Backups: Even Time Machines Need Insurance

A snapshot helps you return to an earlier state of your files. But what if you lose the entire bucket or PostgreSQL database? That calls for a backup stored separately, one that can survive the loss of your working infrastructure.

In this chapter, we will plan that backup. There is currently no universal command to “save and restore all of StudioFS”: you need to arrange S3 and PostgreSQL backups using the tools provided by your storage and database, then test them together.

## Snapshots and backups serve different purposes

A StudioFS snapshot captures the state of a volume and lets you read it through a separate mount. It uses PostgreSQL metadata and objects in the same storage. This is convenient when you need an earlier version of a project, but it does not create an independent copy of all the data.

If the required objects or snapshot metadata disappear, a record saying the snapshot exists will not bring the files back. Likewise, S3 or PostgreSQL replication can replicate an accidental deletion. A backup needs retained earlier states and separate protection against deletion.

## What belongs in the backup?

In StudioFS, file contents and information about those files live in different places. Plan to preserve them as one set:

| Component | Why you need it |
| --- | --- |
| S3 objects | Encrypted file contents. Preserve every object needed for the chosen recovery point and any snapshots you want to keep. |
| PostgreSQL database | Folder structure, versions, volume configuration, snapshot state, and internal records. The backup must cover the entire relevant StudioFS database, including encryption keys. |
| Protected access settings and a description of the environment | Access to backup storage, PostgreSQL and S3 credentials, endpoint, bucket, region, volume identity, and StudioFS version. You need these to recreate the environment and read the data. |

**An S3 access key and a volume encryption key are different things.** The first lets you access storage; the second is needed to decrypt its contents. In the current implementation, volume key material is held in the configuration and PostgreSQL's `volume_crypto_keys` table. Do not assume that a bucket password alone is enough for recovery.

Metadata, file names, and configuration are not protected by the automatic encryption of S3 objects. PostgreSQL and configuration backups may contain secrets: encrypt them, restrict access, and keep the means to decrypt them somewhere other than the single server you are backing up.

The local cache does not replace this set either. At the same time, a computer may still hold changes that have not reached S3 or PostgreSQL. Do not delete its local state or reinstall the client until you have confirmed that the changes you need have been published.

## A consistent recovery point matters more than two successful jobs

Imagine the database is copied at 12:00 and the objects at 12:30. In between, clients change files and background cleanup deletes some old objects. Both jobs may finish successfully, yet the resulting set may no longer be suitable for recovery.

For your first tested plan, set aside a maintenance window:

1. Ask the team to save projects, close applications, and stop writes **on every client**.
2. Wait for confirmed completion of S3 uploads and metadata synchronization with PostgreSQL. A successful save in an application or a local `fsync` does not confirm this on its own. If there are errors or synchronization status is unknown, investigate first.
3. Unmount clients cleanly and check the shutdown result. A timeout or error does not confirm publication. Do not proceed while leaving data that has not been uploaded unprotected.
4. Coordinate with the administrator to stop every process that can change remote data, including the metadata server and background cleanup. In this stable state, back up PostgreSQL and the required set of S3 objects. The order and tools depend on your infrastructure.
5. Label both parts with the same backup set identifier, and record the time, StudioFS version, and verification results. Resume work only after copying is complete and you have checked the result.

This is a conservative plan that pauses work. Continuous backups also require PostgreSQL history to be coordinated with object versions and their retention. Do not assume that two independent daily backups are automatically consistent; an administrator needs to design and test that plan.

## What `dump` and `load` actually do

The CLI has a diagnostic metadata export command. For example, for a volume named `projectfs` that is already known to this computer:

```bash
studiofs dump projectfs --path-to-dump ./projectfs-metadata.json
```

The command needs this volume's local metadata and access to PostgreSQL. It opens local state, runs `flush_metadata` and `sync_metadata`, then exports the `volumes`, `metadata_state`, `counters`, and `event_log` records from PostgreSQL. **It does not copy file contents from S3 or provide a barrier that waits for uploads from every client to finish.**

The JSON includes `format_info` configuration, so treat it as a secret file. Choose a separate name for each export: exporting to the same path again replaces the previous file and does not keep a backup history.

`studiofs load` imports this JSON into PostgreSQL: it finds the volume by UUID, creates or updates its record, and replaces its `metadata_state`, `counters`, and `event_log`. This changes the database; it is not a safe way to browse an archive.

There are significant limits. The dump does not contain the full Snapshot v2 state, object usage accounting, or the object upload ledger; it also does not export the separate `volume_crypto_keys` table. Having some configuration in the JSON does not make it a full database backup. `load` refuses to replace a volume's state if the target database contains its snapshots or policies, or the corresponding object accounting and upload records. **Do not delete these records manually to bypass the check.**

So a successful `dump`, or even a successful `load`, does not guarantee complete volume recovery. Disaster recovery requires consistent objects, metadata, keys, and a tested startup procedure. That is why this chapter does not offer a universal command to restore over your working environment.

## Where to keep backups and when to delete them

Start with two practical questions: how much work can the team afford to lose, and how long can it wait for recovery? The answers determine backup frequency and the resources needed for recovery.

Keep several generations of backups. Someone may notice a mistake a week later, when the latest archive already contains its consequences. PostgreSQL, S3 objects, and the necessary keys must be retained long enough to restore the same set; old metadata without the corresponding objects will not help.

Keep at least one backup outside the working server and its normal access permissions. Where possible, use a separate account, protection against modification or deletion, and separate permissions for managing retention. Check what your provider offers: enabling versioning and object protection does not mean you have already created an independent backup.

Do not apply automatic deletion rules to working StudioFS objects without understanding the consequences. Set retention for backup storage deliberately too, accounting for every part of the set. Configure alerts for failed or overdue backups and assign someone to check them.

## Test recovery before a disaster

A backup is convincing only once you have tested reading the files. Restore one set in an **isolated environment**: a separate database, separate object storage, and a client that will not connect to the working volume. Old clients and the server must not write to the restored database or operate on its objects. Before starting, check the actual addresses and access permissions, not just the names of the test resources.

Involve the administrator in restoring the database, keys, and objects using the tools for your chosen infrastructure. Once the test client is configured, check the folder structure, open files of different sizes and formats, and compare checksums with known originals. If you preserved snapshots, test reading the snapshots you need as well. Seeing a list of names is not enough: it does not prove the contents are available and can be decrypted.

Record the set used, the steps taken, any problems found, and the actual recovery time. Repeat the test after changes to the StudioFS version, backup setup, keys, or provider. That gives the team a tested procedure and a clear understanding of exactly which work it can recover.

---

[Contents](README.md) · [Українська](../uk/25-backups-even-time-machines-need-insurance.md) · [← Previous](24-snapshots-a-time-machine-for-your-volume.md) · [Next →](26-credentials-encryption-and-access.md)

[Linear source](https://linear.app/valandreev/document/25-backups-even-time-machines-need-insurance-87d9ae41a6eb)
