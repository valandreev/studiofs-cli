# 25. Migrating StudioFS Metadata to a New PostgreSQL Server
> **Work in progress**

This chapter describes moving StudioFS metadata to a new PostgreSQL server while keeping the existing S3 storage. Clients are temporarily stopped, their local state is removed, and after the migration each client reconnects to the volume using `discover`.

`dump` and `load` move only part of StudioFS metadata. They do not copy files from S3 and are not a full PostgreSQL backup. If you need a complete copy of the database, or the volume contains data these commands do not move, use PostgreSQL's standard migration mechanism.

## Before you begin

1. Agree on a short maintenance window and ask all users to save their work and close applications that use StudioFS.
2. Unmount the volume cleanly on every client. Check for errors and uploads that have not finished reaching S3. `dump` synchronizes metadata with PostgreSQL, but does not upload file data to S3.
3. Prepare the new PostgreSQL server and a DSN with the required permissions. Make sure the clients will be configured to use this server afterward.

## Migration

Follow the steps in order. Run `dump` **before** `destroy --local` on the computer performing the export: the command needs that computer's local volume metadata database.

1. On one client that has local volume state, create a dump:

   ```bash
   studiofs dump projectfs --path-to-dump ./projectfs-metadata.json
   ```

   Replace `projectfs` with the volume name. Store the JSON in a secure location: it contains volume configuration. Do not export again to the same path if you want to keep the previous file.

2. After the export succeeds, delete local metadata on **all** clients, including the one where you ran `dump`:

   ```bash
   studiofs destroy projectfs --local
   ```

   This command deletes that client's local metadata and cache, but leaves shared PostgreSQL metadata and S3 data in place. Do not run it if the client still has changes that have not been uploaded.

3. Load the dump into the new PostgreSQL server:

   ```bash
   studiofs load --path-to-dump ./projectfs-metadata.json --postgresql-address "<new-postgres-dsn>"
   ```

   The command changes data in the database specified by the DSN. Check the address before running it, and do not point clients to the new server until loading has completed successfully.

4. On each client, recreate local state from the new server:

   ```bash
   studiofs discover projectfs --meta "<new-postgres-dsn>"
   ```

5. Confirm that clients see the required structure and can open files. Only then resume normal work.

## Limits of `dump` and `load`

`dump` synchronizes this client's local metadata, then exports the `volumes`, `metadata_state`, `counters`, and `event_log` records from PostgreSQL. `load` applies this data to the target database and matches the volume by UUID. These commands do not move file contents into S3.

The dump also omits some StudioFS tables, including the separate `volume_crypto_keys` table, the full Snapshot v2 state, and object accounting. Therefore, a successful `load` does not by itself prove that all data required by a particular volume was migrated. If the volume needs these records, or `load` rejects the target database, do not delete or change tables manually: use a full PostgreSQL migration that preserves all tables, and move the access settings for the same S3 storage.

Treat the dump and DSN as sensitive data: restrict access to the JSON, and do not put a password from the DSN in shared logs or documentation.

---

[Contents](README.md) · [Українська](../uk/25-migrating-studiofs-metadata-to-new-postgresql-server.md) · [← Previous](24-snapshots-a-time-machine-for-your-volume.md) · [Next →](26-credentials-encryption-and-access.md)
