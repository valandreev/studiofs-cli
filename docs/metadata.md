# Metadata Engine

`studiofs-meta` provides synchronous metadata operations through `MetaEngine`.

## Engines

### `RedbMetaEngine`

Local embedded metadata store.
- ACID transactions in local DB.
- Source for foreground reads/writes in client path.
- Contains inode/dentry/file_blocks/counters/xattrs/symlinks/event_log tables.

### `PgMetaEngine`

Distributed metadata engine for multi-client mode.

Current behavior:
- Foreground operations (`create`, `rename`, `unlink`, etc.): local `redb` only.
- Every mutation appends to local event log.
- Background worker batches pending mutations and flushes merged snapshot to PostgreSQL.
- `sync_metadata()` is pull-only from PostgreSQL snapshot.
- Eventual consistency across clients is achieved by ordered PostgreSQL transactions.

## Sync Model

### Flush (background)

1. Read pending local events range from local event log.
2. Open PostgreSQL transaction.
3. Lock/read current remote snapshot.
4. Merge pending local events over remote snapshot.
5. Upsert merged snapshot into `metadata_state` with version increment.
6. Update local `redb` with committed merged snapshot.

### Pull (`sync_metadata`)

- If no local pending mutations: pull newer snapshot from `metadata_state`.
- If local pending exists: skip pull to avoid overwriting unflushed local changes.

## PostgreSQL Tables (used)

- `volumes`
- `counters`
- `event_log`
- `metadata_state`

## Consistency

- Local operations are immediate and synchronous (low latency).
- Cross-client visibility is eventual.
- Serialization/conflict resolution is handled by PostgreSQL transaction order.

## MetadataConfig (important fields)

- `backend = Postgres`
- `postgres.url`
- `postgres.volume_id`
- `cache_ttl_secs`
- `flush_interval_secs`
- `flush_ops_threshold`
- `flush_bytes_threshold`
