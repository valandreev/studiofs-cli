# Metadata Engine

The metadata subsystem (`studiofs-meta`) provides all filesystem metadata operations through the `MetaEngine` trait, with two implementations for local and distributed use.

## MetaEngine Trait

All methods are **synchronous** — the VFS wraps them with `spawn_blocking` for async integration. This keeps the metadata layer simple and transactional.

### Operation Groups

| Group | Key Methods |
|-------|-------------|
| **Format** | `set_format_info`, `get_format_info` |
| **Counters** | `get_counter`, `incr_counter`, `decr_counter`, `set_counter` |
| **Inodes** | `next_inode`, `get_attr`, `set_attr`, `remove_attr` |
| **Directory entries** | `lookup`, `readdir`, `add_entry`, `remove_entry` |
| **File blocks** | `get_file_block`, `set_file_block`, `get_file_blocks`, `remove_file_blocks`, `next_block_id`, `next_block_ids`, `flush_blocks_batch` |
| **GC** | `add_pending_deletion`, `remove_pending_deletion`, `get_pending_deletions`, `get_referenced_object_ids` |
| **High-level** | `create_file`, `create_dir`, `unlink`, `rmdir`, `rename`, `link` |
| **Sync** | `flush_metadata`, `sync_metadata` |

### Error Types

| Error | Description |
|-------|-------------|
| `InodeNotFound` | Inode does not exist |
| `EntryNotFound` | Directory entry not found |
| `EntryExists` | Name already exists (create/link) |
| `NotADirectory` | Operation requires a directory |
| `DirNotEmpty` | Cannot remove non-empty directory |
| `IsADirectory` | Operation not valid on directory |
| `Storage` | Underlying storage error |
| `Serialization` | Encode/decode error |
| `NotFormatted` | Database not initialized |

## Engines

### RedbMetaEngine

Local-only engine backed by [redb](https://github.com/cberner/redb), an embedded key-value store.

**Database tables (9):**

| Table | Key | Value | Description |
|-------|-----|-------|-------------|
| `settings` | `&str` | JSON `&[u8]` | Volume config (`FormatInfo`) |
| `counters` | `&str` | `u64` | Named counters |
| `inodes` | `u64` | postcard `&[u8]` | Inode attributes |
| `dentries` | `parent_ino+name` | postcard `&[u8]` | Directory entries |
| `file_blocks` | `ino+block_idx` (12B) | postcard `&[u8]` | Block references |
| `pending_deletions` | `u64` | `i64` | GC pending objects |
| `symlinks` | `u64` | `&[u8]` | Symlink targets |
| `xattrs` | `ino+name` | `&[u8]` | Extended attributes |
| `event_log` | `u64` (seq) | postcard `&[u8]` | Event sourcing log |

All operations execute within redb write transactions for ACID guarantees.

**Additional methods:**
- `open(path)` — open or create database
- `init_root(uid, gid)` — create root inode (#1) if not present
- `export_snapshot()` → `RedbSnapshot` — full database dump
- `import_snapshot(snapshot)` — replace all data from snapshot

### TonboMetaEngine

Distributed engine wrapping `RedbMetaEngine` with remote S3 manifest synchronization.

**Architecture:**
```
Local redb (read/write cache)
       ↕  batch flush
Remote S3 manifest (shared source of truth)
       ↕  invalidation
Optional NATS bus
```

**Key fields:**
- `local: Arc<RedbMetaEngine>` — all reads/writes go here first
- `remote: Option<RemoteStore>` — S3 manifest storage
- `baseline_snapshot` — last known remote state (for delta computation)
- `inode_ranges / block_ranges` — pre-allocated ID ranges to avoid remote contention

**Flush process:**

1. Compute delta between current redb state and baseline snapshot
2. Serialize delta and PUT to S3 with `If-Match: <etag>` (CAS)
3. On success: update baseline, reset counters
4. On 412 conflict: pull remote manifest, merge with local changes, retry
5. Optional: publish `fs.invalidate.<volume>.all` to NATS (Core or JetStream mode)

**Range-based ID allocation:**

To avoid contending on remote counters for every `next_inode()` / `next_block_id()`:
- Pre-allocate ranges from remote: 4,096 inodes, 65,536 block IDs at a time
- Use CAS on the remote counter to claim a range
- Serve local calls from the pre-allocated range
- Retry up to 10 times on contention

**Metrics tracked:**
- `remote_round_trips`, `cas_retries` — sync performance
- `cache_hits`, `cache_misses`, `ttl_expirations` — cache effectiveness
- `flush_cycles` — total flushes performed

## Schema (`schema.rs`)

### Constants

| Constant | Value |
|----------|-------|
| `DEFAULT_BLOCK_SIZE` | 1 MiB (1,048,576) |
| `ROOT_INO` | 1 |

### Core Types

**`Ino`** = `u64` — inode number

**`FileType`** — `RegularFile` (1), `Directory` (2), `Symlink` (3)

**`InodeAttr`** — full inode metadata:
- `ino`, `kind` (FileType), `mode`, `uid`, `gid`
- `size`, `nlink`
- `mtime`, `ctime`, `atime`, `crtime` (nanosecond timestamps)
- `parent` (parent inode)
- `win_attrs` (Windows file attributes, `u32`)

**`DirEntryValue`** — stored directory entry: `{ ino, kind }`

**`BlockRef`** — reference to a data block:
- `object_id: u64` — sequential object ID
- `offset: u64` — offset within object (currently always 0, one block per object)
- `length: u32` — uncompressed data length
- `flags: BlockFlags` — `COMPRESSED_LZ4` (0x01), `PARTIAL_BLOCK` (0x02)

**`FormatInfo`** — volume configuration persisted in redb `settings` table:
- `name`, `uuid` — volume identity
- `endpoint`, `bucket`, `access_key`, `secret_key`, `region` — data S3 config
- `data_prefix` — S3 key prefix for data objects
- `metadata: MetadataConfig` — backend type + remote config + tuning
- `block_size` — block size in bytes
- `version` — format version for migration

**`MetadataConfig`:**
- `backend` — `RedbLocal` or `Tonbo`
- `remote: Option<RemoteStoreConfig>` — S3 config for Tonbo manifest
- `cache_ttl_secs` — read cache TTL (default 20s)
- `flush_interval_secs` — batch flush interval (default 60s)
- `flush_ops_threshold` — flush after N operations (default 100)
- `flush_bytes_threshold` — flush after N bytes changed (default 1 MiB)

### Named Counters

| Counter | Purpose |
|---------|---------|
| `NEXT_INODE` | Next inode number to allocate |
| `NEXT_BLOCK_ID` | Next object ID to allocate |
| `USED_SPACE` | Total bytes used by file data |
| `TOTAL_INODES` | Total inodes in filesystem |

### Helper Functions

- `block_index_from_offset(offset, block_size)` → block index
- `offset_within_block(offset, block_size)` → offset within block
- `blocks_for_size(size, block_size)` → number of blocks

## Events (`events.rs`)

Event-sourced mutation log for Tonbo replication.

**`MetaEvent` variants:**

| Category | Events |
|----------|--------|
| Inodes | `SetAttr`, `RemoveAttr` |
| Directory | `AddEntry`, `RemoveEntry` |
| Blocks | `SetFileBlock`, `RemoveFileBlock`, `RemoveFileBlocks`, `SetFileBlocksBatch` |
| Symlinks | `SetSymlinkTarget` |
| Extended attrs | `SetXattr`, `RemoveXattr` |
| GC | `AddPendingDeletion`, `RemovePendingDeletion` |

Each event has a `primary_ino()` method for partitioning/ordering.

## Multi-Client Consistency

### Without NATS

- Writes are locally consistent (redb ACID transactions)
- Remote consistency via periodic flush (CAS ensures no lost updates)
- Read staleness bounded by `cache_ttl_secs` (default 20s)
- On cache miss, immediate remote refresh

### With NATS

- After successful metadata flush, publisher broadcasts `fs.invalidate.<volume>.all`
- Subscribers run immediate metadata sync and invalidate read caches
- Block cache eviction is selective: dropped object IDs are evicted, unchanged blocks remain cached
- Effective staleness is usually bounded by flush latency + NATS delivery + sync time
- NATS is optional — correctness never depends on it

For full flow, subject model, and tuning guidance, see [NATS Invalidation Bus](nats.md).
