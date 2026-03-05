use crate::engine::{BatchFlushEntry, BatchFlushResult, MetaEngine, MetaError, MetaResult};
use crate::events::MetaEvent;
use crate::redb_engine::{RedbMetaEngine, RedbSnapshot};
use crate::types::*;
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
static NEXT_PG_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// PostgreSQL-backed metadata engine.
///
/// Current implementation strategy:
/// - Redb remains the local serving store (hot reads)
/// - every mutating op is applied to redb first, then write-through to PG
/// - PG stores the authoritative serialized snapshot per volume
/// - background catch-up thread periodically pulls newer PG snapshot versions
pub struct PgMetaEngine {
    local: Arc<RedbMetaEngine>,
    postgres_url: String,
    volume_id: i64,
    client_id: String,
    local_version: AtomicI64,
    op_lock: Mutex<()>,
    poll_interval: Duration,
    flush_interval: Duration,
    flush_ops_threshold: u32,
    pending_ops: std::sync::atomic::AtomicU32,
    pending_started_at: Mutex<Option<Instant>>,
    force_flush: std::sync::atomic::AtomicBool,
    pending_event_start: Mutex<Option<u64>>,
    pending_event_end: AtomicU64,
}

impl PgMetaEngine {
    fn reserve_counter_range_base(slot: u64, stride: u64, min_value: u64) -> u64 {
        let base = slot.saturating_mul(stride);
        base.max(min_value)
    }

    fn prime_local_id_ranges(&self, slot: u64) -> MetaResult<()> {
        // Keep per-client ID spaces disjoint so background event-merge does not
        // collide on inode/block keys when multiple clients mutate concurrently.
        let inode_base = Self::reserve_counter_range_base(slot, 1_000_000_000, ROOT_INO + 1);
        let block_base = Self::reserve_counter_range_base(slot, 10_000_000_000, 1);

        let next_inode = self.local.get_counter(counters::NEXT_INODE)?;
        if next_inode < inode_base {
            self.local.set_counter(counters::NEXT_INODE, inode_base)?;
        }
        let next_block = self.local.get_counter(counters::NEXT_BLOCK_ID)?;
        if next_block < block_base {
            self.local
                .set_counter(counters::NEXT_BLOCK_ID, block_base)?;
        }
        Ok(())
    }

    pub fn open(path: impl AsRef<Path>) -> MetaResult<Arc<Self>> {
        let local = Arc::new(RedbMetaEngine::open(path)?);
        let info = local.get_format_info()?;
        if info.metadata.backend != MetadataBackend::Postgres {
            return Err(MetaError::Storage(
                "PgMetaEngine open requested for non-Postgres volume".to_string(),
            ));
        }

        let pg = info.metadata.postgres.ok_or_else(|| {
            MetaError::Storage("missing metadata.postgres config for Postgres backend".to_string())
        })?;
        if pg.url.trim().is_empty() {
            return Err(MetaError::Storage(
                "metadata.postgres.url must not be empty".to_string(),
            ));
        }

        let poll_secs = std::env::var("STUDIOFS_PG_SYNC_POLL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(2);
        let flush_interval = Duration::from_secs(info.metadata.flush_interval_secs.max(1));
        let flush_ops_threshold = info.metadata.flush_ops_threshold.max(1);

        let client_slot = NEXT_PG_CLIENT_ID.fetch_add(1, Ordering::SeqCst);
        let engine = Arc::new(Self {
            local,
            postgres_url: pg.url,
            volume_id: pg.volume_id,
            client_id: format!("pid:{}:{client_slot}", std::process::id()),
            local_version: AtomicI64::new(0),
            op_lock: Mutex::new(()),
            poll_interval: Duration::from_secs(poll_secs),
            flush_interval,
            flush_ops_threshold,
            pending_ops: std::sync::atomic::AtomicU32::new(0),
            pending_started_at: Mutex::new(None),
            force_flush: std::sync::atomic::AtomicBool::new(false),
            pending_event_start: Mutex::new(None),
            pending_event_end: AtomicU64::new(0),
        });

        // Initial catch-up from PG and start periodic background catch-up.
        let _ = engine.sync_metadata();
        let _ = engine.prime_local_id_ranges(client_slot);
        Self::start_background_poll(Arc::downgrade(&engine), engine.poll_interval);
        Ok(engine)
    }

    pub fn postgres_url(&self) -> &str {
        &self.postgres_url
    }

    pub fn volume_id(&self) -> i64 {
        self.volume_id
    }

    fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    fn start_background_poll(engine: std::sync::Weak<Self>, interval: Duration) {
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(interval);
                let Some(engine) = engine.upgrade() else {
                    break;
                };
                let _ = engine.flush_due_pending();
                let _ = engine.sync_metadata();
            }
        });
    }

    fn run_async<T, F>(&self, fut: F) -> MetaResult<T>
    where
        F: std::future::Future<Output = MetaResult<T>>,
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| MetaError::Storage(format!("failed to build tokio runtime: {e}")))?;
        rt.block_on(fut)
    }

    fn encode_snapshot(snapshot: &RedbSnapshot) -> MetaResult<Vec<u8>> {
        serde_json::to_vec(snapshot)
            .map_err(|e| MetaError::Serialization(format!("snapshot encode failed: {e}")))
    }

    fn decode_snapshot(bytes: &[u8]) -> MetaResult<RedbSnapshot> {
        serde_json::from_slice(bytes)
            .map_err(|e| MetaError::Serialization(format!("snapshot decode failed: {e}")))
    }

    fn encode_postcard<T: serde::Serialize>(val: &T) -> MetaResult<Vec<u8>> {
        postcard::to_stdvec(val)
            .map_err(|e| MetaError::Serialization(format!("postcard encode failed: {e}")))
    }

    fn encode_dentry_key(parent: Ino, name: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(8 + name.len());
        key.extend_from_slice(&parent.to_le_bytes());
        key.extend_from_slice(name);
        key
    }

    fn encode_file_block_key(ino: Ino, block_index: u32) -> [u8; 12] {
        let mut buf = [0u8; 12];
        buf[..8].copy_from_slice(&ino.to_le_bytes());
        buf[8..12].copy_from_slice(&block_index.to_le_bytes());
        buf
    }

    fn encode_xattr_key(ino: Ino, name: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(8 + name.len());
        key.extend_from_slice(&ino.to_le_bytes());
        key.extend_from_slice(name);
        key
    }

    fn merge_snapshot_with_events(
        mut remote: RedbSnapshot,
        local: &RedbSnapshot,
        events: &[MetaEvent],
    ) -> MetaResult<RedbSnapshot> {
        let mut inodes: std::collections::HashMap<u64, Vec<u8>> = remote.inodes.drain(..).collect();
        let mut dentries: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            remote.dentries.drain(..).collect();
        let mut file_blocks: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            remote.file_blocks.drain(..).collect();
        let mut pending_deletions: std::collections::HashMap<u64, i64> =
            remote.pending_deletions.drain(..).collect();
        let mut symlinks: std::collections::HashMap<u64, Vec<u8>> =
            remote.symlinks.drain(..).collect();
        let mut xattrs: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            remote.xattrs.drain(..).collect();

        for ev in events {
            match ev {
                MetaEvent::SetAttr { attr } => {
                    inodes.insert(attr.ino, Self::encode_postcard(attr)?);
                }
                MetaEvent::RemoveAttr { ino } => {
                    inodes.remove(ino);
                    symlinks.remove(ino);
                    let ino_prefix = ino.to_le_bytes();
                    xattrs.retain(|k, _| !k.starts_with(&ino_prefix));
                }
                MetaEvent::AddEntry {
                    parent,
                    name,
                    entry,
                } => {
                    dentries.insert(
                        Self::encode_dentry_key(*parent, name),
                        Self::encode_postcard(entry)?,
                    );
                }
                MetaEvent::RemoveEntry { parent, name } => {
                    dentries.remove(&Self::encode_dentry_key(*parent, name));
                }
                MetaEvent::SetFileBlock {
                    ino,
                    block_index,
                    block,
                } => {
                    file_blocks.insert(
                        Self::encode_file_block_key(*ino, *block_index).to_vec(),
                        Self::encode_postcard(block)?,
                    );
                }
                MetaEvent::RemoveFileBlock { ino, block_index } => {
                    file_blocks.remove(&Self::encode_file_block_key(*ino, *block_index).to_vec());
                }
                MetaEvent::RemoveFileBlocks { ino } => {
                    let prefix = ino.to_le_bytes();
                    file_blocks.retain(|k, _| !k.starts_with(&prefix));
                }
                MetaEvent::SetFileBlocksBatch { blocks } => {
                    for (ino, block_index, block) in blocks {
                        file_blocks.insert(
                            Self::encode_file_block_key(*ino, *block_index).to_vec(),
                            Self::encode_postcard(block)?,
                        );
                    }
                }
                MetaEvent::SetSymlinkTarget { ino, target } => {
                    symlinks.insert(*ino, target.clone());
                }
                MetaEvent::SetXattr {
                    ino,
                    name,
                    value,
                    flags: _,
                } => {
                    xattrs.insert(Self::encode_xattr_key(*ino, name), value.clone());
                }
                MetaEvent::RemoveXattr { ino, name } => {
                    xattrs.remove(&Self::encode_xattr_key(*ino, name));
                }
                MetaEvent::AddPendingDeletion {
                    object_id,
                    timestamp,
                } => {
                    pending_deletions.insert(*object_id, *timestamp);
                }
                MetaEvent::RemovePendingDeletion { object_id } => {
                    pending_deletions.remove(object_id);
                }
            }
        }

        let mut counters: std::collections::HashMap<String, u64> =
            remote.counters.drain(..).collect();
        for (k, v) in &local.counters {
            let e = counters.entry(k.clone()).or_insert(*v);
            *e = (*e).max(*v);
        }

        remote.settings = local.settings.clone();
        remote.counters = counters.into_iter().collect();
        remote.inodes = inodes.into_iter().collect();
        remote.dentries = dentries.into_iter().collect();
        remote.file_blocks = file_blocks.into_iter().collect();
        remote.pending_deletions = pending_deletions.into_iter().collect();
        remote.symlinks = symlinks.into_iter().collect();
        remote.xattrs = xattrs.into_iter().collect();
        remote.event_log = local.event_log.clone();
        Ok(remote)
    }

    fn pull_snapshot_locked(&self) -> MetaResult<bool> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;

        let row = self.run_async(async move {
            let (client, connection) =
                tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| MetaError::Storage(format!("postgres connect failed: {e}")))?;

            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });

            let row = client
                .query_opt(
                    "SELECT version, snapshot FROM metadata_state WHERE vol=$1",
                    &[&volume_id],
                )
                .await
                .map_err(|e| MetaError::Storage(format!("postgres snapshot select failed: {e}")))?;

            conn_task.abort();
            Ok(row)
        })?;

        let Some(row) = row else {
            return Ok(false);
        };

        let version: i64 = row.get(0);
        let bytes: Vec<u8> = row.get(1);

        if version <= self.local_version.load(Ordering::SeqCst) {
            return Ok(false);
        }

        let snapshot = Self::decode_snapshot(&bytes)?;
        self.local.import_snapshot(&snapshot)?;
        self.local_version.store(version, Ordering::SeqCst);
        Ok(true)
    }

    fn push_local_snapshot_with_lock(
        &self,
        events: &[MetaEvent],
    ) -> MetaResult<(i64, RedbSnapshot)> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let client_id = self.client_id.clone();
        let now_ms = Self::now_millis();
        let local_snapshot = self.local.export_snapshot()?;
        let events_vec = events.to_vec();

        self.run_async(async move {
            let (mut client, connection) =
                tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| MetaError::Storage(format!("postgres connect failed: {e}")))?;

            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });

            client
                .execute("SELECT pg_advisory_lock($1)", &[&volume_id])
                .await
                .map_err(|e| MetaError::Storage(format!("postgres advisory lock failed: {e}")))?;

            let tx = client
                .transaction()
                .await
                .map_err(|e| MetaError::Storage(format!("postgres begin tx failed: {e}")))?;

            let current = tx
                .query_opt(
                    "SELECT snapshot FROM metadata_state WHERE vol=$1 FOR UPDATE",
                    &[&volume_id],
                )
                .await
                .map_err(|e| MetaError::Storage(format!("postgres snapshot read failed: {e}")))?;

            let merged_snapshot = if let Some(row) = current {
                let remote_bytes: Vec<u8> = row.get(0);
                let remote = Self::decode_snapshot(&remote_bytes)?;
                Self::merge_snapshot_with_events(remote, &local_snapshot, &events_vec)?
            } else {
                local_snapshot.clone()
            };
            let payload = Self::encode_snapshot(&merged_snapshot)?;

            let row = tx
                .query_one(
                    r#"
                    INSERT INTO metadata_state (vol, version, snapshot)
                    VALUES ($1, 1, $2)
                    ON CONFLICT (vol) DO UPDATE
                    SET version = metadata_state.version + 1,
                        snapshot = EXCLUDED.snapshot,
                        updated_at = now()
                    RETURNING version
                    "#,
                    &[&volume_id, &payload],
                )
                .await
                .map_err(|e| MetaError::Storage(format!("postgres snapshot upsert failed: {e}")))?;
            let version: i64 = row.get(0);

            let _ = tx
                .execute(
                    "INSERT INTO event_log (vol, event_type, event_payload, client_id, client_timestamp) VALUES ($1, 'snapshot', '{}'::jsonb, $2, $3)",
                    &[&volume_id, &client_id, &now_ms],
                )
                .await;
            let channel = format!("studiofs_vol_{volume_id}");
            let _ = tx
                .execute("SELECT pg_notify($1, $2)", &[&channel, &version.to_string()])
                .await;

            tx.commit()
                .await
                .map_err(|e| MetaError::Storage(format!("postgres commit failed: {e}")))?;
            conn_task.abort();
            Ok((version, merged_snapshot))
        })
    }

    fn reset_pending_locked(&self) {
        self.pending_ops.store(0, Ordering::SeqCst);
        if let Ok(mut started) = self.pending_started_at.lock() {
            *started = None;
        }
        self.force_flush.store(false, Ordering::SeqCst);
        if let Ok(mut seq_start) = self.pending_event_start.lock() {
            *seq_start = None;
        }
        self.pending_event_end.store(0, Ordering::SeqCst);
    }

    fn mark_pending_locked(&self) {
        if self.pending_ops.fetch_add(1, Ordering::SeqCst) == 0
            && let Ok(mut started) = self.pending_started_at.lock()
        {
            *started = Some(Instant::now());
        }
    }

    fn should_flush_due_interval_locked(&self) -> bool {
        let pending = self.pending_ops.load(Ordering::SeqCst);
        if pending == 0 {
            return false;
        }
        if let Ok(started) = self.pending_started_at.lock()
            && let Some(t0) = *started
        {
            return t0.elapsed() >= self.flush_interval;
        }
        true
    }

    fn flush_pending_locked(&self) -> MetaResult<()> {
        if self.pending_ops.load(Ordering::SeqCst) == 0 {
            return Ok(());
        }
        let start = self.pending_event_start.lock().ok().and_then(|g| *g);
        let end = self.pending_event_end.load(Ordering::SeqCst);
        let events = if let Some(s) = start {
            self.local.read_events_range(s, end)?
        } else {
            Vec::new()
        };
        if self.pending_ops.load(Ordering::SeqCst) > 0 && events.is_empty() {
            return Err(MetaError::Storage(
                "pending_ops>0 but no pending events captured".to_string(),
            ));
        }
        let (version, merged_snapshot) = self.push_local_snapshot_with_lock(&events)?;
        self.local.import_snapshot(&merged_snapshot)?;
        self.local_version.store(version, Ordering::SeqCst);
        self.reset_pending_locked();
        Ok(())
    }

    fn flush_due_pending(&self) -> MetaResult<()> {
        let _guard = self
            .op_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg op lock poisoned".to_string()))?;
        if self.force_flush.load(Ordering::SeqCst) || self.should_flush_due_interval_locked() {
            self.flush_pending_locked()?;
        }
        Ok(())
    }

    fn mutate<T, F>(&self, f: F) -> MetaResult<T>
    where
        F: FnOnce(&RedbMetaEngine) -> MetaResult<T>,
    {
        let _guard = self
            .op_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg op lock poisoned".to_string()))?;
        let before_seq = self.local.get_counter("nextEventSeq")?;
        let result = f(&self.local)?;
        let after_seq = self.local.get_counter("nextEventSeq")?;
        if after_seq > before_seq {
            if let Ok(mut seq_start) = self.pending_event_start.lock()
                && seq_start.is_none()
            {
                *seq_start = Some(before_seq);
            }
            self.pending_event_end.store(after_seq, Ordering::SeqCst);
        }
        self.mark_pending_locked();

        if self.pending_ops.load(Ordering::SeqCst) >= self.flush_ops_threshold {
            self.force_flush.store(true, Ordering::SeqCst);
        }
        Ok(result)
    }
}

impl MetaEngine for PgMetaEngine {
    fn flush_metadata(&self) -> MetaResult<()> {
        self.force_flush.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn sync_metadata(&self) -> MetaResult<()> {
        if self.pending_ops.load(Ordering::SeqCst) > 0 {
            return Ok(());
        }
        let _guard = self
            .op_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg op lock poisoned".to_string()))?;
        // Never overwrite local unflushed mutations with remote state.
        if self.pending_ops.load(Ordering::SeqCst) > 0 {
            return Ok(());
        }
        // Pull-only sync. Push is handled by background worker.
        let _ = self.pull_snapshot_locked()?;
        Ok(())
    }

    fn invalidation_sender_id(&self) -> Option<String> {
        Some(self.client_id.clone())
    }

    fn metadata_version_token(&self) -> Option<String> {
        Some(self.local_version.load(Ordering::SeqCst).to_string())
    }

    fn set_format_info(&self, info: &FormatInfo) -> MetaResult<()> {
        self.local.set_format_info(info)
    }

    fn get_format_info(&self) -> MetaResult<FormatInfo> {
        self.local.get_format_info()
    }

    fn get_counter(&self, name: &str) -> MetaResult<u64> {
        self.local.get_counter(name)
    }

    fn incr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        self.mutate(|local| local.incr_counter(name, delta))
    }

    fn decr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        self.mutate(|local| local.decr_counter(name, delta))
    }

    fn set_counter(&self, name: &str, value: u64) -> MetaResult<()> {
        self.mutate(|local| local.set_counter(name, value))
    }

    fn next_inode(&self) -> MetaResult<Ino> {
        self.mutate(|local| local.next_inode())
    }

    fn get_attr(&self, ino: Ino) -> MetaResult<InodeAttr> {
        self.local.get_attr(ino)
    }

    fn set_attr(&self, attr: &InodeAttr) -> MetaResult<()> {
        self.mutate(|local| local.set_attr(attr))
    }

    fn remove_attr(&self, ino: Ino) -> MetaResult<()> {
        self.mutate(|local| local.remove_attr(ino))
    }

    fn lookup(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue> {
        self.local.lookup(parent, name)
    }

    fn readdir(&self, parent: Ino) -> MetaResult<Vec<DirEntry>> {
        self.local.readdir(parent)
    }

    fn add_entry(&self, parent: Ino, name: &[u8], entry: &DirEntryValue) -> MetaResult<()> {
        self.mutate(|local| local.add_entry(parent, name, entry))
    }

    fn remove_entry(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue> {
        self.mutate(|local| local.remove_entry(parent, name))
    }

    fn get_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<BlockRef>> {
        self.local.get_file_block(ino, block_index)
    }

    fn set_file_block(
        &self,
        ino: Ino,
        block_index: u32,
        block: &BlockRef,
    ) -> MetaResult<Option<BlockRef>> {
        self.mutate(|local| local.set_file_block(ino, block_index, block))
    }

    fn get_file_blocks(&self, ino: Ino) -> MetaResult<Vec<(u32, BlockRef)>> {
        self.local.get_file_blocks(ino)
    }

    fn remove_file_blocks(&self, ino: Ino) -> MetaResult<Vec<u64>> {
        self.mutate(|local| local.remove_file_blocks(ino))
    }

    fn remove_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<u64>> {
        self.mutate(|local| local.remove_file_block(ino, block_index))
    }

    fn next_block_id(&self) -> MetaResult<u64> {
        self.mutate(|local| local.next_block_id())
    }

    fn next_block_ids(&self, count: u32) -> MetaResult<Vec<u64>> {
        self.mutate(|local| local.next_block_ids(count))
    }

    fn flush_blocks_batch(&self, entries: &[BatchFlushEntry]) -> MetaResult<Vec<BatchFlushResult>> {
        self.mutate(|local| local.flush_blocks_batch(entries))
    }

    fn add_pending_deletion(&self, object_id: u64, timestamp: i64) -> MetaResult<()> {
        self.mutate(|local| local.add_pending_deletion(object_id, timestamp))
    }

    fn remove_pending_deletion(&self, object_id: u64) -> MetaResult<()> {
        self.mutate(|local| local.remove_pending_deletion(object_id))
    }

    fn get_pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>> {
        self.local.get_pending_deletions()
    }

    fn get_referenced_object_ids(&self) -> MetaResult<HashSet<u64>> {
        self.local.get_referenced_object_ids()
    }

    fn create_file(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.create_file(parent, name, uid, gid, mode))
    }

    fn create_dir(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.create_dir(parent, name, uid, gid, mode))
    }

    fn unlink(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.unlink(parent, name))
    }

    fn rmdir(&self, parent: Ino, name: &[u8]) -> MetaResult<()> {
        self.mutate(|local| local.rmdir(parent, name))
    }

    fn rename_atomic(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> MetaResult<Option<Ino>> {
        self.mutate(|local| local.rename_atomic(parent, name, new_parent, new_name))
    }

    fn rename(&self, parent: Ino, name: &[u8], new_parent: Ino, new_name: &[u8]) -> MetaResult<()> {
        self.mutate(|local| local.rename(parent, name, new_parent, new_name))
    }

    fn link_atomic(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.link_atomic(ino, new_parent, new_name))
    }

    fn link(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.link(ino, new_parent, new_name))
    }

    fn set_symlink_target(&self, ino: Ino, target: &[u8]) -> MetaResult<()> {
        self.mutate(|local| local.set_symlink_target(ino, target))
    }

    fn get_symlink_target(&self, ino: Ino) -> MetaResult<Vec<u8>> {
        self.local.get_symlink_target(ino)
    }

    fn get_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>> {
        self.local.get_xattr(ino, name)
    }

    fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> MetaResult<()> {
        self.mutate(|local| local.set_xattr(ino, name, value, flags))
    }

    fn remove_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<()> {
        self.mutate(|local| local.remove_xattr(ino, name))
    }

    fn list_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>> {
        self.local.list_xattrs(ino)
    }

    fn create_symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        uid: u32,
        gid: u32,
    ) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.create_symlink(parent, name, target, uid, gid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn pg_err(e: tokio_postgres::Error) -> String {
        if let Some(db) = e.as_db_error() {
            format!("{} (sqlstate={})", db.message(), db.code().code())
        } else {
            e.to_string()
        }
    }

    fn pg_dsn() -> Option<String> {
        let _ = dotenvy::dotenv();
        std::env::var("STUDIOFS_TEST_PG_DSN")
            .ok()
            .filter(|v| !v.trim().is_empty())
    }

    fn run_async<T, F>(fut: F) -> MetaResult<T>
    where
        F: std::future::Future<Output = MetaResult<T>>,
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| MetaError::Storage(format!("failed to build tokio runtime: {e}")))?;
        rt.block_on(fut)
    }

    fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if cond() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        cond()
    }

    fn wait_visible(engine: &PgMetaEngine, parent: Ino, name: &[u8], timeout: Duration) -> bool {
        wait_until(timeout, || {
            let _ = engine.sync_metadata();
            engine.lookup(parent, name).is_ok()
        })
    }

    fn setup_pg_volume(dsn: &str, name: &str, uuid: &str, block_size: u32) -> MetaResult<i64> {
        run_async(async move {
            let (mut client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
                .await
                .map_err(|e| {
                    MetaError::Storage(format!("postgres connect failed: {}", pg_err(e)))
                })?;
            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });

            let tx = client.transaction().await.map_err(|e| {
                MetaError::Storage(format!("postgres begin tx failed: {}", pg_err(e)))
            })?;

            tx.execute("SELECT pg_advisory_xact_lock($1)", &[&902_202_603_i64])
                .await
                .map_err(|e| {
                    MetaError::Storage(format!("postgres advisory lock failed: {}", pg_err(e)))
                })?;

            tx.batch_execute(
                r#"
                CREATE TABLE IF NOT EXISTS volumes (
                    vol BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    uuid TEXT NOT NULL UNIQUE,
                    block_size INTEGER NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS event_log (
                    vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                    seq_id BIGSERIAL PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    event_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    client_id TEXT,
                    client_timestamp BIGINT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS metadata_state (
                    vol BIGINT PRIMARY KEY,
                    version BIGINT NOT NULL,
                    snapshot BYTEA NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                "#,
            )
            .await
            .map_err(|e| {
                MetaError::Storage(format!("postgres schema ensure failed: {}", pg_err(e)))
            })?;

            let row = tx
                .query_one(
                    r#"
                    INSERT INTO volumes (name, uuid, block_size)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (uuid) DO UPDATE
                    SET name = EXCLUDED.name,
                        block_size = EXCLUDED.block_size
                    RETURNING vol
                    "#,
                    &[&name, &uuid, &(block_size as i32)],
                )
                .await
                .map_err(|e| {
                    MetaError::Storage(format!("postgres volume upsert failed: {}", pg_err(e)))
                })?;

            tx.commit().await.map_err(|e| {
                MetaError::Storage(format!("postgres commit failed: {}", pg_err(e)))
            })?;
            conn_task.abort();
            Ok(row.get(0))
        })
    }

    fn format_info(name: &str, uuid: &str, dsn: &str, volume_id: i64) -> FormatInfo {
        FormatInfo {
            name: name.to_string(),
            uuid: uuid.to_string(),
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "studiofs-test".to_string(),
            access_key: "test".to_string(),
            secret_key: "test".to_string(),
            region: "us-east-1".to_string(),
            data_prefix: format!("{name}/objects/"),
            metadata: MetadataConfig {
                backend: MetadataBackend::Postgres,
                remote: None,
                postgres: Some(PostgresConfig {
                    url: dsn.to_string(),
                    volume_id,
                }),
                flush_interval_secs: 1,
                flush_ops_threshold: 1,
                ..MetadataConfig::default()
            },
            version: String::new(),
            block_size: DEFAULT_BLOCK_SIZE,
            chunk_size: 0,
        }
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_write_through_persists_snapshot() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let name = format!("pg-test-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");

        let seed = RedbMetaEngine::open(&db).expect("seed open");
        seed.set_format_info(&format_info(&name, &uuid, &dsn, volume_id))
            .expect("set format");
        seed.init_root(0, 0).expect("init root");
        drop(seed);

        let pg = PgMetaEngine::open(&db).expect("open pg");
        pg.create_file(ROOT_INO, b"a.txt", 0, 0, 0o644)
            .expect("create file");
        pg.flush_metadata().expect("flush metadata");
        let ok = wait_until(Duration::from_secs(10), || {
            run_async(async {
                let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let conn_task = tokio::spawn(async move {
                    let _ = connection.await;
                });
                let row = client
                    .query_opt(
                        "SELECT version FROM metadata_state WHERE vol=$1",
                        &[&volume_id],
                    )
                    .await
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                conn_task.abort();
                Ok::<bool, MetaError>(row.map(|r| r.get::<_, i64>(0) >= 1).unwrap_or(false))
            })
            .unwrap_or(false)
        });
        assert!(ok, "expected metadata_state version >= 1");
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_sync_metadata_between_clients() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let name = format!("pg-sync-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");

        let info = format_info(&name, &uuid, &dsn, volume_id);

        let seed_a = RedbMetaEngine::open(&db_a).expect("seed a");
        seed_a.set_format_info(&info).expect("set format a");
        seed_a.init_root(0, 0).expect("init root a");
        drop(seed_a);

        let seed_b = RedbMetaEngine::open(&db_b).expect("seed b");
        seed_b.set_format_info(&info).expect("set format b");
        seed_b.init_root(0, 0).expect("init root b");
        drop(seed_b);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        a.create_file(ROOT_INO, b"shared.txt", 0, 0, 0o644)
            .expect("create on a");
        a.flush_metadata().expect("flush a");
        assert!(
            wait_visible(&b, ROOT_INO, b"shared.txt", Duration::from_secs(10)),
            "shared.txt should become visible on b"
        );
        let entry = b.lookup(ROOT_INO, b"shared.txt").expect("lookup on b");
        assert!(entry.ino > 0);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_two_clients_different_dirs_no_loss() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let name = format!("pg-dirs-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
        let info = format_info(&name, &uuid, &dsn, volume_id);

        let seed_a = RedbMetaEngine::open(&db_a).expect("seed a");
        seed_a.set_format_info(&info).expect("set format a");
        seed_a.init_root(0, 0).expect("init root a");
        drop(seed_a);

        let seed_b = RedbMetaEngine::open(&db_b).expect("seed b");
        seed_b.set_format_info(&info).expect("set format b");
        seed_b.init_root(0, 0).expect("init root b");
        drop(seed_b);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let dir_a_attr = a
            .create_dir(ROOT_INO, b"client_a", 0, 0, 0o755)
            .expect("a create dir");
        let dir_b_attr = b
            .create_dir(ROOT_INO, b"client_b", 0, 0, 0o755)
            .expect("b create dir");

        for i in 0..20 {
            let name = format!("a_{i:02}.txt");
            a.create_file(dir_a_attr.ino, name.as_bytes(), 0, 0, 0o644)
                .expect("a create file");
        }
        for i in 0..20 {
            let name = format!("b_{i:02}.txt");
            b.create_file(dir_b_attr.ino, name.as_bytes(), 0, 0, 0o644)
                .expect("b create file");
        }

        a.flush_metadata().expect("flush a");
        b.flush_metadata().expect("flush b");
        let converged = wait_until(Duration::from_secs(20), || {
            let _ = a.flush_metadata();
            let _ = b.flush_metadata();
            let _ = a.sync_metadata();
            let _ = b.sync_metadata();
            a.lookup(ROOT_INO, b"client_a").is_ok()
                && a.lookup(ROOT_INO, b"client_b").is_ok()
                && b.lookup(ROOT_INO, b"client_a").is_ok()
                && b.lookup(ROOT_INO, b"client_b").is_ok()
        });
        assert!(converged);

        let a_client_a = a.lookup(ROOT_INO, b"client_a").expect("a lookup client_a");
        let a_client_b = a.lookup(ROOT_INO, b"client_b").expect("a lookup client_b");
        let b_client_a = b.lookup(ROOT_INO, b"client_a").expect("b lookup client_a");
        let b_client_b = b.lookup(ROOT_INO, b"client_b").expect("b lookup client_b");

        let a_files_in_a = a.readdir(a_client_a.ino).expect("a readdir client_a");
        let a_files_in_b = a.readdir(a_client_b.ino).expect("a readdir client_b");
        let b_files_in_a = b.readdir(b_client_a.ino).expect("b readdir client_a");
        let b_files_in_b = b.readdir(b_client_b.ino).expect("b readdir client_b");

        assert_eq!(
            a_files_in_a.len(),
            20,
            "client A should see all files from A"
        );
        assert_eq!(
            a_files_in_b.len(),
            20,
            "client A should see all files from B"
        );
        assert_eq!(
            b_files_in_a.len(),
            20,
            "client B should see all files from A"
        );
        assert_eq!(
            b_files_in_b.len(),
            20,
            "client B should see all files from B"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_same_dir_different_files_no_loss() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let name = format!("pg-samedir-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
        let info = format_info(&name, &uuid, &dsn, volume_id);

        let seed_a = RedbMetaEngine::open(&db_a).expect("seed a");
        seed_a.set_format_info(&info).expect("set format a");
        seed_a.init_root(0, 0).expect("init root a");
        drop(seed_a);

        let seed_b = RedbMetaEngine::open(&db_b).expect("seed b");
        seed_b.set_format_info(&info).expect("set format b");
        seed_b.init_root(0, 0).expect("init root b");
        drop(seed_b);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let shared = a
            .create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
            .expect("create shared dir");
        a.flush_metadata().expect("flush shared");
        assert!(
            wait_visible(&b, ROOT_INO, b"shared", Duration::from_secs(10)),
            "shared dir should become visible on b"
        );

        for i in 0..20 {
            let name = format!("a_{i:02}.dat");
            a.create_file(shared.ino, name.as_bytes(), 0, 0, 0o644)
                .expect("a create file");
        }
        for i in 0..20 {
            let name = format!("b_{i:02}.dat");
            b.create_file(shared.ino, name.as_bytes(), 0, 0, 0o644)
                .expect("b create file");
        }

        a.flush_metadata().expect("flush a");
        b.flush_metadata().expect("flush b");
        let converged = wait_until(Duration::from_secs(20), || {
            let _ = a.flush_metadata();
            let _ = b.flush_metadata();
            let _ = a.sync_metadata();
            let _ = b.sync_metadata();
            let a_ok = a
                .readdir(shared.ino)
                .map(|v| v.len() >= 40)
                .unwrap_or(false);
            let b_ok = b
                .readdir(shared.ino)
                .map(|v| v.len() >= 40)
                .unwrap_or(false);
            a_ok && b_ok
        });
        assert!(converged);

        let a_entries = a.readdir(shared.ino).expect("a readdir");
        let b_entries = b.readdir(shared.ino).expect("b readdir");
        assert_eq!(a_entries.len(), 40, "A should see all files in shared dir");
        assert_eq!(b_entries.len(), 40, "B should see all files in shared dir");
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_concurrent_create_same_name_single_winner() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let name = format!("pg-concurrent-create-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
        let info = format_info(&name, &uuid, &dsn, volume_id);

        let seed_a = RedbMetaEngine::open(&db_a).expect("seed a");
        seed_a.set_format_info(&info).expect("set format a");
        seed_a.init_root(0, 0).expect("init root a");
        drop(seed_a);

        let seed_b = RedbMetaEngine::open(&db_b).expect("seed b");
        seed_b.set_format_info(&info).expect("set format b");
        seed_b.init_root(0, 0).expect("init root b");
        drop(seed_b);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let a2 = Arc::clone(&a);
        let b2 = Arc::clone(&b);
        let barrier_a = Arc::clone(&barrier);
        let barrier_b = Arc::clone(&barrier);

        let t1 = std::thread::spawn(move || {
            barrier_a.wait();
            let out = a2.create_file(ROOT_INO, b"race.txt", 0, 0, 0o644);
            if out.is_ok() {
                let _ = a2.flush_metadata();
            }
            out
        });
        let t2 = std::thread::spawn(move || {
            barrier_b.wait();
            let out = b2.create_file(ROOT_INO, b"race.txt", 0, 0, 0o644);
            if out.is_ok() {
                let _ = b2.flush_metadata();
            }
            out
        });

        let r1 = t1.join().expect("thread a");
        let r2 = t2.join().expect("thread b");

        let ok_count = [r1.is_ok(), r2.is_ok()].into_iter().filter(|v| *v).count();
        assert!(
            ok_count >= 1,
            "at least one client should create race.txt in async sync model"
        );

        a.flush_metadata().expect("flush a");
        b.flush_metadata().expect("flush b");
        assert!(wait_visible(
            &a,
            ROOT_INO,
            b"race.txt",
            Duration::from_secs(12)
        ));
        let entry = a.lookup(ROOT_INO, b"race.txt").expect("lookup race");
        assert!(entry.ino > 0);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_concurrent_rename_single_winner() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let name = format!("pg-concurrent-rename-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
        let info = format_info(&name, &uuid, &dsn, volume_id);

        let seed_a = RedbMetaEngine::open(&db_a).expect("seed a");
        seed_a.set_format_info(&info).expect("set format a");
        seed_a.init_root(0, 0).expect("init root a");
        drop(seed_a);

        let seed_b = RedbMetaEngine::open(&db_b).expect("seed b");
        seed_b.set_format_info(&info).expect("set format b");
        seed_b.init_root(0, 0).expect("init root b");
        drop(seed_b);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");
        a.create_file(ROOT_INO, b"source.txt", 0, 0, 0o644)
            .expect("seed source");
        a.flush_metadata().expect("flush source");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"source.txt",
            Duration::from_secs(10)
        ));

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let a2 = Arc::clone(&a);
        let b2 = Arc::clone(&b);
        let barrier_a = Arc::clone(&barrier);
        let barrier_b = Arc::clone(&barrier);

        let t1 = std::thread::spawn(move || {
            barrier_a.wait();
            let out = a2.rename(ROOT_INO, b"source.txt", ROOT_INO, b"moved_by_a.txt");
            if out.is_ok() {
                let _ = a2.flush_metadata();
            }
            out
        });
        let t2 = std::thread::spawn(move || {
            barrier_b.wait();
            let out = b2.rename(ROOT_INO, b"source.txt", ROOT_INO, b"moved_by_b.txt");
            if out.is_ok() {
                let _ = b2.flush_metadata();
            }
            out
        });

        let r1 = t1.join().expect("thread a");
        let r2 = t2.join().expect("thread b");
        let ok_count = [r1.is_ok(), r2.is_ok()].into_iter().filter(|v| *v).count();
        assert!(ok_count >= 1, "at least one rename should apply");
        assert!(wait_until(Duration::from_secs(12), || {
            let _ = a.sync_metadata();
            a.lookup(ROOT_INO, b"source.txt").is_err()
        }));
        let a_exists = a.lookup(ROOT_INO, b"moved_by_a.txt").is_ok();
        let b_exists = a.lookup(ROOT_INO, b"moved_by_b.txt").is_ok();
        assert_ne!(a_exists, b_exists, "only one destination name should exist");
        assert!(
            a.lookup(ROOT_INO, b"source.txt").is_err(),
            "source must be gone"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_delete_and_recreate() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let name = format!("pg-delete-recreate-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
        let info = format_info(&name, &uuid, &dsn, volume_id);

        let seed_a = RedbMetaEngine::open(&db_a).expect("seed a");
        seed_a.set_format_info(&info).expect("set format a");
        seed_a.init_root(0, 0).expect("init root a");
        drop(seed_a);

        let seed_b = RedbMetaEngine::open(&db_b).expect("seed b");
        seed_b.set_format_info(&info).expect("set format b");
        seed_b.init_root(0, 0).expect("init root b");
        drop(seed_b);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let first = a
            .create_file(ROOT_INO, b"recreate.txt", 0, 0, 0o644)
            .expect("create first");
        a.unlink(ROOT_INO, b"recreate.txt").expect("unlink first");
        let second = a
            .create_file(ROOT_INO, b"recreate.txt", 0, 0, 0o644)
            .expect("create second");
        assert_ne!(
            first.ino, second.ino,
            "recreated entry should have a new inode in current model"
        );

        a.flush_metadata().expect("flush a");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"recreate.txt",
            Duration::from_secs(10)
        ));
        let on_b = b.lookup(ROOT_INO, b"recreate.txt").expect("lookup on b");
        assert_eq!(on_b.ino, second.ino);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_rapid_create_delete() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let name = format!("pg-rapid-cd-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");

        let seed = RedbMetaEngine::open(&db).expect("seed");
        seed.set_format_info(&format_info(&name, &uuid, &dsn, volume_id))
            .expect("set format");
        seed.init_root(0, 0).expect("init root");
        drop(seed);

        let pg = PgMetaEngine::open(&db).expect("open pg");
        for i in 0..200 {
            let name = format!("rapid_{i:03}.tmp");
            pg.create_file(ROOT_INO, name.as_bytes(), 0, 0, 0o644)
                .expect("create");
            pg.unlink(ROOT_INO, name.as_bytes()).expect("unlink");
        }

        let entries = pg.readdir(ROOT_INO).expect("readdir root");
        assert!(
            entries.is_empty(),
            "rapid create/delete should leave root empty"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_hardlinks_cross_client() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let name = format!("pg-hardlink-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
        let info = format_info(&name, &uuid, &dsn, volume_id);

        let seed_a = RedbMetaEngine::open(&db_a).expect("seed a");
        seed_a.set_format_info(&info).expect("set format a");
        seed_a.init_root(0, 0).expect("init root a");
        drop(seed_a);

        let seed_b = RedbMetaEngine::open(&db_b).expect("seed b");
        seed_b.set_format_info(&info).expect("set format b");
        seed_b.init_root(0, 0).expect("init root b");
        drop(seed_b);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = a
            .create_file(ROOT_INO, b"orig.txt", 0, 0, 0o644)
            .expect("create orig");
        a.link(file.ino, ROOT_INO, b"link.txt").expect("hardlink");

        a.flush_metadata().expect("flush a");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"orig.txt",
            Duration::from_secs(10)
        ));
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"link.txt",
            Duration::from_secs(10)
        ));
        let orig = b.lookup(ROOT_INO, b"orig.txt").expect("lookup orig");
        let link = b.lookup(ROOT_INO, b"link.txt").expect("lookup link");
        assert_eq!(orig.ino, link.ino, "hardlink should point to same inode");
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_symlinks_cross_client() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let name = format!("pg-symlink-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
        let info = format_info(&name, &uuid, &dsn, volume_id);

        let seed_a = RedbMetaEngine::open(&db_a).expect("seed a");
        seed_a.set_format_info(&info).expect("set format a");
        seed_a.init_root(0, 0).expect("init root a");
        drop(seed_a);

        let seed_b = RedbMetaEngine::open(&db_b).expect("seed b");
        seed_b.set_format_info(&info).expect("set format b");
        seed_b.init_root(0, 0).expect("init root b");
        drop(seed_b);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let sym = a
            .create_symlink(ROOT_INO, b"my-link", b"target/path.txt", 0, 0)
            .expect("create symlink");
        a.flush_metadata().expect("flush a");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"my-link",
            Duration::from_secs(10)
        ));
        let link = b.lookup(ROOT_INO, b"my-link").expect("lookup symlink");
        assert_eq!(link.ino, sym.ino);
        let target = b.get_symlink_target(link.ino).expect("readlink");
        assert_eq!(target, b"target/path.txt");
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_basic_crud() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let name = format!("pg-basic-crud-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
        let info = format_info(&name, &uuid, &dsn, volume_id);

        let seed = RedbMetaEngine::open(&db).expect("seed");
        seed.set_format_info(&info).expect("set format");
        seed.init_root(0, 0).expect("init root");
        drop(seed);

        let pg = PgMetaEngine::open(&db).expect("open pg");
        let file = pg
            .create_file(ROOT_INO, b"hello.txt", 0, 0, 0o644)
            .expect("create");
        let looked = pg.lookup(ROOT_INO, b"hello.txt").expect("lookup");
        assert_eq!(file.ino, looked.ino);

        pg.rename(ROOT_INO, b"hello.txt", ROOT_INO, b"renamed.txt")
            .expect("rename");
        assert!(pg.lookup(ROOT_INO, b"hello.txt").is_err());
        assert!(pg.lookup(ROOT_INO, b"renamed.txt").is_ok());

        pg.unlink(ROOT_INO, b"renamed.txt").expect("unlink");
        assert!(pg.lookup(ROOT_INO, b"renamed.txt").is_err());

        pg.create_dir(ROOT_INO, b"subdir", 0, 0, 0o755)
            .expect("mkdir");
        pg.rmdir(ROOT_INO, b"subdir").expect("rmdir");
        assert!(pg.lookup(ROOT_INO, b"subdir").is_err());
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_deep_path() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let name = format!("pg-deep-path-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
        let info = format_info(&name, &uuid, &dsn, volume_id);

        let seed = RedbMetaEngine::open(&db).expect("seed");
        seed.set_format_info(&info).expect("set format");
        seed.init_root(0, 0).expect("init root");
        drop(seed);

        let pg = PgMetaEngine::open(&db).expect("open pg");
        let mut parent = ROOT_INO;
        for n in [b"a", b"b", b"c", b"d", b"e"] {
            let dir = pg.create_dir(parent, n, 0, 0, 0o755).expect("mkdir");
            parent = dir.ino;
        }
        let leaf = pg
            .create_file(parent, b"leaf.txt", 0, 0, 0o644)
            .expect("create leaf");
        let looked = pg.lookup(parent, b"leaf.txt").expect("lookup leaf");
        assert_eq!(leaf.ino, looked.ino);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_large_directory() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let name = format!("pg-large-dir-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
        let info = format_info(&name, &uuid, &dsn, volume_id);

        let seed = RedbMetaEngine::open(&db).expect("seed");
        seed.set_format_info(&info).expect("set format");
        seed.init_root(0, 0).expect("init root");
        drop(seed);

        let pg = PgMetaEngine::open(&db).expect("open pg");
        for i in 0..1000 {
            let name = format!("file_{i:04}.dat");
            pg.create_file(ROOT_INO, name.as_bytes(), 0, 0, 0o644)
                .expect("create");
        }
        let entries = pg.readdir(ROOT_INO).expect("readdir");
        assert_eq!(entries.len(), 1000);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_special_filenames() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let name = format!("pg-special-names-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
        let info = format_info(&name, &uuid, &dsn, volume_id);

        let seed = RedbMetaEngine::open(&db).expect("seed");
        seed.set_format_info(&info).expect("set format");
        seed.init_root(0, 0).expect("init root");
        drop(seed);

        let pg = PgMetaEngine::open(&db).expect("open pg");
        let names = vec![
            b"file with spaces.txt".to_vec(),
            "файл_тест_日本語.txt".as_bytes().to_vec(),
            format!("{}.txt", "a".repeat(200)).as_bytes().to_vec(),
            b"file...multiple...dots.txt".to_vec(),
            b"-dashstart.txt".to_vec(),
        ];
        for n in &names {
            pg.create_file(ROOT_INO, n, 0, 0, 0o644).expect("create");
        }
        for n in &names {
            assert!(
                pg.lookup(ROOT_INO, n).is_ok(),
                "name should resolve: {:?}",
                n
            );
        }
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_readdir_during_modifications() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let name = format!("pg-readdir-race-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
        let info = format_info(&name, &uuid, &dsn, volume_id);

        let seed_a = RedbMetaEngine::open(&db_a).expect("seed a");
        seed_a.set_format_info(&info).expect("set format a");
        seed_a.init_root(0, 0).expect("init root a");
        drop(seed_a);

        let seed_b = RedbMetaEngine::open(&db_b).expect("seed b");
        seed_b.set_format_info(&info).expect("set format b");
        seed_b.init_root(0, 0).expect("init root b");
        drop(seed_b);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let writer = std::thread::spawn({
            let a = Arc::clone(&a);
            move || {
                for i in 0..120 {
                    let name = format!("w_{i:03}.dat");
                    let _ = a.create_file(ROOT_INO, name.as_bytes(), 0, 0, 0o644);
                    if i % 3 == 0 {
                        let _ = a.unlink(ROOT_INO, name.as_bytes());
                    }
                }
            }
        });

        for _ in 0..80 {
            let _ = b.sync_metadata();
            let _ = b.readdir(ROOT_INO).expect("readdir should not fail");
            std::thread::sleep(Duration::from_millis(10));
        }
        writer.join().expect("writer join");
        b.sync_metadata().expect("final sync");
        let _ = b.readdir(ROOT_INO).expect("final readdir");
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_visibility_timing() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let name = format!("pg-vis-timing-{}", std::process::id());
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(&dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
        let info = format_info(&name, &uuid, &dsn, volume_id);

        let seed_a = RedbMetaEngine::open(&db_a).expect("seed a");
        seed_a.set_format_info(&info).expect("set format a");
        seed_a.init_root(0, 0).expect("init root a");
        drop(seed_a);

        let seed_b = RedbMetaEngine::open(&db_b).expect("seed b");
        seed_b.set_format_info(&info).expect("set format b");
        seed_b.init_root(0, 0).expect("init root b");
        drop(seed_b);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let mut delays = Vec::new();
        for i in 0..5 {
            let name = format!("vis_{i:02}.dat");
            a.create_file(ROOT_INO, name.as_bytes(), 0, 0, 0o644)
                .expect("create");
            let start = std::time::Instant::now();
            let mut seen = false;
            for _ in 0..50 {
                b.sync_metadata().expect("sync b");
                if b.lookup(ROOT_INO, name.as_bytes()).is_ok() {
                    seen = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            assert!(seen, "file should become visible on peer");
            delays.push(start.elapsed().as_millis() as u64);
        }

        let avg = delays.iter().sum::<u64>() / delays.len() as u64;
        assert!(avg < 5000, "avg visibility delay should stay bounded");
    }
}
