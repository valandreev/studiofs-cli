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
/// - local `redb` remains the foreground serving store
/// - PostgreSQL `event_log` is the runtime delta stream
/// - PostgreSQL `metadata_state` stores bootstrap/checkpoint snapshots
/// - foreign batches are pulled in `batch_version` order
pub struct PgMetaEngine {
    local: Arc<RedbMetaEngine>,
    postgres_url: String,
    volume_id: i64,
    client_id: String,
    local_version: AtomicI64,
    op_lock: Mutex<()>,
    poll_interval: Duration,
    flush_interval: Duration,
    flush_idle_grace: Duration,
    flush_ops_threshold: u32,
    pending_ops: std::sync::atomic::AtomicU32,
    pending_started_at: Mutex<Option<Instant>>,
    last_mutation_at: Mutex<Option<Instant>>,
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
        let flush_idle_grace = Duration::from_millis(
            std::env::var("STUDIOFS_PG_FLUSH_IDLE_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(2_000),
        );
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
            flush_idle_grace,
            flush_ops_threshold,
            pending_ops: std::sync::atomic::AtomicU32::new(0),
            pending_started_at: Mutex::new(None),
            last_mutation_at: Mutex::new(None),
            force_flush: std::sync::atomic::AtomicBool::new(false),
            pending_event_start: Mutex::new(None),
            pending_event_end: AtomicU64::new(0),
        });

        // Bootstrap from the last checkpoint if present, then catch up from
        // append-only event deltas.
        let _ = engine.pull_snapshot_locked();
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

    fn decode_snapshot(bytes: &[u8]) -> MetaResult<RedbSnapshot> {
        serde_json::from_slice(bytes)
            .map_err(|e| MetaError::Serialization(format!("snapshot decode failed: {e}")))
    }

    fn encode_postcard<T: serde::Serialize>(val: &T) -> MetaResult<Vec<u8>> {
        postcard::to_stdvec(val)
            .map_err(|e| MetaError::Serialization(format!("postcard encode failed: {e}")))
    }

    fn project_remote_events(events: &[MetaEvent]) -> Vec<MetaEvent> {
        let mut out = Vec::with_capacity(events.len());
        for event in events {
            match event {
                MetaEvent::SetFileBlock { .. } | MetaEvent::SetFileBlocksBatch { .. } => {}
                MetaEvent::AddPendingDeletion { .. } | MetaEvent::RemovePendingDeletion { .. } => {}
                MetaEvent::SetAttr { attr } if attr.kind == FileType::RegularFile => {
                    let mut remote_attr = attr.clone();
                    remote_attr.size = remote_attr.committed_size;
                    out.push(MetaEvent::SetAttr { attr: remote_attr });
                }
                _ => out.push(event.clone()),
            }
        }
        out
    }

    fn event_conflicts_with_overlay(
        &self,
        event: &MetaEvent,
        overlay_inodes: &HashSet<Ino>,
    ) -> MetaResult<bool> {
        let conflict = match event {
            MetaEvent::SetAttr { attr } => overlay_inodes.contains(&attr.ino),
            MetaEvent::RemoveAttr { ino }
            | MetaEvent::SetFileBlock { ino, .. }
            | MetaEvent::RemoveFileBlock { ino, .. }
            | MetaEvent::RemoveFileBlocks { ino }
            | MetaEvent::CommitFileBlock { ino, .. }
            | MetaEvent::SetSymlinkTarget { ino, .. }
            | MetaEvent::SetXattr { ino, .. }
            | MetaEvent::RemoveXattr { ino, .. } => overlay_inodes.contains(ino),
            MetaEvent::SetFileBlocksBatch { blocks } => blocks
                .iter()
                .any(|(ino, _, _)| overlay_inodes.contains(ino)),
            MetaEvent::AddEntry { entry, .. } | MetaEvent::RenameEntry { entry, .. } => {
                overlay_inodes.contains(&entry.ino)
            }
            MetaEvent::RemoveEntry { parent, name } => match self.local.lookup(*parent, name) {
                Ok(entry) => overlay_inodes.contains(&entry.ino),
                Err(MetaError::EntryNotFound { .. }) => false,
                Err(e) => return Err(e),
            },
            MetaEvent::AddPendingDeletion { .. } | MetaEvent::RemovePendingDeletion { .. } => false,
        };
        Ok(conflict)
    }

    fn batch_conflicts_with_overlay(
        &self,
        events: &[MetaEvent],
        overlay_inodes: &HashSet<Ino>,
    ) -> MetaResult<bool> {
        if overlay_inodes.is_empty() {
            return Ok(false);
        }
        for event in events {
            if self.event_conflicts_with_overlay(event, overlay_inodes)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn pull_snapshot_locked(&self) -> MetaResult<bool> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let local_version = self.local_version.load(Ordering::SeqCst);

        let row = self.run_async(async move {
            let (client, connection) =
                tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| MetaError::Storage(format!("postgres connect failed: {e}")))?;

            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });

            let version_row = client
                .query_opt(
                    "SELECT version FROM metadata_state WHERE vol=$1",
                    &[&volume_id],
                )
                .await
                .map_err(|e| {
                    MetaError::Storage(format!("postgres snapshot version select failed: {e}"))
                })?;

            let Some(version_row) = version_row else {
                conn_task.abort();
                return Ok(None);
            };
            let version: i64 = version_row.get(0);
            if version <= local_version {
                conn_task.abort();
                return Ok(None);
            }

            let row = client
                .query_one(
                    "SELECT version, snapshot FROM metadata_state WHERE vol=$1",
                    &[&volume_id],
                )
                .await
                .map_err(|e| {
                    MetaError::Storage(format!("postgres snapshot payload select failed: {e}"))
                })?;

            conn_task.abort();
            Ok(Some(row))
        })?;

        let Some(row) = row else {
            return Ok(false);
        };

        let version: i64 = row.get(0);
        let bytes: Vec<u8> = row.get(1);

        let snapshot = Self::decode_snapshot(&bytes)?;
        self.local.import_snapshot(&snapshot)?;
        let _ = version;
        Ok(true)
    }

    fn push_event_log_batch_locked(&self, events: &[MetaEvent]) -> MetaResult<i64> {
        let remote_events = Self::project_remote_events(events);
        if remote_events.is_empty() {
            return Ok(self.local_version.load(Ordering::SeqCst));
        }

        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let client_id = self.client_id.clone();
        let now_ms = Self::now_millis();
        let events_vec = remote_events;

        self.run_async(async move {
            let (mut client, connection) =
                tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| MetaError::Storage(format!("postgres connect failed: {e}")))?;

            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });

            let tx = client
                .transaction()
                .await
                .map_err(|e| MetaError::Storage(format!("postgres begin tx failed: {e}")))?;

            let batch_version: i64 = tx
                .query_one(
                    r#"
                    UPDATE metadata_state
                    SET version = version + 1,
                        updated_at = now()
                    WHERE vol = $1
                    RETURNING version
                    "#,
                    &[&volume_id],
                )
                .await
                .map_err(|e| {
                    MetaError::Storage(format!("postgres metadata_state bump failed: {e}"))
                })?
                .get(0);

            let mut last_seq = 0_i64;
            for event in &events_vec {
                let payload = Self::encode_postcard(event)?;
                let row = tx
                    .query_one(
                        r#"
                        INSERT INTO event_log (
                            vol,
                            batch_version,
                            event_type,
                            event_payload,
                            client_id,
                            client_timestamp
                        )
                        VALUES ($1, $2, $3, $4, $5, $6)
                        RETURNING seq_id
                        "#,
                        &[
                            &volume_id,
                            &batch_version,
                            &event.kind(),
                            &payload,
                            &client_id,
                            &now_ms,
                        ],
                    )
                    .await
                    .map_err(|e| {
                        MetaError::Storage(format!("postgres event_log insert failed: {e}"))
                    })?;
                last_seq = row.get(0);
            }

            if last_seq > 0 {
                let channel = format!("studiofs_vol_{volume_id}");
                let _ = tx
                    .execute(
                        "SELECT pg_notify($1, $2)",
                        &[&channel, &batch_version.to_string()],
                    )
                    .await;
            }

            tx.commit()
                .await
                .map_err(|e| MetaError::Storage(format!("postgres commit failed: {e}")))?;
            conn_task.abort();
            Ok(batch_version)
        })
    }

    fn write_checkpoint_locked(&self) -> MetaResult<()> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let snapshot = self.local.export_remote_snapshot()?;
        let payload = serde_json::to_vec(&snapshot)
            .map_err(|e| MetaError::Serialization(format!("snapshot encode failed: {e}")))?;
        let version = self.local_version.load(Ordering::SeqCst);

        self.run_async(async move {
            let (client, connection) =
                tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| MetaError::Storage(format!("postgres connect failed: {e}")))?;
            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });

            client
                .execute(
                    r#"
                    INSERT INTO metadata_state (vol, version, snapshot)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (vol) DO UPDATE
                    SET version = EXCLUDED.version,
                        snapshot = EXCLUDED.snapshot,
                        updated_at = now()
                    "#,
                    &[&volume_id, &version, &payload],
                )
                .await
                .map_err(|e| {
                    MetaError::Storage(format!("postgres checkpoint upsert failed: {e}"))
                })?;

            conn_task.abort();
            Ok(())
        })
    }

    fn pull_foreign_events_locked(&self) -> MetaResult<bool> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let client_id = self.client_id.clone();
        let last_seq = self.local_version.load(Ordering::SeqCst);

        let rows = self.run_async(async move {
            let (client, connection) =
                tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| MetaError::Storage(format!("postgres connect failed: {e}")))?;

            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });

            let rows = client
                .query(
                    r#"
                    SELECT batch_version, seq_id, event_payload, client_id
                    FROM event_log
                    WHERE vol=$1 AND batch_version > $2
                    ORDER BY batch_version ASC, seq_id ASC
                    "#,
                    &[&volume_id, &last_seq],
                )
                .await
                .map_err(|e| {
                    MetaError::Storage(format!("postgres event_log select failed: {e}"))
                })?;
            conn_task.abort();
            Ok::<_, MetaError>(rows)
        })?;

        if rows.is_empty() {
            return Ok(false);
        }

        let overlay_inodes = self.local.local_overlay_inodes()?;
        let mut applied_any = false;
        let mut max_version = last_seq;
        let mut idx = 0;
        while idx < rows.len() {
            let batch_version: i64 = rows[idx].get(0);
            let batch_source: Option<String> = rows[idx].get(3);
            let mut batch_events = Vec::new();

            while idx < rows.len() {
                let row_batch_version: i64 = rows[idx].get(0);
                if row_batch_version != batch_version {
                    break;
                }
                let payload: Vec<u8> = rows[idx].get(2);
                let source_client: Option<String> = rows[idx].get(3);
                if source_client.as_deref() != Some(client_id.as_str()) {
                    let event: MetaEvent = postcard::from_bytes(&payload).map_err(|e| {
                        MetaError::Serialization(format!("postgres event decode failed: {e}"))
                    })?;
                    batch_events.push(event);
                }
                idx += 1;
            }

            if batch_source.as_deref() == Some(client_id.as_str()) {
                max_version = batch_version;
                continue;
            }

            if self.batch_conflicts_with_overlay(&batch_events, &overlay_inodes)? {
                break;
            }

            if !batch_events.is_empty() {
                self.local.apply_replica_events(&batch_events)?;
                applied_any = true;
            }
            max_version = batch_version;
        }
        self.local_version.store(max_version, Ordering::SeqCst);
        Ok(applied_any)
    }

    fn reset_pending_locked(&self) {
        self.pending_ops.store(0, Ordering::SeqCst);
        if let Ok(mut started) = self.pending_started_at.lock() {
            *started = None;
        }
        if let Ok(mut last) = self.last_mutation_at.lock() {
            *last = None;
        }
        self.force_flush.store(false, Ordering::SeqCst);
        if let Ok(mut seq_start) = self.pending_event_start.lock() {
            *seq_start = None;
        }
        self.pending_event_end.store(0, Ordering::SeqCst);
    }

    fn mark_pending_locked(&self) {
        let now = Instant::now();
        if self.pending_ops.fetch_add(1, Ordering::SeqCst) == 0
            && let Ok(mut started) = self.pending_started_at.lock()
        {
            *started = Some(now);
        }
        if let Ok(mut last) = self.last_mutation_at.lock() {
            *last = Some(now);
        }
    }

    fn should_flush_due_interval_locked(&self) -> bool {
        let pending = self.pending_ops.load(Ordering::SeqCst);
        if pending == 0 {
            return false;
        }
        if let Ok(last) = self.last_mutation_at.lock()
            && let Some(t0) = *last
        {
            return t0.elapsed() >= self.flush_idle_grace;
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
            self.reset_pending_locked();
            return Ok(());
        }
        let version = self.push_event_log_batch_locked(&events)?;
        let _ = version;
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
        let _guard = self
            .op_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg op lock poisoned".to_string()))?;
        self.force_flush.store(true, Ordering::SeqCst);
        self.flush_pending_locked()?;
        let _ = self.pull_foreign_events_locked()?;
        self.write_checkpoint_locked()?;
        Ok(())
    }

    fn request_eager_flush(&self) {
        self.force_flush.store(true, Ordering::SeqCst);
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
        let _ = self.pull_foreign_events_locked()?;
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

    fn commit_uploaded_block(
        &self,
        ino: Ino,
        block_index: u32,
        expected_epoch: u64,
        block: &BlockRef,
    ) -> MetaResult<bool> {
        let result = self
            .mutate(|local| local.commit_uploaded_block(ino, block_index, expected_epoch, block));
        if matches!(result, Ok(true)) {
            self.force_flush.store(true, Ordering::SeqCst);
        }
        result
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
    use crate::pg_dsn::ensure_studiofs_database;
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
        let raw = std::env::var("STUDIOFS_TEST_PG_DSN")
            .ok()
            .filter(|v| !v.trim().is_empty())?;
        run_async(async { ensure_studiofs_database(&raw).await })
            .ok()
            .map(|v| v.metadata_dsn)
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

    struct PgTestVolume {
        dsn: String,
        name: String,
        uuid: String,
        volume_id: i64,
    }

    impl PgTestVolume {
        fn new(dsn: &str, prefix: &str) -> Self {
            let name = format!(
                "{prefix}-{}-{}",
                std::process::id(),
                Uuid::new_v4().simple()
            );
            let uuid = Uuid::new_v4().to_string();
            let volume_id =
                setup_pg_volume(dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
            Self {
                dsn: dsn.to_string(),
                name,
                uuid,
                volume_id,
            }
        }

        fn format_info(&self) -> FormatInfo {
            format_info(&self.name, &self.uuid, &self.dsn, self.volume_id)
        }
    }

    impl Drop for PgTestVolume {
        fn drop(&mut self) {
            let _ = run_async(async {
                let (client, connection) =
                    tokio_postgres::connect(&self.dsn, tokio_postgres::NoTls)
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!("postgres connect failed: {}", pg_err(e)))
                        })?;
                let conn_task = tokio::spawn(async move {
                    let _ = connection.await;
                });
                client
                    .execute("DELETE FROM volumes WHERE vol = $1", &[&self.volume_id])
                    .await
                    .map_err(|e| {
                        MetaError::Storage(format!("postgres cleanup failed: {}", pg_err(e)))
                    })?;
                conn_task.abort();
                Ok::<(), MetaError>(())
            });
        }
    }

    fn seed_local(path: &std::path::Path, info: &FormatInfo) {
        let seed = RedbMetaEngine::open(path).expect("seed open");
        seed.set_format_info(info).expect("set format");
        seed.init_root(0, 0).expect("init root");
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
                    batch_version BIGINT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_payload BYTEA NOT NULL,
                    client_id TEXT,
                    client_timestamp BIGINT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS metadata_state (
                    vol BIGINT PRIMARY KEY REFERENCES volumes(vol) ON DELETE CASCADE,
                    version BIGINT NOT NULL,
                    snapshot BYTEA NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                ALTER TABLE event_log
                    ADD COLUMN IF NOT EXISTS batch_version BIGINT;
                UPDATE event_log
                    SET batch_version = 0
                    WHERE batch_version IS NULL;
                ALTER TABLE event_log
                    ALTER COLUMN batch_version SET NOT NULL;
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
            let volume_id: i64 = row.get(0);

            let empty_snapshot = serde_json::to_vec(&RedbSnapshot::default())
                .map_err(|e| MetaError::Serialization(format!("snapshot encode failed: {e}")))?;
            tx.execute(
                r#"
                INSERT INTO metadata_state (vol, version, snapshot)
                VALUES ($1, 0, $2)
                ON CONFLICT (vol) DO NOTHING
                "#,
                &[&volume_id, &empty_snapshot],
            )
            .await
            .map_err(|e| {
                MetaError::Storage(format!(
                    "postgres metadata_state seed failed: {}",
                    pg_err(e)
                ))
            })?;

            tx.commit().await.map_err(|e| {
                MetaError::Storage(format!("postgres commit failed: {}", pg_err(e)))
            })?;
            conn_task.abort();
            Ok(volume_id)
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
    fn test_pg_flush_advances_checkpoint_version() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let volume = PgTestVolume::new(&dsn, "pg-test");
        seed_local(&db, &volume.format_info());

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
                        &[&volume.volume_id],
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
        let volume = PgTestVolume::new(&dsn, "pg-sync");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

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
    fn test_pg_remote_client_reads_committed_blocks_without_local_staged_mirror() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-committed-block-read");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = a
            .create_file(ROOT_INO, b"remote.bin", 0, 0, 0o644)
            .expect("create file");
        let mut attr = a.get_attr(file.ino).expect("get attr");
        attr.size = 4096;
        a.set_attr(&attr).expect("set size");
        let block = BlockRef::new_partial(77, 4096);
        a.set_file_block(file.ino, 0, &block)
            .expect("set staged block");
        a.commit_uploaded_block(file.ino, 0, 0, &block)
            .expect("commit uploaded block");
        a.flush_metadata().expect("flush a");

        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"remote.bin",
            Duration::from_secs(10)
        ));
        let remote = b.lookup(ROOT_INO, b"remote.bin").expect("lookup remote");
        let remote_block = b
            .get_file_block(remote.ino, 0)
            .expect("get remote block")
            .expect("remote committed block");
        assert_eq!(remote_block.object_id, 77);

        let remote_blocks = b.get_file_blocks(remote.ino).expect("get remote blocks");
        assert_eq!(remote_blocks.len(), 1);
        assert_eq!(remote_blocks[0].0, 0);
        assert_eq!(remote_blocks[0].1.object_id, 77);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_overlay_allows_unrelated_remote_batches_to_apply() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-overlay-prefix");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let draft = a
            .create_file(ROOT_INO, b"draft.bin", 0, 0, 0o644)
            .expect("create draft");
        let mut draft_attr = a.get_attr(draft.ino).expect("get draft attr");
        draft_attr.size = 4096;
        a.set_attr(&draft_attr).expect("set draft size");
        a.set_file_block(draft.ino, 0, &BlockRef::new_partial(501, 4096))
            .expect("set draft staged block");
        a.flush_metadata().expect("flush draft namespace");

        b.create_file(ROOT_INO, b"foreign.txt", 0, 0, 0o644)
            .expect("create foreign");
        b.flush_metadata().expect("flush foreign");

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = a.sync_metadata();
            a.lookup(ROOT_INO, b"foreign.txt").is_ok()
        }));

        let draft_after = a.get_attr(draft.ino).expect("get local draft");
        assert_eq!(draft_after.size, 4096);
        assert_eq!(draft_after.committed_size, 0);
        let local_block = a
            .get_file_block(draft.ino, 0)
            .expect("get staged draft block")
            .expect("staged draft block exists");
        assert_eq!(local_block.object_id, 501);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_checkpoint_excludes_uncommitted_local_blocks() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-checkpoint-projection");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let file = a
            .create_file(ROOT_INO, b"draft.bin", 0, 0, 0o644)
            .expect("create draft");

        let mut attr = a.get_attr(file.ino).expect("get attr");
        attr.size = 4096;
        attr.mtime = InodeAttr::now_nanos();
        attr.ctime = attr.mtime;
        a.set_attr(&attr).expect("set staged size");
        a.set_file_block(file.ino, 0, &BlockRef::new_partial(42, 4096))
            .expect("set staged block");
        a.flush_metadata().expect("flush a");

        let b = PgMetaEngine::open(&db_b).expect("open b");
        let b_file = b.lookup(ROOT_INO, b"draft.bin").expect("lookup draft");
        let b_attr = b.get_attr(b_file.ino).expect("get b attr");
        assert_eq!(b_attr.size, 0, "remote-visible size must stay committed");
        assert_eq!(b_attr.committed_size, 0);
        assert!(
            b.get_file_block(b_file.ino, 0)
                .expect("get remote block")
                .is_none(),
            "checkpoint must not leak local-only staged file blocks"
        );
        assert!(
            b.get_file_blocks(b_file.ino)
                .expect("get remote blocks")
                .is_empty(),
            "checkpoint must not leak local-only staged file blocks"
        );
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
        let volume = PgTestVolume::new(&dsn, "pg-dirs");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

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
        let volume = PgTestVolume::new(&dsn, "pg-samedir");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

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
        let volume = PgTestVolume::new(&dsn, "pg-concurrent-create");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

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
        let volume = PgTestVolume::new(&dsn, "pg-concurrent-rename");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

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
        let volume = PgTestVolume::new(&dsn, "pg-delete-recreate");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

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
        let volume = PgTestVolume::new(&dsn, "pg-rapid-cd");
        seed_local(&db, &volume.format_info());

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
        let volume = PgTestVolume::new(&dsn, "pg-hardlink");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

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
        let volume = PgTestVolume::new(&dsn, "pg-symlink");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

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
        let volume = PgTestVolume::new(&dsn, "pg-basic-crud");
        seed_local(&db, &volume.format_info());

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
        let volume = PgTestVolume::new(&dsn, "pg-deep-path");
        seed_local(&db, &volume.format_info());

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
        let volume = PgTestVolume::new(&dsn, "pg-large-dir");
        seed_local(&db, &volume.format_info());

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
        let volume = PgTestVolume::new(&dsn, "pg-special-names");
        seed_local(&db, &volume.format_info());

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
        let volume = PgTestVolume::new(&dsn, "pg-readdir-race");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

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
        let volume = PgTestVolume::new(&dsn, "pg-vis-timing");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

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
