use crate::engine::{MetaEngine, MetaError, MetaResult};
use crate::redb_engine::{RedbMetaEngine, RedbSnapshot};
use crate::types::*;
use async_nats::Client as NatsClient;
use opendal::options::WriteOptions;
use opendal::services::S3;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::runtime::{Builder, Runtime};
use tracing::{debug, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TonboManifest {
    updated_at_unix_secs: i64,
    snapshot: RedbSnapshot,
}

#[derive(Debug)]
struct FlushState {
    pending_ops: u32,
    pending_bytes: u64,
    last_flush: Instant,
    last_refresh: Instant,
    etag: Option<String>,
}

struct RemoteStore {
    op: opendal::Operator,
    manifest_path: String,
    legacy_manifest_paths: Vec<String>,
    inode_counter_path: String,
    block_counter_path: String,
    default_next_inode: u64,
    default_next_block_id: u64,
}

#[derive(Debug, Clone, Copy)]
struct IdRange {
    next: u64,
    end_exclusive: u64,
}

impl IdRange {
    fn new(start: u64, size: u64) -> Self {
        Self {
            next: start,
            end_exclusive: start.saturating_add(size),
        }
    }

    fn remaining(&self) -> u64 {
        self.end_exclusive.saturating_sub(self.next)
    }

    fn take_one(&mut self) -> Option<u64> {
        if self.next >= self.end_exclusive {
            return None;
        }
        let id = self.next;
        self.next = self.next.saturating_add(1);
        Some(id)
    }
}

#[derive(Debug, Default)]
struct RangeState {
    active: Option<IdRange>,
    standby: Option<IdRange>,
}

/// Tonbo-style metadata engine:
/// - local redb is the read/write cache
/// - remote manifest in object storage is the shared metadata source
/// - writes are batched and pushed with optimistic etag checks
pub struct TonboMetaEngine {
    local: Arc<RedbMetaEngine>,
    remote: Option<RemoteStore>,
    volume_name: String,
    invalidation_sender_id: String,
    nats_subject_base: Option<String>,
    nats_client: Option<NatsClient>,
    state: Mutex<FlushState>,
    flush_interval: Duration,
    cache_ttl: Duration,
    flush_ops_threshold: u32,
    flush_bytes_threshold: u64,
    remote_request_timeout: Duration,
    cas_max_retries: u32,
    sync_lock: Mutex<()>,
    rt: Mutex<Option<Runtime>>,
    // Metrics
    remote_round_trips: AtomicU64,
    cas_retries: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    ttl_expirations: AtomicU64,
    flush_cycles: AtomicU64,
    flushed_ops_total: AtomicU64,
    flushed_bytes_total: AtomicU64,
    baseline_snapshot: Mutex<RedbSnapshot>,
    inode_ranges: Mutex<RangeState>,
    block_ranges: Mutex<RangeState>,
    inode_alloc_lock: Mutex<()>,
    block_alloc_lock: Mutex<()>,
}

const INODE_RANGE_SIZE: u64 = 4_096;
const BLOCK_RANGE_SIZE: u64 = 65_536;
const RANGE_CAS_MAX_RETRIES: u32 = 10;
const REMOTE_IO_MAX_ATTEMPTS: usize = 3;
const REMOTE_IO_RETRY_BASE_MS: u64 = 80;
static TONBO_SENDER_SEQ: AtomicU64 = AtomicU64::new(1);

impl TonboMetaEngine {
    fn new_invalidation_sender_id() -> String {
        let seq = TONBO_SENDER_SEQ.fetch_add(1, Ordering::Relaxed);
        format!("pid:{}:{}", std::process::id(), seq)
    }

    pub fn open(path: impl AsRef<Path>) -> MetaResult<Arc<Self>> {
        let local = Arc::new(RedbMetaEngine::open(path)?);
        let info = local.get_format_info().ok();
        let volume_name = info
            .as_ref()
            .map(|v| v.name.clone())
            .unwrap_or_else(|| "unknown".to_string());
        let mut remote = None;
        let mut cache_ttl_secs = 20;
        let mut flush_interval_secs = 60;
        let mut flush_ops_threshold = 100;
        let mut flush_bytes_threshold = 1024 * 1024;

        if let Some(info) = info {
            cache_ttl_secs = info.metadata.cache_ttl_secs;
            flush_interval_secs = info.metadata.flush_interval_secs;
            flush_ops_threshold = info.metadata.flush_ops_threshold;
            flush_bytes_threshold = info.metadata.flush_bytes_threshold;
            if info.metadata.backend == MetadataBackend::Tonbo {
                if let Some(cfg) = &info.metadata.remote {
                    let mut builder = S3::default()
                        .endpoint(&cfg.endpoint)
                        .bucket(&cfg.bucket)
                        .access_key_id(&cfg.access_key)
                        .secret_access_key(&cfg.secret_key)
                        .disable_config_load()
                        .disable_ec2_metadata();
                    if !cfg.region.is_empty() {
                        builder = builder.region(&cfg.region);
                    } else {
                        builder = builder.region("us-east-1");
                    }
                    let op = opendal::Operator::new(builder)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .finish();
                    let raw_prefix = if cfg.prefix.trim().is_empty() {
                        format!("{}/metadata/", info.name)
                    } else {
                        cfg.prefix.clone()
                    };
                    let prefix = if raw_prefix.ends_with('/') {
                        raw_prefix
                    } else {
                        format!("{}/", raw_prefix)
                    };
                    let mut legacy_manifest_paths =
                        vec![format!("{}{}.manifest.json", prefix, info.name)];
                    if !info.uuid.is_empty() && info.uuid != info.name {
                        legacy_manifest_paths
                            .push(format!("{}{}.manifest.json", prefix, info.uuid));
                    }
                    let default_next_inode = local
                        .get_counter(counters::NEXT_INODE)
                        .unwrap_or(ROOT_INO.saturating_add(1))
                        .max(ROOT_INO.saturating_add(1));
                    let default_next_block_id = local
                        .get_counter(counters::NEXT_BLOCK_ID)
                        .unwrap_or(1)
                        .max(1);
                    remote = Some(RemoteStore {
                        op,
                        manifest_path: format!("{}manifest.json", prefix),
                        legacy_manifest_paths,
                        inode_counter_path: format!("{}counters/next_inode", prefix),
                        block_counter_path: format!("{}counters/next_block_id", prefix),
                        default_next_inode,
                        default_next_block_id,
                    });
                }
            }
        }

        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| MetaError::Storage(format!("failed to create tokio runtime: {e}")))?;
        let nats_url = std::env::var("STUDIOFS_NATS_URL")
            .ok()
            .filter(|s| !s.is_empty());
        let nats_client = if let Some(url) = &nats_url {
            let url = url.clone();
            let connect = async_nats::connect(url.clone());
            let connect_result = if tokio::runtime::Handle::try_current().is_ok() {
                tokio::task::block_in_place(|| rt.block_on(connect))
            } else {
                rt.block_on(connect)
            };
            match connect_result {
                Ok(client) => Some(client),
                Err(err) => {
                    warn!("failed to connect NATS at '{}': {}", url, err);
                    None
                }
            }
        } else {
            None
        };

        let engine = Arc::new(Self {
            local,
            remote,
            volume_name: volume_name.clone(),
            invalidation_sender_id: Self::new_invalidation_sender_id(),
            nats_subject_base: if nats_client.is_some() {
                Some(format!("fs.invalidate.{}", volume_name))
            } else {
                None
            },
            nats_client,
            state: Mutex::new(FlushState {
                pending_ops: 0,
                pending_bytes: 0,
                last_flush: Instant::now(),
                last_refresh: Instant::now(),
                etag: None,
            }),
            flush_interval: Duration::from_secs(flush_interval_secs.max(1)),
            cache_ttl: Duration::from_secs(cache_ttl_secs.max(1)),
            flush_ops_threshold: flush_ops_threshold.max(1),
            flush_bytes_threshold: flush_bytes_threshold.max(1),
            remote_request_timeout: Duration::from_secs(12),
            cas_max_retries: 5,
            sync_lock: Mutex::new(()),
            rt: Mutex::new(Some(rt)),
            remote_round_trips: AtomicU64::new(0),
            cas_retries: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            ttl_expirations: AtomicU64::new(0),
            flush_cycles: AtomicU64::new(0),
            flushed_ops_total: AtomicU64::new(0),
            flushed_bytes_total: AtomicU64::new(0),
            baseline_snapshot: Mutex::new(RedbSnapshot::default()),
            inode_ranges: Mutex::new(RangeState::default()),
            block_ranges: Mutex::new(RangeState::default()),
            inode_alloc_lock: Mutex::new(()),
            block_alloc_lock: Mutex::new(()),
        });

        // Best-effort initial pull so an empty local cache can hydrate from remote state.
        let _ = engine.pull_remote_snapshot();
        if let Ok(s) = engine.local.export_snapshot() {
            *engine.baseline_snapshot.lock() = s;
        }
        engine.spawn_background_flush_worker();

        Ok(engine)
    }

    pub fn flush_now(&self) -> MetaResult<()> {
        self.flush_remote_snapshot(true)
    }

    fn run_on_internal_rt<F, T>(&self, fut: F) -> T
    where
        F: Future<Output = T>,
    {
        let mut rt_guard = self.rt.lock();
        let rt = rt_guard
            .as_mut()
            .expect("internal tokio runtime is unavailable");
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| rt.block_on(fut))
        } else {
            rt.block_on(fut)
        }
    }

    fn publish_invalidation_all(&self) {
        let (Some(client), Some(subject_base)) = (&self.nats_client, &self.nats_subject_base)
        else {
            return;
        };
        let subject = format!("{}.all", subject_base);
        let payload = self.invalidation_sender_id.clone().into_bytes();
        let client = client.clone();
        let fut = async move { client.publish(subject, payload.into()).await };
        if let Err(err) = self.run_on_internal_rt(fut) {
            warn!("failed to publish invalidation event: {}", err);
        }
    }

    fn spawn_background_flush_worker(self: &Arc<Self>) {
        if self.remote.is_none() {
            return;
        }

        // Poll frequently enough to honor small flush intervals without
        // introducing noticeable shutdown delays.
        let tick = Duration::from_millis(250);
        let weak = Arc::downgrade(self);
        let thread_name = format!("tonbo-flush-{}", self.volume_name);
        let _ = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                loop {
                    std::thread::sleep(tick);
                    let Some(engine) = weak.upgrade() else {
                        break;
                    };

                    let should_flush = {
                        let state = engine.state.lock();
                        (state.pending_ops > 0 || state.pending_bytes > 0)
                            && state.last_flush.elapsed() >= engine.flush_interval
                    };
                    if !should_flush {
                        continue;
                    }

                    if let Err(err) = engine.flush_remote_snapshot(false) {
                        if TonboMetaEngine::is_temporary_remote_error(&err) {
                            warn!(
                                "tonbo periodic metadata flush deferred due to temporary remote error: {}",
                                err
                            );
                        } else {
                            warn!("tonbo periodic metadata flush failed: {}", err);
                        }
                    }
                }
            });
    }

    fn remote_stat(&self, remote: &RemoteStore, path: &str) -> MetaResult<opendal::Metadata> {
        for attempt in 1..=REMOTE_IO_MAX_ATTEMPTS {
            let fut = async {
                tokio::time::timeout(self.remote_request_timeout, remote.op.stat(path)).await
            };
            match self.run_on_internal_rt(fut) {
                Ok(Ok(meta)) => return Ok(meta),
                Ok(Err(e)) => {
                    let err = MetaError::Storage(e.to_string());
                    if attempt < REMOTE_IO_MAX_ATTEMPTS && Self::is_temporary_remote_error(&err) {
                        let backoff_ms =
                            REMOTE_IO_RETRY_BASE_MS.saturating_mul(1u64 << (attempt - 1));
                        self.run_on_internal_rt(async {
                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        });
                        continue;
                    }
                    return Err(err);
                }
                Err(_) => {
                    let err = MetaError::Storage(format!("timeout while stat {}", path));
                    if attempt < REMOTE_IO_MAX_ATTEMPTS {
                        let backoff_ms =
                            REMOTE_IO_RETRY_BASE_MS.saturating_mul(1u64 << (attempt - 1));
                        self.run_on_internal_rt(async {
                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        });
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        Err(MetaError::Storage(format!("timeout while stat {}", path)))
    }

    fn remote_read_all(&self, remote: &RemoteStore, path: &str) -> MetaResult<Vec<u8>> {
        for attempt in 1..=REMOTE_IO_MAX_ATTEMPTS {
            let fut = async {
                tokio::time::timeout(self.remote_request_timeout, remote.op.read(path)).await
            };
            match self.run_on_internal_rt(fut) {
                Ok(Ok(buf)) => return Ok(buf.to_vec()),
                Ok(Err(e)) => {
                    let err = MetaError::Storage(e.to_string());
                    if attempt < REMOTE_IO_MAX_ATTEMPTS && Self::is_temporary_remote_error(&err) {
                        let backoff_ms =
                            REMOTE_IO_RETRY_BASE_MS.saturating_mul(1u64 << (attempt - 1));
                        self.run_on_internal_rt(async {
                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        });
                        continue;
                    }
                    return Err(err);
                }
                Err(_) => {
                    let err = MetaError::Storage(format!("timeout while read {}", path));
                    if attempt < REMOTE_IO_MAX_ATTEMPTS {
                        let backoff_ms =
                            REMOTE_IO_RETRY_BASE_MS.saturating_mul(1u64 << (attempt - 1));
                        self.run_on_internal_rt(async {
                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        });
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        Err(MetaError::Storage(format!("timeout while read {}", path)))
    }

    fn remote_write_options(
        &self,
        remote: &RemoteStore,
        path: &str,
        payload: Vec<u8>,
        opts: WriteOptions,
    ) -> MetaResult<()> {
        for attempt in 1..=REMOTE_IO_MAX_ATTEMPTS {
            let fut = async {
                tokio::time::timeout(
                    self.remote_request_timeout,
                    remote.op.write_options(path, payload.clone(), opts.clone()),
                )
                .await
            };
            match self.run_on_internal_rt(fut) {
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(e)) => {
                    let err = MetaError::Storage(e.to_string());
                    if attempt < REMOTE_IO_MAX_ATTEMPTS && Self::is_temporary_remote_error(&err) {
                        let backoff_ms =
                            REMOTE_IO_RETRY_BASE_MS.saturating_mul(1u64 << (attempt - 1));
                        self.run_on_internal_rt(async {
                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        });
                        continue;
                    }
                    return Err(err);
                }
                Err(_) => {
                    let err = MetaError::Storage(format!("timeout while write {}", path));
                    if attempt < REMOTE_IO_MAX_ATTEMPTS {
                        let backoff_ms =
                            REMOTE_IO_RETRY_BASE_MS.saturating_mul(1u64 << (attempt - 1));
                        self.run_on_internal_rt(async {
                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        });
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        Err(MetaError::Storage(format!("timeout while write {}", path)))
    }

    fn remote_read_manifest(
        &self,
        remote: &RemoteStore,
        path: &str,
    ) -> MetaResult<(TonboManifest, Option<String>)> {
        let stat = self.remote_stat(remote, path)?;
        let etag = stat.etag().map(ToString::to_string);
        let bytes = self.remote_read_all(remote, path)?;
        let manifest: TonboManifest =
            serde_json::from_slice(&bytes).map_err(|e| MetaError::Serialization(e.to_string()))?;
        Ok((manifest, etag))
    }

    fn remote_read_u64_with_etag(
        &self,
        remote: &RemoteStore,
        path: &str,
    ) -> MetaResult<(Option<u64>, Option<String>)> {
        match self.remote_stat(remote, path) {
            Ok(meta) => {
                let etag = meta.etag().map(ToString::to_string);
                let bytes = self.remote_read_all(remote, path)?;
                let parsed = String::from_utf8(bytes)
                    .ok()
                    .and_then(|s| s.trim().parse::<u64>().ok());
                Ok((parsed, etag))
            }
            Err(err) if Self::is_not_found(&err) => Ok((None, None)),
            Err(err) => Err(err),
        }
    }

    fn reserve_remote_range(
        &self,
        remote: &RemoteStore,
        path: &str,
        default_next: u64,
        size: u64,
    ) -> MetaResult<IdRange> {
        let _guard = self.sync_lock.lock();
        let mut retries = 0u32;

        loop {
            let (maybe_current, etag) = self.remote_read_u64_with_etag(remote, path)?;
            let current = maybe_current.unwrap_or(default_next);
            let new_next = current.saturating_add(size);
            let mut opts = WriteOptions::default();
            if let Some(v) = etag {
                opts.if_match = Some(v);
            } else {
                opts.if_not_exists = true;
            }
            let payload = format!("{new_next}\n").into_bytes();

            match self.remote_write_options(remote, path, payload, opts) {
                Ok(()) => return Ok(IdRange::new(current, size)),
                Err(err) if Self::is_condition_not_match(&err) => {
                    retries = retries.saturating_add(1);
                    if retries >= RANGE_CAS_MAX_RETRIES {
                        return Err(MetaError::Storage(format!(
                            "range CAS retries exhausted for {} after {} attempts",
                            path, RANGE_CAS_MAX_RETRIES
                        )));
                    }
                    let backoff_ms = (20u64).saturating_mul(1u64 << retries.min(6));
                    let _ = self.run_on_internal_rt(async {
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    });
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn allocate_from_ranges(
        &self,
        ranges: &Mutex<RangeState>,
        path: &str,
        default_next: u64,
        size: u64,
    ) -> MetaResult<u64> {
        let Some(remote) = &self.remote else {
            return Err(MetaError::Storage(
                "remote store not configured".to_string(),
            ));
        };

        let mut state = ranges.lock();
        if state.active.is_none() {
            state.active = Some(self.reserve_remote_range(remote, path, default_next, size)?);
        }
        if state.standby.is_none() {
            state.standby = Some(self.reserve_remote_range(remote, path, default_next, size)?);
        }

        let id = if let Some(active) = state.active.as_mut() {
            match active.take_one() {
                Some(v) => v,
                None => {
                    state.active = state.standby.take();
                    if state.active.is_none() {
                        state.active =
                            Some(self.reserve_remote_range(remote, path, default_next, size)?);
                    }
                    state
                        .active
                        .as_mut()
                        .and_then(|r| r.take_one())
                        .ok_or_else(|| {
                            MetaError::Storage("failed to allocate id from refreshed range".into())
                        })?
                }
            }
        } else {
            return Err(MetaError::Storage(
                "failed to initialize active range".to_string(),
            ));
        };

        if let Some(active) = &state.active {
            if active.remaining() <= size && state.standby.is_none() {
                state.standby =
                    Some(self.reserve_remote_range(remote, path, default_next, size)?);
            }
        }

        Ok(id)
    }

    fn bump_local_counter_floor(&self, name: &str, floor: u64) -> MetaResult<()> {
        let current = self.local.get_counter(name).unwrap_or(0);
        if current < floor {
            self.local.set_counter(name, floor)?;
        }
        Ok(())
    }

    fn map_from_vec<K, V>(items: &[(K, V)]) -> HashMap<K, V>
    where
        K: Eq + Hash + Clone,
        V: Clone,
    {
        let mut m = HashMap::with_capacity(items.len());
        for (k, v) in items {
            m.insert(k.clone(), v.clone());
        }
        m
    }

    fn merge_table_delta_with<K, V, F>(
        base: &[(K, V)],
        local: &[(K, V)],
        remote: &[(K, V)],
        mut conflict_resolver: F,
    ) -> Vec<(K, V)>
    where
        K: Eq + Hash + Clone,
        V: Clone + PartialEq,
        F: FnMut(&K, &V, &V) -> V,
    {
        let base_map = Self::map_from_vec(base);
        let local_map = Self::map_from_vec(local);
        let remote_map = Self::map_from_vec(remote);
        let mut merged = remote_map.clone();

        let mut keys: HashSet<K> = HashSet::new();
        keys.extend(base_map.keys().cloned());
        keys.extend(local_map.keys().cloned());
        keys.extend(remote_map.keys().cloned());

        for k in keys {
            let base_v = base_map.get(&k);
            let local_v = local_map.get(&k);
            let remote_v = remote_map.get(&k);
            let local_changed = local_v != base_v;
            let remote_changed = remote_v != base_v;

            if !local_changed {
                continue;
            }

            if !remote_changed {
                match local_v {
                    Some(v) => {
                        merged.insert(k.clone(), v.clone());
                    }
                    None => {
                        merged.remove(&k);
                    }
                }
                continue;
            }

            // Both local and remote changed from the same base key:
            // resolve deterministically to avoid oscillation between clients.
            match (local_v, remote_v) {
                (Some(l), Some(r)) => {
                    merged.insert(k.clone(), conflict_resolver(&k, l, r));
                }
                (Some(l), None) => {
                    merged.insert(k.clone(), l.clone());
                }
                (None, Some(r)) => {
                    merged.insert(k.clone(), r.clone());
                }
                (None, None) => {
                    merged.remove(&k);
                }
            }
        }

        merged.into_iter().collect()
    }

    fn merge_table_delta<K, V>(base: &[(K, V)], local: &[(K, V)], remote: &[(K, V)]) -> Vec<(K, V)>
    where
        K: Eq + Hash + Clone,
        V: Clone + PartialEq + Ord,
    {
        Self::merge_table_delta_with(base, local, remote, |_k, l, r| {
            if l >= r { l.clone() } else { r.clone() }
        })
    }

    fn decode_postcard<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Option<T> {
        postcard::from_bytes(bytes).ok()
    }

    fn encode_postcard<T: serde::Serialize>(value: &T) -> Option<Vec<u8>> {
        postcard::to_stdvec(value).ok()
    }

    fn merge_inodes_delta(
        base: &[(u64, Vec<u8>)],
        local: &[(u64, Vec<u8>)],
        remote: &[(u64, Vec<u8>)],
    ) -> Vec<(u64, Vec<u8>)> {
        Self::merge_table_delta_with(base, local, remote, |_k, l, r| {
            let local_attr = Self::decode_postcard::<InodeAttr>(l);
            let remote_attr = Self::decode_postcard::<InodeAttr>(r);
            let chosen = match (local_attr, remote_attr) {
                (Some(la), Some(ra)) => {
                    if la.ctime != ra.ctime {
                        if la.ctime > ra.ctime { la } else { ra }
                    } else if la.mtime != ra.mtime {
                        if la.mtime > ra.mtime { la } else { ra }
                    } else if la.size != ra.size {
                        if la.size > ra.size { la } else { ra }
                    } else if l >= r {
                        la
                    } else {
                        ra
                    }
                }
                _ => return if l >= r { l.clone() } else { r.clone() },
            };
            Self::encode_postcard(&chosen)
                .unwrap_or_else(|| if l >= r { l.clone() } else { r.clone() })
        })
    }

    fn merge_dentries_delta(
        base: &[(Vec<u8>, Vec<u8>)],
        local: &[(Vec<u8>, Vec<u8>)],
        remote: &[(Vec<u8>, Vec<u8>)],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        Self::merge_table_delta_with(base, local, remote, |_k, l, r| {
            let local_e = Self::decode_postcard::<DirEntryValue>(l);
            let remote_e = Self::decode_postcard::<DirEntryValue>(r);
            let chosen = match (local_e, remote_e) {
                (Some(le), Some(re)) => {
                    if le.ino != re.ino {
                        if le.ino > re.ino { le } else { re }
                    } else if (le.kind as u8) >= (re.kind as u8) {
                        le
                    } else {
                        re
                    }
                }
                _ => return if l >= r { l.clone() } else { r.clone() },
            };
            Self::encode_postcard(&chosen)
                .unwrap_or_else(|| if l >= r { l.clone() } else { r.clone() })
        })
    }

    fn merge_file_blocks_delta(
        base: &[(Vec<u8>, Vec<u8>)],
        local: &[(Vec<u8>, Vec<u8>)],
        remote: &[(Vec<u8>, Vec<u8>)],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        Self::merge_table_delta_with(base, local, remote, |_k, l, r| {
            let local_b = Self::decode_postcard::<BlockRef>(l);
            let remote_b = Self::decode_postcard::<BlockRef>(r);
            let chosen = match (local_b, remote_b) {
                (Some(lb), Some(rb)) => {
                    if lb.object_id != rb.object_id {
                        if lb.object_id > rb.object_id { lb } else { rb }
                    } else if lb.length != rb.length {
                        if lb.length > rb.length { lb } else { rb }
                    } else if (lb.flags.bits()) >= (rb.flags.bits()) {
                        lb
                    } else {
                        rb
                    }
                }
                _ => return if l >= r { l.clone() } else { r.clone() },
            };
            Self::encode_postcard(&chosen)
                .unwrap_or_else(|| if l >= r { l.clone() } else { r.clone() })
        })
    }

    fn merge_counters_delta(
        base: &[(String, u64)],
        local: &[(String, u64)],
        remote: &[(String, u64)],
    ) -> Vec<(String, u64)> {
        let base_map = Self::map_from_vec(base);
        let local_map = Self::map_from_vec(local);
        let mut merged = Self::map_from_vec(remote);

        let mut keys: HashSet<String> = HashSet::new();
        keys.extend(base_map.keys().cloned());
        keys.extend(local_map.keys().cloned());
        keys.extend(merged.keys().cloned());

        for key in keys {
            let base_v = base_map.get(&key).copied();
            let local_v = local_map.get(&key).copied();
            let remote_v = merged.get(&key).copied();

            let changed_local = local_v != base_v;
            if !changed_local {
                continue;
            }

            let merged_v = match key.as_str() {
                counters::NEXT_INODE | counters::NEXT_BLOCK_ID | "nextEventSeq" => {
                    local_v.unwrap_or(0).max(remote_v.unwrap_or(0))
                }
                _ => {
                    let base_i = base_v.unwrap_or(0) as i128;
                    let local_i = local_v.unwrap_or(0) as i128;
                    let remote_i = remote_v.unwrap_or(0) as i128;
                    let delta = local_i - base_i;
                    let candidate = remote_i + delta;
                    candidate.max(0).min(u64::MAX as i128) as u64
                }
            };

            merged.insert(key, merged_v);
        }

        merged.into_iter().collect()
    }

    fn merge_snapshot_with_local_delta(
        base: &RedbSnapshot,
        local: &RedbSnapshot,
        remote: &RedbSnapshot,
    ) -> RedbSnapshot {
        let inodes = Self::merge_inodes_delta(&base.inodes, &local.inodes, &remote.inodes);
        let mut dentries = Self::merge_dentries_delta(&base.dentries, &local.dentries, &remote.dentries);
        Self::resolve_conflicting_rename_targets(&base.dentries, &local.dentries, &remote.dentries, &mut dentries);

        RedbSnapshot {
            settings: Self::merge_table_delta(&base.settings, &local.settings, &remote.settings),
            counters: Self::merge_counters_delta(&base.counters, &local.counters, &remote.counters),
            inodes,
            dentries,
            file_blocks: Self::merge_file_blocks_delta(
                &base.file_blocks,
                &local.file_blocks,
                &remote.file_blocks,
            ),
            pending_deletions: Self::merge_table_delta(
                &base.pending_deletions,
                &local.pending_deletions,
                &remote.pending_deletions,
            ),
            symlinks: Self::merge_table_delta(&base.symlinks, &local.symlinks, &remote.symlinks),
            xattrs: Self::merge_table_delta(&base.xattrs, &local.xattrs, &remote.xattrs),
            event_log: Self::merge_table_delta(
                &base.event_log,
                &local.event_log,
                &remote.event_log,
            ),
        }
    }

    fn decode_dentry_map(dentries: &[(Vec<u8>, Vec<u8>)]) -> HashMap<Vec<u8>, DirEntryValue> {
        let mut out = HashMap::with_capacity(dentries.len());
        for (k, v) in dentries {
            if let Some(entry) = Self::decode_postcard::<DirEntryValue>(v) {
                out.insert(k.clone(), entry);
            }
        }
        out
    }

    fn resolve_conflicting_rename_targets(
        base: &[(Vec<u8>, Vec<u8>)],
        local: &[(Vec<u8>, Vec<u8>)],
        remote: &[(Vec<u8>, Vec<u8>)],
        dentries: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        let base_map = Self::decode_dentry_map(base);
        let local_map = Self::decode_dentry_map(local);
        let remote_map = Self::decode_dentry_map(remote);

        let mut conflict_targets_by_ino: HashMap<u64, HashSet<Vec<u8>>> = HashMap::new();
        let mut winner_key_by_ino: HashMap<u64, Vec<u8>> = HashMap::new();

        for (base_key, base_entry) in base_map.iter() {
            // We only care about a key that both sides removed.
            if local_map.contains_key(base_key) || remote_map.contains_key(base_key) {
                continue;
            }

            let ino = base_entry.ino;
            let local_targets: Vec<Vec<u8>> = local_map
                .iter()
                .filter(|(k, v)| !base_map.contains_key(*k) && v.ino == ino)
                .map(|(k, _)| k.clone())
                .collect();
            let remote_targets: Vec<Vec<u8>> = remote_map
                .iter()
                .filter(|(k, v)| !base_map.contains_key(*k) && v.ino == ino)
                .map(|(k, _)| k.clone())
                .collect();

            // Conflicting renames require both branches to introduce a new key.
            if local_targets.is_empty() || remote_targets.is_empty() {
                continue;
            }

            let mut all_targets = local_targets;
            all_targets.extend(remote_targets);
            all_targets.sort();
            all_targets.dedup();
            let winner = all_targets.last().cloned();
            let Some(winner) = winner else {
                continue;
            };

            let targets = conflict_targets_by_ino.entry(ino).or_default();
            for k in all_targets {
                targets.insert(k);
            }
            winner_key_by_ino
                .entry(ino)
                .and_modify(|current| {
                    if winner > *current {
                        *current = winner.clone();
                    }
                })
                .or_insert(winner);
        }

        if conflict_targets_by_ino.is_empty() {
            return;
        }

        dentries.retain(|(key, raw)| {
            let Some(entry) = Self::decode_postcard::<DirEntryValue>(raw) else {
                return true;
            };
            let Some(targets) = conflict_targets_by_ino.get(&entry.ino) else {
                return true;
            };
            if !targets.contains(key) {
                return true;
            }
            winner_key_by_ino.get(&entry.ino) == Some(key)
        });
    }

    fn is_cache_miss(err: &MetaError) -> bool {
        matches!(
            err,
            MetaError::InodeNotFound(_) | MetaError::EntryNotFound { .. } | MetaError::NotFormatted
        )
    }

    fn is_not_found(err: &MetaError) -> bool {
        matches!(err, MetaError::Storage(msg) if msg.contains("NotFound") || msg.contains("404"))
    }

    fn is_condition_not_match(err: &MetaError) -> bool {
        matches!(err, MetaError::Storage(msg) if msg.contains("ConditionNotMatch") || msg.contains("412"))
    }

    fn has_pending_local_mutations(&self) -> bool {
        let state = self.state.lock();
        state.pending_ops > 0 || state.pending_bytes > 0
    }

    fn is_temporary_remote_error(err: &MetaError) -> bool {
        matches!(
            err,
            MetaError::Storage(msg)
                if msg.contains("temporary")
                    || msg.contains("Temporary")
                    || msg.contains("timeout")
                    || msg.contains("timed out")
                    || msg.contains("error sending request")
        )
    }

    fn maybe_refresh_ttl(&self) -> MetaResult<()> {
        if self.remote.is_none() {
            return Ok(());
        }
        if self.has_pending_local_mutations() {
            // Never refresh from remote while local, not-yet-flushed mutations exist.
            // Pulling a stale remote snapshot here can clobber newly written block refs.
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let should_refresh = {
            let state = self.state.lock();
            state.last_refresh.elapsed() >= self.cache_ttl
        };
        if should_refresh {
            self.ttl_expirations.fetch_add(1, Ordering::Relaxed);
            if let Err(err) = self.pull_remote_snapshot() {
                // Temporary remote hiccups should not break local I/O paths.
                // Keep serving from the local snapshot and retry on next TTL.
                warn!("tonbo ttl refresh failed, using local snapshot: {}", err);
            }
        } else {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn read_with_refresh<T, F>(&self, mut op: F) -> MetaResult<T>
    where
        F: FnMut() -> MetaResult<T>,
    {
        self.maybe_refresh_ttl()?;
        match op() {
            Ok(v) => Ok(v),
            Err(err) if Self::is_cache_miss(&err) => {
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                if self.has_pending_local_mutations() {
                    return Err(err);
                }
                self.pull_remote_snapshot()?;
                op()
            }
            Err(err) => Err(err),
        }
    }

    fn on_mutation(&self, approx_delta_bytes: u64) -> MetaResult<()> {
        if self.remote.is_none() {
            return Ok(());
        }
        let should_flush = {
            let mut state = self.state.lock();
            state.pending_ops = state.pending_ops.saturating_add(1);
            state.pending_bytes = state.pending_bytes.saturating_add(approx_delta_bytes);
            state.pending_ops >= self.flush_ops_threshold
                || state.pending_bytes >= self.flush_bytes_threshold
                || state.last_flush.elapsed() >= self.flush_interval
        };
        if should_flush {
            if let Err(err) = self.flush_remote_snapshot(false) {
                if Self::is_temporary_remote_error(&err) {
                    // Keep pending counters as-is and retry on next mutation/flush cycle.
                    warn!(
                        "tonbo deferred metadata flush due to temporary remote error: {}",
                        err
                    );
                } else {
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    fn pull_remote_snapshot(&self) -> MetaResult<()> {
        let Some(remote) = &self.remote else {
            return Ok(());
        };
        let _guard = self.sync_lock.lock();

        match self.remote_stat(remote, &remote.manifest_path) {
            Ok(meta) => {
                self.remote_round_trips.fetch_add(1, Ordering::Relaxed);
                let etag = meta.etag().map(ToString::to_string);
                let bytes = match self.remote_read_all(remote, &remote.manifest_path) {
                    Ok(v) => v,
                    Err(err) if Self::is_temporary_remote_error(&err) => {
                        warn!(
                            "tonbo remote read failed temporarily, using local snapshot: {}",
                            err
                        );
                        self.state.lock().last_refresh = Instant::now();
                        return Ok(());
                    }
                    Err(err) => return Err(err),
                };
                let manifest: TonboManifest = serde_json::from_slice(&bytes)
                    .map_err(|e| MetaError::Serialization(e.to_string()))?;
                self.local.import_snapshot(&manifest.snapshot)?;
                *self.baseline_snapshot.lock() = manifest.snapshot.clone();

                let mut state = self.state.lock();
                state.last_refresh = Instant::now();
                state.etag = etag;
                Ok(())
            }
            Err(err) if Self::is_not_found(&err) => {
                for legacy in &remote.legacy_manifest_paths {
                    match self.remote_stat(remote, legacy) {
                        Ok(_legacy_meta) => {
                            self.remote_round_trips.fetch_add(1, Ordering::Relaxed);
                            let bytes = match self.remote_read_all(remote, legacy) {
                                Ok(v) => v,
                                Err(read_err) if Self::is_temporary_remote_error(&read_err) => {
                                    warn!(
                                        "tonbo legacy remote read failed temporarily, using local snapshot: {}",
                                        read_err
                                    );
                                    self.state.lock().last_refresh = Instant::now();
                                    return Ok(());
                                }
                                Err(read_err) => return Err(read_err),
                            };
                            let manifest: TonboManifest = serde_json::from_slice(&bytes)
                                .map_err(|e| MetaError::Serialization(e.to_string()))?;
                            self.local.import_snapshot(&manifest.snapshot)?;
                            *self.baseline_snapshot.lock() = manifest.snapshot.clone();
                            let mut state = self.state.lock();
                            state.last_refresh = Instant::now();
                            // Legacy read has no CAS guarantee for primary key path.
                            state.etag = None;
                            return Ok(());
                        }
                        Err(legacy_err) if Self::is_not_found(&legacy_err) => {}
                        Err(legacy_err) if Self::is_temporary_remote_error(&legacy_err) => {
                            warn!(
                                "tonbo legacy remote stat failed temporarily, using local snapshot: {}",
                                legacy_err
                            );
                            self.state.lock().last_refresh = Instant::now();
                            return Ok(());
                        }
                        Err(legacy_err) => return Err(MetaError::Storage(legacy_err.to_string())),
                    }
                }
                let mut state = self.state.lock();
                state.last_refresh = Instant::now();
                if let Ok(s) = self.local.export_snapshot() {
                    *self.baseline_snapshot.lock() = s;
                }
                Ok(())
            }
            Err(err) if Self::is_temporary_remote_error(&err) => {
                warn!(
                    "tonbo remote stat failed temporarily, using local snapshot: {}",
                    err
                );
                self.state.lock().last_refresh = Instant::now();
                Ok(())
            }
            Err(err) => Err(MetaError::Storage(err.to_string())),
        }
    }

    fn flush_remote_snapshot(&self, force: bool) -> MetaResult<()> {
        let Some(remote) = &self.remote else {
            return Ok(());
        };

        let (pending_ops, pending_bytes, etag) = {
            let state = self.state.lock();
            if !force
                && state.pending_ops == 0
                && state.pending_bytes == 0
                && state.last_flush.elapsed() < self.flush_interval
            {
                return Ok(());
            }
            (state.pending_ops, state.pending_bytes, state.etag.clone())
        };

        // Never rewrite remote state when there are no local mutations, except
        // bootstrap of a brand-new remote manifest (force + unknown etag).
        // This prevents stale clients from clobbering fresher manifests on drop.
        if pending_ops == 0 && pending_bytes == 0 {
            if !force || etag.is_some() {
                return Ok(());
            }
        }

        let _guard = self.sync_lock.lock();

        let local_snapshot = self.local.export_snapshot()?;
        let baseline_snapshot = self.baseline_snapshot.lock().clone();
        let mut snapshot_to_write = local_snapshot.clone();
        let mut manifest = TonboManifest {
            updated_at_unix_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or_default(),
            snapshot: snapshot_to_write.clone(),
        };

        self.flush_cycles.fetch_add(1, Ordering::Relaxed);
        self.flushed_ops_total
            .fetch_add(pending_ops as u64, Ordering::Relaxed);
        self.flushed_bytes_total
            .fetch_add(pending_bytes, Ordering::Relaxed);

        let mut current_etag = etag;
        if current_etag.is_none() {
            match self.remote_stat(remote, &remote.manifest_path) {
                Ok(meta) => {
                    current_etag = meta.etag().map(ToString::to_string);
                }
                Err(err) if Self::is_not_found(&err) => {}
                Err(err) => return Err(err),
            }
        }
        let mut retries: u32 = 0;
        loop {
            manifest.snapshot = snapshot_to_write.clone();
            let payload = serde_json::to_vec(&manifest)
                .map_err(|e| MetaError::Serialization(e.to_string()))?;
            let mut opts = WriteOptions::default();
            if let Some(ref v) = current_etag {
                opts.if_match = Some(v.clone());
            } else {
                opts.if_not_exists = true;
            }

            match self.remote_write_options(remote, &remote.manifest_path, payload.clone(), opts) {
                Ok(()) => break,
                Err(err) if Self::is_condition_not_match(&err) => {
                    self.cas_retries.fetch_add(1, Ordering::Relaxed);
                    if retries >= self.cas_max_retries {
                        return Err(MetaError::Storage(format!(
                            "tonbo CAS retries exhausted after {} attempts",
                            self.cas_max_retries
                        )));
                    }
                    self.remote_round_trips.fetch_add(1, Ordering::Relaxed);
                    let (remote_manifest, remote_etag) =
                        self.remote_read_manifest(remote, &remote.manifest_path)?;
                    snapshot_to_write = Self::merge_snapshot_with_local_delta(
                        &baseline_snapshot,
                        &local_snapshot,
                        &remote_manifest.snapshot,
                    );
                    current_etag = remote_etag;
                    let backoff_ms = (50u64).saturating_mul(1u64 << retries.min(6));
                    let _ = self.run_on_internal_rt(async {
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    });
                    retries = retries.saturating_add(1);
                }
                Err(err) => return Err(err),
            }
        }

        self.remote_round_trips.fetch_add(1, Ordering::Relaxed);
        let new_etag = self
            .remote_stat(remote, &remote.manifest_path)
            .ok()
            .and_then(|m| m.etag().map(ToString::to_string));

        {
            let mut state = self.state.lock();
            // Don't blindly zero counters: concurrent local mutations can happen
            // while this flush is in flight. Preserve them for the next cycle.
            state.pending_ops = state.pending_ops.saturating_sub(pending_ops);
            state.pending_bytes = state.pending_bytes.saturating_sub(pending_bytes);
            state.last_flush = Instant::now();
            state.last_refresh = Instant::now();
            if new_etag.is_some() {
                state.etag = new_etag;
            }
        }
        // Never overwrite local state with the flushed snapshot: local may have
        // newer mutations that happened after `local_snapshot` was taken.
        *self.baseline_snapshot.lock() = snapshot_to_write;

        debug!(
            pending_ops,
            pending_bytes,
            cas_retries_in_cycle = retries,
            "tonbo metadata flush complete"
        );
        self.publish_invalidation_all();
        Ok(())
    }

    fn metrics_debug(&self) {
        debug!(
            remote_round_trips = self.remote_round_trips.load(Ordering::Relaxed),
            cas_retries = self.cas_retries.load(Ordering::Relaxed),
            cache_hits = self.cache_hits.load(Ordering::Relaxed),
            cache_misses = self.cache_misses.load(Ordering::Relaxed),
            ttl_expirations = self.ttl_expirations.load(Ordering::Relaxed),
            flush_cycles = self.flush_cycles.load(Ordering::Relaxed),
            flushed_ops_total = self.flushed_ops_total.load(Ordering::Relaxed),
            flushed_bytes_total = self.flushed_bytes_total.load(Ordering::Relaxed),
            volume = self.volume_name,
            "tonbo metadata metrics"
        );
    }
}

impl Drop for TonboMetaEngine {
    fn drop(&mut self) {
        if let Err(err) = self.flush_remote_snapshot(true) {
            warn!("tonbo flush on drop failed: {}", err);
        }
        self.metrics_debug();

        let rt = self.rt.get_mut().take();
        if let Some(rt) = rt {
            if tokio::runtime::Handle::try_current().is_ok() {
                // Dropping a runtime from within a Tokio worker can panic because
                // shutdown needs blocking. Move drop to a plain OS thread.
                if std::thread::Builder::new()
                    .name("tonbo-rt-drop".to_string())
                    .spawn(move || drop(rt))
                    .is_err()
                {
                    // Last-resort: leak instead of panicking during Drop.
                }
            } else {
                drop(rt);
            }
        }
    }
}

impl MetaEngine for TonboMetaEngine {
    fn flush_metadata(&self) -> MetaResult<()> {
        self.flush_now()
    }

    fn sync_metadata(&self) -> MetaResult<()> {
        if self.has_pending_local_mutations() {
            // Preserve local, not-yet-pushed mutations before pulling remote.
            // Otherwise a remote import can clobber freshly written local refs.
            self.flush_remote_snapshot(false)?;
        }
        self.pull_remote_snapshot()
    }

    fn invalidation_sender_id(&self) -> Option<String> {
        Some(self.invalidation_sender_id.clone())
    }

    fn set_format_info(&self, info: &FormatInfo) -> MetaResult<()> {
        let r = self.local.set_format_info(info);
        if r.is_ok() {
            self.on_mutation(1024)?;
        }
        r
    }

    fn get_format_info(&self) -> MetaResult<FormatInfo> {
        self.read_with_refresh(|| self.local.get_format_info())
    }

    fn get_counter(&self, name: &str) -> MetaResult<u64> {
        self.read_with_refresh(|| self.local.get_counter(name))
    }

    fn incr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        let r = self.local.incr_counter(name, delta);
        if r.is_ok() {
            self.on_mutation(64)?;
        }
        r
    }

    fn decr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        let r = self.local.decr_counter(name, delta);
        if r.is_ok() {
            self.on_mutation(64)?;
        }
        r
    }

    fn set_counter(&self, name: &str, value: u64) -> MetaResult<()> {
        let r = self.local.set_counter(name, value);
        if r.is_ok() {
            self.on_mutation(64)?;
        }
        r
    }

    fn next_inode(&self) -> MetaResult<Ino> {
        let id = if let Some(remote) = &self.remote {
            let _guard = self.inode_alloc_lock.lock();
            self.allocate_from_ranges(
                &self.inode_ranges,
                &remote.inode_counter_path,
                remote.default_next_inode,
                INODE_RANGE_SIZE,
            )?
        } else {
            self.local.next_inode()?
        };
        self.bump_local_counter_floor(counters::NEXT_INODE, id.saturating_add(1))?;
        self.on_mutation(32)?;
        Ok(id)
    }

    fn get_attr(&self, ino: Ino) -> MetaResult<InodeAttr> {
        self.read_with_refresh(|| self.local.get_attr(ino))
    }

    fn set_attr(&self, attr: &InodeAttr) -> MetaResult<()> {
        let r = self.local.set_attr(attr);
        if r.is_ok() {
            self.on_mutation(512)?;
        }
        r
    }

    fn remove_attr(&self, ino: Ino) -> MetaResult<()> {
        let r = self.local.remove_attr(ino);
        if r.is_ok() {
            self.on_mutation(256)?;
        }
        r
    }

    fn lookup(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue> {
        self.read_with_refresh(|| self.local.lookup(parent, name))
    }

    fn readdir(&self, parent: Ino) -> MetaResult<Vec<DirEntry>> {
        self.read_with_refresh(|| self.local.readdir(parent))
    }

    fn add_entry(&self, parent: Ino, name: &[u8], entry: &DirEntryValue) -> MetaResult<()> {
        let r = self.local.add_entry(parent, name, entry);
        if r.is_ok() {
            self.on_mutation(256)?;
        }
        r
    }

    fn remove_entry(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue> {
        let r = self.local.remove_entry(parent, name);
        if r.is_ok() {
            self.on_mutation(256)?;
        }
        r
    }

    fn get_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<BlockRef>> {
        self.read_with_refresh(|| self.local.get_file_block(ino, block_index))
    }

    fn set_file_block(
        &self,
        ino: Ino,
        block_index: u32,
        block: &BlockRef,
    ) -> MetaResult<Option<BlockRef>> {
        let r = self.local.set_file_block(ino, block_index, block);
        if r.is_ok() {
            self.on_mutation(256)?;
        }
        r
    }

    fn get_file_blocks(&self, ino: Ino) -> MetaResult<Vec<(u32, BlockRef)>> {
        self.read_with_refresh(|| self.local.get_file_blocks(ino))
    }

    fn remove_file_blocks(&self, ino: Ino) -> MetaResult<Vec<u64>> {
        let r = self.local.remove_file_blocks(ino);
        if r.is_ok() {
            self.on_mutation(512)?;
        }
        r
    }

    fn remove_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<u64>> {
        let r = self.local.remove_file_block(ino, block_index);
        if r.is_ok() {
            self.on_mutation(128)?;
        }
        r
    }

    fn next_block_id(&self) -> MetaResult<u64> {
        let id = if let Some(remote) = &self.remote {
            let _guard = self.block_alloc_lock.lock();
            self.allocate_from_ranges(
                &self.block_ranges,
                &remote.block_counter_path,
                remote.default_next_block_id,
                BLOCK_RANGE_SIZE,
            )?
        } else {
            self.local.next_block_id()?
        };
        self.bump_local_counter_floor(counters::NEXT_BLOCK_ID, id.saturating_add(1))?;
        self.on_mutation(32)?;
        Ok(id)
    }

    fn next_block_ids(&self, count: u32) -> MetaResult<Vec<u64>> {
        if let Some(remote) = &self.remote {
            let _guard = self.block_alloc_lock.lock();
            let mut ids = Vec::with_capacity(count as usize);
            for _ in 0..count {
                ids.push(self.allocate_from_ranges(
                    &self.block_ranges,
                    &remote.block_counter_path,
                    remote.default_next_block_id,
                    BLOCK_RANGE_SIZE,
                )?);
            }
            if let Some(&last) = ids.last() {
                self.bump_local_counter_floor(counters::NEXT_BLOCK_ID, last.saturating_add(1))?;
            }
            self.on_mutation(32 * count as u64)?;
            Ok(ids)
        } else {
            let ids = self.local.next_block_ids(count)?;
            self.on_mutation(32 * count as u64)?;
            Ok(ids)
        }
    }

    fn flush_blocks_batch(
        &self,
        entries: &[crate::engine::BatchFlushEntry],
    ) -> MetaResult<Vec<crate::engine::BatchFlushResult>> {
        let r = self.local.flush_blocks_batch(entries)?;
        // Count mutation bytes: ~256 per entry for block refs + pending deletions.
        self.on_mutation(256 * entries.len() as u64)?;
        Ok(r)
    }

    fn add_pending_deletion(&self, object_id: u64, timestamp: i64) -> MetaResult<()> {
        let r = self.local.add_pending_deletion(object_id, timestamp);
        if r.is_ok() {
            self.on_mutation(128)?;
        }
        r
    }

    fn remove_pending_deletion(&self, object_id: u64) -> MetaResult<()> {
        let r = self.local.remove_pending_deletion(object_id);
        if r.is_ok() {
            self.on_mutation(128)?;
        }
        r
    }

    fn get_pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>> {
        self.read_with_refresh(|| self.local.get_pending_deletions())
    }

    fn get_referenced_object_ids(&self) -> MetaResult<HashSet<u64>> {
        self.read_with_refresh(|| self.local.get_referenced_object_ids())
    }

    fn create_file(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        let r = self.local.create_file(parent, name, uid, gid, mode);
        if r.is_ok() {
            self.on_mutation(512)?;
        }
        r
    }

    fn create_dir(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        let r = self.local.create_dir(parent, name, uid, gid, mode);
        if r.is_ok() {
            self.on_mutation(512)?;
        }
        r
    }

    fn unlink(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr> {
        let r = self.local.unlink(parent, name);
        if r.is_ok() {
            self.on_mutation(512)?;
        }
        r
    }

    fn rmdir(&self, parent: Ino, name: &[u8]) -> MetaResult<()> {
        let r = self.local.rmdir(parent, name);
        if r.is_ok() {
            self.on_mutation(256)?;
        }
        r
    }

    fn rename_atomic(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> MetaResult<Option<Ino>> {
        let r = self.local.rename_atomic(parent, name, new_parent, new_name);
        if r.is_ok() {
            self.on_mutation(512)?;
        }
        r
    }

    fn link_atomic(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
        let r = self.local.link_atomic(ino, new_parent, new_name);
        if r.is_ok() {
            self.on_mutation(256)?;
        }
        r
    }

    fn set_symlink_target(&self, ino: Ino, target: &[u8]) -> MetaResult<()> {
        let r = self.local.set_symlink_target(ino, target);
        if r.is_ok() {
            self.on_mutation(256)?;
        }
        r
    }

    fn get_symlink_target(&self, ino: Ino) -> MetaResult<Vec<u8>> {
        self.read_with_refresh(|| self.local.get_symlink_target(ino))
    }

    fn get_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>> {
        self.read_with_refresh(|| self.local.get_xattr(ino, name))
    }

    fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> MetaResult<()> {
        let r = self.local.set_xattr(ino, name, value, flags);
        if r.is_ok() {
            self.on_mutation(256)?;
        }
        r
    }

    fn remove_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<()> {
        let r = self.local.remove_xattr(ino, name);
        if r.is_ok() {
            self.on_mutation(256)?;
        }
        r
    }

    fn list_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>> {
        self.read_with_refresh(|| self.local.list_xattrs(ino))
    }

    fn create_symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        uid: u32,
        gid: u32,
    ) -> MetaResult<InodeAttr> {
        let r = self.local.create_symlink(parent, name, target, uid, gid);
        if r.is_ok() {
            self.on_mutation(512)?;
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FormatInfo, MetadataConfig, RemoteStoreConfig};
    use aws_config::BehaviorVersion;
    use aws_config::retry::RetryConfig;
    use aws_sdk_s3::Client as S3Client;
    use aws_sdk_s3::config::{Credentials, Region};
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};
    use std::collections::HashMap;
    use std::sync::OnceLock;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::time::{Duration, timeout};

    fn load_test_env() {
        static ONCE: OnceLock<()> = OnceLock::new();
        let _ = ONCE.get_or_init(|| {
            let _ = dotenvy::dotenv();
        });
    }

    #[test]
    fn test_merge_counters_preserves_parallel_deltas() {
        let base = RedbSnapshot {
            counters: vec![
                (crate::types::counters::TOTAL_INODES.to_string(), 1),
                (crate::types::counters::USED_SPACE.to_string(), 100),
                (crate::types::counters::NEXT_INODE.to_string(), 10),
                (crate::types::counters::NEXT_BLOCK_ID.to_string(), 50),
            ],
            ..RedbSnapshot::default()
        };
        let local = RedbSnapshot {
            counters: vec![
                (crate::types::counters::TOTAL_INODES.to_string(), 2), // +1
                (crate::types::counters::USED_SPACE.to_string(), 150), // +50
                (crate::types::counters::NEXT_INODE.to_string(), 18),
                (crate::types::counters::NEXT_BLOCK_ID.to_string(), 90),
            ],
            ..RedbSnapshot::default()
        };
        let remote = RedbSnapshot {
            counters: vec![
                (crate::types::counters::TOTAL_INODES.to_string(), 3),
                (crate::types::counters::USED_SPACE.to_string(), 80),
                (crate::types::counters::NEXT_INODE.to_string(), 12),
                (crate::types::counters::NEXT_BLOCK_ID.to_string(), 120),
            ],
            ..RedbSnapshot::default()
        };

        let merged = TonboMetaEngine::merge_snapshot_with_local_delta(&base, &local, &remote);
        let merged_map: HashMap<String, u64> = merged.counters.into_iter().collect();

        assert_eq!(
            merged_map.get(crate::types::counters::TOTAL_INODES),
            Some(&4),
            "remote(3) + local_delta(+1)"
        );
        assert_eq!(
            merged_map.get(crate::types::counters::USED_SPACE),
            Some(&130),
            "remote(80) + local_delta(+50)"
        );
        assert_eq!(
            merged_map.get(crate::types::counters::NEXT_INODE),
            Some(&18),
            "monotonic floor (max local/remote)"
        );
        assert_eq!(
            merged_map.get(crate::types::counters::NEXT_BLOCK_ID),
            Some(&120),
            "monotonic floor (max local/remote)"
        );
    }

    #[test]
    fn test_merge_table_delta_conflict_is_deterministic() {
        let base = vec![("k".to_string(), 10u64)];
        let local = vec![("k".to_string(), 20u64)];
        let remote = vec![("k".to_string(), 30u64)];
        let merged = TonboMetaEngine::merge_table_delta(&base, &local, &remote);
        let merged_map: HashMap<String, u64> = merged.into_iter().collect();
        assert_eq!(merged_map.get("k"), Some(&30u64));

        // delete vs update conflict keeps updated value (non-destructive)
        let base2 = vec![("k".to_string(), 10u64)];
        let local2: Vec<(String, u64)> = vec![];
        let remote2 = vec![("k".to_string(), 15u64)];
        let merged2 = TonboMetaEngine::merge_table_delta(&base2, &local2, &remote2);
        let merged2_map: HashMap<String, u64> = merged2.into_iter().collect();
        assert_eq!(merged2_map.get("k"), Some(&15u64));
    }

    #[test]
    fn test_merge_inodes_delta_prefers_newer_ctime() {
        let key = 42u64;
        let base_attr = InodeAttr {
            ino: key,
            kind: FileType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            size: 100,
            nlink: 1,
            mtime: 10,
            ctime: 10,
            atime: 10,
            parent: ROOT_INO,
            crtime: 0,
            win_attrs: 0,
        };
        let mut local_attr = base_attr.clone();
        local_attr.size = 120;
        local_attr.ctime = 20;
        let mut remote_attr = base_attr.clone();
        remote_attr.size = 200;
        remote_attr.ctime = 30;

        let base = vec![(key, postcard::to_stdvec(&base_attr).unwrap())];
        let local = vec![(key, postcard::to_stdvec(&local_attr).unwrap())];
        let remote = vec![(key, postcard::to_stdvec(&remote_attr).unwrap())];

        let merged = TonboMetaEngine::merge_inodes_delta(&base, &local, &remote);
        let merged_map: HashMap<u64, Vec<u8>> = merged.into_iter().collect();
        let chosen: InodeAttr = postcard::from_bytes(merged_map.get(&key).unwrap()).unwrap();
        assert_eq!(chosen.ctime, 30);
        assert_eq!(chosen.size, 200);
    }

    #[test]
    fn test_merge_file_blocks_delta_prefers_higher_object_id() {
        let key = vec![1u8; 12];
        let base_block = BlockRef::new_full(10, 4096);
        let local_block = BlockRef::new_full(20, 4096);
        let remote_block = BlockRef::new_full(30, 2048);

        let base = vec![(key.clone(), postcard::to_stdvec(&base_block).unwrap())];
        let local = vec![(key.clone(), postcard::to_stdvec(&local_block).unwrap())];
        let remote = vec![(key.clone(), postcard::to_stdvec(&remote_block).unwrap())];

        let merged = TonboMetaEngine::merge_file_blocks_delta(&base, &local, &remote);
        let merged_map: HashMap<Vec<u8>, Vec<u8>> = merged.into_iter().collect();
        let chosen: BlockRef = postcard::from_bytes(merged_map.get(&key).unwrap()).unwrap();
        assert_eq!(chosen.object_id, 30);
    }

    #[test]
    fn test_merge_dentry_delete_vs_update_keeps_update() {
        fn dentry_key(parent: u64, name: &[u8]) -> Vec<u8> {
            let mut key = Vec::with_capacity(8 + name.len());
            key.extend_from_slice(&parent.to_le_bytes());
            key.extend_from_slice(name);
            key
        }

        let key = dentry_key(ROOT_INO, b"a.txt");
        let base_entry = DirEntryValue {
            ino: 100,
            kind: FileType::RegularFile,
        };
        let remote_entry = DirEntryValue {
            ino: 101,
            kind: FileType::RegularFile,
        };

        let base = vec![(key.clone(), postcard::to_stdvec(&base_entry).unwrap())];
        let local: Vec<(Vec<u8>, Vec<u8>)> = vec![]; // unlink/delete
        let remote = vec![(key.clone(), postcard::to_stdvec(&remote_entry).unwrap())]; // rename/create

        let merged = TonboMetaEngine::merge_dentries_delta(&base, &local, &remote);
        let merged_map: HashMap<Vec<u8>, Vec<u8>> = merged.into_iter().collect();
        let chosen: DirEntryValue = postcard::from_bytes(merged_map.get(&key).unwrap()).unwrap();
        assert_eq!(chosen.ino, 101);
    }

    #[test]
    fn test_merge_rename_vs_unlink_prefers_rename_state() {
        fn dentry_key(parent: u64, name: &[u8]) -> Vec<u8> {
            let mut key = Vec::with_capacity(8 + name.len());
            key.extend_from_slice(&parent.to_le_bytes());
            key.extend_from_slice(name);
            key
        }

        let ino = 200u64;
        let key_a = dentry_key(ROOT_INO, b"a.txt");
        let key_b = dentry_key(ROOT_INO, b"b.txt");

        let mut base_attr = InodeAttr::new_file(ino, ROOT_INO, 0, 0, 0o644);
        base_attr.ctime = 10;
        base_attr.mtime = 10;
        let mut renamed_attr = base_attr.clone();
        renamed_attr.ctime = 20;
        renamed_attr.mtime = 20;

        let base_dentry = DirEntryValue {
            ino,
            kind: FileType::RegularFile,
        };
        let renamed_dentry = DirEntryValue {
            ino,
            kind: FileType::RegularFile,
        };

        let base = RedbSnapshot {
            inodes: vec![(ino, postcard::to_stdvec(&base_attr).unwrap())],
            dentries: vec![(key_a.clone(), postcard::to_stdvec(&base_dentry).unwrap())],
            ..RedbSnapshot::default()
        };

        // Local branch: rename a.txt -> b.txt
        let local = RedbSnapshot {
            inodes: vec![(ino, postcard::to_stdvec(&renamed_attr).unwrap())],
            dentries: vec![(key_b.clone(), postcard::to_stdvec(&renamed_dentry).unwrap())],
            ..RedbSnapshot::default()
        };

        // Remote branch: unlink a.txt + remove inode
        let remote = RedbSnapshot::default();

        let merged = TonboMetaEngine::merge_snapshot_with_local_delta(&base, &local, &remote);
        let inodes_map: HashMap<u64, Vec<u8>> = merged.inodes.into_iter().collect();
        let dentries_map: HashMap<Vec<u8>, Vec<u8>> = merged.dentries.into_iter().collect();

        assert!(
            dentries_map.get(&key_a).is_none(),
            "old source key must be gone"
        );
        assert!(
            dentries_map.get(&key_b).is_some(),
            "renamed target key must exist"
        );
        assert!(
            inodes_map.get(&ino).is_some(),
            "inode must survive when rename wins over conflicting unlink"
        );
    }

    #[test]
    fn test_merge_conflicting_renames_keeps_single_target_for_single_link_inode() {
        fn dentry_key(parent: u64, name: &[u8]) -> Vec<u8> {
            let mut key = Vec::with_capacity(8 + name.len());
            key.extend_from_slice(&parent.to_le_bytes());
            key.extend_from_slice(name);
            key
        }

        let ino = 300u64;
        let key_a = dentry_key(ROOT_INO, b"a.txt");
        let key_b = dentry_key(ROOT_INO, b"b.txt");
        let key_c = dentry_key(ROOT_INO, b"c.txt");

        let mut attr = InodeAttr::new_file(ino, ROOT_INO, 0, 0, 0o644);
        attr.nlink = 1;
        attr.ctime = 20;
        attr.mtime = 20;

        let dentry = DirEntryValue {
            ino,
            kind: FileType::RegularFile,
        };

        let base = RedbSnapshot {
            inodes: vec![(ino, postcard::to_stdvec(&attr).unwrap())],
            dentries: vec![(key_a.clone(), postcard::to_stdvec(&dentry).unwrap())],
            ..RedbSnapshot::default()
        };
        let local = RedbSnapshot {
            inodes: vec![(ino, postcard::to_stdvec(&attr).unwrap())],
            dentries: vec![(key_b.clone(), postcard::to_stdvec(&dentry).unwrap())],
            ..RedbSnapshot::default()
        };
        let remote = RedbSnapshot {
            inodes: vec![(ino, postcard::to_stdvec(&attr).unwrap())],
            dentries: vec![(key_c.clone(), postcard::to_stdvec(&dentry).unwrap())],
            ..RedbSnapshot::default()
        };

        let merged = TonboMetaEngine::merge_snapshot_with_local_delta(&base, &local, &remote);
        let dentries_map: HashMap<Vec<u8>, Vec<u8>> = merged.dentries.into_iter().collect();

        assert!(
            dentries_map.get(&key_a).is_none(),
            "source key must be removed after rename conflict merge"
        );
        let b_ok = dentries_map.get(&key_b).is_some();
        let c_ok = dentries_map.get(&key_c).is_some();
        assert!(b_ok ^ c_ok, "exactly one conflicting rename target must survive");
    }

    async fn build_minio_client(endpoint: &str, access_key: &str, secret_key: &str) -> S3Client {
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .credentials_provider(Credentials::new(
                access_key,
                secret_key,
                None,
                None,
                "studiofs-test",
            ))
            .region(Region::new("us-east-1"))
            .retry_config(RetryConfig::standard().with_max_attempts(1))
            .load()
            .await;
        let conf = aws_sdk_s3::config::Builder::from(&shared)
            .endpoint_url(endpoint)
            .force_path_style(true)
            .build();
        S3Client::from_conf(conf)
    }

    async fn ensure_bucket(client: &S3Client, bucket: &str) -> anyhow::Result<bool> {
        match timeout(
            Duration::from_secs(4),
            client.head_bucket().bucket(bucket).send(),
        )
        .await
        {
            Err(_) => anyhow::bail!("timeout while checking bucket '{bucket}'"),
            Ok(Ok(_)) => Ok(false),
            Ok(Err(_)) => {
                timeout(
                    Duration::from_secs(4),
                    client.create_bucket().bucket(bucket).send(),
                )
                .await
                .map_err(|_| anyhow::anyhow!("timeout while creating bucket '{bucket}'"))??;
                Ok(true)
            }
        }
    }

    async fn delete_bucket_with_contents(client: &S3Client, bucket: &str) -> anyhow::Result<()> {
        let mut token: Option<String> = None;
        loop {
            let mut req = client.list_objects_v2().bucket(bucket);
            if let Some(t) = &token {
                req = req.continuation_token(t);
            }
            let resp = timeout(Duration::from_secs(4), req.send())
                .await
                .map_err(|_| anyhow::anyhow!("timeout while listing bucket '{bucket}'"))??;
            let mut objs = Vec::new();
            if let Some(contents) = resp.contents {
                for item in contents {
                    if let Some(k) = item.key {
                        objs.push(ObjectIdentifier::builder().key(k).build()?);
                    }
                }
            }
            if !objs.is_empty() {
                timeout(
                    Duration::from_secs(4),
                    client
                        .delete_objects()
                        .bucket(bucket)
                        .delete(Delete::builder().set_objects(Some(objs)).build()?)
                        .send(),
                )
                .await
                .map_err(|_| {
                    anyhow::anyhow!("timeout while deleting objects in bucket '{bucket}'")
                })??;
            }
            if resp.is_truncated.unwrap_or(false) {
                token = resp.next_continuation_token;
            } else {
                break;
            }
        }
        timeout(
            Duration::from_secs(4),
            client.delete_bucket().bucket(bucket).send(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("timeout while deleting bucket '{bucket}'"))??;
        Ok(())
    }

    fn is_transient_storage_err(err: &MetaError) -> bool {
        let msg = err.to_string().to_lowercase();
        msg.contains("timeout while")
            || msg.contains("deadline has elapsed")
            || msg.contains("connection")
    }

    fn cleanup_bucket_best_effort(rt: &tokio::runtime::Runtime, client: &S3Client, bucket: &str) {
        if let Err(err) = rt.block_on(delete_bucket_with_contents(client, bucket)) {
            debug!(
                thread = std::thread::current()
                    .name()
                    .unwrap_or("tonbo_minio_test_cleanup"),
                bucket,
                error = %err,
                "cleanup bucket failed (best-effort)"
            );
        }
    }

    #[test]
    #[ignore]
    fn test_tonbo_minio_roundtrip() {
        load_test_env();
        use std::sync::mpsc;
        use std::sync::mpsc::RecvTimeoutError;

        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = (|| -> anyhow::Result<()> {
                let endpoint = std::env::var("STUDIOFS_MINIO_ENDPOINT")
                    .unwrap_or_else(|_| "http://127.0.0.1:9000".into());
                let access_key = std::env::var("STUDIOFS_MINIO_ACCESS_KEY")
                    .unwrap_or_else(|_| "minioadmin".into());
                let secret_key = std::env::var("STUDIOFS_MINIO_SECRET_KEY")
                    .unwrap_or_else(|_| "minioadmin".into());
                let default_bucket = format!(
                    "studiofs-meta-test-{}-{}",
                    std::process::id(),
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                );
                let bucket = std::env::var("STUDIOFS_MINIO_BUCKET").unwrap_or(default_bucket);

                let rt = Builder::new_current_thread().enable_all().build().unwrap();
                let client = rt.block_on(build_minio_client(&endpoint, &access_key, &secret_key));
                let created_bucket = match rt.block_on(ensure_bucket(&client, &bucket)) {
                    Ok(v) => v,
                    Err(err) => {
                        eprintln!("skip tonbo_minio_roundtrip: {}", err);
                        return Ok(());
                    }
                };

                let tag = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0);
                let disk_name = format!("tonbo-test-{}-{tag}", std::process::id());
                let uuid = format!("{}-uuid", disk_name);
                let dir1 = tempfile::tempdir().unwrap();
                let db1 = dir1.path().join("meta1.redb");

                let local1 = RedbMetaEngine::open(&db1).unwrap();
                local1.init_root(0, 0).unwrap();
                let format_info = FormatInfo {
                    name: disk_name.clone(),
                    uuid: uuid.clone(),
                    endpoint: endpoint.clone(),
                    bucket: bucket.clone(),
                    access_key: access_key.clone(),
                    secret_key: secret_key.clone(),
                    region: "us-east-1".into(),
                    data_prefix: format!("{disk_name}/objects/"),
                    metadata: MetadataConfig {
                        backend: MetadataBackend::Tonbo,
                        remote: Some(RemoteStoreConfig {
                            endpoint: endpoint.clone(),
                            bucket: bucket.clone(),
                            access_key: access_key.clone(),
                            secret_key: secret_key.clone(),
                            region: "us-east-1".into(),
                            prefix: format!("{disk_name}/metadata/"),
                        }),
                        ..MetadataConfig::default()
                    },
                    version: String::new(),
                    block_size: DEFAULT_BLOCK_SIZE,
                    chunk_size: 0,
                };
                local1.set_format_info(&format_info).unwrap();
                drop(local1);
                let tonbo1 = TonboMetaEngine::open(&db1).unwrap();
                tonbo1
                    .create_file(ROOT_INO, b"hello.txt", 0, 0, 0o644)
                    .unwrap();
                tonbo1.flush_now().unwrap();
                drop(tonbo1);

                let dir2 = tempfile::tempdir().unwrap();
                let db2 = dir2.path().join("meta2.redb");
                let local2 = RedbMetaEngine::open(&db2).unwrap();
                local2.set_format_info(&format_info).unwrap();
                drop(local2);
                let tonbo2 = TonboMetaEngine::open(&db2).unwrap();
                let entry = tonbo2.lookup(ROOT_INO, b"hello.txt").unwrap();
                assert!(entry.ino > ROOT_INO);
                drop(tonbo2);

                if created_bucket {
                    cleanup_bucket_best_effort(&rt, &client, &bucket);
                }

                Ok(())
            })();
            let _ = tx.send(result.map_err(|e| e.to_string()));
        });

        match rx.recv_timeout(Duration::from_secs(90)) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => panic!("{}", err),
            Err(RecvTimeoutError::Timeout) => {
                panic!("tonbo_minio_roundtrip timed out after 90 seconds")
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("tonbo_minio_roundtrip worker thread disconnected")
            }
        }
    }

    #[test]
    #[ignore]
    fn test_tonbo_minio_stale_client_merge_same_dir() {
        load_test_env();
        use std::sync::mpsc::{self, RecvTimeoutError};

        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = (|| -> anyhow::Result<()> {
                let endpoint = std::env::var("STUDIOFS_MINIO_ENDPOINT")
                    .unwrap_or_else(|_| "http://127.0.0.1:9000".into());
                let access_key = std::env::var("STUDIOFS_MINIO_ACCESS_KEY")
                    .unwrap_or_else(|_| "minioadmin".into());
                let secret_key = std::env::var("STUDIOFS_MINIO_SECRET_KEY")
                    .unwrap_or_else(|_| "minioadmin".into());
                let default_bucket = format!(
                    "studiofs-meta-test-{}-{}",
                    std::process::id(),
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                );
                let bucket = std::env::var("STUDIOFS_MINIO_BUCKET").unwrap_or(default_bucket);

                let rt = Builder::new_current_thread().enable_all().build().unwrap();
                let client = rt.block_on(build_minio_client(&endpoint, &access_key, &secret_key));
                let created_bucket = match rt.block_on(ensure_bucket(&client, &bucket)) {
                    Ok(v) => v,
                    Err(err) => {
                        eprintln!("skip tonbo_minio_stale_client_merge_same_dir: {}", err);
                        return Ok(());
                    }
                };

                let uuid = format!("tonbo-stale-same-dir-{}", std::process::id());
                let disk_name = format!("tonbo-stale-same-dir-{}", std::process::id());
                let format_info = FormatInfo {
                    name: disk_name.clone(),
                    uuid,
                    endpoint: endpoint.clone(),
                    bucket: bucket.clone(),
                    access_key: access_key.clone(),
                    secret_key: secret_key.clone(),
                    region: "us-east-1".into(),
                    data_prefix: format!("{disk_name}/objects/"),
                    metadata: MetadataConfig {
                        backend: MetadataBackend::Tonbo,
                        remote: Some(RemoteStoreConfig {
                            endpoint: endpoint.clone(),
                            bucket: bucket.clone(),
                            access_key: access_key.clone(),
                            secret_key: secret_key.clone(),
                            region: "us-east-1".into(),
                            prefix: format!("{disk_name}/metadata/"),
                        }),
                        ..MetadataConfig::default()
                    },
                    version: String::new(),
                    block_size: DEFAULT_BLOCK_SIZE,
                    chunk_size: 0,
                };

                // Seed remote manifest.
                let seed_dir = tempfile::tempdir().unwrap();
                let seed_db = seed_dir.path().join("seed.redb");
                let seed_local = RedbMetaEngine::open(&seed_db).unwrap();
                seed_local.init_root(0, 0).unwrap();
                seed_local.set_format_info(&format_info).unwrap();
                drop(seed_local);
                let seed = TonboMetaEngine::open(&seed_db).unwrap();
                seed.flush_now().unwrap();
                drop(seed);

                // Two clients start from same baseline.
                let dir1 = tempfile::tempdir().unwrap();
                let db1 = dir1.path().join("meta1.redb");
                let local1 = RedbMetaEngine::open(&db1).unwrap();
                local1.init_root(0, 0).unwrap();
                local1.set_format_info(&format_info).unwrap();
                drop(local1);
                let tonbo1 = TonboMetaEngine::open(&db1).unwrap();

                let dir2 = tempfile::tempdir().unwrap();
                let db2 = dir2.path().join("meta2.redb");
                let local2 = RedbMetaEngine::open(&db2).unwrap();
                local2.init_root(0, 0).unwrap();
                local2.set_format_info(&format_info).unwrap();
                drop(local2);
                let tonbo2 = TonboMetaEngine::open(&db2).unwrap();

                // Client 1 commits first.
                tonbo1.create_file(ROOT_INO, b"a.txt", 0, 0, 0o644).unwrap();
                tonbo1.flush_now().unwrap();

                // Client 2 is stale and must merge on CAS conflict.
                tonbo2.create_file(ROOT_INO, b"b.txt", 0, 0, 0o644).unwrap();
                tonbo2.flush_now().unwrap();

                drop(tonbo1);
                drop(tonbo2);

                // Fresh reader must see both files.
                let verify_dir = tempfile::tempdir().unwrap();
                let verify_db = verify_dir.path().join("meta3.redb");
                let verify_local = RedbMetaEngine::open(&verify_db).unwrap();
                verify_local.set_format_info(&format_info).unwrap();
                drop(verify_local);
                let verify = TonboMetaEngine::open(&verify_db).unwrap();
                let a = verify.lookup(ROOT_INO, b"a.txt").unwrap();
                let b = verify.lookup(ROOT_INO, b"b.txt").unwrap();
                assert!(a.ino > ROOT_INO);
                assert!(b.ino > ROOT_INO);
                assert_eq!(
                    verify
                        .get_counter(crate::types::counters::TOTAL_INODES)
                        .unwrap(),
                    3
                );
                drop(verify);

                if created_bucket {
                    cleanup_bucket_best_effort(&rt, &client, &bucket);
                }

                Ok(())
            })();
            let _ = tx.send(result.map_err(|e| e.to_string()));
        });

        match rx.recv_timeout(Duration::from_secs(90)) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => panic!("{}", err),
            Err(RecvTimeoutError::Timeout) => {
                panic!("tonbo_minio_stale_client_merge_same_dir timed out after 90 seconds")
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("tonbo_minio_stale_client_merge_same_dir worker thread disconnected")
            }
        }
    }

    #[test]
    #[ignore]
    fn test_tonbo_minio_stale_client_merge_different_dirs() {
        load_test_env();
        use std::sync::mpsc::{self, RecvTimeoutError};

        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = (|| -> anyhow::Result<()> {
                let endpoint = std::env::var("STUDIOFS_MINIO_ENDPOINT")
                    .unwrap_or_else(|_| "http://127.0.0.1:9000".into());
                let access_key = std::env::var("STUDIOFS_MINIO_ACCESS_KEY")
                    .unwrap_or_else(|_| "minioadmin".into());
                let secret_key = std::env::var("STUDIOFS_MINIO_SECRET_KEY")
                    .unwrap_or_else(|_| "minioadmin".into());
                let default_bucket = format!(
                    "studiofs-meta-test-{}-{}",
                    std::process::id(),
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                );
                let bucket = std::env::var("STUDIOFS_MINIO_BUCKET").unwrap_or(default_bucket);

                let rt = Builder::new_current_thread().enable_all().build().unwrap();
                let client = rt.block_on(build_minio_client(&endpoint, &access_key, &secret_key));
                let created_bucket = match rt.block_on(ensure_bucket(&client, &bucket)) {
                    Ok(v) => v,
                    Err(err) => {
                        eprintln!(
                            "skip tonbo_minio_stale_client_merge_different_dirs: {}",
                            err
                        );
                        return Ok(());
                    }
                };

                let uuid = format!("tonbo-stale-diff-dirs-{}", std::process::id());
                let disk_name = format!("tonbo-stale-diff-dirs-{}", std::process::id());
                let format_info = FormatInfo {
                    name: disk_name.clone(),
                    uuid,
                    endpoint: endpoint.clone(),
                    bucket: bucket.clone(),
                    access_key: access_key.clone(),
                    secret_key: secret_key.clone(),
                    region: "us-east-1".into(),
                    data_prefix: format!("{disk_name}/objects/"),
                    metadata: MetadataConfig {
                        backend: MetadataBackend::Tonbo,
                        remote: Some(RemoteStoreConfig {
                            endpoint: endpoint.clone(),
                            bucket: bucket.clone(),
                            access_key: access_key.clone(),
                            secret_key: secret_key.clone(),
                            region: "us-east-1".into(),
                            prefix: format!("{disk_name}/metadata/"),
                        }),
                        ..MetadataConfig::default()
                    },
                    version: String::new(),
                    block_size: DEFAULT_BLOCK_SIZE,
                    chunk_size: 0,
                };

                let seed_dir = tempfile::tempdir().unwrap();
                let seed_db = seed_dir.path().join("seed.redb");
                let seed_local = RedbMetaEngine::open(&seed_db).unwrap();
                seed_local.init_root(0, 0).unwrap();
                seed_local.set_format_info(&format_info).unwrap();
                drop(seed_local);
                let seed = TonboMetaEngine::open(&seed_db).unwrap();
                seed.flush_now().unwrap();
                drop(seed);

                let dir1 = tempfile::tempdir().unwrap();
                let db1 = dir1.path().join("meta1.redb");
                let local1 = RedbMetaEngine::open(&db1).unwrap();
                local1.init_root(0, 0).unwrap();
                local1.set_format_info(&format_info).unwrap();
                drop(local1);
                let tonbo1 = TonboMetaEngine::open(&db1).unwrap();

                let dir2 = tempfile::tempdir().unwrap();
                let db2 = dir2.path().join("meta2.redb");
                let local2 = RedbMetaEngine::open(&db2).unwrap();
                local2.init_root(0, 0).unwrap();
                local2.set_format_info(&format_info).unwrap();
                drop(local2);
                let tonbo2 = TonboMetaEngine::open(&db2).unwrap();

                let dir_a = tonbo1.create_dir(ROOT_INO, b"dir-a", 0, 0, 0o755).unwrap();
                tonbo1
                    .create_file(dir_a.ino, b"file-a.txt", 0, 0, 0o644)
                    .unwrap();
                tonbo1.flush_now().unwrap();

                let dir_b = tonbo2.create_dir(ROOT_INO, b"dir-b", 0, 0, 0o755).unwrap();
                tonbo2
                    .create_file(dir_b.ino, b"file-b.txt", 0, 0, 0o644)
                    .unwrap();
                tonbo2.flush_now().unwrap();

                drop(tonbo1);
                drop(tonbo2);

                let verify_dir = tempfile::tempdir().unwrap();
                let verify_db = verify_dir.path().join("meta3.redb");
                let verify_local = RedbMetaEngine::open(&verify_db).unwrap();
                verify_local.set_format_info(&format_info).unwrap();
                drop(verify_local);
                let verify = TonboMetaEngine::open(&verify_db).unwrap();
                let dir_a_e = verify.lookup(ROOT_INO, b"dir-a").unwrap();
                let dir_b_e = verify.lookup(ROOT_INO, b"dir-b").unwrap();
                verify.lookup(dir_a_e.ino, b"file-a.txt").unwrap();
                verify.lookup(dir_b_e.ino, b"file-b.txt").unwrap();
                assert_eq!(
                    verify
                        .get_counter(crate::types::counters::TOTAL_INODES)
                        .unwrap(),
                    5
                );
                drop(verify);

                if created_bucket {
                    cleanup_bucket_best_effort(&rt, &client, &bucket);
                }

                Ok(())
            })();
            let _ = tx.send(result.map_err(|e| e.to_string()));
        });

        match rx.recv_timeout(Duration::from_secs(90)) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => panic!("{}", err),
            Err(RecvTimeoutError::Timeout) => {
                panic!("tonbo_minio_stale_client_merge_different_dirs timed out after 90 seconds")
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("tonbo_minio_stale_client_merge_different_dirs worker thread disconnected")
            }
        }
    }

    #[test]
    #[ignore]
    fn test_tonbo_minio_e2e_rename_then_stale_unlink() {
        load_test_env();
        use std::sync::mpsc::{self, RecvTimeoutError};

        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = (|| -> anyhow::Result<()> {
                let endpoint = std::env::var("STUDIOFS_MINIO_ENDPOINT")
                    .unwrap_or_else(|_| "http://127.0.0.1:9000".into());
                let access_key = std::env::var("STUDIOFS_MINIO_ACCESS_KEY")
                    .unwrap_or_else(|_| "minioadmin".into());
                let secret_key = std::env::var("STUDIOFS_MINIO_SECRET_KEY")
                    .unwrap_or_else(|_| "minioadmin".into());
                let default_bucket = format!(
                    "studiofs-meta-test-{}-{}",
                    std::process::id(),
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                );
                let bucket = std::env::var("STUDIOFS_MINIO_BUCKET").unwrap_or(default_bucket);

                let rt = Builder::new_current_thread().enable_all().build().unwrap();
                let client = rt.block_on(build_minio_client(&endpoint, &access_key, &secret_key));
                let created_bucket = match rt.block_on(ensure_bucket(&client, &bucket)) {
                    Ok(v) => v,
                    Err(err) => {
                        eprintln!("skip tonbo_minio_e2e_rename_then_stale_unlink: {}", err);
                        return Ok(());
                    }
                };

                let disk_name = format!("tonbo-e2e-rn-ul-{}", std::process::id());
                let format_info = FormatInfo {
                    name: disk_name.clone(),
                    uuid: format!("{}-uuid", disk_name),
                    endpoint: endpoint.clone(),
                    bucket: bucket.clone(),
                    access_key: access_key.clone(),
                    secret_key: secret_key.clone(),
                    region: "us-east-1".into(),
                    data_prefix: format!("{disk_name}/objects/"),
                    metadata: MetadataConfig {
                        backend: MetadataBackend::Tonbo,
                        remote: Some(RemoteStoreConfig {
                            endpoint: endpoint.clone(),
                            bucket: bucket.clone(),
                            access_key: access_key.clone(),
                            secret_key: secret_key.clone(),
                            region: "us-east-1".into(),
                            prefix: format!("{disk_name}/metadata/"),
                        }),
                        ..MetadataConfig::default()
                    },
                    version: String::new(),
                    block_size: DEFAULT_BLOCK_SIZE,
                    chunk_size: 0,
                };

                // Seed with a.txt present in remote baseline.
                let seed_dir = tempfile::tempdir().unwrap();
                let seed_db = seed_dir.path().join("seed.redb");
                let seed_local = RedbMetaEngine::open(&seed_db).unwrap();
                seed_local.init_root(0, 0).unwrap();
                seed_local.set_format_info(&format_info).unwrap();
                drop(seed_local);
                let seed = TonboMetaEngine::open(&seed_db).unwrap();
                seed.create_file(ROOT_INO, b"a.txt", 0, 0, 0o644).unwrap();
                if let Err(err) = seed.flush_now() {
                    if is_transient_storage_err(&err) {
                        eprintln!("skip tonbo_minio_e2e_rename_then_stale_unlink: {}", err);
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(err.to_string()));
                }
                drop(seed);

                // Two stale clients from same baseline.
                let dir1 = tempfile::tempdir().unwrap();
                let db1 = dir1.path().join("meta1.redb");
                let local1 = RedbMetaEngine::open(&db1).unwrap();
                local1.init_root(0, 0).unwrap();
                local1.set_format_info(&format_info).unwrap();
                drop(local1);
                let c1 = TonboMetaEngine::open(&db1).unwrap();

                let dir2 = tempfile::tempdir().unwrap();
                let db2 = dir2.path().join("meta2.redb");
                let local2 = RedbMetaEngine::open(&db2).unwrap();
                local2.init_root(0, 0).unwrap();
                local2.set_format_info(&format_info).unwrap();
                drop(local2);
                let c2 = TonboMetaEngine::open(&db2).unwrap();
                // Ensure client2 has stale baseline with a.txt cached before client1 rename.
                if let Err(err) = c2.lookup(ROOT_INO, b"a.txt") {
                    if is_transient_storage_err(&err) {
                        eprintln!("skip tonbo_minio_e2e_rename_then_stale_unlink: {}", err);
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(err.to_string()));
                }

                // Client1 rename, client2 stale unlink old name.
                c1.rename_atomic(ROOT_INO, b"a.txt", ROOT_INO, b"b.txt")
                    .unwrap();
                if let Err(err) = c1.flush_now() {
                    if is_transient_storage_err(&err) {
                        eprintln!("skip tonbo_minio_e2e_rename_then_stale_unlink: {}", err);
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(err.to_string()));
                }
                c2.unlink(ROOT_INO, b"a.txt").unwrap();
                if let Err(err) = c2.flush_now() {
                    if is_transient_storage_err(&err) {
                        eprintln!("skip tonbo_minio_e2e_rename_then_stale_unlink: {}", err);
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(err.to_string()));
                }

                drop(c1);
                drop(c2);

                // Fresh reader should converge: b.txt exists, a.txt absent.
                let verify_dir = tempfile::tempdir().unwrap();
                let verify_db = verify_dir.path().join("meta3.redb");
                let verify_local = RedbMetaEngine::open(&verify_db).unwrap();
                verify_local.init_root(0, 0).unwrap();
                verify_local.set_format_info(&format_info).unwrap();
                drop(verify_local);
                let verify = TonboMetaEngine::open(&verify_db).unwrap();
                verify.lookup(ROOT_INO, b"b.txt").unwrap();
                assert!(verify.lookup(ROOT_INO, b"a.txt").is_err());
                drop(verify);

                if created_bucket {
                    cleanup_bucket_best_effort(&rt, &client, &bucket);
                }
                Ok(())
            })();
            let _ = tx.send(result.map_err(|e| e.to_string()));
        });

        match rx.recv_timeout(Duration::from_secs(90)) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => panic!("{}", err),
            Err(RecvTimeoutError::Timeout) => {
                panic!("tonbo_minio_e2e_rename_then_stale_unlink timed out after 90 seconds")
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("tonbo_minio_e2e_rename_then_stale_unlink worker thread disconnected")
            }
        }
    }

    #[test]
    #[ignore]
    fn test_tonbo_minio_e2e_stale_parallel_write_same_block() {
        load_test_env();
        use std::sync::mpsc::{self, RecvTimeoutError};

        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = (|| -> anyhow::Result<()> {
                let endpoint = std::env::var("STUDIOFS_MINIO_ENDPOINT")
                    .unwrap_or_else(|_| "http://127.0.0.1:9000".into());
                let access_key = std::env::var("STUDIOFS_MINIO_ACCESS_KEY")
                    .unwrap_or_else(|_| "minioadmin".into());
                let secret_key = std::env::var("STUDIOFS_MINIO_SECRET_KEY")
                    .unwrap_or_else(|_| "minioadmin".into());
                let default_bucket = format!(
                    "studiofs-meta-test-{}-{}",
                    std::process::id(),
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                );
                let bucket = std::env::var("STUDIOFS_MINIO_BUCKET").unwrap_or(default_bucket);

                let rt = Builder::new_current_thread().enable_all().build().unwrap();
                let client = rt.block_on(build_minio_client(&endpoint, &access_key, &secret_key));
                let created_bucket = match rt.block_on(ensure_bucket(&client, &bucket)) {
                    Ok(v) => v,
                    Err(err) => {
                        eprintln!(
                            "skip tonbo_minio_e2e_stale_parallel_write_same_block: {}",
                            err
                        );
                        return Ok(());
                    }
                };

                let disk_name = format!("tonbo-e2e-wr-{}", std::process::id());
                let format_info = FormatInfo {
                    name: disk_name.clone(),
                    uuid: format!("{}-uuid", disk_name),
                    endpoint: endpoint.clone(),
                    bucket: bucket.clone(),
                    access_key: access_key.clone(),
                    secret_key: secret_key.clone(),
                    region: "us-east-1".into(),
                    data_prefix: format!("{disk_name}/objects/"),
                    metadata: MetadataConfig {
                        backend: MetadataBackend::Tonbo,
                        remote: Some(RemoteStoreConfig {
                            endpoint: endpoint.clone(),
                            bucket: bucket.clone(),
                            access_key: access_key.clone(),
                            secret_key: secret_key.clone(),
                            region: "us-east-1".into(),
                            prefix: format!("{disk_name}/metadata/"),
                        }),
                        ..MetadataConfig::default()
                    },
                    version: String::new(),
                    block_size: DEFAULT_BLOCK_SIZE,
                    chunk_size: 0,
                };

                // Seed with file.
                let seed_dir = tempfile::tempdir().unwrap();
                let seed_db = seed_dir.path().join("seed.redb");
                let seed_local = RedbMetaEngine::open(&seed_db).unwrap();
                seed_local.init_root(0, 0).unwrap();
                seed_local.set_format_info(&format_info).unwrap();
                drop(seed_local);
                let seed = TonboMetaEngine::open(&seed_db).unwrap();
                let file = seed.create_file(ROOT_INO, b"f.txt", 0, 0, 0o644).unwrap();
                if let Err(err) = seed.flush_now() {
                    if is_transient_storage_err(&err) {
                        eprintln!(
                            "skip tonbo_minio_e2e_stale_parallel_write_same_block: {}",
                            err
                        );
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(err.to_string()));
                }
                drop(seed);

                let dir1 = tempfile::tempdir().unwrap();
                let db1 = dir1.path().join("meta1.redb");
                let local1 = RedbMetaEngine::open(&db1).unwrap();
                local1.init_root(0, 0).unwrap();
                local1.set_format_info(&format_info).unwrap();
                drop(local1);
                let c1 = TonboMetaEngine::open(&db1).unwrap();

                let dir2 = tempfile::tempdir().unwrap();
                let db2 = dir2.path().join("meta2.redb");
                let local2 = RedbMetaEngine::open(&db2).unwrap();
                local2.init_root(0, 0).unwrap();
                local2.set_format_info(&format_info).unwrap();
                drop(local2);
                let c2 = TonboMetaEngine::open(&db2).unwrap();

                // Each client writes same block index with different object id.
                let id1 = match c1.next_block_id() {
                    Ok(v) => v,
                    Err(err) => {
                        if is_transient_storage_err(&err) {
                            eprintln!(
                                "skip tonbo_minio_e2e_stale_parallel_write_same_block: {}",
                                err
                            );
                            return Ok(());
                        }
                        return Err(anyhow::anyhow!(err.to_string()));
                    }
                };
                let blk1 = BlockRef::new_full(id1, 4096);
                c1.set_file_block(file.ino, 0, &blk1).unwrap();
                if let Err(err) = c1.flush_now() {
                    if is_transient_storage_err(&err) {
                        eprintln!(
                            "skip tonbo_minio_e2e_stale_parallel_write_same_block: {}",
                            err
                        );
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(err.to_string()));
                }

                let id2 = match c2.next_block_id() {
                    Ok(v) => v,
                    Err(err) => {
                        if is_transient_storage_err(&err) {
                            eprintln!(
                                "skip tonbo_minio_e2e_stale_parallel_write_same_block: {}",
                                err
                            );
                            return Ok(());
                        }
                        return Err(anyhow::anyhow!(err.to_string()));
                    }
                };
                let blk2 = BlockRef::new_full(id2, 4096);
                c2.set_file_block(file.ino, 0, &blk2).unwrap();
                if let Err(err) = c2.flush_now() {
                    if is_transient_storage_err(&err) {
                        eprintln!(
                            "skip tonbo_minio_e2e_stale_parallel_write_same_block: {}",
                            err
                        );
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(err.to_string()));
                }

                drop(c1);
                drop(c2);

                let verify_dir = tempfile::tempdir().unwrap();
                let verify_db = verify_dir.path().join("meta3.redb");
                let verify_local = RedbMetaEngine::open(&verify_db).unwrap();
                verify_local.init_root(0, 0).unwrap();
                verify_local.set_format_info(&format_info).unwrap();
                drop(verify_local);
                let verify = TonboMetaEngine::open(&verify_db).unwrap();
                let final_block = match verify.get_file_block(file.ino, 0) {
                    Ok(Some(v)) => v,
                    Ok(None) => {
                        return Err(anyhow::anyhow!(
                            "expected final block mapping, got None for inode {}",
                            file.ino
                        ));
                    }
                    Err(err) => {
                        if is_transient_storage_err(&err) {
                            eprintln!(
                                "skip tonbo_minio_e2e_stale_parallel_write_same_block: {}",
                                err
                            );
                            return Ok(());
                        }
                        return Err(anyhow::anyhow!(err.to_string()));
                    }
                };
                assert_eq!(final_block.object_id, id1.max(id2));
                drop(verify);

                if created_bucket {
                    cleanup_bucket_best_effort(&rt, &client, &bucket);
                }
                Ok(())
            })();
            let _ = tx.send(result.map_err(|e| e.to_string()));
        });

        match rx.recv_timeout(Duration::from_secs(90)) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => panic!("{}", err),
            Err(RecvTimeoutError::Timeout) => {
                panic!("tonbo_minio_e2e_stale_parallel_write_same_block timed out after 90 seconds")
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("tonbo_minio_e2e_stale_parallel_write_same_block worker thread disconnected")
            }
        }
    }

    #[test]
    #[ignore]
    fn test_tonbo_minio_e2e_stale_rename_vs_rename() {
        load_test_env();
        use std::sync::mpsc::{self, RecvTimeoutError};

        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = (|| -> anyhow::Result<()> {
                let endpoint = std::env::var("STUDIOFS_MINIO_ENDPOINT")
                    .unwrap_or_else(|_| "http://127.0.0.1:9000".into());
                let access_key = std::env::var("STUDIOFS_MINIO_ACCESS_KEY")
                    .unwrap_or_else(|_| "minioadmin".into());
                let secret_key = std::env::var("STUDIOFS_MINIO_SECRET_KEY")
                    .unwrap_or_else(|_| "minioadmin".into());
                let default_bucket = format!(
                    "studiofs-meta-test-{}-{}",
                    std::process::id(),
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                );
                let bucket = std::env::var("STUDIOFS_MINIO_BUCKET").unwrap_or(default_bucket);

                let rt = Builder::new_current_thread().enable_all().build().unwrap();
                let client = rt.block_on(build_minio_client(&endpoint, &access_key, &secret_key));
                let created_bucket = match rt.block_on(ensure_bucket(&client, &bucket)) {
                    Ok(v) => v,
                    Err(err) => {
                        eprintln!("skip tonbo_minio_e2e_stale_rename_vs_rename: {}", err);
                        return Ok(());
                    }
                };

                let disk_name = format!("tonbo-e2e-rn-rn-{}", std::process::id());
                let format_info = FormatInfo {
                    name: disk_name.clone(),
                    uuid: format!("{}-uuid", disk_name),
                    endpoint: endpoint.clone(),
                    bucket: bucket.clone(),
                    access_key: access_key.clone(),
                    secret_key: secret_key.clone(),
                    region: "us-east-1".into(),
                    data_prefix: format!("{disk_name}/objects/"),
                    metadata: MetadataConfig {
                        backend: MetadataBackend::Tonbo,
                        remote: Some(RemoteStoreConfig {
                            endpoint: endpoint.clone(),
                            bucket: bucket.clone(),
                            access_key: access_key.clone(),
                            secret_key: secret_key.clone(),
                            region: "us-east-1".into(),
                            prefix: format!("{disk_name}/metadata/"),
                        }),
                        ..MetadataConfig::default()
                    },
                    version: String::new(),
                    block_size: DEFAULT_BLOCK_SIZE,
                    chunk_size: 0,
                };

                // Seed with a single source entry: a.txt
                let seed_dir = tempfile::tempdir().unwrap();
                let seed_db = seed_dir.path().join("seed.redb");
                let seed_local = RedbMetaEngine::open(&seed_db).unwrap();
                seed_local.init_root(0, 0).unwrap();
                seed_local.set_format_info(&format_info).unwrap();
                drop(seed_local);
                let seed = TonboMetaEngine::open(&seed_db).unwrap();
                seed.create_file(ROOT_INO, b"a.txt", 0, 0, 0o644).unwrap();
                if let Err(err) = seed.flush_now() {
                    if is_transient_storage_err(&err) {
                        eprintln!("skip tonbo_minio_e2e_stale_rename_vs_rename: {}", err);
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(err.to_string()));
                }
                drop(seed);

                // Two stale clients from same baseline.
                let dir1 = tempfile::tempdir().unwrap();
                let db1 = dir1.path().join("meta1.redb");
                let local1 = RedbMetaEngine::open(&db1).unwrap();
                local1.init_root(0, 0).unwrap();
                local1.set_format_info(&format_info).unwrap();
                drop(local1);
                let c1 = TonboMetaEngine::open(&db1).unwrap();

                let dir2 = tempfile::tempdir().unwrap();
                let db2 = dir2.path().join("meta2.redb");
                let local2 = RedbMetaEngine::open(&db2).unwrap();
                local2.init_root(0, 0).unwrap();
                local2.set_format_info(&format_info).unwrap();
                drop(local2);
                let c2 = TonboMetaEngine::open(&db2).unwrap();

                // Ensure both clients have same stale base.
                c1.lookup(ROOT_INO, b"a.txt").unwrap();
                c2.lookup(ROOT_INO, b"a.txt").unwrap();

                // Concurrent-intent conflict on same source key:
                // c1: a.txt -> b.txt
                // c2: a.txt -> c.txt
                c1.rename_atomic(ROOT_INO, b"a.txt", ROOT_INO, b"b.txt")
                    .unwrap();
                if let Err(err) = c1.flush_now() {
                    if is_transient_storage_err(&err) {
                        eprintln!("skip tonbo_minio_e2e_stale_rename_vs_rename: {}", err);
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(err.to_string()));
                }
                c2.rename_atomic(ROOT_INO, b"a.txt", ROOT_INO, b"c.txt")
                    .unwrap();
                if let Err(err) = c2.flush_now() {
                    if is_transient_storage_err(&err) {
                        eprintln!("skip tonbo_minio_e2e_stale_rename_vs_rename: {}", err);
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(err.to_string()));
                }

                drop(c1);
                drop(c2);

                // Deterministic convergence:
                // dentry conflict winner by inode id keeps exactly one target entry.
                let verify_dir = tempfile::tempdir().unwrap();
                let verify_db = verify_dir.path().join("meta3.redb");
                let verify_local = RedbMetaEngine::open(&verify_db).unwrap();
                verify_local.init_root(0, 0).unwrap();
                verify_local.set_format_info(&format_info).unwrap();
                drop(verify_local);
                let verify = TonboMetaEngine::open(&verify_db).unwrap();

                assert!(verify.lookup(ROOT_INO, b"a.txt").is_err());
                let b_ok = verify.lookup(ROOT_INO, b"b.txt").is_ok();
                let c_ok = verify.lookup(ROOT_INO, b"c.txt").is_ok();
                assert!(
                    b_ok ^ c_ok,
                    "exactly one target of conflicting renames must survive"
                );
                drop(verify);

                if created_bucket {
                    cleanup_bucket_best_effort(&rt, &client, &bucket);
                }
                Ok(())
            })();
            let _ = tx.send(result.map_err(|e| e.to_string()));
        });

        match rx.recv_timeout(Duration::from_secs(90)) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => panic!("{}", err),
            Err(RecvTimeoutError::Timeout) => {
                panic!("tonbo_minio_e2e_stale_rename_vs_rename timed out after 90 seconds")
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("tonbo_minio_e2e_stale_rename_vs_rename worker thread disconnected")
            }
        }
    }

    #[test]
    #[ignore]
    fn test_tonbo_minio_cross_client_visibility_without_explicit_flush() {
        load_test_env();
        use std::sync::mpsc::{self, RecvTimeoutError};

        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = (|| -> anyhow::Result<()> {
                let endpoint = std::env::var("STUDIOFS_MINIO_ENDPOINT")
                    .unwrap_or_else(|_| "http://127.0.0.1:9000".into());
                let access_key = std::env::var("STUDIOFS_MINIO_ACCESS_KEY")
                    .unwrap_or_else(|_| "minioadmin".into());
                let secret_key = std::env::var("STUDIOFS_MINIO_SECRET_KEY")
                    .unwrap_or_else(|_| "minioadmin".into());
                let default_bucket = format!(
                    "studiofs-meta-test-{}-{}",
                    std::process::id(),
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                );
                let bucket = std::env::var("STUDIOFS_MINIO_BUCKET").unwrap_or(default_bucket);

                let rt = Builder::new_current_thread().enable_all().build().unwrap();
                let client = rt.block_on(build_minio_client(&endpoint, &access_key, &secret_key));
                let created_bucket = match rt.block_on(ensure_bucket(&client, &bucket)) {
                    Ok(v) => v,
                    Err(err) => {
                        eprintln!(
                            "skip tonbo_minio_cross_client_visibility_without_explicit_flush: {}",
                            err
                        );
                        return Ok(());
                    }
                };

                let disk_name = format!(
                    "tonbo-visibility-no-explicit-flush-{}-{}",
                    std::process::id(),
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis())
                        .unwrap_or(0)
                );
                let format_info = FormatInfo {
                    name: disk_name.clone(),
                    uuid: format!("{disk_name}-uuid"),
                    endpoint: endpoint.clone(),
                    bucket: bucket.clone(),
                    access_key: access_key.clone(),
                    secret_key: secret_key.clone(),
                    region: "us-east-1".into(),
                    data_prefix: format!("{disk_name}/objects/"),
                    metadata: MetadataConfig {
                        backend: MetadataBackend::Tonbo,
                        remote: Some(RemoteStoreConfig {
                            endpoint: endpoint.clone(),
                            bucket: bucket.clone(),
                            access_key: access_key.clone(),
                            secret_key: secret_key.clone(),
                            region: "us-east-1".into(),
                            prefix: format!("{disk_name}/metadata/"),
                        }),
                        // Tight observation window for the reader.
                        cache_ttl_secs: 1,
                        flush_interval_secs: 2,
                        // Keep size/ops thresholds high so this case depends on
                        // interval-driven flushing behavior.
                        flush_ops_threshold: 1_000_000,
                        flush_bytes_threshold: 1 << 40,
                    },
                    version: String::new(),
                    block_size: DEFAULT_BLOCK_SIZE,
                    chunk_size: 0,
                };

                // Seed remote manifest baseline.
                let seed_dir = tempfile::tempdir().unwrap();
                let seed_db = seed_dir.path().join("seed.redb");
                let seed_local = RedbMetaEngine::open(&seed_db).unwrap();
                seed_local.init_root(0, 0).unwrap();
                seed_local.set_format_info(&format_info).unwrap();
                drop(seed_local);
                let seed = TonboMetaEngine::open(&seed_db).unwrap();
                if let Err(err) = seed.flush_now() {
                    if is_transient_storage_err(&err) {
                        eprintln!(
                            "skip tonbo_minio_cross_client_visibility_without_explicit_flush: {}",
                            err
                        );
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(err.to_string()));
                }
                drop(seed);

                // Two clients from the same remote baseline.
                let dir1 = tempfile::tempdir().unwrap();
                let db1 = dir1.path().join("meta1.redb");
                let local1 = RedbMetaEngine::open(&db1).unwrap();
                local1.init_root(0, 0).unwrap();
                local1.set_format_info(&format_info).unwrap();
                drop(local1);
                let c1 = TonboMetaEngine::open(&db1).unwrap();

                let dir2 = tempfile::tempdir().unwrap();
                let db2 = dir2.path().join("meta2.redb");
                let local2 = RedbMetaEngine::open(&db2).unwrap();
                local2.init_root(0, 0).unwrap();
                local2.set_format_info(&format_info).unwrap();
                drop(local2);
                let c2 = TonboMetaEngine::open(&db2).unwrap();

                // Stabilize flush baseline so the next mutation starts from
                // "just flushed" state and cannot flush immediately by elapsed time.
                if let Err(err) = c1.flush_now() {
                    if is_transient_storage_err(&err) {
                        eprintln!(
                            "skip tonbo_minio_cross_client_visibility_without_explicit_flush: {}",
                            err
                        );
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(err.to_string()));
                }

                // Writer performs a metadata mutation but does NOT call flush_now().
                c1.create_file(ROOT_INO, b"eventual.txt", 0, 0, 0o644).unwrap();

                // Reader should eventually observe the new entry after
                // flush_interval + cache_ttl, without explicit flushes.
                let mut observed = false;
                let deadline = std::time::Instant::now() + Duration::from_secs(8);
                while std::time::Instant::now() < deadline {
                    match c2.lookup(ROOT_INO, b"eventual.txt") {
                        Ok(_) => {
                            observed = true;
                            break;
                        }
                        Err(MetaError::EntryNotFound { .. }) => {}
                        Err(err) if is_transient_storage_err(&err) => {
                            eprintln!(
                                "skip tonbo_minio_cross_client_visibility_without_explicit_flush: {}",
                                err
                            );
                            return Ok(());
                        }
                        Err(err) => {
                            return Err(anyhow::anyhow!(
                                "unexpected lookup error while waiting for visibility: {}",
                                err
                            ));
                        }
                    }
                    std::thread::sleep(Duration::from_millis(200));
                }

                drop(c1);
                drop(c2);

                if created_bucket
                    && let Err(e) = rt.block_on(delete_bucket_with_contents(&client, &bucket))
                {
                    debug!(
                        bucket,
                        error = %e,
                        "cleanup bucket failed (best-effort)"
                    );
                }

                if !observed {
                    return Err(anyhow::anyhow!(
                        "reader did not observe writer mutation within 8s \
                         without explicit flush_now()"
                    ));
                }
                Ok(())
            })();
            let _ = tx.send(result.map_err(|e| e.to_string()));
        });

        match rx.recv_timeout(Duration::from_secs(90)) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => panic!("{}", err),
            Err(RecvTimeoutError::Timeout) => {
                panic!(
                    "tonbo_minio_cross_client_visibility_without_explicit_flush timed out after 90 seconds"
                )
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!(
                    "tonbo_minio_cross_client_visibility_without_explicit_flush worker thread disconnected"
                )
            }
        }
    }
}
