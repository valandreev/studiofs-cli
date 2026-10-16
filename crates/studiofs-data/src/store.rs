//! DataStore — S3 storage abstraction via OpenDAL.

use crate::crypto::{self, ObjectCipher, VolumeMasterKey};
use crate::envelope::{SFEC_HEADER_FIXED_LEN, SfecError, SfecHeader, SfecHeaderLayout};
use crate::object::{ObjectHeader, validate_object_payload_size};
use crate::snapshot_key::{SnapshotArtifactKey, SnapshotArtifactPrefix};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use opendal::layers::{ConcurrentLimitLayer, RetryLayer, TimeoutLayer};
use opendal::{Buffer, EntryMode, Operator, services::S3};
use std::collections::HashSet;
use std::fmt;
use std::net::Ipv6Addr;
use std::ops::Range;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Duration, SystemTime};
use studiofs_meta::{EncryptionConfig, FormatInfo};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::task;
use tracing::{debug, info};

/// Errors returned by the data store layer during storage operations.
#[derive(Debug, Error)]
pub enum DataError {
    /// Storage service rejected the operation or returned an invalid response.
    #[error("storage error: {0}")]
    Storage(String),

    /// OpenDAL reported an underlying storage-layer failure.
    #[error("opendal error: {0}")]
    OpenDal(#[from] opendal::Error),

    /// Configured S3 endpoint violates the transport policy.
    #[error("invalid S3 endpoint: {0}")]
    InvalidEndpoint(String),

    /// Persisted encryption configuration is not supported by this binary.
    #[error("invalid data encryption config: {0}")]
    InvalidCryptoConfig(#[from] studiofs_meta::EncryptionContractError),

    /// Object encryption or authentication failed.
    #[error("crypto error: {0}")]
    Crypto(#[from] crypto::CryptoError),

    /// The authenticated plaintext object has invalid STFS metadata.
    #[error("object error: {0}")]
    Object(#[from] crate::object::ObjectError),

    /// A maintenance key is outside the configured volume or contains path-like segments.
    #[error("invalid object key: {0}")]
    InvalidObjectKey(String),

    /// Dedicated crypto worker failed before returning an encryption result.
    #[error("crypto worker join error: {0}")]
    CryptoWorkerJoin(#[from] task::JoinError),

    /// A caller-requested payload range is empty, reversed, or beyond the object size limit.
    #[error("invalid payload range {start}..{end}")]
    InvalidRange {
        /// Requested inclusive start offset.
        start: u64,
        /// Requested exclusive end offset.
        end: u64,
    },
}

impl DataError {
    /// Return whether this error may be resolved by retrying the same remote operation.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::OpenDal(oe) => {
                oe.kind() == opendal::ErrorKind::RateLimited
                    || oe.kind() == opendal::ErrorKind::Unexpected
                    || oe.is_temporary()
            }
            Self::Storage(_) => true,
            Self::InvalidEndpoint(_)
            | Self::InvalidCryptoConfig(_)
            | Self::Crypto(_)
            | Self::Object(_)
            | Self::InvalidObjectKey(_)
            | Self::InvalidRange { .. }
            | Self::CryptoWorkerJoin(_) => false,
        }
    }
}

/// Result type for all datastore operations.
pub type DataResult<T> = Result<T, DataError>;

/// A file returned by a provider listing without exposing provider-specific types.
#[derive(Debug)]
pub struct DataStoreObjectEntry {
    /// Full storage key for the listed file.
    pub key: String,
    /// Provider-reported modification time, when available.
    pub last_modified: Option<SystemTime>,
}

/// Byte accounting for an encrypted remote write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteObjectWriteStats {
    /// Plaintext STFS object bytes accepted at the DataStore boundary.
    pub plaintext_bytes: u64,
    /// Ciphertext SFEC bytes written to the remote object store.
    pub encrypted_bytes: u64,
}

/// Byte accounting for an encrypted remote read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteObjectReadStats {
    /// Ciphertext SFEC bytes read from the remote object store.
    pub encrypted_bytes: u64,
    /// Plaintext bytes returned: a complete STFS object or a requested payload range.
    pub plaintext_bytes: u64,
}

/// Configuration for the data store.
#[derive(Clone)]
pub struct DataStoreConfig {
    /// Endpoint URL for the S3-compatible backend (for example,
    /// `http://127.0.0.1:9000`).
    pub endpoint: String,
    /// Target bucket name where objects are stored.
    pub bucket: String,
    /// Access key for backend authentication.
    pub access_key: String,
    /// Secret key for backend authentication.
    pub secret_key: String,
    /// Optional region used for signature generation; empty defaults to `us-east-1`.
    pub region: String,
    /// Stable namespace used as the object key prefix (often a volume name).
    pub volume_name: String,
    /// Persisted prefix for remote data objects; empty uses `{volume_name}/objects/`.
    pub data_prefix: String,
    /// Immutable volume UUID authenticated into every encrypted remote payload.
    pub volume_uuid: String,
    /// Transparent remote-object encryption settings.
    pub crypto: DataStoreCryptoConfig,
}

/// Encryption settings used by [`DataStore`] for remote payload objects.
#[derive(Clone)]
pub struct DataStoreCryptoConfig {
    /// Persisted volume encryption contract and active key material.
    pub encryption: EncryptionConfig,
}

impl DataStoreCryptoConfig {
    fn to_runtime(&self, volume_uuid: &str) -> DataResult<DataStoreCryptoRuntime> {
        self.encryption.validate()?;
        Ok(DataStoreCryptoRuntime {
            cipher: Arc::new(ObjectCipher::new(
                &VolumeMasterKey::from_bytes(self.encryption.volume_master_key),
                volume_uuid,
            )?),
            key_id: self.encryption.active_key_id,
        })
    }
}

impl From<EncryptionConfig> for DataStoreCryptoConfig {
    fn from(encryption: EncryptionConfig) -> Self {
        Self { encryption }
    }
}

impl From<&EncryptionConfig> for DataStoreCryptoConfig {
    fn from(encryption: &EncryptionConfig) -> Self {
        encryption.clone().into()
    }
}

impl fmt::Debug for DataStoreCryptoConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataStoreCryptoConfig")
            .field("encryption", &"<redacted>")
            .finish()
    }
}

#[derive(Clone)]
struct DataStoreCryptoRuntime {
    cipher: Arc<ObjectCipher>,
    key_id: u32,
}

impl fmt::Debug for DataStoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataStoreConfig")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("access_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .field("region", &self.region)
            .field("volume_name", &self.volume_name)
            .field("data_prefix", &self.data_prefix)
            .field("volume_uuid", &self.volume_uuid)
            .field("crypto", &self.crypto)
            .finish()
    }
}

/// I/O profile that tunes timeout and concurrency settings per use-case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoProfile {
    /// Default: balanced for mixed read/write workloads.
    /// io_timeout=60s, concurrent_limit=128, retries=3.
    Default,
    /// Optimised for reads: dedicated connection pool so uploads can never
    /// starve reads.
    /// io_timeout=30s, concurrent_limit=512, retries=0.
    Read,
    /// Optimised for writes (uploads, deletes): higher timeout budget for
    /// high-latency object stores.
    /// io_timeout=90s, concurrent_limit=256, retries=0.
    Write,
}

impl From<&FormatInfo> for DataStoreConfig {
    fn from(info: &FormatInfo) -> Self {
        Self {
            endpoint: info.endpoint.clone(),
            bucket: info.bucket.clone(),
            access_key: info.access_key.clone(),
            secret_key: info.secret_key.clone(),
            region: info.region.clone(),
            volume_name: info.name.clone(),
            data_prefix: normalize_data_prefix(&info.name, &info.data_prefix),
            volume_uuid: info.uuid.clone(),
            crypto: DataStoreCryptoConfig::from(&info.encryption),
        }
    }
}

fn normalize_data_prefix(volume_name: &str, data_prefix: &str) -> String {
    if data_prefix == "/" {
        return data_prefix.to_owned();
    }
    let configured_prefix = data_prefix.trim_end_matches('/');
    if configured_prefix.is_empty() {
        format!("{volume_name}/objects/")
    } else {
        format!("{configured_prefix}/")
    }
}

struct ParsedEndpoint {
    url: url::Url,
    normalized: String,
    explicit_scheme: bool,
}

fn invalid_endpoint(message: impl Into<String>) -> DataError {
    DataError::InvalidEndpoint(message.into())
}

/// Process-wide admission for object crypto: one permit per CPU thread that
/// may run SFEC frame work at once, shared by every `DataStore`.
static CRYPTO_ADMISSION: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    let cpus = std::thread::available_parallelism().map_or(1, usize::from);
    Arc::new(Semaphore::new(cpus))
});

/// Frame-parallelism hint for an object payload of `len` bytes.
fn crypto_worker_hint(len: usize) -> usize {
    (len / crypto::DEFAULT_ENCRYPTION_FRAME_SIZE).max(1)
}

/// Run object crypto on the blocking pool once admitted.
///
/// Waits for one CPU permit, then opportunistically takes up to
/// `max_workers - 1` more without waiting; `operation` gets the number of
/// threads it may use. Permits move into the blocking task, so they are held
/// until the crypto work finishes even if the caller is cancelled, and a caller
/// cancelled while still waiting holds nothing.
async fn run_crypto_blocking<T: Send + 'static>(
    admission: &Arc<Semaphore>,
    max_workers: usize,
    operation: impl FnOnce(usize) -> DataResult<T> + Send + 'static,
) -> DataResult<T> {
    let first = Arc::clone(admission)
        .acquire_owned()
        .await
        .expect("crypto admission semaphore is never closed");
    let extra = u32::try_from(
        admission
            .available_permits()
            .min(max_workers.saturating_sub(1)),
    )
    .ok()
    .filter(|&n| n > 0)
    .and_then(|n| Arc::clone(admission).try_acquire_many_owned(n).ok());
    let workers = 1 + extra.as_ref().map_or(0, |permits| permits.num_permits());
    // `spawn_blocking` tasks cannot be aborted once running, so CPU pressure is
    // bounded by admission above rather than by a per-task timeout.
    match task::spawn_blocking(move || {
        let _permits = (first, extra);
        operation(workers)
    })
    .await
    {
        Ok(result) => result,
        Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
        Err(err) => Err(DataError::from(err)),
    }
}

fn parse_s3_endpoint(endpoint: &str) -> DataResult<ParsedEndpoint> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return Err(invalid_endpoint("endpoint URL must not be empty"));
    }

    if let Ok(url) = url::Url::parse(trimmed) {
        if url.scheme() == "memory" {
            return Err(invalid_endpoint(
                "`memory://` is reserved for in-memory test stores and cannot be used as a runtime S3 endpoint",
            ));
        }
        if url.host().is_some() {
            return Ok(ParsedEndpoint {
                url,
                normalized: trimmed.to_string(),
                explicit_scheme: true,
            });
        }
    }

    let normalized = format!("http://{trimmed}");
    let url = url::Url::parse(&normalized).map_err(|_| {
        invalid_endpoint(
            "must be an HTTP(S) URL with a host, for example https://s3.amazonaws.com or http://127.0.0.1:9000",
        )
    })?;
    if url.host().is_none() {
        return Err(invalid_endpoint(
            "must be an HTTP(S) URL with a host, for example https://s3.amazonaws.com or http://127.0.0.1:9000",
        ));
    }

    Ok(ParsedEndpoint {
        url,
        normalized,
        explicit_scheme: false,
    })
}

fn is_loopback_host(host: url::Host<&str>) -> bool {
    match host {
        url::Host::Domain(domain) => {
            domain.eq_ignore_ascii_case("localhost") || domain.ends_with(".localhost")
        }
        url::Host::Ipv4(addr) => addr.is_loopback(),
        url::Host::Ipv6(addr) => is_loopback_ipv6(addr),
    }
}

fn is_loopback_ipv6(addr: Ipv6Addr) -> bool {
    addr.is_loopback() || addr.to_ipv4().is_some_and(|ipv4| ipv4.is_loopback())
}

/// Validate and normalize an S3-compatible endpoint according to StudioFS'
/// transport policy.
///
/// Explicit `http://` endpoints are accepted as an operator-selected cleartext
/// transport. Bare host-port endpoints are accepted only for loopback
/// development/test hosts and are normalized to `http://`.
pub fn normalize_s3_endpoint_transport(endpoint: &str) -> DataResult<String> {
    let parsed = parse_s3_endpoint(endpoint)?;
    if !parsed.url.username().is_empty() || parsed.url.password().is_some() {
        return Err(invalid_endpoint(
            "endpoint URLs must not include credentials; pass S3 credentials separately",
        ));
    }

    match parsed.url.scheme() {
        "https" => Ok(parsed.normalized),
        "http" => {
            if parsed.explicit_scheme || parsed.url.host().is_some_and(is_loopback_host) {
                Ok(parsed.normalized)
            } else {
                Err(invalid_endpoint(
                    "bare host-port S3 endpoints without a scheme are accepted only for localhost, *.localhost, 127.0.0.0/8, [::1], or IPv4-mapped/compatible IPv6 loopback; use http:// explicitly to select plain HTTP for other hosts",
                ))
            }
        }
        _ => Err(invalid_endpoint("must use http:// or https://")),
    }
}

/// Validate an S3-compatible endpoint according to StudioFS' transport policy.
pub fn validate_s3_endpoint_transport(endpoint: &str) -> DataResult<()> {
    normalize_s3_endpoint_transport(endpoint).map(|_| ())
}

/// DataStore — manages payload-object I/O to S3-compatible storage.
pub struct DataStore {
    op: Operator,
    volume_name: String,
    volume_uuid: Arc<str>,
    crypto: DataStoreCryptoRuntime,
    config: DataStoreConfig,
    profile: IoProfile,
}

fn is_ip_endpoint(endpoint: &str) -> bool {
    fn check_host(url: &url::Url) -> bool {
        url.host()
            .map(|h| matches!(h, url::Host::Ipv4(_) | url::Host::Ipv6(_)))
            .unwrap_or(false)
    }

    if let Ok(url) = url::Url::parse(endpoint)
        && check_host(&url)
    {
        return true;
    }
    if let Ok(url) = url::Url::parse(&format!("http://{endpoint}"))
        && check_host(&url)
    {
        return true;
    }
    false
}

static PATH_STYLE_LOGGED_ENDPOINTS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn should_log_path_style_endpoint(seen: &mut HashSet<String>, endpoint: &str) -> bool {
    seen.insert(endpoint.to_string())
}

fn log_path_style_endpoint_once(endpoint: &str) {
    let seen = PATH_STYLE_LOGGED_ENDPOINTS.get_or_init(|| Mutex::new(HashSet::new()));
    match seen.lock() {
        Ok(mut guard) => {
            if should_log_path_style_endpoint(&mut guard, endpoint) {
                info!(
                    endpoint,
                    "using path-style addressing for IP-based S3 endpoint"
                );
            }
        }
        Err(_) => {
            debug!(
                endpoint,
                "skipping path-style addressing log because endpoint log-once state is poisoned"
            );
        }
    }
}

impl DataStore {
    /// Create a new DataStore with the default I/O profile.
    pub fn new(config: &DataStoreConfig) -> DataResult<Self> {
        Self::with_profile(config, IoProfile::Default)
    }

    /// Create a DataStore with a specific I/O profile.
    pub fn with_profile(config: &DataStoreConfig, profile: IoProfile) -> DataResult<Self> {
        Self::with_profile_and_crypto(
            config,
            profile,
            config.crypto.to_runtime(&config.volume_uuid)?,
        )
    }

    fn with_profile_and_crypto(
        config: &DataStoreConfig,
        profile: IoProfile,
        crypto: DataStoreCryptoRuntime,
    ) -> DataResult<Self> {
        let mut config = config.clone();
        config.endpoint = normalize_s3_endpoint_transport(&config.endpoint)?;
        config.data_prefix = normalize_data_prefix(&config.volume_name, &config.data_prefix);

        let mut builder = S3::default()
            .endpoint(&config.endpoint)
            .bucket(&config.bucket)
            .access_key_id(&config.access_key)
            .secret_access_key(&config.secret_key)
            .disable_config_load()
            .disable_ec2_metadata();

        // For IP-based endpoints, ensure we use path-style (default in OpenDAL).
        // Virtual-host style (bucket.host) doesn't work with IP addresses.
        if is_ip_endpoint(&config.endpoint) {
            log_path_style_endpoint_once(&config.endpoint);
        }

        if !config.region.is_empty() {
            builder = builder.region(&config.region);
        } else {
            // For local S3-compatible services
            builder = builder.region("us-east-1");
        }

        fn env_u64(key: &str, default: u64) -> u64 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(default)
        }

        fn env_usize(key: &str, default: usize) -> usize {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(default)
        }

        // Per-profile tuning of timeouts, retries, and concurrency.
        //
        // Read profile: generous io_timeout for high-latency object stores.
        // NO OpenDAL retries on reads — FetchScheduler owns retry logic and
        // adaptive back-pressure, avoiding stacked retry delays across layers.
        let (
            default_meta_timeout_s,
            default_io_timeout_s,
            default_max_retries,
            default_concurrent_limit,
        ): (u64, u64, usize, usize) = match profile {
            IoProfile::Default => (10, 60, 3, 128),
            IoProfile::Read => (3, 30, 0, 512),
            // UploadScheduler owns retry policy for writes; keep OpenDAL retries
            // off by default to avoid stacked retry storms.
            IoProfile::Write => (10, 90, 0, 256),
        };

        let profile_key = match profile {
            IoProfile::Default => "DEFAULT",
            IoProfile::Read => "READ",
            IoProfile::Write => "WRITE",
        };
        let meta_timeout_s = env_u64(
            &format!("STUDIOFS_S3_{}_META_TIMEOUT_SECS", profile_key),
            default_meta_timeout_s,
        );
        let io_timeout_s = env_u64(
            &format!("STUDIOFS_S3_{}_IO_TIMEOUT_SECS", profile_key),
            default_io_timeout_s,
        );
        let max_retries = env_usize(
            &format!("STUDIOFS_S3_{}_MAX_RETRIES", profile_key),
            default_max_retries,
        );
        let concurrent_limit = env_usize(
            &format!("STUDIOFS_S3_{}_CONCURRENT_LIMIT", profile_key),
            default_concurrent_limit,
        );

        let base = Operator::new(builder)?
            // TimeoutLayer MUST come before RetryLayer (see opendal docs).
            .layer(
                TimeoutLayer::new()
                    .with_timeout(Duration::from_secs(meta_timeout_s))
                    .with_io_timeout(Duration::from_secs(io_timeout_s)),
            );

        let op = if max_retries > 0 {
            // Attach RetryLayer only when explicitly enabled. With max_retries=0 we
            // leave retries entirely to upper schedulers (fetch/upload), avoiding
            // stacked retry storms and duplicate warning noise.
            base.layer(
                RetryLayer::new()
                    .with_max_times(max_retries)
                    .with_min_delay(Duration::from_millis(200))
                    .with_max_delay(Duration::from_secs(3))
                    .with_jitter(),
            )
            // Each profile gets its own connection pool — reads never compete
            // with uploads for HTTP connections.
            .layer(ConcurrentLimitLayer::new(concurrent_limit))
            .finish()
        } else {
            base.layer(ConcurrentLimitLayer::new(concurrent_limit))
                .finish()
        };

        debug!(
            profile = ?profile,
            meta_timeout_s,
            io_timeout_s,
            max_retries,
            concurrent_limit,
            "connected to S3: endpoint={}, bucket={}",
            config.endpoint, config.bucket,
        );

        Ok(Self {
            op,
            volume_name: config.volume_name.clone(),
            volume_uuid: Arc::from(config.volume_uuid.as_str()),
            crypto,
            config,
            profile,
        })
    }

    /// Create a DataStore around an already-built operator for focused tests.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_operator_for_test(config: &DataStoreConfig, op: Operator) -> DataResult<Self> {
        let mut config = config.clone();
        config.data_prefix = normalize_data_prefix(&config.volume_name, &config.data_prefix);
        Ok(Self {
            op,
            volume_name: config.volume_name.clone(),
            volume_uuid: Arc::from(config.volume_uuid.as_str()),
            crypto: config.crypto.to_runtime(&config.volume_uuid)?,
            config,
            profile: IoProfile::Default,
        })
    }

    /// Create a read-optimised clone with its own connection pool.
    ///
    /// The new store connects to the same endpoint/bucket but uses tighter
    /// timeouts and a dedicated concurrency limit so that background uploads
    /// can never starve read requests.
    pub fn clone_for_reads(&self) -> DataResult<Self> {
        let mut store =
            Self::with_profile_and_crypto(&self.config, IoProfile::Read, self.crypto.clone())?;
        store.volume_uuid = self.volume_uuid.clone();
        Ok(store)
    }

    /// Create a write-optimised clone with its own connection pool.
    pub fn clone_for_writes(&self) -> DataResult<Self> {
        let mut store =
            Self::with_profile_and_crypto(&self.config, IoProfile::Write, self.crypto.clone())?;
        store.volume_uuid = self.volume_uuid.clone();
        Ok(store)
    }

    /// The I/O profile this store was created with.
    pub fn profile(&self) -> IoProfile {
        self.profile
    }

    /// Return the normalized configured prefix used by typed storage keys.
    pub fn data_prefix(&self) -> &str {
        &self.config.data_prefix
    }

    /// Return the immutable volume UUID used by transparent object crypto.
    pub fn volume_uuid(&self) -> &str {
        &self.volume_uuid
    }

    /// Get the S3 object key for a generic object (content-addressable).
    ///
    /// Format: `{configured_data_prefix}{ab}/{cd}/{id}` for IDs with at least
    /// four characters, or `{configured_data_prefix}{id}` for shorter IDs.
    pub fn object_key(&self, object_id: &str) -> String {
        let configured_prefix = self.config.data_prefix.trim_end_matches('/');
        if object_id.len() < 4 {
            return format!("{configured_prefix}/{object_id}");
        }
        format!(
            "{configured_prefix}/{}/{}/{}",
            &object_id[0..2],
            &object_id[2..4],
            object_id
        )
    }

    /// Upload a generic object to S3.
    pub async fn put_object(&self, object_id: &str, data: impl Into<Buffer>) -> DataResult<()> {
        let data: Buffer = data.into();
        let key = self.object_key(object_id);
        debug!("uploading object: {key} ({} bytes)", data.len());
        self.put_by_key(&key, data).await
    }

    /// Upload bytes to an already-formatted storage key.
    ///
    /// Accepts both contiguous (`Bytes`) and non-contiguous (`Buffer`) payloads.
    /// Note: some service implementations may still materialize into contiguous
    /// buffers internally if vectored writes are unsupported.
    pub async fn put_by_key(&self, key: &str, data: impl Into<Buffer>) -> DataResult<()> {
        self.put_by_key_with_stats(key, data).await.map(|_| ())
    }

    /// Upload bytes under a validated Snapshot v2 artifact key.
    pub async fn put_snapshot_artifact(
        &self,
        key: &SnapshotArtifactKey,
        data: impl Into<Buffer>,
    ) -> DataResult<()> {
        self.validate_volume_key(key.as_str())?;
        self.put_by_key(key.as_str(), data).await
    }

    /// Upload bytes to an already-formatted storage key and return crypto-safe byte counters.
    pub async fn put_by_key_with_stats(
        &self,
        key: &str,
        data: impl Into<Buffer>,
    ) -> DataResult<RemoteObjectWriteStats> {
        let data: Buffer = data.into();
        let plaintext_len = data.len();
        let encrypted = self.encrypt_payload(key, data).await?;
        let encrypted_len = encrypted.len();
        debug!(
            key,
            plaintext_bytes = plaintext_len,
            remote_bytes = encrypted_len,
            "uploading encrypted object"
        );
        self.op.write(key, encrypted).await?;
        Ok(RemoteObjectWriteStats {
            plaintext_bytes: plaintext_len as u64,
            encrypted_bytes: encrypted_len as u64,
        })
    }

    /// Download a generic object from S3.
    pub async fn get_object(&self, object_id: &str) -> DataResult<Buffer> {
        let key = self.object_key(object_id);
        debug!("downloading object: {key}");
        self.get_by_key(&key).await
    }

    /// Download bytes from an already-formatted storage key.
    pub async fn get_by_key(&self, key: &str) -> DataResult<Buffer> {
        self.get_by_key_with_stats(key).await.map(|(data, _)| data)
    }

    /// Download bytes under a validated Snapshot v2 artifact key.
    pub async fn get_snapshot_artifact(&self, key: &SnapshotArtifactKey) -> DataResult<Buffer> {
        self.validate_volume_key(key.as_str())?;
        self.get_by_key(key.as_str()).await
    }

    /// Download bytes from an already-formatted storage key and return crypto-safe byte counters.
    pub async fn get_by_key_with_stats(
        &self,
        key: &str,
    ) -> DataResult<(Buffer, RemoteObjectReadStats)> {
        debug!("downloading by key: {key}");
        let data = self.op.read(key).await?;
        let encrypted_len = data.len();
        let plaintext = self.decrypt_payload(key, data).await?;
        let plaintext_len = plaintext.len();
        Ok((
            plaintext,
            RemoteObjectReadStats {
                encrypted_bytes: encrypted_len as u64,
                plaintext_bytes: plaintext_len as u64,
            },
        ))
    }

    /// Download a regular STFS fragment with a bound on the encrypted response body.
    ///
    /// Accepts at most `CLOUD_FRAGMENT_SIZE` payload bytes plus the STFS header;
    /// actual encrypted bytes must fit the SFEC prefix's header/frame geometry.
    /// Snapshot artifacts and other larger objects must use the generic full-read API.
    pub async fn get_payload_by_key_with_stats(
        &self,
        key: &str,
    ) -> DataResult<(Buffer, RemoteObjectReadStats)> {
        debug!(key, "downloading bounded regular payload");
        // No chunk/prefetch option: each buffer is checked before requesting the next one.
        let mut stream = self.op.reader(key).await?.into_stream(..).await?;
        let mut prefix = Vec::with_capacity(SFEC_HEADER_FIXED_LEN);
        let mut buffers = Vec::new();
        let mut encrypted_len = 0usize;
        let mut limit = SFEC_HEADER_FIXED_LEN;
        while let Some(buffer) = stream.next().await {
            let buffer = buffer?;
            if prefix.len() < SFEC_HEADER_FIXED_LEN {
                let needed = (SFEC_HEADER_FIXED_LEN - prefix.len()).min(buffer.len());
                prefix.extend_from_slice(&buffer.slice(..needed).to_bytes());
                if prefix.len() == SFEC_HEADER_FIXED_LEN {
                    let layout = SfecHeaderLayout::from_prefix(&prefix)
                        .map_err(crypto::CryptoError::from)?;
                    if layout.plaintext_len()
                        > (crate::object::CLOUD_FRAGMENT_SIZE + ObjectHeader::SIZE) as u64
                    {
                        return Err(
                            crypto::CryptoError::from(SfecError::ObjectLengthOverflow).into()
                        );
                    }
                    limit = layout
                        .expected_ciphertext_len()
                        .and_then(|len| {
                            len.checked_add(layout.header_len())
                                .ok_or(SfecError::ObjectLengthOverflow)
                        })
                        .map_err(crypto::CryptoError::from)?;
                }
            }
            if buffer.len() > limit - encrypted_len {
                return Err(crypto::CryptoError::from(SfecError::TrailingMalformedData).into());
            }
            encrypted_len += buffer.len();
            buffers.push(buffer);
        }
        let plaintext = self
            .decrypt_payload(key, buffers.into_iter().flatten().collect())
            .await?;
        let stats = RemoteObjectReadStats {
            encrypted_bytes: encrypted_len as u64,
            plaintext_bytes: plaintext.len() as u64,
        };
        Ok((plaintext, stats))
    }

    /// Read a payload range by opening only the SFEC frames that cover it.
    ///
    /// Frame zero is also authenticated to validate the STFS header. No complete
    /// payload checksum or cache entry can be produced from these partial bytes.
    /// Requests past the authenticated payload length return a short read.
    pub async fn get_payload_range_by_key_with_stats(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> DataResult<(Bytes, RemoteObjectReadStats)> {
        // Caller-range errors are not corruption; envelope-derived failures below stay corrupt.
        let invalid_range = || DataError::Crypto(crypto::CryptoError::FrameLayoutOverflow);
        if range.start >= range.end || range.end > u64::from(crate::object::MAX_OBJECT_SIZE) {
            return Err(DataError::InvalidRange {
                start: range.start,
                end: range.end,
            });
        }
        let fixed = self
            .op
            .read_with(key)
            .range(0..SFEC_HEADER_FIXED_LEN as u64)
            .await?
            .to_bytes();
        let layout = SfecHeaderLayout::from_prefix(&fixed).map_err(crypto::CryptoError::from)?;
        let header_len = layout.header_len();
        let plaintext_len = layout.plaintext_len();
        if plaintext_len < ObjectHeader::SIZE as u64
            || plaintext_len > crate::object::CLOUD_FRAGMENT_SIZE as u64 + ObjectHeader::SIZE as u64
        {
            return Err(crypto::CryptoError::from(SfecError::InvalidRange).into());
        }
        let first_end = u64::from(layout.frame_size())
            .max(ObjectHeader::SIZE as u64)
            .min(plaintext_len);
        // Geometry comes from the prefix alone, so every range is known before frame 0 is decrypted.
        let payload_len = plaintext_len - ObjectHeader::SIZE as u64;
        let start = range.start.min(payload_len) + ObjectHeader::SIZE as u64;
        let end = range.end.min(payload_len) + ObjectHeader::SIZE as u64;
        let remaining_start = start.max(first_end);
        let target_range = if start == end || end <= first_end {
            None
        } else {
            Some(
                layout
                    .ciphertext_range(remaining_start..end)
                    .map_err(crypto::CryptoError::from)?,
            )
        };
        let head_range = layout
            .ciphertext_range(0..first_end)
            .map_err(crypto::CryptoError::from)?;
        // Round 2: the tail and frame 0 are contiguous; a target directly after
        // frame 0 is merged into the same read.
        let merge_target = target_range
            .as_ref()
            .is_some_and(|target| target.start == head_range.end);
        let round2_end = match (&target_range, merge_target) {
            (Some(target), true) => target.end,
            _ => head_range.end,
        };
        let (round2, separate_target) = match &target_range {
            Some(target) if !merge_target => {
                let (round2, target_bytes) = futures::try_join!(
                    self.op
                        .read_with(key)
                        .range(SFEC_HEADER_FIXED_LEN as u64..round2_end)
                        .into_future(),
                    self.op.read_with(key).range(target.clone()).into_future(),
                )?;
                (round2, Some(target_bytes))
            }
            _ => (
                self.op
                    .read_with(key)
                    .range(SFEC_HEADER_FIXED_LEN as u64..round2_end)
                    .await?,
                None,
            ),
        };
        let tail_len = header_len - SFEC_HEADER_FIXED_LEN;
        if round2.len() < tail_len {
            return Err(crypto::CryptoError::from(SfecError::TruncatedHeader).into());
        }
        let header_bytes =
            Bytes::from([fixed.as_ref(), round2.slice(..tail_len).to_bytes().as_ref()].concat());
        // Authoritative strict parse of the header bytes read above.
        SfecHeader::parse(header_bytes.clone()).map_err(crypto::CryptoError::from)?;
        let local = |offset: u64| {
            usize::try_from(offset - SFEC_HEADER_FIXED_LEN as u64).map_err(|_| invalid_range())
        };
        let first_ciphertext = round2.slice(tail_len..local(head_range.end)?.min(round2.len()));
        let mut encrypted_bytes = fixed.len() as u64 + round2.len() as u64;
        let first = self
            .decrypt_payload_range(key, header_bytes.clone(), first_ciphertext, 0..first_end)
            .await?;
        let object_header = ObjectHeader::from_bytes(&first)?;
        let payload_len = usize::try_from(payload_len).map_err(|_| invalid_range())?;
        validate_object_payload_size(&object_header, payload_len)?;

        let payload = if start == end {
            Bytes::new()
        } else if end <= first_end {
            first.slice(start as usize..end as usize)
        } else {
            let target = target_range.ok_or_else(invalid_range)?;
            let ciphertext = if merge_target {
                round2.slice(local(target.start)?..local(target.end)?.min(round2.len()))
            } else {
                let read = separate_target.ok_or_else(invalid_range)?;
                encrypted_bytes += read.len() as u64;
                read
            };
            let remaining = self
                .decrypt_payload_range(key, header_bytes, ciphertext, remaining_start..end)
                .await?;
            if start >= first_end {
                remaining
            } else {
                let mut payload = Vec::with_capacity((end - start) as usize);
                payload.extend_from_slice(&first[start as usize..first_end as usize]);
                payload.extend_from_slice(&remaining);
                Bytes::from(payload)
            }
        };
        let stats = RemoteObjectReadStats {
            encrypted_bytes,
            plaintext_bytes: payload.len() as u64,
        };
        Ok((payload, stats))
    }

    async fn decrypt_payload_range(
        &self,
        key: &str,
        header_bytes: Bytes,
        ciphertext: Buffer,
        range: Range<u64>,
    ) -> DataResult<Bytes> {
        let key = key.to_owned();
        let crypto = self.crypto.clone();
        run_crypto_blocking(&CRYPTO_ADMISSION, 1, move |_| {
            crypto
                .cipher
                .decrypt_range(header_bytes, ciphertext, &key, range)
                .map_err(DataError::from)
        })
        .await
    }

    /// Delete a generic object from S3.
    pub async fn delete_object(&self, object_id: &str) -> DataResult<()> {
        let key = self.object_key(object_id);
        debug!("deleting object: {key}");
        self.delete_by_key(&key).await
    }

    /// Delete an object by an already-formatted storage key.
    pub async fn delete_by_key(&self, key: &str) -> DataResult<()> {
        self.validate_volume_key(key)?;
        debug!("deleting by key: {key}");
        self.op.delete(key).await?;
        Ok(())
    }

    /// Delete a Snapshot v2 artifact by its validated typed key.
    pub async fn delete_snapshot_artifact(&self, key: &SnapshotArtifactKey) -> DataResult<()> {
        self.delete_by_key(key.as_str()).await
    }

    /// Stream files below a validated storage prefix.
    ///
    /// Directory entries are skipped. Provider failures are yielded as
    /// [`DataError::OpenDal`] values and the stream then ends.
    pub async fn list_object_entries(
        &self,
        prefix: &str,
    ) -> DataResult<impl Stream<Item = DataResult<DataStoreObjectEntry>>> {
        self.validate_volume_prefix(prefix)?;
        let lister = self.op.lister_with(prefix).recursive(true).await?;

        Ok(lister.filter_map(|entry| {
            std::future::ready(match entry {
                Ok(entry) if entry.metadata().mode() == EntryMode::FILE => {
                    Some(Ok(DataStoreObjectEntry {
                        key: entry.path().to_owned(),
                        last_modified: entry.metadata().last_modified().map(Into::into),
                    }))
                }
                Ok(_) => None,
                Err(error) => Some(Err(DataError::from(error))),
            })
        }))
    }

    /// Stream Snapshot v2 artifacts below a validated typed snapshot prefix.
    pub async fn list_snapshot_artifacts(
        &self,
        prefix: &SnapshotArtifactPrefix,
    ) -> DataResult<impl Stream<Item = DataResult<DataStoreObjectEntry>>> {
        self.list_object_entries(prefix.as_str()).await
    }

    /// Check if the storage backend is accessible and the bucket exists.
    pub async fn check(&self) -> DataResult<()> {
        self.op.check().await.map_err(DataError::from)
    }

    /// Check if a specific path exists (can be used to check bucket access).
    pub async fn exists(&self, path: &str) -> DataResult<bool> {
        self.op.exists(path).await.map_err(DataError::from)
    }

    /// Delete all data associated with this volume.
    ///
    /// WARNING: This deletes everything under `{volume_name}/`.
    pub async fn delete_volume_data(&self) -> DataResult<()> {
        let prefix = format!("{}/", self.volume_name);
        info!("deleting all objects with prefix: {}", prefix);

        // S3 prefixes need not be objects; remove_all's preliminary stat is unnecessary.
        let objects = self.op.lister_with(&prefix).recursive(true).await?;
        self.op.delete_try_stream(objects).await?;

        Ok(())
    }

    fn validate_volume_key(&self, key: &str) -> DataResult<()> {
        self.validate_volume_path(key, false)
    }

    fn validate_volume_prefix(&self, prefix: &str) -> DataResult<()> {
        self.validate_volume_path(prefix, true)
    }

    fn validate_volume_path(&self, key: &str, allow_prefix: bool) -> DataResult<()> {
        let volume_prefix = &self.config.data_prefix;
        let Some(relative_key) = key.strip_prefix(volume_prefix.as_str()) else {
            return Err(DataError::InvalidObjectKey(key.to_owned()));
        };
        if (!allow_prefix && relative_key.is_empty())
            || key.contains('\\')
            || relative_key
                .split('/')
                .any(|segment| matches!(segment, "." | ".."))
        {
            return Err(DataError::InvalidObjectKey(key.to_owned()));
        }
        Ok(())
    }

    async fn encrypt_payload(&self, key: &str, plaintext: Buffer) -> DataResult<Buffer> {
        let key = key.to_string();
        let crypto = self.crypto.clone();
        let max_workers = crypto_worker_hint(plaintext.len());
        run_crypto_blocking(&CRYPTO_ADMISSION, max_workers, move |workers| {
            crypto
                .cipher
                .encrypt_object(plaintext, &key, crypto.key_id, workers)
                .map_err(DataError::from)
        })
        .await
    }

    async fn decrypt_payload(&self, key: &str, ciphertext: Buffer) -> DataResult<Buffer> {
        let key = key.to_string();
        let crypto = self.crypto.clone();
        let max_workers = crypto_worker_hint(ciphertext.len());
        run_crypto_blocking(&CRYPTO_ADMISSION, max_workers, move |workers| {
            let plaintext = crypto.cipher.decrypt_object(ciphertext, &key, workers)?;
            Ok(Buffer::from(plaintext))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::SFEC_MAGIC;
    use crate::object::{MAGIC as STFS_MAGIC, create_object_buffer, read_object};
    use bytes::Bytes;
    use futures::StreamExt;
    use opendal::raw::{
        Access, Layer, LayeredAccess, OpList, OpRead, OpWrite, RpDelete, RpList, RpRead, RpWrite,
    };
    use opendal::services::Memory;
    use std::thread;
    use studiofs_meta::EncryptionConfig;

    #[derive(Debug, Clone)]
    struct FullPayloadLayer {
        body: Bytes,
        advertised_size: Option<u64>,
        consumed: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[derive(Debug)]
    struct FullPayloadAccess<A> {
        inner: A,
        response: FullPayloadLayer,
    }

    struct FullPayloadReader {
        body: Bytes,
        consumed: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl opendal::raw::oio::Read for FullPayloadReader {
        async fn read(&mut self) -> opendal::Result<Buffer> {
            // Split even the fixed prefix across reads, then bound each yielded buffer.
            let chunk = self.body.split_to(self.body.len().min(17));
            self.consumed
                .fetch_add(chunk.len(), std::sync::atomic::Ordering::SeqCst);
            Ok(chunk.into())
        }
    }

    impl<A: Access> Layer<A> for FullPayloadLayer {
        type LayeredAccess = FullPayloadAccess<A>;

        fn layer(&self, inner: A) -> Self::LayeredAccess {
            FullPayloadAccess {
                inner,
                response: self.clone(),
            }
        }
    }

    impl<A: Access> LayeredAccess for FullPayloadAccess<A> {
        type Inner = A;
        type Reader = FullPayloadReader;
        type Writer = A::Writer;
        type Lister = A::Lister;
        type Deleter = A::Deleter;

        fn inner(&self) -> &Self::Inner {
            &self.inner
        }

        async fn read(&self, _: &str, _: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
            Ok((
                RpRead::new().with_size(self.response.advertised_size),
                FullPayloadReader {
                    body: self.response.body.clone(),
                    consumed: self.response.consumed.clone(),
                },
            ))
        }

        async fn write(
            &self,
            path: &str,
            args: OpWrite,
        ) -> opendal::Result<(RpWrite, Self::Writer)> {
            self.inner.write(path, args).await
        }

        async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
            self.inner.list(path, args).await
        }

        async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
            self.inner.delete().await
        }
    }

    #[tokio::test]
    async fn full_payload_stops_consuming_oversized_response_despite_advertised_size() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let config = encrypted_test_config();
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store = DataStore::with_operator_for_test(&config, raw.clone()).unwrap();
        let key = "unit-volume/objects/full-payload-oversized";
        store
            .put_by_key(key, Bytes::from_static(b"valid plaintext"))
            .await
            .unwrap();
        let encrypted = raw.read(key).await.unwrap().to_bytes();
        let accepted_len = encrypted.len();
        let consumed = Arc::new(AtomicUsize::new(0));
        let segmented = raw.clone().layer(FullPayloadLayer {
            body: encrypted.clone(),
            advertised_size: Some(accepted_len as u64),
            consumed: consumed.clone(),
        });
        let segmented = DataStore::with_operator_for_test(&config, segmented).unwrap();
        let (plaintext, stats) = segmented.get_payload_by_key_with_stats(key).await.unwrap();
        assert_eq!(plaintext.to_bytes(), Bytes::from_static(b"valid plaintext"));
        assert_eq!(stats.encrypted_bytes, accepted_len as u64);
        assert_eq!(consumed.load(Ordering::SeqCst), accepted_len);
        let mut oversized = encrypted.to_vec();
        oversized.resize(accepted_len + 1024 * 1024, 0);
        for advertised_size in [
            None,
            Some(accepted_len as u64),
            Some(oversized.len() as u64),
        ] {
            let consumed = Arc::new(AtomicUsize::new(0));
            let fake = raw.clone().layer(FullPayloadLayer {
                body: Bytes::copy_from_slice(&oversized),
                advertised_size,
                consumed: consumed.clone(),
            });
            let store = DataStore::with_operator_for_test(&config, fake).unwrap();
            let error = store.get_payload_by_key_with_stats(key).await.unwrap_err();
            assert!(matches!(error, DataError::Crypto(_)));
            assert!(!error.is_retryable());
            let actual = consumed.load(Ordering::SeqCst);
            assert!(
                actual <= accepted_len + 17,
                "consumed {actual} of {} bytes",
                oversized.len()
            );
            assert!(actual < oversized.len());
        }
    }

    #[tokio::test]
    async fn full_payload_round_trips_supported_fragment_boundary() {
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store =
            DataStore::with_operator_for_test(&encrypted_test_config(), raw.clone()).unwrap();
        let key = "unit-volume/objects/full-payload-boundary";
        for size in [0, crate::object::CLOUD_FRAGMENT_SIZE] {
            let payload = Bytes::from(vec![0x5a; size]);
            let object = create_object_buffer(payload.clone()).unwrap();
            let plaintext_len = object.len();
            let write_stats = store.put_by_key_with_stats(key, object).await.unwrap();
            let (object, stats) = store.get_payload_by_key_with_stats(key).await.unwrap();
            assert_eq!(read_object(object).unwrap(), payload);
            assert_eq!(stats.encrypted_bytes, write_stats.encrypted_bytes);
            assert_eq!(stats.plaintext_bytes, plaintext_len as u64);
        }
    }

    #[tokio::test]
    async fn full_payload_rejects_excessive_geometry_but_snapshot_keeps_generic_allowance() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use studiofs_meta::snapshot::SnapshotId;

        let config = encrypted_test_config();
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store = DataStore::with_operator_for_test(&config, raw.clone()).unwrap();
        let key = SnapshotArtifactKey::final_artifact(
            &config.data_prefix,
            7,
            &config.volume_uuid,
            &SnapshotId::parse("unit-volume.2026-10-16T12-00-00.000Z").unwrap(),
        )
        .unwrap();
        let payload = Bytes::from(vec![
            0x5a;
            crate::object::CLOUD_FRAGMENT_SIZE
                + ObjectHeader::SIZE
                + 1
        ]);
        store
            .put_snapshot_artifact(&key, payload.clone())
            .await
            .unwrap();
        assert_eq!(
            store.get_snapshot_artifact(&key).await.unwrap().to_bytes(),
            payload
        );
        let encrypted = raw.read(key.as_str()).await.unwrap().to_bytes();
        let consumed = Arc::new(AtomicUsize::new(0));
        let fake = raw.layer(FullPayloadLayer {
            body: encrypted.clone(),
            advertised_size: Some(40),
            consumed: consumed.clone(),
        });
        let store = DataStore::with_operator_for_test(&config, fake).unwrap();
        let error = store
            .get_payload_by_key_with_stats(key.as_str())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DataError::Crypto(crypto::CryptoError::ObjectLengthOverflow)
        ));
        assert!(!error.is_retryable());
        assert_eq!(consumed.load(Ordering::SeqCst), 51);
        assert!(consumed.load(Ordering::SeqCst) < encrypted.len());
    }

    #[tokio::test]
    async fn payload_range_reads_bound_transfer_and_preserve_frame_boundaries() {
        use crate::envelope::DEFAULT_SFEC_FRAME_SIZE;
        use crate::object::CLOUD_FRAGMENT_SIZE;

        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store =
            DataStore::with_operator_for_test(&encrypted_test_config(), raw.clone()).unwrap();
        let key = "unit-volume/objects/aa/bb/range-object";
        let payload = Bytes::from(
            (0..CLOUD_FRAGMENT_SIZE)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<_>>(),
        );
        store
            .put_by_key(key, create_object_buffer(payload.clone()).unwrap())
            .await
            .unwrap();
        let complete_remote_len = raw.stat(key).await.unwrap().content_length();
        let boundary = DEFAULT_SFEC_FRAME_SIZE as u64 - ObjectHeader::SIZE as u64;
        for range in [
            0..4096,
            boundary..boundary + 4096,
            boundary - 100..boundary + 100,
            CLOUD_FRAGMENT_SIZE as u64 - 100..CLOUD_FRAGMENT_SIZE as u64,
            CLOUD_FRAGMENT_SIZE as u64 - 100..CLOUD_FRAGMENT_SIZE as u64 + 100,
        ] {
            let (bytes, stats) = store
                .get_payload_range_by_key_with_stats(key, range.clone())
                .await
                .unwrap();
            let end = (range.end as usize).min(payload.len());
            assert_eq!(
                bytes,
                payload.slice(range.start as usize..end),
                "range {range:?}"
            );
            assert_eq!(stats.plaintext_bytes, bytes.len() as u64);
            assert!(
                stats.encrypted_bytes < complete_remote_len,
                "range {range:?}: {stats:?}"
            );
            assert!(stats.encrypted_bytes <= 2 * DEFAULT_SFEC_FRAME_SIZE as u64 + 512);
        }
        let (bytes, _) = store
            .get_payload_range_by_key_with_stats(
                key,
                CLOUD_FRAGMENT_SIZE as u64..CLOUD_FRAGMENT_SIZE as u64 + 100,
            )
            .await
            .unwrap();
        assert!(bytes.is_empty());
        assert_eq!(
            read_object(store.get_by_key(key).await.unwrap()).unwrap(),
            payload
        );
    }

    #[tokio::test]
    async fn payload_range_authenticates_header_and_requested_frames_only() {
        use crate::envelope::{DEFAULT_SFEC_FRAME_SIZE, SfecEnvelope};
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store =
            DataStore::with_operator_for_test(&encrypted_test_config(), raw.clone()).unwrap();
        let key = "unit-volume/objects/aa/bb/range-tamper";
        let payload = Bytes::from(vec![42; 4 * DEFAULT_SFEC_FRAME_SIZE]);
        store
            .put_by_key(key, create_object_buffer(payload).unwrap())
            .await
            .unwrap();
        let original = raw.read(key).await.unwrap().to_bytes();
        let envelope = SfecEnvelope::parse(original.clone()).unwrap();
        let header_len = envelope.header_bytes().len();
        let second_frame = header_len + DEFAULT_SFEC_FRAME_SIZE + 16;
        let mut tampered = original.to_vec();
        tampered[second_frame] ^= 1;
        raw.write(key, tampered).await.unwrap();
        assert!(matches!(
            store
                .get_payload_range_by_key_with_stats(
                    key,
                    DEFAULT_SFEC_FRAME_SIZE as u64..DEFAULT_SFEC_FRAME_SIZE as u64 + 4096
                )
                .await,
            Err(DataError::Crypto(crypto::CryptoError::AuthenticationFailed))
        ));
        assert_eq!(
            store
                .get_payload_range_by_key_with_stats(key, 0..4096)
                .await
                .unwrap()
                .0,
            Bytes::from(vec![42; 4096])
        );
        assert!(store.get_by_key(key).await.is_err());

        let mut tampered = original.to_vec();
        tampered[header_len] ^= 1;
        raw.write(key, tampered).await.unwrap();
        assert!(matches!(
            store
                .get_payload_range_by_key_with_stats(
                    key,
                    DEFAULT_SFEC_FRAME_SIZE as u64..DEFAULT_SFEC_FRAME_SIZE as u64 + 4096
                )
                .await,
            Err(DataError::Crypto(crypto::CryptoError::AuthenticationFailed))
        ));
    }

    #[tokio::test]
    async fn payload_range_rejects_invalid_stfs_sizes_and_request_bounds() {
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store = DataStore::with_operator_for_test(&encrypted_test_config(), raw).unwrap();
        let key = "unit-volume/objects/aa/bb/range-invalid";
        let mut stfs = create_object_buffer(Bytes::from_static(b"payload"))
            .unwrap()
            .to_bytes()
            .to_vec();
        stfs[8..12].copy_from_slice(&99u32.to_le_bytes());
        store.put_by_key(key, stfs).await.unwrap();
        assert!(matches!(
            store.get_payload_range_by_key_with_stats(key, 0..3).await,
            Err(DataError::Object(
                crate::object::ObjectError::SizeMismatch { .. }
            ))
        ));
        for (range, start, end) in [
            (0..0, 0, 0),
            (Range { start: 2, end: 1 }, 2, 1),
            (0..u64::MAX, 0, u64::MAX),
        ] {
            let err = store
                .get_payload_range_by_key_with_stats(key, range)
                .await
                .unwrap_err();
            assert!(matches!(
                err,
                DataError::InvalidRange { start: s, end: e } if s == start && e == end
            ));
            assert!(!err.is_retryable());
        }
    }

    /// Counts OpenDAL `read` calls on the wrapped accessor.
    #[derive(Debug)]
    struct ReadCounterLayer(Arc<std::sync::atomic::AtomicUsize>);

    #[derive(Debug)]
    struct ReadCounterAccess<A: Access> {
        inner: A,
        reads: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl<A: Access> Layer<A> for ReadCounterLayer {
        type LayeredAccess = ReadCounterAccess<A>;

        fn layer(&self, inner: A) -> Self::LayeredAccess {
            ReadCounterAccess {
                inner,
                reads: self.0.clone(),
            }
        }
    }

    impl<A: Access> LayeredAccess for ReadCounterAccess<A> {
        type Inner = A;
        type Reader = A::Reader;
        type Writer = A::Writer;
        type Lister = A::Lister;
        type Deleter = A::Deleter;

        fn inner(&self) -> &Self::Inner {
            &self.inner
        }

        async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.read(path, args).await
        }

        async fn write(
            &self,
            path: &str,
            args: OpWrite,
        ) -> opendal::Result<(RpWrite, Self::Writer)> {
            self.inner.write(path, args).await
        }

        async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
            self.inner.list(path, args).await
        }

        async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
            self.inner.delete().await
        }
    }

    #[tokio::test]
    async fn payload_range_middle_frame_uses_at_most_three_reads() {
        use crate::envelope::DEFAULT_SFEC_FRAME_SIZE;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let reads = Arc::new(AtomicUsize::new(0));
        let raw = Operator::new(Memory::default())
            .unwrap()
            .layer(ReadCounterLayer(reads.clone()))
            .finish();
        let store = DataStore::with_operator_for_test(&encrypted_test_config(), raw).unwrap();
        let key = "unit-volume/objects/aa/bb/range-count";
        // Exactly four frames of plaintext (STFS header + payload).
        let payload = Bytes::from(vec![
            7u8;
            crate::object::CLOUD_FRAGMENT_SIZE - ObjectHeader::SIZE
        ]);
        store
            .put_by_key(key, create_object_buffer(payload.clone()).unwrap())
            .await
            .unwrap();

        // Plaintext offset 2 * frame + 10 lies in frame 2, away from frames 0 and 1.
        let start = 2 * DEFAULT_SFEC_FRAME_SIZE as u64 + 10 - ObjectHeader::SIZE as u64;
        reads.store(0, Ordering::SeqCst);
        let (bytes, _) = store
            .get_payload_range_by_key_with_stats(key, start..start + 4096)
            .await
            .unwrap();
        assert_eq!(bytes, payload.slice(start as usize..start as usize + 4096));
        let read_calls = reads.load(Ordering::SeqCst);
        assert!(
            read_calls <= 3,
            "expected at most 3 reads, got {read_calls}"
        );
    }

    /// Read `len` payload bytes at `plaintext_start` (STFS header included in
    /// plaintext coordinates) from a four-frame object and return the read-call count.
    async fn range_read_calls(plaintext_start: u64, len: u64) -> usize {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let reads = Arc::new(AtomicUsize::new(0));
        let raw = Operator::new(Memory::default())
            .unwrap()
            .layer(ReadCounterLayer(reads.clone()))
            .finish();
        let store = DataStore::with_operator_for_test(&encrypted_test_config(), raw).unwrap();
        let key = "unit-volume/objects/aa/bb/range-count";
        // Exactly four frames of plaintext (STFS header + payload).
        let payload = Bytes::from(vec![
            7u8;
            crate::object::CLOUD_FRAGMENT_SIZE - ObjectHeader::SIZE
        ]);
        store
            .put_by_key(key, create_object_buffer(payload.clone()).unwrap())
            .await
            .unwrap();

        let start = plaintext_start - ObjectHeader::SIZE as u64;
        reads.store(0, Ordering::SeqCst);
        let (bytes, _) = store
            .get_payload_range_by_key_with_stats(key, start..start + len)
            .await
            .unwrap();
        assert_eq!(bytes, payload.slice(start as usize..(start + len) as usize));
        reads.load(Ordering::SeqCst)
    }

    #[tokio::test]
    async fn payload_range_adjacent_frame_merges_into_head_read() {
        use crate::envelope::DEFAULT_SFEC_FRAME_SIZE;
        // Plaintext offset lies in frame 1, directly after frame 0: prefix plus one merged read.
        let reads = range_read_calls(DEFAULT_SFEC_FRAME_SIZE as u64 + 10, 4096).await;
        assert!(reads <= 2, "expected at most 2 reads, got {reads}");
    }

    #[tokio::test]
    async fn payload_range_distant_frame_reads_head_and_target_separately() {
        use crate::envelope::DEFAULT_SFEC_FRAME_SIZE;
        // Plaintext offset in frame 3 is not adjacent to frame 0: prefix, head, target.
        let reads = range_read_calls(3 * DEFAULT_SFEC_FRAME_SIZE as u64 + 10, 4096).await;
        assert!(reads <= 3, "expected at most 3 reads, got {reads}");
    }

    /// Tracks ranged OpenDAL `read` calls that are pending at the same time.
    #[derive(Debug)]
    struct InFlightReadLayer(Arc<InFlightReads>);

    #[derive(Debug)]
    struct InFlightReads {
        armed: std::sync::atomic::AtomicBool,
        in_flight: tokio::sync::watch::Sender<usize>,
        max_in_flight: std::sync::atomic::AtomicUsize,
    }

    impl InFlightReads {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                armed: std::sync::atomic::AtomicBool::new(false),
                in_flight: tokio::sync::watch::channel(0).0,
                max_in_flight: std::sync::atomic::AtomicUsize::new(0),
            })
        }
    }

    #[derive(Debug)]
    struct InFlightReadAccess<A: Access> {
        inner: A,
        reads: Arc<InFlightReads>,
    }

    impl<A: Access> Layer<A> for InFlightReadLayer {
        type LayeredAccess = InFlightReadAccess<A>;

        fn layer(&self, inner: A) -> Self::LayeredAccess {
            InFlightReadAccess {
                inner,
                reads: self.0.clone(),
            }
        }
    }

    impl<A: Access> LayeredAccess for InFlightReadAccess<A> {
        type Inner = A;
        type Reader = A::Reader;
        type Writer = A::Writer;
        type Lister = A::Lister;
        type Deleter = A::Deleter;

        fn inner(&self) -> &Self::Inner {
            &self.inner
        }

        async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
            // The 40-byte prefix (offset 0) is legitimately alone, so it passes through.
            if !self.reads.armed.load(std::sync::atomic::Ordering::SeqCst)
                || args.range().offset() == 0
            {
                return self.inner.read(path, args).await;
            }
            let max = &self.reads.max_in_flight;
            self.reads.in_flight.send_modify(|pending| {
                *pending += 1;
                max.fetch_max(*pending, std::sync::atomic::Ordering::SeqCst);
            });
            // Bounded wait on the real condition: a second ranged read is pending.
            let mut pending = self.reads.in_flight.subscribe();
            let _ =
                tokio::time::timeout(Duration::from_secs(2), pending.wait_for(|n| *n >= 2)).await;
            let result = self.inner.read(path, args).await;
            self.reads.in_flight.send_modify(|pending| *pending -= 1);
            result
        }

        async fn write(
            &self,
            path: &str,
            args: OpWrite,
        ) -> opendal::Result<(RpWrite, Self::Writer)> {
            self.inner.write(path, args).await
        }

        async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
            self.inner.list(path, args).await
        }

        async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
            self.inner.delete().await
        }
    }

    /// Reads `len` plaintext bytes at `plaintext_start` from a four-frame object and
    /// returns the peak number of concurrently pending ranged reads.
    async fn peak_concurrent_range_reads(plaintext_start: u64, len: u64) -> usize {
        let gate = InFlightReads::new();
        let raw = Operator::new(Memory::default())
            .unwrap()
            .layer(InFlightReadLayer(gate.clone()))
            .finish();
        let store = DataStore::with_operator_for_test(&encrypted_test_config(), raw).unwrap();
        let key = "unit-volume/objects/aa/bb/range-concurrent";
        let payload = Bytes::from(vec![
            7u8;
            crate::object::CLOUD_FRAGMENT_SIZE - ObjectHeader::SIZE
        ]);
        store
            .put_by_key(key, create_object_buffer(payload.clone()).unwrap())
            .await
            .unwrap();

        gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        let start = plaintext_start - ObjectHeader::SIZE as u64;
        let (bytes, _) = store
            .get_payload_range_by_key_with_stats(key, start..start + len)
            .await
            .unwrap();
        assert_eq!(bytes, payload.slice(start as usize..(start + len) as usize));
        gate.max_in_flight.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[tokio::test]
    async fn payload_range_reads_distant_target_concurrently_with_head() {
        use crate::envelope::DEFAULT_SFEC_FRAME_SIZE;
        // Frame 2 is not adjacent to frame 0: the head and target reads must overlap.
        let peak = peak_concurrent_range_reads(2 * DEFAULT_SFEC_FRAME_SIZE as u64 + 10, 4096).await;
        assert_eq!(peak, 2, "head and target reads must be in flight together");
    }

    #[tokio::test]
    async fn payload_range_adjacent_frame_merges_into_single_gated_read() {
        use crate::envelope::DEFAULT_SFEC_FRAME_SIZE;
        // Frame 1 directly follows frame 0 and is merged into the head read: nothing to overlap.
        let peak = peak_concurrent_range_reads(DEFAULT_SFEC_FRAME_SIZE as u64 + 10, 4096).await;
        assert_eq!(peak, 1, "adjacent frame must not issue a separate read");
    }

    #[tokio::test]
    async fn payload_range_short_object_stays_within_remote_length() {
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store =
            DataStore::with_operator_for_test(&encrypted_test_config(), raw.clone()).unwrap();
        let key = "unit-volume/objects/aa/bb/range-short";
        store
            .put_by_key(
                key,
                create_object_buffer(Bytes::from_static(b"short")).unwrap(),
            )
            .await
            .unwrap();
        let remote_len = raw.stat(key).await.unwrap().content_length();
        // Requests well past both the payload and the first frame must clamp, not over-read.
        let (bytes, stats) = store
            .get_payload_range_by_key_with_stats(key, 1..4096)
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"hort"));
        assert_eq!(stats.encrypted_bytes, remote_len);
        assert_eq!(stats.plaintext_bytes, 4);
    }

    fn encrypted_test_config() -> DataStoreConfig {
        DataStoreConfig {
            endpoint: "memory://".into(),
            bucket: "unit-test".into(),
            access_key: "access".into(),
            secret_key: "secret".into(),
            region: "us-east-1".into(),
            volume_name: "unit-volume".into(),
            data_prefix: "unit-volume/objects/".into(),
            volume_uuid: "1a4c0fe2-8550-4892-952c-5ec26c0e0d8d".into(),
            crypto: DataStoreCryptoConfig::from(EncryptionConfig::new([0x42; 32])),
        }
    }

    #[test]
    fn data_prefix_normalization_defaults_and_removes_trailing_slashes() {
        assert_eq!(
            normalize_data_prefix("unit-volume", ""),
            "unit-volume/objects/"
        );
        assert_eq!(
            normalize_data_prefix("unit-volume", "tenant/demo///"),
            "tenant/demo/"
        );
        assert_eq!(normalize_data_prefix("unit-volume", "/"), "/");
    }

    fn assert_send<T: Send>() {}

    #[test]
    fn opendal_buffer_is_send_for_blocking_crypto_handoff() {
        assert_send::<Buffer>();
    }

    #[test]
    fn test_is_ip_endpoint_ipv4() {
        assert!(is_ip_endpoint("http://192.168.1.50:9000"));
        assert!(is_ip_endpoint("https://10.0.0.1:9000"));
        assert!(is_ip_endpoint("192.168.1.50:9000"));
    }

    #[test]
    fn test_is_ip_endpoint_ipv6() {
        assert!(is_ip_endpoint("http://[::1]:9000"));
        assert!(is_ip_endpoint("https://[::1]"));
    }

    #[test]
    fn test_is_ip_endpoint_hostname() {
        assert!(!is_ip_endpoint("http://minio.local:9000"));
        assert!(!is_ip_endpoint("https://s3.amazonaws.com"));
        assert!(!is_ip_endpoint("minio.local:9000"));
    }

    #[test]
    fn datastore_accepts_user_selected_plain_http_endpoint() {
        let mut config = encrypted_test_config();
        config.endpoint = "http://192.168.1.20:10000".to_string();

        let store = DataStore::new(&config)
            .expect("explicit http:// endpoint should honor the operator transport choice");

        assert_eq!(store.config.endpoint, "http://192.168.1.20:10000");
    }

    #[test]
    fn datastore_accepts_https_and_http_endpoints() {
        for endpoint in [
            "https://s3.amazonaws.com",
            "http://s3.example.com",
            "http://localhost:9000",
            "http://my-bucket.localhost:9000",
            "http://My-Bucket.LOCALHOST:9000",
            "http://127.0.0.1:9000",
            "http://127.1.2.3:9000",
            "http://[::1]:9000",
            "http://[::ffff:127.0.0.1]:9000",
            "http://[::127.0.0.1]:9000",
            "http://192.168.1.20:10000",
        ] {
            let mut config = encrypted_test_config();
            config.endpoint = endpoint.to_string();

            DataStore::new(&config)
                .unwrap_or_else(|err| panic!("endpoint {endpoint} should be accepted, got {err}"));
        }
    }

    #[test]
    fn s3_endpoint_policy_rejects_credentials_without_leaking_them() {
        let mut config = encrypted_test_config();
        config.endpoint = "https://AKIA_TEST:super-secret@s3.example.com".to_string();

        let err = DataStore::new(&config)
            .err()
            .expect("endpoint credentials must be rejected")
            .to_string();

        assert!(err.contains("credentials"));
        assert!(!err.contains("AKIA_TEST"));
        assert!(!err.contains("super-secret"));
        assert!(!err.contains("secret"));
        assert!(!err.contains("access"));
    }

    #[test]
    fn datastore_normalizes_bare_loopback_endpoint_before_building_client() {
        let mut config = encrypted_test_config();
        config.endpoint = "127.0.0.1:9000".to_string();

        let store = DataStore::new(&config).expect("bare loopback endpoint should build");

        assert_eq!(store.config.endpoint, "http://127.0.0.1:9000");
    }

    #[test]
    fn datastore_rejects_memory_endpoint_for_runtime_client() {
        let err = match DataStore::new(&encrypted_test_config()) {
            Ok(_) => panic!("memory endpoint is test-only"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("test"));
    }

    #[test]
    fn path_style_endpoint_log_once_helper_is_per_endpoint() {
        let mut seen = std::collections::HashSet::new();

        assert!(should_log_path_style_endpoint(
            &mut seen,
            "http://192.168.1.50:9000"
        ));
        assert!(!should_log_path_style_endpoint(
            &mut seen,
            "http://192.168.1.50:9000"
        ));
        assert!(should_log_path_style_endpoint(
            &mut seen,
            "http://192.168.1.51:9000"
        ));
    }

    #[test]
    fn datastore_config_debug_redacts_crypto_key_material() {
        let rendered = format!("{:?}", encrypted_test_config());

        assert!(rendered.contains("crypto"));
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("volume_master_key"));
        assert!(!rendered.contains("42"));
    }

    #[test]
    fn disabled_crypto_config_is_rejected_without_plaintext_passthrough() {
        let mut config = encrypted_test_config();
        config.crypto.encryption.enabled = false;
        let raw = Operator::new(Memory::default()).unwrap().finish();

        let result = DataStore::with_operator_for_test(&config, raw);

        assert!(matches!(
            result,
            Err(DataError::InvalidCryptoConfig(
                studiofs_meta::EncryptionContractError::Disabled
            ))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn crypto_blocking_helper_runs_work_off_async_worker_thread() {
        let async_worker_thread = thread::current().id();

        let crypto_thread =
            run_crypto_blocking(&CRYPTO_ADMISSION, 1, |_| Ok(thread::current().id()))
                .await
                .unwrap();

        assert_ne!(crypto_thread, async_worker_thread);
    }

    #[tokio::test]
    #[should_panic(expected = "crypto worker panic")]
    async fn crypto_blocking_helper_resumes_worker_panics() {
        let admission = Arc::new(Semaphore::new(1));
        let _ = run_crypto_blocking::<()>(&admission, 1, |_| panic!("crypto worker panic")).await;
    }

    #[tokio::test]
    async fn crypto_admission_grants_only_idle_extra_workers() {
        let admission = Arc::new(Semaphore::new(4));
        let grant = |max| run_crypto_blocking(&admission, max, Ok);

        assert_eq!(grant(3).await.unwrap(), 3);
        assert_eq!(grant(8).await.unwrap(), 4);
        assert_eq!(grant(1).await.unwrap(), 1);
        let held = Arc::clone(&admission).acquire_many_owned(3).await.unwrap();
        assert_eq!(grant(4).await.unwrap(), 1);
        drop(held);
        assert_eq!(admission.available_permits(), 4);
    }

    /// Start crypto work that reports its start and then holds its permits
    /// until `release` is signalled.
    fn gated_crypto(
        admission: &Arc<Semaphore>,
        max_workers: usize,
    ) -> (
        tokio::task::JoinHandle<DataResult<usize>>,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let admission = Arc::clone(admission);
        let handle = tokio::spawn(async move {
            run_crypto_blocking(&admission, max_workers, move |workers| {
                started_tx.send(()).unwrap();
                let _ = release_rx.recv();
                Ok(workers)
            })
            .await
        });
        (handle, started_rx, release_tx)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crypto_admission_bounds_concurrency_and_waiters_hold_nothing() {
        let admission = Arc::new(Semaphore::new(2));
        let (first, first_started, release_first) = gated_crypto(&admission, 1);
        let (second, second_started, release_second) = gated_crypto(&admission, 1);
        task::spawn_blocking(move || {
            first_started.recv().unwrap();
            second_started.recv().unwrap();
        })
        .await
        .unwrap();
        assert_eq!(admission.available_permits(), 0);

        // A third operation must wait; cancelling it while waiting leaks nothing.
        let mut waiting = Box::pin(run_crypto_blocking(&admission, 1, Ok));
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        drop(waiting);

        release_first.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap(), 1);
        assert_eq!(admission.available_permits(), 1);
        release_second.send(()).unwrap();
        assert_eq!(second.await.unwrap().unwrap(), 1);
        assert_eq!(admission.available_permits(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_caller_keeps_permits_until_crypto_work_finishes() {
        let admission = Arc::new(Semaphore::new(3));
        let (caller, started, release) = gated_crypto(&admission, 3);
        task::spawn_blocking(move || started.recv().unwrap())
            .await
            .unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        // The running blocking task still owns all three CPU slots.
        assert_eq!(admission.available_permits(), 0);

        release.send(()).unwrap();
        let all = tokio::time::timeout(Duration::from_secs(10), admission.acquire_many(3))
            .await
            .expect("permits are released once crypto work finishes");
        drop(all.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn high_concurrency_crypto_never_exceeds_admission() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let admission = Arc::new(Semaphore::new(3));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..64)
            .map(|i| {
                let (admission, active, peak) = (
                    Arc::clone(&admission),
                    Arc::clone(&active),
                    Arc::clone(&peak),
                );
                tokio::spawn(async move {
                    run_crypto_blocking(&admission, 1 + i % 4, move |workers| {
                        let now = active.fetch_add(workers, Ordering::SeqCst) + workers;
                        peak.fetch_max(now, Ordering::SeqCst);
                        std::hint::black_box((0..10_000u64).sum::<u64>());
                        active.fetch_sub(workers, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap().unwrap();
        }

        assert!(peak.load(Ordering::SeqCst) <= 3);
        assert_eq!(admission.available_permits(), 3);
    }

    #[test]
    fn profile_clones_share_volume_uuid_allocation() {
        let mut config = encrypted_test_config();
        config.endpoint = "http://127.0.0.1:9000".to_string();
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store = DataStore::with_operator_for_test(&config, raw.clone()).unwrap();

        let read_store = store.clone_for_reads().unwrap();
        let write_store = store.clone_for_writes().unwrap();

        assert!(Arc::ptr_eq(&store.volume_uuid, &read_store.volume_uuid));
        assert!(Arc::ptr_eq(&store.volume_uuid, &write_store.volume_uuid));
    }

    #[tokio::test]
    async fn key_and_object_paths_store_sfec_remotely_and_return_stfs_plaintext() {
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store =
            DataStore::with_operator_for_test(&encrypted_test_config(), raw.clone()).unwrap();

        let key = "unit-volume/objects/aa/bb/key-path-object";
        let key_plaintext = create_object_buffer(Bytes::from_static(b"key path secret"))
            .unwrap()
            .to_bytes();

        store.put_by_key(key, key_plaintext.clone()).await.unwrap();

        let remote_key_bytes = raw.read(key).await.unwrap().to_bytes();
        assert!(remote_key_bytes.starts_with(SFEC_MAGIC));
        assert!(!remote_key_bytes.starts_with(STFS_MAGIC));

        let downloaded_key_bytes = store.get_by_key(key).await.unwrap().to_bytes();
        assert_eq!(downloaded_key_bytes, key_plaintext);
        assert_eq!(
            read_object(Bytes::clone(&downloaded_key_bytes)).unwrap(),
            Bytes::from_static(b"key path secret")
        );

        let object_id = "0011223344556677";
        let object_plaintext = create_object_buffer(Bytes::from_static(b"object path secret"))
            .unwrap()
            .to_bytes();
        store
            .put_object(object_id, object_plaintext.clone())
            .await
            .unwrap();

        let object_key = store.object_key(object_id);
        let remote_object_bytes = raw.read(&object_key).await.unwrap().to_bytes();
        assert!(remote_object_bytes.starts_with(SFEC_MAGIC));
        assert!(!remote_object_bytes.starts_with(STFS_MAGIC));

        let downloaded_object_bytes = store.get_object(object_id).await.unwrap().to_bytes();
        assert_eq!(downloaded_object_bytes, object_plaintext);
        assert_eq!(
            read_object(downloaded_object_bytes).unwrap(),
            Bytes::from_static(b"object path secret")
        );
    }

    #[tokio::test]
    async fn key_path_stats_preserve_plaintext_and_encrypted_byte_counts() {
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store =
            DataStore::with_operator_for_test(&encrypted_test_config(), raw.clone()).unwrap();
        let key = "unit-volume/objects/aa/bb/stats-object";
        let plaintext = create_object_buffer(Bytes::from_static(b"stats secret"))
            .unwrap()
            .to_bytes();

        let write_stats = store
            .put_by_key_with_stats(key, plaintext.clone())
            .await
            .unwrap();

        let remote_bytes = raw.read(key).await.unwrap().to_bytes();
        assert_eq!(write_stats.plaintext_bytes, plaintext.len() as u64);
        assert_eq!(write_stats.encrypted_bytes, remote_bytes.len() as u64);

        let (downloaded, read_stats) = store.get_by_key_with_stats(key).await.unwrap();

        assert_eq!(downloaded.to_bytes(), plaintext);
        assert_eq!(read_stats.encrypted_bytes, remote_bytes.len() as u64);
        assert_eq!(read_stats.plaintext_bytes, write_stats.plaintext_bytes);
    }

    #[tokio::test]
    async fn list_object_entries_streams_file_keys() {
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store =
            DataStore::with_operator_for_test(&encrypted_test_config(), raw.clone()).unwrap();

        for key in [
            "unit-volume/objects/aa/bb/0000000000000001",
            "unit-volume/objects/cc/dd/0000000000000002",
        ] {
            store
                .put_by_key(key, Bytes::from_static(b"payload"))
                .await
                .unwrap();
        }

        let entries = store
            .list_object_entries("unit-volume/objects/")
            .await
            .unwrap();
        let mut entries = std::pin::pin!(entries);
        let mut keys = Vec::new();
        while let Some(entry) = entries.next().await {
            keys.push(entry.unwrap().key);
        }
        keys.sort();

        assert_eq!(
            keys,
            vec![
                "unit-volume/objects/aa/bb/0000000000000001".to_string(),
                "unit-volume/objects/cc/dd/0000000000000002".to_string(),
            ]
        );
    }

    #[derive(Debug)]
    struct FailingListService;

    #[derive(Debug)]
    struct FailingLister;

    impl opendal::raw::oio::List for FailingLister {
        async fn next(&mut self) -> opendal::Result<Option<opendal::raw::oio::Entry>> {
            Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "forced listing failure",
            ))
        }
    }

    impl opendal::raw::Access for FailingListService {
        type Reader = opendal::raw::oio::Reader;
        type Writer = opendal::raw::oio::Writer;
        type Lister = opendal::raw::oio::Lister;
        type Deleter = opendal::raw::oio::Deleter;

        fn info(&self) -> std::sync::Arc<opendal::raw::AccessorInfo> {
            let info = opendal::raw::AccessorInfo::default();
            info.set_native_capability(opendal::Capability {
                list: true,
                ..Default::default()
            });
            info.into()
        }

        async fn list(
            &self,
            _path: &str,
            _args: opendal::raw::OpList,
        ) -> opendal::Result<(opendal::raw::RpList, Self::Lister)> {
            Ok((opendal::raw::RpList::default(), Box::new(FailingLister)))
        }
    }

    #[tokio::test]
    async fn list_object_entries_preserves_provider_error_kind() {
        let raw = Operator::from_inner(std::sync::Arc::new(FailingListService));
        let store = DataStore::with_operator_for_test(&encrypted_test_config(), raw).unwrap();
        let entries = store
            .list_object_entries("unit-volume/objects/")
            .await
            .unwrap();
        let mut entries = std::pin::pin!(entries);

        let error = entries.next().await.unwrap().unwrap_err();
        match error {
            DataError::OpenDal(error) => {
                assert_eq!(error.kind(), opendal::ErrorKind::Unexpected)
            }
            error => panic!("provider failure was misclassified: {error:?}"),
        }
    }

    #[tokio::test]
    async fn maintenance_operations_use_configured_data_prefix() {
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let mut config = encrypted_test_config();
        config.data_prefix = "tenant/demo".into();
        let store = DataStore::with_operator_for_test(&config, raw.clone()).unwrap();
        let key = "tenant/demo/objects/aa/bb/custom-prefix-object";

        store
            .put_by_key(key, Bytes::from_static(b"payload"))
            .await
            .unwrap();

        let entries = store.list_object_entries("tenant/demo/").await.unwrap();
        let mut entries = std::pin::pin!(entries);
        assert_eq!(entries.next().await.unwrap().unwrap().key, key);
        assert!(entries.next().await.is_none());

        store.delete_by_key(key).await.unwrap();
        assert!(!raw.exists(key).await.unwrap());
    }

    #[derive(Debug)]
    struct PrefixStatFailure(opendal::raw::Accessor);

    impl opendal::raw::Access for PrefixStatFailure {
        type Reader = opendal::raw::oio::Reader;
        type Writer = opendal::raw::oio::Writer;
        type Lister = opendal::raw::oio::Lister;
        type Deleter = opendal::raw::oio::Deleter;

        fn info(&self) -> Arc<opendal::raw::AccessorInfo> {
            self.0.info_dyn()
        }

        async fn stat(
            &self,
            _path: &str,
            _args: opendal::raw::OpStat,
        ) -> opendal::Result<opendal::raw::RpStat> {
            Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "prefix stat timed out",
            ))
        }

        async fn list(
            &self,
            path: &str,
            args: opendal::raw::OpList,
        ) -> opendal::Result<(opendal::raw::RpList, Self::Lister)> {
            self.0.list_dyn(path, args).await
        }

        async fn delete(&self) -> opendal::Result<(opendal::raw::RpDelete, Self::Deleter)> {
            self.0.delete_dyn().await
        }
    }

    #[tokio::test]
    async fn delete_volume_data_does_not_stat_prefix_or_delete_neighboring_keys() {
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let owned = [
            "unit-volume/objects/aa/bb/payload",
            "unit-volume/snapshots/marker",
        ];
        let neighbors = [
            "unit-volume-other/objects/payload",
            "unit-volume",
            "other/marker",
        ];
        for key in owned.into_iter().chain(neighbors) {
            raw.write(key, "payload").await.unwrap();
        }
        let op = Operator::from_inner(Arc::new(PrefixStatFailure(raw.clone().into_inner())));
        let store = DataStore::with_operator_for_test(&encrypted_test_config(), op).unwrap();

        store.delete_volume_data().await.unwrap();

        for key in owned {
            assert!(!raw.exists(key).await.unwrap(), "volume key remains: {key}");
        }
        for key in neighbors {
            assert!(
                raw.exists(key).await.unwrap(),
                "neighbor key deleted: {key}"
            );
        }
        store.delete_volume_data().await.unwrap();
    }

    #[tokio::test]
    async fn generic_object_lifecycle_uses_configured_data_prefix() {
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let mut config = encrypted_test_config();
        config.data_prefix = "tenant/demo".into();
        let store = DataStore::with_operator_for_test(&config, raw.clone()).unwrap();
        let object_id = "aabbccdd";
        let key = store.object_key(object_id);

        assert_eq!(key, "tenant/demo/aa/bb/aabbccdd");
        store
            .put_object(object_id, Bytes::from_static(b"payload"))
            .await
            .unwrap();
        assert!(raw.exists(&key).await.unwrap());

        store.delete_object(object_id).await.unwrap();
        assert!(!raw.exists(&key).await.unwrap());
    }

    #[tokio::test]
    async fn delete_by_key_rejects_key_outside_volume_scope() {
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store =
            DataStore::with_operator_for_test(&encrypted_test_config(), raw.clone()).unwrap();

        let error = store
            .delete_by_key("other-volume/object")
            .await
            .unwrap_err();

        assert!(matches!(error, DataError::InvalidObjectKey(_)));
    }

    #[tokio::test]
    async fn delete_by_key_rejects_path_segments() {
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store =
            DataStore::with_operator_for_test(&encrypted_test_config(), raw.clone()).unwrap();

        for key in [
            "unit-volume/../other-volume/object",
            "unit-volume\\objects\\object",
        ] {
            let error = store.delete_by_key(key).await.unwrap_err();

            assert!(matches!(error, DataError::InvalidObjectKey(_)));
        }
    }

    #[tokio::test]
    async fn list_object_entries_rejects_prefix_outside_volume_scope() {
        let raw = Operator::new(Memory::default()).unwrap().finish();
        let store =
            DataStore::with_operator_for_test(&encrypted_test_config(), raw.clone()).unwrap();

        let error = match store.list_object_entries("other-volume/objects/").await {
            Ok(_) => panic!("invalid list prefix must be rejected"),
            Err(error) => error,
        };

        assert!(matches!(error, DataError::InvalidObjectKey(_)));
    }
}
