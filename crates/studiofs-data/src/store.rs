//! DataStore — S3 storage abstraction via OpenDAL.

use crate::crypto::{self, ObjectEncryptionContext, VolumeMasterKey};
use opendal::layers::{ConcurrentLimitLayer, RetryLayer, TimeoutLayer};
use opendal::{Buffer, Operator, services::S3};
use std::collections::HashSet;
use std::fmt;
use std::net::Ipv6Addr;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use studiofs_meta::{EncryptionConfig, FormatInfo};
use thiserror::Error;
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
            Self::InvalidEndpoint(_) | Self::InvalidCryptoConfig(_) | Self::Crypto(_) => false,
        }
    }
}

/// Result type for all datastore operations.
pub type DataResult<T> = Result<T, DataError>;

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
    /// Plaintext STFS object bytes returned at the DataStore boundary.
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
    fn to_runtime(&self) -> DataResult<DataStoreCryptoRuntime> {
        DataStoreCryptoRuntime::try_from(self)
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
    master_key: VolumeMasterKey,
    key_id: u32,
}

impl TryFrom<&DataStoreCryptoConfig> for DataStoreCryptoRuntime {
    type Error = DataError;

    fn try_from(config: &DataStoreCryptoConfig) -> Result<Self, Self::Error> {
        config.encryption.validate()?;
        Ok(Self {
            master_key: VolumeMasterKey::from_bytes(config.encryption.volume_master_key),
            key_id: config.encryption.active_key_id,
        })
    }
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
            volume_uuid: info.uuid.clone(),
            crypto: DataStoreCryptoConfig::from(&info.encryption),
        }
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
    volume_uuid: String,
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
        Self::with_profile_and_crypto(config, profile, config.crypto.to_runtime()?)
    }

    fn with_profile_and_crypto(
        config: &DataStoreConfig,
        profile: IoProfile,
        crypto: DataStoreCryptoRuntime,
    ) -> DataResult<Self> {
        let mut config = config.clone();
        config.endpoint = normalize_s3_endpoint_transport(&config.endpoint)?;

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
            volume_uuid: config.volume_uuid.clone(),
            crypto,
            config,
            profile,
        })
    }

    /// Create a DataStore around an already-built operator for focused tests.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_operator_for_test(config: &DataStoreConfig, op: Operator) -> DataResult<Self> {
        Ok(Self {
            op,
            volume_name: config.volume_name.clone(),
            volume_uuid: config.volume_uuid.clone(),
            crypto: config.crypto.to_runtime()?,
            config: config.clone(),
            profile: IoProfile::Default,
        })
    }

    /// Create a read-optimised clone with its own connection pool.
    ///
    /// The new store connects to the same endpoint/bucket but uses tighter
    /// timeouts and a dedicated concurrency limit so that background uploads
    /// can never starve read requests.
    pub fn clone_for_reads(&self) -> DataResult<Self> {
        Self::with_profile_and_crypto(&self.config, IoProfile::Read, self.crypto.clone())
    }

    /// Create a write-optimised clone with its own connection pool.
    pub fn clone_for_writes(&self) -> DataResult<Self> {
        Self::with_profile_and_crypto(&self.config, IoProfile::Write, self.crypto.clone())
    }

    /// The I/O profile this store was created with.
    pub fn profile(&self) -> IoProfile {
        self.profile
    }

    /// Get the S3 object key for a generic object (content-addressable).
    /// Format: `{volume}/objects/{ab}/{cd}/{id}`
    pub fn object_key(&self, object_id: &str) -> String {
        if object_id.len() < 4 {
            return format!("{}/objects/{}", self.volume_name, object_id);
        }
        format!(
            "{}/objects/{}/{}/{}",
            self.volume_name,
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

    /// Upload bytes to an already-formatted storage key and return crypto-safe byte counters.
    pub async fn put_by_key_with_stats(
        &self,
        key: &str,
        data: impl Into<Buffer>,
    ) -> DataResult<RemoteObjectWriteStats> {
        let data: Buffer = data.into();
        let plaintext_len = data.len();
        let encrypted = self.encrypt_payload(key, data)?;
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

    /// Download bytes from an already-formatted storage key and return crypto-safe byte counters.
    pub async fn get_by_key_with_stats(
        &self,
        key: &str,
    ) -> DataResult<(Buffer, RemoteObjectReadStats)> {
        debug!("downloading by key: {key}");
        let data = self.op.read(key).await?;
        let encrypted_len = data.len();
        let plaintext = self.decrypt_payload(key, data)?;
        let plaintext_len = plaintext.len();
        Ok((
            plaintext,
            RemoteObjectReadStats {
                encrypted_bytes: encrypted_len as u64,
                plaintext_bytes: plaintext_len as u64,
            },
        ))
    }

    /// Delete a generic object from S3.
    pub async fn delete_object(&self, object_id: &str) -> DataResult<()> {
        let key = self.object_key(object_id);
        debug!("deleting object: {key}");
        self.op.delete(&key).await?;
        Ok(())
    }

    /// Delete an object by an already-formatted storage key.
    pub async fn delete_by_key(&self, key: &str) -> DataResult<()> {
        debug!("deleting by key: {key}");
        self.op.delete(key).await?;
        Ok(())
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

        // Delete all objects with the given prefix
        self.op.remove_all(&prefix).await?;

        Ok(())
    }

    /// Get the underlying operator (for advanced use).
    pub fn operator(&self) -> &Operator {
        &self.op
    }

    fn crypto_context<'a>(&'a self, key: &'a str) -> ObjectEncryptionContext<'a> {
        ObjectEncryptionContext {
            volume_uuid: &self.volume_uuid,
            storage_key: key,
            key_id: self.crypto.key_id,
        }
    }

    fn encrypt_payload(&self, key: &str, plaintext: Buffer) -> DataResult<Buffer> {
        // SFS-351 wires the complete-object crypto boundary; chunked encryption
        // belongs to the follow-up serializer path once the crypto API supports it.
        let plaintext = plaintext.to_bytes();
        crypto::encrypt_object(
            plaintext.as_ref(),
            &self.crypto.master_key,
            self.crypto_context(key),
        )
        .map_err(DataError::from)
    }

    fn decrypt_payload(&self, key: &str, ciphertext: Buffer) -> DataResult<Buffer> {
        let plaintext = crypto::decrypt_object(
            ciphertext.to_bytes(),
            &self.crypto.master_key,
            self.crypto_context(key),
        )?;
        Ok(Buffer::from(plaintext))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::SFEC_MAGIC;
    use crate::object::{MAGIC as STFS_MAGIC, create_object_buffer, read_object};
    use bytes::Bytes;
    use opendal::services::Memory;
    use studiofs_meta::EncryptionConfig;

    fn encrypted_test_config() -> DataStoreConfig {
        DataStoreConfig {
            endpoint: "memory://".into(),
            bucket: "unit-test".into(),
            access_key: "access".into(),
            secret_key: "secret".into(),
            region: "us-east-1".into(),
            volume_name: "unit-volume".into(),
            volume_uuid: "1a4c0fe2-8550-4892-952c-5ec26c0e0d8d".into(),
            crypto: DataStoreCryptoConfig::from(EncryptionConfig::new([0x42; 32])),
        }
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
}
