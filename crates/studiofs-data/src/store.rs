//! DataStore — S3 storage abstraction via OpenDAL.

use bytes::Bytes;
use opendal::layers::{ConcurrentLimitLayer, RetryLayer, TimeoutLayer};
use opendal::{Buffer, Operator, services::S3};
use std::fmt;
use std::time::Duration;
use studiofs_meta::FormatInfo;
use thiserror::Error;
use tracing::{debug, info};

#[derive(Debug, Error)]
pub enum DataError {
    #[error("storage error: {0}")]
    Storage(String),

    #[error("opendal error: {0}")]
    OpenDal(#[from] opendal::Error),
}

pub type DataResult<T> = Result<T, DataError>;

/// Configuration for the data store.
#[derive(Clone)]
pub struct DataStoreConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub volume_name: String,
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
        }
    }
}

/// DataStore — manages block-level I/O to S3-compatible storage.
pub struct DataStore {
    op: Operator,
    volume_name: String,
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

impl DataStore {
    /// Create a new DataStore with the default I/O profile.
    pub fn new(config: &DataStoreConfig) -> DataResult<Self> {
        Self::with_profile(config, IoProfile::Default)
    }

    /// Create a DataStore with a specific I/O profile.
    pub fn with_profile(config: &DataStoreConfig, profile: IoProfile) -> DataResult<Self> {
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
            info!("using path-style addressing for IP-based S3 endpoint");
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
            config: config.clone(),
            profile,
        })
    }

    /// Create a read-optimised clone with its own connection pool.
    ///
    /// The new store connects to the same endpoint/bucket but uses tighter
    /// timeouts and a dedicated concurrency limit so that background uploads
    /// can never starve read requests.
    pub fn clone_for_reads(&self) -> DataResult<Self> {
        Self::with_profile(&self.config, IoProfile::Read)
    }

    /// Create a write-optimised clone with its own connection pool.
    pub fn clone_for_writes(&self) -> DataResult<Self> {
        Self::with_profile(&self.config, IoProfile::Write)
    }

    /// The I/O profile this store was created with.
    pub fn profile(&self) -> IoProfile {
        self.profile
    }

    /// Get the S3 object key for a block.
    ///
    /// Format: `{volume}/data/chunks/{hash}/{sliceId}_{blockIndex}_{blockSize}`
    pub fn block_key(&self, slice_id: u64, block_index: u32, block_size: u32) -> String {
        let hash = self.hash_path(slice_id);
        format!(
            "{}/data/chunks/{}/{}_{}_{}",
            self.volume_name, hash, slice_id, block_index, block_size
        )
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

    /// Hash path for distributing objects (same as JuiceFS without HashPrefix).
    fn hash_path(&self, slice_id: u64) -> String {
        format!("{}/{}", slice_id / 1_000_000, slice_id / 1_000)
    }

    /// Upload a block to S3.
    pub async fn put_block(&self, slice_id: u64, block_index: u32, data: Bytes) -> DataResult<()> {
        let block_size = u32::try_from(data.len())
            .map_err(|_| DataError::Storage("block payload size exceeds u32".into()))?;
        let key = self.block_key(slice_id, block_index, block_size);
        debug!("uploading block: {key} ({block_size} bytes)");

        self.op.write(&key, data).await?;
        Ok(())
    }

    /// Download a block from S3.
    pub async fn get_block(
        &self,
        slice_id: u64,
        block_index: u32,
        block_size: u32,
    ) -> DataResult<Bytes> {
        let key = self.block_key(slice_id, block_index, block_size);
        debug!("downloading block: {key}");

        let data = self.op.read(&key).await?.to_bytes();
        Ok(data)
    }

    /// Download a range within a block from S3.
    pub async fn get_block_range(
        &self,
        slice_id: u64,
        block_index: u32,
        block_size: u32,
        offset: u64,
        length: u64,
    ) -> DataResult<Bytes> {
        let key = self.block_key(slice_id, block_index, block_size);
        debug!("downloading block range: {key} offset={offset} len={length}");
        let range_end = offset
            .checked_add(length)
            .ok_or_else(|| DataError::Storage("block range offset + length overflow".into()))?;

        let data = self
            .op
            .read_with(&key)
            .range(offset..range_end)
            .await?
            .to_bytes();
        Ok(data)
    }

    /// Upload a generic object to S3.
    pub async fn put_object(&self, object_id: &str, data: impl Into<Buffer>) -> DataResult<()> {
        let data: Buffer = data.into();
        let key = self.object_key(object_id);
        debug!("uploading object: {key} ({} bytes)", data.len());
        self.op.write(&key, data).await?;
        Ok(())
    }

    /// Upload bytes to an already-formatted storage key.
    ///
    /// Accepts both contiguous (`Bytes`) and non-contiguous (`Buffer`) payloads.
    /// Note: some service implementations may still materialize into contiguous
    /// buffers internally if vectored writes are unsupported.
    pub async fn put_by_key(&self, key: &str, data: impl Into<Buffer>) -> DataResult<()> {
        let data: Buffer = data.into();
        debug!("uploading by key: {key} ({} bytes)", data.len());
        self.op.write(key, data).await?;
        Ok(())
    }

    /// Download a generic object from S3.
    pub async fn get_object(&self, object_id: &str) -> DataResult<Buffer> {
        let key = self.object_key(object_id);
        debug!("downloading object: {key}");
        let data = self.op.read(&key).await?;
        Ok(data)
    }

    /// Download bytes from an already-formatted storage key.
    pub async fn get_by_key(&self, key: &str) -> DataResult<Buffer> {
        debug!("downloading by key: {key}");
        let data = self.op.read(key).await?;
        Ok(data)
    }

    /// Delete a block from S3.
    pub async fn delete_block(
        &self,
        slice_id: u64,
        block_index: u32,
        block_size: u32,
    ) -> DataResult<()> {
        let key = self.block_key(slice_id, block_index, block_size);
        debug!("deleting block: {key}");

        self.op.delete(&key).await?;
        Ok(())
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

    /// Check if a block exists in S3.
    pub async fn block_exists(
        &self,
        slice_id: u64,
        block_index: u32,
        block_size: u32,
    ) -> DataResult<bool> {
        let key = self.block_key(slice_id, block_index, block_size);
        Ok(self.op.exists(&key).await?)
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
