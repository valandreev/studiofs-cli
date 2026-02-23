//! DataStore — S3 storage abstraction via OpenDAL.

use bytes::Bytes;
use opendal::layers::{ConcurrentLimitLayer, RetryLayer, TimeoutLayer};
use opendal::{Operator, services::S3};
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
#[derive(Debug, Clone)]
pub struct DataStoreConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub volume_name: String,
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
}

impl DataStore {
    /// Create a new DataStore connected to S3.
    pub fn new(config: &DataStoreConfig) -> DataResult<Self> {
        let mut builder = S3::default()
            .endpoint(&config.endpoint)
            .bucket(&config.bucket)
            .access_key_id(&config.access_key)
            .secret_access_key(&config.secret_key)
            .disable_config_load()
            .disable_ec2_metadata();

        if !config.region.is_empty() {
            builder = builder.region(&config.region);
        } else {
            // For MinIO and other S3-compatible services
            builder = builder.region("us-east-1");
        }

        let op = Operator::new(builder)?
            // TimeoutLayer MUST come before RetryLayer (see opendal docs).
            // - timeout: 30s for metadata ops (stat, delete)
            // - io_timeout: 300s per read/write — generous for 1 MiB blocks
            //   even on congested networks; truly hung connections time out.
            .layer(
                TimeoutLayer::new()
                    .with_timeout(Duration::from_secs(30))
                    .with_io_timeout(Duration::from_secs(300)),
            )
            // RetryLayer: exponential backoff with jitter for transient errors.
            .layer(
                RetryLayer::new()
                    .with_max_times(3)
                    .with_min_delay(Duration::from_millis(200))
                    .with_max_delay(Duration::from_secs(5))
                    .with_jitter(),
            )
            // Limit total concurrent HTTP requests to the backend.
            .layer(ConcurrentLimitLayer::new(256))
            .finish();
        info!(
            "connected to S3: endpoint={}, bucket={}",
            config.endpoint, config.bucket
        );

        Ok(Self {
            op,
            volume_name: config.volume_name.clone(),
        })
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
        let block_size = data.len() as u32;
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

        let data = self
            .op
            .read_with(&key)
            .range(offset..offset + length)
            .await?
            .to_bytes();
        Ok(data)
    }

    /// Upload a generic object to S3.
    pub async fn put_object(&self, object_id: &str, data: Bytes) -> DataResult<()> {
        let key = self.object_key(object_id);
        debug!("uploading object: {key} ({} bytes)", data.len());
        self.op.write(&key, data).await?;
        Ok(())
    }

    /// Upload bytes to an already-formatted storage key.
    pub async fn put_by_key(&self, key: &str, data: Bytes) -> DataResult<()> {
        debug!("uploading by key: {key} ({} bytes)", data.len());
        self.op.write(key, data).await?;
        Ok(())
    }

    /// Download a generic object from S3.
    pub async fn get_object(&self, object_id: &str) -> DataResult<Bytes> {
        let key = self.object_key(object_id);
        debug!("downloading object: {key}");
        let data = self.op.read(&key).await?.to_bytes();
        Ok(data)
    }

    /// Download bytes from an already-formatted storage key.
    pub async fn get_by_key(&self, key: &str) -> DataResult<Bytes> {
        debug!("downloading by key: {key}");
        let data = self.op.read(key).await?.to_bytes();
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
