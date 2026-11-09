//! OpenDAL operations used by the standalone janitor.

use futures::TryStreamExt as _;
use opendal::services::S3;
use opendal::{EntryMode, ErrorKind, Operator, Result};

/// OpenDAL adapter for listing and mutating payload objects.
#[derive(Clone)]
pub struct JanitorObjectStore {
    operator: Operator,
}

/// Result category for deleting one fragment.
#[derive(Debug, thiserror::Error)]
pub enum JanitorDeleteError {
    /// The fragment was already absent.
    #[error("fragment not found")]
    NotFound,
    /// The provider rejected the delete request.
    #[error("{0}")]
    Other(String),
}

impl JanitorObjectStore {
    /// Wrap a configured object-storage operator.
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Configure a provider-neutral S3-compatible endpoint without loading user credentials.
    pub fn from_s3(
        endpoint: &str,
        bucket: &str,
        access_key_id: &str,
        secret_access_key: &str,
        region: &str,
    ) -> Result<Self> {
        let operator = Operator::new(
            S3::default()
                .endpoint(endpoint)
                .bucket(bucket)
                .access_key_id(access_key_id)
                .secret_access_key(secret_access_key)
                .region(if region.is_empty() {
                    "us-east-1"
                } else {
                    region
                })
                .disable_config_load()
                .disable_ec2_metadata(),
        )?
        .finish();
        Ok(Self::new(operator))
    }

    /// Start a lazy recursive listing below `prefix`; pages are fetched on demand.
    pub async fn list(&self, prefix: &str) -> Result<JanitorLister> {
        Ok(JanitorLister(
            self.operator.lister_with(prefix).recursive(true).await?,
        ))
    }

    /// Return the remote object's last-modified time when the provider supplies one.
    pub async fn last_modified(&self, key: &str) -> Result<Option<std::time::SystemTime>> {
        Ok(self
            .operator
            .stat(key)
            .await?
            .last_modified()
            .map(Into::into))
    }

    /// Delete one object fragment.
    pub async fn delete(&self, key: &str) -> std::result::Result<(), JanitorDeleteError> {
        self.operator.delete(key).await.map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                JanitorDeleteError::NotFound
            } else {
                JanitorDeleteError::Other(error.to_string())
            }
        })
    }
}

/// Streaming listing that holds at most one provider page in memory.
pub struct JanitorLister(opendal::Lister);

impl JanitorLister {
    /// Return the next listed entry, `None` once the listing is exhausted.
    pub async fn next(&mut self) -> Result<Option<JanitorEntry>> {
        Ok(self.0.try_next().await?.map(|entry| JanitorEntry {
            key: entry.path().to_string(),
            is_file: entry.metadata().mode() == EntryMode::FILE,
        }))
    }
}

/// One listed key and whether OpenDAL reports it as a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JanitorEntry {
    /// Object key relative to the operator root.
    pub key: String,
    /// Whether this entry represents a file rather than a directory.
    pub is_file: bool,
}
