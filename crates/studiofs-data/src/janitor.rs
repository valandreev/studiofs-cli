//! OpenDAL operations used by the standalone janitor.

use std::time::{Duration, SystemTime};

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

    /// Whether this provider can resume its listing after one key.
    pub fn supports_start_after(&self) -> bool {
        self.operator.info().full_capability().list_with_start_after
    }

    /// Start a lazy recursive listing below `prefix`.
    pub async fn list(&self, prefix: &str) -> Result<JanitorLister> {
        self.list_after(prefix, None).await
    }

    /// Start a lazy recursive listing below `prefix`, optionally after one key.
    pub async fn list_after(&self, prefix: &str, after_key: Option<&str>) -> Result<JanitorLister> {
        let mut lister = self.operator.lister_with(prefix);
        if let Some(after_key) = after_key {
            if !self.supports_start_after() {
                return Err(opendal::Error::new(
                    ErrorKind::Unsupported,
                    "provider cannot resume listings after a key",
                ));
            }
            lister = lister.start_after(after_key);
        }
        Ok(JanitorLister(lister.recursive(true).await?))
    }

    /// Return the remote object's last-modified time when the provider supplies one.
    pub async fn last_modified(&self, key: &str) -> Result<Option<SystemTime>> {
        Ok(self
            .operator
            .stat(key)
            .await?
            .last_modified()
            .map(Into::into))
    }

    /// Return whether an exact canonical key exists and, when available, its age.
    pub async fn stat_key(&self, key: &str) -> Result<JanitorKeyStat> {
        match self.operator.stat(key).await {
            Ok(metadata) => Ok(JanitorKeyStat::Found(
                metadata.last_modified().map(Into::into),
            )),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(JanitorKeyStat::Missing),
            Err(error) => Err(error),
        }
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

/// Result of statting one exact object key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JanitorKeyStat {
    /// The object is absent.
    Missing,
    /// The object exists; its timestamp may be unavailable.
    Found(Option<SystemTime>),
}

/// Streaming listing that holds at most one provider page in memory.
pub struct JanitorLister(opendal::Lister);

impl JanitorLister {
    /// Return the next listed entry, `None` once the listing is exhausted.
    pub async fn next(&mut self) -> Result<Option<JanitorEntry>> {
        Ok(self.0.try_next().await?.map(|entry| JanitorEntry {
            key: entry.path().to_string(),
            is_file: entry.metadata().mode() == EntryMode::FILE,
            last_modified: entry.metadata().last_modified().map(Into::into),
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
    /// `LastModified` from the listing page, when the provider returned one.
    pub last_modified: Option<SystemTime>,
}

/// Grace-period verdict for one orphan candidate; a greater verdict is stricter,
/// so `max` combines two observations conservatively.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JanitorAge {
    /// Older than the grace period; deletion may proceed.
    Eligible,
    /// Inside the grace period.
    Young,
    /// Missing or future-dated; never infer an age from it.
    Unknown,
}

/// Classify an object's age against `grace`; a timestamp after `now` is `Unknown`.
pub fn janitor_age(now: SystemTime, observed: Option<SystemTime>, grace: Duration) -> JanitorAge {
    match observed.map(|observed| now.duration_since(observed)) {
        Some(Ok(age)) if age >= grace => JanitorAge::Eligible,
        Some(Ok(_)) => JanitorAge::Young,
        None | Some(Err(_)) => JanitorAge::Unknown,
    }
}

/// [`janitor_age`] for a PostgreSQL pending-deletion timestamp in Unix seconds.
pub fn pending_deletion_age(now_unix: i64, deleted_at_unix: i64, grace: Duration) -> JanitorAge {
    match u64::try_from(now_unix.saturating_sub(deleted_at_unix)) {
        Ok(age) if age >= grace.as_secs() => JanitorAge::Eligible,
        Ok(_) => JanitorAge::Young,
        Err(_) => JanitorAge::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn janitor_age_skips_missing_future_and_young_timestamps() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let grace = Duration::from_secs(100);
        let at = |secs| Some(SystemTime::UNIX_EPOCH + Duration::from_secs(secs));
        assert_eq!(janitor_age(now, None, grace), JanitorAge::Unknown);
        assert_eq!(janitor_age(now, at(1_001), grace), JanitorAge::Unknown);
        assert_eq!(janitor_age(now, at(950), grace), JanitorAge::Young);
        assert_eq!(janitor_age(now, at(900), grace), JanitorAge::Eligible);
        assert_eq!(
            pending_deletion_age(1_000, 1_001, grace),
            JanitorAge::Unknown
        );
        assert_eq!(pending_deletion_age(1_000, 950, grace), JanitorAge::Young);
        assert_eq!(
            pending_deletion_age(1_000, 900, grace),
            JanitorAge::Eligible
        );
    }
}
