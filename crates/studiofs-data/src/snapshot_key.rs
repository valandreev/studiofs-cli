//! Validated object keys for immutable Snapshot v2 artifacts.

use std::fmt;
use studiofs_meta::snapshot::{SnapshotId, VolumeId};
use studiofs_meta::snapshot_ports::{
    SnapshotArtifactKind, SnapshotArtifactLocator, SnapshotArtifactScope,
};
use thiserror::Error;
use uuid::Uuid;

/// Validation failures for Snapshot v2 storage keys.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum SnapshotArtifactKeyError {
    /// The configured data prefix is empty or path-unsafe.
    #[error("invalid snapshot artifact prefix")]
    InvalidPrefix,
    /// The volume identity is invalid.
    #[error("invalid snapshot artifact volume identity")]
    InvalidVolume,
    /// The public snapshot ID cannot be one path component.
    #[error("invalid snapshot artifact ID")]
    InvalidSnapshotId,
    /// Attempt/final kind and ordinal do not agree.
    #[error("invalid snapshot artifact kind")]
    InvalidKind,
    /// A listed key is outside the requested immutable snapshot scope.
    #[error("snapshot artifact key is outside its scope")]
    OutsideScope,
}

/// One validated immutable Snapshot v2 object key.
#[derive(Clone, Eq, PartialEq)]
pub struct SnapshotArtifactKey {
    key: String,
    locator: SnapshotArtifactLocator,
}

impl fmt::Debug for SnapshotArtifactKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SnapshotArtifactKey")
            .field(&self.key)
            .finish()
    }
}

impl SnapshotArtifactKey {
    /// Builds an attempt key below the configured data prefix.
    pub fn attempt(
        data_prefix: &str,
        volume_id: VolumeId,
        volume_uuid: &str,
        snapshot_id: &SnapshotId,
        attempt: u64,
    ) -> Result<Self, SnapshotArtifactKeyError> {
        Self::from_locator(
            data_prefix,
            &SnapshotArtifactLocator {
                volume_id,
                volume_uuid: volume_uuid.to_owned(),
                snapshot_id: snapshot_id.clone(),
                attempt: Some(attempt),
                kind: SnapshotArtifactKind::Attempt,
            },
        )
    }

    /// Builds the immutable final metadata artifact key.
    pub fn final_artifact(
        data_prefix: &str,
        volume_id: VolumeId,
        volume_uuid: &str,
        snapshot_id: &SnapshotId,
    ) -> Result<Self, SnapshotArtifactKeyError> {
        Self::from_locator(
            data_prefix,
            &SnapshotArtifactLocator {
                volume_id,
                volume_uuid: volume_uuid.to_owned(),
                snapshot_id: snapshot_id.clone(),
                attempt: None,
                kind: SnapshotArtifactKind::Metadata,
            },
        )
    }

    /// Builds a key from the backend-agnostic artifact locator.
    pub fn from_locator(
        data_prefix: &str,
        locator: &SnapshotArtifactLocator,
    ) -> Result<Self, SnapshotArtifactKeyError> {
        let prefix = SnapshotArtifactPrefix::for_snapshot(
            data_prefix,
            locator.volume_id,
            &locator.volume_uuid,
            &locator.snapshot_id,
        )?;
        let key = match (locator.kind, locator.attempt) {
            (SnapshotArtifactKind::Attempt, Some(attempt)) if attempt > 0 => {
                format!("{}attempts/{attempt}.part", prefix.as_str())
            }
            (SnapshotArtifactKind::Metadata, None) => {
                format!("{}metadata.redb", prefix.as_str())
            }
            _ => return Err(SnapshotArtifactKeyError::InvalidKind),
        };
        Ok(Self {
            key,
            locator: SnapshotArtifactLocator {
                volume_uuid: prefix.volume_uuid,
                ..locator.clone()
            },
        })
    }

    /// Returns the exact validated storage key.
    pub fn as_str(&self) -> &str {
        &self.key
    }

    /// Returns the backend-agnostic locator represented by this key.
    pub fn locator(&self) -> SnapshotArtifactLocator {
        self.locator.clone()
    }
}

/// Validated listing scope for one immutable snapshot.
#[derive(Clone, Eq, PartialEq)]
pub struct SnapshotArtifactPrefix {
    prefix: String,
    volume_id: VolumeId,
    volume_uuid: String,
    snapshot_id: SnapshotId,
}

impl fmt::Debug for SnapshotArtifactPrefix {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SnapshotArtifactPrefix")
            .field(&self.prefix)
            .finish()
    }
}

impl SnapshotArtifactPrefix {
    /// Builds a listing scope from verified volume identity and SnapshotId.
    pub fn for_snapshot(
        data_prefix: &str,
        volume_id: VolumeId,
        volume_uuid: &str,
        snapshot_id: &SnapshotId,
    ) -> Result<Self, SnapshotArtifactKeyError> {
        let prefix = normalize_data_prefix(data_prefix)?;
        if volume_id <= 0 {
            return Err(SnapshotArtifactKeyError::InvalidVolume);
        }
        let volume_uuid = canonical_uuid(volume_uuid)?;
        validate_component(snapshot_id.as_str())?;
        Ok(Self {
            prefix: format!(
                "{prefix}metadata/snapshots/{volume_uuid}/{}/",
                snapshot_id.as_str()
            ),
            volume_id,
            volume_uuid,
            snapshot_id: snapshot_id.clone(),
        })
    }

    /// Builds a listing scope from a backend-agnostic snapshot scope.
    pub fn from_scope(
        data_prefix: &str,
        scope: &SnapshotArtifactScope,
    ) -> Result<Self, SnapshotArtifactKeyError> {
        Self::for_snapshot(
            data_prefix,
            scope.volume_id,
            &scope.volume_uuid,
            &scope.snapshot_id,
        )
    }

    /// Returns the exact recursive listing prefix.
    pub fn as_str(&self) -> &str {
        &self.prefix
    }

    /// Parses one listed key and rejects keys outside this snapshot scope.
    pub fn parse_key(
        &self,
        key: &str,
    ) -> Result<SnapshotArtifactLocator, SnapshotArtifactKeyError> {
        let relative = key
            .strip_prefix(&self.prefix)
            .ok_or(SnapshotArtifactKeyError::OutsideScope)?;
        if relative == "metadata.redb" {
            return Ok(SnapshotArtifactLocator {
                volume_id: self.volume_id,
                volume_uuid: self.volume_uuid.clone(),
                snapshot_id: self.snapshot_id.clone(),
                attempt: None,
                kind: SnapshotArtifactKind::Metadata,
            });
        }
        let attempt = relative
            .strip_prefix("attempts/")
            .and_then(|value| value.strip_suffix(".part"))
            .filter(|value| !value.is_empty() && !value.contains('/'))
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|attempt| *attempt > 0)
            .ok_or(SnapshotArtifactKeyError::OutsideScope)?;
        Ok(SnapshotArtifactLocator {
            volume_id: self.volume_id,
            volume_uuid: self.volume_uuid.clone(),
            snapshot_id: self.snapshot_id.clone(),
            attempt: Some(attempt),
            kind: SnapshotArtifactKind::Attempt,
        })
    }
}

fn normalize_data_prefix(data_prefix: &str) -> Result<String, SnapshotArtifactKeyError> {
    if data_prefix.is_empty() || data_prefix.contains('\\') || data_prefix.contains('\0') {
        return Err(SnapshotArtifactKeyError::InvalidPrefix);
    }
    if data_prefix == "/" {
        return Ok("/".to_owned());
    }
    let trimmed = data_prefix.trim_end_matches('/');
    let mut segments = trimmed.split('/');
    if trimmed.starts_with('/') {
        segments.next();
    }
    if trimmed.is_empty()
        || segments.any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(SnapshotArtifactKeyError::InvalidPrefix);
    }
    Ok(format!("{trimmed}/"))
}

fn canonical_uuid(value: &str) -> Result<String, SnapshotArtifactKeyError> {
    let uuid = Uuid::parse_str(value).map_err(|_| SnapshotArtifactKeyError::InvalidVolume)?;
    let canonical = uuid.hyphenated().to_string();
    (canonical == value)
        .then_some(canonical)
        .ok_or(SnapshotArtifactKeyError::InvalidVolume)
}

fn validate_component(value: &str) -> Result<(), SnapshotArtifactKeyError> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains('\0')
        || value.chars().any(char::is_control)
    {
        return Err(SnapshotArtifactKeyError::InvalidSnapshotId);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{DataStore, DataStoreConfig, DataStoreCryptoConfig};
    use bytes::Bytes;
    use futures::StreamExt;
    use opendal::services::Memory;
    use studiofs_meta::snapshot_ports::{
        SnapshotArtifactKind, SnapshotArtifactLocator, SnapshotArtifactScope,
    };
    use studiofs_meta::{
        EncryptionConfig,
        snapshot::{SnapshotId, VolumeId},
    };

    const SNAPSHOT_ID: &str = "projectfs.2026-08-13T12-00-00.000Z";
    const UUID_A: &str = "550e8400-e29b-41d4-a716-446655440000";
    const UUID_B: &str = "550e8400-e29b-41d4-a716-446655440001";

    #[test]
    fn custom_prefix_round_trips_attempt_and_final_keys() {
        let snapshot_id = SnapshotId::parse(SNAPSHOT_ID).expect("snapshot id");
        let prefix = SnapshotArtifactPrefix::for_snapshot("tenant/demo/", 7, UUID_A, &snapshot_id)
            .expect("prefix");
        assert_eq!(
            prefix.as_str(),
            "tenant/demo/metadata/snapshots/550e8400-e29b-41d4-a716-446655440000/projectfs.2026-08-13T12-00-00.000Z/"
        );

        let attempt = SnapshotArtifactKey::attempt("tenant/demo/", 7, UUID_A, &snapshot_id, 3)
            .expect("attempt key");
        assert_eq!(
            attempt.as_str(),
            "tenant/demo/metadata/snapshots/550e8400-e29b-41d4-a716-446655440000/projectfs.2026-08-13T12-00-00.000Z/attempts/3.part"
        );
        assert_eq!(
            prefix.parse_key(attempt.as_str()).expect("parse attempt"),
            attempt.locator()
        );

        let final_key =
            SnapshotArtifactKey::final_artifact("tenant/demo/", 7, UUID_A, &snapshot_id)
                .expect("final key");
        assert_eq!(
            final_key.as_str(),
            "tenant/demo/metadata/snapshots/550e8400-e29b-41d4-a716-446655440000/projectfs.2026-08-13T12-00-00.000Z/metadata.redb"
        );
        assert_eq!(
            prefix.parse_key(final_key.as_str()).expect("parse final"),
            final_key.locator()
        );
    }

    #[test]
    fn leading_slash_prefix_matches_data_store_normalization() {
        let snapshot_id = SnapshotId::parse(SNAPSHOT_ID).expect("snapshot id");
        let prefix = SnapshotArtifactPrefix::for_snapshot("/tenant/demo/", 7, UUID_A, &snapshot_id)
            .expect("leading slash prefix");
        assert!(
            prefix
                .as_str()
                .starts_with("/tenant/demo/metadata/snapshots/")
        );
    }

    #[test]
    fn rejects_traversal_and_cross_volume_keys() {
        let traversal_id = SnapshotId::parse("bad/name.2026-08-13T12-00-00.000Z")
            .expect("domain parser accepts the public ID shape");
        assert!(
            SnapshotArtifactPrefix::for_snapshot("tenant/../", 7, UUID_A, &traversal_id).is_err()
        );

        let safe_id = SnapshotId::parse(SNAPSHOT_ID).expect("snapshot id");
        assert!(SnapshotArtifactPrefix::for_snapshot("tenant/../", 7, UUID_A, &safe_id).is_err());
        assert!(SnapshotArtifactKey::attempt("tenant/demo/", 7, UUID_A, &safe_id, 0).is_err());

        let scope = SnapshotArtifactScope {
            volume_id: 7,
            volume_uuid: UUID_A.to_string(),
            snapshot_id: safe_id.clone(),
        };
        let other = SnapshotArtifactKey::final_artifact("tenant/demo/", 7, UUID_B, &safe_id)
            .expect("other volume key");
        let prefix = SnapshotArtifactPrefix::from_scope("tenant/demo/", &scope).expect("scope");
        assert!(prefix.parse_key(other.as_str()).is_err());
    }

    #[test]
    fn locator_rendering_keeps_attempt_and_kind_consistent() {
        let snapshot_id = SnapshotId::parse(SNAPSHOT_ID).expect("snapshot id");
        let locator = SnapshotArtifactLocator {
            volume_id: 7,
            volume_uuid: UUID_A.to_string(),
            snapshot_id: snapshot_id.clone(),
            attempt: Some(2),
            kind: SnapshotArtifactKind::Attempt,
        };
        let key = SnapshotArtifactKey::from_locator("tenant/demo/", &locator).expect("key");
        assert_eq!(key.locator(), locator);

        let invalid_volume: VolumeId = 0;
        let invalid = SnapshotArtifactLocator {
            volume_id: invalid_volume,
            ..locator
        };
        assert!(SnapshotArtifactKey::from_locator("tenant/demo/", &invalid).is_err());
    }

    #[tokio::test]
    async fn typed_datastore_round_trips_custom_prefix_and_list() {
        let config = DataStoreConfig {
            endpoint: "memory://".to_owned(),
            bucket: "snapshot-tests".to_owned(),
            access_key: "access".to_owned(),
            secret_key: "secret".to_owned(),
            region: "us-east-1".to_owned(),
            volume_name: "projectfs".to_owned(),
            data_prefix: "tenant/demo/".to_owned(),
            volume_uuid: UUID_A.to_owned(),
            crypto: DataStoreCryptoConfig::from(EncryptionConfig::new([0x42; 32])),
        };
        let operator = opendal::Operator::new(Memory::default())
            .expect("memory operator")
            .finish();
        let store = DataStore::with_operator_for_test(&config, operator).expect("data store");
        let snapshot_id = SnapshotId::parse(SNAPSHOT_ID).expect("snapshot id");
        let key = SnapshotArtifactKey::final_artifact("tenant/demo/", 7, UUID_A, &snapshot_id)
            .expect("final key");
        let prefix = SnapshotArtifactPrefix::for_snapshot("tenant/demo/", 7, UUID_A, &snapshot_id)
            .expect("prefix");

        store
            .put_snapshot_artifact(&key, Bytes::from_static(b"metadata"))
            .await
            .expect("put artifact");
        assert_eq!(
            store
                .get_snapshot_artifact(&key)
                .await
                .expect("get artifact")
                .to_bytes(),
            b"metadata".as_slice()
        );
        let mut entries = store
            .list_snapshot_artifacts(&prefix)
            .await
            .expect("list artifacts");
        let entry = entries
            .next()
            .await
            .expect("listed artifact")
            .expect("entry");
        assert_eq!(entry.key, key.as_str());
        store
            .delete_snapshot_artifact(&key)
            .await
            .expect("delete artifact");
        assert!(store.get_snapshot_artifact(&key).await.is_err());
    }
}
