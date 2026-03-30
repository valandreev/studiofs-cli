//! StudioFS Metadata Engine
//!
//! Provides metadata storage for the virtual filesystem using redb as the embedded KV store.
//! All filesystem structure (inodes, directory entries, chunk/slice mappings) is managed here.

pub mod async_engine;
pub mod engine;
pub mod events;
pub mod pg_dsn;
pub mod pg_engine;
pub mod redb_engine;
pub mod schema;
pub mod types;

use std::path::Path;
use std::sync::Arc;

pub use async_engine::{AsyncMetaEngine, SyncMetaEngineAdapter, execute_sync_meta};
pub use engine::{
    BatchFlushEntry, BatchFlushResult, MetaAcl, MetaBlocks, MetaEngine, MetaEngineCore, MetaError,
    MetaFormat, MetaFsOps, MetaGc, MetaInodes, MetaNamespace, MetaRecovery, MetaReplication,
    MetaResult, MetaSnapshots, MetaSymlink, MetaXattr,
};
pub use events::MetaEvent;
pub use pg_dsn::{
    STUDIOFS_METADATA_DB, StudioPgDsns, ensure_studiofs_database, resolve_studiofs_pg_dsns,
};
pub use pg_engine::{PgMetaEngine, PgOpenOptions};
pub use redb_engine::RedbMetaEngine;
pub use types::*;

#[derive(Debug, Clone, Copy)]
pub struct MetaOpenOptions {
    pub background_poll: bool,
}

impl Default for MetaOpenOptions {
    fn default() -> Self {
        Self {
            background_poll: true,
        }
    }
}

fn ensure_supported_format_version(info: &FormatInfo) -> MetaResult<()> {
    if info.format_version != CURRENT_FORMAT_VERSION {
        return Err(MetaError::UnsupportedFormatVersion {
            found: info.format_version,
            expected: CURRENT_FORMAT_VERSION,
        });
    }
    Ok(())
}

/// Open metadata engine based on format settings.
pub fn open_meta_engine(path: impl AsRef<Path>) -> MetaResult<Arc<dyn MetaEngine>> {
    open_meta_engine_with_options(path, MetaOpenOptions::default())
}

/// Open metadata engine with runtime behavior overrides.
pub fn open_meta_engine_with_options(
    path: impl AsRef<Path>,
    options: MetaOpenOptions,
) -> MetaResult<Arc<dyn MetaEngine>> {
    let path = path.as_ref();
    match RedbMetaEngine::read_format_info_readonly(path) {
        Ok(info) if info.metadata.backend == MetadataBackend::Postgres => {
            ensure_supported_format_version(&info)?;
            let pg = PgMetaEngine::open_with_options(
                path,
                PgOpenOptions {
                    background_poll: options.background_poll,
                },
            )?;
            let pg_info = pg.get_format_info()?;
            ensure_supported_format_version(&pg_info)?;
            if pg_info.metadata.backend != MetadataBackend::Postgres {
                return Err(MetaError::Storage(
                    "metadata backend switched during PgMetaEngine open".to_string(),
                ));
            }
            Ok(pg)
        }
        Ok(info) => {
            ensure_supported_format_version(&info)?;
            let redb = RedbMetaEngine::open(path)?;
            let redb_info = redb.get_format_info()?;
            ensure_supported_format_version(&redb_info)?;
            if redb_info.metadata.backend != info.metadata.backend {
                return Err(MetaError::Storage(
                    "metadata backend switched during RedbMetaEngine open".to_string(),
                ));
            }
            Ok(Arc::new(redb))
        }
        Err(MetaError::NotFormatted) => {
            let redb = RedbMetaEngine::open(path)?;
            Ok(Arc::new(redb))
        }
        Err(ref err) if err.is_redb_repair_needed() => {
            // ReadOnlyDatabase cannot repair an unclean shutdown.  Fall through
            // to Database::create() which automatically recovers from crashes,
            // power loss, and other unclean shutdowns (redb 3.x).
            tracing::warn!(
                "redb needs repair (read-only probe failed: {err}); \
                 attempting writable open for auto-recovery"
            );
            // Open writable to trigger redb auto-repair, read format info,
            // then drop before re-opening through the proper engine path.
            let info = {
                let redb = RedbMetaEngine::open(path)?;
                let info = redb.get_format_info()?;
                ensure_supported_format_version(&info)?;
                info
                // redb dropped here — releases file lock
            };
            if info.metadata.backend == MetadataBackend::Postgres {
                let pg = PgMetaEngine::open_with_options(
                    path,
                    PgOpenOptions {
                        background_poll: options.background_poll,
                    },
                )?;
                Ok(pg)
            } else {
                let redb = RedbMetaEngine::open(path)?;
                Ok(Arc::new(redb))
            }
        }
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_meta_engine_missing_file_creates_redb() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("missing-meta.db");
        assert!(!path.exists());

        let engine = open_meta_engine_with_options(
            &path,
            MetaOpenOptions {
                background_poll: false,
            },
        )
        .expect("open missing metadata path");

        let info = engine.get_format_info();
        assert!(matches!(info, Err(MetaError::NotFormatted)));
    }
}
