//! `studiofs discover` — attach local metadata DB to an existing PG volume.

use clap::Args;
use std::path::Path;
use studiofs_meta::{
    FormatInfo, MetaError, PgSnapshotBackend, RedbMetaEngine, connect_studiofs_postgres,
    decode_format_info as decode_persisted_format_info, ensure_studiofs_database,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Meta(#[from] studiofs_meta::MetaError),
    #[error("{0}")]
    Message(String),
}

pub type DiscoveryResult<T> = std::result::Result<T, DiscoveryError>;

#[derive(Debug, Args)]
#[command(
    about = "Discover an existing PostgreSQL-backed volume and create local metadata for this client"
)]
#[command(long_about = "\
Download the latest PostgreSQL checkpoint for an existing shared volume and \
create the local metadata cache for this client.

Use this on additional machines or user profiles after the volume was created \
with `studiofs init`. Discovery installs one validated local checkpoint; runtime \
convergence starts later when the local metadata engine is mounted.")]
#[command(after_help = "\
EXAMPLES:
  Discover a volume from PostgreSQL:
    studiofs discover myvolume \\
      --meta postgres://studiofs:studiofs@127.0.0.1:5432
")]
pub(crate) struct DiscoverArgs {
    /// Volume name to discover
    name: String,

    /// PostgreSQL server DSN; StudioFS uses database `studiofs`
    #[arg(long = "meta")]
    meta: String,
}

impl DiscoverArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        let meta_dir = super::utils::get_default_meta_dir(&self.name)?;
        run(&self.name, &self.meta, &meta_dir)?;
        Ok(())
    }
}

struct DiscoveredState {
    volume_id: i64,
    checkpoint_version: i64,
    serving_checkpoint: Vec<u8>,
    format_info: FormatInfo,
}

const FETCH_REMOTE_VOLUME_ID_QUERY: &str = r#"
    SELECT v.vol
    FROM volumes v
    INNER JOIN metadata_state ms ON ms.vol = v.vol
    WHERE v.name = $1
    ORDER BY v.vol DESC
    LIMIT 1
    "#;

fn load_remote_discovery_state(name: &str, meta_dsn: &str) -> DiscoveryResult<DiscoveredState> {
    let (postgres_dsn, volume_id) = crate::runtime_bridge::block_on(async move {
        let postgres_dsn = ensure_studiofs_database(meta_dsn).await?.metadata_dsn;
        let volume_id = fetch_remote_volume_id(name, &postgres_dsn).await?;
        Ok::<_, DiscoveryError>((postgres_dsn, volume_id))
    })?;

    let backend = PgSnapshotBackend::open(&postgres_dsn, volume_id)?;
    let descriptor = backend
        .read_bootstrap_checkpoint()
        .map_err(|error| match error {
            MetaError::BootstrapCheckpointDescriptorMissing => {
                DiscoveryError::Message(format!(
                    "volume '{name}' not found or not initialized in metadata_state (run init + first sync)"
                ))
            }
            other => DiscoveryError::Meta(other),
        })?;
    let mut format_info = decode_persisted_format_info(&descriptor.format_info_bytes)?;
    let pg = format_info.metadata.postgres.as_mut().ok_or_else(|| {
        DiscoveryError::Message("discovered format is not postgres-backed".to_string())
    })?;
    pg.url = postgres_dsn;

    Ok(DiscoveredState {
        volume_id: descriptor.volume_id,
        checkpoint_version: descriptor.checkpoint_version,
        serving_checkpoint: descriptor.checkpoint_bytes,
        format_info,
    })
}

async fn fetch_remote_volume_id(name: &str, postgres_dsn: &str) -> DiscoveryResult<i64> {
    let (client, connection) = connect_studiofs_postgres(postgres_dsn).await?;
    let conn_task = tokio::spawn(connection);

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let row = client
            .query_opt(FETCH_REMOTE_VOLUME_ID_QUERY, &[&name])
            .await
            .map_err(|e| DiscoveryError::Message(format!("discover query failed: {e}")))?;

        let row = row.ok_or_else(|| {
            DiscoveryError::Message(format!(
                "volume '{name}' not found or not initialized in metadata_state (run init + first sync)"
            ))
        })?;

        row.try_get::<_, i64>("vol").map_err(|error| {
            DiscoveryError::Message(format!("discover volume id row parse failed: {error}"))
        })
    })
    .await
    .map_err(|_| DiscoveryError::Message("discovery volume query timed out".to_string()))
    .flatten();

    drop(client);
    conn_task.abort();
    let _ = conn_task.await;
    result
}

fn create_local_snapshot(
    name: &str,
    meta_dir: &Path,
    state: DiscoveredState,
) -> DiscoveryResult<std::path::PathBuf> {
    let DiscoveredState {
        checkpoint_version,
        serving_checkpoint,
        format_info,
        ..
    } = state;
    let checkpoint_version = u64::try_from(checkpoint_version).map_err(|_| {
        DiscoveryError::Message("discovered checkpoint version must not be negative".to_string())
    })?;

    std::fs::create_dir_all(meta_dir)?;
    let active_db_path = meta_dir.join(format!("{name}.db"));
    if active_db_path.exists() {
        return Err(DiscoveryError::Message(format!(
            "local metadata already exists at {} (remove it first or use another name)",
            active_db_path.display()
        )));
    }

    let temp_dir = tempfile::Builder::new()
        .prefix(".discover-")
        .tempdir_in(meta_dir)?;
    let temp_db_path = temp_dir.path().join(format!("{name}.db"));

    let engine = RedbMetaEngine::open(&temp_db_path)?;
    engine.initialize_discovered_checkpoint(
        &format_info,
        &serving_checkpoint,
        checkpoint_version,
    )?;
    drop(engine);

    // hard_link is atomic and fails if another process installed the active
    // database while this checkpoint was being staged.
    std::fs::hard_link(&temp_db_path, &active_db_path)?;

    Ok(active_db_path)
}

pub fn run(name: &str, meta_dsn: &str, meta_dir: &Path) -> DiscoveryResult<()> {
    let discovered = load_remote_discovery_state(name, meta_dsn)?;
    let volume_id = discovered.volume_id;
    let db_path = create_local_snapshot(name, meta_dir, discovered)?;
    println!("✓ Volume '{}' discovered", name);
    println!("  Local metadata: {}", db_path.display());
    println!("  Postgres volume_id: {}", volume_id);
    println!("Run `studiofs mount {} <mount-point>` to mount.", name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_meta::{
        FormatInfoDataStore, MetaInodes, MetadataBackend, MetadataConfig, PostgresConfig,
    };

    #[test]
    fn discover_parses_name_and_meta_dsn() {
        let cli = crate::cli::parse_for_test(&[
            "discover",
            "vol",
            "--meta",
            "postgres://studiofs:studiofs@127.0.0.1:5432",
        ])
        .unwrap();

        match cli.command {
            crate::cli::Commands::Discover(DiscoverArgs { name, meta }) => {
                assert_eq!(name, "vol");
                assert_eq!(meta, "postgres://studiofs:studiofs@127.0.0.1:5432");
            }
            other => panic!("expected Discover, got {other:?}"),
        }
    }

    #[test]
    fn discover_volume_query_requires_metadata_state() {
        assert!(FETCH_REMOTE_VOLUME_ID_QUERY.contains("INNER JOIN metadata_state"));
        assert!(FETCH_REMOTE_VOLUME_ID_QUERY.contains("ms.vol = v.vol"));
    }

    fn test_discovered_state(serving_checkpoint: Vec<u8>) -> DiscoveredState {
        let mut format_info = FormatInfo::new(
            "discovered".to_string(),
            "discovered-uuid".to_string(),
            FormatInfoDataStore {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "studiofs-test".to_string(),
                access_key: "test".to_string(),
                secret_key: "test".to_string(),
                region: "us-east-1".to_string(),
            },
            16,
        )
        .expect("create test format info");
        format_info.metadata = MetadataConfig {
            backend: MetadataBackend::Postgres,
            remote: None,
            postgres: Some(PostgresConfig {
                url: String::new(),
                volume_id: 7,
            }),
        };

        DiscoveredState {
            volume_id: 7,
            checkpoint_version: 42,
            serving_checkpoint,
            format_info,
        }
    }

    #[test]
    fn discover_installs_checkpoint_without_opening_runtime_engine() {
        let meta_dir = tempfile::tempdir().expect("create metadata directory");
        let checkpoint_dir = tempfile::tempdir().expect("create checkpoint source directory");
        let checkpoint_source_path = checkpoint_dir.path().join("source.db");
        let checkpoint_source =
            RedbMetaEngine::open(&checkpoint_source_path).expect("open checkpoint source");
        checkpoint_source
            .init_root(0, 0)
            .expect("initialize checkpoint source");
        let checkpoint = checkpoint_source
            .export_serving_checkpoint_bytes()
            .expect("encode serving checkpoint");
        drop(checkpoint_source);

        let db_path = create_local_snapshot(
            "discovered",
            meta_dir.path(),
            test_discovered_state(checkpoint),
        )
        .expect("install discovered checkpoint");

        let engine = RedbMetaEngine::open(&db_path).expect("open installed local database");
        assert_eq!(
            engine
                .get_counter("pgLastAppliedBatchVersion")
                .expect("read replay cursor"),
            42
        );
        assert_eq!(
            engine
                .get_counter("pgLastAppliedNamespaceBatchVersion")
                .expect("read namespace replay cursor"),
            42
        );
        assert_eq!(
            engine
                .get_counter("pgFreshClientPending")
                .expect("read fresh-client marker"),
            1
        );
        assert!(engine.get_attr(studiofs_meta::ROOT_INO).is_ok());
    }

    #[test]
    fn discover_failure_does_not_install_partial_active_database() {
        let meta_dir = tempfile::tempdir().expect("create metadata directory");

        let error = create_local_snapshot(
            "discovered",
            meta_dir.path(),
            test_discovered_state(b"invalid checkpoint".to_vec()),
        )
        .expect_err("invalid checkpoint must fail discovery");

        assert!(matches!(error, DiscoveryError::Meta(_)));
        assert!(!meta_dir.path().join("discovered.db").exists());
        assert_eq!(
            std::fs::read_dir(meta_dir.path())
                .expect("read metadata directory")
                .count(),
            0,
            "failed discovery must clean its temporary database"
        );
    }

    #[test]
    fn discover_rejects_checkpoint_without_root() {
        let meta_dir = tempfile::tempdir().expect("create metadata directory");
        let checkpoint_dir = tempfile::tempdir().expect("create checkpoint source directory");
        let checkpoint_source_path = checkpoint_dir.path().join("source.db");
        let checkpoint_source =
            RedbMetaEngine::open(&checkpoint_source_path).expect("open empty checkpoint source");
        let checkpoint = checkpoint_source
            .export_serving_checkpoint_bytes()
            .expect("encode empty serving checkpoint");
        drop(checkpoint_source);

        let error = create_local_snapshot(
            "discovered",
            meta_dir.path(),
            test_discovered_state(checkpoint),
        )
        .expect_err("checkpoint without root must fail discovery");

        assert!(matches!(error, DiscoveryError::Meta(_)));
        assert!(!meta_dir.path().join("discovered.db").exists());
    }

    #[test]
    fn discover_rejects_negative_checkpoint_before_touching_metadata_directory() {
        let meta_file = tempfile::NamedTempFile::new().expect("create metadata file");
        let mut state = test_discovered_state(Vec::new());
        state.checkpoint_version = -1;

        let error = create_local_snapshot("discovered", meta_file.path(), state)
            .expect_err("negative checkpoint version must fail before disk I/O");

        assert!(matches!(
            error,
            DiscoveryError::Message(message)
                if message == "discovered checkpoint version must not be negative"
        ));
    }

    #[test]
    fn discover_refuses_existing_active_database() {
        let meta_dir = tempfile::tempdir().expect("create metadata directory");
        let active_db_path = meta_dir.path().join("discovered.db");
        std::fs::write(&active_db_path, b"keep this database")
            .expect("create existing active database");

        let error = create_local_snapshot(
            "discovered",
            meta_dir.path(),
            test_discovered_state(Vec::new()),
        )
        .expect_err("existing active database must be preserved");

        assert!(
            matches!(error, DiscoveryError::Message(message) if message.contains("already exists"))
        );
        assert_eq!(
            std::fs::read(&active_db_path).expect("read existing active database"),
            b"keep this database"
        );
        assert_eq!(
            std::fs::read_dir(meta_dir.path())
                .expect("read metadata directory")
                .count(),
            1,
            "existing active database must not create staging files"
        );
    }
}
