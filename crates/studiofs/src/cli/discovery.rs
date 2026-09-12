//! `studiofs discover` — attach local metadata DB to an existing PG volume.

use clap::Args;
use std::path::Path;
use studiofs_meta::{
    FormatInfo, MetaError, MetaFormat, MetaOpenOptions, PgSnapshotBackend, RedbMetaEngine,
    connect_studiofs_postgres, decode_format_info as decode_persisted_format_info,
    ensure_studiofs_database, open_meta_engine_with_options,
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
with `studiofs init`. Runtime convergence after discovery continues through \
PostgreSQL delta batches.")]
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
        let postgres_dsn = ensure_studiofs_database(meta_dsn)
            .await
            .map_err(|e| DiscoveryError::Message(e.to_string()))?
            .metadata_dsn;
        let volume_id = fetch_remote_volume_id(name, &postgres_dsn).await?;
        Ok::<_, DiscoveryError>((postgres_dsn, volume_id))
    })?;

    let backend = PgSnapshotBackend::open(&postgres_dsn, volume_id)?;
    let descriptor = backend
        .read_bootstrap_checkpoint()
        .map_err(|error| match error {
            MetaError::Storage(message)
                if message == "postgres bootstrap checkpoint descriptor is missing" =>
            {
                DiscoveryError::Message(format!(
                    "volume '{name}' not found or not initialized in metadata_state (run init + first sync)"
                ))
            }
            other => DiscoveryError::Meta(other),
        })?;
    let mut format_info = decode_persisted_format_info(&descriptor.format_info_bytes)?;
    let pg = format_info
        .metadata
        .postgres
        .as_mut()
        .expect("descriptor validation guarantees postgres backend configuration");
    pg.url = postgres_dsn;

    Ok(DiscoveredState {
        volume_id: descriptor.volume_id,
        serving_checkpoint: descriptor.checkpoint_bytes,
        format_info,
    })
}

async fn fetch_remote_volume_id(name: &str, postgres_dsn: &str) -> DiscoveryResult<i64> {
    let (client, connection) = connect_studiofs_postgres(postgres_dsn)
        .await
        .map_err(|e| DiscoveryError::Message(e.to_string()))?;
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

        Ok::<i64, DiscoveryError>(row.get(0))
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
    std::fs::create_dir_all(meta_dir)?;
    let db_path = meta_dir.join(format!("{name}.db"));
    if db_path.exists() {
        return Err(DiscoveryError::Message(format!(
            "local metadata already exists at {} (remove it first or use another name)",
            db_path.display()
        )));
    }

    let DiscoveredState {
        serving_checkpoint,
        format_info,
        ..
    } = state;
    let engine = RedbMetaEngine::open(&db_path)?;
    engine.import_serving_checkpoint_bytes(&serving_checkpoint)?;
    engine.set_format_info(&format_info)?;
    let (uid, gid) = {
        #[cfg(unix)]
        // SAFETY: these libc calls are safe here; StudioFS trusts a valid OS process context.
        unsafe {
            (libc::getuid(), libc::getgid())
        }
        #[cfg(windows)]
        {
            (0, 0)
        }
    };
    engine.init_root(uid, gid)?;
    drop(engine);

    let primed_engine = open_meta_engine_with_options(
        &db_path,
        MetaOpenOptions {
            background_poll: false,
            fresh_client: true,
            ..Default::default()
        },
    )?;
    drop(primed_engine);

    Ok(db_path)
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
}
