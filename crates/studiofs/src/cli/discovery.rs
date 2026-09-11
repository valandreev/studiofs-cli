//! `studiofs discover` — attach local metadata DB to an existing PG volume.

use clap::Args;
use sha2::{Digest, Sha256};
use std::path::Path;
use studiofs_meta::{
    FormatInfo, MetaFormat, MetaOpenOptions, RedbMetaEngine, connect_studiofs_postgres,
    ensure_studiofs_database, ensure_supported_format_version,
    ensure_supported_pg_sync_format_version, open_meta_engine_with_options,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("remote format info is missing")]
    MissingFormatInfo,
    #[error("invalid remote format info: {0}")]
    FormatInfoJson(#[from] serde_json::Error),
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

fn decode_format_info(format_info_bytes: &[u8]) -> DiscoveryResult<FormatInfo> {
    if format_info_bytes.is_empty() {
        return Err(DiscoveryError::MissingFormatInfo);
    }
    Ok(serde_json::from_slice(format_info_bytes)?)
}

fn load_remote_discovery_state(name: &str, meta_dsn: &str) -> DiscoveryResult<DiscoveredState> {
    crate::runtime_bridge::block_on(async move {
        let postgres_dsn = ensure_studiofs_database(meta_dsn)
            .await
            .map_err(|e| DiscoveryError::Message(e.to_string()))?
            .metadata_dsn;
        let (
            volume_id,
            serving_checkpoint,
            format_info_bytes,
            pg_format_version,
            checkpoint_sha256,
            checkpoint_publication_state,
            checkpoint_generation,
        ) = fetch_remote_state(name, &postgres_dsn).await?;

        ensure_supported_pg_sync_format_version(pg_format_version)?;
        validate_checkpoint_descriptor(
            &serving_checkpoint,
            checkpoint_sha256.as_deref(),
            checkpoint_publication_state.as_deref(),
            checkpoint_generation,
        )?;
        let mut format_info = decode_format_info(&format_info_bytes)?;
        ensure_supported_format_version(&format_info)?;
        let pg = format_info
            .metadata
            .postgres
            .as_mut()
            .ok_or(DiscoveryError::Message(
                "discovered format is not postgres-backed".to_string(),
            ))?;
        pg.url = postgres_dsn;
        pg.volume_id = volume_id;

        Ok(DiscoveredState {
            volume_id,
            serving_checkpoint,
            format_info,
        })
    })
}

fn validate_checkpoint_descriptor(
    serving_checkpoint: &[u8],
    checkpoint_sha256: Option<&[u8]>,
    checkpoint_publication_state: Option<&str>,
    checkpoint_generation: Option<i64>,
) -> DiscoveryResult<()> {
    if checkpoint_publication_state != Some("published")
        || checkpoint_generation.unwrap_or_default() <= 0
        || checkpoint_sha256 != Some(Sha256::digest(serving_checkpoint).as_slice())
    {
        return Err(DiscoveryError::Message(
            "discovered serving checkpoint descriptor is invalid".to_string(),
        ));
    }
    Ok(())
}

async fn fetch_remote_state(
    name: &str,
    postgres_dsn: &str,
) -> DiscoveryResult<(
    i64,
    Vec<u8>,
    Vec<u8>,
    Option<i16>,
    Option<Vec<u8>>,
    Option<String>,
    Option<i64>,
)> {
    let (client, connection) = connect_studiofs_postgres(postgres_dsn)
        .await
        .map_err(|e| DiscoveryError::Message(e.to_string()))?;
    let conn_task = tokio::spawn(connection);

    let result = async {
        let row = client
            .query_opt(
                r#"
            SELECT
                v.vol,
                ms.snapshot,
                ms.format_info,
                ms.metadata_format_version,
                ms.checkpoint_sha256,
                ms.checkpoint_publication_state,
                ms.checkpoint_generation
            FROM volumes v
            JOIN metadata_state ms ON ms.vol = v.vol
            WHERE v.name = $1
            ORDER BY v.vol DESC
            LIMIT 1
                "#,
                &[&name],
            )
            .await
            .map_err(|e| DiscoveryError::Message(format!("discover query failed: {e}")))?;

        let row = row.ok_or_else(|| {
            DiscoveryError::Message(format!(
                "volume '{name}' not found or not initialized in metadata_state (run init + first sync)"
            ))
        })?;

        Ok::<(
            i64,
            Vec<u8>,
            Vec<u8>,
            Option<i16>,
            Option<Vec<u8>>,
            Option<String>,
            Option<i64>,
        ), DiscoveryError>((
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
            row.get(5),
            row.get(6),
        ))
    }
    .await;

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
    fn discover_rejects_incomplete_checkpoint_publication() {
        let checkpoint = b"checkpoint";
        let digest = Sha256::digest(checkpoint);

        let err = validate_checkpoint_descriptor(
            checkpoint,
            Some(digest.as_slice()),
            Some("pending"),
            Some(1),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            DiscoveryError::Message(message)
                if message == "discovered serving checkpoint descriptor is invalid"
        ));
    }

    #[test]
    fn discover_rejects_checkpoint_checksum_mismatch() {
        let err = validate_checkpoint_descriptor(
            b"checkpoint",
            Some(&[0; 32]),
            Some("published"),
            Some(1),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            DiscoveryError::Message(message)
                if message == "discovered serving checkpoint descriptor is invalid"
        ));
    }
}
