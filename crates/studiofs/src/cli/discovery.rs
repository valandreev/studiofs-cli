//! `studiofs discover` — attach local metadata DB to an existing PG volume.

use clap::Args;
use std::path::Path;
use studiofs_meta::{
    FormatInfo, MetaFormat, RedbMetaEngine, ensure_studiofs_database, redb_engine::RedbSnapshot,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("snapshot settings missing: expected 'format' entry")]
    MissingFormatInfo,
    #[error("snapshot settings item is not a tuple")]
    SnapshotItemNotTuple,
    #[error("snapshot format payload is not a byte array")]
    PayloadNotArray,
    #[error("snapshot format payload contains non-byte value")]
    InvalidFormatPayload,
    #[error("invalid snapshot json: {0}")]
    SnapshotJson(#[from] serde_json::Error),
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
    snapshot_state: RedbSnapshot,
    format_info: FormatInfo,
}

fn extract_format_info(snapshot_bytes: &[u8]) -> DiscoveryResult<FormatInfo> {
    let value: serde_json::Value =
        serde_json::from_slice(snapshot_bytes).map_err(DiscoveryError::from)?;
    let settings = value
        .get("settings")
        .and_then(|v| v.as_array())
        .ok_or(DiscoveryError::MissingFormatInfo)?;

    for item in settings {
        let pair = item
            .as_array()
            .ok_or(DiscoveryError::SnapshotItemNotTuple)?;
        if pair.len() != 2 {
            continue;
        }
        let Some("format") = pair[0].as_str() else {
            continue;
        };
        let arr = pair[1].as_array().ok_or(DiscoveryError::PayloadNotArray)?;
        let mut bytes = Vec::with_capacity(arr.len());
        for n in arr {
            let b = n.as_u64().ok_or(DiscoveryError::InvalidFormatPayload)?;
            bytes.push(u8::try_from(b).map_err(|_| DiscoveryError::InvalidFormatPayload)?);
        }
        let info: FormatInfo = serde_json::from_slice(&bytes).map_err(DiscoveryError::from)?;
        return Ok(info);
    }

    Err(DiscoveryError::MissingFormatInfo)
}

fn load_remote_discovery_state(name: &str, meta_dsn: &str) -> DiscoveryResult<DiscoveredState> {
    crate::runtime_bridge::block_on(async move {
        let postgres_dsn = ensure_studiofs_database(meta_dsn)
            .await
            .map_err(|e| DiscoveryError::Message(e.to_string()))?
            .metadata_dsn;
        let (volume_id, snapshot) = fetch_remote_snapshot(name, &postgres_dsn).await?;

        let mut format_info = extract_format_info(&snapshot)?;
        let snapshot_state: RedbSnapshot = serde_json::from_slice(&snapshot).map_err(|e| {
            DiscoveryError::Message(format!("invalid metadata snapshot payload: {e}"))
        })?;
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
            snapshot_state,
            format_info,
        })
    })
}

async fn fetch_remote_snapshot(name: &str, postgres_dsn: &str) -> DiscoveryResult<(i64, Vec<u8>)> {
    let (client, connection) = tokio_postgres::connect(postgres_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|e| DiscoveryError::Message(format!("cannot connect to postgres: {e}")))?;
    let conn_task = tokio::spawn(connection);

    let result = async {
        let row = client
            .query_opt(
                r#"
            SELECT v.vol, ms.snapshot
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

        Ok::<(i64, Vec<u8>), DiscoveryError>((row.get(0), row.get(1)))
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
        snapshot_state,
        format_info,
        ..
    } = state;
    let engine = RedbMetaEngine::open(&db_path)?;
    engine.import_snapshot(&snapshot_state)?;
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
}
