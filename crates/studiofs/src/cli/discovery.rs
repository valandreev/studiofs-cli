//! `studiofs discover` — attach local metadata DB to an existing PG volume.

use std::path::Path;
use studiofs_meta::{
    FormatInfo, MetaFormat, RedbMetaEngine, ensure_studiofs_database, redb_engine::RedbSnapshot,
};

fn extract_format_info_from_snapshot(snapshot_bytes: &[u8]) -> anyhow::Result<FormatInfo> {
    let value: serde_json::Value = serde_json::from_slice(snapshot_bytes)
        .map_err(|e| anyhow::anyhow!("invalid metadata snapshot json: {e}"))?;
    let settings = value
        .get("settings")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("snapshot.settings missing"))?;

    for item in settings {
        let pair = item
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("snapshot.settings item is not tuple"))?;
        if pair.len() != 2 {
            continue;
        }
        let key = pair[0].as_str().unwrap_or_default();
        if key != "format" {
            continue;
        }
        let arr = pair[1]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("snapshot format payload is not byte array"))?;
        let mut bytes = Vec::with_capacity(arr.len());
        for n in arr {
            let b = n
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("snapshot format payload contains non-byte"))?;
            bytes.push(
                u8::try_from(b)
                    .map_err(|_| anyhow::anyhow!("snapshot format byte out of range"))?,
            );
        }
        let info: FormatInfo = serde_json::from_slice(&bytes)
            .map_err(|e| anyhow::anyhow!("invalid embedded FormatInfo json: {e}"))?;
        return Ok(info);
    }

    Err(anyhow::anyhow!(
        "FormatInfo not found in metadata snapshot settings"
    ))
}

pub fn run(name: &str, meta_dsn: &str, meta_dir: &Path) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    let postgres_dsn = rt
        .block_on(async { ensure_studiofs_database(meta_dsn).await })
        .map_err(|e| anyhow::anyhow!(e.to_string()))?
        .metadata_dsn;

    let (volume_id, snapshot): (i64, Vec<u8>) = rt.block_on(async {
        let (client, connection) = tokio_postgres::connect(&postgres_dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| anyhow::anyhow!("cannot connect to postgres: {e}"))?;
        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });

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
            .map_err(|e| anyhow::anyhow!("discover query failed: {e}"))?;
        conn_task.abort();

        let row = row.ok_or_else(|| {
            anyhow::anyhow!(
                "volume '{name}' not found or not initialized in metadata_state (run init + first sync)"
            )
        })?;
        Ok::<(i64, Vec<u8>), anyhow::Error>((row.get(0), row.get(1)))
    })?;

    let mut format_info = extract_format_info_from_snapshot(&snapshot)?;
    let snapshot_state: RedbSnapshot = serde_json::from_slice(&snapshot)
        .map_err(|e| anyhow::anyhow!("invalid metadata snapshot payload: {e}"))?;
    let pg = format_info
        .metadata
        .postgres
        .as_mut()
        .ok_or_else(|| anyhow::anyhow!("discovered format is not postgres-backed"))?;
    pg.url = postgres_dsn;
    pg.volume_id = volume_id;

    std::fs::create_dir_all(meta_dir)?;
    let db_path = meta_dir.join(format!("{name}.db"));
    if db_path.exists() {
        anyhow::bail!(
            "local metadata already exists at {} (remove it first or use another name)",
            db_path.display()
        );
    }

    let engine = RedbMetaEngine::open(&db_path)?;
    engine.import_snapshot(&snapshot_state)?;
    engine.set_format_info(&format_info)?;
    let (uid, gid) = {
        #[cfg(unix)]
        unsafe {
            (libc::getuid(), libc::getgid())
        }
        #[cfg(windows)]
        {
            (0, 0)
        }
    };
    engine.init_root(uid, gid)?;

    println!("✓ Volume '{}' discovered", name);
    println!("  Local metadata: {}", db_path.display());
    println!("  Postgres volume_id: {}", volume_id);
    println!("Run `studiofs mount {} <mount-point>` to mount.", name);
    Ok(())
}
