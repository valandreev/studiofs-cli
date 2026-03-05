//! `studiofs init` — initialize a new filesystem volume.
//!
//! Bucket creation is automatic: if the target bucket does not exist the
//! command will attempt to create it (AWS SigV4 signed CreateBucket).
//! A compatibility pre-flight check is run on every storage backend before
//! any local state is written.  On failure the command is aborted and any
//! buckets that were created during this run are deleted.

use std::io::Write;
use std::path::Path;
use studiofs_data::{DataStore, DataStoreConfig};
use studiofs_meta::{
    DEFAULT_BLOCK_SIZE, FormatInfo, MetaEngine, MetadataBackend, MetadataConfig, PostgresConfig,
    RedbMetaEngine,
};
use tracing::info;
use uuid::Uuid;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Parse a size string like "4MB", "1GB" into bytes.
/// Supports KB, MB, GB, TB suffixes. Defaults to bytes if no suffix.
fn parse_size(size_str: &str) -> anyhow::Result<u32> {
    let size_str = size_str.trim().to_uppercase();

    let (num_part, suffix) = if let Some(pos) = size_str.find(|c: char| c.is_alphabetic()) {
        (&size_str[..pos], &size_str[pos..])
    } else {
        (&size_str[..], "")
    };

    let num: u64 = num_part
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid number in size: {}", size_str))?;

    let multiplier = match suffix {
        "" => 1,
        "B" => 1,
        "KB" => 1024,
        "MB" => 1024 * 1024,
        "GB" => 1024 * 1024 * 1024,
        "TB" => 1024 * 1024 * 1024 * 1024,
        _ => return Err(anyhow::anyhow!("Unknown size suffix: {}", suffix)),
    };

    let bytes = num.saturating_mul(multiplier);

    if bytes > u32::MAX as u64 {
        return Err(anyhow::anyhow!(
            "Block size too large: {} (max 4GB)",
            size_str
        ));
    }

    Ok(bytes as u32)
}

#[cfg(unix)]
fn get_current_ids() -> (u32, u32) {
    unsafe { (libc::getuid(), libc::getgid()) }
}

#[cfg(windows)]
fn get_current_ids() -> (u32, u32) {
    (0, 0)
}

/// Ensure a bucket is accessible, creating it if necessary.
///
/// Returns `true` if the bucket was created by this call (and thus should be
/// deleted if the overall init is aborted), or `false` if it already existed.
fn ensure_bucket_accessible(
    rt: &tokio::runtime::Runtime,
    label: &str,
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> anyhow::Result<bool> {
    use super::test::bucket_create;

    let cfg = DataStoreConfig {
        endpoint: endpoint.to_string(),
        bucket: bucket.to_string(),
        access_key: access_key.to_string(),
        secret_key: secret_key.to_string(),
        region: region.to_string(),
        volume_name: String::new(),
    };

    print!("  Checking {label} bucket '{bucket}'... ");
    std::io::stdout().flush().ok();

    let ds = DataStore::new(&cfg)
        .map_err(|e| anyhow::anyhow!("cannot build {label} store client: {e}"))?;

    match rt.block_on(ds.check()) {
        Ok(()) => {
            println!("OK (exists)");
            return Ok(false);
        }
        Err(e) => {
            println!("not found — will create ({e})");
        }
    }

    print!("  Creating {label} bucket '{bucket}'... ");
    std::io::stdout().flush().ok();
    rt.block_on(bucket_create(
        endpoint, bucket, access_key, secret_key, region,
    ))
    .map_err(|e| {
        anyhow::anyhow!(
            "bucket '{bucket}' not accessible and could not be created: {e}\n\
                 Tip: check credentials and ensure s3:CreateBucket permission is granted."
        )
    })?;
    println!("OK (created)");
    Ok(true)
}

/// Delete a bucket during abort cleanup (best-effort, errors are logged only).
fn cleanup_delete_bucket(
    rt: &tokio::runtime::Runtime,
    label: &str,
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
) {
    use super::test::bucket_delete_call;
    print!("    Deleting {label} bucket '{bucket}'... ");
    std::io::stdout().flush().ok();
    match rt.block_on(bucket_delete_call(
        endpoint, bucket, access_key, secret_key, region,
    )) {
        Ok(()) => println!("OK"),
        Err(e) => println!("WARN — {e}"),
    }
}

fn check_postgres_accessible(rt: &tokio::runtime::Runtime, dsn: &str) -> anyhow::Result<()> {
    rt.block_on(async {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| anyhow::anyhow!("cannot connect to postgres: {e}"))?;

        let conn_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!("postgres connection task ended: {e}");
            }
        });

        client
            .query_one("SELECT 1", &[])
            .await
            .map_err(|e| anyhow::anyhow!("postgres health query failed: {e}"))?;

        conn_task.abort();
        Ok(())
    })
}

fn init_postgres_schema_and_volume(
    rt: &tokio::runtime::Runtime,
    dsn: &str,
    name: &str,
    uuid: &str,
    block_size: u32,
) -> anyhow::Result<i64> {
    rt.block_on(async {
        let (mut client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| anyhow::anyhow!("cannot connect to postgres: {e}"))?;

        let conn_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!("postgres connection task ended: {e}");
            }
        });

        let tx = client.transaction().await?;

        // Serialize DDL across concurrent initializers to avoid sequence-name races.
        tx.execute("SELECT pg_advisory_xact_lock($1)", &[&902_202_603_i64])
            .await?;

        // Phase 1 schema (MVP): enough to register volume and counters.
        tx.batch_execute(
            r#"
            CREATE TABLE IF NOT EXISTS volumes (
                vol BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                uuid TEXT NOT NULL UNIQUE,
                block_size INTEGER NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );

            CREATE TABLE IF NOT EXISTS counters (
                vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                name TEXT NOT NULL,
                value BIGINT NOT NULL,
                PRIMARY KEY (vol, name)
            );

            CREATE TABLE IF NOT EXISTS event_log (
                vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                seq_id BIGSERIAL PRIMARY KEY,
                event_type TEXT NOT NULL,
                event_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                client_id TEXT,
                client_timestamp BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );

            CREATE TABLE IF NOT EXISTS metadata_state (
                vol BIGINT PRIMARY KEY REFERENCES volumes(vol) ON DELETE CASCADE,
                version BIGINT NOT NULL,
                snapshot BYTEA NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );

            CREATE INDEX IF NOT EXISTS idx_event_log_vol_seq
                ON event_log(vol, seq_id);
            "#,
        )
        .await?;

        let row = tx
            .query_one(
                r#"
                INSERT INTO volumes (name, uuid, block_size)
                VALUES ($1, $2, $3)
                ON CONFLICT (uuid) DO UPDATE
                SET name = EXCLUDED.name,
                    block_size = EXCLUDED.block_size
                RETURNING vol
                "#,
                &[&name, &uuid, &(block_size as i32)],
            )
            .await?;
        let volume_id: i64 = row.get(0);

        tx.execute(
            r#"
            INSERT INTO counters (vol, name, value)
            VALUES
                ($1, 'next_inode', 2),
                ($1, 'next_block_id', 1),
                ($1, 'used_space', 0),
                ($1, 'total_inodes', 1)
            ON CONFLICT (vol, name) DO NOTHING
            "#,
            &[&volume_id],
        )
        .await?;

        tx.commit().await?;
        conn_task.abort();
        Ok(volume_id)
    })
}

fn seed_postgres_metadata_state(
    rt: &tokio::runtime::Runtime,
    dsn: &str,
    volume_id: i64,
    snapshot_json: &[u8],
) -> anyhow::Result<()> {
    rt.block_on(async {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| anyhow::anyhow!("cannot connect to postgres: {e}"))?;
        let conn_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!("postgres connection task ended: {e}");
            }
        });

        client
            .execute(
                r#"
                INSERT INTO metadata_state (vol, version, snapshot)
                VALUES ($1, 1, $2)
                ON CONFLICT (vol) DO NOTHING
                "#,
                &[&volume_id, &snapshot_json],
            )
            .await
            .map_err(|e| anyhow::anyhow!("failed to seed metadata_state: {e}"))?;

        conn_task.abort();
        Ok(())
    })
}

// ── Main run function ─────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub fn run(
    name: &str,
    data_endpoint: &str,
    data_bucket: &str,
    data_access_key: &str,
    data_secret_key: &str,
    data_region: &str,
    postgres_dsn: Option<&str>,
    metadata_endpoint: Option<&str>,
    metadata_bucket: Option<&str>,
    metadata_access_key: Option<&str>,
    metadata_secret_key: Option<&str>,
    metadata_region: Option<&str>,
    metadata_cache_ttl_secs: u64,
    metadata_flush_interval_secs: u64,
    metadata_flush_ops_threshold: u32,
    metadata_flush_bytes_threshold: u64,
    meta_dir: &Path,
    block_size: Option<&str>,
) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    let postgres_dsn = postgres_dsn.ok_or_else(|| {
        anyhow::anyhow!(
            "PostgreSQL metadata backend is required. Pass --postgres postgres://user:pass@host:5432/db"
        )
    })?;
    let eff_data_region = if data_region.is_empty() {
        "us-east-1"
    } else {
        data_region
    };

    if metadata_endpoint.is_some()
        || metadata_bucket.is_some()
        || metadata_access_key.is_some()
        || metadata_secret_key.is_some()
        || metadata_region.is_some()
    {
        return Err(anyhow::anyhow!(
            "--metadata-* options are removed. StudioFS now uses PostgreSQL metadata backend only."
        ));
    }

    // ── 1. Ensure buckets are accessible (create if missing) ─────────────────
    println!("Checking storage backends...");
    println!();
    let data_bucket_created = ensure_bucket_accessible(
        &rt,
        "data",
        data_endpoint,
        data_bucket,
        data_access_key,
        data_secret_key,
        eff_data_region,
    )?;

    let meta_bucket_created = false;
    println!();

    println!("Checking PostgreSQL backend...");
    check_postgres_accessible(&rt, postgres_dsn)?;
    println!("✓ PostgreSQL connectivity check passed.");
    println!();

    // ── 2. Compatibility pre-flight check ─────────────────────────────────────
    println!("Running compatibility pre-flight check...");
    println!();

    println!("  [Data store: {data_endpoint} / {data_bucket}]");
    let data_compat = super::test::run_compat_on_bucket(
        data_endpoint,
        data_bucket,
        data_access_key,
        data_secret_key,
        eff_data_region,
        &rt,
    )?;

    // ── 3. Evaluate compat results ────────────────────────────────────────────
    let data_ok = data_compat.suitable_for_data();
    let meta_ok = true;

    if !data_ok || !meta_ok {
        // Print full reports so the user sees exactly what failed.
        data_compat.print_report();

        // Cleanup: delete buckets we created during this run.
        println!("Compatibility check FAILED — cleaning up...");
        if data_bucket_created {
            cleanup_delete_bucket(
                &rt,
                "data",
                data_endpoint,
                data_bucket,
                data_access_key,
                data_secret_key,
                eff_data_region,
            );
        }
        let _ = meta_bucket_created;

        // Collect failure details for the error message.
        let failures: Vec<String> = data_compat.failure_summary();
        let detail = failures.join("\n  ");
        return Err(anyhow::anyhow!(
            "Storage compatibility check failed — this backend is not suitable for StudioFS:\n  {detail}\n\nRun `studiofs test` for detailed diagnostics."
        ));
    }

    println!("✓ Compatibility check passed.");
    println!();

    // ── 4. Check if metadata directory already exists ────────────────────────
    if meta_dir.exists() {
        println!("! Metadata directory already exists: {:?}", meta_dir);
        print!("  Do you want to CLEAR it and start fresh? [y/N]: ");
        std::io::stdout().flush()?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let input = input.trim().to_lowercase();

        if input == "y" || input == "yes" {
            info!("clearing existing metadata directory {:?}", meta_dir);
            std::fs::remove_dir_all(meta_dir)?;
        } else {
            return Err(anyhow::anyhow!("Initialization cancelled."));
        }
    }

    // ── 5. Create metadata directory and initialize local DB ─────────────────
    std::fs::create_dir_all(meta_dir)?;

    let db_path = meta_dir.join(format!("{name}.db"));
    info!("initializing volume {name:?} at {db_path:?}");

    let engine = RedbMetaEngine::open(&db_path)?;

    // Parse block size if provided, otherwise use default
    let actual_block_size = if let Some(size_str) = block_size {
        parse_size(size_str)?
    } else {
        DEFAULT_BLOCK_SIZE
    };
    let volume_uuid = Uuid::new_v4().to_string();

    let data_prefix = format!("{name}/objects/");
    let postgres_volume_id =
        init_postgres_schema_and_volume(&rt, postgres_dsn, name, &volume_uuid, actual_block_size)?;

    // ── 6. Persist format info ────────────────────────────────────────────────
    let format_info = FormatInfo {
        name: name.to_string(),
        uuid: volume_uuid,
        endpoint: data_endpoint.to_string(),
        bucket: data_bucket.to_string(),
        access_key: data_access_key.to_string(),
        secret_key: data_secret_key.to_string(),
        region: data_region.to_string(),
        data_prefix,
        metadata: MetadataConfig {
            backend: MetadataBackend::Postgres,
            remote: None,
            postgres: Some(PostgresConfig {
                url: postgres_dsn.to_string(),
                volume_id: postgres_volume_id,
            }),
            cache_ttl_secs: metadata_cache_ttl_secs.max(1),
            flush_interval_secs: metadata_flush_interval_secs.max(1),
            flush_ops_threshold: metadata_flush_ops_threshold.max(1),
            flush_bytes_threshold: metadata_flush_bytes_threshold.max(1),
        },
        version: String::new(),
        block_size: actual_block_size,
        chunk_size: 0,
    };
    engine.set_format_info(&format_info)?;

    // Initialize root inode owned by the current user
    let (uid, gid) = get_current_ids();
    engine.init_root(uid, gid)?;
    let snapshot = engine.export_snapshot()?;
    let snapshot_json = serde_json::to_vec(&snapshot)?;
    seed_postgres_metadata_state(&rt, postgres_dsn, postgres_volume_id, &snapshot_json)?;
    drop(engine);

    // ── 7. Report success ─────────────────────────────────────────────────────
    info!("volume {name:?} initialized successfully");
    println!("✓ Volume '{}' initialized", name);
    println!("  UUID:       {}", format_info.uuid);
    println!(
        "  Block size: {} bytes ({} MiB)",
        actual_block_size,
        actual_block_size / (1024 * 1024)
    );
    println!("  Data endpoint: {}", data_endpoint);
    println!("  Data bucket:   {}", data_bucket);
    if let Some(pg) = &format_info.metadata.postgres {
        println!("  Metadata backend: Postgres");
        println!("  Postgres: {}", pg.url);
        println!("  Volume ID: {}", pg.volume_id);
    }
    println!("  Metadata:   {:?}", db_path);
    println!();
    println!("Run `studiofs mount {} <mount-point>` to mount.", name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size_bytes() {
        assert_eq!(parse_size("100").unwrap(), 100);
        assert_eq!(parse_size("100B").unwrap(), 100);
    }

    #[test]
    fn test_parse_size_kb() {
        assert_eq!(parse_size("1KB").unwrap(), 1024);
        assert_eq!(parse_size("512KB").unwrap(), 512 * 1024);
    }

    #[test]
    fn test_parse_size_mb() {
        assert_eq!(parse_size("1MB").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("4MB").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_size("8MB").unwrap(), 8 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_gb() {
        assert_eq!(parse_size("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("2GB").unwrap(), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_case_insensitive() {
        assert_eq!(parse_size("1mb").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("4MB").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_size("8mB").unwrap(), 8 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_whitespace() {
        assert_eq!(parse_size("  4MB  ").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_size("4 MB").unwrap(), 4 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_invalid_suffix() {
        assert!(parse_size("4XB").is_err());
    }

    #[test]
    fn test_parse_size_invalid_number() {
        assert!(parse_size("abcMB").is_err());
    }
}
