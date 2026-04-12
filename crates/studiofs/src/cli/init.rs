//! `studiofs init` — initialize a new filesystem volume.
//!
//! Bucket creation is automatic: if the target bucket does not exist the
//! command will attempt to create it (AWS SigV4 signed CreateBucket).
//! The command validates basic storage connectivity and PostgreSQL availability
//! before writing local metadata state.

use clap::Args;
use std::io::Write;
use std::path::Path;
use studiofs_data::{DataStore, DataStoreConfig};
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, DEFAULT_BLOCK_SIZE, FormatInfo, MetaFormat, MetadataBackend,
    MetadataConfig, PostgresConfig, RedbMetaEngine, ensure_studiofs_database,
};
use tracing::info;
use uuid::Uuid;

// ── Helpers ───────────────────────────────────────────────────────────────────

#[cfg(unix)]
fn get_current_ids() -> (u32, u32) {
    // SAFETY: these libc calls are read-only and safe for trusted process startup.
    unsafe { (libc::getuid(), libc::getgid()) }
}

#[cfg(windows)]
fn get_current_ids() -> (u32, u32) {
    (0, 0)
}

#[derive(Debug, Args)]
#[command(about = "Initialize a new filesystem volume")]
#[command(long_about = "\
Initialize a new filesystem volume by validating S3 object storage, creating \
shared PostgreSQL metadata state, and bootstrapping the local metadata cache \
for the current client.

Path layout is fixed by disk name:
  • Data objects: <disk-name>/objects/...

`--meta` is required and must point at the PostgreSQL server. StudioFS always \
stores metadata in database `studiofs`; it will create that database during \
`init` when the server allows it.

The target S3 bucket is created automatically if it does not exist and the \
provided credentials allow it.")]
#[command(after_help = "\
EXAMPLES:
  AWS S3 + PostgreSQL:
    studiofs init prod --endpoint https://s3.amazonaws.com \
      --bucket my-bucket --access-key AKIA... --secret-key wJal... \
      --region us-east-1 --meta postgres://user:pass@db:5432

  RustFS (local) + PostgreSQL:
    studiofs init dev --endpoint http://127.0.0.1:9000 \
      --bucket test-bucket --access-key studiofs --secret-key studiofs123 \
      --meta postgres://studiofs:studiofs@127.0.0.1:5432

RUNTIME POLICY:
  PG batching / sync cadence is configured per client at mount/runtime, not
  stored in the volume. Use environment variables such as:
    STUDIOFS_PG_FLUSH_INTERVAL_SECS
    STUDIOFS_PG_FLUSH_OPS_THRESHOLD
    STUDIOFS_PG_FLUSH_IDLE_MS
    STUDIOFS_PG_SYNC_POLL_SECS
")]
pub(crate) struct InitArgs {
    /// Disk name (used in object paths like `<name>/objects`)
    name: String,

    /// S3 endpoint URL (e.g. <https://s3.amazonaws.com>, <http://127.0.0.1:9000>)
    #[arg(long)]
    endpoint: String,

    /// S3 bucket name for file data (created if missing when permissions allow)
    #[arg(long)]
    bucket: String,

    /// S3 access key ID
    #[arg(long)]
    access_key: String,

    /// S3 secret access key
    #[arg(long)]
    secret_key: String,

    /// S3 region (defaults to us-east-1 for local S3-compatible services)
    #[arg(long, default_value = "")]
    region: String,

    /// Deprecated option. Metadata is PostgreSQL-only now.
    #[arg(long, hide = true)]
    metadata_endpoint: Option<String>,

    /// PostgreSQL server DSN; StudioFS uses database `studiofs`
    #[arg(long = "meta", required = true)]
    meta: Option<String>,

    /// Deprecated option. Metadata is PostgreSQL-only now.
    #[arg(long, hide = true)]
    metadata_bucket: Option<String>,

    /// Deprecated option. Metadata is PostgreSQL-only now.
    #[arg(long, hide = true)]
    metadata_access_key: Option<String>,

    /// Deprecated option. Metadata is PostgreSQL-only now.
    #[arg(long, hide = true)]
    metadata_secret_key: Option<String>,

    /// Deprecated option. Metadata is PostgreSQL-only now.
    #[arg(long, hide = true)]
    metadata_region: Option<String>,
}

impl InitArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        let meta_dir = super::utils::get_default_meta_dir(&self.name)?;
        run(
            &self.name,
            &self.endpoint,
            &self.bucket,
            &self.access_key,
            &self.secret_key,
            &self.region,
            self.meta.as_deref(),
            self.metadata_endpoint.as_deref(),
            self.metadata_bucket.as_deref(),
            self.metadata_access_key.as_deref(),
            self.metadata_secret_key.as_deref(),
            self.metadata_region.as_deref(),
            &meta_dir,
        )
    }
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

fn volume_name_exists(rt: &tokio::runtime::Runtime, dsn: &str, name: &str) -> anyhow::Result<bool> {
    rt.block_on(async {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| anyhow::anyhow!("cannot connect to postgres: {e}"))?;

        let conn_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!("postgres connection task ended: {e}");
            }
        });

        let has_volumes_table = client
            .query_one("SELECT to_regclass('public.volumes') IS NOT NULL", &[])
            .await
            .map_err(|e| anyhow::anyhow!("postgres schema probe failed: {e}"))?;
        let has_volumes_table: bool = has_volumes_table.get(0);
        if !has_volumes_table {
            conn_task.abort();
            return Ok(false);
        }

        let row = client
            .query_opt("SELECT vol FROM volumes WHERE name = $1 LIMIT 1", &[&name])
            .await
            .map_err(|e| anyhow::anyhow!("postgres volume lookup failed: {e}"))?;

        conn_task.abort();
        Ok(row.is_some())
    })
}

fn init_pg_schema_and_volume(
    rt: &tokio::runtime::Runtime,
    dsn: &str,
    name: &str,
    uuid: &str,
    block_size: u32,
) -> anyhow::Result<i64> {
    rt.block_on(async {
        let block_size_i32 = i32::try_from(block_size).map_err(|_| {
            anyhow::anyhow!("block size {block_size} exceeds PostgreSQL INTEGER range")
        })?;
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
                batch_version BIGINT NOT NULL,
                event_type TEXT NOT NULL,
                event_payload BYTEA NOT NULL,
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

            CREATE TABLE IF NOT EXISTS acl_rules (
                vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                ino BIGINT NOT NULL,
                principal TEXT NOT NULL,
                access_level SMALLINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (vol, ino, principal)
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                id TEXT NOT NULL,
                root_ino BIGINT NOT NULL,
                created_at BIGINT NOT NULL,
                state SMALLINT NOT NULL,
                pinned_object_ids BIGINT[] NOT NULL DEFAULT '{}',
                snapshot_payload BYTEA NOT NULL DEFAULT ''::bytea,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (vol, id)
            );

            ALTER TABLE event_log
                ADD COLUMN IF NOT EXISTS batch_version BIGINT;
            UPDATE event_log
                SET batch_version = 0
                WHERE batch_version IS NULL;
            ALTER TABLE event_log
                ALTER COLUMN batch_version SET NOT NULL;

            CREATE INDEX IF NOT EXISTS idx_event_log_vol_seq
                ON event_log(vol, seq_id);
            CREATE INDEX IF NOT EXISTS idx_event_log_vol_batch_seq
                ON event_log(vol, batch_version, seq_id);
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
                &[&name, &uuid, &block_size_i32],
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

fn seed_pg_metadata_state(
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
    meta_dir: &Path,
) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    let meta_dsn = postgres_dsn.ok_or_else(|| {
        anyhow::anyhow!(
            "PostgreSQL metadata backend is required. Pass --meta postgres://user:pass@host:5432"
        )
    })?;
    let postgres_dsn = rt
        .block_on(async { ensure_studiofs_database(meta_dsn).await })
        .map_err(|e| anyhow::anyhow!(e.to_string()))?
        .metadata_dsn;
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
    ensure_bucket_accessible(
        &rt,
        "data",
        data_endpoint,
        data_bucket,
        data_access_key,
        data_secret_key,
        eff_data_region,
    )?;
    println!();

    println!("Checking PostgreSQL backend...");
    check_postgres_accessible(&rt, &postgres_dsn)?;
    println!("✓ PostgreSQL connectivity check passed.");
    println!();

    if volume_name_exists(&rt, &postgres_dsn, name)? {
        return Err(anyhow::anyhow!(
            "Volume '{name}' already exists in PostgreSQL metadata. Run `studiofs destroy {name}` first, then retry `studiofs init {name}`."
        ));
    }

    // ── 2. Check if metadata directory already exists ────────────────────────
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

    // ── 3. Create metadata directory and initialize local DB ─────────────────
    std::fs::create_dir_all(meta_dir)?;

    let db_path = meta_dir.join(format!("{name}.db"));
    info!("initializing volume {name:?} at {db_path:?}");

    let engine = RedbMetaEngine::open(&db_path)?;

    let actual_block_size = DEFAULT_BLOCK_SIZE;
    let volume_uuid = Uuid::new_v4().to_string();

    let data_prefix = format!("{name}/objects/");
    let postgres_volume_id =
        init_pg_schema_and_volume(&rt, &postgres_dsn, name, &volume_uuid, actual_block_size)?;

    // ── 6. Persist format info ────────────────────────────────────────────────
    let format_info = FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
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
        },
        block_size: actual_block_size,
    };
    engine.set_format_info(&format_info)?;

    // Initialize root inode owned by the current user
    let (uid, gid) = get_current_ids();
    engine.init_root(uid, gid)?;
    let snapshot = engine.export_snapshot()?;
    let snapshot_json = serde_json::to_vec(&snapshot)?;
    seed_pg_metadata_state(&rt, &postgres_dsn, postgres_volume_id, &snapshot_json)?;
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
    use clap::error::ErrorKind;

    #[test]
    fn init_parses_required_runtime_args() {
        let cli = crate::cli::parse_for_test(&[
            "init",
            "vol",
            "--endpoint",
            "http://127.0.0.1:9000",
            "--bucket",
            "bucket",
            "--access-key",
            "ak",
            "--secret-key",
            "sk",
            "--meta",
            "postgres://u:p@127.0.0.1:5432",
        ])
        .unwrap();

        match cli.command {
            crate::cli::Commands::Init(InitArgs {
                name,
                endpoint,
                bucket,
                access_key,
                secret_key,
                meta,
                ..
            }) => {
                assert_eq!(name, "vol");
                assert_eq!(endpoint, "http://127.0.0.1:9000");
                assert_eq!(bucket, "bucket");
                assert_eq!(access_key, "ak");
                assert_eq!(secret_key, "sk");
                assert_eq!(meta.as_deref(), Some("postgres://u:p@127.0.0.1:5432"));
            }
            other => panic!("expected Init, got {other:?}"),
        }
    }

    #[test]
    fn init_rejects_block_size_flag() {
        let err = match crate::cli::parse_for_test(&[
            "init",
            "vol",
            "--endpoint",
            "http://127.0.0.1:9000",
            "--bucket",
            "test",
            "--access-key",
            "ak",
            "--secret-key",
            "sk",
            "--meta",
            "postgres://u:p@127.0.0.1:5432",
            "--block-size",
            "4MB",
        ]) {
            Ok(_) => panic!("expected clap error for removed --block-size"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::UnknownArgument);
    }
}
