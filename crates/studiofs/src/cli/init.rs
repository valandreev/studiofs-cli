//! `studiofs init` — initialize a new filesystem volume.
//!
//! Bucket creation is automatic: if the target bucket does not exist the
//! command will attempt to create it (AWS SigV4 signed CreateBucket).
//! The command validates basic storage connectivity and PostgreSQL availability
//! before writing local metadata state.

use anyhow::Context;
use clap::Args;
use std::io::Write;
use std::path::PathBuf;
use studiofs_data::{DataStore, DataStoreConfig, DataStoreCryptoConfig};
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, DEFAULT_BLOCK_SIZE, ENCRYPTION_KEY_LENGTH, EncryptionConfig,
    FormatInfo, MetaFormat, MetaOpenOptions, MetadataBackend, MetadataConfig, PostgresConfig,
    RedbMetaEngine, connect_studiofs_postgres, ensure_studiofs_database,
    open_meta_engine_with_options,
};
use tracing::info;
use uuid::Uuid;

const INITIAL_METADATA_STATE_VERSION: i64 = 0;
const ACTIVE_CRYPTO_KEY_STATE: &str = "active";

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

struct DeprecatedMetadataArgs {
    endpoint: Option<String>,
    bucket: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    region: Option<String>,
}

impl DeprecatedMetadataArgs {
    fn is_present(&self) -> bool {
        self.endpoint.is_some()
            || self.bucket.is_some()
            || self.access_key.is_some()
            || self.secret_key.is_some()
            || self.region.is_some()
    }
}

struct InitRunRequest {
    name: String,
    object_store: DataStoreConfig,
    postgres_dsn: String,
    deprecated_metadata: DeprecatedMetadataArgs,
    meta_dir: PathBuf,
}

impl InitRunRequest {
    fn from_args(args: InitArgs) -> anyhow::Result<Self> {
        let meta_dir = super::utils::get_default_meta_dir(&args.name)?;
        let region = if args.region.is_empty() {
            "us-east-1".to_string()
        } else {
            args.region
        };

        Ok(Self {
            name: args.name,
            object_store: DataStoreConfig {
                endpoint: args.endpoint,
                bucket: args.bucket,
                access_key: args.access_key,
                secret_key: args.secret_key,
                region,
                volume_name: String::new(),
                volume_uuid: "init-storage-probe".into(),
                crypto: DataStoreCryptoConfig::from(EncryptionConfig::new(
                    [0; ENCRYPTION_KEY_LENGTH],
                )),
            },
            postgres_dsn: args.meta,
            deprecated_metadata: DeprecatedMetadataArgs {
                endpoint: args.metadata_endpoint,
                bucket: args.metadata_bucket,
                access_key: args.metadata_access_key,
                secret_key: args.metadata_secret_key,
                region: args.metadata_region,
            },
            meta_dir,
        })
    }
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
    studiofs init prod --endpoint https://s3.amazonaws.com \\
      --bucket my-bucket --access-key AKIA... --secret-key wJal... \\
      --region us-east-1 --meta postgres://user:pass@db:5432

  RustFS (local) + PostgreSQL:
    studiofs init dev --endpoint http://127.0.0.1:9000 \\
      --bucket test-bucket --access-key studiofs --secret-key studiofs123 \\
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
    meta: String,

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
        run(InitRunRequest::from_args(self)?)
    }
}

/// Ensure a bucket is accessible, creating it if necessary.
///
/// Returns `true` if the bucket was created by this call (and thus should be
/// deleted if the overall init is aborted), or `false` if it already existed.
fn ensure_bucket_accessible(
    rt: &tokio::runtime::Runtime,
    label: &str,
    cfg: &DataStoreConfig,
) -> anyhow::Result<bool> {
    use super::test::bucket_create;

    print!("  Checking {label} bucket '{}'... ", cfg.bucket);
    std::io::stdout().flush().ok();

    let ds = DataStore::new(cfg)
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

    print!("  Creating {label} bucket '{}'... ", cfg.bucket);
    std::io::stdout().flush().ok();
    rt.block_on(bucket_create(
        &cfg.endpoint,
        &cfg.bucket,
        &cfg.access_key,
        &cfg.secret_key,
        &cfg.region,
    ))
    .map_err(|e| {
        anyhow::anyhow!(
            "bucket '{}' not accessible and could not be created: {e}\n\
                 Tip: check credentials and ensure s3:CreateBucket permission is granted.",
            cfg.bucket
        )
    })?;
    println!("OK (created)");
    Ok(true)
}

fn check_postgres_accessible(rt: &tokio::runtime::Runtime, dsn: &str) -> anyhow::Result<()> {
    rt.block_on(async {
        let (client, connection) = connect_studiofs_postgres(dsn)
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
        let (client, connection) = connect_studiofs_postgres(dsn)
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

struct ActiveCryptoKeyRow<'a> {
    algorithm: &'static str,
    key_material: &'a [u8],
    state: &'static str,
}

impl<'a> ActiveCryptoKeyRow<'a> {
    fn from_encryption(encryption: &'a EncryptionConfig) -> Self {
        Self {
            algorithm: encryption.algorithm.as_str(),
            key_material: &encryption.volume_master_key,
            state: ACTIVE_CRYPTO_KEY_STATE,
        }
    }
}

fn init_pg_schema_sql() -> &'static str {
    r#"
            CREATE TABLE IF NOT EXISTS volumes (
                vol BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                uuid TEXT NOT NULL UNIQUE,
                block_size INTEGER NOT NULL,
                encryption_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                encryption_format_version SMALLINT NOT NULL DEFAULT 1,
                active_crypto_key_id INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );

            ALTER TABLE volumes
                ADD COLUMN IF NOT EXISTS encryption_enabled BOOLEAN NOT NULL DEFAULT TRUE;
            ALTER TABLE volumes
                ADD COLUMN IF NOT EXISTS encryption_format_version SMALLINT NOT NULL DEFAULT 1;
            ALTER TABLE volumes
                ADD COLUMN IF NOT EXISTS active_crypto_key_id INTEGER NOT NULL DEFAULT 1;

            CREATE TABLE IF NOT EXISTS volume_crypto_keys (
                vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                key_id INTEGER NOT NULL,
                algorithm TEXT NOT NULL,
                key_material BYTEA NOT NULL,
                state TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (vol, key_id)
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
            "#
}

fn init_pg_volume_upsert_sql() -> &'static str {
    r#"
                INSERT INTO volumes (
                    name,
                    uuid,
                    block_size,
                    encryption_enabled,
                    encryption_format_version,
                    active_crypto_key_id
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (uuid) DO UPDATE
                SET name = EXCLUDED.name,
                    block_size = EXCLUDED.block_size,
                    encryption_enabled = EXCLUDED.encryption_enabled,
                    encryption_format_version = EXCLUDED.encryption_format_version,
                    active_crypto_key_id = EXCLUDED.active_crypto_key_id
                RETURNING vol
                "#
}

fn init_pg_crypto_key_insert_sql() -> &'static str {
    r#"
            INSERT INTO volume_crypto_keys (vol, key_id, algorithm, key_material, state)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (vol, key_id) DO NOTHING
            "#
}

fn init_pg_schema_and_volume(
    rt: &tokio::runtime::Runtime,
    dsn: &str,
    name: &str,
    uuid: &str,
    block_size: u32,
    encryption: &EncryptionConfig,
) -> anyhow::Result<i64> {
    rt.block_on(async {
        let block_size_i32 = i32::try_from(block_size).map_err(|_| {
            anyhow::anyhow!("block size {block_size} exceeds PostgreSQL INTEGER range")
        })?;
        let active_key_id_i32 = i32::try_from(encryption.active_key_id).map_err(|_| {
            anyhow::anyhow!(
                "active encryption key id {} exceeds PostgreSQL INTEGER range",
                encryption.active_key_id
            )
        })?;
        let encryption_format_version_i16 =
            i16::try_from(encryption.format_version).map_err(|_| {
                anyhow::anyhow!(
                    "encryption format version {} exceeds PostgreSQL SMALLINT range",
                    encryption.format_version
                )
            })?;
        let active_key = ActiveCryptoKeyRow::from_encryption(encryption);
        let (mut client, connection) = connect_studiofs_postgres(dsn)
            .await
            .context("cannot connect to postgres")?;

        let conn_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!("postgres connection task ended: {e}");
            }
        });

        let tx = client.transaction().await?;

        // Serialize DDL across concurrent initializers to avoid sequence-name races.
        tx.execute("SELECT pg_advisory_xact_lock($1)", &[&902_202_603_i64])
            .await?;

        tx.batch_execute(init_pg_schema_sql()).await?;

        let row = tx
            .query_one(
                init_pg_volume_upsert_sql(),
                &[
                    &name,
                    &uuid,
                    &block_size_i32,
                    &encryption.enabled,
                    &encryption_format_version_i16,
                    &active_key_id_i32,
                ],
            )
            .await?;
        let volume_id: i64 = row.get(0);

        tx.execute(
            init_pg_crypto_key_insert_sql(),
            &[
                &volume_id,
                &active_key_id_i32,
                &active_key.algorithm,
                &active_key.key_material,
                &active_key.state,
            ],
        )
        .await?;

        tx.execute(
            r#"
            INSERT INTO counters (vol, name, value)
            VALUES
                ($1, 'next_inode', 2),
                ($1, 'nextObjectId', 1),
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
        let (client, connection) = connect_studiofs_postgres(dsn)
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
                VALUES ($1, $2, $3)
                ON CONFLICT (vol) DO NOTHING
                "#,
                &[&volume_id, &INITIAL_METADATA_STATE_VERSION, &snapshot_json],
            )
            .await
            .map_err(|e| anyhow::anyhow!("failed to seed metadata_state: {e}"))?;

        conn_task.abort();
        Ok(())
    })
}

// ── Main run function ─────────────────────────────────────────────────────────

fn run(request: InitRunRequest) -> anyhow::Result<()> {
    let InitRunRequest {
        name,
        object_store,
        postgres_dsn,
        deprecated_metadata,
        meta_dir,
    } = request;
    let rt = tokio::runtime::Runtime::new()?;
    let postgres_dsn = rt
        .block_on(async { ensure_studiofs_database(&postgres_dsn).await })
        .map_err(|e| anyhow::anyhow!(e.to_string()))?
        .metadata_dsn;

    if deprecated_metadata.is_present() {
        return Err(anyhow::anyhow!(
            "--metadata-* options are removed. StudioFS now uses PostgreSQL metadata backend only."
        ));
    }

    // ── 1. Ensure buckets are accessible (create if missing) ─────────────────
    println!("Checking storage backends...");
    println!();
    ensure_bucket_accessible(&rt, "data", &object_store)?;
    println!();

    println!("Checking PostgreSQL backend...");
    check_postgres_accessible(&rt, &postgres_dsn)?;
    println!("✓ PostgreSQL connectivity check passed.");
    println!();

    if volume_name_exists(&rt, &postgres_dsn, &name)? {
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
            std::fs::remove_dir_all(&meta_dir)?;
        } else {
            return Err(anyhow::anyhow!("Initialization cancelled."));
        }
    }

    // ── 3. Create metadata directory and initialize local DB ─────────────────
    std::fs::create_dir_all(&meta_dir)?;

    let db_path = meta_dir.join(format!("{name}.db"));
    info!("initializing volume {name:?} at {db_path:?}");

    let engine = RedbMetaEngine::open(&db_path)?;

    let actual_block_size = DEFAULT_BLOCK_SIZE;
    let volume_uuid = Uuid::new_v4().to_string();

    let data_prefix = format!("{name}/objects/");
    let encryption = EncryptionConfig::try_generate()?;
    let postgres_volume_id = init_pg_schema_and_volume(
        &rt,
        &postgres_dsn,
        &name,
        &volume_uuid,
        actual_block_size,
        &encryption,
    )?;

    // ── 6. Persist format info ────────────────────────────────────────────────
    let format_info = FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: name.clone(),
        uuid: volume_uuid,
        endpoint: object_store.endpoint.clone(),
        bucket: object_store.bucket.clone(),
        access_key: object_store.access_key.clone(),
        secret_key: object_store.secret_key.clone(),
        region: object_store.region.clone(),
        data_prefix,
        metadata: MetadataConfig {
            backend: MetadataBackend::Postgres,
            remote: None,
            postgres: Some(PostgresConfig {
                url: postgres_dsn.to_string(),
                volume_id: postgres_volume_id,
            }),
        },
        encryption,
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

    let primed_engine = open_meta_engine_with_options(
        &db_path,
        MetaOpenOptions {
            background_poll: false,
        },
    )?;
    drop(primed_engine);

    // ── 7. Report success ─────────────────────────────────────────────────────
    info!("volume {name:?} initialized successfully");
    println!("✓ Volume '{}' initialized", name);
    println!("  UUID:       {}", format_info.uuid);
    println!(
        "  Block size: {} bytes ({} MiB)",
        actual_block_size,
        actual_block_size / (1024 * 1024)
    );
    println!("  Data endpoint: {}", object_store.endpoint);
    println!("  Data bucket:   {}", object_store.bucket);
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
                assert_eq!(meta, "postgres://u:p@127.0.0.1:5432");
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

    #[test]
    fn init_seeds_empty_pg_head_at_zero() {
        assert_eq!(
            INITIAL_METADATA_STATE_VERSION, 0,
            "init publishes a snapshot before any event_log batch exists, so the initial PG head must not advertise a missing batch"
        );
    }

    #[test]
    fn init_pg_schema_provisions_volume_encryption_metadata() {
        let ddl = init_pg_schema_sql();

        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS volume_crypto_keys"));
        assert!(ddl.contains("encryption_enabled BOOLEAN NOT NULL DEFAULT TRUE"));
        assert!(ddl.contains("encryption_format_version SMALLINT NOT NULL DEFAULT 1"));
        assert!(ddl.contains("active_crypto_key_id INTEGER NOT NULL DEFAULT 1"));
        assert!(ddl.contains("PRIMARY KEY (vol, key_id)"));
    }

    #[test]
    fn volume_upsert_sql_parameterizes_encryption_enabled() {
        let sql = init_pg_volume_upsert_sql();

        assert!(sql.contains("VALUES ($1, $2, $3, $4, $5, $6)"));
        assert!(!sql.contains("VALUES ($1, $2, $3, TRUE, $4, $5)"));
    }

    #[test]
    fn crypto_key_insert_sql_never_overwrites_existing_key_material() {
        let sql = init_pg_crypto_key_insert_sql();

        assert!(sql.contains("ON CONFLICT (vol, key_id) DO NOTHING"));
        assert!(!sql.contains("key_material = EXCLUDED.key_material"));
    }

    #[test]
    fn active_crypto_key_row_uses_format_encryption_config() {
        let encryption = EncryptionConfig::new([0xC7; studiofs_meta::ENCRYPTION_KEY_LENGTH]);
        let row = ActiveCryptoKeyRow::from_encryption(&encryption);

        assert_eq!(row.algorithm, "xchacha20poly1305-v1");
        assert_eq!(
            row.key_material,
            [0xC7; studiofs_meta::ENCRYPTION_KEY_LENGTH]
        );
        assert_eq!(row.state, "active");
    }
}
