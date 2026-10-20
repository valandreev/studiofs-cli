//! Integration tests for `studiofs dump` / `studiofs load`.
//!
//! Requires RustFS/S3 + PostgreSQL test env (ignored by default).

use bytes::{Bytes, BytesMut};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::OnceLock;
use std::time::Duration;
use studiofs_data::object::{
    CLOUD_FRAGMENT_SIZE, create_object_buffer, format_object_fragment_keys, read_object,
};
use studiofs_data::{DataStore, DataStoreConfig};
use studiofs_meta::{
    DEFAULT_CHUNK_SIZE, MetaError, MetaOpenOptions, PayloadRef, ROOT_INO, chunk_id_from_offset,
    chunk_start_offset, ensure_studiofs_database, open_meta_engine, open_meta_engine_with_options,
    schema::{ChunkId, PayloadRefState, SliceId, SliceOrder, SliceRange, SliceRecord},
};
use uuid::Uuid;

struct TestEnv {
    s3_endpoint: String,
    s3_bucket: String,
    s3_access_key: String,
    s3_secret_key: String,
    s3_region: String,
}

fn workspace_root() -> &'static Path {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    ROOT.get_or_init(|| {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest_dir
            .parent()
            .and_then(|dir| dir.parent())
            .unwrap_or(manifest_dir)
            .to_path_buf()
    })
    .as_path()
}

fn workspace_env_path() -> PathBuf {
    workspace_root().join(".env")
}

fn workspace_dotenv_vars() -> &'static BTreeMap<String, String> {
    static VARS: OnceLock<BTreeMap<String, String>> = OnceLock::new();
    VARS.get_or_init(|| {
        let mut vars = BTreeMap::new();
        if let Ok(iter) = dotenvy::from_path_iter(workspace_env_path()) {
            for item in iter.flatten() {
                vars.insert(item.0, item.1);
            }
        }
        vars
    })
}

fn env_or_dotenv(key: &str, default: Option<&str>) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| workspace_dotenv_vars().get(key).cloned())
        .or_else(|| default.map(ToString::to_string))
}

fn load_test_env() -> TestEnv {
    TestEnv {
        s3_endpoint: env_or_dotenv("STUDIOFS_S3_ENDPOINT", Some("http://127.0.0.1:9000"))
            .expect("resolve STUDIOFS_S3_ENDPOINT"),
        s3_bucket: env_or_dotenv("STUDIOFS_S3_BUCKET", Some("studiofs-test"))
            .expect("resolve STUDIOFS_S3_BUCKET"),
        s3_access_key: env_or_dotenv("STUDIOFS_S3_ACCESS_KEY", Some("studiofs"))
            .expect("resolve STUDIOFS_S3_ACCESS_KEY"),
        s3_secret_key: env_or_dotenv("STUDIOFS_S3_SECRET_KEY", Some("studiofs123"))
            .expect("resolve STUDIOFS_S3_SECRET_KEY"),
        s3_region: env_or_dotenv("STUDIOFS_S3_REGION", Some("us-east-1"))
            .expect("resolve STUDIOFS_S3_REGION"),
    }
}

fn random_name(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        Uuid::new_v4().simple()
    )
}

fn set_home_env(cmd: &mut Command, home: &Path) {
    cmd.env("HOME", home);
    cmd.env("USERPROFILE", home);
    cmd.env_remove("STUDIOFS_CLIENT_ROOT");
}

fn run_studiofs(home: &Path, args: &[&str]) -> Output {
    let exe = env!("CARGO_BIN_EXE_studiofs");
    let mut cmd = Command::new(exe);
    cmd.args(args);
    set_home_env(&mut cmd, home);
    cmd.output().expect("run studiofs command")
}

fn assert_ok(output: &Output, context: &str) {
    if output.status.success() {
        return;
    }
    let mut text = String::new();
    text.push_str(&String::from_utf8_lossy(&output.stdout));
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    panic!("{context} failed:\n{text}");
}

fn default_meta_dir(home: &Path, name: &str) -> PathBuf {
    home.join(".studiofs").join("meta").join(name)
}

async fn put_fragmented_object(
    store: &DataStore,
    data_prefix: &str,
    object_id: u64,
    payload: Bytes,
) -> Result<(), String> {
    let keys = format_object_fragment_keys(data_prefix, object_id);
    for (fragment_index, key) in keys.iter().enumerate() {
        let start = fragment_index * CLOUD_FRAGMENT_SIZE;
        let end = (start + CLOUD_FRAGMENT_SIZE).min(payload.len());
        let fragment_payload = if start < payload.len() {
            payload.slice(start..end)
        } else {
            Bytes::new()
        };
        let object = create_object_buffer(fragment_payload)
            .map_err(|e| format!("encode object fragment: {e}"))?;
        store
            .put_by_key(key, object)
            .await
            .map_err(|e| format!("write object fragment: {e}"))?;
    }
    Ok(())
}

async fn get_fragmented_object(
    store: &DataStore,
    data_prefix: &str,
    object_id: u64,
) -> Result<Bytes, String> {
    let keys = format_object_fragment_keys(data_prefix, object_id);
    let mut payload = BytesMut::new();
    for key in &keys {
        let raw = store
            .get_by_key(key)
            .await
            .map_err(|e| format!("read object fragment: {e}"))?;
        let fragment = read_object(raw).map_err(|e| format!("decode object fragment: {e}"))?;
        payload.extend_from_slice(&fragment);
    }
    Ok(payload.freeze())
}

async fn resolved_pg_dsn(raw: &str) -> String {
    ensure_studiofs_database(raw)
        .await
        .expect("resolve pg dsn")
        .metadata_dsn
}

async fn delete_volume_by_name(pg_dsn: &str, name: &str) {
    let (client, connection) = tokio_postgres::connect(pg_dsn, tokio_postgres::NoTls)
        .await
        .expect("connect postgres");
    let conn_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .execute("DELETE FROM volumes WHERE name = $1", &[&name])
        .await
        .expect("delete volume by name");
    conn_task.abort();
}

async fn volume_exists(pg_dsn: &str, name: &str) -> bool {
    let (client, connection) = tokio_postgres::connect(pg_dsn, tokio_postgres::NoTls)
        .await
        .expect("connect postgres");
    let conn_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM volumes WHERE name = $1)",
            &[&name],
        )
        .await
        .expect("query volume exists");
    conn_task.abort();
    row.get::<_, bool>(0)
}

async fn seed_stale_snapshot_v2_rows(pg_dsn: &str, name: &str) {
    let (client, connection) = tokio_postgres::connect(pg_dsn, tokio_postgres::NoTls)
        .await
        .expect("connect postgres");
    let conn_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let volume_id: i64 = client
        .query_one("SELECT vol FROM volumes WHERE name = $1", &[&name])
        .await
        .expect("find target volume")
        .get(0);
    let snapshot_id = format!("{name}.2026-08-12T14-07-09.000Z");
    client
        .execute(
            "INSERT INTO snapshot_catalog (vol, id, origin, cut_version, captured_at, lifecycle, retention_seconds, ready_at) VALUES ($1, $2, 'manual', 0, clock_timestamp(), 'ready', NULL, clock_timestamp())",
            &[&volume_id, &snapshot_id],
        )
        .await
        .expect("seed stale snapshot catalog row");
    let checksum = vec![0_u8; 32];
    client
        .execute(
            "INSERT INTO snapshot_artifacts (vol, snapshot_id, object_key, format_version, schema_version, sha256, byte_length, verified_at) VALUES ($1, $2, 'snapshots/stale-v2/artifact', 1, 1, $3, 0, clock_timestamp())",
            &[&volume_id, &snapshot_id, &checksum],
        )
        .await
        .expect("seed snapshot artifact row");
    client
        .execute(
            "INSERT INTO snapshot_policies (vol, generation, daily_at, timezone, retention_seconds) VALUES ($1, 1, '02:30:00', 'UTC', 3600)",
            &[&volume_id],
        )
        .await
        .expect("seed stale snapshot policy");
    conn_task.abort();
}

async fn snapshot_v2_state(pg_dsn: &str, name: &str) -> (i64, i64, i64, Option<String>) {
    let (client, connection) = tokio_postgres::connect(pg_dsn, tokio_postgres::NoTls)
        .await
        .expect("connect postgres");
    let conn_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            "SELECT (SELECT count(*) FROM snapshot_catalog WHERE vol = v.vol), (SELECT count(*) FROM snapshot_policies WHERE vol = v.vol), (SELECT count(*) FROM snapshot_artifacts WHERE vol = v.vol), (SELECT object_key FROM snapshot_artifacts WHERE vol = v.vol LIMIT 1) FROM volumes v WHERE v.name = $1",
            &[&name],
        )
        .await
        .expect("count snapshot v2 rows");
    conn_task.abort();
    (row.get(0), row.get(1), row.get(2), row.get(3))
}

fn single_object_slice_graph(
    payload: &PayloadRef,
) -> Result<Vec<(ChunkId, Vec<SliceRecord>)>, MetaError> {
    let mut chunks = BTreeMap::<ChunkId, Vec<SliceRecord>>::new();
    let mut cursor = 0_u64;
    let data_end = u64::from(payload.length);
    while cursor < data_end {
        let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
        let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
        let piece_end = data_end.min(chunk_start.saturating_add(DEFAULT_CHUNK_SIZE as u64));
        let range = SliceRange::new(
            u32::try_from(cursor - chunk_start)
                .map_err(|_| MetaError::Storage("slice start overflow".to_string()))?,
            u32::try_from(piece_end - chunk_start)
                .map_err(|_| MetaError::Storage("slice end overflow".to_string()))?,
        )
        .ok_or_else(|| MetaError::Storage("invalid slice range".to_string()))?;
        let mut slice = SliceRecord::new(
            SliceId(0),
            SliceOrder(0),
            range,
            PayloadRef {
                object_id: payload.object_id,
                offset: payload.offset.saturating_add(
                    u32::try_from(cursor)
                        .map_err(|_| MetaError::Storage("slice offset overflow".to_string()))?,
                ),
                length: u32::try_from(piece_end - cursor)
                    .map_err(|_| MetaError::Storage("slice length overflow".to_string()))?,
                flags: payload.flags,
            },
        );
        slice.payload_state = PayloadRefState::LocalOnly;
        chunks.entry(chunk_id).or_default().push(slice);
        cursor = piece_end;
    }
    Ok(chunks.into_iter().collect())
}

async fn seed_remote_file(home: &Path, disk_name: &str, payload: &[u8]) -> u64 {
    let db_path = default_meta_dir(home, disk_name).join(format!("{disk_name}.db"));
    let meta = open_meta_engine_with_options(
        &db_path,
        MetaOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open meta engine");
    let format_info = meta.get_format_info().expect("get format info");

    let file = meta
        .create_file(ROOT_INO, b"payload.bin", 0, 0, 0o644)
        .expect("create file");
    let object_id = meta.next_object_id().expect("next object ID");
    let block = PayloadRef::new_partial(object_id, payload.len() as u32);
    let mut attr = meta.get_attr(file.ino).expect("get attr");
    attr.size = payload.len() as u64;
    meta.set_attr(&attr).expect("set attr");
    let expected_epoch = attr.content_epoch;
    meta.flush_regular_file_overlay_slices(
        file.ino,
        single_object_slice_graph(&block)
            .expect("slice graph")
            .as_slice(),
    )
    .expect("flush slice graph");
    assert!(
        meta.mark_regular_file_overlay_object_uploaded_if_matches(
            file.ino,
            expected_epoch,
            object_id
        )
        .expect("mark uploaded slice object")
    );
    assert!(
        meta.publish_regular_file_overlay_to_base_if_matches(file.ino, expected_epoch)
            .expect("publish slice graph")
    );

    let store = DataStore::new(&DataStoreConfig::from(&format_info)).expect("datastore");
    put_fragmented_object(
        &store,
        &format_info.data_prefix,
        object_id,
        Bytes::copy_from_slice(payload),
    )
    .await
    .expect("put object");

    meta.flush_metadata().expect("flush metadata");
    // The next step shells out to `studiofs dump`, which re-opens this same
    // redb file. Ensure the local probe handle is dropped before that.
    drop(meta);
    object_id
}

async fn verify_payload_from_discovered(home: &Path, disk_name: &str, expected: &[u8]) {
    let db_path = default_meta_dir(home, disk_name).join(format!("{disk_name}.db"));
    {
        let meta = open_meta_engine_with_options(
            &db_path,
            MetaOpenOptions {
                background_poll: false,
                ..Default::default()
            },
        )
        .expect("open discovered meta");
        let format_info = meta.get_format_info().expect("format info");
        let entry = meta
            .lookup(ROOT_INO, b"payload.bin")
            .expect("lookup payload.bin");
        let attr = meta.get_attr(entry.ino).expect("get payload attr");
        assert_eq!(attr.committed_size, expected.len() as u64);

        let mut spans = meta
            .lookup_regular_file_slices(entry.ino, 0, attr.committed_size)
            .expect("lookup committed slices");
        spans.sort_by_key(|span| (span.chunk_id.0, span.range.start, span.range.end));

        let store = DataStore::new(&DataStoreConfig::from(&format_info)).expect("datastore");
        let mut object_cache: BTreeMap<u64, Bytes> = BTreeMap::new();
        let mut decoded = vec![0_u8; expected.len()];
        let mut covered = vec![false; expected.len()];

        for span in spans {
            let block = span.payload.expect("committed slice must reference data");
            let file_start = span
                .chunk_id
                .0
                .saturating_mul(u64::from(DEFAULT_CHUNK_SIZE))
                .saturating_add(u64::from(span.range.start));
            let file_end = span
                .chunk_id
                .0
                .saturating_mul(u64::from(DEFAULT_CHUNK_SIZE))
                .saturating_add(u64::from(span.range.end));
            let file_start = usize::try_from(file_start).expect("file start fits usize");
            let file_end = usize::try_from(file_end).expect("file end fits usize");
            let block_start = usize::try_from(block.offset).expect("block offset fits usize");
            let block_len = usize::try_from(block.length).expect("block length fits usize");
            let block_end = block_start + block_len;

            assert!(
                file_end <= expected.len(),
                "resolved span stays within file"
            );
            assert_eq!(
                file_end - file_start,
                block_len,
                "span length matches block length"
            );

            let object = if let Some(cached) = object_cache.get(&block.object_id) {
                cached.clone()
            } else {
                let fetched =
                    get_fragmented_object(&store, &format_info.data_prefix, block.object_id)
                        .await
                        .expect("get object");
                object_cache.insert(block.object_id, fetched.clone());
                fetched
            };

            decoded[file_start..file_end].copy_from_slice(&object[block_start..block_end]);
            covered[file_start..file_end].fill(true);
        }

        assert!(
            covered.into_iter().all(|covered| covered),
            "committed payload spans must cover the entire file"
        );
        assert_eq!(decoded, expected);
        drop(meta);
    }
}

#[tokio::test]
#[ignore = "requires RustFS/S3 + PostgreSQL test env (STUDIOFS_S3_*, STUDIOFS_TEST_PG_DSN)"]
async fn dump_load_restores_volume_and_discovery_reads_data() {
    let env = load_test_env();
    let raw_pg = env_or_dotenv("STUDIOFS_TEST_PG_DSN", None)
        .expect("STUDIOFS_TEST_PG_DSN is required for ignored S3/PostgreSQL tests");

    let disk_name = random_name("dump-load-a");
    let home_a = studiofs_test_support::workspace_target_tempdir("home-a-");
    let dump_path = home_a.path().join(format!("{disk_name}.dump.json"));
    let payload = b"studiofs-dump-load-payload-a";

    let init = run_studiofs(
        home_a.path(),
        &[
            "init",
            &disk_name,
            "--endpoint",
            &env.s3_endpoint,
            "--bucket",
            &env.s3_bucket,
            "--access-key",
            &env.s3_access_key,
            "--secret-key",
            &env.s3_secret_key,
            "--region",
            &env.s3_region,
            "--meta",
            &raw_pg,
        ],
    );
    assert_ok(&init, "init");

    seed_remote_file(home_a.path(), &disk_name, payload).await;

    let dump = run_studiofs(
        home_a.path(),
        &[
            "dump",
            &disk_name,
            "--path-to-dump",
            dump_path.to_str().unwrap(),
        ],
    );
    assert_ok(&dump, "dump");

    let pg_dsn = resolved_pg_dsn(&raw_pg).await;
    delete_volume_by_name(&pg_dsn, &disk_name).await;
    assert!(
        !volume_exists(&pg_dsn, &disk_name).await,
        "volume must be deleted"
    );

    let load = run_studiofs(
        home_a.path(),
        &[
            "load",
            "--path-to-dump",
            dump_path.to_str().unwrap(),
            "--postgresql-adress",
            &raw_pg,
        ],
    );
    assert_ok(&load, "load");
    assert!(
        volume_exists(&pg_dsn, &disk_name).await,
        "volume must be restored"
    );

    let home_b = studiofs_test_support::workspace_target_tempdir("home-b-");
    let discover = run_studiofs(home_b.path(), &["discover", &disk_name, "--meta", &raw_pg]);
    assert_ok(&discover, "discover after load");
    verify_payload_from_discovered(home_b.path(), &disk_name, payload).await;
}

#[tokio::test]
#[ignore = "requires RustFS/S3 + PostgreSQL test env (STUDIOFS_S3_*, STUDIOFS_TEST_PG_DSN)"]
async fn dump_load_rejects_target_with_snapshot_v2_without_breaking_other_volumes() {
    let env = load_test_env();
    let raw_pg = env_or_dotenv("STUDIOFS_TEST_PG_DSN", None)
        .expect("STUDIOFS_TEST_PG_DSN is required for ignored S3/PostgreSQL tests");

    let target = random_name("dump-load-target");
    let extra1 = random_name("dump-load-extra1");
    let extra2 = random_name("dump-load-extra2");
    let home = studiofs_test_support::workspace_target_tempdir("home-");
    let dump_path = home.path().join(format!("{target}.dump.json"));
    let payload = b"studiofs-dump-load-payload-b";

    let init_target = run_studiofs(
        home.path(),
        &[
            "init",
            &target,
            "--endpoint",
            &env.s3_endpoint,
            "--bucket",
            &env.s3_bucket,
            "--access-key",
            &env.s3_access_key,
            "--secret-key",
            &env.s3_secret_key,
            "--region",
            &env.s3_region,
            "--meta",
            &raw_pg,
        ],
    );
    assert_ok(&init_target, "init target");
    seed_remote_file(home.path(), &target, payload).await;

    let dump = run_studiofs(
        home.path(),
        &[
            "dump",
            &target,
            "--path-to-dump",
            dump_path.to_str().unwrap(),
        ],
    );
    assert_ok(&dump, "dump target");

    let pg_dsn = resolved_pg_dsn(&raw_pg).await;
    seed_stale_snapshot_v2_rows(&pg_dsn, &target).await;

    for disk in [&extra1, &extra2] {
        let out = run_studiofs(
            home.path(),
            &[
                "init",
                disk,
                "--endpoint",
                &env.s3_endpoint,
                "--bucket",
                &env.s3_bucket,
                "--access-key",
                &env.s3_access_key,
                "--secret-key",
                &env.s3_secret_key,
                "--region",
                &env.s3_region,
                "--meta",
                &raw_pg,
            ],
        );
        assert_ok(&out, "init extra");
    }

    let load = run_studiofs(
        home.path(),
        &[
            "load",
            "--path-to-dump",
            dump_path.to_str().unwrap(),
            "--postgresql-adress",
            &raw_pg,
        ],
    );
    assert!(
        !load.status.success(),
        "load must reject a target with Snapshot v2 state"
    );
    assert_eq!(
        snapshot_v2_state(&pg_dsn, &target).await,
        (1, 1, 1, Some("snapshots/stale-v2/artifact".to_string())),
        "rejected load must preserve Snapshot v2 rows and artifact key"
    );

    let verify_home = studiofs_test_support::workspace_target_tempdir("verify-home-");
    let discover_target = run_studiofs(
        verify_home.path(),
        &["discover", &target, "--meta", &raw_pg],
    );
    assert_ok(&discover_target, "discover restored target");
    verify_payload_from_discovered(verify_home.path(), &target, payload).await;

    for disk in [&extra1, &extra2] {
        let out = run_studiofs(verify_home.path(), &["discover", disk, "--meta", &raw_pg]);
        assert_ok(&out, "discover extra volume");
        let db_path = default_meta_dir(verify_home.path(), disk).join(format!("{disk}.db"));
        let meta = open_meta_engine(&db_path).expect("open extra meta");
        let root = meta.get_attr(ROOT_INO).expect("extra root inode");
        assert_eq!(root.ino, ROOT_INO);
    }
}

/// Reachability accounting and payload upload ledger tables that `load`
/// requires to be empty for the target volume.
///
/// Mirrors `ACCOUNTING_TABLES` in `src/cli/dump_load.rs`; `studiofs` is a
/// binary crate, so integration tests cannot import it. Keep both lists equal:
/// a table dropped from the source list makes the per-table rejection test
/// fail, but a table added there is untested until it is added here.
const ACCOUNTING_TABLES: [&str; 5] = [
    "snapshot_current_references",
    "snapshot_object_reachability",
    "snapshot_reachability_batches",
    "snapshot_reachability_bootstrap",
    "snapshot_payload_upload_objects",
];

const ACCOUNTING_REJECTION: &str =
    "cannot load metadata over existing reachability or payload upload state";

/// Counter name and event type for rows that make the target newer than its dump.
const NEWER_STATE_MARKER: &str = "load-guard-newer-state";
const NEWER_STATE_COUNTER_VALUE: i64 = 4_242;
const NEWER_BLOCK_SIZE: i32 = 65_536;
const ACCOUNTING_REFERENCE_INO: i64 = 4_242;
const LOCK_WAIT_DEADLINE: Duration = Duration::from_secs(30);
/// Mirrors `INIT_SCHEMA_ADVISORY_LOCK` in `src/cli/init.rs`, which
/// `apply_dump` takes before touching the volume row; `studiofs` is a binary
/// crate, so integration tests cannot import it. If the values drift, `load`
/// no longer parks on this lock and the race test fails with "load exited
/// ... before waiting on %pg_advisory_xact_lock%" instead of passing.
const LOAD_ADVISORY_LOCK: i64 = 902_202_603;

/// Target state that a rejected load must leave untouched.
#[derive(Debug, PartialEq, Eq)]
struct TargetVolumeState {
    name: String,
    block_size: i32,
    metadata_version: i64,
    marker_counter: Option<i64>,
    marker_events: i64,
}

/// Test volumes whose PostgreSQL rows and S3 payloads are removed even when a
/// scenario panics.
struct LoadGuardFixture {
    env: TestEnv,
    raw_pg: String,
    pg_dsn: String,
    home: tempfile::TempDir,
    dump_path: PathBuf,
    client: tokio_postgres::Client,
    connection: tokio::task::JoinHandle<()>,
    /// Every name a test volume may carry, including names set by a scenario.
    volume_names: Vec<String>,
    /// Payload stores captured right after `init`. A load replaces the PG
    /// state the source client's local metadata was built from, so cleanup
    /// cannot reopen that local metadata afterwards.
    payload_stores: std::sync::Mutex<Vec<(String, DataStore)>>,
}

impl LoadGuardFixture {
    async fn connect(volume_names: Vec<String>) -> Self {
        let env = load_test_env();
        let raw_pg = env_or_dotenv("STUDIOFS_TEST_PG_DSN", None)
            .expect("STUDIOFS_TEST_PG_DSN is required for ignored S3/PostgreSQL tests");
        let pg_dsn = resolved_pg_dsn(&raw_pg).await;
        let (client, connection) = connect_pg(&pg_dsn).await;
        let home = studiofs_test_support::workspace_target_tempdir("home-");
        let dump_path = home.path().join("load-guard.dump.json");
        Self {
            env,
            raw_pg,
            pg_dsn,
            home,
            dump_path,
            client,
            connection,
            volume_names,
            payload_stores: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn init_volume(&self, name: &str) {
        let init = run_studiofs(
            self.home.path(),
            &[
                "init",
                name,
                "--endpoint",
                &self.env.s3_endpoint,
                "--bucket",
                &self.env.s3_bucket,
                "--access-key",
                &self.env.s3_access_key,
                "--secret-key",
                &self.env.s3_secret_key,
                "--region",
                &self.env.s3_region,
                "--meta",
                &self.raw_pg,
            ],
        );
        assert_ok(&init, "init volume");

        let db_path = default_meta_dir(self.home.path(), name).join(format!("{name}.db"));
        let meta = open_meta_engine_with_options(
            &db_path,
            MetaOpenOptions {
                background_poll: false,
                ..Default::default()
            },
        )
        .expect("open initialized volume");
        let format = meta.get_format_info().expect("read initialized format");
        drop(meta);
        let store = DataStore::new(&DataStoreConfig::from(&format)).expect("open payload store");
        self.payload_stores
            .lock()
            .expect("payload store list is not poisoned")
            .push((name.to_string(), store));
    }

    /// Publishes one payload to `name`, dumps the volume, and returns the
    /// payload object ID.
    async fn seed_and_dump(&self, name: &str, payload: &[u8]) -> i64 {
        let object_id = seed_remote_file(self.home.path(), name, payload).await;
        let dump = run_studiofs(
            self.home.path(),
            &["dump", name, "--path-to-dump", self.dump_path_str()],
        );
        assert_ok(&dump, "dump target");
        i64::try_from(object_id).expect("payload object ID fits BIGINT")
    }

    fn dump_path_str(&self) -> &str {
        self.dump_path.to_str().expect("dump path is UTF-8")
    }

    fn load_args(&self) -> [&str; 5] {
        [
            "load",
            "--path-to-dump",
            self.dump_path_str(),
            "--postgresql-address",
            &self.raw_pg,
        ]
    }

    fn load(&self) -> Output {
        run_studiofs(self.home.path(), &self.load_args())
    }

    fn spawn_load(&self) -> std::process::Child {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_studiofs"));
        cmd.args(self.load_args())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        set_home_env(&mut cmd, self.home.path());
        cmd.spawn().expect("spawn studiofs load")
    }

    async fn volume_id(&self, name: &str) -> i64 {
        self.client
            .query_one("SELECT vol FROM volumes WHERE name = $1", &[&name])
            .await
            .expect("find volume")
            .get(0)
    }

    async fn table_rows(&self, table: &str, volume_id: i64) -> i64 {
        self.client
            .query_one(
                &format!("SELECT count(*) FROM {table} WHERE vol = $1"),
                &[&volume_id],
            )
            .await
            .expect("count volume rows")
            .get(0)
    }

    async fn clear_accounting(&self, volume_id: i64) {
        for table in ACCOUNTING_TABLES {
            self.client
                .execute(
                    &format!("DELETE FROM {table} WHERE vol = $1"),
                    &[&volume_id],
                )
                .await
                .expect("clear volume accounting");
        }
    }

    async fn seed_accounting_row(&self, table: &str, volume_id: i64, object_id: i64, version: i64) {
        seed_accounting_row(&self.client, table, volume_id, object_id, version).await;
    }

    /// Makes the target observably newer than its dump: identity fields,
    /// metadata version, a counter, and an event row.
    async fn advance_past_dump(&self, volume_id: i64, newer_name: &str) -> TargetVolumeState {
        self.client
            .execute(
                "INSERT INTO counters (vol, name, value) VALUES ($1, $2, $3)",
                &[&volume_id, &NEWER_STATE_MARKER, &NEWER_STATE_COUNTER_VALUE],
            )
            .await
            .expect("seed newer counter");
        self.client
            .execute(
                "UPDATE volumes SET name = $2, block_size = $3 WHERE vol = $1",
                &[&volume_id, &newer_name, &NEWER_BLOCK_SIZE],
            )
            .await
            .expect("advance volume identity fields");
        let metadata_version: i64 = self
            .client
            .query_one(
                "UPDATE metadata_state SET version = version + 1 WHERE vol = $1 RETURNING version",
                &[&volume_id],
            )
            .await
            .expect("advance metadata version")
            .get(0);
        self.client
            .execute(
                "INSERT INTO event_log (vol, batch_version, event_type, event_payload) VALUES ($1, $2, $3, $4)",
                &[&volume_id, &metadata_version, &NEWER_STATE_MARKER, &Vec::<u8>::new()],
            )
            .await
            .expect("seed newer event");

        let state = self.target_state(volume_id).await;
        assert_eq!(
            state,
            TargetVolumeState {
                name: newer_name.to_string(),
                block_size: NEWER_BLOCK_SIZE,
                metadata_version,
                marker_counter: Some(NEWER_STATE_COUNTER_VALUE),
                marker_events: 1,
            }
        );
        state
    }

    async fn target_state(&self, volume_id: i64) -> TargetVolumeState {
        let row = self
            .client
            .query_one(
                "SELECT v.name, v.block_size, ms.version, (SELECT value FROM counters WHERE vol = v.vol AND name = $2), (SELECT count(*) FROM event_log WHERE vol = v.vol AND event_type = $2) FROM volumes v JOIN metadata_state ms ON ms.vol = v.vol WHERE v.vol = $1",
                &[&volume_id, &NEWER_STATE_MARKER],
            )
            .await
            .expect("read target volume state");
        TargetVolumeState {
            name: row.get(0),
            block_size: row.get(1),
            metadata_version: row.get(2),
            marker_counter: row.get(3),
            marker_events: row.get(4),
        }
    }

    /// Waits until another backend blocks on a lock while running a statement
    /// that matches `query_pattern`, or fails if `load` exits first or the
    /// deadline passes.
    async fn wait_for_load_lock_wait(&self, load: &mut std::process::Child, query_pattern: &str) {
        let deadline = tokio::time::Instant::now() + LOCK_WAIT_DEADLINE;
        loop {
            let waiting: bool = self
                .client
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND wait_event_type = 'Lock' AND query LIKE $1)",
                    &[&query_pattern],
                )
                .await
                .expect("inspect lock waits")
                .get(0);
            if waiting {
                return;
            }
            if let Some(status) = load.try_wait().expect("poll load process") {
                panic!("load exited with {status} before waiting on {query_pattern}");
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "load did not wait on {query_pattern} within {LOCK_WAIT_DEADLINE:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Removes test volumes from PostgreSQL and S3 and returns every failure.
    async fn cleanup(self) -> Vec<String> {
        let mut failures = Vec::new();
        let payload_stores = self
            .payload_stores
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (name, store) in payload_stores {
            if let Err(error) = store.delete_volume_data().await {
                failures.push(format!("delete payloads of {name}: {error}"));
            }
        }
        if let Err(error) = self
            .client
            .execute(
                "DELETE FROM volumes WHERE name = ANY($1::TEXT[])",
                &[&self.volume_names],
            )
            .await
        {
            failures.push(format!("delete test volumes: {error}"));
        }
        drop(self.client);
        self.connection.abort();
        let _ = self.connection.await;
        failures
    }
}

async fn connect_pg(pg_dsn: &str) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let (client, connection) = tokio_postgres::connect(pg_dsn, tokio_postgres::NoTls)
        .await
        .expect("connect postgres");
    let connection = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, connection)
}

async fn seed_accounting_row(
    client: &tokio_postgres::Client,
    table: &str,
    volume_id: i64,
    object_id: i64,
    version: i64,
) {
    let inserted = match table {
        "snapshot_current_references" => {
            client
                .execute(
                    "INSERT INTO snapshot_current_references (vol, ino, object_id) VALUES ($1, $2, $3)",
                    &[&volume_id, &ACCOUNTING_REFERENCE_INO, &object_id],
                )
                .await
        }
        "snapshot_object_reachability" => {
            client
                .execute(
                    "INSERT INTO snapshot_object_reachability (vol, object_id, start_version) VALUES ($1, $2, $3)",
                    &[&volume_id, &object_id, &version],
                )
                .await
        }
        "snapshot_reachability_batches" => {
            client
                .execute(
                    "INSERT INTO snapshot_reachability_batches (vol, batch_version) VALUES ($1, $2)",
                    &[&volume_id, &version],
                )
                .await
        }
        "snapshot_reachability_bootstrap" => {
            client
                .execute(
                    "INSERT INTO snapshot_reachability_bootstrap (vol, initialized_version) VALUES ($1, $2)",
                    &[&volume_id, &version],
                )
                .await
        }
        "snapshot_payload_upload_objects" => {
            client
                .execute(
                    "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state) VALUES ($1, $2, 'published')",
                    &[&volume_id, &object_id],
                )
                .await
        }
        other => panic!("unknown accounting table {other}"),
    };
    assert_eq!(
        inserted.expect("seed accounting row"),
        1,
        "seed one {table} row"
    );
}

fn combined_output(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// Runs `scenario` and then cleans the fixture's volumes, re-raising a
/// scenario panic only after cleanup.
fn run_load_guard_test(volume_names: Vec<String>, scenario: impl AsyncFnOnce(&LoadGuardFixture)) {
    let runtime = tokio::runtime::Runtime::new().expect("build test runtime");
    let fixture = runtime.block_on(LoadGuardFixture::connect(volume_names));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.block_on(scenario(&fixture));
    }));
    let cleanup_failures = runtime.block_on(fixture.cleanup());
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    assert!(
        cleanup_failures.is_empty(),
        "clean test volumes: {cleanup_failures:?}"
    );
}

#[test]
#[ignore = "requires RustFS/S3 + PostgreSQL test env (STUDIOFS_S3_*, STUDIOFS_TEST_PG_DSN)"]
fn dump_load_into_new_metadata_server_ignores_other_volume_accounting() {
    let target = random_name("dump-load-guard-target");
    let bystander = random_name("dump-load-guard-bystander");
    run_load_guard_test(vec![target.clone(), bystander.clone()], async |fixture| {
        let payload = b"payload read back after migrating metadata";
        fixture.init_volume(&target);
        fixture.init_volume(&bystander);
        let object_id = fixture.seed_and_dump(&target, payload).await;

        // The new metadata server has no row for the dumped UUID; dropping
        // the source row cascades its accounting away.
        delete_volume_by_name(&fixture.pg_dsn, &target).await;
        let bystander_volume = fixture.volume_id(&bystander).await;
        for table in ACCOUNTING_TABLES {
            fixture
                .seed_accounting_row(table, bystander_volume, object_id, 1)
                .await;
        }

        assert_ok(&fixture.load(), "load next to another volume's accounting");
        for table in ACCOUNTING_TABLES {
            assert_eq!(
                fixture.table_rows(table, bystander_volume).await,
                1,
                "load must keep the other volume's {table} row"
            );
        }

        let verify_home = studiofs_test_support::workspace_target_tempdir("verify-home-");
        let discover = run_studiofs(
            verify_home.path(),
            &["discover", &target, "--meta", &fixture.raw_pg],
        );
        assert_ok(&discover, "discover loaded target");
        verify_payload_from_discovered(verify_home.path(), &target, payload).await;
    });
}

#[test]
#[ignore = "requires RustFS/S3 + PostgreSQL test env (STUDIOFS_S3_*, STUDIOFS_TEST_PG_DSN)"]
fn dump_load_rejects_replacing_uuid_with_reachability_or_upload_state() {
    let target = random_name("dump-load-guard-target");
    let newer_name = format!("{target}-newer");
    run_load_guard_test(vec![target.clone(), newer_name.clone()], async |fixture| {
        fixture.init_volume(&target);
        let object_id = fixture
            .seed_and_dump(&target, b"payload referenced by the older dump")
            .await;
        let target_volume = fixture.volume_id(&target).await;
        // Seed one table at a time so each rejection proves its own guard.
        fixture.clear_accounting(target_volume).await;
        let newer = fixture.advance_past_dump(target_volume, &newer_name).await;

        for table in ACCOUNTING_TABLES {
            // A rollback would reuse the newer version for its next batch.
            fixture
                .seed_accounting_row(table, target_volume, object_id, newer.metadata_version)
                .await;

            let load = fixture.load();
            let output = combined_output(&load);
            assert!(
                !load.status.success(),
                "load must reject target rows in {table}; output: {output}"
            );
            assert!(
                output.contains(&format!("{ACCOUNTING_REJECTION} ({table})")),
                "load should name {table} as the blocking state; output: {output}"
            );
            assert_eq!(
                fixture.target_state(target_volume).await,
                newer,
                "rejected load must preserve the newer target state for {table}"
            );
            assert_eq!(
                fixture.table_rows(table, target_volume).await,
                1,
                "rejected load must preserve the {table} row"
            );
            fixture.clear_accounting(target_volume).await;
        }
    });
}

#[test]
#[ignore = "requires RustFS/S3 + PostgreSQL test env (STUDIOFS_S3_*, STUDIOFS_TEST_PG_DSN)"]
fn dump_load_rejects_accounting_row_committed_while_load_waits() {
    let target = random_name("dump-load-guard-target");
    run_load_guard_test(vec![target.clone()], async |fixture| {
        fixture.init_volume(&target);
        let object_id = fixture
            .seed_and_dump(&target, b"payload raced by an accounting writer")
            .await;
        let target_volume = fixture.volume_id(&target).await;
        fixture.clear_accounting(target_volume).await;

        // Hold the load's advisory lock so `load` finishes its schema DDL and
        // then parks before touching the volume row.
        let (gate, gate_connection) = connect_pg(&fixture.pg_dsn).await;
        gate.batch_execute(&format!(
            "BEGIN; SELECT pg_advisory_xact_lock({LOAD_ADVISORY_LOCK})"
        ))
        .await
        .expect("hold load advisory lock");
        let mut load = fixture.spawn_load();
        fixture
            .wait_for_load_lock_wait(&mut load, "%pg_advisory_xact_lock%")
            .await;

        // An uncommitted FK-backed insert holds KEY SHARE on the volume row,
        // which only the load's explicit FOR UPDATE conflicts with.
        let (writer, writer_connection) = connect_pg(&fixture.pg_dsn).await;
        writer.batch_execute("BEGIN").await.expect("begin writer");
        seed_accounting_row(
            &writer,
            "snapshot_reachability_batches",
            target_volume,
            object_id,
            1,
        )
        .await;
        gate.batch_execute("COMMIT")
            .await
            .expect("release load advisory lock");
        fixture
            .wait_for_load_lock_wait(&mut load, "%FROM volumes%FOR UPDATE%")
            .await;
        writer.batch_execute("COMMIT").await.expect("commit writer");
        for (client, connection) in [(gate, gate_connection), (writer, writer_connection)] {
            drop(client);
            connection.abort();
            let _ = connection.await;
        }

        let load = tokio::task::spawn_blocking(move || load.wait_with_output())
            .await
            .expect("join load waiter")
            .expect("wait for load");
        let output = combined_output(&load);
        assert!(
            !load.status.success(),
            "load must see the row committed while it waited; output: {output}"
        );
        assert!(
            output.contains(&format!(
                "{ACCOUNTING_REJECTION} (snapshot_reachability_batches)"
            )),
            "load should name the raced table; output: {output}"
        );
    });
}
