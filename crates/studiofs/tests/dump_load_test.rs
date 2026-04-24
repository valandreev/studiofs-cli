//! Integration tests for `studiofs dump` / `studiofs load`.
//!
//! Requires RustFS/S3 + PostgreSQL test env (ignored by default).

use bytes::{Bytes, BytesMut};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::OnceLock;
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

fn load_test_env() -> Option<TestEnv> {
    Some(TestEnv {
        s3_endpoint: env_or_dotenv("STUDIOFS_S3_ENDPOINT", Some("http://127.0.0.1:9000"))?,
        s3_bucket: env_or_dotenv("STUDIOFS_S3_BUCKET", Some("studiofs-test"))?,
        s3_access_key: env_or_dotenv("STUDIOFS_S3_ACCESS_KEY", Some("studiofs"))?,
        s3_secret_key: env_or_dotenv("STUDIOFS_S3_SECRET_KEY", Some("studiofs123"))?,
        s3_region: env_or_dotenv("STUDIOFS_S3_REGION", Some("us-east-1"))?,
    })
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

fn single_object_slice_graph(
    block: &PayloadRef,
) -> Result<Vec<(ChunkId, Vec<SliceRecord>)>, MetaError> {
    let mut chunks = BTreeMap::<ChunkId, Vec<SliceRecord>>::new();
    let mut cursor = 0_u64;
    let data_end = u64::from(block.length);
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
                object_id: block.object_id,
                offset: block.offset.saturating_add(
                    u32::try_from(cursor)
                        .map_err(|_| MetaError::Storage("slice offset overflow".to_string()))?,
                ),
                length: u32::try_from(piece_end - cursor)
                    .map_err(|_| MetaError::Storage("slice length overflow".to_string()))?,
                flags: block.flags,
            },
        );
        slice.payload_state = PayloadRefState::LocalOnly;
        chunks.entry(chunk_id).or_default().push(slice);
        cursor = piece_end;
    }
    Ok(chunks.into_iter().collect())
}

async fn seed_remote_file(home: &Path, disk_name: &str, payload: &[u8]) {
    let db_path = default_meta_dir(home, disk_name).join(format!("{disk_name}.db"));
    {
        let meta = open_meta_engine_with_options(
            &db_path,
            MetaOpenOptions {
                background_poll: false,
            },
        )
        .expect("open meta engine");
        let format_info = meta.get_format_info().expect("get format info");

        let file = meta
            .create_file(ROOT_INO, b"payload.bin", 0, 0, 0o644)
            .expect("create file");
        let object_id = meta.next_block_id().expect("next block id");
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
    }
}

async fn verify_payload_from_discovered(home: &Path, disk_name: &str, expected: &[u8]) {
    let db_path = default_meta_dir(home, disk_name).join(format!("{disk_name}.db"));
    {
        let meta = open_meta_engine_with_options(
            &db_path,
            MetaOpenOptions {
                background_poll: false,
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
            let block = span.block.expect("committed slice must reference data");
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
#[ignore]
async fn dump_load_restores_volume_and_discovery_reads_data() {
    let Some(env) = load_test_env() else {
        eprintln!("skipping: env not ready");
        return;
    };
    let Some(raw_pg) = env_or_dotenv("STUDIOFS_TEST_PG_DSN", None) else {
        eprintln!("skipping: STUDIOFS_TEST_PG_DSN is not set");
        return;
    };

    let disk_name = random_name("dump-load-a");
    let home_a = tempfile::tempdir().expect("home_a");
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

    let home_b = tempfile::tempdir().expect("home_b");
    let discover = run_studiofs(home_b.path(), &["discover", &disk_name, "--meta", &raw_pg]);
    assert_ok(&discover, "discover after load");
    verify_payload_from_discovered(home_b.path(), &disk_name, payload).await;
}

#[tokio::test]
#[ignore]
async fn dump_load_restores_target_volume_without_breaking_other_volumes() {
    let Some(env) = load_test_env() else {
        eprintln!("skipping: env not ready");
        return;
    };
    let Some(raw_pg) = env_or_dotenv("STUDIOFS_TEST_PG_DSN", None) else {
        eprintln!("skipping: STUDIOFS_TEST_PG_DSN is not set");
        return;
    };

    let target = random_name("dump-load-target");
    let extra1 = random_name("dump-load-extra1");
    let extra2 = random_name("dump-load-extra2");
    let home = tempfile::tempdir().expect("home");
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
    delete_volume_by_name(&pg_dsn, &target).await;

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
    assert_ok(&load, "load target");

    let verify_home = tempfile::tempdir().expect("verify home");
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
