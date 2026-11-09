//! Integration tests for DataStore against a live S3-compatible instance.
//!
//! Prerequisites:
//!   rustfs server
//!
//! Run with:
//!   cargo test -p studiofs-data --test integration_test -- --ignored

use bytes::Bytes;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use studiofs_data::store::{DataStore, DataStoreConfig, DataStoreCryptoConfig};

/// Atomic counter to generate unique object IDs per test run.
static OBJECT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique object ID using PID + counter to avoid cross-run collisions.
fn unique_object_id() -> String {
    let pid = std::process::id() as u64;
    let seq = OBJECT_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:016x}", pid * 10_000 + seq + 100_000)
}

fn workspace_env_path() -> std::path::PathBuf {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .map(std::path::Path::to_path_buf)
        .unwrap_or(manifest_dir)
        .join(".env")
}

fn load_test_env() {
    static ONCE: OnceLock<()> = OnceLock::new();
    let _ = ONCE.get_or_init(|| {
        let env_path = workspace_env_path();
        if env_path.is_file() {
            let _ = dotenvy::from_path(&env_path);
        } else {
            let _ = dotenvy::dotenv();
        }
    });
}

fn test_env_var(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.into())
}

fn optional_test_env_var(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// Default S3-compatible test credentials and endpoint.
fn test_config() -> DataStoreConfig {
    load_test_env();
    DataStoreConfig {
        endpoint: test_env_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:9000"),
        bucket: test_env_var("STUDIOFS_S3_BUCKET", "studiofs-test"),
        access_key: test_env_var("STUDIOFS_S3_ACCESS_KEY", "studiofs"),
        secret_key: test_env_var("STUDIOFS_S3_SECRET_KEY", "studiofs123"),
        region: test_env_var("STUDIOFS_S3_REGION", "us-east-1"),
        volume_name: "integration-test".into(),
        data_prefix: "integration-test/objects/".into(),
        volume_uuid: "d34f3c2a-c3a4-4b01-b626-fb46de61118a".into(),
        crypto: DataStoreCryptoConfig::from(studiofs_meta::EncryptionConfig::new(
            [0x42; studiofs_meta::ENCRYPTION_KEY_LENGTH],
        )),
    }
}

fn make_store() -> DataStore {
    DataStore::new(&test_config()).expect("failed to create DataStore")
}

async fn ensure_bucket() {
    let cfg = test_config();
    ensure_bucket_for(&cfg).await;
}

#[test]
fn bucket_creation_configuration_uses_non_default_region() {
    assert!(bucket_creation_configuration("us-east-1").is_none());

    let configuration = bucket_creation_configuration("eu-central-1")
        .expect("non-default S3 regions require a location constraint");
    assert_eq!(
        configuration
            .location_constraint()
            .map(|location| location.as_str()),
        Some("eu-central-1")
    );
}

fn bucket_creation_configuration(
    region: &str,
) -> Option<aws_sdk_s3::types::CreateBucketConfiguration> {
    (region != "us-east-1").then(|| {
        aws_sdk_s3::types::CreateBucketConfiguration::builder()
            .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::from(region))
            .build()
    })
}

async fn ensure_bucket_for(cfg: &DataStoreConfig) {
    use aws_sdk_s3::Client as S3Client;
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
    use opendal::{Operator, services::S3};

    let builder = S3::default()
        .endpoint(&cfg.endpoint)
        .bucket(&cfg.bucket)
        .access_key_id(&cfg.access_key)
        .secret_access_key(&cfg.secret_key)
        .region(&cfg.region);
    let op = Operator::new(builder).unwrap().finish();
    if op
        .write(".bucket-marker", Bytes::from_static(b"ok"))
        .await
        .is_ok()
    {
        return;
    }

    let conf = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(Credentials::new(
            &cfg.access_key,
            &cfg.secret_key,
            None,
            None,
            "studiofs-test",
        ))
        .region(Region::new(cfg.region.clone()))
        .http_client(SmithyHttpClientBuilder::new().build_http())
        .endpoint_url(&cfg.endpoint)
        .force_path_style(true)
        .build();
    let client = S3Client::from_conf(conf);
    let mut create_bucket = client.create_bucket().bucket(&cfg.bucket);
    if let Some(configuration) = bucket_creation_configuration(&cfg.region) {
        create_bucket = create_bucket.create_bucket_configuration(configuration);
    }
    let create_status = match create_bucket.send().await {
        Ok(_) => "succeeded".to_owned(),
        Err(error) => error
            .as_service_error()
            .and_then(|service_error| service_error.code())
            .map_or_else(
                || "failed without an S3 service error code".to_owned(),
                |code| format!("failed with {code}"),
            ),
    };
    if let Err(marker_error) = op.write(".bucket-marker", Bytes::from_static(b"ok")).await {
        panic!(
            "create or verify configured S3 test bucket failed (CreateBucket {create_status}; marker write failed: {marker_error})"
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live S3 backend (set STUDIOFS_S3_* or .env)"]
async fn put_and_get_object() {
    ensure_bucket().await;
    let store = make_store();

    let payload = Bytes::from(vec![0xABu8; 1024]);
    let object_id = unique_object_id();

    store.put_object(&object_id, payload.clone()).await.unwrap();

    let got = store.get_object(&object_id).await.unwrap().to_bytes();
    assert_eq!(got, payload, "downloaded object must match uploaded data");

    store.delete_object(&object_id).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live S3 backend (set STUDIOFS_S3_* or .env)"]
async fn put_and_get_by_key() {
    ensure_bucket().await;
    let store = make_store();

    let data: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();
    let payload = Bytes::from(data.clone());
    let key = format!("integration-test/objects/raw/{}", unique_object_id());

    store.put_by_key(&key, payload).await.unwrap();
    let got = store.get_by_key(&key).await.unwrap().to_bytes();

    assert_eq!(got.len(), 4096);
    assert_eq!(&got[..], &data[..]);

    store.delete_by_key(&key).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live S3 backend (set STUDIOFS_S3_* or .env)"]
async fn object_exists_lifecycle() {
    ensure_bucket().await;
    let store = make_store();

    let object_id = unique_object_id();
    let payload = Bytes::from(vec![1u8; 512]);
    let key = store.object_key(&object_id);

    assert!(!store.exists(&key).await.unwrap());

    store.put_object(&object_id, payload).await.unwrap();
    assert!(store.exists(&key).await.unwrap());

    store.delete_object(&object_id).await.unwrap();
    assert!(!store.exists(&key).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live S3 backend (set STUDIOFS_S3_* or .env)"]
async fn delete_object_idempotent() {
    ensure_bucket().await;
    let store = make_store();

    let object_id = unique_object_id();
    let payload = Bytes::from(vec![9u8; 256]);

    store.put_object(&object_id, payload).await.unwrap();
    store.delete_object(&object_id).await.unwrap();

    // Second delete should not error (S3 DELETE is idempotent)
    store.delete_object(&object_id).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live S3 backend (set STUDIOFS_S3_* or .env)"]
async fn large_multi_object_write() {
    ensure_bucket().await;
    let store = make_store();

    let mut object_ids = Vec::new();
    for idx in 0u32..3 {
        let object_id = unique_object_id();
        let data = Bytes::from(vec![(idx as u8) + 0xA0; 1024 * 1024]);
        store.put_object(&object_id, data).await.unwrap();
        object_ids.push(object_id);
    }

    for (idx, object_id) in object_ids.iter().enumerate() {
        let payload = store.get_object(object_id).await.unwrap().to_bytes();
        assert_eq!(payload.len(), 1024usize * 1024);
        assert!(payload.iter().all(|&b| b == (idx as u8) + 0xA0));
    }

    for object_id in object_ids {
        store.delete_object(&object_id).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live HTTPS S3-compatible backend (set STUDIOFS_S3_HTTPS_ENDPOINT or .env)"]
async fn https_endpoint_fixture_put_get_when_configured() {
    load_test_env();
    let Some(endpoint) = optional_test_env_var("STUDIOFS_S3_HTTPS_ENDPOINT") else {
        eprintln!("skipping HTTPS S3 fixture: STUDIOFS_S3_HTTPS_ENDPOINT is not set");
        return;
    };

    let mut cfg = test_config();
    cfg.endpoint = endpoint;
    ensure_bucket_for(&cfg).await;
    let store = DataStore::new(&cfg).expect("HTTPS S3 endpoint should build");

    let key = format!("integration-test/objects/https/{}", unique_object_id());
    let payload = Bytes::from_static(b"studiofs https transport fixture");

    let put_res = store.put_by_key(&key, payload.clone()).await;
    let get_res = if put_res.is_ok() {
        Some(store.get_by_key(&key).await)
    } else {
        None
    };
    let delete_res = store.delete_by_key(&key).await;

    put_res.expect("failed to upload object");
    let got = get_res
        .expect("download skipped because upload failed")
        .expect("failed to download object")
        .to_bytes();
    delete_res.expect("failed to delete object");
    assert_eq!(got, payload, "downloaded object must match uploaded data");
}

/// Live check of the janitor's listing assumptions: the provider returns a recursive
/// listing in S3's UTF-8 byte order across pages, with `LastModified` on every entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires live S3 backend (set STUDIOFS_S3_* or .env)"]
async fn janitor_listing_is_byte_ordered_with_last_modified_across_pages() {
    use futures::StreamExt as _;
    use studiofs_data::janitor::JanitorObjectStore;
    use studiofs_data::object::{format_object_fragment_keys, format_object_key};

    let cfg = test_config();
    ensure_bucket_for(&cfg).await;
    let op = opendal::Operator::new(
        opendal::services::S3::default()
            .endpoint(&cfg.endpoint)
            .bucket(&cfg.bucket)
            .access_key_id(&cfg.access_key)
            .secret_access_key(&cfg.secret_key)
            .region(&cfg.region),
    )
    .unwrap()
    // Providers such as B2 shed a few of 1,200 parallel writes with temporary 503s;
    // like production stores, retry only errors OpenDAL marks temporary.
    .layer(
        opendal::layers::RetryLayer::new()
            .with_max_times(4)
            .with_jitter(),
    )
    .finish();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!("janitor-order-probe/{}-{nanos}/", std::process::id());

    // Keys that break naive or locale-aware sorting, plus enough canonical object
    // keys (legacy + fragment) to span more than one 1,000-key listing page.
    let mut keys: Vec<String> = [
        "a",
        "a b",
        "a!",
        "a-b",
        "a.b",
        "a/b",
        "a/b/c",
        "a0",
        "A",
        "Z",
        "_",
        "~",
        "a\u{fc}",
        "a\u{4e2d}",
        "bar baz/foo",
        "bar/baz",
    ]
    .iter()
    .map(|suffix| format!("{prefix}{suffix}"))
    .collect();
    let objects = format!("{prefix}objects/");
    for id in (1..=600u64).map(|n| n * 0x1_0001) {
        keys.push(format_object_key(&objects, id));
        keys.extend(format_object_fragment_keys(&objects, id));
    }

    let writes = futures::stream::iter(keys.iter().cloned())
        .map(|key| {
            let op = op.clone();
            async move {
                let result = op.write(&key, bytes::Bytes::from_static(b"x")).await;
                result.map(|_| ()).map_err(|error| (key, error))
            }
        })
        .buffer_unordered(32)
        .collect::<Vec<_>>()
        .await;

    let mut listed = Vec::new();
    let mut untimed = 0usize;
    let list_result: opendal::Result<()> = async {
        let mut lister = JanitorObjectStore::new(op.clone()).list(&prefix).await?;
        while let Some(entry) = lister.next().await? {
            if entry.is_file {
                untimed += usize::from(entry.last_modified.is_none());
                listed.push(entry.key);
            }
        }
        Ok(())
    }
    .await;

    // Clean up before asserting so a failure leaves no probe objects behind.
    let _ = futures::stream::iter(keys.iter().cloned())
        .map(|key| {
            let op = op.clone();
            async move { op.delete(&key).await }
        })
        .buffer_unordered(32)
        .collect::<Vec<_>>()
        .await;

    let failed: Vec<_> = writes.iter().filter_map(|w| w.as_ref().err()).collect();
    for (key, error) in failed.iter().take(5) {
        println!(
            "probe write failed key_suffix={:?} kind={:?} status_hint={}",
            key.trim_start_matches(&prefix),
            error.kind(),
            error
                .to_string()
                .lines()
                .next()
                .unwrap_or_default()
                .chars()
                .take(160)
                .collect::<String>()
        );
    }
    assert!(
        failed.is_empty(),
        "{} of {} probe writes failed",
        failed.len(),
        keys.len()
    );
    list_result.expect("probe listing failed");
    let mut expected = keys.clone();
    expected.sort();
    let first_disorder = listed.windows(2).position(|pair| pair[0] > pair[1]);
    println!(
        "janitor order probe: written={} listed={} untimed={} first_disorder={first_disorder:?}",
        keys.len(),
        listed.len(),
        untimed
    );
    assert_eq!(first_disorder, None, "listing is not in byte order");
    assert_eq!(listed, expected, "listing differs from the written key set");
    assert_eq!(untimed, 0, "listing entries without LastModified");
}
