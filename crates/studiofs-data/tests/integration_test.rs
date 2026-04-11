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
use studiofs_data::store::{DataStore, DataStoreConfig};

/// Atomic counter to generate unique slice IDs per test run.
static SLICE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique slice ID using PID + counter to avoid cross-run collisions.
fn unique_slice_id() -> u64 {
    let pid = std::process::id() as u64;
    let seq = SLICE_COUNTER.fetch_add(1, Ordering::Relaxed);
    pid * 10_000 + seq + 100_000
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

/// Default S3-compatible test credentials and endpoint.
fn test_config() -> DataStoreConfig {
    load_test_env();
    DataStoreConfig {
        endpoint: test_env_var("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:9000"),
        bucket: test_env_var("STUDIOFS_S3_BUCKET", "studiofs-test"),
        access_key: test_env_var("STUDIOFS_S3_ACCESS_KEY", "studiofs"),
        secret_key: test_env_var("STUDIOFS_S3_SECRET_KEY", "studiofs123"),
        region: "us-east-1".into(),
        volume_name: "integration-test".into(),
    }
}

fn make_store() -> DataStore {
    DataStore::new(&test_config()).expect("failed to create DataStore")
}

async fn ensure_bucket() {
    use aws_sdk_s3::Client as S3Client;
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
    use opendal::{Operator, services::S3};

    let cfg = test_config();
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
    let _ = client.create_bucket().bucket(&cfg.bucket).send().await;
    let _ = op.write(".bucket-marker", Bytes::from_static(b"ok")).await;
}

// ────────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn put_and_get_block() {
    ensure_bucket().await;
    let store = make_store();

    let payload = Bytes::from(vec![0xABu8; 1024]);
    let slice_id = unique_slice_id();
    let block_idx = 0;
    let payload_size =
        u32::try_from(payload.len()).expect("payload size must fit u32 for S3 block test payload");

    // Upload
    store
        .put_block(slice_id, block_idx, payload.clone())
        .await
        .unwrap();

    // Download
    let got = store
        .get_block(slice_id, block_idx, payload_size)
        .await
        .unwrap();
    assert_eq!(got, payload, "downloaded block must match uploaded data");

    // Cleanup
    store
        .delete_block(slice_id, block_idx, payload_size)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn get_block_range() {
    ensure_bucket().await;
    let store = make_store();

    // Create a 4 KiB block with ascending byte pattern
    let data: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();
    let payload = Bytes::from(data.clone());
    let slice_id = unique_slice_id();
    let block_idx = 0;
    let block_size =
        u32::try_from(payload.len()).expect("payload size must fit u32 for S3 block test payload");

    store.put_block(slice_id, block_idx, payload).await.unwrap();

    // Read range [1024..2048)
    let range = store
        .get_block_range(slice_id, block_idx, block_size, 1024, 1024)
        .await
        .unwrap();

    assert_eq!(range.len(), 1024);
    assert_eq!(&range[..], &data[1024..2048]);

    // Cleanup
    store
        .delete_block(slice_id, block_idx, block_size)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn block_exists_lifecycle() {
    ensure_bucket().await;
    let store = make_store();

    let slice_id = unique_slice_id();
    let block_idx = 0;
    let payload = Bytes::from(vec![1u8; 512]);
    let block_size =
        u32::try_from(payload.len()).expect("payload size must fit u32 for S3 block test payload");

    // Should not exist initially (unique slice ID guarantees this)
    assert!(
        !store
            .block_exists(slice_id, block_idx, block_size)
            .await
            .unwrap()
    );

    // Upload
    store.put_block(slice_id, block_idx, payload).await.unwrap();
    assert!(
        store
            .block_exists(slice_id, block_idx, block_size)
            .await
            .unwrap()
    );

    // Delete
    store
        .delete_block(slice_id, block_idx, block_size)
        .await
        .unwrap();
    assert!(
        !store
            .block_exists(slice_id, block_idx, block_size)
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn delete_block_idempotent() {
    ensure_bucket().await;
    let store = make_store();

    let slice_id = unique_slice_id();
    let block_idx = 0;
    let payload = Bytes::from(vec![9u8; 256]);
    let block_size =
        u32::try_from(payload.len()).expect("payload size must fit u32 for S3 block test payload");

    store.put_block(slice_id, block_idx, payload).await.unwrap();
    store
        .delete_block(slice_id, block_idx, block_size)
        .await
        .unwrap();

    // Second delete should not error (S3 DELETE is idempotent)
    store
        .delete_block(slice_id, block_idx, block_size)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn large_multiblock_write() {
    ensure_bucket().await;
    let store = make_store();

    let slice_id = unique_slice_id();

    // Upload 3 blocks of 1 MiB each with different patterns
    for idx in 0u32..3 {
        let data = Bytes::from(vec![(idx as u8) + 0xA0; 1024 * 1024]);
        store.put_block(slice_id, idx, data).await.unwrap();
    }

    // Read back each block and verify
    let block_size = 1024usize * 1024;
    let block_size_u32 =
        u32::try_from(block_size).expect("test block size must fit u32 for storage API");
    for idx in 0u32..3 {
        let block = store
            .get_block(slice_id, idx, block_size_u32)
            .await
            .unwrap();
        assert_eq!(block.len(), block_size);
        assert!(block.iter().all(|&b| b == (idx as u8) + 0xA0));
    }

    // Cleanup
    for idx in 0u32..3 {
        store
            .delete_block(slice_id, idx, block_size_u32)
            .await
            .unwrap();
    }
}
