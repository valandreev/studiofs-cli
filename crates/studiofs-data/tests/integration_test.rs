//! Integration tests for DataStore against a live MinIO instance.
//!
//! Prerequisites:
//!   minio server C:\minio --console-address ":9001"
//!
//! Run with:
//!   cargo test -p studiofs-data --test integration_test -- --ignored

use bytes::Bytes;
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

/// MinIO default credentials and endpoint.
fn test_config() -> DataStoreConfig {
    DataStoreConfig {
        endpoint: "http://127.0.0.1:9000".into(),
        bucket: "studiofs-test".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        region: "us-east-1".into(),
        volume_name: "integration-test".into(),
    }
}

fn make_store() -> DataStore {
    DataStore::new(&test_config()).expect("failed to create DataStore")
}

// ────────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn put_and_get_block() {
    let store = make_store();

    let payload = Bytes::from(vec![0xABu8; 1024]);
    let slice_id = unique_slice_id();
    let block_idx = 0;

    // Upload
    store
        .put_block(slice_id, block_idx, payload.clone())
        .await
        .unwrap();

    // Download
    let got = store
        .get_block(slice_id, block_idx, payload.len() as u32)
        .await
        .unwrap();
    assert_eq!(got, payload, "downloaded block must match uploaded data");

    // Cleanup
    store
        .delete_block(slice_id, block_idx, payload.len() as u32)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn get_block_range() {
    let store = make_store();

    // Create a 4 KiB block with ascending byte pattern
    let data: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();
    let payload = Bytes::from(data.clone());
    let slice_id = unique_slice_id();
    let block_idx = 0;
    let block_size = payload.len() as u32;

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
    let store = make_store();

    let slice_id = unique_slice_id();
    let block_idx = 0;
    let payload = Bytes::from(vec![1u8; 512]);
    let block_size = payload.len() as u32;

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
    let store = make_store();

    let slice_id = unique_slice_id();
    let block_idx = 0;
    let payload = Bytes::from(vec![9u8; 256]);
    let block_size = payload.len() as u32;

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
    let store = make_store();

    let slice_id = unique_slice_id();

    // Upload 3 blocks of 1 MiB each with different patterns
    for idx in 0u32..3 {
        let data = Bytes::from(vec![(idx as u8) + 0xA0; 1024 * 1024]);
        store.put_block(slice_id, idx, data).await.unwrap();
    }

    // Read back each block and verify
    for idx in 0u32..3 {
        let block = store.get_block(slice_id, idx, 1024 * 1024).await.unwrap();
        assert_eq!(block.len(), 1024 * 1024);
        assert!(block.iter().all(|&b| b == (idx as u8) + 0xA0));
    }

    // Cleanup
    for idx in 0u32..3 {
        store
            .delete_block(slice_id, idx, 1024 * 1024)
            .await
            .unwrap();
    }
}
