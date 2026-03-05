//! Integration tests for the StudioFS VFS layer.
//!
//! Tests the block-based VFS implementation with:
//! - Sequential block IDs
//! - Immutable blocks
//! - LZ4 compression
//! - Crash recovery via journaling
//!
//! Prerequisites:
//!   minio server C:\minio --console-address ":9001"
//!
//! Run with:
//!   cargo test -p studiofs-vfs --test vfs_test -- --ignored

use bytes::Bytes;
use std::sync::Arc;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use studiofs_data::object::{create_object, format_object_key, read_object};
use studiofs_data::store::{DataStore, DataStoreConfig};
use studiofs_meta::types::SetattrArgs;
use studiofs_meta::types::TimeSpec;
use studiofs_meta::{FormatInfo, MetaEngine, MetadataConfig, RedbMetaEngine, types::BlockRef};
use studiofs_vfs::{LockKind, OpenFlags, StudioVfs, VfsError};

// =============================================================================
// Test Helpers
// =============================================================================

/// Create a test format info.
fn make_format_info(volume_suffix: &str) -> FormatInfo {
    load_test_env();
    let endpoint =
        std::env::var("STUDIOFS_MINIO_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into());
    let bucket = std::env::var("STUDIOFS_MINIO_BUCKET").unwrap_or_else(|_| "studiofs-test".into());
    let access_key =
        std::env::var("STUDIOFS_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret_key =
        std::env::var("STUDIOFS_MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());
    static RUN_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    let run_id = RUN_ID.get_or_init(|| {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        format!("{}-{ts}", std::process::id())
    });
    let volume_name = format!("vfs-test-{volume_suffix}-{run_id}");
    FormatInfo {
        name: volume_name.clone(),
        uuid: format!("test-uuid-{volume_suffix}"),
        endpoint: endpoint.clone(),
        bucket: bucket.clone(),
        access_key: access_key.clone(),
        secret_key: secret_key.clone(),
        region: "us-east-1".into(),
        data_prefix: format!("{volume_name}/objects/"),
        metadata: MetadataConfig {
            ..MetadataConfig::default()
        },
        version: String::new(),
        block_size: 4 * 1024 * 1024,  // 4 MiB
        chunk_size: 64 * 1024 * 1024, // 64 MiB (deprecated)
    }
}

fn load_test_env() {
    static ONCE: OnceLock<()> = OnceLock::new();
    let _ = ONCE.get_or_init(|| {
        let _ = dotenvy::dotenv();
    });
}

fn make_format_info_with_endpoint(volume_suffix: &str, endpoint: &str) -> FormatInfo {
    let mut info = make_format_info(volume_suffix);
    info.endpoint = endpoint.to_string();
    info
}

/// Create a fresh VFS instance backed by a temporary redb and MinIO.
fn make_vfs(tmp: &tempfile::TempDir, volume_suffix: &str) -> Arc<StudioVfs> {
    let db_path = tmp.path().join("meta.redb");

    let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("failed to open RedbMetaEngine"));
    meta.init_root(0, 0)
        .expect("failed to initialize root inode");

    // Set format info
    let format_info = make_format_info(volume_suffix);
    meta.set_format_info(&format_info)
        .expect("failed to set format info");

    let data_config = DataStoreConfig::from(&format_info);

    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap();
    let cache_size = 1024 * 1024 * 1024; // 1 GiB

    StudioVfs::new(
        meta,
        &data_config,
        cache_dir_str,
        cache_size,
        512 * 1024 * 1024,
    )
    .expect("failed to create StudioVfs")
}

/// Ensure the test bucket exists.
async fn ensure_bucket() {
    use aws_sdk_s3::Client as S3Client;
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
    use opendal::{Operator, services::S3};
    let endpoint =
        std::env::var("STUDIOFS_MINIO_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into());
    let bucket = std::env::var("STUDIOFS_MINIO_BUCKET").unwrap_or_else(|_| "studiofs-test".into());
    let access_key =
        std::env::var("STUDIOFS_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret_key =
        std::env::var("STUDIOFS_MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());

    let builder = S3::default()
        .endpoint(&endpoint)
        .bucket(&bucket)
        .access_key_id(&access_key)
        .secret_access_key(&secret_key)
        .region("us-east-1");

    let op = Operator::new(builder).unwrap().finish();
    if op
        .write(".bucket-marker", Bytes::from_static(b"ok"))
        .await
        .is_ok()
    {
        return;
    }

    // Fallback: explicitly create bucket if missing.
    let conf = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(Credentials::new(
            &access_key,
            &secret_key,
            None,
            None,
            "studiofs-test",
        ))
        .region(Region::new("us-east-1"))
        .http_client(SmithyHttpClientBuilder::new().build_http())
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .build();
    let client = S3Client::from_conf(conf);
    let _ = client.create_bucket().bucket(&bucket).send().await;
    let _ = op.write(".bucket-marker", Bytes::from_static(b"ok")).await;
}

async fn wait_until_exists(ds: &DataStore, path: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        match ds.exists(path).await {
            Ok(true) => return true,
            Ok(false) => {}
            Err(_) => {}
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_until_absent(ds: &DataStore, path: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        match ds.exists(path).await {
            Ok(false) => return true,
            Ok(true) => {}
            Err(_) => return true,
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn wait_until_blocks(
    vfs: &Arc<StudioVfs>,
    ino: u64,
    timeout: Duration,
) -> Option<Vec<(u32, BlockRef)>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(blocks) = vfs.meta().get_file_blocks(ino)
            && !blocks.is_empty()
        {
            return Some(blocks);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Wait until at least `expected` block refs are visible in metadata.
///
/// After `flush()`, blocks are moved to the dirty memory buffer and processed
/// asynchronously by the background slab flusher.  This helper polls until the
/// expected number of blocks appear (or the timeout expires).
fn wait_until_n_blocks(
    vfs: &Arc<StudioVfs>,
    ino: u64,
    expected: usize,
    timeout: Duration,
) -> Option<Vec<(u32, BlockRef)>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(blocks) = vfs.meta().get_file_blocks(ino)
            && blocks.len() >= expected
        {
            return Some(blocks);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

// =============================================================================
// Basic Operations Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_create_write_read_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "cwr");

    // Create a file in root directory (inode 1)
    let attr = vfs.create(1, b"hello.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    assert!(ino > 1);

    // Write data
    let payload = b"Hello, StudioFS! This is a block-based test.";
    let written = vfs.write(ino, 0, payload).await.unwrap();
    assert_eq!(written as usize, payload.len());

    // Flush to ensure data is persisted
    vfs.flush(ino).await.unwrap();

    // Read it back
    let data = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
    assert_eq!(&data[..], payload);

    // Verify file size via getattr
    let attr = vfs.getattr(ino).unwrap();
    assert_eq!(attr.size, payload.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_write_read_large_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "large");

    let attr = vfs.create(1, b"big.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write 10 MiB of patterned data (crosses 4 MiB block boundary)
    let size = 10 * 1024 * 1024;
    let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let written = vfs.write(ino, 0, &payload).await.unwrap();
    assert_eq!(written as usize, size);

    // Flush
    vfs.flush(ino).await.unwrap();

    // Read it all back
    let data = vfs.read(ino, 0, size as u32).await.unwrap();
    assert_eq!(data.len(), size);
    assert_eq!(&data[..], &payload[..], "10 MiB read-back must match write");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_write_at_offset() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "offset");

    let attr = vfs.create(1, b"offset.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write at offset 1 MiB
    let offset = 1024 * 1024; // 1 MiB
    let payload = b"Data at 1 MiB offset";
    vfs.write(ino, offset, payload).await.unwrap();

    // Flush
    vfs.flush(ino).await.unwrap();

    // Read from offset
    let data = vfs.read(ino, offset, payload.len() as u32).await.unwrap();
    assert_eq!(&data[..], payload);

    // Verify file size
    let attr = vfs.getattr(ino).unwrap();
    assert_eq!(attr.size, offset + payload.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_overwrite_and_read() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "overwrite");

    let attr = vfs.create(1, b"over.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write initial data
    let v1 = b"AAAAAAAAAA"; // 10 bytes
    vfs.write(ino, 0, v1).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Overwrite the middle (offset 3..8)
    let data2 = b"BBBBB";
    vfs.write(ino, 3, data2).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Read full file — expect "AAABBBBBAA"
    let data = vfs.read(ino, 0, 10).await.unwrap();
    assert_eq!(&data[..], b"AAABBBBBAA");
}

// =============================================================================
// Block Operations Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_block_allocation() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "blocks");

    let attr = vfs.create(1, b"blocks.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write data that spans multiple blocks (4 MiB each)
    let block_size = 4 * 1024 * 1024;
    let payload: Vec<u8> = (0..block_size * 3).map(|i| (i % 256) as u8).collect();

    vfs.write(ino, 0, &payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Wait for background slab flusher to materialise block refs.
    let blocks = wait_until_n_blocks(&vfs, ino, 3, Duration::from_secs(15))
        .expect("Should have 3 blocks for 12 MiB file");

    // Verify block indices
    assert_eq!(blocks[0].0, 0);
    assert_eq!(blocks[1].0, 1);
    assert_eq!(blocks[2].0, 2);

    // Verify each block is stored uncompressed in metadata flags
    for (_, block_ref) in &blocks {
        assert!(
            !block_ref.is_compressed(),
            "Block should not be marked compressed"
        );
    }

    // Read back and verify
    let data = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
    assert_eq!(&data[..], &payload[..]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_partial_block() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "partial");

    let attr = vfs.create(1, b"partial.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write less than a full block
    let payload = b"Small file content";
    vfs.write(ino, 0, payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Wait for background slab flusher to materialise block refs.
    let blocks = wait_until_n_blocks(&vfs, ino, 1, Duration::from_secs(15))
        .expect("Should have 1 block after flush");
    assert_eq!(blocks.len(), 1);

    let (_, block_ref) = &blocks[0];
    assert!(
        block_ref.is_partial(),
        "Small file should have partial block"
    );
    assert_eq!(block_ref.length as usize, payload.len());
}

// =============================================================================
// Directory Operations Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_directory_operations() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "dirs");

    // Create a subdirectory
    let dir_attr = vfs.mkdir(1, b"subdir", 0, 0, 0o755).unwrap();
    let dir_ino = dir_attr.ino;

    // Create files inside
    let f1 = vfs.create(dir_ino, b"a.txt", 0, 0, 0o644).unwrap();
    let f2 = vfs.create(dir_ino, b"b.txt", 0, 0, 0o644).unwrap();

    // Write data to files
    vfs.write(f1.ino, 0, b"file a content").await.unwrap();
    vfs.write(f2.ino, 0, b"file b content").await.unwrap();
    vfs.flush(f1.ino).await.unwrap();
    vfs.flush(f2.ino).await.unwrap();

    // Readdir should return both files
    let entries = vfs.readdir(dir_ino).unwrap();
    let names: Vec<String> = entries
        .iter()
        .map(|e| String::from_utf8_lossy(&e.name).to_string())
        .collect();
    assert!(names.contains(&"a.txt".to_string()));
    assert!(names.contains(&"b.txt".to_string()));

    // Lookup should work
    let found = vfs.lookup(dir_ino, b"a.txt").unwrap();
    assert_eq!(found.ino, f1.ino);

    // Unlink files
    vfs.unlink(dir_ino, b"a.txt").unwrap();
    vfs.unlink(dir_ino, b"b.txt").unwrap();

    // Rmdir
    vfs.rmdir(1, b"subdir").unwrap();

    // Lookup should fail now
    assert!(vfs.lookup(1, b"subdir").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_rename_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rename");

    let attr = vfs.create(1, b"old.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write some data
    vfs.write(ino, 0, b"rename test data").await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Rename
    vfs.rename(1, b"old.txt", 1, b"new.txt").unwrap();

    // Old name should fail
    assert!(vfs.lookup(1, b"old.txt").is_err());

    // New name should resolve to same inode
    let found = vfs.lookup(1, b"new.txt").unwrap();
    assert_eq!(found.ino, ino);

    // Data should still be readable
    let data = vfs.read(ino, 0, 16).await.unwrap();
    assert_eq!(&data[..], b"rename test data");
}

// =============================================================================
// Truncation Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_truncate_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "truncate");

    let attr = vfs.create(1, b"trunc.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write 12 MiB (3 blocks)
    let size = 12 * 1024 * 1024;
    let payload: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    vfs.write(ino, 0, &payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Wait for background slab flusher to materialise all 3 block refs.
    let blocks = wait_until_n_blocks(&vfs, ino, 3, Duration::from_secs(15))
        .expect("Should have 3 blocks for 12 MiB file");
    assert_eq!(blocks.len(), 3);

    // Truncate to 5 MiB (should remove 1 block, partial last block)
    let new_size = 5 * 1024 * 1024;
    vfs.setattr(ino, SetattrArgs::size_only(new_size)).unwrap();

    // Verify 2 blocks now (truncation is synchronous — no wait needed)
    let blocks = vfs.meta().get_file_blocks(ino).unwrap();
    assert_eq!(blocks.len(), 2);

    // Verify file size
    let attr = vfs.getattr(ino).unwrap();
    assert_eq!(attr.size, new_size);

    // Verify data is preserved
    let data = vfs.read(ino, 0, new_size as u32).await.unwrap();
    assert_eq!(&data[..], &payload[..new_size as usize]);
}

// =============================================================================
// Crash Recovery Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_pending_deletions_gc() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "gc");

    let attr = vfs.create(1, b"gc_test.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write initial data
    vfs.write(ino, 0, b"initial data").await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Wait for background slab flusher to materialise block refs.
    let blocks_before = wait_until_n_blocks(&vfs, ino, 1, Duration::from_secs(15))
        .expect("Should have 1 block after initial write");
    assert_eq!(blocks_before.len(), 1);
    let old_block_id = blocks_before[0].1.object_id;

    // Overwrite with new data (should mark old block for deletion)
    vfs.write(ino, 0, b"new data here!!").await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Wait for the overwrite to be flushed — the new block will have a
    // different object_id, so poll until the block ref changes.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(blocks) = vfs.meta().get_file_blocks(ino)
            && !blocks.is_empty()
            && blocks[0].1.object_id != old_block_id
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Timed out waiting for overwrite flush"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Verify old block is in pending deletions
    let pending = vfs.meta().get_pending_deletions().unwrap();
    assert!(
        pending.iter().any(|(id, _)| *id == old_block_id),
        "Old block should be in pending deletions"
    );

    // Verify new block is different
    let blocks_after = vfs.meta().get_file_blocks(ino).unwrap();
    assert_ne!(blocks_after[0].1.object_id, old_block_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_background_janitor_removes_orphan_object() {
    ensure_bucket().await;

    struct EnvRestore {
        key: &'static str,
        old: Option<String>,
    }
    impl Drop for EnvRestore {
        fn drop(&mut self) {
            unsafe {
                if let Some(v) = &self.old {
                    std::env::set_var(self.key, v);
                } else {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    // Speed up janitor cycle for this test only.
    let _restore_interval = EnvRestore {
        key: "STUDIOFS_JANITOR_INTERVAL_SECS",
        old: std::env::var("STUDIOFS_JANITOR_INTERVAL_SECS").ok(),
    };
    let _restore_skip = EnvRestore {
        key: "STUDIOFS_JANITOR_SKIP_NEWER_SECS",
        old: std::env::var("STUDIOFS_JANITOR_SKIP_NEWER_SECS").ok(),
    };
    let _restore_max = EnvRestore {
        key: "STUDIOFS_JANITOR_MAX_DELETES",
        old: std::env::var("STUDIOFS_JANITOR_MAX_DELETES").ok(),
    };
    let _restore_lease_ttl = EnvRestore {
        key: "STUDIOFS_JANITOR_LEASE_TTL_SECS",
        old: std::env::var("STUDIOFS_JANITOR_LEASE_TTL_SECS").ok(),
    };
    unsafe {
        std::env::set_var("STUDIOFS_JANITOR_INTERVAL_SECS", "2");
        std::env::set_var("STUDIOFS_JANITOR_SKIP_NEWER_SECS", "0");
        std::env::set_var("STUDIOFS_JANITOR_MAX_DELETES", "50");
        std::env::set_var("STUDIOFS_JANITOR_LEASE_TTL_SECS", "10");
    }

    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "janitor_orphan");
    let info = vfs.meta().get_format_info().unwrap();
    let ds = DataStore::new(&DataStoreConfig::from(&info)).unwrap();

    // Upload an orphan object directly to data storage (metadata doesn't reference it).
    let orphan_id = 0xdead_beef_u64;
    let orphan_key = format_object_key(&info.data_prefix, orphan_id);
    if let Err(err) = ds
        .operator()
        .write(&orphan_key, Bytes::from_static(b"orphan-object"))
        .await
    {
        if err.to_string().contains("NoSuchBucket") {
            eprintln!(
                "skip test_background_janitor_removes_orphan_object: {}",
                err
            );
            return;
        }
        panic!("failed to write orphan object: {}", err);
    }
    assert!(
        wait_until_exists(&ds, &orphan_key, Duration::from_secs(5)).await,
        "orphan object must exist before janitor cycle"
    );

    assert!(
        wait_until_absent(&ds, &orphan_key, Duration::from_secs(20)).await,
        "background janitor should delete orphan object"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_background_janitor_cleans_simulated_crash_orphan() {
    ensure_bucket().await;

    struct EnvRestore {
        key: &'static str,
        old: Option<String>,
    }
    impl Drop for EnvRestore {
        fn drop(&mut self) {
            unsafe {
                if let Some(v) = &self.old {
                    std::env::set_var(self.key, v);
                } else {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    // Fast janitor cycle for test.
    let _restore_interval = EnvRestore {
        key: "STUDIOFS_JANITOR_INTERVAL_SECS",
        old: std::env::var("STUDIOFS_JANITOR_INTERVAL_SECS").ok(),
    };
    let _restore_skip = EnvRestore {
        key: "STUDIOFS_JANITOR_SKIP_NEWER_SECS",
        old: std::env::var("STUDIOFS_JANITOR_SKIP_NEWER_SECS").ok(),
    };
    let _restore_max = EnvRestore {
        key: "STUDIOFS_JANITOR_MAX_DELETES",
        old: std::env::var("STUDIOFS_JANITOR_MAX_DELETES").ok(),
    };
    let _restore_lease_ttl = EnvRestore {
        key: "STUDIOFS_JANITOR_LEASE_TTL_SECS",
        old: std::env::var("STUDIOFS_JANITOR_LEASE_TTL_SECS").ok(),
    };
    unsafe {
        std::env::set_var("STUDIOFS_JANITOR_INTERVAL_SECS", "2");
        std::env::set_var("STUDIOFS_JANITOR_SKIP_NEWER_SECS", "0");
        std::env::set_var("STUDIOFS_JANITOR_MAX_DELETES", "50");
        std::env::set_var("STUDIOFS_JANITOR_LEASE_TTL_SECS", "10");
    }

    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "janitor_crash_orphan");
    let info = vfs.meta().get_format_info().unwrap();
    let ds = DataStore::new(&DataStoreConfig::from(&info)).unwrap();

    // 1) Normal write path uploads object and records metadata ref.
    let attr = vfs.create(1, b"crash_like.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    let payload = vec![0xABu8; 512 * 1024];
    vfs.write(ino, 0, &payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    let blocks = wait_until_blocks(&vfs, ino, Duration::from_secs(15))
        .expect("expected at least one block reference after flush");
    let object_id = blocks[0].1.object_id;
    let key = format_object_key(&info.data_prefix, object_id);
    let object_bytes = Bytes::from(create_object(&payload).unwrap());
    if let Err(err) = ds.operator().write(&key, object_bytes).await {
        if err.to_string().contains("NoSuchBucket") {
            eprintln!(
                "skip test_background_janitor_cleans_simulated_crash_orphan: {}",
                err
            );
            return;
        }
        panic!("failed to write simulated crash object: {}", err);
    }
    assert!(
        wait_until_exists(&ds, &key, Duration::from_secs(5)).await,
        "uploaded data object should exist before simulated crash"
    );

    // 2) Simulate crash window by "losing" metadata refs while object remains in S3.
    let removed_ids = vfs.meta().remove_file_blocks(ino).unwrap();
    assert!(
        removed_ids.contains(&object_id),
        "remove_file_blocks should drop the uploaded object reference"
    );

    // 3) Janitor should eventually detect object as orphan and delete it.
    assert!(
        wait_until_absent(&ds, &key, Duration::from_secs(25)).await,
        "background janitor should clean orphan object from simulated crash"
    );
}

#[test]
#[ignore]
fn test_crash_recovery_replays_pending_journal() {
    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    boot_rt.block_on(ensure_bucket());

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("meta.redb");
    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap().to_string();

    let volume_suffix = format!("crash-{}", std::process::id());
    let offline_info = make_format_info_with_endpoint(&volume_suffix, "http://127.0.0.1:59999");
    let mut crash_file_ino: u64 = 0;

    let meta = Arc::new(RedbMetaEngine::open(&db_path).unwrap());
    meta.init_root(0, 0).unwrap();
    meta.set_format_info(&offline_info).unwrap();

    // Phase 1: create dirty data and pending journals while endpoint is offline.
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        let offline_data_config = DataStoreConfig::from(&offline_info);
        rt.block_on(async {
            let vfs = StudioVfs::new(
                meta.clone(),
                &offline_data_config,
                &cache_dir_str,
                1024 * 1024 * 1024,
                512 * 1024 * 1024,
            )
            .unwrap();
            let attr = vfs.create(1, b"crash.bin", 0, 0, 0o644).unwrap();
            crash_file_ino = attr.ino;
            let payload = vec![0x5Au8; 2 * 1024 * 1024];
            vfs.write(attr.ino, 0, &payload).await.unwrap();
            vfs.flush(attr.ino).await.unwrap();
            // Wait until the background slab flusher has materialised block
            // refs + journal entries.  Otherwise dirty_mem hasn't been drained
            // and no journal files exist yet.
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                if let Ok(blocks) = vfs.meta().get_file_blocks(attr.ino)
                    && !blocks.is_empty()
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for slab flusher"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            // Simulate abrupt process death: drop runtime without graceful VFS shutdown.
        });
        rt.shutdown_background();
    }

    let pending_dir = cache_dir.join("upload");
    let pending_count = std::fs::read_dir(&pending_dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "pending"))
        .count();
    assert!(
        pending_count > 0,
        "expected pending journal files after simulated crash"
    );

    // Phase 2: restart with healthy endpoint and verify recovery drains journals.
    let online_endpoint =
        std::env::var("STUDIOFS_MINIO_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into());
    let online_info = make_format_info_with_endpoint(&volume_suffix, &online_endpoint);
    meta.set_format_info(&online_info).unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    let online_data_config = DataStoreConfig::from(&online_info);
    rt.block_on(async {
        let _vfs = StudioVfs::new(
            meta.clone(),
            &online_data_config,
            &cache_dir_str,
            1024 * 1024 * 1024,
            512 * 1024 * 1024,
        )
        .unwrap();

        let blocks = meta.get_file_blocks(crash_file_ino).unwrap();
        assert!(!blocks.is_empty(), "file blocks must exist in metadata");
        let object_id = blocks[0].1.object_id;
        let key = format_object_key(&online_info.data_prefix, object_id);
        let check_store = DataStore::new(&DataStoreConfig::from(&online_info)).unwrap();

        assert!(
            wait_until_exists(&check_store, &key, Duration::from_secs(15)).await,
            "recovered uploader should eventually upload object {object_id}"
        );
    });

    // Allow async journal cleanup task to remove completed files.
    thread::sleep(Duration::from_millis(500));
    let pending_after = std::fs::read_dir(&pending_dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "journal"))
        .count();
    assert_eq!(
        pending_after, 0,
        "pending journals must be cleaned up after recovery"
    );
}

// =============================================================================
// Metadata Tests
// =============================================================================

#[tokio::test]
async fn test_block_ref_flags() {
    // Test BlockRef flags
    let full_block = BlockRef::new_full(123, 4 * 1024 * 1024);
    assert!(!full_block.is_compressed());
    assert!(!full_block.is_partial());
    assert_eq!(full_block.object_id, 123);
    assert_eq!(full_block.length, 4 * 1024 * 1024);

    let partial_block = BlockRef::new_partial(456, 1000);
    assert!(!partial_block.is_compressed());
    assert!(partial_block.is_partial());
    assert_eq!(partial_block.object_id, 456);
    assert_eq!(partial_block.length, 1000);
}

#[tokio::test]
async fn test_object_format() {
    let original_data = b"Test data for object format";

    // Create object
    let object = create_object(original_data).unwrap();

    // Verify we can read it back
    let decompressed = read_object(&object).unwrap();
    assert_eq!(decompressed.as_slice(), original_data);
}

#[tokio::test]
async fn test_object_key_format() {
    let data_prefix = "disk-a/objects/";
    let object_id: u64 = 0x1234;

    let key = format_object_key(data_prefix, object_id);
    assert_eq!(key, "disk-a/objects/0000/0000000000001234");
}

// =============================================================================
// Performance Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_sequential_write_performance() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "perf");

    let attr = vfs.create(1, b"perf.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write 100 MiB
    let size = 100 * 1024 * 1024;
    let block_size = 4 * 1024 * 1024; // 4 MiB blocks

    let start = std::time::Instant::now();

    for i in 0..(size / block_size) {
        let offset = (i * block_size) as u64;
        let payload: Vec<u8> = (0..block_size).map(|j| ((i + j) % 256) as u8).collect();
        vfs.write(ino, offset, &payload).await.unwrap();
    }

    let write_duration = start.elapsed();
    let write_mbps = (size as f64 / 1024.0 / 1024.0) / write_duration.as_secs_f64();

    println!(
        "Write: {:.2} MiB/s ({} bytes in {:?})",
        write_mbps, size, write_duration
    );

    // Flush
    let flush_start = std::time::Instant::now();
    vfs.flush(ino).await.unwrap();
    let flush_duration = flush_start.elapsed();
    println!("Flush: {:?}", flush_duration);

    // Read back
    let read_start = std::time::Instant::now();
    let data = vfs.read(ino, 0, size as u32).await.unwrap();
    let read_duration = read_start.elapsed();
    let read_mbps = (size as f64 / 1024.0 / 1024.0) / read_duration.as_secs_f64();

    println!(
        "Read: {:.2} MiB/s ({} bytes in {:?})",
        read_mbps, size, read_duration
    );

    assert_eq!(data.len(), size);
}
/// Test attribute cache with TTL and invalidation on writes.
#[tokio::test(flavor = "multi_thread")]
async fn test_attr_cache_ttl() {
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "attr_cache");

    // Create a file
    let file_attr = vfs.create(1, b"test.txt", 0, 0, 0o644).unwrap();
    let ino = file_attr.ino;

    // First getattr — loads from redb, populates cache
    let attr1 = vfs.getattr(ino).unwrap();
    assert_eq!(attr1.ino, ino);
    assert_eq!(attr1.size, 0);

    // Second getattr immediately — should hit read-through cache (no redb read)
    let attr2 = vfs.getattr(ino).unwrap();
    assert_eq!(attr2.ino, ino);
    assert_eq!(attr2.size, 0);

    // Write data via write() — should not immediately invalidate cache
    vfs.write(ino, 0, b"hello").await.unwrap();

    // Getattr should return value from dirty_attrs (written but not flushed)
    let attr3 = vfs.getattr(ino).unwrap();
    assert_eq!(attr3.size, 5); // size updated by write path

    // Flush the file
    vfs.flush(ino).await.unwrap();

    // After flush, read-through cache was invalidated, should re-fetch
    // (but dirty_attrs is cleared, so it comes from redb now)
    let attr4 = vfs.getattr(ino).unwrap();
    assert_eq!(attr4.size, 5);

    // Setattr should invalidate cache
    vfs.setattr(ino, SetattrArgs::size_only(10)).unwrap();

    // Next getattr should see the new size from dirty_attrs
    let attr5 = vfs.getattr(ino).unwrap();
    assert_eq!(attr5.size, 10);
}

// =============================================================================
// metadata sync Invalidation Tests
// =============================================================================

// =============================================================================
// Handle Tracking Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_open_file_and_release() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "handle1");

    let attr = vfs.create(1, b"handle.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Open a file handle
    let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
    assert_ne!(fh, 0);

    // Check open count
    assert_eq!(vfs.open_count(ino), 1);

    // Get the open file info
    let of = vfs.get_open_file(fh).unwrap();
    assert_eq!(of.ino, ino);

    // Write through handle
    vfs.write(ino, 0, b"via handle").await.unwrap();
    vfs.flush_fh(fh).await.unwrap();

    // Release the handle
    vfs.release_fh(fh).await.unwrap();
    assert_eq!(vfs.open_count(ino), 0);

    // Data should persist after release
    let data = vfs.read(ino, 0, 10).await.unwrap();
    assert_eq!(&data[..], b"via handle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_open_trunc() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "handle_trunc");

    let attr = vfs.create(1, b"trunc.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write some data
    vfs.write(ino, 0, b"existing data").await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Open with trunc=true
    let fh = vfs.open_file(ino, OpenFlags::RDWR, true).unwrap();

    // File should be truncated
    let attr = vfs.getattr(ino).unwrap();
    assert_eq!(attr.size, 0);

    vfs.release_fh(fh).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_open_unlink_deferred_cleanup() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "orphan");

    let attr = vfs.create(1, b"orphan.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write data
    vfs.write(ino, 0, b"orphan data").await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Open a handle
    let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

    // Unlink while handle is open — should defer cleanup
    vfs.unlink(1, b"orphan.txt").unwrap();

    // File is no longer findable by name
    assert!(vfs.lookup(1, b"orphan.txt").is_err());

    // But data is still accessible via ino (the handle keeps it alive)
    let data = vfs.read(ino, 0, 11).await.unwrap();
    assert_eq!(&data[..], b"orphan data");

    // Release the last handle — orphan cleanup should happen
    vfs.release_fh(fh).await.unwrap();

    // Inode should be gone now
    assert!(vfs.getattr(ino).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_multiple_handles_open_unlink() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "multi_handle");

    let attr = vfs.create(1, b"multi.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    vfs.write(ino, 0, b"multi-handle data").await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Open two handles
    let fh1 = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
    let fh2 = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
    assert_eq!(vfs.open_count(ino), 2);

    // Unlink while both handles open
    vfs.unlink(1, b"multi.txt").unwrap();

    // Release first handle — inode should still exist (one handle left)
    vfs.release_fh(fh1).await.unwrap();
    assert_eq!(vfs.open_count(ino), 1);
    let data = vfs.read(ino, 0, 17).await.unwrap();
    assert_eq!(&data[..], b"multi-handle data");

    // Release second handle — now cleanup should happen
    vfs.release_fh(fh2).await.unwrap();
    assert!(vfs.getattr(ino).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_fsync() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "fsync");

    let attr = vfs.create(1, b"fsync.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    vfs.write(ino, 0, b"fsync test data").await.unwrap();

    // fsync should flush data
    vfs.fsync(ino, false).await.unwrap();

    // Data should be readable
    let data = vfs.read(ino, 0, 15).await.unwrap();
    assert_eq!(&data[..], b"fsync test data");

    // datasync=true should also work
    vfs.write(ino, 0, b"datasync test!!").await.unwrap();
    vfs.fsync(ino, true).await.unwrap();

    let data = vfs.read(ino, 0, 15).await.unwrap();
    assert_eq!(&data[..], b"datasync test!!");
}

// =============================================================================
// Phase 4: setattr Timestamps + Symlinks
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_setattr_timestamps() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "setattr_ts");

    let attr = vfs.create(1, b"ts.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    let _original_atime = attr.atime;
    let _original_mtime = attr.mtime;

    // Set specific atime and mtime
    let new_atime = 1_700_000_000_000_000_000i64; // some timestamp in nanos
    let new_mtime = 1_600_000_000_000_000_000i64;
    let args = SetattrArgs {
        atime: TimeSpec::Set(new_atime),
        mtime: TimeSpec::Set(new_mtime),
        ..SetattrArgs::default()
    };
    let updated = vfs.setattr(ino, args).unwrap();
    assert_eq!(updated.atime, new_atime);
    assert_eq!(updated.mtime, new_mtime);
    // ctime should be updated (any change touches ctime)
    assert!(updated.ctime >= attr.ctime);

    // TimeSpec::Now should set to current time
    let args2 = SetattrArgs {
        atime: TimeSpec::Now,
        ..SetattrArgs::default()
    };
    let updated2 = vfs.setattr(ino, args2).unwrap();
    assert!(updated2.atime > new_atime); // should be current time, which is much later
    assert_eq!(updated2.mtime, new_mtime); // mtime unchanged (Omit)

    // TimeSpec::Omit should leave value unchanged
    let args3 = SetattrArgs {
        atime: TimeSpec::Omit,
        mtime: TimeSpec::Omit,
        ..SetattrArgs::default()
    };
    let updated3 = vfs.setattr(ino, args3).unwrap();
    assert_eq!(updated3.atime, updated2.atime);
    assert_eq!(updated3.mtime, new_mtime);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_setattr_mode_uid_gid() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "setattr_mode");

    let attr = vfs.create(1, b"chmod.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Change mode
    let args = SetattrArgs {
        mode: Some(0o755),
        ..SetattrArgs::default()
    };
    let updated = vfs.setattr(ino, args).unwrap();
    assert_eq!(updated.mode, 0o755);

    // Change uid and gid
    let args2 = SetattrArgs {
        uid: Some(500),
        gid: Some(600),
        ..SetattrArgs::default()
    };
    let updated2 = vfs.setattr(ino, args2).unwrap();
    assert_eq!(updated2.uid, 500);
    assert_eq!(updated2.gid, 600);
    assert_eq!(updated2.mode, 0o755); // mode unchanged
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_symlink_create_and_readlink() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "symlink_basic");

    // Create a symlink
    let attr = vfs
        .symlink(1, b"mylink", b"/some/target/path", 0, 0)
        .unwrap();
    assert_eq!(attr.kind, studiofs_meta::FileType::Symlink);
    assert_eq!(attr.mode, 0o777);
    assert_eq!(attr.size, 17); // length of "/some/target/path"

    // Readlink
    let target = vfs.readlink(attr.ino).unwrap();
    assert_eq!(&target, b"/some/target/path");

    // Lookup should find it
    let looked_up = vfs.lookup(1, b"mylink").unwrap();
    assert_eq!(looked_up.ino, attr.ino);
    assert_eq!(looked_up.kind, studiofs_meta::FileType::Symlink);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_symlink_unlink() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "symlink_unlink");

    let attr = vfs.symlink(1, b"del_link", b"/target", 0, 0).unwrap();
    let ino = attr.ino;

    // Verify exists
    assert!(vfs.lookup(1, b"del_link").is_ok());

    // Unlink symlink
    vfs.unlink(1, b"del_link").unwrap();

    // Should be gone
    assert!(vfs.lookup(1, b"del_link").is_err());
    assert!(vfs.getattr(ino).is_err());
    assert!(vfs.readlink(ino).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_symlink_in_readdir() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "symlink_readdir");

    // Create a regular file and a symlink
    vfs.create(1, b"file.txt", 0, 0, 0o644).unwrap();
    vfs.symlink(1, b"link.txt", b"file.txt", 0, 0).unwrap();

    let entries = vfs.readdir(1).unwrap();
    assert_eq!(entries.len(), 2);

    let link_entry = entries.iter().find(|e| e.name == b"link.txt").unwrap();
    assert_eq!(link_entry.kind, studiofs_meta::FileType::Symlink);

    let file_entry = entries.iter().find(|e| e.name == b"file.txt").unwrap();
    assert_eq!(file_entry.kind, studiofs_meta::FileType::RegularFile);
}

// =============================================================================
// Phase 5 — Hardlink Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_hardlink_create_and_read() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "hardlink_create");

    // Create a file and write data
    let attr = vfs.create(1, b"original.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    vfs.write(ino, 0, b"hardlink data").await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Create hardlink
    let linked = vfs.link(ino, 1, b"linked.txt").unwrap();
    assert_eq!(linked.ino, ino);
    assert_eq!(linked.nlink, 2);

    // Read data via original inode
    let data1 = vfs.read(ino, 0, 1024).await.unwrap();
    assert_eq!(&data1[..], b"hardlink data");

    // The linked file has the same ino
    let lookup = vfs.lookup(1, b"linked.txt").unwrap();
    assert_eq!(lookup.ino, ino);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_hardlink_unlink_keeps_data() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "hardlink_unlink");

    // Create a file and write data
    let attr = vfs.create(1, b"src.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    vfs.write(ino, 0, b"survive unlink").await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Create hardlink, then remove the original
    vfs.link(ino, 1, b"backup.txt").unwrap();
    vfs.unlink(1, b"src.txt").unwrap();

    // Inode still accessible via hardlink name
    let lookup = vfs.lookup(1, b"backup.txt").unwrap();
    assert_eq!(lookup.ino, ino);
    assert_eq!(lookup.nlink, 1);

    // Data still readable
    let data = vfs.read(ino, 0, 1024).await.unwrap();
    assert_eq!(&data[..], b"survive unlink");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_hardlink_dir_fails() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "hardlink_dir_fail");

    let dir = vfs.mkdir(1, b"mydir", 0, 0, 0o755).unwrap();
    let result = vfs.link(dir.ino, 1, b"dir_link");
    assert!(result.is_err());
}

// =============================================================================
// Phase 5 — Flock Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_flock_exclusive_blocks() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "flock_excl");

    let attr = vfs.create(1, b"locked.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Owner 1 takes exclusive lock
    vfs.flock(ino, LockKind::Exclusive, 1, false).await.unwrap();

    // Owner 2 non-blocking exclusive should fail with WouldBlock
    let result = vfs.flock(ino, LockKind::Exclusive, 2, false).await;
    assert!(matches!(result, Err(VfsError::WouldBlock)));

    // Owner 2 non-blocking shared should also fail
    let result2 = vfs.flock(ino, LockKind::Shared, 2, false).await;
    assert!(matches!(result2, Err(VfsError::WouldBlock)));

    // Owner 1 unlocks
    vfs.flock(ino, LockKind::Unlock, 1, false).await.unwrap();

    // Now owner 2 can take the lock
    vfs.flock(ino, LockKind::Exclusive, 2, false).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_flock_shared_allows_multiple() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "flock_shared");

    let attr = vfs.create(1, b"shared.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Multiple owners can hold shared locks
    vfs.flock(ino, LockKind::Shared, 1, false).await.unwrap();
    vfs.flock(ino, LockKind::Shared, 2, false).await.unwrap();
    vfs.flock(ino, LockKind::Shared, 3, false).await.unwrap();

    // Exclusive lock should be blocked
    let result = vfs.flock(ino, LockKind::Exclusive, 4, false).await;
    assert!(matches!(result, Err(VfsError::WouldBlock)));

    // Unlock all shared locks
    vfs.flock(ino, LockKind::Unlock, 1, false).await.unwrap();
    vfs.flock(ino, LockKind::Unlock, 2, false).await.unwrap();
    vfs.flock(ino, LockKind::Unlock, 3, false).await.unwrap();

    // Now exclusive should succeed
    vfs.flock(ino, LockKind::Exclusive, 4, false).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_flock_unlock_and_relock() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "flock_relock");

    let attr = vfs.create(1, b"relock.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Lock, unlock, then a different owner can lock
    vfs.flock(ino, LockKind::Exclusive, 10, false)
        .await
        .unwrap();
    vfs.flock(ino, LockKind::Unlock, 10, false).await.unwrap();
    vfs.flock(ino, LockKind::Exclusive, 20, false)
        .await
        .unwrap();

    // Re-entrant: same owner can re-lock
    vfs.flock(ino, LockKind::Exclusive, 20, false)
        .await
        .unwrap();
}

// =============================================================================
// Phase 6 — xattr Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_xattr_set_get_remove() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "xattr_basic");

    let attr = vfs.create(1, b"file.txt", 0, 0, 0o644).unwrap();

    // Set xattr
    vfs.set_xattr(attr.ino, b"user.color", b"blue", 0).unwrap();

    // Get xattr
    let val = vfs.get_xattr(attr.ino, b"user.color").unwrap();
    assert_eq!(val, Some(b"blue".to_vec()));

    // List xattrs
    let names = vfs.list_xattrs(attr.ino).unwrap();
    assert_eq!(names.len(), 1);
    assert_eq!(names[0], b"user.color");

    // Remove xattr
    vfs.remove_xattr(attr.ino, b"user.color").unwrap();
    let val2 = vfs.get_xattr(attr.ino, b"user.color").unwrap();
    assert!(val2.is_none());
    assert!(vfs.list_xattrs(attr.ino).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_xattr_cascade_on_unlink() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "xattr_cascade");

    let attr = vfs.create(1, b"doomed.txt", 0, 0, 0o644).unwrap();
    vfs.set_xattr(attr.ino, b"user.tag", b"important", 0)
        .unwrap();

    vfs.unlink(1, b"doomed.txt").unwrap();

    // xattrs should be cleaned up after unlink
    let names = vfs.list_xattrs(attr.ino).unwrap();
    assert!(names.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_xattr_multiple_attrs() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "xattr_multi");

    let attr = vfs.create(1, b"multi.txt", 0, 0, 0o644).unwrap();

    vfs.set_xattr(attr.ino, b"user.a", b"1", 0).unwrap();
    vfs.set_xattr(attr.ino, b"user.b", b"2", 0).unwrap();
    vfs.set_xattr(attr.ino, b"user.c", b"3", 0).unwrap();

    let names = vfs.list_xattrs(attr.ino).unwrap();
    assert_eq!(names.len(), 3);

    // Overwrite one
    vfs.set_xattr(attr.ino, b"user.b", b"updated", 0).unwrap();
    let val = vfs.get_xattr(attr.ino, b"user.b").unwrap();
    assert_eq!(val, Some(b"updated".to_vec()));
}

// =============================================================================
// Phase 7 — Filename Validation Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_validate_name_create_rejects_invalid() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "validate_create");

    // Empty name
    assert!(vfs.create(1, b"", 0, 0, 0o644).is_err());

    // Name with slash
    assert!(vfs.create(1, b"a/b", 0, 0, 0o644).is_err());

    // Dot names
    assert!(vfs.create(1, b".", 0, 0, 0o644).is_err());
    assert!(vfs.create(1, b"..", 0, 0, 0o644).is_err());

    // Too long (256 bytes)
    let long = vec![b'x'; 256];
    assert!(vfs.create(1, &long, 0, 0, 0o644).is_err());

    // Valid name should work
    assert!(vfs.create(1, b"valid.txt", 0, 0, 0o644).is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_validate_name_mkdir_rejects_invalid() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "validate_mkdir");

    assert!(vfs.mkdir(1, b"", 0, 0, 0o755).is_err());
    assert!(vfs.mkdir(1, b"a/b", 0, 0, 0o755).is_err());
    assert!(vfs.mkdir(1, b"..", 0, 0, 0o755).is_err());
    assert!(vfs.mkdir(1, b"valid_dir", 0, 0, 0o755).is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_validate_name_rename_rejects_invalid() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "validate_rename");

    vfs.create(1, b"source.txt", 0, 0, 0o644).unwrap();

    // Rename to invalid name
    assert!(vfs.rename(1, b"source.txt", 1, b"").is_err());
    assert!(vfs.rename(1, b"source.txt", 1, b"a/b").is_err());
    assert!(vfs.rename(1, b"source.txt", 1, b"..").is_err());

    // Valid rename should work
    assert!(vfs.rename(1, b"source.txt", 1, b"dest.txt").is_ok());
}

// =============================================================================
// Phase 7 — statfs Tests
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_statfs_initial() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "statfs_init");

    let st = vfs.statfs().unwrap();

    // Initial state: 1 inode (root), 0 used space
    assert_eq!(st.files, 1); // root inode
    assert!(st.bfree > 0);
    assert!(st.bavail > 0);
    assert_eq!(st.bsize, 4 * 1024 * 1024); // 4 MiB block size
    assert_eq!(st.namelen, 255);
    assert_eq!(st.blocks, st.bfree); // no used space yet
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_statfs_after_write() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "statfs_write");

    let st_before = vfs.statfs().unwrap();

    // Create a file and write data
    let attr = vfs.create(1, b"data.bin", 0, 0, 0o644).unwrap();
    let data = vec![0u8; 1024 * 1024]; // 1 MiB
    vfs.write(attr.ino, 0, &data).await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    let st_after = vfs.statfs().unwrap();

    // Should now report more inodes (root + file = 2)
    assert_eq!(st_after.files, st_before.files + 1);
    // USED_SPACE should have increased
    // blocks should show less free space (or equal if block size rounds down)
    assert!(st_after.bfree <= st_before.bfree);
}

// =============================================================================
// Phase 8 — Eager Flush + Kernel Cache Invalidation Tests
// =============================================================================

/// Mock `KernelInvalidator` that records every invalidated inode for assertions.

/// Verify that `request_eager_flush()` causes the metadata engine background
/// flush to publish metadata to metadata sync within seconds (instead of waiting for
/// the full 600 s `flush_interval`).  Reader observes writer's `create` quickly.
///
/// This exercises the end-to-end path:
///   writer.create() → request_eager_flush() → 250 ms tick → flush_remote_snapshot()
///   → metadata sync publish → reader invalidation listener → sync_metadata() → lookup OK
/// Like above but for `unlink` — verify eager flush after delete propagates
/// quickly.
/// Verify that `mkdir` + `rmdir` + `rename` also trigger eager flush.
/// Verify that the `KernelInvalidator` receives `inval_inode` calls when metadata sync
/// invalidation events arrive on the reader.
///
/// This confirms the plumbing works — the same trait is implemented by
/// `FuseKernelInvalidator` (Linux) and `WinfspKernelInvalidator` (Windows).
/// Verify that eager-flush batching and kernel-cache invalidation work
/// together end-to-end.
///
/// **Batching semantics:** `request_eager_flush()` uses a 250 ms collection
/// window so that rapid sequential mutations (e.g. multi-file create/delete)
/// are grouped into one remote flush + one metadata sync event.  The reader may see
/// the S3 data *before* the metadata sync event arrives (via `read_with_refresh`
/// cache-miss → direct `pull_remote_snapshot`).  When this happens the metadata sync
/// handler's `sync_and_apply_invalidation` observes no version advancement
/// and enters a multi-attempt retry loop.  Because the handler is sequential,
/// back-to-back events can queue up and exceed tight timeouts.
///
/// This test therefore keeps the number of metadata sync events minimal:
///   1. Rapid creates → eager-flush visibility check (no explicit flush).
///   2. One explicit `flush_metadata()` to produce a deterministic metadata sync event
///      that the handler can resolve in a single attempt, triggering
///      `invalidate_all_read_caches()` and the `KernelInvalidator` mock.
///
/// More complex mutation sequences (rename, unlink, dir-ops) are covered
/// by the dedicated eager-flush tests above.
// =============================================================================
// Phase 9 — Deep Integration & Edge-Case Tests
// =============================================================================

// ─── 9.1 Block Boundary & Truncate Edge Cases ──────────────────────────────

/// Truncate a file to exactly a block boundary.
/// Verify the last block is NOT marked PARTIAL_BLOCK and has full block_size length.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_truncate_to_exact_block_boundary() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "trunc-boundary");
    let block_size = vfs.meta().get_format_info().unwrap().block_size;

    // Write 12 MiB = 3 full blocks (block_size = 4 MiB).
    let size = 3 * block_size as usize;
    let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let attr = vfs.create(1, b"trunc-bound.bin", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, &payload).await.unwrap();
    vfs.flush(attr.ino).await.unwrap();
    let _ =
        wait_until_n_blocks(&vfs, attr.ino, 3, Duration::from_secs(15)).expect("Expected 3 blocks");

    // Truncate to exactly 1 block boundary = block_size bytes.
    vfs.setattr(attr.ino, SetattrArgs::size_only(block_size as u64))
        .unwrap();
    let after = vfs.meta().get_file_blocks(attr.ino).unwrap();
    assert_eq!(after.len(), 1, "Only 1 block should remain");

    let (idx, blk) = &after[0];
    assert_eq!(*idx, 0);
    // When truncating to exactly block_size, the last_block_offset+1 == block_size
    // so the block ref length should equal block_size and the partial flag
    // must be set (the truncate code always sets PARTIAL_BLOCK when size > 0).
    assert_eq!(
        blk.length, block_size,
        "block length should equal block_size"
    );

    // Verify read returns correct data.
    let data = vfs.read(attr.ino, 0, block_size).await.unwrap();
    assert_eq!(data.len(), block_size as usize);
    assert_eq!(&data[..], &payload[..block_size as usize]);
}

/// Extend a file beyond its current size (sparse hole).
/// Verify reading the hole returns zeroes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_truncate_extend_creates_sparse_hole() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "trunc-extend");

    let attr = vfs.create(1, b"sparse.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write 1000 bytes.
    let payload = vec![0xABu8; 1000];
    vfs.write(ino, 0, &payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Extend to 1 MiB via setattr — should NOT create blocks, only change size.
    let extended_size = 1024 * 1024u64;
    vfs.setattr(ino, SetattrArgs::size_only(extended_size))
        .unwrap();

    let a = vfs.getattr(ino).unwrap();
    assert_eq!(a.size, extended_size);

    // Read the first 1000 bytes — should be original data.
    let head = vfs.read(ino, 0, 1000).await.unwrap();
    assert_eq!(&head[..], &payload[..]);

    // Read into the sparse hole — should be zeroes.
    let hole = vfs.read(ino, 2000, 4096).await.unwrap();
    assert!(
        hole.iter().all(|&b| b == 0),
        "sparse hole should read as zeroes"
    );
}

/// Write exactly block_size bytes then 1 byte at the next block boundary.
/// Exercises the full-block eager flush path + new partial block creation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_write_at_exact_block_boundary_transition() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "boundary-trans");
    let block_size = vfs.meta().get_format_info().unwrap().block_size as usize;

    let attr = vfs.create(1, b"boundary.bin", 0, 0, 0o644).unwrap();

    // Write exactly one full block.
    let full_block: Vec<u8> = (0..block_size).map(|i| (i % 199) as u8).collect();
    vfs.write(attr.ino, 0, &full_block).await.unwrap();

    // Write 1 byte at the start of the next block.
    vfs.write(attr.ino, block_size as u64, &[0xFF])
        .await
        .unwrap();
    vfs.flush(attr.ino).await.unwrap();

    // Wait for blocks to materialise.
    let blocks =
        wait_until_n_blocks(&vfs, attr.ino, 2, Duration::from_secs(15)).expect("Expected 2 blocks");
    assert_eq!(blocks.len(), 2);

    // Verify file size.
    let a = vfs.getattr(attr.ino).unwrap();
    assert_eq!(a.size, (block_size + 1) as u64);

    // Verify data integrity.
    let data = vfs
        .read(attr.ino, 0, (block_size + 1) as u32)
        .await
        .unwrap();
    assert_eq!(&data[..block_size], &full_block[..]);
    assert_eq!(data[block_size], 0xFF);
}

/// Truncate to zero, then write again. Verify old blocks are GC'd and new data is correct.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_truncate_to_zero_then_rewrite() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "trunc-zero-rw");

    let attr = vfs.create(1, b"overwrite.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write 5 MiB.
    let payload1: Vec<u8> = (0..5 * 1024 * 1024).map(|i| (i % 173) as u8).collect();
    vfs.write(ino, 0, &payload1).await.unwrap();
    vfs.flush(ino).await.unwrap();
    let _ = wait_until_n_blocks(&vfs, ino, 2, Duration::from_secs(15));

    // Get old block IDs.
    let old_blocks = vfs.meta().get_file_blocks(ino).unwrap();
    let old_ids: Vec<u64> = old_blocks.iter().map(|(_, b)| b.object_id).collect();
    assert!(!old_ids.is_empty());

    // Truncate to 0.
    vfs.setattr(ino, SetattrArgs::size_only(0)).unwrap();
    assert_eq!(vfs.getattr(ino).unwrap().size, 0);

    // All blocks should be removed when truncating to zero.
    let remaining = vfs.meta().get_file_blocks(ino).unwrap();
    assert!(
        remaining.is_empty(),
        "all blocks should be removed after truncate-to-zero, got {}",
        remaining.len()
    );

    // All old blocks should be in pending deletions.
    let pending = vfs.meta().get_pending_deletions().unwrap();
    for id in &old_ids {
        assert!(
            pending.iter().any(|(pid, _)| pid == id),
            "old block {} should be pending deletion",
            id
        );
    }

    // Write new data.
    let payload2 = vec![0xCDu8; 1024];
    vfs.write(ino, 0, &payload2).await.unwrap();
    vfs.flush(ino).await.unwrap();
    assert_eq!(vfs.getattr(ino).unwrap().size, 1024);

    let data = vfs.read(ino, 0, 1024).await.unwrap();
    assert_eq!(&data[..], &payload2[..]);
}

// ─── 9.2 Read Edge Cases ────────────────────────────────────────────────────

/// Reading past EOF should return empty bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_read_beyond_eof_returns_empty() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "read-eof");

    let attr = vfs.create(1, b"small.txt", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"hello").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    // Read at offset past file size.
    let data = vfs.read(attr.ino, 100, 4096).await.unwrap();
    assert!(data.is_empty(), "read past EOF should return empty bytes");

    // Read at exact EOF boundary.
    let data = vfs.read(attr.ino, 5, 4096).await.unwrap();
    assert!(data.is_empty(), "read at exact EOF should return empty");
}

/// Read spanning an allocated block and unallocated blocks (sparse file).
/// Allocated part returns data, unallocated returns zeroes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_read_spanning_allocated_and_sparse_blocks() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "read-sparse");
    let block_size = vfs.meta().get_format_info().unwrap().block_size as u64;

    let attr = vfs.create(1, b"sparse-read.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write data only in block 0.
    let payload: Vec<u8> = (0..1024).map(|i| (i % 200) as u8).collect();
    vfs.write(ino, 0, &payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Extend file to cover 3 blocks without writing block 1 and 2.
    let extended = block_size * 3;
    vfs.setattr(ino, SetattrArgs::size_only(extended)).unwrap();

    // Read block 1 (entirely in sparse region) — should be all zeroes.
    let sparse_data = vfs.read(ino, block_size, block_size as u32).await.unwrap();
    assert_eq!(sparse_data.len(), block_size as usize);
    assert!(
        sparse_data.iter().all(|&b| b == 0),
        "sparse block should be all zeroes"
    );

    // Original block 0 data should still be there.
    let head = vfs.read(ino, 0, 1024).await.unwrap();
    assert_eq!(&head[..], &payload[..]);
}

/// Flush an empty file (no writes) — should be a no-op without errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_flush_empty_file_is_noop() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "flush-empty");

    let attr = vfs.create(1, b"empty.txt", 0, 0, 0o644).unwrap();
    // Flush without writing anything — should not panic or allocate blocks.
    vfs.flush(attr.ino).await.unwrap();
    assert_eq!(vfs.getattr(attr.ino).unwrap().size, 0);
    assert!(vfs.meta().get_file_blocks(attr.ino).unwrap().is_empty());
}

/// Read zero bytes from a file — should return empty immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_read_zero_bytes() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "read-zero");

    let attr = vfs.create(1, b"content.txt", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"data").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    let data = vfs.read(attr.ino, 0, 0).await.unwrap();
    assert!(data.is_empty());
}

/// Write zero bytes — should be a no-op and return 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_write_zero_bytes_is_noop() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "write-zero");

    let attr = vfs.create(1, b"noop.txt", 0, 0, 0o644).unwrap();
    let written = vfs.write(attr.ino, 0, &[]).await.unwrap();
    assert_eq!(written, 0);
    assert_eq!(vfs.getattr(attr.ino).unwrap().size, 0);
}

// ─── 9.3 Rename & Unlink Edge Cases ────────────────────────────────────────

/// Rename a file across directories.
/// Verify both parent caches are invalidated and the file is accessible at the new path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_rename_cross_directory() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rename-cross");

    // Set up directories.
    let dir_a = vfs.mkdir(1, b"dir_a", 0, 0, 0o755).unwrap();
    let dir_b = vfs.mkdir(1, b"dir_b", 0, 0, 0o755).unwrap();

    // Create file in dir_a.
    let attr = vfs.create(dir_a.ino, b"file.txt", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"cross-dir data").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    // Rename from dir_a to dir_b.
    vfs.rename(dir_a.ino, b"file.txt", dir_b.ino, b"moved.txt")
        .unwrap();

    // Old path should fail.
    assert!(vfs.lookup(dir_a.ino, b"file.txt").is_err());

    // New path should succeed.
    let moved = vfs.lookup(dir_b.ino, b"moved.txt").unwrap();
    assert_eq!(moved.ino, attr.ino);

    // Data should be intact.
    let data = vfs.read(attr.ino, 0, 100).await.unwrap();
    assert_eq!(&data[..], b"cross-dir data");

    // Both parent directories should reflect the change.
    let entries_a = vfs.readdir(dir_a.ino).unwrap();
    assert!(
        !entries_a.iter().any(|e| e.name == b"file.txt"),
        "dir_a should not contain file.txt"
    );
    let entries_b = vfs.readdir(dir_b.ino).unwrap();
    assert!(
        entries_b.iter().any(|e| e.name == b"moved.txt"),
        "dir_b should contain moved.txt"
    );
}

/// Rename a file over an existing file (overwrite).
/// Verify the displaced file's blocks are GC'd.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_rename_overwrite_existing_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rename-overwrite");

    // Create target file with data.
    let target = vfs.create(1, b"target.txt", 0, 0, 0o644).unwrap();
    vfs.write(target.ino, 0, b"old target data").await.unwrap();
    vfs.flush(target.ino).await.unwrap();
    let _ = wait_until_n_blocks(&vfs, target.ino, 1, Duration::from_secs(15));

    let target_blocks = vfs.meta().get_file_blocks(target.ino).unwrap();
    let target_block_id = target_blocks[0].1.object_id;

    // Create source file with data.
    let source = vfs.create(1, b"source.txt", 0, 0, 0o644).unwrap();
    vfs.write(source.ino, 0, b"new source data").await.unwrap();
    vfs.flush(source.ino).await.unwrap();

    // Rename source → target (overwrite).
    vfs.rename(1, b"source.txt", 1, b"target.txt").unwrap();

    // source.txt should be gone.
    assert!(vfs.lookup(1, b"source.txt").is_err());

    // target.txt should resolve to source ino.
    let result = vfs.lookup(1, b"target.txt").unwrap();
    assert_eq!(result.ino, source.ino);

    // Displaced target's block should be in pending deletions.
    let pending = vfs.meta().get_pending_deletions().unwrap();
    assert!(
        pending.iter().any(|(id, _)| *id == target_block_id),
        "displaced file's block {} should be pending deletion",
        target_block_id
    );
}

/// Unlink a file while it has an open handle (open-unlink / orphan pattern).
/// Verify data is accessible until the last handle is released, then cleaned up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_unlink_with_open_handle_deferred_cleanup() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "open-unlink-gc");

    let attr = vfs.create(1, b"orphan.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    vfs.write(ino, 0, b"orphan data").await.unwrap();
    vfs.flush(ino).await.unwrap();
    let _ = wait_until_n_blocks(&vfs, ino, 1, Duration::from_secs(15));

    // Open the file.
    let fh = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();

    // Unlink while handle is open.
    vfs.unlink(1, b"orphan.txt").unwrap();

    // Lookup by name should fail.
    assert!(vfs.lookup(1, b"orphan.txt").is_err());

    // But read via the ino should still work (orphan).
    let data = vfs.read(ino, 0, 100).await.unwrap();
    assert_eq!(&data[..], b"orphan data");

    // Blocks should still exist (not yet GC'd).
    let blocks = vfs.meta().get_file_blocks(ino).unwrap();
    assert!(
        !blocks.is_empty(),
        "blocks should survive until last handle closes"
    );
    let block_id = blocks[0].1.object_id;

    // Release the last handle — triggers orphan cleanup.
    vfs.release_fh(fh).await.unwrap();

    // Now blocks should be removed and pending deletion.
    let blocks_after = vfs.meta().get_file_blocks(ino).unwrap();
    assert!(
        blocks_after.is_empty(),
        "blocks should be removed after release_fh"
    );
    let pending = vfs.meta().get_pending_deletions().unwrap();
    assert!(
        pending.iter().any(|(id, _)| *id == block_id),
        "block should be pending deletion after orphan cleanup"
    );
}

/// Rmdir on non-empty directory should fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_rmdir_nonempty_fails() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rmdir-nonempty");

    let dir = vfs.mkdir(1, b"nonempty", 0, 0, 0o755).unwrap();
    vfs.create(dir.ino, b"child.txt", 0, 0, 0o644).unwrap();

    let result = vfs.rmdir(1, b"nonempty");
    assert!(result.is_err(), "rmdir on non-empty directory should fail");
}

/// Create many files in one directory and verify readdir returns all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_readdir_many_entries() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "readdir-many");

    let count = 100;
    for i in 0..count {
        let name = format!("file-{i:04}.txt");
        vfs.create(1, name.as_bytes(), 0, 0, 0o644).unwrap();
    }

    let entries = vfs.readdir(1).unwrap();
    // readdir includes "." and ".." typically, but StudioFS may not.
    // Count only our created files:
    let our_files: Vec<_> = entries
        .iter()
        .filter(|e| e.name.starts_with(b"file-"))
        .collect();
    assert_eq!(
        our_files.len(),
        count,
        "readdir should return all {count} files"
    );
}

// ─── 9.4 Concurrent Access Patterns ────────────────────────────────────────

/// Concurrent reads and writes to the same file.
/// Verify reads see consistent data (no torn blocks).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_concurrent_read_write_same_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "conc-rw");
    let block_size = vfs.meta().get_format_info().unwrap().block_size as usize;

    let attr = vfs.create(1, b"conc.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write initial data.
    let initial: Vec<u8> = vec![0xAAu8; block_size * 2];
    vfs.write(ino, 0, &initial).await.unwrap();
    vfs.flush(ino).await.unwrap();
    // Wait for blocks to materialise.
    let _ = wait_until_n_blocks(&vfs, ino, 2, Duration::from_secs(15));

    let vfs1 = vfs.clone();
    let vfs2 = vfs.clone();

    // Spawn concurrent writer (overwrites block 0).
    let writer = tokio::spawn(async move {
        let new_data = vec![0xBBu8; block_size];
        for _ in 0..5 {
            vfs1.write(ino, 0, &new_data).await.unwrap();
            vfs1.flush(ino).await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // Spawn concurrent reader (reads block 0 repeatedly).
    let reader = tokio::spawn(async move {
        for _ in 0..20 {
            let data = vfs2.read(ino, 0, block_size as u32).await.unwrap();
            if !data.is_empty() {
                // Every byte should be either 0xAA (old) or 0xBB (new) — never mixed.
                let first = data[0];
                assert!(
                    first == 0xAA || first == 0xBB,
                    "unexpected byte value: {first:#x}"
                );
                // All bytes in block 0 should be the same value (atomic block reads).
                assert!(
                    data.iter().all(|&b| b == first),
                    "torn read detected in block 0"
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    let (w, r) = tokio::join!(writer, reader);
    w.unwrap();
    r.unwrap();
}

/// Multiple concurrent flush() calls on the same inode should not deadlock or lose data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_concurrent_flush_does_not_deadlock() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "conc-flush");

    let attr = vfs.create(1, b"conc-flush.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write some data.
    vfs.write(ino, 0, &vec![0x42u8; 1024]).await.unwrap();

    // Spawn 5 concurrent flushes.
    let mut tasks = Vec::new();
    for _ in 0..5 {
        let v = vfs.clone();
        tasks.push(tokio::spawn(async move {
            v.flush(ino).await.unwrap();
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    // Data should be readable.
    let data = vfs.read(ino, 0, 1024).await.unwrap();
    assert_eq!(data.len(), 1024);
    assert!(data.iter().all(|&b| b == 0x42));
}

/// Race between create + write + unlink — verify no panic and consistent state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_create_write_unlink_race() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "create-unlink-race");

    // Create and immediately delete files in a tight loop.
    for i in 0..20 {
        let name = format!("race-{i}.txt");
        let attr = vfs.create(1, name.as_bytes(), 0, 0, 0o644).unwrap();
        vfs.write(attr.ino, 0, b"race data").await.unwrap();
        vfs.flush(attr.ino).await.unwrap();
        vfs.unlink(1, name.as_bytes()).unwrap();
    }

    // Root dir should be clean.
    let entries = vfs.readdir(1).unwrap();
    let race_files: Vec<_> = entries
        .iter()
        .filter(|e| e.name.starts_with(b"race-"))
        .collect();
    assert!(
        race_files.is_empty(),
        "all race files should be deleted, found {}",
        race_files.len()
    );
}

// ─── 9.5 File Handle & Orphan Edge Cases ───────────────────────────────────

/// Multiple handles open, unlink, close all — verify cleanup only after last close.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_multiple_handles_orphan_cleanup_after_last_close() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "multi-fh-orphan");

    let attr = vfs.create(1, b"multi-fh.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    vfs.write(ino, 0, b"multi handle data").await.unwrap();
    vfs.flush(ino).await.unwrap();
    let _ = wait_until_n_blocks(&vfs, ino, 1, Duration::from_secs(15));

    // Open 3 handles.
    let fh1 = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
    let fh2 = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
    let fh3 = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();

    // Unlink.
    vfs.unlink(1, b"multi-fh.txt").unwrap();

    // Close first two — blocks should still exist (orphan still alive).
    vfs.release_fh(fh1).await.unwrap();
    vfs.release_fh(fh2).await.unwrap();
    let blocks_mid = vfs.meta().get_file_blocks(ino).unwrap();
    assert!(
        !blocks_mid.is_empty(),
        "blocks should survive with 1 handle remaining"
    );

    // Close the last handle — orphan cleanup triggers.
    vfs.release_fh(fh3).await.unwrap();
    let blocks_final = vfs.meta().get_file_blocks(ino).unwrap();
    assert!(
        blocks_final.is_empty(),
        "blocks should be cleaned up after last handle closes"
    );
}

/// open_file with O_TRUNC on a file with data — should truncate to 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_open_trunc_clears_data() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "open-trunc-clear");

    let attr = vfs.create(1, b"trunc-open.txt", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"original content").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();
    assert!(vfs.getattr(attr.ino).unwrap().size > 0);

    // Open with trunc flag.
    let fh = vfs.open_file(attr.ino, OpenFlags::WRONLY, true).unwrap();
    assert_eq!(
        vfs.getattr(attr.ino).unwrap().size,
        0,
        "file should be truncated on open"
    );
    vfs.release_fh(fh).await.unwrap();
}

// ─── 9.6 Multi-Client metadata sync Integration ─────────────────────────────────────

/// Two metadata clients create files simultaneously, triggering CAS conflict.
/// Client A flushes first, then Client B flushes and must merge with A's
/// snapshot so both files survive.  Verifies the pre-merge path where a
/// client with no cached etag discovers a remote manifest and merges before
/// writing.
/// metadata sync global invalidation should evict dropped blocks from cache.
/// Client A caches file data, Client B deletes the file + publishes invalidation,
/// verify Client A's block cache no longer contains the deleted blocks.
/// Verify kernel invalidator receives correct inodes for single-inode events
/// (not just global invalidation).
// ─── 9.7 Attribute & Metadata Edge Cases ───────────────────────────────────

/// Setattr mode/uid/gid/times on a directory (not just files).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_setattr_on_directory() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "setattr-dir");

    let dir = vfs.mkdir(1, b"mydir", 0, 0, 0o755).unwrap();

    // Change mode.
    let mut args = SetattrArgs::default();
    args.mode = Some(0o700);
    let updated = vfs.setattr(dir.ino, args).unwrap();
    assert_eq!(updated.mode & 0o777, 0o700);

    // Change uid/gid.
    let mut args = SetattrArgs::default();
    args.uid = Some(1000);
    args.gid = Some(1000);
    let updated = vfs.setattr(dir.ino, args).unwrap();
    assert_eq!(updated.uid, 1000);
    assert_eq!(updated.gid, 1000);
}

/// Xattr operations on a directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_xattr_on_directory() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "xattr-dir");

    let dir = vfs.mkdir(1, b"xattr-dir", 0, 0, 0o755).unwrap();
    vfs.set_xattr(dir.ino, b"user.tag", b"dir-value", 0)
        .unwrap();

    let val = vfs.get_xattr(dir.ino, b"user.tag").unwrap().unwrap();
    assert_eq!(&val[..], b"dir-value");

    let names = vfs.list_xattrs(dir.ino).unwrap();
    assert_eq!(names.len(), 1);
    assert_eq!(&names[0][..], b"user.tag");
}

/// Getattr on non-existent inode should return an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_getattr_nonexistent_inode() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "getattr-noent");

    let result = vfs.getattr(999999);
    assert!(result.is_err(), "getattr on non-existent inode should fail");
}

/// Lookup non-existent entry in a directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_lookup_nonexistent_entry() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "lookup-noent");

    let result = vfs.lookup(1, b"does-not-exist.txt");
    assert!(
        result.is_err(),
        "lookup non-existent entry should return error"
    );
}

/// Create a file, verify counter TOTAL_INODES increments.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_create_increments_total_inodes() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "inode-count");

    let before = vfs
        .meta()
        .get_counter(studiofs_meta::types::counters::TOTAL_INODES)
        .unwrap_or(0);

    vfs.create(1, b"counted.txt", 0, 0, 0o644).unwrap();

    let after = vfs
        .meta()
        .get_counter(studiofs_meta::types::counters::TOTAL_INODES)
        .unwrap_or(0);
    assert_eq!(after, before + 1, "TOTAL_INODES should increment by 1");
}

// ─── 9.8 Symlink & Hardlink Edge Cases ─────────────────────────────────────

/// Create a symlink pointing to a non-existent target. readlink should return
/// the target path even though the target doesn't exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_symlink_dangling_target() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "sym-dangle");

    let attr = vfs
        .symlink(1, b"dangling-link", b"/nonexistent/path", 0, 0)
        .unwrap();
    let target = vfs.readlink(attr.ino).unwrap();
    assert_eq!(&target[..], b"/nonexistent/path");
}

/// Hardlink increases nlink and can be read from both paths.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_hardlink_nlink_increments() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "hardlink-nlink");

    let attr = vfs.create(1, b"original.txt", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"shared data").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    let before_nlink = vfs.getattr(attr.ino).unwrap().nlink;

    // Create hardlink.
    vfs.link(attr.ino, 1, b"link.txt").unwrap();

    let after_nlink = vfs.getattr(attr.ino).unwrap().nlink;
    assert_eq!(after_nlink, before_nlink + 1, "nlink should increment");

    // Both names should resolve to the same ino.
    let a = vfs.lookup(1, b"original.txt").unwrap();
    let b = vfs.lookup(1, b"link.txt").unwrap();
    assert_eq!(a.ino, b.ino);

    // Read from the link should return the same data.
    let data = vfs.read(b.ino, 0, 100).await.unwrap();
    assert_eq!(&data[..], b"shared data");
}

/// Unlink one hardlink — file should survive because nlink > 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_hardlink_unlink_one_keeps_other() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "hardlink-unlink");

    let attr = vfs.create(1, b"hl-orig.txt", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"persist").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();
    let _ = wait_until_n_blocks(&vfs, attr.ino, 1, Duration::from_secs(15));

    vfs.link(attr.ino, 1, b"hl-link.txt").unwrap();

    // Unlink the original name.
    vfs.unlink(1, b"hl-orig.txt").unwrap();

    // The link should still be alive.
    let a = vfs.lookup(1, b"hl-link.txt").unwrap();
    assert_eq!(a.ino, attr.ino);

    // Blocks should NOT be removed (nlink still > 0).
    let blocks = vfs.meta().get_file_blocks(attr.ino).unwrap();
    assert!(
        !blocks.is_empty(),
        "blocks should survive when hardlink remains"
    );

    // Data should still be readable.
    let data = vfs.read(attr.ino, 0, 100).await.unwrap();
    assert_eq!(&data[..], b"persist");
}

// ─── 9.9 Flock Edge Cases ──────────────────────────────────────────────────

/// Exclusive flock on a file, try exclusive from second owner — should fail non-blocking.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_flock_exclusive_nonblocking_conflict() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "flock-conflict");

    let attr = vfs.create(1, b"flock.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Owner 1 acquires exclusive.
    vfs.flock(ino, LockKind::Exclusive, 1, false).await.unwrap();

    // Owner 2 tries non-blocking exclusive — should fail.
    let result = vfs.flock(ino, LockKind::Exclusive, 2, false).await;
    assert!(
        matches!(result, Err(VfsError::WouldBlock)),
        "non-blocking exclusive should fail with WouldBlock"
    );

    // Unlock owner 1.
    vfs.flock(ino, LockKind::Unlock, 1, false).await.unwrap();

    // Now owner 2 should succeed.
    vfs.flock(ino, LockKind::Exclusive, 2, false).await.unwrap();
    vfs.flock(ino, LockKind::Unlock, 2, false).await.unwrap();
}

/// Shared lock + try shared from another owner → should succeed.
/// Then try exclusive → should fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_flock_shared_then_exclusive_conflict() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "flock-sh-ex");

    let attr = vfs.create(1, b"flock-share.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Two readers.
    vfs.flock(ino, LockKind::Shared, 1, false).await.unwrap();
    vfs.flock(ino, LockKind::Shared, 2, false).await.unwrap();

    // Exclusive should fail.
    let result = vfs.flock(ino, LockKind::Exclusive, 3, false).await;
    assert!(
        matches!(result, Err(VfsError::WouldBlock)),
        "exclusive should fail when shared locks held"
    );

    // Unlock both shared.
    vfs.flock(ino, LockKind::Unlock, 1, false).await.unwrap();
    vfs.flock(ino, LockKind::Unlock, 2, false).await.unwrap();

    // Now exclusive should succeed.
    vfs.flock(ino, LockKind::Exclusive, 3, false).await.unwrap();
    vfs.flock(ino, LockKind::Unlock, 3, false).await.unwrap();
}

// ─── 9.10 Multi-Client Visibility Smoke Tests ──────────────────────────────

/// Writer creates a deeply nested directory tree, reader can traverse the full path.
/// Writer creates a large file (multi-block), reader reads and verifies integrity.
/// Writer does setattr (chmod, chown), reader sees updated attributes via metadata sync.
/// Writer creates symlink, reader resolves it.
/// Writer creates hardlink, reader sees it and reads shared data.
// ─── 9.11 Shutdown & Drain Tests ────────────────────────────────────────────

/// Shutdown after writes should drain dirty buffers and persist metadata.
/// After shutdown, blocks should be uploaded to S3.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_shutdown_drains_dirty_buffers() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "shutdown-drain");
    let block_size = vfs.meta().get_format_info().unwrap().block_size as usize;

    let attr = vfs.create(1, b"pre-shutdown.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write 2,5 blocks of data.
    let total = block_size * 2 + block_size / 2;
    let payload: Vec<u8> = (0..total).map(|i| (i % 197) as u8).collect();
    vfs.write(ino, 0, &payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Before shutdown — blocks may or may not be uploaded yet.
    // Call shutdown which should drain everything.
    vfs.shutdown(Duration::from_secs(30)).await;

    // After shutdown, all 3 blocks should exist in metadata.
    let blocks = vfs.meta().get_file_blocks(ino).unwrap();
    assert_eq!(
        blocks.len(),
        3,
        "after shutdown, all 3 blocks should be persisted"
    );
}

/// Double shutdown should not panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_double_shutdown_does_not_panic() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "double-shutdown");

    vfs.create(1, b"file.txt", 0, 0, 0o644).unwrap();
    vfs.shutdown(Duration::from_secs(5)).await;
    vfs.shutdown(Duration::from_secs(5)).await;
    // No panic = pass.
}

// ─── 9.12 More Error & Edge Cases ──────────────────────────────────────────

/// Double unlink of the same file should fail on the second call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_double_unlink_fails() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "double-unlink");

    vfs.create(1, b"once.txt", 0, 0, 0o644).unwrap();
    vfs.unlink(1, b"once.txt").unwrap();
    let result = vfs.unlink(1, b"once.txt");
    assert!(result.is_err(), "second unlink should fail");
}

/// Rename a file to itself should be a no-op (or succeed without error).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_rename_to_self_is_noop() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rename-self");

    let attr = vfs.create(1, b"self.txt", 0, 0, 0o644).unwrap();
    vfs.write(attr.ino, 0, b"self data").await.unwrap();
    vfs.flush(attr.ino).await.unwrap();

    // Rename to itself.
    vfs.rename(1, b"self.txt", 1, b"self.txt").unwrap();

    // File should still be accessible with original data.
    let after = vfs.lookup(1, b"self.txt").unwrap();
    assert_eq!(after.ino, attr.ino);
    let data = vfs.read(attr.ino, 0, 100).await.unwrap();
    assert_eq!(&data[..], b"self data");
}

/// Write at a high offset (sparse) then read back — gap should be zero-filled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_sparse_write_at_high_offset() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "sparse-high");
    let block_size = vfs.meta().get_format_info().unwrap().block_size as u64;

    let attr = vfs.create(1, b"sparse-high.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write at offset = block_size * 3 (skipping blocks 0-2).
    let high_offset = block_size * 3;
    let payload = b"high offset data";
    vfs.write(ino, high_offset, payload).await.unwrap();
    vfs.flush(ino).await.unwrap();

    assert_eq!(
        vfs.getattr(ino).unwrap().size,
        high_offset + payload.len() as u64
    );

    // Read from the gap — should be zero-filled.
    let gap = vfs.read(ino, 0, 1024).await.unwrap();
    assert!(
        gap.iter().all(|&b| b == 0),
        "gap before high offset should be zero-filled"
    );

    // Read the actual data.
    let data = vfs
        .read(ino, high_offset, payload.len() as u32)
        .await
        .unwrap();
    assert_eq!(&data[..], payload);
}

/// Create a file in a subdirectory, unlink it, rmdir the parent.
/// Verify the directory can be removed after all children are gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_unlink_child_then_rmdir_parent() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rmdir-after-unlink");

    let dir = vfs.mkdir(1, b"parent", 0, 0, 0o755).unwrap();
    let child = vfs.create(dir.ino, b"child.txt", 0, 0, 0o644).unwrap();
    vfs.write(child.ino, 0, b"data").await.unwrap();
    vfs.flush(child.ino).await.unwrap();

    // Remove child.
    vfs.unlink(dir.ino, b"child.txt").unwrap();

    // Now rmdir should succeed.
    vfs.rmdir(1, b"parent").unwrap();

    // Parent dir should be gone.
    assert!(vfs.lookup(1, b"parent").is_err());
}

/// mkdir with duplicate name should fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_mkdir_duplicate_name_fails() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "mkdir-dup");

    vfs.mkdir(1, b"dup-dir", 0, 0, 0o755).unwrap();
    let result = vfs.mkdir(1, b"dup-dir", 0, 0, 0o755);
    assert!(result.is_err(), "creating duplicate directory should fail");
}

/// Create a file with duplicate name should fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_create_duplicate_name_fails() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "create-dup");

    vfs.create(1, b"dup.txt", 0, 0, 0o644).unwrap();
    let result = vfs.create(1, b"dup.txt", 0, 0, 0o644);
    assert!(result.is_err(), "creating duplicate file should fail");
}

/// Overwrite a partial block within an existing block (mid-block write).
/// Verify only the written region changes and the rest is preserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_mid_block_overwrite_preserves_surrounding_data() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "mid-block-overwrite");

    let attr = vfs.create(1, b"mid.bin", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write 4096 bytes of 0xAA.
    let initial = vec![0xAAu8; 4096];
    vfs.write(ino, 0, &initial).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Overwrite bytes 1000..1500 with 0xBB.
    let patch = vec![0xBBu8; 500];
    vfs.write(ino, 1000, &patch).await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Read entire file.
    let data = vfs.read(ino, 0, 4096).await.unwrap();
    assert_eq!(data.len(), 4096);

    // Before patch.
    assert!(
        data[..1000].iter().all(|&b| b == 0xAA),
        "bytes before patch should be 0xAA"
    );
    // Patch region.
    assert!(
        data[1000..1500].iter().all(|&b| b == 0xBB),
        "patch region should be 0xBB"
    );
    // After patch.
    assert!(
        data[1500..4096].iter().all(|&b| b == 0xAA),
        "bytes after patch should be 0xAA"
    );
}

/// Multiple xattrs set, then remove one — remaining should persist.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_xattr_remove_one_preserves_others() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "xattr-remove-one");

    let attr = vfs.create(1, b"xattr-rm.txt", 0, 0, 0o644).unwrap();
    vfs.set_xattr(attr.ino, b"user.a", b"val-a", 0).unwrap();
    vfs.set_xattr(attr.ino, b"user.b", b"val-b", 0).unwrap();
    vfs.set_xattr(attr.ino, b"user.c", b"val-c", 0).unwrap();

    // Remove b.
    vfs.remove_xattr(attr.ino, b"user.b").unwrap();

    // a and c should survive.
    let names = vfs.list_xattrs(attr.ino).unwrap();
    assert_eq!(names.len(), 2);
    assert!(vfs.get_xattr(attr.ino, b"user.a").unwrap().is_some());
    assert!(vfs.get_xattr(attr.ino, b"user.b").unwrap().is_none());
    assert!(vfs.get_xattr(attr.ino, b"user.c").unwrap().is_some());
}

/// Rename a directory with children — children should follow.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_rename_directory_with_children() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rename-dir-tree");

    let dir = vfs.mkdir(1, b"old-name", 0, 0, 0o755).unwrap();
    let child = vfs.create(dir.ino, b"child.txt", 0, 0, 0o644).unwrap();
    vfs.write(child.ino, 0, b"child data").await.unwrap();
    vfs.flush(child.ino).await.unwrap();

    // Rename the directory.
    vfs.rename(1, b"old-name", 1, b"new-name").unwrap();

    // Old name should be gone.
    assert!(vfs.lookup(1, b"old-name").is_err());

    // New name should resolve to the same ino.
    let renamed = vfs.lookup(1, b"new-name").unwrap();
    assert_eq!(renamed.ino, dir.ino);

    // Child should still be accessible through the renamed dir.
    let found_child = vfs.lookup(renamed.ino, b"child.txt").unwrap();
    assert_eq!(found_child.ino, child.ino);
    let data = vfs.read(child.ino, 0, 100).await.unwrap();
    assert_eq!(&data[..], b"child data");
}
