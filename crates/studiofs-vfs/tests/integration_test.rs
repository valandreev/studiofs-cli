//! Integration tests for the VFS layer against a live MinIO instance.
//!
//! These tests wire together RedbMetaEngine + DataStore + MemoryCache into a
//! StudioVfs and exercise end-to-end file operations.
//!
//! Prerequisites:
//!   minio server C:\minio --console-address ":9001"
//!
//! Run with:
//!   cargo test -p studiofs-vfs --test integration_test -- --ignored

use bytes::Bytes;
use std::sync::Arc;

use studiofs_cache::memory::MemoryCache;
use studiofs_data::store::{DataStore, DataStoreConfig};
use studiofs_meta::RedbMetaEngine;
use studiofs_vfs::vfs::StudioVfs;

/// Create a fresh VFS backed by a temporary redb and MinIO.
fn make_vfs(tmp: &tempfile::TempDir, volume_suffix: &str) -> StudioVfs {
    let db_path = tmp.path().join("meta.redb");

    let meta = RedbMetaEngine::open(&db_path).expect("failed to open RedbMetaEngine");
    meta.init_root().expect("failed to initialize root inode");

    let data_config = DataStoreConfig {
        endpoint: "http://127.0.0.1:9000".into(),
        bucket: "studiofs-test".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        region: "us-east-1".into(),
        volume_name: format!("vfs-test-{volume_suffix}"),
    };
    let data = DataStore::new(&data_config).expect("failed to create DataStore");

    let mem_cache = MemoryCache::new(64 * 1024 * 1024); // 64 MiB

    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap();
    let cache_size = 1024 * 1024 * 1024;

    let vfs = StudioVfs::new(
        Arc::new(meta),
        Arc::new(data),
        Arc::new(mem_cache),
        cache_dir_str,
        cache_size,
    ).expect("failed to create StudioVfs");

    (*vfs).clone()
}

/// Ensure the test bucket exists.
async fn ensure_bucket() {
    use opendal::{Operator, services::S3};

    let builder = S3::default()
        .endpoint("http://127.0.0.1:9000")
        .bucket("studiofs-test")
        .access_key_id("minioadmin")
        .secret_access_key("minioadmin")
        .region("us-east-1");

    let op = Operator::new(builder).unwrap().finish();
    let _ = op.write(".bucket-marker", Bytes::from_static(b"ok")).await;
}

// ────────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn create_write_read_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "cwr");

    // Create a file in root directory (inode 1)
    let attr = vfs.create(1, b"hello.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;
    assert!(ino > 1);

    // Write data
    let payload = b"Hello, StudioFS! This is an integration test.";
    let written = vfs.write(ino, 0, payload).await.unwrap();
    assert_eq!(written as usize, payload.len());

    // Read it back
    let data = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
    assert_eq!(&data[..], payload);

    // Verify file size via getattr
    let attr = vfs.getattr(ino).unwrap();
    assert_eq!(attr.size, payload.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn write_read_large_file() {
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

    // Read it all back
    let data = vfs.read(ino, 0, size as u32).await.unwrap();
    assert_eq!(data.len(), size);
    assert_eq!(&data[..], &payload[..], "10 MiB read-back must match write");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn overwrite_and_read() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "overwrite");

    let attr = vfs.create(1, b"over.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write initial data
    let v1 = b"AAAAAAAAAA"; // 10 bytes
    vfs.write(ino, 0, v1).await.unwrap();

    // Overwrite the middle (offset 3..8)
    let v2 = b"BBBBB";
    vfs.write(ino, 3, v2).await.unwrap();

    // Read full file — expect "AAABBBBBAA"
    let data = vfs.read(ino, 0, 10).await.unwrap();
    assert_eq!(&data[..], b"AAABBBBBAA");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn directory_operations() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "dirs");

    // Create a subdirectory
    let dir_attr = vfs.mkdir(1, b"subdir", 0, 0, 0o755).unwrap();
    let dir_ino = dir_attr.ino;

    // Create files inside
    let f1 = vfs.create(dir_ino, b"a.txt", 0, 0, 0o644).unwrap();
    let f2 = vfs.create(dir_ino, b"b.txt", 0, 0, 0o644).unwrap();

    // Readdir should return both files (plus . and ..)
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

    // Suppress unused variable warnings
    let _ = f2;
}

#[tokio::test]
#[ignore]
async fn rename_file() {
    ensure_bucket().await;
    let tmp = tempfile::tempdir().unwrap();
    let vfs = make_vfs(&tmp, "rename");

    let attr = vfs.create(1, b"old.txt", 0, 0, 0o644).unwrap();
    let ino = attr.ino;

    // Write some data
    vfs.write(ino, 0, b"rename test data").await.unwrap();

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
