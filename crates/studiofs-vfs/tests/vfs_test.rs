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

use studiofs_data::store::{DataStore, DataStoreConfig};
use studiofs_data::object::{create_object, read_object, format_object_key};
use studiofs_meta::{RedbMetaEngine, MetaEngine, FormatInfo, types::BlockRef};
use studiofs_vfs::StudioVfs;

// =============================================================================
// Test Helpers
// =============================================================================

/// Create a test format info.
fn make_format_info(volume_suffix: &str) -> FormatInfo {
    FormatInfo {
        name: format!("vfs-test-{volume_suffix}"),
        uuid: format!("test-uuid-{volume_suffix}"),
        endpoint: "http://127.0.0.1:9000".into(),
        bucket: "studiofs-test".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        region: "us-east-1".into(),
        block_size: 4 * 1024 * 1024, // 4 MiB
        chunk_size: 64 * 1024 * 1024, // 64 MiB (deprecated)
    }
}

/// Create a fresh VFS instance backed by a temporary redb and MinIO.
fn make_vfs(tmp: &tempfile::TempDir, volume_suffix: &str) -> Arc<StudioVfs> {
    let db_path = tmp.path().join("meta.redb");

    let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("failed to open RedbMetaEngine"));
    meta.init_root(0, 0).expect("failed to initialize root inode");

    // Set format info
    let format_info = make_format_info(volume_suffix);
    meta.set_format_info(&format_info).expect("failed to set format info");

    let data_config = DataStoreConfig::from(&format_info);
    let data = Arc::new(DataStore::new(&data_config).expect("failed to create DataStore"));

    let cache_dir = tmp.path().join("cache");
    let cache_dir_str = cache_dir.to_str().unwrap();
    let cache_size = 1024 * 1024 * 1024; // 1 GiB

    StudioVfs::new(meta, data, None, cache_dir_str, cache_size).expect("failed to create StudioVfs")
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
    let payload = b"Hello, StudioFS V2! This is a block-based test.";
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
    let v2 = b"BBBBB";
    vfs.write(ino, 3, v2).await.unwrap();
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

    // Verify blocks were allocated
    let blocks = vfs.meta().get_file_blocks(ino).unwrap();
    assert_eq!(blocks.len(), 3, "Should have 3 blocks for 12 MiB file");

    // Verify block indices
    assert_eq!(blocks[0].0, 0);
    assert_eq!(blocks[1].0, 1);
    assert_eq!(blocks[2].0, 2);

    // Verify each block is compressed
    for (_, block_ref) in &blocks {
        assert!(block_ref.is_compressed(), "Block should be compressed");
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

    // Verify single partial block
    let blocks = vfs.meta().get_file_blocks(ino).unwrap();
    assert_eq!(blocks.len(), 1);
    
    let (_, block_ref) = &blocks[0];
    assert!(block_ref.is_partial(), "Small file should have partial block");
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
    #[allow(deprecated)]
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

    // Verify 3 blocks
    let blocks = vfs.meta().get_file_blocks(ino).unwrap();
    assert_eq!(blocks.len(), 3);

    // Truncate to 5 MiB (should remove 1 block, partial last block)
    let new_size = 5 * 1024 * 1024;
    vfs.setattr(ino, Some(new_size)).unwrap();

    // Verify 2 blocks now
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

    // Get the block ID
    let blocks_before = vfs.meta().get_file_blocks(ino).unwrap();
    assert_eq!(blocks_before.len(), 1);
    let old_block_id = blocks_before[0].1.object_id;

    // Overwrite with new data (should mark old block for deletion)
    vfs.write(ino, 0, b"new data here!!").await.unwrap();
    vfs.flush(ino).await.unwrap();

    // Verify old block is in pending deletions
    let pending = vfs.meta().get_pending_deletions().unwrap();
    assert!(pending.iter().any(|(id, _)| *id == old_block_id), 
            "Old block should be in pending deletions");

    // Verify new block is different
    let blocks_after = vfs.meta().get_file_blocks(ino).unwrap();
    assert_ne!(blocks_after[0].1.object_id, old_block_id);
}

// =============================================================================
// Metadata Tests
// =============================================================================

#[tokio::test]
async fn test_block_ref_flags() {
    // Test BlockRef flags
    let full_block = BlockRef::new_full(123, 4 * 1024 * 1024);
    assert!(full_block.is_compressed());
    assert!(!full_block.is_partial());
    assert_eq!(full_block.object_id, 123);
    assert_eq!(full_block.length, 4 * 1024 * 1024);

    let partial_block = BlockRef::new_partial(456, 1000);
    assert!(partial_block.is_compressed());
    assert!(partial_block.is_partial());
    assert_eq!(partial_block.object_id, 456);
    assert_eq!(partial_block.length, 1000);
}

#[tokio::test]
async fn test_object_format() {
    let original_data = b"Test data for object format v2";
    
    // Create object
    let object = create_object(original_data).unwrap();
    
    // Verify we can read it back
    let decompressed = read_object(&object).unwrap();
    assert_eq!(decompressed.as_slice(), original_data);
}

#[tokio::test]
async fn test_object_key_format() {
    let uuid = "550e8400-e29b-41d4-a716-446655440000";
    let object_id: u64 = 0x1234;
    
    let key = format_object_key(uuid, object_id);
    assert_eq!(key, "550e8400-e29b-41d4-a716-446655440000/blocks/0000/0000000000001234");
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
    
    println!("Write: {:.2} MiB/s ({} bytes in {:?})", write_mbps, size, write_duration);

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
    
    println!("Read: {:.2} MiB/s ({} bytes in {:?})", read_mbps, size, read_duration);
    
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
    vfs.setattr(ino, Some(10)).unwrap();

    // Next getattr should see the new size from dirty_attrs
    let attr5 = vfs.getattr(ino).unwrap();
    assert_eq!(attr5.size, 10);
}