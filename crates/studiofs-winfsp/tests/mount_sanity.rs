//! WinFSP mount sanity tests (Windows-only).
//!
//! This integration test suite exercises `studiofs-winfsp` end-to-end through a
//! real WinFSP mountpoint. The goal is to catch regressions in the WinFSP bridge
//! and the VFS integration that are hard to detect with unit tests alone.
//!
//! ## What these tests cover
//!
//! - Mounting and serving a `StudioVfs` instance via WinFSP.
//! - Basic file I/O via the mounted drive, including sequential write paths.
//! - Smoke checks around write-path observability and backpressure behavior.
//!
//! ## Test environment
//!
//! These tests are gated behind `cfg(windows)` and expect a Windows host with
//! WinFSP available. Some tests may depend on a configured `.env` at the
//! workspace root (or environment variables) for online/storage configuration.
#![cfg(windows)]

#[path = "mount_sanity/metadata_baseline.rs"]
mod metadata_baseline;

use std::net::ToSocketAddrs;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use studiofs_data::store::DataStoreConfig;
use studiofs_meta::UploadState;
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, FormatInfo, InodeAttr, MetaFormat, MetadataConfig, RedbMetaEngine,
};
use studiofs_vfs::{StudioVfs, VfsConfig, VfsConfigRaw};
use studiofs_winfsp::mount_and_serve;

fn workspace_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn workspace_env_path() -> std::path::PathBuf {
    workspace_root().join(".env")
}

fn load_test_env() {
    static ONCE: OnceLock<()> = OnceLock::new();
    let _ = ONCE.get_or_init(|| {
        let env_path = workspace_env_path();
        if env_path.exists() {
            let _ = dotenvy::from_path(&env_path);
        } else {
            let _ = dotenvy::dotenv();
        }
    });
}

/// The drive comes from the process env or the workspace `.env`; loading `.env`
/// here keeps the result independent of which test ran first.
fn test_mount_point() -> String {
    load_test_env();
    std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT")
        .expect("set STUDIOFS_WINFSP_TEST_MOUNT_POINT to an unused drive letter")
}

fn workspace_dotenv_vars() -> &'static std::collections::BTreeMap<String, String> {
    static VARS: OnceLock<std::collections::BTreeMap<String, String>> = OnceLock::new();
    VARS.get_or_init(|| {
        let mut vars = std::collections::BTreeMap::new();
        let env_path = workspace_env_path();
        if let Ok(iter) = dotenvy::from_path_iter(&env_path) {
            for item in iter.flatten() {
                vars.insert(item.0, item.1);
            }
        }
        vars
    })
}

fn workspace_dotenv_var(key: &str) -> Option<String> {
    workspace_dotenv_vars().get(key).cloned()
}

fn test_online_env_var(primary: &str, legacy: &str, default: &str) -> String {
    load_test_env();
    test_online_env_var_without_loading_env(primary, legacy, default)
}

fn test_online_env_var_without_loading_env(primary: &str, legacy: &str, default: &str) -> String {
    std::env::var(primary)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| workspace_dotenv_var(primary))
        .or_else(|| workspace_dotenv_var(legacy))
        .unwrap_or_else(|| default.to_string())
}

fn test_online_endpoint() -> String {
    test_online_env_var(
        "STUDIOFS_TEST_ONLINE_S3_ENDPOINT",
        "STUDIOFS_S3_ENDPOINT",
        "http://127.0.0.1:9000",
    )
}

const DEFAULT_AJA_SEQUENTIAL_CHUNK_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_AJA_SEQUENTIAL_CHUNK_COUNT: usize = 8;
const DEFAULT_AJA_MIN_THROUGHPUT_MIB_PER_SEC: f64 = 64.0;
const AJA_MIN_THROUGHPUT_ENV: &str = "STUDIOFS_WINFSP_AJA_MIN_THROUGHPUT_MIB_PER_SEC";
const LEGACY_AJA_MIN_THROUGHPUT_ENV: &str = "STUDIOFS_WINFSP_WRITE_MIN_THROUGHPUT_MIB_PER_SEC";
const MAX_AJA_SEQUENTIAL_CHUNK_BYTES: usize = 64 * 1024 * 1024;
const MAX_AJA_SEQUENTIAL_CHUNK_COUNT: usize = 128;
const MAX_AJA_SEQUENTIAL_TOTAL_BYTES: usize = 512 * 1024 * 1024;
const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;
const FILE_FLAG_NO_BUFFERING: u32 = 0x2000_0000;
const AJA_CACHE_OFF_OPEN_OPTIONS_FLAGS: u32 = FILE_FLAG_WRITE_THROUGH | FILE_FLAG_NO_BUFFERING;
const AJA_CACHE_OFF_ALIGNMENT_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AjaCacheValidationMode {
    CacheOn,
    CacheOff,
}

impl AjaCacheValidationMode {
    fn label(self) -> &'static str {
        match self {
            Self::CacheOn => "cache-on",
            Self::CacheOff => "cache-off",
        }
    }

    fn file_name(self) -> &'static str {
        match self {
            Self::CacheOn => "aja-sequential-cache-on.bin",
            Self::CacheOff => "aja-sequential-cache-off.bin",
        }
    }

    fn open_options_custom_flags(self) -> u32 {
        match self {
            Self::CacheOn => 0,
            Self::CacheOff => AJA_CACHE_OFF_OPEN_OPTIONS_FLAGS,
        }
    }

    fn min_throughput_env_key(self) -> &'static str {
        match self {
            Self::CacheOn => "STUDIOFS_WINFSP_WRITE_CACHE_ON_MIN_THROUGHPUT_MIB_PER_SEC",
            Self::CacheOff => "STUDIOFS_WINFSP_WRITE_CACHE_OFF_MIN_THROUGHPUT_MIB_PER_SEC",
        }
    }
}

struct AlignedChunk {
    ptr: std::ptr::NonNull<u8>,
    len: usize,
    layout: std::alloc::Layout,
}

impl AlignedChunk {
    fn new(len: usize) -> Result<Self, String> {
        if !len.is_multiple_of(AJA_CACHE_OFF_ALIGNMENT_BYTES) {
            return Err(format!(
                "aligned chunk length {len} must be 4096-byte aligned for unbuffered I/O"
            ));
        }
        let layout = std::alloc::Layout::from_size_align(len, AJA_CACHE_OFF_ALIGNMENT_BYTES)
            .map_err(|err| format!("invalid aligned chunk layout for {len} bytes: {err}"))?;
        // SAFETY: `layout` has non-zero alignment and the returned pointer is
        // checked before it is stored. Drop deallocates with the same layout.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr = std::ptr::NonNull::new(ptr)
            .ok_or_else(|| format!("failed to allocate {len} aligned bytes"))?;
        Ok(Self { ptr, len, layout })
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` was allocated for `len` bytes and remains exclusively
        // borrowed through `&mut self` for the duration of this slice.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedChunk {
    fn drop(&mut self) {
        // SAFETY: `ptr` was allocated with `layout` in `AlignedChunk::new`.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

enum AjaSequentialChunk {
    Cached(Vec<u8>),
    Unbuffered(AlignedChunk),
}

impl AjaSequentialChunk {
    fn new(mode: AjaCacheValidationMode, len: usize) -> Result<Self, String> {
        match mode {
            AjaCacheValidationMode::CacheOn => Ok(Self::Cached(vec![0; len])),
            AjaCacheValidationMode::CacheOff => AlignedChunk::new(len).map(Self::Unbuffered),
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            Self::Cached(chunk) => chunk.as_mut_slice(),
            Self::Unbuffered(chunk) => chunk.as_mut_slice(),
        }
    }
}

fn write_aja_chunk(
    writer: &mut impl std::io::Write,
    mode: AjaCacheValidationMode,
    chunk: &[u8],
) -> std::io::Result<()> {
    match mode {
        AjaCacheValidationMode::CacheOn => writer.write_all(chunk),
        AjaCacheValidationMode::CacheOff => {
            let written = writer.write(chunk)?;
            if written == chunk.len() {
                Ok(())
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    format!(
                        "partial unbuffered AJA cache-off write: wrote {written} of {} bytes",
                        chunk.len()
                    ),
                ))
            }
        }
    }
}

fn aja_sequential_validation_modes(
    value: Option<&str>,
) -> Result<Vec<AjaCacheValidationMode>, String> {
    let Some(value) = value else {
        return Ok(vec![
            AjaCacheValidationMode::CacheOn,
            AjaCacheValidationMode::CacheOff,
        ]);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("both") {
        return Ok(vec![
            AjaCacheValidationMode::CacheOn,
            AjaCacheValidationMode::CacheOff,
        ]);
    }

    let mut modes = Vec::new();
    for raw_mode in trimmed.split(',') {
        let mode = match raw_mode.trim().to_ascii_lowercase().as_str() {
            "cache-on" | "on" | "cached" => AjaCacheValidationMode::CacheOn,
            "cache-off" | "off" | "uncached" | "write-through" => AjaCacheValidationMode::CacheOff,
            unsupported => {
                return Err(format!(
                    "STUDIOFS_WINFSP_WRITE_CACHE_MODES contains unsupported mode '{unsupported}'; use cache-on, cache-off, or both"
                ));
            }
        };
        if !modes.contains(&mode) {
            modes.push(mode);
        }
    }
    if modes.is_empty() {
        return Err("STUDIOFS_WINFSP_WRITE_CACHE_MODES did not select any modes".to_string());
    }
    Ok(modes)
}

fn aja_sequential_validation_modes_from_env() -> Result<Vec<AjaCacheValidationMode>, String> {
    load_test_env();
    aja_sequential_validation_modes(
        std::env::var("STUDIOFS_WINFSP_WRITE_CACHE_MODES")
            .ok()
            .as_deref(),
    )
}

fn test_mount_usize_env_var(key: &str, default: usize) -> usize {
    load_test_env();
    std::env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn test_mount_f64_env_var(key: &str, default: f64) -> Result<f64, String> {
    test_mount_f64_env_var_any(&[key], default)
}

fn test_mount_f64_env_var_any(keys: &[&str], default: f64) -> Result<f64, String> {
    load_test_env();
    for key in keys {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                continue;
            }
            let parsed = trimmed.parse::<f64>().map_err(|err| {
                format!("{key} must be a positive MiB/s value, got '{trimmed}': {err}")
            })?;
            if !parsed.is_finite() || parsed <= 0.0 {
                return Err(format!(
                    "{key} must be a positive MiB/s value, got '{trimmed}'"
                ));
            }
            return Ok(parsed);
        }
    }
    Ok(default)
}

fn aja_sequential_chunk_bytes() -> usize {
    test_mount_usize_env_var(
        "STUDIOFS_WINFSP_WRITE_CHUNK_BYTES",
        DEFAULT_AJA_SEQUENTIAL_CHUNK_BYTES,
    )
}

fn aja_sequential_chunk_count() -> usize {
    test_mount_usize_env_var(
        "STUDIOFS_WINFSP_WRITE_CHUNK_COUNT",
        DEFAULT_AJA_SEQUENTIAL_CHUNK_COUNT,
    )
}

fn aja_sequential_min_throughput_mib_per_sec() -> Result<f64, String> {
    test_mount_f64_env_var_any(
        &[AJA_MIN_THROUGHPUT_ENV, LEGACY_AJA_MIN_THROUGHPUT_ENV],
        DEFAULT_AJA_MIN_THROUGHPUT_MIB_PER_SEC,
    )
}

fn aja_sequential_min_throughput_for_mode(mode: AjaCacheValidationMode) -> Result<f64, String> {
    match std::env::var(mode.min_throughput_env_key()) {
        Ok(value) if !value.trim().is_empty() => test_mount_f64_env_var(
            mode.min_throughput_env_key(),
            DEFAULT_AJA_MIN_THROUGHPUT_MIB_PER_SEC,
        ),
        _ => aja_sequential_min_throughput_mib_per_sec(),
    }
}

fn validate_aja_sequential_workload(
    chunk_bytes: usize,
    chunk_count: usize,
) -> Result<(usize, usize), String> {
    if chunk_bytes == 0 {
        return Err("STUDIOFS_WINFSP_WRITE_CHUNK_BYTES must be greater than 0".to_string());
    }
    if chunk_bytes > MAX_AJA_SEQUENTIAL_CHUNK_BYTES {
        return Err(format!(
            "STUDIOFS_WINFSP_WRITE_CHUNK_BYTES={} exceeds the supported max {}; reduce the chunk size for the mounted sequential writer smoke test",
            chunk_bytes, MAX_AJA_SEQUENTIAL_CHUNK_BYTES
        ));
    }
    if chunk_count == 0 {
        return Err("STUDIOFS_WINFSP_WRITE_CHUNK_COUNT must be greater than 0".to_string());
    }
    if chunk_count > MAX_AJA_SEQUENTIAL_CHUNK_COUNT {
        return Err(format!(
            "STUDIOFS_WINFSP_WRITE_CHUNK_COUNT={} exceeds the supported max {}; reduce the chunk count for the mounted sequential writer smoke test",
            chunk_count, MAX_AJA_SEQUENTIAL_CHUNK_COUNT
        ));
    }
    let total_bytes = chunk_bytes.checked_mul(chunk_count).ok_or_else(|| {
        "AJA-style mounted smoke workload overflowed chunk_bytes * chunk_count".to_string()
    })?;
    if total_bytes > MAX_AJA_SEQUENTIAL_TOTAL_BYTES {
        return Err(format!(
            "mounted sequential writer smoke workload would write {} bytes, exceeding the supported max {}; lower STUDIOFS_WINFSP_WRITE_CHUNK_BYTES or STUDIOFS_WINFSP_WRITE_CHUNK_COUNT",
            total_bytes, MAX_AJA_SEQUENTIAL_TOTAL_BYTES
        ));
    }

    Ok((chunk_bytes, chunk_count))
}

fn validate_aja_sequential_workload_for_modes(
    chunk_bytes: usize,
    chunk_count: usize,
    modes: &[AjaCacheValidationMode],
) -> Result<(usize, usize), String> {
    let workload = validate_aja_sequential_workload(chunk_bytes, chunk_count)?;
    if modes.contains(&AjaCacheValidationMode::CacheOff)
        && !chunk_bytes.is_multiple_of(AJA_CACHE_OFF_ALIGNMENT_BYTES)
    {
        return Err(format!(
            "STUDIOFS_WINFSP_WRITE_CHUNK_BYTES={chunk_bytes} must be 4096-byte aligned when cache-off validation is selected"
        ));
    }
    Ok(workload)
}

#[test]
fn aja_cache_validation_modes_default_to_cache_on_and_cache_off() {
    let modes = aja_sequential_validation_modes(None).expect("default validation modes");
    assert_eq!(
        modes,
        vec![
            AjaCacheValidationMode::CacheOn,
            AjaCacheValidationMode::CacheOff
        ],
        "default AJA validation should compare OS-cache-on and stricter cache-off-style writes"
    );
}

#[test]
fn aja_cache_off_validation_mode_uses_stricter_open_flags() {
    assert_eq!(
        AjaCacheValidationMode::CacheOn.open_options_custom_flags(),
        0
    );
    assert_ne!(
        AjaCacheValidationMode::CacheOff.open_options_custom_flags(),
        0
    );
    assert!(
        AjaCacheValidationMode::CacheOff.open_options_custom_flags()
            & AJA_CACHE_OFF_OPEN_OPTIONS_FLAGS
            == AJA_CACHE_OFF_OPEN_OPTIONS_FLAGS,
        "cache-off validation should request the Windows no-intermediate-buffering/write-through path"
    );
}

#[test]
fn aja_cache_off_validation_rejects_unaligned_chunk_bytes() {
    let err =
        validate_aja_sequential_workload_for_modes(4097, 1, &[AjaCacheValidationMode::CacheOff])
            .expect_err("cache-off validation should reject unaligned chunks");

    assert!(err.contains("4096-byte aligned"), "unexpected error: {err}");
}

#[test]
fn aligned_chunk_rejects_unaligned_lengths() {
    let Err(err) = AlignedChunk::new(4097) else {
        panic!("unaligned chunks should be rejected");
    };

    assert!(err.contains("4096-byte aligned"), "unexpected error: {err}");
}

#[test]
fn aja_cache_on_chunk_allows_unaligned_lengths() {
    let mut chunk = AjaSequentialChunk::new(AjaCacheValidationMode::CacheOn, 4097)
        .expect("cache-on chunk allocation should not require unbuffered I/O alignment");

    assert_eq!(chunk.as_mut_slice().len(), 4097);
}

#[test]
fn aja_cache_off_write_rejects_partial_unbuffered_write() {
    struct PartialWriter {
        max_bytes: usize,
    }

    impl std::io::Write for PartialWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len().min(self.max_bytes))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let chunk = vec![0; AJA_CACHE_OFF_ALIGNMENT_BYTES];
    let err = write_aja_chunk(
        &mut PartialWriter { max_bytes: 2048 },
        AjaCacheValidationMode::CacheOff,
        &chunk,
    )
    .expect_err("cache-off unbuffered writes should reject partial chunks");

    assert!(
        err.to_string().contains("partial"),
        "unexpected error: {err}"
    );
}

fn validate_aja_sequential_throughput(
    total_bytes: usize,
    elapsed: Duration,
    min_mib_per_sec: f64,
) -> Result<f64, String> {
    if total_bytes == 0 {
        return Err(
            "AJA-style mounted sequential writer total_bytes must be greater than 0".to_string(),
        );
    }
    if !min_mib_per_sec.is_finite() || min_mib_per_sec <= 0.0 {
        return Err(format!(
            "AJA-style mounted sequential writer minimum throughput must be a positive MiB/s value, got {min_mib_per_sec}"
        ));
    }
    let elapsed_secs = elapsed.as_secs_f64();
    if elapsed_secs <= 0.0 {
        return Err(
            "AJA-style mounted sequential writer elapsed time must be greater than 0".to_string(),
        );
    }

    let throughput_mib_per_sec = (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed_secs;
    if throughput_mib_per_sec < min_mib_per_sec {
        return Err(format!(
            "AJA-style mounted sequential writer throughput fell below the required floor: measured {:.2} MiB/s for {} bytes in {:.3} s, expected at least {:.2} MiB/s",
            throughput_mib_per_sec, total_bytes, elapsed_secs, min_mib_per_sec,
        ));
    }

    Ok(throughput_mib_per_sec)
}

fn endpoint_authority(endpoint: &str) -> Option<String> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return None;
    }

    let (rest, default_port) = if let Some(rest) = trimmed.strip_prefix("http://") {
        (rest, 80)
    } else if let Some(rest) = trimmed.strip_prefix("https://") {
        (rest, 443)
    } else {
        (trimmed, 80)
    };

    let authority = rest.split('/').next()?.trim();
    if authority.is_empty() {
        return None;
    }

    if authority.starts_with('[') {
        if authority.contains("]:") {
            return Some(authority.to_string());
        }
        return Some(format!("{authority}:{default_port}"));
    }

    if authority.contains(':') {
        return Some(authority.to_string());
    }

    Some(format!("{authority}:{default_port}"))
}

fn online_upload_backend_status() -> &'static (String, bool) {
    static STATUS: OnceLock<(String, bool)> = OnceLock::new();
    STATUS.get_or_init(|| {
        let endpoint = test_online_endpoint();
        let available = endpoint_authority(&endpoint)
            .and_then(|authority| {
                authority
                    .to_socket_addrs()
                    .ok()
                    .map(|addrs| addrs.take(4).collect::<Vec<_>>())
            })
            .is_some_and(|addrs| {
                addrs.iter().any(|addr| {
                    std::net::TcpStream::connect_timeout(addr, Duration::from_secs(1)).is_ok()
                })
            });
        (endpoint, available)
    })
}

fn require_online_upload_backend(test_name: &str) -> bool {
    let (endpoint, available) = online_upload_backend_status();
    if *available {
        return true;
    }

    eprintln!(
        "Skipping {test_name}: post-upload WinFSP smoke tests require a reachable S3 endpoint; resolved endpoint '{endpoint}' was not reachable."
    );
    false
}

fn test_format_info(volume_suffix: &str) -> FormatInfo {
    load_test_env();
    test_format_info_without_loading_env(volume_suffix)
}

fn test_format_info_without_loading_env(volume_suffix: &str) -> FormatInfo {
    let volume_name = format!("winfsp-mount-test-{volume_suffix}-{}", std::process::id());
    FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: volume_name.clone(),
        uuid: format!("winfsp-mount-test-uuid-{volume_suffix}"),
        endpoint: test_online_env_var_without_loading_env(
            "STUDIOFS_TEST_ONLINE_S3_ENDPOINT",
            "STUDIOFS_S3_ENDPOINT",
            "http://127.0.0.1:9000",
        ),
        bucket: test_online_env_var_without_loading_env(
            "STUDIOFS_TEST_ONLINE_S3_BUCKET",
            "STUDIOFS_S3_BUCKET",
            "studiofs-test",
        ),
        access_key: test_online_env_var_without_loading_env(
            "STUDIOFS_TEST_ONLINE_S3_ACCESS_KEY",
            "STUDIOFS_S3_ACCESS_KEY",
            "studiofs",
        ),
        secret_key: test_online_env_var_without_loading_env(
            "STUDIOFS_TEST_ONLINE_S3_SECRET_KEY",
            "STUDIOFS_S3_SECRET_KEY",
            "studiofs123",
        ),
        region: "us-east-1".into(),
        data_prefix: format!("{volume_name}/objects/"),
        metadata: MetadataConfig::default(),
        encryption: studiofs_meta::EncryptionConfig::new(
            [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
        ),
        block_size: 4 * 1024 * 1024,
    }
}

fn make_test_vfs(
    tmp: &tempfile::TempDir,
    volume_suffix: &str,
    rt: &tokio::runtime::Runtime,
) -> Arc<StudioVfs> {
    let format_info = test_format_info(volume_suffix);
    make_test_vfs_with_format_info(tmp, format_info, rt)
}

fn make_test_vfs_with_format_info(
    tmp: &tempfile::TempDir,
    format_info: FormatInfo,
    rt: &tokio::runtime::Runtime,
) -> Arc<StudioVfs> {
    let db_path = tmp.path().join("meta.redb");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
    meta.init_root(0, 0).expect("init root");

    meta.set_format_info(&format_info).expect("set format info");
    let data_config = DataStoreConfig::from(&format_info);

    let cache_dir = tmp.path().join("cache");
    let _guard = rt.enter();
    let vfs = StudioVfs::new(
        meta,
        &data_config,
        cache_dir.to_str().expect("cache dir str"),
        64 * 1024 * 1024,
        32 * 1024 * 1024,
        VfsConfig::default()
            .try_into()
            .expect("default VFS config should validate"),
    )
    .expect("create vfs");
    vfs.start().expect("start vfs background workers");
    vfs
}

fn make_trash_enabled_test_vfs_with_format_info(
    tmp: &tempfile::TempDir,
    volume_suffix: &str,
    rt: &tokio::runtime::Runtime,
) -> (Arc<StudioVfs>, FormatInfo) {
    use studiofs_meta::trash::MetaVolumeSettings;
    let format_info = test_format_info(volume_suffix);
    let db_path = tmp.path().join("meta.redb");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
    meta.init_root(0, 0).expect("init root");
    meta.set_format_info(&format_info).expect("set format info");
    meta.compare_and_set_trash_days(0, 7)
        .expect("enable trash")
        .expect("fresh revision");
    let data_config = DataStoreConfig::from(&format_info);
    let cache_dir = tmp.path().join("cache");
    let _guard = rt.enter();
    let vfs = StudioVfs::new(
        meta,
        &data_config,
        cache_dir.to_str().expect("cache dir str"),
        64 * 1024 * 1024,
        32 * 1024 * 1024,
        VfsConfig::default()
            .try_into()
            .expect("default VFS config should validate"),
    )
    .expect("create vfs");
    vfs.start().expect("start vfs background workers");
    (vfs, format_info)
}

/// Same as `make_test_vfs`, with a positive trash period so Move to Trash retains.
fn make_trash_enabled_test_vfs(
    tmp: &tempfile::TempDir,
    volume_suffix: &str,
    rt: &tokio::runtime::Runtime,
) -> Arc<StudioVfs> {
    make_trash_enabled_test_vfs_with_format_info(tmp, volume_suffix, rt).0
}

fn signal_named_event(name: &str) {
    use windows::Win32::Foundation::CloseHandle;
    use windows::Win32::System::Threading::{EVENT_MODIFY_STATE, OpenEventW, SetEvent};
    use windows::core::HSTRING;

    let wide = HSTRING::from(name);
    // SAFETY: `wide` is a valid event name buffer; this is the standard
    // `OpenEventW` query path and failure is handled explicitly.
    let event = unsafe { OpenEventW(EVENT_MODIFY_STATE, false, &wide) };
    let Ok(event) = event else { return };
    // SAFETY: `event` is valid on this branch and can be signaled by this process.
    let _ = unsafe { SetEvent(event) };
    // SAFETY: Ownership of `event` stays with this function; closing it once here
    // avoids handle leaks.
    let _ = unsafe { CloseHandle(event) };
}

fn drive_root(mount_point: &str) -> String {
    if mount_point.ends_with('\\') || mount_point.ends_with('/') {
        mount_point.to_string()
    } else {
        format!("{mount_point}\\")
    }
}

fn wait_for_drive_root(drive_root: &str, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if std::fs::metadata(drive_root).is_ok() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "WinFSP mount at {drive_root} did not become available within {} s",
            timeout.as_secs()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn with_winfsp_mount<R, F>(
    vfs: Arc<StudioVfs>,
    rt: Arc<tokio::runtime::Runtime>,
    mount_point: String,
    event_name: String,
    body: F,
) -> R
where
    F: FnOnce(&str) -> R,
{
    let vfs_for_shutdown = vfs.clone();
    let rt_for_shutdown = rt.clone();
    let event_for_thread = event_name.clone();
    let mp_for_thread = mount_point.clone();
    let handle = std::thread::spawn(move || {
        mount_and_serve(studiofs_winfsp::WinfspMountRequest {
            vfs,
            runtime_handle: rt.handle().clone(),
            mount_point: mp_for_thread,
            volume_label: "StudioFS-Test".to_string(),
            read_only: false,
            shutdown_event_name: Some(event_for_thread),
            mount_ready_callback: None,
        })
        .map_err(|e| e.to_string())
    });

    let drive_root = drive_root(&mount_point);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        wait_for_drive_root(&drive_root, Duration::from_secs(10));
        body(&drive_root)
    }));

    signal_named_event(&event_name);
    let join_result = handle.join();
    rt_for_shutdown
        .block_on(vfs_for_shutdown.shutdown(Duration::from_secs(1)))
        .expect("VFS shutdown should succeed");
    rt_for_shutdown
        .block_on(vfs_for_shutdown.shutdown(Duration::from_secs(1)))
        .expect("repeated VFS shutdown should succeed");

    match result {
        Ok(value) => {
            let mount_result = join_result.expect("mount thread panicked");
            mount_result.expect("WinFSP mount/unmount should complete");
            value
        }
        Err(payload) => {
            if let Ok(Err(error)) = join_result {
                eprintln!("WinFSP mount thread returned during cleanup: {error}");
            }
            std::panic::resume_unwind(payload)
        }
    }
}

fn wait_for_lookup(vfs: &StudioVfs, name: &[u8], label: &str, timeout: Duration) -> InodeAttr {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match vfs.lookup(1, name) {
            Ok(attr) => return attr,
            Err(error) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "lookup {label}: {error:?}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

fn wait_for_pending_upload_observable(vfs: &StudioVfs, ino: u64, label: &str, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let current_attr = vfs.getattr(ino).expect(label);
        if !vfs.pending_uploads_for_inode(ino).is_empty()
            || current_attr.upload_state != UploadState::Ready
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{label} reached Ready before pending upload state was observable"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_upload_ready(vfs: &StudioVfs, ino: u64, label: &str, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let current_attr = vfs.getattr(ino).expect(label);
        if vfs.pending_uploads_for_inode(ino).is_empty()
            && current_attr.upload_state == UploadState::Ready
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{label} did not reach published Ready state within {} s",
            timeout.as_secs()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_unmount_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP mount smoke test: set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "mount_sanity", &rt);
    let event_name = format!("Local\\StudioFsMountSmoke-{}", std::process::id());
    with_winfsp_mount(vfs, rt, mount_point, event_name, |_| {});
}

/// Shell recycle oracle: prints volume signals, recycles `$env:F`, and counts `$I`
/// records whose decoded original path is exactly that file (removing the pair if asked).
const RECYCLE_PROBE_PS1: &str = r#"
$ErrorActionPreference = 'Stop'
Add-Type -TypeDefinition @"
using System; using System.Runtime.InteropServices; using System.Text;
public static class Vd {
 [DllImport("kernel32.dll", CharSet=CharSet.Unicode)] public static extern uint GetDriveTypeW(string r);
 [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)] public static extern bool GetVolumeInformationW(string r, StringBuilder n, int ns, out uint ser, out uint mcl, out uint flags, StringBuilder fs, int fss);
 [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)] public static extern bool GetVolumeNameForVolumeMountPointW(string mp, StringBuilder v, int n);
 public struct Rbi { public int cbSize; public long i64Size; public long i64NumItems; }
 [DllImport("shell32.dll", CharSet=CharSet.Unicode)] public static extern int SHQueryRecycleBinW(string r, ref Rbi q);
}
"@
$root = [IO.Path]::GetPathRoot($env:F)
$n = New-Object Text.StringBuilder 261; $fs = New-Object Text.StringBuilder 261; $ser = 0; $mcl = 0; $fl = 0
$null = [Vd]::GetVolumeInformationW($root, $n, 261, [ref]$ser, [ref]$mcl, [ref]$fl, $fs, 261)
$g = New-Object Text.StringBuilder 100; $gok = [Vd]::GetVolumeNameForVolumeMountPointW($root, $g, 100)
$q = New-Object Vd+Rbi; $q.cbSize = [Runtime.InteropServices.Marshal]::SizeOf($q)
$hr = [Vd]::SHQueryRecycleBinW($root, [ref]$q)
'drive_type={0} fs={1} fs_flags=0x{2:X8} volume_guid={3} shquery_hr=0x{4:X8}' -f [Vd]::GetDriveTypeW($root), $fs, $fl, ($gok -and $g.Length -gt 0), $hr
$top = Join-Path $root '$Recycle.Bin'
# Shell uses a per-SID folder on ACL volumes and the shared top folder otherwise (FAT-style).
$bins = @((Join-Path $top ([Security.Principal.WindowsIdentity]::GetCurrent()).User.Value), $top)
Add-Type -AssemblyName Microsoft.VisualBasic
[Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile($env:F, 'OnlyErrorDialogs', 'SendToRecycleBin', 'ThrowException')
$hits = @($bins | ForEach-Object { Get-ChildItem -LiteralPath $_ -Force -File -Filter '$I*' -ErrorAction SilentlyContinue } | Where-Object {
  $b = [IO.File]::ReadAllBytes($_.FullName)
  $b.Length -gt 28 -and [BitConverter]::ToInt64($b, 0) -eq 2 -and [Text.Encoding]::Unicode.GetString($b, 28, $b.Length - 28).TrimEnd([char]0) -eq $env:F -and
    (Test-Path -LiteralPath (Join-Path $_.DirectoryName ('$R' + $_.Name.Substring(2)))) })
$payloads = @($hits | ForEach-Object { Join-Path $_.DirectoryName ('$R' + $_.Name.Substring(2)) })
$listed = @((New-Object -ComObject Shell.Application).NameSpace(10).Items() | Where-Object { $payloads -contains $_.Path }).Count
'top_listing={0}' -f ((Get-ChildItem -LiteralPath $top -Force -ErrorAction SilentlyContinue | ForEach-Object { $_.Name }) -join ',')
'source_exists={0} recycled_metadata={1} shell_listed={2} layout={3}' -f (Test-Path -LiteralPath $env:F), $hits.Count, $listed, (@($hits | ForEach-Object { if ($_.DirectoryName -eq $top) { 'shared' } else { 'per-sid' } }) -join ',')
if ($env:CLEAN -eq '1') { foreach ($i in $hits) { Remove-Item -LiteralPath $i.FullName, (Join-Path $i.DirectoryName ('$R' + $i.Name.Substring(2))) -Force } }
"#;

/// Runs [`RECYCLE_PROBE_PS1`] on one self-created empty file with a 20 s bound.
fn shell_recycle_probe(script: &std::path::Path, file: &std::path::Path, clean: bool) -> String {
    std::fs::File::create(file).expect("create empty recycle probe file");
    run_shell_probe_ps1(script, file, clean)
}

fn run_shell_probe_ps1(script: &std::path::Path, target: &std::path::Path, clean: bool) -> String {
    use std::process::{Command, Stdio};
    // Per-test step log (written only by the restore script): names the Shell call that stalled.
    let steps_path = script.with_extension("steps.txt");
    let mut child = Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
        ])
        .arg(script)
        .env("F", target)
        .env("CLEAN", if clean { "1" } else { "0" })
        .env("STUDIOFS_PROBE_STEPS", &steps_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start Shell recycle probe");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while child.try_wait().expect("query Shell probe").is_none() {
        if std::time::Instant::now() >= deadline {
            child.kill().expect("stop timed-out Shell probe");
            child.wait().expect("join Shell probe");
            let steps = std::fs::read_to_string(&steps_path)
                .unwrap_or_else(|_| "<no step log>".to_string());
            panic!("Shell recycle probe did not finish within 20 seconds; step log:\n{steps}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let output = child
        .wait_with_output()
        .expect("collect Shell probe output");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    eprintln!(
        "shell recycle probe {}: status={} stdout={stdout} stderr={}",
        target.display(),
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "Shell recycle probe failed");
    stdout
}

/// Native Windows trash gate: Shell recycling of an empty file on a fresh WinFSP volume
/// must produce a `$I` record for exactly that path, as it does on the NTFS control.
/// Restore, Clear and Explorer UI need separate proof.
/// Restore and Clear through Shell on owned data. Restore uses the namespace-10
/// `undelete` verb; Clear removes only this run's own `$I`/`$R` pair.
const RECYCLE_RESTORE_PS1: &str = r#"
$ErrorActionPreference = "Stop"
$log = $env:STUDIOFS_PROBE_STEPS
function Step([string]$s) { Add-Content -LiteralPath $log -Value ((Get-Date).ToString("HH:mm:ss.fff") + " " + $s) }
Remove-Item -LiteralPath $log -ErrorAction SilentlyContinue
Add-Type -AssemblyName Microsoft.VisualBasic
$root = [IO.Path]::GetPathRoot($env:F)
$top = Join-Path $root ('$Recycle.Bin')
Step "start"
function Pair-For([string]$path) {
  @(Get-ChildItem -LiteralPath $top -Force -File -Filter '$I*' -ErrorAction SilentlyContinue | Where-Object {
    $b = [IO.File]::ReadAllBytes($_.FullName)
    $b.Length -gt 28 -and [BitConverter]::ToInt64($b, 0) -eq 2 -and [Text.Encoding]::Unicode.GetString($b, 28, $b.Length - 28).TrimEnd([char]0) -eq $path })
}
[IO.File]::WriteAllText($env:F, 'restore-me')
Step "written"
[Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile($env:F, 'OnlyErrorDialogs', 'SendToRecycleBin', 'ThrowException')
$hit = Pair-For $env:F
Step "pair_found=$($hit.Count)"
$payload = Join-Path $top ('$R' + $hit[0].Name.Substring(2))
'view_bytes_ok={0}' -f ([Text.Encoding]::UTF8.GetString([IO.File]::ReadAllBytes($payload)) -eq 'restore-me')
$ns = (New-Object -ComObject Shell.Application).NameSpace(10)
Step "shell_namespace"
$item = $ns.Items() | Where-Object { $_.Path -eq $payload } | Select-Object -First 1
Step "invoke_undelete item_found=$($null -ne $item)"
$item.InvokeVerb('undelete')
Step "invoked"
$deadline = (Get-Date).AddSeconds(15)
while (-not (Test-Path -LiteralPath $env:F) -and (Get-Date) -lt $deadline) { Start-Sleep -Milliseconds 100 }
$restored = Test-Path -LiteralPath $env:F
Step "waited restored=$restored"
$content_ok = $restored -and ([IO.File]::ReadAllText($env:F) -eq 'restore-me')
'restored_exists={0} content_ok={1} pairs_after_restore={2}' -f $restored, $content_ok, @(Pair-For $env:F).Count
Step "checked content_ok=$content_ok"
[Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile($env:F, 'OnlyErrorDialogs', 'SendToRecycleBin', 'ThrowException')
Step "recycled_again"
$own = Pair-For $env:F
foreach ($i in $own) { Remove-Item -LiteralPath $i.FullName, (Join-Path $i.DirectoryName ('$R' + $i.Name.Substring(2))) -Force -ErrorAction SilentlyContinue }
'cleared_pairs={0}' -f @(Pair-For $env:F).Count
Step "cleared"
"#;

/// Installs a file-backed callback trace when `STUDIOFS_WINFSP_TRACE_LOG` names a
/// path. `STUDIOFS_WINFSP_TRACE_FILTER` narrows the levels (for example `=error` for a
/// light trace that barely perturbs timing). Diagnostics only: the default probe run
/// installs nothing.
fn install_probe_trace_log() {
    if let Some(path) = std::env::var_os("STUDIOFS_WINFSP_TRACE_LOG") {
        install_trace_log_at(std::path::Path::new(&path));
    }
}

type ProbeLayer = Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync>;

/// Native callbacks run on WinFSP dispatcher threads, so probes need the global
/// subscriber. It can be set once per process; tests sharing this binary swap the
/// active layer instead, so an earlier test's subscriber cannot swallow later traces.
fn set_probe_tracing_layer(layer: ProbeLayer) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::reload;
    static HANDLE: OnceLock<reload::Handle<Option<ProbeLayer>, tracing_subscriber::Registry>> =
        OnceLock::new();
    let handle = HANDLE.get_or_init(|| {
        let (router, handle) = reload::Layer::new(None);
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(router))
            .expect("install WinFSP probe tracing router");
        handle
    });
    handle
        .reload(Some(layer))
        .expect("swap WinFSP probe tracing layer");
}

fn install_trace_log_at(path: &std::path::Path) {
    use std::sync::Mutex;
    use tracing_subscriber::Layer;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create WinFSP probe trace directory");
    }
    let filter = std::env::var("STUDIOFS_WINFSP_TRACE_FILTER").unwrap_or_else(|_| {
        "studiofs_winfsp=debug,studiofs_vfs=debug,studiofs_winfsp::metadata_baseline=trace"
            .to_string()
    });
    let file = std::fs::File::create(path).expect("create WinFSP probe trace log");
    let fmt = tracing_subscriber::fmt::layer()
        .with_writer(Mutex::new(file))
        .with_ansi(false)
        .with_thread_ids(true);
    set_probe_tracing_layer(Box::new(
        tracing_subscriber::EnvFilter::new(filter).and_then(fmt),
    ));
}

/// Ignored Shell round trip on an owned mount with retention enabled.
#[test]
#[ignore = "requires WinFSP runtime, an explicitly selected unused drive, and an owned test volume"]
fn winfsp_native_trash_restore_and_clear_shell_probe() {
    use windows::Win32::Storage::FileSystem::GetLogicalDrives;
    install_probe_trace_log();

    let mount_point = test_mount_point();
    let bytes = mount_point.as_bytes();
    assert!(bytes.len() == 2 && bytes[0].is_ascii_uppercase() && bytes[1] == b':');
    // SAFETY: GetLogicalDrives takes no pointers and only queries drive occupancy.
    let drives = unsafe { GetLogicalDrives() };
    assert_eq!(drives & (1 << (bytes[0] - b'A')), 0, "drive is occupied");
    let tmp = studiofs_test_support::workspace_target_tempdir("trash-native-restore-");
    let script = tmp.path().join("recycle-restore.ps1");
    std::fs::write(&script, RECYCLE_RESTORE_PS1).expect("write Shell restore probe");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_trash_enabled_test_vfs(&tmp, "trash-native-restore", &rt);
    let observer = vfs.clone();
    let event = format!(r"Local\StudioFsNativeRestoreProbe-{}", std::process::id());
    with_winfsp_mount(vfs, rt, mount_point, event, |root| {
        let file = std::path::Path::new(root).join("shell-native-restore.txt");
        let out = shell_recycle_probe(&script, &file, false);
        assert!(
            out.contains("view_bytes_ok=True"),
            "the $R view must read the retained bytes: {out}"
        );
        assert!(
            out.contains("restored_exists=True content_ok=True pairs_after_restore=0"),
            "restore output: {out}"
        );
        assert!(out.contains("cleared_pairs=0"), "clear output: {out}");
        use studiofs_meta::trash::{RetainedEntryState, RetainedPurgeReason};
        let meta = observer.meta();
        let store_dir = meta
            .lookup(1, b".studiofs-retained")
            .expect("private retained store directory");
        let store = meta.retained_store().expect("retained store");
        let states: Vec<_> = meta
            .readdir(store_dir.ino)
            .expect("list retained store")
            .into_iter()
            .filter_map(|child| {
                std::str::from_utf8(&child.name)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .map(|receipt| store.retained_entry(receipt).unwrap().unwrap().state)
            .collect();
        assert_eq!(states.len(), 2, "restore + clear receipts: {states:?}");
        assert!(
            states.iter().any(|s| matches!(
                s,
                RetainedEntryState::Restored { parent: 1, name } if name == b"shell-native-restore.txt"
            )),
            "expected Restored receipt: {states:?}"
        );
        assert!(
            states.contains(&RetainedEntryState::PurgeRequested {
                reason: RetainedPurgeReason::User,
            }),
            "expected PurgeRequested(User) receipt after clear: {states:?}"
        );
    });
}

const RECYCLE_DIR_RESTORE_PS1: &str = r#"
$ErrorActionPreference = "Stop"
$log = $env:STUDIOFS_PROBE_STEPS
function Step([string]$s) { Add-Content -LiteralPath $log -Value ((Get-Date).ToString("HH:mm:ss.fff") + " " + $s) }
Remove-Item -LiteralPath $log -ErrorAction SilentlyContinue
Add-Type -AssemblyName Microsoft.VisualBasic
$root = [IO.Path]::GetPathRoot($env:F)
$top = Join-Path $root ('$Recycle.Bin')
Step "start"
function Pair-For([string]$path) {
  @(Get-ChildItem -LiteralPath $top -Force -File -Filter '$I*' -ErrorAction SilentlyContinue | Where-Object {
    $b = [IO.File]::ReadAllBytes($_.FullName)
    $b.Length -gt 28 -and [BitConverter]::ToInt64($b, 0) -eq 2 -and [Text.Encoding]::Unicode.GetString($b, 28, $b.Length - 28).TrimEnd([char]0) -eq $path })
}
$nested = Join-Path $env:F 'nested'
New-Item -ItemType Directory -Path $nested -Force | Out-Null
[IO.File]::WriteAllText((Join-Path $env:F 'a.txt'), 'alpha-123')
[IO.File]::WriteAllText((Join-Path $nested 'b.txt'), 'beta-4567')
Step "written"
[Microsoft.VisualBasic.FileIO.FileSystem]::DeleteDirectory($env:F, 'OnlyErrorDialogs', 'SendToRecycleBin', 'ThrowException')
$hit = Pair-For $env:F
Step "pair_found=$($hit.Count)"
$iBytes = [IO.File]::ReadAllBytes($hit[0].FullName)
$iSize = [BitConverter]::ToInt64($iBytes, 8)
$payload = Join-Path $top ('$R' + $hit[0].Name.Substring(2))
$rItem = Get-Item -LiteralPath $payload -Force
$rIsDir = $rItem.PSIsContainer
$rChildren = @(Get-ChildItem -LiteralPath $payload -Recurse -Force -ErrorAction SilentlyContinue | ForEach-Object { $_.FullName.Substring($payload.Length) })
$rABytesOk = (Test-Path -LiteralPath (Join-Path $payload 'a.txt')) -and ([IO.File]::ReadAllText((Join-Path $payload 'a.txt')) -eq 'alpha-123')
$rBBytesOk = (Test-Path -LiteralPath (Join-Path $payload 'nested\b.txt')) -and ([IO.File]::ReadAllText((Join-Path $payload 'nested\b.txt')) -eq 'beta-4567')
'i_size={0} r_isdir={1} r_children={2} r_a_ok={3} r_b_ok={4}' -f $iSize, $rIsDir, ($rChildren -join ';'), $rABytesOk, $rBBytesOk
$ns = (New-Object -ComObject Shell.Application).NameSpace(10)
Step "shell_namespace"
$item = $ns.Items() | Where-Object { $_.Path -eq $payload } | Select-Object -First 1
Step "invoke_undelete item_found=$($null -ne $item) is_folder=$($item.IsFolder)"
'shell_found={0} shell_isfolder={1}' -f ($null -ne $item), ($null -ne $item -and $item.IsFolder)
$item.InvokeVerb('undelete')
Step "invoked"
$deadline = (Get-Date).AddSeconds(15)
while (-not (Test-Path -LiteralPath (Join-Path $env:F 'nested\b.txt')) -and (Get-Date) -lt $deadline) { Start-Sleep -Milliseconds 100 }
$restored = (Test-Path -LiteralPath $env:F) -and (Test-Path -LiteralPath (Join-Path $env:F 'a.txt')) -and (Test-Path -LiteralPath (Join-Path $env:F 'nested\b.txt'))
Step "waited restored=$restored"
$aOk = $restored -and ([IO.File]::ReadAllText((Join-Path $env:F 'a.txt')) -eq 'alpha-123')
$bOk = $restored -and ([IO.File]::ReadAllText((Join-Path $env:F 'nested\b.txt')) -eq 'beta-4567')
'restored_exists={0} a_ok={1} b_ok={2} pairs_after_restore={3}' -f $restored, $aOk, $bOk, @(Pair-For $env:F).Count
Step "checked a_ok=$aOk b_ok=$bOk"
[Microsoft.VisualBasic.FileIO.FileSystem]::DeleteDirectory($env:F, 'OnlyErrorDialogs', 'SendToRecycleBin', 'ThrowException')
Step "recycled_again"
$own = Pair-For $env:F
foreach ($i in $own) {
  $r = Join-Path $i.DirectoryName ('$R' + $i.Name.Substring(2))
  Remove-Item -LiteralPath $r -Recurse -Force
  Remove-Item -LiteralPath $i.FullName -Force -ErrorAction SilentlyContinue
}
'cleared_pairs={0}' -f @(Pair-For $env:F).Count
Step "cleared"
"#;

#[test]
#[ignore = "requires WinFSP runtime, an explicitly selected unused drive, and an owned test volume"]
fn winfsp_native_trash_directory_restore_and_clear_shell_probe() {
    use windows::Win32::Storage::FileSystem::GetLogicalDrives;
    install_probe_trace_log();

    let mount_point = test_mount_point();
    let bytes = mount_point.as_bytes();
    assert!(bytes.len() == 2 && bytes[0].is_ascii_uppercase() && bytes[1] == b':');
    // SAFETY: GetLogicalDrives takes no pointers and only queries drive occupancy.
    let drives = unsafe { GetLogicalDrives() };
    assert_eq!(drives & (1 << (bytes[0] - b'A')), 0, "drive is occupied");
    let tmp = studiofs_test_support::workspace_target_tempdir("trash-native-dir-restore-");
    let script = tmp.path().join("recycle-dir-restore.ps1");
    std::fs::write(&script, RECYCLE_DIR_RESTORE_PS1).expect("write Shell dir restore probe");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_trash_enabled_test_vfs(&tmp, "trash-native-dir-restore", &rt);
    let observer = vfs.clone();
    let event = format!(
        r"Local\StudioFsNativeDirRestoreProbe-{}",
        std::process::id()
    );
    with_winfsp_mount(vfs, rt, mount_point, event, |root| {
        let dir = std::path::Path::new(root).join("shell-dir-restore");
        let out = run_shell_probe_ps1(&script, &dir, false);
        assert!(
            out.contains("i_size=18")
                && out.contains("r_isdir=True")
                && out.contains("r_a_ok=True r_b_ok=True"),
            "the $R directory view must expose directory kind, recursive size 18, and readable nested files: {out}"
        );
        assert!(
            out.contains("shell_found=True shell_isfolder=True"),
            "Shell NameSpace(10) must see recycled directory as folder: {out}"
        );
        assert!(
            out.contains("restored_exists=True a_ok=True b_ok=True pairs_after_restore=0"),
            "directory restore output: {out}"
        );
        assert!(out.contains("cleared_pairs=0"), "clear output: {out}");
        use studiofs_meta::trash::{RetainedEntryState, RetainedPurgeReason};
        let meta = observer.meta();
        let store_dir = meta
            .lookup(1, b".studiofs-retained")
            .expect("private retained store directory");
        let store = meta.retained_store().expect("retained store");
        let states: Vec<_> = meta
            .readdir(store_dir.ino)
            .expect("list retained store")
            .into_iter()
            .filter_map(|child| {
                std::str::from_utf8(&child.name)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .map(|receipt| store.retained_entry(receipt).unwrap().unwrap().state)
            .collect();
        assert_eq!(states.len(), 2, "restore + clear receipts: {states:?}");
        assert!(
            states.iter().any(|s| matches!(
                s,
                RetainedEntryState::Restored { parent: 1, name } if name == b"shell-dir-restore"
            )),
            "expected Restored receipt: {states:?}"
        );
        assert!(
            states.contains(&RetainedEntryState::PurgeRequested {
                reason: RetainedPurgeReason::User,
            }),
            "expected PurgeRequested(User) receipt after clear: {states:?}"
        );
    });
}

const RECYCLE_DIR_PARTIAL_CLEAR_PS1: &str = r#"
$ErrorActionPreference = "Stop"
$log = $env:STUDIOFS_PROBE_STEPS
function Step([string]$s) { Add-Content -LiteralPath $log -Value ((Get-Date).ToString("HH:mm:ss.fff") + " " + $s) }
Remove-Item -LiteralPath $log -ErrorAction SilentlyContinue
Add-Type -AssemblyName Microsoft.VisualBasic
$root = [IO.Path]::GetPathRoot($env:F)
$top = Join-Path $root ('$Recycle.Bin')
function Pair-For([string]$path) {
  @(Get-ChildItem -LiteralPath $top -Force -File -Filter '$I*' -ErrorAction SilentlyContinue | Where-Object {
    $b = [IO.File]::ReadAllBytes($_.FullName)
    $b.Length -gt 28 -and [BitConverter]::ToInt64($b, 0) -eq 2 -and [Text.Encoding]::Unicode.GetString($b, 28, $b.Length - 28).TrimEnd([char]0) -eq $path })
}
$nested = Join-Path $env:F 'nested'
New-Item -ItemType Directory -Path $nested -Force | Out-Null
[IO.File]::WriteAllText((Join-Path $env:F 'a.txt'), 'alpha-123')
[IO.File]::WriteAllText((Join-Path $nested 'b.txt'), 'beta-4567')
[Microsoft.VisualBasic.FileIO.FileSystem]::DeleteDirectory($env:F, 'OnlyErrorDialogs', 'SendToRecycleBin', 'ThrowException')
$hit = Pair-For $env:F
Step "pair_found=$($hit.Count)"
$payload = Join-Path $top ('$R' + $hit[0].Name.Substring(2))
# Native recursive delete of one child subtree only; the rest of the Clear never happens.
Remove-Item -LiteralPath (Join-Path $payload 'nested') -Recurse -Force
Step "child_deleted"
$iSize = [BitConverter]::ToInt64([IO.File]::ReadAllBytes($hit[0].FullName), 8)
$nestedGone = -not (Test-Path -LiteralPath (Join-Path $payload 'nested'))
$aOk = [IO.File]::ReadAllText((Join-Path $payload 'a.txt')) -eq 'alpha-123'
'after_child_delete i_size={0} nested_gone={1} a_ok={2}' -f $iSize, $nestedGone, $aOk
[IO.Directory]::Move($payload, $env:F)
Step "restored"
$restoredA = [IO.File]::ReadAllText((Join-Path $env:F 'a.txt')) -eq 'alpha-123'
$restoredNestedAbsent = -not (Test-Path -LiteralPath (Join-Path $env:F 'nested'))
'restored_a_ok={0} restored_nested_absent={1} pairs_after_restore={2}' -f $restoredA, $restoredNestedAbsent, @(Pair-For $env:F).Count
"#;

#[test]
#[ignore = "requires WinFSP runtime, an explicitly selected unused drive, and an owned test volume"]
fn winfsp_native_trash_directory_partial_clear_shell_probe() {
    use windows::Win32::Storage::FileSystem::GetLogicalDrives;
    install_probe_trace_log();

    let mount_point = test_mount_point();
    let bytes = mount_point.as_bytes();
    assert!(bytes.len() == 2 && bytes[0].is_ascii_uppercase() && bytes[1] == b':');
    // SAFETY: GetLogicalDrives takes no pointers and only queries drive occupancy.
    let drives = unsafe { GetLogicalDrives() };
    assert_eq!(drives & (1 << (bytes[0] - b'A')), 0, "drive is occupied");
    let tmp = studiofs_test_support::workspace_target_tempdir("trash-native-dir-partial-");
    let script = tmp.path().join("recycle-dir-partial-clear.ps1");
    std::fs::write(&script, RECYCLE_DIR_PARTIAL_CLEAR_PS1).expect("write partial Clear probe");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_trash_enabled_test_vfs(&tmp, "trash-native-dir-partial", &rt);
    let observer = vfs.clone();
    let event = format!(
        r"Local\StudioFsNativeDirPartialProbe-{}",
        std::process::id()
    );
    with_winfsp_mount(vfs, rt, mount_point, event, |root| {
        let dir = std::path::Path::new(root).join("shell-dir-partial");
        let out = run_shell_probe_ps1(&script, &dir, false);
        assert!(
            out.contains("after_child_delete i_size=9 nested_gone=True a_ok=True"),
            "a completed child delete is permanent and $I reflects the surviving tree: {out}"
        );
        assert!(
            out.contains("restored_a_ok=True restored_nested_absent=True pairs_after_restore=0"),
            "the remaining tree restores with exact bytes and no deleted child: {out}"
        );
        use studiofs_meta::trash::RetainedEntryState;
        let meta = observer.meta();
        let store_dir = meta
            .lookup(1, b".studiofs-retained")
            .expect("private retained store directory");
        let store = meta.retained_store().expect("retained store");
        let states: Vec<_> = meta
            .readdir(store_dir.ino)
            .expect("list retained store")
            .into_iter()
            .filter_map(|child| {
                std::str::from_utf8(&child.name)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .map(|receipt| store.retained_entry(receipt).unwrap().unwrap().state)
            .collect();
        assert!(
            matches!(
                states.as_slice(),
                [RetainedEntryState::Restored { parent: 1, name }] if name == b"shell-dir-partial"
            ),
            "expected one Restored receipt: {states:?}"
        );
    });
}

const RECYCLE_DIR_COLLISION_PS1: &str = r##"
$ErrorActionPreference = "Stop"
$log = $env:STUDIOFS_PROBE_STEPS
function Step([string]$s) { Add-Content -LiteralPath $log -Value ((Get-Date).ToString("HH:mm:ss.fff") + " " + $s) }
Remove-Item -LiteralPath $log -ErrorAction SilentlyContinue
Add-Type -AssemblyName Microsoft.VisualBasic
Add-Type @"
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
public static class WinDialogProbe {
  public delegate bool EnumProc(IntPtr hWnd, IntPtr lParam);
  [DllImport("user32.dll")] public static extern bool EnumWindows(EnumProc cb, IntPtr lParam);
  [DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr hWnd);
  [DllImport("user32.dll", CharSet = CharSet.Unicode)] public static extern int GetClassName(IntPtr hWnd, StringBuilder sb, int max);
  [DllImport("user32.dll", CharSet = CharSet.Unicode)] public static extern int GetWindowText(IntPtr hWnd, StringBuilder sb, int max);
  [DllImport("user32.dll")] public static extern uint GetWindowThreadProcessId(IntPtr hWnd, out uint pid);
  [DllImport("user32.dll")] public static extern bool PostMessage(IntPtr hWnd, uint msg, IntPtr wParam, IntPtr lParam);
  [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)] public static extern bool MoveFileEx(string src, string dst, uint flags);
  public const uint WM_CLOSE = 0x0010;
  public static List<string> ExplorerDialogs(uint childPid) {
    var list = new List<string>();
    var targetPids = new HashSet<uint>();
    foreach (var p in Process.GetProcessesByName("explorer")) targetPids.Add((uint)p.Id);
    if (childPid != 0) targetPids.Add(childPid);
    EnumWindows((h, _) => {
      if (!IsWindowVisible(h)) return true;
      uint pid;
      GetWindowThreadProcessId(h, out pid);
      if (!targetPids.Contains(pid)) return true;
      var cls = new StringBuilder(128);
      GetClassName(h, cls, cls.Capacity);
      var c = cls.ToString();
      if (c == "#32770" || c == "OperationStatusWindow") {
        var title = new StringBuilder(256);
        GetWindowText(h, title, title.Capacity);
        list.Add(h.ToInt64() + "|" + c + "|" + title.ToString());
      }
      return true;
    }, IntPtr.Zero);
    return list;
  }
  public static void CloseHandle(long raw) {
    PostMessage(new IntPtr(raw), WM_CLOSE, IntPtr.Zero, IntPtr.Zero);
  }
}
"@
$root = [IO.Path]::GetPathRoot($env:F)
$top = Join-Path $root ('$Recycle.Bin')
Step "start"
function Pair-For([string]$path) {
  @(Get-ChildItem -LiteralPath $top -Force -File -Filter '$I*' -ErrorAction SilentlyContinue | Where-Object {
    $b = [IO.File]::ReadAllBytes($_.FullName)
    $b.Length -gt 28 -and [BitConverter]::ToInt64($b, 0) -eq 2 -and [Text.Encoding]::Unicode.GetString($b, 28, $b.Length - 28).TrimEnd([char]0) -eq $path })
}
[Microsoft.VisualBasic.FileIO.FileSystem]::DeleteDirectory($env:F, 'OnlyErrorDialogs', 'SendToRecycleBin', 'ThrowException')
$hit = Pair-For $env:F
Step "pair_found=$($hit.Count)"
$payload = Join-Path $top ('$R' + $hit[0].Name.Substring(2))
$blockerNested = Join-Path $env:F 'keep.txt'
New-Item -ItemType Directory -Path $env:F -Force | Out-Null
[IO.File]::WriteAllText($blockerNested, 'KEEP-COLLIDING')
Step "blocker_written"
$beforeDialogs = @([WinDialogProbe]::ExplorerDialogs(0))
$env:STUDIOFS_COLLISION_PAYLOAD = $payload
$childScript = '$ns = (New-Object -ComObject Shell.Application).NameSpace(10); $item = $ns.Items() | Where-Object { $_.Path -eq $env:STUDIOFS_COLLISION_PAYLOAD } | Select-Object -First 1; if ($null -ne $item) { $item.InvokeVerb("undelete") }'
$encodedChild = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($childScript))
$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = 'powershell.exe'
$psi.Arguments = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -EncodedCommand $encodedChild"
$psi.UseShellExecute = $false
$psi.CreateNoWindow = $true
$proc = [System.Diagnostics.Process]::Start($psi)
$promptInfo = ''
$promptClass = 'none'
$deadline = (Get-Date).AddSeconds(4)
while (-not $proc.HasExited -and (Get-Date) -lt $deadline) {
  $nowDialogs = @([WinDialogProbe]::ExplorerDialogs([uint32]$proc.Id))
  $newDialogs = @($nowDialogs | Where-Object { $beforeDialogs -notcontains $_ })
  if ($newDialogs.Count -gt 0) {
    $promptInfo = $newDialogs[0]
    $promptClass = $promptInfo.Split('|')[1]
    foreach ($d in $newDialogs) {
      $hwndRaw = [long]($d.Split('|')[0])
      [WinDialogProbe]::CloseHandle($hwndRaw) | Out-Null
    }
    $null = $proc.WaitForExit(1500)
    break
  }
  Start-Sleep -Milliseconds 50
}
$undeleteExited = $proc.HasExited
if (-not $undeleteExited) {
  try { $proc.Kill() } catch {}
  $null = $proc.WaitForExit(1500)
}
Remove-Item Env:\STUDIOFS_COLLISION_PAYLOAD -ErrorAction SilentlyContinue
$keepOk = (Test-Path -LiteralPath $blockerNested) -and ([IO.File]::ReadAllText($blockerNested) -eq 'KEEP-COLLIDING')
$noSilentOverwrite = -not (Test-Path -LiteralPath (Join-Path $env:F 'a.txt')) -and -not (Test-Path -LiteralPath (Join-Path $env:F 'nested\b.txt'))
$pairPreserved = (@(Pair-For $env:F).Count -eq 1) -and ([IO.File]::ReadAllText((Join-Path $payload 'a.txt')) -eq 'alpha-123') -and ([IO.File]::ReadAllText((Join-Path $payload 'nested\b.txt')) -eq 'beta-4567')
Step "after_undelete exited=$undeleteExited prompt=$promptInfo keep=$keepOk no_overwrite=$noSilentOverwrite pair=$pairPreserved"

$blocked = $env:F + '-blocked'
New-Item -ItemType Directory -Path $blocked -Force | Out-Null
[IO.File]::WriteAllText((Join-Path $blocked 'blocker.txt'), 'BLOCKER-KEEP')
$blockedRenameRefused = $false
try {
  [IO.Directory]::Move($payload, $blocked)
} catch {
  $blockedRenameRefused = $true
}
$replaceRenameRefused = -not ([WinDialogProbe]::MoveFileEx($payload, $blocked, 1))
$blockerIntact = ([IO.File]::ReadAllText((Join-Path $blocked 'blocker.txt')) -eq 'BLOCKER-KEEP') -and (-not (Test-Path -LiteralPath (Join-Path $blocked 'a.txt')))

$alt = $env:F + '-alt'
[IO.Directory]::Move($payload, $alt)
$altRestoredOk = (Test-Path -LiteralPath (Join-Path $alt 'a.txt')) -and ([IO.File]::ReadAllText((Join-Path $alt 'a.txt')) -eq 'alpha-123') -and (Test-Path -LiteralPath (Join-Path $alt 'nested\b.txt')) -and ([IO.File]::ReadAllText((Join-Path $alt 'nested\b.txt')) -eq 'beta-4567')
$pairsAfterAltRestore = @(Pair-For $env:F).Count
Step "after_alt blocked_refused=$blockedRenameRefused replace_refused=$replaceRenameRefused blocker_intact=$blockerIntact alt_ok=$altRestoredOk pairs=$pairsAfterAltRestore"
'undelete_exited={0} prompt_seen={1} prompt_class={2} keep_ok={3} no_silent_overwrite={4} pair_preserved={5} blocked_rename_refused={6} replace_rename_refused={7} blocker_intact={8} alt_restored_ok={9} pairs_after_alt_restore={10}' -f $undeleteExited, ($promptInfo -ne ''), $promptClass, $keepOk, $noSilentOverwrite, $pairPreserved, $blockedRenameRefused, $replaceRenameRefused, $blockerIntact, $altRestoredOk, $pairsAfterAltRestore
"##;

#[test]
#[ignore = "requires WinFSP runtime, an explicitly selected unused drive, and an owned test volume"]
fn winfsp_native_trash_directory_collision_shell_probe() {
    use windows::Win32::Storage::FileSystem::GetLogicalDrives;
    install_probe_trace_log();

    let mount_point = test_mount_point();
    let bytes = mount_point.as_bytes();
    assert!(bytes.len() == 2 && bytes[0].is_ascii_uppercase() && bytes[1] == b':');
    // SAFETY: GetLogicalDrives takes no pointers and only queries drive occupancy.
    let drives = unsafe { GetLogicalDrives() };
    assert_eq!(drives & (1 << (bytes[0] - b'A')), 0, "drive is occupied");
    let tmp = studiofs_test_support::workspace_target_tempdir("trash-native-dir-conflict-");
    let script = tmp.path().join("recycle-dir-conflict.ps1");
    std::fs::write(&script, RECYCLE_DIR_COLLISION_PS1).expect("write Shell dir conflict probe");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_trash_enabled_test_vfs(&tmp, "trash-native-dir-conflict", &rt);
    let observer = vfs.clone();
    let event = format!(
        r"Local\StudioFsNativeDirConflictProbe-{}",
        std::process::id()
    );
    with_winfsp_mount(vfs, rt, mount_point, event, |root| {
        let dir = std::path::Path::new(root).join("shell-dir-conflict");
        let nested = dir.join("nested");
        std::fs::create_dir_all(&nested).expect("create conflict source dir");
        std::fs::write(dir.join("a.txt"), "alpha-123").expect("write a.txt");
        std::fs::write(nested.join("b.txt"), "beta-4567").expect("write b.txt");
        let orig_dir_ino = observer
            .lookup(1, b"shell-dir-conflict")
            .expect("lookup source dir before recycle")
            .ino;

        let out = run_shell_probe_ps1(&script, &dir, false);
        assert!(
            out.contains("keep_ok=True no_silent_overwrite=True pair_preserved=True"),
            "collision must never silently overwrite existing files and must preserve $R/$I: {out}"
        );
        assert!(
            out.contains(
                "blocked_rename_refused=True replace_rename_refused=True blocker_intact=True"
            ),
            "occupied alternate rename restore must refuse without overwrite: {out}"
        );
        assert!(
            out.contains("alt_restored_ok=True pairs_after_alt_restore=0"),
            "explicit alternate-destination restore must succeed cleanly: {out}"
        );

        let restored_alt_ino = observer
            .lookup(1, b"shell-dir-conflict-alt")
            .expect("lookup restored alternate directory")
            .ino;
        assert_eq!(
            restored_alt_ino, orig_dir_ino,
            "restored directory must preserve original inode identity"
        );

        use studiofs_meta::trash::RetainedEntryState;
        let meta = observer.meta();
        let store_dir = meta
            .lookup(1, b".studiofs-retained")
            .expect("private retained store directory");
        let store = meta.retained_store().expect("retained store");
        let entries: Vec<_> = meta
            .readdir(store_dir.ino)
            .expect("list retained store")
            .into_iter()
            .filter_map(|child| {
                std::str::from_utf8(&child.name)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .map(|receipt| store.retained_entry(receipt).unwrap().unwrap())
            .collect();
        assert_eq!(entries.len(), 1, "single receipt expected: {entries:?}");
        assert_eq!(entries[0].inode, orig_dir_ino);
        assert!(matches!(
            &entries[0].state,
            RetainedEntryState::Restored { parent: 1, name } if name == b"shell-dir-conflict-alt"
        ));

        for suffix in ["", "-blocked", "-alt"] {
            let p = std::path::Path::new(root).join(format!("shell-dir-conflict{suffix}"));
            if p.exists() {
                std::fs::remove_dir_all(&p).expect("clean owned conflict test directory");
            }
        }
    });
}

const REMOTE_RECOVERY_PROBE_PS1: &str = r##"
$ErrorActionPreference = "Stop"
Add-Type -AssemblyName Microsoft.VisualBasic
Add-Type @"
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
public static class RemoteRecoveryWinApi {
  public delegate bool EnumProc(IntPtr hWnd, IntPtr lParam);
  [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
  public static extern bool MoveFileEx(string src, string dst, uint flags);
  [DllImport("user32.dll")] public static extern bool EnumWindows(EnumProc cb, IntPtr lParam);
  [DllImport("user32.dll")] public static extern bool EnumChildWindows(IntPtr hWndParent, EnumProc cb, IntPtr lParam);
  [DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr hWnd);
  [DllImport("user32.dll", CharSet = CharSet.Unicode)] public static extern int GetClassName(IntPtr hWnd, StringBuilder sb, int max);
  [DllImport("user32.dll", CharSet = CharSet.Unicode)] public static extern int GetWindowText(IntPtr hWnd, StringBuilder sb, int max);
  [DllImport("user32.dll")] public static extern uint GetWindowThreadProcessId(IntPtr hWnd, out uint pid);
  [DllImport("user32.dll")] public static extern bool PostMessage(IntPtr hWnd, uint msg, IntPtr wParam, IntPtr lParam);
  public const uint WM_CLOSE = 0x0010;
  public static List<string> CaptureDialogs(uint childPid) {
    var list = new List<string>();
    var targetPids = new HashSet<uint>();
    foreach (var p in Process.GetProcessesByName("explorer")) targetPids.Add((uint)p.Id);
    if (childPid != 0) targetPids.Add(childPid);
    EnumWindows((h, _) => {
      if (!IsWindowVisible(h)) return true;
      uint pid;
      GetWindowThreadProcessId(h, out pid);
      if (!targetPids.Contains(pid)) return true;
      var cls = new StringBuilder(128);
      GetClassName(h, cls, cls.Capacity);
      var c = cls.ToString();
      if (c == "#32770" || c == "OperationStatusWindow") {
        var title = new StringBuilder(256);
        GetWindowText(h, title, title.Capacity);
        var children = new List<string>();
        EnumChildWindows(h, (ch, __) => {
          var ccls = new StringBuilder(128);
          GetClassName(ch, ccls, ccls.Capacity);
          var ctxt = new StringBuilder(512);
          GetWindowText(ch, ctxt, ctxt.Capacity);
          var t = ctxt.ToString().Replace("\r", " ").Replace("\n", " ").Trim();
          if (t.Length > 0) children.Add(ccls.ToString() + "=" + t);
          return true;
        }, IntPtr.Zero);
        list.Add(h.ToInt64() + "|" + c + "|" + title.ToString() + "|[" + string.Join("; ", children) + "]");
      }
      return true;
    }, IntPtr.Zero);
    return list;
  }
  public static void CloseHandle(long raw) {
    PostMessage(new IntPtr(raw), WM_CLOSE, IntPtr.Zero, IntPtr.Zero);
  }
}
"@

function Pair-For([string]$path) {
  $root = [IO.Path]::GetPathRoot($path)
  $top = Join-Path $root '$Recycle.Bin'
  @(Get-ChildItem -LiteralPath $top -Force -File -Filter '$I*' -ErrorAction SilentlyContinue | Where-Object {
    $b = [IO.File]::ReadAllBytes($_.FullName)
    $b.Length -gt 28 -and [BitConverter]::ToInt64($b, 0) -eq 2 -and [Text.Encoding]::Unicode.GetString($b, 28, $b.Length - 28).TrimEnd([char]0) -eq $path })
}

switch ($env:STUDIOFS_OP) {
  'recycle_dir' {
    [Microsoft.VisualBasic.FileIO.FileSystem]::DeleteDirectory($env:TARGET, 'OnlyErrorDialogs', 'SendToRecycleBin', 'ThrowException')
    $hit = Pair-For $env:TARGET
    if ($hit.Count -ne 1) { throw "expected 1 pair for dir, got $($hit.Count)" }
    $r = Join-Path $hit[0].DirectoryName ('$R' + $hit[0].Name.Substring(2))
    "pair=$($hit[0].FullName)|$r"
  }
  'recycle_file' {
    [Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile($env:TARGET, 'OnlyErrorDialogs', 'SendToRecycleBin', 'ThrowException')
    $hit = Pair-For $env:TARGET
    if ($hit.Count -ne 1) { throw "expected 1 pair for file, got $($hit.Count)" }
    $r = Join-Path $hit[0].DirectoryName ('$R' + $hit[0].Name.Substring(2))
    "pair=$($hit[0].FullName)|$r"
  }
  'win32_move' {
    $sw = [Diagnostics.Stopwatch]::StartNew()
    $ok = [RemoteRecoveryWinApi]::MoveFileEx($env:SRC, $env:DST, 0)
    $err = if ($ok) { 0 } else { [Runtime.InteropServices.Marshal]::GetLastWin32Error() }
    $sw.Stop()
    "move_ok=$ok win32_err=$err elapsed_ms=$($sw.ElapsedMilliseconds)"
  }
  'shell_undelete' {
    $before = @([RemoteRecoveryWinApi]::CaptureDialogs(0))
    $env:STUDIOFS_SHELL_UNDELETE_PAYLOAD = $env:SRC
    $childScript = '$ns = (New-Object -ComObject Shell.Application).NameSpace(10); $item = $ns.Items() | Where-Object { $_.Path -eq $env:STUDIOFS_SHELL_UNDELETE_PAYLOAD } | Select-Object -First 1; if ($null -eq $item) { exit 2 }; $item.InvokeVerb("undelete")'
    $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($childScript))
    $psi = New-Object Diagnostics.ProcessStartInfo
    $psi.FileName = 'powershell.exe'
    $psi.Arguments = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -EncodedCommand $encoded"
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $sw = [Diagnostics.Stopwatch]::StartNew()
    $proc = [Diagnostics.Process]::Start($psi)
    $seen = New-Object Collections.Generic.List[string]
    $deadline = (Get-Date).AddSeconds(4)
    while (-not $proc.HasExited -and (Get-Date) -lt $deadline) {
      $now = @([RemoteRecoveryWinApi]::CaptureDialogs([uint32]$proc.Id))
      $new = @($now | Where-Object { $before -notcontains $_ })
      if ($new.Count -gt 0) {
        foreach ($d in $new) {
          $seen.Add($d)
          [RemoteRecoveryWinApi]::CloseHandle([long]($d.Split('|')[0])) | Out-Null
        }
        $null = $proc.WaitForExit(1500)
        break
      }
      Start-Sleep -Milliseconds 50
    }
    $exited = $proc.HasExited
    $exitCode = if ($exited) { $proc.ExitCode } else { -1 }
    if (-not $exited) {
      try { $proc.Kill() } catch {}
      $null = $proc.WaitForExit(1500)
    }
    $sw.Stop()
    Remove-Item Env:\STUDIOFS_SHELL_UNDELETE_PAYLOAD -ErrorAction SilentlyContinue
    "shell_exited=$exited exit_code=$exitCode elapsed_ms=$($sw.ElapsedMilliseconds) dialogs=$($seen.Count) dialog_details=$($seen -join ' ;; ')"
  }
  default {
    throw "unknown STUDIOFS_OP=$($env:STUDIOFS_OP)"
  }
}
"##;

fn run_remote_recovery_ps1(script: &std::path::Path, envs: &[(&str, &str)]) -> String {
    use std::process::{Command, Stdio};
    let mut cmd = Command::new("powershell.exe");
    cmd.args([
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
    ])
    .arg(script)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let output = cmd.output().expect("run remote recovery PowerShell probe");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        output.status.success(),
        "remote recovery PowerShell probe failed (status={}): stdout={stdout} stderr={stderr}",
        output.status
    );
    stdout
}

fn wait_for_lookup_in(
    vfs: &StudioVfs,
    parent: u64,
    name: &[u8],
    label: &str,
    timeout: Duration,
) -> InodeAttr {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match vfs.lookup(parent, name) {
            Ok(attr) => return attr,
            Err(error) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "lookup {label}: {error:?}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

fn collect_inode_object_ids(vfs: &StudioVfs, ino: u64, size: u64) -> Vec<u64> {
    let spans = vfs
        .meta()
        .lookup_regular_file_slices(ino, 0, size)
        .expect("lookup regular file slices");
    let mut ids: Vec<u64> = spans
        .into_iter()
        .filter_map(|span| span.payload.map(|p| p.object_id))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn wait_for_cached_objects(vfs: &StudioVfs, object_ids: &[u64], label: &str, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if object_ids
            .iter()
            .all(|&id| vfs.cached_store().get(id).is_some())
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{label}: background S3 prefetch did not populate cached objects {object_ids:?} within {} s",
            timeout.as_secs()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Ignored native probe on disposable W: + configured S3:
/// proves evicted retained directory (Win32 MoveFileEx) and file (Shell NameSpace 10
/// `InvokeVerb('undelete')`) fail fast without premature Restored receipt, trigger background
/// S3 fetch, preserve original inodes, and restore exact bytes on retry.
#[test]
#[ignore = "requires WinFSP runtime, configured S3 backend, and unused STUDIOFS_WINFSP_TEST_MOUNT_POINT"]
fn winfsp_native_trash_remote_only_retry_probe() {
    use studiofs_data::store::DataStore;
    use studiofs_meta::trash::RetainedEntryState;
    use windows::Win32::Storage::FileSystem::GetLogicalDrives;

    if !require_online_upload_backend("winfsp_native_trash_remote_only_retry_probe") {
        return;
    }
    install_probe_trace_log();

    let mount_point = test_mount_point();
    let bytes = mount_point.as_bytes();
    assert!(bytes.len() == 2 && bytes[0].is_ascii_uppercase() && bytes[1] == b':');
    // SAFETY: GetLogicalDrives takes no pointers and only queries drive occupancy.
    let drives = unsafe { GetLogicalDrives() };
    assert_eq!(drives & (1 << (bytes[0] - b'A')), 0, "drive is occupied");

    let tmp = studiofs_test_support::workspace_target_tempdir("trash-native-remote-");
    let script = tmp.path().join("remote-recovery.ps1");
    std::fs::write(&script, REMOTE_RECOVERY_PROBE_PS1).expect("write remote recovery PS1");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let (vfs, format_info) =
        make_trash_enabled_test_vfs_with_format_info(&tmp, "trash-native-remote", &rt);
    let data_store = DataStore::new(&DataStoreConfig::from(&format_info))
        .expect("create DataStore for S3 cleanup");
    let observer = vfs.clone();
    let cleanup_rt = rt.clone();
    let event = format!(r"Local\StudioFsNativeRemoteProbe-{}", std::process::id());

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        with_winfsp_mount(vfs, rt, mount_point, event, |root| {
            // --- Phase 1: Evicted remote-only directory via Win32 MoveFileEx ---
            let dir = std::path::Path::new(root).join("remote-dir-probe");
            let sub = dir.join("sub");
            std::fs::create_dir_all(&sub).expect("create remote dir tree");
            let a_bytes: Vec<u8> = (0..16384u32)
                .map(|i| (i as u8).wrapping_mul(11).wrapping_add(3))
                .collect();
            let b_bytes: Vec<u8> = (0..24576u32)
                .map(|i| (i as u8).wrapping_mul(17).wrapping_add(9))
                .collect();
            std::fs::write(dir.join("a.bin"), &a_bytes).expect("write a.bin");
            std::fs::write(sub.join("b.bin"), &b_bytes).expect("write b.bin");

            let orig_dir_ino = wait_for_lookup_in(
                &observer,
                1,
                b"remote-dir-probe",
                "remote-dir-probe",
                Duration::from_secs(5),
            )
            .ino;
            let orig_sub_ino = wait_for_lookup_in(
                &observer,
                orig_dir_ino,
                b"sub",
                "sub",
                Duration::from_secs(5),
            )
            .ino;
            let orig_a_ino = wait_for_lookup_in(
                &observer,
                orig_dir_ino,
                b"a.bin",
                "a.bin",
                Duration::from_secs(5),
            )
            .ino;
            let orig_b_ino = wait_for_lookup_in(
                &observer,
                orig_sub_ino,
                b"b.bin",
                "b.bin",
                Duration::from_secs(5),
            )
            .ino;
            wait_for_upload_ready(
                &observer,
                orig_a_ino,
                "a.bin S3 upload",
                Duration::from_secs(30),
            );
            wait_for_upload_ready(
                &observer,
                orig_b_ino,
                "b.bin S3 upload",
                Duration::from_secs(30),
            );

            let mut dir_obj_ids =
                collect_inode_object_ids(&observer, orig_a_ino, a_bytes.len() as u64);
            dir_obj_ids.extend(collect_inode_object_ids(
                &observer,
                orig_b_ino,
                b_bytes.len() as u64,
            ));
            assert!(
                dir_obj_ids.len() >= 2,
                "expected S3 objects for a.bin and b.bin: {dir_obj_ids:?}"
            );

            let dir_str = dir.to_string_lossy().into_owned();
            let recycle_dir_out = run_remote_recovery_ps1(
                &script,
                &[("STUDIOFS_OP", "recycle_dir"), ("TARGET", &dir_str)],
            );
            let pair_line = recycle_dir_out
                .lines()
                .find_map(|l| l.strip_prefix("pair="))
                .expect("pair line from recycle_dir");
            let (dir_i_path, dir_r_path) = pair_line.split_once('|').expect("I|R split");
            let dir_receipt_hex = std::path::Path::new(dir_r_path)
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|s| s.strip_prefix("$R"))
                .expect("$R hex suffix");
            let dir_receipt = u64::from_str_radix(dir_receipt_hex, 16).expect("parse receipt");

            // Evict ALL local payloads without reading $R first.
            for &id in &dir_obj_ids {
                observer
                    .cached_store()
                    .remove(id)
                    .expect("evict dir object");
                assert!(
                    observer.cached_store().get(id).is_none(),
                    "evicted object {id} must be absent from local cache"
                );
            }

            let move1_out = run_remote_recovery_ps1(
                &script,
                &[
                    ("STUDIOFS_OP", "win32_move"),
                    ("SRC", dir_r_path),
                    ("DST", &dir_str),
                ],
            );
            eprintln!("remote-dir first Win32 MoveFileEx: {}", move1_out.trim());
            assert!(
                move1_out.contains("move_ok=False win32_err=31"),
                "expected ERROR_GEN_FAILURE (31) from STATUS_UNSUCCESSFUL on remote-only dir restore, got: {move1_out}"
            );
            assert!(
                !dir.exists(),
                "failed remote dir restore must leave destination absent"
            );
            assert!(
                std::path::Path::new(dir_i_path).exists()
                    && std::path::Path::new(dir_r_path).exists(),
                "failed remote dir restore must preserve $I/$R pair"
            );
            let meta = observer.meta();
            let store = meta.retained_store().expect("retained store");
            assert_eq!(
                store.retained_entry(dir_receipt).unwrap().unwrap().state,
                RetainedEntryState::Retained,
                "failed remote dir restore must not emit premature Restored receipt"
            );

            wait_for_cached_objects(
                &observer,
                &dir_obj_ids,
                "remote-dir-probe background S3 fetch",
                Duration::from_secs(30),
            );

            let move2_out = run_remote_recovery_ps1(
                &script,
                &[
                    ("STUDIOFS_OP", "win32_move"),
                    ("SRC", dir_r_path),
                    ("DST", &dir_str),
                ],
            );
            eprintln!("remote-dir retry Win32 MoveFileEx: {}", move2_out.trim());
            assert!(
                move2_out.contains("move_ok=True"),
                "retry dir restore must succeed once S3 objects are cached: {move2_out}"
            );
            assert_eq!(
                std::fs::read(dir.join("a.bin")).expect("read restored a.bin"),
                a_bytes
            );
            assert_eq!(
                std::fs::read(sub.join("b.bin")).expect("read restored b.bin"),
                b_bytes
            );
            assert_eq!(
                observer.lookup(1, b"remote-dir-probe").unwrap().ino,
                orig_dir_ino
            );
            assert_eq!(
                observer.meta().lookup(orig_dir_ino, b"sub").unwrap().ino,
                orig_sub_ino
            );
            assert_eq!(
                observer.meta().lookup(orig_dir_ino, b"a.bin").unwrap().ino,
                orig_a_ino
            );
            assert_eq!(
                observer.meta().lookup(orig_sub_ino, b"b.bin").unwrap().ino,
                orig_b_ino
            );
            assert!(matches!(
                store.retained_entry(dir_receipt).unwrap().unwrap().state,
                RetainedEntryState::Restored { parent: 1, ref name } if name == b"remote-dir-probe"
            ));
            assert!(
                !std::path::Path::new(dir_i_path).exists()
                    && !std::path::Path::new(dir_r_path).exists(),
                "successful dir retry restore must remove $I/$R pair"
            );
            std::fs::remove_dir_all(&dir).expect("clean restored dir");

            // --- Phase 2: Evicted remote-only file via Shell NameSpace(10) InvokeVerb('undelete') ---
            let file = std::path::Path::new(root).join("shell-remote-file.bin");
            let file_bytes: Vec<u8> = (0..65536u32)
                .map(|i| (i as u8).wrapping_mul(19).wrapping_add(0x41))
                .collect();
            std::fs::write(&file, &file_bytes).expect("write shell-remote-file.bin");
            let orig_file_ino = wait_for_lookup(
                &observer,
                b"shell-remote-file.bin",
                "shell-remote-file.bin",
                Duration::from_secs(5),
            )
            .ino;
            wait_for_upload_ready(
                &observer,
                orig_file_ino,
                "shell-remote-file.bin S3 upload",
                Duration::from_secs(30),
            );
            let file_obj_ids =
                collect_inode_object_ids(&observer, orig_file_ino, file_bytes.len() as u64);
            assert!(
                !file_obj_ids.is_empty(),
                "expected S3 object for shell-remote-file.bin"
            );

            let file_str = file.to_string_lossy().into_owned();
            let recycle_file_out = run_remote_recovery_ps1(
                &script,
                &[("STUDIOFS_OP", "recycle_file"), ("TARGET", &file_str)],
            );
            let file_pair = recycle_file_out
                .lines()
                .find_map(|l| l.strip_prefix("pair="))
                .expect("pair line from recycle_file");
            let (file_i_path, file_r_path) = file_pair.split_once('|').expect("I|R split");
            let file_receipt_hex = std::path::Path::new(file_r_path)
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|s| s.strip_prefix("$R"))
                .expect("$R hex suffix");
            let file_receipt =
                u64::from_str_radix(file_receipt_hex, 16).expect("parse file receipt");

            for &id in &file_obj_ids {
                observer
                    .cached_store()
                    .remove(id)
                    .expect("evict file object");
                assert!(observer.cached_store().get(id).is_none());
            }

            let shell1_out = run_remote_recovery_ps1(
                &script,
                &[("STUDIOFS_OP", "shell_undelete"), ("SRC", file_r_path)],
            );
            eprintln!(
                "shell-remote-file first InvokeVerb('undelete'): {}",
                shell1_out.trim()
            );
            // Do not assume whether Explorer completed via implicit retry or surfaced a modal dialog;
            // observe both possibilities carefully while strictly forbidding silent loss / corruption.
            let already_restored = file.exists();
            if !already_restored {
                assert!(
                    std::path::Path::new(file_i_path).exists()
                        && std::path::Path::new(file_r_path).exists(),
                    "unrestored remote file must preserve $I/$R pair; shell1={shell1_out}"
                );
                assert_eq!(
                    store.retained_entry(file_receipt).unwrap().unwrap().state,
                    RetainedEntryState::Retained,
                    "unrestored remote file must keep Retained receipt; shell1={shell1_out}"
                );
                wait_for_cached_objects(
                    &observer,
                    &file_obj_ids,
                    "shell-remote-file background S3 fetch",
                    Duration::from_secs(30),
                );
                let shell2_out = run_remote_recovery_ps1(
                    &script,
                    &[("STUDIOFS_OP", "shell_undelete"), ("SRC", file_r_path)],
                );
                eprintln!(
                    "shell-remote-file retry InvokeVerb('undelete'): {}",
                    shell2_out.trim()
                );
                let wait_deadline = std::time::Instant::now() + Duration::from_secs(10);
                while !file.exists() && std::time::Instant::now() < wait_deadline {
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
            assert!(
                file.exists(),
                "shell-remote-file.bin must exist after remote recovery flow; shell1={shell1_out}"
            );
            assert_eq!(
                std::fs::read(&file).expect("read restored shell-remote-file.bin"),
                file_bytes
            );
            assert_eq!(
                observer.lookup(1, b"shell-remote-file.bin").unwrap().ino,
                orig_file_ino,
                "Shell restore must preserve original inode identity (no copy+delete fallback)"
            );
            assert!(matches!(
                store.retained_entry(file_receipt).unwrap().unwrap().state,
                RetainedEntryState::Restored { parent: 1, ref name } if name == b"shell-remote-file.bin"
            ));
            assert!(
                !std::path::Path::new(file_i_path).exists()
                    && !std::path::Path::new(file_r_path).exists(),
                "restored shell-remote-file must clean up $I/$R pair"
            );
            std::fs::remove_file(&file).expect("clean restored shell-remote-file.bin");
        });
    }));

    // Always clean up our owned S3 volume prefix even on panic.
    let s3_clean = cleanup_rt.block_on(data_store.delete_volume_data());
    if let Err(err) = s3_clean {
        eprintln!("warning: S3 test volume cleanup failed: {err}");
    }
    if let Err(payload) = outcome {
        std::panic::resume_unwind(payload);
    }
}

/// Ignored native probe: a read-write mapped writer whose original file handle is
/// closed before the view is modified. Cleanup arrives first; the section keeps the
/// file object, so the mapped flush must still reach the mount as paging writes.
/// Passes only when the bytes reopen exact and the trace orders cleanup before them.
#[test]
#[ignore = "requires WinFSP runtime, an explicitly selected unused drive, and STUDIOFS_WINFSP_TRACE_LOG"]
fn winfsp_native_mapped_writer_survives_handle_close_probe() {
    mapped_writer_probe(false);
}

#[test]
#[ignore = "requires WinFSP runtime, unused drive, and STUDIOFS_WINFSP_TRACE_LOG"]
fn winfsp_native_mapped_writer_survives_restore_probe() {
    mapped_writer_probe(true);
}

fn mapped_writer_probe(restore: bool) {
    use std::io::Write;
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::{CloseHandle, HANDLE};
    use windows::Win32::Storage::FileSystem::GetLogicalDrives;
    use windows::Win32::System::Memory::{
        CreateFileMappingW, FILE_MAP_WRITE, FlushViewOfFile, MapViewOfFile, PAGE_READWRITE,
        UnmapViewOfFile,
    };
    // The order assertion below needs a trace; default it under `target/` when unset.
    let trace_path = std::env::var_os("STUDIOFS_WINFSP_TRACE_LOG")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("target/probe-trace/mapped-writer.log"));
    install_trace_log_at(&trace_path);
    let mount_point = test_mount_point();
    let bytes = mount_point.as_bytes();
    assert!(bytes.len() == 2 && bytes[0].is_ascii_uppercase() && bytes[1] == b':');
    // SAFETY: GetLogicalDrives takes no pointers and only queries drive occupancy.
    let drives = unsafe { GetLogicalDrives() };
    assert_eq!(drives & (1 << (bytes[0] - b'A')), 0, "drive is occupied");

    const NAME: &str = "mapped-writer.bin";
    const LEN: usize = 64 * 1024;
    const NEW: &[u8] = b"MAPPED-WRITER-NEW-BYTES";
    let original: Vec<u8> = (0..LEN)
        .map(|index| (index as u8).wrapping_mul(7).wrapping_add(0x31))
        .collect();
    let mut expected = original.clone();
    expected[..NEW.len()].copy_from_slice(NEW);

    let tmp = studiofs_test_support::workspace_target_tempdir("mapped-writer-native-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = if restore {
        make_trash_enabled_test_vfs(&tmp, "mapped-writer-native", &rt)
    } else {
        make_test_vfs(&tmp, "mapped-writer-native", &rt)
    };
    let observer = vfs.clone();
    let event = format!(r"Local\StudioFsMappedWriterProbe-{}", std::process::id());
    let restore_rt = rt.clone();
    let ino = with_winfsp_mount(vfs, rt, mount_point, event, |root| {
        let path = std::path::Path::new(root).join(NAME);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("open read-write mapped writer through WinFSP mount");
        file.write_all(&original).expect("write original bytes");
        let ino = wait_for_lookup(
            &observer,
            NAME.as_bytes(),
            "mapped writer",
            Duration::from_secs(5),
        )
        .ino;

        // SAFETY: `file` is open read-write; the mapping covers the whole file size.
        let mapping = unsafe {
            CreateFileMappingW(
                HANDLE(file.as_raw_handle()),
                None,
                PAGE_READWRITE,
                0,
                0,
                None,
            )
        }
        .expect("create read-write file mapping through WinFSP mount");
        // SAFETY: `mapping` is valid; the view maps exactly `LEN` bytes.
        let view = unsafe { MapViewOfFile(mapping, FILE_MAP_WRITE, 0, 0, LEN) };
        assert!(!view.Value.is_null(), "MapViewOfFile returned a null view");
        // SAFETY: the view is `LEN` readable and writable bytes until unmapped below.
        let mapped = unsafe { std::slice::from_raw_parts_mut(view.Value as *mut u8, LEN) };
        assert_eq!(
            &mapped[..],
            &original[..],
            "mapped view must expose the written bytes"
        );

        // Close the original handle first. Cleanup arrives; the mapping keeps the file object.
        drop(file);
        if restore {
            // Windows refuses Shell recycling a mapped section. Exercise the
            // retained-store transition directly while native paging I/O stays live.
            restore_rt.block_on(observer.fsync(ino, false)).unwrap();
            observer
                .rename(1, NAME.as_bytes(), 1, b"mapped-retained.bin", false)
                .unwrap();
            let meta = observer.meta();
            let private = meta.create_dir(1, b"private", 0, 0, 0o700).unwrap();
            let receipt = meta.next_inode().unwrap();
            let store = meta.retained_store().unwrap();
            store
                .retain_namespace_entry(1, b"mapped-retained.bin", private.ino, receipt)
                .unwrap();
            restore_rt
                .block_on(observer.restore_retained_entry(store, receipt, 1, NAME.as_bytes()))
                .unwrap();
            assert_eq!(observer.lookup(1, NAME.as_bytes()).unwrap().ino, ino);
        }
        mapped[..NEW.len()].copy_from_slice(NEW);
        // SAFETY: the view and mapping are valid and unused after this point.
        unsafe {
            FlushViewOfFile(view.Value, LEN).expect("flush mapped view after handle close");
            UnmapViewOfFile(view).expect("unmap mapped view");
            CloseHandle(mapping).expect("close mapping handle");
        }
        let reopened = std::fs::read(&path).expect("reopen mapped writer target");
        assert_eq!(
            reopened, expected,
            "mapped bytes must persist after the handle closed"
        );
        ino
    });

    // Diagnostics: cleanup for this file must precede a paging write, and the close last.
    let trace = std::fs::read_to_string(trace_path).expect("read WinFSP probe trace");
    let needle = format!("ino={ino} ");
    let cleanup_at = trace
        .lines()
        .position(|line| line.contains("cleanup") && line.contains(&needle))
        .expect("cleanup callback for the mapped writer handle must be traced");
    let paging_after_cleanup = trace.lines().skip(cleanup_at + 1).any(|line| {
        line.contains("write") && line.contains(&needle) && line.contains("constrained_io=true")
    });
    assert!(
        paging_after_cleanup,
        "a constrained (paging) write must follow the handle's cleanup:\n{trace}"
    );
}

/// Default-disabled control: with a zero period, Shell's Move to Trash is a direct
/// delete. The source is gone, no `$I`/`$R` pair is published, and Shell reports
/// no refusal.
#[test]
#[ignore = "requires WinFSP runtime and an explicitly selected unused drive"]
fn winfsp_default_disabled_trash_is_destructive_shell_probe() {
    use windows::Win32::Storage::FileSystem::GetLogicalDrives;

    let mount_point = test_mount_point();
    let bytes = mount_point.as_bytes();
    assert!(bytes.len() == 2 && bytes[0].is_ascii_uppercase() && bytes[1] == b':');
    // SAFETY: GetLogicalDrives takes no pointers and only queries drive occupancy.
    let drives = unsafe { GetLogicalDrives() };
    assert_eq!(drives & (1 << (bytes[0] - b'A')), 0, "drive is occupied");
    let tmp = studiofs_test_support::workspace_target_tempdir("trash-native-disabled-");
    // Refusal is the expected outcome here, so catch it and report it as output.
    let disabled_probe = RECYCLE_PROBE_PS1.replace(
        "[Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile($env:F, 'OnlyErrorDialogs', 'SendToRecycleBin', 'ThrowException')",
        "try { [Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile($env:F, 'OnlyErrorDialogs', 'SendToRecycleBin', 'ThrowException') } catch { 'delete_refused=' + $_.Exception.Message }",
    );
    assert!(
        disabled_probe.contains("delete_refused="),
        "probe must catch the refusal"
    );
    let script = tmp.path().join("recycle-probe-disabled.ps1");
    std::fs::write(&script, disabled_probe).expect("write Shell recycle probe");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "trash-native-disabled", &rt);
    let event = format!(r"Local\StudioFsNativeDisabledProbe-{}", std::process::id());
    with_winfsp_mount(vfs, rt, mount_point, event, |root| {
        let out = shell_recycle_probe(
            &script,
            &std::path::Path::new(root).join("shell-native-disabled.txt"),
            false,
        );
        assert!(
            out.contains("source_exists=False recycled_metadata=0")
                && !out.contains("delete_refused="),
            "disabled trash must delete directly without publishing a pair: {out}"
        );
    });
}

/// Product-path Shell check: the same Move to Trash oracle on a mount whose
/// trash period is positive. Restore and Clear are not covered by this probe.
#[test]
#[ignore = "requires WinFSP runtime, an explicitly selected unused drive, and an owned test volume"]
fn winfsp_native_trash_retention_shell_probe() {
    use windows::Win32::Storage::FileSystem::GetLogicalDrives;

    let mount_point = test_mount_point();
    let bytes = mount_point.as_bytes();
    assert!(bytes.len() == 2 && bytes[0].is_ascii_uppercase() && bytes[1] == b':');
    // SAFETY: GetLogicalDrives takes no pointers and only queries drive occupancy.
    let drives = unsafe { GetLogicalDrives() };
    assert_eq!(drives & (1 << (bytes[0] - b'A')), 0, "drive is occupied");
    let tmp = studiofs_test_support::workspace_target_tempdir("trash-native-probe-");
    let script = tmp.path().join("recycle-probe.ps1");
    std::fs::write(&script, RECYCLE_PROBE_PS1).expect("write Shell recycle probe");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_trash_enabled_test_vfs(&tmp, "trash-native-probe", &rt);
    let event = format!(r"Local\StudioFsNativeTrashProbe-{}", std::process::id());
    with_winfsp_mount(vfs, rt, mount_point, event, |root| {
        let mounted = shell_recycle_probe(
            &script,
            &std::path::Path::new(root).join("shell-native-trash.txt"),
            false,
        );
        assert!(
            mounted.contains("source_exists=False recycled_metadata=1 shell_listed=1"),
            "mounted Shell output: {mounted}"
        );
    });
}

#[test]
#[ignore = "requires WinFSP runtime and an explicitly selected unused drive"]
fn winfsp_recycle_namespace_feasibility_probe() {
    use windows::Win32::Storage::FileSystem::GetLogicalDrives;

    let mount_point = test_mount_point();
    let bytes = mount_point.as_bytes();
    assert!(bytes.len() == 2 && bytes[0].is_ascii_uppercase() && bytes[1] == b':');
    // SAFETY: GetLogicalDrives takes no pointers and only queries drive occupancy.
    let drives = unsafe { GetLogicalDrives() };
    assert_ne!(drives, 0, "query drive occupancy");
    assert_eq!(drives & (1 << (bytes[0] - b'A')), 0, "drive is occupied");
    let tmp = studiofs_test_support::workspace_target_tempdir("trash-probe-");
    let script = tmp.path().join("recycle-probe.ps1");
    std::fs::write(&script, RECYCLE_PROBE_PS1).expect("write Shell recycle probe");
    // Oracle self-check on the host volume; removes only its own `$I`/`$R` pair.
    let control = shell_recycle_probe(&script, &tmp.path().join("control-empty.txt"), true);
    assert!(
        control.contains("source_exists=False recycled_metadata=1 shell_listed=1"),
        "control volume must recycle through the same Shell path"
    );
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    // Trash is disabled by default (see the default-disabled probe); recycling needs retention.
    let vfs = make_trash_enabled_test_vfs(&tmp, "recycle-probe", &rt);
    let event = format!(r"Local\StudioFsRecycleProbe-{}", std::process::id());
    with_winfsp_mount(vfs, rt, mount_point, event, |root| {
        let mounted = shell_recycle_probe(
            &script,
            &std::path::Path::new(root).join("shell-probe-empty.txt"),
            false,
        );
        assert!(
            mounted.contains("source_exists=False recycled_metadata=1 shell_listed=1"),
            "WinFSP volume must recycle, not permanently delete"
        );
    });
}

/// Full lifecycle smoke test: mount → write → read → readdir → delete → unmount.
///
/// Uses `mount_and_serve` in a background thread with a named Win32 event
/// for external shutdown. File I/O is performed through the drive letter
/// before signalling the event.
#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_write_read_unmount_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP write-read smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "write_read_smoke", &rt);
    let event_name = format!("Local\\StudioFsWriteReadSmoke-{}", std::process::id());

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let test_file = std::path::PathBuf::from(drive_root).join("smoke.txt");
        let payload = b"StudioFS WinFSP smoke-test payload";
        std::fs::write(&test_file, payload).expect("write through WinFSP mount");

        let readback = std::fs::read(&test_file).expect("read through WinFSP mount");
        assert_eq!(readback, payload, "read-back must match written data");

        let entries: Vec<_> = std::fs::read_dir(drive_root)
            .expect("readdir through WinFSP mount")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();
        assert!(
            entries
                .iter()
                .any(|n| n == std::ffi::OsStr::new("smoke.txt")),
            "directory listing must contain the created file, got: {entries:?}"
        );

        std::fs::remove_file(&test_file).expect("delete through WinFSP mount");
    });
}

fn winfsp_fio_command() -> Option<std::path::PathBuf> {
    if let Some(path) = std::env::var_os("STUDIOFS_WINFSP_FIO_PATH") {
        let path = std::path::PathBuf::from(path);
        assert!(
            path.is_file(),
            "STUDIOFS_WINFSP_FIO_PATH is set but is not a valid file: {}",
            path.display()
        );
        return Some(path);
    }

    if let Some(paths) = std::env::var_os("PATH") {
        for path in std::env::split_paths(&paths) {
            let candidate = path.join("fio.exe");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }

    let candidate = std::path::PathBuf::from(r"C:\Program Files\fio\fio.exe");
    candidate.is_file().then_some(candidate)
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_fio_multi_job_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    let Some(fio) = winfsp_fio_command() else {
        eprintln!(
            "Skipping WinFSP fio mounted workload because fio.exe is not installed or STUDIOFS_WINFSP_FIO_PATH is not set"
        );
        return;
    };
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP fio mounted workload: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "fio_multi_job_smoke", &rt);
    let event_name = format!("Local\\StudioFsWinfspFioSmoke-{}", std::process::id());

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let status = std::process::Command::new(&fio)
            .current_dir(drive_root)
            .arg("--name=studiofs-winfsp-fio")
            .arg("--filename=fio.bin")
            .arg("--rw=write")
            .arg("--bs=64k")
            .arg("--size=1m")
            .arg("--numjobs=2")
            .arg("--offset_increment=1m")
            .arg("--ioengine=sync")
            .arg("--fsync=1")
            .arg("--group_reporting")
            .status()
            .expect("run fio workload through WinFSP mount");
        assert!(
            status.success(),
            "fio workload failed with status: {status}"
        );

        let fio_file = std::path::Path::new(drive_root).join("fio.bin");
        assert_eq!(
            std::fs::metadata(&fio_file)
                .expect("fio output metadata through WinFSP mount")
                .len(),
            2 * 1024 * 1024,
            "fio output file should contain disjoint 1 MiB regions for both jobs"
        );
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_regular_file_attributes_do_not_materialize_directories() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP file-attribute smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "file_attribute_smoke", &rt);
    let event_name = format!("Local\\StudioFsFileAttributeSmoke-{}", std::process::id());

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        use std::fs::OpenOptions;
        use std::io::Write;
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_ATTRIBUTE_READONLY: u32 = 0x0000_0001;
        const FILE_ATTRIBUTE_ARCHIVE: u32 = 0x0000_0020;
        const FILE_ATTRIBUTE_NORMAL: u32 = 0x0000_0080;
        const FILE_ATTRIBUTE_TEMPORARY: u32 = 0x0000_0100;

        for (name, attributes) in [
            ("attrs-normal.bin", FILE_ATTRIBUTE_NORMAL),
            ("attrs-archive.bin", FILE_ATTRIBUTE_ARCHIVE),
            ("attrs-readonly.bin", FILE_ATTRIBUTE_READONLY),
            ("attrs-temporary.bin", FILE_ATTRIBUTE_TEMPORARY),
        ] {
            let target = std::path::PathBuf::from(drive_root).join(name);
            let mut file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .attributes(attributes)
                .open(&target)
                .unwrap_or_else(|err| panic!("open {name} with attrs {attributes:#x}: {err}"));
            file.write_all(b"payload")
                .unwrap_or_else(|err| panic!("write payload to {name}: {err}"));
            file.flush()
                .unwrap_or_else(|err| panic!("flush payload for {name}: {err}"));
            drop(file);

            let metadata = std::fs::metadata(&target)
                .unwrap_or_else(|err| panic!("stat {name} after create: {err}"));
            assert!(
                metadata.is_file(),
                "{name} with attrs {attributes:#x} should remain a regular file, got dir={} readonly={}",
                metadata.is_dir(),
                metadata.permissions().readonly()
            );
            assert!(
                !metadata.is_dir(),
                "{name} with attrs {attributes:#x} unexpectedly materialized as a directory"
            );

            let mut permissions = metadata.permissions();
            if permissions.readonly() {
                #[allow(clippy::permissions_set_readonly_false)]
                permissions.set_readonly(false);
                std::fs::set_permissions(&target, permissions)
                    .unwrap_or_else(|err| panic!("clear readonly attr for {name}: {err}"));
            }
            std::fs::remove_file(&target)
                .unwrap_or_else(|err| panic!("delete {name} after attribute smoke test: {err}"));
        }
    });
}

#[test]
fn validate_aja_sequential_workload_accepts_defaults() {
    let workload = validate_aja_sequential_workload(
        DEFAULT_AJA_SEQUENTIAL_CHUNK_BYTES,
        DEFAULT_AJA_SEQUENTIAL_CHUNK_COUNT,
    )
    .expect("default workload should be accepted");
    assert_eq!(
        workload,
        (
            DEFAULT_AJA_SEQUENTIAL_CHUNK_BYTES,
            DEFAULT_AJA_SEQUENTIAL_CHUNK_COUNT
        )
    );
}

#[test]
fn validate_aja_sequential_workload_rejects_oversized_chunk_bytes() {
    let err = validate_aja_sequential_workload(MAX_AJA_SEQUENTIAL_CHUNK_BYTES + 1, 1)
        .expect_err("oversized chunk bytes should be rejected");
    assert!(
        err.contains("STUDIOFS_WINFSP_WRITE_CHUNK_BYTES"),
        "error should point at the chunk-bytes env var: {err}"
    );
}

#[test]
fn validate_aja_sequential_workload_rejects_excessive_total_bytes() {
    let err = validate_aja_sequential_workload(MAX_AJA_SEQUENTIAL_CHUNK_BYTES, 16)
        .expect_err("oversized total workload should be rejected");
    assert!(
        err.contains(
            "lower STUDIOFS_WINFSP_WRITE_CHUNK_BYTES or STUDIOFS_WINFSP_WRITE_CHUNK_COUNT"
        ),
        "error should explain how to lower the mounted workload: {err}"
    );
}

#[test]
fn validate_aja_sequential_throughput_accepts_fast_enough_rate() {
    let throughput = validate_aja_sequential_throughput(
        DEFAULT_AJA_SEQUENTIAL_CHUNK_BYTES * DEFAULT_AJA_SEQUENTIAL_CHUNK_COUNT,
        Duration::from_millis(250),
        DEFAULT_AJA_MIN_THROUGHPUT_MIB_PER_SEC,
    )
    .expect("fast enough throughput should be accepted");

    assert!(
        throughput >= DEFAULT_AJA_MIN_THROUGHPUT_MIB_PER_SEC,
        "measured throughput should stay above the configured floor"
    );
}

#[test]
fn validate_aja_sequential_throughput_rejects_slow_rate() {
    let err = validate_aja_sequential_throughput(
        DEFAULT_AJA_SEQUENTIAL_CHUNK_BYTES * DEFAULT_AJA_SEQUENTIAL_CHUNK_COUNT,
        Duration::from_secs(2),
        DEFAULT_AJA_MIN_THROUGHPUT_MIB_PER_SEC,
    )
    .expect_err("slow throughput should be rejected");

    assert!(
        err.contains("throughput fell below the required floor"),
        "error should explain the measured throughput shortfall: {err}"
    );
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_callback_flush_and_cleanup_observability_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP callback observability smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "callback_observability_smoke", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!(
        "Local\\StudioFsCallbackObservabilitySmoke-{}",
        std::process::id()
    );

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let flush_snapshot_before = vfs_observer.write_path_observability_snapshot();
        let repeated_flush_file = std::path::PathBuf::from(drive_root).join("repeated-flush.bin");
        let mut repeated = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&repeated_flush_file)
            .expect("open repeated-flush file through WinFSP mount");
        use std::io::Write;
        repeated
            .write_all(b"callback flush observability payload")
            .expect("write repeated-flush payload through WinFSP mount");
        repeated
            .sync_all()
            .expect("first sync_all through WinFSP mount");
        let repeated_ino = wait_for_lookup(
            &vfs_observer,
            b"repeated-flush.bin",
            "repeated-flush inode",
            Duration::from_secs(5),
        )
        .ino;
        let clean_deadline = std::time::Instant::now() + Duration::from_secs(5);
        while vfs_observer.inode_has_local_dirty_state(repeated_ino) {
            assert!(
                std::time::Instant::now() < clean_deadline,
                "first sync_all did not reach the clean inode state before the second callback"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        repeated
            .sync_all()
            .expect("second sync_all through WinFSP mount");
        drop(repeated);

        let flush_snapshot_after = vfs_observer.write_path_observability_snapshot();
        assert!(
            flush_snapshot_after.winfsp_callbacks.flush
                >= flush_snapshot_before.winfsp_callbacks.flush + 2,
            "two sync_all calls should surface at least two WinFSP flush callbacks"
        );
        assert!(
            flush_snapshot_after.winfsp_foreground_flushes.flush
                > flush_snapshot_before.winfsp_foreground_flushes.flush,
            "the first sync_all should attribute a foreground barrier to the flush callback path"
        );
        assert!(
            flush_snapshot_after
                .winfsp_foreground_flushes
                .flush_skipped_clean
                > flush_snapshot_before
                    .winfsp_foreground_flushes
                    .flush_skipped_clean,
            "the second sync_all should show a clean-skip flush callback instead of another barrier"
        );
        assert!(
            flush_snapshot_after.winfsp_callbacks.cleanup
                > flush_snapshot_before.winfsp_callbacks.cleanup,
            "closing the clean handle should still surface a cleanup callback"
        );
        assert!(
            flush_snapshot_after.winfsp_foreground_flushes.cleanup
                == flush_snapshot_before.winfsp_foreground_flushes.cleanup,
            "non-delete cleanup should remain observational even after explicit sync_all"
        );
        assert!(
            flush_snapshot_after
                .winfsp_foreground_flushes
                .cleanup_skipped_clean
                == flush_snapshot_before
                    .winfsp_foreground_flushes
                    .cleanup_skipped_clean,
            "non-delete cleanup should not be attributed as a clean-skip barrier"
        );
        // Mounted WinFSP close timing can include unrelated root `fh=0` closes;
        // deterministic file-handle close/release attribution is covered by
        // bridge unit tests.

        let close_snapshot_before = vfs_observer.write_path_observability_snapshot();
        let dirty_close_file = std::path::PathBuf::from(drive_root).join("dirty-close.bin");
        let mut dirty = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&dirty_close_file)
            .expect("open dirty-close file through WinFSP mount");
        dirty
            .write_all(b"dirty close payload")
            .expect("write dirty-close payload through WinFSP mount");
        drop(dirty);

        let close_snapshot_after = vfs_observer.write_path_observability_snapshot();
        assert!(
            close_snapshot_after.winfsp_callbacks.cleanup
                > close_snapshot_before.winfsp_callbacks.cleanup,
            "dropping a dirty writer should surface a cleanup callback"
        );
        assert!(
            close_snapshot_after.winfsp_foreground_flushes.cleanup
                == close_snapshot_before.winfsp_foreground_flushes.cleanup,
            "dirty-handle cleanup should not reclaim foreground barrier ownership"
        );
        assert!(
            close_snapshot_after.winfsp_callbacks.cleanup_flush_fh
                == close_snapshot_before.winfsp_callbacks.cleanup_flush_fh,
            "dirty-handle close should not route non-delete cleanup through flush_fh"
        );
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_aja_style_sequential_writer_validation_smoke() {
    use std::io::Write;
    use std::os::windows::fs::OpenOptionsExt;

    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP AJA-style sequential writer smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "aja_style_sequential_writer", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!(
        "Local\\StudioFsAjaSequentialWriterSmoke-{}",
        std::process::id()
    );
    let validation_modes = match aja_sequential_validation_modes_from_env() {
        Ok(modes) => modes,
        Err(reason) => {
            eprintln!("Skipping WinFSP AJA-style sequential writer smoke test: {reason}");
            return;
        }
    };
    let (chunk_bytes, chunk_count) = match validate_aja_sequential_workload_for_modes(
        aja_sequential_chunk_bytes(),
        aja_sequential_chunk_count(),
        &validation_modes,
    ) {
        Ok(workload) => workload,
        Err(reason) => {
            eprintln!("Skipping WinFSP AJA-style sequential writer smoke test: {reason}");
            return;
        }
    };
    if let Err(reason) = aja_sequential_min_throughput_mib_per_sec() {
        eprintln!("Skipping WinFSP AJA-style sequential writer smoke test: {reason}");
        return;
    }

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        // Approximate a media-style single-handle sequential writer: large
        // contiguous writes without explicit sync_all between chunks, then a
        // dirty handle close.
        for mode in &validation_modes {
            let snapshot_before = vfs_observer.write_path_observability_snapshot();
            let sequential_file = std::path::PathBuf::from(drive_root).join(mode.file_name());
            let mut writer = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .custom_flags(mode.open_options_custom_flags())
                .open(&sequential_file)
                .unwrap_or_else(|err| {
                    panic!(
                        "open AJA-style {} sequential writer file through WinFSP mount: {err}",
                        mode.label()
                    )
                });
            let mut chunk = AjaSequentialChunk::new(*mode, chunk_bytes).unwrap_or_else(|err| {
                panic!(
                    "allocate aligned AJA-style {} sequential chunk buffer: {err}",
                    mode.label()
                )
            });
            let total_bytes = chunk_bytes * chunk_count;
            let started_at = std::time::Instant::now();
            for chunk_index in 0..chunk_count {
                let fill = b'A' + (chunk_index % 26) as u8;
                chunk.as_mut_slice().fill(fill);
                write_aja_chunk(&mut writer, *mode, chunk.as_mut_slice()).unwrap_or_else(|err| {
                    panic!(
                        "write AJA-style {} sequential chunk through WinFSP mount: {err}",
                        mode.label()
                    )
                });
            }
            writer.flush().unwrap_or_else(|err| {
                panic!(
                    "flush AJA-style {} sequential writer through WinFSP mount: {err}",
                    mode.label()
                )
            });
            drop(writer);
            let elapsed = started_at.elapsed();
            let mode_min_throughput_mib_per_sec = aja_sequential_min_throughput_for_mode(*mode)
                .unwrap_or_else(|err| {
                    panic!(
                        "resolve AJA-style {} minimum throughput: {err}",
                        mode.label()
                    )
                });
            let throughput_mib_per_sec = validate_aja_sequential_throughput(
                total_bytes,
                elapsed,
                mode_min_throughput_mib_per_sec,
            )
            .unwrap_or_else(|err| panic!("{err}"));

            let snapshot_after = vfs_observer.write_path_observability_snapshot();
            assert!(
                snapshot_after.winfsp_callbacks.write > snapshot_before.winfsp_callbacks.write,
                "large sequential mounted writes should surface WinFSP write callbacks in {} mode (measured {:.2} MiB/s)",
                mode.label(),
                throughput_mib_per_sec
            );
            assert!(
                snapshot_after.winfsp_callbacks.cleanup > snapshot_before.winfsp_callbacks.cleanup,
                "dirty sequential writer close should still surface cleanup callbacks in {} mode (measured {:.2} MiB/s)",
                mode.label(),
                throughput_mib_per_sec
            );
            assert_eq!(
                snapshot_after.winfsp_foreground_flushes.cleanup,
                snapshot_before.winfsp_foreground_flushes.cleanup,
                "AJA-style {} sequential writer should not reintroduce non-delete cleanup barriers (measured {:.2} MiB/s)",
                mode.label(),
                throughput_mib_per_sec
            );
            assert_eq!(
                snapshot_after
                    .winfsp_foreground_flushes
                    .cleanup_skipped_clean,
                snapshot_before
                    .winfsp_foreground_flushes
                    .cleanup_skipped_clean,
                "AJA-style {} sequential writer should not attribute cleanup as a clean-skip barrier (measured {:.2} MiB/s)",
                mode.label(),
                throughput_mib_per_sec
            );
            assert_eq!(
                snapshot_after.winfsp_callbacks.cleanup_flush_fh,
                snapshot_before.winfsp_callbacks.cleanup_flush_fh,
                "AJA-style {} sequential writer should not route cleanup through flush_fh (measured {:.2} MiB/s)",
                mode.label(),
                throughput_mib_per_sec
            );
        }
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_reopen_read_after_upload_finalize_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    if !require_online_upload_backend("winfsp_mount_reopen_read_after_upload_finalize_smoke") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP post-upload read smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "post_upload_read", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!("Local\\StudioFsPostUploadReadSmoke-{}", std::process::id());

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let test_file = std::path::PathBuf::from(drive_root).join("post-upload.bin");
        let payload = vec![0xA7u8; 12 * 1024 * 1024];
        std::fs::write(&test_file, &payload).expect("write through WinFSP mount");

        let attr = wait_for_lookup(
            &vfs_observer,
            b"post-upload.bin",
            "written file",
            Duration::from_secs(5),
        );
        wait_for_upload_ready(
            &vfs_observer,
            attr.ino,
            "WinFSP file",
            Duration::from_secs(30),
        );

        let reopened = std::fs::read(&test_file).expect("re-open read through WinFSP mount");
        assert_eq!(
            reopened, payload,
            "post-upload read-back must match written data"
        );
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_copy_read_after_upload_finalize_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    if !require_online_upload_backend("winfsp_mount_copy_read_after_upload_finalize_smoke") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP post-upload copy-read smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let source_file = tmp.path().join("copy-source.bin");
    let source_payload: Vec<u8> = (0..(20 * 1024 * 1024))
        .map(|index| ((index as u64).wrapping_mul(11400714819323198485) >> 33) as u8)
        .collect();
    std::fs::write(&source_file, &source_payload).expect("write local source file");

    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "post_upload_copy_read", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!(
        "Local\\StudioFsPostUploadCopyReadSmoke-{}",
        std::process::id()
    );

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let target_file = std::path::PathBuf::from(drive_root).join("copied-post-upload.bin");
        std::fs::copy(&source_file, &target_file).expect("copy through WinFSP mount");

        let attr = wait_for_lookup(
            &vfs_observer,
            b"copied-post-upload.bin",
            "copied file",
            Duration::from_secs(5),
        );
        wait_for_upload_ready(
            &vfs_observer,
            attr.ino,
            "copied WinFSP file",
            Duration::from_secs(45),
        );

        let reopened =
            std::fs::read(&target_file).expect("re-open read copied file through WinFSP mount");
        assert_eq!(
            reopened, source_payload,
            "copied file post-upload read-back must match source payload"
        );
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_set_len_chunked_write_read_after_upload_finalize_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    if !require_online_upload_backend(
        "winfsp_mount_set_len_chunked_write_read_after_upload_finalize_smoke",
    ) {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP set-len chunked-write smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let payload: Vec<u8> = (0..(17 * 1024 * 1024 + 37_777))
        .map(|index| (index as u8).wrapping_mul(19).wrapping_add(0x5D))
        .collect();
    let chunk_pattern = [65_553usize, 131_069, 33_791, 1_048_581, 524_311, 262_147];

    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "post_upload_chunked_write", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!(
        "Local\\StudioFsPostUploadChunkedWriteSmoke-{}",
        std::process::id()
    );

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let target_file = std::path::PathBuf::from(drive_root).join("set-len-chunked.bin");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&target_file)
            .expect("open chunked target file through WinFSP mount");
        file.set_len(payload.len() as u64)
            .expect("pre-extend chunked target file through WinFSP mount");

        let mut offset = 0usize;
        let mut chunk_index = 0usize;
        while offset < payload.len() {
            let chunk_len =
                chunk_pattern[chunk_index % chunk_pattern.len()].min(payload.len() - offset);
            use std::io::{Seek, SeekFrom, Write};
            file.seek(SeekFrom::Start(offset as u64))
                .expect("seek chunked target file");
            file.write_all(&payload[offset..offset + chunk_len])
                .expect("write chunk through WinFSP mount");
            offset += chunk_len;
            chunk_index += 1;
        }
        file.sync_all()
            .expect("sync chunked target file through WinFSP mount");
        drop(file);

        let attr = wait_for_lookup(
            &vfs_observer,
            b"set-len-chunked.bin",
            "chunked target file",
            Duration::from_secs(5),
        );
        wait_for_upload_ready(
            &vfs_observer,
            attr.ino,
            "chunked WinFSP file",
            Duration::from_secs(45),
        );

        let reopened = std::fs::read(&target_file)
            .expect("re-open read chunked file through WinFSP mount after publish");
        assert_eq!(
            reopened, payload,
            "chunked file post-upload read-back must match written payload"
        );
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_same_handle_read_after_upload_finalize_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    if !require_online_upload_backend("winfsp_mount_same_handle_read_after_upload_finalize_smoke") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP same-handle post-upload read smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let payload: Vec<u8> = (0..(14 * 1024 * 1024 + 8191))
        .map(|index| (index as u8).wrapping_mul(7).wrapping_add(0x33))
        .collect();

    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "post_upload_same_handle", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!(
        "Local\\StudioFsPostUploadSameHandleSmoke-{}",
        std::process::id()
    );

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let target_file = std::path::PathBuf::from(drive_root).join("same-handle.bin");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&target_file)
            .expect("open same-handle file through WinFSP mount");
        use std::io::{Read, Seek, SeekFrom, Write};
        file.write_all(&payload)
            .expect("write same-handle payload through WinFSP mount");
        file.sync_all()
            .expect("sync same-handle file through WinFSP mount");

        let attr = wait_for_lookup(
            &vfs_observer,
            b"same-handle.bin",
            "same-handle file",
            Duration::from_secs(5),
        );
        wait_for_upload_ready(
            &vfs_observer,
            attr.ino,
            "same-handle WinFSP file",
            Duration::from_secs(45),
        );

        file.seek(SeekFrom::Start(0))
            .expect("seek same-handle file back to start");
        let mut same_handle_read = Vec::new();
        file.read_to_end(&mut same_handle_read)
            .expect("read same-handle file after publish");
        assert_eq!(
            same_handle_read, payload,
            "same-handle post-upload read-back must match written payload"
        );
        drop(file);
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_reader_open_during_upload_stays_readable_after_finalize_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    if !require_online_upload_backend(
        "winfsp_mount_reader_open_during_upload_stays_readable_after_finalize_smoke",
    ) {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP reader-open-during-upload smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let payload: Vec<u8> = (0..(24 * 1024 * 1024 + 123_457))
        .map(|index| (index as u8).wrapping_mul(23).wrapping_add(0x41))
        .collect();

    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "reader_open_during_upload", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!(
        "Local\\StudioFsReaderOpenDuringUploadSmoke-{}",
        std::process::id()
    );

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let target_file =
            std::path::PathBuf::from(drive_root).join("reader-open-during-upload.bin");
        let mut writer = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&target_file)
            .expect("open writer file through WinFSP mount");
        use std::io::{Read, Seek, SeekFrom, Write};
        writer
            .write_all(&payload)
            .expect("write payload through WinFSP mount");
        writer
            .sync_all()
            .expect("sync writer file through WinFSP mount");

        let attr = wait_for_lookup(
            &vfs_observer,
            b"reader-open-during-upload.bin",
            "reader-open-during-upload file",
            Duration::from_secs(5),
        );
        wait_for_pending_upload_observable(
            &vfs_observer,
            attr.ino,
            "reader-open-during-upload file",
            Duration::from_secs(5),
        );

        let mut reader = std::fs::File::open(&target_file)
            .expect("open reader handle during pending upload through WinFSP mount");
        let mut prefix = vec![0u8; 256 * 1024];
        reader
            .read_exact(&mut prefix)
            .expect("read prefix through reader handle during pending upload");
        assert_eq!(
            prefix,
            payload[..prefix.len()],
            "reader handle must observe correct prefix while upload is still pending"
        );

        wait_for_upload_ready(
            &vfs_observer,
            attr.ino,
            "reader-open-during-upload file",
            Duration::from_secs(45),
        );

        reader
            .seek(SeekFrom::Start(0))
            .expect("seek reader handle back to start after publish");
        let mut reopened_from_existing_reader = Vec::new();
        reader
            .read_to_end(&mut reopened_from_existing_reader)
            .expect("read through reader handle after publish");
        assert_eq!(
            reopened_from_existing_reader, payload,
            "reader handle opened during pending upload must remain readable after publish"
        );

        drop(reader);
        drop(writer);
    });
}

#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_mmap_reader_during_upload_stays_readable_after_finalize_smoke() {
    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    if !require_online_upload_backend(
        "winfsp_mount_mmap_reader_during_upload_stays_readable_after_finalize_smoke",
    ) {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP mmap-reader-during-upload smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let payload: Vec<u8> = (0..(18 * 1024 * 1024 + 65_537))
        .map(|index| (index as u8).wrapping_mul(29).wrapping_add(0x17))
        .collect();

    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "mmap_reader_during_upload", &rt);
    let vfs_observer = vfs.clone();
    let event_name = format!(
        "Local\\StudioFsMmapReaderDuringUploadSmoke-{}",
        std::process::id()
    );

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let target_file =
            std::path::PathBuf::from(drive_root).join("mmap-reader-during-upload.bin");
        std::fs::write(&target_file, &payload).expect("write mmap target through WinFSP mount");

        let attr = wait_for_lookup(
            &vfs_observer,
            b"mmap-reader-during-upload.bin",
            "mmap-reader-during-upload file",
            Duration::from_secs(5),
        );
        wait_for_pending_upload_observable(
            &vfs_observer,
            attr.ino,
            "mmap-reader-during-upload file",
            Duration::from_secs(5),
        );

        use std::os::windows::io::AsRawHandle;
        use windows::Win32::Foundation::{CloseHandle, HANDLE};
        use windows::Win32::System::Memory::{
            CreateFileMappingW, FILE_MAP_READ, MapViewOfFile, PAGE_READONLY, UnmapViewOfFile,
        };

        let reader = std::fs::File::open(&target_file)
            .expect("open mmap reader handle during pending upload through WinFSP mount");
        // SAFETY: `reader` is open for read and remains valid for mapping during this test.
        let mapping = unsafe {
            CreateFileMappingW(
                HANDLE(reader.as_raw_handle()),
                None,
                PAGE_READONLY,
                0,
                0,
                None,
            )
        }
        .expect("create read-only file mapping through WinFSP mount");
        // SAFETY: `mapping` is valid and the requested view maps exactly `payload.len()`
        // bytes to a readable region in this process.
        let view = unsafe { MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, payload.len()) };
        assert!(
            !view.Value.is_null(),
            "MapViewOfFile returned a null view for WinFSP mmap smoke"
        );

        // SAFETY: `view.Value` points into the mapped read-only region, and the
        // byte range is bounded to `payload.len()`.
        let mapped = unsafe { std::slice::from_raw_parts(view.Value as *const u8, payload.len()) };
        assert_eq!(
            &mapped[..256 * 1024],
            &payload[..256 * 1024],
            "memory-mapped reader must observe correct prefix while upload is still pending"
        );

        wait_for_upload_ready(
            &vfs_observer,
            attr.ino,
            "mmap-reader-during-upload file",
            Duration::from_secs(45),
        );

        std::thread::sleep(Duration::from_millis(1500));

        assert_eq!(
            mapped, payload,
            "memory-mapped reader opened during pending upload must remain readable after publish"
        );

        // SAFETY: The mapped view and handle are valid and no longer used below.
        unsafe {
            UnmapViewOfFile(view).expect("unmap WinFSP mmap view");
            CloseHandle(mapping).expect("close WinFSP mmap mapping handle");
        }
        drop(reader);
    });
}

/// Exercises the mounted write path with a tiny valid dirty budget and ensures
/// the runtime completes without hanging or surfacing WinFSP write errors.
///
/// Backpressure and gate-wait telemetry is printed to stderr for offline
/// analysis. Hosted runners can drain the tiny workload before counters cross
/// thresholds, so the mounted smoke keeps those counters diagnostic instead of
/// timing-sensitive pass/fail criteria.
///
/// The elapsed-time assertion serves as a hang-guard: if WinFSP dispatch threads
/// become exhausted waiting on the `local_sync_gate`, the write loop will stall
/// indefinitely rather than completing within the generous timeout.
#[test]
#[ignore = "requires WinFSP runtime + STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn winfsp_mount_write_wall_and_gate_contention_smoke() {
    use std::io::Write;
    use std::os::windows::fs::OpenOptionsExt;

    if std::env::var("STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS").as_deref() != Ok("1") {
        return;
    }
    let mount_point = match std::env::var("STUDIOFS_WINFSP_TEST_MOUNT_POINT") {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            eprintln!(
                "Skipping WinFSP write-wall gate-contention smoke test: \
                 set STUDIOFS_WINFSP_TEST_MOUNT_POINT to a free drive letter (for example `Z:`)."
            );
            return;
        }
    };

    // Use write-through/no-buffering so WinFSP receives deterministic aligned
    // chunks. Hosted Windows cached writes can otherwise arrive at the callback
    // layer as larger coalesced requests and trip the writer-stack buffer limit
    // before this smoke reaches the mounted write-wall path.
    const CHUNK_BYTES: usize = 512 * 1024;
    const CHUNK_COUNT: usize = 16;
    const MAX_DIRTY_BYTES: usize = 4 * 1024 * 1024;
    const TOTAL_BYTES: usize = CHUNK_BYTES * CHUNK_COUNT;
    const MAX_ELAPSED_SECS: u64 = 120;

    let tmp = studiofs_test_support::workspace_target_tempdir("test-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));

    // Build VFS inline with a tiny valid dirty cap so the hard-backpressure guard
    // can fire when mounted writes outrun deferred materialization.
    // This bypasses make_test_vfs(), which hardcodes 300 MiB via VfsConfig::default().
    let vfs = {
        use studiofs_meta::RedbMetaEngine;
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");
        let mut format_info = test_format_info("write_wall_gate_contention");
        format_info.block_size = CHUNK_BYTES as u32;
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);
        let cache_dir = tmp.path().join("cache");
        let _guard = rt.enter();
        let vfs_config = VfsConfigRaw {
            max_dirty_bytes: Some(MAX_DIRTY_BYTES),
            ..VfsConfigRaw::default()
        }
        .validate()
        .expect("wall test VFS config should validate");
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            vfs_config,
        )
        .expect("create wall test vfs");
        vfs.start().expect("start vfs background workers");
        vfs
    };

    let vfs_observer = vfs.clone();
    let event_name = format!("Local\\StudioFsWriteWallGateSmoke-{}", std::process::id());

    with_winfsp_mount(vfs, rt, mount_point, event_name, |drive_root| {
        let snapshot_before = vfs_observer.write_path_observability_snapshot();

        let sequential_file =
            std::path::PathBuf::from(drive_root).join("write-wall-gate-contention.bin");
        let mut writer = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .custom_flags(AJA_CACHE_OFF_OPEN_OPTIONS_FLAGS)
            .open(&sequential_file)
            .expect("open write-wall gate-contention file through WinFSP mount");

        let started_at = std::time::Instant::now();

        for chunk_index in 0..CHUNK_COUNT {
            let fill = b'A' + (chunk_index % 26) as u8;
            let chunk = vec![fill; CHUNK_BYTES];
            if let Err(err) = writer.write_all(&chunk) {
                let snapshot = vfs_observer.write_path_observability_snapshot();
                panic!(
                    "write chunk {chunk_index} through WinFSP mount (write-wall test): {err}; \
                     winfsp_avg_write_bytes={:.0} vfs_avg_write_bytes={:.0} \
                     writer_stack_dirty_bytes={} total_dirty_bytes={} dirty_limit_bytes={} \
                     hard_backpressure_events={}",
                    snapshot.winfsp_write_path.avg_write_bytes,
                    snapshot.vfs_write_path.avg_write_bytes,
                    snapshot.dirty_pressure.writer_stack_dirty_bytes,
                    snapshot.dirty_pressure.total_dirty_bytes,
                    snapshot.dirty_pressure.dirty_limit_bytes,
                    snapshot.write_backpressure.hard_events
                );
            }
        }
        writer
            .flush()
            .expect("flush write-wall gate-contention writer through WinFSP mount");
        // `sync_all()` calls `FlushFileBuffers` on Windows. WinFSP delivers the Flush
        // IRP synchronously to the user-mode filesystem callback, which runs
        // `flush_fh_with_outcome` with `Strict` policy (inode has pending dirty slices
        // from the fallback-path materialisations above). This increments
        // `payload_store_sync_data.calls` *before* `sync_all()` returns, so the
        // snapshot taken below will always observe delta_sync_calls > 0.
        writer
            .sync_all()
            .expect("sync_all write-wall gate-contention file through WinFSP mount");
        drop(writer);

        let elapsed = started_at.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        let throughput_mib_per_sec = (TOTAL_BYTES as f64 / (1024.0 * 1024.0)) / elapsed_secs;

        let snapshot_after = vfs_observer.write_path_observability_snapshot();
        let delta_hard_events = snapshot_after.write_backpressure.hard_events
            - snapshot_before.write_backpressure.hard_events;
        let delta_sync_calls = snapshot_after.payload_store_sync_data.calls
            - snapshot_before.payload_store_sync_data.calls;

        eprintln!(
            "write-wall gate-contention smoke: {:.2} MiB in {:.3} s ({:.2} MiB/s) | \
             hard_backpressure_events=+{delta_hard_events} \
             sync_calls=+{delta_sync_calls} \
             queue_wait_max_ms={:.3} \
             queue_wait_avg_ms={:.3}",
            TOTAL_BYTES as f64 / (1024.0 * 1024.0),
            elapsed_secs,
            throughput_mib_per_sec,
            snapshot_after.payload_store_sync_data.queue_wait_max_ms,
            snapshot_after.payload_store_sync_data.queue_wait_avg_ms,
        );

        assert_eq!(
            std::fs::metadata(&sequential_file)
                .expect("metadata for write-wall gate-contention file")
                .len(),
            TOTAL_BYTES as u64,
            "write-wall gate-contention file length should match the mounted write workload"
        );

        // Hang guard. If WinFSP dispatch threads block indefinitely on the
        // local_sync_gate the write loop will never return. Exceeding this timeout
        // indicates a dispatch-thread exhaustion deadlock.
        assert!(
            elapsed.as_secs() < MAX_ELAPSED_SECS,
            "write-wall gate-contention smoke exceeded {MAX_ELAPSED_SECS} s \
             ({elapsed_secs:.3} s elapsed) — this may indicate a WinFSP \
             dispatch-thread deadlock via the local_sync_gate",
        );
    });
}
