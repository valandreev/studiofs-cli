import os

path = r'e:\dev\studiofs-cli\crates\studiofs-vfs\tests\vfs_test.rs'
with open(path, 'rb') as f:
    text = f.read().decode('utf-8')

struct_code = '''

/// Blanket env-var isolation guard for VFS integration tests.
#[allow(dead_code)]
struct ScopedTestEnv {
    _guards: Vec<ScopedEnvVar>,
}

#[allow(dead_code)]
impl ScopedTestEnv {
    fn new() -> Self {
        const KEYS: &[&str] = &[
            "STUDIOFS_S3_ENDPOINT", "STUDIOFS_S3_BUCKET", "STUDIOFS_S3_ACCESS_KEY", "STUDIOFS_S3_SECRET_KEY",
            "STUDIOFS_MAX_DIRTY_BYTES", "STUDIOFS_MAX_SPILL_BYTES", "STUDIOFS_INGRESS_SEGMENT_MAX_BYTES",
            "STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH", "STUDIOFS_SLAB_FLUSH_INTERVAL_MS", "STUDIOFS_FOREGROUND_DRAIN_ASSIST",
            "STUDIOFS_UPLOAD_DELAY_MS", "STUDIOFS_UPLOAD_REQUEUE_BASE_MS", "STUDIOFS_UPLOAD_REQUEUE_MAX_MS",
            "STUDIOFS_UPLOAD_STATS_INTERVAL_SECS", "STUDIOFS_UPLOAD_WORKER_COUNT", "STUDIOFS_UPLOAD_MAX_SPAWNED",
            "STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS", "STUDIOFS_UPLOAD_MAX_ATTEMPTS", "STUDIOFS_UPLOAD_CACHE_IO_SLOTS",
            "STUDIOFS_UPLOAD_CACHE_IO_USER_PENALTY", "STUDIOFS_UPLOAD_USER_IDLE_GRACE_MS",
            "STUDIOFS_UPLOAD_INITIAL_WINDOW", "STUDIOFS_UPLOAD_MIN_WINDOW", "STUDIOFS_UPLOAD_MAX_WINDOW",
            "STUDIOFS_UPLOAD_INCREASE_STEP", "STUDIOFS_UPLOAD_DECREASE_FACTOR", "STUDIOFS_UPLOAD_DECREASE_COOLDOWN_SECS",
            "STUDIOFS_FETCH_MAX_ATTEMPTS", "STUDIOFS_FETCH_REQUEST_TIMEOUT_SECS", "STUDIOFS_FETCH_INITIAL_WINDOW",
            "STUDIOFS_FETCH_MIN_WINDOW", "STUDIOFS_FETCH_MAX_WINDOW", "STUDIOFS_FETCH_INCREASE_STEP",
            "STUDIOFS_FETCH_DECREASE_FACTOR", "STUDIOFS_FETCH_DECREASE_COOLDOWN_SECS",
            "STUDIOFS_QOS_ACCEPT_SLOTS", "STUDIOFS_QOS_MATERIALIZE_SLOTS", "STUDIOFS_QOS_UPLOAD_SLOTS",
            "STUDIOFS_QOS_COMMIT_SLOTS", "STUDIOFS_QOS_FETCH_SLOTS",
            "STUDIOFS_READ_BUFFER_MB", "STUDIOFS_PREFETCH_CONCURRENCY", "STUDIOFS_READAHEAD_WIDTH",
            "STUDIOFS_READAHEAD_MIN_PREFETCH", "STUDIOFS_READAHEAD_SEQ_STEP", "STUDIOFS_READAHEAD_RANDOM_PENALTY",
            "STUDIOFS_DIR_PREFETCH_MAX_BATCH", "STUDIOFS_READDIR_ATTR_BATCH_CHUNK", "STUDIOFS_READDIR_ATTR_BATCH_MAX_INODES",
            "STUDIOFS_PREFETCH_FOREGROUND_RESERVE", "STUDIOFS_PREFETCH_MAX_PER_CYCLE", "STUDIOFS_PREFETCH_PAUSE_ON_NETERR_MS",
            "STUDIOFS_INGRESS_CHECKPOINT_INTERVAL_SECS", "STUDIOFS_INGRESS_CHECKPOINT_MIN_APPEND_BYTES",
            "STUDIOFS_INGRESS_CHECKPOINT_MIN_MUTATIONS", "STUDIOFS_INGRESS_CHECKPOINT_MAX_SEGMENT_LAG",
            "STUDIOFS_INGRESS_CHECKPOINT_MAX_LIVE_SEGMENTS",
            "STUDIOFS_JANITOR_INTERVAL_SECS", "STUDIOFS_JANITOR_SKIP_NEWER_SECS", "STUDIOFS_JANITOR_MAX_DELETES",
            "STUDIOFS_JANITOR_LEASE_TTL_SECS",
            "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS", "STUDIOFS_DENTRY_CACHE_CAPACITY", "STUDIOFS_DENTRY_CACHE_BYTES",
            "STUDIOFS_PATH_SHORTCUT_CACHE_CAPACITY", "STUDIOFS_PATH_SHORTCUT_CACHE_BYTES",
            "STUDIOFS_ATTR_READ_CACHE_CAPACITY", "STUDIOFS_ATTR_READ_CACHE_BYTES",
            "STUDIOFS_LOCK_TTL_SECS", "STUDIOFS_LOCK_RETRY_BASE_MS", "STUDIOFS_LOCK_RETRY_MAX_MS",
            "STUDIOFS_SHUTDOWN_METADATA_FLUSH_TIMEOUT_SECS",
        ];
        Self { _guards: KEYS.iter().map(|&k| ScopedEnvVar::capture(k)).collect() }
    }
}'''

target1 = '''impl Drop for ScopedEnvVar {
    fn drop(&mut self) {
        unsafe {
            if let Some(value) = &self.old {
                std::env::set_var(self.key, value);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }
}'''

# Replace with properly preserving line endings of target
if '\r\n' in text:
    struct_code = struct_code.replace('\n', '\r\n')
    target1 = target1.replace('\n', '\r\n')

text = text.replace(target1, target1 + struct_code)

target2 = '''#[test]
fn upload_completion_batching_does_not_change_visibility_contract() {
    let _restore_dirty = ScopedEnvVar::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = ScopedEnvVar::capture("STUDIOFS_MAX_SPILL_BYTES");
    let _restore_endpoint = ScopedEnvVar::capture("STUDIOFS_S3_ENDPOINT");
    let _restore_bucket = ScopedEnvVar::capture("STUDIOFS_S3_BUCKET");
    let _restore_access_key = ScopedEnvVar::capture("STUDIOFS_S3_ACCESS_KEY");
    let _restore_secret_key = ScopedEnvVar::capture("STUDIOFS_S3_SECRET_KEY");
    let _restore_upload_timeout = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS");
    let _restore_requeue_base = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_BASE_MS");
    let _restore_requeue_max = ScopedEnvVar::capture("STUDIOFS_UPLOAD_REQUEUE_MAX_MS");
    let _restore_upload_delay = ScopedEnvVar::capture("STUDIOFS_UPLOAD_DELAY_MS");
    let _restore_upload_workers = ScopedEnvVar::capture("STUDIOFS_UPLOAD_WORKER_COUNT");'''

replacement2 = '''#[test]
fn upload_completion_batching_does_not_change_visibility_contract() {
    let _env = ScopedTestEnv::new();'''

if '\r\n' in text:
    target2 = target2.replace('\n', '\r\n')
    replacement2 = replacement2.replace('\n', '\r\n')

text = text.replace(target2, replacement2)

target3 = '''#[test]
fn test_ingress_backpressure_wait_time_accumulates_until_drain() {
    type EnvRestore = ScopedEnvVar;

    let _restore_dirty = EnvRestore::capture("STUDIOFS_MAX_DIRTY_BYTES");
    let _restore_spill = EnvRestore::capture("STUDIOFS_MAX_SPILL_BYTES");
    unsafe {
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "512KB");
    }'''

replacement3 = '''#[test]
fn test_ingress_backpressure_wait_time_accumulates_until_drain() {
    let _env = ScopedTestEnv::new();
    unsafe {
        std::env::remove_var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH");
        std::env::set_var("STUDIOFS_MAX_DIRTY_BYTES", "1");
        std::env::set_var("STUDIOFS_MAX_SPILL_BYTES", "512KB");
    }'''

if '\r\n' in text:
    target3 = target3.replace('\n', '\r\n')
    replacement3 = replacement3.replace('\n', '\r\n')

text = text.replace(target3, replacement3)

with open(path, 'wb') as f:
    f.write(text.encode('utf-8'))
print('File written successfully')
