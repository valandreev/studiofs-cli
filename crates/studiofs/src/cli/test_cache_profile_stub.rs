use crate::cli::test_cache_profile_types::{TestCacheProfileMode, TestCacheProfileResult};
use anyhow::Result;
use std::path::Path;

pub struct TestCacheProfileSession;

impl TestCacheProfileSession {
    pub fn start(
        _mode: Option<TestCacheProfileMode>,
        _run_id: &str,
        _benchmark_mode: &str,
    ) -> Result<Option<Self>> {
        Ok(None)
    }

    pub fn finish(self, _output_dir: &Path) -> Result<TestCacheProfileResult> {
        Ok(TestCacheProfileResult {
            endpoint: None,
            artifacts: Vec::new(),
            warnings: vec!["Profiling is not supported on Windows.".to_string()],
        })
    }
}
