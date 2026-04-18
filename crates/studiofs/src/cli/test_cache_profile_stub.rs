use crate::cli::test_cache_profile_common::collect_write_path_artifacts;
use crate::cli::test_cache_profile_types::{TestCacheProfileMode, TestCacheProfileResult};
use anyhow::Result;
use std::path::{Path, PathBuf};

pub struct TestCacheProfileSession {
    run_id: String,
    benchmark_mode: String,
    write_path_observability_path: Option<PathBuf>,
    warnings: Vec<String>,
}

impl TestCacheProfileSession {
    pub fn start(
        mode: Option<TestCacheProfileMode>,
        run_id: &str,
        benchmark_mode: &str,
        write_path_observability_path: Option<PathBuf>,
    ) -> Result<Option<Self>> {
        if mode.is_none() {
            return Ok(None);
        }

        Ok(Some(Self {
            run_id: run_id.to_string(),
            benchmark_mode: benchmark_mode.to_string(),
            write_path_observability_path,
            warnings: vec![
                "CPU and memory profiling are not supported on Windows; exporting write-path observability artifacts only.".to_string(),
            ],
        }))
    }

    pub fn finish(mut self, output_dir: &Path) -> Result<TestCacheProfileResult> {
        let artifacts = collect_write_path_artifacts(
            output_dir,
            &self.run_id,
            &self.benchmark_mode,
            self.write_path_observability_path.as_deref(),
            &mut self.warnings,
        )?;

        Ok(TestCacheProfileResult {
            endpoint: None,
            artifacts,
            warnings: self.warnings,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::TestCacheProfileSession;
    use crate::cli::test_cache_profile_types::TestCacheProfileMode;
    use studiofs_vfs::{
        VfsWritePathObservabilitySnapshot, VfsWritePathWinfspCallbackStats,
        VfsWritePathWinfspForegroundFlushStats,
    };

    #[test]
    fn windows_stub_exports_write_path_summary_when_profile_requested() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            backend_tag: "local-block-runtime".to_string(),
            winfsp_callbacks: VfsWritePathWinfspCallbackStats {
                cleanup: 2,
                close: 1,
                ..Default::default()
            },
            winfsp_foreground_flushes: VfsWritePathWinfspForegroundFlushStats {
                close: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let temp = tempfile::tempdir().expect("tempdir");
        let metrics_path = temp.path().join("write_path_observability.json");
        std::fs::write(
            &metrics_path,
            serde_json::to_vec(&snapshot).expect("serialize snapshot"),
        )
        .expect("write metrics snapshot");

        let session = TestCacheProfileSession::start(
            Some(TestCacheProfileMode::Pyroscope),
            "run-123",
            "mounted",
            Some(metrics_path),
        )
        .expect("start Windows profile session")
        .expect("profile mode should still start a Windows export session");

        let result = session
            .finish(temp.path())
            .expect("finish Windows profile session");

        assert!(
            result.endpoint.is_none(),
            "Windows stub should not report a profiler endpoint"
        );
        assert_eq!(
            result.artifacts.len(),
            2,
            "JSON + summary artifacts should be exported"
        );
        assert!(
            result.warnings.iter().any(|warning| {
                warning.contains("CPU and memory profiling are not supported on Windows")
            }),
            "Windows stub should explain why only write-path artifacts were exported: {:?}",
            result.warnings
        );

        let summary_artifact = result
            .artifacts
            .iter()
            .find(|artifact| {
                artifact.kind
                    == crate::cli::test_cache_profile_types::ProfileArtifactKind::WritePathSummary
            })
            .expect("write-path summary artifact");
        let summary =
            std::fs::read_to_string(&summary_artifact.path).expect("read write-path summary");
        assert!(summary.contains("winfsp_validation: cleanup_non_delete=cleanup-deferred"));
    }
}
