use crate::cli::benchmark_profile_common::collect_write_path_artifacts;
use crate::cli::benchmark_profile_types::{BenchmarkProfileMode, BenchmarkProfileResult};
use anyhow::Result;
use std::path::{Path, PathBuf};

pub struct BenchmarkProfileSession {
    run_id: String,
    benchmark_mode: String,
    write_path_observability_path: Option<PathBuf>,
    warnings: Vec<String>,
}

impl BenchmarkProfileSession {
    pub fn start(
        mode: Option<BenchmarkProfileMode>,
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
                "Native CPU and memory pprof are not supported on Windows; portable process and allocator telemetry are captured in mounted_portable_profile.".to_string(),
            ],
        }))
    }

    pub fn finish(mut self, output_dir: &Path) -> Result<BenchmarkProfileResult> {
        let artifacts = collect_write_path_artifacts(
            output_dir,
            &self.run_id,
            &self.benchmark_mode,
            self.write_path_observability_path.as_deref(),
            &mut self.warnings,
        )?;

        Ok(BenchmarkProfileResult {
            endpoint: None,
            artifacts,
            warnings: self.warnings,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::BenchmarkProfileSession;
    use crate::cli::benchmark_profile_types::BenchmarkProfileMode;
    use studiofs_vfs::{
        VfsWritePathObservabilitySnapshot, VfsWritePathWinfspCallbackStats,
        VfsWritePathWinfspForegroundFlushStats,
    };

    #[test]
    fn windows_stub_exports_write_path_summary_when_profile_requested() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            backend_tag: "local-payload-runtime".to_string(),
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
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let metrics_path = temp.path().join("write_path_observability.json");
        std::fs::write(
            &metrics_path,
            serde_json::to_vec(&snapshot).expect("serialize snapshot"),
        )
        .expect("write metrics snapshot");

        let session = BenchmarkProfileSession::start(
            Some(BenchmarkProfileMode::Pyroscope),
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
                warning.contains("Native CPU and memory pprof are not supported on Windows")
            }),
            "Windows stub should explain that portable telemetry replaces native pprof: {:?}",
            result.warnings
        );

        let summary_artifact = result
            .artifacts
            .iter()
            .find(|artifact| {
                artifact.kind
                    == crate::cli::benchmark_profile_types::ProfileArtifactKind::WritePathSummary
            })
            .expect("write-path summary artifact");
        let summary =
            std::fs::read_to_string(&summary_artifact.path).expect("read write-path summary");
        assert!(summary.contains("winfsp_validation: cleanup_non_delete=cleanup-deferred"));
    }
}
