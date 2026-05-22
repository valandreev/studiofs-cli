//! Real mounted E2E runner skeleton for StudioFS.

pub mod baseline_gate;
pub mod cli;
pub mod client;
pub mod infra;
pub mod mounted_tree;
mod process_diagnostics;
mod process_job;
pub mod report;
pub mod runner;
pub mod scenario;

#[cfg(test)]
mod process_diagnostics_tests {
    use tempfile::tempdir;

    #[test]
    fn process_diagnostics_captures_current_process_metadata() {
        let _guard = crate::process_job::child_job_test_lock();
        let temp = tempdir().unwrap();
        crate::process_diagnostics::set_process_diagnostics_dir(temp.path().join("diagnostics"))
            .unwrap();

        let artifact = crate::process_diagnostics::capture_process_diagnostics(
            std::process::id(),
            "test current process",
        )
        .unwrap()
        .expect("configured diagnostics dir should produce an artifact");

        assert!(artifact.metadata_path.exists());
        let metadata = std::fs::read_to_string(artifact.metadata_path).unwrap();
        assert!(metadata.contains("\"pid\""));
        assert!(metadata.contains("\"test current process\""));
        assert!(
            !metadata.contains("\"missing_metadata\""),
            "supervising process and dump helper should agree on the artifact name"
        );
    }
}
