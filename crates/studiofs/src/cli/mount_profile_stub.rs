use crate::cli::mount_profile_types::MountProfileMode;
use crate::diagnostics::{
    ProfilingBackendStartResult, RuntimeProfilingBackend, StartProfilingWindowRequest,
};
use crate::investigation::ProfilingCaptureMode;
use std::path::Path;
use std::sync::Arc;

pub struct MountProfileSession;

impl MountProfileSession {
    pub fn start(
        _mode: Option<MountProfileMode>,
        _volume_name: &str,
        _platform: &str,
        _bridge: &str,
        _mount_mode: &str,
        _cache_mode: &str,
        _cache_path: &Path,
    ) -> Option<Self> {
        None
    }

    pub fn finish(self) {}
}

struct UnsupportedRuntimeProfilingBackend;

impl RuntimeProfilingBackend for UnsupportedRuntimeProfilingBackend {
    fn supported_capture_modes(&self) -> Vec<ProfilingCaptureMode> {
        vec![
            ProfilingCaptureMode::WallBlocking,
            ProfilingCaptureMode::Cpu,
        ]
    }

    fn start_window(&self, _request: &StartProfilingWindowRequest) -> ProfilingBackendStartResult {
        ProfilingBackendStartResult::Unsupported {
            error: "profiling backend unavailable on this platform".to_string(),
        }
    }
}

pub(crate) fn runtime_profiling_backend(
    _volume_name: &str,
    _platform: &str,
    _bridge: &str,
    _mount_mode: &str,
    _cache_mode: &str,
    _cache_path: &Path,
) -> Arc<dyn RuntimeProfilingBackend> {
    Arc::new(UnsupportedRuntimeProfilingBackend)
}
