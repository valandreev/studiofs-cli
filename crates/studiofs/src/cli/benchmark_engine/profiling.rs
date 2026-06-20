use std::time::Duration;

use super::result::{
    BenchmarkProfileReport, BenchmarkReport, CpuProfileReport, ProfileArtifactSummary,
    ProfileEndpointReport,
};
use crate::cli::benchmark_profile_types::BenchmarkProfileResult;

pub(crate) fn process_cpu_time() -> Option<Duration> {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: `getrusage` initializes the provided pointer when it returns 0.
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if rc != 0 {
            return None;
        }
        // SAFETY: guarded by successful `getrusage` above.
        let usage = unsafe { usage.assume_init() };
        let user = Duration::new(
            usage.ru_utime.tv_sec as u64,
            (usage.ru_utime.tv_usec as u32).saturating_mul(1_000),
        );
        let sys = Duration::new(
            usage.ru_stime.tv_sec as u64,
            (usage.ru_stime.tv_usec as u32).saturating_mul(1_000),
        );
        Some(user.saturating_add(sys))
    }

    #[cfg(windows)]
    {
        #[repr(C)]
        #[derive(Default)]
        struct FileTime {
            dw_low_date_time: u32,
            dw_high_date_time: u32,
        }
        unsafe extern "system" {
            fn GetCurrentProcess() -> *mut std::ffi::c_void;
            fn GetProcessTimes(
                hProcess: *mut std::ffi::c_void,
                lpCreationTime: *mut FileTime,
                lpExitTime: *mut FileTime,
                lpKernelTime: *mut FileTime,
                lpUserTime: *mut FileTime,
            ) -> i32;
        }
        let mut creation = FileTime::default();
        let mut exit = FileTime::default();
        let mut kernel = FileTime::default();
        let mut user = FileTime::default();
        // SAFETY: pointers are valid and writable for this call.
        let ok = unsafe {
            GetProcessTimes(
                GetCurrentProcess(),
                &mut creation,
                &mut exit,
                &mut kernel,
                &mut user,
            )
        };
        if ok == 0 {
            tracing::warn!(
                target: "studiofs.cache.profile",
                "GetProcessTimes failed; CPU profile sample unavailable on this call"
            );
            return None;
        }
        let ft_to_u64 =
            |ft: FileTime| ((ft.dw_high_date_time as u64) << 32) | ft.dw_low_date_time as u64;
        let total_100ns = ft_to_u64(kernel).saturating_add(ft_to_u64(user));
        Some(Duration::from_nanos(total_100ns.saturating_mul(100)))
    }

    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}

pub(crate) fn cpu_utilization_percent(cpu: Duration, wall: Duration) -> f64 {
    let wall_ns = wall.as_nanos() as f64;
    if wall_ns <= 0.0 {
        0.0
    } else {
        (cpu.as_nanos() as f64 / wall_ns) * 100.0
    }
}

pub(crate) fn merge_profile_report(
    mut report: BenchmarkReport,
    profile_report: Option<BenchmarkProfileResult>,
) -> BenchmarkReport {
    let Some(profile_report) = profile_report else {
        return report;
    };

    let artifacts = profile_report
        .artifacts
        .into_iter()
        .map(|artifact| ProfileArtifactSummary {
            kind: artifact.kind,
            path: artifact.path,
            bytes: artifact.bytes,
            sent_to_endpoint: artifact.sent_to_endpoint,
        })
        .collect();

    let profile_mode = "pyroscope".to_string();
    let endpoint = profile_report
        .endpoint
        .map(|endpoint| ProfileEndpointReport {
            url: endpoint.url,
            application_name: endpoint.application_name,
        });

    match report.profile.take() {
        Some(existing) => {
            report.profile = Some(BenchmarkProfileReport {
                enabled: existing.enabled,
                profile_mode,
                cpu_profile: existing.cpu_profile,
                endpoint,
                artifacts,
                warnings: profile_report.warnings,
            });
        }
        None => {
            report.profile = Some(BenchmarkProfileReport {
                enabled: true,
                profile_mode,
                cpu_profile: CpuProfileReport {
                    total_wall_ms: 0.0,
                    total_cpu_ms: 0.0,
                    cpu_utilization_percent: 0.0,
                    write_phase_cpu_percent: 0.0,
                    read_phase_cpu_percent: 0.0,
                },
                endpoint,
                artifacts,
                warnings: profile_report.warnings,
            });
        }
    }

    report
}
