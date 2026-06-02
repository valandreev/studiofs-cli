//! Low-level macFUSE repeated-read probe report model.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Probe input mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProbeMode {
    /// Synthetic in-memory file exposed by the probe filesystem.
    Memory,
    /// Local file exposed through passthrough reads.
    LocalFile,
}

/// Low-level probe configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeConfig {
    /// File size exposed by each probe mode.
    pub file_bytes: u64,
    /// Read buffer size used by the client side measurement loop.
    pub chunk_bytes: usize,
    /// Number of repeated-read passes to run for each mode.
    pub read_passes: u32,
    /// Probe modes to execute.
    pub modes: Vec<ProbeMode>,
    /// Whether to run the notify/store experiment when supported by the API.
    pub notify_store_experiment: bool,
}

impl ProbeConfig {
    /// Validate SFS-479 completion gates that can be checked before mounting.
    pub fn validate(&self) -> Result<(), ProbeConfigError> {
        const MIN_READ_PASSES: u32 = 3;
        if self.read_passes < MIN_READ_PASSES {
            return Err(ProbeConfigError::ReadPassesBelowMinimum {
                actual: self.read_passes,
                minimum: MIN_READ_PASSES,
            });
        }
        for required in [ProbeMode::Memory, ProbeMode::LocalFile] {
            if !self.modes.contains(&required) {
                return Err(ProbeConfigError::MissingRequiredMode(required));
            }
        }
        Ok(())
    }
}

/// Probe configuration validation error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeConfigError {
    /// Required probe mode is missing.
    MissingRequiredMode(ProbeMode),
    /// Repeated-read passes are below the SFS-479 minimum.
    ReadPassesBelowMinimum {
        /// Configured pass count.
        actual: u32,
        /// Required minimum pass count.
        minimum: u32,
    },
}

/// Raw measurements collected for one repeated-read pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPassObservation {
    /// One-based read pass number.
    pub pass: u32,
    /// Bytes delivered to the reader.
    pub bytes_read: u64,
    /// Elapsed wall-clock time in nanoseconds.
    pub elapsed_ns: u128,
    /// Number of FUSE read callbacks that re-entered userspace.
    pub callback_count: u64,
    /// Bytes returned through FUSE read callbacks.
    pub callback_bytes: u64,
    /// Distribution of read callback response sizes.
    pub callback_size_distribution: BTreeMap<u64, u64>,
}

/// Serializable per-pass report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProbeReadPassReport {
    /// One-based read pass number.
    pub pass: u32,
    /// Bytes delivered to the reader.
    pub bytes_read: u64,
    /// Elapsed wall-clock time in nanoseconds.
    pub elapsed_ns: u128,
    /// Throughput in MiB/s.
    pub throughput_mib_s: f64,
    /// Number of FUSE read callbacks that re-entered userspace.
    pub callback_count: u64,
    /// Bytes returned through FUSE read callbacks.
    pub callback_bytes: u64,
    /// Distribution of read callback response sizes.
    pub callback_size_distribution: BTreeMap<u64, u64>,
    /// Whether this pass re-entered userspace for file data.
    pub reentered_userspace: bool,
}

/// Cache behavior inferred from repeated-read callback observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KernelCacheInterpretation {
    /// Later passes avoided FUSE read callbacks after the first pass populated cache.
    KernelCacheHit,
    /// Later passes still re-entered FUSE for file data.
    ReenteredUserspace,
    /// The observations are incomplete or internally inconsistent.
    Inconclusive,
}

/// Serializable report for one probe mode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProbeModeReport {
    /// Probe input mode.
    pub mode: ProbeMode,
    /// Per-pass repeated-read measurements.
    pub read_passes: Vec<ProbeReadPassReport>,
    /// Total number of read callbacks across all passes.
    pub total_callback_count: u64,
    /// Total callback bytes across all passes.
    pub total_callback_bytes: u64,
    /// Combined read callback response-size distribution.
    pub callback_size_distribution: BTreeMap<u64, u64>,
    /// Whether macFUSE exposes notify/store for this probe path.
    pub notify_store_supported: bool,
    /// Kernel-cache interpretation for this mode.
    pub kernel_cache_interpretation: KernelCacheInterpretation,
}

/// Go/no-go guidance for investing in a full low-level macFUSE bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoNoGoGuidance {
    /// Low-level macFUSE avoided repeated userspace reads in every required mode.
    Go,
    /// At least one required mode still re-entered userspace on repeated reads.
    NoGo,
    /// The probe did not collect enough data to decide.
    Inconclusive,
}

/// Complete SFS-479 low-level probe report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProbeReport {
    /// Per-mode reports.
    pub modes: Vec<ProbeModeReport>,
    /// Minimum repeated-read passes observed for any mode.
    pub min_read_passes: u32,
    /// Human-readable kernel-cache summary.
    pub kernel_cache_summary: String,
    /// Go/no-go guidance for full bridge implementation.
    pub go_no_go: GoNoGoGuidance,
}

impl ProbeReport {
    /// Build a complete probe report from per-mode results.
    pub fn new(modes: Vec<ProbeModeReport>) -> Self {
        let min_read_passes = modes
            .iter()
            .map(|mode| mode.read_passes.len() as u32)
            .min()
            .unwrap_or(0);
        let go_no_go = if min_read_passes < 3 || modes.len() < 2 {
            GoNoGoGuidance::Inconclusive
        } else if modes.iter().all(|mode| {
            mode.kernel_cache_interpretation == KernelCacheInterpretation::KernelCacheHit
        }) {
            GoNoGoGuidance::Go
        } else {
            GoNoGoGuidance::NoGo
        };
        let kernel_cache_summary = kernel_cache_summary(&modes);

        Self {
            modes,
            min_read_passes,
            kernel_cache_summary,
            go_no_go,
        }
    }
}

/// Build a serializable report from raw repeated-read observations.
pub fn summarize_mode(
    mode: ProbeMode,
    notify_store_supported: bool,
    observations: Vec<ReadPassObservation>,
) -> ProbeModeReport {
    let mut total_callback_count = 0;
    let mut total_callback_bytes = 0;
    let mut combined_distribution = BTreeMap::new();
    let mut read_passes = Vec::with_capacity(observations.len());

    for observation in observations {
        total_callback_count += observation.callback_count;
        total_callback_bytes += observation.callback_bytes;
        for (size, count) in &observation.callback_size_distribution {
            *combined_distribution.entry(*size).or_insert(0) += *count;
        }
        read_passes.push(ProbeReadPassReport {
            pass: observation.pass,
            bytes_read: observation.bytes_read,
            elapsed_ns: observation.elapsed_ns,
            throughput_mib_s: throughput_mib_s(observation.bytes_read, observation.elapsed_ns),
            callback_count: observation.callback_count,
            callback_bytes: observation.callback_bytes,
            callback_size_distribution: observation.callback_size_distribution,
            reentered_userspace: observation.callback_count > 0,
        });
    }

    let kernel_cache_interpretation = interpret_kernel_cache(&read_passes);
    ProbeModeReport {
        mode,
        read_passes,
        total_callback_count,
        total_callback_bytes,
        callback_size_distribution: combined_distribution,
        notify_store_supported,
        kernel_cache_interpretation,
    }
}

fn kernel_cache_summary(modes: &[ProbeModeReport]) -> String {
    modes
        .iter()
        .map(|mode| {
            let mode_name = match mode.mode {
                ProbeMode::Memory => "memory",
                ProbeMode::LocalFile => "local-file",
            };
            match mode.kernel_cache_interpretation {
                KernelCacheInterpretation::KernelCacheHit => {
                    format!("{mode_name} repeated reads avoided userspace callbacks")
                }
                KernelCacheInterpretation::ReenteredUserspace => {
                    format!("{mode_name} repeated reads re-entered userspace")
                }
                KernelCacheInterpretation::Inconclusive => {
                    format!("{mode_name} repeated reads were inconclusive")
                }
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn throughput_mib_s(bytes_read: u64, elapsed_ns: u128) -> f64 {
    if elapsed_ns == 0 {
        return 0.0;
    }
    let mib = bytes_read as f64 / 1_048_576.0;
    let seconds = elapsed_ns as f64 / 1_000_000_000.0;
    mib / seconds
}

fn interpret_kernel_cache(read_passes: &[ProbeReadPassReport]) -> KernelCacheInterpretation {
    let Some((first, rest)) = read_passes.split_first() else {
        return KernelCacheInterpretation::Inconclusive;
    };
    if read_passes.len() < 3 || first.callback_count == 0 {
        return KernelCacheInterpretation::Inconclusive;
    }
    if rest.iter().all(|pass| pass.callback_count == 0) {
        KernelCacheInterpretation::KernelCacheHit
    } else {
        KernelCacheInterpretation::ReenteredUserspace
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_read_report_records_callback_distribution_and_cache_interpretation() {
        let report = summarize_mode(
            ProbeMode::Memory,
            false,
            vec![
                observation(1, 3, 3 * 1024 * 1024),
                observation(2, 0, 0),
                observation(3, 0, 0),
            ],
        );

        assert_eq!(report.mode, ProbeMode::Memory);
        assert!(!report.notify_store_supported);
        assert_eq!(
            report.kernel_cache_interpretation,
            KernelCacheInterpretation::KernelCacheHit
        );
        assert_eq!(report.total_callback_count, 3);
        assert_eq!(report.total_callback_bytes, 3 * 1024 * 1024);
        assert_eq!(
            report.callback_size_distribution.get(&(1024 * 1024)),
            Some(&3)
        );
        assert!(report.read_passes[0].reentered_userspace);
        assert!(!report.read_passes[1].reentered_userspace);
        assert_eq!(report.read_passes[0].throughput_mib_s, 30.0);
    }

    #[test]
    fn probe_report_requires_memory_and_local_file_results_before_go_guidance() {
        let memory = summarize_mode(
            ProbeMode::Memory,
            false,
            vec![
                observation(1, 3, 3 * 1024 * 1024),
                observation(2, 0, 0),
                observation(3, 0, 0),
            ],
        );
        let local_file = summarize_mode(
            ProbeMode::LocalFile,
            false,
            vec![
                observation(1, 3, 3 * 1024 * 1024),
                observation(2, 3, 3 * 1024 * 1024),
                observation(3, 3, 3 * 1024 * 1024),
            ],
        );

        let report = ProbeReport::new(vec![memory, local_file]);

        assert_eq!(report.modes.len(), 2);
        assert_eq!(report.min_read_passes, 3);
        assert_eq!(report.go_no_go, GoNoGoGuidance::NoGo);
        assert!(
            report
                .kernel_cache_summary
                .contains("local-file repeated reads re-entered userspace")
        );
    }

    #[test]
    fn probe_config_validates_required_modes_and_three_read_passes() {
        let config = ProbeConfig {
            file_bytes: 1024 * 1024,
            chunk_bytes: 128 * 1024,
            read_passes: 3,
            modes: vec![ProbeMode::Memory, ProbeMode::LocalFile],
            notify_store_experiment: true,
        };

        config.validate().unwrap();

        let missing_local_file = ProbeConfig {
            modes: vec![ProbeMode::Memory],
            ..config.clone()
        };
        assert_eq!(
            missing_local_file.validate().unwrap_err(),
            ProbeConfigError::MissingRequiredMode(ProbeMode::LocalFile)
        );

        let too_few_passes = ProbeConfig {
            read_passes: 2,
            ..config
        };
        assert_eq!(
            too_few_passes.validate().unwrap_err(),
            ProbeConfigError::ReadPassesBelowMinimum {
                actual: 2,
                minimum: 3
            }
        );
    }

    fn observation(pass: u32, callback_count: u64, callback_bytes: u64) -> ReadPassObservation {
        let mut callback_size_distribution = BTreeMap::new();
        if callback_count > 0 {
            callback_size_distribution.insert(1024 * 1024, callback_count);
        }
        ReadPassObservation {
            pass,
            bytes_read: 3 * 1024 * 1024,
            elapsed_ns: 100_000_000,
            callback_count,
            callback_bytes,
            callback_size_distribution,
        }
    }
}
