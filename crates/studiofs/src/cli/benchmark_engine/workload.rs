use anyhow::{Context, Result, anyhow};
use std::path::{Path, PathBuf};
use studiofs_vfs::parse_size_bytes;

use super::BenchmarkAccessPattern;

pub(crate) const WORKLOAD_FILES_PER_TIER: usize = 100;
pub(crate) const DEFAULT_WORKLOAD_FILES: usize = WORKLOAD_FILES_PER_TIER * 3;
pub(crate) const SMALL_FILE_MIN_BYTES: u64 = 1024;
pub(crate) const SMALL_FILE_MAX_BYTES: u64 = 10 * 1024;
pub(crate) const LARGE_FILE_MULTIPLIER: u64 = 8;
pub(crate) const DEFAULT_SMALL_CLASS_MAX_BYTES: u64 = SMALL_FILE_MAX_BYTES;
pub(crate) const DEFAULT_BENCHMARK_BLOCK_SIZE_STR: &str = "16MB";
pub(crate) const PUBLIC_BENCH_SEED: u64 = 0x005f_5b01_1202_6561;
pub(crate) const PUBLIC_BENCH_SMALL_FILES: usize = 1000;
pub(crate) const PUBLIC_BENCH_MEDIUM_FILES: usize = 80;
pub(crate) const PUBLIC_BENCH_SMALL_MIN_BYTES: u64 = 1024;
pub(crate) const PUBLIC_BENCH_SMALL_MAX_BYTES: u64 = 250 * 1024;
pub(crate) const PUBLIC_BENCH_MEDIUM_MIN_BYTES: u64 = 5 * 1024 * 1024;
pub(crate) const PUBLIC_BENCH_MEDIUM_MAX_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const PUBLIC_BENCH_MEDIUM_GRANULARITY_BYTES: u64 = 1024;
pub(crate) const PUBLIC_BENCH_LARGE_BASE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub(crate) const PUBLIC_BENCH_LARGE_DELTA_BYTES: u64 = 100 * 1024 * 1024;
pub(crate) const PUBLIC_BENCH_LARGE_GRANULARITY_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SizeClass {
    Small,
    Medium,
    Large,
}

impl SizeClass {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum WorkloadSizeInput {
    Default,
    Percentages { small: u8, medium: u8, large: u8 },
    ExplicitList(Vec<u64>),
}

impl WorkloadSizeInput {
    fn display_label(&self) -> String {
        match self {
            Self::Default => "default".to_string(),
            Self::Percentages {
                small,
                medium,
                large,
            } => format!("small={small}%,medium={medium}%,large={large}%"),
            Self::ExplicitList(items) => format!("explicit-list({})", items.len()),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct WorkloadPlan {
    pub(crate) sizes: Vec<u64>,
    pub(crate) source: String,
    pub(crate) small_files: usize,
    pub(crate) medium_files: usize,
    pub(crate) large_files: usize,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SizeClassBounds {
    pub(crate) small_max: u64,
    pub(crate) medium_max: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicBenchFileSpec {
    pub(crate) index: usize,
    pub(crate) size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicBenchWorkload {
    pub(crate) small_files: Vec<PublicBenchFileSpec>,
    pub(crate) medium_files: Vec<PublicBenchFileSpec>,
    pub(crate) large_file: PublicBenchFileSpec,
    pub(crate) read_order: Vec<usize>,
    pub(crate) large_file_read_offsets: Vec<u64>,
}

impl PublicBenchWorkload {
    pub(crate) fn file_sizes(&self) -> Vec<u64> {
        let mut sizes = Vec::with_capacity(
            self.small_files
                .len()
                .saturating_add(self.medium_files.len())
                .saturating_add(1),
        );
        sizes.extend(self.small_files.iter().map(|file| file.size));
        sizes.extend(self.medium_files.iter().map(|file| file.size));
        sizes.push(self.large_file.size);
        sizes
    }
}

pub(crate) fn classify_size(size: u64, small_max: u64, medium_max: u64) -> SizeClass {
    if size <= small_max {
        SizeClass::Small
    } else if size <= medium_max {
        SizeClass::Medium
    } else {
        SizeClass::Large
    }
}

#[derive(Clone, Debug)]
pub(crate) struct WorkFile {
    pub(crate) path: PathBuf,
    pub(crate) size: u64,
    pub(crate) index: usize,
}

#[derive(Clone)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        let state = seed ^ 0x9e3779b97f4a7c15;
        Self {
            state: state.max(1),
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 7;
        x ^= x >> 9;
        x ^= x << 8;
        self.state = x;
        x
    }

    fn next_usize(&mut self, max_exclusive: usize) -> usize {
        if max_exclusive <= 1 {
            return 0;
        }
        (self.next_u64() % max_exclusive as u64) as usize
    }
}

pub(crate) fn parse_size_arg(label: &str, raw: &str) -> Result<u64> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(anyhow!("{label} is empty"));
    }

    parse_size_bytes(raw)
        .or_else(|| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .with_context(|| {
            format!("invalid {label} '{raw}', expected value like 4KB, 2MB, 1GB or integer bytes")
        })
}

pub(crate) fn parse_sizes_arg(raw: Option<&str>) -> Result<WorkloadSizeInput> {
    let Some(raw) = raw else {
        return Ok(WorkloadSizeInput::Default);
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(WorkloadSizeInput::Default);
    }

    if raw.contains('=') {
        let mut small = 0u8;
        let mut medium = 0u8;
        let mut large = 0u8;
        let mut seen_any = false;
        let mut seen_small = false;
        let mut seen_medium = false;
        let mut seen_large = false;

        for segment in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (name, value_raw) = segment.split_once('=').ok_or_else(|| {
                anyhow!("invalid --sizes segment '{segment}', expected key=value")
            })?;
            let value = value_raw
                .trim()
                .trim_end_matches('%')
                .parse::<u8>()
                .with_context(|| format!("invalid percentage in --sizes segment '{segment}'"))?;
            seen_any = true;
            match name.trim().to_ascii_lowercase().as_str() {
                "small" => {
                    if seen_small {
                        return Err(anyhow!("duplicate 'small' entry in --sizes"));
                    }
                    small = value;
                    seen_small = true;
                }
                "medium" => {
                    if seen_medium {
                        return Err(anyhow!("duplicate 'medium' entry in --sizes"));
                    }
                    medium = value;
                    seen_medium = true;
                }
                "large" => {
                    if seen_large {
                        return Err(anyhow!("duplicate 'large' entry in --sizes"));
                    }
                    large = value;
                    seen_large = true;
                }
                other => {
                    return Err(anyhow!(
                        "invalid size class '{other}' in --sizes; expected small, medium, large"
                    ));
                }
            }
        }

        if !seen_any {
            return Err(anyhow!(
                "--sizes percentages are empty; expected values like small=60,medium=30,large=10"
            ));
        }
        if u16::from(small) + u16::from(medium) + u16::from(large) != 100 {
            return Err(anyhow!(
                "--sizes percentages must add up to 100 (got {} + {} + {})",
                small,
                medium,
                large
            ));
        }
        return Ok(WorkloadSizeInput::Percentages {
            small,
            medium,
            large,
        });
    }

    let mut sizes = Vec::new();
    for value in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        sizes.push(parse_size_arg("sizes", value)?);
    }
    if sizes.is_empty() {
        return Err(anyhow!(
            "--sizes explicit list is empty; provide comma-separated sizes like 4KB,128KB,1MB"
        ));
    }
    Ok(WorkloadSizeInput::ExplicitList(sizes))
}

pub(crate) fn build_access_sequence(
    len: usize,
    pattern: BenchmarkAccessPattern,
    seed: u64,
) -> Vec<usize> {
    let mut order: Vec<usize> = (0..len).collect();
    if len <= 1 {
        return order;
    }
    let mut rng = XorShift64::new(seed);

    match pattern {
        BenchmarkAccessPattern::Sequential => {}
        BenchmarkAccessPattern::Random => {
            for i in (1..order.len()).rev() {
                let j = rng.next_usize(i + 1);
                order.swap(i, j);
            }
        }
        BenchmarkAccessPattern::Mixed => {
            let swaps = (order.len() / 4).max(1);
            for _ in 0..swaps {
                let a = rng.next_usize(order.len());
                let b = rng.next_usize(order.len());
                order.swap(a, b);
            }
        }
    }

    order
}

pub(crate) fn build_work_sizes(
    block_size: u64,
    size_input: &WorkloadSizeInput,
    size_class_bounds: SizeClassBounds,
    workload_repeat: usize,
) -> WorkloadPlan {
    let mut rng = XorShift64::new(0x5ed0_f00d_beef);
    let small_max = size_class_bounds.small_max.max(1);
    let medium_max = size_class_bounds
        .medium_max
        .max(small_max.saturating_add(1));
    let medium_size = block_size
        .max(1)
        .min(medium_max)
        .max(small_max.saturating_add(1));
    let large_size = block_size
        .max(1)
        .saturating_mul(LARGE_FILE_MULTIPLIER)
        .max(medium_max.saturating_add(1))
        .max(1);

    let mut generate_small = |out: &mut Vec<u64>, count: usize| {
        let lower = SMALL_FILE_MIN_BYTES.min(small_max).max(1);
        let upper = small_max.max(lower);
        let span = upper.saturating_sub(lower).saturating_add(1);
        for _ in 0..count {
            let size = lower + (rng.next_u64() % span);
            out.push(size);
        }
    };

    match size_input {
        WorkloadSizeInput::Default => {
            let mut sizes = Vec::with_capacity(DEFAULT_WORKLOAD_FILES);
            generate_small(&mut sizes, WORKLOAD_FILES_PER_TIER);
            for _ in 0..WORKLOAD_FILES_PER_TIER {
                sizes.push(medium_size);
            }
            for _ in 0..WORKLOAD_FILES_PER_TIER {
                sizes.push(large_size);
            }
            repeat_workload_plan(
                WorkloadPlan {
                    sizes,
                    source: size_input.display_label(),
                    small_files: WORKLOAD_FILES_PER_TIER,
                    medium_files: WORKLOAD_FILES_PER_TIER,
                    large_files: WORKLOAD_FILES_PER_TIER,
                },
                workload_repeat,
            )
        }
        WorkloadSizeInput::Percentages {
            small,
            medium,
            large,
        } => {
            let total = DEFAULT_WORKLOAD_FILES as u64;
            let small_count = (total * u64::from(*small) / 100) as usize;
            let mut medium_count = (total * u64::from(*medium) / 100) as usize;
            let mut large_count = (total * u64::from(*large) / 100) as usize;
            let assigned = small_count + medium_count + large_count;
            if assigned < DEFAULT_WORKLOAD_FILES {
                medium_count += DEFAULT_WORKLOAD_FILES - assigned;
            } else if assigned > DEFAULT_WORKLOAD_FILES {
                let overshoot = assigned - DEFAULT_WORKLOAD_FILES;
                let trim = overshoot.min(medium_count);
                medium_count -= trim;
                let remain = overshoot - trim;
                if remain > 0 {
                    large_count = large_count.saturating_sub(remain);
                }
            }

            let mut sizes = Vec::with_capacity(DEFAULT_WORKLOAD_FILES);
            generate_small(&mut sizes, small_count);
            for _ in 0..medium_count {
                sizes.push(medium_size);
            }
            for _ in 0..large_count {
                sizes.push(large_size);
            }

            repeat_workload_plan(
                WorkloadPlan {
                    sizes,
                    source: size_input.display_label(),
                    small_files: small_count,
                    medium_files: medium_count,
                    large_files: large_count,
                },
                workload_repeat,
            )
        }
        WorkloadSizeInput::ExplicitList(items) => {
            let mut small_files = 0usize;
            let mut medium_files = 0usize;
            let mut large_files = 0usize;
            for size in items {
                match classify_size(*size, small_max, medium_max) {
                    SizeClass::Small => small_files += 1,
                    SizeClass::Medium => medium_files += 1,
                    SizeClass::Large => large_files += 1,
                }
            }
            repeat_workload_plan(
                WorkloadPlan {
                    sizes: items.clone(),
                    source: size_input.display_label(),
                    small_files,
                    medium_files,
                    large_files,
                },
                workload_repeat,
            )
        }
    }
}

pub(crate) fn public_bench_workload() -> PublicBenchWorkload {
    let mut rng = XorShift64::new(PUBLIC_BENCH_SEED);
    let small_files = generate_public_bench_files(
        0,
        PUBLIC_BENCH_SMALL_FILES,
        PUBLIC_BENCH_SMALL_MIN_BYTES,
        PUBLIC_BENCH_SMALL_MAX_BYTES,
        PUBLIC_BENCH_MEDIUM_GRANULARITY_BYTES,
        &mut rng,
    );
    let medium_files = generate_public_bench_files(
        PUBLIC_BENCH_SMALL_FILES,
        PUBLIC_BENCH_MEDIUM_FILES,
        PUBLIC_BENCH_MEDIUM_MIN_BYTES,
        PUBLIC_BENCH_MEDIUM_MAX_BYTES,
        PUBLIC_BENCH_MEDIUM_GRANULARITY_BYTES,
        &mut rng,
    );
    let large_file = PublicBenchFileSpec {
        index: PUBLIC_BENCH_SMALL_FILES + PUBLIC_BENCH_MEDIUM_FILES,
        size: generate_public_bench_large_size(&mut rng),
    };
    let read_order = shuffled_indices(large_file.index + 1, rng.next_u64());
    let large_file_read_offsets = shuffled_large_file_offsets(large_file.size, rng.next_u64());

    PublicBenchWorkload {
        small_files,
        medium_files,
        large_file,
        read_order,
        large_file_read_offsets,
    }
}

fn generate_public_bench_files(
    start_index: usize,
    count: usize,
    min_bytes: u64,
    max_bytes: u64,
    granularity_bytes: u64,
    rng: &mut XorShift64,
) -> Vec<PublicBenchFileSpec> {
    let steps = (max_bytes - min_bytes) / granularity_bytes;
    (0..count)
        .map(|offset| {
            let size = min_bytes + (rng.next_u64() % (steps + 1)) * granularity_bytes;
            PublicBenchFileSpec {
                index: start_index + offset,
                size,
            }
        })
        .collect()
}

fn generate_public_bench_large_size(rng: &mut XorShift64) -> u64 {
    let span_steps = (PUBLIC_BENCH_LARGE_DELTA_BYTES * 2) / PUBLIC_BENCH_LARGE_GRANULARITY_BYTES;
    let mut offset_steps = (rng.next_u64() % (span_steps + 1)) as i64
        - (PUBLIC_BENCH_LARGE_DELTA_BYTES / PUBLIC_BENCH_LARGE_GRANULARITY_BYTES) as i64;
    if offset_steps == 0 {
        offset_steps = 1;
    }
    (PUBLIC_BENCH_LARGE_BASE_BYTES as i64
        + offset_steps * PUBLIC_BENCH_LARGE_GRANULARITY_BYTES as i64) as u64
}

fn shuffled_indices(len: usize, seed: u64) -> Vec<usize> {
    build_access_sequence(len, BenchmarkAccessPattern::Random, seed)
}

fn shuffled_large_file_offsets(size: u64, seed: u64) -> Vec<u64> {
    let chunk_count = (size / PUBLIC_BENCH_LARGE_GRANULARITY_BYTES) as usize;
    shuffled_indices(chunk_count, seed)
        .into_iter()
        .map(|index| index as u64 * PUBLIC_BENCH_LARGE_GRANULARITY_BYTES)
        .collect()
}

#[cfg(test)]
fn is_monotonic_non_decreasing(sizes: impl IntoIterator<Item = u64>) -> bool {
    let mut iter = sizes.into_iter();
    let Some(mut previous) = iter.next() else {
        return true;
    };
    for size in iter {
        if size < previous {
            return false;
        }
        previous = size;
    }
    true
}

#[cfg(test)]
fn is_constant_step_ladder(sizes: impl IntoIterator<Item = u64>) -> bool {
    let mut iter = sizes.into_iter();
    let Some(first) = iter.next() else {
        return true;
    };
    let Some(second) = iter.next() else {
        return true;
    };
    let Some(step) = second.checked_sub(first) else {
        return false;
    };
    let mut previous = second;
    for size in iter {
        if size.checked_sub(previous) != Some(step) {
            return false;
        }
        previous = size;
    }
    true
}

fn repeat_workload_plan(plan: WorkloadPlan, workload_repeat: usize) -> WorkloadPlan {
    let repeat = workload_repeat.max(1);
    if repeat == 1 {
        return plan;
    }

    let mut sizes = Vec::with_capacity(plan.sizes.len().saturating_mul(repeat));
    for _ in 0..repeat {
        sizes.extend_from_slice(&plan.sizes);
    }

    WorkloadPlan {
        sizes,
        source: format!("{} x{}", plan.source, repeat),
        small_files: plan.small_files.saturating_mul(repeat),
        medium_files: plan.medium_files.saturating_mul(repeat),
        large_files: plan.large_files.saturating_mul(repeat),
    }
}

pub(crate) fn deterministic_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut out = vec![0u8; len];
    fill_deterministic_bytes(&mut out, len, seed);
    out
}

pub(crate) fn fill_deterministic_bytes(buffer: &mut Vec<u8>, len: usize, seed: u64) {
    buffer.resize(len, 0);
    let mut rng = XorShift64::new(seed ^ 0x6ac9_fe0f);
    let out = &mut buffer[..len];
    let mut chunks = out.chunks_exact_mut(8);
    for chunk in &mut chunks {
        chunk.copy_from_slice(&rng.next_u64().to_le_bytes());
    }
    let remainder = chunks.into_remainder();
    if !remainder.is_empty() {
        let bytes = rng.next_u64().to_le_bytes();
        remainder.copy_from_slice(&bytes[..remainder.len()]);
    }
}

pub(crate) fn build_file_worklist(work_sizes: &[u64], mount_root: &Path) -> Vec<WorkFile> {
    work_sizes
        .iter()
        .enumerate()
        .map(|(idx, size)| WorkFile {
            path: mount_root.join(format!("benchmark_file_{idx:06}.bin")),
            size: *size,
            index: idx,
        })
        .collect()
}

pub(crate) fn split_stream_segments(order: &[usize], stream_count: usize) -> Vec<Vec<usize>> {
    if order.is_empty() {
        return Vec::new();
    }
    let streams = stream_count.max(1).min(order.len());
    let base = order.len() / streams;
    let extra = order.len() % streams;
    let mut segments = Vec::with_capacity(streams);
    let mut start = 0usize;
    for stream_idx in 0..streams {
        let mut len = base;
        if stream_idx < extra {
            len += 1;
        }
        let end = start + len;
        segments.push(order[start..end].to_vec());
        start = end;
    }
    segments
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_meta::DEFAULT_BLOCK_SIZE;

    #[test]
    fn workload_profile_has_expected_tiers() {
        let block_size = DEFAULT_BLOCK_SIZE as u64;
        let bounds = SizeClassBounds {
            small_max: SMALL_FILE_MAX_BYTES,
            medium_max: block_size,
        };
        let plan = build_work_sizes(block_size, &WorkloadSizeInput::Default, bounds, 1);
        let sizes = plan.sizes;
        assert_eq!(sizes.len(), WORKLOAD_FILES_PER_TIER * 3);

        let small = &sizes[..WORKLOAD_FILES_PER_TIER];
        let medium = &sizes[WORKLOAD_FILES_PER_TIER..WORKLOAD_FILES_PER_TIER * 2];
        let large = &sizes[WORKLOAD_FILES_PER_TIER * 2..];

        assert!(
            small
                .iter()
                .all(|size| *size >= SMALL_FILE_MIN_BYTES && *size <= SMALL_FILE_MAX_BYTES)
        );
        assert!(medium.iter().all(|size| *size == block_size));
        assert!(
            large
                .iter()
                .all(|size| *size == block_size * LARGE_FILE_MULTIPLIER)
        );
    }

    #[test]
    fn sizes_percentages_parse_and_sum_validation() {
        let parsed = parse_sizes_arg(Some("small=60,medium=30,large=10")).unwrap();
        assert!(matches!(
            parsed,
            WorkloadSizeInput::Percentages {
                small: 60,
                medium: 30,
                large: 10
            }
        ));
        assert!(parse_sizes_arg(Some("small=40,medium=40,large=10")).is_err());
    }

    #[test]
    fn sizes_percentages_allow_missing_classes_as_zero() {
        let parsed = parse_sizes_arg(Some("small=50,large=50")).unwrap();
        assert!(matches!(
            parsed,
            WorkloadSizeInput::Percentages {
                small: 50,
                medium: 0,
                large: 50
            }
        ));
        assert!(parse_sizes_arg(Some("small=50,small=50")).is_err());
    }

    #[test]
    fn sizes_explicit_list_parses() {
        let parsed = parse_sizes_arg(Some("4KB,1MB,16MB")).unwrap();
        match parsed {
            WorkloadSizeInput::ExplicitList(values) => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], 4 * 1024);
                assert_eq!(values[1], 1024 * 1024);
                assert_eq!(values[2], 16 * 1024 * 1024);
            }
            other => panic!("expected explicit list, got {other:?}"),
        }
    }

    #[test]
    fn workload_respects_custom_size_class_bounds() {
        let bounds = SizeClassBounds {
            small_max: 2 * 1024,
            medium_max: 6 * 1024,
        };
        let plan = build_work_sizes(
            DEFAULT_BLOCK_SIZE as u64,
            &WorkloadSizeInput::ExplicitList(vec![1024, 4 * 1024, 8 * 1024]),
            bounds,
            1,
        );
        assert_eq!(plan.small_files, 1);
        assert_eq!(plan.medium_files, 1);
        assert_eq!(plan.large_files, 1);

        let default_plan = build_work_sizes(
            DEFAULT_BLOCK_SIZE as u64,
            &WorkloadSizeInput::Default,
            bounds,
            1,
        );
        assert!(
            default_plan.sizes[..WORKLOAD_FILES_PER_TIER]
                .iter()
                .all(|size| *size <= bounds.small_max)
        );
    }

    #[test]
    fn workload_repeat_duplicates_explicit_size_lists() {
        let plan = build_work_sizes(
            DEFAULT_BLOCK_SIZE as u64,
            &WorkloadSizeInput::ExplicitList(vec![
                4 * 1024,
                DEFAULT_BLOCK_SIZE as u64,
                128 * 1024 * 1024,
            ]),
            SizeClassBounds {
                small_max: SMALL_FILE_MAX_BYTES,
                medium_max: DEFAULT_BLOCK_SIZE as u64,
            },
            4,
        );

        assert_eq!(plan.sizes.len(), 12);
        assert_eq!(plan.small_files, 4);
        assert_eq!(plan.medium_files, 4);
        assert_eq!(plan.large_files, 4);
        assert_eq!(plan.source, "explicit-list(3) x4");
        assert_eq!(plan.sizes[0], 4 * 1024);
        assert_eq!(plan.sizes[3], 4 * 1024);
        assert_eq!(plan.sizes[4], DEFAULT_BLOCK_SIZE as u64);
    }

    #[test]
    fn deterministic_bytes_are_stable_across_calls() {
        let first = deterministic_bytes(31, 42);
        let second = deterministic_bytes(31, 42);

        assert_eq!(first, second);
    }

    #[test]
    fn deterministic_bytes_depend_on_seed() {
        let first = deterministic_bytes(31, 42);
        let second = deterministic_bytes(31, 43);

        assert_ne!(first, second);
    }

    #[test]
    fn deterministic_bytes_avoid_xorshift_zero_state() {
        let zero_state_seed = 0x9e3779b97f4a7c15 ^ 0x6ac9_fe0f;

        let bytes = deterministic_bytes(31, zero_state_seed);

        assert!(bytes.iter().any(|byte| *byte != 0));
    }

    #[test]
    fn fill_deterministic_bytes_reuses_existing_capacity() {
        let mut buffer = Vec::with_capacity(64);
        fill_deterministic_bytes(&mut buffer, 31, 42);
        let capacity = buffer.capacity();
        let expected = deterministic_bytes(31, 42);
        assert_eq!(&buffer[..31], expected.as_slice());

        fill_deterministic_bytes(&mut buffer, 16, 43);

        assert_eq!(buffer.capacity(), capacity);
        assert_eq!(buffer.len(), 16);
    }

    #[test]
    fn public_bench_workload_has_fixed_counts_bounds_and_granularity() {
        let workload = public_bench_workload();

        assert_eq!(workload.small_files.len(), 1000);
        assert_eq!(workload.medium_files.len(), 80);
        assert_eq!(workload.large_file.size, 4_215_275_520);
        assert_ne!(workload.large_file.size, 4 * 1024 * 1024 * 1024);
        assert_eq!(workload.file_sizes().len(), 1081);

        assert!(
            workload
                .small_files
                .iter()
                .all(|file| file.size >= 1024 && file.size <= 250 * 1024)
        );
        assert!(workload.medium_files.iter().all(|file| {
            file.size >= 5 * 1024 * 1024 && file.size <= 8 * 1024 * 1024 && file.size % 1024 == 0
        }));
        assert_eq!(workload.large_file.size % (1024 * 1024), 0);
    }

    #[test]
    fn public_bench_workload_exact_values_are_stable_for_fixed_seed() {
        let workload = public_bench_workload();

        let small_head: Vec<u64> = workload
            .small_files
            .iter()
            .take(8)
            .map(|file| file.size)
            .collect();
        let medium_head: Vec<u64> = workload
            .medium_files
            .iter()
            .take(8)
            .map(|file| file.size)
            .collect();

        assert_eq!(
            small_head,
            vec![
                139_264, 89_088, 6_144, 11_264, 49_152, 38_912, 232_448, 102_400
            ]
        );
        assert_eq!(
            medium_head,
            vec![
                7_809_024, 5_628_928, 7_109_632, 6_328_320, 6_029_312, 8_283_136, 6_051_840,
                5_682_176,
            ]
        );
        assert_eq!(
            &workload.read_order[..12],
            &[932, 794, 647, 604, 531, 905, 481, 589, 851, 365, 142, 395]
        );
        assert_eq!(
            &workload.large_file_read_offsets[..8],
            &[
                2_819_620_864,
                1_937_768_448,
                3_212_836_864,
                2_472_542_208,
                1_170_210_816,
                1_756_364_800,
                2_302_672_896,
                3_596_615_680,
            ]
        );
    }

    #[test]
    fn public_bench_workload_is_not_monotonic_or_stepwise() {
        let workload = public_bench_workload();

        assert!(!is_monotonic_non_decreasing(
            workload.small_files.iter().map(|file| file.size)
        ));
        assert!(!is_monotonic_non_decreasing(
            workload.medium_files.iter().map(|file| file.size)
        ));
        assert!(!is_constant_step_ladder(
            workload.small_files.iter().map(|file| file.size)
        ));
        assert!(!is_constant_step_ladder(
            workload.medium_files.iter().map(|file| file.size)
        ));
    }
}
