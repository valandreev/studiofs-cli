use clap::Args;
use studiofs_vfs::{VfsConfig, VfsConfigRaw, parse_size_bytes};

const MIN_READAHEAD_BLOCKS: u32 = 1;
const MIN_PREFETCH_CONCURRENCY: usize = 1;

#[derive(Debug, Args, Default, Clone)]
pub(crate) struct PublicRuntimeTuningArgs {
    /// Read buffer budget for foreground reads (for example 4GB).
    /// Rounded up to the nearest MiB for the internal VFS config.
    #[arg(long, value_name = "SIZE")]
    pub(crate) read_buffer: Option<String>,

    /// Sequential readahead width in blocks.
    #[arg(long, value_name = "BLOCKS", value_parser = parse_readahead_blocks)]
    pub(crate) readahead: Option<u32>,

    /// Prefetch concurrency budget.
    #[arg(long, value_name = "COUNT", value_parser = parse_prefetch_concurrency)]
    pub(crate) prefetch: Option<usize>,

    /// Aggregate background upload limit in Mbps; zero disables the limit.
    #[arg(long, value_name = "MBPS", value_parser = clap::value_parser!(u64).range(0..=1_000_000))]
    pub(crate) upload_limit: Option<u64>,

    /// Aggregate object download limit in Mbps; zero disables the limit.
    #[arg(long, value_name = "MBPS", value_parser = clap::value_parser!(u64).range(0..=1_000_000))]
    pub(crate) download_limit: Option<u64>,

    /// Enable or disable background verification of the local payload cache.
    #[arg(long, value_name = "BOOL")]
    pub(crate) cache_scrub: Option<bool>,

    /// Seconds to wait between local cache scrub passes.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) cache_scrub_interval_secs: Option<u64>,

    /// Local bytes per second budget for cache scrub reads.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) cache_scrub_bytes_per_sec: Option<u64>,

    /// Local payload checks per second for cache scrub.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) cache_scrub_iops: Option<u64>,
}

impl PublicRuntimeTuningArgs {
    pub(crate) fn apply_to_vfs_config(&self, config: &mut VfsConfigRaw) -> anyhow::Result<()> {
        if let Some(raw) = self.read_buffer.as_deref() {
            config.read_buffer_mb = Some(parse_read_buffer_mb(raw)?);
        }
        if let Some(value) = self.readahead {
            config.readahead_width = Some(value);
        }
        if let Some(value) = self.prefetch {
            config.prefetch_concurrency = Some(value);
        }
        if let Some(value) = self.upload_limit {
            config.upload_limit = Some(value);
        }
        if let Some(value) = self.download_limit {
            config.download_limit = Some(value);
        }
        config.cache_scrub = self.cache_scrub.or(config.cache_scrub);
        config.cache_scrub_interval_secs = self
            .cache_scrub_interval_secs
            .or(config.cache_scrub_interval_secs);
        config.cache_scrub_bytes_per_sec = self
            .cache_scrub_bytes_per_sec
            .or(config.cache_scrub_bytes_per_sec);
        config.cache_scrub_iops = self.cache_scrub_iops.or(config.cache_scrub_iops);
        Ok(())
    }

    pub(crate) fn has_any(&self) -> bool {
        self.read_buffer.is_some()
            || self.readahead.is_some()
            || self.prefetch.is_some()
            || self.upload_limit.is_some()
            || self.download_limit.is_some()
            || self.cache_scrub.is_some()
            || self.cache_scrub_interval_secs.is_some()
            || self.cache_scrub_bytes_per_sec.is_some()
            || self.cache_scrub_iops.is_some()
    }
}

pub(crate) fn runtime_tuning_summary_lines(config: &VfsConfigRaw) -> [String; 9] {
    let defaults = VfsConfig::default();
    [
        format!(
            "  read-buffer: {}",
            config
                .read_buffer_mb
                .map(format_read_buffer_mb)
                .unwrap_or_else(|| format!(
                    "default ({})",
                    format_read_buffer_mb(defaults.read_buffer_mb)
                ))
        ),
        format!(
            "  readahead: {}",
            config
                .readahead_width
                .map(|value| value.to_string())
                .unwrap_or_else(|| format!("default ({})", defaults.readahead_width))
        ),
        format!(
            "  prefetch: {}",
            config
                .prefetch_concurrency
                .map(|value| value.to_string())
                .unwrap_or_else(|| format!("default ({})", defaults.prefetch_concurrency))
        ),
        format!(
            "  upload-limit: {} Mbps",
            config.upload_limit.unwrap_or(defaults.upload_limit)
        ),
        format!(
            "  download-limit: {} Mbps",
            config.download_limit.unwrap_or(defaults.download_limit)
        ),
        format!(
            "  cache-scrub: {}",
            config.cache_scrub.unwrap_or(defaults.cache_scrub)
        ),
        format!(
            "  cache-scrub-interval-secs: {}",
            config
                .cache_scrub_interval_secs
                .unwrap_or(defaults.cache_scrub_interval_secs)
        ),
        format!(
            "  cache-scrub-bytes-per-sec: {}",
            config
                .cache_scrub_bytes_per_sec
                .unwrap_or(defaults.cache_scrub_bytes_per_sec)
        ),
        format!(
            "  cache-scrub-iops: {}",
            config.cache_scrub_iops.unwrap_or(defaults.cache_scrub_iops)
        ),
    ]
}

fn parse_read_buffer_mb(raw: &str) -> anyhow::Result<u64> {
    let bytes = parse_size_bytes(raw)
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| anyhow::anyhow!("invalid read-buffer '{}' (must be > 0)", raw))?;
    Ok(bytes.div_ceil(1024 * 1024))
}

fn parse_readahead_blocks(raw: &str) -> Result<u32, String> {
    let value = raw
        .parse::<u32>()
        .map_err(|_| format!("invalid value '{raw}' for --readahead"))?;
    if value < MIN_READAHEAD_BLOCKS {
        return Err(format!("--readahead must be >= {MIN_READAHEAD_BLOCKS}"));
    }
    Ok(value)
}

fn parse_prefetch_concurrency(raw: &str) -> Result<usize, String> {
    let value = raw
        .parse::<usize>()
        .map_err(|_| format!("invalid value '{raw}' for --prefetch"))?;
    if value < MIN_PREFETCH_CONCURRENCY {
        return Err(format!("--prefetch must be >= {MIN_PREFETCH_CONCURRENCY}"));
    }
    Ok(value)
}

fn format_read_buffer_mb(value: u64) -> String {
    if value.is_multiple_of(1024) {
        format!("{}GiB", value / 1024)
    } else {
        format!("{value}MiB")
    }
}
