use clap::Args;
use studiofs_vfs::{VfsConfig, VfsConfigRaw, parse_size_bytes};

#[derive(Debug, Args, Default, Clone)]
pub(crate) struct PublicRuntimeTuningArgs {
    /// Read buffer budget for foreground reads (for example 4GB).
    #[arg(long, value_name = "SIZE")]
    pub(crate) read_buffer: Option<String>,

    /// Sequential readahead width in blocks.
    #[arg(long, value_name = "BLOCKS")]
    pub(crate) readahead: Option<u32>,

    /// Prefetch concurrency budget.
    #[arg(long, value_name = "COUNT")]
    pub(crate) prefetch: Option<usize>,

    /// Delay background writeback to coalesce overwrite/delete churn.
    #[arg(long, value_name = "MS")]
    pub(crate) writeback_delay: Option<u64>,
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
        if let Some(value) = self.writeback_delay {
            config.upload_delay_ms = Some(value);
        }
        Ok(())
    }

    pub(crate) fn has_any(&self) -> bool {
        self.read_buffer.is_some()
            || self.readahead.is_some()
            || self.prefetch.is_some()
            || self.writeback_delay.is_some()
    }
}

pub(crate) fn runtime_tuning_summary_lines(config: &VfsConfigRaw) -> [String; 4] {
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
            "  writeback-delay: {}",
            config
                .upload_delay_ms
                .map(|value| format!("{value}ms"))
                .unwrap_or_else(|| format!("default ({}ms)", defaults.upload_delay_ms))
        ),
    ]
}

fn parse_read_buffer_mb(raw: &str) -> anyhow::Result<u64> {
    let bytes = parse_size_bytes(raw)
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| anyhow::anyhow!("invalid read-buffer '{}' (must be > 0)", raw))?;
    Ok(bytes.div_ceil(1024 * 1024))
}

fn format_read_buffer_mb(value: u64) -> String {
    if value.is_multiple_of(1024) {
        format!("{}GB", value / 1024)
    } else {
        format!("{value}MB")
    }
}
