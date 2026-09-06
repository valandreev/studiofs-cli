use studiofs_vfs::StudioVfs;

const ADAPTIVE_METADATA_TIMEOUT_MS: u32 = 60_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum WinfspMetadataCachePolicy {
    Conservative,
    #[default]
    Adaptive,
}

impl WinfspMetadataCachePolicy {
    pub(crate) fn parse(value: Option<&str>) -> Self {
        match value.map(str::trim).filter(|value| !value.is_empty()) {
            None | Some("adaptive") | Some("auto") => Self::Adaptive,
            Some("conservative") | Some("direct") => Self::Conservative,
            Some(value) => {
                tracing::warn!(
                    env_key = "STUDIOFS_WINFSP_METADATA_CACHE_POLICY",
                    env_value = value,
                    fallback_policy = "conservative",
                    "invalid WinFSP metadata cache policy; using conservative fallback"
                );
                Self::Conservative
            }
        }
    }

    pub(crate) fn allows_read_only_admission(self, vfs: &StudioVfs) -> bool {
        matches!(self, Self::Adaptive) && vfs.coherence_allows_os_cache()
    }

    pub(crate) const fn timeouts(self) -> WinfspCacheTimeouts {
        match self {
            Self::Conservative => WinfspCacheTimeouts::conservative(),
            Self::Adaptive => WinfspCacheTimeouts::adaptive(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WinfspCacheTimeouts {
    pub(crate) file_info_ms: u32,
    pub(crate) dir_info_ms: u32,
    pub(crate) volume_info_ms: u32,
    pub(crate) security_ms: u32,
    pub(crate) stream_info_ms: u32,
    pub(crate) extended_attribute_ms: u32,
}

impl WinfspCacheTimeouts {
    const fn adaptive() -> Self {
        Self {
            file_info_ms: super::DEFAULT_FILE_INFO_TIMEOUT_MS,
            dir_info_ms: super::DEFAULT_DIR_INFO_TIMEOUT_MS,
            volume_info_ms: super::DEFAULT_VOLUME_INFO_TIMEOUT_MS,
            security_ms: ADAPTIVE_METADATA_TIMEOUT_MS,
            stream_info_ms: ADAPTIVE_METADATA_TIMEOUT_MS,
            extended_attribute_ms: ADAPTIVE_METADATA_TIMEOUT_MS,
        }
    }

    const fn conservative() -> Self {
        Self {
            file_info_ms: 0,
            dir_info_ms: 0,
            volume_info_ms: 0,
            security_ms: 0,
            stream_info_ms: 0,
            extended_attribute_ms: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{WinfspCacheTimeouts, WinfspMetadataCachePolicy};

    #[test]
    fn missing_policy_defaults_to_adaptive() {
        assert_eq!(
            WinfspMetadataCachePolicy::parse(None),
            WinfspMetadataCachePolicy::Adaptive
        );
        assert_eq!(
            WinfspMetadataCachePolicy::Adaptive.timeouts(),
            WinfspCacheTimeouts::adaptive()
        );
    }

    #[test]
    fn unknown_policy_fails_safe_to_conservative() {
        assert_eq!(
            WinfspMetadataCachePolicy::parse(Some("unexpected")),
            WinfspMetadataCachePolicy::Conservative
        );
        assert_eq!(
            WinfspMetadataCachePolicy::Conservative.timeouts(),
            WinfspCacheTimeouts::conservative()
        );
    }
}
