use studiofs_cache::v3::CacheBackendDetection;

pub(crate) fn describe_cache_layout_state(
    detection: Option<&CacheBackendDetection>,
) -> &'static str {
    if let Some(detection) = detection {
        if detection.has_cache_v3_contract() {
            "Cache 3.0 ready"
        } else {
            "Cache 3.0 not initialized"
        }
    } else {
        "unknown"
    }
}
