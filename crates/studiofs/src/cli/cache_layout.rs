use studiofs_cache::runtime::CacheBackendDetection;

pub(crate) fn describe_cache_layout_state(
    detection: Option<&CacheBackendDetection>,
) -> &'static str {
    if let Some(detection) = detection {
        if detection.has_cache_v3_contract() {
            "Local block runtime ready"
        } else {
            "Local block runtime not initialized"
        }
    } else {
        "unknown"
    }
}
