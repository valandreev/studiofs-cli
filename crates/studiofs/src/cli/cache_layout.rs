use studiofs_cache::runtime::LocalBlockRuntimeDetection;

pub(crate) fn describe_cache_layout_state(
    detection: Option<&LocalBlockRuntimeDetection>,
) -> &'static str {
    if let Some(detection) = detection {
        if detection.has_runtime_layout() {
            "Local block runtime ready"
        } else {
            "Local block runtime not initialized"
        }
    } else {
        "unknown"
    }
}
