use studiofs_cache::runtime::LocalPayloadRuntimeDetection;

pub(crate) fn describe_cache_layout_state(
    detection: Option<&LocalPayloadRuntimeDetection>,
) -> &'static str {
    if let Some(detection) = detection {
        if detection.runtime_backend().is_some() {
            "Local Payload Runtime ready"
        } else if detection.has_runtime_layout() {
            "Local Payload Runtime incomplete"
        } else {
            "Local Payload Runtime not initialized"
        }
    } else {
        "unknown"
    }
}
