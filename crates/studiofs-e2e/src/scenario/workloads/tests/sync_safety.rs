use super::*;

#[test]
fn mounted_sync_safety_acceptance_is_registered_after_numbered_scenarios() {
    let definitions = super::super::super::registry::available_scenarios();
    let scenario_index = definitions
        .iter()
        .position(|definition| definition.name == MOUNTED_SYNC_SAFETY_TWO_CLIENT)
        .expect("sync-safety acceptance should be registered");

    assert!(
        scenario_index >= 26,
        "the new scenario must stay outside numbered scenarios 1 through 26"
    );
    assert!(
        super::super::super::registry::resolve_scenario(MOUNTED_SYNC_SAFETY_TWO_CLIENT).is_some()
    );
}

#[test]
fn sync_safety_sample_match_requires_exact_count_and_known_reason() {
    let sample = serde_json::json!({
        "sync_safety": {
            "state": "syncing",
            "reasons": ["metadata_sync_pending"],
        }
    });
    let unknown_with_blocker = serde_json::json!({
        "sync_safety": {
            "state": "unknown",
            "reasons": ["status_unavailable", "publishing_metadata"],
        }
    });
    let mut sample_values = vec![sample.clone(); SYNC_SAFETY_STABLE_SAMPLES];
    sample_values[1] = unknown_with_blocker.clone();
    let samples = serde_json::Value::Array(sample_values);

    assert!(samples_match(&samples, is_metadata_wait));
    assert!(samples_match(&samples, is_dirty_paused_work));
    let metadata_with_upload = serde_json::json!({
        "sync_safety": {
            "state": "unknown",
            "reasons": ["publishing_metadata", "uploading"],
        }
    });
    assert!(!samples_match(
        &serde_json::Value::Array(vec![metadata_with_upload; SYNC_SAFETY_STABLE_SAMPLES]),
        is_metadata_wait,
    ));
    let unknown = serde_json::json!({
        "sync_safety": {
            "state": "unknown",
            "reasons": ["status_unavailable"],
        }
    });
    assert!(!samples_match(
        &serde_json::Value::Array(vec![unknown.clone(); SYNC_SAFETY_STABLE_SAMPLES]),
        is_dirty_paused_work,
    ));
    assert!(!samples_match(
        &serde_json::Value::Array(vec![unknown; SYNC_SAFETY_STABLE_SAMPLES]),
        is_metadata_wait,
    ));
    assert!(samples_match(
        &serde_json::Value::Array(vec![unknown_with_blocker; SYNC_SAFETY_STABLE_SAMPLES]),
        is_metadata_wait,
    ));
    let safe_with_blocker = serde_json::json!({
        "sync_safety": {
            "state": "safe",
            "reasons": ["metadata_sync_pending"],
        }
    });
    assert!(!samples_match(
        &serde_json::Value::Array(vec![safe_with_blocker; SYNC_SAFETY_STABLE_SAMPLES]),
        is_metadata_wait,
    ));
    let safe_dirty_reason = serde_json::json!({
        "state": "safe",
        "reasons": ["metadata_sync_pending"],
    });
    assert!(!is_dirty_paused_work(&safe_dirty_reason));
    let missing = serde_json::json!({ "sync_safety": {} });
    assert!(!samples_match(
        &serde_json::Value::Array(vec![missing; SYNC_SAFETY_STABLE_SAMPLES]),
        is_dirty_paused_work,
    ));
    assert!(!samples_match(
        &serde_json::Value::Array(vec![sample; SYNC_SAFETY_STABLE_SAMPLES - 1]),
        |_| true,
    ));
}
