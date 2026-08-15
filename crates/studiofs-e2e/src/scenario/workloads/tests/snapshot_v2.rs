use super::*;

#[test]
fn snapshot_v2_scenarios_are_registered_and_selectable_independently() {
    let definitions = super::super::super::registry::available_scenarios();
    let names = definitions
        .iter()
        .map(|definition| definition.name)
        .collect::<Vec<_>>();

    assert!(names.contains(&"snapshot-manual-recovery"));
    assert!(names.contains(&"snapshot-materializer-takeover"));
    assert!(super::super::super::registry::resolve_scenario("snapshot-manual-recovery").is_some());
    assert!(
        super::super::super::registry::resolve_scenario("snapshot-materializer-takeover").is_some()
    );
}

#[test]
fn snapshot_v2_uses_exact_recovery_cli_forms() {
    assert_eq!(snapshot_create_args(), ["snapshot", "create"]);
    assert_eq!(
        snapshot_mount_args(
            "projectfs",
            "projectfs.2026-08-15T12-00-00.000Z",
            Path::new("Z:"),
        ),
        vec![
            "smount".to_string(),
            "projectfs".to_string(),
            "projectfs.2026-08-15T12-00-00.000Z".to_string(),
            "Z:".to_string(),
        ]
    );
}

#[test]
fn snapshot_id_parser_requires_the_recorded_create_marker() {
    assert_eq!(
        parse_snapshot_id("Creating manual snapshot projectfs.2026-08-15T12-00-00.000Z...\n"),
        Some("projectfs.2026-08-15T12-00-00.000Z".to_string())
    );
    assert_eq!(
        parse_snapshot_id("Snapshot projectfs.legacy is Ready."),
        None
    );
    assert_eq!(parse_snapshot_id(""), None);
    assert_eq!(
        parse_snapshot_id(
            "diagnostic\nCreating manual snapshot projectfs.multiline...\ncompleted\n"
        ),
        Some("projectfs.multiline".to_string())
    );
    assert_eq!(
        parse_snapshot_id("Creating manual snapshot projectfs.no-suffix"),
        None
    );
}

#[test]
fn snapshot_state_diagnostics_do_not_render_raw_last_error() {
    let state = SnapshotState {
        id: "projectfs.snapshot".to_string(),
        origin: "manual".to_string(),
        cut_version: 7,
        lifecycle: "creating".to_string(),
        job_state: "running".to_string(),
        captured_at: Some("2026-08-15T12:00:00.000Z".to_string()),
        claimant: Some("worker-a".to_string()),
        lease_expires_at: Some("2026-08-15T12:00:00.000Z".to_string()),
        lease_generation: 3,
        last_error: Some("postgres://user:secret@host/db".to_string()),
        artifact_key: None,
        artifact_size: None,
        artifact_sha256: None,
    };

    let diagnostic = format_state(&state);
    assert!(diagnostic.contains("last_error=present"));
    assert!(!diagnostic.contains("postgres://user:secret@host/db"));
}

#[test]
fn snapshot_diagnostics_redact_normalized_and_encoded_credentials() {
    let mut config = _test_config(Path::new("sandbox"));
    config.s3.access_key = "access/key".to_string();
    config.s3.secret_key = "secret+key".to_string();

    config.meta_dsn = "postgres://studiofs:p%40ss%20word@127.0.0.1:25432".to_string();
    let mixed_diagnostic = redact_diagnostics(
        &config,
        "postgres://studiofs:p%40ss%20word@db; postgres://studiofs:p@ss%20word@db; postgres://studiofs:p@ss word@db; access%2Fkey; secret%2Bkey",
    );
    assert!(!mixed_diagnostic.contains("p%40ss%20word"));
    assert!(!mixed_diagnostic.contains("p@ss%20word"));
    assert!(!mixed_diagnostic.contains("p@ss word"));
    assert!(!mixed_diagnostic.contains("access%2Fkey"));
    assert!(!mixed_diagnostic.contains("secret%2Bkey"));

    config.meta_dsn = "postgres://studiofs:%C3%A9@127.0.0.1:25432".to_string();
    let diagnostic = redact_diagnostics(
        &config,
        "postgres://studiofs:%C3%A9@db; postgres://studiofs:%c3%a9@db; postgres://studiofs:é@db",
    );

    assert!(!diagnostic.contains("%C3%A9"));
    assert!(!diagnostic.contains("é"));

    config.meta_dsn = "postgresql://studiofs:p%40ss%20word@127.0.0.1:25432".to_string();
    let postgresql_diagnostic = redact_diagnostics(
        &config,
        "postgresql://studiofs:p%40ss%20word@db; postgresql://studiofs:p@ss%20word@db",
    );
    assert!(!postgresql_diagnostic.contains("p%40ss%20word"));
    assert!(!postgresql_diagnostic.contains("p@ss%20word"));
}
