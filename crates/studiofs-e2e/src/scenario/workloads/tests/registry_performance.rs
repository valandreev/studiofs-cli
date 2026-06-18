use super::*;

#[test]
fn registry_exposes_postgres_outage_recovery_scenario() {
    let scenarios = available_scenarios();

    assert!(scenarios.iter().any(|scenario| {
        scenario.name == POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY
            && scenario.requires_studiofs_bin
            && scenario.description.contains("PostgreSQL outage")
    }));
    assert!(resolve_scenario(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY).is_some());
    assert!(resolve_scenario("postgres-outage").is_some());
}

#[test]
fn registry_exposes_rustfs_outage_recovery_scenario() {
    let scenarios = available_scenarios();

    assert!(scenarios.iter().any(|scenario| {
        scenario.name == RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY
            && scenario.requires_studiofs_bin
            && scenario.description.contains("RustFS/S3 outage")
    }));
    assert!(resolve_scenario(RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY).is_some());
    assert!(resolve_scenario("rustfs-outage").is_some());
    assert!(resolve_scenario("s3-outage").is_some());
}

#[test]
fn registry_exposes_mounted_client_crash_restart_recovery_scenario() {
    let scenarios = available_scenarios();

    assert!(scenarios.iter().any(|scenario| {
        scenario.name == MOUNTED_CLIENT_CRASH_RESTART_RECOVERY
            && scenario.requires_studiofs_bin
            && scenario.description.contains("crash/restart")
    }));
    assert!(resolve_scenario(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY).is_some());
    assert!(resolve_scenario("client-crash-restart").is_some());
    assert!(resolve_scenario("crash-restart").is_some());
}

#[test]
fn registry_exposes_mounted_performance_probe_scenario_99_alias() {
    let scenarios = available_scenarios();

    assert!(scenarios.iter().any(|scenario| {
        scenario.name == MOUNTED_PERFORMANCE_PROBE
            && scenario.requires_studiofs_bin
            && scenario.description.contains("4GiB")
    }));
    assert!(resolve_scenario(MOUNTED_PERFORMANCE_PROBE).is_some());
    assert!(resolve_scenario("performance-probe").is_some());
    assert!(resolve_scenario("99").is_some());
}

#[test]
fn registry_exposes_mounted_fuse_write_baseline_sfs469_scenario() {
    let scenarios = available_scenarios();
    let scenario = scenarios
        .iter()
        .find(|scenario| scenario.name == MOUNTED_FUSE_WRITE_BASELINE)
        .expect("SFS-469 FUSE write baseline scenario should be registered");

    assert!(scenario.requires_studiofs_bin);
    assert!(scenario.description.contains("SFS-469"));
    assert!(
        scenario
            .description
            .contains("macOS/Linux FUSE write baseline")
    );
    assert!(resolve_scenario(MOUNTED_FUSE_WRITE_BASELINE).is_some());
    assert!(resolve_scenario("fuse-write-baseline").is_some());
    assert!(resolve_scenario("sfs-469").is_some());
    assert!(resolve_scenario("98").is_some());
}

#[test]
fn registry_exposes_mounted_fuse_upload_backlog_read_sfs531_scenario() {
    let scenarios = available_scenarios();
    let scenario = scenarios
        .iter()
        .find(|scenario| scenario.name == MOUNTED_FUSE_UPLOAD_BACKLOG_READ_REGRESSION)
        .expect("SFS-531 FUSE upload-backlog read regression scenario should be registered");

    assert!(scenario.requires_studiofs_bin);
    assert!(scenario.description.contains("SFS-531"));
    assert!(scenario.description.contains("mounted-write"));
    assert!(resolve_scenario(MOUNTED_FUSE_UPLOAD_BACKLOG_READ_REGRESSION).is_some());
    assert!(resolve_scenario("fuse-upload-backlog-read-regression").is_some());
    assert!(resolve_scenario("sfs-531").is_some());
    assert!(resolve_scenario("531").is_some());
}

#[test]
fn sfs531_probe_defaults_reproduce_mounted_upload_backlog_read_shape() {
    let defaults = PerformanceProbeConfigDefaults::sfs531_upload_backlog_read_regression();

    assert_eq!(defaults.file_bytes, 256 * 1024 * 1024);
    assert_eq!(defaults.chunk_bytes, 4 * 1024 * 1024);
    assert_eq!(defaults.read_passes, 1);
    assert_eq!(defaults.sample_delay_ms, 250);
    assert!(defaults.wait_for_upload_drain);
    assert_eq!(defaults.upload_drain_timeout_secs, 180);
    assert_eq!(
        defaults.seed_mode,
        Some(PerformanceProbeSeedMode::MountedWrite)
    );
}

#[test]
fn performance_probe_seed_mode_summary_records_read_only_seeded_backend() {
    let config = PerformanceProbeConfig {
        file_bytes: 1024,
        chunk_bytes: 256,
        read_passes: 3,
        sample_delay: Duration::from_millis(10),
        wait_for_upload_drain: false,
        upload_drain_timeout: Duration::from_secs(1),
        upload_drain_poll: Duration::from_millis(25),
        seed_mode: PerformanceProbeSeedMode::ReadOnlySeeded,
    };

    let summary = PerformanceProbeConfigSummary::from(config);

    assert_eq!(summary.seed_mode, "read_only_seeded");
}

#[test]
fn performance_probe_read_only_seed_cache_budget_keeps_headroom_above_payload() {
    let file_bytes = 4 * 1024 * 1024 * 1024;

    let cache_budget_bytes = seeded_vfs_cache_budget_bytes(file_bytes);

    assert!(
        cache_budget_bytes > file_bytes,
        "seeded read-only probe cache budget must leave room beyond the payload"
    );
    assert_eq!(
        cache_budget_bytes - file_bytes,
        96 * 1024 * 1024,
        "seeded read-only probe should include dirty-limit and eviction headroom"
    );
}

#[test]
fn performance_probe_macos_read_only_mount_uses_macfuse_backend_override() {
    let mut overrides = Vec::new();

    push_read_only_bridge_mount_overrides(&mut overrides);

    assert!(
        overrides
            .iter()
            .any(|(key, value)| key == "STUDIOFS_MOUNT_BACKEND" && value == "macfuse")
    );
}

#[test]
fn performance_probe_macos_read_only_mount_uses_read_only_mount_arg_override() {
    let mut overrides = Vec::new();

    push_read_only_bridge_mount_arg_overrides(&mut overrides);

    assert!(overrides.iter().any(|value| value == "--read-only"));
}
