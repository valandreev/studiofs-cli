//! Compose infra contract tests.
//!
//! These tests validate the repository `docker/compose.e2e.yml` contract and the
//! command construction behavior of `DockerComposeStack`.
//!
//! The tests are intentionally "dry": they use in-process `CommandExecutor`
//! fakes to record and inspect the generated `CommandSpec`s rather than invoking
//! Docker on the host.

use std::cell::RefCell;
use std::path::PathBuf;

use studiofs_e2e::infra::{
    CommandExecutor, CommandOutput, ComposeConfig, DockerComposeStack, InfraError,
};

#[test]
fn compose_file_defines_required_services_healthchecks_and_project_ports() {
    let compose = std::fs::read_to_string(repo_root().join("docker/compose.e2e.yml")).unwrap();

    for service in ["postgres:", "rustfs:", "toxiproxy:"] {
        assert!(compose.contains(service), "missing service {service}");
    }
    assert!(
        compose.contains("postgres:18-alpine"),
        "PostgreSQL image must match the planned production major version"
    );
    assert!(
        compose.contains("pg_isready"),
        "postgres healthcheck must use pg_isready"
    );
    assert!(
        compose.contains("http://localhost:9000/health"),
        "rustfs healthcheck must probe the S3 service health endpoint"
    );
    assert!(
        compose.contains("STUDIOFS_E2E_POSTGRES_PORT")
            && compose.contains("STUDIOFS_E2E_RUSTFS_PORT")
            && compose.contains("STUDIOFS_E2E_TOXIPROXY_API_PORT"),
        "compose file must use runner-provided host port env vars"
    );
    assert!(
        compose.contains("127.0.0.1:${STUDIOFS_E2E_POSTGRES_PORT")
            && compose.contains("127.0.0.1:${STUDIOFS_E2E_RUSTFS_PORT")
            && compose.contains("127.0.0.1:${STUDIOFS_E2E_TOXIPROXY_API_PORT"),
        "compose services must bind to loopback only"
    );
    assert!(
        compose.contains("postgres-data:") && compose.contains("rustfs-data:"),
        "named volumes must be explicit so down --volumes can remove them"
    );
    assert!(
        compose.contains("rustfs/rustfs:1.0.0-alpha.94"),
        "RustFS image must stay pinned for baseline E2E infra"
    );
    assert!(!compose.contains("rustfs/rustfs:latest"));
}

#[test]
fn compose_preflight_reports_baseline_image_contract() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let stack = DockerComposeStack::new(ComposeConfig {
        run_id: "compose-baseline-contract".to_string(),
        sandbox: temp.path().join("compose-baseline-contract"),
        compose_file: repo_root().join("docker/compose.e2e.yml"),
        port_range: Some("31100-31110".to_string()),
        wait_timeout_secs: 90,
    })
    .unwrap();

    let checks = stack.preflight(&RecordingExecutor::default());
    let baseline = checks
        .iter()
        .find(|check| check.name == "compose baseline contract")
        .expect("baseline contract check");

    assert!(baseline.passed, "{baseline:?}");
    assert!(baseline.detail.contains("Local First baseline"));
}

#[test]
fn compose_preflight_accepts_quoted_baseline_images() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let compose_file = temp.path().join("compose.e2e.yml");
    std::fs::write(
        &compose_file,
        r#"
services:
  postgres:
    image: "postgres:18-alpine"
  rustfs:
    image: 'rustfs/rustfs:1.0.0-alpha.94'
"#,
    )
    .unwrap();
    let stack = DockerComposeStack::new(ComposeConfig {
        run_id: "compose-quoted-images".to_string(),
        sandbox: temp.path().join("compose-quoted-images"),
        compose_file,
        port_range: Some("31200-31210".to_string()),
        wait_timeout_secs: 90,
    })
    .unwrap();

    let checks = stack.preflight(&RecordingExecutor::default());
    let baseline = checks
        .iter()
        .find(|check| check.name == "compose baseline contract")
        .expect("baseline contract check");

    assert!(baseline.passed, "{baseline:?}");
}

#[test]
fn compose_preflight_accepts_yaml_aliases_for_baseline_images() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let compose_file = temp.path().join("compose.e2e.yml");
    std::fs::write(
        &compose_file,
        r#"
x-images:
  postgres: &postgres-image postgres:18-alpine
  rustfs: &rustfs-image rustfs/rustfs:1.0.0-alpha.94

services:
  postgres:
    image: *postgres-image
  rustfs:
    image: *rustfs-image
"#,
    )
    .unwrap();
    let stack = DockerComposeStack::new(ComposeConfig {
        run_id: "compose-aliased-images".to_string(),
        sandbox: temp.path().join("compose-aliased-images"),
        compose_file,
        port_range: Some("31300-31310".to_string()),
        wait_timeout_secs: 90,
    })
    .unwrap();

    let checks = stack.preflight(&RecordingExecutor::default());
    let baseline = checks
        .iter()
        .find(|check| check.name == "compose baseline contract")
        .expect("baseline contract check");

    assert!(baseline.passed, "{baseline:?}");
}

#[test]
fn compose_preflight_accepts_escaped_yaml_strings_for_baseline_images() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let compose_file = temp.path().join("compose.e2e.yml");
    std::fs::write(
        &compose_file,
        r#"
services:
  "postgres":
    image: "postgres:18-\u0061lpine"
  "rustfs":
    image: "rustfs/rustfs:1.0.0-alpha.94"
"#,
    )
    .unwrap();
    let stack = DockerComposeStack::new(ComposeConfig {
        run_id: "compose-escaped-images".to_string(),
        sandbox: temp.path().join("compose-escaped-images"),
        compose_file,
        port_range: Some("31400-31410".to_string()),
        wait_timeout_secs: 90,
    })
    .unwrap();

    let checks = stack.preflight(&RecordingExecutor::default());
    let baseline = checks
        .iter()
        .find(|check| check.name == "compose baseline contract")
        .expect("baseline contract check");

    assert!(baseline.passed, "{baseline:?}");
}

#[test]
fn compose_stack_allocates_project_scoped_ports_and_builds_waiting_up_command() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let stack = DockerComposeStack::new(ComposeConfig {
        run_id: "Compose.Run".to_string(),
        sandbox: temp.path().join("compose-infra-run"),
        compose_file: repo_root().join("docker/compose.e2e.yml"),
        port_range: Some("31000-31010".to_string()),
        wait_timeout_secs: 90,
    })
    .unwrap();

    assert_eq!(stack.project_name(), "studiofs-e2e-compose-run");
    assert_eq!(
        stack
            .ports()
            .iter()
            .map(|port| (port.service.as_str(), port.host_port))
            .collect::<Vec<_>>(),
        vec![
            ("postgres", 31000),
            ("rustfs", 31001),
            ("rustfs-console", 31002),
            ("toxiproxy-api", 31003),
        ]
    );

    let command = stack.up_command();
    assert_eq!(command.program, "docker");
    assert!(
        command
            .args
            .windows(2)
            .any(|pair| pair == ["compose", "--project-name"])
    );
    assert!(command.args.contains(&"up".to_string()));
    assert!(command.args.contains(&"--wait".to_string()));
    assert!(command.args.contains(&"--wait-timeout".to_string()));
    assert!(command.args.contains(&"90".to_string()));
    assert!(
        command
            .env
            .iter()
            .any(|(key, value)| key == "STUDIOFS_E2E_POSTGRES_PORT" && value == "31000")
    );
    assert_eq!(
        command.cwd,
        Some(repo_root().join("docker").canonicalize().unwrap())
    );
}

#[test]
fn compose_stack_lifecycle_collects_logs_and_tears_down_volumes() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let stack = DockerComposeStack::new(ComposeConfig {
        run_id: "compose-lifecycle".to_string(),
        sandbox: temp.path().join("compose-lifecycle"),
        compose_file: repo_root().join("docker/compose.e2e.yml"),
        port_range: Some("32000-32010".to_string()),
        wait_timeout_secs: 120,
    })
    .unwrap();
    let executor = RecordingExecutor::default();

    stack.prepare_artifacts().unwrap();
    stack.start(&executor).unwrap();
    stack.restart(&executor).unwrap();
    stack.stop_service("postgres", &executor).unwrap();
    stack.start_service("postgres", &executor).unwrap();
    let log_path = stack.collect_logs(&executor).unwrap();
    let diagnostics = stack.collect_optional_diagnostics(&executor).unwrap();
    let postgres_state = stack
        .collect_post_scenario_postgres_state(&executor, "basic-multi-client-smoke", "passed")
        .unwrap();
    stack.stop(&executor).unwrap();
    stack.teardown(&executor).unwrap();

    assert_eq!(
        std::fs::read_to_string(log_path).unwrap(),
        "postgres ready\nrustfs ready\n"
    );
    assert_eq!(
        diagnostics,
        vec![
            "postgres metadata dump".to_string(),
            "rustfs object listing".to_string()
        ]
    );
    assert_eq!(
        std::fs::read_to_string(
            temp.path()
                .join("compose-lifecycle")
                .join("artifacts/docker/postgres-schema.sql")
        )
        .unwrap(),
        "postgres schema\n"
    );
    assert_eq!(
        std::fs::read_to_string(
            temp.path()
                .join("compose-lifecycle")
                .join("artifacts/docker/rustfs-object-listing.txt")
        )
        .unwrap(),
        "/data/studiofs-e2e/object\n"
    );
    let postgres_state_json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(postgres_state).unwrap()).unwrap();
    assert_eq!(postgres_state_json["scenario"], "basic-multi-client-smoke");
    assert_eq!(postgres_state_json["scenario_status"], "passed");
    assert_eq!(postgres_state_json["volumes"][0]["name"], "projectfs");
    assert_eq!(postgres_state_json["volumes"][0]["event_count"], 3);
    let calls = executor.calls.borrow();
    assert!(calls.iter().any(|call| {
        call.args.contains(&"up".to_string()) && call.args.contains(&"--wait".to_string())
    }));
    assert!(
        calls
            .iter()
            .any(|call| call.args.contains(&"restart".to_string()))
    );
    assert!(calls.iter().any(|call| {
        call.args.contains(&"stop".to_string()) && call.args.contains(&"postgres".to_string())
    }));
    assert!(calls.iter().any(|call| {
        call.args.contains(&"stop".to_string())
            && call.args.contains(&"postgres".to_string())
            && call
                .env
                .iter()
                .any(|(key, value)| key == "STUDIOFS_E2E_POSTGRES_PORT" && value == "32000")
    }));
    assert!(calls.iter().any(|call| {
        call.args.contains(&"up".to_string())
            && call.args.contains(&"--wait".to_string())
            && call.args.contains(&"postgres".to_string())
    }));
    assert!(calls.iter().any(|call| {
        call.args.contains(&"up".to_string())
            && call.args.contains(&"--wait".to_string())
            && call.args.contains(&"postgres".to_string())
            && call
                .env
                .iter()
                .any(|(key, value)| key == "STUDIOFS_E2E_POSTGRES_PORT" && value == "32000")
    }));
    assert!(
        calls
            .iter()
            .any(|call| call.args.contains(&"stop".to_string()))
    );
    assert!(calls.iter().any(|call| {
        call.args.contains(&"down".to_string()) && call.args.contains(&"--volumes".to_string())
    }));
    assert!(
        calls
            .iter()
            .any(|call| call.args.contains(&"down".to_string()) && call.timeout.is_some()),
        "compose cleanup must be bounded so a hung docker compose down cannot hang the runner"
    );
    assert!(calls.iter().any(|call| {
        call.args.contains(&"exec".to_string())
            && call.args.contains(&"-T".to_string())
            && call.args.contains(&"postgres".to_string())
            && call.args.contains(&"pg_dump".to_string())
    }));
    assert!(calls.iter().any(|call| {
        call.args.contains(&"exec".to_string())
            && call.args.contains(&"-T".to_string())
            && call.args.contains(&"rustfs".to_string())
            && call
                .args
                .contains(&"find /data -mindepth 1 -maxdepth 8 -print | sort".to_string())
    }));
}

#[test]
fn optional_diagnostics_preserve_stdout_when_command_fails() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let stack = DockerComposeStack::new(ComposeConfig {
        run_id: "compose-diagnostic-failure".to_string(),
        sandbox: temp.path().join("compose-diagnostic-failure"),
        compose_file: repo_root().join("docker/compose.e2e.yml"),
        port_range: Some("32200-32210".to_string()),
        wait_timeout_secs: 120,
    })
    .unwrap();
    let executor = FailingDiagnosticsExecutor;

    let diagnostics = stack.collect_optional_diagnostics(&executor).unwrap();

    assert_eq!(diagnostics, vec!["rustfs object listing".to_string()]);
    let artifact_dir = temp
        .path()
        .join("compose-diagnostic-failure")
        .join("artifacts/docker");
    assert_eq!(
        std::fs::read_to_string(artifact_dir.join("postgres-schema.sql")).unwrap(),
        "partial schema stdout before failure\n"
    );
    let status: serde_json::Value = serde_json::from_slice(
        &std::fs::read(artifact_dir.join("postgres-schema.status.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(status["action"], "postgres metadata dump");
    assert_eq!(status["status"], 17);
    assert_eq!(status["collected"], false);
}

#[test]
fn compose_stack_rejects_port_ranges_smaller_than_required_service_ports() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let err = DockerComposeStack::new(ComposeConfig {
        run_id: "compose-small-range".to_string(),
        sandbox: temp.path().join("compose-small-range"),
        compose_file: repo_root().join("docker/compose.e2e.yml"),
        port_range: Some("32000-32002".to_string()),
        wait_timeout_secs: 120,
    })
    .unwrap_err();

    match err {
        InfraError::PortRangeTooSmall {
            available, needed, ..
        } => {
            assert_eq!(available, 3);
            assert_eq!(needed, 4);
        }
        other => panic!("expected small port range error, got {other:?}"),
    }
}

#[test]
fn compose_stack_accepts_whitespace_around_port_range_bounds() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let stack = DockerComposeStack::new(ComposeConfig {
        run_id: "compose-whitespace-range".to_string(),
        sandbox: temp.path().join("compose-whitespace-range"),
        compose_file: repo_root().join("docker/compose.e2e.yml"),
        port_range: Some(" 32100 - 32110 ".to_string()),
        wait_timeout_secs: 120,
    })
    .unwrap();

    assert_eq!(stack.ports()[0].host_port, 32100);
    assert_eq!(stack.ports()[3].host_port, 32103);
}

#[derive(Default)]
/// A `CommandExecutor` that records every call for later assertions in tests.
struct RecordingExecutor {
    /// Captured invocations in call order.
    calls: RefCell<Vec<studiofs_e2e::infra::CommandSpec>>,
}

impl CommandExecutor for RecordingExecutor {
    fn run(&self, command: &studiofs_e2e::infra::CommandSpec) -> std::io::Result<CommandOutput> {
        self.calls.borrow_mut().push(command.clone());
        let stdout = if command.args.contains(&"pg_dump".to_string()) {
            "postgres schema\n"
        } else if command.args.contains(&"psql".to_string()) {
            r#"[{"vol":1,"name":"projectfs","uuid":"vol-uuid","metadata_version":2,"event_count":3,"max_seq_id":3,"last_event_created_at":"2026-05-18T00:00:00Z"}]"#
        } else if command
            .args
            .contains(&"find /data -mindepth 1 -maxdepth 8 -print | sort".to_string())
        {
            "/data/studiofs-e2e/object\n"
        } else {
            "postgres ready\nrustfs ready\n"
        };
        Ok(CommandOutput {
            status: 0,
            stdout: stdout.to_string(),
            stderr: String::new(),
        })
    }
}

/// A `CommandExecutor` that forces `pg_dump` to fail while still returning a
/// partial stdout payload, for exercising optional diagnostics behavior.
struct FailingDiagnosticsExecutor;

impl CommandExecutor for FailingDiagnosticsExecutor {
    fn run(&self, command: &studiofs_e2e::infra::CommandSpec) -> std::io::Result<CommandOutput> {
        if command.args.contains(&"pg_dump".to_string()) {
            return Ok(CommandOutput {
                status: 17,
                stdout: "partial schema stdout before failure\n".to_string(),
                stderr: "pg_dump failed\n".to_string(),
            });
        }
        Ok(CommandOutput {
            status: 0,
            stdout: "/data/studiofs-e2e/object\n".to_string(),
            stderr: String::new(),
        })
    }
}

/// Returns the workspace repository root (two ancestors above `studiofs-e2e`).
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap()
        .to_path_buf()
}
