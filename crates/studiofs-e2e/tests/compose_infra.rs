//! Crate documentation for lint completion.
use std::cell::RefCell;
use std::path::PathBuf;
use std::time::Duration;

use studiofs_e2e::infra::{
    CommandExecutor, CommandOutput, ComposeConfig, DockerComposeStack, InfraError,
};
use tempfile::tempdir;

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
        "RustFS image must be pinned to a versioned tag"
    );
    assert!(
        !compose.contains("rustfs/rustfs:latest"),
        "RustFS image must not use latest in reproducible E2E infra"
    );
}

#[test]
fn compose_stack_allocates_project_scoped_ports_and_builds_waiting_up_command() {
    let temp = tempdir().unwrap();
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
    let temp = tempdir().unwrap();
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
    let post_start_checks = stack.post_start_preflight(&executor);
    stack.restart(&executor).unwrap();
    stack.stop_service("postgres", &executor).unwrap();
    stack.start_service("postgres", &executor).unwrap();
    let log_path = stack.collect_logs(&executor).unwrap();
    let diagnostics = stack.collect_optional_diagnostics(&executor).unwrap();
    stack.stop(&executor).unwrap();
    stack.teardown(&executor).unwrap();

    assert_eq!(
        std::fs::read_to_string(log_path).unwrap(),
        "postgres ready\nrustfs ready\n"
    );
    assert_eq!(post_start_checks.len(), 1);
    assert!(
        post_start_checks
            .iter()
            .all(|check| check.passed && check.name.starts_with("host health "))
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
    let temp = tempdir().unwrap();
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
fn post_start_preflight_retries_transient_host_rustfs_health_timeout() {
    let temp = tempdir().unwrap();
    let stack = DockerComposeStack::new(ComposeConfig {
        run_id: "compose-transient-host-health".to_string(),
        sandbox: temp.path().join("compose-transient-host-health"),
        compose_file: repo_root().join("docker/compose.e2e.yml"),
        port_range: Some("32300-32310".to_string()),
        wait_timeout_secs: 120,
    })
    .unwrap();
    let executor = TransientRustfsHealthExecutor::default();

    let checks = stack.post_start_preflight(&executor);

    assert_eq!(checks.len(), 1);
    assert!(checks.iter().all(|check| check.passed));
    assert_eq!(*executor.rustfs_health_attempts.borrow(), 2);
}

#[test]
fn post_start_preflight_heartbeat_reports_retry_progress() {
    let temp = tempdir().unwrap();
    let stack = DockerComposeStack::new(ComposeConfig {
        run_id: "compose-host-health-progress".to_string(),
        sandbox: temp.path().join("compose-host-health-progress"),
        compose_file: repo_root().join("docker/compose.e2e.yml"),
        port_range: Some("32300-32310".to_string()),
        wait_timeout_secs: 120,
    })
    .unwrap();
    let executor = TransientRustfsHealthExecutor::default();
    let mut heartbeats = Vec::new();

    let checks = stack.post_start_preflight_with_heartbeat(
        &executor,
        Duration::ZERO,
        &mut |port, attempts, _elapsed| {
            heartbeats.push((port.service.clone(), attempts));
        },
    );

    assert_eq!(checks.len(), 1);
    assert!(checks.iter().all(|check| check.passed));
    assert_eq!(heartbeats, vec![("rustfs".to_string(), 1)]);
}

#[test]
fn compose_stack_rejects_port_ranges_smaller_than_required_service_ports() {
    let temp = tempdir().unwrap();
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
    let temp = tempdir().unwrap();
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

#[test]
fn compose_stack_can_advance_to_highest_port_block_inside_range() {
    let temp = tempdir().unwrap();
    let mut stack = DockerComposeStack::new(ComposeConfig {
        run_id: "compose-next-port-block".to_string(),
        sandbox: temp.path().join("compose-next-port-block"),
        compose_file: repo_root().join("docker/compose.e2e.yml"),
        port_range: Some("32400-32410".to_string()),
        wait_timeout_secs: 120,
    })
    .unwrap();

    assert!(stack.advance_port_block().unwrap());
    assert_eq!(
        stack
            .ports()
            .iter()
            .map(|port| (port.service.as_str(), port.host_port))
            .collect::<Vec<_>>(),
        vec![
            ("postgres", 32407),
            ("rustfs", 32408),
            ("rustfs-console", 32409),
            ("toxiproxy-api", 32410),
        ]
    );
}

#[derive(Default)]
struct RecordingExecutor {
    calls: RefCell<Vec<studiofs_e2e::infra::CommandSpec>>,
}

impl CommandExecutor for RecordingExecutor {
    fn run(&self, command: &studiofs_e2e::infra::CommandSpec) -> std::io::Result<CommandOutput> {
        self.calls.borrow_mut().push(command.clone());
        let stdout = if command.args.contains(&"pg_dump".to_string()) {
            "postgres schema\n"
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

#[derive(Default)]
struct TransientRustfsHealthExecutor {
    rustfs_health_attempts: RefCell<usize>,
}

impl CommandExecutor for TransientRustfsHealthExecutor {
    fn run(&self, command: &studiofs_e2e::infra::CommandSpec) -> std::io::Result<CommandOutput> {
        if command.program == "curl"
            && command
                .args
                .iter()
                .any(|arg| arg == "http://127.0.0.1:32301/health")
        {
            let mut attempts = self.rustfs_health_attempts.borrow_mut();
            *attempts += 1;
            if *attempts == 1 {
                return Ok(CommandOutput {
                    status: 28,
                    stdout: String::new(),
                    stderr: "curl: (28) Operation timed out after 2000 milliseconds with 0 bytes received\n"
                        .to_string(),
                });
            }
        }
        Ok(CommandOutput {
            status: 0,
            stdout: String::new(),
            stderr: String::new(),
        })
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap()
        .to_path_buf()
}
