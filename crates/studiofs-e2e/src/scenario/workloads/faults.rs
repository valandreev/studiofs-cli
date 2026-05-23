//! Fault orchestration helpers for advanced mounted scenarios.

use super::*;

pub(in crate::scenario) fn run_fault_chaos_operations(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let mut operations = run_mounted_chaos_operations(
        context,
        &clients[..3],
        isolation,
        ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
        ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
    )?;
    let compose = compose_control_from_manifest(context)?;
    let executor = SystemCommandExecutor;
    operations.push(required_op(
        started,
        operations.len() + 1,
        &clients[0],
        "stop_postgres",
        "compose/postgres",
        compose
            .stop_service(POSTGRES_SERVICE_NAME, &executor)
            .map_err(ScenarioError::from),
    )?);
    let postgres_outage_path = isolation.path("postgres-outage.txt");
    let postgres_outage_payload = isolation.payload(
        "postgres_outage",
        &postgres_outage_path,
        "accepted while postgres is down\n",
    );
    operations.push(required_record(fs_op(
        started,
        operations.len() + 1,
        &clients[0],
        "local_write_during_pg_outage",
        &postgres_outage_path,
        || {
            let path = clients[0].mount_point.join(&postgres_outage_path);
            run_bounded_mounted_io(
                format!("fault chaos postgres outage write {}", path.display()),
                MOUNTED_WRITE_READBACK_TIMEOUT,
                move || write_parented(path, &postgres_outage_payload),
            )
            .map_err(scenario_error_to_io)
        },
    ))?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        &clients[0],
        "start_postgres",
        "compose/postgres",
        compose
            .start_service(POSTGRES_SERVICE_NAME, &executor)
            .map_err(ScenarioError::from),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        &clients[0],
        "crash_remount",
        clients[0].name.as_str(),
        controller
            .force_kill(&clients[0].name)
            .and_then(|()| controller.remount(&clients[0].name))
            .map_err(ScenarioError::from),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        &clients[1],
        "stop_rustfs",
        "compose/rustfs",
        compose
            .stop_service(RUSTFS_SERVICE_NAME, &executor)
            .map_err(ScenarioError::from),
    )?);
    let rustfs_outage_path = isolation.path("rustfs-outage.txt");
    let rustfs_outage_payload = isolation.payload(
        "rustfs_outage",
        &rustfs_outage_path,
        "accepted while rustfs is down\n",
    );
    operations.push(required_record(fs_op(
        started,
        operations.len() + 1,
        &clients[1],
        "local_write_during_s3_outage",
        &rustfs_outage_path,
        || {
            let path = clients[1].mount_point.join(&rustfs_outage_path);
            run_bounded_mounted_io(
                format!("fault chaos rustfs outage write {}", path.display()),
                MOUNTED_WRITE_READBACK_TIMEOUT,
                move || write_parented(path, &rustfs_outage_payload),
            )
            .map_err(scenario_error_to_io)
        },
    ))?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        &clients[1],
        "start_rustfs",
        "compose/rustfs",
        compose
            .start_service(RUSTFS_SERVICE_NAME, &executor)
            .map_err(ScenarioError::from),
    )?);
    operations.push(fs_op(
        started,
        operations.len() + 1,
        &clients[3],
        "cold_observer_readdir",
        &isolation.root,
        || {
            let path = clients[3].mount_point.join(&isolation.root);
            run_bounded_mounted_io(
                format!("fault chaos cold observer readdir {}", path.display()),
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                move || fs::read_dir(path).map(|_| ()),
            )
            .map_err(scenario_error_to_io)
        },
    ));
    Ok(operations)
}
