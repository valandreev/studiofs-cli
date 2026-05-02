use std::cell::{Cell, RefCell};
use std::io;
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;

use studiofs_e2e::client::{
    ClientCommand, ClientCommandOutput, ClientControllerConfig, ClientLifecycleState,
    ClientProcess, ClientProcessExecutor, MultiClientController, S3Config,
};
use tempfile::tempdir;

#[test]
fn controller_bootstraps_mounts_restarts_and_kills_isolated_clients() {
    let temp = tempdir().unwrap();
    let calls = Rc::new(RefCell::new(Vec::new()));
    let killed = Rc::new(Cell::new(0));
    let executor = RecordingExecutor {
        calls: Rc::clone(&calls),
        killed: Rc::clone(&killed),
        next_pid: Cell::new(100),
    };
    let config = ClientControllerConfig {
        studiofs_bin: Path::new("studiofs-test-bin").to_path_buf(),
        sandbox: temp.path().join("run-1"),
        volume_name: "projectfs".to_string(),
        meta_dsn: "postgres://studiofs:studiofs@127.0.0.1:25432".to_string(),
        s3: S3Config {
            endpoint: "http://127.0.0.1:29000".to_string(),
            bucket: "studiofs-e2e".to_string(),
            access_key: "studiofs".to_string(),
            secret_key: "studiofs123".to_string(),
            region: "us-east-1".to_string(),
        },
        mount_ready_timeout: Duration::from_millis(1),
    };
    let mut controller = MultiClientController::new(config, executor);

    controller
        .create_clients(["client-a", "client-b", "client-c"])
        .unwrap();
    controller.init_first_client().unwrap();
    controller.discover_additional_clients().unwrap();
    controller.mount_all().unwrap();
    controller.restart("client-b").unwrap();
    controller.force_kill_all().unwrap();

    let clients = controller.clients();
    assert_eq!(clients.len(), 3);
    assert!(clients.iter().all(|client| client.client_root.exists()));
    assert_ne!(clients[0].client_root, clients[1].client_root);
    assert_ne!(clients[1].cache_dir, clients[2].cache_dir);
    assert_eq!(
        clients[0].local_metadata_db,
        clients[0]
            .client_root
            .join("meta")
            .join("projectfs")
            .join("projectfs.db")
    );
    assert!(clients[0].stdout_log.ends_with("studiofs.stdout.log"));
    assert!(clients[0].stderr_log.ends_with("studiofs.stderr.log"));
    assert!(clients[0].tracing_log.ends_with("studiofs-tracing.log"));
    assert!(
        clients
            .iter()
            .all(|client| client.state == ClientLifecycleState::Killed)
    );

    let calls = calls.borrow();
    assert_eq!(calls[0].args[0], "init");
    assert_eq!(calls[1].args[0], "discover");
    assert_eq!(calls[2].args[0], "discover");
    assert_eq!(
        calls.iter().filter(|call| call.action == "mount").count(),
        4
    );
    assert!(calls.iter().all(|call| {
        call.env
            .iter()
            .any(|(key, _)| key == "STUDIOFS_CLIENT_ROOT")
    }));
    assert!(calls.iter().all(|call| {
        call.env
            .iter()
            .any(|(key, _)| key == "STUDIOFS_DAEMON_LOG_DIR")
    }));
    assert!(
        calls
            .iter()
            .all(|call| call.stdout_log.ends_with("studiofs.stdout.log"))
    );
    assert!(
        calls
            .iter()
            .all(|call| call.stderr_log.ends_with("studiofs.stderr.log"))
    );
    assert_eq!(killed.get(), 3);
}

#[derive(Clone)]
struct RecordingExecutor {
    calls: Rc<RefCell<Vec<ClientCommand>>>,
    killed: Rc<Cell<usize>>,
    next_pid: Cell<u32>,
}

impl ClientProcessExecutor for RecordingExecutor {
    fn run(&self, command: &ClientCommand) -> io::Result<ClientCommandOutput> {
        self.calls.borrow_mut().push(command.clone());
        Ok(ClientCommandOutput {
            status: 0,
            stdout: String::new(),
            stderr: String::new(),
        })
    }

    fn spawn(&self, command: &ClientCommand) -> io::Result<Box<dyn ClientProcess>> {
        self.calls.borrow_mut().push(command.clone());
        let pid = self.next_pid.get();
        self.next_pid.set(pid + 1);
        Ok(Box::new(RecordingProcess {
            pid,
            killed: Rc::clone(&self.killed),
        }))
    }

    fn wait_until_mount_ready(
        &self,
        _command: &ClientCommand,
        _timeout: Duration,
    ) -> io::Result<()> {
        Ok(())
    }
}

struct RecordingProcess {
    pid: u32,
    killed: Rc<Cell<usize>>,
}

impl ClientProcess for RecordingProcess {
    fn id(&self) -> u32 {
        self.pid
    }

    fn try_wait(&mut self) -> io::Result<Option<i32>> {
        Ok(None)
    }

    fn kill(&mut self) -> io::Result<()> {
        self.killed.set(self.killed.get() + 1);
        Ok(())
    }
}
