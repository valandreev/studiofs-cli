//! Native Linux trash across independently mounted PostgreSQL/S3 clients.

use super::recoverable_trash_gvfs::GvfsSession;
use super::*;

pub(in crate::scenario) struct RecoverableTrash;
const NAME: &str = "mounted-recoverable-trash-two-client";
const LIMIT: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(200);
const FILE_BYTES: &[u8] = b"shared retained file\0exact bytes\n";
const CHILD_BYTES: &[u8] = b"shared retained directory child\xff\n";
const OUTSIDE_ALIAS: &str = "outside-hardlink.bin";
const GVFS_RESTORE_BYTES: &[u8] = b"gvfs trash:/// restore\0exact\n";
const GVFS_EMPTY_BYTES: &[u8] = b"gvfs trash:/// empty\xfe\n";

impl Scenario for RecoverableTrash {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: NAME,
            description: "Linux native trash list, remote reads, restart, restore and Clear across two mounted clients, plus GVFS trash:/// list/restore/delete",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        let config = context
            .client_controller_config
            .clone()
            .ok_or(ScenarioError::MissingComposeInfrastructure)?;
        let binary = config.studiofs_bin.clone();
        let mut controller = MultiClientController::new(config, SystemClientProcessExecutor);
        let result = probe(context, &binary, &mut controller);
        let cleanup = if matches!(
            result,
            Err(ScenarioError::MountedIoTimeout { .. } | ScenarioError::MountedIoWorkerLimit { .. })
        ) {
            controller.emergency_kill_all_no_wait("recoverable trash timeout")
        } else {
            controller.unmount_all()
        };
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(()), Err(cleanup)) => Err(cleanup.into()),
            (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
                context: NAME.into(),
                primary: Box::new(primary),
                cleanup: Box::new(cleanup.into()),
            }),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary: serde_json::Value =
            serde_json::from_slice(&fs::read(context.scenario_dir.join("trash-summary.json"))?)?;
        if summary["passed"] != true {
            return Err(io::Error::other("mounted trash acceptance incomplete").into());
        }
        Ok(vec![format!(
            "artifacts/scenarios/{NAME}/trash-summary.json"
        )])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
    fn unsupported_platform(&self) -> Option<&'static str> {
        (!cfg!(target_os = "linux")).then_some("Linux GIO/FUSE required")
    }
}

pub(super) fn checked_command(mut command: std::process::Command, label: &str) -> io::Result<()> {
    let output = command.output()?;
    if !output.status.success() {
        // Arguments/environment may contain credentials; report only the operation.
        return Err(io::Error::other(format!(
            "{label} failed with {}",
            output.status
        )));
    }
    Ok(())
}

pub(super) fn wait_absent(path: PathBuf) -> Result<(), ScenarioError> {
    run_bounded_mounted_io("shared trash removal convergence", LIMIT, move || {
        let deadline = Instant::now() + Duration::from_secs(80);
        loop {
            match fs::symlink_metadata(&path) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error),
                Ok(_) if Instant::now() >= deadline => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "shared trash entry remained",
                    ));
                }
                Ok(_) => thread::sleep(POLL),
            }
        }
    })
}

pub(super) fn expect_payload(path: &Path, expected: &[u8]) -> Result<(), ScenarioError> {
    let actual = wait_for_mounted_payload_readback(path, expected, LIMIT, POLL)?;
    if actual != expected {
        return Err(
            io::Error::other(format!("exact payload mismatch at {}", path.display())).into(),
        );
    }
    Ok(())
}

#[cfg(unix)]
fn real_identity(path: &Path) -> io::Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = fs::symlink_metadata(path)?;
    Ok((metadata.ino(), metadata.nlink()))
}

#[cfg(not(unix))]
fn real_identity(_path: &Path) -> io::Result<(u64, u64)> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Unix inode identity required",
    ))
}

fn wait_restored_identity(mount: PathBuf, original: u64) -> Result<[(u64, u64); 2], ScenarioError> {
    run_bounded_mounted_io(
        "original inode and outside hardlink convergence",
        LIMIT,
        move || {
            let deadline = Instant::now() + Duration::from_secs(80);
            loop {
                let identities = [
                    real_identity(&mount.join("restored.bin")),
                    real_identity(&mount.join(OUTSIDE_ALIAS)),
                ];
                if identities
                    .iter()
                    .all(|identity| matches!(identity, Ok(value) if *value == (original, 2)))
                {
                    return Ok([(original, 2); 2]);
                }
                for identity in &identities {
                    if let Err(error) = identity
                        && error.kind() != io::ErrorKind::NotFound
                    {
                        return Err(io::Error::new(error.kind(), error.to_string()));
                    }
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "expected original real inode {original} with nlink=2 for restored file and outside alias; observed {identities:?}"
                        ),
                    ));
                }
                thread::sleep(POLL);
            }
        },
    )
}

fn probe(
    context: &ScenarioContext,
    binary: &Path,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<(), ScenarioError> {
    controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: vec!["client-a".into(), "client-b".into()],
        clean_existing_local_metadata: true,
    })?;
    let clients = controller.clients();
    let a = clients[0].clone();
    let b = clients[1].clone();
    fs::write(
        context.scenario_dir.join("clients.json"),
        serde_json::to_vec_pretty(&clients)?,
    )?;
    let mut retention = std::process::Command::new("timeout");
    retention
        .args(["45s"])
        .arg(binary)
        .args(["trash-days", &a.volume_name, "7"])
        .env("STUDIOFS_CLIENT_ROOT", &a.client_root);
    checked_command(retention, "enable shared trash retention")?;
    controller.mount_all()?;
    eprintln!("trash acceptance: two independent clients mounted");
    let uid = std::process::Command::new("id").arg("-u").output()?;
    if !uid.status.success() {
        return Err(io::Error::other("could not determine mount uid").into());
    }
    let trash = format!(".Trash-{}", String::from_utf8_lossy(&uid.stdout).trim());
    let a_mount = a.mount_point.clone();
    let (original_ino, original_nlink) =
        run_bounded_mounted_io("write file and directory payloads", LIMIT, move || {
            fs::create_dir(a_mount.join("album"))?;
            for (name, bytes) in [("note.bin", FILE_BYTES), ("album/child.bin", CHILD_BYTES)] {
                let mut file = fs::File::create(a_mount.join(name))?;
                file.write_all(bytes)?;
                file.sync_all()?;
            }
            fs::hard_link(a_mount.join("note.bin"), a_mount.join(OUTSIDE_ALIAS))?;
            // This is the real original inode, captured before GIO creates a
            // synthetic trash projection. Never use the trash-view inode as oracle.
            let original = real_identity(&a_mount.join("note.bin"))?;
            if original.1 != 2 || real_identity(&a_mount.join(OUTSIDE_ALIAS))? != original {
                return Err(io::Error::other(
                    "original file and outside alias must share one real inode with nlink=2",
                ));
            }
            Ok(original)
        })?;
    // Observe the peer namespace before retention without warming its payload cache.
    let peer = b.mount_point.clone();
    run_bounded_mounted_io("peer metadata publication", LIMIT, move || {
        let deadline = Instant::now() + Duration::from_secs(80);
        while !(peer.join("note.bin").is_file()
            && peer.join("album/child.bin").is_file()
            && real_identity(&peer.join(OUTSIDE_ALIAS))
                .is_ok_and(|identity| identity == (original_ino, 2)))
        {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "peer namespace did not converge",
                ));
            }
            thread::sleep(POLL);
        }
        Ok(())
    })?;
    let xdg = context.scenario_dir.join("isolated-xdg");
    fs::create_dir_all(xdg.join("data"))?;
    let writer = a.mount_point.clone();
    run_bounded_mounted_io("GIO trash file and directory", LIMIT, move || {
        for name in ["note.bin", "album"] {
            let mut gio = std::process::Command::new("timeout");
            gio.args(["30s", "gio", "trash"])
                .arg(writer.join(name))
                .env("HOME", &xdg)
                .env("XDG_DATA_HOME", xdg.join("data"))
                .env("XDG_CONFIG_HOME", xdg.join("config"))
                .env("XDG_CACHE_HOME", xdg.join("cache"));
            checked_command(gio, "GIO trash")?;
        }
        Ok(())
    })?;
    let files_b = b.mount_point.join(&trash).join("files");
    let info_b = b.mount_point.join(&trash).join("info");
    let peer_files = files_b.clone();
    let peer_info = info_b.clone();
    run_bounded_mounted_io("peer retained receipt publication", LIMIT, move || {
        let deadline = Instant::now() + Duration::from_secs(80);
        while !(peer_files.join("note.bin").is_file()
            && peer_files.join("album/child.bin").is_file()
            && peer_info.join("note.bin.trashinfo").is_file()
            && peer_info.join("album.trashinfo").is_file())
        {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "peer retained receipts did not converge",
                ));
            }
            thread::sleep(POLL);
        }
        Ok(())
    })?;
    eprintln!("trash acceptance: both retained receipts visible on peer before writer shutdown");
    // Stop the writer and restart the observer before its first payload read.
    // Its independent cache has only served namespace/sidecar metadata so far.
    controller.unmount("client-a")?;
    controller.restart("client-b")?;
    eprintln!("trash acceptance: writer stopped and peer restarted before first payload read");
    for (path, bytes) in [
        (files_b.join("note.bin"), FILE_BYTES),
        (files_b.join("album/child.bin"), CHILD_BYTES),
    ] {
        expect_payload(&path, bytes)?;
    }
    eprintln!("trash acceptance: peer exact retained file and directory bytes verified");
    let sidecar = bounded_mounted_read(
        info_b.join("note.bin.trashinfo"),
        "peer generated sidecar",
        LIMIT,
    )?;
    if !String::from_utf8_lossy(&sidecar).contains("Path=note.bin\n") {
        return Err(io::Error::other("peer trashinfo original path mismatch").into());
    }
    let listing = bounded_mounted_read_dir_names(files_b.clone(), "peer trash listing", LIMIT)?;
    if !["note.bin", "album"]
        .iter()
        .all(|name| listing.iter().any(|entry| entry == *name))
    {
        return Err(io::Error::other("shared trash listing missing receipt").into());
    }
    let restore_from = files_b.join("note.bin");
    let restore_to = b.mount_point.join("restored.bin");
    let clear = files_b.join("album");
    run_bounded_mounted_io("peer restore and recursive Clear", LIMIT, move || {
        fs::rename(restore_from, restore_to)?;
        fs::remove_dir_all(clear)
    })?;
    let peer_identity = wait_restored_identity(b.mount_point.clone(), original_ino)?;
    expect_payload(&b.mount_point.join("restored.bin"), FILE_BYTES)?;
    expect_payload(&b.mount_point.join(OUTSIDE_ALIAS), FILE_BYTES)?;
    controller.mount("client-a")?;
    let writer_identity = wait_restored_identity(a.mount_point.clone(), original_ino)?;
    expect_payload(&a.mount_point.join("restored.bin"), FILE_BYTES)?;
    expect_payload(&a.mount_point.join(OUTSIDE_ALIAS), FILE_BYTES)?;
    for mount in [&a.mount_point, &b.mount_point] {
        for suffix in [
            "files/note.bin",
            "files/album",
            "info/note.bin.trashinfo",
            "info/album.trashinfo",
        ] {
            wait_absent(mount.join(&trash).join(suffix))?;
        }
    }
    eprintln!("trash acceptance: restored bytes and Clear converged across both mounts");
    let mut gvfs = GvfsSession::start(context.sandbox.join("gvfs"))?;
    let desktop = gvfs_trash_phase(&gvfs, &a.mount_point, &b.mount_point, &trash);
    let settled = gvfs.stop();
    let desktop = match (desktop, settled) {
        (Ok(desktop), Ok(_)) => desktop,
        (Err(primary), Ok(_)) => return Err(primary),
        (Ok(_), Err(cleanup)) => return Err(cleanup.into()),
        (Err(primary), Err(cleanup)) => {
            return Err(ScenarioError::CleanupAfterFailure {
                context: "private GVFS session".into(),
                primary: Box::new(primary),
                cleanup: Box::new(cleanup.into()),
            });
        }
    };
    eprintln!(
        "trash acceptance: GVFS trash:/// list, restore and delete converged; private session settled"
    );
    fs::write(
        context.scenario_dir.join("trash-summary.json"),
        serde_json::to_vec_pretty(&json!({
            "passed": true, "clients": 2, "writer_unmounted_before_peer_payload_read": true,
            "peer_restarted_before_payload_read": true, "file_sha256": bytes_sha256(FILE_BYTES),
            "directory_child_sha256": bytes_sha256(CHILD_BYTES),
            "shared_list": true, "peer_restore": true, "peer_recursive_clear": true,
            "writer_remount_observed_restored_bytes_and_clear": true,
            "original_real_inode": original_ino, "original_nlink": original_nlink,
            "outside_hardlink": OUTSIDE_ALIAS,
            "peer_restored_and_alias_identity": peer_identity,
            "writer_remounted_restored_and_alias_identity": writer_identity,
            "restored_and_alias_exact_bytes_on_both_clients": true,
            "gvfs_trash_uri": desktop,
            "gvfs_private_session_settled": true,
        }))?,
    )?;
    Ok(())
}

fn wait_trash_listing(
    gvfs: &GvfsSession,
    origins: &[PathBuf],
    present: bool,
) -> Result<Vec<(String, String)>, ScenarioError> {
    let deadline = Instant::now() + Duration::from_secs(80);
    loop {
        let listing = gvfs.list()?;
        // Writer and peer mounts are both aggregated, so one shared receipt is
        // listed once per mount; every origin must agree.
        if origins.iter().all(|orig| {
            listing
                .iter()
                .any(|(_, path)| Path::new(path) == orig.as_path())
                == present
        }) {
            return Ok(listing);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("trash:/// presence of {origins:?} != {present}; listing {listing:?}"),
            )
            .into());
        }
        thread::sleep(POLL);
    }
}

/// Desktop path: `gio trash` → `trash:///` listing → `--restore` → per-item
/// `gio remove trash:///…` (the Empty Trash backend delete),
/// asserting writer and peer StudioFS state after each step.
fn gvfs_trash_phase(
    gvfs: &GvfsSession,
    writer: &Path,
    peer: &Path,
    trash: &str,
) -> Result<serde_json::Value, ScenarioError> {
    let (restore_name, empty_name) = ("gvfs-restore.bin", "gvfs-empty.bin");
    let writer_root = writer.to_path_buf();
    run_bounded_mounted_io("write GVFS payloads", LIMIT, move || {
        for (name, bytes) in [
            (restore_name, GVFS_RESTORE_BYTES),
            (empty_name, GVFS_EMPTY_BYTES),
        ] {
            let mut file = fs::File::create(writer_root.join(name))?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        Ok(())
    })?;
    for (name, bytes) in [
        (restore_name, GVFS_RESTORE_BYTES),
        (empty_name, GVFS_EMPTY_BYTES),
    ] {
        expect_payload(&peer.join(name), bytes)?;
    }
    let receipts = |mount: &Path, name: &str| {
        let root = mount.join(trash);
        [
            root.join("files").join(name),
            root.join("info").join(format!("{name}.trashinfo")),
        ]
    };
    let assert_trashed = |name: &str, bytes: &[u8]| -> Result<(), ScenarioError> {
        for mount in [writer, peer] {
            wait_absent(mount.join(name))?;
            let [payload, info] = receipts(mount, name);
            expect_payload(&payload, bytes)?;
            let sidecar = bounded_mounted_read(info, "GVFS trashinfo receipt", LIMIT)?;
            if !String::from_utf8_lossy(&sidecar).contains(&format!("Path={name}\n")) {
                return Err(io::Error::other("GVFS receipt original path mismatch").into());
            }
        }
        Ok(())
    };

    // 1. Move to Trash, then 2. the entry must appear in trash:/// with its origin.
    let restore_orig = writer.join(restore_name);
    let restore_origins = [restore_orig.clone(), peer.join(restore_name)];
    gvfs.gio(
        &["trash".as_ref(), restore_orig.as_os_str()],
        "gio trash (restore case)",
    )?;
    let listing = wait_trash_listing(gvfs, &restore_origins, true)?;
    let restore_uri = listing
        .iter()
        .find(|(_, path)| *path == restore_orig.to_string_lossy())
        .map(|(uri, _)| uri.clone())
        .ok_or_else(|| io::Error::other("restore entry vanished from listing"))?;
    assert_trashed(restore_name, GVFS_RESTORE_BYTES)?;
    eprintln!("trash acceptance: GVFS listed {restore_uri}; receipts on writer and peer");

    // 3. Restore through the trash backend to the original path.
    gvfs.gio(
        &["trash".as_ref(), "--restore".as_ref(), restore_uri.as_ref()],
        "gio trash --restore",
    )?;
    wait_trash_listing(gvfs, &restore_origins, false)?;
    for mount in [writer, peer] {
        expect_payload(&mount.join(restore_name), GVFS_RESTORE_BYTES)?;
        for receipt in receipts(mount, restore_name) {
            wait_absent(receipt)?;
        }
    }
    eprintln!("trash acceptance: GVFS restore converged on writer and peer");

    // 4. Trash a second entry and purge it through the backend delete that
    // Empty Trash uses (`trash_item_delete`), scoped to this exact URI:
    // `gio trash --empty` would also purge the host user's other-volume trash.
    let empty_orig = writer.join(empty_name);
    let empty_origins = [empty_orig.clone(), peer.join(empty_name)];
    gvfs.gio(
        &["trash".as_ref(), empty_orig.as_os_str()],
        "gio trash (empty case)",
    )?;
    let listing = wait_trash_listing(gvfs, &empty_origins, true)?;
    assert_trashed(empty_name, GVFS_EMPTY_BYTES)?;
    let empty_uri = listing
        .iter()
        .find(|(_, path)| Path::new(path) == empty_orig.as_path())
        .map(|(uri, _)| uri.clone())
        .ok_or_else(|| io::Error::other("empty entry vanished from listing"))?;
    gvfs.gio(
        &["remove".as_ref(), empty_uri.as_ref()],
        "gio remove trash:/// entry",
    )?;
    let after_empty = wait_trash_listing(gvfs, &empty_origins, false)?;
    for mount in [writer, peer] {
        wait_absent(mount.join(empty_name))?;
        for receipt in receipts(mount, empty_name) {
            wait_absent(receipt)?;
        }
        expect_payload(&mount.join(restore_name), GVFS_RESTORE_BYTES)?;
    }
    eprintln!("trash acceptance: GVFS delete of {empty_uri} purged the receipt on writer and peer");
    Ok(json!({
        "listed_uri": restore_uri, "listed_orig_path": restore_orig,
        "listed_once_per_mount": restore_origins,
        "deleted_uri": empty_uri,
        "empty_listing_before_purge": listing.len(),
        "restore_sha256": bytes_sha256(GVFS_RESTORE_BYTES),
        "empty_sha256": bytes_sha256(GVFS_EMPTY_BYTES),
        "restored_exact_bytes_on_writer_and_peer": true,
        "empty_purged_receipts_on_writer_and_peer": true,
        "listing_after_empty": after_empty.len(),
    }))
}
