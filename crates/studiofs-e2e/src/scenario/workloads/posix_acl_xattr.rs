//! Mounted Linux FUSE rejection of unsupported POSIX ACL xattrs (SFS-934).

use super::*;

const SUMMARY: &str = "posix-acl-xattr.json";
const IO_TIMEOUT: Duration = Duration::from_secs(60);

pub(in crate::scenario) struct PosixAclXattrRejection;

impl Scenario for PosixAclXattrRejection {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: "mounted-posix-acl-xattr-rejection",
            description: "setfacl-style ACL xattrs fail through the kernel instead of being stored",
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
        let mut controller = MultiClientController::new(config, SystemClientProcessExecutor);
        let result = probe(context, &mut controller);
        let cleanup = if matches!(
            result,
            Err(ScenarioError::MountedIoTimeout { .. } | ScenarioError::MountedIoWorkerLimit { .. })
        ) {
            controller.emergency_kill_all_no_wait("posix acl xattr timeout")
        } else {
            controller.unmount_all()
        };
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(()), Err(cleanup)) => Err(cleanup.into()),
            (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
                context: "posix acl xattr".into(),
                primary: Box::new(primary),
                cleanup: Box::new(cleanup.into()),
            }),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary: serde_json::Value =
            serde_json::from_slice(&fs::read(context.scenario_dir.join(SUMMARY))?)?;
        let failures = acceptance_failures(&summary);
        if !failures.is_empty() {
            return Err(io::Error::other(format!(
                "posix acl xattr acceptance failed ({}): {summary}",
                failures.join("; ")
            ))
            .into());
        }
        Ok(Vec::new())
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

/// Each recorded mounted result must match what a filesystem without ACL support returns.
fn acceptance_failures(summary: &serde_json::Value) -> Vec<String> {
    let mut failures = Vec::new();
    for key in [
        "set_access_file",
        "set_default_dir",
        "get_access_file",
        "remove_access_file",
    ] {
        if summary[key].as_str() != Some("EOPNOTSUPP") {
            failures.push(format!("{key} = {}, want EOPNOTSUPP", summary[key]));
        }
    }
    if summary["set_user_control"].as_str() != Some("ok") {
        failures.push("user.* control xattr was not stored".into());
    }
    let names: Vec<&str> = summary["list_file"]
        .as_array()
        .map(|names| names.iter().filter_map(|name| name.as_str()).collect())
        .unwrap_or_default();
    if names
        .iter()
        .any(|name| name.starts_with("system.posix_acl_"))
    {
        failures.push("listxattr advertises a POSIX ACL name".into());
    }
    if !names.contains(&"user.studiofs-control") {
        failures.push("listxattr lost the user.* control xattr".into());
    }
    if summary["mode_before"] != summary["mode_after"] {
        failures.push("failed ACL set changed the mode bits".into());
    }
    failures
}

fn probe(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<(), ScenarioError> {
    controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: vec!["client-a".into()],
        clean_existing_local_metadata: true,
    })?;
    controller.mount_all()?;
    let mount = controller.clients()[0].mount_point.clone();
    let summary = run_bounded_mounted_io("posix acl xattr probe", IO_TIMEOUT, move || {
        mounted_acl_probe(&mount)
    })?;
    fs::write(
        context.scenario_dir.join(SUMMARY),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn mounted_acl_probe(mount: &Path) -> io::Result<serde_json::Value> {
    use std::os::unix::fs::PermissionsExt as _;

    let file = mount.join("acl.bin");
    let dir = mount.join("acl-dir");
    fs::write(&file, b"acl")?;
    fs::create_dir(&dir)?;
    let mode_before = fs::metadata(&file)?.permissions().mode();
    let acl = minimal_acl_xattr();
    let summary = json!({
        "set_access_file": xattr::set(&file, "system.posix_acl_access", &acl),
        "set_default_dir": xattr::set(&dir, "system.posix_acl_default", &acl),
        "get_access_file": xattr::get(&file, "system.posix_acl_access"),
        "remove_access_file": xattr::remove(&file, "system.posix_acl_access"),
        "set_user_control": xattr::set(&file, "user.studiofs-control", b"v"),
        "list_file": xattr::list(&file)?,
        "mode_before": mode_before,
        "mode_after": fs::metadata(&file)?.permissions().mode(),
    });
    Ok(summary)
}

#[cfg(not(target_os = "linux"))]
fn mounted_acl_probe(_mount: &Path) -> io::Result<serde_json::Value> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "system.posix_acl_* names are Linux kernel ACL xattrs; this scenario needs Linux FUSE",
    ))
}

/// A valid `setfacl` blob (user::rw-, group::r--, other::r--): the kernel parses ACL
/// xattrs before FUSE sees them, so an invalid value would fail with EINVAL instead.
#[cfg(any(target_os = "linux", test))]
fn minimal_acl_xattr() -> Vec<u8> {
    const VERSION: u32 = 2;
    let mut acl = VERSION.to_le_bytes().to_vec();
    for (tag, perm) in [(0x01_u16, 6_u16), (0x04, 4), (0x20, 4)] {
        acl.extend_from_slice(&tag.to_le_bytes());
        acl.extend_from_slice(&perm.to_le_bytes());
        acl.extend_from_slice(&u32::MAX.to_le_bytes());
    }
    acl
}

#[cfg(target_os = "linux")]
mod xattr {
    use std::ffi::CString;
    use std::io;
    use std::os::unix::ffi::OsStrExt as _;
    use std::path::Path;

    fn c_path(path: &Path) -> CString {
        CString::new(path.as_os_str().as_bytes()).expect("mount paths contain no NUL")
    }

    fn c_name(name: &str) -> CString {
        CString::new(name).expect("xattr names contain no NUL")
    }

    /// `ok` or the errno name, so the summary shows exactly what the kernel returned.
    fn outcome(rc: libc::ssize_t) -> String {
        if rc >= 0 {
            return "ok".into();
        }
        match io::Error::last_os_error().raw_os_error() {
            Some(libc::EOPNOTSUPP) => "EOPNOTSUPP".into(),
            Some(libc::ENODATA) => "ENODATA".into(),
            Some(libc::EINVAL) => "EINVAL".into(),
            Some(libc::EPERM) => "EPERM".into(),
            Some(libc::EACCES) => "EACCES".into(),
            Some(errno) => format!("errno {errno}"),
            None => "unknown error".into(),
        }
    }

    pub(super) fn set(path: &Path, name: &str, value: &[u8]) -> String {
        let (path, name) = (c_path(path), c_name(name));
        // SAFETY: both strings are NUL-terminated and `value` outlives the call.
        let rc = unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
            )
        };
        outcome(rc as libc::ssize_t)
    }

    pub(super) fn get(path: &Path, name: &str) -> String {
        let (path, name) = (c_path(path), c_name(name));
        let mut buf = [0_u8; 256];
        // SAFETY: both strings are NUL-terminated and `buf` is writable for its length.
        let rc = unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                buf.as_mut_ptr().cast(),
                buf.len(),
            )
        };
        outcome(rc)
    }

    pub(super) fn remove(path: &Path, name: &str) -> String {
        let (path, name) = (c_path(path), c_name(name));
        // SAFETY: both strings are NUL-terminated.
        let rc = unsafe { libc::removexattr(path.as_ptr(), name.as_ptr()) };
        outcome(rc as libc::ssize_t)
    }

    pub(super) fn list(path: &Path) -> io::Result<Vec<String>> {
        let path = c_path(path);
        let mut buf = [0_u8; 4096];
        // SAFETY: the path is NUL-terminated and `buf` is writable for its length.
        let rc = unsafe { libc::listxattr(path.as_ptr(), buf.as_mut_ptr().cast(), buf.len()) };
        let len = usize::try_from(rc).map_err(|_| io::Error::last_os_error())?;
        Ok(buf[..len]
            .split(|byte| *byte == 0)
            .filter(|name| !name.is_empty())
            .map(|name| String::from_utf8_lossy(name).into_owned())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_acl_blob_matches_the_kernel_xattr_layout() {
        let acl = minimal_acl_xattr();
        assert_eq!(acl.len(), 4 + 3 * 8);
        assert_eq!(&acl[..4], &2_u32.to_le_bytes());
        assert_eq!(
            &acl[4..6],
            &1_u16.to_le_bytes(),
            "first entry is ACL_USER_OBJ"
        );
    }

    #[test]
    fn acceptance_rejects_stored_or_advertised_acl() {
        let passing = json!({
            "set_access_file": "EOPNOTSUPP", "set_default_dir": "EOPNOTSUPP",
            "get_access_file": "EOPNOTSUPP", "remove_access_file": "EOPNOTSUPP",
            "set_user_control": "ok", "list_file": ["user.studiofs-control"],
            "mode_before": 33188, "mode_after": 33188,
        });
        assert!(acceptance_failures(&passing).is_empty());

        let mut stored = passing.clone();
        stored["set_access_file"] = json!("ok");
        stored["list_file"] = json!(["system.posix_acl_access", "user.studiofs-control"]);
        assert_eq!(acceptance_failures(&stored).len(), 2);
    }
}
