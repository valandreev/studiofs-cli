//! Own the Linux tray separately so D-Bus cannot hold mount shutdown.

use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use super::MountArgs;
use crate::cli::umount::reap_child_with_timeout;

const TRAY_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

pub(crate) fn dispatch_mount_with_auto_tray(args: MountArgs) -> anyhow::Result<()> {
    if args.should_detach() {
        return args.dispatch();
    }
    let _tray = std::env::current_exe()
        .and_then(|exe| {
            TrayProcess::spawn(
                Command::new(exe)
                    .args([
                        "internal",
                        "tray",
                        "--",
                        &std::process::id().to_string(),
                        args.volume_name(),
                    ])
                    .stdout(Stdio::null()),
            )
        })
        .inspect_err(|error| {
            tracing::warn!(error = %error, "failed to spawn tray helper; continuing without tray");
        })
        .ok();
    args.dispatch()
}

struct TrayProcess(Child);

impl TrayProcess {
    fn spawn(command: &mut Command) -> std::io::Result<Self> {
        let parent_id = std::process::id();
        // SAFETY: the child closure uses only Linux syscalls and allocation-free errno results.
        unsafe {
            command.pre_exec(move || terminate_with_parent(parent_id));
        }
        command.stdin(Stdio::piped()).spawn().map(Self)
    }

    fn stop(&mut self) {
        drop(self.0.stdin.take());
        if !reap_child_with_timeout(&mut self.0, TRAY_SHUTDOWN_GRACE) {
            tracing::warn!(
                "tray helper did not stop; terminating it without delaying mount completion"
            );
            if let Err(error) = self.0.kill() {
                tracing::warn!(error = %error, "failed to terminate tray helper");
            }
        }
        if let Err(error) = self.0.wait() {
            tracing::warn!(error = %error, "failed to reap tray helper");
        }
    }
}

impl Drop for TrayProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

pub(super) fn terminate_with_parent(process_id: u32) -> std::io::Result<()> {
    // SAFETY: prctl sets only this helper's parent-death signal.
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // Parent exit before prctl must not leave a helper attached to a new parent.
    // SAFETY: getppid has no preconditions.
    if u32::try_from(unsafe { libc::getppid() }) != Ok(process_id) {
        return Err(std::io::Error::from_raw_os_error(libc::ECHILD));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;
    use std::time::Instant;

    #[test]
    #[ignore = "controlled by tools/test-linux-tray.py"]
    fn owned_backend_fixture() {
        use std::io::{BufRead, Read, Write};
        let mut child = TrayProcess::spawn(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tray::linux::tests::backend_drop_fixture",
                    "--ignored",
                    "--nocapture",
                ])
                .stdout(Stdio::piped()),
        )
        .unwrap();
        let mut output = std::io::BufReader::new(child.0.stdout.take().unwrap());
        assert!(
            output
                .by_ref()
                .lines()
                .any(|line| line.unwrap().ends_with("backend-ready"))
        );
        println!("owner-ready {}", child.0.id());
        std::io::stdout().flush().unwrap();
        let _ = std::io::stdin().read(&mut [0]).unwrap();
        child.stop();
        let status = child.0.try_wait().unwrap().unwrap();
        assert!(status.success() || status.signal() == Some(libc::SIGKILL));
    }

    #[test]
    #[ignore = "controlled by owned_backend_fixture"]
    fn backend_drop_fixture() {
        use std::io::{Read, Write};
        let tray = tray_icon::TrayIconBuilder::new()
            .with_icon(tray_icon::Icon::from_rgba(vec![0, 0, 0, 255], 1, 1).unwrap())
            .build()
            .unwrap();
        println!("backend-ready");
        std::io::stdout().flush().unwrap();
        let _ = std::io::stdin().read(&mut [0]).unwrap();
        drop(tray);
    }

    #[test]
    fn tray_owner_reaps_a_helper_that_ignores_eof() {
        let mut child =
            TrayProcess::spawn(Command::new("sh").args(["-c", "kill -STOP $$"])).unwrap();
        let start = Instant::now();
        child.stop();
        assert!(start.elapsed() < Duration::from_secs(5));
        assert_eq!(
            child.0.try_wait().unwrap().unwrap().signal(),
            Some(libc::SIGKILL)
        );
    }

    #[test]
    fn tray_owner_allows_a_helper_to_exit_on_eof() {
        let mut child =
            TrayProcess::spawn(Command::new("sh").args(["-c", "read line; exit 0"])).unwrap();
        child.stop();
        assert!(child.0.try_wait().unwrap().unwrap().success());
    }
}
