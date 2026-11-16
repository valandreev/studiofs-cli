//! Scenario-owned private GVFS session for desktop `trash:///` acceptance.
//!
//! `gvfsd` and `gvfsd-trash` run on a private `dbus-run-session` bus with a
//! sandboxed `HOME`/`XDG_*`, so the host user's session and home trash are
//! never touched. Closing the holder's stdin ends the bus; the D-Bus-activated
//! daemons exit with it, and `stop` proves that by scanning `/proc`.

use std::process::{Command, Stdio};

use super::*;

const SESSION_LIMIT: Duration = Duration::from_secs(20);
const GIO_TIMEOUT: &str = "30s";

pub(super) struct GvfsSession {
    holder: Option<std::process::Child>,
    root: PathBuf,
    bus: String,
}

impl GvfsSession {
    pub(super) fn start(root: PathBuf) -> io::Result<Self> {
        for dir in ["home", "data", "config", "cache", "runtime"] {
            fs::create_dir_all(root.join(dir))?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(root.join("runtime"), fs::Permissions::from_mode(0o700))?;
        }
        let address_file = root.join("bus-address");
        let mut command = Command::new("dbus-run-session");
        command
            .args([
                "--",
                "sh",
                "-c",
                r#"printf %s "$DBUS_SESSION_BUS_ADDRESS" > "$1.tmp" && mv "$1.tmp" "$1" && exec cat >/dev/null"#,
                "sh",
            ])
            .arg(&address_file)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(fs::File::create(root.join("dbus-run-session.stderr.log"))?);
        sandbox_env(&mut command, &root);
        let holder = command.spawn()?;
        let mut session = Self {
            holder: Some(holder),
            root,
            bus: String::new(),
        };
        let deadline = Instant::now() + SESSION_LIMIT;
        loop {
            match fs::read_to_string(&address_file) {
                Ok(bus) if !bus.is_empty() => {
                    session.bus = bus;
                    return Ok(session);
                }
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            if let Some(holder) = session.holder.as_mut()
                && let Some(status) = holder.try_wait()?
            {
                return Err(io::Error::other(format!(
                    "private dbus-run-session exited early with {status}"
                )));
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "private session bus address was not published",
                ));
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    /// Runs `gio <args>` on the private bus; returns stdout or a redacted error.
    pub(super) fn gio(&self, args: &[&std::ffi::OsStr], label: &str) -> io::Result<String> {
        let mut command = Command::new("timeout");
        command.arg(GIO_TIMEOUT).arg("gio").args(args);
        sandbox_env(&mut command, &self.root);
        command.env("DBUS_SESSION_BUS_ADDRESS", &self.bus);
        let output = command.output()?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        // GLib 2.80 can exit 0 while printing `gio: ...: <error>`; stderr is the oracle.
        if !output.status.success() || stderr.contains("gio: ") {
            return Err(io::Error::other(format!(
                "{label} failed with {}: {}",
                output.status,
                stderr.trim()
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Returns `(trash URI, trash::orig-path)` for every top-level `trash:///` entry.
    pub(super) fn list(&self) -> io::Result<Vec<(String, String)>> {
        let output = self.gio(
            &[
                "list".as_ref(),
                "-u".as_ref(),
                "-a".as_ref(),
                "trash::orig-path".as_ref(),
                "trash:///".as_ref(),
            ],
            "gio list trash:///",
        )?;
        parse_trash_listing(&output)
    }

    /// Ends the private bus and proves no daemon from this session survives.
    pub(super) fn stop(&mut self) -> io::Result<usize> {
        let Some(mut holder) = self.holder.take() else {
            return Ok(0);
        };
        drop(holder.stdin.take());
        // A hung holder is killed, but its owned daemons are still reaped below.
        let holder_result =
            match crate::process_job::wait_for_child_exit(&mut holder, SESSION_LIMIT) {
                Ok(true) => Ok(()),
                outcome => {
                    let _ = holder.kill();
                    let _ = holder.wait();
                    outcome.and(Err(io::Error::other(
                        "private dbus-run-session did not exit after stdin close",
                    )))
                }
            };
        // Reap before reporting so a hung holder never orphans owned daemons.
        let survivors_result = self.reap_survivors();
        holder_result.and(survivors_result)
    }

    /// Waits for processes owned by this private `XDG_RUNTIME_DIR` to exit and
    /// kills any that outlive the bound.
    fn reap_survivors(&self) -> io::Result<usize> {
        let marker = self.root.join("runtime");
        let deadline = Instant::now() + SESSION_LIMIT;
        loop {
            let survivors = session_processes(&marker);
            if survivors.is_empty() {
                return Ok(0);
            }
            if Instant::now() >= deadline {
                for pid in &survivors {
                    let _ = Command::new("kill").arg(pid.to_string()).status();
                }
                return Err(io::Error::other(format!(
                    "{} private GVFS process(es) outlived the session bus",
                    survivors.len()
                )));
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for GvfsSession {
    fn drop(&mut self) {
        if self.holder.is_some()
            && let Err(error) = self.stop()
        {
            eprintln!("private GVFS session settlement failed: {error}");
        }
    }
}

fn sandbox_env(command: &mut Command, root: &Path) {
    command
        .env("HOME", root.join("home"))
        .env("XDG_DATA_HOME", root.join("data"))
        .env("XDG_CONFIG_HOME", root.join("config"))
        .env("XDG_CACHE_HOME", root.join("cache"))
        .env("XDG_RUNTIME_DIR", root.join("runtime"))
        .env_remove("DBUS_SESSION_BUS_ADDRESS")
        .env_remove("DISPLAY")
        .env_remove("WAYLAND_DISPLAY");
}

/// PIDs whose environment carries this session's private `XDG_RUNTIME_DIR`.
fn session_processes(runtime: &Path) -> Vec<u32> {
    let needle = format!("XDG_RUNTIME_DIR={}", runtime.display()).into_bytes();
    let Ok(entries) = fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
        .filter(|pid| {
            fs::read(format!("/proc/{pid}/environ"))
                .is_ok_and(|environ| environ.split(|byte| *byte == 0).any(|var| var == needle))
        })
        .collect()
}

fn parse_trash_listing(output: &str) -> io::Result<Vec<(String, String)>> {
    output
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let uri = line.split('\t').next().unwrap_or_default();
            let orig = line
                .split('\t')
                .find_map(|field| field.strip_prefix("trash::orig-path="));
            match orig {
                Some(orig) if uri.starts_with("trash:///") => Ok((uri.to_owned(), orig.to_owned())),
                _ => Err(io::Error::other(format!(
                    "unexpected trash:/// listing line: {line}"
                ))),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::parse_trash_listing;

    #[test]
    fn parses_gio_trash_listing_with_orig_path() {
        let parsed = parse_trash_listing(
            "trash:///a.bin\t3\t(regular)\ttrash::orig-path=/m/a.bin\n\
             trash:///%5Cmnt%5Ca\\x20b\t1\t(regular)\ttrash::orig-path=/m/a b\n",
        )
        .expect("listing");
        assert_eq!(
            parsed,
            vec![
                ("trash:///a.bin".into(), "/m/a.bin".into()),
                ("trash:///%5Cmnt%5Ca\\x20b".into(), "/m/a b".into()),
            ]
        );
        assert!(parse_trash_listing("trash:///x\t1\t(regular)\n").is_err());
        assert!(parse_trash_listing("").expect("empty").is_empty());
    }
}
