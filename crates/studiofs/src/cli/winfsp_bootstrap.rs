use std::fmt::{Display, Formatter};
use std::io::{self, IsTerminal};
use std::process::Command;
use winreg::RegKey;
use winreg::enums::HKEY_LOCAL_MACHINE;

const WINFSP_REG_PATHS: [&str; 2] = ["SOFTWARE\\WOW6432Node\\WinFsp", "SOFTWARE\\WinFsp"];
const MOUNTMGR_VALUE_NAME: &str = "MountUseMountmgrFromFSD";

fn mountmgr_from_fsd_enabled() -> bool {
    let hklm = RegKey::predef(HKEY_LOCAL_MACHINE);
    WINFSP_REG_PATHS.iter().any(|path| {
        hklm.open_subkey(path)
            .ok()
            .and_then(|key| key.get_value::<u32, _>(MOUNTMGR_VALUE_NAME).ok())
            .is_some_and(|value| value == 1)
    })
}

#[derive(Debug)]
pub enum MountmgrBootstrapError {
    Registry {
        op: &'static str,
        path: &'static str,
        detail: String,
    },
    Process(io::Error),
    ExitNotZero(Option<i32>),
    NonInteractiveTerminal,
}

impl Display for MountmgrBootstrapError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Registry { op, path, detail } => {
                write!(f, "{op} registry path {path} failed: {detail}")
            }
            Self::Process(err) => write!(f, "bootstrap process failed: {err}"),
            Self::ExitNotZero(code) => write!(
                f,
                "one-time WinFSP bootstrap failed or was canceled (exit code {code:?})"
            ),
            Self::NonInteractiveTerminal => write!(
                f,
                "WinFSP one-time bootstrap requires an interactive terminal for UAC approval"
            ),
        }
    }
}

impl std::error::Error for MountmgrBootstrapError {}

fn set_mountmgr_enabled() -> Result<(), MountmgrBootstrapError> {
    let hklm = RegKey::predef(HKEY_LOCAL_MACHINE);
    for path in WINFSP_REG_PATHS {
        let (key, _) =
            hklm.create_subkey(path)
                .map_err(|err| MountmgrBootstrapError::Registry {
                    op: "open/create",
                    path,
                    detail: err.to_string(),
                })?;
        key.set_value(MOUNTMGR_VALUE_NAME, &1u32).map_err(|err| {
            MountmgrBootstrapError::Registry {
                op: "set",
                path,
                detail: err.to_string(),
            }
        })?;
    }
    Ok(())
}

fn ps_single_quote(value: &str) -> String {
    value.replace('\'', "''")
}

fn run_elevated_mountmgr_bootstrap() -> Result<(), MountmgrBootstrapError> {
    let exe = std::env::current_exe().map_err(MountmgrBootstrapError::Process)?;
    let exe = ps_single_quote(&exe.to_string_lossy());

    let script = format!(
        "$ErrorActionPreference = 'Stop'; \
try {{ \
  $p = Start-Process -FilePath '{exe}' -ArgumentList @('internal','winfsp-bootstrap-mountmgr') -Verb RunAs -Wait -PassThru -ErrorAction Stop; \
  if ($null -eq $p -or $null -eq $p.ExitCode) {{ exit 1 }}; \
  exit $p.ExitCode \
}} catch {{ \
  exit 1 \
}}"
    );

    let status = Command::new("powershell")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            &script,
        ])
        .status()
        .map_err(MountmgrBootstrapError::Process)?;

    if !status.success() {
        return Err(MountmgrBootstrapError::ExitNotZero(status.code()));
    }

    Ok(())
}

pub fn ensure_mountmgr_bootstrap() -> Result<(), MountmgrBootstrapError> {
    if mountmgr_from_fsd_enabled() {
        return Ok(());
    }

    if !std::io::stdin().is_terminal()
        && !std::io::stdout().is_terminal()
        && !std::io::stderr().is_terminal()
    {
        return Err(MountmgrBootstrapError::NonInteractiveTerminal);
    }

    eprintln!(
        "Windows one-time setup: requesting Administrator approval to enable WinFSP Mount Manager integration (MountUseMountmgrFromFSD=1)."
    );
    run_elevated_mountmgr_bootstrap()?;

    if !mountmgr_from_fsd_enabled() {
        return Err(MountmgrBootstrapError::Registry {
            op: "read",
            path: "HKLM\\SOFTWARE\\WOW6432Node\\WinFsp / HKLM\\SOFTWARE\\WinFsp",
            detail: "mount manager flag did not persist after bootstrap".into(),
        });
    }

    eprintln!("WinFSP one-time setup completed. Continuing mount...");
    Ok(())
}

pub fn run_internal_bootstrap_mountmgr() -> Result<(), MountmgrBootstrapError> {
    set_mountmgr_enabled()?;
    Ok(())
}
