use anyhow::Context;
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

fn set_mountmgr_from_fsd_enabled() -> anyhow::Result<()> {
    let hklm = RegKey::predef(HKEY_LOCAL_MACHINE);
    for path in WINFSP_REG_PATHS {
        let (key, _) = hklm
            .create_subkey(path)
            .with_context(|| format!("failed to open/create HKLM\\{path}"))?;
        key.set_value(MOUNTMGR_VALUE_NAME, &1u32)
            .with_context(|| format!("failed to set HKLM\\{path}\\{MOUNTMGR_VALUE_NAME}=1"))?;
    }
    Ok(())
}

fn ps_single_quote(value: &str) -> String {
    value.replace('\'', "''")
}

fn run_elevated_bootstrap_process() -> anyhow::Result<()> {
    let exe = std::env::current_exe().context("failed to resolve current executable path")?;
    let exe = ps_single_quote(&exe.to_string_lossy());

    let script = format!(
        "$p = Start-Process -FilePath '{exe}' -ArgumentList @('internal','winfsp-bootstrap-mountmgr') -Verb RunAs -Wait -PassThru; exit $p.ExitCode"
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
        .context("failed to spawn elevated WinFSP bootstrap process")?;

    if !status.success() {
        anyhow::bail!(
            "one-time WinFSP bootstrap failed or was canceled (exit code {:?})",
            status.code()
        );
    }

    Ok(())
}

pub fn ensure_mountmgr_bootstrap() -> anyhow::Result<()> {
    if mountmgr_from_fsd_enabled() {
        return Ok(());
    }

    eprintln!(
        "Windows one-time setup: requesting Administrator approval to enable WinFSP Mount Manager integration (MountUseMountmgrFromFSD=1)."
    );
    run_elevated_bootstrap_process()?;

    if !mountmgr_from_fsd_enabled() {
        anyhow::bail!(
            "WinFSP one-time bootstrap did not persist MountUseMountmgrFromFSD=1. Please rerun mount as Administrator once."
        );
    }

    eprintln!("WinFSP one-time setup completed. Continuing mount...");
    Ok(())
}

pub fn run_internal_bootstrap_mountmgr() -> anyhow::Result<()> {
    set_mountmgr_from_fsd_enabled()?;
    Ok(())
}
