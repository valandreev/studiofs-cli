//! Process-level checks for volume-name validation at command dispatch.

use std::fs;
use std::process::{Command, Output, Stdio};

const COMMANDS: &[&str] = &["init", "discover", "mount", "destroy"];

const INVALID_VOLUME_NAMES: &[&str] = &[
    "",
    ".",
    "../sentinel",
    "/absolute",
    "slash/name",
    r"back\slash",
    r"C:\absolute",
    "C:relative",
    r"\\server\share",
    r"\\?\C:\device",
    r"\\.\PhysicalDrive0",
    "name:stream",
    "trailing.",
    "trailing ",
    ".. ",
];

const VALID_VOLUME_NAMES: &[&str] = &["name with spaces", "том"];

const UNREACHABLE_POSTGRES_DSN: &str =
    "host=127.0.0.1 port=0 user=unused dbname=unused connect_timeout=1";

fn run_cli(root: &std::path::Path, command: &str, name: &str) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_studiofs"));
    cmd.arg(command);

    match command {
        "init" => {
            cmd.arg(name).args([
                "--endpoint",
                "://invalid",
                "--bucket",
                "unused",
                "--access-key",
                "unused",
                "--secret-key",
                "unused",
                "--meta",
                UNREACHABLE_POSTGRES_DSN,
            ]);
        }
        "discover" => {
            cmd.arg(name).args(["--meta", UNREACHABLE_POSTGRES_DSN]);
        }
        "mount" => {
            cmd.arg(name).arg("Z:").arg("--detach");
        }
        "destroy" => {
            cmd.arg(name);
        }
        _ => unreachable!("test command must be known"),
    }

    cmd.env("STUDIOFS_CLIENT_ROOT", root)
        .env("HOME", root)
        .env("USERPROFILE", root)
        .env_remove("STUDIOFS_DAEMON_LOG_DIR")
        .env_remove("STUDIOFS_DAEMON_LOG_STEM")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run studiofs process")
}

fn output_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn command_dispatch_rejects_unsafe_volume_names_before_side_effects_or_prompts() {
    for &name in INVALID_VOLUME_NAMES {
        for command in COMMANDS {
            let client_root = studiofs_test_support::workspace_target_tempdir("volume-name-");
            let sentinel_dir = client_root.path().join("sentinel");
            fs::create_dir(&sentinel_dir).expect("create traversal sentinel directory");
            let sentinel = client_root.path().join("sentinel.db");
            let sentinel_contents = b"must remain unchanged";
            fs::write(&sentinel, sentinel_contents).expect("write traversal sentinel");

            let output = run_cli(client_root.path(), command, name);
            let text = output_text(&output);

            assert!(
                !output.status.success(),
                "{command} accepted unsafe volume name {name:?}: {text}"
            );
            assert!(
                text.contains("invalid volume name"),
                "{command} did not report invalid volume name {name:?}: {text}"
            );
            assert!(
                !text.contains("Do you want to proceed?")
                    && !text.contains("To confirm, please type the volume name"),
                "{command} prompted before rejecting {name:?}: {text}"
            );
            assert_eq!(
                fs::read(&sentinel).expect("read traversal sentinel after command"),
                sentinel_contents,
                "{command} changed a file outside the metadata directory for {name:?}"
            );
            assert!(
                !client_root.path().join("meta").exists(),
                "{command} created metadata for unsafe volume name {name:?}"
            );
            assert!(
                !client_root.path().join("mounts").exists(),
                "{command} created mount state for unsafe volume name {name:?}"
            );
        }
    }
}

#[test]
fn existing_metadata_and_absolute_targets_are_unchanged_for_parent_and_absolute_names() {
    let absolute_target = studiofs_test_support::workspace_target_tempdir("volume-name-absolute-");
    let absolute_name = absolute_target.path().to_string_lossy().into_owned();
    let absolute_sentinel = absolute_target.path().join("absolute-sentinel");
    fs::write(&absolute_sentinel, b"absolute target must remain unchanged")
        .expect("write absolute target sentinel");
    let absolute_entry_count = fs::read_dir(absolute_target.path())
        .expect("read absolute target directory")
        .count();

    for (name, test_absolute_target) in [("..", false), (absolute_name.as_str(), true)] {
        for command in COMMANDS {
            let client_root =
                studiofs_test_support::workspace_target_tempdir("volume-name-existing-");
            let meta = client_root.path().join("meta");
            fs::create_dir(&meta).expect("create pre-existing metadata directory");
            fs::write(
                meta.join("metadata-sentinel"),
                b"metadata must remain unchanged",
            )
            .expect("write metadata sentinel");
            let outside_meta_sentinel = client_root.path().join("outside-meta-sentinel");
            fs::write(
                &outside_meta_sentinel,
                b"outside meta must remain unchanged",
            )
            .expect("write outside metadata sentinel");

            let output = run_cli(client_root.path(), command, name);
            let text = output_text(&output);

            assert!(
                !output.status.success() && text.contains("invalid volume name"),
                "{command} did not reject unsafe volume name {name:?}: {text}"
            );
            assert!(
                !text.contains("Do you want to proceed?")
                    && !text.contains("To confirm, please type the volume name"),
                "{command} prompted before rejecting {name:?}: {text}"
            );
            assert_eq!(
                fs::read(meta.join("metadata-sentinel")).expect("read metadata sentinel"),
                b"metadata must remain unchanged",
                "{command} changed metadata for {name:?}"
            );
            assert_eq!(
                fs::read_dir(&meta)
                    .expect("read metadata directory")
                    .count(),
                1,
                "{command} changed the metadata directory for {name:?}"
            );
            assert_eq!(
                fs::read(&outside_meta_sentinel).expect("read outside sentinel after command"),
                b"outside meta must remain unchanged"
            );
            if test_absolute_target {
                assert_eq!(
                    fs::read(&absolute_sentinel).expect("read absolute target sentinel"),
                    b"absolute target must remain unchanged"
                );
                assert_eq!(
                    fs::read_dir(absolute_target.path())
                        .expect("read absolute target directory after command")
                        .count(),
                    absolute_entry_count,
                    "{command} changed the absolute target directory for {name:?}"
                );
            }
        }
    }
}

#[test]
fn ordinary_space_and_unicode_volume_names_pass_name_validation() {
    for &name in VALID_VOLUME_NAMES {
        for command in COMMANDS {
            let client_root = studiofs_test_support::workspace_target_tempdir("volume-name-valid-");
            let output = run_cli(client_root.path(), command, name);
            let text = output_text(&output);

            assert!(
                !output.status.success(),
                "{command} unexpectedly completed without local or remote setup for {name:?}: {text}"
            );
            assert!(
                !text.contains("invalid volume name"),
                "{command} rejected valid volume name {name:?}: {text}"
            );
        }
    }
}
