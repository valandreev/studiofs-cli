//! Mounted E2E acceptance scenarios for the ADR 0014 local-only scratch layer (SFS-1083).

use std::collections::BTreeSet;

use studiofs_data::janitor::JanitorObjectStore;

use super::*;
use crate::client::ClientControllerConfig;

pub(in crate::scenario) const SCRATCH_ISOLATION: &str = "mounted-scratch-isolation-two-client";
pub(in crate::scenario) const SCRATCH_ATOMIC_SAVE: &str = "mounted-scratch-atomic-save-two-client";
pub(in crate::scenario) const SCRATCH_DURABILITY: &str = "mounted-scratch-durability";
pub(in crate::scenario) const SCRATCH_CONFLICT: &str = "mounted-scratch-conflict-two-client";
pub(in crate::scenario) const SCRATCH_ORPHAN_RESTORE: &str =
    "mounted-scratch-orphan-restore-two-client";
pub(in crate::scenario) const SCRATCH_APPLEDOUBLE: &str = "mounted-scratch-appledouble-two-client";

const WAIT: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(200);
const DB_TIMEOUT: Duration = Duration::from_secs(15);

/// Scenario 1: scratch writes add zero `event_log` rows and zero S3 PUTs; peer never sees them.
pub(in crate::scenario) struct ScratchIsolation;

/// Scenario 2: atomic save (`shot.exr.tmp` -> `shot.exr`) publishes with zero writer payload copy.
pub(in crate::scenario) struct ScratchAtomicSave;

/// Scenario 3: scratch data survives clean unmount/remount and forced kill (single client).
pub(in crate::scenario) struct ScratchDurability;

/// Scenario 5: peer tracked entry wins over local scratch name (`*.scratch-conflict-<n>`).
pub(in crate::scenario) struct ScratchConflict;

/// Scenario 6: peer directory removal relocates local scratch children to orphans and restores cleanly.
pub(in crate::scenario) struct ScratchOrphanRestore;

/// Scenario 8: built-in `._*` AppleDouble sidecars keep real data locally on the creating client.
pub(in crate::scenario) struct ScratchAppleDouble;

impl Scenario for ScratchIsolation {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: SCRATCH_ISOLATION,
            description: "scratch writes add zero event_log rows and zero S3 PUTs and stay hidden from peers",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        const SUMMARY: &str = "scratch-isolation.json";
        run_with_controller(context, SCRATCH_ISOLATION, |config, controller| {
            controller.bootstrap_volume(VolumeBootstrapOptions {
                client_names: vec!["client-a".into(), "client-b".into()],
                clean_existing_local_metadata: true,
            })?;
            controller.mount_all()?;
            let a = controller.clients()[0].mount_point.clone();
            let observer = controller.clients()[1].clone();
            let b = observer.mount_point.clone();

            let rules = b"*.tmp\n/scratch-dir/\n";
            write_mounted_file_with_readback(
                a.join(".studiofsignore"),
                rules,
                "scratch isolation rules",
                true,
            )?;
            expect_bytes(&b.join(".studiofsignore"), rules)?;
            let probe_attempts = wait_for_rules_active(&a, &b, "client-a")?;

            // Wait for baseline publication (.studiofsignore + probe markers) to settle completely.
            let snap_before = wait_until_sync_safe(controller, "client-a")?;
            let _ = wait_until_sync_safe(controller, "client-b")?;
            let events_before = query_event_log_count(config)?;
            let s3_before = count_s3_objects(config)?;
            let completed_before = upload_counter(&snap_before, "completed_uploads");
            let encrypted_before = upload_counter(&snap_before, "encrypted_objects_written");

            let scratch_dir = a.join("scratch-dir").join("nested");
            run_bounded_mounted_io("create scratch nested dir", WAIT, move || {
                fs::create_dir_all(&scratch_dir)
            })?;
            let tmp_payload = vec![0x5a_u8; 64 * 1024];
            let nested_payload = vec![0x3c_u8; 64 * 1024];
            let autosave_payload = vec![0x71_u8; 16 * 1024];
            write_mounted_file_with_readback(
                a.join("work.tmp"),
                &tmp_payload,
                "scratch tmp write",
                true,
            )?;
            write_mounted_file_with_readback(
                a.join("scratch-dir").join("nested").join("render.exr"),
                &nested_payload,
                "scratch nested write",
                true,
            )?;
            write_mounted_file_with_readback(
                a.join("scratch-dir").join("autosave.blend1"),
                &autosave_payload,
                "scratch autosave write",
                true,
            )?;

            // Verify zero event_log rows and zero S3 PUTs before writing any tracked fence.
            let snap_after = wait_until_sync_safe(controller, "client-a")?;
            let events_after = query_event_log_count(config)?;
            let s3_after = count_s3_objects(config)?;
            let completed_after = upload_counter(&snap_after, "completed_uploads");
            let encrypted_after = upload_counter(&snap_after, "encrypted_objects_written");

            if events_after != events_before {
                return Err(io::Error::other(format!(
                    "scratch writes added event_log rows: {events_before} -> {events_after}"
                ))
                .into());
            }
            if s3_after != s3_before {
                return Err(io::Error::other(format!(
                    "scratch writes added S3 objects: {s3_before} -> {s3_after}"
                ))
                .into());
            }
            if completed_after != completed_before || encrypted_after != encrypted_before {
                return Err(io::Error::other(format!(
                    "scratch writes triggered uploads: completed {completed_before}->{completed_after}, encrypted {encrypted_before}->{encrypted_after}"
                ))
                .into());
            }

            // Write a tracked fence after the scratch writes to prove peer replication caught up.
            write_mounted_file_with_readback(
                a.join("sync-fence.txt"),
                b"fence\n",
                "scratch isolation fence",
                true,
            )?;
            expect_bytes(&b.join("sync-fence.txt"), b"fence\n")?;
            wait_for_mounted_paths_absent_until(
                std::slice::from_ref(&observer),
                &[
                    "work.tmp",
                    "scratch-dir",
                    "scratch-dir/nested/render.exr",
                    "scratch-dir/autosave.blend1",
                ],
                Instant::now() + WAIT,
                POLL,
                "scratch files stay invisible to peer",
            )?;

            // Local reads still return exact scratch payloads.
            expect_bytes(&a.join("work.tmp"), &tmp_payload)?;
            expect_bytes(
                &a.join("scratch-dir").join("nested").join("render.exr"),
                &nested_payload,
            )?;
            expect_bytes(
                &a.join("scratch-dir").join("autosave.blend1"),
                &autosave_payload,
            )?;

            write_summary(
                context,
                SUMMARY,
                &json!({
                    "event_log_delta": events_after - events_before,
                    "s3_object_delta": s3_after - s3_before,
                    "completed_uploads_delta": completed_after - completed_before,
                    "encrypted_objects_delta": encrypted_after - encrypted_before,
                    "peer_absent": true,
                    "rule_probe_attempts": probe_attempts,
                }),
            )
        })
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary = read_summary(context, "scratch-isolation.json")?;
        if summary["event_log_delta"] != 0
            || summary["s3_object_delta"] != 0
            || summary["completed_uploads_delta"] != 0
            || summary["encrypted_objects_delta"] != 0
            || summary["peer_absent"] != true
        {
            return Err(io::Error::other("scratch isolation acceptance incomplete").into());
        }
        Ok(vec![format!(
            "artifacts/scenarios/{SCRATCH_ISOLATION}/scratch-isolation.json"
        )])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

impl Scenario for ScratchAtomicSave {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: SCRATCH_ATOMIC_SAVE,
            description: "atomic save renames shot.exr.tmp to shot.exr and publishes without copying payload on writer",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        const SUMMARY: &str = "scratch-atomic-save.json";
        run_with_controller(context, SCRATCH_ATOMIC_SAVE, |_config, controller| {
            controller.bootstrap_volume(VolumeBootstrapOptions {
                client_names: vec!["client-a".into(), "client-b".into()],
                clean_existing_local_metadata: true,
            })?;
            controller.mount_all()?;
            let client_a = controller.clients()[0].clone();
            let a = client_a.mount_point.clone();
            let observer = controller.clients()[1].clone();
            let b = observer.mount_point.clone();

            let rules = b"*.tmp\n";
            write_mounted_file_with_readback(
                a.join(".studiofsignore"),
                rules,
                "atomic save rules",
                true,
            )?;
            expect_bytes(&b.join(".studiofsignore"), rules)?;
            let probe_attempts = wait_for_rules_active(&a, &b, "client-a")?;
            let _ = wait_until_sync_safe(controller, "client-a")?;

            let objects_before_write = list_cache_object_files(&client_a)?;

            let payload: Vec<u8> = (0..(128 * 1024))
                .map(|idx| ((idx * 31 + 17) & 0xff) as u8)
                .collect();
            let tmp_path = a.join("shot.exr.tmp");
            let final_path = a.join("shot.exr");
            write_synced_mounted_file(&tmp_path, &payload, "write shot.exr.tmp")?;

            // Wait for the scratch payload object to materialize in the local payload store.
            let objects_before_rename = wait_for_cache_object_growth(
                &client_a,
                &objects_before_write,
                "shot.exr.tmp local cache object materialization",
            )?;
            let ino_before = unix_ino(&tmp_path)?;

            // Confirm the scratch temp file is still invisible to the peer before rename.
            write_mounted_file_with_readback(
                a.join("pre-rename.marker"),
                b"ok\n",
                "pre-rename marker",
                true,
            )?;
            expect_bytes(&b.join("pre-rename.marker"), b"ok\n")?;
            wait_for_mounted_paths_absent_until(
                std::slice::from_ref(&observer),
                &["shot.exr.tmp", "shot.exr"],
                Instant::now() + WAIT,
                POLL,
                "shot.exr.tmp stays scratch before rename",
            )?;

            // Snapshot object files right before rename (includes pre-rename.marker's object).
            let objects_immediately_before_rename = list_cache_object_files(&client_a)?;

            let src = tmp_path.clone();
            let dst = final_path.clone();
            run_bounded_mounted_io("atomic rename shot.exr.tmp -> shot.exr", WAIT, move || {
                fs::rename(&src, &dst)
            })?;

            // Peer observes the promoted file with exact bytes.
            expect_bytes(&b.join("shot.exr"), &payload)?;
            let _ = wait_until_sync_safe(controller, "client-a")?;

            let ino_after = unix_ino(&final_path)?;
            if ino_before != ino_after {
                return Err(io::Error::other(format!(
                    "atomic-save promotion changed inode number: {ino_before:?} -> {ino_after:?}"
                ))
                .into());
            }

            let objects_after_rename = list_cache_object_files(&client_a)?;
            let added_after_rename: Vec<PathBuf> = objects_after_rename
                .difference(&objects_immediately_before_rename)
                .cloned()
                .collect();
            if !added_after_rename.is_empty() {
                return Err(io::Error::other(format!(
                    "atomic-save promotion created new writer payload object(s) instead of reusing scratch object: {added_after_rename:?}"
                ))
                .into());
            }

            wait_for_mounted_paths_absent_until(
                std::slice::from_ref(&observer),
                &["shot.exr.tmp"],
                Instant::now() + WAIT,
                POLL,
                "shot.exr.tmp absent on peer after rename",
            )?;

            write_summary(
                context,
                SUMMARY,
                &json!({
                    "peer_payload_verified": true,
                    "inode_preserved": ino_before == ino_after,
                    "ino": ino_after,
                    "objects_before_write": objects_before_write.len(),
                    "objects_before_rename": objects_before_rename.len(),
                    "objects_added_by_rename": added_after_rename.len(),
                    "rule_probe_attempts": probe_attempts,
                }),
            )
        })
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary = read_summary(context, "scratch-atomic-save.json")?;
        if summary["peer_payload_verified"] != true
            || summary["inode_preserved"] != true
            || summary["objects_added_by_rename"] != 0
        {
            return Err(io::Error::other("scratch atomic-save acceptance incomplete").into());
        }
        Ok(vec![format!(
            "artifacts/scenarios/{SCRATCH_ATOMIC_SAVE}/scratch-atomic-save.json"
        )])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

impl Scenario for ScratchDurability {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: SCRATCH_DURABILITY,
            description: "scratch data survives clean unmount/remount and forced kill on a single client",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        const SUMMARY: &str = "scratch-durability.json";
        run_with_controller(context, SCRATCH_DURABILITY, |_config, controller| {
            controller.bootstrap_volume(VolumeBootstrapOptions {
                client_names: vec!["client-a".into()],
                clean_existing_local_metadata: true,
            })?;
            controller.mount_all()?;
            let client = controller.clients()[0].clone();
            let a = client.mount_point.clone();

            let rules = b"*.tmp\n/autosave/\n";
            write_mounted_file_with_readback(
                a.join(".studiofsignore"),
                rules,
                "durability scratch rules",
                true,
            )?;
            let probe_attempts = wait_for_rules_active_single_client(
                controller,
                "client-a",
                &a,
                &client.volume_name,
            )?;

            let autosave_dir = a.join("autosave");
            run_bounded_mounted_io("create autosave dir", WAIT, move || {
                fs::create_dir(&autosave_dir)
            })?;
            let unmount_payload = b"scratch data surviving clean unmount and remount\0\n";
            let nested_payload = b"nested autosave surviving unmount and forced kill\0\n";
            let kill_payload = b"scratch data synced before forced kill\0\n";

            write_synced_mounted_file(
                &a.join("before-unmount.tmp"),
                unmount_payload,
                "write before-unmount.tmp",
            )?;
            write_synced_mounted_file(
                &a.join("autosave").join("scene.blend"),
                nested_payload,
                "write autosave/scene.blend",
            )?;

            // Clean unmount + remount.
            controller.unmount("client-a")?;
            controller.mount("client-a")?;
            expect_bytes(&a.join("before-unmount.tmp"), unmount_payload)?;
            expect_bytes(&a.join("autosave").join("scene.blend"), nested_payload)?;

            // Write another synced scratch file, then force-kill the mount process.
            write_synced_mounted_file(
                &a.join("before-kill.tmp"),
                kill_payload,
                "write before-kill.tmp",
            )?;
            controller.force_kill("client-a")?;
            controller.remount("client-a")?;

            expect_bytes(&a.join("before-unmount.tmp"), unmount_payload)?;
            expect_bytes(&a.join("autosave").join("scene.blend"), nested_payload)?;
            expect_bytes(&a.join("before-kill.tmp"), kill_payload)?;

            let list_output = controller.run_cli(
                "client-a",
                "scratch-list-after-kill",
                vec![
                    "scratch".into(),
                    "list".into(),
                    "--volume".into(),
                    client.volume_name.clone(),
                ],
            )?;
            for expected in [
                "before-unmount.tmp",
                "autosave/scene.blend",
                "before-kill.tmp",
            ] {
                if !list_output.stdout.contains(expected) {
                    return Err(io::Error::other(format!(
                        "scratch list after kill missing `{expected}`:\n{}",
                        list_output.stdout
                    ))
                    .into());
                }
            }

            write_summary(
                context,
                SUMMARY,
                &json!({
                    "survived_unmount_remount": true,
                    "survived_force_kill": true,
                    "rule_probe_attempts": probe_attempts,
                }),
            )
        })
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary = read_summary(context, "scratch-durability.json")?;
        if summary["survived_unmount_remount"] != true || summary["survived_force_kill"] != true {
            return Err(io::Error::other("scratch durability acceptance incomplete").into());
        }
        Ok(vec![format!(
            "artifacts/scenarios/{SCRATCH_DURABILITY}/scratch-durability.json"
        )])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

impl Scenario for ScratchConflict {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: SCRATCH_CONFLICT,
            description: "peer tracked entry wins over local scratch name and renames scratch to *.scratch-conflict-<n>",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        const SUMMARY: &str = "scratch-conflict.json";
        run_with_controller(context, SCRATCH_CONFLICT, |_config, controller| {
            controller.bootstrap_volume(VolumeBootstrapOptions {
                client_names: vec!["client-a".into(), "client-b".into()],
                clean_existing_local_metadata: true,
            })?;
            controller.mount_all()?;
            let a = controller.clients()[0].mount_point.clone();
            let observer = controller.clients()[1].clone();
            let b = observer.mount_point.clone();

            let rules = b"*.tmp\n";
            write_mounted_file_with_readback(
                a.join(".studiofsignore"),
                rules,
                "conflict scratch rules",
                true,
            )?;
            expect_bytes(&b.join(".studiofsignore"), rules)?;
            let probe_attempts = wait_for_rules_active(&a, &b, "client-a")?;

            let local_scratch_payload = b"client-a local scratch payload\n";
            let peer_tracked_payload = b"client-b peer tracked winner payload\n";

            // client-a creates local scratch file `clash.tmp`.
            write_mounted_file_with_readback(
                a.join("clash.tmp"),
                local_scratch_payload,
                "local scratch clash.tmp",
                true,
            )?;

            // client-b creates a tracked file and renames it to `clash.tmp` inside the tracked root:
            // per ADR 0014, a tracked file renamed to a matching name in a tracked parent stays tracked.
            write_mounted_file_with_readback(
                b.join("clash-source.txt"),
                peer_tracked_payload,
                "peer tracked source",
                true,
            )?;
            let src = b.join("clash-source.txt");
            let dst = b.join("clash.tmp");
            run_bounded_mounted_io("peer rename tracked -> clash.tmp", WAIT, move || {
                fs::rename(&src, &dst)
            })?;

            // client-a must see the peer's tracked `clash.tmp` win.
            expect_bytes(&a.join("clash.tmp"), peer_tracked_payload)?;

            // client-a's displaced scratch entry must appear as `clash.tmp.scratch-conflict-<n>`.
            let conflict_name = wait_for_conflict_entry(&a, "clash.tmp.scratch-conflict-")?;
            expect_bytes(&a.join(&conflict_name), local_scratch_payload)?;

            // The conflict copy stays local to client-a; client-b never sees it.
            write_mounted_file_with_readback(
                a.join("conflict-fence.txt"),
                b"ok\n",
                "conflict fence",
                true,
            )?;
            expect_bytes(&b.join("conflict-fence.txt"), b"ok\n")?;
            wait_for_mounted_paths_absent_until(
                std::slice::from_ref(&observer),
                &[conflict_name.as_str()],
                Instant::now() + WAIT,
                POLL,
                "conflict copy stays local to client-a",
            )?;

            write_summary(
                context,
                SUMMARY,
                &json!({
                    "tracked_winner_visible": true,
                    "conflict_name": conflict_name,
                    "conflict_payload_preserved": true,
                    "conflict_hidden_from_peer": true,
                    "rule_probe_attempts": probe_attempts,
                }),
            )
        })
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary = read_summary(context, "scratch-conflict.json")?;
        if summary["tracked_winner_visible"] != true
            || summary["conflict_payload_preserved"] != true
            || summary["conflict_hidden_from_peer"] != true
            || !summary["conflict_name"]
                .as_str()
                .is_some_and(|name| name.starts_with("clash.tmp.scratch-conflict-"))
        {
            return Err(io::Error::other("scratch conflict acceptance incomplete").into());
        }
        Ok(vec![format!(
            "artifacts/scenarios/{SCRATCH_CONFLICT}/scratch-conflict.json"
        )])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

impl Scenario for ScratchOrphanRestore {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: SCRATCH_ORPHAN_RESTORE,
            description: "peer directory removal relocates local scratch children to orphans and restores cleanly",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        const SUMMARY: &str = "scratch-orphan-restore.json";
        run_with_controller(context, SCRATCH_ORPHAN_RESTORE, |_config, controller| {
            controller.bootstrap_volume(VolumeBootstrapOptions {
                client_names: vec!["client-a".into(), "client-b".into()],
                clean_existing_local_metadata: true,
            })?;
            controller.mount_all()?;
            let client_a = controller.clients()[0].clone();
            let a = client_a.mount_point.clone();
            let observer = controller.clients()[1].clone();
            let b = observer.mount_point.clone();

            let rules = b"*.tmp\n";
            write_mounted_file_with_readback(
                a.join(".studiofsignore"),
                rules,
                "orphan scratch rules",
                true,
            )?;
            expect_bytes(&b.join(".studiofsignore"), rules)?;
            let probe_attempts = wait_for_rules_active(&a, &b, "client-a")?;

            // Create a tracked directory `shots/` visible to both clients.
            let shots_a = a.join("shots");
            run_bounded_mounted_io("create tracked dir shots", WAIT, move || {
                fs::create_dir(&shots_a)
            })?;
            wait_for_dir_present(&b.join("shots"))?;

            // client-a creates a local scratch child inside `shots/`.
            let orphan_payload = b"orphaned scratch child payload restored cleanly\0\n";
            write_synced_mounted_file(
                &a.join("shots").join("render.tmp"),
                orphan_payload,
                "write shots/render.tmp",
            )?;

            // Fence to confirm replication caught up and peer sees `shots` as empty.
            write_mounted_file_with_readback(
                a.join("pre-rmdir.marker"),
                b"ok\n",
                "pre-rmdir marker",
                true,
            )?;
            expect_bytes(&b.join("pre-rmdir.marker"), b"ok\n")?;
            wait_for_mounted_paths_absent_until(
                std::slice::from_ref(&observer),
                &["shots/render.tmp"],
                Instant::now() + WAIT,
                POLL,
                "scratch child hidden from peer before rmdir",
            )?;

            // Peer removes `shots/`.
            let shots_b = b.join("shots");
            run_bounded_mounted_io("peer rmdir shots", WAIT, move || fs::remove_dir(&shots_b))?;

            // Wait for client-a to observe `shots/` removed and list the materialized orphan.
            wait_for_mounted_paths_absent_until(
                std::slice::from_ref(&client_a),
                &["shots"],
                Instant::now() + WAIT,
                POLL,
                "client-a observes peer rmdir of shots",
            )?;

            let orphan_id = wait_for_materialized_orphan(
                controller,
                "client-a",
                &client_a.volume_name,
                "shots/render.tmp",
            )?;

            // Restore the orphaned scratch file cleanly via `studiofs scratch restore`.
            let restore_output = controller.run_cli(
                "client-a",
                "scratch-restore",
                vec![
                    "scratch".into(),
                    "restore".into(),
                    "--volume".into(),
                    client_a.volume_name.clone(),
                    orphan_id.clone(),
                ],
            )?;

            // Restored scratch file is readable on client-a with exact original bytes.
            expect_bytes(&a.join("shots").join("render.tmp"), orphan_payload)?;

            // Orphan list is now empty.
            let after_list = controller.run_cli(
                "client-a",
                "scratch-list-orphaned-after-restore",
                vec![
                    "scratch".into(),
                    "list".into(),
                    "--volume".into(),
                    client_a.volume_name.clone(),
                    "--orphaned".into(),
                ],
            )?;
            if !after_list.stdout.contains("No orphaned scratch entries.") {
                return Err(io::Error::other(format!(
                    "expected empty orphan list after restore, got:\n{}",
                    after_list.stdout
                ))
                .into());
            }

            write_summary(
                context,
                SUMMARY,
                &json!({
                    "orphan_id": orphan_id,
                    "restore_stdout": restore_output.stdout.trim(),
                    "restored_payload_verified": true,
                    "orphan_list_empty_after_restore": true,
                    "rule_probe_attempts": probe_attempts,
                }),
            )
        })
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary = read_summary(context, "scratch-orphan-restore.json")?;
        if summary["restored_payload_verified"] != true
            || summary["orphan_list_empty_after_restore"] != true
            || summary["orphan_id"].as_str().is_none_or(str::is_empty)
        {
            return Err(io::Error::other("scratch orphan-restore acceptance incomplete").into());
        }
        Ok(vec![format!(
            "artifacts/scenarios/{SCRATCH_ORPHAN_RESTORE}/scratch-orphan-restore.json"
        )])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

impl Scenario for ScratchAppleDouble {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: SCRATCH_APPLEDOUBLE,
            description: "built-in ._* AppleDouble sidecars keep real data on the creating client without publishing",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        const SUMMARY: &str = "scratch-appledouble.json";
        run_with_controller(context, SCRATCH_APPLEDOUBLE, |config, controller| {
            controller.bootstrap_volume(VolumeBootstrapOptions {
                client_names: vec!["client-a".into(), "client-b".into()],
                clean_existing_local_metadata: true,
            })?;
            controller.mount_all()?;
            let a = controller.clients()[0].mount_point.clone();
            let observer = controller.clients()[1].clone();
            let b = observer.mount_point.clone();

            // No `.studiofsignore` file is written: `._*` is a built-in scratch rule.
            let _ = wait_until_sync_safe(controller, "client-a")?;
            let _ = wait_until_sync_safe(controller, "client-b")?;
            let events_before = query_event_log_count(config)?;
            let s3_before = count_s3_objects(config)?;

            // AppleDouble magic header (0x00051607) + version (0x00020000) + sidecar payload.
            let mut sidecar_payload = vec![0x00, 0x05, 0x16, 0x07, 0x00, 0x02, 0x00, 0x00];
            sidecar_payload.extend_from_slice(b"studiofs appledouble sidecar payload\0\n");
            write_synced_mounted_file(
                &a.join("._shot.exr"),
                &sidecar_payload,
                "write ._shot.exr sidecar",
            )?;
            expect_bytes(&a.join("._shot.exr"), &sidecar_payload)?;

            let _ = wait_until_sync_safe(controller, "client-a")?;
            let events_after = query_event_log_count(config)?;
            let s3_after = count_s3_objects(config)?;
            if events_after != events_before || s3_after != s3_before {
                return Err(io::Error::other(format!(
                    "._* sidecar write leaked to remote: events {events_before}->{events_after}, s3 {s3_before}->{s3_after}"
                ))
                .into());
            }

            // Tracked companion file publishes normally; sidecar stays invisible to peer.
            let shot_payload = b"tracked shot payload\n";
            write_mounted_file_with_readback(
                a.join("shot.exr"),
                shot_payload,
                "write tracked shot.exr",
                true,
            )?;
            expect_bytes(&b.join("shot.exr"), shot_payload)?;
            wait_for_mounted_paths_absent_until(
                std::slice::from_ref(&observer),
                &["._shot.exr"],
                Instant::now() + WAIT,
                POLL,
                "._* sidecar stays hidden from peer",
            )?;

            // Sidecar bytes survive unmount and remount on the creating client.
            controller.unmount("client-a")?;
            controller.mount("client-a")?;
            expect_bytes(&a.join("._shot.exr"), &sidecar_payload)?;

            write_summary(
                context,
                SUMMARY,
                &json!({
                    "event_log_delta": events_after - events_before,
                    "s3_object_delta": s3_after - s3_before,
                    "sidecar_persisted_across_remount": true,
                    "peer_absent": true,
                }),
            )
        })
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary = read_summary(context, "scratch-appledouble.json")?;
        if summary["event_log_delta"] != 0
            || summary["s3_object_delta"] != 0
            || summary["sidecar_persisted_across_remount"] != true
            || summary["peer_absent"] != true
        {
            return Err(io::Error::other("scratch appledouble acceptance incomplete").into());
        }
        Ok(vec![format!(
            "artifacts/scenarios/{SCRATCH_APPLEDOUBLE}/scratch-appledouble.json"
        )])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn run_with_controller<F>(
    context: &ScenarioContext,
    scenario_name: &str,
    body: F,
) -> Result<(), ScenarioError>
where
    F: FnOnce(
        &ClientControllerConfig,
        &mut MultiClientController<SystemClientProcessExecutor>,
    ) -> Result<(), ScenarioError>,
{
    let config = context
        .client_controller_config
        .clone()
        .ok_or(ScenarioError::MissingComposeInfrastructure)?;
    let mut controller = MultiClientController::new(config.clone(), SystemClientProcessExecutor);
    let result = body(&config, &mut controller);
    let cleanup = if matches!(
        result,
        Err(ScenarioError::MountedIoTimeout { .. } | ScenarioError::MountedIoWorkerLimit { .. })
    ) {
        controller.emergency_kill_all_no_wait(&format!("{scenario_name} mounted I/O timeout"))
    } else {
        controller.unmount_all()
    }
    .map_err(ScenarioError::from);
    match (result, cleanup) {
        (result, Ok(())) | (Ok(()), result) => result,
        (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
            context: scenario_name.into(),
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        }),
    }
}

fn query_event_log_count(config: &ClientControllerConfig) -> Result<i64, ScenarioError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let (client, connection) = tokio::time::timeout(
            DB_TIMEOUT,
            tokio_postgres::connect(&config.meta_dsn, tokio_postgres::NoTls),
        )
        .await
        .map_err(|_| io::Error::other("event_log count connection timed out"))?
        .map_err(|error| io::Error::other(format!("event_log count connect failed: {error}")))?;
        let connection = tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = tokio::time::timeout(
            DB_TIMEOUT,
            client.query_one(
                "SELECT COUNT(*) FROM event_log ev JOIN volumes v ON v.vol = ev.vol WHERE v.name = $1",
                &[&config.volume_name],
            ),
        )
        .await;
        drop(client);
        connection.abort();
        let _ = connection.await;
        row.map_err(|_| io::Error::other("event_log count query timed out"))?
            .map(|r| r.get(0))
            .map_err(|error| io::Error::other(format!("event_log count query failed: {error}")).into())
    })
}

fn count_s3_objects(config: &ClientControllerConfig) -> Result<usize, ScenarioError> {
    let store = JanitorObjectStore::from_s3(
        &config.s3.endpoint,
        &config.s3.bucket,
        &config.s3.access_key,
        &config.s3.secret_key,
        &config.s3.region,
        8,
    )
    .map_err(|error| io::Error::other(format!("init S3 store failed: {error}")))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut lister = tokio::time::timeout(DB_TIMEOUT, store.list(""))
            .await
            .map_err(|_| io::Error::other("S3 list timed out"))?
            .map_err(|error| io::Error::other(format!("S3 list failed: {error}")))?;
        let mut count = 0usize;
        while let Some(_entry) = tokio::time::timeout(DB_TIMEOUT, lister.next())
            .await
            .map_err(|_| io::Error::other("S3 list next timed out"))?
            .map_err(|error| io::Error::other(format!("S3 list next failed: {error}")))?
        {
            count += 1;
        }
        Ok(count)
    })
}

fn upload_counter(snapshot: &serde_json::Value, field: &str) -> u64 {
    snapshot["upload"][field].as_u64().unwrap_or(0)
}

fn wait_until_sync_safe(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    client_name: &str,
) -> Result<serde_json::Value, ScenarioError> {
    let deadline = Instant::now() + WAIT;
    loop {
        let snapshot = controller.runtime_snapshot(client_name)?;
        let safe = snapshot["sync_safety"]["state"].as_str() == Some("safe");
        let pending = snapshot["upload"]["pending_uploads"].as_u64().unwrap_or(1);
        let active = snapshot["upload"]["active_uploads"].as_u64().unwrap_or(1);
        if safe && pending == 0 && active == 0 {
            return Ok(snapshot);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "{client_name} did not reach sync_safety=safe with idle uploads: {}",
                snapshot["sync_safety"]
            ))
            .into());
        }
        thread::sleep(POLL);
    }
}

/// Creates `probe-<label>-<n>.tmp` and a tracked marker on `writer`; rules are active once
/// `observer` sees the marker but not the probe.
fn wait_for_rules_active(
    writer: &Path,
    observer: &Path,
    label: &str,
) -> Result<u32, ScenarioError> {
    let deadline = Instant::now() + WAIT;
    for attempt in 1.. {
        let probe = format!("probe-{label}-{attempt}.tmp");
        let marker = format!("probe-{label}-{attempt}.marker");
        write_mounted_file_with_readback(writer.join(&probe), b"p\n", "rule probe", true)?;
        write_mounted_file_with_readback(writer.join(&marker), b"m\n", "rule probe marker", true)?;
        expect_bytes(&observer.join(&marker), b"m\n")?;
        if !observer.join(&probe).exists() {
            return Ok(attempt);
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    Err(io::Error::other(format!("{label}: ignore rules never became active")).into())
}

fn wait_for_rules_active_single_client(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    client_name: &str,
    mount: &Path,
    volume_name: &str,
) -> Result<u32, ScenarioError> {
    let deadline = Instant::now() + WAIT;
    for attempt in 1.. {
        let probe = format!("probe-single-{attempt}.tmp");
        write_mounted_file_with_readback(mount.join(&probe), b"p\n", "single rule probe", true)?;
        let output = controller.run_cli(
            client_name,
            "scratch-list-rule-probe",
            vec![
                "scratch".into(),
                "list".into(),
                "--volume".into(),
                volume_name.to_string(),
            ],
        )?;
        if output.stdout.contains(&probe) {
            return Ok(attempt);
        }
        if Instant::now() >= deadline {
            break;
        }
        thread::sleep(POLL);
    }
    Err(io::Error::other(format!("{client_name}: ignore rules never became active")).into())
}

fn write_synced_mounted_file(
    path: &Path,
    payload: &[u8],
    label: &str,
) -> Result<(), ScenarioError> {
    let path_owned = path.to_path_buf();
    let bytes = payload.to_vec();
    run_bounded_mounted_io(label, WAIT, move || {
        let mut file = fs::File::create(&path_owned)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    })?;
    expect_bytes(path, payload)
}

fn expect_bytes(path: &Path, expected: &[u8]) -> Result<(), ScenarioError> {
    let readback = wait_for_mounted_payload_readback(path, expected, WAIT, POLL)
        .map_err(|error| io::Error::other(format!("{}: {error}", path.display())))?;
    if readback != expected {
        return Err(io::Error::other(format!("payload mismatch at {}", path.display())).into());
    }
    Ok(())
}

fn list_cache_object_files(client: &ClientSpec) -> Result<BTreeSet<PathBuf>, ScenarioError> {
    let mut files = BTreeSet::new();
    if !client.cache_dir.exists() {
        return Ok(files);
    }
    for entry in fs::read_dir(&client.cache_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            let objects_dir = entry.path().join("payload").join("objects");
            if objects_dir.exists() {
                collect_files_recursive(&objects_dir, &mut files)?;
            }
        }
    }
    Ok(files)
}

fn collect_files_recursive(dir: &Path, out: &mut BTreeSet<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_files_recursive(&path, out)?;
        } else {
            out.insert(path);
        }
    }
    Ok(())
}

fn wait_for_cache_object_growth(
    client: &ClientSpec,
    baseline: &BTreeSet<PathBuf>,
    label: &str,
) -> Result<BTreeSet<PathBuf>, ScenarioError> {
    let deadline = Instant::now() + WAIT;
    loop {
        let current = list_cache_object_files(client)?;
        if current.len() > baseline.len() {
            return Ok(current);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for {label} (baseline={})",
                baseline.len()
            ))
            .into());
        }
        thread::sleep(POLL);
    }
}

fn unix_ino(path: &Path) -> Result<Option<u64>, ScenarioError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let path_owned = path.to_path_buf();
        let ino = run_bounded_mounted_io("stat inode number", WAIT, move || {
            Ok(fs::symlink_metadata(&path_owned)?.ino())
        })?;
        Ok(Some(ino))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(None)
    }
}

fn wait_for_conflict_entry(root: &Path, prefix: &str) -> Result<String, ScenarioError> {
    let deadline = Instant::now() + WAIT;
    loop {
        let dir = root.to_path_buf();
        let prefix_owned = prefix.to_string();
        let found = run_bounded_mounted_io("scan for scratch conflict entry", WAIT, move || {
            for entry in fs::read_dir(&dir)? {
                let name = entry?.file_name().to_string_lossy().into_owned();
                if name.starts_with(&prefix_owned) {
                    return Ok(Some(name));
                }
            }
            Ok(None)
        })?;
        if let Some(name) = found {
            return Ok(name);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for conflict entry starting with `{prefix}` under {}",
                root.display()
            ))
            .into());
        }
        thread::sleep(POLL);
    }
}

fn wait_for_dir_present(path: &Path) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + WAIT;
    loop {
        let probe = path.to_path_buf();
        let present = run_bounded_mounted_io("check peer dir present", WAIT, move || {
            match fs::symlink_metadata(&probe) {
                Ok(meta) => Ok(meta.is_dir()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
                Err(error) => Err(error),
            }
        })?;
        if present {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for directory {}",
                path.display()
            ))
            .into());
        }
        thread::sleep(POLL);
    }
}

fn wait_for_materialized_orphan(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    client_name: &str,
    volume_name: &str,
    expected_suffix: &str,
) -> Result<String, ScenarioError> {
    let deadline = Instant::now() + WAIT;
    loop {
        let output = controller.run_cli(
            client_name,
            "scratch-list-orphaned",
            vec![
                "scratch".into(),
                "list".into(),
                "--volume".into(),
                volume_name.to_string(),
                "--orphaned".into(),
            ],
        )?;
        let last_stdout = output.stdout;
        for line in last_stdout.lines() {
            if line.contains(expected_suffix)
                && line.contains("materialized")
                && let Some(id) = line.split_whitespace().next()
            {
                return Ok(id.to_string());
            }
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for materialized orphan `{expected_suffix}`; last output:\n{last_stdout}"
            ))
            .into());
        }
        thread::sleep(POLL);
    }
}

fn write_summary(
    context: &ScenarioContext,
    filename: &str,
    value: &serde_json::Value,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join(filename),
        serde_json::to_vec_pretty(value)?,
    )?;
    Ok(())
}

fn read_summary(
    context: &ScenarioContext,
    filename: &str,
) -> Result<serde_json::Value, ScenarioError> {
    Ok(serde_json::from_slice(&fs::read(
        context.scenario_dir.join(filename),
    )?)?)
}
