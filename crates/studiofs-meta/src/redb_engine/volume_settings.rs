use super::*;
use crate::trash::{MetaVolumeSettings, VolumeSettings};

pub(super) const VOLUME_SETTINGS_KEY: &str = "volume_settings";
pub(super) const BASE_VOLUME_SETTINGS_KEY: &str = "base_volume_settings";

impl RedbMetaEngine {
    pub(super) fn apply_volume_settings_in_txn(
        txn: &RedbWriteTransaction<'_>,
        key: &str,
        settings: &VolumeSettings,
    ) -> MetaResult<()> {
        let mut table = txn
            .open_table(SETTINGS_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        if let Some(format) = table.get("format")? {
            crate::decode_format_info(format.value())?;
        }
        let current = table
            .get(key)?
            .map(|entry| {
                serde_json::from_slice::<VolumeSettings>(entry.value())
                    .map_err(|error| MetaError::Serialization(error.to_string()))
            })
            .transpose()?
            .unwrap_or_default();
        if !current.is_superseded_by(settings)? {
            return Ok(());
        }
        if current.logical_quota_bytes.is_none() && settings.logical_quota_bytes.is_some() {
            txn.invalidate(InvalidationIntent::Reset);
        }
        let bytes = serde_json::to_vec(settings)
            .map_err(|error| MetaError::Serialization(error.to_string()))?;
        table.insert(key, bytes.as_slice())?;
        Ok(())
    }
}

impl MetaVolumeSettings for RedbMetaEngine {
    fn volume_settings(&self) -> MetaResult<VolumeSettings> {
        let txn = self.begin_read_txn()?;
        let table = txn
            .open_table(SETTINGS_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        table
            .get(VOLUME_SETTINGS_KEY)?
            .or(table.get(BASE_VOLUME_SETTINGS_KEY)?)
            .map(|entry| {
                serde_json::from_slice(entry.value())
                    .map_err(|error| MetaError::Serialization(error.to_string()))
            })
            .transpose()
            .map(Option::unwrap_or_default)
    }

    fn compare_and_set_trash_days(
        &self,
        expected_revision: u64,
        days: u32,
    ) -> MetaResult<Option<VolumeSettings>> {
        self.update_settings(expected_revision, |settings| settings.trash.set_days(days))
    }

    fn compare_and_set_logical_quota_bytes(
        &self,
        expected_revision: u64,
        bytes: Option<u64>,
    ) -> MetaResult<Option<VolumeSettings>> {
        self.update_settings(expected_revision, |settings| {
            settings.logical_quota_bytes = bytes
        })
    }
}

impl RedbMetaEngine {
    fn update_settings(
        &self,
        expected_revision: u64,
        update: impl FnOnce(&mut VolumeSettings),
    ) -> MetaResult<Option<VolumeSettings>> {
        let txn = self.begin_write_txn()?;
        let mut settings = {
            let table = txn
                .open_table(SETTINGS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            if let Some(format) = table.get("format")? {
                crate::decode_format_info(format.value())?;
            }
            table
                .get(VOLUME_SETTINGS_KEY)?
                .or(table.get(BASE_VOLUME_SETTINGS_KEY)?)
                .map(|entry| {
                    serde_json::from_slice::<VolumeSettings>(entry.value())
                        .map_err(|e| MetaError::Serialization(e.to_string()))
                })
                .transpose()?
                .unwrap_or_default()
        };
        if settings.revision != expected_revision {
            return Ok(None);
        }
        settings.revision = settings
            .revision
            .checked_add(1)
            .ok_or_else(|| MetaError::Storage("volume settings revision overflow".into()))?;
        let previous_limit = settings.logical_quota_bytes;
        update(&mut settings);
        if previous_limit.is_none() && settings.logical_quota_bytes.is_some() {
            txn.invalidate(InvalidationIntent::Reset);
        }
        let encoded =
            serde_json::to_vec(&settings).map_err(|e| MetaError::Serialization(e.to_string()))?;
        txn.open_table(SETTINGS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .insert(VOLUME_SETTINGS_KEY, encoded.as_slice())?;
        Self::append_event_in_txn(&txn, &crate::MetaEvent::SetVolumeSettings { settings })?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(Some(settings))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_header_blocks_settings_event_and_checkpoint3_before_upgrade() {
        let temp = studiofs_test_support::workspace_target_tempdir("trash-legacy-fence-");
        let engine = RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
        let mut format = FormatInfo::new(
            "legacy".into(),
            "legacy-test".into(),
            FormatInfoDataStore {
                endpoint: "http://127.0.0.1:9".into(),
                bucket: "test".into(),
                access_key: "test".into(),
                secret_key: "test".into(),
                region: "test".into(),
            },
            4,
        )
        .unwrap();
        format.format_version = 5;
        engine.set_format_info(&format).unwrap();
        assert!(matches!(
            engine.compare_and_set_trash_days(0, 7),
            Err(MetaError::UnsupportedFormatVersion {
                found: 5,
                expected: 6
            })
        ));
        assert!(engine.read_events_range(0, u64::MAX).unwrap().is_empty());
        assert!(matches!(
            engine.export_serving_checkpoint_bytes(),
            Err(MetaError::UnsupportedFormatVersion {
                found: 5,
                expected: 6
            })
        ));
        assert_eq!(engine.volume_settings().unwrap(), VolumeSettings::default());
        engine.init_root(0, 0).unwrap();
        let attr = engine
            .create_file(ROOT_INO, b"offline-accepted", 0, 0, 0o600)
            .unwrap();
        let journal = engine.read_events_range(0, u64::MAX).unwrap();
        let mut upgraded = format.clone();
        upgraded.format_version = 6;
        engine.upgrade_local_format5(&upgraded).unwrap();
        engine.upgrade_local_format5(&upgraded).unwrap();
        assert_eq!(engine.get_attr(attr.ino).unwrap(), attr);
        assert_eq!(engine.read_events_range(0, u64::MAX).unwrap(), journal);
        assert_eq!(engine.get_format_info().unwrap().format_version, 6);
        assert!(engine.export_serving_checkpoint_bytes().is_ok());
    }

    #[test]
    fn journal_failure_rolls_back_settings_cas() {
        let temp = studiofs_test_support::workspace_target_tempdir("trash-settings-rollback-");
        let engine = RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
        engine
            .set_counter(crate::schema::counters::NEXT_EVENT_SEQ, u64::MAX)
            .unwrap();
        assert!(engine.compare_and_set_trash_days(0, 7).is_err());
        assert_eq!(engine.volume_settings().unwrap(), VolumeSettings::default());
    }

    #[test]
    fn settings_journal_replay_and_remote_checkpoint_roundtrip() {
        let temp = studiofs_test_support::workspace_target_tempdir("trash-settings-replay-");
        let source = RedbMetaEngine::open(temp.path().join("source.redb")).unwrap();
        let expected = source.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let events = source.read_events_range(0, u64::MAX).unwrap();
        assert_eq!(
            events,
            vec![crate::MetaEvent::SetVolumeSettings { settings: expected }]
        );
        let replica = RedbMetaEngine::open(temp.path().join("replica.redb")).unwrap();
        replica.apply_replica_events_to_base(&events).unwrap();
        replica.apply_replica_aux_state(&events).unwrap();
        replica.apply_replica_serving_state(&events).unwrap();
        replica
            .apply_replica_serving_state_for_pending_replay(&events)
            .unwrap();
        assert_eq!(replica.volume_settings().unwrap(), expected);
        let snapshot = replica.export_remote_snapshot().unwrap();
        let bootstrapped = RedbMetaEngine::open(temp.path().join("bootstrap.redb")).unwrap();
        bootstrapped.import_remote_snapshot(&snapshot).unwrap();
        assert_eq!(bootstrapped.volume_settings().unwrap(), expected);
    }

    #[test]
    fn logical_quota_settings_cas_roundtrips_and_rejects_stale_revision() {
        let temp = studiofs_test_support::workspace_target_tempdir("logical-quota-settings-");
        let engine = RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
        let accepted = engine
            .compare_and_set_logical_quota_bytes(0, Some(4096))
            .unwrap()
            .unwrap();
        assert_eq!(accepted.logical_quota_bytes, Some(4096));
        assert_eq!(accepted.revision, 1);
        assert_eq!(
            engine.compare_and_set_logical_quota_bytes(0, None).unwrap(),
            None
        );
        let replay = engine.read_events_range(0, u64::MAX).unwrap();
        assert_eq!(
            replay,
            vec![crate::MetaEvent::SetVolumeSettings { settings: accepted }]
        );
        drop(engine);
        let reopened = RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
        assert_eq!(reopened.volume_settings().unwrap(), accepted);
    }

    #[test]
    fn replay_rejects_equal_revision_conflict_and_ignores_stale_update() {
        let temp = studiofs_test_support::workspace_target_tempdir("trash-settings-conflict-");
        let engine = RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
        let accepted = engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let mut conflict = accepted;
        conflict.trash.set_days(30);
        assert!(
            engine
                .apply_replica_serving_state(&[crate::MetaEvent::SetVolumeSettings {
                    settings: conflict
                }])
                .is_err()
        );
        engine
            .apply_replica_serving_state(&[crate::MetaEvent::SetVolumeSettings {
                settings: VolumeSettings::default(),
            }])
            .unwrap();
        assert_eq!(engine.volume_settings().unwrap(), accepted);
    }

    #[test]
    fn concurrent_storage_cas_has_one_winner() {
        let temp = studiofs_test_support::workspace_target_tempdir("trash-settings-race-");
        let engine = Arc::new(RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap());
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let threads: Vec<_> = [7, 30]
            .into_iter()
            .map(|days| {
                let engine = engine.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    engine.compare_and_set_trash_days(0, days).unwrap()
                })
            })
            .collect();
        barrier.wait();
        let results: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_some()).count(), 1);
        assert_eq!(
            engine.volume_settings().unwrap(),
            results.into_iter().flatten().next().unwrap()
        );
    }

    #[test]
    fn corrupted_policy_fails_storage_read_and_cas_without_replacement() {
        let temp = studiofs_test_support::workspace_target_tempdir("trash-settings-invalid-");
        let engine = RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
        let bytes = br#"{"revision":1,"trash":{"days":7,"last_positive_days":1}}"#;
        let txn = engine.begin_write_txn().unwrap();
        txn.open_table(SETTINGS_TABLE)
            .unwrap()
            .insert(VOLUME_SETTINGS_KEY, bytes.as_slice())
            .unwrap();
        txn.commit().unwrap();
        assert!(matches!(
            engine.volume_settings(),
            Err(MetaError::Serialization(_))
        ));
        assert!(matches!(
            engine.compare_and_set_trash_days(1, 30),
            Err(MetaError::Serialization(_))
        ));
        let txn = engine.begin_read_txn().unwrap();
        assert_eq!(
            txn.open_table(SETTINGS_TABLE)
                .unwrap()
                .get(VOLUME_SETTINGS_KEY)
                .unwrap()
                .unwrap()
                .value(),
            bytes
        );
    }

    #[test]
    fn storage_cas_rejects_stale_revision_and_survives_reopen() {
        let temp = studiofs_test_support::workspace_target_tempdir("trash-settings-");
        let path = temp.path().join("meta.redb");
        let engine = RedbMetaEngine::open(&path).unwrap();
        assert_eq!(engine.volume_settings().unwrap(), VolumeSettings::default());
        let first = engine.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        assert_eq!(first.revision, 1);
        assert!(engine.compare_and_set_trash_days(0, 1).unwrap().is_none());
        assert_eq!(engine.volume_settings().unwrap(), first);
        let disabled = engine.compare_and_set_trash_days(1, 0).unwrap().unwrap();
        drop(engine);
        let reopened = RedbMetaEngine::open(&path).unwrap();
        assert_eq!(reopened.volume_settings().unwrap(), disabled);
        assert_eq!(disabled.trash.existing_entry_days(), 7);
        assert!(!disabled.trash.enabled());
    }
}
