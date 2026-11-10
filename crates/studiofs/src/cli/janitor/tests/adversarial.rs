//! Deterministic adversarial boundaries for the standalone janitor.

use super::*;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;

struct WriterInterleavingStore {
    inner: FakeJanitorStore,
    writer_active: Arc<AtomicBool>,
    payload_keys: Mutex<HashSet<String>>,
    delete_attempts: AtomicU64,
}

impl JanitorObjectStore for WriterInterleavingStore {
    type Listing = FakeListing;

    async fn list(&self, prefix: &str, after_key: Option<&str>) -> anyhow::Result<Self::Listing> {
        self.inner.list(prefix, after_key).await
    }

    async fn last_modified(&self, _key: &str) -> anyhow::Result<Option<SystemTime>> {
        // The writer starts after listing, before the metadata deletion claim.
        self.writer_active.store(true, Ordering::SeqCst);
        Ok(Some(SystemTime::now() - Duration::from_secs(3600)))
    }

    async fn delete(&self, key: &str) -> Result<(), JanitorDeleteError> {
        self.delete_attempts.fetch_add(1, Ordering::SeqCst);
        self.payload_keys.lock().unwrap().remove(key);
        Ok(())
    }
}

struct WriterAwareMetadata {
    inner: FakeJanitorMetadata,
    writer_active: Arc<AtomicBool>,
}

impl JanitorMetadataSource for WriterAwareMetadata {
    fn try_acquire_global_janitor_lock(&mut self) -> anyhow::Result<bool> {
        self.inner.try_acquire_global_janitor_lock()
    }

    fn validate_protection(&mut self) -> anyhow::Result<()> {
        self.inner.validate_protection()
    }

    fn check_eligibility(
        &mut self,
        object_ids: &[u64],
        eligible_before: i64,
    ) -> anyhow::Result<JanitorProtection> {
        self.inner.check_eligibility(object_ids, eligible_before)
    }

    fn gc_candidate_page(
        &mut self,
        after: Option<(i64, u64)>,
        limit: usize,
        eligible_before: i64,
        backfill: bool,
    ) -> anyhow::Result<Vec<(u64, i64)>> {
        self.inner
            .gc_candidate_page(after, limit, eligible_before, backfill)
    }

    fn claim_deletions(
        &mut self,
        object_ids: &[u64],
    ) -> anyhow::Result<std::collections::HashMap<u64, i64>> {
        if self.writer_active.load(Ordering::SeqCst) {
            self.inner.claim_protected.extend_from_slice(object_ids);
        }
        self.inner.claim_deletions(object_ids)
    }

    fn settle_deletions(
        &mut self,
        settlements: &[JanitorDeletionSettlement],
    ) -> anyhow::Result<std::collections::HashSet<u64>> {
        self.inner.settle_deletions(settlements)
    }

    fn prune_deletion_fences(&mut self) -> anyhow::Result<u64> {
        self.inner.prune_deletion_fences()
    }

    fn load_sweep_checkpoint(
        &mut self,
        scope: JanitorSweepScope<'_>,
        persist_initial: bool,
    ) -> anyhow::Result<JanitorSweepCheckpoint> {
        self.inner.load_sweep_checkpoint(scope, persist_initial)
    }

    fn save_sweep_checkpoint(
        &mut self,
        scope: JanitorSweepScope<'_>,
        checkpoint: &JanitorSweepCheckpoint,
    ) -> anyhow::Result<()> {
        self.inner.save_sweep_checkpoint(scope, checkpoint)
    }

    fn release_global_janitor_lock(&mut self) -> anyhow::Result<()> {
        self.inner.release_global_janitor_lock()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sfs978_writer_started_after_listing_is_protected_at_deletion_claim() {
    let mut keys = canonical_keys(978);
    keys.sort();
    let expected_payload: HashSet<_> = keys.iter().cloned().collect();
    let writer_active = Arc::new(AtomicBool::new(false));
    let store = WriterInterleavingStore {
        inner: FakeJanitorStore {
            entries: keys
                .iter()
                .cloned()
                .map(|key| JanitorEntry {
                    key,
                    is_file: true,
                    last_modified: None,
                })
                .collect(),
            ..FakeJanitorStore::default()
        },
        writer_active: Arc::clone(&writer_active),
        payload_keys: Mutex::new(keys.into_iter().collect()),
        delete_attempts: AtomicU64::new(0),
    };
    let mut metadata = WriterAwareMetadata {
        inner: FakeJanitorMetadata::eligible(),
        writer_active,
    };

    let stats = run_one_cycle(JanitorCycleContext {
        store: &store,
        prefix: "test/objects/",
        dry_run: false,
        skip_newer_than_secs: 0,
        budget: RunBudget::deletes(1),
        metadata: &mut metadata,
    })
    .await
    .unwrap();

    assert_eq!(stats.claims, 1);
    assert_eq!(stats.claim_protected, 1);
    assert_eq!(stats.deleted, 0);
    assert!(metadata.inner.finishes.is_empty());
    assert_eq!(store.delete_attempts.load(Ordering::SeqCst), 0);
    assert_eq!(*store.payload_keys.lock().unwrap(), expected_payload);
}

struct ApplyThenTimeoutStore {
    inner: FakeJanitorStore,
    payload_keys: Mutex<HashSet<String>>,
    lose_first_delete_response: AtomicBool,
}

impl JanitorObjectStore for ApplyThenTimeoutStore {
    type Listing = FakeListing;

    async fn list(&self, prefix: &str, after_key: Option<&str>) -> anyhow::Result<Self::Listing> {
        self.inner.list(prefix, after_key).await
    }

    async fn last_modified(&self, key: &str) -> anyhow::Result<Option<SystemTime>> {
        self.inner.last_modified(key).await
    }

    async fn delete(&self, key: &str) -> Result<(), JanitorDeleteError> {
        let existed = self.payload_keys.lock().unwrap().remove(key);
        if self
            .lose_first_delete_response
            .swap(false, Ordering::SeqCst)
        {
            return Err(JanitorDeleteError::Other(
                "response timed out after DELETE applied".to_owned(),
            ));
        }
        if existed {
            Ok(())
        } else {
            Err(JanitorDeleteError::NotFound)
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sfs978_applied_delete_with_lost_response_replays_idempotently_after_restart() {
    let mut keys = canonical_keys(979);
    keys.sort();
    let store = ApplyThenTimeoutStore {
        inner: FakeJanitorStore {
            entries: keys
                .iter()
                .cloned()
                .map(|key| JanitorEntry {
                    key,
                    is_file: true,
                    last_modified: Some(SystemTime::now() - Duration::from_secs(3600)),
                })
                .collect(),
            ..FakeJanitorStore::default()
        },
        payload_keys: Mutex::new(keys.into_iter().collect()),
        lose_first_delete_response: AtomicBool::new(true),
    };
    let mut metadata = FakeJanitorMetadata::eligible();

    let first = run_one_cycle(JanitorCycleContext {
        store: &store,
        prefix: "test/objects/",
        dry_run: false,
        skip_newer_than_secs: 0,
        budget: RunBudget::deletes(1),
        metadata: &mut metadata,
    })
    .await
    .unwrap();
    assert_eq!(first.deleted, 0);
    assert_eq!(first.delete_errors, 1);
    assert_eq!(metadata.gc_candidate_retries, [979]);
    assert_eq!(
        metadata.finishes[0].2,
        UploadedObjectDeletionOutcome::Retryable
    );
    assert_eq!(
        store.payload_keys.lock().unwrap().len(),
        studiofs_data::object::CLOUD_FRAGMENT_COUNT
    );

    // A fresh metadata handle models restart after the retry becomes eligible again.
    let mut restarted_metadata = FakeJanitorMetadata::eligible();
    let restarted = run_one_cycle(JanitorCycleContext {
        store: &store,
        prefix: "test/objects/",
        dry_run: false,
        skip_newer_than_secs: 0,
        budget: RunBudget::deletes(1),
        metadata: &mut restarted_metadata,
    })
    .await
    .unwrap();

    assert_eq!(restarted.deleted, 1);
    assert_eq!(restarted_metadata.claims, [979]);
    assert_eq!(
        restarted_metadata
            .finishes
            .iter()
            .map(|finish| finish.2)
            .collect::<Vec<_>>(),
        [UploadedObjectDeletionOutcome::Deleted]
    );
    assert!(store.payload_keys.lock().unwrap().is_empty());
}
