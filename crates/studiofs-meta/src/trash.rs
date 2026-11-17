//! Platform-neutral retention policy. Native trash views must use this policy
//! rather than deriving expiry from OS-specific sidecar files.

use serde::{Deserialize, Serialize};

pub(crate) const RETAINED_RECEIPT_XATTR: &[u8] = b"system.studiofs.retained.v1";

/// Neutral receipt stored on a private per-entry directory, never an OS sidecar.
/// The directory's `data` link owns the retained namespace reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetainedEntry {
    /// Receipt schema, independent of native metadata formats.
    pub version: u8,
    /// Local transition revision; initial retention is zero and terminal requests are one.
    pub revision: u64,
    /// Unique directory inode identifying this namespace entry (including hard links).
    pub receipt: crate::Ino,
    /// Payload inode, which may also have live hard links.
    pub inode: crate::Ino,
    /// Original parent identity captured by the source rename transaction.
    pub original_parent: crate::Ino,
    /// Original single component, without any client's mount path.
    pub original_name: Vec<u8>,
    /// Trusted local deletion time, independent of native sidecar contents.
    pub deleted_at_unix_secs: u64,
    /// Local lifecycle state; shared publication still uses existing event ordering.
    pub state: RetainedEntryState,
}

/// Durable local outcome; terminal receipts make operation retries explicit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetainedEntryState {
    /// The private receipt directory still owns its `data` link.
    Retained,
    /// Payload moved to this destination without replacing an existing entry.
    Restored {
        /// Destination directory identity.
        parent: crate::Ino,
        /// Destination component chosen by the user.
        name: Vec<u8>,
    },
    /// A local request only. The data reference remains until shared settlement.
    PurgeRequested {
        /// Explicit Clear/purge or expiry under the current shared policy.
        reason: RetainedPurgeReason,
    },
}

/// Why an application requested removal of a retained namespace entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetainedPurgeReason {
    /// Explicit native purge or Clear.
    User,
    /// Retention elapsed; never capacity pressure. The proposer judged it under
    /// shared settings `settings_revision`. Commit accepts it only while that is
    /// still the current revision. Revisions never repeat, so a proposal rejected
    /// once stays stale even after the period returns to its old value (ABA).
    Expired {
        /// Shared volume-settings revision the expiry was judged under.
        settings_revision: u64,
    },
}

/// Shared whole-day retention policy, stored separately from immutable format info.
///
/// This value is not a machine-local mount option. Its enclosing metadata record
/// must supply the replication revision and compare-and-set authority.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "TrashPolicyFields")]
pub struct TrashPolicy {
    days: u32,
    last_positive_days: u32,
}

#[derive(Deserialize)]
struct TrashPolicyFields {
    days: u32,
    last_positive_days: u32,
}

impl TryFrom<TrashPolicyFields> for TrashPolicy {
    type Error = &'static str;

    fn try_from(fields: TrashPolicyFields) -> Result<Self, Self::Error> {
        if fields.days > 0 && fields.days != fields.last_positive_days {
            return Err("enabled trash days must equal the last positive period");
        }
        Ok(Self {
            days: fields.days,
            last_positive_days: fields.last_positive_days,
        })
    }
}

/// Mutable shared volume settings, separate from immutable storage identity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeSettings {
    /// Monotonic revision used for atomic compare-and-set updates.
    pub revision: u64,
    /// Retention admission and expiry policy.
    pub trash: TrashPolicy,
    /// Optional logical capacity limit; `None` means unlimited.
    #[serde(default)]
    pub logical_quota_bytes: Option<u64>,
}

impl VolumeSettings {
    /// Replica ordering of tag-19 settings: a higher revision replaces `self`,
    /// an equal revision must be identical, and an older one is ignored.
    pub fn is_superseded_by(&self, next: &Self) -> crate::MetaResult<bool> {
        if next.revision == self.revision && next != self {
            return Err(crate::MetaError::Storage(
                "conflicting volume settings at equal revision".into(),
            ));
        }
        Ok(next.revision > self.revision)
    }
}

/// Local serving capability for mutable volume settings.
pub trait MetaVolumeSettings: Send + Sync {
    /// Read the durable serving value; absent settings mean the disabled default.
    fn volume_settings(&self) -> crate::MetaResult<VolumeSettings>;

    /// Atomically update retention if the serving revision still matches.
    /// Returns `None` for a stale revision, leaving storage unchanged.
    fn compare_and_set_trash_days(
        &self,
        expected_revision: u64,
        days: u32,
    ) -> crate::MetaResult<Option<VolumeSettings>>;

    /// Atomically update the logical quota if the serving revision still matches.
    fn compare_and_set_logical_quota_bytes(
        &self,
        expected_revision: u64,
        bytes: Option<u64>,
    ) -> crate::MetaResult<Option<VolumeSettings>>;
}

/// Local retained namespace transactions. Applications enforce current ACLs
/// and native view policy; purge requests do not confer shared deletion authority.
pub trait MetaRetainedStore: MetaVolumeSettings {
    /// Adopt a complete, freshly materialized local stage payload without changing
    /// the source inode or its links. The caller holds the source session gate,
    /// has durably synced every stage body, and must transfer runtime upload
    /// ownership before removing the stage. Metadata and recovery events commit
    /// together; this operation alone does not restore the namespace.
    fn protect_retained_file_payload(
        &self,
        receipt: crate::Ino,
        source: RetainedSourceFence,
        stage: &[u8],
    ) -> crate::MetaResult<crate::InodeAttr>;
    /// Read the neutral per-entry receipt.
    fn retained_entry(&self, receipt: crate::Ino) -> crate::MetaResult<Option<RetainedEntry>>;
    /// Bind a caller-reserved receipt to the actual source in one local commit.
    fn retain_namespace_entry(
        &self,
        parent: crate::Ino,
        name: &[u8],
        store: crate::Ino,
        receipt: crate::Ino,
    ) -> crate::MetaResult<RetainedEntry>;
    /// Restore the original namespace identity without overwriting a destination.
    /// Adopt fresh local payloads from `stage`, move `receipt/data`, and journal
    /// the complete restored namespace in one transaction. The caller holds source gates.
    fn restore_retained_entry(
        &self,
        receipt: crate::Ino,
        parent: crate::Ino,
        name: &[u8],
        stage: &[u8],
        sources: &[RetainedSourceFence],
    ) -> crate::MetaResult<RetainedRestoreResult>;
    /// Record a purge request, preserving the payload reference pending settlement.
    fn request_retained_purge(
        &self,
        receipt: crate::Ino,
        reason: RetainedPurgeReason,
    ) -> crate::MetaResult<RetainedEntry>;
    /// Permanently delete one child entry beneath a retained directory tree in one local commit.
    /// Journals an authority fence on `receipt` (`Retained`) alongside the namespace delete.
    fn purge_retained_child(
        &self,
        receipt: crate::Ino,
        parent_source_ino: crate::Ino,
        name: &[u8],
        expected_ino: crate::Ino,
        is_dir: bool,
    ) -> crate::MetaResult<RetainedChildPurgeOutcome>;
}

/// Outcome of purging a single child entry beneath a retained directory view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetainedChildPurgeOutcome {
    /// Child was removed from the retained tree; holds the updated inode attributes
    /// when a regular file or symlink was unlinked.
    Removed(Option<crate::InodeAttr>),
    /// The receipt is no longer `Retained` or `(parent_source_ino, name)` no longer
    /// resolves to `expected_ino` beneath `receipt/data`.
    TargetChanged,
}

/// Committed restore identity and payload ownership captured by the same transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedRestoreResult {
    /// Terminal receipt.
    pub entry: RetainedEntry,
    /// Fresh payload epochs for the adopted original file inodes.
    pub upload_epochs: std::collections::HashMap<crate::Ino, u64>,
}

/// `(ctime, size, content_epoch)` observed before copying one retained source inode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetainedSourceFence {
    /// Source inode copied into the restore stage.
    pub ino: crate::Ino,
    /// Change time observed before copying the inode.
    pub ctime: i64,
    /// Logical size observed before copying the inode.
    pub size: u64,
    /// Payload generation observed before copying the inode.
    pub content_epoch: u64,
}

impl TrashPolicy {
    /// Current setting. Zero disables admission of new retained entries.
    pub const fn days(self) -> u32 {
        self.days
    }

    /// Whether new native Move to Trash operations may retain data.
    pub const fn enabled(self) -> bool {
        self.days > 0
    }

    /// Apply an explicit shared setting change without resetting deletion times.
    pub fn set_days(&mut self, days: u32) {
        self.days = days;
        if days > 0 {
            self.last_positive_days = days;
        }
    }

    /// Period applicable to existing entries, including after disabling admission.
    pub const fn existing_entry_days(self) -> u32 {
        self.last_positive_days
    }

    /// Whether an entry is eligible for expiry at an explicitly supplied time.
    ///
    /// Eligibility is not deletion authority: shared settlement and live,
    /// snapshot, open-handle and unpublished references still protect payloads.
    /// A clock before the initial deletion time cannot expire an entry.
    pub fn is_expired(self, deleted_at_unix_secs: u64, now_unix_secs: u64) -> bool {
        let Some(age) = now_unix_secs.checked_sub(deleted_at_unix_secs) else {
            return false;
        };
        self.last_positive_days > 0 && age >= u64::from(self.last_positive_days) * 86_400
    }
}

#[cfg(test)]
mod tests {
    use super::TrashPolicy;

    #[test]
    fn decode_rejects_inconsistent_positive_period() {
        for json in [
            r#"{"days":7,"last_positive_days":0}"#,
            r#"{"days":7,"last_positive_days":1}"#,
        ] {
            assert!(serde_json::from_str::<TrashPolicy>(json).is_err());
        }
        assert!(
            serde_json::from_str::<TrashPolicy>(r#"{"days":0,"last_positive_days":7}"#).is_ok()
        );
    }

    #[test]
    fn new_volume_disables_retention_and_expiry() {
        let policy = TrashPolicy::default();
        assert!(!policy.enabled());
        assert_eq!(policy.days(), 0);
        assert!(!policy.is_expired(0, u64::MAX));
    }

    #[test]
    fn expiry_uses_initial_deletion_time_and_inclusive_whole_day_boundary() {
        let mut policy = TrashPolicy::default();
        policy.set_days(2);
        assert!(!policy.is_expired(100, 99));
        assert!(!policy.is_expired(100, 100 + 2 * 86_400 - 1));
        assert!(policy.is_expired(100, 100 + 2 * 86_400));
    }

    #[test]
    fn disabling_stops_admission_but_preserves_expiry() {
        let mut policy = TrashPolicy::default();
        policy.set_days(3);
        policy.set_days(0);
        policy.set_days(0);
        assert!(!policy.enabled());
        assert_eq!(policy.existing_entry_days(), 3);
        assert!(!policy.is_expired(0, 2 * 86_400));
        assert!(policy.is_expired(0, 3 * 86_400));
    }

    #[test]
    fn positive_changes_apply_to_existing_entries() {
        let mut policy = TrashPolicy::default();
        policy.set_days(30);
        assert!(!policy.is_expired(0, 10 * 86_400));
        policy.set_days(5);
        assert!(policy.is_expired(0, 10 * 86_400));
        policy.set_days(0);
        policy.set_days(20);
        assert!(!policy.is_expired(0, 10 * 86_400));
    }

    #[test]
    fn maximum_period_does_not_overflow() {
        let mut policy = TrashPolicy::default();
        policy.set_days(u32::MAX);
        let duration = u64::from(u32::MAX) * 86_400;
        assert!(!policy.is_expired(0, duration - 1));
        assert!(policy.is_expired(0, duration));
        assert!(!policy.is_expired(u64::MAX - 1, u64::MAX));
    }

    #[test]
    fn disabled_policy_roundtrip_preserves_last_positive_period() {
        let mut policy = TrashPolicy::default();
        policy.set_days(7);
        policy.set_days(0);
        let bytes = serde_json::to_vec(&policy).unwrap();
        let recovered: TrashPolicy = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(recovered, policy);
        assert!(recovered.is_expired(0, 7 * 86_400));
    }
}
