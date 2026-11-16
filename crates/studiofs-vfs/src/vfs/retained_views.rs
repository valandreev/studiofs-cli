//! SFS-1054 private retained store and trash-name views.
//!
//! Retained entries live under a hidden root directory that no user path can
//! reach. A positive trash period turns a rename into the trash root into a
//! retained move; the receipt, not the OS name, is the authority. Names under
//! the trash root are views derived from receipts. Restore and purge are the
//! only transitions reachable through those names.

use super::ignored_entries::TrashViewBacking;
use super::namespace::NamespaceMutationOps;
use super::{
    AccessLevel, FileSessionOperation, FileType, Ino, InodeAttr, MetaError, ROOT_INO, StudioVfs,
    VfsError, VfsResult,
};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::Ordering;
use studiofs_meta::trash::{
    RetainedChildPurgeOutcome, RetainedEntry, RetainedEntryState, RetainedPurgeReason,
};

/// Active native OS trash projection profile for a VFS mount.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NativeTrashProfile {
    /// Windows `$RECYCLE.BIN` with `$R<hex>` / `$I<hex>` pairs.
    #[default]
    Windows,
    /// Linux Freedesktop `.Trash-<uid>/{files,info}` with `.trashinfo` records.
    Linux { uid: u32 },
    /// macOS `.Trashes/<uid>` user trash container.
    MacOs { uid: u32 },
}

impl NativeTrashProfile {
    pub(super) fn root_name(self) -> Vec<u8> {
        match self {
            Self::Windows => TRASH_ROOT_NAME.to_vec(),
            Self::Linux { uid } => format!(".Trash-{uid}").into_bytes(),
            Self::MacOs { .. } => b".Trashes".to_vec(),
        }
    }

    pub(super) fn matches_root_name(self, name: &[u8]) -> bool {
        match self {
            Self::Windows => name.eq_ignore_ascii_case(TRASH_ROOT_NAME),
            Self::Linux { uid } => name == format!(".Trash-{uid}").as_bytes(),
            Self::MacOs { .. } => name == b".Trashes",
        }
    }

    pub(super) fn root_mode(self) -> u16 {
        match self {
            Self::Linux { .. } => 0o700,
            Self::Windows | Self::MacOs { .. } => 0o755,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TrashContainerRole {
    WindowsRoot(Ino),
    LinuxRoot(Ino),
    LinuxFiles(Ino),
    LinuxInfo(Ino),
    /// Always-empty `expunged` sink: GVFS delete renames a receipt here (= Clear).
    LinuxExpunged(Ino),
    MacOsRoot(Ino),
    MacOsUser(Ino),
}

fn posix_trash_name_prefix(name: &[u8], limit: usize) -> Vec<u8> {
    let mut end = name.len().min(limit);
    if let Ok(text) = std::str::from_utf8(name) {
        while !text.is_char_boundary(end) {
            end -= 1;
        }
    }
    name[..end].to_vec()
}

/// Hidden root directory holding every retained receipt.
pub(super) const PRIVATE_STORE_NAME: &[u8] = b".studiofs-retained";

/// Synthesized `(name, attr)` entries for a retained directory listing.
pub(super) type TrashListingEntries = Vec<(Vec<u8>, InodeAttr)>;

tokio::task_local! {
    /// Set only by the internal restore seam. Public entrypoints never set it, so
    /// a user operation running on another task always sees the store as absent.
    static TRUSTED_RETAINED_SCOPE: bool;
}

/// Runs `fut` with trusted access to the private store. Only the restore copy
/// and its cleanup use this; it is not an exported bypass.
pub(super) async fn trusted_retained_scope<T>(fut: impl Future<Output = T>) -> T {
    TRUSTED_RETAINED_SCOPE.scope(true, fut).await
}

pub(super) fn in_trusted_retained_scope() -> bool {
    TRUSTED_RETAINED_SCOPE
        .try_with(|trusted| *trusted)
        .unwrap_or(false)
}
/// Windows recycle-bin root whose children are retained views.
const TRASH_ROOT_NAME: &[u8] = b"$RECYCLE.BIN";

/// Canonical view name for a receipt: `$R` plus its uppercase hex inode.
pub(super) fn trash_view_name(receipt: Ino) -> String {
    format!("$R{receipt:06X}")
}

/// Parses only the canonical spelling, so every receipt has one name.
pub(super) fn trash_view_receipt(name: &[u8]) -> Option<Ino> {
    let hex = name.strip_prefix(b"$R")?;
    let receipt = Ino::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?;
    (trash_view_name(receipt).as_bytes() == name).then_some(receipt)
}

impl StudioVfs {
    /// Configures the native OS trash projection profile for this client mount.
    pub fn set_native_trash_profile(&self, profile: NativeTrashProfile) {
        *self.state.metrics.native_trash_profile.lock() = profile;
        self.state.metrics.posix_trash_view_names.lock().clear();
        self.clear_trash_view_ignored_entries();
        self.state
            .metrics
            .active_trash_root_ino
            .store(0, Ordering::Release);
        let _ = self.trash_root_ino();
        self.bump_directory_generation(ROOT_INO);
    }

    /// Returns the active native OS trash projection profile for this client mount.
    pub fn native_trash_profile(&self) -> NativeTrashProfile {
        *self.state.metrics.native_trash_profile.lock()
    }

    /// Returns `true` when bridges must use zero entry/attribute kernel TTLs and
    /// bypass cached directory snapshots (trash containers, generated records, and
    /// retained views).
    pub fn should_bypass_bridge_entry_cache(&self, ino: Ino) -> bool {
        if self.is_synthesized_ignored_ino(ino) {
            return !self.is_runtime_stats_file_ino(ino);
        }
        ino != 0
            && self
                .state
                .metrics
                .active_trash_root_ino
                .load(Ordering::Acquire)
                == ino
    }

    /// Whether a native Move to Trash currently retains data (positive period).
    pub(super) fn trash_retention_enabled(&self) -> VfsResult<bool> {
        let meta = self.sync_meta();
        let Some(store) = meta.retained_store() else {
            return Ok(false);
        };
        Ok(store.volume_settings()?.trash.enabled())
    }

    /// Returns a root directory, creating it once; a concurrent creator wins cleanly.
    fn ensure_root_directory(&self, name: &[u8], mode: u16) -> VfsResult<Ino> {
        let meta = self.sync_meta();
        match meta.lookup(ROOT_INO, name) {
            Ok(entry) => return Ok(entry.ino),
            Err(MetaError::EntryNotFound { .. }) => {}
            Err(err) => return Err(err.into()),
        }
        // Use the VFS inode reservation, as `mkdir` does.
        let ino = self.reserve_namespace_ino()?;
        match meta.create_dir_with_ino(ino, ROOT_INO, name, 0, 0, mode) {
            Ok(attr) => {
                self.invalidate_attr_read_cache([ROOT_INO]);
                self.dentry_cache_invalidate(ROOT_INO, name);
                self.bump_namespace_for_parents([ROOT_INO]);
                Ok(attr.ino)
            }
            Err(MetaError::EntryExists { .. }) => Ok(meta.lookup(ROOT_INO, name)?.ino),
            Err(err) => Err(err.into()),
        }
    }

    /// Returns `true` if `ino` is `.studiofs-retained` or every parent binding
    /// resolves beneath `.studiofs-retained`.
    pub(super) fn inode_is_in_private_store(&self, ino: Ino) -> VfsResult<bool> {
        if ino == 0 || ino == ROOT_INO {
            return Ok(false);
        }
        let store = match self.sync_meta().lookup(ROOT_INO, PRIVATE_STORE_NAME) {
            Ok(entry) => entry.ino,
            Err(MetaError::EntryNotFound { .. }) => return Ok(false),
            Err(err) => return Err(err.into()),
        };
        self.inode_is_private(ino, store, 0)
    }

    /// Rejects any inode that is the private store or lies below it, as if it
    /// did not exist. The restore seam is the only trusted caller.
    // ponytail: one root lookup plus an ancestor walk per check; cache the store inode if profiling shows it.
    pub(super) fn reject_private_store_ino(&self, ino: Ino) -> VfsResult<()> {
        if ino == 0 || ino == ROOT_INO || in_trusted_retained_scope() {
            return Ok(());
        }
        if self.inode_is_in_private_store(ino)? {
            return Err(VfsError::Meta(MetaError::InodeNotFound(ino)));
        }
        Ok(())
    }

    /// Private means every parent binding is private, down to the store itself.
    /// A payload with a live outside hard link keeps its outside parent, so it
    /// stays reachable by that name. Without bindings, `attr.parent` decides.
    fn inode_is_private(&self, ino: Ino, store: Ino, depth: usize) -> VfsResult<bool> {
        if ino == store {
            return Ok(true);
        }
        if ino == 0 || ino == ROOT_INO || depth > 64 {
            return Ok(false);
        }
        let meta = self.sync_meta();
        let mut parents: Vec<Ino> = meta
            .entry_bindings_for_inode(ino)?
            .into_iter()
            .map(|(parent, _)| parent)
            .collect();
        if parents.is_empty() {
            match meta.get_attr(ino) {
                Ok(attr) => parents.push(attr.parent),
                Err(MetaError::InodeNotFound(_)) => return Ok(false),
                Err(err) => return Err(err.into()),
            }
        }
        for parent in parents {
            if !self.inode_is_private(parent, store, depth + 1)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Path form of the store guard: the store name at the root and its subtree.
    pub(super) fn reject_private_store_dir(&self, dir: Ino) -> VfsResult<()> {
        self.reject_private_store_ino(dir)
    }

    /// Hides the store name at the root and everything beneath it from paths.
    pub(super) fn reject_private_store_path(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        if parent == ROOT_INO && name == PRIVATE_STORE_NAME {
            return Err(Self::ignored_entry_not_found(parent, name));
        }
        self.reject_private_store_dir(parent)
    }

    fn trash_view_receipt_access_by_receipt_inner(
        &self,
        receipt: Ino,
    ) -> VfsResult<Option<AccessLevel>> {
        let meta = self.sync_meta();
        let Some(store) = meta.retained_store() else {
            return Ok(None);
        };
        let Some(entry) = store.retained_entry(receipt)? else {
            return Ok(None);
        };
        if entry.state != RetainedEntryState::Retained {
            return Ok(None);
        }
        // A missing original parent, or one that is itself trashed, hides the
        // entry until it is restored. A new directory at the old path does not
        // inherit it: the identity is the inode, not the name.
        if meta.get_attr(entry.original_parent).is_err()
            || self.directory_has_root_child_ancestor(entry.original_parent, |name| {
                super::is_probable_trash_component(name) || name == PRIVATE_STORE_NAME
            })?
        {
            return Ok(None);
        }
        let access = self.effective_access_level(entry.original_parent)?;
        if access == AccessLevel::NoAccess {
            return Ok(None);
        }
        Ok(Some(access))
    }

    /// Returns `original_parent_access` when `receipt` is currently visible
    /// to this client under its ACL and ancestor rules.
    pub(super) fn trash_view_receipt_access_by_receipt(
        &self,
        receipt: Ino,
    ) -> VfsResult<Option<AccessLevel>> {
        self.trash_view_receipt_access_by_receipt_inner(receipt)
    }

    pub(super) fn trash_container_role(&self, ino: Ino) -> VfsResult<Option<TrashContainerRole>> {
        let active_root = self
            .state
            .metrics
            .active_trash_root_ino
            .load(Ordering::Acquire);
        if ino < super::FIRST_IGNORED_INO && active_root != 0 && ino != active_root {
            return Ok(None);
        }
        let profile = self.native_trash_profile();
        if let Some(trash_root) = self.trash_root_ino()?
            && trash_root == ino
        {
            return Ok(Some(match profile {
                NativeTrashProfile::Windows => TrashContainerRole::WindowsRoot(ino),
                NativeTrashProfile::Linux { .. } => TrashContainerRole::LinuxRoot(ino),
                NativeTrashProfile::MacOs { .. } => TrashContainerRole::MacOsRoot(ino),
            }));
        }
        if ino < super::FIRST_IGNORED_INO {
            return Ok(None);
        }
        let Some(ignored) = self.ignored_entry(ino) else {
            return Ok(None);
        };
        let Some(trash_root) = self.trash_root_ino()? else {
            return Ok(None);
        };
        if ignored.parent != trash_root {
            return Ok(None);
        }
        Ok(match (&ignored.view, profile) {
            (Some(TrashViewBacking::LinuxFilesDir), NativeTrashProfile::Linux { .. }) => {
                Some(TrashContainerRole::LinuxFiles(ino))
            }
            (Some(TrashViewBacking::LinuxInfoDir), NativeTrashProfile::Linux { .. }) => {
                Some(TrashContainerRole::LinuxInfo(ino))
            }
            (Some(TrashViewBacking::LinuxExpungedDir), NativeTrashProfile::Linux { .. }) => {
                Some(TrashContainerRole::LinuxExpunged(ino))
            }
            (Some(TrashViewBacking::MacOsUserDir), NativeTrashProfile::MacOs { .. }) => {
                Some(TrashContainerRole::MacOsUser(ino))
            }
            _ => None,
        })
    }

    pub(super) fn is_trash_container_ino(&self, ino: Ino) -> VfsResult<bool> {
        Ok(self.trash_container_role(ino)?.is_some())
    }

    pub(super) fn is_posix_trash_move_target_dir(&self, ino: Ino) -> VfsResult<bool> {
        Ok(matches!(
            self.trash_container_role(ino)?,
            Some(TrashContainerRole::LinuxFiles(_) | TrashContainerRole::MacOsUser(_))
        ))
    }

    pub(super) fn is_trash_non_target_container_ino(&self, ino: Ino) -> VfsResult<bool> {
        Ok(matches!(
            self.trash_container_role(ino)?,
            Some(
                TrashContainerRole::LinuxRoot(_)
                    | TrashContainerRole::LinuxInfo(_)
                    | TrashContainerRole::LinuxExpunged(_)
                    | TrashContainerRole::MacOsRoot(_)
            )
        ))
    }

    fn linux_subcontainer_attr(&self, trash_root: Ino, uid: u32, info: bool) -> InodeAttr {
        if info {
            self.linux_container_attr(trash_root, uid, b"info", TrashViewBacking::LinuxInfoDir)
        } else {
            self.linux_container_attr(trash_root, uid, b"files", TrashViewBacking::LinuxFilesDir)
        }
    }

    fn linux_expunged_attr(&self, trash_root: Ino, uid: u32) -> InodeAttr {
        self.linux_container_attr(
            trash_root,
            uid,
            b"expunged",
            TrashViewBacking::LinuxExpungedDir,
        )
    }

    fn linux_container_attr(
        &self,
        trash_root: Ino,
        uid: u32,
        name: &[u8],
        backing: TrashViewBacking,
    ) -> InodeAttr {
        let attr = self.upsert_ignored_view_entry(
            trash_root,
            name,
            FileType::Directory,
            0o700,
            None,
            0,
            Some(backing),
        );
        self.update_ignored_entry(attr.ino, |a| {
            a.uid = uid;
            a.gid = uid;
        })
        .unwrap_or(attr)
    }

    fn macos_user_container_attr(&self, trash_root: Ino, uid: u32) -> InodeAttr {
        let name = uid.to_string();
        let attr = self.upsert_ignored_view_entry(
            trash_root,
            name.as_bytes(),
            FileType::Directory,
            0o700,
            None,
            0,
            Some(TrashViewBacking::MacOsUserDir),
        );
        self.update_ignored_entry(attr.ino, |a| {
            a.uid = uid;
            a.gid = uid;
        })
        .unwrap_or(attr)
    }

    /// Synchronizes stable deterministic POSIX trash view names for all visible
    /// receipts under the active client's ACL rules.
    // shortcut: trash lookups scan visible receipts; cache by generation if large bins require it.
    fn sync_posix_trash_view_names(&self) -> VfsResult<HashMap<Ino, Vec<u8>>> {
        let receipts = self.visible_trash_receipts()?;
        let meta = self.sync_meta();
        let Some(store) = meta.retained_store() else {
            self.state.metrics.posix_trash_view_names.lock().clear();
            return Ok(HashMap::new());
        };
        let mut visible_entries = Vec::with_capacity(receipts.len());
        for receipt in receipts {
            if let Some(entry) = store.retained_entry(receipt)? {
                visible_entries.push((receipt, entry.deleted_at_unix_secs, entry.original_name));
            }
        }
        visible_entries.sort_by_key(|(receipt, deleted_at, _)| (*deleted_at, *receipt));

        let visible_set: HashSet<Ino> = visible_entries.iter().map(|(r, _, _)| *r).collect();
        let mut map = self.state.metrics.posix_trash_view_names.lock();
        map.retain(|receipt, _| visible_set.contains(receipt));
        let mut used_names: HashSet<Vec<u8>> = map.values().cloned().collect();
        let name_limit = super::MAX_NAME_BYTES
            - if matches!(
                self.native_trash_profile(),
                NativeTrashProfile::Linux { .. }
            ) {
                b".trashinfo".len()
            } else {
                0
            };
        for (receipt, _, original_name) in visible_entries {
            if map.contains_key(&receipt) {
                continue;
            }
            let base_name = posix_trash_name_prefix(&original_name, name_limit);
            let assigned = if !used_names.contains(&base_name) {
                base_name
            } else {
                let mut suffix = 2usize;
                loop {
                    let suffix_text = format!(".{suffix}");
                    let mut candidate =
                        posix_trash_name_prefix(&original_name, name_limit - suffix_text.len());
                    candidate.extend_from_slice(suffix_text.as_bytes());
                    if !used_names.contains(&candidate) {
                        break candidate;
                    }
                    suffix += 1;
                }
            };
            used_names.insert(assigned.clone());
            map.insert(receipt, assigned);
        }
        Ok(map.clone())
    }

    pub(super) fn posix_view_name_for_receipt(&self, receipt: Ino) -> VfsResult<Option<Vec<u8>>> {
        Ok(self.sync_posix_trash_view_names()?.remove(&receipt))
    }

    fn posix_receipt_for_view_name(&self, name: &[u8]) -> VfsResult<Option<Ino>> {
        let map = self.sync_posix_trash_view_names()?;
        Ok(map
            .into_iter()
            .find_map(|(receipt, view_name)| (view_name == name).then_some(receipt)))
    }

    fn linux_receipt_for_sidecar_name(&self, name: &[u8]) -> VfsResult<Option<Ino>> {
        let Some(stem) = name.strip_suffix(b".trashinfo") else {
            return Ok(None);
        };
        if stem.is_empty() {
            return Ok(None);
        }
        self.posix_receipt_for_view_name(stem)
    }

    /// Verifies that `(parent, name)` still corresponds to `receipt` under the
    /// active native trash profile.
    pub(super) fn trash_entry_matches_receipt(
        &self,
        parent: Ino,
        name: &[u8],
        receipt: Ino,
    ) -> VfsResult<bool> {
        let Some(role) = self.trash_container_role(parent)? else {
            return Ok(false);
        };
        match role {
            TrashContainerRole::WindowsRoot(_) => Ok(trash_view_receipt(name) == Some(receipt)
                || trash_sidecar_receipt(name) == Some(receipt)),
            TrashContainerRole::LinuxFiles(_) | TrashContainerRole::MacOsUser(_) => {
                Ok(self.posix_view_name_for_receipt(receipt)?.as_deref() == Some(name))
            }
            TrashContainerRole::LinuxInfo(_) => {
                let Some(stem) = name.strip_suffix(b".trashinfo") else {
                    return Ok(false);
                };
                Ok(self.posix_view_name_for_receipt(receipt)?.as_deref() == Some(stem))
            }
            TrashContainerRole::LinuxRoot(_)
            | TrashContainerRole::LinuxExpunged(_)
            | TrashContainerRole::MacOsRoot(_) => Ok(false),
        }
    }

    /// Returns the receipt and its original parent's current effective access level
    /// when `(parent, name)` is a visible restorable/viewable retained entry.
    ///
    /// Sidecars (`$I*` / `*.trashinfo`), terminal receipts, purge-requested
    /// receipts, and inaccessible receipts return `Ok(None)`.
    pub(super) fn trash_view_receipt_access(
        &self,
        parent: Ino,
        name: &[u8],
    ) -> VfsResult<Option<(Ino, AccessLevel)>> {
        let Some(role) = self.trash_container_role(parent)? else {
            return Ok(None);
        };
        let receipt = match role {
            TrashContainerRole::WindowsRoot(_) => match trash_view_receipt(name) {
                Some(r) => r,
                None => return Ok(None),
            },
            TrashContainerRole::LinuxFiles(_) | TrashContainerRole::MacOsUser(_) => {
                match self.posix_receipt_for_view_name(name)? {
                    Some(r) => r,
                    None => return Ok(None),
                }
            }
            TrashContainerRole::LinuxRoot(_)
            | TrashContainerRole::LinuxInfo(_)
            | TrashContainerRole::LinuxExpunged(_)
            | TrashContainerRole::MacOsRoot(_) => return Ok(None),
        };
        let Some(access) = self.trash_view_receipt_access_by_receipt(receipt)? else {
            return Ok(None);
        };
        Ok(Some((receipt, access)))
    }

    /// Returns the receipt when `(parent, name)` is a visible restorable view.
    ///
    /// Terminal, purge-requested, and sidecar names return `Ok(None)`.
    pub(super) fn trash_view_receipt_for(
        &self,
        parent: Ino,
        name: &[u8],
    ) -> VfsResult<Option<Ino>> {
        Ok(self
            .trash_view_receipt_access(parent, name)?
            .map(|(receipt, _)| receipt))
    }

    /// Returns the receipt when `(parent, name)` may purge a retained entry via
    /// `unlink` / `rmdir`: either the primary restorable view OR a Linux
    /// `info/<name>.trashinfo` sidecar record.
    pub(super) fn trash_purge_receipt_for(
        &self,
        parent: Ino,
        name: &[u8],
    ) -> VfsResult<Option<Ino>> {
        if let Some(receipt) = self.trash_view_receipt_for(parent, name)? {
            return Ok(Some(receipt));
        }
        if matches!(
            self.trash_container_role(parent)?,
            Some(TrashContainerRole::LinuxInfo(_))
        ) && let Some(receipt) = self.linux_receipt_for_sidecar_name(name)?
            && self
                .trash_view_receipt_access_by_receipt(receipt)?
                .is_some()
        {
            return Ok(Some(receipt));
        }
        Ok(None)
    }

    pub(super) fn bump_trash_view_generations(&self, receipt: Ino) {
        for dir_ino in self.collect_trash_view_dir_inos_for_receipt(receipt) {
            self.bump_directory_generation(dir_ino);
        }
        if let Ok(Some(trash)) = self.trash_root_ino() {
            self.bump_directory_generation(trash);
            match self.native_trash_profile() {
                NativeTrashProfile::Windows => {}
                NativeTrashProfile::Linux { uid } => {
                    self.bump_directory_generation(
                        self.linux_subcontainer_attr(trash, uid, false).ino,
                    );
                    self.bump_directory_generation(
                        self.linux_subcontainer_attr(trash, uid, true).ino,
                    );
                }
                NativeTrashProfile::MacOs { uid } => {
                    self.bump_directory_generation(self.macos_user_container_attr(trash, uid).ino);
                }
            }
        }
    }

    /// Validates delete access on a retained view or generated `.trashinfo` before
    /// WinFSP / unlink marks delete disposition.
    pub fn enforce_retained_view_delete_access(&self, view_ino: Ino) -> VfsResult<()> {
        if view_ino < super::FIRST_IGNORED_INO {
            return Ok(());
        }
        let Some(ignored) = self.ignored_entry(view_ino) else {
            return Ok(());
        };
        let receipt = match &ignored.view {
            Some(TrashViewBacking::Payload { receipt })
            | Some(TrashViewBacking::Subtree { receipt, .. }) => *receipt,
            Some(TrashViewBacking::Bytes { receipt, .. })
                if matches!(
                    self.trash_container_role(ignored.parent)?,
                    Some(TrashContainerRole::LinuxInfo(_))
                ) =>
            {
                *receipt
            }
            Some(
                TrashViewBacking::Bytes { .. }
                | TrashViewBacking::VirtualTrashRoot
                | TrashViewBacking::LinuxFilesDir
                | TrashViewBacking::LinuxInfoDir
                | TrashViewBacking::LinuxExpungedDir
                | TrashViewBacking::MacOsUserDir,
            )
            | None => return Ok(()),
        };
        match self.trash_view_receipt_access_by_receipt(receipt)? {
            None => Err(VfsError::Meta(MetaError::InodeNotFound(view_ino))),
            Some(AccessLevel::ReadWrite) => self.enforce_metadata_mutation_authority(receipt),
            Some(actual) => Err(VfsError::Meta(MetaError::AccessDenied {
                ino: view_ino,
                principal: self.acl_principal.clone(),
                required: AccessLevel::ReadWrite,
                actual,
            })),
        }
    }

    pub(super) fn cleanup_posix_draft_trashinfo(&self, target_dir: Ino, target_name: &[u8]) {
        let Ok(Some(TrashContainerRole::LinuxFiles(_))) = self.trash_container_role(target_dir)
        else {
            return;
        };
        let NativeTrashProfile::Linux { uid } = self.native_trash_profile() else {
            return;
        };
        let Ok(Some(trash)) = self.trash_root_ino() else {
            return;
        };
        let info_dir = self.linux_subcontainer_attr(trash, uid, true).ino;
        let mut draft_name = target_name.to_vec();
        draft_name.extend_from_slice(b".trashinfo");
        if self
            .ignored_entry_lookup(info_dir, &draft_name)
            .is_some_and(|existing| existing.view.is_none())
            && self.unlink_ignored_entry(info_dir, &draft_name)
        {
            self.bump_directory_generation(info_dir);
        }
    }

    pub(super) fn cleanup_trash_view_cache_on_terminal(&self, receipt: Ino) {
        let view_dirs = self.collect_trash_view_dir_inos_for_receipt(receipt);
        let posix_name = self
            .state
            .metrics
            .posix_trash_view_names
            .lock()
            .remove(&receipt);
        if let Ok(Some(trash)) = self.trash_root_ino() {
            self.unlink_ignored_subtree(trash, trash_view_name(receipt).as_bytes());
            self.unlink_ignored_entry(trash, trash_sidecar_name(receipt).as_bytes());
            match self.native_trash_profile() {
                NativeTrashProfile::Windows => {}
                NativeTrashProfile::Linux { uid } => {
                    let files_dir = self.linux_subcontainer_attr(trash, uid, false).ino;
                    let info_dir = self.linux_subcontainer_attr(trash, uid, true).ino;
                    if let Some(ref view_name) = posix_name {
                        self.unlink_ignored_subtree(files_dir, view_name);
                        let mut sidecar = view_name.clone();
                        sidecar.extend_from_slice(b".trashinfo");
                        self.unlink_ignored_entry(info_dir, &sidecar);
                    }
                    self.bump_directory_generation(files_dir);
                    self.bump_directory_generation(info_dir);
                }
                NativeTrashProfile::MacOs { uid } => {
                    let user_dir = self.macos_user_container_attr(trash, uid).ino;
                    if let Some(ref view_name) = posix_name {
                        self.unlink_ignored_subtree(user_dir, view_name);
                    }
                    self.bump_directory_generation(user_dir);
                }
            }
            self.bump_directory_generation(trash);
        }
        for dir_ino in view_dirs {
            self.bump_directory_generation(dir_ino);
        }
        self.bump_directory_generation(ROOT_INO);
    }

    /// Retains `(parent, name)` under a fresh receipt instead of deleting it.
    pub(super) fn retain_entry_into_trash(
        &self,
        namespace_mutations: &NamespaceMutationOps<'_>,
        parent: Ino,
        name: &[u8],
        target_parent: Ino,
        target_name: &[u8],
    ) -> VfsResult<()> {
        let store_dir = self.ensure_root_directory(PRIVATE_STORE_NAME, 0o700)?;
        let profile = self.native_trash_profile();
        let trash_root = self.ensure_root_directory(&profile.root_name(), profile.root_mode())?;
        self.state
            .metrics
            .active_trash_root_ino
            .store(trash_root, Ordering::Release);
        let meta = self.sync_meta();
        let source_ino = meta.lookup(parent, name)?.ino;
        let receipt = meta.next_inode()?;
        let store = meta
            .retained_store()
            .ok_or(VfsError::OperationNotSupported)?;
        store.retain_namespace_entry(parent, name, store_dir, receipt)?;
        namespace_mutations.record_removal(parent, name, [source_ino]);
        self.cleanup_posix_draft_trashinfo(target_parent, target_name);
        self.bump_acl_namespace_epoch();
        self.bump_trash_view_generations(receipt);
        Ok(())
    }

    /// Restores a visible view into `(parent, name)`, never over an existing entry.
    pub(super) async fn restore_trash_view_entry(
        &self,
        receipt: Ino,
        parent: Ino,
        name: &[u8],
    ) -> VfsResult<()> {
        let meta = self.sync_meta();
        let store = meta
            .retained_store()
            .ok_or(VfsError::OperationNotSupported)?;
        self.restore_retained_entry(store, receipt, parent, name)
            .await?;
        self.cleanup_trash_view_cache_on_terminal(receipt);
        Ok(())
    }

    /// Requests Clear for a visible view. Needs write access on the original parent.
    pub(super) fn request_trash_view_purge(&self, receipt: Ino) -> VfsResult<()> {
        let meta = self.sync_meta();
        let store = meta
            .retained_store()
            .ok_or(VfsError::OperationNotSupported)?;
        let entry = store
            .retained_entry(receipt)?
            .ok_or(VfsError::Meta(MetaError::InodeNotFound(receipt)))?;
        self.enforce_access(entry.original_parent, AccessLevel::ReadWrite)?;
        store.request_retained_purge(receipt, RetainedPurgeReason::User)?;
        self.cleanup_trash_view_cache_on_terminal(receipt);
        Ok(())
    }
}

/// Sidecar name paired with a view: `$I` plus the same six-digit hex suffix.
fn trash_sidecar_name(receipt: Ino) -> String {
    format!("$I{receipt:06X}")
}

/// Receipt for a canonical `$I` name; the suffix must match its `$R` pair exactly.
pub(super) fn trash_sidecar_receipt(name: &[u8]) -> Option<Ino> {
    let rest = name.strip_prefix(b"$I")?;
    let mut view = b"$R".to_vec();
    view.extend_from_slice(rest);
    trash_view_receipt(&view)
}

fn percent_encode_trashinfo_path(parts: &[Vec<u8>]) -> String {
    let mut out = String::new();
    for (idx, part) in parts.iter().enumerate() {
        if idx > 0 {
            out.push('/');
        }
        for &byte in part {
            match byte {
                b'A'..=b'Z'
                | b'a'..=b'z'
                | b'0'..=b'9'
                | b'-'
                | b'.'
                | b'_'
                | b'~'
                | b'!'
                | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'@' => out.push(byte as char),
                _ => out.push_str(&format!("%{byte:02X}")),
            }
        }
    }
    out
}

/// Native OS trash projection. Names and sidecar bytes are generated locally
/// from the shared receipt; no client mount path or client-written time becomes
/// shared authority.
impl StudioVfs {
    /// Stored or active virtual trash root inode, independently of its current
    /// user visibility.
    pub(super) fn trash_root_ino(&self) -> VfsResult<Option<Ino>> {
        let profile = self.native_trash_profile();
        let root_name = profile.root_name();
        match self.sync_meta().lookup(ROOT_INO, &root_name) {
            Ok(entry) => {
                self.state
                    .metrics
                    .active_trash_root_ino
                    .store(entry.ino, Ordering::Release);
                Ok(Some(entry.ino))
            }
            Err(MetaError::EntryNotFound { .. }) => {
                if let Some(ignored) = self.ignored_entry_lookup(ROOT_INO, &root_name)
                    && matches!(ignored.view, Some(TrashViewBacking::VirtualTrashRoot))
                {
                    self.state
                        .metrics
                        .active_trash_root_ino
                        .store(ignored.attr.ino, Ordering::Release);
                    return Ok(Some(ignored.attr.ino));
                }
                self.state
                    .metrics
                    .active_trash_root_ino
                    .store(0, Ordering::Release);
                Ok(None)
            }
            Err(err) => Err(err.into()),
        }
    }

    pub(super) fn normalize_trash_root_attr(&self, mut attr: InodeAttr) -> InodeAttr {
        let profile = self.native_trash_profile();
        match profile {
            NativeTrashProfile::Windows => attr,
            NativeTrashProfile::Linux { uid } => {
                attr.uid = uid;
                attr.gid = uid;
                attr.mode = (attr.mode & !0o777) | 0o700;
                attr
            }
            NativeTrashProfile::MacOs { uid } => {
                attr.uid = uid;
                attr.gid = 0;
                attr.mode = (attr.mode & !0o777) | 0o755;
                attr
            }
        }
    }

    fn ensure_virtual_trash_root(&self) -> InodeAttr {
        let profile = self.native_trash_profile();
        let root_name = profile.root_name();
        let attr = self.upsert_ignored_view_entry(
            ROOT_INO,
            &root_name,
            FileType::Directory,
            profile.root_mode(),
            None,
            0,
            Some(TrashViewBacking::VirtualTrashRoot),
        );
        let updated = self
            .update_ignored_entry(attr.ino, |a| {
                *a = self.normalize_trash_root_attr(a.clone());
            })
            .unwrap_or_else(|| self.normalize_trash_root_attr(attr));
        self.state
            .metrics
            .active_trash_root_ino
            .store(updated.ino, Ordering::Release);
        updated
    }

    /// Positive retention keeps the native root available; otherwise it is
    /// listed only while this client can read a retained entry beneath it.
    pub(super) fn trash_root_is_listed(&self) -> VfsResult<bool> {
        let retention_enabled = self.trash_retention_enabled()?;
        if retention_enabled && (!self.read_only_mount || self.trash_root_ino()?.is_some()) {
            return Ok(true);
        }
        Ok(!self.visible_trash_receipts()?.is_empty())
    }

    /// With a positive period the trash root must exist for native clients.
    pub(super) fn ensure_trash_root_when_enabled(&self) -> VfsResult<()> {
        if self.read_only_mount {
            return Ok(());
        }
        if self.trash_retention_enabled()? {
            let profile = self.native_trash_profile();
            let ino = self.ensure_root_directory(&profile.root_name(), profile.root_mode())?;
            self.state
                .metrics
                .active_trash_root_ino
                .store(ino, Ordering::Release);
        }
        Ok(())
    }

    /// Records this client's mount drive, such as `W:`, for `$I` paths. Local only.
    pub fn set_native_trash_path_prefix(&self, prefix: &str) {
        *self.state.metrics.native_trash_prefix.lock() =
            prefix.trim_end_matches(['\\', '/']).to_owned();
    }

    /// Attributes of the active profile's root trash folder when `name` names it.
    pub(super) fn visible_trash_root(&self, name: &[u8]) -> VfsResult<Option<InodeAttr>> {
        let profile = self.native_trash_profile();
        if !profile.matches_root_name(name) {
            return Ok(None);
        }
        self.ensure_trash_root_when_enabled()?;
        let meta = self.sync_meta();
        match meta.lookup(ROOT_INO, &profile.root_name()) {
            Ok(entry) => {
                self.state
                    .metrics
                    .active_trash_root_ino
                    .store(entry.ino, Ordering::Release);
                Ok(Some(
                    self.normalize_trash_root_attr(meta.get_attr(entry.ino)?),
                ))
            }
            Err(MetaError::EntryNotFound { .. }) => {
                if !self.visible_trash_receipts()?.is_empty() {
                    return Ok(Some(self.ensure_virtual_trash_root()));
                }
                Ok(None)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Receipts whose retained entry is currently visible to this client.
    pub(super) fn visible_trash_receipts(&self) -> VfsResult<Vec<Ino>> {
        let meta = self.sync_meta();
        let store_ino = match meta.lookup(ROOT_INO, PRIVATE_STORE_NAME) {
            Ok(entry) => entry.ino,
            Err(MetaError::EntryNotFound { .. }) => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };
        let mut receipts = Vec::new();
        for child in meta.readdir(store_ino)? {
            let Some(receipt) = std::str::from_utf8(&child.name)
                .ok()
                .and_then(|text| text.parse::<Ino>().ok())
            else {
                continue;
            };
            if self
                .trash_view_receipt_access_by_receipt(receipt)?
                .is_some()
            {
                receipts.push(receipt);
            }
        }
        receipts.sort_unstable();
        Ok(receipts)
    }

    /// Directory listing for any active trash container (`$RECYCLE.BIN`, `.Trash-<uid>`,
    /// `.Trash-<uid>/files`, `.Trash-<uid>/info`, `.Trashes`, `.Trashes/<uid>`).
    pub(super) fn trash_container_listing(
        &self,
        container_ino: Ino,
    ) -> VfsResult<Option<TrashListingEntries>> {
        let Some(role) = self.trash_container_role(container_ino)? else {
            return Ok(None);
        };
        let listing = match role {
            TrashContainerRole::WindowsRoot(trash) => self.trash_root_listing(trash)?,
            TrashContainerRole::LinuxRoot(trash) => {
                let NativeTrashProfile::Linux { uid } = self.native_trash_profile() else {
                    return Ok(Some(Vec::new()));
                };
                vec![
                    (
                        b"files".to_vec(),
                        self.linux_subcontainer_attr(trash, uid, false),
                    ),
                    (
                        b"info".to_vec(),
                        self.linux_subcontainer_attr(trash, uid, true),
                    ),
                    (b"expunged".to_vec(), self.linux_expunged_attr(trash, uid)),
                ]
            }
            TrashContainerRole::LinuxExpunged(_) => Vec::new(),
            TrashContainerRole::MacOsRoot(trash) => {
                let NativeTrashProfile::MacOs { uid } = self.native_trash_profile() else {
                    return Ok(Some(Vec::new()));
                };
                vec![(
                    uid.to_string().into_bytes(),
                    self.macos_user_container_attr(trash, uid),
                )]
            }
            TrashContainerRole::LinuxFiles(files_dir) => {
                let names = self.sync_posix_trash_view_names()?;
                let mut entries = Vec::new();
                for (_, view_name) in names {
                    if let Some(attr) = self.trash_child_attr(files_dir, &view_name)? {
                        entries.push((view_name, attr));
                    }
                }
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                entries
            }
            TrashContainerRole::LinuxInfo(info_dir) => {
                let names = self.sync_posix_trash_view_names()?;
                let mut entries = Vec::new();
                let mut active_names = HashSet::new();
                for (_, view_name) in names {
                    let mut info_name = view_name;
                    info_name.extend_from_slice(b".trashinfo");
                    if let Some(attr) = self.trash_child_attr(info_dir, &info_name)? {
                        active_names.insert(info_name.clone());
                        entries.push((info_name, attr));
                    }
                }
                for (draft_name, draft_attr) in self.list_ignored_children(info_dir) {
                    if !active_names.contains(&draft_name) {
                        entries.push((draft_name, draft_attr));
                    }
                }
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                entries
            }
            TrashContainerRole::MacOsUser(user_dir) => {
                let names = self.sync_posix_trash_view_names()?;
                let mut entries = Vec::new();
                let mut active_names = HashSet::new();
                for (_, view_name) in names {
                    if let Some(attr) = self.trash_child_attr(user_dir, &view_name)? {
                        active_names.insert(view_name.clone());
                        entries.push((view_name, attr));
                    }
                }
                for (ignored_name, ignored_attr) in self.list_ignored_children(user_dir) {
                    if !active_names.contains(&ignored_name) {
                        entries.push((ignored_name, ignored_attr));
                    }
                }
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                entries
            }
        };
        Ok(Some(listing))
    }

    fn list_ignored_children(&self, parent: Ino) -> Vec<(Vec<u8>, InodeAttr)> {
        let registry = self.state.metrics.ignored_entries.lock();
        let Some(children) = registry.children_by_parent.get(&parent) else {
            return Vec::new();
        };
        children
            .iter()
            .filter_map(|ino| registry.by_ino.get(ino))
            .filter(|ignored| ignored.view.is_none())
            .map(|ignored| (ignored.name.clone(), ignored.attr.clone()))
            .collect()
    }

    /// Each visible receipt contributes a `$R`/`$I` pair with matching suffixes.
    pub(super) fn trash_root_listing(&self, trash: Ino) -> VfsResult<Vec<(Vec<u8>, InodeAttr)>> {
        let mut listing = Vec::new();
        for receipt in self.visible_trash_receipts()? {
            for name in [trash_view_name(receipt), trash_sidecar_name(receipt)] {
                if let Some(attr) = self.trash_child_attr(trash, name.as_bytes())? {
                    listing.push((name.into_bytes(), attr));
                }
            }
        }
        Ok(listing)
    }

    fn trash_directory_bytes_total(&self, root_dir: Ino) -> VfsResult<u64> {
        let meta = self.sync_meta();
        let mut total = 0u64;
        let mut stack = vec![root_dir];
        let mut visited = HashSet::from([root_dir]);
        while let Some(dir) = stack.pop() {
            for child in meta.readdir(dir)? {
                if child.name == b"." || child.name == b".." {
                    continue;
                }
                match child.kind {
                    FileType::Directory => {
                        if visited.insert(child.ino) {
                            stack.push(child.ino);
                        }
                    }
                    FileType::RegularFile => {
                        let attr = self.getattr_unchecked(child.ino)?;
                        total = total.saturating_add(attr.size);
                    }
                    FileType::Symlink => {}
                }
            }
        }
        Ok(total)
    }

    fn trash_view_mode_and_size(&self, attr: &InodeAttr, regular_size: u64) -> (u16, u64) {
        match attr.kind {
            FileType::Directory
                if matches!(
                    self.native_trash_profile(),
                    NativeTrashProfile::Linux { .. }
                ) =>
            {
                (0o700, 0)
            }
            FileType::Directory => (0o555, 0),
            FileType::Symlink => (0o777, attr.size),
            FileType::RegularFile => (0o444, regular_size),
        }
    }

    pub(super) fn retained_view_source_ino(
        &self,
        ignored: &super::ignored_entries::IgnoredEntry,
    ) -> VfsResult<Option<Ino>> {
        match &ignored.view {
            Some(TrashViewBacking::Payload { receipt }) => {
                let meta = self.sync_meta();
                let Some(store) = meta.retained_store() else {
                    return Ok(None);
                };
                Ok(store.retained_entry(*receipt)?.map(|entry| entry.inode))
            }
            Some(TrashViewBacking::Subtree { source_ino, .. }) => Ok(Some(*source_ino)),
            Some(
                TrashViewBacking::Bytes { .. }
                | TrashViewBacking::VirtualTrashRoot
                | TrashViewBacking::LinuxFilesDir
                | TrashViewBacking::LinuxInfoDir
                | TrashViewBacking::LinuxExpungedDir
                | TrashViewBacking::MacOsUserDir,
            )
            | None => Ok(None),
        }
    }

    /// Returns `(receipt, source_dir_ino, original_parent)` when `view_ino` is a
    /// visible retained directory view or a nested directory below it.
    pub(super) fn retained_view_subtree_dir(
        &self,
        view_ino: Ino,
    ) -> VfsResult<Option<(Ino, Ino, Ino)>> {
        if view_ino < super::FIRST_IGNORED_INO {
            return Ok(None);
        }
        let Some(ignored) = self.ignored_entry(view_ino) else {
            return Ok(None);
        };
        let (receipt, maybe_source_ino) = match &ignored.view {
            Some(TrashViewBacking::Payload { receipt }) => (*receipt, None),
            Some(TrashViewBacking::Subtree {
                receipt,
                source_ino,
                ..
            }) => (*receipt, Some(*source_ino)),
            Some(
                TrashViewBacking::Bytes { .. }
                | TrashViewBacking::VirtualTrashRoot
                | TrashViewBacking::LinuxFilesDir
                | TrashViewBacking::LinuxInfoDir
                | TrashViewBacking::LinuxExpungedDir
                | TrashViewBacking::MacOsUserDir,
            )
            | None => return Ok(None),
        };
        self.ensure_ignored_entry_snapshot_visible(&ignored)?;
        let meta = self.sync_meta();
        let Some(store) = meta.retained_store() else {
            return Err(VfsError::Meta(MetaError::InodeNotFound(view_ino)));
        };
        let entry = store
            .retained_entry(receipt)?
            .ok_or(VfsError::Meta(MetaError::InodeNotFound(view_ino)))?;
        let source_ino = maybe_source_ino.unwrap_or(entry.inode);
        if ignored.attr.kind != FileType::Directory {
            return Err(VfsError::Meta(MetaError::NotADirectory(view_ino)));
        }
        Ok(Some((receipt, source_ino, entry.original_parent)))
    }

    /// Attributes of a synthesized child of any trash container, or `None` when
    /// `name` is not a visible container child, view, or generated sidecar.
    pub(super) fn trash_child_attr(
        &self,
        parent: Ino,
        name: &[u8],
    ) -> VfsResult<Option<InodeAttr>> {
        let Some(role) = self.trash_container_role(parent)? else {
            return Ok(None);
        };
        enum ChildKind {
            Payload(Ino),
            WindowsSidecar(Ino),
            LinuxTrashInfo(Ino),
        }
        let kind = match role {
            TrashContainerRole::WindowsRoot(trash) => {
                let (receipt, is_sidecar) =
                    match (trash_view_receipt(name), trash_sidecar_receipt(name)) {
                        (Some(receipt), _) => (receipt, false),
                        (None, Some(receipt)) => (receipt, true),
                        (None, None) => return Ok(None),
                    };
                let view = trash_view_name(receipt);
                if self
                    .trash_view_receipt_for(trash, view.as_bytes())?
                    .is_none()
                {
                    return Ok(None);
                }
                if is_sidecar {
                    ChildKind::WindowsSidecar(receipt)
                } else {
                    ChildKind::Payload(receipt)
                }
            }
            TrashContainerRole::LinuxRoot(trash) => {
                let NativeTrashProfile::Linux { uid } = self.native_trash_profile() else {
                    return Ok(None);
                };
                if name == b"files" {
                    return Ok(Some(self.linux_subcontainer_attr(trash, uid, false)));
                }
                if name == b"info" {
                    return Ok(Some(self.linux_subcontainer_attr(trash, uid, true)));
                }
                if name == b"expunged" {
                    return Ok(Some(self.linux_expunged_attr(trash, uid)));
                }
                return Ok(None);
            }
            TrashContainerRole::LinuxExpunged(_) => return Ok(None),
            TrashContainerRole::MacOsRoot(trash) => {
                let NativeTrashProfile::MacOs { uid } = self.native_trash_profile() else {
                    return Ok(None);
                };
                if name == uid.to_string().as_bytes() {
                    return Ok(Some(self.macos_user_container_attr(trash, uid)));
                }
                return Ok(None);
            }
            TrashContainerRole::LinuxFiles(_) | TrashContainerRole::MacOsUser(_) => {
                let Some(receipt) = self.trash_view_receipt_for(parent, name)? else {
                    return Ok(None);
                };
                ChildKind::Payload(receipt)
            }
            TrashContainerRole::LinuxInfo(_) => {
                let Some(receipt) = self.linux_receipt_for_sidecar_name(name)? else {
                    return Ok(None);
                };
                ChildKind::LinuxTrashInfo(receipt)
            }
        };

        let receipt = match kind {
            ChildKind::Payload(r) | ChildKind::WindowsSidecar(r) | ChildKind::LinuxTrashInfo(r) => {
                r
            }
        };
        let meta = self.sync_meta();
        let Some(store) = meta.retained_store() else {
            return Ok(None);
        };
        let Some(entry) = store.retained_entry(receipt)? else {
            return Ok(None);
        };

        match kind {
            ChildKind::Payload(receipt) => {
                // Dirty-aware: a cleaned-up writer's pending bytes are not in redb yet.
                let root_attr = self.getattr_unchecked(entry.inode)?;
                let total_size = if root_attr.kind == FileType::Directory {
                    self.trash_directory_bytes_total(entry.inode)?
                } else {
                    root_attr.size
                };
                let (mode, view_size) = self.trash_view_mode_and_size(&root_attr, total_size);
                let backing = TrashViewBacking::Payload { receipt };
                Ok(Some(self.upsert_ignored_view_entry(
                    parent,
                    name,
                    root_attr.kind,
                    mode,
                    Some(&root_attr),
                    view_size,
                    Some(backing),
                )))
            }
            ChildKind::WindowsSidecar(receipt) => {
                let root_attr = self.getattr_unchecked(entry.inode)?;
                let total_size = if root_attr.kind == FileType::Directory {
                    self.trash_directory_bytes_total(entry.inode)?
                } else {
                    root_attr.size
                };
                let Some(bytes) = self.sidecar_bytes(&entry, total_size)? else {
                    return Ok(None);
                };
                let len = bytes.len() as u64;
                Ok(Some(self.upsert_ignored_view_entry(
                    parent,
                    name,
                    FileType::RegularFile,
                    0o444,
                    None,
                    len,
                    Some(TrashViewBacking::Bytes {
                        receipt,
                        data: bytes,
                    }),
                )))
            }
            ChildKind::LinuxTrashInfo(receipt) => {
                let Some(bytes) = self.trashinfo_bytes(&entry)? else {
                    return Ok(None);
                };
                let len = bytes.len() as u64;
                Ok(Some(self.upsert_ignored_view_entry(
                    parent,
                    name,
                    FileType::RegularFile,
                    0o444,
                    None,
                    len,
                    Some(TrashViewBacking::Bytes {
                        receipt,
                        data: bytes,
                    }),
                )))
            }
        }
    }

    /// Attributes of a child inside a retained directory view or nested subdir.
    pub(super) fn trash_subtree_child_attr(
        &self,
        parent_view_ino: Ino,
        name: &[u8],
    ) -> VfsResult<Option<InodeAttr>> {
        let Some((receipt, source_dir_ino, _)) = self.retained_view_subtree_dir(parent_view_ino)?
        else {
            return Ok(None);
        };
        let meta = self.sync_meta();
        let child_entry = match meta.lookup(source_dir_ino, name) {
            Ok(entry) => entry,
            Err(MetaError::EntryNotFound { .. }) => {
                self.unlink_ignored_entry(parent_view_ino, name);
                return Ok(None);
            }
            Err(err) => return Err(err.into()),
        };
        let child_attr = self.getattr_unchecked(child_entry.ino)?;
        let (mode, view_size) = self.trash_view_mode_and_size(&child_attr, child_attr.size);
        let backing = TrashViewBacking::Subtree {
            receipt,
            source_ino: child_entry.ino,
            parent_source_ino: source_dir_ino,
        };
        Ok(Some(self.upsert_ignored_view_entry(
            parent_view_ino,
            name,
            child_attr.kind,
            mode,
            Some(&child_attr),
            view_size,
            Some(backing),
        )))
    }

    /// Directory listing inside a retained directory view or nested subdir.
    pub(super) fn trash_subtree_listing(
        &self,
        dir_view_ino: Ino,
    ) -> VfsResult<Option<TrashListingEntries>> {
        let Some((receipt, source_dir_ino, _)) = self.retained_view_subtree_dir(dir_view_ino)?
        else {
            return Ok(None);
        };
        let meta = self.sync_meta();
        let mut listing = Vec::new();
        for child in meta.readdir(source_dir_ino)? {
            if child.name == b"." || child.name == b".." {
                continue;
            }
            let child_attr = self.getattr_unchecked(child.ino)?;
            let (mode, view_size) = self.trash_view_mode_and_size(&child_attr, child_attr.size);
            let backing = TrashViewBacking::Subtree {
                receipt,
                source_ino: child.ino,
                parent_source_ino: source_dir_ino,
            };
            let view_attr = self.upsert_ignored_view_entry(
                dir_view_ino,
                &child.name,
                child_attr.kind,
                mode,
                Some(&child_attr),
                view_size,
                Some(backing),
            );
            listing.push((child.name, view_attr));
        }
        Ok(Some(listing))
    }

    /// Permanently deletes one non-directory child of a retained directory view
    /// (native recursive Clear). Like a local-disk delete, each child is gone on
    /// success even if the rest of the Clear is cancelled or the process dies.
    pub(super) async fn trash_subtree_unlink(
        &self,
        parent_view_ino: Ino,
        name: &[u8],
    ) -> VfsResult<bool> {
        self.trash_subtree_delete(parent_view_ino, name, false)
            .await
    }

    /// Permanently deletes one empty nested directory of a retained directory view.
    pub(super) async fn trash_subtree_rmdir(
        &self,
        parent_view_ino: Ino,
        name: &[u8],
    ) -> VfsResult<bool> {
        self.trash_subtree_delete(parent_view_ino, name, true).await
    }

    /// Reuses the receipt-fenced metadata delete and ordinary unlink cleanup.
    async fn trash_subtree_delete(
        &self,
        parent_view_ino: Ino,
        name: &[u8],
        is_dir: bool,
    ) -> VfsResult<bool> {
        let Some((receipt, source_dir_ino, original_parent)) =
            self.retained_view_subtree_dir(parent_view_ino)?
        else {
            return Ok(false);
        };
        self.enforce_public_access(original_parent, AccessLevel::ReadWrite)?;
        let meta = self.sync_meta();
        let store = meta
            .retained_store()
            .ok_or(VfsError::OperationNotSupported)?;
        let child = meta.lookup(source_dir_ino, name).map_err(|err| match err {
            MetaError::EntryNotFound { .. } => Self::ignored_entry_not_found(parent_view_ino, name),
            err => VfsError::Meta(err),
        })?;
        let view_ino = self
            .ignored_entry_lookup(parent_view_ino, name)
            .map_or(child.ino, |ignored| ignored.attr.ino);
        match (is_dir, child.kind == FileType::Directory) {
            (false, true) => return Err(VfsError::Meta(MetaError::IsADirectory(view_ino))),
            (true, false) => return Err(VfsError::Meta(MetaError::NotADirectory(view_ino))),
            _ => {}
        }
        let delete_session = self
            .state
            .file_session
            .enter_op(child.ino, FileSessionOperation::Delete)
            .await;
        let result = async {
            let open_handles = self.handle_table.open_count(child.ino);
            let outcome = store
                .purge_retained_child(receipt, source_dir_ino, name, child.ino, is_dir)
                .map_err(|err| match err {
                    MetaError::DirNotEmpty(_) => VfsError::Meta(MetaError::DirNotEmpty(view_ino)),
                    err => VfsError::Meta(err),
                })?;
            let removed = match outcome {
                RetainedChildPurgeOutcome::Removed(attr) => attr,
                RetainedChildPurgeOutcome::TargetChanged => {
                    return Err(Self::ignored_entry_not_found(parent_view_ino, name));
                }
            };
            if let Some(attr) = removed.filter(|attr| attr.kind != FileType::Directory) {
                self.record_committed_link_change(attr.ino, attr.nlink, Some(attr.ctime));
                self.invalidate_attr_read_cache([attr.ino]);
                if attr.nlink > 0 {
                    // An outside hard link keeps the inode live.
                } else if open_handles > 0 {
                    // No upload tombstone: the purge can still lose, and the last
                    // close marks the inode deleted only once metadata removes it.
                    self.retire_os_cache_inode(attr.ino);
                    self.handle_table.mark_orphan(attr.ino);
                } else {
                    self.retire_os_cache_inode(attr.ino);
                    // A just-journaled purge is unacknowledged on a replicated
                    // volume, so the guard would only scan the journal and defer;
                    // the lifecycle sweep finalizes it after acknowledgement.
                    let unacknowledged = meta
                        .local_outbound_ack_snapshot()
                        .is_some_and(|ack| !ack.is_acknowledged());
                    if !unacknowledged
                        && let Some(cleanup) =
                            self.collect_orphan_deleted_inode_cleanup(attr.ino).await?
                    {
                        self.finalize_deleted_inode_runtime_cleanup(attr.ino, cleanup.removed_ids)
                            .await;
                    }
                }
            }
            Ok(true)
        }
        .await;
        drop(delete_session);
        self.state.file_session.gc_if_idle(child.ino);
        if result.is_ok() {
            if is_dir {
                self.unlink_ignored_subtree(parent_view_ino, name);
            } else {
                self.unlink_ignored_entry(parent_view_ino, name);
            }
            self.bump_directory_generation(parent_view_ino);
            // The sidecar size reflects the surviving tree.
            self.bump_trash_view_generations(receipt);
        }
        result
    }

    /// Reads a retained payload or nested subtree view using a prevalidated
    /// snapshot. Public paths never reach the payload itself; this seam is the
    /// only reader.
    pub(super) async fn read_trash_view_snapshot(
        &self,
        ignored: &super::ignored_entries::IgnoredEntry,
        receipt: Ino,
        offset: u64,
        buffer: &mut [u8],
    ) -> VfsResult<u32> {
        let view = ignored.attr.ino;
        let source_ino = match &ignored.view {
            Some(TrashViewBacking::Payload { receipt: r }) if *r == receipt => {
                if self.trash_view_receipt_for(ignored.parent, &ignored.name)? != Some(receipt) {
                    return Err(VfsError::Meta(MetaError::InodeNotFound(view)));
                }
                None
            }
            Some(TrashViewBacking::Subtree {
                receipt: r,
                source_ino,
                parent_source_ino,
            }) if *r == receipt => {
                if !self.subtree_view_child_is_live(*parent_source_ino, &ignored.name, *source_ino)
                    || self
                        .trash_view_receipt_access_by_receipt(receipt)?
                        .is_none()
                {
                    return Err(VfsError::Meta(MetaError::InodeNotFound(view)));
                }
                Some(*source_ino)
            }
            _ => return Err(VfsError::Meta(MetaError::InodeNotFound(view))),
        };
        let meta = self.sync_meta();
        let store = meta
            .retained_store()
            .ok_or(VfsError::OperationNotSupported)?;
        let entry = store
            .retained_entry(receipt)?
            .ok_or(VfsError::Meta(MetaError::InodeNotFound(receipt)))?;
        self.enforce_access_async(entry.original_parent, AccessLevel::ReadOnly)
            .await?;
        if ignored.attr.kind == FileType::Directory {
            return Err(VfsError::Meta(MetaError::IsADirectory(view)));
        }
        let target_ino = source_ino.unwrap_or(entry.inode);
        trusted_retained_scope(self.read_into_impl(None, target_ino, offset, buffer)).await
    }

    fn original_path_parts(&self, entry: &RetainedEntry) -> VfsResult<Option<Vec<Vec<u8>>>> {
        let meta = self.sync_meta();
        let mut parts = vec![entry.original_name.clone()];
        let mut current = entry.original_parent;
        for _ in 0..256 {
            if current == ROOT_INO {
                break;
            }
            let Some((parent, name)) = meta.entry_bindings_for_inode(current)?.into_iter().next()
            else {
                return Ok(None);
            };
            parts.push(name);
            current = parent;
        }
        if current != ROOT_INO {
            return Ok(None);
        }
        parts.reverse();
        Ok(Some(parts))
    }

    /// `$I` version 2 record. The original path is rebuilt from the original
    /// parent's current binding chain, so a moved parent keeps its identity and
    /// a recreated path does not.
    fn sidecar_bytes(&self, entry: &RetainedEntry, size: u64) -> VfsResult<Option<Vec<u8>>> {
        let Some(parts) = self.original_path_parts(entry)? else {
            return Ok(None);
        };
        let prefix = self.state.metrics.native_trash_prefix.lock().clone();
        let path = format!("{prefix}\\{}", String::from_utf8_lossy(&parts.join(&b'\\')));
        let units: Vec<u16> = path.encode_utf16().chain(std::iter::once(0)).collect();
        let filetime = entry
            .deleted_at_unix_secs
            .saturating_add(11_644_473_600)
            .saturating_mul(10_000_000);
        let mut out = Vec::with_capacity(24 + 4 + units.len() * 2);
        out.extend_from_slice(&2u64.to_le_bytes());
        out.extend_from_slice(&size.to_le_bytes());
        out.extend_from_slice(&filetime.to_le_bytes());
        out.extend_from_slice(&(units.len() as u32).to_le_bytes());
        for unit in units {
            out.extend_from_slice(&unit.to_le_bytes());
        }
        Ok(Some(out))
    }

    /// Freedesktop `.trashinfo` record derived from the shared receipt.
    fn trashinfo_bytes(&self, entry: &RetainedEntry) -> VfsResult<Option<Vec<u8>>> {
        let Some(parts) = self.original_path_parts(entry)? else {
            return Ok(None);
        };
        let path = percent_encode_trashinfo_path(&parts);
        let secs = entry.deleted_at_unix_secs.min(i64::MAX as u64) as i64;
        let dt = chrono::DateTime::from_timestamp(secs, 0)
            .unwrap_or(chrono::DateTime::UNIX_EPOCH)
            .with_timezone(&chrono::Local)
            .format("%Y-%m-%dT%H:%M:%S");
        Ok(Some(
            format!("[Trash Info]\nPath={path}\nDeletionDate={dt}\n").into_bytes(),
        ))
    }

    /// Draft sidecars written under Windows `$RECYCLE.BIN` (`$I*`) or Linux
    /// `.Trash-<uid>/info` (`*.trashinfo`) stay local: they never create a
    /// published entry and never become authority.
    pub(super) fn is_trash_sidecar_draft(&self, parent: Ino, name: &[u8]) -> bool {
        match self.trash_container_role(parent) {
            Ok(Some(TrashContainerRole::WindowsRoot(_))) => {
                name.len() >= 2 && name[..2].eq_ignore_ascii_case(b"$I")
            }
            // GIO atomically writes sidecars through temporary names in this directory.
            Ok(Some(TrashContainerRole::LinuxInfo(_))) => true,
            _ => false,
        }
    }

    pub(super) fn is_macos_trash_metadata_entry(&self, parent: Ino, name: &[u8]) -> bool {
        name == b".DS_Store"
            && matches!(
                self.trash_container_role(parent),
                Ok(Some(TrashContainerRole::MacOsUser(_)))
            )
    }

    pub(super) fn is_active_retained_sidecar_name(
        &self,
        parent: Ino,
        name: &[u8],
    ) -> VfsResult<bool> {
        match self.trash_container_role(parent)? {
            Some(TrashContainerRole::LinuxInfo(_)) => {
                let Some(receipt) = self.linux_receipt_for_sidecar_name(name)? else {
                    return Ok(false);
                };
                Ok(self
                    .trash_view_receipt_access_by_receipt(receipt)?
                    .is_some())
            }
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod guard_tests;

#[cfg(test)]
mod native_tests;

#[cfg(test)]
mod tests {
    use super::super::tests::make_unstarted_test_vfs;
    use super::*;
    use studiofs_meta::schema::ROOT_INO as META_ROOT;
    use studiofs_meta::trash::MetaVolumeSettings;
    use studiofs_meta::{MetaFormat, MetaFsOps, MetaInodes, MetaNamespace};

    fn store_ino(meta: &studiofs_meta::RedbMetaEngine) -> Ino {
        meta.lookup(META_ROOT, PRIVATE_STORE_NAME).unwrap().ino
    }

    #[test]
    fn trash_view_name_round_trips_only_canonical_spelling() {
        let name = trash_view_name(0x2A);
        assert_eq!(name, "$R00002A");
        assert_eq!(trash_view_receipt(name.as_bytes()), Some(0x2A));
        for bad in [
            &b"$R00002a"[..],
            b"$R2A",
            b"R00002A",
            b"$R",
            b"$R+2A",
            b"$R0000002A0",
        ] {
            assert_eq!(trash_view_receipt(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn private_store_is_hidden_from_root_listing_lookup_and_rename() {
        let tmp = studiofs_test_support::workspace_target_tempdir("retained-store-hidden-");
        let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-store-hidden");
        meta.create_file(META_ROOT, b"source", 0, 0, 0o644).unwrap();
        let store = vfs
            .ensure_root_directory(PRIVATE_STORE_NAME, 0o700)
            .unwrap();
        meta.create_file(store, b"probe", 0, 0, 0o600).unwrap();

        assert!(vfs.lookup(ROOT_INO, PRIVATE_STORE_NAME).is_err());
        assert!(
            vfs.readdir(ROOT_INO)
                .unwrap()
                .iter()
                .all(|entry| entry.name != PRIVATE_STORE_NAME)
        );
        assert!(vfs.lookup(store, b"probe").is_err());
        assert!(vfs.readdir(store).is_err());
        assert!(
            vfs.rename(ROOT_INO, b"source", store, b"moved", false)
                .is_err()
        );
        assert!(meta.lookup(META_ROOT, b"source").is_ok());
        assert_eq!(store_ino(&meta), store);
    }

    #[test]
    fn trash_rename_is_destructive_when_disabled_and_retained_when_enabled() {
        let tmp = studiofs_test_support::workspace_target_tempdir("retained-trash-rename-");
        let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-trash-rename");
        meta.create_file(META_ROOT, b"source", 0, 0, 0o644).unwrap();
        let trash = meta
            .create_dir(META_ROOT, TRASH_ROOT_NAME, 0, 0, 0o755)
            .unwrap();

        // Period zero is a direct delete: the source is gone and nothing is retained.
        vfs.rename(ROOT_INO, b"source", trash.ino, b"$R000001", false)
            .unwrap();
        assert!(meta.lookup(META_ROOT, b"source").is_err());
        assert!(
            meta.lookup(META_ROOT, PRIVATE_STORE_NAME).is_err(),
            "period zero never creates the retained store"
        );

        // A trash name that already exists is still a collision at period zero.
        meta.create_file(META_ROOT, b"again", 0, 0, 0o644).unwrap();
        meta.create_file(trash.ino, b"$R000002", 0, 0, 0o644)
            .unwrap();
        assert!(matches!(
            vfs.rename(ROOT_INO, b"again", trash.ino, b"$R000002", false),
            Err(VfsError::Meta(MetaError::EntryExists { .. }))
        ));
        assert!(meta.lookup(META_ROOT, b"again").is_ok());

        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        meta.create_file(META_ROOT, b"source", 0, 0, 0o644).unwrap();
        vfs.rename(ROOT_INO, b"source", trash.ino, b"$R000001", false)
            .unwrap();

        assert!(meta.lookup(META_ROOT, b"source").is_err());
        let receipt = meta.readdir(store_ino(&meta)).unwrap()[0].ino;
        let view = trash_view_name(receipt);
        assert_eq!(
            vfs.trash_view_receipt_for(trash.ino, view.as_bytes())
                .unwrap(),
            Some(receipt)
        );
        let entry = meta.retained_entry(receipt).unwrap().unwrap();
        assert_eq!(entry.state, RetainedEntryState::Retained);
        assert_eq!(entry.original_parent, META_ROOT);
        assert_eq!(entry.original_name, b"source");
    }

    /// Builds one payload-backed source retained through the VFS rename path.
    fn retained_source_in_trash(
        vfs: &StudioVfs,
        meta: &studiofs_meta::RedbMetaEngine,
        rt: &tokio::runtime::Runtime,
    ) -> (Ino, Ino, String) {
        // Every VFS call runs inside one block_on, as the existing restore tests do.
        rt.block_on(async {
            meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
            let source = vfs.create(ROOT_INO, b"source", 0, 0, 0o644).unwrap();
            vfs.write(source.ino, 0, b"orig").await.unwrap();
            vfs.fsync(source.ino, false).await.unwrap();
            let trash = vfs.ensure_root_directory(TRASH_ROOT_NAME, 0o755).unwrap();
            let store = vfs
                .ensure_root_directory(PRIVATE_STORE_NAME, 0o700)
                .unwrap();
            let receipt = meta.next_inode().unwrap();
            meta.retain_namespace_entry(ROOT_INO, b"source", store, receipt)
                .unwrap();
            (trash, receipt, trash_view_name(receipt))
        })
    }

    #[test]
    fn trash_view_restore_moves_entry_back_and_hides_its_view() {
        let tmp = studiofs_test_support::workspace_target_tempdir("retained-trash-restore-");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-trash-restore");
        let (trash, receipt, view) = retained_source_in_trash(&vfs, &meta, &rt);

        vfs.rename(trash, view.as_bytes(), ROOT_INO, b"restored", false)
            .unwrap();
        assert!(meta.lookup(META_ROOT, b"restored").is_ok());
        assert_eq!(
            meta.retained_entry(receipt).unwrap().unwrap().state,
            RetainedEntryState::Restored {
                parent: META_ROOT,
                name: b"restored".to_vec(),
            }
        );
        assert_eq!(
            vfs.trash_view_receipt_for(trash, view.as_bytes()).unwrap(),
            None
        );
    }

    /// A collision refusal keeps the entry retained, so the same view can retry
    /// once the destination is free.
    #[test]
    fn trash_view_restore_retries_after_occupied_destination_refusal() {
        let tmp = studiofs_test_support::workspace_target_tempdir("retained-trash-retry-");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-trash-retry");
        let (trash, receipt, view) = retained_source_in_trash(&vfs, &meta, &rt);
        vfs.create(ROOT_INO, b"occupied", 0, 0, 0o644).unwrap();

        assert!(
            vfs.rename(trash, view.as_bytes(), ROOT_INO, b"occupied", false)
                .is_err()
        );
        assert_eq!(
            meta.retained_entry(receipt).unwrap().unwrap().state,
            RetainedEntryState::Retained
        );
        vfs.rename(trash, view.as_bytes(), ROOT_INO, b"restored", false)
            .unwrap();
    }

    #[test]
    fn trash_view_unlink_requests_purge_and_hides_the_name() {
        let tmp = studiofs_test_support::workspace_target_tempdir("retained-trash-purge-");
        let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-trash-purge");
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        meta.create_file(META_ROOT, b"source", 0, 0, 0o644).unwrap();
        let trash = meta
            .create_dir(META_ROOT, TRASH_ROOT_NAME, 0, 0, 0o755)
            .unwrap();
        vfs.rename(ROOT_INO, b"source", trash.ino, b"$R000001", false)
            .unwrap();
        let receipt = meta.readdir(store_ino(&meta)).unwrap()[0].ino;
        let view = trash_view_name(receipt);

        vfs.unlink(trash.ino, view.as_bytes()).unwrap();

        assert_eq!(
            meta.retained_entry(receipt).unwrap().unwrap().state,
            RetainedEntryState::PurgeRequested {
                reason: RetainedPurgeReason::User,
            }
        );
        assert_eq!(
            vfs.trash_view_receipt_for(trash.ino, view.as_bytes())
                .unwrap(),
            None
        );
    }

    #[test]
    fn first_root_readdir_includes_trash_root_and_bumps_cache_token() {
        let tmp = studiofs_test_support::workspace_target_tempdir("retained-first-root-");
        let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-first-root");
        // Seed a negative root lookup and capture root cache token while trash is disabled.
        assert!(vfs.lookup(ROOT_INO, TRASH_ROOT_NAME).is_err());
        assert_eq!(
            vfs.dentry_cache_lookup(ROOT_INO, TRASH_ROOT_NAME),
            Some(super::super::DentryCacheValue::Negative)
        );
        let before = vfs.namespace_cache_token_for_parent(ROOT_INO);

        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        let names: Vec<_> = vfs
            .readdir(ROOT_INO)
            .unwrap()
            .into_iter()
            .map(|entry| entry.name)
            .collect();
        assert!(
            names.iter().any(|name| name == TRASH_ROOT_NAME),
            "first readdir(ROOT_INO) after enabling trash must include $RECYCLE.BIN, got {names:?}"
        );
        let after = vfs.namespace_cache_token_for_parent(ROOT_INO);
        assert_ne!(after, before, "root cache token must advance");
        let trash = meta.lookup(ROOT_INO, TRASH_ROOT_NAME).unwrap().ino;
        assert_eq!(
            vfs.dentry_cache_lookup(ROOT_INO, TRASH_ROOT_NAME),
            Some(super::super::DentryCacheValue::Positive(trash)),
            "readdir must replace the real negative cache entry with the created root"
        );
        assert!(vfs.lookup(ROOT_INO, TRASH_ROOT_NAME).is_ok());
    }

    #[test]
    fn read_only_root_views_do_not_create_missing_trash_root() {
        let tmp = studiofs_test_support::workspace_target_tempdir("retained-read-only-root-");
        let (rt, _writable_vfs, meta) = make_unstarted_test_vfs(&tmp, "retained-read-only-root");
        meta.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        meta.create_file(META_ROOT, b"visible.txt", 0, 0, 0o644)
            .unwrap();

        let data_config = studiofs_data::store::DataStoreConfig::from(
            &meta.get_format_info().expect("format info"),
        );
        let cache_dir = tmp.path().join("read-only-cache");
        let read_only_vfs = {
            let _guard = rt.enter();
            StudioVfs::new_read_only_with_snapshot_mount_fence(
                meta.clone(),
                &data_config,
                cache_dir.to_str().expect("cache dir"),
                64 * 1024 * 1024,
                32 * 1024 * 1024,
                super::super::VfsConfig::default()
                    .try_into()
                    .expect("valid VFS config"),
                std::sync::Arc::new(|_| Ok(())),
            )
            .expect("read-only VFS")
        };

        let version_before = read_only_vfs.meta().metadata_version_token();
        let next_ino = meta.next_inode().expect("allocator baseline");
        let entries = read_only_vfs
            .readdir(ROOT_INO)
            .expect("read-only root listing");
        assert!(entries.iter().any(|entry| entry.name == b"visible.txt"));
        assert!(entries.iter().all(|entry| entry.name != TRASH_ROOT_NAME));
        assert!(matches!(
            read_only_vfs.lookup(ROOT_INO, TRASH_ROOT_NAME),
            Err(VfsError::Meta(MetaError::EntryNotFound { .. }))
        ));
        assert!(meta.lookup(META_ROOT, TRASH_ROOT_NAME).is_err());
        assert_eq!(
            meta.next_inode().expect("allocator after reads"),
            next_ino + 1,
            "read-only root views must not reserve inode IDs"
        );
        assert_eq!(
            read_only_vfs.meta().metadata_version_token(),
            version_before,
            "read-only root views must not journal namespace changes"
        );

        meta.create_dir(META_ROOT, TRASH_ROOT_NAME, 0, 0, 0o755)
            .expect("seed existing trash root");
        assert!(read_only_vfs.lookup(ROOT_INO, TRASH_ROOT_NAME).is_ok());
        assert!(
            read_only_vfs
                .readdir(ROOT_INO)
                .unwrap()
                .iter()
                .any(|entry| entry.name == TRASH_ROOT_NAME)
        );
    }
}
