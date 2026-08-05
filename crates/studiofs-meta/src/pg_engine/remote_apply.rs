use crate::engine::{MetaError, MetaInodes, MetaNamespace, MetaPayloadObjects, MetaResult};
use crate::events::{MetaEvent, decode_event_with_type, encode_event};
use crate::redb_engine::RedbMetaEngine;
use crate::redb_engine::layout::{encode_acl_rule_key, encode_dentry_key, encode_xattr_key};
use crate::schema::{DEFAULT_CHUNK_SIZE, chunk_start_offset};
use crate::types::*;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Default)]
pub(super) struct OverlayConflictSets {
    pub(super) block_inodes: HashSet<Ino>,
    pub(super) attr_inodes: HashSet<Ino>,
    pub(super) remove_attr_inodes: HashSet<Ino>,
    pub(super) dentry_keys: HashSet<Vec<u8>>,
    pub(super) symlink_inodes: HashSet<Ino>,
    pub(super) xattr_inodes: HashSet<Ino>,
    pub(super) xattr_keys: HashSet<Vec<u8>>,
    pub(super) acl_keys: HashSet<Vec<u8>>,
}

pub(super) struct RecentFlush {
    pub(super) batch_version: i64,
    pub(super) overlay: OverlayConflictSets,
}

pub(super) struct DecodedReplayBatch {
    pub(super) batch_version: i64,
    pub(super) events: Vec<MetaEvent>,
    pub(super) is_self_batch: bool,
}

pub(super) struct RemoteApplyBatchOutcome {
    pub(super) changed: bool,
    pub(super) protected_overlay: OverlayConflictSets,
}

pub(super) struct RemoteReplayApplyOutcome {
    pub(super) changed: bool,
    pub(super) max_version: i64,
    pub(super) max_namespace_version: i64,
    pub(super) flushed_event_seq: Option<u64>,
}

pub(super) struct RemoteReplayApplyContext {
    pub(super) last_seq: i64,
    pub(super) current_namespace_version: i64,
    pub(super) flushed_event_seq: u64,
    pub(super) next_event_seq: u64,
    pub(super) initial_overlay: OverlayConflictSets,
}

pub(super) struct ProjectedRemoteBatch {
    pub(super) events: Vec<MetaEvent>,
    pub(super) visible_ops: u32,
    pub(super) needs_eager_flush: bool,
    pub(super) needs_local_replay: bool,
    pub(super) overlay: OverlayConflictSets,
}

pub(super) struct RemoteApplyCoordinator<'a> {
    pub(super) local: &'a RedbMetaEngine,
}

impl<'a> RemoteApplyCoordinator<'a> {
    pub(super) fn new(local: &'a RedbMetaEngine) -> Self {
        Self { local }
    }

    #[cfg(test)]
    pub(super) fn project_remote_events(events: &[MetaEvent]) -> Vec<MetaEvent> {
        events
            .iter()
            .filter_map(Self::project_remote_event)
            .collect()
    }

    fn project_remote_event(event: &MetaEvent) -> Option<MetaEvent> {
        match event {
            MetaEvent::SetRegularFileOverlaySlices { .. }
            | MetaEvent::RemoveRegularFileGraph { .. }
            | MetaEvent::AddPendingDeletion { .. }
            | MetaEvent::RemovePendingDeletion { .. } => None,
            MetaEvent::CreateSnapshot { snapshot } => {
                let mut synced = snapshot.clone();
                synced.state = SnapshotState::Synced;
                Some(MetaEvent::CreateSnapshot { snapshot: synced })
            }
            MetaEvent::SetAttr { attr } if attr.kind == FileType::RegularFile => {
                let mut remote_attr = attr.clone();
                remote_attr.size = remote_attr.committed_size;
                Some(MetaEvent::SetAttr { attr: remote_attr })
            }
            _ => Some(event.clone()),
        }
    }

    pub(super) fn project_local_batch(events: &[MetaEvent]) -> ProjectedRemoteBatch {
        let remote_events = Self::project_local_events(events);
        let visible_ops = u32::try_from(remote_events.len()).unwrap_or(u32::MAX);
        let needs_eager_flush = Self::local_events_need_eager_flush(&remote_events);
        let needs_local_replay = Self::local_events_need_serving_replay(events);
        let overlay = Self::overlay_conflicts_for_events(&remote_events);
        ProjectedRemoteBatch {
            events: remote_events,
            visible_ops,
            needs_eager_flush,
            needs_local_replay,
            overlay,
        }
    }

    fn hidden_regular_inodes_for_local_events(events: &[MetaEvent]) -> HashSet<Ino> {
        let committed_regular_inodes = events
            .iter()
            .filter_map(|event| match event {
                MetaEvent::SetAttr { attr }
                    if attr.kind == FileType::RegularFile && attr.committed_content_epoch > 0 =>
                {
                    Some(attr.ino)
                }
                _ => None,
            })
            .collect::<HashSet<_>>();
        let mut hidden_regular_inodes = HashSet::new();
        let mut published_regular_inodes = HashSet::new();
        for event in events {
            match event {
                MetaEvent::SetAttr { attr }
                    if attr.kind == FileType::RegularFile
                        && !committed_regular_inodes.contains(&attr.ino)
                        && !Self::regular_file_attr_is_ready_for_serving(attr) =>
                {
                    hidden_regular_inodes.insert(attr.ino);
                }
                MetaEvent::SetAttr { attr } if attr.is_unpublished_empty_draft() => {
                    hidden_regular_inodes.insert(attr.ino);
                }
                MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices }
                    if !committed_regular_inodes.contains(ino)
                        && chunk_slices.iter().any(|(_, slices)| {
                            slices
                                .iter()
                                .any(|slice| slice.payload_state != PayloadRefState::Published)
                        }) =>
                {
                    hidden_regular_inodes.insert(*ino);
                }
                MetaEvent::PublishRegularFileBaseSlices { ino, chunk_slices }
                    if chunk_slices.is_empty()
                        || chunk_slices.iter().any(|(_, slices)| {
                            slices.iter().any(|slice| {
                                slice.payload_state == PayloadRefState::Published
                                    && slice.payload.is_some()
                            })
                        }) =>
                {
                    published_regular_inodes.insert(*ino);
                }
                _ => {}
            }
        }
        for ino in published_regular_inodes {
            hidden_regular_inodes.remove(&ino);
        }
        hidden_regular_inodes
    }

    fn local_events_need_serving_replay(events: &[MetaEvent]) -> bool {
        let hidden_regular_inodes = Self::hidden_regular_inodes_for_local_events(events);
        let deleted_hidden_regular_inodes = events
            .iter()
            .filter_map(|event| match event {
                MetaEvent::RemoveAttr { ino } if hidden_regular_inodes.contains(ino) => Some(*ino),
                _ => None,
            })
            .collect::<HashSet<_>>();
        events.iter().any(|event| match event {
            MetaEvent::AddEntry { entry, .. } => {
                entry.kind == FileType::RegularFile
                    && hidden_regular_inodes.contains(&entry.ino)
                    && !deleted_hidden_regular_inodes.contains(&entry.ino)
            }
            MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices } => {
                !deleted_hidden_regular_inodes.contains(ino)
                    && chunk_slices.iter().any(|(_, slices)| {
                        slices.iter().any(|slice| {
                            slice.payload.is_some()
                                && slice.payload_state == PayloadRefState::LocalOnly
                        })
                    })
            }
            _ => false,
        })
    }

    fn project_local_events(events: &[MetaEvent]) -> Vec<MetaEvent> {
        let hidden_regular_inodes = Self::hidden_regular_inodes_for_local_events(events);
        let hidden_regular_parent_attr_indices = events
            .windows(2)
            .enumerate()
            .filter_map(|(index, pair)| match pair {
                [
                    MetaEvent::SetAttr { attr },
                    MetaEvent::AddEntry { parent, entry, .. },
                ] if attr.ino == *parent
                    && entry.kind == FileType::RegularFile
                    && hidden_regular_inodes.contains(&entry.ino) =>
                {
                    Some(index)
                }
                _ => None,
            })
            .collect::<HashSet<_>>();
        let hidden_zero_regular_add_inodes = events
            .iter()
            .filter_map(|event| match event {
                MetaEvent::SetAttr { attr }
                    if hidden_regular_inodes.contains(&attr.ino)
                        && attr.is_unpublished_empty_draft() =>
                {
                    Some(attr.ino)
                }
                _ => None,
            })
            .collect::<HashSet<_>>();
        let mut out = Vec::with_capacity(events.len());
        for (index, event) in events.iter().enumerate() {
            match event {
                MetaEvent::AddEntry { entry, .. }
                    if entry.kind == FileType::RegularFile
                        && hidden_regular_inodes.contains(&entry.ino) => {}
                MetaEvent::SetAttr { attr }
                    if hidden_regular_inodes.contains(&attr.ino)
                        || (hidden_zero_regular_add_inodes.contains(&attr.ino)
                            && attr.is_unpublished_empty_draft()) => {}
                MetaEvent::SetAttr { .. }
                    if hidden_regular_parent_attr_indices.contains(&index) => {}
                MetaEvent::RemoveAttr { ino } if hidden_regular_inodes.contains(ino) => {}
                _ => {
                    if let Some(event) = Self::project_remote_event(event) {
                        out.push(event);
                    }
                }
            }
        }
        out
    }

    fn local_events_need_eager_flush(events: &[MetaEvent]) -> bool {
        events.iter().any(|event| match event {
            MetaEvent::SetAttr { attr } if attr.is_unpublished_empty_draft() => false,
            MetaEvent::SetRegularFileOverlaySlices { .. }
            | MetaEvent::RemoveRegularFileGraph { .. }
            | MetaEvent::AddPendingDeletion { .. }
            | MetaEvent::RemovePendingDeletion { .. } => false,
            MetaEvent::SetAttr { attr } if attr.kind == FileType::RegularFile => {
                attr.upload_state == UploadState::Ready
            }
            // Namespace and ordinary attribute changes are remote-visible, but
            // they do not need to bypass the shared PG group-commit window. An
            // explicit request_eager_flush() still provides the immediate
            // publication barrier for callers that require it.
            MetaEvent::SetAttr { .. }
            | MetaEvent::RemoveAttr { .. }
            | MetaEvent::AddEntry { .. }
            | MetaEvent::RemoveEntry { .. }
            | MetaEvent::RenameEntry { .. } => false,
            MetaEvent::PublishRegularFileBaseSlices { .. }
            | MetaEvent::CreateSnapshot { .. }
            | MetaEvent::UpdateSnapshotState { .. } => true,
            MetaEvent::SetSymlinkTarget { .. }
            | MetaEvent::SetXattr { .. }
            | MetaEvent::RemoveXattr { .. }
            | MetaEvent::SetAclRule { .. }
            | MetaEvent::RemoveAclRule { .. } => false,
        })
    }

    pub(super) fn overlay_conflicts_for_events(events: &[MetaEvent]) -> OverlayConflictSets {
        let mut overlay = OverlayConflictSets::default();
        for event in events {
            match event {
                MetaEvent::SetAttr { attr } => {
                    overlay.attr_inodes.insert(attr.ino);
                }
                MetaEvent::RemoveAttr { ino } => {
                    overlay.attr_inodes.insert(*ino);
                    overlay.block_inodes.insert(*ino);
                    overlay.symlink_inodes.insert(*ino);
                    overlay.xattr_inodes.insert(*ino);
                }
                MetaEvent::AddEntry { parent, name, .. }
                | MetaEvent::RemoveEntry { parent, name } => {
                    overlay.dentry_keys.insert(encode_dentry_key(*parent, name));
                }
                MetaEvent::RenameEntry {
                    parent,
                    name,
                    new_parent,
                    new_name,
                    entry,
                    displaced_set_attr,
                    displaced_remove_attr_ino,
                    moved_dir_attr,
                    old_parent_attr,
                    new_parent_attr,
                } => {
                    overlay.dentry_keys.insert(encode_dentry_key(*parent, name));
                    overlay
                        .dentry_keys
                        .insert(encode_dentry_key(*new_parent, new_name));
                    overlay.attr_inodes.insert(entry.ino);
                    if let Some(attr) = displaced_set_attr.as_ref() {
                        overlay.attr_inodes.insert(attr.ino);
                    }
                    if let Some(ino) = displaced_remove_attr_ino {
                        overlay.attr_inodes.insert(*ino);
                    }
                    if let Some(attr) = moved_dir_attr.as_ref() {
                        overlay.attr_inodes.insert(attr.ino);
                    }
                    if let Some(attr) = old_parent_attr.as_ref() {
                        overlay.attr_inodes.insert(attr.ino);
                    }
                    if let Some(attr) = new_parent_attr.as_ref() {
                        overlay.attr_inodes.insert(attr.ino);
                    }
                }
                MetaEvent::SetRegularFileOverlaySlices { ino, .. }
                | MetaEvent::PublishRegularFileBaseSlices { ino, .. }
                | MetaEvent::RemoveRegularFileGraph { ino } => {
                    overlay.block_inodes.insert(*ino);
                }
                MetaEvent::SetSymlinkTarget { ino, .. } => {
                    overlay.symlink_inodes.insert(*ino);
                    overlay.attr_inodes.insert(*ino);
                }
                MetaEvent::SetXattr { ino, name, .. } | MetaEvent::RemoveXattr { ino, name } => {
                    overlay.xattr_inodes.insert(*ino);
                    overlay.xattr_keys.insert(encode_xattr_key(*ino, name));
                }
                MetaEvent::SetAclRule { ino, principal, .. }
                | MetaEvent::RemoveAclRule { ino, principal } => {
                    overlay.attr_inodes.insert(*ino);
                    overlay
                        .acl_keys
                        .insert(encode_acl_rule_key(*ino, principal));
                }
                MetaEvent::AddPendingDeletion { .. }
                | MetaEvent::RemovePendingDeletion { .. }
                | MetaEvent::CreateSnapshot { .. }
                | MetaEvent::UpdateSnapshotState { .. } => {}
            }
        }
        overlay
    }

    pub(super) fn merge_overlay_conflicts(
        base: &OverlayConflictSets,
        extra: &OverlayConflictSets,
    ) -> OverlayConflictSets {
        let mut merged = base.clone();
        merged
            .block_inodes
            .extend(extra.block_inodes.iter().copied());
        merged.attr_inodes.extend(extra.attr_inodes.iter().copied());
        merged
            .remove_attr_inodes
            .extend(extra.remove_attr_inodes.iter().copied());
        merged.dentry_keys.extend(extra.dentry_keys.iter().cloned());
        merged
            .symlink_inodes
            .extend(extra.symlink_inodes.iter().copied());
        merged
            .xattr_inodes
            .extend(extra.xattr_inodes.iter().copied());
        merged.xattr_keys.extend(extra.xattr_keys.iter().cloned());
        merged.acl_keys.extend(extra.acl_keys.iter().cloned());
        merged
    }

    pub(super) fn collect_replay_reconcile_inodes(
        candidates: &mut HashSet<Ino>,
        batch_events: &[MetaEvent],
    ) {
        candidates.extend(Self::regular_file_serving_reconcile_inodes(batch_events));
    }

    pub(super) fn collect_replay_reconcile_dentries(
        candidates: &mut HashSet<Vec<u8>>,
        batch_events: &[MetaEvent],
    ) {
        candidates.extend(Self::overlay_conflicts_for_events(batch_events).dentry_keys);
    }

    pub(super) fn event_has_aux_apply_effect(event: &MetaEvent) -> bool {
        match event {
            MetaEvent::RemoveAttr { .. }
            | MetaEvent::RemoveRegularFileGraph { .. }
            | MetaEvent::PublishRegularFileBaseSlices { .. }
            | MetaEvent::SetSymlinkTarget { .. }
            | MetaEvent::SetXattr { .. }
            | MetaEvent::RemoveXattr { .. }
            | MetaEvent::SetAclRule { .. }
            | MetaEvent::RemoveAclRule { .. }
            | MetaEvent::AddPendingDeletion { .. }
            | MetaEvent::RemovePendingDeletion { .. }
            | MetaEvent::CreateSnapshot { .. }
            | MetaEvent::UpdateSnapshotState { .. } => true,
            MetaEvent::SetAttr { .. }
            | MetaEvent::SetRegularFileOverlaySlices { .. }
            | MetaEvent::AddEntry { .. }
            | MetaEvent::RemoveEntry { .. }
            | MetaEvent::RenameEntry { .. } => false,
        }
    }

    pub(super) fn aux_event_conflicts_with_overlay(
        event: &MetaEvent,
        overlay: &OverlayConflictSets,
        removed_inodes: &HashSet<Ino>,
        removed_inodes_with_local_regular_file_state: &HashSet<Ino>,
    ) -> bool {
        match event {
            MetaEvent::RemoveAttr { ino } => {
                if overlay.remove_attr_inodes.contains(ino) {
                    return true;
                }
                if overlay.block_inodes.contains(ino) {
                    return true;
                }
                if removed_inodes.contains(ino) {
                    return removed_inodes_with_local_regular_file_state.contains(ino);
                }
                overlay.attr_inodes.contains(ino)
                    || overlay.symlink_inodes.contains(ino)
                    || overlay.xattr_inodes.contains(ino)
            }
            MetaEvent::SetRegularFileOverlaySlices { .. } => false,
            MetaEvent::RemoveRegularFileGraph { ino } => {
                if removed_inodes.contains(ino) {
                    return false;
                }
                overlay.block_inodes.contains(ino)
            }
            MetaEvent::PublishRegularFileBaseSlices { ino, .. } => {
                overlay.block_inodes.contains(ino)
            }
            MetaEvent::SetSymlinkTarget { ino, .. } => {
                overlay.block_inodes.contains(ino)
                    || overlay.attr_inodes.contains(ino)
                    || overlay.symlink_inodes.contains(ino)
            }
            MetaEvent::SetXattr { ino, name, .. } | MetaEvent::RemoveXattr { ino, name } => {
                overlay.block_inodes.contains(ino)
                    || overlay.attr_inodes.contains(ino)
                    || overlay.xattr_keys.contains(&encode_xattr_key(*ino, name))
            }
            MetaEvent::SetAclRule { ino, principal, .. }
            | MetaEvent::RemoveAclRule { ino, principal } => {
                overlay.attr_inodes.contains(ino)
                    || overlay
                        .acl_keys
                        .contains(&encode_acl_rule_key(*ino, principal))
            }
            MetaEvent::SetAttr { .. }
            | MetaEvent::AddEntry { .. }
            | MetaEvent::RemoveEntry { .. }
            | MetaEvent::RenameEntry { .. }
            | MetaEvent::AddPendingDeletion { .. }
            | MetaEvent::RemovePendingDeletion { .. }
            | MetaEvent::CreateSnapshot { .. }
            | MetaEvent::UpdateSnapshotState { .. } => false,
        }
    }

    pub(super) fn serving_event_conflicts_with_overlay(
        event: &MetaEvent,
        overlay: &OverlayConflictSets,
        removed_inodes: &HashSet<Ino>,
        removed_inodes_with_local_regular_file_state: &HashSet<Ino>,
    ) -> bool {
        match event {
            MetaEvent::SetAttr { attr } => {
                overlay.block_inodes.contains(&attr.ino) || overlay.attr_inodes.contains(&attr.ino)
            }
            MetaEvent::RemoveAttr { ino } => {
                if overlay.remove_attr_inodes.contains(ino) {
                    return true;
                }
                if overlay.block_inodes.contains(ino) {
                    return true;
                }
                if removed_inodes.contains(ino) {
                    return removed_inodes_with_local_regular_file_state.contains(ino);
                }
                overlay.attr_inodes.contains(ino)
                    || overlay.symlink_inodes.contains(ino)
                    || overlay.xattr_inodes.contains(ino)
            }
            MetaEvent::AddEntry { parent, name, .. } | MetaEvent::RemoveEntry { parent, name } => {
                overlay
                    .dentry_keys
                    .contains(&encode_dentry_key(*parent, name))
            }
            MetaEvent::RenameEntry { .. } => false,
            MetaEvent::SetSymlinkTarget { ino, .. } => {
                overlay.block_inodes.contains(ino)
                    || overlay.attr_inodes.contains(ino)
                    || overlay.symlink_inodes.contains(ino)
            }
            MetaEvent::SetXattr { ino, name, .. } | MetaEvent::RemoveXattr { ino, name } => {
                overlay.block_inodes.contains(ino)
                    || overlay.attr_inodes.contains(ino)
                    || overlay.xattr_keys.contains(&encode_xattr_key(*ino, name))
            }
            MetaEvent::SetAclRule { ino, principal, .. }
            | MetaEvent::RemoveAclRule { ino, principal } => {
                overlay.attr_inodes.contains(ino)
                    || overlay
                        .acl_keys
                        .contains(&encode_acl_rule_key(*ino, principal))
            }
            MetaEvent::SetRegularFileOverlaySlices { .. } => false,
            MetaEvent::PublishRegularFileBaseSlices { ino, .. } => {
                overlay.block_inodes.contains(ino)
            }
            MetaEvent::RemoveRegularFileGraph { .. }
            | MetaEvent::AddPendingDeletion { .. }
            | MetaEvent::RemovePendingDeletion { .. }
            | MetaEvent::CreateSnapshot { .. }
            | MetaEvent::UpdateSnapshotState { .. } => false,
        }
    }

    pub(super) fn regular_file_serving_reconcile_inodes(events: &[MetaEvent]) -> HashSet<Ino> {
        events
            .iter()
            .filter_map(|event| match event {
                MetaEvent::SetAttr { attr } if attr.kind == FileType::RegularFile => Some(attr.ino),
                MetaEvent::PublishRegularFileBaseSlices { ino, .. }
                | MetaEvent::RemoveAttr { ino }
                | MetaEvent::RemoveRegularFileGraph { ino } => Some(*ino),
                _ => None,
            })
            .collect()
    }

    pub(super) fn partial_regular_file_publish_inodes(batch_events: &[MetaEvent]) -> HashSet<Ino> {
        let mut ready_sizes = HashMap::new();
        let mut publish_inodes = HashSet::new();
        for event in batch_events {
            match event {
                MetaEvent::SetAttr { attr }
                    if attr.kind == FileType::RegularFile
                        && attr.upload_state == UploadState::Ready
                        && attr.content_epoch == attr.committed_content_epoch
                        && attr.size == attr.committed_size =>
                {
                    ready_sizes.insert(attr.ino, attr.committed_size);
                }
                MetaEvent::PublishRegularFileBaseSlices { ino, .. } => {
                    publish_inodes.insert(*ino);
                }
                _ => {}
            }
        }

        publish_inodes
            .into_iter()
            .filter(|ino| match ready_sizes.get(ino) {
                Some(committed_size) => !Self::batch_publish_payload_covers_committed_extent(
                    batch_events,
                    *ino,
                    *committed_size,
                ),
                None => true,
            })
            .collect()
    }

    pub(super) fn removed_inodes_for_batch(&self, batch_events: &[MetaEvent]) -> HashSet<Ino> {
        let mut removed = HashSet::new();
        for event in batch_events {
            match event {
                MetaEvent::RemoveEntry { parent, name } => {
                    if let Ok(entry) = self.local.lookup(*parent, name) {
                        removed.insert(entry.ino);
                    }
                }
                MetaEvent::RemoveAttr { ino } => {
                    removed.insert(*ino);
                }
                _ => {}
            }
        }
        removed
    }

    pub(super) fn local_regular_file_state_needs_foreign_protection(
        &self,
        ino: Ino,
    ) -> MetaResult<bool> {
        if self.local.has_local_regular_file_overlay(ino)? {
            return Ok(true);
        }
        let Some(lifecycle) = self.local.get_inode_lifecycle(ino)? else {
            return Ok(false);
        };
        if lifecycle.state == InodeLifecycleState::Published {
            return Ok(false);
        }
        let attr = match self.local.get_attr(ino) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => return Ok(false),
            Err(err) => return Err(err),
        };
        let current_attr_is_committed_base = attr.kind == FileType::RegularFile
            && attr.upload_state == UploadState::Ready
            && attr.size == attr.committed_size
            && attr.content_epoch == attr.committed_content_epoch
            && (lifecycle.has_published_base
                || attr.committed_size > 0
                || attr.committed_content_epoch > 0);
        Ok(!current_attr_is_committed_base)
    }

    pub(super) fn removed_inode_has_local_regular_file_state_to_preserve(
        &self,
        ino: Ino,
    ) -> MetaResult<bool> {
        self.local_regular_file_state_needs_foreign_protection(ino)
    }

    pub(super) fn current_regular_file_overlay_conflicts_for_batch(
        &self,
        batch_events: &[MetaEvent],
    ) -> MetaResult<OverlayConflictSets> {
        let mut overlay = OverlayConflictSets::default();
        let serving_ready_regular_files =
            self.serving_ready_regular_files_for_batch(batch_events)?;
        for event in batch_events {
            let ino = match event {
                MetaEvent::SetAttr { attr } if attr.kind == FileType::RegularFile => attr.ino,
                MetaEvent::AddEntry {
                    parent,
                    name,
                    entry,
                } if entry.kind == FileType::RegularFile
                    && !serving_ready_regular_files.contains(&entry.ino)
                    && self.local.lookup(*parent, name).is_ok() =>
                {
                    overlay.dentry_keys.insert(encode_dentry_key(*parent, name));
                    continue;
                }
                MetaEvent::PublishRegularFileBaseSlices { ino, .. }
                | MetaEvent::RemoveRegularFileGraph { ino } => *ino,
                MetaEvent::RemoveEntry { parent, name } => {
                    let Ok(entry) = self.local.lookup(*parent, name) else {
                        continue;
                    };
                    if self.local_uploading_dentry_needs_foreign_protection(entry.ino)? {
                        overlay.dentry_keys.insert(encode_dentry_key(*parent, name));
                    }
                    entry.ino
                }
                _ => continue,
            };
            if self.local.has_local_regular_file_overlay(ino)? {
                overlay.attr_inodes.insert(ino);
                overlay.block_inodes.insert(ino);
            } else if self.local_regular_file_state_needs_foreign_protection(ino)? {
                overlay.attr_inodes.insert(ino);
            }
        }
        Ok(overlay)
    }

    fn serving_ready_regular_files_for_batch(
        &self,
        batch_events: &[MetaEvent],
    ) -> MetaResult<HashSet<Ino>> {
        let mut ready = HashSet::new();
        for event in batch_events {
            let MetaEvent::SetAttr { attr } = event else {
                continue;
            };
            if Self::regular_file_attr_is_ready_for_serving(attr)
                && (attr.committed_size == 0
                    || Self::batch_publish_covers_committed_extent(
                        batch_events,
                        attr.ino,
                        attr.committed_size,
                    )
                    || self
                        .local
                        .get_base_attr(attr.ino)?
                        .is_some_and(|base_attr| {
                            Self::regular_file_attr_is_ready_for_serving(&base_attr)
                        }))
            {
                ready.insert(attr.ino);
            }
        }
        Ok(ready)
    }

    fn regular_file_attr_is_ready_for_serving(attr: &InodeAttr) -> bool {
        attr.kind == FileType::RegularFile
            && attr.upload_state == UploadState::Ready
            && attr.content_epoch == attr.committed_content_epoch
            && attr.size == attr.committed_size
    }

    pub(super) fn local_uploading_dentry_needs_foreign_protection(
        &self,
        ino: Ino,
    ) -> MetaResult<bool> {
        let attr = match self.local.get_attr(ino) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => return Ok(false),
            Err(err) => return Err(err),
        };
        Ok(attr.kind == FileType::RegularFile
            && attr.upload_state != UploadState::Ready
            && (attr.size <= attr.committed_size || attr.committed_size == 0)
            && self.local.has_local_regular_file_overlay(ino)?)
    }

    pub(super) fn grandfather_removed_local_regular_file_attrs(
        &self,
        inodes: &HashSet<Ino>,
    ) -> MetaResult<Vec<MetaEvent>> {
        let mut events = Vec::new();
        for ino in inodes {
            let mut attr = match self.local.get_attr(*ino) {
                Ok(attr) => attr,
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            };
            if attr.kind != FileType::RegularFile {
                continue;
            }
            attr.upload_state = UploadState::Ready;
            events.push(MetaEvent::SetAttr { attr });
        }
        Ok(events)
    }

    pub(super) fn batch_publish_covers_committed_extent(
        batch_events: &[MetaEvent],
        target_ino: Ino,
        committed_size: u64,
    ) -> bool {
        if committed_size == 0 {
            return false;
        }

        let mut ranges = Vec::new();
        for event in batch_events {
            let MetaEvent::PublishRegularFileBaseSlices { ino, chunk_slices } = event else {
                continue;
            };
            if *ino != target_ino {
                continue;
            }
            for (chunk_id, slices) in chunk_slices {
                let chunk_start = chunk_start_offset(*chunk_id, DEFAULT_CHUNK_SIZE);
                for slice in slices {
                    let start = chunk_start.saturating_add(u64::from(slice.range.start));
                    let end = chunk_start
                        .saturating_add(u64::from(slice.range.end))
                        .min(committed_size);
                    if start < end {
                        ranges.push((start, end));
                    }
                }
            }
        }

        ranges.sort_unstable_by_key(|(start, end)| (*start, *end));
        let mut covered_until = 0;
        for (start, end) in ranges {
            if start > covered_until {
                return false;
            }
            covered_until = covered_until.max(end);
            if covered_until >= committed_size {
                return true;
            }
        }
        false
    }

    fn batch_publish_payload_covers_committed_extent(
        batch_events: &[MetaEvent],
        target_ino: Ino,
        committed_size: u64,
    ) -> bool {
        if committed_size == 0 {
            return false;
        }

        let mut ranges = Vec::new();
        for event in batch_events {
            let MetaEvent::PublishRegularFileBaseSlices { ino, chunk_slices } = event else {
                continue;
            };
            if *ino != target_ino {
                continue;
            }
            for (chunk_id, slices) in chunk_slices {
                let chunk_start = chunk_start_offset(*chunk_id, DEFAULT_CHUNK_SIZE);
                for slice in slices {
                    if slice.payload.is_none() {
                        continue;
                    }
                    let start = chunk_start.saturating_add(u64::from(slice.range.start));
                    let end = chunk_start
                        .saturating_add(u64::from(slice.range.end))
                        .min(committed_size);
                    if start < end {
                        ranges.push((start, end));
                    }
                }
            }
        }

        ranges.sort_unstable_by_key(|(start, end)| (*start, *end));
        let mut covered_until = 0;
        for (start, end) in ranges {
            if start > covered_until {
                return false;
            }
            covered_until = covered_until.max(end);
            if covered_until >= committed_size {
                return true;
            }
        }
        false
    }

    pub(super) fn preserve_published_content_state_for_attr_only_events(
        &self,
        batch_events: &[MetaEvent],
    ) -> MetaResult<Vec<MetaEvent>> {
        let published_inodes: HashSet<Ino> = batch_events
            .iter()
            .filter_map(|event| match event {
                MetaEvent::PublishRegularFileBaseSlices { ino, .. } => Some(*ino),
                _ => None,
            })
            .collect();
        let graph_event_inodes: HashSet<Ino> = batch_events
            .iter()
            .filter_map(|event| match event {
                MetaEvent::SetRegularFileOverlaySlices { ino, .. }
                | MetaEvent::PublishRegularFileBaseSlices { ino, .. } => Some(*ino),
                _ => None,
            })
            .collect();
        if !batch_events.iter().any(|event| {
            matches!(
                event,
                MetaEvent::SetAttr { attr }
                    if attr.kind == FileType::RegularFile
                        && !graph_event_inodes.contains(&attr.ino)
            )
        }) {
            return Ok(batch_events.to_vec());
        }

        batch_events
            .iter()
            .map(|event| {
                let MetaEvent::SetAttr { attr } = event else {
                    return Ok(event.clone());
                };
                if attr.kind != FileType::RegularFile
                    || graph_event_inodes.contains(&attr.ino)
                    || published_inodes.contains(&attr.ino)
                {
                    return Ok(event.clone());
                }
                let Some(base_attr) = self.local.get_base_attr(attr.ino)? else {
                    return Ok(event.clone());
                };
                if base_attr.kind != FileType::RegularFile
                    || base_attr.upload_state != UploadState::Ready
                    || base_attr.content_epoch != base_attr.committed_content_epoch
                    || base_attr.size != base_attr.committed_size
                {
                    return Ok(event.clone());
                }
                if attr.upload_state == UploadState::Ready
                    && (attr.content_epoch > base_attr.content_epoch
                        || attr.committed_size >= base_attr.committed_size)
                {
                    return Ok(event.clone());
                }
                if base_attr.committed_size > 0 {
                    let object_backed = self
                        .local
                        .lookup_regular_file_slices(base_attr.ino, 0, base_attr.committed_size)?
                        .iter()
                        .any(|span| span.payload.is_some());
                    if !object_backed {
                        return Ok(event.clone());
                    }
                }

                let incoming_is_newer_unpublished = attr.upload_state != UploadState::Ready
                    && attr.content_epoch > base_attr.content_epoch;
                let mut merged = if incoming_is_newer_unpublished {
                    attr.clone()
                } else {
                    base_attr.clone()
                };
                merged.committed_size = base_attr.committed_size;
                merged.committed_content_epoch = base_attr.committed_content_epoch;
                if !incoming_is_newer_unpublished {
                    merged.size = base_attr.size;
                    merged.content_epoch = base_attr.content_epoch;
                    merged.upload_state = base_attr.upload_state;
                }
                Ok(MetaEvent::SetAttr { attr: merged })
            })
            .collect()
    }

    pub(super) fn protected_base_range_preserve_inodes(
        &self,
        batch_events: &[MetaEvent],
        protected_regular_file_inodes: &HashSet<Ino>,
    ) -> MetaResult<HashSet<Ino>> {
        let mut preserve_inodes = protected_regular_file_inodes.clone();
        for event in batch_events {
            let MetaEvent::SetAttr { attr } = event else {
                continue;
            };
            if attr.kind != FileType::RegularFile
                || !preserve_inodes.contains(&attr.ino)
                || attr.upload_state != UploadState::Ready
                || attr.content_epoch != attr.committed_content_epoch
                || attr.size != attr.committed_size
            {
                continue;
            }
            let current = match self.local.get_attr(attr.ino) {
                Ok(current) => current,
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            };
            if current.kind != FileType::RegularFile
                || current.upload_state != UploadState::Ready
                || current.content_epoch != current.committed_content_epoch
                || current.size != current.committed_size
            {
                continue;
            }
            let remote_carries_complete_graph = Self::batch_publish_covers_committed_extent(
                batch_events,
                attr.ino,
                attr.committed_size,
            );
            let remote_payload_covers_complete_graph =
                Self::batch_publish_payload_covers_committed_extent(
                    batch_events,
                    attr.ino,
                    attr.committed_size,
                );
            let remote_should_replace_protected_graph = attr.committed_size
                < current.committed_size
                || (remote_carries_complete_graph
                    && remote_payload_covers_complete_graph
                    && (attr.committed_size > current.committed_size
                        || attr.content_epoch > current.content_epoch));
            if remote_should_replace_protected_graph
                || (attr.content_epoch == current.content_epoch
                    && (attr.mtime >= current.mtime || remote_carries_complete_graph))
            {
                tracing::debug!(
                    ino = attr.ino,
                    remote_committed_size = attr.committed_size,
                    current_committed_size = current.committed_size,
                    remote_content_epoch = attr.content_epoch,
                    current_content_epoch = current.content_epoch,
                    remote_mtime = attr.mtime,
                    current_mtime = current.mtime,
                    "ready foreign attr replaces protected committed base graph"
                );
                preserve_inodes.remove(&attr.ino);
            }
        }
        Ok(preserve_inodes)
    }

    pub(super) fn protected_regular_file_attr_merges(
        &self,
        batch_events: &[MetaEvent],
        protected_regular_file_inodes: &HashSet<Ino>,
        base_range_preserve_inodes: &HashSet<Ino>,
    ) -> MetaResult<Vec<MetaEvent>> {
        let mut merged = Vec::new();
        let payload_publish_inodes: HashSet<Ino> = batch_events
            .iter()
            .filter_map(|event| match event {
                MetaEvent::PublishRegularFileBaseSlices { ino, chunk_slices }
                    if chunk_slices
                        .iter()
                        .flat_map(|(_, slices)| slices)
                        .any(|slice| slice.payload.is_some()) =>
                {
                    Some(*ino)
                }
                _ => None,
            })
            .collect();
        for event in batch_events {
            let MetaEvent::SetAttr { attr } = event else {
                continue;
            };
            if attr.kind != FileType::RegularFile
                || !protected_regular_file_inodes.contains(&attr.ino)
            {
                continue;
            }
            let current = match self.local.get_attr(attr.ino) {
                Ok(current) => current,
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            };
            if current.kind != FileType::RegularFile || current.upload_state != UploadState::Ready {
                continue;
            }
            let ready_complete_publish = attr.upload_state == UploadState::Ready
                && attr.content_epoch == attr.committed_content_epoch
                && attr.size == attr.committed_size
                && Self::batch_publish_covers_committed_extent(
                    batch_events,
                    attr.ino,
                    attr.committed_size,
                );
            if ready_complete_publish {
                if base_range_preserve_inodes.contains(&attr.ino) {
                    merged.push(MetaEvent::SetAttr { attr: attr.clone() });
                }
                continue;
            }
            if attr.content_epoch > current.content_epoch
                && !payload_publish_inodes.contains(&attr.ino)
            {
                continue;
            }
            let mut merged_attr = attr.clone();
            merged_attr.size = merged_attr.size.max(current.size);
            merged_attr.committed_size = merged_attr.committed_size.max(current.committed_size);
            merged_attr.content_epoch = merged_attr.content_epoch.max(current.content_epoch);
            merged_attr.committed_content_epoch = merged_attr
                .committed_content_epoch
                .max(current.committed_content_epoch);
            merged_attr.mtime = merged_attr.mtime.max(current.mtime);
            merged_attr.ctime = merged_attr.ctime.max(current.ctime);
            merged.push(MetaEvent::SetAttr { attr: merged_attr });
        }
        Ok(merged)
    }

    pub(super) fn decode_foreign_batch(
        rows: &[tokio_postgres::Row],
        start_idx: usize,
        client_id: &str,
    ) -> MetaResult<(i64, usize, Vec<MetaEvent>, bool)> {
        let batch_version: i64 = rows[start_idx].get(0);
        let batch_client: Option<&str> = rows[start_idx].get(4);
        let is_self_batch = batch_client == Some(client_id);
        let mut idx = start_idx;
        let mut batch_events = Vec::new();

        while idx < rows.len() {
            let row_batch_version: i64 = rows[idx].get(0);
            let row_client: Option<&str> = rows[idx].get(4);
            if row_batch_version != batch_version || row_client != batch_client {
                break;
            }
            let event_type: &str = rows[idx].get(2);
            let payload: &[u8] = rows[idx].get(3);
            let event = decode_event_with_type(event_type, payload)?;
            batch_events.push(event);
            idx += 1;
        }

        Ok((batch_version, idx, batch_events, is_self_batch))
    }

    pub(super) fn apply_foreign_batch(
        &self,
        batch_events: &[MetaEvent],
        overlay: &OverlayConflictSets,
        base_protected_regular_file_inodes: &HashSet<Ino>,
    ) -> MetaResult<RemoteApplyBatchOutcome> {
        let batch_events =
            self.preserve_published_content_state_for_attr_only_events(batch_events)?;
        let current_regular_file_conflicts =
            self.current_regular_file_overlay_conflicts_for_batch(&batch_events)?;
        let overlay = Self::merge_overlay_conflicts(overlay, &current_regular_file_conflicts);
        let mut protected_regular_file_inodes = base_protected_regular_file_inodes.clone();
        protected_regular_file_inodes
            .extend(current_regular_file_conflicts.block_inodes.iter().copied());
        let removed_inodes = self.removed_inodes_for_batch(&batch_events);
        let mut removed_inodes_with_local_regular_file_state = HashSet::new();
        for ino in &removed_inodes {
            if self.removed_inode_has_local_regular_file_state_to_preserve(*ino)? {
                removed_inodes_with_local_regular_file_state.insert(*ino);
            }
        }
        let partial_publish_inodes = Self::partial_regular_file_publish_inodes(&batch_events);
        let mut base_range_preserve_inodes = self
            .protected_base_range_preserve_inodes(&batch_events, &protected_regular_file_inodes)?;
        base_range_preserve_inodes.extend(partial_publish_inodes.iter().copied());
        let protected_attr_merges = self.protected_regular_file_attr_merges(
            &batch_events,
            &protected_regular_file_inodes,
            &base_range_preserve_inodes,
        )?;
        self.local
            .apply_replica_events_to_base_preserving_protected_base_ranges(
                &batch_events,
                &base_range_preserve_inodes,
            )?;
        let serving_events: Vec<MetaEvent> = batch_events
            .iter()
            .filter(|event| {
                if let MetaEvent::SetAttr { attr } = event
                    && attr.kind == FileType::RegularFile
                    && protected_regular_file_inodes.contains(&attr.ino)
                    && !base_range_preserve_inodes.contains(&attr.ino)
                    && attr.upload_state == UploadState::Ready
                    && attr.content_epoch == attr.committed_content_epoch
                    && attr.size == attr.committed_size
                    && Self::batch_publish_covers_committed_extent(
                        &batch_events,
                        attr.ino,
                        attr.committed_size,
                    )
                {
                    return true;
                }
                matches!(
                    event,
                    MetaEvent::PublishRegularFileBaseSlices { ino, .. }
                        if protected_regular_file_inodes.contains(ino)
                ) || !Self::serving_event_conflicts_with_overlay(
                    event,
                    &overlay,
                    &removed_inodes,
                    &removed_inodes_with_local_regular_file_state,
                )
            })
            .cloned()
            .collect();
        let applied_serving_state = !serving_events.is_empty();
        self.local.apply_replica_serving_state(&serving_events)?;
        if !protected_attr_merges.is_empty() {
            self.local
                .apply_replica_serving_state(&protected_attr_merges)?;
        }
        let grandfathered_attrs = self.grandfather_removed_local_regular_file_attrs(
            &removed_inodes_with_local_regular_file_state,
        )?;
        if !grandfathered_attrs.is_empty() {
            self.local
                .apply_replica_serving_state(&grandfathered_attrs)?;
        }
        let mut aux_events = Vec::new();
        for event in &batch_events {
            if !Self::event_has_aux_apply_effect(event) {
                continue;
            }
            if matches!(
                event,
                MetaEvent::PublishRegularFileBaseSlices { ino, .. }
                    if protected_regular_file_inodes.contains(ino)
                        || partial_publish_inodes.contains(ino)
            ) {
                continue;
            }
            if !Self::aux_event_conflicts_with_overlay(
                event,
                &overlay,
                &removed_inodes,
                &removed_inodes_with_local_regular_file_state,
            ) {
                aux_events.push(event.clone());
            }
        }

        if !aux_events.is_empty() {
            self.local.apply_replica_aux_state(&aux_events)?;
        }
        Ok(RemoteApplyBatchOutcome {
            changed: applied_serving_state
                || !aux_events.is_empty()
                || !grandfathered_attrs.is_empty(),
            protected_overlay: current_regular_file_conflicts,
        })
    }

    pub(super) fn apply_visible_self_batch(&self, batch_events: &[MetaEvent]) -> MetaResult<bool> {
        if batch_events.is_empty() {
            return Ok(false);
        }

        let batch_events =
            self.preserve_published_content_state_for_attr_only_events(batch_events)?;
        let partial_publish_inodes = Self::partial_regular_file_publish_inodes(&batch_events);
        if partial_publish_inodes.is_empty() {
            self.local.apply_replica_events_to_base(&batch_events)?;
        } else {
            self.local
                .apply_replica_events_to_base_preserving_protected_base_ranges(
                    &batch_events,
                    &partial_publish_inodes,
                )?;
        }
        for event in &batch_events {
            if let MetaEvent::RenameEntry {
                parent,
                name,
                new_parent,
                new_name,
                entry,
                ..
            } = event
            {
                self.local.reconcile_serving_rename_from_base(
                    *parent,
                    name,
                    *new_parent,
                    new_name,
                    entry,
                )?;
            }
        }
        let aux_events: Vec<MetaEvent> = batch_events
            .iter()
            .filter(|event| {
                Self::event_has_aux_apply_effect(event)
                    && !matches!(
                        event,
                        MetaEvent::PublishRegularFileBaseSlices { ino, .. }
                            if partial_publish_inodes.contains(ino)
                    )
            })
            .cloned()
            .collect();
        if aux_events.is_empty() {
            return Ok(false);
        }

        self.local.apply_replica_aux_state(&aux_events)?;
        Ok(true)
    }

    pub(super) fn overlay_conflicts_for_local_range(
        local: &RedbMetaEngine,
        start: u64,
        end: u64,
    ) -> MetaResult<OverlayConflictSets> {
        if end <= start {
            return Ok(OverlayConflictSets::default());
        }

        let events = local.read_events_range(start, end)?;
        Ok(Self::overlay_conflicts_for_events(&events))
    }

    pub(super) fn acknowledge_local_prefix_from_self_batch(
        &self,
        batch_events: &[MetaEvent],
        flushed_event_seq: u64,
        next_event_seq: u64,
    ) -> MetaResult<Option<u64>> {
        if batch_events.is_empty() {
            return Ok(None);
        }

        let start = flushed_event_seq;
        let end = next_event_seq;
        if end <= start {
            return Ok(None);
        }

        let local_events = self.local.read_events_range(start, end)?;
        for consumed_events in 1..=local_events.len() {
            let projected_prefix = Self::project_local_batch(&local_events[..consumed_events]);
            let matches_prefix = projected_prefix.events.iter().zip(batch_events.iter()).all(
                |(local, remote)| match (encode_event(local), encode_event(remote)) {
                    (Ok(local), Ok(remote)) => local == remote,
                    _ => false,
                },
            );
            if projected_prefix.events.len() == batch_events.len() && matches_prefix {
                let next_seq = start.max(1).saturating_add(consumed_events as u64);
                return Ok(Some(next_seq));
            }
        }

        Ok(None)
    }

    pub(super) fn effective_replay_overlay(
        overlay: &OverlayConflictSets,
        recent_flush: Option<&RecentFlush>,
        batch_version: i64,
    ) -> OverlayConflictSets {
        if let Some(recent_flush) = recent_flush
            && batch_version < recent_flush.batch_version
        {
            return Self::merge_overlay_conflicts(overlay, &recent_flush.overlay);
        }
        overlay.clone()
    }

    pub(super) fn protected_base_inodes_for_replay(
        recent_flush: Option<&RecentFlush>,
    ) -> Option<&HashSet<Ino>> {
        recent_flush.map(|recent_flush| &recent_flush.overlay.block_inodes)
    }

    pub(super) fn reconcile_regular_file_serving_from_base_with_overlay(
        &self,
        inodes: &HashSet<Ino>,
        overlay: &OverlayConflictSets,
    ) -> MetaResult<bool> {
        if inodes.is_empty() {
            return Ok(false);
        }
        let mut ready_inodes = HashSet::new();
        for ino in inodes {
            if overlay.attr_inodes.contains(ino) || overlay.block_inodes.contains(ino) {
                continue;
            }
            if self.local_regular_file_state_needs_foreign_protection(*ino)? {
                continue;
            }
            ready_inodes.insert(*ino);
        }
        self.local
            .reconcile_regular_file_serving_from_base_with_protected_dentries(
                &ready_inodes,
                &overlay.dentry_keys,
            )
    }

    pub(super) fn reconcile_serving_dentries_from_base_with_overlay(
        &self,
        dentry_keys: &HashSet<Vec<u8>>,
        overlay: &OverlayConflictSets,
    ) -> MetaResult<bool> {
        if dentry_keys.is_empty() {
            return Ok(false);
        }
        self.local
            .reconcile_serving_dentries_from_base(dentry_keys, &overlay.dentry_keys)
    }

    pub(super) fn apply_decoded_batches(
        &self,
        batches: Vec<DecodedReplayBatch>,
        recent_flush: Option<&RecentFlush>,
        mut context: RemoteReplayApplyContext,
    ) -> MetaResult<RemoteReplayApplyOutcome> {
        if batches.is_empty() {
            return Ok(RemoteReplayApplyOutcome {
                changed: false,
                max_version: context.last_seq,
                max_namespace_version: context.current_namespace_version,
                flushed_event_seq: None,
            });
        }

        let mut overlay = context.initial_overlay.clone();
        let mut applied_any = false;
        let mut max_version = context.last_seq;
        let mut max_namespace_version = context.current_namespace_version;
        let mut reconcile_inodes = HashSet::new();
        let mut reconcile_dentry_keys = HashSet::new();
        let mut replay_protected_overlay = OverlayConflictSets::default();
        let mut acknowledged_flushed_event_seq = None;
        let mut overlay_is_dirty = false;
        let empty_protected_base_inodes = HashSet::new();
        for batch in batches {
            Self::collect_replay_reconcile_inodes(&mut reconcile_inodes, &batch.events);
            Self::collect_replay_reconcile_dentries(&mut reconcile_dentry_keys, &batch.events);
            if batch.is_self_batch {
                applied_any |= self.apply_visible_self_batch(&batch.events)?;
                let acknowledged_seq = self.acknowledge_local_prefix_from_self_batch(
                    &batch.events,
                    context.flushed_event_seq,
                    context.next_event_seq,
                )?;
                if let Some(next_flushed_event_seq) = acknowledged_seq {
                    context.flushed_event_seq = next_flushed_event_seq;
                    acknowledged_flushed_event_seq = Some(next_flushed_event_seq);
                    overlay_is_dirty = true;
                }
            } else {
                let effective_overlay =
                    Self::effective_replay_overlay(&overlay, recent_flush, batch.batch_version);
                let protected_base_inodes = Self::protected_base_inodes_for_replay(recent_flush)
                    .unwrap_or(&empty_protected_base_inodes);
                let outcome = self.apply_foreign_batch(
                    &batch.events,
                    &effective_overlay,
                    protected_base_inodes,
                )?;
                applied_any |= outcome.changed;
                replay_protected_overlay = Self::merge_overlay_conflicts(
                    &replay_protected_overlay,
                    &outcome.protected_overlay,
                );
            }
            if batch.events.iter().any(MetaEvent::is_namespace_mutation) {
                max_namespace_version = max_namespace_version.max(batch.batch_version);
            }
            max_version = batch.batch_version;
            if overlay_is_dirty {
                overlay = Self::overlay_conflicts_for_local_range(
                    self.local,
                    context.flushed_event_seq,
                    context.next_event_seq,
                )?;
                overlay_is_dirty = false;
            }
        }
        let final_overlay = Self::merge_overlay_conflicts(&overlay, &replay_protected_overlay);
        applied_any |= self.reconcile_regular_file_serving_from_base_with_overlay(
            &reconcile_inodes,
            &final_overlay,
        )?;
        applied_any |= self.reconcile_serving_dentries_from_base_with_overlay(
            &reconcile_dentry_keys,
            &final_overlay,
        )?;
        Ok(RemoteReplayApplyOutcome {
            changed: applied_any,
            max_version,
            max_namespace_version,
            flushed_event_seq: acknowledged_flushed_event_seq,
        })
    }

    pub(super) fn apply_event_rows(
        &self,
        rows: Vec<tokio_postgres::Row>,
        recent_flush: Option<&RecentFlush>,
        client_id: &str,
        context: RemoteReplayApplyContext,
    ) -> MetaResult<RemoteReplayApplyOutcome> {
        if rows.is_empty() {
            return Ok(RemoteReplayApplyOutcome {
                changed: false,
                max_version: context.last_seq,
                max_namespace_version: context.current_namespace_version,
                flushed_event_seq: None,
            });
        }

        let mut batches = Vec::new();
        let mut idx = 0;
        while idx < rows.len() {
            let (batch_version, next_idx, events, is_self_batch) =
                Self::decode_foreign_batch(&rows, idx, client_id)?;
            idx = next_idx;
            batches.push(DecodedReplayBatch {
                batch_version,
                events,
                is_self_batch,
            });
        }
        self.apply_decoded_batches(batches, recent_flush, context)
    }

    pub(super) fn reconcile_recent_flush(
        &self,
        recent_flush: Option<&RecentFlush>,
        local_version: i64,
        overlay: &OverlayConflictSets,
    ) -> MetaResult<bool> {
        let Some(recent_flush) = recent_flush else {
            return Ok(false);
        };
        if local_version < recent_flush.batch_version {
            return Ok(false);
        }
        let mut reconcile_inodes = recent_flush.overlay.attr_inodes.clone();
        reconcile_inodes.extend(recent_flush.overlay.block_inodes.iter().copied());
        let mut changed =
            self.reconcile_regular_file_serving_from_base_with_overlay(&reconcile_inodes, overlay)?;
        changed |= self.reconcile_serving_dentries_from_base_with_overlay(
            &recent_flush.overlay.dentry_keys,
            overlay,
        )?;
        Ok(changed)
    }
}
