use super::*;

pub(crate) struct PendingLocalNamespaceState {
    pub(crate) bindings: HashMap<Vec<u8>, Option<DirEntryValue>>,
    pub(crate) removed_inodes: HashSet<Ino>,
}

#[cfg(test)]
thread_local! {
    static REPLICA_INVALIDATION_DERIVATIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

/// Counts replica invalidation derivations that read redb on this thread.
#[cfg(test)]
pub(crate) fn take_replica_invalidation_derivations() -> usize {
    REPLICA_INVALIDATION_DERIVATIONS.with(|count| count.replace(0))
}

#[cfg(test)]
fn record_replica_invalidation_derivation() {
    REPLICA_INVALIDATION_DERIVATIONS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
thread_local! {
    static LOCAL_DENTRY_JOURNAL_SCANS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
thread_local! {
    static PENDING_SERVING_REPLAYS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Counts non-empty pending-log serving replays on this thread.
#[cfg(test)]
pub(crate) fn take_pending_serving_replays() -> usize {
    PENDING_SERVING_REPLAYS.with(|count| count.replace(0))
}

/// Counts full local-journal reductions for dentry removal state on this thread.
#[cfg(test)]
pub(crate) fn take_local_dentry_journal_scans() -> usize {
    LOCAL_DENTRY_JOURNAL_SCANS.with(|count| count.replace(0))
}

fn collect_prefix_keys<V: redb::Value + 'static>(
    table: &impl ReadableTable<&'static [u8], V>,
    prefix: &[u8],
    #[cfg(test)] scan: super::RedbRowReadScan,
) -> MetaResult<Vec<Vec<u8>>> {
    let rows = table
        .range(prefix..)?
        .map(|row| row.map_err(MetaError::from));
    #[cfg(test)]
    let rows =
        super::inject_redb_row_read_error(scan, rows, |(key, _)| key.value().starts_with(prefix));
    let mut keys = Vec::new();
    for row in rows {
        let (key, _) = row?;
        if !key.value().starts_with(prefix) {
            break;
        }
        keys.push(key.value().to_vec());
    }
    Ok(keys)
}

#[derive(Default)]
pub(super) struct LocalDentryMutationReducer {
    states: HashMap<Vec<u8>, LocalDentryMutation>,
    loaded: HashSet<Vec<u8>>,
    touched: HashSet<Vec<u8>>,
    retained_created: HashSet<Vec<u8>>,
}

impl LocalDentryMutationReducer {
    pub(super) fn load_key_from_table(
        &mut self,
        table: &redb::Table<'_, &[u8], u8>,
        key: &[u8],
    ) -> MetaResult<()> {
        if self.loaded.insert(key.to_vec())
            && let Some(value) = table
                .get(key)
                .map_err(|error| MetaError::Storage(error.to_string()))?
        {
            self.states
                .insert(key.to_vec(), LocalDentryMutation::try_from(value.value())?);
        }
        Ok(())
    }

    fn load_event_keys_from_table(
        &mut self,
        table: &redb::Table<'_, &[u8], u8>,
        event: &crate::events::MetaEvent,
    ) -> MetaResult<()> {
        use crate::events::MetaEvent;

        match event {
            MetaEvent::AddEntry { parent, name, .. } | MetaEvent::RemoveEntry { parent, name } => {
                self.load_key_from_table(table, &encode_dentry_key(*parent, name))?;
            }
            MetaEvent::RenameEntry {
                parent,
                name,
                new_parent,
                new_name,
                ..
            } => {
                self.load_key_from_table(table, &encode_dentry_key(*parent, name))?;
                self.load_key_from_table(table, &encode_dentry_key(*new_parent, new_name))?;
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn apply_to_table(
        &mut self,
        table: &redb::Table<'_, &[u8], u8>,
        event: &crate::events::MetaEvent,
    ) -> MetaResult<()> {
        self.load_event_keys_from_table(table, event)?;
        self.apply(event);
        Ok(())
    }

    pub(super) fn apply_retained_to_table(
        &mut self,
        table: &redb::Table<'_, &[u8], u8>,
        event: &crate::events::MetaEvent,
    ) -> MetaResult<()> {
        self.load_event_keys_from_table(table, event)?;
        self.apply_retained(event);
        Ok(())
    }

    pub(super) fn apply(&mut self, event: &crate::events::MetaEvent) {
        use crate::events::MetaEvent;

        match event {
            MetaEvent::AddEntry { parent, name, .. } => {
                let key = encode_dentry_key(*parent, name);
                self.touched.insert(key.clone());
                self.states.insert(key, LocalDentryMutation::Created);
            }
            MetaEvent::RemoveEntry { parent, name } => {
                let key = encode_dentry_key(*parent, name);
                self.touched.insert(key.clone());
                if self.states.remove(&key) != Some(LocalDentryMutation::Created) {
                    self.states.insert(key, LocalDentryMutation::Removed);
                }
            }
            MetaEvent::RenameEntry {
                parent,
                name,
                new_parent,
                new_name,
                ..
            } => {
                let source_key = encode_dentry_key(*parent, name);
                let destination_key = encode_dentry_key(*new_parent, new_name);
                let was_created =
                    self.states.get(&source_key) == Some(&LocalDentryMutation::Created);
                if was_created {
                    self.states.remove(&source_key);
                    self.touched.insert(source_key);
                }
                self.states.remove(&destination_key);
                self.touched.insert(destination_key.clone());
                if was_created {
                    self.states
                        .insert(destination_key, LocalDentryMutation::Created);
                }
            }
            _ => {}
        }
    }

    pub(super) fn is_removed(&self, key: &[u8]) -> bool {
        self.states.get(key) == Some(&LocalDentryMutation::Removed)
    }

    fn apply_retained(&mut self, event: &crate::events::MetaEvent) {
        use crate::events::MetaEvent;

        match event {
            MetaEvent::AddEntry { parent, name, .. } => {
                let key = encode_dentry_key(*parent, name);
                self.loaded.insert(key.clone());
                self.retained_created.insert(key.clone());
                self.states.insert(key, LocalDentryMutation::Created);
            }
            MetaEvent::RemoveEntry { parent, name } => {
                let key = encode_dentry_key(*parent, name);
                self.loaded.insert(key.clone());
                if self.retained_created.remove(&key) {
                    self.states.remove(&key);
                } else {
                    self.states.insert(key, LocalDentryMutation::Removed);
                }
            }
            MetaEvent::RenameEntry {
                parent,
                name,
                new_parent,
                new_name,
                ..
            } => {
                let source_key = encode_dentry_key(*parent, name);
                let destination_key = encode_dentry_key(*new_parent, new_name);
                let was_created =
                    self.states.get(&source_key) == Some(&LocalDentryMutation::Created);
                self.retained_created.remove(&source_key);
                if was_created {
                    self.states.remove(&source_key);
                }
                self.retained_created.remove(&destination_key);
                self.states.remove(&destination_key);
                if was_created {
                    self.states
                        .insert(destination_key.clone(), LocalDentryMutation::Created);
                    self.retained_created.insert(destination_key);
                }
            }
            _ => {}
        }
    }

    pub(super) fn persist_touched(&self, table: &mut redb::Table<'_, &[u8], u8>) -> MetaResult<()> {
        for key in &self.touched {
            if let Some(state) = self.states.get(key) {
                table
                    .insert(key.as_slice(), *state as u8)
                    .map_err(|error| MetaError::Storage(error.to_string()))?;
            } else {
                let _ = table
                    .remove(key.as_slice())
                    .map_err(|error| MetaError::Storage(error.to_string()))?;
            }
        }
        Ok(())
    }
}

/// Local journal reductions shared by the renames of one replay pass.
///
/// Each reduction runs at most once, on the first rename that needs it,
/// instead of once per rename. The pass must not append local journal events,
/// move the flushed cursor or rewrite the dentry mutation baseline while this
/// value is alive; replica serving and self-batch reconciliation write only
/// namespace mirror tables.
#[derive(Default)]
pub(crate) struct ReplayLocalJournal {
    reducer: Option<LocalDentryMutationReducer>,
    pending_aliases: Option<PendingLocalAliases>,
}

impl ReplayLocalJournal {
    fn is_removed(&mut self, txn: &redb::WriteTransaction, dentry_key: &[u8]) -> MetaResult<bool> {
        let reducer = match &mut self.reducer {
            Some(reducer) => reducer,
            empty => empty.insert(RedbMetaEngine::reduce_local_dentry_mutations_in_txn(txn)?),
        };
        // Keys the journal never touched keep their baseline state.
        let baseline = txn
            .open_table(LOCAL_DENTRY_MUTATIONS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        reducer.load_key_from_table(&baseline, dentry_key)?;
        Ok(reducer.is_removed(dentry_key))
    }

    /// Dentry keys the unflushed local journal still binds to `entry`.
    fn pending_aliases(
        &mut self,
        txn: &redb::WriteTransaction,
        entry: &DirEntryValue,
    ) -> MetaResult<&HashSet<Vec<u8>>> {
        let aliases = match &mut self.pending_aliases {
            Some(aliases) => aliases,
            empty => empty.insert(RedbMetaEngine::reduce_pending_local_aliases_in_txn(txn)?),
        };
        Ok(aliases.for_entry(entry))
    }
}

/// Unflushed local dentry aliases for every inode, reduced in one journal pass.
#[derive(Default)]
struct PendingLocalAliases {
    by_entry: HashMap<(Ino, u8), HashSet<Vec<u8>>>,
    entries_by_key: HashMap<Vec<u8>, HashSet<(Ino, u8)>>,
    empty: HashSet<Vec<u8>>,
}

impl PendingLocalAliases {
    fn identity(entry: &DirEntryValue) -> (Ino, u8) {
        (entry.ino, entry.kind as u8)
    }

    fn insert(&mut self, identity: (Ino, u8), key: Vec<u8>) {
        self.entries_by_key
            .entry(key.clone())
            .or_default()
            .insert(identity);
        self.by_entry.entry(identity).or_default().insert(key);
    }

    fn apply(&mut self, event: &crate::events::MetaEvent) {
        use crate::events::MetaEvent;

        match event {
            MetaEvent::AddEntry {
                parent,
                name,
                entry,
            } => self.insert(Self::identity(entry), encode_dentry_key(*parent, name)),
            MetaEvent::RemoveEntry { parent, name } => {
                let key = encode_dentry_key(*parent, name);
                for identity in self.entries_by_key.remove(&key).unwrap_or_default() {
                    if let Some(keys) = self.by_entry.get_mut(&identity) {
                        keys.remove(&key);
                    }
                }
            }
            MetaEvent::RenameEntry {
                parent,
                name,
                new_parent,
                new_name,
                entry,
                ..
            } => {
                let identity = Self::identity(entry);
                let old_key = encode_dentry_key(*parent, name);
                let moved = self
                    .by_entry
                    .get_mut(&identity)
                    .is_some_and(|keys| keys.remove(&old_key));
                if moved {
                    if let Some(entries) = self.entries_by_key.get_mut(&old_key) {
                        entries.remove(&identity);
                    }
                    self.insert(identity, encode_dentry_key(*new_parent, new_name));
                }
            }
            _ => {}
        }
    }

    fn for_entry(&self, entry: &DirEntryValue) -> &HashSet<Vec<u8>> {
        self.by_entry
            .get(&Self::identity(entry))
            .unwrap_or(&self.empty)
    }
}

/// An inode's merged attributes plus the dentries bound to it, keyed by dentry key.
type ServingInodeView = (Option<InodeAttr>, BTreeMap<Vec<u8>, DirEntryValue>);

/// Inodes and dentry keys a set of replayed events can touch.
#[derive(Default)]
struct ReplayScope {
    inodes: BTreeSet<Ino>,
    dentry_keys: BTreeSet<Vec<u8>>,
}

#[derive(PartialEq)]
struct ReplayScopeView {
    inodes: Vec<(ServingInodeView, Vec<Vec<u8>>)>,
    dentries: Vec<Option<DirEntryValue>>,
}

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    static REGULAR_FILE_SERVING_RECONCILE_CALLS: Cell<usize> = const { Cell::new(0) };
    static PENDING_LOCAL_ALIAS_EVENT_SCAN_COUNT: Cell<usize> = const { Cell::new(0) };
}

impl RedbMetaEngine {
    pub(super) fn ensure_pruned_dentry_baseline(
        pruned_through: u64,
        baseline_version: Option<u64>,
    ) -> MetaResult<()> {
        if (pruned_through == 0 && baseline_version.is_none()) || baseline_version == Some(1) {
            Ok(())
        } else {
            Err(MetaError::Storage(
                "local event history was pruned before dentry authority was recorded; replace local state from a serving checkpoint".to_string(),
            ))
        }
    }

    fn invalidation_binding(
        txn: &RedbWriteTransaction<'_>,
        key: &[u8],
    ) -> MetaResult<Option<DirEntryValue>> {
        let tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        if tombstones
            .get(key)
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .is_some()
        {
            return Ok(None);
        }
        for table in [DENTRIES_TABLE, BASE_DENTRIES_TABLE] {
            let table = txn
                .open_table(table)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            if let Some(value) = table
                .get(key)
                .map_err(|error| MetaError::Storage(error.to_string()))?
            {
                return decode(value.value()).map(Some);
            }
        }
        Ok(None)
    }

    fn invalidation_ancestors(
        txn: &RedbWriteTransaction<'_>,
        mut ino: Ino,
    ) -> MetaResult<Option<Vec<Vec<u8>>>> {
        let mut names = Vec::new();
        let mut visited = HashSet::new();
        let mut bytes = 0usize;
        while ino != ROOT_INO {
            if names.len() >= 128 || !visited.insert(ino) {
                return Ok(None);
            }
            match Self::get_attr_merged_in_write_txn(txn, ino) {
                Ok(attr) if attr.kind == FileType::Directory => {}
                Ok(_) | Err(MetaError::InodeNotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            }
            let prefix = dentry_by_inode_prefix(ino);
            let mut binding = None;
            // Prefer the serving mirror: base may already contain the next rename.
            for index in [DENTRIES_BY_INODE_TABLE, BASE_DENTRIES_BY_INODE_TABLE] {
                let index = txn
                    .open_table(index)
                    .map_err(|error| MetaError::Storage(error.to_string()))?;
                let mut scanned = 0;
                for row in index
                    .range(prefix.as_slice()..)
                    .map_err(|error| MetaError::Storage(error.to_string()))?
                {
                    let (key, _) = row.map_err(|error| MetaError::Storage(error.to_string()))?;
                    if !key.value().starts_with(&prefix) {
                        break;
                    }
                    scanned += 1;
                    if scanned > 8 {
                        return Ok(None);
                    }
                    let (_, parent, name) = decode_dentry_by_inode_key(key.value())?;
                    let key = encode_dentry_key(parent, name);
                    if !Self::invalidation_binding(txn, &key)?
                        .is_some_and(|entry| entry.ino == ino && entry.kind == FileType::Directory)
                    {
                        continue;
                    }
                    if binding.is_some() {
                        return Ok(None);
                    }
                    binding = Some((parent, name.to_vec()));
                }
                if binding.is_some() {
                    break;
                }
            }
            let Some((parent, name)) = binding else {
                return Ok(None);
            };
            bytes += name.len();
            if bytes > 64 * 1024 {
                return Ok(None);
            }
            names.push(name);
            ino = parent;
        }
        names.reverse();
        Ok(Some(names))
    }

    fn invalidate_directory_descendants(
        txn: &RedbWriteTransaction<'_>,
        ino: Ino,
    ) -> MetaResult<()> {
        if txn.invalidations_suppressed() {
            return Ok(());
        }
        #[cfg(test)]
        record_replica_invalidation_derivation();
        let mut pending = vec![(ino, Self::invalidation_ancestors(txn, ino)?, 0usize)];
        let mut visited = HashSet::new();
        let mut remaining = crate::coherence::MAX_INVALIDATION_INTENTS;
        let mut remaining_bytes = 1024 * 1024usize;
        while let Some((parent, ancestors, depth)) = pending.pop() {
            if !visited.insert(parent)
                || depth >= 128
                || ancestors.as_ref().is_some_and(|parts| parts.len() >= 128)
            {
                txn.invalidate(InvalidationIntent::Reset);
                return Ok(());
            }
            let prefix = dentry_prefix(parent);
            let mut entries = BTreeMap::new();
            // Copy a bounded key set before opening the same tables for merged lookup.
            for table in [DENTRIES_TABLE, BASE_DENTRIES_TABLE] {
                let table = txn
                    .open_table(table)
                    .map_err(|error| MetaError::Storage(error.to_string()))?;
                for row in table
                    .range(prefix.as_slice()..)
                    .map_err(|error| MetaError::Storage(error.to_string()))?
                {
                    let (key, _) = row.map_err(|error| MetaError::Storage(error.to_string()))?;
                    if !key.value().starts_with(&prefix) {
                        break;
                    }
                    if remaining == 0 {
                        txn.invalidate(InvalidationIntent::Reset);
                        return Ok(());
                    }
                    remaining -= 1;
                    let (_, name) = decode_dentry_key(key.value())?;
                    let bytes = key.value().len().saturating_add(name.len());
                    if bytes > remaining_bytes {
                        txn.invalidate(InvalidationIntent::Reset);
                        return Ok(());
                    }
                    remaining_bytes -= bytes;
                    entries.insert(key.value().to_vec(), name.to_vec());
                }
            }
            for (key, name) in entries {
                let Some(entry) = Self::invalidation_binding(txn, &key)? else {
                    continue;
                };
                let bytes = ancestors.as_ref().map_or(0, |parts| {
                    parts
                        .iter()
                        .map(|part| part.len() + std::mem::size_of::<Vec<u8>>())
                        .sum::<usize>()
                }) + name.len();
                if bytes.saturating_mul(2) > remaining_bytes {
                    txn.invalidate(InvalidationIntent::Reset);
                    return Ok(());
                }
                remaining_bytes -= bytes * 2;
                txn.invalidate(InvalidationIntent::Entry {
                    parent,
                    name: name.clone(),
                    ancestors: ancestors.clone(),
                    ino: Some(entry.ino),
                });
                txn.invalidate(InvalidationIntent::Directory { ino: parent });
                txn.invalidate(InvalidationIntent::Attributes { ino: entry.ino });
                if entry.kind == FileType::Directory {
                    let next = ancestors.as_ref().map(|parts| {
                        let mut parts = parts.clone();
                        parts.push(name);
                        parts
                    });
                    pending.push((entry.ino, next, depth + 1));
                }
            }
        }
        Ok(())
    }

    fn invalidate_replica_entry(
        txn: &RedbWriteTransaction<'_>,
        parent: Ino,
        name: &[u8],
        new_ino: Option<Ino>,
    ) -> MetaResult<()> {
        if txn.invalidations_suppressed() {
            return Ok(());
        }
        #[cfg(test)]
        record_replica_invalidation_derivation();
        txn.invalidate(InvalidationIntent::Entry {
            parent,
            name: name.to_vec(),
            ancestors: Self::invalidation_ancestors(txn, parent)?,
            ino: Self::invalidation_binding(txn, &encode_dentry_key(parent, name))?
                .map(|entry| entry.ino)
                .or(new_ino),
        });
        txn.invalidate(InvalidationIntent::Directory { ino: parent });
        txn.invalidate(InvalidationIntent::Attributes { ino: parent });
        // Preserve the displaced binding before either serving or base removes it.
        for table in [DENTRIES_TABLE, BASE_DENTRIES_TABLE] {
            let table = txn
                .open_table(table)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            if let Some(entry) = Self::lookup_exact_in_opened_dentry_table(&table, parent, name)? {
                txn.invalidate(InvalidationIntent::Attributes { ino: entry.ino });
            }
        }
        Ok(())
    }

    fn invalidate_replica_inode(
        txn: &RedbWriteTransaction<'_>,
        ino: Ino,
        content: bool,
    ) -> MetaResult<()> {
        if txn.invalidations_suppressed() {
            return Ok(());
        }
        #[cfg(test)]
        record_replica_invalidation_derivation();
        txn.invalidate(InvalidationIntent::Attributes { ino });
        if content {
            txn.invalidate(InvalidationIntent::Content { ino });
        }
        for index in [DENTRIES_BY_INODE_TABLE, BASE_DENTRIES_BY_INODE_TABLE] {
            for key in Self::dentry_keys_for_inode_in_write_index(txn, index, ino)? {
                let (parent, name) = decode_dentry_key(&key)?;
                Self::invalidate_replica_entry(txn, parent, name, Some(ino))?;
            }
        }
        Ok(())
    }

    fn replica_content_identity_changed(
        txn: &RedbWriteTransaction<'_>,
        attr: &InodeAttr,
    ) -> MetaResult<bool> {
        if attr.kind != FileType::RegularFile {
            return Ok(false);
        }
        let current = match Self::get_attr_merged_in_write_txn(txn, attr.ino) {
            Ok(current) => current,
            Err(MetaError::InodeNotFound(_)) => return Ok(true),
            Err(error) => return Err(error),
        };
        Ok(current.kind != attr.kind
            || current.size != attr.size
            || current.content_epoch != attr.content_epoch
            || current.committed_size != attr.committed_size
            || current.committed_content_epoch != attr.committed_content_epoch)
    }

    fn invalidate_replica_event(
        txn: &RedbWriteTransaction<'_>,
        event: &crate::events::MetaEvent,
    ) -> MetaResult<()> {
        // Intent derivation only reads; skip it when the journal would drop the result.
        if txn.invalidations_suppressed() {
            return Ok(());
        }
        use crate::events::MetaEvent;
        match event {
            MetaEvent::SetAttr { attr } => {
                Self::invalidate_replica_inode(
                    txn,
                    attr.ino,
                    Self::replica_content_identity_changed(txn, attr)?,
                )?;
                if attr.kind == FileType::Directory {
                    txn.invalidate(InvalidationIntent::Directory { ino: attr.ino });
                }
            }
            MetaEvent::RemoveAttr { ino }
            | MetaEvent::SetXattr { ino, .. }
            | MetaEvent::RemoveXattr { ino, .. }
            | MetaEvent::SetAclRule { ino, .. }
            | MetaEvent::RemoveAclRule { ino, .. } => {
                if matches!(event, MetaEvent::RemoveAttr { .. })
                    && Self::get_attr_merged_in_write_txn(txn, *ino)
                        .is_ok_and(|attr| attr.kind == FileType::Directory)
                {
                    Self::invalidate_directory_descendants(txn, *ino)?;
                }
                Self::invalidate_replica_inode(txn, *ino, false)?
            }
            MetaEvent::SetSymlinkTarget { ino, .. }
            | MetaEvent::RemoveRegularFileGraph { ino }
            | MetaEvent::SetRegularFileOverlaySlices { ino, .. }
            | MetaEvent::PublishRegularFileBaseSlices { ino, .. } => {
                Self::invalidate_replica_inode(txn, *ino, true)?
            }
            MetaEvent::AddEntry {
                parent,
                name,
                entry,
            } => {
                Self::invalidate_replica_entry(txn, *parent, name, Some(entry.ino))?;
                Self::invalidate_replica_inode(txn, entry.ino, false)?;
            }
            MetaEvent::RemoveEntry { parent, name } => {
                Self::invalidate_replica_entry(txn, *parent, name, None)?
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
                if entry.kind == FileType::Directory {
                    Self::invalidate_directory_descendants(txn, entry.ino)?;
                }
                Self::invalidate_replica_entry(txn, *parent, name, Some(entry.ino))?;
                Self::invalidate_replica_entry(txn, *new_parent, new_name, Some(entry.ino))?;
                Self::invalidate_replica_inode(txn, entry.ino, false)?;
                if let Some(ino) = displaced_remove_attr_ino {
                    Self::invalidate_replica_inode(txn, *ino, false)?;
                }
                for attr in [
                    displaced_set_attr,
                    moved_dir_attr,
                    old_parent_attr,
                    new_parent_attr,
                ] {
                    if let Some(attr) = attr.as_ref() {
                        Self::invalidate_replica_inode(txn, attr.ino, false)?;
                    }
                }
            }
            MetaEvent::AddPendingDeletion { .. } | MetaEvent::RemovePendingDeletion { .. } => {}
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn reset_regular_file_serving_reconcile_call_count_for_test() {
        REGULAR_FILE_SERVING_RECONCILE_CALLS.set(0);
    }

    #[cfg(test)]
    pub(crate) fn regular_file_serving_reconcile_call_count_for_test() -> usize {
        REGULAR_FILE_SERVING_RECONCILE_CALLS.get()
    }

    #[cfg(test)]
    pub(crate) fn reset_pending_local_alias_event_scan_count_for_test() {
        PENDING_LOCAL_ALIAS_EVENT_SCAN_COUNT.set(0);
    }

    #[cfg(test)]
    pub(crate) fn pending_local_alias_event_scan_count_for_test() -> usize {
        PENDING_LOCAL_ALIAS_EVENT_SCAN_COUNT.get()
    }

    fn remove_dentry_mirrors_for_inode_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        let keys = Self::dentry_keys_for_inode_in_write_index(txn, DENTRIES_BY_INODE_TABLE, ino)?;
        for key in keys {
            let (parent, name) = decode_dentry_key(&key)?;
            Self::remove_dentry_mirror_in_txn(txn, parent, name)?;
        }
        Ok(())
    }

    fn remove_base_dentries_for_inode_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        let keys =
            Self::dentry_keys_for_inode_in_write_index(txn, BASE_DENTRIES_BY_INODE_TABLE, ino)?;
        for key in keys {
            let (parent, name) = decode_dentry_key(&key)?;
            Self::remove_base_dentry_in_txn(txn, parent, name)?;
        }
        Ok(())
    }

    /// Export a snapshot projected to the remote-visible published namespace.
    pub fn export_remote_snapshot(&self) -> MetaResult<RedbSnapshot> {
        let txn = self.begin_read_txn()?;
        Self::export_remote_snapshot_from_read_txn(&txn)
    }

    fn export_remote_snapshot_from_read_txn(txn: &ReadTransaction) -> MetaResult<RedbSnapshot> {
        Self::export_remote_snapshot_from_read_txn_with_check(txn, &|| Ok(()))
    }

    fn export_remote_snapshot_from_read_txn_with_check(
        txn: &ReadTransaction,
        check: &impl Fn() -> MetaResult<()>,
    ) -> MetaResult<RedbSnapshot> {
        check()?;
        let mut snapshot = Self::export_snapshot_from_read_txn_with_check(txn, false, check)?;
        let mut published_regular_inodes = HashSet::new();
        for (ino, bytes) in &snapshot.regular_file_base {
            check()?;
            let regular: RegularFileBaseState = decode(bytes)?;
            'chunks: for chunk in regular.chunks.values() {
                check()?;
                for slice in chunk.slices.values() {
                    check()?;
                    if slice.payload_state == PayloadRefState::Published && slice.payload.is_some()
                    {
                        published_regular_inodes.insert(*ino);
                        break 'chunks;
                    }
                }
            }
        }

        let mut projected_inodes = Vec::with_capacity(snapshot.inodes.len());
        let mut hidden_regular_inodes = HashSet::new();
        for (ino, bytes) in snapshot.inodes.drain(..) {
            check()?;
            let mut attr: InodeAttr = decode(&bytes)?;
            if attr.kind == FileType::RegularFile {
                let lifecycle = Self::get_inode_lifecycle_from_read_txn(txn, ino)?;
                let visible_in_remote_namespace = published_regular_inodes.contains(&ino)
                    || (lifecycle
                        .as_ref()
                        .is_none_or(|record| record.has_published_base)
                        && Self::regular_file_remote_visible_without_published_base(&attr));
                if !visible_in_remote_namespace {
                    hidden_regular_inodes.insert(ino);
                    continue;
                }
                attr.size = attr.committed_size;
            }
            projected_inodes.push((ino, encode(&attr)?));
        }
        snapshot.inodes = projected_inodes;
        let mut dentries = Vec::with_capacity(snapshot.dentries.len());
        for (key, bytes) in snapshot.dentries.drain(..) {
            check()?;
            let (parent, _) = decode_dentry_key(&key)?;
            let entry: DirEntryValue = postcard::from_bytes(&bytes).map_err(|e| {
                MetaError::Serialization(format!(
                    "checkpoint dentry projection decode failed for parent {parent}: {e}"
                ))
            })?;
            if !hidden_regular_inodes.contains(&entry.ino) {
                dentries.push((key, bytes));
            }
        }
        snapshot.dentries = dentries;
        let mut regular_file_base = Vec::with_capacity(snapshot.regular_file_base.len());
        for (ino, bytes) in snapshot.regular_file_base.drain(..) {
            check()?;
            if !hidden_regular_inodes.contains(&ino) {
                regular_file_base.push((ino, bytes));
            }
        }
        snapshot.regular_file_base = regular_file_base;

        let mut referenced_objects = BTreeMap::new();
        for (_, bytes) in &snapshot.regular_file_base {
            check()?;
            let regular: RegularFileBaseState = decode(bytes)?;
            for chunk in regular.chunks.values() {
                check()?;
                for slice in chunk.slices.values() {
                    check()?;
                    if let Some(block) = slice.payload.as_ref() {
                        referenced_objects
                            .entry(block.object_id)
                            .or_insert(block.length as u64);
                    }
                }
            }
        }
        let mut committed_used_space = 0_u64;
        for len in referenced_objects.values() {
            check()?;
            committed_used_space = committed_used_space.saturating_add(*len);
        }

        let mut regular_file_overlay = Vec::with_capacity(snapshot.regular_file_base.len());
        for (ino, bytes) in &snapshot.regular_file_base {
            check()?;
            regular_file_overlay.push((*ino, bytes.clone()));
        }
        snapshot.regular_file_overlay = regular_file_overlay;
        snapshot.event_log.clear();
        let mut counters = Vec::with_capacity(snapshot.counters.len());
        for (name, value) in snapshot.counters.drain(..) {
            check()?;
            if name.as_str() != crate::schema::counters::NEXT_EVENT_SEQ
                && !crate::pg_engine::is_local_pg_snapshot_state_counter(&name)
            {
                counters.push((name, value));
            }
        }
        snapshot.counters = counters;
        if let Some((_, value)) = snapshot
            .counters
            .iter_mut()
            .find(|(name, _)| name.as_str() == counters::USED_SPACE)
        {
            *value = committed_used_space;
        } else {
            snapshot
                .counters
                .push((counters::USED_SPACE.to_string(), committed_used_space));
        }

        Ok(snapshot)
    }

    pub(super) fn export_serving_checkpoint_from_read_txn(
        txn: &ReadTransaction,
    ) -> MetaResult<crate::snapshot::ServingCheckpoint> {
        Self::export_serving_checkpoint_from_read_txn_with_check(txn, &|| Ok(()))
    }

    pub(super) fn export_serving_checkpoint_from_read_txn_with_check(
        txn: &ReadTransaction,
        check: &impl Fn() -> MetaResult<()>,
    ) -> MetaResult<crate::snapshot::ServingCheckpoint> {
        check()?;
        let snapshot = Self::export_remote_snapshot_from_read_txn_with_check(txn, check)?;
        Ok(crate::snapshot::ServingCheckpoint {
            inodes: snapshot.inodes,
            dentries: snapshot.dentries,
            regular_file_base: snapshot.regular_file_base,
            symlinks: snapshot.symlinks,
            xattrs: snapshot.xattrs,
            acl_rules: snapshot.acl_rules,
            directory_aliases: snapshot.directory_aliases,
        })
    }

    /// Encode the shared serving checkpoint for PostgreSQL initialization.
    ///
    /// The byte representation contains only the remote-visible read model;
    /// local counters, event journals, cleanup state, and snapshot control
    /// data stay in the local database or PostgreSQL control-plane tables.
    pub fn export_serving_checkpoint_bytes(&self) -> MetaResult<Vec<u8>> {
        self.begin_serving_checkpoint_snapshot()?
            .export_serving_checkpoint_bytes()
    }

    /// Return symlink inode ids whose local state differs from the imported base view.
    pub fn local_overlay_symlink_inodes(&self) -> MetaResult<HashSet<Ino>> {
        let txn = self.begin_read_txn()?;
        let current = txn
            .open_table(SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(SYMLINK_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut overlay = HashSet::new();
        for row in current
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (ino, current_bytes) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let ino = ino.value();
            match base
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(base_bytes) if base_bytes.value() == current_bytes.value() => {}
                _ => {
                    overlay.insert(ino);
                }
            }
        }
        for row in tombstones
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (ino, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            overlay.insert(ino.value());
        }
        Ok(overlay)
    }

    /// Return encoded xattr keys whose local state differs from the imported base view.
    pub fn local_overlay_xattr_keys(&self) -> MetaResult<HashSet<Vec<u8>>> {
        let txn = self.begin_read_txn()?;
        let current = txn
            .open_table(XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(XATTR_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut overlay = HashSet::new();
        for row in current
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, current_value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value().to_vec();
            match base
                .get(key_bytes.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(base_value) if base_value.value() == current_value.value() => {}
                _ => {
                    overlay.insert(key_bytes);
                }
            }
        }
        for row in tombstones
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            overlay.insert(key.value().to_vec());
        }

        Ok(overlay)
    }

    /// Return inode ids that currently have locally divergent xattr state.
    pub fn local_overlay_xattr_inodes(&self) -> MetaResult<HashSet<Ino>> {
        self.local_overlay_xattr_keys()?
            .into_iter()
            .map(|key| decode_inode_prefix(&key, "xattr"))
            .collect()
    }

    /// Return encoded ACL keys whose local state differs from the imported base view.
    pub fn local_overlay_acl_keys(&self) -> MetaResult<HashSet<Vec<u8>>> {
        let txn = self.begin_read_txn()?;
        let current = txn
            .open_table(ACL_RULES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_ACL_RULES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(ACL_RULE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut overlay = HashSet::new();
        for row in current
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, current_value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value().to_vec();
            match base
                .get(key_bytes.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(base_value) if base_value.value() == current_value.value() => {}
                _ => {
                    overlay.insert(key_bytes);
                }
            }
        }
        for row in tombstones
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            overlay.insert(key.value().to_vec());
        }
        Ok(overlay)
    }

    /// Returns true if local metadata still contains overlay state that is not
    /// yet remote-visible in the base view.
    pub fn has_local_overlay_state(&self) -> MetaResult<bool> {
        if !self.local_overlay_inodes()?.is_empty() {
            return Ok(true);
        }

        let txn = self.begin_read_txn()?;
        let has_base_metadata = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .next()
            .is_some()
            || txn
                .open_table(BASE_DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some()
            || txn
                .open_table(REGULAR_FILE_BASE_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some()
            || txn
                .open_table(BASE_SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some()
            || txn
                .open_table(BASE_XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some()
            || txn
                .open_table(BASE_ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some();

        if !has_base_metadata {
            return Ok(false);
        }

        Ok(!self.local_overlay_attr_inodes()?.is_empty()
            || !self.local_overlay_dentries()?.is_empty()
            || !self.local_overlay_symlink_inodes()?.is_empty()
            || !self.local_overlay_xattr_keys()?.is_empty()
            || !self.local_overlay_acl_keys()?.is_empty())
    }

    /// Read decoded events from local event log in [start_seq, end_seq).
    pub fn read_events_range(
        &self,
        start_seq: u64,
        end_seq: u64,
    ) -> MetaResult<Vec<crate::events::MetaEvent>> {
        self.read_events_range_with_check(start_seq, end_seq, || Ok(()))
    }

    pub(crate) fn read_events_range_with_check(
        &self,
        start_seq: u64,
        end_seq: u64,
        mut check: impl FnMut() -> MetaResult<()>,
    ) -> MetaResult<Vec<crate::events::MetaEvent>> {
        if end_seq <= start_seq {
            return Ok(Vec::new());
        }
        let txn = self.begin_read_txn()?;
        let table = txn
            .open_table(EVENT_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut out = Vec::with_capacity((end_seq - start_seq).min(1024) as usize);
        for row in table
            .range(start_seq..end_seq)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            check()?;
            let (_seq, bytes) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let ev = crate::events::decode_event(bytes.value())?;
            out.push(ev);
        }
        Ok(out)
    }

    pub(crate) fn read_events_range_in_write_txn_with_check(
        txn: &redb::WriteTransaction,
        start_seq: u64,
        end_seq: u64,
        mut check: impl FnMut() -> MetaResult<()>,
    ) -> MetaResult<Vec<crate::events::MetaEvent>> {
        if end_seq <= start_seq {
            return Ok(Vec::new());
        }
        let table = txn
            .open_table(EVENT_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut out = Vec::new();
        for row in table
            .range(start_seq..end_seq)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            check()?;
            let (_seq, bytes) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            out.push(crate::events::decode_event(bytes.value())?);
        }
        Ok(out)
    }

    /// Apply only remote-visible auxiliary state from foreign replica events
    /// without re-appending it to the local event log. Namespace/attr state is
    /// kept in `base_*`.
    pub fn apply_replica_aux_state(&self, events: &[crate::events::MetaEvent]) -> MetaResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let txn = self.begin_write_txn()?;
        Self::apply_replica_aux_state_in_txn(&txn, events.iter())?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(crate) fn apply_replica_aux_state_in_txn<'event>(
        txn: &RedbWriteTransaction<'_>,
        events: impl IntoIterator<Item = &'event crate::events::MetaEvent>,
    ) -> MetaResult<()> {
        for event in events {
            Self::invalidate_replica_event(txn, event)?;
            match event {
                crate::events::MetaEvent::SetAttr { .. } => {}
                crate::events::MetaEvent::RemoveAttr { ino } => {
                    let mut symlinks = txn
                        .open_table(BASE_SYMLINKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    symlinks.remove(*ino)?;
                    drop(symlinks);

                    let mut xattrs = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let prefix = xattr_prefix(*ino);
                    let keys_to_remove = collect_prefix_keys(
                        &xattrs,
                        &prefix,
                        #[cfg(test)]
                        super::RedbRowReadScan::RemoveAttrAuxXattrs,
                    )?;
                    for key in keys_to_remove {
                        xattrs.remove(key.as_slice())?;
                    }

                    Self::clear_regular_file_graph_in_txn(txn, *ino)?;
                }
                crate::events::MetaEvent::AddEntry { .. } => {}
                crate::events::MetaEvent::RemoveEntry { .. } => {}
                crate::events::MetaEvent::RenameEntry { .. } => {}
                crate::events::MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices } => {
                    for (chunk_id, slices) in chunk_slices {
                        Self::set_regular_file_chunk_state_in_txn(
                            txn,
                            REGULAR_FILE_BASE_TABLE,
                            false,
                            *ino,
                            *chunk_id,
                            slices,
                        )?;
                    }
                }
                crate::events::MetaEvent::RemoveRegularFileGraph { ino } => {
                    Self::clear_regular_file_graph_in_txn(txn, *ino)?;
                }
                crate::events::MetaEvent::PublishRegularFileBaseSlices { ino, chunk_slices } => {
                    Self::merge_regular_file_base_publish_in_txn(
                        txn,
                        *ino,
                        chunk_slices.as_slice(),
                        false,
                    )?;
                }
                crate::events::MetaEvent::SetSymlinkTarget { ino, target } => {
                    let mut table = txn
                        .open_table(BASE_SYMLINKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table
                        .insert(*ino, target.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::SetXattr {
                    ino, name, value, ..
                } => {
                    Self::set_xattr_value_in_table_in_txn(
                        txn,
                        BASE_XATTRS_TABLE,
                        *ino,
                        name,
                        value,
                    )?;
                }
                crate::events::MetaEvent::RemoveXattr { ino, name } => {
                    let mut table = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_xattr_key(*ino, name);
                    table.remove(key.as_slice())?;
                }
                crate::events::MetaEvent::SetAclRule {
                    ino,
                    principal,
                    access_level,
                } => {
                    let mut table = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    table
                        .insert(key.as_slice(), *access_level as u8)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveAclRule { ino, principal } => {
                    let mut table = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    table.remove(key.as_slice())?;
                }
                crate::events::MetaEvent::AddPendingDeletion {
                    object_id,
                    timestamp,
                } => {
                    let mut table = txn
                        .open_table(PENDING_DELETIONS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table
                        .insert(*object_id, *timestamp)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemovePendingDeletion { object_id } => {
                    let mut table = txn
                        .open_table(PENDING_DELETIONS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table.remove(*object_id)?;
                }
            }
        }

        Ok(())
    }

    /// Apply only namespace/attr portions of replica events into the remote-visible base view.
    pub fn apply_replica_events_to_base(
        &self,
        events: &[crate::events::MetaEvent],
    ) -> MetaResult<()> {
        self.apply_replica_events_to_base_inner(events, &HashSet::new())
    }

    /// Apply replica events while preserving protected local base ranges for stale publish batches.
    pub fn apply_replica_events_to_base_preserving_protected_base_ranges(
        &self,
        events: &[crate::events::MetaEvent],
        protected_regular_file_inodes: &HashSet<Ino>,
    ) -> MetaResult<()> {
        self.apply_replica_events_to_base_inner(events, protected_regular_file_inodes)
    }

    fn apply_replica_events_to_base_inner(
        &self,
        events: &[crate::events::MetaEvent],
        protected_regular_file_inodes: &HashSet<Ino>,
    ) -> MetaResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let txn = self.begin_write_txn()?;
        Self::apply_replica_events_to_base_in_txn(
            &txn,
            events.iter(),
            protected_regular_file_inodes,
        )?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(crate) fn apply_replica_events_to_base_in_txn<'event>(
        txn: &RedbWriteTransaction<'_>,
        events: impl IntoIterator<Item = &'event crate::events::MetaEvent>,
        protected_regular_file_inodes: &HashSet<Ino>,
    ) -> MetaResult<()> {
        for event in events {
            Self::invalidate_replica_event(txn, event)?;
            match event {
                crate::events::MetaEvent::SetAttr { attr } => {
                    let data = encode(attr)?;
                    let mut table = txn
                        .open_table(BASE_INODES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table
                        .insert(attr.ino, data.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveAttr { ino } => {
                    Self::remove_base_dentries_for_inode_in_txn(txn, *ino)?;

                    let mut table = txn
                        .open_table(BASE_INODES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table.remove(*ino)?;

                    let mut symlinks = txn
                        .open_table(BASE_SYMLINKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    symlinks.remove(*ino)?;
                    drop(symlinks);

                    let mut xattrs = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let prefix = xattr_prefix(*ino);
                    let keys_to_remove = collect_prefix_keys(
                        &xattrs,
                        &prefix,
                        #[cfg(test)]
                        super::RedbRowReadScan::RemoveAttrBaseXattrs,
                    )?;
                    for key in keys_to_remove {
                        xattrs.remove(key.as_slice())?;
                    }

                    let mut acl = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let prefix = acl_rule_prefix(*ino);
                    let keys_to_remove = collect_prefix_keys(
                        &acl,
                        &prefix,
                        #[cfg(test)]
                        super::RedbRowReadScan::RemoveAttrBaseAclRules,
                    )?;
                    for key in keys_to_remove {
                        acl.remove(key.as_slice())?;
                    }
                }
                crate::events::MetaEvent::AddEntry {
                    parent,
                    name,
                    entry,
                } => {
                    Self::set_base_dentry_in_txn(txn, *parent, name, entry)?;
                }
                crate::events::MetaEvent::RemoveEntry { parent, name } => {
                    Self::remove_base_dentry_in_txn(txn, *parent, name)?;
                }
                crate::events::MetaEvent::RenameEntry {
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
                    // The event names one exact binding; a case sibling is resolved below.
                    let src_matches =
                        Self::lookup_exact_in_write_table(txn, BASE_DENTRIES_TABLE, *parent, name)?
                            .map(|current| current.ino == entry.ino && current.kind == entry.kind)
                            .unwrap_or(false);
                    if src_matches {
                        Self::remove_base_dentry_in_txn(txn, *parent, name)?;
                    } else {
                        let prior_keys = Self::base_dentry_keys_for_rename_entry(txn, entry)?;
                        if prior_keys.len() > 1 {
                            tracing::warn!(
                                ino = entry.ino,
                                source_parent = *parent,
                                source_name = %String::from_utf8_lossy(name),
                                destination_parent = *new_parent,
                                destination_name = %String::from_utf8_lossy(new_name),
                                prior_key_count = prior_keys.len(),
                                "replica rename found multiple base dentries for inode; preserving existing links and applying destination"
                            );
                        } else if let Some(prior_key) = prior_keys.first() {
                            let (prior_parent, prior_name) = decode_dentry_key(prior_key)?;
                            Self::remove_base_dentry_in_txn(txn, prior_parent, prior_name)?;
                        }
                    }
                    Self::remove_base_dentry_in_txn(txn, *new_parent, new_name)?;
                    Self::set_base_dentry_in_txn(txn, *new_parent, new_name, entry)?;

                    let mut inodes = txn
                        .open_table(BASE_INODES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    if let Some(attr) = displaced_set_attr.as_ref() {
                        let data = encode(attr)?;
                        inodes
                            .insert(attr.ino, data.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                    if let Some(ino) = displaced_remove_attr_ino {
                        inodes.remove(*ino)?;
                    }
                    if let Some(attr) = moved_dir_attr.as_ref() {
                        let data = encode(attr)?;
                        inodes
                            .insert(attr.ino, data.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                    if let Some(attr) = old_parent_attr.as_ref() {
                        let data = encode(attr)?;
                        inodes
                            .insert(attr.ino, data.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                    if let Some(attr) = new_parent_attr.as_ref() {
                        let data = encode(attr)?;
                        inodes
                            .insert(attr.ino, data.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                }
                crate::events::MetaEvent::SetSymlinkTarget { ino, target } => {
                    let mut table = txn
                        .open_table(BASE_SYMLINKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table
                        .insert(*ino, target.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::SetXattr {
                    ino, name, value, ..
                } => {
                    Self::set_xattr_value_in_table_in_txn(
                        txn,
                        BASE_XATTRS_TABLE,
                        *ino,
                        name,
                        value,
                    )?;
                }
                crate::events::MetaEvent::RemoveXattr { ino, name } => {
                    let mut table = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_xattr_key(*ino, name);
                    table.remove(key.as_slice())?;
                }
                crate::events::MetaEvent::SetAclRule {
                    ino,
                    principal,
                    access_level,
                } => {
                    let mut table = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    table
                        .insert(key.as_slice(), *access_level as u8)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveAclRule { ino, principal } => {
                    let mut table = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    table.remove(key.as_slice())?;
                }
                crate::events::MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices } => {
                    for (chunk_id, slices) in chunk_slices {
                        Self::set_regular_file_chunk_state_in_txn(
                            txn,
                            REGULAR_FILE_BASE_TABLE,
                            false,
                            *ino,
                            *chunk_id,
                            slices,
                        )?;
                    }
                }
                crate::events::MetaEvent::PublishRegularFileBaseSlices { ino, chunk_slices } => {
                    Self::merge_regular_file_base_publish_in_txn(
                        txn,
                        *ino,
                        chunk_slices.as_slice(),
                        protected_regular_file_inodes.contains(ino),
                    )?;
                }
                crate::events::MetaEvent::RemoveRegularFileGraph { .. }
                | crate::events::MetaEvent::AddPendingDeletion { .. }
                | crate::events::MetaEvent::RemovePendingDeletion { .. } => {}
            }
        }

        Ok(())
    }

    /// Sync the foreground serving tables to a remote-visible state for objects
    /// that do not currently have local overlay conflicts.
    pub fn apply_replica_serving_state(
        &self,
        events: &[crate::events::MetaEvent],
    ) -> MetaResult<()> {
        self.apply_replica_serving_state_inner(events, false, false)
    }

    /// Replays this client's unflushed events on top of the serving state that already
    /// includes them. An event whose inode the serving state tombstones was superseded by a
    /// later local removal in the same window, so it is dropped: replaying it would
    /// resurrect the removed inode through the base row, or leave a row the removal
    /// cannot clear. A rename keeps its dentry move when its entry and parents are live,
    /// but drops attr payloads for tombstoned inodes.
    pub(crate) fn apply_replica_serving_state_for_pending_replay(
        &self,
        events: &[crate::events::MetaEvent],
    ) -> MetaResult<()> {
        if events.is_empty() {
            return Ok(());
        }
        #[cfg(test)]
        PENDING_SERVING_REPLAYS.with(|count| count.set(count.get() + 1));

        let txn = self.begin_write_txn()?;
        let live_events = {
            let tombstones = txn
                .open_table(INODE_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            // Replay never writes base, so this view is stable for the whole window.
            let base_dentries = txn
                .open_table(BASE_DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut live_events = Vec::with_capacity(events.len());
            for event in events {
                Self::push_pending_replay_events_for_tombstones(
                    &tombstones,
                    &base_dentries,
                    event,
                    &mut live_events,
                )?;
            }
            live_events
        };
        Self::apply_replica_serving_state_in_txn_with(&txn, &live_events, true, true)?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Appends the replay form of `event` to `out`; a superseded event appends nothing.
    fn push_pending_replay_events_for_tombstones<'event, T, B>(
        tombstones: &T,
        base_dentries: &B,
        event: &'event crate::events::MetaEvent,
        out: &mut Vec<Cow<'event, crate::events::MetaEvent>>,
    ) -> MetaResult<()>
    where
        T: ReadableTable<u64, u8>,
        B: ReadableTable<&'static [u8], &'static [u8]>,
    {
        use crate::events::MetaEvent;
        let tombstoned = |ino: Ino| -> MetaResult<bool> { Ok(tombstones.get(ino)?.is_some()) };
        let single = |ino: Ino| -> MetaResult<Option<Cow<'event, MetaEvent>>> {
            Ok((!tombstoned(ino)?).then_some(Cow::Borrowed(event)))
        };
        let replay = match event {
            MetaEvent::SetAttr { attr } => single(attr.ino)?,
            MetaEvent::RemoveAttr { ino }
            | MetaEvent::SetXattr { ino, .. }
            | MetaEvent::RemoveXattr { ino, .. }
            | MetaEvent::SetAclRule { ino, .. }
            | MetaEvent::RemoveAclRule { ino, .. }
            | MetaEvent::SetSymlinkTarget { ino, .. }
            | MetaEvent::SetRegularFileOverlaySlices { ino, .. }
            | MetaEvent::PublishRegularFileBaseSlices { ino, .. }
            | MetaEvent::RemoveRegularFileGraph { ino } => single(*ino)?,
            MetaEvent::AddEntry { parent, entry, .. } => {
                (!tombstoned(*parent)? && !tombstoned(entry.ino)?).then_some(Cow::Borrowed(event))
            }
            MetaEvent::RemoveEntry { parent, .. } => single(*parent)?,
            MetaEvent::RenameEntry {
                parent,
                new_parent,
                entry,
                ..
            } => {
                if tombstoned(*parent)? || tombstoned(*new_parent)? || tombstoned(entry.ino)? {
                    return Ok(());
                }
                let superseded_attr = |attr: &Option<InodeAttr>| -> MetaResult<bool> {
                    match attr {
                        Some(attr) => tombstoned(attr.ino),
                        None => Ok(false),
                    }
                };
                let MetaEvent::RenameEntry {
                    displaced_set_attr,
                    displaced_remove_attr_ino,
                    moved_dir_attr,
                    old_parent_attr,
                    new_parent_attr,
                    ..
                } = event
                else {
                    unreachable!("matched RenameEntry above");
                };
                let drop_displaced_set = superseded_attr(displaced_set_attr.as_ref())?;
                let drop_displaced_remove = match displaced_remove_attr_ino {
                    Some(ino) => tombstoned(*ino)?,
                    None => false,
                };
                let drop_moved_dir = superseded_attr(moved_dir_attr.as_ref())?;
                let drop_old_parent = superseded_attr(old_parent_attr.as_ref())?;
                let drop_new_parent = superseded_attr(new_parent_attr.as_ref())?;
                if !(drop_displaced_set
                    || drop_displaced_remove
                    || drop_moved_dir
                    || drop_old_parent
                    || drop_new_parent)
                {
                    out.push(Cow::Borrowed(event));
                    Self::push_rename_namespace_replay(base_dentries, event, out)?;
                    return Ok(());
                }
                let mut sanitized = event.clone();
                if let MetaEvent::RenameEntry {
                    displaced_set_attr,
                    displaced_remove_attr_ino,
                    moved_dir_attr,
                    old_parent_attr,
                    new_parent_attr,
                    ..
                } = &mut sanitized
                {
                    if drop_displaced_set {
                        **displaced_set_attr = None;
                    }
                    if drop_displaced_remove {
                        *displaced_remove_attr_ino = None;
                    }
                    if drop_moved_dir {
                        **moved_dir_attr = None;
                    }
                    if drop_old_parent {
                        **old_parent_attr = None;
                    }
                    if drop_new_parent {
                        **new_parent_attr = None;
                    }
                }
                out.push(Cow::Owned(sanitized));
                Self::push_rename_namespace_replay(base_dentries, event, out)?;
                return Ok(());
            }
            _ => Some(Cow::Borrowed(event)),
        };
        out.extend(replay);
        Ok(())
    }

    /// Restore both sides of the local rename. The serving rename arm restores the source
    /// only while base still binds it to the moved entry, so only that binding is removed;
    /// a foreign entry that reused the source name stays visible.
    fn push_rename_namespace_replay<'event, B: ReadableTable<&'static [u8], &'static [u8]>>(
        base_dentries: &B,
        event: &'event crate::events::MetaEvent,
        out: &mut Vec<Cow<'event, crate::events::MetaEvent>>,
    ) -> MetaResult<()> {
        if let crate::events::MetaEvent::RenameEntry {
            parent,
            name,
            new_parent,
            new_name,
            entry,
            ..
        } = event
        {
            let source_in_base =
                Self::lookup_exact_in_opened_dentry_table(base_dentries, *parent, name)?
                    .is_some_and(|base| base.ino == entry.ino && base.kind == entry.kind);
            if source_in_base {
                out.push(Cow::Owned(crate::events::MetaEvent::RemoveEntry {
                    parent: *parent,
                    name: name.clone(),
                }));
            }
            out.push(Cow::Owned(crate::events::MetaEvent::AddEntry {
                parent: *new_parent,
                name: new_name.clone(),
                entry: entry.clone(),
            }));
        }
        Ok(())
    }

    fn apply_replica_serving_state_inner(
        &self,
        events: &[crate::events::MetaEvent],
        reject_stale_ready_growth: bool,
        discard_unchanged: bool,
    ) -> MetaResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let txn = self.begin_write_txn()?;
        Self::apply_replica_serving_state_in_txn_with(
            &txn,
            events,
            reject_stale_ready_growth,
            discard_unchanged,
        )?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    /// `discard_unchanged` drops the scopes journaled for `events` when, as a whole, they
    /// left the touched inodes, dentries and file rows exactly as they were. Replaying
    /// this client's own pending events does that in steady state; the kernel already
    /// saw them, and the transaction is atomic, so intermediate states are unobservable.
    pub(crate) fn apply_replica_serving_state_in_txn_with<E: Borrow<crate::events::MetaEvent>>(
        txn: &RedbWriteTransaction<'_>,
        events: &[E],
        reject_stale_ready_growth: bool,
        discard_unchanged: bool,
    ) -> MetaResult<()> {
        let scope = if discard_unchanged {
            Self::replay_events_scope(txn, events.iter().map(Borrow::borrow))?
        } else {
            None
        };
        let invalidation_scope = txn.invalidation_scope();
        let view_before = scope
            .as_ref()
            .map(|scope| Self::replay_scope_view(txn, scope))
            .transpose()?;
        Self::apply_replica_serving_state_in_txn(txn, events, reject_stale_ready_growth)?;
        if let (Some(scope), Some(before)) = (scope.as_ref(), view_before.as_ref())
            && Self::replay_scope_view(txn, scope)? == *before
        {
            invalidation_scope.discard();
        }
        Ok(())
    }

    /// Applies replica events to the active serving tables within the supplied transaction.
    /// Uses the shared setters defined in `redb_engine/txn.rs` to write foreign mirror state;
    /// event-specific replay logic controls which updates are applied.
    pub(crate) fn apply_replica_serving_state_in_txn<E: Borrow<crate::events::MetaEvent>>(
        txn: &RedbWriteTransaction<'_>,
        events: &[E],
        reject_stale_ready_growth: bool,
    ) -> MetaResult<()> {
        let mut local_removals = ReplayLocalJournal::default();
        for event in events {
            let event = event.borrow();
            Self::invalidate_replica_event(txn, event)?;
            match event {
                crate::events::MetaEvent::SetAttr { attr } => {
                    let current = match Self::get_attr_merged_in_write_txn(txn, attr.ino) {
                        Ok(current) => Some(current),
                        Err(MetaError::InodeNotFound(_)) => None,
                        Err(err) => return Err(err),
                    };
                    if reject_stale_ready_growth
                        && let Some(current) = current.as_ref()
                        && current.kind == FileType::RegularFile
                        && current.upload_state == UploadState::Ready
                        && current.size == current.committed_size
                        && current.content_epoch == current.committed_content_epoch
                        && attr.upload_state != UploadState::Ready
                        && ((attr.mtime < current.mtime
                            && attr.ctime < current.ctime
                            && attr.committed_size > current.committed_size)
                            // Append publication grows committed size within the same epoch.
                            || (attr.content_epoch == current.content_epoch
                                && attr.committed_content_epoch < current.committed_content_epoch))
                    {
                        continue;
                    }
                    if reject_stale_ready_growth
                        && let Some(current) = current.as_ref()
                        && current.kind == FileType::RegularFile
                        && current.upload_state == UploadState::Ready
                        && current.size == current.committed_size
                        && current.content_epoch == current.committed_content_epoch
                        && attr.upload_state == UploadState::Ready
                        && attr.size == attr.committed_size
                        && (attr.content_epoch < current.content_epoch
                            || (attr.content_epoch == current.content_epoch
                                && attr.size > current.size
                                && ((attr.mtime < current.mtime && attr.ctime < current.ctime)
                                    || !Self::ready_regular_file_growth_has_payload(
                                        events.iter().map(Borrow::borrow),
                                        attr.ino,
                                        current.size,
                                        attr.size,
                                    ))))
                    {
                        continue;
                    }
                    if let Some(base_attr) =
                        Self::stale_ready_regular_file_base_attr_in_txn(txn, attr)?
                    {
                        let current = Self::get_attr_merged_in_write_txn(txn, attr.ino)?;
                        let current_is_newer = current.content_epoch > attr.content_epoch
                            || (current.content_epoch == attr.content_epoch
                                && current.size > attr.size);
                        let current_matches_ready_attr = reject_stale_ready_growth
                            && current.kind == FileType::RegularFile
                            && current.upload_state == UploadState::Ready
                            && attr.upload_state == UploadState::Ready
                            && current.size == attr.size
                            && current.committed_size == attr.committed_size
                            && current.content_epoch == attr.content_epoch
                            && current.committed_content_epoch == attr.committed_content_epoch;
                        let skip_stale_base = reject_stale_ready_growth
                            && (current_matches_ready_attr
                                || (attr.mtime > base_attr.mtime && attr.ctime > base_attr.ctime));
                        if !current_is_newer && !skip_stale_base {
                            Self::set_inode_in_txn(txn, &base_attr)?;
                            Self::clear_regular_file_serving_overlay_in_txn(txn, attr.ino)?;
                        }
                        continue;
                    }
                    Self::set_inode_in_txn(txn, attr)?;
                    if Self::ready_regular_file_attr_can_follow_base_in_txn(txn, attr)? {
                        Self::clear_regular_file_serving_overlay_in_txn(txn, attr.ino)?;
                    }
                }
                crate::events::MetaEvent::RemoveAttr { ino } => {
                    Self::remove_dentry_mirrors_for_inode_in_txn(txn, *ino)?;
                    Self::remove_inode_mirror_in_txn(txn, *ino)?;
                    Self::remove_symlink_mirror_in_txn(txn, *ino)?;

                    let current_keys =
                        Self::keys_for_inode_in_write_table(txn, XATTRS_TABLE, *ino)?;
                    for key in current_keys {
                        let name = xattr_name_from_key(&key);
                        Self::remove_xattr_mirror_in_txn(txn, *ino, name)?;
                    }
                    let base_keys =
                        Self::keys_for_inode_in_write_table(txn, BASE_XATTRS_TABLE, *ino)?;
                    for key in base_keys {
                        let name = xattr_name_from_key(&key);
                        Self::remove_xattr_mirror_in_txn(txn, *ino, name)?;
                    }

                    let prefix = acl_rule_prefix(*ino);
                    let mut acl_rules = txn
                        .open_table(ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let acl_rule_keys = collect_prefix_keys(
                        &acl_rules,
                        &prefix,
                        #[cfg(test)]
                        super::RedbRowReadScan::RemoveAttrServingAclRules,
                    )?;
                    for key in acl_rule_keys {
                        acl_rules.remove(key.as_slice())?;
                    }
                    Self::clear_regular_file_graph_in_txn(txn, *ino)?;
                    Self::clear_regular_file_serving_overlay_in_txn(txn, *ino)?;
                }
                crate::events::MetaEvent::AddEntry {
                    parent,
                    name,
                    entry,
                } => {
                    Self::set_dentry_in_txn(txn, *parent, name, entry)?;
                }
                crate::events::MetaEvent::RemoveEntry { parent, name } => {
                    Self::remove_dentry_overlay_in_txn(txn, *parent, name)?;
                }
                crate::events::MetaEvent::RenameEntry {
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
                    Self::reconcile_serving_rename_from_base_in_txn(
                        txn,
                        *parent,
                        name,
                        *new_parent,
                        new_name,
                        entry,
                        &mut local_removals,
                    )?;

                    if let Some(attr) = displaced_set_attr.as_ref() {
                        Self::set_inode_in_txn(txn, attr)?;
                    }
                    if let Some(ino) = displaced_remove_attr_ino {
                        Self::remove_inode_mirror_in_txn(txn, *ino)?;
                    }
                    if let Some(attr) = moved_dir_attr.as_ref() {
                        Self::set_inode_in_txn(txn, attr)?;
                    }
                    if let Some(attr) = old_parent_attr.as_ref() {
                        Self::set_inode_in_txn(txn, attr)?;
                    }
                    if let Some(attr) = new_parent_attr.as_ref() {
                        Self::set_inode_in_txn(txn, attr)?;
                    }
                }
                crate::events::MetaEvent::SetSymlinkTarget { ino, target } => {
                    Self::set_symlink_in_txn(txn, *ino, target)?;
                }
                crate::events::MetaEvent::SetXattr {
                    ino, name, value, ..
                } => {
                    Self::set_xattr_in_txn(txn, *ino, name, value)?;
                }
                crate::events::MetaEvent::RemoveXattr { ino, name } => {
                    Self::remove_xattr_mirror_in_txn(txn, *ino, name)?;
                }
                crate::events::MetaEvent::SetAclRule {
                    ino,
                    principal,
                    access_level,
                } => {
                    let mut rules = txn
                        .open_table(ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let mut tombstones = txn
                        .open_table(ACL_RULE_TOMBSTONES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    tombstones.remove(key.as_slice())?;
                    rules
                        .insert(key.as_slice(), *access_level as u8)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveAclRule { ino, principal } => {
                    let mut rules = txn
                        .open_table(ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let mut tombstones = txn
                        .open_table(ACL_RULE_TOMBSTONES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    rules.remove(key.as_slice())?;
                    tombstones
                        .insert(key.as_slice(), 1)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices } => {
                    let mut current = Self::regular_file_chunk_states_for_inode_in_write_txn(
                        txn,
                        REGULAR_FILE_OVERLAY_TABLE,
                        *ino,
                    )?;
                    for (chunk_id, slices) in chunk_slices {
                        // Pending-state replay can apply a stale whole-chunk snapshot after an
                        // upload completion. Immutable payload completion is monotonic, so keep
                        // the newer local state for the same payload reference.
                        let mut slices = slices.clone();
                        if let Some(current) = current.get(chunk_id) {
                            let completed =
                                super::payload_objects::CompletedOverlayPayloads::new(current);
                            for slice in &mut slices {
                                completed.preserve(slice);
                            }
                        }
                        let next = Self::set_regular_file_chunk_state_in_txn(
                            txn,
                            REGULAR_FILE_OVERLAY_TABLE,
                            true,
                            *ino,
                            *chunk_id,
                            &slices,
                        )?;
                        if let Some(next) = next {
                            current.insert(*chunk_id, next);
                        } else {
                            current.remove(chunk_id);
                        }
                    }
                }
                crate::events::MetaEvent::PublishRegularFileBaseSlices { ino, .. } => {
                    if Self::serving_overlay_can_follow_base_publish_in_txn(txn, *ino)? {
                        Self::clear_regular_file_serving_overlay_in_txn(txn, *ino)?;
                    }
                }
                crate::events::MetaEvent::RemoveRegularFileGraph { .. }
                | crate::events::MetaEvent::AddPendingDeletion { .. }
                | crate::events::MetaEvent::RemovePendingDeletion { .. } => {}
            }
        }

        Ok(())
    }

    /// Reconcile stale regular-file serving mirrors to the imported base view
    /// after the PG layer has proven that no local event-log overlay still owns
    /// those inodes.
    pub fn reconcile_regular_file_serving_from_base(
        &self,
        inodes: &HashSet<Ino>,
    ) -> MetaResult<bool> {
        self.reconcile_regular_file_serving_from_base_with_protected_dentries(
            inodes,
            &HashSet::new(),
        )
    }

    /// Reconcile stale regular-file serving mirrors while preserving dentry keys
    /// that the PG layer still considers local overlay state.
    pub fn reconcile_regular_file_serving_from_base_with_protected_dentries(
        &self,
        inodes: &HashSet<Ino>,
        protected_dentry_keys: &HashSet<Vec<u8>>,
    ) -> MetaResult<bool> {
        if inodes.is_empty() {
            return Ok(false);
        }
        let txn = self.begin_write_txn()?;
        let changed =
            Self::reconcile_regular_file_serving_from_base_with_protected_dentries_in_txn(
                &txn,
                inodes,
                protected_dentry_keys,
            )?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(changed)
    }

    pub(crate) fn reconcile_regular_file_serving_from_base_with_protected_dentries_in_txn(
        txn: &RedbWriteTransaction<'_>,
        inodes: &HashSet<Ino>,
        protected_dentry_keys: &HashSet<Vec<u8>>,
    ) -> MetaResult<bool> {
        if inodes.is_empty() {
            return Ok(false);
        }
        let mut changed = false;
        let base_attrs = {
            let base = txn
                .open_table(BASE_INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            inodes
                .iter()
                .map(|ino| {
                    let attr = base
                        .get(*ino)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .map(|value| decode::<InodeAttr>(value.value()))
                        .transpose()?;
                    Ok((*ino, attr))
                })
                .collect::<MetaResult<HashMap<Ino, Option<InodeAttr>>>>()?
        };

        let mut local_journal = ReplayLocalJournal::default();
        for ino in inodes {
            let invalidation_scope = txn.invalidation_scope();
            let view_before = Self::serving_view_of_inode(txn, *ino)?;
            Self::invalidate_replica_inode(txn, *ino, false)?;
            let base_attr = base_attrs.get(ino).and_then(|attr| attr.as_ref());

            match base_attr {
                Some(attr) if attr.kind == FileType::RegularFile => {
                    let content_changed = Self::replica_content_identity_changed(txn, attr)?;
                    Self::set_inode_in_txn(txn, attr)?;
                    Self::remove_stale_dentries_for_base_entry(
                        txn,
                        &DirEntryValue {
                            ino: *ino,
                            kind: attr.kind,
                        },
                        protected_dentry_keys,
                        &mut local_journal,
                    )?;
                    Self::install_base_dentries_for_entry_in_txn(
                        txn,
                        &DirEntryValue {
                            ino: *ino,
                            kind: attr.kind,
                        },
                        protected_dentry_keys,
                    )?;
                    Self::invalidate_replica_inode(txn, *ino, content_changed)?;
                    if attr.upload_state == UploadState::Ready
                        && attr.content_epoch == attr.committed_content_epoch
                        && attr.size == attr.committed_size
                    {
                        Self::clear_regular_file_serving_overlay_in_txn(txn, *ino)?;
                    }
                    changed = true;
                }
                Some(_) => {}
                None => {
                    Self::remove_dentry_mirrors_for_inode_in_txn(txn, *ino)?;
                    Self::remove_inode_mirror_in_txn(txn, *ino)?;
                    Self::clear_regular_file_serving_overlay_in_txn(txn, *ino)?;
                    changed = true;
                }
            }
            // Reconciling toward base is routinely a no-op for this client's own
            // acknowledged writes; only a real change to what the mount serves
            // can leave cached kernel state stale.
            if Self::serving_view_of_inode(txn, *ino)? == view_before {
                invalidation_scope.discard();
            }
        }

        Ok(changed)
    }

    /// What replaying `events` can touch, when every event is a plain one whose
    /// effect is fully captured by inode, dentry and file-row state. Any other
    /// event returns `None`, which keeps the intents.
    fn replay_events_scope<'event>(
        txn: &RedbWriteTransaction<'_>,
        events: impl IntoIterator<Item = &'event crate::events::MetaEvent>,
    ) -> MetaResult<Option<ReplayScope>> {
        use crate::events::MetaEvent;
        let mut scope = ReplayScope::default();
        for event in events {
            match event {
                MetaEvent::SetAttr { attr } => {
                    scope.inodes.insert(attr.ino);
                }
                MetaEvent::SetRegularFileOverlaySlices { ino, .. }
                | MetaEvent::PublishRegularFileBaseSlices { ino, .. }
                | MetaEvent::RemoveRegularFileGraph { ino } => {
                    scope.inodes.insert(*ino);
                }
                MetaEvent::AddEntry {
                    parent,
                    name,
                    entry,
                } => {
                    scope.inodes.extend([*parent, entry.ino]);
                    scope.dentry_keys.insert(encode_dentry_key(*parent, name));
                }
                MetaEvent::RemoveEntry { parent, name } => {
                    scope.inodes.insert(*parent);
                    scope.dentry_keys.insert(encode_dentry_key(*parent, name));
                }
                // A removed directory takes its descendants' names with it.
                MetaEvent::RemoveAttr { ino } => {
                    match Self::get_attr_merged_in_write_txn(txn, *ino) {
                        Ok(attr) if attr.kind == FileType::Directory => return Ok(None),
                        Ok(_) | Err(MetaError::InodeNotFound(_)) => {}
                        Err(error) => return Err(error),
                    }
                    scope.inodes.insert(*ino);
                }
                MetaEvent::AddPendingDeletion { .. } | MetaEvent::RemovePendingDeletion { .. } => {}
                _ => return Ok(None),
            }
        }
        Ok(Some(scope))
    }

    fn replay_scope_view(
        txn: &RedbWriteTransaction<'_>,
        scope: &ReplayScope,
    ) -> MetaResult<ReplayScopeView> {
        let mut inodes = Vec::with_capacity(scope.inodes.len());
        for ino in &scope.inodes {
            inodes.push((
                Self::serving_view_of_inode(txn, *ino)?,
                Self::regular_file_rows_snapshot(txn, *ino)?,
            ));
        }
        let mut dentries = Vec::with_capacity(scope.dentry_keys.len());
        for key in &scope.dentry_keys {
            dentries.push(Self::invalidation_binding(txn, key)?);
        }
        Ok(ReplayScopeView { inodes, dentries })
    }

    /// Raw overlay and base chunk rows of `ino`, so content changes show up.
    fn regular_file_rows_snapshot(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<Vec<Vec<u8>>> {
        let prefix = regular_file_chunk_prefix(ino);
        let mut rows = Vec::new();
        for definition in [REGULAR_FILE_OVERLAY_TABLE, REGULAR_FILE_BASE_TABLE] {
            let table = txn
                .open_table(definition)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            for row in table
                .range(prefix.as_ref()..)
                .map_err(|error| MetaError::Storage(error.to_string()))?
            {
                let (key, value) = row.map_err(|error| MetaError::Storage(error.to_string()))?;
                if !key.value().starts_with(prefix.as_ref()) {
                    break;
                }
                let mut bytes = key.value().to_vec();
                bytes.extend_from_slice(value.value());
                rows.push(bytes);
            }
            rows.push(Vec::new());
        }
        Ok(rows)
    }

    /// The merged attributes and dentries the foreground view exposes for `ino`.
    fn serving_view_of_inode(
        txn: &RedbWriteTransaction<'_>,
        ino: Ino,
    ) -> MetaResult<ServingInodeView> {
        let attr = match Self::get_attr_merged_in_write_txn(txn, ino) {
            Ok(attr) => Some(attr),
            Err(MetaError::InodeNotFound(_)) => None,
            Err(error) => return Err(error),
        };
        let mut dentries = BTreeMap::new();
        for index in [DENTRIES_BY_INODE_TABLE, BASE_DENTRIES_BY_INODE_TABLE] {
            for key in Self::dentry_keys_for_inode_in_write_index(txn, index, ino)? {
                if let Some(entry) = Self::invalidation_binding(txn, &key)?
                    && entry.ino == ino
                {
                    dentries.insert(key, entry);
                }
            }
        }
        Ok((attr, dentries))
    }

    #[cfg(test)]
    pub(crate) fn record_regular_file_serving_reconcile_call_for_test() {
        REGULAR_FILE_SERVING_RECONCILE_CALLS
            .set(REGULAR_FILE_SERVING_RECONCILE_CALLS.get().saturating_add(1));
    }

    /// Reconcile exact dentry keys from the authoritative base namespace into
    /// serving after a local namespace overlay for those keys has been flushed.
    pub fn reconcile_serving_dentries_from_base(
        &self,
        dentry_keys: &HashSet<Vec<u8>>,
        protected_dentry_keys: &HashSet<Vec<u8>>,
    ) -> MetaResult<bool> {
        if dentry_keys.is_empty() {
            return Ok(false);
        }

        let txn = self.begin_write_txn()?;
        let changed = Self::reconcile_serving_dentries_from_base_in_txn(
            &txn,
            dentry_keys,
            protected_dentry_keys,
        )?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(changed)
    }

    pub(crate) fn serving_dentry_keys_for_inode_in_txn(
        txn: &RedbWriteTransaction<'_>,
        ino: Ino,
    ) -> MetaResult<Vec<Vec<u8>>> {
        Self::dentry_keys_for_inode_in_write_index(txn, DENTRIES_BY_INODE_TABLE, ino)
    }

    pub(crate) fn reconcile_serving_dentries_from_base_in_txn(
        txn: &RedbWriteTransaction<'_>,
        dentry_keys: &HashSet<Vec<u8>>,
        protected_dentry_keys: &HashSet<Vec<u8>>,
    ) -> MetaResult<bool> {
        if dentry_keys.is_empty() {
            return Ok(false);
        }
        let mut changed = false;
        for key in dentry_keys {
            if protected_dentry_keys.contains(key) {
                continue;
            }
            let (parent, name) = decode_dentry_key(key)?;
            let base_entry =
                Self::lookup_exact_in_write_table(txn, BASE_DENTRIES_TABLE, parent, name)?;
            let serving_entry =
                Self::lookup_exact_in_write_table(txn, DENTRIES_TABLE, parent, name)?;
            let binding_before = Self::invalidation_binding(txn, key)?;
            // Most keys already serve their base binding, so write first and
            // derive intents only for a binding that actually changed; they
            // match what journaling before the write would have recorded.
            txn.without_invalidations(|| match &base_entry {
                Some(entry) => Self::set_dentry_in_txn(txn, parent, name, entry),
                None => Self::remove_dentry_mirror_in_txn(txn, parent, name),
            })?;
            if Self::invalidation_binding(txn, key)? != binding_before
                && !txn.invalidations_suppressed()
            {
                #[cfg(test)]
                record_replica_invalidation_derivation();
                txn.invalidate(InvalidationIntent::Entry {
                    parent,
                    name: name.to_vec(),
                    ancestors: Self::invalidation_ancestors(txn, parent)?,
                    ino: binding_before.as_ref().map(|entry| entry.ino),
                });
                txn.invalidate(InvalidationIntent::Directory { ino: parent });
                txn.invalidate(InvalidationIntent::Attributes { ino: parent });
                for displaced in [&serving_entry, &base_entry].into_iter().flatten() {
                    txn.invalidate(InvalidationIntent::Attributes { ino: displaced.ino });
                }
                if let Some(entry) = &base_entry {
                    Self::invalidate_replica_inode(txn, entry.ino, false)?;
                }
            }
            changed = true;
        }

        Ok(changed)
    }

    fn serving_overlay_can_follow_base_publish_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<bool> {
        let attr = match Self::get_attr_merged_in_write_txn(txn, ino) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => return Ok(true),
            Err(err) => return Err(err),
        };
        Ok(attr.kind != FileType::RegularFile
            || (attr.content_epoch == attr.committed_content_epoch
                && attr.size == attr.committed_size))
    }

    fn ready_regular_file_attr_can_follow_base_in_txn(
        txn: &redb::WriteTransaction,
        attr: &InodeAttr,
    ) -> MetaResult<bool> {
        if attr.kind != FileType::RegularFile
            || attr.upload_state != UploadState::Ready
            || attr.content_epoch != attr.committed_content_epoch
            || attr.size != attr.committed_size
        {
            return Ok(false);
        }

        if attr.committed_size == 0 {
            return Ok(true);
        }

        let base = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(attr.ino);
        let mut rows = base
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        match rows.next() {
            Some(row) => {
                let (key, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                Ok(key.value().starts_with(prefix.as_ref()))
            }
            None => Ok(false),
        }
    }

    fn ready_regular_file_growth_has_payload<'event>(
        events: impl IntoIterator<Item = &'event crate::events::MetaEvent>,
        ino: Ino,
        required_start: u64,
        required_end: u64,
    ) -> bool {
        if required_end <= required_start {
            return false;
        }

        let mut ranges = Vec::new();
        for event in events {
            let crate::events::MetaEvent::PublishRegularFileBaseSlices {
                ino: event_ino,
                chunk_slices,
            } = event
            else {
                continue;
            };
            if *event_ino != ino {
                continue;
            }
            for (chunk_id, slices) in chunk_slices {
                let chunk_start = chunk_start_offset(*chunk_id, DEFAULT_CHUNK_SIZE);
                for slice in slices {
                    if slice.payload.is_none() {
                        continue;
                    }
                    let start = chunk_start
                        .saturating_add(u64::from(slice.range.start))
                        .max(required_start);
                    let end = chunk_start
                        .saturating_add(u64::from(slice.range.end))
                        .min(required_end);
                    if start < end {
                        ranges.push((start, end));
                    }
                }
            }
        }

        ranges.sort_unstable_by_key(|(start, end)| (*start, *end));
        let mut covered_until = required_start;
        for (start, end) in ranges {
            if start > covered_until {
                return false;
            }
            covered_until = covered_until.max(end);
            if covered_until >= required_end {
                return true;
            }
        }
        false
    }

    fn stale_ready_regular_file_base_attr_in_txn(
        txn: &redb::WriteTransaction,
        attr: &InodeAttr,
    ) -> MetaResult<Option<InodeAttr>> {
        if attr.kind != FileType::RegularFile || attr.upload_state != UploadState::Ready {
            return Ok(None);
        }

        let base = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let Some(base_attr) = Self::get_attr_from_opened_table(&base, attr.ino)? else {
            return Ok(None);
        };

        Ok((base_attr.kind == FileType::RegularFile
            && (base_attr.committed_content_epoch > attr.committed_content_epoch
                || (base_attr.committed_content_epoch == attr.committed_content_epoch
                    && base_attr.committed_size > attr.committed_size)))
            .then_some(base_attr))
    }

    fn clear_regular_file_serving_overlay_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        Self::clear_regular_file_chunk_rows_in_txn(txn, REGULAR_FILE_OVERLAY_TABLE, ino)?;
        Self::clear_regular_file_pending_compaction_rows_in_txn(txn, ino)?;
        Ok(())
    }

    /// Reconcile a rename in the serving overlay using the authoritative base namespace.
    pub fn reconcile_serving_rename_from_base(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
        entry: &DirEntryValue,
    ) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
        Self::reconcile_serving_rename_from_base_in_txn(
            &txn,
            parent,
            name,
            new_parent,
            new_name,
            entry,
            &mut ReplayLocalJournal::default(),
        )?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Reconciles serving state toward base for this client's own
    /// acknowledged renames and keeps their intents only when the reconcile
    /// changed what the serving view exposes.
    ///
    /// The mounted kernel already performed these renames through the local
    /// serving view, so re-announcing an unchanged result would only purge
    /// cache state and mark the page as a serving correction. The scope covers
    /// both names and the moved inode with every name bound to it, which is
    /// everything the rename reconcile may rewrite.
    pub(crate) fn reconcile_own_serving_renames_from_base_in_txn<'event>(
        txn: &RedbWriteTransaction<'_>,
        events: impl IntoIterator<Item = &'event crate::events::MetaEvent>,
        local_removals: &mut ReplayLocalJournal,
    ) -> MetaResult<bool> {
        let renames: Vec<_> = events
            .into_iter()
            .filter(|event| matches!(event, crate::events::MetaEvent::RenameEntry { .. }))
            .collect();
        if renames.is_empty() {
            return Ok(false);
        }
        let mut scope = ReplayScope::default();
        for event in &renames {
            if let crate::events::MetaEvent::RenameEntry {
                parent,
                name,
                new_parent,
                new_name,
                entry,
                ..
            } = event
            {
                scope.inodes.extend([*parent, *new_parent, entry.ino]);
                scope.dentry_keys.insert(encode_dentry_key(*parent, name));
                scope
                    .dentry_keys
                    .insert(encode_dentry_key(*new_parent, new_name));
            }
        }
        let before = Self::replay_scope_view(txn, &scope)?;
        // Descendant paths are not part of the captured view, and their
        // historical ancestry is what the kernel cached, so a directory
        // rename derives them before writing and keeps them only on a real
        // change.
        let descendant_scope = txn.invalidation_scope();
        for event in &renames {
            if let crate::events::MetaEvent::RenameEntry { entry, .. } = event
                && entry.kind == FileType::Directory
            {
                Self::invalidate_directory_descendants(txn, entry.ino)?;
            }
        }
        // Reconcile without deriving the remaining intents up front: for this
        // client's own renames the view almost never changes. A real
        // correction journals the intents the reconcile would have derived
        // before writing, from the captured pre-write view.
        txn.without_invalidations(|| -> MetaResult<()> {
            for event in &renames {
                if let crate::events::MetaEvent::RenameEntry {
                    parent,
                    name,
                    new_parent,
                    new_name,
                    entry,
                    ..
                } = event
                {
                    Self::reconcile_serving_rename_from_base_in_txn(
                        txn,
                        *parent,
                        name,
                        *new_parent,
                        new_name,
                        entry,
                        local_removals,
                    )?;
                }
            }
            Ok(())
        })?;
        if Self::replay_scope_view(txn, &scope)? != before && !txn.invalidations_suppressed() {
            Self::journal_rename_reconcile_intents(txn, &renames, &scope, &before)?;
        } else {
            descendant_scope.discard();
        }
        Ok(true)
    }

    /// Reduces pending namespace events to their final binding per dentry.
    /// Inode removal events clear every indexed alias, including base names
    /// whose original AddEntry is no longer inside the pending event range.
    pub(crate) fn pending_local_namespace_bindings_in_txn(
        txn: &redb::WriteTransaction,
        events: &[crate::events::MetaEvent],
    ) -> MetaResult<PendingLocalNamespaceState> {
        use crate::events::MetaEvent;

        let mut bindings = HashMap::new();
        let mut removed_inodes = HashSet::new();
        for event in events {
            match event {
                MetaEvent::SetAttr { attr } => {
                    removed_inodes.remove(&attr.ino);
                }
                MetaEvent::AddEntry {
                    parent,
                    name,
                    entry,
                } => {
                    bindings.insert(encode_dentry_key(*parent, name), Some(entry.clone()));
                }
                MetaEvent::RemoveEntry { parent, name } => {
                    bindings.insert(encode_dentry_key(*parent, name), None);
                }
                MetaEvent::RenameEntry {
                    parent,
                    name,
                    new_parent,
                    new_name,
                    entry,
                    displaced_remove_attr_ino,
                    ..
                } => {
                    if let Some(ino) = displaced_remove_attr_ino {
                        Self::remove_pending_bindings_for_inode_in_txn(txn, *ino, &mut bindings)?;
                        removed_inodes.insert(*ino);
                    }
                    bindings.insert(encode_dentry_key(*parent, name), None);
                    bindings.insert(
                        encode_dentry_key(*new_parent, new_name),
                        Some(entry.clone()),
                    );
                }
                MetaEvent::RemoveAttr { ino } => {
                    Self::remove_pending_bindings_for_inode_in_txn(txn, *ino, &mut bindings)?;
                    removed_inodes.insert(*ino);
                }
                _ => {}
            }
        }

        Ok(PendingLocalNamespaceState {
            bindings,
            removed_inodes,
        })
    }

    fn remove_pending_bindings_for_inode_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        bindings: &mut HashMap<Vec<u8>, Option<DirEntryValue>>,
    ) -> MetaResult<()> {
        for binding in bindings.values_mut() {
            if binding.as_ref().is_some_and(|entry| entry.ino == ino) {
                *binding = None;
            }
        }
        let mut keys =
            Self::dentry_keys_for_inode_in_write_index(txn, DENTRIES_BY_INODE_TABLE, ino)?;
        keys.extend(Self::dentry_keys_for_inode_in_write_index(
            txn,
            BASE_DENTRIES_BY_INODE_TABLE,
            ino,
        )?);
        for key in keys {
            if bindings.contains_key(&key) {
                continue;
            }
            // A base index row can be stale after a published rename displaced
            // this inode; only clear names that still resolve to it.
            let (parent, name) = decode_dentry_key(&key)?;
            if Self::lookup_merged_exact_in_write_txn(txn, parent, name)?
                .is_some_and(|entry| entry.ino == ino)
            {
                bindings.insert(key, None);
            }
        }
        Ok(())
    }

    /// Reapplies the final namespace bindings from the still-pending local
    /// suffix after an earlier self prefix is mirrored into the base view.
    /// Rows that already match are skipped; returns whether anything changed.
    pub(crate) fn rebase_pending_local_namespace_in_txn(
        txn: &RedbWriteTransaction<'_>,
        state: &PendingLocalNamespaceState,
    ) -> MetaResult<bool> {
        let mut changed = false;
        for (key, entry) in &state.bindings {
            let (parent, name) = decode_dentry_key(key)?;
            if Self::lookup_merged_exact_in_write_txn(txn, parent, name)?.as_ref() == entry.as_ref()
            {
                continue;
            }
            if let Some(entry) = entry {
                Self::set_dentry_in_txn(txn, parent, name, entry)?;
            } else {
                Self::remove_dentry_overlay_in_txn(txn, parent, name)?;
            }
            changed = true;
        }
        for &ino in &state.removed_inodes {
            match Self::get_attr_merged_in_write_txn(txn, ino) {
                Ok(_) => {
                    Self::remove_inode_overlay_in_txn(txn, ino)?;
                    changed = true;
                }
                Err(MetaError::InodeNotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(changed)
    }

    /// Journals, after the write, the name and inode intents
    /// [`Self::reconcile_serving_rename_from_base_in_txn`] derives before it,
    /// using the bindings captured in `before`. Directory descendants are
    /// derived by the caller before the write.
    fn journal_rename_reconcile_intents(
        txn: &RedbWriteTransaction<'_>,
        renames: &[&crate::events::MetaEvent],
        scope: &ReplayScope,
        before: &ReplayScopeView,
    ) -> MetaResult<()> {
        let bindings_before: HashMap<&Vec<u8>, &Option<DirEntryValue>> =
            scope.dentry_keys.iter().zip(&before.dentries).collect();
        let inode_names_before: HashMap<Ino, &BTreeMap<Vec<u8>, DirEntryValue>> = scope
            .inodes
            .iter()
            .zip(&before.inodes)
            .map(|(ino, ((_, dentries), _))| (*ino, dentries))
            .collect();
        let journal_entry = |parent: Ino, name: &[u8], ino: Option<Ino>| -> MetaResult<()> {
            #[cfg(test)]
            record_replica_invalidation_derivation();
            txn.invalidate(InvalidationIntent::Entry {
                parent,
                name: name.to_vec(),
                ancestors: Self::invalidation_ancestors(txn, parent)?,
                ino,
            });
            txn.invalidate(InvalidationIntent::Directory { ino: parent });
            txn.invalidate(InvalidationIntent::Attributes { ino: parent });
            if let Some(ino) = ino {
                txn.invalidate(InvalidationIntent::Attributes { ino });
            }
            Ok(())
        };
        for event in renames {
            let crate::events::MetaEvent::RenameEntry {
                parent,
                name,
                new_parent,
                new_name,
                entry,
                ..
            } = event
            else {
                continue;
            };
            for (parent, name) in [(*parent, name), (*new_parent, new_name)] {
                let key = encode_dentry_key(parent, name);
                let ino = bindings_before
                    .get(&key)
                    .and_then(|binding| binding.as_ref())
                    .map(|binding| binding.ino)
                    .or(Some(entry.ino));
                journal_entry(parent, name, ino)?;
            }
            txn.invalidate(InvalidationIntent::Attributes { ino: entry.ino });
            if let Some(names) = inode_names_before.get(&entry.ino) {
                for key in names.keys() {
                    let (parent, name) = decode_dentry_key(key)?;
                    journal_entry(parent, name, Some(entry.ino))?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn reconcile_serving_rename_from_base_in_txn(
        txn: &RedbWriteTransaction<'_>,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
        entry: &DirEntryValue,
        local_removals: &mut ReplayLocalJournal,
    ) -> MetaResult<()> {
        if entry.kind == FileType::Directory {
            Self::invalidate_directory_descendants(txn, entry.ino)?;
        }
        Self::invalidate_replica_entry(txn, parent, name, Some(entry.ino))?;
        Self::invalidate_replica_entry(txn, new_parent, new_name, Some(entry.ino))?;
        Self::invalidate_replica_inode(txn, entry.ino, false)?;

        Self::reconcile_serving_rename_from_base_in_write_txn(
            txn,
            parent,
            name,
            new_parent,
            new_name,
            entry,
            local_removals,
        )?;

        Ok(())
    }

    fn reconcile_serving_rename_from_base_in_write_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
        entry: &DirEntryValue,
        local_removals: &mut ReplayLocalJournal,
    ) -> MetaResult<()> {
        let src_in_base =
            Self::lookup_exact_in_write_table(txn, BASE_DENTRIES_TABLE, parent, name)?
                .map(|value| value.ino == entry.ino && value.kind == entry.kind)
                .unwrap_or(false);
        let dst_in_base =
            Self::lookup_exact_in_write_table(txn, BASE_DENTRIES_TABLE, new_parent, new_name)?
                .map(|value| value.ino == entry.ino && value.kind == entry.kind)
                .unwrap_or(false);
        Self::remove_stale_dentries_for_base_entry(txn, entry, &HashSet::new(), local_removals)?;

        if dst_in_base {
            let destination_key = encode_dentry_key(new_parent, new_name);
            // The base rename winner moved this logical source to the
            // destination.  Remove any serving entry still occupying the old
            // source name, even if it points at a local overlay inode; payload
            // and inode protection are handled separately from namespace
            // visibility.
            Self::remove_dentry_mirror_in_txn(txn, parent, name)?;
            if local_removals.is_removed(txn, &destination_key)? {
                Self::remove_dentry_overlay_in_txn(txn, new_parent, new_name)?;
            } else {
                Self::set_dentry_in_txn(txn, new_parent, new_name, entry)?;
            }
        } else if src_in_base {
            Self::remove_dentry_mirror_if_matches_in_txn(txn, new_parent, new_name, entry)?;
            Self::set_dentry_in_txn(txn, parent, name, entry)?;
        } else {
            Self::remove_dentry_mirror_if_matches_in_txn(txn, parent, name, entry)?;
            Self::remove_dentry_mirror_if_matches_in_txn(txn, new_parent, new_name, entry)?;
        }

        Ok(())
    }

    /// Reduce the dentry baseline and the whole retained local journal into
    /// per-key local mutation state. The result answers any dentry key.
    fn reduce_local_dentry_mutations_in_txn(
        txn: &redb::WriteTransaction,
    ) -> MetaResult<LocalDentryMutationReducer> {
        let counters = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let end_seq = counters
            .get(crate::schema::counters::NEXT_EVENT_SEQ)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| value.value())
            .unwrap_or(1);
        let pruned_through = counters
            .get(crate::schema::counters::LOCAL_EVENT_LOG_PRUNED_THROUGH_SEQ)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| value.value())
            .unwrap_or(0);
        let baseline_version = counters
            .get(crate::schema::counters::LOCAL_DENTRY_MUTATION_BASELINE_VERSION)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| value.value());
        drop(counters);
        Self::ensure_pruned_dentry_baseline(pruned_through, baseline_version)?;

        let baseline = txn
            .open_table(LOCAL_DENTRY_MUTATIONS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut reducer = LocalDentryMutationReducer::default();
        #[cfg(test)]
        LOCAL_DENTRY_JOURNAL_SCANS.with(|count| count.set(count.get() + 1));
        let events = txn
            .open_table(EVENT_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let start_seq = events
            .first()
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|(seq, _)| seq.value())
            .unwrap_or(end_seq);
        if end_seq <= start_seq {
            return Ok(reducer);
        }
        for row in events
            .range(start_seq..end_seq)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_seq, bytes) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let event = crate::events::decode_event(bytes.value())?;
            reducer.apply_retained_to_table(&baseline, &event)?;
        }
        Ok(reducer)
    }

    fn remove_dentry_mirror_if_matches_in_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
        entry: &DirEntryValue,
    ) -> MetaResult<()> {
        let matches = Self::lookup_exact_in_write_table(txn, DENTRIES_TABLE, parent, name)?
            .map(|current| current.ino == entry.ino && current.kind == entry.kind)
            .unwrap_or(false);
        if matches {
            Self::remove_dentry_mirror_in_txn(txn, parent, name)?;
        }
        Ok(())
    }

    fn remove_stale_dentries_for_base_entry(
        txn: &redb::WriteTransaction,
        entry: &DirEntryValue,
        protected_dentry_keys: &HashSet<Vec<u8>>,
        local_journal: &mut ReplayLocalJournal,
    ) -> MetaResult<()> {
        let keys =
            Self::dentry_keys_for_inode_in_write_index(txn, DENTRIES_BY_INODE_TABLE, entry.ino)?;
        let current = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut candidates = Vec::new();
        for key in keys {
            if protected_dentry_keys.contains(&key) {
                continue;
            }

            // Keys come from the by-inode index, so they are exact by construction.
            let (parent, name) = decode_dentry_key(&key)?;
            let current_matches =
                Self::lookup_exact_in_opened_dentry_table(&current, parent, name)?
                    .map(|value| value.ino == entry.ino && value.kind == entry.kind)
                    .unwrap_or(false);
            if !current_matches {
                continue;
            }

            let base_matches = Self::lookup_exact_in_opened_dentry_table(&base, parent, name)?
                .map(|value| value.ino == entry.ino && value.kind == entry.kind)
                .unwrap_or(false);
            if !base_matches {
                candidates.push(key);
            }
        }
        drop(base);
        drop(current);

        if candidates.is_empty() {
            return Ok(());
        }

        let protected = local_journal.pending_aliases(txn, entry)?.clone();
        for key in candidates {
            if protected.contains(&key) {
                continue;
            }
            let (parent, name) = decode_dentry_key(&key)?;
            Self::remove_dentry_mirror_in_txn(txn, parent, name)?;
        }
        Ok(())
    }

    fn install_base_dentries_for_entry_in_txn(
        txn: &redb::WriteTransaction,
        entry: &DirEntryValue,
        protected_dentry_keys: &HashSet<Vec<u8>>,
    ) -> MetaResult<()> {
        let keys = Self::base_dentry_keys_for_rename_entry(txn, entry)?;
        for key in keys {
            if protected_dentry_keys.contains(&key) {
                continue;
            }
            let (parent, name) = decode_dentry_key(&key)?;
            Self::set_dentry_in_txn(txn, parent, name, entry)?;
        }
        Ok(())
    }

    // Pending local AddEntry aliases are local namespace state, not stale
    // remote rename losers, so foreign rename reconciliation must preserve them.
    fn reduce_pending_local_aliases_in_txn(
        txn: &redb::WriteTransaction,
    ) -> MetaResult<PendingLocalAliases> {
        let counters = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let start_seq = counters
            .get(crate::schema::counters::LOCAL_PG_FLUSHED_EVENT_SEQ)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| value.value())
            .unwrap_or(1);
        let end_seq = counters
            .get(crate::schema::counters::NEXT_EVENT_SEQ)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| value.value())
            .unwrap_or(1);
        drop(counters);

        let mut aliases = PendingLocalAliases::default();
        if end_seq <= start_seq {
            return Ok(aliases);
        }

        let events = txn
            .open_table(EVENT_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for row in events
            .range(start_seq..end_seq)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            #[cfg(test)]
            PENDING_LOCAL_ALIAS_EVENT_SCAN_COUNT
                .set(PENDING_LOCAL_ALIAS_EVENT_SCAN_COUNT.get().saturating_add(1));
            let (_seq, bytes) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            aliases.apply(&crate::events::decode_event(bytes.value())?);
        }
        Ok(aliases)
    }

    fn base_dentry_keys_for_rename_entry(
        txn: &redb::WriteTransaction,
        entry: &DirEntryValue,
    ) -> MetaResult<Vec<Vec<u8>>> {
        let keys = Self::dentry_keys_for_inode_in_write_index(
            txn,
            BASE_DENTRIES_BY_INODE_TABLE,
            entry.ino,
        )?;
        let mut matching = Vec::new();
        let base = txn
            .open_table(BASE_DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for key in keys {
            let (parent, name) = decode_dentry_key(&key)?;
            let base_matches = Self::lookup_exact_in_opened_dentry_table(&base, parent, name)?
                .map(|value| value.ino == entry.ino && value.kind == entry.kind)
                .unwrap_or(false);
            if base_matches {
                matching.push(key);
            }
        }
        Ok(matching)
    }

    /// Return the lowest sequence number still present in the local event log.
    #[cfg(test)]
    pub(crate) fn first_event_seq(&self) -> MetaResult<Option<u64>> {
        let txn = self.begin_read_txn()?;
        let table = txn
            .open_table(EVENT_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let first = table
            .first()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(first.map(|(seq, _)| seq.value()))
    }
}

impl MetaReplication for RedbMetaEngine {
    fn take_invalidation_batches(&self) -> Vec<crate::coherence::InvalidationBatch> {
        self.invalidation_journal
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    }

    fn metadata_invalidation_notifier(&self) -> Option<Arc<tokio::sync::Notify>> {
        Some(Arc::clone(&self.invalidation_notify))
    }

    fn prepare_for_close(&self) -> MetaResult<()> {
        self.seal_relaxed_durability_for_close();
        Ok(())
    }

    fn check_consistency(&self) -> MetaResult<()> {
        Ok(())
    }

    fn flush_metadata(&self) -> MetaResult<()> {
        self.flush_local_metadata()
    }

    fn flush_local_metadata(&self) -> MetaResult<()> {
        RedbMetaEngine::flush_local_metadata(self)
    }

    fn sync_metadata(&self) -> MetaResult<()> {
        Ok(())
    }

    fn sync_local_metadata(&self) -> MetaResult<()> {
        RedbMetaEngine::sync_local_metadata(self)
    }

    fn note_active_open_inode(&self, _ino: Ino) {}

    fn note_inactive_open_inode(&self, _ino: Ino) {}

    fn sync_metadata_for_invalidation_poll(&self) -> MetaResult<()> {
        Ok(())
    }

    fn metadata_coherence_progress(&self) -> crate::engine::MetadataCoherenceProgress {
        crate::engine::MetadataCoherenceProgress {
            source_connected: true,
            ..crate::engine::MetadataCoherenceProgress::default()
        }
    }

    fn invalidation_sender_id(&self) -> Option<String> {
        None
    }

    fn request_eager_flush(&self) {}

    fn metadata_version_token(&self) -> Option<String> {
        None
    }

    fn namespace_version_token(&self) -> Option<String> {
        self.metadata_version_token()
    }

    fn replication_backlog(&self) -> MetadataReplicationBacklog {
        MetadataReplicationBacklog::default()
    }

    fn metadata_authority_status(&self) -> MetadataAuthorityStatus {
        MetadataAuthorityStatus::writable()
    }

    fn remote_metadata_size(&self) -> MetaResult<Option<u64>> {
        Ok(None)
    }
}
