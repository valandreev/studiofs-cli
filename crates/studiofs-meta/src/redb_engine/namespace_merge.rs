//! Replay-time resolution of directories created concurrently under one name.
//!
//! Peers create names during sync windows without seeing each other. Every
//! client replays the shared log in one global order and these rules read only
//! replicated base state, so all clients reach the same namespace.

use super::*;
use crate::events::MetaEvent;
use redb::ReadableTableMetadata;

/// Bounds traversal when a previously canonical directory becomes an alias.
const MAX_ALIAS_HOPS: usize = 8;

impl RedbMetaEngine {
    /// Rewrites one replay batch so a directory that reaches an existing
    /// directory's name merges into it instead of displacing it.
    ///
    /// The directory whose `AddEntry` replays first keeps the name (canonical);
    /// a later one becomes its alias, and events naming the alias as a parent
    /// move to the canonical directory. Returns the batch unchanged when no rule
    /// applies.
    pub(crate) fn canonicalize_replay_batch_in_txn<'event>(
        txn: &RedbWriteTransaction<'_>,
        events: &'event [MetaEvent],
    ) -> MetaResult<Cow<'event, [MetaEvent]>> {
        let mut aliases = txn
            .open_table(DIRECTORY_ALIASES_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let mut merge = BatchMerge {
            has_aliases: !aliases
                .is_empty()
                .map_err(|error| MetaError::Storage(error.to_string()))?,
            dentries: HashMap::new(),
            removed_aliases_of: HashSet::new(),
        };
        let mut canonical: Option<Vec<MetaEvent>> = None;
        for (index, event) in events.iter().enumerate() {
            let rewritten = merge.rewrite(txn, &mut aliases, event)?;
            let effective = match &rewritten {
                Rewrite::Keep => Some(event),
                Rewrite::Drop => None,
                Rewrite::Replace(replacement) => Some(replacement),
            };
            if let Some(effective) = effective {
                merge.observe_dentry_effect(effective);
            }
            match rewritten {
                Rewrite::Keep => {
                    if let Some(canonical) = &mut canonical {
                        canonical.push(event.clone());
                    }
                }
                Rewrite::Drop => {
                    canonical.get_or_insert_with(|| events[..index].to_vec());
                }
                Rewrite::Replace(replacement) => canonical
                    .get_or_insert_with(|| events[..index].to_vec())
                    .push(replacement),
            }
        }
        Ok(canonical.map_or(Cow::Borrowed(events), Cow::Owned))
    }
}

enum Rewrite {
    Keep,
    Drop,
    Replace(MetaEvent),
}

struct BatchMerge {
    has_aliases: bool,
    /// Dentries written earlier in this batch; `None` records a removal.
    dentries: HashMap<(Ino, Vec<u8>), Option<DirEntryValue>>,
    /// Canonical directories whose alias was removed earlier in this batch.
    removed_aliases_of: HashSet<Ino>,
}

impl BatchMerge {
    fn rewrite(
        &mut self,
        txn: &RedbWriteTransaction<'_>,
        aliases: &mut redb::Table<'_, u64, u64>,
        event: &MetaEvent,
    ) -> MetaResult<Rewrite> {
        Ok(match event {
            MetaEvent::AddEntry {
                parent,
                name,
                entry,
            } => {
                let new_parent = self.resolve(aliases, *parent)?;
                let mut new_entry = entry.clone();
                if entry.kind == FileType::Directory {
                    new_entry.ino = self.resolve(aliases, entry.ino)?;
                    if let Some(existing) = self.dentry(txn, new_parent, name)?
                        && existing.kind == FileType::Directory
                        && existing.ino != new_entry.ino
                    {
                        aliases
                            .insert(new_entry.ino, existing.ino)
                            .map_err(|error| MetaError::Storage(error.to_string()))?;
                        self.has_aliases = true;
                        return Ok(Rewrite::Drop);
                    }
                }
                if new_parent == *parent && new_entry.ino == entry.ino {
                    Rewrite::Keep
                } else {
                    Rewrite::Replace(MetaEvent::AddEntry {
                        parent: new_parent,
                        name: name.clone(),
                        entry: new_entry,
                    })
                }
            }
            MetaEvent::RemoveEntry { parent, name } => {
                let new_parent = self.resolve(aliases, *parent)?;
                // An rmdir of an alias emits RemoveAttr(alias) first; its name
                // now belongs to the merged directory, which must stay.
                if let Some(existing) = self.dentry(txn, new_parent, name)?
                    && self.removed_aliases_of.contains(&existing.ino)
                {
                    return Ok(Rewrite::Drop);
                }
                if new_parent == *parent {
                    Rewrite::Keep
                } else {
                    Rewrite::Replace(MetaEvent::RemoveEntry {
                        parent: new_parent,
                        name: name.clone(),
                    })
                }
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
                if !self.has_aliases {
                    return Ok(Rewrite::Keep);
                }
                let rewritten = MetaEvent::RenameEntry {
                    parent: self.resolve(aliases, *parent)?,
                    name: name.clone(),
                    new_parent: self.resolve(aliases, *new_parent)?,
                    new_name: new_name.clone(),
                    entry: DirEntryValue {
                        ino: self.resolve(aliases, entry.ino)?,
                        kind: entry.kind,
                    },
                    displaced_set_attr: Box::new(
                        self.non_alias_attr(aliases, displaced_set_attr.as_ref().as_ref())?,
                    ),
                    displaced_remove_attr_ino: *displaced_remove_attr_ino,
                    moved_dir_attr: Box::new(
                        self.resolve_attr(aliases, moved_dir_attr.as_ref().as_ref())?,
                    ),
                    // An alias parent's attrs would overwrite the canonical owner's.
                    old_parent_attr: Box::new(
                        self.non_alias_attr(aliases, old_parent_attr.as_ref().as_ref())?,
                    ),
                    new_parent_attr: Box::new(
                        self.non_alias_attr(aliases, new_parent_attr.as_ref().as_ref())?,
                    ),
                };
                if &rewritten == event {
                    Rewrite::Keep
                } else {
                    Rewrite::Replace(rewritten)
                }
            }
            MetaEvent::SetAttr { attr } => {
                if !self.has_aliases {
                    return Ok(Rewrite::Keep);
                }
                if self.alias_target(aliases, attr.ino)?.is_some() {
                    // The canonical directory's owner stays authoritative.
                    return Ok(Rewrite::Drop);
                }
                let parent = self.resolve(aliases, attr.parent)?;
                if parent == attr.parent {
                    Rewrite::Keep
                } else {
                    let mut attr = attr.clone();
                    attr.parent = parent;
                    Rewrite::Replace(MetaEvent::SetAttr { attr })
                }
            }
            MetaEvent::RemoveAttr { ino } => match self.alias_target_if_any(aliases, *ino)? {
                Some(canonical) => {
                    self.removed_aliases_of.insert(canonical);
                    Rewrite::Drop
                }
                None => Rewrite::Keep,
            },
            MetaEvent::SetXattr { ino, .. }
            | MetaEvent::RemoveXattr { ino, .. }
            | MetaEvent::SetAclRule { ino, .. }
            | MetaEvent::RemoveAclRule { ino, .. } => {
                if self.alias_target_if_any(aliases, *ino)?.is_some() {
                    Rewrite::Drop
                } else {
                    Rewrite::Keep
                }
            }
            _ => Rewrite::Keep,
        })
    }

    fn observe_dentry_effect(&mut self, event: &MetaEvent) {
        match event {
            MetaEvent::AddEntry {
                parent,
                name,
                entry,
            } => {
                self.dentries
                    .insert((*parent, name.clone()), Some(entry.clone()));
            }
            MetaEvent::RemoveEntry { parent, name } => {
                self.dentries.insert((*parent, name.clone()), None);
            }
            MetaEvent::RenameEntry {
                parent,
                name,
                new_parent,
                new_name,
                entry,
                ..
            } => {
                self.dentries.insert((*parent, name.clone()), None);
                self.dentries
                    .insert((*new_parent, new_name.clone()), Some(entry.clone()));
            }
            _ => {}
        }
    }

    fn dentry(
        &self,
        txn: &RedbWriteTransaction<'_>,
        parent: Ino,
        name: &[u8],
    ) -> MetaResult<Option<DirEntryValue>> {
        if let Some(entry) = self.dentries.get(&(parent, name.to_vec())) {
            return Ok(entry.clone());
        }
        RedbMetaEngine::lookup_in_write_table(txn, BASE_DENTRIES_TABLE, parent, name)
    }

    fn alias_target_if_any(
        &self,
        aliases: &redb::Table<'_, u64, u64>,
        ino: Ino,
    ) -> MetaResult<Option<Ino>> {
        if !self.has_aliases {
            return Ok(None);
        }
        self.alias_target(aliases, ino)
    }

    fn alias_target(
        &self,
        aliases: &redb::Table<'_, u64, u64>,
        ino: Ino,
    ) -> MetaResult<Option<Ino>> {
        let target = aliases
            .get(ino)
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .map(|target| target.value());
        Ok(target)
    }

    fn resolve(&self, aliases: &redb::Table<'_, u64, u64>, ino: Ino) -> MetaResult<Ino> {
        if !self.has_aliases {
            return Ok(ino);
        }
        let mut current = ino;
        for _ in 0..MAX_ALIAS_HOPS {
            match self.alias_target(aliases, current)? {
                Some(target) => current = target,
                None => return Ok(current),
            }
        }
        Err(MetaError::Storage(format!(
            "directory alias chain from inode {ino} exceeds {MAX_ALIAS_HOPS} hops"
        )))
    }

    fn resolve_attr(
        &self,
        aliases: &redb::Table<'_, u64, u64>,
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Option<InodeAttr>> {
        let Some(attr) = attr else {
            return Ok(None);
        };
        let mut attr = attr.clone();
        attr.ino = self.resolve(aliases, attr.ino)?;
        attr.parent = self.resolve(aliases, attr.parent)?;
        Ok(Some(attr))
    }

    fn non_alias_attr(
        &self,
        aliases: &redb::Table<'_, u64, u64>,
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Option<InodeAttr>> {
        match attr {
            Some(attr) if self.alias_target(aliases, attr.ino)?.is_none() => {
                self.resolve_attr(aliases, Some(attr))
            }
            _ => Ok(None),
        }
    }
}
