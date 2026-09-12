//! Platform-neutral cache scopes produced by committed metadata changes.

use crate::types::Ino;
use std::collections::HashSet;

/// A conservative scope whose cached view may no longer match local metadata.
/// These are advisory notifications, never instructions to discard dirty writes.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum InvalidationIntent {
    /// Cached inode attributes may have changed.
    Attributes {
        /// Affected inode.
        ino: Ino,
    },
    /// A positive or negative cached name binding may have changed.
    Entry {
        /// Parent inode.
        parent: Ino,
        /// Exact name, including names removed by this commit.
        name: Vec<u8>,
        /// Historical root-to-parent components; absent when provenance is unknown.
        ancestors: Option<Vec<Vec<u8>>>,
        /// Pre-mutation binding, or the created inode when no old binding existed.
        ino: Option<Ino>,
    },
    /// Cached directory enumeration may have changed.
    Directory {
        /// Directory inode.
        ino: Ino,
    },
    /// Cached clean content may have changed; consult the current serving state.
    Content {
        /// File inode.
        ino: Ino,
    },
    /// Exact coverage was lost or metadata was replaced wholesale.
    Reset,
}

/// Cache scopes ordered by successful local commit, within one engine lifetime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidationBatch {
    /// Monotonic local generation, independent of remote event sequence numbers.
    pub generation: u64,
    /// All affected scopes, or an explicit reset when detail could not be retained.
    pub intents: Vec<InvalidationIntent>,
}

pub(crate) const MAX_INVALIDATION_INTENTS: usize = 4096;
const MAX_INVALIDATION_BYTES: usize = 1024 * 1024;

#[derive(Default)]
pub(crate) struct PendingInvalidations {
    intents: Vec<InvalidationIntent>,
    bytes: usize,
    seen: HashSet<InvalidationIntent>,
}

impl PendingInvalidations {
    pub(crate) fn push(&mut self, intent: InvalidationIntent) {
        if matches!(self.intents.first(), Some(InvalidationIntent::Reset)) {
            return;
        }
        if self.seen.contains(&intent) {
            return;
        }
        let bytes = std::mem::size_of::<InvalidationIntent>()
            + match &intent {
                InvalidationIntent::Entry {
                    name, ancestors, ..
                } => {
                    name.len()
                        + ancestors.as_ref().map_or(0, |parts| {
                            parts
                                .iter()
                                .map(|part| part.len() + std::mem::size_of::<Vec<u8>>())
                                .sum::<usize>()
                        })
                }
                _ => 0,
            };
        if intent == InvalidationIntent::Reset
            || self.intents.len() >= MAX_INVALIDATION_INTENTS
            || bytes > MAX_INVALIDATION_BYTES.saturating_sub(self.bytes)
        {
            self.intents.clear();
            self.bytes = 0;
            self.seen.clear();
            self.intents.push(InvalidationIntent::Reset);
            self.seen.insert(InvalidationIntent::Reset);
        } else {
            self.bytes += bytes;
            self.seen.insert(intent.clone());
            self.intents.push(intent);
        }
    }
}

#[derive(Default)]
pub(crate) struct InvalidationJournal {
    generation: u64,
    batches: Vec<InvalidationBatch>,
    count: usize,
    bytes: usize,
}

impl InvalidationJournal {
    pub(crate) fn publish(&mut self, mut pending: PendingInvalidations) -> bool {
        if pending.intents.is_empty() {
            return false;
        }
        self.generation = self.generation.saturating_add(1);
        if self.count + pending.intents.len() > MAX_INVALIDATION_INTENTS
            || pending.bytes > MAX_INVALIDATION_BYTES.saturating_sub(self.bytes)
            || matches!(pending.intents.first(), Some(InvalidationIntent::Reset))
        {
            self.batches.clear();
            self.count = 0;
            self.bytes = 0;
            pending = PendingInvalidations::default();
            pending.push(InvalidationIntent::Reset);
        }
        self.count += pending.intents.len();
        self.bytes += pending.bytes;
        self.batches.push(InvalidationBatch {
            generation: self.generation,
            intents: pending.intents,
        });
        true
    }

    pub(crate) fn take(&mut self) -> Vec<InvalidationBatch> {
        self.count = 0;
        self.bytes = 0;
        std::mem::take(&mut self.batches)
    }
}
