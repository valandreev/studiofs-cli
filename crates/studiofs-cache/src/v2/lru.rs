//! Intrusive doubly-linked LRU list for eviction ordering.
//!
//! The list is *intrusive*: each [`LruNode`] is embedded directly inside a
//! [`super::index::IndexEntry`] which is heap-allocated via `Box`. Stable
//! heap addresses allow storing raw `NonNull<LruNode>` pointers in the link
//! fields without lifetime annotations.
//!
//! # Invariants
//!
//! * Every `LruNode` present in an `LruList` is owned by a live
//!   `Box<IndexEntry>` that is simultaneously present in the corresponding
//!   `BlockIndex::entries` map.
//! * All list operations must be serialised by the `Mutex` that guards the
//!   owning [`super::index::BlockIndex`].  The raw-pointer manipulation is
//!   sound because the mutex ensures single-threaded access to both the list
//!   links and the pointed-to nodes.
//! * A node must not be moved after it has been inserted into a list.  The
//!   `Box<IndexEntry>` heap allocation satisfies this requirement.

use std::ptr::NonNull;

// ---------------------------------------------------------------------------
// LruNode
// ---------------------------------------------------------------------------

/// A node in an intrusive doubly-linked LRU list.
///
/// Embed one of these in a heap-allocated entry (`Box<IndexEntry>`) and
/// pass a [`NonNull`] pointer to the list operations.  The node records the
/// `object_id` so that [`super::index::BlockIndex`] can map back to the
/// owning `HashMap` entry.
pub struct LruNode {
    pub(super) object_id: u64,
    pub(super) prev: Option<NonNull<LruNode>>,
    pub(super) next: Option<NonNull<LruNode>>,
}

impl LruNode {
    /// Create a new unlinked node for `object_id`.
    pub fn new(object_id: u64) -> Self {
        Self {
            object_id,
            prev: None,
            next: None,
        }
    }

    /// The `object_id` this node tracks.
    #[inline]
    pub fn object_id(&self) -> u64 {
        self.object_id
    }
}

// ---------------------------------------------------------------------------
// LruList
// ---------------------------------------------------------------------------

/// Intrusive doubly-linked LRU list.
///
/// * `head` → oldest entry (eviction candidate)
/// * `tail` → most recently used entry
///
/// # Safety
///
/// The list stores `NonNull<LruNode>` raw pointers into `Box<IndexEntry>`
/// values owned by the surrounding [`super::index::BlockIndex`].  All access
/// **must** be serialised by the `Mutex` wrapping that `BlockIndex`; see the
/// module-level invariants.
pub struct LruList {
    head: Option<NonNull<LruNode>>,
    tail: Option<NonNull<LruNode>>,
    len: usize,
}

// SAFETY: LruList is only accessed under Mutex<BlockIndex>, which provides
// the exclusive-access guarantees required for the raw-pointer manipulation.
unsafe impl Send for LruList {}

impl LruList {
    /// Create an empty list.
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
            len: 0,
        }
    }

    /// Number of nodes currently in the list.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` when the list is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Append `node` to the tail (MRU end).
    ///
    /// # Safety
    ///
    /// * `node` must not be linked in any list.
    /// * `node` must remain alive (its containing `Box` must not be dropped)
    ///   for as long as it is in the list.
    pub(super) unsafe fn push_back(&mut self, mut node: NonNull<LruNode>) {
        // SAFETY: caller guarantees node is live and unlinked.
        let node_ref = unsafe { node.as_mut() };
        debug_assert!(
            node_ref.prev.is_none() && node_ref.next.is_none(),
            "push_back: node is already linked"
        );

        node_ref.prev = self.tail;
        node_ref.next = None;

        match self.tail {
            Some(mut tail) => {
                // SAFETY: tail is a live linked node owned by BlockIndex.
                unsafe { tail.as_mut().next = Some(node) };
            }
            None => {
                // List was empty; this node becomes the head too.
                self.head = Some(node);
            }
        }
        self.tail = Some(node);
        self.len += 1;
    }

    /// Unlink `node` from the list.
    ///
    /// # Safety
    ///
    /// * `node` must currently be linked in `self`.
    /// * `node` must be alive (its containing `Box` must not be dropped).
    pub(super) unsafe fn remove(&mut self, mut node: NonNull<LruNode>) {
        // SAFETY: caller guarantees node is live and in this list.
        let node_ref = unsafe { node.as_mut() };
        let prev = node_ref.prev;
        let next = node_ref.next;

        match prev {
            Some(mut p) => {
                // SAFETY: p is a live linked node.
                unsafe { p.as_mut().next = next };
            }
            None => {
                // node was the head.
                self.head = next;
            }
        }
        match next {
            Some(mut n) => {
                // SAFETY: n is a live linked node.
                unsafe { n.as_mut().prev = prev };
            }
            None => {
                // node was the tail.
                self.tail = prev;
            }
        }

        // Clear the node's own links so push_back's debug_assert fires if it
        // is accidentally re-inserted without being unlinked first.
        node_ref.prev = None;
        node_ref.next = None;

        self.len -= 1;
    }

    /// Move `node` to the tail (MRU position).
    ///
    /// No-op when `node` is already the tail.
    ///
    /// # Safety
    ///
    /// Same invariants as [`remove`] and [`push_back`].
    pub(super) unsafe fn touch(&mut self, node: NonNull<LruNode>) {
        if self.tail == Some(node) {
            return;
        }
        // SAFETY: propagated from caller.
        unsafe {
            self.remove(node);
            self.push_back(node);
        }
    }

    /// Remove and return the `object_id` of the oldest *evictable* node
    /// at the head of the list.
    ///
    /// Scans from `head` toward `tail`, skipping nodes for which
    /// `is_evictable(object_id)` returns `false`.  Returns `None` when no
    /// evictable node exists.
    ///
    /// Time complexity: O(k) where k is the number of non-evictable nodes
    /// before the first evictable one.  In typical operation k is small because
    /// dirty blocks are a minority.  If future workloads create long runs of
    /// non-evictable blocks at the LRU head (e.g., all blocks dirty or
    /// uploading at once), consider maintaining a separate singly-linked
    /// "clean list" that only contains evictable nodes, reducing this to O(1).
    ///
    /// # Safety
    ///
    /// All nodes in the list must be backed by live `Box<IndexEntry>` values.
    pub(super) unsafe fn pop_front_evictable(
        &mut self,
        is_evictable: impl Fn(u64) -> bool,
    ) -> Option<u64> {
        let mut cursor = self.head;
        while let Some(node) = cursor {
            // SAFETY: cursor is a live node per the module invariant.
            let (oid, next) = unsafe {
                let node_ref = node.as_ref();
                (node_ref.object_id, node_ref.next)
            };
            if is_evictable(oid) {
                // SAFETY: node is in this list and is live.
                unsafe { self.remove(node) };
                return Some(oid);
            }
            cursor = next;
        }
        None
    }

    /// Return the `object_id` of the oldest node that matches `predicate`
    /// without unlinking it from the list.
    ///
    /// # Safety
    ///
    /// All nodes in the list must be backed by live `Box<IndexEntry>` values.
    pub(super) unsafe fn find_front_matching(
        &self,
        predicate: impl Fn(u64) -> bool,
    ) -> Option<u64> {
        let mut cursor = self.head;
        while let Some(node) = cursor {
            let (object_id, next) = unsafe {
                let node_ref = node.as_ref();
                (node_ref.object_id, node_ref.next)
            };
            if predicate(object_id) {
                return Some(object_id);
            }
            cursor = next;
        }
        None
    }

    /// Visit nodes from the LRU head (oldest) to tail (MRU).
    ///
    /// # Safety
    ///
    /// All nodes in the list must be backed by live `Box<IndexEntry>` values.
    pub(super) unsafe fn for_each_oldest_first(&self, mut visit: impl FnMut(u64)) {
        let mut cursor = self.head;
        while let Some(node) = cursor {
            let (object_id, next) = unsafe {
                let node_ref = node.as_ref();
                (node_ref.object_id, node_ref.next)
            };
            visit(object_id);
            cursor = next;
        }
    }
}

impl Drop for LruList {
    fn drop(&mut self) {
        // BlockIndex::drop must unlink all nodes before the list is dropped.
        // If this assertion fires it means a BlockIndex leaked without calling
        // its own cleanup path.
        debug_assert!(
            self.is_empty(),
            "LruList dropped with {} nodes still linked; \
             BlockIndex must drain the list before dropping",
            self.len
        );
    }
}

impl Default for LruList {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: allocate a node on the heap and return both the Box and a NonNull ptr.
    fn make_node(oid: u64) -> Box<LruNode> {
        Box::new(LruNode::new(oid))
    }

    unsafe fn ptr(node: &mut Box<LruNode>) -> NonNull<LruNode> {
        NonNull::from(node.as_mut())
    }

    #[test]
    fn push_single_head_tail_equal() {
        let mut list = LruList::new();
        let mut n = make_node(1);
        unsafe { list.push_back(ptr(&mut n)) };
        assert_eq!(list.len(), 1);
        assert_eq!(list.head, list.tail);
        unsafe { list.remove(ptr(&mut n)) };
    }

    #[test]
    fn push_order_head_to_tail() {
        let mut list = LruList::new();
        let mut a = make_node(1);
        let mut b = make_node(2);
        let mut c = make_node(3);
        unsafe {
            list.push_back(ptr(&mut a));
            list.push_back(ptr(&mut b));
            list.push_back(ptr(&mut c));
        }
        // head = oldest = a, tail = newest = c
        assert_eq!(unsafe { list.head.unwrap().as_ref().object_id }, 1);
        assert_eq!(unsafe { list.tail.unwrap().as_ref().object_id }, 3);
        assert_eq!(list.len(), 3);
        // clean up
        unsafe {
            list.remove(ptr(&mut a));
            list.remove(ptr(&mut b));
            list.remove(ptr(&mut c));
        }
    }

    #[test]
    fn remove_middle() {
        let mut list = LruList::new();
        let mut a = make_node(1);
        let mut b = make_node(2);
        let mut c = make_node(3);
        unsafe {
            list.push_back(ptr(&mut a));
            list.push_back(ptr(&mut b));
            list.push_back(ptr(&mut c));
            list.remove(ptr(&mut b));
        }
        assert_eq!(list.len(), 2);
        assert_eq!(unsafe { list.head.unwrap().as_ref().object_id }, 1);
        assert_eq!(unsafe { list.tail.unwrap().as_ref().object_id }, 3);
        unsafe {
            list.remove(ptr(&mut a));
            list.remove(ptr(&mut c));
        }
    }

    #[test]
    fn touch_moves_to_tail() {
        let mut list = LruList::new();
        let mut a = make_node(1);
        let mut b = make_node(2);
        let mut c = make_node(3);
        unsafe {
            list.push_back(ptr(&mut a));
            list.push_back(ptr(&mut b));
            list.push_back(ptr(&mut c));
            // Touch a (was head) — should become the new tail.
            list.touch(ptr(&mut a));
        }
        assert_eq!(unsafe { list.head.unwrap().as_ref().object_id }, 2);
        assert_eq!(unsafe { list.tail.unwrap().as_ref().object_id }, 1);
        unsafe {
            list.remove(ptr(&mut b));
            list.remove(ptr(&mut c));
            list.remove(ptr(&mut a));
        }
    }

    #[test]
    fn touch_already_tail_is_noop() {
        let mut list = LruList::new();
        let mut a = make_node(1);
        let mut b = make_node(2);
        unsafe {
            list.push_back(ptr(&mut a));
            list.push_back(ptr(&mut b));
            list.touch(ptr(&mut b)); // b is already tail
        }
        assert_eq!(list.len(), 2);
        assert_eq!(unsafe { list.head.unwrap().as_ref().object_id }, 1);
        assert_eq!(unsafe { list.tail.unwrap().as_ref().object_id }, 2);
        unsafe {
            list.remove(ptr(&mut a));
            list.remove(ptr(&mut b));
        }
    }

    #[test]
    fn pop_front_evictable_skips_non_evictable() {
        let mut list = LruList::new();
        let mut a = make_node(1); // non-evictable
        let mut b = make_node(2); // non-evictable
        let mut c = make_node(3); // evictable
        unsafe {
            list.push_back(ptr(&mut a));
            list.push_back(ptr(&mut b));
            list.push_back(ptr(&mut c));
            let oid = list.pop_front_evictable(|oid| oid == 3);
            assert_eq!(oid, Some(3));
        }
        assert_eq!(list.len(), 2);
        unsafe {
            list.remove(ptr(&mut a));
            list.remove(ptr(&mut b));
        }
    }

    #[test]
    fn pop_front_evictable_none_when_all_pinned() {
        let mut list = LruList::new();
        let mut a = make_node(1);
        let mut b = make_node(2);
        unsafe {
            list.push_back(ptr(&mut a));
            list.push_back(ptr(&mut b));
            let result = list.pop_front_evictable(|_| false);
            assert_eq!(result, None);
            list.remove(ptr(&mut a));
            list.remove(ptr(&mut b));
        }
    }
}
