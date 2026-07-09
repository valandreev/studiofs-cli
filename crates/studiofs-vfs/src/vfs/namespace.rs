use super::*;
use crate::{LockTier, OrderedMutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Controls how namespace paths are parsed and rendered (POSIX vs Windows separator rules).
pub enum NamespacePathStyle {
    /// POSIX-style paths using forward-slash (`/`) separators.
    Posix,
    /// Windows-style paths using backslash (`\`) separators.
    Windows,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Represents the resolved parent directory and its target component name.
pub struct NamespaceParent {
    /// Inode of the directory that owns the component.
    pub parent: Ino,
    /// Raw name bytes of the component under `parent`.
    pub name: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DentryCacheKey {
    parent: Ino,
    name: Arc<[u8]>,
}

impl DentryCacheKey {
    fn new(parent: Ino, name: &[u8]) -> Self {
        Self {
            parent,
            name: Arc::from(name),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct DeferredNamespaceKey {
    pub(super) parent: Ino,
    pub(super) name: Arc<[u8]>,
}

impl DeferredNamespaceKey {
    pub(super) fn new(parent: Ino, name: &[u8]) -> Self {
        Self {
            parent,
            name: Arc::from(name),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) enum DeferredNamespaceOp {
    CreateFile {
        attr: InodeAttr,
        parent: Ino,
        name: Vec<u8>,
        uid: u32,
        gid: u32,
        mode: u16,
    },
    CreateDir {
        attr: InodeAttr,
        parent: Ino,
        name: Vec<u8>,
        uid: u32,
        gid: u32,
        mode: u16,
    },
    SetAttr {
        attr: InodeAttr,
        args: SetattrArgs,
    },
}

impl DeferredNamespaceOp {
    pub(super) fn ino(&self) -> Ino {
        match self {
            Self::CreateFile { attr, .. }
            | Self::CreateDir { attr, .. }
            | Self::SetAttr { attr, .. } => attr.ino,
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct DeferredNamespaceInner {
    pub(super) entries: HashMap<DeferredNamespaceKey, Ino>,
    pub(super) attrs: HashMap<Ino, InodeAttr>,
    pub(super) pending_inodes: HashMap<Ino, usize>,
    pub(super) errors: HashMap<Ino, String>,
    pub(super) queue: VecDeque<DeferredNamespaceOp>,
    pub(super) reserved_inos: VecDeque<Ino>,
    pub(super) locally_created_dirs: HashSet<Ino>,
}

pub(super) struct DeferredNamespaceState {
    pub(super) inner: OrderedMutex<DeferredNamespaceInner>,
    pub(super) notify: tokio::sync::Notify,
    pub(super) draining: AtomicBool,
}

impl DeferredNamespaceState {
    fn new() -> Self {
        Self {
            inner: OrderedMutex::new(DeferredNamespaceInner::default(), LockTier::Namespace),
            notify: tokio::sync::Notify::new(),
            draining: AtomicBool::new(false),
        }
    }
}

/// Cached result for one `(parent, name)` namespace lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DentryCacheValue {
    /// Positive cache entry pointing to an existing inode.
    Positive(Ino),
    /// Negative cache entry indicating component is absent under the parent.
    Negative,
}

#[derive(Debug, Clone)]
struct DentryCacheMeta {
    namespace_epoch: u64,
    parent_generation: u64,
}

#[derive(Debug, Clone, Copy)]
struct NamespaceCacheToken {
    namespace_epoch: u64,
    parent_generation: u64,
}

/// Metrics for dentry cache hits, misses, and hot-set occupancy.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DentryCacheMetrics {
    /// Total cache hits across protected and unprotected rings.
    pub(super) hits: u64,
    /// Hits served from protected (hot) ring.
    pub(super) protected_hits: u64,
    /// Misses from all rings.
    pub(super) misses: u64,
    /// Hits for entries that cache a missing child.
    pub(super) negative_hits: u64,
    /// Entries evicted under capacity pressure.
    pub(super) evictions: u64,
    /// Current number of entries in cache.
    pub(super) occupancy_entries: usize,
    /// Current estimated bytes consumed by cache entries.
    pub(super) occupancy_bytes: usize,
    /// Current entries in protected (hot) ring.
    pub(super) protected_occupancy_entries: usize,
    /// Current estimated bytes consumed by protected (hot) ring.
    pub(super) protected_occupancy_bytes: usize,
    /// Configured entry capacity bound for this cache.
    pub(super) entry_budget: usize,
    /// Configured byte capacity bound for this cache.
    pub(super) byte_budget: usize,
}

impl DentryCacheMetrics {
    /// Returns the fraction of hits that were negative cache entries.
    pub(super) fn negative_hit_ratio(self) -> f64 {
        if self.hits == 0 {
            return 0.0;
        }
        self.negative_hits as f64 / self.hits as f64
    }

    /// Returns the fraction of hits served from protected (hot) storage.
    pub(super) fn hot_hit_ratio(self) -> f64 {
        if self.hits == 0 {
            return 0.0;
        }
        self.protected_hits as f64 / self.hits as f64
    }
}

impl From<DentryCacheMetrics> for VfsNamespaceCacheSnapshot {
    fn from(value: DentryCacheMetrics) -> Self {
        Self {
            hits: value.hits,
            protected_hits: value.protected_hits,
            misses: value.misses,
            negative_hits: value.negative_hits,
            evictions: value.evictions,
            negative_hit_ratio: value.negative_hit_ratio(),
            hot_hit_ratio: value.hot_hit_ratio(),
            occupancy_entries: value.occupancy_entries,
            occupancy_bytes: value.occupancy_bytes,
            protected_occupancy_entries: value.protected_occupancy_entries,
            protected_occupancy_bytes: value.protected_occupancy_bytes,
            entry_budget: value.entry_budget,
            byte_budget: value.byte_budget,
        }
    }
}

#[derive(Debug, Default)]
struct DentryCachePolicy {
    parent_index: HashMap<Ino, HashSet<DentryCacheKey>>,
}

impl DentryCachePolicy {
    fn estimate_name_entry_charge(name_len: usize) -> usize {
        std::mem::size_of::<DentryCacheKey>()
            .saturating_add(std::mem::size_of::<
                TwoTierEntry<DentryCacheValue, DentryCacheMeta>,
            >())
            .saturating_add(name_len)
            .saturating_add(DENTRY_CACHE_INDEX_NODE_OVERHEAD * 2)
    }

    fn take_parent_keys(&mut self, parent: Ino) -> Option<HashSet<DentryCacheKey>> {
        self.parent_index.remove(&parent)
    }
}

impl CachePolicy<DentryCacheKey, DentryCacheValue> for DentryCachePolicy {
    type Meta = DentryCacheMeta;
    type InsertContext = NamespaceCacheToken;
    type LookupContext<'a> = NamespaceCacheToken;

    fn make_meta(
        &mut self,
        _key: &DentryCacheKey,
        _value: &DentryCacheValue,
        ctx: Self::InsertContext,
    ) -> Self::Meta {
        DentryCacheMeta {
            namespace_epoch: ctx.namespace_epoch,
            parent_generation: ctx.parent_generation,
        }
    }

    fn is_stale(
        &self,
        _key: &DentryCacheKey,
        _value: &DentryCacheValue,
        meta: &Self::Meta,
        ctx: Self::LookupContext<'_>,
    ) -> bool {
        meta.namespace_epoch != ctx.namespace_epoch
            || meta.parent_generation != ctx.parent_generation
    }

    fn estimate_entry_charge(
        &self,
        key: &DentryCacheKey,
        _value: &DentryCacheValue,
        _meta: &Self::Meta,
    ) -> usize {
        Self::estimate_name_entry_charge(key.name.len())
    }

    fn is_negative(&self, value: &DentryCacheValue) -> bool {
        matches!(value, DentryCacheValue::Negative)
    }

    fn on_insert(&mut self, key: &DentryCacheKey, _value: &DentryCacheValue, _meta: &Self::Meta) {
        self.parent_index
            .entry(key.parent)
            .or_default()
            .insert(key.clone());
    }

    fn on_remove(&mut self, key: &DentryCacheKey, _value: &DentryCacheValue, _meta: &Self::Meta) {
        if let Some(keys) = self.parent_index.get_mut(&key.parent) {
            keys.remove(key);
            if keys.is_empty() {
                self.parent_index.remove(&key.parent);
            }
        }
    }
}

/// Two-tier cache for direct dentry lookups keyed by `(parent, name)`.
#[derive(Debug)]
pub(super) struct DentryCache {
    inner: TwoTierCache<DentryCacheKey, DentryCacheValue, DentryCachePolicy>,
}

impl DentryCache {
    fn new(entry_budget: usize, byte_budget: usize) -> Self {
        Self {
            inner: TwoTierCache::new(entry_budget, byte_budget, DentryCachePolicy::default()),
        }
    }

    #[cfg(test)]
    fn estimate_entry_charge(name: &[u8]) -> usize {
        DentryCachePolicy::estimate_name_entry_charge(name.len())
    }

    fn metrics(&self) -> DentryCacheMetrics {
        let metrics = self.inner.metrics();
        DentryCacheMetrics {
            hits: metrics.hits,
            protected_hits: metrics.protected_hits,
            misses: metrics.misses,
            negative_hits: metrics.negative_hits,
            evictions: metrics.evictions,
            occupancy_entries: self.inner.entry_count(),
            occupancy_bytes: self.inner.used_bytes(),
            protected_occupancy_entries: self.inner.protected_entries(),
            protected_occupancy_bytes: self.inner.protected_bytes(),
            entry_budget: self.inner.entry_budget,
            byte_budget: self.inner.byte_budget,
        }
    }

    fn get(
        &mut self,
        parent: Ino,
        name: &[u8],
        namespace_epoch: u64,
        parent_generation: u64,
    ) -> Option<DentryCacheValue> {
        let key = DentryCacheKey::new(parent, name);
        self.inner.get(
            &key,
            NamespaceCacheToken {
                namespace_epoch,
                parent_generation,
            },
        )
    }

    fn put(
        &mut self,
        parent: Ino,
        name: &[u8],
        value: DentryCacheValue,
        namespace_epoch: u64,
        parent_generation: u64,
    ) {
        let key = DentryCacheKey::new(parent, name);
        self.inner.put(
            key,
            value,
            NamespaceCacheToken {
                namespace_epoch,
                parent_generation,
            },
        );
    }

    fn invalidate(&mut self, parent: Ino, name: &[u8]) {
        let key = DentryCacheKey::new(parent, name);
        self.inner.remove(&key);
    }

    fn invalidate_parent(&mut self, parent: Ino) {
        let Some(keys) = self.inner.policy_mut().take_parent_keys(parent) else {
            return;
        };
        for key in keys {
            self.inner.remove(&key);
        }
    }

    #[cfg(test)]
    fn entry_value(&self, key: &DentryCacheKey) -> Option<DentryCacheValue> {
        self.inner.peek(key).map(|entry| entry.value)
    }

    #[cfg(test)]
    fn entry_len(&self) -> usize {
        self.inner.entry_count()
    }

    #[cfg(test)]
    fn protected_order_len(&self) -> usize {
        self.inner.protected_order_len()
    }
}

/// Encoded normalized full-path token used by the path-shortcut cache.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct PathShortcutCacheKey {
    style: NamespacePathStyle,
    path: Arc<[u8]>,
}

impl PathShortcutCacheKey {
    fn from_components(style: NamespacePathStyle, components: &[String]) -> Option<Self> {
        if components.is_empty() {
            return None;
        }
        let estimated = components
            .iter()
            .map(|component| component.len())
            .sum::<usize>()
            .saturating_add(components.len().saturating_sub(1));
        let mut encoded = Vec::with_capacity(estimated);
        for (idx, component) in components.iter().enumerate() {
            if idx > 0 {
                encoded.push(0);
            }
            encoded.extend_from_slice(component.as_bytes());
        }
        Some(Self {
            style,
            path: Arc::from(encoded.into_boxed_slice()),
        })
    }
}

/// Cached value for memoized full-path resolution.
#[derive(Debug, Clone)]
pub(super) enum PathShortcutCacheValue {
    /// Positive path resolution: full path maps to inode.
    Positive(Ino),
    /// Negative path resolution: parent+name pair is known missing.
    Negative { parent: Ino, name: Arc<[u8]> },
}

/// Captured parent and generation snapshot used to validate shortcut freshness.
#[derive(Debug, Clone)]
pub(super) struct PathShortcutParentToken {
    parent: Ino,
    generation: u64,
}

#[derive(Debug, Clone)]
struct PathShortcutCacheMeta {
    namespace_epoch: u64,
    parent_tokens: Arc<[PathShortcutParentToken]>,
}

struct PathShortcutLookupCtx<'a> {
    namespace_epoch: u64,
    generation_of: &'a mut dyn FnMut(Ino) -> u64,
}

struct PathShortcutInsertCtx {
    namespace_epoch: u64,
    parent_tokens: Arc<[PathShortcutParentToken]>,
}

/// Metrics for path shortcut cache hits, misses, and capacity state.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct PathShortcutCacheMetrics {
    /// Total cache hits across protected and unprotected rings.
    pub(super) hits: u64,
    /// Hits served from protected (hot) ring.
    pub(super) protected_hits: u64,
    /// Misses from all rings.
    pub(super) misses: u64,
    /// Hits for negative path-shortcut entries.
    pub(super) negative_hits: u64,
    /// Entries evicted under capacity pressure.
    pub(super) evictions: u64,
    /// Current number of entries in cache.
    pub(super) occupancy_entries: usize,
    /// Current estimated bytes consumed by cache entries.
    pub(super) occupancy_bytes: usize,
    /// Entries currently in protected (hot) ring.
    pub(super) protected_occupancy_entries: usize,
    /// Current estimated bytes consumed by protected (hot) ring.
    pub(super) protected_occupancy_bytes: usize,
    /// Configured entry capacity bound for this cache.
    pub(super) entry_budget: usize,
    /// Configured byte capacity bound for this cache.
    pub(super) byte_budget: usize,
}

impl PathShortcutCacheMetrics {
    /// Returns the fraction of hits that were negative cache entries.
    pub(super) fn negative_hit_ratio(self) -> f64 {
        if self.hits == 0 {
            return 0.0;
        }
        self.negative_hits as f64 / self.hits as f64
    }

    /// Returns the fraction of hits served from protected (hot) storage.
    pub(super) fn hot_hit_ratio(self) -> f64 {
        if self.hits == 0 {
            return 0.0;
        }
        self.protected_hits as f64 / self.hits as f64
    }
}

impl From<PathShortcutCacheMetrics> for VfsNamespaceCacheSnapshot {
    fn from(value: PathShortcutCacheMetrics) -> Self {
        Self {
            hits: value.hits,
            protected_hits: value.protected_hits,
            misses: value.misses,
            negative_hits: value.negative_hits,
            evictions: value.evictions,
            negative_hit_ratio: value.negative_hit_ratio(),
            hot_hit_ratio: value.hot_hit_ratio(),
            occupancy_entries: value.occupancy_entries,
            occupancy_bytes: value.occupancy_bytes,
            protected_occupancy_entries: value.protected_occupancy_entries,
            protected_occupancy_bytes: value.protected_occupancy_bytes,
            entry_budget: value.entry_budget,
            byte_budget: value.byte_budget,
        }
    }
}

#[derive(Debug)]
struct PathShortcutCachePolicy;

impl PathShortcutCachePolicy {
    fn estimate_entry_charge_for(
        key: &PathShortcutCacheKey,
        value: &PathShortcutCacheValue,
        parent_tokens: &[PathShortcutParentToken],
    ) -> usize {
        let negative_name_len = match value {
            PathShortcutCacheValue::Positive(_) => 0,
            PathShortcutCacheValue::Negative { name, .. } => name.len(),
        };
        std::mem::size_of::<PathShortcutCacheKey>()
            .saturating_add(std::mem::size_of::<
                TwoTierEntry<PathShortcutCacheValue, PathShortcutCacheMeta>,
            >())
            .saturating_add(key.path.len())
            .saturating_add(negative_name_len)
            .saturating_add(
                parent_tokens
                    .len()
                    .saturating_mul(std::mem::size_of::<PathShortcutParentToken>()),
            )
            .saturating_add(DENTRY_CACHE_INDEX_NODE_OVERHEAD)
    }
}

impl Default for PathShortcutCachePolicy {
    fn default() -> Self {
        Self
    }
}

impl CachePolicy<PathShortcutCacheKey, PathShortcutCacheValue> for PathShortcutCachePolicy {
    type Meta = PathShortcutCacheMeta;
    type InsertContext = PathShortcutInsertCtx;
    type LookupContext<'a> = PathShortcutLookupCtx<'a>;

    fn make_meta(
        &mut self,
        _key: &PathShortcutCacheKey,
        _value: &PathShortcutCacheValue,
        ctx: Self::InsertContext,
    ) -> Self::Meta {
        PathShortcutCacheMeta {
            namespace_epoch: ctx.namespace_epoch,
            parent_tokens: ctx.parent_tokens,
        }
    }

    fn is_stale(
        &self,
        _key: &PathShortcutCacheKey,
        _value: &PathShortcutCacheValue,
        meta: &Self::Meta,
        ctx: Self::LookupContext<'_>,
    ) -> bool {
        meta.namespace_epoch != ctx.namespace_epoch
            || meta
                .parent_tokens
                .iter()
                .any(|token| (ctx.generation_of)(token.parent) != token.generation)
    }

    fn estimate_entry_charge(
        &self,
        key: &PathShortcutCacheKey,
        value: &PathShortcutCacheValue,
        meta: &Self::Meta,
    ) -> usize {
        Self::estimate_entry_charge_for(key, value, &meta.parent_tokens)
    }

    fn is_negative(&self, value: &PathShortcutCacheValue) -> bool {
        matches!(value, PathShortcutCacheValue::Negative { .. })
    }
}

/// Full path memoization cache for normalized paths to reduce component walk cost.
#[derive(Debug)]
pub(super) struct PathShortcutCache {
    inner: TwoTierCache<PathShortcutCacheKey, PathShortcutCacheValue, PathShortcutCachePolicy>,
}

impl PathShortcutCache {
    fn new(entry_budget: usize, byte_budget: usize) -> Self {
        Self {
            inner: TwoTierCache::new(entry_budget, byte_budget, PathShortcutCachePolicy),
        }
    }

    #[cfg(test)]
    fn estimate_entry_charge(
        key: &PathShortcutCacheKey,
        value: &PathShortcutCacheValue,
        parent_tokens: &[PathShortcutParentToken],
    ) -> usize {
        PathShortcutCachePolicy::estimate_entry_charge_for(key, value, parent_tokens)
    }

    fn metrics(&self) -> PathShortcutCacheMetrics {
        let metrics = self.inner.metrics();
        PathShortcutCacheMetrics {
            hits: metrics.hits,
            protected_hits: metrics.protected_hits,
            misses: metrics.misses,
            negative_hits: metrics.negative_hits,
            evictions: metrics.evictions,
            occupancy_entries: self.inner.entry_count(),
            occupancy_bytes: self.inner.used_bytes(),
            protected_occupancy_entries: self.inner.protected_entries(),
            protected_occupancy_bytes: self.inner.protected_bytes(),
            entry_budget: self.inner.entry_budget,
            byte_budget: self.inner.byte_budget,
        }
    }

    fn get(
        &mut self,
        key: &PathShortcutCacheKey,
        namespace_epoch: u64,
        mut generation_of: impl FnMut(Ino) -> u64,
    ) -> Option<PathShortcutCacheValue> {
        self.inner.get(
            key,
            PathShortcutLookupCtx {
                namespace_epoch,
                generation_of: &mut generation_of,
            },
        )
    }

    fn put(
        &mut self,
        key: PathShortcutCacheKey,
        value: PathShortcutCacheValue,
        namespace_epoch: u64,
        parent_tokens: Vec<PathShortcutParentToken>,
    ) {
        self.inner.put(
            key,
            value,
            PathShortcutInsertCtx {
                namespace_epoch,
                parent_tokens: Arc::from(parent_tokens),
            },
        );
    }

    #[cfg(test)]
    fn entry_len(&self) -> usize {
        self.inner.entry_count()
    }
}

/// Resolves namespace paths and components with path-style normalization and cache reuse.
pub struct NamespaceResolver<'a> {
    vfs: &'a StudioVfs,
    style: NamespacePathStyle,
}

impl<'a> NamespaceResolver<'a> {
    /// Builds a resolver for a chosen namespace path style.
    pub(super) fn new(vfs: &'a StudioVfs, style: NamespacePathStyle) -> Self {
        Self { vfs, style }
    }

    fn path_components(&self, path: &str) -> Vec<String> {
        let (stripped, separator) = match self.style {
            NamespacePathStyle::Posix => (path.trim_start_matches('/'), '/'),
            NamespacePathStyle::Windows => (path.trim_start_matches('\\'), '\\'),
        };

        let mut components: Vec<String> = Vec::new();
        for raw in stripped.split(separator) {
            if raw.is_empty() || raw == "." {
                continue;
            }
            if raw == ".." {
                components.pop();
                continue;
            }
            let normalized = match self.style {
                NamespacePathStyle::Posix => raw,
                NamespacePathStyle::Windows => raw.trim_end_matches(['.', ' ']),
            };
            if !normalized.is_empty() {
                components.push(normalized.to_string());
            }
        }
        components
    }

    fn path_not_found(path: &str) -> VfsError {
        VfsError::Meta(MetaError::EntryNotFound {
            parent: ROOT_INO,
            name: path.to_string(),
        })
    }

    fn component_not_found(parent: Ino, name: &[u8]) -> VfsError {
        VfsError::Meta(MetaError::EntryNotFound {
            parent,
            name: String::from_utf8_lossy(name).into_owned(),
        })
    }

    fn is_entry_not_found(err: &VfsError) -> bool {
        matches!(err, VfsError::Meta(MetaError::EntryNotFound { .. }))
    }

    fn resolve_components_cached(&self, components: &[String]) -> VfsResult<InodeAttr> {
        if components.is_empty() {
            return self.vfs.getattr(ROOT_INO);
        }

        let cache_key = PathShortcutCacheKey::from_components(self.style, components);
        let do_resolve = || -> VfsResult<InodeAttr> {
            let mut current = ROOT_INO;
            let mut attr = self.vfs.getattr(ROOT_INO)?;
            let mut parent_tokens = Vec::with_capacity(components.len());
            for component in components {
                parent_tokens.push(PathShortcutParentToken {
                    parent: current,
                    generation: self.vfs.directory_generation(current),
                });
                match self.lookup_component(current, component.as_bytes()) {
                    Ok(next_attr) => {
                        current = next_attr.ino;
                        attr = next_attr;
                    }
                    Err(err) if Self::is_entry_not_found(&err) => {
                        if let Some(key) = cache_key.clone() {
                            self.vfs.path_shortcut_cache_insert_negative_by_key(
                                key,
                                current,
                                component.as_bytes(),
                                parent_tokens,
                            );
                        }
                        return Err(err);
                    }
                    Err(err) => return Err(err),
                }
            }
            if let Some(key) = cache_key.clone() {
                self.vfs
                    .path_shortcut_cache_insert_positive_by_key(key, attr.ino, parent_tokens);
            }
            Ok(attr)
        };

        if let Some(key) = &cache_key
            && let Some(cached) = self.vfs.path_shortcut_cache_lookup_by_key(key)
        {
            return match cached {
                PathShortcutCacheValue::Positive(ino) => match self.vfs.getattr(ino) {
                    Ok(attr) => Ok(attr),
                    Err(VfsError::Meta(MetaError::InodeNotFound(_))) => do_resolve(),
                    Err(err) => Err(err),
                },
                PathShortcutCacheValue::Negative { parent, name } => {
                    Err(Self::component_not_found(parent, name.as_ref()))
                }
            };
        }

        do_resolve()
    }

    /// Resolves a single path component under `parent` into inode metadata.
    pub fn lookup_component(&self, parent: Ino, name: &[u8]) -> VfsResult<InodeAttr> {
        self.vfs.lookup_component_impl(parent, name)
    }

    /// Resolves a full path, applying `.`/`..` trimming and platform-specific separators.
    pub fn resolve_path(&self, path: &str) -> VfsResult<InodeAttr> {
        let components = self.path_components(path);
        self.resolve_components_cached(&components)
    }

    /// Resolves a path to `(parent_inode, final_component_name)`.
    pub fn resolve_parent(&self, path: &str) -> VfsResult<NamespaceParent> {
        let components = self.path_components(path);
        if components.is_empty() {
            return Err(Self::path_not_found(path));
        }

        let current = if components.len() == 1 {
            ROOT_INO
        } else {
            self.resolve_components_cached(&components[..components.len() - 1])?
                .ino
        };

        Ok(NamespaceParent {
            parent: current,
            name: components.last().unwrap().as_bytes().to_vec(),
        })
    }

    /// Builds a best-effort absolute path for an inode using cached/readdir ancestry.
    ///
    /// Returns `None` when ancestry cannot be reconstructed within the lookup window.
    pub fn path_for_inode(&self, ino: Ino) -> Option<String> {
        if ino == ROOT_INO {
            return Some(match self.style {
                NamespacePathStyle::Posix => "/".to_string(),
                NamespacePathStyle::Windows => "\\".to_string(),
            });
        }

        let mut components: Vec<String> = Vec::new();
        let mut current = ino;
        for _ in 0..256 {
            let attr = self.vfs.getattr(current).ok()?;
            let parent = attr.parent;
            if parent == 0 || current == ROOT_INO {
                break;
            }

            let entries = self.vfs.readdir(parent).ok()?;
            let name = entries
                .iter()
                .find(|entry| entry.ino == current)
                .map(|entry| String::from_utf8_lossy(&entry.name).into_owned())?;
            components.push(name);
            current = parent;
        }

        if current != ROOT_INO {
            return None;
        }

        components.reverse();
        let separator = match self.style {
            NamespacePathStyle::Posix => "/",
            NamespacePathStyle::Windows => "\\",
        };
        Some(format!("{}{}", separator, components.join(separator)))
    }
}

/// Coordinates cache/epoch side effects for namespace mutating operations.
pub(super) struct NamespaceMutationOps<'a> {
    vfs: &'a StudioVfs,
}

impl<'a> NamespaceMutationOps<'a> {
    /// Creates a mutation coordinator tied to the current VFS.
    pub(super) fn new(vfs: &'a StudioVfs) -> Self {
        Self { vfs }
    }

    /// Records insertion: invalidates affected attr caches and stores positive dentry cache.
    pub(super) fn record_insertion(
        &self,
        parent: Ino,
        name: &[u8],
        child: Ino,
        attr_invalidations: impl IntoIterator<Item = Ino>,
    ) {
        let mut invalidated = vec![parent];
        invalidated.extend(attr_invalidations);
        self.vfs.invalidate_attr_read_cache(invalidated);
        self.vfs.bump_namespace_for_parents([parent]);
        self.vfs.dentry_cache_insert_positive(parent, name, child);
    }

    /// Records ignored insertions where filesystem state is already visible elsewhere.
    pub(super) fn record_ignored_insertion(
        &self,
        parent: Ino,
        attr_invalidations: impl IntoIterator<Item = Ino>,
    ) {
        let mut invalidated = vec![parent];
        invalidated.extend(attr_invalidations);
        self.vfs.invalidate_attr_read_cache(invalidated);
        self.vfs.bump_namespace_for_parents([parent]);
    }

    /// Records removal: invalidates caches and stores a negative dentry marker.
    pub(super) fn record_removal(
        &self,
        parent: Ino,
        name: &[u8],
        attr_invalidations: impl IntoIterator<Item = Ino>,
    ) {
        let mut invalidated = vec![parent];
        invalidated.extend(attr_invalidations);
        self.vfs.invalidate_attr_read_cache(invalidated);
        self.vfs.bump_namespace_for_parents([parent]);
        self.vfs.dentry_cache_insert_negative(parent, name);
    }

    /// Records a rename that is rejected for destination update but should invalidate source state.
    pub(super) fn record_ignored_rename(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        attr_invalidations: impl IntoIterator<Item = Ino>,
    ) {
        let mut invalidated = vec![parent];
        if parent != new_parent {
            invalidated.push(new_parent);
        }
        invalidated.extend(attr_invalidations);
        self.vfs.invalidate_attr_read_cache(invalidated);
        self.vfs.bump_namespace_for_parents([parent, new_parent]);
        self.vfs.dentry_cache_insert_negative(parent, name);
    }

    /// Records successful rename/move and updates both old and new parent cache states.
    pub(super) fn record_rename(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
        moved_ino: Ino,
    ) {
        let mut invalidated = if parent == new_parent {
            vec![parent]
        } else {
            vec![parent, new_parent]
        };
        invalidated.push(moved_ino);
        self.vfs.invalidate_attr_read_cache(invalidated);
        self.vfs.bump_namespace_for_parents([parent, new_parent]);
        self.vfs.dentry_cache_insert_negative(parent, name);
        self.vfs
            .dentry_cache_insert_positive(new_parent, new_name, moved_ino);
    }
}

/// Abstraction for backends that can invalidate inode-specific kernel cache entries.
pub trait KernelInvalidator: Send + Sync {
    /// Invalidate the full kernel cache state for the inode, including attributes and data.
    fn inval_inode(&self, ino: u64);

    /// Invalidate only kernel attribute cache state associated with the given inode.
    fn inval_inode_attrs(&self, ino: u64) {
        self.inval_inode(ino);
    }
}

/// Holds namespace-scoped caches and generation counters used for coherence.
pub(super) struct NamespaceState {
    /// Dentry cache by `(parent, name)` with positive/negative entries.
    pub(super) dentry_cache: OrderedMutex<DentryCache>,
    /// Memoized full-path lookup cache.
    pub(super) path_shortcut_cache: OrderedMutex<PathShortcutCache>,
    /// Cached inode attribute reads.
    pub(super) attr_read_cache: OrderedMutex<AttrReadCache>,
    /// Local dirty attribute snapshots awaiting persistence.
    pub(super) dirty_attrs: OrderedMutex<std::collections::HashMap<Ino, InodeAttr>>,
    /// Global namespace epoch to invalidate stale dentry/path cache entries.
    pub(super) namespace_epoch: AtomicU64,
    /// Per-directory generation map used to detect stale parent chains.
    pub(super) directory_generations: OrderedMutex<std::collections::HashMap<Ino, u64>>,
    /// Linux opt-in namespace metadata admission queue and in-memory visibility overlay.
    pub(super) deferred: DeferredNamespaceState,
}

impl NamespaceState {
    /// Creates namespace caches with configured capacity limits for all cache layers.
    pub(super) fn new(
        dentry_cache_capacity: usize,
        dentry_cache_bytes: usize,
        path_shortcut_cache_capacity: usize,
        path_shortcut_cache_bytes: usize,
        attr_read_cache_capacity: usize,
        attr_read_cache_bytes: usize,
    ) -> Self {
        Self {
            dentry_cache: OrderedMutex::new(
                DentryCache::new(dentry_cache_capacity, dentry_cache_bytes),
                LockTier::Namespace,
            ),
            path_shortcut_cache: OrderedMutex::new(
                PathShortcutCache::new(path_shortcut_cache_capacity, path_shortcut_cache_bytes),
                LockTier::Namespace,
            ),
            attr_read_cache: OrderedMutex::new(
                AttrReadCache::new(attr_read_cache_capacity, attr_read_cache_bytes),
                LockTier::Namespace,
            ),
            dirty_attrs: OrderedMutex::new(std::collections::HashMap::new(), LockTier::Namespace),
            namespace_epoch: AtomicU64::new(0),
            directory_generations: OrderedMutex::new(
                std::collections::HashMap::new(),
                LockTier::Namespace,
            ),
            deferred: DeferredNamespaceState::new(),
        }
    }
}

impl StudioVfs {
    fn namespace_cache_token_for_parent(&self, parent: Ino) -> NamespaceCacheToken {
        let parent_generation = self
            .state
            .namespace
            .directory_generations
            .lock()
            .get(&parent)
            .copied()
            .unwrap_or(0);
        NamespaceCacheToken {
            namespace_epoch: self.state.namespace.namespace_epoch.load(Ordering::Acquire),
            parent_generation,
        }
    }

    /// Increments the global namespace epoch used for cache freshness checks.
    pub(super) fn bump_namespace_epoch(&self) {
        self.state
            .namespace
            .namespace_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if current == u64::MAX {
                    warn!("namespace_epoch saturated at u64::MAX");
                }
                Some(current.saturating_add(1))
            })
            .ok();
    }

    /// Increments generation for a directory whose child set changed.
    pub(super) fn bump_directory_generation(&self, ino: Ino) {
        let mut generations = self.state.namespace.directory_generations.lock();
        let current = generations.get(&ino).copied().unwrap_or(0);
        let next = current.saturating_add(1);
        if current == u64::MAX {
            warn!(ino, "directory generation saturated at u64::MAX");
        }
        generations.insert(ino, next);
    }

    /// Bumps namespace epoch and generation for all listed parent directories.
    pub(super) fn bump_namespace_for_parents(&self, parents: impl IntoIterator<Item = Ino>) {
        self.bump_namespace_epoch();
        let mut generations = self.state.namespace.directory_generations.lock();
        let mut seen = HashSet::new();
        for parent in parents {
            if !seen.insert(parent) {
                continue;
            }
            let current = generations.get(&parent).copied().unwrap_or(0);
            let next = current.saturating_add(1);
            if current == u64::MAX {
                warn!(parent, "directory generation saturated at u64::MAX");
            }
            generations.insert(parent, next);
        }
    }

    /// Looks up `(parent,name)` in dentry cache using current namespace tokens.
    pub(super) fn dentry_cache_lookup(&self, parent: Ino, name: &[u8]) -> Option<DentryCacheValue> {
        let token = self.namespace_cache_token_for_parent(parent);
        self.state.namespace.dentry_cache.lock().get(
            parent,
            name,
            token.namespace_epoch,
            token.parent_generation,
        )
    }

    /// Caches a successful child lookup for `(parent,name)`.
    pub(super) fn dentry_cache_insert_positive(&self, parent: Ino, name: &[u8], child: Ino) {
        let token = self.namespace_cache_token_for_parent(parent);
        self.state.namespace.dentry_cache.lock().put(
            parent,
            name,
            DentryCacheValue::Positive(child),
            token.namespace_epoch,
            token.parent_generation,
        );
    }

    /// Caches a confirmed missing child for `(parent,name)`.
    pub(super) fn dentry_cache_insert_negative(&self, parent: Ino, name: &[u8]) {
        let token = self.namespace_cache_token_for_parent(parent);
        self.state.namespace.dentry_cache.lock().put(
            parent,
            name,
            DentryCacheValue::Negative,
            token.namespace_epoch,
            token.parent_generation,
        );
    }

    /// Removes one dentry entry from cache.
    pub(super) fn dentry_cache_invalidate(&self, parent: Ino, name: &[u8]) {
        self.state
            .namespace
            .dentry_cache
            .lock()
            .invalidate(parent, name);
    }

    /// Removes all dentry entries for a given parent directory.
    pub(super) fn dentry_cache_invalidate_parent(&self, parent: Ino) {
        self.state
            .namespace
            .dentry_cache
            .lock()
            .invalidate_parent(parent);
        self.state
            .namespace
            .deferred
            .inner
            .lock()
            .locally_created_dirs
            .remove(&parent);
    }

    /// Returns current dentry cache counters and capacity snapshot.
    pub(super) fn dentry_cache_metrics(&self) -> DentryCacheMetrics {
        self.state.namespace.dentry_cache.lock().metrics()
    }

    /// Reads directory generation token for inode, defaulting to `0`.
    pub(super) fn directory_generation(&self, ino: Ino) -> u64 {
        self.state
            .namespace
            .directory_generations
            .lock()
            .get(&ino)
            .copied()
            .unwrap_or(0)
    }

    /// Looks up path-shortcut cache by pre-encoded key and validates by generations.
    pub(super) fn path_shortcut_cache_lookup_by_key(
        &self,
        key: &PathShortcutCacheKey,
    ) -> Option<PathShortcutCacheValue> {
        let namespace_epoch = self.state.namespace.namespace_epoch.load(Ordering::Acquire);
        let generations = self.state.namespace.directory_generations.lock();
        self.state
            .namespace
            .path_shortcut_cache
            .lock()
            .get(key, namespace_epoch, |parent| {
                generations.get(&parent).copied().unwrap_or(0)
            })
    }

    /// Caches positive full-path resolution by key with captured parent generation tokens.
    pub(super) fn path_shortcut_cache_insert_positive_by_key(
        &self,
        key: PathShortcutCacheKey,
        child_ino: Ino,
        parent_tokens: Vec<PathShortcutParentToken>,
    ) {
        let namespace_epoch = self.state.namespace.namespace_epoch.load(Ordering::Acquire);
        self.state.namespace.path_shortcut_cache.lock().put(
            key,
            PathShortcutCacheValue::Positive(child_ino),
            namespace_epoch,
            parent_tokens,
        );
    }

    /// Caches negative full-path resolution by key with missing `(parent,name)` pair.
    pub(super) fn path_shortcut_cache_insert_negative_by_key(
        &self,
        key: PathShortcutCacheKey,
        parent: Ino,
        name: &[u8],
        parent_tokens: Vec<PathShortcutParentToken>,
    ) {
        let namespace_epoch = self.state.namespace.namespace_epoch.load(Ordering::Acquire);
        self.state.namespace.path_shortcut_cache.lock().put(
            key,
            PathShortcutCacheValue::Negative {
                parent,
                name: Arc::from(name),
            },
            namespace_epoch,
            parent_tokens,
        );
    }

    /// Returns current path-shortcut cache counters and capacity snapshot.
    pub(super) fn path_shortcut_cache_metrics(&self) -> PathShortcutCacheMetrics {
        self.state.namespace.path_shortcut_cache.lock().metrics()
    }

    /// Creates a resolver bound to this VFS for the requested path style.
    pub fn namespace_resolver(&self, style: NamespacePathStyle) -> NamespaceResolver<'_> {
        NamespaceResolver::new(self, style)
    }

    /// Returns namespace mutation coordinator helper.
    pub(super) fn namespace_mutations(&self) -> NamespaceMutationOps<'_> {
        NamespaceMutationOps::new(self)
    }

    /// Installs a kernel invalidation callback used by bridge backends.
    pub fn set_kernel_invalidator(&self, inv: Arc<dyn KernelInvalidator>) {
        *self.kernel_invalidator.write() = Some(inv);
    }

    /// Clears the kernel invalidation callback.
    pub fn clear_kernel_invalidator(&self) {
        *self.kernel_invalidator.write() = None;
    }
}

#[cfg(test)]
mod tests {
    use super::super::local_dirty::PendingPayload;
    use super::*;
    use bytes::BytesMut;
    use std::sync::Arc;
    use studiofs_meta::engine::MetaFsOps;
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig};
    use studiofs_meta::{MetaFormat, MetaInodes, MetaNamespace, RedbMetaEngine};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("namespace-resolver-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("namespace-resolver-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            encryption: studiofs_meta::EncryptionConfig::new(
                [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
            ),
            block_size: 4 * 1024 * 1024,
        }
    }

    fn make_unstarted_test_vfs(
        tmp: &TempDir,
        volume_suffix: &str,
    ) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
        make_unstarted_test_vfs_with_config(tmp, volume_suffix, VfsConfig::default())
    }

    fn make_unstarted_test_vfs_with_config(
        tmp: &TempDir,
        volume_suffix: &str,
        cfg: VfsConfig,
    ) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info(volume_suffix);
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);

        let cache_dir = tmp.path().join("cache");
        let rt = Arc::new(Runtime::new().expect("runtime"));
        let _guard = rt.enter();
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            cfg.try_into().expect("test VFS config should validate"),
        )
        .expect("create vfs");
        (rt, vfs, meta)
    }

    fn make_test_vfs(
        tmp: &TempDir,
        volume_suffix: &str,
    ) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
        let (rt, vfs, meta) = make_unstarted_test_vfs(tmp, volume_suffix);
        let _guard = rt.enter();
        vfs.start().expect("start vfs background workers");
        (rt, vfs, meta)
    }

    fn make_test_vfs_with_config(
        tmp: &TempDir,
        volume_suffix: &str,
        cfg: VfsConfig,
    ) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
        let (rt, vfs, meta) = make_unstarted_test_vfs_with_config(tmp, volume_suffix, cfg);
        let _guard = rt.enter();
        vfs.start().expect("start vfs background workers");
        (rt, vfs, meta)
    }

    #[test]
    fn namespace_mutex_lock_populates_lock_order_stack() {
        let state = NamespaceState::new(8, 1024, 8, 1024, 8, 1024);

        let guard = state.attr_read_cache.lock();

        assert_eq!(crate::debug_lock_stack(), vec![crate::LockTier::Namespace]);

        drop(guard);
        assert!(crate::debug_lock_stack().is_empty());
    }

    #[test]
    fn dentry_cache_is_bounded_and_supports_negative_entries() {
        let single_charge = DentryCache::estimate_entry_charge(b"a");
        let mut cache = DentryCache::new(2, single_charge * 4);

        cache.put(ROOT_INO, b"a", DentryCacheValue::Positive(11), 0, 0);
        cache.put(ROOT_INO, b"b", DentryCacheValue::Negative, 0, 0);
        assert_eq!(cache.entry_len(), 2);
        assert_eq!(
            cache.get(ROOT_INO, b"a", 0, 0),
            Some(DentryCacheValue::Positive(11))
        );
        assert_eq!(
            cache.get(ROOT_INO, b"b", 0, 0),
            Some(DentryCacheValue::Negative)
        );

        cache.put(ROOT_INO, b"c", DentryCacheValue::Positive(33), 0, 0);
        assert_eq!(cache.entry_len(), 2);
        assert_eq!(cache.get(ROOT_INO, b"a", 0, 0), None);
        assert_eq!(
            cache.get(ROOT_INO, b"b", 0, 0),
            Some(DentryCacheValue::Negative)
        );
        assert_eq!(
            cache.get(ROOT_INO, b"c", 0, 0),
            Some(DentryCacheValue::Positive(33))
        );
    }

    #[test]
    fn dentry_cache_enforces_byte_budget_under_scan() {
        let charge = DentryCache::estimate_entry_charge(b"k0000");
        let mut cache = DentryCache::new(1024, charge * 32);

        for i in 0..2000usize {
            let key = format!("k{i:04}");
            cache.put(
                ROOT_INO,
                key.as_bytes(),
                DentryCacheValue::Positive(i as u64),
                0,
                0,
            );
        }

        let metrics = cache.metrics();
        assert!(metrics.occupancy_entries <= metrics.entry_budget);
        assert!(metrics.occupancy_bytes <= metrics.byte_budget);
    }

    #[test]
    fn dentry_cache_tracks_hits_misses_and_negative_ratio() {
        let charge = DentryCache::estimate_entry_charge(b"a");
        let mut cache = DentryCache::new(1, charge * 8);

        assert_eq!(cache.get(ROOT_INO, b"missing", 0, 0), None);
        cache.put(ROOT_INO, b"a", DentryCacheValue::Negative, 0, 0);
        assert_eq!(
            cache.get(ROOT_INO, b"a", 0, 0),
            Some(DentryCacheValue::Negative)
        );
        cache.put(ROOT_INO, b"b", DentryCacheValue::Positive(22), 0, 0);

        let metrics = cache.metrics();
        assert_eq!(metrics.hits, 1);
        assert_eq!(metrics.misses, 1);
        assert_eq!(metrics.negative_hits, 1);
        assert_eq!(metrics.evictions, 1);
        assert!((metrics.negative_hit_ratio() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn dentry_cache_tracks_hotset_occupancy_and_hits() {
        let charge = DentryCache::estimate_entry_charge(b"hot");
        let mut cache = DentryCache::new(8, charge * 64);
        cache.put(ROOT_INO, b"hot", DentryCacheValue::Positive(1), 0, 0);
        let _ = cache.get(ROOT_INO, b"hot", 0, 0);
        let _ = cache.get(ROOT_INO, b"hot", 0, 0);

        let metrics = cache.metrics();
        assert!(metrics.protected_occupancy_entries >= 1);
        assert!(metrics.protected_hits >= 1);
        assert!(metrics.hot_hit_ratio() > 0.0);
    }

    #[test]
    fn dentry_cache_compacts_lazy_orders_without_budget_pressure() {
        let charge = DentryCache::estimate_entry_charge(b"hot");
        let mut cache = DentryCache::new(8, charge * 128);
        cache.put(ROOT_INO, b"hot", DentryCacheValue::Positive(1), 0, 0);

        for _ in 0..5000 {
            assert_eq!(
                cache.get(ROOT_INO, b"hot", 0, 0),
                Some(DentryCacheValue::Positive(1))
            );
        }

        assert!(
            cache.protected_order_len() <= DENTRY_CACHE_ORDER_COMPACT_SLACK + 2,
            "protected lazy queue should stay compact under steady-state hits"
        );
    }

    #[test]
    fn dentry_cache_key_clone_is_refcounted() {
        let key = DentryCacheKey::new(ROOT_INO, b"shared-name");
        let cloned = key.clone();
        assert!(
            Arc::ptr_eq(&key.name, &cloned.name),
            "key clone should not duplicate name bytes"
        );
    }

    #[test]
    fn dentry_cache_rejects_stale_namespace_tokens_lazily() {
        let charge = DentryCache::estimate_entry_charge(b"entry");
        let mut cache = DentryCache::new(8, charge * 32);
        cache.put(ROOT_INO, b"entry", DentryCacheValue::Positive(7), 1, 4);

        assert_eq!(
            cache.get(ROOT_INO, b"entry", 1, 4),
            Some(DentryCacheValue::Positive(7))
        );
        assert_eq!(cache.get(ROOT_INO, b"entry", 2, 4), None);
        assert_eq!(
            cache.get(ROOT_INO, b"entry", 2, 4),
            None,
            "stale entry should be removed on first stale hit"
        );
        assert_eq!(
            cache.metrics().evictions,
            0,
            "stale invalidation should not count as eviction"
        );
    }

    #[test]
    fn path_shortcut_cache_is_bounded_and_supports_negative_entries() {
        let key_a = PathShortcutCacheKey::from_components(
            NamespacePathStyle::Posix,
            &[String::from("a"), String::from("b")],
        )
        .expect("path key a");
        let key_b = PathShortcutCacheKey::from_components(
            NamespacePathStyle::Posix,
            &[String::from("a"), String::from("missing")],
        )
        .expect("path key b");
        let tokens = vec![PathShortcutParentToken {
            parent: ROOT_INO,
            generation: 0,
        }];
        let single_charge = PathShortcutCache::estimate_entry_charge(
            &key_a,
            &PathShortcutCacheValue::Positive(11),
            &tokens,
        );
        let mut cache = PathShortcutCache::new(2, single_charge * 4);

        cache.put(
            key_a.clone(),
            PathShortcutCacheValue::Positive(11),
            0,
            tokens.clone(),
        );
        cache.put(
            key_b.clone(),
            PathShortcutCacheValue::Negative {
                parent: ROOT_INO,
                name: Arc::from(&b"missing"[..]),
            },
            0,
            tokens.clone(),
        );
        assert_eq!(cache.entry_len(), 2);
        assert!(matches!(
            cache.get(&key_a, 0, |_| 0),
            Some(PathShortcutCacheValue::Positive(11))
        ));
        assert!(matches!(
            cache.get(&key_b, 0, |_| 0),
            Some(PathShortcutCacheValue::Negative { .. })
        ));

        let key_c = PathShortcutCacheKey::from_components(
            NamespacePathStyle::Posix,
            &[String::from("a"), String::from("c")],
        )
        .expect("path key c");
        cache.put(
            key_c.clone(),
            PathShortcutCacheValue::Positive(33),
            0,
            tokens,
        );
        assert_eq!(cache.entry_len(), 2);
        assert!(cache.get(&key_c, 0, |_| 0).is_some());
    }

    #[test]
    fn path_shortcut_cache_rejects_stale_parent_generations() {
        let key = PathShortcutCacheKey::from_components(
            NamespacePathStyle::Posix,
            &[
                String::from("a"),
                String::from("b"),
                String::from("file.txt"),
            ],
        )
        .expect("path key");
        let tokens = vec![
            PathShortcutParentToken {
                parent: ROOT_INO,
                generation: 1,
            },
            PathShortcutParentToken {
                parent: 42,
                generation: 7,
            },
        ];
        let charge = PathShortcutCache::estimate_entry_charge(
            &key,
            &PathShortcutCacheValue::Positive(9),
            &tokens,
        );
        let mut cache = PathShortcutCache::new(8, charge * 8);
        cache.put(key.clone(), PathShortcutCacheValue::Positive(9), 3, tokens);

        assert!(
            cache
                .get(&key, 3, |ino| if ino == 42 { 7 } else { 1 })
                .is_some()
        );
        assert!(
            cache
                .get(&key, 3, |ino| if ino == 42 { 8 } else { 1 })
                .is_none(),
            "parent generation drift must invalidate cached path"
        );
        assert_eq!(
            cache.metrics().evictions,
            0,
            "stale invalidation should not count as eviction"
        );
    }

    #[test]
    fn path_shortcut_cache_tracks_hotset_occupancy_and_hits() {
        let key = PathShortcutCacheKey::from_components(
            NamespacePathStyle::Posix,
            &[String::from("hot"), String::from("path")],
        )
        .expect("path key");
        let tokens = vec![PathShortcutParentToken {
            parent: ROOT_INO,
            generation: 0,
        }];
        let charge = PathShortcutCache::estimate_entry_charge(
            &key,
            &PathShortcutCacheValue::Positive(9),
            &tokens,
        );
        let mut cache = PathShortcutCache::new(8, charge * 32);
        cache.put(
            key.clone(),
            PathShortcutCacheValue::Positive(9),
            1,
            tokens.clone(),
        );
        let _ = cache.get(&key, 1, |_| 0);
        let _ = cache.get(&key, 1, |_| 0);

        let metrics = cache.metrics();
        assert!(metrics.protected_occupancy_entries >= 1);
        assert!(metrics.protected_hits >= 1);
        assert!(metrics.hot_hit_ratio() > 0.0);
    }

    #[test]
    fn readdir_hydrates_attr_cache_for_followup_getattr() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "readdir-attr-hydration");
        let created = vfs
            .create(ROOT_INO, b"batch-stat.txt", 1000, 1000, 0o644)
            .expect("create file");

        let entries = vfs.readdir(ROOT_INO).expect("readdir root");
        assert!(
            entries.iter().any(|entry| entry.ino == created.ino),
            "test file should be visible in root readdir"
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut cached_after_readdir = false;
        while Instant::now() < deadline {
            if vfs
                .state
                .namespace
                .attr_read_cache
                .lock()
                .contains(created.ino)
            {
                cached_after_readdir = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            cached_after_readdir,
            "readdir should hydrate attr cache for listed children"
        );

        let before = vfs.attr_read_cache_metrics();
        let loaded = vfs.getattr(created.ino).expect("getattr");
        let after = vfs.attr_read_cache_metrics();
        assert_eq!(loaded.ino, created.ino);
        assert!(
            after.hits > before.hits,
            "follow-up getattr should hit attr cache after readdir hydration"
        );
    }

    #[test]
    fn readdir_with_attrs_returns_child_attrs_from_same_snapshot() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "readdir-with-attrs");
        let created = vfs
            .create(ROOT_INO, b"batch-stat.txt", 1000, 1000, 0o640)
            .expect("create file");

        let entries = vfs.readdir_with_attrs(ROOT_INO).expect("readdir root");
        let listed = entries
            .iter()
            .find(|entry| entry.entry.ino == created.ino)
            .expect("created file should be listed");

        assert_eq!(
            listed.attr.as_ref().expect("listed attr").mode & 0o777,
            0o640
        );
    }

    #[test]
    fn readdir_with_attrs_respects_attr_batch_cap() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let cfg = VfsConfig {
            readdir_attr_batch_chunk: 1,
            readdir_attr_batch_max_inodes: 1,
            ..VfsConfig::default()
        };
        let (_rt, vfs, _meta) = make_test_vfs_with_config(&tmp, "readdir-with-attrs-cap", cfg);
        let first = vfs
            .create(ROOT_INO, b"cap-first.txt", 1000, 1000, 0o640)
            .expect("create first file");
        let second = vfs
            .create(ROOT_INO, b"cap-second.txt", 1000, 1000, 0o640)
            .expect("create second file");

        let entries = vfs.readdir_with_attrs(ROOT_INO).expect("readdir root");
        let created_entries = entries
            .iter()
            .filter(|entry| entry.entry.ino == first.ino || entry.entry.ino == second.ino)
            .collect::<Vec<_>>();

        assert_eq!(created_entries.len(), 2);
        assert_eq!(
            created_entries
                .iter()
                .filter(|entry| entry.attr.is_some())
                .count(),
            1
        );
    }

    #[test]
    fn readdir_with_attrs_keeps_dangling_entries_without_attr() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, meta) = make_test_vfs(&tmp, "readdir-with-attrs-dangling");
        let created = vfs
            .create(ROOT_INO, b"dangling.txt", 1000, 1000, 0o644)
            .expect("create file");
        meta.remove_attr(created.ino)
            .expect("remove attr while leaving dentry behind");

        let entries = vfs.readdir_with_attrs(ROOT_INO).expect("readdir root");
        let listed = entries
            .iter()
            .find(|entry| entry.entry.name.as_slice() == b"dangling.txt")
            .expect("dangling dentry should stay visible");

        assert!(listed.attr.is_none());
    }

    #[test]
    fn readdir_keeps_dangling_dentry_but_evicts_stale_attr_cache() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, meta) = make_test_vfs(&tmp, "readdir-stale-attr-dangling-dentry");
        let created = vfs
            .create(ROOT_INO, b"dangling-cached.txt", 1000, 1000, 0o644)
            .expect("create file");
        vfs.getattr(created.ino).expect("prime attr cache");
        assert!(
            vfs.state
                .namespace
                .attr_read_cache
                .lock()
                .contains(created.ino),
            "test setup should prime the attr cache"
        );

        meta.remove_attr(created.ino)
            .expect("remove attr while leaving dentry behind");

        let entries = vfs.readdir(ROOT_INO).expect("readdir root");
        assert!(
            entries
                .iter()
                .any(|entry| entry.name.as_slice() == b"dangling-cached.txt"),
            "readdir should not perform inode-existence healing"
        );
        assert!(
            !vfs.state
                .namespace
                .attr_read_cache
                .lock()
                .contains(created.ino),
            "readdir should evict stale attr cache entries for dangling dentries"
        );
        assert!(
            meta.lookup(ROOT_INO, b"dangling-cached.txt").is_ok(),
            "readdir should leave the dangling dentry for foreground repair"
        );
    }

    #[test]
    fn rmdir_heals_dangling_children_before_empty_check() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, meta) = make_test_vfs(&tmp, "rmdir-dangling-child");
        let dir = vfs
            .mkdir(ROOT_INO, b"parent", 1000, 1000, 0o755)
            .expect("mkdir parent");
        let child = vfs
            .create(dir.ino, b"dangling.txt", 1000, 1000, 0o644)
            .expect("create child");
        meta.remove_attr(child.ino)
            .expect("remove child attr while leaving dentry");

        vfs.rmdir(ROOT_INO, b"parent")
            .expect("rmdir should heal dangling children");

        assert!(meta.lookup(ROOT_INO, b"parent").is_err());
    }

    #[test]
    fn readdir_invalidates_stale_negative_path_shortcut_for_observed_child() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, meta) = make_test_vfs(&tmp, "readdir-stale-negative-path-shortcut");
        let dir = vfs
            .mkdir(ROOT_INO, b"observer", 1000, 1000, 0o755)
            .expect("mkdir observer");
        let resolver = vfs.namespace_resolver(NamespacePathStyle::Windows);

        assert!(
            resolver
                .resolve_path(r"\observer\rename-source.txt")
                .is_err(),
            "initial miss should seed negative dentry and path-shortcut caches"
        );
        assert_eq!(
            vfs.dentry_cache_lookup(dir.ino, b"rename-source.txt"),
            Some(DentryCacheValue::Negative)
        );

        let created = meta
            .create_file(dir.ino, b"rename-source.txt", 1000, 1000, 0o644)
            .expect("simulate remotely projected file");
        let before_generation = vfs.directory_generation(dir.ino);
        let entries = vfs.readdir(dir.ino).expect("readdir observer dir");
        assert!(
            entries.iter().any(|entry| entry.ino == created.ino),
            "readdir should observe the remotely projected child"
        );
        assert!(
            vfs.directory_generation(dir.ino) > before_generation,
            "observing a child over a stale negative dentry should invalidate parent generation"
        );

        let resolved = resolver
            .resolve_path(r"\observer\rename-source.txt")
            .expect("readdir should invalidate stale negative full-path cache");
        assert_eq!(resolved.ino, created.ino);
    }

    #[test]
    fn remote_upload_placeholder_visibility_is_disabled_before_first_publish() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "remote-upload-placeholder-visibility");
        let ino = 9_201;
        let mut attr = InodeAttr::new_file(ino, ROOT_INO, 1000, 1000, 0o644);
        attr.upload_state = MetaUploadState::Uploading;
        attr.size = 0;
        attr.committed_size = 0;

        assert!(
            !vfs.remote_upload_placeholder_visible(ino, &attr),
            "remote namespace should stay hidden until the first published version exists"
        );

        vfs.state.local_dirty.pending_payloads.lock().insert(
            (ino, 0),
            PendingPayload {
                data: BytesMut::from(&b"local"[..]),
                dirty: true,
                reserved_object_id: None,
                persisted_len: None,
            },
        );

        assert!(
            !vfs.remote_upload_placeholder_visible(ino, &attr),
            "local overlay state should not re-enable the retired remote placeholder path"
        );
    }

    #[test]
    fn namespace_mutations_update_dentry_cache_entries() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "dentry-cache-hooks");
        let key = DentryCacheKey::new(ROOT_INO, b"cache-test.txt");

        assert!(
            vfs.lookup(ROOT_INO, b"cache-test.txt").is_err(),
            "initial lookup should miss"
        );
        {
            let cache = vfs.state.namespace.dentry_cache.lock();
            assert_eq!(cache.entry_value(&key), Some(DentryCacheValue::Negative));
        }

        let created = vfs
            .create(ROOT_INO, b"cache-test.txt", 1000, 1000, 0o644)
            .expect("create file");
        {
            let cache = vfs.state.namespace.dentry_cache.lock();
            assert_eq!(
                cache.entry_value(&key),
                Some(DentryCacheValue::Positive(created.ino))
            );
        }

        vfs.unlink(ROOT_INO, b"cache-test.txt")
            .expect("unlink file");
        {
            let cache = vfs.state.namespace.dentry_cache.lock();
            assert_eq!(cache.entry_value(&key), Some(DentryCacheValue::Negative));
        }
    }

    #[test]
    fn unlink_invalidates_parent_attr_cache() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "unlink-invalidates-parent-attr-cache");
        let dir = vfs
            .mkdir(ROOT_INO, b"dir", 1000, 1000, 0o755)
            .expect("mkdir dir");
        vfs.create(dir.ino, b"gone.txt", 1000, 1000, 0o644)
            .expect("create file");

        let _ = vfs.getattr(dir.ino).expect("prime parent attr cache");
        assert!(
            vfs.state.namespace.attr_read_cache.lock().contains(dir.ino),
            "parent dir should be cached before unlink"
        );

        vfs.unlink(dir.ino, b"gone.txt").expect("unlink file");

        assert!(
            !vfs.state.namespace.attr_read_cache.lock().contains(dir.ino),
            "unlink should invalidate parent directory attr cache"
        );
    }

    #[test]
    fn link_invalidates_new_parent_attr_cache() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "link-invalidates-parent-attr-cache");
        let source = vfs
            .create(ROOT_INO, b"source.txt", 1000, 1000, 0o644)
            .expect("create source");
        let dir = vfs
            .mkdir(ROOT_INO, b"links", 1000, 1000, 0o755)
            .expect("mkdir links");

        let _ = vfs.getattr(dir.ino).expect("prime new parent attr cache");
        assert!(
            vfs.state.namespace.attr_read_cache.lock().contains(dir.ino),
            "new parent dir should be cached before link"
        );

        vfs.link(source.ino, dir.ino, b"linked.txt")
            .expect("create hardlink");

        assert!(
            !vfs.state.namespace.attr_read_cache.lock().contains(dir.ino),
            "link should invalidate destination parent attr cache"
        );
    }

    #[test]
    fn setattr_does_not_bump_namespace_generations() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "setattr-no-namespace-invalidate");
        let created = vfs
            .create(ROOT_INO, b"setattr.txt", 1000, 1000, 0o644)
            .expect("create file");
        let key = DentryCacheKey::new(ROOT_INO, b"setattr.txt");

        let _ = vfs
            .lookup(ROOT_INO, b"setattr.txt")
            .expect("prime dentry cache");
        let before_epoch = vfs.state.namespace.namespace_epoch.load(Ordering::SeqCst);
        let before_root_generation = vfs
            .state
            .namespace
            .directory_generations
            .lock()
            .get(&ROOT_INO)
            .copied()
            .unwrap_or(0);

        vfs.setattr(
            created.ino,
            SetattrArgs {
                mode: Some(0o600),
                ..SetattrArgs::default()
            },
        )
        .expect("setattr");

        let after_epoch = vfs.state.namespace.namespace_epoch.load(Ordering::SeqCst);
        let after_root_generation = vfs
            .state
            .namespace
            .directory_generations
            .lock()
            .get(&ROOT_INO)
            .copied()
            .unwrap_or(0);
        assert_eq!(before_epoch, after_epoch);
        assert_eq!(before_root_generation, after_root_generation);

        let cache = vfs.state.namespace.dentry_cache.lock();
        assert_eq!(
            cache.entry_value(&key),
            Some(DentryCacheValue::Positive(created.ino))
        );
    }

    #[test]
    fn write_does_not_bump_namespace_generations() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "write-no-namespace-invalidate");
        let created = vfs
            .create(ROOT_INO, b"write.txt", 1000, 1000, 0o644)
            .expect("create file");
        let key = DentryCacheKey::new(ROOT_INO, b"write.txt");

        let _ = vfs
            .lookup(ROOT_INO, b"write.txt")
            .expect("prime dentry cache");
        let before_epoch = vfs.state.namespace.namespace_epoch.load(Ordering::SeqCst);
        let before_root_generation = vfs
            .state
            .namespace
            .directory_generations
            .lock()
            .get(&ROOT_INO)
            .copied()
            .unwrap_or(0);

        rt.block_on(async {
            vfs.write(created.ino, 0, b"payload").await.expect("write");
        });

        let after_epoch = vfs.state.namespace.namespace_epoch.load(Ordering::SeqCst);
        let after_root_generation = vfs
            .state
            .namespace
            .directory_generations
            .lock()
            .get(&ROOT_INO)
            .copied()
            .unwrap_or(0);
        assert_eq!(before_epoch, after_epoch);
        assert_eq!(before_root_generation, after_root_generation);

        let cache = vfs.state.namespace.dentry_cache.lock();
        assert_eq!(
            cache.entry_value(&key),
            Some(DentryCacheValue::Positive(created.ino))
        );
    }

    #[test]
    fn namespace_resolver_path_shortcut_hits_and_renames_invalidate() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "path-shortcut-rename-invalidate");

        let a = vfs
            .mkdir(ROOT_INO, b"a", 1000, 1000, 0o755)
            .expect("mkdir a");
        let b = vfs.mkdir(a.ino, b"b", 1000, 1000, 0o755).expect("mkdir b");
        let file = vfs
            .create(b.ino, b"file.txt", 1000, 1000, 0o644)
            .expect("create file");

        let resolver = vfs.namespace_resolver(NamespacePathStyle::Posix);
        let first = resolver
            .resolve_path("/a/b/file.txt")
            .expect("resolve first path");
        let second = resolver
            .resolve_path("/a/b/file.txt")
            .expect("resolve warm path");
        assert_eq!(first.ino, file.ino);
        assert_eq!(second.ino, file.ino);
        let metrics = vfs.path_shortcut_cache_metrics();
        assert!(
            metrics.hits >= 1,
            "second full-path resolution should hit path shortcut cache"
        );

        vfs.rename(ROOT_INO, b"a", ROOT_INO, b"x")
            .expect("rename subtree root");
        assert!(
            resolver.resolve_path("/a/b/file.txt").is_err(),
            "old path must stop resolving after rename"
        );
        let moved = resolver
            .resolve_path("/x/b/file.txt")
            .expect("new path resolves after rename");
        assert_eq!(moved.ino, file.ino);
    }

    #[test]
    fn namespace_resolver_path_shortcut_invalidation_on_delete_subtree() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "path-shortcut-delete-invalidate");

        let parent = vfs
            .mkdir(ROOT_INO, b"parent", 1000, 1000, 0o755)
            .expect("mkdir parent");
        let child = vfs
            .mkdir(parent.ino, b"child", 1000, 1000, 0o755)
            .expect("mkdir child");
        vfs.create(child.ino, b"gone.txt", 1000, 1000, 0o644)
            .expect("create file");

        let resolver = vfs.namespace_resolver(NamespacePathStyle::Posix);
        resolver
            .resolve_path("/parent/child/gone.txt")
            .expect("warm full path");
        vfs.unlink(child.ino, b"gone.txt")
            .expect("unlink leaf file");
        assert!(
            resolver.resolve_path("/parent/child/gone.txt").is_err(),
            "deleted path must not resolve from stale shortcut"
        );
    }

    #[test]
    fn namespace_resolver_full_path_matches_component_lookup_for_ignored_and_blocked_paths() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, meta) = make_test_vfs(&tmp, "fullpath-component-parity");

        let ignored_dir = vfs
            .mkdir(ROOT_INO, b".Spotlight-V100", 1000, 1000, 0o755)
            .expect("mkdir ignored root metadata dir");
        let ignored_child = vfs
            .mkdir(ignored_dir.ino, b"Store-V2", 1000, 1000, 0o755)
            .expect("mkdir child under ignored dir");

        let resolver = vfs.namespace_resolver(NamespacePathStyle::Windows);
        let full_path_attr = resolver
            .resolve_path("\\.Spotlight-V100\\Store-V2")
            .expect("resolve ignored subtree");
        let component_attr = vfs
            .lookup(
                vfs.lookup(ROOT_INO, b".Spotlight-V100").unwrap().ino,
                b"Store-V2",
            )
            .expect("component lookup ignored subtree");
        assert_eq!(full_path_attr.ino, ignored_child.ino);
        assert_eq!(component_attr.ino, ignored_child.ino);

        meta.create_dir(ROOT_INO, b".Trashes", 1000, 1000, 0o755)
            .expect("seed blocked root dir in metadata");
        assert!(
            vfs.lookup(ROOT_INO, b".Trashes").is_err(),
            "trash-like root entries should be hidden from component lookup"
        );
        assert!(
            resolver.resolve_path("\\.Trashes").is_err(),
            "trash-like root entries should be hidden from full-path resolution"
        );
    }

    #[test]
    fn namespace_resolver_windows_and_posix_match_on_same_namespace_entry() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "windows-posix-parity");

        let dir = vfs
            .mkdir(ROOT_INO, b"docs", 1000, 1000, 0o755)
            .expect("mkdir docs");
        let file = vfs
            .create(dir.ino, b"readme.txt", 1000, 1000, 0o644)
            .expect("create readme");

        let posix = vfs
            .namespace_resolver(NamespacePathStyle::Posix)
            .resolve_path("/docs/readme.txt")
            .expect("resolve posix path");
        let windows = vfs
            .namespace_resolver(NamespacePathStyle::Windows)
            .resolve_path("\\docs\\readme.txt")
            .expect("resolve windows path");
        assert_eq!(posix.ino, file.ino);
        assert_eq!(windows.ino, file.ino);
    }

    #[test]
    fn namespace_resolver_windows_parent_normalization_matches_winfsp_semantics() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "windows-parent-normalization");

        let dir = vfs
            .mkdir(ROOT_INO, b"folder", 1000, 1000, 0o755)
            .expect("mkdir folder");
        let resolved = vfs
            .namespace_resolver(NamespacePathStyle::Windows)
            .resolve_parent("\\folder\\child.txt.  ")
            .expect("resolve parent with trailing dot/space");

        assert_eq!(resolved.parent, dir.ino);
        assert_eq!(resolved.name, b"child.txt".to_vec());
    }

    #[test]
    fn hidden_platform_root_metadata_name_matches_trashes_only() {
        assert!(is_hidden_platform_root_metadata_name(b".Trashes"));
        assert!(!is_hidden_platform_root_metadata_name(b".Trash"));
        assert!(!is_hidden_platform_root_metadata_name(b".Spotlight-V100"));
        assert!(!is_hidden_platform_root_metadata_name(b"visible"));
    }

    #[test]
    fn heal_dangling_entry_if_inode_missing_prunes_stale_root_trashes_dentry() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, meta) = make_test_vfs(&tmp, "heal-hidden-root-trashes");

        let trashes = meta
            .create_dir(ROOT_INO, b".Trashes", 1000, 1000, 0o755)
            .expect("seed .Trashes");
        meta.remove_attr(trashes.ino)
            .expect("remove .Trashes attr to simulate stale hidden dentry");

        assert!(
            meta.lookup(ROOT_INO, b".Trashes").is_ok(),
            "test setup requires a dentry that still points to the removed inode"
        );

        assert!(
            vfs.heal_dangling_entry_if_inode_missing(ROOT_INO, b".Trashes")
                .expect("heal stale .Trashes"),
            "stale hidden root entry should be healed"
        );
        assert!(meta.lookup(ROOT_INO, b".Trashes").is_err());
    }
}
