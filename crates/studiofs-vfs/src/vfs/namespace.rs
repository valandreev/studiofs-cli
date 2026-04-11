use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NamespacePathStyle {
    Posix,
    Windows,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceParent {
    pub parent: Ino,
    pub name: Vec<u8>,
}

pub struct NamespaceResolver<'a> {
    vfs: &'a StudioVfs,
    style: NamespacePathStyle,
}

impl<'a> NamespaceResolver<'a> {
    pub(super) fn new(vfs: &'a StudioVfs, style: NamespacePathStyle) -> Self {
        Self { vfs, style }
    }

    fn path_components(&self, path: &str) -> Vec<String> {
        let stripped = match self.style {
            NamespacePathStyle::Posix => path.trim_start_matches('/'),
            NamespacePathStyle::Windows => path.trim_start_matches('\\'),
        };

        let mut components: Vec<String> = Vec::new();
        let parts: Box<dyn Iterator<Item = &str>> = match self.style {
            NamespacePathStyle::Posix => Box::new(stripped.split('/')),
            NamespacePathStyle::Windows => Box::new(stripped.split('\\')),
        };

        for raw in parts {
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

    pub fn lookup_component(&self, parent: Ino, name: &[u8]) -> VfsResult<InodeAttr> {
        self.vfs.lookup_component_impl(parent, name)
    }

    pub fn resolve_path(&self, path: &str) -> VfsResult<InodeAttr> {
        let components = self.path_components(path);
        self.resolve_components_cached(&components)
    }

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

pub(super) struct NamespaceMutationOps<'a> {
    vfs: &'a StudioVfs,
}

impl<'a> NamespaceMutationOps<'a> {
    pub(super) fn new(vfs: &'a StudioVfs) -> Self {
        Self { vfs }
    }

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

    pub(super) fn record_rename(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
        moved_ino: Ino,
    ) {
        let invalidated = if parent == new_parent {
            vec![parent]
        } else {
            vec![parent, new_parent]
        };
        self.vfs.invalidate_attr_read_cache(invalidated);
        self.vfs.bump_namespace_for_parents([parent, new_parent]);
        self.vfs.dentry_cache_insert_negative(parent, name);
        self.vfs
            .dentry_cache_insert_positive(new_parent, new_name, moved_ino);
    }
}

pub trait KernelInvalidator: Send + Sync {
    fn inval_inode(&self, ino: u64);
}

pub(super) struct NamespaceState {
    pub(super) dentry_cache: Mutex<DentryCache>,
    pub(super) path_shortcut_cache: Mutex<PathShortcutCache>,
    pub(super) attr_read_cache: Mutex<AttrReadCache>,
    pub(super) dirty_attrs: Mutex<std::collections::HashMap<Ino, InodeAttr>>,
    pub(super) namespace_epoch: AtomicU64,
    pub(super) directory_generations: Mutex<std::collections::HashMap<Ino, u64>>,
}

impl NamespaceState {
    pub(super) fn new(
        dentry_cache_capacity: usize,
        dentry_cache_bytes: usize,
        path_shortcut_cache_capacity: usize,
        path_shortcut_cache_bytes: usize,
        attr_read_cache_capacity: usize,
        attr_read_cache_bytes: usize,
    ) -> Self {
        Self {
            dentry_cache: Mutex::new(DentryCache::new(dentry_cache_capacity, dentry_cache_bytes)),
            path_shortcut_cache: Mutex::new(PathShortcutCache::new(
                path_shortcut_cache_capacity,
                path_shortcut_cache_bytes,
            )),
            attr_read_cache: Mutex::new(AttrReadCache::new(
                attr_read_cache_capacity,
                attr_read_cache_bytes,
            )),
            dirty_attrs: Mutex::new(std::collections::HashMap::new()),
            namespace_epoch: AtomicU64::new(0),
            directory_generations: Mutex::new(std::collections::HashMap::new()),
        }
    }
}

impl StudioVfs {
    pub fn namespace_resolver(&self, style: NamespacePathStyle) -> NamespaceResolver<'_> {
        NamespaceResolver::new(self, style)
    }

    pub(super) fn namespace_mutations(&self) -> NamespaceMutationOps<'_> {
        NamespaceMutationOps::new(self)
    }

    pub fn set_kernel_invalidator(&self, inv: Arc<dyn KernelInvalidator>) {
        *self.kernel_invalidator.write() = Some(inv);
    }

    pub fn clear_kernel_invalidator(&self) {
        *self.kernel_invalidator.write() = None;
    }
}
