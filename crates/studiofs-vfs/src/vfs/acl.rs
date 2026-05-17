use super::{
    AccessLevel, AtomicU64, Ino, MetaError, MetaResult, Mutex, Ordering, ROOT_INO, StudioVfs,
    VfsError, VfsResult, run_meta,
};
use crate::AclMeta;
use std::collections::HashMap;

/// Shared state used by ACL computations, including caches and cache invalidation epochs.
pub(super) struct AclState {
    /// Cache of effective ACL values computed per inode.
    pub(super) effective_acl_cache: Mutex<HashMap<Ino, EffectiveAclCacheEntry>>,
    /// Epoch counter invalidating access-level cache entries when ACL entries change.
    pub(super) acl_epoch: AtomicU64,
    /// Epoch counter invalidating access-level cache entries when namespace ACL policy changes.
    pub(super) acl_namespace_epoch: AtomicU64,
}

impl AclState {
    /// Creates a fresh ACL state with empty cache and zeroed epochs.
    pub(super) fn new() -> Self {
        Self {
            effective_acl_cache: Mutex::new(HashMap::new()),
            acl_epoch: AtomicU64::new(0),
            acl_namespace_epoch: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone, Copy)]
/// Cached effective-access result for a single inode with its invalidation epochs.
pub(super) struct EffectiveAclCacheEntry {
    access_level: AccessLevel,
    acl_epoch: u64,
    acl_namespace_epoch: u64,
}

impl StudioVfs {
    /// Increments the ACL epoch to invalidate cached effective access levels.
    pub(super) fn bump_acl_epoch(&self) {
        self.state
            .acl
            .acl_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(1))
            })
            .ok();
    }

    /// Increments the namespace ACL epoch to invalidate cached ACL derivations.
    pub(super) fn bump_acl_namespace_epoch(&self) {
        self.state
            .acl
            .acl_namespace_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(1))
            })
            .ok();
    }

    /// Returns the ACL entry for an inode for the active principal, falling back to wildcard.
    pub(super) fn acl_rule_for_inode(&self, ino: Ino) -> VfsResult<Option<AccessLevel>> {
        let meta = self.sync_meta();
        if let Some(level) = meta.get_acl_rule(ino, &self.acl_principal)? {
            return Ok(Some(level));
        }
        if self.acl_principal != "*" {
            return Ok(meta.get_acl_rule(ino, "*")?);
        }
        Ok(None)
    }

    fn acl_rule_for_inode_meta<M: ?Sized + AclMeta>(
        meta: &M,
        principal: &str,
        ino: Ino,
    ) -> MetaResult<Option<AccessLevel>> {
        if let Some(level) = meta.get_acl_rule(ino, principal)? {
            return Ok(Some(level));
        }
        if principal != "*" {
            return meta.get_acl_rule(ino, "*");
        }
        Ok(None)
    }

    fn access_satisfies(actual: AccessLevel, required: AccessLevel) -> bool {
        (actual as u8) >= (required as u8)
    }

    /// Reads a cached access level for the inode if it is still valid for current epochs.
    pub(super) fn effective_access_level_cached(&self, ino: Ino) -> Option<AccessLevel> {
        let acl_epoch = self.state.acl.acl_epoch.load(Ordering::Acquire);
        let acl_namespace_epoch = self.state.acl.acl_namespace_epoch.load(Ordering::Acquire);
        self.state
            .acl
            .effective_acl_cache
            .lock()
            .get(&ino)
            .copied()
            .filter(|entry| {
                entry.acl_epoch == acl_epoch && entry.acl_namespace_epoch == acl_namespace_epoch
            })
            .map(|entry| entry.access_level)
    }

    /// Stores an effective access level for an inode in the bounded cache.
    pub(super) fn effective_access_level_cache_insert(&self, ino: Ino, access_level: AccessLevel) {
        let acl_epoch = self.state.acl.acl_epoch.load(Ordering::Acquire);
        let acl_namespace_epoch = self.state.acl.acl_namespace_epoch.load(Ordering::Acquire);
        let mut cache = self.state.acl.effective_acl_cache.lock();
        if cache.len() >= self.effective_acl_cache_capacity {
            let evict_count = (self.effective_acl_cache_capacity / 4).max(1);
            let keys_to_remove: Vec<Ino> = cache.keys().copied().take(evict_count).collect();
            for key in keys_to_remove {
                cache.remove(&key);
            }
        }
        cache.insert(
            ino,
            EffectiveAclCacheEntry {
                access_level,
                acl_epoch,
                acl_namespace_epoch,
            },
        );
    }

    /// Computes effective access for an inode by walking up namespace ACL rules and caching the result.
    pub(super) fn effective_access_level(&self, ino: Ino) -> VfsResult<AccessLevel> {
        if let Some(access_level) = self.effective_access_level_cached(ino) {
            return Ok(access_level);
        }

        let mut current = ino;
        let mut depth = 0usize;
        let access_level = loop {
            if depth >= 4096 {
                break AccessLevel::NoAccess;
            }
            if let Some(level) = self.acl_rule_for_inode(current)? {
                break level;
            }

            let attr = self.getattr_unchecked(current)?;
            if current == ROOT_INO || attr.parent == 0 || attr.parent == current {
                break AccessLevel::ReadWrite;
            }
            current = attr.parent;
            depth += 1;
        };

        self.effective_access_level_cache_insert(ino, access_level);
        Ok(access_level)
    }

    fn effective_access_level_meta<M: ?Sized + AclMeta>(
        meta: &M,
        principal: &str,
        ino: Ino,
    ) -> MetaResult<AccessLevel> {
        let mut current = ino;
        let mut depth = 0usize;
        loop {
            if depth >= 4096 {
                return Ok(AccessLevel::NoAccess);
            }
            if let Some(level) = Self::acl_rule_for_inode_meta(meta, principal, current)? {
                return Ok(level);
            }
            let attr = meta.get_attr(current)?;
            if current == ROOT_INO || attr.parent == 0 || attr.parent == current {
                return Ok(AccessLevel::ReadWrite);
            }
            current = attr.parent;
            depth += 1;
        }
    }

    /// Enforces minimum required access level, or returns `AccessDenied` / not-found errors.
    pub(super) fn enforce_access(&self, ino: Ino, required: AccessLevel) -> VfsResult<()> {
        let actual = self.effective_access_level(ino)?;
        if Self::access_satisfies(actual, required) {
            if required == AccessLevel::ReadWrite {
                self.enforce_metadata_mutation_authority(ino)?;
            }
            return Ok(());
        }
        if actual == AccessLevel::NoAccess {
            return Err(VfsError::Meta(MetaError::InodeNotFound(ino)));
        }
        Err(VfsError::Meta(MetaError::AccessDenied {
            ino,
            principal: self.acl_principal.clone(),
            required,
            actual,
        }))
    }

    /// Same as [`Self::enforce_access`], but fetches metadata using async execution path when needed.
    pub(super) async fn enforce_access_async(
        &self,
        ino: Ino,
        required: AccessLevel,
    ) -> VfsResult<()> {
        if let Some(actual) = self.effective_access_level_cached(ino) {
            if Self::access_satisfies(actual, required) {
                if required == AccessLevel::ReadWrite {
                    self.enforce_metadata_mutation_authority(ino)?;
                }
                return Ok(());
            }
            if actual == AccessLevel::NoAccess {
                return Err(VfsError::Meta(MetaError::InodeNotFound(ino)));
            }
            return Err(VfsError::Meta(MetaError::AccessDenied {
                ino,
                principal: self.acl_principal.clone(),
                required,
                actual,
            }));
        }

        let meta = self.meta.clone();
        let principal = self.acl_principal.clone();
        let actual = run_meta(meta, move |engine| {
            Self::effective_access_level_meta(engine.as_ref(), &principal, ino)
        })
        .await?;
        self.effective_access_level_cache_insert(ino, actual);
        if Self::access_satisfies(actual, required) {
            if required == AccessLevel::ReadWrite {
                self.enforce_metadata_mutation_authority(ino)?;
            }
            return Ok(());
        }
        if actual == AccessLevel::NoAccess {
            return Err(VfsError::Meta(MetaError::InodeNotFound(ino)));
        }
        Err(VfsError::Meta(MetaError::AccessDenied {
            ino,
            principal: self.acl_principal.clone(),
            required,
            actual,
        }))
    }

    /// Sets (or overwrites) an ACL rule for an inode and principal.
    pub fn set_acl_rule(
        &self,
        ino: Ino,
        principal: &str,
        access_level: AccessLevel,
    ) -> VfsResult<()> {
        let meta = self.sync_meta();
        meta.set_acl_rule(ino, principal, access_level)?;
        self.bump_acl_epoch();
        meta.request_eager_flush();
        Ok(())
    }

    /// Removes an ACL rule for an inode and principal.
    pub fn remove_acl_rule(&self, ino: Ino, principal: &str) -> VfsResult<()> {
        let meta = self.sync_meta();
        meta.remove_acl_rule(ino, principal)?;
        self.bump_acl_epoch();
        meta.request_eager_flush();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::{DataStoreConfig, VfsConfig};
    use super::*;
    use std::sync::Arc;
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig};
    use studiofs_meta::{InodeAttr, MetaFormat, MetaInodes, RedbMetaEngine};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("acl-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("acl-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            block_size: 4 * 1024 * 1024,
        }
    }

    fn make_test_vfs(
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
            cfg.try_into().expect("VFS config should validate"),
        )
        .expect("create vfs");
        vfs.start().expect("start vfs background workers");
        (rt, vfs, meta)
    }

    #[test]
    fn effective_access_level_depth_limit_fails_closed() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, meta) =
            make_test_vfs(&tmp, "depth-limit-fails-closed", VfsConfig::default());

        let mut parent = ROOT_INO;
        let mut deepest = ROOT_INO;
        for _ in 0..4097usize {
            let ino = meta.next_inode().expect("allocate inode");
            let attr = InodeAttr::new_dir(ino, parent, 1000, 1000, 0o755);
            meta.set_attr(&attr).expect("set attr for deep chain");
            parent = ino;
            deepest = ino;
        }

        assert_eq!(
            vfs.effective_access_level(deepest)
                .expect("compute effective access for deep inode"),
            AccessLevel::NoAccess
        );
    }

    #[test]
    fn effective_acl_cache_overflow_evicts_partially() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = VfsConfig {
            effective_acl_cache_capacity: 4,
            ..VfsConfig::default()
        };
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "partial-cache-eviction", cfg);

        for ino in 10..=14 {
            vfs.effective_access_level_cache_insert(ino, AccessLevel::ReadOnly);
        }

        let cache = vfs.state.acl.effective_acl_cache.lock();
        assert!(
            cache.len() > 1 && cache.len() <= 4,
            "cache overflow should evict partially instead of clearing the whole cache"
        );
        assert!(
            cache.contains_key(&14),
            "newest entry should still be present after partial eviction"
        );
    }
}
