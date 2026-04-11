use super::{
    AccessLevel, AtomicU64, Ino, MetaEngine, MetaError, MetaResult, Mutex, Ordering, ROOT_INO,
    StudioVfs, VfsError, VfsResult, run_meta,
};
use std::collections::HashMap;

pub(super) struct AclState {
    pub(super) effective_acl_cache: Mutex<HashMap<Ino, EffectiveAclCacheEntry>>,
    pub(super) acl_epoch: AtomicU64,
    pub(super) acl_namespace_epoch: AtomicU64,
}

impl AclState {
    pub(super) fn new() -> Self {
        Self {
            effective_acl_cache: Mutex::new(HashMap::new()),
            acl_epoch: AtomicU64::new(0),
            acl_namespace_epoch: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct EffectiveAclCacheEntry {
    access_level: AccessLevel,
    acl_epoch: u64,
    acl_namespace_epoch: u64,
}

impl StudioVfs {
    pub(super) fn bump_acl_epoch(&self) {
        self.state
            .acl
            .acl_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(1))
            })
            .ok();
    }

    pub(super) fn bump_acl_namespace_epoch(&self) {
        self.state
            .acl
            .acl_namespace_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(1))
            })
            .ok();
    }

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

    fn acl_rule_for_inode_meta(
        meta: &dyn MetaEngine,
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

    pub(super) fn effective_access_level_cache_insert(&self, ino: Ino, access_level: AccessLevel) {
        let acl_epoch = self.state.acl.acl_epoch.load(Ordering::Acquire);
        let acl_namespace_epoch = self.state.acl.acl_namespace_epoch.load(Ordering::Acquire);
        let mut cache = self.state.acl.effective_acl_cache.lock();
        if cache.len() >= self.effective_acl_cache_capacity {
            cache.clear();
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

    pub(super) fn effective_access_level(&self, ino: Ino) -> VfsResult<AccessLevel> {
        if let Some(access_level) = self.effective_access_level_cached(ino) {
            return Ok(access_level);
        }

        let mut current = ino;
        let mut depth = 0usize;
        let access_level = loop {
            if depth >= 4096 {
                break AccessLevel::ReadWrite;
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

    fn effective_access_level_meta(
        meta: &dyn MetaEngine,
        principal: &str,
        ino: Ino,
    ) -> MetaResult<AccessLevel> {
        let mut current = ino;
        let mut depth = 0usize;
        loop {
            if depth >= 4096 {
                return Ok(AccessLevel::ReadWrite);
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

    pub(super) fn enforce_access(&self, ino: Ino, required: AccessLevel) -> VfsResult<()> {
        let actual = self.effective_access_level(ino)?;
        if Self::access_satisfies(actual, required) {
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

    pub(super) async fn enforce_access_async(
        &self,
        ino: Ino,
        required: AccessLevel,
    ) -> VfsResult<()> {
        if let Some(actual) = self.effective_access_level_cached(ino) {
            if Self::access_satisfies(actual, required) {
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

        let meta = self.meta_async.clone();
        let principal = self.acl_principal.clone();
        let actual = run_meta(meta, move |engine| {
            Self::effective_access_level_meta(engine.as_ref(), &principal, ino)
        })
        .await?;
        self.effective_access_level_cache_insert(ino, actual);
        if Self::access_satisfies(actual, required) {
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

    pub fn remove_acl_rule(&self, ino: Ino, principal: &str) -> VfsResult<()> {
        let meta = self.sync_meta();
        meta.remove_acl_rule(ino, principal)?;
        self.bump_acl_epoch();
        meta.request_eager_flush();
        Ok(())
    }
}
