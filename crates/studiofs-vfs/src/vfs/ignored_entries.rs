use super::{
    FileType, Ino, InodeAttr, MetaError, Ordering, ROOT_INO, StudioVfs, VfsError,
    is_blocked_root_macos_metadata, is_ignored_macos_metadata, is_probable_trash_component,
};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub(super) struct IgnoredEntry {
    pub(super) attr: InodeAttr,
    pub(super) parent: Ino,
    pub(super) name: Vec<u8>,
}

#[derive(Debug, Default)]
pub(super) struct IgnoredEntryRegistry {
    pub(super) by_ino: HashMap<Ino, IgnoredEntry>,
    pub(super) by_name: HashMap<(Ino, Vec<u8>), Ino>,
    pub(super) children_by_parent: HashMap<Ino, HashSet<Ino>>,
}

fn remove_parent_child_link(registry: &mut IgnoredEntryRegistry, parent: Ino, child: Ino) {
    if let Some(children) = registry.children_by_parent.get_mut(&parent) {
        children.remove(&child);
        if children.is_empty() {
            registry.children_by_parent.remove(&parent);
        }
    }
}

impl StudioVfs {
    pub(super) fn ignored_entry_attr(&self, ino: Ino) -> Option<InodeAttr> {
        self.state
            .metrics
            .ignored_entries
            .lock()
            .by_ino
            .get(&ino)
            .map(|ignored| ignored.attr.clone())
    }

    pub(super) fn ignored_dir_attr(&self, ino: Ino) -> Option<InodeAttr> {
        self.ignored_entry_attr(ino)
            .filter(|attr| attr.kind == FileType::Directory)
    }

    pub(super) fn ignored_entry_lookup(&self, parent: Ino, name: &[u8]) -> Option<IgnoredEntry> {
        let key = (parent, name.to_vec());
        let registry = self.state.metrics.ignored_entries.lock();
        let ino = registry.by_name.get(&key).copied()?;
        registry.by_ino.get(&ino).cloned()
    }

    pub(super) fn ignored_entry(&self, ino: Ino) -> Option<IgnoredEntry> {
        self.state
            .metrics
            .ignored_entries
            .lock()
            .by_ino
            .get(&ino)
            .cloned()
    }

    pub(super) fn create_ignored_entry(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
        kind: FileType,
    ) -> InodeAttr {
        let mut registry = self.state.metrics.ignored_entries.lock();
        let key = (parent, name.to_vec());
        if let Some(ino) = registry.by_name.get(&key).copied()
            && let Some(existing) = registry.by_ino.get(&ino)
        {
            return existing.attr.clone();
        }

        let ino = self
            .state
            .metrics
            .next_ignored_ino
            .fetch_add(1, Ordering::Relaxed);
        let attr = match kind {
            FileType::Directory => InodeAttr::new_dir(ino, parent, uid, gid, mode & 0o7777),
            FileType::RegularFile => InodeAttr::new_file(ino, parent, uid, gid, mode & 0o7777),
            FileType::Symlink => InodeAttr::new_symlink(ino, parent, uid, gid, 0),
        };
        let ignored = IgnoredEntry {
            attr: attr.clone(),
            parent,
            name: key.1.clone(),
        };
        registry.by_name.insert(key, ino);
        registry
            .children_by_parent
            .entry(parent)
            .or_default()
            .insert(ino);
        registry.by_ino.insert(ino, ignored);
        attr
    }

    pub(super) fn update_ignored_entry<F>(&self, ino: Ino, update: F) -> Option<InodeAttr>
    where
        F: FnOnce(&mut InodeAttr),
    {
        let mut registry = self.state.metrics.ignored_entries.lock();
        let ignored = registry.by_ino.get_mut(&ino)?;
        update(&mut ignored.attr);
        Some(ignored.attr.clone())
    }

    pub(super) fn unlink_ignored_entry(&self, parent: Ino, name: &[u8]) -> bool {
        let key = (parent, name.to_vec());
        let mut registry = self.state.metrics.ignored_entries.lock();
        let Some(ino) = registry.by_name.remove(&key) else {
            return false;
        };
        remove_parent_child_link(&mut registry, parent, ino);

        if self.handle_table.open_count(ino) > 0 {
            self.handle_table.mark_orphan(ino);
        } else {
            registry.by_ino.remove(&ino);
        }
        true
    }

    pub(super) fn unlink_ignored_subtree(&self, parent: Ino, name: &[u8]) -> bool {
        let key = (parent, name.to_vec());
        let mut registry = self.state.metrics.ignored_entries.lock();
        let Some(root_ino) = registry.by_name.remove(&key) else {
            return false;
        };

        let mut pending = vec![root_ino];
        let mut to_remove = Vec::new();
        while let Some(current) = pending.pop() {
            to_remove.push(current);
            if let Some(children) = registry.children_by_parent.get(&current) {
                pending.extend(children.iter().copied());
            }
        }

        for ino in to_remove {
            if let Some(ignored) = registry.by_ino.get(&ino).cloned() {
                registry
                    .by_name
                    .remove(&(ignored.parent, ignored.name.clone()));
                remove_parent_child_link(&mut registry, ignored.parent, ino);
                registry.children_by_parent.remove(&ino);
                if self.handle_table.open_count(ino) > 0 {
                    self.handle_table.mark_orphan(ino);
                } else {
                    registry.by_ino.remove(&ino);
                }
            }
        }
        true
    }

    pub(super) fn release_ignored_entry(&self, ino: Ino) -> bool {
        let mut registry = self.state.metrics.ignored_entries.lock();
        let Some(ignored) = registry.by_ino.remove(&ino) else {
            return false;
        };
        registry.by_name.remove(&(ignored.parent, ignored.name));
        remove_parent_child_link(&mut registry, ignored.parent, ino);
        registry.children_by_parent.remove(&ino);
        true
    }

    pub(super) fn ignored_entry_not_found(parent: Ino, name: &[u8]) -> VfsError {
        VfsError::Meta(MetaError::EntryNotFound {
            parent,
            name: String::from_utf8_lossy(name).into_owned(),
        })
    }

    pub(super) fn is_blocked_root_metadata_path(&self, parent: Ino, name: &[u8]) -> bool {
        parent == ROOT_INO && is_blocked_root_macos_metadata(name)
    }

    pub(super) fn should_ignore_regular_file_path(&self, parent: Ino, name: &[u8]) -> bool {
        is_ignored_macos_metadata(name)
            || self.is_blocked_root_metadata_path(parent, name)
            || self.ignored_dir_attr(parent).is_some()
    }

    pub(super) fn should_ignore_directory_path(&self, parent: Ino, name: &[u8]) -> bool {
        self.is_blocked_root_metadata_path(parent, name) || self.ignored_dir_attr(parent).is_some()
    }

    pub(super) fn ignored_directory_has_probable_trash_ancestor(&self, dir_ino: Ino) -> bool {
        let mut current = dir_ino;
        let mut hops = 0usize;
        while current != 0 && hops < 1024 {
            let Some(entry) = self.ignored_entry(current) else {
                break;
            };
            if entry.parent == ROOT_INO && is_probable_trash_component(&entry.name) {
                return true;
            }
            current = entry.parent;
            hops += 1;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::super::{DataStoreConfig, VfsConfig};
    use super::*;
    use std::sync::Arc;
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig};
    use studiofs_meta::{MetaFormat, RedbMetaEngine};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("ignored-entries-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("ignored-entries-uuid-{volume_suffix}"),
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
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");
        vfs.start().expect("start vfs background workers");
        (rt, vfs, meta)
    }

    #[test]
    fn ignored_create_invalidates_parent_attr_cache() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "ignored-create-invalidates-parent-attr-cache");
        let dir = vfs
            .mkdir(ROOT_INO, b"dir", 1000, 1000, 0o755)
            .expect("mkdir dir");

        let _ = vfs.getattr(dir.ino).expect("prime parent attr cache");
        assert!(vfs.state.namespace.attr_read_cache.lock().contains(dir.ino));

        let _ignored = vfs
            .create(dir.ino, b".fseventsd", 1000, 1000, 0o644)
            .expect("create ignored file");

        assert!(!vfs.state.namespace.attr_read_cache.lock().contains(dir.ino));
    }

    #[test]
    fn ignored_link_invalidates_new_parent_attr_cache() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "ignored-link-invalidates-parent-attr-cache");
        let source = vfs
            .create(ROOT_INO, b"source.txt", 1000, 1000, 0o644)
            .expect("create source");
        let dir = vfs
            .mkdir(ROOT_INO, b"links", 1000, 1000, 0o755)
            .expect("mkdir links");

        let _ = vfs.getattr(dir.ino).expect("prime new parent attr cache");
        assert!(vfs.state.namespace.attr_read_cache.lock().contains(dir.ino));

        let _ignored = vfs
            .link(source.ino, dir.ino, b".fseventsd")
            .expect("create ignored hardlink");

        assert!(!vfs.state.namespace.attr_read_cache.lock().contains(dir.ino));
    }

    #[test]
    fn rename_between_ignored_names_invalidates_parent_attr_cache() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "ignored-rename-invalidates-parent-attr-cache");
        vfs.mkdir(ROOT_INO, b".Spotlight-V100", 1000, 1000, 0o755)
            .expect("create ignored dir");

        let _ = vfs.getattr(ROOT_INO).expect("prime root attr cache");
        assert!(
            vfs.state
                .namespace
                .attr_read_cache
                .lock()
                .contains(ROOT_INO)
        );

        vfs.rename(ROOT_INO, b".Spotlight-V100", ROOT_INO, b".fseventsd")
            .expect("rename ignored entry");

        assert!(
            !vfs.state
                .namespace
                .attr_read_cache
                .lock()
                .contains(ROOT_INO)
        );
        assert!(vfs.lookup(ROOT_INO, b".Spotlight-V100").is_err());
        assert!(vfs.lookup(ROOT_INO, b".fseventsd").is_ok());
    }

    #[test]
    fn rename_into_ignored_trash_invalidates_destination_parent_attr_cache() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) =
            make_test_vfs(&tmp, "ignored-trash-rename-invalidates-parent-attr-cache");
        vfs.create(ROOT_INO, b"source.txt", 1000, 1000, 0o644)
            .expect("create source");
        let trash_root = vfs
            .mkdir(ROOT_INO, b".Trash", 1000, 1000, 0o755)
            .expect("create ignored trash root");
        let trash_child = vfs
            .mkdir(trash_root.ino, b"501", 1000, 1000, 0o755)
            .expect("create ignored trash child");

        let _ = vfs
            .getattr(trash_child.ino)
            .expect("prime destination parent attr cache");
        assert!(
            vfs.state
                .namespace
                .attr_read_cache
                .lock()
                .contains(trash_child.ino)
        );

        vfs.rename(ROOT_INO, b"source.txt", trash_child.ino, b"source.txt")
            .expect("rename into ignored trash subtree");

        assert!(
            !vfs.state
                .namespace
                .attr_read_cache
                .lock()
                .contains(trash_child.ino)
        );
        assert!(vfs.lookup(ROOT_INO, b"source.txt").is_err());
        assert!(
            vfs.lookup(trash_child.ino, b"source.txt").is_err(),
            "ignored trash target should not preserve a recoverable entry"
        );
    }

    #[test]
    fn rename_ignored_source_into_ignored_trash_does_not_create_target() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "ignored-source-trash-rename-deletes");
        vfs.mkdir(ROOT_INO, b".Spotlight-V100", 1000, 1000, 0o755)
            .expect("create ignored source");
        let trash_root = vfs
            .mkdir(ROOT_INO, b".Trash", 1000, 1000, 0o755)
            .expect("create ignored trash root");
        let trash_child = vfs
            .mkdir(trash_root.ino, b"501", 1000, 1000, 0o755)
            .expect("create ignored trash child");

        vfs.rename(
            ROOT_INO,
            b".Spotlight-V100",
            trash_child.ino,
            b".Spotlight-V100",
        )
        .expect("rename ignored source into ignored trash subtree");

        assert!(vfs.lookup(ROOT_INO, b".Spotlight-V100").is_err());
        assert!(
            vfs.lookup(trash_child.ino, b".Spotlight-V100").is_err(),
            "ignored-source trash rename must not create a recoverable target"
        );
    }

    #[test]
    fn unlink_ignored_subtree_removes_nested_children() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "ignored-subtree-unlink");

        let trash =
            vfs.create_ignored_entry(ROOT_INO, b".Trash", 1000, 1000, 0o755, FileType::Directory);
        let user =
            vfs.create_ignored_entry(trash.ino, b"501", 1000, 1000, 0o755, FileType::Directory);
        let nested =
            vfs.create_ignored_entry(user.ino, b"nested", 1000, 1000, 0o755, FileType::Directory);
        let file = vfs.create_ignored_entry(
            nested.ino,
            b"file.txt",
            1000,
            1000,
            0o644,
            FileType::RegularFile,
        );

        assert!(vfs.unlink_ignored_subtree(ROOT_INO, b".Trash"));
        assert!(vfs.ignored_entry(trash.ino).is_none());
        assert!(vfs.ignored_entry(user.ino).is_none());
        assert!(vfs.ignored_entry(nested.ino).is_none());
        assert!(vfs.ignored_entry(file.ino).is_none());
    }
}
