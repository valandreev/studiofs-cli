use studiofs_meta::Ino;
use studiofs_vfs::{LocalSyncPolicy, OpenFlags};

use super::CreateCallbackKind;

pub(super) struct CreateCallbackPlan {
    pub(super) parent_ino: Ino,
    pub(super) name: Vec<u8>,
    pub(super) create_options: u32,
    pub(super) open_flags: OpenFlags,
    pub(super) write_local_sync_policy: LocalSyncPolicy,
    pub(super) kind: CreateCallbackKind,
}

pub(super) struct RenameCallbackPlan {
    pub(super) old_parent: Ino,
    pub(super) old_name: Vec<u8>,
    pub(super) new_parent: Ino,
    pub(super) new_name: Vec<u8>,
}
