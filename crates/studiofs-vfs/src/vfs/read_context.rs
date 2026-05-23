use std::time::{Duration, Instant};

use super::{PrefetchReadContext, ReadState, StudioVfs};
use crate::access::{AccessClass, AccessPolicy};
use crate::utils::now_unix_millis;
use studiofs_meta::types::{Ino, InodeAttr};

#[derive(Debug, Clone)]
pub(super) struct ReadRequestContext {
    pub(super) attr: InodeAttr,
    pub(super) pending_content_epoch: Option<u64>,
}

impl ReadRequestContext {
    pub(super) fn from_attr(vfs: &StudioVfs, ino: Ino, attr: InodeAttr) -> Self {
        let incarnation_state = vfs.refresh_inode_incarnation_state_from_attr(ino, &attr);
        Self {
            pending_content_epoch: incarnation_state
                .pending_incarnation
                .as_ref()
                .map(|pending| pending.content_epoch),
            attr,
        }
    }

    pub(super) fn placeholder_visible(&self, vfs: &StudioVfs, ino: Ino) -> bool {
        vfs.remote_upload_placeholder_visible(ino, &self.attr)
    }

    pub(super) fn read_len(&self, offset: u64, requested_len: usize) -> usize {
        self.attr
            .size
            .min(offset.saturating_add(requested_len as u64))
            .saturating_sub(offset) as usize
    }

    pub(super) fn prefetch_context(&self) -> PrefetchReadContext {
        PrefetchReadContext {
            size: self.attr.size,
            pending_content_epoch: self.pending_content_epoch,
        }
    }
}

pub(super) struct ForegroundReadScope<'a> {
    read_state: &'a ReadState,
    latency_window: Duration,
    started: Instant,
    shard_idx: usize,
    bytes_read: u64,
    access_class: AccessClass,
    policy: AccessPolicy,
}

impl<'a> ForegroundReadScope<'a> {
    pub(super) fn new(
        read_state: &'a ReadState,
        latency_window: Duration,
        policy: AccessPolicy,
    ) -> Self {
        let now_ms = now_unix_millis();
        read_state.mark_foreground_read(now_ms);
        let shard_idx = read_state.select_foreground_latency_shard();
        Self {
            read_state,
            latency_window,
            started: Instant::now(),
            shard_idx,
            bytes_read: 0,
            access_class: policy.class,
            policy,
        }
    }

    pub(super) fn record_bytes(&mut self, bytes_read: u64) {
        self.bytes_read = bytes_read;
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ForegroundReadTelemetry {
    pub(super) bytes_read: u64,
    pub(super) elapsed: Duration,
    pub(super) access_class: AccessClass,
    pub(super) policy: AccessPolicy,
}

impl Drop for ForegroundReadScope<'_> {
    fn drop(&mut self) {
        let now_ms = now_unix_millis();
        self.read_state.record_foreground_read_latency(
            self.shard_idx,
            ForegroundReadTelemetry {
                bytes_read: self.bytes_read,
                elapsed: self.started.elapsed(),
                access_class: self.access_class,
                policy: self.policy,
            },
            now_ms,
            self.latency_window,
        );
        self.read_state.unmark_foreground_read(now_ms);
    }
}
