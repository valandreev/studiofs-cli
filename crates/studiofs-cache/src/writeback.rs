//! Writeback cache — tracks dirty blocks and asynchronously flushes them to S3.

use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use studiofs_meta::Ino;
use tokio::sync::{mpsc, Notify};
use tracing::{debug, error};

/// Identifies a dirty block pending upload.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DirtyBlockKey {
    pub ino: Ino,
    /// Slice ID this block belongs to.
    pub slice_id: u64,
    /// Block index within the slice.
    pub block_index: u32,
}

/// A dirty block waiting to be flushed.
#[derive(Debug, Clone)]
pub struct DirtyBlock {
    pub key: DirtyBlockKey,
    pub data: Bytes,
}

/// Writeback cache configuration.
#[derive(Debug, Clone)]
pub struct WritebackConfig {
    /// Max number of dirty blocks before forcing a flush.
    pub max_dirty_blocks: usize,
    /// Flush interval in milliseconds.
    pub flush_interval_ms: u64,
    /// Number of concurrent upload workers.
    pub upload_concurrency: usize,
}

impl Default for WritebackConfig {
    fn default() -> Self {
        Self {
            max_dirty_blocks: 1024,
            flush_interval_ms: 1000,
            upload_concurrency: 8,
        }
    }
}

/// The writeback manager tracks dirty blocks and provides a channel for flushing.
pub struct WritebackManager {
    /// Currently dirty blocks.
    dirty: DashMap<DirtyBlockKey, Bytes>,
    /// Channel to send flush commands.
    flush_tx: mpsc::UnboundedSender<DirtyBlock>,
    /// Notification for flush completion.
    flush_notify: Arc<Notify>,
    /// Config.
    config: WritebackConfig,
}

impl WritebackManager {
    /// Create a new writeback manager. Returns the manager and a receiver for the flush channel.
    pub fn new(config: WritebackConfig) -> (Self, mpsc::UnboundedReceiver<DirtyBlock>) {
        let (flush_tx, flush_rx) = mpsc::unbounded_channel();
        let manager = Self {
            dirty: DashMap::new(),
            flush_tx,
            flush_notify: Arc::new(Notify::new()),
            config,
        };
        (manager, flush_rx)
    }

    /// Mark a block as dirty (needs to be uploaded to S3).
    pub fn mark_dirty(&self, key: DirtyBlockKey, data: Bytes) {
        self.dirty.insert(key, data);

        // If we have too many dirty blocks, trigger flush
        if self.dirty.len() >= self.config.max_dirty_blocks {
            debug!("dirty block limit reached, triggering flush");
            self.trigger_flush();
        }
    }

    /// Trigger flushing of all dirty blocks.
    pub fn trigger_flush(&self) {
        let mut entries: Vec<(DirtyBlockKey, Bytes)> = Vec::new();

        // Drain dirty map
        self.dirty.retain(|_, _| false);

        // We need to iterate and remove
        for entry in self.dirty.iter() {
            entries.push((entry.key().clone(), entry.value().clone()));
        }

        for (key, data) in entries {
            self.dirty.remove(&key);
            let block = DirtyBlock {
                key,
                data,
            };
            if let Err(e) = self.flush_tx.send(block) {
                error!("failed to send dirty block to flush channel: {e}");
            }
        }
    }

    /// Flush all dirty blocks and wait for completion.
    pub async fn flush_all(&self) {
        self.trigger_flush();
        // Wait for flusher to process
        self.flush_notify.notified().await;
    }

    /// Check if there are dirty blocks for a given inode.
    pub fn has_dirty(&self, ino: Ino) -> bool {
        self.dirty.iter().any(|e| e.key().ino == ino)
    }

    /// Get the number of dirty blocks.
    pub fn dirty_count(&self) -> usize {
        self.dirty.len()
    }

    /// Get a reference to the flush notification.
    pub fn flush_notify(&self) -> Arc<Notify> {
        self.flush_notify.clone()
    }
}
