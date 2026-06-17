//! Transport-neutral ownership for native FUSE replies.

use std::sync::{Arc, Mutex};

use thiserror::Error;

use crate::native::abi::{error_reply, ok_reply};
use crate::native::protocol::FuseReply;

/// Transport context that owns the final kernel reply commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyOrigin {
    /// Classic `/dev/fuse` writeback path.
    Classic,
    /// FUSE-over-io_uring style commit/fetch path.
    IoUring {
        /// Transport queue that supplied the request.
        queue_id: u16,
        /// Request generation within that queue.
        generation: u64,
    },
}

/// Error returned when a reply token is completed after ownership ended.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum ReplyCompletionError {
    /// The token already sent a reply or was explicitly dropped.
    #[error("FUSE reply token already completed")]
    AlreadyCompleted,
}

/// Reply sink used by concrete transports to commit completed replies.
pub trait ReplySink: Clone + Send + Sync + 'static {
    /// Commits a fully framed reply for the supplied transport origin.
    fn complete(&self, origin: ReplyOrigin, reply: FuseReply);
    /// Records a token dropped without a kernel reply.
    fn dropped(&self, origin: ReplyOrigin, unique: u64);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplyState {
    Pending,
    Completed,
}

/// Owned reply capability for one FUSE request.
///
/// The token can be moved to async work and completed from any transport
/// backend. Completion is fail-closed: exactly one reply or drop event wins.
#[derive(Debug, Clone)]
pub struct ReplyToken<S> {
    unique: u64,
    origin: ReplyOrigin,
    sink: S,
    state: Arc<Mutex<ReplyState>>,
}

impl<S: ReplySink> ReplyToken<S> {
    /// Builds a token for a classic `/dev/fuse` request.
    pub fn new_classic(unique: u64, sink: S) -> Self {
        Self::new(unique, ReplyOrigin::Classic, sink)
    }

    /// Builds a token for an io_uring queue request.
    pub fn new_io_uring(unique: u64, sink: S, queue_id: u16, generation: u64) -> Self {
        Self::new(
            unique,
            ReplyOrigin::IoUring {
                queue_id,
                generation,
            },
            sink,
        )
    }

    fn new(unique: u64, origin: ReplyOrigin, sink: S) -> Self {
        Self {
            unique,
            origin,
            sink,
            state: Arc::new(Mutex::new(ReplyState::Pending)),
        }
    }

    /// Sends a successful reply with the supplied payload.
    pub fn send_success(&self, payload: &[u8]) -> Result<(), ReplyCompletionError> {
        self.complete(FuseReply {
            unique: self.unique,
            bytes: ok_reply(self.unique, payload),
        })
    }

    /// Sends an errno reply.
    pub fn send_errno(&self, errno: i32) -> Result<(), ReplyCompletionError> {
        self.complete(FuseReply {
            unique: self.unique,
            bytes: error_reply(self.unique, errno).to_vec(),
        })
    }

    /// Sends an owned data reply without forcing callers to retain the buffer.
    pub fn send_data(&self, data: Vec<u8>) -> Result<(), ReplyCompletionError> {
        self.complete(FuseReply {
            unique: self.unique,
            bytes: ok_reply(self.unique, &data),
        })
    }

    /// Marks the token as intentionally dropped without replying to the kernel.
    pub fn drop_without_reply(&self) {
        if self.take_completion_slot().is_ok() {
            self.sink.dropped(self.origin, self.unique);
        }
    }

    fn complete(&self, reply: FuseReply) -> Result<(), ReplyCompletionError> {
        self.take_completion_slot()?;
        self.sink.complete(self.origin, reply);
        Ok(())
    }

    fn take_completion_slot(&self) -> Result<(), ReplyCompletionError> {
        let mut state = self.state.lock().expect("reply token state poisoned");
        if *state == ReplyState::Completed {
            return Err(ReplyCompletionError::AlreadyCompleted);
        }
        *state = ReplyState::Completed;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct RecordedReplySinkState {
    completed: Vec<(ReplyOrigin, FuseReply)>,
    dropped: Vec<(ReplyOrigin, u64)>,
}

/// In-memory reply sink used by dispatcher and protocol tests.
#[derive(Debug, Clone, Default)]
pub struct RecordedReplySink {
    state: Arc<Mutex<RecordedReplySinkState>>,
}

impl RecordedReplySink {
    /// Returns completed replies in commit order.
    pub fn completed_replies(&self) -> Vec<FuseReply> {
        self.state
            .lock()
            .expect("recorded reply sink state poisoned")
            .completed
            .iter()
            .map(|(_, reply)| reply.clone())
            .collect()
    }

    /// Returns request ids completed by this sink.
    pub fn completed_unique_ids(&self) -> Vec<u64> {
        self.state
            .lock()
            .expect("recorded reply sink state poisoned")
            .completed
            .iter()
            .map(|(_, reply)| reply.unique)
            .collect()
    }

    /// Returns request ids dropped by this sink.
    pub fn dropped_unique_ids(&self) -> Vec<u64> {
        self.state
            .lock()
            .expect("recorded reply sink state poisoned")
            .dropped
            .iter()
            .map(|(_, unique)| *unique)
            .collect()
    }
}

impl ReplySink for RecordedReplySink {
    fn complete(&self, origin: ReplyOrigin, reply: FuseReply) {
        self.state
            .lock()
            .expect("recorded reply sink state poisoned")
            .completed
            .push((origin, reply));
    }

    fn dropped(&self, origin: ReplyOrigin, unique: u64) {
        self.state
            .lock()
            .expect("recorded reply sink state poisoned")
            .dropped
            .push((origin, unique));
    }
}
