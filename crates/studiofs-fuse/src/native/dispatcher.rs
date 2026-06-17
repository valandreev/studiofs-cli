//! Transport-independent native FUSE dispatcher.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Sender, TrySendError};
use smallvec::SmallVec;
use thiserror::Error;

use crate::native::abi::error_reply;
use crate::native::protocol::{FuseOpcode, FuseReply, FuseRequest};
use crate::native::reply::{ReplySink, ReplyToken};
use crate::native::transport::{FuseTransport, TransportError};

/// Operation layer invoked by the transport-independent dispatcher.
///
/// Implementations translate typed FUSE request envelopes into fully framed
/// replies. The trait deliberately has no dependency on classic read/write or
/// future io_uring transport details.
pub trait FuseOperationHandler {
    /// Handles one decoded request and returns bytes ready for the transport.
    fn handle(&mut self, request: FuseRequest) -> FuseReply;
}

/// Minimal handler used while wiring the native transport foundation.
///
/// It replies `ENOSYS` for every operation, which lets mount, negotiation, and
/// teardown paths be exercised before the StudioFS VFS adapter is connected.
#[derive(Debug, Default)]
pub struct MinimalNativeHandler;

impl FuseOperationHandler for MinimalNativeHandler {
    fn handle(&mut self, request: FuseRequest) -> FuseReply {
        FuseReply::contiguous(
            request.header.unique,
            error_reply(request.header.unique, libc::ENOSYS).to_vec(),
        )
    }
}

/// Summary returned after a dispatcher loop exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatchSummary {
    /// Number of requests handed to the operation handler.
    pub handled_requests: usize,
    /// Whether loop termination happened after receiving `FUSE_DESTROY`.
    pub saw_destroy: bool,
}

/// Dispatcher queue class selected from a typed FUSE opcode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestClass {
    /// Metadata and namespace operations.
    Metadata,
    /// File read operations.
    Read,
    /// File write operations.
    Write,
    /// Ordering-sensitive operations such as flush, fsync, and release.
    Barrier,
    /// Background maintenance and low-priority work.
    Background,
}

impl RequestClass {
    /// Number of dispatcher queue classes.
    pub const CLASS_COUNT: usize = 5;

    const ALL: [Self; Self::CLASS_COUNT] = [
        Self::Metadata,
        Self::Read,
        Self::Write,
        Self::Barrier,
        Self::Background,
    ];

    fn index(self) -> usize {
        match self {
            Self::Metadata => 0,
            Self::Read => 1,
            Self::Write => 2,
            Self::Barrier => 3,
            Self::Background => 4,
        }
    }

    fn for_opcode(opcode: FuseOpcode) -> Self {
        match opcode {
            FuseOpcode::Read => Self::Read,
            FuseOpcode::Write => Self::Write,
            FuseOpcode::Flush
            | FuseOpcode::Fsync
            | FuseOpcode::Release
            | FuseOpcode::Releasedir
            | FuseOpcode::Setattr => Self::Barrier,
            FuseOpcode::Forget | FuseOpcode::BatchForget | FuseOpcode::Other(_) => Self::Background,
            FuseOpcode::Lookup
            | FuseOpcode::Getattr
            | FuseOpcode::Mkdir
            | FuseOpcode::Create
            | FuseOpcode::Unlink
            | FuseOpcode::Rmdir
            | FuseOpcode::Rename
            | FuseOpcode::Open
            | FuseOpcode::Statfs
            | FuseOpcode::Init
            | FuseOpcode::Opendir
            | FuseOpcode::Readdir
            | FuseOpcode::Readdirplus
            | FuseOpcode::Destroy => Self::Metadata,
        }
    }
}

fn lock_or_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Owned request plus reply capability passed from transport intake to workers.
#[derive(Debug)]
pub struct OwnedFuseRequest<S: ReplySink> {
    /// Parsed FUSE request envelope.
    pub request: FuseRequest,
    /// Reply token that must be completed exactly once.
    pub reply: ReplyToken<S>,
}

impl<S: ReplySink> OwnedFuseRequest<S> {
    /// Builds an owned request from the classic `/dev/fuse` transport.
    pub fn classic(request: FuseRequest, sink: S) -> Self {
        Self {
            reply: ReplyToken::new_classic(request.header.unique, sink),
            request,
        }
    }

    fn class(&self) -> RequestClass {
        RequestClass::for_opcode(self.request.opcode)
    }
}

/// Operation handler used by the asynchronous native dispatcher.
pub trait AsyncFuseOperationHandler<S: ReplySink>: Clone + Send + Sync + 'static {
    /// Handles one owned request and completes or drops its reply token.
    fn handle(&self, envelope: OwnedFuseRequest<S>);
}

/// Resource identity used by ordering-sensitive dispatcher gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceKey {
    /// Gate work by inode.
    Inode(u64),
    /// Gate work by file handle.
    Handle(u64),
}

#[derive(Debug, Default)]
struct BarrierState {
    entries: HashMap<ResourceKey, BarrierEntry>,
}

const PENDING_WRITE_INLINE_CAPACITY: usize = 8;

#[derive(Debug)]
struct BarrierEntry {
    next_sequence: u64,
    pending_writes: SmallVec<[u64; PENDING_WRITE_INLINE_CAPACITY]>,
    waiters: usize,
    changed: Arc<Condvar>,
}

impl Default for BarrierEntry {
    fn default() -> Self {
        Self {
            next_sequence: 0,
            pending_writes: SmallVec::new(),
            waiters: 0,
            changed: Arc::new(Condvar::new()),
        }
    }
}

#[derive(Debug)]
struct BarrierShared {
    shards: [Mutex<BarrierState>; Self::SHARD_COUNT],
}

impl BarrierShared {
    const SHARD_COUNT: usize = 64;

    fn shard_index(resource: ResourceKey) -> usize {
        let mut value = match resource {
            ResourceKey::Inode(inode) => inode,
            ResourceKey::Handle(handle) => handle ^ 0xa5a5_a5a5_a5a5_a5a5,
        };
        value ^= value >> 30;
        value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value ^= value >> 27;
        value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^= value >> 31;

        (value as usize) & (Self::SHARD_COUNT - 1)
    }

    fn shard(&self, resource: ResourceKey) -> &Mutex<BarrierState> {
        &self.shards[Self::shard_index(resource)]
    }
}

const _: () = assert!(BarrierShared::SHARD_COUNT.is_power_of_two());

impl Default for BarrierShared {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(BarrierState::default())),
        }
    }
}

/// Per-resource gates for write and barrier operation ordering.
#[derive(Debug, Clone, Default)]
pub struct OperationBarrierGates {
    shared: Arc<BarrierShared>,
}

impl OperationBarrierGates {
    /// Records a pending write for `resource` until the returned guard drops.
    pub fn begin_write(&self, resource: ResourceKey) -> PendingWriteGuard {
        let mut state = lock_or_recover(self.shared.shard(resource));
        let entry = state.entries.entry(resource).or_default();
        entry.next_sequence += 1;
        let sequence = entry.next_sequence;
        entry.pending_writes.push(sequence);
        PendingWriteGuard {
            resource,
            sequence,
            shared: Arc::clone(&self.shared),
        }
    }

    /// Waits until writes for `resource` drain.
    pub fn wait_for_barrier(&self, resource: ResourceKey) {
        let shard = self.shared.shard(resource);
        let mut state = lock_or_recover(shard);
        let barrier_sequence = state
            .entries
            .get(&resource)
            .map(|entry| entry.next_sequence)
            .unwrap_or(0);
        while state.entries.get(&resource).is_some_and(|entry| {
            entry
                .pending_writes
                .iter()
                .min()
                .is_some_and(|sequence| *sequence <= barrier_sequence)
        }) {
            let changed = {
                let entry = state
                    .entries
                    .get_mut(&resource)
                    .expect("barrier entry exists while writes are pending");
                entry.waiters += 1;
                Arc::clone(&entry.changed)
            };
            state = changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let remove_entry = if let Some(entry) = state.entries.get_mut(&resource) {
                entry.waiters -= 1;
                entry.pending_writes.is_empty() && entry.waiters == 0
            } else {
                false
            };
            if remove_entry {
                state.entries.remove(&resource);
            }
        }
    }
}

/// Guard that marks one pending write complete on drop.
#[derive(Debug)]
pub struct PendingWriteGuard {
    resource: ResourceKey,
    sequence: u64,
    shared: Arc<BarrierShared>,
}

impl Drop for PendingWriteGuard {
    fn drop(&mut self) {
        let mut state = lock_or_recover(self.shared.shard(self.resource));
        let mut changed = None;
        let mut remove_entry = false;
        if let Some(entry) = state.entries.get_mut(&self.resource) {
            if let Some(index) = entry
                .pending_writes
                .iter()
                .position(|sequence| *sequence == self.sequence)
            {
                entry.pending_writes.swap_remove(index);
            }
            if entry.waiters > 0 {
                changed = Some(Arc::clone(&entry.changed));
            }
            remove_entry = entry.pending_writes.is_empty() && entry.waiters == 0;
        }
        if remove_entry {
            state.entries.remove(&self.resource);
        }
        drop(state);
        if let Some(changed) = changed {
            changed.notify_all();
        }
    }
}

/// Point-in-time dispatcher telemetry used by tests and future exporters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchTelemetrySnapshot {
    queue_depths: [usize; RequestClass::CLASS_COUNT],
    /// Number of submit calls rejected due to bounded queue pressure.
    pub backpressure_events: usize,
}

impl DispatchTelemetrySnapshot {
    /// Returns the queued or in-flight count for an operation class.
    pub fn queue_depth(&self, class: RequestClass) -> usize {
        self.queue_depths[class.index()]
    }
}

#[derive(Debug, Default)]
struct DispatchTelemetryState {
    queue_depths: [AtomicUsize; RequestClass::CLASS_COUNT],
    backpressure_events: AtomicUsize,
}

impl DispatchTelemetryState {
    fn snapshot(&self) -> DispatchTelemetrySnapshot {
        DispatchTelemetrySnapshot {
            queue_depths: self
                .queue_depths
                .each_ref()
                .map(|depth| depth.load(Ordering::Relaxed)),
            backpressure_events: self.backpressure_events.load(Ordering::Relaxed),
        }
    }
}

struct QueueDepthGuard<'a> {
    telemetry: &'a DispatchTelemetryState,
    class: RequestClass,
}

impl<'a> QueueDepthGuard<'a> {
    fn new(telemetry: &'a DispatchTelemetryState, class: RequestClass) -> Self {
        Self { telemetry, class }
    }
}

impl Drop for QueueDepthGuard<'_> {
    fn drop(&mut self) {
        self.telemetry.queue_depths[self.class.index()].fetch_sub(1, Ordering::Relaxed);
    }
}

/// Error returned by the native dispatcher.
#[derive(Debug, Error)]
pub enum DispatchError {
    /// The underlying transport failed while receiving a request or sending a reply.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// The underlying transport failed while receiving the next request.
    #[error("receive FUSE request failed")]
    Receive {
        /// Original transport error.
        #[source]
        source: TransportError,
    },
    /// The underlying transport failed while sending a reply.
    #[error(
        "send FUSE reply failed: opcode={opcode:?}; unique={unique}; reply_len={reply_len}; reply_error={reply_error:?}"
    )]
    SendReply {
        /// Request opcode whose reply failed.
        opcode: FuseOpcode,
        /// Request id whose reply failed.
        unique: u64,
        /// Serialized reply length.
        reply_len: usize,
        /// FUSE reply error code from the output header, when present.
        reply_error: Option<i32>,
        /// Original transport error.
        #[source]
        source: TransportError,
    },
    /// A bounded operation queue has no capacity for another request.
    #[error("native FUSE dispatcher queue full for {class:?}")]
    QueueFull {
        /// Operation class that rejected the request.
        class: RequestClass,
    },
    /// A worker thread panicked during shutdown.
    #[error("native FUSE dispatcher worker panicked")]
    WorkerPanicked,
    /// Worker threads did not exit before the shutdown deadline.
    #[error(
        "native FUSE dispatcher shutdown timed out with {remaining_workers} workers still running"
    )]
    ShutdownTimeout {
        /// Number of worker threads still running when the timeout elapsed.
        remaining_workers: usize,
    },
    /// The dispatcher is no longer accepting work.
    #[error("native FUSE dispatcher is shut down")]
    Shutdown,
}

impl PartialEq for DispatchError {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::QueueFull { class: left }, Self::QueueFull { class: right }) if left == right
        ) || matches!((self, other), (Self::WorkerPanicked, Self::WorkerPanicked))
            || matches!((self, other), (Self::Shutdown, Self::Shutdown))
            || matches!(
                (self, other),
                (
                    Self::ShutdownTimeout {
                        remaining_workers: left
                    },
                    Self::ShutdownTimeout {
                        remaining_workers: right
                    }
                ) if left == right
            )
    }
}

fn transport_error_raw_errno(error: &TransportError) -> Option<i32> {
    match error {
        TransportError::Io(error) => error.raw_os_error(),
        TransportError::Abi(_) => None,
    }
}

/// Transport-independent request loop for a native FUSE session.
///
/// The dispatcher owns a transport and an operation handler and repeatedly
/// receives decoded requests, delegates operation behavior, and sends replies.
pub struct NativeDispatcher<T, H> {
    transport: T,
    handler: H,
}

impl<T, H> NativeDispatcher<T, H> {
    /// Creates a dispatcher from a transport backend and operation handler.
    pub fn new(transport: T, handler: H) -> Self {
        Self { transport, handler }
    }

    /// Returns the owned transport and handler after the loop is no longer needed.
    pub fn into_parts(self) -> (T, H) {
        (self.transport, self.handler)
    }
}

impl<T: FuseTransport, H: FuseOperationHandler> NativeDispatcher<T, H> {
    /// Runs requests until transport EOF/shutdown or `FUSE_DESTROY`.
    ///
    /// A transport that returns `Ok(None)` ends the loop cleanly. `FUSE_DESTROY`
    /// is still delivered to the handler so the operation layer can perform any
    /// final accounting before the loop exits.
    pub fn run_until_destroy(&mut self) -> Result<DispatchSummary, DispatchError> {
        let mut summary = DispatchSummary {
            handled_requests: 0,
            saw_destroy: false,
        };
        while let Some(request) = self
            .transport
            .receive_request()
            .map_err(|error| DispatchError::Receive { source: error })?
        {
            let is_destroy = request.opcode == FuseOpcode::Destroy;
            let opcode = request.opcode;
            let unique = request.header.unique;
            let reply = self.handler.handle(request);
            if !reply.is_no_reply() {
                let reply_len = reply.frame_len();
                let reply_error = reply.error_code();
                if let Err(error) = self.transport.send_reply(reply) {
                    if opcode == FuseOpcode::Release
                        && reply_error == Some(0)
                        && transport_error_raw_errno(&error) == Some(libc::ENOENT)
                    {
                        summary.handled_requests += 1;
                        continue;
                    }
                    return Err(DispatchError::SendReply {
                        opcode,
                        unique,
                        reply_len,
                        reply_error,
                        source: error,
                    });
                }
            }
            summary.handled_requests += 1;
            if is_destroy {
                summary.saw_destroy = true;
                break;
            }
        }

        Ok(DispatchSummary {
            handled_requests: summary.handled_requests,
            saw_destroy: summary.saw_destroy,
        })
    }
}

/// Builder for a bounded asynchronous native dispatcher.
pub struct NativeAsyncDispatcherBuilder<H> {
    handler: H,
    capacities: [usize; RequestClass::CLASS_COUNT],
    worker_counts: [usize; RequestClass::CLASS_COUNT],
}

impl<H> NativeAsyncDispatcherBuilder<H> {
    /// Overrides the bounded queue capacity for one operation class.
    pub fn queue_capacity(mut self, class: RequestClass, capacity: usize) -> Self {
        self.capacities[class.index()] = capacity;
        self
    }

    /// Overrides worker thread count for one operation class.
    pub fn worker_count(mut self, class: RequestClass, workers: usize) -> Self {
        self.worker_counts[class.index()] = workers.max(1);
        self
    }

    /// Starts worker threads for every operation class.
    pub fn spawn<S>(self) -> NativeAsyncDispatcher<S>
    where
        H: AsyncFuseOperationHandler<S>,
        S: ReplySink,
    {
        let telemetry = Arc::new(DispatchTelemetryState::default());
        let mut senders = Vec::with_capacity(RequestClass::ALL.len());
        let mut workers = Vec::with_capacity(RequestClass::ALL.len());

        for class in RequestClass::ALL {
            let (sender, receiver) = crossbeam_channel::bounded(self.capacities[class.index()]);
            for _ in 0..self.worker_counts[class.index()] {
                let handler = self.handler.clone();
                let worker_telemetry = Arc::clone(&telemetry);
                let worker_receiver = receiver.clone();
                let worker = thread::spawn(move || {
                    loop {
                        let envelope = worker_receiver.recv();
                        let Ok(envelope) = envelope else {
                            break;
                        };
                        let _depth = QueueDepthGuard::new(worker_telemetry.as_ref(), class);
                        handler.handle(envelope);
                    }
                });
                workers.push(worker);
            }
            senders.push(sender);
        }

        NativeAsyncDispatcher::<S> {
            senders,
            workers,
            capacities: self.capacities,
            telemetry,
        }
    }
}

/// Bounded worker dispatcher for owned native FUSE requests.
pub struct NativeAsyncDispatcher<S: ReplySink> {
    senders: Vec<Sender<OwnedFuseRequest<S>>>,
    workers: Vec<JoinHandle<()>>,
    capacities: [usize; RequestClass::CLASS_COUNT],
    telemetry: Arc<DispatchTelemetryState>,
}

impl<S> NativeAsyncDispatcher<S>
where
    S: ReplySink,
{
    /// Starts a dispatcher builder around an operation handler.
    pub fn builder<H>(handler: H) -> NativeAsyncDispatcherBuilder<H>
    where
        H: AsyncFuseOperationHandler<S>,
    {
        NativeAsyncDispatcherBuilder {
            handler,
            capacities: [64; RequestClass::CLASS_COUNT],
            worker_counts: [1; RequestClass::CLASS_COUNT],
        }
    }

    /// Attempts to enqueue one owned request without waiting for handler work.
    pub fn submit(&self, envelope: OwnedFuseRequest<S>) -> Result<(), DispatchError> {
        let class = envelope.class();
        let reserved = self.telemetry.queue_depths[class.index()].fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| {
                if current >= self.capacities[class.index()] {
                    None
                } else {
                    Some(current + 1)
                }
            },
        );
        if reserved.is_err() {
            self.telemetry
                .backpressure_events
                .fetch_add(1, Ordering::Relaxed);
            envelope.reply.send_errno(libc::EAGAIN);
            return Err(DispatchError::QueueFull { class });
        }

        match self.senders[class.index()].try_send(envelope) {
            Ok(()) => Ok(()),
            Err(TrySendError::Disconnected(envelope)) => {
                self.telemetry.queue_depths[class.index()].fetch_sub(1, Ordering::Relaxed);
                envelope.reply.send_errno(libc::EIO);
                Err(DispatchError::Shutdown)
            }
            Err(TrySendError::Full(envelope)) => {
                self.telemetry.queue_depths[class.index()].fetch_sub(1, Ordering::Relaxed);
                self.telemetry
                    .backpressure_events
                    .fetch_add(1, Ordering::Relaxed);
                envelope.reply.send_errno(libc::EAGAIN);
                Err(DispatchError::QueueFull { class })
            }
        }
    }

    /// Returns a point-in-time telemetry snapshot.
    pub fn telemetry(&self) -> DispatchTelemetrySnapshot {
        self.telemetry.snapshot()
    }

    /// Stops intake and waits for worker queues to drain.
    pub fn shutdown(self) -> Result<(), DispatchError> {
        self.shutdown_with_timeout(Self::DEFAULT_WORKER_JOIN_TIMEOUT)
    }

    /// Stops intake and waits up to `timeout` for worker queues to drain.
    pub fn shutdown_with_timeout(mut self, timeout: Duration) -> Result<(), DispatchError> {
        self.senders.clear();
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        let mut workers = self.workers;

        while !workers.is_empty() {
            let mut pending = Vec::with_capacity(workers.len());
            for worker in workers {
                if worker.is_finished() {
                    worker.join().map_err(|_| DispatchError::WorkerPanicked)?;
                } else {
                    pending.push(worker);
                }
            }
            if pending.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(DispatchError::ShutdownTimeout {
                    remaining_workers: pending.len(),
                });
            }
            workers = pending;
            let remaining = deadline.saturating_duration_since(Instant::now());
            thread::sleep(remaining.min(Duration::from_millis(1)));
        }
        Ok(())
    }
}

impl<S: ReplySink> NativeAsyncDispatcher<S> {
    const DEFAULT_WORKER_JOIN_TIMEOUT: Duration = Duration::from_secs(30);
    /// Whether worker receivers use an MPMC queue rather than a mutex-wrapped MPSC receiver.
    pub const USES_MPMC_WORKER_QUEUE: bool = true;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::abi::{FUSE_IN_HEADER_LEN, ok_reply};
    use crate::native::reply::RecordedReplySink;
    use crate::native::transport::FrameQueueTransport;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn frame(opcode: u32, unique: u64) -> Vec<u8> {
        let len = FUSE_IN_HEADER_LEN as u32;
        let mut frame = Vec::with_capacity(FUSE_IN_HEADER_LEN);
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(&opcode.to_le_bytes());
        frame.extend_from_slice(&unique.to_le_bytes());
        frame.extend_from_slice(&1_u64.to_le_bytes());
        frame.extend_from_slice(&1000_u32.to_le_bytes());
        frame.extend_from_slice(&1000_u32.to_le_bytes());
        frame.extend_from_slice(&77_u32.to_le_bytes());
        frame.extend_from_slice(&0_u32.to_le_bytes());
        frame
    }

    #[derive(Default)]
    struct RecordingHandler {
        opcodes: Vec<FuseOpcode>,
    }

    impl FuseOperationHandler for RecordingHandler {
        fn handle(&mut self, request: FuseRequest) -> FuseReply {
            self.opcodes.push(request.opcode);
            FuseReply::contiguous(request.header.unique, ok_reply(request.header.unique, &[]))
        }
    }

    #[test]
    fn dispatcher_sends_handler_replies_until_destroy() {
        let transport = FrameQueueTransport::new([frame(1, 0x10), frame(38, 0x11)]);
        let handler = RecordingHandler::default();
        let mut dispatcher = NativeDispatcher::new(transport, handler);

        let summary = dispatcher
            .run_until_destroy()
            .expect("dispatcher should run request loop");
        let (transport, handler) = dispatcher.into_parts();

        assert_eq!(
            summary,
            DispatchSummary {
                handled_requests: 2,
                saw_destroy: true,
            }
        );
        assert_eq!(handler.opcodes, [FuseOpcode::Lookup, FuseOpcode::Destroy]);
        assert_eq!(transport.sent_replies().len(), 2);
    }

    struct EnoentOnReleaseReplyTransport {
        inner: FrameQueueTransport,
    }

    impl FuseTransport for EnoentOnReleaseReplyTransport {
        fn receive_request(&mut self) -> Result<Option<FuseRequest>, TransportError> {
            self.inner.receive_request()
        }

        fn send_reply(&mut self, reply: FuseReply) -> Result<(), TransportError> {
            if reply.unique == 0x20 {
                return Err(std::io::Error::from_raw_os_error(libc::ENOENT).into());
            }
            self.inner.send_reply(reply)
        }
    }

    #[test]
    fn dispatcher_treats_release_reply_enoent_as_completed_release() {
        let transport = EnoentOnReleaseReplyTransport {
            inner: FrameQueueTransport::new([frame(18, 0x20)]),
        };
        let handler = RecordingHandler::default();
        let mut dispatcher = NativeDispatcher::new(transport, handler);

        let summary = dispatcher
            .run_until_destroy()
            .expect("release reply ENOENT should not fail dispatcher");
        let (_transport, handler) = dispatcher.into_parts();

        assert_eq!(handler.opcodes, [FuseOpcode::Release]);
        assert_eq!(
            summary,
            DispatchSummary {
                handled_requests: 1,
                saw_destroy: false,
            }
        );
    }

    #[derive(Default)]
    struct NoReplyHandler {
        handled: usize,
    }

    impl FuseOperationHandler for NoReplyHandler {
        fn handle(&mut self, request: FuseRequest) -> FuseReply {
            self.handled += 1;
            FuseReply::no_reply(request.header.unique)
        }
    }

    #[test]
    fn dispatcher_does_not_write_no_reply_frames() {
        let transport = FrameQueueTransport::new([frame(2, 0x10)]);
        let handler = NoReplyHandler::default();
        let mut dispatcher = NativeDispatcher::new(transport, handler);

        let summary = dispatcher
            .run_until_destroy()
            .expect("dispatcher should run request loop");
        let (transport, handler) = dispatcher.into_parts();

        assert_eq!(
            summary,
            DispatchSummary {
                handled_requests: 1,
                saw_destroy: false,
            }
        );
        assert_eq!(handler.handled, 1);
        assert!(transport.sent_replies().is_empty());
    }

    struct EioOnReplyTransport {
        inner: FrameQueueTransport,
    }

    impl FuseTransport for EioOnReplyTransport {
        fn receive_request(&mut self) -> Result<Option<FuseRequest>, TransportError> {
            self.inner.receive_request()
        }

        fn send_reply(&mut self, _reply: FuseReply) -> Result<(), TransportError> {
            Err(std::io::Error::from_raw_os_error(libc::EIO).into())
        }
    }

    #[test]
    fn dispatcher_preserves_send_reply_transport_source_errno() {
        let transport = EioOnReplyTransport {
            inner: FrameQueueTransport::new([frame(1, 0x21)]),
        };
        let handler = RecordingHandler::default();
        let mut dispatcher = NativeDispatcher::new(transport, handler);

        match dispatcher.run_until_destroy() {
            Err(DispatchError::SendReply { source, .. }) => {
                assert_eq!(transport_error_raw_errno(&source), Some(libc::EIO));
            }
            result => panic!("expected structured send reply error, got {result:?}"),
        }
    }

    #[test]
    fn minimal_handler_replies_to_sfs525_opcode_set_and_unknown_ops() {
        let mut handler = MinimalNativeHandler;

        for opcode in [
            FuseOpcode::Lookup,
            FuseOpcode::Getattr,
            FuseOpcode::Opendir,
            FuseOpcode::Readdir,
            FuseOpcode::Open,
            FuseOpcode::Read,
            FuseOpcode::Release,
            FuseOpcode::Statfs,
            FuseOpcode::Destroy,
            FuseOpcode::Other(999),
        ] {
            let request = FuseRequest {
                header: crate::native::abi::FuseInHeader::new(
                    FUSE_IN_HEADER_LEN as u32,
                    0,
                    0x55,
                    1,
                    1000,
                    1000,
                    77,
                ),
                opcode,
                payload: Vec::new(),
            };

            let reply = handler.handle(request);
            let bytes = reply.to_vec();

            assert_eq!(&bytes[0..4], &16_u32.to_le_bytes());
            assert_eq!(&bytes[4..8], &(-(libc::ENOSYS)).to_le_bytes());
            assert_eq!(&bytes[8..16], &0x55_u64.to_le_bytes());
        }
    }

    #[test]
    fn reply_token_consumes_itself_when_sending_success() {
        let sink = RecordedReplySink::default();
        let token = ReplyToken::new_classic(0x71, sink.clone());

        token.send_success(&[0xde, 0xad, 0xbe, 0xef]);

        assert_eq!(sink.completed_unique_ids(), [0x71]);
        assert_eq!(
            sink.completed_replies()[0].to_vec()[16..],
            [0xde, 0xad, 0xbe, 0xef]
        );
    }

    #[test]
    fn reply_token_consumes_itself_when_sending_errno() {
        let sink = RecordedReplySink::default();
        let token = ReplyToken::new_io_uring(0x72, sink.clone(), 3, 11);

        token.send_errno(libc::ENOENT);

        assert_eq!(sink.completed_unique_ids(), [0x72]);
        assert_eq!(
            &sink.completed_replies()[0].to_vec()[4..8],
            &(-(libc::ENOENT)).to_le_bytes()
        );
    }

    #[test]
    fn reply_token_send_data_keeps_payload_separate_for_vectored_write() {
        let sink = RecordedReplySink::default();
        let token = ReplyToken::new_classic(0x74, sink.clone());
        let payload = vec![1, 2, 3, 4];

        token.send_data(payload.clone());

        assert_eq!(sink.completed_unique_ids(), [0x74]);
        assert_eq!(sink.completed_replies()[0].to_vec()[16..], payload);
        assert_eq!(sink.completed_replies()[0].io_slices().len(), 2);
    }

    #[test]
    fn reply_token_drop_without_completion_fails_closed_with_eio() {
        let sink = RecordedReplySink::default();
        let token = ReplyToken::new_classic(0x73, sink.clone());

        drop(token);

        assert_eq!(sink.dropped_unique_ids(), [0x73]);
        assert_eq!(sink.completed_unique_ids(), [0x73]);
        assert_eq!(
            &sink.completed_replies()[0].to_vec()[4..8],
            &(-(libc::EIO)).to_le_bytes()
        );
    }

    #[derive(Clone)]
    struct BlockingAsyncHandler {
        started: mpsc::Sender<u64>,
        unblock: Arc<Mutex<mpsc::Receiver<()>>>,
    }

    impl AsyncFuseOperationHandler<RecordedReplySink> for BlockingAsyncHandler {
        fn handle(&self, envelope: OwnedFuseRequest<RecordedReplySink>) {
            self.started
                .send(envelope.request.header.unique)
                .expect("test should receive handler start");
            self.unblock
                .lock()
                .expect("unblock receiver poisoned")
                .recv()
                .expect("test should unblock handler");
            envelope.reply.send_success(&[]);
        }
    }

    #[derive(Clone)]
    struct BlockingOneAsyncHandler {
        blocked_unique: u64,
        started: mpsc::Sender<u64>,
        unblock: Arc<Mutex<mpsc::Receiver<()>>>,
    }

    impl AsyncFuseOperationHandler<RecordedReplySink> for BlockingOneAsyncHandler {
        fn handle(&self, envelope: OwnedFuseRequest<RecordedReplySink>) {
            let unique = envelope.request.header.unique;
            self.started
                .send(unique)
                .expect("test should receive handler start");
            if unique == self.blocked_unique {
                self.unblock
                    .lock()
                    .expect("unblock receiver poisoned")
                    .recv()
                    .expect("test should unblock handler");
            }
            envelope.reply.send_success(&[]);
        }
    }

    #[derive(Clone)]
    struct PanickingAsyncHandler {
        started: mpsc::Sender<()>,
    }

    impl AsyncFuseOperationHandler<RecordedReplySink> for PanickingAsyncHandler {
        fn handle(&self, _envelope: OwnedFuseRequest<RecordedReplySink>) {
            self.started
                .send(())
                .expect("test should receive handler start");
            panic!("intentional handler panic");
        }
    }

    #[derive(Clone)]
    struct WaitingAsyncHandler {
        started: mpsc::Sender<()>,
        blocker: Arc<(Mutex<bool>, Condvar)>,
    }

    impl AsyncFuseOperationHandler<RecordedReplySink> for WaitingAsyncHandler {
        fn handle(&self, envelope: OwnedFuseRequest<RecordedReplySink>) {
            self.started
                .send(())
                .expect("test should receive handler start");
            let (lock, changed) = self.blocker.as_ref();
            let mut released = lock.lock().expect("blocker mutex poisoned");
            while !*released {
                released = changed
                    .wait(released)
                    .expect("blocker condvar wait should recover");
            }
            envelope.reply.send_success(&[]);
        }
    }

    #[test]
    fn async_dispatcher_submits_hot_operation_without_waiting_for_handler_reply() {
        let sink = RecordedReplySink::default();
        let (started_tx, started_rx) = mpsc::channel();
        let (unblock_tx, unblock_rx) = mpsc::channel();
        let dispatcher =
            NativeAsyncDispatcher::<RecordedReplySink>::builder(BlockingAsyncHandler {
                started: started_tx,
                unblock: Arc::new(Mutex::new(unblock_rx)),
            })
            .queue_capacity(RequestClass::Read, 1)
            .spawn();

        dispatcher
            .submit(OwnedFuseRequest::classic(
                request(FuseOpcode::Read, 0x80),
                sink.clone(),
            ))
            .expect("first read should enter bounded queue");

        assert_eq!(
            started_rx
                .recv_timeout(Duration::from_millis(250))
                .expect("worker should start out of line"),
            0x80
        );
        assert_eq!(sink.completed_unique_ids(), Vec::<u64>::new());

        unblock_tx
            .send(())
            .expect("handler should still be waiting");
        dispatcher
            .shutdown()
            .expect("dispatcher should join workers");

        assert_eq!(sink.completed_unique_ids(), [0x80]);
    }

    #[test]
    fn async_dispatcher_rejects_when_operation_queue_is_full() {
        let sink = RecordedReplySink::default();
        let (started_tx, started_rx) = mpsc::channel();
        let (_unblock_tx, unblock_rx) = mpsc::channel();
        let dispatcher =
            NativeAsyncDispatcher::<RecordedReplySink>::builder(BlockingAsyncHandler {
                started: started_tx,
                unblock: Arc::new(Mutex::new(unblock_rx)),
            })
            .queue_capacity(RequestClass::Read, 1)
            .spawn();

        dispatcher
            .submit(OwnedFuseRequest::classic(
                request(FuseOpcode::Read, 0x81),
                sink.clone(),
            ))
            .expect("first read should occupy worker");
        assert_eq!(
            started_rx
                .recv_timeout(Duration::from_millis(250))
                .expect("worker should block on first read"),
            0x81
        );

        let rejected = dispatcher.submit(OwnedFuseRequest::classic(
            request(FuseOpcode::Read, 0x82),
            sink.clone(),
        ));

        assert_eq!(
            rejected,
            Err(DispatchError::QueueFull {
                class: RequestClass::Read
            })
        );
        assert_eq!(dispatcher.telemetry().backpressure_events, 1);
        assert_eq!(dispatcher.telemetry().queue_depth(RequestClass::Read), 1);
    }

    #[test]
    fn async_dispatcher_can_run_multiple_workers_for_one_operation_class() {
        let sink = RecordedReplySink::default();
        let (started_tx, started_rx) = mpsc::channel();
        let (unblock_tx, unblock_rx) = mpsc::channel();
        let dispatcher =
            NativeAsyncDispatcher::<RecordedReplySink>::builder(BlockingAsyncHandler {
                started: started_tx,
                unblock: Arc::new(Mutex::new(unblock_rx)),
            })
            .queue_capacity(RequestClass::Read, 2)
            .worker_count(RequestClass::Read, 2)
            .spawn();

        dispatcher
            .submit(OwnedFuseRequest::classic(
                request(FuseOpcode::Read, 0x91),
                sink.clone(),
            ))
            .expect("first read should enter bounded queue");
        dispatcher
            .submit(OwnedFuseRequest::classic(
                request(FuseOpcode::Read, 0x92),
                sink.clone(),
            ))
            .expect("second read should enter bounded queue");

        let first = started_rx
            .recv_timeout(Duration::from_millis(250))
            .expect("first worker should start");
        let second = started_rx
            .recv_timeout(Duration::from_millis(250))
            .expect("second worker should start without waiting for first completion");
        let mut started = [first, second];
        started.sort();
        assert_eq!(started, [0x91, 0x92]);

        unblock_tx.send(()).expect("first read should unblock");
        unblock_tx.send(()).expect("second read should unblock");
        dispatcher
            .shutdown()
            .expect("dispatcher should join workers");
    }

    #[test]
    fn async_dispatcher_idle_worker_takes_request_behind_slow_worker() {
        let sink = RecordedReplySink::default();
        let (started_tx, started_rx) = mpsc::channel();
        let (unblock_tx, unblock_rx) = mpsc::channel();
        let dispatcher =
            NativeAsyncDispatcher::<RecordedReplySink>::builder(BlockingOneAsyncHandler {
                blocked_unique: 0xb1,
                started: started_tx,
                unblock: Arc::new(Mutex::new(unblock_rx)),
            })
            .queue_capacity(RequestClass::Read, 3)
            .worker_count(RequestClass::Read, 2)
            .spawn();

        dispatcher
            .submit(OwnedFuseRequest::classic(
                request(FuseOpcode::Read, 0xb1),
                sink.clone(),
            ))
            .expect("first read should enter bounded queue");
        assert_eq!(
            started_rx
                .recv_timeout(Duration::from_millis(250))
                .expect("slow worker should start"),
            0xb1
        );

        dispatcher
            .submit(OwnedFuseRequest::classic(
                request(FuseOpcode::Read, 0xb2),
                sink.clone(),
            ))
            .expect("second read should enter bounded queue");
        assert_eq!(
            started_rx
                .recv_timeout(Duration::from_millis(250))
                .expect("fast worker should start"),
            0xb2
        );

        dispatcher
            .submit(OwnedFuseRequest::classic(
                request(FuseOpcode::Read, 0xb3),
                sink.clone(),
            ))
            .expect("third read should enter bounded queue");
        assert_eq!(
            started_rx
                .recv_timeout(Duration::from_millis(250))
                .expect("idle worker should take queued read"),
            0xb3
        );

        unblock_tx.send(()).expect("slow read should unblock");
        dispatcher
            .shutdown()
            .expect("dispatcher should join workers");
    }

    #[test]
    fn async_dispatcher_releases_queue_depth_when_handler_panics() {
        let sink = RecordedReplySink::default();
        let (started_tx, started_rx) = mpsc::channel();
        let dispatcher =
            NativeAsyncDispatcher::<RecordedReplySink>::builder(PanickingAsyncHandler {
                started: started_tx,
            })
            .queue_capacity(RequestClass::Read, 1)
            .spawn();

        dispatcher
            .submit(OwnedFuseRequest::classic(
                request(FuseOpcode::Read, 0xa1),
                sink.clone(),
            ))
            .expect("read should enter bounded queue");
        started_rx
            .recv_timeout(Duration::from_millis(250))
            .expect("handler should start and panic");

        let deadline = std::time::Instant::now() + Duration::from_millis(250);
        while dispatcher.telemetry().queue_depth(RequestClass::Read) != 0
            && std::time::Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        assert_eq!(dispatcher.telemetry().queue_depth(RequestClass::Read), 0);

        let disconnected = dispatcher.submit(OwnedFuseRequest::classic(
            request(FuseOpcode::Read, 0xa2),
            sink.clone(),
        ));

        assert_eq!(disconnected, Err(DispatchError::Shutdown));
        assert_eq!(sink.completed_unique_ids(), [0xa1, 0xa2]);
        assert_eq!(
            &sink.completed_replies()[1].to_vec()[4..8],
            &(-(libc::EIO)).to_le_bytes()
        );
        assert_eq!(dispatcher.shutdown(), Err(DispatchError::WorkerPanicked));
    }

    #[test]
    fn dispatcher_classifies_write_and_barrier_opcodes_explicitly() {
        assert_eq!(
            RequestClass::for_opcode(FuseOpcode::Write),
            RequestClass::Write
        );
        assert_eq!(
            RequestClass::for_opcode(FuseOpcode::Flush),
            RequestClass::Barrier
        );
        assert_eq!(
            RequestClass::for_opcode(FuseOpcode::Fsync),
            RequestClass::Barrier
        );
        assert_eq!(
            RequestClass::for_opcode(FuseOpcode::Release),
            RequestClass::Barrier
        );
        assert_eq!(
            RequestClass::for_opcode(FuseOpcode::Forget),
            RequestClass::Background
        );
        assert_eq!(
            RequestClass::for_opcode(FuseOpcode::BatchForget),
            RequestClass::Background
        );
    }

    #[test]
    fn request_class_count_matches_class_table() {
        assert_eq!(RequestClass::ALL.len(), RequestClass::CLASS_COUNT);
    }

    #[test]
    fn async_dispatcher_uses_mpmc_worker_queue() {
        const {
            assert!(NativeAsyncDispatcher::<RecordedReplySink>::USES_MPMC_WORKER_QUEUE);
        }
    }

    #[test]
    fn barrier_entry_keeps_common_pending_writes_inline() {
        assert_eq!(PENDING_WRITE_INLINE_CAPACITY, 8);
    }

    #[test]
    fn async_dispatcher_shutdown_timeout_returns_when_worker_is_stuck() {
        let sink = RecordedReplySink::default();
        let (started_tx, started_rx) = mpsc::channel();
        let blocker = Arc::new((Mutex::new(false), Condvar::new()));
        let dispatcher = NativeAsyncDispatcher::<RecordedReplySink>::builder(WaitingAsyncHandler {
            started: started_tx,
            blocker: Arc::clone(&blocker),
        })
        .queue_capacity(RequestClass::Read, 1)
        .spawn();

        dispatcher
            .submit(OwnedFuseRequest::classic(
                request(FuseOpcode::Read, 0xc1),
                sink.clone(),
            ))
            .expect("read should enter bounded queue");
        started_rx
            .recv_timeout(Duration::from_millis(250))
            .expect("handler should start and block");

        match dispatcher.shutdown_with_timeout(Duration::from_millis(10)) {
            Err(DispatchError::ShutdownTimeout { remaining_workers }) => {
                assert!(
                    remaining_workers >= 1,
                    "blocked worker should still be running at timeout"
                );
            }
            result => panic!("expected shutdown timeout, got {result:?}"),
        }

        let (lock, changed) = blocker.as_ref();
        *lock.lock().expect("blocker mutex poisoned") = true;
        changed.notify_all();
    }

    #[test]
    fn barrier_gate_waits_for_pending_writes_on_same_inode() {
        let gates = OperationBarrierGates::default();
        let pending = gates.begin_write(ResourceKey::Inode(10));
        let (finished_tx, finished_rx) = mpsc::channel();
        let waiter_gates = gates.clone();

        std::thread::spawn(move || {
            waiter_gates.wait_for_barrier(ResourceKey::Inode(10));
            finished_tx
                .send(())
                .expect("test should observe barrier completion");
        });

        assert!(finished_rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(pending);

        finished_rx
            .recv_timeout(Duration::from_millis(250))
            .expect("barrier should finish after same-inode write drains");
    }

    #[test]
    fn barrier_gate_does_not_wait_for_unrelated_inode_writes() {
        let gates = OperationBarrierGates::default();
        let _pending = gates.begin_write(ResourceKey::Inode(20));
        let (finished_tx, finished_rx) = mpsc::channel();
        let waiter_gates = gates.clone();

        std::thread::spawn(move || {
            waiter_gates.wait_for_barrier(ResourceKey::Inode(21));
            finished_tx
                .send(())
                .expect("test should observe unrelated barrier completion");
        });

        finished_rx
            .recv_timeout(Duration::from_millis(250))
            .expect("barrier should not wait for other inode writes");
    }

    #[test]
    fn barrier_gate_shards_independent_resource_state() {
        assert_ne!(
            BarrierShared::shard_index(ResourceKey::Inode(20)),
            BarrierShared::shard_index(ResourceKey::Inode(21))
        );
        assert_ne!(
            BarrierShared::shard_index(ResourceKey::Inode(64)),
            BarrierShared::shard_index(ResourceKey::Inode(128))
        );
        assert_eq!(BarrierShared::SHARD_COUNT, 64);
    }

    #[test]
    fn barrier_gate_waits_only_for_writes_pending_when_barrier_started() {
        let gates = OperationBarrierGates::default();
        let pending_before_barrier = gates.begin_write(ResourceKey::Inode(30));
        let (finished_tx, finished_rx) = mpsc::channel();
        let waiter_gates = gates.clone();

        std::thread::spawn(move || {
            waiter_gates.wait_for_barrier(ResourceKey::Inode(30));
            finished_tx
                .send(())
                .expect("test should observe barrier completion");
        });

        assert!(finished_rx.recv_timeout(Duration::from_millis(50)).is_err());
        let _pending_after_barrier = gates.begin_write(ResourceKey::Inode(30));
        drop(pending_before_barrier);

        finished_rx
            .recv_timeout(Duration::from_millis(250))
            .expect("barrier should ignore writes that started after it");
    }

    fn request(opcode: FuseOpcode, unique: u64) -> FuseRequest {
        FuseRequest {
            header: crate::native::abi::FuseInHeader::new(
                FUSE_IN_HEADER_LEN as u32,
                0,
                unique,
                1,
                1000,
                1000,
                77,
            ),
            opcode,
            payload: Vec::new(),
        }
    }
}
