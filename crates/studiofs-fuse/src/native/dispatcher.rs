//! Transport-independent native FUSE dispatcher.

use std::collections::HashMap;
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

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
        FuseReply {
            unique: request.header.unique,
            bytes: error_reply(request.header.unique, libc::ENOSYS).to_vec(),
        }
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
    const ALL: [Self; 5] = [
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
            FuseOpcode::Flush | FuseOpcode::Fsync | FuseOpcode::Release => Self::Barrier,
            FuseOpcode::Other(_) => Self::Background,
            FuseOpcode::Lookup
            | FuseOpcode::Getattr
            | FuseOpcode::Open
            | FuseOpcode::Statfs
            | FuseOpcode::Init
            | FuseOpcode::Opendir
            | FuseOpcode::Readdir
            | FuseOpcode::Destroy => Self::Metadata,
        }
    }
}

/// Owned request plus reply capability passed from transport intake to workers.
#[derive(Debug, Clone)]
pub struct OwnedFuseRequest<S> {
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
    pending_writes: HashMap<ResourceKey, usize>,
}

#[derive(Debug, Default)]
struct BarrierShared {
    state: Mutex<BarrierState>,
    changed: Condvar,
}

/// Per-resource gates for write and barrier operation ordering.
#[derive(Debug, Clone, Default)]
pub struct OperationBarrierGates {
    shared: Arc<BarrierShared>,
}

impl OperationBarrierGates {
    /// Records a pending write for `resource` until the returned guard drops.
    pub fn begin_write(&self, resource: ResourceKey) -> PendingWriteGuard {
        let mut state = self.shared.state.lock().expect("barrier state poisoned");
        *state.pending_writes.entry(resource).or_default() += 1;
        PendingWriteGuard {
            resource,
            shared: Arc::clone(&self.shared),
        }
    }

    /// Waits until writes for `resource` drain.
    pub fn wait_for_barrier(&self, resource: ResourceKey) {
        let mut state = self.shared.state.lock().expect("barrier state poisoned");
        while state.pending_writes.get(&resource).copied().unwrap_or(0) > 0 {
            state = self
                .shared
                .changed
                .wait(state)
                .expect("barrier state poisoned while waiting");
        }
    }
}

/// Guard that marks one pending write complete on drop.
#[derive(Debug)]
pub struct PendingWriteGuard {
    resource: ResourceKey,
    shared: Arc<BarrierShared>,
}

impl Drop for PendingWriteGuard {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().expect("barrier state poisoned");
        if let Some(count) = state.pending_writes.get_mut(&self.resource) {
            *count -= 1;
            if *count == 0 {
                state.pending_writes.remove(&self.resource);
                self.shared.changed.notify_all();
            }
        }
    }
}

/// Point-in-time dispatcher telemetry used by tests and future exporters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchTelemetrySnapshot {
    queue_depths: [usize; 5],
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
    queue_depths: [usize; 5],
    backpressure_events: usize,
}

impl DispatchTelemetryState {
    fn snapshot(&self) -> DispatchTelemetrySnapshot {
        DispatchTelemetrySnapshot {
            queue_depths: self.queue_depths,
            backpressure_events: self.backpressure_events,
        }
    }
}

/// Error returned by the native dispatcher.
#[derive(Debug, Error)]
pub enum DispatchError {
    /// The underlying transport failed while receiving a request or sending a reply.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// A bounded operation queue has no capacity for another request.
    #[error("native FUSE dispatcher queue full for {class:?}")]
    QueueFull {
        /// Operation class that rejected the request.
        class: RequestClass,
    },
    /// A worker thread panicked during shutdown.
    #[error("native FUSE dispatcher worker panicked")]
    WorkerPanicked,
}

impl PartialEq for DispatchError {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::QueueFull { class: left }, Self::QueueFull { class: right }) if left == right
        ) || matches!((self, other), (Self::WorkerPanicked, Self::WorkerPanicked))
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
        while let Some(request) = self.transport.receive_request()? {
            let is_destroy = request.opcode == FuseOpcode::Destroy;
            let reply = self.handler.handle(request);
            self.transport.send_reply(reply)?;
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
    capacities: [usize; 5],
}

impl<H> NativeAsyncDispatcherBuilder<H> {
    /// Overrides the bounded queue capacity for one operation class.
    pub fn queue_capacity(mut self, class: RequestClass, capacity: usize) -> Self {
        self.capacities[class.index()] = capacity;
        self
    }

    /// Starts worker threads for every operation class.
    pub fn spawn<S>(self) -> NativeAsyncDispatcher<S>
    where
        H: AsyncFuseOperationHandler<S>,
        S: ReplySink,
    {
        let telemetry = Arc::new(Mutex::new(DispatchTelemetryState::default()));
        let mut senders = Vec::with_capacity(RequestClass::ALL.len());
        let mut workers = Vec::with_capacity(RequestClass::ALL.len());

        for class in RequestClass::ALL {
            let (sender, receiver) = mpsc::sync_channel(self.capacities[class.index()]);
            let handler = self.handler.clone();
            let worker_telemetry = Arc::clone(&telemetry);
            let worker = thread::spawn(move || {
                while let Ok(envelope) = receiver.recv() {
                    handler.handle(envelope);
                    worker_telemetry
                        .lock()
                        .expect("dispatcher telemetry state poisoned")
                        .queue_depths[class.index()] -= 1;
                }
            });
            senders.push(Some(sender));
            workers.push(worker);
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
pub struct NativeAsyncDispatcher<S> {
    senders: Vec<Option<SyncSender<OwnedFuseRequest<S>>>>,
    workers: Vec<JoinHandle<()>>,
    capacities: [usize; 5],
    telemetry: Arc<Mutex<DispatchTelemetryState>>,
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
            capacities: [64; 5],
        }
    }

    /// Attempts to enqueue one owned request without waiting for handler work.
    pub fn submit(&self, envelope: OwnedFuseRequest<S>) -> Result<(), DispatchError> {
        let class = envelope.class();
        {
            let mut telemetry = self
                .telemetry
                .lock()
                .expect("dispatcher telemetry state poisoned");
            if telemetry.queue_depths[class.index()] >= self.capacities[class.index()] {
                telemetry.backpressure_events += 1;
                drop(telemetry);
                envelope.reply.send_errno(libc::EAGAIN).ok();
                return Err(DispatchError::QueueFull { class });
            }
            telemetry.queue_depths[class.index()] += 1;
        }

        match self.senders[class.index()]
            .as_ref()
            .expect("dispatcher sender missing before shutdown")
            .try_send(envelope)
        {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(envelope)) => {
                let mut telemetry = self
                    .telemetry
                    .lock()
                    .expect("dispatcher telemetry state poisoned");
                telemetry.queue_depths[class.index()] -= 1;
                telemetry.backpressure_events += 1;
                drop(telemetry);
                envelope.reply.send_errno(libc::EAGAIN).ok();
                Err(DispatchError::QueueFull { class })
            }
            Err(TrySendError::Disconnected(envelope)) => {
                self.telemetry
                    .lock()
                    .expect("dispatcher telemetry state poisoned")
                    .queue_depths[class.index()] -= 1;
                envelope.reply.drop_without_reply();
                Err(DispatchError::QueueFull { class })
            }
        }
    }

    /// Returns a point-in-time telemetry snapshot.
    pub fn telemetry(&self) -> DispatchTelemetrySnapshot {
        self.telemetry
            .lock()
            .expect("dispatcher telemetry state poisoned")
            .snapshot()
    }

    /// Stops intake and waits for worker queues to drain.
    pub fn shutdown(mut self) -> Result<(), DispatchError> {
        for sender in &mut self.senders {
            sender.take();
        }
        for worker in self.workers {
            worker.join().map_err(|_| DispatchError::WorkerPanicked)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::abi::{FUSE_IN_HEADER_LEN, ok_reply};
    use crate::native::reply::{RecordedReplySink, ReplyCompletionError, ReplyToken};
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
            FuseReply {
                unique: request.header.unique,
                bytes: ok_reply(request.header.unique, &[]),
            }
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

            assert_eq!(&reply.bytes[0..4], &16_u32.to_le_bytes());
            assert_eq!(&reply.bytes[4..8], &(-(libc::ENOSYS)).to_le_bytes());
            assert_eq!(&reply.bytes[8..16], &0x55_u64.to_le_bytes());
        }
    }

    #[test]
    fn reply_token_sends_success_exactly_once() {
        let sink = RecordedReplySink::default();
        let token = ReplyToken::new_classic(0x71, sink.clone());

        token
            .send_success(&[0xde, 0xad, 0xbe, 0xef])
            .expect("first success reply should be accepted");
        let second = token.send_errno(libc::EIO);

        assert_eq!(second, Err(ReplyCompletionError::AlreadyCompleted));
        assert_eq!(sink.completed_unique_ids(), [0x71]);
        assert_eq!(
            sink.completed_replies()[0].bytes[16..],
            [0xde, 0xad, 0xbe, 0xef]
        );
    }

    #[test]
    fn reply_token_sends_errno_exactly_once() {
        let sink = RecordedReplySink::default();
        let token = ReplyToken::new_io_uring(0x72, sink.clone(), 3, 11);

        token
            .send_errno(libc::ENOENT)
            .expect("first errno reply should be accepted");
        let second = token.send_data(vec![1, 2, 3]);

        assert_eq!(second, Err(ReplyCompletionError::AlreadyCompleted));
        assert_eq!(sink.completed_unique_ids(), [0x72]);
        assert_eq!(
            &sink.completed_replies()[0].bytes[4..8],
            &(-(libc::ENOENT)).to_le_bytes()
        );
    }

    #[test]
    fn reply_token_records_drop_without_later_reply() {
        let sink = RecordedReplySink::default();
        let token = ReplyToken::new_classic(0x73, sink.clone());

        token.drop_without_reply();
        let second = token.send_success(&[]);

        assert_eq!(second, Err(ReplyCompletionError::AlreadyCompleted));
        assert_eq!(sink.dropped_unique_ids(), [0x73]);
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
            envelope
                .reply
                .send_success(&[])
                .expect("handler should own first reply completion");
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
