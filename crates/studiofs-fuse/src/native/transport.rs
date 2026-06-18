//! Transport boundary for Linux-native FUSE sessions.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use io_uring::{opcode, types};
use thiserror::Error;

use crate::native::abi::{AbiParseError, FUSE_IN_HEADER_LEN, FUSE_OUT_HEADER_LEN};
use crate::native::protocol::{FuseLocalCacheReadRange, FuseOpcode, FuseReply, FuseRequest};
use crate::native::reply::{ReplyOrigin, ReplySink};

const DEFAULT_CLASSIC_MAX_WRITE: usize = 16 * 1024 * 1024;
const PREAD_COPY_BUFFER_LEN: usize = 128 * 1024;
const SPLICE_PIPE_CHUNK_LEN: usize = 64 * 1024;
const FUSE_MIN_READ_BUFFER: usize = 8192;
const FUSE_URING_IN_OUT_HEADER_SZ: usize = 128;
const FUSE_URING_OP_IN_OUT_SZ: usize = 128;
const FUSE_URING_ENT_IN_OUT_SZ: usize = 32;
const FUSE_URING_REQ_HEADER_SZ: usize =
    FUSE_URING_IN_OUT_HEADER_SZ + FUSE_URING_OP_IN_OUT_SZ + FUSE_URING_ENT_IN_OUT_SZ;

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FuseUringEntInOut {
    flags: u64,
    commit_id: u64,
    payload_sz: u32,
    padding: u32,
    reserved: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FuseUringReqHeader {
    in_out: [u8; FUSE_URING_IN_OUT_HEADER_SZ],
    op_in: [u8; FUSE_URING_OP_IN_OUT_SZ],
    ring_ent_in_out: FuseUringEntInOut,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FuseUringCmdReq {
    flags: u64,
    commit_id: u64,
    qid: u16,
    padding: [u8; 6],
}

const _: [(); FUSE_URING_ENT_IN_OUT_SZ] = [(); std::mem::size_of::<FuseUringEntInOut>()];
const _: [(); FUSE_URING_REQ_HEADER_SZ] = [(); std::mem::size_of::<FuseUringReqHeader>()];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuseUringCommandOp {
    Register,
    CommitAndFetch,
}

impl FuseUringCommandOp {
    fn as_raw(self) -> u32 {
        match self {
            Self::Register => 1,
            Self::CommitAndFetch => 2,
        }
    }
}

#[derive(Debug)]
pub struct FuseUringCommand<'a> {
    pub op: FuseUringCommandOp,
    fuse_fd: RawFd,
    cmd_req: FuseUringCmdReq,
    iovecs: &'a [libc::iovec; 2],
    _header: &'a FuseUringReqHeader,
    _payload: &'a mut [u8],
}

impl FuseUringCommand<'_> {
    fn cmd_bytes(&self) -> [u8; 80] {
        let mut cmd = [0; 80];
        let cmd_req = &self.cmd_req as *const FuseUringCmdReq;
        // SAFETY: `cmd` has 80 bytes and `FuseUringCmdReq` is a 24-byte repr(C)
        // UAPI payload copied byte-for-byte into the SQE128 command area.
        unsafe {
            std::ptr::copy_nonoverlapping(
                cmd_req.cast::<u8>(),
                cmd.as_mut_ptr(),
                std::mem::size_of::<FuseUringCmdReq>(),
            );
        }
        cmd
    }

    pub fn build_sqe(&self, user_data: u64) -> io_uring::squeue::Entry128 {
        let mut sqe = opcode::UringCmd80::new(types::Fd(self.fuse_fd), self.op.as_raw())
            .cmd(self.cmd_bytes())
            .addr(Some(self.iovecs.as_ptr() as u64))
            .build()
            .user_data(user_data);
        set_uring_cmd_iovec_count(&mut sqe, self.iovecs.len() as u32);
        sqe
    }
}

#[repr(C)]
struct IoUringSqePrefix {
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off_or_cmd_op: u64,
    addr: u64,
    len: u32,
}

fn set_uring_cmd_iovec_count(sqe: &mut io_uring::squeue::Entry128, iovec_count: u32) {
    // SAFETY: `io_uring::squeue::Entry128` is repr(C) and starts with the
    // 64-byte Linux io_uring SQE. The public `UringCmd80` builder exposes
    // `addr` but not the adjacent `len` field, which FUSE over io_uring uses
    // as the iovec count for `sqe->addr`.
    unsafe {
        (*(sqe as *mut _ as *mut IoUringSqePrefix)).len = iovec_count;
    }
}

#[derive(Debug)]
pub struct FuseUringEntry {
    qid: u16,
    user_data: u64,
    generation: u64,
    header: Box<FuseUringReqHeader>,
    payload: Vec<u8>,
    iovecs: Box<[libc::iovec; 2]>,
}

// SAFETY: `FuseUringEntry` owns the heap allocations referenced by its iovec
// array (`header` and `payload`). Moving the entry moves only owning handles;
// the pointees remain stable, and all mutation goes through `&mut self`.
unsafe impl Send for FuseUringEntry {}

impl FuseUringEntry {
    pub fn new(qid: u16, payload_len: usize) -> Self {
        Self {
            qid,
            user_data: u64::from(qid) + 1,
            generation: 0,
            header: Box::new(FuseUringReqHeader {
                in_out: [0; FUSE_URING_IN_OUT_HEADER_SZ],
                op_in: [0; FUSE_URING_OP_IN_OUT_SZ],
                ring_ent_in_out: FuseUringEntInOut {
                    payload_sz: payload_len as u32,
                    ..Default::default()
                },
            }),
            payload: vec![0; payload_len],
            iovecs: Box::new([
                libc::iovec {
                    iov_base: std::ptr::null_mut(),
                    iov_len: 0,
                },
                libc::iovec {
                    iov_base: std::ptr::null_mut(),
                    iov_len: 0,
                },
            ]),
        }
    }

    pub fn register_command(&mut self, fuse_fd: RawFd) -> FuseUringCommand<'_> {
        self.command(FuseUringCommandOp::Register, fuse_fd, 0)
    }

    pub fn commit_and_fetch_command(
        &mut self,
        fuse_fd: RawFd,
        commit_id: u64,
    ) -> FuseUringCommand<'_> {
        self.command(FuseUringCommandOp::CommitAndFetch, fuse_fd, commit_id)
    }

    #[cfg(test)]
    fn write_request_for_test(&mut self, frame: &[u8]) {
        let header_len = FUSE_IN_HEADER_LEN.min(frame.len());
        self.header.in_out[..header_len].copy_from_slice(&frame[..header_len]);
        let op_len = frame.len().saturating_sub(FUSE_IN_HEADER_LEN);
        self.header.op_in[..op_len].copy_from_slice(&frame[FUSE_IN_HEADER_LEN..]);
        self.header.ring_ent_in_out.payload_sz = 0;
        self.header.ring_ent_in_out.commit_id = u64::from_le_bytes(
            self.header.in_out[8..16]
                .try_into()
                .expect("test frame should include unique"),
        );
    }

    #[cfg(test)]
    fn write_request_parts_for_test(&mut self, frame: &[u8], op_len: usize) {
        let header_len = FUSE_IN_HEADER_LEN.min(frame.len());
        self.header.in_out[..header_len].copy_from_slice(&frame[..header_len]);
        let payload = &frame[FUSE_IN_HEADER_LEN + op_len..];
        self.header.op_in[..op_len].copy_from_slice(&frame[FUSE_IN_HEADER_LEN..][..op_len]);
        self.payload[..payload.len()].copy_from_slice(payload);
        self.header.ring_ent_in_out.payload_sz = payload.len() as u32;
        self.header.ring_ent_in_out.commit_id = u64::from_le_bytes(
            self.header.in_out[8..16]
                .try_into()
                .expect("test frame should include unique"),
        );
    }

    fn request_frame(&self) -> Result<Vec<u8>, TransportError> {
        let header = &self.header.in_out[..FUSE_IN_HEADER_LEN];
        let opcode = u32::from_le_bytes(header[4..8].try_into().map_err(|_| {
            AbiParseError::FrameTooShort {
                expected: FUSE_IN_HEADER_LEN,
                actual: header.len(),
            }
        })?);
        let op_len = fuse_uring_op_header_len(FuseOpcode::from_raw(opcode));
        let payload_len = self.header.ring_ent_in_out.payload_sz as usize;
        if payload_len > self.payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "FUSE io_uring payload size exceeds registered entry buffer",
            )
            .into());
        }
        let mut frame = Vec::with_capacity(FUSE_IN_HEADER_LEN + op_len + payload_len);
        frame.extend_from_slice(header);
        frame.extend_from_slice(&self.header.op_in[..op_len]);
        frame.extend_from_slice(&self.payload[..payload_len]);
        let len = u32::try_from(frame.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "FUSE io_uring request frame length exceeds u32",
            )
        })?;
        frame[0..4].copy_from_slice(&len.to_le_bytes());
        Ok(frame)
    }

    fn commit_id(&self) -> u64 {
        self.header.ring_ent_in_out.commit_id
    }

    fn user_data(&self) -> u64 {
        self.user_data
    }

    fn generation(&self) -> u64 {
        self.generation
    }

    fn next_generation(&mut self) -> u64 {
        self.generation = self.generation.saturating_add(1);
        self.generation
    }

    #[cfg(test)]
    fn reply_payload_len_for_test(&self) -> u32 {
        self.header.ring_ent_in_out.payload_sz
    }

    #[cfg(test)]
    fn reply_payload_for_test(&self) -> &[u8] {
        let len = self.header.ring_ent_in_out.payload_sz as usize;
        &self.payload[..len]
    }

    #[cfg(test)]
    fn iovecs_ptr_for_test(&self) -> *const libc::iovec {
        self.iovecs.as_ptr()
    }

    fn command(
        &mut self,
        op: FuseUringCommandOp,
        fuse_fd: RawFd,
        commit_id: u64,
    ) -> FuseUringCommand<'_> {
        *self.iovecs = [
            libc::iovec {
                iov_base: self.header.as_mut() as *mut FuseUringReqHeader as *mut libc::c_void,
                iov_len: std::mem::size_of::<FuseUringReqHeader>(),
            },
            libc::iovec {
                iov_base: self.payload.as_mut_ptr().cast(),
                iov_len: self.payload.len(),
            },
        ];
        FuseUringCommand {
            op,
            fuse_fd,
            cmd_req: FuseUringCmdReq {
                flags: 0,
                commit_id,
                qid: self.qid,
                padding: [0; 6],
            },
            iovecs: self.iovecs.as_ref(),
            _header: self.header.as_ref(),
            _payload: self.payload.as_mut_slice(),
        }
    }
}

fn fuse_uring_op_header_len(opcode: FuseOpcode) -> usize {
    match opcode {
        FuseOpcode::Forget => 8,
        FuseOpcode::Getattr => 16,
        FuseOpcode::Setattr => 88,
        FuseOpcode::Mkdir => 8,
        FuseOpcode::Create => 16,
        FuseOpcode::Rename => 8,
        FuseOpcode::Open | FuseOpcode::Opendir => 8,
        FuseOpcode::Read | FuseOpcode::Write | FuseOpcode::Readdir | FuseOpcode::Readdirplus => 40,
        FuseOpcode::Release | FuseOpcode::Flush | FuseOpcode::Releasedir => 24,
        FuseOpcode::Fsync => 16,
        FuseOpcode::BatchForget => 8,
        FuseOpcode::Init => 16,
        _ => 0,
    }
}

pub fn io_uring_payload_len_for_session(max_write: u32, max_pages: u16) -> usize {
    FUSE_MIN_READ_BUFFER
        .max(max_write as usize)
        .max(usize::from(max_pages).saturating_mul(page_size_for_io_uring_payload()))
}

fn page_size_for_io_uring_payload() -> usize {
    // SAFETY: sysconf with _SC_PAGESIZE has no memory-safety preconditions.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(page_size).unwrap_or(4096).max(1)
}

pub fn linux_fuse_io_uring_queue_count() -> u16 {
    std::fs::read_to_string("/sys/devices/system/cpu/possible")
        .ok()
        .and_then(|raw| parse_linux_possible_cpu_count(&raw))
        .and_then(|count| u16::try_from(count).ok())
        .filter(|count| *count > 0)
        .or_else(|| {
            std::thread::available_parallelism()
                .ok()
                .and_then(|count| u16::try_from(count.get()).ok())
        })
        .unwrap_or(1)
}

fn parse_linux_possible_cpu_count(raw: &str) -> Option<usize> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut total = 0_usize;
    for part in trimmed.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return None;
        }
        let count = if let Some((start, end)) = part.split_once('-') {
            let start = start.parse::<usize>().ok()?;
            let end = end.parse::<usize>().ok()?;
            end.checked_sub(start)?.checked_add(1)?
        } else {
            part.parse::<usize>().ok()?;
            1
        };
        total = total.checked_add(count)?;
    }
    Some(total)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseIoUringCompletion {
    pub user_data: u64,
    pub result: i32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FuseIoUringTransportMetricsSnapshot {
    /// Number of registered io_uring queue entries.
    pub queue_depth: u16,
    /// Number of REGISTER uring_cmd submissions.
    pub register_commands: u64,
    /// Number of COMMIT_AND_FETCH uring_cmd submissions.
    pub commit_and_fetch_commands: u64,
    /// Number of CQEs drained from the io_uring completion queue.
    pub cqe_completions: u64,
    /// Number of CQEs whose result was a kernel error.
    pub cqe_errors: u64,
    /// Number of submit calls that flushed pending SQEs without waiting.
    pub submit_flushes: u64,
    /// Total time spent flushing pending SQEs without waiting.
    pub submit_flush_total_ns: u64,
    /// Number of submit_and_wait calls issued for register or commit/fetch.
    pub submit_waits: u64,
    /// Total time spent inside submit_and_wait calls.
    pub submit_wait_total_ns: u64,
    /// Total time spent polling/draining completion queue entries.
    pub cqe_drain_total_ns: u64,
}

#[derive(Debug, Default)]
struct FuseIoUringTransportMetrics {
    register_commands: u64,
    commit_and_fetch_commands: u64,
    cqe_completions: u64,
    cqe_errors: u64,
    submit_flushes: u64,
    submit_flush_total_ns: u64,
    submit_waits: u64,
    submit_wait_total_ns: u64,
    cqe_drain_total_ns: u64,
}

impl FuseIoUringTransportMetrics {
    fn record_submit_flush(&mut self, started_at: Instant) {
        self.submit_flushes = self.submit_flushes.saturating_add(1);
        self.submit_flush_total_ns = self
            .submit_flush_total_ns
            .saturating_add(elapsed_transport_latency_ns(started_at));
    }

    fn record_cqe_drain(&mut self, started_at: Instant) {
        self.cqe_drain_total_ns = self
            .cqe_drain_total_ns
            .saturating_add(elapsed_transport_latency_ns(started_at));
    }
}

pub trait FuseUringBackend {
    fn submit(&mut self, entry: io_uring::squeue::Entry128) -> io::Result<()>;
    fn submit_pending(&mut self) -> io::Result<usize>;
    fn submit_and_wait(&mut self, wanted: usize) -> io::Result<usize>;
    fn next_completion(&mut self) -> io::Result<Option<FuseIoUringCompletion>>;
    fn poll_fd(&self) -> Option<RawFd> {
        None
    }
}

pub struct IoUringBackend {
    ring: io_uring::IoUring<io_uring::squeue::Entry128, io_uring::cqueue::Entry>,
}

impl IoUringBackend {
    pub fn new(queue_depth: u32) -> io::Result<Self> {
        Ok(Self {
            ring:
                io_uring::IoUring::<io_uring::squeue::Entry128, io_uring::cqueue::Entry>::builder()
                    .build(queue_depth)?,
        })
    }
}

impl FuseUringBackend for IoUringBackend {
    fn submit(&mut self, entry: io_uring::squeue::Entry128) -> io::Result<()> {
        // SAFETY: every SQE references memory owned by the corresponding
        // `FuseUringEntry`, and entries are retained by `FuseIoUringTransport`
        // until the backend is dropped.
        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| io::Error::other("io_uring submission queue is full"))?;
        }
        Ok(())
    }

    fn submit_and_wait(&mut self, wanted: usize) -> io::Result<usize> {
        self.ring.submit_and_wait(wanted)
    }

    fn submit_pending(&mut self) -> io::Result<usize> {
        self.ring.submit()
    }

    fn next_completion(&mut self) -> io::Result<Option<FuseIoUringCompletion>> {
        Ok(self
            .ring
            .completion()
            .next()
            .map(|completion| FuseIoUringCompletion {
                user_data: completion.user_data(),
                result: completion.result(),
            }))
    }

    fn poll_fd(&self) -> Option<RawFd> {
        Some(self.ring.as_raw_fd())
    }
}

#[derive(Debug)]
pub struct FuseIoUringTransport<B> {
    fuse_fd: RawFd,
    entries: Vec<FuseUringEntry>,
    backend: B,
    ready: bool,
    metrics: FuseIoUringTransportMetrics,
}

impl<B> FuseIoUringTransport<B> {
    pub fn with_backend(fuse_fd: RawFd, queue_count: u16, payload_len: usize, backend: B) -> Self {
        let entries = (0..queue_count)
            .map(|qid| FuseUringEntry::new(qid, payload_len))
            .collect();
        Self {
            fuse_fd,
            entries,
            backend,
            ready: false,
            metrics: FuseIoUringTransportMetrics::default(),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }

    pub fn metrics_snapshot(&self) -> FuseIoUringTransportMetricsSnapshot {
        FuseIoUringTransportMetricsSnapshot {
            queue_depth: self.entries.len().min(usize::from(u16::MAX)) as u16,
            register_commands: self.metrics.register_commands,
            commit_and_fetch_commands: self.metrics.commit_and_fetch_commands,
            cqe_completions: self.metrics.cqe_completions,
            cqe_errors: self.metrics.cqe_errors,
            submit_flushes: self.metrics.submit_flushes,
            submit_flush_total_ns: self.metrics.submit_flush_total_ns,
            submit_waits: self.metrics.submit_waits,
            submit_wait_total_ns: self.metrics.submit_wait_total_ns,
            cqe_drain_total_ns: self.metrics.cqe_drain_total_ns,
        }
    }

    #[cfg(test)]
    fn backend(&self) -> &B {
        &self.backend
    }

    #[cfg(test)]
    fn entry_mut_for_test(&mut self, index: usize) -> &mut FuseUringEntry {
        &mut self.entries[index]
    }

    #[cfg(test)]
    fn entry_for_test(&self, index: usize) -> &FuseUringEntry {
        &self.entries[index]
    }

    fn has_pending_commit_id(&self, unique: u64) -> bool {
        self.entries.iter().any(|entry| entry.commit_id() == unique)
    }
}

impl<B: FuseUringBackend> FuseIoUringTransport<B> {
    pub fn register_entries(&mut self) -> Result<(), TransportError> {
        for entry in &mut self.entries {
            let user_data = entry.user_data();
            let command = entry.register_command(self.fuse_fd);
            let sqe = command.build_sqe(user_data);
            self.backend.submit(sqe)?;
            self.metrics.register_commands = self.metrics.register_commands.saturating_add(1);
        }
        let started_at = Instant::now();
        self.backend.submit_pending()?;
        self.metrics.record_submit_flush(started_at);
        self.ready = true;
        Ok(())
    }

    pub fn receive_request_with_origin(
        &mut self,
    ) -> Result<Option<(FuseRequest, ReplyOrigin)>, TransportError> {
        let started_at = Instant::now();
        let Some(completion) = self.backend.next_completion()? else {
            self.metrics.record_cqe_drain(started_at);
            return Ok(None);
        };
        self.metrics.record_cqe_drain(started_at);
        self.metrics.cqe_completions = self.metrics.cqe_completions.saturating_add(1);
        if completion.result < 0 {
            self.metrics.cqe_errors = self.metrics.cqe_errors.saturating_add(1);
            return Err(io::Error::from_raw_os_error(-completion.result).into());
        }
        let index = completion
            .user_data
            .checked_sub(1)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid io_uring cqe user_data")
            })?;
        let entry = self.entries.get_mut(index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "io_uring cqe referenced an unknown FUSE ring entry",
            )
        })?;
        let generation = entry.next_generation();
        let origin = ReplyOrigin::IoUring {
            queue_id: entry.qid,
            generation,
        };
        let frame = entry.request_frame()?;
        FuseRequest::from_frame(&frame)
            .map(|request| Some((request, origin)))
            .map_err(TransportError::from)
    }

    pub fn send_reply_from_origin(
        &mut self,
        origin: ReplyOrigin,
        reply: FuseReply,
    ) -> Result<(), TransportError> {
        let ReplyOrigin::IoUring {
            queue_id,
            generation,
        } = origin
        else {
            return self.send_reply(reply);
        };
        let Some(entry) = self
            .entries
            .iter()
            .find(|entry| entry.qid == queue_id && entry.generation() == generation)
        else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "stale io_uring reply origin does not match an active ring entry generation",
            )
            .into());
        };
        if entry.commit_id() != reply.unique {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "io_uring reply origin matched a different FUSE request id",
            )
            .into());
        }
        self.send_reply(reply)
    }
}

impl<B: FuseUringBackend> FuseTransport for FuseIoUringTransport<B> {
    fn receive_request(&mut self) -> Result<Option<FuseRequest>, TransportError> {
        self.receive_request_with_origin()
            .map(|request| request.map(|(request, _origin)| request))
    }

    fn send_reply(&mut self, reply: FuseReply) -> Result<(), TransportError> {
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.commit_id() == reply.unique)
        else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no io_uring ring entry is waiting for this FUSE reply",
            )
            .into());
        };
        if reply.is_no_reply() {
            entry.header.ring_ent_in_out.payload_sz = 0;
        } else if let Some(header) = reply.local_cache_range_header() {
            let range = reply.local_cache_range().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "FUSE local-cache reply header is missing its range",
                )
            })?;
            let payload_len_u32 = range.len;
            let payload_len = payload_len_u32 as usize;
            if payload_len > entry.payload.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "FUSE io_uring local-cache reply exceeds registered entry buffer",
                )
                .into());
            }
            entry.header.in_out[..FUSE_OUT_HEADER_LEN].copy_from_slice(header);
            let mut output = &mut entry.payload[..payload_len];
            copy_local_cache_range_with_pread(range, &mut output)?;
            entry.header.ring_ent_in_out.payload_sz = payload_len_u32;
        } else {
            let bytes = reply.to_vec();
            if bytes.len() < FUSE_OUT_HEADER_LEN {
                return Err(AbiParseError::FrameTooShort {
                    expected: FUSE_OUT_HEADER_LEN,
                    actual: bytes.len(),
                }
                .into());
            }
            entry.header.in_out[..FUSE_OUT_HEADER_LEN]
                .copy_from_slice(&bytes[..FUSE_OUT_HEADER_LEN]);
            let payload = &bytes[FUSE_OUT_HEADER_LEN..];
            if payload.len() > entry.payload.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "FUSE io_uring reply payload exceeds registered entry buffer",
                )
                .into());
            }
            entry.payload[..payload.len()].copy_from_slice(payload);
            entry.header.ring_ent_in_out.payload_sz = payload.len() as u32;
        }
        let user_data = entry.user_data();
        let command = entry.commit_and_fetch_command(self.fuse_fd, reply.unique);
        let sqe = command.build_sqe(user_data);
        self.backend.submit(sqe)?;
        self.metrics.commit_and_fetch_commands =
            self.metrics.commit_and_fetch_commands.saturating_add(1);
        let started_at = Instant::now();
        self.backend.submit_pending()?;
        self.metrics.record_submit_flush(started_at);
        Ok(())
    }

    fn send_reply_from_origin(
        &mut self,
        origin: ReplyOrigin,
        reply: FuseReply,
    ) -> Result<(), TransportError> {
        FuseIoUringTransport::send_reply_from_origin(self, origin, reply)
    }
}

#[derive(Debug)]
pub struct RegisteredFuseIoUringTransport<T, B> {
    _io: T,
    inner: FuseIoUringTransport<B>,
}

impl<T: AsRawFd, B: FuseUringBackend> RegisteredFuseIoUringTransport<T, B> {
    pub fn new(
        io: T,
        queue_count: u16,
        payload_len: usize,
        backend: B,
    ) -> Result<Self, TransportError> {
        let mut inner =
            FuseIoUringTransport::with_backend(io.as_raw_fd(), queue_count, payload_len, backend);
        inner.register_entries()?;
        Ok(Self { _io: io, inner })
    }

    #[cfg(test)]
    fn inner_for_test(&self) -> &FuseIoUringTransport<B> {
        &self.inner
    }
}

impl<T, B: FuseUringBackend> FuseTransport for RegisteredFuseIoUringTransport<T, B> {
    fn receive_request(&mut self) -> Result<Option<FuseRequest>, TransportError> {
        self.inner.receive_request()
    }

    fn send_reply(&mut self, reply: FuseReply) -> Result<(), TransportError> {
        self.inner.send_reply(reply)
    }

    fn send_reply_from_origin(
        &mut self,
        origin: ReplyOrigin,
        reply: FuseReply,
    ) -> Result<(), TransportError> {
        self.inner.send_reply_from_origin(origin, reply)
    }

    fn request_shutdown(&mut self) -> Result<(), TransportError> {
        self.inner.request_shutdown()
    }
}

#[derive(Debug)]
pub struct HybridFuseTransport<C, B> {
    classic: C,
    io_uring: FuseIoUringTransport<B>,
    classic_fallbacks: u64,
    classic_receive_fallbacks: u64,
    classic_reply_fallbacks: u64,
    io_uring_runtime_error_fallbacks: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HybridFuseTransportMetricsSnapshot {
    /// Effective transport mode for this hybrid surface.
    pub mode: LinuxNativeTransportMode,
    /// io_uring-side counters and timing totals.
    pub io_uring: FuseIoUringTransportMetricsSnapshot,
    /// Number of receive/send operations that used the classic fallback path.
    pub classic_fallbacks: u64,
    /// Number of receive operations that used the classic fallback path.
    pub classic_receive_fallbacks: u64,
    /// Number of reply operations that used the classic fallback path.
    pub classic_reply_fallbacks: u64,
    /// Number of receive operations that fell back after an io_uring runtime error.
    pub io_uring_runtime_error_fallbacks: u64,
}

impl<C, B> HybridFuseTransport<C, B> {
    pub fn new(classic: C, io_uring: FuseIoUringTransport<B>) -> Self {
        Self {
            classic,
            io_uring,
            classic_fallbacks: 0,
            classic_receive_fallbacks: 0,
            classic_reply_fallbacks: 0,
            io_uring_runtime_error_fallbacks: 0,
        }
    }

    pub fn metrics_snapshot(&self) -> HybridFuseTransportMetricsSnapshot {
        HybridFuseTransportMetricsSnapshot {
            mode: LinuxNativeTransportMode::IoUring,
            io_uring: self.io_uring.metrics_snapshot(),
            classic_fallbacks: self.classic_fallbacks,
            classic_receive_fallbacks: self.classic_receive_fallbacks,
            classic_reply_fallbacks: self.classic_reply_fallbacks,
            io_uring_runtime_error_fallbacks: self.io_uring_runtime_error_fallbacks,
        }
    }

    #[cfg(test)]
    fn io_uring_for_test(&self) -> &FuseIoUringTransport<B> {
        &self.io_uring
    }
}

impl<C, B: FuseUringBackend> HybridFuseTransport<C, B> {
    pub fn register_io_uring(
        classic: C,
        fuse_fd: RawFd,
        queue_count: u16,
        payload_len: usize,
        backend: B,
    ) -> Result<Self, TransportError> {
        let mut io_uring =
            FuseIoUringTransport::with_backend(fuse_fd, queue_count, payload_len, backend);
        io_uring.register_entries()?;
        Ok(Self::new(classic, io_uring))
    }
}

impl<C> HybridFuseTransport<C, IoUringBackend> {
    pub fn register_kernel_io_uring(
        classic: C,
        fuse_fd: RawFd,
        queue_count: u16,
        payload_len: usize,
    ) -> Result<Self, TransportError> {
        Self::try_register_kernel_io_uring(classic, fuse_fd, queue_count, payload_len)
            .map_err(|(_classic, error)| error)
    }

    pub fn try_register_kernel_io_uring(
        classic: C,
        fuse_fd: RawFd,
        queue_count: u16,
        payload_len: usize,
    ) -> Result<Self, (C, TransportError)> {
        let backend = match IoUringBackend::new(u32::from(queue_count)) {
            Ok(backend) => backend,
            Err(error) => return Err((classic, TransportError::from(error))),
        };
        let mut io_uring =
            FuseIoUringTransport::with_backend(fuse_fd, queue_count, payload_len, backend);
        if let Err(error) = io_uring.register_entries() {
            return Err((classic, error));
        }
        Ok(Self::new(classic, io_uring))
    }
}

impl<C: FuseTransport, B: FuseUringBackend> HybridFuseTransport<C, B> {
    fn receive_classic_fallback(
        &mut self,
    ) -> Result<Option<(FuseRequest, ReplyOrigin)>, TransportError> {
        self.classic_fallbacks = self.classic_fallbacks.saturating_add(1);
        self.classic_receive_fallbacks = self.classic_receive_fallbacks.saturating_add(1);
        self.classic
            .receive_request()
            .map(|request| request.map(|request| (request, ReplyOrigin::Classic)))
    }

    pub fn receive_request_with_origin(
        &mut self,
    ) -> Result<Option<(FuseRequest, ReplyOrigin)>, TransportError> {
        match self.io_uring.receive_request_with_origin() {
            Ok(Some(request)) => return Ok(Some(request)),
            Ok(None) => {}
            Err(TransportError::Io(_)) => {
                self.io_uring_runtime_error_fallbacks =
                    self.io_uring_runtime_error_fallbacks.saturating_add(1);
                return self.receive_classic_fallback();
            }
            Err(error) => return Err(error),
        }
        let Some(io_uring_fd) = self.io_uring.backend.poll_fd() else {
            return self.receive_classic_fallback();
        };
        let Some(classic_fd) = self.classic.poll_fd() else {
            return self.receive_classic_fallback();
        };

        loop {
            match wait_for_hybrid_readiness(Some(classic_fd), Some(io_uring_fd))? {
                HybridReadiness::IoUring => {
                    if let Some(request) = self.io_uring.receive_request_with_origin()? {
                        return Ok(Some(request));
                    }
                }
                HybridReadiness::Classic => {
                    return self.receive_classic_fallback();
                }
                HybridReadiness::Both => {
                    if let Some(request) = self.io_uring.receive_request_with_origin()? {
                        return Ok(Some(request));
                    }
                    return self.receive_classic_fallback();
                }
            }
        }
    }
}

impl<C: FuseTransport, B: FuseUringBackend> FuseTransport for HybridFuseTransport<C, B> {
    fn receive_request(&mut self) -> Result<Option<FuseRequest>, TransportError> {
        self.receive_request_with_origin()
            .map(|request| request.map(|(request, _origin)| request))
    }

    fn send_reply(&mut self, reply: FuseReply) -> Result<(), TransportError> {
        if self.io_uring.has_pending_commit_id(reply.unique) {
            return self.io_uring.send_reply(reply);
        }
        self.classic_fallbacks = self.classic_fallbacks.saturating_add(1);
        self.classic_reply_fallbacks = self.classic_reply_fallbacks.saturating_add(1);
        self.classic.send_reply(reply)
    }

    fn send_reply_from_origin(
        &mut self,
        origin: ReplyOrigin,
        reply: FuseReply,
    ) -> Result<(), TransportError> {
        if matches!(origin, ReplyOrigin::IoUring { .. }) {
            return self.io_uring.send_reply_from_origin(origin, reply);
        }
        self.classic_fallbacks = self.classic_fallbacks.saturating_add(1);
        self.classic_reply_fallbacks = self.classic_reply_fallbacks.saturating_add(1);
        self.classic.send_reply_from_origin(origin, reply)
    }

    fn request_shutdown(&mut self) -> Result<(), TransportError> {
        self.io_uring.request_shutdown()?;
        self.classic.request_shutdown()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HybridReadiness {
    IoUring,
    Classic,
    Both,
}

fn wait_for_hybrid_readiness(
    classic_fd: Option<RawFd>,
    io_uring_fd: Option<RawFd>,
) -> io::Result<HybridReadiness> {
    let mut pollfds = [
        libc::pollfd {
            fd: classic_fd.unwrap_or(-1),
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: io_uring_fd.unwrap_or(-1),
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    let poll_len = match (classic_fd, io_uring_fd) {
        (Some(_), Some(_)) => 2,
        (Some(_), None) | (None, Some(_)) => 1,
        (None, None) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "hybrid transport has no pollable file descriptors",
            ));
        }
    };
    if classic_fd.is_none() {
        pollfds[0] = pollfds[1];
    }

    loop {
        // SAFETY: pollfds points to `poll_len` initialized pollfd entries.
        let result = unsafe { libc::poll(pollfds.as_mut_ptr(), poll_len, -1) };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }

        let classic_ready = classic_fd.is_some()
            && pollfds[0].revents & (libc::POLLIN | libc::POLLERR | libc::POLLHUP) != 0;
        let io_uring_index = usize::from(classic_fd.is_some());
        let io_uring_ready = io_uring_fd.is_some()
            && pollfds[io_uring_index].revents & (libc::POLLIN | libc::POLLERR | libc::POLLHUP)
                != 0;

        return match (classic_ready, io_uring_ready) {
            (true, true) => Ok(HybridReadiness::Both),
            (true, false) => Ok(HybridReadiness::Classic),
            (false, true) => Ok(HybridReadiness::IoUring),
            (false, false) => continue,
        };
    }
}

#[derive(Debug)]
pub enum LinuxNativeFuseTransport<C, B> {
    /// Classic `/dev/fuse` read/write transport.
    Classic(C),
    /// Hybrid FUSE-over-io_uring transport with classic fallback.
    Hybrid(HybridFuseTransport<C, B>),
}

impl<C, B> LinuxNativeFuseTransport<C, B> {
    pub fn metrics_snapshot(&self) -> HybridFuseTransportMetricsSnapshot {
        match self {
            Self::Classic(_) => HybridFuseTransportMetricsSnapshot {
                mode: LinuxNativeTransportMode::Classic,
                io_uring: FuseIoUringTransportMetricsSnapshot::default(),
                classic_fallbacks: 0,
                classic_receive_fallbacks: 0,
                classic_reply_fallbacks: 0,
                io_uring_runtime_error_fallbacks: 0,
            },
            Self::Hybrid(transport) => transport.metrics_snapshot(),
        }
    }
}

impl<C: FuseTransport, B: FuseUringBackend> FuseTransport for LinuxNativeFuseTransport<C, B> {
    fn receive_request(&mut self) -> Result<Option<FuseRequest>, TransportError> {
        match self {
            Self::Classic(transport) => transport.receive_request(),
            Self::Hybrid(transport) => transport.receive_request(),
        }
    }

    fn send_reply(&mut self, reply: FuseReply) -> Result<(), TransportError> {
        match self {
            Self::Classic(transport) => transport.send_reply(reply),
            Self::Hybrid(transport) => transport.send_reply(reply),
        }
    }

    fn send_reply_from_origin(
        &mut self,
        origin: ReplyOrigin,
        reply: FuseReply,
    ) -> Result<(), TransportError> {
        match self {
            Self::Classic(transport) => transport.send_reply_from_origin(origin, reply),
            Self::Hybrid(transport) => transport.send_reply_from_origin(origin, reply),
        }
    }

    fn request_shutdown(&mut self) -> Result<(), TransportError> {
        match self {
            Self::Classic(transport) => transport.request_shutdown(),
            Self::Hybrid(transport) => transport.request_shutdown(),
        }
    }
}

thread_local! {
    static SPLICE_PIPE: RefCell<Option<SplicePipe>> = const { RefCell::new(None) };
    static PREAD_COPY_BUFFER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    #[cfg(test)]
    static SPLICE_PIPE_CREATE_COUNT: RefCell<u64> = const { RefCell::new(0) };
    #[cfg(test)]
    static PREAD_COPY_BUFFER_CREATE_COUNT: RefCell<u64> = const { RefCell::new(0) };
}

#[derive(Debug)]
struct SplicePipe {
    read: OwnedFd,
    write: OwnedFd,
}

/// Local cache read reply strategy selected by a native Linux transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalCacheReplyStrategy {
    /// Copy from the local cache fd with `pread` and write bytes to the FUSE fd.
    PreadBytes,
    /// Use Linux `splice(file_fd -> pipe -> fuse_fd)` when both endpoints are fds.
    Splice,
}

#[derive(Debug, Default)]
struct LocalCacheReplyMetrics {
    pread_bytes_replies: AtomicU64,
    splice_replies: AtomicU64,
    payload_bytes: AtomicU64,
}

/// Requested Linux-native FUSE transport backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinuxNativeTransportSelection {
    /// Always use classic blocking `/dev/fuse` read/write transport.
    Classic,
    /// Prefer FUSE-over-io_uring when capability probing says it is safe.
    IoUring,
    /// Try FUSE-over-io_uring, then fall back to classic transport.
    Auto,
}

/// Effective Linux-native FUSE transport backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinuxNativeTransportMode {
    /// Classic blocking `/dev/fuse` read/write transport.
    Classic,
    /// FUSE-over-io_uring transport.
    IoUring,
}

/// Capability probe inputs for FUSE-over-io_uring transport selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoUringTransportProbe {
    /// The running kernel exposes the evolving FUSE-over-io_uring ABI.
    pub kernel_supported: bool,
    /// The FUSE module has io_uring transport enabled.
    pub fuse_module_enabled: bool,
    /// FUSE init capability negotiation advertised `FUSE_CAP_OVER_IO_URING`.
    pub fuse_cap_over_io_uring: bool,
    /// Configured submission/completion queue depth for the io_uring backend.
    pub queue_depth: Option<usize>,
    /// System security policy permits this process to use io_uring.
    pub restricted_by_policy: bool,
}

/// Reason an io_uring transport request resolved to the classic backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoUringFallbackReason {
    /// Caller explicitly selected classic transport.
    ClassicRequested,
    /// The running kernel does not expose the FUSE-over-io_uring ABI.
    KernelUnsupported,
    /// The FUSE module has io_uring transport disabled.
    FuseModuleDisabled,
    /// FUSE capability negotiation did not advertise io_uring transport.
    CapabilityMissing,
    /// No valid io_uring queue depth was configured.
    QueueDepthUnavailable,
    /// System security policy prevents io_uring use by this process.
    RestrictedByPolicy,
    /// The request class is not supported by the current FUSE-over-io_uring ABI.
    UnsupportedRequestClass,
}

/// Effective transport decision exposed to mount diagnostics and telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinuxNativeTransportDecision {
    /// The requested transport selection.
    pub requested: LinuxNativeTransportSelection,
    /// The backend StudioFS should use for this session.
    pub mode: LinuxNativeTransportMode,
    /// Queue depth used by the io_uring backend when active.
    pub io_uring_queue_depth: Option<usize>,
    /// Reason an io_uring-capable selection fell back to classic transport.
    pub fallback_reason: Option<IoUringFallbackReason>,
}

impl LinuxNativeTransportDecision {
    /// Builds a classic decision with a fallback reason for tests and probes.
    pub fn classic_with_fallback(reason: IoUringFallbackReason) -> Self {
        Self {
            requested: LinuxNativeTransportSelection::Auto,
            mode: LinuxNativeTransportMode::Classic,
            io_uring_queue_depth: None,
            fallback_reason: Some(reason),
        }
    }
}

/// Resolves requested Linux-native transport selection against io_uring support.
pub fn resolve_linux_native_transport(
    selection: LinuxNativeTransportSelection,
    probe: IoUringTransportProbe,
) -> LinuxNativeTransportDecision {
    if selection == LinuxNativeTransportSelection::Classic {
        return LinuxNativeTransportDecision {
            requested: selection,
            mode: LinuxNativeTransportMode::Classic,
            io_uring_queue_depth: None,
            fallback_reason: Some(IoUringFallbackReason::ClassicRequested),
        };
    }

    let fallback_reason = if !probe.kernel_supported {
        Some(IoUringFallbackReason::KernelUnsupported)
    } else if !probe.fuse_module_enabled {
        Some(IoUringFallbackReason::FuseModuleDisabled)
    } else if !probe.fuse_cap_over_io_uring {
        Some(IoUringFallbackReason::CapabilityMissing)
    } else if probe.restricted_by_policy {
        Some(IoUringFallbackReason::RestrictedByPolicy)
    } else if probe.queue_depth.is_none_or(|queue_depth| queue_depth == 0) {
        Some(IoUringFallbackReason::QueueDepthUnavailable)
    } else {
        None
    };

    if let Some(fallback_reason) = fallback_reason {
        return LinuxNativeTransportDecision {
            requested: selection,
            mode: LinuxNativeTransportMode::Classic,
            io_uring_queue_depth: None,
            fallback_reason: Some(fallback_reason),
        };
    }

    LinuxNativeTransportDecision {
        requested: selection,
        mode: LinuxNativeTransportMode::IoUring,
        io_uring_queue_depth: probe.queue_depth,
        fallback_reason: None,
    }
}

/// Per-request backend route selected after session-level transport resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinuxNativeRequestRoute {
    /// Backend that should handle this request.
    pub mode: LinuxNativeTransportMode,
    /// Reason this request fell back from io_uring to classic transport.
    pub fallback_reason: Option<IoUringFallbackReason>,
    /// Number of fallback events represented by this route.
    pub fallback_count: u64,
}

impl LinuxNativeRequestRoute {
    /// Builds a classic fallback route for unsupported io_uring request classes.
    pub fn classic_fallback(reason: IoUringFallbackReason) -> Self {
        Self {
            mode: LinuxNativeTransportMode::Classic,
            fallback_reason: Some(reason),
            fallback_count: 1,
        }
    }
}

/// Routes one FUSE request through the active Linux-native transport backend.
pub fn route_linux_native_request(
    active_mode: LinuxNativeTransportMode,
    opcode: FuseOpcode,
) -> LinuxNativeRequestRoute {
    if active_mode == LinuxNativeTransportMode::IoUring && io_uring_unsupported_opcode(opcode) {
        return LinuxNativeRequestRoute::classic_fallback(
            IoUringFallbackReason::UnsupportedRequestClass,
        );
    }

    LinuxNativeRequestRoute {
        mode: active_mode,
        fallback_reason: None,
        fallback_count: 0,
    }
}

fn io_uring_unsupported_opcode(opcode: FuseOpcode) -> bool {
    matches!(opcode, FuseOpcode::Other(_))
}

fn elapsed_transport_latency_ns(started_at: Instant) -> u64 {
    started_at.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Snapshot of native transport local-cache read reply counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LocalCacheReplyMetricsSnapshot {
    /// Replies served by `pread -> bytes -> write`.
    pub pread_bytes_replies: u64,
    /// Replies served by `splice(file_fd -> pipe -> fuse_fd)`.
    pub splice_replies: u64,
    /// Payload bytes copied or spliced from a local cache fd.
    pub payload_bytes: u64,
}

impl LocalCacheReplyMetrics {
    fn record(&self, strategy: LocalCacheReplyStrategy, payload_bytes: u32) {
        match strategy {
            LocalCacheReplyStrategy::PreadBytes => {
                self.pread_bytes_replies.fetch_add(1, Ordering::Relaxed);
            }
            LocalCacheReplyStrategy::Splice => {
                self.splice_replies.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.payload_bytes
            .fetch_add(u64::from(payload_bytes), Ordering::Relaxed);
    }

    fn snapshot(&self) -> LocalCacheReplyMetricsSnapshot {
        LocalCacheReplyMetricsSnapshot {
            pread_bytes_replies: self.pread_bytes_replies.load(Ordering::Relaxed),
            splice_replies: self.splice_replies.load(Ordering::Relaxed),
            payload_bytes: self.payload_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Error)]
/// Errors returned by FUSE transport implementations.
pub enum TransportError {
    /// A raw frame could not be decoded according to the FUSE ABI.
    #[error(transparent)]
    Abi(#[from] AbiParseError),
    /// The underlying file descriptor or stream returned an I/O error.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Boundary between typed FUSE requests and a concrete I/O backend.
///
/// Implementations can use classic blocking `read(2)`/`write(2)`, in-memory
/// frames for tests, or a future io_uring backend without changing dispatcher
/// logic.
pub trait FuseTransport {
    /// Receives the next request, or `Ok(None)` when the session ended cleanly.
    fn receive_request(&mut self) -> Result<Option<FuseRequest>, TransportError>;
    /// Sends one reply frame back to the kernel.
    fn send_reply(&mut self, reply: FuseReply) -> Result<(), TransportError>;
    /// Sends one reply frame back to the transport origin that produced it.
    fn send_reply_from_origin(
        &mut self,
        _origin: ReplyOrigin,
        reply: FuseReply,
    ) -> Result<(), TransportError> {
        self.send_reply(reply)
    }
    /// Requests a local shutdown so future receives stop without an I/O error.
    fn request_shutdown(&mut self) -> Result<(), TransportError> {
        Ok(())
    }
    /// Returns a pollable fd for multiplexing transports, when available.
    fn poll_fd(&self) -> Option<RawFd> {
        None
    }
}

#[derive(Debug)]
struct TransportReplySinkState<T> {
    transport: T,
    errors: Vec<TransportError>,
    dropped: Vec<(ReplyOrigin, u64)>,
}

/// Reply sink that commits async completions back through a concrete transport.
#[derive(Debug)]
pub struct TransportReplySink<T> {
    state: Arc<Mutex<TransportReplySinkState<T>>>,
}

impl<T> TransportReplySink<T> {
    /// Builds a reply sink around an owned transport.
    pub fn new(transport: T) -> Self {
        Self {
            state: Arc::new(Mutex::new(TransportReplySinkState {
                transport,
                errors: Vec::new(),
                dropped: Vec::new(),
            })),
        }
    }

    /// Recovers the owned transport when no reply tokens still hold this sink.
    pub fn try_into_inner(self) -> Result<T, Self> {
        match Arc::try_unwrap(self.state) {
            Ok(state) => Ok(state
                .into_inner()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .transport),
            Err(state) => Err(Self { state }),
        }
    }

    /// Returns how many transport send errors have been observed.
    pub fn send_error_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .errors
            .len()
    }

    /// Drains send errors observed while committing async replies.
    pub fn take_send_errors(&self) -> Vec<TransportError> {
        std::mem::take(
            &mut self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .errors,
        )
    }

    /// Returns reply origins and ids that were intentionally dropped.
    pub fn dropped_replies(&self) -> Vec<(ReplyOrigin, u64)> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .dropped
            .clone()
    }
}

impl<T> Clone for TransportReplySink<T> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<T> ReplySink for TransportReplySink<T>
where
    T: FuseTransport + Send + 'static,
{
    fn complete(&self, origin: ReplyOrigin, reply: FuseReply) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Err(error) = state.transport.send_reply_from_origin(origin, reply) {
            state.errors.push(error);
        }
    }

    fn dropped(&self, origin: ReplyOrigin, unique: u64) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .dropped
            .push((origin, unique));
    }
}

#[derive(Debug, Default)]
/// In-memory transport used by protocol and dispatcher tests.
pub struct FrameQueueTransport {
    inbound: VecDeque<Vec<u8>>,
    sent_replies: Vec<FuseReply>,
    shutdown_requested: bool,
}

impl FrameQueueTransport {
    /// Creates a queue transport from raw inbound FUSE frames.
    pub fn new(inbound: impl IntoIterator<Item = Vec<u8>>) -> Self {
        Self {
            inbound: inbound.into_iter().collect(),
            sent_replies: Vec::new(),
            shutdown_requested: false,
        }
    }

    /// Returns every reply sent through this test transport.
    pub fn sent_replies(&self) -> &[FuseReply] {
        &self.sent_replies
    }
}

impl FuseTransport for FrameQueueTransport {
    fn receive_request(&mut self) -> Result<Option<FuseRequest>, TransportError> {
        if self.shutdown_requested {
            return Ok(None);
        }
        self.inbound
            .pop_front()
            .map(|frame| FuseRequest::from_frame(&frame).map_err(TransportError::from))
            .transpose()
    }

    fn send_reply(&mut self, reply: FuseReply) -> Result<(), TransportError> {
        if reply.is_no_reply() {
            return Ok(());
        }
        self.sent_replies.push(reply);
        Ok(())
    }

    fn request_shutdown(&mut self) -> Result<(), TransportError> {
        self.shutdown_requested = true;
        Ok(())
    }
}

#[derive(Debug)]
/// Classic blocking read/write transport for a FUSE device descriptor.
///
/// The kernel expects request reads to use a sufficiently large buffer. This
/// transport owns and reuses that buffer so sessions do not allocate per frame.
pub struct ClassicReadWriteTransport<T> {
    io: T,
    read_buffer: Vec<u8>,
    local_cache_pread_frame: Vec<u8>,
    shutdown_requested: bool,
    local_cache_strategy: LocalCacheReplyStrategy,
    local_cache_output_fd: Option<RawFd>,
    transport_fd: Option<RawFd>,
    local_cache_metrics: Arc<LocalCacheReplyMetrics>,
}

impl<T> ClassicReadWriteTransport<T> {
    /// Wraps an I/O object using Linux's default 16 MiB `max_write` frame size.
    pub fn new(io: T) -> Self {
        Self::with_max_write(io, DEFAULT_CLASSIC_MAX_WRITE)
    }

    /// Wraps an I/O object sized for a negotiated or configured `max_write`.
    pub fn with_max_write(io: T, max_write: usize) -> Self {
        Self {
            io,
            read_buffer: vec![0; FUSE_IN_HEADER_LEN + max_write],
            local_cache_pread_frame: Vec::new(),
            shutdown_requested: false,
            local_cache_strategy: LocalCacheReplyStrategy::PreadBytes,
            local_cache_output_fd: None,
            transport_fd: None,
            local_cache_metrics: Arc::new(LocalCacheReplyMetrics::default()),
        }
    }

    /// Records the input transport fd so hybrid transports can poll it.
    pub fn with_transport_fd(mut self, transport_fd: RawFd) -> Self {
        self.transport_fd = Some(transport_fd);
        self
    }

    /// Wraps an I/O object and records its raw output fd for splice replies.
    pub fn with_output_fd(io: T, output_fd: RawFd) -> Self {
        let mut transport = Self::new(io);
        transport.local_cache_output_fd = Some(output_fd);
        transport
    }

    /// Selects the local-cache read reply strategy for this transport.
    pub fn with_local_cache_strategy(mut self, strategy: LocalCacheReplyStrategy) -> Self {
        self.local_cache_strategy = strategy;
        self
    }

    /// Snapshot transport-local read reply counters.
    pub fn local_cache_reply_metrics_snapshot(&self) -> LocalCacheReplyMetricsSnapshot {
        self.local_cache_metrics.snapshot()
    }

    /// Consumes the transport and returns the wrapped I/O object.
    pub fn into_inner(self) -> T {
        self.io
    }
}

impl<T: Read + Write> FuseTransport for ClassicReadWriteTransport<T> {
    fn receive_request(&mut self) -> Result<Option<FuseRequest>, TransportError> {
        if self.shutdown_requested {
            return Ok(None);
        }

        let bytes_read = loop {
            match self.io.read(&mut self.read_buffer) {
                Ok(bytes_read) => break bytes_read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if fuse_teardown_read_error(&error) => return Ok(None),
                Err(error) => return Err(error.into()),
            }
        };
        if bytes_read == 0 {
            return Ok(None);
        }
        if bytes_read < FUSE_IN_HEADER_LEN {
            return Err(AbiParseError::FrameTooShort {
                expected: FUSE_IN_HEADER_LEN,
                actual: bytes_read,
            }
            .into());
        }

        let frame_len = u32::from_le_bytes([
            self.read_buffer[0],
            self.read_buffer[1],
            self.read_buffer[2],
            self.read_buffer[3],
        ]) as usize;
        if frame_len < FUSE_IN_HEADER_LEN {
            return Err(AbiParseError::FrameTooShort {
                expected: FUSE_IN_HEADER_LEN,
                actual: frame_len,
            }
            .into());
        }
        if frame_len > bytes_read {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "classic FUSE read returned a partial request frame",
            )
            .into());
        }

        FuseRequest::from_frame(&self.read_buffer[..frame_len])
            .map(Some)
            .map_err(TransportError::from)
    }

    fn send_reply(&mut self, reply: FuseReply) -> Result<(), TransportError> {
        if reply.is_no_reply() {
            return Ok(());
        }
        if let Some(range) = reply.local_cache_range() {
            let header = reply.local_cache_range_header().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "local cache range reply is missing its output header",
                )
            })?;
            let source_fd = reply.local_cache_source_fd().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "local cache range reply is missing its source fd",
                )
            })?;
            let owned_range = FuseLocalCacheReadRange::borrowed(
                source_fd,
                range.file_offset,
                range.len,
                range.content_epoch,
            );
            match self.local_cache_strategy {
                LocalCacheReplyStrategy::PreadBytes => {
                    let frame_len =
                        header
                            .len()
                            .checked_add(range.len as usize)
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "local cache reply frame length overflows usize",
                                )
                            })?;
                    self.local_cache_pread_frame.clear();
                    self.local_cache_pread_frame.reserve(frame_len);
                    self.local_cache_pread_frame.extend_from_slice(header);
                    copy_local_cache_range_with_pread(
                        owned_range,
                        &mut self.local_cache_pread_frame,
                    )?;
                    self.io.write_all(&self.local_cache_pread_frame)?;
                }
                LocalCacheReplyStrategy::Splice => {
                    let Some(output_fd) = self.local_cache_output_fd else {
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "splice local-cache replies require a raw FUSE fd transport",
                        )
                        .into());
                    };
                    self.io.write_all(header)?;
                    splice_local_cache_range_to_fd(owned_range, output_fd)?;
                }
            }
            self.local_cache_metrics
                .record(self.local_cache_strategy, range.len);
            return Ok(());
        }
        let io_slices = reply.io_slices();
        let slices = io_slices.as_slice();
        if slices.len() == 1 {
            self.io.write_all(slices[0].as_ref())?;
        } else {
            let mut written = 0;
            let expected: usize = slices.iter().map(|slice| slice.len()).sum();
            while written < expected {
                let pending = pending_reply_slices(slices, written);
                let bytes = self.io.write_vectored(pending.as_slice())?;
                if bytes == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "classic FUSE write accepted zero reply bytes",
                    )
                    .into());
                }
                written += bytes;
            }
        }
        Ok(())
    }

    fn request_shutdown(&mut self) -> Result<(), TransportError> {
        self.shutdown_requested = true;
        Ok(())
    }

    fn poll_fd(&self) -> Option<RawFd> {
        self.transport_fd
    }
}

fn copy_local_cache_range_with_pread<W: Write>(
    range: FuseLocalCacheReadRange,
    writer: &mut W,
) -> io::Result<()> {
    let mut remaining = usize::try_from(range.len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "local cache read range length exceeds usize",
        )
    })?;
    let mut offset = i64::try_from(range.file_offset).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "local cache read range offset exceeds off_t",
        )
    })?;

    with_thread_local_pread_copy_buffer(|buffer| {
        while remaining > 0 {
            let request_len = remaining.min(buffer.len());
            let read = loop {
                // SAFETY: `buffer` points to writable memory of `request_len`
                // bytes, `range.fd` is supplied by the VFS as an open stable local
                // cache file descriptor, and `pread` does not mutate the shared fd
                // offset.
                let result = unsafe {
                    libc::pread(
                        range.fd,
                        buffer.as_mut_ptr().cast(),
                        request_len,
                        offset as libc::off_t,
                    )
                };
                if result < 0 {
                    let error = io::Error::last_os_error();
                    if error.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(error);
                }
                break usize::try_from(result).map_err(|_| {
                    io::Error::other("local cache read returned an invalid byte count")
                })?;
            };
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "local cache range ended before requested reply length",
                ));
            }
            writer.write_all(&buffer[..read])?;
            remaining -= read;
            offset =
                offset
                    .checked_add(i64::try_from(read).map_err(|_| {
                        io::Error::other("local cache read length does not fit off_t")
                    })?)
                    .ok_or_else(|| io::Error::other("local cache read offset overflowed"))?;
        }

        Ok(())
    })
}

fn with_thread_local_pread_copy_buffer<R>(
    f: impl FnOnce(&mut [u8]) -> io::Result<R>,
) -> io::Result<R> {
    PREAD_COPY_BUFFER.with(|buffer| {
        let mut buffer = buffer.borrow_mut();
        if buffer.len() != PREAD_COPY_BUFFER_LEN {
            if buffer.capacity() < PREAD_COPY_BUFFER_LEN {
                #[cfg(test)]
                PREAD_COPY_BUFFER_CREATE_COUNT.with(|count| {
                    *count.borrow_mut() += 1;
                });
            }
            buffer.resize(PREAD_COPY_BUFFER_LEN, 0);
        }
        f(buffer.as_mut_slice())
    })
}

#[cfg(test)]
fn reset_pread_copy_buffer_for_test() {
    PREAD_COPY_BUFFER.with(|buffer| {
        *buffer.borrow_mut() = Vec::new();
    });
    PREAD_COPY_BUFFER_CREATE_COUNT.with(|count| {
        *count.borrow_mut() = 0;
    });
}

#[cfg(test)]
fn pread_copy_buffer_create_count_for_test() -> u64 {
    PREAD_COPY_BUFFER_CREATE_COUNT.with(|count| *count.borrow())
}

/// Splices a stable local cache range into a raw output fd through a pipe.
///
/// This is the Linux fd-level building block for `splice(file_fd -> pipe ->
/// fuse_fd)` read replies. Generic test transports use the `pread` bytes
/// strategy because they do not expose a raw FUSE fd.
pub fn splice_local_cache_range_to_fd(
    range: FuseLocalCacheReadRange,
    output_fd: RawFd,
) -> io::Result<()> {
    let result = with_thread_local_splice_pipe(|pipe| {
        splice_local_cache_range_with_pipe(range, output_fd, pipe)
    });
    if result.is_err() {
        discard_thread_local_splice_pipe();
    }
    result
}

fn with_thread_local_splice_pipe<R>(f: impl FnOnce(&SplicePipe) -> io::Result<R>) -> io::Result<R> {
    SPLICE_PIPE.with(|pipe| {
        let mut pipe = pipe.borrow_mut();
        if pipe.is_none() {
            *pipe = Some(create_splice_pipe()?);
        }
        let pipe = pipe
            .as_ref()
            .ok_or_else(|| io::Error::other("thread-local splice pipe was not initialized"))?;
        f(pipe)
    })
}

fn create_splice_pipe() -> io::Result<SplicePipe> {
    let mut pipe_fds = [-1; 2];
    // SAFETY: `pipe_fds` points to storage for exactly two descriptors.
    let pipe_result = unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) };
    if pipe_result < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `pipe2` initialized both fds on success and ownership moves into
    // `OwnedFd` so they are closed on every return path.
    let pipe_read = unsafe { OwnedFd::from_raw_fd(pipe_fds[0]) };
    // SAFETY: see `pipe_read`.
    let pipe_write = unsafe { OwnedFd::from_raw_fd(pipe_fds[1]) };
    #[cfg(test)]
    SPLICE_PIPE_CREATE_COUNT.with(|count| {
        *count.borrow_mut() += 1;
    });
    Ok(SplicePipe {
        read: pipe_read,
        write: pipe_write,
    })
}

fn splice_local_cache_range_with_pipe(
    range: FuseLocalCacheReadRange,
    output_fd: RawFd,
    pipe: &SplicePipe,
) -> io::Result<()> {
    let mut remaining = usize::try_from(range.len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "local cache read range length exceeds usize",
        )
    })?;
    let mut source_offset = libc::off_t::try_from(range.file_offset).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "local cache read range offset exceeds off_t",
        )
    })?;

    // FRAMING INVARIANT: `send_reply` has already written a FUSE header whose
    // payload length is `range.len`. Upstream local-cache range builders must
    // clamp `range.len` to bytes actually available from this fd; otherwise an
    // early EOF here means the kernel has received a short framed reply.
    while remaining > 0 {
        let moved_into_pipe = splice_once(
            range.fd,
            Some(&mut source_offset),
            pipe.write.as_raw_fd(),
            None,
            remaining,
        )?;
        if moved_into_pipe == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "local cache range ended before requested splice length",
            ));
        }

        let mut pipe_remaining = moved_into_pipe;
        while pipe_remaining > 0 {
            let moved_out =
                splice_once(pipe.read.as_raw_fd(), None, output_fd, None, pipe_remaining)?;
            if moved_out == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "splice accepted zero bytes into output fd",
                ));
            }
            pipe_remaining -= moved_out;
        }

        remaining -= moved_into_pipe;
    }

    Ok(())
}

#[cfg(test)]
fn reset_splice_pipe_pool_for_test() {
    discard_thread_local_splice_pipe();
    SPLICE_PIPE_CREATE_COUNT.with(|count| {
        *count.borrow_mut() = 0;
    });
}

fn discard_thread_local_splice_pipe() {
    SPLICE_PIPE.with(|pipe| {
        *pipe.borrow_mut() = None;
    });
}

#[cfg(test)]
fn splice_pipe_create_count_for_test() -> u64 {
    SPLICE_PIPE_CREATE_COUNT.with(|count| *count.borrow())
}

fn splice_once(
    input_fd: RawFd,
    input_offset: Option<&mut libc::off_t>,
    output_fd: RawFd,
    output_offset: Option<&mut libc::off_t>,
    len: usize,
) -> io::Result<usize> {
    let input_offset_ptr = input_offset
        .map(|offset| offset as *mut libc::off_t)
        .unwrap_or(std::ptr::null_mut());
    let output_offset_ptr = output_offset
        .map(|offset| offset as *mut libc::off_t)
        .unwrap_or(std::ptr::null_mut());
    let request_len = len.min(SPLICE_PIPE_CHUNK_LEN);

    loop {
        // SAFETY: fds are caller-provided open descriptors, optional offsets
        // either point to valid `off_t` storage for this call or are null, and
        // splice owns no Rust references to transferred bytes.
        let result = unsafe {
            libc::splice(
                input_fd,
                input_offset_ptr,
                output_fd,
                output_offset_ptr,
                request_len,
                0,
            )
        };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        return usize::try_from(result)
            .map_err(|_| io::Error::other("splice returned an invalid byte count"));
    }
}

struct PendingReplySlices<'a> {
    slices: [io::IoSlice<'a>; 2],
    len: usize,
}

impl<'a> PendingReplySlices<'a> {
    fn as_slice(&self) -> &[io::IoSlice<'a>] {
        &self.slices[..self.len]
    }
}

fn pending_reply_slices<'a>(
    slices: &'a [io::IoSlice<'a>],
    mut offset: usize,
) -> PendingReplySlices<'a> {
    debug_assert!(
        slices.len() <= 2,
        "pending_reply_slices only supports up to 2 slices"
    );
    let mut pending = [io::IoSlice::new(&[]), io::IoSlice::new(&[])];
    let mut len = 0;
    for slice in slices {
        if offset >= slice.len() {
            offset -= slice.len();
            continue;
        }
        pending[len] = io::IoSlice::new(&slice.as_ref()[offset..]);
        len += 1;
        offset = 0;
    }
    PendingReplySlices {
        slices: pending,
        len,
    }
}

fn fuse_teardown_read_error(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(errno) if errno == libc::ENODEV || errno == libc::EAGAIN)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::abi::{FUSE_IN_HEADER_LEN, error_reply};
    use crate::native::protocol::{FuseLocalCacheReadRange, FuseOpcode};
    use std::io::Seek;
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;

    #[repr(C)]
    struct IoUringSqePrefixForTest {
        opcode: u8,
        flags: u8,
        ioprio: u16,
        fd: i32,
        off_or_cmd_op: u64,
        addr: u64,
        len: u32,
    }

    fn uring_cmd_sqe_addr_for_test(sqe: &io_uring::squeue::Entry128) -> u64 {
        // SAFETY: `io_uring::squeue::Entry128` is repr(C) and starts with a
        // 64-byte SQE whose first fields follow the Linux io_uring UAPI.
        unsafe { (*(sqe as *const _ as *const IoUringSqePrefixForTest)).addr }
    }

    fn uring_cmd_sqe_len_for_test(sqe: &io_uring::squeue::Entry128) -> u32 {
        // SAFETY: `io_uring::squeue::Entry128` is repr(C) and starts with a
        // 64-byte SQE whose first fields follow the Linux io_uring UAPI.
        unsafe { (*(sqe as *const _ as *const IoUringSqePrefixForTest)).len }
    }

    #[derive(Debug, Default)]
    struct RecordingUringBackend {
        submitted: Vec<io_uring::squeue::Entry128>,
        completions: VecDeque<FuseIoUringCompletion>,
        submit_flushes: usize,
        submit_waits: Vec<usize>,
    }

    impl RecordingUringBackend {
        fn submitted_ops(&self) -> Vec<u32> {
            vec![1; self.submitted.len()]
        }

        fn submitted_len(&self) -> usize {
            self.submitted.len()
        }

        fn submit_flushes(&self) -> usize {
            self.submit_flushes
        }

        fn submit_waits(&self) -> &[usize] {
            &self.submit_waits
        }

        fn push_completion(&mut self, completion: FuseIoUringCompletion) {
            self.completions.push_back(completion);
        }
    }

    impl FuseUringBackend for RecordingUringBackend {
        fn submit(&mut self, entry: io_uring::squeue::Entry128) -> io::Result<()> {
            self.submitted.push(entry);
            Ok(())
        }

        fn submit_pending(&mut self) -> io::Result<usize> {
            self.submit_flushes = self.submit_flushes.saturating_add(1);
            Ok(self.submitted.len())
        }

        fn submit_and_wait(&mut self, wanted: usize) -> io::Result<usize> {
            self.submit_waits.push(wanted);
            Ok(self.submitted.len())
        }

        fn next_completion(&mut self) -> io::Result<Option<FuseIoUringCompletion>> {
            Ok(self.completions.pop_front())
        }
    }

    #[derive(Debug)]
    struct DelayedReadyUringBackend {
        poll_read: OwnedFd,
        _poll_write: OwnedFd,
        first_completion_poll: bool,
        completion: FuseIoUringCompletion,
    }

    impl DelayedReadyUringBackend {
        fn new(completion: FuseIoUringCompletion) -> Self {
            let (poll_read, poll_write) = pipe_pair_for_test();
            write_one_byte_for_test(poll_write.as_raw_fd());
            Self {
                poll_read,
                _poll_write: poll_write,
                first_completion_poll: true,
                completion,
            }
        }
    }

    impl FuseUringBackend for DelayedReadyUringBackend {
        fn submit(&mut self, _entry: io_uring::squeue::Entry128) -> io::Result<()> {
            Ok(())
        }

        fn submit_pending(&mut self) -> io::Result<usize> {
            Ok(1)
        }

        fn submit_and_wait(&mut self, wanted: usize) -> io::Result<usize> {
            Ok(wanted)
        }

        fn next_completion(&mut self) -> io::Result<Option<FuseIoUringCompletion>> {
            if self.first_completion_poll {
                self.first_completion_poll = false;
                return Ok(None);
            }
            Ok(Some(self.completion))
        }

        fn poll_fd(&self) -> Option<RawFd> {
            Some(self.poll_read.as_raw_fd())
        }
    }

    #[derive(Debug)]
    struct FailingReceiveUringBackend;

    impl FuseUringBackend for FailingReceiveUringBackend {
        fn submit(&mut self, _entry: io_uring::squeue::Entry128) -> io::Result<()> {
            Ok(())
        }

        fn submit_pending(&mut self) -> io::Result<usize> {
            Ok(0)
        }

        fn submit_and_wait(&mut self, wanted: usize) -> io::Result<usize> {
            Ok(wanted)
        }

        fn next_completion(&mut self) -> io::Result<Option<FuseIoUringCompletion>> {
            Err(io::Error::from_raw_os_error(libc::EIO))
        }
    }

    struct PanicClassicTransport {
        poll_read: OwnedFd,
        _poll_write: OwnedFd,
    }

    impl PanicClassicTransport {
        fn new_not_ready() -> Self {
            let (poll_read, poll_write) = pipe_pair_for_test();
            Self {
                poll_read,
                _poll_write: poll_write,
            }
        }
    }

    impl FuseTransport for PanicClassicTransport {
        fn receive_request(&mut self) -> Result<Option<FuseRequest>, TransportError> {
            panic!("classic transport should not be read when only io_uring fd is ready")
        }

        fn send_reply(&mut self, _reply: FuseReply) -> Result<(), TransportError> {
            Ok(())
        }

        fn poll_fd(&self) -> Option<RawFd> {
            Some(self.poll_read.as_raw_fd())
        }
    }

    #[derive(Debug)]
    struct EioReplyTransport;

    impl FuseTransport for EioReplyTransport {
        fn receive_request(&mut self) -> Result<Option<FuseRequest>, TransportError> {
            Ok(None)
        }

        fn send_reply(&mut self, _reply: FuseReply) -> Result<(), TransportError> {
            Err(io::Error::from_raw_os_error(libc::EIO).into())
        }
    }

    #[derive(Clone)]
    struct TransportNoReplyAsyncHandler<S> {
        started: std::sync::mpsc::Sender<u64>,
        _sink: std::marker::PhantomData<S>,
    }

    impl<S> crate::native::dispatcher::AsyncFuseOperationHandler<S> for TransportNoReplyAsyncHandler<S>
    where
        S: crate::native::reply::ReplySink,
    {
        fn handle(&self, envelope: crate::native::dispatcher::OwnedFuseRequest<S>) {
            self.started
                .send(envelope.request.header.unique)
                .expect("test should receive handler start");
            envelope.reply.send_no_reply();
        }
    }

    fn pipe_pair_for_test() -> (OwnedFd, OwnedFd) {
        let mut fds = [0; 2];
        // SAFETY: `fds` points to two writable c_int slots for libc to fill.
        let result = unsafe { libc::pipe(fds.as_mut_ptr()) };
        assert_eq!(result, 0, "test pipe should be created");
        // SAFETY: successful `pipe` returns two owned fds that are transferred
        // into OwnedFd exactly once.
        unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
    }

    fn write_one_byte_for_test(fd: RawFd) {
        let byte = [1_u8];
        // SAFETY: fd is an open write end of a pipe and byte points to one byte.
        let written = unsafe { libc::write(fd, byte.as_ptr().cast(), byte.len()) };
        assert_eq!(written, 1, "test pipe should accept readiness byte");
    }

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

    fn frame_with_payload(opcode: u32, unique: u64, payload_len: usize) -> Vec<u8> {
        let len = FUSE_IN_HEADER_LEN + payload_len;
        let mut frame = frame(opcode, unique);
        frame[0..4].copy_from_slice(&(len as u32).to_le_bytes());
        frame.resize(len, 0xab);
        frame
    }

    fn frame_with_payload_bytes(opcode: u32, unique: u64, payload: &[u8]) -> Vec<u8> {
        let mut frame = frame(opcode, unique);
        let len = FUSE_IN_HEADER_LEN + payload.len();
        frame[0..4].copy_from_slice(&(len as u32).to_le_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    #[test]
    fn transport_yields_typed_request_and_records_reply_without_exposing_backend() {
        let mut transport = FrameQueueTransport::new([frame(26, 0x55)]);

        let request = transport
            .receive_request()
            .expect("transport receive should parse")
            .expect("transport should yield one request");

        assert_eq!(request.opcode, FuseOpcode::Init);

        let reply = FuseReply::contiguous(
            request.header.unique,
            error_reply(request.header.unique, libc::ENOSYS).to_vec(),
        );
        transport
            .send_reply(reply)
            .expect("transport send should accept reply");

        assert_eq!(transport.sent_replies().len(), 1);
    }

    #[test]
    fn auto_io_uring_transport_falls_back_to_classic_when_kernel_capability_is_missing() {
        let decision = resolve_linux_native_transport(
            LinuxNativeTransportSelection::Auto,
            IoUringTransportProbe {
                kernel_supported: false,
                fuse_module_enabled: true,
                fuse_cap_over_io_uring: true,
                queue_depth: Some(64),
                restricted_by_policy: false,
            },
        );

        assert_eq!(
            decision,
            LinuxNativeTransportDecision::classic_with_fallback(
                IoUringFallbackReason::KernelUnsupported
            )
        );
    }

    #[test]
    fn io_uring_transport_routes_interrupt_requests_through_classic_fallback() {
        let route =
            route_linux_native_request(LinuxNativeTransportMode::IoUring, FuseOpcode::Other(36));

        assert_eq!(
            route,
            LinuxNativeRequestRoute::classic_fallback(
                IoUringFallbackReason::UnsupportedRequestClass
            )
        );
    }

    #[test]
    fn io_uring_uapi_layout_matches_linux_fuse_abi_7_42() {
        assert_eq!(FUSE_URING_IN_OUT_HEADER_SZ, 128);
        assert_eq!(FUSE_URING_OP_IN_OUT_SZ, 128);
        assert_eq!(
            std::mem::size_of::<FuseUringEntInOut>(),
            FUSE_URING_ENT_IN_OUT_SZ
        );
        assert_eq!(
            std::mem::size_of::<FuseUringReqHeader>(),
            FUSE_URING_REQ_HEADER_SZ
        );
        assert_eq!(std::mem::size_of::<FuseUringCmdReq>(), 24);
    }

    #[test]
    fn io_uring_opcode_header_lengths_match_linux_fuse_uapi() {
        let cases = [
            (FuseOpcode::Lookup, 0),
            (FuseOpcode::Forget, 8),
            (FuseOpcode::Getattr, 16),
            (FuseOpcode::Setattr, 88),
            (FuseOpcode::Mkdir, 8),
            (FuseOpcode::Create, 16),
            (FuseOpcode::Unlink, 0),
            (FuseOpcode::Rmdir, 0),
            (FuseOpcode::Rename, 8),
            (FuseOpcode::Open, 8),
            (FuseOpcode::Read, 40),
            (FuseOpcode::Write, 40),
            (FuseOpcode::Statfs, 0),
            (FuseOpcode::Release, 24),
            (FuseOpcode::Fsync, 16),
            (FuseOpcode::Flush, 24),
            (FuseOpcode::Init, 16),
            (FuseOpcode::Opendir, 8),
            (FuseOpcode::Readdir, 40),
            (FuseOpcode::Releasedir, 24),
            (FuseOpcode::BatchForget, 8),
            (FuseOpcode::Destroy, 0),
            (FuseOpcode::Readdirplus, 40),
            (FuseOpcode::Other(52), 0),
        ];

        for (opcode, expected_len) in cases {
            assert_eq!(fuse_uring_op_header_len(opcode), expected_len, "{opcode:?}");
        }
    }

    #[test]
    fn io_uring_payload_len_matches_kernel_ring_max_payload_contract() {
        assert_eq!(io_uring_payload_len_for_session(512, 1), 8192);
        assert_eq!(
            io_uring_payload_len_for_session(16 * 1024 * 1024, 256),
            16 * 1024 * 1024
        );
        assert_eq!(
            io_uring_payload_len_for_session(128 * 1024, 512),
            512 * page_size_for_io_uring_payload()
        );
    }

    #[test]
    fn possible_cpu_count_parser_supports_linux_range_lists() {
        assert_eq!(parse_linux_possible_cpu_count("0"), Some(1));
        assert_eq!(parse_linux_possible_cpu_count("0-3"), Some(4));
        assert_eq!(parse_linux_possible_cpu_count("0-1,4,8-9\n"), Some(5));
        assert_eq!(parse_linux_possible_cpu_count(""), None);
    }

    #[test]
    fn io_uring_entry_prepares_register_command_with_header_and_payload_iovecs() {
        let mut entry = FuseUringEntry::new(7, 64 * 1024);

        let command = entry.register_command(-1);

        assert_eq!(command.op, FuseUringCommandOp::Register);
        assert_eq!(command.cmd_req.qid, 7);
        assert_eq!(command.cmd_req.commit_id, 0);
        assert_eq!(command.iovecs.len(), 2);
        assert_eq!(command.iovecs[0].iov_len, FUSE_URING_REQ_HEADER_SZ);
        assert_eq!(command.iovecs[1].iov_len, 64 * 1024);
    }

    #[test]
    fn io_uring_entry_owns_stable_iovecs_for_pending_uring_cmd() {
        let mut entry = FuseUringEntry::new(7, 64 * 1024);
        let entry_iovecs = entry.iovecs_ptr_for_test();

        let command = entry.register_command(-1);

        assert_eq!(command.iovecs.as_ptr(), entry_iovecs);
    }

    #[test]
    fn io_uring_entry_iovecs_survive_entry_move_after_command_build() {
        let mut entry = FuseUringEntry::new(7, 64 * 1024);
        let iovecs = entry.iovecs_ptr_for_test();
        let header_iovec_base = {
            let command = entry.register_command(-1);
            command.iovecs[0].iov_base
        };

        let moved = entry;

        assert_eq!(moved.iovecs_ptr_for_test(), iovecs);
        assert_eq!(moved.iovecs[0].iov_base, header_iovec_base);
    }

    #[test]
    fn io_uring_commit_and_fetch_command_serializes_kernel_cmd_payload() {
        let mut entry = FuseUringEntry::new(3, 4096);

        let command = entry.commit_and_fetch_command(-1, 0x55aa);

        assert_eq!(command.op.as_raw(), 2);
        assert_eq!(command.cmd_req.qid, 3);
        assert_eq!(command.cmd_req.commit_id, 0x55aa);
        assert_eq!(&command.cmd_bytes()[8..16], &0x55aa_u64.to_le_bytes());
    }

    #[test]
    fn io_uring_command_builds_sqe128_uring_cmd_entry() {
        let mut entry = FuseUringEntry::new(3, 4096);
        let command = entry.register_command(-1);

        let sqe = command.build_sqe(0x44);

        assert_eq!(std::mem::size_of_val(&sqe), 128);
    }

    #[test]
    fn io_uring_command_sets_iovec_pointer_and_count_on_sqe() {
        let mut entry = FuseUringEntry::new(3, 4096);
        let command = entry.register_command(-1);
        let iovecs_ptr = command.iovecs.as_ptr() as u64;

        let sqe = command.build_sqe(0x44);

        assert_eq!(uring_cmd_sqe_addr_for_test(&sqe), iovecs_ptr);
        assert_eq!(uring_cmd_sqe_len_for_test(&sqe), 2);
    }

    #[test]
    fn io_uring_transport_registers_one_entry_per_queue_before_receiving_requests() {
        let backend = RecordingUringBackend::default();
        let mut transport = FuseIoUringTransport::with_backend(-1, 2, 4096, backend);

        transport
            .register_entries()
            .expect("registration should submit entries");

        assert_eq!(transport.backend().submitted_ops(), [1, 1]);
        assert_eq!(transport.backend().submit_flushes(), 1);
        assert!(transport.backend().submit_waits().is_empty());
        assert!(transport.is_ready());
    }

    #[test]
    fn registered_io_uring_transport_owns_fuse_fd_and_registers_entries() {
        let fuse_file = std::fs::File::open("/dev/null").expect("/dev/null should open");
        let backend = RecordingUringBackend::default();

        let transport = RegisteredFuseIoUringTransport::new(fuse_file, 2, 4096, backend)
            .expect("registered io_uring transport should submit register commands");

        assert_eq!(transport.inner_for_test().backend().submitted_ops(), [1, 1]);
        assert!(transport.inner_for_test().is_ready());
    }

    #[test]
    fn io_uring_transport_receives_cqe_backed_init_request_from_ring_entry() {
        let mut backend = RecordingUringBackend::default();
        backend.push_completion(FuseIoUringCompletion {
            user_data: 1,
            result: 0,
        });
        let mut transport = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        let mut frame = frame(26, 0x123);
        let payload = [7, 0, 0, 0, 42, 0, 0, 0, 0, 0, 2, 0, 1, 0, 0, 0];
        let frame_len = (frame.len() + payload.len()) as u32;
        frame[0..4].copy_from_slice(&frame_len.to_le_bytes());
        frame.extend_from_slice(&payload);
        transport
            .entry_mut_for_test(0)
            .write_request_for_test(&frame);

        let request = transport
            .receive_request()
            .expect("io_uring receive should parse")
            .expect("cqe should yield request");

        assert_eq!(request.opcode, FuseOpcode::Init);
        assert_eq!(request.header.unique, 0x123);
    }

    #[test]
    fn io_uring_transport_receives_request_with_io_uring_reply_origin() {
        let mut backend = RecordingUringBackend::default();
        backend.push_completion(FuseIoUringCompletion {
            user_data: 1,
            result: 0,
        });
        let mut transport = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        transport
            .entry_mut_for_test(0)
            .write_request_for_test(&frame(26, 0x123));

        let (request, origin) = transport
            .receive_request_with_origin()
            .expect("io_uring receive should parse")
            .expect("cqe should yield request and origin");

        assert_eq!(request.header.unique, 0x123);
        assert_eq!(
            origin,
            ReplyOrigin::IoUring {
                queue_id: 0,
                generation: 1,
            }
        );
    }

    #[test]
    fn io_uring_transport_rejects_stale_generation_reply_origin() {
        let mut backend = RecordingUringBackend::default();
        backend.push_completion(FuseIoUringCompletion {
            user_data: 1,
            result: 0,
        });
        let mut transport = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        transport
            .entry_mut_for_test(0)
            .write_request_for_test(&frame(2, 0x128));
        let (_request, _origin) = transport
            .receive_request_with_origin()
            .expect("io_uring receive should parse")
            .expect("cqe should yield request and origin");

        let error = transport
            .send_reply_from_origin(
                ReplyOrigin::IoUring {
                    queue_id: 0,
                    generation: 0,
                },
                FuseReply::no_reply(0x128),
            )
            .expect_err("stale generation should be rejected");

        match error {
            TransportError::Io(error) => assert_eq!(error.kind(), io::ErrorKind::NotFound),
            error => panic!("expected stale origin I/O error, got {error:?}"),
        }
        assert_eq!(transport.backend().submitted_len(), 0);
    }

    #[test]
    fn io_uring_transport_reconstructs_write_request_from_op_header_and_payload_iovecs() {
        let mut backend = RecordingUringBackend::default();
        backend.push_completion(FuseIoUringCompletion {
            user_data: 1,
            result: 0,
        });
        let mut transport = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        let mut write_in = [0_u8; 40];
        write_in[0..8].copy_from_slice(&0xabc_u64.to_le_bytes());
        write_in[8..16].copy_from_slice(&128_u64.to_le_bytes());
        write_in[16..20].copy_from_slice(&4_u32.to_le_bytes());
        let file_data = [0xde, 0xad, 0xbe, 0xef];
        let mut payload = Vec::from(write_in);
        payload.extend_from_slice(&file_data);
        let frame = frame_with_payload_bytes(16, 0x125, &payload);
        transport
            .entry_mut_for_test(0)
            .write_request_parts_for_test(&frame, write_in.len());

        let request = transport
            .receive_request()
            .expect("io_uring receive should parse")
            .expect("cqe should yield request");

        assert_eq!(request.opcode, FuseOpcode::Write);
        assert_eq!(request.payload, payload);
    }

    #[test]
    fn io_uring_transport_commits_reply_and_fetches_next_request() {
        let mut backend = RecordingUringBackend::default();
        backend.push_completion(FuseIoUringCompletion {
            user_data: 1,
            result: 0,
        });
        let mut transport = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        let mut frame = frame(26, 0x124);
        frame[0..4].copy_from_slice(&(FUSE_IN_HEADER_LEN as u32).to_le_bytes());
        transport
            .entry_mut_for_test(0)
            .write_request_for_test(&frame);
        let request = transport
            .receive_request()
            .expect("receive should parse")
            .expect("request should be present");

        transport
            .send_reply(FuseReply::contiguous(
                request.header.unique,
                error_reply(request.header.unique, libc::ENOSYS).to_vec(),
            ))
            .expect("reply commit should submit commit-and-fetch");

        assert_eq!(transport.backend().submitted_len(), 1);
        assert_eq!(transport.backend().submit_flushes(), 1);
        assert!(transport.backend().submit_waits().is_empty());
        assert_eq!(transport.entry_for_test(0).reply_payload_len_for_test(), 0);
    }

    #[test]
    fn io_uring_transport_commits_no_reply_request_and_fetches_next_request() {
        let mut backend = RecordingUringBackend::default();
        backend.push_completion(FuseIoUringCompletion {
            user_data: 1,
            result: 0,
        });
        let mut transport = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        transport
            .entry_mut_for_test(0)
            .write_request_for_test(&frame(2, 0x126));
        let request = transport
            .receive_request()
            .expect("receive should parse")
            .expect("request should be present");

        transport
            .send_reply(FuseReply::no_reply(request.header.unique))
            .expect("no-reply request should still commit the io_uring entry");

        assert_eq!(transport.backend().submitted_len(), 1);
        assert_eq!(transport.backend().submit_flushes(), 1);
        assert!(transport.backend().submit_waits().is_empty());
        assert_eq!(transport.entry_for_test(0).reply_payload_len_for_test(), 0);
    }

    #[test]
    fn io_uring_transport_copies_local_cache_reply_range_into_payload_buffer() {
        let mut backend = RecordingUringBackend::default();
        backend.push_completion(FuseIoUringCompletion {
            user_data: 1,
            result: 0,
        });
        let mut cache_file = tempfile::tempfile().expect("create local cache file");
        cache_file
            .write_all(b"0123456789abcdef")
            .expect("write cache contents");
        cache_file.rewind().expect("rewind cache file");
        let mut transport = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        transport
            .entry_mut_for_test(0)
            .write_request_for_test(&frame(15, 0x127));
        let request = transport
            .receive_request()
            .expect("receive should parse")
            .expect("request should be present");
        let reply = FuseReply::local_cache_range_data(
            request.header.unique,
            FuseLocalCacheReadRange::borrowed(cache_file.as_raw_fd(), 4, 6, 9),
        )
        .expect("build local cache range reply");

        transport
            .send_reply(reply)
            .expect("local cache reply should fill io_uring payload");

        assert_eq!(transport.entry_for_test(0).reply_payload_len_for_test(), 6);
        assert_eq!(
            transport.entry_for_test(0).reply_payload_for_test(),
            b"456789"
        );
        assert_eq!(transport.backend().submitted_len(), 1);
    }

    #[test]
    fn hybrid_transport_receives_io_uring_cqe_before_classic_fallback() {
        let mut backend = RecordingUringBackend::default();
        backend.push_completion(FuseIoUringCompletion {
            user_data: 1,
            result: 0,
        });
        let mut io_uring = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        io_uring
            .entry_mut_for_test(0)
            .write_request_for_test(&frame(38, 0x130));
        let classic = FrameQueueTransport::new([frame(38, 0x131)]);
        let mut transport = HybridFuseTransport::new(classic, io_uring);

        let request = transport
            .receive_request()
            .expect("hybrid receive should parse")
            .expect("io_uring cqe should yield request first");

        assert_eq!(request.header.unique, 0x130);
    }

    #[test]
    fn hybrid_transport_receives_io_uring_request_with_reply_origin() {
        let mut backend = RecordingUringBackend::default();
        backend.push_completion(FuseIoUringCompletion {
            user_data: 1,
            result: 0,
        });
        let mut io_uring = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        io_uring
            .entry_mut_for_test(0)
            .write_request_for_test(&frame(38, 0x138));
        let classic = FrameQueueTransport::new([]);
        let mut transport = HybridFuseTransport::new(classic, io_uring);

        let (request, origin) = transport
            .receive_request_with_origin()
            .expect("hybrid receive should parse")
            .expect("io_uring cqe should yield request and origin");

        assert_eq!(request.header.unique, 0x138);
        assert_eq!(
            origin,
            ReplyOrigin::IoUring {
                queue_id: 0,
                generation: 1,
            }
        );
    }

    #[test]
    fn hybrid_transport_receives_classic_fallback_request_with_classic_origin() {
        let backend = RecordingUringBackend::default();
        let io_uring = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        let classic = FrameQueueTransport::new([frame(38, 0x139)]);
        let mut transport = HybridFuseTransport::new(classic, io_uring);

        let (request, origin) = transport
            .receive_request_with_origin()
            .expect("classic fallback receive should parse")
            .expect("classic fallback should yield request and origin");

        assert_eq!(request.header.unique, 0x139);
        assert_eq!(origin, ReplyOrigin::Classic);
    }

    #[test]
    fn hybrid_transport_falls_back_to_classic_receive_after_io_uring_runtime_error() {
        let io_uring = FuseIoUringTransport::with_backend(-1, 1, 4096, FailingReceiveUringBackend);
        let classic = FrameQueueTransport::new([frame(38, 0x13c)]);
        let mut transport = HybridFuseTransport::new(classic, io_uring);

        let (request, origin) = transport
            .receive_request_with_origin()
            .expect("hybrid receive should fall back to classic after io_uring runtime error")
            .expect("classic fallback should yield the next request");

        assert_eq!(request.header.unique, 0x13c);
        assert_eq!(origin, ReplyOrigin::Classic);
        assert_eq!(transport.metrics_snapshot().classic_fallbacks, 1);
        assert_eq!(
            transport
                .metrics_snapshot()
                .io_uring_runtime_error_fallbacks,
            1
        );
        assert_eq!(transport.metrics_snapshot().classic_receive_fallbacks, 1);
        assert_eq!(transport.metrics_snapshot().classic_reply_fallbacks, 0);
    }

    #[test]
    fn hybrid_transport_waits_for_io_uring_fd_instead_of_blocking_on_classic_fallback() {
        let backend = DelayedReadyUringBackend::new(FuseIoUringCompletion {
            user_data: 1,
            result: 0,
        });
        let mut io_uring = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        io_uring
            .entry_mut_for_test(0)
            .write_request_for_test(&frame(38, 0x136));
        let classic = PanicClassicTransport::new_not_ready();
        let mut transport = HybridFuseTransport::new(classic, io_uring);

        let request = transport
            .receive_request()
            .expect("hybrid receive should wait on io_uring readiness")
            .expect("io_uring readiness should yield request");

        assert_eq!(request.header.unique, 0x136);
    }

    #[test]
    fn hybrid_transport_sends_io_uring_replies_to_matching_ring_entry() {
        let mut backend = RecordingUringBackend::default();
        backend.push_completion(FuseIoUringCompletion {
            user_data: 1,
            result: 0,
        });
        let mut io_uring = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        io_uring
            .entry_mut_for_test(0)
            .write_request_for_test(&frame(38, 0x132));
        let classic = FrameQueueTransport::new([]);
        let mut transport = HybridFuseTransport::new(classic, io_uring);
        let request = transport
            .receive_request()
            .expect("hybrid receive should parse")
            .expect("io_uring cqe should yield request");

        transport
            .send_reply(FuseReply::contiguous(
                request.header.unique,
                error_reply(request.header.unique, libc::ENOSYS).to_vec(),
            ))
            .expect("hybrid send should commit io_uring reply");

        assert_eq!(transport.io_uring_for_test().backend().submitted_len(), 1);
    }

    #[test]
    fn transport_reply_sink_commits_io_uring_no_reply_completion() {
        let mut backend = RecordingUringBackend::default();
        backend.push_completion(FuseIoUringCompletion {
            user_data: 1,
            result: 0,
        });
        let mut io_uring = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        io_uring
            .entry_mut_for_test(0)
            .write_request_for_test(&frame(2, 0x137));
        let classic = FrameQueueTransport::new([]);
        let mut transport = HybridFuseTransport::new(classic, io_uring);
        let request = transport
            .receive_request()
            .expect("hybrid receive should parse")
            .expect("io_uring cqe should yield request");
        let sink = TransportReplySink::new(transport);
        let token = crate::native::reply::ReplyToken::new_io_uring(
            request.header.unique,
            sink.clone(),
            0,
            1,
        );

        token.send_no_reply();
        transport = sink
            .try_into_inner()
            .expect("test should own the only transport sink reference");

        assert_eq!(transport.io_uring_for_test().backend().submitted_len(), 1);
        assert_eq!(transport.io_uring_for_test().backend().submit_flushes(), 1);
    }

    #[test]
    fn transport_reply_sink_exposes_send_errors_and_dropped_replies() {
        let sink = TransportReplySink::new(EioReplyTransport);
        let token = crate::native::reply::ReplyToken::new_classic(0x13b, sink.clone());

        token.send_success(&[]);
        sink.dropped(
            ReplyOrigin::IoUring {
                queue_id: 7,
                generation: 9,
            },
            0x13c,
        );

        let errors = sink.take_send_errors();
        assert_eq!(errors.len(), 1);
        match &errors[0] {
            TransportError::Io(error) => assert_eq!(error.raw_os_error(), Some(libc::EIO)),
            error => panic!("expected EIO transport error, got {error:?}"),
        }
        assert_eq!(
            sink.dropped_replies(),
            [(
                ReplyOrigin::IoUring {
                    queue_id: 7,
                    generation: 9,
                },
                0x13c
            )]
        );
    }

    #[test]
    fn async_dispatcher_commits_hybrid_io_uring_request_through_transport_sink() {
        let mut backend = RecordingUringBackend::default();
        backend.push_completion(FuseIoUringCompletion {
            user_data: 1,
            result: 0,
        });
        let mut io_uring = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        io_uring
            .entry_mut_for_test(0)
            .write_request_for_test(&frame(2, 0x13a));
        let classic = FrameQueueTransport::new([]);
        let mut transport = HybridFuseTransport::new(classic, io_uring);
        let (request, origin) = transport
            .receive_request_with_origin()
            .expect("hybrid receive should parse")
            .expect("io_uring cqe should yield request and origin");
        let sink = TransportReplySink::new(transport);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let dispatcher = crate::native::dispatcher::NativeAsyncDispatcher::<_>::builder(
            TransportNoReplyAsyncHandler {
                started: started_tx,
                _sink: std::marker::PhantomData,
            },
        )
        .spawn();

        crate::native::dispatcher::submit_transport_request_with_origin(
            &dispatcher,
            request,
            origin,
            sink.clone(),
        )
        .expect("origin-aware async submit should enqueue request");

        assert_eq!(
            started_rx
                .recv_timeout(std::time::Duration::from_millis(250))
                .expect("worker should process request"),
            0x13a
        );
        dispatcher
            .shutdown()
            .expect("dispatcher should join no-reply worker");
        transport = sink
            .try_into_inner()
            .expect("test should own the only transport sink reference");

        assert_eq!(transport.io_uring_for_test().backend().submitted_len(), 1);
        assert_eq!(transport.io_uring_for_test().backend().submit_flushes(), 1);
    }

    #[test]
    fn hybrid_transport_factory_registers_io_uring_entries_for_classic_fallback_fd() {
        let fuse_file = std::fs::File::open("/dev/null").expect("/dev/null should open");
        let fuse_fd = fuse_file.as_raw_fd();
        let classic = ClassicReadWriteTransport::new(fuse_file);
        let backend = RecordingUringBackend::default();

        let transport = HybridFuseTransport::register_io_uring(classic, fuse_fd, 2, 4096, backend)
            .expect("hybrid factory should register io_uring entries");

        assert_eq!(
            transport.io_uring_for_test().backend().submitted_ops(),
            [1, 1]
        );
    }

    #[test]
    fn hybrid_transport_exposes_kernel_io_uring_factory_for_mount_startup() {
        type KernelIoUringFactory = fn(
            ClassicReadWriteTransport<std::fs::File>,
            RawFd,
            u16,
            usize,
        ) -> Result<
            HybridFuseTransport<ClassicReadWriteTransport<std::fs::File>, IoUringBackend>,
            TransportError,
        >;

        let _factory: KernelIoUringFactory = HybridFuseTransport::register_kernel_io_uring;
    }

    #[test]
    fn io_uring_transport_metrics_report_registration_cqes_and_commit_fetches() {
        let mut backend = RecordingUringBackend::default();
        backend.push_completion(FuseIoUringCompletion {
            user_data: 1,
            result: 0,
        });
        let mut transport = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);

        transport
            .register_entries()
            .expect("registration should submit entries");
        transport
            .entry_mut_for_test(0)
            .write_request_for_test(&frame(38, 0x133));
        let request = transport
            .receive_request()
            .expect("io_uring receive should parse")
            .expect("cqe should yield request");
        transport
            .send_reply(FuseReply::contiguous(
                request.header.unique,
                error_reply(request.header.unique, libc::ENOSYS).to_vec(),
            ))
            .expect("reply commit should submit commit-and-fetch");

        let metrics = transport.metrics_snapshot();
        assert_eq!(metrics.queue_depth, 1);
        assert_eq!(metrics.register_commands, 1);
        assert_eq!(metrics.commit_and_fetch_commands, 1);
        assert_eq!(metrics.cqe_completions, 1);
        assert_eq!(metrics.cqe_errors, 0);
        assert_eq!(metrics.submit_flushes, 2);
        assert_eq!(metrics.submit_waits, 0);
    }

    #[test]
    fn hybrid_transport_metrics_count_classic_fallback_activity() {
        let backend = RecordingUringBackend::default();
        let io_uring = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        let classic = FrameQueueTransport::new([frame(38, 0x134)]);
        let mut transport = HybridFuseTransport::new(classic, io_uring);

        let request = transport
            .receive_request()
            .expect("classic fallback receive should parse")
            .expect("classic fallback should yield request");
        transport
            .send_reply(FuseReply::contiguous(
                request.header.unique,
                error_reply(request.header.unique, libc::ENOSYS).to_vec(),
            ))
            .expect("classic fallback send should write reply");

        let metrics = transport.metrics_snapshot();
        assert_eq!(metrics.mode, LinuxNativeTransportMode::IoUring);
        assert_eq!(metrics.classic_fallbacks, 2);
        assert_eq!(metrics.io_uring.cqe_completions, 0);
    }

    #[test]
    fn linux_native_fuse_transport_enum_dispatches_classic_requests() {
        let classic = FrameQueueTransport::new([frame(38, 0x135)]);
        let mut transport: LinuxNativeFuseTransport<_, RecordingUringBackend> =
            LinuxNativeFuseTransport::Classic(classic);

        let request = transport
            .receive_request()
            .expect("classic enum transport should parse")
            .expect("classic enum transport should yield request");

        assert_eq!(request.header.unique, 0x135);
    }

    #[test]
    fn linux_native_fuse_transport_enum_exposes_hybrid_metrics() {
        let backend = RecordingUringBackend::default();
        let io_uring = FuseIoUringTransport::with_backend(-1, 1, 4096, backend);
        let classic = FrameQueueTransport::new([]);
        let transport =
            LinuxNativeFuseTransport::Hybrid(HybridFuseTransport::new(classic, io_uring));

        let metrics = transport.metrics_snapshot();

        assert_eq!(metrics.mode, LinuxNativeTransportMode::IoUring);
        assert_eq!(metrics.classic_fallbacks, 0);
    }

    #[derive(Debug)]
    struct MemoryFuseDevice {
        input: Vec<u8>,
        cursor: usize,
        output: Vec<u8>,
    }

    impl MemoryFuseDevice {
        fn new(input: Vec<u8>) -> Self {
            Self {
                input,
                cursor: 0,
                output: Vec::new(),
            }
        }
    }

    impl Read for MemoryFuseDevice {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.cursor >= self.input.len() {
                return Ok(0);
            }
            let available = self.input.len() - self.cursor;
            let len = available.min(buf.len());
            buf[..len].copy_from_slice(&self.input[self.cursor..self.cursor + len]);
            self.cursor += len;
            Ok(len)
        }
    }

    impl Write for MemoryFuseDevice {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.output.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct FuseDeviceLikeReader {
        frame: Vec<u8>,
        min_read_len: usize,
        output: Vec<u8>,
    }

    impl Read for FuseDeviceLikeReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if buf.len() < self.min_read_len {
                return Err(io::Error::from_raw_os_error(libc::EINVAL));
            }
            if self.frame.is_empty() {
                return Ok(0);
            }
            let len = self.frame.len();
            buf[..len].copy_from_slice(&self.frame);
            self.frame.clear();
            Ok(len)
        }
    }

    impl Write for FuseDeviceLikeReader {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.output.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct ErroringFuseDevice {
        errors: VecDeque<io::Error>,
        frame: Vec<u8>,
    }

    #[derive(Debug, Default)]
    struct RecordingWriteDevice {
        output: Vec<u8>,
        write_lens: Vec<usize>,
    }

    impl Read for RecordingWriteDevice {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Ok(0)
        }
    }

    impl Write for RecordingWriteDevice {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.write_lens.push(buf.len());
            self.output.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Read for ErroringFuseDevice {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if let Some(error) = self.errors.pop_front() {
                return Err(error);
            }
            if self.frame.is_empty() {
                return Ok(0);
            }
            let len = self.frame.len();
            buf[..len].copy_from_slice(&self.frame);
            self.frame.clear();
            Ok(len)
        }
    }

    struct SequenceFuseDevice {
        frames: VecDeque<Vec<u8>>,
        output: Vec<u8>,
    }

    impl Read for SequenceFuseDevice {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let Some(frame) = self.frames.pop_front() else {
                return Ok(0);
            };
            let len = frame.len();
            buf[..len].copy_from_slice(&frame);
            Ok(len)
        }
    }

    impl Write for SequenceFuseDevice {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.output.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Write for ErroringFuseDevice {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn classic_read_write_transport_reads_frame_and_writes_reply_bytes() {
        let mut transport = ClassicReadWriteTransport::new(MemoryFuseDevice::new(frame(26, 0x77)));

        let request = transport
            .receive_request()
            .expect("classic receive should parse")
            .expect("classic receive should yield one request");

        assert_eq!(request.opcode, FuseOpcode::Init);

        let bytes = error_reply(request.header.unique, libc::EIO).to_vec();
        transport
            .send_reply(FuseReply::contiguous(request.header.unique, bytes.clone()))
            .expect("classic send should write reply");

        assert_eq!(transport.into_inner().output, bytes);
    }

    #[test]
    fn classic_read_write_transport_writes_local_cache_range_from_fd() {
        let mut cache_file = tempfile::tempfile().expect("create local cache file");
        cache_file
            .write_all(b"0123456789abcdef")
            .expect("write cache contents");
        cache_file.rewind().expect("rewind cache file");
        let mut transport = ClassicReadWriteTransport::new(MemoryFuseDevice::new(Vec::new()));

        let reply = FuseReply::local_cache_range_data(
            0x88,
            FuseLocalCacheReadRange::borrowed(cache_file.as_raw_fd(), 4, 6, 9),
        )
        .expect("build local cache range reply");
        transport
            .send_reply(reply)
            .expect("classic send should write local cache range");

        let metrics = transport.local_cache_reply_metrics_snapshot();
        assert_eq!(metrics.pread_bytes_replies, 1);
        assert_eq!(metrics.payload_bytes, 6);
        let output = transport.into_inner().output;
        assert_eq!(&output[16..], b"456789");
    }

    #[test]
    fn classic_read_write_transport_writes_pread_local_cache_range_as_one_frame() {
        let mut cache_file = tempfile::tempfile().expect("create local cache file");
        cache_file
            .write_all(b"0123456789abcdef")
            .expect("write cache contents");
        cache_file.rewind().expect("rewind cache file");
        let mut transport = ClassicReadWriteTransport::new(RecordingWriteDevice::default());

        let reply = FuseReply::local_cache_range_data(
            0x89,
            FuseLocalCacheReadRange::borrowed(cache_file.as_raw_fd(), 4, 6, 9),
        )
        .expect("build local cache range reply");
        transport
            .send_reply(reply)
            .expect("classic send should write local cache range");

        let output = transport.into_inner();
        assert_eq!(output.write_lens, [22]);
        assert_eq!(&output.output[16..], b"456789");
    }

    #[test]
    fn classic_local_cache_reply_path_returns_errors_instead_of_panicking_on_invariants() {
        let source = include_str!("transport.rs");
        let send_reply = source
            .split("impl<T: Read + Write> FuseTransport for ClassicReadWriteTransport<T>")
            .nth(1)
            .and_then(|rest| {
                rest.split("fn send_reply(&mut self, reply: FuseReply)")
                    .nth(1)
            })
            .and_then(|rest| rest.split("fn request_shutdown").next())
            .expect("classic send_reply body should be present");

        assert!(!send_reply.contains("local cache range reply should carry a header"));
        assert!(!send_reply.contains("local cache range reply should own a source fd"));
    }

    #[test]
    fn classic_read_write_transport_reuses_pread_reply_frame_buffer() {
        let source = include_str!("transport.rs");
        let transport_struct = source
            .split("pub struct ClassicReadWriteTransport<T>")
            .nth(1)
            .and_then(|rest| rest.split("impl<T> ClassicReadWriteTransport<T>").next())
            .expect("classic transport struct should be present");
        let pread_branch = source
            .split("LocalCacheReplyStrategy::PreadBytes => {")
            .nth(1)
            .and_then(|rest| rest.split("LocalCacheReplyStrategy::Splice => {").next())
            .expect("pread local cache reply branch should be present");

        assert!(transport_struct.contains("local_cache_pread_frame: Vec<u8>"));
        assert!(!pread_branch.contains("Vec::with_capacity"));
    }

    #[test]
    fn pread_local_cache_copy_buffer_is_heap_allocated() {
        let source = include_str!("transport.rs");
        let copy_fn = source
            .split("fn copy_local_cache_range_with_pread")
            .nth(1)
            .and_then(|rest| rest.split("pub fn splice_local_cache_range_to_fd").next())
            .expect("pread copy function should be present");

        assert!(copy_fn.contains("with_thread_local_pread_copy_buffer"));
        assert!(!copy_fn.contains("let mut buffer = [0; COPY_BUFFER_LEN]"));
    }

    #[test]
    fn pread_local_cache_copy_buffer_is_reused_per_thread() {
        reset_pread_copy_buffer_for_test();
        let mut cache_file = tempfile::tempfile().expect("create local cache file");
        cache_file
            .write_all(b"abcdefghijklmnopqrstuvwxyz")
            .expect("write cache contents");
        let mut transport = ClassicReadWriteTransport::new(MemoryFuseDevice::new(Vec::new()));

        for offset in [0, 8] {
            let reply = FuseReply::local_cache_range_data(
                0x88,
                FuseLocalCacheReadRange::borrowed(cache_file.as_raw_fd(), offset, 4, 9),
            )
            .expect("build local cache range reply");
            transport
                .send_reply(reply)
                .expect("classic send should write local cache range");
        }

        assert_eq!(pread_copy_buffer_create_count_for_test(), 1);
    }

    #[test]
    fn local_cache_range_reply_keeps_stable_fd_after_source_file_is_dropped() {
        let mut cache_file = tempfile::tempfile().expect("create local cache file");
        cache_file
            .write_all(b"stable-fd-payload")
            .expect("write cache contents");
        let reply = FuseReply::local_cache_range_data(
            0x89,
            FuseLocalCacheReadRange::borrowed(cache_file.as_raw_fd(), 7, 2, 9),
        )
        .expect("build local cache range reply");
        drop(cache_file);
        let mut transport = ClassicReadWriteTransport::new(MemoryFuseDevice::new(Vec::new()));

        transport
            .send_reply(reply)
            .expect("reply should own a stable duplicated fd");

        let output = transport.into_inner().output;
        assert_eq!(&output[16..], b"fd");
    }

    #[test]
    fn splice_local_cache_range_to_fd_copies_file_range_through_pipe() {
        let mut cache_file = tempfile::tempfile().expect("create local cache file");
        cache_file
            .write_all(b"abcdefghijklmnopqrstuvwxyz")
            .expect("write cache contents");
        let mut output_file = tempfile::tempfile().expect("create output file");

        splice_local_cache_range_to_fd(
            FuseLocalCacheReadRange::borrowed(cache_file.as_raw_fd(), 10, 8, 11),
            output_file.as_raw_fd(),
        )
        .expect("splice local cache range");

        output_file.rewind().expect("rewind output file");
        let mut output = String::new();
        output_file
            .read_to_string(&mut output)
            .expect("read output");
        assert_eq!(output, "klmnopqr");
    }

    #[test]
    fn splice_local_cache_range_reuses_thread_local_pipe() {
        reset_splice_pipe_pool_for_test();
        let mut cache_file = tempfile::tempfile().expect("create local cache file");
        cache_file
            .write_all(b"abcdefghijklmnopqrstuvwxyz")
            .expect("write cache contents");
        let output_file = tempfile::tempfile().expect("create output file");

        splice_local_cache_range_to_fd(
            FuseLocalCacheReadRange::borrowed(cache_file.as_raw_fd(), 0, 4, 11),
            output_file.as_raw_fd(),
        )
        .expect("first splice local cache range");
        splice_local_cache_range_to_fd(
            FuseLocalCacheReadRange::borrowed(cache_file.as_raw_fd(), 4, 4, 11),
            output_file.as_raw_fd(),
        )
        .expect("second splice local cache range");

        assert_eq!(splice_pipe_create_count_for_test(), 1);
    }

    #[test]
    fn splice_local_cache_range_discards_pipe_after_partial_error() {
        reset_splice_pipe_pool_for_test();
        let mut cache_file = tempfile::tempfile().expect("create local cache file");
        cache_file
            .write_all(b"AAAABBBB")
            .expect("write cache contents");
        let mut output_file = tempfile::tempfile().expect("create output file");

        let error = splice_local_cache_range_to_fd(
            FuseLocalCacheReadRange::borrowed(cache_file.as_raw_fd(), 0, 4, 11),
            -1,
        )
        .expect_err("invalid output fd should fail after filling the pipe");
        assert_eq!(error.raw_os_error(), Some(libc::EBADF));

        splice_local_cache_range_to_fd(
            FuseLocalCacheReadRange::borrowed(cache_file.as_raw_fd(), 4, 4, 11),
            output_file.as_raw_fd(),
        )
        .expect("next splice should not read stale bytes from failed pipe");

        output_file.rewind().expect("rewind output file");
        let mut output = String::new();
        output_file
            .read_to_string(&mut output)
            .expect("read output");
        assert_eq!(output, "BBBB");
        assert_eq!(splice_pipe_create_count_for_test(), 2);
    }

    #[test]
    fn splice_once_caps_request_len_to_default_pipe_capacity() {
        let source = include_str!("transport.rs");
        let splice_once_fn = source
            .split("fn splice_once")
            .nth(1)
            .expect("splice_once should be present");

        assert!(source.contains("SPLICE_PIPE_CHUNK_LEN: usize = 64 * 1024"));
        assert!(splice_once_fn.contains("len.min(SPLICE_PIPE_CHUNK_LEN)"));
        assert!(!splice_once_fn.contains("len.min(128 * 1024)"));
    }

    #[test]
    fn splice_local_cache_range_documents_fuse_framing_invariant() {
        let source = include_str!("transport.rs");
        let splice_fn = source
            .split("fn splice_local_cache_range_with_pipe")
            .nth(1)
            .and_then(|rest| rest.split("#[cfg(test)]").next())
            .expect("splice helper should be present");

        assert!(splice_fn.contains("FRAMING INVARIANT"));
        assert!(splice_fn.contains("range.len"));
    }

    #[test]
    fn classic_read_write_transport_splices_local_cache_range_when_output_fd_is_available() {
        let mut cache_file = tempfile::tempfile().expect("create local cache file");
        cache_file
            .write_all(b"0123456789abcdef")
            .expect("write cache contents");
        let output_file = tempfile::tempfile().expect("create output file");
        let output_fd = output_file.as_raw_fd();
        let mut transport = ClassicReadWriteTransport::with_output_fd(output_file, output_fd)
            .with_local_cache_strategy(LocalCacheReplyStrategy::Splice);
        let reply = FuseReply::local_cache_range_data(
            0x90,
            FuseLocalCacheReadRange::borrowed(cache_file.as_raw_fd(), 8, 4, 12),
        )
        .expect("build local cache range reply");

        transport
            .send_reply(reply)
            .expect("classic transport should splice when output fd is available");

        let metrics = transport.local_cache_reply_metrics_snapshot();
        assert_eq!(metrics.splice_replies, 1);
        assert_eq!(metrics.payload_bytes, 4);
        let mut output_file = transport.into_inner();
        output_file.rewind().expect("rewind output file");
        let mut output = Vec::new();
        output_file.read_to_end(&mut output).expect("read output");
        assert_eq!(&output[16..], b"89ab");
    }

    #[test]
    fn classic_read_write_transport_reads_fuse_device_frames_with_large_buffer() {
        let mut transport = ClassicReadWriteTransport::new(FuseDeviceLikeReader {
            frame: frame(26, 0x88),
            min_read_len: 8192,
            output: Vec::new(),
        });

        let request = transport
            .receive_request()
            .expect("transport should use a large read buffer")
            .expect("transport should parse one frame");

        assert_eq!(request.header.unique, 0x88);
    }

    #[test]
    fn classic_read_write_transport_reads_default_linux_max_write_frame() {
        let payload_len = 16 * 1024 * 1024;
        let mut transport = ClassicReadWriteTransport::new(MemoryFuseDevice::new(
            frame_with_payload(26, 0x8a, payload_len),
        ));

        let request = transport
            .receive_request()
            .expect("transport should read a Linux-default max_write frame")
            .expect("transport should parse the large frame");

        assert_eq!(request.payload.len(), payload_len);
    }

    #[test]
    fn classic_read_write_transport_reuses_large_read_buffer_between_requests() {
        let mut transport = ClassicReadWriteTransport::new(SequenceFuseDevice {
            frames: VecDeque::from([frame(26, 0x88), frame(38, 0x89)]),
            output: Vec::new(),
        });
        let initial_buffer = transport.read_buffer.as_ptr();

        transport
            .receive_request()
            .expect("first frame should parse")
            .expect("first request should be present");
        transport
            .receive_request()
            .expect("second frame should parse")
            .expect("second request should be present");

        assert_eq!(transport.read_buffer.as_ptr(), initial_buffer);
        assert!(transport.read_buffer.len() >= FUSE_IN_HEADER_LEN + DEFAULT_CLASSIC_MAX_WRITE);
    }

    #[test]
    fn classic_read_write_transport_treats_fuse_teardown_errors_as_end_of_stream() {
        for errno in [libc::ENODEV, libc::EAGAIN] {
            let mut transport = ClassicReadWriteTransport::new(ErroringFuseDevice {
                errors: VecDeque::from([io::Error::from_raw_os_error(errno)]),
                frame: Vec::new(),
            });

            let request = transport
                .receive_request()
                .expect("teardown read error should not fail dispatcher");

            assert_eq!(request, None);
        }
    }

    #[test]
    fn classic_read_write_transport_retries_interrupted_read() {
        let mut transport = ClassicReadWriteTransport::new(ErroringFuseDevice {
            errors: VecDeque::from([io::Error::from(io::ErrorKind::Interrupted)]),
            frame: frame(26, 0x99),
        });

        let request = transport
            .receive_request()
            .expect("interrupted read should be retried")
            .expect("request should be read after retry");

        assert_eq!(request.header.unique, 0x99);
    }

    #[test]
    fn transport_shutdown_stops_future_intake_without_dropping_sent_replies() {
        let mut transport = FrameQueueTransport::new([frame(26, 0x77)]);
        transport
            .request_shutdown()
            .expect("shutdown request should be accepted");

        let request = transport
            .receive_request()
            .expect("receive after shutdown should not error");

        assert_eq!(request, None);
    }
}
