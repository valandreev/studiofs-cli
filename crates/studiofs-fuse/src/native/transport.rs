//! Transport boundary for Linux-native FUSE sessions.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;

use crate::native::abi::{AbiParseError, FUSE_IN_HEADER_LEN};
use crate::native::protocol::{FuseLocalCacheReadRange, FuseReply, FuseRequest};

const DEFAULT_CLASSIC_MAX_WRITE: usize = 16 * 1024 * 1024;

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
    /// Requests a local shutdown so future receives stop without an I/O error.
    fn request_shutdown(&mut self) -> Result<(), TransportError> {
        Ok(())
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
    shutdown_requested: bool,
    local_cache_strategy: LocalCacheReplyStrategy,
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
            shutdown_requested: false,
            local_cache_strategy: LocalCacheReplyStrategy::PreadBytes,
            local_cache_metrics: Arc::new(LocalCacheReplyMetrics::default()),
        }
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
            let header = reply
                .local_cache_range_header()
                .expect("local cache range reply should carry a header");
            let source_fd = reply
                .local_cache_source_fd()
                .expect("local cache range reply should own a source fd");
            self.io.write_all(header)?;
            let owned_range = FuseLocalCacheReadRange::borrowed(
                source_fd,
                range.file_offset,
                range.len,
                range.content_epoch,
            );
            match self.local_cache_strategy {
                LocalCacheReplyStrategy::PreadBytes => {
                    copy_local_cache_range_with_pread(owned_range, &mut self.io)?;
                }
                LocalCacheReplyStrategy::Splice => {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "splice local-cache replies require a raw FUSE fd transport",
                    )
                    .into());
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
}

fn copy_local_cache_range_with_pread<W: Write>(
    range: FuseLocalCacheReadRange,
    writer: &mut W,
) -> io::Result<()> {
    const COPY_BUFFER_LEN: usize = 128 * 1024;

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
    let mut buffer = [0; COPY_BUFFER_LEN];

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
        offset = offset
            .checked_add(
                i64::try_from(read)
                    .map_err(|_| io::Error::other("local cache read length does not fit off_t"))?,
            )
            .ok_or_else(|| io::Error::other("local cache read offset overflowed"))?;
    }

    Ok(())
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

    while remaining > 0 {
        let moved_into_pipe = splice_once(
            range.fd,
            Some(&mut source_offset),
            pipe_write.as_raw_fd(),
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
                splice_once(pipe_read.as_raw_fd(), None, output_fd, None, pipe_remaining)?;
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
    let request_len = len.min(128 * 1024);

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

        transport
            .send_reply(FuseReply::local_cache_range_data(
                0x88,
                FuseLocalCacheReadRange::borrowed(cache_file.as_raw_fd(), 4, 6, 9),
            ))
            .expect("classic send should write local cache range");

        let metrics = transport.local_cache_reply_metrics_snapshot();
        assert_eq!(metrics.pread_bytes_replies, 1);
        assert_eq!(metrics.payload_bytes, 6);
        let output = transport.into_inner().output;
        assert_eq!(&output[16..], b"456789");
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
        );
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
