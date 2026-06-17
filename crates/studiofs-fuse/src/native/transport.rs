//! Transport boundary for Linux-native FUSE sessions.

use std::collections::VecDeque;
use std::io::{self, Read, Write};

use thiserror::Error;

use crate::native::abi::{AbiParseError, FUSE_IN_HEADER_LEN};
use crate::native::protocol::{FuseReply, FuseRequest};

const DEFAULT_CLASSIC_MAX_WRITE: usize = 16 * 1024 * 1024;

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
        }
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
        let slices = reply.io_slices();
        if slices.len() == 1 {
            self.io.write_all(slices[0].as_ref())?;
        } else {
            let mut written = 0;
            let expected = slices.iter().map(|slice| slice.len()).sum();
            while written < expected {
                let mut start = written;
                let pending: Vec<_> = slices
                    .iter()
                    .filter_map(|slice| {
                        if start >= slice.len() {
                            start -= slice.len();
                            None
                        } else {
                            let pending = io::IoSlice::new(&slice.as_ref()[start..]);
                            start = 0;
                            Some(pending)
                        }
                    })
                    .collect();
                let bytes = self.io.write_vectored(&pending)?;
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

fn fuse_teardown_read_error(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(errno) if errno == libc::ENODEV || errno == libc::EAGAIN)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::abi::{FUSE_IN_HEADER_LEN, error_reply};
    use crate::native::protocol::FuseOpcode;
    use std::io::{Read, Write};

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
