//! Transport boundary for Linux-native FUSE sessions.

use std::collections::VecDeque;
use std::io::{self, Read, Write};

use thiserror::Error;

use crate::native::abi::{AbiParseError, FUSE_IN_HEADER_LEN};
use crate::native::protocol::{FuseReply, FuseRequest};

#[derive(Debug, Error)]
pub enum TransportError {
    #[error(transparent)]
    Abi(#[from] AbiParseError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub trait FuseTransport {
    fn receive_request(&mut self) -> Result<Option<FuseRequest>, TransportError>;
    fn send_reply(&mut self, reply: FuseReply) -> Result<(), TransportError>;
}

#[derive(Debug, Default)]
pub struct FrameQueueTransport {
    inbound: VecDeque<Vec<u8>>,
    sent_replies: Vec<FuseReply>,
}

impl FrameQueueTransport {
    pub fn new(inbound: impl IntoIterator<Item = Vec<u8>>) -> Self {
        Self {
            inbound: inbound.into_iter().collect(),
            sent_replies: Vec::new(),
        }
    }

    pub fn sent_replies(&self) -> &[FuseReply] {
        &self.sent_replies
    }
}

impl FuseTransport for FrameQueueTransport {
    fn receive_request(&mut self) -> Result<Option<FuseRequest>, TransportError> {
        self.inbound
            .pop_front()
            .map(|frame| FuseRequest::from_frame(&frame).map_err(TransportError::from))
            .transpose()
    }

    fn send_reply(&mut self, reply: FuseReply) -> Result<(), TransportError> {
        self.sent_replies.push(reply);
        Ok(())
    }
}

#[derive(Debug)]
pub struct ClassicReadWriteTransport<T> {
    io: T,
}

impl<T> ClassicReadWriteTransport<T> {
    pub fn new(io: T) -> Self {
        Self { io }
    }

    pub fn into_inner(self) -> T {
        self.io
    }
}

impl<T: Read + Write> FuseTransport for ClassicReadWriteTransport<T> {
    fn receive_request(&mut self) -> Result<Option<FuseRequest>, TransportError> {
        let mut header = [0; FUSE_IN_HEADER_LEN];
        match self.io.read_exact(&mut header) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(TransportError::Io(err)),
        }

        let frame_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        if frame_len < FUSE_IN_HEADER_LEN {
            return Err(AbiParseError::FrameTooShort {
                expected: FUSE_IN_HEADER_LEN,
                actual: frame_len,
            }
            .into());
        }

        let mut frame = Vec::with_capacity(frame_len);
        frame.extend_from_slice(&header);
        frame.resize(frame_len, 0);
        self.io.read_exact(&mut frame[FUSE_IN_HEADER_LEN..])?;
        FuseRequest::from_frame(&frame)
            .map(Some)
            .map_err(TransportError::from)
    }

    fn send_reply(&mut self, reply: FuseReply) -> Result<(), TransportError> {
        self.io.write_all(&reply.bytes)?;
        Ok(())
    }
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

    #[test]
    fn transport_yields_typed_request_and_records_reply_without_exposing_backend() {
        let mut transport = FrameQueueTransport::new([frame(26, 0x55)]);

        let request = transport
            .receive_request()
            .expect("transport receive should parse")
            .expect("transport should yield one request");

        assert_eq!(request.opcode, FuseOpcode::Init);

        let reply = FuseReply {
            unique: request.header.unique,
            bytes: error_reply(request.header.unique, libc::ENOSYS).to_vec(),
        };
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
            .send_reply(FuseReply {
                unique: request.header.unique,
                bytes: bytes.clone(),
            })
            .expect("classic send should write reply");

        assert_eq!(transport.into_inner().output, bytes);
    }
}
