//! Transport boundary for Linux-native FUSE sessions.

use std::collections::VecDeque;

use thiserror::Error;

use crate::native::abi::AbiParseError;
use crate::native::protocol::{FuseReply, FuseRequest};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransportError {
    #[error(transparent)]
    Abi(#[from] AbiParseError),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::abi::{FUSE_IN_HEADER_LEN, error_reply};
    use crate::native::protocol::FuseOpcode;

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
}
