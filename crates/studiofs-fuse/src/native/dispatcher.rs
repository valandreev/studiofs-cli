//! Transport-independent native FUSE dispatcher.

use thiserror::Error;

use crate::native::abi::error_reply;
use crate::native::protocol::{FuseOpcode, FuseReply, FuseRequest};
use crate::native::transport::{FuseTransport, TransportError};

pub trait FuseOperationHandler {
    fn handle(&mut self, request: FuseRequest) -> FuseReply;
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatchSummary {
    pub handled_requests: usize,
    pub saw_destroy: bool,
}

#[derive(Debug, Error)]
pub enum DispatchError {
    #[error(transparent)]
    Transport(#[from] TransportError),
}

pub struct NativeDispatcher<T, H> {
    transport: T,
    handler: H,
}

impl<T, H> NativeDispatcher<T, H> {
    pub fn new(transport: T, handler: H) -> Self {
        Self { transport, handler }
    }

    pub fn into_parts(self) -> (T, H) {
        (self.transport, self.handler)
    }
}

impl<T: FuseTransport, H: FuseOperationHandler> NativeDispatcher<T, H> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::abi::{FUSE_IN_HEADER_LEN, ok_reply};
    use crate::native::transport::FrameQueueTransport;

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
                header: crate::native::abi::FuseInHeader {
                    len: FUSE_IN_HEADER_LEN as u32,
                    opcode: 0,
                    unique: 0x55,
                    nodeid: 1,
                    uid: 1000,
                    gid: 1000,
                    pid: 77,
                },
                opcode,
                payload: Vec::new(),
            };

            let reply = handler.handle(request);

            assert_eq!(&reply.bytes[0..4], &16_u32.to_le_bytes());
            assert_eq!(&reply.bytes[4..8], &(-(libc::ENOSYS)).to_le_bytes());
            assert_eq!(&reply.bytes[8..16], &0x55_u64.to_le_bytes());
        }
    }
}
