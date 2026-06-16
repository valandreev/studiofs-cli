//! Linux-native FUSE session lifecycle and INIT negotiation.

use thiserror::Error;

use crate::native::dispatcher::{
    DispatchError, DispatchSummary, FuseOperationHandler, NativeDispatcher,
};
use crate::native::lifecycle::{
    DirectMountOptions, FuseMountSystem, LifecycleError, NativeMountLifecycle,
};
use crate::native::protocol::{FuseCapabilities, FuseInitReply, FuseInitRequest, FuseOpcode};
use crate::native::transport::{FuseTransport, TransportError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeSessionConfig {
    pub requested_capabilities: FuseCapabilities,
    pub max_readahead: u32,
    pub max_write: u32,
    pub max_background: u16,
    pub congestion_threshold: u16,
    pub max_pages: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NegotiatedSession {
    pub protocol_major: u32,
    pub protocol_minor: u32,
    pub capabilities: FuseCapabilities,
    pub max_readahead: u32,
    pub max_write: u32,
    pub max_background: u16,
    pub congestion_threshold: u16,
    pub max_pages: u16,
}

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("FUSE session ended before INIT")]
    MissingInit,
    #[error("first FUSE request was {0:?}, expected INIT")]
    ExpectedInit(FuseOpcode),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
}

#[derive(Debug)]
pub struct NativeFuseSession<T> {
    transport: T,
    config: NativeSessionConfig,
    negotiated: Option<NegotiatedSession>,
}

impl<T> NativeFuseSession<T> {
    pub fn new(transport: T, config: NativeSessionConfig) -> Self {
        Self {
            transport,
            config,
            negotiated: None,
        }
    }

    pub fn negotiated(&self) -> Option<NegotiatedSession> {
        self.negotiated
    }

    pub fn into_transport(self) -> T {
        self.transport
    }
}

impl<T: FuseTransport> NativeFuseSession<T> {
    pub fn handshake(&mut self) -> Result<NegotiatedSession, SessionError> {
        let request = self
            .transport
            .receive_request()?
            .ok_or(SessionError::MissingInit)?;
        if request.opcode != FuseOpcode::Init {
            return Err(SessionError::ExpectedInit(request.opcode));
        }

        let init = FuseInitRequest::parse(&request.payload).map_err(TransportError::from)?;
        let capabilities = self
            .config
            .requested_capabilities
            .negotiate(init.capabilities);
        let max_readahead = self.config.max_readahead.min(init.max_readahead);
        let negotiated = NegotiatedSession {
            protocol_major: init.major,
            protocol_minor: init.minor,
            capabilities,
            max_readahead,
            max_write: self.config.max_write,
            max_background: self.config.max_background,
            congestion_threshold: self.config.congestion_threshold,
            max_pages: self.config.max_pages,
        };

        let reply = FuseInitReply {
            major: negotiated.protocol_major,
            minor: negotiated.protocol_minor,
            max_readahead: negotiated.max_readahead,
            capabilities: negotiated.capabilities,
            max_background: negotiated.max_background,
            congestion_threshold: negotiated.congestion_threshold,
            max_write: negotiated.max_write,
            time_gran: 1,
            max_pages: negotiated.max_pages,
        }
        .into_reply(request.header.unique);
        self.transport.send_reply(reply)?;
        self.negotiated = Some(negotiated);
        Ok(negotiated)
    }
}

pub struct MountedNativeSession<L, T, H> {
    lifecycle: NativeMountLifecycle<L>,
    mount_point: String,
    fuse_fd: std::os::fd::RawFd,
    uid: libc::uid_t,
    gid: libc::gid_t,
    mount_options: DirectMountOptions,
    session: NativeFuseSession<T>,
    handler: H,
}

pub struct MountedNativeSessionParts<L, T, H> {
    pub lifecycle: NativeMountLifecycle<L>,
    pub mount_point: String,
    pub fuse_fd: std::os::fd::RawFd,
    pub uid: libc::uid_t,
    pub gid: libc::gid_t,
    pub mount_options: DirectMountOptions,
    pub session: NativeFuseSession<T>,
    pub handler: H,
}

impl<L, T, H> MountedNativeSession<L, T, H> {
    pub fn new(parts: MountedNativeSessionParts<L, T, H>) -> Self {
        Self {
            lifecycle: parts.lifecycle,
            mount_point: parts.mount_point,
            fuse_fd: parts.fuse_fd,
            uid: parts.uid,
            gid: parts.gid,
            mount_options: parts.mount_options,
            session: parts.session,
            handler: parts.handler,
        }
    }
}

impl<L: FuseMountSystem, T: FuseTransport, H: FuseOperationHandler> MountedNativeSession<L, T, H> {
    pub fn run(mut self) -> Result<(NegotiatedSession, DispatchSummary), SessionError> {
        self.lifecycle.mount_direct(
            self.mount_point.clone(),
            self.fuse_fd,
            self.uid,
            self.gid,
            &self.mount_options,
        )?;
        let negotiated = self.session.handshake()?;
        let transport = self.session.into_transport();
        let mut dispatcher = NativeDispatcher::new(transport, self.handler);
        let dispatch = dispatcher.run_until_destroy()?;
        let (mut transport, _handler) = dispatcher.into_parts();
        transport.request_shutdown()?;
        self.lifecycle.unmount(&self.mount_point)?;
        Ok((negotiated, dispatch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::abi::FUSE_IN_HEADER_LEN;
    use crate::native::abi::ok_reply;
    use crate::native::dispatcher::FuseOperationHandler;
    use crate::native::lifecycle::{DirectMountRequest, LifecycleError};
    use crate::native::protocol::{FuseReply, FuseRequest};
    use crate::native::transport::FrameQueueTransport;

    fn init_frame(unique: u64, max_readahead: u32, capabilities: FuseCapabilities) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&7_u32.to_le_bytes());
        payload.extend_from_slice(&38_u32.to_le_bytes());
        payload.extend_from_slice(&max_readahead.to_le_bytes());
        payload.extend_from_slice(&capabilities.low_bits().to_le_bytes());
        payload.extend_from_slice(&capabilities.high_bits().to_le_bytes());

        let len = (FUSE_IN_HEADER_LEN + payload.len()) as u32;
        let mut frame = Vec::with_capacity(len as usize);
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(&26_u32.to_le_bytes());
        frame.extend_from_slice(&unique.to_le_bytes());
        frame.extend_from_slice(&1_u64.to_le_bytes());
        frame.extend_from_slice(&1000_u32.to_le_bytes());
        frame.extend_from_slice(&1000_u32.to_le_bytes());
        frame.extend_from_slice(&77_u32.to_le_bytes());
        frame.extend_from_slice(&0_u32.to_le_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    #[test]
    fn handshake_negotiates_init_and_records_session_state() {
        let kernel_caps = FuseCapabilities::ASYNC_READ
            | FuseCapabilities::BIG_WRITES
            | FuseCapabilities::OVER_IO_URING;
        let requested_caps = FuseCapabilities::ASYNC_READ
            | FuseCapabilities::AUTO_INVAL_DATA
            | FuseCapabilities::OVER_IO_URING;
        let transport = FrameQueueTransport::new([init_frame(0x44, 4 * 1024 * 1024, kernel_caps)]);
        let config = NativeSessionConfig {
            requested_capabilities: requested_caps,
            max_readahead: 1024 * 1024,
            max_write: 16 * 1024 * 1024,
            max_background: 50,
            congestion_threshold: 40,
            max_pages: 256,
        };
        let mut session = NativeFuseSession::new(transport, config);

        let negotiated = session.handshake().expect("INIT handshake should succeed");

        assert_eq!(
            negotiated,
            NegotiatedSession {
                protocol_major: 7,
                protocol_minor: 38,
                capabilities: FuseCapabilities::ASYNC_READ | FuseCapabilities::OVER_IO_URING,
                max_readahead: 1024 * 1024,
                max_write: 16 * 1024 * 1024,
                max_background: 50,
                congestion_threshold: 40,
                max_pages: 256,
            }
        );
        assert_eq!(session.negotiated(), Some(negotiated));
        assert_eq!(session.transport.sent_replies().len(), 1);
    }

    #[derive(Default)]
    struct RecordingLifecycleSystem {
        mounted: Vec<String>,
        unmounted: Vec<String>,
    }

    impl FuseMountSystem for RecordingLifecycleSystem {
        fn mount(&mut self, request: &DirectMountRequest) -> Result<(), LifecycleError> {
            self.mounted.push(request.mount_point.clone());
            Ok(())
        }

        fn unmount(&mut self, mount_point: &str) -> Result<(), LifecycleError> {
            self.unmounted.push(mount_point.to_owned());
            Ok(())
        }
    }

    #[derive(Default)]
    struct ReplyingHandler;

    impl FuseOperationHandler for ReplyingHandler {
        fn handle(&mut self, request: FuseRequest) -> FuseReply {
            FuseReply {
                unique: request.header.unique,
                bytes: ok_reply(request.header.unique, &[]),
            }
        }
    }

    #[test]
    fn mounted_native_session_mounts_handshakes_dispatches_and_unmounts() {
        let kernel_caps = FuseCapabilities::ASYNC_READ | FuseCapabilities::BIG_WRITES;
        let transport =
            FrameQueueTransport::new([init_frame(0x44, 4 * 1024 * 1024, kernel_caps), {
                let len = FUSE_IN_HEADER_LEN as u32;
                let mut frame = Vec::with_capacity(FUSE_IN_HEADER_LEN);
                frame.extend_from_slice(&len.to_le_bytes());
                frame.extend_from_slice(&38_u32.to_le_bytes());
                frame.extend_from_slice(&0x45_u64.to_le_bytes());
                frame.extend_from_slice(&1_u64.to_le_bytes());
                frame.extend_from_slice(&1000_u32.to_le_bytes());
                frame.extend_from_slice(&1000_u32.to_le_bytes());
                frame.extend_from_slice(&77_u32.to_le_bytes());
                frame.extend_from_slice(&0_u32.to_le_bytes());
                frame
            }]);
        let config = NativeSessionConfig {
            requested_capabilities: FuseCapabilities::ASYNC_READ,
            max_readahead: 1024 * 1024,
            max_write: 16 * 1024 * 1024,
            max_background: 50,
            congestion_threshold: 40,
            max_pages: 256,
        };
        let mounted = MountedNativeSession::new(MountedNativeSessionParts {
            lifecycle: NativeMountLifecycle::new(RecordingLifecycleSystem::default()),
            mount_point: "/mnt/studiofs".to_owned(),
            fuse_fd: 7,
            uid: 1000,
            gid: 1000,
            mount_options: DirectMountOptions {
                source: "studiofs".into(),
                fs_name: "studiofs-volume".into(),
                subtype: Some("studiofs".into()),
                read_only: false,
                allow_other: false,
                default_permissions: false,
            },
            session: NativeFuseSession::new(transport, config),
            handler: ReplyingHandler,
        });

        let (negotiated, dispatch) = mounted.run().expect("mounted session should run");

        assert_eq!(negotiated.capabilities, FuseCapabilities::ASYNC_READ);
        assert_eq!(
            dispatch,
            DispatchSummary {
                handled_requests: 1,
                saw_destroy: true,
            }
        );
    }
}
