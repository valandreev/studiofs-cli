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

const STUDIOFS_NATIVE_WRITEBACK_CACHE_ENV: &str = "STUDIOFS_NATIVE_WRITEBACK_CACHE";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Local policy for the native FUSE INIT negotiation.
pub struct NativeSessionConfig {
    /// Capabilities StudioFS is willing to enable when the kernel also supports them.
    pub requested_capabilities: FuseCapabilities,
    /// Maximum readahead advertised back to the kernel.
    pub max_readahead: u32,
    /// Maximum write payload size accepted by the session.
    pub max_write: u32,
    /// Maximum number of in-flight background requests requested from the kernel.
    pub max_background: u16,
    /// Background request threshold where the kernel should consider congestion.
    pub congestion_threshold: u16,
    /// Maximum number of memory pages accepted in one request.
    pub max_pages: u16,
}

impl NativeSessionConfig {
    /// Linux 6.x native-safe session profile.
    pub fn linux6_safe(writeback_cache: bool) -> Self {
        let mut requested_capabilities = FuseCapabilities::ASYNC_READ
            | FuseCapabilities::BIG_WRITES
            | FuseCapabilities::MAX_PAGES;
        if writeback_cache {
            requested_capabilities = requested_capabilities | FuseCapabilities::WRITEBACK_CACHE;
        }
        Self {
            requested_capabilities,
            max_readahead: 16 * 1024 * 1024,
            max_write: 1024 * 1024,
            max_background: 50,
            congestion_threshold: 40,
            max_pages: 256,
        }
    }

    /// Linux 6.x native-safe session profile using process environment toggles.
    pub fn linux6_safe_from_env() -> Self {
        Self::linux6_safe_from_env_value(
            std::env::var(STUDIOFS_NATIVE_WRITEBACK_CACHE_ENV)
                .ok()
                .as_deref(),
        )
    }

    fn linux6_safe_from_env_value(writeback_cache: Option<&str>) -> Self {
        Self::linux6_safe(writeback_cache.is_some_and(parse_native_bool_env_value))
    }
}

fn parse_native_bool_env_value(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Result of a completed FUSE INIT handshake.
pub struct NegotiatedSession {
    /// Kernel FUSE protocol major version.
    pub protocol_major: u32,
    /// Kernel FUSE protocol minor version.
    pub protocol_minor: u32,
    /// Capability bits accepted by both the kernel and StudioFS.
    pub capabilities: FuseCapabilities,
    /// Effective readahead limit after clamping local and kernel preferences.
    pub max_readahead: u32,
    /// Effective maximum write payload size.
    pub max_write: u32,
    /// Effective background request limit.
    pub max_background: u16,
    /// Effective congestion threshold for background requests.
    pub congestion_threshold: u16,
    /// Effective maximum request size in pages.
    pub max_pages: u16,
}

#[derive(Debug, Error)]
/// Errors produced while mounting, negotiating, or dispatching a native session.
pub enum SessionError {
    /// The transport ended before the kernel sent the mandatory INIT request.
    #[error("FUSE session ended before INIT")]
    MissingInit,
    /// The first request was not the mandatory FUSE INIT request.
    #[error("first FUSE request was {0:?}, expected INIT")]
    ExpectedInit(FuseOpcode),
    /// The transport failed while reading the INIT request or writing a reply.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// The request dispatcher failed after negotiation.
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
    /// The mount lifecycle failed while attaching or detaching the filesystem.
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
}

#[derive(Debug)]
/// Native FUSE session state before and after INIT negotiation.
///
/// This type owns the transport until negotiation completes. After a successful
/// handshake, callers can recover the transport and hand it to the dispatcher.
pub struct NativeFuseSession<T> {
    transport: T,
    config: NativeSessionConfig,
    negotiated: Option<NegotiatedSession>,
}

impl<T> NativeFuseSession<T> {
    /// Creates a session with no negotiated kernel state yet.
    pub fn new(transport: T, config: NativeSessionConfig) -> Self {
        Self {
            transport,
            config,
            negotiated: None,
        }
    }

    /// Returns the negotiated state if the INIT handshake has completed.
    pub fn negotiated(&self) -> Option<NegotiatedSession> {
        self.negotiated
    }

    /// Consumes the session and returns the underlying transport.
    pub fn into_transport(self) -> T {
        self.transport
    }
}

impl<T: FuseTransport> NativeFuseSession<T> {
    /// Performs the mandatory FUSE INIT handshake.
    ///
    /// The first kernel request must be INIT. The reply advertises the
    /// intersection of local and kernel capabilities and records the negotiated
    /// limits for the caller.
    pub fn handshake(&mut self) -> Result<NegotiatedSession, SessionError> {
        let request = self
            .transport
            .receive_request()
            .map_err(|error| {
                TransportError::Io(std::io::Error::other(format!("receive INIT: {error}")))
            })?
            .ok_or(SessionError::MissingInit)?;
        if request.opcode != FuseOpcode::Init {
            return Err(SessionError::ExpectedInit(request.opcode));
        }

        let init = FuseInitRequest::parse(&request.payload)
            .map_err(TransportError::from)
            .map_err(|error| {
                TransportError::Io(std::io::Error::other(format!("parse INIT: {error}")))
            })?;
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
        self.transport.send_reply(reply).map_err(|error| {
            TransportError::Io(std::io::Error::other(format!(
                "send INIT reply: {error}; kernel_caps={:?}; negotiated={negotiated:?}",
                init.capabilities
            )))
        })?;
        self.negotiated = Some(negotiated);
        Ok(negotiated)
    }
}

/// Direct-mounted native FUSE session with its lifecycle and dispatcher inputs.
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

/// Construction parts for `MountedNativeSession`.
pub struct MountedNativeSessionParts<L, T, H> {
    /// Mount lifecycle used to attach and detach the filesystem.
    pub lifecycle: NativeMountLifecycle<L>,
    /// Target path for the direct mount.
    pub mount_point: String,
    /// Already opened FUSE descriptor passed to the kernel mount request.
    pub fuse_fd: std::os::fd::RawFd,
    /// User id advertised to the kernel for permission checks.
    pub uid: libc::uid_t,
    /// Group id advertised to the kernel for permission checks.
    pub gid: libc::gid_t,
    /// Mount identity and permission options.
    pub mount_options: DirectMountOptions,
    /// Session that will perform INIT negotiation after the mount is active.
    pub session: NativeFuseSession<T>,
    /// Operation handler used by the dispatcher after negotiation.
    pub handler: H,
}

impl<L, T, H> MountedNativeSession<L, T, H> {
    /// Builds a mounted session from explicit lifecycle, session, and handler parts.
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
    /// Mounts, negotiates INIT, dispatches requests until DESTROY, and unmounts.
    pub fn run(mut self) -> Result<(NegotiatedSession, DispatchSummary), SessionError> {
        let mount_point = self.mount_point.clone();
        self.lifecycle.mount_direct(
            mount_point.clone(),
            self.fuse_fd,
            self.uid,
            self.gid,
            &self.mount_options,
        )?;

        let run_result = (|| {
            let negotiated = self.session.handshake()?;
            let transport = self.session.into_transport();
            let mut dispatcher = NativeDispatcher::new(transport, self.handler);
            let dispatch = dispatcher.run_until_destroy()?;
            let (mut transport, _handler) = dispatcher.into_parts();
            transport.request_shutdown()?;
            Ok((negotiated, dispatch))
        })();

        let unmount_result = self.lifecycle.unmount(&mount_point);
        match (run_result, unmount_result) {
            (Ok(summary), Ok(())) => Ok(summary),
            (Ok(_), Err(error)) => Err(error.into()),
            (Err(run_error), Err(unmount_error)) => {
                tracing::error!(
                    ?unmount_error,
                    "failed to unmount after native FUSE session error"
                );
                Err(run_error)
            }
            (Err(run_error), Ok(())) => Err(run_error),
        }
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
    use std::io;
    use std::sync::{Arc, Mutex};
    use tracing::span::{Attributes, Record};
    use tracing::{Event, Id, Level, Metadata, Subscriber};

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

    #[test]
    fn linux6_safe_config_does_not_request_writeback_cache_by_default() {
        let config = NativeSessionConfig::linux6_safe(false);

        assert!(
            !config
                .requested_capabilities
                .contains(FuseCapabilities::WRITEBACK_CACHE)
        );
    }

    #[test]
    fn linux6_safe_config_requests_max_pages_when_advertising_max_pages() {
        let config = NativeSessionConfig::linux6_safe(false);

        assert!(
            config
                .requested_capabilities
                .contains(FuseCapabilities::MAX_PAGES),
            "init_out.max_pages is only valid when FUSE_MAX_PAGES is negotiated"
        );
    }

    #[test]
    fn linux6_safe_config_bounds_max_write_by_max_pages() {
        let config = NativeSessionConfig::linux6_safe(false);

        assert!(
            config.max_write <= u32::from(config.max_pages) * 4096,
            "Linux rejects INIT replies where max_write exceeds max_pages * page_size"
        );
    }

    #[test]
    fn linux6_safe_config_can_opt_into_writeback_cache_profile() {
        let config = NativeSessionConfig::linux6_safe(true);

        assert!(
            config
                .requested_capabilities
                .contains(FuseCapabilities::WRITEBACK_CACHE)
        );
    }

    #[test]
    fn linux6_safe_env_profile_toggles_writeback_cache() {
        let disabled = NativeSessionConfig::linux6_safe_from_env_value(None);
        assert!(
            !disabled
                .requested_capabilities
                .contains(FuseCapabilities::WRITEBACK_CACHE)
        );

        let enabled = NativeSessionConfig::linux6_safe_from_env_value(Some("1"));
        assert!(
            enabled
                .requested_capabilities
                .contains(FuseCapabilities::WRITEBACK_CACHE)
        );
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

    #[derive(Clone, Default)]
    struct SharedRecordingLifecycleSystem {
        mounted: Arc<Mutex<Vec<String>>>,
        unmounted: Arc<Mutex<Vec<String>>>,
        fail_unmount: bool,
    }

    impl FuseMountSystem for SharedRecordingLifecycleSystem {
        fn mount(&mut self, request: &DirectMountRequest) -> Result<(), LifecycleError> {
            self.mounted
                .lock()
                .expect("mounted recorder should not be poisoned")
                .push(request.mount_point.clone());
            Ok(())
        }

        fn unmount(&mut self, mount_point: &str) -> Result<(), LifecycleError> {
            self.unmounted
                .lock()
                .expect("unmounted recorder should not be poisoned")
                .push(mount_point.to_owned());
            if self.fail_unmount {
                return Err(io::Error::other("test unmount failure").into());
            }
            Ok(())
        }
    }

    struct CountingErrorSubscriber {
        error_events: Arc<Mutex<usize>>,
    }

    impl Subscriber for CountingErrorSubscriber {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            *metadata.level() <= Level::ERROR
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            if *event.metadata().level() == Level::ERROR {
                *self
                    .error_events
                    .lock()
                    .expect("error event counter should not be poisoned") += 1;
            }
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    #[derive(Default)]
    struct ReplyingHandler;

    impl FuseOperationHandler for ReplyingHandler {
        fn handle(&mut self, request: FuseRequest) -> FuseReply {
            FuseReply::contiguous(request.header.unique, ok_reply(request.header.unique, &[]))
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

    #[test]
    fn mounted_native_session_unmounts_after_handshake_failure() {
        let lifecycle_system = SharedRecordingLifecycleSystem::default();
        let unmounted = lifecycle_system.unmounted.clone();
        let config = NativeSessionConfig {
            requested_capabilities: FuseCapabilities::ASYNC_READ,
            max_readahead: 1024 * 1024,
            max_write: 16 * 1024 * 1024,
            max_background: 50,
            congestion_threshold: 40,
            max_pages: 256,
        };
        let mounted = MountedNativeSession::new(MountedNativeSessionParts {
            lifecycle: NativeMountLifecycle::new(lifecycle_system),
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
            session: NativeFuseSession::new(FrameQueueTransport::new([]), config),
            handler: ReplyingHandler,
        });

        let result = mounted.run();

        assert!(matches!(result, Err(SessionError::MissingInit)));
        assert_eq!(
            unmounted
                .lock()
                .expect("unmounted recorder should not be poisoned")
                .as_slice(),
            ["/mnt/studiofs"]
        );
    }

    #[test]
    fn mounted_native_session_logs_unmount_failure_after_session_failure() {
        let lifecycle_system = SharedRecordingLifecycleSystem {
            fail_unmount: true,
            ..SharedRecordingLifecycleSystem::default()
        };
        let config = NativeSessionConfig {
            requested_capabilities: FuseCapabilities::ASYNC_READ,
            max_readahead: 1024 * 1024,
            max_write: 16 * 1024 * 1024,
            max_background: 50,
            congestion_threshold: 40,
            max_pages: 256,
        };
        let mounted = MountedNativeSession::new(MountedNativeSessionParts {
            lifecycle: NativeMountLifecycle::new(lifecycle_system),
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
            session: NativeFuseSession::new(FrameQueueTransport::new([]), config),
            handler: ReplyingHandler,
        });
        let error_events = Arc::new(Mutex::new(0));
        let subscriber = CountingErrorSubscriber {
            error_events: error_events.clone(),
        };

        let result = tracing::subscriber::with_default(subscriber, || mounted.run());

        assert!(matches!(result, Err(SessionError::MissingInit)));
        assert_eq!(
            *error_events
                .lock()
                .expect("error event counter should not be poisoned"),
            1
        );
    }
}
