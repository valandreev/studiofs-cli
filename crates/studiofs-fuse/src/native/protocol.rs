//! Transport-independent FUSE protocol types.

use crate::native::abi::{
    AbiParseError, FUSE_IN_HEADER_LEN, FUSE_OUT_HEADER_LEN, FuseInHeader, fuse_out_len_for_payload,
    ok_reply, parse_in_header,
};
use std::io::{self, IoSlice};
use std::ops::BitOr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;

/// Linux FUSE operation codes understood at the native transport boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuseOpcode {
    /// `FUSE_LOOKUP`: resolve a child name under a directory node.
    Lookup,
    /// `FUSE_FORGET`: drop kernel lookup references for an inode.
    Forget,
    /// `FUSE_GETATTR`: fetch attributes for a node.
    Getattr,
    /// `FUSE_SETATTR`: update attributes for a node.
    Setattr,
    /// `FUSE_OPEN`: open a regular file.
    Open,
    /// `FUSE_READ`: read file data from an open file handle or node.
    Read,
    /// `FUSE_WRITE`: write file data to an open file handle or node.
    Write,
    /// `FUSE_STATFS`: fetch filesystem-level capacity and limit information.
    Statfs,
    /// `FUSE_FSYNC`: synchronize dirty file data for a handle or inode.
    Fsync,
    /// `FUSE_RELEASE`: close a previously opened file handle.
    Release,
    /// `FUSE_FLUSH`: flush dirty file data for a handle or inode.
    Flush,
    /// `FUSE_INIT`: negotiate protocol version, limits, and capabilities.
    Init,
    /// `FUSE_OPENDIR`: open a directory stream.
    Opendir,
    /// `FUSE_READDIR`: read directory entries from an open directory stream.
    Readdir,
    /// `FUSE_RELEASEDIR`: close a previously opened directory stream.
    Releasedir,
    /// `FUSE_READDIRPLUS`: read directory entries with attributes.
    Readdirplus,
    /// `FUSE_BATCH_FORGET`: drop kernel lookup references for multiple inodes.
    BatchForget,
    /// `FUSE_MKDIR`: create a directory.
    Mkdir,
    /// `FUSE_CREATE`: create and open a regular file.
    Create,
    /// `FUSE_UNLINK`: remove a regular file name.
    Unlink,
    /// `FUSE_RMDIR`: remove an empty directory name.
    Rmdir,
    /// `FUSE_RENAME`: rename a namespace entry.
    Rename,
    /// `FUSE_DESTROY`: terminate the kernel connection.
    Destroy,
    /// Any opcode not yet modeled by the native bridge.
    Other(u32),
}

/// Transport-independent FUSE request envelope.
///
/// The dispatcher and operation layer consume this type instead of raw bytes so
/// classic `/dev/fuse` and future io_uring backends can share the same request
/// handling path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseRequest {
    /// Fixed kernel request header.
    pub header: FuseInHeader,
    /// Decoded opcode for routing.
    pub opcode: FuseOpcode,
    /// Operation-specific request bytes following [`FuseInHeader`].
    pub payload: Vec<u8>,
}

/// Fully framed FUSE reply ready for transport writes.
#[derive(Debug, Clone)]
pub struct FuseReply {
    /// Request id this reply answers.
    pub unique: u64,
    frame: FuseReplyFrame,
}

/// Stable local cache file range that a Linux transport may use for optimized read replies.
#[derive(Debug, Clone)]
pub struct FuseLocalCacheReadRange {
    /// Source local cache file descriptor.
    pub fd: RawFd,
    /// Byte offset inside the local cache file.
    pub file_offset: u64,
    /// Number of bytes available for this reply.
    pub len: u32,
    /// StudioFS content epoch validated by the VFS when the range was produced.
    pub content_epoch: u64,
    /// StudioFS invalidation generation validated by the VFS when the range was produced.
    pub invalidation_generation: u64,
    source: Option<Arc<OwnedFd>>,
}

impl FuseLocalCacheReadRange {
    /// Builds a range from a borrowed source fd.
    pub fn borrowed(fd: RawFd, file_offset: u64, len: u32, content_epoch: u64) -> Self {
        Self::borrowed_with_generation(fd, file_offset, len, content_epoch, 0)
    }

    /// Builds a range from a borrowed source fd and explicit invalidation generation.
    pub fn borrowed_with_generation(
        fd: RawFd,
        file_offset: u64,
        len: u32,
        content_epoch: u64,
        invalidation_generation: u64,
    ) -> Self {
        Self {
            fd,
            file_offset,
            len,
            content_epoch,
            invalidation_generation,
            source: None,
        }
    }

    /// Builds a range that owns a duplicated source fd before crossing API boundaries.
    pub fn try_dup(fd: RawFd, file_offset: u64, len: u32, content_epoch: u64) -> io::Result<Self> {
        Self::try_dup_with_generation(fd, file_offset, len, content_epoch, 0)
    }

    /// Builds a duplicated range with an explicit invalidation generation.
    pub fn try_dup_with_generation(
        fd: RawFd,
        file_offset: u64,
        len: u32,
        content_epoch: u64,
        invalidation_generation: u64,
    ) -> io::Result<Self> {
        let source = duplicate_fd(fd)?;
        Ok(Self {
            fd: source.as_raw_fd(),
            file_offset,
            len,
            content_epoch,
            invalidation_generation,
            source: Some(source),
        })
    }
}

/// Owned FUSE reply frame storage.
#[derive(Debug, Clone)]
pub enum FuseReplyFrame {
    /// No kernel reply should be written for this request.
    NoReply,
    /// Complete `fuse_out_header` plus any operation-specific payload.
    Contiguous(Vec<u8>),
    /// Separate `fuse_out_header` and data payload for vectored transport writes.
    Segmented {
        /// Fixed `fuse_out_header` bytes.
        header: [u8; FUSE_OUT_HEADER_LEN],
        /// Operation-specific payload.
        payload: Vec<u8>,
    },
    /// Optimized read reply backed by a stable local cache file range.
    LocalCacheRange {
        /// Fixed `fuse_out_header` bytes.
        header: [u8; FUSE_OUT_HEADER_LEN],
        /// Stable local cache range selected for this reply.
        range: FuseLocalCacheReadRange,
        /// Duplicated fd that keeps the local cache file alive until transport send.
        source: Arc<OwnedFd>,
    },
}

impl FuseReply {
    /// Builds a marker for FUSE requests such as `FORGET` that must not be replied to.
    pub fn no_reply(unique: u64) -> Self {
        Self {
            unique,
            frame: FuseReplyFrame::NoReply,
        }
    }

    /// Builds a reply from a complete contiguous frame.
    pub fn contiguous(unique: u64, bytes: Vec<u8>) -> Self {
        Self {
            unique,
            frame: FuseReplyFrame::Contiguous(bytes),
        }
    }

    /// Builds a successful data reply with separate header and payload storage.
    pub fn segmented_data(unique: u64, payload: Vec<u8>) -> Self {
        let mut header = [0; FUSE_OUT_HEADER_LEN];
        let len = fuse_out_len_for_payload(payload.len());
        header[0..4].copy_from_slice(&len.to_le_bytes());
        header[4..8].copy_from_slice(&0_i32.to_le_bytes());
        header[8..16].copy_from_slice(&unique.to_le_bytes());
        Self {
            unique,
            frame: FuseReplyFrame::Segmented { header, payload },
        }
    }

    /// Builds a successful read reply backed by a local cache file range.
    pub fn local_cache_range_data(unique: u64, range: FuseLocalCacheReadRange) -> io::Result<Self> {
        let source = match &range.source {
            Some(source) => Arc::clone(source),
            None => duplicate_fd(range.fd)?,
        };
        let mut header = [0; FUSE_OUT_HEADER_LEN];
        let len = fuse_out_len_for_payload(range.len as usize);
        header[0..4].copy_from_slice(&len.to_le_bytes());
        header[4..8].copy_from_slice(&0_i32.to_le_bytes());
        header[8..16].copy_from_slice(&unique.to_le_bytes());
        Ok(Self {
            unique,
            frame: FuseReplyFrame::LocalCacheRange {
                header,
                range,
                source,
            },
        })
    }

    /// Returns a contiguous copy of the reply frame.
    pub fn to_vec(&self) -> Vec<u8> {
        match &self.frame {
            FuseReplyFrame::NoReply => Vec::new(),
            FuseReplyFrame::Contiguous(bytes) => bytes.clone(),
            FuseReplyFrame::Segmented { header, payload } => {
                let mut bytes = Vec::with_capacity(header.len() + payload.len());
                bytes.extend_from_slice(header);
                bytes.extend_from_slice(payload);
                bytes
            }
            FuseReplyFrame::LocalCacheRange { header, .. } => header.to_vec(),
        }
    }

    /// Returns the `fuse_out_header.error` field when this reply has an output header.
    pub fn error_code(&self) -> Option<i32> {
        let header = match &self.frame {
            FuseReplyFrame::NoReply => return None,
            FuseReplyFrame::Contiguous(bytes) => bytes.get(..FUSE_OUT_HEADER_LEN)?,
            FuseReplyFrame::Segmented { header, .. } => header,
            FuseReplyFrame::LocalCacheRange { header, .. } => header,
        };
        let bytes = header.get(4..8)?;
        let mut raw = [0; 4];
        raw.copy_from_slice(bytes);
        Some(i32::from_le_bytes(raw))
    }

    /// Returns the total serialized frame length.
    pub fn frame_len(&self) -> usize {
        match &self.frame {
            FuseReplyFrame::NoReply => 0,
            FuseReplyFrame::Contiguous(bytes) => bytes.len(),
            FuseReplyFrame::Segmented { header, payload } => header.len() + payload.len(),
            FuseReplyFrame::LocalCacheRange { header, range, .. } => {
                header.len() + range.len as usize
            }
        }
    }

    /// Returns vectored slices for transport writeback without heap allocation.
    pub fn io_slices(&self) -> FuseReplyIoSlices<'_> {
        match &self.frame {
            FuseReplyFrame::NoReply => FuseReplyIoSlices::empty(),
            FuseReplyFrame::Contiguous(bytes) => FuseReplyIoSlices::one(IoSlice::new(bytes)),
            FuseReplyFrame::Segmented { header, payload } => {
                FuseReplyIoSlices::two(IoSlice::new(header), IoSlice::new(payload))
            }
            FuseReplyFrame::LocalCacheRange { header, .. } => {
                FuseReplyIoSlices::one(IoSlice::new(header))
            }
        }
    }

    /// Returns the already-framed FUSE reply header for an optimized range reply.
    pub fn local_cache_range_header(&self) -> Option<&[u8; FUSE_OUT_HEADER_LEN]> {
        match &self.frame {
            FuseReplyFrame::LocalCacheRange { header, .. } => Some(header),
            _ => None,
        }
    }

    /// Returns whether this request must be completed without kernel writeback.
    pub fn is_no_reply(&self) -> bool {
        matches!(self.frame, FuseReplyFrame::NoReply)
    }

    /// Returns a view of contiguous reply bytes when this reply owns one frame buffer.
    pub fn bytes(&self) -> Option<&[u8]> {
        match &self.frame {
            FuseReplyFrame::NoReply => None,
            FuseReplyFrame::Contiguous(bytes) => Some(bytes),
            FuseReplyFrame::Segmented { .. } => None,
            FuseReplyFrame::LocalCacheRange { .. } => None,
        }
    }

    /// Returns the optimized local-cache range selected for this reply, if any.
    pub fn local_cache_range(&self) -> Option<FuseLocalCacheReadRange> {
        match &self.frame {
            FuseReplyFrame::LocalCacheRange { range, .. } => Some(range.clone()),
            _ => None,
        }
    }

    /// Returns the duplicated local-cache fd owned by this reply, if any.
    pub fn local_cache_source_fd(&self) -> Option<RawFd> {
        match &self.frame {
            FuseReplyFrame::LocalCacheRange { source, .. } => Some(source.as_raw_fd()),
            _ => None,
        }
    }
}

#[cfg(unix)]
fn duplicate_fd(fd: RawFd) -> io::Result<Arc<OwnedFd>> {
    // SAFETY: `fcntl(F_DUPFD_CLOEXEC)` duplicates an open fd without taking
    // ownership of the caller's descriptor.
    let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicated < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: `duplicated` is a fresh fd returned by `fcntl` and ownership
        // moves into `OwnedFd`.
        Ok(Arc::new(unsafe { OwnedFd::from_raw_fd(duplicated) }))
    }
}

/// Fixed-size vectored reply slices.
pub struct FuseReplyIoSlices<'a> {
    slices: [IoSlice<'a>; 2],
    len: usize,
}

impl<'a> FuseReplyIoSlices<'a> {
    fn empty() -> Self {
        Self {
            slices: [IoSlice::new(&[]), IoSlice::new(&[])],
            len: 0,
        }
    }

    fn one(first: IoSlice<'a>) -> Self {
        Self {
            slices: [first, IoSlice::new(&[])],
            len: 1,
        }
    }

    fn two(first: IoSlice<'a>, second: IoSlice<'a>) -> Self {
        Self {
            slices: [first, second],
            len: 2,
        }
    }

    /// Returns active slices.
    pub fn as_slice(&self) -> &[IoSlice<'a>] {
        &self.slices[..self.len]
    }

    /// Returns active slice count.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns whether there are no active slices.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Parsed `FUSE_INIT` request payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseInitRequest {
    /// Kernel protocol major version.
    pub major: u32,
    /// Kernel protocol minor version.
    pub minor: u32,
    /// Maximum readahead requested by the kernel.
    pub max_readahead: u32,
    /// Capability bits advertised by the kernel.
    pub capabilities: FuseCapabilities,
}

/// Negotiated `FUSE_INIT` response payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseInitReply {
    /// Protocol major version accepted by the daemon.
    pub major: u32,
    /// Protocol minor version accepted by the daemon.
    pub minor: u32,
    /// Readahead limit accepted by the daemon.
    pub max_readahead: u32,
    /// Capability bits enabled for this session.
    pub capabilities: FuseCapabilities,
    /// Maximum number of background requests the daemon accepts.
    pub max_background: u16,
    /// Kernel congestion threshold for background requests.
    pub congestion_threshold: u16,
    /// Maximum write size accepted by the daemon.
    pub max_write: u32,
    /// Attribute timestamp granularity in nanoseconds.
    pub time_gran: u32,
    /// Maximum number of pages per request for kernels that support it.
    pub max_pages: u16,
}

/// Set of Linux FUSE capability bits represented as a 64-bit mask.
///
/// The kernel splits these over `flags` and `flags2` in INIT payloads. This
/// wrapper keeps negotiation code independent of that wire split.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FuseCapabilities {
    bits: u64,
}

/// Result of comparing requested daemon capabilities with kernel support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseCapabilityReport {
    /// Capability mask the daemon asked to enable.
    pub requested: FuseCapabilities,
    /// Capability mask advertised by the kernel.
    pub kernel: FuseCapabilities,
    /// Intersection that will be enabled for the session.
    pub enabled: FuseCapabilities,
    /// Requested bits that were not enabled because the kernel did not offer them.
    pub disabled: FuseCapabilities,
    /// Whether the kernel advertised FUSE-over-io_uring support.
    pub io_uring_available: bool,
    /// Whether FUSE-over-io_uring was both requested and enabled.
    pub io_uring_enabled: bool,
}

impl FuseCapabilities {
    /// Empty capability mask.
    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    /// `FUSE_CAP_ASYNC_READ`: allow the kernel to issue asynchronous reads.
    pub const ASYNC_READ: Self = Self { bits: 1 << 0 };
    /// `FUSE_CAP_BIG_WRITES`: allow writes larger than the legacy 4 KiB limit.
    pub const BIG_WRITES: Self = Self { bits: 1 << 5 };
    /// `FUSE_CAP_AUTO_INVAL_DATA`: kernel invalidates cached data on mtime/size changes.
    pub const AUTO_INVAL_DATA: Self = Self { bits: 1 << 12 };
    /// `FUSE_CAP_READDIRPLUS`: allow directory reads to include entry attributes.
    pub const READDIRPLUS: Self = Self { bits: 1 << 13 };
    /// `FUSE_CAP_WRITEBACK_CACHE`: allow kernel page-cache writeback.
    pub const WRITEBACK_CACHE: Self = Self { bits: 1 << 16 };
    /// `FUSE_MAX_PAGES`: `fuse_init_out.max_pages` contains the max request pages.
    pub const MAX_PAGES: Self = Self { bits: 1 << 22 };
    /// `FUSE_CAP_SPLICE_WRITE`: allow splice-based writes from the kernel.
    pub const SPLICE_WRITE: Self = Self { bits: 1 << 7 };
    /// `FUSE_CAP_SPLICE_MOVE`: allow moving pages with splice where supported.
    pub const SPLICE_MOVE: Self = Self { bits: 1 << 8 };
    /// `FUSE_CAP_SPLICE_READ`: allow splice-based reads to the kernel.
    pub const SPLICE_READ: Self = Self { bits: 1 << 9 };
    /// `FUSE_OVER_IO_URING`: kernel advertises FUSE request delivery over io_uring.
    pub const OVER_IO_URING: Self = Self { bits: 1 << 41 };

    /// Returns true when all bits in `capability` are present.
    pub fn contains(self, capability: Self) -> bool {
        self.bits & capability.bits == capability.bits
    }

    /// Lower 32 capability bits as stored in the INIT `flags` field.
    pub fn low_bits(self) -> u32 {
        self.bits as u32
    }

    /// Upper 32 capability bits as stored in the INIT `flags2` field.
    pub fn high_bits(self) -> u32 {
        (self.bits >> 32) as u32
    }

    /// Enables only capabilities requested by the daemon and offered by the kernel.
    pub fn negotiate(self, kernel: Self) -> Self {
        Self {
            bits: self.bits & kernel.bits,
        }
    }

    /// Builds a structured report for telemetry and tests around negotiation.
    pub fn report_negotiation(self, kernel: Self) -> FuseCapabilityReport {
        let enabled = self.negotiate(kernel);
        FuseCapabilityReport {
            requested: self,
            kernel,
            enabled,
            disabled: self.difference(enabled),
            io_uring_available: kernel.contains(Self::OVER_IO_URING),
            io_uring_enabled: enabled.contains(Self::OVER_IO_URING),
        }
    }

    /// Combines the kernel INIT `flags` and `flags2` fields into one mask.
    pub fn from_kernel_flags(flags: u32, flags2: u32) -> Self {
        Self {
            bits: u64::from(flags) | (u64::from(flags2) << 32),
        }
    }

    fn difference(self, other: Self) -> Self {
        Self {
            bits: self.bits & !other.bits,
        }
    }
}

impl FuseInitRequest {
    /// Parses a `FUSE_INIT` request payload.
    ///
    /// Kernels before the extended flags field are accepted by treating
    /// missing `flags2` as zero.
    pub fn parse(payload: &[u8]) -> Result<Self, AbiParseError> {
        const FUSE_INIT_IN_MIN_LEN: usize = 16;
        if payload.len() < FUSE_INIT_IN_MIN_LEN {
            return Err(AbiParseError::FrameTooShort {
                expected: FUSE_INIT_IN_MIN_LEN,
                actual: payload.len(),
            });
        }

        let flags2 = if payload.len() >= 20 {
            read_u32(payload, 16)
        } else {
            0
        };

        Ok(Self {
            major: read_u32(payload, 0),
            minor: read_u32(payload, 4),
            max_readahead: read_u32(payload, 8),
            capabilities: FuseCapabilities::from_kernel_flags(read_u32(payload, 12), flags2),
        })
    }
}

impl FuseInitReply {
    /// Serializes this INIT reply into a complete FUSE response frame.
    pub fn into_reply(self, unique: u64) -> FuseReply {
        const FUSE_INIT_OUT_PAYLOAD_LEN: usize = 64;
        let mut payload = Vec::with_capacity(FUSE_INIT_OUT_PAYLOAD_LEN);
        payload.extend_from_slice(&self.major.to_le_bytes());
        payload.extend_from_slice(&self.minor.to_le_bytes());
        payload.extend_from_slice(&self.max_readahead.to_le_bytes());
        payload.extend_from_slice(&self.capabilities.low_bits().to_le_bytes());
        payload.extend_from_slice(&self.max_background.to_le_bytes());
        payload.extend_from_slice(&self.congestion_threshold.to_le_bytes());
        payload.extend_from_slice(&self.max_write.to_le_bytes());
        payload.extend_from_slice(&self.time_gran.to_le_bytes());
        payload.extend_from_slice(&self.max_pages.to_le_bytes());
        payload.extend_from_slice(&0_u16.to_le_bytes());
        payload.extend_from_slice(&self.capabilities.high_bits().to_le_bytes());
        payload.resize(FUSE_INIT_OUT_PAYLOAD_LEN, 0);

        FuseReply::contiguous(unique, ok_reply(unique, &payload))
    }
}

fn read_u32(frame: &[u8], offset: usize) -> u32 {
    let mut value = [0; 4];
    value.copy_from_slice(&frame[offset..offset + 4]);
    u32::from_le_bytes(value)
}

impl BitOr for FuseCapabilities {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self {
            bits: self.bits | rhs.bits,
        }
    }
}

impl FuseRequest {
    /// Parses a raw request frame into a typed envelope.
    pub fn from_frame(frame: &[u8]) -> Result<Self, AbiParseError> {
        let header = parse_in_header(frame)?;
        Ok(Self {
            header,
            opcode: FuseOpcode::from_raw(header.opcode),
            payload: frame[FUSE_IN_HEADER_LEN..].to_vec(),
        })
    }
}

impl FuseOpcode {
    fn from_raw(opcode: u32) -> Self {
        match opcode {
            1 => Self::Lookup,
            2 => Self::Forget,
            3 => Self::Getattr,
            4 => Self::Setattr,
            9 => Self::Mkdir,
            10 => Self::Unlink,
            11 => Self::Rmdir,
            12 => Self::Rename,
            14 => Self::Open,
            15 => Self::Read,
            16 => Self::Write,
            17 => Self::Statfs,
            18 => Self::Release,
            20 => Self::Fsync,
            25 => Self::Flush,
            26 => Self::Init,
            27 => Self::Opendir,
            28 => Self::Readdir,
            29 => Self::Releasedir,
            35 => Self::Create,
            38 => Self::Destroy,
            42 => Self::BatchForget,
            44 => Self::Readdirplus,
            other => Self::Other(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::abi::error_reply;

    fn frame_with_payload(opcode: u32, unique: u64, payload: &[u8]) -> Vec<u8> {
        let len = (FUSE_IN_HEADER_LEN + payload.len()) as u32;
        let mut frame = Vec::with_capacity(len as usize);
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(&opcode.to_le_bytes());
        frame.extend_from_slice(&unique.to_le_bytes());
        frame.extend_from_slice(&1_u64.to_le_bytes());
        frame.extend_from_slice(&1000_u32.to_le_bytes());
        frame.extend_from_slice(&1000_u32.to_le_bytes());
        frame.extend_from_slice(&77_u32.to_le_bytes());
        frame.extend_from_slice(&0_u32.to_le_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    #[test]
    fn request_from_frame_preserves_envelope_and_decodes_init_opcode() {
        let payload = [7, 0, 0, 0, 38, 0, 0, 0, 0, 0, 2, 0, 1, 0, 0, 0];
        let frame = frame_with_payload(26, 0x1234, &payload);

        let request = FuseRequest::from_frame(&frame).expect("INIT frame should parse");

        assert_eq!(request.header.len, 56);
        assert_eq!(request.header.opcode, 26);
        assert_eq!(request.header.unique, 0x1234);
        assert_eq!(request.header.nodeid, 1);
        assert_eq!(request.header.uid, 1000);
        assert_eq!(request.header.gid, 1000);
        assert_eq!(request.header.pid, 77);
        assert_eq!(request.opcode, FuseOpcode::Init);
        assert_eq!(request.payload, payload);
    }

    #[test]
    fn opcode_table_decodes_sfs525_transport_level_request_set() {
        for (raw, expected) in [
            (1, FuseOpcode::Lookup),
            (2, FuseOpcode::Forget),
            (3, FuseOpcode::Getattr),
            (4, FuseOpcode::Setattr),
            (9, FuseOpcode::Mkdir),
            (10, FuseOpcode::Unlink),
            (11, FuseOpcode::Rmdir),
            (12, FuseOpcode::Rename),
            (14, FuseOpcode::Open),
            (15, FuseOpcode::Read),
            (16, FuseOpcode::Write),
            (17, FuseOpcode::Statfs),
            (18, FuseOpcode::Release),
            (20, FuseOpcode::Fsync),
            (25, FuseOpcode::Flush),
            (26, FuseOpcode::Init),
            (27, FuseOpcode::Opendir),
            (28, FuseOpcode::Readdir),
            (29, FuseOpcode::Releasedir),
            (35, FuseOpcode::Create),
            (38, FuseOpcode::Destroy),
            (42, FuseOpcode::BatchForget),
            (44, FuseOpcode::Readdirplus),
        ] {
            assert_eq!(FuseOpcode::from_raw(raw), expected);
        }
    }

    #[test]
    fn capability_negotiation_enables_only_requested_kernel_supported_flags() {
        let requested = FuseCapabilities::ASYNC_READ
            | FuseCapabilities::BIG_WRITES
            | FuseCapabilities::OVER_IO_URING;
        let kernel = FuseCapabilities::ASYNC_READ
            | FuseCapabilities::AUTO_INVAL_DATA
            | FuseCapabilities::OVER_IO_URING;

        let enabled = requested.negotiate(kernel);

        assert_eq!(
            enabled,
            FuseCapabilities::ASYNC_READ | FuseCapabilities::OVER_IO_URING
        );
    }

    #[test]
    fn io_uring_capability_uses_linux_uapi_bit_41() {
        assert_eq!(FuseCapabilities::OVER_IO_URING.low_bits(), 0);
        assert_eq!(FuseCapabilities::OVER_IO_URING.high_bits(), 1 << 9);
    }

    #[test]
    fn capability_report_enumerates_io_uring_availability() {
        let requested = FuseCapabilities::ASYNC_READ | FuseCapabilities::OVER_IO_URING;
        let kernel = FuseCapabilities::ASYNC_READ | FuseCapabilities::OVER_IO_URING;

        let report = requested.report_negotiation(kernel);

        assert_eq!(report.enabled, requested);
        assert_eq!(report.disabled, FuseCapabilities::empty());
        assert!(report.io_uring_available);
        assert!(report.io_uring_enabled);
    }

    #[test]
    fn local_cache_range_data_returns_error_for_invalid_borrowed_fd() {
        let result = FuseReply::local_cache_range_data(
            0x55,
            FuseLocalCacheReadRange::borrowed(-1, 0, 4096, 7),
        );

        assert!(result.is_err());
    }

    #[test]
    fn fuse_reply_error_code_reads_fuse_out_header_errno() {
        let reply = FuseReply::contiguous(0x55, error_reply(0x55, libc::EIO).to_vec());

        assert_eq!(reply.error_code(), Some(-libc::EIO));
    }

    #[test]
    fn fuse_reply_error_code_avoids_panicking_header_slices() {
        let source = include_str!("protocol.rs");
        let body = source
            .split("pub fn error_code(&self)")
            .nth(1)
            .and_then(|rest| rest.split("pub fn frame_len(&self)").next())
            .expect("error_code body should be present");

        assert!(!body.contains("expect("));
        assert!(body.contains(".get(4..8)"));
    }

    #[test]
    fn init_request_parse_reads_protocol_limits_and_split_capability_flags() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&7_u32.to_le_bytes());
        payload.extend_from_slice(&38_u32.to_le_bytes());
        payload.extend_from_slice(&(2 * 1024 * 1024_u32).to_le_bytes());
        payload.extend_from_slice(
            &(FuseCapabilities::ASYNC_READ | FuseCapabilities::BIG_WRITES)
                .low_bits()
                .to_le_bytes(),
        );
        payload.extend_from_slice(&FuseCapabilities::OVER_IO_URING.high_bits().to_le_bytes());

        let init = FuseInitRequest::parse(&payload).expect("INIT payload should parse");

        assert_eq!(
            init,
            FuseInitRequest {
                major: 7,
                minor: 38,
                max_readahead: 2 * 1024 * 1024,
                capabilities: FuseCapabilities::ASYNC_READ
                    | FuseCapabilities::BIG_WRITES
                    | FuseCapabilities::OVER_IO_URING,
            }
        );
    }

    #[test]
    fn init_reply_serializes_negotiated_payload_inside_ok_reply_header() {
        let init = FuseInitReply {
            major: 7,
            minor: 38,
            max_readahead: 1024 * 1024,
            capabilities: FuseCapabilities::ASYNC_READ
                | FuseCapabilities::BIG_WRITES
                | FuseCapabilities::OVER_IO_URING,
            max_background: 50,
            congestion_threshold: 40,
            max_write: 16 * 1024 * 1024,
            time_gran: 1,
            max_pages: 256,
        };

        let reply = init.into_reply(0x99);
        let bytes = reply.to_vec();

        assert_eq!(&bytes[0..4], &80_u32.to_le_bytes());
        assert_eq!(&bytes[4..8], &0_i32.to_le_bytes());
        assert_eq!(&bytes[8..16], &0x99_u64.to_le_bytes());
        assert_eq!(&bytes[16..20], &7_u32.to_le_bytes());
        assert_eq!(&bytes[20..24], &38_u32.to_le_bytes());
        assert_eq!(&bytes[24..28], &(1024 * 1024_u32).to_le_bytes());
        assert_eq!(
            &bytes[28..32],
            &(FuseCapabilities::ASYNC_READ
                | FuseCapabilities::BIG_WRITES
                | FuseCapabilities::OVER_IO_URING)
                .low_bits()
                .to_le_bytes()
        );
        assert_eq!(&bytes[32..34], &50_u16.to_le_bytes());
        assert_eq!(&bytes[34..36], &40_u16.to_le_bytes());
        assert_eq!(&bytes[36..40], &(16 * 1024 * 1024_u32).to_le_bytes());
        assert_eq!(&bytes[40..44], &1_u32.to_le_bytes());
        assert_eq!(&bytes[44..46], &256_u16.to_le_bytes());
        assert_eq!(&bytes[46..48], &0_u16.to_le_bytes());
        assert_eq!(
            &bytes[48..52],
            &(FuseCapabilities::ASYNC_READ
                | FuseCapabilities::BIG_WRITES
                | FuseCapabilities::OVER_IO_URING)
                .high_bits()
                .to_le_bytes()
        );
    }

    #[test]
    #[should_panic(expected = "FUSE reply length exceeds u32 frame limit")]
    fn segmented_reply_length_panics_when_payload_exceeds_u32_frame_limit() {
        let _ = fuse_out_len_for_payload(usize::MAX);
    }
}
