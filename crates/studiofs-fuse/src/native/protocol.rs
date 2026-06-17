//! Transport-independent FUSE protocol types.

use crate::native::abi::{
    AbiParseError, FUSE_IN_HEADER_LEN, FuseInHeader, ok_reply, parse_in_header,
};
use std::ops::BitOr;

/// Linux FUSE operation codes understood at the native transport boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuseOpcode {
    /// `FUSE_LOOKUP`: resolve a child name under a directory node.
    Lookup,
    /// `FUSE_GETATTR`: fetch attributes for a node.
    Getattr,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseReply {
    /// Request id this reply answers.
    pub unique: u64,
    /// Complete `fuse_out_header` plus any operation-specific payload.
    pub bytes: Vec<u8>,
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

        FuseReply {
            unique,
            bytes: ok_reply(unique, &payload),
        }
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
            3 => Self::Getattr,
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
            38 => Self::Destroy,
            other => Self::Other(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            (3, FuseOpcode::Getattr),
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
            (38, FuseOpcode::Destroy),
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

        assert_eq!(&reply.bytes[0..4], &80_u32.to_le_bytes());
        assert_eq!(&reply.bytes[4..8], &0_i32.to_le_bytes());
        assert_eq!(&reply.bytes[8..16], &0x99_u64.to_le_bytes());
        assert_eq!(&reply.bytes[16..20], &7_u32.to_le_bytes());
        assert_eq!(&reply.bytes[20..24], &38_u32.to_le_bytes());
        assert_eq!(&reply.bytes[24..28], &(1024 * 1024_u32).to_le_bytes());
        assert_eq!(
            &reply.bytes[28..32],
            &(FuseCapabilities::ASYNC_READ
                | FuseCapabilities::BIG_WRITES
                | FuseCapabilities::OVER_IO_URING)
                .low_bits()
                .to_le_bytes()
        );
        assert_eq!(&reply.bytes[32..34], &50_u16.to_le_bytes());
        assert_eq!(&reply.bytes[34..36], &40_u16.to_le_bytes());
        assert_eq!(&reply.bytes[36..40], &(16 * 1024 * 1024_u32).to_le_bytes());
        assert_eq!(&reply.bytes[40..44], &1_u32.to_le_bytes());
        assert_eq!(&reply.bytes[44..46], &256_u16.to_le_bytes());
        assert_eq!(&reply.bytes[46..48], &0_u16.to_le_bytes());
        assert_eq!(
            &reply.bytes[48..52],
            &(FuseCapabilities::ASYNC_READ
                | FuseCapabilities::BIG_WRITES
                | FuseCapabilities::OVER_IO_URING)
                .high_bits()
                .to_le_bytes()
        );
    }
}
