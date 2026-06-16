//! Transport-independent FUSE protocol types.

use crate::native::abi::{
    AbiParseError, FUSE_IN_HEADER_LEN, FuseInHeader, ok_reply, parse_in_header,
};
use std::ops::BitOr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuseOpcode {
    Lookup,
    Getattr,
    Open,
    Read,
    Statfs,
    Release,
    Init,
    Opendir,
    Readdir,
    Destroy,
    Other(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseRequest {
    pub header: FuseInHeader,
    pub opcode: FuseOpcode,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseReply {
    pub unique: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseInitRequest {
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub capabilities: FuseCapabilities,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseInitReply {
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub capabilities: FuseCapabilities,
    pub max_background: u16,
    pub congestion_threshold: u16,
    pub max_write: u32,
    pub time_gran: u32,
    pub max_pages: u16,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FuseCapabilities {
    bits: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseCapabilityReport {
    pub requested: FuseCapabilities,
    pub kernel: FuseCapabilities,
    pub enabled: FuseCapabilities,
    pub disabled: FuseCapabilities,
    pub io_uring_available: bool,
    pub io_uring_enabled: bool,
    pub clone_fd_available: bool,
    pub clone_fd_enabled: bool,
}

impl FuseCapabilities {
    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    pub const ASYNC_READ: Self = Self { bits: 1 << 0 };
    pub const BIG_WRITES: Self = Self { bits: 1 << 5 };
    pub const AUTO_INVAL_DATA: Self = Self { bits: 1 << 12 };
    pub const READDIRPLUS: Self = Self { bits: 1 << 13 };
    pub const SPLICE_WRITE: Self = Self { bits: 1 << 7 };
    pub const SPLICE_MOVE: Self = Self { bits: 1 << 8 };
    pub const SPLICE_READ: Self = Self { bits: 1 << 9 };
    pub const CLONE_FD: Self = Self { bits: 1 << 28 };
    pub const OVER_IO_URING: Self = Self { bits: 1 << 50 };

    pub fn contains(self, capability: Self) -> bool {
        self.bits & capability.bits == capability.bits
    }

    pub fn low_bits(self) -> u32 {
        self.bits as u32
    }

    pub fn high_bits(self) -> u32 {
        (self.bits >> 32) as u32
    }

    pub fn negotiate(self, kernel: Self) -> Self {
        Self {
            bits: self.bits & kernel.bits,
        }
    }

    pub fn report_negotiation(self, kernel: Self) -> FuseCapabilityReport {
        let enabled = self.negotiate(kernel);
        FuseCapabilityReport {
            requested: self,
            kernel,
            enabled,
            disabled: self.difference(enabled),
            io_uring_available: kernel.contains(Self::OVER_IO_URING),
            io_uring_enabled: enabled.contains(Self::OVER_IO_URING),
            clone_fd_available: kernel.contains(Self::CLONE_FD),
            clone_fd_enabled: enabled.contains(Self::CLONE_FD),
        }
    }

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
            17 => Self::Statfs,
            18 => Self::Release,
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

        assert_eq!(
            request,
            FuseRequest {
                header: FuseInHeader {
                    len: 56,
                    opcode: 26,
                    unique: 0x1234,
                    nodeid: 1,
                    uid: 1000,
                    gid: 1000,
                    pid: 77,
                },
                opcode: FuseOpcode::Init,
                payload: payload.to_vec(),
            }
        );
    }

    #[test]
    fn opcode_table_decodes_sfs525_transport_level_request_set() {
        for (raw, expected) in [
            (1, FuseOpcode::Lookup),
            (3, FuseOpcode::Getattr),
            (14, FuseOpcode::Open),
            (15, FuseOpcode::Read),
            (17, FuseOpcode::Statfs),
            (18, FuseOpcode::Release),
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
            | FuseCapabilities::CLONE_FD
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
    fn capability_report_enumerates_io_uring_and_clone_fd_availability() {
        let requested = FuseCapabilities::ASYNC_READ | FuseCapabilities::OVER_IO_URING;
        let kernel = FuseCapabilities::ASYNC_READ
            | FuseCapabilities::CLONE_FD
            | FuseCapabilities::OVER_IO_URING;

        let report = requested.report_negotiation(kernel);

        assert_eq!(report.enabled, requested);
        assert_eq!(report.disabled, FuseCapabilities::empty());
        assert!(report.io_uring_available);
        assert!(report.io_uring_enabled);
        assert!(report.clone_fd_available);
        assert!(!report.clone_fd_enabled);
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
        payload.extend_from_slice(&(1_u32 << 18).to_le_bytes());

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
            capabilities: FuseCapabilities::ASYNC_READ | FuseCapabilities::BIG_WRITES,
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
            &(FuseCapabilities::ASYNC_READ | FuseCapabilities::BIG_WRITES)
                .low_bits()
                .to_le_bytes()
        );
        assert_eq!(&reply.bytes[32..34], &50_u16.to_le_bytes());
        assert_eq!(&reply.bytes[34..36], &40_u16.to_le_bytes());
        assert_eq!(&reply.bytes[36..40], &(16 * 1024 * 1024_u32).to_le_bytes());
        assert_eq!(&reply.bytes[40..44], &1_u32.to_le_bytes());
        assert_eq!(&reply.bytes[44..46], &256_u16.to_le_bytes());
    }
}
