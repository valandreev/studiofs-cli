//! Transport-independent FUSE protocol types.

use crate::native::abi::{AbiParseError, FUSE_IN_HEADER_LEN, FuseInHeader, parse_in_header};
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FuseCapabilities {
    bits: u64,
}

impl FuseCapabilities {
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

    pub fn negotiate(self, kernel: Self) -> Self {
        Self {
            bits: self.bits & kernel.bits,
        }
    }
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
}
