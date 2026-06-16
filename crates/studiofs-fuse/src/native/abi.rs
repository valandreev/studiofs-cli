//! Linux FUSE ABI parsing and reply framing.

use thiserror::Error;

pub const FUSE_IN_HEADER_LEN: usize = 40;
pub const FUSE_OUT_HEADER_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseInHeader {
    pub len: u32,
    pub opcode: u32,
    pub unique: u64,
    pub nodeid: u64,
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AbiParseError {
    #[error("FUSE frame too short: expected at least {expected} bytes, got {actual}")]
    FrameTooShort { expected: usize, actual: usize },
}

pub fn parse_in_header(frame: &[u8]) -> Result<FuseInHeader, AbiParseError> {
    if frame.len() < FUSE_IN_HEADER_LEN {
        return Err(AbiParseError::FrameTooShort {
            expected: FUSE_IN_HEADER_LEN,
            actual: frame.len(),
        });
    }

    Ok(FuseInHeader {
        len: read_u32(frame, 0),
        opcode: read_u32(frame, 4),
        unique: read_u64(frame, 8),
        nodeid: read_u64(frame, 16),
        uid: read_u32(frame, 24),
        gid: read_u32(frame, 28),
        pid: read_u32(frame, 32),
    })
}

pub fn error_reply(unique: u64, errno: i32) -> [u8; FUSE_OUT_HEADER_LEN] {
    let mut reply = [0; FUSE_OUT_HEADER_LEN];
    reply[0..4].copy_from_slice(&(FUSE_OUT_HEADER_LEN as u32).to_le_bytes());
    reply[4..8].copy_from_slice(&(-errno).to_le_bytes());
    reply[8..16].copy_from_slice(&unique.to_le_bytes());
    reply
}

fn read_u32(frame: &[u8], offset: usize) -> u32 {
    let mut value = [0; 4];
    value.copy_from_slice(&frame[offset..offset + 4]);
    u32::from_le_bytes(value)
}

fn read_u64(frame: &[u8], offset: usize) -> u64 {
    let mut value = [0; 8];
    value.copy_from_slice(&frame[offset..offset + 8]);
    u64::from_le_bytes(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame_with_header(
        len: u32,
        opcode: u32,
        unique: u64,
        nodeid: u64,
        uid: u32,
        gid: u32,
        pid: u32,
    ) -> Vec<u8> {
        let mut frame = Vec::with_capacity(FUSE_IN_HEADER_LEN);
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(&opcode.to_le_bytes());
        frame.extend_from_slice(&unique.to_le_bytes());
        frame.extend_from_slice(&nodeid.to_le_bytes());
        frame.extend_from_slice(&uid.to_le_bytes());
        frame.extend_from_slice(&gid.to_le_bytes());
        frame.extend_from_slice(&pid.to_le_bytes());
        frame.extend_from_slice(&0_u32.to_le_bytes());
        frame
    }

    #[test]
    fn parse_in_header_reads_linux_fuse_header_fields() {
        let frame = frame_with_header(56, 26, 0xfeed_beef, 1, 1000, 1001, 42);

        let header = parse_in_header(&frame).expect("FUSE header should parse");

        assert_eq!(
            header,
            FuseInHeader {
                len: 56,
                opcode: 26,
                unique: 0xfeed_beef,
                nodeid: 1,
                uid: 1000,
                gid: 1001,
                pid: 42,
            }
        );
    }

    #[test]
    fn error_reply_builds_negative_errno_out_header_for_unique_request() {
        let reply = error_reply(0xfeed_beef, libc::ENOENT);

        assert_eq!(
            reply,
            [
                16, 0, 0, 0, 254, 255, 255, 255, 239, 190, 237, 254, 0, 0, 0, 0,
            ]
        );
    }
}
