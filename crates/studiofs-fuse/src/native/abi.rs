//! Linux FUSE ABI parsing and reply framing.

use thiserror::Error;

/// Size in bytes of Linux `struct fuse_in_header`.
///
/// The native bridge parses this prefix from every request frame read from
/// `/dev/fuse`. Keeping the size explicit makes ABI layout assumptions visible
/// in tests and transport code.
pub const FUSE_IN_HEADER_LEN: usize = 40;

/// Size in bytes of Linux `struct fuse_out_header`.
///
/// Every response written to `/dev/fuse` starts with this header before any
/// operation-specific payload bytes.
pub const FUSE_OUT_HEADER_LEN: usize = 16;

/// Parsed Linux FUSE request header.
///
/// This is the transport-neutral representation of the fixed request prefix
/// supplied by the kernel. It intentionally stores only the common envelope;
/// operation-specific bytes remain in the request payload.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseInHeader {
    /// Total request frame length, including this header.
    pub len: u32,
    /// Raw Linux FUSE opcode number.
    pub opcode: u32,
    /// Kernel-assigned request id that must be echoed in the reply header.
    pub unique: u64,
    /// FUSE node id the request targets.
    pub nodeid: u64,
    /// User id of the process that triggered the request.
    pub uid: u32,
    /// Group id of the process that triggered the request.
    pub gid: u32,
    /// Process id of the task that triggered the request.
    pub pid: u32,
    _padding: u32,
}

#[cfg(test)]
impl FuseInHeader {
    pub(crate) fn new(
        len: u32,
        opcode: u32,
        unique: u64,
        nodeid: u64,
        uid: u32,
        gid: u32,
        pid: u32,
    ) -> Self {
        Self {
            len,
            opcode,
            unique,
            nodeid,
            uid,
            gid,
            pid,
            _padding: 0,
        }
    }
}

/// Error returned when a raw FUSE frame cannot be decoded safely.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AbiParseError {
    /// The buffer did not contain enough bytes for the expected ABI structure.
    #[error("FUSE frame too short: expected at least {expected} bytes, got {actual}")]
    FrameTooShort {
        /// Minimum number of bytes required for the structure being parsed.
        expected: usize,
        /// Number of bytes actually present in the supplied frame.
        actual: usize,
    },
}

/// Parses the fixed `fuse_in_header` prefix from a raw kernel request frame.
///
/// The parser is little-endian because Linux FUSE frames use the native kernel
/// ABI on little-endian targets supported by StudioFS today.
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
        _padding: read_u32(frame, 36),
    })
}

/// Builds an error-only FUSE reply header for `unique`.
///
/// Linux expects negative errno values in `fuse_out_header.error`; callers pass
/// a positive libc errno such as `libc::ENOENT`.
pub fn error_reply(unique: u64, errno: i32) -> [u8; FUSE_OUT_HEADER_LEN] {
    let mut reply = [0; FUSE_OUT_HEADER_LEN];
    reply[0..4].copy_from_slice(&(FUSE_OUT_HEADER_LEN as u32).to_le_bytes());
    reply[4..8].copy_from_slice(&errno.wrapping_neg().to_le_bytes());
    reply[8..16].copy_from_slice(&unique.to_le_bytes());
    reply
}

/// Builds a successful FUSE reply containing the fixed header and `payload`.
///
/// The returned vector is ready to write to the active FUSE transport.
pub fn ok_reply(unique: u64, payload: &[u8]) -> Vec<u8> {
    let len = fuse_out_len_for_payload(payload.len());
    let mut reply = Vec::with_capacity(len as usize);
    reply.extend_from_slice(&len.to_le_bytes());
    reply.extend_from_slice(&0_i32.to_le_bytes());
    reply.extend_from_slice(&unique.to_le_bytes());
    reply.extend_from_slice(payload);
    reply
}

pub(crate) fn fuse_out_len_for_payload(payload_len: usize) -> u32 {
    let len = FUSE_OUT_HEADER_LEN
        .checked_add(payload_len)
        .expect("FUSE reply length exceeds u32 frame limit");
    u32::try_from(len).expect("FUSE reply length exceeds u32 frame limit")
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
    fn abi_header_lengths_match_linux_fuse_layout() {
        assert_eq!(std::mem::size_of::<FuseInHeader>(), FUSE_IN_HEADER_LEN);
        assert_eq!(FUSE_OUT_HEADER_LEN, 16);
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
                _padding: 0,
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

    #[test]
    fn error_reply_wraps_min_errno_without_panicking() {
        let reply = error_reply(0xfeed_beef, i32::MIN);

        assert_eq!(&reply[4..8], &i32::MIN.to_le_bytes());
    }

    #[test]
    #[should_panic(expected = "FUSE reply length exceeds u32 frame limit")]
    fn fuse_out_len_for_payload_panics_when_payload_exceeds_u32_frame_limit() {
        let _ = fuse_out_len_for_payload(usize::MAX);
    }
}
