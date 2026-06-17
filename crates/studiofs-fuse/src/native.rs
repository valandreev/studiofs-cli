//! Linux-native FUSE bridge building blocks.

pub mod abi;
pub mod dispatcher;
pub mod lifecycle;
pub mod protocol;
pub mod reply;
pub mod session;
pub mod transport;
pub mod vfs_adapter;

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::native::abi::{FUSE_IN_HEADER_LEN, FUSE_OUT_HEADER_LEN, FuseInHeader};
    use crate::native::dispatcher::FuseOperationHandler;
    use crate::native::protocol::{FuseOpcode, FuseRequest};
    use crate::native::vfs_adapter::{LinuxNativeBridge, NativeAttr, NativeFileType, NativeVfs};

    #[derive(Debug)]
    struct RecordingNativeVfs {
        calls: Mutex<Vec<(u64, Vec<u8>)>>,
        attr: NativeAttr,
    }

    impl NativeVfs for RecordingNativeVfs {
        fn lookup(&self, parent: u64, name: &[u8]) -> Result<NativeAttr, i32> {
            self.calls
                .lock()
                .expect("lookup calls lock should not be poisoned")
                .push((parent, name.to_vec()));
            Ok(self.attr)
        }

        fn getattr(&self, _ino: u64) -> Result<NativeAttr, i32> {
            Ok(self.attr)
        }
    }

    fn lookup_request(parent: u64, name: &[u8]) -> FuseRequest {
        let mut payload = Vec::from(name);
        payload.push(0);
        FuseRequest {
            header: FuseInHeader::new(
                (FUSE_IN_HEADER_LEN + payload.len()) as u32,
                1,
                0xfeed_beef,
                parent,
                1000,
                1000,
                77,
            ),
            opcode: FuseOpcode::Lookup,
            payload,
        }
    }

    fn getattr_request(ino: u64) -> FuseRequest {
        FuseRequest {
            header: FuseInHeader::new(FUSE_IN_HEADER_LEN as u32, 3, 0xabc, ino, 1000, 1000, 77),
            opcode: FuseOpcode::Getattr,
            payload: Vec::new(),
        }
    }

    fn reply_error(reply: &[u8]) -> i32 {
        let mut raw = [0; 4];
        raw.copy_from_slice(&reply[4..8]);
        i32::from_le_bytes(raw)
    }

    fn entry_reply_nodeid(reply: &[u8]) -> u64 {
        let mut raw = [0; 8];
        raw.copy_from_slice(&reply[FUSE_OUT_HEADER_LEN..FUSE_OUT_HEADER_LEN + 8]);
        u64::from_le_bytes(raw)
    }

    fn attr_reply_size(reply: &[u8]) -> u64 {
        let attr_payload_offset = FUSE_OUT_HEADER_LEN + 16;
        let mut raw = [0; 8];
        raw.copy_from_slice(&reply[attr_payload_offset + 8..attr_payload_offset + 16]);
        u64::from_le_bytes(raw)
    }

    #[test]
    fn native_bridge_lookup_calls_vfs_and_returns_entry_reply() {
        let vfs = Arc::new(RecordingNativeVfs {
            calls: Mutex::new(Vec::new()),
            attr: NativeAttr {
                ino: 42,
                size: 4096,
                kind: NativeFileType::RegularFile,
                perm: 0o644,
                uid: 1000,
                gid: 1000,
            },
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(lookup_request(1, b"alpha.txt"));
        let reply = reply
            .bytes()
            .expect("lookup entry reply should be contiguous");

        assert_eq!(
            vfs.calls
                .lock()
                .expect("lookup calls lock should not be poisoned")
                .as_slice(),
            &[(1, b"alpha.txt".to_vec())]
        );
        assert_eq!(reply_error(reply), 0);
        assert_eq!(entry_reply_nodeid(reply), 42);
    }

    #[test]
    fn native_bridge_getattr_calls_vfs_and_returns_attr_reply() {
        let vfs = Arc::new(RecordingNativeVfs {
            calls: Mutex::new(Vec::new()),
            attr: NativeAttr {
                ino: 99,
                size: 8192,
                kind: NativeFileType::RegularFile,
                perm: 0o600,
                uid: 1000,
                gid: 1000,
            },
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(getattr_request(99));
        let reply = reply.bytes().expect("getattr reply should be contiguous");

        assert_eq!(reply_error(reply), 0);
        assert_eq!(attr_reply_size(reply), 8192);
    }
}
