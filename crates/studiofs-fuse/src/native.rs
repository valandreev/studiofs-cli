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
    use crate::native::vfs_adapter::{
        LinuxNativeBridge, NativeAttr, NativeDirEntry, NativeFileType, NativeStatFs, NativeVfs,
    };

    #[derive(Debug)]
    struct RecordingNativeVfs {
        lookups: Mutex<Vec<(u64, Vec<u8>)>>,
        opens: Mutex<Vec<(u64, i32)>>,
        releases: Mutex<Vec<u64>>,
        reads: Mutex<Vec<(u64, u64, u64, u32)>>,
        writes: Mutex<Vec<(u64, u64, u64, Vec<u8>, i32)>>,
        flushes: Mutex<Vec<(u64, u64)>>,
        fsyncs: Mutex<Vec<(u64, bool)>>,
        opendirs: Mutex<Vec<u64>>,
        releasedirs: Mutex<Vec<(u64, u64)>>,
        readdirs: Mutex<Vec<(u64, u64, i64)>>,
        mkdirs: Mutex<Vec<(u64, Vec<u8>, u32, u32, u16)>>,
        creates: Mutex<Vec<(u64, Vec<u8>, u32, u32, u16, i32)>>,
        unlinks: Mutex<Vec<(u64, Vec<u8>)>>,
        renames: Mutex<Vec<(u64, Vec<u8>, u64, Vec<u8>)>>,
        truncates: Mutex<Vec<(u64, Option<u64>, u64)>>,
        statfs_calls: Mutex<u32>,
        attr: NativeAttr,
        fh: u64,
        dir_fh: u64,
        read_data: Vec<u8>,
        dir_entries: Vec<NativeDirEntry>,
    }

    impl NativeVfs for RecordingNativeVfs {
        fn lookup(&self, parent: u64, name: &[u8]) -> Result<NativeAttr, i32> {
            self.lookups
                .lock()
                .expect("lookup calls lock should not be poisoned")
                .push((parent, name.to_vec()));
            Ok(self.attr)
        }

        fn getattr(&self, _ino: u64) -> Result<NativeAttr, i32> {
            Ok(self.attr)
        }

        fn open(&self, ino: u64, flags: i32) -> Result<u64, i32> {
            self.opens
                .lock()
                .expect("open calls lock should not be poisoned")
                .push((ino, flags));
            Ok(self.fh)
        }

        fn release(&self, fh: u64) -> Result<(), i32> {
            self.releases
                .lock()
                .expect("release calls lock should not be poisoned")
                .push(fh);
            Ok(())
        }

        fn read(&self, fh: u64, ino: u64, offset: u64, size: u32) -> Result<Vec<u8>, i32> {
            self.reads
                .lock()
                .expect("read calls lock should not be poisoned")
                .push((fh, ino, offset, size));
            Ok(self.read_data[..usize::min(self.read_data.len(), size as usize)].to_vec())
        }

        fn write(
            &self,
            fh: u64,
            ino: u64,
            offset: u64,
            data: &[u8],
            flags: i32,
        ) -> Result<u32, i32> {
            self.writes
                .lock()
                .expect("write calls lock should not be poisoned")
                .push((fh, ino, offset, data.to_vec(), flags));
            Ok(data.len() as u32)
        }

        fn flush(&self, ino: u64, fh: u64) -> Result<(), i32> {
            self.flushes
                .lock()
                .expect("flush calls lock should not be poisoned")
                .push((ino, fh));
            Ok(())
        }

        fn fsync(&self, ino: u64, datasync: bool) -> Result<(), i32> {
            self.fsyncs
                .lock()
                .expect("fsync calls lock should not be poisoned")
                .push((ino, datasync));
            Ok(())
        }

        fn opendir(&self, ino: u64) -> Result<u64, i32> {
            self.opendirs
                .lock()
                .expect("opendir calls lock should not be poisoned")
                .push(ino);
            Ok(self.dir_fh)
        }

        fn releasedir(&self, ino: u64, fh: u64) -> Result<(), i32> {
            self.releasedirs
                .lock()
                .expect("releasedir calls lock should not be poisoned")
                .push((ino, fh));
            Ok(())
        }

        fn readdir(&self, ino: u64, fh: u64, offset: i64) -> Result<Vec<NativeDirEntry>, i32> {
            self.readdirs
                .lock()
                .expect("readdir calls lock should not be poisoned")
                .push((ino, fh, offset));
            Ok(self.dir_entries.clone())
        }

        fn mkdir(
            &self,
            parent: u64,
            name: &[u8],
            uid: u32,
            gid: u32,
            mode: u16,
        ) -> Result<NativeAttr, i32> {
            self.mkdirs
                .lock()
                .expect("mkdir calls lock should not be poisoned")
                .push((parent, name.to_vec(), uid, gid, mode));
            Ok(self.attr)
        }

        fn create(
            &self,
            parent: u64,
            name: &[u8],
            uid: u32,
            gid: u32,
            mode: u16,
            flags: i32,
        ) -> Result<(NativeAttr, u64), i32> {
            self.creates
                .lock()
                .expect("create calls lock should not be poisoned")
                .push((parent, name.to_vec(), uid, gid, mode, flags));
            Ok((self.attr, self.fh))
        }

        fn unlink(&self, parent: u64, name: &[u8]) -> Result<(), i32> {
            self.unlinks
                .lock()
                .expect("unlink calls lock should not be poisoned")
                .push((parent, name.to_vec()));
            Ok(())
        }

        fn rename(
            &self,
            parent: u64,
            name: &[u8],
            new_parent: u64,
            new_name: &[u8],
        ) -> Result<(), i32> {
            self.renames
                .lock()
                .expect("rename calls lock should not be poisoned")
                .push((parent, name.to_vec(), new_parent, new_name.to_vec()));
            Ok(())
        }

        fn truncate(&self, ino: u64, fh: Option<u64>, size: u64) -> Result<NativeAttr, i32> {
            self.truncates
                .lock()
                .expect("truncate calls lock should not be poisoned")
                .push((ino, fh, size));
            Ok(NativeAttr { size, ..self.attr })
        }

        fn statfs(&self) -> Result<NativeStatFs, i32> {
            *self
                .statfs_calls
                .lock()
                .expect("statfs calls lock should not be poisoned") += 1;
            Ok(NativeStatFs {
                blocks: 100,
                bfree: 80,
                bavail: 70,
                files: 10,
                ffree: 9,
                bsize: 4096,
                namelen: 255,
                frsize: 4096,
            })
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

    fn open_request(ino: u64, flags: i32) -> FuseRequest {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(flags as u32).to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        FuseRequest {
            header: FuseInHeader::new(
                (FUSE_IN_HEADER_LEN + payload.len()) as u32,
                14,
                0x1234,
                ino,
                1000,
                1000,
                77,
            ),
            opcode: FuseOpcode::Open,
            payload,
        }
    }

    fn release_request(ino: u64, fh: u64) -> FuseRequest {
        let mut payload = Vec::new();
        payload.extend_from_slice(&fh.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&0_u64.to_le_bytes());
        FuseRequest {
            header: FuseInHeader::new(
                (FUSE_IN_HEADER_LEN + payload.len()) as u32,
                18,
                0x5678,
                ino,
                1000,
                1000,
                77,
            ),
            opcode: FuseOpcode::Release,
            payload,
        }
    }

    fn read_request(ino: u64, fh: u64, offset: u64, size: u32) -> FuseRequest {
        let mut payload = Vec::new();
        payload.extend_from_slice(&fh.to_le_bytes());
        payload.extend_from_slice(&offset.to_le_bytes());
        payload.extend_from_slice(&size.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&0_u64.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        FuseRequest {
            header: FuseInHeader::new(
                (FUSE_IN_HEADER_LEN + payload.len()) as u32,
                15,
                0x9abc,
                ino,
                1000,
                1000,
                77,
            ),
            opcode: FuseOpcode::Read,
            payload,
        }
    }

    fn write_request(ino: u64, fh: u64, offset: u64, flags: i32, data: &[u8]) -> FuseRequest {
        let mut payload = Vec::new();
        payload.extend_from_slice(&fh.to_le_bytes());
        payload.extend_from_slice(&offset.to_le_bytes());
        payload.extend_from_slice(&(data.len() as u32).to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&0_u64.to_le_bytes());
        payload.extend_from_slice(&(flags as u32).to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(data);
        FuseRequest {
            header: FuseInHeader::new(
                (FUSE_IN_HEADER_LEN + payload.len()) as u32,
                16,
                0xdef0,
                ino,
                1000,
                1000,
                77,
            ),
            opcode: FuseOpcode::Write,
            payload,
        }
    }

    fn flush_request(ino: u64, fh: u64) -> FuseRequest {
        let mut payload = Vec::new();
        payload.extend_from_slice(&fh.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&0_u64.to_le_bytes());
        FuseRequest {
            header: FuseInHeader::new(
                (FUSE_IN_HEADER_LEN + payload.len()) as u32,
                25,
                0xaaaa,
                ino,
                1000,
                1000,
                77,
            ),
            opcode: FuseOpcode::Flush,
            payload,
        }
    }

    fn fsync_request(ino: u64, fh: u64, datasync: bool) -> FuseRequest {
        let mut payload = Vec::new();
        payload.extend_from_slice(&fh.to_le_bytes());
        payload.extend_from_slice(&(u32::from(datasync)).to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        FuseRequest {
            header: FuseInHeader::new(
                (FUSE_IN_HEADER_LEN + payload.len()) as u32,
                20,
                0xbbbb,
                ino,
                1000,
                1000,
                77,
            ),
            opcode: FuseOpcode::Fsync,
            payload,
        }
    }

    fn opendir_request(ino: u64) -> FuseRequest {
        open_like_request(ino, 27, FuseOpcode::Opendir, 0xcccc)
    }

    fn releasedir_request(ino: u64, fh: u64) -> FuseRequest {
        release_like_request(ino, fh, 29, FuseOpcode::Releasedir, 0xdddd)
    }

    fn readdir_request(ino: u64, fh: u64, offset: i64, size: u32) -> FuseRequest {
        let mut request = read_request(ino, fh, offset as u64, size);
        request.header = FuseInHeader::new(request.header.len, 28, 0xeeee, ino, 1000, 1000, 77);
        request.opcode = FuseOpcode::Readdir;
        request
    }

    fn mkdir_request(parent: u64, name: &[u8], mode: u32) -> FuseRequest {
        let mut payload = Vec::new();
        payload.extend_from_slice(&mode.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(name);
        payload.push(0);
        FuseRequest {
            header: FuseInHeader::new(
                (FUSE_IN_HEADER_LEN + payload.len()) as u32,
                9,
                0x1001,
                parent,
                1000,
                1001,
                77,
            ),
            opcode: FuseOpcode::Mkdir,
            payload,
        }
    }

    fn create_request(parent: u64, name: &[u8], mode: u32, flags: i32) -> FuseRequest {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(flags as u32).to_le_bytes());
        payload.extend_from_slice(&mode.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(name);
        payload.push(0);
        FuseRequest {
            header: FuseInHeader::new(
                (FUSE_IN_HEADER_LEN + payload.len()) as u32,
                35,
                0x1002,
                parent,
                1000,
                1001,
                77,
            ),
            opcode: FuseOpcode::Create,
            payload,
        }
    }

    fn unlink_request(parent: u64, name: &[u8]) -> FuseRequest {
        let mut payload = Vec::from(name);
        payload.push(0);
        FuseRequest {
            header: FuseInHeader::new(
                (FUSE_IN_HEADER_LEN + payload.len()) as u32,
                10,
                0x1003,
                parent,
                1000,
                1000,
                77,
            ),
            opcode: FuseOpcode::Unlink,
            payload,
        }
    }

    fn rename_request(parent: u64, name: &[u8], new_parent: u64, new_name: &[u8]) -> FuseRequest {
        let mut payload = Vec::new();
        payload.extend_from_slice(&new_parent.to_le_bytes());
        payload.extend_from_slice(name);
        payload.push(0);
        payload.extend_from_slice(new_name);
        payload.push(0);
        FuseRequest {
            header: FuseInHeader::new(
                (FUSE_IN_HEADER_LEN + payload.len()) as u32,
                12,
                0x1004,
                parent,
                1000,
                1000,
                77,
            ),
            opcode: FuseOpcode::Rename,
            payload,
        }
    }

    fn statfs_request(ino: u64) -> FuseRequest {
        FuseRequest {
            header: FuseInHeader::new(FUSE_IN_HEADER_LEN as u32, 17, 0x1005, ino, 1000, 1000, 77),
            opcode: FuseOpcode::Statfs,
            payload: Vec::new(),
        }
    }

    fn truncate_request(ino: u64, fh: Option<u64>, size: u64) -> FuseRequest {
        const FATTR_FH: u32 = 1 << 0;
        const FATTR_SIZE: u32 = 1 << 3;
        let mut payload = Vec::new();
        let valid = FATTR_SIZE | if fh.is_some() { FATTR_FH } else { 0 };
        payload.extend_from_slice(&valid.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&fh.unwrap_or(0).to_le_bytes());
        payload.extend_from_slice(&size.to_le_bytes());
        payload.resize(88, 0);
        FuseRequest {
            header: FuseInHeader::new(
                (FUSE_IN_HEADER_LEN + payload.len()) as u32,
                4,
                0x1006,
                ino,
                1000,
                1000,
                77,
            ),
            opcode: FuseOpcode::Setattr,
            payload,
        }
    }

    fn open_like_request(ino: u64, opcode: u32, decoded: FuseOpcode, unique: u64) -> FuseRequest {
        let mut payload = Vec::new();
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        FuseRequest {
            header: FuseInHeader::new(
                (FUSE_IN_HEADER_LEN + payload.len()) as u32,
                opcode,
                unique,
                ino,
                1000,
                1000,
                77,
            ),
            opcode: decoded,
            payload,
        }
    }

    fn release_like_request(
        ino: u64,
        fh: u64,
        opcode: u32,
        decoded: FuseOpcode,
        unique: u64,
    ) -> FuseRequest {
        let mut payload = Vec::new();
        payload.extend_from_slice(&fh.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&0_u64.to_le_bytes());
        FuseRequest {
            header: FuseInHeader::new(
                (FUSE_IN_HEADER_LEN + payload.len()) as u32,
                opcode,
                unique,
                ino,
                1000,
                1000,
                77,
            ),
            opcode: decoded,
            payload,
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

    fn open_reply_fh(reply: &[u8]) -> u64 {
        let mut raw = [0; 8];
        raw.copy_from_slice(&reply[FUSE_OUT_HEADER_LEN..FUSE_OUT_HEADER_LEN + 8]);
        u64::from_le_bytes(raw)
    }

    fn write_reply_size(reply: &[u8]) -> u32 {
        let mut raw = [0; 4];
        raw.copy_from_slice(&reply[FUSE_OUT_HEADER_LEN..FUSE_OUT_HEADER_LEN + 4]);
        u32::from_le_bytes(raw)
    }

    fn create_reply_fh(reply: &[u8]) -> u64 {
        let mut raw = [0; 8];
        let offset = FUSE_OUT_HEADER_LEN + 128;
        raw.copy_from_slice(&reply[offset..offset + 8]);
        u64::from_le_bytes(raw)
    }

    fn statfs_reply_blocks(reply: &[u8]) -> u64 {
        let mut raw = [0; 8];
        raw.copy_from_slice(&reply[FUSE_OUT_HEADER_LEN..FUSE_OUT_HEADER_LEN + 8]);
        u64::from_le_bytes(raw)
    }

    fn recording_vfs(attr: NativeAttr) -> Arc<RecordingNativeVfs> {
        Arc::new(RecordingNativeVfs {
            lookups: Mutex::new(Vec::new()),
            opens: Mutex::new(Vec::new()),
            releases: Mutex::new(Vec::new()),
            reads: Mutex::new(Vec::new()),
            writes: Mutex::new(Vec::new()),
            flushes: Mutex::new(Vec::new()),
            fsyncs: Mutex::new(Vec::new()),
            opendirs: Mutex::new(Vec::new()),
            releasedirs: Mutex::new(Vec::new()),
            readdirs: Mutex::new(Vec::new()),
            mkdirs: Mutex::new(Vec::new()),
            creates: Mutex::new(Vec::new()),
            unlinks: Mutex::new(Vec::new()),
            renames: Mutex::new(Vec::new()),
            truncates: Mutex::new(Vec::new()),
            statfs_calls: Mutex::new(0),
            attr,
            fh: 7001,
            dir_fh: 8001,
            read_data: b"payload bytes".to_vec(),
            dir_entries: vec![NativeDirEntry {
                ino: 123,
                kind: NativeFileType::RegularFile,
                name: b"child.txt".to_vec(),
            }],
        })
    }

    #[test]
    fn native_bridge_lookup_calls_vfs_and_returns_entry_reply() {
        let vfs = recording_vfs(NativeAttr {
            ino: 42,
            size: 4096,
            kind: NativeFileType::RegularFile,
            perm: 0o644,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(lookup_request(1, b"alpha.txt"));
        let reply = reply
            .bytes()
            .expect("lookup entry reply should be contiguous");

        assert_eq!(
            vfs.lookups
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
        let vfs = recording_vfs(NativeAttr {
            ino: 99,
            size: 8192,
            kind: NativeFileType::RegularFile,
            perm: 0o600,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(getattr_request(99));
        let reply = reply.bytes().expect("getattr reply should be contiguous");

        assert_eq!(reply_error(reply), 0);
        assert_eq!(attr_reply_size(reply), 8192);
    }

    #[test]
    fn native_bridge_open_calls_vfs_and_returns_file_handle() {
        let vfs = recording_vfs(NativeAttr {
            ino: 99,
            size: 8192,
            kind: NativeFileType::RegularFile,
            perm: 0o600,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(open_request(99, libc::O_RDONLY));
        let reply = reply.bytes().expect("open reply should be contiguous");

        assert_eq!(
            vfs.opens
                .lock()
                .expect("open calls lock should not be poisoned")
                .as_slice(),
            &[(99, libc::O_RDONLY)]
        );
        assert_eq!(reply_error(reply), 0);
        assert_eq!(open_reply_fh(reply), 7001);
    }

    #[test]
    fn native_bridge_release_calls_vfs_and_returns_empty_success() {
        let vfs = recording_vfs(NativeAttr {
            ino: 99,
            size: 8192,
            kind: NativeFileType::RegularFile,
            perm: 0o600,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(release_request(99, 7001));
        let reply = reply.bytes().expect("release reply should be contiguous");

        assert_eq!(
            vfs.releases
                .lock()
                .expect("release calls lock should not be poisoned")
                .as_slice(),
            &[7001]
        );
        assert_eq!(reply_error(reply), 0);
        assert_eq!(reply.len(), FUSE_OUT_HEADER_LEN);
    }

    #[test]
    fn native_bridge_read_calls_vfs_and_returns_data_reply() {
        let vfs = recording_vfs(NativeAttr {
            ino: 99,
            size: 8192,
            kind: NativeFileType::RegularFile,
            perm: 0o600,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(read_request(99, 7001, 5, 7));
        let reply = reply.to_vec();

        assert_eq!(
            vfs.reads
                .lock()
                .expect("read calls lock should not be poisoned")
                .as_slice(),
            &[(7001, 99, 5, 7)]
        );
        assert_eq!(reply_error(&reply), 0);
        assert_eq!(&reply[FUSE_OUT_HEADER_LEN..], b"payload");
    }

    #[test]
    fn native_bridge_write_calls_vfs_and_returns_written_size() {
        let vfs = recording_vfs(NativeAttr {
            ino: 99,
            size: 8192,
            kind: NativeFileType::RegularFile,
            perm: 0o600,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(write_request(99, 7001, 5, libc::O_SYNC, b"written"));
        let reply = reply.bytes().expect("write reply should be contiguous");

        assert_eq!(
            vfs.writes
                .lock()
                .expect("write calls lock should not be poisoned")
                .as_slice(),
            &[(7001, 99, 5, b"written".to_vec(), libc::O_SYNC)]
        );
        assert_eq!(reply_error(reply), 0);
        assert_eq!(write_reply_size(reply), 7);
    }

    #[test]
    fn native_bridge_flush_calls_vfs_and_returns_empty_success() {
        let vfs = recording_vfs(NativeAttr {
            ino: 99,
            size: 8192,
            kind: NativeFileType::RegularFile,
            perm: 0o600,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(flush_request(99, 7001));
        let reply = reply.bytes().expect("flush reply should be contiguous");

        assert_eq!(
            vfs.flushes
                .lock()
                .expect("flush calls lock should not be poisoned")
                .as_slice(),
            &[(99, 7001)]
        );
        assert_eq!(reply_error(reply), 0);
        assert_eq!(reply.len(), FUSE_OUT_HEADER_LEN);
    }

    #[test]
    fn native_bridge_fsync_calls_vfs_and_returns_empty_success() {
        let vfs = recording_vfs(NativeAttr {
            ino: 99,
            size: 8192,
            kind: NativeFileType::RegularFile,
            perm: 0o600,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(fsync_request(99, 7001, true));
        let reply = reply.bytes().expect("fsync reply should be contiguous");

        assert_eq!(
            vfs.fsyncs
                .lock()
                .expect("fsync calls lock should not be poisoned")
                .as_slice(),
            &[(99, true)]
        );
        assert_eq!(reply_error(reply), 0);
        assert_eq!(reply.len(), FUSE_OUT_HEADER_LEN);
    }

    #[test]
    fn native_bridge_opendir_calls_vfs_and_returns_directory_handle() {
        let vfs = recording_vfs(NativeAttr {
            ino: 99,
            size: 8192,
            kind: NativeFileType::Directory,
            perm: 0o755,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(opendir_request(99));
        let reply = reply.bytes().expect("opendir reply should be contiguous");

        assert_eq!(
            vfs.opendirs
                .lock()
                .expect("opendir calls lock should not be poisoned")
                .as_slice(),
            &[99]
        );
        assert_eq!(reply_error(reply), 0);
        assert_eq!(open_reply_fh(reply), 8001);
    }

    #[test]
    fn native_bridge_releasedir_calls_vfs_and_returns_empty_success() {
        let vfs = recording_vfs(NativeAttr {
            ino: 99,
            size: 8192,
            kind: NativeFileType::Directory,
            perm: 0o755,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(releasedir_request(99, 8001));
        let reply = reply
            .bytes()
            .expect("releasedir reply should be contiguous");

        assert_eq!(
            vfs.releasedirs
                .lock()
                .expect("releasedir calls lock should not be poisoned")
                .as_slice(),
            &[(99, 8001)]
        );
        assert_eq!(reply_error(reply), 0);
        assert_eq!(reply.len(), FUSE_OUT_HEADER_LEN);
    }

    #[test]
    fn native_bridge_readdir_calls_vfs_and_returns_dirent_reply() {
        let vfs = recording_vfs(NativeAttr {
            ino: 99,
            size: 8192,
            kind: NativeFileType::Directory,
            perm: 0o755,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(readdir_request(99, 8001, 0, 4096));
        let reply = reply.to_vec();

        assert_eq!(
            vfs.readdirs
                .lock()
                .expect("readdir calls lock should not be poisoned")
                .as_slice(),
            &[(99, 8001, 0)]
        );
        assert_eq!(reply_error(&reply), 0);
        assert!(
            reply
                .windows(b"child.txt".len())
                .any(|window| window == b"child.txt")
        );
    }

    #[test]
    fn native_bridge_mkdir_calls_vfs_and_returns_entry_reply() {
        let vfs = recording_vfs(NativeAttr {
            ino: 201,
            size: 4096,
            kind: NativeFileType::Directory,
            perm: 0o755,
            uid: 1000,
            gid: 1001,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(mkdir_request(1, b"new-dir", 0o40755));
        let reply = reply.bytes().expect("mkdir reply should be contiguous");

        assert_eq!(
            vfs.mkdirs
                .lock()
                .expect("mkdir calls lock should not be poisoned")
                .as_slice(),
            &[(1, b"new-dir".to_vec(), 1000, 1001, 0o755)]
        );
        assert_eq!(reply_error(reply), 0);
        assert_eq!(entry_reply_nodeid(reply), 201);
    }

    #[test]
    fn native_bridge_create_calls_vfs_and_returns_entry_and_file_handle() {
        let vfs = recording_vfs(NativeAttr {
            ino: 202,
            size: 0,
            kind: NativeFileType::RegularFile,
            perm: 0o644,
            uid: 1000,
            gid: 1001,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(create_request(1, b"new-file", 0o100644, libc::O_RDWR));
        let reply = reply.bytes().expect("create reply should be contiguous");

        assert_eq!(
            vfs.creates
                .lock()
                .expect("create calls lock should not be poisoned")
                .as_slice(),
            &[(1, b"new-file".to_vec(), 1000, 1001, 0o644, libc::O_RDWR)]
        );
        assert_eq!(reply_error(reply), 0);
        assert_eq!(entry_reply_nodeid(reply), 202);
        assert_eq!(create_reply_fh(reply), 7001);
    }

    #[test]
    fn native_bridge_unlink_calls_vfs_and_returns_empty_success() {
        let vfs = recording_vfs(NativeAttr {
            ino: 99,
            size: 0,
            kind: NativeFileType::RegularFile,
            perm: 0o644,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(unlink_request(1, b"old-file"));
        let reply = reply.bytes().expect("unlink reply should be contiguous");

        assert_eq!(
            vfs.unlinks
                .lock()
                .expect("unlink calls lock should not be poisoned")
                .as_slice(),
            &[(1, b"old-file".to_vec())]
        );
        assert_eq!(reply_error(reply), 0);
        assert_eq!(reply.len(), FUSE_OUT_HEADER_LEN);
    }

    #[test]
    fn native_bridge_rename_calls_vfs_and_returns_empty_success() {
        let vfs = recording_vfs(NativeAttr {
            ino: 99,
            size: 0,
            kind: NativeFileType::RegularFile,
            perm: 0o644,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(rename_request(1, b"old", 2, b"new"));
        let reply = reply.bytes().expect("rename reply should be contiguous");

        assert_eq!(
            vfs.renames
                .lock()
                .expect("rename calls lock should not be poisoned")
                .as_slice(),
            &[(1, b"old".to_vec(), 2, b"new".to_vec())]
        );
        assert_eq!(reply_error(reply), 0);
        assert_eq!(reply.len(), FUSE_OUT_HEADER_LEN);
    }

    #[test]
    fn native_bridge_statfs_calls_vfs_and_returns_statfs_reply() {
        let vfs = recording_vfs(NativeAttr {
            ino: 99,
            size: 0,
            kind: NativeFileType::RegularFile,
            perm: 0o644,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(statfs_request(1));
        let reply = reply.bytes().expect("statfs reply should be contiguous");

        assert_eq!(
            *vfs.statfs_calls
                .lock()
                .expect("statfs calls lock should not be poisoned"),
            1
        );
        assert_eq!(reply_error(reply), 0);
        assert_eq!(statfs_reply_blocks(reply), 100);
    }

    #[test]
    fn native_bridge_setattr_size_calls_truncate_and_returns_attr_reply() {
        let vfs = recording_vfs(NativeAttr {
            ino: 99,
            size: 8192,
            kind: NativeFileType::RegularFile,
            perm: 0o644,
            uid: 1000,
            gid: 1000,
        });
        let mut bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle(truncate_request(99, Some(7001), 1234));
        let reply = reply.bytes().expect("setattr reply should be contiguous");

        assert_eq!(
            vfs.truncates
                .lock()
                .expect("truncate calls lock should not be poisoned")
                .as_slice(),
            &[(99, Some(7001), 1234)]
        );
        assert_eq!(reply_error(reply), 0);
        assert_eq!(attr_reply_size(reply), 1234);
    }
}
