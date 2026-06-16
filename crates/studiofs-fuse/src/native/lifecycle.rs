//! Linux-native FUSE mount lifecycle.

use std::ffi::{CString, NulError};
use std::fs::{File, OpenOptions};
use std::io;
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectMountOptions {
    pub source: String,
    pub fs_name: String,
    pub subtype: Option<String>,
    pub read_only: bool,
    pub allow_other: bool,
    pub default_permissions: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectMountRequest {
    pub mount_point: String,
    pub source: String,
    pub fs_type: String,
    pub flags: libc::c_ulong,
    pub data: CString,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fusermount3MountRequest {
    pub mount_point: String,
    pub options: CString,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fusermount3Command {
    pub program: String,
    pub argv: Vec<String>,
    pub env: Vec<(String, String)>,
}

#[derive(Debug, Error)]
pub enum LifecycleError {
    #[error("mount option contains NUL")]
    Nul(#[from] NulError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug)]
pub struct FuseDevice {
    file: File,
}

impl FuseDevice {
    pub fn open_default() -> Result<Self, LifecycleError> {
        Self::open("/dev/fuse")
    }

    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, LifecycleError> {
        Ok(Self {
            file: OpenOptions::new().read(true).write(true).open(path)?,
        })
    }

    pub fn from_file(file: File) -> Self {
        Self { file }
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn into_file(self) -> File {
        self.file
    }
}

impl AsRawFd for FuseDevice {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

pub trait FuseMountSystem {
    fn mount(&mut self, request: &DirectMountRequest) -> Result<(), LifecycleError>;
    fn unmount(&mut self, mount_point: &str) -> Result<(), LifecycleError>;
}

pub struct NativeMountLifecycle<S> {
    system: S,
}

pub trait MountSyscalls {
    fn mount(
        &mut self,
        source: &CString,
        target: &CString,
        fs_type: &CString,
        flags: libc::c_ulong,
        data: &CString,
    ) -> io::Result<()>;
    fn umount2(&mut self, target: &CString, flags: libc::c_int) -> io::Result<()>;
}

pub trait CloneFdSyscalls {
    fn attach_clone_fd(&mut self, source_fd: RawFd, clone_fd: RawFd) -> io::Result<()>;
}

const FUSE_DEV_IOC_CLONE: libc::c_ulong = ioc_read(229 as libc::c_ulong, 0, 4);

#[derive(Debug)]
pub struct LinuxMountSystem<C> {
    syscalls: C,
}

#[derive(Debug, Default)]
pub struct LibcMountSyscalls;

#[derive(Debug, Default)]
pub struct LibcCloneFdSyscalls;

pub struct FuseCloneFd<C> {
    syscalls: C,
}

#[derive(Debug)]
pub struct UnixSocketPair {
    parent: OwnedFd,
    child: OwnedFd,
}

#[derive(Debug, Default)]
pub struct FusermountFdReceiver;

#[derive(Debug, Clone)]
pub struct Fusermount3MountSystem {
    program: String,
}

impl<C> FuseCloneFd<C> {
    pub fn new(syscalls: C) -> Self {
        Self { syscalls }
    }

    pub fn into_inner(self) -> C {
        self.syscalls
    }
}

impl<C: CloneFdSyscalls> FuseCloneFd<C> {
    pub fn attach(&mut self, source_fd: RawFd, clone_fd: RawFd) -> Result<(), LifecycleError> {
        self.syscalls.attach_clone_fd(source_fd, clone_fd)?;
        Ok(())
    }
}

const fn ioc_read(type_: libc::c_ulong, nr: libc::c_ulong, size: libc::c_ulong) -> libc::c_ulong {
    const IOC_NRBITS: libc::c_ulong = 8;
    const IOC_TYPEBITS: libc::c_ulong = 8;
    const IOC_SIZEBITS: libc::c_ulong = 14;
    const IOC_NRSHIFT: libc::c_ulong = 0;
    const IOC_TYPESHIFT: libc::c_ulong = IOC_NRSHIFT + IOC_NRBITS;
    const IOC_SIZESHIFT: libc::c_ulong = IOC_TYPESHIFT + IOC_TYPEBITS;
    const IOC_DIRSHIFT: libc::c_ulong = IOC_SIZESHIFT + IOC_SIZEBITS;
    const IOC_READ: libc::c_ulong = 2;

    (IOC_READ << IOC_DIRSHIFT) | (size << IOC_SIZESHIFT) | (type_ << IOC_TYPESHIFT) | nr
}

fn cmsg_space_for_fd() -> usize {
    // SAFETY: `CMSG_SPACE` is a pure size computation for the supplied payload
    // length; no pointers are dereferenced.
    unsafe { libc::CMSG_SPACE(mem::size_of::<RawFd>() as libc::c_uint) as usize }
}

struct AncillaryFdBuffer {
    storage: Vec<usize>,
    len_bytes: usize,
}

impl AncillaryFdBuffer {
    fn for_fd() -> Self {
        let len_bytes = cmsg_space_for_fd();
        let word_len = len_bytes.div_ceil(mem::size_of::<usize>());
        Self {
            storage: vec![0; word_len],
            len_bytes,
        }
    }

    fn as_mut_ptr(&mut self) -> *mut libc::c_void {
        self.storage.as_mut_ptr().cast()
    }

    fn len_bytes(&self) -> usize {
        self.len_bytes
    }
}

fn set_close_on_exec(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fcntl` operates on an fd received from the kernel/helper. It
    // does not outlive this call, and errors are propagated.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: same descriptor as above; setting FD_CLOEXEC is idempotent.
    let result = unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

impl<C> LinuxMountSystem<C> {
    pub fn new(syscalls: C) -> Self {
        Self { syscalls }
    }

    pub fn into_inner(self) -> C {
        self.syscalls
    }
}

impl<S> NativeMountLifecycle<S> {
    pub fn new(system: S) -> Self {
        Self { system }
    }

    pub fn into_inner(self) -> S {
        self.system
    }
}

impl DirectMountOptions {
    pub fn mount_request(
        &self,
        mount_point: impl Into<String>,
        fuse_fd: RawFd,
        uid: libc::uid_t,
        gid: libc::gid_t,
    ) -> Result<DirectMountRequest, LifecycleError> {
        let mut data = vec![
            format!("fd={fuse_fd}"),
            "rootmode=40000".to_owned(),
            format!("user_id={uid}"),
            format!("group_id={gid}"),
            format!("fsname={}", self.fs_name),
        ];
        if self.allow_other {
            data.push("allow_other".to_owned());
        }
        if self.default_permissions {
            data.push("default_permissions".to_owned());
        }

        let mut flags = libc::MS_NOSUID | libc::MS_NODEV;
        if self.read_only {
            flags |= libc::MS_RDONLY;
        }

        Ok(DirectMountRequest {
            mount_point: mount_point.into(),
            source: self.source.clone(),
            fs_type: self
                .subtype
                .as_ref()
                .map_or_else(|| "fuse".to_owned(), |subtype| format!("fuse.{subtype}")),
            flags,
            data: CString::new(data.join(","))?,
        })
    }

    pub fn fusermount_request(
        &self,
        mount_point: impl Into<String>,
    ) -> Result<Fusermount3MountRequest, LifecycleError> {
        let mut options = vec![
            if self.read_only {
                "ro".to_owned()
            } else {
                "rw".to_owned()
            },
            "nosuid".to_owned(),
            "nodev".to_owned(),
            format!("fsname={}", self.fs_name),
        ];
        if let Some(subtype) = &self.subtype {
            options.push(format!("subtype={subtype}"));
        }
        if self.allow_other {
            options.push("allow_other".to_owned());
        }
        if self.default_permissions {
            options.push("default_permissions".to_owned());
        }

        Ok(Fusermount3MountRequest {
            mount_point: mount_point.into(),
            options: CString::new(options.join(","))?,
        })
    }
}

impl Fusermount3MountRequest {
    pub fn command_for_comm_fds(
        &self,
        program: impl Into<String>,
        child_fd: RawFd,
        parent_fd: RawFd,
    ) -> Fusermount3Command {
        let program = program.into();
        Fusermount3Command {
            argv: vec![
                program.clone(),
                "-o".to_owned(),
                self.options.to_string_lossy().into_owned(),
                "--".to_owned(),
                self.mount_point.clone(),
            ],
            env: vec![
                ("_FUSE_COMMFD".to_owned(), child_fd.to_string()),
                ("_FUSE_COMMFD2".to_owned(), parent_fd.to_string()),
            ],
            program,
        }
    }
}

impl Fusermount3Command {
    fn spawn(&self, parent_fd_to_close: RawFd) -> io::Result<Child> {
        let mut command = Command::new(&self.program);
        command
            .args(self.argv.iter().skip(1))
            .envs(self.env.iter().map(|(key, value)| (key, value)))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        // SAFETY: the pre-exec hook only invokes the async-signal-safe
        // `close(2)` syscall on the child copy of the receiver socket.
        unsafe {
            command.pre_exec(move || {
                let result = libc::close(parent_fd_to_close);
                if result == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            });
        }
        command.spawn()
    }
}

impl UnixSocketPair {
    pub fn open() -> Result<Self, LifecycleError> {
        let mut fds = [-1; 2];
        // SAFETY: `fds` points to storage for exactly two descriptors, as
        // required by `socketpair(2)`.
        let result =
            unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        if result != 0 {
            return Err(io::Error::last_os_error().into());
        }

        // SAFETY: `socketpair` initialized both descriptors on success, and
        // ownership is transferred into `OwnedFd` exactly once.
        let parent = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        // SAFETY: see above; this is the peer descriptor.
        let child = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        Ok(Self { parent, child })
    }

    pub fn parent_fd(&self) -> RawFd {
        self.parent.as_raw_fd()
    }

    pub fn child_fd(&self) -> RawFd {
        self.child.as_raw_fd()
    }

    fn into_parent_after_spawn(self) -> OwnedFd {
        self.parent
    }
}

impl FusermountFdReceiver {
    pub fn receive(comm_fd: RawFd) -> Result<File, LifecycleError> {
        let mut byte = [0_u8; 1];
        let mut iov = libc::iovec {
            iov_base: byte.as_mut_ptr().cast(),
            iov_len: byte.len(),
        };
        let mut control = AncillaryFdBuffer::for_fd();
        // SAFETY: zeroed `msghdr` is valid because every pointer/length field
        // used by `recvmsg` is assigned immediately below.
        let mut msg: libc::msghdr = unsafe { mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr();
        msg.msg_controllen = control.len_bytes();

        loop {
            // SAFETY: `msg` references valid iovec and control buffers for the
            // duration of the call; `comm_fd` is expected to be a Unix stream
            // socket owned by the caller.
            let received = unsafe { libc::recvmsg(comm_fd, &mut msg, 0) };
            if received == -1 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error.into());
            }
            if received == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "fusermount3 closed communication socket without sending a FUSE fd",
                )
                .into());
            }
            break;
        }

        // SAFETY: `msg` was filled by `recvmsg`; `CMSG_FIRSTHDR` either returns
        // a valid control message within the control buffer or null.
        let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
        if cmsg.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "fusermount3 response did not contain a control message",
            )
            .into());
        }

        // SAFETY: `cmsg` is non-null and points into `control`, validated above.
        let cmsg_ref = unsafe { &*cmsg };
        if cmsg_ref.cmsg_level != libc::SOL_SOCKET || cmsg_ref.cmsg_type != libc::SCM_RIGHTS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "fusermount3 response did not contain SCM_RIGHTS",
            )
            .into());
        }

        // SAFETY: SCM_RIGHTS payload contains at least one descriptor when sent
        // by fusermount3/libfuse; copy the descriptor value before wrapping it.
        let fd = unsafe { *(libc::CMSG_DATA(cmsg).cast::<RawFd>()) };
        set_close_on_exec(fd)?;
        // SAFETY: ownership of the received descriptor transfers to this
        // process via SCM_RIGHTS; `File` will close it on drop.
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

impl Default for Fusermount3MountSystem {
    fn default() -> Self {
        Self::new("fusermount3")
    }
}

impl Fusermount3MountSystem {
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
        }
    }

    pub fn mount(&self, request: &Fusermount3MountRequest) -> Result<FuseDevice, LifecycleError> {
        let sockets = UnixSocketPair::open()?;
        let command = request.command_for_comm_fds(
            self.program.clone(),
            sockets.child_fd(),
            sockets.parent_fd(),
        );
        let mut child = command.spawn(sockets.parent_fd())?;
        let parent = sockets.into_parent_after_spawn();
        let file = FusermountFdReceiver::receive(parent.as_raw_fd());
        let status = child.wait()?;
        if !status.success() {
            return Err(
                io::Error::other(format!("fusermount3 exited with status {status}")).into(),
            );
        }
        Ok(FuseDevice::from_file(file?))
    }
}

impl<S: FuseMountSystem> NativeMountLifecycle<S> {
    pub fn mount_direct(
        &mut self,
        mount_point: impl Into<String>,
        fuse_fd: RawFd,
        uid: libc::uid_t,
        gid: libc::gid_t,
        options: &DirectMountOptions,
    ) -> Result<(), LifecycleError> {
        let request = options.mount_request(mount_point, fuse_fd, uid, gid)?;
        self.system.mount(&request)
    }

    pub fn unmount(&mut self, mount_point: &str) -> Result<(), LifecycleError> {
        self.system.unmount(mount_point)
    }
}

impl<C: MountSyscalls> FuseMountSystem for LinuxMountSystem<C> {
    fn mount(&mut self, request: &DirectMountRequest) -> Result<(), LifecycleError> {
        let source = CString::new(request.source.as_str())?;
        let target = CString::new(request.mount_point.as_str())?;
        let fs_type = CString::new(request.fs_type.as_str())?;
        self.syscalls
            .mount(&source, &target, &fs_type, request.flags, &request.data)?;
        Ok(())
    }

    fn unmount(&mut self, mount_point: &str) -> Result<(), LifecycleError> {
        let target = CString::new(mount_point)?;
        self.syscalls.umount2(&target, libc::MNT_DETACH)?;
        Ok(())
    }
}

impl MountSyscalls for LibcMountSyscalls {
    fn mount(
        &mut self,
        source: &CString,
        target: &CString,
        fs_type: &CString,
        flags: libc::c_ulong,
        data: &CString,
    ) -> io::Result<()> {
        // SAFETY: all pointers are valid NUL-terminated C strings for the
        // duration of the call, and `data` points to a FUSE mount option string
        // accepted by the Linux kernel.
        let result = unsafe {
            libc::mount(
                source.as_ptr(),
                target.as_ptr(),
                fs_type.as_ptr(),
                flags,
                data.as_ptr().cast(),
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn umount2(&mut self, target: &CString, flags: libc::c_int) -> io::Result<()> {
        // SAFETY: `target` is a valid NUL-terminated C string for the duration
        // of the call; flags are passed through from the lifecycle policy.
        let result = unsafe { libc::umount2(target.as_ptr(), flags) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

impl CloneFdSyscalls for LibcCloneFdSyscalls {
    fn attach_clone_fd(&mut self, source_fd: RawFd, clone_fd: RawFd) -> io::Result<()> {
        let source = u32::try_from(source_fd)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "negative FUSE fd"))?;
        // SAFETY: `clone_fd` is expected to be an open `/dev/fuse` clone
        // descriptor, and `source` points to the primary FUSE fd value for the
        // duration of the ioctl call.
        let result = unsafe { libc::ioctl(clone_fd, FUSE_DEV_IOC_CLONE, &source) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_mount_request_includes_fd_identity_and_kernel_options() {
        let options = DirectMountOptions {
            source: "studiofs".into(),
            fs_name: "studiofs-volume".into(),
            subtype: Some("studiofs".into()),
            read_only: true,
            allow_other: true,
            default_permissions: true,
        };

        let request = options
            .mount_request("/mnt/studiofs", 7, 1000, 1001)
            .expect("mount request should build");

        assert_eq!(request.mount_point, "/mnt/studiofs");
        assert_eq!(request.source, "studiofs");
        assert_eq!(request.fs_type, "fuse.studiofs");
        assert_eq!(
            request.flags,
            libc::MS_NOSUID | libc::MS_NODEV | libc::MS_RDONLY
        );
        assert_eq!(
            request.data.to_str().expect("data should be utf-8"),
            "fd=7,rootmode=40000,user_id=1000,group_id=1001,fsname=studiofs-volume,allow_other,default_permissions"
        );
    }

    #[derive(Default)]
    struct RecordingMountSystem {
        mounted: Vec<DirectMountRequest>,
        unmounted: Vec<String>,
    }

    impl FuseMountSystem for RecordingMountSystem {
        fn mount(&mut self, request: &DirectMountRequest) -> Result<(), LifecycleError> {
            self.mounted.push(request.clone());
            Ok(())
        }

        fn unmount(&mut self, mount_point: &str) -> Result<(), LifecycleError> {
            self.unmounted.push(mount_point.to_owned());
            Ok(())
        }
    }

    #[test]
    fn lifecycle_mounts_and_unmounts_through_system_boundary() {
        let options = DirectMountOptions {
            source: "studiofs".into(),
            fs_name: "studiofs-volume".into(),
            subtype: None,
            read_only: false,
            allow_other: false,
            default_permissions: false,
        };
        let mut lifecycle = NativeMountLifecycle::new(RecordingMountSystem::default());

        lifecycle
            .mount_direct("/mnt/studiofs", 11, 1000, 1000, &options)
            .expect("mount should be delegated");
        lifecycle
            .unmount("/mnt/studiofs")
            .expect("unmount should be delegated");
        let system = lifecycle.into_inner();

        assert_eq!(system.mounted.len(), 1);
        assert_eq!(
            system.mounted[0].data.to_str().unwrap(),
            "fd=11,rootmode=40000,user_id=1000,group_id=1000,fsname=studiofs-volume"
        );
        assert_eq!(system.unmounted, ["/mnt/studiofs"]);
    }

    #[derive(Default)]
    struct RecordingSyscalls {
        mounts: Vec<(String, String, String, libc::c_ulong, String)>,
        unmounts: Vec<(String, libc::c_int)>,
    }

    impl MountSyscalls for RecordingSyscalls {
        fn mount(
            &mut self,
            source: &CString,
            target: &CString,
            fs_type: &CString,
            flags: libc::c_ulong,
            data: &CString,
        ) -> io::Result<()> {
            self.mounts.push((
                source.to_str().unwrap().to_owned(),
                target.to_str().unwrap().to_owned(),
                fs_type.to_str().unwrap().to_owned(),
                flags,
                data.to_str().unwrap().to_owned(),
            ));
            Ok(())
        }

        fn umount2(&mut self, target: &CString, flags: libc::c_int) -> io::Result<()> {
            self.unmounts
                .push((target.to_str().unwrap().to_owned(), flags));
            Ok(())
        }
    }

    #[test]
    fn linux_mount_system_delegates_to_mount_and_umount_syscalls() {
        let request = DirectMountOptions {
            source: "studiofs".into(),
            fs_name: "studiofs-volume".into(),
            subtype: Some("studiofs".into()),
            read_only: false,
            allow_other: false,
            default_permissions: false,
        }
        .mount_request("/mnt/studiofs", 3, 1000, 1000)
        .expect("mount request should build");
        let mut system = LinuxMountSystem::new(RecordingSyscalls::default());

        system.mount(&request).expect("mount should delegate");
        system
            .unmount("/mnt/studiofs")
            .expect("unmount should delegate");
        let syscalls = system.into_inner();

        assert_eq!(
            syscalls.mounts,
            [(
                "studiofs".to_owned(),
                "/mnt/studiofs".to_owned(),
                "fuse.studiofs".to_owned(),
                libc::MS_NOSUID | libc::MS_NODEV,
                "fd=3,rootmode=40000,user_id=1000,group_id=1000,fsname=studiofs-volume".to_owned(),
            )]
        );
        assert_eq!(
            syscalls.unmounts,
            [("/mnt/studiofs".to_owned(), libc::MNT_DETACH)]
        );
    }

    #[derive(Default)]
    struct RecordingCloneSyscalls {
        clones: Vec<(RawFd, RawFd)>,
    }

    impl CloneFdSyscalls for RecordingCloneSyscalls {
        fn attach_clone_fd(&mut self, source_fd: RawFd, clone_fd: RawFd) -> io::Result<()> {
            self.clones.push((source_fd, clone_fd));
            Ok(())
        }
    }

    fn send_fd_for_test(sock_fd: RawFd, fd: RawFd) -> io::Result<()> {
        let mut byte = [0_u8; 1];
        let mut iov = libc::iovec {
            iov_base: byte.as_mut_ptr().cast(),
            iov_len: byte.len(),
        };
        let mut control = AncillaryFdBuffer::for_fd();
        // SAFETY: zeroed `msghdr` is valid because all used fields are assigned
        // before `sendmsg`.
        let mut msg: libc::msghdr = unsafe { mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr();
        msg.msg_controllen = control.len_bytes();

        // SAFETY: `msg` has a valid control buffer large enough for one fd.
        let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
        if cmsg.is_null() {
            return Err(io::Error::other("missing cmsghdr"));
        }

        // SAFETY: `cmsg` points into `control`; fields and payload are written
        // within the buffer sized by `CMSG_SPACE`.
        unsafe {
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(mem::size_of::<RawFd>() as libc::c_uint) as usize;
            *libc::CMSG_DATA(cmsg).cast::<RawFd>() = fd;
        }
        // SAFETY: `cmsg` is still valid and initialized above.
        msg.msg_controllen = unsafe { (*cmsg).cmsg_len };

        loop {
            // SAFETY: `msg` references valid iovec/control buffers for this
            // synchronous call.
            let sent = unsafe { libc::sendmsg(sock_fd, &msg, 0) };
            if sent == -1 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if sent == 1 {
                return Ok(());
            }
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "sendmsg did not send the fd notification byte",
            ));
        }
    }

    #[test]
    fn ancillary_fd_buffer_is_aligned_for_cmsghdr() {
        let mut buffer = AncillaryFdBuffer::for_fd();
        let ptr = buffer.as_mut_ptr() as usize;

        assert_eq!(ptr % mem::align_of::<libc::cmsghdr>(), 0);
        assert!(buffer.len_bytes() >= cmsg_space_for_fd());
    }

    #[test]
    fn fuse_clone_fd_attaches_clone_to_primary_connection() {
        let mut clone_fd = FuseCloneFd::new(RecordingCloneSyscalls::default());

        clone_fd
            .attach(3, 4)
            .expect("clone fd attachment should delegate to ioctl boundary");

        assert_eq!(clone_fd.into_inner().clones, [(3, 4)]);
    }

    #[test]
    fn fusermount_request_uses_helper_options_without_kernel_fd_identity() {
        let options = DirectMountOptions {
            source: "studiofs".into(),
            fs_name: "studiofs-volume".into(),
            subtype: Some("studiofs".into()),
            read_only: true,
            allow_other: true,
            default_permissions: true,
        };

        let request = options
            .fusermount_request("/mnt/studiofs")
            .expect("fusermount request should build");

        assert_eq!(request.mount_point, "/mnt/studiofs");
        assert_eq!(
            request.options.to_str().expect("options should be utf-8"),
            "ro,nosuid,nodev,fsname=studiofs-volume,subtype=studiofs,allow_other,default_permissions"
        );
    }

    #[test]
    fn fusermount_command_sets_commfd_env_and_helper_arguments() {
        let request = Fusermount3MountRequest {
            mount_point: "/mnt/studiofs".into(),
            options: CString::new("rw,nosuid,nodev,fsname=studiofs").unwrap(),
        };

        let command = request.command_for_comm_fds("fusermount3", 5, 6);

        assert_eq!(
            command.argv,
            [
                "fusermount3",
                "-o",
                "rw,nosuid,nodev,fsname=studiofs",
                "--",
                "/mnt/studiofs",
            ]
        );
        assert_eq!(
            command.env,
            [
                ("_FUSE_COMMFD".to_owned(), "5".to_owned()),
                ("_FUSE_COMMFD2".to_owned(), "6".to_owned())
            ]
        );
    }

    #[test]
    fn unix_fd_receiver_accepts_scm_rights_descriptor() {
        use std::io::{Read, Seek, Write};

        let sockets = UnixSocketPair::open().expect("socketpair should open");
        let mut source = tempfile::tempfile().expect("temp file should open");
        source
            .write_all(b"fd-payload")
            .expect("temp file should be writable");
        source
            .rewind()
            .expect("temp file should rewind before sending");

        send_fd_for_test(sockets.child_fd(), source.as_raw_fd()).expect("fd should send");
        drop(source);

        let mut received =
            FusermountFdReceiver::receive(sockets.parent_fd()).expect("fd should be received");
        let mut payload = String::new();
        received
            .read_to_string(&mut payload)
            .expect("received fd should be readable");

        assert_eq!(payload, "fd-payload");
    }
}
