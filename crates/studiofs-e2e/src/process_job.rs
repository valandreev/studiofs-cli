//! Process containment helpers for spawned E2E children.

use std::io;
use std::process::{Child, Command};
use std::sync::Once;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub(crate) const CHILD_TERMINATION_GRACE: Duration = Duration::from_secs(2);
#[cfg(unix)]
const SIGNAL_CLEANUP_GRACE: Duration = Duration::from_millis(750);
const CONTROL_C_EXIT_CODE: i32 = 130;

static SIGNAL_HANDLER_INSTALLED: Once = Once::new();
static SIGNAL_CLEANUP_STARTED: AtomicBool = AtomicBool::new(false);

pub(crate) fn install_cleanup_handler() {
    SIGNAL_HANDLER_INSTALLED.call_once(|| {
        if let Err(error) = ctrlc::set_handler(|| {
            if !SIGNAL_CLEANUP_STARTED.swap(true, Ordering::SeqCst) {
                imp::terminate_registered_children_for_process_exit();
            }
            std::process::exit(CONTROL_C_EXIT_CODE);
        }) {
            eprintln!("studiofs-e2e: failed to install process cleanup signal handler: {error}");
        }
    });
}

pub(crate) fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> io::Result<bool> {
    let deadline = Instant::now() + timeout;
    loop {
        if child.try_wait()?.is_some() {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[cfg(test)]
pub(crate) fn child_job_test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(all(test, windows))]
struct ChildJobTestCleanupRegistrationGuard;

#[cfg(all(test, windows))]
impl Drop for ChildJobTestCleanupRegistrationGuard {
    fn drop(&mut self) {
        CHILD_JOB_TEST_CLEANUP_REGISTRATION.with(|enabled| enabled.set(false));
    }
}

#[cfg(all(test, windows))]
thread_local! {
    static CHILD_JOB_TEST_CLEANUP_REGISTRATION: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(all(test, windows))]
fn child_job_test_cleanup_registration_scope() -> ChildJobTestCleanupRegistrationGuard {
    CHILD_JOB_TEST_CLEANUP_REGISTRATION.with(|enabled| enabled.set(true));
    ChildJobTestCleanupRegistrationGuard
}

#[cfg(windows)]
mod imp {
    use super::*;
    use std::ffi::c_void;
    use std::mem;
    use std::os::windows::io::AsRawHandle as _;
    use std::ptr;
    use std::sync::{Mutex, OnceLock};

    const JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS: u32 = 9;
    const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: u32 = 0x0000_2000;
    const ERROR_ACCESS_DENIED: i32 = 5;

    #[repr(C)]
    #[derive(Default)]
    struct JobObjectBasicLimitInformation {
        per_process_user_time_limit: i64,
        per_job_user_time_limit: i64,
        limit_flags: u32,
        minimum_working_set_size: usize,
        maximum_working_set_size: usize,
        active_process_limit: u32,
        affinity: usize,
        priority_class: u32,
        scheduling_class: u32,
    }

    #[repr(C)]
    #[derive(Default)]
    struct IoCounters {
        read_operation_count: u64,
        write_operation_count: u64,
        other_operation_count: u64,
        read_transfer_count: u64,
        write_transfer_count: u64,
        other_transfer_count: u64,
    }

    #[repr(C)]
    #[derive(Default)]
    struct JobObjectExtendedLimitInformation {
        basic_limit_information: JobObjectBasicLimitInformation,
        io_info: IoCounters,
        process_memory_limit: usize,
        job_memory_limit: usize,
        peak_process_memory_used: usize,
        peak_job_memory_used: usize,
    }

    unsafe extern "system" {
        fn CreateJobObjectW(lpJobAttributes: *mut c_void, lpName: *const u16) -> *mut c_void;
        fn SetInformationJobObject(
            hJob: *mut c_void,
            JobObjectInformationClass: u32,
            lpJobObjectInformation: *const c_void,
            cbJobObjectInformationLength: u32,
        ) -> i32;
        fn AssignProcessToJobObject(hJob: *mut c_void, hProcess: *mut c_void) -> i32;
        fn TerminateJobObject(hJob: *mut c_void, uExitCode: u32) -> i32;
        fn CloseHandle(hObject: *mut c_void) -> i32;
    }

    pub(crate) struct ChildJob {
        handle: *mut c_void,
    }

    static CHILD_JOBS: OnceLock<Mutex<Vec<usize>>> = OnceLock::new();

    impl ChildJob {
        pub(crate) fn prepare_command(_command: &mut Command) -> io::Result<()> {
            Ok(())
        }

        pub(crate) fn assign_to_child(child: &Child) -> io::Result<Self> {
            // SAFETY: null attributes/name request a private unnamed job object.
            let handle = unsafe { CreateJobObjectW(ptr::null_mut(), ptr::null()) };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }

            let mut job = Self { handle };
            if let Err(error) = job.enable_kill_on_close() {
                let _ = job.close();
                return Err(error);
            }
            if let Err(error) = job.assign(child) {
                let _ = job.close();
                return Err(job_assignment_error(error));
            }
            register_job(handle);
            Ok(job)
        }

        pub(crate) fn terminate(&self) -> io::Result<bool> {
            if self.handle.is_null() {
                return Ok(false);
            }
            // SAFETY: `handle` is a live job object handle owned by `self`.
            let ok = unsafe { TerminateJobObject(self.handle, 1) };
            if ok == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(true)
            }
        }

        pub(crate) fn force_kill(&self) -> io::Result<bool> {
            self.terminate()
        }

        fn enable_kill_on_close(&mut self) -> io::Result<()> {
            let mut info = JobObjectExtendedLimitInformation::default();
            info.basic_limit_information.limit_flags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            // SAFETY: `info` is the documented extended limit structure for class 9.
            let ok = unsafe {
                SetInformationJobObject(
                    self.handle,
                    JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
                    ptr::addr_of!(info).cast(),
                    mem::size_of::<JobObjectExtendedLimitInformation>() as u32,
                )
            };
            if ok == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }

        fn assign(&mut self, child: &Child) -> io::Result<()> {
            let process_handle = child.as_raw_handle();
            // SAFETY: `process_handle` is borrowed from a live `Child`; the job handle is valid.
            let ok = unsafe { AssignProcessToJobObject(self.handle, process_handle.cast()) };
            if ok == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }

        fn close(&mut self) -> io::Result<()> {
            if self.handle.is_null() {
                return Ok(());
            }
            unregister_job(self.handle);
            // SAFETY: `handle` is owned by `self` and closed at most once.
            let ok = unsafe { CloseHandle(self.handle) };
            self.handle = ptr::null_mut();
            if ok == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }
    }

    fn job_assignment_error(error: io::Error) -> io::Error {
        let detail = if error.raw_os_error() == Some(ERROR_ACCESS_DENIED) {
            "child may already be constrained by an outer Windows Job Object that does not allow breakaway"
        } else {
            "child could not be assigned to the StudioFS E2E Windows Job Object"
        };
        io::Error::new(error.kind(), format!("{detail}: {error}"))
    }

    impl Drop for ChildJob {
        fn drop(&mut self) {
            let _ = self.close();
        }
    }

    pub(super) fn terminate_registered_children_for_process_exit() {
        for handle in registered_jobs() {
            // SAFETY: entries are live job handles registered by ChildJob. A stale
            // handle may fail TerminateJobObject, which is acceptable during exit.
            let _ = unsafe { TerminateJobObject(handle as *mut c_void, 1) };
        }
    }

    fn child_jobs() -> &'static Mutex<Vec<usize>> {
        CHILD_JOBS.get_or_init(|| Mutex::new(Vec::new()))
    }

    fn register_job(handle: *mut c_void) {
        #[cfg(test)]
        if !super::CHILD_JOB_TEST_CLEANUP_REGISTRATION.with(|enabled| enabled.get()) {
            return;
        }

        let mut jobs = child_jobs()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let handle = handle as usize;
        if !jobs.contains(&handle) {
            jobs.push(handle);
        }
    }

    fn unregister_job(handle: *mut c_void) {
        let mut jobs = child_jobs()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let handle = handle as usize;
        jobs.retain(|registered| *registered != handle);
    }

    fn registered_jobs() -> Vec<usize> {
        child_jobs()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};

        #[test]
        fn kill_on_close_job_terminates_assigned_child() {
            let _guard = super::super::child_job_test_lock();
            let mut child = Command::new("cmd")
                .args(["/C", "ping -n 30 127.0.0.1 >NUL"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn hung command");
            let job = ChildJob::assign_to_child(&child).expect("assign child to kill-on-close job");

            drop(job);

            if let Err(error) = wait_for_child_exit(&mut child, Duration::from_secs(2)) {
                let _ = child.kill();
                let _ = child.wait();
                panic!("kill-on-close job did not terminate child: {error}");
            }
        }

        #[test]
        fn signal_cleanup_terminates_registered_job_child() {
            let _guard = super::super::child_job_test_lock();
            let _registration = super::super::child_job_test_cleanup_registration_scope();
            let mut child = Command::new("cmd")
                .args(["/C", "ping -n 30 127.0.0.1 >NUL"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn hung command");
            let job = ChildJob::assign_to_child(&child).expect("assign child to registered job");

            terminate_registered_children_for_process_exit();

            if let Err(error) = wait_for_child_exit(&mut child, Duration::from_secs(2)) {
                let _ = child.kill();
                let _ = child.wait();
                panic!("registered job cleanup did not terminate child: {error}");
            }
            drop(job);
        }

        #[test]
        fn access_denied_assignment_error_mentions_outer_job_constraint() {
            let error = job_assignment_error(io::Error::from_raw_os_error(ERROR_ACCESS_DENIED));

            assert!(error.to_string().contains("outer Windows Job Object"));
            assert!(error.to_string().contains("does not allow breakaway"));
        }

        fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> io::Result<()> {
            let deadline = Instant::now() + timeout;
            loop {
                if child.try_wait()?.is_some() {
                    return Ok(());
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "child still running",
                    ));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

#[cfg(unix)]
mod imp {
    use super::*;
    use std::os::unix::process::CommandExt as _;
    use std::sync::{Mutex, OnceLock};

    static CHILD_GROUPS: OnceLock<Mutex<Vec<libc::pid_t>>> = OnceLock::new();

    pub(crate) struct ChildJob {
        pgid: libc::pid_t,
    }

    impl ChildJob {
        pub(crate) fn prepare_command(command: &mut Command) -> io::Result<()> {
            // SAFETY: `pre_exec` runs in the child process after fork and before
            // exec. The closure only calls async-signal-safe libc functions.
            unsafe {
                command.pre_exec(|| {
                    if libc::setsid() == -1 {
                        return Err(io::Error::last_os_error());
                    }
                    #[cfg(target_os = "linux")]
                    {
                        if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                            return Err(io::Error::last_os_error());
                        }
                    }
                    Ok(())
                });
            }
            Ok(())
        }

        pub(crate) fn assign_to_child(child: &Child) -> io::Result<Self> {
            let pgid = child_pid(child)?;
            register_group(pgid);
            Ok(Self { pgid })
        }

        pub(crate) fn terminate(&self) -> io::Result<bool> {
            signal_process_group(self.pgid, libc::SIGTERM)
        }

        pub(crate) fn force_kill(&self) -> io::Result<bool> {
            if self.pgid <= 0 {
                return Ok(false);
            }
            signal_process_group(self.pgid, libc::SIGKILL)
        }

        #[cfg(not(test))]
        pub(crate) fn detach(&mut self) {
            if self.pgid > 0 {
                unregister_group(self.pgid);
                self.pgid = 0;
            }
        }
    }

    impl Drop for ChildJob {
        fn drop(&mut self) {
            let _ = self.force_kill();
            unregister_group(self.pgid);
        }
    }

    pub(super) fn terminate_registered_children_for_process_exit() {
        let groups = registered_groups();
        for pgid in &groups {
            let _ = signal_process_group(*pgid, libc::SIGTERM);
        }
        std::thread::sleep(SIGNAL_CLEANUP_GRACE);
        for pgid in &groups {
            let _ = signal_process_group(*pgid, libc::SIGKILL);
        }
    }

    fn child_pid(child: &Child) -> io::Result<libc::pid_t> {
        child.id().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("child pid {} does not fit libc::pid_t", child.id()),
            )
        })
    }

    fn child_groups() -> &'static Mutex<Vec<libc::pid_t>> {
        CHILD_GROUPS.get_or_init(|| Mutex::new(Vec::new()))
    }

    fn register_group(pgid: libc::pid_t) {
        let mut groups = child_groups()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !groups.contains(&pgid) {
            groups.push(pgid);
        }
    }

    fn unregister_group(pgid: libc::pid_t) {
        let mut groups = child_groups()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        groups.retain(|registered| *registered != pgid);
    }

    fn registered_groups() -> Vec<libc::pid_t> {
        child_groups()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn signal_process_group(pgid: libc::pid_t, signal: libc::c_int) -> io::Result<bool> {
        if pgid <= 0 {
            return Ok(false);
        }
        // SAFETY: negative pid targets the process group whose id is `pgid`.
        let result = unsafe { libc::kill(-pgid, signal) };
        if result == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(false)
        } else {
            Err(error)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::fs;
        use std::process::Stdio;

        #[test]
        fn force_kill_terminates_child_process_group() {
            let pid_file =
                studiofs_test_support::workspace_target_temp_path("studiofs-e2e-process-group-")
                    .with_extension("pid");
            let mut command = Command::new("sh");
            command
                .args(["-c", "sleep 30 & echo $! > \"$PID_FILE\"; wait"])
                .env("PID_FILE", &pid_file)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            ChildJob::prepare_command(&mut command).expect("prepare isolated process group");
            let mut child = command.spawn().expect("spawn child with grandchild");
            let job = ChildJob::assign_to_child(&child).expect("track child group");
            let grandchild = wait_for_pid_file(&pid_file, Duration::from_secs(2))
                .expect("grandchild pid should be written");

            job.force_kill().expect("kill process group");

            assert!(
                wait_for_child_exit(&mut child, Duration::from_secs(2)).expect("probe child exit"),
                "child shell should exit after process-group kill"
            );
            assert!(
                wait_for_pid_exit(grandchild, Duration::from_secs(2)),
                "grandchild process should exit after process-group kill"
            );
            let _ = fs::remove_file(pid_file);
        }

        fn wait_for_pid_file(path: &std::path::Path, timeout: Duration) -> Option<libc::pid_t> {
            let deadline = Instant::now() + timeout;
            loop {
                if let Ok(contents) = fs::read_to_string(path)
                    && let Ok(pid) = contents.trim().parse()
                {
                    return Some(pid);
                }
                if Instant::now() >= deadline {
                    return None;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        }

        fn wait_for_pid_exit(pid: libc::pid_t, timeout: Duration) -> bool {
            let deadline = Instant::now() + timeout;
            loop {
                if !pid_is_running(pid) {
                    return true;
                }
                if Instant::now() >= deadline {
                    return false;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        }

        fn pid_is_running(pid: libc::pid_t) -> bool {
            // SAFETY: signal 0 probes for pid existence without sending a signal.
            let result = unsafe { libc::kill(pid, 0) };
            result == 0 || io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
        }
    }
}

#[cfg(not(any(windows, unix)))]
mod imp {
    use super::*;

    pub(crate) struct ChildJob;

    impl ChildJob {
        pub(crate) fn prepare_command(_command: &mut Command) -> io::Result<()> {
            Ok(())
        }

        pub(crate) fn assign_to_child(_child: &Child) -> io::Result<Self> {
            Ok(Self)
        }

        pub(crate) fn terminate(&self) -> io::Result<bool> {
            Ok(false)
        }

        pub(crate) fn force_kill(&self) -> io::Result<bool> {
            Ok(false)
        }
    }

    pub(super) fn terminate_registered_children_for_process_exit() {}
}

pub(crate) use imp::ChildJob;
