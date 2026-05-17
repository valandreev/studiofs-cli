//! Process containment helpers for spawned E2E children.

use std::io;
use std::process::Child;

#[cfg(windows)]
mod imp {
    use super::*;
    use std::ffi::c_void;
    use std::mem;
    use std::os::windows::io::AsRawHandle as _;
    use std::ptr;

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

    impl ChildJob {
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

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};

        #[test]
        fn kill_on_close_job_terminates_assigned_child() {
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

#[cfg(not(windows))]
mod imp {
    use super::*;

    pub(crate) struct ChildJob;

    impl ChildJob {
        pub(crate) fn assign_to_child(_child: &Child) -> io::Result<Self> {
            Ok(Self)
        }

        pub(crate) fn terminate(&self) -> io::Result<bool> {
            Ok(false)
        }
    }
}

pub(crate) use imp::ChildJob;
