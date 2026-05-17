//! Build script for `studiofs`.
//!
//! Ensures WinFSP delayed-loading linker flags are applied at the binary
//! link stage so `/DELAYLOAD:winfsp-x64.dll` affects the final executable.

/// Entry point for the build script.
fn main() {
    // Required for WinFSP delayed loading — must be in the final binary's build.rs
    // so the linker flag /DELAYLOAD:winfsp-x64.dll applies to the exe, not just the lib.
    #[cfg(windows)]
    winfsp::build::winfsp_link_delayload();
}
