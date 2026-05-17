//! Build script for the `studiofs-winfsp` crate.
//!
//! This script invokes WinFSP delayed-load linkage initialization so the
//! Windows-specific runtime dependencies are prepared during compilation.

fn main() {
    // Required for winfsp delayed loading.
    #[cfg(windows)]
    winfsp::build::winfsp_link_delayload();
}
