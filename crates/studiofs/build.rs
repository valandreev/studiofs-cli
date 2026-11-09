//! Build script for `studiofs`.
//! Ensures WinFSP delayed-loading linker flags are applied at the binary
//! link stage so `/DELAYLOAD:winfsp-x64.dll` affects the final executable.

// Shared with `tests/build_version.rs`, which cannot `include!` a file with inner docs.
include!("build_support.rs");
