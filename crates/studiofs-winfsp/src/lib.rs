//! StudioFS WinFSP backend for Windows.
//!
//! Uses `winfsp-rs` crate to expose StudioVfs as a Windows drive.

// This crate only compiles on Windows.
// On Linux/macOS, studiofs-fuse is used instead.
