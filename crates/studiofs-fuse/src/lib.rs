//! StudioFS FUSE backend for Linux and macOS.
//!
//! Uses `fuser` crate to expose StudioVfs as a FUSE filesystem.

// This crate only compiles on Unix platforms.
// On Windows, studiofs-winfsp is used instead.
