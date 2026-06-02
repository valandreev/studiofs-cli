//! Platform-neutral helpers shared by StudioFS bridge adapters.
//!
//! This crate is limited to behavior-neutral conversions that can be reused by
//! Linux FUSE and future platform bridges. Platform quirks such as macOS Finder
//! metadata, xattr flags, Unicode normalization, and cache policy stay in the
//! concrete bridge crates.

#[cfg(unix)]
/// POSIX bridge conversions shared by Unix bridge adapters.
pub mod posix;
