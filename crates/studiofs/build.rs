#![allow(missing_docs)]

fn main() {
    // Required for winfsp delayed loading — must be in the final binary's build.rs
    // so the linker flag /DELAYLOAD:winfsp-x64.dll applies to the exe, not just the lib.
    #[cfg(windows)]
    winfsp::build::winfsp_link_delayload();
}
