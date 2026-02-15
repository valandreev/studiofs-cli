fn main() {
    // Required for winfsp delayed loading.
    #[cfg(windows)]
    winfsp::build::winfsp_link_delayload();
}
