# Linux libfuse3 FFI Uses A Small C Shim

The Linux libfuse3 bridge will use a small C shim plus narrow Rust `extern` declarations instead of generating broad bindgen bindings. This matches the existing macFUSE bridge shape: C owns the `fuse_lowlevel_ops` layout and tiny helper accessors, while Rust owns mount lifecycle, callback translation, and `StudioVfs` calls. The narrower shim keeps Rocky Linux builds simpler and avoids exposing the whole libfuse API surface as a StudioFS dependency.
