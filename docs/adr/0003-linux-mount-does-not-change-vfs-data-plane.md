# Linux Mount Does Not Change VFS Data Plane

The Linux platform bridge work is scoped to the actual Linux mount layer: libfuse3 low-level callbacks, mount lifecycle, callback-to-`StudioVfs` translation, and per-file/per-handle bridge policy. The shared VFS data plane already performs well on Windows and macOS, so this effort must not redesign or retune VFS writer/cache/read semantics unless a Linux-mounted correctness bug proves the shared contract itself is wrong.
