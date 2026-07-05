# Linux Mount Requires System libfuse3

The Linux platform bridge will require the system-provided libfuse3 development/runtime packages and `fusermount3`. StudioFS will not vendor, bundle, or build libfuse3 from source for the normal Linux mount path. Ubuntu 24.04+ and Rocky Linux 9.4+ support should use their system packages; Rocky Linux 8.6+ remains best-effort and must not force dependency bundling into the reference-platform build.
