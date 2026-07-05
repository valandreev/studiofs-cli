# Linux FUSE Bridge Lives In studiofs-linux-fuse

The new Linux platform bridge will live in a new crate named `studiofs-linux-fuse`. The old `studiofs-fuse` name is historically tied to removed fuser/raw-FUSE attempts, while the new crate name makes the platform boundary explicit: Linux-only libfuse3 low-level callbacks over the shared `StudioVfs` contract. The user-facing command remains `studiofs mount <disk-name> <mount-point>`.
