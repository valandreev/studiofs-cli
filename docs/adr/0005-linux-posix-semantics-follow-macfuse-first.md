# Linux POSIX Semantics Follow macFUSE First

When existing platform bridges differ in callback sequencing or OS-specific details, the Linux bridge should use macOS/macFUSE as the primary semantic reference because both bridges expose POSIX/FUSE-style inode callbacks. Windows WinFSP remains a user-visible parity floor for outcomes such as read/write behavior, rename/delete results, attributes, and symlink visibility, but Linux must not copy Windows-specific cleanup, cache-manager, or reparse-point sequencing unless the Linux workload requires an equivalent.
