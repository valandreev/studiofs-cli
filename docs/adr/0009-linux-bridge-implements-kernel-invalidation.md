# Linux Bridge Implements Kernel Invalidation

The Linux platform bridge must implement kernel invalidation from the first production version. StudioFS relies on bridge-level invalidation to clear system cache state after local writes, truncation, remote metadata changes, and other unsafe transitions; `direct_io` does not remove the need to invalidate attributes, dentries, and any kernel-maintained open state. The libfuse3 shim must therefore expose the low-level notification calls needed by the existing `KernelInvalidator` contract.

The implemented bridge uses a bounded 8192-inode coalescing queue with producer
backpressure and one dedicated notifier. Data invalidation dominates
attribute-only invalidation. Notifications are skipped only when lookup/open
liveness proves the inode is unobserved; the liveness lock is never held across
`fuse_lowlevel_notify_inval_inode`. Shutdown removes the VFS producer first,
drains accepted work, and retains the raw session after a bounded timeout
rather than risking notification use-after-free.
