# Linux Bridge Implements Kernel Invalidation

The Linux platform bridge must implement kernel invalidation from the first production version. StudioFS relies on bridge-level invalidation to clear system cache state after local writes, truncation, remote metadata changes, and other unsafe transitions; `direct_io` does not remove the need to invalidate attributes, dentries, and any kernel-maintained open state. The libfuse3 shim must therefore expose the low-level notification calls needed by the existing `KernelInvalidator` contract.
