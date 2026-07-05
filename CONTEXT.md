# StudioFS

StudioFS is a local-first virtual filesystem whose platform-specific mount code translates operating-system filesystem callbacks into the shared VFS contract.

## Language

**Platform bridge**:
A thin OS-specific adapter that maps kernel or driver filesystem callbacks to `StudioVfs` operations without owning the shared data plane.
_Avoid_: Mount module, backend data plane

**Linux VFX reference platform**:
The Linux environments StudioFS must support for VFX/media workflows: Ubuntu 24.04 LTS and above, plus Rocky Linux 9.4 and above.
_Avoid_: Generic Linux, latest Linux

**Linux best-effort platform**:
A Linux environment StudioFS may support when the same bridge works without compromising the reference platforms; currently Rocky Linux 8.6 and above is best-effort.
_Avoid_: Compatibility floor, required platform

**Mount UX**:
The user-facing mount experience: `studiofs mount <disk-name> <mount-point>` plus only storage-location controls for local cache, cache size, and S3 endpoint override.
_Avoid_: Tuning surface, performance profile CLI
