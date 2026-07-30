# Metadata-authority contract with local allocators

## Decision

Keep `MetadataAuthorityStatus` and the existing shared VFS gate. Its two fields
remain independent: `inode_id_headroom` reports whether the trusted inode
allocator can issue another ID, and `object_id_headroom` does the same for the
payload allocator. `allows_new_mutations()` remains their conjunction, so a
terminal condition in either allocator makes the mount read-only for new
mutations.

For a PostgreSQL-backed engine, both flags are true after a complete trusted
paired allocator record is loaded, regardless of PostgreSQL connectivity. They
remain true while each allocator serves or durably reserves local intervals.
Local-counter exhaustion sets the affected flag false and does not wrap or
rotate the namespace.

At `PgMetaEngine::open_with_options`, a new or untrustworthy paired record
needs PostgreSQL namespace registration. If registration and the subsequent
paired redb write commit, both flags become true. If PostgreSQL is unavailable,
or a legacy volume blocks registration, both flags are false: the known local
view opens for reads only. There is no foreground or background retry; close
and reopen after PostgreSQL is available to retry non-legacy recovery.

## VFS and platform behavior

`StudioVfs::enforce_metadata_mutation_authority` remains the sole policy point.
It returns the existing `MetaError::AccessDenied { actual: ReadOnly }` for new
write-capable opens, writes through an existing handle, and namespace
mutations. Read-only opens and reads of known local data remain available. No
macFUSE, Linux FUSE, or WinFSP-specific allocator branch or errno mapping is
added.

Range-window prefetch state, warnings, and PostgreSQL reservation retries are
removed with the old allocator. Replace them with one typed `tracing` event for
each failed bootstrap/recovery attempt, legacy-volume guard, or counter
exhaustion. Include volume, allocator state, and PostgreSQL error when present;
the two status flags expose availability rather than a new diagnostic service.

Direct redb keeps its existing local-only allocation semantics and remains
writable through its normal metadata authority status.

## Proof

Tests must show trusted local state allocates while PostgreSQL is offline; an
untrustworthy or legacy state is readable but blocks mutations; an existing
writer is blocked after downgrade; and reopening after PostgreSQL recovery
restores authority only after the paired state commits. The shared VFS test
must remain the platform-neutral proof for macFUSE, Linux FUSE, and WinFSP.
