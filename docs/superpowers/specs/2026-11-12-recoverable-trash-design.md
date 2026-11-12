# Recoverable trash for StudioFS 0.2.3

Status: product interview completed; implementation and native feasibility
validation remain pending. This records the contract, not an implementation plan.

## User decisions

- Match the local-disk experience through the operating system's native trash
  UI, including its explicit collision choices and confirmation behavior.
- Retain only explicit OS **Move to Trash** operations. `rm`, `rm -rf`,
  Shift+Delete, ordinary save/overwrite, and rename replacement bypass trash.
- Support files, directories, hard links, and still-open files according to
  native OS behavior; do not convert every unlink into recoverable deletion.
- Trash is global per volume across Windows, Linux, and macOS. Clearing trash
  is global too, subject to each principal's current access and write rights.
- Read-only access permits listing. Permanent purge, including Clear, requires
  current write access on the original parent. This supersedes the earlier
  interview shorthand that every visible entry could be purged.
  Visibility follows current permissions on the original parent, not the
  permissions recorded at deletion. No access means the entry is hidden.
- There is **no separate administrator bypass in 0.2.3**. Use the current/default
  user model; ACL redesign is deferred. This supersedes the earlier admin-all
  requirement in SFS-1054/SFS-1055.
- Track the original parent's identity: moving it preserves the relationship
  and uses its current permissions. If it is missing, hide the entry; neither
  an ancestor nor a new directory at the same path grants access.
- Trash TTL is configured in whole days for the entire volume. At volume
  creation the default is zero: trash is disabled. Positive values enable
  recoverable OS Move to Trash with that retention period. This explicitly
  supersedes the earlier seven-day default. The user delegated the simplest
  retention-change behavior to engineering.
- Provide a CLI command to change the shared volume's trash-days setting after
  creation, including enabling it with a positive value and disabling it with
  zero. Replicate the versioned shared setting to peers; this requires a new
  mutable volume-setting path within the enforced compatibility boundary,
  not machine-local CLI settings or in-place mutation of immutable FormatInfo.
- Offline, unpublished files remain recoverable locally. Peers gain visibility
  only after the necessary metadata replication and payload publication.
- Retained trash counts toward the logical volume quota until purge/expiry;
  it is not automatically purged early for capacity pressure. Hidden entries
  can block new writes until expiry; the user explicitly accepted this outcome.
- Native collision UI governs an explicit restore choice. Never overwrite
  silently; preserve recoverability when a selected restore cannot complete.

## Engineering choices and implementation obligations

- For positive TTL, apply the current period to all entries, measured from each
  entry's initial deletion time. Expiry runs in the background; shortening the
  positive period can make existing entries eligible immediately. Zero disables
  new retention, not an implicit purge or indefinite expiry suspension for
  existing data. Existing entries expire using the last positive TTL; retain
  that period when disabling. Re-enabling uses the new positive period.
- Preserve the existing ACL-principal trust boundary. A configured mount
  principal is not authenticated per-request OS identity; make no new security
  claim. No new authentication framework or special admin principal is required.
- Listing does not authorize purge without original-parent write access.
  Restore requires current original-parent and destination write permissions.
  A denied source or destination leaves the entry
  recoverable; it does not grant a write bypass.
- Charge retained trash against the future logical quota until purge/expiry.
  Snapshots or other references may retain physical payload bytes afterward;
  logical quota is not an S3 billing limit.
- Protect local unpublished payloads, live links, open handles, and snapshots.
  Reuse the existing metadata/publication and janitor/GC lifecycle; never create
  a second independent destructive cleanup path.
- Make local transitions atomic/retryable and define authority/version fencing
  for offline restore versus peer purge/expiry. Do not promise one simultaneous
  global winner while clients are offline. Local purge alone never authorizes
  remote payload GC; shared settlement and live/snapshot references fence it.
  A failed or conflicting restore must not discard accepted local data. Define
  and test the conflict outcome before enabling retained data.
- Do not purge before retention expires to relieve disk or quota pressure.
- Establish an enforced persisted-format/capability boundary that incompatible
  old clients actually reject before writing any retained entries. Documentation
  alone or an unknown flag silently ignored by old clients is insufficient.
  Preserve local-first callbacks and durability;
  trash does not add foreground PostgreSQL or S3 dependencies.

## Approach and current evidence

Prefer native trash integration with one shared retained-data lifecycle.
Platform-specific per-user trash folders and metadata are views of the same
volume store, filtered by current permissions and adapted to the local mount
path. Exact synthesis/storage mechanisms require platform feasibility evidence.
Universal unlink retention is rejected because it contradicts the user's
explicit bypass operations. Exact platform mechanisms remain implementation
work, and must establish that OS trash intent is distinguishable safely.

Today `docs/platform-bridges.md` (Trash and recycle-bin behavior) documents
immediate deletion for trash renames. The existing VFS entry point is
`crates/studiofs-vfs/src/vfs/namespace_ops.rs`:
`delete_source_tree_and_invalidate_trash_target`.
`crates/studiofs-vfs/src/vfs/acl.rs` evaluates a mount-wide configured principal,
wildcard fallback, and inherited ACLs. POSIX mode/uid/gid enforcement is separate;
WinFSP currently returns a shared descriptor granting Everyone full access
(`crates/studiofs-winfsp/src/context.rs`). None establishes a volume-admin role.
Per-user OS trash folder names do not add identity isolation: all OS users of
one mount share its configured VFS principal; without one, wildcard ACLs apply.
Keep separate OS permission enforcement intact and document these limitations.
An original parent that is itself trashed counts as missing until restored;
its previous standalone trash entries remain hidden meanwhile. A directory
retained as one entry keeps its children as part of that tree.
The mounted durability boundary remains
`docs/juicefs-local-first-rust-contract.md`.

## Acceptance and validation

1. Native Move to Trash, list, restore, and clear work across supported clients;
   direct `rm`, Shift+Delete, save, and rename replacement do not create entries.
2. Revoking original-parent access hides an entry. Moving that parent preserves
   its identity; deleting/recreating its path does not restore access.
3. Native collision choices require explicit selection. Destination permission
   denial at either original parent or destination and failed restore leave
   the retained data available for retry.
4. New volumes default to TTL zero (disabled); positive whole-day values enable
   retention. Period changes and expiry use a controllable clock. Setting zero
   stops new retention; existing data expires under the last positive TTL.
   Verify creation and the post-creation CLI update, including peer convergence.
   Read-only principals can list but cannot purge. Clear/purge requires current
   original-parent write access and cannot include hidden entries.
5. Offline local-only data survives delete/restart/restore; peer visibility
   follows successful replication/publication without claiming immediate sync.
6. Focused failure/concurrency tests cover restore versus purge/expiry, retained
   payload GC, snapshot overlap, hard links, and open handles. Verify quota
   charging when SFS-1058 is implemented. No full suites are required here.

Native UI integration, intent recognition, and cross-platform behavior remain
unproven capabilities. Record Windows, Linux, and macOS mounted/native evidence
separately; unit tests do not prove native recycle-bin behavior. Update English
and Ukrainian guides and the platform contract with the implemented guarantee.
Before implementing platform views, prove that native software can discover and
open trash roots, Explorer offers recycling on a StudioFS WinFSP disk volume,
and per-SID recycle-bin behavior works with the configured security descriptor.
If native integration is impossible, report concrete evidence before changing
the user's native-UI requirement. Do not silently replace it with CLI-only UX.

Delivery order: SFS-1055 approval, version bump to 0.2.3, SFS-1054,
SFS-1056, SFS-1057, then SFS-1058. No ExecPlan is created by this document.
