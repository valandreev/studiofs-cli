# Mount tray indicator

Run a mount with `--tray` to show its synchronization status in the system tray
or menu bar. The indicator describes work accepted by that mount.

The red pulse means local changes still need synchronization: writer data,
staged or retrying uploads, file-version publication, or metadata acknowledgement.
An upload finishing does not by itself mean the file is published. Keep the
machine running while synchronization is pending; restore connectivity if the
mount is offline.

The animated blue/yellow Ukrainian flag appears only when the mount's shared
runtime contract confirms remote completion of accepted data and metadata.
Ordinary reads, prefetch and unrelated peer writes do not themselves represent
local outbound work. Startup, missing legacy telemetry, failed verification and
stale or unavailable status cannot certify completion and do not show the flag.

The tooltip says “safe to shut down” only for a fresh confirmed observation.
Applications can write again after that observation. Close applications and
unmount normally before powering off; the tray observation does not replace the
filesystem's shutdown barriers. Local crash recovery from cache is a separate,
weaker guarantee than this indicator's remote synchronization guarantee.

The tray polls mount-local diagnostics every 300 ms, with a one-second request
timeout. Reported activity expires after two seconds without a fresh heartbeat,
including active upload estimates. Startup remains in its starting state during
the five-second diagnostics grace period. The icon animation itself performs no
PostgreSQL or S3 requests. The same runtime contract and color mapping serve
Windows, macOS and Linux.

The runtime checks the durable acknowledgement of its own local metadata
journal, not a moving global peer head. It reads that boundary before and after
checking writer, staging, upload and publication owners. Activity leases remain
visible while work moves between those owners, including delayed publication
retries. Overlapping activity, incomplete startup or missing proof prevents a
safe result even when the visible queues happen to be empty.
