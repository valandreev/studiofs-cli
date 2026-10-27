# Mount tray indicator

Foreground mounts automatically try to show synchronization status in the system
tray or menu bar. No `--tray` option is needed. With `mount --detach`, the
background mount process owns its tray; it remains after the launching command
returns and closes when that mount exits.
The indicator describes work accepted by that mount.

On Linux, StudioFS 0.2.1 uses the GTK-free KSNI/StatusNotifierItem backend over
the user's session D-Bus. A desktop host such as KDE Plasma is needed to display
the icon. GNOME needs a compatible AppIndicator/KStatusNotifierItem extension;
GNOME hosts may not display tooltips even though StudioFS supplies the data.
GTK3, GLib, libxdo and AppIndicator libraries are no longer StudioFS tray
prerequisites. Windows and macOS retain their native backends.

Without a usable session bus, Linux mounts continue without an indicator.
The Linux tray runs in a separate, owned helper process and needs no X11/Wayland
event loop. It observes the mount parent's local Health diagnostics using the
same classifier and animation as the native Windows/macOS tray. If the bus is usable but its StatusNotifierWatcher
is absent, the upstream backend waits for it and registers when it appears or
restarts. An absent icon does not confirm synchronization. A tray
initialization error never dispatches a second mount. The parent serves the mount
independently of tray startup, updates and destruction. After mount completion it
closes the helper's control pipe, allows two seconds for normal shutdown, then
terminates and reaps a helper blocked in D-Bus. The original mount result is
preserved. Linux also kills the helper if its mount parent dies. For detached
mounts, that parent is the serving daemon, so the tray survives the launcher's
exit. Windows/macOS retain their native event-loop lifecycle.

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

The tray polls mount-local Health diagnostics every 300 ms while starting,
active, or unavailable, and every 750 ms when idle, with a one-second request
timeout. Health reads runtime counters and pending work without enumerating
inode lifecycles; per-inode ZeroIO details are computed only for explicit
RuntimeSnapshot requests. Reported activity expires after two seconds without
a fresh heartbeat, including active upload estimates. Startup remains in its
starting state during the five-second diagnostics grace period. The icon animation
itself performs no PostgreSQL or S3 requests. The same runtime contract and color mapping serve
Windows, macOS and Linux.

Polling pauses are interrupted during shutdown. An in-flight diagnostics request
retains its one-second timeout.

The runtime checks the durable acknowledgement of its own local metadata
journal, not a moving global peer head. It reads that boundary before and after
checking writer, staging, upload and publication owners. Activity leases remain
visible while work moves between those owners, including delayed publication
retries. Overlapping activity, incomplete startup or missing proof prevents a
safe result even when the visible queues happen to be empty.

## Focused Linux backend checks

The tests below need `dbus-run-session` (`dbus-daemon` on Ubuntu) and GNU
`timeout`. They exercise real backend startup, icon/tooltip updates and shutdown
without a watcher, and the initialization-error path without a session bus. The Ubuntu CI job runs both
explicitly despite their `#[ignore]` attributes. Run from the repository root:

```bash
mkdir -p target
cargo test --locked -p studiofs --bin studiofs --no-run --message-format=json > target/tray-test-artifacts.json
timeout 20s dbus-run-session -- cargo test --locked -p studiofs --bin studiofs \
  tray::supported::tests::linux_backend_updates_and_stops_without_a_watcher \
  -- --exact --ignored --nocapture
timeout 20s env DBUS_SESSION_BUS_ADDRESS=unix:path=/nonexistent/studiofs-sfs-884 \
  cargo test --locked -p studiofs --bin studiofs \
  tray::supported::tests::linux_backend_returns_error_without_a_session_bus \
  -- --exact --ignored --nocapture
cargo test --locked -p studiofs --bin studiofs tray::linux::tests:: -- --nocapture
cargo build --locked -p studiofs
timeout 30s dbus-run-session -- /usr/bin/python3 tools/test-linux-tray.py target/debug/studiofs target/tray-test-artifacts.json
```

Build the test binary first so compilation does not consume the timeout. Run the
tests separately: one needs a working isolated bus, the other an invalid bus
address. The raw backend timeouts bound each test process, including destruction.
The process-owner tests check normal EOF exit and forced cleanup of a stopped
helper. The Python fault tests use `python3-dbus` and `python3-gi`: an isolated
socket never answers authentication, or a private watcher never replies to
registration. They verify that the rebuilt CLI still returns its mount error
and releases the helper. A separate readiness-controlled fixture finishes backend
startup without a watcher, introduces a nonreplying late watcher, then verifies
that the process owner kills and reaps the backend blocked in destruction.
The daemon re-execution case also verifies that detached mounts start their tray.
These checks do not
establish rendering, watcher restart behavior or mounted lifecycle.
Before release, rebuild `studiofs`, check all four states on a real KDE/SNI host
and GNOME with its extension, start/restart the watcher, and exercise foreground
mount exit/error propagation and detached/headless operation. Record the host,
extension and actual tooltip behavior separately from compile/test results.
