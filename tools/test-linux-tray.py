#!/usr/bin/env python3
"""Exercise real Linux mount/tray cleanup against private, nonresponsive buses."""

import os
import json
from pathlib import Path
import selectors
import socket
import subprocess
import sys
import tempfile
import time
import unittest

if len(sys.argv) < 3:
    sys.exit("usage: test-linux-tray.py <binary> <artifacts.json> [unittest options]")

import dbus
import dbus.service
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GLib


BINARY = Path(sys.argv.pop(1)).resolve()
ARTIFACTS = map(json.loads, Path(sys.argv.pop(1)).read_text().splitlines())
TEST_BINARY = next(row["executable"] for row in ARTIFACTS
                   if row.get("reason") == "compiler-artifact"
                   and row["profile"]["test"] and row["target"]["name"] == "studiofs")
TARGET = Path(__file__).resolve().parents[1] / "target"


class TrayProcessTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory(prefix="tray-stalled-", dir=TARGET)
        self.addCleanup(self.root.cleanup)

    def mount(self, address, **overrides):
        env = dict(os.environ, STUDIOFS_CLIENT_ROOT=self.root.name,
                   DBUS_SESSION_BUS_ADDRESS=address)
        env.pop("STUDIOFS_DAEMON_MOUNT_POINT", None)
        env.update(overrides)
        for name in ("DISPLAY", "WAYLAND_DISPLAY", "WAYLAND_SOCKET"):
            env.pop(name, None)
        child = subprocess.Popen(
            [str(BINARY), "mount", "tray-uninitialized",
             str(Path(self.root.name) / "mnt"), "--foreground"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        self.addCleanup(self.reap, child)
        return child

    @staticmethod
    def reap(child):
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5)

    def assert_mount_error(self, child):
        _stdout, stderr = child.communicate(timeout=6)
        self.assertEqual(child.returncode, 1, stderr)
        self.assertIn("format info not found", stderr)
        self.assertNotIn("panicked", stderr)

    def test_ignored_fixtures_exit_without_a_controller(self):
        env = dict(os.environ)
        env.pop("STUDIOFS_TRAY_TEST_FIXTURE", None)
        for fixture in ("backend_drop_fixture", "owned_backend_fixture"):
            with self.subTest(fixture=fixture):
                child = subprocess.Popen(
                    [TEST_BINARY, "--exact", f"tray::linux::tests::{fixture}",
                     "--ignored", "--nocapture", "--test-threads=1"],
                    env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                self.addCleanup(self.reap, child)
                # Keep stdin open: communicate() would hide the wait by sending EOF.
                child.wait(timeout=5)
                stdout, stderr = child.communicate(timeout=1)
                self.assertEqual(child.returncode, 0, (stdout, stderr))
                self.assertIn(b"1 passed", stdout)
                self.assertNotIn(b"backend-ready", stdout)
                self.assertNotIn(b"owner-ready", stdout)

    def assert_mount_survives_stalled_bus_authentication(self, **overrides):
        address = str(Path(self.root.name) / "bus")
        with socket.socket(socket.AF_UNIX) as listener:
            listener.bind(address)
            listener.listen(1)
            listener.settimeout(5)
            child = self.mount("unix:path=" + address, **overrides)
            connection, _ = listener.accept()
            with connection:
                connection.settimeout(5)
                self.assertTrue(connection.recv(256), "observe authentication request")
                # Never answer authentication; cleanup must close the real backend socket.
                self.assert_mount_error(child)
                while connection.recv(256):
                    pass

    def test_mount_result_survives_stalled_bus_authentication(self):
        self.assert_mount_survives_stalled_bus_authentication()

    def test_reexecuted_detached_mount_still_starts_tray(self):
        self.assert_mount_survives_stalled_bus_authentication(
            STUDIOFS_DAEMON_MOUNT_POINT=str(Path(self.root.name) / "mnt"))

    def test_mount_result_survives_stalled_watcher_registration(self):
        DBusGMainLoop(set_as_default=True)
        bus = dbus.SessionBus(private=True)
        self.addCleanup(bus.close)
        loop = GLib.MainLoop()
        registrations = []

        class Watcher(dbus.service.Object):
            @dbus.service.method("org.kde.StatusNotifierWatcher", in_signature="s",
                                 out_signature="", async_callbacks=("reply", "error"))
            def RegisterStatusNotifierItem(self, name, reply, error):
                registrations.append(str(name))
                loop.quit()  # Deliberately retain the call without replying.

        watcher = Watcher(bus, "/StatusNotifierWatcher")
        self.addCleanup(watcher.remove_from_connection)
        name = dbus.service.BusName("org.kde.StatusNotifierWatcher", bus=bus, do_not_queue=True)
        self.addCleanup(bus.release_name, name.get_name())
        child = self.mount(os.environ["DBUS_SESSION_BUS_ADDRESS"])
        timer = GLib.timeout_add(5000, lambda: (loop.quit(), False)[1])
        loop.run()
        if registrations:
            GLib.source_remove(timer)
        self.assertTrue(registrations, "observe blocked backend registration")
        helper_pid = int(registrations[0].split("-")[1])
        self.assert_mount_error(child)
        with self.assertRaises(ProcessLookupError, msg="helper must be killed and reaped"):
            os.kill(helper_pid, 0)

    def test_helper_dies_with_mount_parent(self):
        address = str(Path(self.root.name) / "bus")
        with socket.socket(socket.AF_UNIX) as listener:
            listener.bind(address)
            listener.listen(1)
            listener.settimeout(5)
            child = self.mount("unix:path=" + address)
            connection, _ = listener.accept()
            with connection:
                connection.settimeout(5)
                self.assertTrue(connection.recv(256), "observe blocked authentication")
                child.kill()
                child.wait(timeout=5)
                while connection.recv(256):
                    pass
                child.communicate(timeout=5)

    def test_owner_reaps_backend_when_late_watcher_blocks_drop(self):
        DBusGMainLoop(set_as_default=True)
        bus = dbus.SessionBus(private=True)
        self.addCleanup(bus.close)
        self.assertFalse(bus.name_has_owner("org.kde.StatusNotifierWatcher"))
        loop = GLib.MainLoop()
        registrations = []

        class Watcher(dbus.service.Object):
            @dbus.service.method("org.kde.StatusNotifierWatcher", in_signature="s",
                                 out_signature="", async_callbacks=("reply", "error"))
            def RegisterStatusNotifierItem(self, name, reply, error):
                registrations.append(str(name))
                loop.quit()

        owner = subprocess.Popen(
            [TEST_BINARY, "--exact", "tray::linux::tests::owned_backend_fixture",
             "--ignored", "--nocapture"],
            env=dict(os.environ, STUDIOFS_TRAY_TEST_FIXTURE="1"),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.addCleanup(self.reap, owner)
        with selectors.DefaultSelector() as ready:
            ready.register(owner.stdout, selectors.EVENT_READ)
            output = b""
            deadline = time.monotonic() + 5
            while b"owner-ready " not in output or not output.endswith(b"\n"):
                self.assertTrue(ready.select(max(0, deadline - time.monotonic())),
                                "backend initialization must finish without a watcher")
                chunk = os.read(owner.stdout.fileno(), 4096)
                self.assertTrue(chunk, output)
                output += chunk
        watcher = Watcher(bus, "/StatusNotifierWatcher")
        self.addCleanup(watcher.remove_from_connection)
        name = dbus.service.BusName("org.kde.StatusNotifierWatcher", bus=bus, do_not_queue=True)
        self.addCleanup(bus.release_name, name.get_name())
        timer = GLib.timeout_add(5000, lambda: (loop.quit(), False)[1])
        loop.run()
        if registrations:
            GLib.source_remove(timer)
        self.assertTrue(registrations, "observe late registration in the running service")
        helper_pid = int(registrations[0].split("-")[1])
        self.assertIn(f"owner-ready {helper_pid}".encode(), output)
        stdout, stderr = owner.communicate(input=b"x", timeout=6)
        self.assertEqual(owner.returncode, 0, (stdout, stderr))
        self.assertIn(b"1 passed", stdout)
        with self.assertRaises(ProcessLookupError, msg="stuck backend must be reaped"):
            os.kill(helper_pid, 0)


if __name__ == "__main__":
    unittest.main()
