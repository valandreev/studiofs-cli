#!/usr/bin/env python3
"""Unprivileged checks for the mount helper's configuration boundary."""
import importlib.machinery
import importlib.util
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch


loader = importlib.machinery.SourceFileLoader(
    "mount_helper", str(Path(__file__).with_name("mount.fuse.studiofs")))
spec = importlib.util.spec_from_loader(loader.name, loader)
helper = importlib.util.module_from_spec(spec)
loader.exec_module(helper)


class MountCommandTests(unittest.TestCase):
    def setUp(self):
        self.env = dict(STUDIOFS_BINARY="/test/studiofs", TEST_DEV="xfstest-test",
                        SCRATCH_DEV="xfstest-scratch", TEST_DIR="/test/xfstest-test",
                        SCRATCH_MNT="/test/xfstest-scratch")

    def test_maps_both_volumes_and_read_only(self):
        for source, target in [("xfstest-test", "/test/xfstest-test"),
                               ("xfstest-scratch", "/test/xfstest-scratch")]:
            command = helper.mount_command(source, target, "ro,allow_other", self.env)
            self.assertEqual(command[:4], ["/test/studiofs", "mount", source, target])
            self.assertIn("--foreground", command)
            self.assertIn("--read-only", command)
            self.assertIn("--allow-other=true", command)

    def test_accepts_noatime_as_no_op(self):
        command = helper.mount_command("xfstest-test", "/test/xfstest-test", "rw,noatime", self.env)
        self.assertNotIn("--read-only", command)
        self.assertFalse(any("atime" in arg for arg in command))

    def test_rejects_unknown_source_target_and_options(self):
        for source, target, options in [
            ("test", "/test/xfstest-test", "rw"),
            ("xfstest-test", "/test/xfstest-scratch", "rw"),
            ("xfstest-test", "/test/xfstest-test", "remount,ro"),
            ("xfstest-test", "/test/xfstest-test", "relatime"),
            ("xfstest-test", "/test/xfstest-test", "strictatime"),
        ]:
            with self.subTest(source=source, target=target, options=options):
                with self.assertRaises(ValueError):
                    helper.mount_command(source, target, options, self.env)

    def test_mountpoint_basename_must_match_volume(self):
        self.env["TEST_DIR"] = "/test/parent"
        with self.assertRaises(ValueError):
            helper.mount_command("xfstest-test", "/test/parent", "rw", self.env)

    def test_volumes_must_be_distinct(self):
        self.env["SCRATCH_DEV"] = self.env["TEST_DEV"]
        with self.assertRaises(ValueError):
            helper.mount_command("xfstest-test", "/test/xfstest-test", "rw", self.env)

    def test_mount_table_requires_exact_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            table = Path(directory) / "mounts"
            table.write_text("other /test/xfstest-test fuse.studiofs rw 0 0\n"
                             "xfstest-test /test/xfstest-test-other fuse.studiofs rw 0 0\n")
            self.assertFalse(helper.attached("xfstest-test", "/test/xfstest-test", table))
            table.write_text("xfstest-test /test/xfstest-test fuse.studiofs rw 0 0\n")
            self.assertTrue(helper.attached("xfstest-test", "/test/xfstest-test", table))


class MountReadinessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.target = str(root / "xfstest-test")
        self.env = dict(os.environ)
        self.env.update(STUDIOFS_BINARY="/test/studiofs", TEST_DEV="xfstest-test",
                        SCRATCH_DEV="xfstest-scratch", TEST_DIR=self.target,
                        SCRATCH_MNT=str(root / "xfstest-scratch"),
                        STUDIOFS_CLIENT_ROOT=str(root / "client"),
                        XFSTESTS_RUN=str(root / "run"))

    def tearDown(self):
        self.temp.cleanup()

    def run_helper(self, process, run, *, monotonic=None, executable=None, mounted=True):
        with patch.object(helper.os, "environ", self.env), \
             patch.object(helper.sys, "argv", ["mount.fuse.studiofs", "xfstest-test",
                                                self.target, "-o", "rw"]), \
             patch.object(helper.subprocess, "Popen", return_value=process) as popen, \
             patch.object(helper.subprocess, "run", side_effect=run) as run_command, \
             patch.object(helper, "attached", return_value=mounted), \
             patch.object(helper.os, "killpg") as killpg:
            mono_patch = patch.object(helper.time, "monotonic", side_effect=monotonic) if monotonic else patch.object(helper.time, "monotonic", return_value=0)
            exe_patch = patch.object(helper.sys, "executable", executable) if executable is not None else patch.object(helper.sys, "executable", helper.sys.executable)
            with mono_patch, exe_patch:
                try:
                    result = helper.main()
                except OSError as error:
                    result = error
        return result, popen, run_command, killpg

    def test_exited_daemon_reports_not_ready_without_cleanup_commands(self):
        class ExitedProcess:
            pid = 123

            def poll(self):
                return 1

        result, _, run_command, killpg = self.run_helper(ExitedProcess(), [], mounted=False)

        self.assertEqual(result, 1)
        run_command.assert_not_called()
        killpg.assert_not_called()

    def test_exited_daemon_detaches_its_mount_without_killing_process(self):
        class ExitedProcess:
            pid = 123

            def poll(self):
                return 1

        for statuses in ((0,), (1, 0)):
            with self.subTest(statuses=statuses):
                result, _, run_command, killpg = self.run_helper(
                    ExitedProcess(), [subprocess.CompletedProcess([], code) for code in statuses])
                self.assertEqual(result, 1)
                self.assertEqual(run_command.call_count, len(statuses))
                if len(statuses) == 2:
                    self.assertIn("/usr/bin/umount", run_command.call_args.args[0])
                killpg.assert_not_called()


    def test_probe_launch_failure_cleans_up_live_daemon(self):
        class LiveProcess:
            pid = 123
            returncode = None

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                self.returncode = 0
                return 0

        process = LiveProcess()
        result, _, run_command, killpg = self.run_helper(
            process,
            [FileNotFoundError("probe interpreter missing"), subprocess.CompletedProcess([], 0)],
            monotonic=[0, 0])

        self.assertIsInstance(result, FileNotFoundError)
        self.assertEqual(run_command.call_count, 2)  # failed probe launch, then FUSE detach
        killpg.assert_called_once_with(123, signal.SIGTERM)
        self.assertEqual(process.returncode, 0)

    def test_pid_record_failure_cleans_up_launched_daemon(self):
        process = Mock(pid=123)
        process.poll.return_value = None
        with patch.object(Path, "write_text", side_effect=OSError("artifact disk full")):
            result, _, _, killpg = self.run_helper(
                process, [subprocess.CompletedProcess([], 0)])
        self.assertIsInstance(result, OSError)
        killpg.assert_called_once_with(123, signal.SIGTERM)
        process.wait.assert_called_once()

    def test_sanitized_path_gets_daemon_path_and_absolute_probe_interpreter(self):
        class LiveProcess:
            pid = 123

            def poll(self):
                return None

        result, popen, run_command, killpg = self.run_helper(
            LiveProcess(), [subprocess.CompletedProcess([], 0)], executable="")

        self.assertEqual(result, 0)
        daemon_env = popen.call_args.kwargs["env"]
        self.assertEqual(daemon_env["PATH"], "/usr/sbin:/usr/bin:/sbin:/bin")
        probe_args = run_command.call_args.args[0]
        self.assertEqual(probe_args[0], "/usr/bin/python3")
        self.assertEqual(run_command.call_args.kwargs["env"]["PATH"], daemon_env["PATH"])
        killpg.assert_not_called()


class PreviousMountTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.pidfile = Path(self.temp.name) / "old.pid"
        self.pidfile.write_text("321\n")
        self.command = ["/test/studiofs", "mount", "vol", "/mnt/vol", "--foreground"]

    def tearDown(self):
        self.temp.cleanup()

    def test_waits_for_matching_pid_then_accepts_exit_or_replacement(self):
        matching = ("\0".join(self.command) + "\0").encode()
        with patch.object(helper.Path, "read_bytes", side_effect=[matching, FileNotFoundError]), \
             patch.object(helper.time, "monotonic", side_effect=[0, 0]), \
             patch.object(helper.time, "sleep") as sleep:
            helper.wait_previous(self.pidfile, self.command)
        self.assertEqual(sleep.call_count, 1)

        replacement = ("/other\0mount\0vol\0/mnt/vol\0").encode()
        with patch.object(helper.Path, "read_bytes", return_value=replacement), \
             patch.object(helper.time, "monotonic", return_value=0), \
             patch.object(helper.time, "sleep") as sleep:
            helper.wait_previous(self.pidfile, self.command)
        sleep.assert_not_called()

    def test_matching_previous_pid_times_out(self):
        matching = ("\0".join(self.command) + "\0").encode()
        with patch.object(helper.Path, "read_bytes", return_value=matching) as read, \
             patch.object(helper.time, "monotonic", side_effect=[0, 0, 121]), \
             patch.object(helper.time, "sleep") as sleep:
            with self.assertRaisesRegex(ValueError, "previous task daemon did not finish"):
                helper.wait_previous(self.pidfile, self.command)
        self.assertEqual(read.call_count, 2)
        sleep.assert_called_once_with(0.1)


if __name__ == "__main__":
    unittest.main()
