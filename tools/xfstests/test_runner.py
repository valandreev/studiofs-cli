"""Focused mocked checks for run.sh classification and exclusion behavior."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent
RUNNER = ROOT / "run.sh"


class RunnerTests(unittest.TestCase):
    def run_case(self, mode, excluded=False, two_cases=False, selector="generic/001"):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            checkout = root / "xfstests"
            (checkout / "tests/generic").mkdir(parents=True)
            (checkout / "tests/generic/group.list").write_text("001 auto\n")
            (checkout / "tests/generic/001").touch()
            if two_cases:
                (checkout / "tests/generic/002").touch()
            check = checkout / "check"
            check.write_text("""#!/bin/bash
case "$MODE" in
 pass) mkdir -p "$RESULT_BASE"; echo '<testcase name="generic/001" />' > "$RESULT_BASE/result.xml" ;;
 notrun) mkdir -p "$RESULT_BASE"; echo '<testcase name="generic/001"><skipped/></testcase>' > "$RESULT_BASE/result.xml" ;;
 skip_error) mkdir -p "$RESULT_BASE"; echo '<testcase name="generic/001"><skipped/></testcase>' > "$RESULT_BASE/result.xml"; exit 1 ;;
 fail) mkdir -p "$RESULT_BASE"; echo '<testcase name="generic/001"><failure /></testcase>' > "$RESULT_BASE/result.xml"; exit 1 ;;
 both) mkdir -p "$RESULT_BASE"; echo '<testcase name="generic/001"><skipped/><failure /></testcase>' > "$RESULT_BASE/result.xml" ;;
 malformed) mkdir -p "$RESULT_BASE"; echo '<testcase name="generic/001"' > "$RESULT_BASE/result.xml" ;;
 preflight) mkdir -p "$REPORT_DIR"; echo '<testsuite tests="0"/>' > "$REPORT_DIR/result.xml"; exit 1 ;;
 missing) exit 1 ;;
 timeout) sleep 5 ;;
esac
""")
            check.chmod(0o755)
            binary = root / "studiofs"
            binary.write_text("#!/bin/sh\nexit 0\n")
            binary.chmod(0o755)
            run = root / "run"
            env = os.environ | {
                "MODE": mode, "TEST_DEV": "testvol", "TEST_DIR": str(root / "testmnt"),
                "SCRATCH_DEV": "scratchvol", "SCRATCH_MNT": str(root / "scratchmnt"),
                "STUDIOFS_BINARY": str(binary), "STUDIOFS_CLIENT_ROOT": str(root / "client"),
                "XFSTESTS_RUN": str(root / "evidence"), "TEST_TIMEOUT": "1",
                "FSTYP": "fuse", "FUSE_SUBTYP": ".studiofs",
            }
            if excluded:
                # The runner reads exclusions from its own directory; test this with a copy.
                (root / "tools/xfstests").mkdir(parents=True)
                copied = root / "tools/xfstests/run.sh"
                copied.write_bytes(RUNNER.read_bytes())
                copied.chmod(0o755)
                (root / "tools/xfstests/exclude").write_text("001 known unsupported\n")
                runner = copied
            else:
                runner = RUNNER
            selectors = ["generic/001", "generic/002"] if two_cases else ([selector] if selector else [])
            result = subprocess.run([runner, checkout, run, *selectors], env=env,
                                    text=True, capture_output=True)
            manifest = (run / "manifest.tsv").read_text() if (run / "manifest.tsv").exists() else ""
            log_exists = (run / "cases/001/check.log").exists()
            if mode == "preflight":
                self.assertTrue((run / "cases/001/results/result.xml").exists())
            return result, manifest, log_exists

    def test_pass_notrun_fail_and_missing_report(self):
        for mode, verdict, code in [("pass", "pass", 0), ("notrun", "notrun", 0),
                                    ("skip_error", "harness", 1),
                                    ("fail", "fail", 1), ("both", "fail", 1),
                                    ("malformed", "harness", 1),
                                    ("preflight", "harness", 1),
                                    ("missing", "harness", 1)]:
            with self.subTest(mode=mode):
                result, manifest, log_exists = self.run_case(mode)
                self.assertEqual(result.returncode, code, result.stderr)
                self.assertIn("generic/001\t", manifest)
                self.assertTrue(log_exists)
                self.assertIn(f"\t{verdict}\n", manifest)

    def test_timeout_is_not_pass(self):
        result, manifest, _ = self.run_case("timeout")
        self.assertEqual(result.returncode, 1)
        self.assertIn("\ttimeout\n", manifest)

    def test_empty_selection_does_not_expand_to_entire_group(self):
        result, manifest, log_exists = self.run_case("pass", selector="")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(manifest, "")
        self.assertFalse(log_exists)

    def test_entire_group_requires_explicit_selector(self):
        result, manifest, _ = self.run_case("pass", selector="generic/*")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("generic/001\t", manifest)

    def test_setup_failure_stops_before_next_case(self):
        result, manifest, _ = self.run_case("missing", two_cases=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn("\tharness\n", manifest)
        self.assertNotIn("generic/002", manifest)

    def test_exclusion_is_recorded_without_running_check(self):
        result, manifest, log_exists = self.run_case("fail", excluded=True)
        self.assertEqual(result.returncode, 0)
        self.assertIn("\texcluded\n", manifest)
        self.assertFalse(log_exists)

    def test_timeout_recovery_respects_mount_and_pid_identity(self):
        for mode, check_count in [("other", 1), ("absent", 2)]:
            with self.subTest(findmnt=mode), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                checkout = root / "xfstests"
                (checkout / "tests/generic").mkdir(parents=True)
                (checkout / "tests/generic/group.list").write_text("001 auto\n002 auto\n")
                for test_id in ("001", "002"):
                    (checkout / f"tests/generic/{test_id}").touch()
                check = checkout / "check"
                check.write_text("#!/bin/sh\nexit 0\n")
                check.chmod(0o755)
                binary = root / "studiofs"
                binary.write_text("#!/bin/sh\nexit 0\n")
                binary.chmod(0o755)

                mockbin = root / "mockbin"
                mockbin.mkdir()
                findmnt = mockbin / "findmnt"
                findmnt.write_text("""#!/bin/sh
last=
for arg do last=$arg; done
if [ "$MOCK_FINDMNT" = other ] && [ "$last" = "$TEST_DIR" ]; then
  printf 'unrelated %s ext4\\n' "$TEST_DIR"
fi
""")
                findmnt.chmod(0o755)
                timeout = mockbin / "timeout"
                timeout.write_text("""#!/bin/bash
for arg in "$@"; do
  if [[ $arg == ./check ]]; then echo check >> "$MOCK_DIR/check.calls"; exit 124; fi
done
exec /usr/bin/timeout "$@"
""")
                timeout.chmod(0o755)
                for command in ("umount", "fusermount3"):
                    command_path = mockbin / command
                    command_path.write_text("#!/bin/sh\necho called >> \"$MOCK_DIR/unmount.calls\"\nexit 1\n")
                    command_path.chmod(0o755)

                process = subprocess.Popen([
                    "/usr/bin/python3", "-c", "import time; time.sleep(60)",
                    str(binary), "testvol", str(root / "testmnt")])
                try:
                    evidence = root / "evidence"
                    pid_dir = evidence / "mounts"
                    pid_dir.mkdir(parents=True)
                    (pid_dir / "testvol.pid").write_text(f"{process.pid}\n")
                    run = root / "run"
                    env = os.environ | {
                        "PATH": f"{mockbin}:/usr/bin:/bin", "MOCK_DIR": str(root),
                        "MOCK_FINDMNT": mode, "TEST_DEV": "testvol",
                        "TEST_DIR": str(root / "testmnt"), "SCRATCH_DEV": "scratchvol",
                        "SCRATCH_MNT": str(root / "scratchmnt"),
                        "STUDIOFS_BINARY": str(binary), "STUDIOFS_CLIENT_ROOT": str(root / "client"),
                        "XFSTESTS_RUN": str(evidence), "TEST_TIMEOUT": "1",
                        "FSTYP": "fuse", "FUSE_SUBTYP": ".studiofs",
                    }
                    result = subprocess.run([RUNNER, checkout, run, "generic/001", "generic/002"],
                                            env=env, text=True, capture_output=True, timeout=10)
                    manifest = (run / "manifest.tsv").read_text()
                    calls = (root / "check.calls").read_text().splitlines()
                    self.assertEqual(result.returncode, 1, result.stderr)
                    self.assertIn("generic/001\t124\t", manifest)
                    self.assertEqual(len(calls), check_count)
                    self.assertFalse((root / "unmount.calls").exists())
                    self.assertIsNone(process.poll(), "runner signalled the unrelated recorded PID")
                finally:
                    process.terminate()
                    process.wait(timeout=5)

    def test_timeout_reaps_escaped_descendants_and_restores_cpus(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            checkout = root / "xfstests"
            (checkout / "tests/generic").mkdir(parents=True)
            (checkout / "tests/generic/group.list").write_text("650 auto\n")
            (checkout / "tests/generic/650").touch()
            # Like fsstress and the mount daemons, these children leave timeout's process group.
            # The recorded TEST_DEV daemon and its watchdog child are mount recovery's to stop;
            # a daemon whose argv does not match its pidfile is not trusted.
            check = checkout / "check"
            check.write_text("""#!/bin/bash
cd "$MOCK_DIR"
setsid sleep 60 > writer.out & echo $! > escaped.pid
setsid bash -c 'exec -a "$STUDIOFS_BINARY" bash mount testvol "$TEST_DIR"' &
setsid bash -c 'exec -a "$STUDIOFS_BINARY" bash mount scratchvol /elsewhere' &
until [[ -s watchdog.testvol.pid && -s watchdog.scratchvol.pid ]]; do sleep 0.05; done
echo 0 > "$XFSTESTS_CPU_SYSFS/cpu1/online"
sleep 5
""")
            check.chmod(0o755)
            (root / "mount").write_text("""echo $$ > "$XFSTESTS_RUN/mounts/$1.pid"
echo $$ > "$MOCK_DIR/daemon.$1.pid"
sleep 60 & echo $! > "$MOCK_DIR/watchdog.$1.pid"
exec 9<> "$MOCK_DIR/fifo"; read -r -t 60 -u 9
""")
            os.mkfifo(root / "fifo")
            (root / "evidence/mounts").mkdir(parents=True)
            binary = root / "studiofs"
            binary.write_text("#!/bin/sh\nexit 0\n")
            binary.chmod(0o755)
            cpus = root / "cpu"
            for cpu, state in (("cpu1", "1"), ("cpu2", "0")):
                (cpus / cpu).mkdir(parents=True)
                (cpus / cpu / "online").write_text(state + "\n")
            run = root / "run"
            env = os.environ | {
                "MOCK_DIR": str(root), "XFSTESTS_CPU_SYSFS": str(cpus),
                "TEST_DEV": "testvol", "TEST_DIR": str(root / "testmnt"),
                "SCRATCH_DEV": "scratchvol", "SCRATCH_MNT": str(root / "scratchmnt"),
                "STUDIOFS_BINARY": str(binary), "STUDIOFS_CLIENT_ROOT": str(root / "client"),
                "XFSTESTS_RUN": str(root / "evidence"), "TEST_TIMEOUT": "1",
                "FSTYP": "fuse", "FUSE_SUBTYP": ".studiofs",
            }
            env.pop("STUDIOFS_XFSTESTS_CASE", None)
            pid_files = ["escaped.pid", "daemon.testvol.pid", "watchdog.testvol.pid",
                         "daemon.scratchvol.pid", "watchdog.scratchvol.pid"]
            try:
                result = subprocess.run([RUNNER, checkout, run, "generic/650"], env=env,
                                        text=True, capture_output=True, timeout=30)
                escaped, daemon, watchdog, stray, stray_watchdog = (
                    int((root / name).read_text()) for name in pid_files)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("generic/650\t124\t", (run / "manifest.tsv").read_text())
                self.assertFalse(alive(escaped), "escaped testcase descendant survived")
                rows = {line.split("\t")[0]: line.split("\t")
                        for line in (run / "cases/650/reaped.tsv").read_text().splitlines()}
                # Open files identify what an escaped writer was filling; no runner fd leaks in.
                self.assertEqual(rows[str(escaped)][3].split(" "),
                                 [str(run / "cases/650/check.log"), str(root / "writer.out")])
                self.assertIn(str(stray), rows)
                self.assertIn(str(stray_watchdog), rows)
                self.assertNotIn(str(daemon), rows)
                self.assertNotIn(str(watchdog), rows)
                self.assertEqual((cpus / "cpu1/online").read_text().strip(), "1")
                self.assertEqual((cpus / "cpu2/online").read_text().strip(), "0")
                self.assertEqual((run / "cases/650/cpu-restored.txt").read_text(), "cpu1\n")
            finally:
                for name in pid_files:
                    path = root / name
                    if path.exists() and alive(pid := int(path.read_text())):
                        os.kill(pid, 9)


def alive(pid):
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except FileNotFoundError:
        return False
    return stat.rsplit(")", 1)[1].split()[0] != "Z"


if __name__ == "__main__":
    unittest.main()
