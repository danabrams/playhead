"""playhead-y27o rails for scripts/gate_terminal_line.py and the gate's preflight."""
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import gate_terminal_line as gtl  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))


class TerminalLineTests(unittest.TestCase):
    def test_a_green_run_with_tests_is_PASS_and_names_both_counts(self):
        line, rc = gtl.terminal_line(
            "✔ Test run with 12 tests in 3 suites passed after 1.0 seconds.\n"
            "\t Executed 3 tests, with 0 failures (0 unexpected) in 0.1 (0.2) seconds\n", 0)
        self.assertEqual(rc, 0)
        self.assertEqual(line, "fast-gate: PASS (15 tests: 12 swift-testing + 3 xctest)")

    def test_a_green_exit_that_ran_zero_tests_is_FAIL_exit_1(self):
        line, rc = gtl.terminal_line("** TEST SUCCEEDED **\n", 0)
        self.assertEqual(rc, 1)
        self.assertTrue(line.startswith("fast-gate: FAIL rc=0"), line)
        self.assertIn("zero tests executed", line)

    def test_a_red_exit_passes_through_and_still_names_the_count(self):
        line, rc = gtl.terminal_line("✘ Test run with 5 tests in 1 suite failed after 0.4 seconds with 1 issue.\n", 65)
        self.assertEqual(rc, 65)
        self.assertTrue(line.startswith("fast-gate: FAIL rc=65 (5 tests executed"), line)

    def test_the_LAST_summary_wins_over_the_first(self):
        # The residual re-run prints a second summary; the console's first pass is not the count.
        line, rc = gtl.terminal_line(
            "✔ Test run with 100 tests in 9 suites passed after 9.0 seconds.\n"
            "✔ Test run with 4 tests in 1 suite passed after 0.1 seconds.\n", 0)
        self.assertEqual((line, rc), ("fast-gate: PASS (4 tests: 4 swift-testing + 0 xctest)", 0))

    def test_a_missing_log_with_a_green_rc_is_FAIL(self):
        with tempfile.TemporaryDirectory() as d:
            rc = gtl.main(["--log", os.path.join(d, "never-written.log"), "--rc", "0"])
        self.assertEqual(rc, 1)


class GatePreflightTests(unittest.TestCase):
    def test_a_toolchain_without_xcodebuild_is_refused_with_a_terminal_FAIL_line(self):
        env = dict(os.environ, DEVELOPER_DIR="/nonexistent/Xcode.app/Contents/Developer",
                   PATH="/usr/bin:/bin", PLAYHEAD_SKIP_LINT="1")
        proc = subprocess.run(["bash", "scripts/fast-gate.sh"], cwd=ROOT, capture_output=True,
                              text=True, env=env, timeout=120)
        self.assertNotEqual(proc.returncode, 0)
        lines = [l for l in (proc.stdout + proc.stderr).strip().splitlines() if l.strip()]
        self.assertTrue(lines and lines[-1].startswith("fast-gate: FAIL"), lines[-3:])
        self.assertIn("DEVELOPER_DIR", proc.stdout + proc.stderr)

    def test_a_stubbed_xcodebuild_that_ignores_version_is_not_refused(self):
        # playhead-k99yv: the gate's own rails (test_gate_baseline) drive
        # fast-gate.sh against a stub that cats a log and exits 65 regardless of
        # its arguments. The preflight must judge the shim's OUTPUT, not the
        # exit code, or every one of those rails reads as a missing toolchain.
        import tempfile, stat
        with tempfile.TemporaryDirectory() as tmp:
            stub = os.path.join(tmp, "xcodebuild")
            with open(stub, "w") as f:
                f.write("#!/bin/sh\necho 'stub: not a toolchain'\nexit 65\n")
            os.chmod(stub, os.stat(stub).st_mode | stat.S_IXUSR)
            env = dict(os.environ, PATH=tmp + os.pathsep + "/usr/bin:/bin",
                       PLAYHEAD_SKIP_LINT="1", PLAYHEAD_SKIP_DISK_PREFLIGHT="1",
                       PLAYHEAD_SIM_TRIM="0")
            env.pop("DEVELOPER_DIR", None)
            proc = subprocess.run(["bash", "scripts/fast-gate.sh", "-only-testing:PlayheadTests/Nothing"],
                                  cwd=ROOT, capture_output=True, text=True, env=env, timeout=120)
            out = proc.stdout + proc.stderr
            self.assertNotIn("no usable xcodebuild", out, out[-800:])
            self.assertNotEqual(proc.returncode, 69, out[-800:])


class GateWedgeCeilingTests(unittest.TestCase):
    """playhead-1nh0q: the gate's outer wall-clock ceiling.

    2026-09-08: fast-gate.sh wedged TWICE on the same FM test — xcodebuild
    stayed alive, the test host spun at ~870% CPU, and only the log's mtime
    said anything was wrong. This drives the real fast-gate.sh against a stub
    xcodebuild that never returns, past a tiny test-only ceiling
    (PLAYHEAD_GATE_DEADLINE_S=1), and proves the ceiling actually FIRES rather
    than merely that the code parses: the stub's own hang is a busy
    `while :; do :; done` builtin loop (no forked child, so nothing can hold
    `tee`'s pipe open once the process is killed — and it never returns on its
    own, so the run cannot pass by accident) and the assertion on ELAPSED TIME
    is what tells a working kill from a no-op one — a broken kill would leave
    the stub spinning and the whole test would only end when Python's own
    subprocess timeout forcibly kills it, tens of seconds late rather than
    ~1s early.
    """

    def test_a_hanging_xcodebuild_is_killed_and_the_gate_exits_the_WEDGE_code(self):
        import stat
        import time

        with tempfile.TemporaryDirectory() as tmp:
            bindir = os.path.join(tmp, "bin")
            os.makedirs(bindir)
            stub = os.path.join(bindir, "xcodebuild")
            # -version must answer FAST (the toolchain preflight calls it
            # before the watchdog exists) — only the `test` subcommand hangs.
            # A busy `while :; do :; done` never returns on its own and forks
            # NO CHILD, so a SIGKILL of THIS process alone closes its stdout
            # immediately (a forked `sleep` would be orphaned and keep the
            # pipe open regardless of the kill) — `read -t N` was tried first
            # and rejected: with stdin already at EOF in this harness it
            # returns near-instantly rather than waiting out N, so the stub
            # never actually hung and the ceiling never got a chance to fire.
            with open(stub, "w", encoding="utf-8") as f:
                f.write(
                    "#!/bin/bash\n"
                    'case "$1" in -version) echo "Xcode 17.0"; exit 0 ;; esac\n'
                    'echo \'◇ Test "WedgeProbe.test1" started.\'\n'
                    "while :; do :; done\n"
                    'echo "fast-gate-test: should never print — the ceiling should have fired first"\n'
                    "exit 0\n"
                )
            os.chmod(stub, os.stat(stub).st_mode | stat.S_IXUSR)
            # PREPEND to the inherited PATH rather than replacing it: the
            # memory sampler that fast-gate.sh backgrounds calls bare
            # `sysctl` (in /usr/sbin, not /usr/bin), and a narrowed PATH
            # crashes it — its traceback then leaks into this captured
            # output and corrupts the last-line assertion below.
            env = dict(os.environ, PATH=bindir + os.pathsep + os.environ["PATH"],
                       PLAYHEAD_SKIP_LINT="1", PLAYHEAD_SKIP_DISK_PREFLIGHT="1",
                       PLAYHEAD_SIM_TRIM="0", PLAYHEAD_GATE_DEADLINE_S="1")
            env.pop("DEVELOPER_DIR", None)
            env.pop("PLAYHEAD_SKIP_BASELINE", None)
            start = time.monotonic()
            proc = subprocess.run(["bash", "scripts/fast-gate.sh"], cwd=ROOT,
                                  capture_output=True, text=True, env=env, timeout=45)
            elapsed = time.monotonic() - start
            out = proc.stdout + proc.stderr
            # The stub never returns on its own; the ceiling is 1s. Completing
            # in well under Python's 45s subprocess timeout proves the kill
            # fired rather than the run being forcibly ended some other way.
            self.assertLess(elapsed, 15, out[-2000:])
            self.assertEqual(proc.returncode, 124, out[-2000:])
            self.assertNotIn("should never print", out, out[-2000:])
            self.assertIn("fast-gate: WEDGE", out, out[-2000:])
            self.assertIn("WedgeProbe.test1", out, out[-2000:])
            lines = [l for l in out.strip().splitlines() if l.strip()]
            self.assertTrue(lines and lines[-1].startswith("fast-gate:"), lines[-5:])
            self.assertIn("rc=124", lines[-1], lines[-5:])

    def test_a_quick_stub_is_unaffected_by_the_ceiling(self):
        # The default (and this test's) ceiling is 5400s; a near-instant stub
        # must complete quickly and never read WEDGE — the watchdog it starts
        # is cancelled, not merely outlived.
        import stat
        import time

        with tempfile.TemporaryDirectory() as tmp:
            bindir = os.path.join(tmp, "bin")
            os.makedirs(bindir)
            stub = os.path.join(bindir, "xcodebuild")
            with open(stub, "w") as f:
                f.write(
                    "#!/bin/sh\n"
                    'case "$1" in -version) echo "Xcode 17.0"; exit 0 ;; esac\n'
                    "echo '** TEST SUCCEEDED **'\n"
                    "exit 0\n"
                )
            os.chmod(stub, os.stat(stub).st_mode | stat.S_IXUSR)
            env = dict(os.environ, PATH=bindir + os.pathsep + os.environ["PATH"],
                       PLAYHEAD_SKIP_LINT="1", PLAYHEAD_SKIP_DISK_PREFLIGHT="1",
                       PLAYHEAD_SIM_TRIM="0")
            env.pop("DEVELOPER_DIR", None)
            env.pop("PLAYHEAD_SKIP_BASELINE", None)
            start = time.monotonic()
            proc = subprocess.run(["bash", "scripts/fast-gate.sh", "-only-testing:PlayheadTests/Nothing"],
                                  cwd=ROOT, capture_output=True, text=True, env=env, timeout=45)
            elapsed = time.monotonic() - start
            out = proc.stdout + proc.stderr
            self.assertLess(elapsed, 30, out[-2000:])
            self.assertNotIn("fast-gate: WEDGE", out, out[-2000:])
            self.assertNotEqual(proc.returncode, 124, out[-2000:])


class DeveloperDirResolutionTests(unittest.TestCase):
    """playhead-k99yv: with no DEVELOPER_DIR and no xcodebuild on PATH, the gate
    resolves the newest Xcode*.app under the apps root and SAYS so; with none
    there it returns 1 and exports nothing; an explicit setting is never
    overridden."""

    def _run(self, apps_root, env_extra=None):
        env = {"PATH": "/usr/bin:/bin", "HOME": os.environ.get("HOME", "/tmp")}
        env.update(env_extra or {})
        # The value is read back from a CHILD process: a bare assignment (no
        # export) would satisfy a same-shell echo and never reach xcodebuild.
        script = ("set -u; . scripts/gate_toolchain.sh; resolve_developer_dir \"$1\"; rc=$?; "
                  "echo \"RC=$rc DD=$(bash -c 'echo ${DEVELOPER_DIR:-unset}')\"")
        return subprocess.run(["bash", "-c", script, "_", apps_root], cwd=ROOT,
                              capture_output=True, text=True, env=env, timeout=60)

    def test_the_newest_xcode_with_an_executable_xcodebuild_is_exported_and_announced(self):
        import tempfile, stat
        with tempfile.TemporaryDirectory() as root:
            for name, has_tool in (("Xcode-15.app", True), ("Xcode-16.app", True), ("Xcode-17.app", False)):
                d = os.path.join(root, name, "Contents", "Developer", "usr", "bin")
                os.makedirs(d)
                if has_tool:
                    tool = os.path.join(d, "xcodebuild")
                    with open(tool, "w") as f:
                        f.write("#!/bin/sh\necho Xcode\n")
                    os.chmod(tool, os.stat(tool).st_mode | stat.S_IXUSR)
            proc = self._run(root)
            self.assertIn("RC=0", proc.stdout, proc.stdout + proc.stderr)
            self.assertIn("DD=%s" % os.path.join(root, "Xcode-16.app", "Contents", "Developer"), proc.stdout)
            self.assertIn("DEVELOPER_DIR resolved to", proc.stdout)

    def test_an_empty_apps_root_returns_1_and_exports_nothing(self):
        import tempfile
        with tempfile.TemporaryDirectory() as root:
            proc = self._run(root)
            self.assertIn("RC=1 DD=unset", proc.stdout, proc.stdout + proc.stderr)

    def test_an_explicit_but_broken_DEVELOPER_DIR_is_never_overridden(self):
        import tempfile
        with tempfile.TemporaryDirectory() as root:
            proc = self._run(root, {"DEVELOPER_DIR": "/nonexistent/Xcode.app/Contents/Developer"})
            self.assertIn("RC=2 DD=/nonexistent/Xcode.app/Contents/Developer", proc.stdout, proc.stdout + proc.stderr)
            self.assertIn("not overriding", proc.stdout)


if __name__ == "__main__":
    unittest.main()
