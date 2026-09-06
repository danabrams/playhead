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


if __name__ == "__main__":
    unittest.main()
