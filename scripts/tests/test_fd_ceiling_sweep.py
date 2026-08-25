"""Tests for playhead-vk68m's ceiling/host-death sweep (scripts/fd_ceiling_sweep.py).

The sweep answers one question — do the runs that lose their host also reach the
descriptor ceiling? — and the way to get it wrong is the way playhead-s34ux got
it wrong first: divide the peak by `kern.maxfilesperproc` (61,440) instead of
`RLIMIT_NOFILE` soft (2,560) and 99.2 % reads as 4.1 %. The two differ by 24x
here and a percentage against the wrong denominator looks exactly like one
against the right one.

So the rails concentrate on the DENOMINATOR, on the invocation cut, and on
de-duplication — the three places a count can silently become a different count.
"""

import importlib.util
import os
import pathlib
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


sweep = _load("fd_ceiling_sweep", "fd_ceiling_sweep.py")


BANNER = "Command line invocation:\n"
PROBE = "[s34ux-fd] pid=1 RLIMIT_NOFILE soft=2560 hard=9223372036854775807 fds=2 maxfd=3\n"
VERDICT = "** TEST FAILED **\n"


def log(peak=2539, denominator="RLIMIT_NOFILE soft", ceiling=2560, probe=True,
        pids=("111",), verdict=True, signal_line="", invocations=1):
    body = BANNER * invocations
    if probe:
        body += PROBE
    for pid in pids:
        body += f"Playhead[{pid}:12345] some app output\n"
    body += (f"gate-memory: test host peak open fds {peak} of {denominator} "
             f"{ceiling} (99.2 % of it)\n")
    if signal_line:
        body += signal_line + "\n"
    if verdict:
        body += VERDICT
    return body


class DenominatorRails(unittest.TestCase):

    def test_the_binding_soft_limit_is_used_when_the_log_carries_it(self):
        row = sweep.classify_run(log(peak=2539))
        self.assertAlmostEqual(sweep.share(row), 100.0 * 2539 / 2560, places=3)

    def test_a_kernel_cap_denominator_is_REFUSED_rather_than_used(self):
        """4.1 % and 99.2 % are the same run. The wrong one is never printed."""
        row = sweep.classify_run(
            log(peak=2539, denominator="kern.maxfilesperproc",
                ceiling=61440, probe=False))
        self.assertEqual(sweep.share(row), -1.0)

    def test_the_probe_line_rescues_a_log_whose_gate_line_used_the_fallback(self):
        row = sweep.classify_run(
            log(peak=2539, denominator="kern.maxfilesperproc",
                ceiling=61440, probe=True))
        self.assertAlmostEqual(sweep.share(row), 100.0 * 2539 / 2560, places=3)

    def test_a_log_with_no_fd_line_at_all_yields_no_row(self):
        self.assertEqual(sweep.classify_run("nothing to see here\n"), {})


class FateRails(unittest.TestCase):

    def test_one_host_pid_and_a_verdict_is_COMPLETE(self):
        self.assertEqual(sweep.classify_run(log())["fate"], "COMPLETE")

    def test_two_host_pids_is_RESTARTED(self):
        row = sweep.classify_run(log(pids=("111", "222")))
        self.assertEqual(row["fate"], "RESTARTED")
        self.assertEqual(row["host_pids"], 2)

    def test_no_terminal_verdict_is_NO_VERDICT_even_with_one_pid(self):
        self.assertEqual(sweep.classify_run(log(verdict=False))["fate"], "NO-VERDICT")

    def test_a_signal_death_is_NO_VERDICT_even_with_a_verdict_line(self):
        """`Killed: 9` beats `** TEST FAILED **`: the run did not finish."""
        row = sweep.classify_run(log(signal_line="Killed: 9"))
        self.assertEqual(row["fate"], "NO-VERDICT")
        self.assertEqual(row["signal"], "Killed: 9")


class InvocationCutRails(unittest.TestCase):

    def test_pids_are_counted_in_the_LAST_invocation_only(self):
        """Two invocations each with their own host is not a mid-run restart.

        `fast-gate.sh` retries a wedged simulator into the same log and the
        residual pass is a second `xcodebuild test`; counting pids over the file
        reports one-pid-per-invocation as a restart.
        """
        first = log(pids=("111",))
        second = log(pids=("222",))
        row = sweep.classify_run(first + second)
        self.assertEqual(row["invocations"], 2)
        self.assertEqual(row["host_pids"], 1)
        self.assertEqual(row["fate"], "COMPLETE")


class CollectionRails(unittest.TestCase):

    def _write(self, directory, name, body):
        path = os.path.join(directory, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            fh.write(body)
        return path

    def test_the_same_run_preserved_twice_is_counted_ONCE(self):
        body = log() + "x" * (200 * 1024)
        with tempfile.TemporaryDirectory(prefix="vk68m-collect-") as tmp:
            self._write(tmp, "a/gate.log", body)
            self._write(tmp, "b/gate-copy.log", body)
            rows = sweep.collect([tmp])
        self.assertEqual(len(rows), 1)

    def test_a_log_under_the_size_floor_is_skipped(self):
        with tempfile.TemporaryDirectory(prefix="vk68m-collect-") as tmp:
            self._write(tmp, "small.log", log())
            self.assertEqual(sweep.collect([tmp]), [])

    def test_a_missing_root_is_skipped_rather_than_raising(self):
        self.assertEqual(sweep.collect(["/no/such/root/anywhere"]), [])

    def test_a_big_log_without_the_marker_is_not_read_as_a_run(self):
        with tempfile.TemporaryDirectory(prefix="vk68m-collect-") as tmp:
            self._write(tmp, "big.log", "y" * (200 * 1024))
            self.assertEqual(sweep.collect([tmp]), [])


if __name__ == "__main__":
    unittest.main()
