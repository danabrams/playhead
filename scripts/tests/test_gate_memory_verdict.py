"""Tests for playhead-3rql's memory verdict (scripts/gate_memory_verdict.py).

The unit under test answers one question about a finished gate run: did it
reach a verdict, and if not, what was the box's memory doing? Every case here
is one that a real run got wrong before the script existed.

Conventions follow test_disk_preflight.py: importlib module loading, in-code
fixture builders, stdlib unittest, nothing touching a real volume.
"""

import importlib.util
import io
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


gmv = _load("gate_memory_verdict", "gate_memory_verdict.py")


SUMMARY = "Test run with 11569 tests in 1417 suites passed after 332.534 seconds.\n"


def log(*, pids=("2232",), summary=True, xctest=None, killed=None, restart=False,
        started=0, finished=0):
    parts = []
    for pid in pids:
        parts.append(f"2026-08-20 04:47:06.000000-0400 Playhead[{pid}:7506716] [App] up\n")
    parts.extend("◇ Test \"t%d\" started.\n" % i for i in range(started))
    parts.extend("✔ Test \"t%d\" passed after 1.0 seconds.\n" % i for i in range(finished))
    if restart:
        parts.append("Restarting after unexpected exit, crash, or test timeout;\n")
    if summary:
        parts.append(SUMMARY)
    if xctest:
        parts.append(f"** TEST {xctest} **\n")
    if killed:
        parts.append(f"scripts/fast-gate.sh: line 265: 39324 {killed}   xcodebuild test\n")
    return "".join(parts)


def series(rows):
    """rows: list of dicts of column -> value; returns a csv path in a temp dir."""
    columns = list(rows[0].keys())
    handle = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False)
    handle.write(",".join(columns) + "\n")
    for row in rows:
        handle.write(",".join(str(row[c]) for c in columns) + "\n")
    handle.close()
    return handle.name


def sample(log_bytes=0, active=1000, wired=1000, compressor=0, swap=0,
           available=4000, **extra):
    row = {
        "log_bytes": log_bytes,
        "active_mib": active,
        "wired_mib": wired,
        "compressor_mib": compressor,
        "swap_used_mib": swap,
        "available_mib": available,
        "testhost_mib": 0,
    }
    row.update(extra)
    return row


def run(text, rc=0, series_path="", ram=16384):
    path = tempfile.NamedTemporaryFile("w", suffix=".log", delete=False)
    path.write(text)
    path.close()
    saved = gmv.ram_mib
    gmv.ram_mib = lambda: ram
    buf = io.StringIO()
    try:
        code = gmv.report(path.name, rc, series_path, out=buf)
    finally:
        gmv.ram_mib = saved
    return code, buf.getvalue()


class Classification(unittest.TestCase):

    def test_a_run_that_printed_the_swift_testing_summary_is_complete(self):
        code, out = run(log(), rc=0)
        self.assertEqual(code, 0)
        self.assertIn("reached a verdict", out)

    def test_the_xctest_format_alone_is_enough(self):
        # Reading only Swift Testing's summary is the 2026-07-31 triage bug.
        code, out = run(log(summary=False, xctest="FAILED"), rc=65)
        self.assertEqual(code, 0, out)
        self.assertIn("reached a verdict", out)

    def test_no_outcome_line_in_either_format_is_no_verdict(self):
        code, out = run(log(summary=False), rc=0)
        self.assertEqual(code, 1)
        self.assertIn("NO-VERDICT", out)
        self.assertIn("neither format", out)

    def test_a_second_host_pid_is_a_restart_even_without_the_marker(self):
        # EXP1 lost its host and xcodebuild never printed the marker.
        code, out = run(log(pids=("2232", "2792"), summary=False), rc=0)
        self.assertEqual(code, 1)
        self.assertIn("2232 -> 2792", out)

    def test_the_restart_marker_alone_is_enough(self):
        code, out = run(log(restart=True, summary=False), rc=0)
        self.assertIn("Restarting after unexpected exit", out)

    def test_killed_9_names_the_kernel_not_the_tests(self):
        code, out = run(log(killed="Killed: 9", summary=False), rc=137)
        self.assertEqual(code, 1)
        self.assertIn("kernel killed xcodebuild", out)
        self.assertIn("exited 137", out)

    def test_a_log_with_no_host_lines_is_not_a_restart(self):
        # Anti-vacuity: absence of pids must not read as "the host changed".
        code, out = run(log(pids=(), summary=True), rc=0)
        self.assertEqual(code, 0)
        self.assertNotIn("replaced mid-run", out)

    def test_it_says_how_many_were_in_flight_when_it_stopped(self):
        code, out = run(log(summary=False, started=100, finished=60), rc=137)
        self.assertIn("100 test case(s) started", out)
        self.assertIn("roughly 40 in flight", out)


class MultipleInvocations(unittest.TestCase):
    """A gate log can hold more than one `xcodebuild test`.

    fast-gate retries once on a wedged simulator and runs a residual pass, and
    a run whose script was edited mid-flight re-executed the whole thing. Each
    invocation has its own host pid, so counting pids over the file reports a
    mid-run host restart that never happened.
    """

    BANNER = "Command line invocation:\n    xcodebuild test\n"

    def test_two_invocations_are_not_a_host_restart(self):
        text = (
            self.BANNER + log(pids=("100",))
            + self.BANNER + log(pids=("200",))
        )
        code, out = run(text, rc=0)
        self.assertEqual(code, 0, out)
        self.assertNotIn("replaced mid-run", out)
        self.assertIn("last of 2 invocations", out)

    def test_a_restart_INSIDE_the_last_invocation_is_still_caught(self):
        text = (
            self.BANNER + log(pids=("100",))
            + self.BANNER + log(pids=("200", "201"), summary=False)
        )
        code, out = run(text, rc=0)
        self.assertEqual(code, 1)
        self.assertIn("200 -> 201", out)
        self.assertIn("2 xcodebuild invocations", out)

    def test_a_single_invocation_is_unchanged(self):
        code, out = run(self.BANNER + log(pids=("100", "101"), summary=False), rc=0)
        self.assertIn("100 -> 101", out)
        self.assertNotIn("invocations", out)


class MemoryReporting(unittest.TestCase):

    def test_without_a_series_it_refuses_to_claim_anything_about_memory(self):
        code, out = run(log(summary=False), rc=137, series_path="")
        self.assertIn("NO MEMORY SERIES", out)
        self.assertNotIn("DEMAND EXCEEDED", out)

    def test_demand_is_active_wired_compressor_swap_not_free(self):
        # A box with almost no free pages is not short of anything; a box whose
        # demand exceeds RAM is. The two readings disagree by design.
        path = series([
            sample(log_bytes=10_000, active=4000, wired=3000, compressor=6000,
                   swap=9000, available=200),
        ])
        _, out = run(log(summary=False), rc=137, series_path=path, ram=16384)
        self.assertIn("21.5 GiB peak", out)
        self.assertIn("DEMAND EXCEEDED RAM by 5.5 GiB", out)

    def test_the_aftermath_peak_is_reported_separately_and_labelled(self):
        # The most expensive memory event in a killed run happens AFTER it
        # stopped. Quoting that as what the tests needed is the whole defect.
        path = series([
            sample(log_bytes=10_000, active=1000, wired=1000, compressor=0, swap=0),
            sample(log_bytes=100_000, active=2000, wired=2000, compressor=0, swap=0),
            sample(log_bytes=100_000, active=9000, wired=9000, compressor=0, swap=0),
        ])
        _, out = run(log(summary=False), rc=137, series_path=path, ram=16384)
        self.assertIn("3.9 GiB peak WHILE THE RUN WAS STILL PRODUCING OUTPUT", out)
        self.assertIn("17.6 GiB AFTER the run went quiet", out)
        self.assertIn("Do not quote it", out)

    def test_a_few_hundred_epilogue_bytes_do_not_drag_the_aftermath_into_the_run(self):
        # xcodebuild writes its diagnostics line minutes later. A boundary of
        # "the last sample in which the log grew AT ALL" puts that inside the
        # run and reports the teardown peak as the run's peak.
        path = series([
            sample(log_bytes=10_000, active=1000, wired=1000),
            sample(log_bytes=100_000, active=2000, wired=2000),
            sample(log_bytes=100_300, active=9000, wired=9000),
        ])
        _, out = run(log(summary=False), rc=137, series_path=path, ram=16384)
        self.assertIn("3.9 GiB peak WHILE THE RUN WAS STILL PRODUCING OUTPUT", out)
        self.assertIn("AFTER the run went quiet", out)

    def test_an_absent_column_reads_as_not_recorded_and_never_as_zero(self):
        # A series written by an older sampler has no disk column. Printing
        # "disk free min 0.0 GiB" would invent a disk-full diagnosis.
        path = series([sample(log_bytes=10, active=1000, wired=1000)])
        _, out = run(log(summary=False), rc=137, series_path=path)
        self.assertIn("disk free min not recorded", out)
        self.assertIn("footprint not recorded", out)
        self.assertIn("pids not recorded", out)

    def test_a_recorded_disk_column_is_printed_as_a_number(self):
        path = series([sample(log_bytes=10, disk_free_mib=20480,
                              testhost_footprint_mib=285, testhost_pid=99)])
        _, out = run(log(summary=False), rc=137, series_path=path)
        self.assertIn("disk free min 20.0 GiB", out)
        self.assertIn("footprint 285 MiB", out)
        self.assertIn("1 distinct pid(s)", out)

    def test_demand_within_ram_does_not_claim_exhaustion(self):
        path = series([sample(log_bytes=10, active=1000, wired=1000,
                              compressor=100, swap=100)])
        _, out = run(log(summary=False), rc=65, series_path=path, ram=16384)
        self.assertNotIn("DEMAND EXCEEDED", out)

    def test_a_complete_run_still_reports_its_peak_when_a_series_exists(self):
        path = series([sample(log_bytes=10, active=1000, wired=1000,
                              compressor=100, swap=100)])
        code, out = run(log(), rc=0, series_path=path)
        self.assertEqual(code, 0)
        self.assertIn("Peak demand 2.1 GiB", out)

    def test_an_unreadable_log_cannot_evaluate_rather_than_passing(self):
        buf = io.StringIO()
        code = gmv.report("/nonexistent/path.log", 137, "", out=buf)
        self.assertEqual(code, 2)
        self.assertIn("CANNOT EVALUATE", buf.getvalue())


if __name__ == "__main__":
    unittest.main()


class FileDescriptorReporting(unittest.TestCase):
    """playhead-s34ux — a column nobody prints is a column nobody reads.

    That bead's whole cost was that the number was never taken: gates were
    failing 56 tests on denied file opens while every printed quantity said the
    box was healthy. The peak is now on the verdict of EVERY run, with its
    denominator, because a count without one is this repo's standing defect
    class.
    """

    def _fixed_ceiling(self, value):
        saved = gmv.max_files_per_proc
        gmv.max_files_per_proc = lambda: value
        self.addCleanup(lambda: setattr(gmv, "max_files_per_proc", saved))

    def test_the_peak_is_printed_WITH_its_denominator(self):
        self._fixed_ceiling(61440)
        path = series([sample(log_bytes=10, testhost_fds=100),
                       sample(log_bytes=20, testhost_fds=2539)])
        _, out = run(log(summary=False), rc=137, series_path=path)
        self.assertIn("peak open fds 2539", out)
        self.assertIn("kern.maxfilesperproc 61440", out)
        self.assertIn("4.1 %", out)

    def test_a_series_with_NO_fd_column_says_NOT_RECORDED_never_zero(self):
        """An older sampler wrote no column. `peak open fds 0` would claim the
        host held no descriptors, which is a refutation nobody measured."""
        path = series([sample(log_bytes=10)])
        _, out = run(log(summary=False), rc=137, series_path=path)
        self.assertIn("NOT RECORDED", out)
        self.assertNotIn("peak open fds 0", out)

    def test_a_column_of_only_FAILED_reads_is_NOT_RECORDED_too(self):
        """-1 is what `fd_sample` writes when the table could not be read at
        all. A run of nothing but -1 measured nothing, and must not report a
        peak of -1 as though it were a count."""
        path = series([sample(log_bytes=10, testhost_fds=-1),
                       sample(log_bytes=20, testhost_fds=-1)])
        _, out = run(log(summary=False), rc=137, series_path=path)
        self.assertIn("NOT RECORDED", out)
        self.assertNotIn("peak open fds -1", out)

    def test_a_FAILED_read_does_not_drag_the_peak_down(self):
        """A mid-run -1 is a failed read, not a moment with fewer descriptors.
        Averaging or min-ing it in would be the same defect one layer along."""
        self._fixed_ceiling(61440)
        path = series([sample(log_bytes=10, testhost_fds=2539),
                       sample(log_bytes=20, testhost_fds=-1)])
        _, out = run(log(summary=False), rc=137, series_path=path)
        self.assertIn("peak open fds 2539", out)

    def test_the_system_file_table_is_reported_beside_the_per_process_one(self):
        """ENFILE and EMFILE are different bugs with different remedies, and
        only the pair can tell them apart."""
        self._fixed_ceiling(61440)
        path = series([sample(log_bytes=10, testhost_fds=2539,
                              sys_num_files=7791, sys_max_files=122880)])
        _, out = run(log(summary=False), rc=137, series_path=path)
        self.assertIn("system-wide 7791", out)
        self.assertIn("kern.maxfiles 122880", out)

    def test_it_is_reported_on_a_COMPLETE_run_as_well(self):
        """The reading that most needs its instrument named is the quiet one:
        s34ux's gates completed, restarted no host, and sat comfortably inside
        the box's memory while failing 56 tests on denied opens."""
        self._fixed_ceiling(61440)
        path = series([sample(log_bytes=10, testhost_fds=2539)])
        _, out = run(log(summary=True), rc=0, series_path=path)
        self.assertIn("reached a verdict", out)
        self.assertIn("peak open fds 2539", out)

    def test_an_unreadable_ceiling_still_prints_the_peak_and_says_unknown(self):
        """sysctl can fail. The peak is still the measurement; what is missing
        is the denominator, and the line must say which of the two it lost."""
        self._fixed_ceiling(-1)
        path = series([sample(log_bytes=10, testhost_fds=2539)])
        _, out = run(log(summary=False), rc=137, series_path=path)
        self.assertIn("peak open fds 2539", out)
        self.assertIn("unknown", out)
        self.assertNotIn("% of it", out)
