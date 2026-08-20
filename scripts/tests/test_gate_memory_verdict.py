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
