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
        pids=("111",), verdict=True, signal_line="", invocations=1,
        started=0, denied=None):
    body = BANNER * invocations
    if probe:
        body += PROBE
    for pid in pids:
        body += f"Playhead[{pid}:12345] some app output\n"
    for i in range(started):
        body += f'◇ Test "a test number {i}" started.\n'
    body += (f"gate-memory: test host peak open fds {peak} of {denominator} "
             f"{ceiling} (99.2 % of it)\n")
    if denied is not None:
        body += (f"gate-baseline: RED (5 known / 3 NEW) — {denied} tests hit a "
                 f"RESOURCE FAILURE (re-run)\n")
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


class ContingencyArmRails(unittest.TestCase):
    """The 2x2 table is this sweep's whole deliverable and had no rail on the
    PRINT at all — swapping its two columns left every test green
    (playhead-vk68m review round 4)."""

    def _row(self, fate, peak):
        # Every key `report` reads. -1 is this module's `not recorded`.
        return {"fate": fate, "peak": peak, "probe_soft": 2560,
                "denominator": "RLIMIT_NOFILE soft", "ceiling": 2560,
                "host_pids": 1, "started": 0, "resource_casualties": -1,
                "invocations": 1, "reached_verdict": True, "signal": ""}

    def test_only_a_RESTART_counts_as_losing_the_host(self):
        """NO-VERDICT is the `Killed: 9` MEMORY signature on this box.

        Counting it as host loss would answer a question about DESCRIPTORS with
        the other resource's failures.
        """
        table = sweep.contingency(
            [self._row("RESTARTED", 2500), self._row("COMPLETE", 2500),
             self._row("NO-VERDICT", 2500)], 90.0)
        self.assertEqual(table.get((True, True)), 1)
        self.assertEqual(table.get((True, False)), 1)
        self.assertEqual(table.get(("no-verdict", True)), 1)

    def test_a_row_with_no_binding_limit_is_in_no_cell(self):
        row = self._row("COMPLETE", 2500)
        row["probe_soft"] = -1
        row["denominator"] = "kern.maxfilesperproc"
        row["ceiling"] = 61440
        self.assertEqual(sweep.contingency([row], 90.0), {})

    def test_the_threshold_names_the_band_a_run_is_IN(self):
        exactly = self._row("COMPLETE", int(2560 * 0.90))
        self.assertEqual(sweep.contingency([exactly], 90.0), {(True, False): 1})

    def test_the_printed_table_puts_each_count_under_the_RIGHT_heading(self):
        """The columns are `lost the host` then `completed`, in that order.

        Swapping them is invisible to every other rail here, and the table is
        the only thing this tool exists to produce.
        """
        import io
        from contextlib import redirect_stdout
        rows = [(f"/l{i}.log", r) for i, r in enumerate(
            [self._row("RESTARTED", 2500), self._row("COMPLETE", 2500),
             self._row("COMPLETE", 2500), self._row("COMPLETE", 100)])]
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            sweep.report(rows, 90.0, 0)
        lines = [line for line in buffer.getvalue().splitlines() if "ceiling" in line]
        at = next(line for line in lines if line.strip().startswith("AT the"))
        below = next(line for line in lines if line.strip().startswith("below the"))
        self.assertEqual(at.split()[-2:], ["1", "2"], at)
        self.assertEqual(below.split()[-2:], ["0", "1"], below)


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
            rows, _unparsed = sweep.collect([tmp])
        self.assertEqual(len(rows), 1)

    def test_a_log_under_the_size_floor_is_skipped(self):
        with tempfile.TemporaryDirectory(prefix="vk68m-collect-") as tmp:
            self._write(tmp, "small.log", log())
            self.assertEqual(sweep.collect([tmp]), ([], 0))

    def test_a_missing_root_is_skipped_rather_than_raising(self):
        self.assertEqual(sweep.collect(["/no/such/root/anywhere"]), ([], 0))

    def test_a_big_log_without_the_marker_is_not_read_as_a_run(self):
        with tempfile.TemporaryDirectory(prefix="vk68m-collect-") as tmp:
            self._write(tmp, "big.log", "y" * (200 * 1024))
            self.assertEqual(sweep.collect([tmp]), ([], 0))


class DenialCountRails(unittest.TestCase):
    """The gate's OWN casualty count, not a count of lines saying `RESOURCE`.

    The field this replaces was `len(re.findall("RESOURCE", tail, re.I))`. It
    was never printed and never written to the CSV, and on the run1 log it
    counts the gate's explanatory prose — dozens of lines — against a real
    casualty figure of 27. A number that names one thing and would be read as
    another is this repo's standing defect class, so it is now the figure the
    gate itself prints, and -1 when the gate never printed one.
    """

    def test_the_casualty_count_is_the_gates_own_number(self):
        self.assertEqual(sweep.classify_run(log(denied=27))["resource_casualties"], 27)

    def test_a_run_that_never_printed_the_line_is_MINUS_ONE_not_zero(self):
        """A restarted run never reaches the verdict block. 0 would claim it was clean."""
        self.assertEqual(sweep.classify_run(log())["resource_casualties"], -1)

    def test_the_word_RESOURCE_elsewhere_is_not_a_casualty_count(self):
        text = log() + ("  RESOURCE — this does NOT say whether the box was short\n"
                        "  RESOURCE         swift-testing::something (unable to open)\n")
        self.assertEqual(sweep.classify_run(text)["resource_casualties"], -1)

    def test_a_single_casualty_is_spelled_test_and_still_parses(self):
        text = log().replace("** TEST FAILED **",
                             "1 test hit a RESOURCE FAILURE (re-run)\n** TEST FAILED **")
        self.assertEqual(sweep.classify_run(text)["resource_casualties"], 1)

    def test_a_DIFFERENT_quantity_that_also_counts_TESTS_is_not_the_casualty_count(self):
        """Anti-vacuity, and the rail above could not see this.

        `test_the_word_RESOURCE_elsewhere_is_not_a_casualty_count` guards the
        word RESOURCE appearing in prose. It does NOT guard the pattern latching
        onto a different quantity that is also spelled `<n> tests`, which is the
        defect class this field was renamed for in the first place — and every
        real log carries several. Measured: relaxing `_RESOURCE_CASUALTIES` to
        `(\\d+) tests?` survives all 76 rails as they stood and reports **1**
        against run 1's real figure of **27**, because the first match in the
        tail is `Test run with 11785 tests in 1441 suites`.

        So the fixture carries the two sentences a full-plan tail always has —
        Swift Testing's own summary and the `Failing tests:` block — around the
        one that is the answer.
        """
        text = log(denied=27).replace(
            "gate-memory: test host peak open fds",
            "✘ Test run with 11785 tests in 1441 suites failed after 252.792 seconds.\n"
            "Failing tests:\n\t1 test crashed\n"
            "gate-memory: test host peak open fds")
        self.assertEqual(sweep.classify_run(text)["resource_casualties"], 27)


class StartedCountRails(unittest.TestCase):

    def test_started_counts_the_tests_the_last_invocation_announced(self):
        self.assertEqual(sweep.classify_run(log(started=7))["started"], 7)
        self.assertEqual(sweep.classify_run(log())["started"], 0)


class ContingencyRails(unittest.TestCase):
    """The 2x2 the whole sweep exists to build."""

    @staticmethod
    def _row(share_pct, fate):
        # `share()` reads peak / probe_soft, so a row is built by choosing a peak.
        return {"peak": int(round(share_pct * 2560 / 100.0)), "denominator":
                "RLIMIT_NOFILE soft", "ceiling": 2560, "probe_soft": 2560,
                "fate": fate}

    def test_the_four_cells_are_filled_by_share_and_by_fate(self):
        rows = [self._row(99.0, "COMPLETE"), self._row(99.0, "RESTARTED"),
                self._row(50.0, "COMPLETE"), self._row(50.0, "RESTARTED")]
        table = sweep.contingency(rows, 90.0)
        self.assertEqual(table[(True, False)], 1)
        self.assertEqual(table[(True, True)], 1)
        self.assertEqual(table[(False, False)], 1)
        self.assertEqual(table[(False, True)], 1)

    def test_a_run_EXACTLY_at_the_threshold_is_AT_the_ceiling(self):
        """`>=`, not `>`: the threshold names the band the run is in."""
        row = {"peak": 2304, "denominator": "RLIMIT_NOFILE soft", "ceiling": 2560,
               "probe_soft": 2560, "fate": "COMPLETE"}
        self.assertAlmostEqual(sweep.share(row), 90.0, places=6)
        self.assertEqual(sweep.contingency([row], 90.0), {(True, False): 1})
        self.assertEqual(sweep.contingency([row], 90.1), {(False, False): 1})

    def test_a_row_with_no_binding_limit_is_ABSENT_rather_than_counted(self):
        row = sweep.classify_run(
            log(denominator="kern.maxfilesperproc", ceiling=61440, probe=False))
        self.assertEqual(sweep.contingency([row], 90.0), {})

    def test_only_a_RESTART_is_HOST_LOSS_and_NO_VERDICT_is_neither_arm(self):
        """Changed deliberately at playhead-vk68m review round 4.

        This rail used to require NO-VERDICT to land in the host-loss cell
        alongside RESTARTED. On this box NO-VERDICT is the `Killed: 9` /
        exit-137 signature playhead-3rql attributed to MEMORY — a booted
        simulator costs 10-13 GiB of a 16 GiB box — so counting it here would
        answer a question about DESCRIPTORS with the other resource's deaths.
        It now gets its own key and `report` names it as neither arm.
        """
        self.assertEqual(sweep.contingency([self._row(99.0, "RESTARTED")], 90.0),
                         {(True, True): 1})
        self.assertEqual(sweep.contingency([self._row(99.0, "NO-VERDICT")], 90.0),
                         {("no-verdict", True): 1})
        self.assertEqual(sweep.contingency([self._row(50.0, "NO-VERDICT")], 90.0),
                         {("no-verdict", False): 1})


if __name__ == "__main__":
    unittest.main()
