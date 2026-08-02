"""Tests for playhead-3nfa's disk-headroom preflight (scripts/disk_preflight.py).

The unit under test decides one thing: given how much room is free and how much
a gate needs, does the gate start? The interesting cases are the ones a wedge
made expensive — being one byte short, being short and having something to
reclaim, being short and having nothing to reclaim, and the reclaim path
recovering (or failing to).

Free space and the cleaner are both injected, so nothing here touches a real
volume or removes a real file.

Conventions follow test_gate_baseline.py: importlib module loading, in-code
fixture builders, stdlib unittest.
"""

import importlib.util
import io
import pathlib
import sys
import unittest
from contextlib import redirect_stdout

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


dp = _load("disk_preflight", "disk_preflight.py")
GIB = dp.GIB


def gib(n):
    return int(n * GIB)


class FakeCleaner:
    """Stands in for scripts/disk-cleanup.sh.

    `dry` is what it prints for --dry-run; `frees` is how many bytes a real run
    hands back. Records every invocation so a test can assert the cleaner was
    NOT run when no one opted in.
    """

    def __init__(self, dry="", frees=0):
        self.dry = dry
        self.frees = frees
        self.calls = []

    def __call__(self, cmd):
        self.calls.append(list(cmd))
        if "--dry-run" in cmd:
            return 0, self.dry
        return 0, "removed something\n"

    @property
    def real_runs(self):
        return [c for c in self.calls if "--dry-run" not in c]


def run(free_before, min_gib=12.0, argv=(), cleaner=None, freed=None):
    """Drive main() with injected free space. `freed` is the reading AFTER a
    real cleaner run, which is how the reclaim-then-recheck path is exercised."""
    readings = [free_before]

    def free_fn(_path):
        return readings[-1]

    def runner(cmd):
        rc, out = (cleaner or FakeCleaner())(cmd)
        if "--dry-run" not in cmd and freed is not None:
            readings.append(freed)
        return rc, out

    args = ["--min-gib", str(min_gib), "--cleaner", str(ROOT / "scripts" / "disk-cleanup.sh")]
    args += list(argv)
    buf = io.StringIO()
    err = io.StringIO()
    real_err, sys.stderr = sys.stderr, err
    try:
        with redirect_stdout(buf):
            rc = dp.main(args, free_fn=free_fn, runner=runner)
    finally:
        sys.stderr = real_err
    return rc, buf.getvalue(), err.getvalue()


# ---------------------------------------------------------------------------
class ThresholdTests(unittest.TestCase):
    def test_plenty_of_room_passes_quietly_enough(self):
        rc, out, err = run(gib(30), min_gib=12)
        self.assertEqual(rc, 0)
        self.assertIn("OK", out)
        self.assertEqual(err, "")

    def test_exactly_at_the_threshold_passes(self):
        # The threshold is a floor, not an exclusive bound. A run that has
        # precisely the measured requirement is a run that was measured to fit.
        rc, _out, _err = run(gib(12), min_gib=12)
        self.assertEqual(rc, 0)

    def test_one_byte_short_refuses(self):
        rc, _out, err = run(gib(12) - 1, min_gib=12)
        self.assertEqual(rc, 1)
        self.assertIn("REFUSING TO START", err)

    def test_the_refusal_goes_to_stderr_not_stdout(self):
        # fast-gate.sh tees stdout into the log the baseline check reads. A
        # refusal on stdout would be a stray "REMOVE"-shaped line in that log.
        _rc, out, err = run(gib(1), min_gib=12)
        self.assertNotIn("REFUSING", out)
        self.assertIn("REFUSING", err)


class RefusalContentTests(unittest.TestCase):
    """The refusal's whole job is to be actionable. These assert the specific
    facts that cost hours when they were missing."""

    def setUp(self):
        self.text = dp.format_refusal(gib(4.0), gib(12.0), [], sim_id="ABC-123")

    def test_names_both_numbers_and_the_shortfall(self):
        self.assertIn("4.00 GiB", self.text)
        self.assertIn("12.00 GiB", self.text)
        self.assertIn("8.00 GiB", self.text)  # short by

    def test_explains_that_the_failure_mode_is_a_wedge_not_an_error(self):
        self.assertIn("WEDGES", self.text)

    def test_names_the_TMPDIR_deleting_reservoir(self):
        self.assertIn("Deleting-", self.text)

    def test_the_chmod_is_u_plus_rwx_and_says_why_u_plus_w_is_not_enough(self):
        # The bug that stranded 15 GiB: 0o300 already grants WRITE. It is READ
        # that is missing. A remedy line saying `u+w` would silently not work.
        self.assertIn("u+rwx", self.text)
        self.assertNotIn("chmod -R u+w ", self.text)
        self.assertIn("READ", self.text)

    def test_names_the_cleaner_and_the_reclaim_flag(self):
        self.assertIn("scripts/disk-cleanup.sh", self.text)
        self.assertIn("--reclaim-disk", self.text)

    def test_warns_against_rm_on_a_booted_simulator(self):
        self.assertIn("NEVER `rm` a booted simulator", self.text)

    def test_substitutes_the_real_sim_udid_when_known(self):
        self.assertIn("simctl erase ABC-123", self.text)
        self.assertNotIn("<udid>", self.text)

    def test_falls_back_to_a_placeholder_when_the_udid_is_unknown(self):
        text = dp.format_refusal(gib(4), gib(12), [], sim_id="")
        self.assertIn("<udid>", text)

    def test_does_NOT_advertise_the_override(self):
        # Same reasoning as PLAYHEAD_SKIP_BASELINE. An escape hatch printed in
        # the failure message stops being an escape hatch and becomes the
        # documented workaround, and then the check protects nobody.
        self.assertNotIn("PLAYHEAD_SKIP_DISK_PREFLIGHT", self.text)

    def test_reports_reclaim_candidates_when_the_cleaner_found_some(self):
        text = dp.format_refusal(
            gib(4), gib(12),
            [("xcresult-superseded", "104M", "/a/b.xcresult")], sim_id="")
        self.assertIn("104M", text)
        self.assertIn("/a/b.xcresult", text)
        self.assertIn("1 candidate", text)

    def test_says_so_plainly_when_there_is_nothing_to_reclaim(self):
        # The dangerous refusal is the one that lists remedies that will not
        # help. If the cleaner has nothing, say the space is elsewhere.
        self.assertIn("NOTHING to reclaim", self.text)


class SurveyTests(unittest.TestCase):
    def test_parses_the_cleaners_dry_run_lines(self):
        got = dp.parse_dry_run(
            "[DRY] live build cwd(s): /x\n"
            "[DRY] REMOVE (worktree-unregistered, 2.7G): /Users/d/playhead/.worktrees/zz/.derivedData\n"
            "[DRY] SKIP (symlink): /nope\n"
            "[DRY] REMOVE (coresim-stranded, 3.1G): /var/folders/q/T/Deleting-ABC\n"
            "[DRY] done (dry_run=1)\n")
        self.assertEqual(
            got,
            [("worktree-unregistered", "2.7G", "/Users/d/playhead/.worktrees/zz/.derivedData"),
             ("coresim-stranded", "3.1G", "/var/folders/q/T/Deleting-ABC")])

    def test_a_skip_line_is_not_a_removal(self):
        self.assertEqual(dp.parse_dry_run("[DRY] SKIP (live build): /x"), [])

    def test_no_output_means_no_candidates(self):
        self.assertEqual(dp.parse_dry_run(""), [])

    def test_a_missing_cleaner_is_survivable(self):
        self.assertEqual(dp.survey("/nonexistent/cleaner.sh"), [])

    def test_a_cleaner_that_errors_yields_no_candidates_rather_than_garbage(self):
        cleaner = str(ROOT / "scripts" / "disk-cleanup.sh")
        self.assertEqual(dp.survey(cleaner, runner=lambda _c: (3, "boom")), [])


class ReclaimOptInTests(unittest.TestCase):
    def test_nothing_is_deleted_without_the_flag(self):
        # The load-bearing one. A preflight that quietly deletes on every run
        # hides a real capacity problem, and the repo's rails are strict about
        # rm. Without --reclaim the cleaner is only ever asked --dry-run.
        cleaner = FakeCleaner(dry="[DRY] REMOVE (x, 1G): /Users/dabrams/playhead/.worktrees/a/.derivedData\n")
        rc, _out, _err = run(gib(2), min_gib=12, cleaner=cleaner)
        self.assertEqual(rc, 1)
        self.assertEqual(cleaner.real_runs, [])

    def test_the_cleaner_is_not_run_at_all_when_there_is_room(self):
        cleaner = FakeCleaner()
        rc, _out, _err = run(gib(40), min_gib=12, cleaner=cleaner)
        self.assertEqual(rc, 0)
        self.assertEqual(cleaner.calls, [])

    def test_reclaim_runs_the_cleaner_once_and_proceeds_when_it_worked(self):
        cleaner = FakeCleaner()
        rc, out, err = run(gib(9), min_gib=12, argv=["--reclaim"],
                           cleaner=cleaner, freed=gib(20))
        self.assertEqual(rc, 0)
        self.assertEqual(len(cleaner.real_runs), 1)
        self.assertIn("reclaimed", out)
        self.assertEqual(err, "")

    def test_reclaim_refuses_when_it_did_not_free_enough(self):
        # Self-heal ONCE, then refuse. A tool that keeps papering over it hides
        # the capacity problem it is standing on.
        cleaner = FakeCleaner()
        rc, out, err = run(gib(9), min_gib=12, argv=["--reclaim"],
                           cleaner=cleaner, freed=gib(10))
        self.assertEqual(rc, 1)
        self.assertEqual(len(cleaner.real_runs), 1)
        self.assertIn("still short", out)
        self.assertIn("REFUSING TO START", err)


class DefaultThresholdTests(unittest.TestCase):
    def test_the_default_is_stated_in_gib_and_is_plausible(self):
        # Not a taste assertion — a tripwire. The number is derived from a
        # measured drawdown recorded in the module docstring; if someone drops
        # it to 2 or inflates it to 40 without re-measuring, the derivation
        # comment and the constant have parted company.
        self.assertGreaterEqual(dp.DEFAULT_MIN_GIB, 8.0)
        self.assertLessEqual(dp.DEFAULT_MIN_GIB, 20.0)

    def test_the_derivation_is_recorded_next_to_the_constant(self):
        src = (ROOT / "scripts" / "disk_preflight.py").read_text(encoding="utf-8")
        self.assertIn("peak drawdown", src)


if __name__ == "__main__":
    unittest.main()
