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

    def test_only_REMOVE_parses_even_when_another_verb_carries_the_same_payload(self):
        # The VERB is the contract, not the shape. Today's SKIP lines happen to
        # carry no size, so a parser that accepted any verb would look correct;
        # the moment someone improves the cleaner to log "SKIP (live build,
        # 2.7G): …" — an obvious improvement — every skipped path would start
        # being advertised as reclaimable space in the refusal.
        self.assertEqual(dp.parse_dry_run("[DRY] SKIP (live build, 2.7G): /x"), [])
        self.assertEqual(dp.parse_dry_run("[DRY] REFUSE (outside safe, 1G): /x"), [])

    def test_no_output_means_no_candidates(self):
        self.assertEqual(dp.parse_dry_run(""), [])

    def test_a_missing_cleaner_is_survivable(self):
        self.assertEqual(dp.survey("/nonexistent/cleaner.sh"), [])

    def test_a_cleaner_that_errors_yields_no_candidates_rather_than_garbage(self):
        cleaner = str(ROOT / "scripts" / "disk-cleanup.sh")
        self.assertEqual(dp.survey(cleaner, runner=lambda _c: (3, "boom")), [])

    def test_a_cleaner_that_errored_is_not_believed_even_where_it_looks_parseable(self):
        # A cleaner that dies partway has already printed REMOVE lines for the
        # work it got through. Its exit code is the statement that the survey is
        # incomplete, so a refusal must not quote a total built from it — that
        # is a number whose denominator nobody can name.
        cleaner = str(ROOT / "scripts" / "disk-cleanup.sh")
        out = ("[DRY] REMOVE (worktree-unregistered, 2.7G): "
               "/Users/dabrams/playhead/.worktrees/zz/.derivedData\n"
               "du: fts_read: Permission denied\n")
        self.assertEqual(dp.survey(cleaner, runner=lambda _c: (3, out)), [])


class ResolveSimIdTests(unittest.TestCase):
    LISTING = (
        "== Devices ==\n"
        "-- iOS 27.0 --\n"
        "    iPhone 17 Pro (AAAAAAAA-1111-2222-3333-444444444444) (Shutdown)\n"
        "    iPhone 17 (19B59D40-3826-4A31-AE37-70F59BA7C96E) (Shutdown)\n"
    )

    def runner(self, cmd):
        return (0, self.LISTING) if cmd[:2] == ["xcrun", "simctl"] else (0, "")

    def test_resolves_a_udid_from_a_name_destination(self):
        got = dp.resolve_sim_id("platform=iOS Simulator,name=iPhone 17", self.runner)
        self.assertEqual(got, "19B59D40-3826-4A31-AE37-70F59BA7C96E")

    def test_does_not_match_a_longer_device_name_with_the_same_prefix(self):
        # "iPhone 17" must not resolve to "iPhone 17 Pro" — erasing the wrong
        # simulator is a worse outcome than printing a placeholder.
        got = dp.resolve_sim_id("platform=iOS Simulator,name=iPhone 17 Pro", self.runner)
        self.assertEqual(got, "AAAAAAAA-1111-2222-3333-444444444444")

    def test_an_id_destination_needs_no_lookup(self):
        self.assertEqual(dp.resolve_sim_id("platform=iOS Simulator,id=abc", self.runner), "")

    def test_an_unknown_name_falls_back_to_the_placeholder(self):
        self.assertEqual(dp.resolve_sim_id("name=iPhone 99", self.runner), "")

    def test_simctl_failing_is_survivable(self):
        # The global xcode-select on this box is CommandLineTools, which has no
        # simctl. A refusal must still print.
        self.assertEqual(dp.resolve_sim_id("name=iPhone 17", lambda _c: (72, "no simctl")), "")


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


class FastGateWiringTests(unittest.TestCase):
    """scripts/fast-gate.sh, driven for real against a stubbed xcodebuild.

    The verdict above is pure and cheap to test; what actually protects the box
    is the SHELL composition — that a refusal stops the run BEFORE xcodebuild is
    ever invoked, and that the override reaches it. Same harness shape as
    test_gate_baseline.FastGateWiringTests.
    """

    def _skeleton(self, tmp):
        tmp = pathlib.Path(tmp)
        (tmp / "scripts").mkdir()
        for name in ("fast-gate.sh", "gate_baseline.py", "disk_preflight.py"):
            (tmp / "scripts" / name).write_bytes((ROOT / "scripts" / name).read_bytes())
        (tmp / "scripts" / "fast-gate.sh").chmod(0o755)
        # A stub cleaner: the real one hardcodes the repo root and would sweep
        # the developer's actual worktrees from inside a unit test.
        (tmp / "scripts" / "disk-cleanup.sh").write_text(
            "#!/bin/sh\necho '[DRY] done (dry_run=1)'\n")
        (tmp / "scripts" / "disk-cleanup.sh").chmod(0o755)
        # A scheme that already names the plan, so the xcodegen bootstrap —
        # which needs the gitignored model — never triggers.
        scheme = tmp / "Playhead.xcodeproj" / "xcshareddata" / "xcschemes"
        scheme.mkdir(parents=True)
        (scheme / "Playhead.xcscheme").write_text("PlayheadFastTests.xctestplan\n")

        bindir = tmp / "bin"
        bindir.mkdir()
        stub = bindir / "xcodebuild"
        # Records that it ran AND with what arguments, then reports a clean run
        # so nothing downstream mistakes the stub for the thing under test.
        stub.write_text(
            "#!/bin/sh\n"
            "printf '%%s\\n' \"$@\" > %s\n"
            "echo '** TEST SUCCEEDED **'\n"
            "echo 'Test run with 1 test passed after 0.1 seconds.'\n"
            "exit 0\n" % (tmp / "xcodebuild-argv.txt"))
        stub.chmod(0o755)
        return tmp, bindir

    def _run(self, tmp, bindir, args, extra_env=None):
        import os
        import subprocess

        env = dict(os.environ)
        env["PATH"] = str(bindir) + os.pathsep + env["PATH"]
        env["PLAYHEAD_SKIP_LINT"] = "1"
        env["PLAYHEAD_SKIP_BASELINE"] = "1"
        env.pop("PLAYHEAD_SKIP_DISK_PREFLIGHT", None)
        env.pop("PLAYHEAD_DISK_MIN_GIB", None)
        if extra_env:
            env.update(extra_env)
        return subprocess.run(
            ["bash", str(tmp / "scripts" / "fast-gate.sh")] + args,
            cwd=str(tmp), env=env, capture_output=True, text=True)

    def _ran_xcodebuild(self, tmp):
        return (tmp / "xcodebuild-argv.txt").exists()

    def test_a_short_disk_stops_the_run_BEFORE_xcodebuild(self):
        # The whole bead. Not "fails afterwards" — never starts.
        import tempfile as _t
        with _t.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(d)
            proc = self._run(tmp, bindir, [], {"PLAYHEAD_DISK_MIN_GIB": "99999"})
            self.assertEqual(28, proc.returncode, proc.stdout + proc.stderr)
            self.assertIn("REFUSING TO START", proc.stderr)
            self.assertFalse(self._ran_xcodebuild(tmp))

    def test_the_override_lets_it_through(self):
        import tempfile as _t
        with _t.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(d)
            proc = self._run(tmp, bindir, [], {"PLAYHEAD_DISK_MIN_GIB": "99999",
                                               "PLAYHEAD_SKIP_DISK_PREFLIGHT": "1"})
            self.assertNotIn("REFUSING", proc.stderr)
            self.assertTrue(self._ran_xcodebuild(tmp))
            self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_a_normal_run_passes_through_and_says_so_in_one_line(self):
        import tempfile as _t
        with _t.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(d)
            proc = self._run(tmp, bindir, [], {"PLAYHEAD_DISK_MIN_GIB": "0.001"})
            self.assertIn("disk preflight OK", proc.stdout)
            self.assertTrue(self._ran_xcodebuild(tmp))
            self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_reclaim_disk_is_consumed_and_never_forwarded_to_xcodebuild(self):
        # A stray --reclaim-disk on the xcodebuild command line is an immediate
        # hard error, i.e. this flag would break every run that used it.
        import tempfile as _t
        with _t.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(d)
            proc = self._run(tmp, bindir, ["--reclaim-disk"],
                             {"PLAYHEAD_DISK_MIN_GIB": "0.001"})
            self.assertTrue(self._ran_xcodebuild(tmp))
            argv = (tmp / "xcodebuild-argv.txt").read_text()
            self.assertNotIn("--reclaim-disk", argv)
            self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)


class CleanerWiringTests(unittest.TestCase):
    """scripts/disk-cleanup.sh, read as text.

    Every assertion here is a defect that has already cost this project hours.
    They are textual because the alternative is a test that deletes things.
    """

    def setUp(self):
        self.src = (ROOT / "scripts" / "disk-cleanup.sh").read_text(encoding="utf-8")
        # Comments are allowed to SAY "pgrep -f" — the header explains why it is
        # banned. Only executable lines are policed.
        self.code = "\n".join(ln for ln in self.src.splitlines()
                              if not ln.lstrip().startswith("#"))

    def test_pgrep_is_exact_match_never_full_argv(self):
        # `pgrep -f xcodebuild` self-matches ANY shell whose argv contains the
        # string — including this script and the agent running it. On
        # 2026-08-01 an -f pattern killed a healthy gate and then raised a
        # phantom second-build alarm in monitoring.
        self.assertIn("pgrep -x xcodebuild", self.code)
        self.assertNotIn("pgrep -f", self.code)

    def test_a_live_build_is_resolved_by_cwd_and_skipped(self):
        self.assertIn("lsof -a -p", self.src)
        self.assertIn("SKIP (live build)", self.src)

    def test_permissions_are_repaired_before_rm_and_with_read(self):
        # 0o300 dirs (write+exec, no READ) stop `rm -rf` dead. u+w does not fix
        # them — that is the bug that stranded 15 GiB.
        self.assertIn("chmod -R u+rwx", self.src)
        self.assertNotIn("chmod -R u+w ", self.src)

    def test_the_chmod_precedes_the_rm(self):
        self.assertLess(self.src.index("chmod -R u+rwx"),
                        self.src.index("rm -rf -- "))

    def test_removal_is_fenced_to_the_three_safe_prefixes(self):
        self.assertIn("REFUSE (outside safe prefixes)", self.src)
        self.assertIn("/private/tmp/playhead-", self.src)
        self.assertIn(".worktrees/", self.src)

    def test_the_coresim_trash_root_must_look_like_a_real_TMPDIR(self):
        # An unset or hostile TMPDIR must not turn `$TRASH_ROOT/Deleting-*`
        # into `/Deleting-*` or, worse, a bare glob at the filesystem root.
        self.assertIn("/var/folders/*", self.src)
        self.assertIn("/nonexistent", self.src)

    def test_the_newest_xcresult_is_kept(self):
        # It is the one you open after a failure. Pruning it would trade a disk
        # problem for a diagnosis problem.
        self.assertIn("xcresult-superseded", self.src)
        self.assertIn("ls -dt", self.src)

    def test_symlinks_are_still_refused(self):
        self.assertIn("SKIP (symlink)", self.src)

    def test_an_unmeasurable_size_is_reported_as_unreadable_not_as_zero(self):
        # `du` cannot descend a 0o300 directory, so exactly the trees this sweep
        # exists to clear measure 0B. Printing that is a number whose
        # denominator nobody can name.
        self.assertIn('size="unreadable"', self.code)

    def test_the_unreadable_marker_stays_parseable_by_the_preflight(self):
        # disk_preflight.parse_dry_run reads `(reason, size)` with `[^)]*` for
        # the size, so a marker containing parentheses would silently drop the
        # candidate out of the refusal's survey.
        got = dp.parse_dry_run("[DRY] REMOVE (coresim-stranded, unreadable): /x")
        self.assertEqual(got, [("coresim-stranded", "unreadable", "/x")])


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
