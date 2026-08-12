"""Tests for playhead-voez's gate baseline (scripts/gate_baseline.py).

The unit under test is pure: recorded gate output in, a verdict out. Everything
here feeds it synthetic xcodebuild logs in the two real formats and asserts the
verdict, because the verdict is the only thing the gate's exit code is allowed
to be about.

Conventions follow test_l2f_lexical_anchor.py: importlib module loading, in-code
fixture builders, stdlib unittest.
"""

import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


gb = _load("gate_baseline", "gate_baseline.py")


# ---------------------------------------------------------------------------
# Fixture builders — the two formats, byte-faithful to real xcodebuild output.
# ---------------------------------------------------------------------------

TERMINAL_FAILED = (
    "Test run with 9562 tests in 1210 suites failed after 1211.923 seconds "
    "with 72 issues.\n"
    "** TEST FAILED **\n"
)
TERMINAL_PASSED = (
    "Test run with 9562 tests in 1210 suites passed after 1102.004 seconds.\n"
    "** TEST SUCCEEDED **\n"
)


def st_pass(name, seconds="0.017"):
    """A Swift Testing test that passed, display-name form."""
    return (
        '◇ Test "%s" started.\n'
        '✔ Test "%s" passed after %s seconds.\n' % (name, name, seconds)
    )


def st_fail_timeout(name, source="FooTests.swift", line=42, seconds="149.751"):
    """A Swift Testing test starved past its .timeLimit — the load-flake shape."""
    return (
        '◇ Test "%s" started.\n'
        '✘ Test "%s" recorded an issue at %s:%d:6: '
        "Time limit was exceeded: 60.000 seconds\n"
        '✘ Test "%s" failed after %s seconds with 1 issue.\n'
        % (name, name, source, line, name, seconds)
    )


def st_fail_expect(name, expr="a == b", source="FooTests.swift", line=42,
                   seconds="0.031"):
    """A Swift Testing test that failed an expectation — the regression shape."""
    return (
        '◇ Test "%s" started.\n'
        '✘ Test "%s" recorded an issue at %s:%d:6: Expectation failed: %s\n'
        '✘ Test "%s" failed after %s seconds with 1 issue.\n'
        % (name, name, source, line, expr, name, seconds)
    )


def st_fail_func(name, seconds="149.751", source="BarTests.swift"):
    """Undecorated @Test func — no display name, so the bare func() form."""
    return (
        "◇ Test %s started.\n"
        "✘ Test %s recorded an issue at %s:9:6: "
        "Time limit was exceeded: 60.000 seconds\n"
        "✘ Test %s failed after %s seconds with 1 issue.\n"
        % (name, name, source, name, seconds)
    )


def st_skip(name, reason="perf pass only — see playhead-zx0l"):
    """A DELIBERATE Swift Testing skip. Its glyph is ➜, not ✔/✘."""
    return (
        '◇ Test "%s" started.\n'
        '➜ Test "%s" skipped: "%s"\n' % (name, name, reason)
    )


def st_silent(name):
    """Started, then nothing — what a dead test host leaves behind.

    Byte-faithful to the real shape: playhead-tl6l measured 15 of these on
    bead/mn5e and 33 on main, and the module counted exactly zero of them.
    """
    return '◇ Test "%s" started.\n' % name


def xc_pass(suite, method, seconds="0.001"):
    return (
        "Test Case '-[PlayheadTests.%s %s]' started.\n"
        "Test Case '-[PlayheadTests.%s %s]' passed (%s seconds).\n"
        % (suite, method, suite, method, seconds)
    )


def xc_skip(suite, method, seconds="0.002"):
    return (
        "Test Case '-[PlayheadTests.%s %s]' started.\n"
        "Test Case '-[PlayheadTests.%s %s]' skipped (%s seconds).\n"
        % (suite, method, suite, method, seconds)
    )


def xc_fail(suite, method, seconds="0.025"):
    return (
        "Test Case '-[PlayheadTests.%s %s]' started.\n"
        "Test Case '-[PlayheadTests.%s %s]' failed (%s seconds).\n"
        % (suite, method, suite, method, seconds)
    )


HOST_RESTART = (
    "Restarting after unexpected exit, crash, or test timeout; summary will "
    "include totals from previous launches.\n"
)


def failing_block(*entries):
    """xcodebuild's own summary block, tab-indented exactly as it prints it.

    Copied from the real 2026-08-12 logs, duplicates and all — a name listed
    twice is one test xcodebuild retried.
    """
    return "\nFailing tests:\n" + "".join("\t%s\n" % e for e in entries) + "\n"


def log(*chunks, terminal=TERMINAL_FAILED):
    return (
        "lint: clean\n"
        "fast-gate: plan=PlayheadFastTests dest=iPhone 17 jobs=4\n"
        + "".join(chunks)
        + terminal
    )


def baseline(tests, runs=3, plan="PlayheadFastTests"):
    """Build a baseline dict. `tests` is {key: (failed_runs, kinds)}."""
    out = {
        "plan": plan,
        "mode": "full-plan",
        "runs_observed": runs,
        "tests": {},
    }
    for key, (failed, kinds) in tests.items():
        framework, name = key.split("::", 1)
        out["tests"][key] = {
            "framework": framework,
            "name": name,
            "seen_runs": runs,
            "failed_runs": failed,
            "kinds": list(kinds),
        }
    return out


# ---------------------------------------------------------------------------
# Parsing — both formats.
# ---------------------------------------------------------------------------

class ParseSwiftTestingTests(unittest.TestCase):
    def test_display_name_failure_is_captured_with_its_timeout_kind(self):
        run = gb.parse_run(log(st_fail_timeout("a mark survives its backfill")))
        key = "swift-testing::a mark survives its backfill"
        self.assertIn(key, run.failures)
        self.assertEqual({gb.KIND_TIMEOUT}, run.failures[key].kinds)
        self.assertAlmostEqual(149.751, run.failures[key].seconds, places=3)

    def test_expectation_failure_is_an_assertion_not_a_timeout(self):
        run = gb.parse_run(log(st_fail_expect("banner fires on entry")))
        key = "swift-testing::banner fires on entry"
        self.assertEqual({gb.KIND_ASSERTION}, run.failures[key].kinds)

    def test_bare_function_form_is_not_invisible(self):
        run = gb.parse_run(log(st_fail_func("totalFailureEmitsFailedWithItsReason()")))
        self.assertIn(
            "swift-testing::totalFailureEmitsFailedWithItsReason()", run.failures
        )

    def test_a_test_recording_both_kinds_reports_both(self):
        name = "mixed"
        text = log(
            '◇ Test "%s" started.\n'
            '✘ Test "%s" recorded an issue at F.swift:1:6: '
            "Time limit was exceeded: 60.000 seconds\n"
            '✘ Test "%s" recorded an issue at F.swift:2:6: '
            "Expectation failed: x == y\n"
            '✘ Test "%s" failed after 61.0 seconds with 2 issues.\n'
            % (name, name, name, name)
        )
        run = gb.parse_run(text)
        self.assertEqual(
            {gb.KIND_TIMEOUT, gb.KIND_ASSERTION},
            run.failures["swift-testing::mixed"].kinds,
        )

    def test_parameterised_argument_lines_still_resolve_the_source_file(self):
        name = "Invariant 1 (DRAIN): eligible queue drains"
        text = log(
            '◇ Test "%s" started.\n'
            '✘ Test "%s" recorded an issue with 2 arguments depth → 8, '
            "mix → preAnalysis at StallHarness.swift:192:6: "
            "Time limit was exceeded: 60.000 seconds\n"
            '✘ Test "%s" with 12 test cases failed after 149.9 seconds '
            "with 12 issues.\n" % (name, name, name)
        )
        run = gb.parse_run(text)
        key = "swift-testing::" + name
        self.assertIn(key, run.failures)
        self.assertEqual("StallHarness.swift", run.failures[key].source)
        self.assertEqual({gb.KIND_TIMEOUT}, run.failures[key].kinds)

    def test_a_failing_SUITE_is_not_mistaken_for_a_test(self):
        text = log(
            '✘ Suite "RepeatedAdCacheService (playhead-43ed)" failed after '
            "158.124 seconds with 14 issues.\n",
            st_pass("ok"),
        )
        run = gb.parse_run(text)
        self.assertEqual({}, dict(run.failures))

    def test_carriage_return_junk_before_the_glyph_does_not_hide_a_failure(self):
        # Real logs carry \r-overwritten prefixes: "X◇ Test", "​✘ Test".
        text = log(
            'r✘ Test "starved" recorded an issue at F.swift:1:6: '
            "Time limit was exceeded: 60.000 seconds\n"
            '​✘ Test "starved" failed after 149.7 seconds with 1 issue.\n'
        )
        run = gb.parse_run(text)
        self.assertIn("swift-testing::starved", run.failures)

    def test_a_failure_evidenced_ONLY_by_its_failed_after_line_survives_junk(self):
        # The test above does not actually pin the fail-line patterns: the ISSUE
        # line alone is enough to create the failure, so anchoring the fail
        # patterns to the line start left it green (R18 SURVIVED on first run).
        # Strip the issue line and the fail line is the only evidence there is.
        run = gb.parse_run(log('r✘ Test "starved" failed after 149.7 seconds.\n'))
        self.assertIn("swift-testing::starved", run.failures)
        self.assertAlmostEqual(
            149.7, run.failures["swift-testing::starved"].seconds, places=1
        )

    def test_carriage_return_junk_does_not_hide_a_PASS_either(self):
        # A missed pass is not harmless: every baseline member that passed would
        # read as "did not run" and turn the gate red for nothing.
        run = gb.parse_run(log('r✔ Test "quiet" passed after 0.4 seconds.\n'))
        self.assertIn("swift-testing::quiet", run.passed)

    def test_passes_are_collected(self):
        run = gb.parse_run(log(st_pass("quiet one"), st_fail_timeout("loud one")))
        self.assertIn("swift-testing::quiet one", run.passed)
        self.assertNotIn("swift-testing::loud one", run.passed)


class ParseXCTestTests(unittest.TestCase):
    def test_xctest_failure_is_captured_fully_qualified(self):
        run = gb.parse_run(log(xc_fail("SettingsViewTests", "testFoo")))
        key = "xctest::PlayheadTests.SettingsViewTests/testFoo"
        self.assertIn(key, run.failures)
        self.assertAlmostEqual(0.025, run.failures[key].seconds, places=3)

    def test_a_SLOW_xctest_failure_is_still_an_assertion_never_a_flake(self):
        # The duration heuristic INVERTS between frameworks. XCTest failures are
        # assertions; a 3.5s one is not a load flake and must not be classed as
        # a timeout, or the ynmk-shaped regression walks straight through.
        run = gb.parse_run(log(xc_fail("MetricKitTests", "testHatches", "3.559")))
        key = "xctest::PlayheadTests.MetricKitTests/testHatches"
        self.assertEqual({gb.KIND_ASSERTION}, run.failures[key].kinds)

    def test_xctest_pass_is_collected(self):
        run = gb.parse_run(log(xc_pass("A", "testB")))
        self.assertIn("xctest::PlayheadTests.A/testB", run.passed)

    def test_both_formats_coexist_in_one_run(self):
        run = gb.parse_run(
            log(st_fail_timeout("swifty"), xc_fail("XCSuite", "testLegacy"))
        )
        self.assertEqual(
            {"swift-testing::swifty", "xctest::PlayheadTests.XCSuite/testLegacy"},
            set(run.failures),
        )


class CompletenessTests(unittest.TestCase):
    def test_a_log_without_a_terminal_marker_is_incomplete(self):
        text = "lint: clean\n" + st_pass("a") + st_fail_timeout("b")
        self.assertFalse(gb.parse_run(text).complete)

    def test_a_terminal_marker_makes_it_complete(self):
        self.assertTrue(gb.parse_run(log(st_pass("a"))).complete)

    def test_swift_testing_summary_alone_is_enough(self):
        text = (
            st_pass("a")
            + "Test run with 10 tests in 2 suites passed after 3.0 seconds.\n"
        )
        self.assertTrue(gb.parse_run(text).complete)

    def test_the_real_truncated_reference_log_is_reported_incomplete(self):
        # The 2026-08-01 combined-main log everyone was quoting "72 failures"
        # from: 930 XCTest cases started, 900 passed, no terminal verdict. It is
        # a fragment. Treating it as a run is how a baseline gets built from a
        # tail. If the file is gone, the unit rules above still cover this.
        path = pathlib.Path(
            "/private/tmp/claude-501/-Users-dabrams-playhead/"
            "bee44b12-4e4b-4bbd-ada7-8d211ef3d4e6/scratchpad/combined-final.log"
        )
        if not path.exists():
            self.skipTest("reference log not present")
        run = gb.parse_run(path.read_text(encoding="utf-8", errors="replace"))
        self.assertFalse(run.complete)

    def test_only_the_last_attempt_is_read_after_a_sim_recovery(self):
        text = (
            st_fail_timeout("victim of the wedged sim")
            + "fast-gate: wedged simulator — recovering sim ABC and retrying once\n"
            + st_pass("victim of the wedged sim")
            + TERMINAL_PASSED
        )
        run = gb.parse_run(text)
        self.assertEqual({}, dict(run.failures))
        self.assertIn("swift-testing::victim of the wedged sim", run.passed)


class CrashedHostParseTests(unittest.TestCase):
    """playhead-tl6l — a dead test host emits no per-test line at all.

    These are the parse-level rails. The whole defect is that a population with
    no result line falls out of the arithmetic in BOTH directions, so every
    assertion here is about seeing something that emits nothing.
    """

    def test_a_started_test_that_reports_nothing_has_NO_VERDICT(self):
        run = gb.parse_run(log(st_pass("fine"), st_silent("the host died here")))
        self.assertEqual({"swift-testing::the host died here"}, run.no_verdict)
        # And it is emphatically NOT a failure — folding it into failures would
        # report it as NEW, which sends the reader to triage their own diff.
        self.assertEqual({}, dict(run.failures))

    def test_a_DELIBERATE_swift_testing_skip_is_an_outcome_not_silence(self):
        # Without this the census reads 45 where the truth is 15: PerfGate skips
        # look identical to crash casualties from the started-set alone.
        run = gb.parse_run(log(st_skip("enqueue of 1000 jobs completes quickly")))
        self.assertEqual(set(), run.no_verdict)
        self.assertIn(
            "swift-testing::enqueue of 1000 jobs completes quickly", run.skipped
        )

    def test_a_bare_func_swift_testing_skip_is_also_an_outcome(self):
        text = log(
            "◇ Test theDeadlineReturnsPromptly() started.\n"
            '➜ Test theDeadlineReturnsPromptly() skipped: "perf pass only"\n'
        )
        run = gb.parse_run(text)
        self.assertEqual(set(), run.no_verdict)

    def test_an_XCTest_skip_is_an_outcome_not_silence(self):
        run = gb.parse_run(log(xc_skip("PlayheadRuntimeLaunchPerfTests",
                                       "testInitFitsLaunchBudget")))
        self.assertEqual(set(), run.no_verdict)

    def test_a_skip_is_NOT_a_pass_so_the_ABSENT_arm_keeps_firing(self):
        # PerfGate-ing a family out of the plan must still show up as a gate
        # failure demanding a refresh — that is what stops coverage shrinking
        # quietly. A skip counted as "ran" would silence exactly that.
        run = gb.parse_run(log(st_skip("known")))
        self.assertNotIn("swift-testing::known", run.ran)
        base = baseline({"swift-testing::known": (3, ["timeout"])})
        v = gb.verdict(base, run)
        self.assertEqual(["swift-testing::known"], v.absent)
        self.assertEqual(1, v.exit_code)

    def test_the_host_restart_marker_is_read_and_QUOTED(self):
        run = gb.parse_run(log(HOST_RESTART, st_pass("after")))
        self.assertEqual(1, run.host_restarts)
        self.assertIn("Restarting after unexpected exit", run.restart_evidence)

    def test_the_failing_tests_block_is_parsed_entries_and_distinct_apart(self):
        run = gb.parse_run(
            log(st_pass("a"))
            + failing_block(
                "DownloadShowAttributionTests.attributionSurvivesProcessRestart()",
                "DownloadShowAttributionTests.attributionSurvivesProcessRestart()",
                "DelegateWorkJournalTests.stagingFailureReleasesOwnership()",
            )
        )
        self.assertEqual(3, len(run.blamed_entries))
        self.assertEqual(2, len(run.blamed))

    def test_the_block_ends_at_the_first_unindented_line(self):
        text = (
            log(st_pass("a"))
            + failing_block("SuiteTests.one()")
            + "** TEST FAILED **\nsomething else entirely\n"
        )
        self.assertEqual(["SuiteTests.one()"], gb.parse_run(text).blamed)

    def test_an_indented_line_that_is_not_in_a_block_is_not_swallowed(self):
        # The result-bundle path is printed tab-indented right above the block.
        text = log(
            "Test session results, code coverage, and logs:\n"
            "\t/Users/dabrams/playhead/.derivedData/Logs/Test/Test.xcresult\n",
            st_pass("a"),
        )
        self.assertEqual([], gb.parse_run(text).blamed)

    def test_a_block_entry_never_becomes_a_failure_or_a_pass(self):
        run = gb.parse_run(log(st_pass("a")) + failing_block("SuiteTests.one()"))
        self.assertEqual({}, dict(run.failures))
        self.assertEqual({"swift-testing::a"}, run.passed)


class AmbiguityTests(unittest.TestCase):
    def test_a_name_that_both_passed_and_failed_counts_as_FAILED(self):
        # Swift Testing's console line carries no suite, so two same-named tests
        # collide on one key. Resolve conservatively: never let a colliding pass
        # erase a real failure.
        text = log(st_pass("collide"), st_fail_expect("collide"))
        run = gb.parse_run(text)
        self.assertIn("swift-testing::collide", run.failures)
        self.assertNotIn("swift-testing::collide", run.passed)


# ---------------------------------------------------------------------------
# The verdict — the whole point of the bead.
# ---------------------------------------------------------------------------

class VerdictTests(unittest.TestCase):
    def test_exactly_the_baseline_set_is_GREEN_and_exit_zero(self):
        base = baseline({"swift-testing::known": (3, ["timeout"])})
        run = gb.parse_run(log(st_fail_timeout("known"), st_pass("other")))
        v = gb.verdict(base, run)
        self.assertTrue(v.ok)
        self.assertEqual(0, v.exit_code)
        self.assertIn("1 known", v.render())
        self.assertIn("0 new", v.render())

    def test_a_NEW_failure_fails_the_gate_and_is_NAMED(self):
        base = baseline({"swift-testing::known": (3, ["timeout"])})
        run = gb.parse_run(log(st_fail_timeout("known"), st_fail_expect("stranger")))
        v = gb.verdict(base, run)
        self.assertFalse(v.ok)
        self.assertEqual(["swift-testing::stranger"], v.new_failures)
        self.assertIn("stranger", v.render())

    def test_a_NEW_xctest_failure_fails_the_gate(self):
        # The ynmk regression's exact shape: a fast XCTest assertion failure in
        # a run whose Swift Testing failures all look like the usual flakes.
        base = baseline({"swift-testing::known": (3, ["timeout"])})
        run = gb.parse_run(
            log(st_fail_timeout("known"), xc_fail("SkipRefusalTests", "testAudit"))
        )
        v = gb.verdict(base, run)
        self.assertFalse(v.ok)
        self.assertEqual(
            ["xctest::PlayheadTests.SkipRefusalTests/testAudit"], v.new_failures
        )

    def test_a_DETERMINISTIC_baseline_member_that_PASSES_fails_the_gate(self):
        # Dan's decision: the baseline is exact, not a ceiling.
        base = baseline({"swift-testing::always red": (3, ["timeout"])}, runs=3)
        run = gb.parse_run(log(st_pass("always red")))
        v = gb.verdict(base, run)
        self.assertFalse(v.ok)
        self.assertEqual(["swift-testing::always red"], v.deterministic_passed)
        self.assertIn("always red", v.render())
        self.assertIn("--accept-baseline", v.render())

    def test_a_LOAD_SENSITIVE_member_that_passes_does_not_fail_the_gate(self):
        # 2 failures in 3 observations: this one flaps with machine load. On a
        # quieter run it passes while the deterministic member still fails —
        # the ordinary shape of a quiet box. The gate reports the passer as a
        # removal candidate but must not go red for it.
        base = baseline(
            {
                "swift-testing::flappy": (2, ["timeout"]),
                "swift-testing::steady": (3, ["timeout"]),
            },
            runs=3,
        )
        run = gb.parse_run(log(st_pass("flappy"), st_fail_timeout("steady")))
        v = gb.verdict(base, run)
        self.assertTrue(v.ok)
        self.assertEqual(["swift-testing::flappy"], v.load_sensitive_passed)
        self.assertIn("flappy", v.render())

    def test_a_load_sensitive_member_failing_a_DIFFERENT_WAY_is_NEW(self):
        # The hole the tolerance mechanism must not open: "known to time out"
        # never licenses "known to fail its expectations".
        base = baseline({"swift-testing::flappy": (2, ["timeout"])}, runs=3)
        run = gb.parse_run(log(st_fail_expect("flappy")))
        v = gb.verdict(base, run)
        self.assertFalse(v.ok)
        self.assertEqual(["swift-testing::flappy"], v.kind_changed)
        self.assertIn("assertion", v.render())

    def test_a_deterministic_member_failing_a_different_way_is_also_NEW(self):
        base = baseline({"swift-testing::always red": (3, ["timeout"])}, runs=3)
        run = gb.parse_run(log(st_fail_expect("always red")))
        v = gb.verdict(base, run)
        self.assertFalse(v.ok)
        self.assertEqual(["swift-testing::always red"], v.kind_changed)

    def test_a_run_that_executed_NOTHING_is_never_called_GREEN(self):
        # Live on 2026-08-01: `** TEST FAILED **` after zero tests (the sim erase
        # sent xcodebuild to the clone helper, which cannot find simctl). Zero
        # failures is not zero problems, and GREEN on that reads as a clean sweep.
        base = baseline({"swift-testing::a": (2, ["timeout"]),
                         "swift-testing::b": (2, ["timeout"])}, runs=2)
        run = gb.parse_run("error: unable to find utility \"simctl\"\n** TEST FAILED **\n")
        v = gb.verdict(base, run)
        self.assertFalse(v.ok)
        self.assertNotIn("GREEN", v.render())
        self.assertIn("did not exercise the plan", v.render())

    def test_a_long_absent_list_is_summarised_not_dumped(self):
        tests = {"swift-testing::t%02d" % i: (2, ["timeout"]) for i in range(40)}
        run = gb.parse_run("** TEST FAILED **\n" + st_fail_timeout("t00"))
        v = gb.verdict(baseline(tests, runs=2), run)
        rendered = v.render()
        self.assertIn("and 29 more", rendered)
        self.assertIn("39 of 40", rendered)

    def test_a_baseline_member_that_did_not_RUN_fails_the_gate(self):
        # A name nobody can reach is how djl0's rails "survived". A baseline
        # entry that never ran is not evidence of anything.
        base = baseline({"swift-testing::vanished": (3, ["timeout"])}, runs=3)
        run = gb.parse_run(log(st_pass("something else")))
        v = gb.verdict(base, run)
        self.assertFalse(v.ok)
        self.assertEqual(["swift-testing::vanished"], v.absent)
        self.assertIn("vanished", v.render())

    def test_a_fully_GREEN_run_against_a_nonempty_baseline_is_FICTION(self):
        # Single-run rot detection is unsound for load-sensitive members — one
        # quiet run proves nothing about a starvation flake. But ZERO failures
        # against a measured floor of dozens is categorical, not quiet: either
        # the suite stopped running or the file is fiction. This is the one
        # sound single-run rot rule, and it is the belt behind `absent`.
        base = baseline(
            {
                "swift-testing::f1": (1, ["timeout"]),
                "swift-testing::f2": (1, ["timeout"]),
            },
            runs=3,
        )
        run = gb.parse_run(log(st_pass("f1"), st_pass("f2")))
        v = gb.verdict(base, run)
        self.assertFalse(v.ok)
        self.assertTrue(v.baseline_fiction)
        self.assertIn("--accept-baseline", v.render())

    def test_an_INCOMPLETE_log_refuses_to_judge_rather_than_reporting_green(self):
        base = baseline({"swift-testing::known": (3, ["timeout"])})
        run = gb.parse_run("lint: clean\n" + st_pass("known"))  # no terminal marker
        v = gb.verdict(base, run)
        self.assertFalse(v.ok)
        self.assertEqual(gb.EXIT_CANNOT_EVALUATE, v.exit_code)
        self.assertIn("incomplete", v.render().lower())

    def test_an_empty_baseline_with_a_green_run_is_green(self):
        v = gb.verdict(baseline({}, runs=0), gb.parse_run(log(st_pass("a"))))
        self.assertTrue(v.ok)
        self.assertEqual(0, v.exit_code)

    def test_an_empty_baseline_with_any_failure_is_red(self):
        v = gb.verdict(baseline({}, runs=0), gb.parse_run(log(st_fail_expect("a"))))
        self.assertFalse(v.ok)
        self.assertEqual(["swift-testing::a"], v.new_failures)

    def test_a_single_observation_baseline_says_it_is_unconfirmed(self):
        base = baseline({"swift-testing::known": (1, ["timeout"])}, runs=1)
        run = gb.parse_run(log(st_fail_timeout("known")))
        v = gb.verdict(base, run)
        self.assertTrue(v.ok)
        self.assertIn("unconfirmed", v.render().lower())

    def test_several_new_failures_are_all_named_not_just_counted(self):
        base = baseline({})
        run = gb.parse_run(log(st_fail_expect("one"), st_fail_expect("two")))
        v = gb.verdict(base, run)
        rendered = v.render()
        self.assertIn("one", rendered)
        self.assertIn("two", rendered)
        self.assertIn("2 NEW", rendered)


class CrashedHostVerdictTests(unittest.TestCase):
    """playhead-tl6l — what the reader sees, and what the exit code says.

    The hazard being closed: `RED (N known / 0 new)` is the documented
    all-clear, and a run whose host died in seven suites could print exactly
    that. It cannot any more.
    """

    def _crashed(self, extra=()):
        return gb.parse_run(
            log(st_fail_timeout("known"), st_silent("lost"), *extra) + HOST_RESTART
        )

    def test_the_ALL_CLEAR_STRING_can_never_stand_alone_again(self):
        base = baseline({"swift-testing::known": (3, ["timeout"])})
        rendered = gb.verdict(base, self._crashed()).render()
        first = rendered.splitlines()[0]
        self.assertIn("RED (1 known / 0 new)", first)
        self.assertIn("NO VERDICT", first)
        self.assertIn("crashed host", first)
        self.assertNotEqual("gate-baseline: RED (1 known / 0 new)", first)

    def test_a_test_with_no_verdict_is_NOT_folded_into_NEW(self):
        # The remedies differ: a NEW failure is triaged against the diff, a lost
        # one means the run made no claim and must be re-run. Folding them would
        # send the reader to look for a regression in their own change.
        base = baseline({"swift-testing::known": (3, ["timeout"])})
        v = gb.verdict(base, self._crashed())
        self.assertEqual([], v.new_failures)
        self.assertEqual(["swift-testing::lost"], v.no_verdict)

    def test_it_is_REPORTED_but_does_NOT_by_itself_fail_the_gate(self):
        # The deliberate call (see the module docstring): it fires on main
        # today, on a pre-existing crash owned by another bead, and a gate that
        # is red for a reason its reader cannot fix is one they route around.
        base = baseline({"swift-testing::known": (3, ["timeout"])})
        v = gb.verdict(base, self._crashed())
        self.assertEqual(gb.EXIT_OK, v.exit_code)
        self.assertIn("NO VERDICT", v.render())

    def test_a_crashed_run_can_never_be_GREEN(self):
        run = gb.parse_run(log(st_pass("fine"), st_silent("lost")) + HOST_RESTART)
        v = gb.verdict(gb.empty_baseline("PlayheadFastTests"), run)
        self.assertEqual(gb.EXIT_OK, v.exit_code)
        self.assertNotIn("GREEN", v.render())
        self.assertIn("RED", v.render())

    def test_a_clean_run_says_nothing_about_crashes_at_all(self):
        # The other half of the hair-trigger argument: silence when there is
        # nothing to say, or the category becomes wallpaper.
        run = gb.parse_run(log(st_pass("fine"), st_skip("perf")),)
        v = gb.verdict(gb.empty_baseline("PlayheadFastTests"), run)
        self.assertNotIn("NO VERDICT", v.render())
        self.assertIn("GREEN", v.render())

    def test_a_restart_with_nothing_lost_does_not_claim_ZERO_TESTS_LOST(self):
        # A headline reading "0 tests got NO VERDICT" would be a number that
        # means one thing and reads as another.
        run = gb.parse_run(log(st_pass("fine")) + HOST_RESTART)
        v = gb.verdict(gb.empty_baseline("PlayheadFastTests"), run)
        first = v.render().splitlines()[0]
        self.assertNotIn("0 test", first)
        self.assertIn("CRASHED", first)

    def test_an_ABSENT_baseline_member_is_still_FATAL_and_now_names_the_CAUSE(self):
        # Policy unchanged — a name nobody reached is not evidence. What changes
        # is that it no longer reads "renamed, deleted or newly skipped", which
        # sent the reader hunting a rename that never happened.
        base = baseline({"swift-testing::known": (3, ["timeout"])})
        run = gb.parse_run(log(st_silent("known")) + HOST_RESTART)
        v = gb.verdict(base, run)
        self.assertEqual(["swift-testing::known"], v.absent)
        self.assertEqual(gb.EXIT_REGRESSION, v.exit_code)
        self.assertIn("the host died mid-test", v.render())
        self.assertNotIn("renamed, deleted or newly skipped", v.render())

    def test_a_genuinely_renamed_member_keeps_the_RENAME_cause(self):
        base = baseline({"swift-testing::gone": (3, ["timeout"])})
        v = gb.verdict(base, gb.parse_run(log(st_pass("other"))))
        self.assertIn("renamed, deleted or newly skipped", v.render())
        self.assertNotIn("the host died mid-test", v.render())


class BlamedBlockTests(unittest.TestCase):
    """playhead-tl6l — the `Failing tests:` block is a LEAD, not a census.

    The summary spells a test `Suite.function()`; Swift Testing's console prints
    its @Test display name. The two share nothing, so an entry can be matched
    only in the two spellings that ARE comparable. Guessing at the rest is what
    produced the bead's own wrong measurement (19 "invisible" failures that had
    all reported under their display names).
    """

    def test_an_XCTest_entry_that_reported_is_reconciled(self):
        run = gb.parse_run(
            log(xc_fail("DownloadTests", "testFoo"))
            + failing_block("DownloadTests.testFoo")
        )
        v = gb.verdict(gb.empty_baseline("PlayheadFastTests"), run)
        self.assertEqual([], v.blamed_unmatched)

    def test_a_bare_func_swift_testing_entry_that_reported_is_reconciled(self):
        run = gb.parse_run(
            log(st_fail_func("resumeConsumesBlob()"))
            + failing_block("ResumeSuspendedTransferTests.resumeConsumesBlob()")
        )
        v = gb.verdict(gb.empty_baseline("PlayheadFastTests"), run)
        self.assertEqual([], v.blamed_unmatched)

    def test_a_name_that_emitted_NOTHING_ANYWHERE_is_reported_unmatched(self):
        # The one genuine casualty in the 2026-08-12 main control run: it never
        # even printed a `started` line, so the started-set cannot see it and
        # only the block can.
        entry = "SkipOrchestratorRevertTests.failedSuggestNoRestoresLatestBufferedRevision()"
        run = gb.parse_run(log(st_pass("other")) + failing_block(entry))
        v = gb.verdict(gb.empty_baseline("PlayheadFastTests"), run)
        self.assertEqual([entry], v.blamed_unmatched)
        self.assertIn(entry, v.render())

    def test_a_started_but_silent_bare_func_entry_is_matched_not_double_counted(self):
        # It is already named by the NO VERDICT census under its own key; the
        # block must not report it a second time as a separate finding.
        run = gb.parse_run(
            log("◇ Test cancelReapsAttribution() started.\n")
            + failing_block("DownloadShowAttributionTests.cancelReapsAttribution()")
            + HOST_RESTART
        )
        v = gb.verdict(gb.empty_baseline("PlayheadFastTests"), run)
        self.assertEqual(["swift-testing::cancelReapsAttribution()"], v.no_verdict)
        self.assertEqual([], v.blamed_unmatched)

    def test_the_output_says_ENTRIES_and_DISTINCT_and_never_conflates_them(self):
        run = gb.parse_run(
            log(st_pass("a"))
            + failing_block("S.one()", "S.one()", "S.two()")
            + HOST_RESTART
        )
        v = gb.verdict(gb.empty_baseline("PlayheadFastTests"), run)
        self.assertEqual(3, v.blamed_entry_count)
        self.assertEqual(2, len(v.blamed_distinct))
        self.assertIn("3 entries, 2 distinct name(s)", v.render())

    def test_the_unmatched_list_is_labelled_a_LEAD_not_a_verdict(self):
        # It is 15-of-15 unmatched on a real run purely because those tests have
        # display names. Printing that as a finding without the caveat would be
        # a number that reads as fifteen lost tests.
        run = gb.parse_run(
            log(st_pass("a")) + failing_block("S.displayNamedTest()") + HOST_RESTART
        )
        rendered = gb.verdict(gb.empty_baseline("PlayheadFastTests"), run).render()
        self.assertIn("LEAD, not a count", rendered)
        self.assertIn("display name", rendered)


class ClassificationTests(unittest.TestCase):
    def test_failing_every_observation_is_deterministic(self):
        base = baseline({"swift-testing::x": (3, ["timeout"])}, runs=3)
        self.assertEqual(gb.TIER_DETERMINISTIC, gb.tier_of(base["tests"]["swift-testing::x"]))

    def test_failing_some_observations_is_load_sensitive(self):
        base = baseline({"swift-testing::x": (2, ["timeout"])}, runs=3)
        self.assertEqual(gb.TIER_LOAD_SENSITIVE, gb.tier_of(base["tests"]["swift-testing::x"]))

    def test_a_single_observation_can_never_be_deterministic(self):
        # One run cannot distinguish "always fails" from "starved once". Being
        # wrong in the load-sensitive direction costs a missed rot report; being
        # wrong the other way makes the gate cry wolf and get bypassed.
        base = baseline({"swift-testing::x": (1, ["timeout"])}, runs=1)
        self.assertEqual(gb.TIER_LOAD_SENSITIVE, gb.tier_of(base["tests"]["swift-testing::x"]))


# ---------------------------------------------------------------------------
# Refresh — must be trivial, or people route around the pass-direction arm.
# ---------------------------------------------------------------------------

class MergeTests(unittest.TestCase):
    def test_merge_records_a_new_failure_and_bumps_the_run_count(self):
        base = baseline({}, runs=0)
        run = gb.parse_run(log(st_fail_timeout("fresh")))
        merged = gb.merge(base, run, plan="PlayheadFastTests")
        self.assertEqual(1, merged["runs_observed"])
        entry = merged["tests"]["swift-testing::fresh"]
        self.assertEqual(1, entry["failed_runs"])
        self.assertEqual(1, entry["seen_runs"])
        self.assertEqual(["timeout"], entry["kinds"])

    def test_merge_promotes_a_consistently_failing_test_to_deterministic(self):
        base = baseline({"swift-testing::x": (2, ["timeout"])}, runs=2)
        run = gb.parse_run(log(st_fail_timeout("x")))
        merged = gb.merge(base, run, plan="PlayheadFastTests")
        self.assertEqual(gb.TIER_DETERMINISTIC, gb.tier_of(merged["tests"]["swift-testing::x"]))

    def test_two_observations_are_NOT_enough_to_promote(self):
        # Measured Jaccard between two full runs on identical code is 0.46, so
        # "failed twice" is weak evidence. Promoting there would turn ordinary
        # churn into gate failures.
        base = baseline({"swift-testing::x": (1, ["timeout"])}, runs=1)
        merged = gb.merge(base, gb.parse_run(log(st_fail_timeout("x"))),
                          plan="PlayheadFastTests")
        entry = merged["tests"]["swift-testing::x"]
        self.assertEqual(2, entry["seen_runs"])
        self.assertEqual(gb.TIER_LOAD_SENSITIVE, gb.tier_of(entry))

    def test_merge_demotes_a_member_that_passed_this_run(self):
        base = baseline({"swift-testing::x": (2, ["timeout"])}, runs=2)
        run = gb.parse_run(log(st_pass("x")))
        merged = gb.merge(base, run, plan="PlayheadFastTests")
        entry = merged["tests"]["swift-testing::x"]
        self.assertEqual(3, entry["seen_runs"])
        self.assertEqual(2, entry["failed_runs"])
        self.assertEqual(gb.TIER_LOAD_SENSITIVE, gb.tier_of(entry))

    def test_merge_DROPS_a_test_that_has_never_failed_in_any_observation(self):
        # Self-pruning: the file is the record of what is known-broken, so a
        # genuinely fixed test leaves it. A shrinking diff is good news.
        base = baseline({"swift-testing::x": (1, ["timeout"])}, runs=1)
        merged = gb.merge(base, gb.parse_run(log(st_pass("x"))), plan="PlayheadFastTests")
        self.assertEqual(1, merged["tests"]["swift-testing::x"]["failed_runs"])
        merged2 = gb.merge(merged, gb.parse_run(log(st_pass("x"))), plan="PlayheadFastTests")
        # still 1 failure in 3 -> stays, but a never-failed entry is dropped:
        base2 = baseline({}, runs=2)
        base2["tests"]["swift-testing::y"] = {
            "framework": "swift-testing", "name": "y",
            "seen_runs": 2, "failed_runs": 0, "kinds": [],
        }
        merged3 = gb.merge(base2, gb.parse_run(log(st_pass("y"))), plan="PlayheadFastTests")
        self.assertNotIn("swift-testing::y", merged3["tests"])
        self.assertIn("swift-testing::x", merged2["tests"])

    def test_merge_DROPS_a_baseline_member_that_no_longer_exists(self):
        # Realistic shape: most of the baseline is reached and ONE entry is not.
        # A fixture where nothing is reached is indistinguishable from a run that
        # never happened, and is refused outright — see the test below.
        base = baseline(
            {
                "swift-testing::renamed away": (2, ["timeout"]),
                "swift-testing::still here": (2, ["timeout"]),
                "swift-testing::also here": (2, ["timeout"]),
            },
            runs=2,
        )
        run = gb.parse_run(
            log(st_fail_timeout("still here"), st_fail_timeout("also here"))
        )
        merged = gb.merge(base, run, plan="PlayheadFastTests")
        self.assertNotIn("swift-testing::renamed away", merged["tests"])
        self.assertIn("swift-testing::still here", merged["tests"])

    def test_merge_REFUSES_a_run_that_executed_no_tests_at_all(self):
        # Measured live: erasing the simulator made xcodebuild reach for the
        # clone helper, which resolves `simctl` via the GLOBAL xcode-select
        # (CommandLineTools, no simctl). It printed `** TEST FAILED **` after
        # ZERO tests. Accepting that deletes every entry as unreachable and calls
        # the empty result a baseline — the file destroyed by the command meant
        # to maintain it.
        base = baseline({"swift-testing::x": (2, ["timeout"])}, runs=2)
        empty = gb.parse_run(
            'error: unable to find utility "simctl", not a developer tool\n'
            "** TEST FAILED **\n"
        )
        self.assertTrue(empty.complete)
        with self.assertRaises(gb.CannotEvaluate):
            gb.merge(base, empty, plan="PlayheadFastTests")

    def test_merge_REFUSES_an_empty_run_even_with_NO_baseline_to_compare(self):
        # Isolates the zero-results guard. With a populated baseline the
        # too-few-reached guard would refuse anyway, so only the FIRST ever
        # --accept-baseline exercises this one — and that is exactly when a
        # garbage run would be enshrined as the definition of known-broken.
        empty = gb.parse_run("** TEST FAILED **\n")
        with self.assertRaises(gb.CannotEvaluate):
            gb.merge(gb.empty_baseline("PlayheadFastTests"), empty,
                     plan="PlayheadFastTests")

    def test_merge_REFUSES_a_run_that_reached_too_little_of_the_baseline(self):
        base = baseline(
            {"swift-testing::a": (2, ["timeout"]),
             "swift-testing::b": (2, ["timeout"]),
             "swift-testing::c": (2, ["timeout"]),
             "swift-testing::d": (2, ["timeout"])},
            runs=2,
        )
        run = gb.parse_run(log(st_fail_timeout("a")))  # 1 of 4
        with self.assertRaises(gb.CannotEvaluate):
            gb.merge(base, run, plan="PlayheadFastTests")

    def test_merge_unions_kinds_rather_than_replacing_them(self):
        base = baseline({"swift-testing::x": (1, ["timeout"])}, runs=1)
        merged = gb.merge(base, gb.parse_run(log(st_fail_expect("x"))),
                          plan="PlayheadFastTests")
        self.assertEqual(["assertion", "timeout"],
                         sorted(merged["tests"]["swift-testing::x"]["kinds"]))

    def test_merge_REFUSES_an_incomplete_log(self):
        base = baseline({}, runs=0)
        run = gb.parse_run("lint: clean\n" + st_fail_timeout("x"))
        with self.assertRaises(gb.CannotEvaluate):
            gb.merge(base, run, plan="PlayheadFastTests")

    def test_merge_resets_when_the_plan_changes(self):
        base = baseline({"swift-testing::x": (2, ["timeout"])}, runs=2,
                        plan="PlayheadFastTests")
        merged = gb.merge(base, gb.parse_run(log(st_fail_timeout("q"))),
                          plan="PlayheadIntegrationTests")
        self.assertEqual("PlayheadIntegrationTests", merged["plan"])
        self.assertEqual(1, merged["runs_observed"])
        self.assertNotIn("swift-testing::x", merged["tests"])

    def test_merged_file_round_trips_through_json(self):
        base = baseline({}, runs=0)
        merged = gb.merge(base, gb.parse_run(log(st_fail_timeout("x"))),
                          plan="PlayheadFastTests")
        with tempfile.TemporaryDirectory() as d:
            p = pathlib.Path(d) / "b.json"
            gb.save_baseline(p, merged)
            self.assertEqual(merged, gb.load_baseline(p))
            self.assertTrue(p.read_text().endswith("\n"))


class CrashedHostMergeTests(unittest.TestCase):
    """playhead-tl6l — a crash must not shrink the file from inside `accept`.

    `merge` prunes anything the run did not reach, on the theory that it was
    renamed or deleted. A crash makes that theory false, so the one command
    whose job is to maintain the record would have quietly deleted exactly the
    entries the crash hid — and the shrinking diff would have read as good news.
    """

    # Every fixture keeps at least half the recorded set REACHED, or `merge`'s
    # own "did this run exercise the plan" guard refuses before the prune is
    # reached and the rail proves nothing.
    ANCHOR = "swift-testing::anchor"

    def test_a_recorded_entry_with_NO_VERDICT_is_carried_forward_not_pruned(self):
        base = baseline({self.ANCHOR: (3, ["timeout"]),
                         "swift-testing::known": (3, ["timeout"])}, runs=3)
        run = gb.parse_run(
            log(st_fail_timeout("anchor"), st_silent("known")) + HOST_RESTART
        )
        merged = gb.merge(base, run, plan="PlayheadFastTests")
        self.assertIn("swift-testing::known", merged["tests"])
        entry = merged["tests"]["swift-testing::known"]
        # Carried forward means UNCHANGED — the run observed nothing, so it must
        # not be credited with an observation in either direction.
        self.assertEqual(3, entry["seen_runs"])
        self.assertEqual(3, entry["failed_runs"])

    def test_a_genuinely_unreached_entry_is_still_pruned(self):
        # The prune is load-bearing (it is what keeps the pass-direction arm
        # affordable) and must survive this change untouched.
        base = baseline({self.ANCHOR: (3, ["timeout"]),
                         "swift-testing::gone": (3, ["timeout"])}, runs=3)
        run = gb.parse_run(log(st_fail_timeout("anchor")))
        merged = gb.merge(base, run, plan="PlayheadFastTests")
        self.assertNotIn("swift-testing::gone", merged["tests"])

    def test_a_newly_SKIPPED_entry_is_still_pruned_not_protected(self):
        # A skip is a decision somebody made; a crash is not. Protecting a skip
        # would re-hide the very thing the ABSENT arm exists to surface.
        base = baseline({self.ANCHOR: (3, ["timeout"]),
                         "swift-testing::known": (3, ["timeout"])}, runs=3)
        run = gb.parse_run(log(st_fail_timeout("anchor"), st_skip("known")))
        merged = gb.merge(base, run, plan="PlayheadFastTests")
        self.assertNotIn("swift-testing::known", merged["tests"])


class PlanScopeTests(unittest.TestCase):
    def test_a_baseline_for_another_plan_refuses_to_judge(self):
        base = baseline({"swift-testing::x": (2, ["timeout"])}, plan="PlayheadFastTests")
        run = gb.parse_run(log(st_pass("x")))
        v = gb.verdict(base, run, plan="PlayheadIntegrationTests")
        self.assertEqual(gb.EXIT_CANNOT_EVALUATE, v.exit_code)
        self.assertIn("PlayheadIntegrationTests", v.render())


# ---------------------------------------------------------------------------
# CLI — what fast-gate.sh actually calls.
# ---------------------------------------------------------------------------

class CLITests(unittest.TestCase):
    def _run(self, args):
        return gb.main(args)

    def test_check_exits_zero_on_exactly_the_baseline(self):
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            gb.save_baseline(d / "b.json", baseline({"swift-testing::k": (3, ["timeout"])}))
            (d / "run.log").write_text(log(st_fail_timeout("k")), encoding="utf-8")
            self.assertEqual(
                0, self._run(["check", "--log", str(d / "run.log"),
                              "--baseline", str(d / "b.json")])
            )

    def test_check_exits_nonzero_on_a_new_failure(self):
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            gb.save_baseline(d / "b.json", baseline({"swift-testing::k": (3, ["timeout"])}))
            (d / "run.log").write_text(
                log(st_fail_timeout("k"), st_fail_expect("new")), encoding="utf-8"
            )
            self.assertEqual(
                gb.EXIT_REGRESSION,
                self._run(["check", "--log", str(d / "run.log"),
                           "--baseline", str(d / "b.json")]),
            )

    def test_check_with_a_MISSING_baseline_file_cannot_evaluate(self):
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            (d / "run.log").write_text(log(st_pass("a")), encoding="utf-8")
            self.assertEqual(
                gb.EXIT_CANNOT_EVALUATE,
                self._run(["check", "--log", str(d / "run.log"),
                           "--baseline", str(d / "nope.json")]),
            )

    def test_accept_writes_the_file_and_exits_zero(self):
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            (d / "run.log").write_text(log(st_fail_timeout("k")), encoding="utf-8")
            rc = self._run(["accept", "--log", str(d / "run.log"),
                            "--baseline", str(d / "b.json")])
            self.assertEqual(0, rc)
            written = gb.load_baseline(d / "b.json")
            self.assertIn("swift-testing::k", written["tests"])

    def test_accept_refuses_an_incomplete_log_and_writes_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            (d / "run.log").write_text("lint: clean\n" + st_fail_timeout("k"),
                                       encoding="utf-8")
            rc = self._run(["accept", "--log", str(d / "run.log"),
                            "--baseline", str(d / "b.json")])
            self.assertEqual(gb.EXIT_CANNOT_EVALUATE, rc)
            self.assertFalse((d / "b.json").exists())


# ---------------------------------------------------------------------------
# What an accept SAYS. playhead-26od R5: it used to say membership and nothing
# else, which let one accept describe 28 entries as "all timeouts" while three
# were assertion-only, and arm fifteen hard failures without naming one.
# ---------------------------------------------------------------------------

class TierChangeTests(unittest.TestCase):
    def test_crossing_into_deterministic_is_reported_as_a_promotion(self):
        base = baseline({"swift-testing::x": (2, ["timeout"])}, runs=2)
        merged = gb.merge(base, gb.parse_run(log(st_fail_timeout("x"))),
                          plan="PlayheadFastTests")
        promoted, demoted = gb.tier_changes(base, merged)
        self.assertEqual(["swift-testing::x"], promoted)
        self.assertEqual([], demoted)

    def test_an_ALREADY_deterministic_entry_is_not_re_announced(self):
        # Only a CHANGE is news. Re-announcing every deterministic entry on
        # every accept is how the loud line stops being read.
        base = baseline({"swift-testing::x": (3, ["timeout"])}, runs=3)
        merged = gb.merge(base, gb.parse_run(log(st_fail_timeout("x"))),
                          plan="PlayheadFastTests")
        self.assertEqual(gb.TIER_DETERMINISTIC, gb.tier_of(merged["tests"]["swift-testing::x"]))
        self.assertEqual(([], []), gb.tier_changes(base, merged))

    def test_a_newly_added_entry_can_never_be_a_promotion(self):
        # It enters at seen_runs=1, below MIN_RUNS_FOR_DETERMINISTIC.
        base = baseline({}, runs=2)
        merged = gb.merge(base, gb.parse_run(log(st_fail_timeout("fresh"))),
                          plan="PlayheadFastTests")
        self.assertIn("swift-testing::fresh", merged["tests"])
        self.assertEqual(([], []), gb.tier_changes(base, merged))

    def test_falling_out_of_deterministic_is_reported_as_a_demotion(self):
        base = baseline({"swift-testing::x": (3, ["timeout"])}, runs=3)
        merged = gb.merge(base, gb.parse_run(log(st_pass("x"))),
                          plan="PlayheadFastTests")
        promoted, demoted = gb.tier_changes(base, merged)
        self.assertEqual([], promoted)
        self.assertEqual(["swift-testing::x"], demoted)

    def test_a_plan_change_resets_rather_than_reporting_phantom_promotions(self):
        base = baseline({"swift-testing::x": (3, ["timeout"])}, runs=3,
                        plan="PlayheadIntegrationTests")
        merged = gb.merge(base, gb.parse_run(log(st_fail_timeout("x"))),
                          plan="PlayheadFastTests")
        self.assertEqual(([], []), gb.tier_changes(base, merged))

    def test_the_kind_census_names_every_kind_not_just_the_majority(self):
        entries = [
            {"kinds": ["timeout"]},
            {"kinds": ["timeout"]},
            {"kinds": ["assertion"]},
            {"kinds": ["assertion", "timeout"]},
            {"kinds": []},
        ]
        self.assertEqual(
            {"timeout": 2, "assertion": 1, "assertion+timeout": 1, "unknown": 1},
            gb.kind_census(entries),
        )


class AcceptOutputTests(unittest.TestCase):
    """The accept transcript is the operator's only view of what they accepted."""

    def _accept(self, base_tests, base_runs, run_log):
        import contextlib
        import io
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            (d / "run.log").write_text(run_log, encoding="utf-8")
            if base_tests is not None:
                gb.save_baseline(d / "b.json", baseline(base_tests, runs=base_runs))
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                rc = gb.main(["accept", "--log", str(d / "run.log"),
                              "--baseline", str(d / "b.json")])
            return rc, buffer.getvalue()

    def test_every_added_entry_carries_its_KIND(self):
        # The defect this closes: an accept whose added set was three quarters
        # timeouts was justified as "all timeouts" because no kind was on screen.
        rc, out = self._accept({}, 2, log(st_fail_timeout("slow"),
                                          st_fail_expect("wrong")))
        self.assertEqual(0, rc)
        self.assertIn("+ [timeout] swift-testing::slow", out)
        self.assertIn("+ [assertion] swift-testing::wrong", out)

    def test_the_added_set_is_summarised_by_kind(self):
        rc, out = self._accept({}, 2, log(st_fail_timeout("slow"),
                                          st_fail_expect("wrong")))
        self.assertEqual(0, rc)
        self.assertIn("added 2: 1 assertion, 1 timeout", out)

    def test_a_promotion_is_ANNOUNCED_and_named(self):
        rc, out = self._accept({"swift-testing::x": (2, ["timeout"])}, 2,
                               log(st_fail_timeout("x")))
        self.assertEqual(0, rc)
        self.assertIn("ARMED", out)
        self.assertIn("now deterministic [timeout] 3/3  swift-testing::x", out)

    def test_an_accept_that_promotes_NOTHING_stays_quiet(self):
        rc, out = self._accept({"swift-testing::x": (1, ["timeout"])}, 1,
                               log(st_fail_timeout("x")))
        self.assertEqual(0, rc)
        self.assertNotIn("ARMED", out)

    def test_an_accept_over_a_crashed_run_SAYS_SO(self):
        # playhead-tl6l. An accept is a claim a human signs in a commit message,
        # and "this observation says nothing about N tests" is part of it.
        rc, out = self._accept({"swift-testing::x": (1, ["timeout"])}, 1,
                               log(st_fail_timeout("x"), st_silent("lost"))
                               + HOST_RESTART)
        self.assertEqual(0, rc)
        self.assertIn("NO VERDICT", out)
        self.assertIn("restarted the test host", out)

    def test_an_accept_ANNOUNCES_what_it_carried_forward(self):
        rc, out = self._accept({"swift-testing::x": (1, ["timeout"]),
                                "swift-testing::lost": (1, ["timeout"])}, 1,
                               log(st_fail_timeout("x"), st_silent("lost"))
                               + HOST_RESTART)
        self.assertEqual(0, rc)
        self.assertIn("CARRIED FORWARD", out)
        self.assertIn("= swift-testing::lost", out)

    def test_a_clean_accept_says_nothing_about_crashes(self):
        rc, out = self._accept({"swift-testing::x": (1, ["timeout"])}, 1,
                               log(st_fail_timeout("x")))
        self.assertEqual(0, rc)
        self.assertNotIn("NO VERDICT", out)
        self.assertNotIn("CARRIED FORWARD", out)


class RealCrashedRunTests(unittest.TestCase):
    """The two 2026-08-12 full-plan logs, byte-for-byte, playhead-tl6l.

    Everything else in this file is synthetic. These are the actual runs the
    bead was filed from — 7.2 MB each, both with a dead test host — and they are
    the only rails that prove the module reads a real crash rather than a
    fixture built to match its own parser.
    """

    SCRATCH = pathlib.Path(
        "/private/tmp/claude-501/-Users-dabrams-playhead/"
        "bee44b12-4e4b-4bbd-ada7-8d211ef3d4e6/scratchpad"
    )

    def _run(self, relative):
        path = self.SCRATCH / relative
        if not path.exists():
            self.skipTest("preserved gate log not present: %s" % path)
        return gb.parse_run(path.read_text(encoding="utf-8", errors="replace"))

    def _check(self, relative, expected_no_verdict, entries, distinct):
        run = self._run(relative)
        self.assertTrue(run.complete)
        self.assertEqual(1, run.host_restarts)
        # The measured census. Both counts were derived independently of this
        # module (source-level @Test mapping) before it was written.
        self.assertEqual(expected_no_verdict, len(run.no_verdict))
        self.assertEqual(entries, len(run.blamed_entries))
        self.assertEqual(distinct, len(run.blamed))
        base = gb.load_baseline(ROOT / "scripts" / "gate-baseline.PlayheadFastTests.json")
        v = gb.verdict(base, run, plan="PlayheadFastTests")
        first = v.render().splitlines()[0]
        self.assertIn("NO VERDICT", first)
        self.assertIn("crashed host", first)
        return run, v

    def test_the_mn5e_branch_run(self):
        # 15 lost, and 19 summary entries of which 4 are retries of a name
        # already listed.
        run, v = self._check("mn5e-r6/fullplan.log", 15, 19, 15)
        self.assertEqual(71, len(v.known_failures))
        self.assertEqual(16, len(v.new_failures))
        # None of the lost tests leaked into either side of the arithmetic.
        self.assertFalse(set(v.no_verdict) & set(v.new_failures))
        self.assertFalse(set(v.no_verdict) & set(v.known_failures))

    def test_the_main_control_run(self):
        run, v = self._check("main-control/main-fullplan.log", 33, 18, 14)
        self.assertEqual(85, len(v.known_failures))
        self.assertEqual(14, len(v.new_failures))
        # The one genuine casualty that never even printed a `started` line, so
        # only the summary block can see it.
        self.assertIn(
            "SkipOrchestratorRevertTests."
            "failedSuggestNoRestoresLatestBufferedRevision()",
            v.blamed_unmatched,
        )

    def test_the_skips_are_subtracted_and_it_MATTERS(self):
        # Measured: without parsing skips the census reads 45 and 63 rather than
        # 15 and 33, and 30 of the difference is PerfGate on both runs.
        for relative in ("mn5e-r6/fullplan.log", "main-control/main-fullplan.log"):
            run = self._run(relative)
            naive = run.started - run.ran
            self.assertEqual(30, len({k for k in naive - run.no_verdict
                                      if k.startswith(gb.FRAMEWORK_XCTEST)}))

    def test_the_old_code_saw_NONE_of_this(self):
        # The regression rail proper: before this change the verdict line was
        # `RED (85 known / 14 NEW)` with no mention of a crash, on a run where
        # 33 tests never reached a verdict. Assert the run really does contain
        # what the old parser had no category for.
        run = self._run("main-control/main-fullplan.log")
        self.assertGreater(len(run.no_verdict), 0)
        self.assertGreater(len(run.blamed_entries), 0)
        self.assertGreater(run.host_restarts, 0)


class CommittedBaselineTests(unittest.TestCase):
    """The file the gate actually reads must stay loadable and self-consistent."""

    def test_every_committed_baseline_parses_and_is_internally_consistent(self):
        paths = sorted((ROOT / "scripts").glob("gate-baseline.*.json"))
        if not paths:
            self.skipTest("no committed baseline yet")
        for path in paths:
            data = gb.load_baseline(path)
            # The filename is the plan, so a file can never be applied to the
            # wrong population by a rename.
            self.assertEqual(path.name, "gate-baseline.%s.json" % data["plan"])
            self.assertGreaterEqual(data["runs_observed"], 1)
            for key, entry in data["tests"].items():
                self.assertTrue(key.startswith(entry["framework"] + "::"), key)
                self.assertGreaterEqual(entry["seen_runs"], entry["failed_runs"], key)
                self.assertGreaterEqual(entry["failed_runs"], 1, key)
                self.assertLessEqual(entry["seen_runs"], data["runs_observed"], key)
                self.assertTrue(entry["kinds"], key)
                for kind in entry["kinds"]:
                    self.assertIn(kind, (gb.KIND_TIMEOUT, gb.KIND_ASSERTION,
                                         gb.KIND_UNKNOWN), key)


class FastGateWiringTests(unittest.TestCase):
    """scripts/fast-gate.sh, driven for real against a stubbed xcodebuild.

    The verdict logic above is pure and cheap to test; the thing that actually
    reaches a human is the SHELL composition of xcodebuild's exit code with the
    check's. That is where a mistake silently turns 65 into 0, so it is exercised
    end to end here rather than reasoned about.
    """

    def _skeleton(self, tmp, log_text, xcodebuild_rc=65):
        tmp = pathlib.Path(tmp)
        (tmp / "scripts").mkdir()
        # playhead-3nfa put `scripts/disk_preflight.py` in front of everything
        # fast-gate.sh does, and this skeleton was not told. Every one of the
        # eleven tests below then died at exit 28 — "no such file" reads to the
        # shell exactly like "the volume is short" — so the rails certifying the
        # gate's exit-code composition certified nothing, on main, for as long as
        # nobody ran them. Copy it, and see the dedicated wiring test below for
        # why the rest of the class then skips it deliberately.
        for name in ("fast-gate.sh", "gate_baseline.py", "disk_preflight.py"):
            (tmp / "scripts" / name).write_bytes((ROOT / "scripts" / name).read_bytes())
        (tmp / "scripts" / "fast-gate.sh").chmod(0o755)
        # A scheme that already names the plan, so the xcodegen bootstrap — which
        # needs the gitignored model — never triggers.
        scheme = tmp / "Playhead.xcodeproj" / "xcshareddata" / "xcschemes"
        scheme.mkdir(parents=True)
        (scheme / "Playhead.xcscheme").write_text("PlayheadFastTests.xctestplan\n")

        (tmp / "run.log").write_text(log_text, encoding="utf-8")
        bindir = tmp / "bin"
        bindir.mkdir()
        stub = bindir / "xcodebuild"
        stub.write_text(
            "#!/bin/sh\ncat %s\nexit %d\n" % (tmp / "run.log", xcodebuild_rc)
        )
        stub.chmod(0o755)
        return tmp, bindir

    def _run(self, tmp, bindir, args, extra_env=None):
        import os
        import subprocess

        env = dict(os.environ)
        env["PATH"] = str(bindir) + os.pathsep + env["PATH"]
        env["PLAYHEAD_SKIP_LINT"] = "1"
        # These tests are about how the gate composes xcodebuild's exit code with
        # the baseline verdict. Leaving the disk preflight armed would make all
        # eleven of them depend on how much room the box happens to have, which
        # is a different subject and a flake. The preflight's own wiring — that
        # it runs, that a refusal is 28, that xcodebuild is never reached — is
        # asserted once, explicitly, in `test_the_disk_preflight_runs_BEFORE_xcodebuild`.
        env["PLAYHEAD_SKIP_DISK_PREFLIGHT"] = "1"
        env.pop("PLAYHEAD_SKIP_BASELINE", None)
        if extra_env:
            env.update(extra_env)
        proc = subprocess.run(
            ["bash", str(tmp / "scripts" / "fast-gate.sh")] + args,
            cwd=str(tmp), env=env, capture_output=True, text=True,
        )
        return proc

    def _with_baseline(self, tmp, tests, runs=3):
        gb.save_baseline(
            tmp / "scripts" / "gate-baseline.PlayheadFastTests.json",
            baseline(tests, runs=runs),
        )

    def test_known_failures_only_make_the_gate_exit_ZERO(self):
        with tempfile.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(d, log(st_fail_timeout("known")))
            self._with_baseline(tmp, {"swift-testing::known": (3, ["timeout"])})
            proc = self._run(tmp, bindir, [])
            self.assertIn("RED (1 known / 0 new)", proc.stdout)
            self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_a_new_failure_makes_the_gate_exit_65_and_names_it(self):
        with tempfile.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(
                d, log(st_fail_timeout("known"), st_fail_expect("stranger"))
            )
            self._with_baseline(tmp, {"swift-testing::known": (3, ["timeout"])})
            proc = self._run(tmp, bindir, [])
            self.assertEqual(65, proc.returncode)
            self.assertIn("stranger", proc.stdout)
            self.assertIn("1 NEW", proc.stdout)

    def test_a_selective_run_passes_the_raw_exit_code_through(self):
        # scripts/mutation-battery.sh drives fast-gate.sh this way and reads the
        # exit code as the mutation's verdict. Absorbing a focused failure into
        # "known" would credit every mutation as KILLED.
        with tempfile.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(d, log(st_fail_expect("focused")))
            self._with_baseline(tmp, {"swift-testing::known": (3, ["timeout"])})
            proc = self._run(
                tmp, bindir, ["-only-testing:PlayheadTests/SomeSuite"]
            )
            self.assertEqual(65, proc.returncode)
            self.assertIn("SKIPPED", proc.stdout)

    def test_a_selective_run_that_PASSES_still_exits_zero(self):
        with tempfile.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(
                d, log(st_pass("focused"), terminal=TERMINAL_PASSED), xcodebuild_rc=0
            )
            self._with_baseline(tmp, {"swift-testing::known": (3, ["timeout"])})
            proc = self._run(tmp, bindir, ["-only-testing:PlayheadTests/S"])
            self.assertEqual(0, proc.returncode)

    def test_accept_baseline_writes_the_file(self):
        with tempfile.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(d, log(st_fail_timeout("fresh")))
            proc = self._run(tmp, bindir, ["--accept-baseline"])
            self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
            written = gb.load_baseline(
                tmp / "scripts" / "gate-baseline.PlayheadFastTests.json"
            )
            self.assertIn("swift-testing::fresh", written["tests"])
            self.assertIn("+ [timeout] swift-testing::fresh", proc.stdout)

    def test_accept_baseline_REFUSES_a_selective_run(self):
        # Accepting a filtered run would delete every entry the filter excluded
        # and call the result a baseline.
        with tempfile.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(d, log(st_fail_timeout("fresh")))
            self._with_baseline(tmp, {"swift-testing::known": (3, ["timeout"])})
            proc = self._run(
                tmp, bindir, ["--accept-baseline", "-only-testing:PlayheadTests/S"]
            )
            self.assertEqual(2, proc.returncode)
            self.assertIn("REFUSED", proc.stderr)
            still = gb.load_baseline(
                tmp / "scripts" / "gate-baseline.PlayheadFastTests.json"
            )
            self.assertIn("swift-testing::known", still["tests"])

    def test_accept_baseline_is_never_forwarded_to_xcodebuild(self):
        with tempfile.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(d, log(st_fail_timeout("fresh")))
            stub = bindir / "xcodebuild"
            stub.write_text(
                '#!/bin/sh\nfor a in "$@"; do\n'
                '  [ "$a" = "--accept-baseline" ] && { echo LEAKED; exit 9; }\n'
                "done\ncat %s\nexit 65\n" % (tmp / "run.log")
            )
            stub.chmod(0o755)
            proc = self._run(tmp, bindir, ["--accept-baseline"])
            self.assertNotIn("LEAKED", proc.stdout)
            self.assertEqual(0, proc.returncode)

    def test_a_build_failure_is_not_laundered_into_a_pass(self):
        # No terminal verdict: the check cannot evaluate, so xcodebuild's own
        # exit code must survive. This is the single most dangerous direction —
        # a broken build reporting green.
        with tempfile.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(
                d, "error: no such module 'Foo'\n** BUILD FAILED **\n", xcodebuild_rc=65
            )
            self._with_baseline(tmp, {"swift-testing::known": (3, ["timeout"])})
            proc = self._run(tmp, bindir, [])
            self.assertEqual(65, proc.returncode)
            self.assertIn("CANNOT EVALUATE", proc.stdout)

    def test_a_missing_baseline_file_leaves_the_raw_exit_code_alone(self):
        with tempfile.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(d, log(st_fail_timeout("x")))
            proc = self._run(tmp, bindir, [])
            self.assertEqual(65, proc.returncode)

    def test_the_skip_env_var_bypasses_the_check(self):
        with tempfile.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(d, log(st_fail_timeout("known")))
            self._with_baseline(tmp, {"swift-testing::known": (3, ["timeout"])})
            proc = self._run(tmp, bindir, [], {"PLAYHEAD_SKIP_BASELINE": "1"})
            self.assertEqual(65, proc.returncode)
            self.assertIn("bypassed", proc.stdout)

    def test_a_deterministic_member_passing_turns_a_GREEN_xcodebuild_run_RED(self):
        # The pass-direction arm has to be able to overturn a 0 from xcodebuild,
        # or Dan's decision is unimplementable.
        with tempfile.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(
                d, log(st_pass("known"), terminal=TERMINAL_PASSED), xcodebuild_rc=0
            )
            self._with_baseline(tmp, {"swift-testing::known": (3, ["timeout"])})
            proc = self._run(tmp, bindir, [])
            self.assertEqual(65, proc.returncode)
            self.assertIn("NOW PASSES", proc.stdout)

    def test_a_crashed_host_reaches_the_OPERATOR_even_though_it_exits_zero(self):
        """playhead-tl6l's exit-code decision, asserted end to end.

        NOT fatal — it fires on main today, on a pre-existing crash owned by
        another bead, and a gate that is red for a reason its reader cannot fix
        is one they learn to route around. What it must never do is stay quiet:
        the reassuring `RED (N known / 0 new)` has to arrive with the loss
        attached, or the exit code is a lie of omission.
        """
        with tempfile.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(
                d, log(st_fail_timeout("known"), st_silent("lost")) + HOST_RESTART
            )
            self._with_baseline(tmp, {"swift-testing::known": (3, ["timeout"])})
            proc = self._run(tmp, bindir, [])
            self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
            self.assertIn("RED (1 known / 0 new)", proc.stdout)
            self.assertIn("NO VERDICT", proc.stdout)
            self.assertIn("crashed host", proc.stdout)
            self.assertIn("swift-testing::lost", proc.stdout)

    def test_a_crashed_run_that_ALSO_regressed_still_exits_65(self):
        # The loss must not launder a real regression into a quieter category.
        with tempfile.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(
                d,
                log(st_fail_timeout("known"), st_fail_expect("stranger"),
                    st_silent("lost")) + HOST_RESTART,
            )
            self._with_baseline(tmp, {"swift-testing::known": (3, ["timeout"])})
            proc = self._run(tmp, bindir, [])
            self.assertEqual(65, proc.returncode)
            self.assertIn("1 NEW", proc.stdout)
            self.assertIn("NO VERDICT", proc.stdout)

    def test_the_disk_preflight_runs_BEFORE_xcodebuild(self):
        """The one test in this class that leaves the preflight armed.

        It is what makes skipping it in the other eleven honest. A refusal must
        be exit 28 (POSIX ENOSPC — the error a wedged gate would otherwise have
        hidden) and must happen before xcodebuild is reached at all; the stub
        prints the run log, so an empty stdout is the proof that it never ran.
        The threshold is forced impossibly high rather than depending on how
        much room this box has.
        """
        with tempfile.TemporaryDirectory() as d:
            tmp, bindir = self._skeleton(d, log(st_fail_timeout("known")))
            self._with_baseline(tmp, {"swift-testing::known": (3, ["timeout"])})
            proc = self._run(tmp, bindir, [], {
                "PLAYHEAD_SKIP_DISK_PREFLIGHT": "0",
                "PLAYHEAD_DISK_MIN_GIB": "999999",
            })
            self.assertEqual(28, proc.returncode, proc.stdout + proc.stderr)
            self.assertNotIn("Test run with", proc.stdout)


if __name__ == "__main__":
    unittest.main()
