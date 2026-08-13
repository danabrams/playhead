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
import os
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


def baseline(tests, runs=3, plan="PlayheadFastTests", no_verdict=None,
             census_runs=None, census_lost=None):
    """Build a baseline dict. `tests` is {key: (failed_runs, kinds)}.

    `no_verdict` defaults to None, which builds a file with NO recorded
    crashed-host census — the shape every baseline on disk has until somebody
    accepts one, and the shape whose arm is inert. Pass a list (including an
    EMPTY list, which is a positive claim rather than an absence) to build a
    recorded one.

    The census is a UNION WITH OBSERVATION COUNTS (playhead-tl6l R4), so a
    recorded name needs a `seen_runs`/`lost_runs` pair. `census_runs` is how
    many observations back the record — it defaults to the threshold, i.e. an
    ARMED record, because that is the state most rails are about — and
    `census_lost` overrides `lost_runs` per name so a LOAD-SENSITIVE entry can
    be built (lost in some observations, not all).
    """
    out = {
        "plan": plan,
        "mode": "full-plan",
        "runs_observed": runs,
        "tests": {},
    }
    if no_verdict is not None:
        observations = (gb.MIN_RUNS_FOR_DETERMINISTIC if census_runs is None
                        else census_runs)
        lost = dict(census_lost or {})
        out[gb.NO_VERDICT_KEY] = {
            gb.CENSUS_RUNS_KEY: observations,
            gb.CENSUS_TESTS_KEY: {
                key: {"seen_runs": observations,
                      "lost_runs": lost.get(key, observations)}
                for key in sorted(no_verdict)
            },
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

    def test_a_real_log_cut_short_is_reported_incomplete(self):
        # This rail used to read the 2026-08-01 combined-main log everyone was
        # quoting "72 failures" from — 930 XCTest cases started, 900 passed, no
        # terminal verdict — out of a session scratchpad, and `skipTest` when it
        # was gone. It had been skipping ever since (playhead-fer3's defect
        # class, found a second time in the same file): a rail that reports OK
        # without running.
        #
        # The claim does not need THAT log. Any real log with its tail cut is a
        # fragment, so it is made from a committed fixture, and it runs.
        text = (FIXTURES / "crashed-run-main-76b0a09a.log").read_text(encoding="utf-8")
        self.assertTrue(gb.parse_run(text).complete)
        cut = [line for line in text.splitlines()
               if not gb._TERMINAL.search(line)]
        run = gb.parse_run("\n".join(cut) + "\n")
        self.assertFalse(run.complete)
        # …and it is genuinely a run's worth of material, not an empty string:
        # the point is that a fragment full of results still refuses judgement.
        self.assertGreater(len(run.failures), 50)
        v = gb.verdict(baseline({"swift-testing::x": (3, ["timeout"])}), run)
        self.assertEqual(gb.EXIT_CANNOT_EVALUATE, v.exit_code)
        self.assertIn("CANNOT EVALUATE", v.render())

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
        # playhead-buvn FLIPPED HALF OF THIS, and this is the half that stands.
        # tl6l's call was "reportable, not fatal" outright. It is now "reportable
        # while the population is UNRECORDED" — a baseline that has never
        # recorded a census makes no claim about how many tests may be lost, so
        # there is nothing to exceed and the arm is inert. That is what lets the
        # arming land without turning main red for a pre-existing crash owned by
        # another bead, which is the trade CLAUDE.md's lint policy describes.
        base = baseline({"swift-testing::known": (3, ["timeout"])})
        self.assertNotIn(gb.NO_VERDICT_KEY, base)
        v = gb.verdict(base, self._crashed())
        self.assertIsNone(v.no_verdict_recorded)
        self.assertEqual(gb.EXIT_OK, v.exit_code)
        self.assertIn("NO VERDICT", v.render())
        # …and it must SAY it is inert, or a reader takes exit 0 as a claim.
        self.assertIn("NOT RECORDED", v.render())
        self.assertIn("INERT", v.render())

    def test_a_crashed_run_can_never_be_GREEN(self):
        # Constructed so the exit code cannot be what keeps GREEN away: the
        # casualty IS recorded, so nothing is new and the run exits 0. GREEN is
        # still unreachable, which is the claim.
        run = gb.parse_run(log(st_pass("fine"), st_silent("lost")) + HOST_RESTART)
        base = baseline({}, no_verdict=["swift-testing::lost"])
        v = gb.verdict(base, run)
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


class ArmedCensusTests(unittest.TestCase):
    """playhead-buvn — the census is armed on the DIFF, not on the count.

    The hole this closes, stated as tl6l's R1 review stated it: a change that
    CRASHES the test host used to exit 0. Its victims are HEALTHY tests, so
    they are in nobody's failure baseline — `new_failures` is empty because
    they emitted no failure line, and `absent` is empty because that arm covers
    only tests already recorded as broken. The crash destroyed the evidence of
    itself and the gate agreed with it.
    """

    RECORD = ["swift-testing::known-casualty"]

    def _run(self, *silent):
        return gb.parse_run(
            log(st_pass("healthy"), *[st_silent(name) for name in silent])
            + HOST_RESTART
        )

    def test_a_casualty_NOT_in_the_record_FAILS_the_gate(self):
        # THE POINT OF THE BEAD. Nothing else on the verdict is red: no new
        # failure, no kind change, no absent member, no fiction. The census is
        # the only witness the crash left.
        v = gb.verdict(baseline({}, no_verdict=self.RECORD),
                       self._run("known-casualty", "fresh-casualty"))
        self.assertEqual([], v.new_failures)
        self.assertEqual([], v.absent)
        self.assertFalse(v.baseline_fiction)
        self.assertEqual(["swift-testing::fresh-casualty"], v.new_casualties)
        self.assertEqual(gb.EXIT_REGRESSION, v.exit_code)
        self.assertIn("NEW CASUALTY     swift-testing::fresh-casualty", v.render())

    def test_the_HEADLINE_says_which_of_the_casualties_is_the_fatal_one(self):
        # Without this the first line reads `RED (0 known / 0 new) — 2 tests got
        # NO VERDICT (crashed host)` on a run that exits 65, and every category
        # named on it says zero. The reader goes hunting in the wrong place.
        v = gb.verdict(baseline({}, no_verdict=self.RECORD),
                       self._run("known-casualty", "fresh-casualty"))
        first = v.render().splitlines()[0]
        self.assertIn("2 tests got NO VERDICT", first)
        self.assertIn("1 NOT RECORDED", first)
        self.assertEqual(gb.EXIT_REGRESSION, v.exit_code)

    def test_the_HEADLINE_stays_quiet_when_every_casualty_is_recorded(self):
        v = gb.verdict(baseline({}, no_verdict=self.RECORD),
                       self._run("known-casualty"))
        first = v.render().splitlines()[0]
        self.assertIn("1 test got NO VERDICT", first)
        self.assertNotIn("NOT RECORDED", first)

    def test_a_HUGE_loss_is_TRUNCATED_like_every_other_category(self):
        v = gb.verdict(baseline({}, no_verdict=[]),
                       self._run(*("lost-%02d" % i for i in range(40))))
        rendered = v.render()
        named = [ln for ln in rendered.splitlines() if "NEW CASUALTY     swift" in ln]
        self.assertEqual(gb._MAX_LISTED, len(named))
        self.assertIn("NEW CASUALTY     … and 30 more", rendered)
        self.assertEqual(gb.EXIT_REGRESSION, v.exit_code)

    def test_a_casualty_that_IS_in_the_record_is_quiet(self):
        # The other half of the same trade: the pre-existing crash owned by
        # playhead-rouw must not make every gate on this box exit 65.
        v = gb.verdict(baseline({}, no_verdict=self.RECORD),
                       self._run("known-casualty"))
        self.assertEqual([], v.new_casualties)
        self.assertEqual(gb.EXIT_OK, v.exit_code)

    def test_the_SAME_COUNT_with_a_DIFFERENT_NAME_still_fails(self):
        # Why the record is a SET and not a count. One test dies, another
        # recovers, the total is unchanged — and a count would call that quiet.
        v = gb.verdict(baseline({}, no_verdict=self.RECORD),
                       self._run("substituted-casualty"))
        self.assertEqual(1, len(v.no_verdict))
        self.assertEqual(1, len(v.no_verdict_recorded))
        self.assertEqual(["swift-testing::substituted-casualty"], v.new_casualties)
        self.assertEqual(gb.EXIT_REGRESSION, v.exit_code)

    def test_a_RECORDED_EMPTY_census_is_a_CLAIM_and_is_armed(self):
        # `[]` says "no test should lose a verdict". Absent says "nobody looked".
        # Spelling them the same way is the defect class this repo keeps hitting.
        v = gb.verdict(baseline({}, no_verdict=[]), self._run("anything"))
        self.assertEqual(set(), v.no_verdict_recorded)
        self.assertEqual(["swift-testing::anything"], v.new_casualties)
        self.assertEqual(gb.EXIT_REGRESSION, v.exit_code)

    def test_a_FRESH_baseline_records_NOTHING_rather_than_a_ZERO(self):
        # `{"runs_observed": 0, "tests": {}}` would spell "nobody has looked"
        # as a measurement of zero. Three states, three claims: absent is
        # INERT, 1-2 observations is PROVISIONAL, 3+ is ARMED.
        fresh = gb.empty_baseline("PlayheadFastTests")
        self.assertNotIn(gb.NO_VERDICT_KEY, fresh)
        self.assertIsNone(gb.recorded_census(fresh))
        v = gb.verdict(fresh, self._run("anything"), plan="PlayheadFastTests")
        self.assertIsNone(v.no_verdict_recorded)
        self.assertEqual(gb.EXIT_OK, v.exit_code)
        self.assertIn("INERT", v.render())

    def test_an_UNRECORDED_census_is_inert_even_for_a_LARGE_loss(self):
        v = gb.verdict(baseline({}), self._run(*("lost-%d" % i for i in range(40))))
        self.assertEqual(40, len(v.no_verdict))
        self.assertEqual([], v.new_casualties)
        self.assertEqual(gb.EXIT_OK, v.exit_code)
        self.assertIn("INERT", v.render())

    def test_FEWER_casualties_than_recorded_reports_the_IMPROVEMENT(self):
        v = gb.verdict(
            baseline({}, no_verdict=["swift-testing::a", "swift-testing::b"]),
            self._run("a"),
        )
        self.assertEqual(["swift-testing::b"], v.recovered_casualties)
        self.assertEqual(gb.EXIT_OK, v.exit_code)
        self.assertIn("REPORTED AGAIN   swift-testing::b", v.render())

    def test_a_run_that_loses_NOTHING_still_reports_the_whole_record_as_recovered(self):
        # The best case, and the one an early draft rendered NOTHING for: the
        # crash block was skipped when `crashed_host` was false, so a run that
        # fixed everything printed no good news at all.
        run = gb.parse_run(log(st_pass("healthy"), terminal=TERMINAL_PASSED))
        v = gb.verdict(baseline({}, no_verdict=["swift-testing::a"]), run)
        self.assertFalse(v.crashed_host)
        self.assertEqual(["swift-testing::a"], v.recovered_casualties)
        self.assertEqual(gb.EXIT_OK, v.exit_code)
        rendered = v.render()
        self.assertIn("REPORTED AGAIN   swift-testing::a", rendered)
        self.assertIn("GREEN", rendered)

    def test_a_HOST_RESTART_that_costs_nobody_a_verdict_is_NOT_fatal(self):
        # buvn step 4, decided explicitly. A restart with nothing silent means
        # every test was re-run and no information was lost. It is reported and
        # it forecloses GREEN; it does not fail the gate on its own.
        run = gb.parse_run(log(st_pass("fine")) + HOST_RESTART)
        v = gb.verdict(baseline({}, no_verdict=[]), run)
        self.assertEqual(1, v.host_restarts)
        self.assertEqual([], v.new_casualties)
        self.assertEqual(gb.EXIT_OK, v.exit_code)
        self.assertNotIn("GREEN", v.render())

    def test_a_BLAMED_name_alone_is_a_LEAD_and_never_a_NEW_CASUALTY(self):
        # The summary block cannot be reconciled against display names, so it
        # must not be allowed to arm anything. Only the started-set census can.
        run = gb.parse_run(
            log(st_pass("fine"), terminal=TERMINAL_PASSED)
            + failing_block("GhostTests.neverPrintedALine()")
        )
        v = gb.verdict(baseline({}, no_verdict=[]), run)
        self.assertEqual(["GhostTests.neverPrintedALine()"], v.blamed_unmatched)
        self.assertEqual([], v.new_casualties)
        self.assertEqual(gb.EXIT_OK, v.exit_code)

    def test_accept_UNIONS_the_census_and_COUNTS_the_observations(self):
        """playhead-tl6l R4 — the record's shape, and why it changed.

        R2 recorded a SET and REPLACED it on every accept, arguing from two
        runs whose casualty sets were IDENTICAL (Jaccard 1.00) that churn was
        not a risk. A third full-plan run on this branch lost FIFTEEN where
        those two lost eleven — the same eleven all three times, four more on a
        loud night, none ever recovering. Under replace, a run like that
        reports four NEW casualties on a record that has never been wrong; the
        union records the four instead and lets the counts say which is which.
        """
        base = baseline({})
        first = gb.merge(base, self._run("a", "b"), plan="PlayheadFastTests")
        census = gb.recorded_census(first)
        self.assertEqual(1, census.runs_observed)
        self.assertEqual({"swift-testing::a", "swift-testing::b"}, census.names)
        # A later run that loses a DIFFERENT set adds to the record rather than
        # replacing it, and every entry carries how often it was seen and lost.
        second = gb.recorded_census(
            gb.merge(first, self._run("b", "c"), plan="PlayheadFastTests"))
        self.assertEqual(2, second.runs_observed)
        self.assertEqual({"swift-testing::a", "swift-testing::b",
                          "swift-testing::c"}, second.names)
        # `a` was never reached on the second (crashed) run, so it is carried
        # forward untouched; `b` lost twice; `c` is new.
        self.assertEqual({"seen_runs": 1, "lost_runs": 1},
                         second.tests["swift-testing::a"])
        self.assertEqual({"seen_runs": 2, "lost_runs": 2},
                         second.tests["swift-testing::b"])
        self.assertEqual({"seen_runs": 1, "lost_runs": 1},
                         second.tests["swift-testing::c"])

    def test_a_recorded_name_that_REPORTS_is_DEMOTED_not_deleted(self):
        # The other half of the union, and the answer to R2's objection to one:
        # a name that comes back must be able to LEAVE the record eventually,
        # or a union is a licence nobody can revoke. It leaves by losing ground
        # on the counts, one observation at a time, not by being silently
        # dropped on the strength of a single quiet run.
        base = baseline({}, no_verdict=["swift-testing::a"], census_runs=3)
        merged = gb.recorded_census(gb.merge(
            base, gb.parse_run(log(st_pass("a"), st_pass("healthy"))),
            plan="PlayheadFastTests"))
        self.assertIn("swift-testing::a", merged.names)
        self.assertEqual({"seen_runs": 4, "lost_runs": 3},
                         merged.tests["swift-testing::a"])
        self.assertEqual(gb.TIER_LOAD_SENSITIVE,
                         merged.tier("swift-testing::a"))

    def test_a_DETERMINISTIC_entry_that_REPORTS_AGAIN_fails_the_gate(self):
        """The pass-direction arm, and the price of the union.

        Dan's rule for the failure baseline applied where it is sound: the
        entry claims this test loses its verdict in EVERY observation, the run
        watched it start and report, so the record has rotted. Without this a
        crash that gets genuinely fixed stays recorded forever and the gate
        never speaks about those names again.
        """
        base = baseline({}, no_verdict=["swift-testing::a"], census_runs=3)
        run = gb.parse_run(log(st_pass("a"), st_pass("healthy")))
        v = gb.verdict(base, run)
        self.assertEqual(["swift-testing::a"], v.census_now_reports)
        self.assertEqual(gb.EXIT_REGRESSION, v.exit_code)
        self.assertIn("NOW REPORTS      swift-testing::a", v.render())

    def test_a_LOAD_SENSITIVE_entry_that_reports_again_is_NOT_fatal(self):
        # Lost in 2 of its 3 observations: it does not claim to be lost every
        # time, so reporting is exactly what it said might happen.
        base = baseline({}, no_verdict=["swift-testing::a"], census_runs=3,
                        census_lost={"swift-testing::a": 2})
        v = gb.verdict(base, gb.parse_run(log(st_pass("a"), st_pass("healthy"))))
        self.assertEqual([], v.census_now_reports)
        self.assertEqual(["swift-testing::a"], v.recovered_casualties)
        self.assertEqual(gb.EXIT_OK, v.exit_code)

    def test_a_recorded_name_that_is_now_SKIPPED_is_fatal_and_that_is_a_choice(self):
        """Decided rather than fallen into, because it could go either way.

        The entry claims this test cannot produce an outcome; the run watched
        it start and produce one. That the outcome was a SKIP does not make the
        claim less falsified — the host survived long enough to report it — and
        the population has plainly changed, which is a refresh the operator
        should have to sign. It is also what `tests` already does: a newly
        skipped baseline member is ABSENT and fatal, which is what makes
        PerfGate-ing a family visible instead of a quiet loss of coverage.

        (Swift Testing's own trait-disabled skips can never reach here: they
        emit no `started` line, so they are in neither set.)
        """
        base = baseline({}, no_verdict=["xctest::PlayheadTests.PerfTests/testSlow"],
                        census_runs=3)
        run = gb.parse_run(log(xc_skip("PerfTests", "testSlow"), st_pass("healthy")))
        v = gb.verdict(base, run)
        self.assertEqual(["xctest::PlayheadTests.PerfTests/testSlow"],
                         v.census_now_reports)
        self.assertEqual(gb.EXIT_REGRESSION, v.exit_code)

    def test_a_recorded_name_that_NEVER_STARTED_is_never_fatal(self):
        # A rename, a deletion and a host that died before the start line are
        # indistinguishable from a log. The arm fires on positive evidence
        # only, so it never fires here — however deterministic the entry.
        base = baseline({}, no_verdict=["swift-testing::a"], census_runs=9)
        v = gb.verdict(base, gb.parse_run(log(st_pass("healthy"))))
        self.assertEqual(gb.TIER_DETERMINISTIC,
                         gb.recorded_census(base).tier("swift-testing::a"))
        self.assertEqual([], v.census_now_reports)
        self.assertEqual(gb.EXIT_OK, v.exit_code)
        self.assertIn("did not start at all this run", v.render())

    def test_a_PROVISIONAL_census_NAMES_a_new_casualty_without_failing(self):
        """Tonight's four, and the reason the arm waits.

        Two observations do not bound a population this load-sensitive — this
        one was measured not to be bounded by them. The casualty is named on
        every run from the first, because a newly-observed casualty is
        indistinguishable from a regression; what the third observation buys is
        the right to make it fatal.
        """
        base = baseline({}, no_verdict=self.RECORD, census_runs=2)
        v = gb.verdict(base, self._run("known-casualty", "fresh-casualty"))
        self.assertFalse(v.census_armed)
        self.assertEqual(["swift-testing::fresh-casualty"], v.new_casualties)
        self.assertEqual(gb.EXIT_OK, v.exit_code)
        rendered = v.render()
        self.assertIn("NEW CASUALTY     swift-testing::fresh-casualty", rendered)
        self.assertIn("PROVISIONAL", rendered)
        # …and the headline must not shout what it is not going to act on.
        first = rendered.splitlines()[0]
        self.assertIn("not yet recorded", first)
        self.assertNotIn("NOT RECORDED", first)

    def test_the_THIRD_observation_ARMS_it(self):
        base = baseline({}, no_verdict=self.RECORD, census_runs=3)
        v = gb.verdict(base, self._run("known-casualty", "fresh-casualty"))
        self.assertTrue(v.census_armed)
        self.assertEqual(gb.EXIT_REGRESSION, v.exit_code)
        self.assertNotIn("PROVISIONAL", v.render())

    def test_the_OLD_LIST_SHAPE_is_read_as_ONE_observation_and_arms_nothing(self):
        # No committed file ever carried it, but reading a bare list as an
        # armed record would arm a pass-direction check on counts nobody
        # measured. It is read as the weakest true claim it supports.
        base = baseline({})
        base[gb.NO_VERDICT_KEY] = ["swift-testing::a"]
        v = gb.verdict(base, self._run("a", "b"))
        self.assertEqual({"swift-testing::a"}, v.no_verdict_recorded)
        self.assertEqual(1, v.census_runs_observed)
        self.assertFalse(v.census_armed)
        self.assertEqual(gb.EXIT_OK, v.exit_code)
        self.assertIn("OLD CENSUS SHAPE", v.render())

    def test_accept_on_a_CLEAN_run_CREDITS_an_observation_not_an_erasure(self):
        # Under the replaced set a single clean run wiped the record, which is
        # how eleven measured casualties could be forgotten by one quiet night.
        base = baseline({"swift-testing::known": (3, ["timeout"])},
                        no_verdict=["swift-testing::a"], census_runs=3)
        merged = gb.merge(base, gb.parse_run(log(st_fail_timeout("known"),
                                                 st_pass("a"))),
                          plan="PlayheadFastTests")
        census = gb.recorded_census(merged)
        self.assertEqual(4, census.runs_observed)
        self.assertEqual({"seen_runs": 4, "lost_runs": 3},
                         census.tests["swift-testing::a"])

    def test_the_record_is_PER_PLAN_and_never_crosses(self):
        base = baseline({}, plan="PlayheadIntegrationTests",
                        no_verdict=["swift-testing::a"])
        merged = gb.merge(base, self._run("b"), plan="PlayheadFastTests")
        self.assertEqual("PlayheadFastTests", merged["plan"])
        # A different plan is a different population; carrying the old census
        # across would name tests this plan never runs — including its
        # observation COUNT, which is what would otherwise arrive pre-armed.
        census = gb.recorded_census(merged)
        self.assertEqual({"swift-testing::b"}, census.names)
        self.assertEqual(1, census.runs_observed)


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


class CensusMergeTests(unittest.TestCase):
    """playhead-tl6l R4 — what the UNION owes, and what it must not do.

    A union that only ever grows is a licence nobody can revoke, and a union
    that prunes on a crashed run is the same defect `protected` closes for
    `tests` one layer down: the crash shrinks the record from inside the one
    command whose job is to maintain it. Both directions are pinned here.
    """

    def _census(self, base, run_log):
        return gb.recorded_census(
            gb.merge(base, gb.parse_run(run_log), plan="PlayheadFastTests"))

    def test_a_CRASHED_run_carries_an_unreached_census_entry_forward(self):
        base = baseline({}, no_verdict=["swift-testing::a"], census_runs=3)
        census = self._census(
            base, log(st_pass("healthy"), st_silent("b")) + HOST_RESTART)
        # `a` never started. On a run whose host died that is exactly what the
        # crash does, so it keeps its counts and is credited NO observation.
        self.assertEqual({"seen_runs": 3, "lost_runs": 3},
                         census.tests["swift-testing::a"])
        self.assertEqual(4, census.runs_observed)

    def test_a_HEALTHY_run_PRUNES_a_census_entry_that_never_started(self):
        # The shrink path. Without it a renamed or deleted test stays in the
        # record for good, and the record stops describing the tree.
        base = baseline({}, no_verdict=["swift-testing::gone"], census_runs=3)
        census = self._census(base, log(st_pass("healthy")))
        self.assertEqual(set(), census.names)

    def test_the_census_OBSERVATION_COUNT_is_its_own_and_not_the_files(self):
        # The file may have nine observations of FAILURES and none of the
        # census — which is the state main is in today. Sharing one counter
        # would arrive pre-armed off observations nobody made.
        base = baseline({"swift-testing::k": (9, ["timeout"])}, runs=9)
        census = self._census(base, log(st_fail_timeout("k"), st_silent("lost"))
                              + HOST_RESTART)
        self.assertEqual(10, base["runs_observed"] + 1)
        self.assertEqual(1, census.runs_observed)
        self.assertFalse(census.armed)

    def test_repeated_accepts_over_the_SAME_crash_PROMOTE_it(self):
        # Three observations of the same loss is what buys the right to call it
        # deterministic — and arms its recovery, which is how it can ever leave.
        merged = baseline({"swift-testing::anchor": (5, ["timeout"])}, runs=5)
        crashed = log(st_fail_timeout("anchor"), st_silent("lost")) + HOST_RESTART
        tiers = []
        for _ in range(3):
            merged = gb.merge(merged, gb.parse_run(crashed),
                              plan="PlayheadFastTests")
            census = gb.recorded_census(merged)
            tiers.append(census.tier("swift-testing::lost"))
        self.assertEqual([gb.TIER_LOAD_SENSITIVE, gb.TIER_LOAD_SENSITIVE,
                          gb.TIER_DETERMINISTIC], tiers)
        self.assertEqual({"seen_runs": 3, "lost_runs": 3},
                         census.tests["swift-testing::lost"])

    def test_a_crash_can_never_SHRINK_the_census_across_repeated_accepts(self):
        # The census analogue of test_accept_CARRIES_FORWARD_across_a_SECOND
        # _and_THIRD_crash: the record is maintained BY repeated accepts, so one
        # carry-forward proves nothing.
        base = baseline({"swift-testing::anchor": (5, ["timeout"])}, runs=5,
                        no_verdict=["swift-testing::a", "swift-testing::b"],
                        census_runs=4)
        crashed = log(st_fail_timeout("anchor"), st_silent("a")) + HOST_RESTART
        merged = base
        for _ in range(3):
            merged = gb.merge(merged, gb.parse_run(crashed),
                              plan="PlayheadFastTests")
            census = gb.recorded_census(merged)
            self.assertEqual({"swift-testing::a", "swift-testing::b"},
                             census.names)
            self.assertEqual({"seen_runs": 4, "lost_runs": 4},
                             census.tests["swift-testing::b"])

    def test_the_accept_ANNOUNCES_a_census_promotion_and_the_ARMING(self):
        import contextlib
        import io
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            gb.save_baseline(d / "b.json", baseline(
                {"swift-testing::anchor": (5, ["timeout"])}, runs=5,
                no_verdict=["swift-testing::lost"], census_runs=2,
                census_lost={"swift-testing::lost": 2}))
            (d / "run.log").write_text(
                log(st_fail_timeout("anchor"), st_silent("lost")) + HOST_RESTART,
                encoding="utf-8")
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                rc = gb.main(["accept", "--log", str(d / "run.log"),
                              "--baseline", str(d / "b.json")])
            out = buffer.getvalue()
        self.assertEqual(0, rc)
        # Crossing arms a hard failure on that name, so it must be named.
        self.assertIn("!~ now deterministic 3/3  swift-testing::lost", out)
        self.assertIn("CENSUS RECORD ARMED: 3 observations recorded", out)
        # playhead-o89d R4. R3 renamed this banner and pinned its SPELLING in the
        # same commit whose rule was "the SPELLING and the EVENT it names" — and
        # pinned the event on the two entry-level promotions only. Measured: a
        # mutant inverting this line to "a test that loses its verdict and IS in
        # the record fails the gate" SURVIVED all 175 tests. It is the sentence
        # that states what the operator is arming, and inverted it arms nothing
        # while claiming the opposite of the rule.
        self.assertIn("is NOT in the record fails the gate", out)
        # playhead-o89d R3. The banner is spelled for its OWN side, and it says
        # which event became fatal. Both were unpinned: a mutant that respelled
        # this banner as the `tests` promotion verbatim — "Each of these PASSING
        # now fails the gate", about a crashed-host name — SURVIVED the suite, as
        # did one that changed `REPORTING AGAIN` here to `PASSES`. For a census
        # entry it is REPORTING AT ALL that is fatal, pass or fail, so an operator
        # who reads `PASSING` concludes a failing report is safe. It is not.
        self.assertIn("  CENSUS ARMED: 1 census entry crossed into DETERMINISTIC", out)
        self.assertIn("REPORTING AGAIN now fails the gate", out)
        self.assertNotIn("PASSING now fails the gate", out)
        # …and a bare `ARMED:` belongs to the failure record, which did not move.
        self.assertNotIn("\n  ARMED:", out)

    def test_the_accept_NAMES_a_census_entry_that_reported_again(self):
        import contextlib
        import io
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            gb.save_baseline(d / "b.json", baseline(
                {"swift-testing::anchor": (5, ["timeout"])}, runs=5,
                no_verdict=["swift-testing::back"], census_runs=2))
            (d / "run.log").write_text(
                log(st_fail_timeout("anchor"), st_pass("back")), encoding="utf-8")
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                rc = gb.main(["accept", "--log", str(d / "run.log"),
                              "--baseline", str(d / "b.json")])
            out = buffer.getvalue()
        self.assertEqual(0, rc)
        self.assertIn("~= reported again         2/3  swift-testing::back", out)
        self.assertIn("UNIONED with counts", out)

    def test_the_accept_NAMES_a_census_entry_that_lost_its_verdict_for_the_FIRST_time(self):
        """playhead-o89d R4, and it is R2's `CENSUS DISARMED:` defect one layer down.

        R2 fixed a census DEMOTION being spelled identically to a load-sensitive
        recurrence — "one line, two opposite meanings". The membership lines under
        it had the same hole and nothing pinned them: a mutant rendering a name
        that has NEVER lost its verdict before in the words of one that has
        (`~= reported again`) survived the whole suite. A first loss is what
        ENTERS the record the census arms on; a recurrence is a name already in
        it. And the transition count was unpinned in the same block, so a record
        that GREW could be printed as one that shrank.
        """
        import contextlib
        import io
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            gb.save_baseline(d / "b.json", baseline(
                {"swift-testing::anchor": (5, ["timeout"])}, runs=5,
                no_verdict=["swift-testing::a"], census_runs=3))
            (d / "run.log").write_text(
                log(st_fail_timeout("anchor"), st_silent("a"), st_silent("b"))
                + HOST_RESTART, encoding="utf-8")
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                rc = gb.main(["accept", "--log", str(d / "run.log"),
                              "--baseline", str(d / "b.json")])
            out = buffer.getvalue()
        self.assertEqual(0, rc)
        self.assertIn("~+ NOW LOSES ITS VERDICT  swift-testing::b", out)
        self.assertNotIn("reported again", out)
        # 1 -> 2, in that order. The direction is the whole claim.
        self.assertIn("crashed-host census: 1 -> 2 name(s) over 4 observation(s)", out)

    def test_the_accept_NAMES_a_census_entry_the_prune_DROPPED(self):
        """playhead-o89d R4. The one census event that SHRINKS the record.

        `merge_census` prunes a recorded name that never started on a HEALTHY
        run, because there it means renamed, deleted or newly skipped. That is
        the direction CLAUDE.md calls unforgivable for `tests`, and its line was
        unpinned: a mutant announcing the prune as `~- recovered (started and
        reported this run)` survived, telling the operator a name came back when
        in fact nobody can reach it any more.
        """
        import contextlib
        import io
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            gb.save_baseline(d / "b.json", baseline(
                {"swift-testing::anchor": (5, ["timeout"])}, runs=5,
                no_verdict=["swift-testing::gone"], census_runs=3))
            (d / "run.log").write_text(log(st_fail_timeout("anchor")),
                                       encoding="utf-8")
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                rc = gb.main(["accept", "--log", str(d / "run.log"),
                              "--baseline", str(d / "b.json")])
            out = buffer.getvalue()
        self.assertEqual(0, rc)
        self.assertIn("~- dropped (never started this run — renamed, deleted or "
                      "skipped)  swift-testing::gone", out)
        self.assertNotIn("recovered", out)
        self.assertIn("crashed-host census: 1 -> 0 name(s)", out)


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

    def _accept(self, base_tests, base_runs, run_log, base=None):
        """`base` overrides the built baseline verbatim — e.g. one with a census."""
        import contextlib
        import io
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            (d / "run.log").write_text(run_log, encoding="utf-8")
            if base is not None:
                gb.save_baseline(d / "b.json", base)
            elif base_tests is not None:
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
        # playhead-o89d R4: …and the accept must not ALSO claim it changed
        # nothing. The two lines contradict each other and only one is read.
        self.assertNotIn("membership unchanged", out)

    def test_the_added_set_is_summarised_by_kind(self):
        rc, out = self._accept({}, 2, log(st_fail_timeout("slow"),
                                          st_fail_expect("wrong")))
        self.assertEqual(0, rc)
        self.assertIn("added 2: 1 assertion, 1 timeout", out)

    def test_a_promotion_is_ANNOUNCED_and_named(self):
        rc, out = self._accept({"swift-testing::x": (2, ["timeout"])}, 2,
                               log(st_fail_timeout("x")))
        self.assertEqual(0, rc)
        self.assertIn("  ARMED:", out)
        self.assertIn("now deterministic [timeout] 3/3  swift-testing::x", out)
        # playhead-o89d R3, the mirror of the `DISARMED` discrimination R2
        # shipped: it is the FAILURE record that moved, and what its promotion
        # makes fatal is a PASS. The census banner says the opposite thing about
        # a different record and must not be able to stand in for this one.
        self.assertIn("PASSING now fails the gate", out)
        self.assertNotIn("CENSUS ARMED", out)
        # playhead-o89d R4. The header's two quantities were interchangeable: a
        # mutant printing the entry count as the observation count and vice versa
        # survived. They are the numerator and denominator of every tier decision
        # in the file, and here they are 3 and 1.
        self.assertIn("observations=3  known-broken=1", out)

    def test_a_promotion_detail_carries_the_ENTRYS_OWN_kind(self):
        """playhead-o89d R4. A promoted entry is not in the `added` set.

        `+ [kind]` is rail-pinned, but it only ever prints for names ENTERING the
        file, and a promotion by construction is a name already in it. So the
        promotion detail is the ONLY place a promoted entry's kind is shown, and
        it was unpinned: a mutant hard-coding `[timeout]` there survived. The
        kind is what the tolerance is built on — "known to time out" does not
        licence "known to fail its expectations" — so an operator justifying an
        accept from a constant is justifying it from nothing.
        """
        rc, out = self._accept({"swift-testing::x": (2, ["assertion"])}, 2,
                               log(st_fail_expect("x")))
        self.assertEqual(0, rc)
        self.assertIn("now deterministic [assertion] 3/3  swift-testing::x", out)
        self.assertNotIn("[timeout]", out)

    def test_an_accept_that_promotes_NOTHING_stays_quiet(self):
        rc, out = self._accept({"swift-testing::x": (1, ["timeout"])}, 1,
                               log(st_fail_timeout("x")))
        self.assertEqual(0, rc)
        self.assertNotIn("ARMED", out)

    def test_a_demotion_is_ANNOUNCED_and_named(self):
        """playhead-o89d review. The direction that makes the gate LOOSER.

        A `tests` demotion has one cause — a DETERMINISTIC entry passed, which
        hard-failed the gate — and accepting it revokes the licence that made
        that pass fatal. It used to print four bare words, and the accept it
        first fired on was written up in its commit message as "the
        pass-direction arm doing its job on a LOAD-SENSITIVE entry: reported,
        not fatal": the AFTER tier read as though it were the BEFORE tier, on
        an event that was fatal. The line has to name the tier being left.
        """
        rc, out = self._accept({"swift-testing::x": (3, ["timeout"])}, 3,
                               log(st_pass("x"), st_fail_timeout("other")))
        self.assertEqual(0, rc)
        self.assertIn("  DISARMED:", out)
        self.assertIn("LEFT DETERMINISTIC", out)
        # The counts, so "why the record was wrong" is checkable from the line.
        self.assertIn("no longer deterministic [timeout] 3/4  swift-testing::x", out)
        # …and it is the FAILURE record that moved, not a crashed-host name.
        self.assertNotIn("CENSUS DISARMED", out)
        # playhead-o89d R4. R3's rule for a tier banner was "the SPELLING and the
        # EVENT it names", and it applied that to the two PROMOTIONS only. Both
        # demotion banners still stated their event and their consequence in
        # unpinned prose: mutants that made this one say the licence is KEPT and
        # the next pass STILL fatal, or that named the CENSUS event (`REPORTS
        # AGAIN`) as what hard-failed the gate, both survived. A demotion is the
        # direction that makes the gate LOOSER, which this test's own docstring
        # says is the one that has to be justified out loud.
        self.assertIn("hard-failed the gate (`NOW PASSES`)", out)
        self.assertIn("revokes that licence", out)
        self.assertIn("next pass is NOT fatal", out)
        self.assertNotIn("REPORTS AGAIN", out)

    def test_an_accept_that_demotes_NOTHING_stays_quiet(self):
        rc, out = self._accept({"swift-testing::x": (1, ["timeout"])}, 1,
                               log(st_fail_timeout("x")))
        self.assertEqual(0, rc)
        self.assertNotIn("DISARMED", out)

    def test_a_CENSUS_demotion_is_ANNOUNCED_and_named(self):
        """playhead-o89d R2. R1 fixed this asymmetry one layer up and left it here.

        A census demotion has exactly one cause, and it is the census twin of
        the `tests` one: the name was recorded as losing its verdict in EVERY
        observation, this run watched it start and REPORT, and that report is
        what hard-failed the gate. Accepting revokes the licence.

        Before this rail the event printed `~= reported again 3/4` — spelled
        identically to a LOAD-SENSITIVE casualty coming back, where coming back
        is good news and costs nothing. One line, two opposite meanings, tier
        left to the reader's arithmetic.
        """
        base = baseline({}, no_verdict=["swift-testing::lost"], census_runs=3)
        rc, out = self._accept(None, None,
                               log(st_pass("lost"), st_fail_timeout("other")),
                               base=base)
        self.assertEqual(0, rc)
        self.assertIn("CENSUS DISARMED:", out)
        self.assertIn("LEFT DETERMINISTIC", out)
        self.assertIn("no longer deterministic 3/4  swift-testing::lost", out)
        # Spelled for its own side: a bare `DISARMED:` means a recorded FAILURE
        # stopped being deterministic, and nothing here is one.
        self.assertNotIn("  DISARMED:", out)
        # playhead-o89d R4, the census half of the same hole: the SPELLING was
        # pinned and the CONSEQUENCE was not. A mutant saying accepting KEEPS the
        # licence and the next report is STILL fatal survived — which tells the
        # operator the record did not loosen, on the only event that loosens it
        # and the only way this record ever shrinks.
        self.assertIn("Accepting revokes that licence", out)
        self.assertIn("its next report is NOT fatal", out)
        self.assertNotIn("STILL fatal", out)

    def test_an_accept_that_demotes_NO_CENSUS_ENTRY_stays_quiet(self):
        """The load-sensitive casualty coming back is the case that must NOT shout."""
        base = baseline({}, no_verdict=["swift-testing::lost"], census_runs=3,
                        census_lost={"swift-testing::lost": 2})
        rc, out = self._accept(None, None,
                               log(st_pass("lost"), st_fail_timeout("other")),
                               base=base)
        self.assertEqual(0, rc)
        # Still ledgered, still with the counts that place it in its tier — the
        # banner is what a demotion adds, not what it replaces.
        self.assertIn("reported again         2/4  swift-testing::lost", out)
        self.assertNotIn("DISARMED", out)

    def test_an_accept_over_a_crashed_run_SAYS_SO(self):
        # playhead-tl6l. An accept is a claim a human signs in a commit message,
        # and "this observation says nothing about N tests" is part of it.
        rc, out = self._accept({"swift-testing::x": (1, ["timeout"])}, 1,
                               log(st_fail_timeout("x"), st_silent("lost"))
                               + HOST_RESTART)
        self.assertEqual(0, rc)
        self.assertIn("NO VERDICT", out)
        self.assertIn("restarted the test host", out)
        # playhead-o89d R4. THE COUNT, not just the word. CLAUDE.md records this
        # number being wrong three times, every one of them a value that named
        # one thing read as though it named another — and here the accept path's
        # headline could be made to count the CARRIED-FORWARD entries instead of
        # the casualties (a mutant swapping them survived). Nothing recorded is
        # protected in this scenario, so that mutant reads "the host died and 0
        # test(s) reported nothing" on a run that lost one.
        self.assertIn("the host died and 1 test(s) reported nothing", out)
        # …and the census is announced as PROVISIONAL, which is the state that
        # makes an unrecorded casualty reportable-but-not-fatal. Both the banner
        # and its licence were unpinned; a mutant declaring it already fatal
        # survived, and it is the sentence a first accept is signed against.
        self.assertIn("CENSUS NOW LIVE: 1 crashed-host name(s)", out)
        self.assertIn("It takes 3 before an unrecorded casualty can fail the gate", out)
        self.assertIn("they are named and not fatal", out)

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


class TruncatedOutcomeLineTests(unittest.TestCase):
    """A verdict line whose tail was overwritten is STILL a verdict (R1 review).

    xcodebuild interleaves the app's stdout into the runner's, and it does it
    MID-TOKEN. The real 2026-08-12 main log carries, on one line:

        ✔ Test "Detects 'risk free'" passed after 100.732 secon2026-08-12
        17:02:37.680439-0400 Playhead[93019:53604390] [EvidenceCatalogBuilder] …

    Nothing marks it as truncated. A parser anchored on the trailing word
    `seconds` therefore scored a test that PASSED as one that started and said
    nothing — three of them on main, one on mn5e, which is the whole difference
    between the census as first reported (33/15) and as measured (30/14).

    These rails are synthetic on purpose. The two preserved 7.2 MB logs live in
    a session-specific scratchpad and `skipTest` when it is gone; this defect
    must stay pinned after they do.
    """

    SPLICE = ("2026-08-12 17:02:37.680439-0400 Playhead[93019:53604390] "
              "[EvidenceCatalogBuilder] Built evidence catalog: 3 entries\n")

    def test_a_truncated_PASS_line_is_read_as_a_PASS(self):
        run = gb.parse_run(log(
            '◇ Test "victim" started.\n'
            '✔ Test "victim" passed after 100.732 secon' + self.SPLICE,
            terminal=TERMINAL_PASSED,
        ))
        self.assertIn(gb.st_key("victim"), run.passed)
        self.assertEqual(set(), run.no_verdict)

    def test_a_pass_truncated_before_the_DURATION_is_still_a_pass(self):
        run = gb.parse_run(log(
            '◇ Test "victim" started.\n'
            '✔ Test "victim" passed after' + self.SPLICE,
            terminal=TERMINAL_PASSED,
        ))
        self.assertIn(gb.st_key("victim"), run.passed)
        self.assertEqual(set(), run.no_verdict)

    def test_a_truncated_PASS_does_not_forecloses_GREEN(self):
        # The consequence, not just the parse: a phantom casualty makes GREEN
        # unreachable on a run where nothing whatsoever went wrong.
        run = gb.parse_run(log(
            '◇ Test "victim" started.\n'
            '✔ Test "victim" passed after 1.0 secon' + self.SPLICE,
            terminal=TERMINAL_PASSED,
        ))
        v = gb.verdict(gb.empty_baseline("PlayheadFastTests"), run,
                       plan="PlayheadFastTests")
        self.assertFalse(v.crashed_host)
        self.assertIn("GREEN", v.render())

    def test_a_truncated_FAIL_line_is_still_a_FAILURE(self):
        # The worse direction, and the reason the fix is not scoped to passes:
        # a lost failure reads as a crash casualty, which is NOT fatal.
        #
        # The ISSUE line is truncated too, deliberately. With it intact this
        # rail is VACUOUS — `recorded an issue` alone puts the key in
        # `failures`, so the strict pattern passes the test without parsing the
        # `failed after` line at all. R1's own battery caught that. Here the
        # terminal line is the only surviving evidence of the failure.
        run = gb.parse_run(log(
            '◇ Test "victim" started.\n'
            '✘ Test "victim" recorded an iss' + self.SPLICE +
            '✘ Test "victim" failed after 0.03 secon' + self.SPLICE,
        ))
        self.assertIn(gb.st_key("victim"), run.failures)
        self.assertEqual(set(), run.no_verdict)

    def test_a_truncated_XCTest_result_is_still_an_outcome(self):
        run = gb.parse_run(log(
            "Test Case '-[PlayheadTests.S testFoo]' started.\n"
            "Test Case '-[PlayheadTests.S testFoo]' failed (0.02" + self.SPLICE,
        ))
        self.assertIn(gb.xc_key("PlayheadTests.S", "testFoo"), run.failures)
        self.assertEqual(set(), run.no_verdict)

    def test_an_INTACT_verdict_followed_by_app_output_is_NOT_rewritten(self):
        # The splice repair must never touch a line that already reads as a
        # verdict. Here the following line is xcodebuild's own restart banner:
        # swallow it into the pass line and the restart handler claims the whole
        # line first, so a test that PASSED becomes a crashed-host casualty AND
        # the restart is double-reported. Two wrong numbers from one greedy join.
        run = gb.parse_run(log(
            '◇ Test "victim" started.\n'
            '✔ Test "victim" passed after 1.0 seconds.' + self.SPLICE +
            "Restarting after unexpected exit, crash, or test timeout; summary "
            "will include totals from previous launches.\n",
            terminal=TERMINAL_PASSED,
        ))
        self.assertIn(gb.st_key("victim"), run.passed)
        self.assertEqual(set(), run.no_verdict)
        self.assertEqual(1, run.host_restarts)

    def test_the_DISPLACED_app_output_is_kept_and_still_scanned(self):
        # The repair rewrites two lines into two lines; it must not DROP the
        # bytes the intrusion carried. If xcodebuild's restart banner landed
        # after the intrusion on the same physical line, discarding the tail
        # would discard the crash's own most direct witness.
        run = gb.parse_run(log(
            '◇ Test "victim" started.\n'
            '✔ Test "vic' + self.SPLICE.rstrip("\n")
            + " Restarting after unexpected exit, crash, or test timeout\n"
            + 'tim" passed after 1.0 seconds.\n',
            terminal=TERMINAL_PASSED,
        ))
        self.assertIn(gb.st_key("victim"), run.passed)
        self.assertEqual(1, run.host_restarts)

    def test_the_reconstruction_is_HEAD_then_TAIL_and_not_the_other_way(self):
        run = gb.parse_run(log(
            '◇ Test "victim tested twice" started.\n'
            '✔ Test "victim tes' + self.SPLICE + 'ted twice" passed after 1.0 seconds.\n',
            terminal=TERMINAL_PASSED,
        ))
        self.assertIn(gb.st_key("victim tested twice"), run.passed)
        self.assertEqual(set(), run.no_verdict)

    def test_an_INTACT_line_still_yields_its_duration(self):
        # Widening the pattern must not quietly drop the duration it still has.
        run = gb.parse_run(log(st_fail_timeout("slow", seconds="149.751")))
        self.assertEqual(149.751, run.failures[gb.st_key("slow")].seconds)
        run = gb.parse_run(log(xc_fail("S", "testFoo", seconds="0.025")))
        self.assertEqual(0.025, run.failures[gb.xc_key("PlayheadTests.S",
                                                       "testFoo")].seconds)


class CrashedHostSafetyPropertyTests(unittest.TestCase):
    """The properties tl6l's exit-code decision RESTS on (R1 review).

    NO VERDICT is reportable-not-fatal only because the headline carries it,
    GREEN is foreclosed, an ABSENT baseline member still fails, and `accept`
    carries forward. Three of those were true in the code and asserted NOWHERE:
    deleting `no_verdict` from `crashed_host`, deleting `absent` from the
    exit-code arm, or rendering lost tests under NEW FAILURE each left all 117
    tests green. An unpinned safety property is one edit from being gone.
    """

    def test_NO_VERDICT_alone_forecloses_GREEN(self):
        # No restart marker and no summary block: the census is the ONLY
        # evidence, which is exactly the case a real hang produces.
        run = gb.parse_run(log(st_pass("fine"), st_silent("ghost"),
                               terminal=TERMINAL_PASSED))
        self.assertEqual(0, run.host_restarts)
        v = gb.verdict(gb.empty_baseline("PlayheadFastTests"), run,
                       plan="PlayheadFastTests")
        self.assertEqual([], v.blamed_unmatched)
        self.assertTrue(v.crashed_host)
        rendered = v.render()
        self.assertNotIn("GREEN", rendered)
        self.assertIn("RED", rendered)
        self.assertIn("1 test got NO VERDICT", rendered)

    def test_an_ABSENT_baseline_member_makes_the_gate_EXIT_NONZERO(self):
        # Constructed so ABSENT is the ONLY reason: the run carries a real
        # known failure, so `baseline_fiction` cannot be what turns it red.
        base = baseline({
            "swift-testing::known": (3, ["timeout"]),
            "swift-testing::gone": (3, ["timeout"]),
        })
        run = gb.parse_run(log(st_fail_timeout("known"), st_pass("other")))
        v = gb.verdict(base, run, plan="PlayheadFastTests")
        self.assertEqual(["swift-testing::gone"], v.absent)
        self.assertEqual([], v.new_failures)
        self.assertEqual([], v.kind_changed)
        self.assertEqual([], v.deterministic_passed)
        self.assertFalse(v.baseline_fiction)
        self.assertEqual(gb.EXIT_REGRESSION, v.exit_code)

    def test_a_baseline_member_lost_to_the_CRASH_is_still_fatal(self):
        # The same arm, on the population the bead is about: it is reported with
        # the crash as its cause AND it still exits non-zero.
        base = baseline({
            "swift-testing::known": (3, ["timeout"]),
            "swift-testing::gone": (3, ["timeout"]),
        })
        run = gb.parse_run(log(st_fail_timeout("known"), st_silent("gone"),
                               HOST_RESTART))
        v = gb.verdict(base, run, plan="PlayheadFastTests")
        self.assertIn("swift-testing::gone", v.absent_crashed)
        self.assertEqual(gb.EXIT_REGRESSION, v.exit_code)
        self.assertIn("no verdict — the host died mid-test", v.render())

    def test_a_lost_test_is_NEVER_rendered_under_NEW_FAILURE(self):
        # Its remedy is "re-run", not "triage against your diff". Folding it in
        # would send the reader looking for a change that caused nothing.
        run = gb.parse_run(log(st_pass("fine"), st_silent("ghost"), HOST_RESTART))
        v = gb.verdict(gb.empty_baseline("PlayheadFastTests"), run,
                       plan="PlayheadFastTests")
        self.assertEqual(["swift-testing::ghost"], v.no_verdict)
        new_lines = [ln for ln in v.render().splitlines() if "NEW FAILURE" in ln]
        self.assertEqual([], new_lines)
        self.assertIn("  NO VERDICT       swift-testing::ghost", v.render())

    def test_the_headline_names_an_unmatched_BLAMED_name_on_its_own(self):
        # The third arm of `headline_tail`: nothing started silently and no
        # restart was printed, so the block is the only evidence there is.
        run = gb.parse_run(
            log(st_pass("fine"), terminal=TERMINAL_PASSED)
            + failing_block("GhostTests.neverPrintedALine()")
        )
        self.assertEqual(set(), run.no_verdict)
        self.assertEqual(0, run.host_restarts)
        v = gb.verdict(gb.empty_baseline("PlayheadFastTests"), run,
                       plan="PlayheadFastTests")
        first = v.render().splitlines()[0]
        self.assertIn("matched no console result", first)
        self.assertNotIn("GREEN", first)

    def test_accept_CARRIES_FORWARD_across_a_SECOND_and_THIRD_crash(self):
        # The subtlest claim: one carry-forward is not enough, because the file
        # is maintained by repeated accepts. A member lost to the crash must
        # still be at its ORIGINAL counts after three of them, and must still be
        # pruned the moment a healthy run genuinely fails to reach it.
        base = baseline(
            {"swift-testing::lost": (5, ["timeout"]),
             "swift-testing::a": (5, ["timeout"]),
             "swift-testing::b": (5, ["timeout"]),
             "swift-testing::c": (5, ["timeout"])},
            runs=5,
        )
        crashed = log(st_silent("lost"), st_fail_timeout("a"),
                      st_fail_timeout("b"), st_fail_timeout("c"), HOST_RESTART)
        merged = base
        for _ in range(3):
            merged = gb.merge(merged, gb.parse_run(crashed),
                              plan="PlayheadFastTests")
            entry = merged["tests"]["swift-testing::lost"]
            self.assertEqual(5, entry["seen_runs"])
            self.assertEqual(5, entry["failed_runs"])
        self.assertEqual(8, merged["runs_observed"])
        # …and it is NOT immortal: a clean run that does not reach it prunes it.
        clean = log(st_fail_timeout("a"), st_fail_timeout("b"),
                    st_fail_timeout("c"))
        final = gb.merge(merged, gb.parse_run(clean), plan="PlayheadFastTests")
        self.assertNotIn("swift-testing::lost", final["tests"])

    def test_a_crash_that_swallows_MOST_of_the_baseline_REFUSES_to_accept(self):
        # Carrying forward is the remedy for a few lost members. When the crash
        # took down more than half, `merge` refuses outright rather than write
        # a file built mostly from carry-forwards.
        base = baseline(
            {"swift-testing::l1": (5, ["timeout"]),
             "swift-testing::l2": (5, ["timeout"]),
             "swift-testing::l3": (5, ["timeout"]),
             "swift-testing::kept": (5, ["timeout"])},
            runs=5,
        )
        crashed = log(st_silent("l1"), st_silent("l2"), st_silent("l3"),
                      st_fail_timeout("kept"), HOST_RESTART)
        with self.assertRaises(gb.CannotEvaluate) as caught:
            gb.merge(base, gb.parse_run(crashed), plan="PlayheadFastTests")
        self.assertIn("reached only", str(caught.exception))


FIXTURES = ROOT / "scripts" / "tests" / "fixtures"

# THE BASELINE THESE RAILS RUN AGAINST IS FROZEN, NOT THE LIVE ONE (playhead-o89d).
#
# They used to load `scripts/gate-baseline.PlayheadFastTests.json` and assert exact
# counts off it — 85 known, 14 new, a census of exactly two observations, `absent`
# empty. Every one of those is a property of the file's CONTENTS on the day the
# rails were written, and the file's whole purpose is to change: a documented,
# sanctioned `--accept-baseline` moved it from 117 entries to 121 and turned five
# of these rails red without touching a line of `gate_baseline.py`.
#
# The worst of the five was `test_the_fixture_still_agrees_with_the_FULL_LOG_it_came_from`,
# and it shows why freezing is the right fix rather than re-typing the numbers.
# The distilled fixtures drop uninteresting start/pass PAIRS whole; a name that is
# not a baseline member on distillation day therefore has no lines to keep. Admit
# that name to the baseline later and the fixture reports it ABSENT while the full
# log reports it passing — the distillation's "every quantity comes out identical"
# invariant is broken by an edit to a DIFFERENT file. Re-typing 85 as 83 would buy
# one accept's worth of green.
#
# So: the rails assert things about the ENGINE reading a real crashed run, and they
# get a baseline that never moves. Regenerate this only alongside the fixtures
# themselves (scripts/tests/make_crashed_run_fixture.py), never to chase an accept.
RAILS_BASELINE = FIXTURES / "gate-baseline.crashed-run-rails.json"

# The full 7.2 MB logs, when whoever is running this still has them. Optional by
# design — the committed fixtures are the rails, and these are a bonus check
# that the distillation did not drift from its source.
FULL_LOG_DIR = pathlib.Path(
    os.environ.get(
        "PLAYHEAD_GATE_LOG_DIR",
        "/private/tmp/claude-501/-Users-dabrams-playhead/"
        "bee44b12-4e4b-4bbd-ada7-8d211ef3d4e6/scratchpad",
    )
)

# fixture -> (full log, no_verdict, blamed entries, blamed distinct, known, new,
#             host restarts)
REAL_RUNS = {
    "crashed-run-main-76b0a09a.log": (
        "main-control/main-fullplan.log", 11, 18, 14, 85, 14, 1),
    "crashed-run-mn5e.log": (
        "mn5e-r6/fullplan.log", 11, 19, 15, 71, 16, 1),
    # playhead-tl6l R4. The THIRD observation, and the only one that could have
    # shown the record's shape was wrong: a real full-plan gate on the armed
    # code, 2026-08-12 23:10. Fifteen casualties where the first two runs had
    # eleven — the same eleven, plus four that had PASSED in both.
    "crashed-run-tl6l-realgate.log": (
        "tl6l-realgate/realgate.log", 15, 14, 12, 68, 6, 2),
}


class RealCrashedRunTests(unittest.TestCase):
    """The two 2026-08-12 full-plan runs, playhead-tl6l / playhead-fer3.

    Everything else in this file is synthetic. These are the actual runs the
    bead was filed from — both with a dead test host — and they are the only
    rails that prove the module reads a REAL crash rather than a fixture built
    to match its own parser.

    THEY USED TO SKIP THEMSELVES, which is the reason playhead-fer3 exists.
    They pointed at a session-specific scratchpad and called `skipTest` when it
    was gone, so the file's highest-value rails were guaranteed to disappear and
    the suite reported `OK` when they did. A rail that reports OK without
    running is the same defect class as a gate reporting an all-clear for a run
    that lost part of the plan — the exact thing this module was written to
    stop, one layer up.

    What is committed instead is a DISTILLATION: every failure, issue, skip,
    splice and casualty line kept byte-exact, every uninteresting start/pass
    PAIR dropped whole so the arithmetic is untouched. 7.2 MB becomes 68 KB and
    every quantity `gate_baseline.py` computes comes out identical — census,
    failures, known, new, absent, kind changes, both pass-direction lists and
    the exit code. `scripts/tests/make_crashed_run_fixture.py` regenerates them
    and REFUSES to write a fixture whose numbers drift.

    A MISSING FIXTURE IS A FAILURE, NOT A SKIP. That is the whole point.
    """

    def _run(self, fixture):
        path = FIXTURES / fixture
        self.assertTrue(
            path.exists(),
            "the distilled crashed-run fixture is missing: %s\n"
            "It is COMMITTED — regenerate with "
            "scripts/tests/make_crashed_run_fixture.py <full.log> %s.\n"
            "This rail fails rather than skips because a real-data rail that "
            "reports OK without running is the defect this module exists to "
            "stop." % (path, path),
        )
        return gb.parse_run(path.read_text(encoding="utf-8"))

    def _check(self, fixture):
        _, expected, entries, distinct, known, new, restarts = REAL_RUNS[fixture]
        run = self._run(fixture)
        self.assertTrue(run.complete)
        self.assertEqual(restarts, run.host_restarts)
        self.assertEqual(expected, len(run.no_verdict))
        self.assertEqual(entries, len(run.blamed_entries))
        self.assertEqual(distinct, len(run.blamed))
        base = gb.load_baseline(RAILS_BASELINE)
        v = gb.verdict(base, run, plan="PlayheadFastTests")
        first = v.render().splitlines()[0]
        self.assertIn("NO VERDICT", first)
        self.assertIn("crashed host", first)
        self.assertEqual(known, len(v.known_failures))
        self.assertEqual(new, len(v.new_failures))
        return run, v

    def test_the_mn5e_branch_run(self):
        # 11 lost, and 19 summary entries of which 4 are retries of a name
        # already listed. This number has been wrong three times — 19, then 15,
        # then 14 — each time by counting healthy tests as casualties. See
        # test_the_spliced_verdict_lines_are_read_as_PASSES for the last one.
        run, v = self._check("crashed-run-mn5e.log")
        # None of the lost tests leaked into either side of the arithmetic.
        self.assertFalse(set(v.no_verdict) & set(v.new_failures))
        self.assertFalse(set(v.no_verdict) & set(v.known_failures))

    def test_the_main_control_run(self):
        run, v = self._check("crashed-run-main-76b0a09a.log")
        # The one genuine casualty that never even printed a `started` line, so
        # only the summary block can see it.
        self.assertIn(
            "SkipOrchestratorRevertTests."
            "failedSuggestNoRestoresLatestBufferedRevision()",
            v.blamed_unmatched,
        )

    def test_BOTH_runs_lost_THE_SAME_ELEVEN_TESTS(self):
        """The measurement R2's design rested on, kept because it still holds.

        Two full-plan runs, DIFFERENT trees, and the casualty sets are
        identical — Jaccard 1.00, against 0.46 for the failure set on identical
        code. Every one of the eleven is a download/cache/streaming test, the
        same family xcodebuild's own `Failing tests:` block names.

        R2 read this as licence to record a SET and REPLACE it on each accept,
        and wrote here that a third result would reopen the union question. It
        did — see the next rail. What survives unchanged is the other half of
        R2's argument: the record is a set of NAMES rather than a count,
        because a count cannot see eleven tests dying while eleven different
        ones recover.
        """
        sets = [set(self._run(name).no_verdict)
                for name in ("crashed-run-main-76b0a09a.log",
                             "crashed-run-mn5e.log")]
        self.assertEqual(sets[0], sets[1])
        self.assertEqual(11, len(sets[0]))

    def test_the_THIRD_run_REOPENED_the_union_question_and_ANSWERED_it(self):
        """playhead-tl6l R4 — why the record is a union with counts.

        Two observations put the churn at zero. The third, a real full-plan
        gate on the armed code, lost FIFTEEN: the same eleven yet again, plus
        four that had PASSED in both earlier runs. Jaccard falls 1.00 -> 0.73,
        nothing has EVER recovered, and the eleven are now a deterministic core
        measured across three different trees.

        Under a replaced set an armed gate on a night like that reports four
        NEW casualties and exits 65 — red for a reason its reader cannot fix,
        which is the hazard R2's own exit-code argument was built to avoid.
        The counts say instead that eleven names are 3/3 and the four enter at
        1/1, and that is the distinction the tiers already know how to express.
        """
        runs = {name: self._run(name) for name in REAL_RUNS}
        sets = {name: set(run.no_verdict) for name, run in runs.items()}
        third = sets["crashed-run-tl6l-realgate.log"]
        first = sets["crashed-run-main-76b0a09a.log"]
        second = sets["crashed-run-mn5e.log"]
        self.assertEqual(15, len(third))
        core = first & second & third
        self.assertEqual(11, len(core))
        self.assertEqual(4, len(third - core))
        # Nothing has ever recovered: the core is present in all three.
        self.assertEqual(set(), (first | second) - third)
        # …and the four are casualties in NEITHER earlier run. That they
        # actively PASSED in both — the fact that makes them load-sensitive
        # rather than newly-added tests — is measured on the 7.2 MB originals
        # in test_the_four_CHURN_NAMES_passed_in_the_earlier_runs below. The
        # distillation cannot carry it: run 1 and run 2 drop the start/pass
        # PAIR of every test that is uninteresting IN THAT RUN, and these four
        # were uninteresting in both.
        for key in third - core:
            for name in ("crashed-run-main-76b0a09a.log", "crashed-run-mn5e.log"):
                self.assertNotIn(key, runs[name].no_verdict,
                                 "%s / %s" % (name, key))

    def test_the_THREE_runs_PROMOTE_ELEVEN_and_leave_FOUR_load_sensitive(self):
        """The tier split, end to end on real data, through `merge` itself.

        This is the rail that would go red if the counts stopped meaning what
        they say: three accepts of the three real runs must leave exactly the
        eleven deterministic and the four load-sensitive.
        """
        base = gb.load_baseline(RAILS_BASELINE)
        merged = base
        for name in ("crashed-run-main-76b0a09a.log", "crashed-run-mn5e.log",
                     "crashed-run-tl6l-realgate.log"):
            merged = gb.merge(merged, self._run(name), plan="PlayheadFastTests")
        census = gb.recorded_census(merged)
        self.assertEqual(3, census.runs_observed)
        self.assertTrue(census.armed)
        tiers = {}
        for key in census.tests:
            tiers.setdefault(census.tier(key), []).append(key)
        self.assertEqual(11, len(tiers[gb.TIER_DETERMINISTIC]))
        self.assertEqual(4, len(tiers[gb.TIER_LOAD_SENSITIVE]))
        for key in tiers[gb.TIER_DETERMINISTIC]:
            self.assertEqual({"seen_runs": 3, "lost_runs": 3}, census.tests[key])
        # ONE OBSERVATION, not one of three: the earlier runs reached these
        # four and watched them pass, but there was no entry to credit. Pinned
        # because "1" here and "1 of 3" in the prose are different quantities
        # that read the same, which is this file's standing defect class.
        for key in tiers[gb.TIER_LOAD_SENSITIVE]:
            self.assertEqual({"seen_runs": 1, "lost_runs": 1}, census.tests[key])

    def test_the_THIRD_run_would_NOT_have_failed_the_gate_on_a_TWO_RUN_record(self):
        """The defect this round exists to fix, checked against the run itself.

        The record R2's design would have held on the night of run 3 is the one
        built by accepting runs 1 and 2 — eleven names, two observations. The
        `tests` half is taken from run 3 so that NEW FAILURES cannot be what
        turns it red; the census is then the only thing that can, which is the
        vacuity trap R3 fell into and named.
        """
        base = gb.load_baseline(RAILS_BASELINE)
        third = self._run("crashed-run-tl6l-realgate.log")
        # Failures accepted from run 3 itself -> zero NEW failures, zero absent.
        tests_side = gb.merge(base, third, plan="PlayheadFastTests")
        # Census from runs 1 and 2 only.
        census_side = base
        for name in ("crashed-run-main-76b0a09a.log", "crashed-run-mn5e.log"):
            census_side = gb.merge(census_side, self._run(name),
                                   plan="PlayheadFastTests")
        probe = dict(tests_side)
        probe[gb.NO_VERDICT_KEY] = census_side[gb.NO_VERDICT_KEY]
        v = gb.verdict(probe, third, plan="PlayheadFastTests")
        self.assertEqual([], v.new_failures)
        self.assertEqual([], v.absent)
        self.assertFalse(v.baseline_fiction)
        self.assertEqual(2, v.census_runs_observed)
        self.assertFalse(v.census_armed)
        self.assertEqual(4, len(v.new_casualties))
        self.assertEqual(gb.EXIT_OK, v.exit_code)
        # Named, though — silence is not the remedy, waiting is.
        rendered = v.render()
        self.assertIn("PROVISIONAL", rendered)
        for key in v.new_casualties:
            self.assertIn("NEW CASUALTY     %s" % key, rendered)

    def test_the_skips_are_subtracted_and_it_MATTERS(self):
        # Without parsing skips the census is 30 higher on both runs, and every
        # one of the 30 is an XCTest PerfGate skip.
        for fixture in REAL_RUNS:
            run = self._run(fixture)
            naive = run.started - run.ran
            self.assertEqual(30, len({k for k in naive - run.no_verdict
                                      if k.startswith(gb.FRAMEWORK_XCTEST)}))

    def test_the_spliced_verdict_lines_are_read_as_PASSES(self):
        """The exact lines that inflated the census, by name — R1 and R2.

        Not synthetic: xcodebuild spliced an app log line into the middle of a
        verdict, and a parser that read a line at a time scored the passing test
        as a crashed-host casualty. The first four are R1's — the cut landed
        after ` after`, so widening the pattern was enough. The rest are R2's:
        the cut landed inside the NAME, or inside the verb, or inside the word
        `Test` itself, and no pattern over `Test "` can ever see those. They are
        why the census is 11 and 11 rather than 30 and 14.
        """
        cases = {
            "crashed-run-main-76b0a09a.log": [
                # cut after ` after` — R1's four
                "Detects 'risk free'",
                "a clean 12s music run yields exactly one span ending at the "
                "music→speech drop",
                "apportionment is robust to extreme word-length variation",
                # cut immediately after the verb
                "Multiple events loadable by analysisAssetId",
                "fast transcript chunks override stale transcript watermark",
                # cut inside the NAME
                "a byte-exact span outside the 90s window is eligible for the "
                "ORIGINAL reason too",
                "flag OFF is byte-identical for both the exempt and the guarded "
                "shape",
                "Cycle 4 B4 M: 3 ad-free observations ⇒ "
                "episodesObservedWithoutSampleCount == 3",
            ],
            "crashed-run-mn5e.log": [
                "a kickoff that FIRED increments only `firedCount` — a success "
                "is never counted as a give-up",
                "shadowConfidence outside [0, 1] is clamped defensively",
                # cut inside the word `Test` — `✔ Tes` + `t "watermark …`
                "watermark within one shard past the duration is tolerated; "
                "beyond it is not",
            ],
        }
        for fixture, names in cases.items():
            run = self._run(fixture)
            for name in names:
                key = gb.st_key(name)
                self.assertIn(key, run.passed, "%s: %r" % (fixture, name))
                self.assertNotIn(key, run.no_verdict, "%s: %r" % (fixture, name))

    def test_the_old_code_saw_NONE_of_this(self):
        # The regression rail proper: before playhead-tl6l the verdict line was
        # `RED (85 known / 14 NEW)` with no mention of a crash at all.
        run = self._run("crashed-run-main-76b0a09a.log")
        self.assertGreater(len(run.no_verdict), 0)
        self.assertGreater(len(run.blamed_entries), 0)
        self.assertGreater(run.host_restarts, 0)

    def test_the_fixture_still_agrees_with_the_FULL_LOG_it_came_from(self):
        """Only rail here allowed to skip — and it can, because it adds nothing
        the committed fixtures do not already assert. It exists so that whoever
        still has the 7.2 MB originals can prove the distillation has not
        drifted from them; without them, the fixtures above are the rails.
        """
        checked = 0
        for fixture, (relative, *_rest) in REAL_RUNS.items():
            full = FULL_LOG_DIR / relative
            if not full.exists():
                continue
            base = gb.load_baseline(RAILS_BASELINE)
            source = gb.parse_run(full.read_text(encoding="utf-8", errors="replace"))
            distilled = self._run(fixture)
            self.assertEqual(set(source.no_verdict), set(distilled.no_verdict), fixture)
            self.assertEqual(set(source.failures), set(distilled.failures), fixture)
            self.assertEqual(source.blamed_entries, distilled.blamed_entries, fixture)
            one = gb.verdict(base, source, plan="PlayheadFastTests")
            two = gb.verdict(base, distilled, plan="PlayheadFastTests")
            self.assertEqual(one.absent, two.absent, fixture)
            self.assertEqual(one.load_sensitive_passed, two.load_sensitive_passed,
                             fixture)
            self.assertEqual(one.exit_code, two.exit_code, fixture)
            checked += 1
        if not checked:
            self.skipTest(
                "the 7.2 MB source logs are not on this machine (set "
                "PLAYHEAD_GATE_LOG_DIR if they are elsewhere). The committed "
                "fixtures above still ran."
            )

    def test_the_four_CHURN_NAMES_passed_in_the_earlier_runs(self):
        """Load-sensitive, not newly-added — and only the full logs can say so.

        The tier split rests on the four having been REACHED in runs 1 and 2,
        because a name that merely did not exist yet would also enter the
        record at 1/1. Measured 2026-08-13 on all three originals: every one of
        the four is in `passed` on BOTH earlier runs. The distilled fixtures
        drop those pass lines by construction — a test uninteresting in the run
        being distilled loses its start/pass pair whole, which is where all the
        size goes — so this is the one claim that has to live here. It skips
        for the same reason its neighbour does, and it is the only claim the
        committed rails do not otherwise carry.
        """
        available = {name: FULL_LOG_DIR / relative
                     for name, (relative, *_r) in REAL_RUNS.items()
                     if (FULL_LOG_DIR / relative).exists()}
        if len(available) != len(REAL_RUNS):
            self.skipTest("the full-plan source logs are not all on this machine")
        runs = {name: gb.parse_run(path.read_text(encoding="utf-8", errors="replace"))
                for name, path in available.items()}
        third = set(runs["crashed-run-tl6l-realgate.log"].no_verdict)
        earlier = ("crashed-run-main-76b0a09a.log", "crashed-run-mn5e.log")
        core = third.intersection(*[set(runs[n].no_verdict) for n in earlier])
        self.assertEqual(11, len(core))
        churn = sorted(third - core)
        self.assertEqual(4, len(churn))
        for key in churn:
            for name in earlier:
                self.assertIn(key, runs[name].passed, "%s / %s" % (name, key))


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
                # THESE FOUR RAN ON NOTHING until playhead-tl6l R4. R2 added the
                # census block below and indented them into it, so every
                # per-entry invariant on the file the gate actually reads was
                # conditional on a key no committed baseline has ever carried —
                # and `entry`/`key` were whatever the loops above had leaked.
                # They hold on the committed file; nothing was checking that.
                self.assertGreaterEqual(entry["failed_runs"], 1, key)
                self.assertLessEqual(entry["seen_runs"], data["runs_observed"], key)
                self.assertTrue(entry["kinds"], key)
                for kind in entry["kinds"]:
                    self.assertIn(kind, (gb.KIND_TIMEOUT, gb.KIND_ASSERTION,
                                         gb.KIND_UNKNOWN), key)
            # playhead-buvn. The crashed-host census is ARMED, so a hand-edit
            # that spells it wrong silently disarms it — `{}` or `null` both
            # read as "never recorded" and the gate goes quiet about the one
            # regression class only this arm can see.
            if gb.NO_VERDICT_KEY in data:
                census = data[gb.NO_VERDICT_KEY]
                self.assertIsInstance(census, dict, path.name)
                self.assertGreaterEqual(census[gb.CENSUS_RUNS_KEY], 1, path.name)
                self.assertLessEqual(census[gb.CENSUS_RUNS_KEY],
                                     data["runs_observed"], path.name)
                for key, entry in census[gb.CENSUS_TESTS_KEY].items():
                    self.assertRegex(key, r"^(swift-testing|xctest)::", path.name)
                    self.assertGreaterEqual(entry["lost_runs"], 1, key)
                    self.assertGreaterEqual(entry["seen_runs"], entry["lost_runs"], key)
                    self.assertLessEqual(entry["seen_runs"],
                                         census[gb.CENSUS_RUNS_KEY], key)


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
