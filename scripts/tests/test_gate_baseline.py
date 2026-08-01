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


def xc_pass(suite, method, seconds="0.001"):
    return (
        "Test Case '-[PlayheadTests.%s %s]' started.\n"
        "Test Case '-[PlayheadTests.%s %s]' passed (%s seconds).\n"
        % (suite, method, suite, method, seconds)
    )


def xc_fail(suite, method, seconds="0.025"):
    return (
        "Test Case '-[PlayheadTests.%s %s]' started.\n"
        "Test Case '-[PlayheadTests.%s %s]' failed (%s seconds).\n"
        % (suite, method, suite, method, seconds)
    )


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
        for name in ("fast-gate.sh", "gate_baseline.py"):
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
            self.assertIn("+ swift-testing::fresh", proc.stdout)

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


if __name__ == "__main__":
    unittest.main()
