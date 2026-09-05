"""Rails for playhead-gjlp0 — the mutation battery's verdict machinery.

TWO HALVES, AND THE SECOND ONE IS THE POINT.

The Python half exercises `scripts/mutation_verdict.py` directly. The SHELL half
drives the real `scripts/mutation-battery.sh` end to end against a stubbed
`scripts/fast-gate.sh` that writes a canned log and a canned `.xcresult`, in a
throwaway `git archive` of the tree — because "does the verdict reach the table"
is a property of argument plumbing and of a bash case statement, not of Python.
`scripts/mutation-battery-gate-baseline.py` makes the same argument for the
gate, and playhead-s34ux is the cautionary tale: a classifier that was correct
in Python and INERT on the path the product takes, with 297 green rails over it.

EVERY RAIL IN THE `ShellLadderTests` CLASS AND EVERY `# FAILS-ON-OLD` RAIL IN
THE PYTHON HALF FAILS AGAINST THE BATTERY AS IT STOOD BEFORE THIS BEAD. That is
checked, not asserted: `test_the_old_reading_is_reproduced_and_is_wrong` runs
the pre-bead algorithm over the committed BD37 fixture and asserts it says
SURVIVED, so the rail that says VOID is pinned against a demonstrated
disagreement rather than against nothing.

The two `.log` fixtures are byte-exact distillations of preserved batch logs,
each verified to classify identically to its 4-5 MB original. The healthy one is
load-bearing in the other direction: a checker that VOIDs everything passes
every crash rail.

WHAT IT COSTS, MEASURED, BECAUSE ~8 MINUTES IS NOT WHAT A `unittest` MODULE IN
THIS REPO COSTS (playhead-gjlp0 R2). `test_gate_baseline` is 334 rails in about
a second. This module is **117 rails**, and the split is not subtle: **36** run
the real battery end to end and **4** read its `--list`, so **77** exercise
`mutation_verdict.py` directly — and the **68** of those that sit in classes
needing neither a sandbox nor a `--list` run in **0.40 s**.

Counted off the AST rather than by hand, and stated as three numbers that ADD UP
because R3 found the previous wording quoting two different granularities in one
sentence: it said 62 + 29 + 4 against a total of 104, and the missing nine were
rails that live in a class whose SETUP reaches the battery while the rail itself
does not. `36 + 4 + 77 = 117` is the same population counted one way.

The unit is one invocation of `scripts/mutation-battery.sh` and it is ~12-15 s
of which almost none is the rail: the battery walks all 1,109 MUTATIONS records
through `rec_file`/`rec_name` before it does anything at all — 2,218 command
substitutions, i.e. 2,218 forks — and `--list` costs **14.0 s** on this box for
the same reason. That is the battery's own preflight, every real operator
invocation pays it too, and no rail can avoid it without stubbing away the thing
under test.

What WAS avoidable is six separate `--list` runs, one per shell class plus the
table rail: `battery_listing()` caches it per root and took **~70 s** off the
module, which is why R2 added 12 rails for +94 s rather than +164 s. If this
ever has to get materially cheaper again, the honest lever is the battery's
fork-per-record preflight, not the rails.
"""

import io
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"
BD37 = FIXTURES / "mutation-verdict-bd37-distilled.log"
HEALTHY = FIXTURES / "mutation-verdict-healthy-control.log"

sys.path.insert(0, str(SCRIPTS))
import mutation_verdict as mv          # noqa: E402
import gate_baseline as gb             # noqa: E402

BD37_TEST = "the two bounds produce DISTINGUISHABLE rows in one store"
HEALTHY_TEST = (
    "THE EXTENDED PROPERTY: cards ∪ list is the entered set, and they never overlap"
)

# A minimal but byte-faithful console. Every shape here was copied off a real
# preserved log rather than invented.
APP = "2026-08-25 09:00:00.000000-0400 Playhead[%s:22131557] [AppDelegate] hello\n"


def console(pids=("4001",), tests=(), terminal="** TEST SUCCEEDED **",
            summary="✔ Test run with 3 tests in 1 suite passed after 0.6 seconds.",
            restarts=0, extra=""):
    """Build a console log. `tests` is [(name, outcome)] with outcome in
    started/passed/failed/skipped/issue."""
    out = ["Command line invocation:", "    /Applications/Xcode-beta.app/…/xcodebuild test"]
    for pid in pids:
        out.append(APP % pid.rstrip("\n"))
    for name, outcome in tests:
        if outcome in ("started", "passed", "failed", "issue"):
            out.append('◇ Test "%s" started.' % name)
        if outcome == "passed":
            out.append('✔ Test "%s" passed after 1.234 seconds.' % name)
        elif outcome == "failed":
            out.append('✘ Test "%s" failed after 0.147 seconds with 1 issue.' % name)
        elif outcome == "issue":
            out.append('✘ Test "%s" recorded an issue at F.swift:1:1: Expectation failed: x'
                       % name)
            out.append('✘ Test "%s" failed after 0.147 seconds with 1 issue.' % name)
        elif outcome == "skipped":
            out.append('➜ Test "%s" skipped.' % name)
    for _ in range(restarts):
        out.append("Restarting after unexpected exit, crash, or test timeout; "
                   "summary will include totals from previous launches.")
    if extra:
        out.append(extra)
    if summary:
        out.append(summary)
    if terminal:
        out.append(terminal)
    return "\n".join(out) + "\n"


def xctest_console(suite, method, outcome, module="PlayheadTests", **kw):
    """XCTest's own console format. The MODULE prefix is the point.

    xcodebuild prints `-[PlayheadTests.SomeTests testFoo]` and the MUTATIONS
    table spells the same expectation `SomeTests/testFoo`; a resolver that
    compares those literally finds nothing. Every rail that uses this builder
    exists because the first cut of `candidate_keys` did exactly that.
    """
    bracket = "'-[%s.%s %s]'" % (module, suite, method)
    lines = ["Test Case %s started." % bracket]
    if outcome == "passed":
        lines.append("Test Case %s passed (0.003 seconds)." % bracket)
    elif outcome == "failed":
        lines.append("Test Case %s failed (0.003 seconds)." % bracket)
    return console(extra="\n".join(lines), **kw)


def bundle_payload(cases, framework="swift-testing", suite="S"):
    """`xcresulttool get test-results tests` shaped payload. `cases` is
    [(name, result, failure_messages)].

    `nodeIdentifierURL` is not decoration: `gate_baseline._is_swift_testing`
    decides the framework from whether it ends in `)`, and REFUSES a node that
    has none. A fixture without it would be testing a different bundle format
    from the one xcresulttool emits.
    """
    nodes = []
    for name, result, messages in cases:
        ident = ("%s/%s()" % (suite, name) if framework == "swift-testing"
                 else "%s/%s" % (suite, name))
        node = {
            "nodeType": "Test Case",
            "name": name,
            "nodeIdentifier": ident,
            "nodeIdentifierURL": "test://com.apple.xcode/Playhead/PlayheadTests/" + ident,
            "result": result,
            "children": [{"nodeType": "Failure Message", "name": m} for m in messages],
        }
        nodes.append(node)
    return {
        "testNodes": [{
            "nodeType": "Unit test bundle",
            # `PlayheadTests`, not `PlayheadTests.xctest`. The node name is what
            # `_collect_case` folds into an XCTest key AND into the
            # `-only-testing:` target, so the wrong spelling here would make an
            # XCTest bundle rail agree with a resolver nothing else agrees with.
            # `test_gate_baseline.py` uses the same name for the same reason.
            "name": "PlayheadTests",
            "children": [{"nodeType": "Test Suite", "name": suite,
                          "children": nodes}],
        }]
    }


def fake_bundle(directory, payload, ok=True, stderr=b""):
    """A directory that looks like an .xcresult, plus a runner that returns
    `payload` instead of shelling out to xcresulttool."""
    import json
    path = pathlib.Path(str(directory)) / "b.xcresult"
    path.mkdir(exist_ok=True)

    class Completed(object):
        returncode = 0 if ok else 1
        stdout = json.dumps(payload).encode()

    Completed.stderr = stderr
    return path, (lambda argv: Completed())


class VerdictTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = scratch_dir("playhead-mv-test.")
        self.addCleanup(shutil.rmtree, str(self.dir), True)

    def write_log(self, text, name="batch.log", mtime=None):
        p = self.dir / name
        p.write_text(text, encoding="utf-8")
        if mtime is not None:
            os.utime(str(p), (mtime, mtime))
        return p

    def read(self, log, names, xcresult=None, rc=0, since=None, reader=None):
        floor = since if since is not None else int(pathlib.Path(str(log)).stat().st_mtime)
        reading = mv.read_batch(str(log), floor, xcresult=xcresult, rc=rc,
                                xcresult_reader=reader)
        return reading, dict(mv.classify(reading, names))


# ---------------------------------------------------------------------------
# The ladder
# ---------------------------------------------------------------------------
class LadderTests(VerdictTestCase):

    def test_a_started_only_test_is_no_verdict_not_passed(self):
        # FAILS-ON-OLD: this is the whole bead. The old battery had one bucket
        # for "did not fail", and this test lands in it.
        log = self.write_log(console(tests=[("A", "started"), ("B", "passed")]))
        _reading, states = self.read(log, ["A", "B"])
        self.assertEqual(states["A"], mv.NO_VERDICT)
        self.assertEqual(states["B"], mv.PASSED)

    def test_a_failed_test_is_failed(self):
        log = self.write_log(console(tests=[("A", "failed")], terminal="** TEST FAILED **"))
        _r, states = self.read(log, ["A"])
        self.assertEqual(states["A"], mv.FAILED)

    def test_an_issue_line_alone_is_a_failure(self):
        log = self.write_log(console(tests=[("A", "issue")], terminal="** TEST FAILED **"))
        _r, states = self.read(log, ["A"])
        self.assertEqual(states["A"], mv.FAILED)

    def test_a_skipped_test_is_not_a_pass(self):
        # FAILS-ON-OLD: a skip is "not in the failure list", so it read SURVIVED.
        log = self.write_log(console(tests=[("A", "skipped")]))
        _r, states = self.read(log, ["A"])
        self.assertEqual(states["A"], mv.SKIPPED)
        self.assertIn(mv.SKIPPED, mv.UNJUDGED)

    def test_a_name_no_roster_mentions_is_absent(self):
        log = self.write_log(console(tests=[("A", "passed")]))
        _r, states = self.read(log, ["nobody has this name"])
        self.assertEqual(states["nobody has this name"], mv.ABSENT)

    def test_absent_and_no_verdict_are_different_states(self):
        # They were different in the old code too (`never_ran` vs `missing`),
        # and the point of this rail is that the THIRD state did not exist.
        self.assertNotEqual(mv.ABSENT, mv.NO_VERDICT)
        self.assertNotIn(mv.ABSENT, mv.UNJUDGED)

    def test_only_failed_and_passed_are_stated(self):
        self.assertEqual(mv.STATED, frozenset((mv.FAILED, mv.PASSED)))
        for state in (mv.NO_VERDICT, mv.CRASHED, mv.DENIED, mv.SKIPPED):
            self.assertNotIn(state, mv.STATED)

    def test_xctest_resolves_by_both_spellings(self):
        text = console(tests=[])
        text += ("Test Case '-[PlayheadTests.SomeTests testThing]' started.\n"
                 "Test Case '-[PlayheadTests.SomeTests testThing]' passed (0.003 seconds).\n")
        log = self.write_log(text)
        _r, states = self.read(log, ["testThing", "PlayheadTests.SomeTests/testThing"])
        self.assertEqual(states["testThing"], mv.PASSED)
        self.assertEqual(states["PlayheadTests.SomeTests/testThing"], mv.PASSED)

    def test_an_xctest_that_started_and_never_reported_is_no_verdict(self):
        # FAILS-ON-OLD, and it is the XCTest half of the same hole.
        text = console(tests=[])
        text += "Test Case '-[PlayheadTests.SomeTests testThing]' started.\n"
        log = self.write_log(text)
        _r, states = self.read(log, ["testThing"])
        self.assertEqual(states["testThing"], mv.NO_VERDICT)

    def test_a_spliced_pass_line_is_still_a_pass(self):
        # The phn3 shape: an app-log line lands mid-verdict and the tail is
        # displaced. `gate_baseline.rejoin_spliced_lines` puts it back. Without
        # the rejoin this reads NO-VERDICT and the batch VOIDs a healthy run.
        # The intrusion lands INSIDE the verdict line and carries its own
        # newline, so one logical line becomes two physical ones: the head
        # keeps the app-log text, and the tail arrives on the next line.
        text = console(tests=[("A", "started")], summary=None, terminal=None)
        text += ('✔ Test "A" pa' + (APP % "4001")
                 + "ssed after 1.234 seconds.\n"
                 + "✔ Test run with 1 test in 1 suite passed after 0.6 seconds.\n"
                 + "** TEST SUCCEEDED **\n")
        log = self.write_log(text)
        _r, states = self.read(log, ["A"])
        self.assertEqual(states["A"], mv.PASSED)

    def test_an_octal_escaped_start_line_pairs_with_its_plain_verdict(self):
        # playhead-3rql: the start line escapes non-ASCII, the verdict line does
        # not. Keyed literally those are two tests, one of them a phantom
        # casualty. `st_key` decodes, so they are one.
        name = "a b — c"
        text = console(tests=[], summary=None, terminal=None)
        text += ('◇ Test "a b \\342\\200\\224 c" started.\n'
                 '✔ Test "a b — c" passed after 1.0 seconds.\n'
                 "✔ Test run with 1 test in 1 suite passed after 0.6 seconds.\n"
                 "** TEST SUCCEEDED **\n")
        log = self.write_log(text)
        reading, states = self.read(log, [name])
        self.assertEqual(states[name], mv.PASSED)
        self.assertEqual(len(reading.run.no_verdict), 0)


class XCTestSpellingTests(VerdictTestCase):
    """The MUTATIONS table's XCTest spelling must resolve — playhead-gjlp0 R1.

    `gate_baseline` keys an XCTest case off the WHOLE bracketed name, module
    included (`xctest::PlayheadTests.SomeTests/testFoo`), because its
    `_XC_RESULT` group is greedy over `[A-Za-z0-9_.]+`. The scraper this bead
    deleted used a NON-greedy prefix and registered `SomeTests/testFoo`, which
    is the spelling 48 mutations in the table use as their SOLE expectation.
    Compared literally the two never meet, and the first cut of `candidate_keys`
    compared them literally: every such expectation resolved ABSENT, which is
    the arm that exits 2 out of the baseline preflight, so the XCTest half of
    the battery refused to run at all.
    """

    def test_the_bare_suite_spelling_resolves_against_a_module_qualified_key(self):
        log = self.write_log(xctest_console("SomeTests", "testFoo", "passed"))
        _r, states = self.read(log, ["SomeTests/testFoo"])
        self.assertEqual(states["SomeTests/testFoo"], mv.PASSED)

    def test_the_bare_suite_spelling_also_carries_a_FAILURE_through(self):
        # The direction that decides a KILL. A resolver that only fixed the
        # PASSED arm would turn every XCTest kill into an ERROR.
        log = self.write_log(xctest_console("SomeTests", "testFoo", "failed",
                                            terminal="** TEST FAILED **"))
        _r, states = self.read(log, ["SomeTests/testFoo"])
        self.assertEqual(states["SomeTests/testFoo"], mv.FAILED)

    def test_the_bare_suite_spelling_resolves_against_the_BUNDLE_too(self):
        # The bundle is the verdict source on every real run, and it spells the
        # suite module-qualified as well — from a different code path.
        log = self.write_log(xctest_console("SomeTests", "testFoo", "passed"))
        path, reader = fake_bundle(self.dir, bundle_payload(
            [("testFoo", gb.XCRESULT_PASSED, [])],
            framework="xctest", suite="SomeTests"))
        _r, states = self.read(log, ["SomeTests/testFoo"], xcresult=path, reader=reader)
        self.assertEqual(states["SomeTests/testFoo"], mv.PASSED)

    def test_a_started_only_xctest_is_still_NO_VERDICT_under_the_bare_spelling(self):
        # The widening must not manufacture a pass out of a roster entry.
        log = self.write_log(xctest_console("SomeTests", "testFoo", "started"))
        _r, states = self.read(log, ["SomeTests/testFoo"])
        self.assertEqual(states["SomeTests/testFoo"], mv.NO_VERDICT)

    def test_the_suffix_match_is_anchored_on_the_dot(self):
        # `.SomeTests/testFoo` must not match `…OtherSomeTests/testFoo`. Without
        # the leading dot the widening would resolve one suite's expectation
        # against another suite's verdict.
        log = self.write_log(xctest_console("OtherSomeTests", "testFoo", "passed"))
        _r, states = self.read(log, ["SomeTests/testFoo"])
        self.assertEqual(states["SomeTests/testFoo"], mv.ABSENT)

    def test_a_swift_testing_display_name_containing_a_slash_still_wins(self):
        # Several real expectations contain a `/` inside the display name
        # (`… a fast/final twin`). The XCTest arm must not shadow them.
        name = "Hot path does not promote a candidate from a fast/final twin"
        log = self.write_log(console(tests=[(name, "passed")]))
        _r, states = self.read(log, [name])
        self.assertEqual(states[name], mv.PASSED)

    def test_the_table_really_still_uses_that_spelling(self):
        # ANTI-VACUITY. Every rail above is a claim about a shape; this one is
        # the claim that the shape is still in the battery. If the MUTATIONS
        # table is ever rewritten to the module-qualified spelling, this fails
        # and says the rails above have become decoration.
        listing = battery_listing(ROOT)
        bare = []
        for line in listing.split("\n"):
            if "expects:" not in line:
                continue
            for want in line.split("expects:", 1)[1].strip().split(";"):
                if re.match(r"^[A-Za-z0-9_]+Tests/test[A-Za-z0-9_]+$", want):
                    bare.append(want)
        self.assertGreater(len(bare), 40, "the table no longer uses the bare "
                                          "XCTest spelling; the rails above are dead")
        # And none of them is ABSENT against a roster spelled the way xcodebuild
        # spells it — measured over the whole table rather than over one sample.
        text = console(tests=[], summary=None, terminal=None)
        for want in sorted(set(bare)):
            suite, method = want.split("/", 1)
            text += "Test Case '-[PlayheadTests.%s %s]' passed (0.001 seconds).\n" % (
                suite, method)
        text += ("✔ Test run with 1 test in 1 suite passed after 0.6 seconds.\n"
                 "** TEST SUCCEEDED **\n")
        log = self.write_log(text, name="table.log")
        _r, states = self.read(log, sorted(set(bare)))
        absent = [n for n, s in states.items() if s != mv.PASSED]
        self.assertEqual(absent, [], "unresolvable expectations: %r" % absent[:5])


class WholeTableResolutionTests(VerdictTestCase):
    """EVERY expectation in the table must resolve — playhead-gjlp0 R2.

    The rail above measures ONE shape (`SomeTests/testFoo`, 128 expectations)
    and it is the shape R1's P1 broke. This one takes the whole table — 1,910
    expectations across 1,109 records — sorts it into the three spellings the
    battery actually uses, builds a roster in the words xcodebuild really
    prints, and asserts every single one comes back PASSED.

    It exists because R1's lesson was about POPULATION, not about the resolver:
    the twelve shell rails all drove one Swift Testing display name, so a whole
    naming system sat outside what they measured. A rail over a sample of the
    table has the same exposure one size down.
    """

    #: The three spellings, decided the way the battery's own comments decide
    #: them. `Suite/method` and a bare `testFoo` are XCTest; anything else is a
    #: Swift Testing DISPLAY name, including the 28 that contain a `/`.
    XCTEST_QUALIFIED = re.compile(r"^[A-Za-z0-9_]+/[A-Za-z0-9_]+$")
    XCTEST_BARE = re.compile(r"^test[A-Za-z0-9_]*$")

    @classmethod
    def spelling(cls, want):
        if cls.XCTEST_QUALIFIED.match(want):
            return "xctest-qualified"
        if cls.XCTEST_BARE.match(want):
            return "xctest-bare"
        return "display-name"

    @staticmethod
    def table_expectations():
        out = []
        for line in battery_listing(ROOT).split("\n"):
            if "expects:" not in line:
                continue
            for want in line.split("expects:", 1)[1].strip().split(";"):
                out.append(want)
        return out

    def test_the_table_has_all_three_spellings_and_a_lot_of_them(self):
        # ANTI-VACUITY. The rail below is only worth its runtime while the
        # table really carries every shape it claims to cover.
        counts = {}
        for want in self.table_expectations():
            counts[self.spelling(want)] = counts.get(self.spelling(want), 0) + 1
        self.assertEqual(sorted(counts), ["display-name", "xctest-bare",
                                          "xctest-qualified"], counts)
        self.assertGreater(counts["display-name"], 1500, counts)
        self.assertGreater(counts["xctest-qualified"], 100, counts)
        self.assertGreater(counts["xctest-bare"], 10, counts)
        self.assertEqual([w for w in self.table_expectations() if w != w.strip()], [],
                         "an expectation carries edge whitespace; the shell writes it "
                         "verbatim into the names file and the scorer matches exactly")
        self.assertEqual([w for w in self.table_expectations() if not w], [],
                         "an EMPTY expectation: the record would be KILLED for free")

    def roster(self, wants):
        lines = ["Command line invocation:", "    /usr/bin/xcodebuild test"]
        for want in wants:
            kind = self.spelling(want)
            if kind == "xctest-qualified":
                suite, method = want.split("/", 1)
                lines.append("Test Case '-[PlayheadTests.%s %s]' passed (0.001 seconds)."
                             % (suite, method))
            elif kind == "xctest-bare":
                lines.append("Test Case '-[PlayheadTests.SomeHostSuite %s]' "
                             "passed (0.001 seconds)." % want)
            else:
                lines.append('✔ Test "%s" passed after 0.100 seconds.' % want)
        lines.append("✔ Test run with 9 tests in 3 suites passed after 1.0 seconds.")
        lines.append("** TEST SUCCEEDED **")
        return "\n".join(lines) + "\n"

    def test_every_expectation_in_the_table_resolves_off_the_console(self):
        wants = self.table_expectations()
        log = self.write_log(self.roster(wants), name="whole-table.log")
        reading, states = self.read(log, wants)
        self.assertEqual(reading.batch_state, mv.BATCH_OK, reading.batch_reasons)
        unresolved = sorted({w for w in wants if states[w] != mv.PASSED})
        self.assertEqual(unresolved, [], "%d unresolvable: %r"
                         % (len(unresolved), unresolved[:5]))

    def test_every_expectation_in_the_table_resolves_off_the_BUNDLE(self):
        # The bundle is the verdict source on every real run, and it spells
        # both frameworks from a different code path than the console does.
        wants = self.table_expectations()
        suites = {}
        for want in wants:
            kind = self.spelling(want)
            if kind == "xctest-qualified":
                suite, method, ident = want.split("/", 1)[0], want.split("/", 1)[1], None
            elif kind == "xctest-bare":
                suite, method = "SomeHostSuite", want
            else:
                suite, method = "STSuite", None
            bucket = suites.setdefault(suite, [])
            if method is None:
                ident = "%s/f%d()" % (suite, len(bucket))
                name = want
            else:
                ident = "%s/%s" % (suite, method)
                name = method
            bucket.append({
                "nodeType": "Test Case", "name": name, "nodeIdentifier": ident,
                "nodeIdentifierURL":
                    "test://com.apple.xcode/Playhead/PlayheadTests/" + ident,
                "result": gb.XCRESULT_PASSED, "children": [],
            })
        payload = {"testNodes": [{
            "nodeType": "Unit test bundle", "name": "PlayheadTests",
            "children": [{"nodeType": "Test Suite", "name": s, "children": c}
                         for s, c in suites.items()]}]}
        log = self.write_log(self.roster(wants), name="whole-table-bundle.log")
        path, reader = fake_bundle(self.dir, payload)
        reading, states = self.read(log, wants, xcresult=path, reader=reader)
        self.assertEqual(reading.verdict_source, gb.VERDICT_SOURCE_BUNDLE)
        unresolved = sorted({w for w in wants if states[w] != mv.PASSED})
        self.assertEqual(unresolved, [], "%d unresolvable: %r"
                         % (len(unresolved), unresolved[:5]))


class PrecedenceTests(VerdictTestCase):
    """Two same-named tests share one key. Resolve toward the worse news,
    except that a stated FAILURE outranks everything — it is the only positive
    evidence that the rail fired."""

    def test_failed_outranks_a_colliding_pass(self):
        log = self.write_log(console(tests=[("A", "passed"), ("A", "failed")],
                                     terminal="** TEST FAILED **"))
        _r, states = self.read(log, ["A"])
        self.assertEqual(states["A"], mv.FAILED)

    def test_a_crash_outranks_a_colliding_pass(self):
        log = self.write_log(console(tests=[("A", "passed")]))
        path, reader = fake_bundle(self.dir, bundle_payload([
            ("A", gb.XCRESULT_PASSED, []),
            ("A", gb.XCRESULT_FAILED, ["Test crashed with signal trap."]),
        ]))
        _r, states = self.read(log, ["A"], xcresult=path, reader=reader)
        self.assertEqual(states["A"], mv.CRASHED)

    def test_the_precedence_order_is_stated_once(self):
        # A list rather than a chain of ifs, so a new state cannot be added
        # without deciding where it sits.
        self.assertEqual(
            mv._PRECEDENCE,
            (mv.FAILED, mv.CRASHED, mv.DENIED, mv.NO_VERDICT, mv.SKIPPED, mv.PASSED))


class BatchHealthTests(VerdictTestCase):

    def test_a_clean_batch_is_ok(self):
        log = self.write_log(console(tests=[("A", "passed")]))
        reading, _s = self.read(log, ["A"])
        self.assertEqual(reading.batch_state, mv.BATCH_OK)
        self.assertEqual(reading.batch_reasons, [])

    def test_two_host_pids_void_the_batch(self):
        # FAILS-ON-OLD: the old battery had no notion of batch health at all.
        log = self.write_log(console(pids=("4001", "4002"), tests=[("A", "passed")]))
        reading, _s = self.read(log, ["A"])
        self.assertEqual(reading.batch_state, mv.BATCH_VOID)
        self.assertEqual(reading.host_replacements, 1)
        self.assertTrue(any("REPLACED 1 time" in r for r in reading.batch_reasons))

    def test_the_restart_marker_alone_voids_the_batch(self):
        log = self.write_log(console(tests=[("A", "passed")], restarts=1))
        reading, _s = self.read(log, ["A"])
        self.assertEqual(reading.batch_state, mv.BATCH_VOID)

    def test_replacements_are_counted_from_pids_not_from_the_phrase(self):
        # The bead's cheap first cut was `grep -c 'Restarting after…'`, and on
        # its own specimen that returns 11 for 10 replacements because
        # `gate-memory:`'s verdict block QUOTES the message back into the log.
        # Here: one real restart, plus the gate's quotation of it.
        quote = ("gate-memory:   * xcodebuild printed `Restarting after unexpected "
                 "exit, crash, or test timeout`")
        log = self.write_log(console(pids=("4001", "4002"), tests=[("A", "passed")],
                                     restarts=1, extra=quote))
        reading, _s = self.read(log, ["A"])
        self.assertEqual(log.read_text(encoding="utf-8").count(
            "Restarting after unexpected exit"), 2)
        self.assertEqual(reading.host_replacements, 1)

    def test_a_log_with_no_terminal_marker_is_void(self):
        log = self.write_log(console(tests=[("A", "passed")], summary=None, terminal=None))
        reading, _s = self.read(log, ["A"])
        self.assertEqual(reading.batch_state, mv.BATCH_VOID)
        self.assertTrue(any("neither format reported an outcome" in r
                            for r in reading.batch_reasons))

    def test_either_terminal_format_is_enough(self):
        # Reading only one of the two formats is this repo's oldest gate-triage
        # mistake (2026-07-31).
        only_swift = self.write_log(console(tests=[("A", "passed")], terminal=None),
                                    name="a.log")
        only_xcode = self.write_log(console(tests=[("A", "passed")], summary=None),
                                    name="b.log")
        for log in (only_swift, only_xcode):
            reading, _s = self.read(log, ["A"])
            self.assertEqual(reading.batch_state, mv.BATCH_OK, log.name)

    def test_a_signal_death_voids_the_batch(self):
        log = self.write_log(console(tests=[("A", "passed")], extra="Killed: 9"))
        reading, _s = self.read(log, ["A"])
        self.assertEqual(reading.batch_state, mv.BATCH_VOID)

    def test_exit_137_voids_the_batch_even_with_a_clean_log(self):
        log = self.write_log(console(tests=[("A", "passed")]))
        reading, _s = self.read(log, ["A"], rc=137)
        self.assertEqual(reading.batch_state, mv.BATCH_VOID)

    def test_reasons_accumulate_rather_than_short_circuit(self):
        log = self.write_log(console(pids=("4001", "4002"), tests=[("A", "passed")],
                                     restarts=1, extra="Killed: 9"))
        reading, _s = self.read(log, ["A"], rc=137)
        self.assertGreaterEqual(len(reading.batch_reasons), 4)


class BundleTests(VerdictTestCase):
    """The bundle is the verdict source (playhead-t53a) and a missing one is
    never a silent fallback."""

    def bundle(self, payload, ok=True, stderr=b""):
        return fake_bundle(self.dir, payload, ok=ok, stderr=stderr)

    def test_the_bundle_can_rescue_a_console_silent_pass(self):
        log = self.write_log(console(tests=[("A", "started")]))
        path, reader = self.bundle(bundle_payload([("A", gb.XCRESULT_PASSED, [])]))
        _r, states = self.read(log, ["A"], xcresult=path, reader=reader)
        self.assertEqual(states["A"], mv.PASSED)

    def test_a_crash_message_is_not_a_kill(self):
        # The direction that matters: taken at face value the bundle records a
        # dead host's tests as `Failed`, which would credit a KILL for a host
        # death. playhead-t53a routes it to the census; here it is CRASHED.
        log = self.write_log(console(tests=[("A", "started")]))
        path, reader = self.bundle(bundle_payload(
            [("A", gb.XCRESULT_FAILED, ["Test crashed with signal trap."])]))
        reading, states = self.read(log, ["A"], xcresult=path, reader=reader)
        self.assertEqual(states["A"], mv.CRASHED)
        self.assertEqual(reading.batch_state, mv.BATCH_VOID)

    def test_an_unrecognised_failure_message_stays_a_failure(self):
        log = self.write_log(console(tests=[("A", "started")]))
        path, reader = self.bundle(bundle_payload(
            [("A", gb.XCRESULT_FAILED, ["Expectation failed: capped.truncated"])]))
        _r, states = self.read(log, ["A"], xcresult=path, reader=reader)
        self.assertEqual(states["A"], mv.FAILED)

    def test_a_resource_denial_is_not_a_kill(self):
        log = self.write_log(console(tests=[("A", "started")]))
        path, reader = self.bundle(bundle_payload(
            [("A", gb.XCRESULT_FAILED,
              ['The file "x" couldn\'t be opened. NSPOSIXErrorDomain Code=24 '
               '"Too many open files"'])]))
        _r, states = self.read(log, ["A"], xcresult=path, reader=reader)
        self.assertEqual(states["A"], mv.DENIED)

    def test_an_unreadable_bundle_is_never_a_silent_console_fallback(self):
        log = self.write_log(console(tests=[("A", "passed")]))
        path, reader = self.bundle({}, ok=False, stderr=b"boom")
        with self.assertRaises(gb.XcresultUnreadable):
            mv.read_batch(str(log), int(log.stat().st_mtime), xcresult=path,
                          xcresult_reader=reader)

    def test_the_verdict_source_is_named_either_way(self):
        log = self.write_log(console(tests=[("A", "passed")]))
        reading, _s = self.read(log, ["A"])
        self.assertEqual(reading.verdict_source, gb.VERDICT_SOURCE_CONSOLE)
        path, reader = self.bundle(bundle_payload([("A", gb.XCRESULT_PASSED, [])]))
        reading, _s = self.read(log, ["A"], xcresult=path, reader=reader)
        self.assertEqual(reading.verdict_source, gb.VERDICT_SOURCE_BUNDLE)

    def test_the_crash_reason_names_a_TEST_not_a_KEY(self):
        # `run.crashed` is keyed `swift-testing::<name>`. Printing the key raw
        # invites a reader to grep for a spelling that appears nowhere else —
        # a rendering read as the thing it renders, one more time.
        log = self.write_log(console(tests=[("A", "started")]))
        path, reader = self.bundle(bundle_payload(
            [("A", gb.XCRESULT_FAILED, ["Test crashed with signal trap."])]))
        reading, _s = self.read(log, ["A"], xcresult=path, reader=reader)
        reason = [r for r in reading.batch_reasons if "host died" in r]
        self.assertEqual(len(reason), 1, reading.batch_reasons)
        self.assertTrue(reason[0].endswith("e.g. A"), reason[0])
        self.assertNotIn("swift-testing::", reason[0])

    def test_the_console_roster_survives_the_bundle(self):
        # UNIONED, not replaced: a test the bundle never mentions is still a
        # casualty if its start line survived.
        log = self.write_log(console(tests=[("A", "started"), ("B", "started")]))
        path, reader = self.bundle(bundle_payload([("B", gb.XCRESULT_PASSED, [])]))
        _r, states = self.read(log, ["A", "B"], xcresult=path, reader=reader)
        self.assertEqual(states["A"], mv.NO_VERDICT)
        self.assertEqual(states["B"], mv.PASSED)

    def test_a_result_string_the_parser_cannot_read_VOIDS_the_batch_and_says_so(self):
        # playhead-gjlp0 R4. `gate_baseline` files a Test Case whose `result` is
        # none of Passed/Failed/Skipped/Expected-Failure into `run.unjudged`,
        # keyed and carrying the word it could not read, and unions its key into
        # the roster — so the test reads NO-VERDICT, which is right. What was
        # wrong is that the BATCH read OK, so the run said "the expected test
        # reached NO VERDICT" and its epilogue sent the reader to re-run and
        # then to suspect the mutation of killing the test host. Re-running
        # cannot clear an unreadable result string.
        log = self.write_log(console(tests=[("A", "started")]))
        path, reader = self.bundle(bundle_payload([("A", "Bikeshed", [])]))
        reading, states = self.read(log, ["A"], xcresult=path, reader=reader)
        self.assertEqual(states["A"], mv.NO_VERDICT)
        self.assertEqual(reading.batch_state, mv.BATCH_VOID, reading.batch_reasons)
        reason = [r for r in reading.batch_reasons if "does not recognise" in r]
        self.assertEqual(len(reason), 1, reading.batch_reasons)
        # The word it could not read, the test's NAME rather than its key, and
        # the remedy — which is the opposite of every other VOID reason's.
        self.assertIn("'Bikeshed'", reason[0])
        self.assertIn("e.g. A ", reason[0])
        self.assertNotIn("swift-testing::", reason[0])
        self.assertIn("RE-RUNNING WILL NOT CHANGE IT", reason[0])
        # AND WHAT TO DO INSTEAD. Mutant R4-5 survived the first cut of this
        # rail: it pinned the consequence ("do not re-run") and left the ACTION
        # unpinned, so an edit that deleted the only sentence saying where the
        # fix goes was invisible. A reason that only says what will not help is
        # half a reason.
        self.assertIn("gate_baseline.py", reason[0])
        self.assertIn("XCRESULT_", reason[0])
        out = io.StringIO()
        mv.render(reading, [("A", states["A"])], out)
        self.assertIn("1 unreadable", out.getvalue())

    def test_the_census_never_presents_a_CRASH_as_a_test_the_other_columns_missed(self):
        """playhead-gjlp0 R5 — the census counted two of its own columns twice.

        `no_verdict` is `started - ran - skipped - resource`, so a CRASHED key
        and an UNREADABLE one each land in it as well as in their own column,
        while `resource-denied` is genuinely disjoint. Printed as one line of
        `·`-separated counts, the columns after `started` add to SIX on the
        four-test batch below. Three columns that look like siblings behaving
        three different ways is a value that names one thing read as though it
        named another — this module's own subject, in its own census.

        Driven on the overlap itself rather than on the wording, so a future
        re-flattening fails here rather than reading plausibly.
        """
        log = self.write_log(console(tests=[("A", "started"), ("B", "started"),
                                            ("C", "started"), ("D", "passed")]))
        path, reader = self.bundle(bundle_payload([
            ("A", gb.XCRESULT_FAILED, ["Test crashed with signal trap."]),
            ("B", "Bananas", []),
            ("C", gb.XCRESULT_FAILED, ["Migration failed: unable to open database file"]),
            ("D", gb.XCRESULT_PASSED, []),
        ]))
        reading, states = self.read(log, ["A", "B", "C", "D"],
                                    xcresult=path, reader=reader)
        run = reading.run
        self.assertEqual(states, {"A": mv.CRASHED, "B": mv.NO_VERDICT,
                                  "C": mv.DENIED, "D": mv.PASSED})
        # THE OVERLAP, stated. Both are inside NO VERDICT here; the denial is not.
        self.assertLessEqual(set(run.crashed), run.no_verdict)
        self.assertLessEqual(set(run.unjudged), run.no_verdict)
        self.assertEqual(set(run.resource) & run.no_verdict, set())
        self.assertEqual(len(run.no_verdict), 2)

        out = io.StringIO()
        mv.render(reading, list(states.items()), out)
        census = [line for line in out.getvalue().split("\n")
                  if "batch census:" in line]
        self.assertEqual(len(census), 1, out.getvalue())
        # The counts that PARTITION, and nothing else, on the census line.
        self.assertIn("4 started", census[0])
        self.assertIn("2 NO VERDICT", census[0])
        self.assertIn("1 resource-denied", census[0])
        self.assertNotIn("crashed", census[0])
        self.assertNotIn("unreadable", census[0])
        # The two DIAGNOSES, on their own line, saying they are not extra tests.
        why = [line for line in out.getvalue().split("\n") if "and WHY" in line]
        self.assertEqual(len(why), 1, out.getvalue())
        self.assertIn("1 crashed", why[0])
        self.assertIn("1 unreadable", why[0])
        self.assertIn("ALREADY COUNTS", why[0])
        self.assertIn("never extra", why[0])

    def test_the_diagnosis_line_states_its_zeroes_on_a_healthy_batch(self):
        """The anti-fabrication half: it makes a claim in BOTH directions.

        A line that appeared only when something was wrong would be a guard
        whose false branch says nothing — the shape that shipped `sim-trim.sh`
        inert (playhead-81ig) and that R2 closed one layer up in this same
        script.
        """
        log = self.write_log(console(tests=[("A", "passed")]))
        reading, states = self.read(log, ["A"])
        out = io.StringIO()
        mv.render(reading, list(states.items()), out)
        why = [line for line in out.getvalue().split("\n") if "and WHY" in line]
        self.assertEqual(len(why), 1, out.getvalue())
        self.assertIn("0 crashed", why[0])
        self.assertIn("0 unreadable", why[0])

    def test_a_bundle_whose_RESULTS_ARE_ALL_READABLE_never_says_that(self):
        # The anti-fabrication half. A reason that fires on a healthy batch
        # would be worse than no reason at all: it is checked here in the
        # direction that must stay silent, over all four spellings the parser
        # does recognise. Measured before the arm was written — `unjudged` is
        # EMPTY across four preserved full-plan bundles, ~48,300 nodes.
        log = self.write_log(console(tests=[("A", "started"), ("B", "started"),
                                            ("C", "started"), ("D", "started")]))
        path, reader = self.bundle(bundle_payload([
            ("A", gb.XCRESULT_PASSED, []),
            ("B", gb.XCRESULT_FAILED, ["Expectation failed: x"]),
            ("C", gb.XCRESULT_SKIPPED, []),
            ("D", gb.XCRESULT_EXPECTED_FAILURE, []),
        ]))
        reading, _s = self.read(log, ["A"], xcresult=path, reader=reader)
        self.assertEqual(reading.run.unjudged, {})
        self.assertEqual([r for r in reading.batch_reasons
                          if "does not recognise" in r], [])
        self.assertEqual(reading.batch_state, mv.BATCH_OK, reading.batch_reasons)
        out = io.StringIO()
        mv.render(reading, [("A", mv.PASSED)], out)
        self.assertIn("0 unreadable", out.getvalue())


class FreshnessTests(VerdictTestCase):
    """A log is evidence only if this run produced it (playhead-8cjo)."""

    def test_a_log_older_than_the_floor_is_refused(self):
        log = self.write_log(console(tests=[("A", "passed")]), mtime=1_000_000)
        with self.assertRaises(mv.CannotEvaluate) as caught:
            mv.read_batch(str(log), 2_000_000)
        message = str(caught.exception)
        self.assertIn("STALE", message)
        self.assertIn(str(log), message)          # names the PATH
        self.assertIn("1970", message.replace("1969", "1970"))  # names the DATE

    def test_a_log_at_the_floor_is_accepted(self):
        log = self.write_log(console(tests=[("A", "passed")]), mtime=1_000_000)
        reading = mv.read_batch(str(log), 1_000_000)
        self.assertEqual(reading.batch_state, mv.BATCH_OK)

    def test_a_stale_bundle_is_refused_even_with_a_fresh_log(self):
        log = self.write_log(console(tests=[("A", "passed")]))
        stale = self.dir / "old.xcresult"
        stale.mkdir()
        os.utime(str(stale), (1_000_000, 1_000_000))
        with self.assertRaises(mv.CannotEvaluate) as caught:
            mv.read_batch(str(log), int(log.stat().st_mtime), xcresult=stale)
        self.assertIn("result bundle is STALE", str(caught.exception))

    def test_a_missing_log_cannot_evaluate_rather_than_reporting_nothing_failed(self):
        with self.assertRaises(mv.CannotEvaluate):
            mv.read_batch(str(self.dir / "nope.log"), 0)

    def test_the_verify_subcommand_refuses_by_name_and_date(self):
        log = self.write_log(console(tests=[("A", "passed")]), mtime=1_000_000)
        rc = mv.main(["verify", "--log", str(log), "--since", "2000000"])
        self.assertEqual(rc, mv.EXIT_CANNOT_EVALUATE)
        rc = mv.main(["verify", "--log", str(log), "--since", "1000000"])
        self.assertEqual(rc, mv.EXIT_OK)


class OutputFileTests(VerdictTestCase):
    """The shell reads this file with awk, so its shape is a contract."""

    def run_cli(self, log, names, extra=()):
        namefile = self.dir / "names.txt"
        namefile.write_text("\n".join(names) + "\n", encoding="utf-8")
        out = self.dir / "out.txt"
        rc = mv.main(["classify", "--log", str(log), "--names", str(namefile),
                      "--out", str(out), "--since",
                      str(int(pathlib.Path(str(log)).stat().st_mtime))] + list(extra))
        return rc, out.read_text(encoding="utf-8")

    def test_the_out_file_carries_state_tab_name(self):
        log = self.write_log(console(tests=[("A", "passed")]))
        rc, text = self.run_cli(log, ["A"])
        self.assertEqual(rc, mv.EXIT_OK)
        self.assertIn("PASSED\tA\n", text)
        self.assertIn("#batch\tOK\n", text)

    def test_a_void_batch_exits_three_and_says_so_in_the_file(self):
        log = self.write_log(console(pids=("1", "2"), tests=[("A", "passed")]))
        rc, text = self.run_cli(log, ["A"])
        self.assertEqual(rc, mv.EXIT_VOID)
        self.assertIn("#batch\tVOID\n", text)
        self.assertTrue(any(line.startswith("#reason\t") for line in text.split("\n")))

    def test_every_failure_is_listed_not_only_the_expected_ones(self):
        # The baseline guard reads these: a suite red on a test no mutation
        # names still means every verdict in the run is worthless.
        log = self.write_log(console(tests=[("A", "passed"), ("Z", "failed")],
                                     terminal="** TEST FAILED **"))
        _rc, text = self.run_cli(log, ["A"])
        self.assertIn("#failures\t1\n", text)
        self.assertIn("#failure\tZ\n", text)

    def test_a_denied_resource_is_a_RED_TEST_the_failure_count_does_not_hold(self):
        # playhead-gjlp0 R3. `gate_baseline` lifts a denied key OUT of
        # `failures` (playhead-s34ux: a denial names the BOX, not the code), so
        # `#failures` reads 0 over a log carrying `✘ … failed`. Written out as
        # its own count and its own list, because the caller's remedy differs.
        log = self.write_log(console(
            tests=[("A", "passed")],
            extra=('◇ Test "Z" started.\n'
                   '✘ Test "Z" recorded an issue at F.swift:1:1: '
                   'Migration failed: unable to open database file\n'
                   '✘ Test "Z" failed after 0.1 seconds with 1 issue.'),
            terminal="** TEST FAILED **"))
        _rc, text = self.run_cli(log, ["A"])
        self.assertIn("#failures\t0\n", text)
        self.assertNotIn("#failure\tZ\n", text)
        self.assertIn("#resource\t1\n", text)
        self.assertIn("#denied\tZ\n", text)

    def test_a_denied_name_is_written_as_a_NAME_and_not_as_a_KEY(self):
        # The mirror of the crash reason's own rule: `run.resource` is keyed,
        # so the raw value carries `swift-testing::` in front of the test name
        # — a spelling that appears nowhere else and that a reader would grep
        # for in vain.
        log = self.write_log(console(
            tests=[("A", "passed")],
            extra=('◇ Test "Z" started.\n'
                   '✘ Test "Z" recorded an issue at F.swift:1:1: '
                   'Migration failed: unable to open database file\n'
                   '✘ Test "Z" failed after 0.1 seconds with 1 issue.'),
            terminal="** TEST FAILED **"))
        _rc, text = self.run_cli(log, ["A"])
        self.assertNotIn("::", text.split("#denied\t", 1)[1].split("\n", 1)[0])

    def test_a_clean_batch_states_a_ZERO_denial_count_rather_than_omitting_it(self):
        # ANTI-FABRICATION, and the shell depends on it: the baseline REFUSES
        # when `#resource` is missing, so a rail that only ever checks the
        # non-zero direction would pass against a scorer that never writes the
        # field at all.
        log = self.write_log(console(tests=[("A", "passed"), ("Z", "failed")],
                                     terminal="** TEST FAILED **"))
        _rc, text = self.run_cli(log, ["A"])
        self.assertIn("#resource\t0\n", text)
        self.assertNotIn("#denied\t", text)

    def test_a_console_only_run_says_it_had_no_bundle(self):
        log = self.write_log(console(tests=[("A", "passed")]))
        _rc, text = self.run_cli(log, ["A"])
        self.assertIn("#bundle\t(none — console only)\n", text)

    def test_a_name_containing_a_tab_is_refused_not_mangled(self):
        log = self.write_log(console(tests=[("A", "passed")]))
        namefile = self.dir / "names.txt"
        namefile.write_text("bad\tname\n", encoding="utf-8")
        rc = mv.main(["classify", "--log", str(log), "--names", str(namefile),
                      "--out", str(self.dir / "o.txt"), "--since", "0"])
        self.assertEqual(rc, mv.EXIT_CANNOT_EVALUATE)


class ScratchHygieneTests(unittest.TestCase):
    """Every throwaway directory must land where the ONE cleaner can find it.

    playhead-gjlp0 R3. `tempfile.mkdtemp()` defaults to `$TMPDIR`, and nothing
    in this repo sweeps that: `scripts/disk-cleanup.sh` handles `.worktrees/*`,
    `/private/tmp/playhead-*` and `$TMPDIR/Deleting-*`, and that is the list.
    The sandbox is a ~67 MiB copy of the tree; `tearDownModule` drops it, and
    `tearDownModule` does not run when a pass is interrupted. Measured at R3:
    10 orphaned sandboxes, 670 MiB, from one earlier day of review rounds.

    Pinned in SOURCE rather than by observation, because the failure is
    invisible on any single green run — the directory is created, used, and
    (usually) removed, and the leak only shows up days later on a box that
    refuses to gate below 13.5 GiB.
    """

    def test_every_mkdtemp_in_this_module_names_the_swept_root(self):
        import ast
        src = pathlib.Path(__file__).read_text(encoding="utf-8")
        offenders = []
        for node in ast.walk(ast.parse(src)):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            if not (isinstance(fn, ast.Attribute) and fn.attr == "mkdtemp"):
                continue
            if not any(kw.arg == "dir" for kw in node.keywords):
                offenders.append(node.lineno)
        self.assertEqual(
            offenders, [],
            "mkdtemp without dir= at line(s) %s — it would land in $TMPDIR, "
            "which scripts/disk-cleanup.sh does not sweep. Use scratch_dir()."
            % offenders)

    def test_the_cleaner_really_sweeps_that_root(self):
        # THE ANTI-VACUITY HALF. The rail above is worth nothing if the glob it
        # aims at is not the glob the cleaner uses, so read the cleaner.
        #
        # It matches the LOOP HEADER, not the string. A first cut asserted the
        # glob appeared anywhere in the file and PASSED with the loop rewritten
        # to sweep something else — the section's own `# 2. /private/tmp/
        # playhead-* cleanup` comment satisfied it. Driven: rewriting the `for`
        # line left that rail green. A string that names a mechanism read as
        # evidence the mechanism is there is this repo's standing defect class,
        # and an anti-vacuity guard that commits it is worse than none.
        cleaner = (ROOT / "scripts" / "disk-cleanup.sh").read_text(encoding="utf-8")
        loop = re.compile(r"^\s*for\s+\w+\s+in\s+%s/playhead-\*\s*;\s*do\s*$"
                          % re.escape(SCRATCH_ROOT), re.M)
        self.assertRegex(cleaner, loop,
                         "scripts/disk-cleanup.sh no longer LOOPS over "
                         "%s/playhead-*, so SCRATCH_ROOT is a reservoir nobody "
                         "reaps" % SCRATCH_ROOT)

    def test_scratch_dir_puts_a_directory_there(self):
        d = scratch_dir("playhead-mv-hygiene.")
        self.addCleanup(shutil.rmtree, str(d), True)
        self.assertTrue(str(d).startswith(SCRATCH_ROOT + "/playhead-"), str(d))
        self.assertTrue(d.is_dir())


# ---------------------------------------------------------------------------
# The preserved specimens
# ---------------------------------------------------------------------------
def old_battery_verdict(log_text, want):
    """The pre-playhead-gjlp0 algorithm, reproduced exactly.

    `extract_ran` scraped `◇ … started`, `extract_failures` scraped `✘ … failed`
    / `recorded an issue`, and the verdict was the arithmetic between them. Kept
    here so every rail below is pinned against a DEMONSTRATED disagreement
    rather than against a remembered one.
    """
    started, failed = [], []
    for line in log_text.split("\n"):
        m = re.search(r'◇ Test "(.+?)" started', line)
        if m:
            started.append(m.group(1))
        m = re.search(r'✘ Test "(.+?)" (?:failed|recorded an issue)', line)
        if m:
            failed.append(m.group(1))
        m = re.search(r"Test Case '-\[[A-Za-z0-9_.]*?([A-Za-z0-9_]+) ([A-Za-z0-9_]+)\]' started",
                      line)
        if m:
            started += [m.group(2), m.group(1) + "/" + m.group(2)]
        m = re.search(r"Test Case '-\[[A-Za-z0-9_.]*?([A-Za-z0-9_]+) ([A-Za-z0-9_]+)\]' failed",
                      line)
        if m:
            failed += [m.group(2), m.group(1) + "/" + m.group(2)]
    if want not in started:
        return "ERROR"
    return "KILLED" if want in failed else "SURVIVED"


class PreservedSpecimenTests(VerdictTestCase):

    def test_the_old_reading_is_reproduced_and_is_wrong(self):
        # The pin. Without this, "the new code says VOID" is a claim about
        # nothing in particular.
        text = BD37.read_text(encoding="utf-8")
        self.assertEqual(old_battery_verdict(text, BD37_TEST), "SURVIVED")

    def test_bd37_is_void_and_its_expected_test_has_no_verdict(self):
        reading, states = self.read(BD37, [BD37_TEST])
        self.assertEqual(reading.batch_state, mv.BATCH_VOID)
        self.assertEqual(states[BD37_TEST], mv.NO_VERDICT)

    def test_bd37_has_eleven_hosts_and_ten_replacements(self):
        reading, _s = self.read(BD37, [BD37_TEST])
        self.assertEqual(len(reading.host_pids), 11)
        self.assertEqual(reading.host_replacements, 10)

    def test_bd37_reached_a_terminal_marker_so_completeness_is_not_the_tell(self):
        # It printed `Test run with 29 tests in 8 suites passed` and
        # `** TEST FAILED **`. A checker that only asked "did it report" would
        # have passed this batch.
        reading, _s = self.read(BD37, [BD37_TEST])
        self.assertTrue(reading.run.complete)

    def test_the_healthy_control_passes_and_is_not_void(self):
        # The anti-fabrication direction. A checker that VOIDs everything
        # satisfies every rail above this one.
        reading, states = self.read(HEALTHY, [HEALTHY_TEST])
        self.assertEqual(reading.batch_state, mv.BATCH_OK)
        self.assertEqual(states[HEALTHY_TEST], mv.PASSED)
        self.assertEqual(len(reading.run.no_verdict), 0)
        self.assertEqual(len(reading.host_pids), 1)

    def test_the_old_reading_agrees_with_the_new_one_on_the_healthy_control(self):
        text = HEALTHY.read_text(encoding="utf-8")
        self.assertEqual(old_battery_verdict(text, HEALTHY_TEST), "SURVIVED")
        _r, states = self.read(HEALTHY, [HEALTHY_TEST])
        self.assertEqual(states[HEALTHY_TEST], mv.PASSED)


# ---------------------------------------------------------------------------
# `drop_bundle` — the one rm -rf this bead added
# ---------------------------------------------------------------------------
BATTERY = SCRIPTS / "mutation-battery.sh"


def shell_function(name):
    """The SHIPPED text of one function, lifted out of the battery.

    Extracted rather than re-typed: a rail that re-types the function tests the
    typing. The block runs from `name() {` to the first line that is exactly
    `}`, which is this script's uniform style.
    """
    lines = BATTERY.read_text(encoding="utf-8").split("\n")
    start = None
    for i, line in enumerate(lines):
        if line.startswith(name + "() {"):
            start = i
            break
    if start is None:
        raise AssertionError("%s() is not in %s any more" % (name, BATTERY))
    for j in range(start + 1, len(lines)):
        if lines[j] == "}":
            return "\n".join(lines[start:j + 1])
    raise AssertionError("%s() has no closing brace" % name)


class StateOfTests(unittest.TestCase):
    """`state_of` is the shell's reader of the scorer's file — playhead-gjlp0 R1.

    Two readers of one file that disagree is this bead's own defect shape, so
    the shell reader has to survive everything a Swift Testing display name can
    legally contain. `awk -v x=VALUE` ESCAPE-PROCESSES VALUE, which is why the
    name goes through the environment instead.
    """

    def setUp(self):
        self.dir = scratch_dir("playhead-mv-state.")
        self.addCleanup(shutil.rmtree, str(self.dir), True)
        self.out = self.dir / "outcomes.txt"

    def ask(self, lines, want):
        self.out.write_text("".join(l + "\n" for l in lines), encoding="utf-8")
        script = ('%s\nstate_of %s "$1" || echo NO-STATE\n'
                  % (shell_function('state_of'), "'%s'" % self.out))
        proc = subprocess.run(["bash", "-uo", "pipefail", "-c", script, "_", want],
                              capture_output=True, text=True)
        return proc.stdout.strip()

    NAMES = [
        "plain name",
        "a\\b name",                      # the one `-v` mangles
        "100% coverage",
        "-leading dash",
        "#hash start",
        "a | b",
        "tab\\there is impossible but \\t is not",
        "cards \u222a list is the entered set, and they never overlap",
        "V60 downgrades to consumedAutoSkip/consumed",
        "$WORK and `backticks` and ; and & and (parens)",
    ]

    def test_every_shape_a_display_name_can_carry_round_trips(self):
        rows = ["#batch\tOK"] + ["PASSED\t" + n for n in self.NAMES]
        for name in self.NAMES:
            self.assertEqual(self.ask(rows, name), "PASSED", repr(name))

    def test_a_name_no_line_mentions_is_NO_STATE_and_not_a_default(self):
        self.assertEqual(self.ask(["#batch\tOK", "PASSED\tA"], "B"), "NO-STATE")

    def test_a_header_line_can_never_answer_for_a_test(self):
        # `#failure\t<name>` carries a NAME in column 2. If the header filter
        # were dropped, an expectation would read the word `#failure` as its
        # state — the file's own metadata answering a question about a test.
        rows = ["#batch\tOK", "#failure\tZ"]
        self.assertEqual(self.ask(rows, "Z"), "NO-STATE")

    def test_the_first_line_wins_and_the_lookup_stops_there(self):
        rows = ["FAILED\tA", "PASSED\tA"]
        self.assertEqual(self.ask(rows, "A"), "FAILED")


class PrintEvidenceTests(unittest.TestCase):
    """A MISSING line and a line saying "none" are two different claims.

    `print_evidence` used to print NOTHING for a mutation with no evidence
    line — which is every ERROR raised before scoring. The table is the part
    that gets quoted, so a row with no path under it is a row whose reader has
    nowhere to go.
    """

    def setUp(self):
        self.dir = scratch_dir("playhead-mv-ev.")
        self.addCleanup(shutil.rmtree, str(self.dir), True)
        self.ev = self.dir / "evidence"

    def show(self, rows, name):
        self.ev.write_text("".join(r + "\n" for r in rows), encoding="utf-8")
        script = ("EVIDENCE=%s\nWORK=%s\n%s\nprint_evidence \"$1\"\n"
                  % ("'%s'" % self.ev, "'%s'" % self.dir,
                     shell_function("print_evidence")))
        proc = subprocess.run(["bash", "-uo", "pipefail", "-c", script, "_", name],
                              capture_output=True, text=True)
        return proc.stdout + proc.stderr

    def test_a_recorded_log_and_bundle_are_both_printed(self):
        out = self.show(["M05\t/w/batch-1.log\t/w/batch-1.xcresult"], "M05")
        self.assertIn("/w/batch-1.log", out)
        self.assertIn("/w/batch-1.xcresult", out)

    def test_a_dash_bundle_is_not_printed_as_a_path(self):
        # The battery records "-" when no bundle exists. Printing it would be a
        # line naming a thing, read as evidence the thing is there.
        out = self.show(["M05\t/w/batch-1.log\t-"], "M05")
        self.assertIn("/w/batch-1.log", out)
        self.assertNotIn("\n%s           -" % (" " * 16), out)

    def test_no_line_at_all_SAYS_SO_and_names_the_work_dir(self):
        out = self.show(["OTHER\t/w/batch-1.log\t-"], "M05")
        self.assertIn("evidence: NONE", out)
        self.assertIn(str(self.dir), out)

    def test_an_empty_evidence_file_says_the_same_thing(self):
        out = self.show([], "M05")
        self.assertIn("evidence: NONE", out)


class DropBundleTests(unittest.TestCase):
    """CLAUDE.md's rm -rf rail: the path is PROVED, not assumed.

    `case`'s `*` crosses `/`, so `"$WORK"/*.xcresult` on its own is matched by
    `$WORK/../../x.xcresult`. These drive the shipped function text — not a
    re-typing of it — with arguments that climb out, and assert the target
    SURVIVES.
    """

    def setUp(self):
        self.dir = scratch_dir("playhead-mv-drop.")
        self.addCleanup(shutil.rmtree, str(self.dir), True)
        self.work = self.dir / "work"
        self.work.mkdir()

    def drop(self, path, work=None):
        script = "%s\nWORK=%s\ndrop_bundle %s\n" % (
            shell_function("drop_bundle"),
            "'%s'" % (self.work if work is None else work),
            "'%s'" % path)
        proc = subprocess.run(["bash", "-uo", "pipefail", "-c", script],
                              capture_output=True, text=True)
        return proc

    def test_a_bundle_under_work_is_removed_and_the_removal_is_announced(self):
        target = self.work / "batch-7.xcresult"
        target.mkdir()
        proc = self.drop(target)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertFalse(target.exists())
        self.assertIn("dropped the .xcresult bundle", proc.stdout)

    def test_a_path_that_climbs_out_with_dotdot_is_refused(self):
        outside = self.dir / "outside.xcresult"
        outside.mkdir()
        proc = self.drop(str(self.work) + "/../outside.xcresult")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertTrue(outside.exists(), "drop_bundle climbed out of $WORK")
        self.assertIn("refusing", proc.stderr)

    def test_a_nested_path_under_work_is_refused(self):
        nested = self.work / "sub"
        nested.mkdir()
        target = nested / "x.xcresult"
        target.mkdir()
        proc = self.drop(target)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertTrue(target.exists())

    def test_a_path_outside_work_entirely_is_refused(self):
        other = self.dir / "other.xcresult"
        other.mkdir()
        proc = self.drop(other)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertTrue(other.exists())

    def test_an_empty_WORK_removes_nothing(self):
        # `WORK=` makes the prefix pattern `/*.xcresult`, which matches every
        # absolute path on the box.
        target = self.dir / "x.xcresult"
        target.mkdir()
        proc = self.drop(target, work="")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertTrue(target.exists())

    def test_a_bundle_that_is_not_there_is_not_an_error_and_claims_nothing(self):
        proc = self.drop(self.work / "batch-9.xcresult")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertNotIn("dropped", proc.stdout)

    def test_every_call_site_passes_a_bare_name_under_work(self):
        # ANTI-VACUITY, and the half the rails above cannot see: the guard is
        # only worth having if the CALLERS stay inside it. Both do, and the
        # batch component is numeric in all 1,109 MUTATIONS records.
        text = BATTERY.read_text(encoding="utf-8")
        calls = re.findall(r"^\s*drop_bundle (.+)$", text, re.M)
        self.assertEqual(sorted(set(calls)), ['"$BASE_BUNDLE"', '"$BUNDLE"'])
        for var, literal in (("BASE_BUNDLE", '"$WORK/baseline.xcresult"'),
                             ("BUNDLE", '"$WORK/batch-$b.xcresult"')):
            self.assertIn("%s=%s" % (var, literal), text)
        records = re.search(r"^MUTATIONS=\(\n(.*?)^\)\n", text, re.S | re.M).group(1)
        batches = [line.strip().strip('"').split("|")[1]
                   for line in records.split("\n")
                   if line.strip() and not line.strip().startswith("#")
                   and len(line.strip().strip('"').split("|")) > 1]
        self.assertGreater(len(batches), 1000)
        self.assertEqual([b for b in batches if not b.isdigit()], [])


# ---------------------------------------------------------------------------
# The shell half — the real battery, a stubbed gate
# ---------------------------------------------------------------------------
_SANDBOX = {}


#: EVERY throwaway directory this module makes goes under `/private/tmp` with
#: the `playhead-` prefix, and that is not cosmetic — playhead-gjlp0 R3.
#:
#: `tempfile.mkdtemp()` defaults to `$TMPDIR`, which NOTHING in this repo
#: reaps: `scripts/disk-cleanup.sh` sweeps `.worktrees/*`, `/private/tmp/
#: playhead-*` and `$TMPDIR/Deleting-*`, and that is the whole list. The
#: sandbox is a ~67 MiB copy of the tree and it is dropped by
#: `tearDownModule`, which does not run when a pass is INTERRUPTED — a Ctrl-C,
#: a timeout, a killed harness. Measured at R3: **10 orphaned sandboxes, 670
#: MiB**, all from one earlier day of review rounds, with no sweeper that could
#: ever have found them. That is the accumulation R2 fixed one layer down when
#: it made these rails reap their own `$WORK`, and this is the same defect in
#: the rails' own scaffolding: a box whose gate REFUSES below 13.5 GiB, and a
#: new reservoir nobody was counting.
#:
#: `mutation-battery.sh` already made exactly this choice for `$WORK` and says
#: so in a comment. Following it costs one keyword and buys the existing 3-day
#: cron, rather than a second cleaner that drifts from the first.
SCRATCH_ROOT = "/private/tmp"


def scratch_dir(prefix):
    """A throwaway directory the existing disk cleaner can actually find."""
    return pathlib.Path(tempfile.mkdtemp(prefix=prefix, dir=SCRATCH_ROOT))


def battery_sandbox():
    """A throwaway git checkout of the tree, built once and reused.

    The real `mutation-battery.sh` needs every file in MUTABLE_FILES to exist
    and be tracked (`require_clean_tree`, `HASHES_BEFORE`, `git checkout --`),
    and needs a mutation's anchor to match its real source exactly once. A
    `git archive` is ~53 MB and takes about a second; hand-picking files would
    be a SECOND registration of MUTABLE_FILES, which is exactly the drift the
    battery's own preflight exists to catch.
    """
    if "dir" in _SANDBOX:
        return _SANDBOX["dir"]
    root = scratch_dir("playhead-mb-sandbox.").resolve()
    # The WORKING TREE, not `git archive HEAD`. A rail that copies the last
    # commit tests the code you have already shipped: the first run of these
    # rails passed a sandbox built from HEAD while the fix under test sat
    # uncommitted, and every one of them failed on a function the working tree
    # had and the archive did not.
    listing = subprocess.run(["git", "ls-files", "-z"], cwd=str(ROOT),
                             stdout=subprocess.PIPE, check=True).stdout
    for raw in listing.split(b"\0"):
        if not raw:
            continue
        rel = raw.decode("utf-8")
        src = ROOT / rel
        if not src.is_file():        # a submodule gitlink, or a deleted file
            continue
        dst = root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src), str(dst))
    env = dict(os.environ, GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_SYSTEM="/dev/null")
    for cmd in (["git", "init", "-q", "-b", "main"],
                ["git", "config", "user.email", "t@example.com"],
                ["git", "config", "user.name", "t"],
                ["git", "add", "-A"],
                ["git", "commit", "-qm", "base"]):
        subprocess.run(cmd, cwd=str(root), check=True, env=env, capture_output=True)
    _SANDBOX["dir"] = root
    return root


def drop_sandbox():
    root = _SANDBOX.pop("dir", None)
    if root:
        shutil.rmtree(str(root), ignore_errors=True)
    _LISTING.clear()


#: `--list` output per root. It costs 14.0 s — the battery walks all 1,109
#: records through two command substitutions apiece before printing anything —
#: and six places here asked for it independently. Cached rather than
#: hand-inlined: the whole point of resolving an expectation from `--list` is
#: that a rail naming a test the table no longer has must fail loudly.
_LISTING = {}


def battery_listing(root):
    key = str(root)
    if key not in _LISTING:
        _LISTING[key] = subprocess.run(
            ["bash", "scripts/mutation-battery.sh", "--list"],
            cwd=key, capture_output=True, text=True, check=True).stdout
    return _LISTING[key]


def expectations_of(root, mutation):
    """One mutation's expectation list, from the battery's own `--list`."""
    seen = False
    for line in battery_listing(root).split("\n"):
        if re.match(r"^%s\s" % re.escape(mutation), line):
            seen = True
            continue
        if seen and "expects:" in line:
            return line.split("expects:", 1)[1].strip().split(";")
    raise AssertionError("could not resolve %s's expectation from --list; "
                         "the rail below would test nothing" % mutation)


STUB_GATE = r"""#!/bin/bash
# Stubbed fast-gate for playhead-gjlp0's shell rails. It writes a canned console
# log to stdout — which is what mutation-battery.sh captures — and, when asked,
# a canned .xcresult directory at $PLAYHEAD_RESULT_BUNDLE. It runs no build and
# touches no simulator.
N=0
[ -f "$MB_STUB_DIR/count" ] && N="$(cat "$MB_STUB_DIR/count")"
N=$((N + 1))
echo "$N" >"$MB_STUB_DIR/count"
if [ "$N" -eq 1 ]; then
  cat "$MB_STUB_DIR/baseline.log"
else
  cat "$MB_STUB_DIR/batch.log"
fi
# Record whether the bundle path reached us. A rail asserts on this file rather
# than on the battery's own output, because argument plumbing is the property
# under test and the battery could report a bundle it never passed.
if [ -n "${PLAYHEAD_RESULT_BUNDLE:-}" ]; then
  echo "$PLAYHEAD_RESULT_BUNDLE" >>"$MB_STUB_DIR/bundle-paths"
  # MB_STUB_BUNDLE_FROM lets a rail withhold the bundle for the first N-1 calls
  # — i.e. give the BASELINE none and every batch one. Until playhead-gjlp0 R5
  # no rail could build that shape, so the mixed run was outside the population
  # the console-only rails measured, and the SURVIVED epilogue's claim about
  # where those verdicts came from was never driven against it.
  if [ -f "$MB_STUB_DIR/make-bundle" ] && [ "$N" -ge "${MB_STUB_BUNDLE_FROM:-1}" ]; then
    mkdir -p "$PLAYHEAD_RESULT_BUNDLE"
    : >"$PLAYHEAD_RESULT_BUNDLE/Info.plist"
  fi
fi
exit "${MB_STUB_RC:-65}"
"""


def tearDownModule():
    # Module-level rather than per-class: two classes drive the battery and the
    # sandbox is a ~53 MB copy of the tree. Dropping it in one class's
    # tearDownClass would make the other rebuild it.
    drop_sandbox()


@unittest.skipUnless(shutil.which("git"), "git is required")
class ShellBatteryHarness(unittest.TestCase):
    """The machinery for driving the real battery. Carries no rails itself.

    Subclasses set `MUTATION`; `setUpClass` resolves that mutation's expectation
    FROM THE BATTERY'S OWN `--list` rather than hard-coding it, so a rail that
    names an expectation the table no longer has fails loudly instead of
    quietly testing nothing.
    """

    MUTATION = None
    #: Most rails here drive a mutation with ONE expectation and would silently
    #: stop exercising the ladder if it grew a second, so the default is to
    #: refuse. `ShellMultiExpectationTests` sets it, because the `;` shape is
    #: what it is for — 419 of the 1,109 records carry it and until
    #: playhead-gjlp0 R2 no shell rail drove one.
    ALLOW_MULTI = False

    @classmethod
    def setUpClass(cls):
        cls.root = battery_sandbox()
        # Resolved from the battery itself rather than hard-coded: a rail that
        # names a test the battery no longer expects would quietly stop
        # exercising the ladder.
        cls.expects = expectations_of(cls.root, cls.MUTATION)
        cls.expect = ";".join(cls.expects)
        if len(cls.expects) > 1 and not cls.ALLOW_MULTI:
            raise AssertionError("%s now has several expectations; these rails assume one"
                                 % cls.MUTATION)

    def setUp(self):
        self.stub = scratch_dir("playhead-mb-stub.")
        self.addCleanup(shutil.rmtree, str(self.stub), True)
        self.gate = self.root / "scripts" / "fast-gate.sh"
        original = self.gate.read_bytes()
        self.addCleanup(self.gate.write_bytes, original)
        self.gate.write_text(STUB_GATE, encoding="utf-8")
        self.gate.chmod(0o755)

    def stub_xcresult(self, *payloads):
        """Make the stubbed gate write a real bundle DIRECTORY, and shadow
        `xcrun` so the scorer can actually read it — playhead-gjlp0 R2.

        Until this existed, the ONLY shell rail that made a bundle at all was
        the one asserting an unreadable bundle is refused, so no end-to-end
        rail ever took a verdict from a bundle. That is the bead's entire
        verdict source: `mutation_verdict.py` reads it, `fast-gate.sh` writes
        it, and the whole argument for the fix is that the console cannot be
        trusted with a verdict. A Python rail cannot see the plumbing and the
        shell rails could not see the bundle, so between them the shipped path
        was measured by nothing.

        Payloads are answered IN ORDER, one per `xcresulttool` call — the
        baseline's read is the first, each batch's the next — and the last one
        repeats, so a rail that does not care about later batches may pass one.

        Returns the env overlay to hand to `run_battery`.
        """
        import json
        (self.stub / "make-bundle").write_text("y", encoding="utf-8")
        for i, payload in enumerate(payloads, 1):
            (self.stub / ("xcresult-%d.json" % i)).write_text(
                json.dumps(payload), encoding="utf-8")
        binned = self.stub / "bin"
        binned.mkdir(exist_ok=True)
        (binned / "xcrun").write_text(
            "#!/bin/bash\n"
            "# playhead-gjlp0 R2 rail stub. The battery itself never shells out\n"
            "# to xcrun (checked), so shadowing it reaches only the scorer.\n"
            'N=0\n'
            '[ -f "%(d)s/xcrun-n" ] && N="$(cat "%(d)s/xcrun-n")"\n'
            'N=$((N + 1)); echo "$N" >"%(d)s/xcrun-n"\n'
            'F="%(d)s/xcresult-$N.json"\n'
            '[ -f "$F" ] || F="%(d)s/xcresult-%(last)d.json"\n'
            'exec /bin/cat "$F"\n' % {"d": self.stub, "last": len(payloads)},
            encoding="utf-8")
        (binned / "xcrun").chmod(0o755)
        return {"PATH": str(binned) + os.pathsep + os.environ["PATH"]}

    def run_battery(self, baseline_log, batch_log, args=None, env=None):
        (self.stub / "baseline.log").write_text(baseline_log, encoding="utf-8")
        (self.stub / "batch.log").write_text(batch_log, encoding="utf-8")
        env = dict(os.environ, MB_STUB_DIR=str(self.stub),
                   GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_SYSTEM="/dev/null",
                   **(env or {}))
        proc = subprocess.run(
            ["bash", "scripts/mutation-battery.sh"] + list(args or ["--only", self.MUTATION]),
            cwd=str(self.root), capture_output=True, text=True, env=env, timeout=600)
        # Checked after EVERY run rather than once at the end: a rail that
        # leaves a mutant in the sandbox corrupts every rail after it, which is
        # the failure the battery's own `restore_and_verify` exists to catch.
        status = subprocess.run(["git", "status", "--porcelain", "--", "Playhead"],
                                cwd=str(self.root), capture_output=True, text=True,
                                env=env).stdout
        self.assertEqual(status.strip(), "",
                         "the sandbox kept an injected mutant:\n" + status)
        self._reap_work_dir(proc)
        return proc

    @staticmethod
    def _reap_work_dir(proc):
        """Remove the $WORK the battery kept, RECORDING WHAT WAS IN IT FIRST.

        `proc.work_dir_files` is the listing, so a rail can assert on what a run
        LEAVES BEHIND — which is a quantity no other rail here can reach, and
        the one playhead-gjlp0 R4's disk findings are about. Without it the only
        evidence of what the run kept is the sentence the run prints about
        itself, and a sentence about a directory is exactly the kind of claim
        this bead keeps finding printed without checking.

        Every rail here deliberately produces a non-KILL outcome, and the
        battery sets KEEP_WORK=1 on exactly those — so a full pass leaves a
        dozen directories under /private/tmp. That is the accumulation
        playhead-8cjo measured (52 of them, 684 MiB, from four beads), and a
        rail that adds to it while testing the fix for it is not funny. The path
        is taken from the battery's OWN line and re-checked against the prefix
        before anything is removed.
        """
        proc.work_dir_files = None
        for line in (proc.stdout + proc.stderr).split("\n"):
            if "logs kept in " not in line:
                continue
            kept = line.split("logs kept in ", 1)[1].strip()
            if kept.startswith("/private/tmp/playhead-mutation-battery."):
                try:
                    proc.work_dir_files = sorted(os.listdir(kept))
                except OSError:
                    proc.work_dir_files = None
                shutil.rmtree(kept, ignore_errors=True)

    def out(self, proc):
        return proc.stdout + "\n" + proc.stderr

    def verdict_of(self, proc, name=None):
        """The word in the RESULTS table's VERDICT column for one mutation.

        Parsed rather than grepped: the VOID advice block contains the words
        KILLED and SURVIVED in prose, so `assertNotIn("KILLED", out)` passes or
        fails for reasons that have nothing to do with the verdict. That is the
        rail committing the defect it exists to catch.
        """
        want = name or self.MUTATION
        for line in self.out(proc).split("\n"):
            m = re.match(r"^(%s)\s+(KILLED|SURVIVED|VOID|ERROR|DRY-RUN)\b" % re.escape(want),
                         line)
            if m:
                return m.group(2)
        return None


class ShellLadderTests(ShellBatteryHarness):
    """Drives the real `scripts/mutation-battery.sh`. No xcodebuild, no build.

    Every rail here is a property the old script could not have satisfied: it
    had no VOID verdict, no exit code 5, no freshness floor, no evidence line
    and no baseline check on whether an expectation was JUDGED.
    """

    MUTATION = "M05"

    # -- THE DEMONSTRATION -------------------------------------------------
    def test_a_crash_looping_batch_reports_VOID_not_SURVIVED(self):
        green = console(tests=[(self.expect, "passed")])
        crashed = console(pids=("4001", "4002", "4003"),
                          tests=[(self.expect, "started")],
                          summary="✔ Test run with 29 tests in 8 suites passed after 0.6 s.",
                          terminal="** TEST FAILED **", restarts=2)
        proc = self.run_battery(green, crashed)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "VOID", out[-4000:])
        self.assertIn("A VOID IS NOT A VERDICT", out)
        self.assertEqual(proc.returncode, 5, out[-4000:])

    def test_the_same_batch_under_the_old_reading_says_SURVIVED(self):
        # The other half of the demonstration, so "new says VOID" is measured
        # against something rather than asserted into the void.
        crashed = console(pids=("4001", "4002", "4003"),
                          tests=[(self.expect, "started")],
                          terminal="** TEST FAILED **", restarts=2)
        self.assertEqual(old_battery_verdict(crashed, self.expect), "SURVIVED")

    def test_a_started_but_unjudged_test_on_a_HEALTHY_batch_is_also_VOID(self):
        # The per-test arm, distinct from the batch arm: one host, a terminal
        # marker, and the expected test still never reported.
        green = console(tests=[(self.expect, "passed")])
        silent = console(tests=[(self.expect, "started"), ("other", "passed")])
        proc = self.run_battery(green, silent)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "VOID", out[-4000:])
        self.assertIn("reached NO VERDICT", out, out[-4000:])
        self.assertEqual(proc.returncode, 5, out[-4000:])
        # playhead-gjlp0 R4, pinned here rather than in a rail of its own
        # because the epilogue is printed on EVERY void and this run already
        # has one. R4 added a batch reason whose remedy is the OPPOSITE of the
        # epilogue's ("re-running will not change it"), so the epilogue stopped
        # being unconditionally true the moment that arm landed — two
        # contradictory instructions in one output, which is what R2 closed for
        # the failure count and the failure list.
        #
        # R5 found a SECOND shape a re-run cannot clear — an expectation the
        # mutation itself made SKIP — so the exemption is no longer about batch
        # `#reason` lines alone. This rail holds the general sentence; the
        # [SKIPPED] half is pinned on the one run that produces one, in
        # ShellConsoleOnlyVoidTests.
        self.assertIn("UNLESS THE ROW SAYS OTHERWISE", out, out[-4000:])
        self.assertIn("TWO shapes do", out, out[-4000:])

    def test_a_positive_pass_still_reports_SURVIVED_and_names_its_log(self):
        green = console(tests=[(self.expect, "passed")])
        proc = self.run_battery(green, green)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "SURVIVED", out[-4000:])
        self.assertIn("still green:", out)
        self.assertRegex(out, r"evidence: \S*batch-\d+\.log")
        self.assertEqual(proc.returncode, 1, out[-4000:])

        # --- playhead-gjlp0 R4: WHAT THE RUN KEEPS -------------------------
        # Asserted on THIS run rather than in a class of its own, deliberately:
        # every battery invocation costs ~12-15 s of fork-per-record preflight,
        # and a rail whose whole subject is disk cost should not spend one to
        # re-create a kept work directory this rail already has. It needs a
        # SURVIVED, which is what makes `KEEP_WORK=1`, so this is the run.
        #
        # TWO PROPERTIES OF THE DIRECTORY A KEPT RUN LEAVES BEHIND.
        #
        # 1. NO `.log.last`. That file was `last_attempt`'s output and had three
        #    readers; this bead deleted two of them and left a BYTE-EXACT
        #    duplicate of the largest artifact in $WORK being written once per
        #    batch so one `grep -q` could read it. Measured on a preserved
        #    37-batch run: 178.9 MiB of `.log` beside 178.9 MiB of `.log.last`,
        #    exactly half the directory (MiB, not MB — R5 re-derived it as
        #    187,563,745 bytes). It is a predicate now.
        # 2. THE KEPT LINE COUNTS WHAT IS ACTUALLY THERE. It used to say
        #    "per-batch xcodebuild logs", full stop, while the directory had
        #    started holding an `.xcresult` per non-KILL batch — the biggest
        #    thing in it. A category where a measurement belongs.
        files = proc.work_dir_files
        self.assertIsNotNone(files, "the run kept no work dir to measure:\n" + out[-2000:])
        self.assertEqual([f for f in files if f.endswith(".last")], [],
                         "a duplicate of the batch log is being kept: %r" % (files,))
        bundles = [f for f in files if f.endswith(".xcresult")]
        stated = re.search(r"including (\d+) \.xcresult bundle\(s\)", out)
        self.assertIsNotNone(stated, "the kept-work line states no bundle count:\n"
                             + out[-2000:])
        self.assertEqual(int(stated.group(1)), len(bundles),
                         "the line claims %s bundles and the directory holds %d: %r"
                         % (stated.group(1), len(bundles), files))
        # And a size, because "logs kept in <path>" is what a reader deciding
        # whether to reclaim had to go on.
        self.assertRegex(out, r"mutation-battery:\s+\S+ in it, including")

    def test_a_real_failure_still_reports_KILLED(self):
        green = console(tests=[(self.expect, "passed")])
        red = console(tests=[(self.expect, "issue")], terminal="** TEST FAILED **")
        proc = self.run_battery(green, red)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "KILLED", out[-4000:])
        self.assertEqual(proc.returncode, 0, out[-4000:])

    def test_a_KILL_on_a_batch_that_lost_its_host_is_VOID_too(self):
        # A crashing host reddens tests for reasons that have nothing to do
        # with the mutation (playhead-4xmz's DW18), so the batch outranks even
        # a FAILED reading. This is the arm most likely to be argued with.
        green = console(tests=[(self.expect, "passed")])
        red = console(pids=("1", "2"), tests=[(self.expect, "issue")],
                      terminal="** TEST FAILED **", restarts=1)
        proc = self.run_battery(green, red)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "VOID", out[-4000:])
        self.assertEqual(proc.returncode, 5, out[-4000:])

    def test_a_baseline_that_never_judges_the_expectation_refuses_to_run(self):
        silent = console(tests=[(self.expect, "started")])
        proc = self.run_battery(silent, silent)
        out = self.out(proc)
        self.assertIn("RAN and was never JUDGED", out, out[-4000:])
        self.assertEqual(proc.returncode, 2, out[-4000:])

    def test_a_void_baseline_refuses_and_does_not_implicate_the_tree(self):
        crashed = console(pids=("1", "2"), tests=[(self.expect, "passed")], restarts=1)
        proc = self.run_battery(crashed, crashed)
        out = self.out(proc)
        self.assertIn("THE BASELINE BATCH IS VOID", out, out[-4000:])
        self.assertIn("not a claim about the tree", out)
        self.assertEqual(proc.returncode, 2, out[-4000:])

    def test_a_void_baseline_whose_reason_says_do_not_re_run_is_not_told_to_re_run(self):
        """playhead-gjlp0 R5 — R4's fix, in the arm R4 did not reach.

        R4 qualified the BATCH epilogue's "run it again" so it defers to a
        stated reason. The BASELINE arm said `the suites were never judged.
        Re-run.` flat, two lines under a reason that can read `RE-RUNNING WILL
        NOT CHANGE IT`. Driven, with a baseline bundle carrying an unrecognised
        result string: both sentences were printed, in that order.

        "The suites were never judged" was false here as well — they WERE
        judged, in a word this parser cannot read.
        """
        green = console(tests=[(self.expect, "passed")])
        env = self.stub_xcresult(bundle_payload([(self.expect, "Bikeshed", [])]))
        proc = self.run_battery(green, green, env=env)
        out = self.out(proc)
        self.assertIn("THE BASELINE BATCH IS VOID", out, out[-4000:])
        self.assertIn("RE-RUNNING WILL NOT CHANGE IT", out, out[-4000:])
        self.assertIn("READ THE REASON(S) ABOVE FIRST", out, out[-4000:])
        self.assertIn("XCRESULT_", out, out[-4000:])
        self.assertNotIn("suites were never judged", out, out[-4000:])
        self.assertEqual(proc.returncode, 2, out[-4000:])

    def test_a_red_baseline_still_refuses_on_a_test_no_mutation_names(self):
        # The failure list the baseline reads now comes out of the scorer's
        # `#failure` lines, so this rail pins that it still sees EVERY failure
        # and not merely the expected ones.
        red = console(tests=[(self.expect, "passed"), ("somebody else", "issue")],
                      terminal="** TEST FAILED **")
        proc = self.run_battery(red, red)
        out = self.out(proc)
        self.assertIn("RED before any mutation", out, out[-4000:])
        self.assertIn("somebody else", out)
        self.assertEqual(proc.returncode, 2, out[-4000:])

    def test_the_bundle_path_reaches_fast_gate(self):
        # Argument plumbing, which is the half a Python test cannot see.
        green = console(tests=[(self.expect, "passed")])
        self.last_proc = self.run_battery(green, green)
        paths = (self.stub / "bundle-paths").read_text(encoding="utf-8").split()
        self.assertTrue(paths, "PLAYHEAD_RESULT_BUNDLE never reached fast-gate")
        # The evidence line must not name a bundle that does not exist: the
        # stub writes none here, so the battery must record "-".
        self.assertNotRegex(self.out(self.last_proc), r"evidence:.*\n\s+\S+\.xcresult")
        self.assertTrue(any(p.endswith("baseline.xcresult") for p in paths), paths)
        self.assertTrue(any(re.search(r"batch-\d+\.xcresult$", p) for p in paths), paths)

    def test_an_unreadable_bundle_refuses_rather_than_falling_back(self):
        # The stub writes a directory that is not a real .xcresult, so
        # xcresulttool fails. The battery must REFUSE — a silent console
        # fallback is the defect playhead-t53a removed from the gate.
        (self.stub / "make-bundle").write_text("y", encoding="utf-8")
        green = console(tests=[(self.expect, "passed")])
        proc = self.run_battery(green, green)
        out = self.out(proc)
        self.assertIn("CANNOT EVALUATE", out, out[-4000:])
        self.assertIn("could not be READ", out)
        self.assertEqual(proc.returncode, 2, out[-4000:])

    def test_a_stale_batch_log_is_refused_by_name_and_date(self):
        # Reproduces the collision playhead-8cjo measured: `batch-1414.log`
        # exists twice on this box, three days apart. The battery drives the
        # scorer with a floor captured immediately before the run, so an
        # artifact from an earlier run cannot be scored.
        green = console(tests=[(self.expect, "passed")])
        stale = self.stub / "batch-1414.log"
        stale.write_text(green, encoding="utf-8")
        old = time.time() - 3 * 86400
        os.utime(str(stale), (old, old))
        names = self.stub / "names.txt"
        names.write_text(self.expect + "\n", encoding="utf-8")
        proc = subprocess.run(
            ["python3", "scripts/mutation_verdict.py", "classify",
             "--log", str(stale), "--rc", "65", "--since", str(int(time.time())),
             "--names", str(names), "--out", str(self.stub / "o.txt")],
            cwd=str(self.root), capture_output=True, text=True)
        self.assertEqual(proc.returncode, mv.EXIT_CANNOT_EVALUATE,
                         proc.stdout + proc.stderr)
        self.assertIn("STALE", proc.stderr)
        self.assertIn("batch-1414.log", proc.stderr)


# A scorer that writes a well-formed outcomes file MINUS the `#failures` header
# — i.e. the two halves of this bead disagreeing about their own file format.
STUB_SCORER = r"""import sys
args = dict(zip(sys.argv[1:-1:1], sys.argv[2::1]))
out = args["--out"]
names = open(args["--names"], encoding="utf-8").read().split("\n")
lines = ["#batch\tOK", "#source\tstub", "#log\t" + args["--log"],
         "#bundle\t(none)", "#hosts\t1", "#no_verdict\t0"]
for n in names:
    if n:
        lines.append("PASSED\t" + n)
open(out, "w", encoding="utf-8").write("\n".join(lines) + "\n")
print("  stub scorer: wrote", out)
"""


class ShellEmptyExpectationTests(ShellBatteryHarness):
    """playhead-ngsm. An entry whose expectation is EMPTY used to be reported
    KILLED: the ladder iterates the expected-test list, zero iterations leave
    `missing` empty, and the empty-`missing` arm is the KILL arm. A vacuity
    control written that way was credited as a working rail. The record is
    injected into the script IN PLACE (the gate stub above sets the precedent)
    and restored by cleanup; the battery must refuse it before the lock, the
    baseline and any edit.
    """

    MUTATION = "M05"

    def _inject_empty_expectation(self):
        script = self.root / "scripts" / "mutation-battery.sh"
        original = script.read_bytes()
        self.addCleanup(script.write_bytes, original)
        text = original.decode("utf-8")
        marker = "MUTATIONS=(\n"
        self.assertIn(marker, text)
        text = text.replace(marker, marker + '  "NGSM99|9999|RUNNER|"\n', 1)
        script.write_text(text, encoding="utf-8")

    def test_an_EMPTY_expectation_is_refused_at_load_time_not_credited_KILLED(self):
        self._inject_empty_expectation()
        green = console(tests=[(self.expect, "passed")])
        proc = self.run_battery(green, green, args=["--only", "NGSM99"])
        out = self.out(proc)
        self.assertEqual(proc.returncode, 3, out[-4000:])
        self.assertIn("EMPTY expectation", out)
        # The VERDICT, not the word: the refusal text itself explains what a
        # zero-iteration ladder would have credited.
        self.assertNotIn("NGSM99|KILLED", out)
        self.assertNotIn("|KILLED|", out)
        self.assertNotIn("All mutations killed", out)

    def test_a_record_WITH_an_expectation_is_not_refused_by_the_new_guard(self):
        # Anti-vacuity: the guard must not fire on the real table. M05 runs to
        # a verdict exactly as before.
        green = console(tests=[(self.expect, "passed")])
        proc = self.run_battery(green, green)
        self.assertNotIn("EMPTY expectation", self.out(proc))


class ShellInstrumentFaultTests(ShellBatteryHarness):
    """The battery and the scorer disagreeing about their own file format.

    `scored_field` prints "" for a MISSING field and for a field that says
    nothing, and the baseline's test read `""` as NOT-ZERO — i.e. as a RED
    TREE, printed with no failures under it and a remedy ("fix the tree") that
    nobody can act on. An instrument that went quiet must not be able to make a
    claim about the codebase.
    """

    MUTATION = "M05"

    def test_a_scorer_that_omits_the_failure_count_blames_the_INSTRUMENT(self):
        scorer = self.root / "scripts" / "mutation_verdict.py"
        original = scorer.read_bytes()
        self.addCleanup(scorer.write_bytes, original)
        scorer.write_text(STUB_SCORER, encoding="utf-8")
        green = console(tests=[(self.expect, "passed")])
        proc = self.run_battery(green, green)
        out = self.out(proc)
        self.assertIn("fault in the INSTRUMENT", out, out[-4000:])
        self.assertNotIn("are RED before any mutation", out)
        self.assertEqual(proc.returncode, 2, out[-4000:])

    def test_a_scorer_that_CRASHES_on_a_batch_is_ERROR_and_names_its_log(self):
        # The batch-level `*)` arm — "the batch could not be SCORED" — is the
        # only branch the fix added that no rail reached. Driven with the
        # baseline skipped, so the failure lands on a BATCH rather than on the
        # preflight, and with a scorer that dies rather than one that refuses.
        scorer = self.root / "scripts" / "mutation_verdict.py"
        original = scorer.read_bytes()
        self.addCleanup(scorer.write_bytes, original)
        scorer.write_text("raise SystemExit('boom: the scorer died')\n",
                          encoding="utf-8")
        green = console(tests=[(self.expect, "passed")])
        proc = self.run_battery(green, green,
                                args=["--only", self.MUTATION],
                                env={"PLAYHEAD_MB_SKIP_BASELINE": "1"})
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "ERROR", out[-4000:])
        self.assertIn("could not be SCORED", out, out[-4000:])
        self.assertRegex(out, r"evidence: \S*batch-\d+\.log")
        self.assertEqual(proc.returncode, 3, out[-4000:])

    def test_a_batch_with_no_failure_count_says_UNKNOWN_not_none(self):
        # `(none)` is a measurement and is earned only by a STATED zero. With
        # the baseline skipped, a batch can reach the observed-failures block
        # with no `#failures` field, and printing `(none)` there would be an
        # absence dressed as a reading.
        scorer = self.root / "scripts" / "mutation_verdict.py"
        original = scorer.read_bytes()
        self.addCleanup(scorer.write_bytes, original)
        scorer.write_text(STUB_SCORER, encoding="utf-8")
        green = console(tests=[(self.expect, "passed")])
        proc = self.run_battery(green, green,
                                args=["--only", self.MUTATION],
                                env={"PLAYHEAD_MB_SKIP_BASELINE": "1"})
        out = self.out(proc)
        self.assertIn("observed failures: UNKNOWN", out, out[-4000:])
        self.assertNotIn("(none)", out, out[-4000:])

    def test_a_COUNT_that_disagrees_with_the_LIST_says_so_and_never_says_none(self):
        # THE MIRROR of the rail above (playhead-gjlp0 R2). R1 armed "the
        # scorer stated NO count"; a count of N over an EMPTY list printed the
        # header saying N and then `(none)` — two contradictory readings of one
        # file, with nothing to tell a reader which to believe, and `(none)` is
        # a measurement earned by a STATED ZERO alone.
        scorer = self.root / "scripts" / "mutation_verdict.py"
        original = scorer.read_bytes()
        self.addCleanup(scorer.write_bytes, original)
        scorer.write_text(
            STUB_SCORER.replace('"#no_verdict\\t0"]', '"#no_verdict\\t0", "#failures\\t2"]'),
            encoding="utf-8")
        green = console(tests=[(self.expect, "passed")])
        proc = self.run_battery(green, green,
                                args=["--only", self.MUTATION],
                                env={"PLAYHEAD_MB_SKIP_BASELINE": "1"})
        out = self.out(proc)
        self.assertIn("observed failures (ALL of them, 2)", out, out[-4000:])
        self.assertIn("THE COUNT AND THE LIST DISAGREE", out, out[-4000:])
        self.assertNotIn("(none)", out, out[-4000:])

    def test_a_scorer_that_omits_the_resource_count_blames_the_INSTRUMENT(self):
        # playhead-gjlp0 R3 added `#resource` to the outcomes-file format, and
        # a field the baseline REQUIRES has to be guarded in the same direction
        # as `#failures`: `scored_field` prints "" for a missing field, and the
        # numeric test would read "" as NOT ZERO — a denial count invented out
        # of an absence. This stub states `#failures 0` so the guard above is
        # satisfied and only the new one can fire.
        scorer = self.root / "scripts" / "mutation_verdict.py"
        original = scorer.read_bytes()
        self.addCleanup(scorer.write_bytes, original)
        scorer.write_text(
            STUB_SCORER.replace('"#no_verdict\\t0"]',
                                '"#no_verdict\\t0", "#failures\\t0"]'),
            encoding="utf-8")
        green = console(tests=[(self.expect, "passed")])
        proc = self.run_battery(green, green)
        out = self.out(proc)
        self.assertIn("no usable '#resource' count", out, out[-4000:])
        self.assertNotIn("baseline green", out, out[-4000:])
        self.assertEqual(proc.returncode, 2, out[-4000:])

    def test_the_same_stub_WITH_the_count_gets_through_the_guard(self):
        # ANTI-FABRICATION: the rails above must fail for the MISSING FIELD and
        # not merely because the scorer was replaced. Same stub plus the two
        # lines, and the run reaches a verdict.
        #
        # `#resource\t0` joined this stub at R3, when the field became part of
        # the format both halves must agree about. That is the guard working
        # rather than a rail being loosened to fit: a scorer that does not
        # write it can no longer license a baseline.
        scorer = self.root / "scripts" / "mutation_verdict.py"
        original = scorer.read_bytes()
        self.addCleanup(scorer.write_bytes, original)
        scorer.write_text(
            STUB_SCORER.replace('"#no_verdict\\t0"]',
                                '"#no_verdict\\t0", "#failures\\t0", "#resource\\t0"]'),
            encoding="utf-8")
        green = console(tests=[(self.expect, "passed")])
        proc = self.run_battery(green, green)
        out = self.out(proc)
        self.assertNotIn("fault in the INSTRUMENT", out, out[-4000:])
        self.assertIn("baseline green", out, out[-4000:])
        self.assertEqual(self.verdict_of(proc), "SURVIVED", out[-4000:])


#: One test denied a resource, spelled the way a real log spells it. The prose
#: match (`unable to open database file`) is the one `gate_baseline` keeps
#: because `AnalysisStore` throws `sqlite3_system_errno()` away before it logs
#: — see playhead-enzva — so this is the shape that actually reaches a console.
DENIED_TEST = ('◇ Test "a rail no mutation names" started.\n'
               '✘ Test "a rail no mutation names" recorded an issue at A.swift:1:1: '
               'Migration failed: unable to open database file\n'
               '✘ Test "a rail no mutation names" failed after 0.1 seconds with 1 issue.')


class ShellDeniedResourceTests(ShellBatteryHarness):
    """A RED TEST THE FAILURE COUNT DOES NOT HOLD — playhead-gjlp0 R3.

    `gate_baseline` lifts a denied resource out of `failures` (playhead-s34ux),
    which is right, and it made this battery stop refusing a baseline that had
    gone red. DRIVEN both ways before these rails were written:

        pre-bead (`git show a82e52a9:scripts/mutation-battery.sh`)
            rc 2, "the focused suites are RED before any mutation", test named
        this bead as it stood
            "baseline green", M05 SURVIVED, `observed failures (ALL of them, 0)`
            followed by `(none)`

    So the first rail here fails against the branch it reviews, and the second
    is the anti-fabrication half: the same console WITHOUT the denial must get
    through, or the rail is measuring the stub rather than the guard.
    """

    MUTATION = "M05"

    def test_a_baseline_with_a_DENIED_test_refuses_instead_of_saying_green(self):
        baseline = console(tests=[(self.expect, "passed")], extra=DENIED_TEST,
                           summary="✔ Test run with 2 tests in 1 suite failed after 1.0 seconds.",
                           terminal="** TEST FAILED **")
        proc = self.run_battery(baseline, baseline)
        out = self.out(proc)
        self.assertIn("DENIED A RESOURCE", out, out[-4000:])
        self.assertIn("a rail no mutation names", out, out[-4000:])
        self.assertNotIn("baseline green", out, out[-4000:])
        # The pre-bead REMEDY was wrong for this cause and must not come back:
        # "fix the tree first" is unactionable when the box denied a descriptor.
        self.assertNotIn("are RED before any mutation", out, out[-4000:])
        self.assertIn("Re-run.", out, out[-4000:])
        self.assertEqual(proc.returncode, 2, out[-4000:])

    def test_the_same_console_WITHOUT_the_denial_reaches_a_verdict(self):
        green = console(tests=[(self.expect, "passed")])
        proc = self.run_battery(green, green)
        out = self.out(proc)
        self.assertNotIn("DENIED A RESOURCE", out, out[-4000:])
        self.assertIn("baseline green", out, out[-4000:])
        self.assertEqual(self.verdict_of(proc), "SURVIVED", out[-4000:])

    def test_a_BATCH_denial_is_named_beside_the_failure_list(self):
        # The batch half. `(none)` under `observed failures (ALL of them, 0)`
        # is a true statement about FAILURES and reads as a clean batch, which
        # is what the ledger in docs/investigations/ is transcribed from. The
        # baseline is skipped so the refusal above is not in the way — which
        # also means the stubbed gate's FIRST answer is the batch's, so both
        # slots carry the same console (the shape every baseline-skipping rail
        # here uses).
        batch = console(tests=[(self.expect, "passed")], extra=DENIED_TEST,
                        summary="✔ Test run with 2 tests in 1 suite failed after 1.0 seconds.",
                        terminal="** TEST FAILED **")
        proc = self.run_battery(batch, batch,
                                args=["--only", self.MUTATION],
                                env={"PLAYHEAD_MB_SKIP_BASELINE": "1"})
        out = self.out(proc)
        self.assertIn("ALSO DENIED A RESOURCE (1)", out, out[-4000:])
        self.assertIn("⚠ a rail no mutation names", out, out[-4000:])
        # It is REPORTED, not fatal: the declared expectation positively passed
        # and a denial elsewhere names the box, so the verdict still stands.
        self.assertEqual(self.verdict_of(proc), "SURVIVED", out[-4000:])


class ShellBundleTests(ShellBatteryHarness):
    """The .xcresult bundle, driven end to end — playhead-gjlp0 R2.

    The bead moved the battery's verdict onto the bundle and no shell rail ever
    read one: the single rail that created a bundle asserted that an UNREADABLE
    one is refused. So "the bundle is the verdict source" was a property of
    Python plus a property of a `case` statement, with nothing joining them —
    the playhead-s34ux shape this module's own header warns about.

    Every rail here makes the CONSOLE say the opposite of the BUNDLE, so a run
    that quietly fell back to the console fails rather than passing for the
    wrong reason.
    """

    MUTATION = "M05"

    def test_the_bundle_OVERRIDES_a_console_pass(self):
        green = console(tests=[(self.expect, "passed")])
        env = self.stub_xcresult(
            bundle_payload([(self.expect, gb.XCRESULT_PASSED, [])]),
            bundle_payload([(self.expect, gb.XCRESULT_FAILED,
                             ["Expectation failed: capped.truncated"])]))
        proc = self.run_battery(green, green, env=env)
        out = self.out(proc)
        self.assertIn("verdicts read from: the .xcresult bundle", out, out[-4000:])
        self.assertEqual(self.verdict_of(proc), "KILLED", out[-4000:])
        self.assertEqual(proc.returncode, 0, out[-4000:])

    def test_a_SURVIVED_rests_on_the_BUNDLES_own_pass_not_the_consoles(self):
        # The direction that decides whether the fix is worth anything: the
        # console shows a FAILURE and the bundle a pass, so a KILLED here would
        # mean the console is still the verdict.
        green = console(tests=[(self.expect, "passed")])
        red = console(tests=[(self.expect, "issue")], terminal="** TEST FAILED **")
        env = self.stub_xcresult(
            bundle_payload([(self.expect, gb.XCRESULT_PASSED, [])]))
        proc = self.run_battery(green, red, env=env)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "SURVIVED", out[-4000:])
        # ...and the epilogue may claim the bundle, because there was one.
        self.assertNotIn("NO .xcresult BUNDLE", out, out[-4000:])
        self.assertNotIn("AND AT LEAST ONE RUN BEHIND THIS TABLE HAD NO", out, out[-4000:])
        self.assertEqual(proc.returncode, 1, out[-4000:])

        # --- playhead-gjlp0 R4: THE KEPT-WORK LINE COUNTS A REAL BUNDLE -----
        # The mirror of the count assertion on `ShellLadderTests`'s console-only
        # SURVIVED, and it is what makes that one non-vacuous: there the answer
        # is 0, which a hard-coded 0 would satisfy. Here a bundle really is kept
        # — this batch is SURVIVED, so `BATCH_KEEP_EVIDENCE=1` and `drop_bundle`
        # is not called — so the line has to have counted something.
        files = proc.work_dir_files
        self.assertIsNotNone(files, "the run kept no work dir:\n" + out[-2000:])
        bundles = [f for f in files if f.endswith(".xcresult")]
        self.assertGreaterEqual(len(bundles), 1,
                                "a SURVIVED batch dropped its bundle: %r" % (files,))
        stated = re.search(r"including (\d+) \.xcresult bundle\(s\)", out)
        self.assertIsNotNone(stated, "the kept-work line states no bundle count:\n"
                             + out[-2000:])
        self.assertEqual(int(stated.group(1)), len(bundles),
                         "the line claims %s bundles and the directory holds %d: %r"
                         % (stated.group(1), len(bundles), files))
        self.assertEqual([f for f in files if f.endswith(".last")], [],
                         "a duplicate of the batch log is being kept: %r" % (files,))

    def test_a_bundle_STATED_crash_is_VOID_and_never_a_KILL(self):
        # playhead-4xmz's DW18: a trapping mutant takes the host down and
        # reddens tests for reasons that have nothing to do with the mutation.
        # The bundle books that as a FAILURE carrying a crash message; reading
        # it as one would write a healthy test into a KILLED column.
        green = console(tests=[(self.expect, "passed")])
        env = self.stub_xcresult(
            bundle_payload([(self.expect, gb.XCRESULT_PASSED, [])]),
            bundle_payload([(self.expect, gb.XCRESULT_FAILED,
                             ["Test crashed with signal trap."])]))
        proc = self.run_battery(green, console(tests=[(self.expect, "started")]),
                                env=env)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "VOID", out[-4000:])
        self.assertIn("STATES the host died", out, out[-4000:])
        self.assertEqual(proc.returncode, 5, out[-4000:])

    def test_a_batch_of_nothing_but_KILLS_drops_its_bundle_and_says_so(self):
        # A focused .xcresult is tens of megabytes on a box whose gate refuses
        # to start below 13.5 GiB, and a bundle that vanishes without a line is
        # a reader concluding the run never wrote one. Both halves are here
        # because neither had an end-to-end rail.
        green = console(tests=[(self.expect, "passed")])
        env = self.stub_xcresult(
            bundle_payload([(self.expect, gb.XCRESULT_PASSED, [])]),
            bundle_payload([(self.expect, gb.XCRESULT_FAILED, ["Expectation failed"])]))
        proc = self.run_battery(green, green, env=env)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "KILLED", out[-4000:])
        dropped = re.findall(r"dropped the \.xcresult bundle .*: (\S+)", out)
        self.assertTrue(any(d.endswith("baseline.xcresult") for d in dropped), out[-4000:])
        self.assertTrue(any(re.search(r"batch-\d+\.xcresult$", d) for d in dropped),
                        out[-4000:])


class ShellConsoleOnlyTests(ShellBatteryHarness):
    """A run scored without a bundle must SAY SO — playhead-gjlp0 R2.

    `score_batch` omits `--xcresult` when the directory is not there, and until
    R2 nothing said so: the only difference between a bundle run and a console
    run was one line of the scorer's census, which reads as a neutral fact
    about provenance. Meanwhile the SURVIVED epilogue asserted, without
    checking, that every verdict above it came from the bundle.
    """

    MUTATION = "M05"

    def test_no_bundle_is_named_per_batch_and_qualifies_the_SURVIVED_epilogue(self):
        green = console(tests=[(self.expect, "passed")])
        proc = self.run_battery(green, green)          # the stub writes no bundle
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "SURVIVED", out[-4000:])
        self.assertIn("NO .xcresult BUNDLE — the baseline", out, out[-4000:])
        self.assertIn("NO .xcresult BUNDLE — batch", out, out[-4000:])
        self.assertIn("AND AT LEAST ONE RUN BEHIND THIS TABLE HAD NO", out, out[-4000:])
        # The claim it replaced. A reader who trusts this sentence would go
        # looking for a bundle that was never written.
        self.assertNotIn("read\nfrom the batch's own .xcresult bundle", out)
        self.assertEqual(proc.returncode, 1, out[-4000:])
        # playhead-gjlp0 R5: the epilogue names WHICH runs, and here it is both.
        # This is the anti-fabrication half of the rail below — a list that said
        # "the baseline" on every run would pass that one and mean nothing.
        self.assertIn("scored off the CONSOLE alone: the baseline, batch", out, out[-4000:])

    def test_a_BASELINE_only_bundle_loss_does_not_claim_the_SURVIVED_came_off_the_console(self):
        """playhead-gjlp0 R5 — R2's own defect, in the sentence R2 wrote to fix it.

        `CONSOLE_ONLY` is one flag set by the baseline AND by every batch, and
        the SURVIVED epilogue said `Those verdicts came off the CONSOLE alone`.
        When it is the BASELINE that lost its bundle and the batches kept
        theirs, that is false: the SURVIVED verdicts came off their own batch's
        bundle. R2 fixed the epilogue's HEADER to name RUNS and left the
        verdict sentence claiming the stronger thing — and it was invisible
        because every console-only rail here drops BOTH bundles, so the mixed
        run was outside the population they measured. The same monoculture
        finding R1 and R2 each made once about the rails.
        """
        green = console(tests=[(self.expect, "passed")])
        env = self.stub_xcresult(bundle_payload([(self.expect, gb.XCRESULT_PASSED, [])]))
        env["MB_STUB_BUNDLE_FROM"] = "2"       # the baseline gets none; batch 1 does
        proc = self.run_battery(green, green, env=env)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "SURVIVED", out[-4000:])
        self.assertIn("NO .xcresult BUNDLE — the baseline", out, out[-4000:])
        self.assertNotIn("NO .xcresult BUNDLE — batch", out, out[-4000:])
        # The list names the baseline and NOTHING else.
        named = [line for line in out.split("\n") if "scored off the CONSOLE alone:" in line]
        self.assertEqual(len(named), 1, out[-4000:])
        self.assertEqual(named[0].split(":", 1)[1].strip(), "the baseline", out[-4000:])
        # And the verdict sentence no longer says these verdicts came off it.
        self.assertNotIn("Those verdicts came off the CONSOLE alone", out)
        self.assertIn("If the only run named is THE BASELINE", out, out[-4000:])
        # The batch really was scored off its bundle — otherwise this rail would
        # pass for the wrong reason.
        self.assertIn("verdicts read from: the .xcresult bundle", out, out[-4000:])
        self.assertEqual(proc.returncode, 1, out[-4000:])


class ShellConsoleOnlyVoidTests(ShellBatteryHarness):
    """The console-only warning belongs on VOID and ERROR too — R3.

    R2 put it on SURVIVED, which was the sentence printed without checking. But
    a console-only SURVIVED still rests on a positive `✔`; it is a console-only
    **VOID** that the missing bundle may have INVENTED. playhead-t53a measured
    80 of 87 reported casualties across 27 crash-free full-plan logs to be
    verdicts the console parser could not read, and the VOID epilogue tells a
    reader to re-run and, if it recurs, to suspect the mutation of killing the
    test host — the wrong thing to chase when the bundle simply never arrived.
    """

    MUTATION = "M05"

    def test_a_console_only_VOID_says_the_missing_bundle_may_have_caused_it(self):
        green = console(tests=[(self.expect, "passed")])
        silent = console(tests=[(self.expect, "started")])   # started, never judged
        proc = self.run_battery(green, silent)               # the stub writes no bundle
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "VOID", out[-4000:])
        self.assertIn("AND AT LEAST ONE RUN BEHIND THIS TABLE HAD NO", out, out[-4000:])
        self.assertIn("MANUFACTURES a NO VERDICT", out, out[-4000:])
        self.assertEqual(proc.returncode, 5, out[-4000:])

    def test_a_VOID_with_a_BUNDLE_does_not_get_the_warning(self):
        # ANTI-FABRICATION. The paragraph must be a property of the MISSING
        # BUNDLE, not of the VOID verdict — otherwise it would be printed on
        # every void run and say nothing.
        green = console(tests=[(self.expect, "passed")])
        silent = console(tests=[(self.expect, "started")])
        # SKIPPED so the bundle is REAL and readable and still judges nobody:
        # a skipped rail asked no question, so the verdict stays VOID and the
        # only variable between this rail and the one above is the bundle.
        env = self.stub_xcresult(
            bundle_payload([(self.expect, gb.XCRESULT_PASSED, [])]),
            bundle_payload([(self.expect, gb.XCRESULT_SKIPPED, [])]))
        proc = self.run_battery(green, silent, env=env)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "VOID", out[-4000:])
        self.assertNotIn("AND AT LEAST ONE RUN BEHIND THIS TABLE HAD NO", out, out[-4000:])
        self.assertNotIn("MANUFACTURES a NO VERDICT", out, out[-4000:])

        # --- playhead-gjlp0 R5: A SKIP IS THE SECOND VOID A RE-RUN CANNOT CLEAR
        # Folded into this run rather than paid for with another ~13 s battery
        # invocation, because this is already the only rail whose expectation
        # comes back SKIPPED. The baseline watched that same test PASS minutes
        # earlier on the unmutated tree, so the MUTATION is the likeliest reason
        # it skipped and every re-run skips it again — the same shape R4 closed
        # for an unreadable result string, one state over, and the epilogue's
        # exemption named only R4's because it was scoped to batch `#reason`
        # lines while [SKIPPED] is a per-test state.
        self.assertIn("[SKIPPED]", out, out[-4000:])
        self.assertIn("UNLESS THE ROW SAYS OTHERWISE", out, out[-4000:])
        self.assertIn("TWO shapes do", out, out[-4000:])
        self.assertIn("every re-run will skip it again", out, out[-4000:])
        # And what to DO — the hole L4 and R4-5 both went through: a consequence
        # with no action is half a remedy.
        self.assertIn("a different rail or a different mutant", out, out[-4000:])

    def test_a_console_only_ERROR_says_a_lost_START_line_reads_the_same_way(self):
        # The third epilogue. `never ran` is ABSENT, and a START line the
        # console lost makes a test ABSENT just as surely as a typo does.
        green = console(tests=[(self.expect, "passed")])
        nothing = console(tests=[])                          # the name is in no roster
        proc = self.run_battery(green, nothing)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "ERROR", out[-4000:])
        self.assertIn("AND AT LEAST ONE RUN BEHIND THIS TABLE HAD NO", out, out[-4000:])
        self.assertIn("makes a test invisible in both", out, out[-4000:])
        self.assertEqual(proc.returncode, 3, out[-4000:])


class ShellMultiExpectationTests(ShellBatteryHarness):
    """A record with SEVERAL expectations — playhead-gjlp0 R2.

    419 of the 1,109 records carry a `;`-separated list and no shell rail drove
    one: `setUpClass` actively REFUSED such a mutation, so the arm that decides
    a verdict across several expectations — and the ORDER of the four arms,
    which is what makes one unjudged expectation outrank one that passed — was
    measured by nothing that runs the battery.
    """

    MUTATION = "FD02"
    ALLOW_MULTI = True

    @classmethod
    def setUpClass(cls):
        super(ShellMultiExpectationTests, cls).setUpClass()
        if len(cls.expects) < 2:
            raise AssertionError("%s no longer has several expectations; these rails "
                                 "would test the single-expectation path" % cls.MUTATION)

    def test_one_expectation_FAILED_and_one_UNJUDGED_is_VOID_not_KILLED(self):
        # The whole ladder in one record. KILLED requires EVERY expectation to
        # have failed; an expectation nobody judged is evidence in neither
        # direction, and the unjudged arm must outrank the missing one.
        green = console(tests=[(e, "passed") for e in self.expects])
        mixed = console(tests=[(self.expects[0], "issue")] +
                              [(e, "started") for e in self.expects[1:]],
                        terminal="** TEST FAILED **")
        proc = self.run_battery(green, mixed)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "VOID", out[-4000:])
        self.assertIn("[NO-VERDICT]", out, out[-4000:])
        self.assertEqual(proc.returncode, 5, out[-4000:])

    def test_a_partial_kill_is_SURVIVED_naming_ONLY_the_ones_that_passed(self):
        green = console(tests=[(e, "passed") for e in self.expects])
        partial = console(tests=[(self.expects[0], "issue")] +
                                [(e, "passed") for e in self.expects[1:]],
                          terminal="** TEST FAILED **")
        proc = self.run_battery(green, partial)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "SURVIVED", out[-4000:])
        still = [l for l in out.split("\n") if "still green:" in l]
        self.assertEqual(len(still), 1, out[-4000:])
        for passing in self.expects[1:]:
            self.assertIn(passing, still[0])
        self.assertNotIn(self.expects[0], still[0])
        self.assertEqual(proc.returncode, 1, out[-4000:])


class ShellMultiBatchTests(ShellBatteryHarness):
    """Several mutations, several batches, one invocation — playhead-gjlp0 R2.

    Every other shell rail drives `--only <one mutation>`, which is one member
    in one batch. Two shapes were therefore never driven: a batch with more
    than one member (160 batches have exactly two, 57 have more), and a
    selection that APPENDS a `*99` vacuity control living in a DIFFERENT batch
    — 337 records reach that path, and it is the only way this script runs
    `run_focused` twice and writes two of everything ($WORK/expect-N.txt,
    outcomes-N.txt, batch-N.log, batch-N.xcresult, and one freshness floor per
    batch rather than per run).
    """

    MUTATION = "TK03"        # batch 1242, with TK10; series control TK99 is batch 1246
    ALLOW_MULTI = True       # TK03 carries three, which is not what these rails are about

    def test_two_batches_run_and_every_member_gets_its_own_verdict(self):
        others = {name: expectations_of(self.root, name) for name in ("TK10", "TK99")}
        every = list(dict.fromkeys(
            self.expects + others["TK10"] + others["TK99"]))
        # Only the expectations TK03 alone declares fail, so TK03 must SURVIVE
        # on the ones it shares, TK10 must SURVIVE, and the appended control
        # must SURVIVE in its own batch.
        shared = set(others["TK10"]) | set(others["TK99"])
        green = console(tests=[(e, "passed") for e in every])
        mixed = console(tests=[(e, "issue" if e not in shared else "passed")
                               for e in every],
                        terminal="** TEST FAILED **")
        proc = self.run_battery(green, mixed, args=["--batch", "1242"])
        out = self.out(proc)
        self.assertIn("TK99 is APPENDED", out, out[-4000:])
        self.assertIn("=== batch 1242: 2 mutation(s) ===", out, out[-4000:])
        self.assertIn("=== batch 1246: 1 mutation(s) ===", out, out[-4000:])
        self.assertEqual(self.verdict_of(proc, "TK03"), "SURVIVED", out[-6000:])
        self.assertEqual(self.verdict_of(proc, "TK10"), "SURVIVED", out[-6000:])
        self.assertEqual(self.verdict_of(proc, "TK99"), "SURVIVED", out[-6000:])
        # Each verdict names ITS OWN batch's log, not the last one written.
        self.assertRegex(out, r"evidence: \S*batch-1242\.log")
        self.assertRegex(out, r"evidence: \S*batch-1246\.log")
        self.assertEqual(proc.returncode, 1, out[-4000:])


class ShellObservedFailuresTests(ShellBatteryHarness):
    """A KILLED column is not evidence — the batch's WHOLE failure list is.

    `docs/investigations/playhead-8cjo-mutation-ledger.md` opens with it: "a
    mutant that reports KILLED while killing a DIFFERENT set is a false
    credit", and its whole observed column is transcribed from this block. The
    pre-gjlp0 battery printed it; the first cut of the fix replaced it with a
    COUNT, which cannot show a mutant killing tests it never declared.
    """

    MUTATION = "M05"

    def test_a_failure_no_mutation_declared_is_printed_by_name(self):
        green = console(tests=[(self.expect, "passed")])
        collateral = console(
            tests=[(self.expect, "issue"), ("A TEST NOBODY DECLARED", "issue")],
            terminal="** TEST FAILED **")
        proc = self.run_battery(green, collateral)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "KILLED", out[-4000:])
        self.assertIn("observed failures (ALL of them, 2)", out, out[-4000:])
        self.assertIn("✘ A TEST NOBODY DECLARED", out, out[-4000:])
        self.assertEqual(proc.returncode, 0, out[-4000:])

    def test_a_clean_batch_says_none_rather_than_printing_an_empty_block(self):
        green = console(tests=[(self.expect, "passed")])
        proc = self.run_battery(green, green)
        out = self.out(proc)
        self.assertIn("observed failures (ALL of them, 0)", out, out[-4000:])
        self.assertIn("(none)", out, out[-4000:])


class ShellNoTestsTests(ShellBatteryHarness):
    """A batch that never reached the test phase still names its log."""

    MUTATION = "M05"

    def test_a_batch_that_never_ran_tests_is_ERROR_and_names_its_log(self):
        green = console(tests=[(self.expect, "passed")])
        broken = "error: no such module 'Nope'\n** BUILD FAILED **\n"
        proc = self.run_battery(green, broken)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "ERROR", out[-4000:])
        self.assertRegex(out, r"evidence: \S*batch-\d+\.log")
        self.assertNotIn("evidence: NONE", out)
        self.assertEqual(proc.returncode, 3, out[-4000:])


class ShellXCTestExpectationTests(ShellBatteryHarness):
    """The XCTest half of the battery, driven end to end — playhead-gjlp0 R1.

    `ShellLadderTests` drives a mutation whose expectation is a Swift Testing
    DISPLAY name, and every rail in it passed while the battery could not score
    a single one of the 48 mutations whose sole expectation is spelled
    `SomeTests/testFoo`. Those refused at the baseline preflight with "an
    expectation names a test that never ran" — a loud failure, and a total one:
    the whole of playhead-le02's XCTest support was unreachable.

    SF04 rather than M05 because SF04's expectation is exactly that shape and
    its anchor still applies (LE07's does not, which is a separate finding).
    """

    MUTATION = "SF04"

    def xctest_run(self, outcome):
        suite, method = self.expect.split("/", 1)
        return xctest_console(suite, method, outcome,
                              terminal=("** TEST FAILED **" if outcome == "failed"
                                        else "** TEST SUCCEEDED **"))

    def test_the_expectation_really_is_the_bare_xctest_spelling(self):
        # ANTI-VACUITY: if SF04 is ever respelled or retyped, these rails stop
        # exercising the resolver and this says so.
        self.assertRegex(self.expect, r"^[A-Za-z0-9_]+Tests/test[A-Za-z0-9_]+$")

    def test_an_xctest_expectation_passes_the_baseline_and_reports_KILLED(self):
        proc = self.run_battery(self.xctest_run("passed"), self.xctest_run("failed"))
        out = self.out(proc)
        self.assertIn("baseline green", out, out[-4000:])
        self.assertEqual(self.verdict_of(proc), "KILLED", out[-4000:])
        self.assertEqual(proc.returncode, 0, out[-4000:])

    def test_an_xctest_expectation_that_positively_passes_reports_SURVIVED(self):
        green = self.xctest_run("passed")
        proc = self.run_battery(green, green)
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "SURVIVED", out[-4000:])
        self.assertEqual(proc.returncode, 1, out[-4000:])

    def test_an_xctest_expectation_that_never_reports_is_VOID_not_SURVIVED(self):
        # The bead's own defect, on the XCTest side: started, never judged.
        proc = self.run_battery(self.xctest_run("passed"), self.xctest_run("started"))
        out = self.out(proc)
        self.assertEqual(self.verdict_of(proc), "VOID", out[-4000:])
        self.assertEqual(proc.returncode, 5, out[-4000:])


if __name__ == "__main__":
    unittest.main()
