#!/usr/bin/env python3
"""playhead-voez — make the default gate's verdict about the DIFF, not the count.

WHY THIS EXISTS
---------------
`scripts/fast-gate.sh` exits 65 on a clean checkout, every time, with dozens of
failures nobody introduced. A gate that is red no matter what cannot answer the
only question it exists to answer — *did my change break something?* — so every
bead in flight was hand-diffing its failure set against a baseline nobody had
written down. That is not a hypothetical cost: playhead-aqo9 burned six extra
builds and retracted two conclusions over it, and a real regression
(playhead-ynmk, #313) merged as "looks like the usual flakes".

So: record what is known-broken in a committed file, and make the gate's exit
code a statement about the difference between this run and that file.

    RED (N known / 0 new)   -> exit 0
    RED (N known / 2 NEW)   -> exit non-zero, both named
    a baseline test PASSES  -> exit non-zero, named  (Dan, 2026-07-29: "yes")

THE HARD PART: THE BASELINE SET IS NOT STABLE
---------------------------------------------
An exact set is the right hygiene but a flat exact set does not survive contact
with this machine. Measured full-gate failure counts on unchanged main ranged
33 -> 46 -> 60 -> 72 across one day, and 50 of 62 recorded issues in the
2026-08-01 run were literally `Time limit was exceeded: 60.000 seconds`. The
failing set is not a property of the tree; it is largely a property of how
starved the box was. Membership, not just count, moves.

The design that survives that has three parts.

1. TIERS DERIVED FROM MEASUREMENT, NOT TASTE. Every entry carries how many
   observations it was seen in and how many it failed in. `failed == seen` with
   at least MIN_RUNS_FOR_DETERMINISTIC observations makes it DETERMINISTIC; anything else is
   LOAD-SENSITIVE. Nobody hand-labels a test flaky — the file records counts and
   the tier falls out. The threshold is three observations, not two, because the
   measured run-to-run Jaccard is 0.46 — see MIN_RUNS_FOR_DETERMINISTIC.

2. DAN'S PASS-DIRECTION ARM APPLIES WHERE IT IS SOUND. A DETERMINISTIC member
   that passes fails the gate: that entry claimed to fail every time and did
   not, so the list has rotted and must shrink. A LOAD-SENSITIVE member that
   passes is reported as a removal candidate but does not fail the gate,
   because a single quiet run is not evidence that a starvation flake is fixed.
   Making it fail there would fire on every quiet box, and a gate that cries
   wolf is a gate people learn to bypass — which is the failure mode this bead
   is fixing, not one to re-introduce.

3. THE TOLERANCE IS NOT A HOLE, BECAUSE IDENTITY INCLUDES THE FAILURE KIND.
   This is the load-bearing part. A load-sensitive entry is not "this test may
   fail"; it is "this test may TIME OUT". If a known-timeout test instead fails
   an expectation, its kind is not in the recorded set and it is reported as
   NEW. So the only regression the tolerance can absorb is one that (a) lands
   in a test already named in the file and (b) manifests as a >=60s starvation
   rather than an assertion — i.e. a fresh deadlock inside an already-starving
   test. That residue is named, bounded and written down here; everything else,
   including every regression in every test not in the file, is reported.

Three further arms close the ways a failure could hide without being seen:

  * ABSENT — a baseline member that neither passed nor failed did not run at
    all. A name nobody can reach is not evidence (two of playhead-djl0's
    mutation rails "survived" for exactly this reason), so it fails the gate.
  * INCOMPLETE — a log with no terminal verdict is a fragment, and every test
    after the cut looks like it never ran. Refuse to judge rather than report
    green. The 2026-08-01 log everyone quoted "72 failures" from is such a
    fragment: 930 XCTest cases started, 900 reported, no terminal marker.
  * FICTION — zero failures against a non-empty baseline. Against a measured
    floor of dozens that is categorical, not quiet.

A CRASHED HOST PRODUCES NO VERDICT, AND THAT IS NOT A FAILURE (playhead-tl6l)
----------------------------------------------------------------------------
Everything above reads per-test result lines. A test whose HOST died emits
none: no `failed after`, no `Test Case … failed`, no issue. It is therefore
absent from the run's observed set entirely — matched against the baseline as
neither "known" nor reported as NEW. It falls out of the arithmetic in BOTH
directions, silently, and the run still prints a confident `RED (N known /
0 new)` and exits 0. Same shape as the wedge `scripts/disk_preflight.py` exists
to catch: the failure destroys the evidence of itself.

MEASURED on two full-plan runs, 2026-08-12 (main @ 76b0a09a and bead/mn5e).
Both carry xcodebuild's restart marker, and after discounting skips:

    main:  33 Swift Testing tests started and reported NOTHING
    mn5e:  15                    "                          "

and the baseline file — 117 tests over 9 observed runs — has never once
recorded any of them. Nine runs of silence.

So this module now tracks a third outcome and a fourth verdict category:

  * SKIPPED is parsed (`➜ Test "x" skipped:` and XCTest's `skipped (0.0s)`),
    because "started and said nothing" is only meaningful once a deliberate
    skip has been subtracted from it. Without that the no-verdict set on those
    same two logs reads 45 and 63, of which 30 are PerfGate skips — a number
    that means one thing and is read as another, which is this repo's standing
    defect class.
  * NO VERDICT — started, then neither passed, failed nor skipped. Exact,
    console-only, no name mapping required. This is the census.
  * HOST RESTART — xcodebuild's own `Restarting after unexpected exit, crash,
    or test timeout` line, quoted as corroborating evidence.

WHY THE `Failing tests:` BLOCK IS A LEAD AND NOT A CENSUS
---------------------------------------------------------
The obvious fix — diff xcodebuild's `Failing tests:` summary against the tests
that reported — cannot be done soundly from a log, and the reason is worth
writing down because it is what the bead's own evidence tripped over.

The summary prints `SuiteType.function()`. Swift Testing's console prints the
test's DISPLAY NAME, `@Test("A completion delivered to a NEW manager instance
still carries the show")`. The two spellings share nothing. Grepping a summary
entry against the console therefore returns zero for a test that reported
perfectly well — which is exactly how playhead-tl6l came to be filed claiming
19 invisible failures. Re-measured through the source-level mapping: of the 15
distinct summary names in the mn5e run, ALL 15 had reported and were already
counted; of the 14 on main, 13 had. The real number of block entries invisible
to this module was 0 and 1, not 19 and 18. The blindness is real, the
population named in the bead was not.

And the residue cuts both ways: `Suite.cancelReapsAttribution()` (reported
under a display name) and `Suite.failedSuggestNoRestoresLatestBufferedRevision()`
(the one genuine casualty on main, which never emitted a line at all) are
INDISTINGUISHABLE from the log. Resolving them would mean parsing
`PlayheadTests/**.swift` for `@Test("…")` attributes, which trades a sound
log-only tool for a source-coupled heuristic whose every miss is a false crash
alarm. Not worth it: the NO VERDICT census above already covers every casualty
that got as far as starting.

So the block is parsed, reported with an exact entries-vs-distinct count, and
labelled a LEAD. It earns its place three ways: it names a casualty that never
started (invisible to the started-set); it makes the duplicate entries legible
(xcodebuild repeats a name it retried — 4 of 19 on mn5e); and it lets an ABSENT
baseline member be reported with the RIGHT CAUSE. Before this, a crashed
baseline member read `(renamed, deleted or newly skipped)`, sending the reader
to look for a rename that never happened.

WHAT NO VERDICT DOES TO THE EXIT CODE, AND WHY
----------------------------------------------
It does not fail the gate on its own. It changes the headline, forecloses
GREEN, and protects the baseline from being quietly shrunk by a crash.

The argument, in the terms this repo already uses. A run that produced no
verdict for part of the plan is arguably worse than one with a known
regression — but it fires on main TODAY, on a pre-existing crash owned by a
different bead, so arming it here would make every full-plan gate on this box
exit 65 for a reason nobody in the middle of a bead can fix. CLAUDE.md is
explicit that this is the worse trade: "one red rule and everyone learns to
route around the gate, which is strictly worse than having no linter". The
repo's own answer to "red for a pre-existing reason" is to RECORD it and diff
against the record — and this bead is not permitted to refresh the baseline,
which is the only place such a record could live.

What it does instead is remove every way to misread the run:

  * the headline carries it, so the reassuring `RED (N known / 0 new)` can
    never stand alone again — it reads `RED (N known / 0 new) — 33 tests got
    NO VERDICT (crashed host)`;
  * GREEN is unreachable while the count is non-zero, on the same principle
    that already forbids GREEN for a run that executed nothing;
  * a baseline member with no verdict still fails the gate, because it is
    ABSENT — unchanged policy, now with the crash named as the cause;
  * `accept` CARRIES FORWARD a baseline entry that got no verdict instead of
    dropping it. `merge` prunes anything the run did not reach, on the theory
    that it was renamed or deleted; a crash makes that theory false, and the
    command meant to maintain the file would have silently deleted exactly the
    entries the crash hid. It is announced, not silent.

Arming it is a one-line change (`no_verdict` into `exit_code`) and should
happen once the host crash (playhead-rouw) is fixed and a run can be observed
at zero. That is playhead-buvn — this module deliberately does not decide it.

THE FILE CONVERGES; IT DOES NOT ARRIVE COMPLETE
-----------------------------------------------
The recorded set is the UNION of what has been observed, and with a measured
run-to-run Jaccard of 0.46 each new run surfaces flakes the earlier ones missed.
Capture-recapture on the first two full runs (32 and 28 failures, 19 shared)
puts the true population near 47; 41 are recorded after two observations. So the
next accept or two will legitimately add a handful of names.

That is not a defect to engineer around. A newly-observed flake is
indistinguishable from a regression until it has been seen at least once, and
absorbing unrecognised names on the theory that they are probably flakes is the
exact hole this module exists to close. Report, record, move on.

KNOWN RESIDUE, WRITTEN DOWN RATHER THAN GLOSSED
-----------------------------------------------
Swift Testing's console line prints the test's display name and no suite, so two
same-named tests in different suites share one key. Measured on the 2026-08-01
full run: 56 of 9,819 names, 0.57%. Identity is resolved toward FAILED — a
colliding pass never erases a failure — so the dangerous direction is closed,
and the residue is that a regression landing in the passing twin of a colliding
pair while the other twin still fails would read as known. Adding the source
file to the key would separate them, but only failures carry a source (it comes
from the issue line), so a baseline key built that way could never be matched
against a pass and the pass-direction arm would stop working. The collision is
the cheaper of the two costs.

XCTest has no such problem: its key is the fully qualified
`Target.Suite/testMethod`.

BOTH FRAMEWORKS, AND WHY THE HEURISTIC INVERTS
----------------------------------------------
Swift Testing prints `✘ Test "name" failed after 123.4 seconds`; XCTest prints
`Test Case '-[Suite testFoo]' failed (0.025 seconds)`. Triage that greps only
the first is how playhead-ynmk merged unnoticed, so both are parsed here.

They behave oppositely and duration is the trap: a slow Swift Testing failure is
usually starvation, while a slow XCTest failure is still an assertion. This
module therefore never classifies on duration at all. Swift Testing kind comes
from the issue text (`Time limit was exceeded` -> timeout, anything else ->
assertion), which is the direct evidence rather than a proxy; XCTest failures
are always assertions, because XCTest has no time-limit issue to report.

WHAT AN ACCEPT MUST SAY OUT LOUD
--------------------------------
`accept` reported membership and nothing else until playhead-26od R5, and both
halves of that omission had already done damage on one branch.

  * The KIND of each added entry was never printed, so the third accept — 28
    entries, three of them assertion-ONLY and a fourth mixed — was justified in
    its commit message as "all timeouts". Under this module's own identity rule
    a load-sensitive entry means MAY TIME OUT, so that is a different claim, and
    it was written from the count because the count was all the tool showed.
  * TIER PROMOTIONS were never printed, so the same accept crossed fifteen
    entries into `deterministic` — arming the pass-direction check, after which
    each of them PASSING hard-fails the gate — and said nothing. A hard failure
    armed silently is the exact species of quiet this file exists to remove.

Both are now printed: a kind census plus a per-entry kind on every `+` line, and
a loud `ARMED:` block naming every promotion with its `failed/seen` count. The
POLICY is untouched — what is deterministic, and that its passing is fatal, is
Dan's call and lives at `MIN_RUNS_FOR_DETERMINISTIC`. Only the reporting changed.

USAGE
-----
    gate_baseline.py check  --log RUN.log --baseline scripts/gate-baseline.json
    gate_baseline.py accept --log RUN.log --baseline scripts/gate-baseline.json

`check` is what `scripts/fast-gate.sh` runs; `accept` is what
`scripts/fast-gate.sh --accept-baseline` runs. Exit codes: 0 ok, 1 regression,
2 could not evaluate.
"""

import argparse
import json
import pathlib
import re
import sys

EXIT_OK = 0
EXIT_REGRESSION = 1
EXIT_CANNOT_EVALUATE = 2

KIND_TIMEOUT = "timeout"
KIND_ASSERTION = "assertion"
KIND_UNKNOWN = "unknown"

TIER_DETERMINISTIC = "deterministic"
TIER_LOAD_SENSITIVE = "load-sensitive"

# How many observations an entry must fail in a row before its PASSING is
# allowed to fail the gate.
#
# MEASURED, not guessed. Two full runs on identical code, same quiet box,
# nothing else running: 32 failures and 28 failures, 19 in common, union 41 —
# a Jaccard of 0.46. Under that much churn "failed twice" is weak evidence of
# "fails every time", and every entry wrongly promoted becomes a NOW PASSES
# gate failure on some later quiet run.
#
# The asymmetry decides it. A false pass-direction alarm costs a gate run, a
# refresh commit, and a little more of the trust this bead exists to restore —
# and a gate people stop believing is the exact thing being fixed. A missed one
# costs a stale name that is still printed as a removal candidate every run.
# So: be slow to promote, and let the file earn it.
MIN_RUNS_FOR_DETERMINISTIC = 3

# How many names to print per category before collapsing to a count. A verdict
# nobody reads is a verdict nobody acts on.
_MAX_LISTED = 10

FRAMEWORK_SWIFT_TESTING = "swift-testing"
FRAMEWORK_XCTEST = "xctest"


class CannotEvaluate(Exception):
    """The run cannot be judged — refuse rather than guess in either direction."""


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
#
# Every pattern below SEARCHES rather than anchors. Real xcodebuild logs carry
# \r-overwritten prefixes ("X◇ Test", "​✘ Test" with a zero-width space), and an
# anchored `^\W*` misses the ones whose junk happens to be a word character —
# a silently dropped failure, in the direction that reads as success.

_ST_FAIL_NAMED = re.compile(r'✘ Test "(.+?)" (?:with \d+ test cases? )?failed after ([\d.]+) seconds')
_ST_FAIL_FUNC = re.compile(r'✘ Test ([A-Za-z_][A-Za-z0-9_]*\(\)) (?:with \d+ test cases? )?failed after ([\d.]+) seconds')
# Greedy `.*` before ` at <file>.swift:` so parameterised runs — which splice
# "with 2 arguments depth → 8, mix → preAnalysis" in between — resolve to the
# LAST such marker, which is the source location rather than an argument value.
_ST_ISSUE_NAMED = re.compile(r'✘ Test "(.+?)" recorded an issue(?P<mid>.*?)(?: at ([A-Za-z0-9_+]+\.swift):(\d+):(\d+))?: (.*)$')
_ST_ISSUE_FUNC = re.compile(r'✘ Test ([A-Za-z_][A-Za-z0-9_]*\(\)) recorded an issue(?P<mid>.*?)(?: at ([A-Za-z0-9_+]+\.swift):(\d+):(\d+))?: (.*)$')
_ST_PASS_NAMED = re.compile(r'✔ Test "(.+?)" (?:with \d+ test cases? )?passed after ([\d.]+) seconds')
_ST_PASS_FUNC = re.compile(r'✔ Test ([A-Za-z_][A-Za-z0-9_]*\(\)) (?:with \d+ test cases? )?passed after ([\d.]+) seconds')
_ST_START_NAMED = re.compile(r'◇ Test "(.+?)" started')
_ST_START_FUNC = re.compile(r'◇ Test ([A-Za-z_][A-Za-z0-9_]*\(\)) started')
# A DELIBERATE skip is a third outcome, not silence. PerfGate alone accounts for
# 30 XCTest skips and ~11 Swift Testing skips in a full run; without this the
# no-verdict census reads 45 where the truth is 15.
_ST_SKIP_NAMED = re.compile(r'➜ Test "(.+?)" skipped')
_ST_SKIP_FUNC = re.compile(r'➜ Test ([A-Za-z_][A-Za-z0-9_]*\(\)) skipped')

_XC_RESULT = re.compile(
    r"Test Case '-\[([A-Za-z0-9_.]+) ([A-Za-z0-9_:]+)\]' (failed|passed|skipped) "
    r"\(([\d.]+) seconds\)"
)
_XC_START = re.compile(r"Test Case '-\[([A-Za-z0-9_.]+) ([A-Za-z0-9_:]+)\]' started")

_TERMINAL = re.compile(r"\*\* TEST (FAILED|SUCCEEDED) \*\*|Test run with \d+ tests? in \d+ suites? (?:failed|passed) after")

_WEDGED_SIM = "fast-gate: wedged simulator"

_TIME_LIMIT = "Time limit was exceeded"

# playhead-tl6l. xcodebuild's own words when the test host died and it started a
# new one. Unambiguous, needs no name mapping, and present in both full-plan runs
# measured on 2026-08-12 — while the verdict printed above it said nothing.
_HOST_RESTART = re.compile(
    r"Restarting after unexpected exit, crash, or test timeout"
)

# The summary block xcodebuild prints after the terminal marker:
#
#     Failing tests:
#     \tDownloadShowAttributionTests.attributionSurvivesProcessRestart()
#     \tDownloadShowAttributionTests.attributionSurvivesProcessRestart()
#
# A repeated name is a RETRY, not two tests. Entries and distinct names are both
# reported, because a count that silently means one when read as the other is
# this repo's standing defect class.
_FAILING_TESTS_HEADER = re.compile(r"^\s*Failing tests:\s*$")
_BLOCK_ENTRY = re.compile(r"^[ \t]+(\S.*?)\s*$")
_BLOCK_NAME = re.compile(r"^([A-Za-z_][A-Za-z0-9_.]*)\.([A-Za-z_][A-Za-z0-9_]*)(\(\))?$")


def st_key(name):
    return FRAMEWORK_SWIFT_TESTING + "::" + name


def xc_key(suite, method):
    return FRAMEWORK_XCTEST + "::" + suite + "/" + method


class Failure(object):
    __slots__ = ("key", "framework", "name", "kinds", "seconds", "source")

    def __init__(self, key, framework, name):
        self.key = key
        self.framework = framework
        self.name = name
        self.kinds = set()
        self.seconds = None
        self.source = None

    def __repr__(self):  # pragma: no cover - debugging aid
        return "Failure(%r, kinds=%r, %ss)" % (self.key, sorted(self.kinds), self.seconds)


class RunResult(object):
    def __init__(self):
        self.failures = {}
        self.passed = set()
        self.skipped = set()
        self.started = set()
        self.complete = False
        # playhead-tl6l
        self.host_restarts = 0
        self.restart_evidence = None
        self.blamed_entries = []   # `Failing tests:` lines, duplicates INTACT

    @property
    def ran(self):
        """Keys with a definite outcome. Started-but-silent is NOT an outcome.

        A SKIP is deliberately not in here. It is an outcome for the purpose of
        "did the host die", and no outcome at all for the purpose of "is this
        baseline entry still failing" — the ABSENT arm must keep firing on a
        newly-skipped member, which is what makes PerfGate-ing a family visible
        rather than a quiet loss of coverage.
        """
        return set(self.failures) | self.passed

    @property
    def blamed(self):
        """The `Failing tests:` names, de-duplicated, first-seen order kept."""
        return list(dict.fromkeys(self.blamed_entries))

    @property
    def no_verdict(self):
        """Started, then said nothing at all — the crashed-host casualties.

        Exact and console-only: both the start line and the outcome line carry
        the same identity, so this needs no mapping between xcodebuild's
        `Suite.function()` spelling and Swift Testing's display names.
        """
        return self.started - self.ran - self.skipped


def last_attempt(text):
    """fast-gate retries once on a wedged simulator and both attempts land in one
    log. Attempt 1's casualties — tests that were mid-flight when the sim died —
    would otherwise union with attempt 2 and manufacture failures out of an
    infrastructure artefact. Keep only what follows the last retry banner."""
    cut = 0
    lines = text.splitlines(True)
    for i, line in enumerate(lines):
        if _WEDGED_SIM in line:
            cut = i + 1
    return "".join(lines[cut:])


def _kind_of_issue(message):
    return KIND_TIMEOUT if _TIME_LIMIT in message else KIND_ASSERTION


def parse_run(text):
    """Parse one xcodebuild gate log into a RunResult."""
    run = RunResult()
    failures = run.failures

    def failure_for(key, framework, name):
        if key not in failures:
            failures[key] = Failure(key, framework, name)
        return failures[key]

    in_block = False
    for line in last_attempt(text).splitlines():
        if not run.complete and _TERMINAL.search(line):
            run.complete = True

        # --- xcodebuild's own summary block (playhead-tl6l) -----------------
        # Read before anything else: its entries are indented names, and a
        # `Suite.func()` line must never be mistaken for something else.
        if in_block:
            m = _BLOCK_ENTRY.match(line)
            if m:
                run.blamed_entries.append(m.group(1))
                continue
            in_block = False
        if _FAILING_TESTS_HEADER.match(line):
            in_block = True
            continue

        if _HOST_RESTART.search(line):
            run.host_restarts += 1
            if run.restart_evidence is None:
                run.restart_evidence = line.strip()
            continue

        # --- Swift Testing -------------------------------------------------
        m = _ST_ISSUE_NAMED.search(line) or _ST_ISSUE_FUNC.search(line)
        if m:
            name, source, message = m.group(1), m.group(3), m.group(6)
            failure = failure_for(st_key(name), FRAMEWORK_SWIFT_TESTING, name)
            failure.kinds.add(_kind_of_issue(message))
            if source and not failure.source:
                failure.source = source
            continue

        m = _ST_FAIL_NAMED.search(line) or _ST_FAIL_FUNC.search(line)
        if m:
            name = m.group(1)
            failure = failure_for(st_key(name), FRAMEWORK_SWIFT_TESTING, name)
            seconds = float(m.group(2))
            if failure.seconds is None or seconds > failure.seconds:
                failure.seconds = seconds
            continue

        m = _ST_PASS_NAMED.search(line) or _ST_PASS_FUNC.search(line)
        if m:
            run.passed.add(st_key(m.group(1)))
            continue

        m = _ST_SKIP_NAMED.search(line) or _ST_SKIP_FUNC.search(line)
        if m:
            run.skipped.add(st_key(m.group(1)))
            continue

        m = _ST_START_NAMED.search(line) or _ST_START_FUNC.search(line)
        if m:
            run.started.add(st_key(m.group(1)))
            continue

        # --- XCTest --------------------------------------------------------
        m = _XC_RESULT.search(line)
        if m:
            suite, method, outcome, seconds = m.groups()
            key = xc_key(suite, method)
            if outcome == "skipped":
                run.skipped.add(key)
            elif outcome == "failed":
                failure = failure_for(key, FRAMEWORK_XCTEST, suite + "/" + method)
                # XCTest reports assertions, never time-limit issues. A 3.5s
                # XCTest failure is as real as a 0.025s one — the slow-is-a-flake
                # heuristic belongs to Swift Testing alone and must never be
                # applied here.
                failure.kinds.add(KIND_ASSERTION)
                failure.seconds = float(seconds)
            else:
                run.passed.add(key)
            continue

        m = _XC_START.search(line)
        if m:
            run.started.add(xc_key(m.group(1), m.group(2)))

    for failure in failures.values():
        if not failure.kinds:
            failure.kinds.add(KIND_UNKNOWN)

    # Swift Testing's console line carries no suite name, so two same-named
    # tests in different suites collide on one key. Resolve toward FAILED: a
    # colliding pass must never erase a real failure.
    run.passed -= set(failures)
    # Same rule one step further out: a skip colliding with a real outcome must
    # never swallow it. The residue is unchanged and is the one already written
    # down above — two same-named tests share a key, so a skipped twin still
    # accounts for a SILENT twin. That is the 0.57%-of-names collision cost,
    # not a new hole.
    run.skipped -= set(failures) | run.passed
    return run


# ---------------------------------------------------------------------------
# The baseline file
# ---------------------------------------------------------------------------

def empty_baseline(plan):
    return {"plan": plan, "mode": "full-plan", "runs_observed": 0, "tests": {}}


def load_baseline(path):
    with open(str(path), encoding="utf-8") as handle:
        return json.load(handle)


def save_baseline(path, data):
    path = pathlib.Path(str(path))
    payload = json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False)
    path.write_text(payload + "\n", encoding="utf-8")


def tier_of(entry):
    if (entry["seen_runs"] >= MIN_RUNS_FOR_DETERMINISTIC
            and entry["failed_runs"] == entry["seen_runs"]):
        return TIER_DETERMINISTIC
    return TIER_LOAD_SENSITIVE


def tier_changes(baseline, merged):
    """Which entries changed TIER in this merge: `(promoted, demoted)`.

    Promotion is the one that matters. Crossing into `deterministic` ARMS the
    pass-direction check — from that merge onward the entry PASSING hard-fails
    the gate — and until playhead-26od R5 an accept reported only membership,
    so an accept could arm fifteen hard failures and print nothing about any of
    them. Arming a failure silently is the single thing this module exists not
    to do; the whole point of `--accept-baseline` is that a human writes down
    what changed, and a human cannot write down what they were never shown.

    Pure, so the CLI only has to print it. A newly ADDED entry can never appear
    here: it enters at `seen_runs = 1`, which is below
    `MIN_RUNS_FOR_DETERMINISTIC` by construction, so promotion always takes at
    least one further observation and is always a CHANGE to something already
    recorded.
    """
    old = baseline.get("tests", {}) if baseline.get("plan") == merged.get("plan") else {}
    promoted = []
    demoted = []
    for key, entry in merged.get("tests", {}).items():
        now = tier_of(entry)
        before = tier_of(old[key]) if key in old else None
        if now == TIER_DETERMINISTIC and before != TIER_DETERMINISTIC:
            promoted.append(key)
        elif before == TIER_DETERMINISTIC and now != TIER_DETERMINISTIC:
            demoted.append(key)
    return sorted(promoted), sorted(demoted)


def _kinds_label(entry):
    """`timeout`, `assertion`, `assertion+timeout` — never blank."""
    return "+".join(sorted(entry.get("kinds", []))) or KIND_UNKNOWN


def kind_census(entries):
    """`{kinds-label: count}` over a list of baseline entries.

    Exists because of what it would have caught. playhead-26od's third accept
    added 28 entries and was justified in the commit message as "all timeouts";
    three were assertion-only and a fourth mixed, which under this module's own
    identity rule ("a load-sensitive entry means MAY TIME OUT") is a different
    claim entirely. Nothing in the accept output named a single entry's kind, so
    the summary was written from the count. Print the census and it cannot be.
    """
    census = {}
    for entry in entries:
        label = _kinds_label(entry)
        census[label] = census.get(label, 0) + 1
    return census


def merge(baseline, run, plan):
    """Fold one run's observations into the baseline and return the new file.

    Self-pruning by construction: an entry the run never reached is dropped
    (renamed, deleted or newly skipped), and an entry that has failed in none of
    its observations is dropped (fixed). Both directions shrink the file without
    anyone editing it, which is what keeps the pass-direction arm affordable.
    """
    if not run.complete:
        raise CannotEvaluate(
            "the log has no terminal verdict — it is a fragment, not a run"
        )

    # A run can carry a terminal verdict and still have executed NOTHING.
    # Measured: erasing the simulator made xcodebuild reach for the clone
    # helper, which resolves `simctl` through the GLOBAL xcode-select
    # (CommandLineTools, no simctl); it printed `** TEST FAILED **` after zero
    # tests. Accepting that would silently DELETE every entry as unreachable and
    # call the empty result a baseline — the file destroyed by the very command
    # meant to maintain it.
    if not run.ran:
        raise CannotEvaluate(
            "the run recorded no test results at all — it did not exercise the plan"
        )
    existing = set(baseline.get("tests", {})) if baseline.get("plan") == plan else set()
    if existing:
        reached = len(existing & run.ran)
        if reached * 2 < len(existing):
            raise CannotEvaluate(
                "the run reached only %d of the %d recorded tests — too few to be a "
                "run of this plan, and accepting it would drop the rest"
                % (reached, len(existing))
            )

    if baseline.get("plan") != plan:
        # A different plan is a different population. Carrying entries across
        # would name tests the new plan never runs, which reads as ABSENT
        # forever.
        baseline = empty_baseline(plan)

    merged = {
        "plan": plan,
        "mode": baseline.get("mode", "full-plan"),
        "runs_observed": baseline.get("runs_observed", 0) + 1,
        "tests": {},
    }

    old = baseline.get("tests", {})
    # playhead-tl6l. A crashed host is not a rename, and the prune below cannot
    # tell them apart on its own — so a run whose host died would have DELETED
    # every recorded entry it took down with it, quietly, from inside the one
    # command whose job is to maintain the file. Carry those forward untouched:
    # unchanged counts, no observation credited, still on the list.
    protected = run.no_verdict
    for key in sorted(set(old) | set(run.failures)):
        previous = old.get(key)
        failure = run.failures.get(key)
        reached = key in run.ran

        if previous is None:
            merged["tests"][key] = {
                "framework": failure.framework,
                "name": failure.name,
                "seen_runs": 1,
                "failed_runs": 1,
                "kinds": sorted(failure.kinds),
                "source": failure.source,
            }
            continue

        if not reached:
            if key in protected:
                merged["tests"][key] = dict(previous)
            continue  # renamed, deleted or skipped — it is not knowledge

        entry = dict(previous)
        entry["seen_runs"] = previous["seen_runs"] + 1
        if failure is not None:
            entry["failed_runs"] = previous["failed_runs"] + 1
            entry["kinds"] = sorted(set(previous.get("kinds", [])) | failure.kinds)
            if failure.source:
                entry["source"] = failure.source
        if entry["failed_runs"] == 0:
            continue  # never failed in any observation — it is fixed
        merged["tests"][key] = entry

    return merged


# ---------------------------------------------------------------------------
# The verdict
# ---------------------------------------------------------------------------

class Verdict(object):
    def __init__(self):
        self.cannot_evaluate = None
        self.new_failures = []
        self.kind_changed = []
        self.kind_detail = {}
        self.deterministic_passed = []
        self.load_sensitive_passed = []
        self.absent = []
        self.baseline_fiction = False
        self.known_failures = []
        self.runs_observed = 0
        self.total_failures = 0
        self.baseline_size = 0
        # playhead-tl6l — the run's SILENCE, tracked as its own thing.
        self.no_verdict = []
        self.absent_crashed = set()
        self.host_restarts = 0
        self.restart_evidence = None
        self.blamed_entry_count = 0
        self.blamed_distinct = []
        self.blamed_unmatched = []

    @property
    def ok(self):
        return self.exit_code == EXIT_OK

    @property
    def crashed_host(self):
        """Did this run fail to produce a verdict for part of the plan?

        Any one of the three is enough, and they are independent evidence:
        xcodebuild said it restarted the host; tests started and said nothing;
        or the summary block blamed a name that never emitted a console line.
        """
        return bool(self.host_restarts or self.no_verdict or self.blamed_unmatched)

    @property
    def headline_tail(self):
        """What rides on the RED line. Says only what was actually observed.

        Three different observations, three different sentences — a headline
        reading `0 tests got NO VERDICT` because some OTHER piece of evidence
        fired would be precisely the number that means one thing and is read as
        another.
        """
        if self.no_verdict:
            return " — %d test%s got NO VERDICT (crashed host)" % (
                len(self.no_verdict), "" if len(self.no_verdict) == 1 else "s",
            )
        if self.host_restarts:
            return " — the test host CRASHED and was restarted"
        if self.blamed_unmatched:
            return " — %d name(s) in `Failing tests:` matched no console result" % (
                len(self.blamed_unmatched),
            )
        return ""

    @property
    def exit_code(self):
        if self.cannot_evaluate:
            return EXIT_CANNOT_EVALUATE
        # NO VERDICT is deliberately NOT here. See the module docstring: it
        # fires on main today, on a pre-existing crash owned by playhead-rouw,
        # and a gate that is red for a reason the reader cannot fix is one they
        # learn to route around. It changes the headline, forecloses GREEN, and
        # protects `accept`; a baseline member with no verdict still fails, via
        # `absent`, exactly as it did before. Arming it is playhead-buvn.
        if (self.new_failures or self.kind_changed or self.deterministic_passed
                or self.absent or self.baseline_fiction):
            return EXIT_REGRESSION
        return EXIT_OK

    def render(self):
        out = []
        if self.cannot_evaluate:
            out.append("gate-baseline: CANNOT EVALUATE — " + self.cannot_evaluate)
            out.append(
                "  The gate's own exit code stands; this check made no claim."
            )
            return "\n".join(out)

        # Case carries signal, exactly as the bead specifies it: "0 new" is the
        # quiet all-clear, "2 NEW" is the shout. Someone skimming a CI log reads
        # the capitals before they read the number.
        headline = "%d known / %d %s" % (
            len(self.known_failures),
            len(self.new_failures),
            "NEW" if self.new_failures else "new",
        )
        # playhead-tl6l: the count rides ON the headline, not under it. The whole
        # hazard was that a crashed run could print the reassuring `RED (N known
        # / 0 new)` — the exact string CLAUDE.md tells people to read as an
        # all-clear — while an entire test family never reached a verdict. That
        # string can no longer stand alone.
        tail = self.headline_tail
        # GREEN is reserved for "nothing failed AND nothing else is wrong". A run
        # that executed no tests has zero failures too, and calling that GREEN is
        # how a broken run reads as a clean sweep. A run that lost part of the
        # plan to a dead host is the same claim with the same answer.
        if self.ok and self.total_failures == 0 and not self.crashed_host:
            out.append("gate-baseline: GREEN (%s)" % headline)
        else:
            out.append("gate-baseline: RED (%s)%s" % (headline, tail))

        out.extend(self._render_no_verdict())

        for key in self.new_failures:
            out.append("  NEW FAILURE      %s" % key)
        for key in self.kind_changed:
            out.append("  FAILS DIFFERENTLY %s — %s" % (key, self.kind_detail.get(key, "")))
        for key in self.deterministic_passed:
            out.append("  NOW PASSES       %s  (recorded as failing every run)" % key)
        if self.absent:
            out.append(
                "  DID NOT RUN — %d of %d recorded tests were never reached. If that "
                "is most of them, the run did not exercise the plan (a wedged "
                "simulator or a failed install reports `** TEST FAILED **` after "
                "zero tests)." % (len(self.absent), self.baseline_size)
            )
        for key in self.absent[:_MAX_LISTED]:
            # The CAUSE, not a guess at it. Before playhead-tl6l every absent
            # member was reported as a rename, which sent the reader looking for
            # a rename that had not happened.
            cause = ("no verdict — the host died mid-test"
                     if key in self.absent_crashed
                     else "renamed, deleted or newly skipped")
            out.append("  DID NOT RUN      %s  (%s)" % (key, cause))
        if len(self.absent) > _MAX_LISTED:
            out.append("  DID NOT RUN      … and %d more"
                       % (len(self.absent) - _MAX_LISTED))
        if self.baseline_fiction:
            out.append(
                "  BASELINE IS FICTION — the run had zero failures while %d are "
                "recorded as known-broken." % len(self.known_failures)
            )
        for key in self.load_sensitive_passed:
            out.append("  (passed this run, load-sensitive, removal candidate) %s" % key)

        if self.runs_observed < MIN_RUNS_FOR_DETERMINISTIC:
            out.append(
                "  NOTE: the baseline is unconfirmed — built from %d observation(s). "
                "Nothing can be classed deterministic, so the pass-direction arm is "
                "inert until a second `--accept-baseline` run." % self.runs_observed
            )

        if self.exit_code != EXIT_OK:
            out.append("")
            out.append(
                "The gate is RED for a reason that is NOT in the baseline. Fix it, or —"
            )
            out.append(
                "if the change is intended — refresh the record with "
                "`scripts/fast-gate.sh --accept-baseline` and justify the diff in the"
            )
            out.append(
                "commit message. A shrinking baseline is good news; a growing one needs"
            )
            out.append("a reason.")
        return "\n".join(out)

    def _render_no_verdict(self):
        """The crashed-host block. Its own category, because its REMEDY differs.

        A NEW failure is triaged against the diff. A test with no verdict was
        never judged at all, and the only honest response is to run it again —
        so it must not be folded into NEW, where it would read as something a
        reader could act on by looking at their own change.
        """
        if not self.crashed_host:
            return []
        out = []
        if self.no_verdict:
            out.extend([
                "  NO VERDICT — %d test(s) started and then reported neither pass, "
                "fail nor skip." % len(self.no_verdict),
                "  The run made NO CLAIM about them: they count as neither known nor "
                "NEW, so the",
                "  known/new split above is a verdict about the REST of the plan. "
                "Re-run before",
                "  reading it as one.",
            ])
        else:
            out.append(
                "  NO VERDICT — every test that started reported an outcome, but the "
                "evidence below says part of this run was still lost. Read it before "
                "reading the split above."
            )
        if self.host_restarts:
            out.append(
                "  HOST RESTART     xcodebuild restarted the test host %d time(s): %r"
                % (self.host_restarts, self.restart_evidence or "")
            )
        for key in self.no_verdict[:_MAX_LISTED]:
            out.append("  NO VERDICT       %s" % key)
        if len(self.no_verdict) > _MAX_LISTED:
            out.append("  NO VERDICT       … and %d more"
                       % (len(self.no_verdict) - _MAX_LISTED))
        if self.blamed_distinct:
            out.append(
                "  BLAMED           xcodebuild's `Failing tests:` summary: %d entries, "
                "%d distinct name(s); %d matched no console line in any spelling."
                % (self.blamed_entry_count, len(self.blamed_distinct),
                   len(self.blamed_unmatched))
            )
            out.append(
                "                   A repeated entry is a RETRY, not a second test. "
                "The unmatched list is a LEAD, not a count: the summary spells a test "
                "`Suite.function()` while Swift Testing's console prints its @Test "
                "display name, so a display-named test that reported perfectly well "
                "is unmatched here too. Cross-check it against the census above."
            )
            for name in self.blamed_unmatched[:_MAX_LISTED]:
                out.append("  BLAMED, UNMATCHED %s" % name)
            if len(self.blamed_unmatched) > _MAX_LISTED:
                out.append("  BLAMED, UNMATCHED … and %d more"
                           % (len(self.blamed_unmatched) - _MAX_LISTED))
        return out


def _blamed_is_matched(entry, identities):
    """Can this `Failing tests:` entry be tied to a console identity BY NAME?

    Only two spellings are ever comparable, and both are exact — no fuzzy
    matching, because a false match here hides a casualty and a false miss
    manufactures one:

      * XCTest, whose console key is `Target.Suite/method`; and
      * a Swift Testing test with NO custom display name, whose console line
        prints the bare `function()`.

    A Swift Testing test WITH a display name is unmatchable in principle and is
    reported as such rather than guessed at. See the module docstring.
    """
    m = _BLOCK_NAME.match(entry)
    if not m:
        return False
    suite, method = m.group(1), m.group(2)
    if st_key(method + "()") in identities:
        return True
    tail = "." + suite + "/" + method
    return any(key.startswith(FRAMEWORK_XCTEST + "::")
               and (key.endswith(tail) or key == xc_key(suite, method))
               for key in identities)


def verdict(baseline, run, plan=None):
    """Compare one run against the baseline. Pure; the CLI only prints it."""
    result = Verdict()
    result.runs_observed = baseline.get("runs_observed", 0)
    result.total_failures = len(run.failures)
    result.baseline_size = len(baseline.get("tests", {}))

    if plan is not None and baseline.get("plan") != plan:
        result.cannot_evaluate = (
            "the baseline was recorded for plan '%s' but this run is '%s'. "
            "Different plans run different populations." % (baseline.get("plan"), plan)
        )
        return result

    if not run.complete:
        result.cannot_evaluate = (
            "the log is incomplete — no terminal verdict, so every test after the "
            "cut looks like it never ran"
        )
        return result

    # playhead-tl6l. Computed before the baseline comparison so the ABSENT arm
    # below can name the CAUSE it now knows.
    no_verdict = run.no_verdict
    result.no_verdict = sorted(no_verdict)
    result.host_restarts = run.host_restarts
    result.restart_evidence = run.restart_evidence
    blamed = run.blamed
    result.blamed_entry_count = len(run.blamed_entries)
    result.blamed_distinct = blamed
    identities = run.ran | run.passed | run.skipped | run.started
    result.blamed_unmatched = [name for name in blamed
                               if not _blamed_is_matched(name, identities)]

    entries = baseline.get("tests", {})

    for key in sorted(run.failures):
        failure = run.failures[key]
        entry = entries.get(key)
        if entry is None:
            result.new_failures.append(key)
            continue
        known = set(entry.get("kinds", []))
        unexpected = failure.kinds - known
        if unexpected:
            result.kind_changed.append(key)
            result.kind_detail[key] = "recorded as %s, failed as %s" % (
                "/".join(sorted(known)) or "?", "/".join(sorted(unexpected))
            )
        else:
            result.known_failures.append(key)

    for key in sorted(entries):
        if key in run.failures:
            continue
        if key in run.passed:
            if tier_of(entries[key]) == TIER_DETERMINISTIC:
                result.deterministic_passed.append(key)
            else:
                result.load_sensitive_passed.append(key)
        else:
            result.absent.append(key)
            if key in no_verdict:
                result.absent_crashed.add(key)

    if entries and not run.failures:
        result.baseline_fiction = True

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _read(path):
    return pathlib.Path(str(path)).read_text(encoding="utf-8", errors="replace")


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="gate_baseline.py",
        description="Judge a gate run against the committed baseline of known failures.",
    )
    sub = parser.add_subparsers(dest="command")

    for name in ("check", "accept"):
        p = sub.add_parser(name)
        p.add_argument("--log", required=True)
        p.add_argument("--baseline", required=True)
        p.add_argument("--plan", default=None)

    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return EXIT_CANNOT_EVALUATE

    log_path = pathlib.Path(args.log)
    if not log_path.exists():
        sys.stderr.write("gate-baseline: no such log: %s\n" % log_path)
        return EXIT_CANNOT_EVALUATE

    run = parse_run(_read(log_path))
    baseline_path = pathlib.Path(args.baseline)

    if args.command == "accept":
        plan = args.plan or "PlayheadFastTests"
        if baseline_path.exists():
            base = load_baseline(baseline_path)
        else:
            base = empty_baseline(plan)
        try:
            merged = merge(base, run, plan=plan)
        except CannotEvaluate as exc:
            sys.stderr.write("gate-baseline: REFUSING to accept — %s\n" % exc)
            sys.stderr.write("gate-baseline: the baseline file was NOT written.\n")
            return EXIT_CANNOT_EVALUATE
        added = sorted(set(merged["tests"]) - set(base.get("tests", {})))
        removed = sorted(set(base.get("tests", {})) - set(merged["tests"]))
        promoted, demoted = tier_changes(base, merged)
        save_baseline(baseline_path, merged)
        print("gate-baseline: wrote %s" % baseline_path)
        print("  plan=%s  observations=%d  known-broken=%d"
              % (merged["plan"], merged["runs_observed"], len(merged["tests"])))
        if added:
            census = kind_census([merged["tests"][key] for key in added])
            print("  added %d: %s" % (
                len(added),
                ", ".join("%d %s" % (census[label], label) for label in sorted(census)),
            ))
        # The KIND rides on every added line. The justification an operator
        # writes for this accept is a claim about these entries, and a claim
        # about their kinds is only checkable if the kinds were on screen.
        for key in added:
            print("  + [%s] %s" % (_kinds_label(merged["tests"][key]), key))
        for key in removed:
            print("  - %s" % key)
        if not added and not removed:
            print("  (membership unchanged; counts updated)")
        # playhead-tl6l: say what the crash cost this observation. An accept is
        # a claim a human signs in a commit message, and "27 of these entries
        # were never actually observed" is part of the claim.
        no_verdict = run.no_verdict
        if no_verdict:
            protected = sorted(set(no_verdict) & set(merged["tests"]))
            print(
                "  NO VERDICT: the host died and %d test(s) reported nothing%s. This "
                "observation says nothing about them."
                % (len(no_verdict),
                   " (xcodebuild restarted the test host %d time(s))" % run.host_restarts
                   if run.host_restarts else "")
            )
            if protected:
                print(
                    "  CARRIED FORWARD: %d recorded entr%s kept unchanged rather than "
                    "pruned — a crash is not a rename, and dropping them here is how "
                    "the file would shrink without anyone deciding to shrink it."
                    % (len(protected), "y was" if len(protected) == 1 else "ies were")
                )
                for key in protected[:_MAX_LISTED]:
                    print("  = %s" % key)
                if len(protected) > _MAX_LISTED:
                    print("  = … and %d more" % (len(protected) - _MAX_LISTED))
        if promoted:
            print(
                "  ARMED: %d entr%s crossed into DETERMINISTIC — failed in every one "
                "of their observations. Each of these PASSING now fails the gate, so "
                "say in the commit message why that is the right reading."
                % (len(promoted), "y" if len(promoted) == 1 else "ies")
            )
            for key in promoted:
                entry = merged["tests"][key]
                print("  ! now deterministic [%s] %d/%d  %s" % (
                    _kinds_label(entry), entry["failed_runs"], entry["seen_runs"], key,
                ))
        for key in demoted:
            print("  ~ no longer deterministic  %s" % key)
        return EXIT_OK

    if not baseline_path.exists():
        sys.stderr.write(
            "gate-baseline: CANNOT EVALUATE — no baseline at %s. Create one with "
            "`scripts/fast-gate.sh --accept-baseline`.\n" % baseline_path
        )
        return EXIT_CANNOT_EVALUATE

    base = load_baseline(baseline_path)
    result = verdict(base, run, plan=args.plan)
    print(result.render())
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
