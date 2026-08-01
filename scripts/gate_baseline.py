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
   at least two observations makes it DETERMINISTIC; anything else is
   LOAD-SENSITIVE. Nobody hand-labels a test flaky — the file records counts and
   the tier falls out. One observation can never mint a deterministic entry,
   because one run cannot tell "always fails" from "starved once".

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

# One observation cannot distinguish "fails every time" from "was starved once",
# so a tier is only promoted to deterministic once there are two to compare.
MIN_RUNS_FOR_DETERMINISTIC = 2

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

_XC_RESULT = re.compile(
    r"Test Case '-\[([A-Za-z0-9_.]+) ([A-Za-z0-9_:]+)\]' (failed|passed) \(([\d.]+) seconds\)"
)
_XC_START = re.compile(r"Test Case '-\[([A-Za-z0-9_.]+) ([A-Za-z0-9_:]+)\]' started")

_TERMINAL = re.compile(r"\*\* TEST (FAILED|SUCCEEDED) \*\*|Test run with \d+ tests? in \d+ suites? (?:failed|passed) after")

_WEDGED_SIM = "fast-gate: wedged simulator"

_TIME_LIMIT = "Time limit was exceeded"


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
        self.started = set()
        self.complete = False

    @property
    def ran(self):
        """Keys with a definite outcome. Started-but-silent is NOT an outcome."""
        return set(self.failures) | self.passed


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

    for line in last_attempt(text).splitlines():
        if not run.complete and _TERMINAL.search(line):
            run.complete = True

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

        m = _ST_START_NAMED.search(line) or _ST_START_FUNC.search(line)
        if m:
            run.started.add(st_key(m.group(1)))
            continue

        # --- XCTest --------------------------------------------------------
        m = _XC_RESULT.search(line)
        if m:
            suite, method, outcome, seconds = m.groups()
            key = xc_key(suite, method)
            if outcome == "failed":
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

    @property
    def ok(self):
        return self.exit_code == EXIT_OK

    @property
    def exit_code(self):
        if self.cannot_evaluate:
            return EXIT_CANNOT_EVALUATE
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
        if self.total_failures == 0 and not self.new_failures:
            out.append("gate-baseline: GREEN (%s)" % headline)
        else:
            out.append("gate-baseline: RED (%s)" % headline)

        for key in self.new_failures:
            out.append("  NEW FAILURE      %s" % key)
        for key in self.kind_changed:
            out.append("  FAILS DIFFERENTLY %s — %s" % (key, self.kind_detail.get(key, "")))
        for key in self.deterministic_passed:
            out.append("  NOW PASSES       %s  (recorded as failing every run)" % key)
        for key in self.absent:
            out.append("  DID NOT RUN      %s  (renamed, deleted or newly skipped)" % key)
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


def verdict(baseline, run, plan=None):
    """Compare one run against the baseline. Pure; the CLI only prints it."""
    result = Verdict()
    result.runs_observed = baseline.get("runs_observed", 0)
    result.total_failures = len(run.failures)

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
        save_baseline(baseline_path, merged)
        print("gate-baseline: wrote %s" % baseline_path)
        print("  plan=%s  observations=%d  known-broken=%d"
              % (merged["plan"], merged["runs_observed"], len(merged["tests"])))
        for key in added:
            print("  + %s" % key)
        for key in removed:
            print("  - %s" % key)
        if not added and not removed:
            print("  (membership unchanged; counts updated)")
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
