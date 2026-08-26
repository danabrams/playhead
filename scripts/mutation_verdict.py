#!/usr/bin/env python3
"""Score one mutation batch's artifacts — playhead-gjlp0.

WHY THIS EXISTS
---------------
`scripts/mutation-battery.sh` used to decide SURVIVED by the ABSENCE of a
failure line:

    if   the expected name is not in the STARTED roster -> ERROR "never ran"
    elif the expected name is not in the FAILURE list   -> SURVIVED

Both arms are readings of silence, and the second one is the dangerous
direction. A test whose host died emits `◇ … started` and then nothing at all,
so it scores exactly like a test that ran and passed — and a SURVIVED verdict
is the one that sends the next person to write a test that already exists.

MEASURED, on the specimen this bead was filed from
(`/private/tmp/playhead-mutation-battery.F6R3wB`, 2026-08-23 04:14):

    the expected test        10 `◇ … started` lines, ZERO `✔`/`✘` lines
    the batch                2,688 started · 356 passed · 0 failed · 2,332 NO VERDICT
    the host                 11 distinct pids, i.e. 10 replacements
    the log's own last page  `gate-memory: THE RUN DID NOT REACH A VERDICT — RESTARTED`
    `failed-1457.txt`        0 bytes
    the verdict printed      `BD37 SURVIVED … still green: …`

Applied by hand, that mutant dies deterministically in 0.147 s. The rail
existed and worked; only the reading was wrong. This is `playhead-t53a` one
tool over — the gate's census moved to the `.xcresult` bundle for exactly this
reason and the battery never followed.

THE LADDER, AND WHAT EACH STATE READS IF THE THING IT MEASURES NEVER HAPPENED
----------------------------------------------------------------------------
Per expected test:

    FAILED      a STATED failure verdict exists for this name.
                If the mutation changed nothing, nothing states a failure -> not FAILED.
    PASSED      a STATED pass verdict exists for this name.
                If the test never got to report, nothing states a pass -> not PASSED.
                This is the whole fix: `SURVIVED` now requires a positive `✔`.
    NO-VERDICT  the name is in the roster and NOTHING judged it.
                Reads as NO-VERDICT precisely when no instrument judged it.
    CRASHED     the bundle STATES the host died under this test.
    DENIED      the bundle STATES a resource was denied to it (playhead-s34ux).
    SKIPPED     it was skipped; a skipped rail asked no question.
    ABSENT      no roster mentions it at all — a harness fault, not a survivor.

Per batch:

    OK          one test host, a terminal marker, no signal death.
    VOID        the host was replaced, or died, or the run never reported.
                A batch with host restarts is not a verdict about any mutation.

`OK` is the only state that can be reached by silence, and it is deliberately
the one that is CHECKED POSITIVELY: it requires a terminal marker in the log
(`** TEST SUCCEEDED/FAILED **` or `Test run with N tests …`), so a truncated
log is VOID rather than clean.

WHERE THE FACTS COME FROM
-------------------------
Nothing here is a fifth parser. `scripts/gate_baseline.py` already reads both
console formats, rejoins spliced verdict lines, decodes octal-escaped glyphs
and takes verdicts from the `.xcresult` bundle; `scripts/gate_memory_verdict.py`
already classifies a run's health off the test host's own pid. Both are
imported. This module is the mapping from those facts onto the battery's
question, and nothing else.

    the VERDICT      the .xcresult bundle when there is one, else the console
    the ROSTER       the console's `◇ … started` lines, UNIONED with the bundle
    the HEALTH       distinct test-host pids (the app's own testimony), plus the
                     restart marker as a boolean, plus a signal death, plus
                     whether either format reported an outcome at all

COUNT FROM THE PID, NOT FROM THE PHRASE. On the specimen above,
`grep -c 'Restarting after unexpected exit'` returns 11 while the host was
replaced 10 times: the eleventh hit is `gate-memory:`'s own verdict block
QUOTING the message back into the same log. A count of a phrase that the
run's own diagnostics also print is a count of two different things added
together. The pid series says 11 distinct hosts and needs no interpretation.

FRESHNESS: A LOG IS EVIDENCE ONLY IF THIS RUN PRODUCED IT
---------------------------------------------------------
`mutation-battery.sh` creates `WORK` per INVOCATION and sets `KEEP_WORK=1` on
every failure path, so failed runs leave their whole directory behind — 52 of
them on this box on 2026-08-25, 684 MiB, carrying EIGHT different mutation
series. (Read the 52 as a reading taken at one moment: the same afternoon it
was 56 and then 53, because other worktrees are producing and reaping them
while you look. Only the directory THIS invocation created is a fixed
quantity, which is the whole argument below.) Batch
numbers are assigned per mutant and are NOT unique across beads or across time,
so a lookup by batch number finds another investigation's evidence: playhead-8cjo
measured its checker finding a three-day-old `batch-1414.log` from
playhead-2d6i's vacuity control and reporting OK.

So every artifact this module reads must be NEWER than a floor the caller
states (`--since`), and one that is not is REFUSED BY NAME AND DATE rather than
scored. The battery passes the epoch it captured immediately before launching
the batch, which makes the floor a claim about THIS batch's invocation and not
merely about this run.
"""

import argparse
import os
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import gate_baseline as gb            # noqa: E402
import gate_memory_verdict as gmv     # noqa: E402


# --- the states, spelled once ---------------------------------------------
FAILED = "FAILED"
PASSED = "PASSED"
NO_VERDICT = "NO-VERDICT"
CRASHED = "CRASHED"
DENIED = "DENIED"
SKIPPED = "SKIPPED"
ABSENT = "ABSENT"

BATCH_OK = "OK"
BATCH_VOID = "VOID"

#: States that are POSITIVE evidence — an instrument stated this outcome.
STATED = frozenset((FAILED, PASSED))

#: States that mean "nobody judged this test". Every one of them must keep a
#: mutation out of both KILLED and SURVIVED: a test that was not judged is not
#: evidence in either direction.
UNJUDGED = frozenset((NO_VERDICT, CRASHED, DENIED, SKIPPED))

EXIT_OK = 0
EXIT_VOID = 3
EXIT_CANNOT_EVALUATE = 2


class CannotEvaluate(Exception):
    """The artifacts cannot support any verdict. Never a silent fallback."""


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------
def _stamp(epoch):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch))


def require_fresh(path, since, what):
    """Refuse an artifact older than `since`, naming it and its date.

    The refusal is by NAME AND DATE deliberately. "stale artifact" sends a
    reader looking for the wrong file; `batch-1414.log is from 2026-08-22
    17:44, this batch started 2026-08-25 08:59` tells them which run they have
    in their hand.
    """
    p = pathlib.Path(str(path))
    if not p.exists():
        raise CannotEvaluate("%s does not exist: %s" % (what, p))
    mtime = p.stat().st_mtime
    if mtime < since:
        raise CannotEvaluate(
            "%s is STALE — %s was last written %s, before this batch started at %s.\n"
            "Batch numbers repeat across beads and `KEEP_WORK=1` leaves failed runs'\n"
            "directories behind, so a log found by batch number is very often another\n"
            "investigation's. Pin the log to the RUN (its own $WORK), not to the number."
            % (what, p, _stamp(mtime), _stamp(since))
        )
    return p


# ---------------------------------------------------------------------------
# Reading the run
# ---------------------------------------------------------------------------
class BatchReading(object):
    """Everything one batch's artifacts say, in the battery's own terms."""

    def __init__(self):
        self.run = None
        self.batch_state = BATCH_OK
        self.batch_reasons = []
        self.verdict_source = gb.VERDICT_SOURCE_CONSOLE
        self.host_pids = []
        self.restart_marker = False
        self.log_path = None
        self.bundle_path = None

    @property
    def host_replacements(self):
        """Replacements, not hosts. One host is zero replacements."""
        return max(len(self.host_pids) - 1, 0)


def read_batch(log_path, since, xcresult=None, rc=0, xcresult_reader=None):
    """Parse one batch's log (and bundle) into a BatchReading.

    `xcresult` absent is not an error — a build that never reached the test
    phase writes no bundle, and fast-gate itself only passes `--xcresult` when
    there is one. `xcresult` present and UNREADABLE is a hard error: falling
    back to the console on a bad path would reinstate the defect this module
    exists to delete, invisibly.
    """
    reading = BatchReading()
    log = require_fresh(log_path, since, "the batch log")
    reading.log_path = str(log)
    text = log.read_text(encoding="utf-8", errors="replace")

    reading.run = gb.parse_run(text)

    scoped, _invocations = gmv.last_invocation(text)
    reading.host_pids = gmv.host_pids(scoped)
    reading.restart_marker = "Restarting after unexpected exit" in scoped

    if xcresult:
        bundle_dir = require_fresh(xcresult, since, "the result bundle")
        reading.bundle_path = str(bundle_dir)
        payload = gb.read_xcresult(bundle_dir, runner=xcresult_reader)
        bundle = gb.parse_xcresult(payload)
        gb.with_xcresult_verdicts(reading.run, bundle)
    reading.verdict_source = reading.run.verdict_source

    _classify_batch(reading, scoped, rc)
    return reading


def _classify_batch(reading, scoped, rc):
    """OK or VOID, and why.

    Reasons ACCUMULATE rather than short-circuit: a run that lost its host AND
    was killed by a signal should say both, because the remedies differ.
    """
    reasons = reading.batch_reasons

    if len(reading.host_pids) > 1:
        reasons.append(
            "the test host was REPLACED %d time(s) mid-batch: pids %s"
            % (reading.host_replacements, " -> ".join(reading.host_pids))
        )
    if reading.restart_marker:
        reasons.append(
            "xcodebuild printed `Restarting after unexpected exit, crash, or "
            "test timeout`"
        )
    signal = gmv.killed_by_signal(scoped)
    if signal:
        reasons.append(
            "the shell reported `%s` — the kernel killed xcodebuild itself" % signal
        )
    if rc in (137, 143):
        reasons.append(
            "xcodebuild exited %d (128 + %s)"
            % (rc, "SIGKILL" if rc == 137 else "SIGTERM")
        )
    if not gmv.reached_a_verdict(scoped):
        reasons.append(
            "neither format reported an outcome: no `Test run with N tests …` "
            "line and no `** TEST SUCCEEDED/FAILED **` line"
        )
    if reading.run.crashed:
        # The KEY, framework prefix stripped. `run.crashed` is keyed, not named
        # — printing the key raw would put `swift-testing::` in front of a test
        # name and invite a reader to search for a name that is not spelled
        # that way anywhere else.
        example = sorted(reading.run.crashed)[0].split("::", 1)[-1]
        reasons.append(
            "the .xcresult bundle STATES the host died under %d test(s), e.g. %s"
            % (len(reading.run.crashed), example)
        )
    reading.batch_state = BATCH_VOID if reasons else BATCH_OK


# ---------------------------------------------------------------------------
# Resolving one expectation
# ---------------------------------------------------------------------------
def candidate_keys(run, want):
    """Every key spelling `want` could name, restricted to keys this run knows.

    The battery's expectations are Swift Testing DISPLAY names, and since
    playhead-le02 also XCTest method names written either bare (`testFoo`) or
    qualified (`SomeTests/testFoo`). `extract_failures` used to register both
    spellings for every XCTest result; this reproduces that, from the run's own
    key space instead of from a second regex.

    THE SUITE IS SPELLED MODULE-QUALIFIED ON ONE SIDE AND BARE ON THE OTHER, AND
    A FIRST CUT OF THIS FUNCTION COMPARED THEM DIRECTLY. `gate_baseline` keys an
    XCTest case off the WHOLE bracketed name — `xctest::PlayheadTests.SomeTests/
    testFoo` — because its `_XC_RESULT` group is greedy over `[A-Za-z0-9_.]+`.
    The MUTATIONS table spells the same test `SomeTests/testFoo`, because the
    scraper this module replaced used a NON-greedy prefix and threw the module
    away. Compared literally, every one of the 48 mutations whose sole
    expectation is written that way resolved ABSENT — and ABSENT is the arm that
    exits 2 out of the baseline preflight with "an expectation names a test that
    never ran", so the XCTest half of the battery refused to run at all. So the
    bare spelling is matched as a dotted SUFFIX of the key's suite, which is
    exactly what `gate_baseline._blamed_matches` already does for the
    `Failing tests:` block's own third spelling. The leading `.` is what keeps
    it exact: `.SomeTests/testFoo` cannot match `…OtherSomeTests/testFoo`.
    """
    known = (set(run.started) | set(run.passed) | set(run.failures)
             | set(run.skipped) | set(run.crashed) | set(run.resource))
    out = []

    st = gb.st_key(want)
    if st in known:
        out.append(st)

    prefix = gb.FRAMEWORK_XCTEST + "::"
    if "/" in want:
        suite, method = want.split("/", 1)
        xc = gb.xc_key(suite, method)
        if xc in known:
            out.append(xc)
        tail = "." + suite + "/" + method
        for key in sorted(known):
            if key.startswith(prefix) and key.endswith(tail):
                out.append(key)
    else:
        for key in sorted(known):
            if key.startswith(prefix) and key.rsplit("/", 1)[-1] == want:
                out.append(key)
    return out


def _state_of_key(run, key):
    # FAILURES FIRST, and the order is load-bearing. `parse_xcresult` has
    # already lifted crashed and resource-denied keys OUT of `failures`, so a
    # key still in there is a stated failure and nothing else — which is what
    # keeps a dead host from being credited as a KILL.
    if key in run.failures:
        return FAILED
    if key in run.crashed:
        return CRASHED
    if key in run.resource:
        return DENIED
    if key in run.skipped:
        return SKIPPED
    if key in run.passed:
        return PASSED
    return NO_VERDICT


#: Worst-first. When one expectation resolves to several keys — two same-named
#: tests in different suites share a Swift Testing key, and an XCTest method
#: name can be written two ways — the answer is the WORSE one, except that a
#: stated FAILURE outranks everything because it is the one thing that is
#: positive evidence the rail fired.
_PRECEDENCE = (FAILED, CRASHED, DENIED, NO_VERDICT, SKIPPED, PASSED)


def state_of(run, want):
    keys = candidate_keys(run, want)
    if not keys:
        return ABSENT
    states = {_state_of_key(run, key) for key in keys}
    for state in _PRECEDENCE:
        if state in states:
            return state
    return NO_VERDICT       # unreachable; a state not in _PRECEDENCE is a bug


def classify(reading, names):
    """[(name, state)] in the order given."""
    return [(name, state_of(reading.run, name)) for name in names]


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def render(reading, outcomes, out):
    run = reading.run
    print("  verdicts read from: %s" % reading.verdict_source, file=out)
    print(
        "  batch census: %d started · %d passed · %d failed · %d skipped · "
        "%d NO VERDICT · %d crashed · %d resource-denied"
        % (len(run.started), len(run.passed), len(run.failures), len(run.skipped),
           len(run.no_verdict), len(run.crashed), len(run.resource)),
        file=out,
    )
    print(
        "  test hosts: %d distinct pid(s) = %d replacement(s); restart marker %s"
        % (len(reading.host_pids), reading.host_replacements,
           "PRESENT" if reading.restart_marker else "absent"),
        file=out,
    )
    if reading.batch_state == BATCH_VOID:
        print("  BATCH IS VOID — it cannot support a verdict about any mutation:", file=out)
        for reason in reading.batch_reasons:
            print("    * %s" % reason, file=out)
    for name, state in outcomes:
        print("    %-10s %s" % (state, name), file=out)


def write_outcomes(path, reading, outcomes):
    """`STATE<TAB>NAME`, plus `#` header lines the shell reads with `sed`.

    A file rather than stdout because a Swift Testing display name may contain
    anything except a newline — quoting one through a shell pipeline is how the
    `;`-in-a-name fault got into this battery in the first place.
    """
    run = reading.run
    lines = [
        "#batch\t%s" % reading.batch_state,
        "#source\t%s" % reading.verdict_source,
        "#log\t%s" % reading.log_path,
        "#bundle\t%s" % (reading.bundle_path or "(none — console only)"),
        "#hosts\t%d" % len(reading.host_pids),
        "#no_verdict\t%d" % len(run.no_verdict),
        "#failures\t%d" % len(run.failures),
    ]
    for reason in reading.batch_reasons:
        lines.append("#reason\t%s" % reason)
    # EVERY failure in the batch, not only the expected ones. The baseline
    # guard needs this: a focused suite that is red on a test no mutation names
    # still means every verdict in the run is worthless, and reporting only the
    # named ones would hide exactly that.
    for key in sorted(run.failures):
        lines.append("#failure\t%s" % run.failures[key].name)
    for name, state in outcomes:
        lines.append("%s\t%s" % (state, name))
    pathlib.Path(str(path)).write_text("\n".join(lines) + "\n", encoding="utf-8")


def read_names(path):
    text = pathlib.Path(str(path)).read_text(encoding="utf-8")
    names = [line for line in text.split("\n") if line != ""]
    # The outcome file is TAB-separated and the shell reads it with awk, so a
    # name carrying a TAB would be silently truncated into a name that matches
    # nothing — which is how the `;`-in-a-display-name fault got into this
    # battery in the first place. Refuse rather than mangle.
    bad = [n for n in names if "\t" in n]
    if bad:
        raise CannotEvaluate(
            "an expected test name contains a TAB, which this file format "
            "cannot carry: %r" % bad[0])
    return names


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command")

    classify_p = sub.add_parser(
        "classify", help="score one batch's expected tests against its artifacts")
    classify_p.add_argument("--log", required=True)
    classify_p.add_argument("--xcresult", default=None)
    classify_p.add_argument("--rc", type=int, default=0)
    classify_p.add_argument(
        "--since", type=int, required=True,
        help="epoch seconds; an artifact older than this is REFUSED, not scored")
    classify_p.add_argument("--names", required=True, help="file, one expected name per line")
    classify_p.add_argument("--out", required=True, help="file to write STATE<TAB>NAME into")

    verify_p = sub.add_parser(
        "verify",
        help="freshness only: refuse an artifact this run did not produce")
    verify_p.add_argument("--log", required=True)
    verify_p.add_argument("--xcresult", default=None)
    verify_p.add_argument("--since", type=int, required=True)

    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return EXIT_CANNOT_EVALUATE

    try:
        if args.command == "verify":
            require_fresh(args.log, args.since, "the batch log")
            if args.xcresult:
                require_fresh(args.xcresult, args.since, "the result bundle")
            print("mutation-verdict: artifacts are from this run (floor %s)"
                  % _stamp(args.since))
            return EXIT_OK

        names = read_names(args.names)
        reading = read_batch(args.log, args.since, xcresult=args.xcresult, rc=args.rc)
        outcomes = classify(reading, names)
        write_outcomes(args.out, reading, outcomes)
        render(reading, outcomes, sys.stdout)
        return EXIT_VOID if reading.batch_state == BATCH_VOID else EXIT_OK
    except CannotEvaluate as exc:
        print("mutation-verdict: CANNOT EVALUATE — %s" % exc, file=sys.stderr)
        return EXIT_CANNOT_EVALUATE
    except gb.XcresultUnreadable as exc:
        print("mutation-verdict: CANNOT EVALUATE — the result bundle was asked "
              "for and could not be read: %s" % exc, file=sys.stderr)
        return EXIT_CANNOT_EVALUATE


if __name__ == "__main__":
    sys.exit(main())
