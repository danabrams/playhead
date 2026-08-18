#!/usr/bin/env python3
"""Distil a 7.2 MB full-plan gate log into a committed fixture — playhead-fer3.

WHY THIS EXISTS. `RealCrashedRunTests` is the only place anything proves
`gate_baseline.py` reads a REAL crashed run rather than a fixture built to match
its own parser. Those rails used to point at a session-specific scratchpad and
`skipTest` when it was gone, so the suite reported `117 tests, 1 skipped, OK`
with its highest-value rails inert — a rail that reports OK when it did not run
is the same defect class as a gate reporting an all-clear for a run that lost
part of the plan.

Committing the logs is not an option (14 MB for two). Distilling them is, and
the reduction is chosen so the numbers the rails assert come out UNCHANGED:

  * every line carrying a FAILURE, an ISSUE or a SKIP is kept — that fixes the
    known/new arithmetic and the skip subtraction exactly;
  * every line belonging to a test that lost its verdict is kept;
  * every line belonging to a test the COMMITTED BASELINE names is kept, even
    when it merely passed. Without this the fixture reads 32 recorded members
    as ABSENT where the full log reads none, and ABSENT is fatal — the one
    quantity a naive distillation gets wrong, in the direction that would have
    the fixture claim a regression the real run did not have;
  * every SPLICED line is kept BYTE-EXACT, together with the line that carries
    its displaced tail — these are the whole point, and paraphrasing one would
    turn the rail back into a fixture built to match the parser;
  * `Failing tests:`, the host-restart marker and the terminal verdict are kept;
  * a start/pass PAIR for an otherwise uninteresting test is dropped WHOLE.
    Dropping both halves together is what leaves `started - ran - skipped`
    untouched, and it is where all the size goes: 11,000 passing tests.

Regenerate with:

    python3 scripts/tests/make_crashed_run_fixture.py <log> <out.log>

The acceptance test is not this script — it is `RealCrashedRunTests`, which
asserts the distilled census equals the census measured on the full log.
"""
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import gate_baseline as gb  # noqa: E402

_GLYPH = re.compile(r"[◇✔✘➜]")
_KEEP_MARKERS = (
    "Restarting after unexpected exit, crash, or test timeout",
    "Failing tests:",
    "** TEST FAILED **",
    "** TEST SUCCEEDED **",
)


def _identity(line):
    """The key a parseable test line refers to, or None."""
    for pattern in (gb._ST_ISSUE_NAMED, gb._ST_FAIL_NAMED, gb._ST_PASS_NAMED,
                    gb._ST_SKIP_NAMED, gb._ST_START_NAMED):
        m = pattern.search(line)
        if m:
            return gb.st_key(m.group(1))
    for pattern in (gb._ST_ISSUE_FUNC, gb._ST_FAIL_FUNC, gb._ST_PASS_FUNC,
                    gb._ST_SKIP_FUNC, gb._ST_START_FUNC):
        m = pattern.search(line)
        if m:
            return gb.st_key(m.group(1))
    m = gb._XC_RESULT.search(line) or gb._XC_START.search(line)
    if m:
        return gb.xc_key(m.group(1), m.group(2))
    return None


def distil(text, baseline_keys=()):
    raw = text.splitlines()
    run = gb.parse_run(text)
    interesting = (set(run.no_verdict) | set(run.failures) | run.skipped
                   | set(baseline_keys))

    # EVERY line an app-log write landed in the middle of, plus the line
    # carrying its displaced tail. Not just the ones the rejoin repairs: a cut
    # that lands after ` after` leaves a head that still parses, and those are
    # the four R1 review found. Both classes are the point of these fixtures, so
    # both are kept byte-exact whether or not the current parser needs help.
    #
    # THE TAIL IS NOT ALWAYS ON LINE N+1 (playhead-phn3). This loop assumed it
    # was, which is the same premise the repair itself carried and the same one
    # that cost an otherwise-green merge gate four phantom casualties. Keeping
    # only N and N+1 drops the tail of a wider splice, and the distillation then
    # grows a casualty the source run did not have — caught by this script's own
    # refusal rather than shipped, but a refusal with no explanation is a bad
    # afternoon. The span comes from the parser's own walk so the two cannot
    # drift; when it finds nothing, N+1 is kept exactly as before.
    spliced = set()
    tails = {}
    for number, line in enumerate(raw):
        match = gb._APP_LOG_INTRUSION.search(line)
        if match and match.start() > 0 and gb._REPAIRABLE_HEAD.search(line[:match.start()]):
            head = line[:match.start()]
            span = gb._displaced_tail_span(raw, number, head) or 1
            tails[number] = span
            for offset in range(0, span + 1):
                if number + offset < len(raw):
                    spliced.add(number + offset)

    # A KEPT SPLICED LINE DRAGS ITS TEST IN WITH IT. Found by this script's own
    # refusal on the third real log (playhead-tl6l R4): the splice landed in two
    # tests' STARTED lines, which are kept byte-exact by the rule above, while
    # their PASS lines were dropped as uninteresting — and the distillation grew
    # two crashed-host casualties out of two tests that passed. That is the
    # fixture manufacturing the exact defect the fixture exists to measure.
    # Whatever a spliced line refers to, in any of its three readable forms, is
    # interesting; keeping more lines can only ever be safe.
    # The REPAIRED form is the one that names the test, and it is `head +
    # following` with the intrusion cut out — not `raw[n] + raw[n+1]`, which
    # still has the app log wedged in the middle and parses as nothing. Getting
    # that wrong leaves the refusal firing with no explanation, which is how
    # this was found the second time.
    for number in sorted(spliced):
        forms = [raw[number]]
        match = gb._APP_LOG_INTRUSION.search(raw[number])
        span = tails.get(number, 1)
        if number + span < len(raw):
            forms.append(raw[number + span])
            if match:
                forms.append(raw[number][:match.start()] + raw[number + span])
        for form in forms:
            key = _identity(form)
            if key is not None:
                interesting.add(key)

    kept = []
    in_block = False
    for number, line in enumerate(raw):
        if in_block:
            if gb._BLOCK_ENTRY.match(line):
                kept.append(line)
                continue
            in_block = False
        if gb._FAILING_TESTS_HEADER.match(line):
            in_block = True
            kept.append(line)
            continue
        if any(marker in line for marker in _KEEP_MARKERS):
            kept.append(line)
            continue
        if number in spliced:
            kept.append(line)
            continue
        if not _GLYPH.search(line) and "Test Case '-[" not in line:
            continue
        key = _identity(line)
        if key is not None and key in interesting:
            kept.append(line)
    return "\n".join(kept) + "\n"


def main(argv):
    if len(argv) not in (3, 4):
        sys.stderr.write(__doc__)
        return 2
    source = pathlib.Path(argv[1])
    target = pathlib.Path(argv[2])
    baseline_path = pathlib.Path(argv[3]) if len(argv) == 4 else (
        pathlib.Path(__file__).resolve().parents[1]
        / "gate-baseline.PlayheadFastTests.json"
    )
    baseline_keys = gb.load_baseline(baseline_path).get("tests", {}).keys()
    text = source.read_text(encoding="utf-8", errors="replace")
    distilled = distil(text, baseline_keys)

    before = gb.parse_run(text)
    after = gb.parse_run(distilled)
    verdict_before = gb.verdict(gb.load_baseline(baseline_path), before,
                                plan="PlayheadFastTests")
    verdict_after = gb.verdict(gb.load_baseline(baseline_path), after,
                               plan="PlayheadFastTests")
    if verdict_before.absent != verdict_after.absent:
        sys.stderr.write(
            "REFUSING to write: the ABSENT set differs (%d vs %d).\n"
            % (len(verdict_before.absent), len(verdict_after.absent))
        )
        return 1
    if set(before.no_verdict) != set(after.no_verdict):
        sys.stderr.write(
            "REFUSING to write: the distilled census differs.\n  lost: %s\n  gained: %s\n"
            % (sorted(set(before.no_verdict) - set(after.no_verdict)),
               sorted(set(after.no_verdict) - set(before.no_verdict)))
        )
        return 1
    if set(before.failures) != set(after.failures):
        sys.stderr.write("REFUSING to write: the failure set differs.\n")
        return 1
    target.write_text(distilled, encoding="utf-8")
    print("wrote %s — %d lines, %d bytes (from %d lines, %d bytes)"
          % (target, len(distilled.splitlines()), len(distilled.encode("utf-8")),
             len(text.splitlines()), len(text.encode("utf-8"))))
    print("  no_verdict=%d  failures=%d  blamed=%d entries/%d distinct  restarts=%d"
          % (len(after.no_verdict), len(after.failures), len(after.blamed_entries),
             len(after.blamed), after.host_restarts))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
