#!/usr/bin/env python3
"""Does a run that LOSES ITS HOST also reach the descriptor ceiling? (playhead-vk68m)

playhead-s34ux established that the test host reaches its `RLIMIT_NOFILE` soft
limit of 2,560, and made `fast-gate.sh` print the peak against that BINDING
limit on every run. That turns a lead into something checkable from logs already
on disk: at the ceiling every `open()` in the process fails, including ones
inside Apple frameworks never written to expect it, and the s34ux merge gate of
2026-08-24 lost its host with `[BiomeStorage] Failed to open lockfile` repeating
immediately before the restart marker.

A host restart on this box is STOCHASTIC (playhead-3rql: four completed, two
killed in one day on identical code), so one co-occurrence is not causation.
This builds the contingency table instead.

WHAT IS COUNTED, AND WHAT IS NOT:

  * Only logs that carry a `peak open fds` line. That line is younger than the
    fd instrumentation, so n is SMALL by construction and the report says so.
  * Only the LAST xcodebuild invocation in each log, via
    `gate_memory_verdict.last_invocation` — a log can hold the wedged-sim retry
    and the residual re-run, each with its own host pid, and unioning them
    reports one-pid-per-invocation as a mid-run restart.
  * Logs are DE-DUPLICATED by the sha256 of their bytes. The same run is often
    preserved twice (once in the worktree, once copied to the artifacts
    directory) and counting it twice would inflate n in whichever cell it lands.
  * A run whose fd line says the BINDING soft limit was not in the log is
    reported separately and never folded into a percentage — the two
    denominators differ by 24x here and a share cannot be told apart by eye.

The verdict categories are `gate_memory_verdict`'s own, so this cannot disagree
with what the gate printed at the time.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_HERE, filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gmv = _load("gmv", "gate_memory_verdict.py")

# `gate-memory: test host peak open fds 2539 of RLIMIT_NOFILE soft 2560 (99.2 % of it)`
_FD_LINE = re.compile(
    r"peak open fds (\d+) of (RLIMIT_NOFILE soft|kern\.maxfilesperproc) (\d+|unknown)"
)
# The Swift-side probe prints the soft limit even on runs whose gate line used
# the fallback denominator, so it is a second route to the same number.
_PROBE_SOFT = re.compile(r"\[s34ux-fd\][^\n]*RLIMIT_NOFILE soft=(\d+)")
# COUNT EVERY STARTED TEST, NOT ONLY THE NAMED ONES (playhead-vk68m review R4).
# This was `◇ Test ["\']`, which requires a quoted DISPLAY NAME — and a `@Test`
# with no display name prints `◇ Test sendDiagnosticsPerformsNoNetworkIO()
# started.` with no quote at all. Measured on one preserved log: `◇ Test `
# appears 12,208 times and only 11,533 carry a quote, so the pattern dropped
# **674 real tests** in order to exclude the ONE `◇ Test run started.` banner it
# was presumably aimed at. It also put this column 674 apart from
# `gate_memory_verdict.inflight_at_end`, which counts the same event on the same
# log with the unquoted pattern — two instruments, one quantity, no agreement.
# The banner is excluded by name instead.
_STARTED = re.compile(r"◇ Test (?!run started\.)")
# `gate-baseline: RED (5 known / 3 NEW) — ... 27 tests hit a RESOURCE FAILURE`
# The gate's own count of tests DENIED A FILE. This replaces a `re.I` count of
# lines containing the word RESOURCE, which was never printed, never written to
# the CSV, and named nothing: on the run1 log that count is the gate's own
# explanatory prose (dozens of lines) while the casualty figure is 27, so a
# reader who took one for the other would have been out by more than 3x. -1 is
# `not recorded` — a restarted run never reaches the line at all, and 0 there
# would claim a clean run.
_RESOURCE_CASUALTIES = re.compile(r"(\d+) tests? hit a RESOURCE FAILURE")


def classify_run(text: str) -> dict:
    """One row: the fd peak, the denominator it was measured against, the fate.

    The fd and probe lines fall back to the WHOLE text when the last invocation
    does not carry them. `fast-gate.sh` prints the `peak open fds` line once, at
    the very end, so in practice it is always inside the last invocation; the
    Swift probe line is printed by the test host mid-run and a residual re-run
    can leave it in an earlier one, which is what the fallback is for. Both are
    properties of the RUN rather than of an invocation, so unioning them is
    sound where unioning host pids (below) is not.
    """
    tail, invocations = gmv.last_invocation(text)
    match = _FD_LINE.search(tail) or _FD_LINE.search(text)
    if match is None:
        return {}
    peak = int(match.group(1))
    denominator = match.group(2)
    try:
        ceiling = int(match.group(3))
    except ValueError:
        ceiling = -1
    probe = _PROBE_SOFT.search(tail) or _PROBE_SOFT.search(text)
    soft = int(probe.group(1)) if probe else -1
    casualties = _RESOURCE_CASUALTIES.search(tail) or _RESOURCE_CASUALTIES.search(text)
    pids = gmv.host_pids(tail)
    verdict = gmv.reached_a_verdict(tail)
    signal_death = gmv.killed_by_signal(tail)
    if signal_death or not verdict:
        fate = "NO-VERDICT"
    elif len(pids) > 1:
        fate = "RESTARTED"
    else:
        fate = "COMPLETE"
    return {
        "peak": peak,
        "denominator": denominator,
        "ceiling": ceiling,
        "probe_soft": soft,
        "host_pids": len(pids),
        "reached_verdict": verdict,
        "signal": signal_death or "",
        "fate": fate,
        "invocations": invocations,
        "started": len(_STARTED.findall(tail)),
        "resource_casualties": int(casualties.group(1)) if casualties else -1,
    }


def contingency(rows: list[dict], ceiling_pct: float) -> dict[tuple[bool, bool], int]:
    """{(at_ceiling, lost_the_host): n} over rows that carry a BINDING limit.

    `at` is `share >= ceiling_pct`, not `>`: the threshold names the band a run
    is IN, so a run at exactly the threshold is at the ceiling. Rows with no
    binding soft limit are absent from the table entirely — never folded in
    against `kern.maxfilesperproc`, which is 24x larger.

    ONLY `RESTARTED` COUNTS AS LOSING THE HOST, AND THE REASON IS THIS TABLE'S
    WHOLE SUBJECT (playhead-vk68m review round 4). The predicate used to be
    `fate != "COMPLETE"`, which folds `NO-VERDICT` in beside `RESTARTED` — and
    on this box `NO-VERDICT` is the `Killed: 9` / exit-137 signature that
    playhead-3rql measured and attributed to MEMORY: a booted simulator costs
    10-13 GiB of a 16 GiB box, and one full plan in three was killed regardless
    of which tests ran. Counting a memory death as evidence in a table built to
    ask whether the DESCRIPTOR ceiling kills hosts would answer the question
    with the other resource's failures. It does not bite on today's population —
    no row is NO-VERDICT — which is exactly why it had to be fixed before one
    is, rather than after.

    A NO-VERDICT row is not silently dropped either: it is returned under its
    own key so `main` can report it as neither arm.
    """
    table: dict[tuple[bool, bool], int] = {}
    for row in rows:
        pct = share(row)
        if pct < 0:
            continue
        if row["fate"] == "NO-VERDICT":
            table[("no-verdict", pct >= ceiling_pct)] = (
                table.get(("no-verdict", pct >= ceiling_pct), 0) + 1)
            continue
        key = (pct >= ceiling_pct, row["fate"] == "RESTARTED")
        table[key] = table.get(key, 0) + 1
    return table


def share(row: dict) -> float:
    """peak / the BINDING limit, or -1 when this log does not carry it.

    Never falls back to `kern.maxfilesperproc`: 2,539 is 99.2 % of the soft
    limit and 4.1 % of the kernel cap, and reading one as the other is the
    mistake that closed s34ux as refuted.
    """
    ceiling = row["probe_soft"] if row["probe_soft"] > 0 else (
        row["ceiling"] if row["denominator"] == "RLIMIT_NOFILE soft" else -1)
    if ceiling <= 0:
        return -1.0
    return 100.0 * row["peak"] / ceiling


def collect(roots: list[str]) -> tuple[list[tuple[str, dict]], int]:
    """`(rows, unparsed)` — see `main`'s headline for why the second exists."""
    seen: dict[str, str] = {}
    rows: list[tuple[str, dict]] = []
    unparsed = 0
    for root in roots:
        if not os.path.isdir(root):
            continue
        for dirpath, _dirnames, filenames in os.walk(root):
            for name in filenames:
                if not name.endswith(".log"):
                    continue
                path = os.path.join(dirpath, name)
                try:
                    if os.path.getsize(path) < 100 * 1024:
                        continue
                    with open(path, "rb") as fh:
                        raw = fh.read()
                except OSError:
                    continue
                if b"peak open fds" not in raw:
                    continue
                digest = hashlib.sha256(raw).hexdigest()
                if digest in seen:
                    continue
                seen[digest] = path
                row = classify_run(raw.decode("utf-8", "replace"))
                if row:
                    rows.append((path, row))
                else:
                    unparsed += 1
    return rows, unparsed


def report(rows: list[tuple[str, dict]], ceiling_pct: float, unparsed: int) -> None:
    """Print the listing and the contingency table.

    Split out of `main` so the TABLE — the only thing this tool exists to
    produce — can be driven by a rail. It could not be: `main` was one
    unbroken function, so swapping the two columns of the table left every
    test green (playhead-vk68m review round 4).
    """
    # NAME WHAT WAS COUNTED. `rows` holds logs whose fd line the REGEX parsed;
    # the sha256 pre-filter admits any log carrying the literal `peak open fds`.
    # A log that carries the literal in a wording `_FD_LINE` does not know is
    # dropped silently, and the old headline called `len(rows)` "logs carrying a
    # `peak open fds` line", which is the pre-filter's population, not this one
    # (playhead-vk68m review R4). Both numbers are printed so a gap is visible.
    print(f"logs whose `peak open fds` line PARSED, de-duplicated by content: "
          f"{len(rows)}")
    if unparsed:
        print(f"  ...and {unparsed} log(s) carried the literal `peak open fds` "
              f"in a wording this parser does not know — NOT counted anywhere else")
    print()
    header = (f"{'peak':>6} {'share':>7} {'fate':<11} {'pids':>4} {'started':>8} "
              f"{'denied':>7} {'inv':>3}  log")
    print(header)
    print("-" * len(header))
    for path, row in rows:
        pct = share(row)
        share_text = "   n/a " if pct < 0 else f"{pct:6.1f}%"
        denied = row["resource_casualties"]
        denied_text = "      -" if denied < 0 else f"{denied:>7}"
        print(f"{row['peak']:>6} {share_text} {row['fate']:<11} {row['host_pids']:>4} "
              f"{row['started']:>8} {denied_text} {row['invocations']:>3}  "
              f"{os.path.basename(path)}")

    measurable = [(p, r) for p, r in rows if share(r) >= 0]
    unmeasurable = len(rows) - len(measurable)
    print(f"\n{unmeasurable} log(s) carry no BINDING soft limit and are EXCLUDED from "
          f"the table rather than folded in against the kernel cap.\n")

    table = contingency([r for _p, r in measurable], ceiling_pct)
    n = sum(table.values())
    print(f"CONTINGENCY TABLE, n = {n}  (ceiling = >= {ceiling_pct:.0f} % of the binding soft limit)")
    print(f"{'':<22}{'lost the host':>15}{'completed':>12}")
    for at in (True, False):
        label = "AT the ceiling" if at else "below the ceiling"
        print(f"  {label:<20}{table.get((at, True), 0):>15}{table.get((at, False), 0):>12}")
    voided = sum(v for k, v in table.items() if k[0] == "no-verdict")
    if voided:
        print(f"\n  {voided} run(s) reached NO VERDICT and are in NEITHER arm: on this "
              f"box that is the `Killed: 9` MEMORY signature (playhead-3rql), and a "
              f"memory death is not evidence about the descriptor ceiling.")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("roots", nargs="*", default=[
        "/private/tmp", "/Users/dabrams/playhead",
        "/Users/dabrams/.claude", "/Users/dabrams/playhead-gate-artifacts",
        os.environ.get("TMPDIR", "/tmp"),
    ])
    ap.add_argument("--ceiling-pct", type=float, default=90.0,
                    help="a run is AT THE CEILING at or above this share of the "
                         "BINDING soft limit (default 90, the gate's own threshold)")
    ap.add_argument("--csv", default="")
    args = ap.parse_args(argv)

    rows, unparsed = collect(args.roots)
    rows.sort(key=lambda item: item[1]["peak"], reverse=True)

    report(rows, args.ceiling_pct, unparsed)

    if args.csv:
        with open(args.csv, "w") as fh:
            # `share_pct` and `binding_soft` are -1 when the log carries no
            # binding limit, and `resource_casualties` is -1 when the gate never
            # printed its RESOURCE line. -1 is `not recorded`; 0 in any of the
            # three would be a claim about the run.
            fh.write("log,peak,share_pct,denominator,binding_soft,fate,host_pids,"
                     "reached_verdict,signal,started,resource_casualties,"
                     "invocations\n")
            for path, row in rows:
                fh.write(f"{path},{row['peak']},{share(row):.2f},{row['denominator']},"
                         f"{row['probe_soft']},{row['fate']},{row['host_pids']},"
                         f"{row['reached_verdict']},{row['signal']},{row['started']},"
                         f"{row['resource_casualties']},{row['invocations']}\n")
        print(f"\nwrote {args.csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
