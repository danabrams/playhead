#!/usr/bin/env python3
"""Say, in memory terms, why a gate run never reached a verdict (playhead-3rql).

## The defect this exists to remove

Six consecutive merge gates produced NO VERDICT across two branches. Each one
ended as a bare `exit 137` or a `Restarting after unexpected exit` line, and
each was triaged by hand. The conclusion the team reached from those six runs
was that a specific bead's 38 new tests were expensive, and the controlled
experiment that settled it (keep those 38, remove 38 UNRELATED ones) still died
at the same point. The tests were never the variable.

What the runs could not say for themselves is the thing that was actually true:

    this box has 16 GiB of RAM; a booted iOS 27 simulator costs 10-13 GiB of
    memory demand while completely idle; macOS at rest costs 6-10 GiB; and the
    whole test run adds about 2 GiB on top of that.

Measured 2026-08-20 (playhead-3rql), sampling `vm_stat` + `vm.swapusage` every
10 s across whole runs and around bare simulator boots:

    sim shut down                         6.4 - 9.8 GiB demand
    sim booted, idle, settled 5 min      16.5 - 20.5 GiB demand, 200-290 procs
    full gate, app launched, pre-tests   20.9 GiB demand
    full gate, at the moment it died     20.5 GiB demand

Demand is `active + wired + compressor + swap`. It is FLAT across the test
phase: the run is already at the box's ceiling before the first Swift Testing
test starts. That is why "which tests ran" does not predict the outcome.

## What this prints, and why it is a reporter rather than a gate

It cannot refuse a run into existence. What it removes is the reconstruction:
a run that dies now ends by naming WHICH process died, WHERE in the run, and
what the box's memory was doing at the time, with the series on disk beside the
log. A future occurrence should cost minutes, not a day.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import subprocess
import sys

MIB = 1024 * 1024


# --------------------------------------------------------------------------
# reading the log
# --------------------------------------------------------------------------

def host_pids(text: str) -> list[str]:
    """The test-host pids that appear in the console, in first-seen order.

    More than one means the host was replaced. This is deliberately NOT the
    `Restarting after unexpected exit` marker: that line is xcodebuild's, it is
    buffered, and a run has been observed losing its host with the marker never
    printed at all (playhead-3rql's EXP1). The pid is the app's own testimony.
    """
    seen: list[str] = []
    for match in re.finditer(r"Playhead\[(\d+):", text):
        pid = match.group(1)
        if pid not in seen:
            seen.append(pid)
    return seen


def reached_a_verdict(text: str) -> bool:
    """Did the run report an outcome for the population, in EITHER format?

    Swift Testing prints `Test run with N tests ... passed|failed after`, and
    xcodebuild prints `** TEST SUCCEEDED **` / `** TEST FAILED **`. Reading only
    one of the two is this repo's oldest gate-triage mistake (2026-07-31), so
    both count and either is enough.
    """
    if re.search(r"Test run with [\d,]+ tests.*?(passed|failed) after", text):
        return True
    return bool(re.search(r"^\*\* TEST (SUCCEEDED|FAILED) \*\*", text, re.M))


def killed_by_signal(text: str) -> str | None:
    """The shell's report of a signal death, e.g. `Killed: 9`, or None."""
    match = re.search(r"\b(Killed: 9|Terminated: 15|Abort trap: 6)\b", text)
    return match.group(1) if match else None


def inflight_at_end(text: str) -> tuple[int, int]:
    """(started, finished) Swift Testing test cases seen on the console.

    A coarse figure on purpose. The console splices app output into these lines
    (playhead-t53a), so the difference is an estimate of what was in flight and
    not a census — the census belongs to the .xcresult. It is quoted here only
    to say WHERE in the schedule the run stopped.
    """
    started = len(re.findall(r"◇ Test ", text))
    finished = len(re.findall(r"[✔✘] Test ", text))
    return started, finished


# --------------------------------------------------------------------------
# reading the box
# --------------------------------------------------------------------------

def ram_mib() -> int:
    try:
        out = subprocess.run(
            ["sysctl", "-n", "hw.memsize"], capture_output=True, text=True, check=False
        ).stdout.strip()
        return int(out) // MIB
    except (OSError, ValueError):
        return 0


def series_rows(path: str) -> list[dict[str, str]]:
    if not path or not os.path.exists(path):
        return []
    with open(path, newline="") as handle:
        return list(csv.DictReader(handle))


def demand_mib(row: dict[str, str]) -> int:
    """active + wired + compressor + swap.

    NOT `Pages free`, and not `available`. macOS keeps very few free pages by
    design and reclaims `inactive` on demand, so a box at rest here reads about
    1 GiB free and is not short of anything. Quoting `Pages free` as free memory
    is this repo's standing defect class — a value that names one thing read as
    though it named another — and playhead-3rql was filed with that reading in
    it. Demand is what the box is being asked to hold.
    """
    def get(key: str) -> int:
        try:
            return int(row.get(key, 0))
        except (TypeError, ValueError):
            return 0
    return get("active_mib") + get("wired_mib") + get("compressor_mib") + get("swap_used_mib")


def split_at_last_progress(rows: list[dict[str, str]]) -> tuple[list, list]:
    """(during the run, after it stopped producing output).

    The split matters because the most expensive memory event in a killed run
    is the AFTERMATH, not the run: xcodebuild spends minutes collecting
    diagnostics from a 200-process simulator (spindump), and on playhead-3rql's
    EXP1 that pushed swap from 8.3 GiB to 30.2 GiB. Reporting one peak over the
    whole series answers "how much memory did the tests need?" with a number
    measured after they had already stopped — the standing defect class. So the
    peak is reported twice, and each is labelled with what it is a peak OF.
    """
    def size_of(row) -> int:
        try:
            return int(row.get("log_bytes", 0))
        except (TypeError, ValueError):
            return 0

    # The boundary is the last sample in which the log grew APPRECIABLY. A bare
    # "last sample in which it grew at all" puts the boundary after the
    # aftermath, because xcodebuild's own epilogue (`Failure collecting
    # diagnostics ...`, a few hundred bytes) is written minutes later and would
    # drag the whole diagnostics phase back inside "the run".
    floor = 1024
    last = 0
    previous = size_of(rows[0]) if rows else 0
    for index, row in enumerate(rows):
        size = size_of(row)
        if size - previous >= floor:
            last = index
        previous = max(previous, size)
    return rows[: last + 1], rows[last + 1 :]


def summarise_series(rows: list[dict[str, str]]) -> dict[str, int]:
    if not rows:
        return {}
    demands = [demand_mib(r) for r in rows]
    def col(name: str) -> list[int]:
        out = []
        for r in rows:
            try:
                out.append(int(r.get(name, 0)))
            except (TypeError, ValueError):
                pass
        return out or [0]
    pids = [p for p in col("testhost_pid") if p]
    has = lambda name: any(name in r for r in rows)  # noqa: E731
    during, after = split_at_last_progress(rows)
    return {
        "demand_peak_during_run": max([demand_mib(r) for r in during] or [0]),
        "demand_peak_after_run": max([demand_mib(r) for r in after] or [0]),
        "has_disk": has("disk_free_mib"),
        "has_footprint": has("testhost_footprint_mib"),
        "has_pid": has("testhost_pid"),
        "demand_start": demands[0],
        "demand_peak": max(demands),
        "swap_peak": max(col("swap_used_mib")),
        "compressor_peak": max(col("compressor_mib")),
        "available_min": min(col("available_mib")),
        "disk_free_min": min([d for d in col("disk_free_mib") if d >= 0] or [-1]),
        "host_footprint_peak": max([f for f in col("testhost_footprint_mib") if f >= 0] or [-1]),
        "host_rss_peak": max(col("testhost_mib")),
        "distinct_host_pids": len(set(pids)),
    }


# --------------------------------------------------------------------------
# the verdict
# --------------------------------------------------------------------------

def classify(text: str, rc: int) -> tuple[str, list[str]]:
    """(verdict, reasons). Verdict is one of COMPLETE / NO-VERDICT / RESTARTED."""
    reasons: list[str] = []
    pids = host_pids(text)
    signal = killed_by_signal(text)
    verdict = "COMPLETE"

    if len(pids) > 1:
        verdict = "RESTARTED"
        reasons.append(
            f"the test host was replaced mid-run: pids {' -> '.join(pids)}"
        )
    if "Restarting after unexpected exit" in text:
        verdict = "RESTARTED"
        reasons.append("xcodebuild printed `Restarting after unexpected exit, crash, or test timeout`")
    if signal:
        verdict = "NO-VERDICT"
        reasons.append(f"the shell reported `{signal}` — the kernel killed xcodebuild itself")
    if rc in (137, 143):
        verdict = "NO-VERDICT"
        reasons.append(f"xcodebuild exited {rc} (128 + {'SIGKILL' if rc == 137 else 'SIGTERM'})")
    if not reached_a_verdict(text):
        verdict = "NO-VERDICT"
        reasons.append(
            "neither format reported an outcome: no `Test run with N tests ...` "
            "line and no `** TEST SUCCEEDED/FAILED **` line"
        )
    return verdict, reasons


def report(log_path: str, rc: int, series_path: str, out=sys.stdout) -> int:
    try:
        with open(log_path, "rb") as handle:
            text = handle.read().decode("utf-8", "replace")
    except OSError as exc:
        print(f"gate-memory: CANNOT EVALUATE — {exc}", file=out)
        return 2

    verdict, reasons = classify(text, rc)
    started, finished = inflight_at_end(text)
    rows = series_rows(series_path)
    stats = summarise_series(rows)
    ram = ram_mib()

    if verdict == "COMPLETE":
        line = f"gate-memory: the run reached a verdict (xcodebuild exit {rc})."
        if stats:
            line += (
                f" Peak demand {stats['demand_peak_during_run'] / 1024:.1f} GiB"
                f" of {ram / 1024:.1f} GiB RAM; swap peaked at"
                f" {stats['swap_peak'] / 1024:.1f} GiB."
            )
        print(line, file=out)
        return 0

    print("", file=out)
    print(f"gate-memory: THE RUN DID NOT REACH A VERDICT — {verdict}", file=out)
    for reason in reasons:
        print(f"gate-memory:   * {reason}", file=out)
    if started:
        print(
            f"gate-memory:   * the console shows {started} test case(s) started and "
            f"{finished} finished, so it stopped with roughly {started - finished} in flight",
            file=out,
        )

    if not stats:
        print(
            "gate-memory: NO MEMORY SERIES was recorded for this run, so this "
            "cannot say whether the box ran out. Re-run with the sampler "
            "(scripts/gate-memory-sample.py) — a verdict about memory taken "
            "after the fact is one reading at the end, and one reading cannot "
            "tell a leak from a spike.",
            file=out,
        )
        return 1

    disk = (
        f"{stats['disk_free_min'] / 1024:.1f} GiB"
        if stats["has_disk"] and stats["disk_free_min"] >= 0
        else "not recorded"
    )
    footprint = (
        f"{stats['host_footprint_peak']} MiB"
        if stats["has_footprint"] and stats["host_footprint_peak"] >= 0
        else "not recorded"
    )
    pidcount = (
        f"{stats['distinct_host_pids']} distinct pid(s)"
        if stats["has_pid"]
        else "pids not recorded"
    )
    print(
        f"gate-memory: box RAM {ram / 1024:.1f} GiB   demand"
        f" {stats['demand_start'] / 1024:.1f} GiB at start ->"
        f" {stats['demand_peak_during_run'] / 1024:.1f} GiB peak WHILE THE RUN WAS"
        f" STILL PRODUCING OUTPUT   (active+wired+compressor+swap)",
        file=out,
    )
    if stats["demand_peak_after_run"] > stats["demand_peak_during_run"]:
        print(
            f"gate-memory: it then reached {stats['demand_peak_after_run'] / 1024:.1f}"
            " GiB AFTER the run went quiet — that is xcodebuild collecting"
            " diagnostics from the simulator, i.e. the aftermath. Do not quote it"
            " as what the tests needed.",
            file=out,
        )
    print(
        f"gate-memory: swap peak {stats['swap_peak'] / 1024:.1f} GiB   compressor peak"
        f" {stats['compressor_peak'] / 1024:.1f} GiB   disk free min {disk}",
        file=out,
    )
    print(
        f"gate-memory: test host peak rss {stats['host_rss_peak']} MiB, peak physical"
        f" footprint {footprint}, {pidcount}",
        file=out,
    )
    if stats["demand_peak_during_run"] > ram:
        print(
            f"gate-memory: DEMAND EXCEEDED RAM by"
            f" {(stats['demand_peak_during_run'] - ram) / 1024:.1f} GiB. This is memory"
            " exhaustion, not a test failure, and no change to WHICH TESTS RAN"
            " will fix it — measured at playhead-3rql, the test host's own"
            " footprint is ~285 MiB and the run adds ~2 GiB to a box already"
            " over its ceiling from the simulator alone.",
            file=out,
        )
    return 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", required=True)
    parser.add_argument("--rc", type=int, default=0)
    parser.add_argument("--series", default="")
    args = parser.parse_args(argv)
    return report(args.log, args.rc, args.series)


if __name__ == "__main__":
    sys.exit(main())
