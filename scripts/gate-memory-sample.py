#!/usr/bin/env python3
"""Sample system and per-process memory for the whole of a gate run.

The precedent is scripts/gate-disk-sample.sh, which samples `df` every 5 s so a
transient drawdown can be measured rather than inferred from an end-state
reading.  This does the same for memory, and exists because playhead-3rql had
exactly one memory reading -- taken at the end of a run -- and a single point
cannot tell a leak from a late spike.

Two things it deliberately does that a bare `vm_stat` loop does not:

  * It records the COMPRESSOR and SWAP, not just `Pages free`.  On macOS
    `Pages free` is not free memory: the kernel keeps very few free pages by
    design and reclaims `inactive` on demand.  A box at rest here reads ~1 GiB
    free.  Quoting `Pages free` as "free memory" is this repo's standing defect
    class -- a value that names one thing read as though it named another.
    `available = free + inactive + speculative + purgeable` is the reclaimable
    figure; compressor size and swapouts are what actually say the box is short.

  * It records PER-PROCESS RSS for the test host, xcodebuild, and the simulator,
    so a run that dies of memory names WHICH process grew.  Aggregate pressure
    cannot distinguish "the test host accumulates across 11,600 tests" from
    "xcodebuild accumulates result data" -- and those have different fixes.

Usage:  gate-memory-sample.py OUT.csv [--interval 10] [--log GATE.log]

`--log` records that file's size at every sample, which is what aligns the
series against the log byte offset a run dies at.
"""

from __future__ import annotations

import argparse
import os
import re
import signal
import subprocess
import sys
import time

MIB = 1024 * 1024


def vm_stat_sample() -> dict[str, int]:
    out = subprocess.run(["vm_stat"], capture_output=True, text=True, check=False).stdout
    m = re.search(r"page size of (\d+) bytes", out)
    page = int(m.group(1)) if m else 4096
    vals: dict[str, int] = {}
    for line in out.splitlines():
        if ":" not in line:
            continue
        key, _, raw = line.partition(":")
        raw = raw.strip().rstrip(".")
        if not raw.isdigit():
            continue
        vals[key.strip()] = int(raw)
    get = lambda k: vals.get(k, 0)  # noqa: E731
    counters = {
        # These two are cumulative event counts, not page counts.
        "swapins": get("Swapins"),
        "swapouts": get("Swapouts"),
    }
    pages = {
        "free": get("Pages free"),
        "active": get("Pages active"),
        "inactive": get("Pages inactive"),
        "speculative": get("Pages speculative"),
        "wired": get("Pages wired down"),
        "purgeable": get("Pages purgeable"),
        "compressor": get("Pages occupied by compressor"),
    }
    sample = {k: v * page // MIB for k, v in pages.items()}
    sample.update(counters)
    return sample


def swap_used_mib() -> int:
    out = subprocess.run(
        ["sysctl", "-n", "vm.swapusage"], capture_output=True, text=True, check=False
    ).stdout
    m = re.search(r"used\s*=\s*([\d.]+)([MG])", out)
    if not m:
        return 0
    value = float(m.group(1))
    return int(value * 1024) if m.group(2) == "G" else int(value)


CLASSES = (
    # (column, predicate on the executable path)
    ("xcodebuild", lambda p: p.endswith("/xcodebuild") or p == "xcodebuild"),
    # The test host in the simulator IS the app; PlayheadTests.xctest is injected.
    ("testhost", lambda p: "/Playhead.app/" in p),
    ("simulator", lambda p: "/CoreSimulator/" in p or ".simruntime/" in p),
    ("compiler", lambda p: p.endswith(("/swift-frontend", "/clang", "/swiftc", "/ld"))),
)


def process_sample() -> tuple[dict[str, int], list[tuple[int, str]]]:
    out = subprocess.run(
        ["ps", "-Ao", "rss=,comm="], capture_output=True, text=True, check=False
    ).stdout
    totals = {name: 0 for name, _ in CLASSES}
    rows: list[tuple[int, str]] = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        rss_raw, _, path = line.partition(" ")
        if not rss_raw.isdigit():
            continue
        rss_mib = int(rss_raw) // 1024
        path = path.strip()
        rows.append((rss_mib, path))
        for name, pred in CLASSES:
            if pred(path):
                totals[name] += rss_mib
                break
    rows.sort(reverse=True)
    return totals, rows[:8]


COLUMNS = [
    "epoch",
    "elapsed_s",
    "log_bytes",
    "free_mib",
    "active_mib",
    "inactive_mib",
    "speculative_mib",
    "wired_mib",
    "purgeable_mib",
    "compressor_mib",
    "available_mib",
    "swap_used_mib",
    "swapins",
    "swapouts",
    "xcodebuild_mib",
    "testhost_mib",
    "simulator_mib",
    "compiler_mib",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--interval", type=float, default=10.0)
    ap.add_argument("--log", default="")
    args = ap.parse_args()

    top_path = args.out + ".top"
    start = time.time()
    stop = {"now": False}

    def handle(_sig, _frm):
        stop["now"] = True

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)

    with open(args.out, "w", buffering=1) as csv, open(top_path, "w", buffering=1) as top:
        csv.write(",".join(COLUMNS) + "\n")
        while not stop["now"]:
            now = time.time()
            vm = vm_stat_sample()
            procs, biggest = process_sample()
            log_bytes = 0
            if args.log:
                try:
                    log_bytes = os.path.getsize(args.log)
                except OSError:
                    log_bytes = 0
            available = (
                vm["free"] + vm["inactive"] + vm["speculative"] + vm["purgeable"]
            )
            row = [
                int(now),
                round(now - start, 1),
                log_bytes,
                vm["free"],
                vm["active"],
                vm["inactive"],
                vm["speculative"],
                vm["wired"],
                vm["purgeable"],
                vm["compressor"],
                available,
                swap_used_mib(),
                vm["swapins"],
                vm["swapouts"],
                procs["xcodebuild"],
                procs["testhost"],
                procs["simulator"],
                procs["compiler"],
            ]
            csv.write(",".join(str(v) for v in row) + "\n")
            top.write(
                f"{int(now)} "
                + " | ".join(f"{r}MiB {os.path.basename(p)}" for r, p in biggest)
                + "\n"
            )
            # Sleep in slices so a signal is honoured promptly.
            deadline = now + args.interval
            while not stop["now"] and time.time() < deadline:
                time.sleep(0.25)
    return 0


if __name__ == "__main__":
    sys.exit(main())
