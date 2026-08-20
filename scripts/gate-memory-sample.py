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


def process_sample() -> tuple[dict[str, int], list[tuple[int, str]], int]:
    out = subprocess.run(
        ["ps", "-Ao", "pid=,rss=,comm="], capture_output=True, text=True, check=False
    ).stdout
    totals = {name: 0 for name, _ in CLASSES}
    rows: list[tuple[int, str]] = []
    host_pid = 0
    host_rss = -1
    for line in out.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 3 or not parts[0].isdigit() or not parts[1].isdigit():
            continue
        pid = int(parts[0])
        rss_mib = int(parts[1]) // 1024
        path = parts[2].strip()
        rows.append((rss_mib, path))
        for name, pred in CLASSES:
            if pred(path):
                totals[name] += rss_mib
                if name == "testhost" and rss_mib > host_rss:
                    host_rss, host_pid = rss_mib, pid
                break
    rows.sort(reverse=True)
    return totals, rows[:8], host_pid


def footprint_mib(pid: int) -> int:
    """Physical footprint of one process, in MiB, or -1.

    RSS is not the quantity that matters under pressure: a page the compressor
    has taken is no longer resident, so a process whose footprint is growing can
    show a FLAT or FALLING rss while the compressor fills. That is the standing
    defect class in instrument form, so the sampler measures both.
    """
    if not pid:
        return -1
    try:
        out = subprocess.run(
            ["/usr/bin/footprint", "-p", str(pid)],
            capture_output=True, text=True, check=False, timeout=20,
        ).stdout
    except (OSError, subprocess.TimeoutExpired):
        return -1
    m = re.search(r"Footprint:\s+([\d.]+)\s*([KMG]B)", out)
    if not m:
        return -1
    value = float(m.group(1))
    scale = {"KB": 1 / 1024, "MB": 1.0, "GB": 1024.0}[m.group(2)]
    return int(value * scale)


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
    "testhost_footprint_mib",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--interval", type=float, default=10.0)
    ap.add_argument("--log", default="")
    ap.add_argument(
        "--footprint",
        action="store_true",
        help="also sample the test host's PHYSICAL FOOTPRINT via /usr/bin/footprint. "
             "Costs a vmmap of the target each sample, so it is opt-in: use it on a "
             "diagnostic run, not on a run whose verdict you intend to trust.",
    )
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
            procs, biggest, host_pid = process_sample()
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
                footprint_mib(host_pid) if args.footprint else -1,
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
