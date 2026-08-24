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
import ctypes
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


# ---------------------------------------------------------------------------
# playhead-s34ux: the test host's OPEN FILE DESCRIPTORS.
# ---------------------------------------------------------------------------
# Full-plan gates were failing with `SQLITE_CANTOPEN` in a ROTATING set of
# suites, and one run failed to open a `.swift` SOURCE file in the same
# breath -- a shape no database explanation covers. The hypothesis was
# descriptor exhaustion in the single test host, and it was UNMEASURED: the
# bead named `lsof -p <pid> | wc -l` as the cheap thing to do.
#
# Two corrections to that recipe, both measured here rather than assumed,
# and both this repo's standing defect class (a value that names one thing
# read as though it named another):
#
#   * `lsof -p PID | wc -l` IS NOT THE OPEN-FD COUNT. lsof also lists `cwd`,
#     `rtd`, `txt` and every mapped dylib, none of which occupy a descriptor.
#     Measured on a python process holding 3 descriptors, lsof printed 16
#     lines; after opening 100 files it printed 116 and the real count was
#     103. So the lsof figure over-reports by a constant ~13 here and by
#     however many images a 400-dylib test host has mapped -- which is
#     exactly the direction that would manufacture a false "exhausted".
#
#   * `proc_pidinfo(PROC_PIDLISTFDS)` CALLED WITH A NULL BUFFER RETURNS THE
#     TABLE'S CAPACITY, NOT THE LIVE COUNT. Measured: 3 fds -> reported 45;
#     open 100 -> reported 148; close all 100 -> STILL reported 148. It is a
#     high-water allocation and it never falls, so reading it as "open fds"
#     would report a leak on a process that has none.
#
# So the count is taken the only way that is accurate: PROC_PIDLISTFDS with a
# real buffer, counting the entries the kernel actually fills in. That is one
# syscall, measured at 1-17 us, cheap enough to sit on the default path.
#
# A read that FAILS records -1, never 0. A printed zero would read as "the
# host had no descriptors open", i.e. it would invent a refutation of the
# very hypothesis this column exists to test.

_LIBC = ctypes.CDLL(None, use_errno=True)
PROC_PIDLISTFDS = 1

# `proc_fdtype` values from <sys/proc_info.h>. Only the ones a test host is
# expected to hold in bulk are named; anything else lands in `other`, because
# a bucket that silently absorbs an unknown type is a bucket that cannot
# report a surprise.
PROX_FDTYPE_VNODE = 1
PROX_FDTYPE_SOCKET = 2
PROX_FDTYPE_KQUEUE = 5
PROX_FDTYPE_PIPE = 6


class _ProcFDInfo(ctypes.Structure):
    _fields_ = [("proc_fd", ctypes.c_int32), ("proc_fdtype", ctypes.c_uint32)]


_FDINFO_SIZE = ctypes.sizeof(_ProcFDInfo)


def fd_sample(pid: int) -> dict[str, int]:
    """Open descriptors held by `pid`, by kind, plus the highest fd number.

    Every value is -1 when the table could not be read at all (no pid, the
    process exited, EPERM across a uid boundary). -1 is `not recorded`; 0 is
    a claim.

    `max_fd` is worth having next to `count`: descriptor numbers are handed
    out lowest-free-first, so `max_fd` tracks the high-water mark while
    `count` tracks what is held right now. count low + max_fd high means
    descriptors ARE being closed; the two rising together is accumulation.
    """
    unknown = {
        "count": -1, "vnode": -1, "socket": -1,
        "kqueue": -1, "pipe": -1, "other": -1, "max_fd": -1, "capacity": -1,
    }
    if pid <= 0:
        return unknown
    capacity = _LIBC.proc_pidinfo(
        ctypes.c_int(pid), ctypes.c_int(PROC_PIDLISTFDS),
        ctypes.c_uint64(0), None, ctypes.c_int(0),
    )
    if capacity <= 0:
        return unknown
    # Head-room: the table can grow between the sizing call and the read, and
    # a short buffer silently TRUNCATES (proc_pidinfo returns what it wrote,
    # with no error), which would under-report a host that is filling up.
    capacity += _FDINFO_SIZE * 256
    buffer = (_ProcFDInfo * (capacity // _FDINFO_SIZE))()
    written = _LIBC.proc_pidinfo(
        ctypes.c_int(pid), ctypes.c_int(PROC_PIDLISTFDS),
        ctypes.c_uint64(0), ctypes.byref(buffer), ctypes.c_int(capacity),
    )
    if written <= 0:
        return unknown
    entries = written // _FDINFO_SIZE
    sample = {
        "count": entries, "vnode": 0, "socket": 0,
        "kqueue": 0, "pipe": 0, "other": 0, "max_fd": -1,
        "capacity": capacity // _FDINFO_SIZE,
    }
    kinds = {
        PROX_FDTYPE_VNODE: "vnode", PROX_FDTYPE_SOCKET: "socket",
        PROX_FDTYPE_KQUEUE: "kqueue", PROX_FDTYPE_PIPE: "pipe",
    }
    for index in range(entries):
        entry = buffer[index]
        sample[kinds.get(entry.proc_fdtype, "other")] += 1
        if entry.proc_fd > sample["max_fd"]:
            sample["max_fd"] = entry.proc_fd
    return sample


def system_file_table() -> tuple[int, int]:
    """(kern.num_files, kern.maxfiles), or (-1, -1).

    The per-process ceiling is not the only one. A SYSTEM-wide file table that
    is full fails an open with ENFILE, which surfaces identically to the
    per-process EMFILE at every layer above it -- and this box runs a ~300
    process simulator alongside the host. Sampling both is what makes the two
    distinguishable after the fact instead of arguable.
    """
    out = subprocess.run(
        ["sysctl", "-n", "kern.num_files", "kern.maxfiles"],
        capture_output=True, text=True, check=False,
    ).stdout.split()
    try:
        return int(out[0]), int(out[1])
    except (IndexError, ValueError):
        return -1, -1


def disk_free_mib(path: str = "/System/Volumes/Data") -> int:
    """Free space on the volume the swapfile lives on.

    Swap is the only thing standing between this box and a memorystatus kill,
    and swap is a FILE. A run can therefore die of memory because the DISK ran
    out — which reads as neither, unless both are on the same series.
    scripts/gate-disk-sample.sh measures this for the gate's own artefacts; this
    column exists so a memory reading is never interpreted without it.
    """
    try:
        st = os.statvfs(path)
    except OSError:
        return -1
    return int(st.f_bavail * st.f_frsize) // MIB


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
    "testhost_pid",
    "disk_free_mib",
    "testhost_fds",
    "testhost_fd_vnode",
    "testhost_fd_socket",
    "testhost_fd_kqueue",
    "testhost_fd_pipe",
    "testhost_fd_other",
    "testhost_fd_max",
    "testhost_fd_capacity",
    "sys_num_files",
    "sys_max_files",
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
            fds = fd_sample(host_pid)
            num_files, max_files = system_file_table()
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
                host_pid,
                disk_free_mib(),
                fds["count"],
                fds["vnode"],
                fds["socket"],
                fds["kqueue"],
                fds["pipe"],
                fds["other"],
                fds["max_fd"],
                fds["capacity"],
                num_files,
                max_files,
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
