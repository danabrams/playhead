#!/usr/bin/env python3
"""NAME the test host's open file descriptors, by PATH (playhead-vk68m).

`scripts/gate-memory-sample.py` counts descriptors by KIND — vnode, socket,
kqueue, pipe — which was enough to establish that the test host reaches its
`RLIMIT_NOFILE` soft limit of 2,560 (playhead-s34ux). It cannot say WHAT any of
them are, and that is the question playhead-vk68m exists to answer:

  * the PEAK is transient (3 -> 2,539 -> 453), and
  * the FLOOR is not: 20-27 vnodes before the ramp, 449 vnodes flat for 19
    samples over ~190 s after the test phase, in the SAME process with no
    restart.

Re-derived from `fd-series-s34ux.csv` at review: the pre-ramp plateau is 20
vnodes over five samples with a single reading of 27 immediately before the
climb, and the tail plateau of 449 is 19 consecutive samples, not 22. So the
descriptors acquired during a run and never released are ~422-429 — 16.5-16.8 %
of the budget gone before the next plan opens a single store. Nothing in the
by-kind series names one of them. This does.

--------------------------------------------------------------------------
WHAT THIS COUNTS, AND WHAT IT EXCLUDES — read this before quoting a number
--------------------------------------------------------------------------

The bead named `lsof -p <pid>` as the one-line diagnostic and the sampler's own
header already records why that recipe is a trap, so it is worth being exact
about what is counted here:

  * The population is the process's FILE DESCRIPTOR TABLE, enumerated with
    `proc_pidinfo(PROC_PIDLISTFDS)` into a real buffer — the same call, with the
    same head-room, as `gate-memory-sample.py`. So the COUNT here is directly
    comparable to `testhost_fds` in the memory series.
  * `lsof -p PID | wc -l` is NOT that number. lsof also prints `cwd`, `rtd`,
    `txt` and every mapped dylib, none of which occupies a descriptor. When
    `--cross-check` is passed, lsof is run immediately after the snapshot (two
    calls cannot share an instant) and ONLY its rows whose FD column is a
    NUMBER (`12u`, `13r`, …) are compared; `cwd`/`rtd`/`txt`/`mem`/`NOFD` rows
    are counted separately and reported as EXCLUDED, so the exclusion is
    visible rather than assumed. An lsof that FAILS is reported as a failure,
    never as zero descriptors.
  * A path is read per descriptor with `proc_pidfdinfo(PROC_PIDFDVNODEPATHINFO)`.
    That flavour exists only for VNODE descriptors. Sockets, kqueues and pipes
    have no path and are reported as `<socket>`, `<kqueue>`, `<pipe>` — they are
    not dropped, because a bucket that silently absorbs an unknown kind cannot
    report a surprise.
  * `vip_path` is documented as the TAIL of the path. It is normally complete;
    when the kernel truncates, the record is still reported, marked truncated.
  * A descriptor that closes between the enumeration and its path read yields no
    path. Those are counted as `<gone>` rather than dropped: they are the
    signature of a churning table, and dropping them would understate the count.

--------------------------------------------------------------------------
THE INSTRUMENT PROVES ITSELF BEFORE IT REPORTS ANYTHING
--------------------------------------------------------------------------

A ctypes struct with a wrong offset does not fail — it returns a plausible
string from the wrong bytes, which is this repo's standing defect class in its
purest form. So `self_test()` runs on every invocation, before any sampling:
this process opens a temporary file at a path it chose, reads that descriptor's
path back through the very same code path, and requires an exact match. It also
requires the kernel's returned byte count to equal `sizeof(vnode_fdinfowithpath)`
— a layout that is the wrong SIZE is caught even when it would have produced a
readable string. A failed self-test refuses to sample and exits 3.
"""

from __future__ import annotations

import argparse
import ctypes
import gzip
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time

_LIBC = ctypes.CDLL(None, use_errno=True)

PROC_PIDLISTFDS = 1
PROC_PIDFDVNODEPATHINFO = 2

# `proc_fdtype` values from <sys/proc_info.h>.
PROX_FDTYPE_VNODE = 1
PROX_FDTYPE_SOCKET = 2
PROX_FDTYPE_PSHM = 3
PROX_FDTYPE_PSEM = 4
PROX_FDTYPE_KQUEUE = 5
PROX_FDTYPE_PIPE = 6
PROX_FDTYPE_FSEVENTS = 7
PROX_FDTYPE_NETPOLICY = 9
PROX_FDTYPE_CHANNEL = 10

_KIND_NAMES = {
    PROX_FDTYPE_VNODE: "vnode",
    PROX_FDTYPE_SOCKET: "socket",
    PROX_FDTYPE_PSHM: "pshm",
    PROX_FDTYPE_PSEM: "psem",
    PROX_FDTYPE_KQUEUE: "kqueue",
    PROX_FDTYPE_PIPE: "pipe",
    PROX_FDTYPE_FSEVENTS: "fsevents",
    PROX_FDTYPE_NETPOLICY: "netpolicy",
    PROX_FDTYPE_CHANNEL: "channel",
}

MAXPATHLEN = 1024


class _ProcFDInfo(ctypes.Structure):
    _fields_ = [("proc_fd", ctypes.c_int32), ("proc_fdtype", ctypes.c_uint32)]


class _ProcFileInfo(ctypes.Structure):
    _fields_ = [
        ("fi_openflags", ctypes.c_uint32),
        ("fi_status", ctypes.c_uint32),
        ("fi_offset", ctypes.c_int64),
        ("fi_type", ctypes.c_int32),
        ("fi_guardflags", ctypes.c_uint32),
    ]


class _VinfoStat(ctypes.Structure):
    _fields_ = [
        ("vst_dev", ctypes.c_uint32),
        ("vst_mode", ctypes.c_uint16),
        ("vst_nlink", ctypes.c_uint16),
        ("vst_ino", ctypes.c_uint64),
        ("vst_uid", ctypes.c_uint32),
        ("vst_gid", ctypes.c_uint32),
        ("vst_atime", ctypes.c_int64),
        ("vst_atimensec", ctypes.c_int64),
        ("vst_mtime", ctypes.c_int64),
        ("vst_mtimensec", ctypes.c_int64),
        ("vst_ctime", ctypes.c_int64),
        ("vst_ctimensec", ctypes.c_int64),
        ("vst_birthtime", ctypes.c_int64),
        ("vst_birthtimensec", ctypes.c_int64),
        ("vst_size", ctypes.c_int64),
        ("vst_blocks", ctypes.c_int64),
        ("vst_blksize", ctypes.c_int32),
        ("vst_flags", ctypes.c_uint32),
        ("vst_gen", ctypes.c_uint32),
        ("vst_rdev", ctypes.c_uint32),
        ("vst_qspare", ctypes.c_int64 * 2),
    ]


class _VnodeInfo(ctypes.Structure):
    _fields_ = [
        ("vi_stat", _VinfoStat),
        ("vi_type", ctypes.c_int32),
        ("vi_pad", ctypes.c_int32),
        ("vi_fsid", ctypes.c_int32 * 2),
    ]


class _VnodeInfoPath(ctypes.Structure):
    _fields_ = [
        ("vip_vi", _VnodeInfo),
        # c_ubyte, NOT c_char: ctypes returns a `c_char * N` field already cut
        # at the first NUL, so a NUL search over it can never find one and every
        # path reads as TRUNCATED. The raw bytes are what the kernel wrote.
        ("vip_path", ctypes.c_ubyte * MAXPATHLEN),
    ]


class _VnodeFdInfoWithPath(ctypes.Structure):
    _fields_ = [
        ("pfi", _ProcFileInfo),
        ("pvip", _VnodeInfoPath),
    ]


_VNODE_PATH_SIZE = ctypes.sizeof(_VnodeFdInfoWithPath)
_FDINFO_SIZE = ctypes.sizeof(_ProcFDInfo)


def fd_path(pid: int, fd: int) -> tuple[str, bool]:
    """(path, truncated) for one VNODE descriptor, or ("", False).

    An empty path is `could not read` — the descriptor closed under us, or the
    kernel refused. It is never reported as a path.
    """
    buf = _VnodeFdInfoWithPath()
    written = _LIBC.proc_pidfdinfo(
        ctypes.c_int(pid), ctypes.c_int(fd),
        ctypes.c_int(PROC_PIDFDVNODEPATHINFO),
        ctypes.byref(buf), ctypes.c_int(_VNODE_PATH_SIZE),
    )
    if written != _VNODE_PATH_SIZE:
        return "", False
    raw = bytes(bytearray(buf.pvip.vip_path))
    end = raw.find(b"\0")
    truncated = end < 0
    if truncated:
        end = len(raw)
    return raw[:end].decode("utf-8", "replace"), truncated


def self_test() -> tuple[bool, str]:
    """Prove the layout against a path this process chose. See the header.

    Returns (ok, detail). A wrong offset yields a plausible string from the
    wrong bytes, so `is it a string` is not the check — `is it THE string` is.
    """
    with tempfile.NamedTemporaryFile(prefix="vk68m-fdpath-", suffix=".probe") as tmp:
        want = os.path.realpath(tmp.name)
        got, truncated = fd_path(os.getpid(), tmp.fileno())
        if not got:
            return False, (
                f"proc_pidfdinfo(PROC_PIDFDVNODEPATHINFO) returned no path for this "
                f"process's own fd {tmp.fileno()}; expected {want!r}. "
                f"struct size is {_VNODE_PATH_SIZE}")
        if truncated:
            return False, f"self-test path came back truncated: {got!r}"
        if os.path.realpath(got) != want:
            return False, (
                f"LAYOUT IS WRONG: read {got!r} for a descriptor on {want!r}. "
                f"Refusing to report paths off a struct that does not match.")
    return True, f"ok (vnode_fdinfowithpath = {_VNODE_PATH_SIZE} bytes)"


_LIST_FDS_HEADROOM = 256      # descriptors of slack on the first attempt
_LIST_FDS_ATTEMPTS = 4        # each further attempt quadruples the slack


def list_fds(pid: int) -> list[tuple[int, int]] | None:
    """[(fd, fdtype)] for `pid`, or None when the table could not be read.

    None is `not recorded`; an empty list is a claim. Same buffer discipline as
    `gate-memory-sample.py`: size, then read with head-room, because a short
    buffer TRUNCATES silently and would under-report a host that is filling up.

    A BUFFER THE KERNEL FILLED EXACTLY IS A TRUNCATED READ, and the head-room
    alone does not detect one. `proc_pidinfo` writes `min(actual, buffersize)`
    and returns what it wrote, so a full buffer is a SHORT COUNT wearing the
    shape of a complete one — and it can only happen while the table is growing
    fastest, which is exactly the sample this instrument exists to take. So a
    read that comes back full is retried with four times the slack, and one that
    is still full after `_LIST_FDS_ATTEMPTS` returns None. A number that is
    quietly too small is worse than no number: `-1`/`None` says "not recorded",
    while an under-count says "there is head-room" and refutes the bead.
    """
    if pid <= 0:
        return None
    sized = _LIBC.proc_pidinfo(
        ctypes.c_int(pid), ctypes.c_int(PROC_PIDLISTFDS),
        ctypes.c_uint64(0), None, ctypes.c_int(0),
    )
    if sized <= 0:
        return None
    slack = _LIST_FDS_HEADROOM
    for _ in range(_LIST_FDS_ATTEMPTS):
        # Slots rather than bytes, so the buffer is never SMALLER than the size
        # handed to the kernel — that direction is a write past the end.
        slots = sized // _FDINFO_SIZE + slack
        capacity = slots * _FDINFO_SIZE
        buffer = (_ProcFDInfo * slots)()
        written = _LIBC.proc_pidinfo(
            ctypes.c_int(pid), ctypes.c_int(PROC_PIDLISTFDS),
            ctypes.c_uint64(0), ctypes.byref(buffer), ctypes.c_int(capacity),
        )
        if written <= 0:
            return None
        if written >= capacity:
            slack *= 4
            continue
        return [(buffer[i].proc_fd, buffer[i].proc_fdtype)
                for i in range(written // _FDINFO_SIZE)]
    return None


def snapshot(pid: int) -> dict | None:
    """One full dump: every descriptor, with a path where one exists."""
    entries = list_fds(pid)
    if entries is None:
        return None
    rows: list[dict] = []
    for fd, fdtype in entries:
        kind = _KIND_NAMES.get(fdtype, f"type{fdtype}")
        if fdtype == PROX_FDTYPE_VNODE:
            path, truncated = fd_path(pid, fd)
            if not path:
                rows.append({"fd": fd, "kind": kind, "path": "<gone>"})
            else:
                row = {"fd": fd, "kind": kind, "path": path}
                if truncated:
                    row["truncated"] = True
                rows.append(row)
        else:
            rows.append({"fd": fd, "kind": kind, "path": f"<{kind}>"})
    return {
        "pid": pid,
        "epoch": time.time(),
        "count": len(rows),
        "max_fd": max((r["fd"] for r in rows), default=-1),
        "rows": rows,
    }


# --------------------------------------------------------------------------
# lsof cross-check
# --------------------------------------------------------------------------

def lsof_cross_check(pid: int) -> dict:
    """lsof's view, split into DESCRIPTORS and the rows that are not.

    `-F ftn` gives one field per line: `p<pid>` (lsof emits the process field
    whether or not it is asked for), then per file `f<fd>`, `t<type>`,
    `n<name>`. The FD field is a NUMBER for a real descriptor and a word
    (`cwd`, `rtd`, `txt`, `mem`, `NOFD`, `DEL`) otherwise. Splitting on that is
    the whole correction — an unsplit `wc -l` over-reports by however many
    images the host has mapped, which on a 400-dylib test host is the direction
    that manufactures a false `exhausted`.

    lsof is run immediately AFTER the snapshot, not atomically with it: two
    calls cannot share an instant, and over a churning table the two readings
    will differ by whatever moved in between. That difference is worth seeing;
    it is not a layout error, and it is why the two counts are printed side by
    side rather than compared with an assertion.

    A FAILING `lsof` IS REPORTED, NOT RETURNED AS ZERO ROWS. Before the review
    round of playhead-vk68m this call took `.stdout` off a `check=False` run
    and never looked at the exit status — so an lsof that could not run at all
    parsed to `descriptor_rows: 0` and printed `0` beside a kernel count of
    2,539, which reads as a disagreement between two instruments rather than as
    one instrument not having run. That is the same shape, in the same file, as
    the `etimes` defect in `find_test_host()` below.

    VALIDATED, 2026-08-24, on a process holding 100 known descriptors: lsof's
    numeric-FD set and `PROC_PIDLISTFDS` agreed EXACTLY (nothing in the
    kernel's set that lsof missed), with 18 rows excluded as `cwd`/`txt`. The
    only two extra fds lsof reported were the pipes `subprocess.run` had just
    opened to read lsof's OWN output — an artefact of a process measuring
    ITSELF, impossible against a foreign target, and named here so it is not
    re-derived as a discrepancy.
    """
    try:
        proc = subprocess.run(
            ["/usr/sbin/lsof", "-p", str(pid), "-n", "-P", "-F", "ftn"],
            capture_output=True, text=True, check=False, timeout=180,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"ok": False, "error": str(exc)}
    parsed = parse_lsof_fields(proc.stdout)
    if proc.returncode != 0 and parsed["descriptor_rows"] == 0:
        return {"ok": False, "returncode": proc.returncode,
                "error": f"lsof exited {proc.returncode} and reported no "
                         f"descriptors: {proc.stderr.strip()[:200]}"}
    parsed["returncode"] = proc.returncode
    return parsed


def parse_lsof_fields(out: str) -> dict:
    """Split `lsof -F ftn` output into DESCRIPTORS and everything else.

    Separated from the subprocess call so the split — the whole correction this
    cross-check exists for — can be tested without lsof on the box.
    """
    numeric: list[tuple[int, str]] = []
    non_numeric: dict[str, int] = {}
    cur_fd: str | None = None
    cur_name = ""
    def flush() -> None:
        if cur_fd is None:
            return
        stripped = cur_fd.rstrip("rwu-NRWU ")
        if stripped.isdigit():
            numeric.append((int(stripped), cur_name))
        else:
            key = stripped or cur_fd
            non_numeric[key] = non_numeric.get(key, 0) + 1
    for line in out.splitlines():
        if not line:
            continue
        tag, value = line[0], line[1:]
        if tag == "f":
            flush()
            cur_fd, cur_name = value, ""
        elif tag == "n" and cur_fd is not None:
            cur_name = value
    flush()
    return {
        "ok": True,
        # ROWS and DISTINCT FD NUMBERS are two quantities. lsof emits more than
        # one row for the same descriptor in some cases, so a row count is an
        # upper bound on descriptors held and the distinct-fd count is the one
        # comparable to PROC_PIDLISTFDS. Both are printed; neither stands alone.
        "distinct_fds": len({fd for fd, _ in numeric}),
        "descriptor_rows": len(numeric),
        "excluded_rows": sum(non_numeric.values()),
        "excluded_by_kind": non_numeric,
        "total_rows": len(numeric) + sum(non_numeric.values()),
        "fds": sorted(fd for fd, _ in numeric),
        "names": {fd: name for fd, name in numeric},
    }


# --------------------------------------------------------------------------
# grouping
# --------------------------------------------------------------------------

_GROUPERS: list[tuple[str, re.Pattern[str]]] = [
    ("simulator device data", re.compile(r"^/Users/[^/]+/Library/Developer/CoreSimulator/Devices/[^/]+/data(/.*)?$")),
    ("simulator runtime bundle", re.compile(r"^/Library/Developer/CoreSimulator/(Volumes|Profiles)(/.*)?$")),
    ("derived data / build products", re.compile(r".*/\.?[Dd]erived[Dd]ata(/.*)?$")),
    ("repo source tree", re.compile(r"^/Users/[^/]+/playhead(/.*)?$")),
    ("system framework / dylib", re.compile(r"^/(System|usr/lib|Library)(/.*)?$")),
    ("tmp", re.compile(r"^(/private)?(/var)?/tmp(/.*)?$")),
]


def group_of(path: str) -> str:
    if path.startswith("<"):
        return path
    for name, pattern in _GROUPERS:
        if pattern.match(path):
            return name
    return "other"


def leaf_family(path: str) -> str:
    """A path collapsed to something countable: directory + file EXTENSION.

    Store paths differ only by a UUID, so the raw path histogram has a long tail
    of singletons and says nothing. Collapsing the last component to its suffix
    turns 143 unique store paths into one row that says `143`.
    """
    if path.startswith("<"):
        return path
    head, _, tail = path.rpartition("/")
    if "." in tail:
        suffix = tail[tail.index("."):]
    else:
        suffix = "/<no-suffix>"
    # Collapse a UUID-ish directory component too.
    head = re.sub(r"/[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}", "/<uuid>", head)
    head = re.sub(r"/[0-9A-Fa-f]{32}", "/<hex32>", head)
    return f"{head}/*{suffix}"


def summarise(snap: dict) -> dict:
    by_kind: dict[str, int] = {}
    by_group: dict[str, int] = {}
    by_family: dict[str, int] = {}
    by_path: dict[str, int] = {}
    for row in snap["rows"]:
        by_kind[row["kind"]] = by_kind.get(row["kind"], 0) + 1
        path = row["path"]
        by_group[group_of(path)] = by_group.get(group_of(path), 0) + 1
        fam = leaf_family(path)
        by_family[fam] = by_family.get(fam, 0) + 1
        by_path[path] = by_path.get(path, 0) + 1
    return {
        "pid": snap["pid"], "epoch": snap["epoch"], "count": snap["count"],
        "max_fd": snap["max_fd"], "by_kind": by_kind, "by_group": by_group,
        "by_family": by_family, "distinct_paths": len(by_path),
        "duplicated_paths": sum(n for n in by_path.values() if n > 1),
    }


# --------------------------------------------------------------------------
# host discovery
# --------------------------------------------------------------------------

def find_test_host() -> int:
    """The simulator test host's pid, or 0.

    Same predicate as `gate-memory-sample.py` — the test host IS the app;
    `PlayheadTests.xctest` is injected into it — so the two instruments cannot
    disagree about which process they are describing.

    A FAILING `ps` IS REPORTED, NOT RETURNED AS ZERO. An earlier revision of
    this function asked `ps` for `etimes`, which is a Linux keyword macOS `ps`
    does not know, and this function dutifully returned 0 on every call — so
    the watcher sat through a whole run finding no host and saying nothing.
    A guard whose false branch makes no claim is this repo's standing defect
    class, and it is the reason for the `returncode` check below rather than a
    bare `.stdout`.

    **WHAT macOS `ps` ACTUALLY DOES WITH AN UNKNOWN KEYWORD — measured, because
    the sentence here used to say it "wrote NOTHING to stdout" and that is
    false, which is this docstring committing the class it is about.** Run
    `ps -Ao pid=,rss=,etimes=,comm=` on this box: it exits **1**, prints
    `ps: etimes: keyword not found` to stderr, AND writes a **complete process
    listing to stdout** — 509 lines, 53 KB — with the unrecognised column
    silently DROPPED, so every line arrives with three fields where the caller
    expects four. So the old code failed twice over and either alone was
    enough: the non-zero exit went unchecked, and had it been ignored the
    three-field lines would still have failed the four-field parse. Do not
    "simplify" the `returncode` check away on the theory that a bad keyword
    produces no output — it produces plausible output of the wrong shape, which
    is strictly worse.
    """
    result = subprocess.run(
        ["ps", "-Ao", "pid=,rss=,comm="],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        print(f"gate-fd-paths: `ps` FAILED (rc={result.returncode}): "
              f"{result.stderr.strip()[:200]} — no host can be discovered",
              file=sys.stderr)
        return 0
    best_pid, best_rss = 0, -1
    for line in result.stdout.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 3 or not parts[0].isdigit() or not parts[1].isdigit():
            continue
        if "/Playhead.app/" in parts[2]:
            rss = int(parts[1])
            if rss > best_rss:
                best_rss, best_pid = rss, int(parts[0])
    return best_pid


def record_high_water(high_water: dict[int, int], pid: int, count: int) -> None:
    """Remember the MOST this pid has ever held — not the last.

    `pin_decision`'s entire discriminator is "more than the pinned process has
    EVER held", and this is the only thing that makes `ever` true. Written as a
    plain assignment it degrades silently to "more than it LAST held", which
    lets a shrinking host lose the pin to a leftover — the defect the pin
    exists to prevent. It is a separate function only so a rail can reach it:
    inline in the sampling loop it survived every mutation
    (playhead-vk68m review round 4).
    """
    high_water[pid] = max(high_water.get(pid, -1), count)


def pin_decision(pinned: int, high_water: dict[int, int], pid: int, count: int) -> str:
    """`pin` | `keep` | `promote` — who owns the UN-SUFFIXED dump files.

    `pin` for the first process seen. `promote` when `pid` is holding MORE
    descriptors than the pinned process has EVER held. `keep` otherwise.

    WHY A PROMOTION RULE AND NOT FIRST-SEEN-WINS. The pin exists because
    `find_test_host()` picks the largest-RSS process under `/Playhead.app/`,
    which is right during a run and wrong the instant it ends: after the host
    exited, `--last` was rewritten with twelve descriptors belonging to a stale
    simulator app (pid 85292), so the file named for this run's tail held
    something else's. First-seen-wins fixes that and BREAKS ITS MIRROR, which
    is measured in this bead's own archive rather than inferred: on run 1 the
    watcher was started before a cold build, and the first `/Playhead.app/`
    process it saw was a LEFTOVER holding exactly 20 descriptors, unchanged,
    for 62 consecutive samples — `artifacts/run1/full/sample-0001-00020.json.gz`
    through `-0062-00020.json.gz`, all pid 58651 — while the real test host
    (pid 71372, peak 2,402) first appeared at sample 63. Pinned first-seen,
    `peak.json` for that run would have held the leftover's twenty.

    A process holding more descriptors than the pinned one has EVER held cannot
    be a stale remnant of it. That is the whole discriminator, and it needs no
    clock — the `etimes` age bound this replaces asked macOS `ps` for a Linux
    keyword and silently returned 0 forever. The twelve-descriptor clobber
    stays excluded because 12 is not more than 2,402.

    IT IS NOT OSCILLATION-PROOF, AND AN EARLIER VERSION OF THIS PARAGRAPH SAID
    IT WAS (playhead-vk68m review round 4). "Monotone, so it cannot oscillate
    between two live processes" is false, and driving it shows why in four
    lines: two hosts BOTH growing hand the pin back and forth forever, because
    each one's next sample exceeds the other's high-water mark —

        pid 1 @ 100 -> pin       pid 2 @ 150 -> promote
        pid 1 @ 200 -> promote   pid 2 @ 250 -> promote   ...

    What is monotone is the THRESHOLD, not the pin. Every rail below tested a
    single transition, so nothing saw it. The rule is kept because the case it
    was built for is real and measured and the oscillating case is not: this
    gate runs ONE test host at a time, and `--full-dir` keeps every sample with
    its pid, so even an oscillating watch loses no reading — only the
    un-suffixed `peak.json`/`last.json` change owner. Said plainly: this is a
    heuristic that makes a wrong pin VISIBLE (the census, the pid in every
    line), not one that makes it impossible.

    What it does NOT do: bound the watcher to one RUN. A watcher left running
    across two gates will promote the second run's host once it passes the
    first's peak — correctly, by this rule's own definition of the subject.
    `--deadline` is what ends a watch, and `--full-dir` keeps every sample
    regardless, so no reading is lost either way.
    """
    if pinned == 0:
        return "pin"
    if pid != pinned and count > high_water.get(pinned, -1):
        return "promote"
    return "keep"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--pid", type=int, default=None,
                    help="target pid; omit to discover the simulator test host. "
                         "Default is None rather than 0 because `--pid 0` used to be "
                         "indistinguishable from `no --pid` and silently discovered "
                         "instead — a sentinel a caller can also type is not a "
                         "sentinel (playhead-vk68m review round 4).")
    ap.add_argument("--out", default="",
                    help="JSONL of per-sample SUMMARIES (one line per sample)")
    ap.add_argument("--last", default="",
                    help="full dump of the most recent sample, rewritten each time")
    ap.add_argument("--full-dir", default="",
                    help="write EVERY sample's full dump, gzipped, into this directory. "
                         "A run is one shot at a 15-minute question, so the default on a "
                         "diagnostic run is to keep all of it: ~40 KB per sample.")
    ap.add_argument("--peak", default="",
                    help="full dump of the HIGHEST-count sample seen so far, rewritten "
                         "only when a new high is reached — per process, so an "
                         "interloper's `*.pid<N>.*` file is a high-water mark too and "
                         "not merely its last sample. The floor and the peak are "
                         "different populations and only one of them is at the tail.")
    ap.add_argument("--interval", type=float, default=10.0)
    ap.add_argument("--watch", action="store_true",
                    help="sample until the host exits and --deadline passes")
    ap.add_argument("--deadline", type=float, default=0.0,
                    help="stop this many seconds after the subject DISAPPEARS (0 = at "
                         "once). Its clock does not run before ANY /Playhead.app/ "
                         "process has been sampled, so a watcher started ahead of the "
                         "gate waits, bounded only by --max-minutes. READ THAT "
                         "LITERALLY: `any`, not `the host` — see LIMIT-1 in main().")
    ap.add_argument("--max-minutes", type=float, default=60.0)
    ap.add_argument("--cross-check", action="store_true",
                    help="also run lsof on the same pid and report the difference")
    ap.add_argument("--top", type=int, default=40)
    args = ap.parse_args()

    # WHO AM I, AND WHAT WAS I ASKED FOR. Two invocations appending to one
    # --out or one redirected log are indistinguishable without this, which is
    # exactly why playhead-vk68m's own never-seen/gone finding (M5b) had to be
    # recorded as "corroboration rather than proof": `artifacts/run2/watcher.log`
    # holds two interleaved runs, one ending `0 samples`, and the argv of the
    # one that measured nothing did not survive. It does now.
    print(f"gate-fd-paths: pid {os.getpid()} argv {' '.join(sys.argv[1:])!r}",
          file=sys.stderr)
    ok, detail = self_test()
    print(f"gate-fd-paths: self-test {detail}", file=sys.stderr)
    if not ok:
        return 3

    stop = {"now": False}
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.__setitem__("now", True))
    # WHY THE WATCH ENDED. A census that reports only `N sample(s)` cannot be
    # told apart from a watch that ended before its subject started (LIMIT-1
    # below), and "it ran and found little" reads exactly like "it stopped
    # early". Three exits, three sentences, and the default is the one that
    # says nobody set it — the same reason `-1` is `not recorded` everywhere
    # else in this file.
    ended = "ended without recording a reason"

    # THE PINNED HOST. `find_test_host()` picks the largest-RSS process whose
    # executable is inside `/Playhead.app/`, which is right DURING a run and
    # wrong at both ends of it: a leftover app from the previous run satisfies
    # the same predicate BEFORE the host launches, and a stale one satisfies it
    # AFTER the host exits. Both were observed on this bead's own run 1. So one
    # process owns the un-suffixed `--last`/`--peak` files and everyone else is
    # written to `*.pid<N>.*`; `pin_decision` above is the rule and carries the
    # evidence for it.
    pinned = {"pid": 0}
    high_water: dict[int, int] = {}
    samples_by_pid: dict[int, int] = {}
    peak_snap: dict[int, dict] = {}
    last_snap: dict[int, dict] = {}

    out_fh = open(args.out, "a", buffering=1) if args.out else None
    started = time.time()
    gone_since = 0.0
    samples = 0
    waiting_announced = False
    if args.full_dir:
        os.makedirs(args.full_dir, exist_ok=True)

    while not stop["now"]:
        pid = args.pid if args.pid is not None else find_test_host()
        snap = snapshot(pid) if pid else None
        if snap is not None:
            count = snap["count"]
            decision = pin_decision(pinned["pid"], high_water, pid, count)
            if decision == "pin":
                pinned["pid"] = pid
                # The COUNT is printed with the pid because that is what makes a
                # wrong pin visible: a leftover app holds a couple of dozen
                # descriptors and a test host holds hundreds, and the number is
                # the only thing in this line that can tell them apart.
                print(f"gate-fd-paths: pinned host pid {pid} "
                      f"(holding {count} descriptors)", file=sys.stderr)
            elif decision == "promote":
                previous = pinned["pid"]
                print(f"gate-fd-paths: HOST REPINNED {previous} (never held more "
                      f"than {high_water.get(previous, 0)}) -> {pid} (holding "
                      f"{count}); the earlier subject's dumps move to "
                      f"*.pid{previous}.* and keep their readings",
                      file=sys.stderr)
                pinned["pid"] = pid
                # The demoted subject keeps its evidence, under its own name.
                if args.peak and previous in peak_snap:
                    _atomic_json(_scoped(args.peak, previous, pid), peak_snap[previous])
                if args.last and previous in last_snap:
                    _atomic_json(_scoped(args.last, previous, pid), last_snap[previous])
                if args.peak:
                    _atomic_json(args.peak, peak_snap.get(pid, snap))
            elif pid != pinned["pid"] and pid not in samples_by_pid:
                print(f"gate-fd-paths: OTHER /Playhead.app/ process {pid} "
                      f"(holding {count}); its dumps go to *.pid{pid}.* and the "
                      f"pinned host's are left as they stand", file=sys.stderr)
            record_high_water(high_water, pid, count)
            samples_by_pid[pid] = samples_by_pid.get(pid, 0) + 1
        if snap is None:
            if not args.watch:
                print(f"gate-fd-paths: no readable fd table for pid {pid}", file=sys.stderr)
                if out_fh:
                    out_fh.close()
                return 4
            if samples == 0:
                # NEVER SEEN IS NOT GONE. `--deadline` says how long to keep
                # sampling AFTER the subject disappears; starting its clock
                # before anything has ever appeared makes a watcher launched
                # ahead of the gate — the only order in which it can catch the
                # ramp — exit on its second cycle at the default deadline of 0,
                # having measured nothing and claimed nothing about it.
                #
                # LIMIT-1, STATED BECAUSE IT IS HALF OF THE SAME CONFLATION AND
                # IS NOT CLOSED. The condition is `samples == 0` — has ANY
                # `/Playhead.app/` process been sampled — not "has the HOST been
                # sampled", which is not a question this predicate can answer
                # (`find_test_host` cannot tell a leftover from a host; that is
                # why `pin_decision` exists). So a LEFTOVER app that is sampled,
                # exits, and is followed two cycles later by the real host still
                # ends the watch at `--deadline 0`, leaving `peak.json` holding
                # the leftover's couple of dozen descriptors. That is run 1's
                # own process timeline (pid 58651 for 62 samples, then pid
                # 71372) and it survived only because the two were ADJACENT
                # samples. Driven through `main()` in
                # `WatchStartupRails.test_LIMIT_a_leftover_that_exits_before_the_host_ENDS_the_watch`.
                # Two things keep it from reading as a completed measurement:
                # the census prints every pid with its peak, and the watch now
                # prints WHY it ended. Closing it properly needs a discriminator
                # that does not exist here (the withdrawn `etimes` age bound was
                # the last attempt), so it is named rather than papered over.
                if not waiting_announced:
                    print("gate-fd-paths: no /Playhead.app/ process yet — waiting "
                          f"(bounded by --max-minutes {args.max_minutes:g})",
                          file=sys.stderr)
                    waiting_announced = True
            elif gone_since == 0.0:
                gone_since = time.time()
            elif time.time() - gone_since >= args.deadline:
                ended = (f"the subject went away and --deadline "
                         f"{args.deadline:g}s expired")
                break
        else:
            gone_since = 0.0
            samples += 1
            summary = summarise(snap)
            cross = lsof_cross_check(pid) if args.cross_check else None
            if cross is not None:
                summary["lsof"] = {
                    k: v for k, v in cross.items() if k not in ("fds", "names")
                }
            if out_fh:
                out_fh.write(json.dumps(summary, sort_keys=True) + "\n")
            last_snap[pid] = snap
            if args.last:
                _atomic_json(_scoped(args.last, pid, pinned["pid"]), snap)
            if args.full_dir:
                name = os.path.join(
                    args.full_dir,
                    f"sample-{samples:04d}-{snap['count']:05d}.json.gz")
                tmp = name + ".partial"
                with gzip.open(tmp, "wt") as fh:
                    json.dump(snap, fh, sort_keys=True)
                os.replace(tmp, name)
            if record_peak(peak_snap, pid, snap) and args.peak:
                _atomic_json(_scoped(args.peak, pid, pinned["pid"]), snap)
            if not args.watch:
                report(snap, args, cross)
                if out_fh:
                    out_fh.close()
                return 0
        if time.time() - started > args.max_minutes * 60:
            ended = f"--max-minutes {args.max_minutes:g} reached"
            break
        deadline = time.time() + args.interval
        while not stop["now"] and time.time() < deadline:
            time.sleep(0.2)
        if stop["now"]:
            ended = "signalled (SIGINT/SIGTERM)"

    if out_fh:
        out_fh.close()
    print(f"gate-fd-paths: watch {ended}", file=sys.stderr)
    for line in census_lines(samples, samples_by_pid, high_water, pinned["pid"]):
        print(line, file=sys.stderr)
    return 0


def record_peak(peak_snap: dict[int, dict], pid: int, snap: dict) -> bool:
    """Keep `pid`'s HIGHEST-count snapshot; True when this one replaced it.

    A high-water file PER PROCESS. The previous revision wrote every non-pinned
    sample straight over the scoped path, so a file whose own comment called it
    "an interloper's high-water file" actually held that process's LAST sample —
    the standing defect class in this instrument's own bookkeeping. It is a
    function rather than three lines inside the sampling loop because a
    predicate buried in `main()` is one a mutation battery cannot reach: the
    version that overwrote unconditionally survived every rail in this file.
    """
    if snap["count"] > peak_snap.get(pid, {}).get("count", -1):
        peak_snap[pid] = snap
        return True
    return False


def census_lines(samples: int, samples_by_pid: dict[int, int],
                 high_water: dict[int, int], pinned: int) -> list[str]:
    """Every process this watch saw, with its sample count and its peak.

    A watch that printed only `N samples` could not be read afterwards: the
    JSONL carries a `pid` per row and nothing summarises it, so "which process
    is `peak.json` about" was a question only a reader who went and grouped the
    file could answer — and on run 1, where the first 62 samples belong to a
    leftover holding 20 descriptors, it is the question that matters.
    """
    lines = [f"gate-fd-paths: {samples} sample(s) over "
             f"{len(samples_by_pid)} process(es)"]
    for pid in sorted(samples_by_pid, key=lambda p: -high_water.get(p, -1)):
        mark = "  <- PINNED, owns the un-suffixed dumps" if pid == pinned else ""
        lines.append(f"gate-fd-paths:   pid {pid}  {samples_by_pid[pid]:>4} samples  "
                     f"peak {high_water.get(pid, -1)}{mark}")
    return lines


def _scoped(path: str, pid: int, pinned: int) -> str:
    """`path` for the pinned host, `path` with `.pid<N>` spliced in for anyone else.

    Splices before the extension so `last.json` -> `last.pid482.json` rather
    than `last.json.pid482`, which reads as a partial file.

    The split is on the BASENAME, not the whole path. Splitting the whole path
    on its last dot turns `artifacts/run.1/last` into `artifacts/run.pid9.1/last`
    — a directory that does not exist, so the write fails rather than landing
    somewhere wrong, but it fails for a reason nobody would find.
    """
    if pid == pinned:
        return path
    directory, name = os.path.split(path)
    root, dot, extension = name.rpartition(".")
    scoped = f"{name}.pid{pid}" if not dot else f"{root}.pid{pid}.{extension}"
    return os.path.join(directory, scoped) if directory else scoped


def _atomic_json(path: str, payload: dict) -> None:
    """Write via a `.partial` rename, so no reader can ever see half a dump.

    The peak and last files are rewritten while a run is live and are read by a
    human the moment the run ends; a truncated file read as a small one would be
    a count that names one thing read as another.
    """
    tmp = path + ".partial"
    with open(tmp, "w") as fh:
        json.dump(payload, fh, sort_keys=True)
    os.replace(tmp, path)


def report(snap: dict, args, cross: dict | None = None) -> None:
    summary = summarise(snap)
    print(f"pid {snap['pid']}  count {snap['count']}  max_fd {snap['max_fd']}")
    print("\nBY KIND")
    for kind, n in sorted(summary["by_kind"].items(), key=lambda kv: -kv[1]):
        print(f"  {n:6d}  {kind}")
    print("\nBY GROUP")
    for group, n in sorted(summary["by_group"].items(), key=lambda kv: -kv[1]):
        print(f"  {n:6d}  {group}")
    print(f"\nBY FAMILY (top {args.top})")
    for fam, n in sorted(summary["by_family"].items(), key=lambda kv: -kv[1])[:args.top]:
        print(f"  {n:6d}  {fam}")
    if args.cross_check:
        # The caller's reading, not a second one: two lsof runs a second apart
        # describe two different tables, and printing the later one beside the
        # earlier snapshot invents a discrepancy.
        cc = cross if cross is not None else lsof_cross_check(snap["pid"])
        print("\nLSOF CROSS-CHECK")
        if not cc.get("ok"):
            print(f"  lsof failed: {cc.get('error')}")
        else:
            print(f"  lsof total rows            {cc['total_rows']}")
            print(f"  of which DESCRIPTOR rows   {cc['descriptor_rows']}")
            print(f"  DISTINCT fd numbers        {cc['distinct_fds']}")
            print(f"  EXCLUDED (cwd/rtd/txt/...) {cc['excluded_rows']}  {cc['excluded_by_kind']}")
            print(f"  PROC_PIDLISTFDS count      {snap['count']}")


if __name__ == "__main__":
    sys.exit(main())
