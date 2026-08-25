#!/usr/bin/env python3
"""NAME the test host's open file descriptors, by PATH (playhead-vk68m).

`scripts/gate-memory-sample.py` counts descriptors by KIND — vnode, socket,
kqueue, pipe — which was enough to establish that the test host reaches its
`RLIMIT_NOFILE` soft limit of 2,560 (playhead-s34ux). It cannot say WHAT any of
them are, and that is the question playhead-vk68m exists to answer:

  * the PEAK is transient (3 -> 2,539 -> 453), and
  * the FLOOR is not: 27 vnodes before the ramp, 449 vnodes flat for 22 samples
    over 220 s after the test phase, in the SAME process with no restart.

~429 descriptors acquired during a run and never released is 16.8 % of the
budget gone before the next plan opens a single store. Nothing in the by-kind
series names one of them. This does.

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
    `--cross-check` is passed, lsof is run in the same instant and ONLY its rows
    whose FD column is a NUMBER (`12u`, `13r`, …) are compared; `cwd`/`rtd`/
    `txt`/`mem`/`NOFD` rows are counted separately and reported as EXCLUDED, so
    the exclusion is visible rather than assumed.
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


def list_fds(pid: int) -> list[tuple[int, int]] | None:
    """[(fd, fdtype)] for `pid`, or None when the table could not be read.

    None is `not recorded`; an empty list is a claim. Same buffer discipline as
    `gate-memory-sample.py`: size, then read with head-room, because a short
    buffer TRUNCATES silently and would under-report a host that is filling up.
    """
    if pid <= 0:
        return None
    capacity = _LIBC.proc_pidinfo(
        ctypes.c_int(pid), ctypes.c_int(PROC_PIDLISTFDS),
        ctypes.c_uint64(0), None, ctypes.c_int(0),
    )
    if capacity <= 0:
        return None
    capacity += _FDINFO_SIZE * 256
    buffer = (_ProcFDInfo * (capacity // _FDINFO_SIZE))()
    written = _LIBC.proc_pidinfo(
        ctypes.c_int(pid), ctypes.c_int(PROC_PIDLISTFDS),
        ctypes.c_uint64(0), ctypes.byref(buffer), ctypes.c_int(capacity),
    )
    if written <= 0:
        return None
    return [(buffer[i].proc_fd, buffer[i].proc_fdtype)
            for i in range(written // _FDINFO_SIZE)]


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

    `-F pftn` gives one field per line: `p<pid>`, then per file `f<fd>`,
    `t<type>`, `n<name>`. The FD field is a NUMBER for a real descriptor and a
    word (`cwd`, `rtd`, `txt`, `mem`, `NOFD`, `DEL`) otherwise. Splitting on
    that is the whole correction — an unsplit `wc -l` over-reports by however
    many images the host has mapped, which on a 400-dylib test host is the
    direction that manufactures a false `exhausted`.

    VALIDATED, 2026-08-24, on a process holding 100 known descriptors: lsof's
    numeric-FD set and `PROC_PIDLISTFDS` agreed EXACTLY (nothing in the
    kernel's set that lsof missed), with 18 rows excluded as `cwd`/`txt`. The
    only two extra fds lsof reported were the pipes `subprocess.run` had just
    opened to read lsof's OWN output — an artefact of a process measuring
    ITSELF, impossible against a foreign target, and named here so it is not
    re-derived as a discrepancy.
    """
    try:
        out = subprocess.run(
            ["/usr/sbin/lsof", "-p", str(pid), "-n", "-P", "-F", "ftn"],
            capture_output=True, text=True, check=False, timeout=180,
        ).stdout
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"ok": False, "error": str(exc)}
    return parse_lsof_fields(out)


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
    """
    out = subprocess.run(
        ["ps", "-Ao", "pid=,rss=,comm="], capture_output=True, text=True, check=False
    ).stdout
    best_pid, best_rss = 0, -1
    for line in out.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 3 or not parts[0].isdigit() or not parts[1].isdigit():
            continue
        if "/Playhead.app/" in parts[2]:
            rss = int(parts[1])
            if rss > best_rss:
                best_rss, best_pid = rss, int(parts[0])
    return best_pid


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--pid", type=int, default=0,
                    help="target pid; default is to discover the simulator test host")
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
                         "whenever a new high is reached. The floor and the peak are "
                         "different populations and only one of them is at the tail.")
    ap.add_argument("--interval", type=float, default=10.0)
    ap.add_argument("--watch", action="store_true",
                    help="sample until the host exits and --deadline passes")
    ap.add_argument("--deadline", type=float, default=0.0,
                    help="stop this many seconds after the host disappears (0 = at once)")
    ap.add_argument("--max-minutes", type=float, default=60.0)
    ap.add_argument("--cross-check", action="store_true",
                    help="also run lsof on the same pid and report the difference")
    ap.add_argument("--top", type=int, default=40)
    args = ap.parse_args()

    ok, detail = self_test()
    print(f"gate-fd-paths: self-test {detail}", file=sys.stderr)
    if not ok:
        return 3

    stop = {"now": False}
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.__setitem__("now", True))

    out_fh = open(args.out, "a", buffering=1) if args.out else None
    started = time.time()
    gone_since = 0.0
    samples = 0
    peak_count = -1
    if args.full_dir:
        os.makedirs(args.full_dir, exist_ok=True)

    while not stop["now"]:
        pid = args.pid or find_test_host()
        snap = snapshot(pid) if pid else None
        if snap is None:
            if not args.watch:
                print(f"gate-fd-paths: no readable fd table for pid {pid}", file=sys.stderr)
                return 4
            if gone_since == 0.0:
                gone_since = time.time()
            elif time.time() - gone_since >= args.deadline:
                break
        else:
            gone_since = 0.0
            samples += 1
            summary = summarise(snap)
            if args.cross_check:
                summary["lsof"] = {
                    k: v for k, v in lsof_cross_check(pid).items()
                    if k not in ("fds", "names")
                }
            if out_fh:
                out_fh.write(json.dumps(summary, sort_keys=True) + "\n")
            if args.last:
                _atomic_json(args.last, snap)
            if args.full_dir:
                name = os.path.join(
                    args.full_dir,
                    f"sample-{samples:04d}-{snap['count']:05d}.json.gz")
                tmp = name + ".partial"
                with gzip.open(tmp, "wt") as fh:
                    json.dump(snap, fh, sort_keys=True)
                os.replace(tmp, name)
            if args.peak and snap["count"] > peak_count:
                peak_count = snap["count"]
                _atomic_json(args.peak, snap)
            if not args.watch:
                report(snap, args)
                return 0
        if time.time() - started > args.max_minutes * 60:
            break
        deadline = time.time() + args.interval
        while not stop["now"] and time.time() < deadline:
            time.sleep(0.2)

    if out_fh:
        out_fh.close()
    print(f"gate-fd-paths: {samples} samples", file=sys.stderr)
    return 0


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


def report(snap: dict, args) -> None:
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
        cc = lsof_cross_check(snap["pid"])
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
