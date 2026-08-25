#!/usr/bin/env python3
"""Summarise a `gate-fd-paths.py` dump into the by-PATH-FAMILY table
playhead-882eg's before/after is reported in.

Input is one of that tool's `--last` / `--peak` / `--full-dir` JSON dumps:
`{count, epoch, max_fd, pid, rows: [{fd, kind, path}, ...]}`.

The number this prints is a **TOTAL descriptor count**, not a vnode count —
`rows` is the whole `PROC_PIDLISTFDS` table, sockets and kqueues included, so
it is the same column as `testhost_fds` in the memory series and the same
column the ~453 FLOOR and the ~2,539 PEAK were both quoted in. Pairing a vnode
figure with a total figure is a mistake already made once on this data
(playhead-vk68m), so the column is named on every line rather than assumed.

Families are matched on a distinctive path FRAGMENT and the first match wins,
so the order below is the precedence. `distinct` is the number of different
paths inside the family — the discriminator that separates "81 connections to
ONE file" (the defect) from "81 files opened once each" (not the same thing).

    python3 scripts/fd-tail-summary.py before/last.json after/last.json
"""
from __future__ import annotations

import collections
import json
import sys

FAMILIES = [
    ("production analysis.sqlite", "Application Support/Playhead/AnalysisStore/analysis.sqlite"),
    ("production ad_catalog.sqlite", "Application Support/AdCatalog/ad_catalog.sqlite"),
    ("Caches/Diagnostics/surface-status-*.jsonl", "Diagnostics/surface-status-"),
    ("Documents/bg-task-log.jsonl", "bg-task-log.jsonl"),
    ("tmp/PlayheadTestScratch/*", "PlayheadTestScratch"),
    ("SwiftData Playhead.store", "Application Support/Playhead.store"),
]


def classify(path: str) -> str:
    for name, fragment in FAMILIES:
        if fragment in path:
            return name
    return "other / infrastructure"


def summarise(path_to_dump: str) -> None:
    dump = json.load(open(path_to_dump))
    rows = dump["rows"]
    by_family: dict[str, list[str]] = collections.defaultdict(list)
    for row in rows:
        by_family[classify(row["path"])].append(row["path"])
    print(f"{path_to_dump}")
    print(f"  pid {dump['pid']}  count {dump['count']} TOTAL descriptors "
          f"(max_fd {dump['max_fd']}); rows carried {len(rows)}")
    if dump["count"] != len(rows):
        print(f"  !! count and len(rows) DISAGREE — read the dump before quoting either")
    for name, _ in FAMILIES + [("other / infrastructure", "")]:
        paths = by_family.get(name)
        if not paths:
            continue
        distinct = len(set(paths))
        print(f"  {len(paths):5d}  {name}   ({distinct} distinct path(s))")
        if distinct <= 4:
            counts = collections.Counter(paths)
            for p, n in counts.most_common():
                print(f"           {n:5d} x  ...{p[-70:]}")
    print()


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    for arg in sys.argv[1:]:
        summarise(arg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
