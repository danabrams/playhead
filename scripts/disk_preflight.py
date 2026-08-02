#!/usr/bin/env python3
"""Disk-headroom preflight for the gate (playhead-3nfa).

WHY THIS EXISTS
---------------
A full-plan gate is a *transient* disk event, not a steady one: the destination
simulator inflates during the Swift Testing bulk and shrinks back afterwards.
When the box runs out of room mid-run the failure does not present as an error.
It presents as a HANG — xcodebuild stays alive, writes nothing, never exits,
having failed to write its result bundle. There is no POSIX 28, no non-zero
exit, nothing in the log. On 2026-08-01 this box hit 100% capacity four times
and every one was caught by someone happening to look, not by the tooling.

So the gate refuses to start when it does not have the room it needs, and says
what to do about it. A gate that declines to start is strictly better than one
that wedges silently and then lies.

THE THRESHOLD IS MEASURED, NOT GUESSED
--------------------------------------
See DEFAULT_MIN_GIB below for the derivation and the observation it came from.
Re-derive it when the suite grows: run a full cold gate while sampling
`df` on /System/Volumes/Data, take (free_at_start - min_free_during), and add
the margin. `scripts/gate-disk-sample.sh` does exactly that.

A threshold that is too HIGH is as bad as none. It refuses runs that would have
succeeded, and people learn to set the override — which is how a safety check
becomes a speed bump nobody has felt in months.

WHAT IT DOES NOT DO
-------------------
It does not delete anything unless `--reclaim` is passed. Reclaiming is
delegated wholesale to `scripts/disk-cleanup.sh` so this repo has exactly one
cleaner with one set of safety rails, rather than two that drift apart.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys

GIB = 1024.0 ** 3

# ---------------------------------------------------------------------------
# THE NUMBER, AND WHERE IT COMES FROM
# ---------------------------------------------------------------------------
# Derivation (measured 2026-08-02 on this box, one full cold PlayheadFastTests
# gate in a fresh worktree, sampling free space on /System/Volumes/Data every
# 5s from before `xcodebuild` started until after it printed its verdict):
#
#     peak drawdown = free_at_start - min_free_during_run
#
# The drawdown is what a gate actually costs while it runs: a cold
# `.derivedData` built from empty, plus the simulator's transient inflation,
# minus whatever it hands back along the way. The threshold is that drawdown
# plus a margin, because the sampler is periodic (it can miss a spike between
# samples) and because a wedge at 100% is unrecoverable-in-place while a
# refusal costs nothing.
#
# Superseded numbers, kept so the trend is legible:
#   * playhead-voez (2026-08-01, before #328): ~13 GB free finished, ~10 GB
#     free wedged. CLAUDE.md rounded that to "~8 GB of headroom".
#   * playhead-cgka (#328) then fixed the suite reclaiming scratch only at
#     process boundaries; peak scratch went 331 -> 100 MiB.
# Both predate the measurement this constant is set from. Do not restore an
# older figure without re-measuring.
DEFAULT_MIN_GIB = 12.0

# Only these prefixes may ever be handed to the cleaner. Mirrors the rails in
# CLAUDE.md; the cleaner enforces them again on its own side.
SAFE_PREFIXES = ("/Users/dabrams/playhead/.worktrees/", "/private/tmp/playhead-")


def free_bytes(path: str) -> int:
    """Free bytes available to this user on the volume holding `path`."""
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize


def fmt_gib(n_bytes: float) -> str:
    return f"{n_bytes / GIB:.2f} GiB"


# ---------------------------------------------------------------------------
# Reclaim reporting
# ---------------------------------------------------------------------------
# `disk-cleanup.sh --dry-run` prints one line per candidate:
#     [DRY] REMOVE (reason, size): /path/to/thing
# We parse those purely to tell the operator how much is sitting there. Nothing
# here decides to delete; that is the cleaner's job and only when --reclaim
# asked for it.
_DRY_LINE = re.compile(r"^\[DRY\]\s+REMOVE\s+\(([^,]+),\s*([^)]*)\):\s*(.+)$")


def parse_dry_run(text: str) -> list[tuple[str, str, str]]:
    """Extract (reason, size, path) triples from a `--dry-run` transcript."""
    out: list[tuple[str, str, str]] = []
    for line in text.splitlines():
        m = _DRY_LINE.match(line.strip())
        if m:
            out.append((m.group(1).strip(), m.group(2).strip(), m.group(3).strip()))
    return out


def _run(cmd: list[str]) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    except (OSError, subprocess.SubprocessError) as exc:  # pragma: no cover - defensive
        return 127, f"{exc}"
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def survey(cleaner: str, runner=_run) -> list[tuple[str, str, str]]:
    """Ask the cleaner what it *would* remove. Never removes anything."""
    if not os.path.exists(cleaner):
        return []
    rc, out = runner([cleaner, "--dry-run"])
    if rc != 0:
        return []
    return parse_dry_run(out)


# ---------------------------------------------------------------------------
# The refusal
# ---------------------------------------------------------------------------
# Deliberately loud, and deliberately does NOT mention the override. Same
# reasoning as PLAYHEAD_SKIP_BASELINE: an escape hatch printed in the failure
# message stops being an escape hatch and becomes the documented workaround.
def format_refusal(free_b: int, min_b: int, candidates, sim_id: str = "") -> str:
    bar = "=" * 72
    short = min_b - free_b
    lines = [
        "",
        bar,
        "fast-gate: REFUSING TO START — not enough disk headroom.",
        bar,
        f"  free now : {fmt_gib(free_b)}",
        f"  required : {fmt_gib(min_b)}   (short by {fmt_gib(short)})",
        "",
        "  A gate that runs out of room does NOT fail — it WEDGES. xcodebuild",
        "  stays alive with zero output and never exits, because it could not",
        "  write its result bundle. That is indistinguishable from a slow test",
        "  run until you have lost several minutes to it. Hence this refusal.",
        "",
        "  RECLAIM, in the order that pays:",
        "",
        "  1. Stranded simulator data in $TMPDIR/Deleting-*.",
        "     `simctl erase` does NOT delete a device's data in place — it MOVES",
        "     it to $TMPDIR/Deleting-<uuid>/ and reaps it asynchronously. If the",
        "     reap hits a directory it cannot READ it dies there and the bytes",
        "     stay forever, while erase still reports success. 15 GiB was",
        "     stranded this way across seven devices on 2026-08-01. The suite",
        "     manufactures exactly such a directory by design (DownloadManagerTests",
        "     chmods a complete/ dir to 0o300 and restores it in a defer that an",
        "     abnormal exit skips).",
        '     chmod -R u+rwx "$TMPDIR"/Deleting-*  &&  rm -rf "$TMPDIR"/Deleting-*',
        "     u+w is NOT enough: 0o300 already grants write. It is READ that is",
        "     missing, and read is what the reaper needs to walk the tree.",
        "",
        "  2. scripts/disk-cleanup.sh   (orphan .derivedData, superseded",
        "     .xcresult bundles at ~100 MB each, stale /private/tmp/playhead-*)",
        "     Preview it first with --dry-run. Or re-run this gate with",
        "     --reclaim-disk to have it run the cleaner once and try again.",
        "",
        "  3. Close a finished bead. Each worktree holds ~2.7 GB of .derivedData",
        "     and nothing reclaims it until `git worktree remove` runs.",
        "",
        "  4. The destination simulator, if it has inflated:",
        f"     DEVELOPER_DIR=... xcrun simctl shutdown {sim_id or '<udid>'}",
        f"     DEVELOPER_DIR=... xcrun simctl erase {sim_id or '<udid>'}",
        "     then sweep $TMPDIR/Deleting-* per (1), because that is where erase",
        "     just put it. NEVER `rm` a booted simulator's directory.",
    ]
    if candidates:
        total = ", ".join(f"{size} {path}" for _reason, size, path in candidates)
        lines += [
            "",
            f"  disk-cleanup.sh reports {len(candidates)} candidate(s) right now:",
            f"    {total}",
        ]
    else:
        lines += [
            "",
            "  disk-cleanup.sh reports NOTHING to reclaim — the space is in live",
            "  worktrees, the simulator, or outside this repo. Options 1, 3 and 4.",
        ]
    lines += [bar, ""]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
def main(argv=None, free_fn=free_bytes, runner=_run) -> int:
    ap = argparse.ArgumentParser(description="Refuse to start a gate below the disk headroom it needs.")
    ap.add_argument("--path", default=".", help="any path on the volume to check (default: cwd)")
    ap.add_argument("--min-gib", type=float, default=DEFAULT_MIN_GIB)
    ap.add_argument("--cleaner", default="scripts/disk-cleanup.sh")
    ap.add_argument("--sim-id", default="", help="destination simulator UDID, for the remedy text")
    ap.add_argument("--reclaim", action="store_true",
                    help="run the cleaner ONCE when short, then re-check (explicit opt-in)")
    ap.add_argument("--quiet", action="store_true", help="print nothing when there is enough room")
    args = ap.parse_args(argv)

    min_b = int(args.min_gib * GIB)
    free_b = free_fn(args.path)

    if free_b >= min_b:
        if not args.quiet:
            print(f"fast-gate: disk preflight OK — {fmt_gib(free_b)} free, needs {fmt_gib(min_b)}")
        return 0

    if args.reclaim:
        print(f"fast-gate: disk preflight SHORT — {fmt_gib(free_b)} free, needs "
              f"{fmt_gib(min_b)}. --reclaim-disk given; running {args.cleaner} once.")
        rc, out = runner([args.cleaner])
        if out.strip():
            print(out.rstrip())
        if rc != 0:
            print(f"fast-gate: {args.cleaner} exited {rc}")
        free_b = free_fn(args.path)
        if free_b >= min_b:
            print(f"fast-gate: reclaimed — {fmt_gib(free_b)} free, proceeding.")
            return 0
        print(f"fast-gate: still short after reclaim — {fmt_gib(free_b)} free.")

    sys.stderr.write(format_refusal(free_b, min_b, survey(args.cleaner, runner), args.sim_id))
    sys.stderr.flush()
    return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
