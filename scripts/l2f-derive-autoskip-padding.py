#!/usr/bin/env python3
"""Derive (and re-verify) the asymmetric auto-skip edge-padding margins.

playhead-98co: auto-skip must be late-safe at the span start and conservative
at the span end. This script recomputes the per-edge / per-anchor-tier signed
error distributions that back `AutoSkipEdgePadding` and asserts the frozen
margins still cover every observed content-clip-direction event. Run it
whenever the boundary stack changes (stinger bank re-learn, joint recipe
retune) BEFORE trusting the frozen margins.

Data:
  --dump   pipeline dump JSON with per-window `stingerRefinement` traces.
           Default: the 2026-07-16 xsdz39bank build (the derivation build).
  Gold:    the 2026-07-15 44-break artifact (primary) and gold v6 (robustness)
           from TestFixtures/Corpus/Evaluations, resolved relative to the repo.

Signed error convention: predicted - gold. Start < 0 and end > 0 are the
content-clipping directions.

See docs/autoskip-edge-padding-derivation-2026-07-20.md for the full
derivation, adjudications (ted requalify), and the honest n-is-small caveats.
"""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import statistics
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
EVAL_DIR = REPO / "TestFixtures" / "Corpus" / "Evaluations"
GOLD_44 = EVAL_DIR / (
    "earaudit-oracle-gold-"
    "b77c2804aa2a9afe59f2193c4d73a86b2b1b5193b00b864a607b39d49fb1ce82.json"
)
GOLD_V6 = EVAL_DIR / (
    "earaudit-oracle-gold-"
    "836b81885f6d279a84c1ef0dee83302e7df6ed28f0d20ec2db621b518f1ef220.json"
)
DEFAULT_DUMP = pathlib.Path(
    "/Users/dabrams/playhead-baselines/"
    "playhead-dogfood-diagnostics-pipeline-dump-53ep-xsdz39bank-20260716.json"
)

# Frozen margins (must mirror AutoSkipEdgePadding.swift).
START_MARGIN_STINGER = 0.75
END_MARGIN_STINGER = 0.75
END_MARGIN_UNANCHORED = 10.25
# start unanchored: unskippable (markOnly) — no margin to verify.
# byte tier: derived from the xsdz.44 spike (analysis/byte-forensics-
# spike-2026-07-17.md), not recomputable from the dump; constants noted only.
START_MARGIN_BYTE = 0.50
END_MARGIN_BYTE = 0.75

# 2026-07-16 otm-ted-requalify adjudication: the 07-15 gold pre-roll end
# (70.1) was re-attested to 101.3 (gold v6). The 44-gold "+30.92 late end"
# on this break is a gold under-label, not a prediction miss. Exclude it
# from the 44-gold stinger-end tier check (v6 carries the corrected label
# and is checked in full).
ADJUDICATED_44_END = {("ted-business-2026-05-25-the-secret-to-making-the-right-career-de", 70.1)}

# The nikki-glaser pre-anchor misfire mode (2 of 7 snapped starts ~29 s
# early in both golds) is handled by per-show demotion in the policy, not
# by margin. Exclude the demoted show from the snapped-START margin check.
START_DEMOTED_SHOWS = {"The Nikki Glaser Podcast"}


def overlap(a: float, b: float, c: float, d: float) -> float:
    return max(0.0, min(b, d) - max(a, c))


def load_rows(eval_path: pathlib.Path, dump_path: pathlib.Path) -> list[dict]:
    evaluation = json.load(eval_path.open(encoding="utf-8"))
    if evaluation.get("artifact_kind") != "oracle_earaudit_gold_boundary_evaluation":
        raise SystemExit(f"unexpected artifact_kind in {eval_path}")
    dump = json.load(dump_path.open(encoding="utf-8"))
    predictions: dict[str, list[dict]] = {}
    for episode in dump.get("episodes", []):
        predictions[episode["episodeId"]] = [
            {
                "start": float(w["startTime"]),
                "end": float(w["endTime"]),
                "trace": w.get("stingerRefinement") or {},
            }
            for w in episode.get("adWindows", [])
        ]
    rows: list[dict] = []
    for asset in evaluation["assets"]:
        for break_ in asset["full_breaks"]:
            best, best_overlap_s = None, 0.0
            for pred in predictions.get(asset["episode_id"], []):
                candidate = overlap(
                    break_["start_seconds"], break_["end_seconds"],
                    pred["start"], pred["end"],
                )
                if candidate > best_overlap_s:
                    best_overlap_s, best = candidate, pred
            if best is None:
                continue
            rows.append({
                "show": asset["show_name"],
                "episode": asset["episode_id"],
                "gold_start": break_["start_seconds"],
                "gold_end": break_["end_seconds"],
                "tol": float(break_["boundary_tolerance_seconds"]),
                "signed_start": best["start"] - break_["start_seconds"],
                "signed_end": best["end"] - break_["end_seconds"],
                "start_snapped": bool(best["trace"].get("startSnapped")),
                "end_snapped": bool(best["trace"].get("endSnapped")),
            })
    return rows


def describe(label: str, values: list[float]) -> None:
    if not values:
        print(f"  {label}: n=0")
        return
    ordered = sorted(values)
    print(
        f"  {label}: n={len(ordered)} min={ordered[0]:+8.2f} "
        f"p50={statistics.median(ordered):+7.2f} max={ordered[-1]:+8.2f}"
    )


def ub95(n: int) -> float:
    """One-sided 95% binomial upper bound given 0 observed events."""
    return 1.0 - 0.05 ** (1.0 / n) if n else float("nan")


def check(rows: list[dict], label: str, *, apply_44_adjudication: bool) -> int:
    print(f"== {label}: {len(rows)} matched breaks")
    start_snapped = [r for r in rows if r["start_snapped"]]
    end_snapped = [r for r in rows if r["end_snapped"]]
    end_unsnapped = [r for r in rows if not r["end_snapped"]]
    describe("START snapped  ", [r["signed_start"] for r in start_snapped])
    describe("START unsnapped", [r["signed_start"] for r in rows if not r["start_snapped"]])
    describe("END   snapped  ", [r["signed_end"] for r in end_snapped])
    describe("END   unsnapped", [r["signed_end"] for r in end_unsnapped])

    failures = 0

    def fail(msg: str) -> None:
        nonlocal failures
        failures += 1
        print(f"  MARGIN VIOLATION: {msg}")

    for r in start_snapped:
        if r["show"] in START_DEMOTED_SHOWS:
            continue  # per-show demotion: markOnly, margin not load-bearing
        if r["signed_start"] < -START_MARGIN_STINGER:
            fail(
                f"snapped start {r['signed_start']:+.2f} < -{START_MARGIN_STINGER} "
                f"({r['show']} {r['episode']})"
            )
    for r in end_snapped:
        if apply_44_adjudication and (r["episode"], r["gold_end"]) in ADJUDICATED_44_END:
            continue
        if r["signed_end"] > END_MARGIN_STINGER:
            fail(
                f"snapped end {r['signed_end']:+.2f} > {END_MARGIN_STINGER} "
                f"({r['show']} {r['episode']})"
            )
    for r in end_unsnapped:
        if r["signed_end"] > END_MARGIN_UNANCHORED:
            fail(
                f"unsnapped end {r['signed_end']:+.2f} > {END_MARGIN_UNANCHORED} "
                f"({r['show']} {r['episode']})"
            )

    n_start = len([r for r in start_snapped if r["show"] not in START_DEMOTED_SHOWS])
    print(
        f"  0-event 95% UBs: start-snapped n={n_start} -> {ub95(n_start) * 100:.1f}% | "
        f"end-snapped n={len(end_snapped)} -> {ub95(len(end_snapped)) * 100:.1f}% | "
        f"end-unsnapped n={len(end_unsnapped)} -> {ub95(len(end_unsnapped)) * 100:.1f}%"
    )
    if failures == 0:
        print("  OK: frozen margins cover every observed clip-direction event")
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dump", type=pathlib.Path, default=DEFAULT_DUMP)
    args = parser.parse_args(argv)
    if not args.dump.exists():
        raise SystemExit(f"dump not found: {args.dump} (pass --dump)")

    print(
        "frozen margins: "
        f"start stinger {START_MARGIN_STINGER}s / byte {START_MARGIN_BYTE}s / "
        f"unanchored markOnly | end stinger {END_MARGIN_STINGER}s / "
        f"byte {END_MARGIN_BYTE}s / unanchored {END_MARGIN_UNANCHORED}s"
    )
    failures = 0
    failures += check(
        load_rows(GOLD_44, args.dump), "44-break gold 2026-07-15 (primary)",
        apply_44_adjudication=True,
    )
    failures += check(
        load_rows(GOLD_V6, args.dump), "gold v6 (robustness)",
        apply_44_adjudication=False,
    )
    if failures:
        print(f"\nFAILED: {failures} margin violation(s) — re-derive before flag-ON")
        return 1
    print("\nPASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
