#!/usr/bin/env python3
"""
l2f-bd4xqf-analyze.py — DAI boundary-undersizing diagnosis (bd-4xqf).

Pairs each rediff-confirmed DAI ad slot (rotated:true episodes) with the
overlapping pipeline candidateDecodedSpanList entries AND the overlapping
adWindows entries, computes clipped-overlap coverage, and classifies each
pair as CAND_NARROW, FUSION_DROP, or OK to localize where the long span is
dropped.

Requires the post-#201 pipeline-dump schema (candidateDecodedSpanList).
Gracefully no-ops on pre-#201 dumps with an explanatory message.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from typing import Any

REPO_ROOT = "/Users/dabrams/playhead"
PIPELINE_DUMP_GLOB = "playhead-dogfood-diagnostics-pipeline-dump-*.json"
REDIFF_PATH = "playhead-dogfood-diagnostics-tier-a-rediff.json"

CAND_THRESHOLD = 0.6
ADWINDOW_THRESHOLD = 0.6


def clipped_overlap(a_start: float, a_end: float, b_start: float, b_end: float) -> float:
    """Return the length of [a_start,a_end] ∩ [b_start,b_end], clamped at 0."""
    lo = max(a_start, b_start)
    hi = min(a_end, b_end)
    return max(0.0, hi - lo)


def newest_pipeline_dump(root: str) -> str | None:
    matches = glob.glob(os.path.join(root, PIPELINE_DUMP_GLOB))
    if not matches:
        return None
    return max(matches, key=os.path.getmtime)


def classify_pair(cand_cov: float, win_cov: float) -> str:
    if cand_cov < CAND_THRESHOLD:
        return "CAND_NARROW"
    if win_cov < ADWINDOW_THRESHOLD:
        return "FUSION_DROP"
    return "OK"


def analyze(episodes_dump: list[dict[str, Any]], episodes_rediff: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute per-pair verdicts and aggregate counts."""
    dump_by_id = {e["episodeId"]: e for e in episodes_dump}
    pairs: list[dict[str, Any]] = []

    for rep in episodes_rediff:
        if not rep.get("rotated"):
            continue
        slots = rep.get("adSlots") or []
        if not slots:
            continue
        ep_id = rep["episodeId"]
        dep = dump_by_id.get(ep_id)
        if dep is None:
            continue
        cand_list = dep.get("candidateDecodedSpanList") or []
        win_list = dep.get("adWindows") or []
        for slot in slots:
            s_start = float(slot.get("startSeconds", 0.0))
            s_end = float(slot.get("endSeconds", 0.0))
            width = s_end - s_start
            if width <= 0:
                continue
            cand_total = sum(
                clipped_overlap(s_start, s_end, float(c.get("startTime", 0.0)), float(c.get("endTime", 0.0)))
                for c in cand_list
            )
            win_total = sum(
                clipped_overlap(s_start, s_end, float(w.get("startTime", 0.0)), float(w.get("endTime", 0.0)))
                for w in win_list
            )
            cand_cov = cand_total / width
            win_cov = win_total / width
            verdict = classify_pair(cand_cov, win_cov)
            pairs.append({
                "episodeId": ep_id,
                "slotStart": s_start,
                "slotEnd": s_end,
                "slotWidth": width,
                "candidateCoverage": cand_cov,
                "adwindowCoverage": win_cov,
                "verdict": verdict,
            })

    counts = {"CAND_NARROW": 0, "FUSION_DROP": 0, "OK": 0}
    for p in pairs:
        counts[p["verdict"]] += 1
    total = len(pairs) or 1  # guard divide-by-zero in pct
    pct = {k: 100.0 * v / total for k, v in counts.items()}
    return {"pairs": pairs, "counts": counts, "pct": pct, "total": len(pairs)}


def render_markdown(result: dict[str, Any]) -> str:
    lines = ["# bd-4xqf boundary-undersizing analysis", ""]
    if result["total"] == 0:
        lines.append("_No rotated rediff episodes with adSlots paired against the dump._")
        return "\n".join(lines) + "\n"
    lines.append("| episodeId | slot [s,e] | width | cand cov | win cov | verdict |")
    lines.append("|---|---|---:|---:|---:|---|")
    for p in result["pairs"]:
        lines.append(
            f"| {p['episodeId']} | [{p['slotStart']:.2f}, {p['slotEnd']:.2f}] | "
            f"{p['slotWidth']:.2f} | {p['candidateCoverage']:.2%} | "
            f"{p['adwindowCoverage']:.2%} | {p['verdict']} |"
        )
    lines.append("")
    lines.append(f"**Total pairs:** {result['total']}")
    for k in ("CAND_NARROW", "FUSION_DROP", "OK"):
        lines.append(f"- {k}: {result['counts'][k]} ({result['pct'][k]:.1f}%)")
    return "\n".join(lines) + "\n"


def run_self_test() -> int:
    """Synthetic data exercising all three verdicts."""
    slot = [{"startSeconds": 0.0, "endSeconds": 100.0}]
    dump = [
        {"episodeId": "ok-ep", "candidateDecodedSpanList": [{"startTime": 0.0, "endTime": 100.0}],
         "adWindows": [{"startTime": 0.0, "endTime": 100.0}]},
        {"episodeId": "fusion-ep", "candidateDecodedSpanList": [{"startTime": 0.0, "endTime": 100.0}],
         "adWindows": [{"startTime": 0.0, "endTime": 20.0}]},
        {"episodeId": "narrow-ep", "candidateDecodedSpanList": [{"startTime": 0.0, "endTime": 10.0}],
         "adWindows": [{"startTime": 0.0, "endTime": 100.0}]},
    ]
    rediff = [{"episodeId": e["episodeId"], "rotated": True, "adSlots": slot} for e in dump]
    res = analyze(dump, rediff)
    expected = {"OK": 1, "FUSION_DROP": 1, "CAND_NARROW": 1}
    if res["counts"] != expected or res["total"] != 3:
        print(f"SELF-TEST FAIL: counts={res['counts']} total={res['total']} expected={expected}", file=sys.stderr)
        return 1
    print("self-test ok: all three verdicts fired (OK, FUSION_DROP, CAND_NARROW)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--json", action="store_true", help="emit JSON instead of markdown")
    ap.add_argument("--self-test", action="store_true", help="run synthetic self-test and exit")
    ap.add_argument("--repo-root", default=REPO_ROOT, help=argparse.SUPPRESS)
    args = ap.parse_args()

    if args.self_test:
        return run_self_test()

    dump_path = newest_pipeline_dump(args.repo_root)
    if dump_path is None:
        print(f"no pipeline-dump JSON found at {args.repo_root}/{PIPELINE_DUMP_GLOB}", file=sys.stderr)
        return 0
    rediff_path = os.path.join(args.repo_root, REDIFF_PATH)
    if not os.path.exists(rediff_path):
        print(f"rediff JSON missing: {rediff_path}", file=sys.stderr)
        return 0

    with open(dump_path) as f:
        dump = json.load(f)
    with open(rediff_path) as f:
        rediff = json.load(f)

    dump_eps = dump.get("episodes", [])
    rediff_eps = rediff.get("episodes", [])

    # Pre-#201 fallback: none of the dump episodes have candidateDecodedSpanList.
    if not any("candidateDecodedSpanList" in e for e in dump_eps):
        print(
            "dump is pre-#201 (no candidateDecodedSpanList field) — re-run "
            "PipelineDumpLiveTests on Mac Catalyst with PLAYHEAD_PIPELINE_DUMP=1 "
            "to enable analysis"
        )
        return 0

    result = analyze(dump_eps, rediff_eps)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(render_markdown(result), end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
