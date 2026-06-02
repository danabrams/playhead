#!/usr/bin/env python3
"""
l2f-bd4xqf-compare.py — baseline-vs-treatment delta report for bd-4xqf.

Companion to scripts/l2f-bd4xqf-analyze.py. Takes TWO pipeline-dump JSONs
plus the rediff JSON, joins per (episodeId, rediff slot), and reports
deltas vs the 0.18 boundary-coverage baseline. Intended use: env-gated
Mac Catalyst dumps with spanFinalizerEnabled=false vs true.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

REPO_ROOT = "/Users/dabrams/playhead"
REDIFF_PATH = "playhead-dogfood-diagnostics-tier-a-rediff.json"
CAND_THRESHOLD = 0.6
ADWINDOW_THRESHOLD = 0.6
VERDICTS = ["OK", "CAND_NARROW", "FUSION_DROP"]


def clipped_overlap(a_start: float, a_end: float, b_start: float, b_end: float) -> float:
    return max(0.0, min(a_end, b_end) - max(a_start, b_start))


def classify(cand_cov: float | None, win_cov: float) -> str:
    if cand_cov is not None and cand_cov < CAND_THRESHOLD:
        return "CAND_NARROW"
    if win_cov < ADWINDOW_THRESHOLD:
        return "FUSION_DROP"
    return "OK"


def measure_pair(dep: dict[str, Any], s_start: float, s_end: float) -> dict[str, Any]:
    width = s_end - s_start
    win_list = dep.get("adWindows") or []
    cand_list = dep.get("candidateDecodedSpanList")  # may be missing pre-#201

    overlapping = [
        w for w in win_list
        if clipped_overlap(s_start, s_end, float(w.get("startTime", 0.0)), float(w.get("endTime", 0.0))) > 0
    ]
    win_total = sum(
        clipped_overlap(s_start, s_end, float(w.get("startTime", 0.0)), float(w.get("endTime", 0.0)))
        for w in win_list
    )
    win_cov = win_total / width if width > 0 else 0.0

    cand_cov: float | None = None
    if cand_list is not None:
        cand_total = sum(
            clipped_overlap(s_start, s_end, float(c.get("startTime", 0.0)), float(c.get("endTime", 0.0)))
            for c in cand_list
        )
        cand_cov = cand_total / width if width > 0 else 0.0

    constraints: list[str] = []
    any_boundary_adjust = False
    for w in overlapping:
        cs = w.get("spanFinalizerConstraintsFired") or []
        if isinstance(cs, list):
            constraints.extend(str(c) for c in cs)
        s_adj = float(w.get("boundaryRefinementStartAdjustment", 0.0) or 0.0)
        e_adj = float(w.get("boundaryRefinementEndAdjustment", 0.0) or 0.0)
        if s_adj != 0.0 or e_adj != 0.0:
            any_boundary_adjust = True
    mean_constraints = (len(constraints) / len(overlapping)) if overlapping else 0.0

    return {
        "winCoverage": win_cov,
        "candCoverage": cand_cov,
        "verdict": classify(cand_cov, win_cov),
        "constraintsFired": constraints,
        "meanConstraintsPerWindow": mean_constraints,
        "anyBoundaryAdjustment": any_boundary_adjust,
        "overlappingWindowCount": len(overlapping),
    }


def compare(baseline_eps: list[dict[str, Any]], treatment_eps: list[dict[str, Any]],
            rediff_eps: list[dict[str, Any]]) -> dict[str, Any]:
    base_by_id = {e["episodeId"]: e for e in baseline_eps}
    treat_by_id = {e["episodeId"]: e for e in treatment_eps}
    only_baseline = sorted(set(base_by_id) - set(treat_by_id))
    only_treatment = sorted(set(treat_by_id) - set(base_by_id))

    pairs: list[dict[str, Any]] = []
    for rep in rediff_eps:
        if not rep.get("rotated"):
            continue
        ep_id = rep["episodeId"]
        bep, tep = base_by_id.get(ep_id), treat_by_id.get(ep_id)
        if bep is None or tep is None:
            continue
        for slot in rep.get("adSlots") or []:
            s_start = float(slot.get("startSeconds", 0.0))
            s_end = float(slot.get("endSeconds", 0.0))
            if s_end <= s_start:
                continue
            b = measure_pair(bep, s_start, s_end)
            t = measure_pair(tep, s_start, s_end)
            d_cand = (t["candCoverage"] - b["candCoverage"]) if (
                b["candCoverage"] is not None and t["candCoverage"] is not None
            ) else None
            pairs.append({
                "episodeId": ep_id,
                "slotStart": s_start,
                "slotEnd": s_end,
                "slotWidth": s_end - s_start,
                "baseline": b,
                "treatment": t,
                "deltaPipelineCoverage": t["winCoverage"] - b["winCoverage"],
                "deltaCandidateCoverage": d_cand,
                "newConstraintsFiredInTreatment": sorted(set(t["constraintsFired"]) - set(b["constraintsFired"])),
                "verdictChange": f"{b['verdict']}->{t['verdict']}" if b["verdict"] != t["verdict"] else None,
            })

    matrix: dict[str, dict[str, int]] = {v: {w: 0 for w in VERDICTS} for v in VERDICTS}
    base_win = treat_win = base_cand = treat_cand = 0.0
    cand_pairs = 0
    counter: dict[str, int] = {}
    for p in pairs:
        matrix[p["baseline"]["verdict"]][p["treatment"]["verdict"]] += 1
        base_win += p["baseline"]["winCoverage"]
        treat_win += p["treatment"]["winCoverage"]
        if p["baseline"]["candCoverage"] is not None and p["treatment"]["candCoverage"] is not None:
            base_cand += p["baseline"]["candCoverage"]
            treat_cand += p["treatment"]["candCoverage"]
            cand_pairs += 1
        for c in p["treatment"]["constraintsFired"]:
            counter[c] = counter.get(c, 0) + 1

    n = len(pairs)
    mb_win = base_win / n if n else 0.0
    mt_win = treat_win / n if n else 0.0
    mb_cand = base_cand / cand_pairs if cand_pairs else None
    mt_cand = treat_cand / cand_pairs if cand_pairs else None
    by_delta = sorted(pairs, key=lambda p: p["deltaPipelineCoverage"], reverse=True)

    return {
        "totalPairs": n,
        "onlyInBaseline": only_baseline,
        "onlyInTreatment": only_treatment,
        "meanBaselinePipelineCoverage": mb_win,
        "meanTreatmentPipelineCoverage": mt_win,
        "deltaMeanPipelineCoverage": mt_win - mb_win,
        "meanBaselineCandidateCoverage": mb_cand,
        "meanTreatmentCandidateCoverage": mt_cand,
        "deltaMeanCandidateCoverage": (mt_cand - mb_cand) if (mb_cand is not None and mt_cand is not None) else None,
        "verdictTransitionMatrix": matrix,
        "topPositiveDelta": by_delta[:5],
        "topNegativeDelta": sorted(by_delta, key=lambda p: p["deltaPipelineCoverage"])[:5],
        "mostFiredConstraints": sorted(counter.items(), key=lambda kv: kv[1], reverse=True),
        "pairs": pairs,
    }


def _delta_rows(pairs: list[dict[str, Any]]) -> list[str]:
    rows = ["| episodeId | slot | Δ pipeline cov | baseline → treatment | verdict change |",
            "|---|---|---:|---|---|"]
    for p in pairs:
        vc = p["verdictChange"] or "—"
        rows.append(f"| {p['episodeId']} | [{p['slotStart']:.1f},{p['slotEnd']:.1f}] | "
                    f"{p['deltaPipelineCoverage']:+.2%} | "
                    f"{p['baseline']['winCoverage']:.2%} → {p['treatment']['winCoverage']:.2%} | {vc} |")
    return rows


def render_markdown(r: dict[str, Any], base_fin: bool, treat_fin: bool) -> str:
    lines = ["# bd-4xqf baseline-vs-treatment delta report", ""]
    n = r["totalPairs"]
    if n == 0:
        lines.append("_No (episodeId, rediff slot) pairs joined across both dumps._")
        if r["onlyInBaseline"]:
            lines.append(f"- only in baseline: {r['onlyInBaseline']}")
        if r["onlyInTreatment"]:
            lines.append(f"- only in treatment: {r['onlyInTreatment']}")
        return "\n".join(lines) + "\n"
    if treat_fin and not base_fin:
        lines += ["_Treatment has SpanFinalizer telemetry; baseline does not (expected wire-in scenario)._", ""]
    lines.append(f"**Pairs analyzed:** {n}")
    lines.append(f"**Mean pipeline coverage:** {r['meanBaselinePipelineCoverage']:.2%} → "
                 f"{r['meanTreatmentPipelineCoverage']:.2%} (Δ {r['deltaMeanPipelineCoverage']:+.2%})")
    if r["meanBaselineCandidateCoverage"] is not None:
        lines.append(f"**Mean candidate coverage:** {r['meanBaselineCandidateCoverage']:.2%} → "
                     f"{r['meanTreatmentCandidateCoverage']:.2%} (Δ {r['deltaMeanCandidateCoverage']:+.2%})")
    else:
        lines.append("_Candidate coverage unavailable (one or both dumps are pre-#201)._")
    lines += ["", "## Verdict transition matrix (baseline → treatment)", "",
              "| baseline \\ treatment | " + " | ".join(VERDICTS) + " |",
              "|---|" + "---|" * len(VERDICTS)]
    for bv in VERDICTS:
        lines.append(f"| {bv} | " + " | ".join(str(r["verdictTransitionMatrix"][bv][tv]) for tv in VERDICTS) + " |")
    lines += ["", "## Top 5 pairs by Δ pipeline coverage (most positive)", ""] + _delta_rows(r["topPositiveDelta"])
    lines += ["", "## Top 5 pairs by Δ pipeline coverage (most negative)", ""] + _delta_rows(r["topNegativeDelta"])
    if r["mostFiredConstraints"]:
        lines += ["", "## Most-fired SpanFinalizer constraints in treatment", ""]
        for name, count in r["mostFiredConstraints"][:10]:
            lines.append(f"- `{name}`: {count}")
    if r["onlyInBaseline"] or r["onlyInTreatment"]:
        lines += ["", "## Episode-set asymmetry (informational)", ""]
        if r["onlyInBaseline"]:
            lines.append(f"- only in baseline: {len(r['onlyInBaseline'])}")
        if r["onlyInTreatment"]:
            lines.append(f"- only in treatment: {len(r['onlyInTreatment'])}")
    lines.append("")
    return "\n".join(lines) + "\n"


def has_finalizer_telemetry(eps: list[dict[str, Any]]) -> bool:
    return any(w.get("spanFinalizerConstraintsFired") for e in eps for w in (e.get("adWindows") or []))


def run_self_test() -> int:
    # A: 20%→90% (FUSION_DROP→OK); B: 90%→30% (OK→FUSION_DROP); C: stable OK
    def ep(eid: str, win_end: float, fired: list[str] | None = None) -> dict[str, Any]:
        w = {"startTime": 0.0, "endTime": win_end}
        if fired is not None:
            w["spanFinalizerConstraintsFired"] = fired
        return {"episodeId": eid, "candidateDecodedSpanList": [{"startTime": 0.0, "endTime": 100.0}],
                "adWindows": [w]}
    baseline = [ep("A", 20.0), ep("B", 90.0), ep("C", 100.0)]
    treatment = [ep("A", 90.0, ["minDuration", "silenceAnchor"]),
                 ep("B", 30.0, ["minDuration"]),
                 ep("C", 100.0)]
    slot = [{"startSeconds": 0.0, "endSeconds": 100.0}]
    rediff = [{"episodeId": e["episodeId"], "rotated": True, "adSlots": slot} for e in baseline]
    res = compare(baseline, treatment, rediff)
    by_ep = {p["episodeId"]: p for p in res["pairs"]}
    m = res["verdictTransitionMatrix"]
    checks = [
        ("totalPairs=3", res["totalPairs"] == 3),
        ("meanBase=0.7", abs(res["meanBaselinePipelineCoverage"] - 0.7) < 1e-6),
        ("meanTreat=2.2/3", abs(res["meanTreatmentPipelineCoverage"] - (2.2 / 3.0)) < 1e-6),
        ("A delta=+0.7", abs(by_ep["A"]["deltaPipelineCoverage"] - 0.7) < 1e-6),
        ("B delta=-0.6", abs(by_ep["B"]["deltaPipelineCoverage"] - (-0.6)) < 1e-6),
        ("A verdict change", by_ep["A"]["verdictChange"] == "FUSION_DROP->OK"),
        ("B verdict change", by_ep["B"]["verdictChange"] == "OK->FUSION_DROP"),
        ("C no change", by_ep["C"]["verdictChange"] is None),
        ("matrix entries", m["FUSION_DROP"]["OK"] == 1 and m["OK"]["FUSION_DROP"] == 1 and m["OK"]["OK"] == 1),
        ("topPositive A first", res["topPositiveDelta"][0]["episodeId"] == "A"),
        ("topNegative B first", res["topNegativeDelta"][0]["episodeId"] == "B"),
        ("constraints tallied", {n for n, _ in res["mostFiredConstraints"]} == {"minDuration", "silenceAnchor"}),
    ]
    failures = [name for name, ok in checks if not ok]
    if failures:
        print("SELF-TEST FAIL: " + ", ".join(failures), file=sys.stderr)
        return 1
    print("self-test ok: +Δ, -Δ, verdict transitions, transition matrix, constraint accounting all verified")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--baseline", help="baseline pipeline-dump JSON path")
    ap.add_argument("--treatment", help="treatment pipeline-dump JSON path")
    ap.add_argument("--rediff", default=None, help="rediff JSON path (defaults to repo-root tier-a-rediff)")
    ap.add_argument("--json", action="store_true", help="emit JSON instead of markdown")
    ap.add_argument("--self-test", action="store_true", help="run synthetic self-test and exit")
    args = ap.parse_args()

    if args.self_test:
        return run_self_test()
    if not args.baseline or not args.treatment:
        print("--baseline and --treatment are required (or use --self-test)", file=sys.stderr)
        return 2
    rediff_path = args.rediff or os.path.join(REPO_ROOT, REDIFF_PATH)
    for path in (args.baseline, args.treatment, rediff_path):
        if not os.path.exists(path):
            print(f"missing file: {path}", file=sys.stderr)
            return 2
    with open(args.baseline) as f:
        baseline = json.load(f)
    with open(args.treatment) as f:
        treatment = json.load(f)
    with open(rediff_path) as f:
        rediff = json.load(f)
    base_eps = baseline.get("episodes")
    treat_eps = treatment.get("episodes")
    if base_eps is None:
        print(f"baseline dump has no 'episodes' array: {args.baseline}", file=sys.stderr)
        return 2
    if treat_eps is None:
        print(f"treatment dump has no 'episodes' array: {args.treatment}", file=sys.stderr)
        return 2
    result = compare(base_eps, treat_eps, rediff.get("episodes", []))
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(render_markdown(result, has_finalizer_telemetry(base_eps), has_finalizer_telemetry(treat_eps)), end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
