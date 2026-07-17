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


# --- false-widening (playhead-xsdz.32) -------------------------------------
# The coverage metric above only measures adWindow seconds INSIDE true rediff
# slots — it cannot penalize a treatment that "wins" coverage by enclosing
# content (the product's stated worst outcome, which a width-WIDENING oracle
# risks). These helpers measure the complementary quantity: adWindow seconds
# that fall OUTSIDE every true DAI slot.
#
# HONEST CAVEAT: rediff truth is DAI-only (baked-in host reads are invisible to
# it). So `falseWideningSeconds` is an UPPER BOUND on real content-eating — it
# also counts any legitimate host-read ad detection. The precise ("evidence
# proxy") variant subtracts windows that carry lexical/FM/chapter ad-evidence;
# that requires the dump to emit per-window evidence, which it does not yet, so
# it is reported as a TODO rather than computed. Use the upper bound as a
# conservative gate: if it is small, there is provably little content-eating.

def merge_intervals(ivals: list[tuple[float, float]]) -> list[tuple[float, float]]:
    s = sorted((a, b) for a, b in ivals if b > a)
    if not s:
        return []
    out: list[list[float]] = [list(s[0])]
    for a, b in s[1:]:
        if a <= out[-1][1]:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return [(a, b) for a, b in out]


def total_len(ivals: list[tuple[float, float]]) -> float:
    return sum(b - a for a, b in ivals)


def intersect_len(a_merged: list[tuple[float, float]], b_merged: list[tuple[float, float]]) -> float:
    # both inputs assumed merged + sorted; two-pointer sweep
    total = 0.0
    i = j = 0
    while i < len(a_merged) and j < len(b_merged):
        total += max(0.0, min(a_merged[i][1], b_merged[j][1]) - max(a_merged[i][0], b_merged[j][0]))
        if a_merged[i][1] < b_merged[j][1]:
            i += 1
        else:
            j += 1
    return total


def episode_false_widening(ep: dict[str, Any], true_slots: list[tuple[float, float]]) -> dict[str, Any]:
    all_intervals = [
        (float(w.get("startTime", 0.0)), float(w.get("endTime", 0.0)))
        for w in (ep.get("adWindows") or [])
    ]
    # ELIGIBLE-only = windows auto-skip actually acts on (playhead-xsdz.36.1 / Dan
    # 2026-07-17). blocked/markOnly windows never skip, so they eat no content; they
    # are the banner/safety tier. The eligible metric is the auto-skip go/no-go gate;
    # the all-windows metric stays as the over-widening early-warning.
    eligible_intervals = [
        (float(w.get("startTime", 0.0)), float(w.get("endTime", 0.0)))
        for w in (ep.get("adWindows") or [])
        if w.get("eligibilityGate") == "eligible"
    ]
    slots = merge_intervals(true_slots)

    def _fw(intervals: list[tuple[float, float]]) -> tuple[float, float]:
        windows = merge_intervals(intervals)
        ad_total = total_len(windows)
        return ad_total, intersect_len(windows, slots)

    ad_total, inside = _fw(all_intervals)
    elig_total, elig_inside = _fw(eligible_intervals)
    return {
        "adWindowSeconds": ad_total,
        "adWindowSecondsInsideTrueSlots": inside,
        "falseWideningSeconds": ad_total - inside,   # ALL windows outside true slots (upper bound; incl. blocked/markOnly)
        "falseWideningSecondsEligible": elig_total - elig_inside,  # AUTO-SKIP relevant: only eligibilityGate==eligible
        "trueSlotSeconds": total_len(slots),
    }
# ---------------------------------------------------------------------------


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


def load_gold_breaks(path: str) -> dict[str, list[tuple[float, float]]]:
    """episode_id -> gold full-break intervals from an earaudit-oracle-gold
    artifact. Evidence-aware false-widening (playhead-xsdz.32): gold-attested
    ad regions count as TRUE ad audio even where tier-a disagrees — the
    2026-07-16 gate breach was ~2/3 tier-a edge noise on regions Dan's gold
    labels endorse."""
    with open(path, "r", encoding="utf-8") as handle:
        artifact = json.load(handle)
    if artifact.get("artifact_kind") != "oracle_earaudit_gold_boundary_evaluation":
        raise SystemExit(f"--gold-evaluation: unexpected artifact_kind in {path}")
    return {
        asset["episode_id"]: [
            (float(b["start_seconds"]), float(b["end_seconds"]))
            for b in asset.get("full_breaks", [])
        ]
        for asset in artifact.get("assets", [])
    }


def compare(baseline_eps: list[dict[str, Any]], treatment_eps: list[dict[str, Any]],
            rediff_eps: list[dict[str, Any]],
            gold_breaks: dict[str, list[tuple[float, float]]] | None = None) -> dict[str, Any]:
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

    # False-widening (playhead-xsdz.32): per-episode adWindow seconds enclosing
    # content OUTSIDE every true DAI slot. Computed at EPISODE granularity (not
    # per-slot) to avoid double-attributing a window that spans two slots.
    ep_true_slots: dict[str, list[tuple[float, float]]] = {}
    for rep in rediff_eps:
        if not rep.get("rotated"):
            continue
        slots = [
            (float(s.get("startSeconds", 0.0)), float(s.get("endSeconds", 0.0)))
            for s in (rep.get("adSlots") or [])
            if float(s.get("endSeconds", 0.0)) > float(s.get("startSeconds", 0.0))
        ]
        if slots:
            ep_true_slots[rep["episodeId"]] = slots

    fw_rows: list[dict[str, Any]] = []
    fw_base_total = fw_treat_total = 0.0
    fw_treat_total_elig = 0.0
    for ep_id, slots in sorted(ep_true_slots.items()):
        bep, tep = base_by_id.get(ep_id), treat_by_id.get(ep_id)
        if bep is None or tep is None:
            continue
        truth = slots + (gold_breaks or {}).get(ep_id, [])
        fb = episode_false_widening(bep, truth)
        ft = episode_false_widening(tep, truth)
        fw_base_total += fb["falseWideningSeconds"]
        fw_treat_total += ft["falseWideningSeconds"]
        fw_treat_total_elig += ft["falseWideningSecondsEligible"]
        fw_rows.append({
            "episodeId": ep_id,
            "showSlug": (tep.get("showSlug") or bep.get("showSlug") or ""),
            "baseline": fb,
            "treatment": ft,
            "deltaFalseWideningSeconds": ft["falseWideningSeconds"] - fb["falseWideningSeconds"],
        })
    fw_by_show: dict[str, float] = {}
    for row in fw_rows:
        fw_by_show[row["showSlug"]] = fw_by_show.get(row["showSlug"], 0.0) + row["treatment"]["falseWideningSeconds"]
    fw_worst = sorted(fw_rows, key=lambda r: r["treatment"]["falseWideningSeconds"], reverse=True)
    fw_worst_elig = sorted(fw_rows, key=lambda r: r["treatment"]["falseWideningSecondsEligible"], reverse=True)

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
        # False-widening gate (playhead-xsdz.32) — upper bound; DAI-only truth.
        "falseWidening": {
            "episodeCount": len(fw_rows),
            "baselineTotalSeconds": fw_base_total,
            "treatmentTotalSeconds": fw_treat_total,
            "deltaTotalSeconds": fw_treat_total - fw_base_total,
            "baselineMeanSecondsPerEpisode": (fw_base_total / len(fw_rows)) if fw_rows else 0.0,
            "treatmentMeanSecondsPerEpisode": (fw_treat_total / len(fw_rows)) if fw_rows else 0.0,
            "treatmentMaxSecondsPerEpisode": (fw_worst[0]["treatment"]["falseWideningSeconds"] if fw_worst else 0.0),
            # ELIGIBLE-only = the auto-skip go/no-go metric (Dan 2026-07-17). All-windows
            # above is the tracked over-widening early-warning (counts blocked/markOnly too).
            "treatmentMeanSecondsPerEpisodeEligible": (fw_treat_total_elig / len(fw_rows)) if fw_rows else 0.0,
            "treatmentMaxSecondsPerEpisodeEligible": (fw_worst_elig[0]["treatment"]["falseWideningSecondsEligible"] if fw_worst_elig else 0.0),
            "perShowTreatmentSeconds": dict(sorted(fw_by_show.items(), key=lambda kv: kv[1], reverse=True)),
            "worstEpisodes": fw_worst[:8],
            "worstEpisodesEligible": fw_worst_elig[:8],
            "note": "UPPER BOUND: rediff truth is DAI-only, so this also counts legitimate baked-in "
                    "host-read detections. Evidence-proxy variant (subtract windows carrying "
                    "lexical/FM/chapter ad-evidence) is a TODO pending per-window evidence in the dump.",
            "evidenceProxyImplemented": False,
        },
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

    fw = r.get("falseWidening")
    if fw and fw["episodeCount"]:
        lines += ["", "## ⚠️ False-widening — content-seconds enclosed (playhead-xsdz.32)", ""]
        lines.append(f"**Treatment content-seconds enclosed:** total {fw['treatmentTotalSeconds']:.1f}s "
                     f"across {fw['episodeCount']} episodes "
                     f"(mean {fw['treatmentMeanSecondsPerEpisode']:.1f}s/ep, "
                     f"max {fw['treatmentMaxSecondsPerEpisode']:.1f}s/ep) — "
                     f"baseline {fw['baselineTotalSeconds']:.1f}s "
                     f"(Δ {fw['deltaTotalSeconds']:+.1f}s).")
        lines.append(f"_{fw['note']}_")
        if fw["worstEpisodes"]:
            lines += ["", "| episodeId | show | treatment enclosed (s) | Δ vs baseline (s) |",
                      "|---|---|---:|---:|"]
            for row in fw["worstEpisodes"]:
                lines.append(f"| {row['episodeId']} | {row['showSlug']} | "
                             f"{row['treatment']['falseWideningSeconds']:.1f} | "
                             f"{row['deltaFalseWideningSeconds']:+.1f} |")

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
    # False-widening (playhead-xsdz.32): separate fixtures so the coverage
    # assertions above stay undisturbed.
    #   D: window [5,40] over slot [10,20]  -> enclosed 25s outside the slot
    #   E: window [10,20] exactly on slot   -> enclosed 0s
    #   F: two windows [0,5]+[95,100] over slot [10,20] -> enclosed 10s (both outside)
    def fw_ep(eid, windows):
        return {"episodeId": eid, "showSlug": eid.lower(),
                "adWindows": [{"startTime": s, "endTime": e} for s, e in windows]}
    fw_dump = [fw_ep("D", [(5, 40)]), fw_ep("E", [(10, 20)]), fw_ep("F", [(0, 5), (95, 100)])]
    fw_slot = [{"startSeconds": 10.0, "endSeconds": 20.0}]
    fw_rediff = [{"episodeId": e["episodeId"], "rotated": True, "adSlots": fw_slot} for e in fw_dump]
    fw_res = compare(fw_dump, fw_dump, fw_rediff)["falseWidening"]
    fw_by = {row["episodeId"]: row for row in fw_res["worstEpisodes"]}
    checks += [
        ("fw D=25s", abs(fw_by["D"]["treatment"]["falseWideningSeconds"] - 25.0) < 1e-6),
        ("fw E=0s", abs(fw_by["E"]["treatment"]["falseWideningSeconds"] - 0.0) < 1e-6),
        ("fw F=10s (both windows outside)", abs(fw_by["F"]["treatment"]["falseWideningSeconds"] - 10.0) < 1e-6),
        ("fw max=25s", abs(fw_res["treatmentMaxSecondsPerEpisode"] - 25.0) < 1e-6),
        ("fw total=35s", abs(fw_res["treatmentTotalSeconds"] - 35.0) < 1e-6),
        ("fw D inside=10s", abs(fw_by["D"]["treatment"]["adWindowSecondsInsideTrueSlots"] - 10.0) < 1e-6),
        ("fw worst is D", fw_res["worstEpisodes"][0]["episodeId"] == "D"),
    ]

    # Evidence-aware gate (playhead-xsdz.32): gold full-breaks on D cover
    # its widened region entirely -> D's enclosed seconds drop to 0 while
    # F (no gold) is unchanged.
    gold = {"D": [(0.0, 200.0)]}
    fw_gold = compare(fw_dump, fw_dump, fw_rediff, gold_breaks=gold)["falseWidening"]
    fw_gold_by = {row["episodeId"]: row for row in fw_gold["worstEpisodes"]}
    checks += [
        ("fw-gold D=0s (gold acquits)", abs(fw_gold_by["D"]["treatment"]["falseWideningSeconds"] - 0.0) < 1e-6),
        ("fw-gold F=10s (unchanged, no gold)", abs(fw_gold_by["F"]["treatment"]["falseWideningSeconds"] - 10.0) < 1e-6),
        ("fw-gold total=10s", abs(fw_gold["treatmentTotalSeconds"] - 10.0) < 1e-6),
    ]

    failures = [name for name, ok in checks if not ok]
    if failures:
        print("SELF-TEST FAIL: " + ", ".join(failures), file=sys.stderr)
        return 1
    print("self-test ok: +Δ, -Δ, verdict transitions, transition matrix, constraint accounting, "
          "false-widening (enclosed-seconds, union, max/total) all verified")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--baseline", help="baseline pipeline-dump JSON path")
    ap.add_argument("--treatment", help="treatment pipeline-dump JSON path")
    ap.add_argument("--rediff", default=None, help="rediff JSON path (defaults to repo-root tier-a-rediff)")
    ap.add_argument("--json", action="store_true", help="emit JSON instead of markdown")
    ap.add_argument("--max-fp-seconds-per-episode", type=float, default=None,
                    help="HARD GATE (playhead-xsdz.32): fail (exit 3) if any treatment episode's "
                         "content-seconds-enclosed (false-widening) exceeds this. The activation "
                         "go/no-go must set this — eating content is the product's worst outcome.")
    ap.add_argument("--gold-evaluation", default=None,
                    help="earaudit-oracle-gold artifact: gold full-breaks count as TRUE ad "
                         "audio in the false-widening computation (evidence-aware gate)")
    ap.add_argument("--gate-metric", choices=["eligible", "all"], default="eligible",
                    help="which false-widening metric the HARD GATE checks (Dan 2026-07-17): "
                         "'eligible' = only skip-eligible windows (auto-skip go/no-go, the default); "
                         "'all' = every adWindow incl. blocked/markOnly (conservative over-widening bound). "
                         "The other metric is always reported alongside as an early-warning.")
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
    gold_breaks = load_gold_breaks(args.gold_evaluation) if args.gold_evaluation else None
    result = compare(base_eps, treat_eps, rediff.get("episodes", []), gold_breaks=gold_breaks)
    if gold_breaks is not None:
        result["falseWidening"]["evidenceAware"] = {
            "goldEvaluation": os.path.basename(args.gold_evaluation),
            "goldEpisodes": sum(1 for e in gold_breaks.values() if e),
        }
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(render_markdown(result, has_finalizer_telemetry(base_eps), has_finalizer_telemetry(treat_eps)), end="")

    # False-widening HARD GATE (playhead-xsdz.32): activation must not eat content.
    if args.max_fp_seconds_per_episode is not None:
        fw = result["falseWidening"]
        gate_eligible = args.gate_metric == "eligible"
        gate_max = fw["treatmentMaxSecondsPerEpisodeEligible"] if gate_eligible else fw["treatmentMaxSecondsPerEpisode"]
        gate_worst = (fw["worstEpisodesEligible"] if gate_eligible else fw["worstEpisodes"])
        gate_key = "falseWideningSecondsEligible" if gate_eligible else "falseWideningSeconds"
        label = "ELIGIBLE / auto-skip" if gate_eligible else "ALL windows"
        # Always report the OTHER metric as an early-warning (over-widening tendency).
        ew_max = fw["treatmentMaxSecondsPerEpisode"] if gate_eligible else fw["treatmentMaxSecondsPerEpisodeEligible"]
        ew_label = "all-windows" if gate_eligible else "eligible-only"
        if gate_max > args.max_fp_seconds_per_episode:
            worst = gate_worst[0] if gate_worst else None
            wid = worst["episodeId"] if worst else "?"
            print(f"\nGATE FAIL (false-widening, {label}): max {gate_max:.1f}s/ep "
                  f"({wid}) exceeds the {args.max_fp_seconds_per_episode:.1f}s/ep budget. "
                  f"Auto-skip would eat content — do NOT flip.", file=sys.stderr)
            print(f"  early-warning ({ew_label} over-widening): max {ew_max:.1f}s/ep.", file=sys.stderr)
            return 3
        print(f"\nGATE PASS (false-widening, {label}): max {gate_max:.1f}s/ep "
              f"<= {args.max_fp_seconds_per_episode:.1f}s/ep budget.", file=sys.stderr)
        print(f"  early-warning ({ew_label} over-widening): max {ew_max:.1f}s/ep "
              f"{'(also under budget)' if ew_max <= args.max_fp_seconds_per_episode else '(OVER budget — over-widening tendency to watch)'}.",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
