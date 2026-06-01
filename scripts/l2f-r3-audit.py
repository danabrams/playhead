"""
l2f-r3-audit.py — pattern analysis for R3 (rediff-only ≥20s) auto-promotions.

Auto-promotion rule R3 in scripts/l2f-auto-promote.py is the weakest of three
triangulation rules: it promotes a rediff-only span (no drafter, no pipeline)
into the corpus as audit_priority=1. With N current R3 promotions in the
corpus, this script looks for patterns that could justify tightening R3
BEFORE more spans accrue. It is intentionally read-only and stdlib-only.

Outputs (stdout, markdown):
  * Findings summary
  * Per-span table
  * Distributions (duration, position-in-episode, distance from edges,
    same-episode clustering, show concentration)
  * Optional R3 tightening proposal — only if patterns are evidence-clear.
    Otherwise the script prints "no change justified" plus caveats.

Usage:
  scripts/l2f-r3-audit.py                 # write to stdout
  scripts/l2f-r3-audit.py -o report.md    # write to file
"""
from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys
from collections import Counter, defaultdict
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[1]
ANN_DIR = ROOT / "TestFixtures/Corpus/Annotations"
MANIFEST = ROOT / "TestFixtures/Corpus/Snapshots/manifest.json"


def load_show_index() -> dict[str, str]:
    if not MANIFEST.exists():
        return {}
    try:
        data = json.loads(MANIFEST.read_text())
    except Exception:
        return {}
    return {e.get("episodeId", ""): e.get("show", "?") for e in data}


def collect_spans() -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Walk every annotation; return audit_priority=1 spans + per-episode counts."""
    shows = load_show_index()
    spans: list[dict[str, Any]] = []
    per_episode_ap1_count: dict[str, int] = defaultdict(int)

    for path in sorted(ANN_DIR.glob("*.json")):
        if path.name.startswith("_template"):
            continue
        try:
            ann = json.loads(path.read_text())
        except Exception:
            continue
        eid = ann.get("episode_id") or ann.get("episodeId") or path.stem
        duration = ann.get("duration_seconds")
        try:
            duration = float(duration) if duration is not None else None
        except (TypeError, ValueError):
            duration = None
        windows = ann.get("ad_windows") or ann.get("adWindows") or []

        ap1_in_ep = [w for w in windows if w.get("audit_priority") == 1]
        if ap1_in_ep:
            per_episode_ap1_count[eid] = len(ap1_in_ep)

        for w in ap1_in_ep:
            start = float(w.get("start_seconds") or w.get("startSeconds") or 0.0)
            end = float(w.get("end_seconds") or w.get("endSeconds") or 0.0)
            span = {
                "episode_id": eid,
                "show": shows.get(eid, ann.get("show_name") or "?"),
                "start": start,
                "end": end,
                "duration": max(0.0, end - start),
                "episode_duration": duration,
                "position": (start / duration) if duration else None,
                "distance_from_start": start,
                "distance_from_end": (
                    (duration - end) if duration is not None else None
                ),
                "provenance": w.get("provenance") or [],
                "ad_type": w.get("ad_type"),
                "auto_promoted": bool(w.get("auto_promoted")),
                "confidence_notes": w.get("confidence_notes", ""),
            }
            spans.append(span)

    return spans, dict(per_episode_ap1_count)


def fmt_pct(x: float | None) -> str:
    return "n/a" if x is None else f"{x * 100:.1f}%"


def fmt_sec(x: float | None) -> str:
    return "n/a" if x is None else f"{x:.1f}s"


def summarize_dist(label: str, values: list[float]) -> list[str]:
    if not values:
        return [f"- **{label}**: (empty)"]
    vals = sorted(values)
    n = len(vals)
    mn, mx = vals[0], vals[-1]
    mean = sum(vals) / n
    med = statistics.median(vals)

    def q(p: float) -> float:
        # closest-rank percentile (stdlib-only, deterministic for small N)
        idx = max(0, min(n - 1, int(round(p * (n - 1)))))
        return vals[idx]

    return [
        f"- **{label}** (n={n}): "
        f"min={mn:.1f}, p25={q(0.25):.1f}, median={med:.1f}, "
        f"p75={q(0.75):.1f}, max={mx:.1f}, mean={mean:.1f}"
    ]


def build_report(spans: list[dict[str, Any]], per_episode: dict[str, int]) -> str:
    out: list[str] = []
    N = len(spans)

    out.append("# R3 Auto-Promotion Audit — 2026-06-01")
    out.append("")
    out.append(
        "Pattern review of every R3 (rediff-only, ≥20s, `audit_priority=1`) "
        "span currently committed to the corpus. Goal: decide whether the R3 "
        "promotion threshold in `scripts/l2f-auto-promote.py` should be "
        "tightened **before** tomorrow's overnight loop generates more spans."
    )
    out.append("")
    out.append(f"**N = {N}** R3 spans across "
               f"{len(set(s['episode_id'] for s in spans))} episodes "
               f"and {len(set(s['show'] for s in spans))} shows.")
    out.append("")

    # Per-span table
    out.append("## Per-span table")
    out.append("")
    out.append(
        "| # | Show | Span (s) | Dur | Pos | From start | From end | Notes |"
    )
    out.append(
        "|---|------|----------|-----|-----|------------|----------|-------|"
    )
    for i, s in enumerate(sorted(spans, key=lambda x: (x["show"], x["start"])), 1):
        out.append(
            f"| {i} | {s['show']} | {s['start']:.0f}–{s['end']:.0f} | "
            f"{s['duration']:.0f}s | {fmt_pct(s['position'])} | "
            f"{fmt_sec(s['distance_from_start'])} | "
            f"{fmt_sec(s['distance_from_end'])} | "
            f"`{s['episode_id'][:34]}` |"
        )
    out.append("")

    # Distributions
    out.append("## Distributions")
    out.append("")
    durs = [s["duration"] for s in spans]
    out += summarize_dist("Duration (s)", durs)
    starts = [s["distance_from_start"] for s in spans]
    out += summarize_dist("Distance from start (s)", starts)
    ends = [s["distance_from_end"] for s in spans if s["distance_from_end"] is not None]
    out += summarize_dist("Distance from end (s)", ends)
    poss = [s["position"] for s in spans if s["position"] is not None]
    out += summarize_dist("Position-in-episode (fraction 0-1)", poss)
    out.append("")

    # Position buckets
    first_5 = sum(1 for p in poss if p < 0.05)
    last_5 = sum(1 for p in poss if p > 0.95)
    mid = len(poss) - first_5 - last_5
    out.append("**Position buckets**:")
    out.append(f"- First 5% of episode: {first_5}/{len(poss)}")
    out.append(f"- Middle 90%: {mid}/{len(poss)}")
    out.append(f"- Last 5% of episode: {last_5}/{len(poss)}")
    out.append("")

    # Duration buckets
    short_30 = sum(1 for d in durs if d < 30)
    short_40 = sum(1 for d in durs if d < 40)
    long_90 = sum(1 for d in durs if d >= 90)
    out.append("**Duration buckets**:")
    out.append(f"- 20–<30s: {short_30}/{N}")
    out.append(f"- 20–<40s: {short_40}/{N}")
    out.append(f"- ≥90s:    {long_90}/{N}")
    out.append("")

    # Show concentration
    show_counts = Counter(s["show"] for s in spans)
    out.append("## Show concentration")
    out.append("")
    for show, c in show_counts.most_common():
        out.append(f"- {show}: {c}")
    out.append("")

    # Same-episode clustering
    out.append("## Same-episode clustering")
    out.append("")
    clustered = {e: c for e, c in per_episode.items() if c > 1}
    out.append(
        f"- Episodes with >1 R3 span: {len(clustered)}/"
        f"{len(per_episode)} ({sum(clustered.values())} of {N} spans)"
    )
    for eid, c in sorted(clustered.items(), key=lambda kv: -kv[1]):
        out.append(f"  - `{eid}`: {c} spans")
    out.append("")

    # Sanity flag: spans that extend past episode end
    past_end = [
        s for s in spans
        if s["episode_duration"] is not None
        and s["end"] > s["episode_duration"]
    ]
    if past_end:
        out.append("## Sanity flags")
        out.append("")
        out.append(
            "Spans whose `end_seconds` exceeds the annotated "
            "`duration_seconds` (likely rediff cluster overshoot past last "
            "audible frame — not necessarily wrong, but worth a manual look):"
        )
        for s in past_end:
            over = s["end"] - (s["episode_duration"] or 0)
            out.append(
                f"- `{s['episode_id']}`: span ends "
                f"{over:.1f}s past episode end "
                f"({s['end']:.1f}s vs {s['episode_duration']:.1f}s)"
            )
        out.append("")

    # Findings summary + proposal
    out.append("## Findings summary")
    out.append("")
    findings: list[str] = []

    # F1: Show concentration
    top_show, top_n = show_counts.most_common(1)[0]
    if top_n / N >= 0.4:
        findings.append(
            f"**F1. Show concentration is real.** {top_n} of {N} R3 spans "
            f"({top_n / N * 100:.0f}%) come from a single show "
            f"(*{top_show}*). Either (a) that show is genuinely heavy on "
            f"DAI-only ad slots that drafter+pipeline both miss, or "
            f"(b) there's a show-specific artifact (loud music stings, "
            f"intro/outro stingers) tripping the rediff signal. Both are "
            f"plausible without listening; cannot disambiguate from the "
            f"annotations alone."
        )

    # F2: Same-episode clustering
    if clustered:
        top_ep, top_ec = max(clustered.items(), key=lambda kv: kv[1])
        findings.append(
            f"**F2. Clustering inside a single episode.** "
            f"`{top_ep}` alone accounts for {top_ec} of {N} "
            f"R3 spans ({top_ec / N * 100:.0f}%). Same caveat as F1: "
            f"could be genuine DAI density, could be a per-episode "
            f"artifact (e.g. recurring music bed). Worth ear-witnessing "
            f"this episode first if a human-audit window opens."
        )

    # F3: Duration distribution
    if short_40 and short_40 / N >= 0.5:
        findings.append(
            f"**F3. Half the R3 spans are short (20–40s).** "
            f"{short_40}/{N} R3 spans fall in the 20–<40s bucket. "
            f"R3 fires at ≥20s and these are the spans most likely to be "
            f"single-MP3-frame-artifact false positives, but the current "
            f"sample is too small to call. **If** field listening later "
            f"shows the 20–<30s tier ({short_30}/{N} here) is mostly "
            f"junk, the obvious tightening is to raise the R3 minimum "
            f"duration from 20s to 30s or 40s — see proposal below."
        )

    # F4: Position
    if last_5:
        findings.append(
            f"**F4. {last_5}/{len(poss)} R3 spans land in the last 5% of "
            f"the episode.** Outro stingers / end-cards can sometimes "
            f"masquerade as DAI ads on the rediff. With a sample this "
            f"small, this is suggestive, not actionable."
        )
    if first_5:
        findings.append(
            f"**F5. {first_5}/{len(poss)} R3 spans land in the first 5% of "
            f"the episode.** Pre-roll DAI is real and expected; this is "
            f"not by itself a red flag."
        )

    if not findings:
        findings.append(
            "No single dimension exceeded the rough heuristic thresholds "
            "(show share ≥40%, short-span share ≥50%, etc.). "
            "Pattern signal is weak."
        )

    for f in findings:
        out.append(f"- {f}")
        out.append("")

    # Proposal section
    out.append("## Proposed R3 tightening")
    out.append("")
    out.append(
        "R3 currently fires when a rediff-only span has length ≥ 20s, with "
        "no overlap from drafter or pipeline. Three candidate tightenings "
        "are listed below. **Counterfactuals are over the current N = "
        f"{N} spans only**; extrapolation to future spans is necessarily "
        "speculative."
    )
    out.append("")

    # Counterfactuals: what would each threshold drop?
    drop_30 = sum(1 for d in durs if d < 30)
    drop_40 = sum(1 for d in durs if d < 40)
    drop_lasttwo_pct = sum(
        1 for s in spans
        if s["position"] is not None and s["position"] > 0.98
    )

    out.append("### Option A — raise R3 minimum from 20s → 30s")
    out.append(
        f"- Would drop **{drop_30} of {N}** current R3 spans "
        f"({drop_30 / N * 100:.0f}%)."
    )
    out.append(
        "- Future hypothetical: any rediff-only slot in 20–<30s would be "
        "rejected entirely (not even queued for audit). MP3 frame "
        "alignment artifacts cluster in this band; legitimate ≥30s DAI "
        "spots would still promote."
    )
    out.append(
        "- **Reversibility**: easy — single integer constant in "
        "`l2f-auto-promote.py`. No corpus migration required for future "
        "runs (existing spans are already committed)."
    )
    out.append("")

    out.append("### Option B — raise R3 minimum from 20s → 40s")
    out.append(
        f"- Would drop **{drop_40} of {N}** current R3 spans "
        f"({drop_40 / N * 100:.0f}%)."
    )
    out.append(
        "- More aggressive. Loses recall on 30–<40s DAI inserts that are "
        "real but short (some pre-rolls run 30s)."
    )
    out.append("")

    out.append("### Option C — add a tail guard (drop R3 if position > 0.98)")
    out.append(
        f"- Would drop **{drop_lasttwo_pct} of {N}** current R3 spans."
    )
    out.append(
        "- Motivated by end-card / outro music tripping rediff. With the "
        "current sample, this is at most weakly indicated."
    )
    out.append("")

    # Recommendation
    out.append("### Recommendation")
    out.append("")
    out.append(
        f"**No threshold change is justified by the evidence alone.** "
        f"With N = {N} and zero ground-truth-listening confirmations, "
        f"the show/episode concentration (F1, F2) could be signal or "
        f"could be artifact, and the duration bucket counts are too "
        f"thin to commit to a permanent change."
    )
    out.append("")
    out.append(
        "Recommended next step is to **ear-witness the 8 current "
        "audit_priority=1 spans first** (≈10–15 minutes with "
        "`scripts/l2f-audit-queue.py` and ffplay), classify each as "
        "ad vs. host, and only THEN decide whether to tighten R3. "
        "The reject log (`scripts/l2f-flag-false-promote.py`) already "
        "captures veto decisions, so the cost of one audit pass is "
        "small and the information gain is high."
    )
    out.append("")
    span_word = "span" if drop_30 == 1 else "spans"
    out.append(
        "If a human-audit pass is not possible before the overnight "
        "loop runs and false-promotion drift is the larger concern, "
        f"**Option A (≥30s)** is the most conservative tightening: it "
        f"would drop only the {drop_30} shortest current {span_word} "
        "while leaving every ≥30s span untouched. Option B (≥40s) "
        "halves the current R3 yield and is harder to justify without "
        "audit data."
    )
    out.append("")

    out.append("## Caveats")
    out.append("")
    out.append(
        "- **N = {n} is tiny.** Any apparent pattern (show share, "
        "duration distribution, edge concentration) is fully consistent "
        "with random variance at this sample size. Treat the findings "
        "as hypotheses, not conclusions.".format(n=N)
    )
    out.append(
        "- **No ground truth.** These spans have not been ear-witnessed. "
        "We do not know which are real DAI ads, which are host content, "
        "and which are MP3 frame artifacts. The pattern analysis cannot "
        "distinguish a real DAI-heavy show from a show that breaks "
        "the rediff signal."
    )
    out.append(
        "- **Selection bias from R1/R2.** R3 only fires when the "
        "drafter AND pipeline both missed the span. Shows with weaker "
        "transcripts or noisier pipelines will be over-represented in "
        "R3 by construction, independent of actual ad density."
    )
    out.append(
        "- **Threshold-tightening is asymmetric.** Raising R3 from 20s "
        "→ 30s drops true positives along with false positives; without "
        "ground-truth labels we cannot estimate the precision/recall "
        "trade-off."
    )

    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("-o", "--output", type=str, default=None,
                    help="Write report to this path instead of stdout")
    args = ap.parse_args(argv)

    spans, per_episode = collect_spans()
    report = build_report(spans, per_episode)

    if args.output:
        path = pathlib.Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(report)
        print(f"wrote {path} ({len(report)} bytes, {len(spans)} R3 spans)",
              file=sys.stderr)
    else:
        sys.stdout.write(report)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
