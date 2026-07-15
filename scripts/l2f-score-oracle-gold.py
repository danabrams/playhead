#!/usr/bin/env python3
"""Score boundary predictions against the gold oracle ear-audit evaluation.

Compares a prediction source against the earaudit-oracle-gold artifact
(playhead-l2f.10) so boundary work is measured against Dan's gold labels
automatically instead of via re-audit. Two prediction sources:

  --slots-ledger  the frozen oracle-emission ledger (JSONL; scores the
                  emitted rediff slot edges — reproduces the 2026-07-15
                  audit statistics)
  --dump          a pipeline dump JSON (scores production adWindows
                  startTime/endTime per episode)

Matching: each gold full_break is matched to the prediction with the
largest overlap in the same episode (fingerprint-checked when the source
carries fingerprints). Per-edge absolute errors are reported raw and
tolerance-adjusted (errors within the break's boundary_tolerance_seconds
score as zero). Breaks with no overlapping prediction are reported as
missed and excluded from edge statistics. Predictions overlapping a
content_veto are reported as veto hits.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys


class ScoringError(RuntimeError):
    pass


def load_evaluation(path: pathlib.Path) -> dict:
    artifact = json.load(path.open(encoding="utf-8"))
    if artifact.get("artifact_kind") != "oracle_earaudit_gold_boundary_evaluation":
        raise ScoringError(f"unexpected artifact_kind in {path}")
    return artifact


def predictions_from_slots_ledger(path: pathlib.Path) -> dict[str, list[dict]]:
    spans: dict[str, list[dict]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        spans.setdefault(entry["episode_id"], []).append(
            {
                "id": entry["id"],
                "start": float(entry["current_start_seconds"]),
                "end": float(entry["current_end_seconds"]),
                "fingerprint": entry.get("audio_fingerprint"),
            }
        )
    return spans


def predictions_from_dump(path: pathlib.Path) -> dict[str, list[dict]]:
    dump = json.load(path.open(encoding="utf-8"))
    spans: dict[str, list[dict]] = {}
    for episode in dump.get("episodes", []):
        spans[episode["episodeId"]] = [
            {
                "start": float(w["startTime"]),
                "end": float(w["endTime"]),
                "fingerprint": episode.get("audioFingerprint"),
            }
            for w in episode.get("adWindows", [])
        ]
    return spans


def overlap(a_start: float, a_end: float, b_start: float, b_end: float) -> float:
    return max(0.0, min(a_end, b_end) - max(a_start, b_start))


def percentile(values: list[float], p: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return float("nan")
    index = min(len(ordered) - 1, int(round(p / 100 * (len(ordered) - 1))))
    return ordered[index]


def score(
    evaluation: dict,
    predictions: dict[str, list[dict]],
    match: str = "overlap",
) -> dict:
    matched = []
    missed = []
    veto_hits = []
    for asset in evaluation["assets"]:
        episode_id = asset["episode_id"]
        episode_predictions = predictions.get(episode_id, [])
        for prediction in episode_predictions:
            fingerprint = prediction.get("fingerprint")
            if fingerprint and fingerprint != asset["audio_fingerprint"]:
                raise ScoringError(f"fingerprint mismatch on {episode_id}")
        for break_ in asset["full_breaks"]:
            tolerance = float(break_["boundary_tolerance_seconds"])
            best = None
            if match == "provenance":
                # Join to the exact slots the break was audited from
                # (union of their emitted spans) — reproduces the audit's
                # identity statistics; only meaningful for --slots-ledger.
                members = [
                    p
                    for p in episode_predictions
                    if p.get("id") in set(break_.get("source_ledger_ids", []))
                ]
                if members:
                    best = {
                        "start": min(p["start"] for p in members),
                        "end": max(p["end"] for p in members),
                    }
            else:
                best_overlap = 0.0
                for prediction in episode_predictions:
                    candidate = overlap(
                        break_["start_seconds"],
                        break_["end_seconds"],
                        prediction["start"],
                        prediction["end"],
                    )
                    if candidate > best_overlap:
                        best_overlap = candidate
                        best = prediction
            if best is None:
                missed.append({"episode_id": episode_id, **break_})
                continue
            start_error = abs(best["start"] - break_["start_seconds"])
            end_error = abs(best["end"] - break_["end_seconds"])
            matched.append(
                {
                    "episode_id": episode_id,
                    "show_name": asset["show_name"],
                    "start_error_raw": round(start_error, 3),
                    "end_error_raw": round(end_error, 3),
                    "start_error": round(max(0.0, start_error - tolerance), 3),
                    "end_error": round(max(0.0, end_error - tolerance), 3),
                    "within_tolerance": start_error <= tolerance
                    and end_error <= tolerance,
                }
            )
        for veto in asset["content_vetoes"]:
            for prediction in episode_predictions:
                enclosed = overlap(
                    veto["start_seconds"],
                    veto["end_seconds"],
                    prediction["start"],
                    prediction["end"],
                )
                if enclosed > 0:
                    veto_hits.append(
                        {
                            "episode_id": episode_id,
                            "enclosed_seconds": round(enclosed, 3),
                        }
                    )
    start_errors = [m["start_error"] for m in matched]
    end_errors = [m["end_error"] for m in matched]
    per_show: dict[str, dict] = {}
    for m in matched:
        per_show.setdefault(m["show_name"], {"start": [], "end": []})
        per_show[m["show_name"]]["start"].append(m["start_error_raw"])
        per_show[m["show_name"]]["end"].append(m["end_error_raw"])
    return {
        "gold_breaks": sum(len(a["full_breaks"]) for a in evaluation["assets"]),
        "matched_breaks": len(matched),
        "missed_breaks": len(missed),
        "within_tolerance": sum(1 for m in matched if m["within_tolerance"]),
        "start_error_seconds": {
            "p50": round(percentile(start_errors, 50), 1),
            "p95": round(percentile(start_errors, 95), 1),
            "mean": round(statistics.mean(start_errors), 1) if start_errors else None,
        },
        "end_error_seconds": {
            "p50": round(percentile(end_errors, 50), 1),
            "p95": round(percentile(end_errors, 95), 1),
            "mean": round(statistics.mean(end_errors), 1) if end_errors else None,
        },
        "per_show_median_raw": {
            show: {
                "start": round(statistics.median(v["start"]), 1),
                "end": round(statistics.median(v["end"]), 1),
                "breaks": len(v["start"]),
            }
            for show, v in sorted(per_show.items())
        },
        "veto_hits": veto_hits,
        "missed": missed,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evaluation", type=pathlib.Path, required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--slots-ledger", type=pathlib.Path)
    source.add_argument("--dump", type=pathlib.Path)
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--match",
        choices=["overlap", "provenance"],
        default="overlap",
        help=(
            "overlap: best-overlap per break (canonical for future runs). "
            "provenance: identity join via source_ledger_ids (reproduces "
            "the audit statistics; requires --slots-ledger)."
        ),
    )
    args = parser.parse_args(argv)
    if args.match == "provenance" and not args.slots_ledger:
        parser.error("--match provenance requires --slots-ledger")

    evaluation = load_evaluation(args.evaluation)
    if args.slots_ledger:
        predictions = predictions_from_slots_ledger(args.slots_ledger)
    else:
        predictions = predictions_from_dump(args.dump)
    report = score(evaluation, predictions, match=args.match)
    if args.json:
        print(json.dumps(report, indent=1, sort_keys=True))
        return 0
    print(
        f"gold breaks {report['gold_breaks']} | matched {report['matched_breaks']} "
        f"| missed {report['missed_breaks']} | within tolerance "
        f"{report['within_tolerance']}/{report['matched_breaks']}"
    )
    print(
        "start error (tol-adjusted): "
        f"p50 {report['start_error_seconds']['p50']}s "
        f"p95 {report['start_error_seconds']['p95']}s "
        f"mean {report['start_error_seconds']['mean']}s"
    )
    print(
        "end   error (tol-adjusted): "
        f"p50 {report['end_error_seconds']['p50']}s "
        f"p95 {report['end_error_seconds']['p95']}s "
        f"mean {report['end_error_seconds']['mean']}s"
    )
    print(f"veto hits: {len(report['veto_hits'])}")
    for show, v in report["per_show_median_raw"].items():
        print(
            f"  {show[:36]:38} n={v['breaks']:2} start {v['start']:7.1f}s "
            f"end {v['end']:7.1f}s"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
