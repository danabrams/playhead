#!/usr/bin/env python3
"""playhead-xsdz.38: sweep joint-refinement configs against gold v2.

Evaluation harness (production-shape application of the offline recipe):
for every episode of a show whose gold breaks support a learned model,
refine EVERY window — gold-matched windows with the break's leave-one-out
model (spike protocol: no break refined with evidence learned from itself),
remaining windows with the full-corpus show model (mirrors the shipped
StingerRefiner, which refines all windows of a bank show). The reference
row runs the byte-identical shipped recipe (`refine_edges`); every other
row runs `refine_edges_joint` with one term configuration.

Metrics per config:
  - gold scorer corpus stats (within-tolerance, start/end p50/p95, missed,
    veto hits) via scripts/l2f-score-oracle-gold.py;
  - morbid-05-29 opener: matched-break end error + content-eat seconds
    past the gold end (the live defect instance);
  - morbid-05-21 first window: final end time (the +43.7s snap that is
    CORRECT per Dan's ear — must stay at ~124.1);
  - nikki start/end medians (raw) — spike regression guardrail;
  - enclosed-seconds proxy: window seconds outside gold breaks summed over
    gold episodes (offline stand-in for the evidence-aware FP gate).

Writes a Markdown results table (--out) and optionally a JSON dump of all
rows (--report-json).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import pathlib
import statistics
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PROTO = _load("l2f_boundary_stinger_prototype", "l2f-boundary-stinger-prototype.py")
SCORE = PROTO.SCORE

OPENER_EPISODE = "morbid-2026-05-29-may-bonus-episode-breaking-dawn-part-1"
KEEP_EPISODE = "morbid-2026-05-21-the-matamoros-devil-murders-part-1"
KEEP_WINDOW_START = 68.08  # proposal start of the must-stay +43.7s end snap
KEEP_END_TARGET = 124.14  # offline landscape peak (production snapped 124.12)
NIKKI_SHOW = "The Nikki Glaser Podcast"


def build_configs() -> list[tuple[str, object | None]]:
    """(row name, JointConfig | None). None = shipped recipe reference.

    Default JointConfig = grid_bonus 0.5 scope 'both', product eat killer
    grid_inconsistency_rate 0.08, derived candidates gated at 0.65, no
    structural anchors, no peak margin. Rows vary one term at a time.
    """
    J = PROTO.JointConfig
    return [
        ("unrefined", "unrefined"),
        ("current-recipe", None),
        ("J-bonus-only", J(grid_inconsistency_rate=0.0)),
        ("J-pen6", J(grid_inconsistency_rate=0.0, offgrid_move_rate=0.006)),
        ("J-pen8", J(grid_inconsistency_rate=0.0, offgrid_move_rate=0.008)),
        ("J-q05", J(grid_inconsistency_rate=0.05)),
        ("J-q08", J()),
        ("J-q12", J(grid_inconsistency_rate=0.12)),
        ("J-q20", J(grid_inconsistency_rate=0.20)),
        ("J-q08-only", J(grid_bonus=0.0, derived_candidates=False)),
        ("J-q08-B25", J(grid_bonus=0.25)),
        ("J-q08-scope-any", J(grid_bonus_scope="any")),
        ("J-q08-no-derived", J(derived_candidates=False)),
        ("J-q08-derived-gate", J(derived_anchor_min_peak=PROTO.SNAP_NCC_FLOOR)),
        ("J-q08-struct", J(structural_anchors=True)),
        ("J-q08-margin02", J(peak_margin=0.02)),
        ("J-q08-margin04", J(peak_margin=0.04)),
    ]


class Harness:
    def __init__(self, evaluation: dict, dump_path: pathlib.Path, audio_dir: pathlib.Path):
        self.evaluation = evaluation
        self.audio_by_stem = {
            p.stem: p
            for p in audio_dir.iterdir()
            if p.suffix.lower() in {".mp3", ".m4a", ".aac", ".wav", ".flac", ".caf"}
        }
        self.envelopes = PROTO.EnvelopeCache(self.audio_by_stem)
        self.predictions = SCORE.predictions_from_dump(dump_path)
        dump = json.load(dump_path.open(encoding="utf-8"))
        self.durations = {
            ep["episodeId"]: float(ep["episodeDurationSeconds"])
            for ep in dump["episodes"]
        }
        for asset in evaluation["assets"]:
            self.durations.setdefault(asset["episode_id"], asset["duration_seconds"])
        self.breaks_by_show = PROTO.group_breaks_by_show(evaluation)
        self.slug_to_show = {}
        for show, show_breaks in self.breaks_by_show.items():
            for b in show_breaks:
                self.slug_to_show[PROTO.show_slug_from_episode_id(b["episode_id"])] = show
        self.gold_by_episode = {
            a["episode_id"]: a["full_breaks"] for a in evaluation["assets"]
        }
        self._model_cache: dict[tuple[str, int | None], dict] = {}

    def model(self, show: str, exclude_index: int | None) -> dict:
        # Leave-one-out folds mirror the prototype's spike protocol
        # (full_templates_only=False). The FULL-corpus model used for
        # windows without a gold match mirrors the shipped bank protocol
        # (full_templates_only=True): without the filter, a truncated
        # edge clip — e.g. the morbid-05-29 opener's 2-second pre clip —
        # wins exemplar selection and then self-matches at peak 1.0,
        # fabricating snaps production could never make.
        key = (show, exclude_index)
        if key not in self._model_cache:
            self._model_cache[key] = PROTO.learn_show_model(
                self.breaks_by_show[show],
                self.envelopes,
                exclude_index,
                full_templates_only=exclude_index is None,
            )
        return self._model_cache[key]

    def refine_all(self, config) -> tuple[dict[str, list[dict]], list[dict]]:
        """Refine every window production-shape. config=None -> shipped
        recipe (refine_edges); else refine_edges_joint(config)."""
        refined = {
            eid: [dict(p) for p in spans] for eid, spans in self.predictions.items()
        }
        touched: set[tuple[str, int]] = set()
        traces = []

        def refine(span, model, eid):
            proposal = {"start": span["start"], "end": span["end"]}
            duration = self.durations[eid]
            if config is None:
                return PROTO.refine_edges(
                    proposal, model, self.envelopes, eid, duration
                )
            return PROTO.refine_edges_joint(
                proposal, model, self.envelopes, eid, duration, config
            )

        # Pass 1 — gold-matched windows, leave-one-out models (mirrors the
        # prototype's main() loop: sequential, matching against the current
        # possibly-already-refined spans).
        for show, show_breaks in sorted(self.breaks_by_show.items()):
            for index, break_ in enumerate(show_breaks):
                eid = break_["episode_id"]
                spans = refined.get(eid, [])
                best, best_i, best_overlap = None, None, 0.0
                for i, span in enumerate(spans):
                    o = SCORE.overlap(
                        break_["start_seconds"], break_["end_seconds"],
                        span["start"], span["end"],
                    )
                    if o > best_overlap:
                        best, best_i, best_overlap = span, i, o
                if best is None:
                    continue
                model = self.model(show, index)
                new_start, new_end, trace = refine(best, model, eid)
                traces.append({
                    "episode_id": eid, "window": best_i,
                    "pass": "gold-matched",
                    "before": [round(best["start"], 2), round(best["end"], 2)],
                    "after": [round(new_start, 2), round(new_end, 2)],
                    **trace,
                })
                best["start"], best["end"] = new_start, new_end
                touched.add((eid, best_i))

        # Pass 2 — every untouched window on episodes of model shows, full
        # show model (production refines all windows of a bank show).
        for eid, spans in sorted(refined.items()):
            show = self.slug_to_show.get(PROTO.show_slug_from_episode_id(eid))
            if show is None:
                continue
            model = self.model(show, None)
            for i, span in enumerate(spans):
                if (eid, i) in touched:
                    continue
                new_start, new_end, trace = refine(span, model, eid)
                traces.append({
                    "episode_id": eid, "window": i,
                    "pass": "unmatched",
                    "before": [round(span["start"], 2), round(span["end"], 2)],
                    "after": [round(new_start, 2), round(new_end, 2)],
                    **trace,
                })
                span["start"], span["end"] = new_start, new_end
        return refined, traces

    # ------------------------------------------------------------------
    # Metrics

    def metrics(self, refined: dict[str, list[dict]]) -> dict:
        report = SCORE.score(self.evaluation, refined)
        opener = next(
            b for b in self.gold_by_episode[OPENER_EPISODE]
            if b["start_seconds"] == 0.0
        )
        opener_windows = refined.get(OPENER_EPISODE, [])
        best, best_overlap = None, 0.0
        for span in opener_windows:
            o = SCORE.overlap(
                opener["start_seconds"], opener["end_seconds"],
                span["start"], span["end"],
            )
            if o > best_overlap:
                best, best_overlap = span, o
        opener_end_err = (
            abs(best["end"] - opener["end_seconds"]) if best else None
        )
        opener_eat = sum(
            max(0.0, span["end"] - opener["end_seconds"])
            for span in opener_windows
            if SCORE.overlap(
                opener["start_seconds"], opener["end_seconds"],
                span["start"], span["end"],
            ) > 0
        )
        keep = next(
            (
                span for span in refined.get(KEEP_EPISODE, [])
                if abs(span["start"] - KEEP_WINDOW_START) < 20.0
                or span["start"] <= KEEP_WINDOW_START <= span["end"]
            ),
            None,
        )
        keep_end = keep["end"] if keep else None
        nikki = report["per_show_median_raw"].get(NIKKI_SHOW, {})
        proxy_total, proxy_max = 0.0, ("", 0.0)
        for asset in self.evaluation["assets"]:
            eid = asset["episode_id"]
            outside = 0.0
            for span in refined.get(eid, []):
                inside = sum(
                    SCORE.overlap(
                        b["start_seconds"], b["end_seconds"],
                        span["start"], span["end"],
                    )
                    for b in asset["full_breaks"]
                )
                outside += (span["end"] - span["start"]) - inside
            proxy_total += outside
            if outside > proxy_max[1]:
                proxy_max = (eid, outside)
        per_break = []
        for asset in self.evaluation["assets"]:
            for b in asset["full_breaks"]:
                best_span, best_o = None, 0.0
                for span in refined.get(asset["episode_id"], []):
                    o = SCORE.overlap(
                        b["start_seconds"], b["end_seconds"],
                        span["start"], span["end"],
                    )
                    if o > best_o:
                        best_span, best_o = span, o
                per_break.append({
                    "episode_id": asset["episode_id"],
                    "break": [b["start_seconds"], b["end_seconds"]],
                    "start_err": (
                        round(abs(best_span["start"] - b["start_seconds"]), 2)
                        if best_span else None
                    ),
                    "end_err": (
                        round(abs(best_span["end"] - b["end_seconds"]), 2)
                        if best_span else None
                    ),
                })
        return {
            "per_break": per_break,
            "within_tolerance": report["within_tolerance"],
            "matched": report["matched_breaks"],
            "missed": report["missed_breaks"],
            "veto_hits": len(report["veto_hits"]),
            "start_p50": report["start_error_seconds"]["p50"],
            "start_p95": report["start_error_seconds"]["p95"],
            "end_p50": report["end_error_seconds"]["p50"],
            "end_p95": report["end_error_seconds"]["p95"],
            "opener_end_err": round(opener_end_err, 2) if opener_end_err is not None else None,
            "opener_eat_s": round(opener_eat, 2),
            "keep_0521_end": round(keep_end, 2) if keep_end is not None else None,
            "keep_0521_ok": (
                keep_end is not None and abs(keep_end - KEEP_END_TARGET) <= 2.0
            ),
            "nikki_start_med": nikki.get("start"),
            "nikki_end_med": nikki.get("end"),
            "proxy_outside_gold_s": round(proxy_total, 1),
            "proxy_max_ep_s": round(proxy_max[1], 1),
            "proxy_max_ep": proxy_max[0],
        }


def render_table(rows: list[tuple[str, dict]], verdict: str) -> str:
    header = (
        "| config | within tol | start p50/p95 | end p50/p95 | missed | veto "
        "| opener end err | opener eat | 05-21 end (keep~124.1) | nikki start/end med "
        "| outside-gold total | outside-gold max ep |\n"
        "|---|---|---|---|---|---|---|---|---|---|---|---|\n"
    )
    lines = []
    for name, m in rows:
        keep = f"{m['keep_0521_end']}" + (" OK" if m["keep_0521_ok"] else " REGRESSED")
        lines.append(
            f"| {name} | {m['within_tolerance']}/{m['matched']} "
            f"| {m['start_p50']}/{m['start_p95']} | {m['end_p50']}/{m['end_p95']} "
            f"| {m['missed']} | {m['veto_hits']} "
            f"| {m['opener_end_err']} | {m['opener_eat_s']} | {keep} "
            f"| {m['nikki_start_med']}/{m['nikki_end_med']} "
            f"| {m['proxy_outside_gold_s']} | {m['proxy_max_ep_s']} ({m['proxy_max_ep'][:24]}) |"
        )
    return header + "\n".join(lines) + "\n\n" + verdict + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evaluation", type=pathlib.Path, required=True)
    parser.add_argument("--dump", type=pathlib.Path, required=True)
    parser.add_argument(
        "--audio-dir", type=pathlib.Path, default=ROOT / "TestFixtures/Corpus/Audio"
    )
    parser.add_argument("--out", type=pathlib.Path)
    parser.add_argument("--report-json", type=pathlib.Path)
    parser.add_argument(
        "--only", action="append", default=[],
        help="run only the named config rows (repeatable)",
    )
    args = parser.parse_args(argv)

    evaluation = SCORE.load_evaluation(args.evaluation)
    harness = Harness(evaluation, args.dump, args.audio_dir)

    rows = []
    trace_store = {}
    for name, config in build_configs():
        if args.only and name not in args.only:
            continue
        if config == "unrefined":
            refined, traces = {
                eid: [dict(p) for p in spans]
                for eid, spans in harness.predictions.items()
            }, []
        else:
            refined, traces = harness.refine_all(config)
        m = harness.metrics(refined)
        rows.append((name, m))
        trace_store[name] = traces
        print(
            f"{name:16} tol {m['within_tolerance']:2}/{m['matched']} | "
            f"start {m['start_p50']}/{m['start_p95']} end {m['end_p50']}/{m['end_p95']} | "
            f"opener err {m['opener_end_err']} eat {m['opener_eat_s']} | "
            f"0521 end {m['keep_0521_end']} {'OK' if m['keep_0521_ok'] else 'REGRESSED'} | "
            f"nikki {m['nikki_start_med']}/{m['nikki_end_med']} | "
            f"outside {m['proxy_outside_gold_s']} (max {m['proxy_max_ep_s']})"
        )

    if args.out:
        args.out.write_text(render_table(rows, ""), encoding="utf-8")
    if args.report_json:
        args.report_json.write_text(
            json.dumps(
                {"rows": dict(rows), "traces": trace_store},
                indent=1, sort_keys=True, default=str,
            ),
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
