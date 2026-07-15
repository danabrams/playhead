#!/usr/bin/env python3
"""Offline stinger-anchored boundary refinement prototype (playhead-l2f.6).

Tests the 2026-07-15 recipe against the gold oracle ear-audit labels WITHOUT
touching production code: learn per-show stinger anchors + edge offsets +
pod-width grids from the gold breaks themselves (leave-one-out per break, so
no break is refined with evidence learned from itself), refine the baseline
production window edges, and score before/after with the l2f.10 gold scorer.

Matching engine: PCM envelope normalized cross-correlation (50 Hz log-RMS
envelopes via ffmpeg). Chromaprint raw frames (v1 of this prototype) proved
phase-fragile across different files at 8 fps; envelope NCC is sample-derived
and resolves to 20 ms, comfortably under the gold +-0.3s tolerance.

Recipe being measured (bead notes, 2026-07-15):
- per-show stingers: the same music brackets every break (morbid, TED/iHeart,
  nikki short stinger) — learned as high-NCC alignments between edge-adjacent
  clips across a show's OTHER breaks;
- per-show edge offsets: SYSK's music straddles the splice by ~1s — learned
  as the median residual between mapped and actual gold edges;
- pod-width grid: morbid/nikki pods sit on a 30s grid — learned when a show's
  other-break widths cluster on multiples of 30.

--emit-bank (playhead-l2f.6 production wire-in): build FULL-CORPUS (NOT
leave-one-out) per-show models from the gold evaluation artifact + the
retained corpus audio, and write the production `StingerBank` JSON that
ships bundled under `Playhead/Resources`. Emit-mode envelopes are extracted
at 16 kHz (`BANK_SAMPLE_RATE`) so the templates match the runtime envelopes
the app computes from its persisted 16 kHz analysis shards; the default
leave-one-out evaluation path keeps the original 8 kHz extraction so its
published spike numbers stay reproducible.

Join-key contract (documented decision, playhead-l2f.6): production
identifies a show inside `AdDetectionService.runBackfill` by the opaque
`podcastId` string it receives — the RSS feed URL in the shipping app, and
the corpus `showSlug` on the Catalyst pipeline-dump measurement path (the
dump harness passes `entry.showSlug` as `podcastId`). Neither side computes
the other, so each bank entry carries a `showKeys` alias list containing
BOTH forms — the corpus slug (derived from the gold `episode_id` stem) and
the show's feed URL(s) (from the corpus snapshots manifest) — and the
runtime resolves an entry by exact string match of `podcastId` against any
alias.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import pathlib
import re
import statistics
import subprocess
import sys

import numpy as np
from scipy.signal import correlate

ROOT = pathlib.Path(__file__).resolve().parents[1]
ENVELOPE_HZ = 50
SAMPLE_RATE = 8000
HOP = SAMPLE_RATE // ENVELOPE_HZ
# --emit-bank only: the app computes runtime envelopes from its persisted
# 16 kHz mono analysis shards, so bank templates must be learned from
# 16 kHz-decoded envelopes for NCC parity. Frame rate stays ENVELOPE_HZ.
BANK_SAMPLE_RATE = 16000
BANK_SCHEMA_VERSION = 1
DEFAULT_SNAPSHOTS_MANIFEST = ROOT / "TestFixtures/Corpus/Snapshots/manifest.json"
PRE_CLIP = (12.0, 2.0)  # seconds before/after the gold START edge
POST_CLIP = (2.0, 12.0)  # seconds before/after the gold END edge
LEARN_NCC_MIN = 0.50  # template-vs-clip alignment confidence during learning
SNAP_NCC_FLOOR = 0.50  # absolute floor for refine-time matches
SNAP_NCC_MARGIN = 0.15  # accept matches within this of the learning confidence
MAX_EDGE_MOVE_SECONDS = 75.0  # refuse snaps that move an edge farther than this
TEMPLATE_INNER = 6.0  # template seconds on the stinger side of the edge
TEMPLATE_OUTER = 1.0  # template seconds past the edge (SYSK straddle)
OFFSET_SPREAD_MAX = 2.0  # learned offsets must agree within this many seconds
SEARCH_RADIUS_SECONDS = 90.0
GRID_SECONDS = 30.0
GRID_MIN_PEAK = 0.65  # recipe v2: grid rides only on a confident anchor snap
GRID_SNAP_TOLERANCE = 3.0
GRID_MIN_FRACTION = 0.6


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SCORE = _load("l2f_score_oracle_gold", "l2f-score-oracle-gold.py")


class EnvelopeCache:
    def __init__(
        self,
        audio_by_episode: dict[str, pathlib.Path],
        sample_rate: int = SAMPLE_RATE,
    ):
        self.audio_by_episode = audio_by_episode
        self.sample_rate = sample_rate
        self.hop = sample_rate // ENVELOPE_HZ
        self._cache: dict[tuple[str, float, float], np.ndarray] = {}

    def get(self, episode_id: str, start_s: float, end_s: float) -> np.ndarray:
        start_s = max(0.0, start_s)
        key = (episode_id, round(start_s, 2), round(end_s, 2))
        if key not in self._cache:
            self._cache[key] = self._extract(episode_id, start_s, end_s)
        return self._cache[key]

    def _extract(self, episode_id: str, start_s: float, end_s: float) -> np.ndarray:
        duration = max(0.1, end_s - start_s)
        result = subprocess.run(
            [
                "ffmpeg", "-v", "error",
                "-ss", f"{start_s:.3f}", "-t", f"{duration:.3f}",
                "-i", str(self.audio_by_episode[episode_id]),
                "-ac", "1", "-ar", str(self.sample_rate), "-f", "f32le", "-",
            ],
            capture_output=True,
            check=True,
        )
        pcm = np.frombuffer(result.stdout, dtype=np.float32)
        if pcm.size < self.hop:
            return np.zeros(1, dtype=np.float64)
        frames = (
            pcm[: pcm.size - pcm.size % self.hop]
            .reshape(-1, self.hop)
            .astype(np.float64)
        )
        rms = np.sqrt((frames**2).mean(axis=1))
        return np.log1p(rms * 100.0)


def ncc_curve(template: np.ndarray, target: np.ndarray) -> np.ndarray | None:
    """Normalized cross-correlation of template at every valid target offset."""
    n = template.size
    if n < ENVELOPE_HZ or target.size < n:
        return None
    t = template - template.mean()
    t_norm = np.linalg.norm(t)
    if t_norm == 0:
        return None
    x = target
    ones = np.ones(n)
    sums = correlate(x, ones, mode="valid")
    sq_sums = correlate(x**2, ones, mode="valid")
    local_var = np.maximum(sq_sums - sums**2 / n, 1e-12)
    raw = correlate(x, t, mode="valid")
    return raw / (np.sqrt(local_var) * t_norm)


def ncc_align(template: np.ndarray, target: np.ndarray) -> tuple[int, float]:
    """Strongest alignment of template inside target: (offset, peak)."""
    ncc = ncc_curve(template, target)
    if ncc is None:
        return -1, 0.0
    offset = int(np.argmax(ncc))
    return offset, float(ncc[offset])


def ncc_nearest(
    template: np.ndarray,
    target: np.ndarray,
    preferred_offset: int,
    gate: float,
) -> tuple[int, float]:
    """Qualifying peak nearest to preferred_offset (repeated stingers from
    neighboring breaks can outscore the local one; proximity breaks the tie)."""
    ncc = ncc_curve(template, target)
    if ncc is None:
        return -1, 0.0
    peak_max = float(ncc.max())
    if peak_max < gate:
        return -1, peak_max
    # Strongest wins; proximity only tiebreaks near-equal repeats (identical
    # stinger audio at a neighboring break scores within a hair of the max).
    qualifying = np.flatnonzero(ncc >= max(gate, peak_max - 0.08))
    offset = int(qualifying[np.argmin(np.abs(qualifying - preferred_offset))])
    return offset, float(ncc[offset])


def learn_show_model(
    show_breaks: list[dict],
    envelopes: EnvelopeCache,
    exclude_index: int | None,
    full_templates_only: bool = False,
) -> dict:
    """Learn a per-show model from `show_breaks`.

    `exclude_index` selects the evaluation regime: an int runs the original
    leave-one-out spike protocol (no break refined with evidence learned
    from itself); `None` runs the FULL-CORPUS protocol used by --emit-bank
    (every gold break contributes to the shipped production model).

    `full_templates_only` (emit-bank only; default False keeps the spike
    protocol byte-identical): drop clips whose edge template was truncated
    by the episode boundary (e.g. a break starting at 0.0s) from the
    stinger-learning set. A truncated 1-second template wins exemplar
    selection with cheap short-template NCC scores and then poisons the
    learned-offset spread with misaligned residuals; the truncated clip's
    short envelope also scores an unmatchable 0.0 against every full-width
    candidate, dragging their medians down. Grid learning still uses every
    break's width.
    """
    others = [
        b
        for i, b in enumerate(show_breaks)
        if exclude_index is None or i != exclude_index
    ]
    model: dict = {"pre": None, "post": None, "grid": None}
    if len(others) >= 2:
        full_template_samples = int(
            (TEMPLATE_INNER + TEMPLATE_OUTER) * ENVELOPE_HZ
        )
        for side, (before_s, after_s), edge_key in (
            ("pre", PRE_CLIP, "start_seconds"),
            ("post", POST_CLIP, "end_seconds"),
        ):
            clips = []
            for b in others:
                edge = b[edge_key]
                clip_start = max(0.0, edge - before_s)
                env = envelopes.get(b["episode_id"], clip_start, edge + after_s)
                edge_sample = int(round((edge - clip_start) * ENVELOPE_HZ))
                clips.append({"break": b, "env": env, "edge_sample": edge_sample})
            # The stinger hugs the edge; a full-clip template dilutes it
            # with uncorrelated ad/content audio. Use a short edge-hugging
            # subsegment as the template, matched into the others' full clips.
            for c in clips:
                if side == "pre":
                    lo = c["edge_sample"] - int(TEMPLATE_INNER * ENVELOPE_HZ)
                    hi = c["edge_sample"] + int(TEMPLATE_OUTER * ENVELOPE_HZ)
                else:
                    lo = c["edge_sample"] - int(TEMPLATE_OUTER * ENVELOPE_HZ)
                    hi = c["edge_sample"] + int(TEMPLATE_INNER * ENVELOPE_HZ)
                lo = max(0, lo)
                c["template"] = c["env"][lo:hi]
                c["template_edge"] = c["edge_sample"] - lo
            if full_templates_only:
                clips = [
                    c for c in clips if c["template"].size == full_template_samples
                ]
                if len(clips) < 3:
                    # Exemplar + >= 2 offset contributors are required below;
                    # fewer full-width clips cannot produce a shippable side.
                    continue
            best_exemplar, best_score = None, 0.0
            for i, candidate in enumerate(clips):
                peaks = []
                for j, other in enumerate(clips):
                    if i == j:
                        continue
                    _, peak = ncc_align(candidate["template"], other["env"])
                    peaks.append(peak)
                score = statistics.median(peaks) if peaks else 0.0
                if score > best_score:
                    best_exemplar, best_score = candidate, score
            if best_exemplar is None or best_score < LEARN_NCC_MIN:
                continue
            offsets = []
            for other in clips:
                if other is best_exemplar:
                    continue
                offset, peak = ncc_align(best_exemplar["template"], other["env"])
                if offset < 0 or peak < LEARN_NCC_MIN:
                    continue
                mapped_edge = offset + best_exemplar["template_edge"]
                residual = (other["edge_sample"] - mapped_edge) / ENVELOPE_HZ
                offsets.append(residual)
            if len(offsets) >= 2 and max(offsets) - min(offsets) <= OFFSET_SPREAD_MAX:
                model[side] = {
                    "template": best_exemplar["template"],
                    "edge_sample": best_exemplar["template_edge"],
                    "offset": statistics.median(offsets),
                    "support": len(offsets) + 1,
                    "spread": round(max(offsets) - min(offsets), 2),
                    "confidence": round(best_score, 3),
                }
    if len(others) >= 2:
        widths = [b["end_seconds"] - b["start_seconds"] for b in others]
        on_grid = [
            w
            for w in widths
            if abs(w - GRID_SECONDS * round(w / GRID_SECONDS)) <= GRID_SNAP_TOLERANCE
            and round(w / GRID_SECONDS) >= 1
        ]
        if len(on_grid) / len(widths) >= GRID_MIN_FRACTION:
            model["grid"] = GRID_SECONDS
    return model


def refine_edges(
    proposal: dict,
    model: dict,
    envelopes: EnvelopeCache,
    episode_id: str,
    duration: float,
) -> tuple[float, float, dict]:
    new_start, new_end = proposal["start"], proposal["end"]
    trace = {"start_snapped": False, "end_snapped": False, "grid_applied": False}
    for side, key in (("pre", "start"), ("post", "end")):
        entry = model.get(side)
        if not entry:
            continue
        center = proposal[key]
        span_start = max(0.0, center - SEARCH_RADIUS_SECONDS)
        span_end = min(duration, center + SEARCH_RADIUS_SECONDS)
        window = envelopes.get(episode_id, span_start, span_end)
        gate = max(SNAP_NCC_FLOOR, entry["confidence"] - SNAP_NCC_MARGIN)
        offset, peak = ncc_align(entry["template"], window)
        if offset < 0 or peak < gate:
            continue
        snapped = (
            span_start
            + (offset + entry["edge_sample"]) / ENVELOPE_HZ
            + entry["offset"]
        )
        if abs(snapped - center) > MAX_EDGE_MOVE_SECONDS:
            continue
        if key == "start":
            new_start = snapped
            trace["start_snapped"] = True
            trace["start_peak"] = round(peak, 3)
        else:
            new_end = snapped
            trace["end_snapped"] = True
            trace["end_peak"] = round(peak, 3)
    if model.get("grid") and trace["start_snapped"] != trace["end_snapped"]:
        # Recipe v2 guards (2026-07-16): anchor peak floor + move cap on the
        # grid-adjusted edge; v1's uncapped low-peak grid widenings breached
        # the 90s false-widening budget on the first flag-ON dump.
        anchor_peak = trace.get("start_peak") if trace["start_snapped"] else trace.get("end_peak")
        if anchor_peak is not None and anchor_peak >= GRID_MIN_PEAK:
            grid = model["grid"]
            width = new_end - new_start
            snapped_width = grid * max(1, round(width / grid))
            if trace["start_snapped"]:
                candidate = new_start + snapped_width
                if abs(candidate - proposal["end"]) <= MAX_EDGE_MOVE_SECONDS:
                    new_end = candidate
                    trace["grid_applied"] = True
            else:
                candidate = new_end - snapped_width
                if abs(candidate - proposal["start"]) <= MAX_EDGE_MOVE_SECONDS:
                    new_start = candidate
                    trace["grid_applied"] = True
    new_start = max(0.0, min(new_start, duration - 1.0))
    new_end = max(new_start + 1.0, min(new_end, duration))
    # Sanity: refinement must not abandon the presence evidence entirely.
    if SCORE.overlap(new_start, new_end, proposal["start"], proposal["end"]) <= 0:
        return proposal["start"], proposal["end"], {
            **trace,
            "reverted_no_overlap": True,
            "start_snapped": False,
            "end_snapped": False,
            "grid_applied": False,
        }
    return new_start, new_end, trace


def group_breaks_by_show(evaluation: dict) -> dict[str, list[dict]]:
    breaks_by_show: dict[str, list[dict]] = {}
    for asset in evaluation["assets"]:
        for b in asset["full_breaks"]:
            breaks_by_show.setdefault(asset["show_name"], []).append(
                {**b, "episode_id": asset["episode_id"], "show_name": asset["show_name"]}
            )
    return breaks_by_show


def show_slug_from_episode_id(episode_id: str) -> str:
    """Corpus episode ids are `<show-slug>-<yyyy-mm-dd>-<title-slug>`."""
    match = re.match(r"^(.+?)-\d{4}-\d{2}-\d{2}-", episode_id)
    if not match:
        raise ValueError(f"cannot derive show slug from episode_id {episode_id!r}")
    return match.group(1)


def load_feed_urls_by_slug(manifest_path: pathlib.Path | None) -> dict[str, list[str]]:
    """slug -> feed-URL aliases from the corpus snapshots manifest.

    The manifest is gitignored alongside the audio (main repo only), so a
    missing file degrades to slug-only keys rather than failing the emit.
    """
    if manifest_path is None or not manifest_path.exists():
        return {}
    entries = json.loads(manifest_path.read_text(encoding="utf-8"))
    feeds: dict[str, set[str]] = {}
    for entry in entries:
        slug = entry.get("showSlug")
        feed = entry.get("feedUrl")
        if slug and feed:
            feeds.setdefault(slug, set()).add(feed)
    return {slug: sorted(urls) for slug, urls in feeds.items()}


def build_bank(
    evaluation: dict,
    envelopes: EnvelopeCache,
    feed_urls_by_slug: dict[str, list[str]],
) -> dict:
    """Build the production StingerBank payload (FULL-CORPUS models).

    A show ships in the bank only when the learner produced at least one
    stinger side — a pod-width grid alone is inert at runtime (the grid
    only applies after exactly one edge snapped, which requires a stinger).
    """
    shows = []
    for show, show_breaks in sorted(group_breaks_by_show(evaluation).items()):
        model = learn_show_model(
            show_breaks, envelopes, None, full_templates_only=True
        )
        if not model["pre"] and not model["post"]:
            continue
        slugs = {show_slug_from_episode_id(b["episode_id"]) for b in show_breaks}
        if len(slugs) != 1:
            raise ValueError(f"show {show!r} spans multiple slugs: {sorted(slugs)}")
        slug = slugs.pop()
        entry: dict = {
            "showKeys": [slug, *feed_urls_by_slug.get(slug, [])],
            "showName": show,
        }
        for side in ("pre", "post"):
            side_model = model[side]
            if not side_model:
                continue
            template = [round(float(v), 5) for v in side_model["template"].tolist()]
            if len(template) < ENVELOPE_HZ:
                # The runtime refiner refuses sub-second templates; do not
                # ship one the app would reject.
                continue
            entry[side] = {
                "template": template,
                "edgeSampleIndex": int(side_model["edge_sample"]),
                "edgeOffsetSeconds": round(float(side_model["offset"]), 3),
                "confidence": float(side_model["confidence"]),
                "support": int(side_model["support"]),
            }
        if "pre" not in entry and "post" not in entry:
            continue
        if model["grid"]:
            entry["podWidthGridSeconds"] = float(model["grid"])
        shows.append(entry)
    return {
        "schemaVersion": BANK_SCHEMA_VERSION,
        "envelopeHz": ENVELOPE_HZ,
        "pcmSampleRate": envelopes.sample_rate,
        "generator": "scripts/l2f-boundary-stinger-prototype.py --emit-bank",
        "protocol": (
            "full-corpus per-show stinger anchors (envelope NCC) + learned "
            "edge offsets + 30s pod-width grid"
        ),
        "shows": shows,
    }


def emit_bank(args: argparse.Namespace) -> int:
    evaluation = SCORE.load_evaluation(args.evaluation)
    audio_by_stem = {
        p.stem: p
        for p in args.audio_dir.iterdir()
        if p.suffix.lower() in {".mp3", ".m4a", ".aac", ".wav", ".flac", ".caf"}
    }
    envelopes = EnvelopeCache(audio_by_stem, sample_rate=BANK_SAMPLE_RATE)
    feed_urls = load_feed_urls_by_slug(args.snapshots_manifest)
    bank = build_bank(evaluation, envelopes, feed_urls)
    exclusions = []
    for spec in args.exclude_side:
        slug, _, side = spec.partition(":")
        if not slug or side not in {"pre", "post"}:
            raise SystemExit(f"--exclude-side expects SLUG:pre|post, got {spec!r}")
        exclusions.append((slug, side))
    if exclusions:
        kept = []
        for entry in bank["shows"]:
            for slug, side in exclusions:
                if slug in entry["showKeys"] and side in entry:
                    del entry[side]
            if "pre" in entry or "post" in entry:
                kept.append(entry)
        bank["shows"] = kept
    bank["sources"] = {
        "curatedExclusions": sorted(f"{s_}:{d}" for s_, d in exclusions) or None,
        "evaluation": args.evaluation.name,
        "evaluationSha256": hashlib.sha256(args.evaluation.read_bytes()).hexdigest(),
        "snapshotsManifest": (
            "TestFixtures/Corpus/Snapshots/manifest.json"
            if args.snapshots_manifest and args.snapshots_manifest.exists()
            else None
        ),
        "snapshotsManifestSha256": (
            hashlib.sha256(args.snapshots_manifest.read_bytes()).hexdigest()
            if args.snapshots_manifest and args.snapshots_manifest.exists()
            else None
        ),
    }
    args.emit_bank.write_text(
        json.dumps(bank, indent=1, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"bank: {len(bank['shows'])} shows -> {args.emit_bank}")
    for entry in bank["shows"]:
        sides = [s for s in ("pre", "post") if s in entry]
        grid = entry.get("podWidthGridSeconds")
        detail = ", ".join(
            f"{s}(conf={entry[s]['confidence']:.3f}, support={entry[s]['support']}, "
            f"len={len(entry[s]['template'])})"
            for s in sides
        )
        print(
            f"  {entry['showKeys'][0]:34} {detail}"
            + (f", grid={grid:.0f}s" if grid else "")
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evaluation", type=pathlib.Path, required=True)
    parser.add_argument("--dump", type=pathlib.Path)
    parser.add_argument(
        "--audio-dir", type=pathlib.Path, default=ROOT / "TestFixtures/Corpus/Audio"
    )
    parser.add_argument("--report", type=pathlib.Path)
    parser.add_argument(
        "--emit-bank",
        type=pathlib.Path,
        help=(
            "write the production StingerBank JSON (full-corpus per-show "
            "models) to this path instead of running the leave-one-out "
            "refinement evaluation"
        ),
    )
    parser.add_argument(
        "--snapshots-manifest",
        type=pathlib.Path,
        default=DEFAULT_SNAPSHOTS_MANIFEST,
        help="corpus snapshots manifest supplying feed-URL show aliases",
    )
    parser.add_argument(
        "--exclude-side",
        action="append",
        default=[],
        metavar="SLUG:SIDE",
        help=(
            "curated emit-time exclusion (repeatable), e.g. smartless:post; "
            "recorded in the bank's provenance"
        ),
    )
    args = parser.parse_args(argv)

    if args.emit_bank:
        return emit_bank(args)
    if not args.dump:
        parser.error("--dump is required unless --emit-bank is given")

    evaluation = SCORE.load_evaluation(args.evaluation)
    audio_by_stem = {
        p.stem: p
        for p in args.audio_dir.iterdir()
        if p.suffix.lower() in {".mp3", ".m4a", ".aac", ".wav", ".flac", ".caf"}
    }
    envelopes = EnvelopeCache(audio_by_stem)

    breaks_by_show = group_breaks_by_show(evaluation)
    durations: dict[str, float] = {}
    for asset in evaluation["assets"]:
        durations[asset["episode_id"]] = asset["duration_seconds"]

    refined = {
        eid: [dict(p) for p in spans]
        for eid, spans in SCORE.predictions_from_dump(args.dump).items()
    }
    traces = []
    for show, show_breaks in sorted(breaks_by_show.items()):
        for index, break_ in enumerate(show_breaks):
            eid = break_["episode_id"]
            model = learn_show_model(show_breaks, envelopes, index)
            spans = refined.get(eid, [])
            best, best_overlap = None, 0.0
            for span in spans:
                o = SCORE.overlap(
                    break_["start_seconds"], break_["end_seconds"],
                    span["start"], span["end"],
                )
                if o > best_overlap:
                    best, best_overlap = span, o
            if best is None:
                continue
            new_start, new_end, trace = refine_edges(
                best, model, envelopes, eid, durations[eid]
            )
            trace.update(
                {
                    "show": show,
                    "episode_id": eid,
                    "break_start": break_["start_seconds"],
                    "before": [round(best["start"], 2), round(best["end"], 2)],
                    "after": [round(new_start, 2), round(new_end, 2)],
                    "model_sides": {
                        "pre": bool(model.get("pre")),
                        "post": bool(model.get("post")),
                        "grid": bool(model.get("grid")),
                    },
                }
            )
            traces.append(trace)
            best["start"], best["end"] = new_start, new_end

    before = SCORE.score(evaluation, SCORE.predictions_from_dump(args.dump))
    after = SCORE.score(evaluation, refined)
    snapped = sum(1 for t in traces if t["start_snapped"] or t["end_snapped"])
    gold_by_key = {
        (a["episode_id"], b["start_seconds"]): b
        for a in evaluation["assets"]
        for b in a["full_breaks"]
    }
    cohort = {"n": 0, "start_before": [], "start_after": [], "end_before": [], "end_after": []}
    for t in traces:
        if not (t["start_snapped"] or t["end_snapped"] or t["grid_applied"]):
            continue
        gold_break = gold_by_key.get((t["episode_id"], t["break_start"]))
        if gold_break is None:
            continue
        cohort["n"] += 1
        cohort["start_before"].append(abs(t["before"][0] - gold_break["start_seconds"]))
        cohort["start_after"].append(abs(t["after"][0] - gold_break["start_seconds"]))
        cohort["end_before"].append(abs(t["before"][1] - gold_break["end_seconds"]))
        cohort["end_after"].append(abs(t["after"][1] - gold_break["end_seconds"]))
    report = {
        "recipe": (
            "leave-one-out per-show stinger anchors (envelope NCC) + learned "
            "offsets + 30s grid"
        ),
        "breaks_touched": len(traces),
        "breaks_snapped": snapped,
        "before": {k: before[k] for k in (
            "matched_breaks", "missed_breaks", "within_tolerance",
            "start_error_seconds", "end_error_seconds",
        )},
        "after": {k: after[k] for k in (
            "matched_breaks", "missed_breaks", "within_tolerance",
            "start_error_seconds", "end_error_seconds",
        )},
        "veto_hits_before": len(before["veto_hits"]),
        "veto_hits_after": len(after["veto_hits"]),
        "per_show_after": after["per_show_median_raw"],
        "snapped_cohort": {
            "n": cohort["n"],
            "start_median_before": round(statistics.median(cohort["start_before"]), 2) if cohort["n"] else None,
            "start_median_after": round(statistics.median(cohort["start_after"]), 2) if cohort["n"] else None,
            "end_median_before": round(statistics.median(cohort["end_before"]), 2) if cohort["n"] else None,
            "end_median_after": round(statistics.median(cohort["end_after"]), 2) if cohort["n"] else None,
        },
        "traces": traces,
    }
    if args.report:
        args.report.write_text(
            json.dumps(report, indent=1, sort_keys=True), encoding="utf-8"
        )
    for label, result in (("BEFORE", before), ("AFTER", after)):
        print(
            f"{label}: within tol {result['within_tolerance']}/{result['matched_breaks']} | "
            f"start p50 {result['start_error_seconds']['p50']}s "
            f"p95 {result['start_error_seconds']['p95']}s | "
            f"end p50 {result['end_error_seconds']['p50']}s "
            f"p95 {result['end_error_seconds']['p95']}s"
        )
    print(
        f"snapped {snapped}/{len(traces)} breaks; veto hits "
        f"{len(before['veto_hits'])} -> {len(after['veto_hits'])}"
    )
    if cohort["n"]:
        print(
            f"SNAPPED COHORT (n={cohort['n']}): start median "
            f"{statistics.median(cohort['start_before']):.1f}s -> "
            f"{statistics.median(cohort['start_after']):.1f}s | end median "
            f"{statistics.median(cohort['end_before']):.1f}s -> "
            f"{statistics.median(cohort['end_after']):.1f}s"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
