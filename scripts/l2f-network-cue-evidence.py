#!/usr/bin/env python3
"""Offline evidence run for playhead-xsdz.40: do sibling iHeart-network shows
share the SAME boundary cue audio (network-level stingers)?

Hypothesis (Dan, by ear, 2026-07-15 audit): TED Business and Stuff You Should
Know — both iHeart — bracket ad breaks with the same network-level musical
in/out cues. If true, a NETWORK cue bank generalizes stinger anchoring to
shows the user has never played (the per-show-bank cold-start gap).

Method: mirror scripts/l2f-boundary-stinger-prototype.py exactly — 50 Hz
log-RMS envelope clips around every gold v2 break edge, edge-hugging
templates (TEMPLATE_INNER/TEMPLATE_OUTER), normalized cross-correlation —
and NCC-align every edge template against every other edge clip:

- WITHIN-show, same-side blocks are the reference scale (this is the signal
  the shipped per-show StingerBank locks onto);
- CROSS-show blocks between iHeart siblings are the hypothesis test: shared
  network cues should push cross-show peaks toward the within-show scale
  (>~0.6-0.7); absent sharing they sit at the noise floor (~0.2-0.4);
- cross-SIDE pairings are included because cue roles may differ per show
  (one show's break-in cue could be another's break-out cue);
- non-iHeart CONTROL shows (morbid, smartless — strong per-show stinger
  shows) empirically pin the cross-show noise floor.

Corollary (also Dan's): SYSK's ~1s edge-straddle offsets may be NETWORK
constants. If any cross-show cue alignment clears the gate, we learn each
show's edge offset against the SHARED exemplar and compare.

Network membership is identified from the corpus snapshots manifest where
possible (omnycontent.com feeds carry Omny Studio org GUIDs; iHeartMedia's
org is e73c998e-6e60-432f-8610-ae210140c5b1 — SYSK and Nikki Glaser both
live under it) plus a documented manual entry for TED Business (Acast-hosted
feed; the URL does not encode the network, membership is Dan's audit
assertion re the TED Audio Collective / iHeart partnership).

Reuses EnvelopeCache / ncc_align / clip geometry from the prototype by
import (importlib pattern, hyphenated filename) — no copied engine code.

Usage (from repo root):
  python3 scripts/l2f-network-cue-evidence.py \
    --evaluation TestFixtures/Corpus/Evaluations/earaudit-oracle-gold-<sha>.json \
    --out-json /Users/dabrams/playhead-baselines/xsdz40-iheart-cue-matrix-<date>.json

Read-only over corpus audio + frozen artifacts; writes only --out-json.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import pathlib
import statistics
import sys
from collections import defaultdict

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PROTO = _load("l2f_boundary_stinger_prototype", "l2f-boundary-stinger-prototype.py")

ENVELOPE_HZ = PROTO.ENVELOPE_HZ

# Omny Studio feed URLs embed the owning organization's GUID as the first
# path component under /d/playlist/. iHeartMedia's Omny org GUID, observed
# on both stuff-you-should-know and the-nikki-glaser-podcast manifest feeds.
IHEART_OMNY_ORG_GUID = "e73c998e-6e60-432f-8610-ae210140c5b1"

# Manual membership entries for shows whose feed URL does not encode the
# network (documented dict per the xsdz.40 evidence-phase instructions).
MANUAL_IHEART_SLUGS: dict[str, str] = {
    "ted-business": (
        "manual: Dan 2026-07-15 audit assertion (TED Business is iHeart; "
        "TED Audio Collective x iHeartMedia partnership). Feed is Acast-"
        "hosted (feeds.acast.com/public/shows/675727607205a5bc68e57057), "
        "which encodes no network identity."
    ),
}

# Non-iHeart controls with known-strong per-show stingers: their cross-show
# blocks against the iHeart shows measure the empirical noise floor.
DEFAULT_CONTROL_SLUGS = ["morbid", "smartless"]

MATCH_GATE = 0.50  # same confidence floor the prototype learns/snaps with


def iheart_membership(manifest_path: pathlib.Path | None) -> dict[str, str]:
    """slug -> how we established iHeart membership (auto via feed URL org
    GUID where possible, else the documented manual dict)."""
    membership = dict(MANUAL_IHEART_SLUGS)
    if manifest_path and manifest_path.exists():
        for entry in json.loads(manifest_path.read_text(encoding="utf-8")):
            slug = entry.get("showSlug")
            feed = entry.get("feedUrl") or ""
            if slug and IHEART_OMNY_ORG_GUID in feed:
                membership[slug] = (
                    f"auto: omnycontent.com feed under iHeart Omny org GUID "
                    f"{IHEART_OMNY_ORG_GUID}"
                )
    return membership


def collect_edge_clips(
    evaluation: dict,
    slugs: set[str],
    envelopes,
    template_inner: float = PROTO.TEMPLATE_INNER,
    template_outer: float = PROTO.TEMPLATE_OUTER,
) -> list[dict]:
    """One clip per gold break edge for the selected shows, with the same
    clip windows + edge-hugging template geometry the prototype learns from.

    `template_inner`/`template_outer` default to the prototype's 6s/1s but
    can be narrowed for a short-cue sensitivity pass: a genuinely shared
    ~1-2s network sting would be diluted inside a 7s template (its energy
    is a minority of the window), so the hypothesis gets a second chance
    at e.g. 2s/0.5s before we call the cross-show matrix conclusive."""
    full_template_samples = int((template_inner + template_outer) * ENVELOPE_HZ)
    clips: list[dict] = []
    for asset in evaluation["assets"]:
        slug = PROTO.show_slug_from_episode_id(asset["episode_id"])
        if slug not in slugs:
            continue
        for break_index, b in enumerate(asset["full_breaks"]):
            for side, (before_s, after_s), edge_key in (
                ("pre", PROTO.PRE_CLIP, "start_seconds"),
                ("post", PROTO.POST_CLIP, "end_seconds"),
            ):
                edge = b[edge_key]
                clip_start = max(0.0, edge - before_s)
                env = envelopes.get(asset["episode_id"], clip_start, edge + after_s)
                edge_sample = int(round((edge - clip_start) * ENVELOPE_HZ))
                if side == "pre":
                    lo = edge_sample - int(template_inner * ENVELOPE_HZ)
                    hi = edge_sample + int(template_outer * ENVELOPE_HZ)
                else:
                    lo = edge_sample - int(template_outer * ENVELOPE_HZ)
                    hi = edge_sample + int(template_inner * ENVELOPE_HZ)
                lo = max(0, lo)
                template = env[lo:hi]
                clips.append(
                    {
                        "id": f"{slug}/{asset['episode_id'].split(slug + '-', 1)[-1][:10]}/b{break_index}/{side}",
                        "slug": slug,
                        "side": side,
                        "node": f"{slug}:{side}",
                        "episode_id": asset["episode_id"],
                        "break_index": break_index,
                        "edge_seconds": edge,
                        "env": env,
                        "edge_sample": edge_sample,
                        "template": template,
                        "template_edge": edge_sample - lo,
                        "full_template": template.size == full_template_samples,
                    }
                )
    return clips


def pairwise(clips: list[dict]) -> list[dict]:
    """NCC of every full-width edge template into every OTHER edge clip.

    Truncated templates (break edges near t=0) are excluded as template
    sources — the prototype's emit protocol documents how they poison
    scores — but their clips remain valid targets when long enough.
    Targets shorter than the template are skipped entirely: ncc_curve
    returns None there and the unmatchable 0.0 would drag block medians
    down (same failure mode the prototype documents for truncated clips).
    """
    results = []
    for a in clips:
        if not a["full_template"]:
            continue
        for b in clips:
            if a is b or b["env"].size < a["template"].size:
                continue
            offset, peak = PROTO.ncc_align(a["template"], b["env"])
            record = {
                "from": a["id"],
                "to": b["id"],
                "from_node": a["node"],
                "to_node": b["node"],
                "peak": round(float(peak), 3),
            }
            if offset >= 0:
                mapped_edge = offset + a["template_edge"]
                record["residual_seconds"] = round(
                    (b["edge_sample"] - mapped_edge) / ENVELOPE_HZ, 2
                )
            results.append(record)
    return results


def node_blocks(results: list[dict]) -> dict[tuple[str, str], list[float]]:
    """Pool peaks per unordered (show:side, show:side) node pair. Direction
    (which clip donated the template) is an implementation detail of the
    same acoustic question, so both directions pool into one block."""
    blocks: dict[tuple[str, str], list[float]] = defaultdict(list)
    for r in results:
        key = tuple(sorted((r["from_node"], r["to_node"])))
        blocks[key].append(r["peak"])
    return blocks


def block_stats(peaks: list[float]) -> dict:
    return {
        "n": len(peaks),
        "median": round(statistics.median(peaks), 3),
        "max": round(max(peaks), 3),
    }


def matrix_markdown(nodes: list[str], blocks: dict[tuple[str, str], list[float]]) -> str:
    """Symmetric node matrix as a markdown table: median (max) per block."""
    lines = ["| template \\ clip | " + " | ".join(nodes) + " |"]
    lines.append("|" + "---|" * (len(nodes) + 1))
    for a in nodes:
        row = [a]
        for b in nodes:
            peaks = blocks.get(tuple(sorted((a, b))))
            row.append(f"{statistics.median(peaks):.2f} ({max(peaks):.2f})" if peaks else "—")
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def shared_cue_offsets(
    clips: list[dict], iheart_slugs: set[str], gate: float
) -> dict:
    """Dan's corollary: IF a shared cross-show cue exists, learn each show's
    edge offset against one SHARED exemplar template and compare.

    Exemplar selection: the full-width iHeart template with the highest
    median peak against clips of OTHER iHeart shows. Offsets are the
    residuals (clip edge minus mapped template edge, seconds) of gated
    matches, grouped per show:side.
    """
    iheart_clips = [c for c in clips if c["slug"] in iheart_slugs]
    best = None
    for a in iheart_clips:
        if not a["full_template"]:
            continue
        cross_peaks = []
        for b in iheart_clips:
            if b["slug"] == a["slug"]:
                continue
            _, peak = PROTO.ncc_align(a["template"], b["env"])
            cross_peaks.append(float(peak))
        if not cross_peaks:
            continue
        score = statistics.median(cross_peaks)
        if best is None or score > best["median_cross_peak"]:
            best = {
                "exemplar": a["id"],
                "median_cross_peak": round(score, 3),
                "max_cross_peak": round(max(cross_peaks), 3),
            }
    if best is None:
        return {"status": "no full-width iHeart templates"}
    if best["max_cross_peak"] < gate:
        return {
            **best,
            "status": (
                f"moot: best cross-show exemplar never clears the {gate} "
                "match gate, so there is no shared cue to learn offsets against"
            ),
        }
    exemplar = next(c for c in iheart_clips if c["id"] == best["exemplar"])
    offsets: dict[str, list[float]] = defaultdict(list)
    for b in iheart_clips:
        if b is exemplar:
            continue
        offset, peak = PROTO.ncc_align(exemplar["template"], b["env"])
        if offset < 0 or peak < gate:
            continue
        mapped_edge = offset + exemplar["template_edge"]
        offsets[b["node"]].append((b["edge_sample"] - mapped_edge) / ENVELOPE_HZ)
    return {
        **best,
        "status": "computed",
        "offsets_by_node": {
            node: {
                "n": len(vals),
                "median_seconds": round(statistics.median(vals), 2),
                "spread_seconds": round(max(vals) - min(vals), 2),
            }
            for node, vals in sorted(offsets.items())
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evaluation", type=pathlib.Path, required=True)
    parser.add_argument(
        "--audio-dir", type=pathlib.Path, default=ROOT / "TestFixtures/Corpus/Audio"
    )
    parser.add_argument(
        "--snapshots-manifest",
        type=pathlib.Path,
        default=ROOT / "TestFixtures/Corpus/Snapshots/manifest.json",
    )
    parser.add_argument(
        "--controls",
        default=",".join(DEFAULT_CONTROL_SLUGS),
        help="comma-separated non-iHeart control slugs (empirical noise floor)",
    )
    parser.add_argument(
        "--template-inner",
        type=float,
        default=PROTO.TEMPLATE_INNER,
        help="template seconds on the stinger side of the edge (default: prototype's)",
    )
    parser.add_argument(
        "--template-outer",
        type=float,
        default=PROTO.TEMPLATE_OUTER,
        help="template seconds past the edge (default: prototype's)",
    )
    parser.add_argument("--out-json", type=pathlib.Path)
    args = parser.parse_args(argv)

    evaluation = PROTO.SCORE.load_evaluation(args.evaluation)
    membership = iheart_membership(args.snapshots_manifest)
    audio_by_stem = {
        p.stem: p
        for p in args.audio_dir.iterdir()
        if p.suffix.lower() in {".mp3", ".m4a", ".aac", ".wav", ".flac", ".caf"}
    }
    envelopes = PROTO.EnvelopeCache(audio_by_stem)

    gold_slugs = {
        PROTO.show_slug_from_episode_id(a["episode_id"]) for a in evaluation["assets"]
    }
    iheart_slugs = {s for s in membership if s in gold_slugs}
    control_slugs = [
        s for s in args.controls.split(",") if s and s in gold_slugs
    ]
    print("iHeart membership (in gold):")
    for slug in sorted(iheart_slugs):
        print(f"  {slug}: {membership[slug]}")
    print(f"controls (non-iHeart noise floor): {control_slugs}")

    clips = collect_edge_clips(
        evaluation,
        iheart_slugs | set(control_slugs),
        envelopes,
        template_inner=args.template_inner,
        template_outer=args.template_outer,
    )
    print(
        f"template geometry: inner={args.template_inner}s outer={args.template_outer}s"
    )
    truncated = [c["id"] for c in clips if not c["full_template"]]
    print(f"clips: {len(clips)} edges ({len(truncated)} truncated: {truncated})")

    results = pairwise(clips)
    blocks = node_blocks(results)

    nodes = sorted(
        {c["node"] for c in clips},
        key=lambda n: (n.split(":")[0] not in iheart_slugs, n),
    )
    print("\nnode similarity matrix — NCC peak median (max) per block:")
    print(matrix_markdown(nodes, blocks))

    corollary = shared_cue_offsets(clips, iheart_slugs, MATCH_GATE)
    print("\nshared-cue offset corollary:")
    print(json.dumps(corollary, indent=1))

    if args.out_json:
        payload = {
            "artifact_kind": "xsdz40_network_cue_evidence",
            "generator": "scripts/l2f-network-cue-evidence.py",
            "evaluation": args.evaluation.name,
            "evaluationSha256": hashlib.sha256(
                args.evaluation.read_bytes()
            ).hexdigest(),
            "engine": {
                "envelopeHz": ENVELOPE_HZ,
                "pcmSampleRate": envelopes.sample_rate,
                "preClip": PROTO.PRE_CLIP,
                "postClip": PROTO.POST_CLIP,
                "templateInnerSeconds": args.template_inner,
                "templateOuterSeconds": args.template_outer,
                "matchGate": MATCH_GATE,
            },
            "iheartMembership": {s: membership[s] for s in sorted(iheart_slugs)},
            "controlSlugs": control_slugs,
            "truncatedTemplates": truncated,
            "nodeMatrix": {
                f"{a} × {b}": block_stats(peaks)
                for (a, b), peaks in sorted(blocks.items())
            },
            "pairs": results,
            "sharedCueOffsetCorollary": corollary,
        }
        args.out_json.write_text(
            json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(f"\nwrote {args.out_json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
