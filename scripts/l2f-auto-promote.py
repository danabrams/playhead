#!/usr/bin/env python3
"""
l2f-auto-promote.py — autonomous, conservative Tier-A ground-truth promotion.

For every episode that has BOTH a transcript-heuristic draft (<id>.draft.json
from l2f-draft-annotation.swift) AND a DAI-rediff draft (<id>.dai-rediff.json
from l2f-dai-rediff.py) under TestFixtures/Corpus/Drafts/, optionally combined
with the per-episode confirmed-ad-windows entry from the most-recent
playhead-dogfood-diagnostics-pipeline-dump-*.json at the repo root, this
script TRIANGULATES the three signals and writes Annotation files into
TestFixtures/Corpus/Annotations/<id>.json — fully autonomous, no human in the
loop.

================================================================
Triangulation rules (HIGH PRECISION, LOWER RECALL — by design)
================================================================
The user does NOT review these. Mistakes compound. The bar is: a span must
have at least two independent signals agreeing OR a single strong physical
signal (a long DAI rediff). Anything weaker is dropped on the floor.

A "span" is built from the union of (drafter, pipeline, rediff). Two spans
are merged when they share any-overlap (>0s intersection).

PROMOTE iff one of these rules fires (priority order, first match wins):

  R1 ── DAI confirmed by content:
        rediff span length ≥ 10s AND (drafter overlap OR pipeline overlap)
        → audit_priority = 3 (low; triangulated)
        ad_type = "dai"
        provenance = ["rediff", drafter?, pipeline?]

  R2 ── drafter & pipeline agree on a host-read:
        drafter overlap pipeline AND combined-span ≥ 20s AND (any of:
          drafter has non-null advertiser_guess
          OR drafter ad_type starts with "host_read"/"blended_host_read"
          OR pipeline window has skipConfidence ≥ 0.85)
        → audit_priority = 3 (low; triangulated)
        ad_type = drafter ad_type if present, else "host_read"
        provenance = ["drafter", "pipeline"]

  R3 ── physical DAI alone, content heuristics missed:
        rediff span length ≥ 20s AND NOT (drafter overlap OR pipeline overlap)
        → audit_priority = 1 (high; rediff-only, earwitness-audit candidate)
        ad_type = "dai"
        provenance = ["rediff"]

REJECT otherwise. In particular:
  * rediff slots < 10s alone   (MP3 frame artifacts; below noise floor)
  * drafter-only spans         (no second source corroborates)
  * pipeline-only spans        (no second source corroborates)
  * any span where the SOLE evidence is short / single-source

We deliberately tolerate false negatives. False positives in the committed
Annotations/ corpus poison every downstream eval that uses them as ground
truth — including chapter-fusion lift, A/B harnesses, and xsdz precision
numbers — so we err on the side of silence.

================================================================
Output schema (matches existing TestFixtures/Corpus/Annotations/*.json)
================================================================
{
  "episode_id":        "<show-date-slug>",
  "show_name":         "<from manifest if available, else drafter show_name>",
  "duration_seconds":  <from pipeline-dump OR drafter, else null>,
  "ad_windows": [
    {
      "start_seconds":    float,        # merged-span start
      "end_seconds":      float,        # merged-span end
      "advertiser":       str | null,   # drafter advertiser_guess if any
      "product":          null,
      "ad_type":          "dai" | "host_read" | "blended_host_read",
      "transition_type":  null,
      "confidence_notes": "<rule + provenance + skip-conf + flank stats>",
      "provenance":       ["drafter"?, "pipeline"?, "rediff"?],  # NEW
      "auto_promoted":    true,                                  # NEW
      "audit_priority":   1 | 3                                  # NEW
    },
    ...
  ],
  "content_windows": [<derived from ad_windows + duration>],
  "variant_of":      null,
  "audio_fingerprint": "sha256:<from rediff if available, else drafter, else null>",
  "auto_promoted":   true,                                       # NEW (top-level)
  "auto_promoted_at": "<iso8601-utc>",
  "auto_promoted_by": "scripts/l2f-auto-promote.py"
}

Existing Annotations files (manually committed pre-corpus-loop) DO NOT carry
the auto_promoted/audit_priority fields. They are additive and downstream
readers tolerate unknown JSON keys (verified against
scripts/l2f-promote-reviewed-corpus.py which reads only the documented
schema).

================================================================
Idempotence + safety
================================================================
* If TestFixtures/Corpus/Annotations/<episode-id>.json ALREADY EXISTS, we
  skip + log (never overwrite a human-committed annotation). Pass --force to
  intentionally re-promote (e.g. after rule tuning).
* Writes are atomic: write to <path>.tmp, fsync, os.replace.
* --dry-run prints what WOULD be promoted; writes nothing.
* Re-running the script the same day on the same drafts is a no-op.

================================================================
Usage
================================================================
    scripts/l2f-auto-promote.py --dry-run
    scripts/l2f-auto-promote.py
    scripts/l2f-auto-promote.py --episode morbid
    scripts/l2f-auto-promote.py --force --episode smartless

Writes a summary to playhead-dogfood-diagnostics-auto-promote.json at the
repo root (matches the existing diagnostics naming convention; git-ignored
via `playhead-dogfood-diagnostics-*.json`).
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import os
import pathlib
import re
import sys
from typing import Any

REPO = pathlib.Path(__file__).resolve().parents[1]
DRAFTS = REPO / "TestFixtures/Corpus/Drafts"
ANNOTATIONS = REPO / "TestFixtures/Corpus/Annotations"
MANIFEST = REPO / "TestFixtures/Corpus/Snapshots/manifest.json"
DIAG_OUT = REPO / "playhead-dogfood-diagnostics-auto-promote.json"

# Thresholds — see file docstring for rule definitions.
# Comparisons use `>=` with a small EPS to absorb float-subtraction noise; a
# cluster envelope of (e.g.) end-start = 19.999999999999545 from a pipeline
# window literally 4076.36→4096.36 should be treated as the 20.0s the user
# wrote down, not rejected on numerics.
R1_REDIFF_MIN_SECONDS = 10.0
R2_COMBINED_MIN_SECONDS = 20.0
R2_PIPELINE_MIN_SKIP_CONFIDENCE = 0.85
R3_REDIFF_MIN_SECONDS = 20.0
LENGTH_EPS = 0.01  # tolerate float subtraction noise


def log(*a: Any) -> None:
    print(*a, file=sys.stderr, flush=True)


# ────────────────────────────── input loading ──────────────────────────────


def load_manifest() -> dict[str, dict]:
    """episode_id → manifest row. Empty dict if manifest missing."""
    if not MANIFEST.exists():
        return {}
    try:
        rows = json.loads(MANIFEST.read_text())
    except Exception as e:
        log(f"WARN: failed to parse manifest: {e}")
        return {}
    return {r["episodeId"]: r for r in rows if "episodeId" in r}


def latest_pipeline_dump() -> tuple[pathlib.Path | None, dict[str, dict]]:
    """
    Find the most-recent playhead-dogfood-diagnostics-pipeline-dump-*.json at
    repo root (by mtime). Returns (path, {episode_id: episode_dict}).
    Returns (None, {}) if none exist.
    """
    cands = sorted(
        REPO.glob("playhead-dogfood-diagnostics-pipeline-dump-*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not cands:
        return None, {}
    path = cands[0]
    try:
        obj = json.loads(path.read_text())
    except Exception as e:
        log(f"WARN: failed to parse pipeline dump {path.name}: {e}")
        return path, {}
    episodes = obj.get("episodes") or []
    return path, {e["episodeId"]: e for e in episodes if "episodeId" in e}


def find_episode_pairs() -> list[str]:
    """
    Episode IDs that have BOTH a <id>.draft.json AND a <id>.dai-rediff.json
    in TestFixtures/Corpus/Drafts/.
    """
    drafter_ids = {
        p.name[: -len(".draft.json")]
        for p in DRAFTS.glob("*.draft.json")
    }
    rediff_ids = {
        p.name[: -len(".dai-rediff.json")]
        for p in DRAFTS.glob("*.dai-rediff.json")
    }
    return sorted(drafter_ids & rediff_ids)


# ───────────────────────────── span extraction ─────────────────────────────


def _to_float(v: Any) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def load_drafter_spans(episode_id: str) -> list[dict]:
    """
    Returns list of {start, end, advertiser_guess, ad_type, transition_type}.
    """
    p = DRAFTS / f"{episode_id}.draft.json"
    if not p.exists():
        return []
    try:
        obj = json.loads(p.read_text())
    except Exception as e:
        log(f"WARN: drafter parse failure {episode_id}: {e}")
        return []
    out = []
    for w in obj.get("ad_windows") or []:
        s = _to_float(w.get("start_seconds"))
        e = _to_float(w.get("end_seconds"))
        if s is None or e is None or e <= s:
            continue
        out.append(
            {
                "start": s,
                "end": e,
                "advertiser_guess": w.get("advertiser_guess"),
                "ad_type": w.get("ad_type"),
                "transition_type": w.get("transition_type"),
            }
        )
    return out


def load_rediff_spans(episode_id: str) -> list[dict]:
    """
    Returns list of {start, end, confidence_notes}.
    """
    p = DRAFTS / f"{episode_id}.dai-rediff.json"
    if not p.exists():
        return []
    try:
        obj = json.loads(p.read_text())
    except Exception as e:
        log(f"WARN: rediff parse failure {episode_id}: {e}")
        return []
    out = []
    for w in obj.get("ad_windows") or []:
        s = _to_float(w.get("start_seconds"))
        e = _to_float(w.get("end_seconds"))
        if s is None or e is None or e <= s:
            continue
        out.append(
            {
                "start": s,
                "end": e,
                "confidence_notes": w.get("confidence_notes") or "",
            }
        )
    return out


def load_pipeline_spans(episode_id: str, pipeline_idx: dict[str, dict]) -> list[dict]:
    """
    Returns list of {start, end, skipConfidence, decisionState, eligibilityGate}.
    """
    ep = pipeline_idx.get(episode_id)
    if not ep:
        return []
    out = []
    for w in ep.get("adWindows") or []:
        s = _to_float(w.get("startTime"))
        e = _to_float(w.get("endTime"))
        if s is None or e is None or e <= s:
            continue
        out.append(
            {
                "start": s,
                "end": e,
                "skipConfidence": _to_float(w.get("skipConfidence")) or 0.0,
                "decisionState": w.get("decisionState"),
                "eligibilityGate": w.get("eligibilityGate"),
            }
        )
    return out


# ──────────────────────── span-merging / overlap ────────────────────────


def _overlaps(a: dict, b: dict) -> bool:
    return a["start"] < b["end"] and b["start"] < a["end"]


def _overlap_amount(a: dict, b: dict) -> float:
    return max(0.0, min(a["end"], b["end"]) - max(a["start"], b["start"]))


class Cluster:
    """A merged span carrying which source-spans contributed."""

    __slots__ = ("start", "end", "drafter", "pipeline", "rediff")

    def __init__(self, start: float, end: float):
        self.start = start
        self.end = end
        self.drafter: list[dict] = []
        self.pipeline: list[dict] = []
        self.rediff: list[dict] = []

    def length(self) -> float:
        return self.end - self.start

    def absorb(self, s: float, e: float) -> None:
        self.start = min(self.start, s)
        self.end = max(self.end, e)

    def add(self, kind: str, span: dict) -> None:
        # absorb the source-span's bounds into the cluster envelope
        self.absorb(span["start"], span["end"])
        getattr(self, kind).append(span)


def build_clusters(
    drafter: list[dict], pipeline: list[dict], rediff: list[dict]
) -> list[Cluster]:
    """
    Build clusters via any-overlap union-find over the union of all source
    spans. Merge until fixed-point (a cluster can grow during merging, which
    may then bring in additional spans that overlap the new envelope).
    """
    items: list[tuple[str, dict]] = (
        [("drafter", s) for s in drafter]
        + [("pipeline", s) for s in pipeline]
        + [("rediff", s) for s in rediff]
    )
    clusters: list[Cluster] = []
    for kind, span in items:
        attached_to: Cluster | None = None
        for c in clusters:
            if _overlaps(span, {"start": c.start, "end": c.end}):
                if attached_to is None:
                    c.add(kind, span)
                    attached_to = c
                else:
                    # merge this cluster into attached_to
                    attached_to.absorb(c.start, c.end)
                    attached_to.drafter.extend(c.drafter)
                    attached_to.pipeline.extend(c.pipeline)
                    attached_to.rediff.extend(c.rediff)
                    # mark for removal
                    c.start = float("inf")
                    c.end = float("-inf")
        if attached_to is None:
            new_c = Cluster(span["start"], span["end"])
            new_c.add(kind, span)
            clusters.append(new_c)
        clusters = [c for c in clusters if c.length() > 0]

    # final pass: clusters may still have grown into overlap with each other
    # after absorbing; collapse one more time deterministically.
    clusters.sort(key=lambda c: c.start)
    merged: list[Cluster] = []
    for c in clusters:
        if merged and _overlaps(
            {"start": c.start, "end": c.end},
            {"start": merged[-1].start, "end": merged[-1].end},
        ):
            m = merged[-1]
            m.absorb(c.start, c.end)
            m.drafter.extend(c.drafter)
            m.pipeline.extend(c.pipeline)
            m.rediff.extend(c.rediff)
        else:
            merged.append(c)
    return merged


# ───────────────────────── promotion rule engine ─────────────────────────


def evaluate_cluster(c: Cluster) -> dict:
    """
    Return {decision, rule, audit_priority, ad_type, advertiser, provenance,
            confidence_notes, ...} for a cluster.
    decision ∈ {"promote", "reject"}.
    """
    rediff_total = sum(r["end"] - r["start"] for r in c.rediff)
    has_drafter = bool(c.drafter)
    has_pipeline = bool(c.pipeline)
    has_rediff = bool(c.rediff)
    combined_len = c.length()
    max_skip_conf = max(
        (p.get("skipConfidence") or 0.0) for p in c.pipeline
    ) if has_pipeline else 0.0
    drafter_adv = next(
        (
            d.get("advertiser_guess")
            for d in c.drafter
            if d.get("advertiser_guess")
        ),
        None,
    )
    drafter_ad_type = next(
        (d.get("ad_type") for d in c.drafter if d.get("ad_type")), None
    )

    provenance: list[str] = []
    if has_drafter:
        provenance.append("drafter")
    if has_pipeline:
        provenance.append("pipeline")
    if has_rediff:
        provenance.append("rediff")

    base_notes = (
        f"auto-promoted {c.start:.1f}-{c.end:.1f}s "
        f"(len={combined_len:.1f}s). "
        f"rediff-sec={rediff_total:.1f}, "
        f"drafter-spans={len(c.drafter)}, "
        f"pipeline-spans={len(c.pipeline)} "
        f"(max-skip-conf={max_skip_conf:.2f})."
    )

    # ── R1: physical DAI confirmed by content
    if (
        has_rediff
        and rediff_total + LENGTH_EPS >= R1_REDIFF_MIN_SECONDS
        and (has_drafter or has_pipeline)
    ):
        return {
            "decision": "promote",
            "rule": "R1",
            "audit_priority": 3,
            "ad_type": "dai",
            "advertiser": drafter_adv,
            "provenance": provenance,
            "confidence_notes": (
                "RULE R1 (physical DAI rediff confirmed by content signal). "
                + base_notes
            ),
        }

    # ── R2: drafter + pipeline agree on a host-read
    if (
        has_drafter
        and has_pipeline
        and combined_len + LENGTH_EPS >= R2_COMBINED_MIN_SECONDS
        and (
            drafter_adv is not None
            or (
                drafter_ad_type
                and drafter_ad_type.startswith(("host_read", "blended_host_read"))
            )
            or max_skip_conf >= R2_PIPELINE_MIN_SKIP_CONFIDENCE
        )
    ):
        ad_type = drafter_ad_type or "host_read"
        return {
            "decision": "promote",
            "rule": "R2",
            "audit_priority": 3,
            "ad_type": ad_type,
            "advertiser": drafter_adv,
            "provenance": provenance,
            "confidence_notes": (
                "RULE R2 (drafter heuristic + pipeline decision agree). "
                + base_notes
            ),
        }

    # ── R3: physical DAI alone, content heuristics missed
    if (
        has_rediff
        and rediff_total + LENGTH_EPS >= R3_REDIFF_MIN_SECONDS
        and not has_drafter
        and not has_pipeline
    ):
        return {
            "decision": "promote",
            "rule": "R3",
            "audit_priority": 1,
            "ad_type": "dai",
            "advertiser": None,
            "provenance": ["rediff"],
            "confidence_notes": (
                "RULE R3 (rediff-only physical DAI; audit_priority=1 high). "
                + base_notes
            ),
        }

    # ── REJECT (informative diagnostic about which sub-rule failed)
    reasons = []
    if has_rediff and not has_drafter and not has_pipeline:
        # rediff-only path; failed both R1 (needs corroborating content) and
        # R3 (needs ≥20s alone).
        if rediff_total < R3_REDIFF_MIN_SECONDS:
            reasons.append(
                f"rediff-only rediff-sec={rediff_total:.1f}<{R3_REDIFF_MIN_SECONDS} "
                f"(R3); no drafter/pipeline overlap (R1)"
            )
        else:
            reasons.append("rediff-only but unexpectedly missed R3 (bug)")
    if has_drafter and not has_pipeline and not has_rediff:
        reasons.append("drafter-only (no second-source corroboration)")
    if has_pipeline and not has_drafter and not has_rediff:
        reasons.append("pipeline-only (no second-source corroboration)")
    if has_drafter and has_pipeline and not has_rediff:
        if combined_len < R2_COMBINED_MIN_SECONDS:
            reasons.append(
                f"drafter+pipeline combined-len={combined_len:.4f}<{R2_COMBINED_MIN_SECONDS} (R2)"
            )
        else:
            # combined-len OK; R2 failed on the host-read signal disjunction.
            reasons.append(
                f"drafter+pipeline ({combined_len:.1f}s) but no host-read signal "
                f"(adv_guess=None, ad_type={drafter_ad_type!r}, "
                f"max-skip-conf={max_skip_conf:.2f}<{R2_PIPELINE_MIN_SKIP_CONFIDENCE})"
            )
    if has_rediff and (has_drafter or has_pipeline) and rediff_total < R1_REDIFF_MIN_SECONDS:
        # multi-source but rediff too short for R1
        reasons.append(
            f"multi-source but rediff-sec={rediff_total:.1f}<{R1_REDIFF_MIN_SECONDS} (R1)"
        )
    return {
        "decision": "reject",
        "rule": None,
        "audit_priority": None,
        "ad_type": None,
        "advertiser": None,
        "provenance": provenance,
        "confidence_notes": "rejected: " + (", ".join(reasons) or "no rule matched"),
    }


# ─────────────────────────── annotation writing ───────────────────────────


def derive_duration(
    episode_id: str,
    pipeline_idx: dict[str, dict],
) -> float | None:
    ep = pipeline_idx.get(episode_id)
    if ep:
        d = _to_float(ep.get("episodeDurationSeconds"))
        if d:
            return d
    # fall back to drafter's duration_seconds
    p = DRAFTS / f"{episode_id}.draft.json"
    if p.exists():
        try:
            obj = json.loads(p.read_text())
            d = _to_float(obj.get("duration_seconds"))
            if d:
                return d
        except Exception:
            pass
    return None


def derive_show_name(
    episode_id: str,
    manifest: dict[str, dict],
) -> str:
    row = manifest.get(episode_id)
    if row and row.get("show"):
        return row["show"]
    # fall back to drafter
    p = DRAFTS / f"{episode_id}.draft.json"
    if p.exists():
        try:
            obj = json.loads(p.read_text())
            if obj.get("show_name"):
                return obj["show_name"]
        except Exception:
            pass
    return episode_id


def derive_fingerprint(episode_id: str, manifest: dict[str, dict]) -> str | None:
    # Prefer the manifest's recorded sha256 (the snapshot's own fingerprint).
    row = manifest.get(episode_id)
    if row and row.get("sha256"):
        return f"sha256:{row['sha256']}"
    # Else use the rediff's audio_fingerprint (the FRESH sha — still useful).
    p = DRAFTS / f"{episode_id}.dai-rediff.json"
    if p.exists():
        try:
            obj = json.loads(p.read_text())
            fp = obj.get("audio_fingerprint")
            if fp:
                return fp
        except Exception:
            pass
    p = DRAFTS / f"{episode_id}.draft.json"
    if p.exists():
        try:
            obj = json.loads(p.read_text())
            fp = obj.get("audio_fingerprint")
            if fp:
                return fp
        except Exception:
            pass
    return None


def derive_content_windows(
    ad_windows: list[dict], duration: float | None
) -> list[dict]:
    """Inter-ad content windows. Same notes style as existing annotations."""
    if duration is None or duration <= 0:
        return []
    out: list[dict] = []
    cursor = 0.0
    for w in sorted(ad_windows, key=lambda x: x["start_seconds"]):
        if w["start_seconds"] > cursor + 0.001:
            out.append(
                {
                    "start_seconds": cursor,
                    "end_seconds": w["start_seconds"],
                    "notes": "Auto-derived inter-ad content - must NEVER be skipped",
                }
            )
        cursor = max(cursor, w["end_seconds"])
    if duration > cursor + 0.001:
        out.append(
            {
                "start_seconds": cursor,
                "end_seconds": duration,
                "notes": "Auto-derived post-ad content - must NEVER be skipped",
            }
        )
    return out


def build_annotation(
    episode_id: str,
    promoted_clusters: list[tuple[Cluster, dict]],
    pipeline_idx: dict[str, dict],
    manifest: dict[str, dict],
) -> dict:
    ad_windows: list[dict] = []
    for c, ev in promoted_clusters:
        ad_windows.append(
            {
                "start_seconds": round(c.start, 2),
                "end_seconds": round(c.end, 2),
                "advertiser": ev.get("advertiser"),
                "product": None,
                "ad_type": ev.get("ad_type"),
                "transition_type": None,
                "confidence_notes": ev.get("confidence_notes"),
                "provenance": ev.get("provenance"),
                "auto_promoted": True,
                "audit_priority": ev.get("audit_priority"),
            }
        )
    ad_windows.sort(key=lambda w: w["start_seconds"])
    duration = derive_duration(episode_id, pipeline_idx)
    fingerprint = derive_fingerprint(episode_id, manifest)
    return {
        "episode_id": episode_id,
        "show_name": derive_show_name(episode_id, manifest),
        "duration_seconds": duration,
        "ad_windows": ad_windows,
        "content_windows": derive_content_windows(ad_windows, duration),
        "variant_of": None,
        "audio_fingerprint": fingerprint,
        "auto_promoted": True,
        "auto_promoted_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "auto_promoted_by": "scripts/l2f-auto-promote.py",
    }


def atomic_write_json(path: pathlib.Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    data = json.dumps(obj, indent=2, sort_keys=False).encode("utf-8") + b"\n"
    # Write + fsync the temp file, then os.replace for atomic publish.
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


# ─────────────────────────────────── main ───────────────────────────────────


def process_episode(
    episode_id: str,
    pipeline_idx: dict[str, dict],
    manifest: dict[str, dict],
) -> dict:
    drafter = load_drafter_spans(episode_id)
    pipeline = load_pipeline_spans(episode_id, pipeline_idx)
    rediff = load_rediff_spans(episode_id)
    clusters = build_clusters(drafter, pipeline, rediff)
    promoted: list[tuple[Cluster, dict]] = []
    rejected: list[dict] = []
    rule_counts: dict[str, int] = {"R1": 0, "R2": 0, "R3": 0}
    audit_counts: dict[int, int] = {1: 0, 3: 0}
    for c in clusters:
        ev = evaluate_cluster(c)
        if ev["decision"] == "promote":
            promoted.append((c, ev))
            rule_counts[ev["rule"]] += 1
            audit_counts[ev["audit_priority"]] += 1
        else:
            rejected.append(
                {
                    "start": round(c.start, 2),
                    "end": round(c.end, 2),
                    "notes": ev["confidence_notes"],
                    "provenance": ev["provenance"],
                }
            )
    return {
        "episode_id": episode_id,
        "drafter_spans": len(drafter),
        "pipeline_spans": len(pipeline),
        "rediff_spans": len(rediff),
        "clusters": len(clusters),
        "promoted": [
            {
                "start": round(c.start, 2),
                "end": round(c.end, 2),
                "rule": ev["rule"],
                "audit_priority": ev["audit_priority"],
                "ad_type": ev["ad_type"],
                "advertiser": ev.get("advertiser"),
                "provenance": ev["provenance"],
            }
            for (c, ev) in promoted
        ],
        "rejected": rejected,
        "rule_counts": rule_counts,
        "audit_counts": audit_counts,
        "_promoted_clusters": promoted,  # internal; stripped before writing summary
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be promoted; write no annotations.",
    )
    ap.add_argument(
        "--episode",
        action="append",
        help="Only process episode IDs containing this substring (repeatable).",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing Annotations/<id>.json (default skips).",
    )
    args = ap.parse_args()

    manifest = load_manifest()
    pipeline_path, pipeline_idx = latest_pipeline_dump()
    if pipeline_path is None:
        log(
            "WARN: no playhead-dogfood-diagnostics-pipeline-dump-*.json found "
            "at repo root; R2 (drafter+pipeline) will never fire."
        )
    else:
        log(f"pipeline-dump: {pipeline_path.name} ({len(pipeline_idx)} episodes)")
    log(f"manifest: {len(manifest)} entries")

    episode_ids = find_episode_pairs()
    if args.episode:
        needles = args.episode
        episode_ids = [
            e for e in episode_ids if any(n in e for n in needles)
        ]
    if not episode_ids:
        log("ERROR: no episodes have BOTH a .draft.json AND a .dai-rediff.json in Drafts/")
        return 2
    log(f"candidate episodes: {len(episode_ids)}")

    started = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    per_episode_summary: list[dict] = []
    rule_totals: dict[str, int] = {"R1": 0, "R2": 0, "R3": 0}
    audit_totals: dict[int, int] = {1: 0, 3: 0}
    promoted_episodes = 0
    skipped_existing = 0
    written = 0
    rejected_total = 0

    for ep in episode_ids:
        result = process_episode(ep, pipeline_idx, manifest)
        promoted_clusters = result.pop("_promoted_clusters")
        rejected_total += len(result["rejected"])
        for k, v in result["rule_counts"].items():
            rule_totals[k] = rule_totals.get(k, 0) + v
        for k, v in result["audit_counts"].items():
            audit_totals[k] = audit_totals.get(k, 0) + v
        if not promoted_clusters:
            log(
                f"  [SKIP-EMPTY] {ep}: 0 promoted "
                f"(drafter={result['drafter_spans']}, pipeline={result['pipeline_spans']}, "
                f"rediff={result['rediff_spans']}, rejected={len(result['rejected'])})"
            )
            result["wrote"] = False
            result["skipped_reason"] = "no clusters met any promotion rule"
            per_episode_summary.append(result)
            continue

        annotation = build_annotation(ep, promoted_clusters, pipeline_idx, manifest)
        out_path = ANNOTATIONS / f"{ep}.json"
        action: str
        if out_path.exists() and not args.force:
            skipped_existing += 1
            action = "skip-existing"
            log(
                f"  [SKIP-EXISTING] {ep}: {out_path.relative_to(REPO)} already "
                f"exists (use --force to overwrite). Would have promoted "
                f"{len(promoted_clusters)} span(s)."
            )
        elif args.dry_run:
            action = "dry-run"
            log(
                f"  [DRY-RUN] {ep}: would write {len(promoted_clusters)} ad_windows "
                f"(rules: {result['rule_counts']})"
            )
        else:
            atomic_write_json(out_path, annotation)
            written += 1
            promoted_episodes += 1
            action = "wrote"
            log(
                f"  [WROTE] {out_path.relative_to(REPO)}: "
                f"{len(promoted_clusters)} ad_windows (rules: {result['rule_counts']}, "
                f"audit-priority: {result['audit_counts']})"
            )
        result["wrote"] = action == "wrote"
        result["action"] = action
        result["annotation_path"] = str(out_path.relative_to(REPO))
        per_episode_summary.append(result)

    summary = {
        "schemaVersion": 1,
        "tool": "scripts/l2f-auto-promote.py",
        "startedIso": started,
        "completedIso": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "dryRun": args.dry_run,
        "force": args.force,
        "pipelineDump": pipeline_path.name if pipeline_path else None,
        "totals": {
            "episodes_considered": len(episode_ids),
            "annotations_written": written,
            "episodes_with_at_least_one_promotion": sum(
                1 for e in per_episode_summary if e.get("promoted")
            ),
            "skipped_existing": skipped_existing,
            "rejected_clusters": rejected_total,
            "rule_counts": rule_totals,
            "audit_priority_counts": audit_totals,
        },
        "episodes": per_episode_summary,
    }
    DIAG_OUT.write_text(json.dumps(summary, indent=2, sort_keys=False) + "\n")
    log(
        f"\nsummary: episodes={len(episode_ids)} "
        f"written={written} skipped-existing={skipped_existing} "
        f"rejected-clusters={rejected_total}"
    )
    log(
        f"  rules: R1={rule_totals['R1']} R2={rule_totals['R2']} R3={rule_totals['R3']}"
    )
    log(
        f"  audit-priority: high(1)={audit_totals.get(1, 0)} low(3)={audit_totals.get(3, 0)}"
    )
    log(f"  diagnostics: {DIAG_OUT.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
