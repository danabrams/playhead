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
* Every planned annotation is schema-validated before mutation. The batch is
  published under the canonical-manifest lock with rollback on write failure;
  `_canonical-manifest.json` is replaced last so new files are never visible
  to corpus consumers without membership.
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
import hashlib
import json
import math
import os
import pathlib
import re
import stat
import sys
import tempfile
from typing import Any

from l2f_canonical_manifest import (
    CanonicalCorpusError,
    RejectLedgerError,
    commit_canonical_annotations,
    load_canonical_manifest,
    load_reject_ledger,
    matching_reject,
    reject_veto_precommit,
    validate_canonical_annotation,
)
from convert_annotations_to_chapter_goldens import (
    is_gold_annotation,
    path_has_symlink_component,
)

REPO = pathlib.Path(__file__).resolve().parents[1]
DRAFTS = REPO / "TestFixtures/Corpus/Drafts"
ANNOTATIONS = REPO / "TestFixtures/Corpus/Annotations"
MANIFEST = REPO / "TestFixtures/Corpus/Snapshots/manifest.json"
DIAG_OUT = REPO / "playhead-dogfood-diagnostics-auto-promote.json"
REJECTS_LOG = REPO / "TestFixtures/Corpus/Snapshots/audit-rejects.jsonl"
REJECT_OVERLAP_TOLERANCE = 5.0  # seconds; a cluster within this margin of a
                                 # rejected span is treated as the same span
                                 # (boundary jitter between runs is normal)
F_PAST_TOLERANCE = 0.5           # seconds; a cluster whose end exceeds the
                                 # episode duration by more than this is
                                 # considered a rediff artifact (rediff slots
                                 # commonly overshoot the last audible frame
                                 # because the mp3 encoder pads silence). See
                                 # PR #208 / agent A's R3 audit (2026-06-01)
                                 # for the empirical justification: 6/27
                                 # auto-promoted spans had end > duration by
                                 # 44-204s, all confirmable as artifacts via
                                 # the manifest duration field.

# Thresholds — see file docstring for rule definitions.
# Comparisons use `>=` with a small EPS to absorb float-subtraction noise; a
# cluster envelope of (e.g.) end-start = 19.999999999999545 from a pipeline
# window literally 4076.36→4096.36 should be treated as the 20.0s the user
# wrote down, not rejected on numerics.
R1_REDIFF_MIN_SECONDS = 10.0
R2_COMBINED_MIN_SECONDS = 20.0
# R3 ceiling — added 2026-06-02 after the span-43 phantom investigation.
# Empirical: 6 of 7 R3 spans ≥150s were rediff time-misalignment phantoms
# (host content shifted by an earlier ad rotation), not real DAI inserts.
# The decoder's MAX_DURATION is 180s (see DecoderConstants.maxDurationSeconds);
# any rediff slot longer than that — uncorroborated by drafter or pipeline —
# is more likely a phantom than a real ad block. Require corroboration above
# this ceiling: such spans drop to R1 (if drafter or pipeline DOES overlap)
# or get rejected entirely (if neither corroborates).
R3_REDIFF_MAX_UNCORROBORATED_SECONDS = 180.0
R2_PIPELINE_MIN_SKIP_CONFIDENCE = 0.85
R3_REDIFF_MIN_SECONDS = 20.0
LENGTH_EPS = 0.01  # tolerate float subtraction noise
HUMAN_PROVENANCE = frozenset({"human_first_pass", "human_reviewed"})


class PromotionInputError(ValueError):
    """Required promotion evidence is incomplete or unsafe to consume."""


def _require_safe_path(path: pathlib.Path, *, label: str) -> None:
    if path_has_symlink_component(path):
        raise PromotionInputError(f"{label} contains a symbolic link: {path}")


def _read_json_regular(
    path: pathlib.Path, *, label: str, missing_ok: bool = False
) -> Any:
    """Descriptor-bind one JSON read after rejecting every aliased component."""
    _require_safe_path(path, label=label)
    if not path.exists():
        if missing_ok:
            return None
        raise PromotionInputError(f"{label} is missing: {path}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise PromotionInputError(f"{label} is unavailable or unsafe: {path}") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise PromotionInputError(f"{label} is not a regular file: {path}")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            data = handle.read()
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise PromotionInputError(f"cannot read {label} {path}: {error}") from error


def _atomic_write_json(path: pathlib.Path, document: object, *, label: str) -> None:
    """Durably publish diagnostics without following a leaf or parent alias."""
    _require_safe_path(path, label=label)
    path.parent.mkdir(parents=True, exist_ok=True)
    _require_safe_path(path, label=label)
    if path.exists() and not path.is_file():
        raise PromotionInputError(f"{label} is not a regular file: {path}")
    data = json.dumps(document, indent=2, sort_keys=False) + "\n"
    temporary: pathlib.Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temporary = pathlib.Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        _require_safe_path(path, label=label)
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def log(*a: Any) -> None:
    print(*a, file=sys.stderr, flush=True)


# ────────────────────────────── input loading ──────────────────────────────


def load_manifest() -> dict[str, dict]:
    """Load optional snapshot identity evidence without masking corruption."""
    rows = _read_json_regular(
        MANIFEST, label="snapshot manifest", missing_ok=True
    )
    if rows is None:
        return {}
    if not isinstance(rows, list):
        raise PromotionInputError("snapshot manifest must contain an array")

    indexed: dict[str, dict] = {}
    for index, raw_row in enumerate(rows):
        source = f"snapshot manifest row {index}"
        if not isinstance(raw_row, dict):
            raise PromotionInputError(f"{source} must be an object")
        episode_id = raw_row.get("episodeId")
        if (
            not isinstance(episode_id, str)
            or not episode_id.strip()
            or episode_id != episode_id.strip()
        ):
            raise PromotionInputError(
                f"{source}.episodeId must be a non-empty string"
            )
        if episode_id in indexed:
            raise PromotionInputError(
                f"snapshot manifest has duplicate episodeId {episode_id!r}"
            )
        show = raw_row.get("show")
        if show is not None and (
            not isinstance(show, str) or not show.strip() or show != show.strip()
        ):
            raise PromotionInputError(
                f"{source}.show must be a non-empty trimmed string or null"
            )
        sha256 = raw_row.get("sha256")
        if sha256 is not None and (
            not isinstance(sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
        ):
            raise PromotionInputError(
                f"{source}.sha256 must be 64 lowercase hexadecimal characters or null"
            )
        audio_path = raw_row.get("audioPath")
        if audio_path is not None:
            if not isinstance(audio_path, str) or not audio_path.strip():
                raise PromotionInputError(
                    f"{source}.audioPath must be a non-empty string or null"
                )
            if pathlib.Path(audio_path).stem != episode_id:
                raise PromotionInputError(
                    f"{source}.audioPath does not match episodeId {episode_id!r}"
                )
        indexed[episode_id] = raw_row
    return indexed


def latest_pipeline_dump() -> tuple[pathlib.Path | None, dict[str, dict]]:
    """
    Find the most-recent playhead-dogfood-diagnostics-pipeline-dump-*.json at
    repo root (by mtime). Returns (path, {episode_id: episode_dict}).
    Returns (None, {}) if none exist.
    """
    cands = list(REPO.glob("playhead-dogfood-diagnostics-pipeline-dump-*.json"))
    for candidate in cands:
        _require_safe_path(candidate, label="pipeline dump")
        if not candidate.is_file():
            raise PromotionInputError(
                f"pipeline dump is not a regular file: {candidate}"
            )
    cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    if not cands:
        return None, {}
    path = cands[0]
    obj = _read_json_regular(path, label="selected pipeline dump")
    if not isinstance(obj, dict) or not isinstance(obj.get("episodes"), list):
        raise PromotionInputError(
            f"selected pipeline dump {path.name} must contain an episodes array"
        )
    indexed: dict[str, dict] = {}
    for index, raw_episode in enumerate(obj["episodes"]):
        episode_id, episode = _validate_pipeline_episode(
            raw_episode,
            source=f"{path.name} episodes[{index}]",
        )
        if episode_id in indexed:
            raise PromotionInputError(
                f"selected pipeline dump {path.name} has duplicate episodeId {episode_id!r}"
            )
        indexed[episode_id] = episode
    return path, indexed


def load_rejects(
    path: pathlib.Path = REJECTS_LOG,
) -> dict[tuple[str, str], list[tuple[float, float]]]:
    """
    Load TestFixtures/Corpus/Snapshots/audit-rejects.jsonl into a dict mapping
    episodeId → list of (start, end) tuples that previous manual audit demoted.
    Empty dict if the file is missing or empty. Any malformed or duplicate
    record fails loud: silently dropping a veto can re-promote known content.
    Records come from scripts/l2f-flag-false-promote.py.
    """
    return load_reject_ledger(path)


def _cluster_is_rejected(
    c: "Cluster",
    rejects: list[tuple[float, float]],
) -> tuple[float, float] | None:
    """
    Return a reject matching the same span by near-equal endpoints or at least
    50% coverage of the shorter interval. Mere adjacency is not a match.
    """
    return matching_reject(
        c.start,
        c.end,
        rejects,
        endpoint_tolerance=REJECT_OVERLAP_TOLERANCE,
    )


def find_episode_pairs() -> list[str]:
    """
    Episode IDs that have BOTH a <id>.draft.json AND a <id>.dai-rediff.json
    in TestFixtures/Corpus/Drafts/.
    """
    _require_safe_path(DRAFTS, label="promotion drafts directory")
    if not DRAFTS.exists():
        return []
    if not DRAFTS.is_dir():
        raise PromotionInputError(
            f"promotion drafts path is not a regular directory: {DRAFTS}"
        )
    drafter_files = list(DRAFTS.glob("*.draft.json"))
    rediff_files = list(DRAFTS.glob("*.dai-rediff.json"))
    for path in [*drafter_files, *rediff_files]:
        _require_safe_path(path, label="promotion evidence")
        if not path.is_file():
            raise PromotionInputError(f"promotion evidence is not a regular file: {path}")
    drafter_ids = {
        p.name[: -len(".draft.json")]
        for p in drafter_files
    }
    rediff_ids = {
        p.name[: -len(".dai-rediff.json")]
        for p in rediff_files
    }
    return sorted(drafter_ids & rediff_ids)


# ───────────────────────────── span extraction ─────────────────────────────


def _to_float(v: Any) -> float | None:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    result = float(v)
    return result if math.isfinite(result) else None


def _validate_pipeline_episode(
    raw_episode: object,
    *,
    source: str,
) -> tuple[str, dict[str, Any]]:
    """Validate present optional pipeline evidence before it can affect labels."""
    if not isinstance(raw_episode, dict):
        raise PromotionInputError(f"{source} must be an object")
    episode_id = raw_episode.get("episodeId")
    if (
        not isinstance(episode_id, str)
        or not episode_id.strip()
        or episode_id != episode_id.strip()
    ):
        raise PromotionInputError(f"{source}.episodeId must be a non-empty string")
    windows = raw_episode.get("adWindows")
    if not isinstance(windows, list):
        raise PromotionInputError(f"{source}.adWindows must be an array")
    fingerprint = raw_episode.get("audioFingerprint")
    if not isinstance(fingerprint, str) or re.fullmatch(
        r"sha256:[0-9a-f]{64}", fingerprint
    ) is None:
        raise PromotionInputError(
            f"{source}.audioFingerprint must identify the exact coordinate asset"
        )
    duration_value = raw_episode.get("episodeDurationSeconds")
    if duration_value is not None:
        duration = _to_float(duration_value)
        if duration is None or duration <= 0:
            raise PromotionInputError(
                f"{source}.episodeDurationSeconds must be a finite positive number or null"
            )
    for index, window in enumerate(windows):
        window_source = f"{source}.adWindows[{index}]"
        if not isinstance(window, dict):
            raise PromotionInputError(f"{window_source} must be an object")
        start = _to_float(window.get("startTime"))
        end = _to_float(window.get("endTime"))
        if start is None or end is None or start < 0 or end <= start:
            raise PromotionInputError(f"{window_source} has invalid bounds")
        confidence_value = window.get("skipConfidence")
        if confidence_value is not None:
            confidence = _to_float(confidence_value)
            if confidence is None or not 0 <= confidence <= 1:
                raise PromotionInputError(
                    f"{window_source}.skipConfidence must be between 0 and 1"
                )
    return episode_id, raw_episode


def _load_evidence_windows(
    path: pathlib.Path,
    *,
    episode_id: str,
    source: str,
    expected_fingerprint: str,
) -> list[dict[str, Any]]:
    """Load one required evidence file without turning corruption into absence."""
    document = _read_json_regular(path, label=f"{source} evidence for {episode_id}")
    if not isinstance(document, dict):
        raise PromotionInputError(
            f"{source} evidence for {episode_id} must be a JSON object"
        )
    if document.get("episode_id") != episode_id:
        raise PromotionInputError(
            f"{source} evidence episode_id does not match {episode_id}"
        )
    if document.get("audio_fingerprint") != expected_fingerprint:
        raise PromotionInputError(
            f"{source} evidence fingerprint does not match retained asset for {episode_id}"
        )
    comparisons: list[str] | None = None
    if source == "rediff":
        if document.get("coordinate_space") != "snapshot_a":
            raise PromotionInputError(
                f"rediff evidence for {episode_id} is not in retained snapshot-A coordinates"
            )
        comparisons = document.get("comparison_audio_fingerprints")
        if (
            not isinstance(comparisons, list)
            or not comparisons
            or not all(
                isinstance(value, str)
                and re.fullmatch(r"sha256:[0-9a-f]{64}", value)
                for value in comparisons
            )
            or len(set(comparisons)) != len(comparisons)
        ):
            raise PromotionInputError(
                f"rediff evidence for {episode_id} has invalid comparison fingerprints"
            )
        current_comparison = document.get("comparison_audio_fingerprint")
        if (
            not isinstance(current_comparison, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", current_comparison) is None
            or current_comparison not in comparisons
        ):
            raise PromotionInputError(
                f"rediff evidence for {episode_id} has an invalid current comparison fingerprint"
            )
        if expected_fingerprint in comparisons:
            raise PromotionInputError(
                f"rediff evidence for {episode_id} reuses retained A as comparison B"
            )
    windows = document.get("ad_windows")
    if not isinstance(windows, list):
        raise PromotionInputError(
            f"{source} evidence for {episode_id} must contain an ad_windows array"
        )
    if source != "rediff":
        return windows

    assert comparisons is not None
    normalized: list[dict[str, Any]] = []
    for index, raw_window in enumerate(windows):
        if not isinstance(raw_window, dict):
            raise PromotionInputError(
                f"rediff evidence {episode_id} ad_windows[{index}] must be an object"
            )
        window = dict(raw_window)
        comparison = window.get("comparison_audio_fingerprint")
        if comparison is None and len(comparisons) == 1:
            comparison = comparisons[0]
        if (
            not isinstance(comparison, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", comparison) is None
            or comparison not in comparisons
        ):
            raise PromotionInputError(
                f"rediff evidence {episode_id} ad_windows[{index}] is not bound "
                "to one exact comparison asset"
            )
        window["comparison_audio_fingerprint"] = comparison
        normalized.append(window)
    return normalized


def _validated_span_bounds(
    window: object,
    *,
    episode_id: str,
    source: str,
    index: int,
) -> tuple[dict[str, Any], float, float]:
    if not isinstance(window, dict):
        raise PromotionInputError(
            f"{source} evidence {episode_id} ad_windows[{index}] must be an object"
        )
    start = _to_float(window.get("start_seconds"))
    end = _to_float(window.get("end_seconds"))
    if start is None or end is None or start < 0 or end <= start:
        raise PromotionInputError(
            f"{source} evidence {episode_id} ad_windows[{index}] has invalid bounds"
        )
    return window, start, end


def load_drafter_spans(episode_id: str, expected_fingerprint: str) -> list[dict]:
    """
    Returns list of {start, end, advertiser_guess, ad_type, transition_type}.
    """
    p = DRAFTS / f"{episode_id}.draft.json"
    out = []
    for index, raw_window in enumerate(
        _load_evidence_windows(
            p,
            episode_id=episode_id,
            source="drafter",
            expected_fingerprint=expected_fingerprint,
        )
    ):
        w, s, e = _validated_span_bounds(
            raw_window,
            episode_id=episode_id,
            source="drafter",
            index=index,
        )
        for field in ("advertiser_guess", "ad_type", "transition_type"):
            if w.get(field) is not None and not isinstance(w[field], str):
                raise PromotionInputError(
                    f"drafter evidence {episode_id} ad_windows[{index}].{field} "
                    "must be a string or null"
                )
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


def load_rediff_spans(episode_id: str, expected_fingerprint: str) -> list[dict]:
    """
    Returns list of {start, end, confidence_notes, comparison_audio_fingerprint}.
    """
    p = DRAFTS / f"{episode_id}.dai-rediff.json"
    out = []
    for index, raw_window in enumerate(
        _load_evidence_windows(
            p,
            episode_id=episode_id,
            source="rediff",
            expected_fingerprint=expected_fingerprint,
        )
    ):
        w, s, e = _validated_span_bounds(
            raw_window,
            episode_id=episode_id,
            source="rediff",
            index=index,
        )
        if w.get("confidence_notes") is not None and not isinstance(
            w["confidence_notes"], str
        ):
            raise PromotionInputError(
                f"rediff evidence {episode_id} ad_windows[{index}].confidence_notes "
                "must be a string or null"
            )
        out.append(
            {
                "start": s,
                "end": e,
                "confidence_notes": w.get("confidence_notes") or "",
                "comparison_audio_fingerprint": w["comparison_audio_fingerprint"],
            }
        )
    return out


def load_pipeline_spans(
    episode_id: str,
    pipeline_idx: dict[str, dict],
    expected_fingerprint: str,
) -> list[dict]:
    """
    Returns list of {start, end, skipConfidence, decisionState, eligibilityGate}.
    """
    ep = pipeline_idx.get(episode_id)
    if ep is None:
        return []
    actual_episode_id, ep = _validate_pipeline_episode(
        ep,
        source=f"pipeline episode {episode_id!r}",
    )
    if actual_episode_id != episode_id:
        raise PromotionInputError(
            f"pipeline evidence identity {actual_episode_id!r} does not match {episode_id!r}"
        )
    if ep.get("audioFingerprint") != expected_fingerprint:
        raise PromotionInputError(
            f"pipeline evidence fingerprint does not match retained asset for {episode_id}"
        )
    out = []
    for w in ep["adWindows"]:
        s = float(w["startTime"])
        e = float(w["endTime"])
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


def interval_union_seconds(spans: list[dict]) -> float:
    """Count covered seconds once even when a detector repeats a span."""
    intervals = merged_source_intervals(spans)
    if not intervals:
        return 0.0
    return sum(end - start for start, end in intervals)


def merged_source_intervals(spans: list[dict]) -> list[tuple[float, float]]:
    """Return the exact union of source intervals without using a cluster envelope."""
    raw = sorted((span["start"], span["end"]) for span in spans)
    merged: list[tuple[float, float]] = []
    for start, end in raw:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def rediff_intervals_by_comparison(
    spans: list[dict],
) -> dict[str | None, list[tuple[float, float]]]:
    """Union retained-A intervals independently for each comparison B."""
    grouped: dict[str | None, list[dict]] = {}
    for span in spans:
        grouped.setdefault(span.get("comparison_audio_fingerprint"), []).append(span)
    return {
        comparison: merged_source_intervals(comparison_spans)
        for comparison, comparison_spans in grouped.items()
    }


def has_incompatible_rediff_comparisons(spans: list[dict]) -> bool:
    """Reject overlapping per-B unions when their exact A bounds disagree."""
    groups = list(rediff_intervals_by_comparison(spans).values())
    for index, left_group in enumerate(groups):
        for right_group in groups[index + 1:]:
            for left in left_group:
                for right in right_group:
                    overlaps = left[0] < right[1] and right[0] < left[1]
                    if overlaps and left != right:
                        return True
    return False


def r1_promotion_intervals(c: Cluster) -> list[tuple[float, float]]:
    """Return exact qualifying intervals without unioning different B assets."""
    if has_incompatible_rediff_comparisons(c.rediff):
        return []
    corroborators = [*c.drafter, *c.pipeline]
    qualified = [
        span
        for span in c.rediff
        if span["end"] - span["start"] + LENGTH_EPS >= R1_REDIFF_MIN_SECONDS
        and any(
            _overlaps(span, corroborator)
            for corroborator in corroborators
        )
    ]
    per_comparison = sorted({
        interval
        for intervals in rediff_intervals_by_comparison(qualified).values()
        for interval in intervals
    })
    return per_comparison


def r2_candidate_components(c: Cluster) -> list[Cluster]:
    """Return directly overlapping drafter/pipeline components, excluding rediff."""
    candidates: list[Cluster] = []
    for component in build_clusters(c.drafter, c.pipeline, []):
        if not component.drafter or not component.pipeline:
            continue
        if not any(
            _overlaps(drafter, pipeline)
            for drafter in component.drafter
            for pipeline in component.pipeline
        ):
            continue
        candidates.append(component)
    return candidates


def r2_promotion_components(c: Cluster) -> list[Cluster]:
    """Qualify R2 only on connected drafter/pipeline evidence.

    Rediff spans may connect otherwise separate evidence into the outer cluster,
    but they cannot contribute duration or bounds to the content-only R2 rule.
    """
    qualified: list[Cluster] = []
    for component in r2_candidate_components(c):
        if component.length() + LENGTH_EPS < R2_COMBINED_MIN_SECONDS:
            continue
        advertiser = next(
            (
                span.get("advertiser_guess")
                for span in component.drafter
                if span.get("advertiser_guess")
            ),
            None,
        )
        has_host_read = any(
            isinstance(span.get("ad_type"), str)
            and span["ad_type"].startswith(("host_read", "blended_host_read"))
            for span in component.drafter
        )
        max_skip_confidence = max(
            (span.get("skipConfidence") or 0.0) for span in component.pipeline
        )
        if (
            advertiser is not None
            or has_host_read
            or max_skip_confidence >= R2_PIPELINE_MIN_SKIP_CONFIDENCE
        ):
            qualified.append(component)
    return qualified


def promotion_intervals(c: Cluster, evaluation: dict) -> list[tuple[float, float]]:
    """Publish only the evidence bounds that qualified the selected rule."""
    if evaluation.get("rule") == "R1":
        return r1_promotion_intervals(c)
    if evaluation.get("rule") == "R2":
        components = r2_promotion_components(c)
        return (
            [(components[0].start, components[0].end)]
            if len(components) == 1
            else []
        )
    return [(c.start, c.end)]


def evaluate_cluster(c: Cluster) -> dict:
    """
    Return {decision, rule, audit_priority, ad_type, advertiser, provenance,
            confidence_notes, ...} for a cluster.
    decision ∈ {"promote", "reject"}.
    """
    rediff_total = interval_union_seconds(c.rediff)
    has_drafter = bool(c.drafter)
    has_pipeline = bool(c.pipeline)
    has_rediff = bool(c.rediff)
    r1_intervals = r1_promotion_intervals(c)
    r2_candidates = r2_candidate_components(c)
    r2_components = r2_promotion_components(c)
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
    provenance: list[str] = []
    if has_drafter:
        provenance.append("drafter")
    if has_pipeline:
        provenance.append("pipeline")
    if has_rediff:
        provenance.append("rediff")

    if has_incompatible_rediff_comparisons(c.rediff):
        return {
            "decision": "reject",
            "rule": "REDIFF_COMPARISON_CONFLICT",
            "audit_priority": None,
            "ad_type": None,
            "advertiser": None,
            "provenance": provenance,
            "confidence_notes": (
                "rejected: overlapping rediff intervals from distinct comparison "
                "audio assets have incompatible retained-A bounds"
            ),
        }

    base_notes = (
        f"auto-promoted {c.start:.1f}-{c.end:.1f}s "
        f"(len={combined_len:.1f}s). "
        f"rediff-sec={rediff_total:.1f}, "
        f"drafter-spans={len(c.drafter)}, "
        f"pipeline-spans={len(c.pipeline)} "
        f"(max-skip-conf={max_skip_conf:.2f})."
    )

    # ── R1: physical DAI confirmed by content
    if r1_intervals:
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
    if len(r2_components) == 1:
        r2_drafter = [span for component in r2_components for span in component.drafter]
        r2_pipeline = [span for component in r2_components for span in component.pipeline]
        r2_advertiser = next(
            (
                span.get("advertiser_guess")
                for span in r2_drafter
                if span.get("advertiser_guess")
            ),
            None,
        )
        r2_ad_type = next(
            (span.get("ad_type") for span in r2_drafter if span.get("ad_type")),
            None,
        )
        r2_max_skip_conf = max(
            (span.get("skipConfidence") or 0.0) for span in r2_pipeline
        )
        return {
            "decision": "promote",
            "rule": "R2",
            "audit_priority": 3,
            "ad_type": r2_ad_type or "host_read",
            "advertiser": r2_advertiser,
            "provenance": ["drafter", "pipeline"],
            "confidence_notes": (
                "RULE R2 (drafter heuristic + pipeline decision agree). "
                f"qualified-components={len(r2_components)}, "
                f"max-qualified-skip-conf={r2_max_skip_conf:.2f}. "
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
        # R3 ceiling guard: long uncorroborated rediff slots are usually
        # rediff time-misalignment phantoms, not real ad blocks. See
        # R3_REDIFF_MAX_UNCORROBORATED_SECONDS comment + the 7 scrubs in
        # the 2026-06-02 span-43 investigation (transcripts confirmed host
        # content for 6 of 7 R3 spans ≥150s).
        if rediff_total > R3_REDIFF_MAX_UNCORROBORATED_SECONDS + LENGTH_EPS:
            return {
                "decision": "reject",
                "rule": "R3_TOO_LONG_UNCORROBORATED",
                "audit_priority": None,
                "ad_type": None,
                "advertiser": None,
                "provenance": ["rediff"],
                "confidence_notes": (
                    f"R3 ceiling guard: rediff-only span "
                    f"{rediff_total:.1f}s > {R3_REDIFF_MAX_UNCORROBORATED_SECONDS}s "
                    "without drafter or pipeline corroboration. Likely a "
                    "rediff time-misalignment phantom (host content shifted "
                    "by an earlier ad rotation). " + base_notes
                ),
            }
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
    if has_drafter and has_pipeline:
        if len(r2_components) > 1:
            reasons.append(
                "multiple independent R2 components share only an outer rediff cluster; "
                "refusing ambiguous shared metadata"
            )
        elif not r2_candidates:
            reasons.append("drafter+pipeline share a transitive cluster but do not overlap (R2)")
        else:
            duration_qualified = [
                component
                for component in r2_candidates
                if component.length() + LENGTH_EPS >= R2_COMBINED_MIN_SECONDS
            ]
            if not duration_qualified:
                longest = max(component.length() for component in r2_candidates)
                reasons.append(
                    "direct drafter+pipeline combined-len="
                    f"{longest:.4f}<{R2_COMBINED_MIN_SECONDS} (R2; rediff excluded)"
                )
            else:
                candidate_drafter = [
                    span
                    for component in duration_qualified
                    for span in component.drafter
                ]
                candidate_pipeline = [
                    span
                    for component in duration_qualified
                    for span in component.pipeline
                ]
                candidate_types = sorted({
                    span["ad_type"]
                    for span in candidate_drafter
                    if isinstance(span.get("ad_type"), str)
                })
                candidate_max_skip = max(
                    (span.get("skipConfidence") or 0.0)
                    for span in candidate_pipeline
                )
                reasons.append(
                    "direct drafter+pipeline component has no host-read signal "
                    f"(adv_guess=None, ad_types={candidate_types!r}, "
                    f"max-skip-conf={candidate_max_skip:.2f}"
                    f"<{R2_PIPELINE_MIN_SKIP_CONFIDENCE})"
                )
    if has_rediff and (has_drafter or has_pipeline) and not r1_intervals:
        reasons.append(
            "multi-source but no directly corroborated rediff interval reaches "
            f"{R1_REDIFF_MIN_SECONDS:g}s (R1; total rediff-sec={rediff_total:.1f})"
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
    obj = _read_json_regular(
        p, label=f"drafter evidence for {episode_id}", missing_ok=True
    )
    if obj is None:
        return None
    if not isinstance(obj, dict):
        raise PromotionInputError(f"drafter evidence for {episode_id} must be an object")
    d = _to_float(obj.get("duration_seconds"))
    if d:
        return d
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
    obj = _read_json_regular(
        p, label=f"drafter evidence for {episode_id}", missing_ok=True
    )
    if obj is None:
        return episode_id
    if not isinstance(obj, dict):
        raise PromotionInputError(f"drafter evidence for {episode_id} must be an object")
    if isinstance(obj.get("show_name"), str) and obj["show_name"]:
        return obj["show_name"]
    return episode_id


def derive_fingerprint(episode_id: str, manifest: dict[str, dict]) -> str:
    """Hash the retained coordinate asset; matching metadata is insufficient."""
    row = manifest.get(episode_id)
    if not row or not row.get("sha256") or not row.get("audioPath"):
        raise PromotionInputError(
            f"snapshot manifest has no retained audio fingerprint and path for {episode_id}"
        )
    raw_path = pathlib.Path(row["audioPath"])
    if raw_path.is_absolute() or ".." in raw_path.parts:
        raise PromotionInputError(f"snapshot audioPath is unsafe for {episode_id}")
    lexical_audio_root = REPO / "TestFixtures/Corpus/Audio"
    _require_safe_path(lexical_audio_root, label="corpus audio root")
    audio_root = lexical_audio_root.resolve()
    unresolved_audio_path = REPO / raw_path
    _require_safe_path(
        unresolved_audio_path,
        label=f"retained snapshot audio for {episode_id}",
    )
    audio_path = unresolved_audio_path.resolve()
    try:
        audio_path.relative_to(audio_root)
    except ValueError as error:
        raise PromotionInputError(
            f"snapshot audioPath escapes the corpus audio directory for {episode_id}"
        ) from error
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(audio_path, flags)
    except OSError as error:
        raise PromotionInputError(
            f"retained snapshot audio is unavailable for {episode_id}: {audio_path}"
        ) from error
    digest = hashlib.sha256()
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise PromotionInputError(
                f"retained snapshot audio is not a regular file for {episode_id}"
            )
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    actual = digest.hexdigest()
    if actual != row["sha256"]:
        raise PromotionInputError(
            f"retained snapshot audio fingerprint mismatch for {episode_id}: "
            f"manifest={row['sha256']} actual={actual}"
        )
    return f"sha256:{actual}"


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
    expected_fingerprint: str,
) -> dict:
    ad_windows: list[dict] = []
    for c, ev in promoted_clusters:
        for start, end in promotion_intervals(c, ev):
            ad_windows.append(
                {
                    "start_seconds": round(start, 2),
                    "end_seconds": round(end, 2),
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
    current_fingerprint = derive_fingerprint(episode_id, manifest)
    if current_fingerprint != expected_fingerprint:
        raise PromotionInputError(
            f"retained snapshot audio changed while auto-promoting {episode_id}: "
            f"expected={expected_fingerprint} actual={current_fingerprint}"
        )
    return {
        "episode_id": episode_id,
        "show_name": derive_show_name(episode_id, manifest),
        "duration_seconds": duration,
        "ad_windows": ad_windows,
        "content_windows": derive_content_windows(ad_windows, duration),
        "variant_of": None,
        "audio_fingerprint": expected_fingerprint,
        "auto_promoted": True,
        "auto_promoted_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "auto_promoted_by": "scripts/l2f-auto-promote.py",
    }


def auto_promotion_precommit(
    annotations_dir: pathlib.Path = ANNOTATIONS,
    rejects_path: pathlib.Path = REJECTS_LOG,
    manifest: dict[str, dict] | None = None,
):
    """Recheck permanent vetoes and human ownership under the corpus lock."""
    reject_policy = reject_veto_precommit(rejects_path)

    def enforce(existing: dict, proposed: dict) -> None:
        reject_policy(existing, proposed)
        for filename, replacement in proposed.items():
            if manifest is not None:
                current_fingerprint = derive_fingerprint(
                    replacement["episode_id"], manifest
                )
                if current_fingerprint != replacement["audio_fingerprint"]:
                    raise CanonicalCorpusError(
                        "retained snapshot audio changed before auto-promotion publication: "
                        f"{filename}"
                    )
            current = existing.get(filename)
            path = annotations_dir / filename
            if current is None and path.exists():
                if path.is_symlink() or not path.is_file():
                    raise CanonicalCorpusError(
                        f"annotation output is not a regular file: {filename}"
                    )
                try:
                    current = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as error:
                    raise CanonicalCorpusError(
                        f"cannot inspect existing annotation {filename}: {error}"
                    ) from error
                validate_canonical_annotation(current, filename=filename)
            if current is not None:
                provenance_values = [current.get("provenance")]
                provenance_values.extend(
                    window.get("provenance")
                    for window in current.get("ad_windows", [])
                    if isinstance(window, dict)
                )
                has_human_review = any(
                    isinstance(provenance, list)
                    and any(
                        isinstance(value, str)
                        and value.casefold() in HUMAN_PROVENANCE
                        for value in provenance
                    )
                    for provenance in provenance_values
                )
                has_review_attestations = (
                    isinstance(current.get("review_attestations"), list)
                    and bool(current["review_attestations"])
                )
                if (
                    is_gold_annotation(current, require_review_artifacts=False)
                    or has_human_review
                    or has_review_attestations
                ):
                    raise CanonicalCorpusError(
                        "refusing automatic overwrite of human-owned annotation: "
                        f"{filename}"
                    )

    return enforce


# ─────────────────────────────────── main ───────────────────────────────────


def process_episode(
    episode_id: str,
    pipeline_idx: dict[str, dict],
    manifest: dict[str, dict],
    rejects_idx: dict[tuple[str, str], list[tuple[float, float]]] | None = None,
) -> dict:
    target_fingerprint = derive_fingerprint(episode_id, manifest)
    drafter = load_drafter_spans(episode_id, target_fingerprint)
    pipeline = load_pipeline_spans(episode_id, pipeline_idx, target_fingerprint)
    rediff = load_rediff_spans(episode_id, target_fingerprint)
    clusters = build_clusters(drafter, pipeline, rediff)
    promoted: list[tuple[Cluster, dict]] = []
    rejected: list[dict] = []
    rule_counts: dict[str, int] = {"R1": 0, "R2": 0, "R3": 0}
    audit_counts: dict[int, int] = {1: 0, 3: 0}
    rejects = (rejects_idx or {}).get((episode_id, target_fingerprint), [])
    audit_rejected_count = 0
    f_past_rejected_count = 0
    r3_ceiling_rejected_count = 0
    # F_PAST guard inputs: episode duration is required to detect rediff
    # overshoot past last audible frame. derive_duration() returns None when
    # neither the pipeline-dump nor the drafter knows the duration; in that
    # case the guard cannot fire (we don't fail-closed since some valid
    # episodes lack duration metadata in early-stage corpora).
    episode_duration = derive_duration(episode_id, pipeline_idx)
    for c in clusters:
        # Manual-audit veto: if this cluster matches a previously rejected
        # span (within tolerance), skip evaluation entirely. Keeps demoted
        # false promotions from being re-promoted on the next loop run.
        match = _cluster_is_rejected(c, rejects) if rejects else None
        if match is not None:
            audit_rejected_count += 1
            rejected.append(
                {
                    "start": round(c.start, 2),
                    "end": round(c.end, 2),
                    "notes": (
                        f"audit-rejected (matches prior demote "
                        f"{match[0]:.1f}-{match[1]:.1f}s within "
                        f"±{REJECT_OVERLAP_TOLERANCE:.0f}s tolerance)."
                    ),
                    "provenance": ["audit-reject"],
                }
            )
            continue
        # F_PAST veto: cluster ends past the episode's last audible frame.
        # This is overwhelmingly a rediff artifact (mp3 silence padding /
        # ID3v2 tail confusing the chromaprint aligner). Empirical: 6/27
        # spans in the 2026-06-01 corpus had overshoot 44-204s; all
        # confirmable as artifacts. Reject (don't trim) — we'd rather lose
        # a real ad that touches the end-of-episode than promote ~150s of
        # silence as ground truth that biases every downstream eval.
        if (
            episode_duration is not None
            and c.end > episode_duration + F_PAST_TOLERANCE
        ):
            overshoot = c.end - episode_duration
            f_past_rejected_count += 1
            rejected.append(
                {
                    "start": round(c.start, 2),
                    "end": round(c.end, 2),
                    "notes": (
                        f"F_PAST-rejected (cluster end {c.end:.1f}s > "
                        f"episode duration {episode_duration:.1f}s by "
                        f"{overshoot:.1f}s; rediff overshoot artifact)."
                    ),
                    "provenance": ["f-past-reject"],
                }
            )
            continue
        ev = evaluate_cluster(c)
        if ev["decision"] == "promote":
            promoted.append((c, ev))
            output_count = len(promotion_intervals(c, ev))
            rule_counts[ev["rule"]] += output_count
            audit_counts[ev["audit_priority"]] += output_count
        else:
            if ev.get("rule") == "R3_TOO_LONG_UNCORROBORATED":
                r3_ceiling_rejected_count += 1
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
                "start": round(start, 2),
                "end": round(end, 2),
                "rule": ev["rule"],
                "audit_priority": ev["audit_priority"],
                "ad_type": ev["ad_type"],
                "advertiser": ev.get("advertiser"),
                "provenance": ev["provenance"],
            }
            for (c, ev) in promoted
            for (start, end) in promotion_intervals(c, ev)
        ],
        "rejected": rejected,
        "rule_counts": rule_counts,
        "audit_counts": audit_counts,
        "audit_rejected": audit_rejected_count,
        "f_past_rejected": f_past_rejected_count,
        "r3_ceiling_rejected": r3_ceiling_rejected_count,
        "_promoted_clusters": promoted,  # internal; stripped before writing summary
        "_target_fingerprint": target_fingerprint,
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

    try:
        _require_safe_path(DRAFTS, label="promotion drafts directory")
        _require_safe_path(DIAG_OUT, label="auto-promotion diagnostics output")
        manifest = load_manifest()
    except PromotionInputError as error:
        log(f"ERROR: refusing auto-promotion with invalid snapshot manifest: {error}")
        return 2
    try:
        pipeline_path, pipeline_idx = latest_pipeline_dump()
    except PromotionInputError as error:
        log(f"ERROR: refusing auto-promotion with invalid pipeline evidence: {error}")
        return 2
    if pipeline_path is None:
        log(
            "WARN: no playhead-dogfood-diagnostics-pipeline-dump-*.json found "
            "at repo root; R2 (drafter+pipeline) will never fire."
        )
    else:
        log(f"pipeline-dump: {pipeline_path.name} ({len(pipeline_idx)} episodes)")
    log(f"manifest: {len(manifest)} entries")

    try:
        episode_ids = find_episode_pairs()
    except PromotionInputError as error:
        log(f"ERROR: refusing auto-promotion with invalid evidence paths: {error}")
        return 2
    if args.episode:
        needles = args.episode
        episode_ids = [
            e for e in episode_ids if any(n in e for n in needles)
        ]
    if not episode_ids:
        log("ERROR: no episodes have BOTH a .draft.json AND a .dai-rediff.json in Drafts/")
        return 2
    log(f"candidate episodes: {len(episode_ids)}")

    try:
        rejects_idx = load_rejects()
    except RejectLedgerError as error:
        log(f"ERROR: {error}")
        return 2
    if rejects_idx:
        log(
            f"audit-rejects: {sum(len(v) for v in rejects_idx.values())} prior "
            f"demoted span(s) across {len(rejects_idx)} episode(s); these will "
            f"be vetoed before evaluation."
        )

    try:
        canonical_names = set(load_canonical_manifest(ANNOTATIONS))
    except Exception as error:
        log(f"ERROR: {error}")
        return 2

    started = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    per_episode_summary: list[dict] = []
    rule_totals: dict[str, int] = {"R1": 0, "R2": 0, "R3": 0}
    audit_totals: dict[int, int] = {1: 0, 3: 0}
    skipped_existing = 0
    written = 0
    rejected_total = 0
    audit_rejected_total = 0
    f_past_rejected_total = 0
    r3_ceiling_rejected_total = 0
    planned: dict[str, dict] = {}
    planned_results: list[tuple[dict, int]] = []

    for ep in episode_ids:
        try:
            result = process_episode(ep, pipeline_idx, manifest, rejects_idx=rejects_idx)
        except PromotionInputError as error:
            log(f"ERROR: refusing auto-promotion with invalid evidence: {error}")
            return 2
        audit_rejected_total += result.get("audit_rejected", 0)
        f_past_rejected_total += result.get("f_past_rejected", 0)
        r3_ceiling_rejected_total += result.get("r3_ceiling_rejected", 0)
        promoted_clusters = result.pop("_promoted_clusters")
        target_fingerprint = result.pop("_target_fingerprint")
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

        try:
            annotation = build_annotation(
                ep,
                promoted_clusters,
                pipeline_idx,
                manifest,
                target_fingerprint,
            )
            validate_canonical_annotation(annotation, filename=f"{ep}.json")
        except Exception as error:
            log(f"ERROR: refusing invalid auto-promotion for {ep}: {error}")
            return 2
        out_path = ANNOTATIONS / f"{ep}.json"
        action: str
        if out_path.exists() and not args.force:
            if out_path.name not in canonical_names:
                log(
                    f"ERROR: {out_path.relative_to(REPO)} exists outside the canonical "
                    "manifest; inspect it and rerun with --force to enroll a replacement"
                )
                return 2
            skipped_existing += 1
            action = "skip-existing"
            log(
                f"  [SKIP-EXISTING] {ep}: {out_path.relative_to(REPO)} already "
                f"exists (use --force to overwrite). Would have promoted "
                f"{len(annotation['ad_windows'])} span(s)."
            )
        elif args.dry_run:
            action = "dry-run"
            log(
                f"  [DRY-RUN] {ep}: would write {len(annotation['ad_windows'])} ad_windows "
                f"(rules: {result['rule_counts']})"
            )
        else:
            action = "planned"
            planned[out_path.name] = annotation
            planned_results.append((result, len(annotation["ad_windows"])))
        result["wrote"] = action == "wrote"
        result["action"] = action
        result["annotation_path"] = str(out_path.relative_to(REPO))
        per_episode_summary.append(result)

    if planned:
        try:
            published = commit_canonical_annotations(
                ANNOTATIONS,
                planned,
                force=args.force,
                precommit=auto_promotion_precommit(manifest=manifest),
            )
        except Exception as error:
            log(f"ERROR: canonical auto-promotion transaction failed: {error}")
            return 2
        written = len(published)
        for result, promoted_count in planned_results:
            result["wrote"] = True
            result["action"] = "wrote"
            log(
                f"  [WROTE] {result['annotation_path']}: {promoted_count} ad_windows "
                f"(rules: {result['rule_counts']}, "
                f"audit-priority: {result['audit_counts']})"
            )

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
            "audit_rejected_clusters": audit_rejected_total,
            "f_past_rejected_clusters": f_past_rejected_total,
            "r3_ceiling_rejected_clusters": r3_ceiling_rejected_total,
            "rule_counts": rule_totals,
            "audit_priority_counts": audit_totals,
        },
        "episodes": per_episode_summary,
    }
    try:
        _atomic_write_json(
            DIAG_OUT,
            summary,
            label="auto-promotion diagnostics output",
        )
    except PromotionInputError as error:
        log(f"ERROR: refusing unsafe diagnostics publication: {error}")
        return 2
    log(
        f"\nsummary: episodes={len(episode_ids)} "
        f"written={written} skipped-existing={skipped_existing} "
        f"rejected-clusters={rejected_total} "
        f"audit-rejected={audit_rejected_total} "
        f"f-past-rejected={f_past_rejected_total} "
        f"r3-ceiling-rejected={r3_ceiling_rejected_total}"
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
