#!/usr/bin/env python3
"""Build the sanitized 2026-05-06 dogfood fixture (playhead-hygc.1.1).

Reads RAW dogfood evidence that lives outside the repo and emits ONE scrubbed
JSON file (analysis-health.json) containing only the structural facts the
downstream hygc.1.2..1.9 beads need to assert against.

NOTHING about the raw evidence (episode IDs, URLs, transcripts, FM payloads,
hashes, install salts, source URLs, prompt text, episode titles, device paths)
appears in the output. All identifiers are stable synthetic IDs.

Source files (read-only, NOT committed):
  - playhead-dogfood-diagnostics-2026-05-06T23-46-51Z.json   (activity_snapshot)
  - .xcappdata/.../analysis.sqlite                          (assets/chunks/ad_windows/corrections/learning tables; shadow_fm_responses count)
  - .xcappdata/.../bg-task-log.jsonl                         (background-task event counts)

(`shadow-decisions.jsonl` and `asset-lifecycle-log.jsonl` are NOT read — the
shadow FM response count is sourced from the `shadow_fm_responses` SQL table.)

Run: python3 scripts/build-dogfood-fixture-2026-05-06.py [out_path]
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

DEFAULT_DIAG = "/Users/dabrams/playhead/playhead-dogfood-diagnostics-2026-05-06T23-46-51Z.json"
DEFAULT_XCAPPDATA = "/Users/dabrams/playhead/com.playhead.app 2026-05-06 18:39.47.688.xcappdata"

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = (
    REPO_ROOT / "PlayheadTests" / "Fixtures" / "Dogfood" / "2026-05-06" / "analysis-health.json"
)


def diag_path() -> str:
    return os.environ.get("PLAYHEAD_HYGC_DIAG", DEFAULT_DIAG)


def xcappdata_path() -> str:
    return os.environ.get("PLAYHEAD_HYGC_XCAPPDATA", DEFAULT_XCAPPDATA)


def documents_dir() -> str:
    return os.path.join(xcappdata_path(), "AppData", "Documents")


def sqlite_path() -> str:
    return os.path.join(documents_dir(), "ExportedAnalysisStore", "analysis.sqlite")


def bg_task_log_path() -> str:
    return os.path.join(documents_dir(), "bg-task-log.jsonl")


# ---------------------------------------------------------------------------
# Activity snapshot (from dogfood diagnostics JSON)
# ---------------------------------------------------------------------------

def build_activity_rows(diag: dict[str, Any]) -> list[dict[str, Any]]:
    """One scrubbed entry per Activity row.

    Drops episode_id_hash, latest_session timestamps, capability_snapshot
    contents, audio paths. Keeps section/status/is_running/queue_position/
    cached_audio_present and pipeline numerics."""
    raw = diag["activity_snapshot"]["rows"]
    out: list[dict[str, Any]] = []
    for index, row in enumerate(raw, start=1):
        synthetic_id = f"activity_{index:03d}"
        pipeline = row.get("pipeline", {}) or {}
        status = row.get("status", {}) or {}
        analysis_asset = row.get("analysis_asset", {}) or {}
        out.append({
            "id": synthetic_id,
            "section": row.get("section"),
            "queue_position": row.get("queue_position"),
            "is_running": bool(row.get("is_running", False)),
            "cached_audio_present": bool(row.get("cached_audio_present", False)),
            "status": {
                "disposition": status.get("disposition"),
                "reason": status.get("reason"),
                "hint": status.get("hint"),
                "playback_readiness": status.get("playback_readiness"),
            },
            "analysis_state": analysis_asset.get("analysis_state"),
            "pipeline": {
                "download_fraction": pipeline.get("download_fraction"),
                "download_percent": pipeline.get("download_percent"),
                "analysis_fraction": pipeline.get("analysis_fraction"),
                "analysis_percent": pipeline.get("analysis_percent"),
                "analysis_source": pipeline.get("analysis_source"),
                "analysis_watermark_sec": pipeline.get("analysis_watermark_sec"),
                "transcript_fraction": pipeline.get("transcript_fraction"),
                "transcript_percent": pipeline.get("transcript_percent"),
                "transcript_source": pipeline.get("transcript_source"),
                "transcript_watermark_sec": pipeline.get("transcript_watermark_sec"),
                "transcript_covered_sec": pipeline.get("transcript_covered_sec"),
                "fast_transcript_watermark_sec": pipeline.get("fast_transcript_watermark_sec"),
                "feature_coverage_end_sec": pipeline.get("feature_coverage_end_sec"),
                "final_pass_coverage_end_sec": pipeline.get("final_pass_coverage_end_sec"),
                "episode_duration_sec": pipeline.get("episode_duration_sec"),
            },
        })
    return out


# ---------------------------------------------------------------------------
# Analysis assets + per-asset chunk maxima + ad windows (from sqlite)
# ---------------------------------------------------------------------------

def build_analysis_assets(conn: sqlite3.Connection) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Returns (asset_rows, raw_uuid_to_synthetic_id)."""
    cur = conn.execute(
        "SELECT id, analysisState, episodeDurationSec, fastTranscriptCoverageEndTime, "
        "featureCoverageEndTime, finalPassCoverageEndTime, confirmedAdCoverageEndTime, "
        "terminalReason FROM analysis_assets ORDER BY createdAt, id"
    )
    raw_to_syn: dict[str, str] = {}
    out: list[dict[str, Any]] = []
    for index, (
        raw_id, state, duration, fast_wm, feat_wm, final_wm, ad_wm, terminal,
    ) in enumerate(cur, start=1):
        syn = f"asset_{index:03d}"
        raw_to_syn[raw_id] = syn
        out.append({
            "id": syn,
            "analysis_state": state,
            "episode_duration_sec": duration,
            "fast_transcript_coverage_end_sec": fast_wm,
            "feature_coverage_end_sec": feat_wm,
            "final_pass_coverage_end_sec": final_wm,
            "confirmed_ad_coverage_end_sec": ad_wm,
            "terminal_reason": terminal,
        })
    return out, raw_to_syn


def build_chunk_maxima(conn: sqlite3.Connection, raw_to_syn: dict[str, str]) -> list[dict[str, Any]]:
    cur = conn.execute(
        "SELECT analysisAssetId, pass, MAX(endTime), COUNT(*) FROM transcript_chunks "
        "GROUP BY analysisAssetId, pass ORDER BY analysisAssetId, pass"
    )
    out = []
    for asset_id, pass_name, max_end, count in cur:
        if asset_id not in raw_to_syn:
            continue
        out.append({
            "asset_id": raw_to_syn[asset_id],
            "pass": pass_name,
            "max_end_time_sec": max_end,
            "chunk_count": count,
        })
    return out


def build_ad_window_summaries(conn: sqlite3.Connection, raw_to_syn: dict[str, str]) -> list[dict[str, Any]]:
    cur = conn.execute(
        "SELECT analysisAssetId, COUNT(*), "
        "SUM(CASE WHEN decisionState='userMarked' THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN decisionState!='userMarked' THEN 1 ELSE 0 END), "
        "MAX(endTime) "
        "FROM ad_windows GROUP BY analysisAssetId ORDER BY analysisAssetId"
    )
    out = []
    for asset_id, total, user_marked, algorithmic, max_end in cur:
        if asset_id not in raw_to_syn:
            continue
        out.append({
            "asset_id": raw_to_syn[asset_id],
            "total_count": total,
            "user_marked_count": user_marked or 0,
            "algorithmic_count": algorithmic or 0,
            "max_end_time_sec": max_end,
        })
    return out


# Asset-bound CorrectionScope prefixes (see UserCorrectionStore.swift). Only
# these prefixes can be safely sanitized via raw_to_syn — their second token is
# always the analysisAssetId UUID. All other prefixes (sponsorOnShow,
# phraseOnShow, campaignOnShow, domainOwnershipOnShow, jingleOnShow) embed a
# podcastId + a free-form sponsor/phrase/campaign/domain/jingle string, neither
# of which has a synthetic mapping in this fixture — those rows are DROPPED
# rather than risk leaking sponsor/phrase text downstream.
ASSET_BOUND_SCOPE_PREFIXES = ("exactSpan", "exactTimeSpan")


def build_correction_rows(conn: sqlite3.Connection, raw_to_syn: dict[str, str]) -> list[dict[str, Any]]:
    """Reduce correction_events to (correction_type, scope-with-synthetic-asset-id, count).

    Scope strings of the form `exactTimeSpan:<UUID>:<start>:<end>` (or
    `exactSpan:<UUID>:<lower>:<upper>`) get their UUID swapped for the
    synthetic asset id. Counts preserve the duplicate structure: a single
    (type, scope) row that appeared 4 times in raw becomes `count: 4` here.

    Non-asset-bound scopes (sponsorOnShow / phraseOnShow / campaignOnShow /
    domainOwnershipOnShow / jingleOnShow) are DROPPED with a count to stderr —
    they would otherwise embed sponsor/phrase/podcastId text which has no
    synthetic mapping in this fixture.
    """
    counter: Counter[tuple[str, str]] = Counter()
    dropped: Counter[str] = Counter()
    cur = conn.execute("SELECT correctionType, scope FROM correction_events")
    for correction_type, scope in cur:
        ctype = correction_type or "unknown"
        rewritten = rewrite_scope(scope, raw_to_syn)
        if rewritten is None:
            prefix = (scope or "").split(":", 1)[0] or "<empty>"
            dropped[prefix] += 1
            continue
        counter[(ctype, rewritten)] += 1
    if dropped:
        print(
            f"[build-dogfood-fixture] dropped {sum(dropped.values())} non-asset-bound "
            f"correction rows (per-prefix: {dict(dropped)})",
            file=sys.stderr,
        )
    rows = [
        {"correction_type": ctype, "scope": scope, "count": count}
        for (ctype, scope), count in sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    return rows


def rewrite_scope(scope: str, raw_to_syn: dict[str, str]) -> str | None:
    """Sanitize a correction scope string for the fixture.

    Returns the rewritten string when the scope is asset-bound and its UUID
    successfully resolves to a synthetic id; returns ``None`` when the scope
    is non-asset-bound (sponsor/phrase/campaign/domain/jingle) or when the
    embedded UUID is unknown. Callers MUST drop ``None`` rows rather than
    emit them — the alternative is leaking sponsor/phrase text or unmapped
    raw UUIDs.
    """
    if not scope:
        return None
    parts = scope.split(":")
    if len(parts) < 2:
        return None
    if parts[0] not in ASSET_BOUND_SCOPE_PREFIXES:
        return None
    if parts[1] not in raw_to_syn:
        return None
    parts[1] = raw_to_syn[parts[1]]
    return ":".join(parts)


# ---------------------------------------------------------------------------
# Background task event counts (from bg-task-log.jsonl)
# ---------------------------------------------------------------------------

BG_CATEGORIES_OF_INTEREST = {
    "com.playhead.app.analysis.backfill",
    "com.playhead.app.analysis.continued",
    "com.playhead.app.preanalysis.recovery",
    "com.playhead.app.feed-refresh",
}


def build_background_task_summary(path: str) -> dict[str, Any]:
    overall: Counter[str] = Counter()
    by_cat: dict[str, Counter[str]] = defaultdict(Counter)
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            event = row.get("event_type") or row.get("event")
            if not event:
                continue
            overall[event] += 1
            cat = row.get("category") or row.get("identifier")
            if cat:
                by_cat[cat][event] += 1
    return {
        "overall": dict(sorted(overall.items())),
        "by_category": {
            cat: dict(sorted(by_cat[cat].items()))
            for cat in sorted(by_cat)
        },
    }


# ---------------------------------------------------------------------------
# Learning-table counts + shadow FM response count
# ---------------------------------------------------------------------------

LEARNING_TABLES = (
    "sponsor_knowledge_entries",
    "training_examples",
    "ad_copy_fingerprints",
    "boundary_priors",
    "implicit_feedback_events",
    "knowledge_candidate_events",
    "music_bracket_trust",
)


def build_learning_table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    out = {}
    for table in LEARNING_TABLES:
        cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
        (n,) = cur.fetchone()
        out[table] = n
    return out


def build_shadow_fm_count(conn: sqlite3.Connection) -> int:
    cur = conn.execute("SELECT COUNT(*) FROM shadow_fm_responses")
    (n,) = cur.fetchone()
    return n


# ---------------------------------------------------------------------------
# Top-level
# ---------------------------------------------------------------------------

FIXTURE_SCHEMA_VERSION = 1

# Scrubber-audit gate. Before writing the fixture we serialize it to JSON and
# search the bytes for anything that looks like raw user data. Any hit aborts
# the write — the operator should never be able to overwrite the committed
# fixture with a payload that would also fail
# `DogfoodAnalysisHealthFixtureTests.fixtureIsScrubbed`.
#
# The substring list mirrors that test, plus a few extra brand strings the
# May 6 capture is known to contain (so a regression in the per-table
# sanitizer surfaces here, not in CI).
FORBIDDEN_SUBSTRINGS: tuple[tuple[str, str], ...] = (
    # Raw analysisAssetId UUID prefixes from the May 6 capture.
    ("9C109975", "raw analysisAssetId UUID prefix"),
    ("8A9DFC82", "raw analysisAssetId UUID prefix"),
    ("C75C2E85", "raw analysisAssetId UUID prefix"),
    ("E8F0F867", "raw analysisAssetId UUID prefix"),
    # Feed / network identifiers.
    ("flightcast", "raw feed URL component"),
    ("simplecast", "raw feed URL component"),
    ("libsyn", "raw feed URL component"),
    ("acast", "raw feed URL component"),
    ("https://", "raw URL"),
    ("http://", "raw URL"),
    ("feed://", "raw feed URL scheme"),
    # Device-specific paths and developer-machine paths.
    ("/var/mobile/", "device-specific filesystem path"),
    ("/private/", "device-specific filesystem path"),
    ("/Users/", "developer-machine filesystem path"),
    ("~/Library", "device-specific filesystem path"),
    ("~/Containers", "device-specific filesystem path"),
    ("ApplicationSupport", "device-specific filesystem path"),
    ("AudioCache", "device-specific filesystem path"),
    # Raw-bundle field names that should never appear in the sanitized output.
    ("fmResponseBase64", "FM response payload field name"),
    ("episode_id_hash", "raw activity-row hash field name"),
    ("session_id", "raw session UUID field name"),
    ("installation_id", "raw install identifier field name"),
    ("BuildProvenance", "build-stamp file referenced in raw bundle"),
    # FM payload role markers (in case any prompt / response text leaks).
    ("<system>", "FM payload role marker"),
    ("<user>", "FM payload role marker"),
    ("<assistant>", "FM payload role marker"),
    # Show / sponsor brand keywords known to appear in sponsor/phrase
    # corrections from the dogfood capture. None of these are sanitized via
    # raw_to_syn — their presence in the fixture means a non-asset-bound
    # correction scope leaked through.
    ("Squarespace", "sponsor brand string"),
    ("BetterHelp", "sponsor brand string"),
    ("MeUndies", "sponsor brand string"),
    ("Mint Mobile", "sponsor brand string"),
    ("Conan", "show title fragment"),
    ("Diary of a CEO", "show title fragment"),
    # Non-asset-bound CorrectionScope prefixes (see UserCorrectionStore.swift).
    # These MUST be filtered upstream by `rewrite_scope` because their payload
    # has no synthetic mapping; if any survive into the serialized fixture the
    # audit gate must abort the write. This list mirrors the Swift
    # `fixtureIsScrubbed` test's allowlist so the gate stays in lock-step.
    ("sponsorOnShow", "non-asset-bound CorrectionScope prefix"),
    ("phraseOnShow", "non-asset-bound CorrectionScope prefix"),
    ("campaignOnShow", "non-asset-bound CorrectionScope prefix"),
    ("domainOwnershipOnShow", "non-asset-bound CorrectionScope prefix"),
    ("jingleOnShow", "non-asset-bound CorrectionScope prefix"),
)

UUID_PATTERN = re.compile(r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}")
# Case-insensitive — a future capture might emit hex hashes in mixed case.
SHA256_PATTERN = re.compile(r"\b[0-9A-Fa-f]{64}\b")


def audit_scrubbed(fixture_text: str) -> list[str]:
    """Return a list of human-readable issues for any banned content.

    An empty list means the fixture passes the scrub gate. All substring
    matches are case-INSENSITIVE so a sponsor name lowercased by some
    downstream normalizer still triggers (e.g. ``squarespace`` vs
    ``Squarespace``). This must stay in lock-step with the
    ``fixtureIsScrubbed`` Swift test so a regen that passes the gate also
    passes CI.
    """
    issues: list[str] = []
    haystack_lower = fixture_text.lower()
    for token, why in FORBIDDEN_SUBSTRINGS:
        if token.lower() in haystack_lower:
            issues.append(f"contains '{token}' ({why})")
    if (m := UUID_PATTERN.search(fixture_text)) is not None:
        issues.append(f"contains UUID-shaped string: '{m.group(0)}'")
    if (m := SHA256_PATTERN.search(fixture_text)) is not None:
        issues.append(f"contains SHA-256-shaped hex: '{m.group(0)}'")
    return issues


def preflight_inputs() -> None:
    """Fail fast with a precise message when raw inputs are missing."""
    missing: list[str] = []
    for label, path in (
        ("dogfood diagnostics JSON (PLAYHEAD_HYGC_DIAG)", diag_path()),
        ("xcappdata bundle (PLAYHEAD_HYGC_XCAPPDATA)", xcappdata_path()),
        ("analysis.sqlite", sqlite_path()),
        ("bg-task-log.jsonl", bg_task_log_path()),
    ):
        if not os.path.exists(path):
            missing.append(f"  - {label}: {path}")
    if missing:
        print(
            "[build-dogfood-fixture] missing required inputs:\n"
            + "\n".join(missing)
            + "\nSet PLAYHEAD_HYGC_DIAG / PLAYHEAD_HYGC_XCAPPDATA env vars to point at the raw "
              "evidence (see README in PlayheadTests/Fixtures/Dogfood/2026-05-06/).",
            file=sys.stderr,
        )
        sys.exit(2)


def build(out_path: Path) -> None:
    preflight_inputs()

    with open(diag_path(), "r", encoding="utf-8") as f:
        diag = json.load(f)

    activity_rows = build_activity_rows(diag)

    conn = sqlite3.connect(sqlite_path())
    try:
        analysis_assets, raw_to_syn = build_analysis_assets(conn)
        chunk_maxima = build_chunk_maxima(conn, raw_to_syn)
        ad_window_summaries = build_ad_window_summaries(conn, raw_to_syn)
        correction_rows = build_correction_rows(conn, raw_to_syn)
        learning_table_counts = build_learning_table_counts(conn)
        shadow_fm_response_count = build_shadow_fm_count(conn)
    finally:
        conn.close()

    bg_task_summary = build_background_task_summary(bg_task_log_path())

    fixture = {
        "schema_version": FIXTURE_SCHEMA_VERSION,
        "captured_on": "2026-05-06",
        "source_diagnostics_filename": "playhead-dogfood-diagnostics-2026-05-06T23-46-51Z.json",
        "activity_snapshot": {
            "row_count": len(activity_rows),
            "rows": activity_rows,
        },
        "analysis_assets": analysis_assets,
        "transcript_chunk_maxima": chunk_maxima,
        "ad_window_summaries": ad_window_summaries,
        "correction_rows": correction_rows,
        "background_task_events": bg_task_summary,
        "learning_table_counts": learning_table_counts,
        "shadow_fm_response_count": shadow_fm_response_count,
    }

    serialized = json.dumps(fixture, indent=2, sort_keys=True) + "\n"
    issues = audit_scrubbed(serialized)
    if issues:
        print(
            "[build-dogfood-fixture] REFUSING to overwrite "
            f"{out_path} — fixture failed scrub audit:",
            file=sys.stderr,
        )
        for issue in issues:
            print(f"  - {issue}", file=sys.stderr)
        sys.exit(3)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(serialized)
    print(f"wrote {out_path}")


def main() -> None:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUT
    build(out)


if __name__ == "__main__":
    main()
