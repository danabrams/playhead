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
  - .xcappdata/.../analysis.sqlite                          (assets/chunks/ad_windows/corrections/learning tables)
  - .xcappdata/.../bg-task-log.jsonl                         (background-task event counts)
  - .xcappdata/.../shadow-decisions.jsonl                    (shadow FM response COUNT only)
  - .xcappdata/.../asset-lifecycle-log.jsonl                 (lifecycle event counts; not strictly needed but cheap)

Run: python3 scripts/build-dogfood-fixture-2026-05-06.py [out_path]
"""
from __future__ import annotations

import json
import os
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


def shadow_log_path() -> str:
    return os.path.join(documents_dir(), "shadow-decisions.jsonl")


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


def build_correction_rows(conn: sqlite3.Connection, raw_to_syn: dict[str, str]) -> list[dict[str, Any]]:
    """Reduce correction_events to (correction_type, scope-with-synthetic-asset-id, count).

    Scope strings of the form `exactTimeSpan:<UUID>:<start>:<end>` get their
    UUID swapped for the synthetic asset id. Counts preserve the duplicate
    structure: a single (type, scope) row that appeared 4 times in raw becomes
    `count: 4` here.
    """
    counter: Counter[tuple[str, str]] = Counter()
    cur = conn.execute("SELECT correctionType, scope FROM correction_events")
    for correction_type, scope in cur:
        ctype = correction_type or "unknown"
        rewritten = rewrite_scope(scope, raw_to_syn)
        counter[(ctype, rewritten)] += 1
    rows = [
        {"correction_type": ctype, "scope": scope, "count": count}
        for (ctype, scope), count in sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    return rows


def rewrite_scope(scope: str, raw_to_syn: dict[str, str]) -> str:
    """Replace any embedded raw asset UUID with its synthetic id.

    Production scopes look like `exactTimeSpan:<UUID>:<start>:<end>`. We split
    on ':' and substitute the second token if it matches a known UUID.
    """
    if not scope:
        return scope
    parts = scope.split(":")
    if len(parts) >= 2 and parts[1] in raw_to_syn:
        parts[1] = raw_to_syn[parts[1]]
        return ":".join(parts)
    return scope


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


def build(out_path: Path) -> None:
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

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(fixture, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"wrote {out_path}")


def main() -> None:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUT
    build(out)


if __name__ == "__main__":
    main()
