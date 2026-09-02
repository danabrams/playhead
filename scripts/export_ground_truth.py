#!/usr/bin/env python3
"""Reduce a verdict-bearing device pull to a small, tracked ground-truth fixture.

playhead-078u.  The project's only verdict-bearing data is the owner's by-ear
corrections.  They cost a human listening to whole episodes in real time and
they cannot be regenerated from anything.  Until this script they survived only
as an 84 MB SQLite file in an untracked directory, one copy, on one machine —
and before that, in `/private/tmp` under a session UUID no process owned, beside
32 sibling files that had already been truncated to zero bytes.

WHAT THIS EXPORTS, and why the reduction is the point:

  * `correction_events` — the ground truth itself.  Every column.  These carry
    no audio and no transcript: timestamps, UUIDs, correction types, a feed URL.
  * `analysis_assets` — ONLY the assets some correction references, and only
    identity/duration columns.  An episode title and a feed URL are public
    catalogue metadata.
  * `ad_windows` — the detector's verdicts on those same assets, so a precision
    or recall number can be recomputed against the corrections rather than
    trusted.  `evidenceText` is DROPPED (see below).

WHAT IT REFUSES TO EXPORT.  `evidenceText` holds a verbatim quote from the
episode audio (measured: up to 200 characters), and `transcript_chunks` is
transcript in full.  This repository is PUBLIC (`danabrams/playhead`), and the
standing mandate is that content never leaves the device while FACTS about a
show — "there is a commercial from 91 s to 153 s" — are fine to share (memory
project_legal_ondevice, the 2026-07-17 refinement).  A timestamp is a fact; the
sentence spoken at that timestamp is content.

`evidenceText` LIVES IN TWO PLACES AND THE SECOND ONE IS THE TRAP.  It is a
column on `ad_windows`, and it is also nested inside the JSON that
`correction_events.targetRefsJSON` carries, at
`explicitFeedbackDetectionProjection.evidenceText`.  The first version of this
script dropped the column, asserted "no content", passed — and wrote four
verbatim quotes into the fixture through the second path, because the assertion
inspected only the top-level keys of a row.  That is this repository's standing
defect class exactly: a guard naming an ABSENCE whose false branch makes no
claim (it is also playhead-g58r, one level up, in the diagnostics checklist).

So the rule here is by KEY NAME AT ANY DEPTH, not by column: `scrub` walks
nested dicts, lists, and any string column that parses as JSON, and
`assert_no_content` re-walks the finished payload the same way.  The counts are
measured, not assumed — `evidenceText` is the ONLY key path in these blobs whose
value is ever longer than 40 characters, and the only content column on
`ad_windows`.  `test_export_ground_truth` re-introduces the defect to prove the
rail fires.

DETERMINISM.  Rows are ordered by primary key and JSON is written with sorted
keys, so re-running against the same database is byte-identical.  A fixture that
changes only when the DATA changed is the only kind whose diff can be read.

Usage:
    python3 scripts/export_ground_truth.py <pull.sqlite> -o <fixture.json> \
        [--label 2026-08-11-owner-corrections]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

# Keys that carry episode CONTENT rather than facts about it, banned AT ANY
# DEPTH: as a column, as a nested object key, and inside a string column that
# holds JSON. One flat set, deliberately — a per-table rule is what let the
# nested copy through the first time.
CONTENT_KEYS: frozenset[str] = frozenset({"evidenceText"})

# Tables that are content in their entirety and are never read at all.
CONTENT_TABLES: frozenset[str] = frozenset({"transcript_chunks", "decoded_spans"})

# Row lists in the payload. Named so the guard walks data and not the metadata
# block that legitimately mentions the banned key names.
ROW_TABLES: tuple[str, ...] = ("correction_events", "analysis_assets", "ad_windows")

# The asset columns worth keeping: identity, provenance, duration. Deliberately
# a list rather than "everything except", so a new column added upstream is
# absent from the fixture until somebody chooses to add it.
ASSET_COLUMNS: tuple[str, ...] = (
    "id",
    "episodeId",
    "episodeTitle",
    "sourceURL",
    "assetFingerprint",
    "episodeDurationSec",
    "analysisState",
    "analysisVersion",
    "createdAt",
)

EXPORT_SCHEMA_VERSION = 1


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _rows(db: sqlite3.Connection, sql: str, args: tuple = ()) -> list[dict]:
    cursor = db.execute(sql, args)
    names = [d[0] for d in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _columns_of(db: sqlite3.Connection, table: str) -> list[str]:
    return [r["name"] for r in _rows(db, f"PRAGMA table_info({table})")]


def _maybe_json(value: str):
    """Return the parsed object if `value` is a JSON object/array, else None.

    Scalars are deliberately not treated as JSON: the string "null", and any
    bare number, parse successfully but hold nothing to walk.
    """
    stripped = value.strip()
    if not stripped or stripped[0] not in "{[":
        return None
    try:
        return json.loads(stripped)
    except (ValueError, TypeError):
        return None


def scrub(value):
    """Remove every banned key at any depth, including inside JSON strings.

    A string column that holds JSON is parsed, scrubbed and re-serialized with
    sorted keys — ALWAYS, not only when something was removed. Re-serializing
    conditionally would make a blob's byte formatting depend on whether it
    happened to contain content, so two rows carrying the same facts would
    differ in the fixture and the diff would stop being readable. Uniform
    normalization also makes the `excluded` note uniformly true: no blob here is
    byte-identical to its database value, so nobody can mistake one for verbatim.
    """
    if isinstance(value, dict):
        return {k: scrub(v) for k, v in value.items() if k not in CONTENT_KEYS}
    if isinstance(value, list):
        return [scrub(v) for v in value]
    if isinstance(value, str):
        parsed = _maybe_json(value)
        if parsed is not None:
            return json.dumps(scrub(parsed), sort_keys=True, ensure_ascii=False)
        return value
    return value


def _walk_for_content(value, path: str) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            here = f"{path}.{key}"
            if key in CONTENT_KEYS:
                found.append(here)
            found.extend(_walk_for_content(child, here))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(_walk_for_content(child, f"{path}[{index}]"))
    elif isinstance(value, str):
        parsed = _maybe_json(value)
        if parsed is not None:
            found.extend(_walk_for_content(parsed, f"{path}(json)"))
    return found


def assert_no_content(payload: dict) -> None:
    """Fail loudly if a banned key reached the payload, at any depth.

    This is the enforcement, not a comment. It inspects the SHAPE of what is
    about to be written, so it is blind to how the rows were selected — a new
    column, a new nested projection, or a new embedded blob is covered without
    anyone remembering to add it here.

    It walks only the row tables, because the `excluded` metadata block names
    the banned keys on purpose and must not trip its own guard.
    """
    leaked: list[str] = []
    for table in ROW_TABLES:
        leaked.extend(_walk_for_content(payload.get(table, []), table))
    if leaked:
        raise ValueError(
            "refusing to export: content key(s) present at "
            + ", ".join(sorted(leaked))
        )
    for table in CONTENT_TABLES:
        if table in payload:
            raise ValueError(f"refusing to export {table}: content table")


def build_payload(db_path: Path, label: str) -> dict:
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        corrections = scrub(_rows(db, "SELECT * FROM correction_events ORDER BY id"))
        if not corrections:
            raise ValueError(
                f"{db_path} holds no correction_events — nothing to preserve. "
                "A pull with zero corrections is not ground truth; check the "
                "file is the one you meant."
            )

        referenced = sorted({c["analysisAssetId"] for c in corrections})
        placeholders = ",".join("?" for _ in referenced)

        available = set(_columns_of(db, "analysis_assets"))
        asset_columns = [c for c in ASSET_COLUMNS if c in available]
        assets = _rows(
            db,
            f"SELECT {', '.join(asset_columns)} FROM analysis_assets "
            f"WHERE id IN ({placeholders}) ORDER BY id",
            tuple(referenced),
        )

        window_columns = [
            c for c in _columns_of(db, "ad_windows") if c not in CONTENT_KEYS
        ]
        windows = scrub(
            _rows(
                db,
                f"SELECT {', '.join(window_columns)} FROM ad_windows "
                f"WHERE analysisAssetId IN ({placeholders}) ORDER BY id",
                tuple(referenced),
            )
        )
    finally:
        db.close()

    payload = {
        "export_schema_version": EXPORT_SCHEMA_VERSION,
        "label": label,
        "bead": "playhead-078u",
        "source": {
            "filename": db_path.name,
            "sha256": sha256_of(db_path),
            "correction_events": len(corrections),
            "assets_referenced": len(assets),
            "ad_windows": len(windows),
        },
        "excluded": {
            "keys_at_any_depth": sorted(CONTENT_KEYS),
            "tables": sorted(CONTENT_TABLES),
            "why": (
                "This repository is public. Timestamps and identifiers are "
                "facts about a show and may be shared; the words spoken at "
                "those timestamps are content and may not."
            ),
            "note": (
                "A banned key is removed wherever it appears, including inside "
                "JSON carried in a string column such as "
                "correction_events.targetRefsJSON. Such a blob is re-serialized "
                "with sorted keys after scrubbing, so it is NOT byte-identical "
                "to the database value it came from."
            ),
        },
        "correction_events": corrections,
        "analysis_assets": assets,
        "ad_windows": windows,
    }
    assert_no_content(payload)
    return payload


def render(payload: dict) -> str:
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path, help="a device-pull analysis.sqlite")
    parser.add_argument("-o", "--out", type=Path, required=True)
    parser.add_argument(
        "--label",
        default=None,
        help="fixture label; defaults to the output file's stem",
    )
    args = parser.parse_args(argv)

    if not args.database.exists():
        print(f"no such database: {args.database}", file=sys.stderr)
        return 2

    payload = build_payload(args.database, args.label or args.out.stem)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render(payload), encoding="utf-8")

    source = payload["source"]
    print(
        f"wrote {args.out}\n"
        f"  correction_events {source['correction_events']}\n"
        f"  assets referenced {source['assets_referenced']}\n"
        f"  ad_windows        {source['ad_windows']}\n"
        f"  source sha256     {source['sha256']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
