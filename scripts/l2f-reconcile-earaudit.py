#!/usr/bin/env python3
"""Reconcile a completed retained-audio ear audit into partial silver labels.

The source ledger and immutable review remain the evidence of record. This
tool publishes only the regions they actually establish: reviewed full ad
breaks, tight-ad presence anchors, and exact content vetoes. Everything else
in each episode remains unknown.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
import pathlib
import re
import sys
import tempfile
from collections import Counter, defaultdict
from decimal import Decimal
from typing import Iterator, NamedTuple

from convert_annotations_to_chapter_goldens import path_has_symlink_component
from l2f_canonical_manifest import (
    CanonicalCorpusError,
    canonical_manifest_lock,
    load_reject_ledger,
)


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_LEDGER = ROOT / "TestFixtures/Corpus/Audits/rediff-r3-2026-07-07.jsonl"
DEFAULT_REVIEW = ROOT / (
    "TestFixtures/Corpus/Audits/earaudit-boundary-review-2026-07-12-"
    "6b11a85754db5d1ea2fb0243b3b53fd2c61375e7ba54ad42c444102690b98e99.json"
)
DEFAULT_REJECTS = ROOT / "TestFixtures/Corpus/Snapshots/audit-rejects.jsonl"
DEFAULT_OUTPUT_DIR = ROOT / "TestFixtures/Corpus/Evaluations"
DEFAULT_ANNOTATIONS_DIR = ROOT / "TestFixtures/Corpus/Annotations"

FINGERPRINT_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
UTC_TIMESTAMP_PATTERN = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z\Z"
)
CALENDAR_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
CONTENT_ADDRESS_SUFFIX_PATTERN = re.compile(r"-([0-9a-f]{64})\.json\Z")
BOUNDARY_DISPOSITIONS = frozenset({"boundary_off", "edge_clipping"})
DISPOSITIONS = frozenset({"tight_ad", "boundary_off", "edge_clipping", "rejected"})
REVIEW_STATUSES = frozenset({"approved", "rebounded", "rejected", "unsure"})
COMPLETE_REVIEW_STATUSES = frozenset({"approved", "rebounded", "rejected"})
DUPLICATE_ENDPOINT_TOLERANCE_SECONDS = 0.5
LEDGER_FIELDS = frozenset(
    {
        "asset_binding_provenance",
        "audio_fingerprint",
        "audit_date",
        "coordinate_origin",
        "current_end_seconds",
        "current_start_seconds",
        "disposition",
        "duration_seconds",
        "episode_id",
        "id",
        "note",
        "provenance_tier",
        "raw_verdict",
        "recommendation",
        "retained_in_corpus",
        "review_asset_binding",
        "show_name",
    }
)
REVIEW_FIELDS = frozenset(
    {
        "id",
        "status",
        "proposed_start_seconds",
        "proposed_end_seconds",
        "note",
        "reviewer",
        "audio_fingerprint",
        "reviewed_at",
    }
)


class ReconciliationError(ValueError):
    """The evidence cannot be reconciled without guessing."""


class ReconcileResult(NamedTuple):
    artifact_path: pathlib.Path
    artifact_sha256: str
    artifact_changed: bool
    rejects_changed: bool
    reject_count: int


def _finite_number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReconciliationError(f"{field} must be a finite number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as error:
        raise ReconciliationError(f"{field} must be a finite number") from error
    if not math.isfinite(number):
        raise ReconciliationError(f"{field} must be a finite number")
    return number


def _safe_text(value: object, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or value != value.strip() or (not value and not allow_empty):
        raise ReconciliationError(f"{field} must be a canonical string")
    return value


def _utc_timestamp(value: object, field: str) -> str:
    timestamp = _safe_text(value, field)
    if not UTC_TIMESTAMP_PATTERN.fullmatch(timestamp):
        raise ReconciliationError(f"{field} must be a UTC timestamp")
    try:
        dt.datetime.fromisoformat(timestamp[:-1] + "+00:00")
    except ValueError as error:
        raise ReconciliationError(f"{field} must be a UTC timestamp") from error
    return timestamp


def _calendar_date(value: object, field: str) -> str:
    date_text = _safe_text(value, field)
    if not CALENDAR_DATE_PATTERN.fullmatch(date_text):
        raise ReconciliationError(f"{field} must be a canonical calendar date")
    try:
        parsed = dt.date.fromisoformat(date_text)
    except ValueError as error:
        raise ReconciliationError(f"{field} must be a canonical calendar date") from error
    if parsed.isoformat() != date_text:
        raise ReconciliationError(f"{field} must be a canonical calendar date")
    return date_text


def _validate_content_address(path: pathlib.Path, digest: str, label: str) -> None:
    match = CONTENT_ADDRESS_SUFFIX_PATTERN.search(path.name)
    if match is None or match.group(1) != digest:
        raise ReconciliationError(
            f"{label} path does not match its content address: {path}"
        )


def _read_regular_bytes(path: pathlib.Path, label: str) -> bytes:
    if path_has_symlink_component(path) or path.is_symlink() or not path.is_file():
        raise ReconciliationError(f"{label} is missing or not a regular file: {path}")
    try:
        return path.read_bytes()
    except OSError as error:
        raise ReconciliationError(f"cannot read {label} {path}: {error}") from error


def _source_path(path: pathlib.Path) -> str:
    absolute = pathlib.Path(os.path.abspath(path))
    try:
        return absolute.relative_to(ROOT).as_posix()
    except ValueError:
        return absolute.as_posix()


def _validate_output_directory(path: pathlib.Path) -> None:
    if path_has_symlink_component(path) or path.is_symlink():
        raise ReconciliationError(
            f"output directory must not contain a symbolic link: {path}"
        )
    if path.exists() and not path.is_dir():
        raise ReconciliationError(f"output directory is not a directory: {path}")
    existing_ancestor = pathlib.Path(os.path.abspath(path))
    while not existing_ancestor.exists() and existing_ancestor.parent != existing_ancestor:
        existing_ancestor = existing_ancestor.parent
    if not existing_ancestor.is_dir():
        raise ReconciliationError(
            f"output directory has a non-directory ancestor: {existing_ancestor}"
        )


def _validate_artifact_path(path: pathlib.Path) -> None:
    if path_has_symlink_component(path) or path.is_symlink():
        raise ReconciliationError(
            f"content-addressed artifact path must not contain a symbolic link: {path}"
        )
    if path.exists() and not path.is_file():
        raise ReconciliationError(
            f"content-addressed artifact path is not a regular file: {path}"
        )


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _parse_json_text(text: str, label: str) -> object:
    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise ReconciliationError(
                    f"invalid {label}: duplicate JSON object field {key!r}"
                )
            value[key] = item
        return value

    def reject_nonstandard_constant(value: str) -> object:
        raise ReconciliationError(
            f"invalid {label}: {value} must be a finite JSON number"
        )

    def finite_float(value: str) -> float:
        number = float(value)
        if not math.isfinite(number):
            raise ReconciliationError(
                f"invalid {label}: {value} must be a finite JSON number"
            )
        return number

    try:
        return json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_nonstandard_constant,
            parse_float=finite_float,
        )
    except ReconciliationError:
        raise
    except (ValueError, RecursionError) as error:
        raise ReconciliationError(f"invalid {label} JSON: {error}") from error


def _parse_json(data: bytes, label: str) -> object:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ReconciliationError(f"invalid {label} JSON: {error}") from error
    return _parse_json_text(text, label)


def load_ledger(data: bytes) -> list[dict]:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ReconciliationError(f"audit ledger is not UTF-8: {error}") from error
    rows: list[dict] = []
    seen_ids: set[str] = set()
    seen_identities: set[tuple[str, str, float, float]] = set()
    asset_metadata: dict[tuple[str, str], tuple[str, float]] = {}
    for line_number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        row = _parse_json_text(line, f"audit ledger line {line_number}")
        if not isinstance(row, dict) or set(row) != LEDGER_FIELDS:
            raise ReconciliationError(f"audit ledger line {line_number} has invalid fields")
        stable_id = _safe_text(row.get("id"), f"ledger line {line_number} id")
        episode_id = _safe_text(
            row.get("episode_id"), f"ledger line {line_number} episode_id"
        )
        if any(separator in episode_id for separator in ("/", "\\")):
            raise ReconciliationError(f"ledger line {line_number} has unsafe episode_id")
        fingerprint = row.get("audio_fingerprint")
        if not isinstance(fingerprint, str) or not FINGERPRINT_PATTERN.fullmatch(fingerprint):
            raise ReconciliationError(f"ledger line {line_number} has invalid audio fingerprint")
        start = _finite_number(
            row.get("current_start_seconds"), f"ledger line {line_number} start"
        )
        end = _finite_number(row.get("current_end_seconds"), f"ledger line {line_number} end")
        duration = _finite_number(row.get("duration_seconds"), f"ledger line {line_number} duration")
        if duration <= 0 or start < 0 or end <= start or end > duration:
            raise ReconciliationError(f"ledger line {line_number} has invalid bounds")
        if start != float(f"{start:.2f}") or end != float(f"{end:.2f}"):
            raise ReconciliationError(
                f"ledger line {line_number} bounds are not represented by its exact identity"
            )
        expected_id = f"{episode_id}@{start:.2f}-{end:.2f}"
        if stable_id != expected_id:
            raise ReconciliationError(
                f"ledger line {line_number} id does not match its exact identity"
            )
        disposition = row.get("disposition")
        if not isinstance(disposition, str) or disposition not in DISPOSITIONS:
            raise ReconciliationError(f"ledger line {line_number} has invalid disposition")
        expected_verdict = (
            "ad"
            if disposition == "tight_ad"
            else "boundary"
            if disposition == "boundary_off"
            else "content"
        )
        if row.get("raw_verdict") != expected_verdict:
            raise ReconciliationError(
                f"ledger line {line_number} disposition/verdict disagree"
            )
        show_name = _safe_text(row.get("show_name"), f"ledger line {line_number} show_name")
        for field in (
            "asset_binding_provenance",
            "coordinate_origin",
            "provenance_tier",
            "recommendation",
            "review_asset_binding",
        ):
            _safe_text(row.get(field), f"ledger line {line_number} {field}")
        _calendar_date(
            row.get("audit_date"), f"ledger line {line_number} audit_date"
        )
        _safe_text(row.get("note"), f"ledger line {line_number} note", allow_empty=True)
        if row.get("retained_in_corpus") is not False:
            raise ReconciliationError(
                f"ledger line {line_number} must describe non-canonical retained evidence"
            )
        identity = (episode_id, fingerprint, start, end)
        if stable_id in seen_ids or identity in seen_identities:
            raise ReconciliationError(f"duplicate ledger identity at line {line_number}")
        key = (episode_id, fingerprint)
        metadata = (show_name, duration)
        if key in asset_metadata and asset_metadata[key] != metadata:
            raise ReconciliationError(
                f"ledger asset {episode_id}/{fingerprint} has inconsistent metadata"
            )
        asset_metadata[key] = metadata
        normalized = dict(row)
        normalized["current_start_seconds"] = start
        normalized["current_end_seconds"] = end
        normalized["duration_seconds"] = duration
        rows.append(normalized)
        seen_ids.add(stable_id)
        seen_identities.add(identity)
    if not rows:
        raise ReconciliationError("audit ledger is empty")
    return rows


def _validate_review_state(stable_id: str, value: object, location: str) -> dict:
    if not isinstance(value, dict) or set(value) != REVIEW_FIELDS:
        raise ReconciliationError(f"review {location} has invalid fields")
    status = value.get("status")
    if (
        value.get("id") != stable_id
        or not isinstance(status, str)
        or status not in REVIEW_STATUSES
    ):
        raise ReconciliationError(f"review {location} has invalid identity/status")
    start_value = value.get("proposed_start_seconds")
    end_value = value.get("proposed_end_seconds")
    if (start_value is None) != (end_value is None):
        raise ReconciliationError(f"review {location} has incomplete bounds")
    if start_value is not None:
        start = _finite_number(start_value, f"review {location} start")
        end = _finite_number(end_value, f"review {location} end")
        if start < 0 or end <= start:
            raise ReconciliationError(f"review {location} has invalid bounds")
    elif value["status"] in {"approved", "rebounded"}:
        raise ReconciliationError(f"review {location} lacks required bounds")
    _safe_text(value.get("note"), f"review {location} note", allow_empty=True)
    _safe_text(value.get("reviewer"), f"review {location} reviewer")
    _utc_timestamp(value.get("reviewed_at"), f"review {location} timestamp")
    fingerprint = value.get("audio_fingerprint")
    if not isinstance(fingerprint, str) or not FINGERPRINT_PATTERN.fullmatch(fingerprint):
        raise ReconciliationError(f"review {location} has invalid audio fingerprint")
    return value


def _validate_review_binding(
    review: dict,
    row: dict,
    location: str,
) -> None:
    if review["audio_fingerprint"] != row["audio_fingerprint"]:
        raise ReconciliationError(f"review {location} fingerprint does not match ledger")
    start_value = review["proposed_start_seconds"]
    end_value = review["proposed_end_seconds"]
    if start_value is None:
        if review["status"] == "rejected":
            raise ReconciliationError(
                f"rejected review {location} must retain its original candidate bounds"
            )
        return
    start = _finite_number(start_value, f"review {location} start")
    end = _finite_number(end_value, f"review {location} end")
    if start < 0 or end <= start or end > row["duration_seconds"]:
        raise ReconciliationError(f"review {location} has out-of-duration bounds")
    original = (row["current_start_seconds"], row["current_end_seconds"])
    if review["status"] in {"approved", "rejected"} and (start, end) != original:
        qualifier = "approved" if review["status"] == "approved" else "rejected"
        raise ReconciliationError(
            f"{qualifier} review {location} must retain its original candidate bounds"
        )


def load_review(data: bytes, rows: list[dict]) -> dict[str, dict]:
    document = _parse_json(data, "ear-audit review")
    if not isinstance(document, dict) or set(document) != {
        "schema_version",
        "reviews",
        "history",
        "next_sequence",
    }:
        raise ReconciliationError("review document has invalid top-level fields")
    if type(document.get("schema_version")) is not int or document["schema_version"] != 2:
        raise ReconciliationError("review document must use schema_version 2")
    reviews = document.get("reviews")
    history = document.get("history")
    next_sequence = document.get("next_sequence")
    if not isinstance(reviews, dict) or not isinstance(history, list):
        raise ReconciliationError("review document has invalid reviews/history")
    if isinstance(next_sequence, bool) or not isinstance(next_sequence, int) or next_sequence < 1:
        raise ReconciliationError("review document has invalid next_sequence")
    for stable_id, review in reviews.items():
        if not isinstance(stable_id, str) or not stable_id:
            raise ReconciliationError("review document has an invalid review id")
        _validate_review_state(stable_id, review, f"reviews[{stable_id!r}]")

    by_id = {row["id"]: row for row in rows}
    expected_ids = {
        row["id"] for row in rows if row["disposition"] in BOUNDARY_DISPOSITIONS
    }
    actual_ids = set(reviews)
    if actual_ids != expected_ids:
        missing = sorted(expected_ids - actual_ids)
        extra = sorted(actual_ids - expected_ids)
        raise ReconciliationError(
            f"review coverage mismatch; missing={missing}; extra={extra}"
        )
    for stable_id, review in reviews.items():
        if review["status"] not in COMPLETE_REVIEW_STATUSES:
            raise ReconciliationError(
                f"review {stable_id} must have a complete status, not {review['status']}"
            )
        _validate_review_binding(review, by_id[stable_id], stable_id)

    replayed: dict[str, dict] = {}
    last_sequence = 0
    for index, mutation in enumerate(history):
        if not isinstance(mutation, dict) or set(mutation) != {
            "sequence",
            "id",
            "before",
            "after",
            "at",
        }:
            raise ReconciliationError(f"review history entry {index} has invalid fields")
        sequence = mutation.get("sequence")
        stable_id = mutation.get("id")
        if (
            isinstance(sequence, bool)
            or not isinstance(sequence, int)
            or sequence <= last_sequence
            or not isinstance(stable_id, str)
            or not stable_id
        ):
            raise ReconciliationError(f"review history entry {index} has invalid sequence/id")
        before = mutation.get("before")
        after = mutation.get("after")
        if before is not None:
            _validate_review_state(stable_id, before, f"history[{index}].before")
            if stable_id not in by_id:
                raise ReconciliationError(
                    f"review history entry {index} id is not present in the audit ledger"
                )
            _validate_review_binding(
                before,
                by_id[stable_id],
                f"history[{index}].before",
            )
        if after is None:
            raise ReconciliationError(f"review history entry {index} lacks an after state")
        _validate_review_state(stable_id, after, f"history[{index}].after")
        if stable_id not in by_id:
            raise ReconciliationError(
                f"review history entry {index} id is not present in the audit ledger"
            )
        _validate_review_binding(
            after,
            by_id[stable_id],
            f"history[{index}].after",
        )
        if before != replayed.get(stable_id):
            raise ReconciliationError(f"review history entry {index} does not follow prior state")
        if mutation.get("at") != after["reviewed_at"]:
            raise ReconciliationError(f"review history entry {index} timestamp mismatch")
        replayed[stable_id] = after
        last_sequence = sequence
    if next_sequence <= last_sequence or replayed != reviews:
        raise ReconciliationError("review history does not reproduce current review state")

    return reviews


def _reject_identity(record: object, location: str) -> tuple[str, str, float, float]:
    if not isinstance(record, dict):
        raise ReconciliationError(f"{location} must be an object")
    episode_id = record.get("episodeId")
    fingerprint = record.get("audioFingerprint")
    if not isinstance(episode_id, str) or not episode_id.strip() or episode_id != episode_id.strip():
        raise ReconciliationError(f"{location} has invalid episodeId")
    if not isinstance(fingerprint, str) or not FINGERPRINT_PATTERN.fullmatch(fingerprint):
        raise ReconciliationError(f"{location} has invalid audioFingerprint")
    start = _finite_number(record.get("startSeconds"), f"{location} startSeconds")
    end = _finite_number(record.get("endSeconds"), f"{location} endSeconds")
    if start < 0 or end <= start:
        raise ReconciliationError(f"{location} has invalid bounds")
    return episode_id, fingerprint, start, end


def _load_reject_records(data: bytes) -> list[dict]:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ReconciliationError(f"reject ledger is not UTF-8: {error}") from error
    records: list[dict] = []
    seen: set[tuple[str, str, float, float]] = set()
    for line_number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        record = _parse_json_text(line, f"reject ledger line {line_number}")
        identity = _reject_identity(record, f"reject line {line_number}")
        if identity in seen:
            raise ReconciliationError(f"duplicate reject identity at line {line_number}")
        seen.add(identity)
        records.append(record)
    return records


def _overlap(left: tuple[float, float], right: tuple[float, float]) -> bool:
    return max(left[0], right[0]) < min(left[1], right[1])


def _endpoint_difference_within_tolerance(left: float, right: float) -> bool:
    difference = abs(Decimal(str(left)) - Decimal(str(right)))
    return difference <= Decimal(str(DUPLICATE_ENDPOINT_TOLERANCE_SECONDS))


def _reviewed_full_breaks(candidates: list[dict]) -> tuple[list[dict], int]:
    count = len(candidates)
    parents = list(range(count))

    def find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    for left_index, left in enumerate(candidates):
        left_bounds = (left["start_seconds"], left["end_seconds"])
        for right_index in range(left_index + 1, count):
            right = candidates[right_index]
            right_bounds = (right["start_seconds"], right["end_seconds"])
            if not _overlap(left_bounds, right_bounds):
                continue
            endpoints_match = (
                _endpoint_difference_within_tolerance(left_bounds[0], right_bounds[0])
                and _endpoint_difference_within_tolerance(left_bounds[1], right_bounds[1])
            )
            if not endpoints_match:
                raise ReconciliationError(
                    "conflicting reviewed full breaks overlap: "
                    f"{left['review_id']} and {right['review_id']}"
                )
            union(left_index, right_index)

    groups: dict[int, list[dict]] = defaultdict(list)
    for index, candidate in enumerate(candidates):
        groups[find(index)].append(candidate)
    breaks: list[dict] = []
    duplicate_groups = 0
    for group in groups.values():
        start = max(item["start_seconds"] for item in group)
        end = min(item["end_seconds"] for item in group)
        if end <= start:
            raise ReconciliationError("duplicate reviewed full-break intersection is empty")
        if len(group) > 1:
            duplicate_groups += 1
        breaks.append(
            {
                "start_seconds": start,
                "end_seconds": end,
                "source_ledger_ids": sorted(item["ledger_id"] for item in group),
                "source_review_ids": sorted(item["review_id"] for item in group),
                "supporting_tight_ledger_ids": [],
            }
        )
    breaks.sort(
        key=lambda item: (
            item["start_seconds"],
            item["end_seconds"],
            item["source_review_ids"],
        )
    )
    return breaks, duplicate_groups


def _new_reject_record(
    row: dict,
    review: dict | None,
    review_path: str,
    review_sha256: str,
) -> dict:
    if review is None:
        return {
            "assetBindingProvenance": row["asset_binding_provenance"],
            "audioFingerprint": row["audio_fingerprint"],
            "disposition": "isolated_hallucination",
            "endSeconds": row["current_end_seconds"],
            "episodeId": row["episode_id"],
            "id": row["id"],
            "provenance": ["rediff", "retained_snapshot_a_ear_audit"],
            "reason": "Prior retained-audio ear audit marked this exact candidate as content",
            "startSeconds": row["current_start_seconds"],
            "ts": row["audit_date"] + "T00:00:00Z",
        }
    return {
        "assetBindingProvenance": "immutable fingerprint-bound ear-audit review",
        "audioFingerprint": row["audio_fingerprint"],
        "disposition": "ear_audit_rejected",
        "endSeconds": row["current_end_seconds"],
        "episodeId": row["episode_id"],
        "id": row["id"],
        "provenance": ["rediff", "retained_snapshot_a_ear_audit", "immutable_review"],
        "reason": review["note"] or "Completed ear audit marked this exact candidate as content",
        "reviewArtifact": {"path": review_path, "sha256": review_sha256},
        "sourceReviewId": review["id"],
        "startSeconds": row["current_start_seconds"],
        "ts": review["reviewed_at"],
    }


def _canonical_json_bytes(document: dict) -> bytes:
    return (
        json.dumps(
            document,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _reject_jsonl_bytes(records: list[dict]) -> bytes:
    return "".join(
        json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        for record in records
    ).encode("utf-8")


def build_outputs(
    *,
    rows: list[dict],
    reviews: dict[str, dict],
    existing_rejects: list[dict],
    ledger_path: pathlib.Path,
    ledger_sha256: str,
    review_path: pathlib.Path,
    review_sha256: str,
    rejects_path: pathlib.Path,
) -> tuple[bytes, bytes, int]:
    by_asset: dict[tuple[str, str], list[dict]] = defaultdict(list)
    by_id = {row["id"]: row for row in rows}
    for row in rows:
        by_asset[(row["episode_id"], row["audio_fingerprint"])].append(row)

    audit_vetoes: dict[tuple[str, str, float, float], dict] = {}
    for row in rows:
        review = reviews.get(row["id"])
        is_new_veto = review is not None and review["status"] == "rejected"
        if row["disposition"] != "rejected" and not is_new_veto:
            continue
        identity = (
            row["episode_id"],
            row["audio_fingerprint"],
            row["current_start_seconds"],
            row["current_end_seconds"],
        )
        evidence = audit_vetoes.setdefault(
            identity,
            {
                "start_seconds": identity[2],
                "end_seconds": identity[3],
                "source_ledger_ids": [],
                "source_review_ids": [],
            },
        )
        evidence["source_ledger_ids"].append(row["id"])
        if is_new_veto:
            evidence["source_review_ids"].append(review["id"])

    existing_by_identity: dict[tuple[str, str, float, float], dict] = {}
    for index, record in enumerate(existing_rejects, 1):
        identity = _reject_identity(record, f"reject record {index}")
        if identity in existing_by_identity:
            raise ReconciliationError(f"duplicate reject identity: {identity}")
        existing_by_identity[identity] = record
        asset_rows = by_asset.get(identity[:2])
        if asset_rows and identity[3] > asset_rows[0]["duration_seconds"]:
            raise ReconciliationError(f"active reject exceeds asset duration: {identity}")

    review_path_text = _source_path(review_path)
    for identity in sorted(audit_vetoes):
        if identity in existing_by_identity:
            continue
        source_id = audit_vetoes[identity]["source_ledger_ids"][0]
        row = by_id[source_id]
        review = reviews.get(source_id)
        existing_by_identity[identity] = _new_reject_record(
            row,
            review if review and review["status"] == "rejected" else None,
            review_path_text,
            review_sha256,
        )

    sorted_reject_items = sorted(existing_by_identity.items(), key=lambda item: item[0])
    reject_bytes = _reject_jsonl_bytes([record for _, record in sorted_reject_items])
    reject_sha256 = _sha256(reject_bytes)

    asset_documents: list[dict] = []
    duplicate_groups = 0
    attached_tight = 0
    for key in sorted(by_asset):
        asset_rows = by_asset[key]
        candidates = []
        for row in asset_rows:
            review = reviews.get(row["id"])
            if review is None or review["status"] not in {"approved", "rebounded"}:
                continue
            candidates.append(
                {
                    "start_seconds": float(review["proposed_start_seconds"]),
                    "end_seconds": float(review["proposed_end_seconds"]),
                    "ledger_id": row["id"],
                    "review_id": review["id"],
                }
            )
        full_breaks, asset_duplicates = _reviewed_full_breaks(candidates)
        duplicate_groups += asset_duplicates

        presence_anchors: list[dict] = []
        tight_rows = sorted(
            (row for row in asset_rows if row["disposition"] == "tight_ad"),
            key=lambda row: (
                row["current_start_seconds"],
                row["current_end_seconds"],
                row["id"],
            ),
        )
        for row in tight_rows:
            bounds = (row["current_start_seconds"], row["current_end_seconds"])
            matching = [
                item
                for item in full_breaks
                if _overlap(bounds, (item["start_seconds"], item["end_seconds"]))
            ]
            if len(matching) > 1:
                raise ReconciliationError(
                    f"tight evidence {row['id']} overlaps multiple reviewed full breaks"
                )
            if matching:
                matching[0]["supporting_tight_ledger_ids"].append(row["id"])
                matching[0]["supporting_tight_ledger_ids"].sort()
                attached_tight += 1
            else:
                presence_anchors.append(
                    {
                        "start_seconds": bounds[0],
                        "end_seconds": bounds[1],
                        "source_ledger_ids": [row["id"]],
                    }
                )

        asset_vetoes = [
            evidence
            for identity, evidence in sorted(audit_vetoes.items())
            if identity[:2] == key
        ]
        active_vetoes = [
            (identity[2], identity[3], record.get("id", repr(identity)))
            for identity, record in sorted(existing_by_identity.items())
            if identity[:2] == key
        ]
        positive_regions = [
            (item["start_seconds"], item["end_seconds"], item["source_ledger_ids"][0])
            for item in full_breaks
        ] + [
            (row["current_start_seconds"], row["current_end_seconds"], row["id"])
            for row in tight_rows
        ]
        for veto_start, veto_end, veto_id in active_vetoes:
            for start, end, source_id in positive_regions:
                if _overlap((start, end), (veto_start, veto_end)):
                    raise ReconciliationError(
                        f"positive evidence {source_id} overlaps content veto "
                        f"{veto_id}"
                    )
        for veto in asset_vetoes:
            veto["source_ledger_ids"].sort()
            veto["source_review_ids"].sort()

        first = asset_rows[0]
        asset_documents.append(
            {
                "audio_fingerprint": key[1],
                "content_vetoes": asset_vetoes,
                "duration_seconds": first["duration_seconds"],
                "episode_id": key[0],
                "full_breaks": full_breaks,
                "presence_anchors": presence_anchors,
                "show_name": first["show_name"],
            }
        )

    status_counts = Counter(review["status"] for review in reviews.values())
    full_break_count = sum(len(asset["full_breaks"]) for asset in asset_documents)
    anchor_count = sum(len(asset["presence_anchors"]) for asset in asset_documents)
    veto_count = len(audit_vetoes)
    document = {
        "artifact_kind": "retained_audio_partial_silver_evaluation",
        "assets": asset_documents,
        "label_semantics": {
            "content_vetoes": (
                "only the exact interval is labeled human-reviewed content; the separate "
                "reject ledger may conservatively block overlapping promotion candidates "
                "without labeling surrounding audio"
            ),
            "coverage": "partial",
            "full_breaks": "human-reviewed complete contiguous ad-break boundaries",
            "presence_anchors": "ad presence only; bounds are not full-break boundary truth",
            "quality": "silver",
            "unlabeled_audio": "unknown_elsewhere",
        },
        "schema_version": 1,
        "sources": {
            "audit_ledger": {
                "path": _source_path(ledger_path),
                "sha256": ledger_sha256,
            },
            "ear_audit_review": {
                "path": review_path_text,
                "sha256": review_sha256,
            },
            "reject_ledger": {
                "path": _source_path(rejects_path),
                "sha256": reject_sha256,
            },
        },
        "summary": {
            "assets": len(asset_documents),
            "boundary_reviews": len(reviews),
            "content_vetoes": veto_count,
            "duplicate_full_break_groups": duplicate_groups,
            "full_break_assets": sum(bool(asset["full_breaks"]) for asset in asset_documents),
            "full_breaks": full_break_count,
            "labeled_regions": full_break_count + anchor_count + veto_count,
            "ledger_rows": len(rows),
            "presence_anchors": anchor_count,
            "review_status_counts": {
                status: status_counts.get(status, 0)
                for status in ("approved", "rebounded", "rejected")
            },
            "tight_evidence_attached": attached_tight,
        },
    }
    return _canonical_json_bytes(document), reject_bytes, len(sorted_reject_items)


def _stage_atomic_write(path: pathlib.Path, data: bytes) -> pathlib.Path:
    if path_has_symlink_component(path) or path.is_symlink():
        raise ReconciliationError(f"output path must not contain a symbolic link: {path}")
    temporary: pathlib.Path | None = None
    staged = False
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path_has_symlink_component(path.parent) or not path.parent.is_dir():
            raise ReconciliationError(f"output parent is not a safe directory: {path.parent}")
        mode = path.stat().st_mode & 0o777 if path.exists() else 0o644
        with tempfile.NamedTemporaryFile(
            "wb", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temporary = pathlib.Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        staged = True
        return temporary
    except OSError as error:
        raise ReconciliationError(f"cannot stage {path}: {error}") from error
    finally:
        if not staged and temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _discard_staged(path: pathlib.Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError as error:
        raise ReconciliationError(f"cannot remove staged output {path}: {error}") from error


def _publish_staged(path: pathlib.Path, temporary: pathlib.Path) -> None:
    try:
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as error:
        raise ReconciliationError(f"cannot publish {path}: {error}") from error


def _atomic_write(path: pathlib.Path, data: bytes) -> None:
    temporary = _stage_atomic_write(path, data)
    try:
        _publish_staged(path, temporary)
    except BaseException:
        try:
            _discard_staged(temporary)
        except ReconciliationError:
            pass
        raise


def _restore_bytes(path: pathlib.Path, original: bytes) -> None:
    try:
        current = _read_regular_bytes(path, "publication rollback target")
    except ReconciliationError:
        current = None
    if current != original:
        _atomic_write(path, original)
    restored = _read_regular_bytes(path, "restored publication target")
    if restored != original:
        raise ReconciliationError(f"publication rollback did not restore {path}")


def _remove_new_artifact(path: pathlib.Path, expected: bytes) -> None:
    if not path.exists() and not path.is_symlink():
        return
    current = _read_regular_bytes(path, "partially published artifact")
    if current != expected:
        raise ReconciliationError(
            f"refusing to remove unexpected partially published artifact: {path}"
        )
    try:
        path.unlink()
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as error:
        raise ReconciliationError(
            f"cannot remove partially published artifact {path}: {error}"
        ) from error


@contextlib.contextmanager
def _canonical_publication_transaction(
    annotations_dir: pathlib.Path,
) -> Iterator[None]:
    try:
        with canonical_manifest_lock(annotations_dir):
            yield
    except CanonicalCorpusError as error:
        raise ReconciliationError(
            f"cannot coordinate with canonical corpus publication: {error}"
        ) from error


@contextlib.contextmanager
def _reject_transaction(path: pathlib.Path) -> Iterator[None]:
    if path_has_symlink_component(path) or path.is_symlink() or not path.is_file():
        raise ReconciliationError(f"reject ledger is missing or not a regular file: {path}")
    lock_path = path.parent / f".{path.name}.lock"
    if path_has_symlink_component(lock_path) or lock_path.is_symlink():
        raise ReconciliationError(f"reject-ledger lock path is unsafe: {lock_path}")
    flags = (
        os.O_CREAT
        | os.O_RDWR
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as error:
        raise ReconciliationError(f"cannot open reject-ledger lock {lock_path}: {error}") from error
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
        except OSError as error:
            raise ReconciliationError(f"cannot lock reject ledger {path}: {error}") from error
        yield
    finally:
        # Closing the descriptor releases flock. Do not perform a separate
        # LOCK_UN that could report failure after durable outputs committed.
        try:
            os.close(descriptor)
        except OSError:
            pass


def reconcile(
    *,
    ledger_path: pathlib.Path = DEFAULT_LEDGER,
    review_path: pathlib.Path = DEFAULT_REVIEW,
    rejects_path: pathlib.Path = DEFAULT_REJECTS,
    output_dir: pathlib.Path = DEFAULT_OUTPUT_DIR,
    annotations_dir: pathlib.Path = DEFAULT_ANNOTATIONS_DIR,
    dry_run: bool = False,
) -> ReconcileResult:
    ledger_path = pathlib.Path(ledger_path)
    review_path = pathlib.Path(review_path)
    rejects_path = pathlib.Path(rejects_path)
    output_dir = pathlib.Path(output_dir)
    annotations_dir = pathlib.Path(annotations_dir)
    _validate_output_directory(output_dir)
    ledger_bytes = _read_regular_bytes(ledger_path, "audit ledger")
    review_bytes = _read_regular_bytes(review_path, "ear-audit review")
    ledger_sha256 = _sha256(ledger_bytes)
    review_sha256 = _sha256(review_bytes)
    _validate_content_address(review_path, review_sha256, "ear-audit review")
    rows = load_ledger(ledger_bytes)
    reviews = load_review(review_bytes, rows)

    with contextlib.ExitStack() as transactions:
        if not dry_run:
            transactions.enter_context(
                _canonical_publication_transaction(annotations_dir)
            )
        transactions.enter_context(_reject_transaction(rejects_path))
        reject_input = _read_regular_bytes(rejects_path, "reject ledger")
        # Parse with this tool's controlled numeric validation first. The shared
        # canonical loader converts JSON integers directly to float and can
        # otherwise leak OverflowError for adversarially large bounds.
        existing_rejects = _load_reject_records(reject_input)
        try:
            canonical_rejects = load_reject_ledger(rejects_path)
        except ValueError as error:
            raise ReconciliationError(f"invalid active reject ledger: {error}") from error
        if sum(map(len, canonical_rejects.values())) != len(existing_rejects):
            raise ReconciliationError("canonical reject-ledger validation count mismatch")
        artifact_bytes, reject_bytes, reject_count = build_outputs(
            rows=rows,
            reviews=reviews,
            existing_rejects=existing_rejects,
            ledger_path=ledger_path,
            ledger_sha256=ledger_sha256,
            review_path=review_path,
            review_sha256=review_sha256,
            rejects_path=rejects_path,
        )
        artifact_sha256 = _sha256(artifact_bytes)
        artifact_path = output_dir / f"earaudit-partial-silver-{artifact_sha256}.json"
        _validate_artifact_path(artifact_path)
        reject_changed = reject_bytes != reject_input
        artifact_changed = not artifact_path.exists()
        if artifact_path.exists():
            try:
                artifact_matches = (
                    not artifact_path.is_symlink()
                    and artifact_path.is_file()
                    and artifact_path.read_bytes() == artifact_bytes
                )
            except OSError as error:
                raise ReconciliationError(
                    f"cannot validate existing artifact {artifact_path}: {error}"
                ) from error
            if not artifact_matches:
                raise ReconciliationError(
                    f"content-addressed artifact path has unexpected bytes: {artifact_path}"
                )
        if dry_run:
            return ReconcileResult(
                artifact_path,
                artifact_sha256,
                artifact_changed,
                reject_changed,
                reject_count,
            )

        # Prove the artifact destination is writable before reject-first
        # publication. Any reported failure restores the original ledger and
        # removes a newly published artifact before releasing either lock.
        staged_artifact = (
            _stage_atomic_write(artifact_path, artifact_bytes)
            if artifact_changed
            else None
        )
        reject_publication_attempted = False
        try:
            if reject_changed:
                reject_publication_attempted = True
                _atomic_write(rejects_path, reject_bytes)
            published_bytes = _read_regular_bytes(rejects_path, "published reject ledger")
            if published_bytes != reject_bytes:
                raise ReconciliationError(
                    "published reject-ledger bytes do not match the reconciled output"
                )
            try:
                published = load_reject_ledger(rejects_path)
            except ValueError as error:
                raise ReconciliationError(
                    f"published reject ledger is invalid: {error}"
                ) from error
            if sum(map(len, published.values())) != reject_count:
                raise ReconciliationError("published reject-ledger count mismatch")
            if staged_artifact is not None:
                _publish_staged(artifact_path, staged_artifact)
                staged_artifact = None
        except BaseException as publication_error:
            rollback_errors: list[BaseException] = []
            if artifact_changed:
                try:
                    _remove_new_artifact(artifact_path, artifact_bytes)
                except BaseException as error:
                    rollback_errors.append(error)
            if reject_publication_attempted:
                try:
                    _restore_bytes(rejects_path, reject_input)
                except BaseException as error:
                    rollback_errors.append(error)
            if staged_artifact is not None:
                try:
                    _discard_staged(staged_artifact)
                except BaseException as error:
                    rollback_errors.append(error)
            if rollback_errors:
                detail = "; ".join(str(error) for error in rollback_errors)
                raise ReconciliationError(
                    f"publication failed ({publication_error}); rollback failed: {detail}"
                ) from rollback_errors[0]
            raise
        return ReconcileResult(
            artifact_path,
            artifact_sha256,
            artifact_changed,
            reject_changed,
            reject_count,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=pathlib.Path, default=DEFAULT_LEDGER)
    parser.add_argument("--review", type=pathlib.Path, default=DEFAULT_REVIEW)
    parser.add_argument("--rejects-file", type=pathlib.Path, default=DEFAULT_REJECTS)
    parser.add_argument("--output-dir", type=pathlib.Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--annotations-dir", type=pathlib.Path, default=DEFAULT_ANNOTATIONS_DIR
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = reconcile(
            ledger_path=args.ledger,
            review_path=args.review,
            rejects_path=args.rejects_file,
            output_dir=args.output_dir,
            annotations_dir=args.annotations_dir,
            dry_run=args.dry_run,
        )
    except ReconciliationError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    verb = "validated" if args.dry_run else "published"
    print(
        f"{verb}: {result.artifact_path} ({result.artifact_sha256}); "
        f"rejects={result.reject_count}"
    )
    if args.dry_run:
        print("dry-run: no corpus data files changed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
