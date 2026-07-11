#!/usr/bin/env python3
"""Demote one ear-verified false promotion without corrupting the corpus.

The annotation and reject ledger are each replaced atomically. The reject is
written first, so an interrupted command can be rerun safely: a retry either
finishes the annotation update or observes the already-completed demotion.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import json
import math
import os
import pathlib
import re
import sys
import tempfile
import threading
from typing import NamedTuple

from convert_annotations_to_chapter_goldens import path_has_symlink_component
from l2f_canonical_manifest import (
    MANIFEST_FILENAME,
    CanonicalCorpusError,
    canonical_manifest_lock,
    validate_canonical_replacement,
)


ROOT = pathlib.Path(__file__).resolve().parents[1]
ANN_DIR = ROOT / "TestFixtures/Corpus/Annotations"
REJECTS = ROOT / "TestFixtures/Corpus/Snapshots/audit-rejects.jsonl"
CONTENT_NOTE = "Audit-derived content - must NEVER be skipped"
MATCH_TOLERANCE_SECONDS = 2.0
AUTOMATIC_PROVENANCE = frozenset({"rediff", "drafter", "pipeline"})
_PATH_LOCKS: dict[str, threading.RLock] = {}
_PATH_LOCKS_GUARD = threading.Lock()


class DemotionError(ValueError):
    """Raised when a requested demotion is ambiguous or unsafe."""


class DemotionResult(NamedTuple):
    changed: bool
    annotation_path: pathlib.Path
    episode_id: str
    start_seconds: float
    end_seconds: float
    remaining_windows: int
    reject_count: int


def number(window: dict, snake_key: str, camel_key: str) -> float:
    value = window.get(snake_key)
    if value is None:
        value = window.get(camel_key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DemotionError(f"{snake_key} must be a finite number")
    result = float(value)
    if not math.isfinite(result):
        raise DemotionError(f"{snake_key} must be a finite number")
    return result


def derive_content_windows(ad_windows: list[dict], duration: float) -> list[dict]:
    """Return the exact non-ad complement of ``ad_windows`` in [0, duration]."""
    if isinstance(duration, bool) or not isinstance(duration, (int, float)):
        raise DemotionError("duration_seconds must be a finite positive number")
    duration = float(duration)
    if not math.isfinite(duration) or duration <= 0:
        raise DemotionError("duration_seconds must be a finite positive number")

    normalized: list[tuple[float, float]] = []
    for window in ad_windows:
        start = number(window, "start_seconds", "startSeconds")
        end = number(window, "end_seconds", "endSeconds")
        if start < 0 or end <= start or end > duration:
            raise DemotionError(
                f"invalid ad window [{start}, {end}] for duration {duration}"
            )
        normalized.append((start, end))
    normalized.sort()

    content: list[dict] = []
    cursor = 0.0
    for start, end in normalized:
        if start < cursor:
            raise DemotionError(f"overlapping ad windows at {start} < {cursor}")
        if start > cursor:
            content.append(
                {
                    "start_seconds": cursor,
                    "end_seconds": start,
                    "notes": CONTENT_NOTE,
                }
            )
        cursor = end
    if cursor < duration:
        content.append(
            {
                "start_seconds": cursor,
                "end_seconds": duration,
                "notes": CONTENT_NOTE,
            }
        )
    return content


def repair_content_windows(
    ad_windows: list[dict],
    content_windows: list[dict],
    duration: float,
    *,
    snake_case: bool,
) -> list[dict]:
    """Fill the non-ad complement while preserving existing content metadata."""
    complement = derive_content_windows(ad_windows, duration)
    existing: list[tuple[float, float, dict]] = []
    for window in content_windows:
        if not isinstance(window, dict):
            raise DemotionError("content windows must be objects")
        start = number(window, "start_seconds", "startSeconds")
        end = number(window, "end_seconds", "endSeconds")
        if start < 0 or end <= start or end > float(duration):
            raise DemotionError(f"invalid content window [{start}, {end}]")
        existing.append((start, end, dict(window)))
    existing.sort(key=lambda item: (item[0], item[1]))
    for left, right in zip(existing, existing[1:]):
        if right[0] < left[1]:
            raise DemotionError(f"overlapping content windows at {right[0]} < {left[1]}")

    grouped: list[list[tuple[float, float, dict]]] = [[] for _ in complement]
    for start, end, window in existing:
        matching = [
            index
            for index, interval in enumerate(complement)
            if start >= interval["start_seconds"] and end <= interval["end_seconds"]
        ]
        if len(matching) != 1:
            raise DemotionError(
                f"content window [{start}, {end}] overlaps a retained ad window"
            )
        grouped[matching[0]].append((start, end, window))

    start_key = "start_seconds" if snake_case else "startSeconds"
    end_key = "end_seconds" if snake_case else "endSeconds"

    def filler(start: float, end: float) -> dict:
        return {start_key: start, end_key: end, "notes": CONTENT_NOTE}

    repaired: list[dict] = []
    for interval, windows in zip(complement, grouped):
        cursor = interval["start_seconds"]
        for start, end, window in windows:
            if start > cursor:
                repaired.append(filler(cursor, start))
            repaired.append(window)
            cursor = end
        if cursor < interval["end_seconds"]:
            repaired.append(filler(cursor, interval["end_seconds"]))

    generic_fields = {start_key, end_key, "notes"}
    coalesced: list[dict] = []
    for window in repaired:
        is_generic = set(window) == generic_fields and window.get("notes") == CONTENT_NOTE
        previous_is_generic = (
            bool(coalesced)
            and set(coalesced[-1]) == generic_fields
            and coalesced[-1].get("notes") == CONTENT_NOTE
        )
        if (
            is_generic
            and previous_is_generic
            and number(coalesced[-1], "start_seconds", "startSeconds")
            < number(coalesced[-1], "end_seconds", "endSeconds")
            == number(window, "start_seconds", "startSeconds")
        ):
            coalesced[-1][end_key] = window[end_key]
        else:
            coalesced.append(window)
    return coalesced


def atomic_write_bytes(
    path: pathlib.Path,
    data: bytes,
    *,
    mode: int | None = None,
) -> None:
    """Durably replace ``path`` without exposing partial data."""
    if path_has_symlink_component(path):
        raise DemotionError(f"output path must not contain a symbolic link: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path_has_symlink_component(path) or not path.parent.is_dir():
        raise DemotionError(f"output parent is not a safe directory: {path.parent}")
    existing_mode = mode if mode is not None else (
        path.stat().st_mode & 0o777 if path.exists() else 0o644
    )
    temporary: pathlib.Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "wb", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temporary = pathlib.Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, existing_mode)
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def atomic_write_text(path: pathlib.Path, text: str) -> None:
    """Durably replace ``path`` with UTF-8 text."""
    atomic_write_bytes(path, text.encode("utf-8"))


def publish_demotion(
    *,
    annotation_path: pathlib.Path,
    annotation_text: str,
    rejects_path: pathlib.Path,
    rejects_text: str,
    writer=atomic_write_text,
) -> None:
    """Publish ledger and annotation together, restoring exact bytes on failure."""
    originals: dict[pathlib.Path, tuple[bytes, int] | None] = {}
    for path in (rejects_path, annotation_path):
        if path_has_symlink_component(path):
            raise DemotionError(f"demotion output must not contain a symbolic link: {path}")
        originals[path] = (
            (path.read_bytes(), path.stat().st_mode & 0o777) if path.exists() else None
        )
    try:
        # Reject-first ordering makes a process crash conservative and resumable.
        writer(rejects_path, rejects_text)
        writer(annotation_path, annotation_text)
    except BaseException as error:
        rollback_errors: list[str] = []
        for path, original in originals.items():
            try:
                if original is None:
                    path.unlink(missing_ok=True)
                else:
                    atomic_write_bytes(path, original[0], mode=original[1])
            except OSError as rollback_error:
                rollback_errors.append(f"{path}: {rollback_error}")
        detail = f"demotion publication failed: {error}"
        if rollback_errors:
            detail += "; rollback incomplete: " + "; ".join(rollback_errors)
        raise DemotionError(detail) from error


def path_lock(path: pathlib.Path) -> threading.RLock:
    key = str(path.resolve())
    with _PATH_LOCKS_GUARD:
        return _PATH_LOCKS.setdefault(key, threading.RLock())


@contextlib.contextmanager
def exclusive_demotion_transaction(rejects_path: pathlib.Path):
    """Serialize the annotation-plus-ledger transaction across callers."""
    if path_has_symlink_component(rejects_path):
        raise DemotionError(
            f"reject ledger path must not contain a symbolic link: {rejects_path}"
        )
    rejects_path.parent.mkdir(parents=True, exist_ok=True)
    if path_has_symlink_component(rejects_path) or not rejects_path.parent.is_dir():
        raise DemotionError(f"reject ledger parent is not a safe directory: {rejects_path.parent}")
    lock_path = rejects_path.parent / f".{rejects_path.name}.lock"
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as error:
        raise DemotionError(f"cannot open reject-ledger lock: {lock_path}") from error
    try:
        with path_lock(rejects_path):
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def load_reject_records(path: pathlib.Path) -> list[dict]:
    if path_has_symlink_component(path):
        raise DemotionError(f"reject ledger is not a regular file: {path}")
    if not path.exists():
        return []
    if not path.is_file():
        raise DemotionError(f"reject ledger is not a regular file: {path}")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise DemotionError(f"cannot read reject ledger {path}: {error}") from error
    records: list[dict] = []
    identities: set[tuple[str, str, float, float]] = set()
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise DemotionError(f"invalid reject JSON at line {line_number}: {error}") from error
        if not isinstance(record, dict):
            raise DemotionError(f"reject line {line_number} is not an object")
        identity = reject_identity(record)
        if identity is None:
            raise DemotionError(f"reject line {line_number} has invalid identity/bounds")
        if identity in identities:
            raise DemotionError(f"duplicate reject at line {line_number}: {identity}")
        identities.add(identity)
        records.append(record)
    return records


def reject_identity(record: dict) -> tuple[str, str, float, float] | None:
    try:
        episode_id = record["episodeId"]
        fingerprint = record["audioFingerprint"]
        raw_start = record["startSeconds"]
        raw_end = record["endSeconds"]
        if (
            isinstance(raw_start, bool)
            or isinstance(raw_end, bool)
            or not isinstance(raw_start, (int, float))
            or not isinstance(raw_end, (int, float))
        ):
            return None
        start = float(raw_start)
        end = float(raw_end)
    except (KeyError, OverflowError, TypeError, ValueError):
        return None
    if (
        not isinstance(episode_id, str)
        or not episode_id.strip()
        or episode_id != episode_id.strip()
        or not isinstance(fingerprint, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", fingerprint) is None
        or not math.isfinite(start)
        or not math.isfinite(end)
        or start < 0
        or end <= start
    ):
        return None
    return episode_id, fingerprint, start, end


def has_automatic_marker(document: dict) -> bool:
    """Return true only for explicit automatic-promotion provenance."""
    if document.get("auto_promoted") is True or document.get("autoPromoted") is True:
        return True
    if any(
        document.get(key) is not None
        for key in ("auto_promoted_at", "autoPromotedAt", "auto_promoted_by", "autoPromotedBy")
    ):
        return True
    priority = document.get("audit_priority", document.get("auditPriority"))
    if isinstance(priority, int) and not isinstance(priority, bool):
        return True
    provenance = document.get("provenance")
    return isinstance(provenance, list) and any(
        isinstance(value, str) and value.lower() in AUTOMATIC_PROVENANCE
        for value in provenance
    )


def canonical_annotation_paths(annotations_dir: pathlib.Path) -> list[pathlib.Path]:
    """Return regular annotation files from the required schema-v1 manifest."""
    manifest_path = annotations_dir / MANIFEST_FILENAME
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise DemotionError(
            f"canonical manifest is missing or not a regular file: {manifest_path}"
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DemotionError(f"cannot read canonical manifest {manifest_path}: {error}") from error
    if (
        not isinstance(manifest, dict)
        or type(manifest.get("schema_version")) is not int
        or manifest["schema_version"] != 1
    ):
        raise DemotionError("canonical manifest must use schema_version 1")
    names = manifest.get("annotations")
    if not isinstance(names, list) or not names:
        raise DemotionError("canonical manifest annotations must be a non-empty array")

    paths: list[pathlib.Path] = []
    seen: set[str] = set()
    for name in names:
        if (
            not isinstance(name, str)
            or not name
            or pathlib.Path(name).name != name
            or "/" in name
            or "\\" in name
            or name.startswith("_")
            or name.endswith(".example.json")
            or not name.endswith(".json")
        ):
            raise DemotionError(f"unsafe canonical annotation entry: {name!r}")
        if name in seen:
            raise DemotionError(f"duplicate canonical annotation entry: {name}")
        path = annotations_dir / name
        if path.is_symlink() or not path.is_file():
            raise DemotionError(f"canonical annotation is missing or not a regular file: {name}")
        seen.add(name)
        paths.append(path)
    return paths


def resolve_annotation(annotations_dir: pathlib.Path, episode_prefix: str) -> pathlib.Path:
    if not episode_prefix or "/" in episode_prefix or "\\" in episode_prefix:
        raise DemotionError("episode prefix must be a non-empty filename prefix")
    candidates = sorted(
        path
        for path in canonical_annotation_paths(annotations_dir)
        if path.name.startswith(episode_prefix)
    )
    if not candidates:
        raise DemotionError(f"no annotation matching '{episode_prefix}*'")
    if len(candidates) > 1:
        names = ", ".join(path.name for path in candidates)
        raise DemotionError(
            f"ambiguous prefix '{episode_prefix}' matches {len(candidates)} annotations: {names}"
        )
    return candidates[0]


def demote(
    *,
    annotations_dir: pathlib.Path,
    rejects_path: pathlib.Path,
    episode_prefix: str,
    requested_start: float,
    reason: str,
    reviewed_at: str | None = None,
    dry_run: bool = False,
) -> DemotionResult:
    if dry_run:
        with canonical_manifest_lock(annotations_dir):
            return _demote_unlocked(
                annotations_dir=annotations_dir,
                rejects_path=rejects_path,
                episode_prefix=episode_prefix,
                requested_start=requested_start,
                reason=reason,
                reviewed_at=reviewed_at,
                dry_run=True,
            )
    # All corpus writers take the canonical lock first. The ledger lock then
    # protects exact reject history while promotion's under-lock veto check
    # prevents a stale plan from restoring this span.
    with canonical_manifest_lock(annotations_dir):
        with exclusive_demotion_transaction(rejects_path):
            return _demote_unlocked(
                annotations_dir=annotations_dir,
                rejects_path=rejects_path,
                episode_prefix=episode_prefix,
                requested_start=requested_start,
                reason=reason,
                reviewed_at=reviewed_at,
                dry_run=dry_run,
            )


def _demote_unlocked(
    *,
    annotations_dir: pathlib.Path,
    rejects_path: pathlib.Path,
    episode_prefix: str,
    requested_start: float,
    reason: str,
    reviewed_at: str | None = None,
    dry_run: bool = False,
) -> DemotionResult:
    annotation_path = resolve_annotation(annotations_dir, episode_prefix)
    try:
        annotation = json.loads(annotation_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DemotionError(f"cannot read {annotation_path.name}: {error}") from error
    if not isinstance(annotation, dict):
        raise DemotionError(f"{annotation_path.name} must contain a JSON object")

    identity_values = [
        annotation[key]
        for key in ("episode_id", "episodeId")
        if key in annotation
    ]
    if (
        not identity_values
        or any(not isinstance(value, str) or not value for value in identity_values)
        or len(set(identity_values)) != 1
        or identity_values[0] != annotation_path.stem
    ):
        raise DemotionError(
            f"{annotation_path.name} must contain an episode id matching its canonical filename"
        )
    episode_id = identity_values[0]
    fingerprint = annotation.get("audio_fingerprint")
    if not isinstance(fingerprint, str) or re.fullmatch(
        r"sha256:[0-9a-f]{64}", fingerprint
    ) is None:
        raise DemotionError("annotation lacks a valid exact audio fingerprint")
    ads_key = "ad_windows" if "ad_windows" in annotation else "adWindows"
    content_key = "content_windows" if ads_key == "ad_windows" else "contentWindows"
    duration_key = "duration_seconds" if "duration_seconds" in annotation else "durationSeconds"
    windows = annotation.get(ads_key)
    if not isinstance(windows, list):
        raise DemotionError(f"{ads_key} must be an array")
    content_windows = annotation.get(content_key)
    if not isinstance(content_windows, list):
        raise DemotionError(f"{content_key} must be an array")

    matching = [
        window for window in windows
        if abs(number(window, "start_seconds", "startSeconds") - requested_start)
        <= MATCH_TOLERANCE_SECONDS
    ]
    records = load_reject_records(rejects_path)
    prior = [
        identity for record in records
        if (identity := reject_identity(record)) is not None
        and identity[0] == episode_id
        and identity[1] == fingerprint
        and abs(identity[2] - requested_start) <= MATCH_TOLERANCE_SECONDS
    ]
    if not matching:
        if len(prior) == 1:
            try:
                validate_canonical_replacement(
                    annotations_dir,
                    filename=annotation_path.name,
                    replacement=annotation,
                )
            except CanonicalCorpusError as error:
                raise DemotionError(
                    f"invalid canonical corpus on idempotent demotion: {error}"
                ) from error
            identity = prior[0]
            return DemotionResult(
                False,
                annotation_path,
                episode_id,
                identity[2],
                identity[3],
                len(windows),
                len(records),
            )
        starts = [number(window, "start_seconds", "startSeconds") for window in windows]
        raise DemotionError(
            f"no window within +/-{MATCH_TOLERANCE_SECONDS:g}s of start={requested_start}; "
            f"current starts: {starts}"
        )
    if len(matching) > 1:
        raise DemotionError(f"multiple windows match start={requested_start}; refusing ambiguity")

    target = matching[0]
    if not has_automatic_marker(annotation) and not has_automatic_marker(target):
        raise DemotionError("matched window has no automatic-promotion marker; refusing human gold")
    start = number(target, "start_seconds", "startSeconds")
    end = number(target, "end_seconds", "endSeconds")
    new_windows = [window for window in windows if window is not target]
    duration = annotation.get(duration_key)
    new_content = repair_content_windows(
        new_windows,
        content_windows,
        duration,
        snake_case=content_key == "content_windows",
    )
    annotation[ads_key] = new_windows
    annotation[content_key] = new_content
    if "adWindowCount" in annotation:
        annotation["adWindowCount"] = len(new_windows)
    if "ad_window_count" in annotation:
        annotation["ad_window_count"] = len(new_windows)
    try:
        validate_canonical_replacement(
            annotations_dir,
            filename=annotation_path.name,
            replacement=annotation,
        )
    except CanonicalCorpusError as error:
        raise DemotionError(f"invalid demotion output: {error}") from error

    identity = (episode_id, fingerprint, start, end)
    if not any(reject_identity(record) == identity for record in records):
        provenance = target.get("provenance", target.get("source", "unknown"))
        timestamp = reviewed_at or dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
        records.append(
            {
                "id": f"{episode_id}@{start:.2f}-{end:.2f}",
                "ts": timestamp,
                "episodeId": episode_id,
                "audioFingerprint": fingerprint,
                "startSeconds": start,
                "endSeconds": end,
                "provenance": provenance,
                "reason": reason,
                "disposition": "isolated_hallucination",
            }
        )

    if not dry_run:
        rejects_text = "".join(
            json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
            for record in records
        )
        publish_demotion(
            annotation_path=annotation_path,
            annotation_text=json.dumps(
                annotation,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )
            + "\n",
            rejects_path=rejects_path,
            rejects_text=rejects_text,
        )

    return DemotionResult(
        True,
        annotation_path,
        episode_id,
        start,
        end,
        len(new_windows),
        len(records),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("episode_id", help="Full episode id or unambiguous prefix")
    parser.add_argument("start_seconds", type=float, help="Window start (matched within +/-2s)")
    parser.add_argument("--reason", default="", help="Why this promotion is false")
    parser.add_argument("--dry-run", action="store_true", help="Validate and preview only")
    parser.add_argument("--annotations-dir", type=pathlib.Path, default=ANN_DIR, help=argparse.SUPPRESS)
    parser.add_argument("--rejects-file", type=pathlib.Path, default=REJECTS, help=argparse.SUPPRESS)
    parser.add_argument("--reviewed-at", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        result = demote(
            annotations_dir=args.annotations_dir,
            rejects_path=args.rejects_file,
            episode_prefix=args.episode_id,
            requested_start=args.start_seconds,
            reason=args.reason,
            reviewed_at=args.reviewed_at,
            dry_run=args.dry_run,
        )
    except DemotionError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    action = "would demote" if args.dry_run else "demoted"
    if not result.changed:
        action = "already demoted"
    print(
        f"{action}: {result.episode_id} "
        f"{result.start_seconds:.2f}-{result.end_seconds:.2f}s; "
        f"remaining windows={result.remaining_windows}; rejects={result.reject_count}"
    )
    if args.dry_run:
        print("dry-run: no files changed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
