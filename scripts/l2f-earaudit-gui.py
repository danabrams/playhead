#!/usr/bin/env python3
"""Serve the completed rediff audit as a local boundary-review workbench.

The browser writes only a dedicated review JSON document. Corpus annotations
and the tracked audit ledger are read-only inputs.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import math
import mimetypes
import os
import pathlib
import re
import stat
import tempfile
import threading
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_AUDIO_DIR = ROOT / "TestFixtures/Corpus/Audio"
DEFAULT_AUDIT_LEDGER = ROOT / "TestFixtures/Corpus/Audits/rediff-r3-2026-07-07.jsonl"
DEFAULT_REVIEW_FILE = ROOT / "TestFixtures/Corpus/Drafts/l2f-earaudit-review.json"
MAX_BODY_BYTES = 32 * 1024
MAX_NOTE_LENGTH = 4_000
MAX_REVIEWER_LENGTH = 120
ALLOWED_STATUSES = frozenset({"approved", "rebounded", "rejected", "unsure"})
BOUNDARY_DISPOSITIONS = frozenset({"boundary_off", "edge_clipping"})
AUDIO_EXTENSIONS = frozenset({".aac", ".flac", ".m4a", ".mp3", ".mp4", ".wav"})
FINGERPRINT_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
_PATH_LOCKS: dict[str, threading.RLock] = {}
_PATH_LOCKS_GUARD = threading.Lock()


class ValidationError(ValueError):
    """A client-facing validation failure."""


class PersistedStateError(ValidationError):
    """A corrupt or incompatible review file, rather than a bad request."""


class RangeNotSatisfiable(ValueError):
    """An invalid or unsatisfiable HTTP byte range."""


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def atomic_write_json(path: pathlib.Path, document: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = path.stat().st_mode & 0o777 if path.exists() else 0o644
    temporary: pathlib.Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temporary = pathlib.Path(handle.name)
            json.dump(document, handle, indent=2, sort_keys=True, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def path_lock(path: pathlib.Path) -> threading.RLock:
    key = str(path.resolve())
    with _PATH_LOCKS_GUARD:
        return _PATH_LOCKS.setdefault(key, threading.RLock())


@contextlib.contextmanager
def exclusive_file_transaction(path: pathlib.Path, lock: threading.RLock):
    """Serialize readers and writers across store instances and processes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.parent / f".{path.name}.lock"
    with lock, lock_path.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def finite_number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{field} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise ValidationError(f"{field} must be a finite number")
    return number


def fingerprint_audio_handle(handle: object) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
        digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def audio_file_identity(metadata: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def audio_fingerprint_and_identity(
    path: pathlib.Path,
) -> tuple[str, tuple[int, int, int, int, int]]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags | getattr(os, "O_NONBLOCK", 0))
    with os.fdopen(descriptor, "rb") as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise OSError("audio path is not a regular file")
        fingerprint = fingerprint_audio_handle(handle)
        after = os.fstat(handle.fileno())
        if audio_file_identity(before) != audio_file_identity(after):
            raise OSError("audio changed while it was being fingerprinted")
        return fingerprint, audio_file_identity(after)


def audio_fingerprint(path: pathlib.Path) -> str:
    return audio_fingerprint_and_identity(path)[0]


def resolve_audio(episode_id: str, audio_dir: pathlib.Path) -> pathlib.Path | None:
    root = audio_dir.resolve()
    if not root.is_dir():
        return None
    for path in sorted(root.iterdir()):
        if path.stem != episode_id:
            continue
        if path.is_symlink():
            continue
        resolved = path.resolve()
        try:
            resolved.relative_to(root)
        except ValueError:
            continue
        if (
            resolved.stem == episode_id
            and resolved.is_file()
            and resolved.suffix.lower() in AUDIO_EXTENSIONS
        ):
            return resolved
    return None


def load_audit_entries(
    audit_ledger: pathlib.Path,
    audio_dir: pathlib.Path,
) -> list[dict]:
    if not audit_ledger.is_file():
        raise ValidationError(f"audit ledger not found: {audit_ledger}")
    entries: list[dict] = []
    seen: set[str] = set()
    audio_evidence: dict[
        pathlib.Path,
        tuple[str, tuple[int, int, int, int, int]],
    ] = {}
    audio_root = audio_dir.resolve()
    for line_number, line in enumerate(audit_ledger.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValidationError(f"invalid audit JSON at line {line_number}: {error}") from error
        if not isinstance(entry, dict):
            raise ValidationError(f"audit line {line_number} must be an object")
        stable_id = entry.get("id")
        episode_id = entry.get("episode_id")
        if not isinstance(stable_id, str) or not stable_id or len(stable_id) > 500:
            raise ValidationError(f"audit line {line_number} has an invalid id")
        if "/" in stable_id or "\\" in stable_id:
            raise ValidationError(f"audit line {line_number} id must not contain a path separator")
        if stable_id in seen:
            raise ValidationError(f"duplicate audit id at line {line_number}: {stable_id}")
        if (
            not isinstance(episode_id, str)
            or not episode_id
            or len(episode_id) > 500
            or "/" in episode_id
            or "\\" in episode_id
        ):
            raise ValidationError(f"audit line {line_number} has an invalid episode_id")
        start = finite_number(entry.get("current_start_seconds"), "current_start_seconds")
        end = finite_number(entry.get("current_end_seconds"), "current_end_seconds")
        duration = finite_number(entry.get("duration_seconds"), "duration_seconds")
        if start < 0 or end <= start or end > duration:
            raise ValidationError(f"audit line {line_number} has invalid bounds")
        show_name = entry.get("show_name")
        disposition = entry.get("disposition")
        recommendation = entry.get("recommendation")
        raw_verdict = entry.get("raw_verdict")
        provenance_tier = entry.get("provenance_tier")
        fingerprint = entry.get("audio_fingerprint")
        if not isinstance(show_name, str) or not isinstance(disposition, str):
            raise ValidationError(f"audit line {line_number} has invalid display metadata")
        if not isinstance(raw_verdict, str) or not isinstance(provenance_tier, str):
            raise ValidationError(f"audit line {line_number} has invalid verdict/provenance metadata")
        if not isinstance(recommendation, str):
            raise ValidationError(f"audit line {line_number} has invalid recommendation")
        if not isinstance(fingerprint, str) or FINGERPRINT_PATTERN.fullmatch(fingerprint) is None:
            raise ValidationError(
                f"audit line {line_number} lacks an exact audio_fingerprint"
            )
        audio = resolve_audio(episode_id, audio_dir)
        audio_identity = None
        if audio is not None:
            try:
                if audio not in audio_evidence:
                    audio_evidence[audio] = audio_fingerprint_and_identity(audio)
                actual_fingerprint, audio_identity = audio_evidence[audio]
            except OSError as error:
                raise ValidationError(
                    f"cannot fingerprint audit audio for {episode_id}: {error}"
                ) from error
            if actual_fingerprint != fingerprint:
                raise ValidationError(
                    f"audit audio fingerprint mismatch for {episode_id}: "
                    f"expected {fingerprint}, got {actual_fingerprint}"
                )
        normalized = dict(entry)
        normalized.update(
            {
                "current_start_seconds": start,
                "current_end_seconds": end,
                "duration_seconds": duration,
                "audio": str(audio) if audio else None,
                "_audio_root": str(audio_root),
                "_audio_identity": audio_identity,
                "audio_available": audio is not None,
                "boundary_work": disposition in BOUNDARY_DISPOSITIONS,
            }
        )
        entries.append(normalized)
        seen.add(stable_id)
    if not entries:
        raise ValidationError("audit ledger is empty")
    entries.sort(
        key=lambda item: (
            item["show_name"].casefold(),
            item["episode_id"],
            item["current_start_seconds"],
        )
    )
    return entries


class ReviewStore:
    """Atomic, idempotent review state with chronological mutation history."""

    semantic_fields = (
        "id",
        "status",
        "proposed_start_seconds",
        "proposed_end_seconds",
        "note",
        "reviewer",
        "audio_fingerprint",
    )

    def __init__(self, path: pathlib.Path):
        self.path = path
        self._lock = path_lock(path)

    @staticmethod
    def empty_document() -> dict:
        return {"schema_version": 2, "reviews": {}, "history": [], "next_sequence": 1}

    def _load_unlocked(self) -> dict:
        if not self.path.exists():
            return self.empty_document()
        try:
            document = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise PersistedStateError(f"invalid review file {self.path}: {error}") from error
        if not isinstance(document, dict) or document.get("schema_version") != 2:
            raise PersistedStateError("review file has an unsupported schema")
        if not isinstance(document.get("reviews"), dict) or not isinstance(document.get("history"), list):
            raise PersistedStateError("review file has invalid reviews/history fields")
        has_next_sequence = "next_sequence" in document
        next_sequence = document.get("next_sequence")
        if has_next_sequence and (
            isinstance(next_sequence, bool)
            or not isinstance(next_sequence, int)
            or next_sequence < 1
        ):
            raise PersistedStateError("review file has an invalid next_sequence")
        for stable_id, review in document["reviews"].items():
            self._validate_stored_review(stable_id, review, "reviews")
        last_sequence = 0
        replayed: dict[str, dict] = {}
        for index, mutation in enumerate(document["history"]):
            if not isinstance(mutation, dict):
                raise PersistedStateError(f"review history entry {index} must be an object")
            if set(mutation) != {"sequence", "id", "before", "after", "at"}:
                raise PersistedStateError(f"review history entry {index} has invalid fields")
            sequence = mutation.get("sequence")
            stable_id = mutation.get("id")
            if (
                isinstance(sequence, bool)
                or not isinstance(sequence, int)
                or sequence <= last_sequence
            ):
                raise PersistedStateError("review history sequences must be strictly increasing")
            if not isinstance(stable_id, str) or not stable_id:
                raise PersistedStateError(f"review history entry {index} has an invalid id")
            for field in ("before", "after"):
                value = mutation.get(field)
                if value is not None:
                    self._validate_stored_review(stable_id, value, f"history[{index}].{field}")
            before = mutation["before"]
            after = mutation["after"]
            if before != replayed.get(stable_id):
                raise PersistedStateError(
                    f"review history entry {index} does not follow its prior state"
                )
            if after is None:
                raise PersistedStateError(f"review history entry {index} lacks an after state")
            if not isinstance(mutation.get("at"), str) or not mutation["at"]:
                raise PersistedStateError(f"review history entry {index} has an invalid timestamp")
            if after["reviewed_at"] != mutation["at"]:
                raise PersistedStateError(
                    f"review history entry {index} timestamp does not match its review"
                )
            replayed[stable_id] = after
            last_sequence = sequence
        if not has_next_sequence:
            next_sequence = last_sequence + 1
            document["next_sequence"] = next_sequence
        elif next_sequence <= last_sequence:
            raise PersistedStateError("review file next_sequence must follow its history")
        if replayed != document["reviews"]:
            raise PersistedStateError("review history does not reproduce the current reviews")
        return document

    @staticmethod
    def _validate_stored_review(stable_id: object, review: object, location: str) -> None:
        if not isinstance(stable_id, str) or not stable_id or not isinstance(review, dict):
            raise PersistedStateError(f"review file has an invalid {location} entry")
        expected_fields = set(ReviewStore.semantic_fields) | {"reviewed_at"}
        if set(review) != expected_fields:
            raise PersistedStateError(f"review file has invalid fields in {location}")
        if review.get("id") != stable_id or review.get("status") not in ALLOWED_STATUSES:
            raise PersistedStateError(f"review file has an invalid {location} identity/status")
        start = review.get("proposed_start_seconds")
        end = review.get("proposed_end_seconds")
        if (start is None) != (end is None):
            raise PersistedStateError(f"review file has incomplete bounds in {location}")
        if start is not None:
            try:
                start = finite_number(start, "proposed_start_seconds")
                end = finite_number(end, "proposed_end_seconds")
            except ValidationError as error:
                raise PersistedStateError(f"review file has invalid bounds in {location}") from error
            if end <= start:
                raise PersistedStateError(f"review file has invalid bounds in {location}")
        if review.get("status") in {"approved", "rebounded"} and start is None:
            raise PersistedStateError(f"review file lacks required bounds in {location}")
        note = review.get("note")
        reviewer = review.get("reviewer")
        fingerprint = review.get("audio_fingerprint")
        reviewed_at = review.get("reviewed_at")
        if not isinstance(note, str) or len(note) > MAX_NOTE_LENGTH:
            raise PersistedStateError(f"review file has an invalid note in {location}")
        if not isinstance(reviewer, str) or not reviewer.strip() or len(reviewer) > MAX_REVIEWER_LENGTH:
            raise PersistedStateError(f"review file has an invalid reviewer in {location}")
        if not isinstance(fingerprint, str) or FINGERPRINT_PATTERN.fullmatch(fingerprint) is None:
            raise PersistedStateError(f"review file has an invalid audio fingerprint in {location}")
        if not isinstance(reviewed_at, str) or not reviewed_at:
            raise PersistedStateError(f"review file has an invalid timestamp in {location}")

    def snapshot(self) -> dict:
        with exclusive_file_transaction(self.path, self._lock):
            return json.loads(json.dumps(self._load_unlocked()))

    def save(self, review: dict, *, reviewed_at: str | None = None) -> dict:
        with exclusive_file_transaction(self.path, self._lock):
            document = self._load_unlocked()
            stable_id = review["id"]
            previous = document["reviews"].get(stable_id)
            if previous is not None and all(previous.get(key) == review.get(key) for key in self.semantic_fields):
                return dict(previous)
            timestamp = reviewed_at or utc_now()
            stored = {key: review.get(key) for key in self.semantic_fields}
            stored["reviewed_at"] = timestamp
            sequence = document["next_sequence"]
            document["next_sequence"] = sequence + 1
            document["reviews"][stable_id] = stored
            document["history"].append(
                {
                    "sequence": sequence,
                    "id": stable_id,
                    "before": previous,
                    "after": stored,
                    "at": timestamp,
                }
            )
            atomic_write_json(self.path, document)
            return dict(stored)

    def undo(self) -> dict | None:
        with exclusive_file_transaction(self.path, self._lock):
            document = self._load_unlocked()
            if not document["history"]:
                return None
            mutation = document["history"].pop()
            stable_id = mutation["id"]
            if mutation.get("before") is None:
                document["reviews"].pop(stable_id, None)
            else:
                document["reviews"][stable_id] = mutation["before"]
            atomic_write_json(self.path, document)
            return {"id": stable_id, "review": mutation.get("before")}


class AuditApp:
    def __init__(self, entries: list[dict], store: ReviewStore):
        self.entries = [dict(entry) for entry in entries]
        self.by_id = {entry["id"]: entry for entry in self.entries}
        if len(self.by_id) != len(self.entries):
            raise ValidationError("audit entries contain duplicate IDs")
        self.store = store
        self.validated_reviews()

    def validated_reviews(self) -> dict[str, dict]:
        reviews = self.store.snapshot()["reviews"]
        for stable_id, review in reviews.items():
            semantic = {key: review[key] for key in ReviewStore.semantic_fields}
            try:
                normalized = self.validate_review(semantic)
            except ValidationError as error:
                raise PersistedStateError(
                    f"stored review '{stable_id}' is invalid for the audit ledger: {error}"
                ) from error
            if any(normalized[key] != semantic[key] for key in ReviewStore.semantic_fields):
                raise PersistedStateError(f"stored review '{stable_id}' is not canonical")
        return reviews

    def items_payload(self) -> dict:
        reviews = self.validated_reviews()
        items: list[dict] = []
        for entry in self.entries:
            item = {
                key: value
                for key, value in entry.items()
                if key != "audio" and not key.startswith("_")
            }
            item["audio_url"] = (
                "/audio/" + urllib.parse.quote(entry["id"], safe="")
                if entry.get("audio") else None
            )
            item["review"] = reviews.get(entry["id"])
            items.append(item)
        return {"items": items, "summary": self.summary(reviews), "default_filter": "boundary_work"}

    def summary(self, reviews: dict | None = None) -> dict:
        if reviews is None:
            reviews = self.validated_reviews()
        status_counts = {status: 0 for status in sorted(ALLOWED_STATUSES)}
        for review in reviews.values():
            status = review.get("status")
            if status in status_counts:
                status_counts[status] += 1
        disposition_counts: dict[str, int] = {}
        for entry in self.entries:
            disposition = entry["disposition"]
            disposition_counts[disposition] = disposition_counts.get(disposition, 0) + 1
        boundary_ids = {entry["id"] for entry in self.entries if entry.get("boundary_work")}
        boundary_reviewed = sum(1 for stable_id in boundary_ids if stable_id in reviews)
        return {
            "total": len(self.entries),
            "reviewed": len(reviews),
            "boundary_total": len(boundary_ids),
            "boundary_reviewed": boundary_reviewed,
            "boundary_remaining": len(boundary_ids) - boundary_reviewed,
            "statuses": status_counts,
            "dispositions": disposition_counts,
        }

    def validate_review(self, body: object, *, verify_audio: bool = True) -> dict:
        if not isinstance(body, dict):
            raise ValidationError("request body must be a JSON object")
        allowed_keys = set(ReviewStore.semantic_fields)
        unknown = set(body) - allowed_keys
        if unknown:
            raise ValidationError(f"unknown review fields: {', '.join(sorted(unknown))}")
        stable_id = body.get("id")
        status = body.get("status")
        if not isinstance(stable_id, str) or stable_id not in self.by_id:
            raise ValidationError("id is not present in the audit ledger")
        if status not in ALLOWED_STATUSES:
            raise ValidationError(f"status must be one of: {', '.join(sorted(ALLOWED_STATUSES))}")
        entry = self.by_id[stable_id]
        fingerprint = body.get("audio_fingerprint")
        if fingerprint != entry["audio_fingerprint"]:
            raise ValidationError("review audio_fingerprint does not match the audit item")
        start_value = body.get("proposed_start_seconds")
        end_value = body.get("proposed_end_seconds")
        if status == "approved" and start_value is None and end_value is None:
            start_value = entry["current_start_seconds"]
            end_value = entry["current_end_seconds"]
        if (start_value is None) != (end_value is None):
            raise ValidationError("proposed start and end must be supplied together")
        start: float | None = None
        end: float | None = None
        if start_value is not None:
            start = finite_number(start_value, "proposed_start_seconds")
            end = finite_number(end_value, "proposed_end_seconds")
            if start < 0 or end <= start or end > entry["duration_seconds"]:
                raise ValidationError("proposed bounds must satisfy 0 <= start < end <= duration")
            start = round(start, 3)
            end = min(round(end, 3), entry["duration_seconds"])
            if end <= start:
                raise ValidationError("proposed bounds collapse after millisecond normalization")
        if status == "approved" and (
            start != entry["current_start_seconds"] or end != entry["current_end_seconds"]
        ):
            raise ValidationError("approved reviews must keep current bounds; use rebound for edits")
        if status == "rebounded" and start is None:
            raise ValidationError("rebounded reviews require proposed bounds")
        note = body.get("note", "")
        reviewer = body.get("reviewer", "")
        if not isinstance(note, str) or len(note) > MAX_NOTE_LENGTH:
            raise ValidationError(f"note must be a string of at most {MAX_NOTE_LENGTH} characters")
        if not isinstance(reviewer, str) or not reviewer.strip() or len(reviewer) > MAX_REVIEWER_LENGTH:
            raise ValidationError(
                f"reviewer must be a non-empty string of at most {MAX_REVIEWER_LENGTH} characters"
            )
        normalized = {
            "id": stable_id,
            "status": status,
            "proposed_start_seconds": start,
            "proposed_end_seconds": end,
            "note": note.strip(),
            "reviewer": reviewer.strip(),
            "audio_fingerprint": fingerprint,
        }

        if verify_audio:
            with self.open_audio(stable_id):
                pass
        return normalized

    def save_review(self, body: object) -> dict:
        self.validated_reviews()
        normalized = self.validate_review(body, verify_audio=False)
        with self.open_audio(normalized["id"]):
            return self.store.save(normalized)

    def undo_review(self) -> dict | None:
        self.validated_reviews()
        return self.store.undo()

    @contextlib.contextmanager
    def open_audio(self, stable_id: str):
        """Yield one fingerprint-verified descriptor for a complete operation."""
        entry = self.by_id.get(stable_id)
        if entry is None or not entry.get("audio"):
            raise ValidationError("matching audit audio must be staged")
        audio_root = pathlib.Path(entry.get("_audio_root", ""))
        path = pathlib.Path(entry["audio"])
        if (
            not path.is_absolute()
            or not audio_root.is_absolute()
            or path.stem != entry["episode_id"]
            or path.suffix.lower() not in AUDIO_EXTENSIONS
        ):
            raise ValidationError("matching audit audio path is unsafe")
        try:
            if path.parent.resolve(strict=True) != audio_root:
                raise ValidationError("matching audit audio path is unsafe")
        except OSError as error:
            raise ValidationError("matching audit audio must be staged") from error

        root_flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        file_flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        root_descriptor = -1
        descriptor = -1
        try:
            root_descriptor = os.open(audio_root, root_flags)
            descriptor = os.open(path.name, file_flags, dir_fd=root_descriptor)
            with os.fdopen(descriptor, "rb") as handle:
                descriptor = -1
                metadata = os.fstat(handle.fileno())
                if not stat.S_ISREG(metadata.st_mode):
                    raise ValidationError("matching audit audio is not a regular file")
                if audio_file_identity(metadata) != entry.get("_audio_identity"):
                    raise ValidationError("matching audit audio changed during verification")
                yield path, handle
        except OSError as error:
            raise ValidationError("matching audit audio must be staged") from error
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if root_descriptor >= 0:
                os.close(root_descriptor)


def parse_byte_range(value: str | None, size: int) -> tuple[int, int] | None:
    if value is None:
        return None
    match = re.fullmatch(r"bytes=(\d*)-(\d*)", value.strip())
    if match is None or size <= 0:
        raise RangeNotSatisfiable(value)
    first, last = match.groups()
    if not first and not last:
        raise RangeNotSatisfiable(value)
    try:
        start_value = int(first) if first else None
        end_value = int(last) if last else None
    except ValueError as error:
        raise RangeNotSatisfiable(value) from error
    if start_value is None:
        suffix = end_value
        if suffix <= 0:
            raise RangeNotSatisfiable(value)
        start = max(0, size - suffix)
        return start, size - 1
    start = start_value
    if start >= size:
        raise RangeNotSatisfiable(value)
    end = size - 1 if end_value is None else min(end_value, size - 1)
    if end < start:
        raise RangeNotSatisfiable(value)
    return start, end


def audio_mime_type(path: pathlib.Path) -> str:
    overrides = {".m4a": "audio/mp4", ".mp4": "audio/mp4", ".aac": "audio/aac"}
    return overrides.get(path.suffix.lower()) or mimetypes.guess_type(path.name)[0] or "application/octet-stream"


PAGE = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>L2F ear audit</title>
<style>
:root{color-scheme:dark;--bg:#111315;--panel:#181b1e;--surface:#202429;--line:#353b42;--text:#f3f4f5;--muted:#a7afb8;--blue:#5eb1ef;--green:#57c785;--amber:#f2b84b;--red:#ef6a6a;--focus:#b8dcff}
@media(prefers-color-scheme:light){:root{color-scheme:light;--bg:#f4f5f6;--panel:#fff;--surface:#eef0f2;--line:#cbd0d5;--text:#17191b;--muted:#59636d;--blue:#176fa8;--green:#177744;--amber:#8b5b00;--red:#af2929;--focus:#005b96}}
*{box-sizing:border-box;letter-spacing:0}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
button,input,select,textarea{font:inherit;color:inherit}button,select,input,textarea{border:1px solid var(--line);border-radius:6px;background:var(--surface)}button{min-height:40px;padding:8px 12px;font-weight:650;cursor:pointer}button:hover{border-color:var(--blue)}button:focus-visible,input:focus-visible,select:focus-visible,textarea:focus-visible,audio:focus-visible{outline:3px solid var(--focus);outline-offset:2px}button:disabled{opacity:.45;cursor:not-allowed}
.shell{width:min(1180px,100%);margin:0 auto;padding:18px}.topbar{display:flex;align-items:flex-end;justify-content:space-between;gap:16px;margin-bottom:14px}h1{font-size:20px;margin:0}.summary{color:var(--muted);text-align:right;font-variant-numeric:tabular-nums}
.toolbar{display:grid;grid-template-columns:minmax(180px,240px) minmax(180px,1fr) auto auto auto;gap:10px;align-items:end;margin-bottom:12px}.field{display:grid;gap:5px;min-width:0}.field>span,.label{color:var(--muted);font-size:12px;font-weight:650}select,input,textarea{width:100%;padding:8px 10px;min-height:40px}.progress{height:7px;background:var(--surface);border:1px solid var(--line);border-radius:6px;overflow:hidden;margin-bottom:14px}.progress span{display:block;height:100%;width:0;background:var(--green)}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:18px}.item-head{display:grid;grid-template-columns:1fr auto;gap:18px;align-items:start}.show{font-size:22px;font-weight:750}.episode{color:var(--muted);overflow-wrap:anywhere;margin-top:2px}.position{font-variant-numeric:tabular-nums;color:var(--muted);text-align:right}.badges{display:flex;gap:7px;flex-wrap:wrap;margin:14px 0}.badge{border:1px solid var(--line);border-radius:999px;padding:3px 8px;font-size:12px}.badge.tier{border-color:var(--amber);color:var(--amber)}.badge.reviewed{border-color:var(--green);color:var(--green)}.recommendation{padding:10px 0;border-top:1px solid var(--line);border-bottom:1px solid var(--line)}
.audio-area{margin-top:16px}audio{width:100%;height:42px}.transport{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;margin-top:9px}.timeline{position:relative;height:34px;margin:14px 0 6px;border:1px solid var(--line);background:var(--surface);border-radius:6px;overflow:hidden}.timeline .fill{position:absolute;inset:0 auto 0 0;width:0;background:color-mix(in srgb,var(--blue) 18%,transparent)}.marker{position:absolute;top:0;bottom:0;width:2px;background:var(--amber)}.marker.proposed{background:var(--green);border-left:1px dashed var(--panel);width:3px}.timeline-labels{display:flex;justify-content:space-between;color:var(--muted);font-size:12px;font-variant-numeric:tabular-nums}.legend{display:flex;gap:16px;color:var(--muted);font-size:12px}.legend i{display:inline-block;width:11px;height:2px;background:var(--amber);vertical-align:middle;margin-right:5px}.legend i.proposed{background:var(--green)}
.bounds{display:grid;grid-template-columns:1fr auto 1fr auto;gap:9px;align-items:end;margin-top:16px}.change-preview{margin:13px 0;color:var(--muted);font-variant-numeric:tabular-nums}.review-fields{display:grid;grid-template-columns:1fr 240px;gap:12px;margin-top:12px}textarea{min-height:82px;resize:vertical}.actions{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:9px;margin-top:14px}.actions [data-status="approved"]{border-color:var(--green)}.actions [data-status="rebounded"]{border-color:var(--blue)}.actions [data-status="rejected"]{border-color:var(--red)}.actions [data-status="unsure"]{border-color:var(--amber)}.message{min-height:22px;margin-top:10px;color:var(--muted)}.message.error{color:var(--red)}
.empty{padding:60px 10px;text-align:center;color:var(--muted)}
@media(max-width:760px){.shell{padding:12px}.topbar{align-items:start;flex-direction:column}.summary{text-align:left}.toolbar{grid-template-columns:1fr 1fr}.toolbar button{width:100%}.panel{padding:14px}.item-head{grid-template-columns:1fr}.position{text-align:left}.transport{grid-template-columns:repeat(2,1fr)}.bounds{grid-template-columns:1fr auto}.review-fields{grid-template-columns:1fr}.actions{grid-template-columns:1fr 1fr}}
@media(max-width:430px){.toolbar{grid-template-columns:1fr}.actions,.bounds{grid-template-columns:1fr}.transport{grid-template-columns:1fr 1fr}.show{font-size:19px}button{min-width:0;overflow-wrap:anywhere}}
</style>
</head>
<body>
<div class="shell">
  <header class="topbar"><div><h1>L2F ear audit</h1><div class="label">Rediff R3 boundary review</div></div><div id="summary" class="summary">Loading...</div></header>
  <div class="toolbar" aria-label="Audit filters and navigation">
    <label class="field"><span>Queue</span><select id="queueFilter"><option value="boundary_work">Boundary work</option><option value="all">All completed audits</option><option value="tight">Tight</option><option value="rejected">Rejected</option><option value="approved">Approved</option><option value="unsure">Unsure</option></select></label>
    <label class="field"><span>Show</span><select id="showFilter"><option value="">All shows</option></select></label>
    <button id="previous" type="button" title="Previous item">Previous</button>
    <button id="next" type="button" title="Next item">Next</button>
    <button id="undo" type="button" title="Undo latest saved review">Undo</button>
  </div>
  <div class="progress" aria-label="Boundary review progress"><span id="progressBar"></span></div>
  <main id="panel" class="panel">
    <div id="itemView">
      <div class="item-head"><div><div id="showName" class="show"></div><div id="episodeId" class="episode"></div></div><div id="position" class="position"></div></div>
      <div class="badges"><span id="disposition" class="badge"></span><span id="rawVerdict" class="badge"></span><span id="provenance" class="badge tier"></span><span id="reviewStatus" class="badge reviewed"></span></div>
      <div id="recommendation" class="recommendation"></div>
      <section class="audio-area" aria-label="Audio boundary review">
        <audio id="audio" controls preload="metadata"></audio>
        <div class="transport">
          <button id="playCurrent" type="button">Play current</button>
          <button id="playLead" type="button">Lead-in</button>
          <button id="playTail" type="button">Tail</button>
          <button id="context10" type="button">-10 / +10</button>
          <button id="context30" type="button">-30 / +30</button>
          <button id="nudgeBack" type="button" title="Jump back 10s from wherever the playhead is ([)">&#171; 10s</button>
          <button id="nudgeForward" type="button" title="Jump forward 10s from wherever the playhead is (])">10s &#187;</button>
          <button id="playProposedStart" type="button" title="Play across the proposed start (5s either side)">Prop start</button>
          <button id="playProposedEnd" type="button" title="Play across the proposed end (5s either side)">Prop end</button>
        </div>
        <div id="timeline" class="timeline" aria-label="Current and proposed boundary markers"><span id="timelineFill" class="fill"></span><span id="currentStartMarker" class="marker"></span><span id="currentEndMarker" class="marker"></span><span id="proposedStartMarker" class="marker proposed"></span><span id="proposedEndMarker" class="marker proposed"></span></div>
        <div class="timeline-labels"><span id="contextStart"></span><span id="contextEnd"></span></div>
        <div class="legend"><span><i></i>Current</span><span><i class="proposed"></i>Proposed</span></div>
      </section>
      <section class="bounds" aria-label="Proposed boundaries">
        <label class="field"><span>Start seconds</span><input id="proposedStart" type="number" min="0" step="0.1" inputmode="decimal"></label><button id="setStart" type="button" title="Set start from audio playhead">Set from playhead</button>
        <label class="field"><span>End seconds</span><input id="proposedEnd" type="number" min="0" step="0.1" inputmode="decimal"></label><button id="setEnd" type="button" title="Set end from audio playhead">Set from playhead</button>
      </section>
      <div id="changePreview" class="change-preview"></div>
      <section class="review-fields" aria-label="Review details">
        <label class="field"><span>Review note</span><textarea id="reviewNote" maxlength="4000"></textarea></label>
        <label class="field"><span>Reviewer</span><input id="reviewer" maxlength="120" autocomplete="name"></label>
      </section>
      <div class="actions">
        <button type="button" data-status="approved">Keep / approved</button>
        <button type="button" data-status="rebounded">Save rebound</button>
        <button type="button" data-status="rejected">Reject</button>
        <button type="button" data-status="unsure">Unsure</button>
      </div>
      <div id="message" class="message" role="status" aria-live="polite"></div>
    </div>
    <div id="emptyView" class="empty" hidden>No items match the selected filters.</div>
  </main>
</div>
<script>
"use strict";
const el={queue:document.getElementById("queueFilter"),showFilter:document.getElementById("showFilter"),previous:document.getElementById("previous"),next:document.getElementById("next"),undo:document.getElementById("undo"),summary:document.getElementById("summary"),progress:document.getElementById("progressBar"),itemView:document.getElementById("itemView"),empty:document.getElementById("emptyView"),show:document.getElementById("showName"),episode:document.getElementById("episodeId"),position:document.getElementById("position"),disposition:document.getElementById("disposition"),rawVerdict:document.getElementById("rawVerdict"),provenance:document.getElementById("provenance"),reviewStatus:document.getElementById("reviewStatus"),recommendation:document.getElementById("recommendation"),audio:document.getElementById("audio"),start:document.getElementById("proposedStart"),end:document.getElementById("proposedEnd"),note:document.getElementById("reviewNote"),reviewer:document.getElementById("reviewer"),preview:document.getElementById("changePreview"),message:document.getElementById("message"),timeline:document.getElementById("timeline"),fill:document.getElementById("timelineFill"),currentStart:document.getElementById("currentStartMarker"),currentEnd:document.getElementById("currentEndMarker"),proposedStart:document.getElementById("proposedStartMarker"),proposedEnd:document.getElementById("proposedEndMarker"),contextStart:document.getElementById("contextStart"),contextEnd:document.getElementById("contextEnd")};
let items=[],visible=[],index=0,summary={},stopAt=null,mutationPending=false;
function current(){return visible[index]||null}
function formatSeconds(value){if(value===null||value===undefined||!Number.isFinite(Number(value)))return "--";const total=Number(value);const minutes=Math.floor(total/60);const seconds=(total-minutes*60).toFixed(1).padStart(4,"0");return minutes+":"+seconds}
function statusOf(item){return item&&item.review?item.review.status:null}
function setMessage(text,error=false){el.message.textContent=text||"";el.message.classList.toggle("error",error)}
function updateSummary(){const statuses=summary.statuses||{};el.summary.textContent=`Boundary ${summary.boundary_reviewed||0}/${summary.boundary_total||0} reviewed | ${summary.boundary_remaining||0} remaining | ${statuses.rebounded||0} rebound | ${statuses.rejected||0} rejected`;const total=summary.boundary_total||0;el.progress.style.width=(total?100*(summary.boundary_reviewed||0)/total:0)+"%"}
function populateShows(){const selected=el.showFilter.value;const shows=[...new Set(items.map(item=>item.show_name))].sort((a,b)=>a.localeCompare(b));el.showFilter.replaceChildren();const all=document.createElement("option");all.value="";all.textContent="All shows";el.showFilter.appendChild(all);for(const show of shows){const option=document.createElement("option");option.value=show;option.textContent=show;el.showFilter.appendChild(option)}el.showFilter.value=shows.includes(selected)?selected:""}
function matchesQueue(item){const filter=el.queue.value;const status=statusOf(item);if(filter==="all")return true;if(filter==="boundary_work")return item.boundary_work;if(filter==="tight")return item.disposition==="tight_ad";if(filter==="rejected")return item.disposition==="rejected"||status==="rejected";if(filter==="approved")return status==="approved"||status==="rebounded";if(filter==="unsure")return status==="unsure";return true}
function applyFilters(preferredId=null){visible=items.filter(item=>matchesQueue(item)&&(!el.showFilter.value||item.show_name===el.showFilter.value));let wanted=preferredId||localStorage.getItem("l2f-earaudit-last-id");let found=visible.findIndex(item=>item.id===wanted);if(found<0)found=visible.findIndex(item=>!item.review);index=found>=0?found:0;render()}
function setText(node,value){node.textContent=value===null||value===undefined?"":String(value)}
function valueOrNull(input){if(input.value.trim()==="")return null;const value=Number(input.value);return Number.isFinite(value)?value:null}
function bounds(){return {start:valueOrNull(el.start),end:valueOrNull(el.end)}}
function updatePreview(){const item=current();if(!item)return;const proposed=bounds();if(proposed.start===null||proposed.end===null){setText(el.preview,"Proposed bounds are incomplete.");updateMarkers();return}const ds=proposed.start-item.current_start_seconds;const de=proposed.end-item.current_end_seconds;const duration=proposed.end-proposed.start;setText(el.preview,`Current ${formatSeconds(item.current_start_seconds)} - ${formatSeconds(item.current_end_seconds)} | Proposed ${formatSeconds(proposed.start)} - ${formatSeconds(proposed.end)} | Start ${ds>=0?"+":""}${ds.toFixed(1)}s | End ${de>=0?"+":""}${de.toFixed(1)}s | ${duration.toFixed(1)}s total`);updateMarkers()}
function markerPosition(value,start,end){return Math.max(0,Math.min(100,100*(value-start)/(end-start)))}
function updateMarkers(){const item=current();if(!item)return;const contextFrom=Math.max(0,item.current_start_seconds-30);const contextTo=Math.min(item.duration_seconds,item.current_end_seconds+30);setText(el.contextStart,formatSeconds(contextFrom));setText(el.contextEnd,formatSeconds(contextTo));const proposed=bounds();const markers=[[el.currentStart,item.current_start_seconds],[el.currentEnd,item.current_end_seconds],[el.proposedStart,proposed.start],[el.proposedEnd,proposed.end]];for(const pair of markers){const node=pair[0],value=pair[1];node.hidden=value===null;if(value!==null)node.style.left=markerPosition(value,contextFrom,contextTo)+"%"}const playhead=Number(el.audio.currentTime);el.fill.style.width=(Number.isFinite(playhead)?markerPosition(playhead,contextFrom,contextTo):0)+"%"}
function render(){const item=current();el.itemView.hidden=!item;el.empty.hidden=!!item;el.previous.disabled=!item||visible.length<2;el.next.disabled=!item||visible.length<2;stopAt=null;el.audio.pause();if(!item)return;localStorage.setItem("l2f-earaudit-last-id",item.id);setText(el.show,item.show_name);setText(el.episode,item.episode_id);setText(el.position,`${index+1} of ${visible.length} | ${formatSeconds(item.current_start_seconds)} - ${formatSeconds(item.current_end_seconds)}`);setText(el.disposition,item.disposition.replaceAll("_"," "));setText(el.rawVerdict,"raw: "+item.raw_verdict);setText(el.provenance,item.provenance_tier.replaceAll("_"," "));setText(el.reviewStatus,item.review?item.review.status:"not reviewed");setText(el.recommendation,item.recommendation);const review=item.review||{};el.start.value=review.proposed_start_seconds??item.current_start_seconds;el.end.value=review.proposed_end_seconds??item.current_end_seconds;el.note.value=review.note||"";el.reviewer.value=review.reviewer||localStorage.getItem("l2f-earaudit-reviewer")||"";if(item.audio_url){el.audio.src=item.audio_url;el.audio.hidden=false}else{el.audio.removeAttribute("src");el.audio.load();el.audio.hidden=true;setMessage("Audio is not staged. Pass --audio-dir to the local corpus audio directory.",true)}for(const button of document.querySelectorAll(".transport button,#setStart,#setEnd"))button.disabled=!item.audio_url;updatePreview()}
function move(delta){if(!visible.length)return;index=(index+delta+visible.length)%visible.length;setMessage("");render()}
function setMutationPending(pending){mutationPending=pending;for(const button of document.querySelectorAll("[data-status],#undo"))button.disabled=pending}
function seekAndPlay(start,end){const item=current();if(!item||!item.audio_url)return;const stableId=item.id;const begin=Math.max(0,start);stopAt=Math.min(item.duration_seconds,end);const play=()=>{if(!current()||current().id!==stableId)return;el.audio.currentTime=begin;el.audio.play().catch(error=>setMessage(error.message,true))};if(el.audio.readyState>=1)play();else el.audio.addEventListener("loadedmetadata",play,{once:true})}
function playCurrent(){const item=current();if(item)seekAndPlay(item.current_start_seconds,item.current_end_seconds)}
function nudge(delta){const item=current();if(!item)return;stopAt=null;const cap=item.duration_seconds||el.audio.duration||0;el.audio.currentTime=Math.min(Math.max(0,el.audio.currentTime+delta),cap);if(el.audio.paused)el.audio.play().catch(error=>setMessage(error.message,true))}
function playProposedBoundary(which){const raw=which==="start"?el.start.value:el.end.value;const value=parseFloat(raw);if(!Number.isFinite(value)){setMessage("no proposed "+which+" value set",true);return}seekAndPlay(value-5,value+5)}
function playContext(seconds){const item=current();if(item)seekAndPlay(item.current_start_seconds-seconds,item.current_end_seconds+seconds)}
async function jsonRequest(path,options={}){const response=await fetch(path,options);let payload={};try{payload=await response.json()}catch(error){payload={error:"Invalid server response"}}if(!response.ok)throw new Error(payload.error||`Request failed (${response.status})`);return payload}
async function saveReview(status){const item=current();if(!item||mutationPending)return;const submittedId=item.id;const proposed=bounds();if(status==="rebounded"&&(proposed.start===null||proposed.end===null||proposed.start>=proposed.end)){setMessage("Rebound requires valid start and end bounds.",true);return}localStorage.setItem("l2f-earaudit-reviewer",el.reviewer.value);const body={id:submittedId,status:status,proposed_start_seconds:proposed.start,proposed_end_seconds:proposed.end,note:el.note.value,reviewer:el.reviewer.value,audio_fingerprint:item.audio_fingerprint};const submittedIndex=visible.findIndex(candidate=>candidate.id===submittedId);const nextId=visible.length>1&&submittedIndex>=0?visible[(submittedIndex+1)%visible.length].id:null;setMutationPending(true);try{const payload=await jsonRequest("/api/reviews",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});item.review=payload.review;summary=payload.summary;updateSummary();const activeId=current()?.id;applyFilters(activeId===submittedId?nextId:activeId);setMessage(`Saved ${status}.`)}catch(error){setMessage(error.message,true)}finally{setMutationPending(false)}}
async function undo(){if(mutationPending)return;setMutationPending(true);try{const payload=await jsonRequest("/api/undo",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"});summary=payload.summary;const item=items.find(candidate=>candidate.id===payload.id);if(item)item.review=payload.review;updateSummary();applyFilters(payload.id);setMessage(payload.id?"Latest review undone.":"Nothing to undo.")}catch(error){setMessage(error.message,true)}finally{setMutationPending(false)}}
async function boot(){try{const payload=await jsonRequest("/api/items");items=payload.items;summary=payload.summary;el.queue.value=payload.default_filter||"boundary_work";populateShows();updateSummary();applyFilters()}catch(error){el.itemView.hidden=true;el.empty.hidden=false;setText(el.empty,error.message)}}
el.queue.addEventListener("change",()=>{setMessage("");applyFilters()});el.showFilter.addEventListener("change",()=>{setMessage("");applyFilters()});el.previous.addEventListener("click",()=>move(-1));el.next.addEventListener("click",()=>move(1));el.undo.addEventListener("click",undo);el.start.addEventListener("input",updatePreview);el.end.addEventListener("input",updatePreview);el.audio.addEventListener("timeupdate",()=>{if(stopAt!==null&&el.audio.currentTime>=stopAt){el.audio.pause();stopAt=null}updateMarkers()});document.getElementById("playCurrent").addEventListener("click",playCurrent);document.getElementById("playLead").addEventListener("click",()=>{const item=current();if(item)seekAndPlay(item.current_start_seconds-10,item.current_start_seconds)});document.getElementById("playTail").addEventListener("click",()=>{const item=current();if(item)seekAndPlay(item.current_end_seconds,item.current_end_seconds+10)});document.getElementById("context10").addEventListener("click",()=>playContext(10));document.getElementById("context30").addEventListener("click",()=>playContext(30));document.getElementById("nudgeBack").addEventListener("click",()=>nudge(-10));document.getElementById("nudgeForward").addEventListener("click",()=>nudge(10));document.getElementById("playProposedStart").addEventListener("click",()=>playProposedBoundary("start"));document.getElementById("playProposedEnd").addEventListener("click",()=>playProposedBoundary("end"));document.getElementById("setStart").addEventListener("click",()=>{el.start.value=(Math.round(el.audio.currentTime*10)/10).toFixed(1);updatePreview()});document.getElementById("setEnd").addEventListener("click",()=>{el.end.value=(Math.round(el.audio.currentTime*10)/10).toFixed(1);updatePreview()});for(const button of document.querySelectorAll("[data-status]"))button.addEventListener("click",()=>saveReview(button.dataset.status));document.addEventListener("keydown",event=>{const tag=event.target.tagName;if(["INPUT","TEXTAREA","SELECT"].includes(tag))return;if((event.metaKey||event.ctrlKey)&&event.key.toLowerCase()==="z"){event.preventDefault();undo();return}if(event.key==="ArrowLeft")move(-1);else if(event.key==="ArrowRight")move(1);else if(event.key===" "){event.preventDefault();if(el.audio.paused)playCurrent();else el.audio.pause()}else if(event.key.toLowerCase()==="s"){el.start.value=(Math.round(el.audio.currentTime*10)/10).toFixed(1);updatePreview()}else if(event.key.toLowerCase()==="e"){el.end.value=(Math.round(el.audio.currentTime*10)/10).toFixed(1);updatePreview()}else if(event.key==="[")nudge(-10);else if(event.key==="]")nudge(10);else if(event.key.toLowerCase()==="a")saveReview("approved");else if(event.key.toLowerCase()==="r")saveReview("rebounded");else if(event.key.toLowerCase()==="x")saveReview("rejected");else if(event.key.toLowerCase()==="u")saveReview("unsure")});boot();
</script>
</body>
</html>'''


def make_handler(app: AuditApp) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "L2FEarAudit/1"

        def log_message(self, format_string: str, *args: object) -> None:
            return

        def end_headers(self) -> None:
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; style-src 'self' 'unsafe-inline'; "
                "script-src 'self' 'unsafe-inline'; media-src 'self'; connect-src 'self'; "
                "base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
            )
            super().end_headers()

        def send_json(self, document: object, status: int = 200) -> None:
            payload = json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(payload)

        def send_json_error(self, status: int, message: str) -> None:
            self.send_json({"error": message}, status)

        def read_json_body(self) -> object:
            content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            if content_type != "application/json":
                raise ValidationError("Content-Type must be application/json")
            raw_length = self.headers.get("Content-Length")
            if raw_length is None:
                raise ValidationError("Content-Length is required")
            try:
                length = int(raw_length)
            except ValueError as error:
                raise ValidationError("Content-Length must be an integer") from error
            if length < 0:
                raise ValidationError("Content-Length must not be negative")
            if length > MAX_BODY_BYTES:
                raise OverflowError("request body is too large")
            try:
                return json.loads(self.rfile.read(length))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ValidationError(f"request body is not valid JSON: {error}") from error

        def do_GET(self) -> None:
            self.route_get(send_body=True)

        def do_HEAD(self) -> None:
            self.route_get(send_body=False)

        def route_get(self, *, send_body: bool) -> None:
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path == "/":
                payload = PAGE.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                if send_body:
                    self.wfile.write(payload)
                return
            if parsed.path == "/api/items":
                try:
                    self.send_json(app.items_payload())
                except PersistedStateError as error:
                    self.send_json_error(500, str(error))
                return
            if parsed.path == "/api/summary":
                try:
                    self.send_json(app.summary())
                except PersistedStateError as error:
                    self.send_json_error(500, str(error))
                return
            if parsed.path.startswith("/audio/"):
                encoded_id = parsed.path[len("/audio/"):]
                if not encoded_id or "/" in encoded_id:
                    self.send_json_error(400, "invalid audio id")
                    return
                stable_id = urllib.parse.unquote(encoded_id)
                try:
                    with app.open_audio(stable_id) as (path, handle):
                        self.serve_audio(path, handle, send_body=send_body)
                except ValidationError:
                    self.send_json_error(404, "audio is not staged")
                return
            self.send_json_error(404, "not found")

        def serve_audio(
            self,
            path: pathlib.Path,
            handle: object,
            *,
            send_body: bool,
        ) -> None:
            size = os.fstat(handle.fileno()).st_size
            try:
                byte_range = parse_byte_range(self.headers.get("Range"), size)
            except RangeNotSatisfiable:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{size}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            start, end = byte_range if byte_range is not None else (0, size - 1)
            length = end - start + 1
            self.send_response(206 if byte_range is not None else 200)
            self.send_header("Content-Type", audio_mime_type(path))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(length))
            if byte_range is not None:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.end_headers()
            if not send_body:
                return
            try:
                handle.seek(start)
                remaining = length
                while remaining:
                    chunk = handle.read(min(64 * 1024, remaining))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    remaining -= len(chunk)
            except (BrokenPipeError, ConnectionResetError, OSError):
                return

        def do_POST(self) -> None:
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path not in {"/api/reviews", "/api/undo"}:
                self.send_json_error(404, "not found")
                return
            try:
                body = self.read_json_body()
                if parsed.path == "/api/reviews":
                    review = app.save_review(body)
                    self.send_json({"review": review, "summary": app.summary()})
                    return
                if body != {}:
                    raise ValidationError("undo body must be an empty object")
                undone = app.undo_review()
                self.send_json(
                    {
                        "id": undone["id"] if undone else None,
                        "review": undone["review"] if undone else None,
                        "summary": app.summary(),
                    }
                )
            except OverflowError as error:
                self.send_json_error(413, str(error))
            except PersistedStateError as error:
                self.send_json_error(500, str(error))
            except ValidationError as error:
                status = 415 if str(error).startswith("Content-Type") else 422
                if "valid JSON" in str(error) or "Content-Length" in str(error):
                    status = 400
                self.send_json_error(status, str(error))
            except OSError:
                self.send_json_error(500, "review state could not be persisted")

    return Handler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="Bind address; use 0.0.0.0 for LAN access")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--no-open", action="store_true", help="Do not open the browser")
    parser.add_argument("--audio-dir", type=pathlib.Path, default=DEFAULT_AUDIO_DIR)
    parser.add_argument("--audit-ledger", type=pathlib.Path, default=DEFAULT_AUDIT_LEDGER)
    parser.add_argument("--review-file", type=pathlib.Path, default=DEFAULT_REVIEW_FILE)
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65_535:
        parser.error("--port must be between 1 and 65535")
    try:
        entries = load_audit_entries(args.audit_ledger, args.audio_dir)
        app = AuditApp(entries, ReviewStore(args.review_file))
        summary = app.summary()
    except ValidationError as error:
        parser.error(str(error))
    server = ThreadingHTTPServer((args.host, args.port), make_handler(app))
    browser_host = "127.0.0.1" if args.host in {"0.0.0.0", "::"} else args.host
    url = f"http://{browser_host}:{server.server_port}"
    print(f"L2F ear audit: {url}")
    print(
        f"  {summary['total']} completed audit records; "
        f"{summary['boundary_total']} boundary-work items; "
        f"{sum(1 for item in entries if item['audio_available'])} audio files staged"
    )
    print(f"  Reviews: {args.review_file}")
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        print(f"  LAN bind active on {args.host}; stop the server when review is complete")
    if not args.no_open:
        threading.Timer(0.4, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
