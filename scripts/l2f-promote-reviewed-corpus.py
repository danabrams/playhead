#!/usr/bin/env python3
"""
Promote reviewed playhead-l2f audio decisions into corpus annotations.

Default mode is report-only. Pass --promote to write
TestFixtures/Corpus/Annotations/<episode_id>.json for episodes with no
blocking review, metadata, timing, audio, or duration issues.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import wave
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from l2f_canonical_manifest import (
    MANIFEST_FILENAME as CANONICAL_MANIFEST_FILENAME,
    CanonicalCorpusError,
    commit_canonical_annotations,
    load_canonical_annotations,
    reject_veto_precommit,
)
from convert_annotations_to_chapter_goldens import (
    ConversionError,
    annotation_decision,
    has_distinct_review_attestations,
    is_gold_annotation,
    load_review_artifacts,
    path_has_symlink_component,
    validate_annotation,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
CORPUS_DIR = REPO_ROOT / "TestFixtures" / "Corpus"
DEFAULT_REVIEW_FILE = CORPUS_DIR / "Drafts" / "l2f-audio-review.json"
DEFAULT_ANNOTATIONS_DIR = CORPUS_DIR / "Annotations"
DEFAULT_AUDIO_DIR = CORPUS_DIR / "Audio"
DEFAULT_REJECTS_FILE = CORPUS_DIR / "Snapshots" / "audit-rejects.jsonl"
AUDIO_EXTENSIONS = {".m4a", ".mp3", ".mp4", ".aac", ".wav", ".flac"}
VALID_AD_TYPES = {
    "host_read",
    "dynamic_insertion",
    "blended_host_read",
    "produced_segment",
    "promo",
}
VALID_TRANSITION_TYPES = {"explicit", "musical", "hard_cut", "blended"}
FINAL_REVIEW_STATUSES = {"verified_ad", "false_positive", "zero_ad_confirmed"}
BOUNDARY_TOLERANCE_SECONDS = 0.05
FIRST_PASS_PROVENANCE = "human_first_pass"
SECOND_PASS_PROVENANCE = "human_reviewed"
AUDIO_PROBE_TIMEOUT_SECONDS = 10


@dataclass
class EpisodeReport:
    episode_id: str
    entries: list[dict[str, Any]]
    issues: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    annotation: dict[str, Any] | None = None
    skipped: bool = False
    skip_reason: str | None = None
    audio_path: Path | None = None
    duration_seconds: float | None = None

    @property
    def ready(self) -> bool:
        return not self.issues and self.annotation is not None


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report on or promote reviewed L2F audio review decisions."
    )
    parser.add_argument(
        "--review-file",
        default=str(DEFAULT_REVIEW_FILE),
        help="GUI review JSON. Default: TestFixtures/Corpus/Drafts/l2f-audio-review.json",
    )
    parser.add_argument(
        "--queue",
        help="Review queue JSON. Defaults to review-file queue_path, then Drafts review queues.",
    )
    parser.add_argument(
        "--audio-dir",
        default=str(DEFAULT_AUDIO_DIR),
        help="Directory containing local <episode_id> audio files.",
    )
    parser.add_argument(
        "--annotations-dir",
        default=str(DEFAULT_ANNOTATIONS_DIR),
        help="Output directory for promoted annotation JSON.",
    )
    parser.add_argument(
        "--reviews-dir",
        help="Immutable review artifacts. Defaults to a Reviews sibling of annotations-dir.",
    )
    parser.add_argument(
        "--episode",
        action="append",
        default=[],
        help="Promote/report only this episode_id. May be passed multiple times.",
    )
    parser.add_argument(
        "--promote",
        action="store_true",
        help="Write annotations. Without this flag the command only reports readiness.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing annotation file during --promote.",
    )
    parser.add_argument(
        "--second-pass-reviewed",
        action="store_true",
        help=(
            "Promote an existing first-pass annotation to human-reviewed gold. "
            "Requires --promote --force after a second listener has checked it."
        ),
    )
    parser.add_argument(
        "--reviewer-id",
        help="Stable non-empty identity for an optional asset-bound review attestation.",
    )
    parser.add_argument(
        "--reviewed-at",
        help="Timestamp for --reviewer-id. Both fields are required together.",
    )
    parser.add_argument(
        "--rejects-file",
        default=str(DEFAULT_REJECTS_FILE),
        help=argparse.SUPPRESS,
    )
    return parser.parse_args(argv)


def resolve_path(raw: str | Path, base: Path = REPO_ROOT) -> Path:
    """Make an input path absolute without resolving away alias evidence."""
    path = Path(raw)
    if not path.is_absolute():
        if ".." in path.parts:
            raise ValueError(f"input path must not traverse a parent directory: {raw}")
        path = base / path
    return Path(os.path.abspath(path))


def lexical_output_path(raw: str | Path, base: Path = REPO_ROOT) -> Path:
    """Make an output path absolute without resolving away symlink evidence."""
    path = Path(raw)
    if not path.is_absolute():
        path = base / path
    return Path(os.path.abspath(path))


def resolve_local_then_repo(raw: str | Path, local_base: Path) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return Path(os.path.abspath(path))
    if ".." in path.parts:
        raise ValueError(f"input path must not traverse a parent directory: {raw}")
    local = Path(os.path.abspath(local_base / path))
    if path_has_symlink_component(local):
        raise ValueError(f"input path contains a symbolic link: {local}")
    if local.exists() or local.is_symlink():
        return local
    repo = Path(os.path.abspath(REPO_ROOT / path))
    if path_has_symlink_component(repo):
        raise ValueError(f"input path contains a symbolic link: {repo}")
    if repo.exists() or repo.is_symlink():
        return repo
    return local


def require_safe_input_path(path: Path, *, label: str) -> None:
    if path_has_symlink_component(path):
        raise ValueError(f"{label} contains a symbolic link: {path}")


def load_json(path: Path, default: Any = None) -> Any:
    """Read one regular JSON input without following aliases."""
    require_safe_input_path(path, label="JSON input")
    if not path.exists():
        return default
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ValueError(f"JSON input is unavailable or unsafe: {path}") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"JSON input is not a regular file: {path}")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            return json.load(handle)
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def choose_queue_path(
    args: argparse.Namespace,
    review_path: Path,
    review_payload: dict[str, Any] | None,
) -> Path:
    if args.queue:
        return resolve_path(args.queue)
    if review_payload:
        queue_path = review_payload.get("queue_path")
        if isinstance(queue_path, str) and queue_path.strip():
            return resolve_local_then_repo(queue_path, review_path.parent)
    for candidate in (
        CORPUS_DIR / "Drafts" / "review-queue.json",
        CORPUS_DIR / "Drafts" / "codex-review-queue.json",
    ):
        if candidate.exists():
            return Path(os.path.abspath(candidate))
    return Path(os.path.abspath(CORPUS_DIR / "Drafts" / "review-queue.json"))


def load_review_file(path: Path) -> tuple[dict[str, Any] | None, dict[str, Any], bool]:
    payload = load_json(path)
    if payload is None:
        return None, {}, True
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    reviews = payload.get("reviews")
    if reviews is None:
        reviews = {}
    if not isinstance(reviews, dict):
        raise ValueError(f"{path} reviews must be a JSON object")
    return payload, reviews, False


def review_artifact_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def canonical_review_artifact_id(payload: dict[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(review_artifact_bytes(payload)).hexdigest()


def build_review_artifact(
    reports: list[EpisodeReport],
    reviews: dict[str, Any],
    *,
    reviewer: str,
    reviewed_at: str,
    artifact_kind: str = "corpus_review_attestation",
) -> dict[str, Any]:
    if artifact_kind not in {
        "corpus_review_attestation",
        "human_first_pass_attestation",
    }:
        raise ValueError(f"unsupported review artifact kind: {artifact_kind}")
    artifact_reviews: dict[str, Any] = {}
    episodes: list[dict[str, Any]] = []
    for report in reports:
        if report.annotation is None or report.skipped:
            continue
        fingerprint = report.annotation["audio_fingerprint"]
        decision_ids: list[str] = []
        for entry in report.entries:
            entry_id = entry["id"]
            decision = reviews.get(entry_id)
            if not isinstance(decision, dict):
                raise ValueError(f"review artifact lacks decision {entry_id}")
            if clean_string(decision.get("reviewer")) != reviewer:
                raise ValueError(
                    f"reviewer mismatch: {entry_id} does not belong to {reviewer!r}"
                )
            if clean_string(decision.get("audio_fingerprint")) != fingerprint:
                raise ValueError(f"review artifact audio mismatch: {entry_id}")
            if clean_string(decision.get("reviewed_at")) is None:
                raise ValueError(f"review artifact timestamp missing: {entry_id}")
            if clean_string(decision.get("status")) not in FINAL_REVIEW_STATUSES:
                raise ValueError(f"review artifact decision is not final: {entry_id}")
            decision_copy = copy.deepcopy(decision)
            decision_copy["source_reviewed_at"] = decision_copy["reviewed_at"]
            decision_copy["reviewed_at"] = reviewed_at
            decision_episode = clean_string(decision_copy.get("episode_id"))
            if decision_episode is not None and decision_episode != report.episode_id:
                raise ValueError(f"review artifact episode mismatch: {entry_id}")
            decision_copy["episode_id"] = report.episode_id
            artifact_reviews[entry_id] = decision_copy
            decision_ids.append(entry_id)
        episodes.append({
            "episode_id": report.episode_id,
            "audio_fingerprint": fingerprint,
            "decision_ids": decision_ids,
            "annotation_decision": annotation_decision(report.annotation),
        })
    if not episodes:
        raise ValueError("review artifact has no promotable episode decisions")
    if artifact_kind == "human_first_pass_attestation":
        if not artifact_reviews:
            raise ValueError("first-pass review artifact has no source decisions")
        return {
            "schema_version": 1,
            "artifact_kind": artifact_kind,
            "reviewer": reviewer,
            "reviewed_at": reviewed_at,
            "source_decision_count": len(artifact_reviews),
            "audio_bindings": [
                {
                    "episode_id": episode["episode_id"],
                    "audio_fingerprint": episode["audio_fingerprint"],
                    "annotation_decision": episode["annotation_decision"],
                }
                for episode in episodes
            ],
        }
    return {
        "schema_version": 1,
        "artifact_kind": artifact_kind,
        "reviewer": reviewer,
        "reviewed_at": reviewed_at,
        "episodes": episodes,
        "reviews": artifact_reviews,
    }


def persist_review_artifact(reviews_dir: Path, artifact: dict[str, Any]) -> tuple[str, Path]:
    data = review_artifact_bytes(artifact)
    digest = hashlib.sha256(data).hexdigest()
    artifact_id = "sha256:" + digest
    path = reviews_dir / f"{digest}.json"
    if path_has_symlink_component(reviews_dir):
        raise ValueError(f"review artifacts directory must not be a symbolic link: {reviews_dir}")
    reviews_dir.mkdir(parents=True, exist_ok=True)
    if not reviews_dir.is_dir():
        raise ValueError(f"review artifacts path is not a directory: {reviews_dir}")

    def validate_existing() -> None:
        if path.is_symlink() or path.read_bytes() != data:
            raise ValueError(f"immutable review artifact collision: {path}")

    def sync_reviews_directory() -> None:
        directory = os.open(reviews_dir, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    if path.exists() or path.is_symlink():
        validate_existing()
        sync_reviews_directory()
        return artifact_id, path

    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "wb", dir=reviews_dir, prefix=f".{path.name}.", delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o644)
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            validate_existing()
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    sync_reviews_directory()
    return artifact_id, path


def load_queue(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path)
    if not isinstance(payload, dict) or not isinstance(payload.get("entries"), list):
        raise ValueError(f"{path} does not contain a playhead-l2f review queue")
    entries: list[dict[str, Any]] = []
    for index, raw in enumerate(payload["entries"], start=1):
        if not isinstance(raw, dict):
            raise ValueError(f"{path} entries[{index - 1}] must be a JSON object")
        entry = dict(raw)
        episode_id = str(entry.get("episode_id") or "").strip()
        if not episode_id:
            episode_id = str(entry.get("id") or f"episode-{index}").split("#", 1)[0]
        entry["episode_id"] = episode_id
        entry["id"] = str(entry.get("id") or f"{episode_id}#{index}")
        entries.append(entry)
    return entries


def load_manual_entries(review_payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not review_payload:
        return []
    raw_entries = review_payload.get("manual_entries")
    if raw_entries is None:
        return []
    if not isinstance(raw_entries, list):
        raise ValueError("review file manual_entries must be an array")
    entries: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_entries, start=1):
        if not isinstance(raw, dict):
            raise ValueError(f"review file manual_entries[{index - 1}] must be a JSON object")
        entry = dict(raw)
        episode_id = str(entry.get("episode_id") or "").strip()
        if not episode_id:
            raise ValueError(f"review file manual_entries[{index - 1}] missing episode_id")
        entry["episode_id"] = episode_id
        entry["manual_entry"] = True
        entry["false_positive_trap"] = False
        entry.setdefault("source", "manual_missed_ad")
        entry["id"] = str(entry.get("id") or f"manual:{episode_id}#{index}")
        entry.setdefault("candidate_index", f"M{index}")
        entries.append(entry)
    return entries


def clean_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    text = str(value).strip()
    return text or None


def number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def safe_artifact_basename(value: str) -> bool:
    return (
        bool(value)
        and "/" not in value
        and "\\" not in value
        and value not in {".", ".."}
    )


def is_false_positive_trap(entry: dict[str, Any]) -> bool:
    return entry.get("false_positive_trap") is True


def review_for(entry: dict[str, Any], reviews: dict[str, Any]) -> dict[str, Any]:
    review = reviews.get(entry["id"])
    return review if isinstance(review, dict) else {}


def value_from(review: dict[str, Any], entry: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in review and review[key] is not None:
            return review[key]
        if key in entry and entry[key] is not None:
            return entry[key]
    return None


def group_by_episode(entries: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        grouped.setdefault(entry["episode_id"], []).append(entry)
    return dict(sorted(grouped.items()))


def validate_unique_entry_ids(entries: list[dict[str, Any]]) -> None:
    """Require one globally unique review decision key per queue entry."""
    owners: dict[str, str] = {}
    for entry in entries:
        entry_id = entry["id"]
        episode_id = entry["episode_id"]
        if owner := owners.get(entry_id):
            raise ValueError(
                f"duplicate review entry id {entry_id!r} for episodes "
                f"{owner!r} and {episode_id!r}"
            )
        owners[entry_id] = episode_id


def validate_episode_invariants(
    report: EpisodeReport,
    entries: list[dict[str, Any]],
    reviews: dict[str, Any],
    queue_dir: Path,
) -> None:
    """Reject queue rows that cannot describe one coherent episode asset."""
    show_names: set[str] = set()
    variants: set[str] = set()
    audio_paths: set[Path] = set()
    durations: list[float] = []
    duration_keys = ("duration_seconds", "duration", "episode_duration_seconds")

    for entry in entries:
        entry_id = entry["id"]
        review = review_for(entry, reviews)

        review_episode_id = review.get("episode_id")
        if review_episode_id is not None:
            normalized_episode_id = clean_string(review_episode_id)
            if normalized_episode_id != report.episode_id:
                report.issues.append(
                    f"conflicting_episode_id: {entry_id} review identifies "
                    f"{review_episode_id!r}, expected {report.episode_id!r}"
                )

        for field, values in (("show_name", show_names), ("variant_of", variants)):
            raw = value_from(review, entry, field)
            if raw is None:
                continue
            normalized = clean_string(raw)
            if normalized is None:
                report.issues.append(
                    f"invalid_episode_metadata: {entry_id} {field} must be a non-empty string"
                )
            else:
                values.add(normalized)

        raw_audio = value_from(review, entry, "audio_path")
        if raw_audio is not None:
            normalized_audio = clean_string(raw_audio)
            if normalized_audio is None:
                report.issues.append(
                    f"invalid_audio_path: {entry_id} audio_path must be a non-empty string"
                )
            else:
                try:
                    audio_path = resolve_local_then_repo(normalized_audio, queue_dir)
                    require_safe_input_path(audio_path, label="reviewed audio input")
                except ValueError as error:
                    report.issues.append(f"invalid_audio_path: {entry_id}: {error}")
                    continue
                if not audio_path.is_file():
                    report.issues.append(
                        f"invalid_audio_path: {entry_id} does not reference a regular file: "
                        f"{audio_path}"
                    )
                else:
                    audio_paths.add(audio_path)

        # Any review-supplied duration metadata overrides the queue row as a
        # group. This mirrors duration_from_metadata's review-before-entry
        # precedence while still detecting disagreeing aliases or rows.
        duration_source = review if any(
            review.get(key) is not None for key in duration_keys
        ) else entry
        for key in duration_keys:
            raw_duration = duration_source.get(key)
            if raw_duration is None:
                continue
            normalized_duration = number(raw_duration)
            if normalized_duration is None or normalized_duration <= 0:
                report.issues.append(
                    f"invalid_episode_metadata: {entry_id} {key} must be a finite positive number"
                )
            else:
                durations.append(normalized_duration)

    if len(show_names) > 1:
        report.issues.append(
            "conflicting_show_name: episode entries disagree: "
            + ", ".join(repr(value) for value in sorted(show_names))
        )
    if len(variants) > 1:
        report.issues.append(
            "conflicting_variant_of: episode entries disagree: "
            + ", ".join(repr(value) for value in sorted(variants))
        )
    if len(audio_paths) > 1:
        report.issues.append(
            "conflicting_audio_path: episode entries reference different files: "
            + ", ".join(str(value) for value in sorted(audio_paths))
        )
    if durations and max(durations) - min(durations) > BOUNDARY_TOLERANCE_SECONDS:
        report.issues.append(
            "conflicting_duration: episode entries disagree: "
            + ", ".join(f"{value:g}" for value in sorted(set(durations)))
        )


def find_audio(
    episode_id: str,
    entries: list[dict[str, Any]],
    reviews: dict[str, Any],
    audio_dir: Path,
    queue_dir: Path,
) -> Path | None:
    for entry in entries:
        raw = clean_string(value_from(review_for(entry, reviews), entry, "audio_path"))
        if not raw:
            continue
        candidate = resolve_local_then_repo(raw, queue_dir)
        require_safe_input_path(candidate, label="reviewed audio input")
        if candidate.is_file():
            return candidate
    require_safe_input_path(audio_dir, label="reviewed audio directory")
    if audio_dir.is_dir():
        matches: list[Path] = []
        for child in sorted(audio_dir.iterdir()):
            if child.stem != episode_id or child.suffix.lower() not in AUDIO_EXTENSIONS:
                continue
            require_safe_input_path(child, label="reviewed audio input")
            if child.is_file():
                matches.append(child)
        if len(matches) > 1:
            raise ValueError(
                f"ambiguous_audio: multiple media files match {episode_id}: "
                + ", ".join(str(path) for path in matches)
            )
        if matches:
            return matches[0]
    return None


def open_regular_audio(path: Path) -> tuple[int, os.stat_result]:
    """Open one regular input without following aliases or blocking on a FIFO."""
    require_safe_input_path(path, label="reviewed audio input")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"reviewed audio is not a regular file: {path}")
        return descriptor, metadata
    except BaseException:
        os.close(descriptor)
        raise


def stable_audio_identity(metadata: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def fingerprint_descriptor(descriptor: int, path: Path) -> str:
    before = os.fstat(descriptor)
    digest = hashlib.sha256()
    os.lseek(descriptor, 0, os.SEEK_SET)
    for chunk in iter(lambda: os.read(descriptor, 1024 * 1024), b""):
        digest.update(chunk)
    after = os.fstat(descriptor)
    if stable_audio_identity(before) != stable_audio_identity(after):
        raise ValueError(f"reviewed audio changed while hashing: {path}")
    os.lseek(descriptor, 0, os.SEEK_SET)
    return "sha256:" + digest.hexdigest()


def sha256_fingerprint(path: Path) -> str:
    descriptor, _ = open_regular_audio(path)
    try:
        return fingerprint_descriptor(descriptor, path)
    finally:
        os.close(descriptor)


def duration_from_wav_descriptor(descriptor: int) -> float | None:
    try:
        duplicate = os.dup(descriptor)
        with os.fdopen(duplicate, "rb") as audio:
            audio.seek(0)
            with wave.open(audio, "rb") as handle:
                rate = handle.getframerate()
                if rate <= 0:
                    return None
                return handle.getnframes() / float(rate)
    except (wave.Error, OSError, EOFError):
        return None


def run_duration_probe(
    command: list[str],
    pattern: re.Pattern[str] | None = None,
    *,
    pass_fds: tuple[int, ...] = (),
) -> float | None:
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=pass_fds,
            timeout=AUDIO_PROBE_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    output = f"{completed.stdout}\n{completed.stderr}"
    if pattern:
        match = pattern.search(output)
        if not match:
            return None
        return number(match.group(1))
    for line in output.splitlines():
        candidate = number(line.strip())
        if candidate and candidate > 0:
            return candidate
    return None


def duration_from_audio(path: Path, descriptor: int | None = None) -> float | None:
    owned_descriptor = descriptor is None
    if descriptor is None:
        descriptor, _ = open_regular_audio(path)
    try:
        if path.suffix.lower() == ".wav":
            wav_duration = duration_from_wav_descriptor(descriptor)
            if wav_duration and wav_duration > 0:
                return wav_duration

        descriptor_path = f"/dev/fd/{descriptor}"
        inherited_descriptors = (descriptor,)
        ffprobe_candidates = [Path("/opt/homebrew/bin/ffprobe")]
        found_ffprobe = shutil.which("ffprobe")
        if found_ffprobe:
            ffprobe_candidates.append(Path(found_ffprobe))
        for ffprobe in ffprobe_candidates:
            if ffprobe.exists():
                duration = run_duration_probe(
                    [
                        str(ffprobe),
                        "-v",
                        "error",
                        "-show_entries",
                        "format=duration",
                        "-of",
                        "default=noprint_wrappers=1:nokey=1",
                        descriptor_path,
                    ],
                    pass_fds=inherited_descriptors,
                )
                if duration and duration > 0:
                    return duration

        afinfo_candidates = [Path("/usr/bin/afinfo")]
        found_afinfo = shutil.which("afinfo")
        if found_afinfo:
            afinfo_candidates.append(Path(found_afinfo))
        afinfo_pattern = re.compile(
            r"(?:estimated duration|duration):\s*([0-9]+(?:\.[0-9]+)?)\s*sec",
            re.I,
        )
        for afinfo in afinfo_candidates:
            if afinfo.exists():
                duration = run_duration_probe(
                    [str(afinfo), descriptor_path],
                    afinfo_pattern,
                    pass_fds=inherited_descriptors,
                )
                if duration and duration > 0:
                    return duration
        return None
    finally:
        if owned_descriptor:
            os.close(descriptor)


def fingerprint_and_duration(path: Path) -> tuple[str, float | None]:
    descriptor, _ = open_regular_audio(path)
    try:
        fingerprint = fingerprint_descriptor(descriptor, path)
        duration = duration_from_audio(path, descriptor=descriptor)
        if fingerprint_descriptor(descriptor, path) != fingerprint:
            raise ValueError(f"reviewed audio changed while determining duration: {path}")
        return fingerprint, duration
    finally:
        os.close(descriptor)


def duration_from_metadata(entries: list[dict[str, Any]], reviews: dict[str, Any]) -> float | None:
    for entry in entries:
        review = review_for(entry, reviews)
        for source in (review, entry):
            for key in ("duration_seconds", "duration", "episode_duration_seconds"):
                candidate = number(source.get(key))
                if candidate and candidate > 0:
                    return candidate
    return None


def show_name_for(episode_id: str, entries: list[dict[str, Any]], reviews: dict[str, Any]) -> tuple[str, bool]:
    for entry in entries:
        review = review_for(entry, reviews)
        for source in (review, entry):
            show_name = clean_string(source.get("show_name"))
            if show_name:
                return show_name, False

    draft = CORPUS_DIR / "Drafts" / f"{episode_id}.draft.json"
    payload = load_json(draft)
    if isinstance(payload, dict):
        show_name = clean_string(payload.get("show_name"))
        if show_name:
            return show_name, False
    return episode_id, True


def variant_of_for(entries: list[dict[str, Any]], reviews: dict[str, Any]) -> str | None:
    for entry in entries:
        review = review_for(entry, reviews)
        for source in (review, entry):
            variant_of = clean_string(source.get("variant_of"))
            if variant_of:
                return variant_of
    return None


def confidence_notes(review: dict[str, Any], entry: dict[str, Any]) -> str | None:
    notes = clean_string(review.get("confidence_notes"))
    if notes:
        return notes
    review_notes = clean_string(review.get("notes"))
    boundary = clean_string(review.get("boundary_confidence"))
    if review_notes and boundary:
        return f"{review_notes} Boundary confidence: {boundary}."
    if review_notes:
        return review_notes
    return clean_string(entry.get("notes"))


def build_content_windows(duration: float, ad_windows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    content: list[dict[str, Any]] = []
    cursor = 0.0
    sorted_ads = sorted(ad_windows, key=lambda item: item["start_seconds"])
    for index, ad in enumerate(sorted_ads):
        start = float(ad["start_seconds"])
        if start > cursor:
            content.append(
                {
                    "start_seconds": cursor,
                    "end_seconds": start,
                    "notes": content_note(index, len(sorted_ads)),
                }
            )
        cursor = float(ad["end_seconds"])
    if cursor < duration:
        content.append(
            {
                "start_seconds": cursor,
                "end_seconds": duration,
                "notes": "Post-ad content - must NEVER be skipped"
                if sorted_ads
                else "Reviewed no-ad content - must NEVER be skipped",
            }
        )
    return content


def clamp_ad_windows_to_duration(
    report: EpisodeReport,
    duration: float,
    ad_windows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    clamped: list[dict[str, Any]] = []
    for index, ad in enumerate(ad_windows, start=1):
        normalized = dict(ad)
        end = float(normalized["end_seconds"])
        if duration < end <= duration + BOUNDARY_TOLERANCE_SECONDS:
            normalized["end_seconds"] = duration
            report.warnings.append(
                f"clamped_timing: ad {index} end {end} is within "
                f"{BOUNDARY_TOLERANCE_SECONDS:.2f}s of duration {duration}"
            )
        clamped.append(normalized)
    return clamped


def content_note(index: int, ad_count: int) -> str:
    if index == 0:
        return "Pre-ad content - must NEVER be skipped"
    if index >= ad_count:
        return "Post-ad content - must NEVER be skipped"
    return "Inter-ad content - must NEVER be skipped"


def validate_ad_timing(
    report: EpisodeReport,
    ad_windows: list[dict[str, Any]],
    duration: float | None,
) -> None:
    if duration is None:
        return
    sorted_ads = sorted(ad_windows, key=lambda item: item["start_seconds"])
    for index, ad in enumerate(sorted_ads):
        start = float(ad["start_seconds"])
        end = float(ad["end_seconds"])
        if start < 0:
            report.issues.append(f"invalid_timing: ad {index + 1} start {start} is negative")
        if end <= start:
            report.issues.append(f"invalid_timing: ad {index + 1} end {end} <= start {start}")
        if end > duration + BOUNDARY_TOLERANCE_SECONDS:
            report.issues.append(
                f"invalid_timing: ad {index + 1} end {end} exceeds duration {duration}"
            )
        if index > 0:
            previous_end = float(sorted_ads[index - 1]["end_seconds"])
            if start < previous_end:
                report.issues.append(
                    f"overlap: ad {index} ending {previous_end} overlaps ad {index + 1} starting {start}"
                )


def analyze_episode(
    episode_id: str,
    entries: list[dict[str, Any]],
    reviews: dict[str, Any],
    audio_dir: Path,
    queue_dir: Path,
    *,
    second_pass_reviewed: bool = False,
    review_attestation: dict[str, str] | None = None,
) -> EpisodeReport:
    report = EpisodeReport(episode_id=episode_id, entries=entries)

    if not safe_artifact_basename(episode_id):
        report.issues.append("unsafe_episode_id: episode_id cannot contain path separators")

    if not report.issues and all(
        clean_string(review_for(entry, reviews).get("status")) == "false_positive"
        and not is_false_positive_trap(entry)
        for entry in entries
    ):
        report.skipped = True
        report.skip_reason = (
            "false_positive_only: rejected ordinary candidates do not create "
            "whole-episode no-ad annotations"
        )
        return report

    validate_episode_invariants(report, entries, reviews, queue_dir)
    try:
        audio_path = find_audio(episode_id, entries, reviews, audio_dir, queue_dir)
    except ValueError as error:
        report.issues.append(str(error))
        audio_path = None
    report.audio_path = audio_path
    if audio_path is None:
        report.issues.append("missing_audio: no local audio file found")
    resolved_fingerprint: str | None = None
    duration: float | None = None
    if audio_path is not None:
        try:
            resolved_fingerprint, duration = fingerprint_and_duration(audio_path)
        except (OSError, ValueError) as error:
            report.issues.append(f"invalid_audio: {error}")
    if resolved_fingerprint is not None:
        for entry in entries:
            entry_id = entry["id"]
            entry_fingerprint = clean_string(entry.get("audio_fingerprint"))
            review_fingerprint = clean_string(
                review_for(entry, reviews).get("audio_fingerprint")
            )
            if entry_fingerprint != resolved_fingerprint:
                report.issues.append(
                    f"audio_fingerprint_mismatch: {entry_id} queue evidence is not bound "
                    "to the resolved audio"
                )
            if review_fingerprint != resolved_fingerprint:
                report.issues.append(
                    f"audio_fingerprint_mismatch: {entry_id} review decision is not bound "
                    "to the resolved audio"
                )

    if duration is None:
        duration = duration_from_metadata(entries, reviews)
    report.duration_seconds = duration
    if duration is None:
        report.issues.append("missing_duration: unable to determine episode duration")

    show_name, inferred_show_name = show_name_for(episode_id, entries, reviews)
    if inferred_show_name:
        report.issues.append("missing_show_name: no show_name found in review, queue, or draft")
    variant_of = variant_of_for(entries, reviews)
    if variant_of == episode_id:
        report.issues.append("invalid_variant: variant_of cannot equal episode_id")

    ad_windows: list[dict[str, Any]] = []
    reviewed_no_ad_trap = False
    for entry in entries:
        review = review_for(entry, reviews)
        status = clean_string(review.get("status")) or "unreviewed"
        entry_id = entry["id"]
        if status == "unreviewed":
            report.issues.append(f"unreviewed: {entry_id}")
            continue
        if status == "unsure":
            report.issues.append(f"unsure: {entry_id}")
            continue
        if status not in FINAL_REVIEW_STATUSES:
            report.issues.append(f"invalid_status: {entry_id} has status {status}")
            continue
        is_trap = is_false_positive_trap(entry)
        if status == "zero_ad_confirmed" and not is_trap:
            report.issues.append(f"invalid_zero_ad_confirmation: {entry_id} is not a trap entry")
            continue
        if status in {"false_positive", "zero_ad_confirmed"}:
            if is_trap:
                reviewed_no_ad_trap = True
            continue

        start = number(value_from(review, entry, "start_seconds", "start"))
        end = number(value_from(review, entry, "end_seconds", "end"))
        advertiser = clean_string(value_from(review, entry, "advertiser", "advertiser_guess"))
        product = clean_string(value_from(review, entry, "product", "product_guess"))
        ad_type = clean_string(value_from(review, entry, "ad_type"))
        transition_type = clean_string(value_from(review, entry, "transition_type"))
        notes = confidence_notes(review, entry)

        missing = []
        for name, value in (
            ("start_seconds", start),
            ("end_seconds", end),
            ("advertiser", advertiser),
            ("product", product),
            ("ad_type", ad_type),
            ("transition_type", transition_type),
            ("confidence_notes", notes),
        ):
            if value is None:
                missing.append(name)
        if missing:
            report.issues.append(f"missing_ad_metadata: {entry_id} missing {', '.join(missing)}")
            continue
        if ad_type not in VALID_AD_TYPES:
            report.issues.append(f"invalid_ad_type: {entry_id} has {ad_type}")
            continue
        if transition_type not in VALID_TRANSITION_TYPES:
            report.issues.append(f"invalid_transition_type: {entry_id} has {transition_type}")
            continue

        ad_windows.append(
            {
                "start_seconds": start,
                "end_seconds": end,
                "advertiser": advertiser,
                "product": product,
                "ad_type": ad_type,
                "transition_type": transition_type,
                "confidence_notes": notes,
                "provenance": [
                    SECOND_PASS_PROVENANCE
                    if second_pass_reviewed
                    else FIRST_PASS_PROVENANCE
                ],
            }
        )

    validate_ad_timing(report, ad_windows, duration)
    if not report.issues and not ad_windows and not reviewed_no_ad_trap:
        report.issues.append(
            "invalid_no_ad_annotation: no-ad promotion requires a reviewed false_positive_trap entry"
        )

    if report.issues or duration is None or audio_path is None:
        return report

    sorted_ads = clamp_ad_windows_to_duration(
        report,
        duration,
        sorted(ad_windows, key=lambda item: item["start_seconds"]),
    )
    report.annotation = {
        "episode_id": episode_id,
        "show_name": show_name,
        "duration_seconds": duration,
        "ad_windows": sorted_ads,
        "content_windows": build_content_windows(duration, sorted_ads),
        "variant_of": variant_of,
        "audio_fingerprint": resolved_fingerprint,
        "provenance": [
            SECOND_PASS_PROVENANCE
            if second_pass_reviewed
            else FIRST_PASS_PROVENANCE
        ],
    }
    if review_attestation is not None:
        report.annotation["review_attestations"] = [{
            **review_attestation,
            "audio_fingerprint": report.annotation["audio_fingerprint"],
        }]
    return report


def is_first_pass_annotation(document: object) -> bool:
    """Return true only for an annotation awaiting the second listener gate."""
    if not isinstance(document, dict) or document.get("provenance") != [FIRST_PASS_PROVENANCE]:
        return False
    windows = document.get("ad_windows")
    return isinstance(windows, list) and all(
        isinstance(window, dict)
        and window.get("provenance") == [FIRST_PASS_PROVENANCE]
        for window in windows
    )


def is_second_pass_annotation(document: object) -> bool:
    if not isinstance(document, dict) or document.get("provenance") != [SECOND_PASS_PROVENANCE]:
        return False
    windows = document.get("ad_windows")
    return isinstance(windows, list) and all(
        isinstance(window, dict)
        and window.get("provenance") == [SECOND_PASS_PROVENANCE]
        for window in windows
    )


def without_review_provenance(document: dict[str, Any]) -> dict[str, Any]:
    normalized = copy.deepcopy(document)
    normalized.pop("provenance", None)
    normalized.pop("review_attestations", None)
    for window in normalized.get("ad_windows", []):
        if isinstance(window, dict):
            window.pop("provenance", None)
    return normalized


def reviewed_promotion_precommit(
    rejects_path: Path,
    *,
    second_pass_reviewed: bool,
    audio_paths_by_filename: Mapping[str, Path],
    artifact_index: dict[str, dict] | None = None,
    reviews_dir: Path | None = None,
):
    """Recheck assets, vetoes, and the two-listener transition under lock."""
    reject_policy = reject_veto_precommit(rejects_path)

    def enforce(existing: dict[str, dict[str, Any]], proposed: dict[str, dict[str, Any]]) -> None:
        reject_policy(existing, proposed)
        current_artifact_index = artifact_index
        if reviews_dir is not None:
            try:
                current_artifact_index = load_review_artifacts(reviews_dir)
                for filename, candidate in proposed.items():
                    validate_annotation(
                        candidate,
                        source=filename,
                        artifact_index=current_artifact_index,
                    )
            except ConversionError as error:
                raise CanonicalCorpusError(
                    f"review evidence changed before publication: {error}"
                ) from error
        for filename, candidate in proposed.items():
            audio_path = audio_paths_by_filename.get(filename)
            if audio_path is None:
                raise CanonicalCorpusError(
                    f"reviewed promotion has no retained audio path: {filename}"
                )
            try:
                current_fingerprint = sha256_fingerprint(audio_path)
            except (OSError, ValueError) as error:
                raise CanonicalCorpusError(
                    f"cannot recheck retained audio before reviewed promotion: {filename}: {error}"
                ) from error
            if current_fingerprint != candidate["audio_fingerprint"]:
                raise CanonicalCorpusError(
                    "retained audio changed before reviewed promotion publication: "
                    f"{filename}"
                )
            current = existing.get(filename)
            if second_pass_reviewed:
                if current is None or not is_first_pass_annotation(current):
                    raise CanonicalCorpusError(
                        f"second-pass review requires an existing first-pass annotation: {filename}"
                    )
                current_attestations = current.get("review_attestations", [])
                candidate_attestations = candidate.get("review_attestations", [])
                preserves_current_attestations = (
                    isinstance(current_attestations, list)
                    and isinstance(candidate_attestations, list)
                    and candidate_attestations[: len(current_attestations)]
                    == current_attestations
                )
                if (
                    not is_second_pass_annotation(candidate)
                    or not preserves_current_attestations
                    or not has_distinct_review_attestations(
                        candidate,
                        current_artifact_index,
                        require_artifacts=current_artifact_index is not None,
                    )
                    or without_review_provenance(current)
                    != without_review_provenance(candidate)
                ):
                    raise CanonicalCorpusError(
                        f"second-pass review may only advance provenance: {filename}"
                    )
            elif current is not None and is_gold_annotation(
                current,
                current_artifact_index,
                require_review_artifacts=current_artifact_index is not None,
            ):
                raise CanonicalCorpusError(
                    f"refusing first-pass overwrite of human-gold annotation: {filename}"
                )
            elif not is_first_pass_annotation(proposed[filename]):
                raise CanonicalCorpusError(
                    f"first-pass promotion must retain pending provenance: {filename}"
                )

    return enforce


def review_status_counts(entries: list[dict[str, Any]], reviews: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entry in entries:
        status = clean_string(review_for(entry, reviews).get("status")) or "unreviewed"
        counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items()))


def category_for(entry: dict[str, Any], reviews: dict[str, Any]) -> str:
    review = review_for(entry, reviews)
    for source in (review, entry):
        category = clean_string(source.get("category")) or clean_string(source.get("corpus_category"))
        if category:
            return category
    status = clean_string(review.get("status")) or "unreviewed"
    if is_false_positive_trap(entry):
        return "edge_zero_or_false_positive_trap"
    if status == "false_positive":
        return "false_positive_rejected"
    return clean_string(value_from(review, entry, "ad_type")) or "unknown"


def category_coverage(entries: list[dict[str, Any]], reviews: dict[str, Any]) -> dict[str, dict[str, int]]:
    coverage: dict[str, dict[str, int]] = {}
    for entry in entries:
        category = category_for(entry, reviews)
        status = clean_string(review_for(entry, reviews).get("status")) or "unreviewed"
        bucket = coverage.setdefault(category, {"total": 0, "reviewed": 0, "verified_ads": 0})
        bucket["total"] += 1
        if status != "unreviewed":
            bucket["reviewed"] += 1
        if status == "verified_ad":
            bucket["verified_ads"] += 1
    return dict(sorted(coverage.items()))


def print_report(
    review_path: Path,
    review_missing: bool,
    queue_path: Path,
    entries: list[dict[str, Any]],
    reports: list[EpisodeReport],
    reviews: dict[str, Any],
) -> None:
    print("L2F reviewed corpus promotion report")
    print(f"review_file: {review_path}")
    if review_missing:
        print("review_file_status: missing (treating all queue entries as unreviewed)")
    print(f"queue_path: {queue_path}")
    print(f"entries: {len(entries)}")
    print(f"episodes: {len(reports)}")

    print("\nreview_debt:")
    for status, count in review_status_counts(entries, reviews).items():
        print(f"  {status}: {count}")

    print("\ncategory_coverage:")
    for category, counts in category_coverage(entries, reviews).items():
        print(
            f"  {category}: total={counts['total']} "
            f"reviewed={counts['reviewed']} verified_ads={counts['verified_ads']}"
        )

    print("\nepisode_readiness:")
    for report in reports:
        if report.ready:
            state = "READY"
        elif report.skipped:
            state = "SKIPPED"
        else:
            state = "BLOCKED"
        ad_count = len(report.annotation["ad_windows"]) if report.annotation else 0
        duration = f"{report.duration_seconds:.3f}" if report.duration_seconds else "unknown"
        print(
            f"  {state} {report.episode_id}: entries={len(report.entries)} "
            f"ads={ad_count} duration={duration}"
        )
        if report.skip_reason:
            print(f"    skipped: {report.skip_reason}")
        for warning in report.warnings:
            print(f"    warning: {warning}")
        for issue in report.issues:
            print(f"    issue: {issue}")


def write_annotation(path: Path, annotation: dict[str, Any], force: bool) -> None:
    if path.exists() and not force:
        raise FileExistsError(f"{path} already exists; pass --force to overwrite")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(annotation, handle, indent=2)
        handle.write("\n")
    tmp.replace(path)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.second_pass_reviewed and (not args.promote or not args.force):
        print(
            "error: --second-pass-reviewed requires --promote --force",
            file=sys.stderr,
        )
        return 2
    if bool(args.reviewer_id) != bool(args.reviewed_at):
        print("error: --reviewer-id and --reviewed-at must be supplied together", file=sys.stderr)
        return 2
    if args.second_pass_reviewed and not args.reviewer_id:
        print("error: second-pass review requires --reviewer-id and --reviewed-at", file=sys.stderr)
        return 2
    try:
        review_path = resolve_path(args.review_file)
        audio_dir = resolve_path(args.audio_dir)
        annotations_dir = lexical_output_path(args.annotations_dir)
        reviews_dir = (
            lexical_output_path(args.reviews_dir)
            if args.reviews_dir
            else annotations_dir.parent / "Reviews"
        )
        # Preserve the lexical path so the veto reader can detect parent aliases.
        # Resolving first would erase the evidence and silently trust an external
        # ledger reached through a symbolic link.
        rejects_path = lexical_output_path(args.rejects_file)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    try:
        review_payload, reviews, review_missing = load_review_file(review_path)
        if args.reviewer_id and review_payload is None:
            raise ValueError("cannot attest a missing review artifact")
        reviewer_id = args.reviewer_id.strip() if args.reviewer_id else None
        reviewed_at = args.reviewed_at.strip() if args.reviewed_at else None
        if args.reviewer_id and (not reviewer_id or not reviewed_at):
            raise ValueError("reviewer identity and timestamp must be non-empty")
        queue_path = choose_queue_path(args, review_path, review_payload)
        entries = load_queue(queue_path)
        entries.extend(load_manual_entries(review_payload))
        validate_unique_entry_ids(entries)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    selected = set(args.episode)
    if selected:
        available = {entry["episode_id"] for entry in entries}
        missing = sorted(selected - available)
        if missing:
            print(
                "error: episode_not_found: "
                + ", ".join(missing)
                + f" not present in {queue_path}",
                file=sys.stderr,
            )
            return 2
        entries = [entry for entry in entries if entry["episode_id"] in selected]
    if not entries:
        print(f"error: no review queue entries found in {queue_path}", file=sys.stderr)
        return 2
    grouped = group_by_episode(entries)
    reports = [
        analyze_episode(
            episode_id,
            episode_entries,
            reviews,
            audio_dir,
            queue_path.parent,
            second_pass_reviewed=args.second_pass_reviewed,
            review_attestation=None,
        )
        for episode_id, episode_entries in grouped.items()
    ]

    review_artifact: dict[str, Any] | None = None
    review_artifact_id: str | None = None
    if reviewer_id and reviewed_at:
        try:
            review_artifact = build_review_artifact(
                reports,
                reviews,
                reviewer=reviewer_id,
                reviewed_at=reviewed_at,
                artifact_kind=(
                    "corpus_review_attestation"
                    if args.second_pass_reviewed
                    else "human_first_pass_attestation"
                ),
            )
            review_artifact_id = canonical_review_artifact_id(review_artifact)
            for report in reports:
                if report.annotation is not None and not report.skipped:
                    report.annotation["review_attestations"] = [{
                        "reviewer": reviewer_id,
                        "reviewed_at": reviewed_at,
                        "audio_fingerprint": report.annotation["audio_fingerprint"],
                        "review_artifact_id": review_artifact_id,
                    }]
        except Exception as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2

    for report in reports:
        out = annotations_dir / f"{report.episode_id}.json"
        if report.annotation is not None and out.exists() and not args.force:
            report.issues.append("annotation_exists: pass --force to overwrite existing annotation")
            report.annotation = None

    print_report(review_path, review_missing, queue_path, entries, reports, reviews)
    sys.stdout.flush()

    blocked = [report for report in reports if report.issues]
    if args.promote:
        if blocked:
            print(
                f"\nrefusing promotion: {len(blocked)} episode(s) are not promotion-ready",
                file=sys.stderr,
            )
            return 2
        try:
            canonical_snapshot = load_canonical_annotations(
                annotations_dir, reviews_dir=reviews_dir
            )
        except Exception as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        planned = {
            f"{report.episode_id}.json": report.annotation
            for report in reports
            if report.annotation is not None
        }
        audio_paths_by_filename = {
            f"{report.episode_id}.json": report.audio_path
            for report in reports
            if report.annotation is not None and report.audio_path is not None
        }
        if args.second_pass_reviewed:
            for filename, candidate in planned.items():
                try:
                    current = copy.deepcopy(canonical_snapshot[filename])
                except KeyError:
                    print(
                        f"error: first-pass annotation is not canonical: {filename}",
                        file=sys.stderr,
                    )
                    return 2
                existing_attestations = current.get("review_attestations", [])
                if not isinstance(existing_attestations, list):
                    print(f"error: invalid first-pass review attestations: {filename}", file=sys.stderr)
                    return 2
                candidate["review_attestations"] = (
                    copy.deepcopy(existing_attestations)
                    + candidate.get("review_attestations", [])
                )
        try:
            if review_artifact is not None:
                persisted_id, _ = persist_review_artifact(reviews_dir, review_artifact)
                if persisted_id != review_artifact_id:
                    raise ValueError("persisted review artifact identity changed")
            artifact_index = load_review_artifacts(reviews_dir)
            for filename, candidate in planned.items():
                validate_annotation(
                    candidate,
                    source=filename,
                    artifact_index=artifact_index,
                )
        except Exception as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        try:
            written = commit_canonical_annotations(
                annotations_dir,
                planned,
                force=args.force,
                reviews_dir=reviews_dir,
                precommit=reviewed_promotion_precommit(
                    rejects_path,
                    second_pass_reviewed=args.second_pass_reviewed,
                    audio_paths_by_filename=audio_paths_by_filename,
                    artifact_index=artifact_index,
                    reviews_dir=reviews_dir,
                ),
            )
        except Exception as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        print("\npromoted_annotations:")
        for path in written:
            print(f"  {path}")
    else:
        ready = sum(1 for report in reports if report.ready)
        skipped = sum(1 for report in reports if report.skipped)
        print(
            f"\ndry_run: no annotations written; ready_episodes={ready} "
            f"skipped_episodes={skipped} blocked_episodes={len(blocked)}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
