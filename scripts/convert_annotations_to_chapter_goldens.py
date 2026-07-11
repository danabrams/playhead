#!/usr/bin/env python3
"""Convert all-gold corpus annotations into ChapterPlan golden-set fixtures.

Reads the canonical annotation manifest and emits one
`PlayheadTests/Fixtures/ChapterPlanGoldenSet/dogfood/<episode_id>.json` in
the GoldenChapterSet schema consumed by `ChapterPlanGoldenSetLoader`.

Silver and boundary-proposal annotations are deliberately excluded: generated
labels must not become activation gold by passing through this converter.

Anonymization (per au2v.1.22 privacy rule — no advertiser/product names
verbatim in committed fixtures): topic labels are derived from `ad_type`
only; advertiser, product, and confidence notes are dropped.

Usage:
    python3 Scripts/convert_annotations_to_chapter_goldens.py [--dry-run]
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import re
import shutil
import sys
import tempfile
import unicodedata
from collections.abc import Callable
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ANNOTATIONS_DIR = REPO_ROOT / "TestFixtures" / "Corpus" / "Annotations"
REVIEWS_DIR = REPO_ROOT / "TestFixtures" / "Corpus" / "Reviews"
MANIFEST_FILENAME = "_canonical-manifest.json"
DOGFOOD_DIR = (
    REPO_ROOT
    / "PlayheadTests"
    / "Fixtures"
    / "ChapterPlanGoldenSet"
    / "dogfood"
)

AD_TYPE_LABEL = {
    "host_read": "host-read sponsor",
    "blended_host_read": "blended host-read",
    "dynamic": "dynamic insertion",
    "pre_recorded": "pre-recorded spot",
}
AD_FALLBACK_LABEL = "advertisement"
CONTENT_LABEL = "editorial content"

NOTES = (
    "Auto-converted from TestFixtures/Corpus/Annotations/ by "
    "Scripts/convert_annotations_to_chapter_goldens.py. Topic labels are "
    "anonymized to ad_type / 'editorial content' — advertiser, product, "
    "and confidence_notes are stripped (au2v.1.22 privacy rule)."
)

AUTOMATIC_PROVENANCE = frozenset({"rediff", "drafter", "pipeline"})
HUMAN_PROVENANCE = frozenset({"human_reviewed"})
GOLD_REVIEW_ARTIFACT_KINDS = (
    "corpus_review_attestation",
    "human_first_pass_attestation",
)
AD_TYPES = frozenset(
    {
        "host_read",
        "dynamic_insertion",
        "blended_host_read",
        "produced_segment",
        "promo",
        "dai",
    }
)
TRANSITION_TYPES = frozenset({"explicit", "musical", "hard_cut", "blended"})
SHA256_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
PARTITION_EPSILON_SECONDS = 0.05
JSON_INTEGER_MIN = -(2**63)
JSON_INTEGER_MAX = 2**63 - 1


class ConversionError(ValueError):
    """The canonical annotation inputs cannot be converted safely."""


def path_has_symlink_component(path: Path) -> bool:
    """Reject aliases except root-owned top-level macOS compatibility links."""
    absolute = Path(os.path.abspath(path))
    root = Path(absolute.anchor)
    current = root
    for component in absolute.parts[1:]:
        current /= component
        if not current.is_symlink():
            continue
        try:
            metadata = os.lstat(current)
        except OSError:
            return True
        if current.parent != root or metadata.st_uid != 0:
            return True
    return False


def _finite_number(value: object, *, field: str, source: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConversionError(f"{source}: {field} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise ConversionError(f"{source}: {field} must be a finite number")
    return number


def _validate_optional_string(value: object, *, field: str, source: str) -> None:
    if value is not None and not isinstance(value, str):
        raise ConversionError(f"{source}: {field} must be a string or null")


def _validate_tier_metadata(document: dict, *, source: str) -> None:
    if document.get("auto_promoted") is not None and not isinstance(
        document["auto_promoted"], bool
    ):
        raise ConversionError(f"{source}: auto_promoted must be a boolean")
    for field in ("auto_promoted_at", "auto_promoted_by"):
        _validate_optional_string(document.get(field), field=field, source=source)
    provenance = document.get("provenance")
    if provenance is not None and (
        not isinstance(provenance, list)
        or not all(isinstance(value, str) for value in provenance)
    ):
        raise ConversionError(f"{source}: provenance must be an array of strings")
    audit_priority = document.get("audit_priority")
    if audit_priority is not None and (
        isinstance(audit_priority, bool)
        or not isinstance(audit_priority, int)
        or not JSON_INTEGER_MIN <= audit_priority <= JSON_INTEGER_MAX
    ):
        raise ConversionError(f"{source}: audit_priority must be a signed 64-bit integer")


def load_review_artifacts(reviews_dir: Path = REVIEWS_DIR) -> dict[str, dict]:
    """Load immutable artifacts whose filename and exact bytes share one SHA."""
    if path_has_symlink_component(reviews_dir):
        raise ConversionError(
            f"review artifacts directory must not be a symbolic link: {reviews_dir}"
        )
    if not reviews_dir.exists():
        return {}
    if not reviews_dir.is_dir():
        raise ConversionError(f"review artifacts path is not a directory: {reviews_dir}")
    indexed: dict[str, dict] = {}
    for path in sorted(reviews_dir.glob("*.json")):
        if path.is_symlink() or re.fullmatch(r"[0-9a-f]{64}\.json", path.name) is None:
            raise ConversionError(f"review artifact is not content-addressed: {path}")
        try:
            data = path.read_bytes()
            document = json.loads(data)
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise ConversionError(f"cannot read review artifact {path}: {error}") from error
        artifact_id = "sha256:" + hashlib.sha256(data).hexdigest()
        if path.stem != artifact_id.removeprefix("sha256:") or not isinstance(document, dict):
            raise ConversionError(f"review artifact hash mismatch: {path}")
        indexed[artifact_id] = document
    return indexed


def annotation_decision(annotation: dict) -> dict:
    """Return the exact human label semantics a review artifact must attest."""
    return {
        "episode_id": annotation.get("episode_id"),
        "audio_fingerprint": annotation.get("audio_fingerprint"),
        "show_name": annotation.get("show_name"),
        "duration_seconds": annotation.get("duration_seconds"),
        "variant_of": annotation.get("variant_of"),
        "ad_windows": [
            {
                "start_seconds": window.get("start_seconds"),
                "end_seconds": window.get("end_seconds"),
                "advertiser": window.get("advertiser"),
                "product": window.get("product"),
                "ad_type": window.get("ad_type"),
                "transition_type": window.get("transition_type"),
                "confidence_notes": window.get("confidence_notes"),
            }
            for window in annotation.get("ad_windows", [])
            if isinstance(window, dict)
        ],
        "content_windows": [
            {
                "start_seconds": window.get("start_seconds"),
                "end_seconds": window.get("end_seconds"),
                "notes": window.get("notes"),
            }
            for window in annotation.get("content_windows", [])
            if isinstance(window, dict)
        ],
    }


def _json_values_equal(left: object, right: object) -> bool:
    """Compare JSON values without Python's bool-as-int coercion."""
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is bool and type(right) is bool and left == right
    if isinstance(left, (int, float)) or isinstance(right, (int, float)):
        return (
            isinstance(left, (int, float))
            and isinstance(right, (int, float))
            and left == right
        )
    if isinstance(left, dict) or isinstance(right, dict):
        return (
            isinstance(left, dict)
            and isinstance(right, dict)
            and set(left) == set(right)
            and all(_json_values_equal(left[key], right[key]) for key in left)
        )
    if isinstance(left, list) or isinstance(right, list):
        return (
            isinstance(left, list)
            and isinstance(right, list)
            and len(left) == len(right)
            and all(
                _json_values_equal(left_item, right_item)
                for left_item, right_item in zip(left, right)
            )
        )
    return type(left) is type(right) and left == right


def _canonical_review_timestamp(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return False
    return parsed.strftime("%Y-%m-%dT%H:%M:%SZ") == value


def _artifact_binds_attestation(
    artifact: dict, attestation: dict, annotation: dict
) -> bool:
    if type(artifact.get("schema_version")) is not int or artifact["schema_version"] != 1:
        return False
    if artifact.get("reviewer") != attestation["reviewer"]:
        return False
    if artifact.get("reviewed_at") != attestation["reviewed_at"]:
        return False
    if not _canonical_review_timestamp(artifact.get("reviewed_at")):
        return False
    episode_id = annotation.get("episode_id")
    fingerprint = annotation.get("audio_fingerprint")
    expected_decision = annotation_decision(annotation)
    if artifact.get("artifact_kind") == "human_first_pass_attestation":
        bindings = artifact.get("audio_bindings")
        source_count = artifact.get("source_decision_count")
        if (
            isinstance(source_count, bool)
            or not isinstance(source_count, int)
            or source_count <= 0
            or source_count > JSON_INTEGER_MAX
            or not isinstance(bindings, list)
        ):
            return False
        matches = [
            row
            for row in bindings
            if isinstance(row, dict)
            and row.get("episode_id") == episode_id
            and row.get("audio_fingerprint") == fingerprint
        ]
        return (
            len(matches) == 1
            and _json_values_equal(
                matches[0].get("annotation_decision"), expected_decision
            )
        )
    if artifact.get("artifact_kind") != "corpus_review_attestation":
        return False
    episodes = artifact.get("episodes")
    reviews = artifact.get("reviews")
    if not isinstance(episodes, list) or not isinstance(reviews, dict):
        return False
    matches = [
        item for item in episodes
        if isinstance(item, dict)
        and item.get("episode_id") == episode_id
        and item.get("audio_fingerprint") == fingerprint
    ]
    if len(matches) != 1:
        return False
    row = matches[0]
    decision_ids = row.get("decision_ids")
    if (
        not isinstance(decision_ids, list)
        or not decision_ids
        or not all(isinstance(value, str) and value for value in decision_ids)
        or len(set(decision_ids)) != len(decision_ids)
        or not _json_values_equal(row.get("annotation_decision"), expected_decision)
    ):
        return False
    for decision_id in decision_ids:
        owners = [
            item for item in episodes
            if isinstance(item, dict)
            and isinstance(item.get("decision_ids"), list)
            and decision_id in item["decision_ids"]
        ]
        if len(owners) != 1 or owners[0] is not row:
            return False
    for decision_id in row["decision_ids"]:
        decision = reviews.get(decision_id)
        if (
            not isinstance(decision, dict)
            or decision.get("episode_id") != episode_id
            or decision.get("reviewer") != attestation["reviewer"]
            or decision.get("audio_fingerprint") != fingerprint
            or decision.get("reviewed_at") != attestation["reviewed_at"]
            or not _canonical_review_timestamp(decision.get("reviewed_at"))
            or decision.get("status") not in {"verified_ad", "false_positive", "zero_ad_confirmed"}
        ):
            return False
    return True


def validated_review_attestations(
    annotation: dict,
    *,
    source: str,
    artifact_index: dict[str, dict] | None = None,
    require_artifacts: bool = True,
) -> list[dict]:
    """Validate asset-bound reviewer evidence and return a canonical list."""
    raw = annotation.get("review_attestations")
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ConversionError(f"{source}: review_attestations must be an array")
    required = {"reviewer", "reviewed_at", "audio_fingerprint", "review_artifact_id"}
    result: list[dict] = []
    for index, attestation in enumerate(raw):
        where = f"{source}: review_attestations[{index}]"
        if not isinstance(attestation, dict) or set(attestation) != required:
            raise ConversionError(f"{where} must contain exactly {sorted(required)}")
        for field in ("reviewer", "reviewed_at", "audio_fingerprint", "review_artifact_id"):
            value = attestation.get(field)
            if not isinstance(value, str) or not value.strip() or value != value.strip():
                raise ConversionError(f"{where}.{field} must be a non-empty trimmed string")
        if not _canonical_review_timestamp(attestation["reviewed_at"]):
            raise ConversionError(
                f"{where}.reviewed_at must be normalized UTC seconds ending in Z"
            )
        if attestation["audio_fingerprint"] != annotation.get("audio_fingerprint"):
            raise ConversionError(f"{where} references a different audio asset")
        if SHA256_PATTERN.fullmatch(attestation["review_artifact_id"]) is None:
            raise ConversionError(f"{where}.review_artifact_id must be a sha256 fingerprint")
        if require_artifacts and artifact_index is None:
            raise ConversionError(f"{where} has no loaded review-artifact index")
        if artifact_index is not None:
            artifact = artifact_index.get(attestation["review_artifact_id"])
            if artifact is None or not _artifact_binds_attestation(
                artifact, attestation, annotation
            ):
                raise ConversionError(f"{where} does not resolve to matching review evidence")
        result.append(attestation)
    return result


def has_distinct_review_attestations(
    annotation: dict,
    artifact_index: dict[str, dict] | None = None,
    *,
    require_artifacts: bool = True,
) -> bool:
    """Gold requires two identities and two artifacts on the exact same asset."""
    try:
        attestations = validated_review_attestations(
            annotation,
            source="annotation",
            artifact_index=artifact_index,
            require_artifacts=require_artifacts,
        )
    except ConversionError:
        return False
    reviewers = {
        unicodedata.normalize("NFC", item["reviewer"].casefold())
        for item in attestations
    }
    artifacts = {item["review_artifact_id"] for item in attestations}
    if len(attestations) != 2 or len(reviewers) != 2 or len(artifacts) != 2:
        return False
    if artifact_index is None:
        return not require_artifacts
    artifact_kinds = sorted(
        artifact_index[item["review_artifact_id"]].get("artifact_kind")
        for item in attestations
    )
    return artifact_kinds == list(GOLD_REVIEW_ARTIFACT_KINDS)


def validate_annotation(
    annotation: dict,
    *,
    source: str,
    artifact_index: dict[str, dict] | None = None,
    require_review_artifacts: bool = True,
) -> None:
    """Validate the corpus schema and exact ad/content timeline partition."""
    episode_id = annotation.get("episode_id")
    if not isinstance(episode_id, str) or not episode_id:
        raise ConversionError(f"{source}: episode_id must be a non-empty string")
    show_name = annotation.get("show_name")
    if not isinstance(show_name, str) or not show_name:
        raise ConversionError(f"{source}: show_name must be a non-empty string")
    duration = _finite_number(
        annotation.get("duration_seconds"), field="duration_seconds", source=source
    )
    if duration <= 0:
        raise ConversionError(f"{source}: duration_seconds must be positive")
    fingerprint = annotation.get("audio_fingerprint")
    if not isinstance(fingerprint, str) or SHA256_PATTERN.fullmatch(fingerprint) is None:
        raise ConversionError(
            f"{source}: audio_fingerprint must be sha256 plus 64 lowercase hex chars"
        )
    variant_of = annotation.get("variant_of")
    _validate_optional_string(variant_of, field="variant_of", source=source)
    if variant_of == episode_id:
        raise ConversionError(f"{source}: variant_of cannot reference the same episode")
    _validate_tier_metadata(annotation, source=source)
    attestations = validated_review_attestations(
        annotation,
        source=source,
        artifact_index=artifact_index,
        require_artifacts=require_review_artifacts,
    )
    provenance = annotation.get("provenance")
    if (
        isinstance(provenance, list)
        and {value.casefold() for value in provenance if isinstance(value, str)}
        == HUMAN_PROVENANCE
        and not has_distinct_review_attestations(
            annotation,
            artifact_index,
            require_artifacts=require_review_artifacts,
        )
    ):
        raise ConversionError(
            f"{source}: human_reviewed requires two distinct asset-bound review attestations"
        )

    ad_windows = annotation.get("ad_windows")
    content_windows = annotation.get("content_windows")
    if not isinstance(ad_windows, list) or not isinstance(content_windows, list):
        raise ConversionError(f"{source}: ad_windows and content_windows must be arrays")

    intervals: list[tuple[float, float]] = []
    for index, window in enumerate(ad_windows):
        window_source = f"{source}: ad_windows[{index}]"
        if not isinstance(window, dict):
            raise ConversionError(f"{window_source} must be an object")
        start = _finite_number(
            window.get("start_seconds"), field="start_seconds", source=window_source
        )
        end = _finite_number(
            window.get("end_seconds"), field="end_seconds", source=window_source
        )
        if start < 0 or end <= start or end > duration:
            raise ConversionError(f"{window_source} has invalid range [{start}, {end}]")
        if window.get("ad_type") not in AD_TYPES:
            raise ConversionError(
                f"{window_source}: unsupported ad_type {window.get('ad_type')!r}"
            )
        transition = window.get("transition_type")
        if transition is not None and transition not in TRANSITION_TYPES:
            raise ConversionError(f"{window_source}: unsupported transition_type {transition!r}")
        for field in ("advertiser", "product", "confidence_notes"):
            _validate_optional_string(window.get(field), field=field, source=window_source)
        _validate_tier_metadata(window, source=window_source)
        intervals.append((start, end))

    for index, window in enumerate(content_windows):
        window_source = f"{source}: content_windows[{index}]"
        if not isinstance(window, dict):
            raise ConversionError(f"{window_source} must be an object")
        start = _finite_number(
            window.get("start_seconds"), field="start_seconds", source=window_source
        )
        end = _finite_number(
            window.get("end_seconds"), field="end_seconds", source=window_source
        )
        if start < 0 or end <= start or end > duration:
            raise ConversionError(f"{window_source} has invalid range [{start}, {end}]")
        _validate_optional_string(window.get("notes"), field="notes", source=window_source)
        intervals.append((start, end))

    intervals.sort()
    cursor = 0.0
    for start, end in intervals:
        if abs(start - cursor) > PARTITION_EPSILON_SECONDS:
            relation = "gap" if start > cursor else "overlap"
            raise ConversionError(
                f"{source}: timeline partition has a {relation} at {cursor}/{start}"
            )
        cursor = end
    if abs(cursor - duration) > PARTITION_EPSILON_SECONDS:
        raise ConversionError(
            f"{source}: timeline partition ends at {cursor}, expected {duration}"
        )


def _label_tier(
    *, auto_promoted: object, provenance: object, audit_priority: object
) -> str:
    """Mirror CorpusAnnotation's conservative quality-tier derivation."""
    if audit_priority == 1:
        return "boundary_proposal"
    if auto_promoted is True or audit_priority is not None:
        return "silver"
    if provenance is None:
        return "silver"
    if provenance == []:
        return "silver"
    if not isinstance(provenance, list) or not all(
        isinstance(value, str) for value in provenance
    ):
        return "silver"
    normalized = {value.lower() for value in provenance}
    if normalized & AUTOMATIC_PROVENANCE:
        return "silver"
    if normalized <= HUMAN_PROVENANCE:
        return "gold"
    return "silver"


def is_gold_annotation(
    annotation: object,
    artifact_index: dict[str, dict] | None = None,
    *,
    require_review_artifacts: bool = True,
) -> bool:
    """Return true only when the episode and every ad window are human gold."""
    if not isinstance(annotation, dict):
        return False
    has_automatic_episode_marker = (
        annotation.get("auto_promoted") is True
        or annotation.get("auto_promoted_at") is not None
        or annotation.get("auto_promoted_by") is not None
    )
    if _label_tier(
        auto_promoted=has_automatic_episode_marker,
        provenance=annotation.get("provenance"),
        audit_priority=annotation.get("audit_priority"),
    ) != "gold":
        return False
    if not has_distinct_review_attestations(
        annotation,
        artifact_index,
        require_artifacts=require_review_artifacts,
    ):
        return False
    ad_windows = annotation.get("ad_windows")
    if not isinstance(ad_windows, list):
        return False
    return all(
        isinstance(window, dict)
        and _label_tier(
            auto_promoted=(
                window.get("auto_promoted") is True
                or window.get("auto_promoted_at") is not None
                or window.get("auto_promoted_by") is not None
            ),
            provenance=window.get("provenance"),
            audit_priority=window.get("audit_priority"),
        ) == "gold"
        for window in ad_windows
    )


def canonical_annotation_paths(
    annotations_dir: Path = ANNOTATIONS_DIR,
) -> list[Path]:
    """Resolve the required schema-v1 manifest without directory scanning."""
    if path_has_symlink_component(annotations_dir) or not annotations_dir.is_dir():
        raise ConversionError(
            f"canonical annotations directory is missing or not a regular directory: "
            f"{annotations_dir}"
        )
    manifest_path = annotations_dir / MANIFEST_FILENAME
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ConversionError(
            f"canonical manifest is missing or not a regular file: {manifest_path}"
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ConversionError(f"cannot read canonical manifest {manifest_path}: {error}") from error
    if (
        not isinstance(manifest, dict)
        or type(manifest.get("schema_version")) is not int
        or manifest["schema_version"] != 1
    ):
        raise ConversionError("canonical manifest must use schema_version 1")
    names = manifest.get("annotations")
    if not isinstance(names, list) or not names:
        raise ConversionError("canonical manifest annotations must be a non-empty array")

    paths: list[Path] = []
    seen: set[str] = set()
    for name in names:
        if (
            not isinstance(name, str)
            or not name
            or Path(name).name != name
            or "/" in name
            or "\\" in name
            or name.startswith("_")
            or name.endswith(".example.json")
            or not name.endswith(".json")
        ):
            raise ConversionError(f"unsafe canonical annotation entry: {name!r}")
        if name in seen:
            raise ConversionError(f"duplicate canonical annotation entry: {name}")
        path = annotations_dir / name
        if path.is_symlink() or not path.is_file():
            raise ConversionError(f"canonical annotation is missing or not a regular file: {name}")
        seen.add(name)
        paths.append(path)
    return paths


def convert(annotation: dict) -> dict:
    episode_id = annotation["episode_id"]
    raw_hash = annotation["audio_fingerprint"]
    content_hash = raw_hash[len("sha256:"):] if raw_hash.startswith("sha256:") else raw_hash

    chapters: list[dict] = []
    for ad in annotation.get("ad_windows", []):
        chapters.append(
            {
                "startTimeSeconds": float(ad["start_seconds"]),
                "expectedDisposition": "adBreak",
                "expectedTopicLabel": AD_TYPE_LABEL.get(
                    ad.get("ad_type", ""), AD_FALLBACK_LABEL
                ),
            }
        )
    for content in annotation.get("content_windows", []):
        chapters.append(
            {
                "startTimeSeconds": float(content["start_seconds"]),
                "expectedDisposition": "content",
                "expectedTopicLabel": CONTENT_LABEL,
            }
        )
    chapters.sort(key=lambda c: c["startTimeSeconds"])

    return {
        "episodeId": episode_id,
        "episodeContentHash": content_hash,
        "chapters": chapters,
        "notes": NOTES,
    }


def atomic_write_json(path: Path, document: dict) -> None:
    """Durably replace one generated golden without exposing partial JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = path.stat().st_mode & 0o777 if path.exists() else 0o644
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temporary = Path(handle.name)
            json.dump(document, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _atomic_write_bytes(path: Path, data: bytes, *, mode: int) -> None:
    """Restore exact prior bytes during a failed set publication."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "wb", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _fsync_directory(path: Path) -> None:
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def build_conversion_plan(
    annotations_dir: Path = ANNOTATIONS_DIR,
    reviews_dir: Path = REVIEWS_DIR,
    *,
    require_review_artifacts: bool = True,
) -> tuple[dict[str, dict], int]:
    """Validate one committed canonical snapshot before planning outputs."""
    # Imported lazily because the canonical publisher imports this module's
    # annotation validator. Readers must share its lock: a replacement member
    # is written before the manifest and may still be rolled back.
    from l2f_canonical_manifest import canonical_manifest_lock

    with canonical_manifest_lock(annotations_dir):
        return _build_conversion_plan_unlocked(
            annotations_dir,
            reviews_dir,
            require_review_artifacts=require_review_artifacts,
        )


def _build_conversion_plan_unlocked(
    annotations_dir: Path,
    reviews_dir: Path,
    *,
    require_review_artifacts: bool,
) -> tuple[dict[str, dict], int]:
    """Build a plan while the caller holds the canonical publication lock."""
    planned: dict[str, dict] = {}
    skipped_non_gold = 0
    fingerprint_owners: dict[str, str] = {}
    episode_ids: set[str] = set()
    variant_references: dict[str, str] = {}
    artifact_index = load_review_artifacts(reviews_dir) if require_review_artifacts else None
    for src in canonical_annotation_paths(annotations_dir):
        try:
            annotation = json.loads(src.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ConversionError(f"cannot read {src}: {error}") from error
        if not isinstance(annotation, dict):
            raise ConversionError(f"canonical annotation must be an object: {src.name}")
        if annotation.get("episode_id") != src.stem:
            raise ConversionError(f"episode_id must match canonical filename: {src.name}")
        validate_annotation(
            annotation,
            source=src.name,
            artifact_index=artifact_index,
            require_review_artifacts=require_review_artifacts,
        )
        episode_ids.add(annotation["episode_id"])
        if annotation.get("variant_of") is not None:
            variant_references[src.name] = annotation["variant_of"]
        fingerprint = annotation["audio_fingerprint"]
        if owner := fingerprint_owners.get(fingerprint):
            raise ConversionError(
                "canonical annotations must reference unique audio: "
                f"{owner} and {src.name} share {fingerprint}"
            )
        fingerprint_owners[fingerprint] = src.name
        if not is_gold_annotation(
            annotation,
            artifact_index,
            require_review_artifacts=require_review_artifacts,
        ):
            skipped_non_gold += 1
            continue
        try:
            golden = convert(annotation)
        except (KeyError, TypeError, ValueError) as error:
            raise ConversionError(f"cannot convert {src.name}: {error}") from error
        if src.name in planned:
            raise ConversionError(f"duplicate golden output: {src.name}")
        planned[src.name] = golden
    for filename, variant_of in variant_references.items():
        if variant_of not in episode_ids:
            raise ConversionError(
                f"{filename} references non-canonical variant_of episode {variant_of!r}"
            )
    return planned, skipped_non_gold


def _validated_versioned_publication_paths(dogfood_dir: Path) -> tuple[Path, Path]:
    """Return a safe pointer parent and generation root without following aliases."""
    if not dogfood_dir.is_symlink():
        raise ConversionError(
            f"versioned golden publication requires a symlink pointer: {dogfood_dir}"
        )
    parent = dogfood_dir.parent
    generations = parent / f".{dogfood_dir.name}-generations"
    if path_has_symlink_component(parent):
        raise ConversionError(
            f"golden publication parent must not contain a symbolic link: {parent}"
        )
    if path_has_symlink_component(generations):
        raise ConversionError(
            f"golden generation root must not contain a symbolic link: {generations}"
        )
    try:
        pointer_target = Path(os.readlink(dogfood_dir))
    except OSError as error:
        raise ConversionError(f"cannot inspect golden publication pointer: {dogfood_dir}") from error
    lexical_target = Path(os.path.abspath(parent / pointer_target))
    lexical_generations = Path(os.path.abspath(generations))
    if (
        pointer_target.is_absolute()
        or pointer_target.parent != Path(generations.name)
        or pointer_target.name in {"", ".", ".."}
        or lexical_target.parent != lexical_generations
        or lexical_target.is_symlink()
        or not lexical_target.is_dir()
    ):
        raise ConversionError(
            f"golden publication pointer must reference one regular generation: {dogfood_dir}"
        )
    generations.mkdir(parents=True, exist_ok=True)
    if path_has_symlink_component(generations) or not generations.is_dir():
        raise ConversionError(
            f"golden generation root must be a regular directory: {generations}"
        )
    return parent, generations


def publish_versioned_generation(planned: dict[str, dict], dogfood_dir: Path) -> None:
    """Publish a complete immutable set behind one atomic symlink swap."""
    parent, generations = _validated_versioned_publication_paths(dogfood_dir)
    staging: Path | None = None
    if planned:
        material = json.dumps(
            planned,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        generation = generations / hashlib.sha256(material).hexdigest()
        staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=generations))
    else:
        generation = generations / "empty"
        marker = generation / ".gitkeep"
        if (
            generation.is_symlink()
            or not generation.is_dir()
            or {path.name for path in generation.iterdir()} != {marker.name}
            or marker.is_symlink()
            or not marker.is_file()
        ):
            raise ConversionError(
                f"committed empty golden generation sentinel is missing or corrupt: {generation}"
            )
    pointer_temp = parent / f".{dogfood_dir.name}.next-{os.getpid()}"
    try:
        if staging is not None:
            for filename, golden in sorted(planned.items()):
                atomic_write_json(staging / filename, golden)
            _fsync_directory(staging)
            try:
                os.replace(staging, generation)
            except OSError:
                if not generation.is_dir():
                    raise
                _validate_existing_generation(generation, planned)
                shutil.rmtree(staging)
        # The generation name must be durable before a durable pointer can
        # reference it; syncing only the pointer's parent can otherwise leave
        # a dangling symlink after a crash or power loss.
        _fsync_directory(generations)
        relative_target = os.path.relpath(generation, parent)
        pointer_temp.unlink(missing_ok=True)
        os.symlink(relative_target, pointer_temp)
        os.replace(pointer_temp, dogfood_dir)
        _fsync_directory(parent)
    finally:
        pointer_temp.unlink(missing_ok=True)
        if staging is not None and staging.exists():
            shutil.rmtree(staging)


def _validate_existing_generation(
    generation: Path, planned: dict[str, dict]
) -> None:
    """Fail closed if a hash-named immutable generation is not exact."""
    if generation.is_symlink() or not generation.is_dir():
        raise ConversionError(
            f"existing immutable golden generation is incomplete or corrupt: {generation}"
        )
    expected_names = set(planned)
    actual_names = {path.name for path in generation.iterdir()}
    if actual_names != expected_names:
        raise ConversionError(
            f"existing immutable golden generation is incomplete or corrupt: {generation}"
        )
    for filename, expected in planned.items():
        path = generation / filename
        if path.is_symlink() or not path.is_file():
            raise ConversionError(
                f"existing immutable golden generation is incomplete or corrupt: {generation}"
            )
        try:
            actual = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise ConversionError(
                f"existing immutable golden generation is incomplete or corrupt: {generation}"
            ) from error
        if actual != expected:
            raise ConversionError(
                f"existing immutable golden generation is incomplete or corrupt: {generation}"
            )


def regenerate(
    *,
    annotations_dir: Path = ANNOTATIONS_DIR,
    dogfood_dir: Path = DOGFOOD_DIR,
    reviews_dir: Path = REVIEWS_DIR,
    require_review_artifacts: bool = True,
    dry_run: bool = False,
    emit: Callable[[str], None] = print,
) -> tuple[int, int, int]:
    """Write the exact all-gold output set and remove stale generated JSON."""
    from l2f_canonical_manifest import canonical_manifest_lock

    with canonical_manifest_lock(annotations_dir):
        return _regenerate_unlocked(
            annotations_dir=annotations_dir,
            dogfood_dir=dogfood_dir,
            reviews_dir=reviews_dir,
            require_review_artifacts=require_review_artifacts,
            dry_run=dry_run,
            emit=emit,
        )


def _regenerate_unlocked(
    *,
    annotations_dir: Path,
    dogfood_dir: Path,
    reviews_dir: Path,
    require_review_artifacts: bool,
    dry_run: bool,
    emit: Callable[[str], None],
) -> tuple[int, int, int]:
    """Plan and publish while the caller holds the canonical snapshot lock."""
    planned, skipped_non_gold = _build_conversion_plan_unlocked(
        annotations_dir,
        reviews_dir,
        require_review_artifacts=require_review_artifacts,
    )
    if dogfood_dir.is_symlink():
        # Validate the intentional publication pointer before following it even
        # for a dry run. Otherwise an alias can turn stale-output discovery into
        # an external-tree read and the subsequent publish into an external write.
        _validated_versioned_publication_paths(dogfood_dir)
    elif path_has_symlink_component(dogfood_dir):
        raise ConversionError(
            f"golden output directory must not contain a symbolic link: {dogfood_dir}"
        )
    if dogfood_dir.exists() and not dogfood_dir.is_dir():
        raise ConversionError(f"golden output is not a directory: {dogfood_dir}")
    existing = sorted(dogfood_dir.glob("*.json")) if dogfood_dir.is_dir() else []
    unsafe_existing = [path for path in existing if path.is_symlink() or not path.is_file()]
    if unsafe_existing:
        raise ConversionError(
            "existing golden output is not a regular file: "
            + ", ".join(path.name for path in unsafe_existing)
        )
    stale = [path for path in existing if path.name not in planned]

    def display(path: Path) -> Path:
        try:
            return path.relative_to(REPO_ROOT)
        except ValueError:
            return path

    action = "would write" if dry_run else "wrote"
    if dry_run:
        for filename, golden in sorted(planned.items()):
            out_path = dogfood_dir / filename
            emit(f"  {action} {display(out_path)} ({len(golden['chapters'])} chapters)")
    else:
        if dogfood_dir.is_symlink():
            for filename, golden in sorted(planned.items()):
                out_path = dogfood_dir / filename
                emit(f"  {action} {display(out_path)} ({len(golden['chapters'])} chapters)")
            for path in stale:
                emit(f"  removed stale generated golden {display(path)}")
            publish_versioned_generation(planned, dogfood_dir)
            return len(planned), skipped_non_gold, len(stale)
        dogfood_dir.mkdir(parents=True, exist_ok=True)
        if path_has_symlink_component(dogfood_dir) or not dogfood_dir.is_dir():
            raise ConversionError(
                f"golden output directory must be a regular directory: {dogfood_dir}"
            )
        targets = [dogfood_dir / filename for filename in sorted(planned)]
        affected = sorted(set(targets) | set(stale))
        originals: dict[Path, tuple[bytes, int] | None] = {}
        try:
            for path in affected:
                originals[path] = (
                    (path.read_bytes(), path.stat().st_mode & 0o777)
                    if path.exists()
                    else None
                )
            for filename, golden in sorted(planned.items()):
                out_path = dogfood_dir / filename
                emit(f"  {action} {display(out_path)} ({len(golden['chapters'])} chapters)")
                atomic_write_json(out_path, golden)
            for path in stale:
                emit(f"  removed stale generated golden {display(path)}")
                path.unlink()
            if affected:
                directory_fd = os.open(dogfood_dir, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        except BaseException as error:
            rollback_errors: list[str] = []
            for path, original in originals.items():
                try:
                    if original is None:
                        path.unlink(missing_ok=True)
                    else:
                        _atomic_write_bytes(path, original[0], mode=original[1])
                except OSError as rollback_error:
                    rollback_errors.append(f"{path.name}: {rollback_error}")
            detail = f"cannot publish chapter golden set: {error}"
            if rollback_errors:
                detail += "; rollback incomplete: " + "; ".join(rollback_errors)
            raise ConversionError(detail) from error

    removal_action = "would remove" if dry_run else "removed"
    if dry_run:
        for path in stale:
            emit(f"  {removal_action} stale generated golden {display(path)}")

    return len(planned), skipped_non_gold, len(stale)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print conversion plan without writing files.",
    )
    args = parser.parse_args()

    if not ANNOTATIONS_DIR.is_dir():
        print(f"error: annotations dir not found: {ANNOTATIONS_DIR}", file=sys.stderr)
        return 1

    try:
        converted, skipped_non_gold, removed_stale = regenerate(dry_run=args.dry_run)
    except ConversionError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    suffix = " (dry-run)" if args.dry_run else ""
    stale_action = "would remove" if args.dry_run else "removed"
    print(
        f"\nConverted {converted} all-gold annotations to dogfood goldens; "
        f"skipped {skipped_non_gold} non-gold annotations; "
        f"{stale_action} {removed_stale} stale generated goldens.{suffix}"
    )
    if converted == 0:
        disposition = (
            "would publish an empty dogfood set to retire stale gold"
            if args.dry_run
            else "published an empty dogfood set to retire stale gold"
        )
        print(
            f"error: canonical corpus has no all-gold annotations; {disposition}",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
