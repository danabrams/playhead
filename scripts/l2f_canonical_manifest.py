#!/usr/bin/env python3
"""Validated, transactional publication into the canonical L2F corpus."""

from __future__ import annotations

import contextlib
import fcntl
import json
import math
import os
import re
import tempfile
import threading
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from convert_annotations_to_chapter_goldens import (
    ConversionError,
    load_review_artifacts,
    path_has_symlink_component,
    validate_annotation,
)


MANIFEST_FILENAME = "_canonical-manifest.json"
_PATH_LOCKS: dict[str, threading.RLock] = {}
_PATH_LOCKS_GUARD = threading.Lock()
_HELD_PATH_LOCKS = threading.local()


class CanonicalCorpusError(ValueError):
    """Canonical corpus inputs or a publication transaction are unsafe."""


class RejectLedgerError(CanonicalCorpusError):
    """The permanent manual-audit veto ledger is unsafe to consume."""


def _validate_filename(name: object) -> str:
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
        raise CanonicalCorpusError(f"unsafe canonical annotation entry: {name!r}")
    return name


def validate_canonical_annotation(
    document: object,
    *,
    filename: str,
    artifact_index: dict[str, dict] | None = None,
) -> dict[str, Any]:
    """Validate one annotation, including its filename/payload identity."""
    if not isinstance(document, dict):
        raise CanonicalCorpusError(f"canonical annotation must be an object: {filename}")
    if document.get("episode_id") != Path(filename).stem:
        raise CanonicalCorpusError(
            f"episode_id must match canonical filename: {filename}"
        )
    try:
        validate_annotation(
            document,
            source=filename,
            artifact_index=artifact_index,
            require_review_artifacts=bool(document.get("review_attestations")),
        )
    except ConversionError as error:
        raise CanonicalCorpusError(str(error)) from error
    return document


def load_reject_ledger(
    path: Path,
) -> dict[tuple[str, str], list[tuple[float, float]]]:
    """Load strict permanent vetoes keyed by episode and exact audio bytes."""
    if path_has_symlink_component(path):
        raise RejectLedgerError(f"audit reject ledger is not a regular file: {path}")
    if not path.exists():
        return {}
    if not path.is_file():
        raise RejectLedgerError(f"audit reject ledger is not a regular file: {path}")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise RejectLedgerError(f"cannot read audit reject ledger {path}: {error}") from error
    rejects: dict[tuple[str, str], list[tuple[float, float]]] = {}
    seen: set[tuple[str, str, float, float]] = set()
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise RejectLedgerError(
                f"invalid audit reject JSON at line {line_number}: {error}"
            ) from error
        if not isinstance(record, dict):
            raise RejectLedgerError(f"audit reject line {line_number} must be an object")
        episode_id = record.get("episodeId")
        fingerprint = record.get("audioFingerprint")
        raw_start = record.get("startSeconds")
        raw_end = record.get("endSeconds")
        if (
            not isinstance(episode_id, str)
            or not episode_id.strip()
            or episode_id != episode_id.strip()
            or isinstance(raw_start, bool)
            or isinstance(raw_end, bool)
            or not isinstance(raw_start, (int, float))
            or not isinstance(raw_end, (int, float))
            or not isinstance(fingerprint, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", fingerprint) is None
        ):
            raise RejectLedgerError(
                f"audit reject line {line_number} has invalid identity/bounds"
            )
        start = float(raw_start)
        end = float(raw_end)
        if not math.isfinite(start) or not math.isfinite(end) or start < 0 or end <= start:
            raise RejectLedgerError(
                f"audit reject line {line_number} has invalid identity/bounds"
            )
        identity = (episode_id, fingerprint, start, end)
        if identity in seen:
            raise RejectLedgerError(f"duplicate audit reject at line {line_number}: {identity}")
        seen.add(identity)
        rejects.setdefault((episode_id, fingerprint), []).append((start, end))
    return rejects


def matching_reject(
    start: float,
    end: float,
    rejects: list[tuple[float, float]],
    *,
    endpoint_tolerance: float = 5.0,
) -> tuple[float, float] | None:
    """Match the same rejected span without treating mere adjacency as identity."""
    for rejected_start, rejected_end in rejects:
        overlap = max(0.0, min(end, rejected_end) - max(start, rejected_start))
        shorter = min(end - start, rejected_end - rejected_start)
        endpoints_match = (
            abs(start - rejected_start) <= endpoint_tolerance
            and abs(end - rejected_end) <= endpoint_tolerance
        )
        if endpoints_match or (shorter > 0 and overlap / shorter >= 0.5):
            return rejected_start, rejected_end
    return None


def reject_veto_precommit(
    rejects_path: Path,
) -> Callable[[Mapping[str, dict[str, Any]], Mapping[str, dict[str, Any]]], None]:
    """Build an under-publication-lock policy that prevents known false spans."""

    def enforce(
        _existing: Mapping[str, dict[str, Any]],
        proposed: Mapping[str, dict[str, Any]],
    ) -> None:
        rejects = load_reject_ledger(rejects_path)
        for filename, annotation in proposed.items():
            episode_rejects = rejects.get(
                (annotation["episode_id"], annotation["audio_fingerprint"]), []
            )
            for window in annotation["ad_windows"]:
                match = matching_reject(
                    float(window["start_seconds"]),
                    float(window["end_seconds"]),
                    episode_rejects,
                )
                if match is not None:
                    raise CanonicalCorpusError(
                        f"{filename} includes permanently rejected span "
                        f"{match[0]:g}-{match[1]:g}"
                    )

    return enforce


def _load_canonical_corpus(
    annotations_dir: Path,
    *,
    replacements: Mapping[str, dict[str, Any]] | None = None,
    reviews_dir: Path | None = None,
    artifact_index: dict[str, dict] | None = None,
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    """Load the manifest and semantically validate its complete member set."""
    if path_has_symlink_component(annotations_dir) or not annotations_dir.is_dir():
        raise CanonicalCorpusError(
            f"canonical annotations directory is missing or not a regular directory: "
            f"{annotations_dir}"
        )
    manifest_path = annotations_dir / MANIFEST_FILENAME
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise CanonicalCorpusError(
            f"canonical manifest is missing or not a regular file: {manifest_path}"
        )
    try:
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CanonicalCorpusError(
            f"cannot read canonical manifest {manifest_path}: {error}"
        ) from error
    if (
        not isinstance(document, dict)
        or type(document.get("schema_version")) is not int
        or document["schema_version"] != 1
    ):
        raise CanonicalCorpusError("canonical manifest must use schema_version 1")
    names = document.get("annotations")
    if not isinstance(names, list) or not names:
        raise CanonicalCorpusError(
            "canonical manifest annotations must be a non-empty array"
        )

    if artifact_index is None:
        artifact_index = load_review_artifacts(
            reviews_dir if reviews_dir is not None else annotations_dir.parent / "Reviews"
        )
    seen: set[str] = set()
    annotations: dict[str, dict[str, Any]] = {}
    fingerprint_owners: dict[str, str] = {}
    for raw_name in names:
        name = _validate_filename(raw_name)
        if name in seen:
            raise CanonicalCorpusError(f"duplicate canonical annotation entry: {name}")
        seen.add(name)
        path = annotations_dir / name
        if path.is_symlink() or not path.is_file():
            raise CanonicalCorpusError(
                f"canonical annotation is missing or not a regular file: {name}"
            )
        if replacements is not None and name in replacements:
            annotation = replacements[name]
        else:
            try:
                annotation = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise CanonicalCorpusError(f"cannot read canonical annotation {name}: {error}") from error
        validated = validate_canonical_annotation(
            annotation,
            filename=name,
            artifact_index=artifact_index,
        )
        fingerprint = validated["audio_fingerprint"]
        if owner := fingerprint_owners.get(fingerprint):
            raise CanonicalCorpusError(
                "canonical annotations must reference unique audio: "
                f"{owner} and {name} share {fingerprint}"
            )
        fingerprint_owners[fingerprint] = name
        annotations[name] = validated
    unknown_replacements = set(replacements or {}) - seen
    if unknown_replacements:
        raise CanonicalCorpusError(
            "replacement is not a canonical manifest member: "
            + ", ".join(sorted(unknown_replacements))
        )
    episode_ids = {annotation["episode_id"] for annotation in annotations.values()}
    for name, annotation in annotations.items():
        variant_of = annotation.get("variant_of")
        if variant_of is not None and variant_of not in episode_ids:
            raise CanonicalCorpusError(
                f"{name} references non-canonical variant_of episode {variant_of!r}"
            )
    return names, annotations


def load_canonical_manifest(
    annotations_dir: Path,
    *,
    reviews_dir: Path | None = None,
) -> list[str]:
    """Load and semantically validate every member of the schema-v1 manifest."""
    with canonical_manifest_lock(annotations_dir):
        names, _ = _load_canonical_corpus(annotations_dir, reviews_dir=reviews_dir)
        return names


def load_canonical_annotations(
    annotations_dir: Path,
    *,
    reviews_dir: Path | None = None,
) -> dict[str, dict[str, Any]]:
    """Return validated canonical documents keyed by their manifest filenames."""
    with canonical_manifest_lock(annotations_dir):
        _, annotations = _load_canonical_corpus(
            annotations_dir, reviews_dir=reviews_dir
        )
        return annotations


def validate_canonical_replacement(
    annotations_dir: Path,
    *,
    filename: str,
    replacement: dict[str, Any],
    reviews_dir: Path | None = None,
) -> None:
    """Validate the complete corpus as it would look after one replacement."""
    _load_canonical_corpus(
        annotations_dir,
        replacements={_validate_filename(filename): replacement},
        reviews_dir=reviews_dir,
    )


def _path_lock(path: Path) -> threading.RLock:
    key = str(path.resolve())
    with _PATH_LOCKS_GUARD:
        return _PATH_LOCKS.setdefault(key, threading.RLock())


@contextlib.contextmanager
def canonical_manifest_lock(annotations_dir: Path):
    """Serialize publishers in this process and across cooperating processes."""
    if path_has_symlink_component(annotations_dir) or not annotations_dir.is_dir():
        raise CanonicalCorpusError(
            f"canonical annotations directory is missing or not a regular directory: "
            f"{annotations_dir}"
        )
    resolved = annotations_dir.resolve()
    lock_path = resolved / ".canonical-manifest.lock"
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    with _path_lock(resolved):
        held = getattr(_HELD_PATH_LOCKS, "paths", None)
        if held is None:
            held = set()
            _HELD_PATH_LOCKS.paths = held
        key = str(resolved)
        if key in held:
            yield
            return
        try:
            descriptor = os.open(lock_path, flags, 0o600)
        except OSError as error:
            raise CanonicalCorpusError(
                f"cannot open canonical publication lock {lock_path}: {error}"
            ) from error
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            held.add(key)
            yield
        finally:
            held.discard(key)
            # Closing the descriptor releases flock. A separate LOCK_UN can
            # fail after a caller has durably committed and falsely turn that
            # successful transaction into a reported failure.
            try:
                os.close(descriptor)
            except OSError:
                pass


def _atomic_write_bytes(path: Path, data: bytes, *, mode: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    target_mode = mode if mode is not None else (
        path.stat().st_mode & 0o777 if path.exists() else 0o644
    )
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "wb", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, target_mode)
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _json_bytes(document: object) -> bytes:
    try:
        return (
            json.dumps(document, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise CanonicalCorpusError(f"annotation is not valid JSON data: {error}") from error


def commit_canonical_annotations(
    annotations_dir: Path,
    annotations: Mapping[str, dict[str, Any]],
    *,
    force: bool,
    reviews_dir: Path | None = None,
    writer: Callable[[Path, bytes], None] | None = None,
    precommit: Callable[
        [Mapping[str, dict[str, Any]], Mapping[str, dict[str, Any]]], None
    ]
    | None = None,
) -> list[Path]:
    """Publish a validated annotation batch and manifest membership together.

    Every payload is encoded before mutation. Publication is serialized, the
    manifest is replaced last, and ordinary write failures restore all prior
    bytes. New files remain invisible to canonical consumers until the final
    manifest replacement.
    """
    encoded: dict[str, bytes] = {}
    proposed: dict[str, dict[str, Any]] = {}
    resolved_reviews_dir = (
        reviews_dir if reviews_dir is not None else annotations_dir.parent / "Reviews"
    )
    artifact_index = load_review_artifacts(resolved_reviews_dir)
    for raw_name, annotation in annotations.items():
        name = _validate_filename(raw_name)
        validate_canonical_annotation(
            annotation,
            filename=name,
            artifact_index=artifact_index,
        )
        encoded[name] = _json_bytes(annotation)
        proposed[name] = json.loads(encoded[name])

    publish = writer or (lambda path, data: _atomic_write_bytes(path, data))
    with canonical_manifest_lock(annotations_dir):
        # Review artifacts are separate immutable files, so reload their index
        # after acquiring the publication lock and validate both the current
        # corpus and every proposed payload against that same snapshot.
        artifact_index = load_review_artifacts(resolved_reviews_dir)
        existing_names, existing_annotations = _load_canonical_corpus(
            annotations_dir,
            reviews_dir=resolved_reviews_dir,
            artifact_index=artifact_index,
        )
        for name, annotation in proposed.items():
            validate_canonical_annotation(
                annotation,
                filename=name,
                artifact_index=artifact_index,
            )
        if not encoded:
            return []
        replaced_members = sorted(set(encoded) & set(existing_names))
        if len(replaced_members) > 1:
            raise CanonicalCorpusError(
                "refusing non-atomic replacement of multiple canonical members: "
                + ", ".join(replaced_members)
                + "; publish one replacement per transaction"
            )
        if replaced_members and len(encoded) > 1:
            new_members = sorted(set(encoded) - set(existing_names))
            raise CanonicalCorpusError(
                "refusing non-atomic transaction that mixes canonical replacement "
                f"{replaced_members[0]} with new members: "
                + ", ".join(new_members)
                + "; publish the replacement alone"
            )
        resulting_annotations = dict(existing_annotations)
        resulting_annotations.update(proposed)
        fingerprint_owners: dict[str, str] = {}
        for name, annotation in resulting_annotations.items():
            fingerprint = annotation["audio_fingerprint"]
            if owner := fingerprint_owners.get(fingerprint):
                raise CanonicalCorpusError(
                    "canonical annotations must reference unique audio: "
                    f"{owner} and {name} share {fingerprint}"
                )
            fingerprint_owners[fingerprint] = name
        resulting_episode_ids = {
            annotation["episode_id"] for annotation in resulting_annotations.values()
        }
        for name, annotation in resulting_annotations.items():
            variant_of = annotation.get("variant_of")
            if variant_of is not None and variant_of not in resulting_episode_ids:
                raise CanonicalCorpusError(
                    f"{name} references non-canonical variant_of episode {variant_of!r}"
                )
        if precommit is not None:
            precommit(existing_annotations, proposed)
        paths = [annotations_dir / name for name in sorted(encoded)]
        for path in paths:
            if path.is_symlink():
                raise CanonicalCorpusError(
                    f"annotation output must not be a symbolic link: {path.name}"
                )
            if path.exists() and not path.is_file():
                raise CanonicalCorpusError(
                    f"annotation output is not a regular file: {path.name}"
                )
            if path.exists() and not force:
                raise FileExistsError(f"{path} already exists; pass --force to overwrite")

        manifest_path = annotations_dir / MANIFEST_FILENAME
        original_manifest = manifest_path.read_bytes()
        originals: dict[Path, tuple[bytes, int] | None] = {}
        for path in paths:
            originals[path] = (
                (path.read_bytes(), path.stat().st_mode & 0o777) if path.exists() else None
            )
        updated_manifest = _json_bytes(
            {
                "schema_version": 1,
                "annotations": sorted(set(existing_names) | set(encoded)),
            }
        )

        try:
            for path in paths:
                publish(path, encoded[path.name])
            publish(manifest_path, updated_manifest)
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
            try:
                _atomic_write_bytes(manifest_path, original_manifest)
            except OSError as rollback_error:
                rollback_errors.append(f"{MANIFEST_FILENAME}: {rollback_error}")
            if rollback_errors:
                raise CanonicalCorpusError(
                    "publication failed and rollback was incomplete: "
                    + "; ".join(rollback_errors)
                ) from error
            raise
        return paths
