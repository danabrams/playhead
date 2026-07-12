#!/usr/bin/env python3
"""Freeze and score three unchanged-production runs against partial silver.

Only explicitly labeled regions are scored. Unlabeled audio remains unknown,
and the report deliberately omits whole-episode precision/recall claims.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
import re
import secrets
import stat
import statistics
import sys
import unicodedata
from decimal import Decimal
from typing import NamedTuple, Sequence


SCORER_PATH = pathlib.Path(__file__).resolve()
ROOT = SCORER_PATH.parents[1]
EVALUATIONS_DIR = ROOT / "TestFixtures/Corpus/Evaluations"
DEFAULT_EVALUATION = EVALUATIONS_DIR / (
    "earaudit-partial-silver-"
    "0d85a0ec8bfa30873bad63bbc4bb12a3f7613aca76d5b76149e25db2a0be226f.json"
)

SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
FINGERPRINT_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
REVISION_PATTERN = re.compile(r"[0-9a-f]{40}\Z")
RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
UTC_PATTERN = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z\Z"
)
MAX_JSON_INPUT_BYTES = 128 * 1024 * 1024
EXPECTED_ASSET_COUNT = 27
MAX_PREDICTIONS_PER_EPISODE = 4_096
MAX_MUSIC_FEATURES_PER_EPISODE = 4_096
REQUIRED_RUN_IDS = ["baseline-run-1", "baseline-run-2", "baseline-run-3"]

EVALUATION_FIELDS = frozenset(
    {"artifact_kind", "assets", "label_semantics", "schema_version", "sources", "summary"}
)
ASSET_FIELDS = frozenset(
    {
        "audio_fingerprint",
        "content_vetoes",
        "duration_seconds",
        "episode_id",
        "full_breaks",
        "presence_anchors",
        "show_name",
    }
)
FULL_BREAK_FIELDS = frozenset(
    {
        "end_seconds",
        "source_ledger_ids",
        "source_review_ids",
        "start_seconds",
        "supporting_tight_ledger_ids",
    }
)
ANCHOR_FIELDS = frozenset({"end_seconds", "source_ledger_ids", "start_seconds"})
VETO_FIELDS = frozenset(
    {"end_seconds", "source_ledger_ids", "source_review_ids", "start_seconds"}
)
LABEL_SEMANTICS = {
    "content_vetoes": "only the exact interval is labeled human-reviewed content; the separate reject ledger may conservatively block overlapping promotion candidates without labeling surrounding audio",
    "coverage": "partial",
    "full_breaks": "human-reviewed complete contiguous ad-break boundaries",
    "presence_anchors": "ad presence only; bounds are not full-break boundary truth",
    "quality": "silver",
    "unlabeled_audio": "unknown_elsewhere",
}

POLICY_FIELDS = frozenset(
    {
        "artifact_kind",
        "asset_duration_tolerance_seconds",
        "eligible_decision_states",
        "evaluation_sha256",
        "full_break_coverage_threshold",
        "matching",
        "music_edge_cohort",
        "partial_label_semantics",
        "production_capture",
        "required_run_count",
        "run_ids",
        "schema_version",
        "show_split",
        "three_run_aggregation",
    }
)
MATCHING_POLICY = {
    "eligibility": "positive_intersection_seconds",
    "objective": [
        "maximum_cardinality",
        "maximum_total_intersection_seconds",
        "minimum_total_boundary_error_seconds",
        "stable_lexicographic_tie_break",
    ],
}
PARTIAL_LABEL_POLICY = {
    "prediction_precision": "conditional_on_overlap_with_a_labeled_region",
    "positive_veto_conflicts": (
        "excluded_from_conditional_precision_positive_matching_and_counted_as_"
        "labeled_errors_without_changing_primary_detection_matching"
    ),
    "unlabeled_audio": "unknown",
    "was_skipped": "reported_separately_without_changing_eligibility",
    "whole_episode_precision": "not_computed",
}
MUSIC_POLICY = {
    "band_seconds": 8.0,
    "cohorts": ["continuous_music_bed", "nonmusic", "insufficient_context"],
    "continuous_requirement": "all_four_start_outside_start_inside_end_inside_end_outside_bands_pass",
    "minimum_music_window_fraction": 0.3,
    "minimum_windows_per_band": 3,
    "music_bed_present_levels": ["background", "foreground"],
    "window_assignment": "midpoint",
}
PRODUCTION_CAPTURE_POLICY = {
    "ad_detection_config_identity": "AdDetectionConfig.default",
    "entry_point": "AdDetectionService.runBackfill",
    "foundation_model_classifier_identity": (
        "PlayheadRuntime.makeFoundationModelClassifier(SystemLanguageModel.default)"
    ),
    "foundation_model_environment_identity": "production_no_experiment_overrides",
    "foundation_model_redactor_identity": "PromptRedactor.loadDefault",
    "detector_state_identity": (
        "cold_isolated_per_episode:fresh_store;nil_catalog_cache_learning_"
        "orchestration;fallback_metadata;production_calibration"
    ),
    "hot_path_classifier_identity": "CoreMLSequenceClassifier()",
    "narrowing_config_identity": "NarrowingConfig.default",
    "planner_regime": "targetedWithAudit",
    "planner_seed_observations": 5,
    "runner_admission_identity": "permissive_capability_snapshot+battery_level_1.0",
    "scan_cohort_identity": "ScanCohort.productionJSON",
    "source_identity": "clean_full_git_head_before_after_and_binary_build_prefix_match",
}
THREE_RUN_AGGREGATION_POLICY = {
    "missing_values": "excluded_and_reported_with_runs_with_value",
    "statistics": ["median", "minimum", "maximum"],
    "unit": "per_run_metric",
}
SHOW_SPLIT_POLICY = {
    "algorithm": "sha256_salted_bucket_v1",
    "bucket_count": 1000,
    "cohorts": ["development", "holdout"],
    "holdout_bucket_count": 300,
    "salt": "playhead-l2f.8-v1",
}
ELIGIBLE_DECISION_STATES = ["candidate", "confirmed", "applied"]
KNOWN_DECISION_STATES = frozenset(
    {"candidate", "confirmed", "applied", "suppressed", "reverted"}
)
MUSIC_BED_LEVELS = frozenset({"none", "background", "foreground"})

PRODUCTION_CONFIG_FIELDS = frozenset(
    {
        "ad_detection_config_identity",
        "ad_detection_defaults",
        "detector_state_identity",
        "entry_point",
        "foundation_model_classifier_identity",
        "foundation_model_environment_identity",
        "foundation_model_redactor_identity",
        "hot_path_classifier_identity",
        "narrowing_config_identity",
        "narrowing_defaults",
        "pipeline_versions",
        "planner_regime",
        "planner_seed_observations",
        "runner_admission_identity",
        "scan_cohort_identity",
    }
)
PIPELINE_VERSION_FIELDS = frozenset(
    {"feature_schema_version", "model_version", "policy_version"}
)
AD_DETECTION_DEFAULT_FIELDS = frozenset(
    {
        "audio_forensics_enabled",
        "auto_skip_confidence_threshold",
        "bracket_refinement_enabled",
        "bracket_refinement_min_coarse_score",
        "bracket_refinement_min_fine_confidence",
        "bracket_refinement_min_trust",
        "candidate_threshold",
        "chapter_signal_mode",
        "classifier_seed_qualified_threshold",
        "confirmation_threshold",
        "cross_episode_memory_enabled",
        "cross_show_syndication_enabled",
        "detector_version",
        "evidence_fragility_penalty_enabled",
        "fm_backfill_mode",
        "fm_consensus_threshold",
        "fm_scan_budget_seconds",
        "fragility_penalty",
        "fragility_threshold",
        "hot_path_lookahead",
        "lexical_auto_ad_enabled",
        "lexical_auto_ad_qualified_threshold",
        "mark_only_threshold",
        "per_show_threshold_control_enabled",
        "per_show_threshold_integral_gain",
        "per_show_threshold_max_offset",
        "per_show_threshold_min_samples",
        "per_show_threshold_proportional_gain",
        "rediff_slot_ownership_enabled",
        "rediff_slot_shadow_enabled",
        "rhetorical_grammar_enabled",
        "segment_auto_skip_threshold",
        "segment_ui_candidate_threshold",
        "span_finalizer_enabled",
        "splice_slot_ownership_enabled",
        "splice_slot_shadow_enabled",
        "suppression_threshold",
        "temporal_high_confidence_neighbor_threshold",
        "temporal_isolation_penalty_factor",
        "temporal_min_dwell_penalty_factor",
        "temporal_min_dwell_seconds",
        "temporal_neighbor_window_seconds",
        "temporal_regularization_enabled",
        "transcript_boundary_cue_enabled",
    }
)
NARROWING_DEFAULT_FIELDS = frozenset(
    {
        "acoustic_break_snap_max_distance_seconds",
        "lexical_cluster_gap_seconds",
        "lexical_cluster_margin_segments",
        "lexical_cluster_min_hits",
        "lexical_cluster_snap_enabled",
        "max_narrowed_segments_per_phase",
        "per_anchor_padding_segments",
    }
)
RUNTIME_FIELDS = frozenset(
    {
        "architecture",
        "executable_identity",
        "foundation_models_availability",
        "foundation_models_context_size",
        "locale_identifier",
        "os_version",
        "xcode_version_actual",
    }
)

EXPECTED_AD_DETECTION_DEFAULTS = {
    "audio_forensics_enabled": False,
    "auto_skip_confidence_threshold": 0.80,
    "bracket_refinement_enabled": True,
    "bracket_refinement_min_coarse_score": 0.30,
    "bracket_refinement_min_fine_confidence": 0.20,
    "bracket_refinement_min_trust": 0.40,
    "candidate_threshold": 0.40,
    "chapter_signal_mode": "off",
    "classifier_seed_qualified_threshold": 0.50,
    "confirmation_threshold": 0.70,
    "cross_episode_memory_enabled": False,
    "cross_show_syndication_enabled": False,
    "detector_version": "detection-v1",
    "evidence_fragility_penalty_enabled": False,
    "fm_backfill_mode": "full",
    "fm_consensus_threshold": 2,
    "fm_scan_budget_seconds": 300.0,
    "fragility_penalty": 0.85,
    "fragility_threshold": 2.0,
    "hot_path_lookahead": 90.0,
    "lexical_auto_ad_enabled": False,
    "lexical_auto_ad_qualified_threshold": 0.50,
    "mark_only_threshold": 0.60,
    "per_show_threshold_control_enabled": False,
    "per_show_threshold_integral_gain": 0.005,
    "per_show_threshold_max_offset": 0.15,
    "per_show_threshold_min_samples": 5,
    "per_show_threshold_proportional_gain": 0.02,
    "rediff_slot_ownership_enabled": False,
    "rediff_slot_shadow_enabled": False,
    "rhetorical_grammar_enabled": False,
    "segment_auto_skip_threshold": 0.55,
    "segment_ui_candidate_threshold": 0.40,
    "span_finalizer_enabled": False,
    "splice_slot_ownership_enabled": False,
    "splice_slot_shadow_enabled": False,
    "suppression_threshold": 0.25,
    "temporal_high_confidence_neighbor_threshold": 0.80,
    "temporal_isolation_penalty_factor": 0.85,
    "temporal_min_dwell_penalty_factor": 0.90,
    "temporal_min_dwell_seconds": 10.0,
    "temporal_neighbor_window_seconds": 120.0,
    "temporal_regularization_enabled": False,
    "transcript_boundary_cue_enabled": True,
}
EXPECTED_NARROWING_DEFAULTS = {
    "acoustic_break_snap_max_distance_seconds": 2.0,
    "lexical_cluster_gap_seconds": 8.0,
    "lexical_cluster_margin_segments": 3,
    "lexical_cluster_min_hits": 1,
    "lexical_cluster_snap_enabled": True,
    "max_narrowed_segments_per_phase": 60,
    "per_anchor_padding_segments": 5,
}
EXPECTED_PIPELINE_VERSIONS = {
    "feature_schema_version": 1,
    "model_version": "detection-v1",
    "policy_version": "skip-policy-v1",
}

RAW_FIELDS = frozenset(
    {
        "artifact_kind",
        "captured_at_utc",
        "episodes",
        "evaluation_sha256",
        "production_config",
        "run_id",
        "runtime",
        "schema_version",
        "source_revision",
    }
)
RAW_EPISODE_FIELDS = frozenset(
    {
        "audio_fingerprint",
        "duration_seconds",
        "episode_id",
        "music_features",
        "predictions",
        "show_name",
        "transcript_sha256",
    }
)
PREDICTION_FIELDS = frozenset(
    {"confidence", "decision_state", "end_seconds", "start_seconds", "was_skipped"}
)
MUSIC_FEATURE_FIELDS = frozenset(
    {
        "end_seconds",
        "music_bed_change_score",
        "music_bed_level",
        "music_bed_offset_score",
        "music_bed_onset_score",
        "music_probability",
        "start_seconds",
    }
)


class ScoringError(ValueError):
    """An input cannot produce an honest, reproducible baseline."""


class Match(NamedTuple):
    reference_index: int
    prediction_index: int
    intersection_seconds: float


class FreezeResult(NamedTuple):
    report_path: pathlib.Path
    raw_paths: tuple[pathlib.Path, ...]


def _canonical_text(value: object, field: str, *, allow_empty: bool = False) -> str:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or (not value and not allow_empty)
        or "\x00" in value
        or unicodedata.normalize("NFC", value) != value
    ):
        raise ScoringError(f"{field} must be a canonical string")
    return value


def _finite_number(
    value: object,
    field: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScoringError(f"{field} must be a finite number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as error:
        raise ScoringError(f"{field} must be a finite number") from error
    if not math.isfinite(number):
        raise ScoringError(f"{field} must be a finite number")
    if minimum is not None and number < minimum:
        raise ScoringError(f"{field} must be at least {minimum}")
    if maximum is not None and number > maximum:
        raise ScoringError(f"{field} must be at most {maximum}")
    return number


def _integer(value: object, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ScoringError(f"{field} must be an integer at least {minimum}")
    return value


def _require_fields(value: object, expected: frozenset[str], field: str) -> dict:
    if not isinstance(value, dict):
        raise ScoringError(f"{field} must be an object")
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise ScoringError(f"{field} fields mismatch; missing={missing}, extra={extra}")
    return value


def _has_symlink_component(path: pathlib.Path) -> bool:
    absolute = pathlib.Path(os.path.abspath(path))
    root = pathlib.Path(absolute.anchor)
    current = root
    for part in absolute.parts[1:]:
        current /= part
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            continue
        except OSError as error:
            raise ScoringError(f"cannot inspect path {path}: {error}") from error
        if not stat.S_ISLNK(metadata.st_mode):
            continue
        # macOS exposes /var, /tmp, and /etc as root-owned compatibility
        # links. Deeper or user-owned aliases remain forbidden.
        if current.parent != root or metadata.st_uid != 0:
            return True
    return False


def _descriptor_relative_components(path: pathlib.Path, label: str) -> list[str]:
    """Return absolute path components with immutable macOS root links expanded."""
    path = pathlib.Path(path)
    if any(part in {".", ".."} for part in path.parts):
        raise ScoringError(f"{label} path contains traversal: {path}")
    absolute = pathlib.Path(os.path.abspath(path))
    components = list(absolute.parts[1:])
    if not components:
        return []

    first = pathlib.Path(absolute.anchor) / components[0]
    try:
        metadata = os.lstat(first)
    except FileNotFoundError:
        return components
    except OSError as error:
        raise ScoringError(f"cannot inspect {label} path {path}: {error}") from error
    if not stat.S_ISLNK(metadata.st_mode):
        return components
    if components[0] not in {"etc", "tmp", "var"} or metadata.st_uid != 0:
        raise ScoringError(f"{label} path contains a symbolic link: {path}")
    try:
        target = pathlib.Path(os.readlink(first))
    except OSError as error:
        raise ScoringError(f"cannot inspect {label} path {path}: {error}") from error
    if not target.is_absolute():
        target = first.parent / target
    expanded = pathlib.Path(os.path.abspath(target))
    return list(expanded.parts[1:]) + components[1:]


def _open_directory_descriptor(
    path: pathlib.Path,
    label: str,
    *,
    create: bool = False,
) -> int:
    """Open a directory without re-resolving checked path components."""
    components = _descriptor_relative_components(path, label)
    flags = os.O_RDONLY | os.O_NONBLOCK
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        current = os.open(pathlib.Path(path).anchor or os.path.sep, flags)
    except OSError as error:
        raise ScoringError(f"cannot open {label} {path}: {error}") from error
    try:
        for component in components:
            try:
                before = os.stat(component, dir_fd=current, follow_symlinks=False)
            except FileNotFoundError:
                if not create:
                    raise
                try:
                    os.mkdir(component, 0o755, dir_fd=current)
                except FileExistsError:
                    pass
                before = os.stat(component, dir_fd=current, follow_symlinks=False)
            if not stat.S_ISDIR(before.st_mode):
                raise ScoringError(f"{label} contains a non-directory component: {path}")
            next_descriptor = os.open(component, flags, dir_fd=current)
            try:
                after = os.fstat(next_descriptor)
            except BaseException:
                os.close(next_descriptor)
                raise
            if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                os.close(next_descriptor)
                raise ScoringError(f"{label} changed while it was being opened: {path}")
            os.close(current)
            current = next_descriptor
        return current
    except ScoringError:
        os.close(current)
        raise
    except OSError as error:
        os.close(current)
        raise ScoringError(f"cannot open {label} {path}: {error}") from error
    except BaseException:
        os.close(current)
        raise


def _open_regular_descriptor(path: pathlib.Path, label: str) -> int:
    path = pathlib.Path(path)
    _descriptor_relative_components(path, label)
    parent = pathlib.Path(os.path.abspath(path)).parent
    name = pathlib.Path(os.path.abspath(path)).name or "."
    parent_descriptor = _open_directory_descriptor(parent, f"{label} parent")
    flags = os.O_RDONLY | os.O_NONBLOCK
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor: int | None = None
    try:
        before = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode):
            raise ScoringError(f"{label} is missing or not a regular file: {path}")
        descriptor = os.open(name, flags, dir_fd=parent_descriptor)
        after = os.fstat(descriptor)
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            os.close(descriptor)
            descriptor = None
            raise ScoringError(f"{label} changed while it was being opened: {path}")
        return descriptor
    except ScoringError:
        if descriptor is not None:
            os.close(descriptor)
        raise
    except OSError as error:
        if descriptor is not None:
            os.close(descriptor)
        raise ScoringError(
            f"{label} is missing or not a regular file: {path}: {error}"
        ) from error
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
        raise
    finally:
        os.close(parent_descriptor)


def _read_open_regular_bytes(
    descriptor: int,
    path: pathlib.Path,
    label: str,
    maximum_bytes: int,
) -> bytes:
    before = os.fstat(descriptor)
    if not stat.S_ISREG(before.st_mode):
        raise ScoringError(f"{label} is missing or not a regular file: {path}")
    if before.st_size > maximum_bytes:
        raise ScoringError(f"{label} exceeds {maximum_bytes} bytes: {path}")
    chunks: list[bytes] = []
    byte_count = 0
    while True:
        chunk = os.read(descriptor, 1024 * 1024)
        if not chunk:
            break
        byte_count += len(chunk)
        if byte_count > maximum_bytes:
            raise ScoringError(f"{label} exceeds {maximum_bytes} bytes: {path}")
        chunks.append(chunk)
    after = os.fstat(descriptor)
    if (
        after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
        or after.st_ctime_ns != before.st_ctime_ns
        or byte_count != after.st_size
    ):
        raise ScoringError(f"{label} changed while it was being read: {path}")
    return b"".join(chunks)


def _read_regular_bytes(
    path: pathlib.Path,
    label: str,
    *,
    maximum_bytes: int = MAX_JSON_INPUT_BYTES,
) -> bytes:
    path = pathlib.Path(path)
    descriptor = _open_regular_descriptor(path, label)
    try:
        return _read_open_regular_bytes(descriptor, path, label, maximum_bytes)
    finally:
        os.close(descriptor)


def _parse_json(data: bytes, label: str) -> object:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ScoringError(f"invalid {label} JSON: {error}") from error

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ScoringError(f"invalid {label}: duplicate JSON object field {key!r}")
            result[key] = value
        return result

    def reject_constant(value: str) -> object:
        raise ScoringError(f"invalid {label}: {value} must be a finite JSON number")

    def finite_float(value: str) -> float:
        number = float(value)
        if not math.isfinite(number):
            raise ScoringError(f"invalid {label}: {value} must be a finite JSON number")
        return number

    try:
        return json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
            parse_float=finite_float,
        )
    except ScoringError:
        raise
    except (ValueError, RecursionError) as error:
        raise ScoringError(f"invalid {label} JSON: {error}") from error


def read_json_file(path: pathlib.Path, label: str) -> object:
    return _parse_json(_read_regular_bytes(path, label), label)


def sha256_file(path: pathlib.Path) -> str:
    return hashlib.sha256(_read_regular_bytes(path, "SHA-256 input")).hexdigest()


def _validate_content_address(
    path: pathlib.Path,
    digest: str,
    label: str,
    *,
    filename_prefix: str,
) -> None:
    expected_name = f"{filename_prefix}{digest}.json"
    if path.name != expected_name:
        raise ScoringError(f"{label} path does not match its content address: {path}")


def _validate_sha256(value: object, field: str) -> str:
    text = _canonical_text(value, field)
    if not SHA256_PATTERN.fullmatch(text):
        raise ScoringError(f"{field} must be a lowercase SHA-256 digest")
    return text


def _validate_fingerprint(value: object, field: str) -> str:
    text = _canonical_text(value, field)
    if not FINGERPRINT_PATTERN.fullmatch(text):
        raise ScoringError(f"{field} must be a sha256: audio fingerprint")
    return text


def _validate_timestamp(value: object, field: str) -> str:
    text = _canonical_text(value, field)
    if not UTC_PATTERN.fullmatch(text):
        raise ScoringError(f"{field} must be a canonical UTC timestamp")
    try:
        parsed = dt.datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as error:
        raise ScoringError(f"{field} must be a canonical UTC timestamp") from error
    if parsed.utcoffset() != dt.timedelta(0):
        raise ScoringError(f"{field} must be a canonical UTC timestamp")
    return text


def _validate_string_list(value: object, field: str, *, allow_empty: bool = True) -> list[str]:
    if not isinstance(value, list) or (not allow_empty and not value):
        raise ScoringError(f"{field} must be a list of canonical strings")
    result = [_canonical_text(item, f"{field}[{index}]") for index, item in enumerate(value)]
    if len(result) != len(set(result)):
        raise ScoringError(f"{field} contains duplicate strings")
    return result


def _validate_repo_relative_path(value: object, field: str) -> str:
    text = _canonical_text(value, field)
    path = pathlib.PurePosixPath(text)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ScoringError(f"{field} must be a traversal-free repository-relative path")
    if "\\" in text or path.as_posix() != text:
        raise ScoringError(f"{field} must be a traversal-free repository-relative path")
    return text


def _validate_bare_identifier(value: object, field: str) -> str:
    text = _canonical_text(value, field)
    if text in {".", ".."} or "/" in text or "\\" in text:
        raise ScoringError(f"{field} must be a bare identifier")
    return text


def _validate_region(
    value: object,
    fields: frozenset[str],
    field: str,
    duration: float,
) -> dict:
    region = _require_fields(value, fields, field)
    start = _finite_number(region["start_seconds"], f"{field}.start_seconds", minimum=0)
    end = _finite_number(region["end_seconds"], f"{field}.end_seconds", minimum=0)
    if end <= start or end > duration:
        raise ScoringError(f"{field} must be a positive interval within the episode")
    normalized = dict(region)
    normalized["start_seconds"] = start
    normalized["end_seconds"] = end
    for list_field in fields - {"start_seconds", "end_seconds"}:
        normalized[list_field] = _validate_string_list(
            region[list_field], f"{field}.{list_field}"
        )
    return normalized


def load_evaluation(path: pathlib.Path) -> dict:
    data = _read_regular_bytes(path, "evaluation")
    digest = hashlib.sha256(data).hexdigest()
    _validate_content_address(
        path,
        digest,
        "evaluation",
        filename_prefix="earaudit-partial-silver-",
    )
    document = _require_fields(_parse_json(data, "evaluation"), EVALUATION_FIELDS, "evaluation")
    if _integer(document["schema_version"], "evaluation.schema_version", minimum=1) != 1:
        raise ScoringError("evaluation.schema_version must equal 1")
    if document["artifact_kind"] != "retained_audio_partial_silver_evaluation":
        raise ScoringError("evaluation.artifact_kind is not partial silver")
    if document["label_semantics"] != LABEL_SEMANTICS:
        raise ScoringError("evaluation.label_semantics do not preserve partial-silver scope")

    sources = document["sources"]
    expected_source_names = {"audit_ledger", "ear_audit_review", "reject_ledger"}
    if not isinstance(sources, dict) or set(sources) != expected_source_names:
        raise ScoringError("evaluation.sources fields mismatch")
    normalized_sources: dict[str, dict] = {}
    for name in sorted(expected_source_names):
        source = _require_fields(sources[name], frozenset({"path", "sha256"}), f"sources.{name}")
        normalized_sources[name] = {
            "path": _validate_repo_relative_path(source["path"], f"sources.{name}.path"),
            "sha256": _validate_sha256(source["sha256"], f"sources.{name}.sha256"),
        }

    if (
        not isinstance(document["assets"], list)
        or len(document["assets"]) != EXPECTED_ASSET_COUNT
    ):
        raise ScoringError(
            f"evaluation.assets must contain exactly {EXPECTED_ASSET_COUNT} assets"
        )
    normalized_assets: list[dict] = []
    episode_ids: set[str] = set()
    fingerprints: set[str] = set()
    for asset_index, raw_asset in enumerate(document["assets"]):
        field = f"evaluation.assets[{asset_index}]"
        asset = _require_fields(raw_asset, ASSET_FIELDS, field)
        episode_id = _validate_bare_identifier(asset["episode_id"], f"{field}.episode_id")
        show_name = _canonical_text(asset["show_name"], f"{field}.show_name")
        fingerprint = _validate_fingerprint(asset["audio_fingerprint"], f"{field}.audio_fingerprint")
        duration = _finite_number(asset["duration_seconds"], f"{field}.duration_seconds", minimum=0)
        if duration <= 0:
            raise ScoringError(f"{field}.duration_seconds must be positive")
        if episode_id in episode_ids:
            raise ScoringError(f"duplicate evaluation asset {episode_id}")
        if fingerprint in fingerprints:
            raise ScoringError(f"duplicate evaluation audio fingerprint {fingerprint}")
        episode_ids.add(episode_id)
        fingerprints.add(fingerprint)
        lists: dict[str, list[dict]] = {}
        for name, fields in (
            ("full_breaks", FULL_BREAK_FIELDS),
            ("presence_anchors", ANCHOR_FIELDS),
            ("content_vetoes", VETO_FIELDS),
        ):
            if not isinstance(asset[name], list):
                raise ScoringError(f"{field}.{name} must be a list")
            regions = [
                _validate_region(item, fields, f"{field}.{name}[{index}]", duration)
                for index, item in enumerate(asset[name])
            ]
            regions.sort(key=lambda item: (item["start_seconds"], item["end_seconds"]))
            for previous, current in zip(regions, regions[1:]):
                if previous["end_seconds"] > current["start_seconds"]:
                    raise ScoringError(f"{field}.{name} contains overlapping regions")
            lists[name] = regions
        all_regions = [
            (region["start_seconds"], region["end_seconds"], name)
            for name in ("full_breaks", "presence_anchors", "content_vetoes")
            for region in lists[name]
        ]
        all_regions.sort()
        for index, left in enumerate(all_regions):
            for right in all_regions[index + 1 :]:
                if right[0] >= left[1]:
                    break
                if min(left[1], right[1]) > max(left[0], right[0]):
                    raise ScoringError(f"{field} has conflicting overlapping partial labels")
        normalized_assets.append(
            {
                "audio_fingerprint": fingerprint,
                "content_vetoes": lists["content_vetoes"],
                "duration_seconds": duration,
                "episode_id": episode_id,
                "full_breaks": lists["full_breaks"],
                "presence_anchors": lists["presence_anchors"],
                "show_name": show_name,
            }
        )
    normalized_assets.sort(key=lambda item: item["episode_id"])

    summary_fields = frozenset(
        {
            "assets",
            "boundary_reviews",
            "content_vetoes",
            "duplicate_full_break_groups",
            "full_break_assets",
            "full_breaks",
            "labeled_regions",
            "ledger_rows",
            "presence_anchors",
            "review_status_counts",
            "tight_evidence_attached",
        }
    )
    summary = _require_fields(document["summary"], summary_fields, "evaluation.summary")
    normalized_summary = {
        key: _integer(summary[key], f"evaluation.summary.{key}")
        for key in summary_fields - {"review_status_counts"}
    }
    review_counts = _require_fields(
        summary["review_status_counts"],
        frozenset({"approved", "rebounded", "rejected"}),
        "evaluation.summary.review_status_counts",
    )
    normalized_summary["review_status_counts"] = {
        key: _integer(review_counts[key], f"review_status_counts.{key}")
        for key in ("approved", "rebounded", "rejected")
    }
    derived = {
        "assets": len(normalized_assets),
        "content_vetoes": sum(len(asset["content_vetoes"]) for asset in normalized_assets),
        "full_break_assets": sum(bool(asset["full_breaks"]) for asset in normalized_assets),
        "full_breaks": sum(len(asset["full_breaks"]) for asset in normalized_assets),
        "presence_anchors": sum(len(asset["presence_anchors"]) for asset in normalized_assets),
    }
    derived["labeled_regions"] = (
        derived["content_vetoes"] + derived["full_breaks"] + derived["presence_anchors"]
    )
    for key, value in derived.items():
        if normalized_summary[key] != value:
            raise ScoringError(f"evaluation.summary.{key} does not match assets")
    return {
        "artifact_kind": document["artifact_kind"],
        "assets": normalized_assets,
        "label_semantics": LABEL_SEMANTICS,
        "schema_version": 1,
        "sources": normalized_sources,
        "summary": normalized_summary,
        "_sha256": digest,
    }


def load_policy(path: pathlib.Path, *, expected_evaluation_sha256: str) -> dict:
    data = _read_regular_bytes(path, "scoring policy")
    digest = hashlib.sha256(data).hexdigest()
    _validate_content_address(
        path,
        digest,
        "scoring policy",
        filename_prefix="earaudit-partial-silver-baseline-policy-",
    )
    policy = _require_fields(_parse_json(data, "scoring policy"), POLICY_FIELDS, "scoring policy")
    if _integer(policy["schema_version"], "scoring policy.schema_version", minimum=1) != 1:
        raise ScoringError("scoring policy.schema_version must equal 1")
    if policy["artifact_kind"] != "unchanged_production_partial_silver_scoring_policy":
        raise ScoringError("scoring policy.artifact_kind is invalid")
    if policy["evaluation_sha256"] != expected_evaluation_sha256:
        raise ScoringError("scoring policy evaluation_sha256 does not bind this evaluation")
    _validate_sha256(policy["evaluation_sha256"], "scoring policy.evaluation_sha256")
    if _integer(policy["required_run_count"], "scoring policy.required_run_count") != 3:
        raise ScoringError("scoring policy.required_run_count must equal 3")
    if _validate_exact_contract(
        policy["run_ids"], REQUIRED_RUN_IDS, "scoring policy.run_ids"
    ) != REQUIRED_RUN_IDS:
        raise ScoringError("scoring policy.run_ids must predeclare the three baseline runs")
    duration_tolerance = _finite_number(
        policy["asset_duration_tolerance_seconds"],
        "scoring policy.asset_duration_tolerance_seconds",
        minimum=0,
    )
    if duration_tolerance != 1.0:
        raise ScoringError("scoring policy.asset_duration_tolerance_seconds drifted")
    if _validate_exact_contract(
        policy["eligible_decision_states"],
        ELIGIBLE_DECISION_STATES,
        "scoring policy.eligible_decision_states",
    ) != ELIGIBLE_DECISION_STATES:
        raise ScoringError("scoring policy.eligible_decision_states drifted")
    _validate_exact_contract(policy["matching"], MATCHING_POLICY, "scoring policy.matching")
    _validate_exact_contract(
        policy["partial_label_semantics"],
        PARTIAL_LABEL_POLICY,
        "scoring policy.partial_label_semantics",
    )
    _validate_exact_contract(
        policy["production_capture"],
        PRODUCTION_CAPTURE_POLICY,
        "scoring policy.production_capture",
    )
    _validate_exact_contract(
        policy["music_edge_cohort"], MUSIC_POLICY, "scoring policy.music_edge_cohort"
    )
    _validate_exact_contract(
        policy["three_run_aggregation"],
        THREE_RUN_AGGREGATION_POLICY,
        "scoring policy.three_run_aggregation",
    )
    coverage = _finite_number(
        policy["full_break_coverage_threshold"],
        "scoring policy.full_break_coverage_threshold",
        minimum=0,
        maximum=1,
    )
    if coverage != 0.9:
        raise ScoringError("scoring policy.full_break_coverage_threshold drifted")
    _validate_exact_contract(policy["show_split"], SHOW_SPLIT_POLICY, "scoring policy.show_split")
    normalized = dict(policy)
    normalized["asset_duration_tolerance_seconds"] = duration_tolerance
    normalized["full_break_coverage_threshold"] = coverage
    normalized["_sha256"] = digest
    return normalized


def _validate_exact_contract(value: object, expected: object, field: str) -> object:
    """Validate exact frozen values without Python's bool/int equivalence."""
    if isinstance(expected, bool):
        if not isinstance(value, bool) or value is not expected:
            raise ScoringError(f"{field} drifted from the frozen production default")
        return value
    if isinstance(expected, int):
        if isinstance(value, bool) or not isinstance(value, int) or value != expected:
            raise ScoringError(f"{field} drifted from the frozen production default")
        return value
    if isinstance(expected, float):
        number = _finite_number(value, field)
        if number != expected:
            raise ScoringError(f"{field} drifted from the frozen production default")
        return number
    if isinstance(expected, str):
        text = _canonical_text(value, field)
        if text != expected:
            raise ScoringError(f"{field} drifted from the frozen production default")
        return text
    if isinstance(expected, dict):
        fields = frozenset(expected)
        document = _require_fields(value, fields, field)
        return {
            key: _validate_exact_contract(document[key], expected[key], f"{field}.{key}")
            for key in sorted(expected)
        }
    if isinstance(expected, list):
        if not isinstance(value, list) or len(value) != len(expected):
            raise ScoringError(f"{field} drifted from the frozen production default")
        return [
            _validate_exact_contract(item, expected[index], f"{field}[{index}]")
            for index, item in enumerate(value)
        ]
    raise ScoringError(f"internal unsupported frozen contract at {field}")


def _normalize_production_config(value: object) -> dict:
    config = _require_fields(value, PRODUCTION_CONFIG_FIELDS, "raw run.production_config")
    exact_scalars = {
        key: value
        for key, value in PRODUCTION_CAPTURE_POLICY.items()
        if key != "source_identity"
    }
    normalized = {
        key: _validate_exact_contract(
            config[key], expected, f"raw run.production_config.{key}"
        )
        for key, expected in exact_scalars.items()
    }
    normalized["pipeline_versions"] = _validate_exact_contract(
        _require_fields(
            config["pipeline_versions"],
            PIPELINE_VERSION_FIELDS,
            "raw run.production_config.pipeline_versions",
        ),
        EXPECTED_PIPELINE_VERSIONS,
        "raw run.production_config.pipeline_versions",
    )
    normalized["ad_detection_defaults"] = _validate_exact_contract(
        _require_fields(
            config["ad_detection_defaults"],
            AD_DETECTION_DEFAULT_FIELDS,
            "raw run.production_config.ad_detection_defaults",
        ),
        EXPECTED_AD_DETECTION_DEFAULTS,
        "raw run.production_config.ad_detection_defaults",
    )
    normalized["narrowing_defaults"] = _validate_exact_contract(
        _require_fields(
            config["narrowing_defaults"],
            NARROWING_DEFAULT_FIELDS,
            "raw run.production_config.narrowing_defaults",
        ),
        EXPECTED_NARROWING_DEFAULTS,
        "raw run.production_config.narrowing_defaults",
    )
    return {key: normalized[key] for key in sorted(normalized)}


def _normalize_runtime(value: object) -> dict:
    runtime = _require_fields(value, RUNTIME_FIELDS, "raw run.runtime")
    architecture = _canonical_text(runtime["architecture"], "raw run.runtime.architecture")
    if architecture not in {"arm64", "x86_64"}:
        raise ScoringError("raw run.runtime.architecture is not a supported live architecture")
    availability = _canonical_text(
        runtime["foundation_models_availability"],
        "raw run.runtime.foundation_models_availability",
    )
    if availability != "available":
        raise ScoringError("raw run did not observe an available Foundation Models runtime")
    return {
        "architecture": architecture,
        "executable_identity": _canonical_text(
            runtime["executable_identity"], "raw run.runtime.executable_identity"
        ),
        "foundation_models_availability": availability,
        "foundation_models_context_size": _integer(
            runtime["foundation_models_context_size"],
            "raw run.runtime.foundation_models_context_size",
        ),
        "locale_identifier": _canonical_text(
            runtime["locale_identifier"], "raw run.runtime.locale_identifier"
        ),
        "os_version": _canonical_text(runtime["os_version"], "raw run.runtime.os_version"),
        "xcode_version_actual": _canonical_text(
            runtime["xcode_version_actual"], "raw run.runtime.xcode_version_actual"
        ),
    }


def _normalize_prediction(value: object, field: str, duration: float) -> dict:
    prediction = _require_fields(value, PREDICTION_FIELDS, field)
    start = _finite_number(prediction["start_seconds"], f"{field}.start_seconds", minimum=0)
    end = _finite_number(prediction["end_seconds"], f"{field}.end_seconds", minimum=0)
    if end <= start or end > duration:
        raise ScoringError(f"{field} must be a positive interval within the episode")
    state = _canonical_text(prediction["decision_state"], f"{field}.decision_state")
    if state not in KNOWN_DECISION_STATES:
        raise ScoringError(f"{field}.decision_state is unknown")
    was_skipped = prediction["was_skipped"]
    if not isinstance(was_skipped, bool):
        raise ScoringError(f"{field}.was_skipped must be a boolean")
    return {
        "confidence": _finite_number(
            prediction["confidence"], f"{field}.confidence", minimum=0, maximum=1
        ),
        "decision_state": state,
        "end_seconds": end,
        "start_seconds": start,
        "was_skipped": was_skipped,
    }


def _normalize_music_feature(value: object, field: str, duration: float) -> dict:
    feature = _require_fields(value, MUSIC_FEATURE_FIELDS, field)
    start = _finite_number(feature["start_seconds"], f"{field}.start_seconds", minimum=0)
    end = _finite_number(feature["end_seconds"], f"{field}.end_seconds", minimum=0)
    if end <= start or end > duration:
        raise ScoringError(f"{field} must be a positive interval within the episode")
    level = _canonical_text(feature["music_bed_level"], f"{field}.music_bed_level")
    if level not in MUSIC_BED_LEVELS:
        raise ScoringError(f"{field}.music_bed_level is unknown")
    normalized = {
        "end_seconds": end,
        "music_bed_level": level,
        "start_seconds": start,
    }
    for name in (
        "music_probability",
        "music_bed_change_score",
        "music_bed_onset_score",
        "music_bed_offset_score",
    ):
        normalized[name] = _finite_number(
            feature[name], f"{field}.{name}", minimum=0, maximum=1
        )
    return normalized


def load_raw_run(path: pathlib.Path, *, expected_evaluation_sha256: str) -> dict:
    data = _read_regular_bytes(path, "raw production run")
    raw = _require_fields(_parse_json(data, "raw production run"), RAW_FIELDS, "raw production run")
    if _integer(raw["schema_version"], "raw run.schema_version", minimum=1) != 1:
        raise ScoringError("raw run.schema_version must equal 1")
    if raw["artifact_kind"] != "unchanged_production_partial_silver_raw":
        raise ScoringError("raw run.artifact_kind is invalid")
    evaluation_sha = _validate_sha256(raw["evaluation_sha256"], "raw run.evaluation_sha256")
    if evaluation_sha != expected_evaluation_sha256:
        raise ScoringError("raw run.evaluation_sha256 does not bind this evaluation")
    run_id = _canonical_text(raw["run_id"], "raw run.run_id")
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ScoringError("raw run.run_id is not a canonical identifier")
    expected_filename = f"playhead-partial-silver-baseline-{run_id}.json"
    if pathlib.Path(path).name != expected_filename:
        raise ScoringError(
            "raw production run filename must exactly match its run_id: "
            f"expected {expected_filename}"
        )
    revision = _canonical_text(raw["source_revision"], "raw run.source_revision")
    if not REVISION_PATTERN.fullmatch(revision):
        raise ScoringError("raw run.source_revision must be a full lowercase source revision")
    captured = _validate_timestamp(raw["captured_at_utc"], "raw run.captured_at_utc")
    production_config = _normalize_production_config(raw["production_config"])
    runtime = _normalize_runtime(raw["runtime"])
    if (
        not isinstance(raw["episodes"], list)
        or len(raw["episodes"]) != EXPECTED_ASSET_COUNT
    ):
        raise ScoringError(
            f"raw run.episodes must contain exactly {EXPECTED_ASSET_COUNT} episodes"
        )
    episodes: list[dict] = []
    seen_episodes: set[str] = set()
    for episode_index, raw_episode in enumerate(raw["episodes"]):
        field = f"raw run.episodes[{episode_index}]"
        episode = _require_fields(raw_episode, RAW_EPISODE_FIELDS, field)
        episode_id = _validate_bare_identifier(episode["episode_id"], f"{field}.episode_id")
        if episode_id in seen_episodes:
            raise ScoringError(f"raw run has duplicate episode_id {episode_id}")
        seen_episodes.add(episode_id)
        duration = _finite_number(episode["duration_seconds"], f"{field}.duration_seconds", minimum=0)
        if duration <= 0:
            raise ScoringError(f"{field}.duration_seconds must be positive")
        if (
            not isinstance(episode["predictions"], list)
            or len(episode["predictions"]) > MAX_PREDICTIONS_PER_EPISODE
        ):
            raise ScoringError(
                f"{field}.predictions must be a list of at most "
                f"{MAX_PREDICTIONS_PER_EPISODE} entries"
            )
        predictions = [
            _normalize_prediction(value, f"{field}.predictions[{index}]", duration)
            for index, value in enumerate(episode["predictions"])
        ]
        predictions.sort(
            key=lambda item: (
                item["start_seconds"],
                item["end_seconds"],
                item["decision_state"],
                item["was_skipped"],
                item["confidence"],
            )
        )
        if (
            not isinstance(episode["music_features"], list)
            or len(episode["music_features"]) > MAX_MUSIC_FEATURES_PER_EPISODE
        ):
            raise ScoringError(
                f"{field}.music_features must be a list of at most "
                f"{MAX_MUSIC_FEATURES_PER_EPISODE} entries"
            )
        features = [
            _normalize_music_feature(value, f"{field}.music_features[{index}]", duration)
            for index, value in enumerate(episode["music_features"])
        ]
        features.sort(key=lambda item: (item["start_seconds"], item["end_seconds"]))
        feature_intervals = [(item["start_seconds"], item["end_seconds"]) for item in features]
        if len(feature_intervals) != len(set(feature_intervals)):
            raise ScoringError(f"{field}.music_features contains duplicate intervals")
        episodes.append(
            {
                "audio_fingerprint": _validate_fingerprint(
                    episode["audio_fingerprint"], f"{field}.audio_fingerprint"
                ),
                "duration_seconds": duration,
                "episode_id": episode_id,
                "music_features": features,
                "predictions": predictions,
                "show_name": _canonical_text(episode["show_name"], f"{field}.show_name"),
                "transcript_sha256": _validate_sha256(
                    episode["transcript_sha256"], f"{field}.transcript_sha256"
                ),
            }
        )
    episodes.sort(key=lambda item: item["episode_id"])
    return {
        "artifact_kind": raw["artifact_kind"],
        "captured_at_utc": captured,
        "episodes": episodes,
        "evaluation_sha256": evaluation_sha,
        "production_config": production_config,
        "run_id": run_id,
        "runtime": runtime,
        "schema_version": 1,
        "source_revision": revision,
        "_bytes": data,
        "_sha256": hashlib.sha256(data).hexdigest(),
    }


def _decimal(value: float) -> Decimal:
    return Decimal(str(value))


def _intersection(left: tuple[float, float], right: tuple[float, float]) -> float:
    return max(0.0, min(left[1], right[1]) - max(left[0], right[0]))


def maximum_overlap_matching(
    references: Sequence[tuple[float, float]],
    predictions: Sequence[tuple[float, float]],
) -> list[Match]:
    """Maximum-cardinality, overlap, boundary accuracy, then stable matching."""
    for name, intervals in (("reference", references), ("prediction", predictions)):
        for index, interval in enumerate(intervals):
            if (
                not isinstance(interval, (tuple, list))
                or len(interval) != 2
                or isinstance(interval[0], bool)
                or isinstance(interval[1], bool)
                or not isinstance(interval[0], (int, float))
                or not isinstance(interval[1], (int, float))
                or not math.isfinite(float(interval[0]))
                or not math.isfinite(float(interval[1]))
                or interval[1] <= interval[0]
            ):
                raise ScoringError(f"{name} interval {index} is invalid")
    reference_count = len(references)
    prediction_count = len(predictions)
    source = 0
    first_reference = 1
    first_prediction = first_reference + reference_count
    sink = first_prediction + prediction_count
    node_count = sink + 1
    graph: list[list[dict]] = [[] for _ in range(node_count)]

    def add_edge(
        left: int,
        right: int,
        capacity: int,
        cost: tuple[Decimal, Decimal, int],
        *,
        pair: tuple[int, int] | None = None,
    ) -> None:
        forward = {
            "to": right,
            "reverse": len(graph[right]),
            "capacity": capacity,
            "cost": cost,
            "pair": pair,
        }
        reverse = {
            "to": left,
            "reverse": len(graph[left]),
            "capacity": 0,
            "cost": (-cost[0], -cost[1], -cost[2]),
            "pair": None,
        }
        graph[left].append(forward)
        graph[right].append(reverse)

    for reference_index in range(reference_count):
        add_edge(
            source,
            first_reference + reference_index,
            1,
            (Decimal(0), Decimal(0), 0),
        )
    eligible_pairs: list[tuple[int, int, Decimal, Decimal]] = []
    for reference_index, reference in enumerate(references):
        for prediction_index, prediction_interval in enumerate(predictions):
            overlap = min(_decimal(reference[1]), _decimal(prediction_interval[1])) - max(
                _decimal(reference[0]), _decimal(prediction_interval[0])
            )
            if overlap > 0:
                boundary_error = abs(
                    _decimal(reference[0]) - _decimal(prediction_interval[0])
                ) + abs(_decimal(reference[1]) - _decimal(prediction_interval[1]))
                eligible_pairs.append(
                    (reference_index, prediction_index, overlap, boundary_error)
                )
    pair_count = len(eligible_pairs)
    for rank, (
        reference_index,
        prediction_index,
        overlap,
        boundary_error,
    ) in enumerate(eligible_pairs):
        # With cardinality and total overlap equal, binary positional weights
        # make inclusion of the earliest pair dominate every later pair.
        lexicographic_weight = 1 << (pair_count - rank - 1)
        add_edge(
            first_reference + reference_index,
            first_prediction + prediction_index,
            1,
            (-overlap, boundary_error, -lexicographic_weight),
            pair=(reference_index, prediction_index),
        )
    for prediction_index in range(prediction_count):
        add_edge(
            first_prediction + prediction_index,
            sink,
            1,
            (Decimal(0), Decimal(0), 0),
        )

    while True:
        distances: list[tuple[Decimal, Decimal, int] | None] = [None] * node_count
        previous: list[tuple[int, int] | None] = [None] * node_count
        distances[source] = (Decimal(0), Decimal(0), 0)
        for _ in range(node_count - 1):
            changed = False
            for node in range(node_count):
                if distances[node] is None:
                    continue
                for edge_index, edge in enumerate(graph[node]):
                    if not edge["capacity"]:
                        continue
                    base_cost = distances[node]
                    edge_cost = edge["cost"]
                    candidate = (
                        base_cost[0] + edge_cost[0],
                        base_cost[1] + edge_cost[1],
                        base_cost[2] + edge_cost[2],
                    )
                    destination = edge["to"]
                    if distances[destination] is None or candidate < distances[destination]:
                        distances[destination] = candidate
                        previous[destination] = (node, edge_index)
                        changed = True
            if not changed:
                break
        if previous[sink] is None:
            break
        node = sink
        while node != source:
            predecessor = previous[node]
            if predecessor is None:
                raise ScoringError("internal matching path is incomplete")
            prior_node, edge_index = predecessor
            edge = graph[prior_node][edge_index]
            edge["capacity"] -= 1
            graph[node][edge["reverse"]]["capacity"] += 1
            node = prior_node

    result: list[Match] = []
    for reference_index in range(reference_count):
        node = first_reference + reference_index
        for edge in graph[node]:
            if edge["pair"] is not None and edge["capacity"] == 0:
                left, right = edge["pair"]
                result.append(
                    Match(left, right, _intersection(references[left], predictions[right]))
                )
    result.sort(key=lambda match: (match.reference_index, match.prediction_index))
    return result


def _rate(numerator: int | float, denominator: int | float) -> float | None:
    return float(numerator) / float(denominator) if denominator else None


def _stats(values: Sequence[float]) -> dict:
    if not values:
        return {
            "count": 0,
            "maximum": None,
            "mean": None,
            "median": None,
            "minimum": None,
            "p90": None,
        }
    ordered = sorted(float(value) for value in values)
    p90_index = max(0, math.ceil(len(ordered) * 0.9) - 1)
    return {
        "count": len(ordered),
        "maximum": ordered[-1],
        "mean": math.fsum(ordered) / len(ordered),
        "median": float(statistics.median(ordered)),
        "minimum": ordered[0],
        "p90": ordered[p90_index],
    }


def _aggregate(values: Sequence[float | None]) -> dict:
    present = sorted(float(value) for value in values if value is not None)
    if not present:
        return {
            "maximum": None,
            "median": None,
            "minimum": None,
            "runs_with_value": 0,
            "total_runs": len(values),
        }
    return {
        "maximum": present[-1],
        "median": float(statistics.median(present)),
        "minimum": present[0],
        "runs_with_value": len(present),
        "total_runs": len(values),
    }


def _union_duration(intervals: Sequence[tuple[float, float]]) -> float:
    if not intervals:
        return 0.0
    ordered = sorted(intervals)
    total = 0.0
    start, end = ordered[0]
    for next_start, next_end in ordered[1:]:
        if next_start <= end:
            end = max(end, next_end)
        else:
            total += end - start
            start, end = next_start, next_end
    return total + end - start


def _show_assignment(show_name: str, split_policy: dict) -> tuple[str, int]:
    material = f"{split_policy['salt']}\0{show_name}".encode("utf-8")
    bucket = int.from_bytes(hashlib.sha256(material).digest()[:8], "big") % split_policy["bucket_count"]
    cohort = "holdout" if bucket < split_policy["holdout_bucket_count"] else "development"
    return cohort, bucket


def _reference_id(episode_id: str, kind: str, index: int) -> str:
    return f"{episode_id}#{kind}:{index + 1}"


def _music_cohort(
    features: list[dict],
    reference: dict,
    duration: float,
    policy: dict,
) -> tuple[str, dict]:
    edge_policy = policy["music_edge_cohort"]
    band = edge_policy["band_seconds"]
    start = reference["start_seconds"]
    end = reference["end_seconds"]
    ranges = {
        "start_outside": (start - band, start),
        "start_inside": (start, start + band),
        "end_inside": (end - band, end),
        "end_outside": (end, end + band),
    }
    geometry_sufficient = all(
        lower >= 0 and upper <= duration and upper > lower
        for lower, upper in ranges.values()
    )
    summaries: dict[str, dict] = {}
    sufficient = True
    all_pass = True
    present_levels = set(edge_policy["music_bed_present_levels"])
    for name, (lower, upper) in ranges.items():
        assigned = [
            feature
            for feature in features
            if lower <= (feature["start_seconds"] + feature["end_seconds"]) / 2.0 < upper
        ]
        present = sum(feature["music_bed_level"] in present_levels for feature in assigned)
        fraction = _rate(present, len(assigned))
        band_sufficient = bool(
            geometry_sufficient
            and len(assigned) >= edge_policy["minimum_windows_per_band"]
        )
        passes = bool(
            band_sufficient
            and fraction is not None
            and fraction >= edge_policy["minimum_music_window_fraction"]
        )
        sufficient = sufficient and band_sufficient
        all_pass = all_pass and passes
        summaries[name] = {
            "inside_episode": lower >= 0 and upper <= duration,
            "music_fraction": fraction,
            "music_windows": present,
            "passes": passes,
            "windows": len(assigned),
        }
    if not sufficient:
        cohort = "insufficient_context"
    elif all_pass:
        cohort = "continuous_music_bed"
    else:
        cohort = "nonmusic"
    return cohort, summaries


def _eligible_predictions(episode: dict, policy: dict) -> list[dict]:
    eligible = set(policy["eligible_decision_states"])
    return [prediction for prediction in episode["predictions"] if prediction["decision_state"] in eligible]


def _score_run(
    evaluation: dict,
    raw: dict,
    policy: dict,
    *,
    allowed_episode_ids: set[str] | None = None,
) -> dict:
    raw_by_episode = {episode["episode_id"]: episode for episode in raw["episodes"]}
    assets = [
        asset
        for asset in evaluation["assets"]
        if allowed_episode_ids is None or asset["episode_id"] in allowed_episode_ids
    ]
    total_predictions = 0
    eligible_count = 0
    eligible_skipped = 0
    excluded_count = 0
    excluded_skipped = 0
    full_reference_count = 0
    full_detected = 0
    near_complete = 0
    anchor_reference_count = 0
    anchor_detected = 0
    veto_reference_count = 0
    vetoes_hit: set[tuple[str, int]] = set()
    colliding_predictions: set[tuple[str, int]] = set()
    collision_intervals: dict[str, list[tuple[float, float]]] = {}
    matched_positive_predictions = 0
    positive_veto_conflicts = 0
    duplicate_or_fragment_predictions = 0
    content_only_predictions = 0
    unknown_predictions: list[dict] = []
    classifiable_predictions = 0
    full_details: list[dict] = []
    anchor_details: list[dict] = []
    collision_details: list[dict] = []
    start_errors: list[float] = []
    end_errors: list[float] = []
    matched_coverages: list[float] = []
    union_coverages: list[float] = []
    overreach = 0.0
    undershoot = 0.0
    undetected_break_seconds = 0.0
    music_accumulators: dict[str, dict] = {
        name: {
            "absolute_end_errors": [],
            "absolute_start_errors": [],
            "boundary_overreach": 0.0,
            "boundary_undershoot": 0.0,
            "matched_coverages": [],
            "union_coverages": [],
            "detected": 0,
            "end_errors": [],
            "near_complete": 0,
            "reference_breaks": 0,
            "start_errors": [],
        }
        for name in (
            "continuous_music_bed",
            "nonmusic",
            "insufficient_context",
        )
    }

    for asset in assets:
        episode_id = asset["episode_id"]
        episode = raw_by_episode[episode_id]
        predictions = episode["predictions"]
        eligible = _eligible_predictions(episode, policy)
        total_predictions += len(predictions)
        eligible_count += len(eligible)
        eligible_skipped += sum(prediction["was_skipped"] for prediction in eligible)
        excluded = [prediction for prediction in predictions if prediction not in eligible]
        excluded_count += len(excluded)
        excluded_skipped += sum(prediction["was_skipped"] for prediction in excluded)
        prediction_intervals = [
            (prediction["start_seconds"], prediction["end_seconds"]) for prediction in eligible
        ]

        full_references = asset["full_breaks"]
        full_reference_count += len(full_references)
        full_intervals = [
            (reference["start_seconds"], reference["end_seconds"])
            for reference in full_references
        ]
        anchor_references = asset["presence_anchors"]
        anchor_reference_count += len(anchor_references)
        anchor_intervals = [
            (reference["start_seconds"], reference["end_seconds"])
            for reference in anchor_references
        ]
        veto_references = asset["content_vetoes"]
        veto_reference_count += len(veto_references)
        veto_intervals = [
            (reference["start_seconds"], reference["end_seconds"])
            for reference in veto_references
        ]
        positive_intervals = full_intervals + anchor_intervals
        detection_positive_matches = maximum_overlap_matching(
            positive_intervals, prediction_intervals
        )
        full_matches = [
            match
            for match in detection_positive_matches
            if match.reference_index < len(full_intervals)
        ]
        anchor_matches = [
            Match(
                match.reference_index - len(full_intervals),
                match.prediction_index,
                match.intersection_seconds,
            )
            for match in detection_positive_matches
            if match.reference_index >= len(full_intervals)
        ]
        precision_prediction_indexes = [
            prediction_index
            for prediction_index, prediction_interval in enumerate(prediction_intervals)
            if not any(
                _intersection(prediction_interval, veto_interval) > 0
                for veto_interval in veto_intervals
            )
        ]
        precision_matches = maximum_overlap_matching(
            positive_intervals,
            [prediction_intervals[index] for index in precision_prediction_indexes],
        )
        precision_positive_predictions = {
            precision_prediction_indexes[match.prediction_index] for match in precision_matches
        }
        full_by_reference = {match.reference_index: match for match in full_matches}
        full_detected += len(full_matches)
        for reference_index, reference in enumerate(full_references):
            reference_id = _reference_id(episode_id, "full_break", reference_index)
            cohort, bands = _music_cohort(
                episode["music_features"],
                reference,
                episode["duration_seconds"],
                policy,
            )
            music = music_accumulators[cohort]
            music["reference_breaks"] += 1
            covered_intervals = [
                (
                    max(reference["start_seconds"], prediction["start_seconds"]),
                    min(reference["end_seconds"], prediction["end_seconds"]),
                )
                for prediction in eligible
                if min(reference["end_seconds"], prediction["end_seconds"])
                > max(reference["start_seconds"], prediction["start_seconds"])
            ]
            union_intersection = _union_duration(covered_intervals)
            reference_duration = reference["end_seconds"] - reference["start_seconds"]
            union_coverage = union_intersection / reference_duration
            match = full_by_reference.get(reference_index)
            matched_coverage = (
                match.intersection_seconds / reference_duration if match is not None else 0.0
            )
            complete = matched_coverage >= policy["full_break_coverage_threshold"]
            near_complete += int(complete)
            music["near_complete"] += int(complete)
            music["matched_coverages"].append(matched_coverage)
            music["union_coverages"].append(union_coverage)
            matched_coverages.append(matched_coverage)
            union_coverages.append(union_coverage)
            detail = {
                "detected": reference_index in full_by_reference,
                "episode_id": episode_id,
                "music_edge_bands": bands,
                "music_edge_cohort": cohort,
                "reference_end_seconds": reference["end_seconds"],
                "reference_id": reference_id,
                "reference_start_seconds": reference["start_seconds"],
                "show_name": asset["show_name"],
                "union_intersection_seconds": union_intersection,
                "union_reference_coverage": union_coverage,
                "matched_reference_coverage": matched_coverage,
            }
            if match is None:
                undetected_break_seconds += reference["end_seconds"] - reference["start_seconds"]
                detail.update(
                    {
                        "end_error_seconds": None,
                        "intersection_seconds": 0.0,
                        "near_complete": complete,
                        "prediction_end_seconds": None,
                        "prediction_start_seconds": None,
                        "start_error_seconds": None,
                    }
                )
            else:
                prediction_value = eligible[match.prediction_index]
                start_error = prediction_value["start_seconds"] - reference["start_seconds"]
                end_error = prediction_value["end_seconds"] - reference["end_seconds"]
                music["detected"] += 1
                music["absolute_start_errors"].append(abs(start_error))
                music["absolute_end_errors"].append(abs(end_error))
                music["start_errors"].append(start_error)
                music["end_errors"].append(end_error)
                start_errors.append(start_error)
                end_errors.append(end_error)
                match_overreach = max(-start_error, 0.0) + max(end_error, 0.0)
                match_undershoot = max(start_error, 0.0) + max(-end_error, 0.0)
                music["boundary_overreach"] += match_overreach
                music["boundary_undershoot"] += match_undershoot
                overreach += match_overreach
                undershoot += match_undershoot
                detail.update(
                    {
                        "end_error_seconds": end_error,
                        "intersection_seconds": match.intersection_seconds,
                        "near_complete": complete,
                        "prediction_end_seconds": prediction_value["end_seconds"],
                        "prediction_start_seconds": prediction_value["start_seconds"],
                        "start_error_seconds": start_error,
                    }
                )
            full_details.append(detail)

        anchor_by_reference = {match.reference_index: match for match in anchor_matches}
        anchor_detected += len(anchor_matches)
        for reference_index, reference in enumerate(anchor_references):
            match = anchor_by_reference.get(reference_index)
            anchor_details.append(
                {
                    "detected": match is not None,
                    "episode_id": episode_id,
                    "intersection_seconds": match.intersection_seconds if match else 0.0,
                    "reference_end_seconds": reference["end_seconds"],
                    "reference_id": _reference_id(episode_id, "presence_anchor", reference_index),
                    "reference_start_seconds": reference["start_seconds"],
                    "show_name": asset["show_name"],
                }
            )

        for prediction_index, (prediction_value, prediction_interval) in enumerate(
            zip(eligible, prediction_intervals)
        ):
            positive_overlaps = [
                _intersection(prediction_interval, reference) for reference in positive_intervals
            ]
            veto_overlaps = [
                _intersection(prediction_interval, reference) for reference in veto_intervals
            ]
            has_positive = any(overlap > 0 for overlap in positive_overlaps)
            hit_veto_indexes = [index for index, overlap in enumerate(veto_overlaps) if overlap > 0]
            has_veto = bool(hit_veto_indexes)
            matched_positive = prediction_index in precision_positive_predictions
            if not has_positive and not has_veto:
                unknown_predictions.append(
                    {
                        "confidence": prediction_value["confidence"],
                        "decision_state": prediction_value["decision_state"],
                        "end_seconds": prediction_value["end_seconds"],
                        "episode_id": episode_id,
                        "start_seconds": prediction_value["start_seconds"],
                        "was_skipped": prediction_value["was_skipped"],
                    }
                )
                continue
            classifiable_predictions += 1
            if has_positive and has_veto:
                positive_veto_conflicts += 1
            elif matched_positive:
                matched_positive_predictions += 1
            elif has_veto:
                content_only_predictions += 1
            elif has_positive:
                duplicate_or_fragment_predictions += 1
            if has_veto:
                colliding_predictions.add((episode_id, prediction_index))
                for veto_index in hit_veto_indexes:
                    vetoes_hit.add((episode_id, veto_index))
                    veto = veto_intervals[veto_index]
                    overlap_start = max(prediction_interval[0], veto[0])
                    overlap_end = min(prediction_interval[1], veto[1])
                    collision_intervals.setdefault(episode_id, []).append(
                        (overlap_start, overlap_end)
                    )
                    collision_details.append(
                        {
                            "episode_id": episode_id,
                            "overlap_seconds": overlap_end - overlap_start,
                            "prediction_end_seconds": prediction_interval[1],
                            "prediction_start_seconds": prediction_interval[0],
                            "veto_end_seconds": veto[1],
                            "veto_id": _reference_id(episode_id, "content_veto", veto_index),
                            "veto_start_seconds": veto[0],
                        }
                    )

    music_metrics: dict[str, dict] = {}
    for cohort, accumulator in music_accumulators.items():
        references = accumulator["reference_breaks"]
        detected = accumulator["detected"]
        music_metrics[cohort] = {
            "absolute_end_error_seconds": _stats(accumulator["absolute_end_errors"]),
            "absolute_start_error_seconds": _stats(accumulator["absolute_start_errors"]),
            "boundary_overreach_seconds": accumulator["boundary_overreach"],
            "boundary_undershoot_seconds": accumulator["boundary_undershoot"],
            "detected": detected,
            "end_error_seconds": _stats(accumulator["end_errors"]),
            "near_complete": accumulator["near_complete"],
            "near_complete_rate": _rate(accumulator["near_complete"], references),
            "recall": _rate(detected, references),
            "reference_breaks": references,
            "matched_reference_coverage": _stats(accumulator["matched_coverages"]),
            "union_reference_coverage": _stats(accumulator["union_coverages"]),
            "start_error_seconds": _stats(accumulator["start_errors"]),
        }

    absolute_start = [abs(value) for value in start_errors]
    absolute_end = [abs(value) for value in end_errors]
    return {
        "content_veto_collisions": {
            "colliding_pairs": len(collision_details),
            "colliding_predictions": len(colliding_predictions),
            "details": sorted(
                collision_details,
                key=lambda item: (
                    item["episode_id"], item["prediction_start_seconds"], item["veto_start_seconds"]
                ),
            ),
            "reference_vetoes": veto_reference_count,
            "unique_overlap_seconds": math.fsum(
                _union_duration(intervals) for intervals in collision_intervals.values()
            ),
            "vetoes_hit": len(vetoes_hit),
        },
        "detection": {
            "full_breaks": {
                "details": sorted(full_details, key=lambda item: item["reference_id"]),
                "detected": full_detected,
                "missed": full_reference_count - full_detected,
                "near_complete": near_complete,
                "near_complete_rate": _rate(near_complete, full_reference_count),
                "recall": _rate(full_detected, full_reference_count),
                "references": full_reference_count,
            },
            "presence_anchors": {
                "details": sorted(anchor_details, key=lambda item: item["reference_id"]),
                "detected": anchor_detected,
                "missed": anchor_reference_count - anchor_detected,
                "recall": _rate(anchor_detected, anchor_reference_count),
                "references": anchor_reference_count,
            },
        },
        "localization": {
            "absolute_end_error_seconds": _stats(absolute_end),
            "absolute_start_error_seconds": _stats(absolute_start),
            "end_error_seconds": _stats(end_errors),
            "matched_boundary_overreach_seconds": overreach,
            "matched_boundary_undershoot_seconds": undershoot,
            "matched_reference_coverage": _stats(matched_coverages),
            "union_reference_coverage": _stats(union_coverages),
            "start_error_seconds": _stats(start_errors),
            "undetected_full_break_seconds": undetected_break_seconds,
        },
        "music_edge_cohorts": music_metrics,
        "partial_labeled_prediction_precision": {
            "classifiable_predictions": classifiable_predictions,
            "content_only_predictions": content_only_predictions,
            "duplicate_or_fragment_predictions": duplicate_or_fragment_predictions,
            "matched_positive_predictions": matched_positive_predictions,
            "precision": _rate(matched_positive_predictions, classifiable_predictions),
            "positive_veto_conflicts": positive_veto_conflicts,
            "unknown_prediction_details": sorted(
                unknown_predictions,
                key=lambda item: (item["episode_id"], item["start_seconds"], item["end_seconds"]),
            ),
            "unknown_predictions": len(unknown_predictions),
        },
        "prediction_accounting": {
            "eligible": eligible_count,
            "eligible_was_skipped": eligible_skipped,
            "excluded_decision_state": excluded_count,
            "excluded_was_skipped": excluded_skipped,
            "total": total_predictions,
        },
    }


def _validate_controlled_runs(evaluation: dict, runs: list[dict], policy: dict) -> None:
    expected_assets = {asset["episode_id"]: asset for asset in evaluation["assets"]}
    first = runs[0]
    for run in runs:
        if run["source_revision"] != first["source_revision"]:
            raise ScoringError("source_revision drift across raw runs")
        if run["production_config"] != first["production_config"]:
            raise ScoringError("production_config drift across raw runs")
        if run["runtime"] != first["runtime"]:
            raise ScoringError("runtime drift across raw runs")
        actual_assets = {episode["episode_id"]: episode for episode in run["episodes"]}
        if set(actual_assets) != set(expected_assets):
            missing = sorted(set(expected_assets) - set(actual_assets))
            extra = sorted(set(actual_assets) - set(expected_assets))
            raise ScoringError(f"raw run evaluation asset mismatch; missing={missing}, extra={extra}")
        for episode_id, asset in expected_assets.items():
            episode = actual_assets[episode_id]
            for name in ("show_name", "audio_fingerprint"):
                if episode[name] != asset[name]:
                    raise ScoringError(f"raw run {episode_id} {name} does not match evaluation")
            if (
                abs(episode["duration_seconds"] - asset["duration_seconds"])
                > policy["asset_duration_tolerance_seconds"]
            ):
                raise ScoringError(
                    f"raw run {episode_id} duration_seconds exceeds policy tolerance"
                )
            if any(
                region["end_seconds"] > episode["duration_seconds"]
                for name in ("full_breaks", "presence_anchors", "content_vetoes")
                for region in asset[name]
            ):
                raise ScoringError(
                    f"raw run {episode_id} duration_seconds leaves an evaluation label unreachable"
                )
    first_episodes = {episode["episode_id"]: episode for episode in first["episodes"]}
    for run in runs[1:]:
        episodes = {episode["episode_id"]: episode for episode in run["episodes"]}
        for episode_id, first_episode in first_episodes.items():
            episode = episodes[episode_id]
            if episode["duration_seconds"] != first_episode["duration_seconds"]:
                raise ScoringError(f"decoded duration drift for {episode_id}")
            if episode["transcript_sha256"] != first_episode["transcript_sha256"]:
                raise ScoringError(f"transcript_sha256 drift for {episode_id}")
            if episode["music_features"] != first_episode["music_features"]:
                raise ScoringError(f"music_features drift for {episode_id}")


def _summary_for_metrics(metrics_by_run: Sequence[dict]) -> dict:
    def values(path: tuple[str, ...]) -> list[float | None]:
        result: list[float | None] = []
        for metrics in metrics_by_run:
            value: object = metrics
            for key in path:
                value = value[key]
            result.append(value if value is None else float(value))
        return result

    return {
        "classifiable_prediction_denominator": _aggregate(
            values(("partial_labeled_prediction_precision", "classifiable_predictions"))
        ),
        "conditional_labeled_matched_predictions": _aggregate(
            values(("partial_labeled_prediction_precision", "matched_positive_predictions"))
        ),
        "conditional_labeled_precision": _aggregate(
            values(("partial_labeled_prediction_precision", "precision"))
        ),
        "content_only_predictions": _aggregate(
            values(("partial_labeled_prediction_precision", "content_only_predictions"))
        ),
        "content_veto_colliding_predictions": _aggregate(
            values(("content_veto_collisions", "colliding_predictions"))
        ),
        "content_veto_colliding_pairs": _aggregate(
            values(("content_veto_collisions", "colliding_pairs"))
        ),
        "content_veto_overlap_seconds": _aggregate(
            values(("content_veto_collisions", "unique_overlap_seconds"))
        ),
        "content_veto_references": _aggregate(
            values(("content_veto_collisions", "reference_vetoes"))
        ),
        "content_vetoes_hit": _aggregate(
            values(("content_veto_collisions", "vetoes_hit"))
        ),
        "duplicate_or_fragment_predictions": _aggregate(
            values(
                ("partial_labeled_prediction_precision", "duplicate_or_fragment_predictions")
            )
        ),
        "full_break_near_complete_rate": _aggregate(
            values(("detection", "full_breaks", "near_complete_rate"))
        ),
        "full_break_near_complete": _aggregate(
            values(("detection", "full_breaks", "near_complete"))
        ),
        "full_break_detected": _aggregate(
            values(("detection", "full_breaks", "detected"))
        ),
        "full_break_missed": _aggregate(
            values(("detection", "full_breaks", "missed"))
        ),
        "full_break_references": _aggregate(
            values(("detection", "full_breaks", "references"))
        ),
        "full_break_recall": _aggregate(values(("detection", "full_breaks", "recall"))),
        "mean_absolute_end_error_seconds": _aggregate(
            values(("localization", "absolute_end_error_seconds", "mean"))
        ),
        "mean_absolute_start_error_seconds": _aggregate(
            values(("localization", "absolute_start_error_seconds", "mean"))
        ),
        "mean_end_error_seconds": _aggregate(
            values(("localization", "end_error_seconds", "mean"))
        ),
        "mean_matched_reference_coverage": _aggregate(
            values(("localization", "matched_reference_coverage", "mean"))
        ),
        "mean_start_error_seconds": _aggregate(
            values(("localization", "start_error_seconds", "mean"))
        ),
        "mean_union_reference_coverage": _aggregate(
            values(("localization", "union_reference_coverage", "mean"))
        ),
        "matched_boundary_overreach_seconds": _aggregate(
            values(("localization", "matched_boundary_overreach_seconds"))
        ),
        "matched_boundary_undershoot_seconds": _aggregate(
            values(("localization", "matched_boundary_undershoot_seconds"))
        ),
        "matched_localization_denominator": _aggregate(
            values(("localization", "start_error_seconds", "count"))
        ),
        "presence_anchor_recall": _aggregate(
            values(("detection", "presence_anchors", "recall"))
        ),
        "presence_anchor_detected": _aggregate(
            values(("detection", "presence_anchors", "detected"))
        ),
        "presence_anchor_missed": _aggregate(
            values(("detection", "presence_anchors", "missed"))
        ),
        "presence_anchor_references": _aggregate(
            values(("detection", "presence_anchors", "references"))
        ),
        "positive_veto_conflict_predictions": _aggregate(
            values(("partial_labeled_prediction_precision", "positive_veto_conflicts"))
        ),
        "eligible_predictions": _aggregate(
            values(("prediction_accounting", "eligible"))
        ),
        "eligible_was_skipped_predictions": _aggregate(
            values(("prediction_accounting", "eligible_was_skipped"))
        ),
        "excluded_decision_state_predictions": _aggregate(
            values(("prediction_accounting", "excluded_decision_state"))
        ),
        "excluded_was_skipped_predictions": _aggregate(
            values(("prediction_accounting", "excluded_was_skipped"))
        ),
        "total_predictions": _aggregate(values(("prediction_accounting", "total"))),
        "undetected_full_break_seconds": _aggregate(
            values(("localization", "undetected_full_break_seconds"))
        ),
        "unknown_predictions": _aggregate(
            values(("partial_labeled_prediction_precision", "unknown_predictions"))
        ),
    }


def _music_summary_for_metrics(metrics_by_run: Sequence[dict]) -> dict:
    summary: dict[str, dict] = {}
    for cohort in (
        "continuous_music_bed",
        "nonmusic",
        "insufficient_context",
    ):
        cohort_values = [metrics["music_edge_cohorts"][cohort] for metrics in metrics_by_run]
        summary[cohort] = {
            "boundary_overreach_seconds": _aggregate(
                [value["boundary_overreach_seconds"] for value in cohort_values]
            ),
            "boundary_undershoot_seconds": _aggregate(
                [value["boundary_undershoot_seconds"] for value in cohort_values]
            ),
            "detected": _aggregate([value["detected"] for value in cohort_values]),
            "mean_end_error_seconds": _aggregate(
                [value["end_error_seconds"]["mean"] for value in cohort_values]
            ),
            "mean_absolute_end_error_seconds": _aggregate(
                [value["absolute_end_error_seconds"]["mean"] for value in cohort_values]
            ),
            "mean_absolute_start_error_seconds": _aggregate(
                [value["absolute_start_error_seconds"]["mean"] for value in cohort_values]
            ),
            "mean_matched_reference_coverage": _aggregate(
                [value["matched_reference_coverage"]["mean"] for value in cohort_values]
            ),
            "mean_union_reference_coverage": _aggregate(
                [value["union_reference_coverage"]["mean"] for value in cohort_values]
            ),
            "mean_start_error_seconds": _aggregate(
                [value["start_error_seconds"]["mean"] for value in cohort_values]
            ),
            "matched_localization_denominator": _aggregate(
                [value["start_error_seconds"]["count"] for value in cohort_values]
            ),
            "near_complete": _aggregate(
                [value["near_complete"] for value in cohort_values]
            ),
            "near_complete_rate": _aggregate(
                [value["near_complete_rate"] for value in cohort_values]
            ),
            "recall": _aggregate([value["recall"] for value in cohort_values]),
            "reference_breaks": _aggregate(
                [value["reference_breaks"] for value in cohort_values]
            ),
        }
    return summary


def build_report(
    evaluation_path: pathlib.Path,
    policy_path: pathlib.Path,
    raw_paths: Sequence[pathlib.Path],
) -> dict:
    evaluation = load_evaluation(pathlib.Path(evaluation_path))
    policy = load_policy(
        pathlib.Path(policy_path), expected_evaluation_sha256=evaluation["_sha256"]
    )
    if len(raw_paths) != policy["required_run_count"]:
        raise ScoringError(
            f"baseline requires exactly {policy['required_run_count']} raw runs; got {len(raw_paths)}"
        )
    runs = [
        load_raw_run(pathlib.Path(path), expected_evaluation_sha256=evaluation["_sha256"])
        for path in raw_paths
    ]
    run_ids = [run["run_id"] for run in runs]
    if sorted(run_ids) != sorted(policy["run_ids"]):
        raise ScoringError("raw run_id values must exactly match scoring policy.run_ids")
    raw_hashes = [run["_sha256"] for run in runs]
    if len(raw_hashes) != len(set(raw_hashes)):
        raise ScoringError("raw run bytes must be distinct")
    captured_times = [run["captured_at_utc"] for run in runs]
    if len(captured_times) != len(set(captured_times)):
        raise ScoringError("raw runs must have distinct capture timestamps")
    runs.sort(key=lambda run: run["run_id"])
    _validate_controlled_runs(evaluation, runs, policy)

    assignments: list[dict] = []
    cohort_episode_ids: dict[str, set[str]] = {"development": set(), "holdout": set()}
    by_show: dict[str, list[str]] = {}
    for asset in evaluation["assets"]:
        by_show.setdefault(asset["show_name"], []).append(asset["episode_id"])
    for show_name in sorted(by_show):
        cohort, bucket = _show_assignment(show_name, policy["show_split"])
        cohort_episode_ids[cohort].update(by_show[show_name])
        assignments.append(
            {
                "bucket": bucket,
                "cohort": cohort,
                "episode_count": len(by_show[show_name]),
                "show_name": show_name,
            }
        )
    if any(not episode_ids for episode_ids in cohort_episode_ids.values()):
        raise ScoringError("frozen show split must populate development and holdout cohorts")

    report_runs: list[dict] = []
    overall_metrics: list[dict] = []
    split_metrics: dict[str, list[dict]] = {"development": [], "holdout": []}
    for run in runs:
        metrics = _score_run(evaluation, run, policy)
        overall_metrics.append(metrics)
        run_splits: dict[str, dict] = {}
        for cohort in ("development", "holdout"):
            cohort_metrics = _score_run(
                evaluation,
                run,
                policy,
                allowed_episode_ids=cohort_episode_ids[cohort],
            )
            split_metrics[cohort].append(cohort_metrics)
            run_splits[cohort] = cohort_metrics
        report_runs.append(
            {
                "captured_at_utc": run["captured_at_utc"],
                "metrics": metrics,
                "raw_sha256": run["_sha256"],
                "run_id": run["run_id"],
                "show_split": run_splits,
            }
        )

    break_results: dict[str, dict] = {}
    for run_entry in report_runs:
        for detail in run_entry["metrics"]["detection"]["full_breaks"]["details"]:
            accumulator = break_results.setdefault(
                detail["reference_id"],
                {
                    "detected_runs": 0,
                    "episode_id": detail["episode_id"],
                    "music_edge_cohort": detail["music_edge_cohort"],
                    "reference_end_seconds": detail["reference_end_seconds"],
                    "reference_id": detail["reference_id"],
                    "reference_start_seconds": detail["reference_start_seconds"],
                    "show_name": detail["show_name"],
                    "start_errors": [],
                    "end_errors": [],
                    "matched_coverages": [],
                    "union_coverages": [],
                },
            )
            accumulator["matched_coverages"].append(
                detail["matched_reference_coverage"]
            )
            accumulator["union_coverages"].append(
                detail["union_reference_coverage"]
            )
            if detail["detected"]:
                accumulator["detected_runs"] += 1
                accumulator["start_errors"].append(detail["start_error_seconds"])
                accumulator["end_errors"].append(detail["end_error_seconds"])
    frequency: list[dict] = []
    for reference_id in sorted(break_results):
        item = break_results[reference_id]
        frequency.append(
            {
                "detected_runs": item["detected_runs"],
                "end_error_seconds": _stats(item["end_errors"]),
                "episode_id": item["episode_id"],
                "frequency": item["detected_runs"] / policy["required_run_count"],
                "music_edge_cohort": item["music_edge_cohort"],
                "matched_reference_coverage": _stats(item["matched_coverages"]),
                "union_reference_coverage": _stats(item["union_coverages"]),
                "reference_end_seconds": item["reference_end_seconds"],
                "reference_id": item["reference_id"],
                "reference_start_seconds": item["reference_start_seconds"],
                "show_name": item["show_name"],
                "start_error_seconds": _stats(item["start_errors"]),
                "total_runs": policy["required_run_count"],
            }
        )

    summary = _summary_for_metrics(overall_metrics)
    summary["per_break_detection_frequency"] = frequency
    summary["show_split"] = {
        cohort: _summary_for_metrics(split_metrics[cohort])
        for cohort in ("development", "holdout")
    }
    summary["music_edge_cohorts"] = _music_summary_for_metrics(overall_metrics)
    for cohort in ("development", "holdout"):
        summary["show_split"][cohort]["music_edge_cohorts"] = _music_summary_for_metrics(
            split_metrics[cohort]
        )

    controlled_inputs = [
        {
            "audio_fingerprint": episode["audio_fingerprint"],
            "duration_seconds": episode["duration_seconds"],
            "episode_id": episode["episode_id"],
            "music_features_sha256": hashlib.sha256(
                _canonical_bytes(episode["music_features"])
            ).hexdigest(),
            "transcript_sha256": episode["transcript_sha256"],
        }
        for episode in runs[0]["episodes"]
    ]
    scorer_sha256 = hashlib.sha256(
        _read_regular_bytes(SCORER_PATH, "scorer implementation", maximum_bytes=4 * 1024 * 1024)
    ).hexdigest()
    production_config_sha256 = hashlib.sha256(
        _canonical_bytes(runs[0]["production_config"])
    ).hexdigest()
    runtime_sha256 = hashlib.sha256(_canonical_bytes(runs[0]["runtime"])).hexdigest()
    return {
        "artifact_kind": "unchanged_production_partial_silver_baseline",
        "controlled_identity": {
            "inputs": controlled_inputs,
            "inputs_sha256": hashlib.sha256(_canonical_bytes(controlled_inputs)).hexdigest(),
            "production_config": runs[0]["production_config"],
            "production_config_sha256": production_config_sha256,
            "runtime": runs[0]["runtime"],
            "runtime_sha256": runtime_sha256,
            "source_revision": runs[0]["source_revision"],
        },
        "label_scope": dict(policy["partial_label_semantics"]),
        "runs": report_runs,
        "schema_version": 1,
        "show_split_assignments": assignments,
        "sources": {
            "evaluation": {
                "artifact_kind": evaluation["artifact_kind"],
                "sha256": evaluation["_sha256"],
            },
            "policy": {
                "artifact_kind": policy["artifact_kind"],
                "sha256": policy["_sha256"],
            },
            "raw_runs": [
                {
                    "captured_at_utc": run["captured_at_utc"],
                    "run_id": run["run_id"],
                    "sha256": run["_sha256"],
                }
                for run in runs
            ],
            "scorer": {
                "path": "scripts/l2f-score-partial-silver.py",
                "sha256": scorer_sha256,
            },
        },
        "three_run_summary": summary,
    }


def _canonical_bytes(value: object) -> bytes:
    try:
        return (
            json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ScoringError(f"cannot serialize report canonically: {error}") from error


def _validate_output_directory(path: pathlib.Path) -> None:
    if any(part in {".", ".."} for part in path.parts):
        raise ScoringError(f"output directory contains traversal: {path}")
    if _has_symlink_component(path):
        raise ScoringError(f"output directory must not contain a symbolic link: {path}")
    if path.exists() and not path.is_dir():
        raise ScoringError(f"output directory is not a directory: {path}")
    ancestor = pathlib.Path(os.path.abspath(path))
    while not ancestor.exists() and ancestor.parent != ancestor:
        ancestor = ancestor.parent
    if not ancestor.is_dir():
        raise ScoringError(f"output directory has a non-directory ancestor: {ancestor}")


def _read_published_bytes(
    directory_descriptor: int,
    name: str,
    output_dir: pathlib.Path,
    maximum_bytes: int,
) -> bytes:
    flags = os.O_RDONLY | os.O_NONBLOCK
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    path = output_dir / name
    descriptor: int | None = None
    try:
        before = os.stat(name, dir_fd=directory_descriptor, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode):
            raise ScoringError(f"existing frozen artifact is not a regular file: {path}")
        descriptor = os.open(name, flags, dir_fd=directory_descriptor)
        after = os.fstat(descriptor)
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            os.close(descriptor)
            descriptor = None
            raise ScoringError(f"existing frozen artifact changed while opening: {path}")
    except ScoringError:
        if descriptor is not None:
            os.close(descriptor)
        raise
    except OSError as error:
        if descriptor is not None:
            os.close(descriptor)
        raise ScoringError(f"cannot read existing frozen artifact {path}: {error}") from error
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
        raise
    if descriptor is None:
        raise ScoringError(f"cannot read existing frozen artifact {path}")
    try:
        return _read_open_regular_bytes(
            descriptor,
            path,
            "existing frozen artifact",
            maximum_bytes,
        )
    finally:
        os.close(descriptor)


def _assert_output_directory_identity(
    output_dir: pathlib.Path,
    directory_descriptor: int,
) -> None:
    current_descriptor = _open_directory_descriptor(output_dir, "output directory")
    try:
        if not os.path.samestat(
            os.fstat(directory_descriptor), os.fstat(current_descriptor)
        ):
            raise ScoringError(f"output directory changed during freeze: {output_dir}")
    finally:
        os.close(current_descriptor)


def _publish_content_addressed(
    output_dir: pathlib.Path,
    directory_descriptor: int,
    data: bytes,
) -> pathlib.Path:
    digest = hashlib.sha256(data).hexdigest()
    destination_name = f"{digest}.json"
    destination = output_dir / destination_name
    temporary_name = ""
    descriptor = -1
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    for _ in range(128):
        candidate = f".l2f-freeze-{secrets.token_hex(16)}"
        try:
            descriptor = os.open(
                candidate,
                flags,
                0o600,
                dir_fd=directory_descriptor,
            )
            temporary_name = candidate
            break
        except FileExistsError:
            continue
        except OSError as error:
            raise ScoringError(
                f"cannot stage frozen artifact in {output_dir}: {error}"
            ) from error
    if descriptor < 0:
        raise ScoringError(f"cannot allocate a staging file in {output_dir}")
    try:
        try:
            handle = os.fdopen(descriptor, "wb")
        except BaseException:
            os.close(descriptor)
            descriptor = -1
            raise
        descriptor = -1
        with handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(
                temporary_name,
                destination_name,
                src_dir_fd=directory_descriptor,
                dst_dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
        except FileExistsError:
            pass
        except OSError as error:
            raise ScoringError(f"cannot publish frozen artifact {destination}: {error}") from error
        existing = _read_published_bytes(
            directory_descriptor,
            destination_name,
            output_dir,
            len(data),
        )
        if existing != data:
            raise ScoringError(f"content-address collision at {destination}")
        os.fsync(directory_descriptor)
        _assert_output_directory_identity(output_dir, directory_descriptor)
    finally:
        try:
            os.unlink(temporary_name, dir_fd=directory_descriptor)
        except FileNotFoundError:
            pass
        except OSError as error:
            raise ScoringError(
                f"cannot remove frozen-artifact staging file in {output_dir}: {error}"
            ) from error
    return destination


def freeze_report(
    evaluation_path: pathlib.Path,
    policy_path: pathlib.Path,
    raw_paths: Sequence[pathlib.Path],
    output_dir: pathlib.Path,
) -> FreezeResult:
    report = build_report(evaluation_path, policy_path, raw_paths)
    output_dir = pathlib.Path(output_dir)
    _validate_output_directory(output_dir)
    try:
        directory_descriptor = _open_directory_descriptor(
            output_dir,
            "output directory",
            create=True,
        )
    except ScoringError as error:
        raise ScoringError(f"cannot create output directory {output_dir}: {error}") from error
    try:
        _assert_output_directory_identity(output_dir, directory_descriptor)
        stable_inputs = (
            (
                pathlib.Path(evaluation_path),
                "evaluation",
                report["sources"]["evaluation"]["sha256"],
            ),
            (
                pathlib.Path(policy_path),
                "scoring policy",
                report["sources"]["policy"]["sha256"],
            ),
            (
                SCORER_PATH,
                "scorer implementation",
                report["sources"]["scorer"]["sha256"],
            ),
        )
        for path, label, expected_digest in stable_inputs:
            actual_digest = hashlib.sha256(_read_regular_bytes(path, label)).hexdigest()
            if actual_digest != expected_digest:
                raise ScoringError(f"{label} changed during freeze: {path}")
        expected_digests = {
            source["sha256"] for source in report["sources"]["raw_runs"]
        }
        by_digest: dict[str, bytes] = {}
        for path in raw_paths:
            raw_bytes = _read_regular_bytes(pathlib.Path(path), "raw production run")
            digest = hashlib.sha256(raw_bytes).hexdigest()
            if digest not in expected_digests:
                raise ScoringError(f"raw production run changed during freeze: {path}")
            by_digest[digest] = raw_bytes
        if set(by_digest) != expected_digests:
            raise ScoringError("raw production runs changed during freeze")
        frozen_raw = tuple(
            _publish_content_addressed(
                output_dir,
                directory_descriptor,
                by_digest[source["sha256"]],
            )
            for source in report["sources"]["raw_runs"]
        )
        report_path = _publish_content_addressed(
            output_dir,
            directory_descriptor,
            _canonical_bytes(report),
        )
        return FreezeResult(report_path=report_path, raw_paths=frozen_raw)
    finally:
        os.close(directory_descriptor)


def _default_policy() -> pathlib.Path | None:
    matches = sorted(EVALUATIONS_DIR.glob("earaudit-partial-silver-baseline-policy-*.json"))
    return matches[0] if len(matches) == 1 else None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evaluation", type=pathlib.Path, default=DEFAULT_EVALUATION)
    parser.add_argument("--policy", type=pathlib.Path, default=_default_policy())
    parser.add_argument("--raw-run", type=pathlib.Path, action="append", required=True)
    parser.add_argument("--output-dir", type=pathlib.Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.policy is None:
        parser.error("--policy is required when exactly one tracked policy is unavailable")
    if args.dry_run:
        if args.output_dir is not None:
            parser.error("--output-dir cannot be combined with --dry-run")
    elif args.output_dir is None:
        parser.error("--output-dir is required unless --dry-run is used")
    try:
        if args.dry_run:
            sys.stdout.buffer.write(
                _canonical_bytes(build_report(args.evaluation, args.policy, args.raw_run))
            )
        else:
            result = freeze_report(
                args.evaluation, args.policy, args.raw_run, args.output_dir
            )
            print(f"report={result.report_path}")
            for path in result.raw_paths:
                print(f"raw={path}")
    except ScoringError as error:
        parser.exit(1, f"error: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
