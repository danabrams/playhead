#!/usr/bin/env python3
"""Behavioral tests for the partial-silver production-baseline scorer."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "l2f-score-partial-silver.py"
SPEC = importlib.util.spec_from_file_location("l2f_score_partial_silver", SCRIPT)
assert SPEC and SPEC.loader
SCORER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SCORER)

ROOT = SCRIPT.parents[1]
EVALUATIONS_DIR = ROOT / "TestFixtures/Corpus/Evaluations"
TRACKED_EVALUATION = next(
    path
    for path in EVALUATIONS_DIR.iterdir()
    if re.fullmatch(r"earaudit-partial-silver-[0-9a-f]{64}\.json", path.name)
)
TRACKED_POLICY = next(
    path
    for path in EVALUATIONS_DIR.iterdir()
    if re.fullmatch(
        r"earaudit-partial-silver-baseline-policy-[0-9a-f]{64}\.json",
        path.name,
    )
)


def canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def addressed_file(root: pathlib.Path, stem: str, value: object) -> pathlib.Path:
    data = canonical_bytes(value)
    prefixes = {
        "evaluation": "earaudit-partial-silver-",
        "policy": "earaudit-partial-silver-baseline-policy-",
    }
    path = root / f"{prefixes[stem]}{hashlib.sha256(data).hexdigest()}.json"
    path.write_bytes(data)
    return path


def evaluation_document() -> dict:
    document = {
        "artifact_kind": "retained_audio_partial_silver_evaluation",
        "assets": [
            {
                "audio_fingerprint": "sha256:" + "a" * 64,
                "content_vetoes": [
                    {
                        "end_seconds": 85.0,
                        "source_ledger_ids": ["ep@80.00-85.00"],
                        "source_review_ids": ["ep@80.00-85.00"],
                        "start_seconds": 80.0,
                    }
                ],
                "duration_seconds": 100.0,
                "episode_id": "episode-one",
                "full_breaks": [
                    {
                        "end_seconds": 60.0,
                        "source_ledger_ids": ["ep@30.00-60.00"],
                        "source_review_ids": ["ep@30.00-60.00"],
                        "start_seconds": 30.0,
                        "supporting_tight_ledger_ids": [],
                    }
                ],
                "presence_anchors": [
                    {
                        "end_seconds": 75.0,
                        "source_ledger_ids": ["ep@70.00-75.00"],
                        "start_seconds": 70.0,
                    }
                ],
                "show_name": "Example Show",
            }
        ],
        "label_semantics": {
            "content_vetoes": "only the exact interval is labeled human-reviewed content; the separate reject ledger may conservatively block overlapping promotion candidates without labeling surrounding audio",
            "coverage": "partial",
            "full_breaks": "human-reviewed complete contiguous ad-break boundaries",
            "presence_anchors": "ad presence only; bounds are not full-break boundary truth",
            "quality": "silver",
            "unlabeled_audio": "unknown_elsewhere",
        },
        "schema_version": 1,
        "sources": {
            "audit_ledger": {"path": "audit.jsonl", "sha256": "1" * 64},
            "ear_audit_review": {"path": "review.json", "sha256": "2" * 64},
            "reject_ledger": {"path": "rejects.jsonl", "sha256": "3" * 64},
        },
        "summary": {
            "assets": 27,
            "boundary_reviews": 1,
            "content_vetoes": 1,
            "duplicate_full_break_groups": 0,
            "full_break_assets": 1,
            "full_breaks": 1,
            "labeled_regions": 3,
            "ledger_rows": 3,
            "presence_anchors": 1,
            "review_status_counts": {"approved": 0, "rebounded": 1, "rejected": 1},
            "tight_evidence_attached": 0,
        },
    }
    for index in range(2, 28):
        document["assets"].append(
            {
                "audio_fingerprint": f"sha256:{index:064x}",
                "content_vetoes": [],
                "duration_seconds": 100.0,
                "episode_id": f"unlabeled-{index:02d}",
                "full_breaks": [],
                "presence_anchors": [],
                "show_name": f"Fixture Show {index:02d}",
            }
        )
    return document


def policy_document(evaluation_sha256: str) -> dict:
    return {
        "artifact_kind": "unchanged_production_partial_silver_scoring_policy",
        "asset_duration_tolerance_seconds": 1.0,
        "eligible_decision_states": ["candidate", "confirmed", "applied"],
        "evaluation_sha256": evaluation_sha256,
        "full_break_coverage_threshold": 0.9,
        "matching": {
            "eligibility": "positive_intersection_seconds",
            "objective": [
                "maximum_cardinality",
                "maximum_total_intersection_seconds",
                "minimum_total_boundary_error_seconds",
                "stable_lexicographic_tie_break",
            ],
        },
        "music_edge_cohort": {
            "band_seconds": 8.0,
            "cohorts": ["continuous_music_bed", "nonmusic", "insufficient_context"],
            "continuous_requirement": "all_four_start_outside_start_inside_end_inside_end_outside_bands_pass",
            "minimum_music_window_fraction": 0.3,
            "minimum_windows_per_band": 3,
            "music_bed_present_levels": ["background", "foreground"],
            "window_assignment": "midpoint",
        },
        "partial_label_semantics": {
            "prediction_precision": "conditional_on_overlap_with_a_labeled_region",
            "positive_veto_conflicts": (
                "excluded_from_conditional_precision_positive_matching_and_counted_as_"
                "labeled_errors_without_changing_primary_detection_matching"
            ),
            "unlabeled_audio": "unknown",
            "was_skipped": "reported_separately_without_changing_eligibility",
            "whole_episode_precision": "not_computed",
        },
        "production_capture": copy.deepcopy(SCORER.PRODUCTION_CAPTURE_POLICY),
        "required_run_count": 3,
        "run_ids": ["baseline-run-1", "baseline-run-2", "baseline-run-3"],
        "schema_version": 1,
        "show_split": copy.deepcopy(SCORER.SHOW_SPLIT_POLICY),
        "three_run_aggregation": copy.deepcopy(SCORER.THREE_RUN_AGGREGATION_POLICY),
    }


def music_features(level: str = "background", *, complete: bool = True) -> list[dict]:
    midpoints = [22.5, 25.0, 27.5, 32.5, 35.0, 37.5, 52.5, 55.0, 57.5, 62.5, 65.0, 67.5]
    if not complete:
        midpoints = midpoints[:-1]
    return [
        {
            "end_seconds": midpoint + 1.0,
            "music_bed_change_score": 0.0,
            "music_bed_level": level,
            "music_bed_offset_score": 0.0,
            "music_bed_onset_score": 0.0,
            "music_probability": 0.5 if level != "none" else 0.0,
            "start_seconds": midpoint - 1.0,
        }
        for midpoint in midpoints
    ]


def prediction(start: float, end: float, **changes: object) -> dict:
    value = {
        "confidence": 0.8,
        "decision_state": "confirmed",
        "end_seconds": end,
        "start_seconds": start,
        "was_skipped": False,
    }
    value.update(changes)
    return value


def raw_document(run_id: str, predictions: list[dict], *, features: list[dict] | None = None) -> dict:
    return {
        "artifact_kind": "unchanged_production_partial_silver_raw",
        "captured_at_utc": f"2026-07-12T00:00:0{run_id[-1]}Z",
        "episodes": [
            {
                "audio_fingerprint": "sha256:" + "a" * 64,
                "duration_seconds": 100.0,
                "episode_id": "episode-one",
                "music_features": features if features is not None else music_features(),
                "predictions": predictions,
                "show_name": "Example Show",
                "transcript_sha256": "b" * 64,
            }
        ],
        "evaluation_sha256": "EVALUATION_SHA",
        "production_config": {
            "ad_detection_config_identity": "AdDetectionConfig.default",
            "ad_detection_defaults": copy.deepcopy(SCORER.EXPECTED_AD_DETECTION_DEFAULTS),
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
            "runner_admission_identity": "permissive_capability_snapshot+battery_level_1.0",
            "scan_cohort_identity": "ScanCohort.productionJSON",
            "narrowing_config_identity": "NarrowingConfig.default",
            "narrowing_defaults": copy.deepcopy(SCORER.EXPECTED_NARROWING_DEFAULTS),
            "pipeline_versions": copy.deepcopy(SCORER.EXPECTED_PIPELINE_VERSIONS),
            "planner_regime": "targetedWithAudit",
            "planner_seed_observations": 5,
        },
        "run_id": run_id,
        "runtime": {
            "architecture": "arm64",
            "capture_lane": "physical_ios",
            "device_os_build": "24A5380h",
            "device_udid": "00008140-001609A42660801C",
            "executable_identity": "com.playhead.tests@1",
            "foundation_models_availability": "available",
            "foundation_models_context_size": 4096,
            "locale_identifier": "en_US_POSIX",
            "os_version": "Test OS 1",
            "xcode_version_actual": "2630",
        },
        "schema_version": 1,
        "source_revision": "c" * 40,
    }


class PartialSilverScorerTests(unittest.TestCase):
    def make_case(
        self,
        root: pathlib.Path,
        raw_values: list[dict] | None = None,
        evaluation: dict | None = None,
    ):
        evaluation = evaluation or evaluation_document()
        evaluation_path = addressed_file(root, "evaluation", evaluation)
        evaluation_sha = hashlib.sha256(evaluation_path.read_bytes()).hexdigest()
        policy_path = addressed_file(root, "policy", policy_document(evaluation_sha))
        if raw_values is None:
            raw_values = [
                raw_document(
                    "baseline-run-1",
                    [
                        prediction(28.0, 62.0, was_skipped=True),
                        prediction(71.0, 74.0),
                        prediction(79.0, 82.0),
                        prediction(90.0, 95.0),
                        prediction(10.0, 12.0, decision_state="suppressed"),
                    ],
                ),
                raw_document(
                    "baseline-run-2",
                    [prediction(32.0, 57.0), prediction(70.0, 75.0)],
                ),
                raw_document("baseline-run-3", [prediction(5.0, 8.0)]),
            ]
        raw_paths = []
        for raw in raw_values:
            raw["evaluation_sha256"] = evaluation_sha
            episodes_by_id = {episode["episode_id"]: episode for episode in raw["episodes"]}
            for asset in evaluation["assets"]:
                if asset["episode_id"] in episodes_by_id:
                    continue
                raw["episodes"].append(
                    {
                        "audio_fingerprint": asset["audio_fingerprint"],
                        "duration_seconds": asset["duration_seconds"],
                        "episode_id": asset["episode_id"],
                        "music_features": [],
                        "predictions": [],
                        "show_name": asset["show_name"],
                        "transcript_sha256": hashlib.sha256(
                            asset["episode_id"].encode()
                        ).hexdigest(),
                    }
                )
            path = root / f"playhead-partial-silver-baseline-{raw['run_id']}.json"
            path.write_bytes(canonical_bytes(raw))
            raw_paths.append(path)
        return evaluation_path, policy_path, raw_paths

    def test_tracked_policy_is_predeclared_and_content_addressed(self) -> None:
        policy = SCORER.load_policy(TRACKED_POLICY, expected_evaluation_sha256=SCORER.sha256_file(TRACKED_EVALUATION))
        self.assertEqual(policy["required_run_count"], 3)
        self.assertEqual(policy["run_ids"], SCORER.REQUIRED_RUN_IDS)
        self.assertEqual(policy["asset_duration_tolerance_seconds"], 1.0)
        self.assertEqual(policy["matching"]["eligibility"], "positive_intersection_seconds")
        self.assertEqual(policy["music_edge_cohort"]["band_seconds"], 8.0)
        self.assertEqual(policy["music_edge_cohort"]["minimum_windows_per_band"], 3)
        self.assertEqual(policy["full_break_coverage_threshold"], 0.9)
        self.assertEqual(
            policy["three_run_aggregation"],
            SCORER.THREE_RUN_AGGREGATION_POLICY,
        )

    def test_aggregate_reports_fixed_run_denominator_and_missing_values(self) -> None:
        present = SCORER._aggregate([0.25, None, 0.75])
        self.assertEqual(present["median"], 0.5)
        self.assertEqual(present["runs_with_value"], 2)
        self.assertEqual(present["total_runs"], 3)

        absent = SCORER._aggregate([None, None, None])
        self.assertIsNone(absent["median"])
        self.assertEqual(absent["runs_with_value"], 0)
        self.assertEqual(absent["total_runs"], 3)

    def test_matching_maximizes_cardinality_before_total_overlap(self) -> None:
        references = [(0.0, 10.0), (10.0, 20.0)]
        predictions = [(0.0, 20.0), (0.0, 9.0)]
        matches = SCORER.maximum_overlap_matching(references, predictions)
        self.assertEqual({(match.reference_index, match.prediction_index) for match in matches}, {(0, 1), (1, 0)})

    def test_matching_maximizes_overlap_and_is_order_stable(self) -> None:
        references = [(0.0, 10.0), (10.0, 20.0)]
        predictions = [(0.0, 15.0), (5.0, 20.0)]
        matches = SCORER.maximum_overlap_matching(references, predictions)
        self.assertEqual([(m.reference_index, m.prediction_index) for m in matches], [(0, 0), (1, 1)])
        self.assertEqual(sum(m.intersection_seconds for m in matches), 20.0)

    def test_matching_uses_lexicographic_tie_break(self) -> None:
        matches = SCORER.maximum_overlap_matching(
            [(0.0, 10.0), (0.0, 10.0)],
            [(0.0, 10.0), (0.0, 10.0)],
        )
        self.assertEqual(
            [(match.reference_index, match.prediction_index) for match in matches],
            [(0, 0), (1, 1)],
        )
        with self.assertRaisesRegex(SCORER.ScoringError, "invalid"):
            SCORER.maximum_overlap_matching([(False, 1.0)], [(0.0, 1.0)])

    def test_matching_prefers_boundary_accuracy_when_intersection_ties(self) -> None:
        matches = SCORER.maximum_overlap_matching(
            [(30.0, 60.0)],
            [(0.0, 100.0), (30.0, 60.0)],
        )
        self.assertEqual(matches, [SCORER.Match(0, 1, 30.0)])

    def test_three_run_score_separates_detection_localization_and_unknowns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            evaluation_path, policy_path, raw_paths = self.make_case(pathlib.Path(tmp))
            report = SCORER.build_report(evaluation_path, policy_path, raw_paths)

        self.assertEqual(report["artifact_kind"], "unchanged_production_partial_silver_baseline")
        self.assertNotIn("created_at", report)
        self.assertEqual(report["label_scope"]["whole_episode_precision"], "not_computed")
        self.assertEqual(
            report["controlled_identity"]["inputs"][0]["audio_fingerprint"],
            "sha256:" + "a" * 64,
        )
        self.assertEqual(
            report["controlled_identity"]["inputs"][0]["transcript_sha256"],
            "b" * 64,
        )
        run1 = report["runs"][0]["metrics"]
        self.assertEqual(run1["prediction_accounting"]["eligible"], 4)
        self.assertEqual(run1["prediction_accounting"]["eligible_was_skipped"], 1)
        self.assertEqual(run1["prediction_accounting"]["excluded_decision_state"], 1)
        self.assertEqual(run1["detection"]["full_breaks"]["detected"], 1)
        self.assertEqual(run1["detection"]["presence_anchors"]["detected"], 1)
        self.assertEqual(run1["partial_labeled_prediction_precision"]["unknown_predictions"], 1)
        self.assertEqual(run1["content_veto_collisions"]["colliding_predictions"], 1)
        self.assertAlmostEqual(
            run1["localization"]["matched_boundary_overreach_seconds"], 4.0
        )
        self.assertNotIn("unsafe_content_overreach_seconds", run1["localization"])
        self.assertAlmostEqual(
            run1["localization"]["matched_boundary_undershoot_seconds"], 0.0
        )
        self.assertEqual(
            run1["music_edge_cohorts"]["continuous_music_bed"]["reference_breaks"],
            1,
        )

        summary = report["three_run_summary"]
        self.assertEqual(summary["per_break_detection_frequency"][0]["detected_runs"], 2)
        self.assertAlmostEqual(summary["per_break_detection_frequency"][0]["frequency"], 2 / 3)
        coverage = summary["per_break_detection_frequency"][0][
            "matched_reference_coverage"
        ]
        self.assertEqual(coverage["count"], 3)
        self.assertEqual(coverage["minimum"], 0.0)
        self.assertEqual(summary["full_break_recall"]["minimum"], 0.0)
        self.assertEqual(summary["full_break_recall"]["maximum"], 1.0)
        self.assertEqual(summary["full_break_references"]["median"], 1.0)
        self.assertEqual(summary["full_break_missed"]["maximum"], 1.0)
        self.assertEqual(summary["full_break_near_complete"]["maximum"], 1.0)
        self.assertEqual(summary["presence_anchor_references"]["median"], 1.0)
        self.assertEqual(summary["classifiable_prediction_denominator"]["maximum"], 3.0)
        self.assertEqual(summary["eligible_predictions"]["minimum"], 1.0)
        self.assertEqual(summary["eligible_predictions"]["maximum"], 4.0)
        self.assertEqual(summary["eligible_was_skipped_predictions"]["maximum"], 1.0)
        self.assertEqual(summary["excluded_decision_state_predictions"]["maximum"], 1.0)
        self.assertEqual(summary["total_predictions"]["maximum"], 5.0)
        self.assertEqual(summary["matched_localization_denominator"]["minimum"], 0.0)
        self.assertEqual(summary["matched_localization_denominator"]["maximum"], 1.0)
        self.assertEqual(summary["mean_start_error_seconds"]["minimum"], -2.0)
        self.assertEqual(summary["mean_start_error_seconds"]["maximum"], 2.0)
        self.assertEqual(summary["mean_end_error_seconds"]["minimum"], -3.0)
        self.assertEqual(summary["mean_end_error_seconds"]["maximum"], 2.0)
        self.assertEqual(summary["undetected_full_break_seconds"]["maximum"], 30.0)
        self.assertEqual(summary["per_break_detection_frequency"][0]["total_runs"], 3)
        music_summary = summary["music_edge_cohorts"]["continuous_music_bed"]
        self.assertEqual(music_summary["matched_localization_denominator"]["minimum"], 0.0)
        self.assertEqual(music_summary["detected"]["maximum"], 1.0)
        self.assertEqual(music_summary["reference_breaks"]["median"], 1.0)
        self.assertIn("holdout", summary["show_split"])
        self.assertIn("development", summary["show_split"])
        split_music_references = sum(
            summary["show_split"][cohort]["music_edge_cohorts"][
                "continuous_music_bed"
            ]["reference_breaks"]["median"]
            for cohort in ("development", "holdout")
        )
        self.assertEqual(split_music_references, 1.0)
        self.assertEqual(
            report["sources"]["scorer"]["sha256"],
            hashlib.sha256(SCRIPT.read_bytes()).hexdigest(),
        )
        self.assertEqual(len(report["controlled_identity"]["inputs"]), 27)
        self.assertEqual(
            report["controlled_identity"]["production_config_sha256"],
            hashlib.sha256(
                canonical_bytes(report["controlled_identity"]["production_config"])
            ).hexdigest(),
        )

    def test_coverage_and_signed_boundary_errors_are_reported_separately(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            evaluation_path, policy_path, raw_paths = self.make_case(pathlib.Path(tmp))
            report = SCORER.build_report(evaluation_path, policy_path, raw_paths)
        run2 = report["runs"][1]["metrics"]
        self.assertEqual(run2["detection"]["full_breaks"]["near_complete"], 0)
        self.assertAlmostEqual(
            run2["localization"]["matched_reference_coverage"]["mean"], 25 / 30
        )
        self.assertEqual(run2["localization"]["start_error_seconds"]["mean"], 2.0)
        self.assertEqual(run2["localization"]["end_error_seconds"]["mean"], -3.0)
        self.assertEqual(run2["localization"]["absolute_start_error_seconds"]["mean"], 2.0)
        self.assertEqual(run2["localization"]["absolute_end_error_seconds"]["mean"], 3.0)
        self.assertEqual(run2["localization"]["matched_boundary_undershoot_seconds"], 5.0)
        music = run2["music_edge_cohorts"]["continuous_music_bed"]
        self.assertEqual(music["start_error_seconds"]["mean"], 2.0)
        self.assertEqual(music["end_error_seconds"]["mean"], -3.0)
        self.assertAlmostEqual(music["matched_reference_coverage"]["mean"], 25 / 30)

    def test_fragment_union_is_diagnostic_but_cannot_earn_near_complete_success(self) -> None:
        fragmented_runs = [
            raw_document(
                f"baseline-run-{run_number}",
                [prediction(30.0, 50.0), prediction(40.0, 60.0)],
            )
            for run_number in range(1, 4)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            evaluation_path, policy_path, raw_paths = self.make_case(
                pathlib.Path(tmp), fragmented_runs
            )
            report = SCORER.build_report(evaluation_path, policy_path, raw_paths)
        metrics = report["runs"][0]["metrics"]
        full_break = metrics["detection"]["full_breaks"]
        self.assertEqual(full_break["detected"], 1)
        self.assertEqual(full_break["near_complete"], 0)
        self.assertEqual(full_break["details"][0]["union_intersection_seconds"], 30.0)
        self.assertEqual(full_break["details"][0]["union_reference_coverage"], 1.0)
        self.assertEqual(full_break["details"][0]["matched_reference_coverage"], 2 / 3)
        self.assertEqual(metrics["localization"]["matched_reference_coverage"]["mean"], 2 / 3)
        self.assertEqual(metrics["localization"]["union_reference_coverage"]["mean"], 1.0)
        self.assertEqual(
            metrics["partial_labeled_prediction_precision"][
                "duplicate_or_fragment_predictions"
            ],
            1,
        )

    def test_policy_predeclares_every_controlled_capture_identity(self) -> None:
        policy = SCORER.load_policy(
            TRACKED_POLICY,
            expected_evaluation_sha256=SCORER.sha256_file(TRACKED_EVALUATION),
        )
        self.assertEqual(policy["production_capture"], SCORER.PRODUCTION_CAPTURE_POLICY)

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            evaluation_path, policy_path, raw_paths = self.make_case(root)
            raw = json.loads(raw_paths[0].read_text())
            raw["production_config"]["runner_admission_identity"] = "ambient-device-state"
            raw_paths[0].write_bytes(canonical_bytes(raw))
            with self.assertRaisesRegex(SCORER.ScoringError, "runner_admission_identity"):
                SCORER.build_report(evaluation_path, policy_path, raw_paths)

    def test_exact_duplicate_predictions_are_measured_without_true_positive_inflation(self) -> None:
        duplicate_runs = [
            raw_document(
                f"baseline-run-{run_number}",
                [prediction(30.0, 60.0), prediction(30.0, 60.0)],
            )
            for run_number in range(1, 4)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            evaluation_path, policy_path, raw_paths = self.make_case(
                pathlib.Path(tmp), duplicate_runs
            )
            report = SCORER.build_report(evaluation_path, policy_path, raw_paths)
        metrics = report["runs"][0]["metrics"]
        self.assertEqual(metrics["prediction_accounting"]["eligible"], 2)
        self.assertEqual(metrics["detection"]["full_breaks"]["detected"], 1)
        self.assertEqual(metrics["detection"]["full_breaks"]["near_complete"], 1)
        self.assertEqual(
            metrics["partial_labeled_prediction_precision"][
                "duplicate_or_fragment_predictions"
            ],
            1,
        )

    def test_one_prediction_cannot_detect_both_full_break_and_presence_anchor(self) -> None:
        broad_runs = [
            raw_document(
                f"baseline-run-{run_number}",
                [prediction(30.0, 75.0)],
            )
            for run_number in range(1, 4)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            evaluation_path, policy_path, raw_paths = self.make_case(
                pathlib.Path(tmp), broad_runs
            )
            report = SCORER.build_report(evaluation_path, policy_path, raw_paths)

        detection = report["runs"][0]["metrics"]["detection"]
        self.assertEqual(detection["full_breaks"]["detected"], 1)
        self.assertEqual(detection["presence_anchors"]["detected"], 0)
        self.assertEqual(
            detection["full_breaks"]["detected"]
            + detection["presence_anchors"]["detected"],
            1,
        )

    def test_veto_conflict_cannot_steal_clean_prediction_precision_match(self) -> None:
        raws = [
            raw_document(
                f"baseline-run-{index}",
                [prediction(31.0, 59.0), prediction(30.0, 85.0)],
            )
            for index in range(1, 4)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            evaluation_path, policy_path, raw_paths = self.make_case(
                pathlib.Path(tmp), raws
            )
            report = SCORER.build_report(evaluation_path, policy_path, raw_paths)
        precision = report["runs"][0]["metrics"]["partial_labeled_prediction_precision"]
        self.assertEqual(precision["matched_positive_predictions"], 1)
        self.assertEqual(precision["positive_veto_conflicts"], 1)
        self.assertEqual(precision["content_only_predictions"], 0)
        self.assertEqual(precision["duplicate_or_fragment_predictions"], 0)
        self.assertEqual(precision["precision"], 0.5)

        detection = report["runs"][0]["metrics"]["detection"]
        self.assertEqual(detection["full_breaks"]["detected"], 1)
        self.assertEqual(detection["presence_anchors"]["detected"], 1)

    def test_veto_overlap_seconds_do_not_collapse_equal_timelines_across_episodes(self) -> None:
        evaluation = evaluation_document()
        second_asset = evaluation["assets"][1]
        second_asset.update(
            episode_id="episode-two",
            show_name="Second Show",
        )
        second_asset["content_vetoes"] = copy.deepcopy(
            evaluation["assets"][0]["content_vetoes"]
        )
        evaluation["summary"].update(content_vetoes=2, labeled_regions=4)
        raw_values = []
        for run_number in range(1, 4):
            raw = raw_document(
                f"baseline-run-{run_number}", [prediction(80.0, 82.0)]
            )
            second_episode = copy.deepcopy(raw["episodes"][0])
            second_episode.update(
                audio_fingerprint=second_asset["audio_fingerprint"],
                episode_id="episode-two",
                music_features=[],
                predictions=[prediction(80.0, 82.0)],
                show_name="Second Show",
                transcript_sha256="e" * 64,
            )
            raw["episodes"].append(second_episode)
            raw_values.append(raw)
        with tempfile.TemporaryDirectory() as tmp:
            evaluation_path, policy_path, raw_paths = self.make_case(
                pathlib.Path(tmp), raw_values, evaluation
            )
            report = SCORER.build_report(evaluation_path, policy_path, raw_paths)
        self.assertEqual(
            report["runs"][0]["metrics"]["content_veto_collisions"][
                "unique_overlap_seconds"
            ],
            4.0,
        )
        self.assertEqual(
            report["runs"][0]["metrics"]["content_veto_collisions"][
                "colliding_pairs"
            ],
            2,
        )

    def test_music_cohort_requires_all_four_bands_and_three_windows_each(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            raws = [
                raw_document(
                    f"baseline-run-{index}",
                    [],
                    features=music_features(complete=False),
                )
                for index in range(1, 4)
            ]
            evaluation_path, policy_path, raw_paths = self.make_case(root, raws)
            report = SCORER.build_report(evaluation_path, policy_path, raw_paths)
        metrics = report["runs"][0]["metrics"]["music_edge_cohorts"]
        self.assertEqual(metrics["insufficient_context"]["reference_breaks"], 1)
        self.assertEqual(metrics["continuous_music_bed"]["reference_breaks"], 0)

    def test_music_cohort_requires_full_eight_second_episode_context(self) -> None:
        evaluation = evaluation_document()
        reference = evaluation["assets"][0]["full_breaks"][0]
        reference.update(start_seconds=5.0, end_seconds=35.0)
        raws = [
            raw_document(
                f"baseline-run-{index}",
                [prediction(5.0, 35.0)],
                features=music_features(),
            )
            for index in range(1, 4)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            evaluation_path, policy_path, raw_paths = self.make_case(
                pathlib.Path(tmp), raws, evaluation
            )
            report = SCORER.build_report(evaluation_path, policy_path, raw_paths)
        detail = report["runs"][0]["metrics"]["detection"]["full_breaks"]["details"][0]
        self.assertEqual(detail["music_edge_cohort"], "insufficient_context")
        self.assertFalse(detail["music_edge_bands"]["start_outside"]["inside_episode"])

    def test_freeze_is_content_addressed_idempotent_and_input_order_independent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            evaluation_path, policy_path, raw_paths = self.make_case(root)
            output = root / "frozen"
            result1 = SCORER.freeze_report(evaluation_path, policy_path, raw_paths, output)
            result2 = SCORER.freeze_report(evaluation_path, policy_path, list(reversed(raw_paths)), output)

            self.assertEqual(result1.report_path, result2.report_path)
            self.assertEqual(result1.report_path.name, f"{hashlib.sha256(result1.report_path.read_bytes()).hexdigest()}.json")
            self.assertEqual(len(result1.raw_paths), 3)
            for source, frozen in zip(sorted(raw_paths), sorted(result1.raw_paths)):
                self.assertEqual(frozen.name, f"{hashlib.sha256(frozen.read_bytes()).hexdigest()}.json")
                self.assertIn(source.read_bytes(), [path.read_bytes() for path in result1.raw_paths])
            report = json.loads(result1.report_path.read_text())
            self.assertEqual(len(report["sources"]["raw_runs"]), 3)

    def test_rejects_duplicate_json_fields_and_nonstandard_numbers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            duplicate = root / "duplicate.json"
            duplicate.write_text('{"schema_version":1,"schema_version":1}\n')
            nan = root / "nan.json"
            nan.write_text('{"value":NaN}\n')
            with self.assertRaisesRegex(SCORER.ScoringError, "duplicate JSON object field"):
                SCORER.read_json_file(duplicate, "raw run")
            with self.assertRaisesRegex(SCORER.ScoringError, "finite JSON number"):
                SCORER.read_json_file(nan, "raw run")

    def test_rejects_symlink_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            target = root / "target.json"
            target.write_text("{}\n")
            link = root / "link.json"
            link.symlink_to(target)
            with self.assertRaisesRegex(SCORER.ScoringError, "regular file"):
                SCORER.read_json_file(link, "raw run")
            with self.assertRaisesRegex(SCORER.ScoringError, "exceeds 2 bytes"):
                SCORER._read_regular_bytes(target, "raw run", maximum_bytes=2)

    def test_fifo_input_is_rejected_without_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fifo = pathlib.Path(tmp) / "hostile.json"
            os.mkfifo(fifo)
            probe = """
import importlib.util
import pathlib
import sys

spec = importlib.util.spec_from_file_location("l2f_fifo_probe", sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
try:
    module._read_regular_bytes(pathlib.Path(sys.argv[2]), "FIFO probe")
except module.ScoringError:
    raise SystemExit(0)
raise SystemExit(1)
"""
            result = subprocess.run(
                [sys.executable, "-c", probe, str(SCRIPT), str(fifo)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=2,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_input_parent_swap_cannot_substitute_checked_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            source = root / "source"
            hostile = root / "hostile"
            source.mkdir()
            hostile.mkdir()
            path = source / "value.json"
            path.write_bytes(b"expected")
            (hostile / path.name).write_bytes(b"substituted")
            original_open = SCORER.os.open
            swapped = False

            def swapping_open(name, flags, mode=0o777, *, dir_fd=None):
                nonlocal swapped
                if name == path.name and dir_fd is not None and not swapped:
                    swapped = True
                    source.rename(root / "original")
                    source.symlink_to(hostile, target_is_directory=True)
                return original_open(name, flags, mode, dir_fd=dir_fd)

            with mock.patch.object(SCORER.os, "open", side_effect=swapping_open):
                data = SCORER._read_regular_bytes(path, "swap probe")

            self.assertTrue(swapped)
            self.assertEqual(data, b"expected")

    def test_descriptor_readers_close_owned_fds_on_unexpected_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            child = root / "child"
            child.mkdir()
            opened: list[int] = []
            original_open = os.open

            def recording_open(*args, **kwargs):
                descriptor = original_open(*args, **kwargs)
                opened.append(descriptor)
                return descriptor

            with mock.patch.object(SCORER.os, "open", side_effect=recording_open), mock.patch.object(
                SCORER.os,
                "fstat",
                side_effect=RuntimeError("forced post-open failure"),
            ):
                with self.assertRaisesRegex(RuntimeError, "forced post-open"):
                    SCORER._open_directory_descriptor(child, "directory probe")
            self.assertGreaterEqual(len(opened), 2)
            for descriptor in opened:
                with self.assertRaises(OSError):
                    os.fstat(descriptor)

            value = child / "value.json"
            value.write_bytes(b"value")
            parent_descriptor = os.open(child, os.O_RDONLY)
            opened.clear()
            with mock.patch.object(
                SCORER,
                "_open_directory_descriptor",
                return_value=parent_descriptor,
            ), mock.patch.object(
                SCORER.os,
                "open",
                side_effect=recording_open,
            ), mock.patch.object(
                SCORER.os,
                "fstat",
                side_effect=RuntimeError("forced regular-file failure"),
            ):
                with self.assertRaisesRegex(RuntimeError, "forced regular-file"):
                    SCORER._open_regular_descriptor(value, "regular probe")
            for descriptor in [parent_descriptor, *opened]:
                with self.assertRaises(OSError):
                    os.fstat(descriptor)

            directory_descriptor = os.open(child, os.O_RDONLY)
            self.addCleanup(os.close, directory_descriptor)
            opened.clear()
            with mock.patch.object(
                SCORER.os,
                "open",
                side_effect=recording_open,
            ), mock.patch.object(
                SCORER.os,
                "fstat",
                side_effect=RuntimeError("forced published-file failure"),
            ):
                with self.assertRaisesRegex(RuntimeError, "forced published-file"):
                    SCORER._read_published_bytes(
                        directory_descriptor,
                        value.name,
                        child,
                        64,
                    )
            self.assertEqual(len(opened), 1)
            with self.assertRaises(OSError):
                os.fstat(opened[0])

    def test_freezer_closes_private_stage_fd_when_stream_creation_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = pathlib.Path(tmp)
            directory_descriptor = SCORER._open_directory_descriptor(output, "output")
            self.addCleanup(os.close, directory_descriptor)
            opened: list[int] = []
            original_open = os.open

            def recording_open(*args, **kwargs):
                descriptor = original_open(*args, **kwargs)
                opened.append(descriptor)
                return descriptor

            with mock.patch.object(
                SCORER.os,
                "open",
                side_effect=recording_open,
            ), mock.patch.object(
                SCORER.os,
                "fdopen",
                side_effect=RuntimeError("forced stream creation failure"),
            ):
                with self.assertRaisesRegex(RuntimeError, "forced stream creation"):
                    SCORER._publish_content_addressed(
                        output,
                        directory_descriptor,
                        b"bytes",
                    )

            self.assertEqual(len(opened), 1)
            with self.assertRaises(OSError):
                os.fstat(opened[0])
            self.assertEqual(list(output.iterdir()), [])

    def test_publication_parent_swap_never_redirects_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            output = root / "output"
            hostile = root / "hostile"
            output.mkdir()
            hostile.mkdir()
            descriptor = SCORER._open_directory_descriptor(output, "output directory")
            self.addCleanup(os.close, descriptor)
            original_link = SCORER.os.link
            swapped = False

            def swapping_link(source, destination, **kwargs):
                nonlocal swapped
                if not swapped:
                    swapped = True
                    output.rename(root / "original")
                    output.symlink_to(hostile, target_is_directory=True)
                return original_link(source, destination, **kwargs)

            with mock.patch.object(SCORER.os, "link", side_effect=swapping_link):
                with self.assertRaisesRegex(SCORER.ScoringError, "output directory"):
                    SCORER._publish_content_addressed(output, descriptor, b"exact bytes")

            self.assertTrue(swapped)
            self.assertEqual(list(hostile.iterdir()), [])
            published = [path for path in (root / "original").iterdir() if not path.name.startswith(".")]
            self.assertEqual(len(published), 1)
            self.assertEqual(published[0].read_bytes(), b"exact bytes")

    def test_content_address_requires_the_exact_artifact_stem(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            data = canonical_bytes(evaluation_document())
            wrong = root / f"wrong-{hashlib.sha256(data).hexdigest()}.json"
            wrong.write_bytes(data)
            with self.assertRaisesRegex(SCORER.ScoringError, "content address"):
                SCORER.load_evaluation(wrong)

            traversal = evaluation_document()
            traversal["sources"]["audit_ledger"]["path"] = "../audit.jsonl"
            traversal_path = addressed_file(root, "evaluation", traversal)
            with self.assertRaisesRegex(SCORER.ScoringError, "traversal-free"):
                SCORER.load_evaluation(traversal_path)

            evaluation_path = addressed_file(root, "evaluation", evaluation_document())
            evaluation_sha = hashlib.sha256(evaluation_path.read_bytes()).hexdigest()
            policy = policy_document(evaluation_sha)
            policy["music_edge_cohort"]["minimum_windows_per_band"] = 3.0
            policy_path = addressed_file(root, "policy", policy)
            with self.assertRaisesRegex(SCORER.ScoringError, "minimum_windows_per_band"):
                SCORER.load_policy(
                    policy_path,
                    expected_evaluation_sha256=evaluation_sha,
                )

            policy = policy_document(evaluation_sha)
            policy["show_split"]["holdout_bucket_count"] = 301
            policy_path = addressed_file(root, "policy", policy)
            with self.assertRaisesRegex(SCORER.ScoringError, "show_split.*drifted"):
                SCORER.load_policy(
                    policy_path,
                    expected_evaluation_sha256=evaluation_sha,
                )

    def test_raw_run_filename_must_match_embedded_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            evaluation_path, _, raw_paths = self.make_case(root)
            wrong_name = root / "renamed-raw.json"
            raw_paths[0].rename(wrong_name)

            with self.assertRaisesRegex(SCORER.ScoringError, "filename.*run_id"):
                SCORER.load_raw_run(
                    wrong_name,
                    expected_evaluation_sha256=hashlib.sha256(
                        evaluation_path.read_bytes()
                    ).hexdigest(),
                )

    def test_rejects_run_schema_identity_and_control_drift(self) -> None:
        mutations = [
            (lambda raw: raw.update(schema_version=True), "schema_version"),
            (lambda raw: raw.update(captured_at_utc="not-a-time"), "captured_at_utc"),
            (lambda raw: raw.update(source_revision="abc"), "source_revision"),
            (lambda raw: raw["episodes"][0].update(audio_fingerprint="sha256:" + "d" * 64), "audio_fingerprint"),
            (lambda raw: raw["episodes"][0].update(extra=True), "fields"),
            (lambda raw: raw["episodes"][0]["predictions"][0].update(confidence=True), "confidence"),
            (lambda raw: raw["episodes"][0]["predictions"][0].update(decision_state="typo"), "decision_state"),
            (lambda raw: raw["episodes"][0]["music_features"][0].update(music_bed_level="music"), "music_bed_level"),
            (
                lambda raw: raw["production_config"]["ad_detection_defaults"].update(
                    bracket_refinement_enabled=False
                ),
                "bracket_refinement_enabled.*drifted",
            ),
            (lambda raw: raw["runtime"].update(extra=True), "runtime fields"),
        ]
        for mutate, message in mutations:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as tmp:
                root = pathlib.Path(tmp)
                raws = [
                    raw_document(
                        f"baseline-run-{index}", [prediction(30, 60)]
                    )
                    for index in range(1, 4)
                ]
                mutate(raws[0])
                evaluation_path, policy_path, raw_paths = self.make_case(root, raws)
                with self.assertRaisesRegex(SCORER.ScoringError, message):
                    SCORER.build_report(evaluation_path, policy_path, raw_paths)

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            raws = [
                raw_document(f"baseline-run-{index}", [])
                for index in range(1, 4)
            ]
            raws[1]["production_config"] = {"ad_detection_config": "changed"}
            evaluation_path, policy_path, raw_paths = self.make_case(root, raws)
            with self.assertRaisesRegex(SCORER.ScoringError, "production_config.*fields"):
                SCORER.build_report(evaluation_path, policy_path, raw_paths)

    def test_requires_exactly_three_distinct_runs_and_all_evaluation_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            evaluation_path, policy_path, raw_paths = self.make_case(root)
            with self.assertRaisesRegex(SCORER.ScoringError, "exactly 3"):
                SCORER.build_report(evaluation_path, policy_path, raw_paths[:2])
            duplicate = copy.deepcopy(json.loads(raw_paths[1].read_text()))
            duplicate["run_id"] = "baseline-run-1"
            raw_paths[1].write_bytes(canonical_bytes(duplicate))
            with self.assertRaisesRegex(SCORER.ScoringError, "run_id"):
                SCORER.build_report(evaluation_path, policy_path, raw_paths)

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            evaluation_path, policy_path, raw_paths = self.make_case(root)
            missing = json.loads(raw_paths[0].read_text())
            missing["episodes"].pop()
            raw_paths[0].write_bytes(canonical_bytes(missing))
            with self.assertRaisesRegex(SCORER.ScoringError, "exactly 27"):
                SCORER.build_report(evaluation_path, policy_path, raw_paths)

    def test_rejects_evaluation_count_and_cross_run_duration_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            evaluation = evaluation_document()
            evaluation["assets"].pop()
            evaluation["summary"]["assets"] = 26
            path = addressed_file(root, "evaluation", evaluation)
            with self.assertRaisesRegex(SCORER.ScoringError, "exactly 27"):
                SCORER.load_evaluation(path)

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            raws = [
                raw_document(f"baseline-run-{index}", [])
                for index in range(1, 4)
            ]
            raws[1]["episodes"][0]["duration_seconds"] = 100.5
            evaluation_path, policy_path, raw_paths = self.make_case(root, raws)
            with self.assertRaisesRegex(SCORER.ScoringError, "decoded duration drift"):
                SCORER.build_report(evaluation_path, policy_path, raw_paths)

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            raws = [
                raw_document(f"baseline-run-{index}", [])
                for index in range(1, 4)
            ]
            raws[1]["captured_at_utc"] = raws[0]["captured_at_utc"]
            evaluation_path, policy_path, raw_paths = self.make_case(root, raws)
            with self.assertRaisesRegex(SCORER.ScoringError, "distinct capture timestamps"):
                SCORER.build_report(evaluation_path, policy_path, raw_paths)

        for field, changed in [
            ("device_udid", "00008140-001609A42660801D"),
            ("device_os_build", "24A9999z"),
        ]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmp:
                root = pathlib.Path(tmp)
                raws = [
                    raw_document(f"baseline-run-{index}", [])
                    for index in range(1, 4)
                ]
                raws[1]["runtime"][field] = changed
                evaluation_path, policy_path, raw_paths = self.make_case(root, raws)
                with self.assertRaisesRegex(SCORER.ScoringError, "runtime drift"):
                    SCORER.build_report(evaluation_path, policy_path, raw_paths)

    def test_runtime_identity_supports_explicit_catalyst_host_lane(self) -> None:
        raws = [
            raw_document(f"baseline-run-{index}", [])
            for index in range(1, 4)
        ]
        for raw in raws:
            raw["runtime"].update(
                capture_lane="mac_catalyst",
                device_os_build="not_applicable",
                device_udid="not_applicable",
            )
        with tempfile.TemporaryDirectory() as tmp:
            evaluation_path, policy_path, raw_paths = self.make_case(
                pathlib.Path(tmp),
                raws,
            )
            report = SCORER.build_report(evaluation_path, policy_path, raw_paths)

        self.assertEqual(
            report["controlled_identity"]["runtime"]["capture_lane"],
            "mac_catalyst",
        )

    def test_duration_tolerance_cannot_leave_a_label_unreachable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            evaluation = evaluation_document()
            evaluation["assets"][0]["full_breaks"][0].update(
                start_seconds=99.0,
                end_seconds=100.0,
            )
            raws = [
                raw_document(f"baseline-run-{index}", [])
                for index in range(1, 4)
            ]
            for raw in raws:
                raw["episodes"][0]["duration_seconds"] = 99.5
            evaluation_path, policy_path, raw_paths = self.make_case(
                root,
                raws,
                evaluation,
            )

            with self.assertRaisesRegex(SCORER.ScoringError, "label unreachable"):
                SCORER.build_report(evaluation_path, policy_path, raw_paths)

    def test_rejects_output_directory_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "child" / ".." / "frozen"
            with self.assertRaisesRegex(SCORER.ScoringError, "traversal"):
                SCORER._validate_output_directory(path)


if __name__ == "__main__":
    unittest.main()
