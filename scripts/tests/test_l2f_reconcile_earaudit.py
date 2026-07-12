#!/usr/bin/env python3
"""Behavioral tests for retained-audio ear-audit reconciliation."""

from __future__ import annotations

import copy
import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "l2f-reconcile-earaudit.py"
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("l2f_reconcile_earaudit", SCRIPT)
assert SPEC and SPEC.loader
RECONCILE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RECONCILE)

ROOT = SCRIPT.parents[1]
TRACKED_LEDGER = ROOT / "TestFixtures/Corpus/Audits/rediff-r3-2026-07-07.jsonl"
TRACKED_REVIEW = ROOT / (
    "TestFixtures/Corpus/Audits/"
    "earaudit-boundary-review-2026-07-12-"
    "6b11a85754db5d1ea2fb0243b3b53fd2c61375e7ba54ad42c444102690b98e99.json"
)
TRACKED_REJECTS = ROOT / "TestFixtures/Corpus/Snapshots/audit-rejects.jsonl"
TRACKED_ARTIFACT_SHA256 = (
    "0d85a0ec8bfa30873bad63bbc4bb12a3f7613aca76d5b76149e25db2a0be226f"
)
TRACKED_ARTIFACT = ROOT / (
    "TestFixtures/Corpus/Evaluations/earaudit-partial-silver-"
    f"{TRACKED_ARTIFACT_SHA256}.json"
)


def fingerprint(character: str) -> str:
    return "sha256:" + character * 64


def ledger_row(
    episode_id: str,
    start: float,
    end: float,
    disposition: str,
    *,
    asset_fingerprint: str | None = None,
    duration: float = 100.0,
) -> dict:
    verdicts = {
        "tight_ad": ("ad", "Current bounds sounded tight; approve or mark unsure after review."),
        "boundary_off": ("boundary", "Rebound to the full ad break, checking both edges."),
        "edge_clipping": (
            "content",
            "Real DAI structure abuts this proposal; locate and rebound the full ad break.",
        ),
        "rejected": (
            "content",
            "Ear-verified isolated content; retained here for audit history.",
        ),
    }
    raw_verdict, recommendation = verdicts[disposition]
    return {
        "id": f"{episode_id}@{start:.2f}-{end:.2f}",
        "episode_id": episode_id,
        "show_name": "Example Show",
        "current_start_seconds": start,
        "current_end_seconds": end,
        "duration_seconds": duration,
        "disposition": disposition,
        "raw_verdict": raw_verdict,
        "recommendation": recommendation,
        "audio_fingerprint": asset_fingerprint or fingerprint("a"),
        "audit_date": "2026-07-07",
        "coordinate_origin": "fresh_b_proposal",
        "provenance_tier": "boundary_proposal",
        "retained_in_corpus": False,
        "review_asset_binding": "retained_snapshot_a_playback",
        "asset_binding_provenance": "test fixture",
        "note": "",
    }


def review_document(decisions: list[tuple[dict, str, float | None, float | None]]) -> dict:
    reviews: dict[str, dict] = {}
    history: list[dict] = []
    for sequence, (row, status, start, end) in enumerate(decisions, 1):
        stored = {
            "id": row["id"],
            "status": status,
            "proposed_start_seconds": start,
            "proposed_end_seconds": end,
            "note": "",
            "reviewer": "Reviewer",
            "audio_fingerprint": row["audio_fingerprint"],
            "reviewed_at": f"2026-07-12T00:{sequence:02d}:00Z",
        }
        reviews[row["id"]] = stored
        history.append(
            {
                "sequence": sequence,
                "id": row["id"],
                "before": None,
                "after": copy.deepcopy(stored),
                "at": stored["reviewed_at"],
            }
        )
    return {
        "schema_version": 2,
        "reviews": reviews,
        "history": history,
        "next_sequence": len(history) + 1,
    }


def reject_record(row: dict) -> dict:
    return {
        "id": row["id"],
        "ts": "2026-07-07T00:00:00Z",
        "episodeId": row["episode_id"],
        "audioFingerprint": row["audio_fingerprint"],
        "startSeconds": row["current_start_seconds"],
        "endSeconds": row["current_end_seconds"],
        "provenance": ["rediff", "retained_snapshot_a_ear_audit"],
        "reason": "ear-verified content",
        "disposition": "isolated_hallucination",
        "assetBindingProvenance": "test fixture",
    }


class ReconcileEarAuditTests(unittest.TestCase):
    def write_case(
        self,
        root: pathlib.Path,
        rows: list[dict],
        review: dict,
        rejects: list[dict] | None = None,
    ) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path, pathlib.Path, pathlib.Path]:
        ledger = root / "audit.jsonl"
        rejects_path = root / "rejects.jsonl"
        output = root / "evaluations"
        annotations = root / "annotations"
        annotations.mkdir()
        ledger.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
            encoding="utf-8",
        )
        review_bytes = (json.dumps(review, sort_keys=True) + "\n").encode("utf-8")
        review_sha256 = hashlib.sha256(review_bytes).hexdigest()
        review_path = root / f"earaudit-boundary-review-test-{review_sha256}.json"
        review_path.write_bytes(review_bytes)
        rejects_path.write_text(
            "".join(
                json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
                for row in (rejects or [])
            ),
            encoding="utf-8",
        )
        return ledger, review_path, rejects_path, output, annotations

    def reconcile_case(
        self,
        root: pathlib.Path,
        rows: list[dict],
        decisions: list[tuple[dict, str, float | None, float | None]],
        rejects: list[dict] | None = None,
    ):
        paths = self.write_case(root, rows, review_document(decisions), rejects)
        return RECONCILE.reconcile(
            ledger_path=paths[0],
            review_path=paths[1],
            rejects_path=paths[2],
            output_dir=paths[3],
            annotations_dir=paths[4],
        ), paths

    def test_tracked_fixture_reconciles_to_expected_partial_silver_slice(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            rejects = root / "audit-rejects.jsonl"
            ledger_rows = [
                json.loads(line) for line in TRACKED_LEDGER.read_text().splitlines()
            ]
            prior_identities = {
                (
                    row["episode_id"],
                    row["audio_fingerprint"],
                    float(row["current_start_seconds"]),
                    float(row["current_end_seconds"]),
                )
                for row in ledger_rows
                if row["disposition"] == "rejected"
            }
            prior_records = [
                record
                for line in TRACKED_REJECTS.read_text().splitlines()
                if line.strip()
                for record in [json.loads(line)]
                if (
                    record["episodeId"],
                    record["audioFingerprint"],
                    float(record["startSeconds"]),
                    float(record["endSeconds"]),
                )
                in prior_identities
            ]
            self.assertEqual(len(prior_records), 9)
            rejects.write_text(
                "".join(
                    json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
                    for record in prior_records
                )
            )
            output = root / "evaluations"
            annotations = root / "annotations"
            annotations.mkdir()

            result = RECONCILE.reconcile(
                ledger_path=TRACKED_LEDGER,
                review_path=TRACKED_REVIEW,
                rejects_path=rejects,
                output_dir=output,
                annotations_dir=annotations,
            )
            artifact_bytes = result.artifact_path.read_bytes()
            artifact = json.loads(artifact_bytes)

            self.assertEqual(
                artifact["sources"]["audit_ledger"]["sha256"],
                "9869e5734c421cfc95dbf7c691e461a689113d1b0d7e4a90ca007b151d25468b",
            )
            self.assertEqual(
                artifact["sources"]["ear_audit_review"]["sha256"],
                "6b11a85754db5d1ea2fb0243b3b53fd2c61375e7ba54ad42c444102690b98e99",
            )
            self.assertEqual(
                artifact["summary"],
                {
                    "assets": 27,
                    "boundary_reviews": 36,
                    "content_vetoes": 24,
                    "duplicate_full_break_groups": 1,
                    "full_break_assets": 14,
                    "full_breaks": 20,
                    "labeled_regions": 64,
                    "ledger_rows": 70,
                    "presence_anchors": 20,
                    "review_status_counts": {
                        "approved": 0,
                        "rebounded": 21,
                        "rejected": 15,
                    },
                    "tight_evidence_attached": 5,
                },
            )
            self.assertEqual(artifact["label_semantics"]["coverage"], "partial")
            self.assertEqual(artifact["label_semantics"]["quality"], "silver")
            self.assertEqual(
                artifact["label_semantics"]["unlabeled_audio"], "unknown_elsewhere"
            )
            self.assertIn(
                "only the exact interval is labeled",
                artifact["label_semantics"]["content_vetoes"],
            )
            self.assertIn(
                "overlapping promotion candidates",
                artifact["label_semantics"]["content_vetoes"],
            )
            self.assertEqual(len(artifact["assets"]), 27)

            mel = next(
                asset
                for asset in artifact["assets"]
                if asset["episode_id"].startswith("the-mel-robbins-podcast")
            )
            consensus = next(
                item
                for item in mel["full_breaks"]
                if item["start_seconds"] == 4958.1
            )
            self.assertEqual(consensus["end_seconds"], 5003.5)
            self.assertEqual(len(consensus["source_review_ids"]), 2)

            digest = hashlib.sha256(artifact_bytes).hexdigest()
            self.assertEqual(result.artifact_sha256, digest)
            self.assertEqual(result.artifact_path.name, f"earaudit-partial-silver-{digest}.json")
            self.assertTrue(artifact_bytes.endswith(b"\n"))
            self.assertTrue(result.rejects_changed)
            self.assertEqual(len(rejects.read_text().splitlines()), 24)
            loaded = RECONCILE.load_reject_ledger(rejects)
            self.assertEqual(sum(map(len, loaded.values())), 24)

            first_artifact = artifact_bytes
            first_rejects = rejects.read_bytes()
            again = RECONCILE.reconcile(
                ledger_path=TRACKED_LEDGER,
                review_path=TRACKED_REVIEW,
                rejects_path=rejects,
                output_dir=output,
                annotations_dir=annotations,
            )
            self.assertEqual(again.artifact_path, result.artifact_path)
            self.assertFalse(again.rejects_changed)
            self.assertEqual(again.artifact_path.read_bytes(), first_artifact)
            self.assertEqual(rejects.read_bytes(), first_rejects)
            self.assertEqual(len(list(output.glob("*.json"))), 1)

    def test_committed_artifact_is_the_exact_idempotent_default_output(self) -> None:
        artifact_before = TRACKED_ARTIFACT.read_bytes()
        rejects_before = TRACKED_REJECTS.read_bytes()

        result = RECONCILE.reconcile(dry_run=True)

        self.assertEqual(result.artifact_path, TRACKED_ARTIFACT)
        self.assertEqual(result.artifact_sha256, TRACKED_ARTIFACT_SHA256)
        self.assertFalse(result.artifact_changed)
        self.assertFalse(result.rejects_changed)
        self.assertEqual(TRACKED_ARTIFACT.read_bytes(), artifact_before)
        self.assertEqual(TRACKED_REJECTS.read_bytes(), rejects_before)
        self.assertEqual(hashlib.sha256(artifact_before).hexdigest(), TRACKED_ARTIFACT_SHA256)

    def test_duplicate_consensus_is_intersection_and_tight_evidence_does_not_widen_it(self) -> None:
        a = ledger_row("episode", 10, 20, "boundary_off")
        b = ledger_row("episode", 11, 21, "edge_clipping")
        touching = ledger_row("episode", 29.8, 35, "boundary_off")
        separated = ledger_row("episode", 36, 40, "edge_clipping")
        tight_a = ledger_row("episode", 8, 15, "tight_ad")
        tight_b = ledger_row("episode", 14, 25, "tight_ad")
        anchor = ledger_row("episode", 45, 50, "tight_ad")
        rows = [a, b, touching, separated, tight_a, tight_b, anchor]
        decisions = [
            (a, "rebounded", 10.0, 29.8),
            (b, "rebounded", 10.4, 29.8),
            (touching, "approved", 29.8, 35.0),
            (separated, "approved", 36.0, 40.0),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            result, _ = self.reconcile_case(pathlib.Path(tmp), rows, decisions)
            artifact = json.loads(result.artifact_path.read_text())
            asset = artifact["assets"][0]
            self.assertEqual(
                [(x["start_seconds"], x["end_seconds"]) for x in asset["full_breaks"]],
                [(10.4, 29.8), (29.8, 35.0), (36.0, 40.0)],
            )
            self.assertEqual(len(asset["full_breaks"][0]["source_review_ids"]), 2)
            self.assertEqual(
                asset["full_breaks"][0]["supporting_tight_ledger_ids"],
                sorted([tight_a["id"], tight_b["id"]]),
            )
            self.assertEqual(
                [(x["start_seconds"], x["end_seconds"]) for x in asset["presence_anchors"]],
                [(45.0, 50.0)],
            )

    def test_duplicate_endpoint_tolerance_uses_decimal_second_values(self) -> None:
        a = ledger_row("episode", 2, 3, "boundary_off")
        b = ledger_row("episode", 4, 5, "edge_clipping")
        with tempfile.TemporaryDirectory() as tmp:
            result, _ = self.reconcile_case(
                pathlib.Path(tmp),
                [a, b],
                [
                    (a, "rebounded", 0.6, 10.6),
                    (b, "rebounded", 1.1, 11.1),
                ],
            )
            full_breaks = json.loads(result.artifact_path.read_text())["assets"][0][
                "full_breaks"
            ]

            self.assertEqual(len(full_breaks), 1)
            self.assertEqual(
                (full_breaks[0]["start_seconds"], full_breaks[0]["end_seconds"]),
                (1.1, 10.6),
            )
            self.assertEqual(len(full_breaks[0]["source_review_ids"]), 2)

    def test_materially_different_overlapping_reviews_are_a_hard_conflict_without_writes(self) -> None:
        a = ledger_row("episode", 10, 20, "boundary_off")
        b = ledger_row("episode", 11, 21, "edge_clipping")
        review = review_document(
            [(a, "rebounded", 10, 30), (b, "rebounded", 20, 40)]
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            ledger, review_path, rejects, output, annotations = self.write_case(
                root, [a, b], review
            )
            original = rejects.read_bytes()
            with self.assertRaisesRegex(RECONCILE.ReconciliationError, "conflicting reviewed"):
                RECONCILE.reconcile(
                    ledger_path=ledger,
                    review_path=review_path,
                    rejects_path=rejects,
                    output_dir=output,
                    annotations_dir=annotations,
                )
            self.assertEqual(rejects.read_bytes(), original)
            self.assertFalse(output.exists())

    def test_tight_evidence_spanning_two_separate_breaks_is_ambiguous(self) -> None:
        a = ledger_row("episode", 10, 20, "boundary_off")
        b = ledger_row("episode", 30, 40, "edge_clipping")
        tight = ledger_row("episode", 15, 35, "tight_ad")
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RECONCILE.ReconciliationError, "multiple reviewed"):
                self.reconcile_case(
                    pathlib.Path(tmp),
                    [a, b, tight],
                    [(a, "approved", 10, 20), (b, "approved", 30, 40)],
                )

    def test_same_episode_assets_are_isolated_by_fingerprint(self) -> None:
        reviewed = ledger_row(
            "episode", 10, 20, "boundary_off", asset_fingerprint=fingerprint("a")
        )
        other = ledger_row(
            "episode", 15, 16, "tight_ad", asset_fingerprint=fingerprint("b")
        )
        with tempfile.TemporaryDirectory() as tmp:
            result, _ = self.reconcile_case(
                pathlib.Path(tmp), [reviewed, other], [(reviewed, "approved", 10, 20)]
            )
            assets = json.loads(result.artifact_path.read_text())["assets"]
            self.assertEqual(len(assets), 2)
            by_fingerprint = {asset["audio_fingerprint"]: asset for asset in assets}
            self.assertEqual(len(by_fingerprint[fingerprint("a")]["full_breaks"]), 1)
            self.assertEqual(len(by_fingerprint[fingerprint("a")]["presence_anchors"]), 0)
            self.assertEqual(len(by_fingerprint[fingerprint("b")]["full_breaks"]), 0)
            self.assertEqual(len(by_fingerprint[fingerprint("b")]["presence_anchors"]), 1)

    def test_review_coverage_status_and_binding_are_strict(self) -> None:
        boundary = ledger_row("episode", 10, 20, "boundary_off")
        tight = ledger_row("episode", 30, 40, "tight_ad")
        valid = review_document([(boundary, "approved", 10, 20)])
        cases: list[tuple[str, dict, str]] = []
        missing = review_document([])
        cases.append(("missing", missing, "coverage"))
        extra = review_document(
            [(boundary, "approved", 10, 20), (tight, "approved", 30, 40)]
        )
        cases.append(("extra", extra, "coverage"))
        unsure = review_document([(boundary, "unsure", 10, 20)])
        cases.append(("unsure", unsure, "complete status"))
        mismatched = copy.deepcopy(valid)
        mismatched["reviews"][boundary["id"]]["audio_fingerprint"] = fingerprint("f")
        mismatched["history"][0]["after"]["audio_fingerprint"] = fingerprint("f")
        cases.append(("fingerprint", mismatched, "fingerprint"))

        for name, review, message in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                paths = self.write_case(pathlib.Path(tmp), [boundary, tight], review)
                with self.assertRaisesRegex(RECONCILE.ReconciliationError, message):
                    RECONCILE.reconcile(
                        ledger_path=paths[0],
                        review_path=paths[1],
                        rejects_path=paths[2],
                        output_dir=paths[3],
                        annotations_dir=paths[4],
                    )

    def test_collection_valued_enums_are_controlled_validation_errors(self) -> None:
        boundary = ledger_row("episode", 10, 20, "boundary_off")
        malformed_ledger = copy.deepcopy(boundary)
        malformed_ledger["disposition"] = []
        malformed_review = review_document([(boundary, "approved", 10, 20)])
        malformed_review["reviews"][boundary["id"]]["status"] = {}
        malformed_review["history"][0]["after"]["status"] = {}

        cases = [
            ("ledger", [malformed_ledger], review_document([]), "disposition"),
            ("review", [boundary], malformed_review, "identity/status"),
        ]
        for name, rows, review, message in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                paths = self.write_case(pathlib.Path(tmp), rows, review)
                rejects_before = paths[2].read_bytes()

                with self.assertRaisesRegex(RECONCILE.ReconciliationError, message):
                    RECONCILE.reconcile(
                        ledger_path=paths[0],
                        review_path=paths[1],
                        rejects_path=paths[2],
                        output_dir=paths[3],
                        annotations_dir=paths[4],
                    )
                self.assertEqual(paths[2].read_bytes(), rejects_before)
                self.assertFalse(paths[3].exists())

    def test_invalid_bounds_rejected_bounds_and_history_are_rejected(self) -> None:
        boundary = ledger_row("episode", 10, 20, "boundary_off")
        cases: list[tuple[str, list[dict], dict, str]] = []
        nonfinite = copy.deepcopy(boundary)
        nonfinite["duration_seconds"] = float("nan")
        cases.append(
            (
                "nonfinite",
                [nonfinite],
                review_document([(nonfinite, "approved", 10, 20)]),
                "finite",
            )
        )
        outside = review_document([(boundary, "rebounded", 10, 101)])
        cases.append(("outside", [boundary], outside, "bounds"))
        changed_reject = review_document([(boundary, "rejected", 11, 20)])
        cases.append(("changed reject", [boundary], changed_reject, "original candidate"))
        broken_history = review_document([(boundary, "approved", 10, 20)])
        broken_history["history"][0]["before"] = copy.deepcopy(
            broken_history["history"][0]["after"]
        )
        cases.append(("history", [boundary], broken_history, "history"))

        nonfinite_review = review_document(
            [(boundary, "rebounded", float("nan"), 20)]
        )
        cases.append(("nonfinite review", [boundary], nonfinite_review, "finite"))
        overflowing = copy.deepcopy(boundary)
        overflowing["duration_seconds"] = 10**400
        cases.append(
            (
                "overflowing integer",
                [overflowing],
                review_document([(overflowing, "approved", 10, 20)]),
                "finite",
            )
        )
        invalid_timestamp = review_document([(boundary, "approved", 10, 20)])
        invalid_timestamp["reviews"][boundary["id"]]["reviewed_at"] = "not-a-timeZ"
        invalid_timestamp["history"][0]["after"]["reviewed_at"] = "not-a-timeZ"
        invalid_timestamp["history"][0]["at"] = "not-a-timeZ"
        cases.append(("invalid timestamp", [boundary], invalid_timestamp, "UTC timestamp"))

        for name, rows, review, message in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                paths = self.write_case(pathlib.Path(tmp), rows, review)
                with self.assertRaisesRegex(RECONCILE.ReconciliationError, message):
                    RECONCILE.reconcile(
                        ledger_path=paths[0],
                        review_path=paths[1],
                        rejects_path=paths[2],
                        output_dir=paths[3],
                        annotations_dir=paths[4],
                    )

    def test_invalid_audit_date_is_rejected_before_outputs(self) -> None:
        rejected = ledger_row("episode", 10, 20, "rejected")
        rejected["audit_date"] = "not-a-date"
        with tempfile.TemporaryDirectory() as tmp:
            paths = self.write_case(
                pathlib.Path(tmp), [rejected], review_document([])
            )
            rejects_before = paths[2].read_bytes()

            with self.assertRaisesRegex(RECONCILE.ReconciliationError, "calendar date"):
                RECONCILE.reconcile(
                    ledger_path=paths[0],
                    review_path=paths[1],
                    rejects_path=paths[2],
                    output_dir=paths[3],
                    annotations_dir=paths[4],
                )
            self.assertEqual(paths[2].read_bytes(), rejects_before)
            self.assertFalse(paths[3].exists())

    def test_every_history_state_is_valid_for_the_bound_ledger_row(self) -> None:
        boundary = ledger_row("episode", 10, 20, "boundary_off")
        final = review_document([(boundary, "approved", 10, 20)])

        invalid_states = []
        wrong_fingerprint = copy.deepcopy(final["reviews"][boundary["id"]])
        wrong_fingerprint["status"] = "unsure"
        wrong_fingerprint["audio_fingerprint"] = fingerprint("f")
        invalid_states.append(("fingerprint", wrong_fingerprint, "fingerprint"))

        outside_duration = copy.deepcopy(final["reviews"][boundary["id"]])
        outside_duration["status"] = "unsure"
        outside_duration["proposed_end_seconds"] = 101
        invalid_states.append(("duration", outside_duration, "duration"))

        edited_approval = copy.deepcopy(final["reviews"][boundary["id"]])
        edited_approval["proposed_start_seconds"] = 11
        invalid_states.append(("approval", edited_approval, "original candidate"))

        for name, invalid_state, message in invalid_states:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                review = copy.deepcopy(final)
                invalid_state["reviewed_at"] = "2026-07-12T00:00:00Z"
                final_state = copy.deepcopy(review["reviews"][boundary["id"]])
                final_state["reviewed_at"] = "2026-07-12T00:01:00Z"
                review["reviews"][boundary["id"]] = final_state
                review["history"] = [
                    {
                        "sequence": 1,
                        "id": boundary["id"],
                        "before": None,
                        "after": copy.deepcopy(invalid_state),
                        "at": invalid_state["reviewed_at"],
                    },
                    {
                        "sequence": 2,
                        "id": boundary["id"],
                        "before": copy.deepcopy(invalid_state),
                        "after": copy.deepcopy(final_state),
                        "at": final_state["reviewed_at"],
                    },
                ]
                review["next_sequence"] = 3
                paths = self.write_case(pathlib.Path(tmp), [boundary], review)
                with self.assertRaisesRegex(RECONCILE.ReconciliationError, message):
                    RECONCILE.reconcile(
                        ledger_path=paths[0],
                        review_path=paths[1],
                        rejects_path=paths[2],
                        output_dir=paths[3],
                        annotations_dir=paths[4],
                    )

    def test_ledger_bounds_must_be_exactly_represented_by_the_stable_id(self) -> None:
        row = ledger_row("episode", 10.001, 20, "tight_ad")
        with tempfile.TemporaryDirectory() as tmp:
            paths = self.write_case(pathlib.Path(tmp), [row], review_document([]))
            with self.assertRaisesRegex(RECONCILE.ReconciliationError, "exact identity"):
                RECONCILE.reconcile(
                    ledger_path=paths[0],
                    review_path=paths[1],
                    rejects_path=paths[2],
                    output_dir=paths[3],
                    annotations_dir=paths[4],
                )
            self.assertEqual(paths[2].read_bytes(), b"")
            self.assertFalse(paths[3].exists())

    def test_existing_content_addressed_artifact_must_match_its_name(self) -> None:
        row = ledger_row("episode", 10, 20, "tight_ad")
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            result, paths = self.reconcile_case(root, [row], [])
            result.artifact_path.write_text("{}\n", encoding="utf-8")
            rejects_before = paths[2].read_bytes()

            with self.assertRaisesRegex(
                RECONCILE.ReconciliationError, "content-addressed artifact"
            ):
                RECONCILE.reconcile(
                    ledger_path=paths[0],
                    review_path=paths[1],
                    rejects_path=paths[2],
                    output_dir=paths[3],
                    annotations_dir=paths[4],
                )
            self.assertEqual(paths[2].read_bytes(), rejects_before)

    def test_review_source_must_match_its_content_address(self) -> None:
        boundary = ledger_row("episode", 10, 20, "boundary_off")
        review = review_document([(boundary, "rejected", 10, 20)])
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            ledger, valid_review, rejects, output, annotations = self.write_case(
                root, [boundary], review
            )
            mislabeled_review = root / (
                "earaudit-boundary-review-test-" + "0" * 64 + ".json"
            )
            mislabeled_review.write_bytes(valid_review.read_bytes())
            rejects_before = rejects.read_bytes()

            with self.assertRaisesRegex(
                RECONCILE.ReconciliationError, "review.*content address"
            ):
                RECONCILE.reconcile(
                    ledger_path=ledger,
                    review_path=mislabeled_review,
                    rejects_path=rejects,
                    output_dir=output,
                    annotations_dir=annotations,
                )
            self.assertEqual(rejects.read_bytes(), rejects_before)
            self.assertFalse(output.exists())

    def test_unsafe_output_directory_fails_before_reject_publication(self) -> None:
        boundary = ledger_row("episode", 10, 20, "boundary_off")
        review = review_document([(boundary, "rejected", 10, 20)])
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            paths = self.write_case(root, [boundary], review)
            external = root / "external"
            external.mkdir()
            paths[3].symlink_to(external, target_is_directory=True)
            rejects_before = paths[2].read_bytes()

            with self.assertRaisesRegex(RECONCILE.ReconciliationError, "output directory"):
                RECONCILE.reconcile(
                    ledger_path=paths[0],
                    review_path=paths[1],
                    rejects_path=paths[2],
                    output_dir=paths[3],
                    annotations_dir=paths[4],
                )
            self.assertEqual(paths[2].read_bytes(), rejects_before)
            self.assertEqual(list(external.iterdir()), [])

    def test_dangling_artifact_symlink_fails_before_reject_publication(self) -> None:
        boundary = ledger_row("episode", 10, 20, "boundary_off")
        review = review_document([(boundary, "rejected", 10, 20)])
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            paths = self.write_case(root, [boundary], review)
            dry_run = RECONCILE.reconcile(
                ledger_path=paths[0],
                review_path=paths[1],
                rejects_path=paths[2],
                output_dir=paths[3],
                annotations_dir=paths[4],
                dry_run=True,
            )
            paths[3].mkdir()
            dry_run.artifact_path.symlink_to(root / "missing-artifact.json")
            rejects_before = paths[2].read_bytes()

            with self.assertRaisesRegex(RECONCILE.ReconciliationError, "artifact path"):
                RECONCILE.reconcile(
                    ledger_path=paths[0],
                    review_path=paths[1],
                    rejects_path=paths[2],
                    output_dir=paths[3],
                    annotations_dir=paths[4],
                )
            self.assertEqual(paths[2].read_bytes(), rejects_before)
            self.assertTrue(dry_run.artifact_path.is_symlink())

    def test_non_directory_output_ancestor_fails_before_reject_publication(self) -> None:
        boundary = ledger_row("episode", 10, 20, "boundary_off")
        review = review_document([(boundary, "rejected", 10, 20)])
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            ledger, review_path, rejects, _, annotations = self.write_case(
                root, [boundary], review
            )
            blocked = root / "not-a-directory"
            blocked.write_text("blocked\n", encoding="utf-8")
            output = blocked / "evaluations"
            rejects_before = rejects.read_bytes()

            with self.assertRaisesRegex(RECONCILE.ReconciliationError, "output directory"):
                RECONCILE.reconcile(
                    ledger_path=ledger,
                    review_path=review_path,
                    rejects_path=rejects,
                    output_dir=output,
                    annotations_dir=annotations,
                )
            self.assertEqual(rejects.read_bytes(), rejects_before)

    def test_artifact_staging_failure_precedes_reject_publication(self) -> None:
        boundary = ledger_row("episode", 10, 20, "boundary_off")
        review = review_document([(boundary, "rejected", 10, 20)])
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            paths = self.write_case(root, [boundary], review)
            rejects_before = paths[2].read_bytes()
            real_named_temporary = RECONCILE.tempfile.NamedTemporaryFile

            def fail_artifact_stage(*args, **kwargs):
                if pathlib.Path(kwargs["dir"]) == paths[3]:
                    raise PermissionError("simulated read-only artifact directory")
                return real_named_temporary(*args, **kwargs)

            with mock.patch.object(
                RECONCILE.tempfile,
                "NamedTemporaryFile",
                side_effect=fail_artifact_stage,
            ):
                with self.assertRaisesRegex(RECONCILE.ReconciliationError, "cannot stage"):
                    RECONCILE.reconcile(
                        ledger_path=paths[0],
                        review_path=paths[1],
                        rejects_path=paths[2],
                        output_dir=paths[3],
                        annotations_dir=paths[4],
                    )
            self.assertEqual(paths[2].read_bytes(), rejects_before)
            self.assertEqual(list(paths[3].iterdir()), [])

    def test_publication_acquires_canonical_lock_before_reject_lock(self) -> None:
        row = ledger_row("episode", 10, 20, "tight_ad")
        events: list[str] = []

        @contextlib.contextmanager
        def canonical_lock(_path: pathlib.Path):
            events.append("canonical-enter")
            try:
                yield
            finally:
                events.append("canonical-exit")

        @contextlib.contextmanager
        def reject_lock(_path: pathlib.Path):
            self.assertEqual(events, ["canonical-enter"])
            events.append("reject-enter")
            try:
                yield
            finally:
                events.append("reject-exit")

        with tempfile.TemporaryDirectory() as tmp:
            paths = self.write_case(pathlib.Path(tmp), [row], review_document([]))
            with mock.patch.object(
                RECONCILE, "canonical_manifest_lock", side_effect=canonical_lock
            ), mock.patch.object(
                RECONCILE, "_reject_transaction", side_effect=reject_lock
            ):
                RECONCILE.reconcile(
                    ledger_path=paths[0],
                    review_path=paths[1],
                    rejects_path=paths[2],
                    output_dir=paths[3],
                    annotations_dir=paths[4],
                )

        self.assertEqual(
            events,
            ["canonical-enter", "reject-enter", "reject-exit", "canonical-exit"],
        )

    def test_reject_lock_release_cannot_report_failure_after_commit(self) -> None:
        boundary = ledger_row("episode", 10, 20, "edge_clipping")
        review = review_document([(boundary, "rejected", 10, 20)])
        operations: list[int] = []
        real_flock = RECONCILE.fcntl.flock

        def fail_explicit_unlock(descriptor: int, operation: int) -> None:
            operations.append(operation)
            if operation & RECONCILE.fcntl.LOCK_UN:
                raise OSError("injected unlock failure")
            real_flock(descriptor, operation)

        with tempfile.TemporaryDirectory() as tmp:
            paths = self.write_case(pathlib.Path(tmp), [boundary], review)
            with mock.patch.object(
                RECONCILE,
                "_canonical_publication_transaction",
                return_value=contextlib.nullcontext(),
            ), mock.patch.object(
                RECONCILE.fcntl,
                "flock",
                side_effect=fail_explicit_unlock,
            ):
                result = RECONCILE.reconcile(
                    ledger_path=paths[0],
                    review_path=paths[1],
                    rejects_path=paths[2],
                    output_dir=paths[3],
                    annotations_dir=paths[4],
                )

            self.assertEqual(operations, [RECONCILE.fcntl.LOCK_EX])
            self.assertEqual(len(paths[2].read_text().splitlines()), 1)
            self.assertTrue(result.artifact_path.is_file())
            descriptor = RECONCILE.os.open(
                paths[2].parent / f".{paths[2].name}.lock",
                RECONCILE.os.O_RDWR,
            )
            try:
                real_flock(
                    descriptor,
                    RECONCILE.fcntl.LOCK_EX | RECONCILE.fcntl.LOCK_NB,
                )
            finally:
                RECONCILE.os.close(descriptor)

    def test_canonical_lock_release_cannot_report_failure_after_commit(self) -> None:
        boundary = ledger_row("episode", 10, 20, "edge_clipping")
        review = review_document([(boundary, "rejected", 10, 20)])
        operations: list[int] = []
        real_flock = RECONCILE.fcntl.flock

        def fail_explicit_unlock(descriptor: int, operation: int) -> None:
            operations.append(operation)
            if operation & RECONCILE.fcntl.LOCK_UN:
                raise OSError("injected unlock failure")
            real_flock(descriptor, operation)

        with tempfile.TemporaryDirectory() as tmp:
            paths = self.write_case(pathlib.Path(tmp), [boundary], review)
            with mock.patch.object(
                RECONCILE,
                "_reject_transaction",
                return_value=contextlib.nullcontext(),
            ), mock.patch.object(
                RECONCILE.fcntl,
                "flock",
                side_effect=fail_explicit_unlock,
            ):
                result = RECONCILE.reconcile(
                    ledger_path=paths[0],
                    review_path=paths[1],
                    rejects_path=paths[2],
                    output_dir=paths[3],
                    annotations_dir=paths[4],
                )

            self.assertEqual(operations, [RECONCILE.fcntl.LOCK_EX])
            self.assertEqual(len(paths[2].read_text().splitlines()), 1)
            self.assertTrue(result.artifact_path.is_file())
            descriptor = RECONCILE.os.open(
                paths[4] / ".canonical-manifest.lock",
                RECONCILE.os.O_RDWR,
            )
            try:
                real_flock(
                    descriptor,
                    RECONCILE.fcntl.LOCK_EX | RECONCILE.fcntl.LOCK_NB,
                )
            finally:
                RECONCILE.os.close(descriptor)

    def test_failed_reject_verification_restores_original_before_withholding_artifact(
        self,
    ) -> None:
        boundary = ledger_row("episode", 10, 20, "boundary_off")
        review = review_document([(boundary, "rejected", 10, 20)])
        unrelated = ledger_row("other", 30, 40, "rejected")
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            paths = self.write_case(root, [boundary], review)
            wrong_bytes = RECONCILE._reject_jsonl_bytes([reject_record(unrelated)])
            original_bytes = paths[2].read_bytes()
            real_atomic_write = RECONCILE._atomic_write
            publication_attempts = 0

            def publish_wrong_once(path: pathlib.Path, data: bytes) -> None:
                nonlocal publication_attempts
                publication_attempts += 1
                if publication_attempts == 1:
                    path.write_bytes(wrong_bytes)
                    return
                real_atomic_write(path, data)

            with mock.patch.object(
                RECONCILE, "_atomic_write", side_effect=publish_wrong_once
            ):
                with self.assertRaisesRegex(
                    RECONCILE.ReconciliationError, "published reject-ledger bytes"
                ):
                    RECONCILE.reconcile(
                        ledger_path=paths[0],
                        review_path=paths[1],
                        rejects_path=paths[2],
                        output_dir=paths[3],
                        annotations_dir=paths[4],
                    )
            self.assertEqual(publication_attempts, 2)
            self.assertEqual(paths[2].read_bytes(), original_bytes)
            self.assertEqual(list(paths[3].iterdir()), [])

    def test_dry_run_reports_only_its_persistent_coordination_lock(self) -> None:
        row = ledger_row("episode", 10, 20, "tight_ad")
        with tempfile.TemporaryDirectory() as tmp:
            paths = self.write_case(pathlib.Path(tmp), [row], review_document([]))
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = RECONCILE.main(
                    [
                        "--ledger",
                        str(paths[0]),
                        "--review",
                        str(paths[1]),
                        "--rejects-file",
                        str(paths[2]),
                        "--output-dir",
                        str(paths[3]),
                        "--dry-run",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("dry-run: no corpus data files changed", stdout.getvalue())
            self.assertFalse(paths[3].exists())
            self.assertTrue((paths[2].parent / f".{paths[2].name}.lock").is_file())

    def test_duplicate_ledger_identity_is_rejected(self) -> None:
        row = ledger_row("episode", 10, 20, "tight_ad")
        with tempfile.TemporaryDirectory() as tmp:
            paths = self.write_case(pathlib.Path(tmp), [row, copy.deepcopy(row)], review_document([]))
            with self.assertRaisesRegex(RECONCILE.ReconciliationError, "duplicate ledger"):
                RECONCILE.reconcile(
                    ledger_path=paths[0],
                    review_path=paths[1],
                    rejects_path=paths[2],
                    output_dir=paths[3],
                    annotations_dir=paths[4],
                )

    def test_duplicate_json_object_fields_are_rejected_in_every_input(self) -> None:
        row = ledger_row("episode", 10, 20, "tight_ad")
        review = review_document([])
        reject = reject_record(ledger_row("other", 30, 40, "rejected"))
        duplicate_inputs = [
            (
                "ledger",
                lambda: RECONCILE.load_ledger(
                    (json.dumps(row)[:-1] + f',"id":{json.dumps(row["id"])}' + "}").encode()
                ),
            ),
            (
                "review",
                lambda: RECONCILE.load_review(
                    (
                        json.dumps(review)[:-1]
                        + ',"schema_version":2}'
                    ).encode(),
                    [row],
                ),
            ),
            (
                "reject ledger",
                lambda: RECONCILE._load_reject_records(
                    (
                        json.dumps(reject)[:-1]
                        + f',"episodeId":{json.dumps(reject["episodeId"])}'
                        + "}"
                    ).encode()
                ),
            ),
        ]

        for name, load in duplicate_inputs:
            with self.subTest(name=name):
                with self.assertRaisesRegex(RECONCILE.ReconciliationError, "duplicate JSON"):
                    load()

    def test_deeply_nested_json_is_controlled_without_writes(self) -> None:
        row = ledger_row("episode", 10, 20, "tight_ad")
        with tempfile.TemporaryDirectory() as tmp:
            paths = self.write_case(
                pathlib.Path(tmp), [row], review_document([])
            )
            paths[0].write_text("[" * 2000 + "0" + "]" * 2000 + "\n")
            rejects_before = paths[2].read_bytes()

            with self.assertRaisesRegex(RECONCILE.ReconciliationError, "invalid.*JSON"):
                RECONCILE.reconcile(
                    ledger_path=paths[0],
                    review_path=paths[1],
                    rejects_path=paths[2],
                    output_dir=paths[3],
                    annotations_dir=paths[4],
                )
            self.assertEqual(paths[2].read_bytes(), rejects_before)
            self.assertFalse(paths[3].exists())

    def test_overflowing_active_reject_bounds_are_controlled_without_writes(self) -> None:
        row = ledger_row("episode", 10, 20, "tight_ad")
        malformed_reject = reject_record(ledger_row("other", 30, 40, "rejected"))
        malformed_reject["startSeconds"] = 10**400
        malformed_reject["endSeconds"] = 10**401

        with tempfile.TemporaryDirectory() as tmp:
            paths = self.write_case(
                pathlib.Path(tmp),
                [row],
                review_document([]),
                [malformed_reject],
            )
            rejects_before = paths[2].read_bytes()

            with self.assertRaisesRegex(RECONCILE.ReconciliationError, "finite number"):
                RECONCILE.reconcile(
                    ledger_path=paths[0],
                    review_path=paths[1],
                    rejects_path=paths[2],
                    output_dir=paths[3],
                    annotations_dir=paths[4],
                )
            self.assertEqual(paths[2].read_bytes(), rejects_before)
            self.assertFalse(paths[3].exists())

    def test_nonfinite_active_reject_metadata_is_controlled_without_writes(self) -> None:
        row = ledger_row("episode", 10, 20, "tight_ad")
        active_reject = reject_record(ledger_row("other", 30, 40, "rejected"))

        with tempfile.TemporaryDirectory() as tmp:
            paths = self.write_case(
                pathlib.Path(tmp),
                [row],
                review_document([]),
            )
            encoded = json.dumps(active_reject, sort_keys=True)[:-1]
            paths[2].write_text(encoded + ', "metadata": 1e400}\n', encoding="utf-8")
            rejects_before = paths[2].read_bytes()

            with self.assertRaisesRegex(RECONCILE.ReconciliationError, "finite JSON number"):
                RECONCILE.reconcile(
                    ledger_path=paths[0],
                    review_path=paths[1],
                    rejects_path=paths[2],
                    output_dir=paths[3],
                    annotations_dir=paths[4],
                )
            self.assertEqual(paths[2].read_bytes(), rejects_before)
            self.assertFalse(paths[3].exists())

    def test_positive_veto_overlap_is_rejected_before_publication_but_touching_is_allowed(self) -> None:
        boundary = ledger_row("episode", 10, 20, "boundary_off")
        overlapping = ledger_row("episode", 15, 25, "rejected")
        touching = ledger_row("episode", 20, 25, "rejected")
        decisions = [(boundary, "approved", 10, 20)]

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            paths = self.write_case(
                root,
                [boundary, overlapping],
                review_document(decisions),
                [reject_record(overlapping)],
            )
            before = paths[2].read_bytes()
            with self.assertRaisesRegex(RECONCILE.ReconciliationError, "positive.*veto"):
                RECONCILE.reconcile(
                    ledger_path=paths[0],
                    review_path=paths[1],
                    rejects_path=paths[2],
                    output_dir=paths[3],
                    annotations_dir=paths[4],
                )
            self.assertEqual(paths[2].read_bytes(), before)
            self.assertFalse(paths[3].exists())

        with tempfile.TemporaryDirectory() as tmp:
            result, _ = self.reconcile_case(
                pathlib.Path(tmp),
                [boundary, touching],
                decisions,
                [reject_record(touching)],
            )
            self.assertEqual(
                json.loads(result.artifact_path.read_text())["summary"]["content_vetoes"], 1
            )

    def test_active_veto_cannot_contradict_attached_tight_evidence(self) -> None:
        boundary = ledger_row("episode", 10, 20, "boundary_off")
        tight = ledger_row("episode", 5, 15, "tight_ad")
        external_veto = ledger_row("episode", 5, 7, "rejected")
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RECONCILE.ReconciliationError, "positive.*veto"):
                self.reconcile_case(
                    pathlib.Path(tmp),
                    [boundary, tight],
                    [(boundary, "approved", 10, 20)],
                    [reject_record(external_veto)],
                )


if __name__ == "__main__":
    unittest.main()
