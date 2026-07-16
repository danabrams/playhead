#!/usr/bin/env python3
"""Tests for the oracle-gold promotion and scoring scripts (playhead-l2f.10)."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import pathlib
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PROMOTE = _load("l2f_promote_oracle_earaudit", "l2f-promote-oracle-earaudit.py")
SCORE = _load("l2f_score_oracle_gold", "l2f-score-oracle-gold.py")

MERGE_EPISODE = PROMOTE.MERGE_GROUPS[0]["episode_id"]
FP = "sha256:" + "ab" * 32
FP2 = "sha256:" + "cd" * 32


def _ledger_row(row_id, episode, start, end, duration, fingerprint=FP):
    return {
        "id": row_id,
        "episode_id": episode,
        "current_start_seconds": start,
        "current_end_seconds": end,
        "duration_seconds": duration,
        "show_name": "Show",
        "disposition": "d",
        "recommendation": "r",
        "raw_verdict": "unreviewed",
        "provenance_tier": "oracle-emission",
        "audio_fingerprint": fingerprint,
    }


def _review(status, start=None, end=None):
    entry = {"status": status, "note": ""}
    if start is not None:
        entry["proposed_start_seconds"] = start
        entry["proposed_end_seconds"] = end
    return entry


class PromotionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)

    def _write_inputs(self, ledger_rows, reviews):
        ledger = self.root / "ledger.jsonl"
        review = self.root / "review.json"
        ledger.write_text(
            "\n".join(json.dumps(r) for r in ledger_rows) + "\n", encoding="utf-8"
        )
        review.write_text(
            json.dumps({"reviews": reviews, "schema_version": 2}), encoding="utf-8"
        )
        return ledger, review

    def _fixture(self):
        starts = PROMOTE.MERGE_GROUPS[0]["member_starts"]
        rows = [
            _ledger_row("plain", "other-show-ep", 100.0, 190.0, 900.0),
            _ledger_row("nikki", "the-nikki-glaser-podcast-x", 50.0, 140.0, 900.0),
            _ledger_row("rej", "reject-show-ep", 10.0, 40.0, 900.0, FP2),
            _ledger_row("m1", MERGE_EPISODE, starts[0], starts[0] + 89.5, 5000.0),
            _ledger_row("m2", MERGE_EPISODE, starts[1], starts[1] + 15.0, 5000.0),
            _ledger_row("m3", MERGE_EPISODE, starts[2], starts[2] + 63.8, 5000.0),
        ]
        reviews = {
            "plain": _review("rebounded", 98.0, 195.0),
            "nikki": _review("rebounded", 49.0, 141.0),
            "rej": _review("rejected"),
            "m1": _review("rebounded", 1150.0, 1350.0),
            "m2": _review("rebounded", 1273.0, 1289.0),
            "m3": _review("rebounded", 1290.0, 1355.0),
        }
        return rows, reviews

    def test_promotion_merges_vetoes_and_tolerances(self):
        ledger, review = self._write_inputs(*self._fixture())
        rows = PROMOTE.apply_merges(PROMOTE.load_rows(ledger, review))
        artifact = PROMOTE.build_artifact(rows, {"ledger": "l", "review": "r"})
        self.assertEqual(artifact["summary"]["full_breaks"], 3)
        self.assertEqual(artifact["summary"]["content_vetoes"], 1)
        merged_asset = next(
            a for a in artifact["assets"] if a["episode_id"] == MERGE_EPISODE
        )
        merged = merged_asset["full_breaks"][0]
        self.assertEqual(merged["start_seconds"], 1150.0)
        self.assertEqual(merged["end_seconds"], 1350.0)
        self.assertEqual(len(merged["source_ledger_ids"]), 3)
        nikki = next(
            a for a in artifact["assets"] if "nikki" in a["episode_id"]
        )
        tolerances = {
            b["boundary_tolerance_seconds"]
            for a in artifact["assets"]
            for b in a["full_breaks"]
        }
        self.assertEqual(tolerances, {0.3, 0.5})
        self.assertEqual(
            nikki["full_breaks"][0]["boundary_tolerance_seconds"], 0.5
        )
        veto_asset = next(
            a for a in artifact["assets"] if a["episode_id"] == "reject-show-ep"
        )
        self.assertEqual(veto_asset["full_breaks"], [])
        self.assertEqual(veto_asset["content_vetoes"][0]["start_seconds"], 10.0)
        self.assertEqual(artifact["label_semantics"]["quality"], "gold")

    def test_content_addressed_name_matches_bytes(self):
        ledger, review = self._write_inputs(*self._fixture())
        rows = PROMOTE.apply_merges(PROMOTE.load_rows(ledger, review))
        artifact = PROMOTE.build_artifact(rows, {})
        data, digest = PROMOTE.content_addressed_bytes(artifact)
        self.assertEqual(hashlib.sha256(data).hexdigest(), digest)

    def test_approved_rows_promote_at_current_bounds(self):
        rows = [_ledger_row("ok", "approve-show-ep", 40.0, 100.0, 900.0)]
        reviews = {"ok": _review("approved")}
        ledger, review = self._write_inputs(rows, reviews)
        loaded = PROMOTE.load_rows(ledger, review)
        artifact = PROMOTE.build_artifact(loaded, {})
        asset = artifact["assets"][0]
        self.assertEqual(
            (asset["full_breaks"][0]["start_seconds"], asset["full_breaks"][0]["end_seconds"]),
            (40.0, 100.0),
        )
        self.assertEqual(artifact["summary"]["review_status_counts"]["approved"], 1)

    def test_near_duplicate_breaks_dedupe_with_union_provenance(self):
        rows = [
            _ledger_row("first", "dupe-show-ep", 100.0, 190.0, 900.0),
            _ledger_row("second", "dupe-show-ep", 100.4, 190.6, 900.0),
            _ledger_row("distinct", "dupe-show-ep", 400.0, 460.0, 900.0),
        ]
        reviews = {
            "first": _review("rebounded", 99.0, 189.0),
            "second": _review("rebounded", 99.2, 189.8),
            "distinct": _review("rebounded", 401.0, 459.0),
        }
        ledger, review = self._write_inputs(rows, reviews)
        artifact = PROMOTE.build_artifact(PROMOTE.load_rows(ledger, review), {})
        breaks = artifact["assets"][0]["full_breaks"]
        self.assertEqual(len(breaks), 2, "bounds within 1.0s are one break")
        merged = breaks[0]
        self.assertEqual(merged["start_seconds"], 99.0, "primary-audit bounds win")
        self.assertEqual(sorted(merged["source_review_ids"]), ["first", "second"])
        self.assertEqual(artifact["summary"]["full_breaks"], 2)

    def test_review_ledger_mismatch_rejected(self):
        rows, reviews = self._fixture()
        del reviews["plain"]
        ledger, review = self._write_inputs(rows, reviews)
        with self.assertRaises(PROMOTE.PromotionError):
            PROMOTE.load_rows(ledger, review)


class ScoringTests(unittest.TestCase):
    def _evaluation(self):
        return {
            "artifact_kind": "oracle_earaudit_gold_boundary_evaluation",
            "assets": [
                {
                    "audio_fingerprint": FP,
                    "content_vetoes": [
                        {
                            "start_seconds": 500.0,
                            "end_seconds": 530.0,
                            "source_ledger_ids": ["v"],
                            "source_review_ids": ["v"],
                        }
                    ],
                    "duration_seconds": 900.0,
                    "episode_id": "ep-1",
                    "full_breaks": [
                        {
                            "boundary_tolerance_seconds": 0.3,
                            "start_seconds": 100.0,
                            "end_seconds": 190.0,
                            "source_ledger_ids": ["a", "b"],
                            "source_review_ids": ["a"],
                        }
                    ],
                    "presence_anchors": [],
                    "show_name": "Show",
                }
            ],
            "label_semantics": {"quality": "gold"},
            "schema_version": 1,
            "sources": {},
            "summary": {},
        }

    def test_overlap_match_and_veto_hit(self):
        predictions = {
            "ep-1": [
                {"id": "p1", "start": 104.0, "end": 170.0, "fingerprint": FP},
                {"id": "p2", "start": 505.0, "end": 512.0, "fingerprint": FP},
            ]
        }
        report = SCORE.score(self._evaluation(), predictions)
        self.assertEqual(report["matched_breaks"], 1)
        self.assertEqual(report["missed_breaks"], 0)
        self.assertEqual(report["start_error_seconds"]["p50"], 3.7)
        self.assertEqual(report["end_error_seconds"]["p50"], 19.7)
        self.assertEqual(len(report["veto_hits"]), 1)
        self.assertEqual(report["veto_hits"][0]["enclosed_seconds"], 7.0)

    def test_provenance_match_unions_source_slots(self):
        predictions = {
            "ep-1": [
                {"id": "a", "start": 99.0, "end": 150.0, "fingerprint": FP},
                {"id": "b", "start": 160.0, "end": 189.0, "fingerprint": FP},
                {"id": "z", "start": 100.2, "end": 189.9, "fingerprint": FP},
            ]
        }
        report = SCORE.score(self._evaluation(), predictions, match="provenance")
        matched = report["matched_breaks"]
        self.assertEqual(matched, 1)
        # union of a+b = 99.0-189.0 -> raw errors 1.0 / 1.0, tol 0.3 -> 0.7
        self.assertEqual(report["start_error_seconds"]["p50"], 0.7)
        self.assertEqual(report["end_error_seconds"]["p50"], 0.7)

    def test_missed_break_and_within_tolerance(self):
        evaluation = self._evaluation()
        predictions = {"ep-1": [{"id": "p", "start": 99.9, "end": 190.2, "fingerprint": FP}]}
        report = SCORE.score(evaluation, predictions)
        self.assertEqual(report["within_tolerance"], 1)
        report_missed = SCORE.score(evaluation, {"ep-1": []})
        self.assertEqual(report_missed["missed_breaks"], 1)
        self.assertEqual(report_missed["matched_breaks"], 0)

    def test_fingerprint_mismatch_raises(self):
        predictions = {
            "ep-1": [{"id": "p", "start": 100.0, "end": 190.0, "fingerprint": FP2}]
        }
        with self.assertRaises(SCORE.ScoringError):
            SCORE.score(self._evaluation(), predictions)


if __name__ == "__main__":
    unittest.main()
