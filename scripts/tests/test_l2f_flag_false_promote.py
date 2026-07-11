#!/usr/bin/env python3
"""Regression tests for false-promotion demotion and partition repair."""

from __future__ import annotations

import importlib.util
import hashlib
import json
import pathlib
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "l2f-flag-false-promote.py"
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("l2f_flag_false_promote", SCRIPT)
assert SPEC and SPEC.loader
DEMOTE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DEMOTE)


def ad(start: float, end: float, name: str) -> dict:
    return {
        "start_seconds": start,
        "end_seconds": end,
        "advertiser": name,
        "product": "product",
        "ad_type": "dai",
        "transition_type": None,
        "confidence_notes": "keep metadata",
        "provenance": ["rediff"],
        "auto_promoted": True,
        "audit_priority": 1,
    }


def canonical_annotation(
    episode_id: str,
    ad_windows: list[dict],
    content_windows: list[dict] | None = None,
    **extra: object,
) -> dict:
    if content_windows is None:
        content_windows = DEMOTE.derive_content_windows(ad_windows, 100)
    return {
        "episode_id": episode_id,
        "show_name": "Example",
        "duration_seconds": 100,
        "ad_windows": ad_windows,
        "content_windows": content_windows,
        "variant_of": None,
        "audio_fingerprint": "sha256:"
        + hashlib.sha256(episode_id.encode("utf-8")).hexdigest(),
        **extra,
    }


def write_manifest(annotations: pathlib.Path, *names: str) -> None:
    (annotations / DEMOTE.MANIFEST_FILENAME).write_text(
        json.dumps({"schema_version": 1, "annotations": list(names)}),
        encoding="utf-8",
    )


class DemotionTests(unittest.TestCase):
    def test_canonical_repairs_are_explicit_fillers_not_rewritten_content(self) -> None:
        annotations = SCRIPT.parents[1] / "TestFixtures/Corpus/Annotations"
        expected_fillers = {
            ("fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros", 3162.55, 3196.13),
            ("smartless-2026-05-11-quot-kareem-rahma-quot", 1180.08, 1351.54),
            ("the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve", 718.3, 738.75),
            ("the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve", 879.81, 904.97),
            ("the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve", 1694.41, 1726.39),
            ("the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve", 2236.58, 2267.81),
            ("the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol", 2643.0, 2805.03),
            ("the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol", 2854.34, 3211.1),
            ("the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t", 1098.7, 1543.3),
            ("the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev", 3095.99, 3127.82),
            ("the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev", 3155.7, 3224.95),
            ("the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m", 3043.36, 3209.74),
            ("why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi", 3771.23, 3791.92),
            ("why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with", 91.69, 343.94),
            ("why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with", 344.94, 599.18),
            ("why-is-this-happening-the-chris-hayes-po-2026-05-26-the-ai-end-game-the-ethics-of-ai-with-ti", 83.76, 195.03),
        }
        actual_fillers: set[tuple[str, float, float]] = set()
        for path in annotations.glob("*.json"):
            if path.name.startswith("_"):
                continue
            annotation = json.loads(path.read_text())
            for window in annotation["content_windows"]:
                if window.get("notes") == DEMOTE.CONTENT_NOTE:
                    actual_fillers.add((
                        annotation["episode_id"],
                        float(window["start_seconds"]),
                        float(window["end_seconds"]),
                    ))

        self.assertEqual(actual_fillers, set())
        quarantine = SCRIPT.parents[1] / "TestFixtures/Corpus/Quarantine/fresh-b-coordinate-annotations"
        quarantined_fillers: set[tuple[str, float, float]] = set()
        for path in quarantine.glob("*.json"):
            annotation = json.loads(path.read_text())
            for window in annotation["content_windows"]:
                if window.get("notes") == DEMOTE.CONTENT_NOTE:
                    quarantined_fillers.add((
                        annotation["episode_id"],
                        float(window["start_seconds"]),
                        float(window["end_seconds"]),
                    ))
        self.assertEqual(quarantined_fillers, expected_fillers)

    def test_tracked_reject_ledger_preserves_history_and_nine_audit_scrubs(self) -> None:
        ledger = SCRIPT.parents[1] / "TestFixtures/Corpus/Snapshots/audit-rejects.jsonl"
        records = [json.loads(line) for line in ledger.read_text().splitlines() if line.strip()]
        identities = [
            (row["episodeId"], float(row["startSeconds"]), float(row["endSeconds"]))
            for row in records
        ]
        expected_new_ids = {
            "fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros@3162.55-3196.13",
            "the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve@718.30-738.75",
            "the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve@879.81-904.97",
            "the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve@1694.41-1726.39",
            "the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve@2236.58-2267.81",
            "the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev@3095.99-3127.82",
            "the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev@3155.70-3224.95",
            "why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi@3771.23-3791.92",
            "why-is-this-happening-the-chris-hayes-po-2026-05-26-the-ai-end-game-the-ethics-of-ai-with-ti@83.76-195.03",
        }

        self.assertEqual(len(records), 9)
        self.assertEqual(len(set(identities)), 9)
        self.assertTrue(all(row["audioFingerprint"].startswith("sha256:") for row in records))
        self.assertTrue(all("assetBindingProvenance" in row for row in records))
        self.assertEqual(
            {row.get("id") for row in records if row.get("disposition") == "isolated_hallucination"},
            expected_new_ids,
        )

    def test_complement_handles_leading_middle_trailing_and_adjacent_ads(self) -> None:
        windows = [ad(0, 10, "a"), ad(20, 30, "b"), ad(30, 40, "c"), ad(90, 100, "d")]
        self.assertEqual(
            DEMOTE.derive_content_windows(windows, 100),
            [
                {"start_seconds": 10.0, "end_seconds": 20.0,
                 "notes": DEMOTE.CONTENT_NOTE},
                {"start_seconds": 40.0, "end_seconds": 90.0,
                 "notes": DEMOTE.CONTENT_NOTE},
            ],
        )

    def test_demotion_preserves_remaining_ad_metadata_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            rejects = root / "rejects.jsonl"
            path = annotations / "episode.json"
            original_ads = [ad(0, 10, "a"), ad(20, 30, "b"), ad(90, 100, "d")]
            path.write_text(json.dumps(canonical_annotation(
                "episode",
                original_ads,
                custom_metadata={"must": "survive"},
            )))
            write_manifest(annotations, "episode.json")

            result = DEMOTE.demote(
                annotations_dir=annotations,
                rejects_path=rejects,
                episode_prefix="episode",
                requested_start=20,
                reason="ear-verified content",
                reviewed_at="2026-07-07T00:00:00Z",
            )
            self.assertTrue(result.changed)
            updated = json.loads(path.read_text())
            self.assertEqual(updated["ad_windows"], [original_ads[0], original_ads[2]])
            self.assertEqual(updated["custom_metadata"], {"must": "survive"})
            self.assertEqual(updated["content_windows"], [
                {"start_seconds": 10.0, "end_seconds": 90.0,
                 "notes": DEMOTE.CONTENT_NOTE},
            ])
            self.assertEqual(len(rejects.read_text().splitlines()), 1)

            again = DEMOTE.demote(
                annotations_dir=annotations,
                rejects_path=rejects,
                episode_prefix="episode",
                requested_start=20,
                reason="ear-verified content",
                reviewed_at="2026-07-08T00:00:00Z",
            )
            self.assertFalse(again.changed)
            self.assertEqual(len(rejects.read_text().splitlines()), 1)
            self.assertEqual(json.loads(path.read_text()), updated)

    def test_demotion_preserves_content_metadata_while_filling_partition_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            rejects = root / "rejects.jsonl"
            path = annotations / "episode.json"
            preserved = [
                {
                    "start_seconds": 10,
                    "end_seconds": 20,
                    "notes": "hand-labeled lead",
                    "reviewer": "human",
                },
                {
                    "start_seconds": 30,
                    "end_seconds": 50,
                    "notes": "hand-labeled middle",
                },
                {
                    "start_seconds": 60,
                    "end_seconds": 90,
                    "notes": "hand-labeled tail",
                },
            ]
            path.write_text(json.dumps(canonical_annotation(
                "episode",
                [ad(0, 10, "a"), ad(20, 30, "b"), ad(90, 100, "c")],
                preserved,
            )))
            write_manifest(annotations, "episode.json")

            DEMOTE.demote(
                annotations_dir=annotations,
                rejects_path=rejects,
                episode_prefix="episode",
                requested_start=20,
                reason="ear-verified content",
                reviewed_at="2026-07-07T00:00:00Z",
            )

            repaired = json.loads(path.read_text())["content_windows"]
            self.assertEqual(repaired, [
                preserved[0],
                {
                    "start_seconds": 20.0,
                    "end_seconds": 30.0,
                    "notes": DEMOTE.CONTENT_NOTE,
                },
                preserved[1],
                {
                    "start_seconds": 50.0,
                    "end_seconds": 60.0,
                    "notes": DEMOTE.CONTENT_NOTE,
                },
                preserved[2],
            ])

    def test_existing_reject_history_is_preserved_and_not_duplicated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            rejects = root / "rejects.jsonl"
            historical = [
                {"episodeId": "old-a", "audioFingerprint": "sha256:" + "a" * 64,
                 "startSeconds": 1.0, "endSeconds": 2.0},
                {"episodeId": "old-b", "audioFingerprint": "sha256:" + "b" * 64,
                 "startSeconds": 3.0, "endSeconds": 4.0},
            ]
            rejects.write_text("".join(json.dumps(row) + "\n" for row in historical))
            path = annotations / "episode.json"
            path.write_text(json.dumps(canonical_annotation(
                "episode",
                [ad(20, 30, "b")],
            )))
            write_manifest(annotations, "episode.json")

            for _ in range(2):
                DEMOTE.demote(
                    annotations_dir=annotations,
                    rejects_path=rejects,
                    episode_prefix="episode",
                    requested_start=20,
                    reason="ear-verified content",
                    reviewed_at="2026-07-07T00:00:00Z",
                )

            records = [json.loads(line) for line in rejects.read_text().splitlines()]
            self.assertEqual(records[:2], historical)
            self.assertEqual(len(records), 3)
            identities = {
                (row["episodeId"], float(row["startSeconds"]), float(row["endSeconds"]))
                for row in records
            }
            self.assertEqual(len(identities), 3)

    def test_concurrent_demotions_serialize_annotation_and_reject_updates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            rejects = root / "rejects.jsonl"
            path = annotations / "episode.json"
            path.write_text(json.dumps(canonical_annotation(
                "episode",
                [ad(20, 30, "a"), ad(60, 70, "b")],
            )))
            write_manifest(annotations, "episode.json")

            def demote_at(start: float) -> None:
                DEMOTE.demote(
                    annotations_dir=annotations,
                    rejects_path=rejects,
                    episode_prefix="episode",
                    requested_start=start,
                    reason="ear-verified content",
                    reviewed_at="2026-07-07T00:00:00Z",
                )

            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(demote_at, [20.0, 60.0]))

            updated = json.loads(path.read_text())
            self.assertEqual(updated["ad_windows"], [])
            self.assertEqual(updated["content_windows"], [{
                "start_seconds": 0.0,
                "end_seconds": 100.0,
                "notes": DEMOTE.CONTENT_NOTE,
            }])
            self.assertEqual(len(rejects.read_text().splitlines()), 2)

    def test_dry_run_creates_no_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            path = annotations / "episode.json"
            original = canonical_annotation("episode", [ad(20, 30, "a")])
            path.write_text(json.dumps(original))
            write_manifest(annotations, "episode.json")
            rejects = root / "rejects.jsonl"

            result = DEMOTE.demote(
                annotations_dir=annotations,
                rejects_path=rejects,
                episode_prefix="episode",
                requested_start=20,
                reason="preview",
                dry_run=True,
            )

            self.assertTrue(result.changed)
            self.assertEqual(json.loads(path.read_text()), original)
            self.assertFalse(rejects.exists())
            self.assertEqual(sorted(item.name for item in root.iterdir()), ["annotations"])

    def test_two_file_publication_restores_exact_bytes_on_second_write_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotation_path = root / "episode.json"
            rejects_path = root / "rejects.jsonl"
            annotation_path.write_bytes(b"original annotation\n")
            rejects_path.write_bytes(b"original rejects\n")
            calls = 0

            def fail_second(path: pathlib.Path, text: str) -> None:
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("injected annotation failure")
                DEMOTE.atomic_write_text(path, text)

            with self.assertRaisesRegex(DEMOTE.DemotionError, "injected annotation failure"):
                DEMOTE.publish_demotion(
                    annotation_path=annotation_path,
                    annotation_text="new annotation\n",
                    rejects_path=rejects_path,
                    rejects_text="new rejects\n",
                    writer=fail_second,
                )

            self.assertEqual(annotation_path.read_bytes(), b"original annotation\n")
            self.assertEqual(rejects_path.read_bytes(), b"original rejects\n")

    def test_annotation_resolution_treats_prefix_literally_and_rejects_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            literal = annotations / "episode[1].json"
            literal.write_text(json.dumps(canonical_annotation("episode[1]", [])))
            outside = root / "outside.json"
            outside.write_text("{}")
            (annotations / "linked.json").symlink_to(outside)
            write_manifest(annotations, "episode[1].json")

            self.assertEqual(
                DEMOTE.resolve_annotation(annotations, "episode["),
                literal,
            )
            for unsafe in ("", "../outside", "linked"):
                with self.subTest(prefix=unsafe):
                    with self.assertRaises(DEMOTE.DemotionError):
                        DEMOTE.resolve_annotation(annotations, unsafe)

    def test_demotion_requires_canonical_membership_and_exact_payload_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            rejects = root / "rejects.jsonl"
            unlisted = annotations / "unlisted.json"
            unlisted.write_text(json.dumps(canonical_annotation(
                "unlisted",
                [ad(20, 30, "a")],
            )))

            with self.assertRaisesRegex(DEMOTE.DemotionError, "canonical manifest"):
                DEMOTE.demote(
                    annotations_dir=annotations,
                    rejects_path=rejects,
                    episode_prefix="unlisted",
                    requested_start=20,
                    reason="must not mutate",
                )

            canonical = annotations / "canonical.json"
            canonical.write_text(json.dumps(canonical_annotation(
                "wrong-id",
                [ad(20, 30, "a")],
            )))
            write_manifest(annotations, "canonical.json")
            with self.assertRaisesRegex(DEMOTE.DemotionError, "matching its canonical filename"):
                DEMOTE.demote(
                    annotations_dir=annotations,
                    rejects_path=rejects,
                    episode_prefix="canonical",
                    requested_start=20,
                    reason="must not mutate",
                )

            self.assertEqual(json.loads(canonical.read_text())["episode_id"], "wrong-id")
            self.assertFalse(rejects.exists())

    def test_demotion_preflights_every_canonical_member_and_unique_audio(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            rejects = root / "rejects.jsonl"
            target = canonical_annotation("target", [ad(20, 30, "target")])
            duplicate = canonical_annotation("duplicate", [])
            duplicate["audio_fingerprint"] = target["audio_fingerprint"]
            target_path = annotations / "target.json"
            target_path.write_text(json.dumps(target), encoding="utf-8")
            (annotations / "duplicate.json").write_text(
                json.dumps(duplicate), encoding="utf-8"
            )
            write_manifest(annotations, "target.json", "duplicate.json")

            with self.assertRaisesRegex(DEMOTE.DemotionError, "unique audio"):
                DEMOTE.demote(
                    annotations_dir=annotations,
                    rejects_path=rejects,
                    episode_prefix="target",
                    requested_start=20,
                    reason="must not mutate a corrupt corpus",
                )

            self.assertEqual(json.loads(target_path.read_text()), target)
            self.assertFalse(rejects.exists())

    def test_idempotent_demotion_still_preflights_the_complete_corpus(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            rejects = root / "rejects.jsonl"
            target = canonical_annotation("target", [])
            duplicate = canonical_annotation("duplicate", [])
            duplicate["audio_fingerprint"] = target["audio_fingerprint"]
            target_path = annotations / "target.json"
            target_path.write_text(json.dumps(target), encoding="utf-8")
            (annotations / "duplicate.json").write_text(
                json.dumps(duplicate), encoding="utf-8"
            )
            write_manifest(annotations, "target.json", "duplicate.json")
            rejects.write_text(
                json.dumps(
                    {"episodeId": "target", "startSeconds": 20, "endSeconds": 30}
                    | {"audioFingerprint": target["audio_fingerprint"]}
                )
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                DEMOTE.DemotionError,
                "invalid canonical corpus on idempotent demotion.*unique audio",
            ):
                DEMOTE.demote(
                    annotations_dir=annotations,
                    rejects_path=rejects,
                    episode_prefix="target",
                    requested_start=20,
                    reason="already removed",
                )

            self.assertEqual(json.loads(target_path.read_text()), target)
            self.assertEqual(len(rejects.read_text().splitlines()), 1)

    def test_demotion_rejects_a_symbolic_link_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            target = annotations / "episode.json"
            target.write_text(
                json.dumps(canonical_annotation("episode", [ad(20, 30, "a")])),
                encoding="utf-8",
            )
            real_manifest = root / "manifest.json"
            real_manifest.write_text(
                json.dumps({"schema_version": 1, "annotations": ["episode.json"]}),
                encoding="utf-8",
            )
            (annotations / DEMOTE.MANIFEST_FILENAME).symlink_to(real_manifest)

            with self.assertRaisesRegex(DEMOTE.DemotionError, "not a regular file"):
                DEMOTE.resolve_annotation(annotations, "episode")

    def test_demotion_rejects_json_boolean_manifest_schema_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            annotations = pathlib.Path(tmp)
            (annotations / DEMOTE.MANIFEST_FILENAME).write_text(
                json.dumps({"schema_version": True, "annotations": ["episode.json"]}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(DEMOTE.DemotionError, "schema_version 1"):
                DEMOTE.resolve_annotation(annotations, "episode")

    def test_demotion_rejects_template_manifest_members(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            annotations = pathlib.Path(tmp)
            (annotations / DEMOTE.MANIFEST_FILENAME).write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "annotations": ["episode.example.json"],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(DEMOTE.DemotionError, "unsafe canonical"):
                DEMOTE.resolve_annotation(annotations, "episode")

    def test_demotion_refuses_a_human_gold_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            rejects = root / "rejects.jsonl"
            path = annotations / "episode.json"
            human_window = ad(20, 30, "human")
            human_window.pop("auto_promoted")
            human_window.pop("provenance")
            human_window.pop("audit_priority")
            original = canonical_annotation("episode", [human_window])
            path.write_text(json.dumps(original))
            write_manifest(annotations, "episode.json")

            with self.assertRaisesRegex(DEMOTE.DemotionError, "refusing human gold"):
                DEMOTE.demote(
                    annotations_dir=annotations,
                    rejects_path=rejects,
                    episode_prefix="episode",
                    requested_start=20,
                    reason="operator mistake",
                )

            self.assertEqual(json.loads(path.read_text()), original)
            self.assertFalse(rejects.exists())

    def test_any_integer_audit_priority_is_a_non_gold_demotion_marker(self) -> None:
        for marker_owner in ("episode", "window"):
            with self.subTest(marker_owner=marker_owner), tempfile.TemporaryDirectory() as tmp:
                root = pathlib.Path(tmp)
                annotations = root / "annotations"
                annotations.mkdir()
                rejects = root / "rejects.jsonl"
                path = annotations / "episode.json"
                proposed_window = ad(20, 30, "proposal")
                proposed_window.pop("auto_promoted")
                proposed_window.pop("provenance")
                proposed_window.pop("audit_priority")
                annotation_metadata = {}
                if marker_owner == "episode":
                    annotation_metadata["audit_priority"] = 0
                else:
                    proposed_window["audit_priority"] = 0
                original = canonical_annotation(
                    "episode",
                    [proposed_window],
                    **annotation_metadata,
                )
                path.write_text(json.dumps(original), encoding="utf-8")
                write_manifest(annotations, "episode.json")

                result = DEMOTE.demote(
                    annotations_dir=annotations,
                    rejects_path=rejects,
                    episode_prefix="episode",
                    requested_start=20,
                    reason="non-gold proposal",
                )

                self.assertTrue(result.changed)
                self.assertEqual(json.loads(path.read_text())["ad_windows"], [])
                self.assertEqual(len(rejects.read_text().splitlines()), 1)

    def test_demotion_semantically_validates_output_before_publication(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            rejects = root / "rejects.jsonl"
            path = annotations / "episode.json"
            invalid = canonical_annotation("episode", [ad(20, 30, "automatic")])
            invalid.pop("show_name")
            path.write_text(json.dumps(invalid), encoding="utf-8")
            write_manifest(annotations, "episode.json")

            with self.assertRaisesRegex(DEMOTE.DemotionError, "invalid demotion output"):
                DEMOTE.demote(
                    annotations_dir=annotations,
                    rejects_path=rejects,
                    episode_prefix="episode",
                    requested_start=20,
                    reason="must not publish invalid corpus data",
                )

            self.assertEqual(json.loads(path.read_text()), invalid)
            self.assertFalse(rejects.exists())

    def test_demotion_refuses_malformed_or_duplicate_reject_history_without_mutation(self) -> None:
        valid_reject = {
            "episodeId": "old",
            "audioFingerprint": "sha256:" + "a" * 64,
            "startSeconds": 1,
            "endSeconds": 2,
        }
        invalid_ledgers = [
            json.dumps({"foo": "bar"}) + "\n",
            (json.dumps(valid_reject) + "\n") * 2,
            json.dumps({"episodeId": "old", "startSeconds": True, "endSeconds": 2})
            + "\n",
            json.dumps({"episodeId": "old", "startSeconds": "1", "endSeconds": 2})
            + "\n",
            json.dumps({"episodeId": "old", "startSeconds": 10**1000, "endSeconds": 2})
            + "\n",
            json.dumps({"episodeId": "old ", "startSeconds": 1, "endSeconds": 2})
            + "\n",
        ]
        for ledger_text in invalid_ledgers:
            with self.subTest(ledger_text=ledger_text), tempfile.TemporaryDirectory() as tmp:
                root = pathlib.Path(tmp)
                annotations = root / "annotations"
                annotations.mkdir()
                path = annotations / "episode.json"
                original = canonical_annotation(
                    "episode",
                    [ad(20, 30, "automatic")],
                )
                path.write_text(json.dumps(original), encoding="utf-8")
                write_manifest(annotations, "episode.json")
                rejects = root / "rejects.jsonl"
                rejects.write_text(ledger_text, encoding="utf-8")

                with self.assertRaises(DEMOTE.DemotionError):
                    DEMOTE.demote(
                        annotations_dir=annotations,
                        rejects_path=rejects,
                        episode_prefix="episode",
                        requested_start=20,
                        reason="must not mutate",
                    )

                self.assertEqual(json.loads(path.read_text()), original)
                self.assertEqual(rejects.read_text(encoding="utf-8"), ledger_text)

    def test_demotion_refuses_a_broken_reject_ledger_symlink_without_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            rejects = root / "rejects.jsonl"
            rejects.symlink_to(root / "missing-ledger.jsonl")
            original = canonical_annotation("episode", [ad(20, 30, "automatic")])
            path = annotations / "episode.json"
            path.write_text(json.dumps(original), encoding="utf-8")
            write_manifest(annotations, "episode.json")

            with self.assertRaisesRegex(DEMOTE.DemotionError, "symbolic link"):
                DEMOTE.demote(
                    annotations_dir=annotations,
                    rejects_path=rejects,
                    episode_prefix="episode",
                    requested_start=20,
                    reason="must fail closed",
                )

            self.assertTrue(rejects.is_symlink())
            self.assertFalse(rejects.exists())
            self.assertEqual(json.loads(path.read_text()), original)

    def test_demotion_refuses_symlinked_reject_parent_without_external_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            original = canonical_annotation("episode", [ad(20, 30, "automatic")])
            annotation_path = annotations / "episode.json"
            annotation_path.write_text(json.dumps(original), encoding="utf-8")
            write_manifest(annotations, "episode.json")

            external = root / "external"
            external.mkdir()
            rejects_target = external / "rejects.jsonl"
            rejects_target.write_text("", encoding="utf-8")
            sentinel = external / "sentinel"
            sentinel.write_text("unchanged", encoding="utf-8")
            alias = root / "alias"
            alias.symlink_to(external, target_is_directory=True)

            with self.assertRaisesRegex(DEMOTE.DemotionError, "symbolic link"):
                DEMOTE.demote(
                    annotations_dir=annotations,
                    rejects_path=alias / "rejects.jsonl",
                    episode_prefix="episode",
                    requested_start=20,
                    reason="must fail closed",
                )

            self.assertEqual(json.loads(annotation_path.read_text()), original)
            self.assertEqual(rejects_target.read_text(encoding="utf-8"), "")
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "unchanged")
            self.assertEqual(
                sorted(path.name for path in external.iterdir()),
                ["rejects.jsonl", "sentinel"],
            )


if __name__ == "__main__":
    unittest.main()
