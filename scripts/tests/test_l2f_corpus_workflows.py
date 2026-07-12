#!/usr/bin/env python3
"""Integrity tests for corpus promotion and permanent reject workflows."""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import json
import os
import pathlib
import sys
import tempfile
import threading
import unittest
import wave
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from unittest import mock


SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

import l2f_canonical_manifest as CANONICAL  # noqa: E402
import convert_annotations_to_chapter_goldens as CONVERTER  # noqa: E402


def annotation(episode_id: str, *, show_name: str = "Example") -> dict:
    return {
        "episode_id": episode_id,
        "show_name": show_name,
        "duration_seconds": 10,
        "ad_windows": [],
        "content_windows": [
            {"start_seconds": 0, "end_seconds": 10, "notes": None}
        ],
        "variant_of": None,
        "audio_fingerprint": "sha256:"
        + hashlib.sha256(episode_id.encode("utf-8")).hexdigest(),
    }


def load_script(module_name: str, filename: str):
    spec = importlib.util.spec_from_file_location(module_name, SCRIPTS / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


AUTO_PROMOTE = load_script("l2f_auto_promote", "l2f-auto-promote.py")
REDIFF = load_script("l2f_dai_rediff", "l2f-dai-rediff.py")
REVIEW_PROMOTE = load_script(
    "l2f_promote_reviewed_corpus", "l2f-promote-reviewed-corpus.py"
)
EARAUDIT = load_script("l2f_earaudit", "l2f-earaudit.py")
R3_AUDIT = load_script("l2f_r3_audit_workflow", "l2f-r3-audit.py")
DEMOTE = load_script("l2f_flag_false_promote_workflow", "l2f-flag-false-promote.py")


def annotation_with_ad(episode_id: str, *, automatic: bool) -> dict:
    document = annotation(episode_id)
    document["ad_windows"] = [{
        "start_seconds": 2,
        "end_seconds": 4,
        "advertiser": "Example",
        "product": "Product",
        "ad_type": "host_read",
        "transition_type": "explicit",
        "confidence_notes": "reviewed",
    }]
    document["content_windows"] = [
        {"start_seconds": 0, "end_seconds": 2, "notes": None},
        {"start_seconds": 4, "end_seconds": 10, "notes": None},
    ]
    if automatic:
        document["auto_promoted"] = True
        document["ad_windows"][0].update({
            "auto_promoted": True,
            "provenance": ["rediff"],
            "audit_priority": 1,
        })
    else:
        document["provenance"] = [REVIEW_PROMOTE.SECOND_PASS_PROVENANCE]
        document["ad_windows"][0]["provenance"] = [
            REVIEW_PROMOTE.SECOND_PASS_PROVENANCE
        ]
        document["review_attestations"] = [
            {
                "reviewer": "Reviewer One",
                "reviewed_at": "2026-05-12T03:06:35Z",
                "audio_fingerprint": document["audio_fingerprint"],
                "review_artifact_id": "sha256:" + "a" * 64,
            },
            {
                "reviewer": "Reviewer Two",
                "reviewed_at": "2026-07-10T12:00:00Z",
                "audio_fingerprint": document["audio_fingerprint"],
                "review_artifact_id": "sha256:" + "b" * 64,
            },
        ]
    return document


class CorpusWorkflowIntegrityTests(unittest.TestCase):
    def test_reviewed_audio_duration_stays_bound_when_path_becomes_fifo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            audio = root / "episode.wav"
            with wave.open(str(audio), "wb") as handle:
                handle.setnchannels(1)
                handle.setsampwidth(2)
                handle.setframerate(8_000)
                handle.writeframes(b"\0\0" * 8_000)
            expected = REVIEW_PROMOTE.sha256_fingerprint(audio)
            original_duration = REVIEW_PROMOTE.duration_from_audio
            retained = root / "retained.wav"
            keeper = -1

            def swap_before_duration(path, descriptor=None):
                nonlocal keeper
                path.rename(retained)
                os.mkfifo(path)
                keeper = os.open(path, os.O_RDWR | os.O_NONBLOCK)
                return original_duration(path, descriptor=descriptor)

            try:
                with mock.patch.object(
                    REVIEW_PROMOTE,
                    "duration_from_audio",
                    side_effect=swap_before_duration,
                ):
                    actual, duration = REVIEW_PROMOTE.fingerprint_and_duration(audio)
            finally:
                if keeper >= 0:
                    os.close(keeper)

            self.assertEqual(actual, expected)
            self.assertEqual(duration, 1.0)

    def test_duration_probe_has_a_finite_timeout(self) -> None:
        with mock.patch.object(
            REVIEW_PROMOTE.subprocess,
            "run",
            side_effect=REVIEW_PROMOTE.subprocess.TimeoutExpired(["probe"], 10),
        ) as run:
            self.assertIsNone(REVIEW_PROMOTE.run_duration_probe(["probe"]))
        self.assertEqual(
            run.call_args.kwargs["timeout"],
            REVIEW_PROMOTE.AUDIO_PROBE_TIMEOUT_SECONDS,
        )

    def test_legacy_earaudit_reads_canonical_snake_case_bounds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "Annotations"
            annotations.mkdir()
            document = annotation_with_ad("episode", automatic=True)
            (annotations / "episode.json").write_text(
                json.dumps(document), encoding="utf-8"
            )
            (annotations / CANONICAL.MANIFEST_FILENAME).write_text(
                json.dumps({"schema_version": 1, "annotations": ["episode.json"]}),
                encoding="utf-8",
            )

            with (
                mock.patch.object(EARAUDIT, "ANN_DIR", annotations),
                mock.patch.object(EARAUDIT, "MANIFEST", root / "missing-manifest.json"),
            ):
                spans = EARAUDIT.load_spans()

            self.assertEqual(len(spans), 1)
            self.assertEqual(spans[0]["episodeId"], "episode")
            self.assertEqual((spans[0]["start"], spans[0]["end"]), (2.0, 4.0))

    def test_r3_report_uses_one_canonical_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = pathlib.Path(tmp) / "report.md"
            document = annotation_with_ad("episode", automatic=True)
            with mock.patch.object(
                R3_AUDIT,
                "load_canonical_annotations",
                return_value={"episode.json": document},
            ) as load_snapshot:
                self.assertEqual(
                    R3_AUDIT.main(["--output", str(output)]),
                    0,
                )

            load_snapshot.assert_called_once_with(R3_AUDIT.ANN_DIR)
            self.assertTrue(output.is_file())

    def test_committed_first_pass_attestation_is_resolvable_and_asset_bound(self) -> None:
        corpus = SCRIPTS.parent / "TestFixtures/Corpus"
        artifact_path = corpus / "Reviews/88306e39982d867a3df5a6561f6ae8a199ac3fb50c6266d00ceb21d1194cee3a.json"
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        artifact_id = "sha256:" + hashlib.sha256(artifact_path.read_bytes()).hexdigest()
        manifest = json.loads(
            (corpus / "Annotations/_canonical-manifest.json").read_text(encoding="utf-8")
        )["annotations"]
        bindings = {
            row["episode_id"]: row
            for row in artifact["audio_bindings"]
        }
        self.assertEqual(len(manifest), 12)
        for filename in manifest:
            document = json.loads((corpus / "Annotations" / filename).read_text())
            self.assertEqual(document["provenance"], ["human_first_pass"])
            binding = bindings[document["episode_id"]]
            self.assertEqual(binding["audio_fingerprint"], document["audio_fingerprint"])
            self.assertEqual(
                binding["annotation_decision"],
                REVIEW_PROMOTE.annotation_decision(document),
            )
            self.assertEqual(document["review_attestations"], [{
                "reviewer": artifact["reviewer"],
                "reviewed_at": artifact["reviewed_at"],
                "audio_fingerprint": document["audio_fingerprint"],
                "review_artifact_id": artifact_id,
            }])

    def test_review_artifact_cannot_be_replayed_against_changed_decisions(self) -> None:
        document = annotation_with_ad("episode", automatic=False)
        attestation = document["review_attestations"][0]
        artifact = {
            "schema_version": 1,
            "artifact_kind": "human_first_pass_attestation",
            "reviewer": attestation["reviewer"],
            "reviewed_at": attestation["reviewed_at"],
            "source_decision_count": 1,
            "audio_bindings": [{
                "episode_id": document["episode_id"],
                "audio_fingerprint": document["audio_fingerprint"],
                "annotation_decision": REVIEW_PROMOTE.annotation_decision(document),
            }],
        }
        self.assertTrue(CONVERTER._artifact_binds_attestation(
            artifact, attestation, document
        ))
        for field in ("schema_version", "source_decision_count"):
            with self.subTest(floating_integer_field=field):
                floating = json.loads(json.dumps(artifact))
                floating[field] = float(floating[field])
                self.assertFalse(CONVERTER._artifact_binds_attestation(
                    floating, attestation, document
                ))
        overflow = json.loads(json.dumps(artifact))
        overflow["source_decision_count"] = 2**63
        self.assertFalse(CONVERTER._artifact_binds_attestation(
            overflow, attestation, document
        ))

        changed = json.loads(json.dumps(document))
        changed["ad_windows"][0]["advertiser"] = "Different advertiser"
        self.assertFalse(CONVERTER._artifact_binds_attestation(
            artifact, attestation, changed
        ))

        replayed = json.loads(json.dumps(document))
        replayed["episode_id"] = "other-episode"
        self.assertFalse(CONVERTER._artifact_binds_attestation(
            artifact, attestation, replayed
        ))

    def test_modern_review_artifact_binds_decisions_to_episode_and_audio(self) -> None:
        document = annotation_with_ad("episode", automatic=False)
        attestation = document["review_attestations"][0]
        decision_id = "episode#1"
        artifact = {
            "schema_version": 1,
            "artifact_kind": "corpus_review_attestation",
            "reviewer": attestation["reviewer"],
            "reviewed_at": attestation["reviewed_at"],
            "episodes": [{
                "episode_id": document["episode_id"],
                "audio_fingerprint": document["audio_fingerprint"],
                "decision_ids": [decision_id],
                "annotation_decision": REVIEW_PROMOTE.annotation_decision(document),
            }],
            "reviews": {
                decision_id: {
                    "episode_id": document["episode_id"],
                    "reviewer": attestation["reviewer"],
                    "reviewed_at": attestation["reviewed_at"],
                    "audio_fingerprint": document["audio_fingerprint"],
                    "status": "verified_ad",
                }
            },
        }
        self.assertTrue(CONVERTER._artifact_binds_attestation(
            artifact, attestation, document
        ))
        artifact["reviews"][decision_id]["reviewed_at"] = "2026-07-10T12:00:01Z"
        self.assertFalse(CONVERTER._artifact_binds_attestation(
            artifact, attestation, document
        ))
        artifact["reviews"][decision_id]["reviewed_at"] = attestation["reviewed_at"]
        artifact["reviews"][decision_id]["episode_id"] = "other-episode"
        self.assertFalse(CONVERTER._artifact_binds_attestation(
            artifact, attestation, document
        ))

    def test_review_artifact_decision_numbers_cannot_be_replaced_by_booleans(self) -> None:
        document = annotation("boolean-number-decision")
        document["duration_seconds"] = 1
        document["content_windows"] = [
            {"start_seconds": 0, "end_seconds": 1, "notes": None}
        ]
        attestation = {
            "reviewer": "Reviewer",
            "reviewed_at": "2026-07-10T12:00:00Z",
            "audio_fingerprint": document["audio_fingerprint"],
            "review_artifact_id": "sha256:" + "a" * 64,
        }
        numeric_decision = CONVERTER.annotation_decision(document)
        numeric_decision["duration_seconds"] = 1.0
        numeric_decision["content_windows"][0]["start_seconds"] = 0.0
        numeric_decision["content_windows"][0]["end_seconds"] = 1.0

        decision_id = "boolean-number-decision#1"
        artifacts = [
            {
                "schema_version": 1,
                "artifact_kind": "human_first_pass_attestation",
                "reviewer": attestation["reviewer"],
                "reviewed_at": attestation["reviewed_at"],
                "source_decision_count": 1,
                "audio_bindings": [{
                    "episode_id": document["episode_id"],
                    "audio_fingerprint": document["audio_fingerprint"],
                    "annotation_decision": numeric_decision,
                }],
            },
            {
                "schema_version": 1,
                "artifact_kind": "corpus_review_attestation",
                "reviewer": attestation["reviewer"],
                "reviewed_at": attestation["reviewed_at"],
                "episodes": [{
                    "episode_id": document["episode_id"],
                    "audio_fingerprint": document["audio_fingerprint"],
                    "decision_ids": [decision_id],
                    "annotation_decision": numeric_decision,
                }],
                "reviews": {
                    decision_id: {
                        "episode_id": document["episode_id"],
                        "reviewer": attestation["reviewer"],
                        "reviewed_at": attestation["reviewed_at"],
                        "audio_fingerprint": document["audio_fingerprint"],
                        "status": "zero_ad_confirmed",
                    }
                },
            },
        ]

        for artifact in artifacts:
            with self.subTest(kind=artifact["artifact_kind"]):
                self.assertTrue(CONVERTER._artifact_binds_attestation(
                    artifact, attestation, document
                ))
                poisoned = json.loads(json.dumps(artifact))
                if artifact["artifact_kind"] == "human_first_pass_attestation":
                    decision = poisoned["audio_bindings"][0]["annotation_decision"]
                else:
                    decision = poisoned["episodes"][0]["annotation_decision"]
                decision["duration_seconds"] = True
                decision["content_windows"][0]["start_seconds"] = False
                decision["content_windows"][0]["end_seconds"] = True
                self.assertFalse(CONVERTER._artifact_binds_attestation(
                    poisoned, attestation, document
                ))

    def test_review_artifact_reexports_decisions_at_the_pass_attestation_time(self) -> None:
        document = annotation_with_ad("episode", automatic=False)
        report = REVIEW_PROMOTE.EpisodeReport(
            "episode", [{"id": "episode#1", "episode_id": "episode"}]
        )
        report.annotation = document
        source_time = "2026-07-10T11:59:00Z"
        pass_time = "2026-07-10T12:00:00Z"

        artifact = REVIEW_PROMOTE.build_review_artifact(
            [report],
            {
                "episode#1": {
                    "episode_id": "episode",
                    "reviewer": "Reviewer",
                    "reviewed_at": source_time,
                    "audio_fingerprint": document["audio_fingerprint"],
                    "status": "verified_ad",
                }
            },
            reviewer="Reviewer",
            reviewed_at=pass_time,
        )

        exported = artifact["reviews"]["episode#1"]
        self.assertEqual(exported["reviewed_at"], pass_time)
        self.assertEqual(exported["source_reviewed_at"], source_time)

        first_pass_artifact = REVIEW_PROMOTE.build_review_artifact(
            [report],
            {
                "episode#1": {
                    "episode_id": "episode",
                    "reviewer": "Reviewer",
                    "reviewed_at": source_time,
                    "audio_fingerprint": document["audio_fingerprint"],
                    "status": "verified_ad",
                }
            },
            reviewer="Reviewer",
            reviewed_at=pass_time,
            artifact_kind="human_first_pass_attestation",
        )
        self.assertEqual(
            first_pass_artifact["artifact_kind"],
            "human_first_pass_attestation",
        )
        self.assertEqual(first_pass_artifact["source_decision_count"], 1)
        self.assertEqual(
            first_pass_artifact["audio_bindings"][0]["annotation_decision"],
            REVIEW_PROMOTE.annotation_decision(document),
        )

    def test_review_artifact_publication_is_immutable_durable_and_retryable(self) -> None:
        artifact = {
            "schema_version": 1,
            "artifact_kind": "corpus_review_attestation",
            "reviewer": "Reviewer",
            "reviewed_at": "2026-07-10T12:00:00Z",
            "episodes": [],
            "reviews": {},
        }
        with tempfile.TemporaryDirectory() as tmp:
            reviews_dir = pathlib.Path(tmp) / "Reviews"
            artifact_id, path = REVIEW_PROMOTE.persist_review_artifact(
                reviews_dir, artifact
            )
            self.assertEqual(
                artifact_id,
                REVIEW_PROMOTE.canonical_review_artifact_id(artifact),
            )
            self.assertEqual(
                path.read_bytes(),
                REVIEW_PROMOTE.review_artifact_bytes(artifact),
            )
            self.assertEqual(
                REVIEW_PROMOTE.persist_review_artifact(reviews_dir, artifact),
                (artifact_id, path),
            )

            interrupted = {**artifact, "reviewer": "Interrupted Reviewer"}
            interrupted_id = REVIEW_PROMOTE.canonical_review_artifact_id(interrupted)
            interrupted_path = reviews_dir / f"{interrupted_id.removeprefix('sha256:')}.json"
            real_fsync = REVIEW_PROMOTE.os.fsync
            fsync_calls = 0

            def fail_first_directory_sync(descriptor: int) -> None:
                nonlocal fsync_calls
                fsync_calls += 1
                if fsync_calls == 2:
                    raise OSError("injected directory fsync failure")
                real_fsync(descriptor)

            with mock.patch.object(
                REVIEW_PROMOTE.os,
                "fsync",
                side_effect=fail_first_directory_sync,
            ):
                with self.assertRaisesRegex(OSError, "injected directory fsync failure"):
                    REVIEW_PROMOTE.persist_review_artifact(reviews_dir, interrupted)
            self.assertTrue(interrupted_path.is_file())
            self.assertEqual(
                REVIEW_PROMOTE.persist_review_artifact(reviews_dir, interrupted),
                (interrupted_id, interrupted_path),
            )

            different = {**artifact, "reviewer": "Other Reviewer"}
            with mock.patch.object(
                REVIEW_PROMOTE.os,
                "link",
                side_effect=OSError("injected publication failure"),
            ):
                with self.assertRaisesRegex(OSError, "injected publication failure"):
                    REVIEW_PROMOTE.persist_review_artifact(reviews_dir, different)
            self.assertFalse(any(
                child.name.startswith(".") for child in reviews_dir.iterdir()
            ))

    def test_review_artifact_output_path_does_not_resolve_away_symlink_guard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            outside = root / "outside"
            outside.mkdir()
            reviews_link = root / "Reviews"
            reviews_link.symlink_to(outside, target_is_directory=True)
            artifact = {
                "schema_version": 1,
                "artifact_kind": "human_first_pass_attestation",
                "reviewer": "Reviewer",
                "reviewed_at": "2026-07-10T12:00:00Z",
                "source_decision_count": 1,
                "audio_bindings": [],
            }

            output = REVIEW_PROMOTE.lexical_output_path(reviews_link)
            self.assertEqual(output, reviews_link)
            self.assertTrue(output.is_symlink())
            with self.assertRaisesRegex(ValueError, "symbolic link"):
                REVIEW_PROMOTE.persist_review_artifact(output, artifact)
            self.assertEqual(list(outside.iterdir()), [])

    def test_review_artifact_publication_rejects_symlinked_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            outside = root / "outside"
            reviews_target = outside / "nested" / "Corpus" / "Reviews"
            reviews_target.mkdir(parents=True)
            alias = root / "Corpus"
            alias.symlink_to(outside, target_is_directory=True)
            artifact = {
                "schema_version": 1,
                "artifact_kind": "human_first_pass_attestation",
                "reviewer": "Reviewer",
                "reviewed_at": "2026-07-10T12:00:00Z",
                "source_decision_count": 1,
                "audio_bindings": [],
            }

            with self.assertRaisesRegex(ValueError, "symbolic link"):
                REVIEW_PROMOTE.persist_review_artifact(
                    alias / "nested" / "Corpus" / "Reviews",
                    artifact,
                )
            self.assertEqual(list(reviews_target.iterdir()), [])

    def test_reviewed_promotion_rejects_aliased_and_traversing_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            outside = root / "outside"
            outside.mkdir()
            review_target = outside / "review.json"
            review_target.write_text(
                json.dumps({"reviews": {}}), encoding="utf-8"
            )
            review_alias = root / "review.json"
            review_alias.symlink_to(review_target)

            lexical_review = REVIEW_PROMOTE.resolve_path(review_alias)
            self.assertEqual(lexical_review, review_alias)
            with self.assertRaisesRegex(ValueError, "symbolic link"):
                REVIEW_PROMOTE.load_review_file(lexical_review)

            queue_target = outside / "queue.json"
            queue_target.write_text(
                json.dumps({"entries": []}), encoding="utf-8"
            )
            queue_alias = root / "queue.json"
            queue_alias.symlink_to(queue_target)
            with self.assertRaisesRegex(ValueError, "symbolic link"):
                REVIEW_PROMOTE.load_queue(queue_alias)

            corpus = root / "Corpus"
            drafts = corpus / "Drafts"
            drafts.mkdir(parents=True)
            fallback_queue = drafts / "review-queue.json"
            fallback_queue.symlink_to(queue_target)
            args = REVIEW_PROMOTE.parse_args([])
            with mock.patch.object(REVIEW_PROMOTE, "CORPUS_DIR", corpus):
                discovered = REVIEW_PROMOTE.choose_queue_path(
                    args,
                    root / "missing-review.json",
                    None,
                )
            self.assertEqual(discovered, fallback_queue)
            with self.assertRaisesRegex(ValueError, "symbolic link"):
                REVIEW_PROMOTE.load_queue(discovered)

            audio_target = outside / "episode.wav"
            audio_target.write_bytes(b"external audio")
            audio_alias = root / "episode.wav"
            audio_alias.symlink_to(audio_target)
            entries = [{
                "id": "episode#1",
                "episode_id": "episode",
                "audio_path": str(audio_alias),
            }]
            with self.assertRaisesRegex(ValueError, "symbolic link"):
                REVIEW_PROMOTE.find_audio("episode", entries, {}, root, root)

            with self.assertRaisesRegex(ValueError, "traverse a parent directory"):
                REVIEW_PROMOTE.resolve_local_then_repo(
                    "../outside/queue.json",
                    root / "nested",
                )

    def test_gold_rejects_unresolved_review_artifact_hashes(self) -> None:
        document = annotation_with_ad("fabricated-gold", automatic=False)
        artifacts = REVIEW_PROMOTE.load_review_artifacts(
            SCRIPTS.parent / "TestFixtures/Corpus/Reviews"
        )
        with self.assertRaisesRegex(
            ValueError, "does not resolve to matching review evidence"
        ):
            REVIEW_PROMOTE.validate_annotation(
                document,
                source="fabricated-gold.json",
                artifact_index=artifacts,
            )

    def test_gold_requires_exact_first_and_second_pass_artifact_kinds(self) -> None:
        def artifact(document: dict, attestation: dict, kind: str, index: int) -> dict:
            if kind == "human_first_pass_attestation":
                return {
                    "schema_version": 1,
                    "artifact_kind": kind,
                    "reviewer": attestation["reviewer"],
                    "reviewed_at": attestation["reviewed_at"],
                    "source_decision_count": 1,
                    "audio_bindings": [{
                        "episode_id": document["episode_id"],
                        "audio_fingerprint": document["audio_fingerprint"],
                        "annotation_decision": REVIEW_PROMOTE.annotation_decision(document),
                    }],
                }
            decision_id = f"{document['episode_id']}#{index}"
            return {
                "schema_version": 1,
                "artifact_kind": kind,
                "reviewer": attestation["reviewer"],
                "reviewed_at": attestation["reviewed_at"],
                "episodes": [{
                    "episode_id": document["episode_id"],
                    "audio_fingerprint": document["audio_fingerprint"],
                    "decision_ids": [decision_id],
                    "annotation_decision": REVIEW_PROMOTE.annotation_decision(document),
                }],
                "reviews": {
                    decision_id: {
                        "episode_id": document["episode_id"],
                        "reviewer": attestation["reviewer"],
                        "reviewed_at": attestation["reviewed_at"],
                        "audio_fingerprint": document["audio_fingerprint"],
                        "status": "verified_ad",
                    }
                },
            }

        for kinds, expected in (
            (("human_first_pass_attestation", "corpus_review_attestation"), True),
            (("human_first_pass_attestation", "human_first_pass_attestation"), False),
            (("corpus_review_attestation", "corpus_review_attestation"), False),
        ):
            with self.subTest(kinds=kinds):
                document = annotation_with_ad("artifact-kinds", automatic=False)
                artifact_index = {
                    attestation["review_artifact_id"]: artifact(
                        document, attestation, kind, index
                    )
                    for index, (attestation, kind) in enumerate(
                        zip(document["review_attestations"], kinds), start=1
                    )
                }
                self.assertEqual(
                    CONVERTER.has_distinct_review_attestations(
                        document, artifact_index
                    ),
                    expected,
                )
                if expected:
                    CONVERTER.validate_annotation(
                        document,
                        source="artifact-kinds.json",
                        artifact_index=artifact_index,
                    )
                else:
                    with self.assertRaisesRegex(
                        ValueError, "requires two distinct asset-bound review attestations"
                    ):
                        CONVERTER.validate_annotation(
                            document,
                            source="artifact-kinds.json",
                            artifact_index=artifact_index,
                        )

        document = annotation_with_ad("artifact-cardinality", automatic=False)
        third = dict(document["review_attestations"][1])
        third["reviewer"] = "reviewer three"
        third["review_artifact_id"] = "sha256:" + "c" * 64
        document["review_attestations"].append(third)
        self.assertFalse(
            CONVERTER.has_distinct_review_attestations(
                document,
                require_artifacts=False,
            )
        )
        with self.assertRaisesRegex(
            ValueError, "requires two distinct asset-bound review attestations"
        ):
            CONVERTER.validate_annotation(
                document,
                source="artifact-cardinality.json",
                require_review_artifacts=False,
            )

    def test_gold_reviewer_identity_uses_unicode_canonical_equivalence(self) -> None:
        document = annotation_with_ad("unicode-reviewers", automatic=False)
        document["review_attestations"][0]["reviewer"] = "Jos\u00e9"
        document["review_attestations"][1]["reviewer"] = "Jose\u0301"

        self.assertFalse(
            CONVERTER.has_distinct_review_attestations(
                document,
                require_artifacts=False,
            )
        )

    def test_rediff_emits_only_retained_snapshot_coordinates(self) -> None:
        # B inserted 20 frames at index 40. All of A remains aligned, so no
        # coordinate-bearing label is publishable against retained A.
        b_insertion_runs = [(0, 0, 40, 0), (40, 60, 60, 0)]
        self.assertEqual(REDIFF.gaps_in_a(b_insertion_runs, 100, 5), [])
        self.assertEqual(
            REDIFF.gaps_in_b(b_insertion_runs, 120, 5),
            [(40, 60, 40, 60)],
        )

        # A's frames 40..<60 are absent from B. This is the old ad retained in
        # snapshot A and is therefore the only safe corpus coordinate range.
        removed_from_a_runs = [(0, 0, 40, 0), (60, 40, 40, 0)]
        self.assertEqual(
            REDIFF.gaps_in_a(removed_from_a_runs, 100, 5),
            [(40, 60, 40, 40)],
        )

    def test_rediff_draft_binds_a_coordinates_to_a_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            snapshot = root / "episode.mp3"
            snapshot.write_bytes(b"retained snapshot A")
            snapshot_sha = hashlib.sha256(snapshot.read_bytes()).hexdigest()
            with mock.patch.object(REDIFF, "DRAFTS", root / "drafts"):
                output = REDIFF.write_draft(
                    "episode",
                    snapshot,
                    [{
                        "startSeconds": 10,
                        "endSeconds": 20,
                        "confidence": 0.9,
                        "leftRunSeconds": 60,
                        "rightRunSeconds": 60,
                    }],
                    "b" * 64,
                    snapshot_sha,
                )
            document = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(document["coordinate_space"], "snapshot_a")
            self.assertEqual(
                document["audio_fingerprint"],
                "sha256:" + snapshot_sha,
            )
            self.assertNotEqual(document["audio_fingerprint"], "sha256:" + "b" * 64)
            self.assertEqual(
                document["comparison_audio_fingerprint"], "sha256:" + "b" * 64
            )
            self.assertEqual(
                document["ad_windows"][0]["comparison_audio_fingerprint"],
                "sha256:" + "b" * 64,
            )

    def test_rediff_draft_rejects_snapshot_changed_after_alignment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            snapshot = root / "episode.mp3"
            snapshot.write_bytes(b"snapshot bytes used for alignment")
            expected_sha = hashlib.sha256(snapshot.read_bytes()).hexdigest()
            snapshot.write_bytes(b"replacement snapshot bytes")
            with (
                mock.patch.object(REDIFF, "DRAFTS", root / "drafts"),
                self.assertRaisesRegex(REDIFF.RediffInputError, "changed during rediff"),
            ):
                REDIFF.write_draft(
                    "episode",
                    snapshot,
                    [],
                    "b" * 64,
                    expected_sha,
                )

    def test_rediff_rejects_fresh_asset_changed_during_alignment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            snapshot = root / "episode.mp3"
            fresh = root / "episode.fresh.mp3"
            snapshot.write_bytes(b"retained snapshot")
            fresh.write_bytes(b"fresh bytes used for alignment")
            snapshot_sha = hashlib.sha256(snapshot.read_bytes()).hexdigest()
            fresh_sha = hashlib.sha256(fresh.read_bytes()).hexdigest()
            fresh.write_bytes(b"replacement fresh bytes")
            with self.assertRaisesRegex(
                REDIFF.RediffInputError, "fresh comparison audio changed"
            ):
                REDIFF.verify_rediff_inputs_unchanged(
                    snapshot,
                    fresh,
                    episode_id="episode",
                    expected_snapshot_sha=snapshot_sha,
                    expected_fresh_sha=fresh_sha,
                )

    def test_rediff_cumulatively_merges_valid_same_a_evidence(self) -> None:
        def slot(start: float, end: float) -> dict:
            return {
                "startSeconds": start,
                "endSeconds": end,
                "confidence": 0.9,
                "leftRunSeconds": 60,
                "rightRunSeconds": 60,
            }

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            snapshot = root / "episode.mp3"
            snapshot.write_bytes(b"retained snapshot A")
            snapshot_sha = hashlib.sha256(snapshot.read_bytes()).hexdigest()
            with mock.patch.object(REDIFF, "DRAFTS", root / "drafts"):
                output = REDIFF.write_draft(
                    "episode", snapshot, [slot(10, 20)], "b" * 64, snapshot_sha
                )
                first_bytes = output.read_bytes()
                prior = REDIFF.load_reusable_draft("episode", snapshot_sha)
                self.assertIsNotNone(prior)
                self.assertEqual(output.read_bytes(), first_bytes)

                REDIFF.write_draft(
                    "episode",
                    snapshot,
                    [slot(15, 25)],
                    "c" * 64,
                    snapshot_sha,
                    prior,
                )
                merged = json.loads(output.read_text(encoding="utf-8"))
                self.assertEqual(
                    [
                        (
                            window["start_seconds"],
                            window["end_seconds"],
                            window["comparison_audio_fingerprint"],
                        )
                        for window in merged["ad_windows"]
                    ],
                    [
                        (10, 20, "sha256:" + "b" * 64),
                        (15, 25, "sha256:" + "c" * 64),
                    ],
                )
                self.assertEqual(
                    merged["comparison_audio_fingerprints"],
                    ["sha256:" + "b" * 64, "sha256:" + "c" * 64],
                )

    def test_rediff_main_persists_a_successful_zero_gap_comparison(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            snapshot = root / "episode.mp3"
            fresh = root / "episode.fresh.mp3"
            diagnostics = root / "diagnostics.json"
            output = root / "drafts" / "episode.dai-rediff.json"
            snapshot.write_bytes(b"retained snapshot A")
            fresh.write_bytes(b"distinct comparison B")
            snapshot_sha = hashlib.sha256(snapshot.read_bytes()).hexdigest()
            fresh_sha = hashlib.sha256(fresh.read_bytes()).hexdigest()
            prior = {
                "episode_id": "episode",
                "audio_fingerprint": "sha256:" + snapshot_sha,
                "coordinate_space": "snapshot_a",
                "comparison_audio_fingerprint": "sha256:" + "b" * 64,
                "comparison_audio_fingerprints": ["sha256:" + "b" * 64],
                "ad_windows": [{
                    "start_seconds": 10,
                    "end_seconds": 20,
                    "comparison_audio_fingerprint": "sha256:" + "b" * 64,
                }],
            }
            aligned = {
                "ok": True,
                "fingerprintsA": 100,
                "fingerprintsB": 100,
                "secondsPerFpA": 1.0,
                "secondsPerFpB": 1.0,
                "runs": 1,
                "alignedSecondsA": 100.0,
                "adSlots": [],
            }
            manifest_row = {
                "episodeId": "episode",
                "_snapshot_path": snapshot,
                "_snapshot_sha": snapshot_sha,
            }
            original_write_draft = REDIFF.write_draft

            with (
                mock.patch.object(REDIFF, "REPO", root),
                mock.patch.object(REDIFF, "DRAFTS", root / "drafts"),
                mock.patch.object(REDIFF, "DIAG_OUT", diagnostics),
                mock.patch.object(REDIFF, "MANIFEST", root / "manifest.json"),
                mock.patch.object(REDIFF, "load_validated_manifest", return_value=[manifest_row]),
                mock.patch.object(REDIFF, "load_reusable_draft", return_value=prior),
                mock.patch.object(REDIFF, "rediff_pair", return_value=aligned),
                mock.patch.object(REDIFF, "verify_rediff_inputs_unchanged"),
                mock.patch.object(
                    REDIFF, "write_draft", wraps=original_write_draft
                ) as write,
                mock.patch.object(sys, "argv", ["l2f-dai-rediff.py", "--dry-run"]),
                self.assertRaises(SystemExit) as exit_context,
            ):
                REDIFF.main()

            self.assertEqual(exit_context.exception.code, 0)
            write.assert_called_once_with(
                "episode", snapshot, [], fresh_sha, snapshot_sha, prior
            )
            document = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(
                document["comparison_audio_fingerprints"],
                sorted(["sha256:" + "b" * 64, "sha256:" + fresh_sha]),
            )
            self.assertEqual(
                document["ad_windows"],
                prior["ad_windows"],
            )

    def test_rediff_retires_legacy_or_changed_a_drafts_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            drafts = pathlib.Path(tmp)
            path = drafts / "episode.dai-rediff.json"
            expected_sha = "a" * 64
            valid = {
                "episode_id": "episode",
                "coordinate_space": "snapshot_a",
                "audio_fingerprint": "sha256:" + expected_sha,
                "ad_windows": [{"start_seconds": 10, "end_seconds": 20}],
                "comparison_audio_fingerprint": "sha256:" + "b" * 64,
                "comparison_audio_fingerprints": ["sha256:" + "b" * 64],
            }
            with mock.patch.object(REDIFF, "DRAFTS", drafts):
                path.write_text(json.dumps(valid), encoding="utf-8")
                before = path.read_bytes()
                loaded = REDIFF.load_reusable_draft("episode", expected_sha)
                self.assertIsNotNone(loaded)
                self.assertEqual(
                    loaded["ad_windows"][0]["comparison_audio_fingerprint"],
                    "sha256:" + "b" * 64,
                )
                self.assertEqual(path.read_bytes(), before)

                negative_current_comparison = {
                    **valid,
                    "ad_windows": [{
                        "start_seconds": 10,
                        "end_seconds": 20,
                        "comparison_audio_fingerprint": "sha256:" + "b" * 64,
                    }],
                    "comparison_audio_fingerprint": "sha256:" + "c" * 64,
                    "comparison_audio_fingerprints": [
                        "sha256:" + "b" * 64,
                        "sha256:" + "c" * 64,
                    ],
                }
                path.write_text(
                    json.dumps(negative_current_comparison), encoding="utf-8"
                )
                loaded = REDIFF.load_reusable_draft("episode", expected_sha)
                self.assertIsNotNone(loaded)
                self.assertEqual(
                    loaded["ad_windows"][0]["comparison_audio_fingerprint"],
                    "sha256:" + "b" * 64,
                )

                for invalid in (
                    {**valid, "coordinate_space": "fresh_b"},
                    {**valid, "audio_fingerprint": "sha256:" + "b" * 64},
                    {**valid, "ad_windows": "not-an-array"},
                    {
                        key: value
                        for key, value in valid.items()
                        if not key.startswith("comparison_audio_fingerprint")
                    },
                    {
                        **valid,
                        "comparison_audio_fingerprints": [
                            "sha256:" + "b" * 64,
                            "sha256:" + "b" * 64,
                        ],
                    },
                    {
                        **valid,
                        "comparison_audio_fingerprint": "sha256:" + "c" * 64,
                    },
                    {
                        **valid,
                        "comparison_audio_fingerprint": "sha256:" + "c" * 64,
                        "comparison_audio_fingerprints": [
                            "sha256:" + "b" * 64,
                            "sha256:" + "c" * 64,
                        ],
                    },
                    {
                        **valid,
                        "comparison_audio_fingerprint": "sha256:" + expected_sha,
                        "comparison_audio_fingerprints": ["sha256:" + expected_sha],
                    },
                ):
                    with self.subTest(invalid=invalid):
                        path.write_text(json.dumps(invalid), encoding="utf-8")
                        self.assertIsNone(
                            REDIFF.load_reusable_draft("episode", expected_sha)
                        )
                        self.assertFalse(path.exists())

    def test_rediff_draft_aliases_never_retire_or_publish_external_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            external = root / "external"
            external.mkdir()
            victim = external / "episode.dai-rediff.json"
            victim.write_text("{malformed", encoding="utf-8")
            sentinel = external / "sentinel"
            sentinel.write_text("unchanged", encoding="utf-8")
            alias = root / "Drafts"
            alias.symlink_to(external, target_is_directory=True)
            snapshot = root / "episode.mp3"
            snapshot.write_bytes(b"retained snapshot")
            snapshot_sha = hashlib.sha256(snapshot.read_bytes()).hexdigest()

            with mock.patch.object(REDIFF, "DRAFTS", alias):
                with self.assertRaisesRegex(REDIFF.RediffInputError, "symbolic link"):
                    REDIFF.load_reusable_draft("episode", snapshot_sha)
                with self.assertRaisesRegex(REDIFF.RediffInputError, "symbolic link"):
                    REDIFF.write_draft(
                        "new-episode",
                        snapshot,
                        [],
                        "b" * 64,
                        snapshot_sha,
                    )

            self.assertEqual(victim.read_text(encoding="utf-8"), "{malformed")
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "unchanged")
            self.assertEqual(
                sorted(path.name for path in external.iterdir()),
                ["episode.dai-rediff.json", "sentinel"],
            )

    def test_rediff_refuses_retained_a_as_fresh_comparison_b(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            snapshot = root / "episode.mp3"
            snapshot.write_bytes(b"retained snapshot A")
            snapshot_sha = hashlib.sha256(snapshot.read_bytes()).hexdigest()
            with (
                mock.patch.object(REDIFF, "DRAFTS", root / "drafts"),
                self.assertRaisesRegex(
                    REDIFF.RediffInputError,
                    "comparison B must differ from retained snapshot A",
                ),
            ):
                REDIFF.write_draft(
                    "episode",
                    snapshot,
                    [],
                    snapshot_sha,
                    snapshot_sha,
                )

    def test_rediff_manifest_is_fully_validated_before_use(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            audio_root = root / "TestFixtures/Corpus/Audio"
            snapshots = root / "TestFixtures/Corpus/Snapshots"
            audio_root.mkdir(parents=True)
            snapshots.mkdir(parents=True)
            audio = audio_root / "episode.mp3"
            audio.write_bytes(b"retained snapshot")
            digest = hashlib.sha256(audio.read_bytes()).hexdigest()
            manifest = snapshots / "manifest.json"
            valid = {
                "episodeId": "episode",
                "audioPath": "TestFixtures/Corpus/Audio/episode.mp3",
                "sha256": digest,
                "enclosureUrl": "https://example.test/episode.mp3",
            }
            with mock.patch.multiple(
                REDIFF,
                REPO=root,
                AUDIO_ROOT=audio_root,
                MANIFEST=manifest,
            ):
                manifest.write_text(json.dumps([valid]), encoding="utf-8")
                loaded = REDIFF.load_validated_manifest()
                self.assertEqual(loaded[0]["_snapshot_sha"], digest)

                invalid_manifests = (
                    [valid, dict(valid)],
                    [{**valid, "episodeId": "../episode"}],
                    [{**valid, "audioPath": "outside/episode.mp3"}],
                    [{**valid, "enclosureUrl": "file:///tmp/episode.mp3"}],
                    [valid, "not-an-object"],
                )
                for payload in invalid_manifests:
                    with self.subTest(payload=payload):
                        manifest.write_text(json.dumps(payload), encoding="utf-8")
                        with self.assertRaises(REDIFF.RediffInputError):
                            REDIFF.load_validated_manifest()

                real_manifest = snapshots / "real-manifest.json"
                real_manifest.write_text(json.dumps([valid]), encoding="utf-8")
                manifest.unlink()
                manifest.symlink_to(real_manifest)
                with self.assertRaisesRegex(REDIFF.RediffInputError, "symbolic link"):
                    REDIFF.load_validated_manifest()
                manifest.unlink()

                target = root / "outside-snapshot.mp3"
                target.write_bytes(audio.read_bytes())
                audio.unlink()
                audio.symlink_to(target)
                manifest.write_text(json.dumps([valid]), encoding="utf-8")
                with self.assertRaisesRegex(REDIFF.RediffInputError, "symbolic link"):
                    REDIFF.load_validated_manifest()

    def test_rediff_rejects_nonmonotonic_repeated_content_alignment(self) -> None:
        with (
            mock.patch.object(
                REDIFF,
                "fpcalc_raw",
                side_effect=[([1] * 100, 1.0), ([1] * 100, 1.0)],
            ),
            mock.patch.object(
                REDIFF,
                "find_runs",
                return_value=[(50, 0, 20, 0), (10, 20, 20, 0)],
            ),
            mock.patch.object(REDIFF, "merge_runs", side_effect=lambda runs: runs),
        ):
            result = REDIFF.rediff_pair(pathlib.Path("A"), pathlib.Path("B"))
        self.assertFalse(result["ok"])
        self.assertIn("non-monotonic-a", result["error"])

    def test_auto_promotion_thresholds_do_not_double_count_rediff_overlap(self) -> None:
        duplicate_r3 = AUTO_PROMOTE.build_clusters(
            [],
            [],
            [
                {"start": 0.0, "end": 11.0},
                {"start": 0.0, "end": 11.0},
            ],
        )[0]
        self.assertEqual(AUTO_PROMOTE.interval_union_seconds(duplicate_r3.rediff), 11.0)
        self.assertEqual(AUTO_PROMOTE.evaluate_cluster(duplicate_r3)["decision"], "reject")

        duplicate_r1 = AUTO_PROMOTE.build_clusters(
            [{"start": 0.0, "end": 4.5, "ad_type": "host_read"}],
            [],
            [
                {"start": 0.0, "end": 4.5},
                {"start": 0.0, "end": 4.5},
            ],
        )[0]
        self.assertEqual(AUTO_PROMOTE.evaluate_cluster(duplicate_r1)["decision"], "reject")
        self.assertEqual(
            AUTO_PROMOTE.interval_union_seconds(
                [
                    {"start": 0.0, "end": 10.0},
                    {"start": 10.0, "end": 20.0},
                ]
            ),
            20.0,
        )

    def test_r1_requires_each_published_rediff_interval_to_meet_the_rule(self) -> None:
        split_short = AUTO_PROMOTE.build_clusters(
            [{"start": 0.0, "end": 30.0, "ad_type": "host_read"}],
            [],
            [
                {"start": 1.0, "end": 7.0},
                {"start": 20.0, "end": 26.0},
            ],
        )[0]

        evaluation = AUTO_PROMOTE.evaluate_cluster(split_short)

        self.assertEqual(AUTO_PROMOTE.interval_union_seconds(split_short.rediff), 12.0)
        self.assertEqual(evaluation["decision"], "reject")

        overlapping_short = AUTO_PROMOTE.build_clusters(
            [{"start": 0.0, "end": 20.0, "ad_type": "host_read"}],
            [],
            [
                {"start": 0.0, "end": 6.0},
                {"start": 5.0, "end": 11.0},
            ],
        )[0]
        overlapping_evaluation = AUTO_PROMOTE.evaluate_cluster(overlapping_short)
        self.assertEqual(
            AUTO_PROMOTE.interval_union_seconds(overlapping_short.rediff), 11.0
        )
        self.assertEqual(overlapping_evaluation["decision"], "reject")

        one_qualified = AUTO_PROMOTE.build_clusters(
            [{"start": 0.0, "end": 30.0, "ad_type": "host_read"}],
            [],
            [
                {"start": 1.0, "end": 12.0},
                {"start": 20.0, "end": 26.0},
            ],
        )[0]
        qualified_evaluation = AUTO_PROMOTE.evaluate_cluster(one_qualified)
        self.assertEqual(qualified_evaluation["rule"], "R1")
        self.assertEqual(
            AUTO_PROMOTE.promotion_intervals(one_qualified, qualified_evaluation),
            [(1.0, 12.0)],
        )

        comparison_b = "sha256:" + "b" * 64
        comparison_c = "sha256:" + "c" * 64
        incompatible_comparisons = AUTO_PROMOTE.build_clusters(
            [{"start": 0.0, "end": 30.0, "ad_type": "host_read"}],
            [],
            [
                {
                    "start": 0.0,
                    "end": 12.0,
                    "comparison_audio_fingerprint": comparison_b,
                },
                {
                    "start": 10.0,
                    "end": 22.0,
                    "comparison_audio_fingerprint": comparison_c,
                },
            ],
        )[0]
        incompatible_evaluation = AUTO_PROMOTE.evaluate_cluster(
            incompatible_comparisons
        )
        self.assertEqual(incompatible_evaluation["decision"], "reject")
        self.assertEqual(
            incompatible_evaluation["rule"], "REDIFF_COMPARISON_CONFLICT"
        )
        self.assertEqual(
            AUTO_PROMOTE.r1_promotion_intervals(incompatible_comparisons),
            [],
        )

        incompatible_r3 = AUTO_PROMOTE.build_clusters(
            [],
            [],
            incompatible_comparisons.rediff,
        )[0]
        self.assertEqual(
            AUTO_PROMOTE.evaluate_cluster(incompatible_r3)["rule"],
            "REDIFF_COMPARISON_CONFLICT",
        )

        incompatible_r2 = AUTO_PROMOTE.build_clusters(
            [{"start": 5.0, "end": 15.0, "advertiser_guess": "Example"}],
            [{"start": 6.0, "end": 16.0, "skipConfidence": 0.95}],
            incompatible_comparisons.rediff,
        )[0]
        self.assertEqual(
            AUTO_PROMOTE.evaluate_cluster(incompatible_r2)["rule"],
            "REDIFF_COMPARISON_CONFLICT",
        )

        compatible_comparisons = AUTO_PROMOTE.build_clusters(
            [{"start": 0.0, "end": 40.0, "ad_type": "host_read"}],
            [],
            [
                {
                    "start": 0.0,
                    "end": 12.0,
                    "comparison_audio_fingerprint": comparison_b,
                },
                {
                    "start": 10.0,
                    "end": 24.0,
                    "comparison_audio_fingerprint": comparison_b,
                },
                {
                    "start": 24.0,
                    "end": 36.0,
                    "comparison_audio_fingerprint": comparison_c,
                },
            ],
        )[0]
        compatible_evaluation = AUTO_PROMOTE.evaluate_cluster(
            compatible_comparisons
        )
        self.assertEqual(compatible_evaluation["rule"], "R1")
        self.assertEqual(
            AUTO_PROMOTE.promotion_intervals(
                compatible_comparisons, compatible_evaluation
            ),
            [(0.0, 24.0), (24.0, 36.0)],
        )

        union_equivalent_comparisons = AUTO_PROMOTE.build_clusters(
            [{"start": 0.0, "end": 30.0, "ad_type": "host_read"}],
            [],
            [
                {
                    "start": 0.0,
                    "end": 10.0,
                    "comparison_audio_fingerprint": comparison_b,
                },
                {
                    "start": 10.0,
                    "end": 20.0,
                    "comparison_audio_fingerprint": comparison_b,
                },
                {
                    "start": 0.0,
                    "end": 20.0,
                    "comparison_audio_fingerprint": comparison_c,
                },
            ],
        )[0]
        union_equivalent_evaluation = AUTO_PROMOTE.evaluate_cluster(
            union_equivalent_comparisons
        )
        self.assertEqual(union_equivalent_evaluation["rule"], "R1")
        self.assertEqual(
            AUTO_PROMOTE.promotion_intervals(
                union_equivalent_comparisons, union_equivalent_evaluation
            ),
            [(0.0, 20.0)],
        )

        post_union_conflict = AUTO_PROMOTE.build_clusters(
            [{"start": 0.0, "end": 30.0, "ad_type": "host_read"}],
            [],
            [
                {
                    "start": 0.0,
                    "end": 10.0,
                    "comparison_audio_fingerprint": comparison_b,
                },
                {
                    "start": 10.0,
                    "end": 20.0,
                    "comparison_audio_fingerprint": comparison_b,
                },
                {
                    "start": 10.0,
                    "end": 20.0,
                    "comparison_audio_fingerprint": comparison_c,
                },
            ],
        )[0]
        self.assertEqual(
            AUTO_PROMOTE.evaluate_cluster(post_union_conflict)["rule"],
            "REDIFF_COMPARISON_CONFLICT",
        )

    def test_r2_requires_direct_drafter_pipeline_overlap(self) -> None:
        bridged = AUTO_PROMOTE.build_clusters(
            [
                {
                    "start": 0.0,
                    "end": 20.0,
                    "advertiser_guess": "Example",
                    "ad_type": "host_read",
                }
            ],
            [{"start": 24.0, "end": 40.0, "skipConfidence": 0.95}],
            [{"start": 19.0, "end": 25.0}],
        )[0]
        self.assertFalse(
            any(
                AUTO_PROMOTE._overlaps(drafter, pipeline)
                for drafter in bridged.drafter
                for pipeline in bridged.pipeline
            )
        )
        bridged_evaluation = AUTO_PROMOTE.evaluate_cluster(bridged)
        self.assertEqual(bridged_evaluation["decision"], "reject")
        self.assertIn("do not overlap (R2)", bridged_evaluation["confidence_notes"])

        directly_overlapping = AUTO_PROMOTE.build_clusters(
            [
                {
                    "start": 0.0,
                    "end": 20.0,
                    "advertiser_guess": "Example",
                    "ad_type": "host_read",
                }
            ],
            [{"start": 19.0, "end": 40.0, "skipConfidence": 0.95}],
            [],
        )[0]
        self.assertEqual(AUTO_PROMOTE.evaluate_cluster(directly_overlapping)["rule"], "R2")

        subthreshold_rediff_bridge = AUTO_PROMOTE.build_clusters(
            [{
                "start": 0.0,
                "end": 5.0,
                "advertiser_guess": "Example",
                "ad_type": "host_read",
            }],
            [{"start": 0.0, "end": 5.0, "skipConfidence": 0.95}],
            [
                {"start": 4.0, "end": 13.0},
                {"start": 12.0, "end": 21.0},
            ],
        )[0]
        self.assertEqual(subthreshold_rediff_bridge.length(), 21.0)
        self.assertEqual(
            AUTO_PROMOTE.r1_promotion_intervals(subthreshold_rediff_bridge),
            [],
        )
        subthreshold_evaluation = AUTO_PROMOTE.evaluate_cluster(
            subthreshold_rediff_bridge
        )
        self.assertEqual(subthreshold_evaluation["decision"], "reject")
        self.assertIn(
            "combined-len=5.0000<20.0 (R2; rediff excluded)",
            subthreshold_evaluation["confidence_notes"],
        )

        qualified_r2_with_short_rediff_tail = AUTO_PROMOTE.build_clusters(
            [{
                "start": 0.0,
                "end": 20.0,
                "advertiser_guess": "Example",
                "ad_type": "host_read",
            }],
            [{"start": 19.0, "end": 40.0, "skipConfidence": 0.95}],
            [{"start": 39.0, "end": 48.0}],
        )[0]
        evaluation = AUTO_PROMOTE.evaluate_cluster(qualified_r2_with_short_rediff_tail)
        self.assertEqual(evaluation["rule"], "R2")
        self.assertEqual(evaluation["provenance"], ["drafter", "pipeline"])
        self.assertEqual(
            AUTO_PROMOTE.promotion_intervals(
                qualified_r2_with_short_rediff_tail,
                evaluation,
            ),
            [(0.0, 40.0)],
        )

        multiple_r2_components = AUTO_PROMOTE.build_clusters(
            [
                {
                    "start": 0.0,
                    "end": 20.0,
                    "advertiser_guess": "Advertiser A",
                    "ad_type": "host_read",
                },
                {
                    "start": 50.0,
                    "end": 70.0,
                    "advertiser_guess": "Advertiser B",
                    "ad_type": "blended_host_read",
                },
            ],
            [
                {"start": 10.0, "end": 30.0, "skipConfidence": 0.95},
                {"start": 60.0, "end": 80.0, "skipConfidence": 0.95},
            ],
            [
                {"start": 29.0, "end": 38.0},
                {"start": 37.0, "end": 46.0},
                {"start": 45.0, "end": 51.0},
            ],
        )[0]
        self.assertEqual(
            AUTO_PROMOTE.r1_promotion_intervals(multiple_r2_components),
            [],
        )
        self.assertEqual(
            [
                (component.start, component.end)
                for component in AUTO_PROMOTE.r2_promotion_components(
                    multiple_r2_components
                )
            ],
            [(0.0, 30.0), (50.0, 80.0)],
        )
        ambiguous_evaluation = AUTO_PROMOTE.evaluate_cluster(
            multiple_r2_components
        )
        self.assertEqual(ambiguous_evaluation["decision"], "reject")
        self.assertIn(
            "multiple independent R2 components",
            ambiguous_evaluation["confidence_notes"],
        )
        self.assertEqual(
            AUTO_PROMOTE.promotion_intervals(
                multiple_r2_components,
                {"rule": "R2"},
            ),
            [],
        )

    def test_auto_promotion_required_evidence_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            drafts = pathlib.Path(tmp)
            original_drafts = AUTO_PROMOTE.DRAFTS
            AUTO_PROMOTE.DRAFTS = drafts
            try:
                episode_id = "episode"
                fingerprint = "sha256:" + "a" * 64
                draft = drafts / f"{episode_id}.draft.json"
                rediff = drafts / f"{episode_id}.dai-rediff.json"

                draft.write_text("{not-json}\n", encoding="utf-8")
                with self.assertRaisesRegex(
                    AUTO_PROMOTE.PromotionInputError,
                    "cannot read drafter evidence",
                ):
                    AUTO_PROMOTE.load_drafter_spans(episode_id, fingerprint)

                draft.write_text(
                    json.dumps({"episode_id": "other", "ad_windows": []}),
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(
                    AUTO_PROMOTE.PromotionInputError,
                    "episode_id does not match",
                ):
                    AUTO_PROMOTE.load_drafter_spans(episode_id, fingerprint)

                rediff.write_text(
                    json.dumps(
                        {
                            "episode_id": episode_id,
                            "audio_fingerprint": fingerprint,
                            "coordinate_space": "snapshot_a",
                            "comparison_audio_fingerprint": "sha256:" + "b" * 64,
                            "comparison_audio_fingerprints": ["sha256:" + "b" * 64],
                            "ad_windows": [
                                {"start_seconds": 10, "end_seconds": "invalid"}
                            ],
                        }
                    ),
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(
                    AUTO_PROMOTE.PromotionInputError,
                    "invalid bounds",
                ):
                    AUTO_PROMOTE.load_rediff_spans(episode_id, fingerprint)

                rediff.write_text(
                    json.dumps({
                        "episode_id": episode_id,
                        "audio_fingerprint": "sha256:" + "b" * 64,
                        "coordinate_space": "snapshot_a",
                        "ad_windows": [],
                    }),
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(
                    AUTO_PROMOTE.PromotionInputError,
                    "fingerprint does not match retained asset",
                ):
                    AUTO_PROMOTE.load_rediff_spans(episode_id, fingerprint)

                valid_rediff = {
                    "episode_id": episode_id,
                    "audio_fingerprint": fingerprint,
                    "coordinate_space": "snapshot_a",
                    "comparison_audio_fingerprint": "sha256:" + "b" * 64,
                    "comparison_audio_fingerprints": ["sha256:" + "b" * 64],
                    "ad_windows": [{"start_seconds": 10, "end_seconds": 20}],
                }
                invalid_comparisons = (
                    {
                        key: value
                        for key, value in valid_rediff.items()
                        if not key.startswith("comparison_audio_fingerprint")
                    },
                    {**valid_rediff, "comparison_audio_fingerprints": []},
                    {
                        **valid_rediff,
                        "comparison_audio_fingerprints": ["not-a-fingerprint"],
                    },
                    {
                        **valid_rediff,
                        "comparison_audio_fingerprints": [
                            "sha256:" + "b" * 64,
                            "sha256:" + "b" * 64,
                        ],
                    },
                    {
                        **valid_rediff,
                        "comparison_audio_fingerprint": "sha256:" + "c" * 64,
                    },
                    {
                        **valid_rediff,
                        "comparison_audio_fingerprint": "sha256:" + "c" * 64,
                        "comparison_audio_fingerprints": [
                            "sha256:" + "b" * 64,
                            "sha256:" + "c" * 64,
                        ],
                    },
                    {
                        **valid_rediff,
                        "comparison_audio_fingerprint": fingerprint,
                        "comparison_audio_fingerprints": [fingerprint],
                    },
                )
                for invalid in invalid_comparisons:
                    with self.subTest(invalid_comparisons=invalid):
                        rediff.write_text(json.dumps(invalid), encoding="utf-8")
                        with self.assertRaisesRegex(
                            AUTO_PROMOTE.PromotionInputError,
                            "comparison",
                        ):
                            AUTO_PROMOTE.load_rediff_spans(episode_id, fingerprint)

                rediff.write_text(json.dumps(valid_rediff), encoding="utf-8")
                self.assertEqual(
                    AUTO_PROMOTE.load_rediff_spans(episode_id, fingerprint),
                    [{
                        "start": 10.0,
                        "end": 20.0,
                        "confidence_notes": "",
                        "comparison_audio_fingerprint": "sha256:" + "b" * 64,
                    }],
                )

                negative_current_comparison = {
                    **valid_rediff,
                    "ad_windows": [{
                        "start_seconds": 10,
                        "end_seconds": 20,
                        "comparison_audio_fingerprint": "sha256:" + "b" * 64,
                    }],
                    "comparison_audio_fingerprint": "sha256:" + "c" * 64,
                    "comparison_audio_fingerprints": [
                        "sha256:" + "b" * 64,
                        "sha256:" + "c" * 64,
                    ],
                }
                rediff.write_text(
                    json.dumps(negative_current_comparison), encoding="utf-8"
                )
                self.assertEqual(
                    AUTO_PROMOTE.load_rediff_spans(episode_id, fingerprint),
                    [{
                        "start": 10.0,
                        "end": 20.0,
                        "confidence_notes": "",
                        "comparison_audio_fingerprint": "sha256:" + "b" * 64,
                    }],
                )
            finally:
                AUTO_PROMOTE.DRAFTS = original_drafts

    def test_present_pipeline_evidence_fails_closed_when_malformed(self) -> None:
        invalid_payloads = [
            "{not-json}",
            json.dumps({"episodes": {}}),
            json.dumps({"episodes": ["not-an-object"]}),
            json.dumps({"episodes": [{"episodeId": "episode"}]}),
            json.dumps(
                {
                    "episodes": [
                        {
                            "episodeId": "episode",
                            "adWindows": [{"startTime": 10, "endTime": 5}],
                        }
                    ]
                }
            ),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            path = root / "playhead-dogfood-diagnostics-pipeline-dump-test.json"
            with mock.patch.object(AUTO_PROMOTE, "REPO", root):
                for payload in invalid_payloads:
                    with self.subTest(payload=payload):
                        path.write_text(payload, encoding="utf-8")
                        with self.assertRaises(AUTO_PROMOTE.PromotionInputError):
                            AUTO_PROMOTE.latest_pipeline_dump()

        with self.assertRaisesRegex(AUTO_PROMOTE.PromotionInputError, "adWindows"):
            AUTO_PROMOTE.load_pipeline_spans(
                "episode",
                {"episode": {"episodeId": "episode"}},
                "sha256:" + "a" * 64,
            )

    def test_auto_promotion_rejects_aliased_evidence_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            external = root / "external"
            external.mkdir()
            fingerprint = "sha256:" + "a" * 64
            drafter = external / "episode.draft.json"
            drafter.write_text(
                json.dumps({
                    "episode_id": "episode",
                    "audio_fingerprint": fingerprint,
                    "ad_windows": [{"start_seconds": 1, "end_seconds": 20}],
                }),
                encoding="utf-8",
            )
            alias = root / "Drafts"
            alias.symlink_to(external, target_is_directory=True)

            with self.assertRaisesRegex(AUTO_PROMOTE.PromotionInputError, "symbolic link"):
                AUTO_PROMOTE._load_evidence_windows(
                    alias / drafter.name,
                    episode_id="episode",
                    source="drafter",
                    expected_fingerprint=fingerprint,
                )

            pipeline_target = external / "pipeline.json"
            pipeline_target.write_text('{"episodes":[]}', encoding="utf-8")
            pipeline_alias = root / "playhead-dogfood-diagnostics-pipeline-dump-alias.json"
            pipeline_alias.symlink_to(pipeline_target)
            with mock.patch.object(AUTO_PROMOTE, "REPO", root):
                with self.assertRaisesRegex(
                    AUTO_PROMOTE.PromotionInputError, "symbolic link"
                ):
                    AUTO_PROMOTE.latest_pipeline_dump()

            manifest_target = external / "manifest.json"
            manifest_target.write_text("[]", encoding="utf-8")
            manifest_parent = root / "Snapshots"
            manifest_parent.symlink_to(external, target_is_directory=True)
            with mock.patch.object(
                AUTO_PROMOTE, "MANIFEST", manifest_parent / "manifest.json"
            ):
                with self.assertRaisesRegex(
                    AUTO_PROMOTE.PromotionInputError, "symbolic link"
                ):
                    AUTO_PROMOTE.load_manifest()

            self.assertEqual(drafter.read_text(encoding="utf-8")[:1], "{")
            self.assertEqual(pipeline_target.read_text(encoding="utf-8"), '{"episodes":[]}')
            self.assertEqual(manifest_target.read_text(encoding="utf-8"), "[]")

    def test_auto_promotion_cannot_mix_coordinate_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            drafts = root / "drafts"
            drafts.mkdir()
            episode_id = "episode"
            audio = root / "TestFixtures/Corpus/Audio/episode.mp3"
            audio.parent.mkdir(parents=True)
            audio.write_bytes(b"retained-audio")
            digest = hashlib.sha256(audio.read_bytes()).hexdigest()
            a_fingerprint = "sha256:" + digest
            common = {"episode_id": episode_id, "ad_windows": []}
            (drafts / f"{episode_id}.draft.json").write_text(
                json.dumps({**common, "audio_fingerprint": a_fingerprint}),
                encoding="utf-8",
            )
            (drafts / f"{episode_id}.dai-rediff.json").write_text(
                json.dumps({
                    **common,
                    "audio_fingerprint": "sha256:" + "b" * 64,
                    "coordinate_space": "snapshot_a",
                }),
                encoding="utf-8",
            )
            with mock.patch.multiple(AUTO_PROMOTE, DRAFTS=drafts, REPO=root):
                with self.assertRaisesRegex(
                    AUTO_PROMOTE.PromotionInputError,
                    "fingerprint does not match retained asset",
                ):
                    AUTO_PROMOTE.process_episode(
                        episode_id,
                        {},
                        {episode_id: {
                            "sha256": digest,
                            "audioPath": "TestFixtures/Corpus/Audio/episode.mp3",
                        }},
                    )
        with self.assertRaisesRegex(AUTO_PROMOTE.PromotionInputError, "identity"):
            AUTO_PROMOTE.load_pipeline_spans(
                "episode",
                {"episode": {
                    "episodeId": "other",
                    "audioFingerprint": "sha256:" + "a" * 64,
                    "adWindows": [],
                }},
                "sha256:" + "a" * 64,
            )

    def test_auto_promotion_hashes_the_retained_manifest_audio(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            audio = root / "TestFixtures/Corpus/Audio/episode.mp3"
            audio.parent.mkdir(parents=True)
            audio.write_bytes(b"actual retained bytes")
            manifest = {"episode": {
                "sha256": "a" * 64,
                "audioPath": "TestFixtures/Corpus/Audio/episode.mp3",
            }}
            with mock.patch.object(AUTO_PROMOTE, "REPO", root):
                with self.assertRaisesRegex(
                    AUTO_PROMOTE.PromotionInputError, "fingerprint mismatch"
                ):
                    AUTO_PROMOTE.derive_fingerprint("episode", manifest)

    def test_auto_promotion_rejects_asset_change_between_evidence_and_output(self) -> None:
        expected = "sha256:" + "a" * 64
        changed = "sha256:" + "b" * 64
        with (
            mock.patch.object(AUTO_PROMOTE, "derive_fingerprint", return_value=changed),
            self.assertRaisesRegex(
                AUTO_PROMOTE.PromotionInputError, "changed while auto-promoting"
            ),
        ):
            AUTO_PROMOTE.build_annotation(
                "episode",
                [],
                {"episode": {"episodeDurationSeconds": 100}},
                {},
                expected,
            )

    def test_auto_promotion_rechecks_asset_under_publication_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            audio = root / "TestFixtures/Corpus/Audio/episode.mp3"
            audio.parent.mkdir(parents=True)
            audio.write_bytes(b"replacement retained bytes")
            digest = hashlib.sha256(audio.read_bytes()).hexdigest()
            proposed = annotation("episode")
            manifest = {"episode": {
                "sha256": digest,
                "audioPath": "TestFixtures/Corpus/Audio/episode.mp3",
            }}
            policy = AUTO_PROMOTE.auto_promotion_precommit(
                root / "annotations",
                root / "rejects.jsonl",
                manifest,
            )
            with (
                mock.patch.object(AUTO_PROMOTE, "REPO", root),
                self.assertRaisesRegex(
                    CANONICAL.CanonicalCorpusError, "changed before.*publication"
                ),
            ):
                policy({}, {"episode.json": proposed})

    def test_r1_output_keeps_exact_rediff_a_boundaries(self) -> None:
        cluster = AUTO_PROMOTE.build_clusters(
            [{
                "start": 0.0,
                "end": 200.0,
                "advertiser_guess": "Example",
                "ad_type": "host_read",
            }],
            [],
            [
                {"start": 40.0, "end": 60.0},
                {"start": 120.0, "end": 140.0},
            ],
        )[0]
        evaluation = AUTO_PROMOTE.evaluate_cluster(cluster)
        self.assertEqual(evaluation["rule"], "R1")
        self.assertEqual(
            AUTO_PROMOTE.promotion_intervals(cluster, evaluation),
            [(40.0, 60.0), (120.0, 140.0)],
        )
        fingerprint = "sha256:" + "a" * 64
        with mock.patch.object(
            AUTO_PROMOTE, "derive_fingerprint", return_value=fingerprint
        ):
            document = AUTO_PROMOTE.build_annotation(
                "episode",
                [(cluster, evaluation)],
                {"episode": {"episodeDurationSeconds": 240}},
                {},
                fingerprint,
            )
        self.assertEqual(
            [
                (window["start_seconds"], window["end_seconds"])
                for window in document["ad_windows"]
            ],
            [(40.0, 60.0), (120.0, 140.0)],
        )

    def test_rediff_rerun_retires_prior_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            drafts = pathlib.Path(tmp)
            stale = drafts / "episode.dai-rediff.json"
            stale.write_text('{"stale":true}\n', encoding="utf-8")
            with mock.patch.object(REDIFF, "DRAFTS", drafts):
                self.assertTrue(REDIFF.retire_stale_draft("episode"))
                self.assertFalse(stale.exists())
                self.assertFalse(REDIFF.retire_stale_draft("episode"))

    def test_present_snapshot_manifest_evidence_fails_closed_when_malformed(self) -> None:
        malformed_payloads = [
            "{not-json}",
            json.dumps({"episodeId": "episode"}),
            json.dumps(["not-an-object"]),
            json.dumps([{"episodeId": " episode"}]),
            json.dumps([
                {"episodeId": "episode"},
                {"episodeId": "episode"},
            ]),
            json.dumps([{"episodeId": "episode", "sha256": "not-a-sha256"}]),
            json.dumps([
                {
                    "episodeId": "episode",
                    "audioPath": "TestFixtures/Corpus/Audio/other.mp3",
                }
            ]),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            manifest = pathlib.Path(tmp) / "manifest.json"
            with mock.patch.object(AUTO_PROMOTE, "MANIFEST", manifest):
                for payload in malformed_payloads:
                    with self.subTest(payload=payload):
                        manifest.write_text(payload, encoding="utf-8")
                        with self.assertRaises(AUTO_PROMOTE.PromotionInputError):
                            AUTO_PROMOTE.load_manifest()

                manifest.write_text(
                    json.dumps([
                        {
                            "episodeId": "episode",
                            "show": "Example",
                            "audioPath": "TestFixtures/Corpus/Audio/episode.mp3",
                            "sha256": "a" * 64,
                        }
                    ]),
                    encoding="utf-8",
                )
                self.assertEqual(
                    AUTO_PROMOTE.load_manifest()["episode"]["sha256"],
                    "a" * 64,
                )

    def test_auto_promotion_main_rejects_malformed_snapshot_manifest_before_publication(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text("{not-json}", encoding="utf-8")
            with (
                mock.patch.multiple(
                    AUTO_PROMOTE,
                    MANIFEST=manifest,
                    DIAG_OUT=root / "diagnostics.json",
                ),
                mock.patch.object(
                    AUTO_PROMOTE,
                    "commit_canonical_annotations",
                ) as publish,
                mock.patch.object(
                    AUTO_PROMOTE,
                    "find_episode_pairs",
                    side_effect=AssertionError("manifest failure must abort first"),
                ),
                mock.patch.object(sys, "argv", ["l2f-auto-promote.py"]),
            ):
                self.assertEqual(AUTO_PROMOTE.main(), 2)

            publish.assert_not_called()
            self.assertFalse((root / "diagnostics.json").exists())

    def test_auto_promotion_missing_required_evidence_aborts_before_publication(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            drafts = root / "drafts"
            annotations = root / "annotations"
            drafts.mkdir()
            annotations.mkdir()
            episode_id = "episode"
            (drafts / f"{episode_id}.dai-rediff.json").write_text(
                json.dumps({"episode_id": episode_id, "ad_windows": []}),
                encoding="utf-8",
            )
            existing = annotations / "existing.json"
            existing.write_text(json.dumps(annotation("existing")), encoding="utf-8")
            manifest = annotations / CANONICAL.MANIFEST_FILENAME
            manifest.write_text(
                json.dumps(
                    {"schema_version": 1, "annotations": ["existing.json"]}
                ),
                encoding="utf-8",
            )
            original_manifest = manifest.read_bytes()

            with (
                mock.patch.multiple(
                    AUTO_PROMOTE,
                    DRAFTS=drafts,
                    ANNOTATIONS=annotations,
                    MANIFEST=root / "snapshot-manifest.json",
                    REJECTS_LOG=root / "rejects.jsonl",
                    DIAG_OUT=root / "diagnostics.json",
                ),
                mock.patch.object(
                    AUTO_PROMOTE,
                    "find_episode_pairs",
                    return_value=[episode_id],
                ),
                mock.patch.object(
                    AUTO_PROMOTE,
                    "latest_pipeline_dump",
                    return_value=(None, {}),
                ),
                mock.patch.object(AUTO_PROMOTE, "load_rejects", return_value={}),
                mock.patch.object(
                    AUTO_PROMOTE,
                    "commit_canonical_annotations",
                ) as publish,
                mock.patch.object(sys, "argv", ["l2f-auto-promote.py"]),
            ):
                self.assertEqual(AUTO_PROMOTE.main(), 2)

            publish.assert_not_called()
            self.assertFalse((annotations / f"{episode_id}.json").exists())
            self.assertEqual(manifest.read_bytes(), original_manifest)
            self.assertFalse((root / "diagnostics.json").exists())

    def test_reviewed_promotion_rejects_ambiguous_discovered_audio(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            audio_dir = root / "audio"
            audio_dir.mkdir()
            (audio_dir / "episode.m4a").write_bytes(b"first")
            (audio_dir / "episode.mp3").write_bytes(b"second")
            entries = [{"id": "episode#1", "episode_id": "episode"}]

            with self.assertRaisesRegex(ValueError, "ambiguous_audio"):
                REVIEW_PROMOTE.find_audio(
                    "episode",
                    entries,
                    {},
                    audio_dir,
                    root,
                )

    def test_reviewed_promotion_rejects_duplicate_review_entry_ids(self) -> None:
        entries = [
            {"id": "shared-review", "episode_id": "first"},
            {"id": "shared-review", "episode_id": "second"},
        ]

        with self.assertRaisesRegex(ValueError, "duplicate review entry id"):
            REVIEW_PROMOTE.validate_unique_entry_ids(entries)

    def test_reviewed_coordinates_require_matching_queue_and_review_fingerprints(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            audio_dir = root / "audio"
            audio_dir.mkdir()
            audio = audio_dir / "episode.wav"
            audio.write_bytes(b"exact retained review asset")
            entry = {
                "id": "episode#1",
                "episode_id": "episode",
                "show_name": "Example",
                "duration_seconds": 10,
                "audio_path": str(audio),
                "audio_fingerprint": "sha256:" + "a" * 64,
                "start_seconds": 2,
                "end_seconds": 4,
                "advertiser_guess": "Example",
                "product_guess": "Product",
                "ad_type": "host_read",
                "transition_type": "explicit",
            }
            review = {
                "status": "verified_ad",
                "audio_fingerprint": "sha256:" + "b" * 64,
                "advertiser": "Example",
                "product": "Product",
                "ad_type": "host_read",
                "transition_type": "explicit",
                "notes": "reviewed",
            }
            report = REVIEW_PROMOTE.analyze_episode(
                "episode", [entry], {entry["id"]: review}, audio_dir, root
            )
            mismatches = [
                issue for issue in report.issues
                if issue.startswith("audio_fingerprint_mismatch:")
            ]
            self.assertEqual(len(mismatches), 2)
            self.assertIsNone(report.annotation)

    def test_reviewed_promotion_rejects_conflicting_episode_invariants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            first_audio = root / "first.wav"
            second_audio = root / "second.wav"
            first_audio.write_bytes(b"first")
            second_audio.write_bytes(b"second")
            entries = [
                {
                    "id": "episode#1",
                    "episode_id": "episode",
                    "audio_path": str(first_audio),
                    "duration_seconds": 10,
                    "variant_of": "original-a",
                },
                {
                    "id": "episode#2",
                    "episode_id": "episode",
                    "audio_path": str(second_audio),
                    "duration_seconds": 20,
                    "variant_of": "original-b",
                },
            ]
            reviews = {
                "episode#1": {"show_name": "First Show"},
                "episode#2": {
                    "episode_id": "different-episode",
                    "show_name": "Second Show",
                },
            }
            report = REVIEW_PROMOTE.EpisodeReport("episode", entries)

            REVIEW_PROMOTE.validate_episode_invariants(
                report,
                entries,
                reviews,
                root,
            )

            issues = "\n".join(report.issues)
            self.assertIn("conflicting_episode_id:", issues)
            self.assertIn("conflicting_show_name:", issues)
            self.assertIn("conflicting_variant_of:", issues)
            self.assertIn("conflicting_audio_path:", issues)
            self.assertIn("conflicting_duration:", issues)

    def test_auto_promote_reject_ledger_is_fail_loud_and_unique(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "rejects.jsonl"
            valid = {
                "episodeId": "episode",
                "audioFingerprint": annotation("episode")["audio_fingerprint"],
                "startSeconds": 10,
                "endSeconds": 20,
            }
            path.write_text(json.dumps(valid) + "\n", encoding="utf-8")
            self.assertEqual(
                AUTO_PROMOTE.load_rejects(path),
                {("episode", annotation("episode")["audio_fingerprint"]): [(10.0, 20.0)]},
            )

            invalid_documents = [
                "{not-json}\n",
                json.dumps({"episodeId": "episode", "startSeconds": 20, "endSeconds": 10})
                + "\n",
                json.dumps({"episodeId": " episode", "startSeconds": 10, "endSeconds": 20})
                + "\n",
                (json.dumps(valid) + "\n") * 2,
            ]
            for document in invalid_documents:
                with self.subTest(document=document):
                    path.write_text(document, encoding="utf-8")
                    with self.assertRaises(AUTO_PROMOTE.RejectLedgerError):
                        AUTO_PROMOTE.load_rejects(path)

            path.unlink()
            path.symlink_to(path.parent / "missing-reject-ledger.jsonl")
            self.assertFalse(path.exists())
            with self.assertRaises(AUTO_PROMOTE.RejectLedgerError):
                AUTO_PROMOTE.load_rejects(path)

            external = pathlib.Path(tmp) / "external"
            external.mkdir()
            external_ledger = external / "rejects.jsonl"
            external_ledger.write_text(json.dumps(valid) + "\n", encoding="utf-8")
            alias = pathlib.Path(tmp) / "alias"
            alias.symlink_to(external, target_is_directory=True)
            with self.assertRaises(AUTO_PROMOTE.RejectLedgerError):
                AUTO_PROMOTE.load_rejects(alias / "rejects.jsonl")
            self.assertEqual(external_ledger.read_text(), json.dumps(valid) + "\n")

    def test_reviewed_promotion_enrolls_outputs_in_required_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            annotations = pathlib.Path(tmp)
            existing = annotations / "existing.json"
            existing.write_text(json.dumps(annotation("existing")), encoding="utf-8")
            manifest = annotations / REVIEW_PROMOTE.CANONICAL_MANIFEST_FILENAME
            manifest.write_text(
                json.dumps({"schema_version": 1, "annotations": ["existing.json"]}),
                encoding="utf-8",
            )

            written = REVIEW_PROMOTE.commit_canonical_annotations(
                annotations,
                {"promoted.json": annotation("promoted")},
                force=False,
            )

            self.assertEqual(written, [annotations / "promoted.json"])
            self.assertEqual(
                json.loads(manifest.read_text(encoding="utf-8")),
                {
                    "schema_version": 1,
                    "annotations": ["existing.json", "promoted.json"],
                },
            )

    def test_reviewed_promotion_requires_explicit_second_pass_before_gold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            rejects = root / "rejects.jsonl"
            audio = root / "episode.wav"
            audio.write_bytes(b"exact reviewed asset")
            fingerprint = REVIEW_PROMOTE.sha256_fingerprint(audio)
            first_pass = annotation_with_ad("episode", automatic=False)
            first_pass["audio_fingerprint"] = fingerprint
            first_pass["provenance"] = [REVIEW_PROMOTE.FIRST_PASS_PROVENANCE]
            first_pass["review_attestations"] = first_pass["review_attestations"][:1]
            first_pass["review_attestations"][0]["audio_fingerprint"] = fingerprint
            for window in first_pass["ad_windows"]:
                window["provenance"] = [REVIEW_PROMOTE.FIRST_PASS_PROVENANCE]
            second_pass = json.loads(json.dumps(first_pass))
            second_pass["provenance"] = [REVIEW_PROMOTE.SECOND_PASS_PROVENANCE]
            for window in second_pass["ad_windows"]:
                window["provenance"] = [REVIEW_PROMOTE.SECOND_PASS_PROVENANCE]
            second_pass["review_attestations"].append({
                "reviewer": "Reviewer Two",
                "reviewed_at": "2026-07-10T12:00:00Z",
                "audio_fingerprint": second_pass["audio_fingerprint"],
                "review_artifact_id": "sha256:" + "b" * 64,
            })

            self.assertFalse(AUTO_PROMOTE.is_gold_annotation(
                first_pass, require_review_artifacts=False
            ))
            self.assertTrue(AUTO_PROMOTE.is_gold_annotation(
                second_pass, require_review_artifacts=False
            ))
            policy = REVIEW_PROMOTE.reviewed_promotion_precommit(
                rejects,
                second_pass_reviewed=True,
                audio_paths_by_filename={"episode.json": audio},
            )
            policy({"episode.json": first_pass}, {"episode.json": second_pass})
            same_reviewer = json.loads(json.dumps(second_pass))
            same_reviewer["review_attestations"][1]["reviewer"] = "reviewer one"
            with self.assertRaisesRegex(
                CANONICAL.CanonicalCorpusError,
                "only advance provenance",
            ):
                policy({"episode.json": first_pass}, {"episode.json": same_reviewer})
            same_artifact = json.loads(json.dumps(second_pass))
            same_artifact["review_attestations"][1]["review_artifact_id"] = (
                same_artifact["review_attestations"][0]["review_artifact_id"]
            )
            with self.assertRaisesRegex(
                CANONICAL.CanonicalCorpusError,
                "only advance provenance",
            ):
                policy({"episode.json": first_pass}, {"episode.json": same_artifact})
            reordered_attestations = json.loads(json.dumps(second_pass))
            reordered_attestations["review_attestations"].reverse()
            with self.assertRaisesRegex(
                CANONICAL.CanonicalCorpusError,
                "only advance provenance",
            ):
                policy(
                    {"episode.json": first_pass},
                    {"episode.json": reordered_attestations},
                )
            changed_second_pass = json.loads(json.dumps(second_pass))
            changed_second_pass["show_name"] = "Changed during second pass"
            with self.assertRaisesRegex(
                CANONICAL.CanonicalCorpusError,
                "only advance provenance",
            ):
                policy(
                    {"episode.json": first_pass},
                    {"episode.json": changed_second_pass},
                )
            with self.assertRaisesRegex(
                CANONICAL.CanonicalCorpusError,
                "existing first-pass annotation",
            ):
                policy({}, {"episode.json": second_pass})

            first_pass_policy = REVIEW_PROMOTE.reviewed_promotion_precommit(
                rejects,
                second_pass_reviewed=False,
                audio_paths_by_filename={"episode.json": audio},
            )
            with self.assertRaisesRegex(CANONICAL.CanonicalCorpusError, "human-gold"):
                first_pass_policy(
                    {"episode.json": annotation_with_ad("episode", automatic=False)},
                    {"episode.json": first_pass},
                )

    def test_reviewed_promotion_rechecks_asset_under_publication_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            audio = root / "episode.wav"
            audio.write_bytes(b"reviewed bytes")
            candidate = annotation_with_ad("episode", automatic=False)
            candidate["audio_fingerprint"] = REVIEW_PROMOTE.sha256_fingerprint(audio)
            candidate["provenance"] = [REVIEW_PROMOTE.FIRST_PASS_PROVENANCE]
            candidate["review_attestations"] = []
            for window in candidate["ad_windows"]:
                window["provenance"] = [REVIEW_PROMOTE.FIRST_PASS_PROVENANCE]
            policy = REVIEW_PROMOTE.reviewed_promotion_precommit(
                root / "rejects.jsonl",
                second_pass_reviewed=False,
                audio_paths_by_filename={"episode.json": audio},
            )

            policy({}, {"episode.json": candidate})
            audio.write_bytes(b"mutated after review")
            with self.assertRaisesRegex(
                CANONICAL.CanonicalCorpusError,
                "retained audio changed before reviewed promotion publication",
            ):
                policy({}, {"episode.json": candidate})

    def test_reviewed_promotion_reloads_review_artifacts_under_publication_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            reviews_dir = root / "Reviews"
            audio = root / "episode.wav"
            audio.write_bytes(b"reviewed bytes")
            fingerprint = REVIEW_PROMOTE.sha256_fingerprint(audio)

            first_pass = annotation_with_ad("episode", automatic=False)
            first_pass["audio_fingerprint"] = fingerprint
            first_pass["provenance"] = [REVIEW_PROMOTE.FIRST_PASS_PROVENANCE]
            first_pass["review_attestations"] = []
            for window in first_pass["ad_windows"]:
                window["provenance"] = [REVIEW_PROMOTE.FIRST_PASS_PROVENANCE]
            first_artifact = {
                "schema_version": 1,
                "artifact_kind": "human_first_pass_attestation",
                "reviewer": "Reviewer One",
                "reviewed_at": "2026-05-12T03:06:35Z",
                "source_decision_count": 1,
                "audio_bindings": [{
                    "episode_id": "episode",
                    "audio_fingerprint": fingerprint,
                    "annotation_decision": REVIEW_PROMOTE.annotation_decision(first_pass),
                }],
            }
            first_id, _ = REVIEW_PROMOTE.persist_review_artifact(
                reviews_dir, first_artifact
            )
            first_pass["review_attestations"] = [{
                "reviewer": "Reviewer One",
                "reviewed_at": "2026-05-12T03:06:35Z",
                "audio_fingerprint": fingerprint,
                "review_artifact_id": first_id,
            }]

            second_pass = json.loads(json.dumps(first_pass))
            second_pass["provenance"] = [REVIEW_PROMOTE.SECOND_PASS_PROVENANCE]
            for window in second_pass["ad_windows"]:
                window["provenance"] = [REVIEW_PROMOTE.SECOND_PASS_PROVENANCE]
            decision_id = "episode#1"
            second_artifact = {
                "schema_version": 1,
                "artifact_kind": "corpus_review_attestation",
                "reviewer": "Reviewer Two",
                "reviewed_at": "2026-07-10T12:00:00Z",
                "episodes": [{
                    "episode_id": "episode",
                    "audio_fingerprint": fingerprint,
                    "decision_ids": [decision_id],
                    "annotation_decision": REVIEW_PROMOTE.annotation_decision(second_pass),
                }],
                "reviews": {
                    decision_id: {
                        "episode_id": "episode",
                        "reviewer": "Reviewer Two",
                        "reviewed_at": "2026-07-10T12:00:00Z",
                        "audio_fingerprint": fingerprint,
                        "status": "verified_ad",
                    }
                },
            }
            second_id, second_path = REVIEW_PROMOTE.persist_review_artifact(
                reviews_dir, second_artifact
            )
            second_pass["review_attestations"].append({
                "reviewer": "Reviewer Two",
                "reviewed_at": "2026-07-10T12:00:00Z",
                "audio_fingerprint": fingerprint,
                "review_artifact_id": second_id,
            })
            captured_index = REVIEW_PROMOTE.load_review_artifacts(reviews_dir)
            policy = REVIEW_PROMOTE.reviewed_promotion_precommit(
                root / "rejects.jsonl",
                second_pass_reviewed=True,
                audio_paths_by_filename={"episode.json": audio},
                artifact_index=captured_index,
                reviews_dir=reviews_dir,
            )

            policy({"episode.json": first_pass}, {"episode.json": second_pass})
            second_path.unlink()
            with self.assertRaisesRegex(
                CANONICAL.CanonicalCorpusError,
                "review evidence changed before publication",
            ):
                policy({"episode.json": first_pass}, {"episode.json": second_pass})

    def test_reviewed_promotion_refuses_missing_or_invalid_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            annotations = pathlib.Path(tmp)
            with self.assertRaisesRegex(ValueError, "canonical manifest"):
                CANONICAL.load_canonical_manifest(annotations)

            manifest = annotations / REVIEW_PROMOTE.CANONICAL_MANIFEST_FILENAME
            manifest.write_text(
                json.dumps({"schema_version": 1, "annotations": ["missing.json"]}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "missing or not a regular file"):
                CANONICAL.load_canonical_manifest(annotations)

            manifest.write_text(
                json.dumps({"schema_version": True, "annotations": ["missing.json"]}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "schema_version 1"):
                CANONICAL.load_canonical_manifest(annotations)

            template = annotations / "episode.example.json"
            template.write_text(
                json.dumps(annotation("episode.example")), encoding="utf-8"
            )
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "annotations": ["episode.example.json"],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "unsafe canonical annotation"):
                CANONICAL.load_canonical_manifest(annotations)

    def test_canonical_publication_rolls_back_the_whole_batch_on_write_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            annotations = pathlib.Path(tmp)
            existing_path = annotations / "existing.json"
            original = annotation("existing", show_name="Before")
            existing_path.write_text(json.dumps(original) + "\n", encoding="utf-8")
            manifest = annotations / CANONICAL.MANIFEST_FILENAME
            manifest.write_text(
                json.dumps({"schema_version": 1, "annotations": ["existing.json"]})
                + "\n",
                encoding="utf-8",
            )
            original_manifest = manifest.read_bytes()
            calls = 0

            def fail_on_second_write(path: pathlib.Path, data: bytes) -> None:
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("injected batch failure")
                CANONICAL._atomic_write_bytes(path, data)

            with self.assertRaisesRegex(OSError, "injected batch failure"):
                CANONICAL.commit_canonical_annotations(
                    annotations,
                    {
                        "first.json": annotation("first"),
                        "second.json": annotation("second"),
                    },
                    force=False,
                    writer=fail_on_second_write,
                )

            self.assertEqual(json.loads(existing_path.read_text()), original)
            self.assertFalse((annotations / "first.json").exists())
            self.assertFalse((annotations / "second.json").exists())
            self.assertEqual(manifest.read_bytes(), original_manifest)

    def test_canonical_reader_waits_for_failed_replacement_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            annotations = pathlib.Path(tmp)
            original = annotation("existing", show_name="Before")
            replacement = annotation("existing", show_name="Uncommitted")
            (annotations / "existing.json").write_text(
                json.dumps(original), encoding="utf-8"
            )
            manifest = annotations / CANONICAL.MANIFEST_FILENAME
            manifest.write_text(
                json.dumps(
                    {"schema_version": 1, "annotations": ["existing.json"]}
                ),
                encoding="utf-8",
            )
            replacement_written = threading.Event()
            finish_writer = threading.Event()

            def fail_manifest_write(path: pathlib.Path, data: bytes) -> None:
                if path == manifest:
                    replacement_written.set()
                    if not finish_writer.wait(timeout=5):
                        raise RuntimeError("test did not release publication writer")
                    raise OSError("injected manifest failure")
                CANONICAL._atomic_write_bytes(path, data)

            with ThreadPoolExecutor(max_workers=2) as pool:
                writer = pool.submit(
                    CANONICAL.commit_canonical_annotations,
                    annotations,
                    {"existing.json": replacement},
                    force=True,
                    writer=fail_manifest_write,
                )
                self.assertTrue(replacement_written.wait(timeout=5))
                reader = pool.submit(CANONICAL.load_canonical_annotations, annotations)
                with self.assertRaises(TimeoutError):
                    reader.result(timeout=0.1)
                finish_writer.set()
                with self.assertRaisesRegex(OSError, "injected manifest failure"):
                    writer.result(timeout=5)
                observed = reader.result(timeout=5)

            self.assertEqual(observed["existing.json"], original)

    def test_canonical_publication_lock_is_reentrant_on_one_thread(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            annotations = pathlib.Path(tmp)
            with CANONICAL.canonical_manifest_lock(annotations):
                with CANONICAL.canonical_manifest_lock(annotations):
                    self.assertTrue(
                        (annotations / ".canonical-manifest.lock").is_file()
                    )

    def test_earaudit_consumes_one_locked_canonical_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "Annotations"
            annotations.mkdir()
            original = annotation_with_ad("episode", automatic=True)
            replacement = annotation_with_ad("episode", automatic=True)
            replacement["ad_windows"][0]["start_seconds"] = 6
            replacement["ad_windows"][0]["end_seconds"] = 8
            replacement["content_windows"] = [
                {"start_seconds": 0, "end_seconds": 6, "notes": None},
                {"start_seconds": 8, "end_seconds": 10, "notes": None},
            ]
            (annotations / "episode.json").write_text(
                json.dumps(original), encoding="utf-8"
            )
            manifest = annotations / CANONICAL.MANIFEST_FILENAME
            manifest.write_text(
                json.dumps(
                    {"schema_version": 1, "annotations": ["episode.json"]}
                ),
                encoding="utf-8",
            )
            replacement_written = threading.Event()
            finish_writer = threading.Event()

            def fail_manifest_write(path: pathlib.Path, data: bytes) -> None:
                if path == manifest:
                    replacement_written.set()
                    if not finish_writer.wait(timeout=5):
                        raise RuntimeError("test did not release publication writer")
                    raise OSError("injected manifest failure")
                CANONICAL._atomic_write_bytes(path, data)

            with (
                mock.patch.object(EARAUDIT, "ANN_DIR", annotations),
                mock.patch.object(EARAUDIT, "MANIFEST", root / "missing.json"),
                ThreadPoolExecutor(max_workers=2) as pool,
            ):
                writer = pool.submit(
                    CANONICAL.commit_canonical_annotations,
                    annotations,
                    {"episode.json": replacement},
                    force=True,
                    writer=fail_manifest_write,
                )
                self.assertTrue(replacement_written.wait(timeout=5))
                reader = pool.submit(EARAUDIT.load_spans)
                with self.assertRaises(TimeoutError):
                    reader.result(timeout=0.1)
                finish_writer.set()
                with self.assertRaisesRegex(OSError, "injected manifest failure"):
                    writer.result(timeout=5)
                spans = reader.result(timeout=5)

            self.assertEqual([(span["start"], span["end"]) for span in spans], [(2, 4)])

    def test_concurrent_canonical_publications_preserve_both_memberships(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            annotations = pathlib.Path(tmp)
            (annotations / "existing.json").write_text(
                json.dumps(annotation("existing")), encoding="utf-8"
            )
            manifest = annotations / CANONICAL.MANIFEST_FILENAME
            manifest.write_text(
                json.dumps({"schema_version": 1, "annotations": ["existing.json"]}),
                encoding="utf-8",
            )

            def publish(episode_id: str) -> None:
                CANONICAL.commit_canonical_annotations(
                    annotations,
                    {f"{episode_id}.json": annotation(episode_id)},
                    force=False,
                )

            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(publish, ["first", "second"]))

            self.assertEqual(
                json.loads(manifest.read_text())["annotations"],
                ["existing.json", "first.json", "second.json"],
            )

    def test_canonical_publication_rejects_duplicate_audio_before_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            annotations = pathlib.Path(tmp)
            existing = annotation("existing")
            (annotations / "existing.json").write_text(json.dumps(existing), encoding="utf-8")
            manifest = annotations / CANONICAL.MANIFEST_FILENAME
            manifest.write_text(
                json.dumps({"schema_version": 1, "annotations": ["existing.json"]}),
                encoding="utf-8",
            )
            duplicate = annotation("duplicate")
            duplicate["audio_fingerprint"] = existing["audio_fingerprint"]

            with self.assertRaisesRegex(CANONICAL.CanonicalCorpusError, "unique audio"):
                CANONICAL.commit_canonical_annotations(
                    annotations,
                    {"duplicate.json": duplicate},
                    force=False,
                )

            self.assertFalse((annotations / "duplicate.json").exists())
            self.assertEqual(json.loads(manifest.read_text())["annotations"], ["existing.json"])

    def test_canonical_publication_revalidates_review_artifacts_under_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "Annotations"
            reviews = root / "custom-reviews"
            annotations.mkdir()
            (annotations / "existing.json").write_text(
                json.dumps(annotation("existing")), encoding="utf-8"
            )
            manifest = annotations / CANONICAL.MANIFEST_FILENAME
            manifest.write_text(
                json.dumps({"schema_version": 1, "annotations": ["existing.json"]}),
                encoding="utf-8",
            )

            candidate = annotation("reviewed")
            candidate["provenance"] = [REVIEW_PROMOTE.FIRST_PASS_PROVENANCE]
            artifact = {
                "schema_version": 1,
                "artifact_kind": "human_first_pass_attestation",
                "reviewer": "Reviewer",
                "reviewed_at": "2026-07-10T12:00:00Z",
                "source_decision_count": 1,
                "audio_bindings": [{
                    "episode_id": candidate["episode_id"],
                    "audio_fingerprint": candidate["audio_fingerprint"],
                    "annotation_decision": CONVERTER.annotation_decision(candidate),
                }],
            }
            artifact_id, artifact_path = REVIEW_PROMOTE.persist_review_artifact(
                reviews, artifact
            )
            candidate["review_attestations"] = [{
                "reviewer": artifact["reviewer"],
                "reviewed_at": artifact["reviewed_at"],
                "audio_fingerprint": candidate["audio_fingerprint"],
                "review_artifact_id": artifact_id,
            }]

            real_lock = CANONICAL.canonical_manifest_lock

            @contextlib.contextmanager
            def remove_artifact_before_lock(path: pathlib.Path):
                artifact_path.unlink()
                with real_lock(path):
                    yield

            with mock.patch.object(
                CANONICAL,
                "canonical_manifest_lock",
                remove_artifact_before_lock,
            ):
                with self.assertRaisesRegex(
                    CANONICAL.CanonicalCorpusError,
                    "does not resolve to matching review evidence",
                ):
                    CANONICAL.commit_canonical_annotations(
                        annotations,
                        {"reviewed.json": candidate},
                        force=False,
                        reviews_dir=reviews,
                    )

            self.assertFalse((annotations / "reviewed.json").exists())
            self.assertEqual(
                json.loads(manifest.read_text())["annotations"], ["existing.json"]
            )

    def test_canonical_publication_rejects_dangling_variant_before_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            annotations = pathlib.Path(tmp)
            existing = annotation("existing")
            (annotations / "existing.json").write_text(
                json.dumps(existing),
                encoding="utf-8",
            )
            manifest = annotations / CANONICAL.MANIFEST_FILENAME
            manifest.write_text(
                json.dumps({"schema_version": 1, "annotations": ["existing.json"]}),
                encoding="utf-8",
            )
            candidate = annotation("variant")
            candidate["variant_of"] = "missing-parent"

            with self.assertRaisesRegex(CANONICAL.CanonicalCorpusError, "non-canonical variant_of"):
                CANONICAL.commit_canonical_annotations(
                    annotations,
                    {"variant.json": candidate},
                    force=False,
                )

            self.assertFalse((annotations / "variant.json").exists())
            self.assertEqual(json.loads(manifest.read_text())["annotations"], ["existing.json"])

    def test_canonical_publication_rejects_multi_member_replacement_before_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            annotations = pathlib.Path(tmp)
            originals = {
                "first.json": annotation("first", show_name="Before first"),
                "second.json": annotation("second", show_name="Before second"),
            }
            for filename, document in originals.items():
                (annotations / filename).write_text(
                    json.dumps(document), encoding="utf-8"
                )
            manifest = annotations / CANONICAL.MANIFEST_FILENAME
            manifest.write_text(
                json.dumps(
                    {"schema_version": 1, "annotations": sorted(originals)}
                ),
                encoding="utf-8",
            )
            original_manifest = manifest.read_bytes()
            replacements = {
                filename: annotation(document["episode_id"], show_name="After")
                for filename, document in originals.items()
            }

            with self.assertRaisesRegex(
                CANONICAL.CanonicalCorpusError,
                "non-atomic replacement of multiple canonical members",
            ):
                CANONICAL.commit_canonical_annotations(
                    annotations,
                    replacements,
                    force=True,
                )

            for filename, document in originals.items():
                self.assertEqual(
                    json.loads((annotations / filename).read_text()), document
                )
            self.assertEqual(manifest.read_bytes(), original_manifest)

    def test_canonical_publication_rejects_replacement_mixed_with_new_member(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            annotations = pathlib.Path(tmp)
            original = annotation("existing", show_name="Before")
            existing_path = annotations / "existing.json"
            existing_path.write_text(json.dumps(original), encoding="utf-8")
            manifest = annotations / CANONICAL.MANIFEST_FILENAME
            manifest.write_text(
                json.dumps({"schema_version": 1, "annotations": ["existing.json"]}),
                encoding="utf-8",
            )
            original_manifest = manifest.read_bytes()

            with self.assertRaisesRegex(
                CANONICAL.CanonicalCorpusError,
                "mixes canonical replacement existing.json with new members: new.json",
            ):
                CANONICAL.commit_canonical_annotations(
                    annotations,
                    {
                        "existing.json": annotation("existing", show_name="After"),
                        "new.json": annotation("new"),
                    },
                    force=True,
                )

            self.assertEqual(json.loads(existing_path.read_text()), original)
            self.assertFalse((annotations / "new.json").exists())
            self.assertEqual(manifest.read_bytes(), original_manifest)

    def test_auto_force_refuses_to_overwrite_human_gold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            gold = annotation_with_ad("episode", automatic=False)
            path = annotations / "episode.json"
            path.write_text(json.dumps(gold), encoding="utf-8")
            (annotations / CANONICAL.MANIFEST_FILENAME).write_text(
                json.dumps({"schema_version": 1, "annotations": ["episode.json"]}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                CANONICAL.CanonicalCorpusError, "human-owned|review evidence"
            ):
                CANONICAL.commit_canonical_annotations(
                    annotations,
                    {"episode.json": annotation_with_ad("episode", automatic=True)},
                    force=True,
                    precommit=AUTO_PROMOTE.auto_promotion_precommit(
                        annotations,
                        root / "rejects.jsonl",
                    ),
                )

            self.assertEqual(json.loads(path.read_text()), gold)

    def test_auto_force_guard_casefolds_human_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            current = annotation_with_ad("episode", automatic=False)
            current["provenance"] = ["HUMAN_REVIEWED"]
            for window in current["ad_windows"]:
                window["provenance"] = ["HUMAN_REVIEWED"]
            replacement = annotation_with_ad("episode", automatic=True)
            policy = AUTO_PROMOTE.auto_promotion_precommit(
                root / "annotations",
                root / "rejects.jsonl",
            )

            with self.assertRaisesRegex(
                CANONICAL.CanonicalCorpusError, "human-owned"
            ):
                policy({"episode.json": current}, {"episode.json": replacement})

    def test_auto_force_refuses_to_overwrite_pending_human_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            pending = annotation_with_ad("episode", automatic=False)
            pending["provenance"] = [REVIEW_PROMOTE.FIRST_PASS_PROVENANCE]
            for window in pending["ad_windows"]:
                window["provenance"] = [REVIEW_PROMOTE.FIRST_PASS_PROVENANCE]
            path = annotations / "episode.json"
            path.write_text(json.dumps(pending), encoding="utf-8")
            (annotations / CANONICAL.MANIFEST_FILENAME).write_text(
                json.dumps({"schema_version": 1, "annotations": ["episode.json"]}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                CANONICAL.CanonicalCorpusError, "human-owned|review evidence"
            ):
                CANONICAL.commit_canonical_annotations(
                    annotations,
                    {"episode.json": annotation_with_ad("episode", automatic=True)},
                    force=True,
                    precommit=AUTO_PROMOTE.auto_promotion_precommit(
                        annotations,
                        root / "rejects.jsonl",
                    ),
                )

            self.assertEqual(json.loads(path.read_text()), pending)

    def test_auto_force_allows_replacing_an_existing_automatic_annotation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            current = annotation_with_ad("episode", automatic=True)
            path = annotations / "episode.json"
            path.write_text(json.dumps(current), encoding="utf-8")
            (annotations / CANONICAL.MANIFEST_FILENAME).write_text(
                json.dumps({"schema_version": 1, "annotations": ["episode.json"]}),
                encoding="utf-8",
            )
            replacement = annotation_with_ad("episode", automatic=True)
            replacement["show_name"] = "Updated automatic label"

            CANONICAL.commit_canonical_annotations(
                annotations,
                {"episode.json": replacement},
                force=True,
                precommit=AUTO_PROMOTE.auto_promotion_precommit(
                    annotations,
                    root / "rejects.jsonl",
                ),
            )

            self.assertEqual(json.loads(path.read_text()), replacement)

    def test_stale_auto_plan_cannot_restore_a_newly_demoted_reject(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            rejects = root / "rejects.jsonl"
            stale = annotation_with_ad("episode", automatic=True)
            path = annotations / "episode.json"
            path.write_text(json.dumps(stale), encoding="utf-8")
            (annotations / CANONICAL.MANIFEST_FILENAME).write_text(
                json.dumps({"schema_version": 1, "annotations": ["episode.json"]}),
                encoding="utf-8",
            )
            DEMOTE.demote(
                annotations_dir=annotations,
                rejects_path=rejects,
                episode_prefix="episode",
                requested_start=2,
                reason="ear-verified content",
            )

            with self.assertRaisesRegex(
                CANONICAL.CanonicalCorpusError, "permanently rejected|review evidence"
            ):
                CANONICAL.commit_canonical_annotations(
                    annotations,
                    {"episode.json": stale},
                    force=True,
                    precommit=AUTO_PROMOTE.auto_promotion_precommit(annotations, rejects),
                )

            self.assertEqual(json.loads(path.read_text())["ad_windows"], [])
            self.assertEqual(len(rejects.read_text().splitlines()), 1)

    def test_reviewed_promotion_cannot_restore_a_permanent_reject(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            (annotations / "existing.json").write_text(
                json.dumps(annotation("existing")), encoding="utf-8"
            )
            manifest = annotations / CANONICAL.MANIFEST_FILENAME
            manifest.write_text(
                json.dumps({"schema_version": 1, "annotations": ["existing.json"]}),
                encoding="utf-8",
            )
            rejects = root / "rejects.jsonl"
            rejects.write_text(json.dumps({
                "episodeId": "reviewed",
                "audioFingerprint": annotation("reviewed")["audio_fingerprint"],
                "startSeconds": 2,
                "endSeconds": 4,
            }) + "\n", encoding="utf-8")

            with self.assertRaisesRegex(
                CANONICAL.CanonicalCorpusError, "permanently rejected|review evidence"
            ):
                CANONICAL.commit_canonical_annotations(
                    annotations,
                    {"reviewed.json": annotation_with_ad("reviewed", automatic=False)},
                    force=False,
                    precommit=CANONICAL.reject_veto_precommit(rejects),
                )

            self.assertFalse((annotations / "reviewed.json").exists())
            self.assertEqual(json.loads(manifest.read_text())["annotations"], ["existing.json"])

    def test_auto_promotion_validation_rejects_incomplete_annotation(self) -> None:
        invalid = annotation("episode")
        invalid["duration_seconds"] = None
        with self.assertRaisesRegex(CANONICAL.CanonicalCorpusError, "finite number"):
            AUTO_PROMOTE.validate_canonical_annotation(invalid, filename="episode.json")

    def test_permanent_reject_matching_does_not_veto_adjacent_distinct_span(self) -> None:
        retained = AUTO_PROMOTE.Cluster(2818.54, 2851.24)
        reject = (2854.30, 3211.10)
        self.assertIsNone(AUTO_PROMOTE._cluster_is_rejected(retained, [reject]))

        jittered_same_span = AUTO_PROMOTE.Cluster(2856.0, 3208.0)
        self.assertEqual(
            AUTO_PROMOTE._cluster_is_rejected(jittered_same_span, [reject]), reject
        )

        exact_content = (100.0, 200.0)
        self.assertEqual(
            CANONICAL.matching_reject(150.0, 250.0, [exact_content]),
            exact_content,
        )
        self.assertEqual(
            CANONICAL.matching_reject(0.0, 300.0, [exact_content]),
            exact_content,
        )
        self.assertIsNone(CANONICAL.matching_reject(200.0, 250.0, [exact_content]))

    def test_permanent_reject_veto_is_scoped_to_exact_audio_asset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = pathlib.Path(tmp) / "rejects.jsonl"
            a = annotation_with_ad("episode", automatic=True)
            b = json.loads(json.dumps(a))
            b["audio_fingerprint"] = "sha256:" + "f" * 64
            ledger.write_text(json.dumps({
                "episodeId": "episode",
                "audioFingerprint": a["audio_fingerprint"],
                "startSeconds": 2,
                "endSeconds": 4,
            }) + "\n")
            policy = CANONICAL.reject_veto_precommit(ledger)
            policy({}, {"episode.json": b})
            with self.assertRaisesRegex(CANONICAL.CanonicalCorpusError, "permanently rejected"):
                policy({}, {"episode.json": a})


if __name__ == "__main__":
    unittest.main()
