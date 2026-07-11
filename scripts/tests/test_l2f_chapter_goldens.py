#!/usr/bin/env python3
"""Regression tests for manifest-driven, gold-only chapter golden conversion."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import pathlib
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from unittest import mock


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "convert_annotations_to_chapter_goldens.py"
SPEC = importlib.util.spec_from_file_location("convert_annotations_to_chapter_goldens", SCRIPT)
assert SPEC and SPEC.loader
CONVERTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONVERTER)


def is_gold(document: object) -> bool:
    return CONVERTER.is_gold_annotation(
        document, require_review_artifacts=False
    )


def annotation(*, window: dict | None = None, **metadata: object) -> dict:
    episode_id = str(metadata.get("episode_id", "episode"))
    content_windows = (
        [{"start_seconds": 0, "end_seconds": 10, "notes": None}]
        if window is None
        else [
            {"start_seconds": 0, "end_seconds": window["start_seconds"], "notes": None},
            {"start_seconds": window["end_seconds"], "end_seconds": 10, "notes": None},
        ]
    )
    document = {
        "episode_id": "episode",
        "show_name": "Example",
        "duration_seconds": 10,
        "audio_fingerprint": "sha256:"
        + hashlib.sha256(episode_id.encode("utf-8")).hexdigest(),
        "ad_windows": [window] if window is not None else [],
        "content_windows": content_windows,
        "variant_of": None,
        "provenance": ["human_reviewed"],
    }
    document.update(metadata)
    if document.get("provenance") == ["human_reviewed"] and "review_attestations" not in metadata:
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


class ChapterGoldenConversionTests(unittest.TestCase):
    def test_canonical_publisher_rejects_symlinked_annotations_root(self) -> None:
        import l2f_canonical_manifest as canonical

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            outside = root / "outside"
            outside.mkdir()
            original = annotation(
                episode_id="episode", provenance=["human_first_pass"]
            )
            (outside / "episode.json").write_text(json.dumps(original), encoding="utf-8")
            manifest = outside / CONVERTER.MANIFEST_FILENAME
            manifest.write_text(
                json.dumps({"schema_version": 1, "annotations": ["episode.json"]}),
                encoding="utf-8",
            )
            annotations = root / "Annotations"
            annotations.symlink_to(outside, target_is_directory=True)
            candidate = annotation(
                episode_id="new-episode", provenance=["human_first_pass"]
            )

            with self.assertRaisesRegex(canonical.CanonicalCorpusError, "regular directory"):
                canonical.commit_canonical_annotations(
                    annotations,
                    {"new-episode.json": candidate},
                    force=False,
                )

            self.assertFalse((outside / "new-episode.json").exists())
            self.assertEqual(
                json.loads(manifest.read_text()),
                {"schema_version": 1, "annotations": ["episode.json"]},
            )

    def test_canonical_publisher_rejects_symlinked_annotations_parent(self) -> None:
        import l2f_canonical_manifest as canonical

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            outside = root / "outside"
            annotations_target = outside / "nested" / "Corpus" / "Annotations"
            annotations_target.mkdir(parents=True)
            original = annotation(
                episode_id="episode", provenance=["human_first_pass"]
            )
            (annotations_target / "episode.json").write_text(
                json.dumps(original), encoding="utf-8"
            )
            manifest = annotations_target / CONVERTER.MANIFEST_FILENAME
            manifest.write_text(
                json.dumps({"schema_version": 1, "annotations": ["episode.json"]}),
                encoding="utf-8",
            )
            alias = root / "Corpus"
            alias.symlink_to(outside, target_is_directory=True)
            candidate = annotation(
                episode_id="new-episode", provenance=["human_first_pass"]
            )

            with self.assertRaisesRegex(canonical.CanonicalCorpusError, "regular directory"):
                canonical.commit_canonical_annotations(
                    alias / "nested" / "Corpus" / "Annotations",
                    {"new-episode.json": candidate},
                    force=False,
                    reviews_dir=root / "Reviews",
                )

            self.assertFalse((annotations_target / "new-episode.json").exists())
            self.assertFalse((annotations_target / ".canonical-manifest.lock").exists())
            self.assertEqual(
                json.loads(manifest.read_text()),
                {"schema_version": 1, "annotations": ["episode.json"]},
            )

    def test_review_artifact_loader_rejects_symlinked_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            outside = root / "outside"
            outside.mkdir()
            data = b'{"schema_version":1}\n'
            artifact = outside / f"{hashlib.sha256(data).hexdigest()}.json"
            artifact.write_bytes(data)
            reviews = root / "Reviews"
            reviews.symlink_to(outside, target_is_directory=True)

            with self.assertRaisesRegex(CONVERTER.ConversionError, "symbolic link"):
                CONVERTER.load_review_artifacts(reviews)

    def test_review_artifact_loader_rejects_symlinked_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            outside = root / "outside"
            reviews_target = outside / "nested" / "Corpus" / "Reviews"
            reviews_target.mkdir(parents=True)
            data = b'{"schema_version":1}\n'
            artifact = reviews_target / f"{hashlib.sha256(data).hexdigest()}.json"
            artifact.write_bytes(data)
            alias = root / "Corpus"
            alias.symlink_to(outside, target_is_directory=True)

            with self.assertRaisesRegex(CONVERTER.ConversionError, "symbolic link"):
                CONVERTER.load_review_artifacts(
                    alias / "nested" / "Corpus" / "Reviews"
                )

    def test_manifest_schema_version_rejects_json_boolean(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            annotations = pathlib.Path(tmp)
            (annotations / CONVERTER.MANIFEST_FILENAME).write_text(
                json.dumps({"schema_version": True, "annotations": ["episode.json"]}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(CONVERTER.ConversionError, "schema_version 1"):
                CONVERTER.canonical_annotation_paths(annotations)

    def test_conversion_plan_waits_for_failed_replacement_rollback(self) -> None:
        import l2f_canonical_manifest as canonical

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "Annotations"
            reviews = root / "Reviews"
            annotations.mkdir()
            original = annotation(
                episode_id="episode", provenance=["human_first_pass"]
            )
            replacement = annotation(
                episode_id="episode", provenance=["human_first_pass"],
                show_name="Uncommitted",
            )
            (annotations / "episode.json").write_text(
                json.dumps(original), encoding="utf-8"
            )
            manifest = annotations / CONVERTER.MANIFEST_FILENAME
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
                canonical._atomic_write_bytes(path, data)

            with ThreadPoolExecutor(max_workers=2) as pool:
                writer = pool.submit(
                    canonical.commit_canonical_annotations,
                    annotations,
                    {"episode.json": replacement},
                    force=True,
                    writer=fail_manifest_write,
                )
                self.assertTrue(replacement_written.wait(timeout=5))
                reader = pool.submit(
                    CONVERTER.build_conversion_plan,
                    annotations,
                    reviews,
                )
                with self.assertRaises(TimeoutError):
                    reader.result(timeout=0.1)
                finish_writer.set()
                with self.assertRaisesRegex(OSError, "injected manifest failure"):
                    writer.result(timeout=5)
                planned, skipped = reader.result(timeout=5)

            self.assertEqual(planned, {})
            self.assertEqual(skipped, 1)
            self.assertEqual(
                json.loads((annotations / "episode.json").read_text()), original
            )

    def test_regeneration_holds_snapshot_lock_through_pointer_publication(self) -> None:
        import l2f_canonical_manifest as canonical

        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "Annotations"
            reviews = root / "Reviews"
            generations = root / ".dogfood-generations"
            previous = generations / "previous"
            empty = generations / "empty"
            dogfood = root / "dogfood"
            annotations.mkdir()
            previous.mkdir(parents=True)
            empty.mkdir()
            (empty / ".gitkeep").touch()
            dogfood.symlink_to(pathlib.Path(".dogfood-generations/previous"))
            original = annotation(
                episode_id="episode", provenance=["human_first_pass"]
            )
            replacement = annotation(
                episode_id="episode",
                provenance=["human_first_pass"],
                show_name="Committed after generation",
            )
            (annotations / "episode.json").write_text(
                json.dumps(original), encoding="utf-8"
            )
            (annotations / CONVERTER.MANIFEST_FILENAME).write_text(
                json.dumps(
                    {"schema_version": 1, "annotations": ["episode.json"]}
                ),
                encoding="utf-8",
            )
            publication_started = threading.Event()
            finish_publication = threading.Event()
            writer_started = threading.Event()
            real_publish = CONVERTER.publish_versioned_generation

            def paused_publish(planned: dict[str, dict], output: pathlib.Path) -> None:
                publication_started.set()
                if not finish_publication.wait(timeout=5):
                    raise RuntimeError("test did not release golden publication")
                real_publish(planned, output)

            def replace_canonical() -> None:
                writer_started.set()
                canonical.commit_canonical_annotations(
                    annotations,
                    {"episode.json": replacement},
                    force=True,
                )

            with (
                mock.patch.object(
                    CONVERTER,
                    "publish_versioned_generation",
                    side_effect=paused_publish,
                ),
                ThreadPoolExecutor(max_workers=2) as pool,
            ):
                regeneration = pool.submit(
                    CONVERTER.regenerate,
                    annotations_dir=annotations,
                    dogfood_dir=dogfood,
                    reviews_dir=reviews,
                    require_review_artifacts=False,
                )
                self.assertTrue(publication_started.wait(timeout=5))
                writer = pool.submit(replace_canonical)
                self.assertTrue(writer_started.wait(timeout=5))
                with self.assertRaises(TimeoutError):
                    writer.result(timeout=0.1)
                finish_publication.set()
                self.assertEqual(regeneration.result(timeout=5), (0, 1, 0))
                writer.result(timeout=5)

            self.assertEqual(
                json.loads((annotations / "episode.json").read_text()), replacement
            )

    def test_regeneration_retires_stale_generation_when_gold_set_becomes_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            generations = root / ".dogfood-generations"
            previous = generations / "previous"
            empty = generations / "empty"
            dogfood = root / "dogfood"
            annotations.mkdir()
            previous.mkdir(parents=True)
            empty.mkdir()
            (empty / ".gitkeep").touch()
            dogfood.symlink_to(pathlib.Path(".dogfood-generations/previous"))
            proposal = annotation(
                auto_promoted=True,
                auto_promoted_by="test-auto-promoter",
            )
            (annotations / "episode.json").write_text(
                json.dumps(proposal), encoding="utf-8"
            )
            (annotations / CONVERTER.MANIFEST_FILENAME).write_text(
                json.dumps(
                    {"schema_version": 1, "annotations": ["episode.json"]}
                ),
                encoding="utf-8",
            )
            existing = dogfood / "episode.json"
            original = b'{"existing":true}\n'
            existing.write_bytes(original)

            result = CONVERTER.regenerate(
                annotations_dir=annotations,
                dogfood_dir=dogfood,
                require_review_artifacts=False,
            )

            self.assertEqual(result, (0, 1, 1))
            self.assertTrue(dogfood.is_symlink())
            self.assertEqual(
                CONVERTER.os.readlink(dogfood), ".dogfood-generations/empty"
            )
            self.assertEqual([path.name for path in dogfood.iterdir()], [".gitkeep"])
            self.assertEqual(
                {path.name for path in generations.iterdir()}, {"empty", "previous"}
            )
            self.assertEqual(list(dogfood.glob("*.json")), [])
            self.assertEqual((previous / "episode.json").read_bytes(), original)

    def test_canonical_manifest_is_the_only_input_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "listed.json").write_text("{}", encoding="utf-8")
            (root / "unlisted.json").write_text("{}", encoding="utf-8")
            (root / CONVERTER.MANIFEST_FILENAME).write_text(
                json.dumps({"schema_version": 1, "annotations": ["listed.json"]}),
                encoding="utf-8",
            )

            self.assertEqual(
                CONVERTER.canonical_annotation_paths(root),
                [root / "listed.json"],
            )

    def test_canonical_manifest_rejects_template_members(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / CONVERTER.MANIFEST_FILENAME).write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "annotations": ["episode.example.json"],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(CONVERTER.ConversionError, "unsafe canonical"):
                CONVERTER.canonical_annotation_paths(root)

    def test_canonical_manifest_must_be_a_regular_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            outside = root / "outside-manifest.json"
            outside.write_text(
                json.dumps({"schema_version": 1, "annotations": ["listed.json"]}),
                encoding="utf-8",
            )
            (root / "listed.json").write_text("{}", encoding="utf-8")
            (root / CONVERTER.MANIFEST_FILENAME).symlink_to(outside)

            with self.assertRaisesRegex(CONVERTER.ConversionError, "regular file"):
                CONVERTER.canonical_annotation_paths(root)

    def test_only_all_human_annotations_are_gold(self) -> None:
        human_window = {
            "start_seconds": 1,
            "end_seconds": 2,
            "advertiser": None,
            "product": None,
            "ad_type": "host_read",
            "transition_type": "explicit",
            "confidence_notes": None,
            "provenance": ["human_reviewed"],
        }
        auto_window = {
            "start_seconds": 1,
            "end_seconds": 2,
            "advertiser": None,
            "product": None,
            "ad_type": "dai",
            "transition_type": None,
            "confidence_notes": None,
            "auto_promoted": True,
            "provenance": ["rediff"],
            "audit_priority": 1,
        }

        self.assertTrue(
            is_gold(
                annotation(window=human_window, provenance=["human_reviewed"])
            )
        )
        missing_episode_provenance = annotation(window=human_window)
        missing_episode_provenance.pop("provenance")
        self.assertFalse(is_gold(missing_episode_provenance))
        missing_window_provenance = dict(human_window)
        missing_window_provenance.pop("provenance")
        self.assertFalse(
            is_gold(annotation(window=missing_window_provenance))
        )
        for insufficient in (["human"], ["manual"]):
            with self.subTest(insufficient=insufficient):
                self.assertFalse(
                    is_gold(
                        annotation(window=human_window, provenance=insufficient)
                    )
                )
        self.assertFalse(is_gold(annotation(window=auto_window)))
        marker_only_window = dict(human_window, auto_promoted_by="future-tool")
        self.assertFalse(is_gold(annotation(window=marker_only_window)))
        self.assertFalse(
            is_gold(
                annotation(window=human_window, auto_promoted_by="future-tool")
            )
        )
        self.assertFalse(
            is_gold(
                annotation(window=human_window, provenance=["future_source"])
            )
        )
        self.assertFalse(
            is_gold(
                annotation(window=human_window, provenance=[])
            )
        )
        self.assertFalse(
            is_gold(
                annotation(window=human_window, audit_priority=0)
            )
        )
        self.assertFalse(
            is_gold(
                annotation(window=dict(human_window, audit_priority=0))
            )
        )

        overflow = annotation(audit_priority=2**63)
        with self.assertRaisesRegex(CONVERTER.ConversionError, "signed 64-bit integer"):
            CONVERTER.validate_annotation(
                overflow,
                source="overflow.json",
                require_review_artifacts=False,
            )

    def test_committed_chapter_goldens_exactly_match_canonical_gold_episodes(self) -> None:
        paths = CONVERTER.canonical_annotation_paths()
        documents = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
        gold_ids = {
            document["episode_id"]
            for document in documents
            if is_gold(document)
        }
        golden_ids = {path.stem for path in CONVERTER.DOGFOOD_DIR.glob("*.json")}

        self.assertEqual(len(paths), 12)
        self.assertEqual(len(gold_ids), 0)
        self.assertEqual(golden_ids, gold_ids)

    def test_regeneration_reconciles_stale_non_gold_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            dogfood = root / "dogfood"
            annotations.mkdir()
            dogfood.mkdir()
            gold = annotation()
            proposal = annotation(
                episode_id="proposal",
                auto_promoted=True,
                auto_promoted_by="test-auto-promoter",
            )
            (annotations / "episode.json").write_text(json.dumps(gold), encoding="utf-8")
            (annotations / "proposal.json").write_text(
                json.dumps(proposal), encoding="utf-8"
            )
            (annotations / CONVERTER.MANIFEST_FILENAME).write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "annotations": ["episode.json", "proposal.json"],
                    }
                ),
                encoding="utf-8",
            )
            stale = dogfood / "proposal.json"
            stale.write_text("{}", encoding="utf-8")
            readme = dogfood / "README.md"
            readme.write_text("not converter output", encoding="utf-8")

            messages: list[str] = []
            self.assertEqual(
                CONVERTER.regenerate(
                    annotations_dir=annotations,
                    dogfood_dir=dogfood,
                    require_review_artifacts=False,
                    dry_run=True,
                    emit=messages.append,
                ),
                (1, 1, 1),
            )
            self.assertTrue(stale.exists())
            self.assertTrue(any("would remove stale generated golden" in row for row in messages))

            self.assertEqual(
                CONVERTER.regenerate(
                    annotations_dir=annotations,
                    dogfood_dir=dogfood,
                    require_review_artifacts=False,
                    emit=lambda _: None,
                ),
                (1, 1, 1),
            )
            self.assertTrue((dogfood / "episode.json").is_file())
            self.assertFalse(stale.exists())
            self.assertEqual(readme.read_text(encoding="utf-8"), "not converter output")

    def test_invalid_partition_aborts_before_any_output_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            dogfood = root / "dogfood"
            annotations.mkdir()
            dogfood.mkdir()

            valid = annotation()
            invalid = annotation(episode_id="invalid")
            invalid["content_windows"] = [
                {"start_seconds": 1, "end_seconds": 10, "notes": None}
            ]
            (annotations / "episode.json").write_text(json.dumps(valid), encoding="utf-8")
            (annotations / "invalid.json").write_text(json.dumps(invalid), encoding="utf-8")
            (annotations / CONVERTER.MANIFEST_FILENAME).write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "annotations": ["episode.json", "invalid.json"],
                    }
                ),
                encoding="utf-8",
            )
            sentinel = dogfood / "sentinel.json"
            sentinel.write_text('{"unchanged":true}\n', encoding="utf-8")

            with self.assertRaisesRegex(CONVERTER.ConversionError, "partition"):
                CONVERTER.regenerate(
                    annotations_dir=annotations,
                    dogfood_dir=dogfood,
                    require_review_artifacts=False,
                    emit=lambda _: None,
                )

            self.assertEqual(sentinel.read_text(encoding="utf-8"), '{"unchanged":true}\n')
            self.assertEqual([path.name for path in dogfood.iterdir()], ["sentinel.json"])

    def test_duplicate_audio_aborts_before_tier_filtering_or_output_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            dogfood = root / "dogfood"
            annotations.mkdir()
            dogfood.mkdir()
            gold = annotation(episode_id="gold")
            proposal = annotation(
                episode_id="proposal",
                audio_fingerprint=gold["audio_fingerprint"],
                auto_promoted=True,
                auto_promoted_by="test-auto-promoter",
            )
            (annotations / "gold.json").write_text(json.dumps(gold), encoding="utf-8")
            (annotations / "proposal.json").write_text(
                json.dumps(proposal), encoding="utf-8"
            )
            (annotations / CONVERTER.MANIFEST_FILENAME).write_text(
                json.dumps({
                    "schema_version": 1,
                    "annotations": ["gold.json", "proposal.json"],
                }),
                encoding="utf-8",
            )
            sentinel = dogfood / "sentinel.json"
            sentinel.write_text('{"unchanged":true}\n', encoding="utf-8")

            with self.assertRaisesRegex(CONVERTER.ConversionError, "unique audio"):
                CONVERTER.regenerate(
                    annotations_dir=annotations,
                    dogfood_dir=dogfood,
                    require_review_artifacts=False,
                    emit=lambda _: None,
                )

            self.assertEqual(sentinel.read_text(encoding="utf-8"), '{"unchanged":true}\n')
            self.assertEqual([path.name for path in dogfood.iterdir()], ["sentinel.json"])

    def test_dangling_variant_aborts_before_output_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            dogfood = root / "dogfood"
            annotations.mkdir()
            dogfood.mkdir()
            variant = annotation(episode_id="variant", variant_of="missing-parent")
            (annotations / "variant.json").write_text(
                json.dumps(variant),
                encoding="utf-8",
            )
            (annotations / CONVERTER.MANIFEST_FILENAME).write_text(
                json.dumps(
                    {"schema_version": 1, "annotations": ["variant.json"]}
                ),
                encoding="utf-8",
            )
            sentinel = dogfood / "sentinel.json"
            sentinel.write_text('{"unchanged":true}\n', encoding="utf-8")

            with self.assertRaisesRegex(CONVERTER.ConversionError, "non-canonical variant_of"):
                CONVERTER.regenerate(
                    annotations_dir=annotations,
                    dogfood_dir=dogfood,
                    require_review_artifacts=False,
                    emit=lambda _: None,
                )

            self.assertEqual(sentinel.read_text(encoding="utf-8"), '{"unchanged":true}\n')
            self.assertEqual([path.name for path in dogfood.iterdir()], ["sentinel.json"])

    def test_atomic_golden_write_preserves_existing_file_on_serialization_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "golden.json"
            path.write_text('{"old":true}\n', encoding="utf-8")

            with mock.patch.object(
                CONVERTER.json,
                "dump",
                side_effect=RuntimeError("injected serialization failure"),
            ):
                with self.assertRaisesRegex(RuntimeError, "injected serialization failure"):
                    CONVERTER.atomic_write_json(path, {"new": True})

            self.assertEqual(path.read_text(encoding="utf-8"), '{"old":true}\n')
            self.assertEqual([item.name for item in path.parent.iterdir()], ["golden.json"])

    def test_process_interruption_cannot_expose_a_mixed_golden_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            generations = root / ".dogfood-generations"
            old = generations / "old"
            old.mkdir(parents=True)
            (old / "old.json").write_text('{"epoch":"old"}\n', encoding="utf-8")
            pointer = root / "dogfood"
            pointer.symlink_to(pathlib.Path(".dogfood-generations/old"))
            planned = {
                "first.json": {"epoch": "new", "value": 1},
                "second.json": {"epoch": "new", "value": 2},
            }
            real_replace = CONVERTER.os.replace

            def interrupt_pointer_swap(source: object, destination: object) -> None:
                if pathlib.Path(destination) == pointer:
                    raise KeyboardInterrupt("process terminated before pointer swap")
                real_replace(source, destination)

            with mock.patch.object(
                CONVERTER.os, "replace", side_effect=interrupt_pointer_swap
            ):
                with self.assertRaisesRegex(KeyboardInterrupt, "before pointer swap"):
                    CONVERTER.publish_versioned_generation(planned, pointer)

            self.assertTrue(pointer.is_symlink())
            self.assertEqual(pointer.resolve(), old.resolve())
            self.assertEqual(
                {path.name: path.read_text() for path in pointer.glob("*.json")},
                {"old.json": '{"epoch":"old"}\n'},
            )

    def test_generation_directory_is_durable_before_pointer_swap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            generations = root / ".dogfood-generations"
            old = generations / "old"
            old.mkdir(parents=True)
            (old / "old.json").write_text('{"epoch":"old"}\n', encoding="utf-8")
            pointer = root / "dogfood"
            pointer.symlink_to(pathlib.Path(".dogfood-generations/old"))
            planned = {"first.json": {"epoch": "new"}}
            events: list[tuple[str, pathlib.Path]] = []
            real_replace = CONVERTER.os.replace
            real_fsync_directory = CONVERTER._fsync_directory

            def record_replace(source: object, destination: object) -> None:
                destination_path = pathlib.Path(destination)
                if destination_path == pointer:
                    events.append(("pointer_swap", destination_path))
                real_replace(source, destination)

            def record_fsync(path: pathlib.Path) -> None:
                events.append(("fsync", pathlib.Path(path)))
                real_fsync_directory(path)

            with mock.patch.object(
                CONVERTER.os, "replace", side_effect=record_replace
            ), mock.patch.object(
                CONVERTER, "_fsync_directory", side_effect=record_fsync
            ):
                CONVERTER.publish_versioned_generation(planned, pointer)

            generation_sync = events.index(("fsync", generations))
            pointer_swap = events.index(("pointer_swap", pointer))
            self.assertLess(generation_sync, pointer_swap)
            material = json.dumps(
                planned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            self.assertEqual(
                CONVERTER.os.readlink(pointer),
                f".dogfood-generations/{hashlib.sha256(material).hexdigest()}",
            )

    def test_existing_hash_named_generation_must_be_complete_and_exact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            generations = root / ".dogfood-generations"
            old = generations / "old"
            old.mkdir(parents=True)
            (old / "old.json").write_text('{"epoch":"old"}\n', encoding="utf-8")
            pointer = root / "dogfood"
            pointer.symlink_to(pathlib.Path(".dogfood-generations/old"))
            planned = {"first.json": {"epoch": "new", "value": 1}}
            material = json.dumps(
                planned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            corrupt = generations / hashlib.sha256(material).hexdigest()
            corrupt.mkdir()
            (corrupt / "first.json").write_text(
                '{"epoch":"tampered"}\n', encoding="utf-8"
            )

            with self.assertRaisesRegex(
                CONVERTER.ConversionError,
                "immutable golden generation is incomplete or corrupt",
            ):
                CONVERTER.publish_versioned_generation(planned, pointer)

            self.assertEqual(pointer.resolve(), old.resolve())
            self.assertEqual((pointer / "old.json").read_text(), '{"epoch":"old"}\n')

    def test_existing_hash_named_generation_must_not_be_a_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            generations = root / ".dogfood-generations"
            old = generations / "old"
            old.mkdir(parents=True)
            (old / "old.json").write_text('{"epoch":"old"}\n', encoding="utf-8")
            pointer = root / "dogfood"
            pointer.symlink_to(pathlib.Path(".dogfood-generations/old"))
            planned = {"first.json": {"epoch": "new", "value": 1}}
            material = json.dumps(
                planned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            generation = generations / hashlib.sha256(material).hexdigest()
            external = root / "external-generation"
            external.mkdir()
            (external / "first.json").write_text(
                json.dumps(planned["first.json"]), encoding="utf-8"
            )
            generation.symlink_to(external, target_is_directory=True)

            with self.assertRaisesRegex(
                CONVERTER.ConversionError,
                "immutable golden generation is incomplete or corrupt",
            ):
                CONVERTER.publish_versioned_generation(planned, pointer)

            self.assertTrue(generation.is_symlink())
            self.assertEqual(pointer.resolve(), old.resolve())
            self.assertEqual((pointer / "old.json").read_text(), '{"epoch":"old"}\n')

    def test_versioned_publication_rejects_symlinked_generation_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            external = root / "external"
            old = external / "old"
            old.mkdir(parents=True)
            sentinel = old / "sentinel.json"
            sentinel.write_text('{"unchanged":true}\n', encoding="utf-8")
            generations = root / ".dogfood-generations"
            generations.symlink_to(external, target_is_directory=True)
            pointer = root / "dogfood"
            pointer.symlink_to(pathlib.Path(".dogfood-generations/old"))

            with self.assertRaisesRegex(CONVERTER.ConversionError, "generation root"):
                CONVERTER.publish_versioned_generation(
                    {"new.json": {"epoch": "new"}}, pointer
                )

            self.assertEqual([path.name for path in external.iterdir()], ["old"])
            self.assertEqual(sentinel.read_text(encoding="utf-8"), '{"unchanged":true}\n')
            self.assertEqual(pointer.resolve(), old.resolve())

    def test_versioned_pointer_rejects_intermediate_alias_dotdot_escape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            generations = root / ".dogfood-generations"
            generations.mkdir()
            # The lexical target exists inside the generation root, so the old
            # abspath/parent check accepted it. POSIX resolves `alias/..` after
            # following alias, however, and the live pointer reaches root/old.
            (generations / "old").mkdir()
            external = root / "external"
            external.mkdir()
            external_parent_generation = root / "old"
            external_parent_generation.mkdir()
            sentinel = external_parent_generation / "sentinel.json"
            sentinel.write_text('{"unchanged":true}\n', encoding="utf-8")
            (generations / "alias").symlink_to(external, target_is_directory=True)
            pointer = root / "dogfood"
            pointer.symlink_to(
                pathlib.Path(".dogfood-generations/alias/../old")
            )
            self.assertEqual(pointer.resolve(), external_parent_generation.resolve())

            with self.assertRaisesRegex(CONVERTER.ConversionError, "regular generation"):
                CONVERTER.publish_versioned_generation(
                    {"new.json": {"epoch": "new"}}, pointer
                )

            self.assertEqual(sentinel.read_text(encoding="utf-8"), '{"unchanged":true}\n')
            self.assertEqual([path.name for path in external_parent_generation.iterdir()], ["sentinel.json"])

    def test_golden_dry_run_rejects_symlinked_existing_member(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            document = annotation(episode_id="episode", provenance=["human_first_pass"])
            (annotations / "episode.json").write_text(json.dumps(document), encoding="utf-8")
            (annotations / CONVERTER.MANIFEST_FILENAME).write_text(
                json.dumps({"schema_version": 1, "annotations": ["episode.json"]}),
                encoding="utf-8",
            )
            external = root / "external.json"
            external.write_text('{"unchanged":true}\n', encoding="utf-8")
            dogfood = root / "dogfood"
            dogfood.mkdir()
            (dogfood / "stale.json").symlink_to(external)

            with self.assertRaisesRegex(CONVERTER.ConversionError, "regular file"):
                CONVERTER.regenerate(
                    annotations_dir=annotations,
                    dogfood_dir=dogfood,
                    require_review_artifacts=False,
                    dry_run=True,
                    emit=lambda _: None,
                )

            self.assertEqual(external.read_text(encoding="utf-8"), '{"unchanged":true}\n')

    def test_nonversioned_publication_rejects_symlinked_output_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            annotations.mkdir()
            document = annotation(episode_id="episode")
            (annotations / "episode.json").write_text(
                json.dumps(document), encoding="utf-8"
            )
            (annotations / CONVERTER.MANIFEST_FILENAME).write_text(
                json.dumps({"schema_version": 1, "annotations": ["episode.json"]}),
                encoding="utf-8",
            )
            external = root / "external"
            external.mkdir()
            sentinel = external / "sentinel"
            sentinel.write_text("unchanged", encoding="utf-8")
            alias = root / "alias"
            alias.symlink_to(external, target_is_directory=True)

            with self.assertRaisesRegex(CONVERTER.ConversionError, "symbolic link"):
                CONVERTER.regenerate(
                    annotations_dir=annotations,
                    dogfood_dir=alias / "dogfood",
                    require_review_artifacts=False,
                    emit=lambda _: None,
                )

            self.assertEqual([path.name for path in external.iterdir()], ["sentinel"])
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "unchanged")

    def test_regeneration_rolls_back_all_outputs_when_one_publish_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            annotations = root / "annotations"
            dogfood = root / "dogfood"
            annotations.mkdir()
            dogfood.mkdir()
            for episode_id in ("first", "second"):
                (annotations / f"{episode_id}.json").write_text(
                    json.dumps(annotation(episode_id=episode_id)), encoding="utf-8"
                )
            (annotations / CONVERTER.MANIFEST_FILENAME).write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "annotations": ["first.json", "second.json"],
                    }
                ),
                encoding="utf-8",
            )
            originals = {
                "first.json": b'{"old":"first"}\n',
                "second.json": b'{"old":"second"}\n',
                "stale.json": b'{"old":"stale"}\n',
            }
            for filename, data in originals.items():
                (dogfood / filename).write_bytes(data)

            real_atomic_write = CONVERTER.atomic_write_json
            for failure in (
                OSError("injected set failure"),
                KeyboardInterrupt("injected set interruption"),
            ):
                with self.subTest(failure=type(failure).__name__):
                    calls = 0

                    def fail_second(path: pathlib.Path, document: dict) -> None:
                        nonlocal calls
                        calls += 1
                        if calls == 2:
                            raise failure
                        real_atomic_write(path, document)

                    with mock.patch.object(
                        CONVERTER, "atomic_write_json", side_effect=fail_second
                    ):
                        with self.assertRaisesRegex(
                            CONVERTER.ConversionError,
                            str(failure),
                        ):
                            CONVERTER.regenerate(
                                annotations_dir=annotations,
                                dogfood_dir=dogfood,
                                require_review_artifacts=False,
                                emit=lambda _: None,
                            )

                    self.assertEqual(
                        {path.name: path.read_bytes() for path in dogfood.iterdir()},
                        originals,
                    )


if __name__ == "__main__":
    unittest.main()
