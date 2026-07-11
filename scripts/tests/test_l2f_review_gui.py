#!/usr/bin/env python3
"""Exact-asset safety tests for the ordinary L2F review GUI."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "l2f-review-gui.py"
SPEC = importlib.util.spec_from_file_location("l2f_review_gui", SCRIPT)
assert SPEC and SPEC.loader
GUI = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = GUI
SPEC.loader.exec_module(GUI)


def fingerprint(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


class ReviewGUIBindingTests(unittest.TestCase):
    def test_playback_and_save_binding_hash_the_actual_opened_audio(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            audio = pathlib.Path(tmp) / "episode.mp3"
            expected_bytes = b"reviewed audio bytes"
            audio.write_bytes(expected_bytes)
            entry = {
                "id": "episode#1",
                "episode_id": "episode",
                "audio_path": str(audio),
                "audio_fingerprint": fingerprint(expected_bytes),
            }

            handle, resolved, size = GUI.open_bound_audio(entry)
            with handle:
                self.assertEqual(handle.read(), expected_bytes)
            self.assertEqual(resolved, audio)
            self.assertEqual(size, len(expected_bytes))

            audio.write_bytes(b"different same-name asset")
            with self.assertRaisesRegex(ValueError, "audio fingerprint mismatch"):
                GUI.open_bound_audio(entry)

    def test_range_reader_uses_the_verified_descriptor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            audio = pathlib.Path(tmp) / "episode.mp3"
            data = b"0123456789"
            audio.write_bytes(data)
            entry = {
                "episode_id": "episode",
                "audio_path": str(audio),
                "audio_fingerprint": fingerprint(data),
            }
            handle, _, size = GUI.open_bound_audio(entry)
            with handle:
                start, end, partial = GUI.parse_range("bytes=3-6", size)
                handle.seek(start)
                received = GUI._LimitedReader(handle, end - start + 1).read()
            self.assertTrue(partial)
            self.assertEqual((start, end), (3, 6))
            self.assertEqual(received, b"3456")

    def test_transcript_must_match_the_queue_audio_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            transcripts = pathlib.Path(tmp)
            expected = fingerprint(b"episode audio")
            path = transcripts / "episode.json"
            transcript = {
                "source_audio_fingerprint": expected,
                "transcription": [
                    {"text": "sponsor message", "offsets": {"from": 0, "to": 1000}}
                ],
            }
            path.write_text(json.dumps(transcript), encoding="utf-8")
            entry = {
                "episode_id": "episode",
                "audio_fingerprint": expected,
                "start_seconds": 0,
                "end_seconds": 1,
            }
            with mock.patch.object(GUI, "TRANSCRIPT_DIR", transcripts):
                self.assertEqual(
                    GUI.transcript_payload_for(entry, full=True)["segments"][0]["text"],
                    "sponsor message",
                )
                transcript["source_audio_fingerprint"] = fingerprint(b"other audio")
                path.write_text(json.dumps(transcript), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "does not match"):
                    GUI.transcript_payload_for(entry, full=True)

    def test_transcript_lookup_rejects_episode_id_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            transcripts = root / "transcripts"
            transcripts.mkdir()
            expected = fingerprint(b"episode audio")
            (root / "outside.json").write_text(
                json.dumps(
                    {
                        "source_audio_fingerprint": expected,
                        "transcription": [
                            {
                                "text": "outside transcript",
                                "offsets": {"from": 0, "to": 1000},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            entry = {
                "episode_id": "../outside",
                "audio_fingerprint": expected,
            }

            with mock.patch.object(GUI, "TRANSCRIPT_DIR", transcripts):
                with self.assertRaisesRegex(ValueError, "unsafe transcript episode id"):
                    GUI.transcript_payload_for(entry, full=True)

    def test_transcript_regular_file_swap_to_fifo_fails_without_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            transcripts = pathlib.Path(tmp)
            expected = fingerprint(b"episode audio")
            path = transcripts / "episode.json"
            path.write_text(
                json.dumps(
                    {
                        "source_audio_fingerprint": expected,
                        "transcription": [
                            {
                                "text": "sponsor message",
                                "offsets": {"from": 0, "to": 1000},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            entry = {
                "episode_id": "episode",
                "audio_fingerprint": expected,
            }
            real_open = GUI.os.open
            opened_flags: list[int] = []

            def swap_then_open(candidate: object, flags: int) -> int:
                if pathlib.Path(candidate) == path:
                    path.unlink()
                    GUI.os.mkfifo(path)
                    opened_flags.append(flags)
                return real_open(candidate, flags)

            with mock.patch.object(GUI, "TRANSCRIPT_DIR", transcripts), mock.patch.object(
                GUI.os, "open", side_effect=swap_then_open
            ):
                with self.assertRaisesRegex(ValueError, "regular file"):
                    GUI.transcript_payload_for(entry, full=True)

            self.assertEqual(len(opened_flags), 1)
            self.assertTrue(opened_flags[0] & GUI.os.O_NONBLOCK)
            self.assertTrue(opened_flags[0] & GUI.os.O_NOFOLLOW)

    def test_malformed_transcript_segments_fail_closed(self) -> None:
        payload = {
            "source_audio_fingerprint": fingerprint(b"audio"),
            "transcription": [
                {"text": "valid", "offsets": {"from": 0, "to": 1000}},
                {"text": "missing time"},
            ],
        }
        with self.assertRaisesRegex(ValueError, "segment 1"):
            GUI.parse_transcript_segments(payload)
        payload["segments"] = []
        with self.assertRaisesRegex(ValueError, "exactly one"):
            GUI.parse_transcript_segments(payload)

    def test_missed_ad_requires_finite_ordered_bounds(self) -> None:
        for review in (
            {},
            {"start_seconds": None, "end_seconds": 2},
            {"start_seconds": 2, "end_seconds": 2},
            {"start_seconds": -1, "end_seconds": 2},
            {"start_seconds": float("nan"), "end_seconds": 2},
        ):
            with self.subTest(review=review), self.assertRaises(ValueError):
                GUI.validated_ad_bounds(review)
        self.assertEqual(
            GUI.validated_ad_bounds({"start_seconds": "1.5", "end_seconds": 2}),
            (1.5, 2.0),
        )
        with self.assertRaisesRegex(ValueError, "episode duration"):
            GUI.validated_ad_bounds(
                {"start_seconds": 1, "end_seconds": 11},
                {"duration_seconds": 10},
            )

    def test_reviewer_identity_is_required_and_trimmed(self) -> None:
        for review in ({}, {"reviewer": None}, {"reviewer": "   "}):
            with self.subTest(review=review), self.assertRaisesRegex(ValueError, "reviewer"):
                GUI.validated_reviewer(review)
        self.assertEqual(GUI.validated_reviewer({"reviewer": "  Listener Two  "}), "Listener Two")

    def test_duplicate_queue_ids_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            queue = root / "queue.json"
            review = root / "review.json"
            queue.write_text(
                json.dumps(
                    {
                        "entries": [
                            {"id": "duplicate", "episode_id": "one"},
                            {"id": "duplicate", "episode_id": "two"},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "duplicate ids"):
                GUI.load_all_entries(queue, review)

    def test_existing_malformed_review_state_is_never_treated_as_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "review.json"
            invalid = (
                [],
                {},
                {"schema": "wrong", "reviews": {}},
                {"schema": "playhead-l2f-audio-review-v1", "reviews": []},
                {
                    "schema": "playhead-l2f-audio-review-v1",
                    "reviews": {},
                    "manual_entries": [None],
                },
                {
                    "schema": "playhead-l2f-audio-review-v1",
                    "reviews": {"entry": None},
                },
            )
            for payload in invalid:
                with self.subTest(payload=payload):
                    path.write_text(json.dumps(payload), encoding="utf-8")
                    with self.assertRaises(ValueError):
                        GUI.load_reviews(path)

    def test_existing_review_must_remain_bound_to_the_queue_asset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "review.json"
            expected = fingerprint(b"expected")
            path.write_text(
                json.dumps(
                    {
                        "schema": "playhead-l2f-audio-review-v1",
                        "reviews": {
                            "episode#1": {
                                "status": "false_positive",
                                "reviewer": "Listener",
                                "audio_fingerprint": fingerprint(b"other"),
                                "reviewed_at": "2026-07-10T12:00:00Z",
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            entry = {
                "id": "episode#1",
                "episode_id": "episode",
                "audio_fingerprint": expected,
            }
            with self.assertRaisesRegex(ValueError, "different audio"):
                GUI.load_bound_reviews(path, [entry])

    def test_review_status_is_whitelisted(self) -> None:
        self.assertEqual(GUI.clean_review_status("verified_ad"), "verified_ad")
        with self.assertRaisesRegex(ValueError, "unsupported review status"):
            GUI.clean_review_status("looks_good")

    def test_concurrent_review_transactions_do_not_lose_decisions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            review_path = root / "review.json"
            queue_path = root / "queue.json"
            bound = fingerprint(b"audio")

            def save(index: int) -> None:
                with GUI.review_transaction(review_path):
                    reviews = GUI.load_reviews(review_path)
                    reviews[f"episode#{index}"] = {
                        "status": "false_positive",
                        "reviewer": f"Listener {index}",
                        "audio_fingerprint": bound,
                        "reviewed_at": "2026-07-10T12:00:00Z",
                    }
                    GUI.save_reviews(review_path, queue_path, reviews)

            with ThreadPoolExecutor(max_workers=8) as executor:
                list(executor.map(save, range(24)))
            self.assertEqual(len(GUI.load_reviews(review_path)), 24)

    def test_failed_manual_persistence_leaves_existing_evidence_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            review_path = root / "review.json"
            queue_path = root / "queue.json"
            GUI.save_reviews(review_path, queue_path, {})
            original = review_path.read_bytes()
            with mock.patch.object(
                GUI,
                "write_json_atomic",
                side_effect=OSError("injected write failure"),
            ):
                with self.assertRaisesRegex(OSError, "injected"):
                    GUI.save_manual_entries(
                        review_path,
                        queue_path,
                        [{"id": "manual:episode#1", "manual_entry": True}],
                        {},
                    )
            self.assertEqual(review_path.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
