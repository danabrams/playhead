#!/usr/bin/env python3
"""Deterministic tests for the local L2F ear-audit server."""

from __future__ import annotations

import importlib.util
import hashlib
import json
import pathlib
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "l2f-earaudit-gui.py"
SPEC = importlib.util.spec_from_file_location("l2f_earaudit_gui", SCRIPT)
assert SPEC and SPEC.loader
GUI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GUI)
TEST_FINGERPRINT = "sha256:" + hashlib.sha256(bytes(range(100))).hexdigest()


def ledger_row(stable_id: str, episode: str, start: float, end: float) -> dict:
    return {
        "id": stable_id,
        "episode_id": episode,
        "show_name": "<Example & Show>",
        "current_start_seconds": start,
        "current_end_seconds": end,
        "duration_seconds": 100.0,
        "raw_verdict": "boundary",
        "disposition": "boundary_off",
        "provenance_tier": "boundary_proposal",
        "recommendation": "Rebound the full slot",
        "audio_fingerprint": TEST_FINGERPRINT,
    }


class ReviewStoreTests(unittest.TestCase):
    def test_save_is_atomic_idempotent_and_undo_uses_action_chronology(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "reviews.json"
            store = GUI.ReviewStore(path)
            first = {
                "id": "later-in-sort",
                "status": "approved",
                "proposed_start_seconds": 10.0,
                "proposed_end_seconds": 20.0,
                "note": "first",
                "reviewer": "Dan",
                "audio_fingerprint": TEST_FINGERPRINT,
            }
            second = {
                "id": "earlier-in-sort",
                "status": "unsure",
                "proposed_start_seconds": 30.0,
                "proposed_end_seconds": 40.0,
                "note": "second",
                "reviewer": "Dan",
                "audio_fingerprint": TEST_FINGERPRINT,
            }

            store.save(first, reviewed_at="2026-07-10T12:00:00Z")
            store.save(second, reviewed_at="2026-07-10T12:01:00Z")
            before = json.loads(path.read_text())
            self.assertEqual(len(before["history"]), 2)

            # Reposting the same semantic review is a no-op, including history.
            store.save(second, reviewed_at="2026-07-10T12:02:00Z")
            self.assertEqual(len(json.loads(path.read_text())["history"]), 2)

            undone = store.undo()
            self.assertEqual(undone["id"], "earlier-in-sort")
            after = json.loads(path.read_text())
            self.assertNotIn("earlier-in-sort", after["reviews"])
            self.assertIn("later-in-sort", after["reviews"])
            self.assertEqual(len(after["history"]), 1)

    def test_concurrent_saves_leave_valid_json_without_lost_reviews(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "reviews.json"
            stores = [GUI.ReviewStore(path), GUI.ReviewStore(path)]

            def save(index: int) -> None:
                stores[index % len(stores)].save(
                    {
                        "id": f"item-{index}",
                        "status": "approved",
                        "proposed_start_seconds": float(index),
                        "proposed_end_seconds": float(index + 1),
                        "note": "",
                        "reviewer": "test",
                        "audio_fingerprint": TEST_FINGERPRINT,
                    },
                    reviewed_at=f"2026-07-10T12:00:{index:02d}Z",
                )

            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(save, range(20)))

            document = json.loads(path.read_text())
            self.assertEqual(len(document["reviews"]), 20)
            self.assertEqual(len(document["history"]), 20)

    def test_malformed_persisted_state_is_rejected_before_resume(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "reviews.json"
            path.write_text(json.dumps({
                "schema_version": 1,
                "reviews": {"item": {"id": "other", "status": "approved"}},
                "history": [],
                "next_sequence": 1,
            }))

            with self.assertRaises(GUI.ValidationError):
                GUI.ReviewStore(path).snapshot()

    def test_persisted_history_must_replay_to_the_current_review_map(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "reviews.json"
            review = {
                "id": "item",
                "status": "approved",
                "proposed_start_seconds": 10.0,
                "proposed_end_seconds": 20.0,
                "note": "",
                "reviewer": "Dan",
                "audio_fingerprint": TEST_FINGERPRINT,
                "reviewed_at": "2026-07-10T12:00:00Z",
            }
            path.write_text(json.dumps({
                "schema_version": 2,
                "reviews": {"item": review},
                "history": [{
                    "sequence": 1,
                    "id": "item",
                    "before": review,
                    "after": review,
                    "at": review["reviewed_at"],
                }],
                "next_sequence": 2,
            }))

            with self.assertRaises(GUI.ValidationError):
                GUI.ReviewStore(path).snapshot()

    def test_legacy_review_state_without_next_sequence_resumes_and_saves(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "reviews.json"
            store = GUI.ReviewStore(path)
            first = {
                "id": "first",
                "status": "approved",
                "proposed_start_seconds": 10.0,
                "proposed_end_seconds": 20.0,
                "note": "",
                "reviewer": "Dan",
                "audio_fingerprint": TEST_FINGERPRINT,
            }
            store.save(first, reviewed_at="2026-07-10T12:00:00Z")
            legacy = json.loads(path.read_text())
            legacy.pop("next_sequence")
            path.write_text(json.dumps(legacy))

            self.assertEqual(store.snapshot()["next_sequence"], 2)
            store.save(
                {
                    **first,
                    "id": "second",
                    "proposed_start_seconds": 30.0,
                    "proposed_end_seconds": 40.0,
                },
                reviewed_at="2026-07-10T12:01:00Z",
            )
            resumed = json.loads(path.read_text())
            self.assertEqual(resumed["next_sequence"], 3)
            self.assertEqual(set(resumed["reviews"]), {"first", "second"})


class AuditLedgerTests(unittest.TestCase):
    def test_tracked_ledger_matches_review_queues_and_canonical_corpus(self) -> None:
        corpus = SCRIPT.parents[1] / "TestFixtures/Corpus"
        manifest = json.loads(
            (corpus / "Annotations/_canonical-manifest.json").read_text()
        )["annotations"]
        ad_windows: set[tuple[str, float, float]] = set()
        for filename in manifest:
            annotation = json.loads((corpus / "Annotations" / filename).read_text())
            ad_windows.update(
                (
                    annotation["episode_id"],
                    float(window["start_seconds"]),
                    float(window["end_seconds"]),
                )
                for window in annotation["ad_windows"]
            )

        with tempfile.TemporaryDirectory() as tmp:
            entries = GUI.load_audit_entries(
                GUI.DEFAULT_AUDIT_LEDGER,
                pathlib.Path(tmp),
            )

        dispositions = {
            value: sum(entry["disposition"] == value for entry in entries)
            for value in ("tight_ad", "boundary_off", "edge_clipping", "rejected")
        }
        self.assertEqual(len(entries), 70)
        self.assertEqual(dispositions, {
            "tight_ad": 25,
            "boundary_off": 14,
            "edge_clipping": 22,
            "rejected": 9,
        })
        self.assertEqual(sum(entry["boundary_work"] for entry in entries), 36)
        for entry in entries:
            identity = (
                entry["episode_id"],
                entry["current_start_seconds"],
                entry["current_end_seconds"],
            )
            self.assertEqual(identity in ad_windows, entry["retained_in_corpus"])

    def test_unsafe_episode_id_and_missing_display_metadata_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            audio = root / "audio"
            audio.mkdir()
            for mutate in (
                lambda row: row.update(episode_id="../outside"),
                lambda row: row.pop("raw_verdict"),
                lambda row: row.pop("provenance_tier"),
            ):
                row = ledger_row("safe-id", "episode", 10, 20)
                mutate(row)
                ledger = root / "audit.jsonl"
                ledger.write_text(json.dumps(row) + "\n")
                with self.assertRaises(GUI.ValidationError):
                    GUI.load_audit_entries(ledger, audio)

    def test_audio_resolution_rejects_episode_alias_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            audio = pathlib.Path(tmp)
            target = audio / "episode-two.mp3"
            target.write_bytes(b"episode two")
            (audio / "episode-one.mp3").symlink_to(target)

            self.assertIsNone(GUI.resolve_audio("episode-one", audio))
            self.assertEqual(GUI.resolve_audio("episode-two", audio), target.resolve())

    def test_ledger_rejects_audio_with_different_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            audio = root / "audio"
            audio.mkdir()
            (audio / "episode.mp3").write_bytes(b"different asset")
            ledger = root / "audit.jsonl"
            ledger.write_text(json.dumps(ledger_row("item", "episode", 10, 20)) + "\n")
            with self.assertRaisesRegex(GUI.ValidationError, "fingerprint mismatch"):
                GUI.load_audit_entries(ledger, audio)


class AuditServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.temp.name)
        audio = root / "episode-one.mp3"
        audio.write_bytes(bytes(range(100)))
        _, audio_identity = GUI.audio_fingerprint_and_identity(audio)
        entries = [
            {
                **ledger_row("episode-one@10.00-20.00", "episode-one", 10, 20),
                "audio": str(audio),
                "_audio_root": str(root.resolve()),
                "_audio_identity": audio_identity,
                "audio_available": True,
            },
            ledger_row("episode-two@30.00-40.00", "episode-two", 30, 40),
        ]
        self.app = GUI.AuditApp(entries, GUI.ReviewStore(root / "reviews.json"))
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), GUI.make_handler(self.app))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temp.cleanup()

    def request(self, path: str, *, method: str = "GET", body: object | None = None,
                headers: dict[str, str] | None = None) -> tuple[int, bytes, object]:
        data = None if body is None else json.dumps(body).encode()
        request_headers = dict(headers or {})
        if data is not None:
            request_headers.setdefault("Content-Type", "application/json")
        req = urllib.request.Request(
            self.base + path,
            data=data,
            method=method,
            headers=request_headers,
        )
        with urllib.request.urlopen(req) as response:
            return response.status, response.read(), response.headers

    def test_review_api_validates_and_resumes_saved_state(self) -> None:
        payload = {
            "id": "episode-one@10.00-20.00",
            "status": "rebounded",
            "proposed_start_seconds": 8.1,
            "proposed_end_seconds": 22.4,
            "note": "Full break",
            "reviewer": "Dan",
            "audio_fingerprint": TEST_FINGERPRINT,
        }
        status, _, _ = self.request("/api/reviews", method="POST", body=payload)
        self.assertEqual(status, 200)
        _, raw, _ = self.request("/api/items")
        items = json.loads(raw)
        review = next(item["review"] for item in items["items"] if item["id"] == payload["id"])
        self.assertEqual(review["status"], "rebounded")
        self.assertEqual(review["proposed_start_seconds"], 8.1)

        for invalid in (
            {**payload, "id": "not-in-ledger"},
            {**payload, "status": "maybe"},
            {**payload, "proposed_start_seconds": 50, "proposed_end_seconds": 40},
            {**payload, "proposed_start_seconds": -1},
            {**payload, "proposed_start_seconds": 3.0004, "proposed_end_seconds": 3.00049},
            {**payload, "reviewer": ""},
            {**payload, "audio_fingerprint": "sha256:" + "f" * 64},
            {
                **payload,
                "status": "approved",
                "proposed_start_seconds": 8.1,
                "proposed_end_seconds": 22.4,
            },
        ):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                self.request("/api/reviews", method="POST", body=invalid)
            self.assertEqual(caught.exception.code, 422)

    def test_semantically_invalid_resumed_reviews_return_server_error(self) -> None:
        review_path = self.app.store.path
        invalid_reviews = (
            {
                "id": "not-in-ledger",
                "status": "approved",
                "proposed_start_seconds": 10.0,
                "proposed_end_seconds": 20.0,
            },
            {
                "id": "episode-one@10.00-20.00",
                "status": "approved",
                "proposed_start_seconds": 11.0,
                "proposed_end_seconds": 20.0,
            },
            {
                "id": "episode-one@10.00-20.00",
                "status": "rebounded",
                "proposed_start_seconds": 10.0,
                "proposed_end_seconds": 101.0,
            },
        )
        for index, values in enumerate(invalid_reviews):
            with self.subTest(values=values):
                review = {
                    **values,
                    "note": "",
                    "reviewer": "Dan",
                    "audio_fingerprint": TEST_FINGERPRINT,
                    "reviewed_at": f"2026-07-10T12:00:0{index}Z",
                }
                review_path.write_text(json.dumps({
                    "schema_version": 2,
                    "reviews": {review["id"]: review},
                    "history": [{
                        "sequence": 1,
                        "id": review["id"],
                        "before": None,
                        "after": review,
                        "at": review["reviewed_at"],
                    }],
                    "next_sequence": 2,
                }))
                with self.assertRaises(urllib.error.HTTPError) as caught:
                    self.request("/api/items")
                self.assertEqual(caught.exception.code, 500)

    def test_audio_supports_normal_open_suffix_and_invalid_ranges(self) -> None:
        audio_path = "/audio/" + urllib.parse.quote("episode-one@10.00-20.00", safe="")
        status, data, headers = self.request(audio_path)
        self.assertEqual((status, len(data), headers.get_content_type()), (200, 100, "audio/mpeg"))

        status, data, headers = self.request(audio_path, headers={"Range": "bytes=10-19"})
        self.assertEqual((status, data), (206, bytes(range(10, 20))))
        self.assertEqual(headers["Content-Range"], "bytes 10-19/100")

        status, data, headers = self.request(audio_path, headers={"Range": "bytes=95-"})
        self.assertEqual((status, data), (206, bytes(range(95, 100))))
        self.assertEqual(headers["Content-Range"], "bytes 95-99/100")

        status, data, headers = self.request(audio_path, headers={"Range": "bytes=-5"})
        self.assertEqual((status, data), (206, bytes(range(95, 100))))
        self.assertEqual(headers["Content-Range"], "bytes 95-99/100")

        for value in (
            "bytes=100-",
            "bytes=30-20",
            "items=0-1",
            "bytes=0-1,4-5",
            "bytes=-0",
            "bytes=" + "9" * 5_000 + "-",
        ):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                self.request(audio_path, headers={"Range": value})
            self.assertEqual(caught.exception.code, 416)
            self.assertEqual(caught.exception.headers["Content-Range"], "bytes */100")

    def test_path_traversal_and_oversized_bodies_are_rejected(self) -> None:
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.request("/audio/..%2Fsecret.mp3")
        self.assertIn(caught.exception.code, (400, 404))

        req = urllib.request.Request(
            self.base + "/api/reviews",
            data=b"{}",
            method="POST",
            headers={"Content-Type": "application/json", "Content-Length": "999999"},
        )
        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(req)
        self.assertEqual(caught.exception.code, 413)

    def test_audio_path_is_revalidated_after_startup(self) -> None:
        audio_path = pathlib.Path(self.temp.name) / "episode-one.mp3"
        outside = pathlib.Path(self.temp.name).parent / "outside-audit-audio.mp3"
        outside.write_bytes(b"private")
        try:
            audio_path.unlink()
            audio_path.symlink_to(outside)
            encoded = "/audio/" + urllib.parse.quote("episode-one@10.00-20.00", safe="")
            with self.assertRaises(urllib.error.HTTPError) as caught:
                self.request(encoded)
            self.assertEqual(caught.exception.code, 404)
        finally:
            outside.unlink(missing_ok=True)

    def test_audio_path_rejects_cross_episode_symlink_added_after_startup(self) -> None:
        audio_path = pathlib.Path(self.temp.name) / "episode-one.mp3"
        other_episode = pathlib.Path(self.temp.name) / "episode-two.mp3"
        other_episode.write_bytes(b"wrong episode")
        audio_path.unlink()
        audio_path.symlink_to(other_episode)

        encoded = "/audio/" + urllib.parse.quote("episode-one@10.00-20.00", safe="")
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.request(encoded)
        self.assertEqual(caught.exception.code, 404)

    def test_review_save_rejects_regular_file_replaced_after_startup(self) -> None:
        audio_path = pathlib.Path(self.temp.name) / "episode-one.mp3"
        replacement = pathlib.Path(self.temp.name) / "replacement.mp3"
        replacement.write_bytes(b"replacement audio")
        replacement.replace(audio_path)

        review = {
            "id": "episode-one@10.00-20.00",
            "status": "approved",
            "proposed_start_seconds": 10.0,
            "proposed_end_seconds": 20.0,
            "note": "",
            "reviewer": "Dan",
            "audio_fingerprint": TEST_FINGERPRINT,
        }
        with self.assertRaisesRegex(GUI.ValidationError, "changed during verification"):
            self.app.save_review(review)

        self.assertFalse(self.app.store.path.exists())

    def test_range_serve_rejects_regular_file_replaced_after_startup(self) -> None:
        audio_path = pathlib.Path(self.temp.name) / "episode-one.mp3"
        replacement = pathlib.Path(self.temp.name) / "replacement.mp3"
        replacement.write_bytes(b"replacement audio")
        replacement.replace(audio_path)

        encoded = "/audio/" + urllib.parse.quote(
            "episode-one@10.00-20.00",
            safe="",
        )
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.request(encoded, headers={"Range": "bytes=10-19"})
        self.assertEqual(caught.exception.code, 404)


class UISmokeTests(unittest.TestCase):
    def test_required_review_controls_are_present(self) -> None:
        page = GUI.PAGE
        for required in (
            'id="queueFilter"',
            'id="showFilter"',
            'id="audio"',
            'id="rawVerdict"',
            'id="proposedStart"',
            'id="proposedEnd"',
            'id="setStart"',
            'id="setEnd"',
            'data-status="approved"',
            'data-status="rebounded"',
            'data-status="rejected"',
            'data-status="unsure"',
            'id="reviewNote"',
            'id="reviewer"',
            'id="previous"',
            'id="next"',
            'id="undo"',
            "textContent",
            'status==="approved"||status==="rebounded"',
            "activeId===submittedId?nextId:activeId",
            "el.audio.pause();if(!item)return",
        ):
            self.assertIn(required, page)


if __name__ == "__main__":
    unittest.main()
