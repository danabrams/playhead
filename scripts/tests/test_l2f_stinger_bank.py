"""Tests for the playhead-l2f.6 --emit-bank mode of
scripts/l2f-boundary-stinger-prototype.py: full-corpus per-show learning,
bank schema, and content shape. Follows the test_l2f_oracle_gold.py
conventions (importlib loading of hyphenated modules, in-code fixture
builders, stdlib unittest)."""

import hashlib
import importlib.util
import json
import pathlib
import shutil
import struct
import sys
import tempfile
import unittest
import wave

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PROTO = _load("l2f_boundary_stinger_prototype", "l2f-boundary-stinger-prototype.py")

ENVELOPE_HZ = PROTO.ENVELOPE_HZ


def _break(start, end):
    return {
        "start_seconds": start,
        "end_seconds": end,
        "boundary_tolerance_seconds": 0.3,
        "source_ledger_ids": [],
        "source_review_ids": [],
    }


def _evaluation(assets):
    return {
        "artifact_kind": "oracle_earaudit_gold_boundary_evaluation",
        "schema_version": 1,
        "assets": assets,
        "summary": {},
        "label_semantics": {},
        "sources": {},
    }


class FakeEnvelopes:
    """Deterministic synthetic envelope world (no ffmpeg, no audio files).

    Every break edge carries an identical stinger: a pre pattern in
    [edge-4, edge+0.5) around break starts, a post pattern in
    [edge-0.5, edge+4) around break ends, and a flat quiet floor
    everywhere else. Identical relative-to-edge audio across breaks is
    exactly what the learner is supposed to lock onto, with zero learned
    offset and confidence ~1.0.
    """

    sample_rate = 16000

    def __init__(self, evaluation):
        self.pre_edges = {}
        self.post_edges = {}
        for asset in evaluation["assets"]:
            eid = asset["episode_id"]
            self.pre_edges[eid] = [b["start_seconds"] for b in asset["full_breaks"]]
            self.post_edges[eid] = [b["end_seconds"] for b in asset["full_breaks"]]

    def _value(self, episode_id, t):
        for edge in self.pre_edges[episode_id]:
            u = t - edge
            if -4.0 <= u < 0.5:
                return 1.5 + ((u * 7.3) % 1.0)
        for edge in self.post_edges[episode_id]:
            u = t - edge
            if -0.5 <= u < 4.0:
                return 2.5 + ((u * 4.7) % 0.8)
        return 0.1

    def get(self, episode_id, start_s, end_s):
        start_s = max(0.0, start_s)
        frames = int(round((end_s - start_s) * ENVELOPE_HZ))
        times = start_s + (np.arange(frames) + 0.5) / ENVELOPE_HZ
        return np.array(
            [self._value(episode_id, t) for t in times], dtype=np.float64
        )


class ShowSlugTests(unittest.TestCase):
    def test_slug_derivation(self):
        self.assertEqual(
            PROTO.show_slug_from_episode_id(
                "the-nikki-glaser-podcast-2025-02-27-513-food-noise"
            ),
            "the-nikki-glaser-podcast",
        )
        self.assertEqual(
            PROTO.show_slug_from_episode_id("morbid-2026-05-21-title"), "morbid"
        )

    def test_slug_derivation_rejects_unparseable_ids(self):
        with self.assertRaises(ValueError):
            PROTO.show_slug_from_episode_id("no-date-here")


class FeedUrlAliasTests(unittest.TestCase):
    def test_missing_manifest_degrades_to_no_aliases(self):
        self.assertEqual(
            PROTO.load_feed_urls_by_slug(pathlib.Path("/nonexistent/manifest.json")),
            {},
        )
        self.assertEqual(PROTO.load_feed_urls_by_slug(None), {})

    def test_manifest_aliases_are_deduped_and_sorted(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest = pathlib.Path(tmp) / "manifest.json"
            manifest.write_text(
                json.dumps(
                    [
                        {"showSlug": "show-a", "feedUrl": "https://b.example/feed"},
                        {"showSlug": "show-a", "feedUrl": "https://a.example/feed"},
                        {"showSlug": "show-a", "feedUrl": "https://a.example/feed"},
                        {"showSlug": "show-b", "feedUrl": "https://c.example/feed"},
                        {"showSlug": "show-c"},  # no feedUrl → skipped
                    ]
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                PROTO.load_feed_urls_by_slug(manifest),
                {
                    "show-a": ["https://a.example/feed", "https://b.example/feed"],
                    "show-b": ["https://c.example/feed"],
                },
            )


class BuildBankTests(unittest.TestCase):
    """Full-corpus learning + bank content shape on the synthetic world."""

    def setUp(self):
        # Show A: 4 breaks (one truncated at 2.0s), widths all on the 30s
        # grid. Show B: 3 breaks, widths off-grid. Show C: a single break —
        # below the >= 2 learning floor, must not ship.
        self.evaluation = _evaluation(
            [
                {
                    "episode_id": "show-a-2026-01-01-ep1",
                    "show_name": "Show A",
                    "duration_seconds": 3000.0,
                    "full_breaks": [
                        _break(2.0, 62.0),      # truncated pre clip
                        _break(500.0, 590.0),
                        _break(1000.0, 1060.0),
                    ],
                    "presence_anchors": [],
                    "content_vetoes": [],
                },
                {
                    "episode_id": "show-a-2026-01-08-ep2",
                    "show_name": "Show A",
                    "duration_seconds": 3000.0,
                    "full_breaks": [_break(700.0, 760.0)],
                    "presence_anchors": [],
                    "content_vetoes": [],
                },
                {
                    "episode_id": "show-b-2026-01-01-ep1",
                    "show_name": "Show B",
                    "duration_seconds": 3000.0,
                    "full_breaks": [
                        _break(300.0, 345.0),
                        _break(900.0, 952.0),
                        _break(1500.0, 1547.0),
                    ],
                    "presence_anchors": [],
                    "content_vetoes": [],
                },
                {
                    "episode_id": "show-c-2026-01-01-ep1",
                    "show_name": "Show C",
                    "duration_seconds": 3000.0,
                    "full_breaks": [_break(400.0, 460.0)],
                    "presence_anchors": [],
                    "content_vetoes": [],
                },
            ]
        )
        self.envelopes = FakeEnvelopes(self.evaluation)
        self.bank = PROTO.build_bank(
            self.evaluation,
            self.envelopes,
            {"show-a": ["https://feeds.example.com/show-a"]},
        )

    def test_bank_header(self):
        self.assertEqual(self.bank["schemaVersion"], PROTO.BANK_SCHEMA_VERSION)
        self.assertEqual(self.bank["envelopeHz"], ENVELOPE_HZ)
        self.assertEqual(self.bank["pcmSampleRate"], 16000)
        self.assertIn("--emit-bank", self.bank["generator"])
        self.assertIn("full-corpus", self.bank["protocol"])

    def test_show_coverage_and_keys(self):
        keys = [entry["showKeys"][0] for entry in self.bank["shows"]]
        self.assertEqual(keys, ["show-a", "show-b"], "show C has < 2 breaks and must not ship")
        show_a = self.bank["shows"][0]
        self.assertEqual(
            show_a["showKeys"], ["show-a", "https://feeds.example.com/show-a"]
        )
        self.assertEqual(show_a["showName"], "Show A")
        # Show B has no manifest alias → slug-only keys.
        self.assertEqual(self.bank["shows"][1]["showKeys"], ["show-b"])

    def test_sides_learned_with_full_corpus_support(self):
        show_a = self.bank["shows"][0]
        full_len = int(
            (PROTO.TEMPLATE_INNER + PROTO.TEMPLATE_OUTER) * ENVELOPE_HZ
        )
        for side in ("pre", "post"):
            self.assertIn(side, show_a, f"show A must learn the {side} side")
            entry = show_a[side]
            self.assertEqual(len(entry["template"]), full_len)
            self.assertTrue(0 <= entry["edgeSampleIndex"] < len(entry["template"]))
            self.assertGreaterEqual(entry["confidence"], PROTO.LEARN_NCC_MIN)
            self.assertLessEqual(entry["confidence"], 1.0)
            self.assertLessEqual(abs(entry["edgeOffsetSeconds"]), 0.1,
                                 "identical planted stingers must learn ~zero offset")
            self.assertTrue(
                all(isinstance(v, float) for v in entry["template"])
            )
        # FULL-CORPUS (not leave-one-out): all 3 full-width show-A pre
        # clips participate → support 3 (the 2.0s break is truncated and
        # excluded from the pre learning set). The post side has 4
        # full-width clips → support 4.
        self.assertEqual(show_a["pre"]["support"], 3)
        self.assertEqual(show_a["post"]["support"], 4)

    def test_grid_detection(self):
        # Show A widths: 60, 90, 60, 60 → all on the 30s grid.
        self.assertEqual(self.bank["shows"][0]["podWidthGridSeconds"], 30.0)
        # Show B widths: 45, 52, 47 → off-grid.
        self.assertNotIn("podWidthGridSeconds", self.bank["shows"][1])

    def test_truncated_edge_template_never_ships(self):
        # The 2.0s break would produce a 255-frame pre template; the
        # emit-bank protocol (full_templates_only) must keep every shipped
        # template full-width so the runtime never matches a stub.
        for entry in self.bank["shows"]:
            for side in ("pre", "post"):
                if side in entry:
                    self.assertEqual(
                        len(entry[side]["template"]),
                        int((PROTO.TEMPLATE_INNER + PROTO.TEMPLATE_OUTER) * ENVELOPE_HZ),
                    )

    def test_leave_one_out_default_is_unchanged(self):
        # The spike protocol (int exclude_index, no full-template filter)
        # must still learn from the OTHER breaks only — pin that the
        # emit-mode knobs did not leak into the default path.
        show_a_breaks = [
            {**b, "episode_id": a["episode_id"], "show_name": a["show_name"]}
            for a in self.evaluation["assets"]
            if a["show_name"] == "Show A"
            for b in a["full_breaks"]
        ]
        model = PROTO.learn_show_model(show_a_breaks, self.envelopes, 1)
        self.assertIsNotNone(model["post"])
        # Exclusion shrinks the post support: 4 breaks minus the excluded
        # one → exemplar + 2 offset contributors.
        self.assertEqual(model["post"]["support"], 3)


class EmitBankCLITests(unittest.TestCase):
    """End-to-end --emit-bank invocation with a patched envelope extractor
    (no ffmpeg): argparse contract, provenance hashing, deterministic
    output bytes."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = pathlib.Path(tmp.name)

        self.evaluation = _evaluation(
            [
                {
                    "episode_id": "show-a-2026-01-01-ep1",
                    "show_name": "Show A",
                    "duration_seconds": 3000.0,
                    "full_breaks": [
                        _break(500.0, 590.0),
                        _break(1000.0, 1060.0),
                        _break(1500.0, 1560.0),
                    ],
                    "presence_anchors": [],
                    "content_vetoes": [],
                },
            ]
        )
        self.evaluation_path = self.tmp / "gold.json"
        self.evaluation_path.write_text(json.dumps(self.evaluation), encoding="utf-8")

        self.audio_dir = self.tmp / "audio"
        self.audio_dir.mkdir()
        (self.audio_dir / "show-a-2026-01-01-ep1.mp3").write_bytes(b"")

        self.manifest_path = self.tmp / "manifest.json"
        self.manifest_path.write_text(
            json.dumps(
                [{"showSlug": "show-a", "feedUrl": "https://feeds.example.com/a"}]
            ),
            encoding="utf-8",
        )

        # Patch the envelope extractor: same synthetic world, but reached
        # through the REAL emit_bank() path (EnvelopeCache construction,
        # sample-rate selection, JSON writing).
        fake = FakeEnvelopes(self.evaluation)
        self.original_cache = PROTO.EnvelopeCache

        class PatchedCache(PROTO.EnvelopeCache):
            def _extract(cache_self, episode_id, start_s, end_s):
                return fake.get(episode_id, start_s, end_s)

        PROTO.EnvelopeCache = PatchedCache
        self.addCleanup(setattr, PROTO, "EnvelopeCache", self.original_cache)

    def _emit(self, out_path, extra=()):
        return PROTO.main(
            [
                "--evaluation", str(self.evaluation_path),
                "--emit-bank", str(out_path),
                "--audio-dir", str(self.audio_dir),
                "--snapshots-manifest", str(self.manifest_path),
                *extra,
            ]
        )

    def test_emit_bank_writes_valid_bank_with_provenance(self):
        out = self.tmp / "bank.json"
        self.assertEqual(self._emit(out), 0)
        bank = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(bank["schemaVersion"], PROTO.BANK_SCHEMA_VERSION)
        self.assertEqual(bank["pcmSampleRate"], PROTO.BANK_SAMPLE_RATE)
        self.assertEqual(
            [e["showKeys"] for e in bank["shows"]],
            [["show-a", "https://feeds.example.com/a"]],
        )
        # Content-addressed provenance: sha256 of the exact input bytes.
        self.assertEqual(bank["sources"]["evaluation"], "gold.json")
        self.assertEqual(
            bank["sources"]["evaluationSha256"],
            hashlib.sha256(self.evaluation_path.read_bytes()).hexdigest(),
        )
        self.assertEqual(
            bank["sources"]["snapshotsManifestSha256"],
            hashlib.sha256(self.manifest_path.read_bytes()).hexdigest(),
        )
        # File hygiene: sorted keys + trailing newline (diff-friendly).
        raw = out.read_text(encoding="utf-8")
        self.assertTrue(raw.endswith("\n"))
        self.assertEqual(raw, json.dumps(bank, indent=1, sort_keys=True) + "\n")

    def test_emit_bank_exclude_side_curates_and_records(self):
        out = self.tmp / "bank-excluded.json"
        self.assertEqual(self._emit(out, extra=["--exclude-side", "show-a:post"]), 0)
        bank = json.loads(out.read_text(encoding="utf-8"))
        entry = bank["shows"][0]
        self.assertIn("pre", entry)
        self.assertNotIn("post", entry)
        self.assertEqual(bank["sources"]["curatedExclusions"], ["show-a:post"])

    def test_emit_bank_exclude_side_rejects_bad_spec(self):
        out = self.tmp / "bank-bad.json"
        with self.assertRaises(SystemExit):
            self._emit(out, extra=["--exclude-side", "show-a:sideways"])
        self.assertFalse(out.exists())

    def test_emit_bank_is_deterministic(self):
        first = self.tmp / "bank1.json"
        second = self.tmp / "bank2.json"
        self._emit(first)
        self._emit(second)
        self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_missing_manifest_degrades_to_slug_only_keys(self):
        out = self.tmp / "bank-no-manifest.json"
        PROTO.main(
            [
                "--evaluation", str(self.evaluation_path),
                "--emit-bank", str(out),
                "--audio-dir", str(self.audio_dir),
                "--snapshots-manifest", str(self.tmp / "missing.json"),
            ]
        )
        bank = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual([e["showKeys"] for e in bank["shows"]], [["show-a"]])
        self.assertIsNone(bank["sources"]["snapshotsManifest"])
        self.assertIsNone(bank["sources"]["snapshotsManifestSha256"])

    def test_dump_still_required_without_emit_bank(self):
        with self.assertRaises(SystemExit):
            PROTO.main(["--evaluation", str(self.evaluation_path)])


@unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg not installed")
class EnvelopeCacheFFmpegTests(unittest.TestCase):
    """Pin the 16 kHz ffmpeg extraction against the closed-form envelope —
    the same log1p(rms*100) constant the app's `StingerEnvelope.compute`
    pins on the Swift side, so bank templates and runtime envelopes stay in
    one acoustic space."""

    def test_constant_signal_envelope_matches_closed_form(self):
        with tempfile.TemporaryDirectory() as tmp:
            wav_path = pathlib.Path(tmp) / "tone-2026-01-01-const.wav"
            rate = 16000
            seconds = 2
            amplitude = 0.5
            with wave.open(str(wav_path), "wb") as handle:
                handle.setnchannels(1)
                handle.setsampwidth(2)
                handle.setframerate(rate)
                sample = struct.pack("<h", int(amplitude * 32767))
                handle.writeframes(sample * rate * seconds)
            cache = PROTO.EnvelopeCache(
                {"tone-2026-01-01-const": wav_path},
                sample_rate=PROTO.BANK_SAMPLE_RATE,
            )
            env = cache.get("tone-2026-01-01-const", 0.0, 2.0)
            self.assertEqual(env.size, 2 * ENVELOPE_HZ)
            expected = np.log1p(amplitude * 100.0)
            self.assertTrue(
                np.allclose(env, expected, atol=0.01),
                f"envelope {env[:5]}... != log1p(50) ≈ {expected:.4f}",
            )
            self.assertEqual(cache.hop, PROTO.BANK_SAMPLE_RATE // ENVELOPE_HZ)


if __name__ == "__main__":
    sys.exit(unittest.main())
