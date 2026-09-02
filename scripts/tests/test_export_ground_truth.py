"""Tests for playhead-078u's ground-truth export (scripts/export_ground_truth.py).

The unit under test reduces an 84 MB verdict-bearing device pull to a small
fixture that git can hold, and it must do so WITHOUT carrying episode content
into a public repository.

Every case here is one the first version of the script got wrong, or one that
would make the fixture stop being evidence:

  * it dropped the `evidenceText` COLUMN, asserted "no content", passed — and
    still wrote four verbatim transcript quotes, because the same key is nested
    inside `correction_events.targetRefsJSON` and the assertion only inspected
    top-level keys. `test_nested_evidence_text_is_removed` and
    `test_guard_fires_when_the_defect_is_reintroduced` are that bug, and the
    second one PROVES THE RAIL by re-introducing it: a guard that has never
    been made to fire is not evidence.
  * a fixture that is not deterministic has an unreadable diff, so nobody
    notices when the DATA changed.
  * a fixture that silently exports zero rows looks like a successful run. The
    checked-in copy is pinned by count here, so truncation fails the suite
    rather than being discovered years later, which is the whole reason this
    bead exists.

Conventions follow test_gate_memory_verdict.py: importlib module loading,
in-code fixture builders, stdlib unittest, nothing touching a real device pull.
"""

import importlib.util
import json
import pathlib
import sqlite3
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
REPO = ROOT.parent


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


egt = _load("export_ground_truth", "export_ground_truth.py")

QUOTE = (
    "a most replayed moment from a previous episode. If you want to listen to "
    "that full episode, I've linked it down below."
)

ASSET_A = "AAAAAAAA-0000-0000-0000-000000000001"
ASSET_B = "BBBBBBBB-0000-0000-0000-000000000002"


def _target_refs(with_quote: bool) -> str:
    """The real shape: a projection nested inside a JSON string column."""
    projection = {
        "id": "1B26604E-59D3-449D-81E2-E8ADD98DA9FF",
        "analysisAssetId": ASSET_A,
        "startTime": 210,
        "endTime": 240,
        "confidence": 0.42,
        "eligibilityGate": "markOnly",
        "wasSkipped": False,
    }
    if with_quote:
        projection["evidenceText"] = QUOTE
    return json.dumps(
        {
            "adWindowId": "B63DFC91-F709-4BB5-80F2-12F6CA31A74D",
            "explicitFeedbackDetectionProjection": projection,
            "exactFeedbackSpan": {"startTimeBitPattern": 1, "endTimeBitPattern": 2},
        }
    )


def build_pull(path: pathlib.Path, *, nested_quote: bool = True) -> None:
    """A miniature device pull carrying the columns the exporter reads."""
    db = sqlite3.connect(path)
    db.executescript(
        """
        CREATE TABLE analysis_assets (
            id TEXT PRIMARY KEY, episodeId TEXT, episodeTitle TEXT,
            sourceURL TEXT, assetFingerprint TEXT, episodeDurationSec REAL,
            analysisState TEXT, analysisVersion TEXT, createdAt REAL
        );
        CREATE TABLE correction_events (
            id TEXT PRIMARY KEY, analysisAssetId TEXT NOT NULL, scope TEXT,
            createdAt REAL, source TEXT, podcastId TEXT, correctionType TEXT,
            targetRefsJSON TEXT
        );
        CREATE TABLE ad_windows (
            id TEXT PRIMARY KEY, analysisAssetId TEXT NOT NULL,
            startTime REAL, endTime REAL, confidence REAL,
            advertiser TEXT, product TEXT, evidenceText TEXT,
            eligibilityGate TEXT
        );
        CREATE TABLE transcript_chunks (id TEXT PRIMARY KEY, text TEXT);
        """
    )
    db.execute(
        "INSERT INTO analysis_assets VALUES (?,?,?,?,?,?,?,?,?)",
        (ASSET_A, "ep-a", "Corrected Episode", "https://feed.example/rss",
         "fp-a", 1676.0, "complete", "v1", 1.0),
    )
    # ASSET_B has NO correction. It must not reach the fixture: the export is
    # the verdict-bearing subset, not the whole pull.
    db.execute(
        "INSERT INTO analysis_assets VALUES (?,?,?,?,?,?,?,?,?)",
        (ASSET_B, "ep-b", "Untouched Episode", "https://feed.example/rss",
         "fp-b", 900.0, "complete", "v1", 2.0),
    )
    db.execute(
        "INSERT INTO correction_events VALUES (?,?,?,?,?,?,?,?)",
        ("C0000001-0000-0000-0000-000000000001", ASSET_A,
         f"exactTimeSpan:{ASSET_A}:210.000:240.000", 100.0,
         "bannerSuggestionConfirmed", "https://feed.example/rss",
         "falseNegative", _target_refs(nested_quote)),
    )
    db.execute(
        "INSERT INTO correction_events VALUES (?,?,?,?,?,?,?,?)",
        ("C0000002-0000-0000-0000-000000000002", ASSET_A,
         f"exactTimeSpan:{ASSET_A}:600.000:660.000", 200.0,
         "manualVeto", "https://feed.example/rss", "falsePositive", None),
    )
    db.execute(
        "INSERT INTO ad_windows VALUES (?,?,?,?,?,?,?,?,?)",
        ("W0000001-0000-0000-0000-000000000001", ASSET_A, 210.0, 240.0, 0.42,
         "Squarespace", "Website builder", QUOTE, "markOnly"),
    )
    db.execute(
        "INSERT INTO ad_windows VALUES (?,?,?,?,?,?,?,?,?)",
        ("W0000002-0000-0000-0000-000000000002", ASSET_B, 10.0, 20.0, 0.9,
         None, None, QUOTE, "eligible"),
    )
    db.execute("INSERT INTO transcript_chunks VALUES ('t1', ?)", (QUOTE,))
    db.commit()
    db.close()


class ExportGroundTruthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = pathlib.Path(self.tmp.name)
        self.db = self.dir / "pull.sqlite"
        build_pull(self.db)
        self.addCleanup(self.tmp.cleanup)

    def payload(self, **kwargs):
        return egt.build_payload(self.db, "test-label", **kwargs)

    # --- the content rules -------------------------------------------------

    def test_evidence_text_column_is_not_exported(self):
        for row in self.payload()["ad_windows"]:
            self.assertNotIn("evidenceText", row)

    def test_nested_evidence_text_is_removed(self):
        """The bug that shipped: the column was dropped, the nested copy was not."""
        rendered = egt.render(self.payload())
        rows = json.loads(rendered)
        blob = json.dumps(
            {k: rows[k] for k in ("correction_events", "analysis_assets", "ad_windows")}
        )
        self.assertNotIn("evidenceText", blob)
        self.assertNotIn(QUOTE, blob)
        self.assertNotIn(QUOTE[:40], blob)

    def test_scrubbed_blob_keeps_every_fact(self):
        """Scrubbing removes the quote and nothing else."""
        row = next(
            c for c in self.payload()["correction_events"] if c["targetRefsJSON"]
        )
        projection = json.loads(row["targetRefsJSON"])[
            "explicitFeedbackDetectionProjection"
        ]
        self.assertNotIn("evidenceText", projection)
        self.assertEqual(projection["startTime"], 210)
        self.assertEqual(projection["endTime"], 240)
        self.assertEqual(projection["eligibilityGate"], "markOnly")
        self.assertEqual(projection["confidence"], 0.42)

    def test_transcript_table_is_never_exported(self):
        self.assertNotIn("transcript_chunks", self.payload())

    def test_guard_fires_when_the_defect_is_reintroduced(self):
        """Prove the rail. A green guard nobody has made fire is not evidence.

        This is the exact payload the first version of the script produced:
        no `evidenceText` COLUMN, a quote nested one level down inside a JSON
        string. The old guard passed it. This one must not.
        """
        smuggled = {
            "correction_events": [
                {"id": "c1", "targetRefsJSON": _target_refs(with_quote=True)}
            ],
            "analysis_assets": [],
            "ad_windows": [],
        }
        with self.assertRaises(ValueError) as caught:
            egt.assert_no_content(smuggled)
        self.assertIn("evidenceText", str(caught.exception))

    def test_guard_fires_on_a_plain_nested_dict(self):
        with self.assertRaises(ValueError):
            egt.assert_no_content(
                {"ad_windows": [{"nested": {"evidenceText": QUOTE}}]}
            )

    def test_guard_fires_on_a_content_table(self):
        with self.assertRaises(ValueError):
            egt.assert_no_content({"transcript_chunks": [{"text": QUOTE}]})

    def test_guard_ignores_the_excluded_metadata_block(self):
        """The manifest names the banned key on purpose; that is not a leak."""
        egt.assert_no_content(
            {
                "excluded": {"keys_at_any_depth": ["evidenceText"]},
                "correction_events": [],
                "analysis_assets": [],
                "ad_windows": [],
            }
        )

    # --- the reduction -----------------------------------------------------

    def test_only_corrected_assets_are_exported(self):
        ids = {a["id"] for a in self.payload()["analysis_assets"]}
        self.assertEqual(ids, {ASSET_A})

    def test_only_windows_on_corrected_assets_are_exported(self):
        ids = {w["analysisAssetId"] for w in self.payload()["ad_windows"]}
        self.assertEqual(ids, {ASSET_A})

    def test_source_hash_and_counts_are_recorded(self):
        source = self.payload()["source"]
        self.assertEqual(source["sha256"], egt.sha256_of(self.db))
        self.assertEqual(source["correction_events"], 2)
        self.assertEqual(source["assets_referenced"], 1)
        self.assertEqual(source["ad_windows"], 1)

    def test_a_pull_with_no_corrections_is_refused(self):
        """Zero corrections is not ground truth, and must not look like success."""
        empty = self.dir / "empty.sqlite"
        build_pull(empty)
        db = sqlite3.connect(empty)
        db.execute("DELETE FROM correction_events")
        db.commit()
        db.close()
        with self.assertRaises(ValueError) as caught:
            egt.build_payload(empty, "empty")
        self.assertIn("correction_events", str(caught.exception))

    # --- readability of the diff -------------------------------------------

    def test_export_is_deterministic(self):
        self.assertEqual(egt.render(self.payload()), egt.render(self.payload()))

    def test_a_pull_without_the_nested_quote_exports_the_same_facts(self):
        """Scrubbing is not load-bearing for the facts: same output either way."""
        clean = self.dir / "clean.sqlite"
        build_pull(clean, nested_quote=False)
        with_quote = json.loads(egt.render(self.payload()))
        without = json.loads(egt.render(egt.build_payload(clean, "test-label")))
        self.assertEqual(
            with_quote["correction_events"], without["correction_events"]
        )


class CheckedInFixtureTests(unittest.TestCase):
    """The fixture in the tree is the artifact this bead exists to protect.

    playhead-078u was filed because 32 of 38 sibling copies of this data had
    already been truncated to zero bytes without anyone noticing. A count
    pinned here is what turns that from a discovery into a test failure.
    """

    FIXTURE = REPO / "TestFixtures/GroundTruth/2026-08-11-owner-corrections.json"
    SOURCE_SHA256 = "bcad2d09c31607ec593adcfe8749c63b063d2624f3cc8970b584d2b128cb7473"

    def setUp(self):
        if not self.FIXTURE.exists():
            self.fail(f"the ground-truth fixture is missing: {self.FIXTURE}")
        self.data = json.loads(self.FIXTURE.read_text(encoding="utf-8"))

    def test_counts_are_pinned(self):
        self.assertEqual(len(self.data["correction_events"]), 30)
        self.assertEqual(len(self.data["analysis_assets"]), 6)
        self.assertEqual(len(self.data["ad_windows"]), 49)

    def test_the_owners_markup_episode_is_present(self):
        """F4CE7F47 with its 6 corrections is the episode the bead names."""
        marked = [
            c
            for c in self.data["correction_events"]
            if c["analysisAssetId"].startswith("F4CE7F47")
        ]
        self.assertEqual(len(marked), 6)

    def test_source_database_hash_is_recorded(self):
        self.assertEqual(self.data["source"]["sha256"], self.SOURCE_SHA256)

    def test_no_content_reached_the_public_repo(self):
        egt.assert_no_content(self.data)


if __name__ == "__main__":
    unittest.main()
