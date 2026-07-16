"""Tests for the playhead-xsdz.41 self-supervised lexical anchor miner
(scripts/l2f-lexical-anchor-miner.py). Synthetic transcript world proving:
discovery of a planted recurring phrase, distinct-episode support counting
(plus the documented single-episode provisional fallback), edge-affinity
rejection of a phrase that also fires mid-content, spread-gate rejection,
the exact-only short-phrase rule, rediff tier skepticism (more support
demanded), subsumption pruning, and the circularity guard (lexical-tagged
edges refused). Follows the test_l2f_lexical_anchor.py conventions
(importlib loading of hyphenated modules, in-code fixtures, stdlib
unittest)."""

import hashlib
import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load(name: str, filename: str):
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


MINER = _load("l2f_lexical_anchor_miner", "l2f-lexical-anchor-miner.py")
PROTO = _load("l2f_lexical_anchor_prototype", "l2f-lexical-anchor-prototype.py")


# ---------------------------------------------------------------------------
# Synthetic world builders
# ---------------------------------------------------------------------------

def _word(norm: str, start_s: float, dur_s: float = 0.3):
    return PROTO.Word(norm, start_s, start_s + dur_s)


def _phrase_words(text: str, start_s: float, gap_s: float = 0.4):
    words = []
    t = start_s
    for raw in text.split():
        words.append(_word(PROTO.normalize_word(raw), t))
        t += gap_s
    return words


def _occupied_spans(plants, gap_s: float = 0.4):
    return [
        (start_s - 0.5, start_s + len(text.split()) * gap_s + 0.5)
        for start_s, text in plants
    ]


def _episode_words(ep_tag: str, duration_s: float, plants):
    """Unique-per-episode filler at 1s cadence + planted phrases.

    `plants` is a list of (start_seconds, text). Unique filler guarantees
    no accidental cross-episode recurring n-grams; filler skips the
    planted spans so a plant stays a contiguous n-gram.
    """
    occupied = _occupied_spans(plants)
    words = []
    for i in range(int(duration_s)):
        t = float(i)
        if any(lo <= t <= hi for lo, hi in occupied):
            continue
        words.append(_word(f"filler{ep_tag}x{i}", t))
    for start_s, text in plants:
        words.extend(_phrase_words(text, start_s))
    return sorted(words, key=lambda w: w.start_s)


def _gold_edge(eid: str, slug: str, side: str, t: float, idx: int = 0):
    return MINER.Edge(eid, slug, side, t, MINER.GOLD_SOURCE, f"{eid}:gold:{idx}:{side}")


def _rediff_edge(eid: str, slug: str, side: str, t: float, idx: int = 0):
    return MINER.Edge(
        eid, slug, side, t, MINER.REDIFF_SOURCE, f"{eid}:rediff:{idx}:{side}"
    )


PHRASE = "zebra quantum flapjack rides"
PHRASE_WORDS = tuple(PROTO.normalize_phrase(PHRASE))


def _mine(words_by_episode, edges, intervals=None, keep=None):
    return MINER.mine_show(
        "testshow",
        words_by_episode,
        edges,
        intervals or {eid: [] for eid in words_by_episode},
        keep_rejected=keep,
    )


# ---------------------------------------------------------------------------
# Discovery + subsumption
# ---------------------------------------------------------------------------

class DiscoveryTests(unittest.TestCase):
    def test_planted_recurring_phrase_discovered(self):
        words_by_episode, edges, intervals = {}, [], {}
        for i in range(3):
            eid = f"testshow-2026-01-0{i + 1}-ep"
            words_by_episode[eid] = _episode_words(str(i), 400.0, [(98.0, PHRASE)])
            edges.append(_gold_edge(eid, "testshow", "pre", 100.0))
            intervals[eid] = [(100.0, 130.0)]
        passed, _ = _mine(words_by_episode, edges, intervals)

        self.assertEqual(len(passed), 1)
        cand = passed[0]
        self.assertEqual(cand.words, PHRASE_WORDS)
        self.assertEqual(cand.side, "pre")
        self.assertEqual(cand.support_kind, "gold")
        self.assertEqual(len(cand.all_episodes), 3)
        self.assertAlmostEqual(cand.median_offset_s, -2.0, places=3)
        self.assertLessEqual(cand.spread_s, 0.01)
        self.assertEqual(cand.affinity, 1.0)
        self.assertGreaterEqual(cand.rate_ratio, MINER.MIN_RATE_RATIO)
        self.assertGreater(cand.confidence, 0.0)
        self.assertEqual(cand.rejected_reason, "")

    def test_long_repeated_span_collapses_to_one_entry(self):
        """A 12-word span repeated at 2 edges yields 8-gram shingles that
        span-merge into a single span-start representative."""
        span = "kumquat velvet trombone glacier pickle sonata walrus lantern origami thunder biscuit meridian"
        span_words = tuple(PROTO.normalize_phrase(span))
        words_by_episode, edges = {}, []
        for i in range(2):
            eid = f"testshow-2026-01-0{i + 1}-ep"
            words_by_episode[eid] = _episode_words(str(i), 400.0, [(102.0, span)])
            edges.append(_gold_edge(eid, "testshow", "pre", 100.0))
        passed, rejected = _mine(
            words_by_episode, edges, keep=lambda c: len(c.words) == 8
        )
        self.assertEqual(len(passed), 1)
        rep = passed[0]
        self.assertEqual(rep.words, span_words[:8])  # span-start shingle
        self.assertEqual(rep.span_shingle_count, 4)  # 5 shingles - 1 rep
        self.assertGreater(rep.span_end_offset_s, rep.median_offset_s)
        merged = [c for c in rejected if c.rejected_reason == "span-merged"]
        self.assertEqual(len(merged), 4)
        for c in merged:
            self.assertEqual(c.absorbed_into, " ".join(rep.words))

    def test_subgrams_absorbed_by_longest_phrase(self):
        words_by_episode, edges = {}, []
        for i in range(3):
            eid = f"testshow-2026-01-0{i + 1}-ep"
            words_by_episode[eid] = _episode_words(str(i), 400.0, [(98.0, PHRASE)])
            edges.append(_gold_edge(eid, "testshow", "pre", 100.0))
        sub = tuple(PHRASE_WORDS[:2])
        passed, rejected = _mine(
            words_by_episode, edges, keep=lambda c: c.words == sub
        )
        self.assertEqual([c.words for c in passed], [PHRASE_WORDS])
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0].rejected_reason, "subsumed")
        self.assertEqual(rejected[0].absorbed_into, " ".join(PHRASE_WORDS))


# ---------------------------------------------------------------------------
# Support counting
# ---------------------------------------------------------------------------

class SupportTests(unittest.TestCase):
    def test_same_episode_repeats_do_not_count_as_distinct_support(self):
        """Two edges in ONE episode of a multi-episode show: support=1 ep."""
        e1 = "testshow-2026-01-01-ep"
        e2 = "testshow-2026-01-02-ep"
        words_by_episode = {
            e1: _episode_words("a", 400.0, [(98.0, PHRASE), (298.0, PHRASE)]),
            e2: _episode_words("b", 400.0, []),
        }
        edges = [
            _gold_edge(e1, "testshow", "pre", 100.0, 0),
            _gold_edge(e1, "testshow", "pre", 300.0, 1),
            _gold_edge(e2, "testshow", "pre", 200.0, 0),
        ]
        passed, rejected = _mine(
            words_by_episode, edges, keep=lambda c: c.words == PHRASE_WORDS
        )
        self.assertEqual(passed, [])
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0].rejected_reason, "support")
        self.assertEqual(rejected[0].all_episodes, (e1,))

    def test_single_episode_show_provisional_fallback(self):
        """One labeled episode + >=3 distinct edges => provisional candidate."""
        eid = "testshow-2026-01-01-ep"
        words_by_episode = {
            eid: _episode_words(
                "a", 700.0, [(98.0, PHRASE), (298.0, PHRASE), (498.0, PHRASE)]
            )
        }
        edges = [
            _gold_edge(eid, "testshow", "pre", 100.0, 0),
            _gold_edge(eid, "testshow", "pre", 300.0, 1),
            _gold_edge(eid, "testshow", "pre", 500.0, 2),
        ]
        passed, _ = _mine(words_by_episode, edges)
        self.assertEqual([c.words for c in passed], [PHRASE_WORDS])
        self.assertEqual(passed[0].support_kind, "single-episode-provisional")
        self.assertEqual(len(passed[0].supporting_edges), 3)

        # Only 2 distinct edges: below the provisional bar.
        passed2, rejected2 = _mine(
            {eid: _episode_words("a", 700.0, [(98.0, PHRASE), (298.0, PHRASE)])},
            edges[:2],
            keep=lambda c: c.words == PHRASE_WORDS,
        )
        self.assertEqual(passed2, [])
        self.assertEqual(rejected2[0].rejected_reason, "support")

    def test_rediff_only_support_demands_three_episodes(self):
        """Rediff skepticism: 2 rediff episodes fail, 3 pass; mixed 1+1 fails."""
        def build(n_eps, sources):
            words_by_episode, edges = {}, []
            for i in range(n_eps):
                eid = f"testshow-2026-01-0{i + 1}-ep"
                words_by_episode[eid] = _episode_words(
                    str(i), 400.0, [(98.0, PHRASE)]
                )
                maker = _gold_edge if sources[i] == "gold" else _rediff_edge
                edges.append(maker(eid, "testshow", "pre", 100.0))
            return words_by_episode, edges

        words, edges = build(2, ["rediff", "rediff"])
        passed, rejected = _mine(words, edges, keep=lambda c: c.words == PHRASE_WORDS)
        self.assertEqual(passed, [])
        self.assertEqual(rejected[0].rejected_reason, "support")

        words, edges = build(3, ["rediff", "rediff", "rediff"])
        passed, _ = _mine(words, edges)
        self.assertEqual([c.words for c in passed], [PHRASE_WORDS])
        self.assertEqual(passed[0].support_kind, "rediff-corroborated")

        words, edges = build(2, ["gold", "rediff"])
        passed, rejected = _mine(words, edges, keep=lambda c: c.words == PHRASE_WORDS)
        self.assertEqual(passed, [])
        self.assertEqual(rejected[0].rejected_reason, "support")

        words, edges = build(2, ["gold", "gold"])
        passed, _ = _mine(words, edges)
        self.assertEqual([c.words for c in passed], [PHRASE_WORDS])
        self.assertEqual(passed[0].support_kind, "gold")


# ---------------------------------------------------------------------------
# Offset spread gate
# ---------------------------------------------------------------------------

class SpreadGateTests(unittest.TestCase):
    def test_inconsistent_offsets_rejected(self):
        """Offsets -15s and +12s: both in-window, no consistent median."""
        words_by_episode, edges = {}, []
        for i, offset in enumerate((-15.0, +12.0)):
            eid = f"testshow-2026-01-0{i + 1}-ep"
            words_by_episode[eid] = _episode_words(
                str(i), 400.0, [(100.0 + offset, PHRASE)]
            )
            edges.append(_gold_edge(eid, "testshow", "pre", 100.0))
        passed, rejected = _mine(
            words_by_episode, edges, keep=lambda c: c.words == PHRASE_WORDS
        )
        self.assertEqual(passed, [])
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0].rejected_reason, "offset-spread")

    def test_outlier_excluded_but_consistent_core_survives(self):
        offsets = (-2.0, -2.4, -14.0)  # median -2.4; -14.0 is an outlier
        words_by_episode, edges = {}, []
        for i, offset in enumerate(offsets):
            eid = f"testshow-2026-01-0{i + 1}-ep"
            words_by_episode[eid] = _episode_words(
                str(i), 400.0, [(100.0 + offset, PHRASE)]
            )
            edges.append(_gold_edge(eid, "testshow", "pre", 100.0))
        passed, _ = _mine(words_by_episode, edges)
        self.assertEqual([c.words for c in passed], [PHRASE_WORDS])
        cand = passed[0]
        self.assertEqual(len(cand.supporting), 2)
        self.assertEqual(len(cand.all_episodes), 2)
        self.assertEqual(len(cand.occurrences), 3)


# ---------------------------------------------------------------------------
# Edge-affinity gate
# ---------------------------------------------------------------------------

class AffinityTests(unittest.TestCase):
    def test_mid_content_phrase_rejected(self):
        """Fires at 2 gold edges but 3x mid-content per episode: trap."""
        words_by_episode, edges, intervals = {}, [], {}
        for i in range(2):
            eid = f"testshow-2026-01-0{i + 1}-ep"
            plants = [(98.0, PHRASE)] + [
                (200.0 + 60.0 * k, PHRASE) for k in range(3)
            ]
            words_by_episode[eid] = _episode_words(str(i), 400.0, plants)
            edges.append(_gold_edge(eid, "testshow", "pre", 100.0))
            intervals[eid] = [(100.0, 130.0)]
        passed, rejected = _mine(
            words_by_episode, edges, intervals, keep=lambda c: c.words == PHRASE_WORDS
        )
        self.assertEqual(passed, [])
        self.assertEqual(len(rejected), 1)
        cand = rejected[0]
        self.assertEqual(cand.rejected_reason, "edge-affinity")
        self.assertEqual(cand.near_count, 2)
        self.assertEqual(cand.content_count, 6)
        self.assertAlmostEqual(cand.affinity, 0.25, places=3)

    def test_in_break_occurrences_are_neutral(self):
        """Mid-pod re-attributions must not count against affinity."""
        words_by_episode, edges, intervals = {}, [], {}
        for i in range(2):
            eid = f"testshow-2026-01-0{i + 1}-ep"
            # Near the pre edge, plus once deep inside the break interval.
            words_by_episode[eid] = _episode_words(
                str(i), 400.0, [(98.0, PHRASE), (160.0, PHRASE)]
            )
            edges.append(_gold_edge(eid, "testshow", "pre", 100.0))
            intervals[eid] = [(100.0, 190.0)]
        passed, _ = _mine(words_by_episode, edges, intervals)
        self.assertEqual([c.words for c in passed], [PHRASE_WORDS])
        cand = passed[0]
        self.assertEqual(cand.near_count, 2)
        self.assertEqual(cand.neutral_count, 2)
        self.assertEqual(cand.content_count, 0)
        self.assertEqual(cand.affinity, 1.0)


# ---------------------------------------------------------------------------
# Exact-only short-phrase rule
# ---------------------------------------------------------------------------

class ShortPhraseRuleTests(unittest.TestCase):
    def _count(self, cand_words, stream_text):
        cand = MINER.Candidate("testshow", "pre", cand_words, [])
        words = _phrase_words(stream_text, 10.0)
        return MINER.count_candidate_occurrences(
            cand, {"testshow-2026-01-01-ep": words}
        )

    def test_short_phrase_counts_exact_only(self):
        hits = self._count(("alpha", "beta", "gamma"), "alpha beta gamma")
        self.assertEqual(len(hits), 1)
        # Fuzzy ASR variant is NOT folded for a short phrase.
        hits = self._count(("alpha", "beta", "gamma"), "alpha beta gama")
        self.assertEqual(hits, [])

    def test_long_phrase_counts_fuzzy_variants(self):
        hits = self._count(
            ("alpha", "beta", "gamma", "delta"), "alpha beta gama delta"
        )
        self.assertEqual(len(hits), 1)

    def test_match_policy_in_bank_entry(self):
        short = MINER.Candidate("testshow", "pre", ("a1", "b2", "c3"), [])
        short.support_kind = "gold"
        long = MINER.Candidate("testshow", "pre", ("a1", "b2", "c3", "d4"), [])
        long.support_kind = "gold"
        self.assertEqual(MINER.candidate_entry(short)["matchPolicy"], "exact")
        self.assertEqual(
            MINER.candidate_entry(long)["matchPolicy"],
            f"fuzzy-{MINER.FUZZY_COUNT_RATIO:.2f}",
        )


# ---------------------------------------------------------------------------
# Circularity guard
# ---------------------------------------------------------------------------

class CircularityGuardTests(unittest.TestCase):
    def test_lexical_tagged_edges_refused(self):
        eid = "testshow-2026-01-01-ep"
        words_by_episode = {eid: _episode_words("a", 400.0, [(98.0, PHRASE)])}
        edge = MINER.Edge(
            eid, "testshow", "pre", 100.0, "lexical-bank-v1", f"{eid}:lex:0:pre"
        )
        with self.assertRaises(MINER.CircularEdgeSourceError):
            _mine(words_by_episode, [edge])

    def test_unknown_source_refused(self):
        eid = "testshow-2026-01-01-ep"
        words_by_episode = {eid: _episode_words("a", 400.0, [])}
        edge = MINER.Edge(
            eid, "testshow", "pre", 100.0, "chapter-v9", f"{eid}:ch:0:pre"
        )
        with self.assertRaises(ValueError):
            _mine(words_by_episode, [edge])

    def test_allowlist_is_structurally_lexical_free(self):
        for source in MINER.ALLOWED_EDGE_SOURCES:
            self.assertNotIn("lex", source.lower())


# ---------------------------------------------------------------------------
# Weak-label loading
# ---------------------------------------------------------------------------

class EdgeLoadingTests(unittest.TestCase):
    def test_rediff_edges_deduped_against_gold(self):
        evaluation = {
            "assets": [
                {
                    "episode_id": "testshow-2026-01-01-ep",
                    "show_name": "Test Show",
                    "duration_seconds": 400.0,
                    "full_breaks": [{"start_seconds": 100.0, "end_seconds": 130.0}],
                }
            ]
        }
        gold_edges, gold_intervals = MINER.load_gold_edges(evaluation)
        self.assertEqual(len(gold_edges), 2)
        self.assertEqual(gold_intervals["testshow-2026-01-01-ep"], [(100.0, 130.0)])

        rediff = {
            "pairs": [
                # Same boundary as gold (within dedupe tolerance): dropped.
                {"episodeId": "testshow-2026-01-01-ep", "slotStart": 104.0, "slotEnd": 133.0},
                # Distinct slot: kept.
                {"episodeId": "testshow-2026-01-01-ep", "slotStart": 250.0, "slotEnd": 290.0},
            ]
        }
        rediff_edges, _ = MINER.load_rediff_edges(rediff, gold_edges)
        times = sorted((e.side, e.time_s) for e in rediff_edges)
        self.assertEqual(times, [("post", 290.0), ("pre", 250.0)])
        for e in rediff_edges:
            self.assertEqual(e.source, MINER.REDIFF_SOURCE)

    def test_gold_preferred_when_occurrence_near_both_edges(self):
        eid = "testshow-2026-01-01-ep"
        words = _episode_words("a", 400.0, [(98.0, PHRASE)])
        edges = [
            _gold_edge(eid, "testshow", "pre", 110.0),      # offset -12
            _rediff_edge(eid, "testshow", "pre", 99.0),     # offset -1 but rediff
        ]
        grouped = MINER.gather_occurrences(words, edges)
        occs = grouped[("pre", PHRASE_WORDS)]
        self.assertEqual(len(occs), 1)
        self.assertEqual(occs[0].edge.source, MINER.GOLD_SOURCE)


# ---------------------------------------------------------------------------
# Validation targets are post-hoc only
# ---------------------------------------------------------------------------

class ValidationIsolationTests(unittest.TestCase):
    def test_mining_discovers_phrases_absent_from_validation_targets(self):
        """The discovery test phrase appears in no validation target: mining
        cannot be seeded by the scorecard patterns."""
        for target in MINER.VALIDATION_TARGETS:
            for pat in target["any_of"]:
                self.assertFalse(
                    MINER._contiguous_subsequence(pat, PHRASE_WORDS)
                    or pat == PHRASE_WORDS
                )

    def test_scorecard_reports_found_and_near(self):
        cand = MINER.Candidate(
            "smartless", "pre", ("well", "be", "right", "back"), []
        )
        cand.support_kind = "gold"
        rejected = MINER.Candidate(
            "smartless", "post", ("back", "to", "the", "show"), []
        )
        rejected.rejected_reason = "support"
        rows = MINER.validation_scorecard(
            {"smartless": [cand]}, {"smartless": [rejected]}
        )
        by_id = {r["target"]["id"]: r for r in rows}
        self.assertEqual(by_id["smartless-pre"]["found"], [cand])
        self.assertEqual(by_id["smartless-resume"]["found"], [])
        self.assertEqual(by_id["smartless-resume"]["near"], [rejected])


# ---------------------------------------------------------------------------
# CLI end to end
# ---------------------------------------------------------------------------

def _token(text, from_ms, to_ms):
    return {"text": text, "offsets": {"from": from_ms, "to": to_ms}, "p": 0.9}


def _transcript_json(plants, duration_s, ep_tag):
    """Whisper-shaped transcript: unique filler + planted phrases."""
    occupied = _occupied_spans(plants)
    tokens = []
    for i in range(int(duration_s)):
        if any(lo <= i <= hi for lo, hi in occupied):
            continue
        tokens.append(_token(f" filler{ep_tag}x{i}", i * 1000, i * 1000 + 300))
    for start_s, text in plants:
        t = int(start_s * 1000)
        for w in text.split():
            tokens.append(_token(f" {w}", t, t + 300))
            t += 400
    tokens.sort(key=lambda t: t["offsets"]["from"])
    segment = {
        "offsets": {"from": 0, "to": int(duration_s * 1000)},
        "text": "".join(t["text"] for t in tokens),
        "tokens": tokens,
    }
    return {"transcription": [segment]}


class CliEndToEndTests(unittest.TestCase):
    def test_bank_and_report_written_with_provenance(self):
        phrase = "purple monkey dishwasher runs"
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = pathlib.Path(tmp)
            transcripts = tmpdir / "transcripts"
            transcripts.mkdir()
            assets = []
            for i in range(2):
                eid = f"testshow-2026-01-0{i + 1}-ep"
                (transcripts / f"{eid}.json").write_text(
                    json.dumps(_transcript_json([(98.0, phrase)], 400.0, str(i)))
                )
                assets.append(
                    {
                        "episode_id": eid,
                        "show_name": "Test Show",
                        "duration_seconds": 400.0,
                        "full_breaks": [
                            {"start_seconds": 100.0, "end_seconds": 130.0}
                        ],
                        "content_vetoes": [],
                    }
                )
            # A show with transcripts but no weak labels: growth-path case.
            (transcripts / "newshow-2026-02-01-ep.json").write_text(
                json.dumps(_transcript_json([], 300.0, "n"))
            )

            evaluation_path = tmpdir / "gold.json"
            evaluation_path.write_text(json.dumps({"assets": assets}))
            rediff_path = tmpdir / "rediff.json"
            rediff_path.write_text(
                json.dumps(
                    {
                        "pairs": [
                            {
                                "episodeId": "testshow-2026-01-01-ep",
                                "slotStart": 250.0,
                                "slotEnd": 280.0,
                            }
                        ]
                    }
                )
            )
            manifest_path = tmpdir / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    [
                        {
                            "episodeId": "testshow-2026-01-01-ep",
                            "show": "Test Show",
                            "showSlug": "testshow",
                            "feedUrl": "https://example.com/feed",
                        }
                    ]
                )
            )
            bank_path = tmpdir / "bank.json"
            report_path = tmpdir / "report.md"

            rc = MINER.main(
                [
                    "--evaluation", str(evaluation_path),
                    "--rediff-baseline", str(rediff_path),
                    "--transcripts-dir", str(transcripts),
                    "--snapshots-manifest", str(manifest_path),
                    "--bank-out", str(bank_path),
                    "--report-out", str(report_path),
                ]
            )
            self.assertEqual(rc, 0)

            bank = json.loads(bank_path.read_text())
            self.assertEqual(bank["bankKind"], "lexical-anchor-candidates")
            self.assertEqual(
                bank["sources"]["evaluationSha256"],
                hashlib.sha256(evaluation_path.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                bank["sources"]["rediffBaselineSha256"],
                hashlib.sha256(rediff_path.read_bytes()).hexdigest(),
            )
            self.assertEqual(bank["sources"]["transcriptCount"], 3)
            self.assertEqual(len(bank["shows"]), 1)
            show = bank["shows"][0]
            self.assertEqual(
                show["showKeys"], ["testshow", "https://example.com/feed"]
            )
            phrases = [e["phrase"] for e in show["entries"]]
            self.assertIn("purple monkey dishwasher runs", phrases)
            entry = show["entries"][phrases.index("purple monkey dishwasher runs")]
            self.assertEqual(entry["side"], "pre")
            self.assertEqual(entry["status"], "candidate")
            self.assertEqual(entry["supportKind"], "gold")
            self.assertEqual(len(entry["supportEpisodes"]), 2)
            self.assertAlmostEqual(entry["offsetMedianSeconds"], -2.0, places=1)

            report = report_path.read_text()
            self.assertIn("Known-anchor validation scorecard", report)
            self.assertIn("Shows awaiting weak labels", report)
            self.assertIn("| newshow | 1 |", report)
            self.assertIn("purple monkey dishwasher runs", report)


if __name__ == "__main__":
    unittest.main()
