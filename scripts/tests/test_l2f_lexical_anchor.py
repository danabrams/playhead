"""Tests for the playhead-xsdz.37 lexical anchor prototype
(scripts/l2f-lexical-anchor-prototype.py): normalisation, whisper-token word
stream, metadata template compilation, fuzzy matching, gold scoring, and the
CLI end to end. Follows the test_l2f_stinger_bank.py conventions (importlib
loading of hyphenated modules, in-code fixture builders, stdlib unittest)."""

import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # Registration is required before exec on Python 3.9: dataclass field
    # resolution looks the module up in sys.modules.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


PROTO = _load("l2f_lexical_anchor_prototype", "l2f-lexical-anchor-prototype.py")


def _token(text, from_ms, to_ms):
    return {"text": text, "offsets": {"from": from_ms, "to": to_ms}, "p": 0.9}


def _segment(tokens):
    return {
        "offsets": {
            "from": tokens[0]["offsets"]["from"],
            "to": tokens[-1]["offsets"]["to"],
        },
        "text": "".join(t["text"] for t in tokens),
        "tokens": tokens,
    }


def _words_from_sentence(sentence, start_ms=0, step_ms=400):
    """One segment whose tokens are the sentence's words (leading spaces)."""
    tokens = []
    t = start_ms
    for word in sentence.split():
        tokens.append(_token(f" {word}", t, t + step_ms))
        t += step_ms
    return [_segment(tokens)]


def _break(start, end):
    return {
        "start_seconds": start,
        "end_seconds": end,
        "boundary_tolerance_seconds": 0.3,
        "source_ledger_ids": [],
        "source_review_ids": [],
    }


class NormalizationTests(unittest.TestCase):
    def test_lowercase_and_punctuation_stripped(self):
        self.assertEqual(PROTO.normalize_word("Wise."), "wise")
        self.assertEqual(PROTO.normalize_word("SmartLess,"), "smartless")

    def test_apostrophes_folded_and_removed(self):
        self.assertEqual(PROTO.normalize_word("We'll"), "well")
        self.assertEqual(PROTO.normalize_word("We’ll"), "well")

    def test_phrase_normalisation_drops_empty_words(self):
        self.assertEqual(
            PROTO.normalize_phrase("we'll be right back!"),
            ("well", "be", "right", "back"),
        )
        self.assertEqual(PROTO.normalize_phrase("-- ..."), ())


class WordStreamTests(unittest.TestCase):
    def test_subword_tokens_merge_into_words_with_timestamps(self):
        segments = [
            _segment(
                [
                    _token("[_BEG_]", 0, 0),
                    _token(" Any", 100, 300),
                    _token("body", 300, 600),
                    _token(" here", 600, 900),
                    _token(".", 900, 900),
                ]
            )
        ]
        words = PROTO.build_word_stream(segments)
        self.assertEqual([w.norm for w in words], ["anybody", "here"])
        self.assertAlmostEqual(words[0].start_s, 0.1)
        self.assertAlmostEqual(words[0].end_s, 0.6)
        self.assertAlmostEqual(words[1].end_s, 0.9)

    def test_control_tokens_are_word_boundaries(self):
        # "[_TT_750]" glued between words must not fuse "up" + "man".
        segments = [
            _segment(
                [
                    _token(" coming", 0, 200),
                    _token(" up", 200, 400),
                    _token("[_TT_750]", 400, 400),
                    _token("man", 400, 700),
                ]
            )
        ]
        words = PROTO.build_word_stream(segments)
        self.assertEqual([w.norm for w in words], ["coming", "up", "man"])

    def test_words_do_not_span_segments(self):
        segments = [
            _segment([_token(" hel", 0, 200)]),
            _segment([_token("lo", 200, 400)]),
        ]
        words = PROTO.build_word_stream(segments)
        self.assertEqual([w.norm for w in words], ["hel", "lo"])


class TitleVariantTests(unittest.TestCase):
    def test_question_mark_split_and_the_stripping(self):
        variants = PROTO.title_variants(
            "Why Is This Happening? The Chris Hayes Podcast"
        )
        self.assertIn("Why Is This Happening", variants)
        self.assertIn("The Chris Hayes Podcast", variants)
        self.assertIn("Chris Hayes Podcast", variants)

    def test_leading_the_stripped(self):
        self.assertIn(
            "Nikki Glaser Podcast",
            PROTO.title_variants("The Nikki Glaser Podcast"),
        )


class TemplateCompilationTests(unittest.TestCase):
    def test_station_cross_product_for_public_radio_show(self):
        phrases = PROTO.compile_show_phrases(
            "on-the-media", ["On The Media"], ["On The Media"]
        )
        labels = {p.label for p in phrases}
        self.assertIn("WNYC is supported by", labels)
        self.assertIn("WNYC Studios is brought to you by", labels)
        self.assertIn("On The Media is sponsored by", labels)
        self.assertIn("support for On The Media comes from", labels)
        self.assertTrue(all(p.family == "a" for p in phrases))

    def test_non_public_radio_show_has_no_station_templates(self):
        phrases = PROTO.compile_show_phrases("smartless", ["SmartLess"], [])
        self.assertFalse(any("WNYC" in p.label or "NPR" in p.label for p in phrases))

    def test_no_duplicate_word_sequences(self):
        phrases = PROTO.compile_show_phrases(
            "on-the-media", ["On The Media"], ["On the media"]
        )
        seqs = [p.words for p in phrases]
        self.assertEqual(len(seqs), len(set(seqs)))

    def test_generic_phrases_cover_both_subfamilies(self):
        families = {p.family for p in PROTO.generic_phrases()}
        self.assertEqual(families, {"b-pre", "b-resume"})


class MatcherTests(unittest.TestCase):
    def _phrase(self, family, text):
        return PROTO.Phrase(family, text, PROTO.normalize_phrase(text))

    def test_exact_match_with_timestamps(self):
        words = PROTO.build_word_stream(
            _words_from_sentence("and now we will be right back folks", 10_000)
        )
        matches, near = PROTO.find_phrase_matches(
            words, self._phrase("b-pre", "we will be right back")
        )
        self.assertEqual(len(matches), 1)
        self.assertTrue(matches[0].exact)
        self.assertAlmostEqual(matches[0].start_s, 10.8)  # third word
        self.assertEqual(near, [])

    def test_contraction_normalises_to_exact(self):
        # ASR "well be right back" == normalised "we'll be right back".
        words = PROTO.build_word_stream(
            _words_from_sentence("i think well be right back everyone")
        )
        matches, _ = PROTO.find_phrase_matches(
            words, self._phrase("b-pre", "we'll be right back")
        )
        self.assertEqual(len(matches), 1)
        self.assertTrue(matches[0].exact)

    def test_condensed_fuzzy_handles_asr_word_splits(self):
        # "smart less" (split) must match "SmartLess is sponsored by".
        words = PROTO.build_word_stream(
            _words_from_sentence("smart less is sponsored by acme")
        )
        matches, _ = PROTO.find_phrase_matches(
            words, self._phrase("a", "SmartLess is sponsored by")
        )
        self.assertEqual(len(matches), 1)
        self.assertGreaterEqual(matches[0].ratio, 0.99)

    def test_near_miss_recorded_not_matched(self):
        words = PROTO.build_word_stream(
            _words_from_sentence("more on that when we get back")
        )
        matches, near = PROTO.find_phrase_matches(
            words, self._phrase("b-pre", "when we come back")
        )
        self.assertEqual(matches, [])
        self.assertEqual(len(near), 1)
        self.assertLess(near[0].ratio, PROTO.FUZZY_RATIO_MIN)
        self.assertGreaterEqual(near[0].ratio, PROTO.NEAR_MISS_FLOOR)

    def test_unrelated_text_produces_nothing(self):
        words = PROTO.build_word_stream(
            _words_from_sentence("the weather in tulsa remains sunny all week")
        )
        matches, near = PROTO.find_phrase_matches(
            words, self._phrase("b-pre", "we will be right back")
        )
        self.assertEqual(matches, [])
        self.assertEqual(near, [])

    def test_single_word_phrase_rejected(self):
        words = PROTO.build_word_stream(_words_from_sentence("back"))
        with self.assertRaises(ValueError):
            PROTO.find_phrase_matches(words, self._phrase("b-pre", "back"))

    def test_dedupe_prefers_exact_over_longer_fuzzy(self):
        words = PROTO.build_word_stream(
            _words_from_sentence("okay and back to the show now")
        )
        exact_short, _ = PROTO.find_phrase_matches(
            words, self._phrase("b-resume", "and back to the show")
        )
        fuzzy_long, _ = PROTO.find_phrase_matches(
            words, self._phrase("b-resume", "and now back to the show")
        )
        self.assertTrue(exact_short and exact_short[0].exact)
        self.assertTrue(fuzzy_long and not fuzzy_long[0].exact)
        kept = PROTO.dedupe_matches(exact_short + fuzzy_long)
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0].phrase.label, "and back to the show")

    def test_dedupe_merges_zero_width_degenerate_spans(self):
        # Corpus word timestamps can be degenerate (start == end); touching
        # zero-width matches at the same instant must still merge.
        phrase = self._phrase("a", "on the media is supported by")
        a = PROTO.Match(phrase, 1114.84, 1114.84, 1.0, True, "x")
        b = PROTO.Match(
            self._phrase("a", "on the media is sponsored by"),
            1114.84,
            1114.84,
            0.87,
            False,
            "x",
        )
        self.assertEqual(len(PROTO.dedupe_matches([a, b])), 1)


class ClassificationTests(unittest.TestCase):
    def _match(self, family, label, t):
        phrase = PROTO.Phrase(family, label, PROTO.normalize_phrase(label))
        return PROTO.Match(phrase, t, t + 2.0, 1.0, True, label)

    def _classify(self, matches, breaks, vetoes=()):
        return PROTO.classify_matches(
            matches, breaks, list(vetoes), window_s=15.0, clearance_s=30.0
        )

    def test_onset_hit_delta_vs_break_start(self):
        scored = self._classify(
            [self._match("a", "acme is sponsored by", 101.0)], [_break(100.0, 160.0)]
        )
        self.assertEqual(scored[0].classification, "onset-hit")
        self.assertEqual(scored[0].break_index, 0)
        self.assertAlmostEqual(scored[0].delta_s, 1.0)

    def test_pre_phrase_slightly_before_start_is_onset_hit(self):
        scored = self._classify(
            [self._match("b-pre", "we will be right back", 97.0)],
            [_break(100.0, 160.0)],
        )
        self.assertEqual(scored[0].classification, "onset-hit")
        self.assertAlmostEqual(scored[0].delta_s, -3.0)

    def test_resume_phrase_scored_against_break_end(self):
        scored = self._classify(
            [self._match("b-resume", "and now back to the show", 161.5)],
            [_break(100.0, 160.0)],
        )
        self.assertEqual(scored[0].classification, "resume-hit")
        self.assertAlmostEqual(scored[0].delta_s, 1.5)

    def test_mid_break_reattribution_is_in_break_not_onset(self):
        scored = self._classify(
            [self._match("a", "acme is sponsored by", 130.0)], [_break(100.0, 160.0)]
        )
        self.assertEqual(scored[0].classification, "in-break")

    def test_far_fire_is_false_and_mid_zone_is_edge_adjacent(self):
        far, mid = self._classify(
            [
                self._match("b-pre", "we will be right back", 500.0),
                self._match("b-pre", "we will be right back", 80.0),
            ],
            [_break(100.0, 160.0)],
        )
        self.assertEqual(far.classification, "false-fire")
        self.assertFalse(far.in_veto)
        self.assertEqual(mid.classification, "edge-adjacent")

    def test_veto_fire_is_definite_false(self):
        scored = self._classify(
            [self._match("b-resume", "welcome back", 210.0)],
            [_break(100.0, 160.0)],
            vetoes=[{"start_seconds": 205.0, "end_seconds": 220.0}],
        )
        self.assertEqual(scored[0].classification, "false-fire")
        self.assertTrue(scored[0].in_veto)


class CLIEndToEndTests(unittest.TestCase):
    """main() against an in-tmpdir corpus: one episode, one gold break,
    framing phrases planted at both edges."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = pathlib.Path(tmp.name)

        self.eid = "show-x-2026-01-01-ep1"
        evaluation = {
            "artifact_kind": "oracle_earaudit_gold_boundary_evaluation",
            "schema_version": 1,
            "assets": [
                {
                    "episode_id": self.eid,
                    "show_name": "Show X",
                    "duration_seconds": 600.0,
                    "audio_fingerprint": "sha256:0",
                    "full_breaks": [_break(100.0, 160.0)],
                    "presence_anchors": [],
                    "content_vetoes": [],
                }
            ],
            "summary": {},
            "label_semantics": {},
            "sources": {},
        }
        self.evaluation_path = self.tmp / "gold.json"
        self.evaluation_path.write_text(json.dumps(evaluation), encoding="utf-8")

        transcripts = self.tmp / "transcripts"
        transcripts.mkdir()
        segments = (
            _words_from_sentence("here is some totally normal chat", 0)
            + _words_from_sentence("and we will be right back", 97_000)
            + _words_from_sentence("show x is sponsored by acme corp", 101_000)
            + _words_from_sentence("and now back to the show", 160_500)
        )
        (transcripts / f"{self.eid}.json").write_text(
            json.dumps({"transcription": segments}), encoding="utf-8"
        )
        self.transcripts = transcripts

        self.manifest_path = self.tmp / "manifest.json"
        self.manifest_path.write_text(
            json.dumps(
                [
                    {
                        "episodeId": self.eid,
                        "show": "Show X",
                        "showSlug": "show-x",
                        "feedUrl": "https://feeds.example.com/x",
                    }
                ]
            ),
            encoding="utf-8",
        )

    def _run(self):
        report = self.tmp / "report.md"
        json_out = self.tmp / "report.json"
        rc = PROTO.main(
            [
                "--evaluation", str(self.evaluation_path),
                "--transcripts-dir", str(self.transcripts),
                "--snapshots-manifest", str(self.manifest_path),
                "--report-out", str(report),
                "--json-out", str(json_out),
            ]
        )
        self.assertEqual(rc, 0)
        return report.read_text(encoding="utf-8"), json.loads(
            json_out.read_text(encoding="utf-8")
        )

    def test_all_three_anchor_kinds_score_on_the_planted_break(self):
        report, payload = self._run()
        agg = payload["aggregate"]
        self.assertEqual(agg["total_breaks"], 1)
        self.assertEqual(agg["breaks_with_start_anchor"], 1)
        self.assertEqual(agg["breaks_with_any_anchor"], 1)
        show = agg["shows"]["Show X"]
        self.assertEqual(show["a_onset_hits"], 1)
        self.assertEqual(show["b_pre_hits"], 1)
        self.assertEqual(show["b_resume_hits"], 1)
        self.assertEqual(sum(show["false_fires"].values()), 0)
        self.assertIn("**1/1 gold breaks**", report)
        self.assertIn("Show X is sponsored by", report)

    def test_missing_transcript_is_skipped_not_fatal(self):
        (self.transcripts / f"{self.eid}.json").unlink()
        _, payload = self._run()
        self.assertEqual(payload["skipped_episodes"], [self.eid])
        self.assertEqual(payload["aggregate"]["total_breaks"], 0)


class ShowSlugTests(unittest.TestCase):
    def test_slug_derivation(self):
        self.assertEqual(
            PROTO.show_slug_from_episode_id("morbid-2026-05-21-title"), "morbid"
        )
        self.assertEqual(
            PROTO.show_slug_from_episode_id(
                "the-nikki-glaser-podcast-2025-02-27-513-food-noise"
            ),
            "the-nikki-glaser-podcast",
        )

    def test_slug_derivation_rejects_unparseable_ids(self):
        with self.assertRaises(ValueError):
            PROTO.show_slug_from_episode_id("no-date-here")


if __name__ == "__main__":
    sys.exit(unittest.main())
