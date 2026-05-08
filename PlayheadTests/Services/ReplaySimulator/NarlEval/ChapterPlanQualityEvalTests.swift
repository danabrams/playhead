// ChapterPlanQualityEvalTests.swift
// playhead-au2v.1.21: synthetic-fixture coverage for the
// `ChapterPlanQualityEval` evaluator. The fixtures live under
// `PlayheadTests/Fixtures/ChapterPlanGoldenSet/synthetic/` and are
// loaded via `#filePath` (not bundle resources) so they don't collide
// with other dated fixture sets in the resource-copy step.
//
// Suites in this file:
//   * ChapterPlanQualityEvalSyntheticTests — exact-report assertions
//     against each named synthetic fixture.
//   * ChapterPlanQualityEvalThresholdTests — sweeps the boundary
//     tolerance and topic-overlap minimum on the same fixture and
//     pins the resulting metric movement, plus initializer-clamp
//     and non-finite-input regression tests.
//   * ChapterPlanQualityEvalTopicMatcherTests — pin the topic-label
//     matcher in isolation (no plan, no golden set).
//   * ChapterPlanQualityEvalAggregationTests — single-episode
//     parity, multi-episode aggregation parity, and the documented
//     duplicate-episode-id contract.
//   * ChapterPlanQualityEvalEmptyPlanTests — pin the documented
//     zero-denominator contract for empty plan, empty golden, and
//     both-empty edges.
//   * ChapterPlanQualityRunnerTests — runner happy-path + error
//     surfaces (cache miss on `runFromCache`, content-hash mismatch
//     on `run(plan:golden:)`, custom-evaluator threading). The
//     "mismatched-hash via cache" scenario collapses to the direct
//     surface and is exercised against `run(plan:golden:)`.
//   * ChapterPlanQualityEvalCodableTests — round-trip the report
//     through JSON to confirm the custom Codable conformance is
//     correct, including the all-zero confusion path and
//     unknown-disposition decode-error guards.
//   * ChapterPlanGoldenSetDirectoryTests — fixture-directory hygiene
//     (every fixture decodes; required basenames present; ids and
//     content hashes are unique and synthetic-prefixed).

import Foundation
import Testing
@testable import Playhead

// MARK: - Plan synthesis helpers

/// Build a `ChapterEvidence` for plan synthesis. Every chapter is
/// `.inferred` (the chapter-generation phase is the only producer in
/// production); `qualityScore` is fixed at 0.9 — quality-score math
/// is not what these tests exercise.
private func makeInferredChapter(
    start: Double,
    end: Double?,
    title: String?,
    disposition: ChapterDisposition,
    quality: Float = 0.9
) -> ChapterEvidence {
    ChapterEvidence(
        startTime: start,
        endTime: end,
        title: title,
        source: .inferred,
        disposition: disposition,
        qualityScore: quality
    )
}

/// Build a `ChapterPlan` from a list of (start, end, title, disposition)
/// tuples. `episodeContentHash` is the fixture's content hash so the
/// runner's hash-mismatch guard is satisfied. `generatedAt` is fixed
/// to `Date(timeIntervalSince1970: 0)` so the plan is byte-deterministic.
private func makePlan(
    contentHash: String,
    chapters: [ChapterEvidence]
) -> ChapterPlan {
    ChapterPlan(
        episodeContentHash: contentHash,
        chapters: chapters,
        planConfidence: ChapterPlan.computePlanConfidence(chapters),
        generatedAt: Date(timeIntervalSince1970: 0)
    )
}

// MARK: - Synthetic-fixture suites

@Suite("ChapterPlanQualityEval / synthetic fixtures")
struct ChapterPlanQualityEvalSyntheticTests {

    // ---- Happy path: perfect recall, precision, disposition.
    @Test
    func happyPath_perfectMatch() throws {
        let golden = try ChapterPlanGoldenSetLoader.loadSynthetic(named: "synthetic-happy-path")
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                makeInferredChapter(start: 0.0,    end: 600.0,  title: "intro segment", disposition: .content),
                makeInferredChapter(start: 600.0,  end: 720.0,  title: nil,             disposition: .adBreak),
                makeInferredChapter(start: 720.0,  end: 1800.0, title: "main interview", disposition: .content),
                makeInferredChapter(start: 1800.0, end: 1920.0, title: nil,             disposition: .adBreak),
                makeInferredChapter(start: 1920.0, end: nil,    title: "wrap up",       disposition: .content)
            ]
        )

        let report = ChapterPlanQualityEval().evaluate(pairs: [(plan, golden)])

        // Boundary recall: 5/5.
        #expect(report.boundaryRecall.matched == 5)
        #expect(report.boundaryRecall.total == 5)
        #expect(report.boundaryRecall.fraction == 1.0)
        // Boundary precision: 5/5.
        #expect(report.boundaryPrecision.matched == 5)
        #expect(report.boundaryPrecision.total == 5)
        #expect(report.boundaryPrecision.fraction == 1.0)
        // Disposition: 5/5.
        #expect(report.dispositionMatchedAgreed == 5)
        #expect(report.dispositionMatchedPairs == 5)
        #expect(report.dispositionAccuracy == 1.0)

        // Confusion: only diagonal entries should be non-zero.
        let confusion = report.perDispositionConfusion
        #expect(confusion[.content]?[.content] == 3)
        #expect(confusion[.adBreak]?[.adBreak] == 2)
        #expect(confusion[.ambiguous]?[.ambiguous] == 0)
        // Off-diagonals are all zero.
        #expect(confusion[.content]?[.adBreak] == 0)
        #expect(confusion[.adBreak]?[.content] == 0)

        // Topic labels: 3 chapters have a non-nil expected label and
        // matching plan title; 2 have nil expected label. Matcher
        // should report 3 matches and 2 NA.
        #expect(report.topicLabelMatches.matched == 3)
        #expect(report.topicLabelMatches.mismatched == 0)
        #expect(report.topicLabelMatches.notApplicable == 2)

        // Per-episode entry exists and aggregates parity-match.
        let perEp = try #require(report.perEpisode["synthetic-happy-path"])
        #expect(perEp.boundaryRecall == report.boundaryRecall)
        #expect(perEp.boundaryPrecision == report.boundaryPrecision)
        #expect(perEp.missedBoundaries == 0)
        #expect(perEp.falsePositiveBoundaries == 0)
    }

    // ---- Partial recall: detector misses ONE ground-truth boundary.
    // Plan emits 4 candidates (intro, ad1, segA, ad2) and is missing
    // the second-segment boundary at 1620s. precision stays 1 because
    // every candidate aligns with a ground-truth.
    @Test
    func partialRecall_missesOneBoundary() throws {
        let golden = try ChapterPlanGoldenSetLoader.loadSynthetic(named: "synthetic-partial-recall")
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                makeInferredChapter(start: 0.0,    end: 500.0,  title: "intro segment",  disposition: .content),
                makeInferredChapter(start: 500.0,  end: 620.0,  title: nil,              disposition: .adBreak),
                makeInferredChapter(start: 620.0,  end: 1500.0, title: "first segment",  disposition: .content),
                makeInferredChapter(start: 1500.0, end: nil,    title: nil,              disposition: .adBreak)
                // missing the 1620s boundary
            ]
        )

        let report = ChapterPlanQualityEval().evaluate(pairs: [(plan, golden)])

        // Recall: 4/5.
        #expect(report.boundaryRecall.matched == 4)
        #expect(report.boundaryRecall.total == 5)
        #expect(report.boundaryRecall.fraction == 0.8)
        // Precision: 4/4 — every candidate aligns.
        #expect(report.boundaryPrecision.matched == 4)
        #expect(report.boundaryPrecision.total == 4)
        #expect(report.boundaryPrecision.fraction == 1.0)
        // Disposition: 4 matched pairs, all agree.
        #expect(report.dispositionMatchedAgreed == 4)
        #expect(report.dispositionMatchedPairs == 4)
        #expect(report.dispositionAccuracy == 1.0)

        let perEp = try #require(report.perEpisode["synthetic-partial-recall"])
        #expect(perEp.missedBoundaries == 1)
        #expect(perEp.falsePositiveBoundaries == 0)
    }

    // ---- Disposition confusion: boundaries align, but the labeler
    // swaps adBreak <-> content on a single chapter. This is the case
    // the bead spec calls out by name.
    @Test
    func dispositionConfusion_singleSwap() throws {
        let golden = try ChapterPlanGoldenSetLoader.loadSynthetic(named: "synthetic-disposition-confusion")
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                makeInferredChapter(start: 0.0,    end: 600.0,  title: "intro segment",  disposition: .content),
                // golden says adBreak — labeler emits content (a swap).
                makeInferredChapter(start: 600.0,  end: 720.0,  title: nil,              disposition: .content),
                makeInferredChapter(start: 720.0,  end: 1800.0, title: "main interview", disposition: .content),
                makeInferredChapter(start: 1800.0, end: nil,    title: nil,              disposition: .adBreak)
            ]
        )

        let report = ChapterPlanQualityEval().evaluate(pairs: [(plan, golden)])

        // Recall + precision = 1.
        #expect(report.boundaryRecall.fraction == 1.0)
        #expect(report.boundaryPrecision.fraction == 1.0)
        // Disposition accuracy: 3/4 (one swap).
        #expect(report.dispositionMatchedAgreed == 3)
        #expect(report.dispositionMatchedPairs == 4)
        #expect(report.dispositionAccuracy == 0.75)

        // Pin the FULL confusion matrix so an unintended cell shift
        // surfaces here, not just on the cells we name.
        // Layout:
        //   golden=content, plan=content: 2 (chapters 1 and 3).
        //   golden=adBreak, plan=content: 1 (the swap on chapter 2).
        //   golden=adBreak, plan=adBreak: 1 (chapter 4).
        //   every other cell: 0.
        let expectedConfusion: [ChapterDisposition: [ChapterDisposition: Int]] = [
            .content:   [.content: 2, .adBreak: 0, .ambiguous: 0],
            .adBreak:   [.content: 1, .adBreak: 1, .ambiguous: 0],
            .ambiguous: [.content: 0, .adBreak: 0, .ambiguous: 0]
        ]
        #expect(report.perDispositionConfusion == expectedConfusion)

        // Per-episode confusion equals aggregate (single-episode).
        let perEp = try #require(report.perEpisode["synthetic-disposition-confusion"])
        #expect(perEp.perDispositionConfusion == expectedConfusion)
    }

    // ---- Topic-label miss: boundaries + dispositions all match, but
    // the plan's title for one chapter has no keyword overlap with
    // the expected topic label.
    @Test
    func topicLabel_overlapMissOnOneChapter() throws {
        let golden = try ChapterPlanGoldenSetLoader.loadSynthetic(named: "synthetic-topic-mismatch")
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                // expected "intro segment" / observed "intro" — overlap = 1/2 = 0.5 (matches at the default 0.5 threshold).
                makeInferredChapter(start: 0.0,    end: 600.0,  title: "intro",        disposition: .content),
                // expected "deep dive interview" / observed "weekly news roundup" — overlap = 0.
                makeInferredChapter(start: 600.0,  end: 1200.0, title: "weekly news roundup", disposition: .content),
                // expected "listener questions" / observed "listener questions" — overlap = 1.
                makeInferredChapter(start: 1200.0, end: nil,    title: "listener questions", disposition: .content)
            ]
        )

        let report = ChapterPlanQualityEval().evaluate(pairs: [(plan, golden)])

        // Boundary metrics: perfect.
        #expect(report.boundaryRecall.fraction == 1.0)
        #expect(report.boundaryPrecision.fraction == 1.0)
        // Disposition: all match.
        #expect(report.dispositionAccuracy == 1.0)
        // Topic matcher: 2 matches (intro, listener) + 1 mismatch (deep dive vs weekly news).
        #expect(report.topicLabelMatches.matched == 2)
        #expect(report.topicLabelMatches.mismatched == 1)
        #expect(report.topicLabelMatches.notApplicable == 0)

        // Per-episode parity: the single-episode aggregate IS the
        // per-episode entry. Pin the topic counts explicitly so a
        // regression that aggregates topic counts at the report
        // level but skips them per-episode (or vice versa) surfaces
        // here.
        let perEp = try #require(report.perEpisode["synthetic-topic-mismatch"])
        #expect(perEp.topicLabelMatches == report.topicLabelMatches)
    }
}

// MARK: - Threshold sensitivity

@Suite("ChapterPlanQualityEval / threshold sensitivity")
struct ChapterPlanQualityEvalThresholdTests {

    /// Goldens at t=600, t=620; plan candidates at t=603 (close to
    /// the t=600 golden) and t=700 (far from any golden). Sweeping
    /// tolerance changes recall and precision in opposite directions:
    ///
    ///   tol = 5s:
    ///     recall:    gold@600 has cand@603 within ±5 → matched.
    ///                gold@620 has nothing within ±5 → missed.
    ///                → 1/2 = 0.5
    ///     precision: cand@603 has gold@600 within ±5 → matched.
    ///                cand@700 has nothing within ±5 → false positive.
    ///                → 1/2 = 0.5
    ///
    ///   tol = 15s (default):
    ///     recall:    gold@600 → cand@603 (Δ=3) ✓; gold@620 → cand@603
    ///                (Δ=17) ✗. → 1/2 = 0.5
    ///     precision: cand@603 → gold@600 (Δ=3) ✓; cand@700 → gold@600
    ///                (Δ=100), gold@620 (Δ=80) ✗. → 1/2 = 0.5
    ///
    ///   tol = 25s (loose):
    ///     recall:    gold@600 → cand@603 (Δ=3) ✓; gold@620 → cand@603
    ///                (Δ=17, ≤25) ✓. → 2/2 = 1.0
    ///     precision: cand@603 still matches gold@600. cand@700 still
    ///                a false positive (Δ to gold@600=100, gold@620=80).
    ///                → 1/2 = 0.5
    ///
    /// The test pins three points along the sweep so any change to
    /// the matcher's tolerance handling shows up here.
    @Test
    func boundaryTolerance_sweepsRecallAndPrecisionPredictably() {
        let golden = GoldenChapterSet(
            episodeId: "synthetic-tolerance",
            episodeContentHash: "synthetic-tolerance-hash",
            chapters: [
                GoldenChapter(startTimeSeconds: 600.0, expectedDisposition: .adBreak, expectedTopicLabel: nil),
                GoldenChapter(startTimeSeconds: 620.0, expectedDisposition: .content, expectedTopicLabel: nil)
            ],
            notes: nil
        )
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                makeInferredChapter(start: 603.0, end: 700.0, title: nil, disposition: .adBreak),
                makeInferredChapter(start: 700.0, end: nil,   title: nil, disposition: .content)
            ]
        )

        // Tight (5s): recall 1/2, precision 1/2.
        let tight = ChapterPlanQualityEval(boundaryToleranceSeconds: 5.0)
            .evaluate(pairs: [(plan, golden)])
        #expect(tight.boundaryRecall.matched == 1)
        #expect(tight.boundaryRecall.total == 2)
        #expect(tight.boundaryRecall.fraction == 0.5)
        #expect(tight.boundaryPrecision.matched == 1)
        #expect(tight.boundaryPrecision.total == 2)
        #expect(tight.boundaryPrecision.fraction == 0.5)

        // Default (15s): recall 1/2 (gold@620 still uncovered), precision 1/2.
        let defaultTol = ChapterPlanQualityEval()
            .evaluate(pairs: [(plan, golden)])
        #expect(defaultTol.boundaryRecall.matched == 1)
        #expect(defaultTol.boundaryRecall.total == 2)
        #expect(defaultTol.boundaryPrecision.matched == 1)
        #expect(defaultTol.boundaryPrecision.total == 2)

        // Loose (25s): recall jumps to 2/2; precision stays 1/2 because
        // the t=700 candidate is still a false positive.
        let loose = ChapterPlanQualityEval(boundaryToleranceSeconds: 25.0)
            .evaluate(pairs: [(plan, golden)])
        #expect(loose.boundaryRecall.matched == 2)
        #expect(loose.boundaryRecall.total == 2)
        #expect(loose.boundaryRecall.fraction == 1.0)
        #expect(loose.boundaryPrecision.matched == 1)
        #expect(loose.boundaryPrecision.total == 2)
        #expect(loose.boundaryPrecision.fraction == 0.5)

        // Greedy disposition pairing pin (R2): at the loose tolerance
        // both goldens claim cand@603 as a candidate, but the greedy
        // matcher consumes each candidate at most once. The closest
        // pair (gold@600 ↔ cand@603, Δ=3) wins; gold@620 finds no
        // remaining in-tolerance candidate. So even though boundary
        // recall counts 2/2, dispositionMatchedPairs is exactly 1.
        // Without this pin a regression that swapped greedy for "match
        // each golden independently" would silently change semantics.
        #expect(loose.dispositionMatchedPairs == 1)
        #expect(loose.dispositionMatchedAgreed == 1) // gold@600.adBreak == cand@603.adBreak

        // Thresholds appear in the report verbatim.
        #expect(tight.thresholdsUsed.boundaryToleranceSeconds == 5.0)
        #expect(defaultTol.thresholdsUsed.boundaryToleranceSeconds == 15.0)
        #expect(loose.thresholdsUsed.boundaryToleranceSeconds == 25.0)
    }

    /// Same expected/observed strings, swept threshold. The token
    /// sets `{deep, dive}` and `{deep, interview}` produce a Jaccard
    /// overlap of 1/3 (~0.333). At threshold 0.3 that overlap is a
    /// `.match`; at threshold 0.5 the same overlap is a `.miss`. We
    /// also pin the degenerate threshold = 0 case (any overlap,
    /// including zero, matches) to lock the `>=` boundary.
    @Test
    func topicOverlap_thresholdAppliedAtBoundary() {
        // expected = {"deep", "dive"}, observed = {"deep", "interview"}
        // intersection = {"deep"} (size 1), union = {"deep","dive","interview"} (size 3)
        // overlap = 1/3 ~= 0.3333.
        let expected = "deep dive"
        let observed = "deep interview"

        // At threshold 0.3: 0.333 >= 0.3 → match.
        #expect(
            ChapterPlanQualityEval.topicLabelOutcome(
                expected: expected,
                observed: observed,
                threshold: 0.3
            ) == .match
        )
        // At threshold 0.5: 0.333 < 0.5 → miss.
        #expect(
            ChapterPlanQualityEval.topicLabelOutcome(
                expected: expected,
                observed: observed,
                threshold: 0.5
            ) == .miss
        )
        // At threshold 0.0: any overlap matches (degenerate).
        #expect(
            ChapterPlanQualityEval.topicLabelOutcome(
                expected: expected,
                observed: observed,
                threshold: 0.0
            ) == .match
        )

        // At threshold 0.0 with DISJOINT token sets: overlap = 0/N = 0,
        // and 0 >= 0 → match. The contract is "threshold met"; a zero
        // threshold is documented as degenerate ("any overlap, even
        // zero, is a match"). Pin this so a future regression that
        // adds `>` instead of `>=` somewhere in the matcher surfaces
        // here rather than silently flipping the boundary.
        #expect(
            ChapterPlanQualityEval.topicLabelOutcome(
                expected: "weather report",
                observed: "music recommendations",
                threshold: 0.0
            ) == .match
        )
    }

    /// Tolerance == 0 means "exact-match boundaries only". A
    /// candidate at exactly the same `Double` value as the golden
    /// boundary still matches; anything else does not.
    @Test
    func boundaryTolerance_zeroIsExactMatchOnly() {
        let golden = GoldenChapterSet(
            episodeId: "synthetic-zero-tolerance",
            episodeContentHash: "synthetic-zero-tolerance-hash",
            chapters: [
                GoldenChapter(startTimeSeconds: 600.0, expectedDisposition: .adBreak, expectedTopicLabel: nil),
                GoldenChapter(startTimeSeconds: 720.0, expectedDisposition: .content, expectedTopicLabel: nil)
            ],
            notes: nil
        )
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                // Exact-match candidate.
                makeInferredChapter(start: 600.0, end: 720.0, title: nil, disposition: .adBreak),
                // 1ms off — within ±15 default but NOT within ±0.
                makeInferredChapter(start: 720.001, end: nil, title: nil, disposition: .content)
            ]
        )

        let exact = ChapterPlanQualityEval(boundaryToleranceSeconds: 0.0)
            .evaluate(pairs: [(plan, golden)])

        // Recall: only the t=600 golden has an exact-matching candidate.
        #expect(exact.boundaryRecall.matched == 1)
        #expect(exact.boundaryRecall.total == 2)
        // Precision: only the t=600 candidate has an exact-matching
        // golden; t=720.001 is a false positive.
        #expect(exact.boundaryPrecision.matched == 1)
        #expect(exact.boundaryPrecision.total == 2)

        // Disposition accuracy is over the single matched pair, which
        // agrees: golden adBreak / plan adBreak.
        #expect(exact.dispositionMatchedAgreed == 1)
        #expect(exact.dispositionMatchedPairs == 1)
    }

    /// Negative or non-finite tolerance is clamped to 0 by the
    /// initializer (defensive guard). A negative tolerance must NOT
    /// silently match nothing or match everything — it must equal
    /// the documented exact-match behavior.
    @Test
    func initializer_clampsNegativeAndNonFiniteTolerance() {
        let negative = ChapterPlanQualityEval(boundaryToleranceSeconds: -10.0)
        #expect(negative.thresholds.boundaryToleranceSeconds == 0.0)

        let nan = ChapterPlanQualityEval(boundaryToleranceSeconds: .nan)
        #expect(nan.thresholds.boundaryToleranceSeconds == 0.0)

        let infinity = ChapterPlanQualityEval(boundaryToleranceSeconds: .infinity)
        // Positive infinity is finite-checked: `.infinity.isFinite` is
        // false, so the guard clamps it to 0. Documented behavior:
        // we treat any non-finite tolerance as a misconfiguration.
        #expect(infinity.thresholds.boundaryToleranceSeconds == 0.0)

        // Topic-overlap minimum: clamp into [0, 1], non-finite falls
        // back to the default 0.5.
        let overOne = ChapterPlanQualityEval(topicOverlapMinimum: 1.5)
        #expect(overOne.thresholds.topicOverlapMinimum == 1.0)

        let negativeOverlap = ChapterPlanQualityEval(topicOverlapMinimum: -0.25)
        #expect(negativeOverlap.thresholds.topicOverlapMinimum == 0.0)

        let nanOverlap = ChapterPlanQualityEval(topicOverlapMinimum: .nan)
        #expect(nanOverlap.thresholds.topicOverlapMinimum == ChapterPlanQualityEval.defaultTopicOverlapMinimum)
    }

    /// A chapter with a non-finite (NaN/Inf) `startTime` cannot match
    /// any boundary and must not poison aggregate metrics. The plan
    /// still contains the chapter (we don't filter from input), but
    /// the matcher's `isFinite` guards skip it.
    @Test
    func nonFiniteCandidateStart_isSkippedFromMatching() throws {
        let golden = GoldenChapterSet(
            episodeId: "synthetic-nonfinite",
            episodeContentHash: "synthetic-nonfinite-hash",
            chapters: [
                GoldenChapter(startTimeSeconds: 600.0, expectedDisposition: .adBreak, expectedTopicLabel: nil)
            ],
            notes: nil
        )
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                // NaN start: skipped by the matcher.
                makeInferredChapter(start: .nan, end: nil, title: nil, disposition: .adBreak),
                // +Infinity start: also skipped.
                makeInferredChapter(start: .infinity, end: nil, title: nil, disposition: .adBreak),
                // -Infinity start: also skipped (symmetric guard).
                makeInferredChapter(start: -.infinity, end: nil, title: nil, disposition: .adBreak),
                // Real candidate: matches the golden.
                makeInferredChapter(start: 605.0, end: nil, title: nil, disposition: .adBreak)
            ]
        )

        let report = ChapterPlanQualityEval().evaluate(pairs: [(plan, golden)])

        // Recall: 1/1 (the real candidate matches the golden).
        #expect(report.boundaryRecall.matched == 1)
        #expect(report.boundaryRecall.total == 1)
        // Precision: numerator counts candidates that found a match,
        // denominator is the input length. All three non-finite
        // candidates are counted in the denominator (they were emitted
        // by the detector) but never find a match → 1 / 4.
        #expect(report.boundaryPrecision.matched == 1)
        #expect(report.boundaryPrecision.total == 4)
        // Disposition: only the real candidate participates in pair
        // matching. NaN/+Inf/-Inf are filtered before pairing (R3
        // explicit-isFinite guard inside evaluateEpisode).
        #expect(report.dispositionMatchedPairs == 1)
        #expect(report.dispositionMatchedAgreed == 1)

        // Per-episode counts must be consistent with the aggregate:
        // 4 candidates, 1 matched → 3 false positives, 0 missed,
        // 1 disposition-matched pair that agrees (both .adBreak).
        let perEp = try #require(report.perEpisode["synthetic-nonfinite"])
        #expect(perEp.falsePositiveBoundaries == 3)
        #expect(perEp.missedBoundaries == 0)
        #expect(perEp.dispositionMatchedPairs == 1)
        #expect(perEp.dispositionMatchedAgreed == 1)
    }

    /// Symmetric edge: a GOLDEN with a non-finite `startTimeSeconds`
    /// is also skipped from matching. A malformed fixture (`.infinity`,
    /// `-.infinity`, or `.nan`) must not poison metrics.
    @Test
    func nonFiniteGoldenStart_isSkippedFromMatching() {
        let golden = GoldenChapterSet(
            episodeId: "synthetic-nonfinite-golden",
            episodeContentHash: "synthetic-nonfinite-golden-hash",
            chapters: [
                // NaN golden: skipped.
                GoldenChapter(startTimeSeconds: .nan, expectedDisposition: .adBreak, expectedTopicLabel: nil),
                // +Infinity golden: skipped.
                GoldenChapter(startTimeSeconds: .infinity, expectedDisposition: .adBreak, expectedTopicLabel: nil),
                // -Infinity golden: skipped.
                GoldenChapter(startTimeSeconds: -.infinity, expectedDisposition: .adBreak, expectedTopicLabel: nil),
                // Real golden: should match.
                GoldenChapter(startTimeSeconds: 600.0, expectedDisposition: .adBreak, expectedTopicLabel: nil)
            ],
            notes: nil
        )
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                makeInferredChapter(start: 605.0, end: nil, title: nil, disposition: .adBreak)
            ]
        )

        let report = ChapterPlanQualityEval().evaluate(pairs: [(plan, golden)])

        // Recall numerator: only the real golden can match. Denominator:
        // all 4 goldens (we don't filter from input — denominator is
        // the input length). So 1/4. Three non-finite goldens are
        // missed boundaries.
        #expect(report.boundaryRecall.matched == 1)
        #expect(report.boundaryRecall.total == 4)
        // Precision numerator: the only candidate finds the real
        // golden. Denominator: 1 candidate. So 1/1.
        #expect(report.boundaryPrecision.matched == 1)
        #expect(report.boundaryPrecision.total == 1)
        // Disposition: only the (real golden, real candidate) pair
        // participates. Both sides are `.adBreak`, so the pair
        // agrees — pin both numerator and denominator for symmetry
        // with `nonFiniteCandidateStart_isSkippedFromMatching`.
        #expect(report.dispositionMatchedPairs == 1)
        #expect(report.dispositionMatchedAgreed == 1)
    }
}

// MARK: - Topic matcher in isolation

@Suite("ChapterPlanQualityEval / topic-label matcher")
struct ChapterPlanQualityEvalTopicMatcherTests {

    @Test
    func tokenize_lowercasesAndSplitsOnNonAlphanumeric() {
        #expect(ChapterPlanQualityEval.tokenize("Deep Dive Interview") == ["deep", "dive", "interview"])
        #expect(ChapterPlanQualityEval.tokenize("Q&A: Listener Questions") == ["q", "a", "listener", "questions"])
        #expect(ChapterPlanQualityEval.tokenize("intro-segment") == ["intro", "segment"])
        #expect(ChapterPlanQualityEval.tokenize("") == [])
        #expect(ChapterPlanQualityEval.tokenize(nil) == [])
        // Whitespace-only input: tokenize yields the empty set
        // (alphanumeric filter strips every character).
        #expect(ChapterPlanQualityEval.tokenize("   ") == [])
    }

    @Test
    func outcome_eitherSideAbsentIsNotApplicable() {
        let t = ChapterPlanQualityEval.defaultTopicOverlapMinimum
        #expect(ChapterPlanQualityEval.topicLabelOutcome(expected: nil,         observed: "intro", threshold: t) == .notApplicable)
        #expect(ChapterPlanQualityEval.topicLabelOutcome(expected: "intro",     observed: nil,     threshold: t) == .notApplicable)
        #expect(ChapterPlanQualityEval.topicLabelOutcome(expected: "",          observed: "intro", threshold: t) == .notApplicable)
        #expect(ChapterPlanQualityEval.topicLabelOutcome(expected: "intro",     observed: "",      threshold: t) == .notApplicable)
        // Both sides whitespace-only → empty token sets → notApplicable.
        #expect(ChapterPlanQualityEval.topicLabelOutcome(expected: "   ",       observed: "intro", threshold: t) == .notApplicable)
    }

    @Test
    func outcome_identicalIsMatch() {
        #expect(
            ChapterPlanQualityEval.topicLabelOutcome(
                expected: "main interview",
                observed: "Main Interview",
                threshold: ChapterPlanQualityEval.defaultTopicOverlapMinimum
            ) == .match
        )
    }

    @Test
    func outcome_disjointIsMiss() {
        #expect(
            ChapterPlanQualityEval.topicLabelOutcome(
                expected: "weather report",
                observed: "music recommendations",
                threshold: ChapterPlanQualityEval.defaultTopicOverlapMinimum
            ) == .miss
        )
    }
}

// MARK: - Aggregation parity

@Suite("ChapterPlanQualityEval / aggregation")
struct ChapterPlanQualityEvalAggregationTests {

    /// On a single-episode input, the aggregate metrics in
    /// `ChapterPlanQualityReport` must equal the per-episode metrics.
    @Test
    func singleEpisode_aggregatesEqualPerEpisode() throws {
        let golden = try ChapterPlanGoldenSetLoader.loadSynthetic(named: "synthetic-disposition-confusion")
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                makeInferredChapter(start: 0.0,    end: 600.0,  title: "intro segment", disposition: .content),
                makeInferredChapter(start: 600.0,  end: 720.0,  title: nil,             disposition: .content),
                makeInferredChapter(start: 720.0,  end: 1800.0, title: "main interview", disposition: .content),
                makeInferredChapter(start: 1800.0, end: nil,    title: nil,             disposition: .adBreak)
            ]
        )

        let report = ChapterPlanQualityEval().evaluate(pairs: [(plan, golden)])
        let perEp = try #require(report.perEpisode["synthetic-disposition-confusion"])

        #expect(report.boundaryRecall == perEp.boundaryRecall)
        #expect(report.boundaryPrecision == perEp.boundaryPrecision)
        #expect(report.dispositionMatchedAgreed == perEp.dispositionMatchedAgreed)
        #expect(report.dispositionMatchedPairs == perEp.dispositionMatchedPairs)
        #expect(report.perDispositionConfusion == perEp.perDispositionConfusion)
        #expect(report.topicLabelMatches == perEp.topicLabelMatches)
    }

    /// Multi-episode aggregation: two distinct episodes, one perfect
    /// match and one with a single disposition swap. Aggregate counts
    /// must equal the SUM of per-episode counts (not just one of them).
    /// This pins the accumulators in `evaluate(pairs:)` against a
    /// regression that drops aggregation across episodes.
    @Test
    func multiEpisode_aggregatesAreSumOfPerEpisode() throws {
        let goldenA = try ChapterPlanGoldenSetLoader.loadSynthetic(named: "synthetic-happy-path")
        let planA = makePlan(
            contentHash: goldenA.episodeContentHash,
            chapters: [
                makeInferredChapter(start: 0.0,    end: 600.0,  title: "intro segment",   disposition: .content),
                makeInferredChapter(start: 600.0,  end: 720.0,  title: nil,               disposition: .adBreak),
                makeInferredChapter(start: 720.0,  end: 1800.0, title: "main interview",  disposition: .content),
                makeInferredChapter(start: 1800.0, end: 1920.0, title: nil,               disposition: .adBreak),
                makeInferredChapter(start: 1920.0, end: nil,    title: "wrap up",         disposition: .content)
            ]
        )
        let goldenB = try ChapterPlanGoldenSetLoader.loadSynthetic(named: "synthetic-disposition-confusion")
        let planB = makePlan(
            contentHash: goldenB.episodeContentHash,
            chapters: [
                makeInferredChapter(start: 0.0,    end: 600.0,  title: "intro segment",  disposition: .content),
                // Disposition swap (matches the disposition-confusion
                // fixture: golden says adBreak, plan says content).
                makeInferredChapter(start: 600.0,  end: 720.0,  title: nil,              disposition: .content),
                makeInferredChapter(start: 720.0,  end: 1800.0, title: "main interview", disposition: .content),
                makeInferredChapter(start: 1800.0, end: nil,    title: nil,              disposition: .adBreak)
            ]
        )

        let report = ChapterPlanQualityEval().evaluate(pairs: [(planA, goldenA), (planB, goldenB)])

        // Per-episode entries should both exist.
        let perA = try #require(report.perEpisode["synthetic-happy-path"])
        let perB = try #require(report.perEpisode["synthetic-disposition-confusion"])

        // Aggregate boundary counts equal the sum.
        #expect(report.boundaryRecall.matched == perA.boundaryRecall.matched + perB.boundaryRecall.matched)
        #expect(report.boundaryRecall.total == perA.boundaryRecall.total + perB.boundaryRecall.total)
        #expect(report.boundaryPrecision.matched == perA.boundaryPrecision.matched + perB.boundaryPrecision.matched)
        #expect(report.boundaryPrecision.total == perA.boundaryPrecision.total + perB.boundaryPrecision.total)

        // Aggregate disposition counts equal the sum.
        #expect(report.dispositionMatchedAgreed == perA.dispositionMatchedAgreed + perB.dispositionMatchedAgreed)
        #expect(report.dispositionMatchedPairs == perA.dispositionMatchedPairs + perB.dispositionMatchedPairs)

        // Aggregate topic counts equal the sum.
        #expect(report.topicLabelMatches.matched == perA.topicLabelMatches.matched + perB.topicLabelMatches.matched)
        #expect(report.topicLabelMatches.mismatched == perA.topicLabelMatches.mismatched + perB.topicLabelMatches.mismatched)
        #expect(report.topicLabelMatches.notApplicable == perA.topicLabelMatches.notApplicable + perB.topicLabelMatches.notApplicable)

        // Aggregate confusion is the cell-wise sum of per-episode
        // confusion. Spot-check the swap cell (golden=adBreak, plan=content):
        // 0 in A + 1 in B = 1 in aggregate.
        #expect(report.perDispositionConfusion[.adBreak]?[.content] == 1)
        #expect(report.perDispositionConfusion[.content]?[.content]
            == (perA.perDispositionConfusion[.content]?[.content] ?? 0)
             + (perB.perDispositionConfusion[.content]?[.content] ?? 0))
    }

    /// Documented duplicate-episode-id contract from `evaluate(pairs:)`:
    /// aggregates double-count across the duplicate pair while the
    /// `perEpisode` dictionary single-counts (later entry wins). The
    /// caller doc-comment documents this; without this regression test
    /// a future "dedupe in aggregates" change would silently alter the
    /// API contract.
    @Test
    func duplicateEpisodeId_aggregatesDoubleCount_perEpisodeSingleCount() throws {
        let golden = try ChapterPlanGoldenSetLoader.loadSynthetic(named: "synthetic-happy-path")
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                makeInferredChapter(start: 0.0,    end: 600.0,  title: "intro segment",  disposition: .content),
                makeInferredChapter(start: 600.0,  end: 720.0,  title: nil,              disposition: .adBreak),
                makeInferredChapter(start: 720.0,  end: 1800.0, title: "main interview", disposition: .content),
                makeInferredChapter(start: 1800.0, end: 1920.0, title: nil,              disposition: .adBreak),
                makeInferredChapter(start: 1920.0, end: nil,    title: "wrap up",        disposition: .content)
            ]
        )

        let report = ChapterPlanQualityEval().evaluate(pairs: [(plan, golden), (plan, golden)])

        // perEpisode contains exactly one entry (later overwrites earlier).
        #expect(report.perEpisode.count == 1)
        // Aggregates double-count: each happy-path pair contributes
        // 5 boundary matches and 5 matched pairs, so the aggregate
        // should be 10.
        #expect(report.boundaryRecall.matched == 10)
        #expect(report.boundaryRecall.total == 10)
        #expect(report.dispositionMatchedPairs == 10)
        #expect(report.dispositionMatchedAgreed == 10)
    }
}

// MARK: - Empty-plan contract

@Suite("ChapterPlanQualityEval / empty plan")
struct ChapterPlanQualityEvalEmptyPlanTests {

    /// When the plan has zero candidates and the golden set has N>0
    /// boundaries:
    ///   recall          = 0/N            (well-defined, fraction = 0.0)
    ///   precision       = 0/0 → 0.0      (zero-denominator contract)
    ///   disposition acc = 0/0 → 0.0      (zero-denominator contract)
    ///   confusion       = all-zero matrix
    ///   topic matches   = all-zero counts
    ///   missed = N, falsePositives = 0
    @Test
    func emptyPlan_nonEmptyGolden_matchesContract() throws {
        let golden = try ChapterPlanGoldenSetLoader.loadSynthetic(named: "synthetic-happy-path")
        let plan = makePlan(contentHash: golden.episodeContentHash, chapters: [])

        let report = ChapterPlanQualityEval().evaluate(pairs: [(plan, golden)])

        #expect(report.boundaryRecall.matched == 0)
        #expect(report.boundaryRecall.total == 5)
        #expect(report.boundaryRecall.fraction == 0.0)

        #expect(report.boundaryPrecision.matched == 0)
        #expect(report.boundaryPrecision.total == 0)
        #expect(report.boundaryPrecision.fraction == 0.0)

        #expect(report.dispositionMatchedAgreed == 0)
        #expect(report.dispositionMatchedPairs == 0)
        #expect(report.dispositionAccuracy == 0.0)

        let perEp = try #require(report.perEpisode["synthetic-happy-path"])
        #expect(perEp.missedBoundaries == 5)
        #expect(perEp.falsePositiveBoundaries == 0)

        // Confusion matrix is fully populated with zero counts.
        let confusion = report.perDispositionConfusion
        for expected in [ChapterDisposition.adBreak, .content, .ambiguous] {
            for observed in [ChapterDisposition.adBreak, .content, .ambiguous] {
                #expect(confusion[expected]?[observed] == 0)
            }
        }

        #expect(report.topicLabelMatches.matched == 0)
        #expect(report.topicLabelMatches.mismatched == 0)
        #expect(report.topicLabelMatches.notApplicable == 0)
    }

    /// Symmetric edge: empty golden set against a non-empty plan.
    /// Recall is 0/0 → 0.0; precision is 0/N — every candidate is a
    /// false positive.
    @Test
    func emptyGolden_nonEmptyPlan_matchesContract() throws {
        let golden = GoldenChapterSet(
            episodeId: "synthetic-empty-golden",
            episodeContentHash: "synthetic-empty-golden-hash",
            chapters: [],
            notes: nil
        )
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                makeInferredChapter(start: 0.0,   end: 600.0, title: "intro",   disposition: .content),
                makeInferredChapter(start: 600.0, end: nil,   title: nil,       disposition: .adBreak)
            ]
        )

        let report = ChapterPlanQualityEval().evaluate(pairs: [(plan, golden)])

        #expect(report.boundaryRecall.matched == 0)
        #expect(report.boundaryRecall.total == 0)
        #expect(report.boundaryRecall.fraction == 0.0)
        #expect(report.boundaryPrecision.matched == 0)
        #expect(report.boundaryPrecision.total == 2)
        #expect(report.boundaryPrecision.fraction == 0.0)

        let perEp = try #require(report.perEpisode["synthetic-empty-golden"])
        #expect(perEp.missedBoundaries == 0)
        #expect(perEp.falsePositiveBoundaries == 2)
    }

    /// Both empty: every metric collapses to 0/0 → 0.0.
    @Test
    func bothEmpty_allMetricsAreZero() throws {
        let golden = GoldenChapterSet(
            episodeId: "synthetic-vacuous",
            episodeContentHash: "synthetic-vacuous-hash",
            chapters: [],
            notes: nil
        )
        let plan = makePlan(contentHash: golden.episodeContentHash, chapters: [])

        let report = ChapterPlanQualityEval().evaluate(pairs: [(plan, golden)])

        #expect(report.boundaryRecall.matched == 0)
        #expect(report.boundaryRecall.total == 0)
        #expect(report.boundaryPrecision.matched == 0)
        #expect(report.boundaryPrecision.total == 0)
        // Aggregate disposition counts must be zero (the zero-pair
        // contract is what makes `dispositionAccuracy` resolve to
        // 0.0 instead of NaN). Pin both numerator and denominator
        // explicitly so a regression that, say, defaulted
        // `dispositionMatchedPairs` to 1 to dodge a divide-by-zero
        // would surface here rather than silently flipping the
        // accuracy field.
        #expect(report.dispositionMatchedPairs == 0)
        #expect(report.dispositionMatchedAgreed == 0)
        #expect(report.dispositionAccuracy == 0.0)

        // Per-episode entry must still be emitted even when both
        // sides are empty — downstream callers iterate `perEpisode`
        // to surface coverage. A vacuous episode has zero of every
        // count.
        let perEp = try #require(report.perEpisode["synthetic-vacuous"])
        #expect(perEp.missedBoundaries == 0)
        #expect(perEp.falsePositiveBoundaries == 0)
        #expect(perEp.dispositionMatchedPairs == 0)
        #expect(perEp.dispositionMatchedAgreed == 0)
    }
}

// MARK: - Runner

@Suite("ChapterPlanQualityRunner")
struct ChapterPlanQualityRunnerTests {

    /// Runner happy path with `run(plan:golden:)` (the synchronous
    /// surface that bead 22's tests will use directly).
    @Test
    func runDirect_returnsReport() throws {
        let golden = try ChapterPlanGoldenSetLoader.loadSynthetic(named: "synthetic-happy-path")
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                makeInferredChapter(start: 0.0,    end: 600.0,  title: "intro segment",   disposition: .content),
                makeInferredChapter(start: 600.0,  end: 720.0,  title: nil,               disposition: .adBreak),
                makeInferredChapter(start: 720.0,  end: 1800.0, title: "main interview",  disposition: .content),
                makeInferredChapter(start: 1800.0, end: 1920.0, title: nil,               disposition: .adBreak),
                makeInferredChapter(start: 1920.0, end: nil,    title: "wrap up",         disposition: .content)
            ]
        )

        let report = try ChapterPlanQualityRunner.run(plan: plan, golden: golden)
        #expect(report.boundaryRecall.fraction == 1.0)
        #expect(report.dispositionAccuracy == 1.0)
    }

    /// Mismatched content hash surfaces as `RunnerError.contentHashMismatch`.
    @Test
    func runDirect_contentHashMismatchThrows() {
        let golden = GoldenChapterSet(
            episodeId: "synthetic-mismatch",
            episodeContentHash: "golden-hash",
            chapters: [],
            notes: nil
        )
        let plan = makePlan(contentHash: "plan-hash", chapters: [])

        #expect(throws: ChapterPlanQualityRunner.RunnerError.contentHashMismatch(
            planHash: "plan-hash", goldenHash: "golden-hash"
        )) {
            _ = try ChapterPlanQualityRunner.run(plan: plan, golden: golden)
        }
    }

    /// `runFromCache` happy path: insert a plan into a temp-directory
    /// `ChapterPlanCache`, then read it back through the runner.
    @Test
    func runFromCache_happyPath() async throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "ChapterPlanQualityRunnerTests-\(UUID().uuidString)",
                isDirectory: true
            )
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let cache = ChapterPlanCache(directory: tempDir)
        let golden = try ChapterPlanGoldenSetLoader.loadSynthetic(named: "synthetic-happy-path")
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                makeInferredChapter(start: 0.0,    end: 600.0,  title: "intro segment",   disposition: .content),
                makeInferredChapter(start: 600.0,  end: 720.0,  title: nil,               disposition: .adBreak),
                makeInferredChapter(start: 720.0,  end: 1800.0, title: "main interview",  disposition: .content),
                makeInferredChapter(start: 1800.0, end: 1920.0, title: nil,               disposition: .adBreak),
                makeInferredChapter(start: 1920.0, end: nil,    title: "wrap up",         disposition: .content)
            ]
        )
        let putOK = await cache.put(contentHash: golden.episodeContentHash, plan: plan)
        #expect(putOK)

        let report = try await ChapterPlanQualityRunner.runFromCache(
            cache: cache,
            golden: golden
        )
        #expect(report.boundaryRecall.fraction == 1.0)
    }

    /// `runFromCache` with no plan in the cache: surfaces as
    /// `RunnerError.planNotInCache`.
    @Test
    func runFromCache_missingPlanThrows() async throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "ChapterPlanQualityRunnerTests-\(UUID().uuidString)",
                isDirectory: true
            )
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let cache = ChapterPlanCache(directory: tempDir)
        let golden = GoldenChapterSet(
            episodeId: "synthetic-missing",
            episodeContentHash: "synthetic-missing-hash",
            chapters: [],
            notes: nil
        )

        await #expect(throws: ChapterPlanQualityRunner.RunnerError.planNotInCache(
            contentHash: "synthetic-missing-hash"
        )) {
            _ = try await ChapterPlanQualityRunner.runFromCache(
                cache: cache,
                golden: golden
            )
        }
    }

    /// The runner accepts a custom `evaluator` (defaulting to one
    /// constructed with default thresholds). A non-default evaluator
    /// MUST be used by the runner — otherwise the `evaluator:` arg is
    /// dead code. Pin this by passing a tight-tolerance evaluator and
    /// observing the resulting threshold appears in the report.
    @Test
    func runDirect_respectsCustomEvaluatorThresholds() throws {
        let golden = GoldenChapterSet(
            episodeId: "synthetic-custom-evaluator",
            episodeContentHash: "synthetic-custom-evaluator-hash",
            chapters: [
                GoldenChapter(startTimeSeconds: 0.0, expectedDisposition: .content, expectedTopicLabel: nil),
                GoldenChapter(startTimeSeconds: 100.0, expectedDisposition: .adBreak, expectedTopicLabel: nil)
            ],
            notes: nil
        )
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                makeInferredChapter(start: 0.0,    end: 100.0, title: nil, disposition: .content),
                // 8s away from gold@100 — within default ±15 but NOT within ±5.
                makeInferredChapter(start: 108.0,  end: nil,   title: nil, disposition: .adBreak)
            ]
        )

        // Default (15s) → both goldens matched.
        let defaultReport = try ChapterPlanQualityRunner.run(plan: plan, golden: golden)
        #expect(defaultReport.boundaryRecall.matched == 2)
        #expect(defaultReport.thresholdsUsed.boundaryToleranceSeconds == 15.0)

        // Custom tight (5s) → only gold@0 matches; gold@100 is missed.
        let tight = ChapterPlanQualityEval(boundaryToleranceSeconds: 5.0)
        let tightReport = try ChapterPlanQualityRunner.run(plan: plan, golden: golden, evaluator: tight)
        #expect(tightReport.boundaryRecall.matched == 1)
        #expect(tightReport.thresholdsUsed.boundaryToleranceSeconds == 5.0)
    }

    /// `runFromCache` with a plan whose content hash matches the
    /// cache key but DIFFERS from the golden's. This can only happen
    /// if the runner's contract is violated upstream (the cache lookup
    /// is keyed by the golden's hash, so a hit implies parity), but we
    /// pin the defensive guard regardless: any future refactor that
    /// dropped the hash equality check on `run(plan:golden:)` would
    /// surface here.
    ///
    /// We bypass the cache lookup directly by calling the synchronous
    /// `run(plan:golden:)` surface — the cached + mismatched scenario
    /// collapses to the same code path. This test keeps the contract
    /// explicit alongside the cache-miss test above.
    @Test
    func runFromCache_mismatchedContentHashThrowsViaRunDirect() throws {
        // A plan and a golden carrying different content hashes. The
        // runner's hash-equality guard MUST throw rather than silently
        // returning a (meaningless) report computed across mismatched
        // episodes.
        let golden = GoldenChapterSet(
            episodeId: "synthetic-cross-hash",
            episodeContentHash: "synthetic-cross-hash-A",
            chapters: [
                GoldenChapter(startTimeSeconds: 0.0, expectedDisposition: .content, expectedTopicLabel: nil)
            ],
            notes: nil
        )
        let plan = makePlan(
            contentHash: "synthetic-cross-hash-B",
            chapters: [
                makeInferredChapter(start: 0.0, end: nil, title: nil, disposition: .content)
            ]
        )

        #expect(throws: ChapterPlanQualityRunner.RunnerError.contentHashMismatch(
            planHash: "synthetic-cross-hash-B",
            goldenHash: "synthetic-cross-hash-A"
        )) {
            _ = try ChapterPlanQualityRunner.run(plan: plan, golden: golden)
        }
    }
}

// MARK: - Codable round-trip

@Suite("ChapterPlanQualityReport / Codable round-trip")
struct ChapterPlanQualityEvalCodableTests {

    /// JSON encode-then-decode preserves every field, including the
    /// `[ChapterDisposition: [ChapterDisposition: Int]]` confusion
    /// matrix which uses a custom Codable conformance.
    @Test
    func report_jsonRoundTripIsLossless() throws {
        let golden = try ChapterPlanGoldenSetLoader.loadSynthetic(named: "synthetic-disposition-confusion")
        let plan = makePlan(
            contentHash: golden.episodeContentHash,
            chapters: [
                makeInferredChapter(start: 0.0,    end: 600.0,  title: "intro segment",  disposition: .content),
                makeInferredChapter(start: 600.0,  end: 720.0,  title: nil,              disposition: .content),
                makeInferredChapter(start: 720.0,  end: 1800.0, title: "main interview", disposition: .content),
                makeInferredChapter(start: 1800.0, end: nil,    title: nil,              disposition: .adBreak)
            ]
        )

        let report = ChapterPlanQualityEval().evaluate(pairs: [(plan, golden)])
        let data = try JSONEncoder().encode(report)
        let decoded = try JSONDecoder().decode(ChapterPlanQualityReport.self, from: data)

        #expect(decoded == report)
    }

    /// Round-trip an empty-plan report (every confusion-matrix cell is
    /// 0). The custom Codable conformance must survive the all-zero
    /// rows path: a decoder that drops zero-valued cells from the wire
    /// format would silently change `Equatable` comparisons here.
    @Test
    func report_jsonRoundTripIsLosslessForEmptyPlan() throws {
        let golden = try ChapterPlanGoldenSetLoader.loadSynthetic(named: "synthetic-happy-path")
        let plan = makePlan(contentHash: golden.episodeContentHash, chapters: [])

        let report = ChapterPlanQualityEval().evaluate(pairs: [(plan, golden)])
        let data = try JSONEncoder().encode(report)
        let decoded = try JSONDecoder().decode(ChapterPlanQualityReport.self, from: data)

        #expect(decoded == report)
        // Pin: the decoded matrix has every disposition row keyed,
        // each containing every disposition column with count 0.
        for expected in [ChapterDisposition.adBreak, .content, .ambiguous] {
            for observed in [ChapterDisposition.adBreak, .content, .ambiguous] {
                #expect(decoded.perDispositionConfusion[expected]?[observed] == 0)
            }
        }
    }

    /// A wire-format JSON containing an unknown `ChapterDisposition`
    /// raw value in the INNER (observed) position must throw
    /// `DecodingError.dataCorrupted` rather than silently dropping the
    /// cell or producing `nil`. This pins the inner-key guard inside
    /// `DispositionConfusionWire.init(from:)`.
    @Test
    func report_decodingFailsOnUnknownInnerDispositionRawValue() {
        // A minimal JSON shaped like a `ChapterPlanQualityReport`,
        // with the confusion matrix containing one valid outer key
        // and a single bogus inner key. Every other field carries a
        // well-formed but trivial value — the goal is to surface the
        // disposition guard in isolation.
        let json = """
        {
          "boundaryRecall": { "matched": 0, "total": 0 },
          "boundaryPrecision": { "matched": 0, "total": 0 },
          "dispositionMatchedAgreed": 0,
          "dispositionMatchedPairs": 0,
          "perDispositionConfusion": { "content": { "future_unknown_case": 1 } },
          "topicLabelMatches": { "matched": 0, "mismatched": 0, "notApplicable": 0 },
          "perEpisode": {},
          "thresholdsUsed": { "boundaryToleranceSeconds": 15.0, "topicOverlapMinimum": 0.5 }
        }
        """.data(using: .utf8)!

        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(ChapterPlanQualityReport.self, from: json)
        }
    }

    /// Symmetric guard: an unknown OUTER (expected) raw value must
    /// also throw `DecodingError.dataCorrupted`. The wire format
    /// validates both the outer and inner key paths — a regression
    /// that only validates one direction would silently accept a
    /// half-corrupt confusion matrix.
    @Test
    func report_decodingFailsOnUnknownOuterDispositionRawValue() {
        let json = """
        {
          "boundaryRecall": { "matched": 0, "total": 0 },
          "boundaryPrecision": { "matched": 0, "total": 0 },
          "dispositionMatchedAgreed": 0,
          "dispositionMatchedPairs": 0,
          "perDispositionConfusion": { "future_unknown_outer_case": { "content": 1 } },
          "topicLabelMatches": { "matched": 0, "mismatched": 0, "notApplicable": 0 },
          "perEpisode": {},
          "thresholdsUsed": { "boundaryToleranceSeconds": 15.0, "topicOverlapMinimum": 0.5 }
        }
        """.data(using: .utf8)!

        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(ChapterPlanQualityReport.self, from: json)
        }
    }
}

// MARK: - Synthetic-fixture directory hygiene

@Suite("ChapterPlanGoldenSet / fixture directory")
struct ChapterPlanGoldenSetDirectoryTests {

    /// Every JSON file in the synthetic directory must decode into a
    /// `GoldenChapterSet`. A new fixture that fails to decode breaks
    /// here with a precise filename.
    @Test
    func everySyntheticFixtureDecodes() throws {
        let fixtures = try ChapterPlanGoldenSetLoader.allSyntheticFixtures()
        #expect(!fixtures.isEmpty, "Expected at least one synthetic fixture")
    }

    /// At least the four named fixtures the bead spec calls out are
    /// present.
    @Test
    func requiredSyntheticFixturesPresent() throws {
        let fixtures = try ChapterPlanGoldenSetLoader.allSyntheticFixtures()
        let basenames = fixtures
            .map { $0.url.deletingPathExtension().lastPathComponent }
        let required = [
            "synthetic-happy-path",
            "synthetic-partial-recall",
            "synthetic-disposition-confusion",
            "synthetic-topic-mismatch"
        ]
        for name in required {
            #expect(basenames.contains(name), "Missing required synthetic fixture: \(name)")
        }
    }

    /// Episode ids must be unique and use the `synthetic-` prefix per
    /// the bead's privacy constraint. Episode content hashes must also
    /// be unique — two fixtures with identical hashes would silently
    /// collide in `ChapterPlanCache`/`ChapterPlanQualityRunner` lookups.
    @Test
    func episodeIdsAndContentHashesAreUniqueAndSyntheticPrefixed() throws {
        let fixtures = try ChapterPlanGoldenSetLoader.allSyntheticFixtures()
        var seenIds = Set<String>()
        var seenHashes = Set<String>()
        for (url, set) in fixtures {
            #expect(set.episodeId.hasPrefix("synthetic-"), "Non-synthetic episodeId in \(url.lastPathComponent): \(set.episodeId)")
            #expect(set.episodeContentHash.hasPrefix("synthetic-"), "Non-synthetic contentHash in \(url.lastPathComponent): \(set.episodeContentHash)")
            let idInserted = seenIds.insert(set.episodeId).inserted
            #expect(idInserted, "Duplicate episodeId across synthetic fixtures: \(set.episodeId)")
            let hashInserted = seenHashes.insert(set.episodeContentHash).inserted
            #expect(hashInserted, "Duplicate episodeContentHash across synthetic fixtures (would collide in cache lookups): \(set.episodeContentHash)")
        }
    }
}
