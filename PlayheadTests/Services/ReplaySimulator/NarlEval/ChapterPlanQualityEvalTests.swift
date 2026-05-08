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
//     pins the resulting metric movement.
//   * ChapterPlanQualityEvalTopicMatcherTests — pin the topic-label
//     matcher in isolation (no plan, no golden set).
//   * ChapterPlanQualityEvalAggregationTests — single-episode
//     aggregation parity check.
//   * ChapterPlanQualityEvalEmptyPlanTests — pin the documented
//     zero-denominator contract for an empty plan against a
//     non-empty golden set.
//   * ChapterPlanQualityRunnerTests — runner happy-path + error
//     surfaces (cache miss, content-hash mismatch).
//   * ChapterPlanQualityEvalCodableTests — round-trip the report
//     through JSON to confirm the custom Codable conformance is
//     correct (bead 22 will persist reports).

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

        // Thresholds appear in the report verbatim.
        #expect(tight.thresholdsUsed.boundaryToleranceSeconds == 5.0)
        #expect(defaultTol.thresholdsUsed.boundaryToleranceSeconds == 15.0)
        #expect(loose.thresholdsUsed.boundaryToleranceSeconds == 25.0)
    }

    /// Same expected/observed strings, swept threshold. At threshold
    /// 0.4 a 0.5 overlap is `.match`; at threshold 0.6 the same
    /// 0.5 overlap is `.miss`.
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
    func nonFiniteCandidateStart_isSkippedFromMatching() {
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
                // Real candidate: matches the golden.
                makeInferredChapter(start: 605.0, end: nil, title: nil, disposition: .adBreak)
            ]
        )

        let report = ChapterPlanQualityEval().evaluate(pairs: [(plan, golden)])

        // Recall: 1/1 (the real candidate matches the golden).
        #expect(report.boundaryRecall.matched == 1)
        #expect(report.boundaryRecall.total == 1)
        // Precision: numerator counts candidates that found a match,
        // denominator is the input length. The NaN candidate is
        // counted in the denominator (it was emitted by the detector)
        // but never finds a match → 1 / 2.
        #expect(report.boundaryPrecision.matched == 1)
        #expect(report.boundaryPrecision.total == 2)
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
    func bothEmpty_allMetricsAreZero() {
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
        #expect(report.dispositionAccuracy == 0.0)
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
            .map { $0.0.deletingPathExtension().lastPathComponent }
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
    /// the bead's privacy constraint.
    @Test
    func episodeIdsAreUniqueAndSyntheticPrefixed() throws {
        let fixtures = try ChapterPlanGoldenSetLoader.allSyntheticFixtures()
        var seen = Set<String>()
        for (url, set) in fixtures {
            #expect(set.episodeId.hasPrefix("synthetic-"), "Non-synthetic episodeId in \(url.lastPathComponent): \(set.episodeId)")
            #expect(set.episodeContentHash.hasPrefix("synthetic-"), "Non-synthetic contentHash in \(url.lastPathComponent): \(set.episodeContentHash)")
            let inserted = seen.insert(set.episodeId).inserted
            #expect(inserted, "Duplicate episodeId across synthetic fixtures: \(set.episodeId)")
        }
    }
}
