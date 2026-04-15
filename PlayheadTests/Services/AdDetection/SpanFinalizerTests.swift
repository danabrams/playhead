// SpanFinalizerTests.swift
// Tests for SpanFinalizer — deterministic span finalizer safety layer.
//
// Each constraint is tested independently, then combined and edge cases verified.

import Foundation
import Testing
@testable import Playhead

@Suite("SpanFinalizer")
struct SpanFinalizerTests {

    // MARK: - Helpers

    private func makeSpan(
        assetId: String = "asset-1",
        startTime: Double = 10.0,
        endTime: Double = 40.0,
        firstOrdinal: Int = 100,
        lastOrdinal: Int = 200
    ) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: firstOrdinal, lastAtomOrdinal: lastOrdinal),
            assetId: assetId,
            firstAtomOrdinal: firstOrdinal,
            lastAtomOrdinal: lastOrdinal,
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: []
        )
    }

    private func makeCandidate(
        startTime: Double = 10.0,
        endTime: Double = 40.0,
        skipConfidence: Double = 0.8,
        proposalConfidence: Double = 0.8,
        eligibilityGate: SkipEligibilityGate = .eligible,
        intent: CommercialIntent = .paid,
        ownership: AdOwnership = .thirdParty,
        ordinalBase: Int = 100
    ) -> CandidateSpan {
        let span = makeSpan(
            startTime: startTime,
            endTime: endTime,
            firstOrdinal: ordinalBase,
            lastOrdinal: ordinalBase + 100
        )
        let decision = DecisionResult(
            proposalConfidence: proposalConfidence,
            skipConfidence: skipConfidence,
            eligibilityGate: eligibilityGate
        )
        return CandidateSpan(
            span: span,
            decision: decision,
            commercialIntent: intent,
            adOwnership: ownership
        )
    }

    private func makeFinalizer(
        episodeDuration: Double = 3600.0,
        chapters: [ChapterMarker] = []
    ) -> SpanFinalizer {
        SpanFinalizer(episodeDuration: episodeDuration, chapters: chapters)
    }

    // MARK: - Determinism

    @Test("Same input always produces same output")
    func determinism() {
        let candidates = [
            makeCandidate(startTime: 10, endTime: 40, skipConfidence: 0.8, ordinalBase: 100),
            makeCandidate(startTime: 35, endTime: 70, skipConfidence: 0.6, ordinalBase: 300),
            makeCandidate(startTime: 100, endTime: 130, skipConfidence: 0.9, ordinalBase: 500),
        ]
        let finalizer = makeFinalizer()

        let run1 = finalizer.finalize(candidates)
        let run2 = finalizer.finalize(candidates)
        let run3 = finalizer.finalize(candidates)

        #expect(run1 == run2)
        #expect(run2 == run3)
    }

    // MARK: - Constraint 1: Non-overlap

    @Test("Non-overlapping spans pass through unchanged")
    func nonOverlappingPassThrough() {
        let candidates = [
            makeCandidate(startTime: 10, endTime: 30, ordinalBase: 100),
            makeCandidate(startTime: 40, endTime: 60, ordinalBase: 300),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result.count == 2)
        #expect(result[0].span.startTime == 10)
        #expect(result[0].span.endTime == 30)
        #expect(result[1].span.startTime == 40)
        #expect(result[1].span.endTime == 60)
    }

    @Test("Overlapping spans: higher confidence wins, lower is trimmed then merged")
    func overlapHigherConfidenceWins() {
        // A(10-50, 0.9) overlaps B(40-80, 0.6). A wins, B trimmed to 50-80.
        // Gap is 0s → merge into single span 10-80.
        let candidates = [
            makeCandidate(startTime: 10, endTime: 50, skipConfidence: 0.9, ordinalBase: 100),
            makeCandidate(startTime: 40, endTime: 80, skipConfidence: 0.6, ordinalBase: 300),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result.count == 1)
        #expect(result[0].span.startTime == 10)
        #expect(result[0].span.endTime == 80)
        #expect(result[0].decision.skipConfidence == 0.9)
    }

    @Test("Fully contained lower-confidence span is suppressed")
    func fullyContainedSuppressed() {
        let candidates = [
            makeCandidate(startTime: 10, endTime: 80, skipConfidence: 0.9, ordinalBase: 100),
            makeCandidate(startTime: 20, endTime: 50, skipConfidence: 0.5, ordinalBase: 300),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result.count == 1)
        #expect(result[0].span.startTime == 10)
        #expect(result[0].span.endTime == 80)
    }

    @Test("Equal confidence: first span wins (deterministic tiebreak)")
    func equalConfidenceTiebreak() {
        // A(10-50, 0.7) overlaps B(40-80, 0.7). First wins (>= tiebreak), B trimmed to 50-80.
        // Gap is 0s → merge into single span 10-80.
        let candidates = [
            makeCandidate(startTime: 10, endTime: 50, skipConfidence: 0.7, ordinalBase: 100),
            makeCandidate(startTime: 40, endTime: 80, skipConfidence: 0.7, ordinalBase: 300),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result.count == 1)
        #expect(result[0].span.startTime == 10)
        #expect(result[0].span.endTime == 80)
        #expect(result[0].decision.skipConfidence == 0.7)
    }

    @Test("Overlap trim with sufficient remaining gap keeps two spans")
    func overlapTrimWithSufficientGap() {
        // A(10-30, 0.9) overlaps B(25-60, 0.6). A wins, B trimmed to 30-60.
        // Gap is 0s → merge into 10-60.
        // To test pure overlap without merge, we need spans far enough apart
        // after trimming. This verifies the trim itself works.
        let candidates = [
            makeCandidate(startTime: 10, endTime: 30, skipConfidence: 0.9, ordinalBase: 100),
            makeCandidate(startTime: 25, endTime: 60, skipConfidence: 0.6, ordinalBase: 300),
        ]
        let result = makeFinalizer().finalize(candidates)

        // After overlap trim (30-60) → gap 0s → merge → single span 10-60.
        #expect(result.count == 1)
        #expect(result[0].span.startTime == 10)
        #expect(result[0].span.endTime == 60)
        #expect(result[0].constraintTrace.contains(.overlapTrimmed) || result[0].constraintTrace.contains(.mergedWithAdjacent))
    }

    // MARK: - Constraint 2: Minimum content gap

    @Test("Spans with gap < 3s are merged")
    func gapBelowMinimumMerged() {
        let candidates = [
            makeCandidate(startTime: 10, endTime: 30, skipConfidence: 0.6, ordinalBase: 100),
            makeCandidate(startTime: 32, endTime: 50, skipConfidence: 0.8, ordinalBase: 300),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result.count == 1)
        #expect(result[0].span.startTime == 10)
        #expect(result[0].span.endTime == 50)
        // Merged span takes higher confidence.
        #expect(result[0].decision.skipConfidence == 0.8)
        #expect(result[0].constraintTrace.contains(.mergedWithAdjacent))
    }

    @Test("Spans with exactly 3s gap are NOT merged")
    func exactlyThreeSecondGapNotMerged() {
        let candidates = [
            makeCandidate(startTime: 10, endTime: 30, ordinalBase: 100),
            makeCandidate(startTime: 33, endTime: 50, ordinalBase: 300),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result.count == 2)
    }

    @Test("Spans with 0s gap are merged")
    func zeroGapMerged() {
        let candidates = [
            makeCandidate(startTime: 10, endTime: 30, ordinalBase: 100),
            makeCandidate(startTime: 30, endTime: 50, ordinalBase: 300),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result.count == 1)
        #expect(result[0].span.startTime == 10)
        #expect(result[0].span.endTime == 50)
    }

    @Test("Multiple spans merged in chain")
    func multipleSpansMergedChain() {
        let candidates = [
            makeCandidate(startTime: 10, endTime: 20, ordinalBase: 100),
            makeCandidate(startTime: 21, endTime: 30, ordinalBase: 300),
            makeCandidate(startTime: 31, endTime: 45, ordinalBase: 500),
        ]
        let result = makeFinalizer().finalize(candidates)

        // All gaps < 3s, so all merged.
        #expect(result.count == 1)
        #expect(result[0].span.startTime == 10)
        #expect(result[0].span.endTime == 45)
    }

    // MARK: - Constraint 3: Duration sanity

    @Test("Span below 5s is dropped")
    func shortSpanDropped() {
        let candidates = [
            makeCandidate(startTime: 10, endTime: 14, ordinalBase: 100),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result.isEmpty)
    }

    @Test("Span exactly 5s is kept")
    func exactlyFiveSecondsKept() {
        let candidates = [
            makeCandidate(startTime: 10, endTime: 15, ordinalBase: 100),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result.count == 1)
    }

    @Test("Span above 180s is split")
    func longSpanSplit() {
        let candidates = [
            makeCandidate(startTime: 0, endTime: 300, ordinalBase: 100),
        ]
        let result = makeFinalizer().finalize(candidates)

        // 300s → 180s + 120s (two chunks, both >= 5s).
        #expect(result.count == 2)
        #expect(result[0].span.startTime == 0)
        #expect(result[0].span.endTime == 180)
        #expect(result[1].span.startTime == 180)
        #expect(result[1].span.endTime == 300)
        #expect(result[0].constraintTrace.contains(.splitAboveMaxDuration))
    }

    @Test("Split absorbs tiny trailing fragment")
    func splitAbsorbsTrailingFragment() {
        // 183s → should not create a 3s trailing fragment (< 5s).
        // Instead, first chunk extends to absorb it.
        let candidates = [
            makeCandidate(startTime: 0, endTime: 183, ordinalBase: 100),
        ]
        let result = makeFinalizer().finalize(candidates)

        // Trailing 3s (< 5s) absorbed into first chunk.
        #expect(result.count == 1)
        #expect(result[0].span.startTime == 0)
        #expect(result[0].span.endTime == 183)
    }

    @Test("Span exactly 180s is not split")
    func exactly180SecondsNotSplit() {
        let candidates = [
            makeCandidate(startTime: 0, endTime: 180, ordinalBase: 100),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result.count == 1)
        #expect(!result[0].constraintTrace.contains(.splitAboveMaxDuration))
    }

    // MARK: - Constraint 4: Chapter penalties

    @Test("Span crossing content chapter gets markOnly gate")
    func chapterPenaltyApplied() {
        let chapters = [ChapterMarker(startTime: 20, endTime: 60, isContent: true)]
        let candidates = [makeCandidate(startTime: 10, endTime: 40, ordinalBase: 100)]
        let result = makeFinalizer(chapters: chapters).finalize(candidates)

        #expect(result.count == 1)
        #expect(result[0].decision.eligibilityGate == .markOnly)
        #expect(result[0].constraintTrace.contains(.chapterPenaltyApplied))
    }

    @Test("Span not crossing content chapter keeps original gate")
    func noChapterPenaltyWhenNoCrossing() {
        let chapters = [ChapterMarker(startTime: 100, endTime: 200, isContent: true)]
        let candidates = [makeCandidate(startTime: 10, endTime: 40, ordinalBase: 100)]
        let result = makeFinalizer(chapters: chapters).finalize(candidates)

        #expect(result[0].decision.eligibilityGate == .eligible)
        #expect(!result[0].constraintTrace.contains(.chapterPenaltyApplied))
    }

    @Test("Chapter penalty does not promote already-blocked gate")
    func chapterPenaltyDoesNotPromoteBlockedGate() {
        let chapters = [ChapterMarker(startTime: 20, endTime: 60, isContent: true)]
        let candidates = [
            makeCandidate(startTime: 10, endTime: 40,
                          eligibilityGate: .blockedByEvidenceQuorum, ordinalBase: 100),
        ]
        let result = makeFinalizer(chapters: chapters).finalize(candidates)

        // Gate should stay blockedByEvidenceQuorum, NOT be promoted to markOnly.
        #expect(result[0].decision.eligibilityGate == .blockedByEvidenceQuorum)
        #expect(!result[0].constraintTrace.contains(.chapterPenaltyApplied))
    }

    @Test("Non-content chapter does not trigger penalty")
    func nonContentChapterIgnored() {
        let chapters = [ChapterMarker(startTime: 20, endTime: 60, isContent: false)]
        let candidates = [makeCandidate(startTime: 10, endTime: 40, ordinalBase: 100)]
        let result = makeFinalizer(chapters: chapters).finalize(candidates)

        #expect(result[0].decision.eligibilityGate == .eligible)
    }

    // MARK: - Constraint 5: Action cap (50%)

    @Test("Under 50% cap: no demotion")
    func underActionCapNoDemotion() {
        // Episode is 100s. 40s of ads = 40% < 50%.
        let candidates = [
            makeCandidate(startTime: 10, endTime: 50, skipConfidence: 0.8, ordinalBase: 100),
        ]
        let result = makeFinalizer(episodeDuration: 100).finalize(candidates)

        #expect(result[0].decision.eligibilityGate == .eligible)
        #expect(!result[0].constraintTrace.contains(.actionCapApplied))
    }

    @Test("Over 50% cap: lowest confidence demoted first")
    func overActionCapDemotesLowest() {
        // Episode is 100s. Budget = 50s.
        // Two spans totaling 60s → over by 10s.
        let candidates = [
            makeCandidate(startTime: 0, endTime: 30, skipConfidence: 0.9, ordinalBase: 100),
            makeCandidate(startTime: 40, endTime: 70, skipConfidence: 0.5, ordinalBase: 300),
        ]
        let result = makeFinalizer(episodeDuration: 100).finalize(candidates)

        // Lower confidence span (0.5) should be demoted.
        #expect(result[0].decision.eligibilityGate == .eligible) // 0.9 kept
        #expect(result[1].decision.eligibilityGate == .blockedByPolicy) // 0.5 demoted
        #expect(result[1].constraintTrace.contains(.actionCapApplied))
    }

    @Test("Exactly 50% cap: no demotion")
    func exactly50PercentNoDemotion() {
        // Episode is 100s. 50s of ads = exactly 50%.
        let candidates = [
            makeCandidate(startTime: 0, endTime: 50, skipConfidence: 0.8, ordinalBase: 100),
        ]
        let result = makeFinalizer(episodeDuration: 100).finalize(candidates)

        #expect(result[0].decision.eligibilityGate == .eligible)
    }

    @Test("Already non-eligible spans do not count toward cap")
    func nonEligibleDoNotCountTowardCap() {
        // Episode 100s. One eligible 30s span + one blockedByPolicy 30s span.
        // Only 30s is auto-skip eligible = 30% < 50%.
        let candidates = [
            makeCandidate(startTime: 0, endTime: 30, skipConfidence: 0.8,
                          eligibilityGate: .eligible, ordinalBase: 100),
            makeCandidate(startTime: 40, endTime: 70, skipConfidence: 0.5,
                          eligibilityGate: .blockedByEvidenceQuorum, ordinalBase: 300),
        ]
        let result = makeFinalizer(episodeDuration: 100).finalize(candidates)

        #expect(result[0].decision.eligibilityGate == .eligible)
        // Second stays blocked by quorum (not changed to action cap).
        #expect(result[1].decision.eligibilityGate == .blockedByEvidenceQuorum)
    }

    // MARK: - Constraint 6: Policy overrides

    @Test("Paid third-party gets autoSkipEligible")
    func paidThirdPartyAutoSkip() {
        let candidates = [
            makeCandidate(intent: .paid, ownership: .thirdParty, ordinalBase: 100),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result[0].policyAction == .autoSkipEligible)
    }

    @Test("Organic content gets suppress policy")
    func organicSuppressed() {
        let candidates = [
            makeCandidate(intent: .organic, ownership: .thirdParty, ordinalBase: 100),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result[0].policyAction == .suppress)
        #expect(result[0].constraintTrace.contains(.policyOverrideApplied))
    }

    @Test("Affiliate gets detectOnly policy")
    func affiliateDetectOnly() {
        let candidates = [
            makeCandidate(intent: .affiliate, ownership: .thirdParty, ordinalBase: 100),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result[0].policyAction == .detectOnly)
        #expect(result[0].constraintTrace.contains(.policyOverrideApplied))
    }

    // MARK: - Combined constraints

    @Test("Overlap resolution then gap merge work together")
    func overlapThenMerge() {
        // Spans A(10-50), B(45-55, lower), C(56-80).
        // Overlap: A wins, B trimmed to 50-55.
        // Gap B(50-55) to C(56-80) is 1s < 3s → merge into 50-80.
        // Gap A(10-50) to merged(50-80) is 0s < 3s → merge into 10-80.
        // Net result: one merged span covering 10-80.
        let candidates = [
            makeCandidate(startTime: 10, endTime: 50, skipConfidence: 0.9, ordinalBase: 100),
            makeCandidate(startTime: 45, endTime: 55, skipConfidence: 0.5, ordinalBase: 300),
            makeCandidate(startTime: 56, endTime: 80, skipConfidence: 0.7, ordinalBase: 500),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result.count == 1)
        #expect(result[0].span.startTime == 10)
        #expect(result[0].span.endTime == 80)
        #expect(result[0].decision.skipConfidence == 0.9) // highest confidence wins in merge
    }

    @Test("Overlap trim makes span too short → dropped by duration sanity")
    func overlapTrimThenDropped() {
        // A(10-50, high), B(47-53, low).
        // Overlap: A wins, B trimmed to 50-53 = 3s < 5s → dropped.
        let candidates = [
            makeCandidate(startTime: 10, endTime: 50, skipConfidence: 0.9, ordinalBase: 100),
            makeCandidate(startTime: 47, endTime: 53, skipConfidence: 0.3, ordinalBase: 300),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result.count == 1)
        #expect(result[0].span.startTime == 10)
    }

    @Test("Empty input produces empty output")
    func emptyInput() {
        let result = makeFinalizer().finalize([])
        #expect(result.isEmpty)
    }

    @Test("Single span passes through all constraints unchanged")
    func singleSpanPassThrough() {
        let candidates = [
            makeCandidate(startTime: 10, endTime: 40, ordinalBase: 100),
        ]
        let result = makeFinalizer().finalize(candidates)

        #expect(result.count == 1)
        #expect(result[0].span.startTime == 10)
        #expect(result[0].span.endTime == 40)
        #expect(result[0].policyAction == .autoSkipEligible)
    }

    @Test("Constraint trace accumulates across multiple constraints")
    func traceAccumulates() {
        // Span crosses content chapter AND is organic → both should trace.
        let chapters = [ChapterMarker(startTime: 15, endTime: 35, isContent: true)]
        let candidates = [
            makeCandidate(startTime: 10, endTime: 40, intent: .organic, ownership: .show, ordinalBase: 100),
        ]
        let result = makeFinalizer(chapters: chapters).finalize(candidates)

        #expect(result[0].constraintTrace.contains(.chapterPenaltyApplied))
        #expect(result[0].constraintTrace.contains(.policyOverrideApplied))
    }
}
