// ChapterPlanTests.swift
// playhead-au2v.1.1: Tests for `ChapterPlan` (Codable round-trip, Equatable
// equivalence, duration-weighted plan-confidence math) and the
// `ChapterSource.inferred` extension.

import Foundation
import Testing
@testable import Playhead

// MARK: - ChapterSource.inferred

@Suite("ChapterSource.inferred")
struct ChapterSourceInferredTests {

    @Test("inferred raw value is stable for cache compatibility")
    func inferredRawValueIsStable() {
        // The raw value gets persisted in `ChapterEvidence` JSON. Pin it
        // so a future rename is caught loudly rather than silently
        // invalidating cached `ChapterPlan` files.
        #expect(ChapterSource.inferred.rawValue == "inferred")
    }

    @Test("CaseIterable lists all four sources")
    func caseIterableCoversAll() {
        let cases = Set(ChapterSource.allCases)
        #expect(cases == [.id3, .pc20, .rssInline, .inferred])
    }

    @Test("inferred is Codable round-trip stable")
    func inferredCodableRoundTrip() throws {
        let encoded = try JSONEncoder().encode(ChapterSource.inferred)
        let decoded = try JSONDecoder().decode(ChapterSource.self, from: encoded)
        #expect(decoded == .inferred)
    }

    @Test("ChapterEvidence with .inferred source is Codable round-trip stable")
    func evidenceWithInferredCodableRoundTrip() throws {
        let original = ChapterEvidence(
            startTime: 12.0,
            endTime: 92.0,
            title: "Sponsor: BetterHelp",
            source: .inferred,
            disposition: .adBreak,
            qualityScore: 0.82
        )
        let encoded = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(ChapterEvidence.self, from: encoded)
        #expect(decoded == original)
        #expect(decoded.source == .inferred)
    }
}

// MARK: - ChapterPlan

@Suite("ChapterPlan")
struct ChapterPlanTests {

    private static func makeChapter(
        start: TimeInterval,
        end: TimeInterval?,
        quality: Float,
        source: ChapterSource = .inferred
    ) -> ChapterEvidence {
        ChapterEvidence(
            startTime: start,
            endTime: end,
            title: "chapter-\(start)",
            source: source,
            disposition: .ambiguous,
            qualityScore: quality
        )
    }

    // MARK: Codable / Equatable

    @Test("Codable round-trip preserves all fields")
    func codableRoundTrip() throws {
        let plan = ChapterPlan(
            episodeContentHash: "abc123",
            chapters: [
                Self.makeChapter(start: 0, end: 60, quality: 0.5),
                Self.makeChapter(start: 60, end: 120, quality: 0.9),
            ],
            planConfidence: 0.7,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            schemaVersion: ChapterPlan.currentSchemaVersion,
            generationDiagnostics: ChapterPlanDiagnostics(
                candidatesDetected: 8,
                candidatesKept: 4,
                operationalUnclearCount: 1,
                semanticUnclearCount: 2
            )
        )
        let data = try JSONEncoder().encode(plan)
        let decoded = try JSONDecoder().decode(ChapterPlan.self, from: data)
        #expect(decoded == plan)
    }

    @Test("schemaVersion defaults to currentSchemaVersion when omitted")
    func schemaVersionDefault() {
        let plan = ChapterPlan(
            episodeContentHash: "h",
            chapters: [],
            planConfidence: 0.0,
            generatedAt: Date()
        )
        #expect(plan.schemaVersion == ChapterPlan.currentSchemaVersion)
    }

    // MARK: planConfidence math (duration-weighted)

    @Test("computePlanConfidence on empty list is 0")
    func confidenceEmpty() {
        #expect(ChapterPlan.computePlanConfidence([]) == 0.0)
    }

    @Test("all-confident chapters yield ~1.0")
    func confidenceAllConfident() {
        let chapters = [
            Self.makeChapter(start: 0, end: 60, quality: 1.0),
            Self.makeChapter(start: 60, end: 180, quality: 1.0),
        ]
        let confidence = ChapterPlan.computePlanConfidence(chapters)
        #expect(abs(confidence - 1.0) < 1e-6)
    }

    @Test("zero-confidence chapters yield 0.0")
    func confidenceAllZero() {
        let chapters = [
            Self.makeChapter(start: 0, end: 60, quality: 0.0),
            Self.makeChapter(start: 60, end: 180, quality: 0.0),
        ]
        #expect(ChapterPlan.computePlanConfidence(chapters) == 0.0)
    }

    @Test("mixed durations weight confidence by interval length, not chapter count")
    func confidenceDurationWeighted() {
        // 60s @ 0.0 quality, 240s @ 1.0 quality.
        // Equal-weight (count) average would be 0.5.
        // Duration-weight average: (0*60 + 1.0*240) / 300 = 0.8.
        let chapters = [
            Self.makeChapter(start: 0, end: 60, quality: 0.0),
            Self.makeChapter(start: 60, end: 300, quality: 1.0),
        ]
        let confidence = ChapterPlan.computePlanConfidence(chapters)
        #expect(abs(confidence - 0.8) < 1e-6)
    }

    @Test("chapter without endTime is treated as 60s (matches builder fallback)")
    func confidenceOpenEndedChapter() {
        // Single chapter starting at 100 with no endTime → 60s nominal,
        // qualityScore 0.5 → confidence 0.5.
        let chapters = [
            Self.makeChapter(start: 100, end: nil, quality: 0.5),
        ]
        let confidence = ChapterPlan.computePlanConfidence(chapters)
        #expect(abs(confidence - 0.5) < 1e-6)
    }

    @Test("malformed (non-positive duration) chapters are ignored")
    func confidenceSkipsMalformed() {
        // Chapter A: end <= start → ignored. Chapter B: well-formed.
        // Non-finite end is covered separately in
        // `confidenceSkipsNonFiniteEnd`.
        let badEnd = Self.makeChapter(start: 100, end: 100, quality: 0.0)
        let good = Self.makeChapter(start: 200, end: 400, quality: 0.7)
        let confidence = ChapterPlan.computePlanConfidence([badEnd, good])
        #expect(abs(confidence - 0.7) < 1e-6)
    }

    @Test("all-malformed chapter list yields 0.0 (no usable duration)")
    func confidenceAllMalformedIsZero() {
        // Every chapter has end <= start → effectiveDuration is 0
        // for all of them → totalDuration is 0 → returns 0 by the
        // "no usable duration" guard, not NaN.
        let chapters = [
            Self.makeChapter(start: 100, end: 100, quality: 0.5),
            Self.makeChapter(start: 200, end: 150, quality: 0.9),
        ]
        let confidence = ChapterPlan.computePlanConfidence(chapters)
        #expect(confidence == 0.0)
    }

    @Test("non-finite endTime is skipped without producing NaN confidence")
    func confidenceSkipsNonFiniteEnd() {
        let bad = Self.makeChapter(start: 0, end: .infinity, quality: 0.7)
        let good = Self.makeChapter(start: 100, end: 200, quality: 0.4)
        let confidence = ChapterPlan.computePlanConfidence([bad, good])
        #expect(confidence.isFinite)
        #expect(abs(confidence - 0.4) < 1e-6)
    }

    @Test("NaN qualityScore is skipped without poisoning the aggregate")
    func confidenceSkipsNaNQuality() {
        // A single .nan input must not turn the whole aggregate into
        // NaN. We skip it and weight the remaining valid chapters
        // normally.
        let bad = Self.makeChapter(start: 0, end: 60, quality: .nan)
        let good = Self.makeChapter(start: 60, end: 120, quality: 0.5)
        let confidence = ChapterPlan.computePlanConfidence([bad, good])
        #expect(confidence.isFinite)
        #expect(abs(confidence - 0.5) < 1e-6)
    }

    @Test("computePlanConfidence clamps out-of-range qualityScore inputs")
    func confidenceClampsOutOfRange() {
        // Defensive: even if an upstream producer mis-emits a score
        // outside [0, 1], the aggregate must stay in [0, 1].
        let chapters = [
            Self.makeChapter(start: 0, end: 60, quality: 1.5),
        ]
        let confidence = ChapterPlan.computePlanConfidence(chapters)
        #expect(confidence <= 1.0)
        #expect(confidence >= 0.0)
    }

    @Test("computePlanConfidence clamps a negative qualityScore to 0")
    func confidenceClampsNegativeQuality() {
        // Symmetric to the upper-bound clamp: a producer mis-emitting a
        // negative finite score must not pull the aggregate below 0.
        // Policy: finite-but-out-of-range scores are clamped (saturated
        // at the boundary they're past); non-finite scores (NaN/Inf)
        // are dropped entirely (see `confidenceSkipsNaNQuality`). The
        // distinction is intentional — finite values still carry a
        // direction signal (positive vs negative); NaN does not.
        let chapters = [
            Self.makeChapter(start: 0, end: 60, quality: -0.5),
        ]
        let confidence = ChapterPlan.computePlanConfidence(chapters)
        #expect(confidence == 0.0)
    }

    @Test("non-finite startTime is skipped without poisoning confidence")
    func confidenceSkipsNonFiniteStart() {
        // A chapter with NaN/Inf startTime signals corrupt bounds. Even
        // when `endTime` is nil (where the 60s fallback math doesn't
        // depend on start), we still drop it: a chapter we cannot
        // anchor in the timeline is not trustworthy enough to weight
        // into the aggregate. Skip it and weight the remaining valid
        // chapters normally.
        let bad = Self.makeChapter(start: .nan, end: nil, quality: 0.9)
        let good = Self.makeChapter(start: 0, end: 60, quality: 0.4)
        let confidence = ChapterPlan.computePlanConfidence([bad, good])
        #expect(confidence.isFinite)
        #expect(abs(confidence - 0.4) < 1e-6)
    }

    @Test("empty chapter list round-trips through Codable")
    func codableRoundTripEmptyChapters() throws {
        // Boundary: a plan with zero chapters (e.g., chapter generation
        // produced nothing) must still round-trip cleanly so the cache
        // can persist a "no chapters detected" verdict.
        let plan = ChapterPlan(
            episodeContentHash: "empty-hash",
            chapters: [],
            planConfidence: 0.0,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let data = try JSONEncoder().encode(plan)
        let decoded = try JSONDecoder().decode(ChapterPlan.self, from: data)
        #expect(decoded == plan)
        #expect(decoded.chapters.isEmpty)
    }
}
