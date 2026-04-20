// CoverageSummaryTests.swift
// Golden (coverage, anchor) table covering every PlaybackReadiness case
// the pure derivation function returns, plus the two boundary cases
// called out in the playhead-cthe spec.
//
// Scope: playhead-cthe (Phase 2 deliverable 2).

import Foundation
import Testing

@testable import Playhead

@Suite("CoverageSummary / derivePlaybackReadiness — golden table (playhead-cthe)")
struct CoverageSummaryTests {

    // MARK: - Canonical base record

    /// A canonical, non-complete coverage record whose ranges can be
    /// overridden per-test. Model/policy/schema versions are fixed so
    /// we don't couple unrelated tests to their values.
    private static func makeCoverage(
        ranges: [ClosedRange<TimeInterval>],
        isComplete: Bool
    ) -> CoverageSummary {
        CoverageSummary(
            coverageRanges: ranges,
            isComplete: isComplete,
            modelVersion: "m1",
            policyVersion: 1,
            featureSchemaVersion: 1,
            updatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }

    // MARK: - .none

    @Test(".none — nil coverage yields .none regardless of anchor")
    func noneWhenCoverageIsNil() {
        #expect(derivePlaybackReadiness(coverage: nil, anchor: nil) == .none)
        #expect(derivePlaybackReadiness(coverage: nil, anchor: 0.0) == .none)
        #expect(derivePlaybackReadiness(coverage: nil, anchor: 42.5) == .none)
    }

    @Test(".none — empty coverage ranges + not complete yields .none")
    func noneWhenCoverageRangesEmpty() {
        let coverage = Self.makeCoverage(ranges: [], isComplete: false)
        #expect(derivePlaybackReadiness(coverage: coverage, anchor: nil) == .none)
        #expect(derivePlaybackReadiness(coverage: coverage, anchor: 100.0) == .none)
    }

    // MARK: - .complete

    @Test(".complete — isComplete=true wins over anchor semantics")
    func completeShortCircuitsAnchor() {
        let coverage = Self.makeCoverage(
            ranges: [0.0...3600.0],
            isComplete: true
        )
        #expect(derivePlaybackReadiness(coverage: coverage, anchor: nil) == .complete)
        #expect(derivePlaybackReadiness(coverage: coverage, anchor: 0.0) == .complete)
        #expect(derivePlaybackReadiness(coverage: coverage, anchor: 3600.0) == .complete)
    }

    // MARK: - .deferredOnly

    @Test(".deferredOnly — non-empty coverage + nil anchor")
    func deferredOnlyWhenAnchorIsNil() {
        let coverage = Self.makeCoverage(
            ranges: [0.0...1000.0],
            isComplete: false
        )
        #expect(derivePlaybackReadiness(coverage: coverage, anchor: nil) == .deferredOnly)
    }

    @Test(".deferredOnly — anchor far from every range")
    func deferredOnlyWhenAnchorFarFromCoverage() {
        // Range covers [0, 1000]; anchor at 2000 looks ahead to 2900.
        // No range contains that segment, but coverage is non-empty.
        let coverage = Self.makeCoverage(
            ranges: [0.0...1000.0],
            isComplete: false
        )
        #expect(derivePlaybackReadiness(coverage: coverage, anchor: 2000.0) == .deferredOnly)
    }

    @Test(".deferredOnly — anchor inside range but lookahead extends past upper bound")
    func deferredOnlyWhenLookaheadOvershoots() {
        // Anchor 300, lookahead 1200. Range only goes to 1000.
        // Starting at 300 would fall into unanalyzed territory before
        // the 15-minute lookahead closes.
        let coverage = Self.makeCoverage(
            ranges: [0.0...1000.0],
            isComplete: false
        )
        #expect(derivePlaybackReadiness(coverage: coverage, anchor: 300.0) == .deferredOnly)
    }

    // MARK: - .proximal

    @Test(".proximal — single range covers [anchor, anchor + 15min]")
    func proximalSingleRange() {
        // Lookahead 900s; at anchor 42.5 the range [0, 1000] covers
        // [42.5, 942.5]. lookaheadEnd=942.5 <= range.upperBound=1000.
        let coverage = Self.makeCoverage(
            ranges: [0.0...1000.0],
            isComplete: false
        )
        #expect(derivePlaybackReadiness(coverage: coverage, anchor: 42.5) == .proximal)
    }

    @Test(".proximal — range exactly covers the lookahead window")
    func proximalExactBoundary() {
        // Anchor 0, lookahead 900. Range [0, 900] exactly covers
        // [0, 900] — the boundary case is inclusive.
        let coverage = Self.makeCoverage(
            ranges: [0.0...900.0],
            isComplete: false
        )
        #expect(derivePlaybackReadiness(coverage: coverage, anchor: 0.0) == .proximal)
    }

    @Test(".deferredOnly — range ends one tick below the lookahead boundary")
    func deferredOnlyJustBelowBoundary() {
        // Symmetric pair to `proximalExactBoundary`: same anchor, same
        // lookahead, but the range ends at 899.99 — strictly below the
        // 900 lookaheadEnd. This locks in the INCLUSIVE-upper-bound
        // interpretation of the `.proximal` boundary: a range must reach
        // AT LEAST lookaheadEnd (not just approach it) to qualify. If
        // the comparison ever flips to strict inequality in either
        // direction, exactly one of `proximalExactBoundary` and this
        // test flips, pinning the semantics at the boundary.
        let coverage = Self.makeCoverage(
            ranges: [0.0...899.99],
            isComplete: false
        )
        #expect(derivePlaybackReadiness(coverage: coverage, anchor: 0.0) == .deferredOnly)
    }

    @Test(".proximal — later range covers the anchor even when earlier ranges do not")
    func proximalSecondRangeCovers() {
        // Two ranges; only the second covers [anchor, anchor+900].
        let coverage = Self.makeCoverage(
            ranges: [0.0...100.0, 500.0...1800.0],
            isComplete: false
        )
        #expect(derivePlaybackReadiness(coverage: coverage, anchor: 600.0) == .proximal)
    }

    // MARK: - Boundary cases called out in the bead spec

    @Test("Boundary — complete implies proximal (short-circuit to .complete)")
    func boundaryCompleteImpliesProximal() {
        // If coverage.isComplete, readiness is .complete regardless of
        // anchor; upstream invariants (SurfaceStatusInvariants
        // violations(coverage:readiness:)) assert that the record's
        // firstCoveredOffset is non-nil.
        let coverage = Self.makeCoverage(
            ranges: [0.0...3600.0],
            isComplete: true
        )
        #expect(coverage.firstCoveredOffset != nil)
        #expect(derivePlaybackReadiness(coverage: coverage, anchor: 1234.5) == .complete)
    }

    @Test("Boundary — proximal requires firstCoveredOffset != nil")
    func boundaryProximalRequiresFirstCoveredOffset() {
        // Any .proximal result is derived from a range that contains
        // the anchor, which is only possible when coverageRanges is
        // non-empty — which by construction means
        // firstCoveredOffset != nil.
        let coverage = Self.makeCoverage(
            ranges: [0.0...1000.0],
            isComplete: false
        )
        #expect(derivePlaybackReadiness(coverage: coverage, anchor: 42.5) == .proximal)
        #expect(coverage.firstCoveredOffset == 0.0)
    }

    // MARK: - Normalization & invariants on construction

    @Test("Init normalizes unsorted ranges into ascending order")
    func initSortsUnsortedRanges() {
        let coverage = CoverageSummary(
            coverageRanges: [500.0...1000.0, 0.0...100.0, 200.0...400.0],
            isComplete: false,
            modelVersion: "m1",
            policyVersion: 1,
            featureSchemaVersion: 1,
            updatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let lowers = coverage.coverageRanges.map(\.lowerBound)
        #expect(lowers == [0.0, 200.0, 500.0])
    }

    @Test("Init merges overlapping ranges into one")
    func initMergesOverlappingRanges() {
        let coverage = CoverageSummary(
            coverageRanges: [0.0...500.0, 400.0...700.0, 600.0...1000.0],
            isComplete: false,
            modelVersion: "m1",
            policyVersion: 1,
            featureSchemaVersion: 1,
            updatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        #expect(coverage.coverageRanges.count == 1)
        #expect(coverage.coverageRanges.first == 0.0...1000.0)
    }

    @Test("Init merges adjacent (touching) ranges")
    func initMergesTouchingRanges() {
        // Two ranges that end/start at the same point are continuous;
        // normalization merges them so the derivation can treat the
        // coverage as a single lookahead-candidate.
        let coverage = CoverageSummary(
            coverageRanges: [0.0...500.0, 500.0...1000.0],
            isComplete: false,
            modelVersion: "m1",
            policyVersion: 1,
            featureSchemaVersion: 1,
            updatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        #expect(coverage.coverageRanges == [0.0...1000.0])
    }

    @Test("firstCoveredOffset mirrors coverageRanges.first?.lowerBound")
    func firstCoveredOffsetTracksFirstRange() {
        let a = CoverageSummary.empty(
            modelVersion: "m1",
            policyVersion: 1,
            featureSchemaVersion: 1
        )
        #expect(a.firstCoveredOffset == nil)
        let b = Self.makeCoverage(ranges: [120.0...600.0], isComplete: false)
        #expect(b.firstCoveredOffset == 120.0)
    }

    // MARK: - Codable round-trip

    @Test("Codable round-trip preserves every field")
    func codableRoundTrip() throws {
        let original = CoverageSummary(
            coverageRanges: [0.0...100.0, 200.0...400.0],
            isComplete: false,
            modelVersion: "model-v42",
            policyVersion: 7,
            featureSchemaVersion: 3,
            updatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let encoder = JSONEncoder()
        let decoder = JSONDecoder()
        let data = try encoder.encode(original)
        let decoded = try decoder.decode(CoverageSummary.self, from: data)
        #expect(decoded == original)
    }
}
