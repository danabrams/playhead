// NarlGroundTruthTests.swift
// playhead-narl.1: Unit tests for the ground-truth construction rules (§A.4).

import Foundation
import Testing
@testable import Playhead

@Suite("NarlGroundTruth – scope parsing")
struct NarlCorrectionScopeParseTests {

    @Test("Parses exactTimeSpan")
    func parsesExactTimeSpan() {
        let parsed = NarlCorrectionScope.parse("exactTimeSpan:asset-1:120.500:180.250")
        if case .exactTimeSpan(let asset, let s, let e) = parsed {
            #expect(asset == "asset-1")
            #expect(s == 120.5)
            #expect(e == 180.25)
        } else {
            Issue.record("Expected exactTimeSpan, got \(parsed)")
        }
    }

    @Test("Parses ordinal exactSpan")
    func parsesExactSpan() {
        let parsed = NarlCorrectionScope.parse("exactSpan:asset-1:3:7")
        if case .exactSpan(let asset, let lo, let hi) = parsed {
            #expect(asset == "asset-1")
            #expect(lo == 3)
            #expect(hi == 7)
        } else {
            Issue.record("Expected exactSpan, got \(parsed)")
        }
    }

    @Test("Parses whole-asset veto (INT64_MAX / Int.max form)")
    func parsesWholeAssetVetoIntMax() {
        // Real exports use the native Int.max on 64-bit platforms, which is
        // identical to INT64_MAX: 9223372036854775807.
        let parsed = NarlCorrectionScope.parse("exactSpan:asset-1:0:9223372036854775807")
        if case .wholeAssetVeto(let asset) = parsed {
            #expect(asset == "asset-1")
        } else {
            Issue.record("Expected wholeAssetVeto, got \(parsed)")
        }
    }

    @Test("Unhandled sponsor-on-show scope")
    func unhandledSponsor() {
        let parsed = NarlCorrectionScope.parse("sponsorOnShow:podcast-1:Acme")
        if case .unhandled = parsed {
            // pass
        } else {
            Issue.record("Expected unhandled for sponsor scope, got \(parsed)")
        }
    }

    @Test("Malformed ordinals are unhandled")
    func malformedOrdinals() {
        let parsed = NarlCorrectionScope.parse("exactSpan:asset-1:foo:bar")
        if case .unhandled = parsed {
            // pass
        } else {
            Issue.record("Expected unhandled for malformed, got \(parsed)")
        }
    }
}

@Suite("NarlGroundTruth – build")
struct NarlGroundTruthBuildTests {

    private func makeTrace(
        baselineAdSpans: [(Double, Double)] = [],
        corrections: [(source: String, scope: String)] = [],
        atoms: [(Double, Double, String)] = []
    ) -> FrozenTrace {
        let baseline = baselineAdSpans.map { s, e in
            ReplaySpanDecision(startTime: s, endTime: e, confidence: 0.9, isAd: true, sourceTag: "baseline")
        }
        let frozenCorrections = corrections.map { src, scope in
            FrozenTrace.FrozenCorrection(source: src, scope: scope, createdAt: 1000)
        }
        let frozenAtoms = atoms.map { s, e, t in
            FrozenTrace.FrozenAtom(startTime: s, endTime: e, text: t)
        }
        return FrozenTrace(
            episodeId: "ep-test",
            podcastId: "pod-test",
            episodeDuration: 3600,
            traceVersion: FrozenTrace.currentTraceVersion,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000),
            featureWindows: [],
            atoms: frozenAtoms,
            evidenceCatalog: [],
            corrections: frozenCorrections,
            decisionEvents: [],
            baselineReplaySpanDecisions: baseline,
            holdoutDesignation: .training
        )
    }

    @Test("Baseline spans with no corrections pass through")
    func baselinePassthrough() {
        let trace = makeTrace(baselineAdSpans: [(120, 180), (600, 660)])
        let gt = NarlGroundTruth.build(for: trace)
        #expect(!gt.isExcluded)
        #expect(gt.adWindows.count == 2)
        #expect(gt.adWindows[0] == NarlTimeRange(start: 120, end: 180))
        #expect(gt.adWindows[1] == NarlTimeRange(start: 600, end: 660))
    }

    @Test("falseNegative correction adds a new positive window")
    func falseNegativeAdds() {
        let trace = makeTrace(
            baselineAdSpans: [(120, 180)],
            corrections: [(source: "reportMissedAd", scope: "exactTimeSpan:ep-test:500.000:560.000")]
        )
        let gt = NarlGroundTruth.build(for: trace)
        #expect(!gt.isExcluded)
        #expect(gt.falseNegativeCorrectionCount == 1)
        #expect(gt.adWindows.count == 2)
        #expect(gt.adWindows.contains(NarlTimeRange(start: 500, end: 560)))
        #expect(gt.adWindows.contains(NarlTimeRange(start: 120, end: 180)))
    }

    @Test("falsePositive correction subtracts a positive window")
    func falsePositiveRemoves() {
        let trace = makeTrace(
            baselineAdSpans: [(120, 180), (600, 660)],
            corrections: [(source: "listenRevert", scope: "exactTimeSpan:ep-test:120.000:180.000")]
        )
        let gt = NarlGroundTruth.build(for: trace)
        #expect(!gt.isExcluded)
        #expect(gt.falsePositiveCorrectionCount == 1)
        #expect(gt.adWindows.count == 1)
        #expect(gt.adWindows.first == NarlTimeRange(start: 600, end: 660))
    }

    @Test("falsePositive partial overlap clips rather than removes")
    func falsePositiveClips() {
        let trace = makeTrace(
            baselineAdSpans: [(100, 200)],
            corrections: [(source: "dismissBanner", scope: "exactTimeSpan:ep-test:150.000:250.000")]
        )
        let gt = NarlGroundTruth.build(for: trace)
        #expect(!gt.isExcluded)
        #expect(gt.adWindows.count == 1)
        #expect(gt.adWindows.first == NarlTimeRange(start: 100, end: 150))
    }

    @Test("Whole-asset veto excludes the episode entirely")
    func wholeAssetVetoExcludes() {
        let trace = makeTrace(
            baselineAdSpans: [(120, 180), (600, 660)],
            corrections: [(source: "listenRevert", scope: "exactSpan:ep-test:0:9223372036854775807")]
        )
        let gt = NarlGroundTruth.build(for: trace)
        #expect(gt.isExcluded)
        #expect(gt.exclusionReason == "wholeAssetVeto:ep-test")
        #expect(gt.adWindows.isEmpty)
    }

    @Test("Ordinal exactSpan correction resolves to time via atoms (falseNegative)")
    func ordinalFalseNegativeResolvesViaAtoms() {
        // Atoms: [0: 0-10], [1: 10-20], [2: 20-30], [3: 30-40]
        // Span ordinals 1..2 → time 10..30
        let atoms: [(Double, Double, String)] = [
            (0, 10, "a0"), (10, 20, "a1"), (20, 30, "a2"), (30, 40, "a3"),
        ]
        let trace = makeTrace(
            baselineAdSpans: [],
            corrections: [(source: "flagAsAd", scope: "exactSpan:ep-test:1:2")],
            atoms: atoms
        )
        let gt = NarlGroundTruth.build(for: trace)
        #expect(!gt.isExcluded)
        #expect(gt.falseNegativeCorrectionCount == 1)
        #expect(gt.ordinalCorrectionCount == 1)
        #expect(gt.adWindows == [NarlTimeRange(start: 10, end: 30)])
    }

    @Test("Ordinal correction with out-of-range upper bound clamps to atom count")
    func ordinalClampsUpperBound() {
        let atoms: [(Double, Double, String)] = [
            (0, 10, "a0"), (10, 20, "a1"), (20, 30, "a2"),
        ]
        let trace = makeTrace(
            corrections: [(source: "flagAsAd", scope: "exactSpan:ep-test:1:99")],
            atoms: atoms
        )
        let gt = NarlGroundTruth.build(for: trace)
        #expect(!gt.isExcluded)
        #expect(gt.adWindows == [NarlTimeRange(start: 10, end: 30)])
    }

    @Test("Ordinal correction with no atoms is silently skipped")
    func ordinalWithoutAtomsSkipped() {
        let trace = makeTrace(
            corrections: [(source: "flagAsAd", scope: "exactSpan:ep-test:1:2")],
            atoms: []
        )
        let gt = NarlGroundTruth.build(for: trace)
        #expect(!gt.isExcluded)
        #expect(gt.adWindows.isEmpty)
        #expect(gt.skippedCorrectionCount == 1)
    }

    @Test("Threshold boundary: exact-edge exactTimeSpan does not clip non-overlapping")
    func exactEdgeNoOverlap() {
        // A falsePositive whose end equals the baseline's start should NOT
        // clip the baseline (closed-open semantics).
        let trace = makeTrace(
            baselineAdSpans: [(100, 200)],
            corrections: [(source: "dismissBanner", scope: "exactTimeSpan:ep-test:50.000:100.000")]
        )
        let gt = NarlGroundTruth.build(for: trace)
        #expect(gt.adWindows.first == NarlTimeRange(start: 100, end: 200))
    }

    @Test("Threshold boundary: exact-edge exactTimeSpan touches inside")
    func exactEdgeTouchingInside() {
        // A falsePositive completely inside the baseline should split it.
        let trace = makeTrace(
            baselineAdSpans: [(100, 200)],
            corrections: [(source: "dismissBanner", scope: "exactTimeSpan:ep-test:140.000:160.000")]
        )
        let gt = NarlGroundTruth.build(for: trace)
        #expect(gt.adWindows.count == 2)
        #expect(gt.adWindows.contains(NarlTimeRange(start: 100, end: 140)))
        #expect(gt.adWindows.contains(NarlTimeRange(start: 160, end: 200)))
    }

    @Test("Unknown correction source is counted as skipped")
    func unknownSourceSkipped() {
        let trace = makeTrace(
            baselineAdSpans: [(120, 180)],
            corrections: [(source: "someFutureCorrection", scope: "exactTimeSpan:ep-test:120.000:180.000")]
        )
        let gt = NarlGroundTruth.build(for: trace)
        #expect(!gt.isExcluded)
        #expect(gt.skippedCorrectionCount == 1)
        // Baseline remains untouched.
        #expect(gt.adWindows == [NarlTimeRange(start: 120, end: 180)])
    }

    @Test("Non-ad baseline spans do not appear in ground truth")
    func nonAdBaselineIgnored() {
        let baseline = [
            ReplaySpanDecision(startTime: 120, endTime: 180, confidence: 0.9, isAd: true, sourceTag: "b"),
            ReplaySpanDecision(startTime: 200, endTime: 260, confidence: 0.2, isAd: false, sourceTag: "b"),
        ]
        let trace = FrozenTrace(
            episodeId: "ep-mixed",
            podcastId: "p",
            episodeDuration: 3600,
            traceVersion: FrozenTrace.currentTraceVersion,
            capturedAt: Date(),
            featureWindows: [], atoms: [], evidenceCatalog: [],
            corrections: [], decisionEvents: [],
            baselineReplaySpanDecisions: baseline,
            holdoutDesignation: .training
        )
        let gt = NarlGroundTruth.build(for: trace)
        #expect(gt.adWindows == [NarlTimeRange(start: 120, end: 180)])
    }
}

@Suite("NarlGroundTruth – range algebra")
struct NarlGroundTruthRangeAlgebraTests {

    @Test("subtract leaves fully-contained target empty")
    func subtractContainedRemoves() {
        let out = NarlGroundTruth.subtract(
            range: NarlTimeRange(start: 0, end: 1000),
            from: [NarlTimeRange(start: 100, end: 200)]
        )
        #expect(out.isEmpty)
    }

    @Test("subtract splits a range that straddles the target")
    func subtractSplits() {
        let out = NarlGroundTruth.subtract(
            range: NarlTimeRange(start: 150, end: 160),
            from: [NarlTimeRange(start: 100, end: 200)]
        )
        #expect(out.count == 2)
        #expect(out.contains(NarlTimeRange(start: 100, end: 150)))
        #expect(out.contains(NarlTimeRange(start: 160, end: 200)))
    }

    @Test("mergeOverlaps merges adjacent and overlapping ranges")
    func mergeOverlaps() {
        let out = NarlGroundTruth.mergeOverlaps([
            NarlTimeRange(start: 0, end: 10),
            NarlTimeRange(start: 10, end: 20),     // adjacent — merges
            NarlTimeRange(start: 15, end: 25),     // overlaps — merges
            NarlTimeRange(start: 100, end: 200),   // disjoint — stays
        ])
        #expect(out.count == 2)
        #expect(out[0] == NarlTimeRange(start: 0, end: 25))
        #expect(out[1] == NarlTimeRange(start: 100, end: 200))
    }
}
