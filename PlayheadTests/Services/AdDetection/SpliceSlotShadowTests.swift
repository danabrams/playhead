// SpliceSlotShadowTests.swift
// playhead-xsdz.21 (Bead C): unit tests for the PURE shadow primitives —
// the frozen v3 breadcrumb formatter, the disposition→reason mapping (reason
// precedence + slot-field sources + sentinels + both widthDeltaSec branches),
// the decision-delta computer, and the projection substitution rule +
// treatment-arm disjointness. Every fixture drives the SAME
// `SpliceSlotDispositionEngine.computeDispositions` the flag-ON path consumes,
// so `shadow == flag-ON` is proven by construction, not re-implemented.

import Foundation
import Testing

@testable import Playhead

// MARK: - Builders (mirror SpliceSlotDispositionTests)

private func edge(_ t: Double, score: Double = 0.5, signals: Int = 1) -> SpliceEdgeEvidence {
    SpliceEdgeEvidence(time: t, stepScore: score, contributingSignals: signals)
}

private func slot(
    _ start: Double,
    _ end: Double,
    coverage: Double = 1.0,
    startScore: Double = 0.5,
    endScore: Double = 0.5
) -> SpliceSlot {
    SpliceSlot(
        startTime: start,
        endTime: end,
        startEdge: edge(start, score: startScore),
        endEdge: edge(end, score: endScore),
        slotConfidence: min(startScore, endScore),
        coreCoverage: coverage
    )
}

private func cand(
    minted: (Double, Double),
    slot s: SpliceSlot?,
    intersects: Bool = true,
    coreMatch: Bool = false,
    slotMatch: Bool = false
) -> SpliceSlotCandidate {
    SpliceSlotCandidate(
        mintedInterval: TimeRange(start: minted.0, end: minted.1),
        slot: s,
        slotIntersectsAtoms: intersects,
        coreBankMatch: coreMatch,
        slotBankMatch: slotMatch
    )
}

/// Diagnostics for a span that resolved a slot (qualified at resolver level;
/// any later demotion is an ENGINE outcome, not a resolver failure).
private func diagWithSlot(_ s: SpliceSlot) -> SpliceSlotDiagnostics {
    SpliceSlotDiagnostics(bestGeometryValidPair: s, failureReason: nil)
}

/// Diagnostics for a slot-less span with the given resolver failure reason.
private func diagFail(
    _ reason: SpliceSlotDiagnostics.FailureReason,
    champion: SpliceSlot? = nil
) -> SpliceSlotDiagnostics {
    SpliceSlotDiagnostics(bestGeometryValidPair: champion, failureReason: reason)
}

/// Build shadow rows for a fixture by running the REAL engine, then mapping.
private func rows(
    assetId: String = "asset-x",
    _ candidates: [SpliceSlotCandidate],
    _ diagnostics: [SpliceSlotDiagnostics]
) -> [SpliceSlotShadowRow] {
    let result = SpliceSlotDispositionEngine.computeDispositions(candidates)
    return SpliceSlotShadowRowBuilder.makeRows(
        assetId: assetId,
        spanIds: (0..<candidates.count).map { "s\($0)" },
        candidates: candidates,
        diagnostics: diagnostics,
        dispositions: result.dispositions
    )
}

// MARK: - Reason enum

@Suite("SpliceSlotShadowReason (playhead-xsdz.21)")
struct SpliceSlotShadowReasonTests {
    @Test("exactly 13 frozen reason tokens")
    func thirteenReasons() {
        #expect(SpliceSlotShadowReason.allCases.count == 13)
        let tokens = Set(SpliceSlotShadowReason.allCases.map(\.rawValue))
        #expect(tokens == [
            "qualifying", "degenerateCore", "noCandidatePairs", "durationOutOfRange",
            "edgeBelowFloor", "slotConfidenceBelowFloor", "coreCoverageBelowMinimum",
            "vetoNewlyEnclosed", "negativeBankVeto", "slotCollision",
            "partialOverlapFallback", "emptyAtomSet", "absorbed"
        ])
    }
}

// MARK: - Frozen v3 formatter

@Suite("SpliceSlotShadowBreadcrumb frozen v3 format (playhead-xsdz.21)")
struct SpliceSlotShadowFormatterTests {

    @Test("real qualifying row: integral widthDeltaSec branch + full field order")
    func realRowIntegralWidthDelta() {
        // slot [100,160] (width 60), minted [110,150] (width 40) → widthDelta 20.
        let row = SpliceSlotShadowRow(
            assetId: "ep7",
            spanId: "sp",
            mintedStart: 110, mintedEnd: 150,
            slotStart: 100, slotEnd: 160,
            widthDeltaSec: 20,
            startEdgeScore: 0.5, endEdgeScore: 0.5, coreCoverage: 1.0,
            qualified: true, reason: .qualifying, decisionDelta: nil
        )
        #expect(SpliceSlotShadowBreadcrumb.format(row) ==
            "spliceslot.shadow assetId=ep7 mintedStart=110 mintedEnd=150 "
            + "slotStart=100 slotEnd=160 widthDeltaSec=20 startEdgeScore=0.500 "
            + "endEdgeScore=0.500 coreCoverage=1 qualified=true reason=qualifying")
    }

    @Test("real row: fractional widthDeltaSec branch renders to 3 decimals")
    func realRowFractionalWidthDelta() {
        // slot width 12.3, minted width 0 → widthDelta 12.300.
        let row = SpliceSlotShadowRow(
            assetId: "ep7",
            spanId: "sp",
            mintedStart: 5.25, mintedEnd: 5.25,
            slotStart: 1.1, slotEnd: 13.4,
            widthDeltaSec: 12.3,
            startEdgeScore: 0.375, endEdgeScore: 0.812, coreCoverage: 0.9,
            qualified: true, reason: .qualifying, decisionDelta: nil
        )
        let line = SpliceSlotShadowBreadcrumb.format(row)
        #expect(line.contains("widthDeltaSec=12.300"))
        #expect(line.contains("mintedStart=5.250"))
        #expect(line.contains("slotStart=1.100"))
        #expect(line.contains("startEdgeScore=0.375"))
        #expect(line.contains("endEdgeScore=0.812"))
        #expect(line.contains("coreCoverage=0.900"))
    }

    @Test("sentinel row (no-pair): slotStart=-1, zeros, widthDeltaSec=0")
    func sentinelRowFormat() {
        let row = SpliceSlotShadowRow(
            assetId: "ep7",
            spanId: "sp",
            mintedStart: 30, mintedEnd: 90,
            slotStart: SpliceSlotShadowRowBuilder.sentinelSlotStart,
            slotEnd: SpliceSlotShadowRowBuilder.sentinelSlotEnd,
            widthDeltaSec: 0,
            startEdgeScore: 0, endEdgeScore: 0, coreCoverage: 0,
            qualified: false, reason: .noCandidatePairs, decisionDelta: nil
        )
        #expect(SpliceSlotShadowBreadcrumb.format(row) ==
            "spliceslot.shadow assetId=ep7 mintedStart=30 mintedEnd=90 "
            + "slotStart=-1 slotEnd=-1 widthDeltaSec=0 startEdgeScore=0 "
            + "endEdgeScore=0 coreCoverage=0 qualified=false reason=noCandidatePairs")
    }
}

// MARK: - Mirror fixtures (reason precedence + slot-field sources)

@Suite("SpliceSlotShadow mirror fixtures (playhead-xsdz.21)")
struct SpliceSlotShadowMirrorTests {

    @Test("strong-inner-splice pod: slots touching at a shared break → TWO qualified=true")
    func strongInnerSplicePodTwoQualified() {
        let s0 = slot(10, 50)
        let s1 = slot(50, 90)
        let r = rows(
            [cand(minted: (15, 48), slot: s0), cand(minted: (52, 88), slot: s1)],
            [diagWithSlot(s0), diagWithSlot(s1)]
        )
        #expect(r.count == 2)
        #expect(r.allSatisfy { $0.qualified && $0.reason == .qualifying })
        // Slot fields come from the winning slot.
        #expect(r[0].slotStart == 10 && r[0].slotEnd == 50)
        #expect(r[1].slotStart == 50 && r[1].slotEnd == 90)
    }

    @Test("overshoot pod: span0 minted crosses the shared break → still TWO qualified=true")
    func overshootPodTwoQualified() {
        let s0 = slot(10, 50)
        let s1 = slot(50, 90)
        let r = rows(
            [cand(minted: (15, 55), slot: s0), cand(minted: (52, 88), slot: s1)],
            [diagWithSlot(s0), diagWithSlot(s1)]
        )
        #expect(r.filter { $0.qualified }.count == 2)
    }

    @Test("weak-inner-splice pod: ONE qualified; enclosed loser=absorbed, non-enclosed loser=slotCollision")
    func weakPodPrecedence() {
        let a = slot(0, 100, coverage: 1.0)   // kept
        let b = slot(50, 150, coverage: 0.9)  // collides w/ A; minted NOT enclosed
        let c = slot(30, 70, coverage: 0.8)   // collides w/ A; minted enclosed → absorbed
        let r = rows(
            [
                cand(minted: (5, 95), slot: a),
                cand(minted: (110, 140), slot: b),
                cand(minted: (35, 65), slot: c)
            ],
            [diagWithSlot(a), diagWithSlot(b), diagWithSlot(c)]
        )
        #expect(r.filter { $0.qualified }.count == 1)
        #expect(r[0].reason == .qualifying)
        #expect(r[1].reason == .slotCollision)
        #expect(r[2].reason == .absorbed)
        // slotCollision loser keeps its minted interval; own discarded slot fields.
        #expect(r[1].mintedStart == 110 && r[1].mintedEnd == 140)
        #expect(r[1].slotStart == 50 && r[1].slotEnd == 150)
        // absorbed row's slot fields come from the ABSORBING span's slot (A).
        #expect(r[2].slotStart == 0 && r[2].slotEnd == 100)
    }

    @Test("absorbee-bank ALL-OR-NOTHING: absorber emits negativeBankVeto; both enclosed keep minted; zero absorbed")
    func absorbeeBankAllOrNothing() {
        let k = slot(0, 100, coverage: 1.0)  // absorber
        let p = slot(20, 40, coverage: 0.9)  // clean, enclosed
        let q = slot(60, 80, coverage: 0.8)  // matched, enclosed
        let r = rows(
            [
                cand(minted: (5, 95), slot: k),
                cand(minted: (25, 35), slot: p, coreMatch: false),
                cand(minted: (65, 75), slot: q, coreMatch: true)
            ],
            [diagWithSlot(k), diagWithSlot(p), diagWithSlot(q)]
        )
        #expect(r[0].reason == .negativeBankVeto) // absorber demoted by absorbee bank match
        #expect(r.filter { $0.reason == .absorbed }.isEmpty) // zero absorbed
        // Both enclosed spans keep their minted interval with their (distinct)
        // prior-pass reasons: the clean span lost the greedy collision
        // (slotCollision); the bank-matched span was directly vetoed in pass 2
        // (negativeBankVeto). Neither is absorbed.
        #expect(r[1].reason == .slotCollision && r[1].mintedStart == 25)
        #expect(r[2].reason == .negativeBankVeto && r[2].mintedStart == 65)
    }

    @Test("absorber-demoted: a would-be absorber demoted at the fixpoint → its enclosed span reverts to prior reason, NOT absorbed")
    func absorberDemotedRevertsEnclosed() {
        // K2 [40,120] encloses clean P (edgeBelowFloor, no slot). K2 is kept
        // through round 1 but demoted in round 2 by S's freed minted overlap, so
        // P is NEVER absorbed and keeps its resolver reason.
        let k2 = slot(40, 120, coverage: 0.90)
        let x = slot(200, 260, coverage: 0.95)  // control, stays kept
        let s = slot(300, 400, coverage: 0.80)  // demoted round 1 (partial vs N)
        let n = slot(340, 420, coverage: 0.70)  // greedy loser vs S
        let champ = slot(55, 85, startScore: 0.05) // P's sub-floor champion
        let r = rows(
            [
                cand(minted: (45, 115), slot: k2),
                cand(minted: (205, 255), slot: x),
                cand(minted: (100, 150), slot: s),
                cand(minted: (350, 450), slot: n),
                cand(minted: (60, 80), slot: nil)   // P: no slot
            ],
            [
                diagWithSlot(k2), diagWithSlot(x), diagWithSlot(s), diagWithSlot(n),
                diagFail(.edgeBelowFloor, champion: champ)
            ]
        )
        // P reverts to its resolver reason and is not absorbed.
        #expect(r[4].reason == .edgeBelowFloor)
        #expect(r[4].reason != .absorbed)
        // P's slot fields come from bestGeometryValidPair (the sub-floor champion).
        #expect(r[4].slotStart == 55 && r[4].slotEnd == 85)
        // The would-be absorber K2 was itself demoted (partial-overlap fallback).
        #expect(r[0].reason == .partialOverlapFallback)
        // X stays qualified.
        #expect(r[1].reason == .qualifying)
    }

    @Test("one row per span, non-qualifying spans included")
    func oneRowPerSpanIncludingNonQualifying() {
        let a = slot(0, 60)
        let r = rows(
            [
                cand(minted: (5, 55), slot: a),
                cand(minted: (200, 260), slot: nil)  // no slot → still one row
            ],
            [diagWithSlot(a), diagFail(.noCandidatePairs)]
        )
        #expect(r.count == 2)
        #expect(r[1].reason == .noCandidatePairs)
        #expect(r[1].slotStart == -1 && r[1].widthDeltaSec == 0)
    }
}

// MARK: - Slot-field source coverage (each reason)

@Suite("SpliceSlotShadow slot-field sources (playhead-xsdz.21)")
struct SpliceSlotShadowSlotFieldTests {

    @Test("gate-failure reasons source slot fields from bestGeometryValidPair")
    func gateFailuresUseBestGeometry() {
        for reason in [SpliceSlotDiagnostics.FailureReason.edgeBelowFloor,
                       .slotConfidenceBelowFloor, .coreCoverageBelowMinimum, .vetoNewlyEnclosed] {
            let champ = slot(12, 48, coverage: 0.7, startScore: 0.2, endScore: 0.3)
            let r = rows(
                [cand(minted: (15, 45), slot: nil)],
                [diagFail(reason, champion: champ)]
            )
            #expect(r[0].slotStart == 12 && r[0].slotEnd == 48)
            #expect(r[0].startEdgeScore == 0.2 && r[0].endEdgeScore == 0.3)
            #expect(r[0].coreCoverage == 0.7)
            #expect(!r[0].qualified)
            // widthDeltaSec is real for gate-failure rows: (48-12)-(45-15)=6.
            #expect(r[0].widthDeltaSec == 6)
        }
    }

    @Test("no-pair reasons emit sentinels only")
    func noPairReasonsSentinel() {
        for reason in [SpliceSlotDiagnostics.FailureReason.degenerateCore,
                       .noCandidatePairs, .durationOutOfRange] {
            let r = rows(
                [cand(minted: (10, 40), slot: nil)],
                [diagFail(reason)]
            )
            #expect(r[0].slotStart == -1 && r[0].slotEnd == -1)
            #expect(r[0].startEdgeScore == 0 && r[0].endEdgeScore == 0 && r[0].coreCoverage == 0)
            #expect(r[0].widthDeltaSec == 0)
        }
    }
}

// MARK: - Decision-delta computer

@Suite("SpliceSlotDecisionDeltaComputer (playhead-xsdz.21)")
struct SpliceSlotDecisionDeltaTests {

    private func entry(_ source: EvidenceSourceType, _ weight: Double) -> EvidenceLedgerEntry {
        EvidenceLedgerEntry(source: source, weight: weight, detail: .classifier(score: weight))
    }

    @Test("ledger mass sums strictly-positive scoring weights")
    func ledgerMass() {
        let ledger = [entry(.classifier, 0.4), entry(.lexical, 0.2), entry(.audioForensics, 0.1)]
        #expect(abs(SpliceSlotDecisionDeltaComputer.ledgerMass(ledger) - 0.7) < 1e-9)
    }

    @Test("distinctKinds counts distinct scoring sources")
    func distinctKinds() {
        let ledger = [entry(.classifier, 0.4), entry(.classifier, 0.1), entry(.lexical, 0.2)]
        #expect(SpliceSlotDecisionDeltaComputer.distinctKinds(ledger) == 2)
    }

    @Test("suppression removes every audioForensics entry")
    func suppression() {
        let ledger = [entry(.classifier, 0.4), entry(.audioForensics, 0.3), entry(.audioForensics, 0.1)]
        let suppressed = SpliceSlotDecisionDeltaComputer.suppressingAudioForensics(ledger)
        #expect(suppressed.count == 1)
        #expect(suppressed.allSatisfy { $0.source != .audioForensics })
    }

    @Test("make() records slot-arm mass BOTH with and without suppression")
    func makeBothArms() {
        let minted = [entry(.classifier, 0.5), entry(.lexical, 0.2)]
        let slotLedger = [entry(.classifier, 0.5), entry(.audioForensics, 0.3)]
        let delta = SpliceSlotDecisionDeltaComputer.make(
            mintedLedger: minted, mintedSkipConfidence: 0.71,
            slotLedger: slotLedger, slotSkipConfidence: 0.66
        )
        #expect(abs(delta.slotLedgerMassWithoutSuppression - 0.8) < 1e-9) // 0.5 + 0.3
        #expect(abs(delta.slotLedgerMassWithSuppression - 0.5) < 1e-9)    // audioForensics dropped
        #expect(delta.slotDistinctKinds == 1)                             // only .classifier survives
        #expect(abs(delta.mintedLedgerMass - 0.7) < 1e-9)
        #expect(delta.mintedDistinctKinds == 2)
        #expect(delta.slotSkipConfidence == 0.66 && delta.mintedSkipConfidence == 0.71)
    }
}

// MARK: - Projection

@Suite("SpliceSlotProjection substitution + disjointness (playhead-xsdz.21)")
struct SpliceSlotProjectionTests {

    private func input(
        _ minted: (Double, Double),
        _ reason: SpliceSlotShadowReason,
        slot: (Double, Double)? = nil
    ) -> SpliceSlotProjectionInput {
        SpliceSlotProjectionInput(
            mintedInterval: TimeRange(start: minted.0, end: minted.1),
            reason: reason,
            wouldBeSlot: slot.map { TimeRange(start: $0.0, end: $0.1) }
        )
    }

    @Test("only qualifying substitutes; absorbed removes; all else keeps minted")
    func substitutionRule() {
        let result = SpliceSlotProjection.project([
            input((15, 48), .qualifying, slot: (10, 50)),   // → [10,50]
            input((110, 140), .slotCollision),              // → keep minted
            input((35, 65), .absorbed, slot: (0, 100)),     // → removed
            input((30, 90), .noCandidatePairs)              // → keep minted
        ])
        #expect(result.treatmentIntervals == [
            TimeRange(start: 10, end: 50),
            TimeRange(start: 110, end: 140),
            TimeRange(start: 30, end: 90)
        ])
    }

    @Test("strong-inner-splice projection is pairwise disjoint (endpoint-touching OK)")
    func disjointStrongPod() {
        let result = SpliceSlotProjection.project([
            input((15, 48), .qualifying, slot: (10, 50)),
            input((52, 88), .qualifying, slot: (50, 90))    // touches at 50 → disjoint
        ])
        #expect(result.disjoint)
    }

    @Test("project(from: rows): weak pod → slot substitution + non-enclosed minted, absorbed removed, disjoint")
    func projectFromRows() {
        // Reuse the weak-pod fixture: A qualifying [0,100], B slotCollision
        // (minted [110,140] kept), C absorbed (removed).
        let a = slot(0, 100, coverage: 1.0)
        let b = slot(50, 150, coverage: 0.9)
        let c = slot(30, 70, coverage: 0.8)
        let podRows = rows(
            [
                cand(minted: (5, 95), slot: a),
                cand(minted: (110, 140), slot: b),
                cand(minted: (35, 65), slot: c)
            ],
            [diagWithSlot(a), diagWithSlot(b), diagWithSlot(c)]
        )
        let result = SpliceSlotProjection.project(from: podRows)
        #expect(result.treatmentIntervals == [
            TimeRange(start: 0, end: 100),      // A: qualifying → slot substitution
            TimeRange(start: 110, end: 140)     // B: slotCollision → minted kept
            // C absorbed → removed
        ])
        #expect(result.disjoint)
    }

    @Test("overlapping treatment intervals are flagged non-disjoint")
    func overlappingFlagged() {
        let result = SpliceSlotProjection.project([
            input((10, 60), .qualifying, slot: (10, 60)),
            input((50, 90), .noCandidatePairs)   // minted [50,90] overlaps [10,60]
        ])
        #expect(!result.disjoint)
    }
}
