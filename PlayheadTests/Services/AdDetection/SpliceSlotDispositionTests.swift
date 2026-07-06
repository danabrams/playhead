// SpliceSlotDispositionTests.swift
// playhead-xsdz.20 (Bead B): unit tests for the PURE disposition engine
// (passes 2–4) and the PURE pass-5 rewriter. These exercise the pinned
// interval / pass-order / fixpoint / absorption semantics directly against the
// side-effect-free functions bead C's shadow and the flag-ON path both consume —
// no acoustics, no store, no actor.

import Foundation
import Testing

@testable import Playhead

// MARK: - Builders

private func edge(_ t: Double) -> SpliceEdgeEvidence {
    SpliceEdgeEvidence(time: t, stepScore: 0.5, contributingSignals: 1)
}

private func slot(_ start: Double, _ end: Double, coverage: Double = 1.0) -> SpliceSlot {
    SpliceSlot(
        startTime: start,
        endTime: end,
        startEdge: edge(start),
        endEdge: edge(end),
        slotConfidence: 0.5,
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

/// Assert every kept slot in the result is pairwise DISJOINT (positive-duration)
/// from every other kept slot — the composition guarantee.
private func assertKeptSlotsPairwiseDisjoint(
    _ result: SpliceSlotDispositionResult,
    sourceLocation: SourceLocation = #_sourceLocation
) {
    var keptRanges: [TimeRange] = []
    for disp in result.dispositions {
        if case .keepSlot(let s) = disp {
            keptRanges.append(TimeRange(start: s.startTime, end: s.endTime))
        }
    }
    for a in 0..<keptRanges.count {
        for b in (a + 1)..<keptRanges.count {
            #expect(
                !keptRanges[a].intersects(keptRanges[b]),
                "kept slots \(keptRanges[a]) and \(keptRanges[b]) positive-overlap",
                sourceLocation: sourceLocation
            )
        }
    }
}

private func isKeep(_ d: SpliceSlotDisposition) -> Bool {
    if case .keepSlot = d { return true }
    return false
}

// MARK: - Engine tests

@Suite("SpliceSlotDispositionEngine (playhead-xsdz.20)")
struct SpliceSlotDispositionEngineTests {

    @Test("strong-inner-splice pod: slots touching at a shared endpoint are DISJOINT → BOTH kept")
    func strongInnerSplicePodBothKept() {
        // slot1=[10,50], slot2=[50,90] share the bitwise-identical break 50.
        let candidates = [
            cand(minted: (15, 48), slot: slot(10, 50)),
            cand(minted: (52, 88), slot: slot(50, 90))
        ]
        let r = SpliceSlotDispositionEngine.computeDispositions(candidates)
        #expect(isKeep(r.dispositions[0]))
        #expect(isKeep(r.dispositions[1]))
        #expect(r.fixpointRounds == 0)
        assertKeptSlotsPairwiseDisjoint(r)
    }

    @Test("overshoot pod: kept slots' own minted extents are excluded from comparison → no phantom demotion")
    func overshootPodBothKept() {
        // span0's minted OVERSHOOTS the shared break 50 by 5s into slot2's region,
        // but it belongs to a KEPT slot so it is excluded from the comparison set.
        let candidates = [
            cand(minted: (15, 55), slot: slot(10, 50)),
            cand(minted: (52, 88), slot: slot(50, 90))
        ]
        let r = SpliceSlotDispositionEngine.computeDispositions(candidates)
        #expect(isKeep(r.dispositions[0]))
        #expect(isKeep(r.dispositions[1]))
        assertKeptSlotsPairwiseDisjoint(r)
    }

    @Test("greedy collision chain A[0,60] B[50,120] C[110,180]: keep A + C, demote B")
    func greedyChainKeepAandC() {
        let candidates = [
            cand(minted: (5, 55), slot: slot(0, 60, coverage: 1.0)),    // A ranks first
            cand(minted: (65, 105), slot: slot(50, 120, coverage: 0.9)), // B free-floating minted
            cand(minted: (115, 175), slot: slot(110, 180, coverage: 0.8))
        ]
        let r = SpliceSlotDispositionEngine.computeDispositions(candidates)
        #expect(isKeep(r.dispositions[0]))            // A kept
        #expect(r.dispositions[1] == .demoted(.greedyCollision)) // B demoted
        #expect(isKeep(r.dispositions[2]))            // C kept (disjoint from A)
        assertKeptSlotsPairwiseDisjoint(r)
    }

    @Test("pass order: A+B slots collide, A ranks first but A is vetoed (pass 2) → B keeps (pass 3)")
    func passOrderVetoBeforeGreedy() {
        // If greedy (pass 3) ran before veto (pass 2), A (higher coverage) would
        // win the collision and demote B. Veto removes A first, so B survives.
        let candidates = [
            cand(minted: (12, 30), slot: slot(10, 60, coverage: 1.0), coreMatch: true), // A vetoed
            cand(minted: (45, 85), slot: slot(40, 90, coverage: 0.9))                    // B
        ]
        let r = SpliceSlotDispositionEngine.computeDispositions(candidates)
        #expect(r.dispositions[0] == .demoted(.negativeBankVeto))
        #expect(isKeep(r.dispositions[1]))
        assertKeptSlotsPairwiseDisjoint(r)
    }

    @Test("negative-bank veto discards on EITHER slot-token OR core-token match; neither → allowed; dormant → allowed")
    func negativeBankVetoEitherMatches() {
        let candidates = [
            cand(minted: (10, 40), slot: slot(5, 45), slotMatch: true),  // slot-only match
            cand(minted: (60, 90), slot: slot(55, 95), coreMatch: true), // core-only match
            cand(minted: (110, 140), slot: slot(105, 145)),              // neither → allowed
            cand(minted: (160, 190), slot: slot(155, 195), coreMatch: false, slotMatch: false) // dormant → allowed
        ]
        let r = SpliceSlotDispositionEngine.computeDispositions(candidates)
        #expect(r.dispositions[0] == .demoted(.negativeBankVeto))
        #expect(r.dispositions[1] == .demoted(.negativeBankVeto))
        #expect(isKeep(r.dispositions[2]))
        #expect(isKeep(r.dispositions[3]))
    }

    @Test("empty-atom-set disqualification runs pre-pass-3 and its minted interval STAYS in comparison sets")
    func emptyAtomSetPrePass3KeepsMintedInComparison() {
        // E's slot has no atoms → disqualified pre-pass-3. E is non-kept, so its
        // MINTED interval remains in the comparison set and partially overlaps K's
        // slot in pass 4 — demoting K. (If the empty check ran at rewrite time,
        // E's minted would have been dropped and K would have survived.)
        let candidates = [
            cand(minted: (15, 55), slot: slot(10, 60, coverage: 1.0), intersects: false), // E: empty slot
            cand(minted: (55, 85), slot: slot(50, 90, coverage: 0.9))                       // K
        ]
        let r = SpliceSlotDispositionEngine.computeDispositions(candidates)
        #expect(r.dispositions[0] == .demoted(.emptyAtomSet))
        #expect(r.dispositions[1] == .demoted(.mintedOverlap))
    }

    @Test("encloses-and-clips: S clips a non-kept minted (partial overlap) → S demoted, X survives, enclosed P NOT absorbed")
    func enclosesAndClipsSDemotedXSurvives() {
        let candidates = [
            cand(minted: (15, 55), slot: slot(10, 60)), // S: encloses P AND clips X
            cand(minted: (50, 80), slot: nil),          // X: partial overlap with S slot
            cand(minted: (20, 40), slot: nil)           // P: enclosed by S slot
        ]
        let r = SpliceSlotDispositionEngine.computeDispositions(candidates)
        #expect(r.dispositions[0] == .demoted(.mintedOverlap)) // S demoted by clip
        #expect(r.dispositions[1] == .noSlot)                  // X survives
        #expect(r.dispositions[2] == .noSlot)                  // P NOT absorbed (S demoted)
    }

    @Test("enclosure absorb: a kept slot absorbs an enclosed bank-clean span")
    func enclosureAbsorb() {
        let candidates = [
            cand(minted: (15, 95), slot: slot(10, 100)), // K
            cand(minted: (30, 50), slot: nil)            // P enclosed, clean
        ]
        let r = SpliceSlotDispositionEngine.computeDispositions(candidates)
        #expect(isKeep(r.dispositions[0]))
        #expect(r.dispositions[1] == .absorbed(absorberIndex: 0))
    }

    @Test("absorbee bank check is ALL-OR-NOTHING: one clean + one matched enclosed → slot demoted, both survive, nothing absorbed")
    func absorbeeBankAllOrNothing() {
        let candidates = [
            cand(minted: (15, 95), slot: slot(10, 100)),        // K encloses both
            cand(minted: (20, 30), slot: nil, coreMatch: false), // P_clean
            cand(minted: (40, 50), slot: nil, coreMatch: true)   // P_matched
        ]
        let r = SpliceSlotDispositionEngine.computeDispositions(candidates)
        #expect(r.dispositions[0] == .demoted(.absorbeeBankMatch))
        #expect(r.dispositions[1] == .noSlot) // survives minted
        #expect(r.dispositions[2] == .noSlot) // survives minted (suppression runs downstream)
    }

    // Shared fixture: K1 encloses a bank-matched Q (demotes K1 in round 1); K1's
    // minted OVERSHOOTS into K2's slot, so once re-introduced it demotes K2 in
    // round 2. K2 would have absorbed the clean P — deferral keeps P alive.
    private func reintroductionFixture() -> [SpliceSlotCandidate] {
        [
            cand(minted: (15, 70), slot: slot(10, 50, coverage: 1.0)),  // K1 (overshoots to 70)
            cand(minted: (65, 115), slot: slot(60, 120, coverage: 0.9)), // K2
            cand(minted: (70, 90), slot: nil, coreMatch: false),         // P clean, enclosed by K2 slot
            cand(minted: (20, 40), slot: nil, coreMatch: true)           // Q matched, enclosed by K1 slot
        ]
    }

    @Test("fixpoint re-introduction converges in exactly 2 rounds")
    func fixpointReintroductionTwoRounds() {
        let candidates = reintroductionFixture()
        let r = SpliceSlotDispositionEngine.computeDispositions(candidates)
        #expect(r.dispositions[0] == .demoted(.absorbeeBankMatch)) // K1 round 1
        #expect(r.dispositions[1] == .demoted(.mintedOverlap))     // K2 round 2 (by re-introduced K1 minted)
        #expect(r.fixpointRounds == 2)
    }

    @Test("absorber demoted after enclosing: the would-be absorbee P SURVIVES with its prior-pass disposition")
    func absorberDemotedEnclosedSurvives() {
        let candidates = reintroductionFixture()
        let r = SpliceSlotDispositionEngine.computeDispositions(candidates)
        // P was enclosed by K2 and would have been absorbed; K2 is demoted, so P
        // reverts to its prior (noSlot) disposition rather than being dropped.
        #expect(r.dispositions[2] == .noSlot)
        #expect(r.dispositions[3] == .noSlot) // Q also survives minted
    }

    @Test("no-slot spans pass through as .noSlot and are inert to the passes")
    func noSlotPassThrough() {
        let candidates = [
            cand(minted: (0, 30), slot: nil),
            cand(minted: (100, 130), slot: slot(95, 135))
        ]
        let r = SpliceSlotDispositionEngine.computeDispositions(candidates)
        #expect(r.dispositions[0] == .noSlot)
        #expect(isKeep(r.dispositions[1]))
    }
}

// MARK: - Rewriter tests

private func atomEv(_ ordinal: Int, _ start: Double, _ end: Double) -> AtomEvidence {
    AtomEvidence(
        atomOrdinal: ordinal,
        startTime: start,
        endTime: end,
        isAnchored: true,
        anchorProvenance: [.classifierSeed(regionId: "r", score: 0.9)],
        hasAcousticBreakHint: false,
        correctionMask: .none
    )
}

private func decodedSpan(
    id: String? = nil,
    assetId: String = "asset-x",
    first: Int,
    last: Int,
    start: Double,
    end: Double,
    provenance: [AnchorRef] = [.classifierSeed(regionId: "r", score: 0.9)]
) -> DecodedSpan {
    DecodedSpan(
        id: id ?? DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: first, lastAtomOrdinal: last),
        assetId: assetId,
        firstAtomOrdinal: first,
        lastAtomOrdinal: last,
        startTime: start,
        endTime: end,
        anchorProvenance: provenance
    )
}

@Suite("SpliceSlotRewriter (playhead-xsdz.20)")
struct SpliceSlotRewriterTests {

    // Atom stream: ordinals 0..4 at [0,10),[10,20),...
    private var atoms: [AtomEvidence] {
        (0..<5).map { atomEv($0, Double($0) * 10, Double($0) * 10 + 10) }
    }

    @Test("unchanged shape: slot intersects the same ordinals → makeId unchanged, .spliceSlot appended, no superseded row")
    func rewriteUnchangedShape() {
        // Span covers ordinals 1..3 (times [10,40)). Slot = [12,38] intersects
        // exactly ordinals 1,2,3 → same first/last → same makeId.
        let span = decodedSpan(first: 1, last: 3, start: 10, end: 40)
        let result = SpliceSlotRewriter.apply(
            decodedSpans: [span],
            dispositions: [.keepSlot(slot(12, 38))],
            atomEvidence: atoms
        )
        #expect(result.finalSpans.count == 1)
        let rewritten = result.finalSpans[0]
        #expect(rewritten.id == span.id)                 // makeId unchanged
        #expect(rewritten.firstAtomOrdinal == 1)
        #expect(rewritten.lastAtomOrdinal == 3)
        #expect(rewritten.startTime == 12)
        #expect(rewritten.endTime == 38)
        #expect(rewritten.anchorProvenance.contains(.spliceSlot))
        #expect(result.supersededIds.isEmpty)
        #expect(result.absorbedIds.isEmpty)
    }

    @Test("changed shape: wider slot intersects more ordinals → new makeId, old id superseded")
    func rewriteChangedShape() {
        // Span covers ordinals 2..2 (times [20,30)). Slot = [5,45] intersects
        // ordinals 0..4 → new first/last → new makeId; old id superseded.
        let span = decodedSpan(first: 2, last: 2, start: 20, end: 30)
        let result = SpliceSlotRewriter.apply(
            decodedSpans: [span],
            dispositions: [.keepSlot(slot(5, 45))],
            atomEvidence: atoms
        )
        let rewritten = result.finalSpans[0]
        #expect(rewritten.firstAtomOrdinal == 0)
        #expect(rewritten.lastAtomOrdinal == 4)
        #expect(rewritten.id != span.id)
        #expect(rewritten.id == DecodedSpan.makeId(assetId: "asset-x", firstAtomOrdinal: 0, lastAtomOrdinal: 4))
        #expect(rewritten.anchorProvenance.contains(.spliceSlot))
        #expect(result.supersededIds == [span.id])
    }

    @Test("absorbed span: dropped from finalSpans, id in both absorbedIds and supersededIds")
    func rewriteAbsorbedRowAbsent() {
        let keeper = decodedSpan(first: 0, last: 4, start: 0, end: 50)
        let absorbee = decodedSpan(first: 2, last: 2, start: 20, end: 30)
        let result = SpliceSlotRewriter.apply(
            decodedSpans: [keeper, absorbee],
            dispositions: [.keepSlot(slot(0, 50)), .absorbed(absorberIndex: 0)],
            atomEvidence: atoms
        )
        let finalIds = Set(result.finalSpans.map(\.id))
        #expect(!finalIds.contains(absorbee.id))
        #expect(result.absorbedIds.contains(absorbee.id))
        #expect(result.supersededIds.contains(absorbee.id))
    }

    @Test("minted / demoted / noSlot spans are carried through verbatim")
    func rewriteMintedUnchanged() {
        let a = decodedSpan(first: 0, last: 1, start: 0, end: 20)
        let b = decodedSpan(first: 3, last: 4, start: 30, end: 50)
        let result = SpliceSlotRewriter.apply(
            decodedSpans: [a, b],
            dispositions: [.demoted(.greedyCollision), .noSlot],
            atomEvidence: atoms
        )
        #expect(result.finalSpans.count == 2)
        #expect(result.finalSpans[0] == a)
        #expect(result.finalSpans[1] == b)
        #expect(result.supersededIds.isEmpty)
        #expect(result.absorbedIds.isEmpty)
        #expect(!result.finalSpans[0].anchorProvenance.contains(.spliceSlot))
    }
}
