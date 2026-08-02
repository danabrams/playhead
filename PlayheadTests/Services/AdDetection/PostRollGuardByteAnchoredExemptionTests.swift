// PostRollGuardByteAnchoredExemptionTests.swift
// playhead-sik9: the post-roll guard exempts a BYTE-ANCHORED inner edge.
//
// THE FIELD CASE. Dan's clean install, 2026-07-31, episode DE0784D8 (92 min).
// A `dayZeroRediffByteExact` post-roll at 5462.6 – 5522.7 on a 5522.65 s
// episode: confidence 1.00, 2-of-2 correct, boundary accuracy measured at 0.4 s
// against his own manual mark on the sibling pre-roll. Under
// `FusionWeightConfig.postRollGuardSeconds` as written (90 s, playhead-wraj)
// the equivalent FUSED span is demoted `.eligible → .markOnly` on the strength
// of its POSITION alone — the most certain signal in the system treated exactly
// like the least. Dan, 2026-08-01: "we do need a rediff exemption."
//
// WHAT THE EXEMPTION IS, PRECISELY. Not "rediff touched this episode" — the
// span's WIDTH must be owned by the byte-exact rediff oracle
// (`DecodedSpan.carriesRediffByteExactWidth`, i.e. `.rediffSlot` in
// `anchorProvenance`). A post-roll's INNER edge is its START; the outer edge is
// the episode end and is bounded by construction, so it cannot overrun into
// anything. The rediff differ is a WIDTH oracle and sets both edges together
// (`SpanExtentSupport.derive` stamps `.rediffByteExact` on both from that one
// test), so at the mapper "the start is byte-anchored" and "the width is
// rediff-owned" are one predicate.
//
// AN EXEMPTION THAT EXEMPTS EVERYTHING IS THE FAILURE MODE, so the NEGATIVE
// cases below are the real deliverable. `guardedTailShapes` is the reviewable
// list: every tail-span shape that is STILL demoted, in one table.

import AVFoundation
import Foundation
import Testing

@testable import Playhead

// MARK: - The DE0784D8 geometry, shared

enum PostRollGuardFieldCase {
    /// Episode duration on Dan's device (seconds).
    static let episodeDuration = 5522.65
    /// The byte-exact post-roll's INNER edge (its start) — the only edge that
    /// can cost show.
    static let postRollStart = 5462.6
    /// The OUTER edge. Note it exceeds `episodeDuration` by 0.05 s (the played
    /// timeline runs marginally past the feed's declared duration), so the
    /// guard's `episodeDuration - endTime <= 90` test is satisfied by -0.05.
    static let postRollEnd = 5522.7
}

/// THE REVIEWABLE LIST — every tail-span shape that is STILL demoted.
///
/// Each entry sits at the exact DE0784D8 tail geometry with the same at-floor
/// ledger, so the ONLY thing that differs from the exempt case is the WIDTH
/// provenance. Read this as the answer to "what did the exemption NOT cover?":
///
///   • `.classifierSeed`  — an UNANCHORED start. A seed landed there; the edge
///                          is invented. Exactly the case Dan's original
///                          2026-07-19 reasoning describes.
///   • `.spliceSlot`      — ACOUSTIC width. A width oracle, and it does set
///                          both edges, but it is not byte-exact
///                          (playhead-xsdz.15: recall 21%, ceiling 6%, stays
///                          OFF). The discriminating negative — the exemption
///                          keys on BYTE-exactness, not on "some oracle owned
///                          this width".
///   • `.evidenceCatalog` — a catalog identity match. Presence, not extent.
///   • `.userCorrection`  — a user-reported time. Presence, not extent.
///   • `[]`               — no provenance at all.
///
/// A playhead-9s6q SEGMENT-RECOVERED slot is the `.classifierSeed` / `[]` row
/// by construction; `PostRollGuardSegmentRecoveredReachabilityTests` proves it
/// can never arrive carrying `.rediffSlot`.
enum PostRollGuardTailShapes {
    static let stillGuarded: [(label: String, provenance: [AnchorRef])] = [
        ("unanchored start (classifier seed)", [.classifierSeed(regionId: "cs", score: 0.7)]),
        ("acoustic splice width", [.spliceSlot]),
        ("catalog identity", [.evidenceCatalog(entry: EvidenceEntry(
            evidenceRef: 1,
            category: .url,
            matchedText: "example dot com",
            normalizedText: "example.com",
            atomOrdinal: 900,
            startTime: PostRollGuardFieldCase.postRollStart,
            endTime: PostRollGuardFieldCase.postRollEnd
        ))]),
        ("user-reported time", [.userCorrection(correctionId: "c1", reportedTime: 5470)]),
        ("no provenance at all", []),
    ]
}

// MARK: - 1. The exemption at the guard site (DecisionMapper)

@Suite("Post-roll guard: byte-anchored inner edge is exempt (playhead-sik9)")
struct PostRollGuardByteAnchoredExemptionTests {

    /// A tail span at the DE0784D8 post-roll geometry with the given width
    /// provenance. Ordinals are constant across shapes so only the provenance
    /// varies between the positive and the negative cases.
    private func makeTailSpan(anchorProvenance: [AnchorRef]) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: "DE0784D8", firstAtomOrdinal: 900, lastAtomOrdinal: 940),
            assetId: "DE0784D8",
            firstAtomOrdinal: 900,
            lastAtomOrdinal: 940,
            startTime: PostRollGuardFieldCase.postRollStart,
            endTime: PostRollGuardFieldCase.postRollEnd,
            anchorProvenance: anchorProvenance
        )
    }

    /// skipConfidence ≈ 0.70 — BELOW `hostReadConfidenceFloor` (0.9). Used for
    /// the rediff cases so the floor would also demote a non-rediff span: it
    /// proves the post-roll guard is not the only thing being bypassed, and
    /// keeps the positive case honest about which half of wraj is under test.
    private func belowFloorLedger() -> [EvidenceLedgerEntry] {
        [
            .init(source: .classifier, weight: 0.30, detail: .classifier(score: 0.7)),
            .init(source: .lexical, weight: 0.20, detail: .lexical(matchedCategories: ["url"])),
            .init(source: .acoustic, weight: 0.20, detail: .acoustic(breakStrength: 0.7)),
        ]
    }

    /// skipConfidence == 0.90 bit-exactly — AT the floor, so `0.9 < 0.9` is
    /// false and the host-read floor cannot fire. Any `.markOnly` produced with
    /// this ledger is the post-roll guard's doing and nothing else's.
    private func atFloorLedger() -> [EvidenceLedgerEntry] {
        [.init(source: .classifier, weight: 0.90, detail: .classifier(score: 0.9))]
    }

    private func mapTail(
        anchorProvenance: [AnchorRef],
        ledger: [EvidenceLedgerEntry],
        certaintyTieredEnabled: Bool = true,
        episodeDuration: Double? = PostRollGuardFieldCase.episodeDuration
    ) -> DecisionResult {
        DecisionMapper(
            span: makeTailSpan(anchorProvenance: anchorProvenance),
            ledger: ledger,
            config: FusionWeightConfig(certaintyTieredEnabled: certaintyTieredEnabled),
            transcriptQuality: .good,
            episodeDuration: episodeDuration
        ).map()
    }

    // MARK: The positive — Dan's post-roll survives the flip

    /// THE BEAD. With wraj ON, the byte-exact post-roll keeps `.eligible`.
    /// Against main this returns `.markOnly`.
    @Test("THE BEAD: the DE0784D8 byte-exact post-roll stays eligible with wraj ON")
    func byteExactPostRollStaysEligible() {
        let result = mapTail(anchorProvenance: [.rediffSlot], ledger: atFloorLedger())
        #expect(result.eligibilityGate == .eligible,
                "a byte-exact rediff tail must not be demoted by POSITION alone")
    }

    /// The same span BELOW the host-read floor. Both halves of wraj exempt a
    /// rediff-anchored span, so a low presence score does not resurrect the
    /// demotion — the certainty is in the bytes, not in the ledger sum.
    @Test("the byte-exact post-roll stays eligible even below the host-read floor")
    func byteExactPostRollStaysEligibleBelowFloor() {
        let result = mapTail(anchorProvenance: [.rediffSlot], ledger: belowFloorLedger())
        #expect(result.eligibilityGate == .eligible)
        #expect(abs(result.skipConfidence - 0.70) < 0.001,
                "declining to demote must not touch the presence score")
    }

    /// The demotion contract is unchanged in the other direction too: not
    /// demoting must not inflate either confidence.
    @Test("the exemption never modifies proposalConfidence or skipConfidence")
    func exemptionLeavesScoresUntouched() {
        let exempt = mapTail(anchorProvenance: [.rediffSlot], ledger: atFloorLedger())
        let guarded = mapTail(anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.7)],
                              ledger: atFloorLedger())
        #expect(abs(exempt.proposalConfidence - guarded.proposalConfidence) < 1e-12)
        #expect(abs(exempt.skipConfidence - guarded.skipConfidence) < 1e-12)
        #expect(exempt.eligibilityGate != guarded.eligibilityGate,
                "only ACTIONABILITY differs between the exempt and guarded shapes")
    }

    // MARK: The negatives — which tail spans are STILL guarded

    /// The reviewable negative list lives in `PostRollGuardTailShapes
    /// .stillGuarded` (file scope, so the `@Test` attribute can reference it).
    /// Every shape there must still demote at the exact DE0784D8 tail geometry.
    @Test(
        "STILL GUARDED: a tail whose width is not byte-derived demotes to markOnly",
        arguments: PostRollGuardTailShapes.stillGuarded
    )
    func nonByteAnchoredTailStillDemotes(shape: (label: String, provenance: [AnchorRef])) {
        let result = mapTail(anchorProvenance: shape.provenance, ledger: atFloorLedger())
        #expect(result.eligibilityGate == .markOnly,
                "\(shape.label): the post-roll guard must still fire — the inner edge is not byte-anchored")
    }

    /// The single most important negative, spelled out on its own because it is
    /// the one a wrong implementation gets wrong. `.spliceSlot` is a width
    /// oracle that stamps both edges of its span, so an implementation that
    /// exempted "spans whose width some oracle owns" would pass every other
    /// test in this file and fail this one.
    @Test("STILL GUARDED: acoustic .spliceSlot width is NOT byte-exact and stays demoted")
    func spliceOwnedTailIsNotExempt() {
        let span = makeTailSpan(anchorProvenance: [.spliceSlot])
        #expect(!span.carriesRediffByteExactWidth,
                "splice width is acoustic; the byte-exact predicate must reject it")
        #expect(mapTail(anchorProvenance: [.spliceSlot], ledger: atFloorLedger())
            .eligibilityGate == .markOnly)
    }

    /// Corroborating provenance alongside `.rediffSlot` does NOT revoke the
    /// exemption — the rediff differ still owns the width, and this matches the
    /// four shipped `carriesRediffByteExactWidth` carve-outs exactly. Pinned so
    /// the behaviour is visible rather than incidental.
    @Test("rediff width plus other provenance is still exempt (the width owner decides)")
    func rediffPlusOtherProvenanceStaysExempt() {
        let mixed: [AnchorRef] = [
            .classifierSeed(regionId: "cs", score: 0.7), .spliceSlot, .rediffSlot,
        ]
        #expect(mapTail(anchorProvenance: mixed, ledger: atFloorLedger())
            .eligibilityGate == .eligible)
    }

    // MARK: Invariants the exemption must not break

    /// The exemption only ever DECLINES to demote. It must never lift a gate
    /// that something else already blocked.
    @Test("the exemption never PROMOTES a blocked rediff tail")
    func exemptionNeverPromotesBlockedGate() {
        // FM-acoustic provenance with only an FM ledger entry →
        // `.blockedByEvidenceQuorum` out of `computeGate()`. Adding
        // `.rediffSlot` makes the span exempt from the post-roll guard, which
        // must leave the harder block completely untouched.
        let span = makeTailSpan(anchorProvenance: [
            .fmAcousticCorroborated(regionId: "r2", breakStrength: 0.7), .rediffSlot,
        ])
        let result = DecisionMapper(
            span: span,
            ledger: [.init(source: .fm, weight: 0.35,
                           detail: .fm(disposition: .containsAd, band: .moderate, cohortPromptLabel: "v1"))],
            config: FusionWeightConfig(certaintyTieredEnabled: true),
            transcriptQuality: .good,
            episodeDuration: PostRollGuardFieldCase.episodeDuration
        ).map()
        #expect(result.eligibilityGate == .blockedByEvidenceQuorum)
    }

    /// A music-only rediff tail is demoted by the UNCONDITIONAL playhead-xtpf
    /// music demotion, which this exemption does not touch. `.markOnly` is a
    /// fixed point and the demoters compose commutatively; the exemption must
    /// not disturb that.
    @Test("the exemption does not disarm the unconditional music-only demotion")
    func exemptionDoesNotDisarmMusicOnlyDemotion() {
        let result = mapTail(
            anchorProvenance: [.sustainedMusicOffset(regionId: "m1", confidence: 0.8), .rediffSlot],
            ledger: atFloorLedger()
        )
        #expect(result.eligibilityGate == .markOnly,
                "music-only provenance is a TARGETING signal; byte width does not make it a verdict")
    }

    /// Flag OFF stays byte-identical: with `certaintyTieredEnabled == false`
    /// neither the guard nor its new exemption exists, so both shapes are
    /// `.eligible` exactly as before this bead.
    @Test("flag OFF is byte-identical for both the exempt and the guarded shape")
    func flagOffIsByteIdentical() {
        for shape in [[AnchorRef.rediffSlot], [.classifierSeed(regionId: "cs", score: 0.7)]] {
            let result = mapTail(
                anchorProvenance: shape, ledger: atFloorLedger(), certaintyTieredEnabled: false
            )
            #expect(result.eligibilityGate == .eligible)
        }
    }

    /// The guard's other inert conditions are untouched — a byte-exact span
    /// OUTSIDE the window was already eligible and stays so, which proves the
    /// exemption did not accidentally become the only reason it survives.
    @Test("a byte-exact span outside the 90s window is eligible for the ORIGINAL reason too")
    func byteExactOutsideWindowIsUnaffected() {
        let outside = mapTail(
            anchorProvenance: [.rediffSlot], ledger: atFloorLedger(),
            episodeDuration: PostRollGuardFieldCase.postRollEnd + 200
        )
        let guarded = mapTail(
            anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.7)],
            ledger: atFloorLedger(),
            episodeDuration: PostRollGuardFieldCase.postRollEnd + 200
        )
        #expect(outside.eligibilityGate == .eligible)
        #expect(guarded.eligibilityGate == .eligible,
                "outside the window the guard is inert for EVERY shape, exempt or not")
    }

    /// The exemption is not smuggling in a duration default: an unknown episode
    /// duration still leaves the guard inert for the guarded shape, which is
    /// the pre-existing "never guess the episode end" contract.
    @Test("unknown episode duration keeps the guard inert for the guarded shape too")
    func unknownDurationStaysInert() {
        let result = mapTail(
            anchorProvenance: [.classifierSeed(regionId: "cs", score: 0.7)],
            ledger: atFloorLedger(),
            episodeDuration: nil
        )
        #expect(result.eligibilityGate == .eligible)
    }
}

// MARK: - 2. playhead-9s6q segment-recovered slots cannot reach the exemption

/// The acceptance asks that a SEGMENT-RECOVERED tail slot still demote. It does,
/// and the reason is structural rather than a threshold: such a slot never
/// acquires `.rediffSlot` width provenance on the path the guard sits on, and on
/// the path where it CAN be minted it is stamped `.unanchored` + `.markOnly`
/// before any mapper sees it. These tests pin both halves, because "it demotes"
/// is only trustworthy if the reason it demotes is a property of the producer
/// rather than an accident of the fixture.
@Suite("Segment-recovered slots never reach the byte-anchored exemption (playhead-sik9)")
struct PostRollGuardSegmentRecoveredReachabilityTests {

    /// The LAGGED path — the only producer that stamps `.rediffSlot` on a
    /// DECODED SPAN, and therefore the only way a span can reach
    /// `DecisionMapper` carrying the exemption key. It calls
    /// `gateAndDiffBytes(alignment:)` with no `recoverNonMonotonicSegments`
    /// argument, so the `false` default applies and a non-monotonic alignment
    /// is rejected WHOLESALE: no acceptance, no played slots, nothing to stamp.
    @Test("the lagged byte path's default rejects a non-monotonic alignment wholesale")
    func laggedPathRejectsNonMonotonicUnderItsDefault() {
        // Swapped halves: the same bytes in the opposite order, which is the
        // canonical non-monotonic structure (a run chains backwards).
        let x = SyntheticMP3.frames(count: 20, seed: 20)
        let y = SyntheticMP3.frames(count: 20, seed: 21)
        let alignment = RediffByteAligner.align(
            aData: SyntheticMP3.file(x + y),
            bData: SyntheticMP3.file(y + x),
            config: SyntheticMP3.smallRunConfig
        )
        #expect(!alignment.monotonicClean)

        // EXACTLY the lagged call — default argument, as in
        // `AdDetectionService.computeByteAlignedPlayedSlots`.
        let outcome = RediffSlotOwnership.gateAndDiffBytes(alignment: alignment)
        guard case .rejectedNonMonotonic = outcome else {
            Issue.record("the lagged default must reject non-monotonic, got \(outcome)")
            return
        }
        // Pin that the default IS the strict arm rather than incidentally
        // agreeing with it: passing the argument explicitly must be identical.
        #expect(outcome == RediffSlotOwnership.gateAndDiffBytes(
            alignment: alignment, recoverNonMonotonicSegments: false
        ))
    }

    /// The DAY-0 path, which DOES opt into segment recovery. Its own
    /// classification (`strictByteExactMask`) is what separates the two
    /// certainties, and a slot no monotonic-clean persona reproduced is not
    /// strict — so the mint stamps it `.unanchored` and `.markOnly`
    /// (`AdDetectionService.mintByteExactDayZeroMarks`), never `.eligible`.
    @Test("a day-0 slot no strict persona reproduced is classified non-strict")
    func dayZeroSegmentRecoveredSlotIsNotStrict() {
        let recovered = [[RediffSlotOwnership.PlayedSlot(
            startSeconds: PostRollGuardFieldCase.postRollStart,
            endSeconds: PostRollGuardFieldCase.postRollEnd,
            leftRunSeconds: 300,
            rightRunSeconds: 300
        )]]
        let unioned = RediffSlotOwnership.unionedPlayedSlots(recovered)
        let mask = RediffSlotOwnership.strictByteExactMask(
            unioned: unioned, strictPerBSideSlots: []
        )
        #expect(mask == [false],
                "no monotonic-clean persona reproduced this tail slot → not strict → unanchored + markOnly")
    }
}

// MARK: - 3. Composition — the exemption cannot outrank playhead-2350

/// playhead-2350 still holds INDEPENDENTLY: an unanchored edge cannot auto-skip
/// at all. The exemption only declines to apply the post-roll demotion inside
/// `DecisionMapper`; the extent block runs afterwards in `runBackfill`'s
/// emission loop and is untouched. The case that matters is a rediff-width span
/// whose GEOMETRY was rewritten by the finalizer — `.rediffSlot` survives the
/// rewrite but the edge claims do not.
@Suite("The byte-anchored exemption never outranks the unanchored-edge block (playhead-sik9)")
struct PostRollGuardExemptionRespects2350Tests {

    @Test("a geometry-REWRITTEN rediff span derives unanchored edges despite .rediffSlot")
    func geometryRewriteRevokesTheEdgeClaim() {
        let support = SpanExtentSupport.derive(
            anchorProvenance: [.rediffSlot], stingerTrace: nil, geometryWasRewritten: true
        )
        #expect(support == .unanchored)
        #expect(!support.isFullyAnchored)
    }

    @Test("and playhead-2350 demotes it downstream, so the exemption cannot leak a skip")
    func unanchoredExtentStillDemotesAnExemptDecision() {
        // The verdict the exempted span carries out of `DecisionMapper`.
        let exempt = DecisionResult(
            proposalConfidence: 0.9, skipConfidence: 0.9, eligibilityGate: .eligible
        )
        let rewritten = SpanExtentSupport.derive(
            anchorProvenance: [.rediffSlot], stingerTrace: nil, geometryWasRewritten: true
        )
        let blocked = exempt.withExtentSupport(rewritten, blockingUnanchoredAutoSkip: true)
        #expect(blocked.eligibilityGate == .markOnly,
                "an unanchored edge blocks auto-skip regardless of what the guard declined to do")
        #expect(abs(blocked.skipConfidence - 0.9) < 1e-12, "presence stays honest")
    }

    @Test("an intact rediff span keeps both byte-exact edges and survives 2350")
    func intactRediffSpanSurvivesTheExtentBlock() {
        let support = SpanExtentSupport.derive(
            anchorProvenance: [.rediffSlot], stingerTrace: nil, geometryWasRewritten: false
        )
        #expect(support.startAnchor == .rediffByteExact,
                "the INNER edge of a post-roll — the one that can cost show")
        #expect(support.endAnchor == .rediffByteExact)
        let exempt = DecisionResult(
            proposalConfidence: 0.9, skipConfidence: 0.9, eligibilityGate: .eligible
        )
        #expect(exempt.withExtentSupport(support, blockingUnanchoredAutoSkip: true)
            .eligibilityGate == .eligible)
    }
}

// MARK: - 4. The surface — an exempt post-roll produces a skip cue

/// The acceptance's other half: `.eligible` has to actually become a SKIP. This
/// drives the real `SkipOrchestrator` with the DE0784D8 post-roll geometry and
/// asserts a cue is pushed — and, in the negative, that the demoted counterpart
/// pushes none. Without this pair "stays eligible" would be a claim about a
/// field on a struct rather than about the user's experience.
@Suite("An exempt byte-exact post-roll reaches the cue surface (playhead-sik9)")
struct PostRollGuardExemptCueSurfacingTests {

    private static func cueStart(_ cue: CMTimeRange) -> Double { CMTimeGetSeconds(cue.start) }
    private static func cueEnd(_ cue: CMTimeRange) -> Double {
        CMTimeGetSeconds(cue.start + cue.duration)
    }

    /// Push the DE0784D8 post-roll through the orchestrator at the given gate
    /// and edge anchors, and return whatever cues it emitted.
    private static func pushedCues(
        gate: SkipEligibilityGate,
        edgeAnchor: AutoSkipEdgeAnchor
    ) async throws -> [CMTimeRange] {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(
            episodeDurationSec: PostRollGuardFieldCase.episodeDuration
        ))
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        nonisolated(unsafe) var cues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { cues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )
        await orchestrator.receiveAdWindows([
            makeSkipTestAdWindow(
                id: "sik9-postroll",
                startTime: PostRollGuardFieldCase.postRollStart,
                endTime: PostRollGuardFieldCase.postRollEnd,
                confidence: 1.0,
                decisionState: "confirmed",
                startEdgeAnchor: edgeAnchor.rawValue,
                endEdgeAnchor: edgeAnchor.rawValue,
                eligibilityGate: gate.rawValue
            )
        ])
        return cues
    }

    /// THE ACCEPTANCE, at the surface. The exempted post-roll produces a cue,
    /// and it covers the tail rather than reaching back into the show: the cue
    /// starts at or after the byte-exact inner edge.
    @Test("an eligible byte-exact post-roll produces a skip cue that never precedes its inner edge")
    func exemptPostRollProducesACue() async throws {
        let cues = try await Self.pushedCues(gate: .eligible, edgeAnchor: .rediffByteExact)
        let cue = try #require(cues.first, "an eligible byte-exact post-roll must produce a skip cue")
        #expect(Self.cueStart(cue) >= PostRollGuardFieldCase.postRollStart,
                "the cue must never begin BEFORE the byte-anchored inner edge — that is the show")
        #expect(Self.cueEnd(cue) <= PostRollGuardFieldCase.postRollEnd,
                "and never past the outer edge")
        #expect(Self.cueEnd(cue) > Self.cueStart(cue), "a real, non-degenerate skip")
    }

    /// The negative at the same surface: the gate the guard WOULD have produced
    /// emits nothing. This is what flipping wraj costs Dan without the
    /// exemption — the same window, one field different, zero skips.
    @Test("the markOnly counterpart produces NO cue — what the un-exempted guard costs")
    func demotedPostRollProducesNoCue() async throws {
        let cues = try await Self.pushedCues(gate: .markOnly, edgeAnchor: .rediffByteExact)
        #expect(cues.isEmpty,
                "markOnly surfaces as a suggest banner, never a cue")
    }
}
