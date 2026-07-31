// RediffDayZeroAutoSkipPromotionTests.swift
// playhead-qs0d: promote the DAY-0 byte-exact rediff mark from mark-only to
// AUTO-SKIP — and first clear the blocker that made every downstream step
// inert.
//
// THE BLOCKER, measured on the owner's clean install 2026-07-31 (episode
// DE0784D8). Both `dayZeroRediffByteExact` windows persisted with
//
//     startEdgeAnchor = unanchored   endEdgeAnchor = unanchored
//
// — the SAME pair carried by the 0.40-confidence `segmentAggregated` windows
// that skipped 210 s of show the same afternoon. So the playhead-2350 gate held
// a 1.00-confidence, sample-accurate boundary at mark-only: correctly, given
// what it could see. `unanchored` there meant "nobody wrote an anchor", NOT
// "the boundary is unknown" — this repo's recurring defect shape. The byte
// divergence IS the DAI splice; nothing recorded it.
//
// Same episode, same afternoon:
//     dayZeroRediffByteExact  2 windows, conf 1.00, OUTER edges  → 2/2 CORRECT, skipped NOTHING
//     segmentAggregated       3 windows, conf 0.40–0.42, INNER   → 0/3, ALL SKIPPED, 210 s lost
//
// The pipeline skipped exactly the wrong population. These tests pin the four
// halves of the fix:
//
//   1. `RediffSlotOwnership.strictByteExactMask` — WHICH minted slots earn an
//      anchor (strict monotonic-clean only; 9s6q segment-recovered slots stay
//      banner, playhead-pyq7 owns validating those).
//   2. The mint persists `rediffByteExact` on BOTH edges + `eligible` for a
//      strict slot, over REAL divergent MP3 bytes, read back from SQLite.
//   3. `AutoSkipEdgePadding.isActive` — the TARGETED activation: the Gate-2
//      master switch stays OFF and only a both-edges-byte-exact span is padded.
//   4. Orchestrator wiring: that span's cue really is the late-safe padded
//      window, and every other anchor combination is byte-identical to today.

import AVFoundation
import Foundation
import Testing

@testable import Playhead

// MARK: - 1. The strict/recovered classification rule (pure)

@Suite("Day-0 strict byte-exact classification (playhead-qs0d)")
struct RediffStrictByteExactMaskTests {

    private typealias Slot = RediffSlotOwnership.PlayedSlot

    private static func slot(_ start: Double, _ end: Double) -> Slot {
        Slot(startSeconds: start, endSeconds: end, leftRunSeconds: 300, rightRunSeconds: 300)
    }

    /// The single-persona happy path: one strict list, one union, every slot
    /// earns the anchor.
    @Test("a union built entirely from monotonic-clean personas is ALL strict")
    func allStrictWhenEveryPersonaIsMonotonicClean() {
        let lists = [[Self.slot(0, 60), Self.slot(5462, 5522)]]
        let unioned = RediffSlotOwnership.unionedPlayedSlots(lists)
        let mask = RediffSlotOwnership.strictByteExactMask(
            unioned: unioned, strictPerBSideSlots: lists
        )
        #expect(mask == [true, true])
    }

    /// The 9s6q lane: EVERY persona needed segment recovery, so the strict
    /// subset is empty and nothing may be anchored. This is the assertion that
    /// keeps a dropped-run chain out of the auto-skip lane.
    @Test("a union with NO monotonic-clean persona is ALL non-strict")
    func nothingIsStrictWhenEveryPersonaWasRecovered() {
        let recovered = [[Self.slot(100, 160)], [Self.slot(100, 160)]]
        let unioned = RediffSlotOwnership.unionedPlayedSlots(recovered)
        let mask = RediffSlotOwnership.strictByteExactMask(
            unioned: unioned, strictPerBSideSlots: []
        )
        #expect(mask == [false])
    }

    /// MIXED personas, DISJOINT slots — the discriminating case. A slot only
    /// the recovered persona found must NOT inherit the strict persona's
    /// certainty, and the strict persona's own slot must not lose it.
    @Test("mixed personas: only the slot the STRICT subset reproduces is strict")
    func mixedPersonasClassifyPerSlot() {
        let strictList = [Self.slot(0, 60)]
        let recoveredList = [Self.slot(1000, 1060)]
        let unioned = RediffSlotOwnership.unionedPlayedSlots([strictList, recoveredList])
        #expect(unioned.count == 2, "fixture control: the two slots stay separate")

        let mask = RediffSlotOwnership.strictByteExactMask(
            unioned: unioned, strictPerBSideSlots: [strictList]
        )
        #expect(mask == [true, false],
                "the recovered-only slot must not borrow the strict persona's anchor")
    }

    /// The subtle one, and the reason the rule is an EXACT geometry match
    /// rather than "did any strict persona overlap this slot". A recovered
    /// persona that WIDENS a strict persona's slot has moved the edge — and a
    /// moved edge is exactly what an anchor claims not to be. The merged slot
    /// must fall back to mark-only even though a strict persona contributed.
    @Test("a recovered persona that WIDENS a strict slot revokes the anchor")
    func widenedSlotLosesStrictness() {
        let strictList = [Self.slot(100, 160)]
        // Overlapping + longer: `mergedAndCapped` fuses these into [100, 200].
        let recoveredList = [Self.slot(150, 200)]
        let unioned = RediffSlotOwnership.unionedPlayedSlots([strictList, recoveredList])
        #expect(unioned.count == 1, "fixture control: the two overlapping slots merged")
        #expect(unioned[0].endSeconds == 200, "fixture control: the recovered persona widened the end")

        let mask = RediffSlotOwnership.strictByteExactMask(
            unioned: unioned, strictPerBSideSlots: [strictList]
        )
        #expect(mask == [false],
                "the surviving edge is the RECOVERED persona's — it has not earned an anchor")
    }

    /// An empty union is an empty mask (not a crash, not a phantom `true`).
    @Test("an empty union yields an empty mask")
    func emptyUnionYieldsEmptyMask() {
        #expect(RediffSlotOwnership.strictByteExactMask(
            unioned: [], strictPerBSideSlots: [[Self.slot(0, 60)]]
        ).isEmpty)
    }

    /// The mask is INDEX-ALIGNED with `unioned`. A silent off-by-one here would
    /// anchor the wrong span, so the count is pinned independently of content.
    @Test("the mask is index-aligned with the union")
    func maskIsIndexAligned() {
        let lists = [[Self.slot(0, 60), Self.slot(500, 560), Self.slot(1000, 1060)]]
        let unioned = RediffSlotOwnership.unionedPlayedSlots(lists)
        let mask = RediffSlotOwnership.strictByteExactMask(
            unioned: unioned, strictPerBSideSlots: lists
        )
        #expect(mask.count == unioned.count)
        #expect(mask.count == 3)
    }
}

// MARK: - 2. The mint, over REAL divergent MP3 bytes, read back from SQLite

@Suite("Day-0 mint persists auto-skip provenance (playhead-qs0d)")
struct RediffDayZeroMintAutoSkipPersistenceTests {

    private func makeService(store: AnalysisStore) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40, confirmationThreshold: 0.70,
                suppressionThreshold: 0.25, hotPathLookahead: 90.0,
                detectorVersion: "qs0d-test", fmBackfillMode: .off,
                rediffSlotOwnershipEnabled: true
            ),
            rediffBSideProvider: nil
        )
    }

    private func insertAsset(store: AnalysisStore, assetId: String, sourceURL: String) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId, episodeId: "ep-\(assetId)", assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil, sourceURL: sourceURL,
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil, episodeDurationSec: 280
        ))
    }

    /// A/B pair with ONE ID3-separated ad block over ≈[95, 165] s — the same
    /// construction `RediffDayZeroMintExitTests.DivergentTriple` uses, and a
    /// chain the byte gate accepts on its STRICT (monotonic-clean) arm.
    private struct StrictDivergentTriple {
        let aURL: URL
        let b0: URL
        let b1: URL
        let bData: Data
        let aData: Data

        static func stage(in dir: URL) throws -> StrictDivergentTriple {
            let adStartFrame = 3637      // ≈ 95.008 s
            let adFrames = 2680          // ≈ 70.008 s
            let contentFrames = 10_719   // ≈ 280.0 s of played (A) audio
            let c1 = SyntheticMP3.frames(count: adStartFrame, seed: 0xC0FFEE)
            let c2 = SyntheticMP3.frames(count: contentFrames - adStartFrame - adFrames, seed: 0xFACADE)
            let ad = SyntheticMP3.frames(count: adFrames, seed: 0xAD_B10C)
            let aData = SyntheticMP3.file(c1 + [SyntheticMP3.id3v2(payloadBytes: 32)] + ad + c2)
            let bData = SyntheticMP3.file(c1 + c2)
            let aURL = dir.appendingPathComponent("a.mp3", isDirectory: false)
            let b0 = dir.appendingPathComponent("b0.fresh.mp3", isDirectory: false)
            let b1 = dir.appendingPathComponent("b1.fresh.mp3", isDirectory: false)
            try aData.write(to: aURL)
            try bData.write(to: b0)
            try bData.write(to: b1)
            return StrictDivergentTriple(aURL: aURL, b0: b0, b1: b1, bData: bData, aData: aData)
        }
    }

    /// THE bead's headline assertion, read back out of SQLite rather than
    /// asserted on an in-memory value: a strict byte-exact day-0 slot persists
    /// with `rediffByteExact` on BOTH edges and `eligibilityGate = eligible`.
    ///
    /// Before this bead the SAME bytes produced `unanchored`/`unanchored` +
    /// `markOnly` — which is exactly what the device showed.
    @Test("a STRICT byte-exact day-0 slot persists rediffByteExact anchors and an eligible gate")
    func strictSlotPersistsAnchorsAndEligibleGate() async throws {
        let dir = try makeTempDir(prefix: "Qs0dStrictMint")
        defer { try? FileManager.default.removeItem(at: dir) }
        let triple = try StrictDivergentTriple.stage(in: dir)

        // Fixture control: this pair really does take the STRICT arm. If the
        // aligner ever stopped reporting a clean chain here, the test below
        // would be asserting the recovered lane's behaviour by accident.
        let alignment = RediffByteAligner.align(aData: triple.aData, bData: triple.bData)
        #expect(alignment.monotonicClean,
                "fixture control: a single removed-in-B block must chain monotonically")

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: triple.aURL.absoluteString)
        let outcome = await makeService(store: store)
            .mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [triple.b0, triple.b1])

        #expect(outcome.exit == .marked, "control: the pair diverges — got \(outcome.exit)")
        #expect(outcome.markCount == 1)

        let rows = try await store.fetchAdWindows(assetId: "a1")
        let row = try #require(rows.first)
        #expect(row.boundaryState == AdDetectionService.dayZeroRediffByteExactBoundaryState)
        #expect(row.startEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue,
                "the byte differ set this edge — persisting `unanchored` is what held it at markOnly")
        #expect(row.endEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue,
                "the byte differ set this edge too")
        #expect(row.eligibilityGate == SkipEligibilityGate.eligible.rawValue,
                "a 1.00-confidence byte-verified DAI splice must be auto-skip eligible")
        #expect(row.confidence == 1.0)
    }

    /// A/B pair with TWO removed-in-B ad blocks. The multi-break structure
    /// makes the run chain drop a run (non-monotonic), so the byte gate can
    /// only accept it through the playhead-9s6q SEGMENT-RECOVERY arm.
    private struct RecoveredMultiBreakPair {
        let aURL: URL
        let b0: URL
        let b1: URL
        let aData: Data
        let bData: Data

        static func stage(in dir: URL) throws -> RecoveredMultiBreakPair {
            // 300-frame blocks: ≈125 KB each, comfortably over the default
            // 64 KiB min-run, and ≈7.84 s per ad — over the 5 s min-ad-width.
            let contentFrames = 300
            let adFrames = 300
            let c0 = SyntheticMP3.frames(count: contentFrames, seed: 0x0C0_0001)
            let c1 = SyntheticMP3.frames(count: contentFrames, seed: 0x0C1_0002)
            let c2 = SyntheticMP3.frames(count: contentFrames, seed: 0x0C2_0003)
            let ad1 = SyntheticMP3.frames(count: adFrames, seed: 0xAD1_0004)
            let ad2 = SyntheticMP3.frames(count: adFrames, seed: 0xAD2_0005)
            let aData = SyntheticMP3.file(c0 + ad1 + c1 + ad2 + c2)
            let bData = SyntheticMP3.file(c0 + c1 + c2)
            let aURL = dir.appendingPathComponent("a.mp3", isDirectory: false)
            let b0 = dir.appendingPathComponent("b0.fresh.mp3", isDirectory: false)
            let b1 = dir.appendingPathComponent("b1.fresh.mp3", isDirectory: false)
            try aData.write(to: aURL)
            try bData.write(to: b0)
            try bData.write(to: b1)
            return RecoveredMultiBreakPair(aURL: aURL, b0: b0, b1: b1, aData: aData, bData: bData)
        }
    }

    /// The OTHER half of the scope contract, and the one that keeps this bead
    /// narrow: a 9s6q SEGMENT-RECOVERED slot still mints a banner but keeps the
    /// conservative `unanchored`/`markOnly` pair. Those boundaries have not
    /// been validated (playhead-pyq7 owns that), and a chain that dropped runs
    /// has not proven its A-timeline mapping at the edge.
    @Test("a SEGMENT-RECOVERED (non-monotonic) day-0 slot stays unanchored and mark-only")
    func segmentRecoveredSlotStaysUnanchoredMarkOnly() async throws {
        let dir = try makeTempDir(prefix: "Qs0dRecoveredMint")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try RecoveredMultiBreakPair.stage(in: dir)

        // Fixture controls. Both matter: if the chain were clean this test
        // would silently assert the strict lane's behaviour, and if the gate
        // rejected outright nothing would mint and the assertions below would
        // pass vacuously over an empty row set.
        let alignment = RediffByteAligner.align(aData: pair.aData, bData: pair.bData)
        try #require(!alignment.monotonicClean,
                     "fixture control: the multi-break chain must go NON-monotonic")
        try #require(RediffActivation.nonMonotonicSegmentRecoveryEnabled,
                     "fixture control: day-0 must be opted into segment recovery")

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: pair.aURL.absoluteString)
        let outcome = await makeService(store: store)
            .mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [pair.b0, pair.b1])

        #expect(outcome.exit == .marked,
                "control: recovery still MINTS a banner — got \(outcome.exit)")
        let rows = try await store.fetchAdWindows(assetId: "a1")
        #expect(!rows.isEmpty, "control: the recovered slots really were persisted")
        for row in rows {
            #expect(row.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue,
                    "a dropped-run chain has not earned a start anchor")
            #expect(row.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue,
                    "a dropped-run chain has not earned an end anchor")
            #expect(row.eligibilityGate == SkipEligibilityGate.markOnly.rawValue,
                    "9s6q segment-recovered slots stay BANNER until playhead-pyq7 validates them")
        }
    }
}

// MARK: - 3. The TARGETED padding activation (pure)

@Suite("Targeted auto-skip padding activation (playhead-qs0d)")
struct AutoSkipEdgePaddingTargetedActivationTests {

    private static let allAnchors = AutoSkipEdgeAnchor.allCases

    /// The whole point of the targeted lane: Dan asked for *rediff* to skip,
    /// not for everything to skip. With the Gate-2 master switch OFF, the
    /// policy is live for EXACTLY one anchor pair.
    @Test("master OFF: the policy is active for both-edges-byteExact and NOTHING else")
    func masterOffActivatesOnlyBothEdgesByteExact() {
        for start in Self.allAnchors {
            for end in Self.allAnchors {
                let expected = (start == .rediffByteExact && end == .rediffByteExact)
                #expect(
                    AutoSkipEdgePadding.isActive(
                        masterEnabled: false, startAnchor: start, endAnchor: end
                    ) == expected,
                    "(\(start.rawValue), \(end.rawValue)) must be \(expected) with the master switch off"
                )
            }
        }
    }

    /// BOTH edges are required. A byte-exact start with an unanchored end would
    /// otherwise pull in `endMarginUnanchoredSeconds` (10.25 s) — a margin
    /// derived for a different population — so the pair is spelled out here
    /// rather than left implicit in the loop above.
    @Test("master OFF: a byte-exact START with a weaker END does NOT activate")
    func masterOffRequiresBothEdges() {
        #expect(!AutoSkipEdgePadding.isActive(
            masterEnabled: false, startAnchor: .rediffByteExact, endAnchor: .unanchored))
        #expect(!AutoSkipEdgePadding.isActive(
            masterEnabled: false, startAnchor: .rediffByteExact, endAnchor: .stingerSnapped))
        #expect(!AutoSkipEdgePadding.isActive(
            masterEnabled: false, startAnchor: .unanchored, endAnchor: .rediffByteExact))
    }

    /// The master switch, when it is eventually flipped, still means what it
    /// always meant: the policy applies to every span.
    @Test("master ON: the policy is active for EVERY anchor pair")
    func masterOnActivatesEverything() {
        for start in Self.allAnchors {
            for end in Self.allAnchors {
                #expect(AutoSkipEdgePadding.isActive(
                    masterEnabled: true, startAnchor: start, endAnchor: end))
            }
        }
    }

    /// The Gate-2 master switch was NOT flipped by this bead. Its preconditions
    /// (wraj surfacing + veto masks + 3-run reproducibility) are unmet, and
    /// flipping it would change behaviour for populations this bead never
    /// measured. Pinned so a later "just turn it on" cannot slip in unnoticed.
    @Test("the Gate-2 master switch is still OFF — this bead did not flip it")
    func masterSwitchRemainsOff() {
        #expect(AutoSkipEdgePadding.isEnabledByDefault == false)
    }
}

// MARK: - 4. Orchestrator wiring

@Suite("Targeted padding wiring — SkipOrchestrator (playhead-qs0d)")
struct AutoSkipEdgePaddingTargetedWiringTests {

    private static func cueStart(_ cue: CMTimeRange) -> Double { CMTimeGetSeconds(cue.start) }
    private static func cueEnd(_ cue: CMTimeRange) -> Double {
        CMTimeGetSeconds(cue.start + cue.duration)
    }

    private static func makeAutoOrchestrator() async throws -> SkipOrchestrator {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        return SkipOrchestrator(store: store, trustService: trustService)
    }

    private static func pushedCue(
        startEdgeAnchor: String,
        endEdgeAnchor: String
    ) async throws -> CMTimeRange? {
        let orchestrator = try await makeAutoOrchestrator()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )
        // NOTE: no `setEdgePaddingEnabled` call anywhere in this suite — the
        // Gate-2 master switch stays at its OFF default throughout.
        await orchestrator.receiveAdWindows([
            makeSkipTestAdWindow(
                id: "qs0d-\(startEdgeAnchor)-\(endEdgeAnchor)",
                startTime: 60, endTime: 120,
                confidence: 0.9, decisionState: "confirmed",
                startEdgeAnchor: startEdgeAnchor,
                endEdgeAnchor: endEdgeAnchor,
                eligibilityGate: SkipEligibilityGate.eligible.rawValue
            )
        ])
        return pushedCues.first
    }

    /// The deliverable. Master switch OFF, and a both-edges-byte-exact span is
    /// nonetheless skipped at the DERIVED late-safe bounds rather than raw.
    ///
    /// Note the direction: before this bead the same eligible span would have
    /// been skipped at 60 → 119 (raw bounds, only the pre-existing 1.0 s pod
    /// cushion). Padding only ever SHRINKS, so the targeted activation cannot
    /// admit a skip that was previously blocked — it makes an already-firing
    /// skip late-safe.
    @Test("master OFF: a both-edges byte-exact span is skipped at 98co late-safe bounds")
    func byteExactSpanIsPaddedWithMasterOff() async throws {
        let cue = try #require(await Self.pushedCue(
            startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue
        ))
        #expect(Self.cueStart(cue) == 60.50,
                "60 + the 0.50 s rediff byte-exact start margin")
        #expect(Self.cueEnd(cue) == 118.25,
                "120 - the 0.75 s rediff end margin - the 1.0 s pod trailing cushion")
    }

    /// The exemption does not leak: a stinger-snapped span — a real anchor
    /// tier, just not the deterministic one — is byte-identical to today.
    @Test("master OFF: a stinger-snapped span is UNCHANGED (raw bounds)")
    func stingerSpanIsUnpaddedWithMasterOff() async throws {
        let cue = try #require(await Self.pushedCue(
            startEdgeAnchor: AutoSkipEdgeAnchor.stingerSnapped.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.stingerSnapped.rawValue
        ))
        #expect(Self.cueStart(cue) == 60, "no start padding — the master switch is off")
        #expect(Self.cueEnd(cue) == 119, "120 - the pre-existing 1.0 s cushion only")
    }

    /// And the population that ate 210 s of show — unanchored on both edges —
    /// is likewise untouched by this bead. It is NOT newly protected (that is
    /// what flipping the master switch would do, and that is Dan's call) and it
    /// is NOT newly exposed.
    @Test("master OFF: an unanchored span is UNCHANGED (raw bounds)")
    func unanchoredSpanIsUnpaddedWithMasterOff() async throws {
        let cue = try #require(await Self.pushedCue(
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        ))
        #expect(Self.cueStart(cue) == 60)
        #expect(Self.cueEnd(cue) == 119)
    }

    /// A MIXED pair must not activate. `rediffByteExact` start + `unanchored`
    /// end would otherwise be padded with the 10.25 s unanchored end margin —
    /// a large, differently-derived pull-in applied to a lane nobody measured.
    @Test("master OFF: a byte-exact START with an unanchored END is UNCHANGED")
    func mixedAnchorSpanIsUnpaddedWithMasterOff() async throws {
        let cue = try #require(await Self.pushedCue(
            startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        ))
        #expect(Self.cueStart(cue) == 60)
        #expect(Self.cueEnd(cue) == 119)
    }
}
