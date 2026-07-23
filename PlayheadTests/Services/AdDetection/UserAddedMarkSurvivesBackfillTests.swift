// UserAddedMarkSurvivesBackfillTests.swift
// playhead-527u: user-ADDED (false-negative) ad marks are SACRED — no automated
// re-analysis pass may undo them. This is the SYMMETRIC counterpart of the
// xsdz.34 veto (false-positive) read side: where a `.userVetoed` atom is
// DE-anchored so its region is never re-detected, a `.userConfirmed` atom is
// FORCE-anchored so its region is ALWAYS re-detected across a backfill / rediff
// re-derivation.
//
// Coverage:
//   S1 store         — activeFalseNegativeScopes returns FN scopes, excludes FP
//   M1 mask adapter  — .falseNegative exactSpan/exactTimeSpan → .userConfirmed;
//                      veto WINS on a conflicting overlap
//   P1 projector     — a confirmed ordinal is anchored even with NO region
//   F1 factory       — confirm mask is effective flag-OFF; non-corrected asset
//                      is byte-identical to NoCorrectionMaskProvider
//   B1 INTEGRATION   — record a user-marked missed ad, run runBackfill AND
//                      revalidateFromFeatures; BOTH the userMarked AdWindow row
//                      AND the region's decoded-span ad-presence persist (crux)
//   R1 rediff gate   — a rediff slot cannot NEWLY ENCLOSE (absorb) a confirmed
//                      region (mirror of the veto §5 gate)
//   R2 provenance    — a `.userCorrection` span is never absorbed/superseded by
//                      the SpliceSlotRewriter

import Foundation
import Testing

@testable import Playhead

// MARK: - Shared builders

private enum Added {
    static let assetId = "asset-527u"

    /// Atom stream: ordinals 0..<count at [0,10),[10,20),… (10 s per atom).
    static func atoms(count: Int, assetId: String = assetId) -> [TranscriptAtom] {
        (0..<count).map { i in
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: assetId,
                    transcriptVersion: "tv1",
                    atomOrdinal: i
                ),
                contentHash: "hash-\(i)",
                startTime: Double(i) * 10,
                endTime: Double(i) * 10 + 10,
                text: "word \(i)",
                chunkIndex: i
            )
        }
    }

    static let emptyCatalog = EvidenceCatalog(
        analysisAssetId: assetId, transcriptVersion: "tv1", entries: []
    )

    /// Clean (no-ad-signal) chunks covering [0, 90] — with NO force-anchor the
    /// backfill produces no decoded span over the mark region, so the test is
    /// load-bearing (fails on the pre-527u behavior, passes on the fix).
    static func cleanChunks(assetId: String = assetId) -> [TranscriptChunk] {
        let texts = [
            "Welcome to the show. Today we discuss science and physics at length together.",
            "Here is the main topic of today's episode about the future of research funding.",
            "Thank you so much for listening along with us. We will see you all next time."
        ]
        return texts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    static func service(store: AnalysisStore) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off
        )
        return AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config
        )
    }
}

// MARK: - S1 store

@Suite("playhead-527u S1 — activeFalseNegativeScopes")
struct AddedMarkStoreScopeTests {

    @Test("returns the .falseNegative scope and EXCLUDES the .falsePositive veto")
    func falseNegativeIncludedFalsePositiveExcluded() async throws {
        let assetId = "asset-527u-s1"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)

        // A user-added missed ad ([100,120], false negative)…
        await corrections.recordVeto(
            startTime: 100, endTime: 120, assetId: assetId,
            podcastId: nil, source: .falseNegative
        )
        // …and an explicit veto ([20,30], false positive).
        await corrections.recordVeto(
            startTime: 20, endTime: 30, assetId: assetId,
            podcastId: nil, source: .manualVeto
        )

        let fnScopes = await corrections.activeFalseNegativeScopes(for: assetId)
        #expect(fnScopes.contains(.exactTimeSpan(assetId: assetId, startTime: 100, endTime: 120)))
        #expect(!fnScopes.contains(.exactTimeSpan(assetId: assetId, startTime: 20, endTime: 30)),
                "the false-positive veto must NOT leak into the confirm direction")

        // Symmetry check: the FP path still sees only its own scope.
        let fpScopes = await corrections.activeFalsePositiveScopes(for: assetId)
        #expect(fpScopes.contains(.exactTimeSpan(assetId: assetId, startTime: 20, endTime: 30)))
        #expect(!fpScopes.contains(.exactTimeSpan(assetId: assetId, startTime: 100, endTime: 120)),
                "the false-negative confirm must NOT leak into the veto direction")
    }
}

// MARK: - M1 mask adapter

@Suite("playhead-527u M1 — StoreBackedCorrectionMaskProvider confirm direction")
struct AddedMarkMaskAdapterTests {

    private let atoms = Added.atoms(count: 5)

    @Test(".falseNegative exactTimeSpan(20,30) → .userConfirmed on the overlapping atom")
    func confirmedTimeSpanMasks() async {
        let provider = StoreBackedCorrectionMaskProvider(
            fromVetoScopes: [],
            confirmedScopes: [.exactTimeSpan(assetId: Added.assetId, startTime: 20, endTime: 30)],
            atoms: atoms
        )
        let masks = await provider.correctionMasks(for: 0...4, in: Added.assetId)
        #expect(masks[2] == .userConfirmed)  // atom 2 is [20,30)
        #expect(masks[1] == nil)
        #expect(masks[3] == nil)
    }

    @Test(".falseNegative exactSpan(1...2) → .userConfirmed on ordinals 1 and 2")
    func confirmedSpanMasks() async {
        let provider = StoreBackedCorrectionMaskProvider(
            fromVetoScopes: [],
            confirmedScopes: [.exactSpan(assetId: Added.assetId, ordinalRange: 1...2)],
            atoms: atoms
        )
        let masks = await provider.correctionMasks(for: 0...4, in: Added.assetId)
        #expect(masks[1] == .userConfirmed)
        #expect(masks[2] == .userConfirmed)
        #expect(masks[0] == nil)
        #expect(masks[3] == nil)
    }

    @Test("veto WINS: an atom in BOTH a veto and a confirm range masks .userVetoed")
    func vetoPrecedenceOnConflict() async {
        let provider = StoreBackedCorrectionMaskProvider(
            fromVetoScopes: [.exactTimeSpan(assetId: Added.assetId, startTime: 20, endTime: 30)],
            confirmedScopes: [.exactTimeSpan(assetId: Added.assetId, startTime: 20, endTime: 30)],
            atoms: atoms
        )
        let masks = await provider.correctionMasks(for: 0...4, in: Added.assetId)
        #expect(masks[2] == .userVetoed,
                "the suppress-direction guardrail must dominate a contradictory pair")
    }

    @Test("show-wide confirm scopes contribute no mask (Layer-A span/time only)")
    func showWideConfirmIgnored() async {
        let provider = StoreBackedCorrectionMaskProvider(
            fromVetoScopes: [],
            confirmedScopes: [.sponsorOnShow(podcastId: "p", sponsor: "acme")],
            atoms: atoms
        )
        let masks = await provider.correctionMasks(for: 0...4, in: Added.assetId)
        #expect(masks.isEmpty)
    }
}

// MARK: - P1 projector

@Suite("playhead-527u P1 — projector force-anchors a confirmed region")
struct AddedMarkProjectorTests {

    @Test("a .userConfirmed atom is anchored even with NO region covering it")
    func confirmedAtomForceAnchored() async {
        let atoms = Added.atoms(count: 5)
        // NO regions at all — the ONLY reason atom 2 could anchor is the confirm mask.
        let provider = StoreBackedCorrectionMaskProvider(
            fromVetoScopes: [],
            confirmedScopes: [.exactSpan(assetId: Added.assetId, ordinalRange: 2...2)],
            atoms: atoms
        )
        let evidence = await AtomEvidenceProjector().project(
            regions: [],
            catalog: Added.emptyCatalog,
            atoms: atoms,
            correctionMaskProvider: provider
        )
        #expect(evidence[2].correctionMask == .userConfirmed)
        #expect(evidence[2].isAnchored, "a confirmed atom is force-anchored with no region")
        // Neighbours stay unanchored (no region, no mask).
        for i in [0, 1, 3, 4] {
            #expect(evidence[i].correctionMask == .none)
            #expect(!evidence[i].isAnchored)
        }
    }

    @Test("the confirmed region decodes into a covering span")
    func confirmedRegionDecodesToSpan() async {
        let atoms = Added.atoms(count: 5)
        let provider = StoreBackedCorrectionMaskProvider(
            fromVetoScopes: [],
            confirmedScopes: [.exactSpan(assetId: Added.assetId, ordinalRange: 2...3)],
            atoms: atoms
        )
        let evidence = await AtomEvidenceProjector().project(
            regions: [], catalog: Added.emptyCatalog, atoms: atoms,
            correctionMaskProvider: provider
        )
        let spans = MinimalContiguousSpanDecoder().decode(atoms: evidence, assetId: Added.assetId)
        #expect(spans.contains { $0.firstAtomOrdinal <= 2 && $0.lastAtomOrdinal >= 3 },
                "force-anchored confirmed atoms decode into a covering span")
    }
}

// MARK: - F1 factory

@Suite("playhead-527u F1 — makeCorrectionMaskProvider confirm-direction gating")
struct AddedMarkFactoryTests {

    @Test("confirm mask is effective with the veto A/B flag OFF")
    func confirmEffectiveFlagOff() async throws {
        let assetId = "asset-527u-f1"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)
        await corrections.recordVeto(
            startTime: 20, endTime: 30, assetId: assetId,
            podcastId: nil, source: .falseNegative
        )
        let atoms = Added.atoms(count: 5, assetId: assetId)

        // enabled:false is the production default for userCorrectionReadSideEnabled.
        let provider = await AdDetectionService.makeCorrectionMaskProvider(
            enabled: false, store: corrections, analysisAssetId: assetId, atoms: atoms
        )
        #expect(provider is StoreBackedCorrectionMaskProvider,
                "a false-negative correction produces a store-backed provider even flag-OFF")
        let masks = await provider.correctionMasks(for: 0...4, in: assetId)
        #expect(masks[2] == .userConfirmed)
    }

    @Test("a non-corrected asset is byte-identical to NoCorrectionMaskProvider")
    func nonCorrectedAssetIsNoOp() async throws {
        let assetId = "asset-527u-f1-clean"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)
        let atoms = Added.atoms(count: 5, assetId: assetId)

        let provider = await AdDetectionService.makeCorrectionMaskProvider(
            enabled: false, store: corrections, analysisAssetId: assetId, atoms: atoms
        )
        #expect(provider is NoCorrectionMaskProvider,
                "no corrections ⇒ NoCorrectionMaskProvider ⇒ no change to non-corrected episodes")
    }

    /// playhead-527u UNGATED-SHIP SAFETY (mandate #2). The confirm direction
    /// ships effective regardless of `userCorrectionReadSideEnabled`. This must
    /// NOT bleed into episodes the user never corrected: a NON-corrected asset's
    /// mask provider must be identical (a no-op `NoCorrectionMaskProvider` with
    /// empty masks) whether the veto A/B flag is ON or OFF. Guards "the confirm
    /// direction changes nothing when there are no false-negative corrections."
    @Test("non-corrected asset: mask provider is byte-identical (empty) flag ON vs OFF")
    func nonCorrectedAssetIdenticalOnOff() async throws {
        let assetId = "asset-527u-onoff"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)
        let atoms = Added.atoms(count: 6, assetId: assetId)

        let off = await AdDetectionService.makeCorrectionMaskProvider(
            enabled: false, store: corrections, analysisAssetId: assetId, atoms: atoms
        )
        let on = await AdDetectionService.makeCorrectionMaskProvider(
            enabled: true, store: corrections, analysisAssetId: assetId, atoms: atoms
        )
        #expect(off is NoCorrectionMaskProvider)
        #expect(on is NoCorrectionMaskProvider,
                "the confirm direction must not synthesize a store-backed provider for an uncorrected asset even flag-ON")
        let masksOff = await off.correctionMasks(for: 0...5, in: assetId)
        let masksOn = await on.correctionMasks(for: 0...5, in: assetId)
        #expect(masksOff.isEmpty && masksOn.isEmpty,
                "an uncorrected asset produces no masks in either flag state — no bleed into uncorrected episodes")
    }

    @Test("flag OFF + only a veto still selects NoCorrectionMaskProvider (xsdz.34 identity preserved)")
    func flagOffVetoOnlyStillNoOp() async throws {
        let assetId = "asset-527u-f1-veto"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)
        await corrections.recordVeto(
            startTime: 20, endTime: 30, assetId: assetId,
            podcastId: nil, source: .manualVeto
        )
        let atoms = Added.atoms(count: 5, assetId: assetId)

        let provider = await AdDetectionService.makeCorrectionMaskProvider(
            enabled: false, store: corrections, analysisAssetId: assetId, atoms: atoms
        )
        #expect(provider is NoCorrectionMaskProvider,
                "flag-OFF must still gate the veto direction (only confirm is flag-independent)")
    }
}

// MARK: - B1 INTEGRATION (crux)

@Suite("playhead-527u B1 — user-added mark survives backfill + revalidate")
struct AddedMarkSurvivesBackfillTests {

    /// A user marks a missed ad at [35,55]; a later runBackfill AND a rediff
    /// revalidateFromFeatures re-derive the episode. BOTH the userMarked AdWindow
    /// ROW and the region's ad-presence (a decoded span covering [35,55]) must
    /// persist — the mark's DETECTION is not silently undone.
    @Test("userMarked row AND the region's decoded-span ad-presence both survive")
    func markAndDetectionSurvive() async throws {
        let assetId = "asset-527u-b1"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)

        // Persist the transcript chunks so `revalidateFromFeatures` (Pass 2) reads
        // them back and genuinely RE-DERIVES the episode (production shape: ASR
        // persists chunks, then analysis/recovery re-runs over them).
        let chunks = Added.cleanChunks(assetId: assetId)
        try await store.insertTranscriptChunks(chunks)

        let service = Added.service(store: store)
        await service.setUserCorrectionStore(corrections)

        // The user marks a missed ad at [35,55] (inside the middle chunk [30,60]).
        let markStart = 35.0
        let markEnd = 55.0
        await service.recordUserMarkedAd(
            analysisAssetId: assetId,
            startTime: markStart,
            endTime: markEnd,
            podcastId: "podcast-527u"
        )

        // Preconditions: the userMarked row and the false-negative correction exist.
        #expect(try await store.fetchAdWindows(assetId: assetId)
            .contains { $0.boundaryState == "userMarked" })
        let fnScopes = await corrections.activeFalseNegativeScopes(for: assetId)
        #expect(!fnScopes.isEmpty, "the false-negative correction was recorded")

        func overlapsMark(_ span: DecodedSpan) -> Bool {
            span.startTime < markEnd && markStart < span.endTime
        }

        // ── Pass 1: runBackfill re-derivation ────────────────────────────────
        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "podcast-527u",
            episodeDuration: 90.0
        )

        let windows1 = try await store.fetchAdWindows(assetId: assetId)
        #expect(windows1.contains { $0.boundaryState == "userMarked" },
                "the userMarked AdWindow row must survive runBackfill")
        let spans1 = try await store.fetchDecodedSpans(assetId: assetId)
        #expect(spans1.contains(where: overlapsMark),
                "the region's ad-presence (a decoded span over the mark) must survive runBackfill")

        // ── Pass 2: rediff revalidateFromFeatures (re-runs the full pipeline) ─
        try await service.revalidateFromFeatures(
            analysisAssetId: assetId,
            podcastId: "podcast-527u",
            episodeDuration: 90.0
        )

        let windows2 = try await store.fetchAdWindows(assetId: assetId)
        #expect(windows2.contains { $0.boundaryState == "userMarked" },
                "the userMarked AdWindow row must survive rediff revalidateFromFeatures")
        let spans2 = try await store.fetchDecodedSpans(assetId: assetId)
        #expect(spans2.contains(where: overlapsMark),
                "the region's ad-presence must survive rediff revalidateFromFeatures")
    }

    /// playhead-527u DOUBLE-WINDOW GUARD + DEFINITIVE-SKIP AC. The 527u
    /// force-anchor re-derives a decoded span over the user's marked region,
    /// which fuses into a NEW `acousticRefined` AdWindow. Without the reconcile
    /// dominance-dedupe that window co-exists with the `userMarked` row → TWO
    /// windows over one ad. This asserts EXACTLY ONE AdWindow surfaces over the
    /// mark, that it is the user's own `userMarked` row, and — per the product
    /// owner's updated AC — that it is AUTO-SKIP-ELIGIBLE (gate == .eligible) at
    /// the user's own boundaries (a manual mark is the highest-certainty ad
    /// signal, so the region auto-skips rather than merely bannering). The
    /// ad-presence decoded span is still preserved. RED pre-fix (gate nil / two
    /// windows); GREEN with the eligibility stamp + dominance dedupe.
    @Test("exactly ONE window over the mark, AUTO-SKIP-ELIGIBLE at the user's boundaries — decoded span preserved")
    func exactlyOneWindowOverMark() async throws {
        let assetId = "asset-527u-onewin"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)
        let chunks = Added.cleanChunks(assetId: assetId)
        try await store.insertTranscriptChunks(chunks)
        let service = Added.service(store: store)
        await service.setUserCorrectionStore(corrections)
        let markStart = 35.0, markEnd = 55.0
        await service.recordUserMarkedAd(
            analysisAssetId: assetId, startTime: markStart, endTime: markEnd, podcastId: "p"
        )
        try await service.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: "p", episodeDuration: 90.0
        )
        try await service.revalidateFromFeatures(
            analysisAssetId: assetId, podcastId: "p", episodeDuration: 90.0
        )

        let all = try await store.fetchAdWindows(assetId: assetId)
        let overlapping = all.filter { $0.startTime < markEnd && markStart < $0.endTime }
        #expect(overlapping.count == 1,
                "exactly one AdWindow may surface over a user-marked region; found \(overlapping.count): \(overlapping.map { "\($0.boundaryState)/\($0.decisionState)" })")
        #expect(overlapping.first?.boundaryState == "userMarked",
                "the single surviving window must be the user's own userMarked row, not a re-derived fusion window")
        // playhead-527u (product-owner AC): a user's manual mark is DEFINITIVE,
        // so the single surviving window is AUTO-SKIP-ELIGIBLE (gate == .eligible)
        // at the user's own boundaries — NOT a banner-only markOnly row. RED
        // pre-fix (recordUserMarkedAd left the gate nil); GREEN once the
        // userMarked row is stamped `.eligible`.
        #expect(overlapping.first?.eligibilityGate == SkipEligibilityGate.eligible.rawValue,
                "a user-MARKED region is definitive → its single surviving window must be auto-skip-eligible")

        // The ad-presence (decoded span) is still re-emitted — the dedupe removes
        // only the redundant AdWindow, not the force-anchored span.
        let spans = try await store.fetchDecodedSpans(assetId: assetId)
        #expect(spans.contains { $0.startTime < markEnd && markStart < $0.endTime },
                "the force-anchored decoded span (ad-presence) must still survive the dedupe")
    }
}

// MARK: - R1 rediff gate

@Suite("playhead-527u R1 — rediff cannot absorb a confirmed region")
struct AddedMarkRediffGateTests {

    private static func played(_ start: Double, _ end: Double) -> RediffSlotOwnership.PlayedSlot {
        RediffSlotOwnership.PlayedSlot(startSeconds: start, endSeconds: end, leftRunSeconds: 60, rightRunSeconds: 60)
    }

    private static func atom(_ ordinal: Int, _ start: Double, _ end: Double) -> AtomEvidence {
        AtomEvidence(
            atomOrdinal: ordinal, startTime: start, endTime: end,
            isAnchored: true, anchorProvenance: [], hasAcousticBreakHint: false, correctionMask: .none
        )
    }

    private static func span(_ start: Double, _ end: Double, _ first: Int, _ last: Int) -> DecodedSpan {
        DecodedSpan(
            id: "s-\(first)-\(last)", assetId: Added.assetId,
            firstAtomOrdinal: first, lastAtomOrdinal: last,
            startTime: start, endTime: end, anchorProvenance: []
        )
    }

    // computeRediffSlotPass folds the confirmed ranges into the newly-enclosed
    // gate (`protectedRanges = vetoedRanges + confirmedRanges`). A confirmed
    // region [10,20] that a wide slot [0,50] would NEWLY enclose (the core
    // [40,50] does not touch it) must block the widening — mirroring the veto §5
    // gate. This exercises the exact public API the service calls.
    @Test("a wide slot NEWLY enclosing a confirmed region is blocked → status-quo width")
    func confirmedBlocksWidening() {
        let spans = [Self.span(40, 50, 4, 4)]
        let atoms = [Self.atom(4, 40, 50)]
        let bundle = RediffSlotOwnership.candidates(
            decodedSpans: spans,
            atomEvidence: atoms,
            playedSlots: [Self.played(0, 50)],
            vetoedRanges: [TimeRange(start: 10, end: 20)],  // confirmed region, folded in
            coreBankMatch: [false],
            slotBankMatch: [false]
        )
        #expect(bundle.synthesizedSlots[0] == nil, "the confirmed region blocks the rediff widening")
        let result = SpliceSlotDispositionEngine.computeDispositions(bundle.candidates)
        #expect(result.dispositions[0] == .noSlot)
        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: spans, dispositions: result.dispositions,
            atomEvidence: atoms, provenance: .rediffSlot
        )
        #expect(rewrite.absorbedIds.isEmpty)
        #expect(rewrite.finalSpans.first?.startTime == 40 && rewrite.finalSpans.first?.endTime == 50)
    }

    // A confirmed span whose slot is stripped (as computeRediffSlotPass does)
    // resolves to `.noSlot` → carried at minted width (never reshaped).
    @Test("a confirmed span with its slot stripped is kept at minted width (no reshape)")
    func confirmedSpanKeptAtMintedWidth() {
        let confirmed = Self.span(40, 50, 4, 4)
        let atoms = [Self.atom(4, 40, 50)]
        // Slot nulled (the service strips the confirmed span's own slot).
        let candidate = SpliceSlotCandidate(
            mintedInterval: TimeRange(start: 40, end: 50),
            slot: nil, slotIntersectsAtoms: true
        )
        let result = SpliceSlotDispositionEngine.computeDispositions([candidate])
        #expect(result.dispositions[0] == .noSlot)
        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: [confirmed], dispositions: result.dispositions,
            atomEvidence: atoms, provenance: .rediffSlot
        )
        #expect(rewrite.finalSpans.count == 1)
        #expect(rewrite.finalSpans[0].startTime == 40 && rewrite.finalSpans[0].endTime == 50)
        #expect(rewrite.supersededIds.isEmpty)
    }
}

// MARK: - R2 provenance rewrite

@Suite("playhead-527u R2 — a .userCorrection span is never absorbed/superseded")
struct AddedMarkProvenanceRewriteTests {

    private static func atomEv(_ ordinal: Int, _ start: Double, _ end: Double) -> AtomEvidence {
        AtomEvidence(
            atomOrdinal: ordinal, startTime: start, endTime: end,
            isAnchored: true, anchorProvenance: [], hasAcousticBreakHint: false, correctionMask: .none
        )
    }

    private static func slot(_ start: Double, _ end: Double) -> SpliceSlot {
        SpliceSlot(
            startTime: start, endTime: end,
            startEdge: SpliceEdgeEvidence(time: start, stepScore: 0.5, contributingSignals: 1),
            endEdge: SpliceEdgeEvidence(time: end, stepScore: 0.5, contributingSignals: 1),
            slotConfidence: 0.5, coreCoverage: 1.0
        )
    }

    @Test("a would-be-absorbed .userCorrection span is carried through verbatim")
    func userCorrectionSpanNeverAbsorbed() {
        // Index 0: an absorber that keeps a wide slot [0,50] enclosing both spans.
        // Index 1: a user-correction span [20,30] the slot would ordinarily absorb.
        let absorber = DecodedSpan(
            id: "absorber", assetId: Added.assetId,
            firstAtomOrdinal: 0, lastAtomOrdinal: 0,
            startTime: 0, endTime: 10, anchorProvenance: []
        )
        let userSpan = DecodedSpan(
            id: "user-correction", assetId: Added.assetId,
            firstAtomOrdinal: 2, lastAtomOrdinal: 2,
            startTime: 20, endTime: 30,
            anchorProvenance: [.userCorrection(correctionId: "cid-1", reportedTime: 25)]
        )
        let atoms = (0..<5).map { Self.atomEv($0, Double($0) * 10, Double($0) * 10 + 10) }

        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: [absorber, userSpan],
            dispositions: [.keepSlot(Self.slot(0, 50)), .absorbed(absorberIndex: 0)],
            atomEvidence: atoms,
            provenance: .rediffSlot
        )
        // The user-correction span survives verbatim — never dropped, never
        // marked for deletion.
        #expect(rewrite.finalSpans.contains { $0.id == "user-correction" },
                "a .userCorrection span must be carried through even when a slot would absorb it")
        #expect(!rewrite.absorbedIds.contains("user-correction"))
        #expect(!rewrite.supersededIds.contains("user-correction"))
    }

    /// playhead-527u ACOUSTIC-ASYMMETRY GUARD (reviewer 527u). A force-anchored
    /// span from `recordUserMarkedAd` carries NO `.userCorrection` provenance —
    /// only a `.userConfirmed` ATOM mask. The shared `SpliceSlotRewriter` must
    /// still refuse to reshape/absorb it (this is the sole protection for the
    /// dormant acoustic splice ownership pass, which — unlike rediff — has no
    /// confirmed-range slot-strip gate). Here the span would be RESHAPED by a
    /// wide kept slot; the guard must carry it verbatim (same id, same bounds).
    @Test("a force-anchored (userConfirmed-atom) span with NO provenance is not reshaped by the rewriter")
    func forceAnchoredSpanNeverReshaped() {
        // Span [20,30], EMPTY provenance — its only protection is the confirmed
        // atom mask on the atom it covers.
        let forced = DecodedSpan(
            id: "forced-mark", assetId: Added.assetId,
            firstAtomOrdinal: 2, lastAtomOrdinal: 2,
            startTime: 20, endTime: 30, anchorProvenance: []
        )
        // atom 2 [20,30] is .userConfirmed; the rest are .none.
        let atoms: [AtomEvidence] = (0..<5).map { ord in
            AtomEvidence(
                atomOrdinal: ord, startTime: Double(ord) * 10, endTime: Double(ord) * 10 + 10,
                isAnchored: true, anchorProvenance: [], hasAcousticBreakHint: false,
                correctionMask: ord == 2 ? .userConfirmed : .none
            )
        }
        // A wide kept slot [0,50] would ordinarily reshape the span to [0,50].
        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: [forced],
            dispositions: [.keepSlot(Self.slot(0, 50))],
            atomEvidence: atoms,
            provenance: .spliceSlot
        )
        #expect(rewrite.finalSpans.count == 1)
        #expect(rewrite.finalSpans.first?.id == "forced-mark",
                "the force-anchored span keeps its id — not reshaped into a new slot-wide id")
        #expect(rewrite.finalSpans.first?.startTime == 20 && rewrite.finalSpans.first?.endTime == 30,
                "the force-anchored span keeps its minted [20,30] bounds")
        #expect(rewrite.supersededIds.isEmpty)
        #expect(rewrite.absorbedIds.isEmpty)
    }
}

// MARK: - R3 reconcile dominance dedupe (over-drop guard)

@Suite("playhead-527u R3 — reconcile drops only the DOMINATED (redundant) window")
struct AddedMarkReconcileDominanceTests {

    /// A minimal reconcilable-shaped fusion AdWindow (current detector version).
    private static func fusion(
        _ id: String, _ start: Double, _ end: Double, assetId: String
    ) -> AdWindow {
        AdWindow(
            id: id, analysisAssetId: assetId,
            startTime: start, endTime: end, confidence: 0.85,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "test-detection-v1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: start,
            metadataSource: "none",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
    }

    private static func userMarkedRow(
        _ id: String, _ start: Double, _ end: Double, assetId: String
    ) -> AdWindow {
        AdWindow(
            id: id, analysisAssetId: assetId,
            startTime: start, endTime: end, confidence: 1.0,
            boundaryState: "userMarked",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "userCorrection",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: start,
            metadataSource: "userCorrection",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
    }

    /// OVER-DROP GUARD (reviewer 527u, mandate #1). A genuinely-distinct ad that
    /// partially overlaps a user mark must NOT be dropped by the dedupe — only
    /// the REDUNDANT window the mark DOMINATES is. Mark [35,55]; redundant
    /// [30,60] (mark covers 20/30 = 67% ≥ 50% → dominated → dropped, the
    /// backfill-atom-aligned re-detection of the same ad); distinct wider
    /// [50,120] (mark covers 5/70 ≈ 7% — NOT dominated → KEPT, a legitimate
    /// auto-skip-eligible detection preserved). RED with R1's blanket
    /// overlap-drop (BOTH windows retired → the distinct ad's skip is lost);
    /// GREEN with the dominance predicate.
    @Test("a distinct wider ad overlapping the mark survives; only the dominated redundant window is dropped")
    func distinctWiderAdSurvivesDominatedDropped() async throws {
        let assetId = "asset-527u-r3"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        try await store.insertAdWindow(Self.userMarkedRow("um-r3", 35, 55, assetId: assetId))
        let service = Added.service(store: store)

        let redundant = Self.fusion("fusion-redundant-r3", 30, 60, assetId: assetId)
        let distinct = Self.fusion("fusion-distinct-r3", 50, 120, assetId: assetId)

        let result = try await service.reconcileBackfillWindows(
            [redundant, distinct], analysisAssetId: assetId
        )
        let persistIds = Set(result.windows.map(\.id))
        #expect(!persistIds.contains("fusion-redundant-r3"),
                "the redundant window the mark DOMINATES must be dropped (double-window dedupe)")
        #expect(persistIds.contains("fusion-distinct-r3"),
                "a genuinely-distinct wider ad merely overlapping the mark must be KEPT — not silently retired (precision/coverage loss)")
    }

    /// RECIPROCAL-DOMINANCE OVER-DROP GUARD (reviewer 527u, mandate #2a). A SMALL
    /// DISTINCT ad lying MOSTLY INSIDE a LARGER user mark must NOT be dropped: the
    /// mark covers ≥half the small window (so the OLD one-sided 0.5-dominance
    /// dropped it, losing the skip of the portion beyond the mark) but the window
    /// covers only a SLIVER of the mark, so it is NOT "the marked ad re-detected".
    /// Mark [35,55]; small distinct [48,58] (mark covers 7/10 = 70% of the window
    /// → one-sided DROP; window covers 7/20 = 35% of the mark → reciprocal KEEP);
    /// redundant re-detection [30,60] (mark covers 20/30 = 67% of the window AND
    /// window covers 20/20 = 100% of the mark → reciprocally dominated → DROPPED).
    /// RED with the one-sided predicate (the small distinct ad is retired); GREEN
    /// with reciprocal dominance.
    @Test("a small distinct ad mostly INSIDE a larger mark survives; the true re-detection is still dropped")
    func smallDistinctAdInsideMarkSurvives() async throws {
        let assetId = "asset-527u-r3-inside"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        try await store.insertAdWindow(Self.userMarkedRow("um-inside", 35, 55, assetId: assetId))
        let service = Added.service(store: store)

        let redundant = Self.fusion("fusion-redundant-inside", 30, 60, assetId: assetId)
        let smallDistinct = Self.fusion("fusion-small-distinct", 48, 58, assetId: assetId)

        let result = try await service.reconcileBackfillWindows(
            [redundant, smallDistinct], analysisAssetId: assetId
        )
        let persistIds = Set(result.windows.map(\.id))
        #expect(!persistIds.contains("fusion-redundant-inside"),
                "the reciprocally-dominated re-detection [30,60] must be dropped (double-window dedupe)")
        #expect(persistIds.contains("fusion-small-distinct"),
                "a small distinct ad [48,58] mostly-inside a larger mark must survive — the one-sided 0.5-dominance wrongly retired it (a precision/coverage loss)")
    }

    /// NO-BLEED: with NO userMarked rows the dominance dedupe drops nothing —
    /// every fusion window is returned for persistence unchanged (an uncorrected
    /// asset's reconcile is unaffected by the 527u dedupe path).
    @Test("no userMarked row ⇒ dominance dedupe drops nothing (no bleed into ordinary assets)")
    func noUserMarkNoDrop() async throws {
        let assetId = "asset-527u-r3-clean"
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: assetId))
        let service = Added.service(store: store)
        let a = Self.fusion("fusion-a", 10, 30, assetId: assetId)
        let b = Self.fusion("fusion-b", 40, 80, assetId: assetId)
        let result = try await service.reconcileBackfillWindows([a, b], analysisAssetId: assetId)
        #expect(Set(result.windows.map(\.id)) == ["fusion-a", "fusion-b"],
                "no user mark ⇒ nothing dominated ⇒ all fusion windows persist unchanged")
    }
}

// MARK: - B2 auto-skip end-state (definitive user mark auto-skips)

@Suite("playhead-527u B2 — the definitive user mark auto-skips at its boundaries")
struct AddedMarkAutoSkipEligibleTests {

    /// PRODUCT-OWNER AC (reviewer 527u). After a user marks a missed ad, the
    /// persisted userMarked window is auto-skip-ELIGIBLE (gate == .eligible) and,
    /// preloaded in AUTO mode, auto-skips (decision `.applied`) over the USER'S
    /// marked boundaries [35,55] — not a banner-only markOnly row. RED pre-fix:
    /// `recordUserMarkedAd` left the gate nil (the persisted-gate assertion
    /// fails). The `.applied`-over-user-boundaries assertion is the end-state
    /// proof the skip fires at the user's span. Deterministic (no wall-clock
    /// sleep): `getDecisionLog()` is populated synchronously within
    /// `beginEpisode`'s preload → evaluateAndPush.
    @Test("persisted user mark is gate=.eligible and auto-skips over the user's boundaries in auto mode")
    func userMarkAutoSkipsAtUserBoundaries() async throws {
        let assetId = "asset-527u-b2"
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId, episodeId: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)
        let service = Added.service(store: store)
        await service.setUserCorrectionStore(corrections)

        let markStart = 35.0, markEnd = 55.0
        await service.recordUserMarkedAd(
            analysisAssetId: assetId, startTime: markStart, endTime: markEnd,
            podcastId: "podcast-1"
        )

        // (1) Persisted gate is definitive-eligible.
        let persisted = try await store.fetchAdWindows(assetId: assetId)
            .first { $0.boundaryState == "userMarked" }
        #expect(persisted?.eligibilityGate == SkipEligibilityGate.eligible.rawValue,
                "recordUserMarkedAd must stamp the definitive mark auto-skip-eligible")

        // (2) Auto mode: the preloaded eligible row auto-skips over [35,55].
        let trust = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let orchestrator = SkipOrchestrator(store: store, trustService: trust)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: assetId, episodeId: assetId, podcastId: "podcast-1"
        )

        let applied = await orchestrator.getDecisionLog().filter { $0.decision == .applied }
        #expect(applied.contains { rec in
            rec.snappedStart <= markStart + 0.01 && rec.snappedEnd >= markEnd - 0.01
        }, "the definitive user mark must AUTO-SKIP over the user's [35,55] boundaries; applied=\(applied.map { ($0.snappedStart, $0.snappedEnd) })")
    }

    /// MUST-RESOLVE #1 (reviewer 527u) — the DEFINITIVE answer. A PRE-EXISTING
    /// userMarked row persisted BEFORE 527u carries `eligibilityGate == nil` (the
    /// pre-fix `recordUserMarkedAd` shape). The product owner is actively
    /// dogfooding and already has such rows (e.g. THEMOVE). This proves such a
    /// nil-gate row STILL auto-skips over its span on the reload path, so NO
    /// migration is needed and pre-existing marks are NOT silently stuck
    /// banner-only. Mechanism: `beginEpisode` preloads the row (confidence 1.0,
    /// `.confirmed` ⇒ preload-eligible) into `receiveAdWindows`, whose gate filter
    /// drops ONLY recognised NON-eligible cases (`.markOnly` → suggest,
    /// `.blocked*` → dropped); `nil` falls THROUGH to the managed path and
    /// `evaluateWindow` never re-checks the gate → `.auto` mode auto-skips.
    ///
    /// NEGATION (proves the nil fall-through is load-bearing): tightening the
    /// `receiveAdWindows` guard from `if let decoded = decodedGate, decoded !=
    /// .eligible` to `if decodedGate != .eligible` (so `nil` is also dropped)
    /// turns this RED — captured by the reviewer as the pre-fix negation.
    @Test("a PRE-EXISTING userMarked row with gate == nil STILL auto-skips on reload (no migration needed)")
    func preexistingNilGateUserMarkAutoSkips() async throws {
        let assetId = "asset-527u-nilgate"
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId, episodeId: assetId))
        let markStart = 35.0, markEnd = 55.0

        // The legacy on-disk shape: recordUserMarkedAd PRE-527u left the gate NIL.
        // Direct insert (not recordUserMarkedAd, which now stamps .eligible).
        let legacy = AdWindow(
            id: "legacy-usermark",
            analysisAssetId: assetId,
            startTime: markStart, endTime: markEnd, confidence: 1.0,
            boundaryState: "userMarked",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "userCorrection",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: markStart,
            metadataSource: "userCorrection",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false
            // eligibilityGate omitted ⇒ nil (the pre-527u persisted shape).
        )
        try await store.insertAdWindow(legacy)
        let persisted = try await store.fetchAdWindows(assetId: assetId)
            .first { $0.id == "legacy-usermark" }
        #expect(persisted?.eligibilityGate == nil,
                "precondition: the legacy row's gate is nil (the pre-527u shape)")

        let trust = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let orchestrator = SkipOrchestrator(store: store, trustService: trust)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: assetId, episodeId: assetId, podcastId: "podcast-1"
        )

        let applied = await orchestrator.getDecisionLog().filter { $0.decision == .applied }
        #expect(applied.contains { rec in
            rec.snappedStart <= markStart + 0.01 && rec.snappedEnd >= markEnd - 0.01
        }, "a gate==nil userMarked row MUST still auto-skip over [35,55] on reload — pre-existing dogfood marks are not silently banner-only; applied=\(applied.map { ($0.snappedStart, $0.snappedEnd) })")
    }

    /// MODE-GATE GUARD (reviewer 527u — property 3 "no unintended auto-skip in
    /// non-auto modes"). The `.eligible` stamp only lets the userMarked row PASS
    /// the `receiveAdWindows` gate FILTER (into the managed set); the trust MODE
    /// gate in `evaluateWindow` still governs whether a skip fires. In `.manual`
    /// mode the row is INGESTED and evaluated to `.confirmed` (banner + manual
    /// "Skip Ad" affordance) but is NEVER auto-skipped. The first #expect proves
    /// the row was ingested (not vacuously green by being dropped); the second is
    /// the trust guarantee. RED if a future change special-cases userMarked to
    /// force-skip regardless of mode.
    @Test("a definitive user mark does NOT auto-skip in manual mode (the mode gate still governs)")
    func userMarkDoesNotAutoSkipInManualMode() async throws {
        let assetId = "asset-527u-b2-manual"
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId, episodeId: assetId))
        let corrections = PersistentUserCorrectionStore(store: store)
        let service = Added.service(store: store)
        await service.setUserCorrectionStore(corrections)

        let markStart = 35.0, markEnd = 55.0
        await service.recordUserMarkedAd(
            analysisAssetId: assetId, startTime: markStart, endTime: markEnd,
            podcastId: "podcast-1"
        )

        let trust = try await makeSkipTestTrustService(mode: "manual", trustScore: 0.9, observations: 10)
        let orchestrator = SkipOrchestrator(store: store, trustService: trust)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: assetId, episodeId: assetId, podcastId: "podcast-1"
        )

        let overMark = await orchestrator.getDecisionLog().filter {
            $0.snappedStart <= markStart + 0.01 && $0.snappedEnd >= markEnd - 0.01
        }
        #expect(!overMark.isEmpty,
                "the eligible user mark must still be INGESTED and evaluated in manual mode (not dropped) — else the no-skip assertion below is vacuous")
        #expect(!overMark.contains { $0.decision == .applied },
                "a definitive user mark must NOT auto-skip in manual mode — the mode gate governs the skip; over-mark decisions=\(overMark.map { $0.decision })")
    }
}

// MARK: - E1 cross-user export no-leak (eligibility stamp stays local)

@Suite("playhead-527u E1 — a userMarked .eligible row never leaks to cross-user export")
struct AddedMarkExportNoLeakTests {

    /// SIDE-EFFECT GUARD (reviewer 527u, mandate #3). Stamping the userMarked row
    /// `eligibilityGate == .eligible` must NOT let it escape the on-device
    /// boundary: `userMarked` is a LOCAL-ONLY boundary state (not an
    /// `AdBoundaryState` raw value), so `CrossUserAnalysisSnapshot.Window
    /// .exported(from:)` returns nil regardless of the gate. Proves the eligible
    /// stamp does not turn a private mark into a shareable cue.
    @Test("a userMarked row with gate == .eligible is excluded from the cross-user snapshot")
    func userMarkedEligibleRowNotExported() {
        let eligibleUserMark = AdWindow(
            id: "um-export",
            analysisAssetId: "asset-527u-e1",
            startTime: 35, endTime: 55, confidence: 1.0,
            boundaryState: "userMarked",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "userCorrection",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 35,
            metadataSource: "userCorrection",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        #expect(CrossUserAnalysisSnapshot.Window.exported(from: eligibleUserMark) == nil,
                "a userMarked (local-only) row must never be exported, even stamped .eligible")
    }
}
