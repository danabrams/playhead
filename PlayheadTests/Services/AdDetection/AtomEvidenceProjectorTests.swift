// AtomEvidenceProjectorTests.swift
// Phase 5 (playhead-4my.5.1): Unit tests for AtomEvidenceProjector.
// Covers all 5.1 acceptance criteria.

import Foundation
import Testing

@testable import Playhead

@Suite("AtomEvidenceProjector", .serialized)
struct AtomEvidenceProjectorTests {

    // MARK: - Helpers

    private func makeAtom(ordinal: Int, startTime: Double = 0, endTime: Double? = nil) -> TranscriptAtom {
        let end = endTime ?? (startTime + 1.0)
        return TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "test-asset",
                transcriptVersion: "tv1",
                atomOrdinal: ordinal
            ),
            contentHash: "hash-\(ordinal)",
            startTime: startTime,
            endTime: end,
            text: "word \(ordinal)",
            chunkIndex: ordinal
        )
    }

    private func makeAtoms(count: Int) -> [TranscriptAtom] {
        (0 ..< count).map { makeAtom(ordinal: $0, startTime: Double($0), endTime: Double($0) + 1.0) }
    }

    private func makeFMConsensusBundle(
        assetId: String = "test-asset",
        firstOrdinal: Int,
        lastOrdinal: Int,
        startTime: Double,
        endTime: Double,
        consensusStrength: FMConsensusStrength = .medium
    ) -> RegionFeatureBundle {
        let region = ProposedRegion(
            analysisAssetId: assetId,
            transcriptVersion: "tv1",
            firstAtomOrdinal: firstOrdinal,
            lastAtomOrdinal: lastOrdinal,
            startTime: startTime,
            endTime: endTime,
            origins: .foundationModel,
            fmConsensusStrength: consensusStrength,
            lexicalCandidates: [],
            sponsorMatches: [],
            fingerprintMatches: [],
            acousticBreaks: [],
            foundationModelSpans: [],
            resolvedEvidenceAnchors: [],
            fmEvidence: nil
        )
        return RegionFeatureBundle(
            region: region,
            lexicalScore: 0.5,
            lexicalHitCount: 0,
            lexicalCategories: [],
            lexicalEvidenceText: nil,
            rmsDropScore: 0,
            spectralChangeScore: 0,
            musicScore: 0,
            speakerChangeScore: 0,
            priorScore: 0,
            transcriptQuality: RegionTranscriptQuality(
                quality: .good,
                qualityScore: 1.0,
                source: .heuristic
            ),
            fmEvidence: RegionFeatureFMEvidence(
                commercialIntent: .paid,
                ownership: .unknown,
                consensusStrength: consensusStrength,
                certainty: .moderate,
                boundaryPrecision: .usable,
                memoryWriteEligible: false,
                alternativeExplanation: .none,
                reasonTags: [],
                resolvedEvidenceAnchors: []
            )
        )
    }

    private func makeAcousticBundle(
        assetId: String = "test-asset",
        firstOrdinal: Int,
        lastOrdinal: Int,
        startTime: Double,
        endTime: Double,
        breaks: [AcousticBreak] = []
    ) -> RegionFeatureBundle {
        let region = ProposedRegion(
            analysisAssetId: assetId,
            transcriptVersion: "tv1",
            firstAtomOrdinal: firstOrdinal,
            lastAtomOrdinal: lastOrdinal,
            startTime: startTime,
            endTime: endTime,
            origins: .acoustic,
            fmConsensusStrength: .none,
            lexicalCandidates: [],
            sponsorMatches: [],
            fingerprintMatches: [],
            acousticBreaks: breaks,
            foundationModelSpans: [],
            resolvedEvidenceAnchors: [],
            fmEvidence: nil
        )
        return RegionFeatureBundle(
            region: region,
            lexicalScore: 0,
            lexicalHitCount: 0,
            lexicalCategories: [],
            lexicalEvidenceText: nil,
            rmsDropScore: 0.8,
            spectralChangeScore: 0,
            musicScore: 0,
            speakerChangeScore: 0,
            priorScore: 0,
            transcriptQuality: RegionTranscriptQuality(
                quality: .good,
                qualityScore: 1.0,
                source: .heuristic
            ),
            fmEvidence: RegionFeatureFMEvidence(
                commercialIntent: .organic,
                ownership: .unknown,
                consensusStrength: .none,
                certainty: .weak,
                boundaryPrecision: .usable,
                memoryWriteEligible: false,
                alternativeExplanation: .none,
                reasonTags: [],
                resolvedEvidenceAnchors: []
            )
        )
    }

    private func makeSingleWindowFMBundle(
        assetId: String = "test-asset",
        firstOrdinal: Int,
        lastOrdinal: Int,
        startTime: Double,
        endTime: Double,
        breaks: [AcousticBreak] = []
    ) -> RegionFeatureBundle {
        // fmConsensusStrength == .low → single-window FM (>= .low, < .medium)
        let region = ProposedRegion(
            analysisAssetId: assetId,
            transcriptVersion: "tv1",
            firstAtomOrdinal: firstOrdinal,
            lastAtomOrdinal: lastOrdinal,
            startTime: startTime,
            endTime: endTime,
            origins: .foundationModel,
            fmConsensusStrength: .low,
            lexicalCandidates: [],
            sponsorMatches: [],
            fingerprintMatches: [],
            acousticBreaks: breaks,
            foundationModelSpans: [],
            resolvedEvidenceAnchors: [],
            fmEvidence: nil
        )
        return RegionFeatureBundle(
            region: region,
            lexicalScore: 0.3,
            lexicalHitCount: 0,
            lexicalCategories: [],
            lexicalEvidenceText: nil,
            rmsDropScore: 0,
            spectralChangeScore: 0,
            musicScore: 0,
            speakerChangeScore: 0,
            priorScore: 0,
            transcriptQuality: RegionTranscriptQuality(
                quality: .good,
                qualityScore: 1.0,
                source: .heuristic
            ),
            fmEvidence: RegionFeatureFMEvidence(
                commercialIntent: .affiliate,
                ownership: .unknown,
                consensusStrength: .low,
                certainty: .weak,
                boundaryPrecision: .usable,
                memoryWriteEligible: false,
                alternativeExplanation: .none,
                reasonTags: [],
                resolvedEvidenceAnchors: []
            )
        )
    }

    private func makeEvidenceEntry(
        ref: Int,
        category: EvidenceCategory,
        atomOrdinal: Int,
        text: String = "evidence"
    ) -> EvidenceEntry {
        EvidenceEntry(
            evidenceRef: ref,
            category: category,
            matchedText: text,
            normalizedText: text.lowercased(),
            atomOrdinal: atomOrdinal,
            startTime: Double(atomOrdinal),
            endTime: Double(atomOrdinal) + 0.5
        )
    }

    // MARK: - 5.1 Acceptance Criteria Tests

    @Test("Every input atom produces an AtomEvidence with startTime/endTime")
    func everyAtomProducesEvidence() async {
        let atoms = makeAtoms(count: 5)
        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [],
            catalog: EvidenceCatalog(analysisAssetId: "test-asset", transcriptVersion: "tv1", entries: []),
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        #expect(evidence.count == atoms.count)
        for (i, ev) in evidence.enumerated() {
            #expect(ev.startTime == atoms[i].startTime)
            #expect(ev.endTime == atoms[i].endTime)
            #expect(ev.atomOrdinal == i)
        }
    }

    @Test("Anchored atoms reflect FM-consensus regions with strength >= .medium")
    func fmConsensusRegionAnchorsAtoms() async {
        let atoms = makeAtoms(count: 5)
        let bundle = makeFMConsensusBundle(
            firstOrdinal: 1, lastOrdinal: 3,
            startTime: 1, endTime: 4,
            consensusStrength: .medium
        )
        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [bundle],
            catalog: EvidenceCatalog(analysisAssetId: "test-asset", transcriptVersion: "tv1", entries: []),
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        #expect(!evidence[0].isAnchored)  // ordinal 0 not in [1,3]
        #expect(evidence[1].isAnchored)
        #expect(evidence[2].isAnchored)
        #expect(evidence[3].isAnchored)
        #expect(!evidence[4].isAnchored)  // ordinal 4 not in [1,3]

        // Provenance contains fmConsensus
        let prov = evidence[1].anchorProvenance
        guard case .fmConsensus(_, _) = prov.first else {
            Issue.record("Expected fmConsensus anchor ref")
            return
        }
    }

    @Test("Trustworthy EvidenceEntry categories anchor atoms")
    func evidenceCatalogAnchorsAtoms() async {
        let atoms = makeAtoms(count: 5)
        let entries = [
            makeEvidenceEntry(ref: 0, category: .url, atomOrdinal: 2, text: "acme.com"),
            makeEvidenceEntry(ref: 1, category: .promoCode, atomOrdinal: 3, text: "CODE10"),
            makeEvidenceEntry(ref: 2, category: .disclosurePhrase, atomOrdinal: 4, text: "brought to you by"),
            makeEvidenceEntry(ref: 3, category: .ctaPhrase, atomOrdinal: 1, text: "sign up today"),
        ]
        let catalog = EvidenceCatalog(
            analysisAssetId: "test-asset",
            transcriptVersion: "tv1",
            entries: entries
        )
        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [],
            catalog: catalog,
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        #expect(!evidence[0].isAnchored)  // no entry at ordinal 0
        #expect(evidence[1].isAnchored)   // ctaPhrase
        #expect(evidence[2].isAnchored)   // url
        #expect(evidence[3].isAnchored)   // promoCode
        #expect(evidence[4].isAnchored)   // disclosurePhrase
    }

    @Test(".brandSpan EvidenceEntry does NOT anchor an atom alone")
    func brandSpanDoesNotAnchorAlone() async {
        let atoms = makeAtoms(count: 3)
        let entries = [
            makeEvidenceEntry(ref: 0, category: .brandSpan, atomOrdinal: 1, text: "Acme")
        ]
        let catalog = EvidenceCatalog(
            analysisAssetId: "test-asset",
            transcriptVersion: "tv1",
            entries: entries
        )
        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [],
            catalog: catalog,
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        #expect(!evidence[1].isAnchored)
    }

    @Test("Multiple anchor sources on same atom merge into anchorProvenance")
    func multipleAnchorSourcesMerge() async {
        let atoms = makeAtoms(count: 5)
        let bundle = makeFMConsensusBundle(
            firstOrdinal: 2, lastOrdinal: 2,
            startTime: 2, endTime: 3,
            consensusStrength: .medium
        )
        let entries = [
            makeEvidenceEntry(ref: 0, category: .url, atomOrdinal: 2, text: "acme.com")
        ]
        let catalog = EvidenceCatalog(
            analysisAssetId: "test-asset",
            transcriptVersion: "tv1",
            entries: entries
        )
        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [bundle],
            catalog: catalog,
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        let prov = evidence[2].anchorProvenance
        #expect(prov.count == 2)
        let hasFM = prov.contains { if case .fmConsensus = $0 { true } else { false } }
        let hasEvidence = prov.contains { if case .evidenceCatalog = $0 { true } else { false } }
        #expect(hasFM)
        #expect(hasEvidence)
    }

    @Test("NoCorrectionMaskProvider leaves all atoms unmasked")
    func noCorrectionMaskProviderLeavesUnmasked() async {
        let atoms = makeAtoms(count: 3)
        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [],
            catalog: EvidenceCatalog(analysisAssetId: "test-asset", transcriptVersion: "tv1", entries: []),
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        for ev in evidence {
            #expect(ev.correctionMask == .none)
        }
    }

    @Test("CorrectionMaskProvider with vetoed range marks atoms .userVetoed")
    func vetoedRangeMarksAtoms() async {
        let atoms = makeAtoms(count: 5)

        struct VetoProvider: CorrectionMaskProvider {
            func correctionMasks(for ordinals: ClosedRange<Int>, in assetId: String) async -> [Int: CorrectionState] {
                // Veto ordinals 1 and 2
                return [1: .userVetoed, 2: .userVetoed]
            }
        }

        let bundle = makeFMConsensusBundle(
            firstOrdinal: 0, lastOrdinal: 4,
            startTime: 0, endTime: 5,
            consensusStrength: .medium
        )
        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [bundle],
            catalog: EvidenceCatalog(analysisAssetId: "test-asset", transcriptVersion: "tv1", entries: []),
            atoms: atoms,
            correctionMaskProvider: VetoProvider()
        )

        #expect(evidence[0].correctionMask == .none)
        #expect(evidence[1].correctionMask == .userVetoed)
        #expect(evidence[2].correctionMask == .userVetoed)
        #expect(evidence[3].correctionMask == .none)
        #expect(evidence[4].correctionMask == .none)

        // Vetoed atoms should NOT be anchored even if covered by FM region
        #expect(evidence[0].isAnchored)
        #expect(!evidence[1].isAnchored)
        #expect(!evidence[2].isAnchored)
        #expect(evidence[3].isAnchored)
        #expect(evidence[4].isAnchored)
    }

    @Test("hasAcousticBreakHint: atoms covered by .acoustic regions have hint=true")
    func acousticRegionSetsBreakHint() async {
        let atoms = makeAtoms(count: 5)
        let acousticBreak = AcousticBreak(time: 2.5, breakStrength: 0.8, signals: [.spectralSpike])
        let acousticBundle = makeAcousticBundle(
            firstOrdinal: 2, lastOrdinal: 2,
            startTime: 2, endTime: 3,
            breaks: [acousticBreak]
        )
        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [acousticBundle],
            catalog: EvidenceCatalog(analysisAssetId: "test-asset", transcriptVersion: "tv1", entries: []),
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        #expect(!evidence[0].hasAcousticBreakHint)
        #expect(!evidence[1].hasAcousticBreakHint)
        #expect(evidence[2].hasAcousticBreakHint)
        #expect(!evidence[3].hasAcousticBreakHint)
        #expect(!evidence[4].hasAcousticBreakHint)
    }

    @Test("Use C: single-window FM + acoustic break within ±2 atoms → isAnchored=true with .fmAcousticCorroborated")
    func useCCorroborationAnchors() async {
        let atoms = makeAtoms(count: 10)

        // Single-window FM covers ordinal 5
        let singleWindowBundle = makeSingleWindowFMBundle(
            firstOrdinal: 5, lastOrdinal: 5,
            startTime: 5, endTime: 6
        )

        // Acoustic break at ordinal 4 (within ±2 of ordinal 5), strength >= 0.5
        let acousticBreak = AcousticBreak(time: 4.5, breakStrength: 0.7, signals: [.pauseCluster])
        let acousticBundle = makeAcousticBundle(
            firstOrdinal: 4, lastOrdinal: 4,
            startTime: 4, endTime: 5,
            breaks: [acousticBreak]
        )

        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [singleWindowBundle, acousticBundle],
            catalog: EvidenceCatalog(analysisAssetId: "test-asset", transcriptVersion: "tv1", entries: []),
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        #expect(evidence[5].isAnchored)
        let prov = evidence[5].anchorProvenance
        guard case .fmAcousticCorroborated(_, let strength) = prov.first else {
            Issue.record("Expected fmAcousticCorroborated anchor ref on ordinal 5")
            return
        }
        #expect(strength == 0.7)
    }

    @Test("Use C: single-window FM without co-located break → isAnchored=false")
    func useCWithoutBreakDoesNotAnchor() async {
        let atoms = makeAtoms(count: 10)
        let singleWindowBundle = makeSingleWindowFMBundle(
            firstOrdinal: 5, lastOrdinal: 5,
            startTime: 5, endTime: 6
        )
        // No acoustic bundle provided

        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [singleWindowBundle],
            catalog: EvidenceCatalog(analysisAssetId: "test-asset", transcriptVersion: "tv1", entries: []),
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        #expect(!evidence[5].isAnchored)
    }

    @Test("Use C: acoustic break alone (no FM) → isAnchored=false")
    func useCBreakAloneDoesNotAnchor() async {
        let atoms = makeAtoms(count: 10)
        let acousticBreak = AcousticBreak(time: 5.5, breakStrength: 0.9, signals: [.energyDrop])
        let acousticBundle = makeAcousticBundle(
            firstOrdinal: 5, lastOrdinal: 5,
            startTime: 5, endTime: 6,
            breaks: [acousticBreak]
        )

        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [acousticBundle],
            catalog: EvidenceCatalog(analysisAssetId: "test-asset", transcriptVersion: "tv1", entries: []),
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        #expect(!evidence[5].isAnchored)
    }

    @Test("Use C: break too far away (3+ atoms) → isAnchored=false")
    func useCBreakTooFarDoesNotAnchor() async {
        let atoms = makeAtoms(count: 15)

        // Single-window FM at ordinal 5
        let singleWindowBundle = makeSingleWindowFMBundle(
            firstOrdinal: 5, lastOrdinal: 5,
            startTime: 5, endTime: 6
        )

        // Acoustic break at ordinal 8 — 3 atoms away from ordinal 5 (exceeds ±2 radius)
        let acousticBreak = AcousticBreak(time: 8.5, breakStrength: 0.8, signals: [.spectralSpike])
        let acousticBundle = makeAcousticBundle(
            firstOrdinal: 8, lastOrdinal: 8,
            startTime: 8, endTime: 9,
            breaks: [acousticBreak]
        )

        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [singleWindowBundle, acousticBundle],
            catalog: EvidenceCatalog(analysisAssetId: "test-asset", transcriptVersion: "tv1", entries: []),
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        #expect(!evidence[5].isAnchored)
    }

    @Test("Use C: weak break (breakStrength < 0.5) → isAnchored=false")
    func useCWeakBreakDoesNotAnchor() async {
        let atoms = makeAtoms(count: 10)

        let singleWindowBundle = makeSingleWindowFMBundle(
            firstOrdinal: 5, lastOrdinal: 5,
            startTime: 5, endTime: 6
        )

        // Weak break (strength below 0.5 threshold)
        let weakBreak = AcousticBreak(time: 4.5, breakStrength: 0.3, signals: [.pauseCluster])
        let acousticBundle = makeAcousticBundle(
            firstOrdinal: 4, lastOrdinal: 4,
            startTime: 4, endTime: 5,
            breaks: [weakBreak]
        )

        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [singleWindowBundle, acousticBundle],
            catalog: EvidenceCatalog(analysisAssetId: "test-asset", transcriptVersion: "tv1", entries: []),
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        #expect(!evidence[5].isAnchored)
    }

    @Test("Empty atoms input produces empty output")
    func emptyAtomsProducesEmptyOutput() async {
        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [],
            catalog: EvidenceCatalog(analysisAssetId: "test-asset", transcriptVersion: "tv1", entries: []),
            atoms: [],
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        #expect(evidence.isEmpty)
    }

    @Test("FM region with strength < .medium (high) does not use primary anchor path")
    func fmRegionWithLowStrengthDoesNotAnchorPrimaryPath() async {
        let atoms = makeAtoms(count: 5)
        // strength = .low → should NOT anchor via primary FM path (needs acoustic break for Use C)
        let bundle = makeFMConsensusBundle(
            firstOrdinal: 2, lastOrdinal: 2,
            startTime: 2, endTime: 3,
            consensusStrength: .low
        )
        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: [bundle],
            catalog: EvidenceCatalog(analysisAssetId: "test-asset", transcriptVersion: "tv1", entries: []),
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        // Without acoustic corroboration, single-window FM alone doesn't anchor.
        #expect(!evidence[2].isAnchored)
    }
}
