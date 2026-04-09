import Foundation
import Testing

@testable import Playhead

@Suite("RegionFeatureExtractor")
struct RegionFeatureExtractorTests {

    @Test("FM-origin regions get a complete bundle and prefer provided FM transcript quality")
    func fmOriginRegionProducesCompleteBundle() {
        let atoms = makeRegionFeatureAtoms()
        let region = RegionProposalBuilder.build(
            RegionProposalInput(
                atoms: atoms,
                lexicalCandidates: [],
                acousticBreaks: [],
                sponsorMatches: [],
                fingerprintMatches: [],
                fmWindows: [
                    makeFMWindow(
                        windowIndex: 0,
                        sourceWindowIndex: 0,
                        lineRefs: Array(0...5),
                        spans: [
                            makeRefinedSpan(
                                firstAtomOrdinal: 0,
                                lastAtomOrdinal: 5,
                                certainty: .moderate,
                                anchors: [makeResolvedAnchor(lineRef: 2, evidenceRef: 101)]
                            )
                        ]
                    )
                ]
            )
        )[0]

        let bundle = RegionFeatureExtractor.extract(
            RegionFeatureExtractor.Input(
                regions: [region],
                atoms: atoms,
                featureWindows: makeRegionFeatureWindows(),
                episodeDuration: 100.0,
                priors: makeRegionFeaturePriors(),
                fmTranscriptQualityWindows: [
                    FMTranscriptQualityWindow(
                        firstAtomOrdinal: 0,
                        lastAtomOrdinal: 5,
                        quality: .degraded
                    )
                ]
            )
        )[0]

        let expectedLexicalScore = LexicalScanner().rescoreRegionText(
            makeRegionFeatureText(),
            analysisAssetId: "asset-1",
            startTime: 0.0,
            endTime: 6.0
        )?.confidence ?? 0.0

        expectNearlyEqual(bundle.lexicalScore, expectedLexicalScore)
        expectNearlyEqual(bundle.lexicalScore, 0.4818652849740932)
        #expect(bundle.lexicalHitCount == 3)
        #expect(bundle.lexicalCategories == [.sponsor, .promoCode, .purchaseLanguage])
        #expect(bundle.lexicalEvidenceText == "use code save20")
        expectNearlyEqual(bundle.rmsDropScore, 1.0)
        expectNearlyEqual(bundle.spectralChangeScore, 0.4)
        expectNearlyEqual(bundle.musicScore, 0.5)
        expectNearlyEqual(bundle.speakerChangeScore, 1.0)
        expectNearlyEqual(bundle.priorScore, 0.8)
        #expect(bundle.transcriptQuality.source == .foundationModel)
        #expect(bundle.transcriptQuality.quality == .degraded)
        #expect(bundle.fmEvidence.commercialIntent == .paid)
        #expect(bundle.fmEvidence.ownership == .thirdParty)
        #expect(bundle.fmEvidence.consensusStrength == .low)
        #expect(bundle.fmEvidence.certainty == .moderate)
        #expect(bundle.fmEvidence.boundaryPrecision == .usable)
        #expect(bundle.fmEvidence.alternativeExplanation == .none)
        #expect(bundle.fmEvidence.reasonTags.isEmpty)
        #expect(bundle.fmEvidence.resolvedEvidenceAnchors.compactMap { $0.entry?.evidenceRef } == [101])
    }

    @Test("lexical-origin regions get rescored lexical features and normalized default FM evidence")
    func lexicalOriginRegionProducesCompleteBundle() {
        let atoms = makeRegionFeatureAtoms()
        let region = RegionProposalBuilder.build(
            RegionProposalInput(
                atoms: atoms,
                lexicalCandidates: [
                    makeLexicalCandidate(startTime: 0.0, endTime: 6.0, confidence: 0.10)
                ],
                acousticBreaks: [],
                sponsorMatches: [],
                fingerprintMatches: [],
                fmWindows: []
            )
        )[0]

        let bundle = RegionFeatureExtractor.extract(
            RegionFeatureExtractor.Input(
                regions: [region],
                atoms: atoms,
                featureWindows: makeRegionFeatureWindows(),
                episodeDuration: 100.0,
                priors: makeRegionFeaturePriors()
            )
        )[0]

        let expectedLexicalScore = LexicalScanner().rescoreRegionText(
            makeRegionFeatureText(),
            analysisAssetId: "asset-1",
            startTime: 0.0,
            endTime: 6.0
        )?.confidence ?? 0.0

        expectNearlyEqual(bundle.lexicalScore, expectedLexicalScore)
        expectNearlyEqual(bundle.lexicalScore, 0.4818652849740932)
        #expect(bundle.lexicalScore != region.lexicalCandidates[0].confidence)
        #expect(bundle.lexicalHitCount == 3)
        #expect(bundle.lexicalCategories == [.sponsor, .promoCode, .purchaseLanguage])
        #expect(bundle.lexicalEvidenceText == "use code save20")
        expectNearlyEqual(bundle.rmsDropScore, 1.0)
        expectNearlyEqual(bundle.spectralChangeScore, 0.4)
        expectNearlyEqual(bundle.musicScore, 0.5)
        expectNearlyEqual(bundle.speakerChangeScore, 1.0)
        expectNearlyEqual(bundle.priorScore, 0.8)
        #expect(bundle.transcriptQuality.source == .heuristic)
        #expect(bundle.transcriptQuality.quality == .good)
        #expect(bundle.fmEvidence.commercialIntent == .unknown)
        #expect(bundle.fmEvidence.ownership == .unknown)
        #expect(bundle.fmEvidence.consensusStrength == .none)
        #expect(bundle.fmEvidence.certainty == .weak)
        #expect(bundle.fmEvidence.boundaryPrecision == .usable)
        #expect(bundle.fmEvidence.alternativeExplanation == .none)
        #expect(bundle.fmEvidence.reasonTags.isEmpty)
        #expect(bundle.fmEvidence.resolvedEvidenceAnchors.isEmpty)
    }

    @Test("non-lexical non-FM regions still get a complete neutral bundle")
    func sponsorOriginRegionWithoutLexicalOrFMEvidenceProducesNeutralBundle() {
        let atoms = makeNeutralRegionFeatureAtoms()
        let region = RegionProposalBuilder.build(
            RegionProposalInput(
                atoms: atoms,
                lexicalCandidates: [],
                acousticBreaks: [],
                sponsorMatches: [
                    makeRegionFeatureSponsorMatch(firstAtomOrdinal: 0, lastAtomOrdinal: 5)
                ],
                fingerprintMatches: [],
                fmWindows: []
            )
        )[0]

        let bundle = RegionFeatureExtractor.extract(
            RegionFeatureExtractor.Input(
                regions: [region],
                atoms: atoms,
                featureWindows: makeRegionFeatureWindows(),
                episodeDuration: 100.0,
                priors: makeRegionFeaturePriors()
            )
        )[0]

        #expect(bundle.lexicalScore == 0.0)
        #expect(bundle.lexicalHitCount == 0)
        #expect(bundle.lexicalCategories.isEmpty)
        #expect(bundle.lexicalEvidenceText == nil)
        expectNearlyEqual(bundle.rmsDropScore, 1.0)
        expectNearlyEqual(bundle.spectralChangeScore, 0.4)
        expectNearlyEqual(bundle.musicScore, 0.5)
        expectNearlyEqual(bundle.speakerChangeScore, 1.0)
        expectNearlyEqual(bundle.priorScore, 0.8)
        #expect(bundle.transcriptQuality.source == RegionTranscriptQualitySource.heuristic)
        #expect(bundle.fmEvidence.commercialIntent == .unknown)
        #expect(bundle.fmEvidence.ownership == .unknown)
        #expect(bundle.fmEvidence.consensusStrength == .none)
        #expect(bundle.fmEvidence.certainty == CertaintyBand.weak)
        #expect(bundle.fmEvidence.boundaryPrecision == BoundaryPrecision.usable)
        #expect(bundle.fmEvidence.alternativeExplanation == .none)
        #expect(bundle.fmEvidence.reasonTags.isEmpty)
        #expect(bundle.fmEvidence.resolvedEvidenceAnchors.isEmpty)
    }

    @Test("duplicate atom ordinals in the extractor input do not trap")
    func duplicateAtomOrdinalsAreTolerated() {
        let baseAtoms = makeNeutralRegionFeatureAtoms()
        // Append a duplicate of ordinal 2 — Dictionary(uniqueKeysWithValues:) would
        // trap; the defensive last-write-wins path must keep extraction running.
        let duplicate = TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "asset-1",
                transcriptVersion: "transcript-v1",
                atomOrdinal: 2
            ),
            contentHash: "neutral-hash-2-dup",
            startTime: 2.0,
            endTime: 3.0,
            text: "duplicate",
            chunkIndex: 2
        )
        let atoms = baseAtoms + [duplicate]

        let region = RegionProposalBuilder.build(
            RegionProposalInput(
                atoms: atoms,
                lexicalCandidates: [],
                acousticBreaks: [],
                sponsorMatches: [
                    makeRegionFeatureSponsorMatch(firstAtomOrdinal: 0, lastAtomOrdinal: 5)
                ],
                fingerprintMatches: [],
                fmWindows: []
            )
        )[0]

        let bundles = RegionFeatureExtractor.extract(
            RegionFeatureExtractor.Input(
                regions: [region],
                atoms: atoms,
                featureWindows: makeRegionFeatureWindows(),
                episodeDuration: 100.0,
                priors: makeRegionFeaturePriors()
            )
        )

        #expect(bundles.count == 1)
    }

    @Test("a hand-constructed region with inverted ordinals yields an empty bundle rather than trapping")
    func invertedRegionOrdinalsYieldEmptyAtoms() {
        let atoms = makeNeutralRegionFeatureAtoms()
        // Bypass the builder: fabricate a ProposedRegion with first > last.
        let invertedRegion = ProposedRegion(
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1",
            firstAtomOrdinal: 5,
            lastAtomOrdinal: 2,
            startTime: 5.0,
            endTime: 2.0,
            origins: .sponsor,
            fmConsensusStrength: .none,
            lexicalCandidates: [],
            sponsorMatches: [],
            fingerprintMatches: [],
            acousticBreaks: [],
            foundationModelSpans: [],
            resolvedEvidenceAnchors: [],
            fmEvidence: nil
        )

        let bundles = RegionFeatureExtractor.extract(
            RegionFeatureExtractor.Input(
                regions: [invertedRegion],
                atoms: atoms,
                featureWindows: makeRegionFeatureWindows(),
                episodeDuration: 100.0,
                priors: makeRegionFeaturePriors()
            )
        )

        // Must not trap. Inverted range → no region atoms → no lexical hits.
        #expect(bundles.count == 1)
        #expect(bundles[0].lexicalHitCount == 0)
        #expect(bundles[0].lexicalScore == 0.0)
        #expect(bundles[0].lexicalCategories.isEmpty)
    }
}

@Suite("RegionScoring")
struct RegionScoringTests {

    @Test("shared scoring helpers match the rule-based classifier on rich inputs")
    func regionScoringMatchesClassifierBreakdown() {
        let windows = makeRegionFeatureWindows()
        let priors = makeRegionFeaturePriors()
        let candidate = makeLexicalCandidate(startTime: 0.0, endTime: 6.0, confidence: 0.25)
        let classifier = RuleBasedClassifier()
        let result = classifier.classify(
            input: ClassifierInput(
                candidate: candidate,
                featureWindows: windows,
                episodeDuration: 100.0
            ),
            priors: priors
        )

        let rms = RegionScoring.computeRmsDropScore(windows: windows)
        let spectral = RegionScoring.computeSpectralChangeScore(windows: windows)
        let music = RegionScoring.computeMusicScore(windows: windows)
        let speaker = RegionScoring.computeSpeakerChangeScore(windows: windows)
        let prior = RegionScoring.computePriorScore(
            startTime: candidate.startTime,
            endTime: candidate.endTime,
            episodeDuration: 100.0,
            priors: priors
        )

        expectNearlyEqual(rms, result.signalBreakdown.rmsDropScore)
        expectNearlyEqual(spectral, result.signalBreakdown.spectralChangeScore)
        expectNearlyEqual(music, result.signalBreakdown.musicScore)
        expectNearlyEqual(speaker, result.signalBreakdown.speakerChangeScore)
        expectNearlyEqual(prior, result.signalBreakdown.priorScore)
        expectNearlyEqual(rms, 1.0)
        expectNearlyEqual(spectral, 0.4)
        expectNearlyEqual(music, 0.5)
        expectNearlyEqual(speaker, 1.0)
        expectNearlyEqual(prior, 0.8)
    }

    @Test("shared scoring helpers keep the classifier guard paths at zero")
    func regionScoringGuardPathsMatchClassifier() {
        let candidate = makeLexicalCandidate(startTime: 0.0, endTime: 6.0, confidence: 0.25)
        let classifier = RuleBasedClassifier()
        let result = classifier.classify(
            input: ClassifierInput(
                candidate: candidate,
                featureWindows: [],
                episodeDuration: 100.0
            ),
            priors: .empty
        )

        #expect(RegionScoring.computeRmsDropScore(windows: []) == 0.0)
        #expect(RegionScoring.computeSpectralChangeScore(windows: []) == 0.0)
        #expect(RegionScoring.computeMusicScore(windows: []) == 0.0)
        #expect(RegionScoring.computeSpeakerChangeScore(windows: []) == 0.0)
        #expect(RegionScoring.computePriorScore(
            startTime: candidate.startTime,
            endTime: candidate.endTime,
            episodeDuration: 100.0,
            priors: .empty
        ) == 0.0)
        #expect(result.signalBreakdown.rmsDropScore == 0.0)
        #expect(result.signalBreakdown.spectralChangeScore == 0.0)
        #expect(result.signalBreakdown.musicScore == 0.0)
        #expect(result.signalBreakdown.speakerChangeScore == 0.0)
        #expect(result.signalBreakdown.priorScore == 0.0)
    }
}

private func makeRegionFeatureText() -> String {
    makeRegionFeatureAtoms().map(\.text).joined(separator: " ")
}

private func makeRegionFeatureAtoms() -> [TranscriptAtom] {
    let texts = [
        "this episode",
        "is brought to",
        "you by acme",
        "corp use code",
        "SAVE20 free",
        "trial."
    ]

    return texts.enumerated().map { ordinal, text in
        TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "asset-1",
                transcriptVersion: "transcript-v1",
                atomOrdinal: ordinal
            ),
            contentHash: "hash-\(ordinal)",
            startTime: Double(ordinal),
            endTime: Double(ordinal + 1),
            text: text,
            chunkIndex: ordinal
        )
    }
}

private func makeNeutralRegionFeatureAtoms() -> [TranscriptAtom] {
    let texts = [
        "welcome back",
        "to the",
        "main show",
        "today we",
        "are discussing",
        "the episode."
    ]

    return texts.enumerated().map { ordinal, text in
        TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "asset-1",
                transcriptVersion: "transcript-v1",
                atomOrdinal: ordinal
            ),
            contentHash: "neutral-hash-\(ordinal)",
            startTime: Double(ordinal),
            endTime: Double(ordinal + 1),
            text: text,
            chunkIndex: ordinal
        )
    }
}

private func makeRegionFeatureWindows() -> [FeatureWindow] {
    let rmsValues = [0.1, 0.6, 0.6, 0.6, 0.6, 0.1]
    let spectralValues = [1.6, 0.1, 0.1, 0.1, 0.1, 1.5]
    let speakerIds = [1, 1, 2, 2, 3, 3]

    return rmsValues.enumerated().map { index, rms in
        FeatureWindow(
            analysisAssetId: "asset-1",
            startTime: Double(index),
            endTime: Double(index + 1),
            rms: rms,
            spectralFlux: spectralValues[index],
            musicProbability: 0.9,
            pauseProbability: 0.1,
            speakerClusterId: speakerIds[index],
            jingleHash: nil,
            featureVersion: 1
        )
    }
}

private func makeRegionFeaturePriors() -> ShowPriors {
    ShowPriors(
        slotPositions: [0.03],
        knownSponsors: [],
        jingleFingerprints: [],
        trustWeight: 0.8
    )
}

private func makeLexicalCandidate(
    startTime: Double,
    endTime: Double,
    confidence: Double = 0.8
) -> LexicalCandidate {
    LexicalCandidate(
        id: UUID().uuidString,
        analysisAssetId: "asset-1",
        startTime: startTime,
        endTime: endTime,
        confidence: confidence,
        hitCount: 2,
        categories: [.sponsor, .urlCTA],
        evidenceText: "evidence",
        detectorVersion: "lexical-v1"
    )
}

private func makeRegionFeatureSponsorMatch(
    firstAtomOrdinal: Int,
    lastAtomOrdinal: Int
) -> SponsorMatch {
    SponsorMatch(
        firstAtomOrdinal: firstAtomOrdinal,
        lastAtomOrdinal: lastAtomOrdinal,
        entityName: "Acme",
        confidence: 0.8,
        startTime: Double(firstAtomOrdinal),
        endTime: Double(lastAtomOrdinal + 1)
    )
}

private func makeResolvedAnchor(
    lineRef: Int,
    evidenceRef: Int,
    kind: EvidenceCategory = .brandSpan,
    source: CommercialEvidenceResolutionSource = .evidenceRef
) -> ResolvedEvidenceAnchor {
    ResolvedEvidenceAnchor(
        entry: EvidenceEntry(
            evidenceRef: evidenceRef,
            category: kind,
            matchedText: "anchor-\(evidenceRef)",
            normalizedText: "anchor-\(evidenceRef)",
            atomOrdinal: lineRef,
            startTime: Double(lineRef),
            endTime: Double(lineRef + 1)
        ),
        lineRef: lineRef,
        kind: kind,
        certainty: .strong,
        resolutionSource: source,
        memoryWriteEligible: true
    )
}

private func makeRefinedSpan(
    firstAtomOrdinal: Int,
    lastAtomOrdinal: Int,
    certainty: CertaintyBand = .moderate,
    anchors: [ResolvedEvidenceAnchor]
) -> RefinedAdSpan {
    RefinedAdSpan(
        commercialIntent: .paid,
        ownership: .thirdParty,
        firstLineRef: firstAtomOrdinal,
        lastLineRef: lastAtomOrdinal,
        firstAtomOrdinal: firstAtomOrdinal,
        lastAtomOrdinal: lastAtomOrdinal,
        certainty: certainty,
        boundaryPrecision: .usable,
        resolvedEvidenceAnchors: anchors,
        memoryWriteEligible: anchors.allSatisfy { $0.memoryWriteEligible },
        alternativeExplanation: .none,
        reasonTags: []
    )
}

private func makeFMWindow(
    windowIndex: Int,
    sourceWindowIndex: Int,
    lineRefs: [Int],
    spans: [RefinedAdSpan]
) -> FMRefinementWindowOutput {
    FMRefinementWindowOutput(
        windowIndex: windowIndex,
        sourceWindowIndex: sourceWindowIndex,
        lineRefs: lineRefs,
        spans: spans,
        latencyMillis: 12
    )
}

private func expectNearlyEqual(
    _ lhs: Double,
    _ rhs: Double,
    tolerance: Double = 1e-12
) {
    #expect(abs(lhs - rhs) <= tolerance)
}
