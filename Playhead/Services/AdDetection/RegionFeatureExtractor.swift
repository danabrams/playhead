import Foundation

struct FMTranscriptQualityWindow: Sendable {
    let firstAtomOrdinal: Int
    let lastAtomOrdinal: Int
    let quality: TranscriptQuality
}

enum RegionTranscriptQualitySource: Sendable {
    case foundationModel
    case heuristic
}

struct RegionTranscriptQuality: Sendable {
    let quality: TranscriptQualityLevel
    let qualityScore: Double
    let source: RegionTranscriptQualitySource
}

struct RegionFeatureFMEvidence: Sendable {
    let commercialIntent: CommercialIntent
    let ownership: Ownership
    let consensusStrength: FMConsensusStrength
    let certainty: CertaintyBand
    let boundaryPrecision: BoundaryPrecision
    let memoryWriteEligible: Bool
    let alternativeExplanation: AlternativeExplanation
    let reasonTags: [ReasonTag]
    let resolvedEvidenceAnchors: [ResolvedEvidenceAnchor]
}

struct RegionFeatureBundle: Sendable {
    let region: ProposedRegion
    let lexicalScore: Double
    let lexicalHitCount: Int
    let lexicalCategories: Set<LexicalPatternCategory>
    let lexicalEvidenceText: String?
    let rmsDropScore: Double
    let spectralChangeScore: Double
    let musicScore: Double
    let speakerChangeScore: Double
    let priorScore: Double
    let transcriptQuality: RegionTranscriptQuality
    let fmEvidence: RegionFeatureFMEvidence
}

enum RegionFeatureExtractor {

    struct Input: Sendable {
        let regions: [ProposedRegion]
        let atoms: [TranscriptAtom]
        let featureWindows: [FeatureWindow]
        let episodeDuration: Double
        let priors: ShowPriors
        let podcastProfile: PodcastProfile?
        let fmTranscriptQualityWindows: [FMTranscriptQualityWindow]

        init(
            regions: [ProposedRegion],
            atoms: [TranscriptAtom],
            featureWindows: [FeatureWindow],
            episodeDuration: Double,
            priors: ShowPriors,
            podcastProfile: PodcastProfile? = nil,
            fmTranscriptQualityWindows: [FMTranscriptQualityWindow] = []
        ) {
            self.regions = regions
            self.atoms = atoms
            self.featureWindows = featureWindows
            self.episodeDuration = episodeDuration
            self.priors = priors
            self.podcastProfile = podcastProfile
            self.fmTranscriptQualityWindows = fmTranscriptQualityWindows
        }
    }

    struct Config: Sendable {
        static let `default` = Config()
    }

    static func extract(
        _ input: Input,
        config: Config = .default
    ) -> [RegionFeatureBundle] {
        let atomsByOrdinal = Dictionary(uniqueKeysWithValues: input.atoms.map { ($0.atomKey.atomOrdinal, $0) })
        let lexicalScanner = LexicalScanner(podcastProfile: input.podcastProfile)

        return input.regions.map { region in
            let regionAtoms = atoms(for: region, atomsByOrdinal: atomsByOrdinal)
            let regionText = regionAtoms.map(\.text).joined(separator: " ")
            let rescoredCandidate = lexicalScanner.rescoreRegionText(
                regionText,
                analysisAssetId: region.analysisAssetId,
                startTime: region.startTime,
                endTime: region.endTime
            )
            let overlappingWindows = featureWindows(
                for: region,
                in: input.featureWindows
            )
            let transcriptQuality = makeTranscriptQuality(
                region: region,
                regionAtoms: regionAtoms,
                fmTranscriptQualityWindows: input.fmTranscriptQualityWindows
            )

            return RegionFeatureBundle(
                region: region,
                lexicalScore: rescoredCandidate?.confidence ?? 0.0,
                lexicalHitCount: rescoredCandidate?.hitCount ?? 0,
                lexicalCategories: rescoredCandidate?.categories ?? [],
                lexicalEvidenceText: rescoredCandidate?.evidenceText,
                rmsDropScore: RegionScoring.computeRmsDropScore(windows: overlappingWindows),
                spectralChangeScore: RegionScoring.computeSpectralChangeScore(windows: overlappingWindows),
                musicScore: RegionScoring.computeMusicScore(windows: overlappingWindows),
                speakerChangeScore: RegionScoring.computeSpeakerChangeScore(windows: overlappingWindows),
                priorScore: RegionScoring.computePriorScore(
                    startTime: region.startTime,
                    endTime: region.endTime,
                    episodeDuration: input.episodeDuration,
                    priors: input.priors
                ),
                transcriptQuality: transcriptQuality,
                fmEvidence: makeFMEvidence(for: region)
            )
        }
    }

    private static func atoms(
        for region: ProposedRegion,
        atomsByOrdinal: [Int: TranscriptAtom]
    ) -> [TranscriptAtom] {
        (region.firstAtomOrdinal...region.lastAtomOrdinal)
            .compactMap { atomsByOrdinal[$0] }
    }

    private static func featureWindows(
        for region: ProposedRegion,
        in allWindows: [FeatureWindow]
    ) -> [FeatureWindow] {
        return allWindows
            .filter { window in
                window.endTime >= region.startTime && window.startTime <= region.endTime
            }
            .sorted { $0.startTime < $1.startTime }
    }

    private static func makeTranscriptQuality(
        region: ProposedRegion,
        regionAtoms: [TranscriptAtom],
        fmTranscriptQualityWindows: [FMTranscriptQualityWindow]
    ) -> RegionTranscriptQuality {
        let heuristicAssessment = heuristicTranscriptQuality(for: regionAtoms)
        let overlappingFMQualities = fmTranscriptQualityWindows
            .filter { qualityWindow in
                overlaps(
                    firstAtomOrdinal: region.firstAtomOrdinal,
                    lastAtomOrdinal: region.lastAtomOrdinal,
                    with: qualityWindow
                )
            }
            .map(\.quality)

        if let fmQuality = aggregateTranscriptQuality(overlappingFMQualities) {
            return RegionTranscriptQuality(
                quality: mapTranscriptQuality(fmQuality),
                qualityScore: heuristicAssessment.qualityScore,
                source: .foundationModel
            )
        }

        return RegionTranscriptQuality(
            quality: heuristicAssessment.quality,
            qualityScore: heuristicAssessment.qualityScore,
            source: .heuristic
        )
    }

    private static func heuristicTranscriptQuality(
        for atoms: [TranscriptAtom]
    ) -> TranscriptQualityAssessment {
        let segment = AdTranscriptSegment(atoms: atoms, segmentIndex: 0)
        return TranscriptQualityEstimator.assess(segment: segment)
    }

    private static func overlaps(
        firstAtomOrdinal: Int,
        lastAtomOrdinal: Int,
        with qualityWindow: FMTranscriptQualityWindow
    ) -> Bool {
        max(firstAtomOrdinal, qualityWindow.firstAtomOrdinal)
        <= min(lastAtomOrdinal, qualityWindow.lastAtomOrdinal)
    }

    private static func aggregateTranscriptQuality(
        _ qualities: [TranscriptQuality]
    ) -> TranscriptQuality? {
        guard !qualities.isEmpty else { return nil }
        if qualities.contains(.unusable) {
            return .unusable
        }
        if qualities.contains(.degraded) {
            return .degraded
        }
        return .good
    }

    private static func mapTranscriptQuality(
        _ quality: TranscriptQuality
    ) -> TranscriptQualityLevel {
        switch quality {
        case .good:
            return .good
        case .degraded:
            return .degraded
        case .unusable:
            return .unusable
        }
    }

    private static func makeFMEvidence(
        for region: ProposedRegion
    ) -> RegionFeatureFMEvidence {
        guard let fmEvidence = region.fmEvidence else {
            return RegionFeatureFMEvidence(
                commercialIntent: .unknown,
                ownership: .unknown,
                consensusStrength: .none,
                certainty: .weak,
                boundaryPrecision: .usable,
                memoryWriteEligible: false,
                alternativeExplanation: .none,
                reasonTags: [],
                resolvedEvidenceAnchors: []
            )
        }

        return RegionFeatureFMEvidence(
            commercialIntent: fmEvidence.commercialIntent,
            ownership: fmEvidence.ownership,
            consensusStrength: region.fmConsensusStrength,
            certainty: fmEvidence.certainty,
            boundaryPrecision: fmEvidence.boundaryPrecision,
            memoryWriteEligible: fmEvidence.memoryWriteEligible,
            alternativeExplanation: fmEvidence.alternativeExplanation,
            reasonTags: fmEvidence.reasonTags,
            resolvedEvidenceAnchors: fmEvidence.resolvedEvidenceAnchors
        )
    }
}
