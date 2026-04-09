import Foundation

struct ProposedRegionOrigins: OptionSet, Sendable {
    let rawValue: Int

    static let lexical = ProposedRegionOrigins(rawValue: 1 << 0)
    static let acoustic = ProposedRegionOrigins(rawValue: 1 << 1)
    static let sponsor = ProposedRegionOrigins(rawValue: 1 << 2)
    static let fingerprint = ProposedRegionOrigins(rawValue: 1 << 3)
    static let foundationModel = ProposedRegionOrigins(rawValue: 1 << 4)
}

enum FMConsensusStrength: Double, Sendable, Comparable {
    case none = 0.0
    case low = 0.35
    case medium = 0.7
    case high = 1.0

    var value: Double { rawValue }

    static func < (lhs: FMConsensusStrength, rhs: FMConsensusStrength) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

struct ProposedRegion: Sendable {
    let analysisAssetId: String
    let transcriptVersion: String
    let firstAtomOrdinal: Int
    let lastAtomOrdinal: Int
    let startTime: Double
    let endTime: Double
    let origins: ProposedRegionOrigins
    let fmConsensusStrength: FMConsensusStrength
    let lexicalCandidates: [LexicalCandidate]
    let sponsorMatches: [SponsorMatch]
    let fingerprintMatches: [FingerprintMatch]
    /// Acoustic breaks associated with this region for provenance.
    ///
    /// The same `AcousticBreak` may appear in multiple `ProposedRegion`s when it
    /// is within `associateAcousticBreaks` edge tolerance of a neighbor but
    /// anchored to a different atom via `makeAcousticProposals`. This is
    /// per-region provenance, not global uniqueness — downstream consumers
    /// doing `regions.flatMap(\.acousticBreaks)` should `Set`-dedupe if they
    /// need a global break list.
    let acousticBreaks: [AcousticBreak]
    let foundationModelSpans: [RefinedAdSpan]
    let resolvedEvidenceAnchors: [ResolvedEvidenceAnchor]
    let fmEvidence: ProposedRegionFMEvidence?
}

struct ProposedRegionFMEvidence: Sendable {
    let commercialIntent: CommercialIntent
    let ownership: Ownership
    let certainty: CertaintyBand
    let boundaryPrecision: BoundaryPrecision
    let memoryWriteEligible: Bool
    let alternativeExplanation: AlternativeExplanation
    let reasonTags: [ReasonTag]
    let resolvedEvidenceAnchors: [ResolvedEvidenceAnchor]
}

struct RegionProposalInput: Sendable {
    let atoms: [TranscriptAtom]
    let lexicalCandidates: [LexicalCandidate]
    let acousticBreaks: [AcousticBreak]
    let sponsorMatches: [SponsorMatch]
    let fingerprintMatches: [FingerprintMatch]
    let fmWindows: [FMRefinementWindowOutput]
}

enum RegionProposalBuilder {

    struct Config: Sendable {
        let fmIoUThreshold: Double
        let minimumWindowCenterSeparation: Double
        let acousticBreakAssociationTolerance: Double

        static let `default` = Config(
            fmIoUThreshold: 0.4,
            minimumWindowCenterSeparation: 0.5,
            acousticBreakAssociationTolerance: 1.0
        )
    }

    static func build(
        _ input: RegionProposalInput,
        config: Config = .default
    ) -> [ProposedRegion] {
        let sortedAtoms = input.atoms.sorted { $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal }
        guard let firstAtom = sortedAtoms.first else { return [] }

        // Defense-in-depth: if two atoms share an ordinal (should not happen given
        // transcript invariants, but `uniqueKeysWithValues:` would trap on collision),
        // keep the last writer. Downstream consumers only need one atom per ordinal.
        let atomsByOrdinal = Dictionary(
            sortedAtoms.map { ($0.atomKey.atomOrdinal, $0) },
            uniquingKeysWith: { _, new in new }
        )
        let metadata = (
            analysisAssetId: firstAtom.atomKey.analysisAssetId,
            transcriptVersion: firstAtom.atomKey.transcriptVersion
        )

        var proposals = makeFMProposals(
            windows: input.fmWindows,
            atomsByOrdinal: atomsByOrdinal,
            config: config
        )
        proposals.append(contentsOf: makeLexicalProposals(input.lexicalCandidates, atoms: sortedAtoms))
        proposals.append(contentsOf: makeSponsorProposals(input.sponsorMatches, atomsByOrdinal: atomsByOrdinal))
        proposals.append(contentsOf: makeFingerprintProposals(input.fingerprintMatches, atomsByOrdinal: atomsByOrdinal))
        proposals.append(contentsOf: makeAcousticProposals(input.acousticBreaks, atomsByOrdinal: atomsByOrdinal))
        proposals.sort { lhs, rhs in
            if lhs.range.firstAtomOrdinal == rhs.range.firstAtomOrdinal {
                return lhs.range.lastAtomOrdinal < rhs.range.lastAtomOrdinal
            }
            return lhs.range.firstAtomOrdinal < rhs.range.firstAtomOrdinal
        }

        var merged: [SourceProposal] = []
        for proposal in proposals {
            if let existingIndex = bestMergeIndex(for: proposal, in: merged) {
                merged[existingIndex] = merge(existing: merged[existingIndex], incoming: proposal)
            } else {
                merged.append(proposal)
            }
        }

        return merged.map { proposal in
            // Belt-and-suspenders: `makeAcousticProposals` spawns standalone
            // acoustic proposals anchored to the atom containing each break.
            // `associateAcousticBreaks` additionally decorates proposals whose
            // edges are within tolerance of a break but did not happen to land
            // in the same atom. Union + dedupe the two paths so a single break
            // that fires via both routes only appears once in the output.
            let associatedBreaks = associateAcousticBreaks(
                input.acousticBreaks,
                with: proposal.range,
                tolerance: config.acousticBreakAssociationTolerance
            )
            let mergedBreaks = dedupAcousticBreaks(proposal.acousticBreaks + associatedBreaks)
            var origins = proposal.origins
            if !mergedBreaks.isEmpty {
                origins.insert(.acoustic)
            }
            return ProposedRegion(
                analysisAssetId: metadata.analysisAssetId,
                transcriptVersion: metadata.transcriptVersion,
                firstAtomOrdinal: proposal.range.firstAtomOrdinal,
                lastAtomOrdinal: proposal.range.lastAtomOrdinal,
                startTime: proposal.range.startTime,
                endTime: proposal.range.endTime,
                origins: origins,
                fmConsensusStrength: proposal.fmConsensusStrength,
                lexicalCandidates: proposal.lexicalCandidates,
                sponsorMatches: proposal.sponsorMatches,
                fingerprintMatches: proposal.fingerprintMatches,
                acousticBreaks: mergedBreaks,
                foundationModelSpans: proposal.foundationModelSpans,
                resolvedEvidenceAnchors: proposal.resolvedEvidenceAnchors,
                fmEvidence: aggregateFMEvidence(from: proposal)
            )
        }
        .sorted { lhs, rhs in
            if lhs.firstAtomOrdinal == rhs.firstAtomOrdinal {
                return lhs.lastAtomOrdinal < rhs.lastAtomOrdinal
            }
            return lhs.firstAtomOrdinal < rhs.firstAtomOrdinal
        }
    }

    private static func makeLexicalProposals(
        _ candidates: [LexicalCandidate],
        atoms: [TranscriptAtom]
    ) -> [SourceProposal] {
        candidates.compactMap { candidate in
            guard let range = canonicalRange(
                startTime: candidate.startTime,
                endTime: candidate.endTime,
                atoms: atoms
            ) else {
                return nil
            }
            return SourceProposal(
                range: range,
                origins: .lexical,
                lexicalCandidates: [candidate]
            )
        }
    }

    private static func makeSponsorProposals(
        _ matches: [SponsorMatch],
        atomsByOrdinal: [Int: TranscriptAtom]
    ) -> [SourceProposal] {
        matches.compactMap { match in
            guard let range = canonicalRange(
                firstAtomOrdinal: match.firstAtomOrdinal,
                lastAtomOrdinal: match.lastAtomOrdinal,
                atomsByOrdinal: atomsByOrdinal
            ) else {
                return nil
            }
            return SourceProposal(
                range: range,
                origins: .sponsor,
                sponsorMatches: [match]
            )
        }
    }

    private static func makeFingerprintProposals(
        _ matches: [FingerprintMatch],
        atomsByOrdinal: [Int: TranscriptAtom]
    ) -> [SourceProposal] {
        matches.compactMap { match in
            guard let range = canonicalRange(
                firstAtomOrdinal: match.firstAtomOrdinal,
                lastAtomOrdinal: match.lastAtomOrdinal,
                atomsByOrdinal: atomsByOrdinal
            ) else {
                return nil
            }
            return SourceProposal(
                range: range,
                origins: .fingerprint,
                fingerprintMatches: [match]
            )
        }
    }

    /// Promote each `AcousticBreak` to a 1-atom-wide standalone proposal with
    /// `.acoustic` origin. Width comes for free from merging with adjacent
    /// lex/FM/sponsor/fingerprint proposals in the main merge loop — standalone
    /// acoustic regions are anchors/hints for Phase 5/6, not classifiable spans.
    /// Breaks whose `time` does not land in any atom are dropped silently.
    /// Iteration follows `AcousticBreakDetector.detectBreaks` ordering (sorted
    /// by time) so output is deterministic.
    private static func makeAcousticProposals(
        _ breaks: [AcousticBreak],
        atomsByOrdinal: [Int: TranscriptAtom]
    ) -> [SourceProposal] {
        guard !atomsByOrdinal.isEmpty else { return [] }
        let sortedAtoms = atomsByOrdinal.values.sorted {
            $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal
        }
        return breaks.compactMap { breakPoint -> SourceProposal? in
            // Find the atom whose [startTime, endTime) contains the break time.
            guard let atom = sortedAtoms.first(where: { atom in
                atom.startTime <= breakPoint.time && breakPoint.time < atom.endTime
            }) else {
                return nil
            }
            let range = CanonicalRange(
                firstAtomOrdinal: atom.atomKey.atomOrdinal,
                lastAtomOrdinal: atom.atomKey.atomOrdinal,
                startTime: atom.startTime,
                endTime: atom.endTime
            )
            return SourceProposal(
                range: range,
                origins: .acoustic,
                acousticBreaks: [breakPoint]
            )
        }
    }

    private static func makeFMProposals(
        windows: [FMRefinementWindowOutput],
        atomsByOrdinal: [Int: TranscriptAtom],
        config: Config
    ) -> [SourceProposal] {
        var observations: [FMObservation] = []
        for window in windows {
            let windowCenter = center(of: window.lineRefs)
            for span in window.spans {
                guard let range = canonicalRange(
                    firstAtomOrdinal: span.firstAtomOrdinal,
                    lastAtomOrdinal: span.lastAtomOrdinal,
                    atomsByOrdinal: atomsByOrdinal
                ) else {
                    continue
                }
                observations.append(FMObservation(
                    windowIndex: window.windowIndex,
                    sourceWindowIndex: window.sourceWindowIndex,
                    windowCenter: windowCenter,
                    range: range,
                    span: span
                ))
            }
        }
        observations.sort { lhs, rhs in
            if lhs.range.firstAtomOrdinal == rhs.range.firstAtomOrdinal {
                return lhs.range.lastAtomOrdinal < rhs.range.lastAtomOrdinal
            }
            return lhs.range.firstAtomOrdinal < rhs.range.firstAtomOrdinal
        }

        var clusters: [FMCluster] = []
        for observation in observations {
            if let clusterIndex = clusters.firstIndex(where: {
                canCluster(cluster: $0, observation: observation, config: config)
            }) {
                clusters[clusterIndex].observations.append(observation)
                clusters[clusterIndex].range = union(lhs: clusters[clusterIndex].range, rhs: observation.range)
            } else {
                clusters.append(FMCluster(observations: [observation], range: observation.range))
            }
        }

        return clusters.enumerated().map { clusterIndex, cluster in
            SourceProposal(
                range: cluster.range,
                origins: .foundationModel,
                foundationModelSpans: cluster.observations.map(\.span),
                resolvedEvidenceAnchors: dedupAnchors(
                    cluster.observations.flatMap { $0.span.resolvedEvidenceAnchors }
                ),
                fmConsensusStrength: consensusStrength(for: cluster, config: config),
                fmClusterID: clusterIndex
            )
        }
    }

    private static func canCluster(
        cluster: FMCluster,
        observation: FMObservation,
        config: Config
    ) -> Bool {
        rangesOverlap(cluster.range, observation.range)
            && intersectionOverUnion(lhs: cluster.range, rhs: observation.range) >= config.fmIoUThreshold
            && anchorsAreConsistent(
                lhs: cluster.observations.flatMap { $0.span.resolvedEvidenceAnchors },
                rhs: observation.span.resolvedEvidenceAnchors
            )
    }

    private static func consensusStrength(
        for cluster: FMCluster,
        config: Config
    ) -> FMConsensusStrength {
        let uniqueWindows = Set(cluster.observations.map(\.windowIndex)).count
        guard uniqueWindows > 1 else { return .low }

        let windowCenters = cluster.observations.map(\.windowCenter)
        let centerSpan = (windowCenters.max() ?? 0) - (windowCenters.min() ?? 0)
        let hasAnchors = cluster.observations.contains { !$0.span.resolvedEvidenceAnchors.isEmpty }
        let pairwiseIoUs = pairwiseIoUs(for: cluster.observations.map(\.range))
        let minimumIoU = pairwiseIoUs.min() ?? 0

        guard hasAnchors,
              minimumIoU >= config.fmIoUThreshold,
              centerSpan >= config.minimumWindowCenterSeparation
        else {
            return .low
        }

        return uniqueWindows >= 3 ? .high : .medium
    }

    private static func canMerge(existing: SourceProposal, incoming: SourceProposal) -> Bool {
        guard rangesOverlap(existing.range, incoming.range) else {
            return false
        }

        if let existingClusterID = existing.fmClusterID,
           let incomingClusterID = incoming.fmClusterID,
           existingClusterID != incomingClusterID {
            return false
        }

        return true
    }

    private static func bestMergeIndex(
        for proposal: SourceProposal,
        in existing: [SourceProposal]
    ) -> Int? {
        existing.enumerated()
            .filter { canMerge(existing: $0.element, incoming: proposal) }
            .max { lhs, rhs in
                intersectionOverUnion(lhs: lhs.element.range, rhs: proposal.range)
                    < intersectionOverUnion(lhs: rhs.element.range, rhs: proposal.range)
            }?
            .offset
    }

    private static func merge(existing: SourceProposal, incoming: SourceProposal) -> SourceProposal {
        SourceProposal(
            range: union(lhs: existing.range, rhs: incoming.range),
            origins: existing.origins.union(incoming.origins),
            lexicalCandidates: existing.lexicalCandidates + incoming.lexicalCandidates,
            sponsorMatches: existing.sponsorMatches + incoming.sponsorMatches,
            fingerprintMatches: existing.fingerprintMatches + incoming.fingerprintMatches,
            foundationModelSpans: existing.foundationModelSpans + incoming.foundationModelSpans,
            resolvedEvidenceAnchors: dedupAnchors(
                existing.resolvedEvidenceAnchors + incoming.resolvedEvidenceAnchors
            ),
            fmConsensusStrength: max(existing.fmConsensusStrength, incoming.fmConsensusStrength),
            fmClusterID: existing.fmClusterID ?? incoming.fmClusterID,
            acousticBreaks: dedupAcousticBreaks(existing.acousticBreaks + incoming.acousticBreaks)
        )
    }

    private static func associateAcousticBreaks(
        _ breaks: [AcousticBreak],
        with range: CanonicalRange,
        tolerance: Double
    ) -> [AcousticBreak] {
        breaks.filter { candidate in
            abs(candidate.time - range.startTime) <= tolerance
                || abs(candidate.time - range.endTime) <= tolerance
        }
    }

    private static func aggregateFMEvidence(from proposal: SourceProposal) -> ProposedRegionFMEvidence? {
        guard !proposal.foundationModelSpans.isEmpty else {
            return nil
        }

        let preferredSpan = preferredFMSpan(from: proposal.foundationModelSpans)
            ?? proposal.foundationModelSpans[0]

        return ProposedRegionFMEvidence(
            commercialIntent: preferredSpan.commercialIntent,
            ownership: preferredSpan.ownership,
            certainty: preferredSpan.certainty,
            boundaryPrecision: preferredSpan.boundaryPrecision,
            memoryWriteEligible: proposal.foundationModelSpans.allSatisfy { $0.memoryWriteEligible },
            alternativeExplanation: preferredSpan.alternativeExplanation,
            reasonTags: Array(Set(proposal.foundationModelSpans.flatMap(\.reasonTags))).sorted {
                $0.rawValue < $1.rawValue
            },
            resolvedEvidenceAnchors: proposal.resolvedEvidenceAnchors
        )
    }

    private static func canonicalRange(
        startTime: Double,
        endTime: Double,
        atoms: [TranscriptAtom]
    ) -> CanonicalRange? {
        let overlapping = atoms.filter { atom in
            atom.endTime > startTime && atom.startTime < endTime
        }

        // No overlap → return nil so the caller can skip this window rather than
        // collapse to a spurious single-atom region at the edge of the transcript.
        guard let first = overlapping.first, let last = overlapping.last else {
            return nil
        }

        return CanonicalRange(
            firstAtomOrdinal: first.atomKey.atomOrdinal,
            lastAtomOrdinal: last.atomKey.atomOrdinal,
            startTime: first.startTime,
            endTime: last.endTime
        )
    }

    private static func canonicalRange(
        firstAtomOrdinal: Int,
        lastAtomOrdinal: Int,
        atomsByOrdinal: [Int: TranscriptAtom]
    ) -> CanonicalRange? {
        let lower = min(firstAtomOrdinal, lastAtomOrdinal)
        let upper = max(firstAtomOrdinal, lastAtomOrdinal)
        guard let first = atomsByOrdinal[lower], let last = atomsByOrdinal[upper] else {
            return nil
        }
        return CanonicalRange(
            firstAtomOrdinal: lower,
            lastAtomOrdinal: upper,
            startTime: first.startTime,
            endTime: last.endTime
        )
    }

    private static func rangesOverlap(_ lhs: CanonicalRange, _ rhs: CanonicalRange) -> Bool {
        lhs.firstAtomOrdinal <= rhs.lastAtomOrdinal && rhs.firstAtomOrdinal <= lhs.lastAtomOrdinal
    }

    private static func union(lhs: CanonicalRange, rhs: CanonicalRange) -> CanonicalRange {
        CanonicalRange(
            firstAtomOrdinal: min(lhs.firstAtomOrdinal, rhs.firstAtomOrdinal),
            lastAtomOrdinal: max(lhs.lastAtomOrdinal, rhs.lastAtomOrdinal),
            startTime: min(lhs.startTime, rhs.startTime),
            endTime: max(lhs.endTime, rhs.endTime)
        )
    }

    private static func intersectionOverUnion(lhs: CanonicalRange, rhs: CanonicalRange) -> Double {
        let lower = max(lhs.firstAtomOrdinal, rhs.firstAtomOrdinal)
        let upper = min(lhs.lastAtomOrdinal, rhs.lastAtomOrdinal)
        guard lower <= upper else { return 0.0 }

        let intersection = Double(upper - lower + 1)
        let union = Double(max(lhs.lastAtomOrdinal, rhs.lastAtomOrdinal) - min(lhs.firstAtomOrdinal, rhs.firstAtomOrdinal) + 1)
        guard union > 0 else { return 0.0 }
        return intersection / union
    }

    private static func pairwiseIoUs(for ranges: [CanonicalRange]) -> [Double] {
        guard ranges.count >= 2 else { return [] }

        var values: [Double] = []
        for lhsIndex in 0..<(ranges.count - 1) {
            for rhsIndex in (lhsIndex + 1)..<ranges.count {
                values.append(intersectionOverUnion(lhs: ranges[lhsIndex], rhs: ranges[rhsIndex]))
            }
        }
        return values
    }

    private static func anchorsAreConsistent(
        lhs: [ResolvedEvidenceAnchor],
        rhs: [ResolvedEvidenceAnchor]
    ) -> Bool {
        let lhsKeys = Set(lhs.map(anchorIdentityKey))
        let rhsKeys = Set(rhs.map(anchorIdentityKey))

        if lhsKeys.isEmpty || rhsKeys.isEmpty {
            return false
        }

        return !lhsKeys.isDisjoint(with: rhsKeys)
    }

    private static func anchorIdentityKey(_ anchor: ResolvedEvidenceAnchor) -> String {
        let evidenceRef = anchor.entry.map { String($0.evidenceRef) } ?? "nil"
        return "\(evidenceRef)|\(anchor.lineRef)|\(anchor.kind.rawValue)|\(anchor.resolutionSource.rawValue)"
    }

    /// Dedupe acoustic breaks by (time, signals) identity. Preserves order of
    /// first appearance. Used to merge breaks coming from both the standalone
    /// acoustic-proposal path and the edge-tolerance decoration path without
    /// double-counting when they both fire on the same final region.
    private static func dedupAcousticBreaks(_ breaks: [AcousticBreak]) -> [AcousticBreak] {
        var seen = Set<String>()
        var deduped: [AcousticBreak] = []
        deduped.reserveCapacity(breaks.count)
        for breakPoint in breaks {
            let signalKey = breakPoint.signals
                .map(\.rawValue)
                .sorted()
                .joined(separator: ",")
            let key = "\(breakPoint.time)|\(signalKey)"
            if seen.insert(key).inserted {
                deduped.append(breakPoint)
            }
        }
        return deduped
    }

    private static func dedupAnchors(_ anchors: [ResolvedEvidenceAnchor]) -> [ResolvedEvidenceAnchor] {
        var seen = Set<String>()
        var deduped: [ResolvedEvidenceAnchor] = []
        deduped.reserveCapacity(anchors.count)

        for anchor in anchors {
            if seen.insert(anchorIdentityKey(anchor)).inserted {
                deduped.append(anchor)
            }
        }
        return deduped
    }

    private static func center(of lineRefs: [Int]) -> Double {
        guard let first = lineRefs.min(), let last = lineRefs.max() else { return 0 }
        return Double(first + last) / 2.0
    }

    private static func preferredFMSpan(from spans: [RefinedAdSpan]) -> RefinedAdSpan? {
        spans.max { lhs, rhs in
            let lhsScore = certaintyScore(lhs.certainty) + boundaryPrecisionScore(lhs.boundaryPrecision)
            let rhsScore = certaintyScore(rhs.certainty) + boundaryPrecisionScore(rhs.boundaryPrecision)
            if lhsScore == rhsScore {
                let lhsWidth = lhs.lastAtomOrdinal - lhs.firstAtomOrdinal
                let rhsWidth = rhs.lastAtomOrdinal - rhs.firstAtomOrdinal
                return lhsWidth < rhsWidth
            }
            return lhsScore < rhsScore
        }
    }

    private static func certaintyScore(_ certainty: CertaintyBand) -> Int {
        switch certainty {
        case .weak:
            return 0
        case .moderate:
            return 1
        case .strong:
            return 2
        }
    }

    private static func boundaryPrecisionScore(_ precision: BoundaryPrecision) -> Int {
        switch precision {
        case .usable:
            return 0
        case .precise:
            return 1
        }
    }
}

private struct CanonicalRange: Sendable {
    let firstAtomOrdinal: Int
    let lastAtomOrdinal: Int
    let startTime: Double
    let endTime: Double
}

private struct SourceProposal: Sendable {
    let range: CanonicalRange
    let origins: ProposedRegionOrigins
    var lexicalCandidates: [LexicalCandidate] = []
    var sponsorMatches: [SponsorMatch] = []
    var fingerprintMatches: [FingerprintMatch] = []
    var foundationModelSpans: [RefinedAdSpan] = []
    var resolvedEvidenceAnchors: [ResolvedEvidenceAnchor] = []
    var fmConsensusStrength: FMConsensusStrength = .none
    var fmClusterID: Int? = nil
    /// Acoustic breaks carried along for provenance — set when a break spawns
    /// a standalone acoustic proposal via `makeAcousticProposals`. Concatenated
    /// across merges so the final `ProposedRegion` can surface them downstream.
    var acousticBreaks: [AcousticBreak] = []
}

private struct FMObservation: Sendable {
    let windowIndex: Int
    let sourceWindowIndex: Int
    let windowCenter: Double
    let range: CanonicalRange
    let span: RefinedAdSpan
}

private struct FMCluster: Sendable {
    var observations: [FMObservation]
    var range: CanonicalRange
}
