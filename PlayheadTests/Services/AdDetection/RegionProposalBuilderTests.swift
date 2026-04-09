import Foundation
import Testing

@testable import Playhead

@Suite("RegionProposalBuilder")
struct RegionProposalBuilderTests {

    @Test("merges overlapping proposals from every source into one canonical region")
    func mergesCrossSourceProposals() {
        let atoms = makeAtoms(count: 6)
        let input = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: [
                makeLexicalCandidate(startTime: 1.1, endTime: 3.9)
            ],
            acousticBreaks: [
                AcousticBreak(time: 1.0, breakStrength: 0.8, signals: [.energyDrop]),
                AcousticBreak(time: 4.0, breakStrength: 0.7, signals: [.energyRise])
            ],
            sponsorMatches: [
                makeSponsorMatch(firstAtomOrdinal: 1, lastAtomOrdinal: 3)
            ],
            fingerprintMatches: [
                makeFingerprintMatch(firstAtomOrdinal: 1, lastAtomOrdinal: 3)
            ],
            fmWindows: [
                makeFMWindow(
                    windowIndex: 0,
                    sourceWindowIndex: 0,
                    lineRefs: [1, 2, 3, 4],
                    spans: [
                        makeRefinedSpan(
                            firstAtomOrdinal: 1,
                            lastAtomOrdinal: 3,
                            anchors: [makeResolvedAnchor(lineRef: 2, evidenceRef: 101)]
                        )
                    ]
                )
            ]
        )

        let regions = RegionProposalBuilder.build(input)

        #expect(regions.count == 1)
        let region = regions[0]
        #expect(region.firstAtomOrdinal == 1)
        #expect(region.lastAtomOrdinal == 3)
        #expect(region.startTime == 1.0)
        #expect(region.endTime == 4.0)
        #expect(region.origins.contains(.lexical))
        #expect(region.origins.contains(.acoustic))
        #expect(region.origins.contains(.sponsor))
        #expect(region.origins.contains(.fingerprint))
        #expect(region.origins.contains(.foundationModel))
        #expect(region.lexicalCandidates.count == 1)
        #expect(region.sponsorMatches.count == 1)
        #expect(region.fingerprintMatches.count == 1)
        #expect(region.acousticBreaks.count == 2)
        #expect(region.resolvedEvidenceAnchors.map { $0.entry?.evidenceRef } == [101])
        #expect(region.fmEvidence?.commercialIntent == .paid)
        #expect(region.fmEvidence?.resolvedEvidenceAnchors.count == 1)
    }

    @Test("clusters overlapping anchor-consistent FM windows into one stronger-consensus region")
    func clustersAnchorConsistentFMWindows() {
        let atoms = makeAtoms(count: 8)
        let singleWindowInput = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: [],
            acousticBreaks: [],
            sponsorMatches: [],
            fingerprintMatches: [],
            fmWindows: [
                makeFMWindow(
                    windowIndex: 0,
                    sourceWindowIndex: 0,
                    lineRefs: [1, 2, 3, 4, 5],
                    spans: [
                        makeRefinedSpan(
                            firstAtomOrdinal: 2,
                            lastAtomOrdinal: 4,
                            anchors: [makeResolvedAnchor(lineRef: 3, evidenceRef: 201)]
                        )
                    ]
                )
            ]
        )
        let clusteredInput = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: [],
            acousticBreaks: [],
            sponsorMatches: [],
            fingerprintMatches: [],
            fmWindows: [
                makeFMWindow(
                    windowIndex: 0,
                    sourceWindowIndex: 0,
                    lineRefs: [1, 2, 3, 4, 5],
                    spans: [
                        makeRefinedSpan(
                            firstAtomOrdinal: 2,
                            lastAtomOrdinal: 4,
                            anchors: [
                                makeResolvedAnchor(lineRef: 3, evidenceRef: 201)
                            ]
                        )
                    ]
                ),
                makeFMWindow(
                    windowIndex: 1,
                    sourceWindowIndex: 1,
                    lineRefs: [2, 3, 4, 5, 6],
                    spans: [
                        makeRefinedSpan(
                            firstAtomOrdinal: 2,
                            lastAtomOrdinal: 5,
                            certainty: .strong,
                            anchors: [
                                makeResolvedAnchor(lineRef: 3, evidenceRef: 201),
                                makeResolvedAnchor(lineRef: 4, evidenceRef: 202)
                            ]
                        )
                    ]
                )
            ]
        )

        let single = RegionProposalBuilder.build(singleWindowInput)
        let clustered = RegionProposalBuilder.build(clusteredInput)

        #expect(single.count == 1)
        #expect(clustered.count == 1)
        #expect(clustered[0].fmConsensusStrength.value > single[0].fmConsensusStrength.value)
        #expect(clustered[0].origins == [.foundationModel])
        #expect(clustered[0].firstAtomOrdinal == 2)
        #expect(clustered[0].lastAtomOrdinal == 5)
        #expect(clustered[0].resolvedEvidenceAnchors.count == 2)
        #expect(Set(clustered[0].resolvedEvidenceAnchors.compactMap { $0.entry?.evidenceRef }) == Set([201, 202]))
        #expect(clustered[0].fmEvidence?.certainty == .strong)
    }

    @Test("single FM span still emits a low-consensus proposal")
    func singleFMSpanProducesLowConsensusProposal() {
        let atoms = makeAtoms(count: 6)
        let input = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: [],
            acousticBreaks: [],
            sponsorMatches: [],
            fingerprintMatches: [],
            fmWindows: [
                makeFMWindow(
                    windowIndex: 0,
                    sourceWindowIndex: 0,
                    lineRefs: [0, 1, 2, 3],
                    spans: [
                        makeRefinedSpan(
                            firstAtomOrdinal: 1,
                            lastAtomOrdinal: 2,
                            anchors: [makeResolvedAnchor(lineRef: 1, evidenceRef: 301)]
                        )
                    ]
                )
            ]
        )

        let regions = RegionProposalBuilder.build(input)

        #expect(regions.count == 1)
        #expect(regions[0].firstAtomOrdinal == 1)
        #expect(regions[0].lastAtomOrdinal == 2)
        #expect(regions[0].startTime == 1.0)
        #expect(regions[0].endTime == 3.0)
        #expect(regions[0].fmConsensusStrength == .low)
        #expect(regions[0].resolvedEvidenceAnchors.count == 1)
    }

    @Test("overlapping FM spans with different anchors stay in separate regions")
    func anchorInconsistentFMSpansDoNotCluster() {
        let atoms = makeAtoms(count: 8)
        let input = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: [],
            acousticBreaks: [],
            sponsorMatches: [],
            fingerprintMatches: [],
            fmWindows: [
                makeFMWindow(
                    windowIndex: 0,
                    sourceWindowIndex: 0,
                    lineRefs: [1, 2, 3, 4, 5],
                    spans: [
                        makeRefinedSpan(
                            firstAtomOrdinal: 2,
                            lastAtomOrdinal: 4,
                            anchors: [makeResolvedAnchor(lineRef: 3, evidenceRef: 401)]
                        )
                    ]
                ),
                makeFMWindow(
                    windowIndex: 1,
                    sourceWindowIndex: 1,
                    lineRefs: [2, 3, 4, 5, 6],
                    spans: [
                        makeRefinedSpan(
                            firstAtomOrdinal: 3,
                            lastAtomOrdinal: 5,
                            anchors: [makeResolvedAnchor(lineRef: 4, evidenceRef: 999)]
                        )
                    ]
                )
            ]
        )

        let regions = RegionProposalBuilder.build(input)

        #expect(regions.count == 2)
        #expect(regions[0].resolvedEvidenceAnchors.compactMap { $0.entry?.evidenceRef } == [401])
        #expect(regions[1].resolvedEvidenceAnchors.compactMap { $0.entry?.evidenceRef } == [999])
        #expect(regions.allSatisfy { $0.fmConsensusStrength == .low })
    }

    @Test("distinct refinement windows sharing a source window still earn FM consensus")
    func refinementWindowsWithSharedSourceWindowStillConsensus() {
        let atoms = makeAtoms(count: 8)
        let input = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: [],
            acousticBreaks: [],
            sponsorMatches: [],
            fingerprintMatches: [],
            fmWindows: [
                makeFMWindow(
                    windowIndex: 10,
                    sourceWindowIndex: 0,
                    lineRefs: [1, 2, 3, 4, 5],
                    spans: [
                        makeRefinedSpan(
                            firstAtomOrdinal: 2,
                            lastAtomOrdinal: 4,
                            anchors: [makeResolvedAnchor(lineRef: 3, evidenceRef: 501)]
                        )
                    ]
                ),
                makeFMWindow(
                    windowIndex: 11,
                    sourceWindowIndex: 0,
                    lineRefs: [2, 3, 4, 5, 6],
                    spans: [
                        makeRefinedSpan(
                            firstAtomOrdinal: 2,
                            lastAtomOrdinal: 5,
                            anchors: [makeResolvedAnchor(lineRef: 3, evidenceRef: 501)]
                        )
                    ]
                )
            ]
        )

        let regions = RegionProposalBuilder.build(input)

        #expect(regions.count == 1)
        #expect(regions[0].fmConsensusStrength == .medium)
    }

    @Test("interior acoustic breaks do not claim boundary provenance")
    func interiorAcousticBreaksDoNotMarkAcousticOrigin() {
        let atoms = makeAtoms(count: 6)
        let input = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: [
                makeLexicalCandidate(startTime: 1.0, endTime: 4.0)
            ],
            acousticBreaks: [
                AcousticBreak(time: 2.5, breakStrength: 0.9, signals: [.spectralSpike])
            ],
            sponsorMatches: [],
            fingerprintMatches: [],
            fmWindows: []
        )

        let regions = RegionProposalBuilder.build(input)

        #expect(regions.count == 1)
        #expect(!regions[0].origins.contains(.acoustic))
        #expect(regions[0].acousticBreaks.isEmpty)
    }
}

private func makeAtoms(count: Int) -> [TranscriptAtom] {
    (0..<count).map { ordinal in
        TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "asset-1",
                transcriptVersion: "transcript-v1",
                atomOrdinal: ordinal
            ),
            contentHash: "hash-\(ordinal)",
            startTime: Double(ordinal),
            endTime: Double(ordinal + 1),
            text: "token \(ordinal)",
            chunkIndex: ordinal
        )
    }
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

private func makeSponsorMatch(
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

private func makeFingerprintMatch(
    firstAtomOrdinal: Int,
    lastAtomOrdinal: Int
) -> FingerprintMatch {
    FingerprintMatch(
        firstAtomOrdinal: firstAtomOrdinal,
        lastAtomOrdinal: lastAtomOrdinal,
        fingerprintId: "fp-\(firstAtomOrdinal)-\(lastAtomOrdinal)",
        similarity: 0.9,
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
