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
            // Both breaks land inside atoms 1 and 3 (within the lex atom span 1..3),
            // so each spawns a standalone 1-atom acoustic proposal that merges with
            // the existing cross-source cluster at atoms 1..3. The edge-tolerance
            // decoration path in associateAcousticBreaks also picks them up because
            // each break is within 1.0s of a region edge; dedupe keeps the output
            // at two breaks, not four.
            acousticBreaks: [
                AcousticBreak(time: 1.5, breakStrength: 0.8, signals: [.energyDrop]),
                AcousticBreak(time: 3.5, breakStrength: 0.7, signals: [.energyRise])
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

    @Test("overlapping FM spans below the IoU threshold do not cluster")
    func lowIoUFMSpansDoNotCluster() {
        let atoms = makeAtoms(count: 10)
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
                            firstAtomOrdinal: 1,
                            lastAtomOrdinal: 4,
                            anchors: [makeResolvedAnchor(lineRef: 2, evidenceRef: 210)]
                        )
                    ]
                ),
                makeFMWindow(
                    windowIndex: 1,
                    sourceWindowIndex: 1,
                    lineRefs: [4, 5, 6, 7, 8],
                    spans: [
                        makeRefinedSpan(
                            firstAtomOrdinal: 4,
                            lastAtomOrdinal: 7,
                            anchors: [makeResolvedAnchor(lineRef: 2, evidenceRef: 210)]
                        )
                    ]
                )
            ]
        )

        let regions = RegionProposalBuilder.build(input)

        #expect(regions.count == 2)
        #expect(regions.allSatisfy { $0.fmConsensusStrength == .low })
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

    @Test("FM windows with insufficient center separation stay low-consensus")
    func insufficientCenterSeparationKeepsLowConsensus() {
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
                            anchors: [makeResolvedAnchor(lineRef: 3, evidenceRef: 601)]
                        )
                    ]
                ),
                makeFMWindow(
                    windowIndex: 1,
                    sourceWindowIndex: 1,
                    lineRefs: [1, 2, 3, 4, 5],
                    spans: [
                        makeRefinedSpan(
                            firstAtomOrdinal: 2,
                            lastAtomOrdinal: 4,
                            anchors: [makeResolvedAnchor(lineRef: 3, evidenceRef: 601)]
                        )
                    ]
                )
            ]
        )

        let regions = RegionProposalBuilder.build(input)

        #expect(regions.count == 1)
        #expect(regions[0].fmConsensusStrength == .low)
    }

    @Test("duplicate atom ordinals do not trap and still produce a deterministic region")
    func duplicateAtomOrdinalsAreTolerated() {
        // Two atoms share ordinal 2. Dictionary(uniqueKeysWithValues:) would trap;
        // the defensive last-write-wins path must keep the builder running.
        var atoms = makeAtoms(count: 5)
        let duplicate = TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "asset-1",
                transcriptVersion: "transcript-v1",
                atomOrdinal: 2
            ),
            contentHash: "hash-2-dup",
            startTime: 2.0,
            endTime: 3.0,
            text: "duplicate",
            chunkIndex: 2
        )
        atoms.append(duplicate)

        let input = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: [],
            acousticBreaks: [],
            sponsorMatches: [
                makeSponsorMatch(firstAtomOrdinal: 1, lastAtomOrdinal: 3)
            ],
            fingerprintMatches: [],
            fmWindows: []
        )

        let regions = RegionProposalBuilder.build(input)

        #expect(regions.count == 1)
        #expect(regions[0].firstAtomOrdinal == 1)
        #expect(regions[0].lastAtomOrdinal == 3)
    }

    @Test("lexical candidates that do not overlap any atom are skipped rather than collapsing to an edge region")
    func lexicalCandidateWithoutAtomOverlapIsSkipped() {
        let atoms = makeAtoms(count: 4) // atoms span times 0..4
        let input = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: [
                // Entirely outside the atom time range.
                makeLexicalCandidate(startTime: 50.0, endTime: 55.0)
            ],
            acousticBreaks: [],
            sponsorMatches: [],
            fingerprintMatches: [],
            fmWindows: []
        )

        let regions = RegionProposalBuilder.build(input)

        #expect(regions.isEmpty)
    }

    @Test("interior acoustic breaks merge into overlapping lexical region and contribute provenance")
    func interiorAcousticBreaksMergeIntoOverlappingRegion() {
        // Post playhead-8jd: interior acoustic breaks that land inside an atom
        // overlapping an existing proposal now spawn a standalone 1-atom
        // acoustic proposal that merges with the overlapping region, lifting
        // `.acoustic` into its origins and flowing the break through as
        // provenance. Previously this signal was silently dropped.
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
        #expect(regions[0].origins.contains(.lexical))
        #expect(regions[0].origins.contains(.acoustic))
        #expect(regions[0].acousticBreaks.count == 1)
        #expect(regions[0].acousticBreaks[0].time == 2.5)
    }

    @Test("standalone acoustic break with no overlapping lexical hit creates its own 1-atom region")
    func standaloneAcousticBreakCreatesOwnRegion() {
        // Lexical hit at atoms 0..1, acoustic break at atom 4 — disjoint.
        // The break should spawn a standalone 1-atom-wide .acoustic region.
        let atoms = makeAtoms(count: 6)
        let input = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: [
                makeLexicalCandidate(startTime: 0.0, endTime: 2.0)
            ],
            acousticBreaks: [
                AcousticBreak(time: 4.5, breakStrength: 0.8, signals: [.spectralSpike])
            ],
            sponsorMatches: [],
            fingerprintMatches: [],
            fmWindows: []
        )

        let regions = RegionProposalBuilder.build(input)

        #expect(regions.count == 2)
        let acoustic = regions.first { $0.origins.contains(.acoustic) }
        #expect(acoustic != nil)
        #expect(acoustic?.origins.contains(.lexical) == false)
        // 1-atom-wide anchor.
        #expect(acoustic?.firstAtomOrdinal == acoustic?.lastAtomOrdinal)
        #expect(acoustic?.firstAtomOrdinal == 4)
        #expect(acoustic?.acousticBreaks.count == 1)
        #expect(acoustic?.acousticBreaks.first?.time == 4.5)
    }

    @Test("acoustic break colocated with lexical hit merges into single region")
    func acousticBreakMergesWithOverlappingLexicalHit() {
        let atoms = makeAtoms(count: 15)
        let input = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: [
                // Spans atoms 10..11 (startTime 10 to endTime 12).
                makeLexicalCandidate(startTime: 10.0, endTime: 12.0)
            ],
            acousticBreaks: [
                // Lands inside atom 10.
                AcousticBreak(time: 10.5, breakStrength: 0.8, signals: [.energyDrop])
            ],
            sponsorMatches: [],
            fingerprintMatches: [],
            fmWindows: []
        )

        let regions = RegionProposalBuilder.build(input)

        #expect(regions.count == 1)
        #expect(regions[0].origins.contains(.lexical))
        #expect(regions[0].origins.contains(.acoustic))
    }

    @Test("acoustic breaks outside the atom time range are dropped silently")
    func outOfRangeAcousticBreaksAreDropped() {
        let atoms = makeAtoms(count: 5)
        let input = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: [],
            acousticBreaks: [
                AcousticBreak(time: -1.0, breakStrength: 0.9, signals: [.energyDrop]),
                AcousticBreak(time: 999_999.0, breakStrength: 0.9, signals: [.spectralSpike])
            ],
            sponsorMatches: [],
            fingerprintMatches: [],
            fmWindows: []
        )

        let regions = RegionProposalBuilder.build(input)

        #expect(regions.isEmpty)
    }

    @Test("acoustic proposal pipeline is deterministic across repeated runs")
    func acousticProposalPipelineIsDeterministic() {
        let atoms = makeAtoms(count: 10)
        let input = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: [
                makeLexicalCandidate(startTime: 1.0, endTime: 3.0)
            ],
            acousticBreaks: [
                AcousticBreak(time: 1.5, breakStrength: 0.7, signals: [.energyDrop]),
                AcousticBreak(time: 5.5, breakStrength: 0.6, signals: [.spectralSpike]),
                AcousticBreak(time: 8.5, breakStrength: 0.8, signals: [.pauseCluster])
            ],
            sponsorMatches: [],
            fingerprintMatches: [],
            fmWindows: []
        )

        let first = RegionProposalBuilder.build(input)
        let second = RegionProposalBuilder.build(input)

        #expect(first.count == second.count)
        for (lhs, rhs) in zip(first, second) {
            #expect(lhs.firstAtomOrdinal == rhs.firstAtomOrdinal)
            #expect(lhs.lastAtomOrdinal == rhs.lastAtomOrdinal)
            #expect(lhs.startTime == rhs.startTime)
            #expect(lhs.endTime == rhs.endTime)
            #expect(lhs.origins.rawValue == rhs.origins.rawValue)
            #expect(lhs.acousticBreaks.count == rhs.acousticBreaks.count)
        }
    }

    @Test("acoustic break near a lexical region edge still fires the decoration path")
    func acousticBreakNearLexicalEdgeFiresDecorationPath() {
        // Lex region spans atoms 11..15 (startTime 11, endTime 16).
        // Break at time 10.5 lands in atom 10 — outside the lex region but
        // within the 1.0s edge tolerance of the region's startTime (11.0).
        // The standalone acoustic proposal does NOT overlap the lex atoms
        // (10 vs 11..15), so the existing `associateAcousticBreaks` decoration
        // path is what lifts `.acoustic` into the lex region's origins.
        let atoms = makeAtoms(count: 20)
        let input = RegionProposalInput(
            atoms: atoms,
            lexicalCandidates: [
                makeLexicalCandidate(startTime: 11.0, endTime: 16.0)
            ],
            acousticBreaks: [
                AcousticBreak(time: 10.5, breakStrength: 0.8, signals: [.energyRise])
            ],
            sponsorMatches: [],
            fingerprintMatches: [],
            fmWindows: []
        )

        let regions = RegionProposalBuilder.build(input)

        // Expect the lex region decorated with .acoustic via the tolerance
        // path, plus a standalone acoustic-only region for the 1-atom anchor.
        let lexRegion = regions.first {
            $0.origins.contains(.lexical) && $0.firstAtomOrdinal == 11
        }
        #expect(lexRegion != nil)
        #expect(lexRegion?.origins.contains(.acoustic) == true)
        #expect(lexRegion?.acousticBreaks.isEmpty == false)
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
