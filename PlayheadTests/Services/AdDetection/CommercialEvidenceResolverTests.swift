import Foundation
import Testing

@testable import Playhead

@Suite("CommercialEvidenceResolver")
struct CommercialEvidenceResolverTests {

    @Test("prefers prompt evidence refs over noisy FM line refs and kinds")
    func prefersPromptEvidenceRef() {
        let segments = [
            makeResolverSegment(index: 1, text: "Visit example.com for the offer.")
        ]
        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1",
            entries: [
                EvidenceEntry(
                    evidenceRef: 11,
                    category: .url,
                    matchedText: "example.com",
                    normalizedText: "example.com",
                    atomOrdinal: 1,
                    startTime: 5,
                    endTime: 10
                )
            ]
        )
        let plan = makeResolverPlan(
            lineRefs: [1],
            promptEvidence: [PromptEvidenceEntry(entry: evidenceCatalog.entries[0], lineRef: 1)]
        )

        let resolved = CommercialEvidenceResolver.resolve(
            anchors: [
                EvidenceAnchorSchema(
                    evidenceRef: 11,
                    lineRef: 999,
                    kind: .promoCode,
                    certainty: .strong
                )
            ],
            plan: plan,
            lineRefLookup: Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) }),
            evidenceCatalog: evidenceCatalog
        )

        #expect(resolved.count == 1)
        #expect(resolved[0].entry?.evidenceRef == 11)
        #expect(resolved[0].lineRef == 1)
        #expect(resolved[0].kind == .url)
        #expect(resolved[0].resolutionSource == .evidenceRef)
        #expect(resolved[0].memoryWriteEligible)
    }

    @Test("line-ref fallback resolves uniquely extracted deterministic evidence")
    func fallbackResolvesUniqueDeterministicEvidence() {
        let segments = [
            makeResolverSegment(index: 1, text: "Visit example.com for the offer.")
        ]
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments[0].atoms,
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1"
        )
        let plan = makeResolverPlan(lineRefs: [1], promptEvidence: [])

        let resolved = CommercialEvidenceResolver.resolve(
            anchors: [
                EvidenceAnchorSchema(
                    evidenceRef: nil,
                    lineRef: 1,
                    kind: .url,
                    certainty: .moderate
                )
            ],
            plan: plan,
            lineRefLookup: Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) }),
            evidenceCatalog: evidenceCatalog
        )

        #expect(resolved.count == 1)
        #expect(resolved[0].entry?.matchedText == "example.com")
        #expect(resolved[0].resolutionSource == .lineRefFallback)
        #expect(resolved[0].memoryWriteEligible)
    }

    @Test("line-ref fallback uses refinement-window context for brand extraction")
    func fallbackUsesWindowContextForBrandExtraction() {
        let segments = [
            makeResolverSegment(index: 0, text: "BetterHelp has supported our show for years."),
            makeResolverSegment(index: 1, text: "Visit betterhelp.com for the offer.")
        ]
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments.flatMap(\.atoms),
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1"
        )
        let plan = makeResolverPlan(lineRefs: [0, 1], promptEvidence: [])

        let resolved = CommercialEvidenceResolver.resolve(
            anchors: [
                EvidenceAnchorSchema(
                    evidenceRef: nil,
                    lineRef: 0,
                    kind: .brandSpan,
                    certainty: .moderate
                )
            ],
            plan: plan,
            lineRefLookup: Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) }),
            evidenceCatalog: evidenceCatalog
        )

        #expect(resolved.count == 1)
        #expect(resolved[0].entry?.matchedText == "BetterHelp")
        #expect(resolved[0].kind == .brandSpan)
        #expect(resolved[0].resolutionSource == .lineRefFallback)
        #expect(resolved[0].memoryWriteEligible)
    }

    @Test("line-ref fallback maps repeated evidence to the canonical catalog entry")
    func fallbackMapsRepeatedEvidenceToCanonicalCatalogEntry() {
        let segments = [
            makeResolverSegment(index: 5, text: "Visit example.com for the offer.")
        ]
        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1",
            entries: [
                EvidenceEntry(
                    evidenceRef: 11,
                    category: .url,
                    matchedText: "example.com",
                    normalizedText: "example.com",
                    atomOrdinal: 1,
                    startTime: 1,
                    endTime: 2
                )
            ]
        )
        let plan = makeResolverPlan(lineRefs: [5], promptEvidence: [])

        let resolved = CommercialEvidenceResolver.resolve(
            anchors: [
                EvidenceAnchorSchema(
                    evidenceRef: nil,
                    lineRef: 5,
                    kind: .url,
                    certainty: .moderate
                )
            ],
            plan: plan,
            lineRefLookup: Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) }),
            evidenceCatalog: evidenceCatalog
        )

        #expect(resolved.count == 1)
        #expect(resolved[0].entry?.evidenceRef == 11)
        #expect(resolved[0].resolutionSource == .lineRefFallback)
        #expect(resolved[0].memoryWriteEligible)
    }

    @Test("unresolved deterministic fallback stays classification-valid but blocks memory writes")
    func unresolvedFallbackBlocksMemoryWrites() {
        let segments = [
            makeResolverSegment(index: 1, text: "Our sponsor is terrific today.")
        ]
        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1",
            entries: []
        )
        let plan = makeResolverPlan(lineRefs: [1], promptEvidence: [])

        let resolved = CommercialEvidenceResolver.resolve(
            anchors: [
                EvidenceAnchorSchema(
                    evidenceRef: nil,
                    lineRef: 1,
                    kind: .brandSpan,
                    certainty: .weak
                )
            ],
            plan: plan,
            lineRefLookup: Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) }),
            evidenceCatalog: evidenceCatalog
        )

        #expect(resolved.count == 1)
        #expect(resolved[0].entry == nil)
        #expect(resolved[0].kind == .brandSpan)
        #expect(resolved[0].resolutionSource == .unresolved)
        #expect(!resolved[0].memoryWriteEligible)
    }
}

private func makeResolverSegment(
    index: Int,
    startTime: Double = 5,
    endTime: Double = 10,
    text: String
) -> AdTranscriptSegment {
    AdTranscriptSegment(
        atoms: [
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: "asset-1",
                    transcriptVersion: "transcript-v1",
                    atomOrdinal: index
                ),
                contentHash: "hash-\(index)",
                startTime: startTime,
                endTime: endTime,
                text: text,
                chunkIndex: index
            )
        ],
        segmentIndex: index
    )
}

private func makeResolverPlan(
    lineRefs: [Int],
    promptEvidence: [PromptEvidenceEntry]
) -> RefinementWindowPlan {
    RefinementWindowPlan(
        windowIndex: 0,
        sourceWindowIndex: 0,
        lineRefs: lineRefs,
        focusLineRefs: lineRefs,
        focusClusters: [lineRefs],
        prompt: "Refine ad spans.",
        promptTokenCount: 8,
        startTime: 0,
        endTime: 10,
        stopReason: .minimumSpan,
        promptEvidence: promptEvidence
    )
}
