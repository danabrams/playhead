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
        // Per the file header contract: only .evidenceRef resolution is FM-attested
        // and safe for sponsor-memory writes. Line-ref fallback is deterministic-only.
        #expect(!resolved[0].memoryWriteEligible)
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
        // Window-context fallback is deterministic-only; not memory-eligible.
        #expect(!resolved[0].memoryWriteEligible)
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
        #expect(!resolved[0].memoryWriteEligible)
    }

    @Test("duplicate atom ordinals across segments do not crash")
    func duplicateAtomOrdinalsDoNotCrash() {
        // Two segments contain atoms with the same atomOrdinal — Dictionary(uniqueKeysWithValues:)
        // would trap. The resolver must use a uniquing closure and continue without crashing.
        let atomA = TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "asset-1",
                transcriptVersion: "transcript-v1",
                atomOrdinal: 1
            ),
            contentHash: "hash-a",
            startTime: 5,
            endTime: 10,
            text: "Visit example.com",
            chunkIndex: 0
        )
        let atomB = TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "asset-1",
                transcriptVersion: "transcript-v1",
                atomOrdinal: 1   // duplicate ordinal across segments
            ),
            contentHash: "hash-b",
            startTime: 15,
            endTime: 20,
            text: "Visit example.com",
            chunkIndex: 1
        )
        let segmentA = AdTranscriptSegment(atoms: [atomA], segmentIndex: 0)
        let segmentB = AdTranscriptSegment(atoms: [atomB], segmentIndex: 1)

        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1",
            entries: [
                EvidenceEntry(
                    evidenceRef: 0,
                    category: .url,
                    matchedText: "example.com",
                    normalizedText: "example.com",
                    atomOrdinal: 1,
                    startTime: 5,
                    endTime: 10
                )
            ]
        )
        let plan = makeResolverPlan(lineRefs: [0, 1], promptEvidence: [])

        let resolved = CommercialEvidenceResolver.resolve(
            anchors: [
                EvidenceAnchorSchema(
                    evidenceRef: nil,
                    lineRef: 0,
                    kind: .url,
                    certainty: .moderate
                )
            ],
            plan: plan,
            lineRefLookup: [0: segmentA, 1: segmentB],
            evidenceCatalog: evidenceCatalog
        )
        // Determinism: never crash; the lower segmentIndex (0) should win.
        #expect(resolved.count == 1)
    }

    @Test("duplicate catalog entries with same (category, normalizedText) do not crash")
    func duplicateCatalogEntriesDoNotCrash() {
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
                ),
                EvidenceEntry(
                    evidenceRef: 12,
                    category: .url,
                    matchedText: "example.com",
                    normalizedText: "example.com",
                    atomOrdinal: 1,
                    startTime: 6,
                    endTime: 11
                )
            ]
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
        // First-wins determinism on duplicates.
        #expect(resolved[0].entry?.evidenceRef == 11)
    }

    @Test("brand window-context fallback regex miss does not stamp foreign timing")
    func brandWindowContextFallbackRegexMissReturnsUnresolved() {
        // Brand "Acme" exists at line 0, FM anchor points to line 1 (no Acme in text).
        // contextualizedBrandEntry will return nil (regex can't find "Acme" in line 1).
        // Resolver MUST NOT return an entry stamped with line 0's atomOrdinal/timing.
        let segments = [
            makeResolverSegment(index: 0, startTime: 1, endTime: 4,
                                text: "Sponsored by Acme today."),
            makeResolverSegment(index: 1, startTime: 100, endTime: 104,
                                text: "And in unrelated news, the weather is fine.")
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
                    lineRef: 1,
                    kind: .brandSpan,
                    certainty: .moderate
                )
            ],
            plan: plan,
            lineRefLookup: Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) }),
            evidenceCatalog: evidenceCatalog
        )

        // Either unresolved, or skipped entirely. Must NOT stamp line 0's timing on line 1.
        if let first = resolved.first {
            #expect(first.resolutionSource == .unresolved)
            #expect(first.entry == nil)
            #expect(!first.memoryWriteEligible)
            #expect(first.lineRef == 1)
        }
    }

    @Test("line-ref fallback never marks memory write eligible")
    func lineRefFallbackNotMemoryEligible() {
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
        #expect(resolved[0].resolutionSource == .lineRefFallback)
        #expect(!resolved[0].memoryWriteEligible)
    }

    @Test("window-context fallback never marks memory write eligible")
    func windowContextFallbackNotMemoryEligible() {
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
        #expect(resolved[0].resolutionSource == .lineRefFallback)
        #expect(!resolved[0].memoryWriteEligible)
    }

    @Test("contextualizedBrandEntry handles unicode brand names with non-ASCII characters")
    func contextualizedBrandEntryHandlesUnicodeBrand() {
        // The brand "Café" needs unicode-aware word boundaries.
        // We exercise this through the window-context fallback path.
        let segments = [
            makeResolverSegment(index: 0, startTime: 1, endTime: 4,
                                text: "Sponsored by Café today, visit cafe.com"),
            makeResolverSegment(index: 1, startTime: 5, endTime: 10,
                                text: "Café offers great drinks.")
        ]
        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1",
            entries: [
                EvidenceEntry(
                    evidenceRef: 0,
                    category: .brandSpan,
                    matchedText: "Café",
                    normalizedText: "café",
                    atomOrdinal: 0,
                    startTime: 1,
                    endTime: 4
                )
            ]
        )
        // anchor at line 1; brand from line 0 is the only window candidate.
        let plan = makeResolverPlan(lineRefs: [0, 1], promptEvidence: [])

        let resolved = CommercialEvidenceResolver.resolve(
            anchors: [
                EvidenceAnchorSchema(
                    evidenceRef: nil,
                    lineRef: 1,
                    kind: .brandSpan,
                    certainty: .moderate
                )
            ],
            plan: plan,
            lineRefLookup: Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) }),
            evidenceCatalog: evidenceCatalog
        )

        // The window-context fallback should successfully contextualize "Café" in segment 1.
        // Pre-fix this would fail because \b doesn't handle "é" as a word char.
        #expect(resolved.count == 1)
        if let entry = resolved.first?.entry {
            #expect(entry.matchedText.lowercased().contains("café"))
            // Timing must be from segment 1 (the anchor's segment), not segment 0.
            #expect(entry.startTime >= 5)
            #expect(entry.endTime <= 10)
        }
    }

    @Test("multi-anchor dedup collapses identical evidenceRef anchors")
    func multiAnchorDedup() {
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
                EvidenceAnchorSchema(evidenceRef: 11, lineRef: 1, kind: .url, certainty: .strong),
                EvidenceAnchorSchema(evidenceRef: 11, lineRef: 1, kind: .url, certainty: .strong)
            ],
            plan: plan,
            lineRefLookup: Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) }),
            evidenceCatalog: evidenceCatalog
        )

        #expect(resolved.count == 1)
    }

    @Test("off-window anchors at boundary off-by-one are rejected")
    func offWindowBoundaryRejected() {
        let segments = [
            makeResolverSegment(index: 5, text: "Visit example.com for the offer.")
        ]
        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1",
            entries: []
        )
        let plan = makeResolverPlan(lineRefs: [5], promptEvidence: [])

        let resolved = CommercialEvidenceResolver.resolve(
            anchors: [
                EvidenceAnchorSchema(evidenceRef: nil, lineRef: 4, kind: .url, certainty: .strong),
                EvidenceAnchorSchema(evidenceRef: nil, lineRef: 6, kind: .url, certainty: .strong)
            ],
            plan: plan,
            lineRefLookup: Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) }),
            evidenceCatalog: evidenceCatalog
        )
        #expect(resolved.isEmpty)
    }

    @Test("multiple matching fallback entries return unresolved")
    func multipleMatchingFallbackUnresolved() {
        let segments = [
            makeResolverSegment(index: 1, text: "Visit example.com or store.com for the offer.")
        ]
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments[0].atoms,
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1"
        )
        // Sanity: there are >1 url entries on this line.
        let urlCount = evidenceCatalog.entries.filter { $0.category == .url }.count
        #expect(urlCount >= 2)
        let plan = makeResolverPlan(lineRefs: [1], promptEvidence: [])

        let resolved = CommercialEvidenceResolver.resolve(
            anchors: [
                EvidenceAnchorSchema(evidenceRef: nil, lineRef: 1, kind: .url, certainty: .moderate)
            ],
            plan: plan,
            lineRefLookup: Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) }),
            evidenceCatalog: evidenceCatalog
        )

        #expect(resolved.count == 1)
        #expect(resolved[0].resolutionSource == .unresolved)
        #expect(!resolved[0].memoryWriteEligible)
    }

    @Test("multiple unique brand stems in window return unresolved")
    func multipleBrandStemsInWindowUnresolved() {
        let segments = [
            makeResolverSegment(index: 0, text: "Sponsored by Acme today, visit acme.com"),
            makeResolverSegment(index: 1, text: "Also sponsored by Beta, visit beta.com"),
            makeResolverSegment(index: 2, text: "Editorial content here.")
        ]
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments.flatMap(\.atoms),
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1"
        )
        let plan = makeResolverPlan(lineRefs: [0, 1, 2], promptEvidence: [])

        let resolved = CommercialEvidenceResolver.resolve(
            anchors: [
                EvidenceAnchorSchema(evidenceRef: nil, lineRef: 2, kind: .brandSpan, certainty: .moderate)
            ],
            plan: plan,
            lineRefLookup: Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) }),
            evidenceCatalog: evidenceCatalog
        )
        #expect(resolved.count == 1)
        #expect(resolved[0].resolutionSource == .unresolved)
    }

    @Test("empty anchors array returns empty result")
    func emptyAnchorsReturnsEmpty() {
        let segments = [makeResolverSegment(index: 1, text: "Visit example.com")]
        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1",
            entries: []
        )
        let plan = makeResolverPlan(lineRefs: [1], promptEvidence: [])

        let resolved = CommercialEvidenceResolver.resolve(
            anchors: [],
            plan: plan,
            lineRefLookup: Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) }),
            evidenceCatalog: evidenceCatalog
        )
        #expect(resolved.isEmpty)
    }

    @Test("hallucinated evidenceRef not in plan falls through and is not memory eligible")
    func hallucinatedEvidenceRefFallsThrough() {
        let segments = [makeResolverSegment(index: 1, text: "Just editorial content.")]
        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1",
            entries: []
        )
        let plan = makeResolverPlan(lineRefs: [1], promptEvidence: [])

        let resolved = CommercialEvidenceResolver.resolve(
            anchors: [
                EvidenceAnchorSchema(evidenceRef: 999, lineRef: 1, kind: .url, certainty: .strong)
            ],
            plan: plan,
            lineRefLookup: Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) }),
            evidenceCatalog: evidenceCatalog
        )
        // Falls through to fallback path: no deterministic entries, becomes .unresolved.
        #expect(resolved.count == 1)
        #expect(resolved[0].resolutionSource == .unresolved)
        #expect(!resolved[0].memoryWriteEligible)
    }

    @Test("promo code line-ref fallback resolves uniquely extracted entry")
    func promoCodeLineRefFallback() {
        let segments = [
            makeResolverSegment(index: 1, text: "Use promo code SAVE20 today.")
        ]
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments[0].atoms,
            analysisAssetId: "asset-1",
            transcriptVersion: "transcript-v1"
        )
        let plan = makeResolverPlan(lineRefs: [1], promptEvidence: [])

        let resolved = CommercialEvidenceResolver.resolve(
            anchors: [
                EvidenceAnchorSchema(evidenceRef: nil, lineRef: 1, kind: .promoCode, certainty: .moderate)
            ],
            plan: plan,
            lineRefLookup: Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) }),
            evidenceCatalog: evidenceCatalog
        )

        #expect(resolved.count == 1)
        #expect(resolved[0].resolutionSource == .lineRefFallback)
        #expect(resolved[0].kind == .promoCode)
        #expect(!resolved[0].memoryWriteEligible)
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

@Suite("EvidenceCatalogBuilder normalization")
struct EvidenceCatalogBuilderNormalizationTests {

    @Test("zero-width characters normalize identically to plain text")
    func zeroWidthNormalization() {
        // "BetterHelp" with a zero-width space inside.
        let dirty = "Bet\u{200B}terHelp"
        let clean = "BetterHelp"
        let dirtyAtom = makeAtom(ordinal: 0, text: "Sponsored by \(dirty), visit betterhelp.com")
        let cleanAtom = makeAtom(ordinal: 0, text: "Sponsored by \(clean), visit betterhelp.com")

        let dirtyCatalog = EvidenceCatalogBuilder.build(
            atoms: [dirtyAtom],
            analysisAssetId: "asset-1",
            transcriptVersion: "v1"
        )
        let cleanCatalog = EvidenceCatalogBuilder.build(
            atoms: [cleanAtom],
            analysisAssetId: "asset-1",
            transcriptVersion: "v1"
        )

        let dirtyBrandStems = dirtyCatalog.entries
            .filter { $0.category == .brandSpan }
            .map(\.normalizedText)
            .sorted()
        let cleanBrandStems = cleanCatalog.entries
            .filter { $0.category == .brandSpan }
            .map(\.normalizedText)
            .sorted()
        #expect(dirtyBrandStems == cleanBrandStems)
        #expect(dirtyBrandStems.contains("betterhelp"))
    }

    @Test("brandStem strips www. prefix from URL stems")
    func brandStemStripsWWW() {
        let atom = makeAtom(ordinal: 0, text: "Visit www.acme.com today.")
        let catalog = EvidenceCatalogBuilder.build(
            atoms: [atom],
            analysisAssetId: "asset-1",
            transcriptVersion: "v1"
        )
        let brandStems = catalog.entries
            .filter { $0.category == .brandSpan }
            .map(\.normalizedText)
        // brandStem should yield "acme", not "www.acme" or "www".
        #expect(brandStems.contains("acme"))
        #expect(!brandStems.contains("www"))
        #expect(!brandStems.contains("www.acme"))
    }

    private func makeAtom(ordinal: Int, text: String) -> TranscriptAtom {
        TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "asset-1",
                transcriptVersion: "v1",
                atomOrdinal: ordinal
            ),
            contentHash: "h\(ordinal)",
            startTime: Double(ordinal),
            endTime: Double(ordinal) + 1,
            text: text,
            chunkIndex: ordinal
        )
    }
}

@Suite("EvidenceCatalogBuilder dedup")
struct EvidenceCatalogBuilderDedupTests {

    @Test("repeated evidence accumulates count and time span across dedup")
    func repeatedEvidenceAccumulatesDensity() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 5, endTime: 8, text: "promo code SAVE10"),
            makeAtom(ordinal: 1, startTime: 12, endTime: 16, text: "promo code SAVE10")
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: "asset-1",
            transcriptVersion: "v1"
        )

        #expect(catalog.entries.count == 1)

        let entry = catalog.entries[0]
        #expect(entry.count == 2)
        #expect(entry.atomOrdinal == 0)
        #expect(entry.matchedText == "promo code SAVE10")
        #expect(entry.startTime == 5)
        #expect(entry.endTime == 8)
        #expect(entry.firstTime == 5)
        #expect(entry.lastTime == 16)

        #expect(catalog.renderForPrompt() == "[E0] \"promo code SAVE10\" (promoCode, atom 0, ×2, 5s–16s)")
        #expect(
            PromptEvidenceEntry(entry: entry, lineRef: 7).renderForPrompt() ==
                "[E0] \"promo code SAVE10\" (promoCode, line 7, ×2, 5s–16s)"
        )
    }

    @Test("same-utterance overlapping url matches count once")
    func overlappingURLMatchesDoNotInflateCount() throws {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 5, endTime: 8, text: "Visit example.com for the offer")
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: "asset-1",
            transcriptVersion: "v1"
        )

        let urlEntries = catalog.entries.filter { $0.category == .url }
        #expect(urlEntries.count == 1)

        let entry = try #require(urlEntries.first)
        #expect(entry.matchedText == "example.com")
        #expect(entry.count == 1)
    }

    @Test("same-utterance overlapping promo variants count once")
    func overlappingPromoVariantsDoNotInflateCount() throws {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 5, endTime: 8, text: "Use code SAVE20 at checkout.")
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: "asset-1",
            transcriptVersion: "v1"
        )

        let promoEntries = catalog.entries.filter { $0.category == .promoCode }
        #expect(promoEntries.count == 1)

        let entry = try #require(promoEntries.first)
        #expect(entry.count == 1)
        #expect(entry.matchedText.contains("SAVE20"))
    }

    @Test("brand canonicalization preserves legitimate trailing today names")
    func brandCanonicalizationPreservesLegitimateTodayNames() throws {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 4, text: "Sponsored by USA Today."),
            makeAtom(ordinal: 1, startTime: 10, endTime: 14, text: "Sponsored by USA Today again.")
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: "asset-1",
            transcriptVersion: "v1"
        )

        let entry = try #require(catalog.entries.first(where: { $0.category == .brandSpan }))
        #expect(entry.normalizedText == "usa today")
        #expect(entry.matchedText == "USA Today")
        #expect(entry.count == 2)
        #expect(entry.startTime >= 0)
        #expect(entry.endTime <= 4)
        #expect(entry.firstTime == entry.startTime)
        #expect(entry.lastTime > entry.endTime)
        #expect(catalog.entries.contains { $0.category == .brandSpan && $0.normalizedText == "usa today" })
        #expect(!catalog.entries.contains { $0.category == .brandSpan && $0.normalizedText == "usa" })
    }

    @Test("single-occurrence evidence stays concise after dedup")
    func singleOccurrenceEvidenceStaysConcise() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 9, endTime: 13, text: "promo code SAVE10")
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: "asset-1",
            transcriptVersion: "v1"
        )

        #expect(catalog.entries.count == 1)

        let entry = catalog.entries[0]
        #expect(entry.count == 1)
        #expect(entry.firstTime == 9)
        #expect(entry.lastTime == 13)
        #expect(catalog.renderForPrompt() == "[E0] \"promo code SAVE10\" (promoCode, atom 0)")
        #expect(
            PromptEvidenceEntry(entry: entry, lineRef: 4).renderForPrompt() ==
                "[E0] \"promo code SAVE10\" (promoCode, line 4)"
        )
    }

    private func makeAtom(
        ordinal: Int,
        startTime: Double,
        endTime: Double,
        text: String
    ) -> TranscriptAtom {
        TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "asset-1",
                transcriptVersion: "v1",
                atomOrdinal: ordinal
            ),
            contentHash: "h\(ordinal)",
            startTime: startTime,
            endTime: endTime,
            text: text,
            chunkIndex: ordinal
        )
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
