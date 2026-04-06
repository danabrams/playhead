// CommercialEvidenceResolver.swift
// Deterministically maps FM evidence anchors back to catalog entries and
// marks which anchors/spans are safe for future sponsor-memory writes.

import Foundation

enum CommercialEvidenceResolutionSource: String, Sendable, Codable, Hashable {
    case evidenceRef
    case lineRefFallback
    case unresolved
}

enum CommercialEvidenceResolver {

    static func resolve(
        anchors: [EvidenceAnchorSchema],
        plan: RefinementWindowPlan,
        lineRefLookup: [Int: AdTranscriptSegment],
        evidenceCatalog: EvidenceCatalog
    ) -> [ResolvedEvidenceAnchor] {
        let validLineRefs = Set(plan.lineRefs)
        let fallbackEntriesByLineRef = deterministicFallbackEntriesByLineRef(
            plan: plan,
            lineRefLookup: lineRefLookup,
            evidenceCatalog: evidenceCatalog
        )

        return anchors.compactMap { anchor in
            if let evidenceRef = anchor.evidenceRef,
               let promptEntry = plan.promptEvidence.first(where: { $0.entry.evidenceRef == evidenceRef }) {
                return ResolvedEvidenceAnchor(
                    entry: promptEntry.entry,
                    lineRef: promptEntry.lineRef,
                    kind: promptEntry.entry.category,
                    certainty: anchor.certainty,
                    resolutionSource: .evidenceRef,
                    memoryWriteEligible: true
                )
            }

            guard validLineRefs.contains(anchor.lineRef) else {
                return nil
            }

            let matchingFallbackEntries = (fallbackEntriesByLineRef[anchor.lineRef] ?? [])
                .filter { $0.category == anchor.kind.category }
            if matchingFallbackEntries.count == 1, let entry = matchingFallbackEntries.first {
                return ResolvedEvidenceAnchor(
                    entry: entry,
                    lineRef: anchor.lineRef,
                    kind: entry.category,
                    certainty: anchor.certainty,
                    resolutionSource: .lineRefFallback,
                    memoryWriteEligible: true
                )
            }

            return ResolvedEvidenceAnchor(
                entry: nil,
                lineRef: anchor.lineRef,
                kind: anchor.kind.category,
                certainty: anchor.certainty,
                resolutionSource: .unresolved,
                memoryWriteEligible: false
            )
        }
    }

    private static func deterministicFallbackEntriesByLineRef(
        plan: RefinementWindowPlan,
        lineRefLookup: [Int: AdTranscriptSegment],
        evidenceCatalog: EvidenceCatalog
    ) -> [Int: [EvidenceEntry]] {
        let windowSegments = plan.lineRefs
            .sorted()
            .compactMap { lineRefLookup[$0] }
        guard !windowSegments.isEmpty else { return [:] }

        let lineRefByAtomOrdinal = Dictionary(
            uniqueKeysWithValues: windowSegments.flatMap { segment in
                segment.atoms.map { ($0.atomKey.atomOrdinal, segment.segmentIndex) }
            }
        )
        let catalogEntriesByKey = Dictionary(
            uniqueKeysWithValues: evidenceCatalog.entries.map { (entryKey(for: $0), $0) }
        )
        let localCatalog = EvidenceCatalogBuilder.build(
            atoms: windowSegments.flatMap(\.atoms),
            analysisAssetId: evidenceCatalog.analysisAssetId,
            transcriptVersion: evidenceCatalog.transcriptVersion
        )

        var entriesByLineRef: [Int: [EvidenceEntry]] = [:]
        for localEntry in localCatalog.entries {
            guard let lineRef = lineRefByAtomOrdinal[localEntry.atomOrdinal],
                  let globalEntry = catalogEntriesByKey[entryKey(for: localEntry)] else {
                continue
            }
            entriesByLineRef[lineRef, default: []].append(globalEntry)
        }
        return entriesByLineRef
    }

    private static func entryKey(for entry: EvidenceEntry) -> String {
        "\(entry.category.rawValue)|\(entry.normalizedText)"
    }
}

private extension EvidenceAnchorKind {
    var category: EvidenceCategory {
        switch self {
        case .url:
            .url
        case .promoCode:
            .promoCode
        case .ctaPhrase:
            .ctaPhrase
        case .disclosurePhrase:
            .disclosurePhrase
        case .brandSpan:
            .brandSpan
        }
    }
}
