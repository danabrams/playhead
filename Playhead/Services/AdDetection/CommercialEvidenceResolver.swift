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

            var matchingFallbackEntries = (fallbackEntriesByLineRef[anchor.lineRef] ?? [])
                .filter { $0.category == anchor.kind.category }
            var usedWindowContextFallback = false
            if matchingFallbackEntries.isEmpty, anchor.kind == .brandSpan {
                matchingFallbackEntries = uniqueWindowFallbackEntries(
                    byLineRef: fallbackEntriesByLineRef,
                    matching: anchor.kind.category
                )
                usedWindowContextFallback = !matchingFallbackEntries.isEmpty
            }
            if matchingFallbackEntries.count == 1, let entry = matchingFallbackEntries.first {
                let resolvedEntry: EvidenceEntry
                if usedWindowContextFallback,
                   let segment = lineRefLookup[anchor.lineRef],
                   let contextualized = contextualizedBrandEntry(
                    from: entry,
                    in: segment
                   ) {
                    resolvedEntry = contextualized
                } else {
                    resolvedEntry = entry
                }

                return ResolvedEvidenceAnchor(
                    entry: resolvedEntry,
                    lineRef: anchor.lineRef,
                    kind: resolvedEntry.category,
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

    private static func uniqueWindowFallbackEntries(
        byLineRef fallbackEntriesByLineRef: [Int: [EvidenceEntry]],
        matching category: EvidenceCategory
    ) -> [EvidenceEntry] {
        var seenKeys = Set<String>()
        var uniqueEntries: [EvidenceEntry] = []

        for entry in fallbackEntriesByLineRef.values
            .flatMap({ $0 })
            .filter({ $0.category == category }) {
            let key = entryKey(for: entry)
            if seenKeys.insert(key).inserted {
                uniqueEntries.append(entry)
            }
        }

        return uniqueEntries
    }

    private static func contextualizedBrandEntry(
        from entry: EvidenceEntry,
        in segment: AdTranscriptSegment
    ) -> EvidenceEntry? {
        let text = segment.text
        let nsText = text as NSString
        let escaped = NSRegularExpression.escapedPattern(for: entry.normalizedText)
        guard let regex = try? NSRegularExpression(
            pattern: #"\b\#(escaped)\b"#,
            options: [.caseInsensitive]
        ) else {
            return nil
        }

        let range = NSRange(location: 0, length: nsText.length)
        guard let match = regex.firstMatch(in: text, range: range) else {
            return nil
        }

        let matchedText = nsText.substring(with: match.range)
        let (startTime, endTime) = interpolateTiming(
            matchRange: match.range,
            textLength: nsText.length,
            segment: segment
        )

        return EvidenceEntry(
            evidenceRef: entry.evidenceRef,
            category: entry.category,
            matchedText: matchedText,
            normalizedText: entry.normalizedText,
            atomOrdinal: segment.firstAtomOrdinal,
            startTime: startTime,
            endTime: endTime
        )
    }

    private static func interpolateTiming(
        matchRange: NSRange,
        textLength: Int,
        segment: AdTranscriptSegment
    ) -> (Double, Double) {
        guard textLength > 0 else { return (segment.startTime, segment.endTime) }

        let duration = segment.endTime - segment.startTime
        let startFraction = Double(matchRange.location) / Double(textLength)
        let endFraction = Double(matchRange.location + matchRange.length) / Double(textLength)
        return (
            segment.startTime + duration * startFraction,
            segment.startTime + duration * endFraction
        )
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
