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

        var seenAnchorKeys = Set<String>()
        var resolvedAnchors: [ResolvedEvidenceAnchor] = []

        for anchor in anchors {
            // Compute a dedup key BEFORE resolution to collapse identical FM anchors.
            // Same (kind, lineRef, evidenceRef-or-nil) → keep only the first.
            let anchorDedupKey = "\(anchor.kind.rawValue)|\(anchor.lineRef)|\(anchor.evidenceRef.map(String.init) ?? "-")"
            guard seenAnchorKeys.insert(anchorDedupKey).inserted else { continue }

            if let evidenceRef = anchor.evidenceRef,
               let promptEntry = plan.promptEvidence.first(where: { $0.entry.evidenceRef == evidenceRef }) {
                resolvedAnchors.append(
                    ResolvedEvidenceAnchor(
                        entry: promptEntry.entry,
                        lineRef: promptEntry.lineRef,
                        kind: promptEntry.entry.category,
                        certainty: anchor.certainty,
                        resolutionSource: .evidenceRef,
                        memoryWriteEligible: true
                    )
                )
                continue
            }

            guard validLineRefs.contains(anchor.lineRef) else {
                continue
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
                let resolvedEntry: EvidenceEntry?
                if usedWindowContextFallback {
                    // Brand window-context fallback: we MUST contextualize the brand
                    // back into the anchor's segment so the timing/atomOrdinal we emit
                    // belong to the line FM actually pointed at — not a different line
                    // where the brand was originally extracted. If contextualization
                    // fails (regex misses, anchor's segment text doesn't contain the
                    // brand), we MUST NOT stamp the anchor's lineRef onto a foreign
                    // segment's timing — fall through to .unresolved instead.
                    if let segment = lineRefLookup[anchor.lineRef],
                       let contextualized = contextualizedBrandEntry(from: entry, in: segment) {
                        resolvedEntry = contextualized
                    } else {
                        resolvedEntry = nil
                    }
                } else {
                    resolvedEntry = entry
                }

                if let resolved = resolvedEntry {
                    resolvedAnchors.append(
                        ResolvedEvidenceAnchor(
                            entry: resolved,
                            lineRef: anchor.lineRef,
                            kind: resolved.category,
                            certainty: anchor.certainty,
                            resolutionSource: .lineRefFallback,
                            // Per the file header contract: only FM-attested .evidenceRef
                            // resolution is safe for sponsor-memory writes. Both
                            // line-ref fallback and window-context fallback are
                            // deterministic-only: they may attach evidence we trust
                            // for classification, but they are NOT FM-attested.
                            memoryWriteEligible: false
                        )
                    )
                    continue
                }
                // contextualization failed → fall through to unresolved
            }

            resolvedAnchors.append(
                ResolvedEvidenceAnchor(
                    entry: nil,
                    lineRef: anchor.lineRef,
                    kind: anchor.kind.category,
                    certainty: anchor.certainty,
                    resolutionSource: .unresolved,
                    memoryWriteEligible: false
                )
            )
        }

        return resolvedAnchors
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

    /// Re-locate a brand evidence entry inside a different transcript segment so the
    /// resulting entry's atomOrdinal/timing belong to that segment rather than the
    /// segment where the brand was originally extracted.
    ///
    /// Timing is computed by **linear interpolation over character offsets**: we
    /// scale the start/end of the regex match by `matchRange / nsText.length`
    /// against the segment's wall-clock duration. This is approximate — words at
    /// the start of a long pause receive bogus offsets — and is suitable for
    /// banner/diagnostic display only. **Do NOT** use these times for tight
    /// skip-cut boundaries; use the FM-attested span boundaries instead.
    ///
    /// The regex uses `.useUnicodeWordBoundaries` so that brand names containing
    /// non-ASCII characters (e.g. "Café", "Müller", "naïve") are recognised as
    /// whole words; the default `\b` only treats `[A-Za-z0-9_]` as word chars
    /// and would fail on letters with diacritics.
    private static func contextualizedBrandEntry(
        from entry: EvidenceEntry,
        in segment: AdTranscriptSegment
    ) -> EvidenceEntry? {
        let text = segment.text
        let nsText = text as NSString
        let escaped = NSRegularExpression.escapedPattern(for: entry.normalizedText)
        guard let regex = try? NSRegularExpression(
            pattern: #"\b\#(escaped)\b"#,
            options: [.caseInsensitive, .useUnicodeWordBoundaries]
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

    /// Char-offset linear interpolation. Approximate; see `contextualizedBrandEntry`
    /// for the precision caveat. Not safe for skip-cut boundaries.
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

        // Use uniquingKeysWith: { first, _ in first } so we never trap on duplicate
        // atomOrdinals (can occur with multi-segment overlap or test fixtures) or
        // duplicate (category, normalizedText) catalog entries (can occur if the
        // catalog was rebuilt with overlapping windows). Keeping the first match
        // is deterministic because windowSegments is pre-sorted by lineRef.
        let lineRefByAtomOrdinal = Dictionary(
            windowSegments.flatMap { segment in
                segment.atoms.map { ($0.atomKey.atomOrdinal, segment.segmentIndex) }
            },
            uniquingKeysWith: { first, _ in first }
        )
        let catalogEntriesByKey = Dictionary(
            evidenceCatalog.entries.map { (entryKey(for: $0), $0) },
            uniquingKeysWith: { first, _ in first }
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
