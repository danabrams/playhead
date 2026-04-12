// EvidenceCatalogBuilder.swift
// Deterministic extraction of commercial evidence entities from transcript atoms.
//
// Runs BEFORE Foundation Models — produces an EvidenceCatalog of typed, ref-numbered
// evidence entries that FM refinement schemas point to via `evidenceRef`. This prevents
// hallucinated evidence from poisoning the SponsorKnowledgeStore.
//
// Complementary to LexicalScanner: LexicalScanner produces merged ad REGIONS;
// EvidenceCatalogBuilder produces fine-grained evidence ENTITIES with stable refs.

import Foundation
import OSLog

// MARK: - Evidence types

/// Category of commercial evidence extracted from transcript text.
enum EvidenceCategory: String, Sendable, CaseIterable, Codable {
    case url              // URLs, vanity URLs, "dot com slash" patterns
    case promoCode        // Discount/coupon/promo codes
    case ctaPhrase        // Calls to action
    case disclosurePhrase // Sponsorship disclosures
    case brandSpan        // Brand-like proper noun spans in commercial context
}

/// A single evidence entry in the catalog.
struct EvidenceEntry: Sendable, Equatable {
    /// Stable integer ref for FM prompts: [E0], [E1], ...
    let evidenceRef: Int
    /// What kind of commercial signal this is.
    let category: EvidenceCategory
    /// The exact text matched in the transcript atom.
    let matchedText: String
    /// Lowercased/trimmed form used for deduplication.
    let normalizedText: String
    /// Which atom this came from (atomKey.atomOrdinal).
    let atomOrdinal: Int
    /// Time position of the representative occurrence kept after dedup.
    let startTime: Double
    /// Time position of the representative occurrence kept after dedup.
    let endTime: Double
    /// How many times this (category, normalizedText) pair appeared.
    let count: Int
    /// Time of the earliest occurrence in episode audio.
    let firstTime: Double
    /// Time of the latest occurrence in episode audio.
    let lastTime: Double

    /// Full coverage window for overlap/scoring consumers.
    var coverageStartTime: Double { firstTime }
    var coverageEndTime: Double { lastTime }

    init(
        evidenceRef: Int,
        category: EvidenceCategory,
        matchedText: String,
        normalizedText: String,
        atomOrdinal: Int,
        startTime: Double,
        endTime: Double,
        count: Int = 1,
        firstTime: Double? = nil,
        lastTime: Double? = nil
    ) {
        self.evidenceRef = evidenceRef
        self.category = category
        self.matchedText = matchedText
        self.normalizedText = normalizedText
        self.atomOrdinal = atomOrdinal
        self.startTime = startTime
        self.endTime = endTime
        self.count = count
        self.firstTime = firstTime ?? startTime
        self.lastTime = lastTime ?? endTime
    }
}

/// The complete evidence catalog for a transcript version.
struct EvidenceCatalog: Sendable {
    let analysisAssetId: String
    let transcriptVersion: String
    /// Ordered by evidenceRef (0, 1, 2, ...).
    let entries: [EvidenceEntry]

    /// Render compact evidence refs for FM prompt injection.
    ///
    /// Output format:
    /// ```
    /// [E0] "betterhelp.com slash podcast" (url, atom 2)
    /// [E1] "BetterHelp" (brandSpan, atom 1, ×4, 12s–67s)
    /// ```
    func renderForPrompt() -> String {
        entries.map { entry in
            "[E\(entry.evidenceRef)] \"\(entry.matchedText)\" " +
                entry.renderPromptMetadata(locationLabel: "atom", locationValue: entry.atomOrdinal)
        }.joined(separator: "\n")
    }

    /// Look up entries by category.
    func entries(for category: EvidenceCategory) -> [EvidenceEntry] {
        entries.filter { $0.category == category }
    }
}

// MARK: - EvidenceCatalogBuilder

/// Stateless extractor that scans transcript atoms for commercial evidence entities
/// and produces a deterministically-ordered EvidenceCatalog.
enum EvidenceCatalogBuilder {

    private static let logger = Logger(subsystem: "com.playhead", category: "EvidenceCatalogBuilder")

    // MARK: - Public API

    /// Build an evidence catalog from transcript atoms.
    ///
    /// - Parameters:
    ///   - atoms: Transcript atoms to scan (need not be pre-sorted).
    ///   - analysisAssetId: The analysis asset these atoms belong to.
    ///   - transcriptVersion: The transcript version string for identity.
    /// - Returns: An EvidenceCatalog with deterministically-ordered entries.
    static func build(
        atoms: [TranscriptAtom],
        analysisAssetId: String,
        transcriptVersion: String
    ) -> EvidenceCatalog {
        let sorted = atoms.sorted { $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal }

        // Phase 1: Extract anchor evidence (URL, promoCode, disclosure) from all atoms.
        // These categories establish commercial context.
        var rawMatches: [RawMatch] = []
        for atom in sorted {
            let matches = extractMatches(from: atom, categories: anchorCategories)
            rawMatches.append(contentsOf: matches)
        }

        // Phase 2: Compute commercial context window from anchor matches.
        let commercialAtomOrdinals = commercialContextOrdinals(from: rawMatches, atoms: sorted)
        let contextualBrandStems = brandStemCandidates(from: rawMatches)

        // Phase 3: Extract context-dependent evidence (CTA, brand spans) only near anchors.
        // CTAs like "check it out" and "sign up now" are too common in normal speech
        // to extract globally — gating behind commercial context prevents noise.
        for atom in sorted {
            let ctas = extractMatches(from: atom, categories: contextCategories,
                                      gatedOrdinals: commercialAtomOrdinals)
            rawMatches.append(contentsOf: ctas)
            let brands = extractBrandSpans(
                from: atom,
                commercialOrdinals: commercialAtomOrdinals,
                contextualBrandStems: contextualBrandStems
            )
            rawMatches.append(contentsOf: brands)
        }

        // Phase 4: Deduplicate by (normalizedText, category).
        let deduped = deduplicate(rawMatches)

        // Phase 5: Sort deterministically and assign evidenceRef integers.
        let entries = assignRefs(deduped)

        logger.info("Built evidence catalog: \(entries.count) entries from \(sorted.count) atoms")

        return EvidenceCatalog(
            analysisAssetId: analysisAssetId,
            transcriptVersion: transcriptVersion,
            entries: entries
        )
    }

    // MARK: - Internal types

    /// Intermediate match before dedup and ref assignment.
    private struct RawMatch {
        let category: EvidenceCategory
        let matchedText: String
        let normalizedText: String
        let atomOrdinal: Int
        let startTime: Double
        let endTime: Double
        /// Character offset of match within the atom text, for deterministic ordering.
        let matchOffset: Int
        /// Character length of the original regex match.
        let matchLength: Int
    }

    /// Deduplicated raw match with repetition density preserved.
    private struct CollapsedMatch {
        let category: EvidenceCategory
        let matchedText: String
        let normalizedText: String
        let atomOrdinal: Int
        let startTime: Double
        let endTime: Double
        let count: Int
        let firstTime: Double
        let lastTime: Double
        let matchOffset: Int
    }

    // MARK: - Pattern extraction

    /// Categories that establish commercial context (extracted globally).
    private static let anchorCategories: Set<EvidenceCategory> = [.url, .promoCode, .disclosurePhrase]

    /// Categories that require commercial context to avoid false positives.
    private static let contextCategories: Set<EvidenceCategory> = [.ctaPhrase]

    /// Extract evidence matches from a single atom for the specified categories.
    /// If `gatedOrdinals` is provided, only extracts from atoms in that set.
    private static func extractMatches(
        from atom: TranscriptAtom,
        categories: Set<EvidenceCategory>,
        gatedOrdinals: Set<Int>? = nil
    ) -> [RawMatch] {
        if let gated = gatedOrdinals, !gated.contains(atom.atomKey.atomOrdinal) {
            return []
        }

        let text = sanitizedText(atom.text)
        guard !text.isEmpty else { return [] }

        let nsText = text as NSString
        let fullRange = NSRange(location: 0, length: nsText.length)
        var matches: [RawMatch] = []

        for (category, patterns) in compiledPatterns where categories.contains(category) {
            for pattern in patterns {
                let regexMatches = pattern.matches(in: text, range: fullRange)
                for match in regexMatches {
                    let rawMatchedText = nsText.substring(with: match.range)
                    // For URL matches, strip leading verb context ("visit", "go to", "head to")
                    // so that "\bvisit \w+\.com" and "\b\w+\.com\b" both normalize to the bare
                    // domain. This prevents duplicate URL evidence entries on atoms where both
                    // patterns fire on the same mention.
                    let matchedText: String
                    if category == .url {
                        matchedText = stripURLVerbPrefix(rawMatchedText)
                    } else {
                        matchedText = rawMatchedText
                    }
                    let normalized = normalizedMatchText(matchedText, category: category)
                    guard !normalized.isEmpty else { continue }

                    let (startTime, endTime) = interpolateTiming(
                        matchRange: match.range,
                        textLength: nsText.length,
                        atomStart: atom.startTime,
                        atomEnd: atom.endTime
                    )

                    matches.append(RawMatch(
                        category: category,
                        matchedText: matchedText,
                        normalizedText: normalized,
                        atomOrdinal: atom.atomKey.atomOrdinal,
                        startTime: startTime,
                        endTime: endTime,
                        matchOffset: match.range.location,
                        matchLength: match.range.length
                    ))
                }
            }
        }

        return matches
    }

    /// Extract brand-like spans from an atom using case-insensitive strategies.
    ///
    /// ASR output is typically lowercase or inconsistently cased, so brand detection
    /// cannot rely on capitalization. Instead we use structural cues:
    /// 1. Noun phrases immediately following disclosure patterns ("sponsored by X")
    /// 2. Domain-name stems from URL patterns ("betterhelp" from "betterhelp dot com")
    ///
    /// Only runs on atoms within commercial context (±2 atoms of URL/promo/disclosure).
    private static func extractBrandSpans(
        from atom: TranscriptAtom,
        commercialOrdinals: Set<Int>,
        contextualBrandStems: Set<String>
    ) -> [RawMatch] {
        guard commercialOrdinals.contains(atom.atomKey.atomOrdinal) else { return [] }

        let text = sanitizedText(atom.text)
        guard !text.isEmpty else { return [] }

        let nsText = text as NSString
        let fullRange = NSRange(location: 0, length: nsText.length)
        var matches: [RawMatch] = []

        for pattern in compiledBrandPatterns {
            let regexMatches = pattern.matches(in: text, range: fullRange)
            for match in regexMatches {
                // Use capture group 1 (the brand name), not the full match
                let brandRange = match.numberOfRanges > 1 ? match.range(at: 1) : match.range
                guard brandRange.location != NSNotFound else { continue }

                let rawText = nsText.substring(with: brandRange)
                let trimmed = trimTrailingStopWords(rawText)
                let normalized = normalize(trimmed)
                guard !normalized.isEmpty else { continue }
                guard normalized.count >= 3 else { continue }
                guard !commonNonBrandPhrases.contains(normalized) else { continue }
                let matchedText = trimmed

                let (startTime, endTime) = interpolateTiming(
                    matchRange: brandRange,
                    textLength: nsText.length,
                    atomStart: atom.startTime,
                    atomEnd: atom.endTime
                )

                matches.append(RawMatch(
                    category: .brandSpan,
                    matchedText: matchedText,
                    normalizedText: normalized,
                    atomOrdinal: atom.atomKey.atomOrdinal,
                    startTime: startTime,
                    endTime: endTime,
                    matchOffset: brandRange.location,
                    matchLength: brandRange.length
                ))
            }
        }

        for stem in contextualBrandStems {
            let pattern = #"\b\#(NSRegularExpression.escapedPattern(for: stem))\b"#
            guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else {
                continue
            }

            for match in regex.matches(in: text, range: fullRange) {
                let rawText = nsText.substring(with: match.range)
                let normalized = normalize(rawText)
                guard !normalized.isEmpty else { continue }

                let (startTime, endTime) = interpolateTiming(
                    matchRange: match.range,
                    textLength: nsText.length,
                    atomStart: atom.startTime,
                    atomEnd: atom.endTime
                )

                matches.append(RawMatch(
                    category: .brandSpan,
                    matchedText: rawText,
                    normalizedText: normalized,
                    atomOrdinal: atom.atomKey.atomOrdinal,
                    startTime: startTime,
                    endTime: endTime,
                    matchOffset: match.range.location,
                    matchLength: match.range.length
                ))
            }
        }

        return matches
    }

    // MARK: - Commercial context

    /// Compute the set of atom ordinals that are within +/-2 of any atom containing
    /// a URL, promo code, or disclosure match. Brand spans are only extracted from
    /// atoms in this context window.
    ///
    /// Assumes contiguous zero-based ordinals (guaranteed by TranscriptAtomizer).
    private static func commercialContextOrdinals(
        from rawMatches: [RawMatch],
        atoms: [TranscriptAtom]
    ) -> Set<Int> {
        let commercialCategories: Set<EvidenceCategory> = [.url, .promoCode, .disclosurePhrase]
        let anchorOrdinals = Set(
            rawMatches
                .filter { commercialCategories.contains($0.category) }
                .map(\.atomOrdinal)
        )

        guard !anchorOrdinals.isEmpty else { return [] }

        let maxOrdinal = atoms.last?.atomKey.atomOrdinal ?? 0
        var contextOrdinals = Set<Int>()
        for ordinal in anchorOrdinals {
            for offset in -2...2 {
                let candidate = ordinal + offset
                if candidate >= 0, candidate <= maxOrdinal {
                    contextOrdinals.insert(candidate)
                }
            }
        }

        return contextOrdinals
    }

    private static func brandStemCandidates(from rawMatches: [RawMatch]) -> Set<String> {
        Set(rawMatches.compactMap { match in
            guard match.category == .url else { return nil }
            return brandStem(from: match.normalizedText)
        })
    }

    /// Sanitize raw atom text BEFORE pattern matching: applies NFKC and strips
    /// Cc/Cf categories so that zero-width joiners and BOMs do not break `\w`
    /// boundaries inside brand names. Casing is preserved (the regex engine
    /// uses `.caseInsensitive`).
    static func sanitizedText(_ text: String) -> String {
        let nfkc = text.precomposedStringWithCompatibilityMapping
        var stripped = String.UnicodeScalarView()
        stripped.reserveCapacity(nfkc.unicodeScalars.count)
        for scalar in nfkc.unicodeScalars {
            if CharacterSet.controlCharacters.contains(scalar) { continue }
            if formatCategoryScalars.contains(scalar) { continue }
            stripped.append(scalar)
        }
        return String(stripped)
    }

    /// Normalize a raw matched text for catalog lookup keys.
    ///
    /// Applies NFKC compatibility decomposition (so visually-identical glyphs
    /// like "ﬁ" and "fi" map to the same string), strips Unicode control and
    /// format characters (Cc/Cf categories — invisible joiners, BOMs, ZWSPs),
    /// lowercases, and trims whitespace. Lookups are stable when upstream
    /// transcribers emit zero-width characters or compatibility codepoints.
    static func normalize(_ text: String) -> String {
        let nfkc = text.precomposedStringWithCompatibilityMapping
        var stripped = String.UnicodeScalarView()
        stripped.reserveCapacity(nfkc.unicodeScalars.count)
        for scalar in nfkc.unicodeScalars {
            // Drop control (Cc) and format (Cf) characters: zero-width spaces,
            // BOMs, joiners, directional marks, etc. Keep whitespace categories
            // (Zs/Zl/Zp) — those are trimmed by the .whitespacesAndNewlines pass
            // below.
            if CharacterSet.controlCharacters.contains(scalar) { continue }
            if formatCategoryScalars.contains(scalar) { continue }
            stripped.append(scalar)
        }
        return String(stripped)
            .lowercased()
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func normalizedMatchText(_ text: String, category: EvidenceCategory) -> String {
        switch category {
        case .promoCode:
            promoCodeToken(from: text) ?? normalize(text)
        default:
            normalize(text)
        }
    }

    private static func promoCodeToken(from text: String) -> String? {
        guard let regex = try? NSRegularExpression(pattern: #"\bcode\s+([A-Za-z0-9]+)\b"#,
                                                   options: [.caseInsensitive]) else {
            return nil
        }
        let nsText = text as NSString
        let range = NSRange(location: 0, length: nsText.length)
        guard let match = regex.firstMatch(in: text, range: range),
              match.numberOfRanges > 1 else {
            return nil
        }
        return normalize(nsText.substring(with: match.range(at: 1)))
    }

    /// Cf (Format) category scalars include zero-width joiners, soft hyphens,
    /// directional marks, and the byte-order mark. CharacterSet doesn't expose
    /// the Cf category directly, so we test via the per-scalar Unicode property.
    private static let formatCategoryScalars: CharacterSet = {
        var set = CharacterSet()
        // Build by walking the BMP + supplementary planes is too expensive at
        // class init; instead, use the documented Unicode general category test.
        // Apple's CharacterSet doesn't expose Cf, so we approximate by listing
        // the most common offenders and falling back to a runtime test.
        let knownFormatScalars: [Unicode.Scalar] = [
            "\u{00AD}", // SOFT HYPHEN
            "\u{200B}", // ZERO WIDTH SPACE
            "\u{200C}", // ZERO WIDTH NON-JOINER
            "\u{200D}", // ZERO WIDTH JOINER
            "\u{200E}", // LEFT-TO-RIGHT MARK
            "\u{200F}", // RIGHT-TO-LEFT MARK
            "\u{202A}", // LRE
            "\u{202B}", // RLE
            "\u{202C}", // PDF
            "\u{202D}", // LRO
            "\u{202E}", // RLO
            "\u{2060}", // WORD JOINER
            "\u{2061}", "\u{2062}", "\u{2063}", "\u{2064}",
            "\u{FEFF}", // BYTE ORDER MARK / ZWNBSP
        ]
        for scalar in knownFormatScalars {
            set.insert(scalar)
        }
        return set
    }()

    private static func brandStem(from normalizedURL: String) -> String? {
        // Strip a leading "www." prefix BEFORE splitting on TLD separators so
        // that "www.acme.com" yields stem "acme" rather than "www.acme".
        let trimmed: String
        if normalizedURL.hasPrefix("www.") {
            trimmed = String(normalizedURL.dropFirst(4))
        } else {
            trimmed = normalizedURL
        }
        let separators = [
            ".com/",
            ".org/",
            ".io/",
            ".co/",
            " dot com slash ",
            " dot org slash ",
            " dot io slash ",
            " dot co slash ",
            " com slash ",
            " org slash ",
            " io slash ",
            " co slash ",
            ".com",
            ".org",
            ".io",
            ".co",
            " dot com",
            " dot org",
            " dot io",
            " dot co",
        ]

        for separator in separators {
            guard let range = trimmed.range(of: separator) else { continue }
            let stem = String(trimmed[..<range.lowerBound])
                .trimmingCharacters(in: .whitespacesAndNewlines)
            if stem.count >= 3 {
                return stem
            }
        }

        return nil
    }

    // MARK: - URL normalization

    /// Leading verb-prefix patterns that the URL regexes may capture as context
    /// (e.g., "visit ", "go to ", "head to "). Stripped at ingestion so that a
    /// context-match like "visit teamcoco.com" normalizes to the same text as
    /// the bare-domain pattern "teamcoco.com". Order matters: longer prefixes
    /// are matched first.
    private static let urlVerbPrefixes: [String] = [
        "head to ",
        "go to ",
        "visit ",
        "check out ",
    ]

    /// Strip a leading verb phrase ("visit ", "go to ", ...) from a URL match.
    /// Case-insensitive; preserves the casing of the remainder.
    private static func stripURLVerbPrefix(_ text: String) -> String {
        let lower = text.lowercased()
        for prefix in urlVerbPrefixes where lower.hasPrefix(prefix) {
            return String(text.dropFirst(prefix.count))
                .trimmingCharacters(in: .whitespacesAndNewlines)
        }
        return text
    }

    // MARK: - Deduplication

    /// Deduplicate matches by (normalizedText, category). Keeps the first occurrence
    /// (lowest atomOrdinal, then lowest matchOffset). Also subsumes:
    /// 1. Bare-domain URLs when a more specific path URL exists globally
    ///    (e.g., removes "acme.com" when "acme.com/offer" is present).
    /// 2. Within a single atom, the LONGER of two URL matches when one contains
    ///    the other as a substring — the shorter (cleaner) domain is preferred.
    ///    This complements stripURLVerbPrefix for any context-capturing patterns
    ///    we may add in the future.
    private static func deduplicate(_ matches: [RawMatch]) -> [CollapsedMatch] {
        // Pre-sort to ensure dedup picks the earliest occurrence deterministically.
        let sorted = matches.sorted { a, b in
            if a.atomOrdinal != b.atomOrdinal { return a.atomOrdinal < b.atomOrdinal }
            if a.matchOffset != b.matchOffset { return a.matchOffset < b.matchOffset }
            if a.matchLength != b.matchLength { return a.matchLength < b.matchLength }
            if a.category != b.category { return a.category.rawValue < b.category.rawValue }
            return a.normalizedText < b.normalizedText
        }

        // Collect all URL normalized texts (globally) to detect path-URL subsumption.
        let urlTexts = Set(sorted.filter { $0.category == .url }.map(\.normalizedText))

        // Collect URL normalized texts per atom for in-atom substring subsumption.
        var urlTextsByAtom: [Int: Set<String>] = [:]
        for match in sorted where match.category == .url {
            urlTextsByAtom[match.atomOrdinal, default: []].insert(match.normalizedText)
        }

        let filtered = sorted.filter { match in
            guard match.category == .url else { return true }

            // Subsume bare-domain URL when a longer path URL exists.
            // e.g., skip "acme.com" when "acme.com/offer" is present.
            let isPathSubsumed = urlTexts.contains { other in
                other != match.normalizedText &&
                other.hasPrefix(match.normalizedText) &&
                (other.dropFirst(match.normalizedText.count).first == "/" ||
                 other.dropFirst(match.normalizedText.count).first == " ")
            }
            if isPathSubsumed { return false }

            // Within the same atom, if another URL match is a strict
            // suffix of this one (i.e., this match has *prefix* context
            // like "visit " or "go to " in front of the same domain),
            // drop this longer match and keep the cleaner one.
            let siblings = urlTextsByAtom[match.atomOrdinal] ?? []
            let isContextSubsumed = siblings.contains { other in
                other != match.normalizedText &&
                other.count < match.normalizedText.count &&
                match.normalizedText.hasSuffix(other)
            }
            return !isContextSubsumed
        }

        let canonicalized = collapseOverlappingOccurrences(canonicalizeBrandVariants(filtered))

        struct AggregatedMatch {
            var category: EvidenceCategory
            var matchedText: String
            var normalizedText: String
            var atomOrdinal: Int
            var startTime: Double
            var endTime: Double
            var count: Int
            var firstTime: Double
            var lastTime: Double
            var matchOffset: Int
        }

        var aggregatedByKey: [String: AggregatedMatch] = [:]
        var orderedKeys: [String] = []
        for match in canonicalized {
            let key = "\(match.category.rawValue)::\(match.normalizedText)"
            if var aggregate = aggregatedByKey[key] {
                aggregate.count += 1
                aggregate.firstTime = min(aggregate.firstTime, match.startTime)
                aggregate.lastTime = max(aggregate.lastTime, match.endTime)
                aggregatedByKey[key] = aggregate
            } else {
                aggregatedByKey[key] = AggregatedMatch(
                    category: match.category,
                    matchedText: match.matchedText,
                    normalizedText: match.normalizedText,
                    atomOrdinal: match.atomOrdinal,
                    startTime: match.startTime,
                    endTime: match.endTime,
                    count: 1,
                    firstTime: match.startTime,
                    lastTime: match.endTime,
                    matchOffset: match.matchOffset
                )
                orderedKeys.append(key)
            }
        }

        var result: [CollapsedMatch] = []
        for key in orderedKeys {
            guard let match = aggregatedByKey[key] else { continue }

            result.append(
                CollapsedMatch(
                    category: match.category,
                    matchedText: match.matchedText,
                    normalizedText: match.normalizedText,
                    atomOrdinal: match.atomOrdinal,
                    startTime: match.startTime,
                    endTime: match.endTime,
                    count: match.count,
                    firstTime: match.firstTime,
                    lastTime: match.lastTime,
                    matchOffset: match.matchOffset
                )
            )
        }

        return result
    }

    // MARK: - Ref assignment

    /// Assign stable evidenceRef integers in deterministic order:
    /// sorted by atomOrdinal, then by category ordinal, then by match offset.
    private static func assignRefs(_ matches: [CollapsedMatch]) -> [EvidenceEntry] {
        let categoryOrder = Dictionary(
            uniqueKeysWithValues: EvidenceCategory.allCases.enumerated().map { ($1, $0) }
        )

        let sorted = matches.sorted { a, b in
            if a.atomOrdinal != b.atomOrdinal { return a.atomOrdinal < b.atomOrdinal }
            let catA = categoryOrder[a.category] ?? 0
            let catB = categoryOrder[b.category] ?? 0
            if catA != catB { return catA < catB }
            return a.matchOffset < b.matchOffset
        }

        return sorted.enumerated().map { index, match in
            EvidenceEntry(
                evidenceRef: index,
                category: match.category,
                matchedText: match.matchedText,
                normalizedText: match.normalizedText,
                atomOrdinal: match.atomOrdinal,
                startTime: match.startTime,
                endTime: match.endTime,
                count: match.count,
                firstTime: match.firstTime,
                lastTime: match.lastTime
            )
        }
    }

    // MARK: - Time interpolation

    /// Estimate timing of a regex match within an atom by linear interpolation
    /// over character position.
    private static func interpolateTiming(
        matchRange: NSRange,
        textLength: Int,
        atomStart: Double,
        atomEnd: Double
    ) -> (start: Double, end: Double) {
        guard textLength > 0 else { return (atomStart, atomEnd) }

        let duration = atomEnd - atomStart
        let startFraction = Double(matchRange.location) / Double(textLength)
        let endFraction = Double(matchRange.location + matchRange.length) / Double(textLength)

        return (atomStart + duration * startFraction, atomStart + duration * endFraction)
    }

    // MARK: - Compiled patterns

    /// Compiled patterns grouped by category (excluding brandSpan, which uses separate logic).
    /// Built once via static let for performance.
    private static let compiledPatterns: [EvidenceCategory: [NSRegularExpression]] = {
        var groups: [EvidenceCategory: [NSRegularExpression]] = [:]

        // URLs / vanity URLs
        groups[.url] = compilePatterns([
            #"\b\w+\.com\/\w+"#,                     // literal URL: betterhelp.com/podcast
            #"\b\w+ dot com slash \w+"#,              // spoken: "betterhelp dot com slash podcast"
            #"\b\w+\.com\b"#,                         // bare domain: betterhelp.com
            #"\b\w+ dot com\b"#,                      // spoken bare domain: "betterhelp dot com"
            #"\b(?!dot\b)\w+ com slash \w+"#,          // ASR-normalized: "betterhelp com slash podcast" (excludes "dot com slash ..." false match)
            #"\bgo to \w+\.com"#,                      // "go to betterhelp.com"
            #"\bvisit \w+\.com"#,                      // "visit betterhelp.com"
            #"\bhead to \w+\.com"#,                    // "head to betterhelp.com"
            #"\b\w+\.co\/\w+"#,                       // short domains: something.co/offer
            #"\b\w+\.org\/\w+"#,                      // .org URLs
            #"\b\w+\.io\/\w+"#,                       // .io URLs
        ])

        // Promo codes — require qualifying prefix to avoid false positives
        // on generic "code" usage (e.g. "source code repository")
        groups[.promoCode] = compilePatterns([
            #"\bpromo code\s+[A-Za-z0-9]+"#,          // "promo code SAVE"
            #"\bdiscount code\s+[A-Za-z0-9]+"#,       // "discount code THIRTY"
            #"\bcoupon code\s+[A-Za-z0-9]+"#,          // "coupon code FREE"
            #"\boffer code\s+[A-Za-z0-9]+"#,           // "offer code COURT"
            #"\benter code\s+[A-Za-z0-9]+"#,           // "enter code X at checkout"
            #"\buse code\s+[A-Za-z0-9]+"#,             // "use code SAVE"
            #"\bcode\s+[A-Za-z0-9]+\s+at checkout"#,  // "code SAVE at checkout"
        ])

        // CTA phrases
        groups[.ctaPhrase] = compilePatterns([
            #"\bget started today\b"#,
            #"\bsign up now\b"#,
            #"\bsign up today\b"#,
            #"\bclick the link\b"#,
            #"\blink in the description\b"#,
            #"\blink in the show notes\b"#,
            #"\btap the link\b"#,
            #"\bcheck it out\b"#,
            #"\bhead over to\b"#,
            #"\bgo check out\b"#,
            #"\btry it free\b"#,
            #"\btry it today\b"#,
            #"\bstart your free trial\b"#,
            #"\bget your free\b"#,
            #"\bdon.?t miss out\b"#,
            #"\bact now\b"#,
            #"\blimited time\b"#,
            #"\bexclusive offer\b"#,
            #"\bspecial offer\b"#,
        ])

        // Disclosure phrases
        groups[.disclosurePhrase] = compilePatterns([
            #"\bbrought to you by\b"#,
            #"\bsponsored by\b"#,
            #"\bpartnered with\b"#,
            #"\bin partnership with\b"#,
            #"\bthanks to our sponsor\b"#,
            #"\bthis episode is sponsored\b"#,
            #"\bthis podcast is brought\b"#,
            #"\ba word from our sponsor\b"#,
            #"\bmessage from our sponsor\b"#,
            #"\bsupported by\b"#,
            #"\btoday s sponsor\b"#,
            #"\btoday's sponsor\b"#,
        ])

        return groups
    }()

    /// Single words to exclude as entire brand spans.
    private static let commonNonBrandPhrases: Set<String> = [
        "our", "the", "this", "their", "your", "my",
        "our friends", "our friends at", "our partner", "our partners",
        "our sponsor", "today", "you", "them", "us",
    ]

    /// Stop words trimmed from the trailing end of brand captures.
    /// Prevents greedy regex from capturing "hello fresh and they" instead of "hello fresh".
    private static let brandStopWords: Set<String> = [
        "and", "or", "but", "for", "the", "a", "an",
        "to", "at", "in", "on", "with", "from", "of", "by", "as",
        "they", "we", "it", "is", "are", "was", "were", "has", "have",
        "that", "this", "will", "can", "so", "if", "do", "did",
        "just", "really", "very", "also", "then", "now", "here", "there",
        "about", "like", "make", "visit", "who", "which", "where",
        "not", "no", "all", "every", "some", "many", "more",
    ]

    /// Trailing discourse tokens that sometimes get captured by disclosure
    /// patterns. These are only stripped when corroborating variants prove the
    /// shorter prefix is the true brand name.
    private static let disclosureTrailingBrandTokens: Set<String> = [
        "again", "today", "tonight", "tomorrow", "yesterday"
    ]

    /// Trim trailing stop words from a brand capture.
    /// "hello fresh and they make" -> "hello fresh"
    private static func trimTrailingStopWords(_ text: String) -> String {
        var words = text.split(separator: " ").map(String.init)
        while let last = words.last, brandStopWords.contains(last.lowercased()) {
            words.removeLast()
        }
        return words.joined(separator: " ")
    }

    private static func canonicalizeBrandVariants(_ matches: [RawMatch]) -> [RawMatch] {
        let brandTexts = Set(
            matches
                .filter { $0.category == .brandSpan }
                .map(\.normalizedText)
        )

        var suffixesByPrefix: [String: Set<String>] = [:]
        for text in brandTexts {
            let words = text.split(separator: " ").map(String.init)
            guard words.count >= 2, let suffix = words.last,
                  disclosureTrailingBrandTokens.contains(suffix) else { continue }
            let prefix = words.dropLast().joined(separator: " ")
            suffixesByPrefix[prefix, default: []].insert(suffix)
        }

        let canonicalPrefixes = Set(
            suffixesByPrefix.compactMap { prefix, suffixes in
                if brandTexts.contains(prefix) || suffixes.count >= 2 {
                    return prefix
                }
                return nil
            }
        )

        return matches.map { match in
            guard match.category == .brandSpan else { return match }

            let words = match.normalizedText.split(separator: " ").map(String.init)
            guard words.count >= 2, let suffix = words.last,
                  disclosureTrailingBrandTokens.contains(suffix) else {
                return match
            }

            let prefix = words.dropLast().joined(separator: " ")
            guard canonicalPrefixes.contains(prefix) else { return match }

            let rawWords = match.matchedText.split(separator: " ").map(String.init)
            let canonicalMatchedText: String
            if rawWords.count == words.count,
               rawWords.last?.lowercased() == suffix {
                canonicalMatchedText = rawWords.dropLast().joined(separator: " ")
            } else {
                canonicalMatchedText = match.matchedText
            }

            return RawMatch(
                category: match.category,
                matchedText: canonicalMatchedText,
                normalizedText: prefix,
                atomOrdinal: match.atomOrdinal,
                startTime: match.startTime,
                endTime: match.endTime,
                matchOffset: match.matchOffset,
                matchLength: match.matchLength
            )
        }
    }

    /// Collapse overlapping regex variants that point at the same mention within
    /// one atom so repetition count reflects utterances, not pattern multiplicity.
    /// Overlap is keyed by atom + category rather than normalizedText alone
    /// because the same spoken mention can be captured by multiple patterns with
    /// different normalized forms (for example "use code SAVE10" and
    /// "code SAVE10 at checkout").
    private static func collapseOverlappingOccurrences(_ matches: [RawMatch]) -> [RawMatch] {
        var collapsed: [RawMatch] = []

        for match in matches {
            let hasOverlappingSibling = collapsed.contains { candidate in
                candidate.category == match.category &&
                candidate.atomOrdinal == match.atomOrdinal &&
                rangesOverlap(
                    startA: candidate.matchOffset,
                    lengthA: candidate.matchLength,
                    startB: match.matchOffset,
                    lengthB: match.matchLength
                )
            }
            if hasOverlappingSibling {
                continue
            }
            collapsed.append(match)
        }

        return collapsed
    }

    private static func rangesOverlap(
        startA: Int,
        lengthA: Int,
        startB: Int,
        lengthB: Int
    ) -> Bool {
        let endA = startA + lengthA
        let endB = startB + lengthB
        return startA < endB && startB < endA
    }

    /// Brand extraction patterns — case-insensitive, use capture groups.
    ///
    /// ASR output is typically lowercase, so these patterns do not rely on
    /// capitalization. Instead they extract noun phrases after disclosure
    /// patterns and domain stems from URL patterns.
    private static let compiledBrandPatterns: [NSRegularExpression] = compilePatterns([
        // Noun phrase after "sponsored by" — capture 1-4 words, then trim stop words
        #"\bsponsored by\s+(?:our\s+friends\s+at\s+)?(\w+(?:\s+\w+){0,3})"#,
        // Noun phrase after "brought to you by"
        #"\bbrought to you by\s+(?:our\s+friends\s+at\s+)?(\w+(?:\s+\w+){0,3})"#,
        // Noun phrase after "partnered with"
        #"\bpartnered with\s+(\w+(?:\s+\w+){0,3})"#,
        // Noun phrase after "supported by" — narrower: skip common non-commercial uses
        #"\bsupported by\s+(?:our\s+friends\s+at\s+)?(\w+(?:\s+\w+){0,3})"#,
        // "thanks to our sponsor(s) X" — narrowed to require "sponsor" to avoid
        // false positives from conversational "thanks to our listeners"
        #"\bthanks to\s+our\s+sponsors?\s+(\w+(?:\s+\w+){0,3})"#,
        // Domain stem from "X dot com" or "X.com" patterns
        #"\b(\w+)\s+dot\s+com\b"#,
        #"\b(\w+)\.com\b"#,
    ])

    /// Compile pattern strings into NSRegularExpression instances.
    /// Patterns that fail to compile are logged and skipped.
    private static func compilePatterns(
        _ patterns: [String],
        options: NSRegularExpression.Options = [.caseInsensitive]
    ) -> [NSRegularExpression] {
        patterns.compactMap { pattern in
            do {
                return try NSRegularExpression(pattern: pattern, options: options)
            } catch {
                logger.error("Failed to compile pattern '\(pattern)': \(error.localizedDescription)")
                return nil
            }
        }
    }
}

// MARK: - Prompt rendering helpers

extension EvidenceEntry {
    func renderPromptMetadata(locationLabel: String, locationValue: Int) -> String {
        var parts = ["\(category.rawValue)", "\(locationLabel) \(locationValue)"]
        if count > 1 {
            parts.append("×\(count)")
            parts.append("\(Self.formatPromptTime(firstTime))–\(Self.formatPromptTime(lastTime))")
        }
        return "(\(parts.joined(separator: ", ")))"
    }

    private static func formatPromptTime(_ value: Double) -> String {
        if value.rounded() == value {
            return "\(Int(value))s"
        }

        let formatted = String(format: "%.1f", locale: Locale(identifier: "en_US_POSIX"), value)
        return formatted.hasSuffix(".0") ? String(formatted.dropLast(2)) + "s" : "\(formatted)s"
    }
}
