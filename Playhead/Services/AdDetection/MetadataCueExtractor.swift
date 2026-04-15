// MetadataCueExtractor.swift
// ef2.2.2: Deterministic metadata cue extraction from episode RSS text.
//
// Pipeline stages:
// 1. HTML stripping + entity decoding
// 2. URL extraction and normalization (eTLD+1, strip tracking params)
// 3. Sentence windowing + casefolding
// 4. Regex-based cue detection (disclosures, promo codes, sponsor aliases)
// 5. Domain classification (show-owned, network-owned, external)
//
// Shadow mode only — results are returned/logged but don't influence
// any live ad detection scoring.

import Foundation
import OSLog

// MARK: - MetadataCueExtractor

/// Extracts structured metadata cues from episode RSS description and summary text.
///
/// Deterministic: same input always produces same output. No ML models,
/// no Foundation Models — pure regex and string matching.
///
/// Thread-safe: all state is either immutable or isolated to method scope.
struct MetadataCueExtractor: Sendable {

    private let logger = Logger(subsystem: "com.playhead", category: "MetadataCueExtractor")

    /// Known sponsor names for fuzzy matching. When a SponsorKnowledgeStore
    /// is available, these are populated from active entries.
    private let knownSponsors: [String]

    /// Domains owned by the show (e.g. "teamcoco.com"). Used to classify
    /// extracted URLs as show-owned vs external.
    private let showOwnedDomains: Set<String>

    /// Domains owned by the podcast network (e.g. "earwolf.com").
    private let networkOwnedDomains: Set<String>

    // MARK: - Init

    init(
        knownSponsors: [String] = [],
        showOwnedDomains: Set<String> = [],
        networkOwnedDomains: Set<String> = []
    ) {
        self.knownSponsors = knownSponsors
        self.showOwnedDomains = showOwnedDomains
        self.networkOwnedDomains = networkOwnedDomains
    }

    // MARK: - Public API

    /// Extract metadata cues from episode description and/or summary text.
    ///
    /// - Parameters:
    ///   - description: The episode's RSS `<description>` content (may contain HTML).
    ///   - summary: The episode's RSS `<itunes:summary>` content (may contain HTML).
    /// - Returns: Array of extracted cues, deduplicated by (type, normalizedValue, sourceField).
    func extractCues(
        description: String?,
        summary: String?
    ) -> [EpisodeMetadataCue] {
        var cues: [EpisodeMetadataCue] = []

        if let description, !description.isEmpty {
            let normalized = Self.normalizeText(description)
            let extracted = extractFromNormalizedText(normalized, sourceField: .description)
            cues.append(contentsOf: extracted)
        }

        if let summary, !summary.isEmpty {
            let normalized = Self.normalizeText(summary)
            let extracted = extractFromNormalizedText(normalized, sourceField: .summary)
            cues.append(contentsOf: extracted)
        }

        let deduplicated = deduplicateCues(cues)

        logger.debug("Extracted \(deduplicated.count) cues from episode metadata")

        return deduplicated
    }

    // MARK: - Text Normalization (Stage 1)

    /// Strip HTML tags, decode HTML entities, collapse whitespace.
    static func normalizeText(_ html: String) -> String {
        var text = html

        // Strip HTML tags
        text = text.replacingOccurrences(
            of: "<[^>]+>",
            with: " ",
            options: .regularExpression
        )

        // Decode common HTML entities
        text = decodeHTMLEntities(text)

        // Collapse whitespace and trim
        text = text.replacingOccurrences(
            of: "\\s+",
            with: " ",
            options: .regularExpression
        ).trimmingCharacters(in: .whitespacesAndNewlines)

        return text
    }

    /// Decode common HTML entities to their character equivalents.
    static func decodeHTMLEntities(_ text: String) -> String {
        var result = text
        // NOTE: &amp; is decoded LAST to prevent double-decoding.
        // If &amp; is first, "&amp;lt;" becomes "&lt;" then "<".
        let entities: [(String, String)] = [
            ("&lt;", "<"),
            ("&gt;", ">"),
            ("&quot;", "\""),
            ("&apos;", "'"),
            ("&#39;", "'"),
            ("&nbsp;", " "),
            ("&#x27;", "'"),
            ("&#x2F;", "/"),
            ("&ndash;", "\u{2013}"),
            ("&mdash;", "\u{2014}"),
            ("&lsquo;", "\u{2018}"),
            ("&rsquo;", "\u{2019}"),
            ("&ldquo;", "\u{201C}"),
            ("&rdquo;", "\u{201D}"),
            ("&hellip;", "\u{2026}"),
            ("&amp;", "&"),
        ]
        for (entity, replacement) in entities {
            result = result.replacingOccurrences(of: entity, with: replacement)
        }

        // Decode numeric entities: &#123; or &#x1F;
        // IMPORTANT: Reversed iteration is required for correctness — each
        // replacement can change the string length, invalidating NSRange offsets
        // for matches at earlier positions. Processing from end to start ensures
        // that only already-processed (later) indices are affected.
        if let numericPattern = try? NSRegularExpression(
            pattern: #"&#(\d+);"#,
            options: []
        ) {
            let nsResult = result as NSString
            let matches = numericPattern.matches(
                in: result,
                range: NSRange(location: 0, length: nsResult.length)
            )
            for match in matches.reversed() {
                let codeStr = nsResult.substring(with: match.range(at: 1))
                if let code = UInt32(codeStr),
                   let scalar = Unicode.Scalar(code) {
                    result = (result as NSString).replacingCharacters(
                        in: match.range,
                        with: String(scalar)
                    )
                }
            }
        }

        if let hexPattern = try? NSRegularExpression(
            pattern: #"&#x([0-9a-fA-F]+);"#,
            options: []
        ) {
            let nsResult = result as NSString
            let matches = hexPattern.matches(
                in: result,
                range: NSRange(location: 0, length: nsResult.length)
            )
            for match in matches.reversed() {
                let hexStr = nsResult.substring(with: match.range(at: 1))
                if let code = UInt32(hexStr, radix: 16),
                   let scalar = Unicode.Scalar(code) {
                    result = (result as NSString).replacingCharacters(
                        in: match.range,
                        with: String(scalar)
                    )
                }
            }
        }

        return result
    }

    // MARK: - URL Extraction and Normalization (Stage 2)

    /// URL regex pattern matching common URL formats in podcast descriptions.
    private static let urlPattern: NSRegularExpression = {
        // Match http(s) URLs and bare domain URLs
        let pattern = #"(?:https?://)?(?:www\.)?([a-zA-Z0-9](?:[a-zA-Z0-9\-]*[a-zA-Z0-9])?\.)+(?:com|net|org|io|co|app|fm|tv|me|info|biz|us|uk|ca|de|fr|es|it|nl|au|dev|tech|store|shop|edu|gov|mil|xyz|link|ly|gg)(?:/[^\s<>\"')\]]*)?(?<![.,;:!?)])"#
        return try! NSRegularExpression(pattern: pattern, options: [.caseInsensitive])
    }()

    /// Extract normalized domains (eTLD+1) from URLs found in text.
    static func extractDomains(from text: String) -> [String] {
        let nsText = text as NSString
        let range = NSRange(location: 0, length: nsText.length)
        let matches = urlPattern.matches(in: text, range: range)

        var results: [String] = []
        for match in matches {
            let urlString = nsText.substring(with: match.range)
            if let domain = normalizeDomain(from: urlString) {
                results.append(domain)
            }
        }
        return results
    }

    /// Extract eTLD+1 domain from a URL string, stripping www. prefix.
    static func normalizeDomain(from urlString: String) -> String? {
        // Add scheme if missing so URL parsing works
        var normalized = urlString
        if !normalized.lowercased().hasPrefix("http://") &&
           !normalized.lowercased().hasPrefix("https://") {
            normalized = "https://" + normalized
        }

        // Strip trailing path/query noise
        guard let url = URL(string: normalized),
              var host = url.host?.lowercased() else {
            return nil
        }

        // Strip www. prefix
        if host.hasPrefix("www.") {
            host = String(host.dropFirst(4))
        }

        // Return the domain (eTLD+1 approximation: last two components)
        let components = host.split(separator: ".")
        guard components.count >= 2 else { return nil }

        // For two-part TLDs like .co.uk, take last 3 components
        let twoPartTLDs: Set<String> = ["co.uk", "co.nz", "co.au", "co.jp", "com.au", "com.br"]
        if components.count >= 3 {
            let lastTwo = components.suffix(2).joined(separator: ".")
            if twoPartTLDs.contains(lastTwo) {
                return components.suffix(3).joined(separator: ".")
            }
        }

        return components.suffix(2).joined(separator: ".")
    }

    // NOTE: Tracking-param stripping is handled by DomainNormalizer in
    // SponsorEntityGraph.swift. This extractor returns only eTLD+1 domains,
    // so URL query parameters are irrelevant at this stage.

    // MARK: - Cue Extraction (Stages 3-5)

    /// Extract all cue types from normalized (HTML-stripped) text.
    private func extractFromNormalizedText(
        _ text: String,
        sourceField: MetadataCueSourceField
    ) -> [EpisodeMetadataCue] {
        var cues: [EpisodeMetadataCue] = []

        // Stage 3: Casefold for pattern matching
        let casefolded = text.lowercased()

        // Disclosure cues
        cues.append(contentsOf: extractDisclosures(from: casefolded, sourceField: sourceField))

        // Promo code cues
        cues.append(contentsOf: extractPromoCodes(from: casefolded, sourceField: sourceField))

        // URL-based cues (domain classification)
        cues.append(contentsOf: extractDomainCues(from: text, sourceField: sourceField))

        // Sponsor alias cues (known sponsor name matching)
        cues.append(contentsOf: extractSponsorAliases(from: casefolded, sourceField: sourceField))

        return cues
    }

    // MARK: - Disclosure Detection

    /// Compiled disclosure patterns. Ordered from most specific to least.
    private static let disclosurePatterns: [(NSRegularExpression, Float)] = {
        let patterns: [(String, Float)] = [
            // Strong disclosures (high confidence)
            // Capture groups use [^.,;:!?\n] character class to stop at
            // sentence punctuation, preventing greedy over-capture.
            (#"(?:this\s+(?:episode|podcast|show)\s+is\s+)?sponsored\s+by\s+([^.,;:!?\n]+\w)"#, 0.95),
            (#"brought\s+to\s+you\s+by\s+([^.,;:!?\n]+\w)"#, 0.95),
            (#"(?:a\s+)?(?:word|message)\s+from\s+(?:our\s+)?sponsor[s]?\s*(?::|\s+)([^.,;:!?\n]+\w)"#, 0.90),
            (#"thanks?\s+to\s+(?:our\s+)?sponsors?\s*[,:]\s*([^.,;:!?\n]+\w)"#, 0.90),
            (#"in\s+(?:partnership|collaboration)\s+with\s+([^.,;:!?\n]+\w)"#, 0.85),
            // Weaker disclosures (lower confidence, no captured sponsor name)
            (#"supported\s+by"#, 0.70),
            (#"presented\s+by"#, 0.70),
            (#"powered\s+by"#, 0.60),
            (#"partner(?:ed)?\s+with"#, 0.65),
            (#"\bad\b"#, 0.30),
        ]
        return patterns.compactMap { (pattern, confidence) in
            guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else {
                return nil
            }
            return (regex, confidence)
        }
    }()

    private func extractDisclosures(
        from casefolded: String,
        sourceField: MetadataCueSourceField
    ) -> [EpisodeMetadataCue] {
        let nsText = casefolded as NSString
        let range = NSRange(location: 0, length: nsText.length)
        var cues: [EpisodeMetadataCue] = []

        for (pattern, confidence) in Self.disclosurePatterns {
            let matches = pattern.matches(in: casefolded, range: range)
            for match in matches {
                // Use captured sponsor name if available, otherwise use full match
                let valueRange: NSRange
                if match.numberOfRanges > 1 && match.range(at: 1).location != NSNotFound {
                    valueRange = match.range(at: 1)
                } else {
                    valueRange = match.range
                }
                let value = nsText.substring(with: valueRange)
                    .trimmingCharacters(in: .whitespacesAndNewlines)

                guard !value.isEmpty else { continue }

                cues.append(EpisodeMetadataCue(
                    cueType: .disclosure,
                    normalizedValue: value,
                    sourceField: sourceField,
                    confidence: confidence,
                    canonicalSponsorId: nil,
                    canonicalOwnerId: nil
                ))
            }
        }

        return cues
    }

    // MARK: - Promo Code Detection

    private static let promoCodePatterns: [(NSRegularExpression, Float)] = {
        let patterns: [(String, Float)] = [
            (#"(?:use|enter|apply)\s+(?:the\s+)?(?:promo\s+)?code\s+[\"']?(\w+)[\"']?"#, 0.90),
            (#"promo(?:tional)?\s+code\s*(?::|\s+)[\"']?(\w+)[\"']?"#, 0.90),
            (#"discount\s+code\s*(?::|\s+)[\"']?(\w+)[\"']?"#, 0.85),
            (#"coupon\s+code\s*(?::|\s+)[\"']?(\w+)[\"']?"#, 0.85),
            (#"code\s+[\"']?(\w+)[\"']?\s+(?:at\s+checkout|for|to\s+(?:get|receive|save))"#, 0.85),
            (#"(?:with|using)\s+code\s+[\"']?(\w+)[\"']?"#, 0.80),
        ]
        return patterns.compactMap { (pattern, confidence) in
            guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else {
                return nil
            }
            return (regex, confidence)
        }
    }()

    private func extractPromoCodes(
        from casefolded: String,
        sourceField: MetadataCueSourceField
    ) -> [EpisodeMetadataCue] {
        let nsText = casefolded as NSString
        let range = NSRange(location: 0, length: nsText.length)
        var cues: [EpisodeMetadataCue] = []

        for (pattern, confidence) in Self.promoCodePatterns {
            let matches = pattern.matches(in: casefolded, range: range)
            for match in matches {
                // Extract the captured code value
                guard match.numberOfRanges > 1,
                      match.range(at: 1).location != NSNotFound else { continue }
                let code = nsText.substring(with: match.range(at: 1))
                    .trimmingCharacters(in: .whitespacesAndNewlines)
                    .uppercased() // Promo codes are conventionally uppercase

                guard !code.isEmpty else { continue }

                // Filter out common false positives
                let falsePositives: Set<String> = ["THE", "A", "AN", "YOUR", "OUR", "MY", "AND", "OR", "FOR", "TO"]
                guard !falsePositives.contains(code) else { continue }

                cues.append(EpisodeMetadataCue(
                    cueType: .promoCode,
                    normalizedValue: code,
                    sourceField: sourceField,
                    confidence: confidence,
                    canonicalSponsorId: nil,
                    canonicalOwnerId: nil
                ))
            }
        }

        return cues
    }

    // MARK: - Domain Classification

    private func extractDomainCues(
        from text: String,
        sourceField: MetadataCueSourceField
    ) -> [EpisodeMetadataCue] {
        let domains = Self.extractDomains(from: text)
        var cues: [EpisodeMetadataCue] = []
        var seenDomains: Set<String> = []

        for domain in domains {
            guard !seenDomains.contains(domain) else { continue }
            seenDomains.insert(domain)

            let cueType: MetadataCueType
            let confidence: Float

            if showOwnedDomains.contains(domain) {
                cueType = .showOwnedDomain
                confidence = 0.95
            } else if networkOwnedDomains.contains(domain) {
                cueType = .networkOwnedDomain
                confidence = 0.90
            } else {
                cueType = .externalDomain
                confidence = 0.80
            }

            cues.append(EpisodeMetadataCue(
                cueType: cueType,
                normalizedValue: domain,
                sourceField: sourceField,
                confidence: confidence,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ))
        }

        return cues
    }

    // MARK: - Sponsor Alias Detection

    private func extractSponsorAliases(
        from casefolded: String,
        sourceField: MetadataCueSourceField
    ) -> [EpisodeMetadataCue] {
        guard !knownSponsors.isEmpty else { return [] }

        let nsText = casefolded as NSString
        let range = NSRange(location: 0, length: nsText.length)
        var cues: [EpisodeMetadataCue] = []

        for sponsor in knownSponsors {
            let escaped = NSRegularExpression.escapedPattern(for: sponsor.lowercased())
            guard let regex = try? NSRegularExpression(
                pattern: #"\b"# + escaped + #"\b"#,
                options: [.caseInsensitive]
            ) else { continue }

            // One cue per sponsor per text — multiple mentions don't add signal.
            if regex.firstMatch(in: casefolded, range: range) != nil {
                cues.append(EpisodeMetadataCue(
                    cueType: .sponsorAlias,
                    normalizedValue: sponsor.lowercased(),
                    sourceField: sourceField,
                    confidence: 0.85,
                    canonicalSponsorId: nil,
                    canonicalOwnerId: nil
                ))
            }
        }

        return cues
    }

    // MARK: - Deduplication

    /// Deduplicate cues by (type, normalizedValue, sourceField), keeping highest confidence.
    private func deduplicateCues(_ cues: [EpisodeMetadataCue]) -> [EpisodeMetadataCue] {
        struct CueKey: Hashable {
            let type: MetadataCueType
            let value: String
            let source: MetadataCueSourceField
        }

        var best: [CueKey: EpisodeMetadataCue] = [:]
        for cue in cues {
            let key = CueKey(
                type: cue.cueType,
                value: cue.normalizedValue,
                source: cue.sourceField
            )
            if let existing = best[key] {
                if cue.confidence > existing.confidence {
                    best[key] = cue
                }
            } else {
                best[key] = cue
            }
        }

        // Return sorted for deterministic output
        return best.values.sorted { lhs, rhs in
            if lhs.cueType.rawValue != rhs.cueType.rawValue {
                return lhs.cueType.rawValue < rhs.cueType.rawValue
            }
            return lhs.normalizedValue < rhs.normalizedValue
        }
    }
}
