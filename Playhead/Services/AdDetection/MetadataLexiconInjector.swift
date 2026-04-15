// MetadataLexiconInjector.swift
// ef2.4.7: Produces ephemeral lexicon entries from episode metadata cues.
//
// Injects external-domain cues and sponsor aliases into the lexical scanner's
// ephemeral lexicon. Show-owned domains are injected as negative patterns
// (they reduce score when the show's own domain is detected in transcript).
//
// Weight formula: baseCategoryWeight * metadataTrust * 0.75
//
// Key constraint: metadata-injected tokens do NOT satisfy the 2-hit minimum
// alone. They are supplementary evidence, not standalone. The `isMetadataOrigin`
// flag on each entry lets the merge stage enforce this rule.

import Foundation
import OSLog

// MARK: - MetadataLexiconEntry

/// A single ephemeral lexicon entry produced from metadata cues.
/// Carries an `isMetadataOrigin` flag so downstream merge logic can enforce
/// the 2-hit rule: metadata-only hit groups are never promoted to candidates.
struct MetadataLexiconEntry: Sendable, Equatable {
    /// The regex pattern to match against normalized transcript text.
    let pattern: NSRegularExpression
    /// The weight for this entry (baseCategoryWeight * metadataTrust * discount).
    let weight: Double
    /// The lexical category this entry maps to.
    let category: LexicalPatternCategory
    /// True when this entry originated from metadata injection.
    /// Used to enforce the 2-hit rule: groups containing ONLY metadata hits
    /// are not promoted to candidates.
    let isMetadataOrigin: Bool
    /// True when this is a negative pattern (show-owned domain).
    /// Negative patterns reduce the hit group's total weight when matched.
    let isNegativePattern: Bool
    /// The raw value that produced this entry (for diagnostics).
    let sourceValue: String
}

// MARK: - MetadataLexiconInjector

/// Produces ephemeral lexicon entries from metadata cues for injection into
/// the lexical scanner.
///
/// Thread-safe: all state is immutable after init.
struct MetadataLexiconInjector: Sendable {

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "MetadataLexiconInjector"
    )

    private let config: MetadataActivationConfig

    init(config: MetadataActivationConfig = .default) {
        self.config = config
    }

    /// Produce ephemeral lexicon entries from metadata cues.
    ///
    /// - Parameters:
    ///   - cues: Episode metadata cues extracted by MetadataCueExtractor.
    ///   - metadataTrust: Aggregate trust score from the reliability matrix (0...1).
    /// - Returns: Ephemeral lexicon entries, or empty if gating prevents injection.
    func inject(
        cues: [EpisodeMetadataCue],
        metadataTrust: Float
    ) -> [MetadataLexiconEntry] {
        guard config.isLexicalInjectionActive else { return [] }
        guard metadataTrust > 0 else { return [] }
        guard metadataTrust >= config.lexicalInjectionMinTrust else { return [] }
        guard !cues.isEmpty else { return [] }

        var entries: [MetadataLexiconEntry] = []

        for cue in cues {
            switch cue.cueType {
            case .externalDomain:
                if let entry = domainEntry(cue: cue, metadataTrust: metadataTrust, negative: false) {
                    entries.append(entry)
                }
            case .sponsorAlias:
                if let entry = sponsorEntry(cue: cue, metadataTrust: metadataTrust) {
                    entries.append(entry)
                }
            case .showOwnedDomain:
                // Show-owned domains are negative patterns: reduce score when own domain detected.
                if let entry = domainEntry(cue: cue, metadataTrust: metadataTrust, negative: true) {
                    entries.append(entry)
                }
            case .disclosure, .promoCode, .networkOwnedDomain:
                // Disclosures and promo codes are already covered by built-in lexical patterns.
                // Network-owned domains are neither positive nor negative ad evidence.
                break
            }
        }

        Self.logger.debug("Injected \(entries.count) metadata lexicon entries (trust: \(metadataTrust))")
        return entries
    }

    // MARK: - Private

    /// Build a domain-based lexicon entry. Compiles the domain into a
    /// word-boundary regex matching the spoken form (e.g. "betterhelp com"
    /// or "betterhelp.com").
    private func domainEntry(
        cue: EpisodeMetadataCue,
        metadataTrust: Float,
        negative: Bool
    ) -> MetadataLexiconEntry? {
        let domain = cue.normalizedValue
        guard !domain.isEmpty else { return nil }

        // Build patterns for both "domain.com" and "domain com" forms.
        let components = domain.split(separator: ".")
        guard components.count >= 2 else { return nil }

        // Spoken form: "betterhelp com" (ASR strips dots)
        let spokenForm = components.joined(separator: " ")
        let escaped = NSRegularExpression.escapedPattern(for: spokenForm)
        guard let regex = try? NSRegularExpression(
            pattern: #"\b"# + escaped + #"\b"#,
            options: [.caseInsensitive]
        ) else { return nil }

        let baseWeight = LexicalScannerCategoryWeights.weight(for: .urlCTA)
        let weight = baseWeight * Double(metadataTrust) * config.lexicalInjectionDiscount

        return MetadataLexiconEntry(
            pattern: regex,
            weight: negative ? -weight : weight,
            category: .urlCTA,
            isMetadataOrigin: true,
            isNegativePattern: negative,
            sourceValue: domain
        )
    }

    /// Build a sponsor-alias lexicon entry.
    private func sponsorEntry(
        cue: EpisodeMetadataCue,
        metadataTrust: Float
    ) -> MetadataLexiconEntry? {
        let sponsor = cue.normalizedValue
        guard !sponsor.isEmpty else { return nil }

        let escaped = NSRegularExpression.escapedPattern(for: sponsor)
        guard let regex = try? NSRegularExpression(
            pattern: #"\b"# + escaped + #"\b"#,
            options: [.caseInsensitive]
        ) else { return nil }

        let baseWeight = LexicalScannerCategoryWeights.weight(for: .sponsor)
        let weight = baseWeight * Double(metadataTrust) * config.lexicalInjectionDiscount

        return MetadataLexiconEntry(
            pattern: regex,
            weight: weight,
            category: .sponsor,
            isMetadataOrigin: true,
            isNegativePattern: false,
            sourceValue: sponsor
        )
    }
}

// MARK: - LexicalScannerCategoryWeights

/// Exposes category base weights for reuse by MetadataLexiconInjector.
/// Mirrors the weights in LexicalScanner.categoryWeight(_:) without
/// creating a dependency on the scanner's internal state.
enum LexicalScannerCategoryWeights {
    static func weight(for category: LexicalPatternCategory) -> Double {
        switch category {
        case .sponsor:          return 1.0
        case .promoCode:        return 1.2
        case .urlCTA:           return 0.8
        case .purchaseLanguage: return 0.9
        case .transitionMarker: return 0.3
        }
    }
}
