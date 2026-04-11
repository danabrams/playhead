// CompiledSponsorLexicon.swift
// Phase 8 (playhead-4my.8.2): Fast string matcher compiled from active
// SponsorKnowledgeStore entries. Integrated into LexicalScanner as a
// second sponsor-term source alongside PodcastProfile.sponsorLexicon.
//
// Recompiled when the knowledge store changes. Only active entries
// contribute patterns — blocked, decayed, candidate, and quarantined
// entries are excluded.

import Foundation

// MARK: - CompiledSponsorLexicon

/// A pre-compiled set of regex patterns derived from active sponsor
/// knowledge entries. Each entry's `normalizedValue` and `aliases` are
/// compiled into word-boundary regexes, matching the same pattern style
/// used by `LexicalScanner.compileSponsorLexicon(from:)`.
///
/// Thread-safe: all state is immutable after init.
struct CompiledSponsorLexicon: Sendable {

    /// Compiled word-boundary patterns for active sponsor entities.
    let patterns: [NSRegularExpression]

    /// Number of active entries that contributed patterns.
    let entryCount: Int

    /// Build a compiled lexicon from a set of knowledge entries.
    /// Only entries with `.active` state are included. Blocked, decayed,
    /// candidate, and quarantined entries are excluded.
    ///
    /// Each active entry contributes its `normalizedValue` plus all
    /// `aliases`, each compiled as `\b<escaped-term>\b` with
    /// case-insensitive matching.
    ///
    /// - Parameter entries: All knowledge entries for a podcast (any state).
    /// - Returns: A compiled lexicon containing patterns for active entries only.
    init(entries: [SponsorKnowledgeEntry]) {
        let activeEntries = entries.filter { $0.state == .active }
        self.entryCount = activeEntries.count

        var compiled: [NSRegularExpression] = []
        for entry in activeEntries {
            // Collect all terms: normalizedValue + aliases.
            var terms = [entry.normalizedValue]
            for alias in entry.aliases {
                let normalized = alias.lowercased().trimmingCharacters(in: .whitespaces)
                if !normalized.isEmpty && !terms.contains(normalized) {
                    terms.append(normalized)
                }
            }

            for term in terms {
                let escaped = NSRegularExpression.escapedPattern(for: term)
                if let regex = try? NSRegularExpression(
                    pattern: #"\b"# + escaped + #"\b"#,
                    options: [.caseInsensitive]
                ) {
                    compiled.append(regex)
                }
            }
        }

        self.patterns = compiled
    }

    /// Empty lexicon — no patterns, no entries.
    static let empty = CompiledSponsorLexicon(entries: [])
}
