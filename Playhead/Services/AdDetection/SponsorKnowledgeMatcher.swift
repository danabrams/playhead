// SponsorKnowledgeMatcher.swift
// Fuzzy-matches known sponsor entities from prior episodes against
// transcript atoms. Phase 8 (playhead-4my.8.1): reads from
// SponsorKnowledgeStore; returns matches for active entries only.

import Foundation

// MARK: - Output types

/// A match of a known sponsor entity against transcript atoms.
struct SponsorMatch: Sendable, Equatable {
    /// First atom ordinal in the matched range.
    let firstAtomOrdinal: Int
    /// Last atom ordinal in the matched range.
    let lastAtomOrdinal: Int
    /// The matched sponsor entity name.
    let entityName: String
    /// Match confidence (0.0...1.0).
    let confidence: Double
    /// Start time in episode seconds.
    let startTime: Double
    /// End time in episode seconds.
    let endTime: Double
}

// MARK: - SponsorKnowledgeMatcher

/// Matches known sponsor entities against transcript atoms.
/// Phase 8: reads active entries from SponsorKnowledgeStore.
/// Falls back to empty results when no store is provided (backward compat).
enum SponsorKnowledgeMatcher {

    /// Match known sponsors against transcript atoms (legacy stub).
    /// Returns empty results when no store is available.
    static func match(
        atoms: [TranscriptAtom]
    ) -> [SponsorMatch] {
        return []
    }

    /// Match known sponsors against transcript atoms using the knowledge store.
    /// Only active entries (with negative memory applied) are used for matching.
    ///
    /// - Parameters:
    ///   - atoms: Transcript atoms to scan.
    ///   - podcastId: The podcast to look up knowledge for.
    ///   - knowledgeStore: The sponsor knowledge store.
    /// - Returns: Sponsor matches from active knowledge entries.
    static func match(
        atoms: [TranscriptAtom],
        podcastId: String,
        knowledgeStore: SponsorKnowledgeStore
    ) async throws -> [SponsorMatch] {
        guard !atoms.isEmpty else { return [] }

        let entries = try await knowledgeStore.activeEntriesWithNegativeMemory(
            forPodcast: podcastId
        )
        guard !entries.isEmpty else { return [] }

        // Build word-boundary regex patterns for each entry's normalized
        // value and aliases, matching CompiledSponsorLexicon's semantics.
        var matchTargets: [(entry: SponsorKnowledgeEntry, patterns: [NSRegularExpression])] = []
        for entry in entries {
            var terms: Set<String> = [entry.normalizedValue]
            for alias in entry.aliases {
                let normalized = alias.lowercased().trimmingCharacters(in: .whitespaces)
                if !normalized.isEmpty {
                    terms.insert(normalized)
                }
            }
            var patterns: [NSRegularExpression] = []
            for term in terms {
                let escaped = NSRegularExpression.escapedPattern(for: term)
                if let regex = try? NSRegularExpression(
                    pattern: #"\b"# + escaped + #"\b"#,
                    options: [.caseInsensitive]
                ) {
                    patterns.append(regex)
                }
            }
            matchTargets.append((entry, patterns))
        }

        var matches: [SponsorMatch] = []

        // Word-boundary match: for each atom, check if any known entity
        // term appears as a whole word. Uses \b...\b regex matching,
        // consistent with CompiledSponsorLexicon's approach.
        for atom in atoms {
            let normalizedText = atom.text.lowercased()
            let nsText = normalizedText as NSString
            let range = NSRange(location: 0, length: nsText.length)
            for (entry, patterns) in matchTargets {
                var matched = false
                for pattern in patterns {
                    if pattern.firstMatch(in: normalizedText, range: range) != nil {
                        let match = SponsorMatch(
                            firstAtomOrdinal: atom.atomKey.atomOrdinal,
                            lastAtomOrdinal: atom.atomKey.atomOrdinal,
                            entityName: entry.entityValue,
                            // Graduated confidence: active entries have >=2
                            // confirmations (min 0.667), reaching 1.0 at 3+.
                            confidence: min(1.0, Double(entry.confirmationCount) / 3.0),
                            startTime: atom.startTime,
                            endTime: atom.endTime
                        )
                        matches.append(match)
                        matched = true
                        break // One match per atom per entry is sufficient.
                    }
                }
                if matched { break }
            }
        }

        // Merge adjacent matches for the same entity into spans.
        return mergeAdjacentMatches(matches)
    }

    /// Merge adjacent matches for the same entity into contiguous spans.
    private static func mergeAdjacentMatches(_ matches: [SponsorMatch]) -> [SponsorMatch] {
        guard !matches.isEmpty else { return [] }

        let sorted = matches.sorted { $0.firstAtomOrdinal < $1.firstAtomOrdinal }
        var merged: [SponsorMatch] = []

        var current = sorted[0]
        for next in sorted.dropFirst() {
            // Gap of 1 atom allowed: e.g. "Squarespace" at atom 5, filler at 6,
            // "Squarespace dot com" at atom 7 should merge into one span.
            if next.entityName == current.entityName
                && next.firstAtomOrdinal <= current.lastAtomOrdinal + 2
            {
                // Extend the current span.
                current = SponsorMatch(
                    firstAtomOrdinal: current.firstAtomOrdinal,
                    lastAtomOrdinal: max(current.lastAtomOrdinal, next.lastAtomOrdinal),
                    entityName: current.entityName,
                    confidence: max(current.confidence, next.confidence),
                    startTime: min(current.startTime, next.startTime),
                    endTime: max(current.endTime, next.endTime)
                )
            } else {
                merged.append(current)
                current = next
            }
        }
        merged.append(current)
        return merged
    }
}
