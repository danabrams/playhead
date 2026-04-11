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
    ///   - correctionStore: The user correction store for negative memory.
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

        // Build a lookup set of normalized values + aliases for each entry.
        var matchTargets: [(entry: SponsorKnowledgeEntry, terms: Set<String>)] = []
        for entry in entries {
            var terms: Set<String> = [entry.normalizedValue]
            for alias in entry.aliases {
                terms.insert(alias.lowercased().trimmingCharacters(in: .whitespaces))
            }
            matchTargets.append((entry, terms))
        }

        var matches: [SponsorMatch] = []

        // Sliding window match: for each atom, check if the normalized text
        // contains any known entity term. This is a simple substring match;
        // Phase 8.2 (CompiledSponsorLexicon) will replace this with a proper
        // lexical scanner.
        for atom in atoms {
            let normalizedText = atom.text.lowercased()
            for (entry, terms) in matchTargets {
                for term in terms where !term.isEmpty {
                    if normalizedText.contains(term) {
                        let match = SponsorMatch(
                            firstAtomOrdinal: atom.atomKey.atomOrdinal,
                            lastAtomOrdinal: atom.atomKey.atomOrdinal,
                            entityName: entry.entityValue,
                            confidence: min(1.0, Double(entry.confirmationCount) / 3.0),
                            startTime: atom.startTime,
                            endTime: atom.endTime
                        )
                        matches.append(match)
                        break // One match per atom per entry is sufficient.
                    }
                }
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
