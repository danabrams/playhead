// SponsorKnowledgeMatcher.swift
// Fuzzy-matches known sponsor entities from prior episodes against
// transcript atoms. Initially returns empty results — populated when
// Phase 8 (SponsorKnowledgeStore) lands.

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
/// Stub implementation — returns empty results until Phase 8.
enum SponsorKnowledgeMatcher {

    /// Match known sponsors against transcript atoms.
    /// - Parameters:
    ///   - atoms: Transcript atoms to scan.
    ///   - knowledgeStore: The sponsor knowledge store (placeholder protocol/type for Phase 8).
    /// - Returns: Sponsor matches (empty until Phase 8 populates the store).
    static func match(
        atoms: [TranscriptAtom]
    ) -> [SponsorMatch] {
        // Phase 8 will add a SponsorKnowledgeStore parameter and real matching logic.
        return []
    }
}
