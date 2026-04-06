// AdCopyFingerprintMatcher.swift
// Near-duplicate matching of transcript atoms against confirmed ad scripts.
// Initially returns empty results — populated when Phase 9
// (AdCopyFingerprintStore) lands.

import Foundation

// MARK: - Output types

/// A match of transcript atoms against a confirmed ad script fingerprint.
struct FingerprintMatch: Sendable, Equatable {
    /// First atom ordinal in the matched range.
    let firstAtomOrdinal: Int
    /// Last atom ordinal in the matched range.
    let lastAtomOrdinal: Int
    /// Identifier of the matched fingerprint.
    let fingerprintId: String
    /// Similarity score (0.0...1.0).
    let similarity: Double
    /// Start time in episode seconds.
    let startTime: Double
    /// End time in episode seconds.
    let endTime: Double
}

// MARK: - AdCopyFingerprintMatcher

/// Matches transcript atoms against confirmed ad script fingerprints.
/// Stub implementation — returns empty results until Phase 9.
enum AdCopyFingerprintMatcher {

    /// Match transcript atoms against known ad copy fingerprints.
    /// - Parameters:
    ///   - atoms: Transcript atoms to scan.
    /// - Returns: Fingerprint matches (empty until Phase 9 populates the store).
    static func match(
        atoms: [TranscriptAtom]
    ) -> [FingerprintMatch] {
        // Phase 9 will add an AdCopyFingerprintStore parameter and real matching logic.
        return []
    }
}
