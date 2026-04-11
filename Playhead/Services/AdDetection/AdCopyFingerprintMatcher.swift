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
/// Phase 9: reads active entries from AdCopyFingerprintStore.
/// Falls back to empty results when no store is provided (backward compat).
enum AdCopyFingerprintMatcher {

    /// Match transcript atoms against known ad copy fingerprints (legacy stub).
    /// Returns empty results when no store is available.
    static func match(
        atoms: [TranscriptAtom]
    ) -> [FingerprintMatch] {
        return []
    }

    /// Match transcript atoms against known ad copy fingerprints using the
    /// fingerprint store. Only active entries are used for matching.
    ///
    /// - Parameters:
    ///   - atoms: Transcript atoms to scan.
    ///   - podcastId: The podcast to look up fingerprints for.
    ///   - fingerprintStore: The ad copy fingerprint store.
    /// - Returns: Fingerprint matches from active fingerprint entries.
    static func match(
        atoms: [TranscriptAtom],
        podcastId: String,
        fingerprintStore: AdCopyFingerprintStore
    ) async throws -> [FingerprintMatch] {
        guard !atoms.isEmpty else { return [] }

        let entries = try await fingerprintStore.activeEntries(forPodcast: podcastId)
        guard !entries.isEmpty else { return [] }

        // Build sliding windows of atoms and compare against stored fingerprints.
        // Use a window of ~30 atoms (roughly a sentence) to build fingerprints
        // for comparison.
        let windowSize = min(30, atoms.count)
        let stride = max(1, windowSize / 3) // Overlap windows for coverage

        var matches: [FingerprintMatch] = []

        var windowStart = 0
        while windowStart < atoms.count {
            let windowEnd = min(windowStart + windowSize, atoms.count)
            let windowAtoms = Array(atoms[windowStart..<windowEnd])

            let windowText = windowAtoms.map(\.text).joined(separator: " ")
            let normalizedText = MinHashUtilities.normalizeText(windowText)
            let ngrams = MinHashUtilities.generateNgrams(normalizedText)
            let windowSignature = MinHashUtilities.computeMinHash(features: ngrams)

            for entry in entries {
                guard let entrySignature = MinHashUtilities.decodeSignature(entry.fingerprintHash) else {
                    continue
                }

                let similarity = MinHashUtilities.jaccardSimilarity(windowSignature, entrySignature)
                if similarity >= MinHashConfig.matchThreshold {
                    let match = FingerprintMatch(
                        firstAtomOrdinal: windowAtoms.first!.atomKey.atomOrdinal,
                        lastAtomOrdinal: windowAtoms.last!.atomKey.atomOrdinal,
                        fingerprintId: entry.id,
                        similarity: similarity,
                        startTime: windowAtoms.first!.startTime,
                        endTime: windowAtoms.last!.endTime
                    )
                    matches.append(match)
                }
            }

            windowStart += stride
        }

        // Merge overlapping matches for the same fingerprint.
        return mergeOverlappingMatches(matches)
    }

    /// Merge overlapping matches for the same fingerprint into contiguous spans.
    private static func mergeOverlappingMatches(_ matches: [FingerprintMatch]) -> [FingerprintMatch] {
        guard !matches.isEmpty else { return [] }

        let sorted = matches.sorted { $0.firstAtomOrdinal < $1.firstAtomOrdinal }
        var merged: [FingerprintMatch] = []

        var current = sorted[0]
        for next in sorted.dropFirst() {
            if next.fingerprintId == current.fingerprintId
                && next.firstAtomOrdinal <= current.lastAtomOrdinal + 2
            {
                current = FingerprintMatch(
                    firstAtomOrdinal: current.firstAtomOrdinal,
                    lastAtomOrdinal: max(current.lastAtomOrdinal, next.lastAtomOrdinal),
                    fingerprintId: current.fingerprintId,
                    similarity: max(current.similarity, next.similarity),
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
