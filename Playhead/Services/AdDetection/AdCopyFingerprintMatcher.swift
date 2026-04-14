// AdCopyFingerprintMatcher.swift
// Near-duplicate matching of transcript atoms against confirmed ad scripts.
// B10: Extended with anchor-aware full-span recovery and hypothesis seeding.

import Foundation

// MARK: - Match strength thresholds

enum FingerprintMatchStrength: Sendable, Equatable {
    /// Jaccard >= 0.8: high confidence — transfer full boundaries after
    /// anchor-landmark alignment + TimeBoundaryResolver validation.
    case strong
    /// Jaccard 0.6–0.8: moderate confidence — seed a hypothesis for
    /// SpanHypothesisEngine verification using matched fragment + landmark priors.
    case normal
}

// MARK: - Anchor alignment result

/// Result of aligning anchor landmarks from a stored fingerprint against
/// anchors found in a new episode occurrence.
struct AnchorAlignmentResult: Sendable, Equatable {
    /// Whether the alignment passed validation.
    let isValid: Bool
    /// Number of landmarks that aligned within tolerance.
    let alignedCount: Int
    /// Total number of landmarks in the stored fingerprint.
    let totalCount: Int
    /// Maximum offset drift observed across aligned landmarks (seconds).
    let maxDriftSeconds: Double
}

// MARK: - Transferred span boundary

/// Full ad span boundaries transferred from a fingerprint match.
struct TransferredSpanBoundary: Sendable, Equatable {
    /// Computed full ad start time in episode seconds.
    let adStartTime: Double
    /// Computed full ad end time in episode seconds.
    let adEndTime: Double
    /// The fingerprint entry that contributed the offsets.
    let sourceEntry: FingerprintEntry
    /// Alignment result from anchor-landmark validation.
    let alignment: AnchorAlignmentResult
    /// Match strength (strong or normal).
    let matchStrength: FingerprintMatchStrength
}

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
    /// Match strength classification based on Jaccard similarity.
    let matchStrength: FingerprintMatchStrength

    init(
        firstAtomOrdinal: Int,
        lastAtomOrdinal: Int,
        fingerprintId: String,
        similarity: Double,
        startTime: Double,
        endTime: Double,
        matchStrength: FingerprintMatchStrength? = nil
    ) {
        self.firstAtomOrdinal = firstAtomOrdinal
        self.lastAtomOrdinal = lastAtomOrdinal
        self.fingerprintId = fingerprintId
        self.similarity = similarity
        self.startTime = startTime
        self.endTime = endTime
        self.matchStrength = matchStrength
            ?? (similarity >= AnchorAlignmentConfig.strongMatchThreshold ? .strong : .normal)
    }
}

// MARK: - Anchor alignment config

enum AnchorAlignmentConfig {
    /// Maximum seconds of drift per landmark before the alignment is invalid.
    static let maxLandmarkDriftSeconds: Double = 10.0
    /// Minimum fraction of landmarks that must align for a strong-match transfer.
    static let minAlignedFraction: Double = 0.5
    /// Jaccard threshold for strong match (full boundary transfer).
    static let strongMatchThreshold: Double = 0.8
}

// MARK: - AdCopyFingerprintMatcher

/// Matches transcript atoms against confirmed ad script fingerprints.
/// Phase 9: reads active entries from AdCopyFingerprintStore.
/// B10: anchor-aware full-span recovery and hypothesis seeding.
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
                if similarity >= MinHashConfig.matchThreshold,
                   let firstAtom = windowAtoms.first,
                   let lastAtom = windowAtoms.last
                {
                    let strength: FingerprintMatchStrength =
                        similarity >= AnchorAlignmentConfig.strongMatchThreshold ? .strong : .normal
                    let match = FingerprintMatch(
                        firstAtomOrdinal: firstAtom.atomKey.atomOrdinal,
                        lastAtomOrdinal: lastAtom.atomKey.atomOrdinal,
                        fingerprintId: entry.id,
                        similarity: similarity,
                        startTime: firstAtom.startTime,
                        endTime: lastAtom.endTime,
                        matchStrength: strength
                    )
                    matches.append(match)
                }
            }

            windowStart += stride
        }

        // Merge overlapping matches for the same fingerprint.
        return mergeOverlappingMatches(matches)
    }

    // MARK: - B10: Anchor-Aware Transfer

    /// Transfer full ad span boundaries from a fingerprint match using stored
    /// span offsets and anchor-landmark alignment.
    ///
    /// For strong matches (Jaccard >= 0.8): transfers full boundaries only
    /// after anchor-landmark alignment passes validation.
    ///
    /// For normal matches (0.6–0.8): returns transferred boundaries with
    /// alignment info so the caller can seed a hypothesis for
    /// SpanHypothesisEngine verification.
    ///
    /// - Parameters:
    ///   - match: The fingerprint match to transfer from.
    ///   - entry: The fingerprint entry containing span offsets and landmarks.
    ///   - episodeAnchors: Anchor events found in the current episode near the match.
    /// - Returns: Transferred span boundary if offsets are available; nil if
    ///   the entry has no span data or alignment fails for strong matches.
    static func transferSpanBoundary(
        match: FingerprintMatch,
        entry: FingerprintEntry,
        episodeAnchors: [AnchorEvent]
    ) -> TransferredSpanBoundary? {
        guard entry.hasSpanOffsets else { return nil }

        let alignment = alignAnchorLandmarks(
            storedLandmarks: entry.anchorLandmarks,
            episodeAnchors: episodeAnchors,
            matchStartTime: match.startTime
        )

        // Strong matches require valid alignment before transferring.
        if match.matchStrength == .strong && !alignment.isValid && !entry.anchorLandmarks.isEmpty {
            return nil
        }

        let adStartTime = match.startTime - entry.spanStartOffset
        let adEndTime = match.endTime + entry.spanEndOffset

        // Sanity: ad end must be after ad start, and duration must be reasonable.
        guard adEndTime > adStartTime else { return nil }
        let transferredDuration = adEndTime - adStartTime
        guard transferredDuration <= entry.spanDurationSeconds * 2.0 else { return nil }

        return TransferredSpanBoundary(
            adStartTime: adStartTime,
            adEndTime: adEndTime,
            sourceEntry: entry,
            alignment: alignment,
            matchStrength: match.matchStrength
        )
    }

    // MARK: - B10: Anchor Landmark Alignment

    /// Align stored anchor landmarks against anchors found in the current
    /// episode. Handles host ad-lib variation by allowing drift up to
    /// `AnchorAlignmentConfig.maxLandmarkDriftSeconds`.
    ///
    /// - Parameters:
    ///   - storedLandmarks: Anchor landmarks from the stored fingerprint entry.
    ///   - episodeAnchors: Anchor events found near the match in the current episode.
    ///   - matchStartTime: Start time of the fingerprint match in episode seconds.
    /// - Returns: Alignment result with validity, aligned count, and max drift.
    static func alignAnchorLandmarks(
        storedLandmarks: [AnchorLandmark],
        episodeAnchors: [AnchorEvent],
        matchStartTime: Double
    ) -> AnchorAlignmentResult {
        guard !storedLandmarks.isEmpty else {
            // No landmarks to check — treat as trivially valid.
            return AnchorAlignmentResult(
                isValid: true,
                alignedCount: 0,
                totalCount: 0,
                maxDriftSeconds: 0
            )
        }

        var alignedCount = 0
        var maxDrift: Double = 0

        for landmark in storedLandmarks {
            let expectedTime = matchStartTime + landmark.offsetSeconds

            // Find the closest episode anchor of the same type.
            let bestDrift = episodeAnchors
                .filter { $0.anchorType == landmark.type }
                .map { abs($0.startTime - expectedTime) }
                .min()

            if let drift = bestDrift, drift <= AnchorAlignmentConfig.maxLandmarkDriftSeconds {
                alignedCount += 1
                maxDrift = max(maxDrift, drift)
            }
        }

        let alignedFraction = Double(alignedCount) / Double(storedLandmarks.count)
        let isValid = alignedFraction >= AnchorAlignmentConfig.minAlignedFraction

        return AnchorAlignmentResult(
            isValid: isValid,
            alignedCount: alignedCount,
            totalCount: storedLandmarks.count,
            maxDriftSeconds: maxDrift
        )
    }

    // MARK: - Merge

    /// Merge overlapping matches for the same fingerprint into contiguous spans.
    /// Groups by fingerprintId first to avoid interleaving across different
    /// fingerprints breaking the merge chain.
    private static func mergeOverlappingMatches(_ matches: [FingerprintMatch]) -> [FingerprintMatch] {
        guard !matches.isEmpty else { return [] }

        // Group by fingerprint to merge each independently.
        var grouped: [String: [FingerprintMatch]] = [:]
        for match in matches {
            grouped[match.fingerprintId, default: []].append(match)
        }

        var merged: [FingerprintMatch] = []
        for (_, group) in grouped {
            let sorted = group.sorted { $0.firstAtomOrdinal < $1.firstAtomOrdinal }
            var current = sorted[0]
            for next in sorted.dropFirst() {
                if next.firstAtomOrdinal <= current.lastAtomOrdinal + 2 {
                    // Merge: take the strongest match strength.
                    let mergedStrength: FingerprintMatchStrength =
                        (current.matchStrength == .strong || next.matchStrength == .strong)
                        ? .strong : .normal
                    current = FingerprintMatch(
                        firstAtomOrdinal: current.firstAtomOrdinal,
                        lastAtomOrdinal: max(current.lastAtomOrdinal, next.lastAtomOrdinal),
                        fingerprintId: current.fingerprintId,
                        similarity: max(current.similarity, next.similarity),
                        startTime: min(current.startTime, next.startTime),
                        endTime: max(current.endTime, next.endTime),
                        matchStrength: mergedStrength
                    )
                } else {
                    merged.append(current)
                    current = next
                }
            }
            merged.append(current)
        }
        // Sort by ordinal (then fingerprintId for tie-breaking) for deterministic
        // output regardless of dictionary iteration order.
        return merged.sorted {
            if $0.firstAtomOrdinal != $1.firstAtomOrdinal {
                return $0.firstAtomOrdinal < $1.firstAtomOrdinal
            }
            return $0.fingerprintId < $1.fingerprintId
        }
    }
}
