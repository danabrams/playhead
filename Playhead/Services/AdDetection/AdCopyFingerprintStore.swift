// AdCopyFingerprintStore.swift
// Phase 9 (playhead-4my.9.1): MinHash fingerprint store for confirmed ad
// scripts with lifecycle management. Catches recurring ad copy across
// episodes by near-duplicate matching on character 4-gram MinHash
// fingerprints.
//
// Design:
//   - FingerprintEntry: per-podcast fingerprint with lifecycle state
//   - FingerprintSourceEvent: append-only provenance log
//   - FingerprintPromotionThresholds: lifecycle transition constants
//   - AdCopyFingerprintStore (struct): lifecycle management + query APIs
//   - Reuses KnowledgeState enum from SponsorKnowledgeStore
//   - Only active entries are surfaced to matcher queries

import Foundation
import OSLog

// MARK: - AnchorLandmark

/// A typed anchor position within a fingerprinted ad span. Used for
/// anchor-aware local alignment when transferring full boundaries from
/// a fingerprint match to a new episode occurrence.
struct AnchorLandmark: Sendable, Equatable, Codable {
    /// The type of anchor (disclosure, url, promoCode, etc.)
    let type: AnchorType
    /// Seconds from fingerprint match start to this landmark.
    let offsetSeconds: Double
    /// Normalized text of the landmark (e.g., "betterhelp dot com").
    let normalizedText: String?
}

// MARK: - FingerprintEntry

/// A MinHash fingerprint entry with lifecycle state and per-entry stats.
struct FingerprintEntry: Sendable, Equatable {
    let id: String
    let podcastId: String
    let fingerprintHash: String
    let normalizedText: String
    let state: KnowledgeState
    let confirmationCount: Int
    let rollbackCount: Int
    let firstSeenAt: Double
    let lastConfirmedAt: Double?
    let lastRollbackAt: Double?
    let decayedAt: Double?
    let blockedAt: Double?
    let metadata: [String: String]?

    // B10: Span offset fields for full-span recovery.
    /// Seconds from fingerprint match start to full ad start (typically positive or zero).
    /// adStart = matchStart - spanStartOffset, so a positive value means the ad starts
    /// before the match point.
    let spanStartOffset: Double
    /// Seconds from fingerprint match end to full ad end (typically positive or zero).
    let spanEndOffset: Double
    /// Total ad duration in seconds when the fingerprint was created.
    let spanDurationSeconds: Double
    /// Canonical sponsor identity for entity-aware matching.
    let canonicalSponsorEntity: NormalizedSponsor?
    /// Typed anchor positions within the ad for alignment validation.
    let anchorLandmarks: [AnchorLandmark]

    init(
        id: String = UUID().uuidString,
        podcastId: String,
        fingerprintHash: String,
        normalizedText: String,
        state: KnowledgeState = .candidate,
        confirmationCount: Int = 0,
        rollbackCount: Int = 0,
        firstSeenAt: Double = Date().timeIntervalSince1970,
        lastConfirmedAt: Double? = nil,
        lastRollbackAt: Double? = nil,
        decayedAt: Double? = nil,
        blockedAt: Double? = nil,
        metadata: [String: String]? = nil,
        spanStartOffset: Double = 0,
        spanEndOffset: Double = 0,
        spanDurationSeconds: Double = 0,
        canonicalSponsorEntity: NormalizedSponsor? = nil,
        anchorLandmarks: [AnchorLandmark] = []
    ) {
        self.id = id
        self.podcastId = podcastId
        self.fingerprintHash = fingerprintHash
        self.normalizedText = normalizedText
        self.state = state
        self.confirmationCount = confirmationCount
        self.rollbackCount = rollbackCount
        self.firstSeenAt = firstSeenAt
        self.lastConfirmedAt = lastConfirmedAt
        self.lastRollbackAt = lastRollbackAt
        self.decayedAt = decayedAt
        self.blockedAt = blockedAt
        self.metadata = metadata
        self.spanStartOffset = spanStartOffset
        self.spanEndOffset = spanEndOffset
        self.spanDurationSeconds = spanDurationSeconds
        self.canonicalSponsorEntity = canonicalSponsorEntity
        self.anchorLandmarks = anchorLandmarks
    }

    /// Rollback rate as a fraction of total observations (confirmations + rollbacks).
    var rollbackRate: Double {
        let total = confirmationCount + rollbackCount
        guard total > 0 else { return 0.0 }
        return Double(rollbackCount) / Double(total)
    }

    /// Whether this entry has span offsets populated (non-zero duration).
    var hasSpanOffsets: Bool {
        spanDurationSeconds > 0
    }
}

// MARK: - FingerprintSourceEvent

/// Append-only event tracking fingerprint observations with provenance.
struct FingerprintSourceEvent: Sendable, Equatable {
    let id: String
    let analysisAssetId: String
    let fingerprintHash: String
    let sourceAdWindowId: String
    let confidence: Double
    let createdAt: Double

    init(
        id: String = UUID().uuidString,
        analysisAssetId: String,
        fingerprintHash: String,
        sourceAdWindowId: String,
        confidence: Double,
        createdAt: Double = Date().timeIntervalSince1970
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.fingerprintHash = fingerprintHash
        self.sourceAdWindowId = sourceAdWindowId
        self.confidence = confidence
        self.createdAt = createdAt
    }
}

// MARK: - Promotion Thresholds

/// Constants governing fingerprint lifecycle transitions.
/// Separate from KnowledgePromotionThresholds to allow independent tuning.
enum FingerprintPromotionThresholds {
    /// Minimum confirmations to promote from quarantined -> active.
    static let minConfirmationsForActive = 2
    /// Maximum rollback rate to allow quarantined -> active promotion.
    static let maxRollbackRateForActive = 0.3
    /// Rollback rate that triggers active -> decayed demotion.
    static let rollbackSpikeThreshold = 0.5
    /// Minimum confidence for initial candidate extraction.
    static let minCandidateConfidence = 0.5
    /// Minimum BoundaryExpander confidence for user-marked ad fingerprint seeding.
    static let minUserMarkedConfidence = 0.7
}

// MARK: - MinHash Constants

/// Constants for MinHash fingerprint generation.
enum MinHashConfig {
    /// Number of hash functions used for MinHash signature.
    static let hashCount = 128
    /// Size of character n-grams for feature extraction.
    static let ngramSize = 4
    /// Filler words removed during text normalization (single-word only;
    /// multi-word phrases can't be matched by the per-word filter).
    static let fillerWords: Set<String> = [
        "um", "uh", "like", "so", "well",
        "basically", "actually", "literally", "right", "okay"
    ]
    /// Minimum Jaccard similarity for a near-duplicate match.
    static let matchThreshold: Double = 0.6
}

// MARK: - MinHash Utilities

/// Pure utility functions for MinHash fingerprint computation.
enum MinHashUtilities {

    /// Normalize transcript text for fingerprinting: lowercase, strip
    /// punctuation, remove filler words, collapse whitespace.
    static func normalizeText(_ text: String) -> String {
        var result = text.lowercased()
        // Strip punctuation (keep alphanumeric and spaces).
        result = result.unicodeScalars.filter {
            CharacterSet.alphanumerics.contains($0) || CharacterSet.whitespaces.contains($0)
        }.map { String($0) }.joined()
        // Remove filler words.
        let words = result.split(separator: " ").filter { word in
            !MinHashConfig.fillerWords.contains(String(word))
        }
        result = words.joined(separator: " ")
        // Collapse whitespace.
        result = result.components(separatedBy: .whitespaces)
            .filter { !$0.isEmpty }
            .joined(separator: " ")
        return result
    }

    /// Generate character 4-gram features from normalized text.
    static func generateNgrams(_ text: String) -> Set<String> {
        guard text.count >= MinHashConfig.ngramSize else {
            return text.isEmpty ? [] : [text]
        }
        var ngrams = Set<String>()
        let chars = Array(text)
        for i in 0...(chars.count - MinHashConfig.ngramSize) {
            let ngram = String(chars[i..<(i + MinHashConfig.ngramSize)])
            ngrams.insert(ngram)
        }
        return ngrams
    }

    /// Compute MinHash signature from a set of n-gram features.
    /// Uses 128 hash functions for a compact fingerprint.
    static func computeMinHash(features: Set<String>) -> [UInt64] {
        guard !features.isEmpty else {
            return Array(repeating: UInt64.max, count: MinHashConfig.hashCount)
        }

        // Use different (a, b) pairs for each hash function.
        // FNV-1a base hash with linear perturbation: h_i(x) = (fnv1a(x) + a_i) ^ b_i
        var signature = Array(repeating: UInt64.max, count: MinHashConfig.hashCount)
        for feature in features {
            let baseHash = fnv1a(feature)
            for i in 0..<MinHashConfig.hashCount {
                let a = UInt64(i) &* 0x517cc1b727220a95
                let b = UInt64(i) &* 0x6c62272e07bb0142
                let h = (baseHash &+ a) ^ b
                if h < signature[i] {
                    signature[i] = h
                }
            }
        }
        return signature
    }

    /// Compute Jaccard similarity between two MinHash signatures.
    static func jaccardSimilarity(_ a: [UInt64], _ b: [UInt64]) -> Double {
        guard a.count == b.count, !a.isEmpty else { return 0.0 }
        var matching = 0
        for i in 0..<a.count {
            if a[i] == b[i] {
                matching += 1
            }
        }
        return Double(matching) / Double(a.count)
    }

    /// Encode a MinHash signature as a hex string for storage.
    static func encodeSignature(_ signature: [UInt64]) -> String {
        signature.map { String(format: "%016llx", $0) }.joined()
    }

    /// Decode a hex-encoded MinHash signature.
    static func decodeSignature(_ hex: String) -> [UInt64]? {
        let chunkSize = 16
        guard hex.count == chunkSize * MinHashConfig.hashCount else { return nil }
        var result: [UInt64] = []
        result.reserveCapacity(MinHashConfig.hashCount)
        var index = hex.startIndex
        for _ in 0..<MinHashConfig.hashCount {
            let end = hex.index(index, offsetBy: chunkSize)
            guard let value = UInt64(hex[index..<end], radix: 16) else { return nil }
            result.append(value)
            index = end
        }
        return result
    }

    /// Generate a fingerprint hash from transcript text.
    /// Returns the hex-encoded MinHash signature.
    static func generateFingerprint(from text: String) -> String {
        let normalized = normalizeText(text)
        let ngrams = generateNgrams(normalized)
        let signature = computeMinHash(features: ngrams)
        return encodeSignature(signature)
    }

    // MARK: - FNV-1a Hash

    private static func fnv1a(_ string: String) -> UInt64 {
        var hash: UInt64 = 0xcbf29ce484222325
        for byte in string.utf8 {
            hash ^= UInt64(byte)
            hash = hash &* 0x100000001b3
        }
        return hash
    }
}

// MARK: - AdCopyFingerprintStore

/// Fingerprint store with quarantine lifecycle. Delegates all SQLite
/// persistence to AnalysisStore (which is itself an actor providing
/// serialized DB access). This type holds no mutable state — lifecycle
/// promotion/demotion logic is pure computation on values.
struct AdCopyFingerprintStore: Sendable {

    private let store: AnalysisStore
    private let logger = Logger(subsystem: "com.playhead", category: "AdCopyFingerprintStore")

    init(store: AnalysisStore) {
        self.store = store
    }

    // MARK: - Write: Record Candidate

    /// Record a candidate fingerprint from a confirmed ad span. Creates a new
    /// entry if one doesn't exist for this (podcastId, fingerprintHash), or
    /// increments the confirmation count on the existing one.
    /// Also appends a FingerprintSourceEvent for provenance.
    ///
    /// The load → promote → upsert cycle runs inside a single
    /// `AnalysisStore.atomicConfirmFingerprint` call, eliminating any
    /// TOCTOU race between concurrent callers.
    func recordCandidate(
        podcastId: String,
        text: String,
        analysisAssetId: String,
        sourceAdWindowId: String,
        confidence: Double
    ) async throws {
        guard confidence >= FingerprintPromotionThresholds.minCandidateConfidence else {
            logger.debug("recordCandidate: skipping low-confidence fingerprint (conf=\(confidence))")
            return
        }

        let normalizedText = MinHashUtilities.normalizeText(text)
        guard !normalizedText.isEmpty else {
            logger.debug("recordCandidate: skipping empty normalized text")
            return
        }

        let fingerprintHash = MinHashUtilities.generateFingerprint(from: text)

        // Pre-decode the new signature for near-duplicate comparison.
        let decodedNew = MinHashUtilities.decodeSignature(fingerprintHash)
        if decodedNew == nil {
            logger.warning("recordCandidate: self-generated hash failed decode — skipping near-duplicate check")
        }

        // Atomic load → promote → upsert inside the AnalysisStore actor.
        let (resolvedHash, _) = try await store.atomicConfirmFingerprint(
            podcastId: podcastId,
            fingerprintHash: fingerprintHash,
            normalizedText: normalizedText,
            promote: { current, confirmations, rollbacks in
                stablePromoteState(current: current, confirmationCount: confirmations, rollbackCount: rollbacks)
            },
            nearDuplicateCheck: { newHash, existingHash in
                guard let decodedNew else { return false }
                guard let decodedExisting = MinHashUtilities.decodeSignature(existingHash) else { return false }
                return MinHashUtilities.jaccardSimilarity(decodedNew, decodedExisting) >= MinHashConfig.matchThreshold
            }
        )

        // Append the provenance event with the resolved hash so it
        // correlates to the actual stored entry (not the raw input hash
        // which may differ for near-duplicate matches).
        let event = FingerprintSourceEvent(
            analysisAssetId: analysisAssetId,
            fingerprintHash: resolvedHash,
            sourceAdWindowId: sourceAdWindowId,
            confidence: confidence
        )
        try await store.appendFingerprintSourceEvent(event)
    }

    // MARK: - Write: Seed from User-Marked Ad (B10)

    /// Seed a fingerprint entry from a user-marked ad when BoundaryExpander
    /// produces a span with confidence >= 0.7. The entry starts at `candidate`
    /// state and goes through the normal trust lifecycle.
    ///
    /// - Parameters:
    ///   - podcastId: The podcast identifier.
    ///   - text: Transcript text within the expanded boundaries.
    ///   - analysisAssetId: The episode asset identifier.
    ///   - sourceAdWindowId: The ad window identifier (may be user-generated).
    ///   - boundary: The expanded boundary from BoundaryExpander.
    ///   - matchStartTime: Start time of the fingerprinted fragment within the ad.
    ///   - matchEndTime: End time of the fingerprinted fragment within the ad.
    ///   - sponsorEntity: Optional sponsor identity extracted from the ad text.
    ///   - anchorLandmarks: Typed anchor positions found within the ad span.
    // Note: The load-then-upsert pattern below has a theoretical TOCTOU window,
    // but it's acceptable because: (a) AnalysisStore actor serializes calls,
    // (b) user-marked seeding is a rare manual action, and (c) the worst case
    // is a benign offset overwrite.
    func seedFromUserMarkedAd(
        podcastId: String,
        text: String,
        analysisAssetId: String,
        sourceAdWindowId: String,
        boundary: ExpandedBoundary,
        matchStartTime: Double,
        matchEndTime: Double,
        sponsorEntity: NormalizedSponsor? = nil,
        anchorLandmarks: [AnchorLandmark] = []
    ) async throws {
        guard boundary.boundaryConfidence >= FingerprintPromotionThresholds.minUserMarkedConfidence else {
            logger.debug("seedFromUserMarkedAd: boundary confidence \(boundary.boundaryConfidence) below threshold")
            return
        }

        let normalizedText = MinHashUtilities.normalizeText(text)
        guard !normalizedText.isEmpty else {
            logger.debug("seedFromUserMarkedAd: skipping empty normalized text")
            return
        }

        let fingerprintHash = MinHashUtilities.generateFingerprint(from: text)
        let spanStartOffset = matchStartTime - boundary.startTime
        let spanEndOffset = boundary.endTime - matchEndTime
        let spanDuration = boundary.endTime - boundary.startTime

        // Check for existing entry to avoid clobbering its lifecycle state.
        // If an entry already exists, update only span offsets; preserve
        // state, confirmationCount, rollbackCount, and timestamps.
        if let existing = try await store.loadFingerprintEntry(
            podcastId: podcastId,
            fingerprintHash: fingerprintHash
        ) {
            let updated = FingerprintEntry(
                id: existing.id,
                podcastId: existing.podcastId,
                fingerprintHash: existing.fingerprintHash,
                normalizedText: existing.normalizedText,
                state: existing.state,
                confirmationCount: existing.confirmationCount,
                rollbackCount: existing.rollbackCount,
                firstSeenAt: existing.firstSeenAt,
                lastConfirmedAt: existing.lastConfirmedAt,
                lastRollbackAt: existing.lastRollbackAt,
                decayedAt: existing.decayedAt,
                blockedAt: existing.blockedAt,
                metadata: existing.metadata,
                spanStartOffset: spanStartOffset,
                spanEndOffset: spanEndOffset,
                spanDurationSeconds: spanDuration,
                canonicalSponsorEntity: sponsorEntity ?? existing.canonicalSponsorEntity,
                anchorLandmarks: anchorLandmarks.isEmpty ? existing.anchorLandmarks : anchorLandmarks
            )
            try await store.upsertFingerprintEntry(updated)
        } else {
            let entry = FingerprintEntry(
                podcastId: podcastId,
                fingerprintHash: fingerprintHash,
                normalizedText: normalizedText,
                state: .candidate,
                confirmationCount: 0,
                firstSeenAt: Date().timeIntervalSince1970,
                spanStartOffset: spanStartOffset,
                spanEndOffset: spanEndOffset,
                spanDurationSeconds: spanDuration,
                canonicalSponsorEntity: sponsorEntity,
                anchorLandmarks: anchorLandmarks
            )
            try await store.upsertFingerprintEntry(entry)
        }

        let event = FingerprintSourceEvent(
            analysisAssetId: analysisAssetId,
            fingerprintHash: fingerprintHash,
            sourceAdWindowId: sourceAdWindowId,
            confidence: boundary.boundaryConfidence
        )
        try await store.appendFingerprintSourceEvent(event)
    }

    // MARK: - Write: Record Rollback

    /// Record a rollback against a fingerprint entry.
    /// Increments rollback count and may demote the entry.
    /// Uses atomic load → demote → upsert to prevent TOCTOU races.
    func recordRollback(
        podcastId: String,
        fingerprintHash: String
    ) async throws {
        try await store.atomicRollbackFingerprint(
            podcastId: podcastId,
            fingerprintHash: fingerprintHash,
            demote: { current, confirmations, rollbacks in
                demoteState(current: current, confirmationCount: confirmations, rollbackCount: rollbacks)
            }
        )
    }

    // MARK: - Query: Active Entries for Matcher

    /// Returns only active entries for a podcast — the set that
    /// AdCopyFingerprintMatcher should use for matching.
    func activeEntries(forPodcast podcastId: String) async throws -> [FingerprintEntry] {
        try await store.loadFingerprintEntries(podcastId: podcastId, state: .active)
    }

    /// Returns all entries for a podcast regardless of state (for diagnostics).
    func allEntries(forPodcast podcastId: String) async throws -> [FingerprintEntry] {
        try await store.loadAllFingerprintEntries(podcastId: podcastId)
    }

    /// Returns a single entry by its natural key.
    func entry(
        podcastId: String,
        fingerprintHash: String
    ) async throws -> FingerprintEntry? {
        try await store.loadFingerprintEntry(
            podcastId: podcastId,
            fingerprintHash: fingerprintHash
        )
    }

    /// Returns source events for a given analysis asset.
    func sourceEvents(forAsset analysisAssetId: String) async throws -> [FingerprintSourceEvent] {
        try await store.loadFingerprintSourceEvents(analysisAssetId: analysisAssetId)
    }

    // MARK: - Promotion Logic

    /// Apply promotion transitions iteratively until the state stabilizes.
    func stablePromoteState(
        current: KnowledgeState,
        confirmationCount: Int,
        rollbackCount: Int
    ) -> KnowledgeState {
        var state = current
        for _ in 0..<5 {
            let next = promoteState(
                current: state,
                confirmationCount: confirmationCount,
                rollbackCount: rollbackCount
            )
            if next == state { break }
            state = next
        }
        return state
    }

    /// Compute the single next state transition when a confirmation is recorded.
    func promoteState(
        current: KnowledgeState,
        confirmationCount: Int,
        rollbackCount: Int
    ) -> KnowledgeState {
        let rollbackRate = (confirmationCount + rollbackCount) > 0
            ? Double(rollbackCount) / Double(confirmationCount + rollbackCount)
            : 0.0

        switch current {
        case .candidate:
            return confirmationCount >= 1 ? .quarantined : .candidate
        case .quarantined:
            if confirmationCount >= FingerprintPromotionThresholds.minConfirmationsForActive
                && rollbackRate <= FingerprintPromotionThresholds.maxRollbackRateForActive
            {
                return .active
            }
            return .quarantined
        case .active:
            if rollbackRate > FingerprintPromotionThresholds.rollbackSpikeThreshold {
                return .decayed
            }
            return .active
        case .decayed:
            if confirmationCount >= FingerprintPromotionThresholds.minConfirmationsForActive
                && rollbackRate <= FingerprintPromotionThresholds.maxRollbackRateForActive
            {
                return .active
            }
            return .decayed
        case .blocked:
            return .blocked
        }
    }

    // MARK: - Demotion Logic

    /// Compute the next state when a rollback is recorded.
    func demoteState(
        current: KnowledgeState,
        confirmationCount: Int,
        rollbackCount: Int
    ) -> KnowledgeState {
        let total = confirmationCount + rollbackCount
        let rollbackRate = total > 0 ? Double(rollbackCount) / Double(total) : 0.0

        switch current {
        case .candidate, .quarantined:
            if rollbackRate > FingerprintPromotionThresholds.rollbackSpikeThreshold {
                return .blocked
            }
            return current
        case .active:
            if rollbackRate > FingerprintPromotionThresholds.rollbackSpikeThreshold {
                return .decayed
            }
            return .active
        case .decayed:
            if rollbackRate > FingerprintPromotionThresholds.rollbackSpikeThreshold {
                return .blocked
            }
            return .decayed
        case .blocked:
            return .blocked
        }
    }
}
