// RepeatedAdCacheTypes.swift
// Value types persisted in (and exchanged with) the RepeatedAdCache.

import Foundation

/// One cached ad-span entry. Keyed on `(showId, fingerprint)`. Historical
/// absolute source boundaries and confidence remain persisted for audit, but
/// are not replayed into another episode's decision. Every other field on a
/// real ad-window decision (transcript text, classifier rationale, FM
/// annotations) is intentionally NOT cached, so:
///   1. We don't store user-content-derived data that could outlive a
///      cache invalidation.
///   2. A cache match cannot become a lossy parallel decision producer.
struct RepeatedAdCacheEntry: Sendable, Hashable {
    let showId: String
    let fingerprint: RepeatedAdFingerprint
    let boundaryStart: Double
    let boundaryEnd: Double
    let confidence: Double
    let lastSeenAt: Date
    /// Confirmation provenance. Legacy rows are preserved but quarantined
    /// from lookup as `.legacyUnconfirmed`.
    let learningSource: CatalogLearningSource
    let learningLifecycle: CatalogLearningLifecycle
    let sourceAssetId: String?
    let sourceWindowId: String?
    /// Opaque identity of the writer that produced this persisted revision.
    /// Material equality is insufficient for stale-writer cleanup because two
    /// legitimate writes can carry byte-identical timestamps and boundaries.
    let producerRevision: String

    init(
        showId: String,
        fingerprint: RepeatedAdFingerprint,
        boundaryStart: Double,
        boundaryEnd: Double,
        confidence: Double,
        lastSeenAt: Date,
        learningSource: CatalogLearningSource = .legacyUnconfirmed,
        learningLifecycle: CatalogLearningLifecycle = .legacyUnconfirmed,
        sourceAssetId: String? = nil,
        sourceWindowId: String? = nil,
        producerRevision: String = UUID().uuidString
    ) {
        self.showId = showId
        self.fingerprint = fingerprint
        self.boundaryStart = boundaryStart
        self.boundaryEnd = boundaryEnd
        self.confidence = confidence
        self.lastSeenAt = lastSeenAt
        self.learningSource = learningSource
        self.learningLifecycle = learningLifecycle
        self.sourceAssetId = sourceAssetId
        self.sourceWindowId = sourceWindowId
        self.producerRevision = producerRevision
    }
}

/// Outcome sample for the auto-disable rolling-window calculator.
/// Persisted so the 14-day window survives a launch.
struct RepeatedAdCacheOutcomeSample: Sendable, Hashable {
    let timestamp: Date
    /// `true` for a cache hit, `false` for a miss. The denominator is
    /// every `recordOutcome(...)` call within the window.
    let isHit: Bool

    init(timestamp: Date, isHit: Bool) {
        self.timestamp = timestamp
        self.isHit = isHit
    }
}

/// Hit-rate snapshot exposed to telemetry consumers (Phase 3 SLI).
struct RepeatedAdCacheHitRateSnapshot: Sendable, Hashable {
    let windowSeconds: TimeInterval
    let totalSamples: Int
    let hitCount: Int
    let missCount: Int

    /// `nil` when `totalSamples == 0` — no samples ⇒ no defined rate.
    /// (Distinct from `0.0`, which is "samples exist, none hit.")
    var hitRate: Double? {
        guard totalSamples > 0 else { return nil }
        return Double(hitCount) / Double(totalSamples)
    }

    init(windowSeconds: TimeInterval, totalSamples: Int, hitCount: Int) {
        self.windowSeconds = windowSeconds
        self.totalSamples = totalSamples
        self.hitCount = hitCount
        self.missCount = totalSamples - hitCount
    }
}

/// Errors specific to ``RepeatedAdCacheService``.
enum RepeatedAdCacheError: Error, Equatable, Sendable {
    case zeroFingerprintNotCacheable
    case storageFailure(String)
}
