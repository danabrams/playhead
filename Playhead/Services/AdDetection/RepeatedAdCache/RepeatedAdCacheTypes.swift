// RepeatedAdCacheTypes.swift
// Value types persisted in (and exchanged with) the RepeatedAdCache.

import Foundation

/// One cached ad-span entry. Keyed on `(showId, fingerprint)`. The values
/// reused on a hit are `(boundaryStart, boundaryEnd, confidence)` —
/// every other field on a real ad-window decision (transcript text,
/// classifier rationale, FM annotations) is intentionally NOT cached.
/// V1 reuses ONLY the boundary + confidence so:
///   1. We don't store user-content-derived data that could outlive a
///      cache invalidation.
///   2. The cache hit path doesn't have to reconstruct a full
///      ClassifierResult — it only has to give the runner enough to
///      skip the classifier round-trip.
struct RepeatedAdCacheEntry: Sendable, Hashable {
    let showId: String
    let fingerprint: RepeatedAdFingerprint
    let boundaryStart: Double
    let boundaryEnd: Double
    let confidence: Double
    let lastSeenAt: Date

    init(
        showId: String,
        fingerprint: RepeatedAdFingerprint,
        boundaryStart: Double,
        boundaryEnd: Double,
        confidence: Double,
        lastSeenAt: Date
    ) {
        self.showId = showId
        self.fingerprint = fingerprint
        self.boundaryStart = boundaryStart
        self.boundaryEnd = boundaryEnd
        self.confidence = confidence
        self.lastSeenAt = lastSeenAt
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
