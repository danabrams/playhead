// RepeatedAdCacheService.swift
// playhead-43ed (B3): local cache that memoizes high-confidence ad-span
// detection results so a recurring sponsor in a new episode can reuse
// prior boundary + confidence data without re-running the classifier.
//
// Lifecycle:
//   `store(...)` after a successful detection ↷
//   `lookup(...)` before classification on the next episode ↷
//   `recordOutcome(hit:)` keeps a rolling 14-day hit-rate window ↷
//   below the configured floor → cache auto-disables itself
//
// Concurrency:
//   The service is an `actor` so writes serialise. The clock is
//   injected (`@Sendable () -> Date`) for deterministic tests.
//
// Storage:
//   Backed by ``RepeatedAdCacheStorage``. Two impls exist: an in-memory
//   one for unit tests, and the AnalysisStore-backed production one
//   that persists across launches.

import Foundation
import OSLog

/// Self-disable reason — exposed so the Diagnostics surface can render
/// an honest "off because hit-rate too low" message instead of guessing.
enum RepeatedAdCacheDisableReason: Sendable, Equatable {
    case userKillSwitch
    case autoDisabledLowHitRate(observedHitRate: Double, samples: Int)
}

/// Errors returned to the runner by `lookup`. Distinguished from
/// `RepeatedAdCacheError` because callers want to differentiate "no hit"
/// (a normal outcome) from a real failure.
enum RepeatedAdCacheLookupOutcome: Sendable {
    case hit(RepeatedAdCacheEntry)
    case miss
    case skippedDisabled
}

actor RepeatedAdCacheService {

    // MARK: Stored state

    let config: RepeatedAdCacheConfig
    private let storage: any RepeatedAdCacheStorage
    private let clock: @Sendable () -> Date
    /// Side-effect hook invoked when the rolling-window guard auto-
    /// disables the cache. Defaults to a no-op; production wires it to
    /// persist the disabled state into UserDefaults so it survives an
    /// app launch (bead §4 implied persistence). The closure is
    /// `@Sendable` because the actor is allowed to escape it via Task.
    private let onAutoDisable: @Sendable (Double, Int) -> Void
    private let logger = Logger(subsystem: "com.playhead", category: "RepeatedAdCacheService")

    /// Live enable/disable. Defaults to `true`. Flipped to `false` by
    /// the user kill-switch (`setEnabled(false)`) OR by the auto-disable
    /// telemetry guard. Any flip → entries + outcome samples cleared.
    private var enabled: Bool

    /// Why the cache is currently disabled. `nil` while enabled.
    private var disableReason: RepeatedAdCacheDisableReason?

    /// Hit rate snapshot from the most recent `recordOutcome` call.
    /// Re-derived on-demand from storage so a launch with persisted
    /// outcomes immediately reflects the right rate. Cached here for
    /// cheap reads from `currentHitRateSnapshot()`.
    private var lastHitRateSnapshot: RepeatedAdCacheHitRateSnapshot?

    // MARK: Init

    init(
        config: RepeatedAdCacheConfig = .production,
        storage: any RepeatedAdCacheStorage,
        initiallyEnabled: Bool = true,
        clock: @Sendable @escaping () -> Date = { Date() },
        onAutoDisable: @Sendable @escaping (Double, Int) -> Void = { _, _ in }
    ) {
        self.config = config
        self.storage = storage
        self.clock = clock
        self.onAutoDisable = onAutoDisable
        self.enabled = initiallyEnabled
        self.disableReason = initiallyEnabled ? nil : .userKillSwitch
    }

    // MARK: Public surface

    func isEnabled() -> Bool { enabled }
    func currentDisableReason() -> RepeatedAdCacheDisableReason? { disableReason }

    /// Toggle the user kill switch. `false` clears entries + outcomes
    /// (bead §6: "disabling clears the cache"). `true` re-enables but
    /// does NOT repopulate — entries are populated lazily by future
    /// detections.
    func setEnabled(_ newValue: Bool) async {
        if newValue == enabled { return }
        enabled = newValue
        if !newValue {
            disableReason = .userKillSwitch
            try? await storage.clearEntries()
            try? await storage.clearOutcomes()
            lastHitRateSnapshot = nil
            logger.info("RepeatedAdCache: disabled by user kill-switch — entries + outcomes cleared")
        } else {
            disableReason = nil
            logger.info("RepeatedAdCache: re-enabled by user")
        }
    }

    /// Store a detection outcome. No-op when the cache is disabled.
    /// No-op when `confidence < storeConfidenceThreshold` — bead §1
    /// requires only high-confidence hits to be cached.
    /// Returns `true` if the entry was actually persisted.
    @discardableResult
    func store(
        showId: String,
        fingerprint: RepeatedAdFingerprint,
        boundaryStart: Double,
        boundaryEnd: Double,
        confidence: Double
    ) async throws -> Bool {
        guard enabled else { return false }
        guard confidence >= config.storeConfidenceThreshold else { return false }
        guard !fingerprint.isZero else {
            // Refuse to cache the all-zeros sentinel — it would collide
            // with any future zero-energy span and poison the cache.
            return false
        }
        guard !showId.isEmpty else { return false }

        let now = clock()
        let entry = RepeatedAdCacheEntry(
            showId: showId,
            fingerprint: fingerprint,
            boundaryStart: boundaryStart,
            boundaryEnd: boundaryEnd,
            confidence: confidence,
            lastSeenAt: now
        )

        try await storage.upsert(entry)

        // Eviction: per-show first (so the global cap doesn't run on a
        // show that's only marginally over its share), then global.
        let perShow = try await storage.count(showId: showId)
        if perShow > config.perShowCap {
            let overflow = perShow - config.perShowCap
            for _ in 0..<overflow {
                let evicted = try await storage.evictOldest(showId: showId)
                if !evicted { break }
            }
        }
        let total = try await storage.totalCount()
        if total > config.globalCap {
            let overflow = total - config.globalCap
            for _ in 0..<overflow {
                let evicted = try await storage.evictOldestGlobal()
                if !evicted { break }
            }
        }

        return true
    }

    /// Look up a high-confidence cached entry that matches the given
    /// fingerprint within `hammingDistanceThreshold` AND belongs to
    /// the same show. Cache misses don't throw; they return
    /// `.miss`. `recordOutcome` is the caller's responsibility — the
    /// lookup itself only reads.
    func lookup(
        showId: String,
        fingerprint: RepeatedAdFingerprint
    ) async throws -> RepeatedAdCacheLookupOutcome {
        guard enabled else { return .skippedDisabled }
        guard !fingerprint.isZero else { return .miss }
        guard !showId.isEmpty else { return .miss }

        // Purge stale entries lazily so a hit can never return a
        // > 90-day entry.
        let cutoff = clock().addingTimeInterval(-config.entryMaxAge)
        try await storage.purgeStale(olderThan: cutoff)

        let candidates = try await storage.fetchAll(showId: showId)

        // First match wins. fetchAll is sorted by `lastSeenAt` DESC so
        // the most-recently-used entry that matches is selected — which
        // is the right LRU semantics ("prefer the freshest match").
        for candidate in candidates {
            if candidate.fingerprint.hammingDistance(to: fingerprint) <= config.hammingDistanceThreshold {
                // Bead §2 requires confidence ≥ 0.85 at original
                // detection. We never write a row that doesn't satisfy
                // that, but defend in depth:
                guard candidate.confidence >= config.storeConfidenceThreshold else { continue }

                // Bump LRU clock and return. Note: we do NOT call
                // recordOutcome here — that's the caller's job because
                // a `lookup` may be exploratory (e.g. corpus replay).
                // Hoist `clock()` to a single value so the storage row
                // and the returned entry agree on `lastSeenAt` byte-
                // for-byte (two clock reads can fall on different
                // microseconds with an injected mock clock).
                let touchedNow = clock()
                try await storage.touch(
                    showId: showId,
                    fingerprint: candidate.fingerprint,
                    at: touchedNow
                )
                let touched = RepeatedAdCacheEntry(
                    showId: candidate.showId,
                    fingerprint: candidate.fingerprint,
                    boundaryStart: candidate.boundaryStart,
                    boundaryEnd: candidate.boundaryEnd,
                    confidence: candidate.confidence,
                    lastSeenAt: touchedNow
                )
                return .hit(touched)
            }
        }
        return .miss
    }

    // MARK: Hit-rate window + auto-disable

    /// Record a single user-facing outcome (hit or miss). Drives the
    /// 14-day rolling window and triggers auto-disable when the
    /// configured floor is breached.
    func recordOutcome(hit: Bool) async throws {
        guard enabled else { return }
        let now = clock()
        try await storage.appendOutcome(.init(timestamp: now, isHit: hit))

        // Trim outside the active window so the table size is bounded.
        let cutoff = now.addingTimeInterval(-config.autoDisableWindow)
        try await storage.purgeOutcomes(olderThan: cutoff)

        let snapshot = try await computeHitRateSnapshot(now: now)
        lastHitRateSnapshot = snapshot

        // Auto-disable: only after enough samples have accumulated.
        if snapshot.totalSamples >= config.autoDisableMinSamples,
           let rate = snapshot.hitRate,
           rate < config.autoDisableHitRateFloor {
            await autoDisable(observedHitRate: rate, samples: snapshot.totalSamples)
        }
    }

    /// Hit-rate snapshot for the current 14-day window. Re-computed
    /// from persisted outcomes so a launch sees the correct value
    /// even if `recordOutcome` was never called this session.
    func currentHitRateSnapshot() async throws -> RepeatedAdCacheHitRateSnapshot {
        let snapshot = try await computeHitRateSnapshot(now: clock())
        lastHitRateSnapshot = snapshot
        return snapshot
    }

    private func computeHitRateSnapshot(now: Date) async throws -> RepeatedAdCacheHitRateSnapshot {
        let cutoff = now.addingTimeInterval(-config.autoDisableWindow)
        let samples = try await storage.fetchOutcomes(newerThan: cutoff)
        let hitCount = samples.filter { $0.isHit }.count
        return RepeatedAdCacheHitRateSnapshot(
            windowSeconds: config.autoDisableWindow,
            totalSamples: samples.count,
            hitCount: hitCount
        )
    }

    private func autoDisable(observedHitRate: Double, samples: Int) async {
        guard enabled else { return }
        enabled = false
        disableReason = .autoDisabledLowHitRate(
            observedHitRate: observedHitRate,
            samples: samples
        )
        try? await storage.clearEntries()
        try? await storage.clearOutcomes()
        lastHitRateSnapshot = nil
        logger.info("RepeatedAdCache: auto-disabled (rate=\(observedHitRate, privacy: .public), samples=\(samples, privacy: .public))")
        // Notify the embedder so it can persist the disabled state
        // across launches (bead §4). Defaulted to a no-op so unit
        // tests that don't care about persistence don't have to wire
        // it up.
        onAutoDisable(observedHitRate, samples)
    }

    // MARK: Maintenance

    /// Purge entries older than `entryMaxAge`. Idempotent.
    @discardableResult
    func purgeStaleEntries() async throws -> Int {
        let cutoff = clock().addingTimeInterval(-config.entryMaxAge)
        return try await storage.purgeStale(olderThan: cutoff)
    }

    /// Force-clear everything (entries + outcomes). Used by Diagnostics
    /// "Clear cache" affordance.
    func clear() async throws {
        try await storage.clearEntries()
        try await storage.clearOutcomes()
        lastHitRateSnapshot = nil
        logger.info("RepeatedAdCache: cleared by explicit request")
    }
}
