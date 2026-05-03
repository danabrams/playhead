// RepeatedAdCacheConfig.swift
// Single source of truth for every threshold that B3 (playhead-43ed) ships with.
//
// Lifted into a struct so reviewer-mandated parametricity holds: tests can
// inject lower per-show / global caps (e.g. 3 / 5 instead of 200 / 2000) to
// drive eviction in unit time, and shipping production callers always read
// from `Config.production`. No call site is allowed to hard-code 0.85, 6 bits,
// 200, 2000, 90 days, 14 days, or 5%.
//
// Spec reference: bead playhead-43ed §1–§4. Each field below cites the bead
// clause it implements and the unit it is expressed in.

import Foundation

/// Configuration for ``RepeatedAdCacheService``. All knobs live here so
/// production code, unit tests, and (eventually) remote-config tuning land
/// at one site.
struct RepeatedAdCacheConfig: Sendable, Hashable {

    // MARK: Match policy (bead §2)

    /// Minimum classifier confidence at original detection for an entry to
    /// be CACHED (`store`). Bead §1 fixes this at `0.85`.
    let storeConfidenceThreshold: Double

    /// Maximum permitted Hamming distance (in bits) between two
    /// 128-bit fingerprints for a `lookup` to count as a match.
    /// Bead §2 fixes this at `≤ 6`.
    let hammingDistanceThreshold: Int

    // MARK: Eviction (bead §3)

    /// Per-show row cap. The 201st write for the same `(showId)` evicts
    /// the least-recently-used entry within that show. Bead §3 = 200.
    let perShowCap: Int

    /// Global row cap across all shows. Eviction order: per-show LRU
    /// runs first (within the show being inserted into), then global LRU
    /// across all shows. Bead §3 = 2000.
    let globalCap: Int

    /// Maximum age before an entry is purged on next access.
    /// Bead §3 = 90 days. Stored as `TimeInterval`.
    let entryMaxAge: TimeInterval

    // MARK: Auto-disable (bead §4)

    /// Window over which `recordOutcome(...)` accumulates hit/miss
    /// samples for the runtime self-disable check. Bead §4 = 14 days.
    let autoDisableWindow: TimeInterval

    /// Hit-rate floor below which the cache auto-disables itself once
    /// the window is full. Bead §4 = 5%.
    let autoDisableHitRateFloor: Double

    /// Minimum number of `recordOutcome` samples in the window before
    /// auto-disable can fire. Without a floor, a cold launch on day 1
    /// with 0/0 samples would either be undefined (NaN) or erroneously
    /// disable the cache. The bead spec is silent on this — we pick a
    /// conservative `50` so the cache cannot self-disable until it has
    /// actually been exercised. Tests inject smaller values.
    let autoDisableMinSamples: Int

    // MARK: Init

    init(
        storeConfidenceThreshold: Double,
        hammingDistanceThreshold: Int,
        perShowCap: Int,
        globalCap: Int,
        entryMaxAge: TimeInterval,
        autoDisableWindow: TimeInterval,
        autoDisableHitRateFloor: Double,
        autoDisableMinSamples: Int
    ) {
        precondition(storeConfidenceThreshold >= 0 && storeConfidenceThreshold <= 1)
        precondition(hammingDistanceThreshold >= 0 && hammingDistanceThreshold <= 128)
        precondition(perShowCap >= 1)
        precondition(globalCap >= 1)
        precondition(entryMaxAge > 0)
        precondition(autoDisableWindow > 0)
        precondition(autoDisableHitRateFloor >= 0 && autoDisableHitRateFloor <= 1)
        precondition(autoDisableMinSamples >= 1)
        self.storeConfidenceThreshold = storeConfidenceThreshold
        self.hammingDistanceThreshold = hammingDistanceThreshold
        self.perShowCap = perShowCap
        self.globalCap = globalCap
        self.entryMaxAge = entryMaxAge
        self.autoDisableWindow = autoDisableWindow
        self.autoDisableHitRateFloor = autoDisableHitRateFloor
        self.autoDisableMinSamples = autoDisableMinSamples
    }

    /// Production defaults — matches the bead spec verbatim.
    static let production = RepeatedAdCacheConfig(
        storeConfidenceThreshold: 0.85,
        hammingDistanceThreshold: 6,
        perShowCap: 200,
        globalCap: 2000,
        entryMaxAge: 90 * 24 * 60 * 60,
        autoDisableWindow: 14 * 24 * 60 * 60,
        autoDisableHitRateFloor: 0.05,
        autoDisableMinSamples: 50
    )
}

// MARK: - Feature flag key

/// `@AppStorage` key for the user-facing kill switch (Diagnostics only).
/// Bead §6: `b3_repeated_ad_cache_enabled`. Default `true` — the cache is
/// shipped on. Toggling to `false` clears the cache and short-circuits all
/// `store` / `lookup` calls.
enum RepeatedAdCacheFeatureFlag {
    static let userDefaultsKey = "b3_repeated_ad_cache_enabled"
    static let defaultValue = true

    /// Companion key written by `RepeatedAdCacheService.autoDisable(...)`
    /// so the auto-disable state survives an app launch (bead §4: "disable
    /// cache automatically if hit rate < 5% after 14 days" — implied
    /// persistence). When this key is `true`, the runtime constructs the
    /// service with `initiallyEnabled: false` even if the user-facing
    /// kill-switch is on. The user toggling the switch off-then-on resets
    /// this key.
    static let autoDisabledKey = "b3_repeated_ad_cache_auto_disabled"
}
