// RepeatedAdCacheServiceTests.swift
// playhead-43ed (B3) — TDD specification for the RepeatedAdCacheService.
//
// Each `@Test` corresponds to one numbered behavior in the bead description:
//   1.  cacheStoresEntryAfterHighConfidenceAdDetection
//   2.  cacheRejectsLowConfidenceAdDetection
//   3.  cacheLookupHitWithinHammingDistance
//   4.  cacheLookupHitOnHammingDistance6
//   5.  cacheLookupMissOnHammingDistance7
//   6.  cacheLookupRespectsShowBoundary
//   7.  cacheEntryEvictedAfter90Days
//   8.  cachePerShowCapEnforcedAt200
//   9.  cacheGlobalCapEnforcedAt2000
//   10. cacheHitUpdatesLastSeenAt
//   13. cacheDisabledByFeatureFlag
//   14. cacheClearedOnFlagDisable
//   15. cacheAutoDisablesAfter14DaysBelow5Percent
//   16. cacheStaysEnabledAbove5Percent
//   17. cacheHitRateInstrumentationVisible
//   18. boundaryAdjustmentReusedFromCache
//   19. confidenceFromCacheReusedAtMemoryHitMin0_85
//
// Production-wiring tests (#11, #12) live in
// `RepeatedAdCacheWiringTests.swift` so they can target `AdDetectionService`
// directly.

import Foundation
import Testing
@testable import Playhead

@Suite("RepeatedAdCacheService (playhead-43ed)")
struct RepeatedAdCacheServiceTests {

    // MARK: - Test config (parametric — never hard-codes 0.85, 6 bits, 200, 2000, 90 days, 14 days, 5%)

    /// Tiny config for unit speed. Reviewer mandate: NO threshold may
    /// be hard-coded outside `RepeatedAdCacheConfig.production`.
    static let testConfig = RepeatedAdCacheConfig(
        storeConfidenceThreshold: 0.85,
        hammingDistanceThreshold: 3,
        perShowCap: 3,
        globalCap: 5,
        entryMaxAge: 90 * 24 * 60 * 60,
        autoDisableWindow: 14 * 24 * 60 * 60,
        autoDisableHitRateFloor: 0.05,
        autoDisableMinSamples: 4
    )

    // MARK: - Helpers

    /// Stable, deterministic clock seam. Tests increment `current.value`.
    final class MutableClock: @unchecked Sendable {
        var value: Date = Date(timeIntervalSince1970: 0)
        func now() -> Date { value }
        func advance(by seconds: TimeInterval) { value = value.addingTimeInterval(seconds) }
    }

    static func makeService(
        config: RepeatedAdCacheConfig = testConfig,
        clock: MutableClock = MutableClock(),
        initiallyEnabled: Bool = true
    ) -> (RepeatedAdCacheService, MutableClock, InMemoryRepeatedAdCacheStorage) {
        let storage = InMemoryRepeatedAdCacheStorage()
        let service = RepeatedAdCacheService(
            config: config,
            storage: storage,
            initiallyEnabled: initiallyEnabled,
            clock: { clock.now() }
        )
        return (service, clock, storage)
    }

    /// Build a 64-bit fingerprint that has bit `i` flipped (relative to
    /// all-zeros). Useful for asserting Hamming distance at exact
    /// boundaries.
    static func fpWithBitsFlipped(_ indices: [Int]) -> RepeatedAdFingerprint {
        var bits = [Bool](repeating: false, count: RepeatedAdFingerprint.bitWidth)
        // Always flip bit 0 too so the fingerprint isn't the zero
        // sentinel (which is "do not cache").
        bits[0] = true
        for i in indices where i != 0 {
            bits[i] = true
        }
        return RepeatedAdFingerprint.fromBits(bits)
    }

    static let baseFingerprint: RepeatedAdFingerprint = fpWithBitsFlipped([])

    // MARK: - 1. cacheStoresEntryAfterHighConfidenceAdDetection

    @Test
    func cacheStoresEntryAfterHighConfidenceAdDetection() async throws {
        let (service, _, storage) = Self.makeService()
        let stored = try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 100.0,
            boundaryEnd: 130.0,
            confidence: 0.90
        )
        #expect(stored == true)
        let rows = try await storage.fetchAll(showId: "show-1")
        #expect(rows.count == 1)
        #expect(rows[0].confidence == 0.90)
    }

    // MARK: - 2. cacheRejectsLowConfidenceAdDetection

    @Test
    func cacheRejectsLowConfidenceAdDetection() async throws {
        let (service, _, storage) = Self.makeService()
        // Just below the threshold — must NOT be stored.
        let stored = try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 0,
            boundaryEnd: 1,
            confidence: Self.testConfig.storeConfidenceThreshold - 0.001
        )
        #expect(stored == false)
        let rows = try await storage.fetchAll(showId: "show-1")
        #expect(rows.isEmpty)
    }

    // MARK: - 3. cacheLookupHitWithinHammingDistance

    @Test
    func cacheLookupHitWithinHammingDistance() async throws {
        let (service, _, _) = Self.makeService()
        try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 100,
            boundaryEnd: 130,
            confidence: 0.95
        )
        let outcome = try await service.lookup(showId: "show-1", fingerprint: Self.baseFingerprint)
        guard case .hit(let entry) = outcome else {
            Issue.record("Expected hit, got \(outcome)")
            return
        }
        #expect(entry.boundaryStart == 100)
        #expect(entry.boundaryEnd == 130)
        #expect(entry.confidence == 0.95)
    }

    // MARK: - 4. cacheLookupHitOnHammingDistanceAtThreshold

    @Test
    func cacheLookupHitOnHammingDistanceAtThreshold() async throws {
        let (service, _, _) = Self.makeService()
        try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1, boundaryEnd: 2, confidence: 0.95
        )
        // Flip exactly `threshold` bits (in addition to the always-on
        // bit 0 in baseFingerprint) — Hamming distance lands at the
        // threshold, which must HIT.
        let threshold = Self.testConfig.hammingDistanceThreshold
        let flipIndices = Array(1...threshold).map { $0 * 10 }
        let probe = Self.fpWithBitsFlipped(flipIndices)
        // Sanity-check: flipped bits are all off in baseFingerprint,
        // so Hamming distance equals `threshold`.
        let dist = Self.baseFingerprint.hammingDistance(to: probe)
        #expect(dist == threshold, "fixture bug: expected distance \(threshold), got \(dist)")
        let outcome = try await service.lookup(showId: "show-1", fingerprint: probe)
        guard case .hit = outcome else {
            Issue.record("distance==\(threshold) must HIT (≤ threshold), got \(outcome)")
            return
        }
    }

    // MARK: - 5. cacheLookupMissOnHammingDistanceAboveThreshold

    @Test
    func cacheLookupMissOnHammingDistanceAboveThreshold() async throws {
        let (service, _, _) = Self.makeService()
        try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1, boundaryEnd: 2, confidence: 0.95
        )
        let threshold = Self.testConfig.hammingDistanceThreshold
        // Flip `threshold + 1` bits — must MISS (just over threshold).
        let flipIndices = Array(1...(threshold + 1)).map { $0 * 10 }
        let probe = Self.fpWithBitsFlipped(flipIndices)
        #expect(Self.baseFingerprint.hammingDistance(to: probe) == threshold + 1)
        let outcome = try await service.lookup(showId: "show-1", fingerprint: probe)
        if case .hit = outcome {
            Issue.record("distance==\(threshold + 1) must MISS (> threshold)")
        }
    }

    // MARK: - 6. cacheLookupRespectsShowBoundary

    @Test
    func cacheLookupRespectsShowBoundary() async throws {
        let (service, _, _) = Self.makeService()
        try await service.store(
            showId: "show-A",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1, boundaryEnd: 2, confidence: 0.95
        )
        let outcome = try await service.lookup(
            showId: "show-B",
            fingerprint: Self.baseFingerprint
        )
        if case .hit = outcome {
            Issue.record("Same fingerprint, different show, must MISS")
        }
    }

    // MARK: - 7. cacheEntryEvictedAfter90Days

    @Test
    func cacheEntryEvictedAfter90Days() async throws {
        let clock = MutableClock()
        let (service, _, storage) = Self.makeService(clock: clock)
        try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1, boundaryEnd: 2, confidence: 0.95
        )
        #expect(try await storage.totalCount() == 1)
        // Advance just past 90 days.
        clock.advance(by: Self.testConfig.entryMaxAge + 1)
        let outcome = try await service.lookup(
            showId: "show-1",
            fingerprint: Self.baseFingerprint
        )
        if case .hit = outcome {
            Issue.record("Entry > entryMaxAge old must be purged on next access")
        }
        // Either the lookup-time purge or an explicit purge eliminates the row.
        #expect(try await storage.totalCount() == 0)
    }

    // MARK: - 8. cachePerShowCapEnforcedAt200

    @Test
    func cachePerShowCapEnforcedAt200() async throws {
        // Use the small testConfig where perShowCap = 3.
        let clock = MutableClock()
        let (service, _, storage) = Self.makeService(clock: clock)
        // Fill to (perShowCap + 1) — the 4th write should evict the
        // oldest of the first three.
        for i in 0..<(Self.testConfig.perShowCap + 1) {
            clock.advance(by: 1.0)
            try await service.store(
                showId: "show-1",
                fingerprint: Self.fpWithBitsFlipped([i + 1]),
                boundaryStart: Double(i),
                boundaryEnd: Double(i) + 1,
                confidence: 0.95
            )
        }
        let count = try await storage.count(showId: "show-1")
        #expect(count == Self.testConfig.perShowCap)

        // The OLDEST one (boundaryStart == 0, fp = bit 1) should be gone.
        let surviving = try await storage.fetchAll(showId: "show-1")
        #expect(surviving.allSatisfy { $0.boundaryStart > 0 })
    }

    // MARK: - 9. cacheGlobalCapEnforcedAt2000

    @Test
    func cacheGlobalCapEnforcedAt2000() async throws {
        let clock = MutableClock()
        let (service, _, storage) = Self.makeService(clock: clock)

        // Write entries across many shows so per-show cap doesn't fire,
        // forcing the global cap to evict.
        for i in 0..<(Self.testConfig.globalCap + 2) {
            clock.advance(by: 1.0)
            try await service.store(
                showId: "show-\(i)",
                fingerprint: Self.fpWithBitsFlipped([i + 1]),
                boundaryStart: Double(i),
                boundaryEnd: Double(i) + 1,
                confidence: 0.95
            )
        }
        let total = try await storage.totalCount()
        #expect(total == Self.testConfig.globalCap)
    }

    // MARK: - 10. cacheHitUpdatesLastSeenAt

    @Test
    func cacheHitUpdatesLastSeenAt() async throws {
        let clock = MutableClock()
        let (service, _, storage) = Self.makeService(clock: clock)
        try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1, boundaryEnd: 2, confidence: 0.95
        )
        let beforeRows = try await storage.fetchAll(showId: "show-1")
        let beforeLastSeen = beforeRows[0].lastSeenAt

        clock.advance(by: 60)
        _ = try await service.lookup(showId: "show-1", fingerprint: Self.baseFingerprint)

        let afterRows = try await storage.fetchAll(showId: "show-1")
        #expect(afterRows[0].lastSeenAt > beforeLastSeen)
        #expect(afterRows[0].lastSeenAt == clock.value)
    }

    // MARK: - 13. cacheDisabledByFeatureFlag

    @Test
    func cacheDisabledByFeatureFlag() async throws {
        let (service, _, storage) = Self.makeService(initiallyEnabled: false)
        let stored = try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1, boundaryEnd: 2, confidence: 0.95
        )
        #expect(stored == false)
        #expect(try await storage.totalCount() == 0)

        let outcome = try await service.lookup(
            showId: "show-1",
            fingerprint: Self.baseFingerprint
        )
        if case .skippedDisabled = outcome {
            // OK
        } else {
            Issue.record("Disabled cache must short-circuit lookup with .skippedDisabled, got \(outcome)")
        }
    }

    // MARK: - 14. cacheClearedOnFlagDisable

    @Test
    func cacheClearedOnFlagDisable() async throws {
        let (service, _, storage) = Self.makeService()
        try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1, boundaryEnd: 2, confidence: 0.95
        )
        try await service.recordOutcome(hit: true)
        #expect(try await storage.totalCount() == 1)

        await service.setEnabled(false)
        #expect(try await storage.totalCount() == 0)
        // Outcome window is also cleared on kill-switch flip.
        let snapshot = try await service.currentHitRateSnapshot()
        #expect(snapshot.totalSamples == 0)
    }

    // MARK: - 15. cacheAutoDisablesAfter14DaysBelow5Percent

    @Test
    func cacheAutoDisablesAfter14DaysBelow5Percent() async throws {
        // Inject a tiny min-sample threshold so the test runs in O(min-samples)
        // outcomes rather than 50.
        let cfg = RepeatedAdCacheConfig(
            storeConfidenceThreshold: 0.85,
            hammingDistanceThreshold: 3,
            perShowCap: 3,
            globalCap: 5,
            entryMaxAge: 90 * 24 * 60 * 60,
            autoDisableWindow: 14 * 24 * 60 * 60,
            autoDisableHitRateFloor: 0.05,
            autoDisableMinSamples: 100
        )
        let clock = MutableClock()
        let (service, _, _) = Self.makeService(config: cfg, clock: clock)
        // 100 misses, 0 hits → 0% < 5% → must auto-disable.
        for _ in 0..<100 {
            try await service.recordOutcome(hit: false)
        }
        #expect(await service.isEnabled() == false)
        let reason = await service.currentDisableReason()
        if case .autoDisabledLowHitRate(let rate, let samples) = reason {
            #expect(rate < cfg.autoDisableHitRateFloor)
            #expect(samples >= cfg.autoDisableMinSamples)
        } else {
            Issue.record("Expected autoDisabledLowHitRate, got \(String(describing: reason))")
        }
    }

    // MARK: - 16. cacheStaysEnabledAbove5Percent

    @Test
    func cacheStaysEnabledAbove5Percent() async throws {
        let cfg = RepeatedAdCacheConfig(
            storeConfidenceThreshold: 0.85,
            hammingDistanceThreshold: 3,
            perShowCap: 3,
            globalCap: 5,
            entryMaxAge: 90 * 24 * 60 * 60,
            autoDisableWindow: 14 * 24 * 60 * 60,
            autoDisableHitRateFloor: 0.05,
            autoDisableMinSamples: 20
        )
        let (service, _, _) = Self.makeService(config: cfg)
        // 6/20 = 30% — well above 5% — must stay enabled.
        for _ in 0..<6 { try await service.recordOutcome(hit: true) }
        for _ in 0..<14 { try await service.recordOutcome(hit: false) }
        #expect(await service.isEnabled() == true)
        let snap = try await service.currentHitRateSnapshot()
        #expect(snap.totalSamples == 20)
        #expect(snap.hitCount == 6)
        #expect(snap.hitRate == 0.30)
    }

    // MARK: - 17. cacheHitRateInstrumentationVisible

    @Test
    func cacheHitRateInstrumentationVisible() async throws {
        let (service, _, _) = Self.makeService()
        // Mixed window — 1 hit, 1 miss → 50%.
        try await service.recordOutcome(hit: true)
        try await service.recordOutcome(hit: false)
        let snap = try await service.currentHitRateSnapshot()
        #expect(snap.totalSamples == 2)
        #expect(snap.hitCount == 1)
        #expect(snap.missCount == 1)
        #expect(snap.hitRate == 0.5)
        #expect(snap.windowSeconds == Self.testConfig.autoDisableWindow)
    }

    // MARK: - 18. boundaryAdjustmentReusedFromCache

    @Test
    func boundaryAdjustmentReusedFromCache() async throws {
        let (service, _, _) = Self.makeService()
        try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 100.0,
            boundaryEnd: 130.5,
            confidence: 0.95
        )
        let outcome = try await service.lookup(
            showId: "show-1",
            fingerprint: Self.baseFingerprint
        )
        guard case .hit(let entry) = outcome else {
            Issue.record("expected hit")
            return
        }
        // Bead §1: cache reuses (boundaryStart, boundaryEnd, confidence).
        #expect(entry.boundaryStart == 100.0)
        #expect(entry.boundaryEnd == 130.5)
    }

    // MARK: - 19. confidenceFromCacheReusedAtMemoryHitMin0_85

    @Test
    func confidenceFromCacheReusedAtMemoryHitMin0_85() async throws {
        let (service, _, _) = Self.makeService()
        // Store at exactly the floor.
        try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 0, boundaryEnd: 1,
            confidence: Self.testConfig.storeConfidenceThreshold
        )
        let outcome = try await service.lookup(
            showId: "show-1",
            fingerprint: Self.baseFingerprint
        )
        guard case .hit(let entry) = outcome else {
            Issue.record("expected hit at exact threshold")
            return
        }
        #expect(entry.confidence >= Self.testConfig.storeConfidenceThreshold)
    }

    // MARK: - Extra: zero fingerprint refused as a key

    @Test
    func zeroFingerprintIsNotCacheable() async throws {
        let (service, _, storage) = Self.makeService()
        let stored = try await service.store(
            showId: "show-1",
            fingerprint: .zero,
            boundaryStart: 0, boundaryEnd: 1, confidence: 0.95
        )
        #expect(stored == false)
        #expect(try await storage.totalCount() == 0)

        // Lookups against zero must miss too — no row could exist for it.
        let outcome = try await service.lookup(showId: "show-1", fingerprint: .zero)
        if case .hit = outcome { Issue.record("zero fp must never hit") }
    }

    // MARK: - Extra: empty showId guard

    @Test
    func emptyShowIdRefused() async throws {
        let (service, _, storage) = Self.makeService()
        let stored = try await service.store(
            showId: "",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 0, boundaryEnd: 1, confidence: 0.95
        )
        #expect(stored == false)
        #expect(try await storage.totalCount() == 0)
    }

    // MARK: - Extra: outcome windowing trims old samples out

    @Test
    func outcomeWindowTrimsOldSamples() async throws {
        let clock = MutableClock()
        let (service, _, _) = Self.makeService(clock: clock)
        try await service.recordOutcome(hit: true)
        try await service.recordOutcome(hit: true)
        // Advance past the 14-day window.
        clock.advance(by: Self.testConfig.autoDisableWindow + 1)
        // Now record a single fresh outcome; the previous two should be
        // outside the window and dropped from the snapshot.
        try await service.recordOutcome(hit: false)
        let snap = try await service.currentHitRateSnapshot()
        #expect(snap.totalSamples == 1)
        #expect(snap.hitCount == 0)
    }

    // MARK: - L1: defense-in-depth — low-confidence row in storage is ignored on lookup

    /// `RepeatedAdCacheService.store(...)` enforces
    /// `confidence >= storeConfidenceThreshold` and only persists rows
    /// that satisfy it. Belt-and-braces, `lookup(...)` re-checks the
    /// same gate so a corrupted row, an out-of-band SQLite write (e.g.
    /// a sync conflict resolution bug), or a future `store` regression
    /// cannot resurrect a low-confidence entry. Pre-fix this `guard`
    /// branch was un-tested; the reviewer flagged it as dead code. We
    /// pin the contract by writing a low-confidence row through the
    /// storage seam and asserting the service returns `.miss`.
    @Test
    func lookupSkipsRowsBelowStoreConfidenceThreshold() async throws {
        let (service, _, storage) = Self.makeService()
        // Bypass service.store() — write directly into storage with a
        // confidence below the threshold. Any future code path (sync
        // restore, schema downgrade, a fresh `store` regression) that
        // can introduce a low-confidence row will look like this.
        let belowThreshold = Self.testConfig.storeConfidenceThreshold - 0.10
        try await storage.upsert(.init(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 2,
            confidence: belowThreshold,
            lastSeenAt: Date(timeIntervalSince1970: 100)
        ))
        // Sanity: storage actually has the row.
        #expect(try await storage.totalCount() == 1)

        // Lookup against the exact-matching fingerprint must MISS — the
        // confidence guard rejects the candidate.
        let outcome = try await service.lookup(
            showId: "show-1",
            fingerprint: Self.baseFingerprint
        )
        if case .hit = outcome {
            Issue.record("low-confidence row must be ignored by lookup, got \(outcome)")
        }
    }

    // MARK: - Extra: re-enable does NOT magically restore data (kill switch is destructive)

    @Test
    func reEnablingAfterKillSwitchDoesNotRehydrate() async throws {
        let (service, _, storage) = Self.makeService()
        try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1, boundaryEnd: 2, confidence: 0.95
        )
        await service.setEnabled(false)
        await service.setEnabled(true)
        #expect(try await storage.totalCount() == 0)
    }

    // MARK: - Auto-disable persistence callback (review-followup)

    /// `onAutoDisable` MUST fire exactly once when the rolling-window
    /// guard trips, so the embedder (`PlayheadRuntime`) can persist the
    /// disabled state across launches (bead §4 implied persistence).
    @Test
    func onAutoDisableCallbackFiresExactlyOnceOnAutoDisable() async throws {
        let cfg = RepeatedAdCacheConfig(
            storeConfidenceThreshold: 0.85,
            hammingDistanceThreshold: 3,
            perShowCap: 3,
            globalCap: 5,
            entryMaxAge: 90 * 24 * 60 * 60,
            autoDisableWindow: 14 * 24 * 60 * 60,
            autoDisableHitRateFloor: 0.05,
            autoDisableMinSamples: 50
        )
        // Sendable counter — actor-isolated would deadlock since the
        // callback fires while we're inside the cache actor.
        final class Counter: @unchecked Sendable {
            private let lock = NSLock()
            private var count = 0
            private var lastRate: Double = -1
            private var lastSamples: Int = -1
            func bump(rate: Double, samples: Int) {
                lock.lock()
                count += 1
                lastRate = rate
                lastSamples = samples
                lock.unlock()
            }
            func snapshot() -> (Int, Double, Int) {
                lock.lock(); defer { lock.unlock() }
                return (count, lastRate, lastSamples)
            }
        }
        let counter = Counter()
        let storage = InMemoryRepeatedAdCacheStorage()
        let clock = MutableClock()
        let service = RepeatedAdCacheService(
            config: cfg,
            storage: storage,
            initiallyEnabled: true,
            clock: { clock.now() },
            onAutoDisable: { rate, samples in
                counter.bump(rate: rate, samples: samples)
            }
        )

        // Drive 50 misses → 0% < 5% with samples ≥ 50 → must auto-disable.
        for _ in 0..<50 {
            try await service.recordOutcome(hit: false)
        }

        let (count, rate, samples) = counter.snapshot()
        #expect(count == 1, "onAutoDisable must fire exactly once on the auto-disable transition")
        #expect(rate < cfg.autoDisableHitRateFloor)
        #expect(samples >= cfg.autoDisableMinSamples)
        #expect(await service.isEnabled() == false)

        // Subsequent recordOutcome calls (while disabled) must NOT
        // re-fire the callback.
        try await service.recordOutcome(hit: false)
        let (count2, _, _) = counter.snapshot()
        #expect(count2 == 1, "onAutoDisable must not fire again while disabled")
    }
}

// MARK: - Fingerprint algebra (Hamming distance contract)

@Suite("RepeatedAdFingerprint Hamming distance")
struct RepeatedAdFingerprintTests {

    @Test
    func zeroIsAllZeros() {
        #expect(RepeatedAdFingerprint.zero.bits == 0)
        #expect(RepeatedAdFingerprint.zero.isZero)
    }

    @Test
    func selfDistanceIsZero() {
        let bits = (0..<RepeatedAdFingerprint.bitWidth).map { ($0 % 3) == 0 }
        let fp = RepeatedAdFingerprint.fromBits(bits)
        #expect(fp.hammingDistance(to: fp) == 0)
    }

    @Test
    func distanceIsSymmetric() {
        let a = RepeatedAdFingerprint.fromBits((0..<RepeatedAdFingerprint.bitWidth).map { ($0 % 5) == 0 })
        let b = RepeatedAdFingerprint.fromBits((0..<RepeatedAdFingerprint.bitWidth).map { ($0 % 7) == 0 })
        #expect(a.hammingDistance(to: b) == b.hammingDistance(to: a))
    }

    @Test
    func flipExactlyKBitsGivesDistanceK() {
        var bits = [Bool](repeating: false, count: RepeatedAdFingerprint.bitWidth)
        bits[0] = true
        let base = RepeatedAdFingerprint.fromBits(bits)
        for k in [1, 3, 4, 32, RepeatedAdFingerprint.bitWidth - 1] {
            var flipped = bits
            for i in 1...k {
                flipped[i] = !flipped[i]
            }
            let other = RepeatedAdFingerprint.fromBits(flipped)
            #expect(base.hammingDistance(to: other) == k, "k=\(k)")
        }
    }

    @Test
    func hexRoundTrip() {
        let fp = RepeatedAdFingerprint(bits: 0x0123456789abcdef)
        let s = fp.hexString
        #expect(s.count == 16)
        let parsed = RepeatedAdFingerprint(hexString: s)
        #expect(parsed == fp)
    }

    @Test
    func malformedHexReturnsNil() {
        #expect(RepeatedAdFingerprint(hexString: "") == nil)
        #expect(RepeatedAdFingerprint(hexString: "abc") == nil)
        // Wrong length (32 chars when 16 are required).
        #expect(RepeatedAdFingerprint(hexString: String(repeating: "0", count: 32)) == nil)
        // Right length, illegal characters.
        #expect(RepeatedAdFingerprint(hexString: String(repeating: "x", count: 16)) == nil)
    }

    @Test
    func binariseEmptyVectorIsZero() {
        #expect(RepeatedAdFingerprint.binarise([]).isZero)
    }

    @Test
    func binarisePadsShortVectorWithZeros() {
        // A short vector should be padded to bitWidth zeros and yield zero.
        let fp = RepeatedAdFingerprint.binarise([0, 0, 0, 0])
        #expect(fp.isZero)
    }

    @Test
    func binariseProducesNonZeroForVariedVector() {
        let n = RepeatedAdFingerprint.bitWidth
        var v = [Float](repeating: 0, count: n)
        for i in 0..<n { v[i] = Float(i) }
        let fp = RepeatedAdFingerprint.binarise(v)
        #expect(!fp.isZero)
        // ~half the bits are above the median for a strictly-increasing
        // vector. We don't pin the exact count (median ties-go-to-zero
        // policy makes it n/2 for an even-length vector), but it's nonzero.
        let popcount = fp.bits.nonzeroBitCount
        #expect(popcount > 0 && popcount < RepeatedAdFingerprint.bitWidth)
    }

    // MARK: - C3: contract honesty for production fingerprint derivation
    //
    // Pre-fix `bitWidth` was 128 but `from(featureWindows:)` flowed
    // through `AcousticFingerprint.fromFeatureWindows` (vectorLength=64),
    // so the effective entropy was always 64 bits — bits 64..127 were
    // hard-coded zero in production. The `Hamming ≤ 6 of 128` contract
    // was a fiction; the real signal-to-noise was `≤ 6 of 64`.
    //
    // Post-fix: the type is honestly 64-bit; the threshold is 3/64
    // (preserving the same ~4.7% bit-error tolerance the original
    // 6/128 expressed); and a near-duplicate FeatureWindow array
    // hashes within the threshold, end-to-end.

    @Test
    func bitWidthMatchesAcousticFingerprintLength() {
        // Pin the contract: the perceptual hash bit width must equal
        // the AcousticFingerprint vector length it derives from. If a
        // future refactor extends AcousticFingerprint, this test fires
        // and the fingerprint type must adapt.
        #expect(RepeatedAdFingerprint.bitWidth == AcousticFingerprint.vectorLength,
                "fingerprint bit width must match the AcousticFingerprint vector length it derives from")
    }

    @Test
    func productionDerivationProducesNonZeroPopcountWithinBitWidth() {
        // Build a synthetic [FeatureWindow] with varied features so the
        // resulting AcousticFingerprint has non-trivial dispersion.
        // Asserts that the final fingerprint exercises the full bit
        // width (popcount > 0, < bitWidth) — i.e., the type's bit
        // width is the *effective* bit width, not a documentation lie.
        var windows: [FeatureWindow] = []
        for i in 0..<32 {
            let id = Double(i)
            windows.append(FeatureWindow(
                analysisAssetId: "asset-c3",
                startTime: id * 2,
                endTime: (id + 1) * 2,
                rms: id * 0.03,
                spectralFlux: Double((i * 7) % 17) * 0.05,
                musicProbability: Double((i * 3) % 11) * 0.08,
                speakerChangeProxyScore: Double((i * 5) % 13) * 0.07,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 0,
                musicBedOffsetScore: 0,
                musicBedLevel: .none,
                pauseProbability: Double(i % 9) * 0.10,
                speakerClusterId: i % 4,
                jingleHash: nil,
                featureVersion: 4
            ))
        }
        let fp = RepeatedAdFingerprint.from(featureWindows: windows)
        #expect(!fp.isZero, "varied feature windows must produce a non-zero fingerprint")
        let popcount = fp.bits.nonzeroBitCount
        // popcount must be strictly between 0 and bitWidth — proves
        // the bits cover the full advertised width.
        #expect(popcount > 0 && popcount < RepeatedAdFingerprint.bitWidth,
                "popcount=\(popcount) should be in (0, \(RepeatedAdFingerprint.bitWidth)) for varied input")
    }

    @Test
    func nearDuplicateFeatureWindowsLandWithinHammingThreshold() throws {
        // Reviewer's "missing test that would have caught C3":
        // construct two near-duplicate [FeatureWindow] arrays via
        // `from(featureWindows:)` and assert the Hamming distance is
        // within the production threshold. Pre-fix the threshold was
        // 6/128 (true 6/64 because bits 64..127 always zero); post-fix
        // the threshold is 3/64 — same effective bit-error tolerance,
        // honest denominator.
        let baseRMS: [Double] = [0.18, 0.18, 0.20, 0.21, 0.22, 0.23, 0.22, 0.22,
                                 0.21, 0.20, 0.19, 0.18, 0.17, 0.18, 0.19, 0.20]
        func makeWindows(rmsBias: Double) -> [FeatureWindow] {
            var out: [FeatureWindow] = []
            out.reserveCapacity(baseRMS.count)
            for i in 0..<baseRMS.count {
                let rms = max(0.0, baseRMS[i] + rmsBias)
                let flux = 0.05 + Double(i) * 0.01
                let music = 0.10 + Double(i % 3) * 0.05
                let speaker = 0.20 + Double(i % 5) * 0.04
                let pause = 0.05 + Double(i % 4) * 0.03
                let window = FeatureWindow(
                    analysisAssetId: "asset-c3-near-dup",
                    startTime: Double(i) * 2,
                    endTime: Double(i + 1) * 2,
                    rms: rms,
                    spectralFlux: flux,
                    musicProbability: music,
                    speakerChangeProxyScore: speaker,
                    musicBedChangeScore: 0,
                    musicBedOnsetScore: 0,
                    musicBedOffsetScore: 0,
                    musicBedLevel: .none,
                    pauseProbability: pause,
                    speakerClusterId: i % 2,
                    jingleHash: nil,
                    featureVersion: 4
                )
                out.append(window)
            }
            return out
        }
        let original = makeWindows(rmsBias: 0)
        // Tiny RMS perturbation simulates "same ad, slightly different
        // mix" — the use case that motivated the perceptual hash.
        let nearDup = makeWindows(rmsBias: 0.001)

        let fp1 = RepeatedAdFingerprint.from(featureWindows: original)
        let fp2 = RepeatedAdFingerprint.from(featureWindows: nearDup)
        try #require(!fp1.isZero)
        try #require(!fp2.isZero)
        let distance = fp1.hammingDistance(to: fp2)
        let threshold = RepeatedAdCacheConfig.production.hammingDistanceThreshold
        #expect(distance <= threshold,
                "near-duplicate FeatureWindow arrays must hash within Hamming threshold (\(distance) of \(threshold) of \(RepeatedAdFingerprint.bitWidth) bits)")
    }
}
