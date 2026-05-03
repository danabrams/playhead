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
        hammingDistanceThreshold: 6,
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

    /// Build a 128-bit fingerprint that has bit `i` flipped (relative to
    /// all-zeros). Useful for asserting Hamming distance at exact
    /// boundaries.
    static func fpWithBitsFlipped(_ indices: [Int]) -> RepeatedAdFingerprint {
        var bits = [Bool](repeating: false, count: 128)
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

    // MARK: - 4. cacheLookupHitOnHammingDistance6

    @Test
    func cacheLookupHitOnHammingDistance6() async throws {
        let (service, _, _) = Self.makeService()
        try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1, boundaryEnd: 2, confidence: 0.95
        )
        // Flip exactly 6 bits (in addition to the always-on bit 0 in
        // baseFingerprint) — Hamming distance becomes exactly 6.
        let probe = Self.fpWithBitsFlipped([10, 20, 30, 40, 50, 60])
        // Sanity-check our test setup: bits 10/20/30/40/50/60 are all
        // off in baseFingerprint, so flipping them yields exactly 6
        // bit differences.
        let dist = Self.baseFingerprint.hammingDistance(to: probe)
        #expect(dist == 6, "fixture bug: expected distance 6, got \(dist)")
        let outcome = try await service.lookup(showId: "show-1", fingerprint: probe)
        guard case .hit = outcome else {
            Issue.record("distance==6 must HIT (≤ threshold), got \(outcome)")
            return
        }
    }

    // MARK: - 5. cacheLookupMissOnHammingDistance7

    @Test
    func cacheLookupMissOnHammingDistance7() async throws {
        let (service, _, _) = Self.makeService()
        try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1, boundaryEnd: 2, confidence: 0.95
        )
        let probe = Self.fpWithBitsFlipped([10, 20, 30, 40, 50, 60, 70])
        #expect(Self.baseFingerprint.hammingDistance(to: probe) == 7)
        let outcome = try await service.lookup(showId: "show-1", fingerprint: probe)
        if case .hit = outcome {
            Issue.record("distance==7 must MISS (> threshold)")
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
            hammingDistanceThreshold: 6,
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
            hammingDistanceThreshold: 6,
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
            hammingDistanceThreshold: 6,
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
        #expect(RepeatedAdFingerprint.zero.high == 0)
        #expect(RepeatedAdFingerprint.zero.low == 0)
        #expect(RepeatedAdFingerprint.zero.isZero)
    }

    @Test
    func selfDistanceIsZero() {
        let bits = (0..<128).map { ($0 % 3) == 0 }
        let fp = RepeatedAdFingerprint.fromBits(bits)
        #expect(fp.hammingDistance(to: fp) == 0)
    }

    @Test
    func distanceIsSymmetric() {
        let a = RepeatedAdFingerprint.fromBits((0..<128).map { ($0 % 5) == 0 })
        let b = RepeatedAdFingerprint.fromBits((0..<128).map { ($0 % 7) == 0 })
        #expect(a.hammingDistance(to: b) == b.hammingDistance(to: a))
    }

    @Test
    func flipExactlyKBitsGivesDistanceK() {
        var bits = [Bool](repeating: false, count: 128)
        bits[0] = true
        let base = RepeatedAdFingerprint.fromBits(bits)
        for k in [1, 6, 7, 64, 127] {
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
        let fp = RepeatedAdFingerprint(high: 0x0123456789abcdef, low: 0xfedcba9876543210)
        let s = fp.hexString
        #expect(s.count == 32)
        let parsed = RepeatedAdFingerprint(hexString: s)
        #expect(parsed == fp)
    }

    @Test
    func malformedHexReturnsNil() {
        #expect(RepeatedAdFingerprint(hexString: "") == nil)
        #expect(RepeatedAdFingerprint(hexString: "abc") == nil)
        #expect(RepeatedAdFingerprint(hexString: String(repeating: "x", count: 32)) == nil)
    }

    @Test
    func binariseEmptyVectorIsZero() {
        #expect(RepeatedAdFingerprint.binarise([]).isZero)
    }

    @Test
    func binarisePadsShortVectorWithZeros() {
        // A vector of length 4 should be padded to 128 zeros and yield zero.
        let fp = RepeatedAdFingerprint.binarise([0, 0, 0, 0])
        #expect(fp.isZero)
    }

    @Test
    func binariseProducesNonZeroForVariedVector() {
        var v = [Float](repeating: 0, count: 128)
        for i in 0..<128 { v[i] = Float(i) }
        let fp = RepeatedAdFingerprint.binarise(v)
        #expect(!fp.isZero)
        // ~half the bits are above the median for a strictly-increasing
        // vector. We don't pin the exact count (median ties-go-to-zero
        // policy makes it 64 for an even-length vector), but it's nonzero.
        let popcount = fp.high.nonzeroBitCount + fp.low.nonzeroBitCount
        #expect(popcount > 0 && popcount < 128)
    }
}
