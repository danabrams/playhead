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
//   18. source boundaries never cross episode boundaries
//   19. cache confidence never becomes automatic decision authority
//
// Production-wiring tests (#11, #12) live in
// `RepeatedAdCacheWiringTests.swift` so they can target `AdDetectionService`
// directly.

import Foundation
import Testing
@testable import Playhead

/// Existing cache mechanics tests use an explicitly confirmed source so the
/// concise calls remain focused on eviction/matching behavior. Production has
/// no provenance-free overload.
private extension RepeatedAdCacheService {
    @discardableResult
    func store(
        showId: String,
        fingerprint: RepeatedAdFingerprint,
        boundaryStart: Double,
        boundaryEnd: Double,
        confidence: Double
    ) async throws -> Bool {
        try await store(
            showId: showId,
            fingerprint: fingerprint,
            boundaryStart: boundaryStart,
            boundaryEnd: boundaryEnd,
            confidence: confidence,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "unit-test-asset",
            sourceWindowId: "unit-test-\(fingerprint.hexString)"
        )
    }
}

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

    final class ScriptedClock: @unchecked Sendable {
        private let lock = NSLock()
        private var values: [Date]

        init(_ values: [Date]) {
            self.values = values
        }

        func now() -> Date {
            lock.lock()
            defer { lock.unlock() }
            return values.isEmpty
                ? Date(timeIntervalSince1970: -1)
                : values.removeFirst()
        }
    }

    final class IntentProbe: @unchecked Sendable {
        private let lock = NSLock()
        private var intent: Bool
        private var autoDisabledClearCount = 0

        init(_ intent: Bool) {
            self.intent = intent
        }

        func setIntent(_ value: Bool) {
            lock.lock()
            intent = value
            lock.unlock()
        }

        func currentIntent() -> Bool {
            lock.lock()
            defer { lock.unlock() }
            return intent
        }

        func clearAutoDisabled() {
            lock.lock()
            autoDisabledClearCount += 1
            lock.unlock()
        }

        func clearCount() -> Int {
            lock.lock()
            defer { lock.unlock() }
            return autoDisabledClearCount
        }
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

    @Test("lookup prefers the closest fingerprint before the newest match")
    func cacheLookupPrefersClosestFingerprint() async throws {
        let clock = MutableClock()
        let (service, _, _) = Self.makeService(clock: clock)
        let exact = Self.baseFingerprint
        let newerNearMatch = Self.fpWithBitsFlipped([1])
        #expect(exact.hammingDistance(to: newerNearMatch) == 1)

        clock.value = Date(timeIntervalSince1970: 10)
        #expect(try await service.store(
            showId: "show-nearest",
            fingerprint: exact,
            boundaryStart: 10,
            boundaryEnd: 40,
            confidence: 0.95
        ))
        clock.value = Date(timeIntervalSince1970: 20)
        #expect(try await service.store(
            showId: "show-nearest",
            fingerprint: newerNearMatch,
            boundaryStart: 100,
            boundaryEnd: 130,
            confidence: 0.95
        ))

        guard case .hit(let selected) = try await service.lookup(
            showId: "show-nearest",
            fingerprint: exact
        ) else {
            Issue.record("expected an exact cache hit")
            return
        }
        #expect(selected.fingerprint == exact)
        #expect(selected.boundaryStart == 10)
        #expect(selected.boundaryEnd == 40)
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

    @Test("concurrent admissions preserve exact capacity without over-eviction")
    func concurrentAdmissionsEnforceCapacityAtomically() async throws {
        let config = RepeatedAdCacheConfig(
            storeConfidenceThreshold: Self.testConfig.storeConfidenceThreshold,
            hammingDistanceThreshold:
                Self.testConfig.hammingDistanceThreshold,
            perShowCap: 2,
            globalCap: 2,
            entryMaxAge: Self.testConfig.entryMaxAge,
            autoDisableWindow: Self.testConfig.autoDisableWindow,
            autoDisableHitRateFloor:
                Self.testConfig.autoDisableHitRateFloor,
            autoDisableMinSamples: Self.testConfig.autoDisableMinSamples
        )
        let (service, _, storage) = Self.makeService(config: config)

        await withTaskGroup(of: Bool.self) { group in
            for index in 1...8 {
                group.addTask {
                    (try? await service.store(
                        showId: "show-concurrent-cap",
                        fingerprint: Self.fpWithBitsFlipped([index]),
                        boundaryStart: Double(index),
                        boundaryEnd: Double(index + 1),
                        confidence: 0.95
                    )) ?? false
                }
            }
            for await admitted in group {
                #expect(admitted)
            }
        }

        #expect(
            try await storage.count(showId: "show-concurrent-cap") == 2
        )
        #expect(try await storage.totalCount() == 2)
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

    @Test("a backward wall clock cannot regress cache recency")
    func cacheHitKeepsLastSeenAtMonotonic() async throws {
        let clock = MutableClock()
        clock.value = Date(timeIntervalSince1970: 100)
        let (service, _, storage) = Self.makeService(clock: clock)
        #expect(try await service.store(
            showId: "show-monotonic-touch",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 2,
            confidence: 0.95
        ))

        clock.value = Date(timeIntervalSince1970: 50)
        let outcome = try await service.lookup(
            showId: "show-monotonic-touch",
            fingerprint: Self.baseFingerprint
        )
        let hit = try #require({
            if case .hit(let entry) = outcome { return entry }
            return nil
        }())
        #expect(hit.lastSeenAt == Date(timeIntervalSince1970: 100))
        let persisted = try #require(
            try await storage.fetchAll(
                showId: "show-monotonic-touch"
            ).first
        )
        #expect(persisted.lastSeenAt == Date(timeIntervalSince1970: 100))
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
            lastSeenAt: Date(timeIntervalSince1970: 100),
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "asset-low",
            sourceWindowId: "window-low"
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

    @Test
    func lookupQuarantinesLegacyUnconfirmedRows() async throws {
        let (service, _, storage) = Self.makeService()
        try await storage.upsert(.init(
            showId: "show-legacy",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 30,
            confidence: 1,
            lastSeenAt: Date(timeIntervalSince1970: 100)
        ))

        let outcome = try await service.lookup(
            showId: "show-legacy",
            fingerprint: Self.baseFingerprint
        )
        if case .hit = outcome {
            Issue.record("legacy unconfirmed recurrence row must not promote")
        }
        #expect(try await storage.totalCount() == 1)
    }

    @Test
    func storeRejectsMismatchedLearningLifecycle() async throws {
        let (service, _, storage) = Self.makeService()
        let stored = try await service.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 30,
            confidence: 1,
            learningSource: .manualSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "asset-1",
            sourceWindowId: "window-1"
        )
        #expect(stored == false)
        #expect(try await storage.totalCount() == 0)
    }

    @Test("store and lookup reject noncanonical recurrence identities")
    func noncanonicalRecurrenceIdentitiesFailClosed() async throws {
        for (showId, sourceAssetId, sourceWindowId, producerRevision) in [
            (
                " show-identity ",
                "asset-identity",
                "window-identity",
                "revision"
            ),
            (
                "show-identity",
                " asset-identity ",
                "window-identity",
                "revision"
            ),
            (
                "show-identity",
                "asset-identity",
                " window-identity ",
                "revision"
            ),
            (
                "show-identity\u{0}other",
                "asset-identity",
                "window-identity",
                "revision"
            ),
            (
                "show-identity",
                "asset-identity\u{0}other",
                "window-identity",
                "revision"
            ),
            (
                "show-identity",
                "asset-identity",
                "window-identity\u{0}other",
                "revision"
            ),
            (
                "show-identity",
                "asset-identity",
                "window-identity",
                "revision\u{0}other"
            ),
        ] {
            let (service, _, storage) = Self.makeService()
            #expect(try await service.store(
                showId: showId,
                fingerprint: Self.baseFingerprint,
                boundaryStart: 1,
                boundaryEnd: 30,
                confidence: 1,
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: sourceAssetId,
                sourceWindowId: sourceWindowId,
                producerRevision: producerRevision
            ) == false)
            #expect(try await storage.totalCount() == 0)
        }

        let (service, _, _) = Self.makeService()
        #expect(try await service.store(
            showId: "show-identity",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 30,
            confidence: 1
        ))
        if case .hit = try await service.lookup(
            showId: " show-identity ",
            fingerprint: Self.baseFingerprint
        ) {
            Issue.record("noncanonical lookup identity must not be sanitized")
        }
    }

    @Test("correction tombstones the matching radius without deleting nearby audit rows")
    func correctionTombstonesRadiusWithoutDeletingNearbyRows() async throws {
        let (service, _, storage) = Self.makeService()
        let near = Self.fpWithBitsFlipped([10, 11, 12])
        let bridge = Self.fpWithBitsFlipped([10, 11, 12, 20, 21, 22])
        #expect(
            Self.baseFingerprint.hammingDistance(to: near)
                == Self.testConfig.hammingDistanceThreshold
        )
        #expect(
            near.hammingDistance(to: bridge)
                == Self.testConfig.hammingDistanceThreshold
        )
        #expect(
            Self.baseFingerprint.hammingDistance(to: bridge)
                > Self.testConfig.hammingDistanceThreshold
        )
        for (showId, fingerprint) in [
            ("show-1", Self.baseFingerprint),
            ("show-1", near),
            ("show-2", Self.baseFingerprint),
        ] {
            _ = try await service.store(
                showId: showId,
                fingerprint: fingerprint,
                boundaryStart: 1,
                boundaryEnd: 30,
                confidence: 1
            )
        }

        let revoked = try await service.revokeMatches(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            sourceAssetId: "corrected-asset",
            sourceWindowId: "corrected-window",
            source: .manualVeto
        )

        #expect(revoked == 1)
        #expect(
            try await storage.count(showId: "show-1") == 1,
            "the nearby row remains auditable instead of being guessed as the corrected source"
        )
        #expect(try await storage.count(showId: "show-2") == 1)
        for probe in [Self.baseFingerprint, near, bridge] {
            if case .hit = try await service.lookup(
                showId: "show-1",
                fingerprint: probe
            ) {
                Issue.record(
                    "a tombstoned recurrence neighborhood must not resurrect through a retained bridge row"
                )
            }
        }
        #expect(try await service.store(
            showId: "show-1",
            fingerprint: near,
            boundaryStart: 2,
            boundaryEnd: 31,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "later-explicit-asset",
            sourceWindowId: "later-explicit-window"
        ) == false)
        guard case .hit = try await service.lookup(
            showId: "show-2",
            fingerprint: Self.baseFingerprint
        ) else {
            Issue.record("fingerprint tombstones must remain exactly show-scoped")
            return
        }
    }

    @Test("malformed correction show cannot retarget a creative tombstone")
    func malformedCorrectionShowFailsClosedOnCreativeScope() async throws {
        let (service, _, _) = Self.makeService()
        #expect(try await service.store(
            showId: "show-correction-scope",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 30,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "existing-correction-asset",
            sourceWindowId: "existing-correction-window"
        ))

        #expect(try await service.revokeMatches(
            showId: " show-correction-scope ",
            fingerprint: Self.baseFingerprint,
            sourceAssetId: "different-correction-asset",
            sourceWindowId: "different-correction-window",
            source: .manualVeto
        ) == 0)
        guard case .hit = try await service.lookup(
            showId: "show-correction-scope",
            fingerprint: Self.baseFingerprint
        ) else {
            Issue.record(
                "malformed show identity must not revoke a canonical show's creative"
            )
            return
        }
    }

    @Test("source provenance revokes an existing row without show or fingerprint")
    func correctionRevokesExactSourceWhenRefingerprintUnavailable() async throws {
        let (service, _, storage) = Self.makeService()
        #expect(try await service.store(
            showId: "show-source-fallback",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 30,
            confidence: 1,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "source-fallback-asset",
            sourceWindowId: "source-fallback-window"
        ))

        let revoked = try await service.revokeMatches(
            showId: nil,
            fingerprint: nil,
            sourceAssetId: "source-fallback-asset",
            sourceWindowId: "source-fallback-window",
            source: .manualVeto
        )

        #expect(revoked == 1)
        #expect(try await storage.totalCount() == 0)
        if case .hit = try await service.lookup(
            showId: "show-source-fallback",
            fingerprint: Self.baseFingerprint
        ) {
            Issue.record("exact-source correction must remove the learned row")
        }
    }

    @Test("consumed learning never downgrades an explicit cache row")
    func explicitLifecycleIsMonotonicInMemory() async throws {
        let (service, _, storage) = Self.makeService()
        #expect(try await service.store(
            showId: "show-lifecycle",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 30,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "explicit-asset",
            sourceWindowId: "explicit-window"
        ))
        #expect(try await service.store(
            showId: "show-lifecycle",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 2,
            boundaryEnd: 31,
            confidence: 0.99,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "consumed-asset",
            sourceWindowId: "consumed-window"
        ) == false)

        let retained = try #require(
            try await storage.fetchAll(showId: "show-lifecycle").first
        )
        #expect(retained.learningSource == .userMarkedAd)
        #expect(retained.learningLifecycle == .explicitConfirmation)
        #expect(retained.sourceAssetId == "explicit-asset")
        #expect(retained.sourceWindowId == "explicit-window")
    }

    @Test("stronger lifecycle cannot regress the durable LRU clock")
    func strongerLifecycleKeepsRecencyMonotonic() async throws {
        let (service, clock, storage) = Self.makeService()
        clock.value = Date(timeIntervalSince1970: 200)
        #expect(try await service.store(
            showId: "show-lifecycle-clock",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 10,
            boundaryEnd: 40,
            confidence: 0.95,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "consumed-clock-asset",
            sourceWindowId: "consumed-clock-window"
        ))

        clock.value = Date(timeIntervalSince1970: 100)
        #expect(try await service.store(
            showId: "show-lifecycle-clock",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 20,
            boundaryEnd: 50,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "explicit-clock-asset",
            sourceWindowId: "explicit-clock-window"
        ))

        let retained = try #require(
            try await storage.fetchAll(showId: "show-lifecycle-clock").first
        )
        #expect(retained.lastSeenAt == Date(timeIntervalSince1970: 200))
        #expect(retained.learningLifecycle == .explicitConfirmation)
        #expect(retained.boundaryStart == 20)
        #expect(retained.sourceAssetId == "explicit-clock-asset")
    }

    @Test("older learning cannot overwrite newer same-lifecycle provenance")
    func sameLifecycleRecencyIsMonotonicInMemory() async throws {
        let (service, clock, storage) = Self.makeService()
        clock.value = Date(timeIntervalSince1970: 200)
        #expect(try await service.store(
            showId: "show-same-lifecycle",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 10,
            boundaryEnd: 40,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "newer-asset",
            sourceWindowId: "newer-window"
        ))

        clock.value = Date(timeIntervalSince1970: 100)
        #expect(try await service.store(
            showId: "show-same-lifecycle",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 20,
            boundaryEnd: 50,
            confidence: 1,
            learningSource: .confirmedSuggestion,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "older-asset",
            sourceWindowId: "older-window"
        ) == false)

        let retained = try #require(
            try await storage.fetchAll(
                showId: "show-same-lifecycle"
            ).first
        )
        #expect(retained.lastSeenAt == Date(timeIntervalSince1970: 200))
        #expect(retained.sourceAssetId == "newer-asset")
        #expect(retained.sourceWindowId == "newer-window")
        #expect(retained.boundaryStart == 10)
        #expect(retained.boundaryEnd == 40)
    }

    @Test("exact source revocation preserves a same-ID geometry replacement")
    func correctionTombstoneIsExactGeometryScoped() async throws {
        let (service, _, storage) = Self.makeService()
        let replacementFingerprint = Self.fpWithBitsFlipped(
            Array(10...20)
        )
        #expect(try await service.store(
            showId: "show-exact-geometry",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 10,
            boundaryEnd: 40,
            confidence: 1,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "asset-exact-geometry",
            sourceWindowId: "reused-window-id"
        ))
        #expect(try await service.store(
            showId: "show-exact-geometry",
            fingerprint: replacementFingerprint,
            boundaryStart: 50,
            boundaryEnd: 80,
            confidence: 1,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "asset-exact-geometry",
            sourceWindowId: "reused-window-id"
        ))

        #expect(try await service.revokeMatches(
            showId: nil,
            fingerprint: nil,
            sourceAssetId: "asset-exact-geometry",
            sourceWindowId: "reused-window-id",
            sourceStartTime: 10,
            sourceEndTime: 40,
            source: .manualVeto
        ) == 1)
        #expect(
            try await storage.fetchAll(showId: "show-exact-geometry")
                .map(\.fingerprint) == [replacementFingerprint]
        )

        #expect(try await service.store(
            showId: "show-exact-geometry",
            fingerprint: Self.fpWithBitsFlipped(Array(30...40)),
            boundaryStart: 10,
            boundaryEnd: 40,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "asset-exact-geometry",
            sourceWindowId: "reused-window-id"
        ) == false)
        #expect(try await service.store(
            showId: "show-exact-geometry",
            fingerprint: Self.fpWithBitsFlipped(Array(45...55)),
            boundaryStart: 90,
            boundaryEnd: 120,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "asset-exact-geometry",
            sourceWindowId: "reused-window-id"
        ))
    }

    @Test("signed-zero geometry cannot evade an exact source tombstone")
    func signedZeroGeometrySharesRevocationIdentity() async throws {
        let (service, _, storage) = Self.makeService()
        #expect(try await service.store(
            showId: "show-signed-zero",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 0.0,
            boundaryEnd: 30,
            confidence: 1,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "asset-signed-zero",
            sourceWindowId: "window-signed-zero"
        ))

        #expect(try await service.revokeMatches(
            showId: nil,
            fingerprint: nil,
            sourceAssetId: "asset-signed-zero",
            sourceWindowId: "window-signed-zero",
            sourceStartTime: -0.0,
            sourceEndTime: 30,
            source: .manualVeto
        ) == 1)
        #expect(
            try await storage.fetchAll(showId: "show-signed-zero").isEmpty
        )
        #expect(try await service.store(
            showId: "show-signed-zero",
            fingerprint: Self.fpWithBitsFlipped(Array(20...40)),
            boundaryStart: 0.0,
            boundaryEnd: 30,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "asset-signed-zero",
            sourceWindowId: "window-signed-zero"
        ) == false)

        let legacyStorage = InMemoryRepeatedAdCacheStorage()
        let legacyKey = try #require(
            RecurrenceMaterialIdentity
                .legacyNegativeZeroTombstoneWindowKey(
                    sourceWindowId: "legacy-window-signed-zero",
                    sourceStartTime: -0.0,
                    sourceEndTime: 30
                )
        )
        #expect(try await legacyStorage.recordRevocation(
            sourceAssetId: "legacy-asset-signed-zero",
            sourceWindowId: legacyKey,
            source: .manualVeto,
            at: Date(timeIntervalSince1970: 10)
        ))
        let reopened = RepeatedAdCacheService(
            config: Self.testConfig,
            storage: legacyStorage
        )
        #expect(try await reopened.store(
            showId: "show-legacy-signed-zero",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 0.0,
            boundaryEnd: 30,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "legacy-asset-signed-zero",
            sourceWindowId: "legacy-window-signed-zero"
        ) == false)
    }

    @Test("source revocation wins a later learning race and survives service recreation")
    func correctionTombstoneSuppressesDelayedLearning() async throws {
        let storage = InMemoryRepeatedAdCacheStorage()
        let first = RepeatedAdCacheService(
            config: Self.testConfig,
            storage: storage
        )
        _ = try await first.revokeMatches(
            showId: nil,
            fingerprint: nil,
            sourceAssetId: "race-asset",
            sourceWindowId: "race-window",
            source: .listenRevert
        )

        let recreated = RepeatedAdCacheService(
            config: Self.testConfig,
            storage: storage
        )
        let poisoned = try await recreated.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 30,
            confidence: 1,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "race-asset",
            sourceWindowId: "race-window"
        )
        #expect(poisoned == false)
        #expect(try await storage.totalCount() == 0)

        let independent = try await recreated.store(
            showId: "show-1",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 30,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "different-asset",
            sourceWindowId: "different-window"
        )
        #expect(independent == true)
    }

    @Test(
        "correction wins a store suspended after its tombstone snapshot",
        .timeLimit(.minutes(1))
    )
    func correctionWinsReentrantStoreRace() async throws {
        let (service, _, storage) = Self.makeService()
        let near = Self.fpWithBitsFlipped([1])
        let gate = RepeatedAdCacheAsyncGate()
        await service._setStoreRevocationSnapshotBarrierForTesting {
            await gate.wait()
        }
        let delayedStore = Task {
            try await service.store(
                showId: "show-store-race",
                fingerprint: near,
                boundaryStart: 1,
                boundaryEnd: 30,
                confidence: 1,
                learningSource: .consumedAutoSkip,
                learningLifecycle: .consumed,
                sourceAssetId: "delayed-store-asset",
                sourceWindowId: "delayed-store-window"
            )
        }
        await gate.waitUntilStarted()
        _ = try await service.revokeMatches(
            showId: "show-store-race",
            fingerprint: Self.baseFingerprint,
            sourceAssetId: "corrected-store-asset",
            sourceWindowId: "corrected-store-window",
            source: .manualVeto
        )
        await gate.release()

        #expect(try await delayedStore.value == false)
        #expect(
            try await storage.totalCount() == 0,
            "a writer using a pre-correction tombstone snapshot must not persist"
        )
        await service._setStoreRevocationSnapshotBarrierForTesting(nil)
    }

    @Test(
        "disable wins a store suspended after its evidence snapshot",
        .timeLimit(.minutes(1))
    )
    func disableWinsReentrantStoreRace() async throws {
        let (service, _, storage) = Self.makeService()
        let gate = RepeatedAdCacheAsyncGate()
        await service._setStoreRevocationSnapshotBarrierForTesting {
            await gate.wait()
        }
        let delayedStore = Task {
            try await service.store(
                showId: "show-disable-race",
                fingerprint: Self.baseFingerprint,
                boundaryStart: 1,
                boundaryEnd: 30,
                confidence: 1,
                learningSource: .consumedAutoSkip,
                learningLifecycle: .consumed,
                sourceAssetId: "delayed-disable-asset",
                sourceWindowId: "delayed-disable-window"
            )
        }
        await gate.waitUntilStarted()
        await service.setEnabled(false)
        await gate.release()

        #expect(try await delayedStore.value == false)
        #expect(
            try await storage.totalCount() == 0,
            "a writer using a pre-disable snapshot must not repopulate the cache"
        )
        #expect(await service.isEnabled() == false)
        await service._setStoreRevocationSnapshotBarrierForTesting(nil)
    }

    @Test(
        "stale post-disable writer cannot delete a newer explicit revision",
        .timeLimit(.minutes(1))
    )
    func staleWriterCleanupPreservesNewerSource() async throws {
        let (service, _, storage) = Self.makeService()
        let gate = RepeatedAdCacheAsyncGate()
        await service._setStorePersistenceBarrierForTesting {
            await gate.wait()
        }
        let delayedStore = Task {
            try await service.store(
                showId: "show-reenable-race",
                fingerprint: Self.baseFingerprint,
                boundaryStart: 1,
                boundaryEnd: 30,
                confidence: 1,
                learningSource: .consumedAutoSkip,
                learningLifecycle: .consumed,
                sourceAssetId: "stale-source-asset",
                sourceWindowId: "stale-source-window"
            )
        }
        await gate.waitUntilStarted()

        await service.setEnabled(false)
        await service.setEnabled(true)
        await service._setStorePersistenceBarrierForTesting(nil)
        #expect(try await service.store(
            showId: "show-reenable-race",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 2,
            boundaryEnd: 31,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "stale-source-asset",
            sourceWindowId: "stale-source-window"
        ))
        await gate.release()

        #expect(try await delayedStore.value == false)
        let retained = try #require(
            try await storage.fetchAll(showId: "show-reenable-race").first
        )
        #expect(retained.boundaryStart == 2)
        #expect(retained.boundaryEnd == 31)
        #expect(retained.learningSource == .userMarkedAd)
        #expect(retained.learningLifecycle == .explicitConfirmation)
    }

    @Test(
        "stale cleanup cannot delete a byte-equivalent newer write",
        .timeLimit(.minutes(1))
    )
    func staleWriterCleanupUsesExactProducerRevision() async throws {
        let (service, _, storage) = Self.makeService()
        let gate = RepeatedAdCacheAsyncGate()
        await service._setStorePersistenceBarrierForTesting {
            await gate.wait()
        }
        let delayedStore = Task {
            try await service.store(
                showId: "show-exact-producer-revision",
                fingerprint: Self.baseFingerprint,
                boundaryStart: 1,
                boundaryEnd: 30,
                confidence: 1,
                learningSource: .consumedAutoSkip,
                learningLifecycle: .consumed,
                sourceAssetId: "same-material-asset",
                sourceWindowId: "same-material-window"
            )
        }
        await gate.waitUntilStarted()

        await service.setEnabled(false)
        await service.setEnabled(true)
        await service._setStorePersistenceBarrierForTesting(nil)
        #expect(try await service.store(
            showId: "show-exact-producer-revision",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 30,
            confidence: 1,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "same-material-asset",
            sourceWindowId: "same-material-window"
        ))
        let newerRevision = try #require(
            try await storage.fetchAll(
                showId: "show-exact-producer-revision"
            ).first?.producerRevision
        )
        await gate.release()

        #expect(try await delayedStore.value == false)
        let retained = try #require(
            try await storage.fetchAll(
                showId: "show-exact-producer-revision"
            ).first
        )
        #expect(retained.producerRevision == newerRevision)
        #expect(retained.sourceAssetId == "same-material-asset")
        #expect(retained.sourceWindowId == "same-material-window")
    }

    @Test(
        "latest disable wins a re-enable suspended during durable cleanup",
        .timeLimit(.minutes(1))
    )
    func latestDisableWinsReentrantEnable() async throws {
        let (service, _, _) = Self.makeService(initiallyEnabled: false)
        let gate = RepeatedAdCacheAsyncGate()
        await service._setMaintenanceClearBarrierForTesting {
            await gate.wait()
        }
        let enabling = Task {
            await service.setEnabled(true)
        }
        await gate.waitUntilStarted()
        let disabling = Task {
            await service.setEnabled(false)
        }
        await service._waitUntilLifecycleCleanupIsWaitingForTesting()
        await gate.release()
        await enabling.value
        await disabling.value

        #expect(await service.isEnabled() == false)
        #expect(await service.currentDisableReason() == .userKillSwitch)
        await service._setMaintenanceClearBarrierForTesting(nil)
    }

    @Test(
        "runtime reconciliation applies a newer disable before clearing auto-disable",
        .timeLimit(.minutes(1))
    )
    func runtimeReconciliationPreservesNewerDisableIntent() async throws {
        let (service, _, _) = Self.makeService(initiallyEnabled: false)
        let gate = RepeatedAdCacheAsyncGate()
        await service._setMaintenanceClearBarrierForTesting {
            await gate.wait()
        }
        let intent = IntentProbe(true)
        let reconciliation = Task {
            await RepeatedAdCacheFeatureFlag.reconcileUserIntent(
                true,
                cache: service,
                currentIntent: { intent.currentIntent() },
                clearAutoDisabled: { intent.clearAutoDisabled() }
            )
        }

        await gate.waitUntilStarted()
        intent.setIntent(false)
        await gate.release()

        #expect(await reconciliation.value == false)
        #expect(await service.isEnabled() == false)
        #expect(await service.currentDisableReason() == .userKillSwitch)
        #expect(intent.clearCount() == 0)
        await service._setMaintenanceClearBarrierForTesting(nil)
    }

    @Test("runtime clears auto-disable only after successful re-enable")
    func runtimeReconciliationClearsAutoDisableAfterSuccess() async {
        let (service, _, _) = Self.makeService(initiallyEnabled: false)
        let intent = IntentProbe(true)

        #expect(
            await RepeatedAdCacheFeatureFlag.reconcileUserIntent(
                true,
                cache: service,
                currentIntent: { intent.currentIntent() },
                clearAutoDisabled: { intent.clearAutoDisabled() }
            )
        )
        #expect(await service.isEnabled())
        #expect(intent.clearCount() == 1)
    }

    @Test(
        "an overlapping clear cannot strand re-enable intent",
        .timeLimit(.minutes(1))
    )
    func overlappingClearDoesNotStrandReEnable() async throws {
        let (service, _, _) = Self.makeService(initiallyEnabled: false)
        let enableGate = RepeatedAdCacheAsyncGate()
        let clearGate = RepeatedAdCacheAsyncGate()
        let sequencer = RepeatedAdCacheSequencedGate(
            first: enableGate,
            second: clearGate
        )
        await service._setMaintenanceClearBarrierForTesting {
            await sequencer.wait()
        }

        let enabling = Task {
            await service.setEnabled(true)
        }
        await enableGate.waitUntilStarted()
        let clearing = Task {
            try await service.clear()
        }
        await clearGate.waitUntilStarted()

        await enableGate.release()
        await enabling.value

        await clearGate.release()
        try await clearing.value
        await service.setEnabled(true)

        #expect(await service.isEnabled())
        #expect(await service.currentDisableReason() == nil)
        await service._setMaintenanceClearBarrierForTesting(nil)
    }

    @Test(
        "newer re-enable cancels stale auto-disable cleanup and callback",
        .timeLimit(.minutes(1))
    )
    func reEnableWinsSuspendedAutoDisableCleanup() async throws {
        final class CallbackCounter: @unchecked Sendable {
            private let lock = NSLock()
            private var value = 0

            func increment() {
                lock.lock()
                value += 1
                lock.unlock()
            }

            func snapshot() -> Int {
                lock.lock()
                defer { lock.unlock() }
                return value
            }
        }

        let config = RepeatedAdCacheConfig(
            storeConfidenceThreshold: 0.85,
            hammingDistanceThreshold: 3,
            perShowCap: 3,
            globalCap: 5,
            entryMaxAge: 90 * 24 * 60 * 60,
            autoDisableWindow: 14 * 24 * 60 * 60,
            autoDisableHitRateFloor: 0.5,
            autoDisableMinSamples: 1
        )
        let callbackCounter = CallbackCounter()
        let storage = InMemoryRepeatedAdCacheStorage()
        let service = RepeatedAdCacheService(
            config: config,
            storage: storage,
            onAutoDisable: { _, _ in callbackCounter.increment() }
        )
        let gate = RepeatedAdCacheAsyncGate()
        await service._setLifecycleCleanupBarrierForTesting {
            await gate.wait()
        }

        let staleAutoDisable = Task {
            try await service.recordOutcome(hit: false)
        }
        await gate.waitUntilStarted()

        let reEnabling = Task {
            await service.setEnabled(true)
        }
        await service._waitUntilLifecycleCleanupIsWaitingForTesting()
        await gate.release()
        try await staleAutoDisable.value
        await reEnabling.value
        try await service.recordOutcome(hit: true)

        #expect(await service.isEnabled())
        #expect(await service.currentDisableReason() == nil)
        #expect(callbackCounter.snapshot() == 0)
        let snapshot = try await service.currentHitRateSnapshot()
        #expect(snapshot.totalSamples == 1)
        #expect(snapshot.hitCount == 1)
        await service._setLifecycleCleanupBarrierForTesting(nil)
    }

    @Test(
        "explicit clear rejects reentrant writers until both stores are empty",
        .timeLimit(.minutes(1))
    )
    func explicitClearWinsReentrantStore() async throws {
        let (service, _, storage) = Self.makeService()
        #expect(try await service.store(
            showId: "show-clear-race",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 30,
            confidence: 1
        ))

        let gate = RepeatedAdCacheAsyncGate()
        await service._setMaintenanceClearBarrierForTesting {
            await gate.wait()
        }
        let clearing = Task {
            try await service.clear()
        }
        await gate.waitUntilStarted()
        #expect(try await service.store(
            showId: "show-clear-race",
            fingerprint: Self.fpWithBitsFlipped([1]),
            boundaryStart: 2,
            boundaryEnd: 31,
            confidence: 1
        ) == false)
        await gate.release()
        try await clearing.value

        #expect(try await storage.totalCount() == 0)
        await service._setMaintenanceClearBarrierForTesting(nil)
    }

    @Test(
        "explicit clear waits for a suspended outcome writer",
        .timeLimit(.minutes(1))
    )
    func explicitClearWinsReentrantOutcomeWrite() async throws {
        let (service, _, storage) = Self.makeService()
        let appendGate = RepeatedAdCacheAsyncGate()
        await storage._setAppendOutcomeBarrierForTesting {
            await appendGate.wait()
        }
        let recording = Task {
            try await service.recordOutcome(hit: true)
        }
        await appendGate.waitUntilStarted()

        let clearing = Task {
            try await service.clear()
        }
        await service._waitUntilOutcomeDrainIsWaitingForTesting()
        await appendGate.release()
        try await recording.value
        try await clearing.value

        #expect(
            try await storage.fetchOutcomes(
                newerThan: Date(timeIntervalSince1970: 0)
            ).isEmpty
        )
        await storage._setAppendOutcomeBarrierForTesting(nil)
    }

    @Test(
        "overlapping explicit clears keep writers fenced until both finish",
        .timeLimit(.minutes(1))
    )
    func overlappingExplicitClearsKeepWriteFence() async throws {
        let (service, _, storage) = Self.makeService()
        #expect(try await service.store(
            showId: "show-overlapping-clear",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 30,
            confidence: 1
        ))

        let firstGate = RepeatedAdCacheAsyncGate()
        let secondGate = RepeatedAdCacheAsyncGate()
        let sequencer = RepeatedAdCacheSequencedGate(
            first: firstGate,
            second: secondGate
        )
        await service._setMaintenanceClearBarrierForTesting {
            await sequencer.wait()
        }

        let firstClear = Task {
            try await service.clear()
        }
        await firstGate.waitUntilStarted()
        let secondClear = Task {
            try await service.clear()
        }
        await secondGate.waitUntilStarted()

        await firstGate.release()
        try await firstClear.value
        #expect(try await service.store(
            showId: "show-overlapping-clear",
            fingerprint: Self.fpWithBitsFlipped([1]),
            boundaryStart: 2,
            boundaryEnd: 31,
            confidence: 1
        ) == false)

        await secondGate.release()
        try await secondClear.value
        #expect(try await storage.totalCount() == 0)
        await service._setMaintenanceClearBarrierForTesting(nil)
    }

    @Test(
        "kill switch waits for a suspended outcome writer",
        .timeLimit(.minutes(1))
    )
    func killSwitchWinsReentrantOutcomeWrite() async throws {
        let (service, _, storage) = Self.makeService()
        let appendGate = RepeatedAdCacheAsyncGate()
        await storage._setAppendOutcomeBarrierForTesting {
            await appendGate.wait()
        }
        let recording = Task {
            try await service.recordOutcome(hit: false)
        }
        await appendGate.waitUntilStarted()

        let disabling = Task {
            await service.setEnabled(false)
        }
        await service._waitUntilOutcomeDrainIsWaitingForTesting()
        await appendGate.release()
        try await recording.value
        await disabling.value

        #expect(await service.isEnabled() == false)
        #expect(
            try await storage.fetchOutcomes(
                newerThan: Date(timeIntervalSince1970: 0)
            ).isEmpty
        )
        await storage._setAppendOutcomeBarrierForTesting(nil)
    }

    @Test(
        "a clear invalidates a suspended auto-disable decision",
        .timeLimit(.minutes(1))
    )
    func clearWinsStaleAutoDisableRequest() async throws {
        let config = RepeatedAdCacheConfig(
            storeConfidenceThreshold: 0.85,
            hammingDistanceThreshold: 3,
            perShowCap: 3,
            globalCap: 5,
            entryMaxAge: 90 * 24 * 60 * 60,
            autoDisableWindow: 14 * 24 * 60 * 60,
            autoDisableHitRateFloor: 0.5,
            autoDisableMinSamples: 1
        )
        let storage = InMemoryRepeatedAdCacheStorage()
        let service = RepeatedAdCacheService(
            config: config,
            storage: storage
        )
        let gate = RepeatedAdCacheAsyncGate()
        await service._setAutoDisableRequestBarrierForTesting {
            await gate.wait()
        }

        let recording = Task {
            try await service.recordOutcome(hit: false)
        }
        await gate.waitUntilStarted()
        try await service.clear()
        await gate.release()
        try await recording.value

        #expect(await service.isEnabled())
        #expect(await service.currentDisableReason() == nil)
        #expect(
            try await storage.fetchOutcomes(
                newerThan: Date(timeIntervalSince1970: 0)
            ).isEmpty
        )
        await service._setAutoDisableRequestBarrierForTesting(nil)
    }

    @Test(
        "correction wins a lookup suspended with a stale candidate snapshot",
        .timeLimit(.minutes(1))
    )
    func correctionWinsReentrantLookupRace() async throws {
        let (service, _, _) = Self.makeService()
        let near = Self.fpWithBitsFlipped([1])
        for (fingerprint, sourceSuffix) in [
            (Self.baseFingerprint, "exact"),
            (near, "near"),
        ] {
            #expect(try await service.store(
                showId: "show-lookup-race",
                fingerprint: fingerprint,
                boundaryStart: 1,
                boundaryEnd: 30,
                confidence: 1,
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "\(sourceSuffix)-asset",
                sourceWindowId: "\(sourceSuffix)-window"
            ))
        }

        let gate = RepeatedAdCacheAsyncGate()
        await service._setLookupCandidateSnapshotBarrierForTesting {
            await gate.wait()
        }
        let delayedLookup = Task {
            try await service.lookup(
                showId: "show-lookup-race",
                fingerprint: Self.baseFingerprint
            )
        }
        await gate.waitUntilStarted()
        _ = try await service.revokeMatches(
            showId: "show-lookup-race",
            fingerprint: Self.baseFingerprint,
            sourceAssetId: "exact-asset",
            sourceWindowId: "exact-window",
            source: .manualVeto
        )
        await gate.release()

        if case .hit = try await delayedLookup.value {
            Issue.record(
                "a pre-correction candidate snapshot must not return a cache hit"
            )
        }
        await service._setLookupCandidateSnapshotBarrierForTesting(nil)
    }

    @Test("store and lookup reject non-finite or malformed recurrence metadata")
    func invalidRecurrenceMetadataFailsClosed() async throws {
        let (service, _, storage) = Self.makeService()
        for (start, end, confidence) in [
            (Double.nan, 30.0, 1.0),
            (1.0, Double.infinity, 1.0),
            (30.0, 1.0, 1.0),
            (1.0, 30.0, Double.nan),
            (1.0, 30.0, 1.1),
        ] {
            #expect(try await service.store(
                showId: "show-invalid",
                fingerprint: Self.baseFingerprint,
                boundaryStart: start,
                boundaryEnd: end,
                confidence: confidence,
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "invalid-asset",
                sourceWindowId: "invalid-window"
            ) == false)
        }

        _ = try await storage.upsert(.init(
            showId: "show-invalid",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 30,
            boundaryEnd: 1,
            confidence: 1,
            lastSeenAt: Date(timeIntervalSince1970: 1),
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "corrupt-asset",
            sourceWindowId: "corrupt-window"
        ))
        if case .hit = try await service.lookup(
            showId: "show-invalid",
            fingerprint: Self.baseFingerprint
        ) {
            Issue.record("malformed persisted recurrence row must be quarantined")
        }

        let (timestampService, _, timestampStorage) = Self.makeService()
        _ = try await timestampStorage.upsert(.init(
            showId: "show-invalid-persisted-time",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 30,
            confidence: 1,
            lastSeenAt: Date(timeIntervalSince1970: -1),
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "invalid-time-asset",
            sourceWindowId: "invalid-time-window"
        ))
        if case .hit = try await timestampService.lookup(
            showId: "show-invalid-persisted-time",
            fingerprint: Self.baseFingerprint
        ) {
            Issue.record(
                "negative persisted recurrence timestamps must be quarantined"
            )
        }
    }

    @Test("lookup fails closed when its LRU timestamp becomes invalid")
    func lookupRejectsInvalidTouchTimestamp() async throws {
        let storage = InMemoryRepeatedAdCacheStorage()
        let clock = ScriptedClock([
            Date(timeIntervalSince1970: 1),
            Date(timeIntervalSince1970: 2),
            Date(timeIntervalSince1970: -1),
        ])
        let service = RepeatedAdCacheService(
            config: Self.testConfig,
            storage: storage,
            clock: { clock.now() }
        )
        #expect(try await service.store(
            showId: "show-invalid-touch",
            fingerprint: Self.baseFingerprint,
            boundaryStart: 1,
            boundaryEnd: 30,
            confidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "invalid-touch-asset",
            sourceWindowId: "invalid-touch-window"
        ))

        if case .hit = try await service.lookup(
            showId: "show-invalid-touch",
            fingerprint: Self.baseFingerprint
        ) {
            Issue.record("an invalid LRU timestamp must not escape in a hit")
        }
        let rows = try await storage.fetchAll(showId: "show-invalid-touch")
        #expect(rows.count == 1)
        #expect(rows[0].lastSeenAt == Date(timeIntervalSince1970: 1))
    }

    @Test("invalid maintenance clocks fail closed")
    func invalidMaintenanceClockFailsClosed() async throws {
        let storage = InMemoryRepeatedAdCacheStorage()
        let service = RepeatedAdCacheService(
            config: Self.testConfig,
            storage: storage,
            clock: { Date(timeIntervalSince1970: .infinity) }
        )

        await #expect(throws: RepeatedAdCacheError.self) {
            _ = try await service.currentHitRateSnapshot()
        }
        #expect(try await service.purgeStaleEntries() == 0)
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

private actor RepeatedAdCacheAsyncGate {
    private var started = false
    private var released = false
    private var startWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseWaiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        started = true
        for waiter in startWaiters {
            waiter.resume()
        }
        startWaiters.removeAll()
        guard !released else { return }
        await withCheckedContinuation { continuation in
            releaseWaiters.append(continuation)
        }
    }

    func waitUntilStarted() async {
        guard !started else { return }
        await withCheckedContinuation { continuation in
            startWaiters.append(continuation)
        }
    }

    func release() {
        released = true
        for waiter in releaseWaiters {
            waiter.resume()
        }
        releaseWaiters.removeAll()
    }
}

private actor RepeatedAdCacheSequencedGate {
    private let first: RepeatedAdCacheAsyncGate
    private let second: RepeatedAdCacheAsyncGate
    private var invocationCount = 0

    init(
        first: RepeatedAdCacheAsyncGate,
        second: RepeatedAdCacheAsyncGate
    ) {
        self.first = first
        self.second = second
    }

    func wait() async {
        invocationCount += 1
        if invocationCount == 1 {
            await first.wait()
        } else {
            await second.wait()
        }
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
        // Canonical SQLite identity is lowercase; case aliases fail closed.
        #expect(RepeatedAdFingerprint(hexString: "0123456789ABCDEF") == nil)
    }

    @Test
    func binariseEmptyVectorIsZero() {
        #expect(RepeatedAdFingerprint.binarise([]).isZero)
    }

    @Test("binarise rejects non-finite or negative vectors")
    func binariseInvalidVectorIsZero() {
        for values: [Float] in [
            [.nan, 1],
            [.infinity, 1],
            [-1, 1],
        ] {
            #expect(RepeatedAdFingerprint.binarise(values).isZero)
        }
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
                featureVersion: 5
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
                    featureVersion: 5
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
