// AnalysisEligibilityTests.swift
// Tests for the AnalysisEligibility contract, its evaluator, caching, and
// invalidation hooks. playhead-2fd (Phase 0 Guardrails).

import Foundation
import Testing
@testable import Playhead

// MARK: - Test Doubles

/// Deterministic clock for TTL tests.
final class FakeAnalysisEligibilityClock: AnalysisEligibilityClock, @unchecked Sendable {
    private let lock = NSLock()
    private var _now: Date
    init(start: Date = Date(timeIntervalSince1970: 1_700_000_000)) {
        self._now = start
    }
    var now: Date {
        lock.lock(); defer { lock.unlock() }
        return _now
    }
    func advance(by interval: TimeInterval) {
        lock.lock(); defer { lock.unlock() }
        _now = _now.addingTimeInterval(interval)
    }
    func set(to date: Date) {
        lock.lock(); defer { lock.unlock() }
        _now = date
    }
}

/// Mutable provider stubs whose values can flip between calls.
final class StubHardwareSupportProvider: HardwareSupportProviding, @unchecked Sendable {
    private let lock = NSLock()
    private var _value: Bool
    private(set) var callCount: Int = 0
    init(_ value: Bool) { self._value = value }
    func isHardwareSupported() -> Bool {
        lock.lock(); defer { lock.unlock() }
        callCount += 1
        return _value
    }
    func set(_ value: Bool) {
        lock.lock(); defer { lock.unlock() }
        _value = value
    }
}

final class StubAppleIntelligenceStateProvider: AppleIntelligenceStateProviding, @unchecked Sendable {
    private let lock = NSLock()
    private var _value: Bool
    init(_ value: Bool) { self._value = value }
    func isAppleIntelligenceEnabled() -> Bool {
        lock.lock(); defer { lock.unlock() }
        return _value
    }
    func set(_ value: Bool) {
        lock.lock(); defer { lock.unlock() }
        _value = value
    }
}

final class StubRegionSupportProvider: RegionSupportProviding, @unchecked Sendable {
    private let lock = NSLock()
    private var _value: Bool
    init(_ value: Bool) { self._value = value }
    func isRegionSupported() -> Bool {
        lock.lock(); defer { lock.unlock() }
        return _value
    }
    func set(_ value: Bool) {
        lock.lock(); defer { lock.unlock() }
        _value = value
    }
}

final class StubLanguageSupportProvider: LanguageSupportProviding, @unchecked Sendable {
    private let lock = NSLock()
    private var _value: Bool
    init(_ value: Bool) { self._value = value }
    func isLanguageSupported() -> Bool {
        lock.lock(); defer { lock.unlock() }
        return _value
    }
    func set(_ value: Bool) {
        lock.lock(); defer { lock.unlock() }
        _value = value
    }
}

final class StubModelAvailabilityProvider: ModelAvailabilityProviding, @unchecked Sendable {
    private let lock = NSLock()
    private var _value: Bool
    init(_ value: Bool) { self._value = value }
    func isModelAvailableNow() -> Bool {
        lock.lock(); defer { lock.unlock() }
        return _value
    }
    func set(_ value: Bool) {
        lock.lock(); defer { lock.unlock() }
        _value = value
    }
}

// MARK: - Helpers

private struct Stubs {
    let hardware: StubHardwareSupportProvider
    let ai: StubAppleIntelligenceStateProvider
    let region: StubRegionSupportProvider
    let language: StubLanguageSupportProvider
    let model: StubModelAvailabilityProvider
    let clock: FakeAnalysisEligibilityClock
    let osVersion: String

    init(
        hardware: Bool = true,
        ai: Bool = true,
        region: Bool = true,
        language: Bool = true,
        model: Bool = true,
        osVersion: String = "Version 26.4 (Build 23F79)"
    ) {
        self.hardware = StubHardwareSupportProvider(hardware)
        self.ai = StubAppleIntelligenceStateProvider(ai)
        self.region = StubRegionSupportProvider(region)
        self.language = StubLanguageSupportProvider(language)
        self.model = StubModelAvailabilityProvider(model)
        self.clock = FakeAnalysisEligibilityClock()
        self.osVersion = osVersion
    }

    func makeEvaluator(ttl: TimeInterval = 4 * 60 * 60) -> AnalysisEligibilityEvaluator {
        AnalysisEligibilityEvaluator(
            hardwareProvider: hardware,
            appleIntelligenceProvider: ai,
            regionProvider: region,
            languageProvider: language,
            modelAvailabilityProvider: model,
            clock: clock,
            osVersionProvider: { self.osVersion },
            ttl: ttl
        )
    }
}

// MARK: - Contract & Combinatorics

@Suite("AnalysisEligibility — 5-field combinatorics")
struct AnalysisEligibilityCombinatoricsTests {

    @Test("Eligible device: all five fields true -> isFullyEligible")
    func testEligibleDevice() {
        let s = Stubs()
        let evaluator = s.makeEvaluator()
        let e = evaluator.evaluate()
        #expect(e.hardwareSupported == true)
        #expect(e.appleIntelligenceEnabled == true)
        #expect(e.regionSupported == true)
        #expect(e.languageSupported == true)
        #expect(e.modelAvailableNow == true)
        #expect(e.isFullyEligible == true)
    }

    @Test("Hardware-ineligible device: only hardwareSupported is false")
    func testHardwareIneligible() {
        let s = Stubs(hardware: false)
        let e = s.makeEvaluator().evaluate()
        #expect(e.hardwareSupported == false)
        #expect(e.isFullyEligible == false)
    }

    @Test("Region-ineligible device: only regionSupported is false")
    func testRegionIneligible() {
        let s = Stubs(region: false)
        let e = s.makeEvaluator().evaluate()
        #expect(e.regionSupported == false)
        #expect(e.isFullyEligible == false)
    }

    @Test("Language-ineligible device: only languageSupported is false")
    func testLanguageIneligible() {
        let s = Stubs(language: false)
        let e = s.makeEvaluator().evaluate()
        #expect(e.languageSupported == false)
        #expect(e.isFullyEligible == false)
    }

    @Test("AI-disabled: appleIntelligenceEnabled false -> not eligible")
    func testAppleIntelligenceDisabled() {
        let s = Stubs(ai: false)
        let e = s.makeEvaluator().evaluate()
        #expect(e.appleIntelligenceEnabled == false)
        #expect(e.isFullyEligible == false)
    }

    @Test("Model not available: modelAvailableNow false -> not eligible")
    func testModelNotAvailable() {
        let s = Stubs(model: false)
        let e = s.makeEvaluator().evaluate()
        #expect(e.modelAvailableNow == false)
        #expect(e.isFullyEligible == false)
    }

    @Test("Multiple ineligibility reasons are independent")
    func testMultipleFailures() {
        let s = Stubs(hardware: false, ai: false, region: false, language: false, model: false)
        let e = s.makeEvaluator().evaluate()
        #expect(e.hardwareSupported == false)
        #expect(e.appleIntelligenceEnabled == false)
        #expect(e.regionSupported == false)
        #expect(e.languageSupported == false)
        #expect(e.modelAvailableNow == false)
        #expect(e.isFullyEligible == false)
    }

    @Test("AnalysisEligibility is Codable round-trip")
    func testCodableRoundTrip() throws {
        let original = AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: false,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: false,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(AnalysisEligibility.self, from: data)
        #expect(decoded == original)
    }
}

// MARK: - Caching & TTL

@Suite("AnalysisEligibility — caching + TTL")
struct AnalysisEligibilityCacheTests {

    @Test("Repeated evaluate() within TTL returns cached value (provider not reconsulted)")
    func testCacheHit() {
        let s = Stubs()
        let evaluator = s.makeEvaluator(ttl: 100)
        _ = evaluator.evaluate()
        #expect(s.hardware.callCount == 1)
        // Flip underlying provider value; cached result should still match.
        s.hardware.set(false)
        let again = evaluator.evaluate()
        #expect(again.hardwareSupported == true, "Cache must shield from live provider changes within TTL")
        #expect(s.hardware.callCount == 1, "Provider must not be reconsulted during cache hit")
    }

    @Test("After TTL expiry a fresh evaluation is performed")
    func testTTLExpiry() {
        let s = Stubs()
        let evaluator = s.makeEvaluator(ttl: 100)
        _ = evaluator.evaluate()
        #expect(s.hardware.callCount == 1)

        s.hardware.set(false)
        s.clock.advance(by: 99)
        _ = evaluator.evaluate()
        #expect(s.hardware.callCount == 1, "Still within TTL, cache must hold")

        s.clock.advance(by: 2) // total 101, past TTL
        let refreshed = evaluator.evaluate()
        #expect(refreshed.hardwareSupported == false, "Post-TTL read must pick up new provider state")
        #expect(s.hardware.callCount == 2)
    }

    @Test("Default TTL is 4 hours")
    func testDefaultTTLIs4Hours() {
        let s = Stubs()
        let evaluator = AnalysisEligibilityEvaluator(
            hardwareProvider: s.hardware,
            appleIntelligenceProvider: s.ai,
            regionProvider: s.region,
            languageProvider: s.language,
            modelAvailabilityProvider: s.model,
            clock: s.clock,
            osVersionProvider: { s.osVersion }
        )
        _ = evaluator.evaluate()
        #expect(s.hardware.callCount == 1)

        s.hardware.set(false)
        s.clock.advance(by: 4 * 60 * 60 - 1)
        _ = evaluator.evaluate()
        #expect(s.hardware.callCount == 1, "Within default 4h TTL cache must hold")

        s.clock.advance(by: 2)
        _ = evaluator.evaluate()
        #expect(s.hardware.callCount == 2, "Past 4h TTL the cache must refresh")
    }
}

// MARK: - Invalidation Hooks

@Suite("AnalysisEligibility — invalidation hooks")
struct AnalysisEligibilityInvalidationTests {

    @Test("Locale change invalidates cache")
    func testLocaleChangeInvalidates() {
        let s = Stubs()
        let evaluator = s.makeEvaluator(ttl: 10_000)
        _ = evaluator.evaluate()
        s.language.set(false)
        evaluator.noteLocaleChanged()
        let e = evaluator.evaluate()
        #expect(e.languageSupported == false)
    }

    @Test("Region change invalidates cache")
    func testRegionChangeInvalidates() {
        let s = Stubs()
        let evaluator = s.makeEvaluator(ttl: 10_000)
        _ = evaluator.evaluate()
        s.region.set(false)
        evaluator.noteRegionChanged()
        let e = evaluator.evaluate()
        #expect(e.regionSupported == false)
    }

    @Test("OS version change invalidates cache")
    func testOSVersionChangeInvalidates() {
        let s = Stubs()
        // We need the osVersion to change — use a mutable closure source.
        let osVersionBox = OSVersionBox("Version 26.4 (Build 23F79)")
        let evaluator = AnalysisEligibilityEvaluator(
            hardwareProvider: s.hardware,
            appleIntelligenceProvider: s.ai,
            regionProvider: s.region,
            languageProvider: s.language,
            modelAvailabilityProvider: s.model,
            clock: s.clock,
            osVersionProvider: { osVersionBox.value },
            ttl: 10_000
        )
        _ = evaluator.evaluate()
        s.hardware.set(false)
        osVersionBox.value = "Version 26.5 (Build 23F99)"
        evaluator.noteOSVersionChangedIfNeeded()
        let e = evaluator.evaluate()
        #expect(e.hardwareSupported == false, "Version bump should force a refresh")
    }

    @Test("noteOSVersionChangedIfNeeded advances internal OS-version baseline (no evaluate() in between)")
    func testNoteOSVersionChangedIfNeededAdvancesBaseline() {
        // M2 fix (cycle 1): noteOSVersionChangedIfNeeded() must update its
        // internal cachedOSVersion baseline even when the caller never calls
        // evaluate() in between. Cycle 2 review correctly flagged that the
        // earlier indirect test was unfalsifiable: any subsequent evaluate()
        // refreshes cachedOSVersion via the slow-path commit, masking the
        // bug. The only direct way to assert the contract is to read the
        // baseline through a DEBUG-only accessor.
        //
        // SENTINEL: this test will FAIL if the `cachedOSVersion = current`
        // line inside noteOSVersionChangedIfNeeded() is removed.
        let s = Stubs()
        let osVersionBox = OSVersionBox("Version 26.4 (Build 23F79)")
        let evaluator = AnalysisEligibilityEvaluator(
            hardwareProvider: s.hardware,
            appleIntelligenceProvider: s.ai,
            regionProvider: s.region,
            languageProvider: s.language,
            modelAvailabilityProvider: s.model,
            clock: s.clock,
            osVersionProvider: { osVersionBox.value },
            ttl: 10_000
        )
        // Seed the cache so cachedOSVersion is initialized to v26.4.
        _ = evaluator.evaluate()
        #expect(evaluator._cachedOSVersionForTesting == "Version 26.4 (Build 23F79)")

        // Bump OS version, then probe — and DO NOT call evaluate() after.
        // This is the exact path where the bug is observable: the only thing
        // that can advance cachedOSVersion is the in-hook write itself.
        osVersionBox.value = "Version 26.5 (Build 23F99)"
        evaluator.noteOSVersionChangedIfNeeded()

        #expect(
            evaluator._cachedOSVersionForTesting == "Version 26.5 (Build 23F99)",
            "noteOSVersionChangedIfNeeded must advance the baseline in-hook (not rely on a follow-up evaluate())"
        )

        // Belt-and-suspenders: a same-version probe now must be a true no-op
        // (no further mutation of cached). Verified by checking the cache
        // pointer is preserved across the no-op call.
        let cachedBefore = evaluator.evaluate()
        let countBefore = s.hardware.callCount
        evaluator.noteOSVersionChangedIfNeeded()
        let cachedAfter = evaluator.evaluate()
        #expect(cachedAfter == cachedBefore, "Same-version probe must not invalidate the cache")
        #expect(s.hardware.callCount == countBefore, "Same-version probe must not reconsult providers")
    }

    @Test("Apple Intelligence settings toggle invalidates cache")
    func testAIToggleInvalidates() {
        let s = Stubs()
        let evaluator = s.makeEvaluator(ttl: 10_000)
        _ = evaluator.evaluate()
        s.ai.set(false)
        evaluator.noteAppleIntelligenceToggled()
        let e = evaluator.evaluate()
        #expect(e.appleIntelligenceEnabled == false)
    }

    @Test("Foreground-after-TTL: TTL-exceeded interval forces refresh, TTL-inside does not")
    func testForegroundAfterTTL() {
        let s = Stubs()
        let evaluator = s.makeEvaluator(ttl: 100)
        _ = evaluator.evaluate()
        #expect(s.hardware.callCount == 1)

        // Foreground after a short background stint (within TTL): no refresh.
        s.hardware.set(false)
        s.clock.advance(by: 50)
        evaluator.noteAppForegrounded()
        _ = evaluator.evaluate()
        #expect(s.hardware.callCount == 1, "Foreground within TTL window must not invalidate")

        // Foreground after > TTL: refresh.
        s.clock.advance(by: 200)
        evaluator.noteAppForegrounded()
        _ = evaluator.evaluate()
        #expect(s.hardware.callCount == 2)
    }

    @Test("Foreground after >TTL with OS version bump: cache rebuilt AND new OS observed")
    func testForegroundAfterTTLObservesOSVersionBump() {
        // C2 fix: noteAppForegrounded() must do MORE than what evaluate()
        // would do on its own. It must (1) probe the OS version (so a
        // foreground after an overnight OS update is caught by callers that
        // never invoke evaluate()) and (2) invalidate the cache past TTL.
        let s = Stubs()
        let osVersionBox = OSVersionBox("Version 26.4 (Build 23F79)")
        let evaluator = AnalysisEligibilityEvaluator(
            hardwareProvider: s.hardware,
            appleIntelligenceProvider: s.ai,
            regionProvider: s.region,
            languageProvider: s.language,
            modelAvailabilityProvider: s.model,
            clock: s.clock,
            osVersionProvider: { osVersionBox.value },
            ttl: 100
        )
        _ = evaluator.evaluate()
        #expect(s.hardware.callCount == 1)

        // Simulate: device backgrounded > TTL, OS updated mid-background,
        // hardware support flipped (e.g. driver/SoC firmware revision moved
        // the bar). On foreground we must observe both signals.
        s.hardware.set(false)
        s.clock.advance(by: 4 * 60 * 60 + 1) // > TTL
        osVersionBox.value = "Version 26.5 (Build 23F99)"

        evaluator.noteAppForegrounded()
        let refreshed = evaluator.evaluate()

        #expect(refreshed.hardwareSupported == false, "Cache must be rebuilt past TTL on foreground")
        #expect(s.hardware.callCount == 2, "Provider must be reconsulted exactly once on the rebuild")

        // After the rebuild, calling noteOSVersionChangedIfNeeded with the
        // SAME (now-current) OS string must NOT invalidate again — proves
        // the foreground hook recorded the new OS version, not just nuked
        // the cache.
        s.hardware.set(true)
        s.clock.advance(by: 1) // still within fresh TTL
        evaluator.noteOSVersionChangedIfNeeded()
        let again = evaluator.evaluate()
        #expect(again.hardwareSupported == false, "Same-version probe must not invalidate; cache must hold")
        #expect(s.hardware.callCount == 2, "Provider call count must stay at 2")
    }

    @Test("Foreground within TTL is a true no-op: cache and call count preserved")
    func testForegroundWithinTTLIsNoOp() {
        // C2 regression guard: when within TTL, noteAppForegrounded() must
        // not nuke the cache, must not reconsult providers, and must not
        // change the OS-version baseline state observable to callers.
        let s = Stubs()
        let evaluator = s.makeEvaluator(ttl: 1_000)
        let first = evaluator.evaluate()
        #expect(s.hardware.callCount == 1)

        // Flip provider state to prove cache-shielding still works.
        s.hardware.set(false)
        s.clock.advance(by: 10) // well inside TTL

        evaluator.noteAppForegrounded()
        let second = evaluator.evaluate()

        #expect(second == first, "Cached record must be returned unchanged")
        #expect(s.hardware.callCount == 1, "Foreground inside TTL must not reconsult providers")
    }

    @Test("invalidate() forces a fresh evaluation")
    func testExplicitInvalidate() {
        let s = Stubs()
        let evaluator = s.makeEvaluator(ttl: 10_000)
        _ = evaluator.evaluate()
        s.hardware.set(false)
        evaluator.invalidate()
        let e = evaluator.evaluate()
        #expect(e.hardwareSupported == false)
    }

    @Test("evaluate() does not deadlock when a provider re-enters")
    func testReentrantProviderDoesNotDeadlock() async {
        // Cycle 3 fix (Issue B): evaluate() must invoke its providers OUTSIDE
        // the internal lock so a provider that re-enters evaluate() (e.g. via
        // a Combine subscription) does not deadlock the non-recursive NSLock.
        //
        // Without the fix, the first evaluate() call below acquires the lock,
        // calls into ReentrantModelAvailabilityProvider.isModelAvailableNow(),
        // which on its first invocation calls evaluator.evaluate() again on
        // the same thread — that nested call attempts lock.lock() and the
        // thread deadlocks. With the fix, the providers run lock-free on the
        // slow path and the re-entrant call observes either a fresh cache
        // miss (recomputing) or the racing winner — either way it returns.
        //
        // We assert progress via a 2-second timeout race: if evaluate()
        // hasn't completed by then the test fails (deadlock detected).
        let s = Stubs()
        let provider = ReentrantModelAvailabilityProvider()
        let evaluator = AnalysisEligibilityEvaluator(
            hardwareProvider: s.hardware,
            appleIntelligenceProvider: s.ai,
            regionProvider: s.region,
            languageProvider: s.language,
            modelAvailabilityProvider: provider,
            clock: s.clock,
            osVersionProvider: { s.osVersion },
            ttl: 10_000
        )
        provider.bind(evaluator: evaluator)

        // Race evaluate() against a 2s timeout. A deadlocked evaluator hangs
        // forever, so the timeout wins and we report failure. We use a
        // child-task race; cancelling the group on first-return signals the
        // sleeping timeout to exit promptly.
        //
        // NOTE: If a regression deadlocks evaluate(), the detached task below
        // hangs the worker thread for the lifetime of the test process —
        // group.cancelAll() cancels the awaiting parent but cannot interrupt
        // a stuck synchronous body. Acceptable: the failure mode (test
        // process hangs / next test runs slow) is loud enough to investigate,
        // and the bug is exactly what we're trying to detect. If this becomes
        // painful, swap to a thread-targeted pthread_kill or move evaluate()
        // to async + cooperative cancellation.
        let result: AnalysisEligibility? = await withTaskGroup(
            of: AnalysisEligibility?.self
        ) { group in
            group.addTask {
                // evaluate() is synchronous; wrap in a detached await so the
                // group sees it as an async unit. Run it on a background
                // queue via Task.detached priority so it doesn't share a
                // cooperative thread with the timeout sleeper.
                await Task.detached(priority: .userInitiated) {
                    evaluator.evaluate()
                }.value
            }
            group.addTask {
                try? await Task.sleep(nanoseconds: 2_000_000_000)
                return nil
            }
            let first = await group.next() ?? nil
            group.cancelAll()
            return first
        }

        #expect(result != nil, "evaluate() deadlocked when its provider re-entered evaluate()")

        // The re-entrant provider must have been invoked at least twice:
        // once for the outer call's slow-path sweep, and once for the inner
        // re-entrant call's slow-path sweep. Without the lock-free refactor,
        // the outer call would deadlock and never reach the inner — so a
        // count of 1 implies the regression has returned even though the
        // top-level deadlock check happened to pass.
        #expect(
            provider.callCount >= 2,
            "Reentrant provider must have been called by both the outer and the inner evaluate(); without the lock-free refactor, the outer call would deadlock and never reach the inner"
        )
    }
}

// MARK: - Re-entrant provider for deadlock regression

/// Model-availability provider that, on its FIRST call, re-enters
/// `evaluator.evaluate()` from inside `isModelAvailableNow()`. Used to
/// regression-test the lock-free provider sweep introduced in cycle 3.
final class ReentrantModelAvailabilityProvider: ModelAvailabilityProviding, @unchecked Sendable {
    private let lock = NSLock()
    private weak var evaluator: AnalysisEligibilityEvaluator?
    private var hasReentered = false
    private(set) var callCount: Int = 0

    func bind(evaluator: AnalysisEligibilityEvaluator) {
        lock.lock(); defer { lock.unlock() }
        self.evaluator = evaluator
    }

    func isModelAvailableNow() -> Bool {
        lock.lock()
        callCount += 1
        let shouldReenter = !hasReentered
        if shouldReenter { hasReentered = true }
        let evaluator = self.evaluator
        lock.unlock()

        // Re-enter exactly once to avoid infinite recursion. If the outer
        // evaluate() holds the lock around providers (the bug), this nested
        // call will deadlock on lock.lock(). With the fix, it returns
        // normally.
        if shouldReenter, let evaluator {
            _ = evaluator.evaluate()
        }
        return true
    }
}

// MARK: - Performance

@Suite("AnalysisEligibility — performance")
struct AnalysisEligibilityPerformanceTests {

    @Test("evaluate() completes in <200ms on first cold evaluation")
    func testEvaluatePerformance() {
        // Cycle 5 fix: budget intentionally generous (200ms, not the
        // earlier 50ms). The goal here is to catch order-of-magnitude
        // regressions (e.g. an accidental sync I/O call inside the
        // provider sweep), not micro-perf jitter. On a busy CI runner a
        // 50ms wall-clock budget flakes; 200ms still fails fast on real
        // regressions while tolerating background-noise spikes.
        let s = Stubs()
        let evaluator = s.makeEvaluator()
        let start = CFAbsoluteTimeGetCurrent()
        _ = evaluator.evaluate()
        let elapsedMS = (CFAbsoluteTimeGetCurrent() - start) * 1000
        #expect(elapsedMS < 200, "evaluate() was \(elapsedMS)ms, budget is 200ms")
    }

    @Test("evaluate() is safe to call repeatedly without blocking")
    func testEvaluateRepeated() {
        let s = Stubs()
        let evaluator = s.makeEvaluator()
        let start = CFAbsoluteTimeGetCurrent()
        for _ in 0..<1000 {
            _ = evaluator.evaluate()
        }
        let elapsedMS = (CFAbsoluteTimeGetCurrent() - start) * 1000
        // 1000 cache hits must still fit within 50ms to prove the cached path
        // is free of blocking I/O.
        #expect(elapsedMS < 50, "1000 cache hits took \(elapsedMS)ms — must remain non-blocking")
    }
}

// MARK: - Policy B documentation

@Suite("AnalysisEligibility — product policy metadata")
struct AnalysisEligibilityPolicyMetadataTests {

    /// Policy B (2026-04-16): ineligible devices still install the app and
    /// download podcasts — analysis is merely marked unavailable. This test
    /// is a regression trip-wire: if a future change removes the policy
    /// documentation constant, this test fails fast so reviewers catch it.
    @Test("Policy B decision is recorded in source")
    func testPolicyBMetadataPresent() {
        #expect(AnalysisEligibility.policyDecisionDate == "2026-04-16")
        #expect(AnalysisEligibility.policyDecision == .downloadOnlyWithUnavailableMessaging)
    }
}

// MARK: - Local OS version box for tests

final class OSVersionBox: @unchecked Sendable {
    private let lock = NSLock()
    private var _value: String
    init(_ value: String) { self._value = value }
    var value: String {
        get { lock.lock(); defer { lock.unlock() }; return _value }
        set { lock.lock(); defer { lock.unlock() }; _value = newValue }
    }
}
