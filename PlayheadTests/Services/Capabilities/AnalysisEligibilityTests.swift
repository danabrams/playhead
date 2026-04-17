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
}

// MARK: - Performance

@Suite("AnalysisEligibility — performance")
struct AnalysisEligibilityPerformanceTests {

    @Test("evaluate() completes in <50ms on first cold evaluation")
    func testEvaluatePerformance() {
        let s = Stubs()
        let evaluator = s.makeEvaluator()
        let start = CFAbsoluteTimeGetCurrent()
        _ = evaluator.evaluate()
        let elapsedMS = (CFAbsoluteTimeGetCurrent() - start) * 1000
        #expect(elapsedMS < 50, "evaluate() was \(elapsedMS)ms, budget is 50ms")
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
