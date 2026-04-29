// LocaleRegionSupportProviderTests.swift
// Unit tests for the Locale-backed region gate. playhead-kgn5.

import Foundation
import Testing
@testable import Playhead

@Suite("LocaleRegionSupportProvider — unit")
struct LocaleRegionSupportProviderTests {
    @Test("US region is supported")
    func usRegionSupported() {
        let p = LocaleRegionSupportProvider(supportedRegions: ["US"], regionProvider: { "US" })
        #expect(p.isRegionSupported())
    }

    @Test("Non-supported region returns false")
    func caRegionUnsupported() {
        let p = LocaleRegionSupportProvider(supportedRegions: ["US"], regionProvider: { "CA" })
        #expect(!p.isRegionSupported())
    }

    @Test("Nil region returns false (no region set on device)")
    func nilRegionUnsupported() {
        let p = LocaleRegionSupportProvider(supportedRegions: ["US"], regionProvider: { nil })
        #expect(!p.isRegionSupported())
    }

    @Test("Default constant is US-only (the dogfood gate)")
    func defaultConstantIsUS() {
        #expect(LocaleRegionSupportProvider.supportedRegions == ["US"])
    }
}

// MARK: - Evaluator integration

@Suite("LocaleRegionSupportProvider — evaluator integration")
struct LocaleRegionSupportProviderEvaluatorIntegrationTests {

    /// Mutable region box so the closure-injected region provider can flip
    /// between evaluator calls (mirroring a Settings → Language & Region
    /// change at runtime).
    final class RegionBox: @unchecked Sendable {
        private let lock = NSLock()
        private var _value: String?
        init(_ value: String?) { self._value = value }
        var value: String? {
            get { lock.lock(); defer { lock.unlock() }; return _value }
            set { lock.lock(); defer { lock.unlock() }; _value = newValue }
        }
    }

    @Test("Real region provider: CA gates evaluator to .regionUnsupported, then noteRegionChanged + US flips it back")
    func regionFlipDrivesEvaluatorVerdict() {
        let regionBox = RegionBox("CA")
        let regionProvider = LocaleRegionSupportProvider(
            supportedRegions: ["US"],
            regionProvider: { regionBox.value }
        )
        // Stub the other four axes to "true" so the only failing gate is region.
        let stubs = (
            hardware: StubHardwareSupportProvider(true),
            ai: StubAppleIntelligenceStateProvider(true),
            language: StubLanguageSupportProvider(true),
            model: StubModelAvailabilityProvider(true)
        )
        let evaluator = AnalysisEligibilityEvaluator(
            hardwareProvider: stubs.hardware,
            appleIntelligenceProvider: stubs.ai,
            regionProvider: regionProvider,
            languageProvider: stubs.language,
            modelAvailabilityProvider: stubs.model,
            clock: SystemAnalysisEligibilityClock(),
            osVersionProvider: { "Version 26.4 (Build 23F79)" },
            ttl: 10_000
        )

        // Initial verdict: region is "CA" → not supported.
        let firstVerdict = evaluator.evaluate()
        #expect(firstVerdict.regionSupported == false)
        #expect(firstVerdict.isFullyEligible == false)

        // Reducer integration: an ineligible region should map to
        // .unavailable / .regionUnsupported.
        let surfaceStatus = episodeSurfaceStatus(
            state: AnalysisState(
                persistedStatus: .queued,
                hasUserPreemptedJob: false,
                hasAppForceQuitFlag: false,
                pendingSinceEnqueuedAt: nil,
                hasAnyConfirmedAnalysis: false
            ),
            cause: nil,
            eligibility: firstVerdict,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(surfaceStatus.disposition == .unavailable)
        #expect(surfaceStatus.analysisUnavailableReason == .regionUnsupported)

        // Flip the device region to US and signal the evaluator.
        regionBox.value = "US"
        evaluator.noteRegionChanged()
        let secondVerdict = evaluator.evaluate()
        #expect(secondVerdict.regionSupported == true)
        #expect(secondVerdict.isFullyEligible == true)
    }
}
