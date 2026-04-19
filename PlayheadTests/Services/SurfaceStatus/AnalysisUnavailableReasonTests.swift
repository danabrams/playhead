// AnalysisUnavailableReasonTests.swift
// Exercises `AnalysisUnavailableReason.derive(from:)` — the pure 1:1
// field→case derivation from an `AnalysisEligibility` snapshot to the
// reason that should surface when the device is ineligible.
//
// Scope: playhead-sueq (Phase 1.5 — "analysisUnavailableReason wiring").
//
// Precedence ladder (most-permanent / least-fixable wins):
//   hardware > region > language > appleIntelligence > model
//
// Rationale: the UI surfaces the root cause, so a device that is
// simultaneously unsupported at the hardware tier AND has AI toggled
// off should still be told "hardware unsupported" — otherwise the user
// would burn a round-trip through Settings only to hit the same wall
// one field deeper.

import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisUnavailableReason — derivation + Codable (playhead-sueq)")
struct AnalysisUnavailableReasonTests {

    // MARK: - Canonical inputs

    private static let t0 = Date(timeIntervalSince1970: 1_700_000_000)

    /// Fully-eligible device — every gate true.
    private static let eligible = AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: true,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: t0
    )

    /// Helper: start from fully eligible, then override specific fields.
    private static func eligibility(
        hardware: Bool = true,
        appleIntelligence: Bool = true,
        region: Bool = true,
        language: Bool = true,
        model: Bool = true
    ) -> AnalysisEligibility {
        AnalysisEligibility(
            hardwareSupported: hardware,
            appleIntelligenceEnabled: appleIntelligence,
            regionSupported: region,
            languageSupported: language,
            modelAvailableNow: model,
            capturedAt: t0
        )
    }

    // MARK: - Fully eligible → nil

    @Test("derive returns nil for a fully-eligible device")
    func derive_returnsNilForFullyEligibleDevice() {
        #expect(AnalysisUnavailableReason.derive(from: Self.eligible) == nil)
    }

    // MARK: - Single-field flips

    @Test("hardware-only flip → .hardwareUnsupported")
    func derive_hardwareOnly_returnsHardwareUnsupported() {
        let e = Self.eligibility(hardware: false)
        #expect(AnalysisUnavailableReason.derive(from: e) == .hardwareUnsupported)
    }

    @Test("region-only flip → .regionUnsupported")
    func derive_regionOnly_returnsRegionUnsupported() {
        let e = Self.eligibility(region: false)
        #expect(AnalysisUnavailableReason.derive(from: e) == .regionUnsupported)
    }

    @Test("language-only flip → .languageUnsupported")
    func derive_languageOnly_returnsLanguageUnsupported() {
        let e = Self.eligibility(language: false)
        #expect(AnalysisUnavailableReason.derive(from: e) == .languageUnsupported)
    }

    @Test("appleIntelligence-only flip → .appleIntelligenceDisabled")
    func derive_appleIntelligenceOnly_returnsAppleIntelligenceDisabled() {
        let e = Self.eligibility(appleIntelligence: false)
        #expect(AnalysisUnavailableReason.derive(from: e) == .appleIntelligenceDisabled)
    }

    @Test("model-only flip → .modelTemporarilyUnavailable")
    func derive_modelOnly_returnsModelTemporarilyUnavailable() {
        let e = Self.eligibility(model: false)
        #expect(AnalysisUnavailableReason.derive(from: e) == .modelTemporarilyUnavailable)
    }

    // MARK: - Precedence cascades

    @Test("all fields false → hardware wins")
    func derive_precedence_allFieldsFalse_returnsHardwareUnsupported() {
        let e = Self.eligibility(
            hardware: false,
            appleIntelligence: false,
            region: false,
            language: false,
            model: false
        )
        #expect(AnalysisUnavailableReason.derive(from: e) == .hardwareUnsupported)
    }

    @Test("hardware=F + region=F → hardware wins")
    func derive_precedence_hardwareBeatsRegion() {
        let e = Self.eligibility(hardware: false, region: false)
        #expect(AnalysisUnavailableReason.derive(from: e) == .hardwareUnsupported)
    }

    @Test("region=F + language=F → region wins")
    func derive_precedence_regionBeatsLanguage() {
        let e = Self.eligibility(region: false, language: false)
        #expect(AnalysisUnavailableReason.derive(from: e) == .regionUnsupported)
    }

    @Test("language=F + appleIntelligence=F → language wins")
    func derive_precedence_languageBeatsAppleIntelligence() {
        let e = Self.eligibility(appleIntelligence: false, language: false)
        #expect(AnalysisUnavailableReason.derive(from: e) == .languageUnsupported)
    }

    @Test("appleIntelligence=F + model=F → appleIntelligence wins")
    func derive_precedence_appleIntelligenceBeatsModel() {
        let e = Self.eligibility(appleIntelligence: false, model: false)
        #expect(AnalysisUnavailableReason.derive(from: e) == .appleIntelligenceDisabled)
    }

    // MARK: - Codable round-trips

    @Test("Codable — each case encodes to its expected raw string")
    func codable_encodesExpectedRawValues() throws {
        let encoder = JSONEncoder()
        let pairs: [(AnalysisUnavailableReason, String)] = [
            (.hardwareUnsupported, "hardware_unsupported"),
            (.regionUnsupported, "region_unsupported"),
            (.languageUnsupported, "language_unsupported"),
            (.appleIntelligenceDisabled, "apple_intelligence_disabled"),
            (.modelTemporarilyUnavailable, "model_temporarily_unavailable"),
        ]
        for (value, expectedRaw) in pairs {
            let data = try encoder.encode(value)
            let str = String(decoding: data, as: UTF8.self)
            // Raw-value enums encode to a JSON string literal, e.g. "\"hardware_unsupported\"".
            #expect(str == "\"\(expectedRaw)\"", "\(value) should encode to \"\(expectedRaw)\", got \(str)")
        }
    }

    @Test("Codable — round-trip preserves every case")
    func codable_roundTripPreservesEveryCase() throws {
        // Iterate `allCases` so adding a new enum case is automatically
        // exercised by this round-trip — a hand-listed array would
        // silently become non-exhaustive. The sibling
        // `codable_encodesExpectedRawValues` test legitimately uses a
        // hand-listed (case, expectedRaw) pair array because its
        // purpose is to pin the exact string form per case.
        let encoder = JSONEncoder()
        let decoder = JSONDecoder()
        for value in AnalysisUnavailableReason.allCases {
            let data = try encoder.encode(value)
            let decoded = try decoder.decode(AnalysisUnavailableReason.self, from: data)
            #expect(decoded == value)
        }
    }

    // MARK: - EpisodeSurfaceStatus integration

    @Test("EpisodeSurfaceStatus carries the derived reason + encodes the field")
    func episodeSurfaceStatus_integratesDerivedReason() throws {
        // Hardware-only flip: reducer Rule 1 fires (isFullyEligible=false)
        // and the derived reason should be `.hardwareUnsupported`.
        let ineligible = Self.eligibility(hardware: false)

        let queuedState = AnalysisState(
            persistedStatus: .queued,
            hasUserPreemptedJob: false,
            hasAppForceQuitFlag: false,
            pendingSinceEnqueuedAt: Self.t0,
            hasAnyConfirmedAnalysis: false
        )

        let out = episodeSurfaceStatus(
            state: queuedState,
            cause: nil,
            eligibility: ineligible,
            coverage: nil,
            readinessAnchor: nil
        )

        #expect(out.disposition == .unavailable)
        #expect(out.analysisUnavailableReason == .hardwareUnsupported)

        // Encode and assert the JSON contains the snake-cased field + value.
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(out)
        let json = String(decoding: data, as: UTF8.self)

        #expect(
            json.contains("\"analysis_unavailable_reason\":\"hardware_unsupported\""),
            "encoded JSON should contain analysis_unavailable_reason → hardware_unsupported, got: \(json)"
        )
    }

    @Test("EpisodeSurfaceStatus Codable round-trip preserves analysisUnavailableReason")
    func episodeSurfaceStatus_codableRoundTripPreservesReason() throws {
        let ineligible = Self.eligibility(appleIntelligence: false)
        let queuedState = AnalysisState(
            persistedStatus: .queued,
            hasUserPreemptedJob: false,
            hasAppForceQuitFlag: false,
            pendingSinceEnqueuedAt: Self.t0,
            hasAnyConfirmedAnalysis: false
        )
        let out = episodeSurfaceStatus(
            state: queuedState,
            cause: nil,
            eligibility: ineligible,
            coverage: nil,
            readinessAnchor: nil
        )
        let encoder = JSONEncoder()
        let decoder = JSONDecoder()
        let data = try encoder.encode(out)
        let decoded = try decoder.decode(EpisodeSurfaceStatus.self, from: data)
        #expect(decoded.analysisUnavailableReason == .appleIntelligenceDisabled)
        #expect(decoded == out)
    }
}
