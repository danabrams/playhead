// CauseTaxonomyTests.swift
// Tests for the four-layer cause taxonomy: enum variants exist, rawValues are
// stable, and ResolutionHint.userFixable returns the expected value for each
// case.

import Foundation
import Testing

@testable import Playhead

@Suite("CauseTaxonomy")
struct CauseTaxonomyTests {

    // MARK: - Layer 1: InternalMissCause

    @Test("InternalMissCause covers the 16 documented variants")
    func internalMissCauseCoversDocumentedVariants() {
        let expected: Set<String> = [
            "no_runtime_grant",
            "task_expired",
            "thermal",
            "low_power_mode",
            "battery_low_unplugged",
            "no_network",
            "wifi_required",
            "media_cap",
            "analysis_cap",
            "user_preempted",
            "user_cancelled",
            "model_temporarily_unavailable",
            "unsupported_episode_language",
            "asr_failed",
            "pipeline_error",
            "app_force_quit_requires_relaunch",
        ]
        let actual = Set(InternalMissCause.allCases.map(\.rawValue))
        #expect(actual == expected)
        #expect(InternalMissCause.allCases.count == 16)
    }

    // MARK: - Layer 2: SurfaceDisposition

    @Test("SurfaceDisposition covers the 5 documented variants")
    func surfaceDispositionCoversDocumentedVariants() {
        let expected: Set<String> = [
            "queued", "paused", "unavailable", "failed", "cancelled",
        ]
        let actual = Set(SurfaceDisposition.allCases.map(\.rawValue))
        #expect(actual == expected)
        #expect(SurfaceDisposition.allCases.count == 5)
    }

    // MARK: - Layer 3: SurfaceReason

    @Test("SurfaceReason covers the 9 documented variants")
    func surfaceReasonCoversDocumentedVariants() {
        let expected: Set<String> = [
            "waiting_for_time",
            "phone_is_hot",
            "power_limited",
            "waiting_for_network",
            "storage_full",
            "analysis_unavailable",
            "resume_in_app",
            "cancelled",
            "couldnt_analyze",
        ]
        let actual = Set(SurfaceReason.allCases.map(\.rawValue))
        #expect(actual == expected)
        #expect(SurfaceReason.allCases.count == 9)
    }

    // MARK: - Layer 4: ResolutionHint

    @Test("ResolutionHint covers the 8 documented variants")
    func resolutionHintCoversDocumentedVariants() {
        let expected: Set<String> = [
            "none",
            "wait",
            "connect_to_wifi",
            "charge_device",
            "free_up_storage",
            "enable_apple_intelligence",
            "open_app_to_resume",
            "retry",
        ]
        let actual = Set(ResolutionHint.allCases.map(\.rawValue))
        #expect(actual == expected)
        #expect(ResolutionHint.allCases.count == 8)
    }

    @Test(
        "ResolutionHint.userFixable is false only for .none and .wait",
        arguments: [
            (ResolutionHint.none, false),
            (.wait, false),
            (.connectToWiFi, true),
            (.chargeDevice, true),
            (.freeUpStorage, true),
            (.enableAppleIntelligence, true),
            (.openAppToResume, true),
            (.retry, true),
        ]
    )
    func userFixableMatchesExpectation(
        hint: ResolutionHint,
        expected: Bool
    ) {
        #expect(hint.userFixable == expected)
    }

    @Test("Every ResolutionHint case is covered by the userFixable table")
    func userFixableIsTotal() {
        // Guard against adding a new hint without updating userFixable.
        // This test fails if ResolutionHint grows without the table below
        // being extended to match.
        let covered: Set<ResolutionHint> = [
            .none,
            .wait,
            .connectToWiFi,
            .chargeDevice,
            .freeUpStorage,
            .enableAppleIntelligence,
            .openAppToResume,
            .retry,
        ]
        #expect(covered.count == ResolutionHint.allCases.count)
        for hint in ResolutionHint.allCases {
            #expect(covered.contains(hint))
        }
    }

    // MARK: - SurfaceAttribution

    @Test("SurfaceAttribution is Hashable and Codable")
    func surfaceAttributionRoundTrips() throws {
        let original = SurfaceAttribution(
            disposition: .paused,
            reason: .resumeInApp,
            hint: .openAppToResume
        )
        let encoded = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(
            SurfaceAttribution.self,
            from: encoded
        )
        #expect(decoded == original)
    }
}
