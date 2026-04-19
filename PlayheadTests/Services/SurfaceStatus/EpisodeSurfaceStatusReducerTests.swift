// EpisodeSurfaceStatusReducerTests.swift
// Exercises the input-precedence ladder of `episodeSurfaceStatus(...)`.
// Each rule has at least one test that asserts the rule fires in
// isolation AND that it wins against every lower-priority rule when
// multiple channels are simultaneously live.

import Foundation
import Testing

@testable import Playhead

@Suite("EpisodeSurfaceStatusReducer — input-precedence ladder (playhead-5bb3)")
struct EpisodeSurfaceStatusReducerTests {

    // MARK: - Canonical inputs

    /// A fully eligible device — every gate true.
    static let eligible = AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: true,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
    )

    /// An ineligible device — Apple Intelligence off.
    static let ineligible = AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: false,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
    )

    /// A "queued" analysis state with no special flags set.
    static let queuedState = AnalysisState(
        persistedStatus: .queued,
        hasUserPreemptedJob: false,
        hasAppForceQuitFlag: false,
        pendingSinceEnqueuedAt: Date(timeIntervalSince1970: 1_700_000_000),
        hasAnyConfirmedAnalysis: false
    )

    /// A "failed" analysis state — used to drive the resource-exhausted
    /// branch of `taskExpired`.
    static let failedState = AnalysisState(
        persistedStatus: .failed,
        hasUserPreemptedJob: false,
        hasAppForceQuitFlag: true,
        pendingSinceEnqueuedAt: nil,
        hasAnyConfirmedAnalysis: false
    )

    // MARK: - Rule 1: eligibility-blocks wins over everything

    @Test("Rule 1 — ineligibility short-circuits to .unavailable even when a cause is live")
    func eligibilityBlocksWinsOverUserPausedCause() {
        // Even a user-paused cause (highest priority among causes) is
        // suppressed when the device is ineligible, because a pause is
        // meaningless on a device that cannot run analysis at all.
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .userPreempted,
            eligibility: Self.ineligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .unavailable)
        #expect(out.reason == .analysisUnavailable)
        #expect(out.hint == .enableAppleIntelligence)
    }

    @Test("Rule 1 — ineligibility wins even when no cause is live")
    func eligibilityBlocksFiresWithoutCause() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.ineligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .unavailable)
        #expect(out.reason == .analysisUnavailable)
    }

    // MARK: - Rule 2: user-paused beats resource / transient / queued

    @Test("Rule 2 — userPreempted surfaces the cancelled disposition")
    func userPausedUserPreempted() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .userPreempted,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .cancelled)
        #expect(out.reason == .cancelled)
        #expect(out.hint == .retry)
    }

    @Test("Rule 2 — appForceQuitRequiresRelaunch surfaces paused + resumeInApp")
    func userPausedForceQuit() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .appForceQuitRequiresRelaunch,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .paused)
        #expect(out.reason == .resumeInApp)
        #expect(out.hint == .openAppToResume)
    }

    // MARK: - Rule 3: resource-blocks beat transient-waits + queued

    @Test("Rule 3 — mediaCap surfaces failed + storageFull")
    func resourceBlockMediaCap() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .mediaCap,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .failed)
        #expect(out.reason == .storageFull)
        #expect(out.hint == .freeUpStorage)
    }

    @Test("Rule 3 — analysisCap surfaces failed + storageFull")
    func resourceBlockAnalysisCap() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .analysisCap,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .failed)
        #expect(out.reason == .storageFull)
        #expect(out.hint == .freeUpStorage)
    }

    @Test("Rule 3 — taskExpired with persisted .failed state surfaces failed + couldntAnalyze")
    func resourceBlockTaskExpiredTerminal() {
        let out = episodeSurfaceStatus(
            state: Self.failedState,
            cause: .taskExpired,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .failed)
        #expect(out.reason == .couldntAnalyze)
        #expect(out.hint == .retry)
    }

    // MARK: - Rule 4: transient-waits beat queued

    @Test("Rule 4 — thermal surfaces paused + phoneIsHot")
    func transientThermal() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .thermal,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .paused)
        #expect(out.reason == .phoneIsHot)
        #expect(out.hint == .wait)
    }

    @Test("Rule 4 — lowPowerMode surfaces paused + powerLimited + chargeDevice")
    func transientLowPower() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .lowPowerMode,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .paused)
        #expect(out.reason == .powerLimited)
        #expect(out.hint == .chargeDevice)
    }

    @Test("Rule 4 — wifiRequired surfaces paused + waitingForNetwork + connectToWiFi")
    func transientWifiRequired() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .wifiRequired,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .paused)
        #expect(out.reason == .waitingForNetwork)
        #expect(out.hint == .connectToWiFi)
    }

    @Test("Rule 4 — taskExpired with retries remaining (non-failed state) surfaces queued wait")
    func transientTaskExpiredWithRetries() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,   // persistedStatus != .failed
            cause: .taskExpired,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .queued)
        #expect(out.reason == .waitingForTime)
        #expect(out.hint == .wait)
    }

    // MARK: - Rule 5: queued fires when nothing else is live

    @Test("Rule 5 — no cause, eligible device surfaces queued + waitingForTime + wait")
    func queuedDefault() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .queued)
        #expect(out.reason == .waitingForTime)
        #expect(out.hint == .wait)
    }

    // MARK: - Pass-through: readiness + coverage

    @Test("readinessAnchor passes through the reducer unchanged")
    func readinessAnchorPassThrough() {
        let anchor: TimeInterval = 123.5
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: anchor
        )
        #expect(out.readinessAnchor == anchor)
    }

    @Test("nil coverage defaults playbackReadiness to .none")
    func nilCoverageDefaultsNone() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.playbackReadiness == .none)
    }

    @Test("non-nil coverage currently still returns .none (Phase 1.5 stub)")
    func nonNilCoverageStillNoneInStub() {
        let coverage = CoverageSummary(hasAnyCoverage: true)
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: coverage,
            readinessAnchor: nil
        )
        // Phase 2 (playhead-cthe) will flip this assertion when the
        // real CoverageSummary lands. Until then the stub always
        // returns .none.
        #expect(out.playbackReadiness == .none)
    }
}
