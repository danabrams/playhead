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

    // MARK: - Cause-routing coverage for variants NOT in the matrix
    //
    // The snapshot matrix keeps only one cause per emitted
    // (disposition, reason) pair (see `EpisodeSurfaceStatusMatrix`).
    // These targeted tests pin the routing for the additional cause
    // variants that share those pairs — a regression that silently
    // re-routes one of them to a different tier would otherwise slip
    // past the matrix. They also pin the reducer's default-branch and
    // forward-compat `.unknown` handling.
    //
    // NOTE: the expected triples below reflect the EpisodeSurfaceStatus
    // reducer's CURRENT output. They do NOT necessarily match the
    // `CauseAttributionPolicy` table in playhead-dfem — that table
    // classifies causes at the attribution layer, while the reducer
    // applies its own ladder on top. Any divergence is deliberately
    // pinned here so a future refactor must audit the mapping.
    //
    // Divergences from dfem's CauseAttributionPolicy worth flagging:
    //   * `.noRuntimeGrant`           dfem → (queued, waitingForTime)
    //                                 reducer → (failed, couldntAnalyze)
    //   * `.unsupportedEpisodeLanguage` dfem → (unavailable, analysisUnavailable)
    //                                 reducer → (failed, couldntAnalyze)
    //   * `.noNetwork`                 dfem → (paused, waitingForNetwork, none)
    //                                 reducer → (paused, waitingForNetwork, wait)
    //   * `.userCancelled`             dfem → (paused, cancelled)
    //                                 reducer → (cancelled, cancelled)
    //   * `.modelTemporarilyUnavailable` when modelAvailableNow=false:
    //                                 reducer Rule 1 fires first because
    //                                 `isFullyEligible` returns false, so
    //                                 the result is (unavailable,
    //                                 analysisUnavailable, enableAppleIntelligence)
    //                                 — this happens to match dfem's
    //                                 CauseAttributionPolicy intent for
    //                                 this combination.
    //
    // These divergences are NOT changed in this polish pass; they are
    // tracked here so a future bead can decide whether to reconcile.

    @Test("default-branch — noRuntimeGrant routes to failed + couldntAnalyze + retry")
    func defaultBranchNoRuntimeGrant() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .noRuntimeGrant,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .failed)
        #expect(out.reason == .couldntAnalyze)
        #expect(out.hint == .retry)
    }

    @Test("default-branch — asrFailed routes to failed + couldntAnalyze + retry")
    func defaultBranchAsrFailed() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .asrFailed,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .failed)
        #expect(out.reason == .couldntAnalyze)
        #expect(out.hint == .retry)
    }

    @Test("default-branch — pipelineError routes to failed + couldntAnalyze + retry")
    func defaultBranchPipelineError() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .pipelineError,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .failed)
        #expect(out.reason == .couldntAnalyze)
        #expect(out.hint == .retry)
    }

    @Test("default-branch — unsupportedEpisodeLanguage routes to failed + couldntAnalyze + retry")
    func defaultBranchUnsupportedEpisodeLanguage() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .unsupportedEpisodeLanguage,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .failed)
        #expect(out.reason == .couldntAnalyze)
        #expect(out.hint == .retry)
    }

    @Test("Rule 4 — noNetwork routes to paused + waitingForNetwork + wait")
    func transientNoNetwork() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .noNetwork,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .paused)
        #expect(out.reason == .waitingForNetwork)
        #expect(out.hint == .wait)
    }

    @Test("Rule 4 — batteryLowUnplugged routes to paused + powerLimited + chargeDevice")
    func transientBatteryLowUnplugged() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .batteryLowUnplugged,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .paused)
        #expect(out.reason == .powerLimited)
        #expect(out.hint == .chargeDevice)
    }

    @Test("Rule 4 — modelTemporarilyUnavailable routes to queued + waitingForTime + wait (modelAvailableNow=true)")
    func transientModelTemporarilyUnavailableAvailable() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .modelTemporarilyUnavailable,
            eligibility: Self.eligible, // modelAvailableNow=true
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .queued)
        #expect(out.reason == .waitingForTime)
        #expect(out.hint == .wait)
    }

    @Test("Rule 1 — modelTemporarilyUnavailable with modelAvailableNow=false short-circuits to unavailable via the eligibility gate")
    func transientModelTemporarilyUnavailableUnavailable() {
        // `modelAvailableNow=false` flips `isFullyEligible` to false,
        // which makes Rule 1 fire before Rule 4 is even consulted. The
        // cause argument is effectively ignored in this branch — the
        // reducer surfaces the eligibility-blocked `.unavailable` row
        // with the `enableAppleIntelligence` hint. This pins the
        // interaction: Rule 1 is load-bearing for the runtime-gone-dark
        // path.
        let modelUnavailable = AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: false,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .modelTemporarilyUnavailable,
            eligibility: modelUnavailable,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .unavailable)
        #expect(out.reason == .analysisUnavailable)
        #expect(out.hint == .enableAppleIntelligence)
    }

    @Test("Rule 2 — userCancelled routes to cancelled + cancelled + retry")
    func userPausedUserCancelled() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .userCancelled,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(out.disposition == .cancelled)
        #expect(out.reason == .cancelled)
        #expect(out.hint == .retry)
    }

    @Test("forward-compat — .unknown(\"futureCause\") routes through the fallback default-branch")
    func defaultBranchUnknown() {
        let out = episodeSurfaceStatus(
            state: Self.queuedState,
            cause: .unknown("futureCause"),
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        // `.unknown` is not recognized by any tier classifier, so it
        // falls through to the default branch — the conservative
        // (failed, couldntAnalyze, retry) row shared with engine
        // errors. This is the forward-compat path for causes added
        // after a release.
        #expect(out.disposition == .failed)
        #expect(out.reason == .couldntAnalyze)
        #expect(out.hint == .retry)
    }
}
