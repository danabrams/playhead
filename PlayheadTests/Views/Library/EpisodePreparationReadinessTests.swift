// EpisodePreparationReadinessTests.swift
// playhead-3xtw: exhaustive tests for the PURE state-derivation of the
// per-episode "Download & Analyze on demand" control
// (`deriveEpisodePreparationReadiness` + its cellular / analysis-state /
// caption helpers). No SwiftUI, no live services — inputs → state + two
// fractions, so every state and edge is covered directly.

import Foundation
import Testing
@testable import Playhead

@Suite("EpisodePreparationReadiness — pure derivation")
struct EpisodePreparationReadinessTests {

    /// Builder with sensible "nothing yet" defaults so each test overrides
    /// only the axis it exercises.
    private func inputs(
        isDownloaded: Bool = false,
        downloadInFlight: Bool = false,
        downloadFraction: Double? = nil,
        analysisActive: Bool = false,
        analysisComplete: Bool = false,
        analysisFailed: Bool = false,
        analysisFraction: Double? = nil,
        userInitiated: Bool = false,
        downloadPermitted: Bool = true
    ) -> EpisodePreparationInputs {
        EpisodePreparationInputs(
            isDownloaded: isDownloaded,
            downloadInFlight: downloadInFlight,
            downloadFraction: downloadFraction,
            analysisActive: analysisActive,
            analysisComplete: analysisComplete,
            analysisFailed: analysisFailed,
            analysisFraction: analysisFraction,
            userInitiated: userInitiated,
            downloadPermitted: downloadPermitted
        )
    }

    // MARK: - Resting states

    @Test("nothing prepared, no intent → idle")
    func testIdleAtRest() {
        let r = deriveEpisodePreparationReadiness(inputs())
        #expect(r.state == .idle)
        #expect(r.downloadFraction == 0)
        #expect(r.analysisFraction == 0)
    }

    @Test("downloaded but not analyzed, no intent, not active → idle (tap to analyze)")
    func testDownloadedNotAnalyzedIsIdle() {
        let r = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisFraction: 0)
        )
        #expect(r.state == .idle)
        // Download zone reads full at rest so a later working transition
        // does not appear to lose the download.
        #expect(r.downloadFraction == 1)
        #expect(r.analysisFraction == 0)
    }

    @Test("analysis complete → ready with both zones full")
    func testReady() {
        let r = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisComplete: true, analysisFraction: 1)
        )
        #expect(r.state == .ready)
        #expect(r.downloadFraction == 1)
        #expect(r.analysisFraction == 1)
    }

    @Test("ready wins even on a metered link with no permission")
    func testReadySupersedesCellular() {
        let r = deriveEpisodePreparationReadiness(
            inputs(analysisComplete: true, downloadPermitted: false)
        )
        #expect(r.state == .ready)
    }

    @Test("ready wins even while a stray download/analysis flag is set")
    func testReadySupersedesWorking() {
        let r = deriveEpisodePreparationReadiness(
            inputs(
                isDownloaded: true,
                analysisActive: true,
                analysisComplete: true,
                userInitiated: true
            )
        )
        #expect(r.state == .ready)
    }

    // MARK: - Downloading

    @Test("user tapped + download kicked (in flight) → downloading")
    func testUserInitiatedWithKickDownloading() {
        // The control folds its optimistic "download kicked" hint into
        // `downloadInFlight`, so a fresh tap that actually starts a transfer
        // shows the bar immediately.
        let r = deriveEpisodePreparationReadiness(
            inputs(downloadInFlight: true, userInitiated: true, downloadPermitted: true)
        )
        #expect(r.state == .downloading)
        #expect(r.analysisFraction == 0)
    }

    @Test("intent + permitted but NO transfer in flight → idle (not a stuck 0% bar)")
    func testUserInitiatedPermittedButNotInFlightIsIdle() {
        // This is the post-cellular-block-cleared case (M2): the user's
        // intent is latched and Wi‑Fi is now available, but no transfer is
        // actually running. The control must stay actionable, not strand on
        // a downloading bar that can never advance.
        let r = deriveEpisodePreparationReadiness(
            inputs(userInitiated: true, downloadPermitted: true)
        )
        #expect(r.state == .idle)
    }

    @Test("download in flight → downloading with live fraction, analysis pinned 0")
    func testDownloadInFlightFraction() {
        let r = deriveEpisodePreparationReadiness(
            inputs(downloadInFlight: true, downloadFraction: 0.42)
        )
        #expect(r.state == .downloading)
        #expect(r.downloadFraction == 0.42)
        #expect(r.analysisFraction == 0)
    }

    @Test("in-flight download shows downloading even when the link is now forbidden")
    func testInFlightBeatsCellularGate() {
        let r = deriveEpisodePreparationReadiness(
            inputs(downloadInFlight: true, downloadFraction: 0.5, downloadPermitted: false)
        )
        #expect(r.state == .downloading)
        #expect(r.downloadFraction == 0.5)
    }

    // MARK: - Waiting for Wi‑Fi

    @Test("intent + not downloaded + not permitted + not in flight → waitingForWifi")
    func testWaitingForWifi() {
        let r = deriveEpisodePreparationReadiness(
            inputs(userInitiated: true, downloadPermitted: false)
        )
        #expect(r.state == .waitingForWifi)
        #expect(r.analysisFraction == 0)
    }

    @Test("no intent on a forbidden link → idle, NOT waitingForWifi")
    func testNoIntentOnCellularStaysIdle() {
        let r = deriveEpisodePreparationReadiness(
            inputs(downloadPermitted: false)
        )
        #expect(r.state == .idle)
    }

    // MARK: - Analyzing

    @Test("downloaded + active analysis → analyzing with coverage fraction")
    func testAutoAnalyzingWithoutTap() {
        // No userInitiated — the auto-pipeline drives the working bar.
        let r = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisActive: true, analysisFraction: 0.3)
        )
        #expect(r.state == .analyzing)
        #expect(r.downloadFraction == 1)
        #expect(r.analysisFraction == 0.3)
    }

    @Test("downloaded + terminal analysis FAILURE → resting idle, never stuck analyzing")
    func testTerminalFailureRestsNotStuck() {
        // The failed*/cancelledBudget SessionStates project to
        // PersistedStatus.failed → analysisFailed. Even with the user's
        // intent latched, this must resolve to an actionable resting glyph
        // (tap to retry), not a perpetual analyzing spinner.
        let r = deriveEpisodePreparationReadiness(
            inputs(
                isDownloaded: true,
                analysisActive: false,
                analysisFailed: true,
                analysisFraction: 0.4,
                userInitiated: true
            )
        )
        #expect(r.state == .idle)
    }

    @Test("downloaded + failure but a retry is already running → analyzing")
    func testFailureWithActiveRetryAnalyzes() {
        let r = deriveEpisodePreparationReadiness(
            inputs(
                isDownloaded: true,
                analysisActive: true,
                analysisFailed: true,
                analysisFraction: 0.5,
                userInitiated: true
            )
        )
        #expect(r.state == .analyzing)
    }

    @Test("partial-completion terminals (project to .done) read as ready")
    func testPartialCompletionIsReady() {
        // completeFeatureOnly / completeTranscriptPartial both project to
        // PersistedStatus.done → analysisComplete == true → ready, even at
        // low measured coverage fraction.
        #expect(episodePreparationAnalysisComplete(status: .done, analysisFraction: 0.2))
        let r = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisComplete: true, analysisFraction: 0.2)
        )
        #expect(r.state == .ready)
    }

    @Test("downloaded + user tapped, analysis not yet started → analyzing at 0")
    func testDownloadedThenTapSkipsToAnalyze() {
        let r = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisFraction: nil, userInitiated: true)
        )
        #expect(r.state == .analyzing)
        #expect(r.downloadFraction == 1)
        #expect(r.analysisFraction == 0)
    }

    // MARK: - Fraction clamping / edges

    @Test("fractions above 1 clamp to 1")
    func testClampHigh() {
        let r = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisActive: true, analysisFraction: 1.7)
        )
        #expect(r.analysisFraction == 1)
    }

    @Test("negative / NaN fractions clamp to 0")
    func testClampLowAndNaN() {
        let neg = deriveEpisodePreparationReadiness(
            inputs(downloadInFlight: true, downloadFraction: -0.5)
        )
        #expect(neg.downloadFraction == 0)

        let nan = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisActive: true, analysisFraction: .nan)
        )
        #expect(nan.analysisFraction == 0)
    }

    @Test("0-duration episode: missing analysis fraction collapses to 0, no crash")
    func testZeroDurationMissingCoverage() {
        let r = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisActive: true, analysisFraction: nil)
        )
        #expect(r.state == .analyzing)
        #expect(r.analysisFraction == 0)
    }

    // MARK: - Cellular gate helper

    @Test("wifi always permits download regardless of policy")
    func testWifiPermits() {
        for policy in CellularPolicy.allCases {
            #expect(episodePreparationDownloadPermitted(reachability: .wifi, policy: policy))
        }
    }

    @Test("unreachable never permits download")
    func testUnreachableNeverPermits() {
        for policy in CellularPolicy.allCases {
            #expect(!episodePreparationDownloadPermitted(reachability: .unreachable, policy: policy))
        }
    }

    @Test("cellular permits only when policy is on")
    func testCellularOnlyOn() {
        #expect(episodePreparationDownloadPermitted(reachability: .cellular, policy: .on))
        #expect(!episodePreparationDownloadPermitted(reachability: .cellular, policy: .off))
        #expect(!episodePreparationDownloadPermitted(reachability: .cellular, policy: .askEachTime))
    }

    // MARK: - analysisState mapping

    @Test("analysis active for queued/running only (projected PersistedStatus)")
    func testAnalysisActiveMapping() {
        #expect(episodePreparationAnalysisActive(status: .queued))
        #expect(episodePreparationAnalysisActive(status: .running))
        #expect(!episodePreparationAnalysisActive(status: .new))
        #expect(!episodePreparationAnalysisActive(status: .done))
        #expect(!episodePreparationAnalysisActive(status: .failed))
        #expect(!episodePreparationAnalysisActive(status: .cancelled))
        #expect(!episodePreparationAnalysisActive(status: nil))
    }

    @Test("analysis complete on terminal done OR high coverage; never on failure")
    func testAnalysisCompleteMapping() {
        #expect(episodePreparationAnalysisComplete(status: .done, analysisFraction: nil))
        #expect(episodePreparationAnalysisComplete(status: .done, analysisFraction: 0.1))
        #expect(episodePreparationAnalysisComplete(status: .running, analysisFraction: 0.99))
        #expect(!episodePreparationAnalysisComplete(status: .running, analysisFraction: 0.5))
        #expect(!episodePreparationAnalysisComplete(status: nil, analysisFraction: nil))
        #expect(!episodePreparationAnalysisComplete(status: .running, analysisFraction: .nan))
        // A failed / cancelled job never reads as complete, even at high coverage.
        #expect(!episodePreparationAnalysisComplete(status: .failed, analysisFraction: 0.99))
        #expect(!episodePreparationAnalysisComplete(status: .cancelled, analysisFraction: 1.0))
    }

    // MARK: - Caption + percent

    @Test("captions match the settled copy")
    func testCaptions() {
        let downloading = EpisodePreparationReadiness(
            state: .downloading, downloadFraction: 0.5, analysisFraction: 0
        )
        #expect(episodePreparationCaption(downloading) == "Downloading 50%")

        let analyzing = EpisodePreparationReadiness(
            state: .analyzing, downloadFraction: 1, analysisFraction: 0.3
        )
        #expect(episodePreparationCaption(analyzing) == "Downloaded · analyzing 30%")

        let waiting = EpisodePreparationReadiness(
            state: .waitingForWifi, downloadFraction: 0, analysisFraction: 0
        )
        #expect(episodePreparationCaption(waiting) == "Waiting for Wi‑Fi")

        // Resting states carry no caption (glyph only).
        for state in [EpisodePreparationControlState.idle, .ready] {
            let r = EpisodePreparationReadiness(state: state, downloadFraction: 1, analysisFraction: 1)
            #expect(episodePreparationCaption(r) == nil)
        }
    }

    @Test("percent rounds and clamps")
    func testPercent() {
        #expect(episodePreparationPercent(0) == "0%")
        #expect(episodePreparationPercent(0.301) == "30%")
        #expect(episodePreparationPercent(0.305) == "31%")
        #expect(episodePreparationPercent(1) == "100%")
        #expect(episodePreparationPercent(1.5) == "100%")
        #expect(episodePreparationPercent(-1) == "0%")
    }

    // MARK: - Terminal SessionState → resting disposition (end-to-end)

    private func asset(state: SessionState) -> AnalysisAsset {
        AnalysisAsset(
            id: "asset", episodeId: "ep", assetFingerprint: "fp",
            weakFingerprint: nil, sourceURL: "https://example.com/a.mp3",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: state.rawValue, analysisVersion: 1, capabilitySnapshot: nil
        )
    }

    private func projectedStatus(_ state: SessionState) -> AnalysisState.PersistedStatus {
        EpisodeSurfaceStatusObserver.analysisState(from: asset(state: state)).persistedStatus
    }

    @Test("every completion terminal projects to ready (even degraded-full)")
    func testCompletionTerminalsAreReady() {
        for state in [SessionState.complete, .completeFull, .completeFeatureOnly, .completeTranscriptPartial] {
            let status = projectedStatus(state)
            // Low measured coverage must not matter — the terminal wins.
            #expect(
                episodePreparationAnalysisComplete(status: status, analysisFraction: 0.05),
                "\(state) should read as complete"
            )
        }
    }

    @Test("every failure terminal is neither active nor complete (→ control rests, not stuck)")
    func testFailureTerminalsRestNotStuck() {
        for state in [SessionState.failed, .failedTranscript, .failedFeature, .cancelledBudget] {
            let status = projectedStatus(state)
            #expect(!episodePreparationAnalysisComplete(status: status, analysisFraction: 0.99))
            #expect(!episodePreparationAnalysisActive(status: status))
            // Feeds `analysisFailed`, which the derivation resolves to idle.
            let r = deriveEpisodePreparationReadiness(
                inputs(isDownloaded: true, analysisFailed: true, userInitiated: true)
            )
            #expect(r.state == .idle, "\(state) must not strand at .analyzing")
        }
    }

    // MARK: - Full lifecycle sweep (idle → downloading → analyzing → ready)

    @Test("control advances through the lifecycle as progress advances")
    func testLifecycleAdvance() {
        // idle
        #expect(deriveEpisodePreparationReadiness(inputs()).state == .idle)
        // user taps and a transfer starts (control folds the kick into downloadInFlight)
        #expect(deriveEpisodePreparationReadiness(
            inputs(downloadInFlight: true, userInitiated: true)
        ).state == .downloading)
        // bytes flowing
        #expect(deriveEpisodePreparationReadiness(
            inputs(downloadInFlight: true, downloadFraction: 0.6, userInitiated: true)
        ).state == .downloading)
        // download done, analysis running
        #expect(deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisActive: true, analysisFraction: 0.2, userInitiated: true)
        ).state == .analyzing)
        // coverage reaches the end
        #expect(deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisComplete: true, analysisFraction: 1, userInitiated: true)
        ).state == .ready)
    }
}
