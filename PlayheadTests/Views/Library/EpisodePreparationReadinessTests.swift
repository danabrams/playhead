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
        analysisTerminatedComplete: Bool = false,
        analysisFailed: Bool = false,
        adScanFraction: ReachRatio? = nil,
        userInitiated: Bool = false,
        downloadPermitted: Bool = true
    ) -> EpisodePreparationInputs {
        EpisodePreparationInputs(
            isDownloaded: isDownloaded,
            downloadInFlight: downloadInFlight,
            downloadFraction: downloadFraction,
            analysisActive: analysisActive,
            analysisComplete: analysisComplete,
            analysisTerminatedComplete: analysisTerminatedComplete,
            analysisFailed: analysisFailed,
            adScanFraction: adScanFraction,
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
            inputs(isDownloaded: true, adScanFraction: 0)
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
            inputs(isDownloaded: true, analysisComplete: true, adScanFraction: 1)
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
            inputs(isDownloaded: true, analysisActive: true, adScanFraction: 0.3)
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
                adScanFraction: 0.4,
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
                adScanFraction: 0.5,
                userInitiated: true
            )
        )
        #expect(r.state == .analyzing)
    }

    // MARK: - playhead-pz32: terminal-but-short → ◐, never a calm ✓
    //
    // SPEC CHANGE (deliberate). The two tests replaced here asserted the
    // defect: a terminal `.done` — which every completion terminal projects
    // to, degraded ones included — made the control read `.ready` at ANY
    // measured coverage. That is precisely the behaviour that told the
    // product owner a 47%-scanned episode was analysed.

    @Test("terminal .done at low ad-scan coverage is NOT complete (pz32)")
    func testTerminalDoneAtLowCoverageIsNotComplete() {
        // A terminal `.done` says "the pipeline stopped", never "the audio was
        // read for ads". Only measured coverage can satisfy the ✓.
        #expect(!episodePreparationAnalysisComplete(
            status: .done, adScanFraction: 0.2, isDegradedTerminal: false
        ))
        // …and unknown coverage is not a licence either.
        #expect(!episodePreparationAnalysisComplete(
            status: .done, adScanFraction: nil, isDegradedTerminal: false
        ))
        // A completion terminal that fell short rests on ◐, not a spinner and
        // not a checkmark.
        let r = deriveEpisodePreparationReadiness(
            inputs(
                isDownloaded: true,
                analysisTerminatedComplete: true,
                adScanFraction: 0.2
            )
        )
        #expect(r.state == .partiallyAnalyzed)
        #expect(r.downloadFraction == 1)
        #expect(r.analysisFraction == 0.2)
    }

    @Test("a completion terminal with a live retry still shows the working bar")
    func testTerminalCompleteWithActiveRetryAnalyzes() {
        // `.partiallyAnalyzed` means "stopped short". If a job is running, the
        // honest render is the working bar — otherwise a re-drive would look
        // like it had already given up.
        let r = deriveEpisodePreparationReadiness(
            inputs(
                isDownloaded: true,
                analysisActive: true,
                analysisTerminatedComplete: true,
                adScanFraction: 0.2
            )
        )
        #expect(r.state == .analyzing)
    }

    @Test("no ad-scan measurement at a completion terminal → ◐ with NO fabricated percentage")
    func testTerminalWithNoMeasurementSaysAmountUnknown() {
        // `semantic_scan_results` rows are deleted whenever the scan cohort revs
        // — prompt/schema/plan/normalization bumps, a locale change, an OS update,
        // or simply a new app build (`pruneOrphanedScansForCurrentCohort`). The
        // coverage is then genuinely UNKNOWN. ◐ is still the honest STATE, but the
        // spoken value must not claim "0% scanned" for a quantity nobody measured.
        for unmeasured: ReachRatio? in [nil, ReachRatio(.nan), ReachRatio(.infinity), ReachRatio(-.infinity)] {
            let readiness = deriveEpisodePreparationReadiness(
                inputs(
                    isDownloaded: true,
                    analysisTerminatedComplete: true,
                    adScanFraction: unmeasured
                )
            )
            #expect(readiness.state == .partiallyAnalyzed)
            #expect(readiness.downloadFraction == 1)
            #expect(!readiness.analysisFractionIsMeasured)
            #expect(
                episodePreparationAccessibilityValue(readiness) == "Amount scanned unknown",
                "\(String(describing: unmeasured)) must not be spoken as a percentage"
            )
        }
        // A MEASURED zero is different: 0 is a real answer and is spoken as one.
        let measuredZero = deriveEpisodePreparationReadiness(
            inputs(
                isDownloaded: true,
                analysisTerminatedComplete: true,
                adScanFraction: 0
            )
        )
        #expect(measuredZero.state == .partiallyAnalyzed)
        #expect(measuredZero.analysisFractionIsMeasured)
        #expect(episodePreparationAccessibilityValue(measuredZero) == "0% scanned for ads")
    }

    @Test("an in-flight download outranks ◐ so the live transfer stays visible")
    func testDownloadInFlightOutranksPartiallyAnalyzed() {
        // Re-downloading evicted audio for a terminal-but-short episode: a
        // frozen ◐ would hide the transfer that is actually happening.
        let readiness = deriveEpisodePreparationReadiness(
            inputs(
                isDownloaded: false,
                downloadInFlight: true,
                downloadFraction: 0.42,
                analysisTerminatedComplete: true,
                adScanFraction: 0.47
            )
        )
        #expect(readiness.state == .downloading)
        #expect(readiness.downloadFraction == 0.42)
    }

    @Test("terminal-but-short on an un-downloaded episode still reads ◐, not ✦")
    func testTerminalShortWithoutCachedAudio() {
        // The audio cache can be evicted after analysis; a completion terminal
        // is a statement about analysis, so it outranks the download branches.
        let r = deriveEpisodePreparationReadiness(
            inputs(
                isDownloaded: false,
                analysisTerminatedComplete: true,
                adScanFraction: 0.47
            )
        )
        #expect(r.state == .partiallyAnalyzed)
        #expect(r.downloadFraction == 0)
        #expect(r.analysisFraction == 0.47)
    }

    @Test("◐ is not actionable-by-derivation: full coverage still wins the ✓")
    func testTerminalCompleteAtFullCoverageIsReady() {
        // The ◐ branch must not swallow genuinely-complete episodes: a
        // non-degraded terminal at full measured coverage is still ✓.
        #expect(episodePreparationAnalysisComplete(
            status: .done, adScanFraction: 1.0, isDegradedTerminal: false
        ))
        let r = deriveEpisodePreparationReadiness(
            inputs(
                isDownloaded: true,
                analysisComplete: true,
                analysisTerminatedComplete: true,
                adScanFraction: 1.0
            )
        )
        #expect(r.state == .ready)
    }

    @Test("downloaded + user tapped, analysis not yet started → analyzing at 0")
    func testDownloadedThenTapSkipsToAnalyze() {
        let r = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, adScanFraction: nil, userInitiated: true)
        )
        #expect(r.state == .analyzing)
        #expect(r.downloadFraction == 1)
        #expect(r.analysisFraction == 0)
    }

    // MARK: - Fraction clamping / edges

    @Test("fractions above 1 clamp to 1")
    func testClampHigh() {
        let r = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisActive: true, adScanFraction: 1.7)
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
            inputs(isDownloaded: true, analysisActive: true, adScanFraction: ReachRatio(.nan))
        )
        #expect(nan.analysisFraction == 0)
    }

    @Test("0-duration episode: missing analysis fraction collapses to 0, no crash")
    func testZeroDurationMissingCoverage() {
        let r = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisActive: true, adScanFraction: nil)
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

    @Test("analysis complete ONLY on high measured ad-scan coverage (pz32)")
    func testAnalysisCompleteMapping() {
        // Measured coverage is the only route to complete — not a terminal
        // status, not an unknown fraction.
        #expect(episodePreparationAnalysisComplete(
            status: .running, adScanFraction: 0.99, isDegradedTerminal: false
        ))
        #expect(episodePreparationAnalysisComplete(
            status: .done, adScanFraction: 1.0, isDegradedTerminal: false
        ))
        // A status with no asset row / unknown coverage never reads complete.
        #expect(!episodePreparationAnalysisComplete(
            status: nil, adScanFraction: nil, isDegradedTerminal: false
        ))
        #expect(!episodePreparationAnalysisComplete(
            status: .done, adScanFraction: nil, isDegradedTerminal: false
        ))
        #expect(!episodePreparationAnalysisComplete(
            status: .running, adScanFraction: 0.5, isDegradedTerminal: false
        ))
        #expect(!episodePreparationAnalysisComplete(
            status: .running, adScanFraction: ReachRatio(.nan), isDegradedTerminal: false
        ))
        // A failed / cancelled job never reads as complete, even at high coverage.
        #expect(!episodePreparationAnalysisComplete(
            status: .failed, adScanFraction: 0.99, isDegradedTerminal: false
        ))
        #expect(!episodePreparationAnalysisComplete(
            status: .cancelled, adScanFraction: 1.0, isDegradedTerminal: false
        ))
        // A degraded terminal never reads as complete, even at full measured
        // coverage — the pipeline itself said it stopped short, and no
        // coverage-measurement bug may override that.
        #expect(!episodePreparationAnalysisComplete(
            status: .done, adScanFraction: 1.0, isDegradedTerminal: true
        ))
    }

    @Test("the threshold is exactly 0.98 and is applied to ad-scan coverage")
    func testCompleteThresholdBoundary() {
        // Pins the cutoff so a future "make the ✓ easier to reach" edit has to
        // change a test that says why it must not.
        #expect(episodePreparationCompleteThreshold == 0.98)
        #expect(episodePreparationAnalysisComplete(
            status: .running, adScanFraction: 0.98, isDegradedTerminal: false
        ))
        #expect(!episodePreparationAnalysisComplete(
            status: .running, adScanFraction: 0.9799, isDegradedTerminal: false
        ))
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
        for state in [EpisodePreparationControlState.idle, .ready, .partiallyAnalyzed] {
            let r = EpisodePreparationReadiness(state: state, downloadFraction: 1, analysisFraction: 1)
            #expect(episodePreparationCaption(r) == nil)
        }
    }

    // MARK: - playhead-pz32: accessibility labels are state-distinct

    @Test("every control state has a UNIQUE accessibility label")
    func testAccessibilityLabelsAreUnique() {
        // The load-bearing property: a screen-reader user must be able to tell
        // "analysed" from "partly analysed". Asserting global uniqueness (not
        // just that one pair differs) means no future state can be added that
        // silently shares a label with another.
        var labels: [String: EpisodePreparationControlState] = [:]
        for state in EpisodePreparationControlState.allCases {
            let readiness = EpisodePreparationReadiness(
                state: state,
                downloadFraction: 1,
                analysisFraction: 0.47,
                analysisFractionIsMeasured: true
            )
            let label = episodePreparationAccessibilityLabel(readiness)
            #expect(!label.isEmpty, "\(state) has an empty accessibility label")
            #expect(
                labels[label] == nil,
                "\(state) shares the accessibility label \"\(label)\" with \(String(describing: labels[label]))"
            )
            labels[label] = state
        }
        #expect(labels.count == EpisodePreparationControlState.allCases.count)
    }

    @Test("partly-analyzed announces itself as partial AND says how much")
    func testPartiallyAnalyzedAccessibility() {
        let partial = EpisodePreparationReadiness(
            state: .partiallyAnalyzed,
            downloadFraction: 1,
            analysisFraction: 0.47,
            analysisFractionIsMeasured: true
        )
        let ready = EpisodePreparationReadiness(
            state: .ready, downloadFraction: 1, analysisFraction: 1
        )
        #expect(episodePreparationAccessibilityLabel(partial) == "Partly analyzed")
        #expect(episodePreparationAccessibilityLabel(ready) == "Analysis ready")
        // The value carries the honest number so "partly" is actionable.
        #expect(episodePreparationAccessibilityValue(partial) == "47% scanned for ads")
        #expect(episodePreparationAccessibilityValue(ready) == "")
    }

    @Test("working-state accessibility values still mirror the visible caption")
    func testWorkingAccessibilityValuesMirrorCaption() {
        for state in [EpisodePreparationControlState.downloading, .analyzing, .waitingForWifi, .idle] {
            let readiness = EpisodePreparationReadiness(
                state: state, downloadFraction: 0.5, analysisFraction: 0.3
            )
            #expect(
                episodePreparationAccessibilityValue(readiness)
                    == (episodePreparationCaption(readiness) ?? ""),
                "\(state) accessibility value drifted from its caption"
            )
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

    private static let assetId = "asset"
    private static let episodeDuration: Double = 1000

    /// An asset whose every DISCREDITED watermark is parked at the end of the
    /// episode. Any implementation that resolves readiness from
    /// `featureCoverageEndTime` or `confirmedAdCoverageEndTime` reads 1.0 here.
    private func asset(state: SessionState) -> AnalysisAsset {
        AnalysisAsset(
            id: Self.assetId, episodeId: "ep", assetFingerprint: "fp",
            weakFingerprint: nil, sourceURL: "https://example.com/a.mp3",
            featureCoverageEndTime: Self.episodeDuration,
            fastTranscriptCoverageEndTime: Self.episodeDuration,
            confirmedAdCoverageEndTime: Self.episodeDuration,
            analysisState: state.rawValue, analysisVersion: 1, capabilitySnapshot: nil,
            episodeDurationSec: Self.episodeDuration
        )
    }

    /// A coverage summary in which EVERY scalar except `adScanCoveredSec` is at
    /// full coverage — including the gap-aware `analysisCoveredSec` the Activity
    /// screen shows. So a mutation that swaps the readiness predicate onto any
    /// other scalar of this same read model is caught, not just one that reverts
    /// to the raw asset watermarks.
    private func coverage(assetId: String = assetId, adScanCoveredSec: CoveredSeconds?) -> AnalysisCoverageSummary {
        AnalysisCoverageSummary(
            assetId: assetId,
            episodeDurationSec: EpisodeSeconds(Self.episodeDuration),
            fastTranscriptCoveredSec: CoveredSeconds(Self.episodeDuration),
            fastTranscriptCoveredSource: .fastTranscriptChunks,
            fastTranscriptCoverageEndSec: WatermarkSeconds(Self.episodeDuration),
            fastTranscriptCoverageEndSource: .fastTranscriptChunks,
            featureCoverageEndSec: WatermarkSeconds(Self.episodeDuration),
            featureCoverageEndSource: .assetWatermark,
            confirmedAdCoverageEndSec: WatermarkSeconds(Self.episodeDuration),
            confirmedAdCoverageEndSource: .assetWatermark,
            finalPassCoverageEndSec: WatermarkSeconds(Self.episodeDuration),
            finalPassCoverageEndSource: .finalPassChunks,
            analysisCoveredSec: CoveredSeconds(Self.episodeDuration),
            adScanCoveredSec: adScanCoveredSec,
            adScanCoveredSource: adScanCoveredSec == nil ? .unknown : .semanticScanResults
        )
    }

    private func projectedStatus(_ state: SessionState) -> AnalysisState.PersistedStatus {
        EpisodeSurfaceStatusObserver.analysisState(from: asset(state: state)).persistedStatus
    }

    /// Drives the PRODUCTION projection (`episodePreparationAnalysisInputs`) —
    /// the same call `EpisodePreparationStatusModel.refresh` makes — rather than
    /// re-deriving the booleans in the test. Without this, swapping the model's
    /// one `adScanFraction` line back to any other quantity was a mutation the
    /// whole suite let through.
    private func derive(
        sessionState: SessionState,
        adScanFraction: ReachRatio?,
        isDownloaded: Bool = true,
        // Attribute the in-helper expectation to the CALLER, otherwise all eight
        // call sites report the same useless line number on failure.
        sourceLocation: SourceLocation = #_sourceLocation
    ) -> EpisodePreparationReadiness {
        let analysis = episodePreparationAnalysisInputs(
            asset: asset(state: sessionState),
            coverage: coverage(
                adScanCoveredSec: adScanFraction.map { CoveredSeconds($0.rawValue * Self.episodeDuration) }
            )
        )
        #expect(analysis.adScanFraction == adScanFraction, sourceLocation: sourceLocation)
        return deriveEpisodePreparationReadiness(
            inputs(
                isDownloaded: isDownloaded,
                analysisActive: analysis.analysisActive,
                analysisComplete: analysis.analysisComplete,
                analysisTerminatedComplete: analysis.analysisTerminatedComplete,
                analysisFailed: analysis.analysisFailed,
                adScanFraction: analysis.adScanFraction
            )
        )
    }

    // MARK: - playhead-pz32: the persisted-artifact projection

    @Test("projection reads ad-scan coverage, not any watermark on the same row")
    func testProjectionReadsAdScanCoverage() {
        // Every other coverage scalar is at 1000/1000 here; only the ad-scan area
        // is short. This is the mutation guard for the single line the bead is
        // about — any other scalar yields 1.0 and flips the ✓ back on.
        let analysis = episodePreparationAnalysisInputs(
            asset: asset(state: .backfill),
            coverage: coverage(adScanCoveredSec: 470)
        )
        #expect(analysis.adScanFraction == 0.47)
        #expect(!analysis.analysisComplete)
        #expect(analysis.analysisActive)
        #expect(!analysis.analysisTerminatedComplete)
        #expect(!analysis.analysisFailed)
    }

    @Test("projection distinguishes the four completion terminals from the RAW column")
    func testProjectionUsesRawAnalysisStateColumn() {
        // Passing the PROJECTED status string instead of the raw column would
        // make `analysisTerminatedComplete` always false and disarm the degraded
        // guard entirely, because `PersistedStatus.done`'s raw value is not a
        // `SessionState` raw value.
        for state in SessionState.allCases {
            let analysis = episodePreparationAnalysisInputs(
                asset: asset(state: state),
                coverage: coverage(adScanCoveredSec: CoveredSeconds(Self.episodeDuration))
            )
            #expect(
                analysis.analysisTerminatedComplete == state.isTerminalCompletion,
                "\(state) terminal-completion projection is wrong"
            )
            // At FULL coverage only the two non-degraded completion terminals —
            // and the still-running states — may read complete.
            let mayBeComplete = !state.isDegradedTerminalCompletion && !state.isTerminalFailure
            #expect(
                analysis.analysisComplete == mayBeComplete,
                "\(state) completeness at full coverage should be \(mayBeComplete)"
            )
        }
    }

    @Test("projection with no asset row claims nothing")
    func testProjectionWithoutAssetClaimsNothing() {
        let analysis = episodePreparationAnalysisInputs(
            asset: nil,
            coverage: coverage(adScanCoveredSec: CoveredSeconds(Self.episodeDuration))
        )
        #expect(analysis == EpisodePreparationAnalysisInputs())
        #expect(analysis.adScanFraction == nil)
        #expect(!analysis.analysisComplete)
    }

    @Test("a coverage summary for a DIFFERENT asset is ignored, not borrowed")
    func testProjectionRejectsMismatchedCoverage() {
        // A mis-joined batch read must under-claim rather than lend one
        // episode's coverage to another and light every row's ✓ at once.
        let analysis = episodePreparationAnalysisInputs(
            asset: asset(state: .completeFull),
            coverage: coverage(assetId: "some-other-asset", adScanCoveredSec: CoveredSeconds(Self.episodeDuration))
        )
        #expect(analysis.adScanFraction == nil)
        #expect(!analysis.analysisComplete)
    }

    /// playhead-pz32 acceptance (c): each DEGRADED terminal must be
    /// distinguishable from `.complete`. Held at IDENTICAL full coverage so the
    /// only varying input is the terminal itself — this fails on the old
    /// behaviour, where all four projected to `.done` and rendered ✓.
    @Test("degraded terminals are distinguishable from .complete at equal coverage")
    func testDegradedTerminalsDistinguishableFromComplete() {
        for state in [SessionState.complete, .completeFull] {
            #expect(
                derive(sessionState: state, adScanFraction: 1.0).state == .ready,
                "\(state) at full ad-scan coverage should be ready"
            )
        }
        // playhead-gqx4 adds `.completeAdScanPartial` to the degraded set: the
        // transcript and features are complete but the audio was not read for
        // ads, which is the commonest real-hardware shape and used to be
        // indistinguishable from a full analysis.
        for state in [
            SessionState.completeFeatureOnly, .completeTranscriptPartial, .completeAdScanPartial
        ] {
            #expect(
                derive(sessionState: state, adScanFraction: 1.0).state == .partiallyAnalyzed,
                "\(state) must never render the same calm ✓ as .complete"
            )
        }
        // And the classifier itself agrees, exhaustively over every case.
        for state in SessionState.allCases {
            let expected = state == .completeFeatureOnly
                || state == .completeTranscriptPartial
                || state == .completeAdScanPartial
            #expect(
                state.isDegradedTerminalCompletion == expected,
                "\(state).isDegradedTerminalCompletion should be \(expected)"
            )
        }
    }

    /// playhead-pz32: `episodePreparationTerminalCompletion` reads the RAW
    /// column, so it must recognise all four completion terminals, reject the
    /// non-terminals and failures, and tolerate an unknown string.
    @Test("terminal-completion classification over every persisted state string")
    func testTerminalCompletionClassification() {
        for state in SessionState.allCases {
            let resolved = episodePreparationTerminalCompletion(analysisState: state.rawValue)
            #expect(
                (resolved != nil) == state.isTerminalCompletion,
                "\(state) terminal-completion classification disagrees with SessionState"
            )
            #expect(resolved == nil || resolved == state)
        }
        #expect(episodePreparationTerminalCompletion(analysisState: "someFutureState") == nil)
        #expect(episodePreparationTerminalCompletion(analysisState: "") == nil)
    }

    @Test("every failure terminal is neither active nor complete (→ control rests, not stuck)")
    func testFailureTerminalsRestNotStuck() {
        for state in [SessionState.failed, .failedTranscript, .failedFeature, .cancelledBudget] {
            let status = projectedStatus(state)
            #expect(!episodePreparationAnalysisComplete(
                status: status, adScanFraction: 0.99, isDegradedTerminal: false
            ))
            #expect(!episodePreparationAnalysisActive(status: status))
            // Feeds `analysisFailed`, which the derivation resolves to idle.
            let r = deriveEpisodePreparationReadiness(
                inputs(isDownloaded: true, analysisFailed: true, userInitiated: true)
            )
            #expect(r.state == .idle, "\(state) must not strand at .analyzing")
            // End-to-end: a failure terminal never reaches ✓ or ◐.
            #expect(derive(sessionState: state, adScanFraction: 1.0).state == .idle)
        }
    }

    /// playhead-pz32 acceptance (a) + (b): the two shapes that lit the ✓ on
    /// Dan's asset 820134BF. Both are expressed in the terms the OLD predicate
    /// used, so both FAIL on the pre-pz32 behaviour.
    @Test("(a) fully-swept DSP watermark + low ad-scan coverage is NOT ready")
    func testFullFeatureWatermarkLowScanIsNotReady() {
        // The old predicate was max(featureCoverageEndTime,
        // confirmedAdCoverageEndTime) / duration >= 0.98. Feature extraction
        // sweeps the whole episode independently of the semantic scan, so this
        // asset had featureCoverageEndTime == duration while only 47% of the
        // audio had ever been read for ads. `analysisState` was `backfill`.
        let readiness = derive(sessionState: .backfill, adScanFraction: 0.47)
        #expect(readiness.state != .ready)
        #expect(readiness.state == .analyzing)
        // The bar and caption report the honest number, not the DSP watermark.
        #expect(readiness.analysisFraction == 0.47)
        #expect(episodePreparationCaption(readiness) == "Downloaded · analyzing 47%")
    }

    @Test("(b) one late ad detection + near-zero ad-scan coverage is NOT ready")
    func testLateAdDetectionAloneIsNotReady() {
        // `confirmedAdCoverageEndTime` is max(endTime) OF DETECTED AD WINDOWS,
        // so a single detection at the end of the episode used to drive the
        // predicate to ~1.0 with nothing scanned. Perversely, an episode where
        // detection did WORSE could look MORE complete. Ad-scan coverage is
        // indifferent to where detections landed.
        for sessionState in [SessionState.backfill, .completeFull, .complete] {
            let readiness = derive(sessionState: sessionState, adScanFraction: 0.01)
            #expect(readiness.state != .ready, "\(sessionState) must not read ready at 1% scanned")
        }
        // Zero and unknown coverage are both under-claims, never ✓ — but they are
        // different claims: 0 is measured, nil is not, and only the first may be
        // spoken as a number.
        let measuredZero = derive(sessionState: .completeFull, adScanFraction: 0)
        #expect(measuredZero.state == .partiallyAnalyzed)
        #expect(measuredZero.analysisFractionIsMeasured)
        let unmeasured = derive(sessionState: .completeFull, adScanFraction: nil)
        #expect(unmeasured.state == .partiallyAnalyzed)
        #expect(!unmeasured.analysisFractionIsMeasured)
        #expect(derive(sessionState: .backfill, adScanFraction: nil).state == .analyzing)
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
            inputs(isDownloaded: true, analysisActive: true, adScanFraction: 0.2, userInitiated: true)
        ).state == .analyzing)
        // coverage reaches the end
        #expect(deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisComplete: true, adScanFraction: 1, userInitiated: true)
        ).state == .ready)
    }
}
