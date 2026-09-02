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

    /// playhead-bcpj narrowed this loop, and the narrowing is the finding.
    ///
    /// `.idle` was in a list called "working-state" while this file's own
    /// vocabulary calls it RESTING, alongside `.partiallyAnalyzed` and
    /// `.ready`. The mirror invariant is a property of the working states,
    /// where the caption is the visible text and the value must not drift from
    /// it. A resting state has no caption at all, so mirroring one only ever
    /// asserted that its value is empty — which is a different claim wearing
    /// this test's name, and it is the claim bcpj deliberately retires: a
    /// downloaded episode waiting its turn now SAYS so.
    ///
    /// `.partiallyAnalyzed` is the precedent and it predates this bead: it
    /// carries "47 % scanned for ads" against a nil caption, and the test above
    /// pins that. It was never in this loop either.
    ///
    /// The three genuine working states keep the invariant, and `.idle`'s two
    /// values are pinned in `EpisodePreparationIdleDownloadedTests`.
    @Test("working-state accessibility values still mirror the visible caption")
    func testWorkingAccessibilityValuesMirrorCaption() {
        for state in [EpisodePreparationControlState.downloading, .analyzing, .waitingForWifi] {
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
    private func coverage(
        assetId: String = assetId,
        adScanCoveredSec: AdScanSeconds?,
        adScanCeilingSec: BridgedTranscriptSeconds = BridgedTranscriptSeconds(EpisodePreparationReadinessTests.episodeDuration)
    ) -> AnalysisCoverageSummary {
        AnalysisCoverageSummary(
            assetId: assetId,
            episodeDurationSec: EpisodeSeconds(Self.episodeDuration),
            fastTranscriptCoveredSec: CoveredSeconds(Self.episodeDuration),
            fastTranscriptCoveredSource: .fastTranscriptChunks,
            fastTranscriptCoverageEndSec: WatermarkSeconds(Self.episodeDuration),
            fastTranscriptCoverageEndSource: .fastTranscriptChunks,
            featureCoverageEndSec: FrontierSeconds(Self.episodeDuration),
            featureCoverageEndSource: .assetWatermark,
            confirmedAdCoverageEndSec: FrontierSeconds(Self.episodeDuration),
            confirmedAdCoverageEndSource: .assetWatermark,
            finalPassCoverageEndSec: WatermarkSeconds(Self.episodeDuration),
            finalPassCoverageEndSource: .finalPassChunks,
            analysisCoveredSec: AnalyzedSeconds(Self.episodeDuration),
            adScanCoveredSec: adScanCoveredSec,
            adScanCoveredSource: adScanCoveredSec == nil ? .unknown : .semanticScanResults,
            // playhead-nffz: full by default, so the ceiling cannot be what
            // withholds the ✓ in any case that does not ask for it.
            adScanCeilingSec: adScanCeilingSec
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
                adScanCoveredSec: adScanFraction.map { AdScanSeconds($0.rawValue * Self.episodeDuration) }
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
                coverage: coverage(adScanCoveredSec: AdScanSeconds(Self.episodeDuration))
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

    /// playhead-nffz — WHAT THE LISTENER SEES, and the one thing this bead was
    /// forbidden to do.
    ///
    /// C065AD03's shape: 44 % of the episode transcribed, all of it scanned. Its
    /// scan is at 98.8 % of everything it could ever read, so ANY fix that
    /// "compares like with like" by dividing the scan by its own ceiling puts a
    /// green ✓ on an episode where the listener will hear every ad in the other
    /// 56 %. playhead-pz32 and playhead-gqx4 are the precedent — this repo has
    /// already shipped `completeFull` declared at 3 % scan once.
    ///
    /// So the readiness ruler is `adScanFraction` and NOTHING ELSE, and this test
    /// is what fails if the ceiling ever reaches it.
    @Test("playhead-nffz — the ✓ ruler is the fraction, never the ceiling: a 44 %-transcribed episode stays ◐")
    func testCeilingIsNotTheReadinessRuler() {
        let analysis = episodePreparationAnalysisInputs(
            asset: asset(state: .completeFull),
            // 440 s scanned of a 1,000 s episode, and 445 s is all there is to
            // scan — 98.9 % of the ceiling, 44.0 % of the episode.
            coverage: coverage(
                adScanCoveredSec: 440,
                adScanCeilingSec: BridgedTranscriptSeconds(445)
            )
        )
        #expect(analysis.adScanFraction == 0.44,
                "the projection carries the EPISODE fraction, the quantity the ✓ is calibrated on")
        #expect(!analysis.analysisComplete,
                "a 44 %-scanned episode is not ready, however little else there was to read")
        #expect(!episodePreparationAnalysisComplete(
            status: .done, adScanFraction: analysis.adScanFraction, isDegradedTerminal: false
        ))
        // And the ratio that WOULD flip it, computed here so the near-miss is on
        // the record rather than implicit.
        #expect(440.0 / 445.0 > episodePreparationCompleteThreshold.rawValue)
    }

    @Test("projection with no asset row claims nothing")
    func testProjectionWithoutAssetClaimsNothing() {
        let analysis = episodePreparationAnalysisInputs(
            asset: nil,
            coverage: coverage(adScanCoveredSec: AdScanSeconds(Self.episodeDuration))
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
            coverage: coverage(assetId: "some-other-asset", adScanCoveredSec: AdScanSeconds(Self.episodeDuration))
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

// MARK: - playhead-bcpj: the two facts `.idle` used to collapse

/// The resting glyph told a listener one thing where there were two, and the
/// owner acted on the ambiguity — re-tapping Download on episodes whose audio
/// was already on the device.
///
/// `playhead-fzrw` made that collapse long-lived rather than momentary: the
/// `analysis_assets` row is minted at DOWNLOAD time now, so a downloaded
/// episode legitimately rests at ✦ for as long as the serial lane takes to
/// reach it — measured at up to 13,678 s on the 2026-08-10 device pull. Before
/// fzrw the row simply did not exist during that window, so this is a
/// pre-existing collapse that fzrw made visible, not a regression.
@Suite("EpisodePreparationReadiness — the downloaded-and-waiting fact (playhead-bcpj)")
struct EpisodePreparationIdleDownloadedTests {

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

    // MARK: The distinction itself

    @Test("a resting episode with audio on disk reports isDownloaded")
    func testRestingDownloadedCarriesTheFact() {
        let readiness = deriveEpisodePreparationReadiness(inputs(isDownloaded: true))
        #expect(readiness.state == .idle)
        #expect(readiness.isDownloaded)
    }

    @Test("a resting episode with no audio does not")
    func testRestingNotDownloadedCarriesTheFact() {
        let readiness = deriveEpisodePreparationReadiness(inputs(isDownloaded: false))
        #expect(readiness.state == .idle)
        #expect(!readiness.isDownloaded)
    }

    /// THE POINT OF THE BEAD. Both are `.idle`, so a caller switching on
    /// `state` alone cannot tell them apart — which is exactly what the control
    /// did. If this ever passes with the two readinesses equal, the glyph and
    /// the VoiceOver value have silently collapsed back into one.
    @Test("the two resting facts are distinguishable from the readiness alone")
    func testTheTwoRestingStatesAreNotEqual() {
        let onDisk = deriveEpisodePreparationReadiness(inputs(isDownloaded: true))
        let absent = deriveEpisodePreparationReadiness(inputs(isDownloaded: false))
        #expect(onDisk.state == absent.state)
        #expect(onDisk != absent)
    }

    // MARK: Every path that returns `.idle`

    /// A terminal analysis FAILURE on downloaded audio rests too, and the audio
    /// is still there — the tap is a retry, not a fetch.
    @Test("a terminal analysis failure rests as downloaded")
    func testTerminalFailureOnDownloadedAudio() {
        let readiness = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisFailed: true, userInitiated: true)
        )
        #expect(readiness.state == .idle)
        #expect(readiness.isDownloaded)
    }

    /// The working-but-nothing-in-flight fallback: the user asked, nothing is
    /// blocking, no transfer has started. There is no audio yet, so the arrow
    /// is right and a ✦ would promise the file is already here.
    @Test("the working fallback with no audio rests as not-downloaded")
    func testWorkingFallbackWithoutAudio() {
        let readiness = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: false, userInitiated: true, downloadPermitted: true)
        )
        #expect(readiness.state == .idle)
        #expect(!readiness.isDownloaded)
    }

    /// The default is `false`, so a construction that does not think about the
    /// question UNDER-claims — same discipline as `analysisFractionIsMeasured`.
    /// Under-claiming shows an arrow for an episode that is present, which
    /// costs a redundant tap; over-claiming hides a download that is missing.
    @Test("a hand-built readiness under-claims rather than over-claims")
    func testDefaultUnderClaims() {
        let bare = EpisodePreparationReadiness(
            state: .idle, downloadFraction: 1, analysisFraction: 0
        )
        #expect(!bare.isDownloaded)
    }

    // MARK: What VoiceOver says

    @Test("VoiceOver names a fetch only when there is something to fetch")
    func testAccessibilityLabelSplits() {
        let onDisk = deriveEpisodePreparationReadiness(inputs(isDownloaded: true))
        let absent = deriveEpisodePreparationReadiness(inputs(isDownloaded: false))
        #expect(episodePreparationAccessibilityLabel(onDisk) == "Prepare now")
        #expect(episodePreparationAccessibilityLabel(absent) == "Download and prepare")
    }

    @Test("VoiceOver says the episode is waiting its turn, not that it is missing")
    func testAccessibilityValueSplits() {
        let onDisk = deriveEpisodePreparationReadiness(inputs(isDownloaded: true))
        let absent = deriveEpisodePreparationReadiness(inputs(isDownloaded: false))
        #expect(episodePreparationAccessibilityValue(onDisk) == "Downloaded, waiting its turn")
        #expect(episodePreparationAccessibilityValue(absent) == "Not downloaded")
    }

    /// The label carried the whole burden before, and for a downloaded episode
    /// it was WRONG rather than merely thin: it named a download that will not
    /// happen. A glyph split alone would have left VoiceOver users with it.
    @Test("no resting label promises a download for audio already on disk")
    func testDownloadedLabelDoesNotPromiseAFetch() {
        let onDisk = deriveEpisodePreparationReadiness(inputs(isDownloaded: true))
        let label = episodePreparationAccessibilityLabel(onDisk)
        #expect(!label.lowercased().contains("download"))
    }

    // MARK: What must NOT change

    /// The other resting and working states are untouched. `.ready` in
    /// particular must keep saying nothing here: it has no caption, and a
    /// download fact spoken over the ✓ would be noise.
    @Test("the non-idle states keep their existing strings")
    func testOtherStatesUnchanged() {
        let ready = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisComplete: true)
        )
        #expect(ready.state == .ready)
        #expect(episodePreparationAccessibilityLabel(ready) == "Analysis ready")
        #expect(episodePreparationAccessibilityValue(ready) == "")

        let analyzing = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, analysisActive: true, adScanFraction: ReachRatio(0.3))
        )
        #expect(analyzing.state == .analyzing)
        #expect(episodePreparationAccessibilityLabel(analyzing) == "Analyzing")
        #expect(episodePreparationAccessibilityValue(analyzing) == "Downloaded · analyzing 30%")
    }

    /// `.idle` stays ACTIONABLE in both spellings — the tap is playhead-kanf's
    /// promote-to-now escape hatch, and the downloaded case is where it is most
    /// useful. Nothing here may turn the resting glyph into an inert one.
    @Test("both resting spellings keep their progress fractions intact")
    func testFractionsSurvive() {
        let onDisk = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: true, adScanFraction: ReachRatio(0.25))
        )
        #expect(onDisk.downloadFraction == 1)
        #expect(onDisk.analysisFraction == 0.25)

        let partial = deriveEpisodePreparationReadiness(
            inputs(isDownloaded: false, downloadFraction: 0.4)
        )
        #expect(partial.downloadFraction == 0.4)
        #expect(!partial.isDownloaded)
    }
}
