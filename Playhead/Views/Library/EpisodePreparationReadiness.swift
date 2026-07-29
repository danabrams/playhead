// EpisodePreparationReadiness.swift
// playhead-3xtw: pure state-derivation for the per-episode
// "Download & Analyze on demand" control.
//
// This is Layer 2 of the feature — a PURE function (inputs → state + two
// fractions) with no SwiftUI, no live services, no I/O. The SwiftUI
// control (`EpisodePreparationControl`) gathers the raw inputs from the
// download manager / analysis store / reachability and renders whatever
// this function returns; the trigger service (`EpisodePreparationCoordinator`)
// performs the side effects. Keeping the decision here means every
// state + fraction combination is exhaustively unit-testable without
// standing up the SwiftUI @Query / @Environment machinery or a live
// scheduler — mirroring the file-scope `libraryRowShouldShowReadinessCheckmark`
// pattern already used by this view.

import Foundation

// MARK: - Control state

/// Resting/working state of the per-episode prepare control.
///
/// Resting states are `.idle` (✦ — not prepared; tap to prepare),
/// `.partiallyAnalyzed` (◐ — the pipeline stopped without covering the
/// episode) and `.ready` (✓ — fully ad-scanned; ad-skips prepared). The
/// three working states drive the segmented readiness bar.
/// `.waitingForWifi` is the cellular-gated variant: the user has expressed
/// intent but a download is required and the current network +
/// `cellularPolicy` forbid it.
enum EpisodePreparationControlState: String, Equatable, Sendable, CaseIterable {
    /// ✦ Nothing prepared yet (not downloaded / not analyzed). Tap to prepare.
    case idle
    /// Download needed but blocked by the cellular policy on a metered link.
    case waitingForWifi
    /// Audio is downloading — the download zone of the bar fills.
    case downloading
    /// Audio is present; analysis is running — the analyze zone fills.
    case analyzing
    /// playhead-pz32: ◐ the pipeline reached a terminal completion but the
    /// episode is NOT fully ad-scanned — either the terminal itself is a
    /// degraded one (`completeFeatureOnly` / `completeTranscriptPartial`) or
    /// measured ad-scan coverage falls short of
    /// ``episodePreparationCompleteThreshold``. Distinct from `.ready`
    /// because it is the honest answer, and distinct from `.analyzing`
    /// because nothing is running — a spinner here would be a lie in the
    /// other direction.
    case partiallyAnalyzed
    /// ✓ The episode's audio has been read for ads end to end.
    ///
    /// Scoped precisely, because the old glyph promised more than it knew: this
    /// asserts that the COVERAGE-LANE screening pass
    /// (``SemanticScanCoverage/coverageScanPass``) examined essentially all of
    /// the audio. It does not assert that boundary refinement (`passB`) has
    /// finished, nor that every resulting skip cue is final — those narrow
    /// already-screened windows and do not change whether the audio was read.
    case ready
}

// MARK: - Inputs

/// Raw, already-gathered inputs the pure derivation consumes. Every field
/// is a plain value the SwiftUI layer resolves from the download manager,
/// the analysis store, and the reachability/cellular context. Fractions
/// are optional so "unknown" (missing coverage, size-unknown transfer,
/// 0-duration episode) is distinct from a genuine `0`.
struct EpisodePreparationInputs: Equatable, Sendable {
    /// The full audio file is cached on disk.
    var isDownloaded: Bool
    /// A download for this episode is actively in flight (foreground or
    /// background). Distinguishes "download running" from "download will
    /// start once permitted".
    var downloadInFlight: Bool
    /// Live byte fraction of the in-flight download in `[0, 1]`, or `nil`
    /// when the transfer size is unknown / not yet observed.
    var downloadFraction: Double?
    /// An analysis job for this episode is queued or running — whether it
    /// was triggered automatically (auto-pipeline) or by the user. Drives
    /// the "auto-analyzing shows the working bar without a tap" behaviour.
    var analysisActive: Bool
    /// The episode is HONESTLY fully analyzed — the authoritative `.ready`
    /// signal, computed by the caller via
    /// ``episodePreparationAnalysisComplete(status:adScanFraction:isDegradedTerminal:)``
    /// from MEASURED semantic ad-scan coverage. playhead-pz32: a terminal
    /// `PersistedStatus.done` no longer implies this on its own — the four
    /// `SessionState` completion terminals all project to `.done`, two of
    /// them explicitly degraded, and none of them proves the audio was read
    /// for ads.
    var analysisComplete: Bool
    /// playhead-pz32: the pipeline reached a terminal COMPLETION for this
    /// episode (any of `.complete`, `.completeFull`, `.completeFeatureOnly`,
    /// `.completeTranscriptPartial`) — it will do no further work on its own.
    /// Combined with `analysisComplete == false` this is what distinguishes
    /// "stopped short" (◐ `.partiallyAnalyzed`) from "still working"
    /// (`.analyzing`).
    var analysisTerminatedComplete: Bool
    /// Analysis reached a terminal FAILURE / cancellation (all
    /// `failed*` / `cancelledBudget` `SessionState`s project to
    /// `PersistedStatus.failed`). A terminal failure must resolve to a
    /// resting, actionable glyph — never a perpetual "analyzing" spinner —
    /// so the user can tap to retry.
    var analysisFailed: Bool
    /// playhead-pz32: fraction of the episode's audio that a semantic ad
    /// scan has actually EXAMINED, in `[0, 1]`, or `nil` when the scan
    /// extent or a positive duration is unknown. Sourced from
    /// ``AnalysisCoverageSummary/adScanFraction``.
    ///
    /// This replaced `max(featureCoverageEndTime, confirmedAdCoverageEndTime)
    /// / episodeDurationSec`, which was the wrong quantity twice over: the
    /// first arm is the DSP feature watermark (it sweeps the whole episode
    /// regardless of the semantic scan) and the second is `max(endTime)` of
    /// detected ad windows (so one late detection lit the ✓ with almost
    /// nothing scanned — meaning AN EPISODE WHERE DETECTION DID WORSE COULD
    /// LOOK MORE COMPLETE). Neither is a measure of audio read for ads.
    var adScanFraction: Double?
    /// The user tapped the control this session (explicit intent). Makes
    /// the control show the working bar immediately, before the first
    /// progress tick arrives.
    var userInitiated: Bool
    /// Whether a download may proceed right now given the current network
    /// and `cellularPolicy` (see `episodePreparationDownloadPermitted`).
    var downloadPermitted: Bool
}

// MARK: - Output

/// Derived control state plus the two bar fractions, both already clamped
/// to `[0, 1]`. The download zone renders `downloadFraction`; the analyze
/// zone renders `analysisFraction`.
struct EpisodePreparationReadiness: Equatable, Sendable {
    var state: EpisodePreparationControlState
    /// Download-zone fill, `[0, 1]`.
    var downloadFraction: Double
    /// Analyze-zone fill, `[0, 1]`.
    var analysisFraction: Double
    /// playhead-pz32: whether ``analysisFraction`` is a real MEASUREMENT or the
    /// zero that stands in for an absent one. Both draw an empty analyze zone,
    /// but only a measured value may be spoken as a percentage — announcing
    /// "0% scanned for ads" for an episode whose scan rows were merely deleted
    /// invents a number. Defaults to `false` so a caller that does not think
    /// about it under-claims.
    var analysisFractionIsMeasured: Bool = false
}

// MARK: - Cellular gate (pure)

/// Whether a NEW download may proceed given the network reachability and
/// the user's `cellularPolicy`. Pure so the cellular decision is tested
/// without `NWPathMonitor`.
///
/// Rules (per the settled design — reuse the existing `cellularPolicy`
/// setting, no new setting):
///   * Wi‑Fi / ethernet → always permitted.
///   * Unreachable → never permitted (there is no link to download over).
///   * Cellular → permitted only when the policy is `.on`. Both `.off`
///     and `.askEachTime` are treated as "do not auto-proceed" — matching
///     `CellularPolicy`'s documented runtime default ("still defaults to
///     Off until the user answers"). We deliberately do NOT introduce an
///     in-row prompt for `.askEachTime`; that would be a new surface.
func episodePreparationDownloadPermitted(
    reachability: TransportSnapshot.Reachability,
    policy: CellularPolicy
) -> Bool {
    switch reachability {
    case .wifi:
        return true
    case .unreachable:
        return false
    case .cellular:
        return policy == .on
    }
}

// MARK: - Derivation

/// Pure state machine for the prepare control. Precedence (highest first):
///
///   1. `.ready` — the episode is honestly, measurably fully ad-scanned.
///      Supersedes everything, including the cellular gate (a fully-analyzed
///      episode is ready regardless of network).
///   2. `.partiallyAnalyzed` — the pipeline reached a completion terminal with
///      nothing running and no transfer in flight, but coverage falls short, is
///      unmeasured, or the terminal is a degraded one. playhead-pz32: this case
///      used to fold into `.ready`.
///   3. Resting `.idle` — no intent and nothing active. The ✦ glyph.
///   4. Working (intent OR an in-flight download OR active analysis):
///      * not downloaded, download in flight → `.downloading`
///      * not downloaded, download blocked by cellular → `.waitingForWifi`
///      * not downloaded, download permitted → `.downloading` (about to start)
///      * downloaded → `.analyzing`
///
/// The download zone always fills before the analyze zone: `.downloading`
/// reports the live download fraction with analysis pinned at 0, and
/// `.analyzing` reports the download zone full (1) with the live ad-scan
/// fraction. Both output fractions are clamped to `[0, 1]`, so a jittery
/// input can never push a bar past full or below empty.
func deriveEpisodePreparationReadiness(
    _ inputs: EpisodePreparationInputs
) -> EpisodePreparationReadiness {
    let download = clampUnit(inputs.downloadFraction)
    let analysis = clampUnit(inputs.adScanFraction)

    // 1. Honestly fully ad-scanned — the calm ✓. Highest precedence.
    if inputs.analysisComplete {
        return EpisodePreparationReadiness(
            state: .ready, downloadFraction: 1, analysisFraction: 1
        )
    }

    // 2. playhead-pz32: the pipeline finished but the episode is NOT fully
    //    ad-scanned. Two shapes land here and both used to render the same
    //    calm ✓ as a genuine full analysis:
    //      * a DEGRADED terminal (`completeFeatureOnly` /
    //        `completeTranscriptPartial`), where the transcript never
    //        advanced far enough for the scan to read the audio;
    //      * a nominally-full terminal whose MEASURED ad-scan coverage is
    //        short.
    //    It must not read as `.analyzing` either: nothing is running, so a
    //    working bar would promise progress that will never arrive. A
    //    completion terminal is about analysis, not about the audio cache, so
    //    this outranks the download branches below — EXCEPT while a transfer is
    //    genuinely in flight, where the live download is the more useful truth
    //    and a frozen ◐ would hide it.
    if inputs.analysisTerminatedComplete, !inputs.analysisActive, !inputs.downloadInFlight {
        // ◐ WITHOUT a percentage when there is no measurement to report. That is
        // the state after a cohort bump: `semantic_scan_results` rows are deleted
        // whenever the prompt/schema/plan/normalization revs, the locale changes,
        // the OS updates or the app build changes
        // (`pruneOrphanedScansForCurrentCohort`), which also means the pipeline no
        // longer trusts those verdicts. "Partly analyzed, amount unknown" is the
        // whole truth.
        //
        // Deliberately NOT the actionable ✦ here, tempting as it is: the tap
        // would promise a re-scan the job state machine does not currently
        // deliver (`insertJob` is `INSERT OR IGNORE` on a `workKey` the completed
        // job already owns, and only `queued`/`paused`/retryable-`failed` rows
        // dispatch), so it would swap an honest inert glyph for a false promise —
        // a different lie, not a fix. Re-drive is playhead-gqx4 / playhead-i7qe's.
        return EpisodePreparationReadiness(
            state: .partiallyAnalyzed,
            downloadFraction: inputs.isDownloaded ? 1 : download,
            analysisFraction: analysis,
            // `isFinite`, not `!= nil`: a NaN clamps to 0 and would otherwise be
            // spoken as the same fabricated "0%" as an absent measurement.
            analysisFractionIsMeasured: inputs.adScanFraction?.isFinite == true
        )
    }

    // 3. No intent and nothing running → resting ✦. Surface any known
    //    progress in the fractions so a partially-prepared-then-abandoned
    //    episode still reads sensibly if a caller chooses to draw them.
    let isWorking = inputs.userInitiated || inputs.downloadInFlight || inputs.analysisActive
    guard isWorking else {
        return EpisodePreparationReadiness(
            state: .idle,
            downloadFraction: inputs.isDownloaded ? 1 : download,
            analysisFraction: analysis
        )
    }

    // 4. Working. The download zone fills first.
    if !inputs.isDownloaded {
        // An actual transfer in flight always reads as downloading, even if
        // the network flipped to a now-forbidden link mid-transfer. (The
        // control folds its optimistic "just kicked a download" hint into
        // `downloadInFlight`, so a fresh tap shows the bar immediately.)
        if inputs.downloadInFlight {
            return EpisodePreparationReadiness(
                state: .downloading, downloadFraction: download, analysisFraction: 0
            )
        }
        // The user asked to prepare, but the cellular policy forbids the
        // download right now. Gate on `userInitiated` so an auto-queued
        // (not user-tapped) episode never claims to be "waiting for Wi‑Fi".
        if inputs.userInitiated, !inputs.downloadPermitted {
            return EpisodePreparationReadiness(
                state: .waitingForWifi, downloadFraction: download, analysisFraction: 0
            )
        }
        // Working, but no transfer is in flight and nothing is blocking it
        // (e.g. a Wi‑Fi-permitted state where a prior cellular block just
        // cleared, or an analysis job queued before the audio exists).
        // Fall back to the actionable idle glyph rather than stranding on a
        // 0% bar that can never advance — a tap re-drives the download.
        return EpisodePreparationReadiness(
            state: .idle, downloadFraction: download, analysisFraction: 0
        )
    }

    // Downloaded, not complete. A terminal analysis FAILURE (with nothing
    // re-running) resolves to the resting glyph — the user can tap to
    // retry — never a perpetual "analyzing" spinner. This is handled here
    // in the pure layer (not via view bookkeeping) so it is exhaustively
    // testable and cannot regress into a stuck state.
    if inputs.analysisFailed, !inputs.analysisActive {
        return EpisodePreparationReadiness(
            state: .idle, downloadFraction: 1, analysisFraction: analysis
        )
    }

    // Downloaded, not complete, working (and not a terminal failure) →
    // analyzing. Download zone full.
    return EpisodePreparationReadiness(
        state: .analyzing, downloadFraction: 1, analysisFraction: analysis
    )
}

// MARK: - Caption + percent (pure)

/// The small caption under the working bar (e.g. "Downloaded · analyzing
/// 30%"). `nil` for the resting states, which render a glyph only.
func episodePreparationCaption(_ readiness: EpisodePreparationReadiness) -> String? {
    switch readiness.state {
    case .idle, .ready, .partiallyAnalyzed:
        return nil
    case .waitingForWifi:
        return "Waiting for Wi‑Fi"
    case .downloading:
        return "Downloading \(episodePreparationPercent(readiness.downloadFraction))"
    case .analyzing:
        return "Downloaded · analyzing \(episodePreparationPercent(readiness.analysisFraction))"
    }
}

// MARK: - Accessibility (pure)

/// playhead-pz32: the control's VoiceOver label per state. Pure so the
/// "analyzed vs. partly analyzed" distinction is unit-testable without
/// standing up SwiftUI — a screen-reader user must be able to tell a fully
/// ad-scanned episode from one the pipeline stopped short on, and that is
/// exactly the kind of guarantee that silently rots when it lives inline in
/// a view body.
func episodePreparationAccessibilityLabel(
    _ readiness: EpisodePreparationReadiness
) -> String {
    switch readiness.state {
    case .idle:              return "Download and analyze"
    case .waitingForWifi:    return "Waiting for Wi‑Fi to download"
    case .downloading:       return "Downloading"
    case .analyzing:         return "Analyzing"
    case .partiallyAnalyzed: return "Partly analyzed"
    case .ready:             return "Analysis ready"
    }
}

/// playhead-pz32: the control's VoiceOver value per state. Working states read
/// out the progress caption. `.partiallyAnalyzed` reads out how much audio was
/// actually screened, because "partly" without a number is not actionable
/// information — UNLESS there is no measurement, in which case saying "0%
/// scanned" would invent one and the honest value is the amount's absence. The
/// two other resting states have no value.
func episodePreparationAccessibilityValue(
    _ readiness: EpisodePreparationReadiness
) -> String {
    switch readiness.state {
    case .partiallyAnalyzed:
        guard readiness.analysisFractionIsMeasured else { return "Amount scanned unknown" }
        return "\(episodePreparationPercent(readiness.analysisFraction)) scanned for ads"
    case .idle, .ready, .waitingForWifi, .downloading, .analyzing:
        return episodePreparationCaption(readiness) ?? ""
    }
}

/// Format a `[0, 1]` fraction as an integer percent for the functional
/// progress caption. Never a vanity metric — pure progress only.
func episodePreparationPercent(_ fraction: Double) -> String {
    let clamped = min(1, max(0, fraction))
    return "\(Int((clamped * 100).rounded()))%"
}

// MARK: - Analysis-state mapping (pure)

/// Fraction at or above which measured ad-scan coverage counts as "the whole
/// episode has been read for ads". Slightly below 1 so the last sub-second of
/// coverage rounding does not strand the control at "analyzing 99%".
///
/// playhead-pz32: this cutoff was never the defect and must not be lowered to
/// make the ✓ easier to reach. The defect was WHICH QUANTITY it was applied
/// to — see ``EpisodePreparationInputs/adScanFraction``.
///
/// It is, however, calibrated for a quantity that can actually reach 1: the
/// ad-scan area bridges sub-ad-width transcript gaps for exactly that reason
/// (``AnalysisCoverageMath/adScanBridgeableGapSec``). Applying 0.98 to the RAW
/// chunk union instead would put the ✓ permanently out of reach, because a
/// transcript chunk spans first-word to last-word and its union tops out around
/// 0.93–0.98 of real audio. If this threshold is ever revisited, revisit the
/// bridging in the same breath — the pair has to be calibrated together.
let episodePreparationCompleteThreshold: Double = 0.98

/// Whether the (canonical, projected) analysis status indicates a job is
/// queued or actively running. Drives the "auto-analyzing shows the
/// working bar without a tap" behaviour. Consumes the projected
/// `AnalysisState.PersistedStatus` (from
/// `EpisodeSurfaceStatusObserver.analysisState(from:)`), NOT the raw
/// `analysis_assets.analysisState` column — the raw column holds
/// `SessionState` values (`spooling`, `backfill`, …) that the observer
/// folds into `.running`. `nil` (no asset row yet) is not active.
func episodePreparationAnalysisActive(status: AnalysisState.PersistedStatus?) -> Bool {
    switch status {
    case .queued, .running:
        return true
    case .new, .done, .failed, .cancelled, nil:
        return false
    }
}

/// playhead-pz32: whether the episode can HONESTLY be reported as analyzed.
///
/// MEASURED semantic ad-scan coverage is the only thing that satisfies this.
/// Three things that used to satisfy it, and why none of them may:
///
///   * **A terminal `.done` status.** All four `SessionState` completion
///     terminals project to `.done`, and two of them
///     (`completeFeatureOnly` / `completeTranscriptPartial`) are explicitly
///     degraded — feature-only means the transcript never advanced past
///     preview, so the semantic scan cannot have read the audio. `.done`
///     said "the pipeline stopped", never "the audio was screened".
///   * **The DSP feature watermark.** Feature extraction sweeps the whole
///     episode independently of the semantic scan, so it hits 100% while
///     most audio has never been read for ads. This is the arm that lit the
///     ✓ on a 47%-scanned episode.
///   * **`max(endTime)` of detected ad windows.** Not coverage at all: one
///     late-placed detection pushes it to the end of the episode, so AN
///     EPISODE WHERE DETECTION DID WORSE CAN LOOK MORE COMPLETE.
///
/// A `.failed` / `.cancelled` status is NOT complete, and a degraded terminal
/// is NOT complete, regardless of coverage — the control falls back to an
/// actionable resting state (or ◐) instead. An unknown / non-finite
/// `adScanFraction` is NOT complete: where the quantity is unmeasured, render
/// not-ready rather than ready.
func episodePreparationAnalysisComplete(
    status: AnalysisState.PersistedStatus?,
    adScanFraction: Double?,
    isDegradedTerminal: Bool
) -> Bool {
    // A failed / cancelled job never reads as ready, even at full coverage —
    // the control returns to an actionable resting glyph so the user can retry.
    if status == .failed || status == .cancelled { return false }
    // A degraded terminal never reads as calm success. Belt and braces: the
    // coverage arm below would normally already fall short (a transcript that
    // never advanced cannot have been scanned), but the terminal is a direct
    // statement from the pipeline that it stopped short, and it must not be
    // possible for a coverage-measurement bug to override it.
    if isDegradedTerminal { return false }
    guard let adScanFraction, adScanFraction.isFinite else { return false }
    return adScanFraction >= episodePreparationCompleteThreshold
}

/// playhead-pz32: whether the persisted analysis state is one of the four
/// completion terminals — the pipeline will do no more work for this episode
/// on its own. Feeds
/// ``EpisodePreparationInputs/analysisTerminatedComplete``.
///
/// Reads the RAW `analysis_assets.analysisState` column rather than the
/// projected `PersistedStatus`, because the projection is exactly what erases
/// the distinction this bead is about: all four completion terminals collapse
/// into `.done`. An unrecognised string (forward-compat) is not a terminal.
func episodePreparationTerminalCompletion(analysisState: String) -> SessionState? {
    guard let sessionState = SessionState(rawValue: analysisState),
          sessionState.isTerminalCompletion else {
        return nil
    }
    return sessionState
}

// MARK: - Persisted-artifact projection (pure)

/// playhead-pz32: the ANALYSIS half of the control's inputs.
///
/// Extracted so the one decision this bead is about — WHICH persisted quantity
/// the ✓ resolves from — lives in a pure function instead of inside an
/// `@Observable` `@MainActor` model that needs a live `PlayheadRuntime` to
/// exercise. Before the extraction, swapping `adScanFraction` back to the DSP
/// feature watermark (or to a constant `1.0`) was a one-line mutation that the
/// entire test suite let through.
struct EpisodePreparationAnalysisInputs: Equatable, Sendable {
    var analysisActive: Bool = false
    var analysisComplete: Bool = false
    var analysisTerminatedComplete: Bool = false
    var analysisFailed: Bool = false
    var adScanFraction: Double?
}

/// playhead-pz32: project the two persisted artifacts an episode row consults —
/// its `analysis_assets` row and its ``AnalysisCoverageSummary`` — into the
/// control's analysis inputs.
///
/// - Parameters:
///   - asset: the latest `analysis_assets` row, or `nil` when the episode has
///     never been queued for analysis (everything false, coverage unknown).
///   - coverage: the coverage summary for THAT asset. A summary whose `assetId`
///     does not match is ignored rather than trusted — a mis-joined batch read
///     must under-claim, never lend one episode's coverage to another.
func episodePreparationAnalysisInputs(
    asset: AnalysisAsset?,
    coverage: AnalysisCoverageSummary?
) -> EpisodePreparationAnalysisInputs {
    guard let asset else { return EpisodePreparationAnalysisInputs() }
    let status = EpisodeSurfaceStatusObserver.analysisState(from: asset).persistedStatus
    // The RAW column, not the projected status: `PersistedStatus` folds all four
    // completion terminals into `.done` and so cannot tell a degraded terminal
    // from a full one.
    let terminal = episodePreparationTerminalCompletion(analysisState: asset.analysisState)
    let adScanFraction = coverage?.assetId == asset.id ? coverage?.adScanFraction : nil
    return EpisodePreparationAnalysisInputs(
        analysisActive: episodePreparationAnalysisActive(status: status),
        analysisComplete: episodePreparationAnalysisComplete(
            status: status,
            adScanFraction: adScanFraction,
            isDegradedTerminal: terminal?.isDegradedTerminalCompletion ?? false
        ),
        analysisTerminatedComplete: terminal != nil,
        analysisFailed: status == .failed || status == .cancelled,
        adScanFraction: adScanFraction
    )
}

// MARK: - Private

private func clampUnit(_ value: Double?) -> Double {
    guard let value, value.isFinite else { return 0 }
    return min(1, max(0, value))
}
