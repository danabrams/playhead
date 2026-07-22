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
/// Resting states are `.idle` (✦ — not prepared; tap to prepare) and
/// `.ready` (✓ — fully analyzed; ad-skips prepared). The three working
/// states drive the segmented readiness bar. `.waitingForWifi` is the
/// cellular-gated variant: the user has expressed intent but a download
/// is required and the current network + `cellularPolicy` forbid it.
enum EpisodePreparationControlState: String, Equatable, Sendable, CaseIterable {
    /// ✦ Nothing prepared yet (not downloaded / not analyzed). Tap to prepare.
    case idle
    /// Download needed but blocked by the cellular policy on a metered link.
    case waitingForWifi
    /// Audio is downloading — the download zone of the bar fills.
    case downloading
    /// Audio is present; analysis is running — the analyze zone fills.
    case analyzing
    /// ✓ Fully analyzed; skip-cues prepared.
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
    /// Analysis has covered the whole episode — the authoritative `.ready`
    /// signal, computed by the caller from coverage-vs-duration (and/or a
    /// terminal `done` analysis state). Note: the partial-completion
    /// `SessionState` terminals (`completeFeatureOnly` /
    /// `completeTranscriptPartial`) project to `PersistedStatus.done`, so
    /// they arrive here as `analysisComplete == true` (calm ✓) — the
    /// control does not distinguish degraded-full from full.
    var analysisComplete: Bool
    /// Analysis reached a terminal FAILURE / cancellation (all
    /// `failed*` / `cancelledBudget` `SessionState`s project to
    /// `PersistedStatus.failed`). A terminal failure must resolve to a
    /// resting, actionable glyph — never a perpetual "analyzing" spinner —
    /// so the user can tap to retry.
    var analysisFailed: Bool
    /// Coverage watermark / duration in `[0, 1]`, or `nil` when coverage
    /// or duration is unknown (legacy rows, 0-duration episodes).
    var analysisFraction: Double?
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
///   1. `.ready` — analysis is complete. Supersedes everything, including
///      the cellular gate (a fully-analyzed episode is ready regardless of
///      network).
///   2. Resting `.idle` — no intent and nothing active. The ✦ glyph.
///   3. Working (intent OR an in-flight download OR active analysis):
///      * not downloaded, download in flight → `.downloading`
///      * not downloaded, download blocked by cellular → `.waitingForWifi`
///      * not downloaded, download permitted → `.downloading` (about to start)
///      * downloaded → `.analyzing`
///
/// The download zone always fills before the analyze zone: `.downloading`
/// reports the live download fraction with analysis pinned at 0, and
/// `.analyzing` reports the download zone full (1) with the live analysis
/// fraction. Both output fractions are clamped to `[0, 1]`, so a jittery
/// input can never push a bar past full or below empty.
func deriveEpisodePreparationReadiness(
    _ inputs: EpisodePreparationInputs
) -> EpisodePreparationReadiness {
    let download = clampUnit(inputs.downloadFraction)
    let analysis = clampUnit(inputs.analysisFraction)

    // 1. Fully analyzed — the calm ✓. Highest precedence.
    if inputs.analysisComplete {
        return EpisodePreparationReadiness(
            state: .ready, downloadFraction: 1, analysisFraction: 1
        )
    }

    // 2. No intent and nothing running → resting ✦. Surface any known
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

    // 3. Working. The download zone fills first.
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
    case .idle, .ready:
        return nil
    case .waitingForWifi:
        return "Waiting for Wi‑Fi"
    case .downloading:
        return "Downloading \(episodePreparationPercent(readiness.downloadFraction))"
    case .analyzing:
        return "Downloaded · analyzing \(episodePreparationPercent(readiness.analysisFraction))"
    }
}

/// Format a `[0, 1]` fraction as an integer percent for the functional
/// progress caption. Never a vanity metric — pure progress only.
func episodePreparationPercent(_ fraction: Double) -> String {
    let clamped = min(1, max(0, fraction))
    return "\(Int((clamped * 100).rounded()))%"
}

// MARK: - Analysis-state mapping (pure)

/// Fraction at or above which analysis coverage counts as "the whole
/// episode is analyzed". Slightly below 1 so the last sub-second of
/// coverage rounding does not strand the control at "analyzing 99%".
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

/// Whether analysis is complete: the pipeline reported a terminal `.done`
/// status, OR coverage has reached the end of the episode
/// (`analysisFraction >= episodePreparationCompleteThreshold`). A terminal
/// `.done` wins even if the fraction is unknown (0-duration / legacy rows).
/// A `.failed` / `.cancelled` status is NOT complete (the control falls
/// back to an actionable resting state so the user can retry).
func episodePreparationAnalysisComplete(
    status: AnalysisState.PersistedStatus?,
    analysisFraction: Double?
) -> Bool {
    if status == .done { return true }
    // A failed / cancelled job never reads as ready, even if coverage
    // happened to reach the end — the control returns to an actionable
    // resting glyph so the user can retry.
    if status == .failed || status == .cancelled { return false }
    guard let analysisFraction, analysisFraction.isFinite else { return false }
    return analysisFraction >= episodePreparationCompleteThreshold
}

// MARK: - Private

private func clampUnit(_ value: Double?) -> Double {
    guard let value, value.isFinite else { return 0 }
    return min(1, max(0, value))
}
