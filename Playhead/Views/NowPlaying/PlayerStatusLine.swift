// PlayerStatusLine.swift
// Slim one-line status row surfaced below the player's scrubber.
//
// Scope: playhead-3bv.4 (UI design §C-2 — "One-line player status below
// the scrubber, mirroring the episode-detail status line. Hidden when
// fully analyzed. Tap → Activity scoped to this episode.").
//
// Design intent:
//   * Mirrors the same `EpisodeSurfaceStatus` reducer path the Library
//     row uses (see `libraryRowStatusLineInputs` in
//     `EpisodeListView.swift`). The reducer is the single source of
//     truth — this view never hand-builds status strings.
//   * Hidden entirely when `playbackReadiness == .complete`. Per the
//     UI design doc, a fully-analyzed episode shows nothing on the
//     player surface — the typographic timeline already carries the
//     "ready" signal.
//   * No spinner. The player explicitly avoids progress indicators
//     during background analysis (§C honesty constraint — a spinner
//     reads as "the app is struggling").
//   * No confidence percentages, no per-segment numbers. Copy comes
//     verbatim from the existing `EpisodeStatusLineCopy.resolve(...)`.
//
// Tap behavior is owned by the caller — the row exposes a button shape
// so the host view (`NowPlayingView`) can route into Activity with
// `focusedEpisodeId` set to this episode.

import SwiftUI

// MARK: - PlayerStatusLineInputs

/// Inputs the player row needs in order to mount `EpisodeStatusLineView`
/// for the currently-playing episode. Mirrors `LibraryRowStatusLineInputs`
/// but does NOT short-circuit `.none` — the player surface is actively
/// engaged with this episode, and "Queued · waiting" is a meaningful
/// signal to render while the user listens.
///
/// `nil` is returned only when the episode is fully analyzed
/// (`playbackReadiness == .complete`), per the UI design contract.
struct PlayerStatusLineInputs: Equatable {
    let status: EpisodeSurfaceStatus
    let coverage: CoverageSummary?
    let anchor: TimeInterval?
}

/// Compute the optional status-line inputs the player should surface for
/// the supplied episode. Returns `nil` when the episode is fully
/// analyzed (per UI design §C-2 — "Hidden when fully analyzed").
///
/// Routes through the canonical `episodeSurfaceStatus(...)` reducer with
/// conservative live-cause / eligibility defaults — the player view does
/// not have direct scheduler cause data in hand, so the reducer falls
/// through to the readiness-driven branch on which the design's example
/// copy ("Skip-ready · first 18 min," "Downloaded · queued for analysis")
/// is keyed.
///
/// Exposed at file scope (mirroring `libraryRowStatusLineInputs`) so
/// behavioral tests can exercise it without SwiftUI's environment.
func playerStatusLineInputs(episode: Episode) -> PlayerStatusLineInputs? {
    // Hide the row entirely once the episode is fully analyzed. Every
    // other readiness state surfaces a meaningful line.
    let readiness = derivePlaybackReadiness(
        coverage: episode.coverageSummary,
        anchor: episode.playbackAnchor
    )
    if readiness == .complete {
        return nil
    }

    let state = AnalysisState(
        persistedStatus: .queued,
        hasUserPreemptedJob: false,
        hasAppForceQuitFlag: false,
        pendingSinceEnqueuedAt: nil,
        hasAnyConfirmedAnalysis: episode.coverageSummary != nil
    )
    let eligibility = AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: true,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: Date()
    )
    let status = episodeSurfaceStatus(
        state: state,
        cause: nil,
        eligibility: eligibility,
        coverage: episode.coverageSummary,
        readinessAnchor: episode.playbackAnchor
    )
    return PlayerStatusLineInputs(
        status: status,
        coverage: episode.coverageSummary,
        anchor: episode.playbackAnchor
    )
}

// MARK: - PlayerStatusLineRow

/// Slim, tap-target-sized row surfaced below the player's scrubber. Wraps
/// the canonical `EpisodeStatusLineView` in a button so the host can
/// route into Activity scoped to this episode.
///
/// The row hides itself when `inputs == nil` (the episode is fully
/// analyzed) — the host view never needs to gate on readiness.
struct PlayerStatusLineRow: View {

    /// Resolved inputs from `playerStatusLineInputs(episode:)`. `nil`
    /// suppresses the entire row.
    let inputs: PlayerStatusLineInputs?

    /// Invoked when the user taps the row. The host (`NowPlayingView`)
    /// routes to Activity scoped to the currently-playing episode.
    let onTap: () -> Void

    var body: some View {
        if let inputs {
            Button(action: onTap) {
                HStack(spacing: 0) {
                    EpisodeStatusLineView(
                        status: inputs.status,
                        coverage: inputs.coverage,
                        anchor: inputs.anchor
                    )
                    Spacer(minLength: 0)
                }
                .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .frame(minHeight: 32)
            .accessibilityIdentifier("PlayerStatusLineRow")
            .accessibilityHint("Opens Activity for this episode")
        }
    }
}
