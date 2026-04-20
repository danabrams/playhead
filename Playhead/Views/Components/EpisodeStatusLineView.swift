// EpisodeStatusLineView.swift
// Single-line status above an episode's body. Pulls copy from
// `EpisodeStatusLineCopy.resolve(...)` — never hand-builds strings —
// so the view contains no display logic beyond styling.
//
// Scope: playhead-zp5y (Phase 2 deliverable 3 — "Episode detail status
// line sourced from EpisodeSurfaceStatus"). The view is a reusable
// component: any surface that wants to render the canonical status
// line composes `EpisodeStatusLineView(status:coverage:anchor:)`.
//
// Design intent (UI design §B):
//   * Single primary line, small mono/serif text per typographic scale.
//   * Optional secondary line ("analyzing remainder") below the primary
//     when a backfill is active.
//   * No CTAs in-component — the resolver's string already includes the
//     "Retry" word when applicable; hooking that up to an action belongs
//     to the caller (the owning episode surface).
//
// This view is intentionally @Observable-friendly without owning any
// state itself: it consumes a plain `EpisodeSurfaceStatus` the caller
// re-derives as its reducer inputs change. SwiftUI re-renders the line
// whenever the containing view's state flips, so there is no separate
// "refresh" path to wire.

import SwiftUI

// MARK: - EpisodeStatusLineView

/// Single-line status surfaced above the episode body. Consumes the
/// reducer's output and re-renders whenever the caller's `status`
/// input flips.
struct EpisodeStatusLineView: View {

    /// The reducer's output. Drives the primary / secondary string.
    let status: EpisodeSurfaceStatus

    /// The coverage summary the reducer consumed. Forwarded to the
    /// copy resolver for the "first X min" vs "next X min" branch.
    let coverage: CoverageSummary?

    /// The readiness anchor the reducer consumed. Forwarded to the
    /// copy resolver for the proximal minutes computation.
    let anchor: TimeInterval?

    /// `true` when a backfill is actively widening the covered region.
    /// Drives the optional "analyzing remainder" secondary line.
    let backfillActive: Bool

    init(
        status: EpisodeSurfaceStatus,
        coverage: CoverageSummary? = nil,
        anchor: TimeInterval? = nil,
        backfillActive: Bool = false
    ) {
        self.status = status
        self.coverage = coverage
        self.anchor = anchor
        self.backfillActive = backfillActive
    }

    var body: some View {
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: coverage,
            anchor: anchor,
            backfillActive: backfillActive
        )
        VStack(alignment: .leading, spacing: 2) {
            Text(line.primary)
                .font(AppTypography.mono(size: 12, weight: .regular))
                .foregroundStyle(AppColors.textSecondary)
                .accessibilityIdentifier("EpisodeStatusLineView.primary")
            if let secondary = line.secondary {
                Text(secondary)
                    .font(AppTypography.mono(size: 11, weight: .regular))
                    .foregroundStyle(AppColors.textTertiary)
                    .accessibilityIdentifier("EpisodeStatusLineView.secondary")
            }
        }
        .accessibilityElement(children: .combine)
        .accessibilityLabel(Text(line.accessibilitySummary))
    }
}

// MARK: - Accessibility

private extension EpisodeStatusLine {
    /// Compact accessibility summary joining primary + secondary with a
    /// comma so VoiceOver reads them as one logical element.
    var accessibilitySummary: String {
        guard let secondary else { return primary }
        return "\(primary), \(secondary)"
    }
}
