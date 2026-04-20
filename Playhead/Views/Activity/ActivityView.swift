// ActivityView.swift
// Four-section Activity screen: Now / Up Next / Paused / Recently Finished.
// Sibling tab to Library and Settings; reached via the third tab in the
// app's TabView.
//
// Scope: playhead-quh7 (Phase 2 deliverable 4 — Activity screen).
//
// # Section responsibilities (from the bead spec + design doc §E)
//
// - Now (live work): episode title + coarse progress phrase. No
//   SurfaceReason rendering. Empty state: "Nothing running — pull down
//   to refresh" (the only section with pull-to-refresh).
// - Up Next (queued, eligible): list with reorder affordance; no
//   SurfaceReason rendering. Empty state: "Nothing queued."
// - Paused: the user-facing home of `SurfaceReason` rendering. Each row
//   reuses `SurfaceReasonCopyTemplates.template(for:)` and
//   `EpisodeStatusLineCopy.hintCopy(_:)` so this view holds zero
//   user-visible copy strings of its own.
// - Recently Finished: success / couldnt_analyze / analysis_unavailable.
//   Sub-copy for the unavailable case comes from
//   `EpisodeStatusLineCopy.unavailableReasonCopy(_:)`.
//
// # Refresh discipline
//
// The view subscribes to `ActivityRefreshNotification` posts from the
// scheduler (didStart / didFinish / wake). On every post, the view asks
// its bound `ActivityRefreshSource` for a fresh batch of inputs and
// re-aggregates. There is no Timer-based polling.

import SwiftUI

// MARK: - ActivityView

/// Four-section Activity screen. Composed by `ContentView` as a tab
/// sibling to Library and Settings.
struct ActivityView: View {

    /// Aggregator that owns the current `ActivitySnapshot` payload. The
    /// view re-renders whenever the snapshot is replaced via
    /// `viewModel.refresh(from:)`.
    @State private var viewModel = ActivityViewModel()

    /// Closure that produces a fresh batch of inputs. Injected so
    /// production wires it to a `ActivitySnapshotProvider` actor and
    /// SwiftUI Previews can hand back static fixtures inline. Default
    /// returns an empty list — the empty-state previews and tests rely
    /// on that.
    let inputProvider: @MainActor () async -> [ActivityEpisodeInput]

    init(
        inputProvider: @escaping @MainActor () async -> [ActivityEpisodeInput] = { [] }
    ) {
        self.inputProvider = inputProvider
    }

    var body: some View {
        NavigationStack {
            ZStack {
                AppColors.background
                    .ignoresSafeArea()

                ScrollView {
                    VStack(alignment: .leading, spacing: Spacing.xl) {
                        nowSection
                        upNextSection
                        pausedSection
                        recentlyFinishedSection
                    }
                    .padding(.horizontal, Spacing.md)
                    .padding(.top, Spacing.sm)
                    .padding(.bottom, Spacing.xxl)
                }
                .refreshable {
                    await refresh()
                }
            }
            .navigationTitle("Activity")
            .navigationBarTitleDisplayMode(.large)
        }
        .task {
            // Initial load.
            await refresh()
        }
        .onReceive(
            NotificationCenter.default.publisher(
                for: ActivityRefreshNotification.name
            )
        ) { _ in
            Task { await refresh() }
        }
    }

    private func refresh() async {
        let inputs = await inputProvider()
        viewModel.refresh(from: inputs)
    }
}

// MARK: - Sections

private extension ActivityView {

    var nowSection: some View {
        ActivitySection(title: "Now") {
            if viewModel.snapshot.now.isEmpty {
                Text("Nothing running — pull down to refresh")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textSecondary)
                    .accessibilityIdentifier("ActivityView.now.empty")
            } else {
                ForEach(viewModel.snapshot.now) { row in
                    NowRowView(row: row)
                }
            }
        }
    }

    var upNextSection: some View {
        ActivitySection(title: "Up Next") {
            if viewModel.snapshot.upNext.isEmpty {
                Text("Nothing queued")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textSecondary)
                    .accessibilityIdentifier("ActivityView.upNext.empty")
            } else {
                ForEach(viewModel.snapshot.upNext) { row in
                    UpNextRowView(row: row)
                }
            }
        }
    }

    @ViewBuilder
    var pausedSection: some View {
        if !viewModel.snapshot.paused.isEmpty {
            ActivitySection(title: "Paused") {
                ForEach(viewModel.snapshot.paused) { row in
                    PausedRowView(row: row)
                }
            }
        }
    }

    @ViewBuilder
    var recentlyFinishedSection: some View {
        if !viewModel.snapshot.recentlyFinished.isEmpty {
            ActivitySection(title: "Recently Finished") {
                ForEach(viewModel.snapshot.recentlyFinished) { row in
                    RecentlyFinishedRowView(row: row)
                }
            }
        }
    }
}

/// Single section helper: renders a bold title followed by a vertical
/// stack of section content. Replaces a `+` operator workaround so the
/// view body composes via SwiftUI's standard `@ViewBuilder` semantics.
private struct ActivitySection<Content: View>: View {
    let title: String
    @ViewBuilder let content: () -> Content
    var body: some View {
        VStack(alignment: .leading, spacing: Spacing.sm) {
            Text(title)
                .font(AppTypography.sans(size: 18, weight: .semibold))
                .foregroundStyle(AppColors.textPrimary)
                .padding(.bottom, Spacing.xs)
            content()
        }
    }
}

// MARK: - Row views

private struct NowRowView: View {
    let row: ActivityNowRow
    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(row.title)
                .font(AppTypography.body)
                .foregroundStyle(AppColors.textPrimary)
                .lineLimit(2)
            Text(row.progressPhrase)
                .font(AppTypography.mono(size: 12, weight: .regular))
                .foregroundStyle(AppColors.textSecondary)
                .accessibilityIdentifier("ActivityView.now.progress")
        }
        .padding(.vertical, Spacing.xs)
        .accessibilityElement(children: .combine)
        .accessibilityLabel(Text("\(row.title), \(row.progressPhrase)"))
    }
}

private struct UpNextRowView: View {
    let row: ActivityUpNextRow
    var body: some View {
        Text(row.title)
            .font(AppTypography.body)
            .foregroundStyle(AppColors.textPrimary)
            .lineLimit(2)
            .padding(.vertical, Spacing.xs)
    }
}

/// Paused row — pulls user-visible copy from
/// `SurfaceReasonCopyTemplates.template(for:)` and
/// `EpisodeStatusLineCopy.hintCopy(_:)`. NEVER builds copy strings
/// inline.
private struct PausedRowView: View {
    let row: ActivityPausedRow
    var body: some View {
        let reasonCopy = SurfaceReasonCopyTemplates.template(for: row.reason)
        let hintCopy = EpisodeStatusLineCopy.hintCopy(row.hint)
        VStack(alignment: .leading, spacing: 2) {
            Text(row.title)
                .font(AppTypography.body)
                .foregroundStyle(AppColors.textPrimary)
                .lineLimit(2)
            Text("\(reasonCopy) · \(hintCopy)")
                .font(AppTypography.mono(size: 12, weight: .regular))
                .foregroundStyle(AppColors.textSecondary)
                .accessibilityIdentifier("ActivityView.paused.reason")
        }
        .padding(.vertical, Spacing.xs)
        .accessibilityElement(children: .combine)
        .accessibilityLabel(Text("Paused: \(row.title), \(reasonCopy), \(hintCopy)"))
    }
}

/// Recently Finished row — pulls user-visible copy from
/// `EpisodeStatusLineCopy.unavailableReasonCopy(_:)` for the
/// analysis-unavailable sub-copy. The `couldnt_analyze` and `success`
/// outcome labels are short enough to live in this file because they
/// are not part of the `SurfaceReasonCopyTemplates` table — Recently
/// Finished outcome glyphs (✓ / ✕ / ℹ) are a distinct surface from
/// SurfaceReason rendering.
private struct RecentlyFinishedRowView: View {
    let row: ActivityRecentlyFinishedRow
    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: Spacing.sm) {
            Text(glyph)
                .font(AppTypography.mono(size: 14, weight: .regular))
                .foregroundStyle(glyphColor)
                .frame(width: 18, alignment: .leading)
                .accessibilityHidden(true)
            VStack(alignment: .leading, spacing: 2) {
                Text(row.title)
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                    .lineLimit(2)
                if let sub = subcopy {
                    Text(sub)
                        .font(AppTypography.mono(size: 12, weight: .regular))
                        .foregroundStyle(AppColors.textSecondary)
                        .accessibilityIdentifier("ActivityView.finished.subcopy")
                }
            }
        }
        .padding(.vertical, Spacing.xs)
        .accessibilityElement(children: .combine)
        .accessibilityLabel(Text(accessibilitySummary))
    }

    private var glyph: String {
        switch row.outcome {
        case .success: return "\u{2713}" // ✓
        case .couldntAnalyze: return "\u{2715}" // ✕
        case .analysisUnavailable: return "\u{2139}" // ℹ
        }
    }

    private var glyphColor: Color {
        switch row.outcome {
        case .success: return AppColors.accent
        case .couldntAnalyze, .analysisUnavailable: return AppColors.textSecondary
        }
    }

    private var subcopy: String? {
        switch row.outcome {
        case .success:
            return nil
        case .couldntAnalyze:
            // Reuse the canonical "Couldn't analyze" reason copy. Since
            // this is the Recently Finished surface and not a paused
            // state, no ResolutionHint trail is appended.
            return SurfaceReasonCopyTemplates.template(for: .couldntAnalyze)
        case .analysisUnavailable(let reason):
            return EpisodeStatusLineCopy.unavailableReasonCopy(reason)
        }
    }

    private var accessibilitySummary: String {
        let prefix: String
        switch row.outcome {
        case .success: prefix = "Done"
        case .couldntAnalyze: prefix = "Couldn't analyze"
        case .analysisUnavailable: prefix = "Analysis unavailable"
        }
        if let sub = subcopy {
            return "\(prefix), \(row.title), \(sub)"
        }
        return "\(prefix), \(row.title)"
    }
}

// MARK: - Previews

#Preview("Activity — empty (Now / Up Next empty states)") {
    ActivityView(inputProvider: { [] })
        .preferredColorScheme(.dark)
}

#Preview("Activity — populated (representative SurfaceReason in Paused)") {
    ActivityView(inputProvider: {
        let now = Date()
        return [
            // Now: a running queued job.
            ActivityEpisodeInput(
                episodeId: "ep-running",
                episodeTitle: "Hard Fork — The OpenAI Memo",
                podcastTitle: "Hard Fork",
                status: previewStatus(.queued, .waitingForTime),
                isRunning: true,
                finishedAt: nil
            ),
            // Up Next: two queued, not running.
            ActivityEpisodeInput(
                episodeId: "ep-queued-1",
                episodeTitle: "Stratechery — Rivian Earnings",
                podcastTitle: "Stratechery",
                status: previewStatus(.queued, .waitingForTime),
                isRunning: false,
                finishedAt: nil
            ),
            ActivityEpisodeInput(
                episodeId: "ep-queued-2",
                episodeTitle: "Decoder — Antitrust Update",
                podcastTitle: "Decoder",
                status: previewStatus(.queued, .waitingForTime),
                isRunning: false,
                finishedAt: nil
            ),
            // Paused: thermal — exercises SurfaceReason → ResolutionHint
            // copy resolution end-to-end.
            ActivityEpisodeInput(
                episodeId: "ep-thermal",
                episodeTitle: "The Daily — Election Recap",
                podcastTitle: "The Daily",
                status: previewStatus(.paused, .phoneIsHot, hint: .wait),
                isRunning: false,
                finishedAt: nil
            ),
            // Paused: storage cap reached — user-fixable hint.
            ActivityEpisodeInput(
                episodeId: "ep-storage",
                episodeTitle: "Conversations with Tyler — Episode 200",
                podcastTitle: "Conversations with Tyler",
                status: previewStatus(.paused, .storageFull, hint: .freeUpStorage),
                isRunning: false,
                finishedAt: nil
            ),
            // Recently Finished: success.
            ActivityEpisodeInput(
                episodeId: "ep-success",
                episodeTitle: "Acquired — TSMC Part II",
                podcastTitle: "Acquired",
                status: previewStatus(.queued, .waitingForTime, readiness: .complete),
                isRunning: false,
                finishedAt: now.addingTimeInterval(-3_600)
            ),
            // Recently Finished: couldn't analyze.
            ActivityEpisodeInput(
                episodeId: "ep-failed",
                episodeTitle: "Pivot — AI Bubble?",
                podcastTitle: "Pivot",
                status: previewStatus(.failed, .couldntAnalyze, hint: .retry),
                isRunning: false,
                finishedAt: now.addingTimeInterval(-7_200)
            ),
            // Recently Finished: analysis unavailable on this device.
            ActivityEpisodeInput(
                episodeId: "ep-unavailable",
                episodeTitle: "Lex Fridman — Episode 480",
                podcastTitle: "Lex Fridman",
                status: previewStatus(
                    .unavailable,
                    .analysisUnavailable,
                    hint: .enableAppleIntelligence,
                    unavailable: .appleIntelligenceDisabled
                ),
                isRunning: false,
                finishedAt: now.addingTimeInterval(-10_800)
            ),
        ]
    })
    .preferredColorScheme(.dark)
}

#Preview("Activity — Paused only (representative SurfaceReason rendering)") {
    ActivityView(inputProvider: {
        [
            ActivityEpisodeInput(
                episodeId: "ep-paused",
                episodeTitle: "Phone-is-hot example",
                podcastTitle: "Preview Podcast",
                status: previewStatus(.paused, .phoneIsHot, hint: .wait),
                isRunning: false,
                finishedAt: nil
            )
        ]
    })
    .preferredColorScheme(.dark)
}

@MainActor
private func previewStatus(
    _ disposition: SurfaceDisposition,
    _ reason: SurfaceReason,
    hint: ResolutionHint = .none,
    unavailable: AnalysisUnavailableReason? = nil,
    readiness: PlaybackReadiness = .none
) -> EpisodeSurfaceStatus {
    EpisodeSurfaceStatus(
        disposition: disposition,
        reason: reason,
        hint: hint,
        analysisUnavailableReason: unavailable,
        playbackReadiness: readiness,
        readinessAnchor: nil
    )
}
