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
    ///
    /// Constructed with a `persistQueueOrder` closure (playhead-cjqq)
    /// so drag-reorders are written back to SwiftData. The default
    /// closure is a no-op for SwiftUI Previews / empty-state usage;
    /// production wires a closure that updates `Episode.queuePosition`
    /// via the model context.
    @State private var viewModel: ActivityViewModel

    /// Closure that produces a fresh batch of inputs. Injected so
    /// production wires it to a `ActivitySnapshotProvider` actor and
    /// SwiftUI Previews can hand back static fixtures inline. Default
    /// returns an empty list — the empty-state previews and tests rely
    /// on that.
    let inputProvider: @MainActor () async -> [ActivityEpisodeInput]

    init(
        inputProvider: @escaping @MainActor () async -> [ActivityEpisodeInput] = { [] },
        persistQueueOrder: @escaping @MainActor ([(episodeId: String, queuePosition: Int)]) -> Void = { _ in }
    ) {
        self.inputProvider = inputProvider
        _viewModel = State(
            initialValue: ActivityViewModel(persistQueueOrder: persistQueueOrder)
        )
    }

    /// playhead-5nwy: skeleton-promotion threshold. If the first inputs
    /// fetch has not resolved within this window the View renders a
    /// neutral skeleton row instead of an empty list, so a slow store
    /// read never looks like a frozen app.
    static let skeletonPromotionDelay: TimeInterval = 0.25

    /// playhead-5nwy: tracked separately from `viewModel.loadState`
    /// because the promotion is a presentation concern (the VM knows
    /// it's loading; only the View knows whether enough time has
    /// elapsed to promote that into a visible skeleton).
    @State private var showSkeleton: Bool = false

    var body: some View {
        NavigationStack {
            ZStack {
                AppColors.background
                    .ignoresSafeArea()

                // List (not ScrollView+VStack) is required so the Up
                // Next section can opt into `.onMove` drag-to-reorder.
                // `.plain` style + hidden separators + cleared
                // backgrounds preserve the prior visual treatment so
                // sections still read as bold-titled groups inside the
                // app's dark surface.
                List {
                    if showSkeleton && viewModel.loadState != .loaded {
                        skeletonSection
                    } else {
                        nowSection
                        upNextSection
                        pausedSection
                        recentlyFinishedSection
                    }
                }
                .listStyle(.plain)
                .scrollContentBackground(.hidden)
                .background(AppColors.background)
                .refreshable {
                    await refresh()
                }
            }
            .navigationTitle("Activity")
            .navigationBarTitleDisplayMode(.large)
        }
        .task {
            // Initial load.
            viewModel.beginLoad()
            await scheduleSkeletonPromotion()
            await refresh()
        }
        .onReceive(
            NotificationCenter.default.publisher(
                for: ActivityRefreshNotification.name
            )
        ) { _ in
            Task { @MainActor in await refresh() }
        }
    }

    private func refresh() async {
        let inputs = await inputProvider()
        viewModel.refresh(from: inputs)
        showSkeleton = false
    }

    /// Spawn a delayed flip into the skeleton state. The timer races
    /// the first `inputProvider()` call: if inputs arrive first the
    /// flip is a no-op (`refresh()` clears `showSkeleton`); if the
    /// store read stalls past the threshold the user sees a neutral
    /// skeleton instead of an empty list.
    private func scheduleSkeletonPromotion() async {
        Task { @MainActor in
            try? await Task.sleep(nanoseconds: UInt64(Self.skeletonPromotionDelay * 1_000_000_000))
            if viewModel.loadState != .loaded {
                showSkeleton = true
            }
        }
    }
}

// MARK: - Sections

private extension ActivityView {

    /// playhead-5nwy: neutral placeholder shown while the initial inputs
    /// fetch has been pending past the promotion threshold. Shape
    /// mimics a row so the layout doesn't reflow when real data lands;
    /// no spinner — a spinning indicator is the visual equivalent of
    /// "frozen" and is exactly the impression we're defending against.
    var skeletonSection: some View {
        Section {
            Text("Catching up\u{2026}")
                .font(AppTypography.body)
                .foregroundStyle(AppColors.textSecondary)
                .accessibilityIdentifier("ActivityView.skeleton")
                .listRowBackground(Color.clear)
                .listRowSeparator(.hidden)
                .padding(.vertical, Spacing.xs)
        } header: {
            sectionHeader("Now")
        }
    }

    var nowSection: some View {
        Section {
            if viewModel.snapshot.now.isEmpty {
                Text("Nothing running — pull down to refresh")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textSecondary)
                    .accessibilityIdentifier("ActivityView.now.empty")
                    .listRowBackground(Color.clear)
                    .listRowSeparator(.hidden)
            } else {
                ForEach(viewModel.snapshot.now) { row in
                    NowRowView(row: row)
                        .listRowBackground(Color.clear)
                        .listRowSeparator(.hidden)
                }
            }
        } header: {
            sectionHeader("Now")
        }
    }

    var upNextSection: some View {
        Section {
            if viewModel.snapshot.upNext.isEmpty {
                Text("Nothing queued")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textSecondary)
                    .accessibilityIdentifier("ActivityView.upNext.empty")
                    .listRowBackground(Color.clear)
                    .listRowSeparator(.hidden)
            } else {
                ForEach(viewModel.snapshot.upNext) { row in
                    UpNextRowView(row: row)
                        .listRowBackground(Color.clear)
                        .listRowSeparator(.hidden)
                }
                // Drag-to-reorder per bead spec ("Up Next: queued
                // eligible work, reorder by drag"). Funnels into the
                // VM so the snapshot's `upNext` ordering is the
                // single source of truth the view re-renders against.
                // No EditButton — modern iOS supports the long-press
                // drag affordance in plain List rows without an
                // explicit edit-mode toggle.
                .onMove { source, destination in
                    viewModel.moveUpNext(from: source, to: destination)
                }
            }
        } header: {
            sectionHeader("Up Next")
        }
    }

    @ViewBuilder
    var pausedSection: some View {
        if !viewModel.snapshot.paused.isEmpty {
            Section {
                ForEach(viewModel.snapshot.paused) { row in
                    PausedRowView(row: row)
                        .listRowBackground(Color.clear)
                        .listRowSeparator(.hidden)
                }
            } header: {
                sectionHeader("Paused")
            }
        }
    }

    @ViewBuilder
    var recentlyFinishedSection: some View {
        if !viewModel.snapshot.recentlyFinished.isEmpty {
            Section {
                ForEach(viewModel.snapshot.recentlyFinished) { row in
                    RecentlyFinishedRowView(row: row)
                        .listRowBackground(Color.clear)
                        .listRowSeparator(.hidden)
                }
            } header: {
                sectionHeader("Recently Finished")
            }
        }
    }

    /// Section header styled to match the prior `ActivitySection`
    /// bold-title look so the visual cadence of the screen survives the
    /// migration from `ScrollView + VStack` to `List + Section`.
    func sectionHeader(_ title: String) -> some View {
        Text(title)
            .font(AppTypography.sans(size: 18, weight: .semibold))
            .foregroundStyle(AppColors.textPrimary)
            .textCase(nil)
            .padding(.top, Spacing.sm)
            .padding(.bottom, Spacing.xs)
            .listRowInsets(EdgeInsets(
                top: 0,
                leading: 0,
                bottom: 0,
                trailing: 0
            ))
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
        case .success:
            // "Done" has no canonical source in
            // SurfaceReasonCopyTemplates / EpisodeStatusLineCopy — it's
            // the success-glyph caption for accessibility only. Inline
            // is acceptable because there is nothing to dedupe against.
            prefix = "Done"
        case .couldntAnalyze:
            // Canonical "Couldn't analyze" copy — same source the
            // visible subcopy on this row pulls from. Avoids the inline
            // string drift the spec-reviewer flagged (issue L2).
            prefix = SurfaceReasonCopyTemplates.template(for: .couldntAnalyze)
        case .analysisUnavailable:
            // No canonical short-form for this outcome — the longer
            // template ("Analysis unavailable on this device") is too
            // verbose for the accessibility prefix and the
            // unavailable-reason subcopy already carries the
            // device-specific detail.
            prefix = "Analysis unavailable"
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
