// EpisodeListView.swift
// Per-podcast episode list with text-led layout.
// Sorted newest-first, with swipe actions for play, queue, and mark played.

import SwiftUI
import SwiftData

// MARK: - EpisodeListView

struct EpisodeListView: View {

    let podcast: Podcast

    /// Injected haptic player — defaults to `SystemHapticPlayer` in
    /// production, tests swap in a `RecordingHapticPlayer`.
    var hapticPlayer: any HapticPlaying = SystemHapticPlayer()

    @Query private var episodes: [Episode]

    @Environment(\.modelContext) private var modelContext
    @Environment(PlayheadRuntime.self) private var runtime

    /// playhead-l274: shared deep-link router for Settings. When the
    /// amber "Free up space →" CTA is tapped we push `.storage`; the
    /// Settings tab observes and scrolls on next appearance.
    @Environment(\.settingsRouter) private var settingsRouter

    @State private var navigateToNowPlaying = false
    @State private var selectedEpisode: Episode?

    /// Tracks whether the user has already dismissed the first-✓
    /// tooltip. Persisted via UserDefaults; see
    /// `OnboardingFlags.firstCheckmarkTooltipSeenKey`.
    @AppStorage(OnboardingFlags.firstCheckmarkTooltipSeenKey)
    private var hasSeenFirstCheckmarkTooltip: Bool = false

    /// Drives the tooltip overlay visibility. Separate from the
    /// persisted flag so a fade-out animation can run before the
    /// tooltip is removed from the view hierarchy.
    @State private var showsFirstCheckmarkTooltip: Bool = false

    init(podcast: Podcast, hapticPlayer: any HapticPlaying = SystemHapticPlayer()) {
        self.podcast = podcast
        self.hapticPlayer = hapticPlayer
        let podcastID = podcast.persistentModelID
        _episodes = Query(
            filter: #Predicate<Episode> { episode in
                episode.podcast?.persistentModelID == podcastID
            },
            sort: [SortDescriptor(\Episode.publishedAt, order: .reverse)]
        )
    }

    func queueEpisode(_ episode: Episode) {
        // Queue functionality will be wired in a future bead.
        // For now, mark as a no-op placeholder with haptic feedback.
        hapticPlayer.play(.save)
    }

    var body: some View {
        ZStack {
            AppColors.background
                .ignoresSafeArea()

            if episodes.isEmpty {
                emptyState
            } else {
                episodeList
            }

            if showsFirstCheckmarkTooltip {
                FirstCheckmarkTooltipView(onDismiss: dismissFirstCheckmarkTooltip)
                    .zIndex(1)
            }
        }
        .animation(Motion.standard, value: showsFirstCheckmarkTooltip)
        .navigationTitle(podcast.title)
        .navigationBarTitleDisplayMode(.large)
        .fullScreenCover(isPresented: $navigateToNowPlaying) {
            NowPlayingView(runtime: runtime)
        }
        .onAppear {
            evaluateFirstCheckmarkTooltip()
        }
        .onChange(of: anyEpisodeHasAnalysis) { _, _ in
            evaluateFirstCheckmarkTooltip()
        }
    }

    // MARK: - First ✓ Tooltip

    /// True iff at least one episode in the current list has a ready
    /// checkmark badge (`analysisSummary.hasAnalysis == true`). Drives
    /// the first-✓ tooltip trigger.
    private var anyEpisodeHasAnalysis: Bool {
        episodes.contains { $0.analysisSummary?.hasAnalysis == true }
    }

    /// Shows the tooltip on list appear (and on state changes) if the
    /// user has never dismissed it and a ✓ badge is visible. The
    /// boolean gate lives in `OnboardingGating` (pure) for testability;
    /// the `showsFirstCheckmarkTooltip` bookkeeping stays here because
    /// it is SwiftUI view state.
    private func evaluateFirstCheckmarkTooltip() {
        if hasSeenFirstCheckmarkTooltip {
            if showsFirstCheckmarkTooltip { showsFirstCheckmarkTooltip = false }
            return
        }
        let shouldShow = OnboardingGating.shouldPresentFirstCheckmarkTooltip(
            hasSeenFirstCheckmarkTooltip: hasSeenFirstCheckmarkTooltip,
            anyEpisodeHasAnalysis: anyEpisodeHasAnalysis
        )
        if shouldShow, !showsFirstCheckmarkTooltip {
            showsFirstCheckmarkTooltip = true
        }
    }

    /// Persists the dismissal and hides the overlay. Called from the
    /// tooltip's `onDismiss` callback.
    private func dismissFirstCheckmarkTooltip() {
        hasSeenFirstCheckmarkTooltip = true
        showsFirstCheckmarkTooltip = false
    }
}

// MARK: - Subviews

private extension EpisodeListView {

    // MARK: Empty State

    var emptyState: some View {
        VStack(spacing: Spacing.md) {
            Image(systemName: "waveform")
                .font(.system(size: 48, weight: .thin))
                .foregroundStyle(AppColors.textSecondary)
                .accessibilityHidden(true)

            Text("No Episodes")
                .font(AppTypography.sans(size: 20, weight: .semibold))
                .foregroundStyle(AppColors.textPrimary)

            Text("Pull to refresh, or episodes will appear after the next feed sync.")
                .font(AppTypography.body)
                .foregroundStyle(AppColors.textSecondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, Spacing.xl)
        }
    }

    // MARK: Episode List

    var episodeList: some View {
        List {
            Section {
                DownloadNextView(
                    episodes: episodes,
                    mediaCapBytes: StorageBudgetSettings.load().mediaCapBytes,
                    onDownload: { picked, _ in
                        // v1 picker only changes notification copy
                        // (bd playhead-hkg8 / UI design §D). Scheduler
                        // behavior is identical regardless of `context`.
                        Task {
                            for episode in picked {
                                await runtime.downloadManager.backgroundDownload(
                                    episodeId: episode.canonicalEpisodeKey,
                                    from: episode.audioURL
                                )
                            }
                        }
                    },
                    onFreeUpSpace: {
                        // playhead-l274: push `.storage` into the shared
                        // `SettingsRouter`. The Settings tab's
                        // `SettingsView` observes `pending` and scrolls
                        // the list to the Storage group anchor; the
                        // router is cleared via `consume()` once the
                        // scroll is honored.
                        settingsRouter?.request(.storage)
                    }
                )
                .listRowBackground(Color.clear)
                .listRowSeparator(.hidden)
                .listRowInsets(EdgeInsets(
                    top: Spacing.sm,
                    leading: Spacing.md,
                    bottom: Spacing.sm,
                    trailing: Spacing.md
                ))
            }

            ForEach(episodes) { episode in
                EpisodeRow(episode: episode)
                    .listRowBackground(AppColors.background)
                    .listRowSeparatorTint(AppColors.textSecondary.opacity(0.2))
                    .contentShape(Rectangle())
                    .onTapGesture {
                        playEpisode(episode)
                    }
                    .swipeActions(edge: .leading) {
                        Button {
                            playEpisode(episode)
                        } label: {
                            Label("Play", systemImage: "play.fill")
                        }
                        .tint(AppColors.accent)
                    }
                    .swipeActions(edge: .trailing) {
                        Button {
                            togglePlayed(episode)
                        } label: {
                            Label(
                                episode.isPlayed ? "Unplayed" : "Played",
                                systemImage: episode.isPlayed
                                    ? "circle" : "checkmark.circle.fill"
                            )
                        }
                        .tint(AppColors.textSecondary)

                        Button {
                            queueEpisode(episode)
                        } label: {
                            Label("Queue", systemImage: "text.badge.plus")
                        }
                        .tint(Palette.mutedSage)
                    }
                    .accessibilityElement(children: .combine)
                    .accessibilityLabel("\(episode.title)\(episode.isPlayed ? ", played" : "")")
                    .accessibilityHint("Tap to play this episode")
            }
        }
        .listStyle(.plain)
        .scrollContentBackground(.hidden)
    }

    // MARK: - Actions

    func playEpisode(_ episode: Episode) {
        selectedEpisode = episode
        Task {
            await runtime.playEpisode(episode)
        }
        navigateToNowPlaying = true
    }

    func togglePlayed(_ episode: Episode) {
        episode.isPlayed.toggle()
    }
}

// MARK: - Episode Row

private struct EpisodeRow: View {

    let episode: Episode

    var body: some View {
        VStack(alignment: .leading, spacing: Spacing.xs) {
            // Title (serif per spec)
            Text(episode.title)
                .font(AppTypography.serif(size: 17, weight: .regular))
                .foregroundStyle(episode.isPlayed ? AppColors.textSecondary : AppColors.textPrimary)
                .lineLimit(2)

            // Date and duration (mono)
            HStack(spacing: Spacing.sm) {
                if let date = episode.publishedAt {
                    Text(Self.formatEpisodeDate(date))
                        .font(AppTypography.timestamp)
                        .foregroundStyle(AppColors.textTertiary)
                }

                if let duration = episode.duration {
                    Text(TimeFormatter.formatDuration(duration))
                        .font(AppTypography.timestamp)
                        .foregroundStyle(AppColors.textTertiary)
                }

                Spacer()

                // Readiness status (playhead-cthe)
                //
                // The ✓ affordance is a DERIVED view of
                // `(coverageSummary, playbackAnchor)`. Per the Phase 2
                // spec, we render the checkmark only for `.proximal` or
                // `.complete` — the two states where starting playback
                // now yields a usable skip-prepared experience. A
                // `.deferredOnly` episode has analysis somewhere, but
                // not near the current playback point, so showing a ✓
                // would mislead the user into thinking ads will be
                // skipped from the start.
                if libraryRowShouldShowReadinessCheckmark(episode: episode) {
                    Image(systemName: "checkmark.circle")
                        .font(.system(size: 12, weight: .medium))
                        .foregroundStyle(Palette.mutedSage)
                        .accessibilityLabel("Analysis complete")
                }

                // Ad count — small copper numeral (not a badge)
                if let summary = episode.analysisSummary, summary.adSegmentCount > 0 {
                    Text("\(summary.adSegmentCount)")
                        .font(AppTypography.mono(size: 12, weight: .semibold))
                        .foregroundStyle(AppColors.accent)
                        .accessibilityLabel("\(summary.adSegmentCount) ad segments detected")
                }
            }

            // Progress bar for partially played episodes
            if !episode.isPlayed, episode.playbackPosition > 0,
               let duration = episode.duration, duration > 0
            {
                GeometryReader { geo in
                    let fraction = min(episode.playbackPosition / duration, 1.0)
                    ZStack(alignment: .leading) {
                        RoundedRectangle(cornerRadius: 1)
                            .fill(AppColors.textSecondary.opacity(0.2))
                            .frame(height: 2)
                        RoundedRectangle(cornerRadius: 1)
                            .fill(AppColors.accent)
                            .frame(width: geo.size.width * fraction, height: 2)
                    }
                }
                .frame(height: 2)
                .accessibilityValue("Progress: \(Int(min(episode.playbackPosition / (episode.duration ?? 1), 1.0) * 100)) percent")
            }
        }
        .padding(.vertical, Spacing.xs)
    }

    // MARK: - Formatting

    /// Compact episode date: "Mar 15" for current year, "Mar 15, 2024" for older.
    private static func formatEpisodeDate(_ date: Date) -> String {
        let calendar = Calendar.current
        let episodeYear = calendar.component(.year, from: date)
        let currentYear = calendar.component(.year, from: .now)

        if episodeYear < currentYear {
            return date.formatted(.dateTime.month(.abbreviated).day().year())
        }
        return date.formatted(.dateTime.month(.abbreviated).day())
    }
}

// MARK: - Library Row Readiness (playhead-cthe)

/// `true` when a Library row should render the ✓ affordance for the
/// supplied episode. Routes through
/// `derivePlaybackReadiness(coverage:anchor:)` so every readiness
/// decision in the app uses the same pure function (NowPlaying /
/// Activity / Library cannot drift).
///
/// Exposed at file scope (rather than nested inside the private
/// `EpisodeRow`) so the behavioral readiness test can exercise it
/// directly without instantiating SwiftUI's @Query / ModelContext
/// environment. The function has no SwiftUI dependency — it reads two
/// Codable attributes off the Episode and computes a Bool.
func libraryRowShouldShowReadinessCheckmark(episode: Episode) -> Bool {
    let readiness = derivePlaybackReadiness(
        coverage: episode.coverageSummary,
        anchor: episode.playbackAnchor
    )
    switch readiness {
    case .proximal, .complete:
        return true
    case .none, .deferredOnly:
        return false
    }
}

// MARK: - Preview

#Preview("Episode List") {
    NavigationStack {
        EpisodeListView(
            podcast: Podcast(
                feedURL: URL(string: "https://example.com/feed")!,
                title: "The Daily",
                author: "The New York Times"
            )
        )
    }
    .environment(PlayheadRuntime(isPreviewRuntime: true))
    .preferredColorScheme(.dark)
    .modelContainer(for: [Podcast.self, Episode.self], inMemory: true)
}
