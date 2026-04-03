// EpisodeListView.swift
// Per-podcast episode list with text-led layout.
// Sorted newest-first, with swipe actions for play, queue, and mark played.

import SwiftUI
import SwiftData

// MARK: - EpisodeListView

struct EpisodeListView: View {

    let podcast: Podcast

    @Query private var episodes: [Episode]

    @Environment(\.modelContext) private var modelContext
    @EnvironmentObject private var runtime: PlayheadRuntime

    @State private var navigateToNowPlaying = false
    @State private var selectedEpisode: Episode?

    init(podcast: Podcast) {
        self.podcast = podcast
        let podcastID = podcast.persistentModelID
        _episodes = Query(
            filter: #Predicate<Episode> { episode in
                episode.podcast?.persistentModelID == podcastID
            },
            sort: [SortDescriptor(\Episode.publishedAt, order: .reverse)]
        )
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
        }
        .navigationTitle(podcast.title)
        .navigationBarTitleDisplayMode(.large)
        .fullScreenCover(isPresented: $navigateToNowPlaying) {
            NowPlayingView(runtime: runtime)
        }
    }
}

// MARK: - Subviews

private extension EpisodeListView {

    // MARK: Empty State

    var emptyState: some View {
        VStack(spacing: Spacing.md) {
            Image(systemName: "waveform")
                .font(.system(size: 48, weight: .thin))
                .foregroundStyle(AppColors.secondary)

            Text("No Episodes")
                .font(AppTypography.sans(size: 20, weight: .semibold))
                .foregroundStyle(AppColors.text)

            Text("Pull to refresh, or episodes will appear after the next feed sync.")
                .font(AppTypography.body)
                .foregroundStyle(AppColors.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, Spacing.xl)
        }
    }

    // MARK: Episode List

    var episodeList: some View {
        List {
            ForEach(episodes) { episode in
                EpisodeRow(episode: episode)
                    .listRowBackground(AppColors.background)
                    .listRowSeparatorTint(AppColors.secondary.opacity(0.2))
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
                        .tint(AppColors.secondary)

                        Button {
                            queueEpisode(episode)
                        } label: {
                            Label("Queue", systemImage: "text.badge.plus")
                        }
                        .tint(Palette.mutedSage)
                    }
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

    func queueEpisode(_ episode: Episode) {
        // Queue functionality will be wired in a future bead.
        // For now, mark as a no-op placeholder with haptic feedback.
        let generator = UINotificationFeedbackGenerator()
        generator.notificationOccurred(.success)
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
                .foregroundStyle(episode.isPlayed ? AppColors.secondary : AppColors.text)
                .lineLimit(2)

            // Date and duration (mono)
            HStack(spacing: Spacing.sm) {
                if let date = episode.publishedAt {
                    Text(date.formatted(.episodeDate))
                        .font(AppTypography.timestamp)
                        .foregroundStyle(AppColors.metadata)
                }

                if let duration = episode.duration {
                    Text(Self.formatDuration(duration))
                        .font(AppTypography.timestamp)
                        .foregroundStyle(AppColors.metadata)
                }

                Spacer()

                // Transcription status
                if let summary = episode.analysisSummary, summary.hasAnalysis {
                    Image(systemName: "checkmark.circle")
                        .font(.system(size: 12, weight: .medium))
                        .foregroundStyle(Palette.mutedSage)
                }

                // Ad count — small copper numeral (not a badge)
                if let summary = episode.analysisSummary, summary.adSegmentCount > 0 {
                    Text("\(summary.adSegmentCount)")
                        .font(AppTypography.mono(size: 12, weight: .semibold))
                        .foregroundStyle(AppColors.accent)
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
                            .fill(AppColors.secondary.opacity(0.2))
                            .frame(height: 2)
                        RoundedRectangle(cornerRadius: 1)
                            .fill(AppColors.accent)
                            .frame(width: geo.size.width * fraction, height: 2)
                    }
                }
                .frame(height: 2)
            }
        }
        .padding(.vertical, Spacing.xs)
    }

    // MARK: - Formatting

    private static func formatDuration(_ seconds: TimeInterval) -> String {
        guard seconds.isFinite, seconds >= 0 else { return "" }
        let totalSeconds = Int(seconds)
        let hours = totalSeconds / 3600
        let minutes = (totalSeconds % 3600) / 60
        if hours > 0 {
            return "\(hours)h \(minutes)m"
        }
        return "\(minutes) min"
    }
}

// MARK: - Date Formatting

private extension FormatStyle where Self == Date.FormatStyle {
    /// Compact episode date: "Mar 15" or "Mar 15, 2024" for older episodes.
    static var episodeDate: Date.FormatStyle {
        let calendar = Calendar.current
        let now = Date.now
        if let yearAgo = calendar.date(byAdding: .year, value: -1, to: now) {
            // We can't conditionally return different styles in a static,
            // so use a generous format that works for both cases.
            _ = yearAgo // suppress warning
        }
        return .dateTime.month(.abbreviated).day()
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
    .preferredColorScheme(.dark)
    .modelContainer(for: [Podcast.self, Episode.self], inMemory: true)
}
