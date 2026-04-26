// LibraryView.swift
// Subscribed podcasts in a compact grid with unplayed counts.
// Long-press for quick actions; pull-to-refresh triggers feed refresh.

import SwiftUI
import SwiftData

// MARK: - LibraryView

struct LibraryView: View {

    @Query(
        filter: #Predicate<Podcast> { _ in true },
        sort: \Podcast.title
    )
    private var podcasts: [Podcast]

    @Environment(\.modelContext) private var modelContext
    @Environment(PlayheadRuntime.self) private var runtime

    @State private var isRefreshing = false

    private let columns = [
        GridItem(.adaptive(minimum: 100, maximum: 140), spacing: Spacing.md)
    ]

    /// Shared discovery service instance — persists across view re-evaluations.
    @State private var discoveryService = PodcastDiscoveryService()

    var body: some View {
        // Precompute unplayed counts once per body evaluation so the
        // grid's cells become pure pass-throughs (playhead-fijb). Pre-fix
        // each cell ran `podcast.episodes.filter { !$0.isPlayed }.count`
        // inside a computed property, so a 50-podcast library traversed
        // every relationship on every cell redraw — visible jank during
        // scroll. We still iterate episodes per body evaluation, but
        // only once per body (not once per cell-redraw), and the
        // dictionary lookup is O(1).
        let unplayedCounts = Self.computeUnplayedCounts(for: podcasts)

        return NavigationStack {
            ZStack {
                AppColors.background
                    .ignoresSafeArea()

                if podcasts.isEmpty {
                    emptyState
                } else {
                    podcastGrid(unplayedCounts: unplayedCounts)
                }
            }
            .navigationTitle("Library")
            .navigationBarTitleDisplayMode(.large)
            .navigationDestination(for: Podcast.self) { podcast in
                EpisodeListView(podcast: podcast)
            }
        }
    }

    /// Builds a `[Podcast.ID: Int]` map of unplayed-episode counts for
    /// every podcast in the supplied list. Called once per `body`
    /// evaluation; result is read by `PodcastGridCell` via O(1) dictionary
    /// lookup so no cell re-traverses the episodes relationship during
    /// scroll. Exposed as `static` and `internal` so the
    /// `LibraryViewUnplayedCountPerfTests` perf-budget test can call it
    /// without rendering SwiftUI.
    static func computeUnplayedCounts(
        for podcasts: [Podcast]
    ) -> [PersistentIdentifier: Int] {
        Dictionary(uniqueKeysWithValues: podcasts.map { podcast in
            (podcast.id, podcast.episodes.lazy.filter { !$0.isPlayed }.count)
        })
    }
}

// MARK: - Subviews

private extension LibraryView {

    // MARK: Empty State

    var emptyState: some View {
        VStack(spacing: Spacing.md) {
            Image(systemName: "square.stack")
                .font(.system(size: 48, weight: .thin))
                .foregroundStyle(AppColors.textSecondary)
                .accessibilityHidden(true)

            Text("No Podcasts Yet")
                .font(AppTypography.sans(size: 20, weight: .semibold))
                .foregroundStyle(AppColors.textPrimary)

            Text("Search to subscribe to your first podcast.")
                .font(AppTypography.body)
                .foregroundStyle(AppColors.textSecondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, Spacing.xl)
        }
    }

    // MARK: Podcast Grid

    func podcastGrid(
        unplayedCounts: [PersistentIdentifier: Int]
    ) -> some View {
        ScrollView {
            LazyVGrid(columns: columns, spacing: Spacing.lg) {
                ForEach(podcasts) { podcast in
                    let unplayedCount = unplayedCounts[podcast.id, default: 0]
                    NavigationLink(value: podcast) {
                        PodcastGridCell(
                            podcast: podcast,
                            unplayedCount: unplayedCount
                        )
                    }
                    .buttonStyle(.plain)
                    .contextMenu {
                        Button(role: .destructive) {
                            unsubscribe(podcast)
                        } label: {
                            Label("Unsubscribe", systemImage: "xmark.circle")
                        }

                        Button {
                            markAllPlayed(podcast)
                        } label: {
                            Label("Mark All Played", systemImage: "checkmark.circle")
                        }
                    }
                    .accessibilityLabel("\(podcast.title), \(unplayedCount) unplayed")
                }
            }
            .padding(.horizontal, Spacing.md)
            .padding(.top, Spacing.sm)
            .padding(.bottom, Spacing.xxl)
        }
        .refreshable {
            await refreshAllFeeds()
        }
    }

    // MARK: - Actions

    func unsubscribe(_ podcast: Podcast) {
        modelContext.delete(podcast)
    }

    func markAllPlayed(_ podcast: Podcast) {
        for episode in podcast.episodes {
            episode.isPlayed = true
        }
    }

    func refreshAllFeeds() async {
        isRefreshing = true
        defer { isRefreshing = false }

        let service = discoveryService
        for podcast in podcasts {
            do {
                let _ = try await service.refreshEpisodes(
                    for: podcast, in: modelContext
                )
            } catch {
                // Silently continue — partial refresh is better than none.
            }
        }
    }
}

// MARK: - Podcast Grid Cell

private struct PodcastGridCell: View {

    let podcast: Podcast
    /// Precomputed by the parent `LibraryView` once per body evaluation
    /// (playhead-fijb). The cell never traverses `podcast.episodes` —
    /// O(1) int read replaces the per-redraw `.filter` that caused the
    /// Library-tab scroll jank flagged by the 2026-04-26 main-thread
    /// audit.
    let unplayedCount: Int
    @State private var artworkImage: UIImage?
    @State private var loadFailed = false

    var body: some View {
        VStack(spacing: Spacing.xs) {
            // Stamp-sized artwork
            ZStack(alignment: .topTrailing) {
                RoundedRectangle(cornerRadius: CornerRadius.medium)
                    .fill(AppColors.surface)
                    .aspectRatio(1, contentMode: .fit)
                    .overlay(
                        Group {
                            if let artworkImage {
                                Image(uiImage: artworkImage)
                                    .resizable()
                                    .scaledToFill()
                            } else if podcast.artworkURL != nil && !loadFailed {
                                ProgressView()
                                    .tint(AppColors.textSecondary)
                            } else {
                                artworkPlaceholder
                            }
                        }
                        .clipShape(RoundedRectangle(cornerRadius: CornerRadius.medium))
                    )
                    .task(id: podcast.artworkURL) {
                        guard let url = podcast.artworkURL else { return }
                        artworkImage = nil
                        loadFailed = false
                        do {
                            let (data, _) = try await URLSession.shared.data(from: url)
                            if let image = UIImage(data: data) {
                                artworkImage = image
                            } else {
                                loadFailed = true
                            }
                        } catch is CancellationError {
                            // Task cancelled by SwiftUI — not a real failure.
                        } catch {
                            loadFailed = true
                        }
                    }
                    .overlay(
                        RoundedRectangle(cornerRadius: CornerRadius.medium)
                            .stroke(AppColors.textSecondary.opacity(0.15), lineWidth: 1)
                    )
                    .themeShadow(AppShadow.card)

                // Unplayed badge
                if unplayedCount > 0 {
                    Text("\(unplayedCount)")
                        .font(AppTypography.mono(size: 11, weight: .semibold))
                        .foregroundStyle(Palette.bone)
                        .padding(.horizontal, 6)
                        .padding(.vertical, 2)
                        .background(
                            Capsule().fill(AppColors.accent)
                        )
                        .offset(x: -4, y: 4)
                        .accessibilityLabel("\(unplayedCount) unplayed")
                }
            }

            // Reserve two caption lines so mixed title lengths don't shift artwork.
            ZStack(alignment: .top) {
                Text("A\nA")
                    .font(AppTypography.caption)
                    .opacity(0)
                    .accessibilityHidden(true)

                Text(podcast.title)
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.textPrimary)
                    .lineLimit(2)
                    .multilineTextAlignment(.center)
                    .frame(maxWidth: .infinity, alignment: .top)
            }
            .frame(maxWidth: .infinity)
        }
    }

    private var artworkPlaceholder: some View {
        Image(systemName: "mic.fill")
            .font(.system(size: 28, weight: .light))
            .foregroundStyle(AppColors.textSecondary.opacity(0.5))
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(AppColors.surface)
            .accessibilityHidden(true)
    }
}

// MARK: - Preview

#Preview("Library — Populated") {
    LibraryView()
        .environment(PlayheadRuntime(isPreviewRuntime: true))
        .preferredColorScheme(.dark)
        .modelContainer(for: [Podcast.self, Episode.self], inMemory: true)
}

#Preview("Library — Empty") {
    LibraryView()
        .environment(PlayheadRuntime(isPreviewRuntime: true))
        .preferredColorScheme(.dark)
        .modelContainer(for: [Podcast.self, Episode.self], inMemory: true)
}
