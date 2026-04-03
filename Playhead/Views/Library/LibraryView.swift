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

    @State private var isRefreshing = false

    private let discoveryService = PodcastDiscoveryService()

    private let columns = [
        GridItem(.adaptive(minimum: 100, maximum: 140), spacing: Spacing.md)
    ]

    var body: some View {
        NavigationStack {
            ZStack {
                AppColors.background
                    .ignoresSafeArea()

                if podcasts.isEmpty {
                    emptyState
                } else {
                    podcastGrid
                }
            }
            .navigationTitle("Library")
            .navigationBarTitleDisplayMode(.large)
            .navigationDestination(for: Podcast.self) { podcast in
                EpisodeListView(podcast: podcast)
            }
        }
    }
}

// MARK: - Subviews

private extension LibraryView {

    // MARK: Empty State

    var emptyState: some View {
        VStack(spacing: Spacing.md) {
            Image(systemName: "square.stack")
                .font(.system(size: 48, weight: .thin))
                .foregroundStyle(AppColors.secondary)

            Text("No Podcasts Yet")
                .font(AppTypography.sans(size: 20, weight: .semibold))
                .foregroundStyle(AppColors.text)

            Text("Search to subscribe to your first podcast.")
                .font(AppTypography.body)
                .foregroundStyle(AppColors.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, Spacing.xl)
        }
    }

    // MARK: Podcast Grid

    var podcastGrid: some View {
        ScrollView {
            LazyVGrid(columns: columns, spacing: Spacing.lg) {
                ForEach(podcasts) { podcast in
                    NavigationLink(value: podcast) {
                        PodcastGridCell(podcast: podcast)
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

        for podcast in podcasts {
            do {
                let _ = try await discoveryService.refreshEpisodes(
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

    private var unplayedCount: Int {
        podcast.episodes.filter { !$0.isPlayed }.count
    }

    var body: some View {
        VStack(spacing: Spacing.xs) {
            // Stamp-sized artwork
            ZStack(alignment: .topTrailing) {
                RoundedRectangle(cornerRadius: CornerRadius.md)
                    .fill(AppColors.surface)
                    .aspectRatio(1, contentMode: .fit)
                    .overlay(
                        Group {
                            if let artworkURL = podcast.artworkURL {
                                AsyncImage(url: artworkURL) { phase in
                                    switch phase {
                                    case .success(let image):
                                        image
                                            .resizable()
                                            .scaledToFill()
                                    case .failure:
                                        artworkPlaceholder
                                    case .empty:
                                        ProgressView()
                                            .tint(AppColors.secondary)
                                    @unknown default:
                                        artworkPlaceholder
                                    }
                                }
                            } else {
                                artworkPlaceholder
                            }
                        }
                        .clipShape(RoundedRectangle(cornerRadius: CornerRadius.md))
                    )
                    .overlay(
                        RoundedRectangle(cornerRadius: CornerRadius.md)
                            .stroke(AppColors.secondary.opacity(0.15), lineWidth: 1)
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
                }
            }

            // Title
            Text(podcast.title)
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.text)
                .lineLimit(2)
                .multilineTextAlignment(.center)
                .frame(maxWidth: .infinity)
        }
    }

    private var artworkPlaceholder: some View {
        Image(systemName: "mic.fill")
            .font(.system(size: 28, weight: .light))
            .foregroundStyle(AppColors.secondary.opacity(0.5))
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(AppColors.surface)
    }
}

// MARK: - Preview

#Preview("Library — Populated") {
    LibraryView()
        .preferredColorScheme(.dark)
        .modelContainer(for: [Podcast.self, Episode.self], inMemory: true)
}

#Preview("Library — Empty") {
    LibraryView()
        .preferredColorScheme(.dark)
        .modelContainer(for: [Podcast.self, Episode.self], inMemory: true)
}
