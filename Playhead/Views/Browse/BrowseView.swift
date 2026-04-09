// BrowseView.swift
// Search-driven podcast discovery with editorial typography.
//
// Uses PodcastDiscoveryService (iTunes Search API) for instant results.
// Debounced search, stamp-sized artwork, subscribe-on-tap flow.

import SwiftUI
import SwiftData

// MARK: - BrowseView

struct BrowseView: View {

    @Environment(\.modelContext) private var modelContext

    @State private var viewModel = BrowseViewModel()

    var body: some View {
        @Bindable var viewModel = viewModel
        NavigationStack {
            ZStack {
                AppColors.background
                    .ignoresSafeArea()

                if viewModel.searchText.isEmpty && viewModel.results.isEmpty {
                    emptyPrompt
                } else if viewModel.results.isEmpty && !viewModel.isSearching {
                    noResults
                } else {
                    resultsList
                }
            }
            .navigationTitle("Browse")
            .navigationBarTitleDisplayMode(.large)
            .searchable(
                text: $viewModel.searchText,
                placement: .navigationBarDrawer(displayMode: .always),
                prompt: "Search podcasts"
            )
            .onChange(of: viewModel.searchText) { _, newValue in
                viewModel.debounceSearch(query: newValue)
            }
            .alert("Error", isPresented: $viewModel.showError) {
                Button("OK", role: .cancel) {}
            } message: {
                Text(viewModel.errorMessage)
            }
        }
    }
}

// MARK: - Subviews

private extension BrowseView {

    var emptyPrompt: some View {
        VStack(spacing: Spacing.md) {
            Image(systemName: "magnifyingglass")
                .font(.system(size: 48, weight: .thin))
                .foregroundStyle(AppColors.textSecondary)
                .accessibilityHidden(true)

            Text("Discover Podcasts")
                .font(AppTypography.sans(size: 20, weight: .semibold))
                .foregroundStyle(AppColors.textPrimary)

            Text("Search by name, topic, or creator.")
                .font(AppTypography.body)
                .foregroundStyle(AppColors.textSecondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, Spacing.xl)
        }
    }

    var noResults: some View {
        VStack(spacing: Spacing.md) {
            Image(systemName: "tray")
                .font(.system(size: 48, weight: .thin))
                .foregroundStyle(AppColors.textSecondary)
                .accessibilityHidden(true)

            Text("No Results")
                .font(AppTypography.sans(size: 20, weight: .semibold))
                .foregroundStyle(AppColors.textPrimary)

            Text("Try a different search term.")
                .font(AppTypography.body)
                .foregroundStyle(AppColors.textSecondary)
        }
    }

    var resultsList: some View {
        ScrollView {
            LazyVStack(spacing: 0) {
                ForEach(viewModel.results) { result in
                    NavigationLink {
                        PodcastDetailView(
                            result: result,
                            discoveryService: viewModel.discoveryService
                        )
                    } label: {
                        SearchResultRow(result: result)
                    }
                    .buttonStyle(.plain)

                    if result.id != viewModel.results.last?.id {
                        Divider()
                            .foregroundStyle(AppColors.textSecondary.opacity(0.2))
                            .padding(.leading, 76)
                    }
                }
            }
            .padding(.bottom, Spacing.xxl)

            if viewModel.isSearching {
                ProgressView()
                    .tint(AppColors.accent)
                    .padding(.top, Spacing.lg)
                    .accessibilityLabel("Searching")
            }
        }
    }
}

// MARK: - Search Result Row

private struct SearchResultRow: View {

    let result: DiscoveryResult

    var body: some View {
        HStack(spacing: Spacing.sm) {
            // Stamp-sized artwork
            artworkView
                .frame(width: 56, height: 56)
                .accessibilityHidden(true)

            // Title + author
            VStack(alignment: .leading, spacing: Spacing.xxs) {
                Text(result.title)
                    .font(AppTypography.sans(size: 15, weight: .medium))
                    .foregroundStyle(AppColors.textPrimary)
                    .lineLimit(2)
                    .multilineTextAlignment(.leading)

                Text(result.author)
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.textSecondary)
                    .lineLimit(1)

                if let genre = result.genre {
                    Text(genre)
                        .font(AppTypography.mono(size: 11, weight: .regular))
                        .foregroundStyle(AppColors.textTertiary)
                        .lineLimit(1)
                }
            }

            Spacer()

            Image(systemName: "chevron.right")
                .font(.system(size: 12, weight: .medium))
                .foregroundStyle(AppColors.textSecondary.opacity(0.5))
                .accessibilityHidden(true)
        }
        .padding(.horizontal, Spacing.md)
        .padding(.vertical, Spacing.sm)
        .contentShape(Rectangle())
        .accessibilityElement(children: .combine)
        .accessibilityLabel("\(result.title) by \(result.author)")
    }

    private var artworkView: some View {
        RoundedRectangle(cornerRadius: CornerRadius.medium)
            .fill(AppColors.surface)
            .overlay(
                Group {
                    if let url = result.artworkURL {
                        AsyncImage(url: url) { phase in
                            switch phase {
                            case .success(let image):
                                image
                                    .resizable()
                                    .scaledToFill()
                            case .failure:
                                artworkPlaceholder
                            case .empty:
                                ProgressView()
                                    .tint(AppColors.textSecondary)
                            @unknown default:
                                artworkPlaceholder
                            }
                        }
                    } else {
                        artworkPlaceholder
                    }
                }
                .clipShape(RoundedRectangle(cornerRadius: CornerRadius.medium))
            )
            .overlay(
                RoundedRectangle(cornerRadius: CornerRadius.medium)
                    .stroke(AppColors.textSecondary.opacity(0.15), lineWidth: 0.5)
            )
    }

    private var artworkPlaceholder: some View {
        Image(systemName: "mic.fill")
            .font(.system(size: 20, weight: .light))
            .foregroundStyle(AppColors.textSecondary.opacity(0.5))
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(AppColors.surface)
    }
}

// MARK: - Podcast Detail View

struct PodcastDetailView: View {

    let result: DiscoveryResult
    let discoveryService: PodcastDiscoveryService

    @Environment(\.modelContext) private var modelContext
    @Environment(\.dismiss) private var dismiss

    @State private var isSubscribing = false
    @State private var subscribed = false
    @State private var errorMessage: String?

    var body: some View {
        ZStack {
            AppColors.background
                .ignoresSafeArea()

            ScrollView {
                VStack(spacing: Spacing.lg) {
                    // Hero artwork
                    heroArtwork
                        .accessibilityLabel("Podcast artwork for \(result.title)")

                    // Metadata
                    VStack(spacing: Spacing.xs) {
                        Text(result.title)
                            .font(AppTypography.title)
                            .foregroundStyle(AppColors.textPrimary)
                            .multilineTextAlignment(.center)

                        Text(result.author)
                            .font(AppTypography.body)
                            .foregroundStyle(AppColors.textSecondary)

                        if let genre = result.genre {
                            Text(genre)
                                .font(AppTypography.mono(size: 12, weight: .medium))
                                .foregroundStyle(AppColors.textTertiary)
                                .padding(.horizontal, Spacing.sm)
                                .padding(.vertical, Spacing.xxs)
                                .background(
                                    Capsule()
                                        .fill(AppColors.surface)
                                )
                        }

                        if let count = result.episodeCount, count > 0 {
                            Text("\(count) episodes")
                                .font(AppTypography.caption)
                                .foregroundStyle(AppColors.textTertiary)
                        }
                    }
                    .padding(.horizontal, Spacing.md)

                    // Subscribe button
                    subscribeButton
                        .padding(.horizontal, Spacing.xl)

                    if let errorMessage {
                        Text(errorMessage)
                            .font(AppTypography.caption)
                            .foregroundStyle(.red)
                            .multilineTextAlignment(.center)
                            .padding(.horizontal, Spacing.md)
                    }
                }
                .padding(.top, Spacing.lg)
                .padding(.bottom, Spacing.xxl)
            }
        }
        .navigationBarTitleDisplayMode(.inline)
    }

    private var heroArtwork: some View {
        RoundedRectangle(cornerRadius: CornerRadius.large)
            .fill(AppColors.surface)
            .frame(width: 200, height: 200)
            .overlay(
                Group {
                    if let url = result.artworkURL {
                        AsyncImage(url: url) { phase in
                            switch phase {
                            case .success(let image):
                                image
                                    .resizable()
                                    .scaledToFill()
                            case .failure:
                                heroPlaceholder
                            case .empty:
                                ProgressView()
                                    .tint(AppColors.textSecondary)
                            @unknown default:
                                heroPlaceholder
                            }
                        }
                    } else {
                        heroPlaceholder
                    }
                }
                .clipShape(RoundedRectangle(cornerRadius: CornerRadius.large))
            )
            .overlay(
                RoundedRectangle(cornerRadius: CornerRadius.large)
                    .stroke(AppColors.textSecondary.opacity(0.15), lineWidth: 1)
            )
            .themeShadow(AppShadow.elevated)
    }

    private var heroPlaceholder: some View {
        Image(systemName: "mic.fill")
            .font(.system(size: 48, weight: .light))
            .foregroundStyle(AppColors.textSecondary.opacity(0.5))
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(AppColors.surface)
            .accessibilityHidden(true)
    }

    private var subscribeButton: some View {
        Button {
            Task {
                await subscribe()
            }
        } label: {
            Group {
                if isSubscribing {
                    ProgressView()
                        .tint(Palette.bone)
                } else if subscribed {
                    Label("Subscribed", systemImage: "checkmark")
                } else {
                    Text("Subscribe")
                }
            }
            .font(AppTypography.sans(size: 16, weight: .semibold))
            .foregroundStyle(subscribed ? AppColors.textPrimary : Palette.bone)
            .frame(maxWidth: .infinity)
            .frame(height: 48)
            .background(
                RoundedRectangle(cornerRadius: CornerRadius.medium)
                    .fill(subscribed ? AppColors.surface : AppColors.accent)
            )
            .overlay(
                RoundedRectangle(cornerRadius: CornerRadius.medium)
                    .stroke(
                        subscribed ? AppColors.textSecondary.opacity(0.3) : .clear,
                        lineWidth: 1
                    )
            )
        }
        .disabled(isSubscribing || subscribed)
        .accessibilityLabel(subscribed ? "Subscribed to \(result.title)" : "Subscribe to \(result.title)")
    }

    @MainActor
    private func subscribe() async {
        guard let feedURL = result.feedURL else {
            errorMessage = "No feed URL available for this podcast."
            return
        }

        isSubscribing = true
        errorMessage = nil

        do {
            let feed = try await discoveryService.fetchFeed(url: feedURL)
            let _ = await discoveryService.persist(feed, from: feedURL, in: modelContext)
            try modelContext.save()
            subscribed = true
        } catch {
            errorMessage = "Could not subscribe: \(error.localizedDescription)"
        }

        isSubscribing = false
    }
}

// MARK: - Preview

#Preview("Browse — Empty") {
    BrowseView()
        .preferredColorScheme(.dark)
        .modelContainer(for: [Podcast.self, Episode.self], inMemory: true)
}
