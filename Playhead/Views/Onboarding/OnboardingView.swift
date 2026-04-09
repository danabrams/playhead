// OnboardingView.swift
// First-launch experience: welcome, value prop, model download, first podcast.
//
// Flow:
// 1. Welcome screen with playhead line motif
// 2. Value proposition (single screen)
// 3. Model download with progress (fast-path first)
// 4. Search and subscribe to first podcast
//
// The aha moment — the first ad skip — happens during the first listen,
// powered by the 12-min preview budget that reliably lands an ad skip.

import SwiftUI
import SwiftData

// MARK: - OnboardingView

struct OnboardingView: View {

    @AppStorage("hasCompletedOnboarding") private var hasCompletedOnboarding = false

    @State private var step: OnboardingStep = .welcome
    @State private var welcomeAppeared = false

    var body: some View {
        ZStack {
            AppColors.background
                .ignoresSafeArea()

            switch step {
            case .welcome:
                WelcomeStepView(onContinue: { advanceTo(.valueProp) })
            case .valueProp:
                ValuePropStepView(onContinue: { advanceTo(.modelDownload) })
            case .modelDownload:
                ModelDownloadStepView(onContinue: { advanceTo(.firstPodcast) })
            case .firstPodcast:
                FirstPodcastStepView(onComplete: { completeOnboarding() })
            }
        }
        .animation(Motion.standard, value: step)
    }

    private func advanceTo(_ next: OnboardingStep) {
        step = next
    }

    private func completeOnboarding() {
        hasCompletedOnboarding = true
    }
}

// MARK: - Steps

enum OnboardingStep: Int, CaseIterable {
    case welcome
    case valueProp
    case modelDownload
    case firstPodcast
}

// MARK: - Welcome

private struct WelcomeStepView: View {

    let onContinue: () -> Void

    @State private var lineExtended = false
    @State private var textVisible = false

    var body: some View {
        GeometryReader { geometry in
            let availableWidth = geometry.size.width

            VStack(spacing: 0) {
                Spacer()

                // Playhead line motif: a horizontal copper line
                // that extends from left, with the app name appearing after.
                VStack(spacing: Spacing.lg) {
                    // The playhead line
                    ZStack(alignment: .leading) {
                        Rectangle()
                            .fill(AppColors.accent)
                            .frame(height: 2)
                            .frame(
                                width: lineExtended ? availableWidth * 0.6 : 0
                            )

                        // Playhead dot at the leading edge of the line
                        Circle()
                            .fill(AppColors.accent)
                            .frame(width: 8, height: 8)
                            .offset(
                                x: lineExtended ? availableWidth * 0.6 - 4 : -4
                            )
                            .opacity(lineExtended ? 1 : 0)
                    }
                    .frame(height: 8)
                    .accessibilityHidden(true)

                    VStack(spacing: Spacing.xs) {
                        Text("Playhead")
                            .font(AppTypography.sans(size: 36, weight: .semibold))
                            .foregroundStyle(AppColors.textPrimary)

                        Text("Podcast listening, minus the ads.")
                            .font(AppTypography.body)
                            .foregroundStyle(AppColors.textSecondary)
                    }
                    .opacity(textVisible ? 1 : 0)
                    .offset(y: textVisible ? 0 : 12)
                }

                Spacer()

                OnboardingButton(label: "Get Started") {
                    onContinue()
                }
                .opacity(textVisible ? 1 : 0)
                .padding(.horizontal, Spacing.xl)
                .padding(.bottom, Spacing.xxl)
            }
            .onAppear {
                // Stagger the entrance: line first, then text.
                withAnimation(.easeOut(duration: 0.8)) {
                    lineExtended = true
                }
                withAnimation(.easeOut(duration: 0.5).delay(0.5)) {
                    textVisible = true
                }
            }
        }
    }
}

// MARK: - Value Prop

private struct ValuePropStepView: View {

    let onContinue: () -> Void

    @State private var appeared = false

    var body: some View {
        VStack(spacing: 0) {
            Spacer()

            VStack(spacing: Spacing.xl) {
                // Feature list — tight, premium, no filler
                VStack(spacing: Spacing.lg) {
                    valuePropRow(
                        icon: "forward.fill",
                        title: "Ads detected, skipped",
                        detail: "On-device analysis spots ads in real time."
                    )

                    valuePropRow(
                        icon: "lock.shield",
                        title: "Entirely on your device",
                        detail: "Audio never leaves your phone. No cloud, no accounts."
                    )

                    valuePropRow(
                        icon: "purchased",
                        title: "Buy once, keep forever",
                        detail: "No subscriptions. One purchase unlocks everything."
                    )
                }
            }
            .opacity(appeared ? 1 : 0)
            .offset(y: appeared ? 0 : 16)

            Spacer()

            OnboardingButton(label: "Continue") {
                onContinue()
            }
            .opacity(appeared ? 1 : 0)
            .padding(.horizontal, Spacing.xl)
            .padding(.bottom, Spacing.xxl)
        }
        .padding(.horizontal, Spacing.lg)
        .onAppear {
            withAnimation(.easeOut(duration: 0.4)) {
                appeared = true
            }
        }
    }

    private func valuePropRow(icon: String, title: String, detail: String) -> some View {
        HStack(alignment: .top, spacing: Spacing.md) {
            Image(systemName: icon)
                .font(.system(size: 20, weight: .medium))
                .foregroundStyle(AppColors.accent)
                .frame(width: 32, alignment: .center)
                .padding(.top, 2)
                .accessibilityHidden(true)

            VStack(alignment: .leading, spacing: Spacing.xxs) {
                Text(title)
                    .font(AppTypography.sans(size: 17, weight: .semibold))
                    .foregroundStyle(AppColors.textPrimary)

                Text(detail)
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textSecondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
        .accessibilityElement(children: .combine)
    }
}

// MARK: - Model Download

private struct ModelDownloadStepView: View {

    let onContinue: () -> Void

    @State private var viewModel = ModelDownloadViewModel()

    var body: some View {
        VStack(spacing: 0) {
            Spacer()

            VStack(spacing: Spacing.lg) {
                // Animated indicator
                ZStack {
                    Circle()
                        .stroke(AppColors.textSecondary.opacity(0.2), lineWidth: 3)
                        .frame(width: 72, height: 72)

                    if viewModel.fastPathReady {
                        Image(systemName: "checkmark")
                            .font(.system(size: 28, weight: .medium))
                            .foregroundStyle(AppColors.accent)
                            .transition(.scale.combined(with: .opacity))
                    } else {
                        Circle()
                            .trim(from: 0, to: viewModel.displayProgress)
                            .stroke(AppColors.accent, style: StrokeStyle(lineWidth: 3, lineCap: .round))
                            .frame(width: 72, height: 72)
                            .rotationEffect(.degrees(-90))
                            .animation(Motion.quick, value: viewModel.displayProgress)
                    }
                }
                .accessibilityLabel(viewModel.fastPathReady ? "Download complete" : "Downloading: \(Int(viewModel.displayProgress * 100)) percent")

                VStack(spacing: Spacing.xs) {
                    Text(viewModel.fastPathReady ? "Ready to go" : "Preparing ad detection")
                        .font(AppTypography.sans(size: 20, weight: .semibold))
                        .foregroundStyle(AppColors.textPrimary)

                    Text(viewModel.statusMessage)
                        .font(AppTypography.caption)
                        .foregroundStyle(AppColors.textSecondary)
                        .multilineTextAlignment(.center)
                }
            }

            Spacer()

            VStack(spacing: Spacing.sm) {
                OnboardingButton(label: "Continue") {
                    onContinue()
                }
                .disabled(!viewModel.canProceed)
                .opacity(viewModel.canProceed ? 1 : 0.4)

                if !viewModel.fastPathReady && viewModel.allModelsReady {
                    // Edge case: fast path not flagged but all are ready
                }

                if viewModel.backgroundModelsRemaining > 0 && viewModel.fastPathReady {
                    Text("Remaining models download in the background.")
                        .font(AppTypography.caption)
                        .foregroundStyle(AppColors.textTertiary)
                }
            }
            .padding(.horizontal, Spacing.xl)
            .padding(.bottom, Spacing.xxl)
        }
        .task {
            await viewModel.startDownloads()
        }
    }
}

// MARK: - First Podcast

private struct FirstPodcastStepView: View {

    let onComplete: () -> Void

    @Environment(\.modelContext) private var modelContext
    @State private var viewModel = BrowseViewModel()
    @State private var subscribedPodcast: Podcast?
    @State private var isSubscribing = false
    @State private var errorMessage: String?

    var body: some View {
        @Bindable var viewModel = viewModel
        VStack(spacing: 0) {
            // Header
            VStack(spacing: Spacing.xs) {
                Text("Find your first podcast")
                    .font(AppTypography.sans(size: 24, weight: .semibold))
                    .foregroundStyle(AppColors.textPrimary)

                Text("Search for a show to try ad-free listening.")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textSecondary)
                    .multilineTextAlignment(.center)
            }
            .padding(.top, Spacing.xxl)
            .padding(.horizontal, Spacing.lg)

            // Search field
            HStack(spacing: Spacing.sm) {
                Image(systemName: "magnifyingglass")
                    .font(.system(size: 16, weight: .medium))
                    .foregroundStyle(AppColors.textSecondary)
                    .accessibilityHidden(true)

                TextField("Search podcasts", text: $viewModel.searchText)
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                    .autocorrectionDisabled()
                    .textInputAutocapitalization(.never)
                    .submitLabel(.search)

                if !viewModel.searchText.isEmpty {
                    Button {
                        viewModel.searchText = ""
                        viewModel.results = []
                    } label: {
                        Image(systemName: "xmark.circle.fill")
                            .font(.system(size: 16))
                            .foregroundStyle(AppColors.textSecondary)
                    }
                    .accessibilityLabel("Clear search")
                }
            }
            .padding(.horizontal, Spacing.md)
            .padding(.vertical, Spacing.sm)
            .background(
                RoundedRectangle(cornerRadius: CornerRadius.medium)
                    .fill(AppColors.surface)
            )
            .overlay(
                RoundedRectangle(cornerRadius: CornerRadius.medium)
                    .stroke(AppColors.textSecondary.opacity(0.2), lineWidth: 1)
            )
            .padding(.horizontal, Spacing.lg)
            .padding(.top, Spacing.lg)

            // Results
            if viewModel.isSearching && viewModel.results.isEmpty {
                Spacer()
                ProgressView()
                    .tint(AppColors.accent)
                    .accessibilityLabel("Searching")
                Spacer()
            } else if viewModel.results.isEmpty && !viewModel.searchText.isEmpty {
                Spacer()
                Text("No results found.")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textSecondary)
                Spacer()
            } else if viewModel.results.isEmpty {
                Spacer()
                Image(systemName: "waveform")
                    .font(.system(size: 40, weight: .thin))
                    .foregroundStyle(AppColors.textSecondary.opacity(0.5))
                    .padding(.bottom, Spacing.sm)
                    .accessibilityHidden(true)
                Text("Type a name or topic above.")
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.textTertiary)
                Spacer()
            } else {
                ScrollView {
                    LazyVStack(spacing: 0) {
                        ForEach(viewModel.results) { result in
                            OnboardingSearchRow(
                                result: result,
                                isSubscribing: isSubscribing,
                                subscribedID: subscribedPodcast != nil ? subscribedPodcast?.feedURL.absoluteString : nil
                            ) {
                                Task { await subscribe(result: result) }
                            }

                            if result.id != viewModel.results.last?.id {
                                Divider()
                                    .foregroundStyle(AppColors.textSecondary.opacity(0.15))
                                    .padding(.leading, 72)
                            }
                        }
                    }
                    .padding(.bottom, Spacing.xxl)
                }
                .padding(.top, Spacing.sm)
            }

            if let errorMessage {
                Text(errorMessage)
                    .font(AppTypography.caption)
                    .foregroundStyle(.red)
                    .padding(.horizontal, Spacing.lg)
                    .padding(.bottom, Spacing.xs)
            }

            // Bottom action
            if subscribedPodcast != nil {
                OnboardingButton(label: "Start Listening") {
                    onComplete()
                }
                .padding(.horizontal, Spacing.xl)
                .padding(.bottom, Spacing.xxl)
                .transition(.move(edge: .bottom).combined(with: .opacity))
            }
        }
        .animation(Motion.standard, value: subscribedPodcast != nil)
        .onChange(of: viewModel.searchText) { _, newValue in
            viewModel.debounceSearch(query: newValue)
        }
    }

    @MainActor
    private func subscribe(result: DiscoveryResult) async {
        guard let feedURL = result.feedURL else {
            errorMessage = "No feed URL available."
            return
        }

        isSubscribing = true
        errorMessage = nil

        do {
            let feed = try await viewModel.discoveryService.fetchFeed(url: feedURL)
            let podcast = await viewModel.discoveryService.persist(feed, from: feedURL, in: modelContext)
            try modelContext.save()
            subscribedPodcast = podcast
        } catch {
            errorMessage = "Could not subscribe: \(error.localizedDescription)"
        }

        isSubscribing = false
    }
}

// MARK: - Onboarding Search Row

private struct OnboardingSearchRow: View {

    let result: DiscoveryResult
    let isSubscribing: Bool
    let subscribedID: String?
    let onSubscribe: () -> Void

    private var isThisSubscribed: Bool {
        subscribedID == result.feedURL?.absoluteString
    }

    var body: some View {
        HStack(spacing: Spacing.sm) {
            // Artwork
            artworkView
                .frame(width: 52, height: 52)
                .accessibilityHidden(true)

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
            }

            Spacer()

            // Subscribe action
            if isThisSubscribed {
                Image(systemName: "checkmark.circle.fill")
                    .font(.system(size: 22))
                    .foregroundStyle(AppColors.accent)
                    .accessibilityLabel("Subscribed")
            } else {
                Button {
                    onSubscribe()
                } label: {
                    Image(systemName: "plus.circle")
                        .font(.system(size: 22, weight: .light))
                        .foregroundStyle(AppColors.accent)
                }
                .disabled(isSubscribing)
                .accessibilityLabel("Subscribe to \(result.title)")
            }
        }
        .padding(.horizontal, Spacing.md)
        .padding(.vertical, Spacing.sm)
        .contentShape(Rectangle())
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
                                image.resizable().scaledToFill()
                            case .failure:
                                artworkPlaceholder
                            case .empty:
                                ProgressView().tint(AppColors.textSecondary)
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
            .font(.system(size: 18, weight: .light))
            .foregroundStyle(AppColors.textSecondary.opacity(0.5))
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(AppColors.surface)
    }
}

// MARK: - Shared Button

private struct OnboardingButton: View {

    let label: String
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Text(label)
                .font(AppTypography.sans(size: 16, weight: .semibold))
                .foregroundStyle(Palette.bone)
                .frame(maxWidth: .infinity)
                .frame(height: 52)
                .background(
                    RoundedRectangle(cornerRadius: CornerRadius.medium)
                        .fill(AppColors.accent)
                )
        }
        .accessibilityLabel(label)
    }
}

// MARK: - Preview

#Preview("Onboarding") {
    OnboardingView()
        .preferredColorScheme(.dark)
        .modelContainer(for: [Podcast.self, Episode.self], inMemory: true)
}
