// NowPlayingView.swift
// Full-screen now-playing experience. Stamp-sized artwork, copper playhead,
// full-width timeline rail, transport controls, speed selector.
// "Quiet Instrument" aesthetic — precise, minimal chrome, typographic hierarchy.

import SwiftUI

// MARK: - NowPlayingView

struct NowPlayingView: View {

    private var runtime: PlayheadRuntime
    private let ownsViewModel: Bool
    @State private var viewModel: NowPlayingViewModel
    @State private var bannerQueue = AdBannerQueue()
    @State private var showTranscriptPeek = false
    @Environment(\.dismiss) private var dismiss

    /// Accepts an optional external ViewModel for shared state with NowPlayingBar.
    /// Falls back to creating its own if none provided.
    init(runtime: PlayheadRuntime, viewModel: NowPlayingViewModel? = nil) {
        self.runtime = runtime
        let resolvedViewModel = viewModel ?? NowPlayingViewModel(runtime: runtime)
        self.ownsViewModel = viewModel == nil
        self._viewModel = State(wrappedValue: resolvedViewModel)
    }

    private var analysisAssetId: String? {
        runtime.currentAnalysisAssetId
    }

    var body: some View {
        ZStack {
            AppColors.background
                .ignoresSafeArea()

            VStack(spacing: 0) {
                // MARK: Top Chrome
                topBar
                    .padding(.horizontal, Spacing.md)
                    .padding(.top, Spacing.sm)

                Spacer(minLength: Spacing.xl)

                // MARK: Artwork
                artworkSection
                    .padding(.horizontal, Spacing.xxl)

                Spacer(minLength: Spacing.lg)

                // MARK: Titles
                titleSection
                    .padding(.horizontal, Spacing.md)

                Spacer(minLength: Spacing.lg)

                // MARK: Timeline
                timelineSection
                    .padding(.horizontal, Spacing.md)

                Spacer(minLength: Spacing.lg)

                // MARK: Transport
                transportSection
                    .padding(.horizontal, Spacing.xl)

                Spacer(minLength: Spacing.md)

                // MARK: Speed
                speedSection
                    .padding(.horizontal, Spacing.md)
                    .padding(.bottom, Spacing.lg)
            }

            // Ad skip banner — slides in at bottom, single lane, auto-dismiss.
            AdBannerView(
                queue: bannerQueue,
                onListen: { item in
                    viewModel.handleListenRewind(item: item)
                },
                // Phase 7.2: "Not an ad" correction from the banner.
                // Uses the current analysis asset ID from the runtime. If the
                // episode changed between skip and tap, the veto silently drops
                // (assetId is nil) — acceptable for this UI path.
                onNotAnAd: { item in
                    guard let assetId = runtime.currentAnalysisAssetId else { return }
                    let correctionStore = runtime.correctionStore
                    Task {
                        let event = CorrectionEvent(
                            analysisAssetId: assetId,
                            scope: CorrectionScope.exactSpan(
                                assetId: assetId,
                                ordinalRange: 0...Int.max
                            ).serialized,
                            createdAt: Date().timeIntervalSince1970,
                            source: .manualVeto,
                            podcastId: item.podcastId
                        )
                        try? await correctionStore.record(event)
                    }
                }
            )
        }
        .onAppear {
            viewModel.startObserving()
            viewModel.observeAdSegments(from: runtime.skipOrchestrator)
            viewModel.observeBanners(from: runtime.skipOrchestrator, into: bannerQueue)
            Task { await viewModel.loadSkipMode(from: runtime.skipOrchestrator) }
        }
        .onDisappear {
            if ownsViewModel {
                viewModel.stopObserving()
            } else {
                viewModel.stopObservingAdSegments()
                viewModel.stopObservingBanners()
            }
        }
        .sheet(isPresented: $showTranscriptPeek) {
            if let assetId = analysisAssetId {
                TranscriptPeekView(
                    peekViewModel: TranscriptPeekViewModel(
                        analysisAssetId: assetId,
                        store: runtime.analysisStore
                    ),
                    currentTime: viewModel.currentTime,
                    // Phase 7.2: inject the persistent correction store so the
                    // "This isn't an ad" gesture writes through to SQLite.
                    correctionStore: runtime.correctionStore
                )
                .presentationDetents([.medium, .large])
                .presentationDragIndicator(.visible)
                .presentationBackground(AppColors.surface)
            }
        }
    }
}

// MARK: - Subviews

private extension NowPlayingView {

    // MARK: Top Bar

    var topBar: some View {
        HStack {
            Button {
                dismiss()
            } label: {
                Image(systemName: "chevron.down")
                    .font(.system(size: 18, weight: .medium))
                    .foregroundStyle(AppColors.textSecondary)
            }
            .accessibilityLabel("Dismiss")
            .accessibilityHint("Closes the now playing screen")

            Spacer()

            VStack(spacing: 2) {
                Text("PLAYING FROM")
                    .font(AppTypography.sans(size: 10, weight: .semibold))
                    .foregroundStyle(AppColors.textTertiary)
                    .tracking(1.2)

                Text(viewModel.podcastTitle)
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.textSecondary)
                    .lineLimit(1)
            }
            .accessibilityElement(children: .combine)

            Spacer()

            // Transcript peek — visible when analysis is available
            if analysisAssetId != nil {
                Button {
                    showTranscriptPeek = true
                } label: {
                    Image(systemName: "text.quote")
                        .font(.system(size: 18, weight: .medium))
                        .foregroundStyle(AppColors.textSecondary)
                }
                .accessibilityLabel("Transcript")
                .accessibilityHint("Opens the transcript peek sheet")
            } else {
                // Balance the chevron when transcript unavailable
                Color.clear
                    .frame(width: 18, height: 18)
            }
        }
    }

    // MARK: Artwork

    var artworkSection: some View {
        RoundedRectangle(cornerRadius: CornerRadius.medium)
            .fill(AppColors.surface)
            .aspectRatio(1, contentMode: .fit)
            .frame(maxWidth: 140, maxHeight: 140)
            .overlay(
                Group {
                    if let artworkURL = viewModel.artworkURL {
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
                    .stroke(AppColors.textSecondary.opacity(0.2), lineWidth: 1)
            )
            .themeShadow(AppShadow.card)
            .accessibilityLabel("Episode artwork")
    }

    private var artworkPlaceholder: some View {
        Image(systemName: "mic.fill")
            .font(.title2)
            .foregroundStyle(AppColors.textSecondary.opacity(0.4))
    }

    // MARK: Titles

    var titleSection: some View {
        VStack(spacing: Spacing.xxs) {
            Text(viewModel.episodeTitle)
                .font(AppTypography.sans(size: 20, weight: .semibold))
                .foregroundStyle(AppColors.textPrimary)
                .lineLimit(2)
                .multilineTextAlignment(.center)
                .accessibilityAddTraits(.isHeader)

            Text(viewModel.podcastTitle)
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textSecondary)
                .lineLimit(1)

            skipModePill
        }
    }

    @ViewBuilder
    var skipModePill: some View {
        if !viewModel.podcastTitle.isEmpty {
            Menu {
                ForEach(SkipMode.allCases, id: \.self) { mode in
                    Button(mode.pillLabel) {
                        viewModel.setSkipMode(mode, orchestrator: runtime.skipOrchestrator)
                    }
                }
            } label: {
                Text(viewModel.activeSkipMode.pillLabel)
                    .font(AppTypography.sans(size: 10, weight: .semibold))
                    .foregroundStyle(viewModel.activeSkipMode.pillForeground)
                    .tracking(0.8)
                    .padding(.horizontal, 8)
                    .padding(.vertical, 3)
                    .background(viewModel.activeSkipMode.pillBackground)
                    .clipShape(Capsule())
                    .contentShape(Rectangle().size(width: 80, height: 44))
                    .frame(minWidth: 44, minHeight: 44)
            }
            .accessibilityLabel("Skip mode: \(viewModel.activeSkipMode.pillLabel)")
            .accessibilityHint("Tap to change skip mode for this show")
        }
    }

    // MARK: Timeline

    var timelineSection: some View {
        VStack(spacing: Spacing.xs) {
            TimelineRailView(
                progress: viewModel.progress,
                adSegments: viewModel.adSegmentRanges,
                onSeek: { fraction in
                    let target = fraction * viewModel.duration
                    viewModel.seek(to: target)
                }
            )

            HStack {
                Text(viewModel.elapsedFormatted)
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.textTertiary)
                    .accessibilityLabel("Elapsed: \(viewModel.elapsedFormatted)")

                Spacer()

                Text(viewModel.remainingFormatted)
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.textTertiary)
                    .accessibilityLabel("Remaining: \(viewModel.remainingFormatted)")
            }
        }
    }

    // MARK: Transport

    var transportSection: some View {
        HStack(spacing: Spacing.xl) {
            Spacer()

            // Skip backward 15s
            TransportButton(
                systemName: "gobackward.15",
                size: 28,
                accessibilityText: "Skip back 15 seconds"
            ) {
                viewModel.skipBackward()
            }

            // Play / Pause
            TransportButton(
                systemName: viewModel.isPlaying
                    ? "pause.fill"
                    : "play.fill",
                size: 42,
                accessibilityText: viewModel.isPlaying ? "Pause" : "Play"
            ) {
                viewModel.togglePlayPause()
            }

            // Skip forward 30s
            TransportButton(
                systemName: "goforward.30",
                size: 28,
                accessibilityText: "Skip forward 30 seconds"
            ) {
                viewModel.skipForward()
            }

            Spacer()
        }
    }

    // MARK: Speed

    var speedSection: some View {
        HStack {
            Spacer()

            SpeedSelectorView(
                currentSpeed: viewModel.playbackSpeed,
                onSpeedChanged: { speed in
                    viewModel.setSpeed(speed)
                }
            )

            Spacer()
        }
    }
}

// MARK: - SkipMode Pill Style

private extension SkipMode {
    var pillLabel: String {
        switch self {
        case .shadow: "Shadow"
        case .manual: "Manual"
        case .auto:   "Auto"
        }
    }

    var pillForeground: Color {
        switch self {
        case .shadow: AppColors.textTertiary
        case .manual: AppColors.textSecondary
        case .auto:   AppColors.accent
        }
    }

    var pillBackground: Color {
        switch self {
        case .shadow: AppColors.textTertiary.opacity(0.12)
        case .manual: AppColors.textSecondary.opacity(0.12)
        case .auto:   AppColors.accent.opacity(0.18)
        }
    }
}

// MARK: - Transport Button

/// A single transport control with haptic feedback.
struct TransportButton: View {
    let systemName: String
    let size: CGFloat
    var accessibilityText: String = ""
    var hapticPlayer: any HapticPlaying = SystemHapticPlayer()
    let action: () -> Void

    /// Factored tap handler so tests can drive the haptic + action path
    /// without rendering a live SwiftUI hierarchy.
    func handleTap() {
        hapticPlayer.play(.control)
        action()
    }

    var body: some View {
        Button {
            handleTap()
        } label: {
            Image(systemName: systemName)
                .font(.system(size: size, weight: .medium))
                .foregroundStyle(AppColors.textPrimary)
                .frame(width: size + 20, height: size + 20)
                .contentShape(Rectangle())
        }
        .buttonStyle(TransportButtonStyle())
        .accessibilityLabel(accessibilityText)
    }
}

/// Subtle scale-down on press. No bounce — precise, mechanical.
private struct TransportButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? 0.92 : 1.0)
            .animation(Motion.quick, value: configuration.isPressed)
    }
}

// MARK: - Preview

#Preview("Now Playing") {
    NowPlayingView(runtime: PlayheadRuntime(isPreviewRuntime: true))
        .preferredColorScheme(.dark)
}
