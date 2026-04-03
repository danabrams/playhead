// NowPlayingView.swift
// Full-screen now-playing experience. Stamp-sized artwork, copper playhead,
// full-width timeline rail, transport controls, speed selector.
// "Quiet Instrument" aesthetic — precise, minimal chrome, typographic hierarchy.

import SwiftUI

// MARK: - NowPlayingView

struct NowPlayingView: View {

    private var runtime: PlayheadRuntime
    @State private var viewModel: NowPlayingViewModel
    @State private var bannerQueue = AdBannerQueue()
    @State private var showTranscriptPeek = false
    @Environment(\.dismiss) private var dismiss

    /// Accepts an optional external ViewModel for shared state with NowPlayingBar.
    /// Falls back to creating its own if none provided.
    init(runtime: PlayheadRuntime, viewModel: NowPlayingViewModel? = nil) {
        self.runtime = runtime
        self._viewModel = State(wrappedValue: viewModel ?? NowPlayingViewModel(runtime: runtime))
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
                }
            )
        }
        .onAppear {
            viewModel.startObserving()
            viewModel.observeAdSegments(from: runtime.skipOrchestrator)
        }
        .onDisappear { viewModel.stopObserving() }
        .sheet(isPresented: $showTranscriptPeek) {
            if let assetId = analysisAssetId {
                TranscriptPeekView(
                    peekViewModel: TranscriptPeekViewModel(
                        analysisAssetId: assetId,
                        store: runtime.analysisStore
                    ),
                    currentTime: viewModel.currentTime
                )
                .presentationDetents([.medium, .large])
                .presentationDragIndicator(.hidden)
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
                    .foregroundStyle(AppColors.secondary)
            }
            .accessibilityLabel("Dismiss")
            .accessibilityHint("Closes the now playing screen")

            Spacer()

            VStack(spacing: 2) {
                Text("PLAYING FROM")
                    .font(AppTypography.sans(size: 10, weight: .semibold))
                    .foregroundStyle(AppColors.metadata)
                    .tracking(1.2)

                Text(viewModel.podcastTitle)
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.secondary)
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
                        .foregroundStyle(AppColors.secondary)
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
        RoundedRectangle(cornerRadius: CornerRadius.md)
            .fill(AppColors.surface)
            .aspectRatio(1, contentMode: .fit)
            .frame(maxWidth: 140, maxHeight: 140)
            .overlay(
                RoundedRectangle(cornerRadius: CornerRadius.md)
                    .stroke(AppColors.secondary.opacity(0.2), lineWidth: 1)
            )
            .themeShadow(AppShadow.card)
            .accessibilityLabel("Episode artwork")
    }

    // MARK: Titles

    var titleSection: some View {
        VStack(spacing: Spacing.xxs) {
            Text(viewModel.episodeTitle)
                .font(AppTypography.sans(size: 20, weight: .semibold))
                .foregroundStyle(AppColors.text)
                .lineLimit(2)
                .multilineTextAlignment(.center)

            Text(viewModel.podcastTitle)
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.secondary)
                .lineLimit(1)
        }
        .accessibilityElement(children: .combine)
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
                    .foregroundStyle(AppColors.metadata)
                    .accessibilityLabel("Elapsed: \(viewModel.elapsedFormatted)")

                Spacer()

                Text(viewModel.remainingFormatted)
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.metadata)
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

// MARK: - Transport Button

/// A single transport control with haptic feedback.
private struct TransportButton: View {
    let systemName: String
    let size: CGFloat
    var accessibilityText: String = ""
    let action: () -> Void

    var body: some View {
        Button {
            HapticManager.light()
            action()
        } label: {
            Image(systemName: systemName)
                .font(.system(size: size, weight: .medium))
                .foregroundStyle(AppColors.text)
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
