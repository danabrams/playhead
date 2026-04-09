// NowPlayingBar.swift
// Persistent mini-player bar shown at the bottom of non-player screens.
//
// Thin bar with playhead progress line, episode title, and play/pause toggle.
// Tap to expand to full NowPlayingView. Sits above the tab bar, respects
// safe area, and uses the same PlaybackService + design tokens as the full
// Now Playing screen.
//
// "Quiet Instrument" aesthetic: copper playhead line, minimal chrome,
// precise animation, no bounce.

import SwiftUI

// MARK: - NowPlayingBar

struct NowPlayingBar: View {

    var viewModel: NowPlayingViewModel

    /// Called when the user taps the bar to expand to full Now Playing.
    var onTap: () -> Void = {}

    /// Injected haptic player — defaults to `SystemHapticPlayer` in production,
    /// tests swap in a `RecordingHapticPlayer` to assert the expected event.
    /// An `@Environment` key with the same default is also provided by the
    /// Design module (see `HapticManager.swift`) so call sites can override
    /// via `.environment(\.hapticPlayer, ...)` if preferred; the init-param
    /// form here is the canonical test seam because SwiftUI `@Environment`
    /// values only resolve inside a live view hierarchy.
    var hapticPlayer: any HapticPlaying = SystemHapticPlayer()

    /// Height of the mini-player bar content (excluding the progress line).
    private static let barHeight: CGFloat = 56

    /// Height of the copper progress line at the top of the bar.
    private static let progressLineHeight: CGFloat = 2

    /// Play/pause tap handler. Factored out so unit tests can drive it
    /// directly with an injected `HapticPlaying` and assert the recorded
    /// event without rendering a real SwiftUI hierarchy.
    func handlePlayPauseTap() {
        hapticPlayer.play(.control)
        viewModel.togglePlayPause()
    }

    var body: some View {
        VStack(spacing: 0) {
            // Copper playhead progress line
            GeometryReader { geo in
                Rectangle()
                    .fill(AppColors.accent)
                    .frame(
                        width: geo.size.width * viewModel.progress,
                        height: Self.progressLineHeight
                    )
            }
            .frame(height: Self.progressLineHeight)
            .accessibilityValue("Playback progress: \(Int(viewModel.progress * 100)) percent")

            // Bar content
            HStack(spacing: Spacing.sm) {
                // Artwork
                RoundedRectangle(cornerRadius: CornerRadius.sm)
                    .fill(AppColors.surface)
                    .frame(width: 40, height: 40)
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
                                        Image(systemName: "mic.fill")
                                            .font(.caption)
                                            .foregroundStyle(AppColors.secondary.opacity(0.4))
                                    case .empty:
                                        ProgressView()
                                            .tint(AppColors.secondary)
                                    @unknown default:
                                        Image(systemName: "mic.fill")
                                            .font(.caption)
                                            .foregroundStyle(AppColors.secondary.opacity(0.4))
                                    }
                                }
                            } else {
                                Image(systemName: "mic.fill")
                                    .font(.caption)
                                    .foregroundStyle(AppColors.secondary.opacity(0.4))
                            }
                        }
                        .clipShape(RoundedRectangle(cornerRadius: CornerRadius.sm))
                    )
                    .overlay(
                        RoundedRectangle(cornerRadius: CornerRadius.sm)
                            .stroke(AppColors.secondary.opacity(0.2), lineWidth: 0.5)
                    )
                    .accessibilityHidden(true)

                // Title + subtitle
                VStack(alignment: .leading, spacing: 2) {
                    Text(viewModel.episodeTitle)
                        .font(AppTypography.sans(size: 14, weight: .medium))
                        .foregroundStyle(AppColors.text)
                        .lineLimit(1)

                    if !viewModel.podcastTitle.isEmpty {
                        Text(viewModel.podcastTitle)
                            .font(AppTypography.caption)
                            .foregroundStyle(AppColors.secondary)
                            .lineLimit(1)
                    }
                }

                Spacer()

                // Play / Pause button
                Button {
                    handlePlayPauseTap()
                } label: {
                    Image(systemName: viewModel.isPlaying ? "pause.fill" : "play.fill")
                        .font(.system(size: 20, weight: .medium))
                        .foregroundStyle(AppColors.text)
                        .frame(width: 44, height: 44)
                        .contentShape(Rectangle())
                }
                .buttonStyle(MiniPlayerButtonStyle())
                .accessibilityLabel(viewModel.isPlaying ? "Pause" : "Play")
            }
            .padding(.horizontal, Spacing.md)
            .frame(height: Self.barHeight)
        }
        .background(
            AppColors.surface
                .shadow(color: .black.opacity(0.1), radius: 8, x: 0, y: -2)
        )
        .contentShape(Rectangle())
        .onTapGesture {
            onTap()
        }
    }
}

// MARK: - Button Style

/// Subtle scale-down on press — matches the transport button style from NowPlayingView.
private struct MiniPlayerButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? 0.9 : 1.0)
            .animation(Motion.quick, value: configuration.isPressed)
    }
}

// MARK: - Preview

#Preview("Mini Player Bar") {
    let runtime = PlayheadRuntime(isPreviewRuntime: true)
    ZStack {
        AppColors.background
            .ignoresSafeArea()

        VStack {
            Spacer()
            NowPlayingBar(viewModel: NowPlayingViewModel(runtime: runtime))
        }
    }
    .preferredColorScheme(.dark)
}
