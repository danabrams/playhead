// NowPlayingView.swift
// Full-screen now-playing experience. Stamp-sized artwork, copper playhead,
// full-width timeline rail, transport controls, speed selector.
// "Quiet Instrument" aesthetic — precise, minimal chrome, typographic hierarchy.

import SwiftUI

// MARK: - NowPlayingView

struct NowPlayingView: View {

    @StateObject private var viewModel = NowPlayingViewModel()

    /// Ad segment ranges expressed as fractions of total duration (0...1).
    /// Fed from SkipOrchestrator in a future bead.
    var adSegments: [ClosedRange<Double>] = []

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
        }
        .onAppear { viewModel.startObserving() }
        .onDisappear { viewModel.stopObserving() }
    }
}

// MARK: - Subviews

private extension NowPlayingView {

    // MARK: Top Bar

    var topBar: some View {
        HStack {
            Button {
                // Dismiss — wired by parent
            } label: {
                Image(systemName: "chevron.down")
                    .font(.system(size: 18, weight: .medium))
                    .foregroundStyle(AppColors.secondary)
            }

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

            Spacer()

            // Balance the chevron
            Color.clear
                .frame(width: 18, height: 18)
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
    }

    // MARK: Timeline

    var timelineSection: some View {
        VStack(spacing: Spacing.xs) {
            TimelineRailView(
                progress: viewModel.progress,
                adSegments: adSegments,
                onSeek: { fraction in
                    let target = fraction * viewModel.duration
                    viewModel.seek(to: target)
                }
            )

            HStack {
                Text(viewModel.elapsedFormatted)
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.metadata)

                Spacer()

                Text(viewModel.remainingFormatted)
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.metadata)
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
                size: 28
            ) {
                viewModel.skipBackward()
            }

            // Play / Pause
            TransportButton(
                systemName: viewModel.isPlaying
                    ? "pause.fill"
                    : "play.fill",
                size: 42
            ) {
                viewModel.togglePlayPause()
            }

            // Skip forward 30s
            TransportButton(
                systemName: "goforward.30",
                size: 28
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
    let action: () -> Void

    var body: some View {
        Button {
            let generator = UIImpactFeedbackGenerator(style: .light)
            generator.impactOccurred()
            action()
        } label: {
            Image(systemName: systemName)
                .font(.system(size: size, weight: .medium))
                .foregroundStyle(AppColors.text)
                .frame(width: size + 20, height: size + 20)
                .contentShape(Rectangle())
        }
        .buttonStyle(TransportButtonStyle())
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
    NowPlayingView(
        adSegments: [0.15...0.22, 0.55...0.60]
    )
    .preferredColorScheme(.dark)
}
