// SpeedSelectorView.swift
// Compact speed selector. Tapping cycles through preset speeds;
// long-press opens a picker. Mono font for the numeric label.

import SwiftUI

struct SpeedSelectorView: View {

    let currentSpeed: Float
    let onSpeedChanged: (Float) -> Void

    /// Injected haptic player — defaults to `SystemHapticPlayer` in
    /// production, tests swap in a `RecordingHapticPlayer`. See
    /// `NowPlayingBar` for the canonical seam pattern.
    var hapticPlayer: any HapticPlaying = SystemHapticPlayer()

    private static let presets: [Float] = [1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 0.5, 0.75]

    @State private var showingPicker = false

    /// Tap handler for the cycle action. Factored out so unit tests can
    /// drive it directly with an injected `HapticPlaying` and assert the
    /// recorded event without rendering a real SwiftUI hierarchy.
    func handleCycleTap() {
        hapticPlayer.play(.control)
        cycleSpeed()
    }

    /// Long-press handler. Factored out for the same test-seam reason.
    func handleLongPress() {
        hapticPlayer.play(.menuOpen)
        showingPicker = true
    }

    var body: some View {
        Button {
            handleCycleTap()
        } label: {
            Text(Self.formatSpeed(currentSpeed))
                .font(AppTypography.mono(size: 14, weight: .medium))
                .foregroundStyle(AppColors.accent)
                .padding(.horizontal, Spacing.sm)
                .padding(.vertical, Spacing.xxs)
                .background(
                    RoundedRectangle(cornerRadius: CornerRadius.small)
                        .fill(AppColors.surface)
                )
                .overlay(
                    RoundedRectangle(cornerRadius: CornerRadius.small)
                        .stroke(AppColors.accent.opacity(0.3), lineWidth: 1)
                )
        }
        .buttonStyle(TransportScaleStyle())
        .accessibilityLabel("Playback speed: \(Self.formatSpeed(currentSpeed))")
        .accessibilityHint("Tap to cycle speed, long press for all options")
        .accessibilityValue(Self.formatSpeed(currentSpeed))
        .onLongPressGesture {
            handleLongPress()
        }
        .confirmationDialog("Playback Speed", isPresented: $showingPicker) {
            ForEach(Self.presets.sorted(), id: \.self) { speed in
                Button(Self.formatSpeed(speed)) {
                    onSpeedChanged(speed)
                }
            }
        }
    }

    // MARK: - Private

    private func cycleSpeed() {
        let sorted = Self.presets.sorted()
        if let index = sorted.firstIndex(where: { $0 > currentSpeed + 0.01 }) {
            onSpeedChanged(sorted[index])
        } else {
            onSpeedChanged(sorted.first ?? 1.0)
        }
    }

    private static func formatSpeed(_ speed: Float) -> String {
        if speed == Float(Int(speed)) {
            return String(format: "%.0fx", speed)
        }
        // Drop trailing zero for .25, .75 etc.
        let formatted = String(format: "%.2f", speed)
        let trimmed = formatted.replacingOccurrences(
            of: "0+$", with: "", options: .regularExpression
        )
        return "\(trimmed)x"
    }
}

/// Subtle scale on press, no bounce.
private struct TransportScaleStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? 0.94 : 1.0)
            .animation(Motion.quick, value: configuration.isPressed)
    }
}

// MARK: - Preview

#Preview("Speed Selector") {
    VStack(spacing: 24) {
        SpeedSelectorView(currentSpeed: 1.0, onSpeedChanged: { _ in })
        SpeedSelectorView(currentSpeed: 1.5, onSpeedChanged: { _ in })
        SpeedSelectorView(currentSpeed: 2.0, onSpeedChanged: { _ in })
    }
    .padding()
    .background(AppColors.background)
    .preferredColorScheme(.dark)
}
