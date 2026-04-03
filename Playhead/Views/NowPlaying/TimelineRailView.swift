// TimelineRailView.swift
// Full-width timeline rail with copper playhead line and recessed ad segments.
// Supports drag-to-scrub with haptic feedback at segment boundaries.
// Ad segments render as subtle recessed charcoal blocks with inner shadow.
// Skip animation: playhead glides forward smoothly (no jump, no bounce).

import SwiftUI

struct TimelineRailView: View {

    /// Current playback progress as a fraction (0...1).
    let progress: Double
    /// Ad segments as fractional ranges of total duration.
    let adSegments: [ClosedRange<Double>]
    /// Called when the user scrubs to a new position (fraction 0...1).
    let onSeek: (Double) -> Void

    @State private var isDragging = false
    @State private var dragProgress: Double = 0

    /// Tracks whether the playhead is gliding through a skip (smooth animation).
    @State private var isGliding = false
    /// The previous progress value, used to detect skip-induced jumps.
    @State private var previousProgress: Double = 0

    private let railHeight: CGFloat = 4
    private let touchTargetHeight: CGFloat = 44

    /// Threshold for detecting a skip jump (vs normal playback advance).
    private let skipJumpThreshold: Double = 0.02

    private var effectiveProgress: Double {
        isDragging ? dragProgress : progress
    }

    var body: some View {
        GeometryReader { geo in
            let width = geo.size.width

            ZStack(alignment: .leading) {
                // MARK: Background Rail
                RoundedRectangle(cornerRadius: railHeight / 2)
                    .fill(AppColors.surface)
                    .frame(height: railHeight)

                // MARK: Ad Segments (recessed charcoal blocks)
                ForEach(Array(adSegments.enumerated()), id: \.offset) { _, segment in
                    let x = segment.lowerBound * width
                    let w = (segment.upperBound - segment.lowerBound) * width

                    adSegmentBlock(width: max(w, 2))
                        .offset(x: x)
                }

                // MARK: Elapsed Fill
                RoundedRectangle(cornerRadius: railHeight / 2)
                    .fill(AppColors.secondary.opacity(0.4))
                    .frame(width: max(effectiveProgress * width, 0), height: railHeight)

                // MARK: Copper Playhead Line
                Rectangle()
                    .fill(AppColors.accent)
                    .frame(width: 2, height: isDragging ? 18 : 12)
                    .offset(x: effectiveProgress * width - 1)
                    .animation(isDragging ? Motion.quick : glideAnimation, value: effectiveProgress)
                    .animation(Motion.quick, value: isDragging)
            }
            .frame(height: railHeight)
            .frame(maxHeight: .infinity)
            .contentShape(Rectangle().size(width: width, height: touchTargetHeight))
            .gesture(
                DragGesture(minimumDistance: 0)
                    .onChanged { value in
                        if !isDragging {
                            isDragging = true
                            let generator = UIImpactFeedbackGenerator(style: .light)
                            generator.impactOccurred()
                        }
                        let fraction = min(max(value.location.x / width, 0), 1)
                        dragProgress = fraction
                    }
                    .onEnded { value in
                        let fraction = min(max(value.location.x / width, 0), 1)
                        onSeek(fraction)
                        isDragging = false
                        let generator = UIImpactFeedbackGenerator(style: .light)
                        generator.impactOccurred()
                    }
            )
        }
        .frame(height: touchTargetHeight)
        .onChange(of: progress) { oldValue, newValue in
            let jump = newValue - oldValue
            if jump > skipJumpThreshold {
                // A skip just fired — animate the glide.
                isGliding = true
                // End the glide state after the animation settles.
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
                    isGliding = false
                }
            }
            previousProgress = newValue
        }
    }

    // MARK: - Skip Glide Animation

    /// When a skip fires, the playhead glides forward over 0.45s with easeInOut.
    /// Normal playback uses no animation (continuous updates).
    private var glideAnimation: Animation? {
        isGliding ? .easeInOut(duration: 0.45) : nil
    }

    // MARK: - Ad Segment Block

    /// Recessed charcoal block with subtle inner shadow.
    /// Darker than the rail background, muted — not aggressive.
    @ViewBuilder
    private func adSegmentBlock(width: CGFloat) -> some View {
        RoundedRectangle(cornerRadius: 1)
            .fill(Palette.charcoal)
            .frame(width: width, height: railHeight)
            .overlay(
                // Inner shadow: darkened top edge to sell the "recessed" illusion.
                RoundedRectangle(cornerRadius: 1)
                    .stroke(Color.black.opacity(0.3), lineWidth: 0.5)
            )
            .overlay(
                // Subtle top inset highlight for depth.
                VStack(spacing: 0) {
                    Color.black.opacity(0.15)
                        .frame(height: 1)
                    Spacer()
                }
            )
            .clipped()
    }
}

// MARK: - Preview

#Preview("Timeline Rail") {
    VStack(spacing: 32) {
        TimelineRailView(
            progress: 0.35,
            adSegments: [0.15...0.22, 0.55...0.60],
            onSeek: { _ in }
        )

        TimelineRailView(
            progress: 0.7,
            adSegments: [],
            onSeek: { _ in }
        )
    }
    .padding()
    .background(AppColors.background)
    .preferredColorScheme(.dark)
}
