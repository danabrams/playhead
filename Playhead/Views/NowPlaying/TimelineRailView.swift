// TimelineRailView.swift
// Full-width timeline rail with copper playhead line and recessed ad segments.
// Supports drag-to-scrub with haptic feedback at segment boundaries.

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

    private let railHeight: CGFloat = 4
    private let touchTargetHeight: CGFloat = 44

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

                // MARK: Ad Segments (recessed blocks)
                ForEach(Array(adSegments.enumerated()), id: \.offset) { _, segment in
                    let x = segment.lowerBound * width
                    let w = (segment.upperBound - segment.lowerBound) * width

                    RoundedRectangle(cornerRadius: 1)
                        .fill(AppColors.secondary.opacity(0.25))
                        .frame(width: max(w, 2), height: railHeight)
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
