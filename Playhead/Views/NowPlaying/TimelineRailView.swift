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

    // Phase 7.2: tap-to-explain for ad segments.
    // Called with the fractional index of the tapped segment when the user
    // taps inside an ad block. The caller maps the index to a DecodedSpan
    // and presents AdRegionPopover.
    // Phase 7.3 NOTE: full AdRegionPopover wiring requires the caller to
    // vend a DecodedSpan for the tapped segment (by matching adSegments[index]
    // to the corresponding DecodedSpan via NowPlayingViewModel). Until that
    // ViewModel method exists, the caller should present a fallback or skip.
    var onAdSegmentTap: ((Int) -> Void)?

    /// Injected haptic player — defaults to `SystemHapticPlayer` in
    /// production, tests swap in a `RecordingHapticPlayer`. See
    /// `NowPlayingBar` for the canonical seam pattern.
    var hapticPlayer: any HapticPlaying = SystemHapticPlayer()

    @State private var isDragging = false
    @State private var dragProgress: Double = 0

    /// Tracks whether the playhead is gliding through a skip (smooth animation).
    @State private var isGliding = false
    /// The previous progress value, used to detect skip-induced jumps.
    @State private var previousProgress: Double = 0
    /// Task for resetting the glide state after animation settles.
    @State private var glideResetTask: Task<Void, Never>?

    private let railHeight: CGFloat = 4
    private let touchTargetHeight: CGFloat = 44

    /// Threshold for detecting a skip jump (vs normal playback advance).
    private let skipJumpThreshold: Double = 0.02

    private var effectiveProgress: Double {
        isDragging ? dragProgress : progress
    }

    /// Scrub-begin handler. Factored out so unit tests can drive it
    /// directly with an injected `HapticPlaying` and assert the recorded
    /// event without constructing a real `DragGesture`.
    func handleScrubBegin() {
        hapticPlayer.play(.control)
    }

    /// Scrub-end handler. Factored out for the same test-seam reason.
    /// Calls `onSeek` with the final fraction and fires the confirm haptic.
    /// Matches the original gesture ordering: seek first, then haptic.
    func handleScrubEnd(fraction: Double) {
        onSeek(fraction)
        hapticPlayer.play(.control)
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
                ForEach(Array(adSegments.enumerated()), id: \.offset) { index, segment in
                    let x = segment.lowerBound * width
                    let w = (segment.upperBound - segment.lowerBound) * width

                    adSegmentBlock(width: max(w, 2))
                        .offset(x: x)
                        // Phase 7.2: tap gesture on each ad block to surface AdRegionPopover.
                        // Uses a tap target at least 44pt tall (inherited from parent frame).
                        .onTapGesture {
                            onAdSegmentTap?(index)
                        }
                        .accessibilityLabel("Ad segment \(index + 1) of \(adSegments.count)")
                        .accessibilityHint("Tap to see details about this ad segment")
                        .accessibilityAddTraits(.isButton)
                }

                // MARK: Elapsed Fill
                RoundedRectangle(cornerRadius: railHeight / 2)
                    .fill(AppColors.textSecondary.opacity(0.4))
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
                            handleScrubBegin()
                        }
                        let fraction = min(max(value.location.x / width, 0), 1)
                        dragProgress = fraction
                    }
                    .onEnded { value in
                        let fraction = min(max(value.location.x / width, 0), 1)
                        isDragging = false
                        handleScrubEnd(fraction: fraction)
                    }
            )
        }
        .frame(height: touchTargetHeight)
        .accessibilityElement()
        .accessibilityLabel("Timeline scrubber")
        .accessibilityValue("\(Int(effectiveProgress * 100)) percent")
        .accessibilityAdjustableAction { direction in
            switch direction {
            case .increment:
                // Advance by 5% of total duration
                let newProgress = min(effectiveProgress + 0.05, 1.0)
                onSeek(newProgress)
            case .decrement:
                // Rewind by 5% of total duration
                let newProgress = max(effectiveProgress - 0.05, 0.0)
                onSeek(newProgress)
            @unknown default:
                break
            }
        }
        .onChange(of: progress) { oldValue, newValue in
            let jump = newValue - oldValue
            if jump > skipJumpThreshold {
                // A skip just fired — animate the glide.
                isGliding = true
                // End the glide state after the animation settles.
                glideResetTask?.cancel()
                glideResetTask = Task {
                    try? await Task.sleep(for: .milliseconds(500))
                    guard !Task.isCancelled else { return }
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
