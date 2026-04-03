// AdBannerView.swift
// Ad skip banner — slides in at bottom of Now Playing when an ad is skipped.
//
// Styled as a calm margin note, not an alert. Long horizontal proportions
// (cue sheet style). Ink background, Bone text, Copper accent on "Listen".
// Auto-dismisses after 8 seconds. Single banner lane with queue — rapid
// sequential skips are coalesced, never stacked.
//
// ┌─────────────────────────────────────────────────┐
// │  Skipped · Squarespace · "Build your website"   │
// │                          [Listen]    [Dismiss x] │
// └─────────────────────────────────────────────────┘

import SwiftUI

// MARK: - Banner Data

/// Data for a single ad skip banner notification.
struct AdSkipBannerItem: Identifiable, Equatable {
    let id: String
    /// Advertiser name, if known.
    let advertiser: String?
    /// Short product/tagline, if known.
    let product: String?
    /// Timestamp in episode seconds where the skipped ad started.
    let adStartTime: Double
    /// Timestamp in episode seconds where the skipped ad ended.
    let adEndTime: Double
}

// MARK: - Banner Queue (ViewModel)

/// Manages banner display queue. Coalesces adjacent skips into a single
/// banner. Ensures only one banner is visible at a time.
@MainActor
final class AdBannerQueue: ObservableObject {

    @Published private(set) var currentBanner: AdSkipBannerItem?

    /// Pending banners waiting to display.
    private var queue: [AdSkipBannerItem] = []

    /// Auto-dismiss timer handle.
    private var dismissTask: Task<Void, Never>?

    /// Duration before auto-dismiss.
    private static let autoDismissSeconds: TimeInterval = 8.0

    /// Maximum gap (seconds) between skipped ads to coalesce into one banner.
    private static let coalesceGap: TimeInterval = 10.0

    // MARK: - Public API

    /// Enqueue a new ad skip banner. If the skip is adjacent to the current
    /// or last queued item, coalesce instead of adding a new entry.
    func enqueue(_ item: AdSkipBannerItem) {
        // Try to coalesce with the most recent item (current or last in queue).
        if let last = queue.last, canCoalesce(last, item) {
            // Replace with the newer item (it has the broader time range).
            queue[queue.count - 1] = item
        } else if let current = currentBanner, queue.isEmpty, canCoalesce(current, item) {
            // Coalesce with the currently displayed banner — update in place.
            currentBanner = item
            restartAutoDismiss()
            return
        } else {
            queue.append(item)
        }

        // If nothing is showing, pop the next one.
        if currentBanner == nil {
            showNext()
        }
    }

    /// Dismiss the current banner (user tapped dismiss or auto-dismiss fired).
    func dismiss() {
        dismissTask?.cancel()
        dismissTask = nil
        currentBanner = nil

        // Show next queued banner after a brief pause so the exit animation
        // finishes before the next slide-in.
        if !queue.isEmpty {
            Task {
                try? await Task.sleep(for: .milliseconds(350))
                showNext()
            }
        }
    }

    // MARK: - Private

    private func showNext() {
        guard !queue.isEmpty else { return }
        currentBanner = queue.removeFirst()
        restartAutoDismiss()
    }

    private func restartAutoDismiss() {
        dismissTask?.cancel()
        dismissTask = Task {
            try? await Task.sleep(for: .seconds(Self.autoDismissSeconds))
            guard !Task.isCancelled else { return }
            dismiss()
        }
    }

    /// Two banners coalesce if they are close in time (adjacent/near-adjacent skips).
    private func canCoalesce(_ a: AdSkipBannerItem, _ b: AdSkipBannerItem) -> Bool {
        abs(a.adEndTime - b.adStartTime) <= Self.coalesceGap
    }
}

// MARK: - AdBannerView

/// The banner overlay. Positioned at the bottom of the Now Playing screen.
/// Slides in from below, slides out on dismiss.
struct AdBannerView: View {

    @ObservedObject var queue: AdBannerQueue

    /// Called when the user taps "Listen" to jump back to the skipped ad.
    var onListen: ((AdSkipBannerItem) -> Void)?

    var body: some View {
        VStack {
            Spacer()

            if let banner = queue.currentBanner {
                bannerCard(banner)
                    .transition(
                        .move(edge: .bottom)
                        .combined(with: .opacity)
                    )
                    .padding(.horizontal, Spacing.md)
                    .padding(.bottom, Spacing.sm)
            }
        }
        .animation(Motion.standard, value: queue.currentBanner?.id)
    }

    // MARK: - Banner Card

    @ViewBuilder
    private func bannerCard(_ item: AdSkipBannerItem) -> some View {
        VStack(alignment: .leading, spacing: Spacing.xs) {
            // Top line: "Skipped · Advertiser · Product"
            HStack(spacing: 0) {
                Text("Skipped")
                    .font(AppTypography.sans(size: 13, weight: .semibold))
                    .foregroundStyle(AppColors.accent)

                if let advertiser = item.advertiser {
                    Text(" · ")
                        .font(AppTypography.sans(size: 13, weight: .regular))
                        .foregroundStyle(boneText)
                    Text(advertiser)
                        .font(AppTypography.sans(size: 13, weight: .medium))
                        .foregroundStyle(boneText)
                }

                if let product = item.product {
                    Text(" · ")
                        .font(AppTypography.sans(size: 13, weight: .regular))
                        .foregroundStyle(boneText.opacity(0.6))
                    Text("\"\(product)\"")
                        .font(AppTypography.mono(size: 12, weight: .regular))
                        .foregroundStyle(boneText.opacity(0.7))
                        .lineLimit(1)
                }

                Spacer(minLength: Spacing.xs)
            }

            // Bottom line: actions
            HStack {
                Spacer()

                // Listen button — copper accent
                Button {
                    onListen?(item)
                } label: {
                    Text("Listen")
                        .font(AppTypography.sans(size: 13, weight: .semibold))
                        .foregroundStyle(AppColors.accent)
                        .padding(.horizontal, Spacing.sm)
                        .padding(.vertical, Spacing.xxs)
                        .background(
                            RoundedRectangle(cornerRadius: CornerRadius.sm)
                                .fill(AppColors.accent.opacity(0.12))
                        )
                }
                .buttonStyle(BannerButtonStyle())

                // Dismiss button
                Button {
                    queue.dismiss()
                } label: {
                    Image(systemName: "xmark")
                        .font(.system(size: 12, weight: .semibold))
                        .foregroundStyle(boneText.opacity(0.5))
                        .frame(width: 28, height: 28)
                        .contentShape(Rectangle())
                }
                .buttonStyle(BannerButtonStyle())
            }
        }
        .padding(.horizontal, Spacing.md)
        .padding(.vertical, Spacing.sm)
        .background(
            RoundedRectangle(cornerRadius: CornerRadius.md)
                .fill(Palette.ink)
                .themeShadow(AppShadow.elevated)
        )
        .onAppear {
            // Subtle haptic on banner appear.
            let generator = UIImpactFeedbackGenerator(style: .soft)
            generator.impactOccurred()
        }
    }

    // MARK: - Constants

    /// Bone text color for use on ink background (always light, regardless of mode).
    private var boneText: Color { Palette.bone }
}

// MARK: - Banner Button Style

/// Subtle scale-down on press — consistent with transport button style.
private struct BannerButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? 0.92 : 1.0)
            .animation(Motion.quick, value: configuration.isPressed)
    }
}

// MARK: - Preview

#Preview("Ad Banner") {
    ZStack {
        AppColors.background
            .ignoresSafeArea()

        AdBannerView(
            queue: {
                let q = AdBannerQueue()
                q.enqueue(AdSkipBannerItem(
                    id: "preview-1",
                    advertiser: "Squarespace",
                    product: "Build your website",
                    adStartTime: 120.0,
                    adEndTime: 180.0
                ))
                return q
            }()
        )
    }
    .preferredColorScheme(.dark)
}

#Preview("Ad Banner — No Metadata") {
    ZStack {
        AppColors.background
            .ignoresSafeArea()

        AdBannerView(
            queue: {
                let q = AdBannerQueue()
                q.enqueue(AdSkipBannerItem(
                    id: "preview-2",
                    advertiser: nil,
                    product: nil,
                    adStartTime: 300.0,
                    adEndTime: 345.0
                ))
                return q
            }()
        )
    }
    .preferredColorScheme(.dark)
}
