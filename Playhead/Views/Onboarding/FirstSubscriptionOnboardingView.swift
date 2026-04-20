// FirstSubscriptionOnboardingView.swift
// One-screen "what Playhead does" moment shown the first time the user
// subscribes to a podcast (NOT at first app launch) plus the first-✓
// tooltip shown the first time a ready-to-skip badge appears on an
// episode the user opens.
//
// Spec: bd playhead-rw49 / UI design §G in
// docs/plans/2026-04-16-podcast-bg-ui-design.md.
//
// Persistence:
// - `OnboardingFlags.firstSubscriptionOnboardingSeenKey` — set when the
//   user taps "Got it".
// - `OnboardingFlags.firstCheckmarkTooltipSeenKey` — set when the user
//   dismisses the tooltip.
// Both flags live in UserDefaults so they persist across app relaunch.
// Copy lives in `OnboardingCopy` for snapshot tests to assert verbatim.

import SwiftUI

// MARK: - Verbatim copy

/// Verbatim strings for the first-subscription onboarding screen and the
/// first-✓ tooltip. Any change here is a product decision — snapshot
/// tests (FirstSubscriptionOnboardingCopyTests) pin these exactly.
enum OnboardingCopy {

    /// Body copy for the first-subscription onboarding screen. The
    /// "[ Got it ]" in the design-doc source is bracketed to indicate a
    /// button; the body copy itself does not contain the brackets.
    static let firstSubscriptionBody: String =
        "Playhead skips ads for you. Tap Download on any episode. We'll fetch it and find the ads in the background — when it's ready, you'll see a ✓ and we'll skip them automatically while you listen. All processing stays on your device."

    /// Label on the single dismiss button. Source: UI design §G button
    /// sketch `[ Got it ]`.
    static let firstSubscriptionButton: String = "Got it"

    /// Copy for the first-✓ tooltip. Dismisses on tap; never reappears.
    static let firstCheckmarkTooltip: String =
        "✓ means we've found ads to skip. Tap play and we'll handle the rest."
}

// MARK: - Persistence

/// UserDefaults-backed flags that gate the two one-shot surfaces.
/// Keys are public so tests can reset / inspect them against an injected
/// `UserDefaults` suite.
enum OnboardingFlags {

    /// Set to `true` after the user taps "Got it" on the
    /// first-subscription onboarding screen.
    static let firstSubscriptionOnboardingSeenKey = "firstSubscriptionOnboardingSeen"

    /// Set to `true` after the user dismisses the first-✓ tooltip.
    static let firstCheckmarkTooltipSeenKey = "firstCheckmarkTooltipSeen"

    /// Returns whether the first-subscription onboarding has already been
    /// acknowledged. Defaults to `false` on fresh installs.
    static func hasSeenFirstSubscriptionOnboarding(
        _ defaults: UserDefaults = .standard
    ) -> Bool {
        defaults.bool(forKey: firstSubscriptionOnboardingSeenKey)
    }

    /// Returns whether the first-✓ tooltip has already been dismissed.
    /// Defaults to `false` on fresh installs.
    static func hasSeenFirstCheckmarkTooltip(
        _ defaults: UserDefaults = .standard
    ) -> Bool {
        defaults.bool(forKey: firstCheckmarkTooltipSeenKey)
    }

    /// Mark the first-subscription onboarding as seen.
    static func markFirstSubscriptionOnboardingSeen(
        _ defaults: UserDefaults = .standard
    ) {
        defaults.set(true, forKey: firstSubscriptionOnboardingSeenKey)
    }

    /// Mark the first-✓ tooltip as dismissed.
    static func markFirstCheckmarkTooltipSeen(
        _ defaults: UserDefaults = .standard
    ) {
        defaults.set(true, forKey: firstCheckmarkTooltipSeenKey)
    }
}

// MARK: - Gate Logic (pure)

/// Pure, side-effect-free gate functions that decide whether each
/// one-shot onboarding surface should be presented. Extracted from the
/// SwiftUI call-sites (`RootView.evaluateFirstSubscriptionOnboarding`
/// and `EpisodeListView.evaluateFirstCheckmarkTooltip`) so regressions
/// in the trigger conditions can be caught by unit tests without a
/// SwiftUI harness. Keep these functions free of framework imports and
/// free of `@AppStorage` / `UserDefaults` access — inputs only.
enum OnboardingGating {

    /// First-subscription onboarding fires iff the user has completed
    /// first-launch onboarding, has not yet tapped "Got it", and has at
    /// least one podcast subscription.
    static func shouldPresentFirstSubscriptionOnboarding(
        hasCompletedOnboarding: Bool,
        hasSeenFirstSubscriptionOnboarding: Bool,
        podcastCount: Int
    ) -> Bool {
        hasCompletedOnboarding
            && !hasSeenFirstSubscriptionOnboarding
            && podcastCount > 0
    }

    /// First-✓ tooltip fires iff the user has not yet dismissed it and
    /// at least one episode in the current list has a ready analysis
    /// (✓ badge visible).
    static func shouldPresentFirstCheckmarkTooltip(
        hasSeenFirstCheckmarkTooltip: Bool,
        anyEpisodeHasAnalysis: Bool
    ) -> Bool {
        !hasSeenFirstCheckmarkTooltip && anyEpisodeHasAnalysis
    }
}

// MARK: - First Subscription Onboarding Screen

/// One-screen modal shown the first time a podcast subscription is
/// added. Presents the skip-for-you promise and a single `Got it` CTA
/// that permanently dismisses the screen via `OnboardingFlags`.
struct FirstSubscriptionOnboardingView: View {

    /// Called after "Got it" is tapped and the flag has been persisted.
    let onDismiss: () -> Void

    @AppStorage(OnboardingFlags.firstSubscriptionOnboardingSeenKey)
    private var hasSeen: Bool = false

    @State private var appeared = false

    var body: some View {
        ZStack {
            AppColors.background
                .ignoresSafeArea()

            VStack(spacing: Spacing.xl) {
                Spacer()

                VStack(spacing: Spacing.lg) {
                    // Small hero: a standalone ✓ in the accent color so
                    // the user forms a visual association with the badge
                    // language used below.
                    Image(systemName: "checkmark.circle")
                        .font(.system(size: 56, weight: .light))
                        .foregroundStyle(AppColors.accent)
                        .accessibilityHidden(true)

                    Text(OnboardingCopy.firstSubscriptionBody)
                        .font(AppTypography.body)
                        .foregroundStyle(AppColors.textPrimary)
                        .multilineTextAlignment(.center)
                        .fixedSize(horizontal: false, vertical: true)
                        .padding(.horizontal, Spacing.lg)
                        .accessibilityIdentifier("firstSubscriptionOnboarding.body")
                }
                .opacity(appeared ? 1 : 0)
                .offset(y: appeared ? 0 : 12)

                Spacer()

                Button {
                    dismiss()
                } label: {
                    Text(OnboardingCopy.firstSubscriptionButton)
                        .font(AppTypography.sans(size: 16, weight: .semibold))
                        .foregroundStyle(Palette.bone)
                        .frame(maxWidth: .infinity)
                        .frame(height: 52)
                        .background(
                            RoundedRectangle(cornerRadius: CornerRadius.medium)
                                .fill(AppColors.accent)
                        )
                }
                .accessibilityIdentifier("firstSubscriptionOnboarding.gotItButton")
                .accessibilityLabel(OnboardingCopy.firstSubscriptionButton)
                .padding(.horizontal, Spacing.xl)
                .padding(.bottom, Spacing.xxl)
                .opacity(appeared ? 1 : 0)
            }
        }
        .onAppear {
            withAnimation(.easeOut(duration: 0.4)) {
                appeared = true
            }
        }
    }

    private func dismiss() {
        hasSeen = true
        onDismiss()
    }
}

// MARK: - First ✓ Tooltip

/// A lightweight overlay bubble that introduces the ✓ readiness badge
/// the first time it appears on an opened episode list. Tap anywhere on
/// the bubble (or the scrim behind it) to dismiss; never reappears.
struct FirstCheckmarkTooltipView: View {

    /// Fired once the user taps to dismiss.
    let onDismiss: () -> Void

    var body: some View {
        ZStack {
            // Dimmed scrim — tap anywhere to dismiss.
            Color.black.opacity(0.35)
                .ignoresSafeArea()
                .contentShape(Rectangle())
                .onTapGesture {
                    onDismiss()
                }
                .accessibilityHidden(true)

            VStack {
                Spacer()

                Text(OnboardingCopy.firstCheckmarkTooltip)
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textPrimary)
                    .multilineTextAlignment(.center)
                    .fixedSize(horizontal: false, vertical: true)
                    .padding(.horizontal, Spacing.lg)
                    .padding(.vertical, Spacing.md)
                    .background(
                        RoundedRectangle(cornerRadius: CornerRadius.medium)
                            .fill(AppColors.surfaceElevated)
                    )
                    .overlay(
                        RoundedRectangle(cornerRadius: CornerRadius.medium)
                            .stroke(AppColors.textSecondary.opacity(0.2), lineWidth: 1)
                    )
                    .themeShadow(AppShadow.elevated)
                    .padding(.horizontal, Spacing.xl)
                    .padding(.bottom, Spacing.xxl)
                    .onTapGesture {
                        onDismiss()
                    }
                    .accessibilityIdentifier("firstCheckmarkTooltip.body")
                    .accessibilityAddTraits(.isButton)
                    .accessibilityHint("Dismiss")
            }
        }
        .transition(.opacity)
    }
}

// MARK: - Previews

#Preview("First Subscription Onboarding") {
    FirstSubscriptionOnboardingView(onDismiss: {})
        .preferredColorScheme(.dark)
}

#Preview("First ✓ Tooltip") {
    ZStack {
        AppColors.background.ignoresSafeArea()
        FirstCheckmarkTooltipView(onDismiss: {})
    }
    .preferredColorScheme(.dark)
}
