// OnboardingView.swift
// First-launch experience. 3-screen flow:
//   1. Welcome — copper playhead-line motif with a subtle horizontal
//      sweep that loops while the screen is visible.
//   2. Value prop — verbatim single-line statement of the product
//      promise.
//   3. Search prompt — invites the user to find their first podcast,
//      then dismisses onboarding and lands the user on the Browse tab.
//
// Bead: playhead-1v8.
//
// Spec adjustments (vs. the original bead text):
// - The model-download screen has been dropped. playhead-c6r removed
//   external model manifests; on-device Foundation Models are
//   system-managed, so onboarding has nothing to download.
// - Every screen has a small "Skip" link in the top-right corner. This
//   is the escape hatch for returning users (re-installs) and for
//   users who don't want a tour.
//
// Persistence:
//   `@AppStorage("hasCompletedOnboarding")` — single boolean, set to
//   true when the user finishes the flow OR taps Skip on any screen.
//   Once true, the flow never reappears (matches the existing pattern
//   used by `RootView` in `PlayheadApp.swift`).
//
// Tab handoff:
//   The "Get started" CTA on the search-prompt screen requests the
//   Browse tab be selected when ContentView mounts. Implemented as a
//   single `@AppStorage` slot consumed once by ContentView; see
//   `OnboardingFlags.requestedInitialTabKey`.

import SwiftUI

// MARK: - Verbatim copy

/// Verbatim user-facing strings for the first-launch onboarding flow.
/// These are pinned by snapshot tests in `OnboardingFlowTests` so that
/// any intentional copy change requires editing both the source and the
/// tests in the same commit.
enum OnboardingFlowCopy {

    /// Wordmark on the welcome screen.
    static let welcomeWordmark = "Playhead"

    /// Value-prop body. From the bead text, verbatim.
    static let valuePropBody = "Your podcasts, without the ads. All on-device, all private."

    /// Search-prompt headline.
    static let searchHeadline = "Find your first podcast."

    /// Search-prompt body.
    static let searchBody = "We don't track what you listen to. Your library lives here, not in the cloud."

    /// Primary CTAs.
    static let welcomeContinueButton = "Get Started"
    static let valuePropContinueButton = "Continue"
    static let searchGetStartedButton = "Get Started"

    /// Top-right escape hatch. Same label on every screen.
    static let skipButton = "Skip"
}

// MARK: - Step model

/// The three onboarding screens, in flow order.
enum OnboardingStep: Int, CaseIterable, Equatable {
    case welcome
    case valueProp
    case searchPrompt

    /// The next step in the flow, or `nil` if this is the terminal step.
    /// `searchPrompt`'s next-step is `nil` because tapping "Get Started"
    /// dismisses onboarding (it does not advance to a fourth screen).
    var next: OnboardingStep? {
        switch self {
        case .welcome: return .valueProp
        case .valueProp: return .searchPrompt
        case .searchPrompt: return nil
        }
    }
}

// MARK: - View Model

/// Pure action-handler for the onboarding flow. Drives `currentStep`,
/// the `hasCompletedOnboarding` write, and the post-flow tab hint.
/// Extracted from the SwiftUI view so unit tests can exercise the
/// handlers without driving SwiftUI gestures.
@MainActor
@Observable
final class OnboardingFlowViewModel {

    /// The currently visible step.
    private(set) var currentStep: OnboardingStep = .welcome

    /// Backing UserDefaults — tests inject a private suite so the
    /// global app state is not polluted.
    private let defaults: UserDefaults

    init(defaults: UserDefaults = .standard, initialStep: OnboardingStep = .welcome) {
        self.defaults = defaults
        self.currentStep = initialStep
    }

    /// Advances from the current step to the next, or finishes the flow
    /// if there is no next step.
    func continueTapped() {
        if let next = currentStep.next {
            currentStep = next
        } else {
            finish(initialTab: .browse)
        }
    }

    /// Skip from any screen. Marks onboarding complete WITHOUT setting
    /// a tab hint — returning users land on the default (Library) tab.
    func skipTapped() {
        finish(initialTab: nil)
    }

    /// Search-prompt's primary CTA. Marks onboarding complete and
    /// requests the Browse tab.
    func getStartedTapped() {
        finish(initialTab: .browse)
    }

    /// Direct setter used only by tests that need to start mid-flow.
    func setStep(_ step: OnboardingStep) {
        currentStep = step
    }

    private func finish(initialTab: OnboardingInitialTab?) {
        if let initialTab {
            defaults.set(initialTab.rawValue, forKey: OnboardingFlags.requestedInitialTabKey)
        }
        defaults.set(true, forKey: OnboardingFlags.hasCompletedOnboardingKey)
    }
}

// MARK: - Persistence keys (extension to existing OnboardingFlags)

extension OnboardingFlags {

    /// Mirrors the `@AppStorage("hasCompletedOnboarding")` key already in
    /// use by `RootView` and `OnboardingView`. Defined here so tests can
    /// reset it against a private UserDefaults suite without
    /// hard-coding the literal in two places.
    static let hasCompletedOnboardingKey = "hasCompletedOnboarding"

    /// One-shot tab hint set when the user finishes onboarding via the
    /// search-prompt screen's "Get Started" CTA. ContentView reads this
    /// once on first appearance and clears it, so the hint never affects
    /// a second app launch.
    static let requestedInitialTabKey = "onboardingRequestedInitialTab"
}

// MARK: - Initial tab hint

/// The set of tabs that onboarding can pre-select on first run.
/// The raw values are persisted in UserDefaults; renaming a case is a
/// breaking change.
enum OnboardingInitialTab: String, Equatable {
    case library
    case browse
}

// MARK: - OnboardingView (root)

struct OnboardingView: View {

    @AppStorage(OnboardingFlags.hasCompletedOnboardingKey)
    private var hasCompletedOnboarding = false

    @State private var viewModel = OnboardingFlowViewModel()

    var body: some View {
        ZStack {
            AppColors.background
                .ignoresSafeArea()

            switch viewModel.currentStep {
            case .welcome:
                WelcomeStepView(
                    onContinue: { viewModel.continueTapped() },
                    onSkip: { viewModel.skipTapped() }
                )
                .transition(.opacity)
            case .valueProp:
                ValuePropStepView(
                    onContinue: { viewModel.continueTapped() },
                    onSkip: { viewModel.skipTapped() }
                )
                .transition(.opacity)
            case .searchPrompt:
                SearchPromptStepView(
                    onGetStarted: { viewModel.getStartedTapped() },
                    onSkip: { viewModel.skipTapped() }
                )
                .transition(.opacity)
            }
        }
        .animation(Motion.standard, value: viewModel.currentStep)
    }
}

// MARK: - Top-right Skip button (shared)

/// Muted, top-right escape hatch shared by all three screens.
/// Lives in a `safeAreaInset(edge: .top)`-style position so it never
/// crowds the screen's primary content.
private struct SkipBar: View {

    let onSkip: () -> Void

    var body: some View {
        HStack {
            Spacer()
            Button(action: onSkip) {
                Text(OnboardingFlowCopy.skipButton)
                    .font(AppTypography.sans(size: 15, weight: .medium))
                    .foregroundStyle(AppColors.textSecondary)
                    .padding(.horizontal, Spacing.md)
                    .padding(.vertical, Spacing.sm)
                    .contentShape(Rectangle())
            }
            .accessibilityLabel(OnboardingFlowCopy.skipButton)
            .accessibilityHint("Skips the introduction and goes straight to the app.")
            .accessibilityIdentifier("onboarding.skipButton")
        }
        .padding(.top, Spacing.xs)
        .padding(.trailing, Spacing.sm)
    }
}

// MARK: - Welcome screen

/// Welcome screen: a copper playhead line anchored at the leading 1/3
/// of the screen, with a subtle horizontal sweep that loops while the
/// screen is visible. The "Playhead" wordmark appears below the line.
private struct WelcomeStepView: View {

    let onContinue: () -> Void
    let onSkip: () -> Void

    /// Drives the looping sweep offset. Goes 0 -> 1 over `loopDuration`
    /// seconds, then resets and repeats forever.
    @State private var sweepPhase: CGFloat = 0
    @State private var contentVisible = false

    /// Sweep loop in seconds. 3.0s matches the bead spec ("~3s loop").
    private let loopDuration: Double = 3.0

    /// Width of the line as a fraction of the available width.
    /// Subtle: the sweep travels less than half the line's own length,
    /// so the motion reads as a slow shimmer rather than a marquee.
    private let lineWidthFraction: CGFloat = 0.42

    var body: some View {
        GeometryReader { geometry in
            let availableWidth = geometry.size.width
            let lineWidth = availableWidth * lineWidthFraction
            // Anchor: leading 1/3 of the available width.
            let anchorX = availableWidth * (1.0 / 3.0)

            VStack(spacing: 0) {
                SkipBar(onSkip: onSkip)
                Spacer()

                VStack(spacing: Spacing.lg) {
                    sweepingLine(width: lineWidth, anchorX: anchorX)

                    Text(OnboardingFlowCopy.welcomeWordmark)
                        .font(AppTypography.sans(size: 36, weight: .semibold))
                        .foregroundStyle(AppColors.textPrimary)
                        .accessibilityAddTraits(.isHeader)
                }
                .opacity(contentVisible ? 1 : 0)
                .offset(y: contentVisible ? 0 : 12)

                Spacer()

                OnboardingPrimaryButton(
                    label: OnboardingFlowCopy.welcomeContinueButton,
                    action: onContinue
                )
                .padding(.horizontal, Spacing.xl)
                .padding(.bottom, Spacing.xxl)
                .opacity(contentVisible ? 1 : 0)
            }
        }
        .onAppear {
            withAnimation(.easeOut(duration: 0.5)) {
                contentVisible = true
            }
            // Looping sweep: drive a normalized 0->1 phase forever; the
            // shape uses it to compute a horizontal offset. easeInOut
            // with `repeatForever(autoreverses: false)` yields a quiet,
            // continuous left-to-right shimmer.
            withAnimation(
                .easeInOut(duration: loopDuration)
                    .repeatForever(autoreverses: false)
            ) {
                sweepPhase = 1
            }
        }
    }

    /// The looping sweep: a copper capsule fixed at `anchorX`, masked
    /// by a horizontally-translating linear gradient so it reads as a
    /// quiet shimmer travelling along the line.
    @ViewBuilder
    private func sweepingLine(width: CGFloat, anchorX: CGFloat) -> some View {
        let lineHeight: CGFloat = 3
        // Maximum sweep travel: the gradient's bright band slides from
        // just outside the leading edge to just outside the trailing
        // edge of the line, so the shimmer enters and exits cleanly.
        let travel = width
        let highlightOffset = (sweepPhase - 0.5) * travel * 2

        ZStack(alignment: .leading) {
            // Base copper line, full opacity, anchored.
            Capsule()
                .fill(AppColors.accent)
                .frame(width: width, height: lineHeight)

            // Sweep highlight: a bone-tinted band that slides across
            // the line. Soft at the edges so it never reads as a
            // hard-edged dot.
            Capsule()
                .fill(
                    LinearGradient(
                        gradient: Gradient(stops: [
                            .init(color: Palette.bone.opacity(0.0), location: 0.0),
                            .init(color: Palette.bone.opacity(0.55), location: 0.5),
                            .init(color: Palette.bone.opacity(0.0), location: 1.0),
                        ]),
                        startPoint: .leading,
                        endPoint: .trailing
                    )
                )
                .frame(width: width * 0.35, height: lineHeight)
                .offset(x: highlightOffset)
                .blendMode(.plusLighter)
                .mask(
                    // Confine the highlight to the underlying line.
                    Capsule().frame(width: width, height: lineHeight)
                )
        }
        .frame(width: width, height: lineHeight)
        .offset(x: anchorX - width / 2)
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityHidden(true)
    }
}

// MARK: - Value prop screen

/// Single-screen value proposition. One line, no paragraph flourishes,
/// a single primary "Continue" CTA.
private struct ValuePropStepView: View {

    let onContinue: () -> Void
    let onSkip: () -> Void

    @State private var appeared = false

    var body: some View {
        VStack(spacing: 0) {
            SkipBar(onSkip: onSkip)
            Spacer()

            Text(OnboardingFlowCopy.valuePropBody)
                .font(AppTypography.sans(size: 22, weight: .regular))
                .foregroundStyle(AppColors.textPrimary)
                .multilineTextAlignment(.center)
                .fixedSize(horizontal: false, vertical: true)
                .padding(.horizontal, Spacing.xl)
                .opacity(appeared ? 1 : 0)
                .offset(y: appeared ? 0 : 12)
                .accessibilityAddTraits(.isHeader)
                .accessibilityIdentifier("onboarding.valueProp.body")

            Spacer()

            OnboardingPrimaryButton(
                label: OnboardingFlowCopy.valuePropContinueButton,
                action: onContinue
            )
            .padding(.horizontal, Spacing.xl)
            .padding(.bottom, Spacing.xxl)
            .opacity(appeared ? 1 : 0)
        }
        .onAppear {
            withAnimation(.easeOut(duration: 0.4)) {
                appeared = true
            }
        }
    }
}

// MARK: - Search-prompt screen

/// Third screen: the user is invited to find their first podcast.
/// Tapping "Get Started" dismisses onboarding and asks ContentView to
/// land on the Browse tab.
private struct SearchPromptStepView: View {

    let onGetStarted: () -> Void
    let onSkip: () -> Void

    @State private var appeared = false

    var body: some View {
        VStack(spacing: 0) {
            SkipBar(onSkip: onSkip)
            Spacer()

            VStack(spacing: Spacing.md) {
                Text(OnboardingFlowCopy.searchHeadline)
                    .font(AppTypography.sans(size: 28, weight: .semibold))
                    .foregroundStyle(AppColors.textPrimary)
                    .multilineTextAlignment(.center)
                    .accessibilityAddTraits(.isHeader)
                    .accessibilityIdentifier("onboarding.searchPrompt.headline")

                Text(OnboardingFlowCopy.searchBody)
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textSecondary)
                    .multilineTextAlignment(.center)
                    .fixedSize(horizontal: false, vertical: true)
                    .padding(.horizontal, Spacing.xl)
                    .accessibilityIdentifier("onboarding.searchPrompt.body")
            }
            .opacity(appeared ? 1 : 0)
            .offset(y: appeared ? 0 : 12)

            Spacer()

            OnboardingPrimaryButton(
                label: OnboardingFlowCopy.searchGetStartedButton,
                action: onGetStarted
            )
            .padding(.horizontal, Spacing.xl)
            .padding(.bottom, Spacing.xxl)
            .opacity(appeared ? 1 : 0)
        }
        .onAppear {
            withAnimation(.easeOut(duration: 0.4)) {
                appeared = true
            }
        }
    }
}

// MARK: - Shared primary button

/// Bone-on-copper primary CTA shared by all three screens.
private struct OnboardingPrimaryButton: View {

    let label: String
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Text(label)
                .font(AppTypography.sans(size: 16, weight: .semibold))
                .foregroundStyle(Palette.bone)
                .frame(maxWidth: .infinity)
                .frame(height: 52)
                .background(
                    RoundedRectangle(cornerRadius: CornerRadius.medium)
                        .fill(AppColors.accent)
                )
        }
        .accessibilityLabel(label)
        .accessibilityIdentifier("onboarding.primaryButton")
    }
}

// MARK: - Preview

#Preview("Onboarding") {
    OnboardingView()
        .preferredColorScheme(.dark)
}
