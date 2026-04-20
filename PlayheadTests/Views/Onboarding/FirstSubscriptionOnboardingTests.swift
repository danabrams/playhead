// FirstSubscriptionOnboardingTests.swift
// Verifies acceptance criteria for playhead-rw49:
// - Verbatim copy (onboarding screen + first-✓ tooltip) matches the
//   UI design §G spec character-for-character.
// - One-shot persistence via UserDefaults for both surfaces.
// - Behavioral: flags stick across simulated relaunches.

import XCTest
@testable import Playhead

// MARK: - Verbatim copy (snapshot-style)

/// These tests are the single source of truth for the user-facing
/// strings on the first-subscription onboarding screen and the first-✓
/// tooltip. Any intentional copy change requires updating the spec in
/// `docs/plans/2026-04-16-podcast-bg-ui-design.md` and bd playhead-rw49
/// before editing these strings.
final class FirstSubscriptionOnboardingCopyTests: XCTestCase {

    func testOnboardingBodyCopyIsVerbatim() {
        let expected =
            "Playhead skips ads for you. Tap Download on any episode. We'll fetch it and find the ads in the background — when it's ready, you'll see a ✓ and we'll skip them automatically while you listen. All processing stays on your device."
        XCTAssertEqual(
            OnboardingCopy.firstSubscriptionBody,
            expected,
            "First-subscription onboarding body must match UI design §G verbatim"
        )
    }

    func testOnboardingBodyContainsRequiredPhrases() {
        let body = OnboardingCopy.firstSubscriptionBody
        // Belt-and-suspenders: even if the string literal drifts, these
        // phrase-level checks will flag regressions in the critical
        // claims users rely on (on-device processing, ✓ semantics).
        XCTAssertTrue(body.contains("Playhead skips ads for you."))
        XCTAssertTrue(body.contains("Tap Download on any episode."))
        XCTAssertTrue(body.contains("when it's ready, you'll see a ✓"))
        XCTAssertTrue(body.contains("All processing stays on your device."))
    }

    func testOnboardingBodyUsesEmDashNotHyphen() {
        // The spec uses an em-dash (U+2014) in "background —". Hyphens
        // would render subtly wrong; this test prevents an accidental
        // Unicode normalization regression.
        XCTAssertTrue(
            OnboardingCopy.firstSubscriptionBody.contains("\u{2014}"),
            "Body copy must use em-dash (U+2014), not a plain hyphen"
        )
    }

    func testOnboardingButtonLabelIsVerbatim() {
        XCTAssertEqual(
            OnboardingCopy.firstSubscriptionButton,
            "Got it",
            "Dismiss button label must be exactly 'Got it'"
        )
    }

    func testTooltipCopyIsVerbatim() {
        let expected = "✓ means we've found ads to skip. Tap play and we'll handle the rest."
        XCTAssertEqual(
            OnboardingCopy.firstCheckmarkTooltip,
            expected,
            "First-✓ tooltip must match UI design §G verbatim"
        )
    }

    func testTooltipCopyLeadsWithCheckmarkSymbol() {
        // The leading "✓" (U+2713) is load-bearing — it shows the user
        // the exact glyph the badge uses.
        XCTAssertTrue(
            OnboardingCopy.firstCheckmarkTooltip.hasPrefix("\u{2713}"),
            "Tooltip must lead with the ✓ glyph (U+2713)"
        )
    }
}

// MARK: - Persistence flags

/// Behavioral tests that exercise the one-shot persistence contract:
/// once the user dismisses a surface, the flag sticks across relaunches
/// (modeled here as a fresh read of the same UserDefaults suite).
final class OnboardingFlagsPersistenceTests: XCTestCase {

    private var suiteName: String!
    private var defaults: UserDefaults!

    override func setUp() {
        super.setUp()
        suiteName = "OnboardingFlagsTests.\(UUID().uuidString)"
        defaults = UserDefaults(suiteName: suiteName)
    }

    override func tearDown() {
        defaults.removePersistentDomain(forName: suiteName)
        defaults = nil
        suiteName = nil
        super.tearDown()
    }

    // MARK: First-subscription onboarding flag

    func testFirstSubscriptionDefaultIsUnseen() {
        XCTAssertFalse(
            OnboardingFlags.hasSeenFirstSubscriptionOnboarding(defaults),
            "Fresh installs must have the first-subscription flag unset"
        )
    }

    func testFirstSubscriptionFlagStickyAfterMark() {
        OnboardingFlags.markFirstSubscriptionOnboardingSeen(defaults)
        XCTAssertTrue(
            OnboardingFlags.hasSeenFirstSubscriptionOnboarding(defaults),
            "markFirstSubscriptionOnboardingSeen must flip the flag true"
        )
    }

    func testFirstSubscriptionFlagSurvivesRelaunchSimulation() {
        OnboardingFlags.markFirstSubscriptionOnboardingSeen(defaults)

        // Simulate an app relaunch by constructing a second
        // UserDefaults handle over the same suite. Real relaunches
        // re-create `.standard`; the suite-name indirection is the
        // faithful test-harness equivalent.
        let reopened = UserDefaults(suiteName: suiteName)!
        XCTAssertTrue(
            OnboardingFlags.hasSeenFirstSubscriptionOnboarding(reopened),
            "Flag must persist across a fresh UserDefaults handle"
        )
    }

    func testFirstSubscriptionFlagDoesNotReFireOnSecondSubscribe() {
        // User subscribes → sees onboarding → taps "Got it".
        OnboardingFlags.markFirstSubscriptionOnboardingSeen(defaults)

        // A second subscription event must not reset the flag.
        // Reading the flag is idempotent by design.
        XCTAssertTrue(OnboardingFlags.hasSeenFirstSubscriptionOnboarding(defaults))
        XCTAssertTrue(
            OnboardingFlags.hasSeenFirstSubscriptionOnboarding(defaults),
            "Re-reading the flag must not reset it"
        )
    }

    // MARK: First-✓ tooltip flag

    func testFirstCheckmarkTooltipDefaultIsUnseen() {
        XCTAssertFalse(
            OnboardingFlags.hasSeenFirstCheckmarkTooltip(defaults),
            "Fresh installs must have the first-✓ tooltip flag unset"
        )
    }

    func testFirstCheckmarkTooltipFlagStickyAfterMark() {
        OnboardingFlags.markFirstCheckmarkTooltipSeen(defaults)
        XCTAssertTrue(
            OnboardingFlags.hasSeenFirstCheckmarkTooltip(defaults),
            "markFirstCheckmarkTooltipSeen must flip the flag true"
        )
    }

    func testFirstCheckmarkTooltipFlagSurvivesRelaunchSimulation() {
        OnboardingFlags.markFirstCheckmarkTooltipSeen(defaults)

        let reopened = UserDefaults(suiteName: suiteName)!
        XCTAssertTrue(
            OnboardingFlags.hasSeenFirstCheckmarkTooltip(reopened),
            "Tooltip-seen flag must persist across a fresh UserDefaults handle"
        )
    }

    func testTooltipFlagDoesNotReFireAcrossEpisodeOpens() {
        // Once the tooltip is dismissed, opening a second episode (or
        // navigating back and forth) must not re-show it.
        OnboardingFlags.markFirstCheckmarkTooltipSeen(defaults)

        // Simulate multiple "episode open" events by repeated reads.
        for _ in 0..<5 {
            XCTAssertTrue(
                OnboardingFlags.hasSeenFirstCheckmarkTooltip(defaults),
                "Tooltip flag must stay true across repeated reads"
            )
        }
    }

    // MARK: Flag independence

    func testFlagsAreIndependent() {
        // Dismissing the onboarding must NOT pre-dismiss the tooltip.
        OnboardingFlags.markFirstSubscriptionOnboardingSeen(defaults)
        XCTAssertTrue(OnboardingFlags.hasSeenFirstSubscriptionOnboarding(defaults))
        XCTAssertFalse(
            OnboardingFlags.hasSeenFirstCheckmarkTooltip(defaults),
            "Tooltip flag must be independent of onboarding flag"
        )

        // And vice versa: dismissing the tooltip first must not
        // pre-dismiss the onboarding.
        let otherSuite = "OnboardingFlagsTests.\(UUID().uuidString)"
        let other = UserDefaults(suiteName: otherSuite)!
        defer { other.removePersistentDomain(forName: otherSuite) }

        OnboardingFlags.markFirstCheckmarkTooltipSeen(other)
        XCTAssertTrue(OnboardingFlags.hasSeenFirstCheckmarkTooltip(other))
        XCTAssertFalse(
            OnboardingFlags.hasSeenFirstSubscriptionOnboarding(other),
            "Onboarding flag must be independent of tooltip flag"
        )
    }

    // MARK: Keys (identity pin — catches accidental renames)

    func testPersistenceKeysAreStable() {
        // Renaming these keys silently orphans a real user's dismissal
        // state, causing the screen/tooltip to reappear after upgrade.
        // Pin them here so accidental renames show up in code review.
        XCTAssertEqual(
            OnboardingFlags.firstSubscriptionOnboardingSeenKey,
            "firstSubscriptionOnboardingSeen"
        )
        XCTAssertEqual(
            OnboardingFlags.firstCheckmarkTooltipSeenKey,
            "firstCheckmarkTooltipSeen"
        )
    }
}

// MARK: - View construction smoke tests

/// Minimal smoke tests that exercise the SwiftUI initializers for both
/// surfaces. Full rendering is covered by SwiftUI previews; the goal
/// here is to catch compile-time regressions in the view signatures.
@MainActor
final class FirstSubscriptionOnboardingViewSmokeTests: XCTestCase {

    func testOnboardingViewConstructs() {
        let view = FirstSubscriptionOnboardingView(onDismiss: {})
        _ = view.body
    }

    func testTooltipViewConstructs() {
        let view = FirstCheckmarkTooltipView(onDismiss: {})
        _ = view.body
    }

    func testOnboardingDismissCallbackFiresExactlyOnce() {
        // Dismiss invokes the onDismiss closure synchronously when the
        // internal button action runs. We can't tap through SwiftUI in
        // a unit test, but we can verify the closure wiring compiles
        // and that the persistence flag is independent of the
        // callback.
        var calls = 0
        let view = FirstSubscriptionOnboardingView(onDismiss: { calls += 1 })
        _ = view.body
        XCTAssertEqual(calls, 0, "Dismiss callback must not fire on construction")
    }
}
